package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectSQLiteTableForSQLServer maps only the SQLite source shapes whose
// stored values and relational behavior can be reproduced by the native SQL
// Server target. SQLite's dynamic storage classes are checked separately by
// the source-data preflight; this function owns the deterministic catalog
// projection and rejects metadata whose target semantics differ.
func projectSQLiteTableForSQLServer(
	source schema.Table,
) (schema.Table, error) {
	if source.Schema != "" {
		return schema.Table{}, sqliteSQLServerPolicy(
			"map SQLite schema namespace",
			source.Schema,
		)
	}
	if source.SQLiteStrict {
		return schema.Table{}, sqliteSQLServerPolicy(
			"map SQLite STRICT table",
			source.Name,
		)
	}
	if source.SQLiteWithoutRowID {
		return schema.Table{}, sqliteSQLServerPolicy(
			"map SQLite WITHOUT ROWID table",
			source.Name,
		)
	}
	if source.MySQLCollation != "" ||
		len(source.ClickHouseOrderBy) != 0 {
		return schema.Table{}, sqliteSQLServerPolicy(
			"map SQLite table metadata",
			source.Name,
		)
	}
	if source.Identity == nil {
		if column, ok := sqliteImplicitRowIDAlias(source); ok {
			return schema.Table{}, sqliteSQLServerPolicy(
				"map SQLite implicit rowid identity",
				source.Name+"."+column,
			)
		}
	}

	projected := cloneSQLServerTargetTable(source)
	projected.SQLiteStrict = false
	projected.SQLiteWithoutRowID = false

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for index, column := range source.Columns {
		if column.Name == "" ||
			sqliteSQLServerColumnExists(sourceColumns, column.Name) {
			return schema.Table{}, sqliteSQLServerPolicy(
				"map SQLite column catalog shape",
				source.Name+"."+column.Name,
			)
		}
		target, err := projectSQLiteColumnForSQLServer(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQLite column %s.%s to SQL Server: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
		sourceColumns[column.Name] = column
	}

	if err := validateSQLiteSQLServerPrimaryKey(
		source,
		projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := projectSQLiteSQLServerIndexes(
		source,
		&projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := projectSQLiteSQLServerForeignKeys(
		source,
		&projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := validateSQLiteSQLServerChecks(
		source,
		projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	return projected, nil
}

func projectSQLiteColumnForSQLServer(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqliteSQLServerPolicy(
			"map SQLite declared type",
			source.Name,
		)
	}
	base := strings.ToLower(strings.Join(
		strings.Fields(source.DeclaredType.Base),
		" ",
	))
	canonical := strings.ToLower(strings.Join(
		strings.Fields(source.Type),
		" ",
	))
	if base == "" || canonical != base {
		return schema.Column{}, sqliteSQLServerPolicy(
			"map SQLite declared type",
			source.Name,
		)
	}
	arguments := append(
		[]int(nil),
		source.DeclaredType.Arguments...,
	)
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
	}
	noArguments := func() bool {
		return len(arguments) == 0
	}

	switch base {
	case "int", "integer", "tinyint", "smallint", "mediumint",
		"bigint", "int2", "int8":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		// Every SQLite INTEGER-affinity declaration can store the complete
		// signed 64-bit runtime domain.
		target.Type = "bigint"
		declaration("bigint")
	case "real", "double", "double precision", "float":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "double precision"
		declaration("double precision")
	case "numeric", "decimal":
		if len(arguments) < 1 || len(arguments) > 2 ||
			arguments[0] < 1 || arguments[0] > 18 {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite numeric modifier",
				source.Name,
			)
		}
		scale := 0
		if len(arguments) == 2 {
			scale = arguments[1]
		}
		if scale != 0 {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite numeric modifier",
				source.Name,
			)
		}
		// Only integral DECIMAL values with at most 18 digits have a
		// representation that can be proven identical: SQLite stores them
		// as signed 64-bit INTEGER values, while fractional NUMERIC values
		// pass through binary64 REAL storage before the target can see them.
		target.Type = "numeric"
		declaration("decimal", arguments[0], scale)
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar":
		target.Type = "text"
		switch {
		case len(arguments) == 0:
			declaration("text")
		case len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 2_000:
			// SQLite's modifier is treated as a Unicode character limit by
			// the value preflight. SQL Server VARCHAR under DMTX's UTF-8
			// collation counts encoded bytes, hence the four-byte expansion.
			declaration("varchar", arguments[0]*4)
		default:
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite character modifier",
				source.Name,
			)
		}
	case "text", "clob":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "text"
		declaration("text")
	case "blob":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "blob"
		declaration("blob")
	case "binary", "varbinary":
		target.Type = "blob"
		switch {
		case len(arguments) == 0:
			declaration("blob")
		case len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 8_000:
			// SQLite does not pad BINARY values, so both declarations map
			// to variable-length binary storage.
			declaration("varbinary", arguments[0])
		default:
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite binary modifier",
				source.Name,
			)
		}
	case "bool", "boolean":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "boolean"
		declaration("bool")
	case "date":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "date"
		declaration("date")
	case "datetime", "timestamp":
		precision := 6
		if len(arguments) == 1 {
			precision = arguments[0]
		} else if len(arguments) != 0 {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite temporal modifier",
				source.Name,
			)
		}
		if precision < 0 || precision > 6 {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite temporal modifier",
				source.Name,
			)
		}
		target.Type = "datetime"
		declaration("timestamp", precision)
	case "uuid":
		if !noArguments() {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "uuid"
		declaration("uuid")
	default:
		return schema.Column{}, sqliteSQLServerPolicy(
			"map SQLite type",
			base,
		)
	}

	if target.Default != nil {
		if _, err := schema.RenderSQLServerDefault(target); err != nil {
			return schema.Column{}, sqliteSQLServerPolicy(
				"map SQLite default",
				source.Name,
			)
		}
	}
	return target, nil
}

func validateSQLiteSQLServerPrimaryKey(
	source schema.Table,
	projected schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	positions := make(map[int]bool)
	keyCount := 0
	for _, column := range source.Columns {
		if column.PrimaryKey != (column.PrimaryKeyPosition > 0) {
			return sqliteSQLServerPolicy(
				"map SQLite primary-key catalog shape",
				source.Name+"."+column.Name,
			)
		}
		if column.PrimaryKeyPosition == 0 {
			continue
		}
		keyCount++
		if positions[column.PrimaryKeyPosition] ||
			column.PrimaryKeyPosition < 1 ||
			sqliteSQLServerComparisonUnsafe(column) {
			return sqliteSQLServerPolicy(
				"map SQLite primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = true
	}
	for expected := 1; expected <= keyCount; expected++ {
		if !positions[expected] {
			return sqliteSQLServerPolicy(
				"map SQLite primary-key catalog shape",
				source.Name,
			)
		}
	}

	if source.Identity == nil {
		return nil
	}
	identity := source.Identity
	column, exists := sourceColumns[identity.Column]
	if !exists ||
		identity.Generation != schema.IdentityByDefault ||
		keyCount != 1 ||
		!column.PrimaryKey ||
		column.PrimaryKeyPosition != 1 ||
		column.DeclaredType == nil ||
		!strings.EqualFold(
			strings.TrimSpace(column.DeclaredType.Base),
			"integer",
		) ||
		len(column.DeclaredType.Arguments) != 0 ||
		column.Default != nil {
		return sqliteSQLServerPolicy(
			"map SQLite AUTOINCREMENT identity",
			source.Name,
		)
	}
	for index := range projected.Columns {
		if projected.Columns[index].Name == identity.Column {
			// SQLite's INTEGER PRIMARY KEY metadata reports nullable even
			// though rowid aliases cannot store NULL. Preserve the physical
			// source invariant required by SQL Server's identity key.
			projected.Columns[index].Nullable = false
			break
		}
	}
	if _, err := schema.CreateSQLServerTable(sqliteSQLServerRenderTable(
		projected,
	)); err != nil {
		return sqliteSQLServerPolicy(
			"map SQLite AUTOINCREMENT identity",
			source.Name,
		)
	}
	return nil
}

func projectSQLiteSQLServerIndexes(
	source schema.Table,
	projected *schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	for index := range source.Indexes {
		sourceIndex := source.Indexes[index]
		targetIndex := &projected.Indexes[index]
		if len(sourceIndex.Columns) == 0 ||
			sourceIndex.Inline && (!sourceIndex.Unique ||
				sourceIndex.Name != "") ||
			!sourceIndex.Inline && sourceIndex.Name == "" {
			return sqliteSQLServerPolicy(
				"map SQLite index catalog shape",
				source.Name+"."+sourceIndex.Name,
			)
		}
		if sourceIndex.Inline {
			// SQL Server models the unnamed SQLite UNIQUE constraint as a
			// deterministic standalone unique index.
			targetIndex.Inline = false
			targetIndex.Name = ""
		}
		seen := make(map[string]bool, len(sourceIndex.Columns))
		for columnIndex, indexed := range sourceIndex.Columns {
			column, exists := sourceColumns[indexed.Name]
			folded := strings.ToLower(indexed.Name)
			if !exists || seen[folded] {
				return sqliteSQLServerPolicy(
					"map SQLite index column",
					source.Name+"."+indexed.Name,
				)
			}
			seen[folded] = true
			if !strings.EqualFold(
				strings.TrimSpace(indexed.Collation),
				"BINARY",
			) {
				return sqliteSQLServerPolicy(
					"map SQLite index collation",
					source.Name+"."+sourceIndex.Name,
				)
			}
			if sqliteSQLServerComparisonUnsafe(column) {
				return sqliteSQLServerPolicy(
					"map SQLite index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if sourceIndex.Unique && column.Nullable {
				return sqliteSQLServerPolicy(
					"map SQLite nullable unique index",
					source.Name+"."+sourceIndex.Name,
				)
			}
			targetIndex.Columns[columnIndex].Collation = ""
		}
	}
	return nil
}

func projectSQLiteSQLServerForeignKeys(
	source schema.Table,
	projected *schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	for index := range source.ForeignKeys {
		sourceForeignKey := source.ForeignKeys[index]
		targetForeignKey := &projected.ForeignKeys[index]
		if sourceForeignKey.Name != "" ||
			len(sourceForeignKey.Columns) == 0 ||
			len(sourceForeignKey.ReferencedColumns) != 0 &&
				len(sourceForeignKey.ReferencedColumns) !=
					len(sourceForeignKey.Columns) {
			return sqliteSQLServerPolicy(
				"map SQLite foreign-key catalog shape",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		switch strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.Match,
		)) {
		case "", "NONE":
			targetForeignKey.Match = "SIMPLE"
		default:
			return sqliteSQLServerPolicy(
				"map SQLite foreign-key match",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		if err := validateSQLiteSQLServerForeignKeyAction(
			sourceForeignKey.OnUpdate,
		); err != nil {
			return err
		}
		if err := validateSQLiteSQLServerForeignKeyAction(
			sourceForeignKey.OnDelete,
		); err != nil {
			return err
		}
		for _, name := range sourceForeignKey.Columns {
			column, exists := sourceColumns[name]
			if !exists {
				return sqliteSQLServerPolicy(
					"map SQLite foreign-key column",
					source.Name+"."+name,
				)
			}
			if sqliteSQLServerComparisonUnsafe(column) {
				return sqliteSQLServerPolicy(
					"map SQLite foreign-key comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	return nil
}

func validateSQLiteSQLServerChecks(
	source schema.Table,
	projected schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	for _, check := range source.Checks {
		if check.Name != "" {
			return sqliteSQLServerPolicy(
				"map SQLite CHECK catalog shape",
				source.Name+"."+check.Name,
			)
		}
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return sqliteSQLServerPolicy(
				"map SQLite CHECK expression",
				source.Name,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists || sqliteSQLServerComparisonUnsafe(column) {
				return sqliteSQLServerPolicy(
					"map SQLite CHECK comparison",
					source.Name+"."+name,
				)
			}
		}
		// Admitted SQLite DECIMAL/NUMERIC values are physically INTEGER,
		// while a fractional SQLite numeric literal is evaluated through
		// binary64. Validate a comparison-only view in which those columns
		// are integral so an exact SQL Server DECIMAL literal cannot silently
		// change the source CHECK around the binary64 precision boundary.
		if _, err := schema.RenderSQLiteCheckForPostgres(
			check.Expression,
			sqliteSQLServerCheckGuardColumns(projected.Columns),
		); err != nil {
			return sqliteSQLServerPolicy(
				"map SQLite CHECK source numeric semantics",
				source.Name,
			)
		}
		// Re-resolve the portable expression against the widened target
		// declarations so numeric literal ranges and families cannot change
		// silently during projection.
		if _, err := schema.RenderPortableCheckForSQLServer(
			check.Expression,
			projected.Columns,
		); err != nil {
			return sqliteSQLServerPolicy(
				"map SQLite CHECK expression",
				source.Name,
			)
		}
	}
	return nil
}

func sqliteSQLServerCheckGuardColumns(
	columns []schema.Column,
) []schema.Column {
	guarded := append([]schema.Column(nil), columns...)
	for index := range guarded {
		declaration := guarded[index].DeclaredType
		if declaration == nil ||
			len(declaration.Arguments) != 2 ||
			declaration.Arguments[1] != 0 {
			continue
		}
		switch strings.ToLower(strings.Join(
			strings.Fields(declaration.Base),
			" ",
		)) {
		case "decimal", "numeric":
			guarded[index].Type = "bigint"
			guarded[index].DeclaredType = &schema.DeclaredType{
				Base: "bigint",
			}
		}
	}
	return guarded
}

// validateSQLiteSQLServerTables validates the complete projected selection.
// Callers must assign the physical SQL Server schema to projected before
// invoking it. The SQL Server planner then proves selected FK targets, key
// compatibility, object-name scopes, and cascade topology as one set.
func validateSQLiteSQLServerTables(
	source []schema.Table,
	projected []schema.Table,
) error {
	if len(source) != len(projected) {
		return sqliteSQLServerPolicy(
			"map SQLite table selection",
			"source and target table counts differ",
		)
	}
	for index := range source {
		if source[index].Schema != "" ||
			source[index].Name != projected[index].Name {
			return sqliteSQLServerPolicy(
				"map SQLite table selection",
				source[index].Name,
			)
		}
	}
	if err := materializeSQLiteSQLServerForeignKeyColumns(
		projected,
	); err != nil {
		return err
	}
	materialized, err := schema.MaterializeSQLServerObjectNames(
		projected,
	)
	if err != nil {
		return fmt.Errorf(
			"plan SQLite to SQL Server relational objects: %w",
			err,
		)
	}
	copy(projected, materialized)
	return nil
}

func materializeSQLiteSQLServerForeignKeyColumns(
	tables []schema.Table,
) error {
	for tableIndex := range tables {
		for foreignKeyIndex := range tables[tableIndex].ForeignKeys {
			foreignKey :=
				&tables[tableIndex].ForeignKeys[foreignKeyIndex]
			if len(foreignKey.ReferencedColumns) != 0 {
				continue
			}
			referencedIndex := -1
			for candidate := range tables {
				if !strings.EqualFold(
					tables[candidate].Name,
					foreignKey.ReferencedTable,
				) {
					continue
				}
				if referencedIndex >= 0 {
					return sqliteSQLServerPolicy(
						"map SQLite foreign-key target",
						tables[tableIndex].Name+"."+
							foreignKey.ReferencedTable,
					)
				}
				referencedIndex = candidate
			}
			if referencedIndex < 0 {
				return sqliteSQLServerPolicy(
					"map SQLite foreign-key target",
					tables[tableIndex].Name+"."+
						foreignKey.ReferencedTable,
				)
			}
			columns := primaryKeyColumns(tables[referencedIndex])
			if len(columns) == 0 {
				return sqliteSQLServerPolicy(
					"map SQLite foreign-key referenced primary key",
					tables[tableIndex].Name+"."+
						foreignKey.ReferencedTable,
				)
			}
			foreignKey.ReferencedColumns = append(
				[]string(nil),
				columns...,
			)
		}
	}
	return nil
}

func validateSQLiteSQLServerForeignKeyAction(value string) error {
	switch strings.ToUpper(strings.Join(strings.Fields(value), " ")) {
	case "", "NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT":
		return nil
	default:
		return sqliteSQLServerPolicy(
			"map SQLite foreign-key action",
			value,
		)
	}
}

func sqliteSQLServerComparisonUnsafe(column schema.Column) bool {
	if column.DeclaredType == nil {
		return true
	}
	switch strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	)) {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob", "uuid",
		// SQL Server right-pads binary strings with zero bytes for
		// comparison, so distinct SQLite BLOB keys such as 0x01 and
		// 0x0100 can compare equal after projection.
		"blob", "binary", "varbinary":
		return true
	default:
		return false
	}
}

func sqliteSQLServerColumnExists(
	columns map[string]schema.Column,
	name string,
) bool {
	for existing := range columns {
		if strings.EqualFold(existing, name) {
			return true
		}
	}
	return false
}

func sqliteSQLServerRenderTable(table schema.Table) schema.Table {
	if table.Schema == "" {
		table.Schema = "dbo"
	}
	return table
}

func sqliteSQLServerPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLServer),
	}
}
