package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectSQLiteTableForMySQL maps only SQLite source metadata whose stored
// values and relational behavior can be represented exactly by the native
// MySQL-family target. SQLite's dynamic storage classes are verified by the
// source-data preflight; this function owns the pure catalog projection.
func projectSQLiteTableForMySQL(
	source schema.Table,
) (schema.Table, error) {
	if source.Schema != "" {
		return schema.Table{}, sqliteMySQLPolicy(
			"map SQLite schema namespace",
			source.Schema,
		)
	}
	if source.SQLiteStrict {
		return schema.Table{}, sqliteMySQLPolicy(
			"map SQLite STRICT table",
			source.Name,
		)
	}
	if source.SQLiteWithoutRowID {
		return schema.Table{}, sqliteMySQLPolicy(
			"map SQLite WITHOUT ROWID table",
			source.Name,
		)
	}
	if strings.TrimSpace(source.MySQLCollation) != "" ||
		len(source.ClickHouseOrderBy) != 0 {
		return schema.Table{}, sqliteMySQLPolicy(
			"map SQLite table metadata",
			source.Name,
		)
	}
	if source.Identity == nil {
		if column, ok := sqliteImplicitRowIDAlias(source); ok {
			return schema.Table{}, sqliteMySQLPolicy(
				"map SQLite implicit rowid identity",
				source.Name+"."+column,
			)
		}
	}

	projected := cloneMySQLTargetTable(source)
	projected.SQLiteStrict = false
	projected.SQLiteWithoutRowID = false

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for index, column := range source.Columns {
		if column.Name == "" ||
			sqliteMySQLColumnExists(sourceColumns, column.Name) {
			return schema.Table{}, sqliteMySQLPolicy(
				"map SQLite column catalog shape",
				source.Name+"."+column.Name,
			)
		}
		target, err := projectSQLiteColumnForMySQL(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQLite column %s.%s to MySQL: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
		sourceColumns[column.Name] = column
	}

	if err := validateSQLiteMySQLPrimaryKey(
		source,
		&projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := projectSQLiteMySQLIndexes(
		source,
		&projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := projectSQLiteMySQLForeignKeys(
		source,
		&projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := validateSQLiteMySQLChecks(
		source,
		projected,
		sourceColumns,
	); err != nil {
		return schema.Table{}, err
	}
	if err := addSQLiteMySQLBooleanChecks(
		source,
		&projected,
	); err != nil {
		return schema.Table{}, err
	}
	return projected, nil
}

func projectSQLiteColumnForMySQL(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqliteMySQLPolicy(
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
		return schema.Column{}, sqliteMySQLPolicy(
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
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "bigint"
		declaration("bigint")
	case "real", "double", "double precision", "float":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "double precision"
		declaration("double")
	case "numeric", "decimal":
		if len(arguments) < 1 || len(arguments) > 2 ||
			arguments[0] < 1 || arguments[0] > 18 {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite numeric modifier",
				source.Name,
			)
		}
		scale := 0
		if len(arguments) == 2 {
			scale = arguments[1]
		}
		if scale != 0 {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite numeric modifier",
				source.Name,
			)
		}
		target.Type = "numeric"
		declaration("decimal", arguments[0], 0)
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar":
		switch {
		case noArguments():
			target.Type = "text"
			declaration("longtext")
		case len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 16_383:
			// SQLite does not pad CHAR values or enforce the modifier.
			// VARCHAR retains source trailing spaces while the value
			// preflight enforces the declared Unicode-character limit.
			target.Type = "varchar"
			declaration("varchar", arguments[0])
		default:
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite character modifier",
				source.Name,
			)
		}
	case "text", "clob":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "text"
		declaration("longtext")
	case "blob":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "blob"
		declaration("longblob")
	case "binary", "varbinary":
		switch {
		case noArguments():
			target.Type = "blob"
			declaration("longblob")
		case len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 65_535:
			// SQLite does not right-pad BINARY values.
			target.Type = "varbinary"
			declaration("varbinary", arguments[0])
		default:
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite binary modifier",
				source.Name,
			)
		}
	case "bool", "boolean":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		target.Type = "integer"
		declaration("tinyint", 1)
	case "date":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
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
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite temporal modifier",
				source.Name,
			)
		}
		if precision < 0 || precision > 6 {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite temporal modifier",
				source.Name,
			)
		}
		target.Type = "datetime"
		declaration("datetime", precision)
	case "uuid":
		if !noArguments() {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite type modifier",
				source.Name,
			)
		}
		// VARCHAR avoids fixed-width padding. The value boundary admits one
		// canonical 36-character UUID spelling.
		target.Type = "uuid"
		declaration("varchar", 36)
	default:
		return schema.Column{}, sqliteMySQLPolicy(
			"map SQLite type",
			base,
		)
	}

	if target.Default != nil {
		if sqliteMySQLCurrentTimestampDefault(
			source.Default.CanonicalSQL(),
		) &&
			target.DeclaredType != nil &&
			len(target.DeclaredType.Arguments) == 1 &&
			target.DeclaredType.Arguments[0] > 0 {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite fractional temporal default",
				source.Name,
			)
		}
		normalized, err := schema.NormalizeMySQLDefault(target)
		if err != nil {
			return schema.Column{}, sqliteMySQLPolicy(
				"map SQLite default",
				source.Name,
			)
		}
		target.Default = normalized
	}
	return target, nil
}

func sqliteMySQLCurrentTimestampDefault(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.Trim(value, " \t\r\n()")
	return value == "CURRENT_TIMESTAMP"
}

func validateSQLiteMySQLPrimaryKey(
	source schema.Table,
	projected *schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	positions := make(map[int]bool)
	keyCount := 0
	for _, column := range source.Columns {
		if column.PrimaryKey != (column.PrimaryKeyPosition > 0) {
			return sqliteMySQLPolicy(
				"map SQLite primary-key catalog shape",
				source.Name+"."+column.Name,
			)
		}
		if column.PrimaryKeyPosition == 0 {
			continue
		}
		keyCount++
		if positions[column.PrimaryKeyPosition] ||
			column.PrimaryKeyPosition < 1 {
			return sqliteMySQLPolicy(
				"map SQLite primary-key catalog shape",
				source.Name+"."+column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = true
	}
	for expected := 1; expected <= keyCount; expected++ {
		if !positions[expected] {
			return sqliteMySQLPolicy(
				"map SQLite primary-key catalog shape",
				source.Name,
			)
		}
	}

	if source.Identity == nil {
		for _, column := range projected.Columns {
			if column.PrimaryKeyPosition > 0 && column.Nullable {
				return sqliteMySQLPolicy(
					"map SQLite nullable primary key",
					source.Name+"."+column.Name,
				)
			}
		}
		return nil
	}
	identity := source.Identity
	column, exists := sourceColumns[identity.Column]
	if !exists ||
		identity.Generation != schema.IdentityByDefault ||
		identity.Frontier != nil && *identity.Frontier < 0 ||
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
		return sqliteMySQLPolicy(
			"map SQLite AUTOINCREMENT identity",
			source.Name,
		)
	}
	for index := range projected.Columns {
		if projected.Columns[index].Name != identity.Column {
			continue
		}
		// SQLite reports its rowid alias nullable even though it cannot
		// physically store NULL.
		projected.Columns[index].Nullable = false
		return nil
	}
	return sqliteMySQLPolicy(
		"map SQLite AUTOINCREMENT identity",
		source.Name,
	)
}

func projectSQLiteMySQLIndexes(
	source schema.Table,
	projected *schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	for index := range source.Indexes {
		sourceIndex := source.Indexes[index]
		targetIndex := &projected.Indexes[index]
		if len(sourceIndex.Columns) == 0 ||
			sourceIndex.Inline &&
				(!sourceIndex.Unique || sourceIndex.Name != "") ||
			!sourceIndex.Inline && sourceIndex.Name == "" {
			return sqliteMySQLPolicy(
				"map SQLite index catalog shape",
				source.Name+"."+sourceIndex.Name,
			)
		}
		if sourceIndex.Inline {
			// MySQL discovers an inline SQLite UNIQUE constraint as a
			// named standalone unique index.
			targetIndex.Inline = false
			targetIndex.Name = ""
		}
		seen := make(map[string]bool, len(sourceIndex.Columns))
		for columnIndex, indexed := range sourceIndex.Columns {
			column, exists := sourceColumns[indexed.Name]
			folded := strings.ToLower(indexed.Name)
			if !exists || seen[folded] {
				return sqliteMySQLPolicy(
					"map SQLite index column",
					source.Name+"."+indexed.Name,
				)
			}
			seen[folded] = true
			if !strings.EqualFold(
				strings.TrimSpace(indexed.Collation),
				"BINARY",
			) {
				return sqliteMySQLPolicy(
					"map SQLite index collation",
					source.Name+"."+sourceIndex.Name,
				)
			}
			if sqliteMySQLTextColumn(column) {
				targetIndex.Columns[columnIndex].Collation = "BINARY"
			} else {
				targetIndex.Columns[columnIndex].Collation = ""
			}
		}
	}
	return nil
}

func projectSQLiteMySQLForeignKeys(
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
			return sqliteMySQLPolicy(
				"map SQLite foreign-key catalog shape",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		switch strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.Match,
		)) {
		case "", "NONE":
			targetForeignKey.Match = "NONE"
		default:
			return sqliteMySQLPolicy(
				"map SQLite foreign-key match",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		if err := validateSQLiteMySQLForeignKeyAction(
			sourceForeignKey.OnUpdate,
		); err != nil {
			return err
		}
		if err := validateSQLiteMySQLForeignKeyAction(
			sourceForeignKey.OnDelete,
		); err != nil {
			return err
		}
		for _, name := range sourceForeignKey.Columns {
			if _, exists := sourceColumns[name]; !exists {
				return sqliteMySQLPolicy(
					"map SQLite foreign-key column",
					source.Name+"."+name,
				)
			}
		}
	}
	return nil
}

func validateSQLiteMySQLChecks(
	source schema.Table,
	projected schema.Table,
	sourceColumns map[string]schema.Column,
) error {
	for _, check := range source.Checks {
		if check.Name != "" {
			return sqliteMySQLPolicy(
				"map SQLite CHECK catalog shape",
				source.Name+"."+check.Name,
			)
		}
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return sqliteMySQLPolicy(
				"map SQLite CHECK expression",
				source.Name,
			)
		}
		for _, name := range referenced {
			if _, exists := sourceColumns[name]; !exists {
				return sqliteMySQLPolicy(
					"map SQLite CHECK column",
					source.Name+"."+name,
				)
			}
		}
		// SQLite evaluates fractional numeric literals through binary64.
		// Treat admitted integral DECIMAL columns as integers during this
		// source-semantics proof so an exact MySQL DECIMAL literal cannot
		// silently change a boundary comparison.
		if _, err := schema.RenderSQLiteCheckForPostgres(
			check.Expression,
			sqliteMySQLCheckGuardColumns(projected.Columns),
		); err != nil {
			return sqliteMySQLPolicy(
				"map SQLite CHECK source semantics",
				source.Name,
			)
		}
		if _, err := schema.RenderPortableCheckForMySQL(
			check.Expression,
			projected.Columns,
		); err != nil {
			return sqliteMySQLPolicy(
				"map SQLite CHECK expression",
				source.Name,
			)
		}
	}
	return nil
}

func addSQLiteMySQLBooleanChecks(
	source schema.Table,
	projected *schema.Table,
) error {
	for _, column := range projected.Columns {
		if !mySQLProjectedBoolean(column) {
			continue
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			mySQLIdentifier(column.Name)+" IN (0, 1)",
			projected.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"plan MySQL boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{
				Name: mySQLBooleanCheckName(
					source,
					column.Name,
				),
				Expression: expression,
			},
		)
	}
	return nil
}

func sqliteMySQLCheckGuardColumns(
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

// validateSQLiteMySQLTables validates and canonicalizes the projected
// selection before target-side foreign-key support indexes are added.
func validateSQLiteMySQLTables(
	source []schema.Table,
	projected []schema.Table,
) ([]schema.Table, error) {
	if len(source) != len(projected) {
		return nil, sqliteMySQLPolicy(
			"map SQLite table selection",
			"source and target table counts differ",
		)
	}
	for index := range source {
		if source[index].Schema != "" ||
			source[index].Name != projected[index].Name {
			return nil, sqliteMySQLPolicy(
				"map SQLite table selection",
				source[index].Name,
			)
		}
	}
	if err := materializeSQLiteMySQLForeignKeyColumns(
		projected,
	); err != nil {
		return nil, err
	}
	materialized, err := schema.MaterializeMySQLObjectNames(
		projected,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"plan SQLite to MySQL relational objects: %w",
			err,
		)
	}
	return materialized, nil
}

func materializeSQLiteMySQLForeignKeyColumns(
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
					return sqliteMySQLPolicy(
						"map SQLite foreign-key target",
						tables[tableIndex].Name+"."+
							foreignKey.ReferencedTable,
					)
				}
				referencedIndex = candidate
			}
			if referencedIndex < 0 {
				return sqliteMySQLPolicy(
					"map SQLite foreign-key target",
					tables[tableIndex].Name+"."+
						foreignKey.ReferencedTable,
				)
			}
			foreignKey.ReferencedTable =
				tables[referencedIndex].Name
			columns := primaryKeyColumns(tables[referencedIndex])
			if len(columns) == 0 ||
				len(columns) != len(foreignKey.Columns) {
				return sqliteMySQLPolicy(
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

func validateSQLiteMySQLForeignKeyAction(value string) error {
	switch strings.ToUpper(strings.Join(strings.Fields(value), " ")) {
	case "", "NO ACTION", "RESTRICT", "CASCADE", "SET NULL":
		return nil
	default:
		return sqliteMySQLPolicy(
			"map SQLite foreign-key action",
			value,
		)
	}
}

func sqliteMySQLTextColumn(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	switch strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	)) {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob", "uuid":
		return true
	default:
		return false
	}
}

func sqliteMySQLColumnExists(
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

func sqliteMySQLPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}
