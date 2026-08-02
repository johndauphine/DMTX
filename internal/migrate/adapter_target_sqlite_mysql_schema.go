package migrate

import (
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectMySQLTableForSQLite maps the shared, version-pinned Oracle MySQL
// 8.0/MariaDB 10.11 discovery shape to SQLite. It deliberately admits only
// values and relational comparisons whose representation can be proven exact.
// In particular, TINYINT(1) remains an integer: MySQL permits values outside
// the boolean domain and the display-width modifier cannot justify inventing
// a target CHECK constraint.
func projectMySQLTableForSQLite(
	source schema.Table,
) (schema.Table, error) {
	if strings.TrimSpace(source.Schema) == "" ||
		!validMySQLSQLiteIdentifier(source.Name) ||
		source.SQLiteStrict ||
		source.SQLiteWithoutRowID ||
		len(source.ClickHouseOrderBy) != 0 {
		return schema.Table{}, sqliteMySQLProjectionPolicy(
			"map MySQL table metadata",
			source.Name,
		)
	}
	if strings.HasPrefix(strings.ToLower(source.Name), "sqlite_") {
		return schema.Table{}, sqliteMySQLProjectionPolicy(
			"map reserved SQLite object name",
			source.Name,
		)
	}
	switch strings.ToLower(strings.TrimSpace(source.MySQLCollation)) {
	case "utf8mb4_0900_bin", "utf8mb4_nopad_bin":
	default:
		return schema.Table{}, sqliteMySQLProjectionPolicy(
			"map MySQL collation",
			source.MySQLCollation,
		)
	}

	projected := cloneSQLiteTargetTable(source)
	projected.Schema = ""
	projected.MySQLCollation = ""
	projected.ClickHouseOrderBy = nil

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	foldedColumns := make(map[string]string, len(source.Columns))
	primaryPositions := make(map[int]string)
	for index, column := range source.Columns {
		if !validMySQLSQLiteIdentifier(column.Name) {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL column name",
				source.Name+"."+column.Name,
			)
		}
		folded := strings.ToLower(column.Name)
		if earlier, duplicate := foldedColumns[folded]; duplicate {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map case-insensitive SQLite column names",
				source.Name+"."+earlier+" and "+column.Name,
			)
		}
		foldedColumns[folded] = column.Name
		sourceColumns[column.Name] = column

		hasPrimaryPosition := column.PrimaryKeyPosition > 0
		if column.PrimaryKey != hasPrimaryPosition {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL primary-key shape",
				source.Name+"."+column.Name,
			)
		}
		if hasPrimaryPosition {
			if column.Nullable {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL primary-key nullability",
					source.Name+"."+column.Name,
				)
			}
			if earlier, duplicate :=
				primaryPositions[column.PrimaryKeyPosition]; duplicate {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL primary-key order",
					source.Name+"."+earlier+" and "+column.Name,
				)
			}
			primaryPositions[column.PrimaryKeyPosition] = column.Name
		}

		target, err := projectMySQLColumnForSQLite(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map MySQL column %s.%s to SQLite: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		if hasPrimaryPosition &&
			mySQLSQLiteComparisonKind(column) == "" {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
		projected.Columns[index] = target
	}
	for position := 1; position <= len(primaryPositions); position++ {
		if _, exists := primaryPositions[position]; !exists {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL primary-key order",
				source.Name,
			)
		}
	}

	indexNames := make(map[string]string, len(source.Indexes))
	for _, sourceIndex := range source.Indexes {
		if sourceIndex.Inline ||
			!validMySQLSQLiteIdentifier(sourceIndex.Name) ||
			len(sourceIndex.Columns) == 0 {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL index shape",
				source.Name+"."+sourceIndex.Name,
			)
		}
		if strings.HasPrefix(
			strings.ToLower(sourceIndex.Name),
			"sqlite_",
		) {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map reserved SQLite object name",
				sourceIndex.Name,
			)
		}
		folded := strings.ToLower(sourceIndex.Name)
		if earlier, duplicate := indexNames[folded]; duplicate {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map case-insensitive SQLite index names",
				source.Name+"."+earlier+" and "+sourceIndex.Name,
			)
		}
		indexNames[folded] = sourceIndex.Name

		seenColumns := make(map[string]struct{}, len(sourceIndex.Columns))
		for _, indexed := range sourceIndex.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists ||
				mySQLSQLiteComparisonKind(column) == "" {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL index comparison",
					source.Name+"."+sourceIndex.Name+"."+
						indexed.Name,
				)
			}
			if _, duplicate := seenColumns[indexed.Name]; duplicate {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL index columns",
					source.Name+"."+sourceIndex.Name+"."+
						indexed.Name,
				)
			}
			seenColumns[indexed.Name] = struct{}{}
			collation := strings.ToUpper(strings.TrimSpace(
				indexed.Collation,
			))
			if mySQLSQLiteTextColumn(column) {
				if collation != "BINARY" {
					return schema.Table{},
						sqliteMySQLProjectionPolicy(
							"map MySQL text index collation",
							source.Name+"."+sourceIndex.Name+
								"."+indexed.Name,
						)
				}
			} else if collation != "" {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL index collation",
					source.Name+"."+sourceIndex.Name+"."+
						indexed.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		sourceForeignKey := source.ForeignKeys[index]
		foreignKey := &projected.ForeignKeys[index]
		if !validMySQLSQLiteIdentifier(sourceForeignKey.Name) ||
			!validMySQLSQLiteIdentifier(
				sourceForeignKey.ReferencedTable,
			) ||
			len(sourceForeignKey.Columns) == 0 ||
			len(sourceForeignKey.Columns) !=
				len(sourceForeignKey.ReferencedColumns) {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL foreign-key shape",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		// SQLite has a single namespace: a reference inside the migrated schema
		// is expressible only unqualified, and one that escapes it names a
		// relation this migration does not carry. See the PostgreSQL projection
		// for the same rule.
		switch sourceForeignKey.ReferencedSchema {
		case "", source.Schema:
			foreignKey.ReferencedSchema = ""
		default:
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL cross-schema foreign key",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		if strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.Match,
		)) != "NONE" {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL foreign-key match",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		foreignKey.Match = "NONE"
		foreignKey.OnUpdate = strings.ToUpper(strings.Join(
			strings.Fields(sourceForeignKey.OnUpdate),
			" ",
		))
		foreignKey.OnDelete = strings.ToUpper(strings.Join(
			strings.Fields(sourceForeignKey.OnDelete),
			" ",
		))
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			switch action {
			case "NO ACTION", "RESTRICT", "CASCADE", "SET NULL":
			default:
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key action",
					source.Name+"."+sourceForeignKey.Name,
				)
			}
		}

		seenLocal := make(map[string]struct{}, len(sourceForeignKey.Columns))
		seenReferenced := make(
			map[string]struct{},
			len(sourceForeignKey.ReferencedColumns),
		)
		for pairIndex, localName := range sourceForeignKey.Columns {
			referencedName :=
				sourceForeignKey.ReferencedColumns[pairIndex]
			local, exists := sourceColumns[localName]
			if !exists ||
				!validMySQLSQLiteIdentifier(referencedName) ||
				mySQLSQLiteReferenceKind(local) == "" {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key comparison",
					source.Name+"."+sourceForeignKey.Name+"."+
						localName,
				)
			}
			if _, duplicate := seenLocal[localName]; duplicate {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key columns",
					source.Name+"."+sourceForeignKey.Name+"."+
						localName,
				)
			}
			if _, duplicate :=
				seenReferenced[referencedName]; duplicate {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key referenced columns",
					source.Name+"."+sourceForeignKey.Name+"."+
						referencedName,
				)
			}
			seenLocal[localName] = struct{}{}
			seenReferenced[referencedName] = struct{}{}
			if (foreignKey.OnUpdate == "SET NULL" ||
				foreignKey.OnDelete == "SET NULL") &&
				!local.Nullable {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL SET NULL foreign key",
					source.Name+"."+sourceForeignKey.Name,
				)
			}
		}
		// SQLite discovery cannot retain table-constraint names. Preserve the
		// behavior and ordered columns, but make the name loss explicit.
		foreignKey.Name = ""
	}

	projected.Checks = make(
		[]schema.CheckConstraint,
		0,
		len(source.Checks),
	)
	checkNames := make(map[string]string, len(source.Checks))
	for _, sourceCheck := range source.Checks {
		if !validMySQLSQLiteIdentifier(sourceCheck.Name) {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL CHECK shape",
				source.Name+"."+sourceCheck.Name,
			)
		}
		folded := strings.ToLower(sourceCheck.Name)
		if earlier, duplicate := checkNames[folded]; duplicate {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map case-insensitive MySQL CHECK names",
				source.Name+"."+earlier+" and "+sourceCheck.Name,
			)
		}
		checkNames[folded] = sourceCheck.Name
		referenced, err := schema.ReferencedCheckColumns(
			sourceCheck.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map MySQL CHECK %s.%s to SQLite: %w",
				source.Name,
				sourceCheck.Name,
				err,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists || mySQLSQLiteCheckKind(column) == "" {
				return schema.Table{}, sqliteMySQLProjectionPolicy(
					"map MySQL CHECK comparison",
					source.Name+"."+sourceCheck.Name+"."+name,
				)
			}
		}
		// Re-validate the portable expression against the projected column
		// declarations. In particular, DECIMAL(p,0) becomes SQLite INTEGER;
		// this rejects fractional or out-of-range literals that MySQL compares
		// exactly but SQLite would parse through binary64 REAL.
		if _, err := schema.RenderSQLiteCheckForPostgres(
			sourceCheck.Expression,
			projected.Columns,
		); err != nil {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL CHECK projected comparison",
				source.Name+"."+sourceCheck.Name,
			)
		}
		expression, err := schema.ParseSQLiteCheckExpression(
			sourceCheck.Expression.CanonicalSQL(),
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map MySQL CHECK %s.%s to SQLite: %w",
				source.Name,
				sourceCheck.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{Expression: expression},
		)
	}

	if projected.Identity != nil {
		identity := projected.Identity
		column, exists := sourceColumns[identity.Column]
		if !exists ||
			normalizedMySQLSQLiteBase(column) != "bigint" ||
			column.Type != "bigint" ||
			column.DeclaredType == nil ||
			len(column.DeclaredType.Arguments) != 0 ||
			column.Nullable ||
			column.Default != nil ||
			column.PrimaryKeyPosition != 1 ||
			len(primaryPositions) != 1 ||
			identity.Generation != schema.IdentityByDefault ||
			identity.Frontier == nil ||
			*identity.Frontier < 0 {
			return schema.Table{}, sqliteMySQLProjectionPolicy(
				"map MySQL identity",
				source.Name,
			)
		}
	}

	if _, err := schema.CreateTable(schema.SQLite, projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite table %s: %w",
			source.Name,
			err,
		)
	}
	if _, err := schema.CreateIndexes(schema.SQLite, projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite indexes for %s: %w",
			source.Name,
			err,
		)
	}
	if _, err := schema.SQLiteSequencePlan(projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite identity for %s: %w",
			source.Name,
			err,
		)
	}
	return projected, nil
}

func projectMySQLColumnForSQLite(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqliteMySQLProjectionPolicy(
			"map MySQL declared type",
			source.Name,
		)
	}
	target := source
	target.Default = nil
	base := normalizedMySQLSQLiteBase(source)
	sourceType := strings.ToLower(strings.Join(
		strings.Fields(source.Type),
		" ",
	))
	arguments := append(
		[]int(nil),
		source.DeclaredType.Arguments...,
	)
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
	}
	fail := func(operation string) (schema.Column, error) {
		return schema.Column{}, sqliteMySQLProjectionPolicy(
			operation,
			source.Name+"."+base,
		)
	}
	noArguments := len(arguments) == 0

	switch base {
	case "tinyint":
		if sourceType != "integer" ||
			len(arguments) > 1 ||
			len(arguments) == 1 && arguments[0] != 1 {
			return fail("map MySQL integer type")
		}
		target.Type = "integer"
		declaration("tinyint")
	case "smallint":
		if sourceType != "integer" || !noArguments {
			return fail("map MySQL integer type")
		}
		target.Type = "integer"
		declaration("smallint")
	case "mediumint":
		if sourceType != "integer" || !noArguments {
			return fail("map MySQL integer type")
		}
		target.Type = "integer"
		declaration("mediumint")
	case "int":
		if sourceType != "integer" || !noArguments {
			return fail("map MySQL integer type")
		}
		target.Type = "integer"
		declaration("int")
	case "bigint":
		if sourceType != "bigint" || !noArguments {
			return fail("map MySQL integer type")
		}
		target.Type = "bigint"
		declaration("bigint")
	case "decimal":
		if sourceType != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 18 ||
			arguments[1] != 0 {
			return fail("map MySQL exact decimal type")
		}
		target.Type = "numeric"
		declaration("bigint")
	case "varchar":
		if sourceType != "varchar" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 16_383 {
			return fail("map MySQL varying text type")
		}
		target.Type = "varchar"
		declaration("varchar", arguments[0])
	case "tinytext", "text", "mediumtext", "longtext":
		if sourceType != "text" || !noArguments {
			return fail("map MySQL text type")
		}
		target.Type = "text"
		declaration("text")
	case "binary":
		if sourceType != "binary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 255 {
			return fail("map MySQL fixed binary type")
		}
		target.Type = "binary"
		declaration("binary", arguments[0])
	case "varbinary":
		if sourceType != "varbinary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 65_535 {
			return fail("map MySQL varying binary type")
		}
		target.Type = "varbinary"
		declaration("varbinary", arguments[0])
	case "tinyblob", "blob", "mediumblob", "longblob":
		if sourceType != "blob" || !noArguments {
			return fail("map MySQL binary large-object type")
		}
		target.Type = "blob"
		declaration("blob")
	case "date":
		if sourceType != "date" || !noArguments {
			return fail("map MySQL temporal type")
		}
		target.Type = "date"
		declaration("date")
	case "time", "datetime", "timestamp":
		if sourceType != base ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return fail("map MySQL temporal type")
		}
		target.Type = base
		declaration(base, arguments[0])
	case "char":
		return fail("map MySQL fixed-width blank-padding type")
	case "double":
		return fail("map MySQL floating-point type")
	case "json":
		return fail("map MySQL JSON type")
	default:
		return fail("map MySQL type")
	}

	projectedDefault, err := projectMySQLDefaultForSQLite(source)
	if err != nil {
		return schema.Column{}, err
	}
	target.Default = projectedDefault
	return target, nil
}

// projectMySQLDefaultForSQLite consumes only the canonical structured
// expression produced by source discovery, then asks SQLite's conservative
// parser to reconstruct target SQL. It never copies catalog SQL.
func projectMySQLDefaultForSQLite(
	source schema.Column,
) (*schema.Expression, error) {
	if source.Default == nil {
		return nil, nil
	}
	base := normalizedMySQLSQLiteBase(source)
	if base == "binary" || base == "varbinary" {
		// MySQL and MariaDB expose these defaults through different
		// connection-character-set paths, and SQLite would not reproduce
		// BINARY's source-side padding.
		return nil, sqliteMySQLProjectionPolicy(
			"map MySQL binary default",
			source.Name,
		)
	}

	canonical := strings.TrimSpace(source.Default.CanonicalSQL())
	if canonical == "" {
		return nil, sqliteMySQLProjectionPolicy(
			"map MySQL default",
			source.Name,
		)
	}
	if (base == "time" ||
		base == "datetime" ||
		base == "timestamp") &&
		source.DeclaredType != nil &&
		len(source.DeclaredType.Arguments) == 1 &&
		source.DeclaredType.Arguments[0] > 0 &&
		mySQLSQLiteCurrentTemporalDefault(canonical) {
		// SQLite's CURRENT_TIME/CURRENT_TIMESTAMP values have whole-second
		// precision and cannot satisfy a fractional MySQL default.
		return nil, sqliteMySQLProjectionPolicy(
			"map MySQL fractional temporal default",
			source.Name,
		)
	}
	if base == "decimal" &&
		!mySQLSQLiteIntegralDefaultFits(canonical) {
		return nil, sqliteMySQLProjectionPolicy(
			"map MySQL exact decimal default",
			source.Name,
		)
	}

	expression, err := schema.ParseSQLiteDefault(canonical)
	if err != nil {
		return nil, fmt.Errorf(
			"map MySQL default for %s to SQLite: %w",
			source.Name,
			err,
		)
	}
	return expression, nil
}

func mySQLSQLiteCurrentTemporalDefault(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "CURRENT_TIME", "CURRENT_TIMESTAMP":
		return true
	default:
		return false
	}
}

func mySQLSQLiteIntegralDefaultFits(value string) bool {
	canonical := strings.TrimSpace(value)
	rational, ok := new(big.Rat).SetString(canonical)
	if !ok || !rational.IsInt() {
		return false
	}
	return rational.Num().IsInt64() &&
		canonical == rational.Num().String()
}

func normalizedMySQLSQLiteBase(column schema.Column) string {
	if column.DeclaredType == nil {
		return ""
	}
	return strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
}

func mySQLSQLiteTextColumn(column schema.Column) bool {
	switch normalizedMySQLSQLiteBase(column) {
	case "varchar", "tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func mySQLSQLiteComparisonKind(column schema.Column) string {
	base := normalizedMySQLSQLiteBase(column)
	if column.DeclaredType == nil {
		return ""
	}
	arguments := column.DeclaredType.Arguments
	switch base {
	case "tinyint":
		if len(arguments) == 0 ||
			len(arguments) == 1 && arguments[0] == 1 {
			return "integer:tinyint"
		}
	case "smallint", "mediumint", "int", "bigint":
		if len(arguments) == 0 {
			return "integer:" + base
		}
	case "decimal":
		if len(arguments) == 2 &&
			arguments[0] >= 1 &&
			arguments[0] <= 18 &&
			arguments[1] == 0 {
			return fmt.Sprintf("decimal:%d:0", arguments[0])
		}
	case "varchar", "tinytext", "text", "mediumtext", "longtext":
		if base == "varchar" {
			if len(arguments) != 1 ||
				arguments[0] < 1 ||
				arguments[0] > 16_383 {
				return ""
			}
		} else if len(arguments) != 0 {
			return ""
		}
		return "text:binary"
	case "binary", "varbinary":
		if len(arguments) == 1 && arguments[0] > 0 {
			return fmt.Sprintf(
				"binary:%s:%d",
				base,
				arguments[0],
			)
		}
	case "tinyblob", "blob", "mediumblob", "longblob":
		if len(arguments) == 0 {
			return "blob"
		}
	case "date":
		if len(arguments) == 0 {
			return "date"
		}
	case "time", "datetime", "timestamp":
		if len(arguments) == 1 &&
			arguments[0] >= 0 &&
			arguments[0] <= 6 {
			return fmt.Sprintf(
				"temporal:%s:%d",
				base,
				arguments[0],
			)
		}
	}
	return ""
}

func mySQLSQLiteReferenceKind(column schema.Column) string {
	return mySQLSQLiteComparisonKind(column)
}

func mySQLSQLiteCheckKind(column schema.Column) string {
	return mySQLSQLiteComparisonKind(column)
}

func validMySQLSQLiteIdentifier(value string) bool {
	return value != "" &&
		utf8.ValidString(value) &&
		!strings.ContainsRune(value, '\x00')
}

// validateMySQLSQLiteTables closes whole-plan invariants that one table
// cannot prove: SQLite object names are database-global, every FK parent must
// be selected and written first, and the referenced key/type shape must remain
// an exact SQLite comparison.
func validateMySQLSQLiteTables(
	sourceTables []schema.Table,
	targetTables []schema.Table,
) error {
	if len(sourceTables) != len(targetTables) {
		return sqliteMySQLProjectionPolicy(
			"map MySQL table set",
			"source and target table counts differ",
		)
	}
	if len(sourceTables) == 0 {
		return nil
	}

	namespace := sourceTables[0].Schema
	sourceByName := make(map[string]schema.Table, len(sourceTables))
	sourcePositions := make(map[string]int, len(sourceTables))
	targetByName := make(map[string]schema.Table, len(targetTables))
	objectNames := make(map[string]string)
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		if namespace == "" ||
			sourceTable.Schema != namespace ||
			sourceTable.Name != targetTable.Name ||
			targetTable.Schema != "" ||
			targetTable.MySQLCollation != "" ||
			len(targetTable.ClickHouseOrderBy) != 0 {
			return sqliteMySQLProjectionPolicy(
				"map MySQL table set order",
				sourceTable.Name,
			)
		}
		if _, duplicate := sourceByName[sourceTable.Name]; duplicate {
			return sqliteMySQLProjectionPolicy(
				"map MySQL table set",
				"duplicate table "+sourceTable.Name,
			)
		}
		key := strings.ToLower(targetTable.Name)
		if earlier, collision := objectNames[key]; collision {
			return sqliteMySQLProjectionPolicy(
				"map SQLite global object names",
				earlier+" and table "+targetTable.Name,
			)
		}
		objectNames[key] = "table " + targetTable.Name
		sourceByName[sourceTable.Name] = sourceTable
		sourcePositions[sourceTable.Name] = index
		targetByName[targetTable.Name] = targetTable
	}
	for _, targetTable := range targetTables {
		for _, targetIndex := range targetTable.Indexes {
			if targetIndex.Inline {
				return sqliteMySQLProjectionPolicy(
					"map MySQL index lifecycle",
					targetTable.Name+"."+targetIndex.Name,
				)
			}
			key := strings.ToLower(targetIndex.Name)
			if earlier, collision := objectNames[key]; collision {
				return sqliteMySQLProjectionPolicy(
					"map SQLite global object names",
					earlier+" and index "+targetIndex.Name,
				)
			}
			objectNames[key] = "index " + targetIndex.Name
		}
	}

	for childPosition, child := range sourceTables {
		localColumns := mySQLSQLiteColumnMap(child)
		targetChild := targetByName[child.Name]
		if len(targetChild.ForeignKeys) != len(child.ForeignKeys) {
			return sqliteMySQLProjectionPolicy(
				"map MySQL foreign-key set",
				child.Name,
			)
		}
		for foreignKeyIndex, foreignKey := range child.ForeignKeys {
			parent, selected := sourceByName[foreignKey.ReferencedTable]
			parentPosition := sourcePositions[foreignKey.ReferencedTable]
			if !selected {
				return sqliteMySQLProjectionPolicy(
					"map MySQL foreign key",
					child.Name+"."+foreignKey.Name+
						" references an unselected table",
				)
			}
			if parentPosition >= childPosition {
				return sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key plan order",
					child.Name+"."+foreignKey.Name+
						" requires parent "+
						foreignKey.ReferencedTable+
						" before child",
				)
			}
			if len(foreignKey.Columns) == 0 ||
				len(foreignKey.Columns) !=
					len(foreignKey.ReferencedColumns) ||
				!mySQLSQLiteUniqueKeyMatches(
					parent,
					foreignKey.ReferencedColumns,
				) {
				return sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key parent key",
					child.Name+"."+foreignKey.Name,
				)
			}

			parentColumns := mySQLSQLiteColumnMap(parent)
			targetForeignKey :=
				targetChild.ForeignKeys[foreignKeyIndex]
			if targetForeignKey.Name != "" ||
				targetForeignKey.ReferencedTable !=
					foreignKey.ReferencedTable ||
				len(targetForeignKey.Columns) !=
					len(foreignKey.Columns) ||
				len(targetForeignKey.ReferencedColumns) !=
					len(foreignKey.ReferencedColumns) {
				return sqliteMySQLProjectionPolicy(
					"map MySQL foreign-key target shape",
					child.Name+"."+foreignKey.Name,
				)
			}
			for pairIndex, localName := range foreignKey.Columns {
				referencedName :=
					foreignKey.ReferencedColumns[pairIndex]
				local, localExists := localColumns[localName]
				referenced, referencedExists :=
					parentColumns[referencedName]
				localKind := mySQLSQLiteReferenceKind(local)
				referencedKind :=
					mySQLSQLiteReferenceKind(referenced)
				if !localExists ||
					!referencedExists ||
					localKind == "" ||
					localKind != referencedKind ||
					targetForeignKey.Columns[pairIndex] !=
						localName ||
					targetForeignKey.ReferencedColumns[pairIndex] !=
						referencedName {
					return sqliteMySQLProjectionPolicy(
						"map MySQL foreign-key comparison",
						child.Name+"."+foreignKey.Name+
							"."+localName,
					)
				}
			}
		}
	}
	return nil
}

func mySQLSQLiteColumnMap(
	table schema.Table,
) map[string]schema.Column {
	columns := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	return columns
}

func mySQLSQLiteUniqueKeyMatches(
	table schema.Table,
	names []string,
) bool {
	if len(names) == 0 {
		return false
	}
	primary := primaryKeyColumns(table)
	if equalStrings(primary, names) {
		return true
	}
	for _, index := range table.Indexes {
		if !index.Unique || len(index.Columns) != len(names) {
			continue
		}
		matches := true
		for position, name := range names {
			if index.Columns[position].Name != name {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func sqliteMySQLProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLite),
	}
}
