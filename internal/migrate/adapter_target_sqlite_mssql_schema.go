package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectSQLServerTableForSQLite maps only SQL Server 2022 shapes whose
// stored values and relational behavior have a conservative SQLite
// representation. SQLite has no exact fixed-point storage class, so
// DECIMAL/NUMERIC is admitted only when every value is an integer that fits
// in SQLite's signed 64-bit INTEGER storage.
func projectSQLServerTableForSQLite(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID ||
		strings.TrimSpace(source.MySQLCollation) != "" {
		return schema.Table{}, sqliteSQLServerProjectionPolicy(
			"map SQL Server table metadata",
			source.Name,
		)
	}
	projected := cloneSQLiteTargetTable(source)
	projected.Schema = ""
	for index, column := range source.Columns {
		target, err := projectSQLServerColumnForSQLite(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQL Server column %s.%s to SQLite: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		if column.Name == "" {
			return schema.Table{}, sqliteSQLServerProjectionPolicy(
				"map SQL Server column",
				source.Name,
			)
		}
		if _, exists := sourceColumns[column.Name]; exists {
			return schema.Table{}, sqliteSQLServerProjectionPolicy(
				"map SQL Server columns",
				source.Name+"."+column.Name,
			)
		}
		sourceColumns[column.Name] = column
		if (column.PrimaryKey || column.PrimaryKeyPosition > 0) &&
			!sqlServerSQLiteExactComparison(column) {
			return schema.Table{}, sqliteSQLServerProjectionPolicy(
				"map SQL Server primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
	}

	for _, index := range source.Indexes {
		if index.Inline || index.Name == "" || len(index.Columns) == 0 {
			return schema.Table{}, sqliteSQLServerProjectionPolicy(
				"map SQL Server index shape",
				source.Name+"."+index.Name,
			)
		}
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists ||
				strings.TrimSpace(indexed.Collation) != "" ||
				!sqlServerSQLiteExactComparison(column) {
				return schema.Table{}, sqliteSQLServerProjectionPolicy(
					"map SQL Server index comparison",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, sqliteSQLServerProjectionPolicy(
					"map SQL Server nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, sqliteSQLServerProjectionPolicy(
				"map SQL Server foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			if strings.EqualFold(strings.TrimSpace(action), "SET DEFAULT") {
				return schema.Table{}, sqliteSQLServerProjectionPolicy(
					"map SQL Server foreign-key action",
					source.Name+"."+foreignKey.Name,
				)
			}
		}
		for _, name := range foreignKey.Columns {
			column, exists := sourceColumns[name]
			if !exists || !sqlServerSQLiteExactReference(column) {
				return schema.Table{}, sqliteSQLServerProjectionPolicy(
					"map SQL Server foreign-key comparison",
					source.Name+"."+foreignKey.Name+"."+name,
				)
			}
		}
	}

	for _, check := range source.Checks {
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQL Server CHECK %s.%s to SQLite: %w",
				source.Name,
				check.Name,
				err,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists || !sqlServerSQLiteExactCheck(column) {
				return schema.Table{}, sqliteSQLServerProjectionPolicy(
					"map SQL Server CHECK comparison",
					source.Name+"."+check.Name+"."+name,
				)
			}
		}
	}

	for _, column := range source.Columns {
		if !sqlServerSQLiteBoolean(column) {
			continue
		}
		expression, err := schema.ParseSQLiteCheckExpression(
			sqliteSQLServerIdentifier(column.Name) + " IN (0, 1)",
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"plan SQLite boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{
				Expression: expression,
			},
		)
	}
	for index := range projected.ForeignKeys {
		projected.ForeignKeys[index].Name = ""
	}
	for index := range projected.Checks {
		projected.Checks[index].Name = ""
	}
	return projected, nil
}

func cloneSQLiteTargetTable(source schema.Table) schema.Table {
	cloned := source
	cloned.Identity = cloneSchemaIdentity(source.Identity)
	cloned.Columns = append([]schema.Column(nil), source.Columns...)
	for index := range cloned.Columns {
		if source.Columns[index].DeclaredType != nil {
			declaration := *source.Columns[index].DeclaredType
			declaration.Arguments = append(
				[]int(nil),
				source.Columns[index].DeclaredType.Arguments...,
			)
			cloned.Columns[index].DeclaredType = &declaration
		}
		cloned.Columns[index].Default = cloneSchemaExpression(
			source.Columns[index].Default,
		)
	}
	cloned.Indexes = clonePostgresProjectionIndexes(source.Indexes)
	cloned.ForeignKeys = clonePostgresProjectionForeignKeys(
		source.ForeignKeys,
	)
	cloned.Checks = append([]schema.CheckConstraint(nil), source.Checks...)
	return cloned
}

func validateSQLServerSQLiteTables(
	sourceTables []schema.Table,
	targetTables []schema.Table,
) error {
	if len(sourceTables) != len(targetTables) {
		return sqliteSQLServerProjectionPolicy(
			"map SQL Server table set",
			"source and target table counts differ",
		)
	}
	sourceByName := make(map[string]schema.Table, len(sourceTables))
	objectNames := make(map[string]string)
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		key := strings.ToLower(targetTable.Name)
		if earlier, exists := objectNames[key]; exists {
			return sqliteSQLServerProjectionPolicy(
				"map SQLite object names",
				earlier+" and table "+targetTable.Name,
			)
		}
		objectNames[key] = "table " + targetTable.Name
		sourceByName[sourceTable.Name] = sourceTable
		for _, index := range targetTable.Indexes {
			if index.Inline {
				continue
			}
			indexKey := strings.ToLower(index.Name)
			if earlier, exists := objectNames[indexKey]; exists {
				return sqliteSQLServerProjectionPolicy(
					"map SQLite object names",
					earlier+" and index "+index.Name,
				)
			}
			objectNames[indexKey] = "index " + index.Name
		}
	}

	for _, table := range sourceTables {
		localColumns := make(map[string]schema.Column, len(table.Columns))
		for _, column := range table.Columns {
			localColumns[column.Name] = column
		}
		for _, foreignKey := range table.ForeignKeys {
			referencedTable, exists := sourceByName[foreignKey.ReferencedTable]
			if !exists {
				return sqliteSQLServerProjectionPolicy(
					"map SQL Server foreign key",
					table.Name+"."+foreignKey.Name+
						" references an unselected table",
				)
			}
			if len(foreignKey.Columns) == 0 ||
				len(foreignKey.Columns) !=
					len(foreignKey.ReferencedColumns) {
				return sqliteSQLServerProjectionPolicy(
					"map SQL Server foreign key",
					table.Name+"."+foreignKey.Name,
				)
			}
			referencedColumns := make(
				map[string]schema.Column,
				len(referencedTable.Columns),
			)
			for _, column := range referencedTable.Columns {
				referencedColumns[column.Name] = column
			}
			for pairIndex, localName := range foreignKey.Columns {
				local, localExists := localColumns[localName]
				referencedName := foreignKey.ReferencedColumns[pairIndex]
				referenced, referencedExists := referencedColumns[referencedName]
				if !localExists || !referencedExists ||
					!sqlServerSQLiteExactReference(local) ||
					!sqlServerSQLiteExactReference(referenced) {
					return sqliteSQLServerProjectionPolicy(
						"map SQL Server foreign-key comparison",
						table.Name+"."+foreignKey.Name+
							"."+localName,
					)
				}
			}
		}
	}
	return nil
}

func projectSQLServerColumnForSQLite(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqliteSQLServerProjectionPolicy(
			"map SQL Server declared type",
			source.Name,
		)
	}
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	base := strings.ToLower(strings.Join(
		strings.Fields(source.DeclaredType.Base),
		" ",
	))
	arguments := append(
		[]int(nil),
		source.DeclaredType.Arguments...,
	)
	sourceType := strings.ToLower(strings.Join(
		strings.Fields(source.Type),
		" ",
	))
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
	}
	noArguments := func() bool { return len(arguments) == 0 }
	fail := func(operation string) (schema.Column, error) {
		return schema.Column{}, sqliteSQLServerProjectionPolicy(
			operation,
			source.Name+"."+base,
		)
	}

	switch base {
	case "tinyint", "smallint", "int":
		if sourceType != "integer" || !noArguments() {
			return fail("map SQL Server type")
		}
		target.Type = "integer"
		declaration(base)
	case "bigint":
		if sourceType != "bigint" || !noArguments() {
			return fail("map SQL Server type")
		}
		target.Type = "bigint"
		declaration("bigint")
	case "bool":
		if sourceType != "boolean" || !noArguments() {
			return fail("map SQL Server type")
		}
		target.Type = "boolean"
		declaration("boolean")
	case "decimal", "numeric":
		if sourceType != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 18 ||
			arguments[1] != 0 {
			return fail("map SQL Server exact numeric")
		}
		target.Type = "bigint"
		declaration("bigint")
	case "real":
		if sourceType != "real" || !noArguments() ||
			source.Default != nil {
			return fail("map SQL Server floating-point type")
		}
		target.Type = "real"
		declaration("real")
	case "double precision":
		if sourceType != "double precision" || !noArguments() ||
			source.Default != nil {
			return fail("map SQL Server floating-point type")
		}
		target.Type = "double precision"
		declaration("double precision")
	case "char", "varchar":
		if sourceType != "text" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			return fail("map SQL Server text type")
		}
		target.Type = "text"
		declaration(base, arguments[0])
	case "text":
		if sourceType != "text" || !noArguments() {
			return fail("map SQL Server text type")
		}
		target.Type = "text"
		declaration("text")
	case "binary", "varbinary":
		if sourceType != "blob" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			return fail("map SQL Server binary type")
		}
		target.Type = "blob"
		declaration("blob")
	case "blob":
		if sourceType != "blob" || !noArguments() {
			return fail("map SQL Server binary type")
		}
		target.Type = "blob"
		declaration("blob")
	case "date":
		if sourceType != "date" || !noArguments() {
			return fail("map SQL Server temporal type")
		}
		target.Type = "date"
		declaration("date")
	case "time":
		if sourceType != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return fail("map SQL Server temporal type")
		}
		target.Type = "time"
		declaration("time", arguments[0])
	case "timestamp":
		if sourceType != "datetime" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return fail("map SQL Server temporal type")
		}
		target.Type = "datetime"
		declaration("timestamp", arguments[0])
	case "smalldatetime":
		if sourceType != "datetime" || !noArguments() {
			return fail("map SQL Server temporal type")
		}
		target.Type = "datetime"
		declaration("datetime")
	case "uuid":
		if sourceType != "uuid" || !noArguments() ||
			source.Default != nil {
			return fail("map SQL Server UUID type")
		}
		target.Type = "uuid"
		declaration("uuid")
	default:
		return schema.Column{}, sqliteSQLServerProjectionPolicy(
			"map SQL Server type",
			base,
		)
	}
	return target, nil
}

func sqlServerSQLiteExactComparison(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) {
	case "tinyint", "smallint", "int", "bigint", "bool":
		return len(column.DeclaredType.Arguments) == 0
	case "decimal", "numeric":
		arguments := column.DeclaredType.Arguments
		return len(arguments) == 2 &&
			arguments[0] >= 1 &&
			arguments[0] <= 18 &&
			arguments[1] == 0
	case "date", "smalldatetime":
		return len(column.DeclaredType.Arguments) == 0
	case "time", "timestamp":
		arguments := column.DeclaredType.Arguments
		return len(arguments) == 1 &&
			arguments[0] >= 0 &&
			arguments[0] <= 6
	default:
		return false
	}
}

func sqlServerSQLiteExactReference(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) {
	case "tinyint", "smallint", "int", "bigint", "bool":
		return len(column.DeclaredType.Arguments) == 0
	case "decimal", "numeric":
		arguments := column.DeclaredType.Arguments
		return len(arguments) == 2 &&
			arguments[0] >= 1 &&
			arguments[0] <= 18 &&
			arguments[1] == 0
	default:
		return false
	}
}

func sqlServerSQLiteExactCheck(column schema.Column) bool {
	return sqlServerSQLiteExactReference(column)
}

func sqlServerSQLiteBoolean(column schema.Column) bool {
	return column.DeclaredType != nil &&
		strings.EqualFold(
			strings.TrimSpace(column.DeclaredType.Base),
			"bool",
		) &&
		len(column.DeclaredType.Arguments) == 0
}

func sqliteSQLServerIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func sqliteSQLServerProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLite),
	}
}
