package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

func projectPostgresSourceTable(
	sourceEngine string,
	sourceTable schema.Table,
) (schema.Table, error) {
	switch sourceEngine {
	case "postgres", "mysql", "mssql":
		return sourceTable, nil
	case "sqlite":
		return projectSQLiteTableForPostgres(sourceTable)
	default:
		return schema.Table{}, fmt.Errorf(
			"PostgreSQL target does not support source engine %q",
			sourceEngine,
		)
	}
}

func projectSQLiteTableForPostgres(
	sourceTable schema.Table,
) (schema.Table, error) {
	if sourceTable.Schema != "" {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite schema namespace",
			sourceTable.Schema,
		)
	}
	if sourceTable.AutoIncrementColumn != "" ||
		sourceTable.SQLiteSequence != nil {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite AUTOINCREMENT",
			sourceTable.Name,
		)
	}
	if sourceTable.SQLiteStrict {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite STRICT table",
			sourceTable.Name,
		)
	}
	if sourceTable.SQLiteWithoutRowID {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite WITHOUT ROWID table",
			sourceTable.Name,
		)
	}
	if column, ok := sqliteImplicitRowIDAlias(sourceTable); ok {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite implicit rowid identity",
			sourceTable.Name+"."+column,
		)
	}
	if len(sourceTable.Indexes) > 0 {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite indexes",
			sourceTable.Name,
		)
	}
	if len(sourceTable.ForeignKeys) > 0 {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite foreign keys",
			sourceTable.Name,
		)
	}
	if len(sourceTable.Checks) > 0 {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite checks",
			sourceTable.Name,
		)
	}

	projected := sourceTable
	projected.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	for index, sourceColumn := range sourceTable.Columns {
		if sourceColumn.Default != nil {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite default",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		if sourceColumn.DeclaredType == nil {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite declared type",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		base := strings.ToLower(strings.TrimSpace(
			sourceColumn.DeclaredType.Base,
		))
		if base == "" ||
			base != strings.ToLower(strings.TrimSpace(sourceColumn.Type)) {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite declared type",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		if len(sourceColumn.DeclaredType.Arguments) != 0 {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type modifier",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		mapped, ok := postgresSQLiteType(base)
		if !ok {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type",
				base,
			)
		}
		projected.Columns[index].Type = mapped
		projected.Columns[index].DeclaredType = nil
	}
	return projected, nil
}

func sqliteImplicitRowIDAlias(table schema.Table) (string, bool) {
	keyIndex := -1
	for index, column := range table.Columns {
		if !column.PrimaryKey && column.PrimaryKeyPosition == 0 {
			continue
		}
		if keyIndex >= 0 {
			return "", false
		}
		keyIndex = index
	}
	if keyIndex < 0 {
		return "", false
	}
	key := table.Columns[keyIndex]
	if key.DeclaredType == nil ||
		len(key.DeclaredType.Arguments) != 0 {
		return "", false
	}
	if strings.ToLower(strings.TrimSpace(key.DeclaredType.Base)) !=
		"integer" {
		return "", false
	}
	return key.Name, true
}

func postgresSQLiteType(source string) (string, bool) {
	switch source {
	case "int", "integer", "tinyint", "smallint", "mediumint",
		"bigint", "int2", "int8":
		return "bigint", true
	case "real", "double", "double precision", "float":
		return "double precision", true
	case "numeric", "decimal":
		return "numeric", true
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		return "text", true
	case "blob", "binary", "varbinary":
		return "bytea", true
	case "bool", "boolean":
		return "boolean", true
	case "date":
		return "date", true
	case "datetime", "timestamp":
		return "timestamp", true
	case "json":
		return "json", true
	case "uuid":
		return "uuid", true
	default:
		return "", false
	}
}

func postgresSQLitePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}
