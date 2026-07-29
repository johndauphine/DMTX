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
		projected := sourceTable
		projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
		return projected, nil
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
	if sourceTable.Identity == nil {
		if column, ok := sqliteImplicitRowIDAlias(sourceTable); ok {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite implicit rowid identity",
				sourceTable.Name+"."+column,
			)
		}
	}

	projected := sourceTable
	projected.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	projected.Indexes = clonePostgresProjectionIndexes(sourceTable.Indexes)
	projected.ForeignKeys = clonePostgresProjectionForeignKeys(sourceTable.ForeignKeys)
	projected.Checks = append([]schema.CheckConstraint(nil), sourceTable.Checks...)
	projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
	for index, sourceColumn := range sourceTable.Columns {
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
		mapped, ok := postgresSQLiteType(base)
		if !ok {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type",
				base,
			)
		}
		projected.Columns[index].Type = mapped
		declaration, err := projectSQLiteDeclaredTypeForPostgres(
			*sourceColumn.DeclaredType,
		)
		if err != nil {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type modifier",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		projected.Columns[index].DeclaredType = declaration
	}
	return projected, nil
}

// projectSQLiteDeclaredTypeForPostgres retains only modifiers whose source
// values can be checked exactly before COPY. SQLite ignores character and
// decimal modifiers, so the writer must reject values that PostgreSQL would
// otherwise truncate or round. CHAR-family declarations use VARCHAR to avoid
// PostgreSQL's trailing-space padding while preserving the declared limit.
func projectSQLiteDeclaredTypeForPostgres(
	value schema.DeclaredType,
) (*schema.DeclaredType, error) {
	if len(value.Arguments) == 0 {
		return nil, nil
	}
	base := strings.ToLower(strings.TrimSpace(value.Base))
	arguments := append([]int(nil), value.Arguments...)
	switch base {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar":
		if len(arguments) != 1 ||
			arguments[0] <= 0 ||
			arguments[0] > 10_485_760 {
			return nil, fmt.Errorf("invalid character length")
		}
		return &schema.DeclaredType{
			Base:      "varchar",
			Arguments: arguments,
		}, nil
	case "numeric", "decimal":
		if len(arguments) < 1 || len(arguments) > 2 ||
			arguments[0] < 1 || arguments[0] > 1000 {
			return nil, fmt.Errorf("invalid numeric precision")
		}
		scale := 0
		if len(arguments) == 2 {
			scale = arguments[1]
		}
		if scale < 0 || scale > arguments[0] {
			return nil, fmt.Errorf("invalid numeric scale")
		}
		return &schema.DeclaredType{
			Base:      "numeric",
			Arguments: arguments,
		}, nil
	case "datetime", "timestamp":
		if len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return nil, fmt.Errorf(
				"invalid temporal precision",
			)
		}
		return &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: arguments,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported modifier")
	}
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
