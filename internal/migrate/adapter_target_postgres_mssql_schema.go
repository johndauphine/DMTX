package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

func projectSQLServerTableForPostgres(
	sourceTable schema.Table,
) (schema.Table, error) {
	if sourceTable.SQLiteStrict || sourceTable.SQLiteWithoutRowID {
		return schema.Table{}, postgresSQLServerPolicy(
			"map SQL Server table metadata",
			sourceTable.Name,
		)
	}
	projected := sourceTable
	projected.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	projected.Indexes = clonePostgresProjectionIndexes(sourceTable.Indexes)
	projected.ForeignKeys = clonePostgresProjectionForeignKeys(
		sourceTable.ForeignKeys,
	)
	projected.Checks = append(
		[]schema.CheckConstraint(nil),
		sourceTable.Checks...,
	)
	projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
	for index, sourceColumn := range sourceTable.Columns {
		targetType, declaration, err :=
			projectSQLServerColumnForPostgres(sourceColumn)
		if err != nil {
			return schema.Table{}, postgresSQLServerPolicy(
				"map SQL Server type",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		projected.Columns[index].Type = targetType
		projected.Columns[index].DeclaredType = declaration
		projected.Columns[index].Default = cloneSchemaExpression(
			sourceColumn.Default,
		)
	}
	return projected, nil
}

func projectSQLServerColumnForPostgres(
	source schema.Column,
) (string, *schema.DeclaredType, error) {
	if source.DeclaredType == nil {
		return "", nil, fmt.Errorf("missing declared type")
	}
	base := strings.ToLower(strings.TrimSpace(source.DeclaredType.Base))
	arguments := source.DeclaredType.Arguments
	noArguments := func() bool { return len(arguments) == 0 }
	copyDeclaration := func(base string) *schema.DeclaredType {
		return &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		}
	}
	switch base {
	case "tinyint", "smallint", "int", "integer":
		if !noArguments() || source.Type != "integer" {
			break
		}
		return "integer", nil, nil
	case "bigint":
		if !noArguments() || source.Type != "bigint" {
			break
		}
		return "bigint", nil, nil
	case "bool", "boolean":
		if !noArguments() || source.Type != "boolean" {
			break
		}
		return "boolean", nil, nil
	case "decimal", "numeric":
		if source.Type != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 38 ||
			arguments[1] < 0 ||
			arguments[1] > arguments[0] {
			break
		}
		return "numeric", copyDeclaration("numeric"), nil
	case "real":
		if !noArguments() || source.Type != "real" {
			break
		}
		return "real", &schema.DeclaredType{Base: "real"}, nil
	case "double precision":
		if !noArguments() || source.Type != "double precision" {
			break
		}
		return "double precision", nil, nil
	case "char", "varchar":
		// SQL Server's admitted UTF-8 modifier is a byte limit, while
		// PostgreSQL VARCHAR counts characters. Keeping the numeric modifier
		// is an explicit safe widening: every source value remains valid, and
		// CHAR rows/defaults retain their discovered padding without asking
		// PostgreSQL to add different padding.
		if source.Type != "text" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		return "text", copyDeclaration("varchar"), nil
	case "text":
		if !noArguments() || source.Type != "text" {
			break
		}
		return "text", nil, nil
	case "binary", "varbinary":
		// BYTEA has no length modifier. This is likewise a safe widening for
		// copied values; binary columns are excluded from comparator-bearing
		// objects because SQL Server's zero-padding equality is not portable.
		if source.Type != "blob" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		return "bytea", nil, nil
	case "blob":
		if !noArguments() || source.Type != "blob" {
			break
		}
		return "bytea", nil, nil
	case "date":
		if !noArguments() || source.Type != "date" {
			break
		}
		return "date", nil, nil
	case "time":
		if source.Type != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		return "time", copyDeclaration("time"), nil
	case "timestamp":
		if source.Type != "datetime" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		return "datetime", copyDeclaration("timestamp"), nil
	case "smalldatetime":
		if !noArguments() || source.Type != "datetime" {
			break
		}
		return "datetime", &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{0},
		}, nil
	case "uuid":
		if !noArguments() || source.Type != "uuid" {
			break
		}
		return "uuid", nil, nil
	}
	return "", nil, fmt.Errorf("unsupported SQL Server declared type")
}

func postgresSQLServerPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}
