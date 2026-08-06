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
	case "char", "varchar", "nchar", "nvarchar":
		// SQL Server's admitted UTF-8 modifier is a byte limit, while
		// PostgreSQL VARCHAR counts characters. Keeping the numeric modifier
		// is an explicit safe widening: every source value remains valid, and
		// CHAR rows/defaults retain their discovered padding without asking
		// PostgreSQL to add different padding.
		//
		// The national spellings arrive already converted to characters by
		// discovery, which halves sys.columns.max_length for them - so the
		// bound below is characters for all four, and nvarchar's own 4000
		// ceiling is enforced where that conversion happens rather than
		// re-derived here from a number that no longer says which type it came
		// from.
		if source.Type != "text" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > sqlServerProjectedTextLengthLimit(base) {
			break
		}
		// A PostgreSQL catalog round trip represents VARCHAR as varchar (not
		// source-neutral text). Keep the projection canonical so retained
		// target authority can be authenticated on later incremental runs.
		return "varchar", copyDeclaration("varchar"), nil
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
		// PostgreSQL catalog discovery represents TIMESTAMP as timestamp, not
		// the source-neutral datetime alias.  Keep the projected generic type
		// canonical so a later prior-authority proof can authenticate the exact
		// table that this projection created.
		return "timestamp", copyDeclaration("timestamp"), nil
	case "smalldatetime":
		if !noArguments() || source.Type != "datetime" {
			break
		}
		return "timestamp", &schema.DeclaredType{
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

// sqlServerProjectedTextLengthLimit is the largest length each SQL Server text
// family can legally declare.
//
// char and varchar declare bytes and cap at 8000; nchar and nvarchar declare
// UTF-16 code units and cap at 4000, which is the same 8000 bytes of storage.
// Beyond either, SQL Server requires MAX and the column arrives as unbounded
// text instead.
//
// This is the third place the same two numbers appear - discovery refuses an
// over-long declaration in sqlServerTextLengthLimit, and the retained row bound
// refuses one again in sqlServerRetainedTextLengthLimit. Three copies of one
// fact is not a design; it is what a pairwise projection costs, and it is why
// an nvarchar(8000) that cannot exist was refused in two of the three and
// accepted here. Until the canonical type work removes the duplication, the
// copies are at least spelled the same way so a search finds all of them.
func sqlServerProjectedTextLengthLimit(base string) int {
	switch base {
	case "nchar", "nvarchar":
		return 4_000
	default:
		return 8_000
	}
}
