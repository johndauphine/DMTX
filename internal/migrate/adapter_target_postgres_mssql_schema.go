package migrate

import (
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

// projectSQLServerColumnForPostgres routes through the canonical type.
//
// This is the first route to leave the pairwise projection behind. What was a
// switch over SQL Server's spellings deciding PostgreSQL's is now two steps
// that each have one job: the source's declaration becomes a canonical type,
// and the canonical type becomes the target's.
//
// Proved equivalent before it was switched, not after.
// TestCanonicalMatchesPairwiseForSQLServerToPostgres runs both paths over the
// SO2010 corpus's shapes and the boundaries that have gone wrong on this branch
// - a bounded national string, an unbounded one, a datetime carrying fractional
// seconds - and requires identical portable types and identical declarations.
// The pairwise code is what has been writing production schemas, so a canonical
// path that differed from it would be a behaviour change wearing a refactor's
// clothes.
//
// Collation is not consulted here and the isKey argument is false, because this
// function projects a column's shape rather than deciding whether it may order
// a paged read. That question is asked at discovery, where key membership is
// known - checkSQLServerSourceKeyCollations - and asking it again here with
// neither the collation nor the key set to hand is how the old rule came to be
// applied to every text column in the table.
func projectSQLServerColumnForPostgres(
	source schema.Column,
) (string, *schema.DeclaredType, error) {
	canonical, err := schema.CanonicalFromSQLServer(source, "", false)
	if err != nil {
		return "", nil, err
	}
	targetType, declared, err := schema.CanonicalToDeclared(
		canonical,
		schema.Postgres,
	)
	if err != nil {
		return "", nil, nameTheColumn(err, source.Name)
	}
	return targetType, declared, nil
}

func postgresSQLServerPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}
