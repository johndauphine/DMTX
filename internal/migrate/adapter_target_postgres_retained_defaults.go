package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresRetainedDefault struct {
	columnName string
	expression *string
}

func preflightPostgresRetainedDefaults(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	for _, table := range tables {
		actual, err := readPostgresRetainedDefaults(ctx, database, table)
		if err != nil {
			return fmt.Errorf(
				"inspect retained PostgreSQL defaults for table %s: %w",
				table.Name,
				err,
			)
		}
		if err := validatePostgresRetainedDefaults(table, actual); err != nil {
			return err
		}
	}
	return nil
}

const postgresRetainedDefaultsQuery = `
	SELECT
		attribute.attname,
		pg_catalog.pg_get_expr(
			default_expression.adbin,
			default_expression.adrelid,
			true
		)
	FROM pg_catalog.pg_attribute AS attribute
	JOIN pg_catalog.pg_class AS relation
	  ON relation.oid = attribute.attrelid
	JOIN pg_catalog.pg_namespace AS namespace
	  ON namespace.oid = relation.relnamespace
	LEFT JOIN pg_catalog.pg_attrdef AS default_expression
	  ON default_expression.adrelid = relation.oid
	 AND default_expression.adnum = attribute.attnum
	WHERE namespace.nspname = $1
	  AND relation.relname = $2
	  AND relation.relkind = 'r'
	  AND attribute.attnum > 0
	  AND NOT attribute.attisdropped
	ORDER BY attribute.attnum
`

func readPostgresRetainedDefaults(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]postgresRetainedDefault, error) {
	rows, err := database.QueryContext(
		ctx,
		postgresRetainedDefaultsQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]postgresRetainedDefault, 0, len(table.Columns))
	for rows.Next() {
		var (
			value      postgresRetainedDefault
			expression sql.NullString
		)
		if err := rows.Scan(&value.columnName, &expression); err != nil {
			return nil, err
		}
		if expression.Valid {
			copied := expression.String
			value.expression = &copied
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePostgresRetainedDefaults(
	table schema.Table,
	actual []postgresRetainedDefault,
) error {
	if len(actual) != len(table.Columns) {
		return fmt.Errorf(
			"preflight PostgreSQL table %s defaults: catalog returned %d columns; planned shape requires %d",
			table.Name,
			len(actual),
			len(table.Columns),
		)
	}
	for index, column := range table.Columns {
		found := actual[index]
		if found.columnName != column.Name {
			return fmt.Errorf(
				"preflight PostgreSQL table %s defaults: catalog column %d is %q; planned column is %q",
				table.Name,
				index+1,
				found.columnName,
				column.Name,
			)
		}
		matches, err := schema.PostgresDefaultSignaturesMatch(
			column,
			found.expression,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight PostgreSQL table %s column %q default cannot be proven equivalent: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		if !matches {
			return fmt.Errorf(
				"preflight PostgreSQL table %s column %q default differs from the planned shape",
				table.Name,
				column.Name,
			)
		}
	}
	return nil
}
