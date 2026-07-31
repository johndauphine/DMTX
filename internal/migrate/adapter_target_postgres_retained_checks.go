package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresRetainedCheck struct {
	name             string
	signature        schema.PostgresCheckSignature
	validated        bool
	local            bool
	inheritanceCount int
	noInherit        bool
	parentConstraint bool
}

func preflightPostgresRetainedChecks(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	planned, err := planPostgresRetainedChecks(tables)
	if err != nil {
		return fmt.Errorf("plan retained PostgreSQL CHECK constraints: %w", err)
	}
	for _, table := range tables {
		actual, err := readPostgresRetainedChecks(
			ctx,
			database,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect retained PostgreSQL CHECK constraints for table %s: %w",
				table.Name,
				err,
			)
		}
		key := postgresRetainedTableKey(table.Schema, table.Name)
		if err := validatePostgresRetainedChecks(
			table,
			planned[key],
			actual,
		); err != nil {
			return err
		}
	}
	return nil
}

func planPostgresRetainedChecks(
	tables []schema.Table,
) (map[string][]postgresRetainedCheck, error) {
	statements, err := schema.PlanPostgresDropRecreateObjects(
		tables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		return nil, err
	}
	names := make(map[string][]string, len(tables))
	for _, statement := range statements {
		if statement.Kind() != schema.PostgresCheckObject {
			continue
		}
		key := postgresRetainedTableKey(
			statement.Schema(),
			statement.Table(),
		)
		names[key] = append(names[key], statement.Name())
	}

	result := make(map[string][]postgresRetainedCheck, len(tables))
	for _, table := range tables {
		key := postgresRetainedTableKey(table.Schema, table.Name)
		checks := append([]schema.CheckConstraint(nil), table.Checks...)
		sort.Slice(checks, func(left, right int) bool {
			return postgresRetainedCheckSortKey(checks[left]) <
				postgresRetainedCheckSortKey(checks[right])
		})
		if len(names[key]) != len(checks) {
			return nil, fmt.Errorf(
				"table %s planned %d PostgreSQL CHECK names for %d constraints",
				table.Name,
				len(names[key]),
				len(checks),
			)
		}
		for index, check := range checks {
			signature, err := schema.PlannedPostgresCheckSignature(
				check.Expression,
				table.Columns,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"plan PostgreSQL CHECK constraint %q on table %s: %w",
					names[key][index],
					table.Name,
					err,
				)
			}
			result[key] = append(result[key], postgresRetainedCheck{
				name:      names[key][index],
				signature: signature,
				validated: true,
				local:     true,
			})
		}
	}
	return result, nil
}

const postgresRetainedChecksQuery = `
	SELECT
		constraint_object.oid::bigint,
		constraint_object.conname,
		constraint_object.convalidated,
		constraint_object.conislocal,
		constraint_object.coninhcount,
		constraint_object.connoinherit,
		COALESCE(
			(to_jsonb(constraint_object)->>'conparentid')::oid,
			0
		) <> 0,
		pg_catalog.pg_get_expr(
			constraint_object.conbin,
			constraint_object.conrelid,
			true
		)
	FROM pg_catalog.pg_constraint AS constraint_object
	JOIN pg_catalog.pg_class AS table_relation
	  ON table_relation.oid = constraint_object.conrelid
	JOIN pg_catalog.pg_namespace AS table_namespace
	  ON table_namespace.oid = table_relation.relnamespace
	WHERE table_namespace.nspname = $1
	  AND table_relation.relname = $2
	  AND constraint_object.contype = 'c'
	ORDER BY constraint_object.conname, constraint_object.oid
`

func readPostgresRetainedChecks(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]postgresRetainedCheck, error) {
	rows, err := database.QueryContext(
		ctx,
		postgresRetainedChecksQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []postgresRetainedCheck
	for rows.Next() {
		var (
			objectID   int64
			expression string
			check      postgresRetainedCheck
		)
		if err := rows.Scan(
			&objectID,
			&check.name,
			&check.validated,
			&check.local,
			&check.inheritanceCount,
			&check.noInherit,
			&check.parentConstraint,
			&expression,
		); err != nil {
			return nil, err
		}
		signature, err := schema.ParsePostgresCheckSignature(
			expression,
			table.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"parse PostgreSQL CHECK constraint %q (object %d): %w",
				check.name,
				objectID,
				err,
			)
		}
		check.signature = signature
		result = append(result, check)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePostgresRetainedChecks(
	table schema.Table,
	planned []postgresRetainedCheck,
	actual []postgresRetainedCheck,
) error {
	plannedByName, err := postgresRetainedChecksByName(planned)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s CHECK constraints: %w",
			table.Name,
			err,
		)
	}
	actualByName, err := postgresRetainedChecksByName(actual)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s CHECK constraints: %w",
			table.Name,
			err,
		)
	}
	for name, expected := range plannedByName {
		found, ok := actualByName[name]
		if !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: required CHECK constraint %q is missing",
				table.Name,
				name,
			)
		}
		if expected != found {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: CHECK constraint %q differs from the planned shape",
				table.Name,
				name,
			)
		}
	}
	for name := range actualByName {
		if _, ok := plannedByName[name]; !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: unexpected CHECK constraint %q is retained",
				table.Name,
				name,
			)
		}
	}
	return nil
}

func postgresRetainedChecksByName(
	values []postgresRetainedCheck,
) (map[string]postgresRetainedCheck, error) {
	result := make(map[string]postgresRetainedCheck, len(values))
	for _, value := range values {
		if _, exists := result[value.name]; exists {
			return nil, fmt.Errorf(
				"duplicate CHECK constraint %q",
				value.name,
			)
		}
		result[value.name] = value
	}
	return result, nil
}

func postgresRetainedCheckSortKey(check schema.CheckConstraint) string {
	return check.Name + "\x00" + check.Expression.CanonicalSQL()
}
