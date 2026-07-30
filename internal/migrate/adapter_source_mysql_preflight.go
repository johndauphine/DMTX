package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type mySQLSourceValueCheck struct {
	table  string
	column string
	kind   string
	query  string
}

func preflightMySQLSourceRows(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	tables []schema.Table,
) error {
	if database == nil {
		return fmt.Errorf("preflight MySQL source rows: database is required")
	}
	checks, err := planMySQLSourceValueChecks(namespace, tables)
	if err != nil {
		return err
	}
	for _, check := range checks {
		var invalid int
		if err := database.QueryRowContext(
			ctx,
			check.query,
		).Scan(&invalid); err != nil {
			return fmt.Errorf(
				"preflight MySQL source %s values for %s.%s: %w",
				check.kind,
				check.table,
				check.column,
				err,
			)
		}
		if invalid != 0 {
			return &schema.PolicyError{
				Operation: "preflight MySQL source " +
					check.kind + " values",
				Type:   check.table + "." + check.column,
				Target: string(schema.MySQL),
			}
		}
	}
	return nil
}

func planMySQLSourceValueChecks(
	namespace string,
	tables []schema.Table,
) ([]mySQLSourceValueCheck, error) {
	if namespace == "" {
		return nil, fmt.Errorf(
			"preflight MySQL source rows: namespace is required",
		)
	}
	checks := make([]mySQLSourceValueCheck, 0)
	for _, table := range tables {
		if table.Schema != namespace {
			return nil, fmt.Errorf(
				"preflight MySQL source rows: table %s has schema %q, want %q",
				table.Name,
				table.Schema,
				namespace,
			)
		}
		qualified := mySQLQualified(namespace, table.Name)
		for _, column := range table.Columns {
			identifier := mySQLIdentifier(column.Name)
			switch strings.ToLower(strings.TrimSpace(column.Type)) {
			case "date", "datetime", "timestamp":
				checks = append(checks, mySQLSourceValueCheck{
					table:  table.Name,
					column: column.Name,
					kind:   "temporal",
					query: "SELECT EXISTS (" +
						"SELECT 1 FROM " + qualified +
						" WHERE " + identifier + " IS NOT NULL" +
						" AND (" +
						"YEAR(" + identifier + ") = 0" +
						" OR MONTH(" + identifier + ") = 0" +
						" OR DAYOFMONTH(" + identifier + ") = 0" +
						" OR LAST_DAY(" + identifier + ") IS NULL" +
						" OR DAYOFMONTH(" + identifier + ")" +
						" > DAYOFMONTH(LAST_DAY(" +
						identifier + "))" +
						"))",
				})
			case "json":
				checks = append(checks, mySQLSourceValueCheck{
					table:  table.Name,
					column: column.Name,
					kind:   "JSON",
					query: "SELECT EXISTS (" +
						"SELECT 1 FROM " + qualified +
						" WHERE " + identifier + " IS NOT NULL" +
						" AND COALESCE(JSON_VALID(" +
						identifier + "), 0) <> 1)",
				})
			}
		}
	}
	return checks, nil
}
