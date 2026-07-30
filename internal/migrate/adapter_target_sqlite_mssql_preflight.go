package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// preflightSQLServerSQLiteObjectNames proves that SQLite's database-wide
// table/index namespace will not turn a later CREATE into a partial-mutation
// failure. Objects owned by selected tables are safe only when dropping those
// tables necessarily removes the object before finalization.
func preflightSQLServerSQLiteObjectNames(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	selected := make(map[string]bool, len(tables))
	plannedTables := make(map[string]string, len(tables))
	plannedIndexes := make(map[string]string)
	for _, table := range tables {
		key := strings.ToLower(table.Name)
		selected[key] = true
		plannedTables[key] = table.Name
		for _, index := range table.Indexes {
			if !index.Inline {
				plannedIndexes[strings.ToLower(index.Name)] = index.Name
			}
		}
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT type, name, tbl_name
		   FROM sqlite_schema
		  WHERE type IN ('table', 'index', 'view', 'trigger')
		    AND name NOT LIKE 'sqlite_%'
		  ORDER BY lower(name), type, name`,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQLite target object names: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var objectType, name, owner string
		if err := rows.Scan(&objectType, &name, &owner); err != nil {
			return fmt.Errorf(
				"read SQLite target object name: %w",
				err,
			)
		}
		key := strings.ToLower(name)
		if planned, exists := plannedTables[key]; exists {
			if objectType == "table" &&
				strings.EqualFold(owner, planned) {
				continue
			}
			return sqliteSQLServerProjectionPolicy(
				"preflight SQLite object name",
				objectType+" "+name+" collides with table "+planned,
			)
		}
		if planned, exists := plannedIndexes[key]; exists {
			if objectType == "index" &&
				selected[strings.ToLower(owner)] {
				continue
			}
			return sqliteSQLServerProjectionPolicy(
				"preflight SQLite object name",
				objectType+" "+name+" collides with index "+planned,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQLite target object names: %w",
			err,
		)
	}
	return nil
}
