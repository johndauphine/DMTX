package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// PostgresToSQLiteWithObserver migrates PostgreSQL base tables through the
// shared source/target adapter runner. Cross-engine resume frontiers remain
// Stage 3 work.
func PostgresToSQLiteWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-SQLite requires source.type postgres and target.type sqlite",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "postgres", target: "sqlite"},
	)
}

func postgresReadQuery(
	namespace string,
	table schema.Table,
	columns []string,
) string {
	return "SELECT " + quotedColumns(columns) +
		" FROM " + postgresQualified(namespace, table.Name) +
		" ORDER BY " + quotedColumns(primaryKeyColumns(table))
}

func postgresQualified(namespace, name string) string {
	return postgresIdentifier(namespace) + "." + postgresIdentifier(name)
}

func postgresReadQueryForColumns(
	namespace string,
	name string,
	columns []string,
	primaryKeys []string,
) string {
	table := schema.Table{Name: name, Columns: make([]schema.Column, len(columns))}
	keys := make(map[string]bool, len(primaryKeys))
	for _, key := range primaryKeys {
		keys[key] = true
	}
	for index, column := range columns {
		table.Columns[index] = schema.Column{
			Name:       column,
			PrimaryKey: keys[column],
		}
	}
	return postgresReadQuery(namespace, table, columns)
}

func postgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
