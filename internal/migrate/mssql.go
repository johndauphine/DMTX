package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// SQLServerToSQLiteWithObserver migrates SQL Server base tables through the
// shared source/target adapter runner.
func SQLServerToSQLiteWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf(
			"SQL Server-to-SQLite requires source.type mssql and target.type sqlite",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mssql", target: "sqlite"},
	)
}

func sqlServerReadQuery(
	namespace string,
	table schema.Table,
	columns []string,
) string {
	return "SELECT " + sqlServerQuotedColumns(columns) +
		" FROM " + sqlServerQualified(namespace, table.Name) +
		" ORDER BY " + sqlServerQuotedColumns(primaryKeyColumns(table))
}

func sqlServerQualified(namespace, name string) string {
	return sqlServerIdentifier(namespace) + "." + sqlServerIdentifier(name)
}

func sqlServerQuotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = sqlServerIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

func sqlServerIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}
