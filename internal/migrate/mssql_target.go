package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

// SQLiteToSQLServerWithObserver migrates deterministic SQLite metadata and
// rows through the shared source/target adapter runner.
func SQLiteToSQLServerWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "mssql" {
		return Result{}, fmt.Errorf(
			"SQLite-to-SQL-Server requires source.type sqlite and target.type mssql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "sqlite", target: "mssql"},
	)
}

func sqlServerPlaceholders(count int) string {
	parts := make([]string, count)
	for index := range parts {
		parts[index] = fmt.Sprintf("@p%d", index+1)
	}
	return strings.Join(parts, ", ")
}
