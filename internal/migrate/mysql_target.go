package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// SQLiteToMySQLWithObserver migrates deterministic SQLite metadata and rows
// through the shared source adapter and the version-pinned native Oracle
// MySQL 8.0 or MariaDB 10.11 target adapter.
func SQLiteToMySQLWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "mysql" {
		return Result{}, fmt.Errorf(
			"SQLite-to-MySQL requires source.type sqlite and target.type mysql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "sqlite", target: "mysql"},
	)
}
