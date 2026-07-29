package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// MySQLToPostgresWithObserver migrates deterministic MySQL metadata and rows
// through the shared source/target adapter runner.
func MySQLToPostgresWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf(
			"MySQL-to-PostgreSQL requires source.type mysql and target.type postgres",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mysql", target: "postgres"},
	)
}
