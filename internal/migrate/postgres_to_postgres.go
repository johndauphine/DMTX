package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// PostgresToPostgresWithObserver migrates a PostgreSQL schema to a distinct
// PostgreSQL endpoint through the shared source/target adapter runner.
func PostgresToPostgresWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "postgres", target: "postgres"},
	)
}
