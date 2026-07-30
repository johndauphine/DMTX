package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// SQLServerToPostgresWithObserver migrates deterministic SQL Server metadata
// and rows through the shared source/target adapter runner.
func SQLServerToPostgresWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf(
			"SQL Server-to-PostgreSQL requires source.type mssql and target.type postgres",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mssql", target: "postgres"},
	)
}
