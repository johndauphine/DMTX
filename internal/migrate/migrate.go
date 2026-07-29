package migrate

import (
	"context"

	"github.com/johndauphine/dmtx/internal/config"
)

// Execute resolves one certified source and target adapter route without
// allowing an unsupported configuration to run a different migration path.
func Execute(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	return executeWithRegistry(ctx, cfg, observer, builtInAdapters)
}

func executeWithRegistry(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	registry adapterRegistry,
) (Result, error) {
	route, err := resolveMigration(cfg, registry)
	if err != nil {
		return Result{}, err
	}
	return route.execute(ctx, cfg, observer)
}
