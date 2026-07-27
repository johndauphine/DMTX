package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
)

// Execute selects the implemented engine pair without allowing an unsupported
// configuration to silently run a different migration path.
func Execute(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if err := engine.ValidateMigration(cfg); err != nil {
		return Result{}, err
	}
	if cfg.Source.Type == "sqlite" && cfg.Target.Type == "sqlite" {
		return SQLiteToSQLiteWithObserver(ctx, cfg, observer)
	}
	if cfg.Source.Type == "postgres" && cfg.Target.Type == "sqlite" {
		return PostgresToSQLiteWithObserver(ctx, cfg, observer)
	}
	return Result{}, fmt.Errorf("unsupported migration pair %s-to-%s", cfg.Source.Type, cfg.Target.Type)
}
