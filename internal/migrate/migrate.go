package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
)

// Execute selects the implemented engine pair without allowing an unsupported
// configuration to silently run a different migration path.
func Execute(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type == "sqlite" && cfg.Target.Type == "sqlite" {
		return SQLiteToSQLiteWithObserver(ctx, cfg, observer)
	}
	return Result{}, fmt.Errorf("unsupported migration pair %s-to-%s", cfg.Source.Type, cfg.Target.Type)
}
