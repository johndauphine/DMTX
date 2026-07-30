package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// SQLServerToMySQLWithObserver migrates deterministic SQL Server metadata and
// rows through the shared source/target adapter runner. The target adapter
// selects and verifies the admitted Oracle MySQL or MariaDB server flavor
// before it projects any source schema.
func SQLServerToMySQLWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "mysql" {
		return Result{}, fmt.Errorf(
			"SQL Server-to-MySQL requires source.type mssql and target.type mysql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mssql", target: "mysql"},
	)
}
