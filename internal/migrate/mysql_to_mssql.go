package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// MySQLToSQLServerWithObserver migrates deterministic, version-pinned Oracle
// MySQL 8.0 or MariaDB 10.11 metadata and rows through the shared source/target
// adapter runner. The MySQL source adapter admits and preserves the concrete
// server flavor before the SQL Server target plans any mutation.
func MySQLToSQLServerWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "mssql" {
		return Result{}, fmt.Errorf(
			"MySQL-to-SQL Server requires source.type mysql and target.type mssql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mysql", target: "mssql"},
	)
}
