package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
)

// ErrDestructiveAcknowledgement classifies a rebuild that would replace
// target data without the operator confirming a backup/destructive action.
var ErrDestructiveAcknowledgement = errors.New("destructive target backup acknowledgement is required")

func requireSQLiteDestructiveAcknowledgement(ctx context.Context, target *sql.DB, tables []string, migration config.Migration) error {
	if migration.TargetMode != "drop_recreate" || migration.DestructiveAcknowledged {
		return nil
	}
	for _, table := range tables {
		exists, err := tableExists(ctx, target, table)
		if err != nil {
			return fmt.Errorf("inspect rebuild target %s: %w", table, err)
		}
		if !exists {
			continue
		}
		var populated bool
		if err := target.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM "+quote(table)+" LIMIT 1)").Scan(&populated); err != nil {
			return fmt.Errorf("inspect rebuild target rows for %s: %w", table, err)
		}
		if populated {
			return fmt.Errorf("%w: target table %q contains rows; rerun with --acknowledge-destructive", ErrDestructiveAcknowledgement, table)
		}
	}
	return nil
}
