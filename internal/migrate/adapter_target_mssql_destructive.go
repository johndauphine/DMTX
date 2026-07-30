package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// PreflightDestructive enforces the drop/recreate backup gate against the
// live selected SQL Server tables. It is deliberately separate from retained
// shape preflight so every catalog and source-data rejection still runs before
// the first target checkpoint or mutation.
func (adapter *sqlServerTargetAdapter) PreflightDestructive(
	ctx context.Context,
	targetTables []schema.Table,
	migration config.Migration,
) error {
	mode, err := normalizeAdapterTargetMode(migration.TargetMode)
	if err != nil {
		return err
	}
	adapter.destructiveAcknowledged = false
	if mode != "drop_recreate" {
		return nil
	}
	if migration.DestructiveAcknowledged {
		adapter.destructiveAcknowledged = true
		return nil
	}
	for _, table := range targetTables {
		exists, err := sqlServerTargetBaseTableExists(
			ctx,
			adapter.database,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQL Server rebuild target %s: %w",
				table.Name,
				err,
			)
		}
		if !exists {
			continue
		}
		var populated bool
		if err := adapter.database.QueryRowContext(
			ctx,
			"SELECT CONVERT(bit, CASE WHEN EXISTS ("+
				"SELECT TOP (1) 1 FROM "+
				sqlServerQualified(table.Schema, table.Name)+
				") THEN 1 ELSE 0 END)",
		).Scan(&populated); err != nil {
			return fmt.Errorf(
				"inspect SQL Server rebuild target rows for %s: %w",
				table.Name,
				err,
			)
		}
		if populated {
			return fmt.Errorf(
				"%w: SQL Server target table %q contains rows; rerun with --acknowledge-destructive",
				ErrDestructiveAcknowledgement,
				table.Name,
			)
		}
	}
	return nil
}
