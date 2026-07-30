package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type mysqlTargetCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// PreflightDestructive enforces the rebuild backup gate against every
// selected live MySQL-family target. PrepareTables repeats this proof while
// holding WRITE locks through one multi-table DROP, so a concurrent insert
// cannot enter between the final row check and destructive DDL.
func (adapter *mysqlTargetAdapter) PreflightDestructive(
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
	return requireUnpopulatedMySQLTargets(
		ctx,
		adapter.database,
		targetTables,
	)
}

func requireUnpopulatedMySQLTargets(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	targetTables []schema.Table,
) error {
	ordered := append([]schema.Table(nil), targetTables...)
	sort.Slice(ordered, func(left, right int) bool {
		return adapterSourceTableKey(
			ordered[left].Schema,
			ordered[left].Name,
		) < adapterSourceTableKey(
			ordered[right].Schema,
			ordered[right].Name,
		)
	})
	for _, table := range ordered {
		kind, exists, err := mysqlTargetRelationKind(
			ctx,
			database,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect MySQL rebuild target %s: %w",
				table.Name,
				err,
			)
		}
		if !exists {
			continue
		}
		if kind != "BASE TABLE" {
			return fmt.Errorf(
				"inspect MySQL rebuild target %s: existing target object is %s, not a base table",
				table.Name,
				kind,
			)
		}
		var populated bool
		if err := database.QueryRowContext(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM "+
				mySQLQualified(table.Schema, table.Name)+
				" LIMIT 1)",
		).Scan(&populated); err != nil {
			return fmt.Errorf(
				"inspect MySQL rebuild target rows for %s: %w",
				table.Name,
				err,
			)
		}
		if populated {
			return fmt.Errorf(
				"%w: MySQL target table %q contains rows; rerun with --acknowledge-destructive",
				ErrDestructiveAcknowledgement,
				table.Name,
			)
		}
	}
	return nil
}

func mysqlTargetRelationKind(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	table schema.Table,
) (string, bool, error) {
	var kind string
	err := database.QueryRowContext(
		ctx,
		`SELECT TABLE_TYPE
		   FROM information_schema.TABLES
		  WHERE TABLE_SCHEMA = ?
		    AND TABLE_NAME = ?`,
		table.Schema,
		table.Name,
	).Scan(&kind)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return kind, true, nil
}
