package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

type sqliteTargetAdapter struct {
	database *sql.DB
}

func validateSQLiteTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Database == "" {
		return fmt.Errorf("SQLite target database path is required")
	}
	return nil
}

func openSQLiteTargetAdapter(
	_ context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validateSQLiteTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlite", endpoint.Database)
	if err != nil {
		return nil, fmt.Errorf("open SQLite target: %w", err)
	}
	return &sqliteTargetAdapter{database: database}, nil
}

func (adapter *sqliteTargetAdapter) Engine() string {
	return "sqlite"
}

func (adapter *sqliteTargetAdapter) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	if _, err := normalizeAdapterTargetMode(mode); err != nil {
		return nil, err
	}
	switch sourceEngine {
	case "postgres", "mysql", "sqlite":
	default:
		return nil, fmt.Errorf(
			"SQLite target does not support source engine %q",
			sourceEngine,
		)
	}
	targetTables := make([]schema.Table, 0, len(sourceTables))
	for _, sourceTable := range sourceTables {
		targetTable := sourceTable
		targetTable.Schema = ""
		targetTable.Identity = cloneSchemaIdentity(sourceTable.Identity)
		if _, err := schema.DropTable(schema.SQLite, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQLite table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateTable(schema.SQLite, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQLite table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateIndexes(schema.SQLite, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQLite indexes for %s: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.SQLiteSequencePlan(targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQLite sequence for %s: %w",
				targetTable.Name,
				err,
			)
		}
		targetTables = append(targetTables, targetTable)
	}
	return targetTables, nil
}

func (adapter *sqliteTargetAdapter) PreflightTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	for _, targetTable := range targetTables {
		exists, err := tableExists(ctx, adapter.database, targetTable.Name)
		if err != nil {
			return fmt.Errorf(
				"preflight SQLite table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if mode == "upsert" && !exists {
			return fmt.Errorf(
				"preflight SQLite table %s: upsert requires an existing target table",
				targetTable.Name,
			)
		}
		if mode == "upsert" {
			discovered, _, err := inspectSQLiteSchema(
				ctx,
				adapter.database,
				targetTable.Name,
			)
			if err != nil {
				return fmt.Errorf(
					"preflight SQLite table %s identity: %w",
					targetTable.Name,
					err,
				)
			}
			if !sameSQLiteIdentityShape(
				targetTable.Identity,
				discovered.Identity,
			) {
				return fmt.Errorf(
					"preflight SQLite table %s: target identity does not match the planned identity",
					targetTable.Name,
				)
			}
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) PrepareTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if mode == "upsert" {
		return nil
	}
	for _, targetTable := range targetTables {
		if err := prepareTarget(ctx, adapter.database, targetTable, mode); err != nil {
			return err
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	return writeSQLiteBatchReceipt(
		ctx,
		adapter.database,
		table,
		columns,
		mode,
		rows,
		nil,
		nil,
	)
}

func (adapter *sqliteTargetAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	return countRows(ctx, adapter.database, table.Name)
}

func (adapter *sqliteTargetAdapter) FinalizeTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if mode == "upsert" {
		for _, targetTable := range targetTables {
			if err := resetSQLiteSequence(
				ctx, adapter.database, targetTable, nil,
			); err != nil {
				return err
			}
		}
		return nil
	}
	for _, targetTable := range targetTables {
		if err := finalizeSQLiteTarget(
			ctx,
			adapter.database,
			targetTable,
			nil,
		); err != nil {
			return err
		}
	}
	return nil
}

func sameSQLiteIdentityShape(
	planned *schema.Identity,
	discovered *schema.Identity,
) bool {
	if planned == nil || discovered == nil {
		return planned == nil && discovered == nil
	}
	return planned.Column == discovered.Column &&
		planned.Generation == discovered.Generation
}

func (adapter *sqliteTargetAdapter) Close() error {
	return adapter.database.Close()
}
