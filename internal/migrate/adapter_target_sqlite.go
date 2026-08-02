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
	database                *sql.DB
	stage4BatchWriter       sqliteStage4NetworkBatchWriter
	sourceEngine            string
	sqlServerRoute          bool
	destructiveAcknowledged bool
	// evolutionCommit is nil in production. Keeping the COMMIT acknowledgement
	// seam per adapter lets the evolution package exercise the real
	// commit-then-error recovery path without a process-global test hook.
	evolutionCommit func(context.Context, *sql.Conn) (sql.Result, error)
	// deleteCommit and deleteAfterReservation are nil in production. They keep
	// the Stage 4 delete receipt path's commit-acknowledgement and writer-fence
	// recovery testable without a package-global hook.
	deleteCommit           func(context.Context, *sql.Conn) (sql.Result, error)
	deleteAfterReservation func(context.Context, *sql.Conn) error
}

func validateSQLiteTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Database == "" {
		return fmt.Errorf("SQLite target database path is required")
	}
	return nil
}

func openSQLiteTargetAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validateSQLiteTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	database, err := openSQLiteTargetDatabase(ctx, endpoint.Database)
	if err != nil {
		return nil, err
	}
	return &sqliteTargetAdapter{
		database:          database,
		stage4BatchWriter: newSQLiteStage4NetworkWriter(database),
	}, nil
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
	adapter.sourceEngine = ""
	adapter.sqlServerRoute = false
	switch sourceEngine {
	case "postgres", "mysql", "sqlite":
	case "mssql":
		adapter.sqlServerRoute = true
	default:
		return nil, fmt.Errorf(
			"SQLite target does not support source engine %q",
			sourceEngine,
		)
	}
	adapter.sourceEngine = sourceEngine
	targetTables := make([]schema.Table, 0, len(sourceTables))
	for _, sourceTable := range sourceTables {
		targetTable := sourceTable
		switch sourceEngine {
		case "postgres":
			var err error
			targetTable, err = projectPostgresTableForSQLite(sourceTable)
			if err != nil {
				return nil, err
			}
		case "mysql":
			var err error
			targetTable, err = projectMySQLTableForSQLite(sourceTable)
			if err != nil {
				return nil, err
			}
		case "mssql":
			var err error
			targetTable, err = projectSQLServerTableForSQLite(sourceTable)
			if err != nil {
				return nil, err
			}
		default:
			targetTable.Schema = ""
			targetTable.Identity = cloneSchemaIdentity(sourceTable.Identity)
		}
		if err := rebaseProjectedForeignKeySchemas(
			sourceTable.Schema,
			"",
			"SQLite",
			&targetTable,
		); err != nil {
			return nil, err
		}
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
	if sourceEngine == "postgres" {
		if err := validatePostgresSQLiteTables(
			sourceTables,
			targetTables,
		); err != nil {
			return nil, err
		}
	}
	if sourceEngine == "mysql" {
		if err := validateMySQLSQLiteTables(
			sourceTables,
			targetTables,
		); err != nil {
			return nil, err
		}
	}
	if sourceEngine == "mssql" {
		if err := validateSQLServerSQLiteTables(
			sourceTables,
			targetTables,
		); err != nil {
			return nil, err
		}
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
	if adapter.sqlServerRoute {
		if err := preflightSQLServerSQLiteObjectNames(
			ctx,
			adapter.database,
			targetTables,
		); err != nil {
			return err
		}
	}
	if err := preflightSQLiteTargetCatalog(
		ctx,
		adapter.database,
		targetTables,
		mode,
	); err != nil {
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
			if err := rejectSQLiteTableTriggers(
				ctx,
				adapter.database,
				targetTable.Name,
			); err != nil {
				return fmt.Errorf(
					"preflight SQLite table %s: %w",
					targetTable.Name,
					err,
				)
			}
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
			if err := validateSQLiteRetainedTable(
				targetTable,
				discovered,
			); err != nil {
				return fmt.Errorf(
					"preflight SQLite table %s retained schema: %w",
					targetTable.Name,
					err,
				)
			}
			if err := preflightSQLiteForeignKeyIntegrity(
				ctx,
				adapter.database,
				targetTable.Name,
			); err != nil {
				return err
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
	return adapter.prepareDropRecreate(ctx, targetTables)
}

func (adapter *sqliteTargetAdapter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	normalized, err := adapter.normalizeWriteBatch(
		table,
		columns,
		rows,
	)
	if err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: int64(len(rows)),
		}, err
	}
	return writeSQLiteBatchReceipt(
		ctx,
		adapter.database,
		table,
		columns,
		mode,
		normalized,
		nil,
		nil,
	)
}

func (adapter *sqliteTargetAdapter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	if adapter == nil {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network target adapter is not configured",
				),
			)
	}
	normalized, err := adapter.normalizeWriteBatch(
		table,
		columns,
		rows,
	)
	if err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: attempted,
		}, err
	}
	writer := adapter.stage4BatchWriter
	if writer == nil && adapter.database != nil {
		writer = newSQLiteStage4NetworkWriter(adapter.database)
	}
	if writer == nil {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network batch writer is not configured",
				),
			)
	}
	return writer.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
		normalized,
	)
}

// WriteStage4NetworkRebuildBatch delegates only to a writer that explicitly
// proves duplicate-safe insert-only replay. This is deliberately not routed
// through WriteBatch(drop_recreate), whose ordinary insert contract is not
// safe after an issued page may already have committed.
func (adapter *sqliteTargetAdapter) WriteStage4NetworkRebuildBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	if adapter == nil {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 rebuild target adapter is not configured",
				),
			)
	}
	normalized, err := adapter.normalizeWriteBatch(table, columns, rows)
	if err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: attempted,
		}, err
	}
	writer := adapter.stage4BatchWriter
	if writer == nil && adapter.database != nil {
		writer = newSQLiteStage4NetworkWriter(adapter.database)
	}
	rebuildWriter, ok := writer.(sqliteStage4NetworkRebuildBatchWriter)
	if !ok || rebuildWriter == nil {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 rebuild batch writer is not configured",
				),
			)
	}
	return rebuildWriter.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		mode,
		normalized,
	)
}

func (adapter *sqliteTargetAdapter) normalizeWriteBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	if adapter.sourceEngine == "postgres" {
		normalized, err := normalizePostgresSQLiteBatch(
			table,
			columns,
			rows,
		)
		if err != nil {
			return nil, err
		}
		rows = normalized
	}
	if adapter.sourceEngine == "mysql" {
		normalized, err := normalizeMySQLSQLiteBatch(
			table,
			columns,
			rows,
		)
		if err != nil {
			return nil, err
		}
		rows = normalized
	}
	if adapter.sqlServerRoute {
		normalized, err := normalizeSQLServerSQLiteBatch(
			table,
			columns,
			rows,
		)
		if err != nil {
			return nil, err
		}
		rows = normalized
	}
	return rows, nil
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
