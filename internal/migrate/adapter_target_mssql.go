package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type sqlServerTargetAdapter struct {
	database                *sql.DB
	batchWriter             sqlServerBatchWriter
	namespace               string
	workloadIdentity        string
	sourceEngine            string
	destructiveAcknowledged bool
	// Delete-reconciliation test seams deliberately operate on the pinned
	// connection so commit-acknowledgement and catalog-race recovery exercises
	// the production transaction rather than a mock transaction wrapper.
	deleteCommit           func(context.Context, *sql.Conn) (sql.Result, error)
	deleteAfterReservation func(context.Context, *sql.Conn) error
}

func (adapter *sqlServerTargetAdapter) sqlServerDatabaseHandle() *sql.DB {
	if adapter == nil {
		return nil
	}
	return adapter.database
}

func validateSQLServerTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return fmt.Errorf(
			"SQL Server host, database, and user are required",
		)
	}
	return nil
}

func openSQLServerTargetAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validateSQLServerTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	workloadIdentity, err := config.NetworkEndpointWorkloadIdentity(endpoint)
	if err != nil {
		return nil, fmt.Errorf("identify SQL Server target workload: %w", err)
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve SQL Server target: %w", err)
	}
	database, err := engine.OpenSQLServer2022Target(ctx, resolved)
	if err != nil {
		return nil, err
	}
	namespace := resolved.Schema
	if namespace == "" {
		namespace = "dbo"
	}
	if isSQLServerDeleteJournalNamespace(namespace) {
		_ = database.Close()
		return nil, fmt.Errorf("SQL Server target schema %q is reserved for DMTX delete receipt evidence", namespace)
	}
	return &sqlServerTargetAdapter{
		database:         database,
		batchWriter:      newSQLServerNativeWriter(database),
		namespace:        namespace,
		workloadIdentity: workloadIdentity,
	}, nil
}

func (adapter *sqlServerTargetAdapter) Engine() string {
	return "mssql"
}

func (adapter *sqlServerTargetAdapter) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return nil, err
	}
	adapter.sourceEngine = ""
	targetTables := make([]schema.Table, 0, len(sourceTables))
	for _, sourceTable := range sourceTables {
		targetTable, err := projectSQLServerTargetTable(
			sourceEngine,
			sourceTable,
		)
		if err != nil {
			return nil, err
		}
		targetTable.Schema = adapter.namespace
		if err := rebaseProjectedForeignKeySchemas(
			sourceTable.Schema,
			adapter.namespace,
			"SQL Server",
			&targetTable,
		); err != nil {
			return nil, err
		}
		if _, err := schema.DropSQLServerTable(targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQL Server table %s drop: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateSQLServerTable(targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan SQL Server table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if err := validateSQLServerWriteShape(
			targetTable,
			adapterColumnNames(targetTable),
			mode,
		); err != nil {
			return nil, err
		}
		targetTables = append(targetTables, targetTable)
	}
	if sourceEngine == "sqlite" {
		if err := validateSQLiteSQLServerTables(
			sourceTables,
			targetTables,
		); err != nil {
			return nil, err
		}
	}
	if _, err := schema.PlanSQLServerDropRecreateObjects(
		targetTables,
	); err != nil {
		return nil, fmt.Errorf(
			"plan SQL Server post-load objects: %w",
			err,
		)
	}
	adapter.sourceEngine = sourceEngine
	return targetTables, nil
}

func (adapter *sqlServerTargetAdapter) PrepareTables(
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
	return prepareSQLServerTargets(
		ctx,
		adapter.database,
		targetTables,
		adapter.destructiveAcknowledged,
	)
}

func (adapter *sqlServerTargetAdapter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	if adapter.batchWriter == nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: int64(len(rows)),
		}, fmt.Errorf("SQL Server native batch writer is not configured")
	}
	if adapter.sourceEngine == "sqlite" {
		normalized, err := normalizeSQLiteSQLServerBatch(
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
		rows = normalized
	}
	if err := validateSQLServerTargetBatchValues(
		table,
		columns,
		rows,
	); err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: int64(len(rows)),
		}, err
	}
	return adapter.batchWriter.WriteBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

// WriteStage4NetworkBatch preserves source normalization while requiring the
// native writer to fence and re-prove replay isolation inside the exact page
// transaction.
func (adapter *sqlServerTargetAdapter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	writer, ok := adapter.batchWriter.(sqlServerStage4NetworkBatchWriter)
	if !ok || isNilInterface(writer) {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQL Server Stage 4 network batch writer is not configured",
				),
			)
	}
	if adapter.sourceEngine == "sqlite" {
		normalized, err := normalizeSQLiteSQLServerBatch(
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
		rows = normalized
	}
	if err := validateSQLServerTargetBatchValues(
		table,
		columns,
		rows,
	); err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: attempted,
		}, err
	}
	return writer.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
	)
}

// WriteStage4NetworkRebuildBatch keeps source normalization and target-value
// checks identical to the ordinary adapter boundary, but delegates only to a
// writer that distinguishes strict fresh inserts from insert-only replay.
func (adapter *sqlServerTargetAdapter) WriteStage4NetworkRebuildBatch(
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
					"SQL Server Stage 4 rebuild target adapter is not configured",
				),
			)
	}
	writer, ok := adapter.batchWriter.(sqlServerStage4NetworkRebuildBatchWriter)
	if !ok || isNilInterface(writer) {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQL Server Stage 4 rebuild batch writer is not configured",
				),
			)
	}
	if adapter.sourceEngine == "sqlite" {
		normalized, err := normalizeSQLiteSQLServerBatch(
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
		rows = normalized
	}
	if err := validateSQLServerTargetBatchValues(
		table,
		columns,
		rows,
	); err != nil {
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: attempted,
		}, err
	}
	return writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

func (adapter *sqlServerTargetAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	if err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count SQL Server table %s: %w",
			table.Name,
			err,
		)
	}
	return count, nil
}

func (adapter *sqlServerTargetAdapter) FinalizeTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	return finalizeSQLServerTargets(
		ctx,
		adapter.database,
		targetTables,
		mode,
	)
}

func (adapter *sqlServerTargetAdapter) Close() error {
	return adapter.database.Close()
}
