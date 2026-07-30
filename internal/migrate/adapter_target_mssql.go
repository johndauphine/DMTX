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
	database    *sql.DB
	batchWriter sqlServerBatchWriter
	namespace   string
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
	if endpoint.Schema != "" && endpoint.Schema != "dbo" {
		return fmt.Errorf(
			"SQL Server target schema must be empty or dbo",
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
	return &sqlServerTargetAdapter{
		database:    database,
		batchWriter: newSQLServerNativeWriter(database),
		namespace:   namespace,
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
	if _, err := schema.PlanSQLServerDropRecreateObjects(
		targetTables,
	); err != nil {
		return nil, fmt.Errorf(
			"plan SQL Server post-load objects: %w",
			err,
		)
	}
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
	return prepareSQLServerTargets(ctx, adapter.database, targetTables)
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
