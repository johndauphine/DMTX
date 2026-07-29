package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresTargetAdapter struct {
	database    *sql.DB
	batchWriter postgresBatchWriter
	namespace   string
}

func validatePostgresTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return fmt.Errorf("PostgreSQL host, database, and user are required")
	}
	return nil
}

func openPostgresTargetAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validatePostgresTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve target: %w", err)
	}
	database, err := engine.OpenPostgres(ctx, resolved)
	if err != nil {
		return nil, err
	}
	return &postgresTargetAdapter{
		database:    database,
		batchWriter: newPostgresNativeWriter(database),
		namespace:   postgresTargetNamespace(resolved),
	}, nil
}

func postgresTargetNamespace(endpoint config.Endpoint) string {
	if endpoint.Schema == "" {
		return "public"
	}
	return endpoint.Schema
}

func postgresTargetTable(
	sourceTable schema.Table,
	namespace string,
) schema.Table {
	targetTable := sourceTable
	targetTable.Schema = namespace
	return targetTable
}

func (adapter *postgresTargetAdapter) Engine() string {
	return "postgres"
}

func (adapter *postgresTargetAdapter) PlanTables(
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
		projected, err := projectPostgresSourceTable(
			sourceEngine,
			sourceTable,
		)
		if err != nil {
			return nil, err
		}
		targetTable := postgresTargetTable(projected, adapter.namespace)
		if _, err := schema.DropTable(schema.Postgres, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan PostgreSQL table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateTable(schema.Postgres, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan PostgreSQL table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if err := validatePostgresWriteShape(
			targetTable,
			adapterColumnNames(targetTable),
			mode,
		); err != nil {
			return nil, err
		}
		targetTables = append(targetTables, targetTable)
	}
	if mode == "drop_recreate" {
		if _, err := schema.DropTables(
			schema.Postgres,
			targetTables,
		); err != nil {
			return nil, fmt.Errorf(
				"plan PostgreSQL target table set: %w",
				err,
			)
		}
	}
	if _, err := schema.PlanPostgresDropRecreateObjects(
		targetTables,
		schema.PostgresObjectPlanOptions{},
	); err != nil {
		return nil, fmt.Errorf(
			"plan PostgreSQL post-load objects: %w",
			err,
		)
	}
	return targetTables, nil
}

func (adapter *postgresTargetAdapter) PrepareTables(
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
	return preparePostgresTargets(
		ctx,
		adapter.database,
		targetTables,
	)
}

func (adapter *postgresTargetAdapter) WriteBatch(
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
		}, fmt.Errorf("PostgreSQL native batch writer is not configured")
	}
	return adapter.batchWriter.WriteBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

func (adapter *postgresTargetAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	if err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count PostgreSQL table %s: %w",
			table.Name,
			err,
		)
	}
	return count, nil
}

func (adapter *postgresTargetAdapter) FinalizeTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	return finalizePostgresTargets(
		ctx,
		adapter.database,
		targetTables,
		mode,
	)
}

func (adapter *postgresTargetAdapter) Close() error {
	return adapter.database.Close()
}
