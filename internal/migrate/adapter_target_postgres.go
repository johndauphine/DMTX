package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresTargetAdapter struct {
	database    *sql.DB
	batchWriter postgresBatchWriter
	namespace   string
}

func (adapter *postgresTargetAdapter) postgresDatabaseHandle() *sql.DB {
	if adapter == nil {
		return nil
	}
	return adapter.database
}

func validatePostgresTargetEndpoint(endpoint config.Endpoint) error {
	_, err := engine.PostgresDSN(endpoint)
	return err
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
	targetTables, err := adapter.planTablesBeforeObjectNameMaterialization(
		sourceEngine,
		sourceTables,
		mode,
	)
	if err != nil {
		return nil, err
	}
	targetTables, err = schema.MaterializePostgresObjectNames(
		targetTables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"materialize PostgreSQL target object names: %w",
			err,
		)
	}
	return targetTables, nil
}

func (adapter *postgresTargetAdapter) PlanTablesAfterPrior(
	sourceEngine string,
	priorSourceTables []schema.Table,
	priorTargetTables []schema.Table,
	priorTargetReservations []TargetSchemaEvolutionNameReservation,
	currentSourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	priorTables, err := adapter.planTablesBeforeObjectNameMaterialization(
		sourceEngine,
		priorSourceTables,
		mode,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"plan PostgreSQL prior target tables: %w",
			err,
		)
	}
	reservations := make(
		[]schema.PostgresObjectNameReservation,
		len(priorTargetReservations),
	)
	for index, reservation := range priorTargetReservations {
		if reservation.Scope != "relation" {
			return nil, fmt.Errorf(
				"PostgreSQL target authority contains unsupported name reservation scope %q",
				reservation.Scope,
			)
		}
		if reservation.Namespace != adapter.namespace {
			return nil, fmt.Errorf(
				"PostgreSQL target relation reservation namespace %q differs from configured target namespace %q",
				reservation.Namespace,
				adapter.namespace,
			)
		}
		if strings.TrimSpace(reservation.Name) == "" ||
			reservation.Name != strings.TrimSpace(reservation.Name) {
			return nil, fmt.Errorf(
				"PostgreSQL target relation reservation %d has a non-canonical name",
				index,
			)
		}
		reservations[index] = schema.PostgresObjectNameReservation{
			Namespace: reservation.Namespace,
			Name:      reservation.Name,
		}
	}
	currentTables, err := adapter.planTablesBeforeObjectNameMaterialization(
		sourceEngine,
		currentSourceTables,
		mode,
	)
	if err != nil {
		return nil, err
	}
	currentTables, err =
		schema.MaterializePostgresObjectNamesAfterPriorAuthority(
			currentTables,
			priorTables,
			priorTargetTables,
			reservations,
			schema.PostgresObjectPlanOptions{},
		)
	if err != nil {
		return nil, fmt.Errorf(
			"materialize PostgreSQL target object names after prior projection: %w",
			err,
		)
	}
	return currentTables, nil
}

func (adapter *postgresTargetAdapter) planTablesBeforeObjectNameMaterialization(
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
		if err := rebaseProjectedForeignKeySchemas(
			sourceTable.Schema,
			adapter.namespace,
			"PostgreSQL",
			&targetTable,
		); err != nil {
			return nil, err
		}
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

func (adapter *postgresTargetAdapter) WriteStage4NetworkBatch(
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
					"PostgreSQL Stage 4 network target adapter is not configured",
				),
			)
	}
	writer, ok := adapter.batchWriter.(postgresStage4NetworkBatchWriter)
	if !ok {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"PostgreSQL Stage 4 network batch writer is not configured",
				),
			)
	}
	return writer.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
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
