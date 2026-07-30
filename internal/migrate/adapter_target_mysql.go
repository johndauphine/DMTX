package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type mysqlTargetAdapter struct {
	database    *sql.DB
	batchWriter mysqlBatchWriter
	namespace   string
}

func (adapter *mysqlTargetAdapter) mySQLDatabaseHandle() *sql.DB {
	if adapter == nil {
		return nil
	}
	return adapter.database
}

func validateMySQLTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return fmt.Errorf("MySQL host, database, and user are required")
	}
	if endpoint.Schema != "" && endpoint.Schema != endpoint.Database {
		return fmt.Errorf(
			"MySQL target schema must be empty or match the target database",
		)
	}
	return nil
}

func openMySQLTargetAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validateMySQLTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve target: %w", err)
	}
	database, err := engine.OpenMySQL80Target(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if err := engine.VerifyMySQL80Target(ctx, database); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"verify MySQL 8.0 target: %w (close: %v)",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("verify MySQL 8.0 target: %w", err)
	}
	return &mysqlTargetAdapter{
		database:    database,
		batchWriter: newMySQLNativeWriter(database),
		namespace:   resolved.Database,
	}, nil
}

func (adapter *mysqlTargetAdapter) Engine() string {
	return "mysql"
}

func (adapter *mysqlTargetAdapter) PlanTables(
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
		targetTable, err := projectMySQLTargetTable(
			sourceEngine,
			sourceTable,
		)
		if err != nil {
			return nil, err
		}
		targetTable.Schema = adapter.namespace
		targetTable, err = schema.AddMySQLForeignKeyIndexes(targetTable)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL table %s foreign-key indexes: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.DropTable(schema.MySQL, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan MySQL table %s drop: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateTable(schema.MySQL, targetTable); err != nil {
			return nil, fmt.Errorf(
				"plan MySQL table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if err := validateMySQLWriteShape(
			targetTable,
			adapterColumnNames(targetTable),
			mode,
		); err != nil {
			return nil, err
		}
		if targetTable.Identity != nil {
			frontier := int64(0)
			if targetTable.Identity.Frontier != nil {
				frontier = *targetTable.Identity.Frontier
			}
			if _, err := schema.MySQLAutoIncrementPlan(
				targetTable,
				frontier,
			); err != nil {
				return nil, fmt.Errorf(
					"plan MySQL table %s identity: %w",
					targetTable.Name,
					err,
				)
			}
		}
		targetTables = append(targetTables, targetTable)
	}
	if _, err := schema.PlanMySQLDropRecreateObjects(
		targetTables,
	); err != nil {
		return nil, fmt.Errorf("plan MySQL post-load objects: %w", err)
	}
	return targetTables, nil
}

func (adapter *mysqlTargetAdapter) PrepareTables(
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
	return prepareMySQLTargets(ctx, adapter.database, targetTables)
}

func (adapter *mysqlTargetAdapter) WriteBatch(
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
		}, fmt.Errorf("MySQL native batch writer is not configured")
	}
	return adapter.batchWriter.WriteBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

func (adapter *mysqlTargetAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	if err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			mySQLQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count MySQL table %s: %w",
			table.Name,
			err,
		)
	}
	return count, nil
}

func (adapter *mysqlTargetAdapter) FinalizeTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	return finalizeMySQLTargets(
		ctx,
		adapter.database,
		targetTables,
		mode,
	)
}

func (adapter *mysqlTargetAdapter) Close() error {
	return adapter.database.Close()
}
