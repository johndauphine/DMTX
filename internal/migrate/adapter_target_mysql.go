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

type mysqlTargetAdapter struct {
	database                      *sql.DB
	batchWriter                   mysqlBatchWriter
	flavor                        engine.MySQLServerFlavor
	namespace                     string
	workloadIdentity              string
	destructiveAcknowledged       bool
	normalizeSQLiteSourceValues   bool
	validateSQLServerSourceValues bool
	// deleteCommit is nil in production. It keeps commit-acknowledgement
	// recovery coverage on the pinned native connection rather than a mock
	// transaction wrapper.
	deleteCommit func(context.Context, *sql.Conn) (sql.Result, error)
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
	switch strings.ToLower(endpoint.Database) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return fmt.Errorf(
			"MySQL target database %q is a reserved system database",
			endpoint.Database,
		)
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
	workloadIdentity, err := config.NetworkEndpointWorkloadIdentity(endpoint)
	if err != nil {
		return nil, fmt.Errorf("identify MySQL target workload: %w", err)
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve target: %w", err)
	}
	database, flavor, err := engine.OpenMySQLTarget(ctx, resolved)
	if err != nil {
		return nil, err
	}
	verifiedFlavor, err := engine.VerifyMySQLTarget(ctx, database)
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"verify MySQL-family target: %w (close: %v)",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("verify MySQL-family target: %w", err)
	}
	if verifiedFlavor != flavor {
		_ = database.Close()
		return nil, fmt.Errorf(
			"verify MySQL-family target: server flavor changed during connection setup",
		)
	}
	return &mysqlTargetAdapter{
		database:         database,
		batchWriter:      newMySQLNativeWriterForFlavor(database, flavor),
		flavor:           flavor,
		namespace:        resolved.Database,
		workloadIdentity: workloadIdentity,
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
			adapter.flavor,
		)
		if err != nil {
			return nil, err
		}
		targetTable.Schema = adapter.namespace
		if err := rebaseProjectedForeignKeySchemas(
			sourceTable.Schema,
			adapter.namespace,
			"MySQL",
			&targetTable,
		); err != nil {
			return nil, err
		}
		targetTables = append(targetTables, targetTable)
	}
	if sourceEngine == "sqlite" {
		targetTables, err = validateSQLiteMySQLTables(
			sourceTables,
			targetTables,
		)
		if err != nil {
			return nil, err
		}
	}
	for index := range targetTables {
		targetTable, addErr := schema.AddMySQLForeignKeyIndexes(
			targetTables[index],
		)
		if addErr != nil {
			return nil, fmt.Errorf(
				"plan MySQL table %s foreign-key indexes: %w",
				targetTables[index].Name,
				addErr,
			)
		}
		targetTables[index] = targetTable
	}
	if sourceEngine == "sqlite" {
		targetTables, err = schema.MaterializeMySQLObjectNames(
			targetTables,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"materialize SQLite to MySQL relational objects: %w",
				err,
			)
		}
	}
	for _, targetTable := range targetTables {
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
		return preflightMySQLRetainedTables(
			ctx,
			adapter.database,
			targetTables,
		)
	}
	return prepareMySQLTargets(
		ctx,
		adapter.database,
		targetTables,
		adapter.flavor,
		adapter.destructiveAcknowledged,
	)
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
	if adapter.normalizeSQLiteSourceValues {
		normalized, err := normalizeSQLiteMySQLBatch(
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
	if adapter.validateSQLServerSourceValues {
		if err := validateMySQLTargetSQLServerBatchValues(
			table,
			columns,
			rows,
		); err != nil {
			return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: int64(len(rows)),
			}, err
		}
	}
	return adapter.batchWriter.WriteBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

// WriteStage4NetworkBatch is the target boundary for a replayable network
// page. It preserves the ordinary source-value normalization but requires a
// native writer that can bind its replay-isolation proof to the exact upsert
// transaction.
func (adapter *mysqlTargetAdapter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	writer, ok := adapter.batchWriter.(mysqlStage4NetworkBatchWriter)
	if !ok || isNilInterface(writer) {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"MySQL Stage 4 network batch writer is not configured",
				),
			)
	}
	if adapter.normalizeSQLiteSourceValues {
		normalized, err := normalizeSQLiteMySQLBatch(
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
	if adapter.validateSQLServerSourceValues {
		if err := validateMySQLTargetSQLServerBatchValues(
			table,
			columns,
			rows,
		); err != nil {
			return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, err
		}
	}
	return writer.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
	)
}

// WriteStage4NetworkRebuildBatch preserves source normalization at the target
// boundary while requiring the native writer's strict-fresh versus
// duplicate-safe insert-only replay contract.
func (adapter *mysqlTargetAdapter) WriteStage4NetworkRebuildBatch(
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
				fmt.Errorf("MySQL Stage 4 rebuild target adapter is not configured"),
			)
	}
	writer, ok := adapter.batchWriter.(mysqlStage4NetworkRebuildBatchWriter)
	if !ok || isNilInterface(writer) {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"MySQL Stage 4 rebuild batch writer is not configured",
				),
			)
	}
	if adapter.normalizeSQLiteSourceValues {
		normalized, err := normalizeSQLiteMySQLBatch(
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
	if adapter.validateSQLServerSourceValues {
		if err := validateMySQLTargetSQLServerBatchValues(
			table,
			columns,
			rows,
		); err != nil {
			return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: attempted,
			}, err
		}
	}
	return writer.WriteStage4NetworkRebuildBatch(
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
