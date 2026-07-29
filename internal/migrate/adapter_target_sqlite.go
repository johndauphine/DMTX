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

func (adapter *sqliteTargetAdapter) PlanTable(
	sourceEngine string,
	sourceTable schema.Table,
	mode string,
) (schema.Table, error) {
	if _, err := normalizeAdapterTargetMode(mode); err != nil {
		return schema.Table{}, err
	}
	switch sourceEngine {
	case "postgres", "mysql", "mssql", "sqlite":
	default:
		return schema.Table{}, fmt.Errorf(
			"SQLite target does not support source engine %q",
			sourceEngine,
		)
	}
	targetTable := sourceTable
	targetTable.Schema = ""
	if _, err := schema.DropTable(schema.SQLite, targetTable); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite table %s: %w",
			targetTable.Name,
			err,
		)
	}
	if _, err := schema.CreateTable(schema.SQLite, targetTable); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite table %s: %w",
			targetTable.Name,
			err,
		)
	}
	if _, err := schema.CreateIndexes(schema.SQLite, targetTable); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite indexes for %s: %w",
			targetTable.Name,
			err,
		)
	}
	if _, err := schema.SQLiteSequencePlan(targetTable); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite sequence for %s: %w",
			targetTable.Name,
			err,
		)
	}
	return targetTable, nil
}

func (adapter *sqliteTargetAdapter) PrepareTable(
	ctx context.Context,
	targetTable schema.Table,
	mode string,
) error {
	return prepareTarget(ctx, adapter.database, targetTable, mode)
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

func (adapter *sqliteTargetAdapter) Close() error {
	return adapter.database.Close()
}
