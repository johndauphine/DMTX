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
	database  *sql.DB
	namespace string
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
		database:  database,
		namespace: postgresTargetNamespace(resolved),
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

func (adapter *postgresTargetAdapter) PrepareTable(
	ctx context.Context,
	sourceTable schema.Table,
	mode string,
) (schema.Table, error) {
	targetTable := postgresTargetTable(sourceTable, adapter.namespace)
	if err := preparePostgresTarget(
		ctx,
		adapter.database,
		targetTable,
		mode,
	); err != nil {
		return schema.Table{}, err
	}
	return targetTable, nil
}

func (adapter *postgresTargetAdapter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	return writePostgresBatchReceipt(
		ctx,
		adapter.database,
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

func (adapter *postgresTargetAdapter) Close() error {
	return adapter.database.Close()
}
