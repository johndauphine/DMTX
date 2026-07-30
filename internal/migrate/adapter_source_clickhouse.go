package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type clickHouseSourceAdapter struct {
	database  *sql.DB
	namespace string
}

var _ sourceAdapter = (*clickHouseSourceAdapter)(nil)
var _ adapterSourceRebuildRowOrderer = (*clickHouseSourceAdapter)(nil)

func (adapter *clickHouseSourceAdapter) clickHouseDatabaseHandle() *sql.DB {
	if adapter == nil {
		return nil
	}
	return adapter.database
}

func validateClickHouseSourceEndpoint(endpoint config.Endpoint) error {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return fmt.Errorf(
			"ClickHouse host, database, and user are required",
		)
	}
	switch strings.ToLower(endpoint.Database) {
	case "system", "information_schema":
		return fmt.Errorf(
			"ClickHouse source database %q is a reserved system database",
			endpoint.Database,
		)
	}
	if endpoint.Schema != "" && endpoint.Schema != endpoint.Database {
		return fmt.Errorf(
			"ClickHouse source schema must be empty or match the source database",
		)
	}
	return nil
}

func openClickHouseSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (sourceAdapter, error) {
	if err := validateClickHouseSourceEndpoint(endpoint); err != nil {
		return nil, err
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve ClickHouse source: %w", err)
	}
	database, err := engine.OpenClickHouse(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if err := engine.VerifyClickHouse248Source(
		ctx,
		database,
		resolved.Database,
	); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"%w (close ClickHouse source: %v)",
				err,
				closeErr,
			)
		}
		return nil, err
	}
	return &clickHouseSourceAdapter{
		database:  database,
		namespace: resolved.Database,
	}, nil
}

func (adapter *clickHouseSourceAdapter) Engine() string {
	return "clickhouse"
}

func (adapter *clickHouseSourceAdapter) DisplayName() string {
	return "ClickHouse"
}

func (adapter *clickHouseSourceAdapter) ListTables(
	ctx context.Context,
) ([]string, error) {
	return engine.ListClickHouseTables(
		ctx,
		adapter.database,
		adapter.namespace,
	)
}

func (adapter *clickHouseSourceAdapter) InspectTable(
	ctx context.Context,
	name string,
) (schema.Table, error) {
	return engine.InspectClickHouseTable(
		ctx,
		adapter.database,
		adapter.namespace,
		name,
	)
}

// RebuildRowOrder returns a total value-order over the admitted row shape.
// The ClickHouse sorting key is a prefix, and every remaining source column is
// an explicit tie-breaker. Identical duplicate rows remain indistinguishable
// ties and are preserved; this method makes no uniqueness claim.
func (adapter *clickHouseSourceAdapter) RebuildRowOrder(
	table schema.Table,
) ([]string, error) {
	return clickHouseRebuildRowOrder(table)
}

func clickHouseRebuildRowOrder(
	table schema.Table,
) ([]string, error) {
	if len(table.ClickHouseOrderBy) == 0 {
		return nil, fmt.Errorf(
			"ClickHouse table %s has no admitted ordering key",
			table.Name,
		)
	}
	columns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"ClickHouse table %s has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := columns[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"ClickHouse table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		columns[column.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(table.Columns))
	order := make([]string, 0, len(table.Columns))
	for _, name := range table.ClickHouseOrderBy {
		if _, exists := columns[name]; !exists {
			return nil, fmt.Errorf(
				"ClickHouse table %s ordering column %s is absent",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"ClickHouse table %s has duplicate ordering column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
		order = append(order, name)
	}
	for _, column := range table.Columns {
		if _, exists := seen[column.Name]; exists {
			continue
		}
		seen[column.Name] = struct{}{}
		order = append(order, column.Name)
	}
	return order, nil
}

func (adapter *clickHouseSourceAdapter) OpenRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	query, err := clickHouseSourceReadQuery(table, columns)
	if err != nil {
		return nil, err
	}
	rows, err := adapter.database.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf(
			"read ClickHouse table %s: %w",
			table.Name,
			err,
		)
	}
	return rows, nil
}

func clickHouseSourceReadQuery(
	table schema.Table,
	columns []string,
) (string, error) {
	expectedColumns := adapterColumnNames(table)
	if !reflect.DeepEqual(columns, expectedColumns) {
		return "", fmt.Errorf(
			"read ClickHouse table %s: projected columns must match source column order",
			table.Name,
		)
	}
	order, err := clickHouseRebuildRowOrder(table)
	if err != nil {
		return "", err
	}
	return "SELECT " + quotedColumns(columns) + " FROM " +
		clickHouseQualified(table.Schema, table.Name) +
		" ORDER BY " + quotedColumns(order), nil
}

func (adapter *clickHouseSourceAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count uint64
	if err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			clickHouseQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count ClickHouse table %s: %w",
			table.Name,
			err,
		)
	}
	if count > uint64(math.MaxInt) {
		return 0, fmt.Errorf(
			"count ClickHouse table %s exceeds supported row count",
			table.Name,
		)
	}
	return int(count), nil
}

func (adapter *clickHouseSourceAdapter) Close() error {
	return adapter.database.Close()
}
