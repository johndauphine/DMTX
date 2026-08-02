package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// adapterRowCountEstimator is deliberately separate from CountRows. A caller
// may use it only after an exact count timed out; implementations must read
// engine-maintained statistics and must never execute COUNT(*) under this
// name.
type adapterRowCountEstimator interface {
	EstimateRows(context.Context, schema.Table) (int64, error)
}

func (adapter *relationalSourceAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("source row-count estimate is unavailable")
	}
	namespace := table.Schema
	if namespace == "" {
		namespace = adapter.namespace
	}
	switch adapter.spec.engine {
	case "postgres":
		return estimatePostgresRows(
			ctx,
			adapter.database,
			namespace,
			table.Name,
		)
	case "mysql":
		return estimateMySQLRows(
			ctx,
			adapter.database,
			namespace,
			table.Name,
		)
	case "mssql":
		return estimateSQLServerRows(
			ctx,
			adapter.database,
			namespace,
			table.Name,
		)
	default:
		return 0, fmt.Errorf(
			"source row-count estimate is unavailable for engine %q",
			adapter.spec.engine,
		)
	}
}

func (adapter *postgresTargetAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	namespace := table.Schema
	if namespace == "" && adapter != nil {
		namespace = adapter.namespace
	}
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("PostgreSQL target row-count estimate is unavailable")
	}
	return estimatePostgresRows(
		ctx,
		adapter.database,
		namespace,
		table.Name,
	)
}

func (adapter *mysqlTargetAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	namespace := table.Schema
	if namespace == "" && adapter != nil {
		namespace = adapter.namespace
	}
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("MySQL target row-count estimate is unavailable")
	}
	return estimateMySQLRows(
		ctx,
		adapter.database,
		namespace,
		table.Name,
	)
}

func (adapter *sqlServerTargetAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	namespace := table.Schema
	if namespace == "" && adapter != nil {
		namespace = adapter.namespace
	}
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("SQL Server target row-count estimate is unavailable")
	}
	return estimateSQLServerRows(
		ctx,
		adapter.database,
		namespace,
		table.Name,
	)
}

func (adapter *sqliteSourceAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	if adapter == nil || adapter.snapshot == nil {
		return 0, fmt.Errorf("SQLite source row-count estimate is unavailable")
	}
	return estimateSQLiteRows(ctx, adapter.snapshot, table.Name)
}

func (adapter *sqliteTargetAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("SQLite target row-count estimate is unavailable")
	}
	return estimateSQLiteRows(ctx, adapter.database, table.Name)
}

func (adapter *clickHouseSourceAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	namespace := table.Schema
	if namespace == "" && adapter != nil {
		namespace = adapter.namespace
	}
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("ClickHouse source row-count estimate is unavailable")
	}
	return estimateClickHouseRows(
		ctx,
		adapter.database,
		namespace,
		table.Name,
	)
}

func (adapter *clickHouseTargetAdapter) EstimateRows(
	ctx context.Context,
	table schema.Table,
) (int64, error) {
	namespace := table.Schema
	if namespace == "" && adapter != nil {
		namespace = adapter.namespace
	}
	if adapter == nil || adapter.database == nil {
		return 0, fmt.Errorf("ClickHouse target row-count estimate is unavailable")
	}
	return estimateClickHouseRows(
		ctx,
		adapter.database,
		namespace,
		table.Name,
	)
}

func estimatePostgresRows(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) (int64, error) {
	if namespace == "" || table == "" {
		return 0, fmt.Errorf(
			"PostgreSQL row-count estimate requires a schema and table",
		)
	}
	var estimate float64
	err := database.QueryRowContext(
		ctx,
		`SELECT relation.reltuples::double precision
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace
  ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = $1
  AND relation.relname = $2
  AND relation.relkind IN ('r', 'p')`,
		namespace,
		table,
	).Scan(&estimate)
	if err != nil {
		return 0, fmt.Errorf(
			"estimate PostgreSQL table %s: %w",
			table,
			err,
		)
	}
	if math.IsNaN(estimate) ||
		math.IsInf(estimate, 0) ||
		estimate < 0 ||
		estimate >= float64(math.MaxInt64) {
		return 0, fmt.Errorf(
			"estimate PostgreSQL table %s: catalog statistics are unavailable",
			table,
		)
	}
	return int64(math.Round(estimate)), nil
}

func estimateMySQLRows(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) (int64, error) {
	if namespace == "" || table == "" {
		return 0, fmt.Errorf(
			"MySQL row-count estimate requires a database and table",
		)
	}
	var estimate sql.NullInt64
	err := database.QueryRowContext(
		ctx,
		`SELECT TABLE_ROWS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ?
  AND TABLE_NAME = ?
  AND TABLE_TYPE = 'BASE TABLE'`,
		namespace,
		table,
	).Scan(&estimate)
	if err != nil {
		return 0, fmt.Errorf(
			"estimate MySQL table %s: %w",
			table,
			err,
		)
	}
	if !estimate.Valid || estimate.Int64 < 0 {
		return 0, fmt.Errorf(
			"estimate MySQL table %s: catalog statistics are unavailable",
			table,
		)
	}
	return estimate.Int64, nil
}

func estimateSQLServerRows(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) (int64, error) {
	if namespace == "" || table == "" {
		return 0, fmt.Errorf(
			"SQL Server row-count estimate requires a schema and table",
		)
	}
	var estimate sql.NullInt64
	err := database.QueryRowContext(
		ctx,
		`SELECT SUM(CONVERT(bigint, source_partition.rows))
FROM sys.partitions AS source_partition
WHERE source_partition.object_id = OBJECT_ID(QUOTENAME(@p1) + N'.' + QUOTENAME(@p2))
  AND source_partition.index_id IN (0, 1)`,
		namespace,
		table,
	).Scan(&estimate)
	if err != nil {
		return 0, fmt.Errorf(
			"estimate SQL Server table %s: %w",
			table,
			err,
		)
	}
	if !estimate.Valid || estimate.Int64 < 0 {
		return 0, fmt.Errorf(
			"estimate SQL Server table %s: catalog statistics are unavailable",
			table,
		)
	}
	return estimate.Int64, nil
}

type sqliteEstimateQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func estimateSQLiteRows(
	ctx context.Context,
	queryer sqliteEstimateQueryer,
	table string,
) (int64, error) {
	if table == "" {
		return 0, fmt.Errorf(
			"SQLite row-count estimate requires a table",
		)
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT source_stat.stat
FROM sqlite_stat1 AS source_stat
LEFT JOIN pragma_index_list(?) AS source_index
  ON source_index.name = source_stat.idx
WHERE source_stat.tbl = ?
  AND (
    source_stat.idx IS NULL
    OR (
      source_index.name IS NOT NULL
      AND source_index.partial = 0
    )
  )
ORDER BY CASE WHEN source_stat.idx IS NULL THEN 0 ELSE 1 END,
         source_stat.idx`,
		table,
		table,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"estimate SQLite table %s: ANALYZE statistics are unavailable: %w",
			table,
			err,
		)
	}
	defer rows.Close()
	var (
		estimate int64
		found    bool
	)
	for rows.Next() {
		var statistics string
		if err := rows.Scan(&statistics); err != nil {
			return 0, fmt.Errorf(
				"estimate SQLite table %s: read ANALYZE statistics: %w",
				table,
				err,
			)
		}
		fields := strings.Fields(statistics)
		if len(fields) == 0 {
			return 0, fmt.Errorf(
				"estimate SQLite table %s: ANALYZE statistics are malformed",
				table,
			)
		}
		value, parseErr := strconv.ParseInt(fields[0], 10, 64)
		if parseErr != nil || value < 0 {
			return 0, fmt.Errorf(
				"estimate SQLite table %s: ANALYZE statistics are malformed",
				table,
			)
		}
		if found && value != estimate {
			return 0, fmt.Errorf(
				"estimate SQLite table %s: full-table ANALYZE statistics disagree",
				table,
			)
		}
		estimate = value
		found = true
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf(
			"estimate SQLite table %s: read ANALYZE statistics: %w",
			table,
			err,
		)
	}
	if !found {
		return 0, fmt.Errorf(
			"estimate SQLite table %s: full-table ANALYZE statistics are unavailable",
			table,
		)
	}
	return estimate, nil
}

func estimateClickHouseRows(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) (int64, error) {
	if namespace == "" || table == "" {
		return 0, fmt.Errorf(
			"ClickHouse row-count estimate requires a database and table",
		)
	}
	var estimate sql.NullInt64
	err := database.QueryRowContext(
		ctx,
		`SELECT total_rows
FROM system.tables
WHERE database = ?
  AND name = ?
  AND is_temporary = 0`,
		namespace,
		table,
	).Scan(&estimate)
	if err != nil {
		return 0, fmt.Errorf(
			"estimate ClickHouse table %s: %w",
			table,
			err,
		)
	}
	if !estimate.Valid || estimate.Int64 < 0 {
		return 0, fmt.Errorf(
			"estimate ClickHouse table %s: catalog statistics are unavailable",
			table,
		)
	}
	return estimate.Int64, nil
}

var (
	_ adapterRowCountEstimator = (*relationalSourceAdapter)(nil)
	_ adapterRowCountEstimator = (*postgresTargetAdapter)(nil)
	_ adapterRowCountEstimator = (*mysqlTargetAdapter)(nil)
	_ adapterRowCountEstimator = (*sqlServerTargetAdapter)(nil)
	_ adapterRowCountEstimator = (*sqliteSourceAdapter)(nil)
	_ adapterRowCountEstimator = (*sqliteTargetAdapter)(nil)
	_ adapterRowCountEstimator = (*clickHouseSourceAdapter)(nil)
	_ adapterRowCountEstimator = (*clickHouseTargetAdapter)(nil)
)
