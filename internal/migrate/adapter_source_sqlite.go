package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

type sqliteSourceAdapter struct {
	database                  *sql.DB
	snapshot                  *sql.Tx
	incrementalDeleteMonitor  *sql.Conn
	incrementalDeleteDataVers int64
}

var _ sourceAdapter = (*sqliteSourceAdapter)(nil)

func validateSQLiteSourceEndpoint(endpoint config.Endpoint) error {
	_, err := canonicalSQLiteSourcePath(endpoint)
	return err
}

func canonicalSQLiteSourcePath(endpoint config.Endpoint) (string, error) {
	path, err := config.CanonicalSQLitePath(endpoint.Database)
	if err != nil {
		return "", fmt.Errorf("resolve SQLite source path: %w", err)
	}
	return path, nil
}

func openSQLiteSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (sourceAdapter, error) {
	path, err := canonicalSQLiteSourcePath(endpoint)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite source: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("SQLite source database path is a directory")
	}

	database, err := sql.Open("sqlite", sqliteReadOnlyURI(path))
	if err != nil {
		return nil, fmt.Errorf("open SQLite source: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("open SQLite source: %w", err)
	}
	// An independent read-only connection records the source data version
	// before the retained snapshot starts. SQLite incremental+delete checks it
	// immediately before it scans source keys, so a later source commit cannot
	// be mistaken for part of that retained view.
	monitor, dataVersion, err := openSQLiteIncrementalDeleteMonitor(ctx, database)
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	snapshot, err := database.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		_ = monitor.Close()
		_ = database.Close()
		return nil, fmt.Errorf("begin SQLite source snapshot: %w", err)
	}
	var schemaEntries int
	if err := snapshot.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema`,
	).Scan(&schemaEntries); err != nil {
		_ = snapshot.Rollback()
		_ = monitor.Close()
		_ = database.Close()
		return nil, fmt.Errorf("establish SQLite source snapshot: %w", err)
	}
	return &sqliteSourceAdapter{
		database:                  database,
		snapshot:                  snapshot,
		incrementalDeleteMonitor:  monitor,
		incrementalDeleteDataVers: dataVersion,
	}, nil
}

func openSQLiteIncrementalDeleteMonitor(
	ctx context.Context,
	database *sql.DB,
) (*sql.Conn, int64, error) {
	if database == nil {
		return nil, 0, errors.New("SQLite source database is unavailable")
	}
	monitor, err := database.Conn(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("open SQLite source change monitor: %w", err)
	}
	var dataVersion int64
	if err := monitor.QueryRowContext(ctx, "PRAGMA data_version").Scan(&dataVersion); err != nil {
		_ = monitor.Close()
		return nil, 0, fmt.Errorf("read SQLite source change monitor: %w", err)
	}
	return monitor, dataVersion, nil
}

// VerifyIncrementalDeleteAuthority rejects a fresh source-key scan once a
// writer has committed after the retained source view began. Replays use a
// previously durable delete plan instead of trying to recreate this view.
func (adapter *sqliteSourceAdapter) VerifyIncrementalDeleteAuthority(
	ctx context.Context,
) error {
	if adapter == nil || adapter.snapshot == nil ||
		adapter.incrementalDeleteMonitor == nil {
		return errors.New("SQLite retained incremental delete authority is unavailable")
	}
	if ctx == nil {
		return errors.New("SQLite incremental delete authority context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var current int64
	if err := adapter.incrementalDeleteMonitor.QueryRowContext(
		ctx,
		"PRAGMA data_version",
	).Scan(&current); err != nil {
		return fmt.Errorf("read SQLite incremental delete source change monitor: %w", err)
	}
	if current != adapter.incrementalDeleteDataVers {
		return fmt.Errorf("SQLite source changed after the retained incremental snapshot was established")
	}
	return nil
}

func sqliteReadOnlyURI(path string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{
		Scheme: "file",
		Path:   normalized,
	}
	query := location.Query()
	query.Set("mode", "ro")
	location.RawQuery = query.Encode()
	return location.String()
}

func (adapter *sqliteSourceAdapter) Engine() string {
	return "sqlite"
}

func (adapter *sqliteSourceAdapter) DisplayName() string {
	return "SQLite"
}

func (adapter *sqliteSourceAdapter) ListTables(
	ctx context.Context,
) ([]string, error) {
	rows, err := adapter.snapshot.QueryContext(ctx, `
		SELECT name
		FROM pragma_table_list
		WHERE schema = 'main'
		  AND type = 'table'
		  AND lower(substr(name, 1, 7)) <> 'sqlite_'
		  AND lower(name) <> 'dmtx_internal_delete_batch_receipts'
		ORDER BY name COLLATE BINARY
	`)
	if err != nil {
		return nil, fmt.Errorf("list SQLite source tables: %w", err)
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read SQLite source table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite source tables: %w", err)
	}
	return names, nil
}

func (adapter *sqliteSourceAdapter) InspectTable(
	ctx context.Context,
	name string,
) (schema.Table, error) {
	if strings.EqualFold(name, sqliteDeleteJournalTable) {
		return schema.Table{}, fmt.Errorf(
			"SQLite table %s is private DMTX delete receipt state and is not a migratable source table",
			sqliteDeleteJournalTable,
		)
	}
	if err := rejectSQLiteTableTriggers(ctx, adapter.snapshot, name); err != nil {
		return schema.Table{}, err
	}
	table, _, err := inspectSQLiteSchema(ctx, adapter.snapshot, name)
	if err != nil {
		return schema.Table{}, err
	}
	if err := requireSQLiteReplaySafePrimaryKey(table); err != nil {
		return schema.Table{}, err
	}
	return table, nil
}

func (adapter *sqliteSourceAdapter) OpenRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	projection, err := sqliteSourceProjection(table, columns)
	if err != nil {
		return nil, err
	}
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return nil, fmt.Errorf(
			"table %s has no primary key; deterministic transfer requires a primary key",
			table.Name,
		)
	}
	query := "SELECT " + projection + " FROM " + quote(table.Name) +
		" ORDER BY " + quotedColumns(keys)
	rows, err := adapter.snapshot.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read SQLite table %s: %w", table.Name, err)
	}
	return rows, nil
}

func sqliteSourceProjection(
	table schema.Table,
	columns []string,
) (string, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	projected := make([]string, len(columns))
	for index, name := range columns {
		column, ok := metadata[name]
		if !ok {
			return "", fmt.Errorf(
				"read SQLite table %s: column %s is not present in schema",
				table.Name,
				name,
			)
		}
		quoted := quote(name)
		base := strings.ToLower(strings.TrimSpace(column.Type))
		if column.DeclaredType != nil {
			base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
		}
		if sqliteSourceRequiresTextProjection(base) {
			projected[index] = "CAST(" + quoted + " AS TEXT) AS " + quoted
			continue
		}
		projected[index] = quoted
	}
	if len(projected) == 0 {
		return "", fmt.Errorf(
			"read SQLite table %s: at least one column is required",
			table.Name,
		)
	}
	return strings.Join(projected, ", "), nil
}

func sqliteSourceRequiresTextProjection(base string) bool {
	switch base {
	case "numeric", "decimal", "json",
		"date", "datetime", "timestamp":
		return true
	default:
		return false
	}
}

func (adapter *sqliteSourceAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	if err := adapter.snapshot.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count SQLite table %s: %w", table.Name, err)
	}
	return count, nil
}

func (adapter *sqliteSourceAdapter) Close() error {
	var result error
	if adapter != nil && adapter.snapshot != nil {
		if err := adapter.snapshot.Rollback(); err != nil &&
			!errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(
				result,
				fmt.Errorf("close SQLite source snapshot: %w", err),
			)
		}
	}
	if adapter != nil && adapter.incrementalDeleteMonitor != nil {
		if err := adapter.incrementalDeleteMonitor.Close(); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf("close SQLite source change monitor: %w", err),
			)
		}
	}
	if adapter != nil && adapter.database != nil {
		if err := adapter.database.Close(); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf("close SQLite source database: %w", err),
			)
		}
	}
	return result
}
