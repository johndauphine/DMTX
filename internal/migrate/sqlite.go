// Package migrate contains database-to-database migration services.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/schema"
	_ "modernc.org/sqlite"
)

type Result struct {
	Tables    int  `json:"tables"`
	Rows      int  `json:"rows"`
	Validated bool `json:"validated"`
}

const sqliteWriteBatchSize = 500

type TableObserver interface {
	BeforeTable(context.Context, string) error
	AfterTable(context.Context, string, int) error
}

// PageObserver records only a target-acknowledged integer-keyset frontier.
// It is optional so table-level observers remain compatible.
type PageObserver interface {
	AfterIntegerKeysetPage(context.Context, string, int, int64) error
}

// TableProgress is the durable, reusable portion of an incomplete table.
type TableProgress struct {
	RowsDone         int
	IntegerWatermark *int64
}

func SQLiteToSQLite(ctx context.Context, cfg config.Config) (Result, error) {
	return SQLiteToSQLiteWithObserver(ctx, cfg, nil)
}

func SQLiteToSQLiteWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("SQLite first pass requires source.type and target.type to be sqlite")
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" {
		return Result{}, fmt.Errorf("SQLite source and target database paths are required")
	}
	if cfg.Source.Database == cfg.Target.Database {
		return Result{}, fmt.Errorf("source and target SQLite databases must differ")
	}
	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open source: %w", err)
	}
	defer source.Close()
	target, err := sql.Open("sqlite", cfg.Target.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open target: %w", err)
	}
	defer target.Close()
	names, err := userTables(ctx, source)
	if err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	result := Result{Validated: true}
	for _, name := range names {
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return Result{}, fmt.Errorf("checkpoint before %s: %w", name, err)
			}
		}
		copied, err := copyTable(ctx, source, target, name, cfg.Migration.TargetMode, observer, TableProgress{})
		if err != nil {
			return Result{}, err
		}
		if err := validateCount(ctx, source, target, name); err != nil {
			return Result{}, err
		}
		if observer != nil {
			if err := observer.AfterTable(ctx, name, copied); err != nil {
				return Result{}, fmt.Errorf("checkpoint after %s: %w", name, err)
			}
		}
		result.Tables++
		result.Rows += copied
	}
	return result, nil
}

func selectedTables(names []string, cfg config.Config) ([]string, error) {
	selected, err := config.SelectTables(names, cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables)
	if err != nil {
		return nil, fmt.Errorf("select source tables: %w", err)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no source tables match migration filters")
	}
	return selected, nil
}

func userTables(ctx context.Context, database *sql.DB) ([]string, error) {
	rows, err := database.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source tables: %w", err)
	}
	return names, nil
}

func copyTable(ctx context.Context, source, target *sql.DB, name, mode string, observer TableObserver, progress TableProgress) (int, error) {
	table, columns, err := inspectTable(ctx, source, name)
	if err != nil {
		return 0, err
	}
	if !hasPrimaryKey(table) {
		return 0, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
	}
	if err := prepareTarget(ctx, target, table, mode); err != nil {
		return 0, err
	}
	if key, ok := integerPrimaryKey(table); ok {
		return copyIntegerKeyset(ctx, source, target, table, columns, key, mode, observer, progress)
	}
	return copyOrderedRows(ctx, source, target, table, columns, mode)
}

func copyIntegerKeyset(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, key string, mode string, observer TableObserver, progress TableProgress) (int, error) {
	keyIndex := columnIndex(columns, key)
	if keyIndex == -1 {
		return 0, fmt.Errorf("integer primary key %s is not a selected column", key)
	}

	var lowerBound int64
	hasLowerBound := progress.IntegerWatermark != nil
	if hasLowerBound {
		lowerBound = *progress.IntegerWatermark
	}
	count := 0
	for {
		query := "SELECT " + quotedColumns(columns) + " FROM " + quote(table.Name)
		arguments := make([]any, 0, 2)
		if hasLowerBound {
			query += " WHERE " + quote(key) + " > ?"
			arguments = append(arguments, lowerBound)
		}
		query += " ORDER BY " + quote(key) + " LIMIT ?"
		arguments = append(arguments, sqliteWriteBatchSize)

		rows, err := source.QueryContext(ctx, query, arguments...)
		if err != nil {
			return 0, fmt.Errorf("read %s keyset page: %w", table.Name, err)
		}
		batch, lastKey, err := scanPage(rows, len(columns), keyIndex)
		rows.Close()
		if err != nil {
			return 0, fmt.Errorf("read %s keyset page: %w", table.Name, err)
		}
		if len(batch) == 0 {
			return count, nil
		}
		if err := writeBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
		if pageObserver, ok := observer.(PageObserver); ok {
			if err := pageObserver.AfterIntegerKeysetPage(ctx, table.Name, progress.RowsDone+count, lastKey); err != nil {
				return 0, fmt.Errorf("checkpoint page for %s: %w", table.Name, err)
			}
		}
		lowerBound = lastKey
		hasLowerBound = true
	}
}

func copyOrderedRows(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, mode string) (int, error) {
	count := 0
	for lowerRow := 0; ; lowerRow += sqliteWriteBatchSize {
		batch, err := readRowNumberPage(ctx, source, table, columns, lowerRow)
		if err != nil {
			return 0, err
		}
		if len(batch) == 0 {
			return count, nil
		}
		if err := writeBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
}

func readRowNumberPage(ctx context.Context, source *sql.DB, table schema.Table, columns []string, lowerRow int) ([][]any, error) {
	order := quotedColumns(primaryKeyColumns(table))
	query := "SELECT " + quotedColumns(columns) + " FROM (SELECT " + quotedColumns(columns) + ", ROW_NUMBER() OVER (ORDER BY " + order + ") AS dmtx_row_number FROM " + quote(table.Name) + ") WHERE dmtx_row_number > ? AND dmtx_row_number <= ? ORDER BY dmtx_row_number"
	rows, err := source.QueryContext(ctx, query, lowerRow, lowerRow+sqliteWriteBatchSize)
	if err != nil {
		return nil, fmt.Errorf("read %s row-number page: %w", table.Name, err)
	}
	defer rows.Close()
	batch, _, err := scanPage(rows, len(columns), -1)
	if err != nil {
		return nil, fmt.Errorf("read %s row-number page: %w", table.Name, err)
	}
	return batch, nil
}

func scanPage(rows *sql.Rows, columnCount, keyIndex int) ([][]any, int64, error) {
	values, pointers := make([]any, columnCount), make([]any, columnCount)
	for index := range values {
		pointers[index] = &values[index]
	}
	batch := make([][]any, 0, sqliteWriteBatchSize)
	var lastKey int64
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return nil, 0, err
		}
		var key int64
		if keyIndex >= 0 {
			var conversionErr error
			key, conversionErr = sqliteIntegerValue(values[keyIndex])
			if conversionErr != nil {
				return nil, 0, conversionErr
			}
		}
		batch = append(batch, append([]any(nil), values...))
		lastKey = key
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return batch, lastKey, nil
}

func integerPrimaryKey(table schema.Table) (string, bool) {
	keys := primaryKeyColumns(table)
	if len(keys) != 1 {
		return "", false
	}
	for _, column := range table.Columns {
		if column.Name == keys[0] && strings.Contains(strings.ToUpper(column.Type), "INT") {
			return column.Name, true
		}
	}
	return "", false
}

func columnIndex(columns []string, name string) int {
	for index, column := range columns {
		if column == name {
			return index
		}
	}
	return -1
}

func sqliteIntegerValue(value any) (int64, error) {
	switch number := value.(type) {
	case int64:
		return number, nil
	case int:
		return int64(number), nil
	case int32:
		return int64(number), nil
	case uint64:
		if number > math.MaxInt64 {
			return 0, fmt.Errorf("integer primary key exceeds signed 64-bit range")
		}
		return int64(number), nil
	default:
		return 0, fmt.Errorf("integer primary key has unexpected value type %T", value)
	}
}

func writeBatch(ctx context.Context, target *sql.DB, table schema.Table, columns []string, mode string, rows [][]any) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, writeStatement(table, columns, mode))
	if err != nil {
		return fmt.Errorf("prepare write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("write %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", table.Name, err)
	}
	return nil
}

func writeStatement(table schema.Table, columns []string, mode string) string {
	statement := "INSERT INTO " + quote(table.Name) + " (" + quotedColumns(columns) + ") VALUES (" + placeholders(len(columns)) + ")"
	if mode != "upsert" {
		return statement
	}
	keys := primaryKeyColumns(table)
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(updates, quote(column)+" = excluded."+quote(column))
		}
	}
	if len(updates) == 0 {
		return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO NOTHING"
	}
	return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO UPDATE SET " + strings.Join(updates, ", ")
}

func primaryKeyColumns(table schema.Table) []string {
	var keys []string
	for _, column := range table.Columns {
		if column.PrimaryKey {
			keys = append(keys, column.Name)
		}
	}
	return keys
}
func hasPrimaryKey(table schema.Table) bool { return len(primaryKeyColumns(table)) > 0 }
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func prepareTarget(ctx context.Context, target *sql.DB, table schema.Table, mode string) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.SQLite, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop %s: %w", table.Name, err)
		}
	}
	exists, err := tableExists(ctx, target, table.Name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	ddl, err := schema.CreateTable(schema.SQLite, table)
	if err != nil {
		return fmt.Errorf("plan %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create %s: %w", table.Name, err)
	}
	return nil
}
func tableExists(ctx context.Context, database *sql.DB, name string) (bool, error) {
	var count int
	err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&count)
	return count > 0, err
}
func validateCount(ctx context.Context, source, target *sql.DB, name string) error {
	sourceCount, err := countRows(ctx, source, name)
	if err != nil {
		return err
	}
	targetCount, err := countRows(ctx, target, name)
	if err != nil {
		return err
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", name, sourceCount, targetCount)
	}
	return nil
}
func countRows(ctx context.Context, database *sql.DB, name string) (int, error) {
	var count int
	err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(name)).Scan(&count)
	return count, err
}
func inspectTable(ctx context.Context, database *sql.DB, name string) (schema.Table, []string, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA table_info("+quote(name)+")")
	if err != nil {
		return schema.Table{}, nil, fmt.Errorf("inspect %s: %w", name, err)
	}
	defer rows.Close()
	table := schema.Table{Name: name}
	var names []string
	for rows.Next() {
		var pos, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := rows.Scan(&pos, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return schema.Table{}, nil, err
		}
		table.Columns = append(table.Columns, schema.Column{Name: columnName, Type: columnType, Nullable: notNull == 0, PrimaryKey: primaryKey > 0})
		names = append(names, columnName)
	}
	return table, names, rows.Err()
}
func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func quotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quote(column)
	}
	return strings.Join(quoted, ", ")
}
func placeholders(count int) string {
	values := make([]string, count)
	for i := range values {
		values[i] = "?"
	}
	return strings.Join(values, ", ")
}
