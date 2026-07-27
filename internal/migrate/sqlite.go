// Package migrate contains database-to-database migration services.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
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

type TableObserver interface {
	BeforeTable(context.Context, string) error
	AfterTable(context.Context, string, int) error
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
	result := Result{Validated: true}
	for _, name := range names {
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return Result{}, fmt.Errorf("checkpoint before %s: %w", name, err)
			}
		}
		copied, err := copyTable(ctx, source, target, name, cfg.Migration.TargetMode)
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

func copyTable(ctx context.Context, source, target *sql.DB, name, mode string) (int, error) {
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
	rows, err := source.QueryContext(ctx, "SELECT "+quotedColumns(columns)+" FROM "+quote(name))
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	defer rows.Close()
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin write for %s: %w", name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, writeStatement(table, columns, mode))
	if err != nil {
		return 0, fmt.Errorf("prepare write for %s: %w", name, err)
	}
	defer statement.Close()
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	count := 0
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf("scan %s: %w", name, err)
		}
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return 0, fmt.Errorf("write %s: %w", name, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit %s: %w", name, err)
	}
	return count, nil
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
