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

// SQLiteToSQLite migrates user tables and verifies each target row count.
func SQLiteToSQLite(ctx context.Context, cfg config.Config) (Result, error) {
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
		copied, err := copyTable(ctx, source, target, name, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validateCount(ctx, source, target, name); err != nil {
			return Result{}, err
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

func copyTable(ctx context.Context, source, target *sql.DB, name, targetMode string) (int, error) {
	table, names, err := inspectTable(ctx, source, name)
	if err != nil {
		return 0, err
	}
	if err := prepareTarget(ctx, target, table, targetMode); err != nil {
		return 0, err
	}

	rows, err := source.QueryContext(ctx, "SELECT "+quotedColumns(names)+" FROM "+quote(name))
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	defer rows.Close()
	transaction, err := target.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin write for %s: %w", name, err)
	}
	defer transaction.Rollback()
	verb := "INSERT"
	if targetMode == "upsert" {
		verb = "INSERT OR REPLACE"
	}
	statement, err := transaction.PrepareContext(ctx, verb+" INTO "+quote(name)+" ("+quotedColumns(names)+") VALUES ("+placeholders(len(names))+")")
	if err != nil {
		return 0, fmt.Errorf("prepare write for %s: %w", name, err)
	}
	defer statement.Close()

	count, values := 0, make([]any, len(names))
	pointers := make([]any, len(names))
	for index := range values {
		pointers[index] = &values[index]
	}
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
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit %s: %w", name, err)
	}
	return count, nil
}

func prepareTarget(ctx context.Context, target *sql.DB, table schema.Table, targetMode string) error {
	if targetMode == "drop_recreate" {
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
		var position, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := rows.Scan(&position, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return schema.Table{}, nil, err
		}
		table.Columns = append(table.Columns, schema.Column{Name: columnName, Type: columnType, Nullable: notNull == 0, PrimaryKey: primaryKey > 0})
		names = append(names, columnName)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, nil, err
	}
	return table, names, nil
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func quotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quote(column)
	}
	return strings.Join(quoted, ", ")
}
func placeholders(count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = "?"
	}
	return strings.Join(values, ", ")
}
