package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

// SQLiteToMySQLWithObserver migrates SQLite tables into a MySQL or MariaDB
// target. It uses DMTX-managed schema creation, bounded transactions, and
// exact row-count validation.
func SQLiteToMySQLWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "mysql" {
		return Result{}, fmt.Errorf("SQLite-to-MySQL requires source.type sqlite and target.type mysql")
	}
	if cfg.Source.Database == "" {
		return Result{}, fmt.Errorf("SQLite source database path is required")
	}
	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open SQLite source: %w", err)
	}
	defer source.Close()
	targetEndpoint, err := resolvedEndpoint(cfg.Target)
	if err != nil {
		return Result{}, fmt.Errorf("resolve target: %w", err)
	}
	target, err := engine.OpenMySQL(ctx, targetEndpoint)
	if err != nil {
		return Result{}, err
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
		table, columns, err := inspectTable(ctx, source, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		if err := prepareMySQLTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copySQLiteRowsToMySQL(ctx, source, target, table, columns, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validateSQLiteToMySQLCount(ctx, source, target, table.Name); err != nil {
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

func prepareMySQLTarget(ctx context.Context, target *sql.DB, table schema.Table, mode string) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.MySQL, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop MySQL table %s: %w", table.Name, err)
		}
	}
	var exists int
	err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check MySQL table %s: %w", table.Name, err)
	}
	if exists > 0 {
		return nil
	}
	ddl, err := schema.CreateTable(schema.MySQL, table)
	if err != nil {
		return fmt.Errorf("plan MySQL table %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create MySQL table %s: %w", table.Name, err)
	}
	return nil
}

func copySQLiteRowsToMySQL(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, mode string) (int, error) {
	query := "SELECT " + quotedColumns(columns) + " FROM " + quote(table.Name) + " ORDER BY " + quotedColumns(primaryKeyColumns(table))
	rows, err := source.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("read SQLite table %s: %w", table.Name, err)
	}
	defer rows.Close()
	count := 0
	batch := make([][]any, 0, sqliteWriteBatchSize)
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf("read SQLite table %s: %w", table.Name, err)
		}
		batch = append(batch, append([]any(nil), values...))
		if len(batch) == sqliteWriteBatchSize {
			if err := writeMySQLBatch(ctx, target, table, columns, mode, batch); err != nil {
				return 0, err
			}
			count += len(batch)
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read SQLite table %s: %w", table.Name, err)
	}
	if len(batch) > 0 {
		if err := writeMySQLBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func writeMySQLBatch(ctx context.Context, target *sql.DB, table schema.Table, columns []string, mode string, rows [][]any) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin MySQL write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, mySQLWriteStatement(table, columns, mode))
	if err != nil {
		return fmt.Errorf("prepare MySQL write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("write MySQL table %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit MySQL table %s: %w", table.Name, err)
	}
	return nil
}

func mySQLWriteStatement(table schema.Table, columns []string, mode string) string {
	statement := "INSERT INTO " + mySQLIdentifier(table.Name) + " (" + mySQLQuotedColumns(columns) + ") VALUES (" + placeholders(len(columns)) + ")"
	if mode != "upsert" {
		return statement
	}
	keys := primaryKeyColumns(table)
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(updates, mySQLIdentifier(column)+" = VALUES("+mySQLIdentifier(column)+")")
		}
	}
	if len(updates) == 0 {
		return statement + " ON DUPLICATE KEY UPDATE " + mySQLIdentifier(keys[0]) + " = " + mySQLIdentifier(keys[0])
	}
	return statement + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
}

func validateSQLiteToMySQLCount(ctx context.Context, source, target *sql.DB, name string) error {
	sourceCount, err := countRows(ctx, source, name)
	if err != nil {
		return err
	}
	var targetCount int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLIdentifier(name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count MySQL table %s: %w", name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", name, sourceCount, targetCount)
	}
	return nil
}
