package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
	"github.com/johndauphine/DMTX/internal/schema"
	_ "modernc.org/sqlite"
)

// SQLiteToPostgresWithObserver migrates SQLite tables into a PostgreSQL target.
// It uses deterministic DDL, bounded transactional inserts, and per-table count
// validation. Network-source restart frontiers are intentionally handled by the
// shared restartability work rather than being silently approximated here.
func SQLiteToPostgresWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf("SQLite-to-PostgreSQL requires source.type sqlite and target.type postgres")
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
	target, err := engine.OpenPostgres(ctx, targetEndpoint)
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
	targetSchema := cfg.Target.Schema
	if targetSchema == "" {
		targetSchema = "public"
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
		table.Schema = targetSchema
		if err := preparePostgresTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copySQLiteRowsToPostgres(ctx, source, target, table, columns, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validateSQLiteToPostgresCount(ctx, source, target, table); err != nil {
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

func preparePostgresTarget(ctx context.Context, target *sql.DB, table schema.Table, mode string) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.Postgres, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop PostgreSQL table %s: %w", table.Name, err)
		}
	}
	var exists bool
	err := target.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`, table.Schema, table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check PostgreSQL table %s: %w", table.Name, err)
	}
	if exists {
		return nil
	}
	ddl, err := schema.CreateTable(schema.Postgres, table)
	if err != nil {
		return fmt.Errorf("plan PostgreSQL table %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create PostgreSQL table %s: %w", table.Name, err)
	}
	return nil
}

func copySQLiteRowsToPostgres(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, mode string) (int, error) {
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
			if err := writePostgresBatch(ctx, target, table, columns, mode, batch); err != nil {
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
		if err := writePostgresBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func writePostgresBatch(ctx context.Context, target *sql.DB, table schema.Table, columns []string, mode string, rows [][]any) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, postgresWriteStatement(table, columns, mode))
	if err != nil {
		return fmt.Errorf("prepare PostgreSQL write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("write PostgreSQL table %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL table %s: %w", table.Name, err)
	}
	return nil
}

func postgresWriteStatement(table schema.Table, columns []string, mode string) string {
	statement := "INSERT INTO " + postgresQualified(table.Schema, table.Name) + " (" + quotedColumns(columns) + ") VALUES (" + postgresPlaceholders(len(columns)) + ")"
	if mode != "upsert" {
		return statement
	}
	keys := primaryKeyColumns(table)
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(updates, postgresIdentifier(column)+" = EXCLUDED."+postgresIdentifier(column))
		}
	}
	if len(updates) == 0 {
		return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO NOTHING"
	}
	return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO UPDATE SET " + strings.Join(updates, ", ")
}

func postgresPlaceholders(count int) string {
	placeholders := make([]string, count)
	for index := range placeholders {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
	}
	return strings.Join(placeholders, ", ")
}

func validateSQLiteToPostgresCount(ctx context.Context, source, target *sql.DB, table schema.Table) error {
	sourceCount, err := countRows(ctx, source, table.Name)
	if err != nil {
		return err
	}
	var targetCount int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(table.Schema, table.Name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count PostgreSQL table %s: %w", table.Name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", table.Name, sourceCount, targetCount)
	}
	return nil
}
