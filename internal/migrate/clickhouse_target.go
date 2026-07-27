package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
	"github.com/johndauphine/DMTX/internal/schema"
	_ "modernc.org/sqlite"
)

// SQLiteToClickHouseWithObserver migrates SQLite tables into ClickHouse with
// deterministic MergeTree DDL, bounded inserts, and count validation. The
// engine capability contract rejects upsert because ClickHouse does not offer
// the relational uniqueness semantics DMTX requires for that mode.
func SQLiteToClickHouseWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "clickhouse" {
		return Result{}, fmt.Errorf("SQLite-to-ClickHouse requires source.type sqlite and target.type clickhouse")
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
	target, err := engine.OpenClickHouse(ctx, targetEndpoint)
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
		table.Schema = cfg.Target.Database
		if err := prepareClickHouseTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copySQLiteRowsToClickHouse(ctx, source, target, table, columns)
		if err != nil {
			return Result{}, err
		}
		if err := validateSQLiteToClickHouseCount(ctx, source, target, table); err != nil {
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

func prepareClickHouseTarget(ctx context.Context, target *sql.DB, table schema.Table, mode string) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.ClickHouse, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop ClickHouse table %s: %w", table.Name, err)
		}
	}
	var exists int
	err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM system.tables WHERE database = ? AND name = ?`, table.Schema, table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check ClickHouse table %s: %w", table.Name, err)
	}
	if exists > 0 {
		return nil
	}
	ddl, err := schema.CreateTable(schema.ClickHouse, table)
	if err != nil {
		return fmt.Errorf("plan ClickHouse table %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create ClickHouse table %s: %w", table.Name, err)
	}
	return nil
}

func copySQLiteRowsToClickHouse(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string) (int, error) {
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
			if err := writeClickHouseBatch(ctx, target, table, columns, batch); err != nil {
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
		if err := writeClickHouseBatch(ctx, target, table, columns, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func writeClickHouseBatch(ctx context.Context, target *sql.DB, table schema.Table, columns []string, rows [][]any) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ClickHouse write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, "INSERT INTO "+clickHouseQualified(table.Schema, table.Name)+" ("+quotedColumns(columns)+") VALUES ("+placeholders(len(columns))+")")
	if err != nil {
		return fmt.Errorf("prepare ClickHouse write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("write ClickHouse table %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ClickHouse table %s: %w", table.Name, err)
	}
	return nil
}

func validateSQLiteToClickHouseCount(ctx context.Context, source, target *sql.DB, table schema.Table) error {
	sourceCount, err := countRows(ctx, source, table.Name)
	if err != nil {
		return err
	}
	var targetCount int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+clickHouseQualified(table.Schema, table.Name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count ClickHouse table %s: %w", table.Name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", table.Name, sourceCount, targetCount)
	}
	return nil
}

func clickHouseQualified(database, name string) string {
	return quote(database) + "." + quote(name)
}
