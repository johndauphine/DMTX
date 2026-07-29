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

// SQLiteToSQLServerWithObserver migrates SQLite tables into a SQL Server
// target using deterministic DDL, bounded transactions, and exact row counts.
func SQLiteToSQLServerWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "mssql" {
		return Result{}, fmt.Errorf("SQLite-to-SQL-Server requires source.type sqlite and target.type mssql")
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
	target, err := engine.OpenSQLServer(ctx, targetEndpoint)
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
		targetSchema = "dbo"
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
		if err := prepareSQLServerTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copySQLiteRowsToSQLServer(ctx, source, target, table, columns, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validateSQLiteToSQLServerCount(ctx, source, target, table); err != nil {
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

func prepareSQLServerTarget(ctx context.Context, target *sql.DB, table schema.Table, mode string) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.SQLServer, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("drop SQL Server table %s: %w", table.Name, err)
		}
	}
	var exists int
	err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @p1 AND table_name = @p2`, table.Schema, table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check SQL Server table %s: %w", table.Name, err)
	}
	if exists > 0 {
		return nil
	}
	ddl, err := schema.CreateTable(schema.SQLServer, table)
	if err != nil {
		return fmt.Errorf("plan SQL Server table %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create SQL Server table %s: %w", table.Name, err)
	}
	return nil
}

func copySQLiteRowsToSQLServer(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, mode string) (int, error) {
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
			if err := writeSQLServerBatch(ctx, target, table, columns, mode, batch); err != nil {
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
		if err := writeSQLServerBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func writeSQLServerBatch(ctx context.Context, target *sql.DB, table schema.Table, columns []string, mode string, rows [][]any) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQL Server write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, sqlServerWriteStatement(table, columns, mode))
	if err != nil {
		return fmt.Errorf("prepare SQL Server write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("write SQL Server table %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQL Server table %s: %w", table.Name, err)
	}
	return nil
}

func sqlServerWriteStatement(table schema.Table, columns []string, mode string) string {
	qualified := sqlServerQualified(table.Schema, table.Name)
	if mode != "upsert" {
		return "INSERT INTO " + qualified + " (" + sqlServerQuotedColumns(columns) + ") VALUES (" + sqlServerPlaceholders(len(columns)) + ")"
	}
	keys := primaryKeyColumns(table)
	conditions := make([]string, len(keys))
	for index, key := range keys {
		conditions[index] = "target." + sqlServerIdentifier(key) + " = source." + sqlServerIdentifier(key)
	}
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(updates, "target."+sqlServerIdentifier(column)+" = source."+sqlServerIdentifier(column))
		}
	}
	statement := "MERGE INTO " + qualified + " AS target USING (VALUES (" + sqlServerPlaceholders(len(columns)) + ")) AS source (" + sqlServerQuotedColumns(columns) + ") ON " + strings.Join(conditions, " AND ")
	if len(updates) > 0 {
		statement += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ", ")
	}
	return statement + " WHEN NOT MATCHED THEN INSERT (" + sqlServerQuotedColumns(columns) + ") VALUES (" + sqlServerSourceColumns(columns) + ");"
}

func sqlServerPlaceholders(count int) string {
	parts := make([]string, count)
	for index := range parts {
		parts[index] = fmt.Sprintf("@p%d", index+1)
	}
	return strings.Join(parts, ", ")
}

func sqlServerSourceColumns(columns []string) string {
	parts := make([]string, len(columns))
	for index, column := range columns {
		parts[index] = "source." + sqlServerIdentifier(column)
	}
	return strings.Join(parts, ", ")
}

func validateSQLiteToSQLServerCount(ctx context.Context, source, target *sql.DB, table schema.Table) error {
	sourceCount, err := countRows(ctx, source, table.Name)
	if err != nil {
		return err
	}
	var targetCount int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(table.Schema, table.Name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count SQL Server table %s: %w", table.Name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", table.Name, sourceCount, targetCount)
	}
	return nil
}
