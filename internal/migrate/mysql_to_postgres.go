package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
	"github.com/johndauphine/DMTX/internal/schema"
)

// MySQLToPostgresWithObserver migrates deterministic MySQL metadata and rows
// into a PostgreSQL target.
func MySQLToPostgresWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf("MySQL-to-PostgreSQL requires source.type mysql and target.type postgres")
	}
	sourceEndpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Result{}, fmt.Errorf("resolve source: %w", err)
	}
	targetEndpoint, err := resolvedEndpoint(cfg.Target)
	if err != nil {
		return Result{}, fmt.Errorf("resolve target: %w", err)
	}
	source, err := engine.OpenMySQL(ctx, sourceEndpoint)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	target, err := engine.OpenPostgres(ctx, targetEndpoint)
	if err != nil {
		return Result{}, err
	}
	defer target.Close()
	sourceSchema := cfg.Source.Schema
	if sourceSchema == "" {
		sourceSchema = cfg.Source.Database
	}
	targetSchema := cfg.Target.Schema
	if targetSchema == "" {
		targetSchema = "public"
	}
	names, err := engine.ListMySQLTables(ctx, source, sourceSchema)
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
		table, err := engine.InspectMySQLTable(ctx, source, sourceSchema, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		columns := mysqlColumnNames(table)
		table.Schema = targetSchema
		if err := preparePostgresTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		rows, err := source.QueryContext(ctx, mySQLReadQuery(sourceSchema, table, columns))
		if err != nil {
			return Result{}, fmt.Errorf("read MySQL table %s: %w", name, err)
		}
		copied, copyErr := copyRowsToPostgres(ctx, rows, target, table, columns, cfg.Migration.TargetMode)
		rows.Close()
		if copyErr != nil {
			return Result{}, copyErr
		}
		if err := validateMySQLToPostgresCount(ctx, source, target, sourceSchema, table); err != nil {
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

func validateMySQLToPostgresCount(ctx context.Context, source, target *sql.DB, sourceSchema string, table schema.Table) error {
	var sourceCount, targetCount int
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(sourceSchema, table.Name)).Scan(&sourceCount); err != nil {
		return fmt.Errorf("count source MySQL table %s: %w", table.Name, err)
	}
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(table.Schema, table.Name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count target PostgreSQL table %s: %w", table.Name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", table.Name, sourceCount, targetCount)
	}
	return nil
}

func copyRowsToPostgres(ctx context.Context, rows *sql.Rows, target *sql.DB, table schema.Table, columns []string, mode string) (int, error) {
	count, batch := 0, make([][]any, 0, sqliteWriteBatchSize)
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf("read source table %s: %w", table.Name, err)
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
		return 0, fmt.Errorf("read source table %s: %w", table.Name, err)
	}
	if len(batch) > 0 {
		if err := writePostgresBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}
