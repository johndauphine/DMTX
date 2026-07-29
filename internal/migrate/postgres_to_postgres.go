package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

// PostgresToPostgresWithObserver migrates a PostgreSQL schema to a distinct
// PostgreSQL endpoint with deterministic discovery, transactional writes, and
// count validation.
func PostgresToPostgresWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf("PostgreSQL-to-PostgreSQL requires source.type and target.type postgres")
	}
	sourceEndpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Result{}, fmt.Errorf("resolve source: %w", err)
	}
	targetEndpoint, err := resolvedEndpoint(cfg.Target)
	if err != nil {
		return Result{}, fmt.Errorf("resolve target: %w", err)
	}
	source, err := engine.OpenPostgres(ctx, sourceEndpoint)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	target, err := engine.OpenPostgres(ctx, targetEndpoint)
	if err != nil {
		return Result{}, err
	}
	defer target.Close()
	sourceSchema, targetSchema := cfg.Source.Schema, cfg.Target.Schema
	if sourceSchema == "" {
		sourceSchema = "public"
	}
	if targetSchema == "" {
		targetSchema = "public"
	}
	names, err := engine.ListPostgresTables(ctx, source, sourceSchema)
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
		table, err := engine.InspectPostgresTable(ctx, source, sourceSchema, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		columns := postgresColumnNames(table)
		table.Schema = targetSchema
		if err := preparePostgresTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copyPostgresRowsToPostgres(ctx, source, target, sourceSchema, table, columns, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validatePostgresToPostgresCount(ctx, source, target, sourceSchema, table); err != nil {
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

func copyPostgresRowsToPostgres(ctx context.Context, source, target *sql.DB, sourceSchema string, table schema.Table, columns []string, mode string) (int, error) {
	rows, err := source.QueryContext(ctx, postgresReadQuery(sourceSchema, table, columns))
	if err != nil {
		return 0, fmt.Errorf("read PostgreSQL table %s: %w", table.Name, err)
	}
	defer rows.Close()
	count, batch := 0, make([][]any, 0, sqliteWriteBatchSize)
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf("read PostgreSQL table %s: %w", table.Name, err)
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
		return 0, fmt.Errorf("read PostgreSQL table %s: %w", table.Name, err)
	}
	if len(batch) > 0 {
		if err := writePostgresBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func validatePostgresToPostgresCount(ctx context.Context, source, target *sql.DB, sourceSchema string, table schema.Table) error {
	var sourceCount, targetCount int
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(sourceSchema, table.Name)).Scan(&sourceCount); err != nil {
		return fmt.Errorf("count source PostgreSQL table %s: %w", table.Name, err)
	}
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(table.Schema, table.Name)).Scan(&targetCount); err != nil {
		return fmt.Errorf("count target PostgreSQL table %s: %w", table.Name, err)
	}
	if sourceCount != targetCount {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", table.Name, sourceCount, targetCount)
	}
	return nil
}
