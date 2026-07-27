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

// PostgresToSQLiteWithObserver migrates PostgreSQL base tables into a SQLite
// target using DMTX's deterministic schema planner and bounded write batches.
// It is intentionally a fresh-run path; cross-engine resume frontiers are
// added with the Stage 2 transfer-state contract.
func PostgresToSQLiteWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("PostgreSQL-to-SQLite requires source.type postgres and target.type sqlite")
	}
	if cfg.Target.Database == "" {
		return Result{}, fmt.Errorf("SQLite target database path is required")
	}
	sourceEndpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Result{}, err
	}
	source, err := engine.OpenPostgres(ctx, sourceEndpoint)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	target, err := sql.Open("sqlite", cfg.Target.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open SQLite target: %w", err)
	}
	defer target.Close()

	namespace := cfg.Source.Schema
	if namespace == "" {
		namespace = "public"
	}
	names, err := engine.ListPostgresTables(ctx, source, namespace)
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
		table, err := engine.InspectPostgresTable(ctx, source, namespace, name)
		if err != nil {
			return Result{}, err
		}
		columns := postgresColumnNames(table)
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		table.Schema = ""
		if err := prepareTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copyPostgresRows(ctx, source, target, namespace, table, columns, cfg.Migration.TargetMode)
		if err != nil {
			return Result{}, err
		}
		if err := validatePostgresCount(ctx, source, target, namespace, table.Name); err != nil {
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

func resolvedEndpoint(endpoint config.Endpoint) (config.Endpoint, error) {
	password, err := config.ExpandSecret(endpoint.Password)
	if err != nil {
		return config.Endpoint{}, fmt.Errorf("resolve source password: %w", err)
	}
	endpoint.Password = password
	return endpoint, nil
}

func postgresColumnNames(table schema.Table) []string {
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		columns[index] = column.Name
	}
	return columns
}

func copyPostgresRows(ctx context.Context, source, target *sql.DB, namespace string, table schema.Table, columns []string, mode string) (int, error) {
	query := postgresReadQuery(namespace, table, columns)
	rows, err := source.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("read PostgreSQL table %s: %w", table.Name, err)
	}
	defer rows.Close()
	count := 0
	batch := make([][]any, 0, sqliteWriteBatchSize)
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf("read PostgreSQL table %s: %w", table.Name, err)
		}
		batch = append(batch, append([]any(nil), values...))
		if len(batch) == sqliteWriteBatchSize {
			if err := writeBatch(ctx, target, table, columns, mode, batch); err != nil {
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
		if err := writeBatch(ctx, target, table, columns, mode, batch); err != nil {
			return 0, err
		}
		count += len(batch)
	}
	return count, nil
}

func postgresReadQuery(namespace string, table schema.Table, columns []string) string {
	return "SELECT " + quotedColumns(columns) + " FROM " + postgresQualified(namespace, table.Name) + " ORDER BY " + quotedColumns(primaryKeyColumns(table))
}

func postgresQualified(namespace, name string) string {
	return postgresIdentifier(namespace) + "." + postgresIdentifier(name)
}

func validatePostgresCount(ctx context.Context, source, target *sql.DB, namespace, name string) error {
	var sourceCount int
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(namespace, name)).Scan(&sourceCount); err != nil {
		return fmt.Errorf("count PostgreSQL table %s: %w", name, err)
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

func postgresReadQueryForColumns(namespace, name string, columns, primaryKeys []string) string {
	table := schema.Table{Name: name, Columns: make([]schema.Column, len(columns))}
	keys := make(map[string]bool, len(primaryKeys))
	for _, key := range primaryKeys {
		keys[key] = true
	}
	for index, column := range columns {
		table.Columns[index] = schema.Column{Name: column, PrimaryKey: keys[column]}
	}
	return postgresReadQuery(namespace, table, columns)
}

func postgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
