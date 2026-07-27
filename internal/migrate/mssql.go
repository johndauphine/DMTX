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

// SQLServerToSQLiteWithObserver migrates SQL Server base tables to SQLite
// using deterministic metadata, bounded target transactions, and row-count
// validation. Cross-engine restart frontiers are added under Stage 2.
func SQLServerToSQLiteWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("SQL Server-to-SQLite requires source.type mssql and target.type sqlite")
	}
	if cfg.Target.Database == "" {
		return Result{}, fmt.Errorf("SQLite target database path is required")
	}
	sourceEndpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Result{}, err
	}
	source, err := engine.OpenSQLServer(ctx, sourceEndpoint)
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
		namespace = "dbo"
	}
	names, err := engine.ListSQLServerTables(ctx, source, namespace)
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
		table, err := engine.InspectSQLServerTable(ctx, source, namespace, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		columns := sqlServerColumnNames(table)
		table.Schema = ""
		if err := prepareTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copyRelationalRows(ctx, source, target, table, columns, cfg.Migration.TargetMode, sqlServerReadQuery(namespace, table, columns), "SQL Server")
		if err != nil {
			return Result{}, err
		}
		if err := validateSQLServerCount(ctx, source, target, namespace, table.Name); err != nil {
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

func sqlServerColumnNames(table schema.Table) []string {
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		columns[index] = column.Name
	}
	return columns
}

func sqlServerReadQuery(namespace string, table schema.Table, columns []string) string {
	return "SELECT " + sqlServerQuotedColumns(columns) + " FROM " + sqlServerQualified(namespace, table.Name) + " ORDER BY " + sqlServerQuotedColumns(primaryKeyColumns(table))
}

func sqlServerQualified(namespace, name string) string {
	return sqlServerIdentifier(namespace) + "." + sqlServerIdentifier(name)
}

func sqlServerQuotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = sqlServerIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

func sqlServerIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}

func validateSQLServerCount(ctx context.Context, source, target *sql.DB, namespace, name string) error {
	var sourceCount int
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(namespace, name)).Scan(&sourceCount); err != nil {
		return fmt.Errorf("count SQL Server table %s: %w", name, err)
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
