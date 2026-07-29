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

// MySQLToSQLiteWithObserver migrates MySQL or MariaDB base tables into SQLite
// with DMTX-planned schema creation, bounded writes, and exact row validation.
func MySQLToSQLiteWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("MySQL-to-SQLite requires source.type mysql and target.type sqlite")
	}
	if cfg.Target.Database == "" {
		return Result{}, fmt.Errorf("SQLite target database path is required")
	}
	sourceEndpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Result{}, err
	}
	source, err := engine.OpenMySQL(ctx, sourceEndpoint)
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
		namespace = cfg.Source.Database
	}
	names, err := engine.ListMySQLTables(ctx, source, namespace)
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
		table, err := engine.InspectMySQLTable(ctx, source, namespace, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(table) {
			return Result{}, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		columns := mysqlColumnNames(table)
		table.Schema = ""
		if err := prepareTarget(ctx, target, table, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		copied, err := copyRelationalRows(ctx, source, target, table, columns, cfg.Migration.TargetMode, mySQLReadQuery(namespace, table, columns), "MySQL")
		if err != nil {
			return Result{}, err
		}
		if err := validateMySQLCount(ctx, source, target, namespace, table.Name); err != nil {
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

func mysqlColumnNames(table schema.Table) []string {
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		columns[index] = column.Name
	}
	return columns
}

func mySQLReadQuery(namespace string, table schema.Table, columns []string) string {
	return "SELECT " + mySQLQuotedColumns(columns) + " FROM " + mySQLQualified(namespace, table.Name) + " ORDER BY " + mySQLQuotedColumns(primaryKeyColumns(table))
}

func mySQLQualified(namespace, name string) string {
	return mySQLIdentifier(namespace) + "." + mySQLIdentifier(name)
}

func mySQLQuotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = mySQLIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

func mySQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func validateMySQLCount(ctx context.Context, source, target *sql.DB, namespace, name string) error {
	var sourceCount int
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(namespace, name)).Scan(&sourceCount); err != nil {
		return fmt.Errorf("count MySQL table %s: %w", name, err)
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
