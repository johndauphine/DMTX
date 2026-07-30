package migrate

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

func (adapter *sqliteTargetAdapter) PreflightSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
	_ string,
) error {
	switch source.Engine() {
	case "postgres":
		return preflightPostgresSQLiteSourceData(ctx, source, plans)
	case "mssql":
	default:
		return nil
	}
	for _, plan := range plans {
		rows, err := source.OpenRows(ctx, plan.source, plan.columns)
		if err != nil {
			return err
		}
		values := make([]any, len(plan.columns))
		destinations := make([]any, len(plan.columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		rowNumber := int64(0)
		for rows.Next() {
			rowNumber++
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				return fmt.Errorf(
					"preflight SQL Server row %s[%d]: %w",
					plan.source.Name,
					rowNumber,
					err,
				)
			}
			if err := validateSQLServerSQLiteRow(
				plan.target,
				plan.columns,
				values,
			); err != nil {
				_ = rows.Close()
				return fmt.Errorf(
					"preflight SQL Server row %s[%d]: %w",
					plan.source.Name,
					rowNumber,
					err,
				)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"preflight SQL Server rows for %s: %w",
				plan.source.Name,
				err,
			)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf(
				"close SQL Server preflight rows for %s: %w",
				plan.source.Name,
				err,
			)
		}
	}
	return nil
}

func validateSQLServerSQLiteBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	for rowIndex, row := range rows {
		if err := validateSQLServerSQLiteRow(
			table,
			columns,
			row,
		); err != nil {
			return fmt.Errorf(
				"validate SQL Server-to-SQLite batch %s[%d]: %w",
				table.Name,
				rowIndex+1,
				err,
			)
		}
	}
	return nil
}

func normalizeSQLServerSQLiteBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	if err := validateSQLServerSQLiteBatch(table, columns, rows); err != nil {
		return nil, err
	}
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex] = cloneAdapterRow(row)
		for columnIndex, name := range columns {
			value, ok := row[columnIndex].(time.Time)
			if !ok {
				continue
			}
			column := metadata[name]
			base := strings.ToLower(
				strings.TrimSpace(column.DeclaredType.Base),
			)
			switch base {
			case "date":
				normalized[rowIndex][columnIndex] = value.Format(
					"2006-01-02",
				)
			case "datetime":
				normalized[rowIndex][columnIndex] = value.Format(
					"2006-01-02 15:04:00",
				)
			case "timestamp":
				scale := column.DeclaredType.Arguments[0]
				layout := "2006-01-02 15:04:05"
				if scale > 0 {
					layout += "." + strings.Repeat("0", scale)
				}
				normalized[rowIndex][columnIndex] = value.Format(layout)
			}
		}
	}
	return normalized, nil
}

func validateSQLServerSQLiteRow(
	table schema.Table,
	columns []string,
	row []any,
) error {
	if len(row) != len(columns) {
		return fmt.Errorf(
			"row has %d values for %d columns",
			len(row),
			len(columns),
		)
	}
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	for index, name := range columns {
		column, ok := metadata[name]
		if !ok {
			return fmt.Errorf("column %q is absent from planned schema", name)
		}
		if err := validateSQLServerSQLiteValue(column, row[index]); err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
	}
	return nil
}

func validateSQLServerSQLiteValue(
	column schema.Column,
	value any,
) error {
	if value == nil {
		if column.Nullable {
			return nil
		}
		return fmt.Errorf("NULL violates the planned non-null contract")
	}
	if column.DeclaredType == nil {
		return fmt.Errorf("planned declared type is missing")
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	switch base {
	case "tinyint", "smallint", "int", "integer", "bigint":
		switch typed := value.(type) {
		case int64:
			return nil
		case int32:
			return nil
		case int:
			return nil
		case string:
			if _, err := strconv.ParseInt(typed, 10, 64); err == nil {
				return nil
			}
		}
		return fmt.Errorf("value does not fit exact SQLite INTEGER storage")
	case "boolean", "bool":
		switch typed := value.(type) {
		case bool:
			return nil
		case int64:
			if typed == 0 || typed == 1 {
				return nil
			}
		}
		return fmt.Errorf("value is outside the SQLite boolean domain")
	case "real", "double", "double precision":
		floating := 0.0
		switch typed := value.(type) {
		case float32:
			floating = float64(typed)
		case float64:
			floating = typed
		default:
			return fmt.Errorf("value is not a finite SQLite REAL")
		}
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return fmt.Errorf("value is not a finite SQLite REAL")
		}
		return nil
	case "char", "varchar", "text", "uuid", "time":
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) ||
			strings.ContainsRune(text, '\x00') ||
			strings.ContainsRune(text, '\uFFFD') {
			return fmt.Errorf("value is not admitted UTF-8 text")
		}
		return nil
	case "blob":
		if _, ok := value.([]byte); !ok {
			return fmt.Errorf("value is not SQLite BLOB data")
		}
		return nil
	case "date", "timestamp", "datetime":
		if _, ok := value.(time.Time); !ok {
			return fmt.Errorf("value is not a validated temporal value")
		}
		return nil
	default:
		return fmt.Errorf("planned type %q has no value contract", base)
	}
}
