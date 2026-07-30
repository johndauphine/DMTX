package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

type mySQLTargetSQLServerValueKind uint8

const (
	mySQLTargetSQLServerValueInvalid mySQLTargetSQLServerValueKind = iota
	mySQLTargetSQLServerValueDate
	mySQLTargetSQLServerValueTime
	mySQLTargetSQLServerValueDateTime
)

type mySQLTargetSQLServerValueColumn struct {
	index     int
	name      string
	kind      mySQLTargetSQLServerValueKind
	nullable  bool
	precision int
}

// PreflightSourceData reads every SQL Server temporal value whose admitted
// MySQL domain is narrower than SQL Server's. It runs after target-catalog
// preflight and before the first target mutation. WriteBatch repeats the same
// validation so a source change between this scan and its batch cannot bypass
// the target boundary.
func (adapter *mysqlTargetAdapter) PreflightSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
	_ string,
) error {
	if source == nil {
		return fmt.Errorf(
			"preflight source data for MySQL target: source adapter is required",
		)
	}
	adapter.normalizeSQLiteSourceValues = false
	adapter.validateSQLServerSourceValues = false
	switch source.Engine() {
	case "sqlite":
		adapter.normalizeSQLiteSourceValues = true
		return preflightSQLiteMySQLSourceData(ctx, source, plans)
	case "mssql":
		adapter.validateSQLServerSourceValues = true
	default:
		return nil
	}
	for _, plan := range plans {
		if err := preflightSQLServerTableValuesForMySQL(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightSQLServerTableValuesForMySQL(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	checked, err := mySQLTargetSQLServerValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	if len(checked) == 0 {
		return nil
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open SQL Server table %s for MySQL value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			safeClose := fmt.Errorf(
				"close SQL Server table %s MySQL value preflight: %w",
				plan.source.Name,
				closeErr,
			)
			if result == nil {
				result = safeClose
			} else {
				result = errors.Join(result, safeClose)
			}
		}
	}()

	values := make([]any, len(plan.columns))
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf(
				"read SQL Server table %s during MySQL value preflight: %w",
				plan.source.Name,
				err,
			)
		}
		if err := validateMySQLTargetSQLServerValues(
			plan.target,
			checked,
			values,
		); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQL Server table %s during MySQL value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	return nil
}

func validateMySQLTargetSQLServerBatchValues(
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	checked, err := mySQLTargetSQLServerValueColumns(table, columns)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf(
				"validate MySQL target values for table %s: row width does not match selected columns",
				table.Name,
			)
		}
		if err := validateMySQLTargetSQLServerValues(
			table,
			checked,
			row,
		); err != nil {
			return err
		}
	}
	return nil
}

func mySQLTargetSQLServerValueColumns(
	table schema.Table,
	columns []string,
) ([]mySQLTargetSQLServerValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	checked := make([]mySQLTargetSQLServerValueColumn, 0)
	for index, name := range columns {
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"validate MySQL target values for table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		value, tracked, err :=
			mySQLTargetSQLServerValueColumnFromSchema(index, column)
		if err != nil {
			return nil, fmt.Errorf(
				"validate MySQL target value domain for %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		if tracked {
			checked = append(checked, value)
		}
	}
	return checked, nil
}

func mySQLTargetSQLServerValueColumnFromSchema(
	index int,
	column schema.Column,
) (mySQLTargetSQLServerValueColumn, bool, error) {
	result := mySQLTargetSQLServerValueColumn{
		index:    index,
		name:     column.Name,
		nullable: column.Nullable,
	}
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	if column.DeclaredType == nil {
		switch semantic {
		case "date", "time", "datetime", "timestamp":
			return result, false, fmt.Errorf(
				"tracked temporal domain has no declared type",
			)
		default:
			return result, false, nil
		}
	}

	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
	arguments := column.DeclaredType.Arguments
	switch base {
	case "date":
		if semantic != "date" || len(arguments) != 0 {
			return result, false, fmt.Errorf(
				"invalid DATE declaration",
			)
		}
		result.kind = mySQLTargetSQLServerValueDate
	case "time":
		if semantic != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return result, false, fmt.Errorf(
				"invalid TIME declaration",
			)
		}
		result.kind = mySQLTargetSQLServerValueTime
		result.precision = arguments[0]
	case "datetime":
		if semantic != "datetime" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return result, false, fmt.Errorf(
				"invalid DATETIME declaration",
			)
		}
		result.kind = mySQLTargetSQLServerValueDateTime
		result.precision = arguments[0]
	default:
		switch semantic {
		case "date", "time", "datetime", "timestamp":
			return result, false, fmt.Errorf(
				"temporal semantic has an unexpected declared type",
			)
		default:
			return result, false, nil
		}
	}
	return result, true, nil
}

func validateMySQLTargetSQLServerValues(
	table schema.Table,
	columns []mySQLTargetSQLServerValueColumn,
	values []any,
) error {
	for _, column := range columns {
		if column.index >= len(values) {
			return mySQLTargetSQLServerValueError(
				table,
				column,
				"row is missing the selected column",
			)
		}
		value := values[column.index]
		if value == nil {
			if column.nullable {
				continue
			}
			return mySQLTargetSQLServerValueError(
				table,
				column,
				"NULL is outside the non-nullable target domain",
			)
		}
		switch column.kind {
		case mySQLTargetSQLServerValueDate:
			temporal, ok := value.(time.Time)
			if !ok ||
				!mySQLTargetSQLServerDateTimeRange(temporal) ||
				!mySQLTargetSQLServerUTC(temporal) ||
				temporal.Hour() != 0 ||
				temporal.Minute() != 0 ||
				temporal.Second() != 0 ||
				temporal.Nanosecond() != 0 {
				return mySQLTargetSQLServerValueError(
					table,
					column,
					"value is not exactly representable as MySQL DATE",
				)
			}
		case mySQLTargetSQLServerValueTime:
			text, ok := value.(string)
			if !ok ||
				!canonicalSQLServerTimeForMySQL(
					text,
					column.precision,
				) {
				return mySQLTargetSQLServerValueError(
					table,
					column,
					"value is not canonical SQL Server TIME text at the planned MySQL precision",
				)
			}
		case mySQLTargetSQLServerValueDateTime:
			temporal, ok := value.(time.Time)
			if !ok ||
				!mySQLTargetSQLServerDateTimeRange(temporal) ||
				!mySQLTargetSQLServerUTC(temporal) ||
				!mySQLTargetSQLServerFractionExact(
					temporal.Nanosecond(),
					column.precision,
				) {
				return mySQLTargetSQLServerValueError(
					table,
					column,
					"value is not exactly representable at the planned MySQL DATETIME precision",
				)
			}
		default:
			return mySQLTargetSQLServerValueError(
				table,
				column,
				"unsupported target value domain",
			)
		}
	}
	return nil
}

func mySQLTargetSQLServerDateTimeRange(value time.Time) bool {
	return value.Year() >= 1000 && value.Year() <= 9999
}

func mySQLTargetSQLServerUTC(value time.Time) bool {
	_, offset := value.Zone()
	return offset == 0
}

func mySQLTargetSQLServerFractionExact(
	nanoseconds int,
	precision int,
) bool {
	if nanoseconds < 0 ||
		nanoseconds >= int(time.Second) ||
		precision < 0 ||
		precision > 6 {
		return false
	}
	unit := int(time.Second)
	for index := 0; index < precision; index++ {
		unit /= 10
	}
	return nanoseconds%unit == 0
}

func canonicalSQLServerTimeForMySQL(
	value string,
	precision int,
) bool {
	if precision < 0 || precision > 6 {
		return false
	}
	wantLength := 8
	if precision > 0 {
		wantLength += 1 + precision
	}
	if len(value) != wantLength ||
		value[2] != ':' ||
		value[5] != ':' {
		return false
	}
	if precision > 0 && value[8] != '.' {
		return false
	}
	digitIndexes := []int{0, 1, 3, 4, 6, 7}
	for index := 9; index < len(value); index++ {
		digitIndexes = append(digitIndexes, index)
	}
	for _, index := range digitIndexes {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	hour := int(value[0]-'0')*10 + int(value[1]-'0')
	minute := int(value[3]-'0')*10 + int(value[4]-'0')
	second := int(value[6]-'0')*10 + int(value[7]-'0')
	return hour <= 23 && minute <= 59 && second <= 59
}

func mySQLTargetSQLServerValueError(
	table schema.Table,
	column mySQLTargetSQLServerValueColumn,
	reason string,
) error {
	return fmt.Errorf(
		"MySQL target column %s.%s rejects a SQL Server source value: %s",
		table.Name,
		column.name,
		reason,
	)
}
