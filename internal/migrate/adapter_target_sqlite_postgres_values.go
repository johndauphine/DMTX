package migrate

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresSQLiteValueKind uint8

const (
	postgresSQLiteValueUnknown postgresSQLiteValueKind = iota
	postgresSQLiteValueInteger
	postgresSQLiteValueBoolean
	postgresSQLiteValueText
	postgresSQLiteValueBlob
	postgresSQLiteValueDate
	postgresSQLiteValueTime
	postgresSQLiteValueTimestamp
)

type postgresSQLiteValueColumn struct {
	column    schema.Column
	kind      postgresSQLiteValueKind
	precision int
	maxRunes  int
}

// preflightPostgresSQLiteSourceData proves that every selected PostgreSQL
// value belongs to the deliberately narrow SQLite storage contract before the
// target's first mutation. Values are checked again at the write boundary so
// a source change between preflight and transfer still fails closed.
func preflightPostgresSQLiteSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
) error {
	for _, plan := range plans {
		if err := preflightPostgresSQLiteTableData(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightPostgresSQLiteTableData(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	ordered, err := postgresSQLiteValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open PostgreSQL table %s for SQLite value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf(
				"close PostgreSQL table %s SQLite value preflight: %w",
				plan.source.Name,
				closeErr,
			)
			if result == nil {
				result = closeErr
			} else {
				result = errors.Join(result, closeErr)
			}
		}
	}()

	values := make([]any, len(plan.columns))
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	rowNumber := int64(0)
	for rows.Next() {
		rowNumber++
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf(
				"read PostgreSQL table %s row %d during SQLite value preflight: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
		if _, err := normalizePostgresSQLiteRow(ordered, values); err != nil {
			return fmt.Errorf(
				"preflight PostgreSQL table %s row %d for SQLite: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate PostgreSQL table %s during SQLite value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	return nil
}

func normalizePostgresSQLiteBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	ordered, err := postgresSQLiteValueColumns(table, columns)
	if err != nil {
		return nil, err
	}
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex], err = normalizePostgresSQLiteRow(
			ordered,
			row,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite table %s row %d: %w",
				table.Name,
				rowIndex+1,
				err,
			)
		}
	}
	return normalized, nil
}

func postgresSQLiteValueColumns(
	table schema.Table,
	names []string,
) ([]postgresSQLiteValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite table %s: planned schema has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite table %s: planned schema has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
	}

	ordered := make([]postgresSQLiteValueColumn, len(names))
	selected := make(map[string]struct{}, len(names))
	for index, name := range names {
		if _, duplicate := selected[name]; duplicate {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite table %s: selected column %s is duplicated",
				table.Name,
				name,
			)
		}
		selected[name] = struct{}{}
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		contract, err := postgresSQLiteValueColumnFromSchema(column)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize PostgreSQL-to-SQLite column %s.%s: %w",
				table.Name,
				name,
				err,
			)
		}
		ordered[index] = contract
	}
	return ordered, nil
}

func postgresSQLiteValueColumnFromSchema(
	column schema.Column,
) (postgresSQLiteValueColumn, error) {
	result := postgresSQLiteValueColumn{column: column}
	if column.DeclaredType == nil {
		return result, fmt.Errorf("planned declared type is missing")
	}
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
	arguments := column.DeclaredType.Arguments
	noArguments := len(arguments) == 0
	switch semantic {
	case "integer":
		if base != "integer" || !noArguments {
			return result, fmt.Errorf("invalid exact INTEGER projection")
		}
		result.kind = postgresSQLiteValueInteger
	case "bigint":
		if base != "bigint" || !noArguments {
			return result, fmt.Errorf("invalid exact BIGINT projection")
		}
		result.kind = postgresSQLiteValueInteger
	case "numeric":
		if base != "bigint" || !noArguments {
			return result, fmt.Errorf("invalid exact NUMERIC projection")
		}
		result.kind = postgresSQLiteValueInteger
	case "boolean":
		if base != "boolean" || !noArguments {
			return result, fmt.Errorf("invalid BOOLEAN projection")
		}
		result.kind = postgresSQLiteValueBoolean
	case "text":
		if base != "text" || !noArguments {
			return result, fmt.Errorf("invalid TEXT projection")
		}
		result.kind = postgresSQLiteValueText
	case "varchar":
		if base != "varchar" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 10_485_760 {
			return result, fmt.Errorf("invalid VARCHAR projection")
		}
		result.kind = postgresSQLiteValueText
		result.maxRunes = arguments[0]
	case "bytea":
		if base != "blob" || !noArguments {
			return result, fmt.Errorf("invalid BYTEA projection")
		}
		result.kind = postgresSQLiteValueBlob
	case "date":
		if base != "date" || !noArguments {
			return result, fmt.Errorf("invalid DATE projection")
		}
		result.kind = postgresSQLiteValueDate
	case "time":
		if base != "time" || len(arguments) > 1 {
			return result, fmt.Errorf("invalid TIME projection")
		}
		result.kind = postgresSQLiteValueTime
		result.precision = 6
		if len(arguments) == 1 {
			result.precision = arguments[0]
		}
		if result.precision < 0 || result.precision > 6 {
			return result, fmt.Errorf("invalid TIME precision")
		}
	case "timestamp":
		if base != "timestamp" || len(arguments) > 1 {
			return result, fmt.Errorf("invalid TIMESTAMP projection")
		}
		result.kind = postgresSQLiteValueTimestamp
		result.precision = 6
		if len(arguments) == 1 {
			result.precision = arguments[0]
		}
		if result.precision < 0 || result.precision > 6 {
			return result, fmt.Errorf("invalid TIMESTAMP precision")
		}
	default:
		return result, fmt.Errorf(
			"source type %q has no exact SQLite value contract",
			semantic,
		)
	}
	return result, nil
}

func normalizePostgresSQLiteRow(
	columns []postgresSQLiteValueColumn,
	row []any,
) ([]any, error) {
	if len(row) != len(columns) {
		return nil, fmt.Errorf(
			"row has %d values for %d selected columns",
			len(row),
			len(columns),
		)
	}
	normalized := make([]any, len(row))
	for index, value := range row {
		column := columns[index]
		if value == nil {
			if !column.column.Nullable {
				return nil, fmt.Errorf(
					"column %s: NULL violates the planned non-null contract",
					column.column.Name,
				)
			}
			continue
		}
		converted, err := normalizePostgresSQLiteValue(column, value)
		if err != nil {
			return nil, fmt.Errorf(
				"column %s: %w",
				column.column.Name,
				err,
			)
		}
		normalized[index] = converted
	}
	return normalized, nil
}

func normalizePostgresSQLiteValue(
	column postgresSQLiteValueColumn,
	value any,
) (any, error) {
	switch column.kind {
	case postgresSQLiteValueInteger:
		return normalizePostgresSQLiteInteger(value)
	case postgresSQLiteValueBoolean:
		boolean, err := normalizePostgresBoolean(value)
		if err != nil {
			return nil, fmt.Errorf("value is outside the strict boolean domain")
		}
		return boolean, nil
	case postgresSQLiteValueText:
		text, err := normalizePostgresText(value)
		if err != nil {
			return nil, fmt.Errorf("value is not admitted UTF-8 text")
		}
		if column.maxRunes > 0 &&
			utf8.RuneCountInString(text) > column.maxRunes {
			return nil, fmt.Errorf(
				"value exceeds the planned VARCHAR length %d",
				column.maxRunes,
			)
		}
		return text, nil
	case postgresSQLiteValueBlob:
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("value is not exact binary data")
		}
		return append([]byte(nil), bytes...), nil
	case postgresSQLiteValueDate:
		return normalizePostgresSQLiteDate(value)
	case postgresSQLiteValueTime:
		return normalizePostgresSQLiteTime(value, column.precision)
	case postgresSQLiteValueTimestamp:
		return normalizePostgresSQLiteTimestamp(
			value,
			column.precision,
		)
	default:
		return nil, fmt.Errorf("planned value contract is unsupported")
	}
}

func normalizePostgresSQLiteInteger(value any) (int64, error) {
	integer, err := exactPostgresInteger(value)
	if err == nil && integer.IsInt64() {
		return integer.Int64(), nil
	}

	numeric, numericErr := normalizePostgresNumericWithModifiers(
		value,
		19,
		0,
	)
	if numericErr != nil ||
		!numeric.Valid ||
		numeric.NaN ||
		numeric.InfinityModifier != pgtype.Finite ||
		numeric.Int == nil {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	integer = new(big.Int).Set(numeric.Int)
	if numeric.Exp > 0 {
		power := new(big.Int).Exp(
			big.NewInt(10),
			big.NewInt(int64(numeric.Exp)),
			nil,
		)
		integer.Mul(integer, power)
	} else if numeric.Exp < 0 {
		// A scale-zero normalization must have removed only trailing zeroes
		// and returned an integral coefficient.
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	if !integer.IsInt64() {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	return integer.Int64(), nil
}

func normalizePostgresSQLiteDate(value any) (string, error) {
	date, err := normalizePostgresDate(value)
	if err != nil ||
		!date.Valid ||
		date.InfinityModifier != pgtype.Finite ||
		date.Time.Year() < 1 ||
		date.Time.Year() > 9999 ||
		date.Time.Hour() != 0 ||
		date.Time.Minute() != 0 ||
		date.Time.Second() != 0 ||
		date.Time.Nanosecond() != 0 {
		return "", fmt.Errorf(
			"value is not exactly representable as SQLite DATE text",
		)
	}
	_, offset := date.Time.Zone()
	if offset != 0 {
		return "", fmt.Errorf(
			"value is not exactly representable as SQLite DATE text",
		)
	}
	return date.Time.Format("2006-01-02"), nil
}

func normalizePostgresSQLiteTimestamp(
	value any,
	precision int,
) (string, error) {
	timestamp, err := normalizePostgresTimestamp(value)
	if err != nil ||
		!timestamp.Valid ||
		timestamp.InfinityModifier != pgtype.Finite ||
		timestamp.Time.Year() < 1 ||
		timestamp.Time.Year() > 9999 {
		return "", fmt.Errorf(
			"value is not exactly representable as SQLite TIMESTAMP text",
		)
	}
	_, offset := timestamp.Time.Zone()
	if offset != 0 ||
		!postgresSQLiteFractionExact(
			timestamp.Time.Nanosecond(),
			precision,
		) {
		return "", fmt.Errorf(
			"value is not exactly representable at the planned SQLite TIMESTAMP precision",
		)
	}
	layout := "2006-01-02 15:04:05"
	if precision > 0 {
		layout += "." + strings.Repeat("0", precision)
	}
	return timestamp.Time.Format(layout), nil
}

func normalizePostgresSQLiteTime(
	value any,
	precision int,
) (string, error) {
	clock, err := normalizePostgresTime(value)
	if err != nil {
		clock, err = parsePostgresSQLiteEndOfDay(value)
	}
	if err != nil ||
		!clock.Valid ||
		clock.Microseconds < 0 ||
		clock.Microseconds > int64(24*time.Hour/time.Microsecond) ||
		!postgresSQLiteFractionExact(
			int(clock.Microseconds%1_000_000)*int(time.Microsecond),
			precision,
		) {
		return "", fmt.Errorf(
			"value is not exactly representable at the planned SQLite TIME precision",
		)
	}
	if clock.Microseconds == int64(24*time.Hour/time.Microsecond) {
		return postgresSQLiteTimeText(24, 0, 0, 0, precision), nil
	}
	hour := clock.Microseconds / int64(time.Hour/time.Microsecond)
	remaining := clock.Microseconds % int64(time.Hour/time.Microsecond)
	minute := remaining / int64(time.Minute/time.Microsecond)
	remaining %= int64(time.Minute / time.Microsecond)
	second := remaining / 1_000_000
	microsecond := remaining % 1_000_000
	return postgresSQLiteTimeText(
		int(hour),
		int(minute),
		int(second),
		int(microsecond),
		precision,
	), nil
}

func parsePostgresSQLiteEndOfDay(value any) (pgtype.Time, error) {
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case []byte:
		if !utf8.Valid(typed) {
			return pgtype.Time{}, fmt.Errorf("invalid TIME text")
		}
		text = string(typed)
	default:
		return pgtype.Time{}, fmt.Errorf("invalid TIME value")
	}
	whole, fraction, hasFraction := strings.Cut(text, ".")
	if whole != "24:00:00" {
		return pgtype.Time{}, fmt.Errorf("invalid TIME text")
	}
	if hasFraction {
		if fraction == "" || len(fraction) > 6 {
			return pgtype.Time{}, fmt.Errorf("invalid TIME fraction")
		}
		for _, digit := range fraction {
			if digit != '0' {
				return pgtype.Time{}, fmt.Errorf(
					"24:00:00 cannot have a non-zero fraction",
				)
			}
		}
	}
	return pgtype.Time{
		Microseconds: int64(24 * time.Hour / time.Microsecond),
		Valid:        true,
	}, nil
}

func postgresSQLiteTimeText(
	hour int,
	minute int,
	second int,
	microsecond int,
	precision int,
) string {
	text := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	if precision > 0 {
		fraction := fmt.Sprintf("%06d", microsecond)
		text += "." + fraction[:precision]
	}
	return text
}

func postgresSQLiteFractionExact(
	nanoseconds int,
	precision int,
) bool {
	if precision < 0 || precision > 6 {
		return false
	}
	if nanoseconds < 0 || nanoseconds >= int(time.Second) {
		return false
	}
	unit := int(time.Second)
	for digits := 0; digits < precision; digits++ {
		unit /= 10
	}
	return nanoseconds%unit == 0
}
