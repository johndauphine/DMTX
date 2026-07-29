package migrate

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

// normalizePostgresRows validates and converts every value before a native
// connection or transaction is acquired. Conversion errors describe only the
// table position and expected type; row values are never included.
func normalizePostgresRows(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	tableColumns := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		tableColumns[column.Name] = column
	}
	ordered := make([]schema.Column, len(columns))
	for index, name := range columns {
		column, ok := tableColumns[name]
		if !ok {
			return nil, fmt.Errorf(
				"normalize PostgreSQL table %s: column %s is not present in schema",
				table.Name,
				name,
			)
		}
		ordered[index] = column
	}

	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf(
				"normalize PostgreSQL table %s row %d: got %d values for %d columns",
				table.Name,
				rowIndex+1,
				len(row),
				len(columns),
			)
		}
		normalized[rowIndex] = make([]any, len(row))
		for columnIndex, value := range row {
			column := ordered[columnIndex]
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf(
						"normalize PostgreSQL table %s row %d column %s: NULL is not allowed",
						table.Name,
						rowIndex+1,
						column.Name,
					)
				}
				continue
			}
			converted, err := normalizePostgresColumnValue(column, value)
			if err != nil {
				return nil, fmt.Errorf(
					"normalize PostgreSQL table %s row %d column %s as %s: %w",
					table.Name,
					rowIndex+1,
					column.Name,
					strings.ToLower(strings.TrimSpace(column.Type)),
					err,
				)
			}
			normalized[rowIndex][columnIndex] = converted
		}
	}
	return normalized, nil
}

func normalizePostgresValue(columnType string, value any) (any, error) {
	return normalizePostgresColumnValue(
		schema.Column{Type: columnType},
		value,
	)
}

func normalizePostgresColumnValue(column schema.Column, value any) (any, error) {
	columnType := strings.ToLower(strings.TrimSpace(column.Type))
	switch columnType {
	case "int", "integer", "int4":
		integer, err := exactPostgresInteger(value)
		if err != nil || !integer.IsInt64() {
			return nil, fmt.Errorf("expected a signed 32-bit integer")
		}
		number := integer.Int64()
		if number < math.MinInt32 || number > math.MaxInt32 {
			return nil, fmt.Errorf("integer is outside the signed 32-bit range")
		}
		return int32(number), nil
	case "bigint", "int8":
		integer, err := exactPostgresInteger(value)
		if err != nil || !integer.IsInt64() {
			return nil, fmt.Errorf("expected a signed 64-bit integer")
		}
		return integer.Int64(), nil
	case "real", "float", "float4", "double", "double precision", "float8":
		return normalizePostgresFloat(value)
	case "decimal", "numeric":
		precision, scale, err := postgresNumericColumnModifiers(column)
		if err != nil {
			return nil, err
		}
		return normalizePostgresNumericWithModifiers(
			value,
			precision,
			scale,
		)
	case "text", "char", "character", "varchar", "character varying":
		normalized, err := normalizePostgresText(value)
		if err != nil {
			return nil, err
		}
		length, constrained, err := postgresCharacterColumnLength(column)
		if err != nil {
			return nil, err
		}
		if constrained && utf8.RuneCountInString(normalized) > length {
			return nil, fmt.Errorf(
				"text exceeds PostgreSQL character length %d",
				length,
			)
		}
		return normalized, nil
	case "uuid":
		return normalizePostgresUUID(value)
	case "blob", "binary", "varbinary", "bytea":
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected binary bytes")
		}
		owned := make([]byte, len(bytes))
		copy(owned, bytes)
		return owned, nil
	case "json", "jsonb":
		return normalizePostgresJSON(value)
	case "bool", "boolean":
		return normalizePostgresBoolean(value)
	case "timestamp", "datetime":
		return normalizePostgresTimestamp(value)
	case "timestamptz":
		return normalizePostgresTimestamptz(value)
	case "date":
		return normalizePostgresDate(value)
	default:
		return nil, fmt.Errorf(
			"unsupported canonical PostgreSQL source type %q",
			columnType,
		)
	}
}

func postgresCharacterColumnLength(
	column schema.Column,
) (int, bool, error) {
	if column.DeclaredType == nil {
		return 0, false, nil
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	switch base {
	case "char", "character", "varchar", "character varying":
	default:
		return 0, false, nil
	}
	if len(column.DeclaredType.Arguments) != 1 {
		return 0, false, fmt.Errorf(
			"invalid PostgreSQL character length modifiers",
		)
	}
	length := column.DeclaredType.Arguments[0]
	if length <= 0 || length > 10_485_760 {
		return 0, false, fmt.Errorf(
			"invalid PostgreSQL character length %d",
			length,
		)
	}
	return length, true, nil
}

func postgresNumericColumnModifiers(
	column schema.Column,
) (int64, int32, error) {
	if column.DeclaredType == nil {
		return postgresDecimalPrecision, postgresDecimalScale, nil
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	if base != "numeric" && base != "decimal" {
		return 0, 0, fmt.Errorf(
			"invalid PostgreSQL numeric declaration %q",
			column.DeclaredType.Base,
		)
	}
	arguments := column.DeclaredType.Arguments
	if len(arguments) < 1 || len(arguments) > 2 ||
		arguments[0] < 1 || arguments[0] > 1000 {
		return 0, 0, fmt.Errorf(
			"invalid PostgreSQL numeric precision",
		)
	}
	scale := 0
	if len(arguments) == 2 {
		scale = arguments[1]
	}
	if scale < 0 || scale > arguments[0] {
		return 0, 0, fmt.Errorf("invalid PostgreSQL numeric scale")
	}
	return int64(arguments[0]), int32(scale), nil
}

func exactPostgresInteger(value any) (*big.Int, error) {
	switch number := value.(type) {
	case int:
		return big.NewInt(int64(number)), nil
	case int8:
		return big.NewInt(int64(number)), nil
	case int16:
		return big.NewInt(int64(number)), nil
	case int32:
		return big.NewInt(int64(number)), nil
	case int64:
		return big.NewInt(number), nil
	case uint:
		return new(big.Int).SetUint64(uint64(number)), nil
	case uint8:
		return new(big.Int).SetUint64(uint64(number)), nil
	case uint16:
		return new(big.Int).SetUint64(uint64(number)), nil
	case uint32:
		return new(big.Int).SetUint64(uint64(number)), nil
	case uint64:
		return new(big.Int).SetUint64(number), nil
	case string:
		return parsePostgresInteger(number)
	case []byte:
		return parsePostgresInteger(string(number))
	default:
		return nil, fmt.Errorf("expected an exact integer")
	}
}

func parsePostgresInteger(value string) (*big.Int, error) {
	if !isSignedDecimalDigits(value) {
		return nil, fmt.Errorf("expected an exact integer")
	}
	integer, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf("expected an exact integer")
	}
	return integer, nil
}

const (
	postgresDecimalPrecision int64 = 38
	postgresDecimalScale     int32 = 10
)

func normalizePostgresNumeric(value any) (pgtype.Numeric, error) {
	return normalizePostgresNumericWithModifiers(
		value,
		postgresDecimalPrecision,
		postgresDecimalScale,
	)
}

func normalizePostgresNumericWithModifiers(
	value any,
	precision int64,
	scale int32,
) (pgtype.Numeric, error) {
	if numeric, ok := value.(pgtype.Numeric); ok {
		if !numeric.Valid ||
			numeric.InfinityModifier != pgtype.Finite {
			return pgtype.Numeric{}, fmt.Errorf("expected a finite exact numeric")
		}
		if numeric.NaN {
			if numeric.Int != nil || numeric.Exp != 0 {
				return pgtype.Numeric{}, fmt.Errorf(
					"expected a valid PostgreSQL numeric NaN",
				)
			}
			return pgtype.Numeric{
				NaN:   true,
				Valid: true,
			}, nil
		}
		return fitPostgresDecimalTo(numeric, precision, scale)
	}

	var source string
	switch number := value.(type) {
	case string:
		source = number
	case []byte:
		source = string(number)
	default:
		integer, err := exactPostgresInteger(value)
		if err != nil {
			return pgtype.Numeric{}, fmt.Errorf(
				"expected an exact numeric string, bytes, or integer",
			)
		}
		return fitPostgresDecimalTo(pgtype.Numeric{
			Int:   integer,
			Valid: true,
		}, precision, scale)
	}
	if source == "NaN" {
		return pgtype.Numeric{
			NaN:   true,
			Valid: true,
		}, nil
	}
	numeric, err := parseExactPostgresNumeric(source)
	if err != nil {
		return pgtype.Numeric{}, err
	}
	return fitPostgresDecimalTo(numeric, precision, scale)
}

func parseExactPostgresNumeric(value string) (pgtype.Numeric, error) {
	if value == "" || strings.ContainsAny(value, "eE") {
		return pgtype.Numeric{}, fmt.Errorf(
			"expected a base-10 numeric without an exponent",
		)
	}
	sign := ""
	if value[0] == '+' || value[0] == '-' {
		sign = value[:1]
		value = value[1:]
	}
	if value == "" || strings.Count(value, ".") > 1 {
		return pgtype.Numeric{}, fmt.Errorf("expected an exact base-10 numeric")
	}
	whole, fraction, _ := strings.Cut(value, ".")
	if whole == "" && fraction == "" {
		return pgtype.Numeric{}, fmt.Errorf("expected an exact base-10 numeric")
	}
	if (whole != "" && !isDecimalDigits(whole)) ||
		(fraction != "" && !isDecimalDigits(fraction)) {
		return pgtype.Numeric{}, fmt.Errorf("expected an exact base-10 numeric")
	}
	digits := whole + fraction
	if sign == "-" {
		digits = "-" + digits
	}
	integer, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return pgtype.Numeric{}, fmt.Errorf("expected an exact base-10 numeric")
	}
	if len(fraction) > math.MaxInt32 {
		return pgtype.Numeric{}, fmt.Errorf("numeric scale is too large")
	}
	return pgtype.Numeric{
		Int:   integer,
		Exp:   -int32(len(fraction)),
		Valid: true,
	}, nil
}

// fitPostgresDecimal guarantees that PostgreSQL DECIMAL(38,10) can store the
// value without rounding. Fractional zeros are removed only when necessary to
// fit the target scale, so every accepted conversion is exact.
func fitPostgresDecimal(numeric pgtype.Numeric) (pgtype.Numeric, error) {
	return fitPostgresDecimalTo(
		numeric,
		postgresDecimalPrecision,
		postgresDecimalScale,
	)
}

func fitPostgresDecimalTo(
	numeric pgtype.Numeric,
	precision int64,
	scale int32,
) (pgtype.Numeric, error) {
	if !numeric.Valid ||
		numeric.NaN ||
		numeric.InfinityModifier != pgtype.Finite ||
		numeric.Int == nil {
		return pgtype.Numeric{}, fmt.Errorf("expected a finite exact numeric")
	}
	coefficient := new(big.Int).Set(numeric.Int)
	exponent := numeric.Exp

	if coefficient.Sign() == 0 {
		if exponent < -scale {
			exponent = -scale
		}
		return pgtype.Numeric{
			Int:   coefficient,
			Exp:   exponent,
			Valid: true,
		}, nil
	}

	ten := big.NewInt(10)
	for exponent < -scale {
		quotient := new(big.Int)
		remainder := new(big.Int)
		quotient.QuoRem(coefficient, ten, remainder)
		if remainder.Sign() != 0 {
			return pgtype.Numeric{}, fmt.Errorf(
				"exact numeric exceeds PostgreSQL DECIMAL(%d,%d) scale",
				precision, scale,
			)
		}
		coefficient = quotient
		exponent++
	}

	absolute := new(big.Int).Abs(new(big.Int).Set(coefficient))
	integerDigits := int64(len(absolute.String())) + int64(exponent)
	if integerDigits < 0 {
		integerDigits = 0
	}
	if integerDigits > precision-int64(scale) {
		return pgtype.Numeric{}, fmt.Errorf(
			"exact numeric exceeds PostgreSQL DECIMAL(%d,%d) integer precision",
			precision, scale,
		)
	}

	return pgtype.Numeric{
		Int:   coefficient,
		Exp:   exponent,
		Valid: true,
	}, nil
}

func normalizePostgresUUID(value any) (pgtype.UUID, error) {
	if uuid, ok := value.(pgtype.UUID); ok {
		if !uuid.Valid {
			return pgtype.UUID{}, fmt.Errorf("expected a valid UUID")
		}
		return uuid, nil
	}
	if bytes, ok := value.([]byte); ok {
		if len(bytes) == 16 {
			var raw [16]byte
			copy(raw[:], bytes)
			return pgtype.UUID{Bytes: raw, Valid: true}, nil
		}
		if !utf8.Valid(bytes) {
			return pgtype.UUID{}, fmt.Errorf("expected a valid UUID")
		}
		value = string(bytes)
	}
	text, ok := value.(string)
	if !ok {
		return pgtype.UUID{}, fmt.Errorf(
			"expected UUID text or 16 binary bytes",
		)
	}
	var uuid pgtype.UUID
	if err := uuid.Scan(text); err != nil {
		return pgtype.UUID{}, fmt.Errorf("expected a valid UUID")
	}
	return uuid, nil
}

func normalizePostgresJSON(value any) ([]byte, error) {
	var document []byte
	switch typed := value.(type) {
	case string:
		document = []byte(typed)
	case []byte:
		document = append([]byte(nil), typed...)
	default:
		return nil, fmt.Errorf("expected JSON text or bytes")
	}
	if !utf8.Valid(document) || !json.Valid(document) {
		return nil, fmt.Errorf("expected valid UTF-8 JSON")
	}
	return document, nil
}

func normalizePostgresText(value any) (string, error) {
	var normalized string
	switch text := value.(type) {
	case string:
		if !utf8.ValidString(text) {
			return "", fmt.Errorf("expected valid UTF-8 text")
		}
		normalized = text
	case []byte:
		if !utf8.Valid(text) {
			return "", fmt.Errorf("expected valid UTF-8 text")
		}
		normalized = string(text)
	default:
		return "", fmt.Errorf("expected UTF-8 text")
	}
	if strings.IndexByte(normalized, 0) >= 0 {
		return "", fmt.Errorf("PostgreSQL text cannot contain NUL")
	}
	return normalized, nil
}

func normalizePostgresBoolean(value any) (bool, error) {
	switch boolean := value.(type) {
	case bool:
		return boolean, nil
	case int:
		return postgresBooleanInteger(int64(boolean))
	case int8:
		return postgresBooleanInteger(int64(boolean))
	case int16:
		return postgresBooleanInteger(int64(boolean))
	case int32:
		return postgresBooleanInteger(int64(boolean))
	case int64:
		return postgresBooleanInteger(boolean)
	case uint:
		return postgresBooleanUnsigned(uint64(boolean))
	case uint8:
		return postgresBooleanUnsigned(uint64(boolean))
	case uint16:
		return postgresBooleanUnsigned(uint64(boolean))
	case uint32:
		return postgresBooleanUnsigned(uint64(boolean))
	case uint64:
		return postgresBooleanUnsigned(boolean)
	case string:
		return parsePostgresBoolean(boolean)
	case []byte:
		if len(boolean) == 1 && (boolean[0] == 0 || boolean[0] == 1) {
			return boolean[0] == 1, nil
		}
		return parsePostgresBoolean(string(boolean))
	default:
		return false, fmt.Errorf("expected a strict boolean")
	}
}

func postgresBooleanInteger(value int64) (bool, error) {
	if value == 0 {
		return false, nil
	}
	if value == 1 {
		return true, nil
	}
	return false, fmt.Errorf("expected boolean 0 or 1")
}

func postgresBooleanUnsigned(value uint64) (bool, error) {
	if value == 0 {
		return false, nil
	}
	if value == 1 {
		return true, nil
	}
	return false, fmt.Errorf("expected boolean 0 or 1")
}

func parsePostgresBoolean(value string) (bool, error) {
	switch value {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf(
			"expected boolean true, false, 1, or 0",
		)
	}
}

func normalizePostgresFloat(value any) (float64, error) {
	var number float64
	switch typed := value.(type) {
	case float32:
		number = float64(typed)
	case float64:
		number = typed
	case string:
		parsed, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, fmt.Errorf("expected a floating-point number")
		}
		number = parsed
	case []byte:
		parsed, err := strconv.ParseFloat(string(typed), 64)
		if err != nil {
			return 0, fmt.Errorf("expected a floating-point number")
		}
		number = parsed
	default:
		return 0, fmt.Errorf("expected a floating-point number")
	}
	return number, nil
}

func normalizePostgresTimestamp(value any) (pgtype.Timestamp, error) {
	switch timestamp := value.(type) {
	case pgtype.Timestamp:
		if !validPostgresTemporal(
			timestamp.Valid,
			timestamp.InfinityModifier,
		) {
			return pgtype.Timestamp{}, fmt.Errorf(
				"expected a valid PostgreSQL timestamp",
			)
		}
		if timestamp.InfinityModifier == pgtype.Finite {
			if err := requirePostgresMicrosecondPrecision(
				timestamp.Time,
			); err != nil {
				return pgtype.Timestamp{}, err
			}
		}
		return timestamp, nil
	case time.Time:
		if err := requirePostgresMicrosecondPrecision(timestamp); err != nil {
			return pgtype.Timestamp{}, err
		}
		return pgtype.Timestamp{Time: timestamp, Valid: true}, nil
	}
	text, err := postgresTemporalText(value)
	if err != nil {
		return pgtype.Timestamp{}, fmt.Errorf("expected a timestamp")
	}
	if modifier, ok := postgresTemporalInfinity(text); ok {
		return pgtype.Timestamp{
			InfinityModifier: modifier,
			Valid:            true,
		}, nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	} {
		if timestamp, parseErr := time.Parse(layout, text); parseErr == nil {
			if err := requirePostgresMicrosecondPrecision(
				timestamp,
			); err != nil {
				return pgtype.Timestamp{}, err
			}
			return pgtype.Timestamp{
				Time:  timestamp,
				Valid: true,
			}, nil
		}
	}
	return pgtype.Timestamp{}, fmt.Errorf("expected a valid timestamp")
}

func normalizePostgresTimestamptz(value any) (pgtype.Timestamptz, error) {
	switch timestamp := value.(type) {
	case pgtype.Timestamptz:
		if !validPostgresTemporal(
			timestamp.Valid,
			timestamp.InfinityModifier,
		) {
			return pgtype.Timestamptz{}, fmt.Errorf(
				"expected a valid PostgreSQL timestamptz",
			)
		}
		if timestamp.InfinityModifier == pgtype.Finite {
			if err := requirePostgresMicrosecondPrecision(
				timestamp.Time,
			); err != nil {
				return pgtype.Timestamptz{}, err
			}
		}
		return timestamp, nil
	case time.Time:
		if err := requirePostgresMicrosecondPrecision(timestamp); err != nil {
			return pgtype.Timestamptz{}, err
		}
		return pgtype.Timestamptz{Time: timestamp, Valid: true}, nil
	}
	text, err := postgresTemporalText(value)
	if err != nil {
		return pgtype.Timestamptz{}, fmt.Errorf("expected a timestamptz")
	}
	if modifier, ok := postgresTemporalInfinity(text); ok {
		return pgtype.Timestamptz{
			InfinityModifier: modifier,
			Valid:            true,
		}, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
	} {
		if timestamp, parseErr := time.Parse(layout, text); parseErr == nil {
			if err := requirePostgresMicrosecondPrecision(
				timestamp,
			); err != nil {
				return pgtype.Timestamptz{}, err
			}
			return pgtype.Timestamptz{
				Time:  timestamp,
				Valid: true,
			}, nil
		}
	}
	return pgtype.Timestamptz{}, fmt.Errorf(
		"expected a valid timestamptz",
	)
}

func requirePostgresMicrosecondPrecision(value time.Time) error {
	if value.Nanosecond()%int(time.Microsecond) != 0 {
		return fmt.Errorf(
			"timestamp precision exceeds PostgreSQL microseconds",
		)
	}
	return nil
}

func normalizePostgresDate(value any) (pgtype.Date, error) {
	switch date := value.(type) {
	case pgtype.Date:
		if !validPostgresTemporal(date.Valid, date.InfinityModifier) {
			return pgtype.Date{}, fmt.Errorf(
				"expected a valid PostgreSQL date",
			)
		}
		return date, nil
	case time.Time:
		return pgtype.Date{Time: date, Valid: true}, nil
	}
	text, err := postgresTemporalText(value)
	if err != nil {
		return pgtype.Date{}, fmt.Errorf("expected a date")
	}
	if modifier, ok := postgresTemporalInfinity(text); ok {
		return pgtype.Date{
			InfinityModifier: modifier,
			Valid:            true,
		}, nil
	}
	date, err := time.Parse("2006-01-02", text)
	if err != nil {
		return pgtype.Date{}, fmt.Errorf("expected a valid date")
	}
	return pgtype.Date{Time: date, Valid: true}, nil
}

func validPostgresTemporal(
	valid bool,
	modifier pgtype.InfinityModifier,
) bool {
	if !valid {
		return false
	}
	switch modifier {
	case pgtype.Finite, pgtype.Infinity, pgtype.NegativeInfinity:
		return true
	default:
		return false
	}
}

func postgresTemporalInfinity(
	value string,
) (pgtype.InfinityModifier, bool) {
	switch value {
	case "infinity":
		return pgtype.Infinity, true
	case "-infinity":
		return pgtype.NegativeInfinity, true
	default:
		return pgtype.Finite, false
	}
}

func postgresTemporalText(value any) (string, error) {
	switch temporal := value.(type) {
	case string:
		if !utf8.ValidString(temporal) {
			return "", fmt.Errorf("expected valid UTF-8 temporal text")
		}
		return temporal, nil
	case []byte:
		if !utf8.Valid(temporal) {
			return "", fmt.Errorf("expected valid UTF-8 temporal text")
		}
		return string(temporal), nil
	default:
		return "", fmt.Errorf("expected temporal text")
	}
}

func isSignedDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	return isDecimalDigits(value)
}

func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
