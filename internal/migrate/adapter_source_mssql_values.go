package migrate

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

// sqlServerSourceRows converts the raw representations exposed by go-mssqldb
// that are not safe to pass through database/sql unchanged.
// DECIMAL/NUMERIC bytes would otherwise be bound as VARBINARY by a target
// driver, and UNIQUEIDENTIFIER bytes need an explicit canonical UUID
// representation. SQL Server TIME is decoded as a year-one time.Time, which
// network target drivers can misinterpret as a datetime, so it is emitted as
// exact clock text. DATE and DATETIME values retain time.Time after their
// driver shape and declared precision are validated. Every conversion is
// validated against discovered metadata.
type sqlServerSourceRows struct {
	adapterRows
	columns []sqlServerSourceValueColumn
}

type sqlServerSourceValueKind uint8

const (
	sqlServerSourceValueInvalid sqlServerSourceValueKind = iota
	sqlServerSourceValueNumeric
	sqlServerSourceValueUUID
	sqlServerSourceValueDate
	sqlServerSourceValueTime
	sqlServerSourceValueDateTime
)

type sqlServerSourceValueColumn struct {
	index        int
	name         string
	kind         sqlServerSourceValueKind
	precision    int
	scale        int
	declaredBase string
	reason       string
}

func wrapSQLServerSourceRows(
	rows adapterRows,
	table schema.Table,
	columns []string,
) adapterRows {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}

	converted := make([]sqlServerSourceValueColumn, 0)
	for index, name := range columns {
		column, ok := metadata[name]
		if !ok {
			converted = append(converted, sqlServerSourceValueColumn{
				index:  index,
				name:   name,
				reason: "column is absent from discovered schema",
			})
			continue
		}
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "decimal", "numeric":
			value := sqlServerNumericSourceColumn(index, column)
			converted = append(converted, value)
		case "uuid":
			value := sqlServerUUIDSourceColumn(index, column)
			converted = append(converted, value)
		case "date":
			value := sqlServerDateSourceColumn(index, column)
			converted = append(converted, value)
		case "time":
			value := sqlServerTimeSourceColumn(index, column)
			converted = append(converted, value)
		case "datetime":
			value := sqlServerDateTimeSourceColumn(index, column)
			converted = append(converted, value)
		}
	}
	if len(converted) == 0 {
		return rows
	}
	return &sqlServerSourceRows{
		adapterRows: rows,
		columns:     converted,
	}
}

func sqlServerNumericSourceColumn(
	index int,
	column schema.Column,
) sqlServerSourceValueColumn {
	value := sqlServerSourceValueColumn{
		index: index,
		name:  column.Name,
		kind:  sqlServerSourceValueInvalid,
	}
	if column.DeclaredType == nil {
		value.reason = "numeric declaration is missing"
		return value
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	arguments := column.DeclaredType.Arguments
	if (base != "decimal" && base != "numeric") ||
		len(arguments) != 2 ||
		arguments[0] < 1 ||
		arguments[0] > 38 ||
		arguments[1] < 0 ||
		arguments[1] > arguments[0] {
		value.reason = "numeric declaration is invalid"
		return value
	}
	value.kind = sqlServerSourceValueNumeric
	value.precision = arguments[0]
	value.scale = arguments[1]
	return value
}

func sqlServerUUIDSourceColumn(
	index int,
	column schema.Column,
) sqlServerSourceValueColumn {
	value := sqlServerSourceValueColumn{
		index: index,
		name:  column.Name,
		kind:  sqlServerSourceValueInvalid,
	}
	if column.DeclaredType == nil ||
		strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) != "uuid" ||
		len(column.DeclaredType.Arguments) != 0 {
		value.reason = "UUID declaration is invalid"
		return value
	}
	value.kind = sqlServerSourceValueUUID
	return value
}

func sqlServerDateSourceColumn(
	index int,
	column schema.Column,
) sqlServerSourceValueColumn {
	value := sqlServerSourceValueColumn{
		index: index,
		name:  column.Name,
		kind:  sqlServerSourceValueInvalid,
	}
	if column.DeclaredType == nil ||
		strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) != "date" ||
		len(column.DeclaredType.Arguments) != 0 {
		value.reason = "date declaration is invalid"
		return value
	}
	value.kind = sqlServerSourceValueDate
	return value
}

func sqlServerTimeSourceColumn(
	index int,
	column schema.Column,
) sqlServerSourceValueColumn {
	value := sqlServerSourceValueColumn{
		index: index,
		name:  column.Name,
		kind:  sqlServerSourceValueInvalid,
	}
	if column.DeclaredType == nil ||
		strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) != "time" ||
		len(column.DeclaredType.Arguments) != 1 ||
		column.DeclaredType.Arguments[0] < 0 ||
		column.DeclaredType.Arguments[0] > 6 {
		value.reason = "time declaration is invalid"
		return value
	}
	value.kind = sqlServerSourceValueTime
	value.scale = column.DeclaredType.Arguments[0]
	return value
}

func sqlServerDateTimeSourceColumn(
	index int,
	column schema.Column,
) sqlServerSourceValueColumn {
	value := sqlServerSourceValueColumn{
		index: index,
		name:  column.Name,
		kind:  sqlServerSourceValueInvalid,
	}
	if column.DeclaredType == nil {
		value.reason = "datetime declaration is missing"
		return value
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	arguments := column.DeclaredType.Arguments
	switch base {
	case "timestamp":
		if len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			value.reason = "datetime declaration is invalid"
			return value
		}
		value.scale = arguments[0]
	case "smalldatetime":
		if len(arguments) != 0 {
			value.reason = "datetime declaration is invalid"
			return value
		}
		value.scale = 0
	default:
		value.reason = "datetime declaration is invalid"
		return value
	}
	value.kind = sqlServerSourceValueDateTime
	value.declaredBase = base
	return value
}

func (rows *sqlServerSourceRows) Scan(destinations ...any) error {
	if err := rows.adapterRows.Scan(destinations...); err != nil {
		return err
	}
	for _, column := range rows.columns {
		if column.index >= len(destinations) {
			return sqlServerSourceValueError(
				column,
				"scan destination is missing",
			)
		}
		destination, ok := destinations[column.index].(*any)
		if !ok {
			return sqlServerSourceValueError(
				column,
				"unsupported scan destination",
			)
		}
		if column.kind == sqlServerSourceValueInvalid {
			return sqlServerSourceValueError(column, column.reason)
		}
		normalized, err := normalizeSQLServerSourceValue(column, *destination)
		if err != nil {
			return err
		}
		*destination = normalized
	}
	return nil
}

func normalizeSQLServerSourceValue(
	column sqlServerSourceValueColumn,
	value any,
) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch column.kind {
	case sqlServerSourceValueNumeric:
		raw, ok := value.([]byte)
		if !ok {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an unexpected value shape",
			)
		}
		text := string(raw)
		if !validSQLServerSourceNumeric(
			text,
			column.precision,
			column.scale,
		) {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid exact numeric",
			)
		}
		return text, nil
	case sqlServerSourceValueUUID:
		raw, ok := value.([]byte)
		if !ok {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an unexpected value shape",
			)
		}
		if len(raw) != 16 {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid UUID",
			)
		}
		return canonicalSQLServerSourceUUID(raw), nil
	case sqlServerSourceValueDate:
		temporal, ok := sqlServerSourceTemporal(value)
		if !ok ||
			temporal.Hour() != 0 ||
			temporal.Minute() != 0 ||
			temporal.Second() != 0 ||
			temporal.Nanosecond() != 0 {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid date",
			)
		}
		return canonicalSQLServerSourceTemporal(temporal), nil
	case sqlServerSourceValueTime:
		temporal, ok := sqlServerSourceTemporal(value)
		if !ok ||
			temporal.Year() != 1 ||
			temporal.Month() != time.January ||
			temporal.Day() != 1 ||
			!validSQLServerSourceTemporalPrecision(
				temporal.Nanosecond(),
				column.scale,
			) {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid time",
			)
		}
		return canonicalSQLServerSourceTime(temporal, column.scale), nil
	case sqlServerSourceValueDateTime:
		temporal, ok := sqlServerSourceTemporal(value)
		if !ok ||
			!validSQLServerSourceTemporalPrecision(
				temporal.Nanosecond(),
				column.scale,
			) ||
			column.declaredBase == "smalldatetime" &&
				(temporal.Second() != 0 || temporal.Nanosecond() != 0) {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid datetime",
			)
		}
		return canonicalSQLServerSourceTemporal(temporal), nil
	default:
		return nil, sqlServerSourceValueError(
			column,
			"unsupported value conversion",
		)
	}
}

func sqlServerSourceTemporal(value any) (time.Time, bool) {
	temporal, ok := value.(time.Time)
	if !ok || temporal.Year() < 1 || temporal.Year() > 9999 {
		return time.Time{}, false
	}
	_, offset := temporal.Zone()
	if offset != 0 {
		return time.Time{}, false
	}
	return temporal, true
}

func canonicalSQLServerSourceTemporal(value time.Time) time.Time {
	return time.Date(
		value.Year(),
		value.Month(),
		value.Day(),
		value.Hour(),
		value.Minute(),
		value.Second(),
		value.Nanosecond(),
		time.UTC,
	)
}

func validSQLServerSourceTemporalPrecision(
	nanosecond int,
	scale int,
) bool {
	if nanosecond < 0 || nanosecond >= int(time.Second) ||
		scale < 0 || scale > 6 {
		return false
	}
	unit := 1
	for digits := scale; digits < 9; digits++ {
		unit *= 10
	}
	return nanosecond%unit == 0
}

func canonicalSQLServerSourceTime(
	value time.Time,
	scale int,
) string {
	result := fmt.Sprintf(
		"%02d:%02d:%02d",
		value.Hour(),
		value.Minute(),
		value.Second(),
	)
	if scale == 0 {
		return result
	}
	unit := 1
	for digits := scale; digits < 9; digits++ {
		unit *= 10
	}
	return result + fmt.Sprintf(
		".%0*d",
		scale,
		value.Nanosecond()/unit,
	)
}

func validSQLServerSourceNumeric(
	value string,
	precision int,
	scale int,
) bool {
	if value == "" || precision < 1 || precision > 38 ||
		scale < 0 || scale > precision {
		return false
	}
	if value[0] == '-' {
		value = value[1:]
	}
	if value == "" {
		return false
	}
	whole, fraction, hasDecimal := strings.Cut(value, ".")
	if whole == "" || !sqlServerSourceDigits(whole) {
		return false
	}
	if scale == 0 {
		if hasDecimal {
			return false
		}
	} else if !hasDecimal ||
		len(fraction) != scale ||
		!sqlServerSourceDigits(fraction) {
		return false
	}
	integerDigits := len(strings.TrimLeft(whole, "0"))
	return integerDigits <= precision-scale
}

func sqlServerSourceDigits(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func canonicalSQLServerSourceUUID(value []byte) string {
	var encoded [32]byte
	hex.Encode(encoded[:], value)
	return string(encoded[0:8]) + "-" +
		string(encoded[8:12]) + "-" +
		string(encoded[12:16]) + "-" +
		string(encoded[16:20]) + "-" +
		string(encoded[20:32])
}

func sqlServerSourceValueError(
	column sqlServerSourceValueColumn,
	reason string,
) error {
	return fmt.Errorf(
		"SQL Server source column %s cannot be converted safely: %s",
		column.name,
		reason,
	)
}
