package migrate

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// sqlServerSourceRows converts the two raw byte representations exposed by
// go-mssqldb that are not safe to pass through database/sql unchanged.
// DECIMAL/NUMERIC bytes would otherwise be bound as VARBINARY by a target
// driver, and UNIQUEIDENTIFIER bytes need an explicit canonical UUID
// representation. Every conversion is validated against discovered metadata.
type sqlServerSourceRows struct {
	adapterRows
	columns []sqlServerSourceValueColumn
}

type sqlServerSourceValueKind uint8

const (
	sqlServerSourceValueInvalid sqlServerSourceValueKind = iota
	sqlServerSourceValueNumeric
	sqlServerSourceValueUUID
)

type sqlServerSourceValueColumn struct {
	index     int
	name      string
	kind      sqlServerSourceValueKind
	precision int
	scale     int
	reason    string
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
	raw, ok := value.([]byte)
	if !ok {
		return nil, sqlServerSourceValueError(
			column,
			"driver returned an unexpected value shape",
		)
	}
	switch column.kind {
	case sqlServerSourceValueNumeric:
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
		if len(raw) != 16 {
			return nil, sqlServerSourceValueError(
				column,
				"driver returned an invalid UUID",
			)
		}
		return canonicalSQLServerSourceUUID(raw), nil
	default:
		return nil, sqlServerSourceValueError(
			column,
			"unsupported value conversion",
		)
	}
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
