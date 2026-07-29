package schema

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const postgresMaximumCharacterLength = 10_485_760

func renderPostgresDeclaredType(value DeclaredType) (string, error) {
	base := strings.ToLower(strings.TrimSpace(value.Base))
	switch base {
	case "char", "character":
		if len(value.Arguments) != 1 ||
			value.Arguments[0] <= 0 ||
			value.Arguments[0] > postgresMaximumCharacterLength {
			return "", postgresDeclaredTypePolicy(value)
		}
		return fmt.Sprintf("CHAR(%d)", value.Arguments[0]), nil
	case "varchar", "character varying":
		if len(value.Arguments) != 1 ||
			value.Arguments[0] <= 0 ||
			value.Arguments[0] > postgresMaximumCharacterLength {
			return "", postgresDeclaredTypePolicy(value)
		}
		return fmt.Sprintf("VARCHAR(%d)", value.Arguments[0]), nil
	case "numeric", "decimal":
		precision, scale, ok := postgresNumericModifiers(value)
		if !ok {
			return "", postgresDeclaredTypePolicy(value)
		}
		return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), nil
	case "timestamp", "datetime":
		if len(value.Arguments) != 1 ||
			value.Arguments[0] < 0 ||
			value.Arguments[0] > 6 {
			return "", postgresDeclaredTypePolicy(value)
		}
		return fmt.Sprintf("TIMESTAMP(%d)", value.Arguments[0]), nil
	case "timestamptz":
		if len(value.Arguments) != 1 ||
			value.Arguments[0] < 0 ||
			value.Arguments[0] > 6 {
			return "", postgresDeclaredTypePolicy(value)
		}
		return fmt.Sprintf(
			"TIMESTAMP(%d) WITH TIME ZONE",
			value.Arguments[0],
		), nil
	default:
		if len(value.Arguments) != 0 {
			return "", postgresDeclaredTypePolicy(value)
		}
		return MapType(base, Postgres)
	}
}

func postgresDeclaredTypePolicy(value DeclaredType) error {
	return &PolicyError{
		Operation: "render PostgreSQL declared type",
		Type:      declaredTypeDescription(value),
		Target:    string(Postgres),
	}
}

func declaredTypeDescription(value DeclaredType) string {
	if len(value.Arguments) == 0 {
		return value.Base
	}
	arguments := make([]string, len(value.Arguments))
	for index, argument := range value.Arguments {
		arguments[index] = strconv.Itoa(argument)
	}
	return value.Base + "(" + strings.Join(arguments, ",") + ")"
}

func postgresNumericModifiers(
	value DeclaredType,
) (precision int, scale int, ok bool) {
	if len(value.Arguments) < 1 || len(value.Arguments) > 2 {
		return 0, 0, false
	}
	precision = value.Arguments[0]
	if len(value.Arguments) == 2 {
		scale = value.Arguments[1]
	}
	if precision < 1 || precision > 1000 || scale < 0 || scale > precision {
		return 0, 0, false
	}
	return precision, scale, true
}

func renderPostgresDefault(column Column) (string, error) {
	expression := column.Default
	if expression == nil {
		return "", postgresDefaultPolicy(column)
	}
	switch expression.kind {
	case expressionNull:
		return "NULL", nil
	case expressionBoolean:
		if !postgresColumnIs(column, "bool", "boolean") {
			return "", postgresDefaultPolicy(column)
		}
		return expression.literal, nil
	case expressionNumber:
		if err := validatePostgresNumberDefault(column, expression.literal); err != nil {
			return "", postgresDefaultPolicy(column)
		}
		return expression.literal, nil
	case expressionString:
		if !postgresColumnIs(
			column,
			"text",
			"char",
			"character",
			"varchar",
			"character varying",
		) ||
			strings.ContainsRune(expression.literal, '\x00') ||
			!utf8.ValidString(expression.literal) {
			return "", postgresDefaultPolicy(column)
		}
		if length, ok := postgresCharacterLength(column); ok &&
			utf8.RuneCountInString(expression.literal) > length {
			return "", postgresDefaultPolicy(column)
		}
		return postgresStringLiteral(expression.literal), nil
	case expressionBlob:
		if !postgresColumnIs(column, "blob", "binary", "varbinary", "bytea") {
			return "", postgresDefaultPolicy(column)
		}
		return "decode('" + strings.ToLower(expression.literal) + "', 'hex')", nil
	case expressionCurrentTime:
		if !postgresColumnIs(column, "time") {
			return "", postgresDefaultPolicy(column)
		}
		return "(date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'))::time", nil
	case expressionCurrentDate:
		if !postgresColumnIs(column, "date") {
			return "", postgresDefaultPolicy(column)
		}
		return "(statement_timestamp() AT TIME ZONE 'UTC')::date", nil
	case expressionCurrentTimestamp:
		if !postgresColumnIs(column, "timestamp", "datetime") {
			return "", postgresDefaultPolicy(column)
		}
		return "date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')", nil
	default:
		return "", postgresDefaultPolicy(column)
	}
}

func postgresDefaultPolicy(column Column) error {
	return &PolicyError{
		Operation: "render PostgreSQL default",
		Type:      column.Name,
		Target:    string(Postgres),
	}
}

func postgresStringLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, "'", "''")
	return "E'" + escaped + "'"
}

func postgresColumnIs(column Column, candidates ...string) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	for _, candidate := range candidates {
		if base == candidate {
			return true
		}
	}
	return false
}

func postgresCharacterLength(column Column) (int, bool) {
	if column.DeclaredType == nil {
		return 0, false
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	if base != "char" && base != "character" &&
		base != "varchar" && base != "character varying" {
		return 0, false
	}
	if len(column.DeclaredType.Arguments) != 1 {
		return 0, false
	}
	return column.DeclaredType.Arguments[0], true
}

func validatePostgresNumberDefault(column Column, value string) error {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "int", "integer", "int4":
		if _, err := strconv.ParseInt(value, 10, 32); err != nil {
			return fmt.Errorf("invalid integer")
		}
		return nil
	case "bigint", "int8":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return fmt.Errorf("invalid bigint")
		}
		return nil
	case "real", "float", "float4", "double", "double precision", "float8":
		number, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
			return fmt.Errorf("invalid floating-point number")
		}
		return nil
	case "decimal", "numeric":
		precision, scale := 38, 10
		if column.DeclaredType != nil {
			var ok bool
			precision, scale, ok = postgresNumericModifiers(
				*column.DeclaredType,
			)
			if !ok {
				return fmt.Errorf("invalid numeric modifiers")
			}
		}
		return validatePostgresDecimalLiteral(value, precision, scale)
	default:
		return fmt.Errorf("numeric literal is incompatible with column")
	}
}

func validatePostgresDecimalLiteral(
	value string,
	precision int,
	scale int,
) error {
	if value == "" || strings.ContainsAny(value, "eE") {
		return fmt.Errorf("numeric exponent is unsupported")
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	whole, fraction, found := strings.Cut(value, ".")
	if !found {
		fraction = ""
	}
	if whole == "" && fraction == "" {
		return fmt.Errorf("empty numeric")
	}
	if (whole != "" && !decimalDigits(whole)) ||
		(fraction != "" && !decimalDigits(fraction)) {
		return fmt.Errorf("invalid numeric")
	}
	for len(fraction) > scale {
		if fraction[len(fraction)-1] != '0' {
			return fmt.Errorf("numeric exceeds scale")
		}
		fraction = fraction[:len(fraction)-1]
	}
	integerDigits := len(strings.TrimLeft(whole, "0"))
	if integerDigits > precision-scale {
		return fmt.Errorf("numeric exceeds precision")
	}
	return nil
}

func decimalDigits(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
