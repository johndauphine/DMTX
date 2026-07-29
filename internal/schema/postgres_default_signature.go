package schema

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// PostgresDefaultKind identifies one PostgreSQL default shape whose
// pg_get_expr representation DMTX can compare without copying executable
// catalog text into a planned schema.
type PostgresDefaultKind string

const (
	PostgresDefaultBoolean          PostgresDefaultKind = "boolean"
	PostgresDefaultInteger          PostgresDefaultKind = "integer"
	PostgresDefaultBigint           PostgresDefaultKind = "bigint"
	PostgresDefaultDoublePrecision  PostgresDefaultKind = "double_precision"
	PostgresDefaultNumeric          PostgresDefaultKind = "numeric"
	PostgresDefaultText             PostgresDefaultKind = "text"
	PostgresDefaultChar             PostgresDefaultKind = "char"
	PostgresDefaultVarchar          PostgresDefaultKind = "varchar"
	PostgresDefaultBytea            PostgresDefaultKind = "bytea"
	PostgresDefaultCurrentTime      PostgresDefaultKind = "current_time"
	PostgresDefaultCurrentDate      PostgresDefaultKind = "current_date"
	PostgresDefaultCurrentTimestamp PostgresDefaultKind = "current_timestamp"
)

// PostgresDefaultSignature is a comparable, non-executable description of a
// supported PostgreSQL default. Present is false both when no default was
// planned and when SQLite DEFAULT NULL becomes an absent pg_attrdef row.
type PostgresDefaultSignature struct {
	Present bool
	Kind    PostgresDefaultKind
	Value   string
}

type postgresDefaultTargetKind uint8

const (
	postgresDefaultTargetUnknown postgresDefaultTargetKind = iota
	postgresDefaultTargetBoolean
	postgresDefaultTargetInteger
	postgresDefaultTargetBigint
	postgresDefaultTargetDoublePrecision
	postgresDefaultTargetNumeric
	postgresDefaultTargetText
	postgresDefaultTargetChar
	postgresDefaultTargetVarchar
	postgresDefaultTargetBytea
	postgresDefaultTargetTime
	postgresDefaultTargetDate
	postgresDefaultTargetTimestamp
)

var (
	postgresCatalogQuotedNumberPattern = regexp.MustCompile(
		`^'([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)'::(integer|bigint|numeric|doubleprecision)$`,
	)
	postgresCatalogByteaDefaultPattern = regexp.MustCompile(
		`^decode\('([0-9A-Fa-f]*)'::text,'hex'::text\)$`,
	)
)

const (
	postgresCatalogCurrentTime = "" +
		"date_trunc('second'::text," +
		"(statement_timestamp()ATTIMEZONE'UTC'::text))" +
		"::timewithouttimezone"
	postgresCatalogCurrentDate = "" +
		"(statement_timestamp()ATTIMEZONE'UTC'::text)::date"
	postgresCatalogCurrentTimestamp = "" +
		"date_trunc('second'::text," +
		"(statement_timestamp()ATTIMEZONE'UTC'::text))"
)

// PlannedPostgresDefaultSignature returns the canonical signature of the
// default DMTX will render for one PostgreSQL column. It rejects a default that
// renderPostgresDefault does not support, and also rejects any supported
// renderer branch without an exact retained-catalog comparison contract.
func PlannedPostgresDefaultSignature(
	column Column,
) (PostgresDefaultSignature, error) {
	if column.Default == nil || column.Default.kind == expressionNull {
		return PostgresDefaultSignature{}, nil
	}
	if _, err := renderPostgresDefault(column); err != nil {
		return PostgresDefaultSignature{}, err
	}
	target, err := postgresDefaultTarget(column)
	if err != nil {
		return PostgresDefaultSignature{}, err
	}

	expression := column.Default
	switch expression.kind {
	case expressionBoolean:
		if target != postgresDefaultTargetBoolean {
			break
		}
		return presentPostgresDefault(
			PostgresDefaultBoolean,
			strings.ToLower(expression.literal),
		), nil
	case expressionNumber:
		return plannedPostgresNumberDefaultSignature(
			column,
			target,
			expression.literal,
		)
	case expressionString:
		switch target {
		case postgresDefaultTargetText:
			return presentPostgresDefault(
				PostgresDefaultText,
				expression.literal,
			), nil
		case postgresDefaultTargetChar:
			return presentPostgresDefault(
				PostgresDefaultChar,
				expression.literal,
			), nil
		case postgresDefaultTargetVarchar:
			return presentPostgresDefault(
				PostgresDefaultVarchar,
				expression.literal,
			), nil
		}
	case expressionBlob:
		if target != postgresDefaultTargetBytea {
			break
		}
		return presentPostgresDefault(
			PostgresDefaultBytea,
			strings.ToLower(expression.literal),
		), nil
	case expressionCurrentTime:
		if target != postgresDefaultTargetTime {
			break
		}
		return presentPostgresDefault(PostgresDefaultCurrentTime, ""), nil
	case expressionCurrentDate:
		if target != postgresDefaultTargetDate {
			break
		}
		return presentPostgresDefault(PostgresDefaultCurrentDate, ""), nil
	case expressionCurrentTimestamp:
		if target != postgresDefaultTargetTimestamp {
			break
		}
		return presentPostgresDefault(
			PostgresDefaultCurrentTimestamp,
			"",
		), nil
	}
	return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
		"canonicalize planned PostgreSQL default",
		column,
	)
}

func plannedPostgresNumberDefaultSignature(
	column Column,
	target postgresDefaultTargetKind,
	value string,
) (PostgresDefaultSignature, error) {
	switch target {
	case postgresDefaultTargetInteger:
		canonical, err := canonicalPostgresIntegerDefault(value, 32)
		if err != nil {
			break
		}
		return presentPostgresDefault(PostgresDefaultInteger, canonical), nil
	case postgresDefaultTargetBigint:
		canonical, err := canonicalPostgresIntegerDefault(value, 64)
		if err != nil {
			break
		}
		return presentPostgresDefault(PostgresDefaultBigint, canonical), nil
	case postgresDefaultTargetDoublePrecision:
		canonical, err := canonicalPostgresDoubleDefault(value)
		if err != nil {
			break
		}
		return presentPostgresDefault(
			PostgresDefaultDoublePrecision,
			canonical,
		), nil
	case postgresDefaultTargetNumeric:
		canonical, err := canonicalPostgresNumericDefault(value)
		if err != nil {
			break
		}
		return presentPostgresDefault(
			PostgresDefaultNumeric,
			canonical,
		), nil
	}
	return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
		"canonicalize planned PostgreSQL default",
		column,
	)
}

// CatalogPostgresDefaultSignature converts PostgreSQL 16 pg_get_expr output
// into the same canonical signature as the DMTX plan. A nil expression means
// there is no pg_attrdef row. Only exact, known deparse shapes are accepted.
func CatalogPostgresDefaultSignature(
	column Column,
	pgGetExpr *string,
) (PostgresDefaultSignature, error) {
	if pgGetExpr == nil {
		return PostgresDefaultSignature{}, nil
	}
	compacted, ok := compactPostgresDefaultDeparse(*pgGetExpr)
	if !ok || compacted == "" {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	target, err := postgresDefaultTarget(column)
	if err != nil {
		return PostgresDefaultSignature{}, err
	}

	switch target {
	case postgresDefaultTargetBoolean:
		if compacted == "true" || compacted == "false" {
			return presentPostgresDefault(
				PostgresDefaultBoolean,
				compacted,
			), nil
		}
	case postgresDefaultTargetInteger, postgresDefaultTargetBigint:
		return catalogPostgresIntegerDefaultSignature(
			column,
			target,
			compacted,
		)
	case postgresDefaultTargetDoublePrecision:
		return catalogPostgresDoubleDefaultSignature(column, compacted)
	case postgresDefaultTargetNumeric:
		return catalogPostgresNumericDefaultSignature(
			column,
			compacted,
		)
	case postgresDefaultTargetText:
		if value, ok := parsePostgresCatalogStringCast(
			compacted,
			"text",
		); ok {
			return presentPostgresDefault(PostgresDefaultText, value), nil
		}
	case postgresDefaultTargetChar:
		if value, ok := parsePostgresCatalogStringCast(
			compacted,
			"bpchar",
		); ok {
			return presentPostgresDefault(PostgresDefaultChar, value), nil
		}
	case postgresDefaultTargetVarchar:
		if value, ok := parsePostgresCatalogStringCast(
			compacted,
			"charactervarying",
		); ok {
			return presentPostgresDefault(PostgresDefaultVarchar, value), nil
		}
	case postgresDefaultTargetBytea:
		match := postgresCatalogByteaDefaultPattern.FindStringSubmatch(compacted)
		if len(match) == 2 && len(match[1])%2 == 0 {
			return presentPostgresDefault(
				PostgresDefaultBytea,
				strings.ToLower(match[1]),
			), nil
		}
	case postgresDefaultTargetTime:
		if compacted == postgresCatalogCurrentTime {
			return presentPostgresDefault(PostgresDefaultCurrentTime, ""), nil
		}
	case postgresDefaultTargetDate:
		if compacted == postgresCatalogCurrentDate {
			return presentPostgresDefault(PostgresDefaultCurrentDate, ""), nil
		}
	case postgresDefaultTargetTimestamp:
		if compacted == postgresCatalogCurrentTimestamp {
			return presentPostgresDefault(
				PostgresDefaultCurrentTimestamp,
				"",
			), nil
		}
	}
	return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
		"canonicalize PostgreSQL catalog default",
		column,
	)
}

func catalogPostgresIntegerDefaultSignature(
	column Column,
	target postgresDefaultTargetKind,
	compacted string,
) (PostgresDefaultSignature, error) {
	value, cast, ok := parsePostgresCatalogNumber(compacted)
	if !ok ||
		cast != "" && cast != "integer" &&
			!(target == postgresDefaultTargetBigint && cast == "bigint") {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	bits := 32
	if target == postgresDefaultTargetBigint {
		bits = 64
	}
	canonical, err := canonicalPostgresIntegerDefault(value, bits)
	if err != nil ||
		validatePostgresNumberDefault(column, canonical) != nil {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	if target == postgresDefaultTargetInteger &&
		cast != "" && cast != "integer" {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	if target == postgresDefaultTargetBigint && cast == "integer" {
		if _, err := strconv.ParseInt(value, 10, 32); err != nil {
			return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
				"canonicalize PostgreSQL catalog default",
				column,
			)
		}
	}
	kind := PostgresDefaultInteger
	if target == postgresDefaultTargetBigint {
		kind = PostgresDefaultBigint
	}
	return presentPostgresDefault(kind, canonical), nil
}

func catalogPostgresDoubleDefaultSignature(
	column Column,
	compacted string,
) (PostgresDefaultSignature, error) {
	value, cast, ok := parsePostgresCatalogNumber(compacted)
	if !ok ||
		cast != "" && cast != "integer" && cast != "bigint" &&
			cast != "numeric" && cast != "doubleprecision" ||
		validatePostgresNumberDefault(column, value) != nil {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	canonical, err := canonicalPostgresDoubleDefault(value)
	if err != nil {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	return presentPostgresDefault(
		PostgresDefaultDoublePrecision,
		canonical,
	), nil
}

func catalogPostgresNumericDefaultSignature(
	column Column,
	compacted string,
) (PostgresDefaultSignature, error) {
	value, cast, ok := parsePostgresCatalogNumber(compacted)
	if !ok ||
		cast != "" && cast != "integer" && cast != "bigint" &&
			cast != "numeric" ||
		validatePostgresNumberDefault(column, value) != nil {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	canonical, err := canonicalPostgresNumericDefault(value)
	if err != nil {
		return PostgresDefaultSignature{}, postgresDefaultSignaturePolicy(
			"canonicalize PostgreSQL catalog default",
			column,
		)
	}
	return presentPostgresDefault(PostgresDefaultNumeric, canonical), nil
}

func parsePostgresCatalogNumber(
	compacted string,
) (value string, cast string, ok bool) {
	if sqliteNumberPattern.MatchString(compacted) {
		return compacted, "", true
	}
	if strings.HasPrefix(compacted, "(+") &&
		strings.HasSuffix(compacted, ")") {
		inner := compacted[2 : len(compacted)-1]
		if inner != "" &&
			inner[0] != '+' &&
			inner[0] != '-' &&
			sqliteNumberPattern.MatchString(inner) {
			return "+" + inner, "", true
		}
	}
	match := postgresCatalogQuotedNumberPattern.FindStringSubmatch(compacted)
	if len(match) == 3 {
		return match[1], match[2], true
	}
	return "", "", false
}

func canonicalPostgresIntegerDefault(
	value string,
	bits int,
) (string, error) {
	parsed, err := strconv.ParseInt(value, 10, bits)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(parsed, 10), nil
}

func canonicalPostgresDoubleDefault(value string) (string, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return "", fmt.Errorf("invalid floating-point default")
	}
	if parsed == 0 {
		// PostgreSQL 16 folds every DMTX-rendered negative-zero literal to 0.
		parsed = 0
	}
	return strconv.FormatFloat(parsed, 'g', -1, 64), nil
}

func canonicalPostgresNumericDefault(value string) (string, error) {
	if !sqliteNumberPattern.MatchString(value) ||
		strings.ContainsAny(value, "eE") {
		return "", fmt.Errorf("invalid numeric default")
	}
	negative := false
	if value[0] == '+' || value[0] == '-' {
		negative = value[0] == '-'
		value = value[1:]
	}
	whole, fraction, found := strings.Cut(value, ".")
	if !found {
		fraction = ""
	}
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	fraction = strings.TrimRight(fraction, "0")
	if whole == "0" && fraction == "" {
		negative = false
	}
	result := whole
	if fraction != "" {
		result += "." + fraction
	}
	if negative {
		result = "-" + result
	}
	return result, nil
}

// PostgresDefaultSignaturesMatch compares one planned default with one
// pg_get_expr result. A supported but different value returns false; an
// unrecognized catalog form returns a classifiable error.
func PostgresDefaultSignaturesMatch(
	column Column,
	pgGetExpr *string,
) (bool, error) {
	planned, err := PlannedPostgresDefaultSignature(column)
	if err != nil {
		return false, err
	}
	actual, err := CatalogPostgresDefaultSignature(column, pgGetExpr)
	if err != nil {
		return false, err
	}
	return planned == actual, nil
}

func presentPostgresDefault(
	kind PostgresDefaultKind,
	value string,
) PostgresDefaultSignature {
	return PostgresDefaultSignature{
		Present: true,
		Kind:    kind,
		Value:   value,
	}
}

func postgresDefaultTarget(
	column Column,
) (postgresDefaultTargetKind, error) {
	rendered, err := renderColumnType(column, Postgres)
	if err != nil {
		// TIME is already a safe renderPostgresDefault branch. It remains
		// explicit here until the general cross-dialect type mapper owns TIME.
		if postgresColumnIs(column, "time") {
			return postgresDefaultTargetTime, nil
		}
		return postgresDefaultTargetUnknown, err
	}
	normalized := strings.ToUpper(strings.Join(strings.Fields(rendered), " "))
	switch {
	case normalized == "BOOLEAN":
		return postgresDefaultTargetBoolean, nil
	case normalized == "INTEGER":
		return postgresDefaultTargetInteger, nil
	case normalized == "BIGINT":
		return postgresDefaultTargetBigint, nil
	case normalized == "DOUBLE PRECISION":
		return postgresDefaultTargetDoublePrecision, nil
	case normalized == "TEXT":
		return postgresDefaultTargetText, nil
	case strings.HasPrefix(normalized, "CHAR("):
		return postgresDefaultTargetChar, nil
	case strings.HasPrefix(normalized, "VARCHAR("):
		return postgresDefaultTargetVarchar, nil
	case strings.HasPrefix(normalized, "NUMERIC("),
		strings.HasPrefix(normalized, "DECIMAL("):
		return postgresDefaultTargetNumeric, nil
	case normalized == "BYTEA":
		return postgresDefaultTargetBytea, nil
	case normalized == "DATE":
		return postgresDefaultTargetDate, nil
	case normalized == "TIMESTAMP" ||
		strings.HasPrefix(normalized, "TIMESTAMP(") &&
			!strings.Contains(normalized, "WITH TIME ZONE"):
		return postgresDefaultTargetTimestamp, nil
	default:
		return postgresDefaultTargetUnknown, postgresDefaultSignaturePolicy(
			"classify PostgreSQL default target",
			column,
		)
	}
}

func compactPostgresDefaultDeparse(value string) (string, bool) {
	var result strings.Builder
	result.Grow(len(value))
	inLiteral := false
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current == '\'' {
			result.WriteByte(current)
			if inLiteral && index+1 < len(value) && value[index+1] == '\'' {
				index++
				result.WriteByte(value[index])
				continue
			}
			inLiteral = !inLiteral
			continue
		}
		if !inLiteral {
			switch current {
			case ' ', '\t', '\r', '\n':
				continue
			}
		}
		result.WriteByte(current)
	}
	return result.String(), !inLiteral
}

func parsePostgresCatalogStringCast(
	compacted string,
	cast string,
) (string, bool) {
	escaped := false
	literalStart := 0
	if strings.HasPrefix(compacted, "E'") {
		escaped = true
		literalStart = 1
	}
	if literalStart >= len(compacted) || compacted[literalStart] != '\'' {
		return "", false
	}
	var value strings.Builder
	for index := literalStart + 1; index < len(compacted); index++ {
		current := compacted[index]
		if current == '\'' {
			if index+1 < len(compacted) && compacted[index+1] == '\'' {
				value.WriteByte('\'')
				index++
				continue
			}
			return value.String(), compacted[index+1:] == "::"+cast
		}
		if escaped && current == '\\' {
			if index+1 >= len(compacted) || compacted[index+1] != '\\' {
				return "", false
			}
			value.WriteByte('\\')
			index++
			continue
		}
		value.WriteByte(current)
	}
	return "", false
}

func postgresDefaultSignaturePolicy(
	operation string,
	column Column,
) error {
	return &PolicyError{
		Operation: operation,
		Type:      column.Name,
		Target:    string(Postgres),
	}
}
