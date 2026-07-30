package schema

import (
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

type sqlServerCatalogDefaultTarget uint8

const (
	sqlServerCatalogDefaultUnknown sqlServerCatalogDefaultTarget = iota
	sqlServerCatalogDefaultBoolean
	sqlServerCatalogDefaultInteger
	sqlServerCatalogDefaultNumeric
	sqlServerCatalogDefaultFloat
	sqlServerCatalogDefaultString
	sqlServerCatalogDefaultBinary
	sqlServerCatalogDefaultDate
	sqlServerCatalogDefaultTime
	sqlServerCatalogDefaultTimestamp
)

// ParseSQLServerCatalogDefault converts one SQL Server 2022
// sys.default_constraints.definition into DMTX's structured expression
// contract. SQL Server's redundant catalog parentheses are removed only after
// their balance and literal boundaries have been proven. Every accepted value
// is then reconstructed from a typed scalar; executable catalog text is never
// retained.
func ParseSQLServerCatalogDefault(
	column Column,
	definition *string,
) (*Expression, error) {
	if definition == nil {
		return nil, nil
	}
	value, err := unwrapSQLServerCatalogDefault(*definition)
	if err != nil {
		return nil, sqlServerCatalogDefaultPolicy(column)
	}
	target := sqlServerCatalogDefaultTargetForColumn(column)
	if target == sqlServerCatalogDefaultUnknown {
		return nil, sqlServerCatalogDefaultPolicy(column)
	}

	if strings.EqualFold(value, "NULL") {
		return &Expression{sql: "NULL", kind: expressionNull}, nil
	}

	if literal, matched, err := parseSQLServerStringLiteral(value); matched {
		canonical, valid := canonicalSQLServerCatalogString(
			column,
			literal,
		)
		if err != nil ||
			target != sqlServerCatalogDefaultString ||
			!valid {
			return nil, sqlServerCatalogDefaultPolicy(column)
		}
		return &Expression{
			sql:     portableCheckStringLiteral(canonical),
			kind:    expressionString,
			literal: canonical,
		}, nil
	}

	switch target {
	case sqlServerCatalogDefaultBoolean:
		switch value {
		case "0":
			return &Expression{
				sql:     "FALSE",
				kind:    expressionBoolean,
				literal: "FALSE",
			}, nil
		case "1":
			return &Expression{
				sql:     "TRUE",
				kind:    expressionBoolean,
				literal: "TRUE",
			}, nil
		}
	case sqlServerCatalogDefaultInteger:
		canonical, err := canonicalSQLServerIntegerDefault(column, value)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case sqlServerCatalogDefaultNumeric:
		canonical, err := canonicalSQLServerNumericDefault(column, value)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case sqlServerCatalogDefaultFloat:
		canonical, err := canonicalSQLServerFloatDefault(value)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case sqlServerCatalogDefaultBinary:
		canonical, err := canonicalSQLServerBinaryDefault(column, value)
		if err == nil {
			return &Expression{
				sql:     "X'" + canonical + "'",
				kind:    expressionBlob,
				literal: canonical,
			}, nil
		}
	case sqlServerCatalogDefaultTimestamp:
		// SQL Server's UTC clock functions have source-type-specific
		// precision and rounding, and they are evaluated differently from
		// PostgreSQL's statement clock. Keep them fail-closed until the
		// neutral expression model can represent those semantics exactly.
	case sqlServerCatalogDefaultDate, sqlServerCatalogDefaultTime:
		// SQL Server has no UTC date-only or time-only builtin. Expressing one
		// requires a cast or conversion, which this catalog boundary rejects.
	}
	return nil, sqlServerCatalogDefaultPolicy(column)
}

func unwrapSQLServerCatalogDefault(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		strings.ContainsRune(value, '\uFFFD') {
		return "", sqlServerCatalogExpressionError{}
	}
	for {
		wrapped, err := sqlServerSingleOuterParentheses(value)
		if err != nil {
			return "", err
		}
		if !wrapped {
			return value, nil
		}
		value = strings.TrimSpace(value[1 : len(value)-1])
		if value == "" {
			return "", sqlServerCatalogExpressionError{}
		}
	}
}

func sqlServerSingleOuterParentheses(value string) (bool, error) {
	depth := 0
	firstClosesAtEnd := false
	for index := 0; index < len(value); {
		switch value[index] {
		case '\'':
			next, _, err := consumeSQLServerString(value, index)
			if err != nil {
				return false, err
			}
			index = next
		case '[':
			next, _, err := consumeSQLServerBracketIdentifier(value, index)
			if err != nil {
				return false, err
			}
			index = next
		case '(':
			depth++
			index++
		case ')':
			depth--
			if depth < 0 {
				return false, sqlServerCatalogExpressionError{}
			}
			index++
			if depth == 0 && value[0] == '(' {
				firstClosesAtEnd = index == len(value)
			}
		default:
			index++
		}
	}
	if depth != 0 {
		return false, sqlServerCatalogExpressionError{}
	}
	return len(value) >= 2 &&
		value[0] == '(' &&
		value[len(value)-1] == ')' &&
		firstClosesAtEnd, nil
}

func parseSQLServerStringLiteral(
	value string,
) (literal string, matched bool, err error) {
	start := 0
	if len(value) >= 2 &&
		(value[0] == 'N' || value[0] == 'n') &&
		value[1] == '\'' {
		start = 1
	}
	if start >= len(value) || value[start] != '\'' {
		return "", false, nil
	}
	next, literal, err := consumeSQLServerString(value, start)
	if err != nil || next != len(value) {
		return "", true, sqlServerCatalogExpressionError{}
	}
	return literal, true, nil
}

func consumeSQLServerString(
	value string,
	start int,
) (next int, literal string, err error) {
	if start >= len(value) || value[start] != '\'' {
		return 0, "", sqlServerCatalogExpressionError{}
	}
	var decoded strings.Builder
	for index := start + 1; index < len(value); {
		if value[index] != '\'' {
			decoded.WriteByte(value[index])
			index++
			continue
		}
		if index+1 < len(value) && value[index+1] == '\'' {
			decoded.WriteByte('\'')
			index += 2
			continue
		}
		return index + 1, decoded.String(), nil
	}
	return 0, "", sqlServerCatalogExpressionError{}
}

func consumeSQLServerBracketIdentifier(
	value string,
	start int,
) (next int, identifier string, err error) {
	if start >= len(value) || value[start] != '[' {
		return 0, "", sqlServerCatalogExpressionError{}
	}
	var decoded strings.Builder
	for index := start + 1; index < len(value); {
		if value[index] != ']' {
			decoded.WriteByte(value[index])
			index++
			continue
		}
		if index+1 < len(value) && value[index+1] == ']' {
			decoded.WriteByte(']')
			index += 2
			continue
		}
		return index + 1, decoded.String(), nil
	}
	return 0, "", sqlServerCatalogExpressionError{}
}

func canonicalSQLServerIntegerDefault(
	column Column,
	value string,
) (string, error) {
	if value == "" || strings.ContainsAny(value, ".eE") {
		return "", sqlServerCatalogExpressionError{}
	}
	base := sqlServerCatalogColumnBase(column)
	if base == "tinyint" {
		unsigned := strings.TrimPrefix(value, "+")
		if unsigned == value && strings.HasPrefix(value, "-") {
			return "", sqlServerCatalogExpressionError{}
		}
		parsed, err := strconv.ParseUint(unsigned, 10, 8)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(parsed, 10), nil
	}
	bits := 32
	switch base {
	case "smallint":
		bits = 16
	case "bigint", "int8":
		bits = 64
	case "int", "integer", "int4":
	default:
		return "", sqlServerCatalogExpressionError{}
	}
	return canonicalPostgresIntegerDefault(value, bits)
}

func canonicalSQLServerNumericDefault(
	column Column,
	value string,
) (string, error) {
	canonical, err := canonicalPostgresNumericDefault(value)
	if err != nil {
		return "", err
	}
	precision, scale, ok := sqlServerCatalogNumericModifiers(column)
	if !ok ||
		validatePostgresDecimalLiteral(value, precision, scale) != nil {
		return "", sqlServerCatalogExpressionError{}
	}
	return canonical, nil
}

func canonicalSQLServerFloatDefault(value string) (string, error) {
	// A decimal token is portable; exponent spellings and SQL Server's
	// special floating-point conversions are intentionally outside this
	// structural catalog subset.
	if strings.ContainsAny(value, "eE") {
		return "", sqlServerCatalogExpressionError{}
	}
	return canonicalPostgresDoubleDefault(value)
}

func sqlServerCatalogNumericModifiers(
	column Column,
) (precision int, scale int, ok bool) {
	base := sqlServerCatalogColumnBase(column)
	switch base {
	case "money":
		return 19, 4, true
	case "smallmoney":
		return 10, 4, true
	case "numeric", "decimal":
		if column.DeclaredType == nil {
			return 38, 10, true
		}
		if len(column.DeclaredType.Arguments) != 2 {
			return 0, 0, false
		}
		precision = column.DeclaredType.Arguments[0]
		scale = column.DeclaredType.Arguments[1]
		return precision, scale,
			precision >= 1 && precision <= 38 &&
				scale >= 0 && scale <= precision
	default:
		return 0, 0, false
	}
}

func canonicalSQLServerBinaryDefault(
	column Column,
	value string,
) (string, error) {
	if len(value) < 2 || !strings.EqualFold(value[:2], "0x") {
		return "", sqlServerCatalogExpressionError{}
	}
	canonical := strings.ToLower(value[2:])
	if len(canonical)%2 != 0 || !isLowerHex(canonical) {
		return "", sqlServerCatalogExpressionError{}
	}
	base := sqlServerCatalogColumnBase(column)
	if base != "binary" && base != "varbinary" &&
		base != "image" && base != "blob" && base != "bytea" {
		return "", sqlServerCatalogExpressionError{}
	}
	if (base == "binary" || base == "varbinary") &&
		column.DeclaredType != nil {
		if len(column.DeclaredType.Arguments) != 1 ||
			column.DeclaredType.Arguments[0] < 0 {
			return "", sqlServerCatalogExpressionError{}
		}
		length := column.DeclaredType.Arguments[0]
		bytes := len(canonical) / 2
		if bytes > length || base == "binary" && bytes != length {
			return "", sqlServerCatalogExpressionError{}
		}
	}
	return canonical, nil
}

func canonicalSQLServerCatalogString(
	column Column,
	value string,
) (string, bool) {
	if !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		strings.ContainsRune(value, '\uFFFD') {
		return "", false
	}
	if column.DeclaredType == nil ||
		len(column.DeclaredType.Arguments) == 0 {
		return value, true
	}
	base := sqlServerCatalogColumnBase(column)
	switch base {
	case "char", "varchar", "nchar", "nvarchar":
	default:
		return "", false
	}
	if len(column.DeclaredType.Arguments) != 1 ||
		column.DeclaredType.Arguments[0] <= 0 {
		return "", false
	}
	length := column.DeclaredType.Arguments[0]
	used := 0
	switch base {
	case "char", "varchar":
		// Admitted narrow SQL Server strings use the UTF-8 BIN2
		// collation, so their modifier is an encoded-byte limit.
		used = len(value)
	case "nchar", "nvarchar":
		// SQL Server's national-string modifier counts UTF-16 code units.
		used = len(utf16.Encode([]rune(value)))
	}
	if used > length {
		return "", false
	}
	if base == "char" || base == "nchar" {
		value += strings.Repeat(" ", length-used)
	}
	return value, true
}

func sqlServerCatalogDefaultTargetForColumn(
	column Column,
) sqlServerCatalogDefaultTarget {
	switch sqlServerCatalogColumnBase(column) {
	case "bit", "bool", "boolean":
		return sqlServerCatalogDefaultBoolean
	case "tinyint", "smallint", "int", "integer", "int4", "bigint", "int8":
		return sqlServerCatalogDefaultInteger
	case "numeric", "decimal", "money", "smallmoney":
		return sqlServerCatalogDefaultNumeric
	case "float", "double", "double precision":
		return sqlServerCatalogDefaultFloat
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext",
		"character", "character varying":
		return sqlServerCatalogDefaultString
	case "binary", "varbinary", "image", "blob", "bytea":
		return sqlServerCatalogDefaultBinary
	case "date":
		return sqlServerCatalogDefaultDate
	case "time":
		return sqlServerCatalogDefaultTime
	case "datetime", "datetime2", "smalldatetime", "timestamp",
		"timestamptz", "datetimeoffset":
		return sqlServerCatalogDefaultTimestamp
	default:
		return sqlServerCatalogDefaultUnknown
	}
}

func sqlServerCatalogColumnBase(column Column) string {
	value := column.Type
	if column.DeclaredType != nil {
		value = column.DeclaredType.Base
	}
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

// ParseSQLServerCatalogCheck converts one trusted, enabled SQL Server 2022
// sys.check_constraints.definition into the existing portable CHECK contract.
// Bracket identifiers and SQL Server N-prefixed strings are decoded
// structurally, then the common typed parser reconstructs the expression.
// SQL Server functions, casts, collations, arithmetic, and other dialect-only
// grammar therefore fail closed.
func ParseSQLServerCatalogCheck(
	definition string,
	columns []Column,
) (Expression, error) {
	normalized, err := normalizeSQLServerCatalogCheck(definition)
	if err != nil {
		return Expression{}, sqlServerCatalogCheckPolicy(
			"invalid catalog CHECK text",
		)
	}
	resolver, err := newPortableCheckColumnResolver(columns)
	if err != nil {
		return Expression{}, sqlServerCatalogCheckPolicy(
			"invalid CHECK columns",
		)
	}
	tokens, err := lexPortableCheck(normalized)
	if err != nil {
		return Expression{}, sqlServerCatalogCheckPolicy(
			"unsupported catalog CHECK grammar",
		)
	}
	parser := portableCheckParser{tokens: tokens, resolver: resolver}
	root, err := parser.parseExpression()
	if err != nil ||
		parser.current().kind != portableCheckTokenEOF ||
		root.family != portableCheckBoolean ||
		sqlServerCatalogCheckUsesNonportableColumn(root, columns) {
		return Expression{}, sqlServerCatalogCheckPolicy(
			"unsupported catalog CHECK grammar",
		)
	}
	expression := Expression{
		sql: renderCanonicalPortableCheck(
			root,
			portableCheckPrecedenceLowest,
		),
		kind: expressionCheck,
	}
	roundTrip, err := parsePlannedPostgresCheck(expression, columns)
	if err != nil ||
		makePostgresCheckSignature(roundTrip) !=
			makePostgresCheckSignature(root) {
		return Expression{}, sqlServerCatalogCheckPolicy(
			"catalog CHECK does not round-trip",
		)
	}
	return expression, nil
}

func sqlServerCatalogCheckUsesNonportableColumn(
	node *portableCheckNode,
	columns []Column,
) bool {
	if node == nil {
		return false
	}
	if node.kind == portableCheckNodeColumn {
		for _, column := range columns {
			if strings.EqualFold(column.Name, node.text) &&
				sqlServerCatalogColumnHasNonportableComparison(
					column,
				) {
				return true
			}
		}
	}
	if sqlServerCatalogCheckUsesNonportableColumn(node.left, columns) ||
		sqlServerCatalogCheckUsesNonportableColumn(node.right, columns) {
		return true
	}
	for _, child := range node.children {
		if sqlServerCatalogCheckUsesNonportableColumn(child, columns) {
			return true
		}
	}
	return false
}

func sqlServerCatalogColumnHasNonportableComparison(column Column) bool {
	switch sqlServerCatalogColumnBase(column) {
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext",
		"character", "character varying",
		"real":
		return true
	default:
		return false
	}
}

func normalizeSQLServerCatalogCheck(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		strings.ContainsRune(value, '\uFFFD') {
		return "", sqlServerCatalogExpressionError{}
	}
	var normalized strings.Builder
	normalized.Grow(len(value))
	for index := 0; index < len(value); {
		switch {
		case value[index] == '[':
			next, identifier, err :=
				consumeSQLServerBracketIdentifier(value, index)
			if err != nil || identifier == "" ||
				!utf8.ValidString(identifier) ||
				strings.ContainsRune(identifier, '\x00') ||
				strings.ContainsRune(identifier, '\uFFFD') {
				return "", sqlServerCatalogExpressionError{}
			}
			normalized.WriteByte('"')
			normalized.WriteString(strings.ReplaceAll(identifier, `"`, `""`))
			normalized.WriteByte('"')
			index = next
		case value[index] == '\'':
			next, literal, err := consumeSQLServerString(value, index)
			if err != nil || !utf8.ValidString(literal) ||
				strings.ContainsRune(literal, '\x00') ||
				strings.ContainsRune(literal, '\uFFFD') {
				return "", sqlServerCatalogExpressionError{}
			}
			normalized.WriteString(portableCheckStringLiteral(literal))
			index = next
		case (value[index] == 'N' || value[index] == 'n') &&
			index+1 < len(value) &&
			value[index+1] == '\'':
			next, literal, err := consumeSQLServerString(value, index+1)
			if err != nil || !utf8.ValidString(literal) ||
				strings.ContainsRune(literal, '\x00') ||
				strings.ContainsRune(literal, '\uFFFD') {
				return "", sqlServerCatalogExpressionError{}
			}
			normalized.WriteString(portableCheckStringLiteral(literal))
			index = next
		case value[index] == '"':
			// sys.check_constraints uses bracket quoting. Accepting
			// QUOTED_IDENTIFIER-dependent text would make catalog meaning
			// depend on session state.
			return "", sqlServerCatalogExpressionError{}
		default:
			normalized.WriteByte(value[index])
			index++
		}
	}
	return normalized.String(), nil
}

func sqlServerCatalogDefaultPolicy(column Column) error {
	return &PolicyError{
		Operation: "parse SQL Server catalog default",
		Type:      column.Name,
		Target:    string(SQLServer),
	}
}

func sqlServerCatalogCheckPolicy(reason string) error {
	return &PolicyError{
		Operation: "parse SQL Server catalog CHECK",
		Type:      reason,
		Target:    string(SQLServer),
	}
}

type sqlServerCatalogExpressionError struct{}

func (sqlServerCatalogExpressionError) Error() string {
	return "invalid SQL Server catalog expression"
}
