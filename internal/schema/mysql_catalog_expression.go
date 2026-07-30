package schema

import (
	"strings"
	"unicode/utf8"
)

type mysqlCatalogDefaultTarget uint8

const (
	mysqlCatalogDefaultUnknown mysqlCatalogDefaultTarget = iota
	mysqlCatalogDefaultBoolean
	mysqlCatalogDefaultInteger
	mysqlCatalogDefaultFloat
	mysqlCatalogDefaultNumeric
	mysqlCatalogDefaultString
	mysqlCatalogDefaultTime
	mysqlCatalogDefaultDate
	mysqlCatalogDefaultTimestamp
)

// ParseMySQLCatalogDefault converts one MySQL 8 COLUMN_DEFAULT value into
// DMTX's structured expression contract. defaultGenerated must reflect the
// DEFAULT_GENERATED token in information_schema.columns.EXTRA. Generated
// defaults fail closed except for the supported type-appropriate CURRENT_*
// forms.
//
// MySQL reports string defaults as unquoted values. This function quotes those
// values from their literal payload and reconstructs every other accepted
// expression from a parsed scalar, so catalog SQL is never retained as
// executable text.
func ParseMySQLCatalogDefault(
	column Column,
	columnDefault *string,
	defaultGenerated bool,
) (*Expression, error) {
	if columnDefault == nil {
		if defaultGenerated {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		// MySQL exposes both an absent default and DEFAULT NULL as SQL NULL in
		// COLUMN_DEFAULT. They have the same DMTX planning semantics.
		return nil, nil
	}

	target := mysqlCatalogDefaultTargetForColumn(column)
	if target == mysqlCatalogDefaultUnknown {
		return nil, mysqlCatalogDefaultPolicy(column)
	}

	if target == mysqlCatalogDefaultString {
		if defaultGenerated ||
			!utf8.ValidString(*columnDefault) ||
			strings.ContainsRune(*columnDefault, '\x00') {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		return &Expression{
			sql:     portableCheckStringLiteral(*columnDefault),
			kind:    expressionString,
			literal: *columnDefault,
		}, nil
	}

	value := strings.TrimSpace(*columnDefault)
	if value == "" {
		return nil, mysqlCatalogDefaultPolicy(column)
	}
	if strings.EqualFold(value, "NULL") {
		if defaultGenerated {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		return &Expression{sql: "NULL", kind: expressionNull}, nil
	}

	if defaultGenerated {
		return parseMySQLCatalogCurrentDefault(column, target, value)
	}

	switch target {
	case mysqlCatalogDefaultBoolean:
		switch strings.ToUpper(value) {
		case "TRUE", "1":
			return &Expression{
				sql:     "TRUE",
				kind:    expressionBoolean,
				literal: "TRUE",
			}, nil
		case "FALSE", "0":
			return &Expression{
				sql:     "FALSE",
				kind:    expressionBoolean,
				literal: "FALSE",
			}, nil
		}
	case mysqlCatalogDefaultInteger:
		canonical, err := canonicalPostgresIntegerDefault(value, 64)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case mysqlCatalogDefaultFloat:
		canonical, err := canonicalPostgresDoubleDefault(value)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case mysqlCatalogDefaultNumeric:
		canonical, err := canonicalPostgresNumericDefault(value)
		if err == nil {
			return &Expression{
				sql:     canonical,
				kind:    expressionNumber,
				literal: canonical,
			}, nil
		}
	case mysqlCatalogDefaultTime,
		mysqlCatalogDefaultDate,
		mysqlCatalogDefaultTimestamp:
		return parseMySQLCatalogCurrentDefault(column, target, value)
	}
	return nil, mysqlCatalogDefaultPolicy(column)
}

func parseMySQLCatalogCurrentDefault(
	column Column,
	target mysqlCatalogDefaultTarget,
	value string,
) (*Expression, error) {
	keyword, ok := canonicalMySQLCurrentKeyword(value)
	if !ok {
		return nil, mysqlCatalogDefaultPolicy(column)
	}

	expression := &Expression{sql: keyword}
	switch keyword {
	case "CURRENT_TIME":
		if target != mysqlCatalogDefaultTime {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentTime
	case "CURRENT_DATE":
		if target != mysqlCatalogDefaultDate {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentDate
	case "CURRENT_TIMESTAMP":
		if target != mysqlCatalogDefaultTimestamp {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentTimestamp
	default:
		return nil, mysqlCatalogDefaultPolicy(column)
	}
	return expression, nil
}

func canonicalMySQLCurrentKeyword(value string) (string, bool) {
	value = strings.TrimSpace(value)
	for hasSingleOuterParentheses(value) {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	if strings.HasSuffix(value, "()") {
		value = strings.TrimSpace(value[:len(value)-2])
	}
	keyword := strings.ToUpper(value)
	switch keyword {
	case "CURTIME":
		return "CURRENT_TIME", true
	case "CURDATE":
		return "CURRENT_DATE", true
	case "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
		return keyword, true
	default:
		return "", false
	}
}

func mysqlCatalogDefaultTargetForColumn(
	column Column,
) mysqlCatalogDefaultTarget {
	canonical := strings.ToLower(strings.Join(strings.Fields(column.Type), " "))
	if canonical == "bool" || canonical == "boolean" {
		// MySQL reports BOOLEAN as TINYINT(1). Source discovery retains that
		// declaration while using Column.Type to record the proven semantic
		// mapping.
		return mysqlCatalogDefaultBoolean
	}
	base := column.Type
	if column.DeclaredType != nil {
		base = column.DeclaredType.Base
	}
	base = strings.ToLower(strings.Join(strings.Fields(base), " "))
	switch base {
	case "bool", "boolean":
		return mysqlCatalogDefaultBoolean
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return mysqlCatalogDefaultInteger
	case "float", "double", "double precision", "real":
		return mysqlCatalogDefaultFloat
	case "decimal", "numeric":
		return mysqlCatalogDefaultNumeric
	case "char", "character", "varchar", "character varying", "text",
		"tinytext", "mediumtext", "longtext":
		return mysqlCatalogDefaultString
	case "time":
		return mysqlCatalogDefaultTime
	case "date":
		return mysqlCatalogDefaultDate
	case "datetime", "timestamp":
		return mysqlCatalogDefaultTimestamp
	default:
		return mysqlCatalogDefaultUnknown
	}
}

func mysqlCatalogDefaultPolicy(column Column) error {
	return &PolicyError{
		Operation: "parse MySQL catalog default",
		Type:      column.Name,
		Target:    string(MySQL),
	}
}

// ParseMySQLCatalogCheck converts one MySQL 8 CHECK_CLAUSE into DMTX's
// portable CHECK expression. The portable parser accepts MySQL backtick
// identifiers, resolves them against columns, and the returned expression is
// reconstructed from the parsed AST rather than retaining catalog text.
func ParseMySQLCatalogCheck(
	checkClause string,
	columns []Column,
) (Expression, error) {
	normalized, err := normalizeMySQLCatalogCheckClause(checkClause)
	if err != nil {
		return Expression{}, err
	}
	portableColumns := mysqlPortableCheckColumns(columns)
	resolver, err := newPortableCheckColumnResolver(portableColumns)
	if err != nil {
		return Expression{}, err
	}
	tokens, err := lexPortableCheck(normalized)
	if err != nil {
		return Expression{}, err
	}
	parser := portableCheckParser{tokens: tokens, resolver: resolver}
	root, err := parser.parseExpression()
	if err != nil {
		return Expression{}, err
	}
	if parser.current().kind != portableCheckTokenEOF {
		return Expression{}, portableCheckPolicy(
			"unexpected MySQL catalog CHECK token",
		)
	}
	if root.family != portableCheckBoolean {
		return Expression{}, portableCheckPolicy(
			"MySQL catalog CHECK root is not boolean",
		)
	}

	expression := Expression{
		sql: renderCanonicalPortableCheck(
			root,
			portableCheckPrecedenceLowest,
		),
		kind: expressionCheck,
	}
	roundTrip, err := parsePlannedPostgresCheck(expression, portableColumns)
	if err != nil {
		return Expression{}, err
	}
	if makePostgresCheckSignature(roundTrip) !=
		makePostgresCheckSignature(root) {
		return Expression{}, portableCheckPolicy(
			"reconstructed MySQL catalog CHECK does not round-trip",
		)
	}
	return expression, nil
}

func mysqlPortableCheckColumns(columns []Column) []Column {
	portable := append([]Column(nil), columns...)
	for index := range portable {
		canonical := strings.ToLower(strings.Join(
			strings.Fields(portable[index].Type),
			" ",
		))
		declaration := portable[index].DeclaredType
		if (canonical == "bool" || canonical == "boolean") &&
			declaration != nil &&
			strings.EqualFold(strings.TrimSpace(declaration.Base), "tinyint") &&
			len(declaration.Arguments) == 1 &&
			declaration.Arguments[0] == 1 {
			portable[index].DeclaredType = nil
		}
	}
	return portable
}

var supportedMySQLCheckCharacterSets = map[string]bool{
	"ascii":   true,
	"latin1":  true,
	"utf8":    true,
	"utf8mb3": true,
	"utf8mb4": true,
}

// normalizeMySQLCatalogCheckClause decodes the constrained character-set
// introduced literal form returned by MySQL 8 CHECK_CLAUSE. MySQL adds one
// catalog escaping layer around SQL string delimiters, for example
// _utf8mb4\'active\'. Only known text character sets and structurally parsed
// literal escapes are accepted.
func normalizeMySQLCatalogCheckClause(value string) (string, error) {
	var normalized strings.Builder
	normalized.Grow(len(value))
	for index := 0; index < len(value); {
		literal, next, matched, err := parseMySQLCatalogCheckString(
			value,
			index,
		)
		if err != nil {
			return "", err
		}
		if matched {
			if !utf8.ValidString(literal) ||
				strings.ContainsRune(literal, '\x00') {
				return "", portableCheckPolicy(
					"invalid MySQL catalog CHECK string",
				)
			}
			normalized.WriteString(portableCheckStringLiteral(literal))
			index = next
			continue
		}
		normalized.WriteByte(value[index])
		index++
	}
	return normalized.String(), nil
}

func parseMySQLCatalogCheckString(
	value string,
	start int,
) (literal string, next int, matched bool, err error) {
	if start >= len(value) || value[start] != '_' {
		return "", start, false, nil
	}
	index := start + 1
	for index < len(value) && portableCheckIdentifierPart(value[index]) {
		index++
	}
	if index == start+1 {
		return "", start, false, nil
	}
	escapedCatalogDelimiter := index+1 < len(value) &&
		value[index] == '\\' && value[index+1] == '\''
	plainDelimiter := index < len(value) && value[index] == '\''
	if !escapedCatalogDelimiter && !plainDelimiter {
		return "", start, false, nil
	}

	characterSet := strings.ToLower(value[start+1 : index])
	if !supportedMySQLCheckCharacterSets[characterSet] {
		return "", 0, true, portableCheckPolicy(
			"unsupported MySQL CHECK string character set",
		)
	}
	if escapedCatalogDelimiter {
		index += 2
		literal, next, err = parseEscapedMySQLCatalogCheckString(value, index)
	} else {
		index++
		literal, next, err = parsePlainMySQLCatalogCheckString(value, index)
	}
	return literal, next, true, err
}

func parsePlainMySQLCatalogCheckString(
	value string,
	index int,
) (string, int, error) {
	var literal strings.Builder
	for index < len(value) {
		switch value[index] {
		case '\'':
			if index+1 < len(value) && value[index+1] == '\'' {
				literal.WriteByte('\'')
				index += 2
				continue
			}
			return literal.String(), index + 1, nil
		case '\\':
			if index+1 >= len(value) {
				break
			}
			decoded, ok := decodeMySQLCheckStringEscape(value[index+1])
			if !ok {
				return "", 0, portableCheckPolicy(
					"unsupported MySQL CHECK string escape",
				)
			}
			literal.WriteByte(decoded)
			index += 2
		default:
			literal.WriteByte(value[index])
			index++
		}
	}
	return "", 0, portableCheckPolicy(
		"unterminated MySQL catalog CHECK string",
	)
}

func parseEscapedMySQLCatalogCheckString(
	value string,
	index int,
) (string, int, error) {
	var literal strings.Builder
	for index < len(value) {
		if value[index] != '\\' {
			if value[index] == '\'' {
				return "", 0, portableCheckPolicy(
					"unexpected MySQL CHECK string delimiter",
				)
			}
			literal.WriteByte(value[index])
			index++
			continue
		}

		runStart := index
		for index < len(value) && value[index] == '\\' {
			index++
		}
		runLength := index - runStart
		if index >= len(value) {
			return "", 0, portableCheckPolicy(
				"unterminated MySQL catalog CHECK string",
			)
		}
		if value[index] == '\'' {
			if runLength%2 == 0 {
				return "", 0, portableCheckPolicy(
					"invalid MySQL catalog CHECK quote escape",
				)
			}
			sqlBackslashes := (runLength - 1) / 2
			for count := 0; count < sqlBackslashes/2; count++ {
				literal.WriteByte('\\')
			}
			if sqlBackslashes%2 == 0 {
				return literal.String(), index + 1, nil
			}
			literal.WriteByte('\'')
			index++
			continue
		}
		if runLength%2 != 0 {
			return "", 0, portableCheckPolicy(
				"invalid MySQL catalog CHECK escape",
			)
		}
		sqlBackslashes := runLength / 2
		for count := 0; count < sqlBackslashes/2; count++ {
			literal.WriteByte('\\')
		}
		if sqlBackslashes%2 != 0 {
			decoded, ok := decodeMySQLCheckStringEscape(value[index])
			if !ok {
				return "", 0, portableCheckPolicy(
					"unsupported MySQL CHECK string escape",
				)
			}
			literal.WriteByte(decoded)
			index++
		}
	}
	return "", 0, portableCheckPolicy(
		"unterminated MySQL catalog CHECK string",
	)
}

func decodeMySQLCheckStringEscape(value byte) (byte, bool) {
	switch value {
	case '\'', '"', '\\':
		return value, true
	case 'b':
		return '\b', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	case 'Z':
		return 0x1a, true
	default:
		return 0, false
	}
}
