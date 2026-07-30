package schema

import (
	"strconv"
	"strings"
	"time"
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
	mysqlCatalogDefaultBlob
)

// ParseMySQLCatalogDefault converts one MySQL 8 COLUMN_DEFAULT value into
// DMTX's structured expression contract. defaultGenerated must reflect the
// DEFAULT_GENERATED token in information_schema.columns.EXTRA. Generated
// defaults fail closed except for the supported type-appropriate CURRENT_*
// forms and the structurally parsed expression forms MySQL emits for
// supported large-object literals.
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
		literal := *columnDefault
		if defaultGenerated {
			base := mysqlColumnBase(column)
			decoded, next, matched, err := parseMySQLCatalogCheckString(
				literal,
				0,
			)
			if err != nil ||
				!matched ||
				next != len(literal) ||
				!strings.HasPrefix(
					strings.ToLower(literal),
					"_utf8mb4",
				) ||
				!mysqlLargeObjectBase(base) {
				return nil, mysqlCatalogDefaultPolicy(column)
			}
			literal = decoded
		}
		if !utf8.ValidString(literal) ||
			strings.ContainsRune(literal, '\x00') {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		return &Expression{
			sql:     portableCheckStringLiteral(literal),
			kind:    expressionString,
			literal: literal,
		}, nil
	}
	if target == mysqlCatalogDefaultBlob {
		if !defaultGenerated {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		value := strings.TrimSpace(*columnDefault)
		if len(value) < 2 ||
			!strings.EqualFold(value[:2], "0x") ||
			len(value[2:])%2 != 0 ||
			!isLowerHex(strings.ToLower(value[2:])) {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		hexadecimal := strings.ToLower(value[2:])
		return &Expression{
			sql:     "X'" + hexadecimal + "'",
			kind:    expressionBlob,
			literal: hexadecimal,
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
		if literal, ok := canonicalMySQLStaticTemporalDefault(
			column,
			target,
			value,
		); ok {
			return &Expression{
				sql:     portableCheckStringLiteral(literal),
				kind:    expressionString,
				literal: literal,
			}, nil
		}
		return parseMySQLCatalogCurrentDefault(column, target, value)
	}
	return nil, mysqlCatalogDefaultPolicy(column)
}

// ParseMariaDBCatalogDefault converts one MariaDB 10.11 COLUMN_DEFAULT value
// into DMTX's structured expression contract. Unlike Oracle MySQL, MariaDB
// returns text literals as quoted SQL strings. Those strings are decoded
// structurally, while unquoted values are accepted only for the narrow scalar
// and current-temporal forms appropriate for the column.
//
// MariaDB exposes a column without a default as SQL NULL and a nullable
// implicit or explicit DEFAULT NULL as the text NULL. They have the same DMTX
// planning semantics and therefore both return nil.
func ParseMariaDBCatalogDefault(
	column Column,
	columnDefault *string,
) (*Expression, error) {
	if columnDefault == nil {
		return nil, nil
	}

	target := mysqlCatalogDefaultTargetForColumn(column)
	if target == mysqlCatalogDefaultUnknown {
		return nil, mariaDBCatalogDefaultPolicy(column)
	}

	value := strings.TrimSpace(*columnDefault)
	if value == "" {
		return nil, mariaDBCatalogDefaultPolicy(column)
	}
	if strings.EqualFold(value, "NULL") {
		// MariaDB reports an implicit or explicit nullable DEFAULT NULL as
		// the non-SQL-NULL text NULL. A SQL NULL COLUMN_DEFAULT instead
		// identifies a column with no default. Neither shape requires target
		// DDL in DMTX.
		if !column.Nullable {
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		return nil, nil
	}

	if target == mysqlCatalogDefaultString {
		if len(value) < 2 || value[0] != '\'' {
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		literal, next, err := parsePlainMySQLCatalogCheckString(value, 1)
		if err != nil || next != len(value) ||
			!utf8.ValidString(literal) ||
			strings.ContainsRune(literal, '\x00') {
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		return &Expression{
			sql:     portableCheckStringLiteral(literal),
			kind:    expressionString,
			literal: literal,
		}, nil
	}
	if target == mysqlCatalogDefaultBlob {
		// MariaDB 10.11 preserves BLOB-family defaults as exact X'...' hex
		// literals. BINARY and VARBINARY defaults instead pass through the
		// connection character set in COLUMN_DEFAULT and can replace
		// arbitrary bytes, so those catalog shapes remain unsupported.
		switch mysqlColumnBase(column) {
		case "tinyblob", "blob", "mediumblob", "longblob":
		default:
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		if len(value) < 3 ||
			(value[0] != 'X' && value[0] != 'x') ||
			value[1] != '\'' ||
			value[len(value)-1] != '\'' {
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		hexadecimal := strings.ToLower(value[2 : len(value)-1])
		if len(hexadecimal)%2 != 0 || !isLowerHex(hexadecimal) {
			return nil, mariaDBCatalogDefaultPolicy(column)
		}
		return &Expression{
			sql:     "X'" + hexadecimal + "'",
			kind:    expressionBlob,
			literal: hexadecimal,
		}, nil
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
		if len(value) >= 2 && value[0] == '\'' {
			literal, next, err := parsePlainMySQLCatalogCheckString(
				value,
				1,
			)
			if err == nil && next == len(value) {
				if canonical, ok := canonicalMySQLStaticTemporalDefault(
					column,
					target,
					literal,
				); ok {
					return &Expression{
						sql: portableCheckStringLiteral(
							canonical,
						),
						kind:    expressionString,
						literal: canonical,
					}, nil
				}
			}
		}
		expression, err := parseMySQLCatalogCurrentDefault(
			column,
			target,
			value,
		)
		if err == nil {
			return expression, nil
		}
	}

	return nil, mariaDBCatalogDefaultPolicy(column)
}

// canonicalMySQLStaticTemporalDefault validates the literal payload that
// Oracle MySQL and MariaDB expose for static temporal defaults. It accepts
// only exact, zero-date-free catalog spellings whose fractional width matches
// the declared column precision. TIME literals remain unsupported because the
// MySQL family admits signed durations that do not share one portable scalar
// domain.
func canonicalMySQLStaticTemporalDefault(
	column Column,
	target mysqlCatalogDefaultTarget,
	value string,
) (string, bool) {
	switch target {
	case mysqlCatalogDefaultDate:
		if len(value) != len("2006-01-02") {
			return "", false
		}
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil ||
			parsed.Year() < 1000 ||
			parsed.Year() > 9999 ||
			parsed.Format("2006-01-02") != value {
			return "", false
		}
		return value, true
	case mysqlCatalogDefaultTimestamp:
		base := mysqlColumnBase(column)
		if base != "datetime" && base != "timestamp" {
			return "", false
		}
		precision := mysqlCatalogTemporalPrecision(column)
		layout := "2006-01-02 15:04:05"
		if precision > 0 {
			layout += "." + strings.Repeat("0", precision)
		}
		if len(value) != len(layout) {
			return "", false
		}
		parsed, err := time.ParseInLocation(layout, value, time.UTC)
		if err != nil ||
			parsed.Year() < 1000 ||
			parsed.Year() > 9999 ||
			parsed.Format(layout) != value {
			return "", false
		}
		if base == "timestamp" {
			first := time.Date(
				1970,
				time.January,
				1,
				0,
				0,
				1,
				0,
				time.UTC,
			)
			last := time.Date(
				2038,
				time.January,
				19,
				3,
				14,
				7,
				999_999_000,
				time.UTC,
			)
			if parsed.Before(first) || parsed.After(last) {
				return "", false
			}
		}
		return value, true
	default:
		return "", false
	}
}

func parseMySQLCatalogCurrentDefault(
	column Column,
	target mysqlCatalogDefaultTarget,
	value string,
) (*Expression, error) {
	keyword, precision, ok := canonicalMySQLCurrentKeyword(value)
	if !ok {
		return nil, mysqlCatalogDefaultPolicy(column)
	}
	expectedPrecision := mysqlCatalogTemporalPrecision(column)

	expression := &Expression{sql: keyword}
	switch keyword {
	case "CURRENT_TIME":
		if target != mysqlCatalogDefaultTime ||
			!mysqlCatalogPrecisionMatches(
				expectedPrecision,
				precision,
			) {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentTime
	case "CURRENT_DATE":
		if target != mysqlCatalogDefaultDate || precision != nil {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentDate
	case "CURRENT_TIMESTAMP":
		if target != mysqlCatalogDefaultTimestamp ||
			!mysqlCatalogPrecisionMatches(
				expectedPrecision,
				precision,
			) {
			return nil, mysqlCatalogDefaultPolicy(column)
		}
		expression.kind = expressionCurrentTimestamp
	default:
		return nil, mysqlCatalogDefaultPolicy(column)
	}
	return expression, nil
}

func canonicalMySQLCurrentKeyword(
	value string,
) (string, *int, bool) {
	value = strings.TrimSpace(value)
	for hasSingleOuterParentheses(value) {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	var precision *int
	if open := strings.LastIndexByte(value, '('); open >= 0 {
		if value[len(value)-1] != ')' {
			return "", nil, false
		}
		raw := strings.TrimSpace(value[open+1 : len(value)-1])
		value = strings.TrimSpace(value[:open])
		if raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 0 || parsed > 6 {
				return "", nil, false
			}
			precision = &parsed
		}
	}
	keyword := strings.ToUpper(value)
	switch keyword {
	case "CURTIME":
		return "CURRENT_TIME", precision, true
	case "CURDATE":
		return "CURRENT_DATE", precision, true
	case "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
		return keyword, precision, true
	default:
		return "", nil, false
	}
}

func mysqlCatalogTemporalPrecision(column Column) int {
	if column.DeclaredType == nil ||
		len(column.DeclaredType.Arguments) != 1 {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) {
	case "time", "datetime", "timestamp":
		precision := column.DeclaredType.Arguments[0]
		if precision >= 0 && precision <= 6 {
			return precision
		}
	}
	return 0
}

func mysqlCatalogPrecisionMatches(expected int, actual *int) bool {
	if actual == nil {
		return expected == 0
	}
	return *actual == expected
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
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
		"bytea":
		return mysqlCatalogDefaultBlob
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

func mariaDBCatalogDefaultPolicy(column Column) error {
	return &PolicyError{
		Operation: "parse MariaDB catalog default",
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
