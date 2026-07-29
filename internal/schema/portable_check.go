package schema

import (
	"strings"
	"unicode/utf8"
)

// RenderSQLiteCheckForPostgres parses a deliberately small, portable subset of
// a previously validated SQLite CHECK expression and renders a PostgreSQL
// expression body. Identifiers are resolved against sourceColumns and every
// output token is generated from the parsed structure; source SQL is never
// copied into the rendered expression.
//
// The portable subset contains boolean logic, scalar comparisons, IS NULL, and
// IN lists made exclusively from literals. Ordering comparisons are numeric
// only. Equality and IN operands must belong to the same value family.
func RenderSQLiteCheckForPostgres(
	expression Expression,
	sourceColumns []Column,
) (string, error) {
	if expression.kind != expressionCheck {
		return "", portableCheckPolicy("non-CHECK expression")
	}

	resolver, err := newPortableCheckColumnResolver(sourceColumns)
	if err != nil {
		return "", err
	}
	tokens, err := lexPortableCheck(expression.sql)
	if err != nil {
		return "", err
	}

	parser := portableCheckParser{
		tokens:   tokens,
		resolver: resolver,
	}
	root, err := parser.parseExpression()
	if err != nil {
		return "", err
	}
	if parser.current().kind != portableCheckTokenEOF {
		return "", portableCheckPolicy("unexpected expression token")
	}
	if root.family != portableCheckBoolean {
		return "", portableCheckPolicy("CHECK root is not boolean")
	}
	return renderPortableCheck(root, portableCheckPrecedenceLowest), nil
}

type portableCheckFamily uint8

const (
	portableCheckInvalid portableCheckFamily = iota
	portableCheckBoolean
	portableCheckNumeric
	portableCheckText
	portableCheckOpaque
	portableCheckNull
)

type portableCheckNodeKind uint8

const (
	portableCheckNodeInvalid portableCheckNodeKind = iota
	portableCheckNodeColumn
	portableCheckNodeNumber
	portableCheckNodeString
	portableCheckNodeBoolean
	portableCheckNodeNull
	portableCheckNodeComparison
	portableCheckNodeIsNull
	portableCheckNodeIn
	portableCheckNodeNot
	portableCheckNodeAnd
	portableCheckNodeOr
)

type portableCheckNode struct {
	kind     portableCheckNodeKind
	family   portableCheckFamily
	text     string
	left     *portableCheckNode
	right    *portableCheckNode
	children []*portableCheckNode
	negated  bool
}

func (node *portableCheckNode) isScalar() bool {
	switch node.kind {
	case portableCheckNodeColumn,
		portableCheckNodeNumber,
		portableCheckNodeString,
		portableCheckNodeBoolean,
		portableCheckNodeNull:
		return true
	default:
		return false
	}
}

type portableCheckColumn struct {
	name   string
	family portableCheckFamily
}

type portableCheckColumnResolver struct {
	columns []portableCheckColumn
}

func newPortableCheckColumnResolver(
	columns []Column,
) (portableCheckColumnResolver, error) {
	resolved := make([]portableCheckColumn, len(columns))
	for index, column := range columns {
		if column.Name == "" ||
			strings.ContainsRune(column.Name, '\x00') ||
			!utf8.ValidString(column.Name) {
			return portableCheckColumnResolver{},
				portableCheckPolicy("invalid source column name")
		}
		for earlier := 0; earlier < index; earlier++ {
			if strings.EqualFold(column.Name, columns[earlier].Name) {
				return portableCheckColumnResolver{},
					portableCheckPolicy("ambiguous source columns")
			}
		}
		resolved[index] = portableCheckColumn{
			name:   column.Name,
			family: portableCheckColumnFamily(column),
		}
	}
	return portableCheckColumnResolver{columns: resolved}, nil
}

func (resolver portableCheckColumnResolver) resolve(
	name string,
) (portableCheckColumn, error) {
	for _, column := range resolver.columns {
		if strings.EqualFold(name, column.name) {
			return column, nil
		}
	}
	return portableCheckColumn{}, portableCheckPolicy("unknown source column")
}

func portableCheckColumnFamily(column Column) portableCheckFamily {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	switch base {
	case "bool", "boolean":
		return portableCheckBoolean
	case "int", "integer", "tinyint", "smallint", "mediumint", "bigint",
		"unsigned big int", "int2", "int8", "real", "double",
		"double precision", "float", "numeric", "decimal":
		return portableCheckNumeric
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		return portableCheckText
	default:
		// Every SQL value supports IS NULL. Other operations on types whose
		// SQLite and PostgreSQL comparison semantics have not been proven
		// equivalent remain fail-closed.
		return portableCheckOpaque
	}
}

type portableCheckTokenKind uint8

const (
	portableCheckTokenInvalid portableCheckTokenKind = iota
	portableCheckTokenEOF
	portableCheckTokenIdentifier
	portableCheckTokenNumber
	portableCheckTokenString
	portableCheckTokenNull
	portableCheckTokenTrue
	portableCheckTokenFalse
	portableCheckTokenLeftParen
	portableCheckTokenRightParen
	portableCheckTokenComma
	portableCheckTokenEqual
	portableCheckTokenNotEqual
	portableCheckTokenLess
	portableCheckTokenLessEqual
	portableCheckTokenGreater
	portableCheckTokenGreaterEqual
	portableCheckTokenIs
	portableCheckTokenIn
	portableCheckTokenNot
	portableCheckTokenAnd
	portableCheckTokenOr
)

type portableCheckToken struct {
	kind portableCheckTokenKind
	text string
}

type portableCheckLexer struct {
	input string
	index int
}

func lexPortableCheck(value string) ([]portableCheckToken, error) {
	if value == "" || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return nil, portableCheckPolicy("invalid CHECK text")
	}
	lexer := portableCheckLexer{input: value}
	tokens := make([]portableCheckToken, 0, 16)
	for {
		token, err := lexer.next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if token.kind == portableCheckTokenEOF {
			return tokens, nil
		}
	}
}

func (lexer *portableCheckLexer) next() (portableCheckToken, error) {
	for lexer.index < len(lexer.input) &&
		portableCheckWhitespace(lexer.input[lexer.index]) {
		lexer.index++
	}
	if lexer.index == len(lexer.input) {
		return portableCheckToken{kind: portableCheckTokenEOF}, nil
	}

	current := lexer.input[lexer.index]
	switch current {
	case '(':
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenLeftParen}, nil
	case ')':
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenRightParen}, nil
	case ',':
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenComma}, nil
	case '\'':
		return lexer.quotedToken(
			'\'',
			portableCheckTokenString,
			true,
		)
	case '"', '`':
		return lexer.quotedToken(
			current,
			portableCheckTokenIdentifier,
			true,
		)
	case '[':
		return lexer.quotedToken(
			']',
			portableCheckTokenIdentifier,
			false,
		)
	case '=':
		if lexer.hasNext('=') {
			return portableCheckToken{}, portableCheckPolicy(
				"unsupported comparison operator",
			)
		}
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenEqual}, nil
	case '!':
		if !lexer.hasNext('=') {
			return portableCheckToken{}, portableCheckPolicy(
				"unsupported CHECK operator",
			)
		}
		lexer.index += 2
		return portableCheckToken{kind: portableCheckTokenNotEqual}, nil
	case '<':
		if lexer.hasNext('=') {
			lexer.index += 2
			return portableCheckToken{kind: portableCheckTokenLessEqual}, nil
		}
		if lexer.hasNext('>') {
			lexer.index += 2
			return portableCheckToken{kind: portableCheckTokenNotEqual}, nil
		}
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenLess}, nil
	case '>':
		if lexer.hasNext('=') {
			lexer.index += 2
			return portableCheckToken{kind: portableCheckTokenGreaterEqual}, nil
		}
		lexer.index++
		return portableCheckToken{kind: portableCheckTokenGreater}, nil
	}

	if portableCheckIdentifierStart(current) {
		return lexer.identifier(), nil
	}
	if portableCheckDigit(current) ||
		((current == '+' || current == '-') &&
			lexer.nextStartsNumber()) ||
		(current == '.' && lexer.nextIsDigit()) {
		return lexer.number()
	}
	return portableCheckToken{},
		portableCheckPolicy("unsupported CHECK character")
}

func (lexer *portableCheckLexer) hasNext(value byte) bool {
	return lexer.index+1 < len(lexer.input) &&
		lexer.input[lexer.index+1] == value
}

func (lexer *portableCheckLexer) nextIsDigit() bool {
	return lexer.index+1 < len(lexer.input) &&
		portableCheckDigit(lexer.input[lexer.index+1])
}

func (lexer *portableCheckLexer) nextStartsNumber() bool {
	if lexer.index+1 >= len(lexer.input) {
		return false
	}
	next := lexer.input[lexer.index+1]
	if portableCheckDigit(next) {
		return true
	}
	return next == '.' &&
		lexer.index+2 < len(lexer.input) &&
		portableCheckDigit(lexer.input[lexer.index+2])
}

func (lexer *portableCheckLexer) quotedToken(
	closing byte,
	kind portableCheckTokenKind,
	doubledClosingEscapes bool,
) (portableCheckToken, error) {
	lexer.index++
	var value strings.Builder
	for lexer.index < len(lexer.input) {
		current := lexer.input[lexer.index]
		if current != closing {
			value.WriteByte(current)
			lexer.index++
			continue
		}
		if doubledClosingEscapes &&
			lexer.index+1 < len(lexer.input) &&
			lexer.input[lexer.index+1] == closing {
			value.WriteByte(closing)
			lexer.index += 2
			continue
		}
		lexer.index++
		return portableCheckToken{kind: kind, text: value.String()}, nil
	}
	return portableCheckToken{}, portableCheckPolicy("unterminated quote")
}

func (lexer *portableCheckLexer) identifier() portableCheckToken {
	start := lexer.index
	lexer.index++
	for lexer.index < len(lexer.input) &&
		portableCheckIdentifierPart(lexer.input[lexer.index]) {
		lexer.index++
	}
	value := lexer.input[start:lexer.index]
	switch strings.ToUpper(value) {
	case "NULL":
		return portableCheckToken{kind: portableCheckTokenNull}
	case "TRUE":
		return portableCheckToken{kind: portableCheckTokenTrue}
	case "FALSE":
		return portableCheckToken{kind: portableCheckTokenFalse}
	case "IS":
		return portableCheckToken{kind: portableCheckTokenIs}
	case "IN":
		return portableCheckToken{kind: portableCheckTokenIn}
	case "NOT":
		return portableCheckToken{kind: portableCheckTokenNot}
	case "AND":
		return portableCheckToken{kind: portableCheckTokenAnd}
	case "OR":
		return portableCheckToken{kind: portableCheckTokenOr}
	default:
		return portableCheckToken{
			kind: portableCheckTokenIdentifier,
			text: value,
		}
	}
}

func (lexer *portableCheckLexer) number() (portableCheckToken, error) {
	start := lexer.index
	if lexer.input[lexer.index] == '+' || lexer.input[lexer.index] == '-' {
		lexer.index++
	}
	wholeStart := lexer.index
	for lexer.index < len(lexer.input) &&
		portableCheckDigit(lexer.input[lexer.index]) {
		lexer.index++
	}
	whole := lexer.input[wholeStart:lexer.index]
	fraction := ""
	hasDecimal := lexer.index < len(lexer.input) &&
		lexer.input[lexer.index] == '.'
	if hasDecimal {
		lexer.index++
		fractionStart := lexer.index
		for lexer.index < len(lexer.input) &&
			portableCheckDigit(lexer.input[lexer.index]) {
			lexer.index++
		}
		fraction = lexer.input[fractionStart:lexer.index]
	}
	if whole == "" && fraction == "" {
		return portableCheckToken{}, portableCheckPolicy(
			"invalid numeric literal",
		)
	}
	raw := lexer.input[start:lexer.index]
	if len(raw) > 1002 {
		return portableCheckToken{}, portableCheckPolicy(
			"numeric literal is too long",
		)
	}
	return portableCheckToken{
		kind: portableCheckTokenNumber,
		text: canonicalPortableCheckNumber(raw, whole, fraction, hasDecimal),
	}, nil
}

func canonicalPortableCheckNumber(
	raw string,
	whole string,
	fraction string,
	hasDecimal bool,
) string {
	sign := ""
	if raw[0] == '-' {
		sign = "-"
	}
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	if !hasDecimal {
		return sign + whole
	}
	if fraction == "" {
		fraction = "0"
	}
	return sign + whole + "." + fraction
}

func portableCheckWhitespace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func portableCheckIdentifierStart(value byte) bool {
	return value == '_' ||
		value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z'
}

func portableCheckIdentifierPart(value byte) bool {
	return portableCheckIdentifierStart(value) ||
		portableCheckDigit(value)
}

func portableCheckDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

type portableCheckParser struct {
	tokens   []portableCheckToken
	index    int
	resolver portableCheckColumnResolver
}

func (parser *portableCheckParser) parseExpression() (
	*portableCheckNode,
	error,
) {
	return parser.parseOr()
}

func (parser *portableCheckParser) parseOr() (*portableCheckNode, error) {
	left, err := parser.parseAnd()
	if err != nil {
		return nil, err
	}
	for parser.match(portableCheckTokenOr) {
		right, err := parser.parseAnd()
		if err != nil {
			return nil, err
		}
		if left.family != portableCheckBoolean ||
			right.family != portableCheckBoolean {
			return nil, portableCheckPolicy(
				"OR operands must be boolean",
			)
		}
		left = &portableCheckNode{
			kind:   portableCheckNodeOr,
			family: portableCheckBoolean,
			left:   left,
			right:  right,
		}
	}
	return left, nil
}

func (parser *portableCheckParser) parseAnd() (*portableCheckNode, error) {
	left, err := parser.parseNot()
	if err != nil {
		return nil, err
	}
	for parser.match(portableCheckTokenAnd) {
		right, err := parser.parseNot()
		if err != nil {
			return nil, err
		}
		if left.family != portableCheckBoolean ||
			right.family != portableCheckBoolean {
			return nil, portableCheckPolicy(
				"AND operands must be boolean",
			)
		}
		left = &portableCheckNode{
			kind:   portableCheckNodeAnd,
			family: portableCheckBoolean,
			left:   left,
			right:  right,
		}
	}
	return left, nil
}

func (parser *portableCheckParser) parseNot() (*portableCheckNode, error) {
	if !parser.match(portableCheckTokenNot) {
		return parser.parsePredicate()
	}
	child, err := parser.parseNot()
	if err != nil {
		return nil, err
	}
	if child.family != portableCheckBoolean {
		return nil, portableCheckPolicy("NOT operand must be boolean")
	}
	return &portableCheckNode{
		kind:   portableCheckNodeNot,
		family: portableCheckBoolean,
		left:   child,
	}, nil
}

func (parser *portableCheckParser) parsePredicate() (
	*portableCheckNode,
	error,
) {
	left, err := parser.parsePrimary()
	if err != nil {
		return nil, err
	}
	switch parser.current().kind {
	case portableCheckTokenEqual,
		portableCheckTokenNotEqual,
		portableCheckTokenLess,
		portableCheckTokenLessEqual,
		portableCheckTokenGreater,
		portableCheckTokenGreaterEqual:
		operator := parser.advance()
		right, err := parser.parsePrimary()
		if err != nil {
			return nil, err
		}
		if err := validatePortableCheckComparison(
			operator.kind,
			left,
			right,
		); err != nil {
			return nil, err
		}
		return &portableCheckNode{
			kind:   portableCheckNodeComparison,
			family: portableCheckBoolean,
			text:   portableCheckOperator(operator.kind),
			left:   left,
			right:  right,
		}, nil
	case portableCheckTokenIs:
		parser.advance()
		negated := parser.match(portableCheckTokenNot)
		if !parser.match(portableCheckTokenNull) {
			return nil, portableCheckPolicy(
				"IS supports only NULL",
			)
		}
		if left.kind != portableCheckNodeColumn {
			return nil, portableCheckPolicy(
				"IS NULL requires a column",
			)
		}
		return &portableCheckNode{
			kind:    portableCheckNodeIsNull,
			family:  portableCheckBoolean,
			left:    left,
			negated: negated,
		}, nil
	case portableCheckTokenIn:
		parser.advance()
		return parser.parseIn(left)
	default:
		return left, nil
	}
}

func (parser *portableCheckParser) parsePrimary() (
	*portableCheckNode,
	error,
) {
	token := parser.current()
	switch token.kind {
	case portableCheckTokenIdentifier:
		parser.advance()
		column, err := parser.resolver.resolve(token.text)
		if err != nil {
			return nil, err
		}
		return &portableCheckNode{
			kind:   portableCheckNodeColumn,
			family: column.family,
			text:   column.name,
		}, nil
	case portableCheckTokenNumber:
		parser.advance()
		return &portableCheckNode{
			kind:   portableCheckNodeNumber,
			family: portableCheckNumeric,
			text:   token.text,
		}, nil
	case portableCheckTokenString:
		parser.advance()
		return &portableCheckNode{
			kind:   portableCheckNodeString,
			family: portableCheckText,
			text:   token.text,
		}, nil
	case portableCheckTokenTrue:
		parser.advance()
		return &portableCheckNode{
			kind:   portableCheckNodeBoolean,
			family: portableCheckBoolean,
			text:   "TRUE",
		}, nil
	case portableCheckTokenFalse:
		parser.advance()
		return &portableCheckNode{
			kind:   portableCheckNodeBoolean,
			family: portableCheckBoolean,
			text:   "FALSE",
		}, nil
	case portableCheckTokenNull:
		parser.advance()
		return &portableCheckNode{
			kind:   portableCheckNodeNull,
			family: portableCheckNull,
			text:   "NULL",
		}, nil
	case portableCheckTokenLeftParen:
		parser.advance()
		node, err := parser.parseExpression()
		if err != nil {
			return nil, err
		}
		if !parser.match(portableCheckTokenRightParen) {
			return nil, portableCheckPolicy(
				"unbalanced CHECK parentheses",
			)
		}
		return node, nil
	default:
		return nil, portableCheckPolicy("expected CHECK operand")
	}
}

func (parser *portableCheckParser) parseIn(
	left *portableCheckNode,
) (*portableCheckNode, error) {
	if left.kind != portableCheckNodeColumn {
		return nil, portableCheckPolicy("IN requires a column")
	}
	switch left.family {
	case portableCheckBoolean, portableCheckNumeric, portableCheckText:
	default:
		return nil, portableCheckPolicy(
			"IN is unsupported for this column type",
		)
	}
	if !parser.match(portableCheckTokenLeftParen) {
		return nil, portableCheckPolicy("IN requires a literal list")
	}
	if parser.current().kind == portableCheckTokenRightParen {
		return nil, portableCheckPolicy("IN list cannot be empty")
	}

	values := make([]*portableCheckNode, 0, 2)
	for {
		value, err := parser.parseInLiteral()
		if err != nil {
			return nil, err
		}
		if value.family != portableCheckNull &&
			value.family != left.family {
			return nil, portableCheckPolicy(
				"IN contains a cross-family literal",
			)
		}
		values = append(values, value)
		if !parser.match(portableCheckTokenComma) {
			break
		}
	}
	if !parser.match(portableCheckTokenRightParen) {
		return nil, portableCheckPolicy("invalid IN literal list")
	}
	return &portableCheckNode{
		kind:     portableCheckNodeIn,
		family:   portableCheckBoolean,
		left:     left,
		children: values,
	}, nil
}

func (parser *portableCheckParser) parseInLiteral() (
	*portableCheckNode,
	error,
) {
	switch parser.current().kind {
	case portableCheckTokenNumber,
		portableCheckTokenString,
		portableCheckTokenTrue,
		portableCheckTokenFalse,
		portableCheckTokenNull:
		return parser.parsePrimary()
	default:
		return nil, portableCheckPolicy(
			"IN list members must be literals",
		)
	}
}

func (parser *portableCheckParser) current() portableCheckToken {
	if parser.index >= len(parser.tokens) {
		return portableCheckToken{kind: portableCheckTokenEOF}
	}
	return parser.tokens[parser.index]
}

func (parser *portableCheckParser) advance() portableCheckToken {
	token := parser.current()
	if parser.index < len(parser.tokens) {
		parser.index++
	}
	return token
}

func (parser *portableCheckParser) match(kind portableCheckTokenKind) bool {
	if parser.current().kind != kind {
		return false
	}
	parser.advance()
	return true
}

func validatePortableCheckComparison(
	operator portableCheckTokenKind,
	left *portableCheckNode,
	right *portableCheckNode,
) error {
	if !left.isScalar() || !right.isScalar() {
		return portableCheckPolicy(
			"comparison operands must be columns or literals",
		)
	}
	ordering := operator == portableCheckTokenLess ||
		operator == portableCheckTokenLessEqual ||
		operator == portableCheckTokenGreater ||
		operator == portableCheckTokenGreaterEqual
	if ordering {
		if left.family != portableCheckNumeric ||
			right.family != portableCheckNumeric {
			return portableCheckPolicy(
				"ordering comparison requires numeric operands",
			)
		}
		return nil
	}

	if left.family == portableCheckNull ||
		right.family == portableCheckNull {
		if left.family == portableCheckOpaque ||
			right.family == portableCheckOpaque {
			return portableCheckPolicy(
				"opaque NULL equality requires IS NULL",
			)
		}
		return nil
	}
	if left.family == portableCheckOpaque ||
		right.family == portableCheckOpaque ||
		left.family != right.family {
		return portableCheckPolicy(
			"comparison contains cross-family operands",
		)
	}
	return nil
}

func portableCheckOperator(kind portableCheckTokenKind) string {
	switch kind {
	case portableCheckTokenEqual:
		return "="
	case portableCheckTokenNotEqual:
		return "<>"
	case portableCheckTokenLess:
		return "<"
	case portableCheckTokenLessEqual:
		return "<="
	case portableCheckTokenGreater:
		return ">"
	case portableCheckTokenGreaterEqual:
		return ">="
	default:
		return ""
	}
}

const (
	portableCheckPrecedenceLowest = iota
	portableCheckPrecedenceOr
	portableCheckPrecedenceAnd
	portableCheckPrecedenceNot
	portableCheckPrecedencePredicate
	portableCheckPrecedenceScalar
)

func renderPortableCheck(
	node *portableCheckNode,
	parentPrecedence int,
) string {
	precedence := portableCheckNodePrecedence(node)
	var rendered string
	switch node.kind {
	case portableCheckNodeColumn:
		rendered = quote(Postgres, node.text)
	case portableCheckNodeNumber, portableCheckNodeBoolean,
		portableCheckNodeNull:
		rendered = node.text
	case portableCheckNodeString:
		rendered = postgresStringLiteral(node.text)
	case portableCheckNodeComparison:
		collateLeft, collateRight := portableCheckTextCollationOperands(
			node.left,
			node.right,
		)
		rendered = renderPortableCheckOperand(
			node.left,
			portableCheckPrecedencePredicate,
			collateLeft,
		) + " " + node.text + " " + renderPortableCheckOperand(
			node.right,
			portableCheckPrecedencePredicate,
			collateRight,
		)
	case portableCheckNodeIsNull:
		rendered = renderPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " IS "
		if node.negated {
			rendered += "NOT "
		}
		rendered += "NULL"
	case portableCheckNodeIn:
		values := make([]string, len(node.children))
		for index, child := range node.children {
			values[index] = renderPortableCheck(
				child,
				portableCheckPrecedenceLowest,
			)
		}
		rendered = renderPortableCheckOperand(
			node.left,
			portableCheckPrecedencePredicate,
			node.left.family == portableCheckText,
		) + " IN (" + strings.Join(values, ", ") + ")"
	case portableCheckNodeNot:
		child := renderPortableCheck(
			node.left,
			portableCheckPrecedenceLowest,
		)
		if !node.left.isScalar() {
			child = "(" + child + ")"
		}
		rendered = "NOT " + child
	case portableCheckNodeAnd:
		rendered = renderPortableCheck(
			node.left,
			portableCheckPrecedenceAnd,
		) + " AND " + renderPortableCheck(
			node.right,
			portableCheckPrecedenceAnd,
		)
	case portableCheckNodeOr:
		rendered = renderPortableCheck(
			node.left,
			portableCheckPrecedenceOr,
		) + " OR " + renderPortableCheck(
			node.right,
			portableCheckPrecedenceOr,
		)
	}
	if precedence < parentPrecedence {
		return "(" + rendered + ")"
	}
	return rendered
}

func renderPortableCheckOperand(
	node *portableCheckNode,
	parentPrecedence int,
	binaryText bool,
) string {
	rendered := renderPortableCheck(node, parentPrecedence)
	if binaryText {
		rendered += " COLLATE " +
			qualified(Postgres, "pg_catalog", "C")
	}
	return rendered
}

func portableCheckTextCollationOperands(
	left *portableCheckNode,
	right *portableCheckNode,
) (bool, bool) {
	if left.kind == portableCheckNodeColumn &&
		left.family == portableCheckText {
		return true, false
	}
	if right.kind == portableCheckNodeColumn &&
		right.family == portableCheckText {
		return false, true
	}
	if left.family == portableCheckText {
		return true, false
	}
	return false, right.family == portableCheckText
}

func portableCheckNodePrecedence(node *portableCheckNode) int {
	switch node.kind {
	case portableCheckNodeOr:
		return portableCheckPrecedenceOr
	case portableCheckNodeAnd:
		return portableCheckPrecedenceAnd
	case portableCheckNodeNot:
		return portableCheckPrecedenceNot
	case portableCheckNodeComparison,
		portableCheckNodeIsNull,
		portableCheckNodeIn:
		return portableCheckPrecedencePredicate
	default:
		return portableCheckPrecedenceScalar
	}
}

func portableCheckPolicy(reason string) error {
	return &PolicyError{
		Operation: "render portable SQLite CHECK",
		Type:      reason,
		Target:    string(Postgres),
	}
}
