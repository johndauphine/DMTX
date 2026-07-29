package schema

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

const postgresCheckSignatureVersion = "dmtx:postgres-check:v1:"

// PostgresCheckSignature is an opaque canonical representation of the
// portable CHECK subset that DMTX can prove equivalent across SQLite and
// PostgreSQL. Signatures are comparable but their encoding is not SQL.
type PostgresCheckSignature string

// PlannedPostgresCheckSignature returns the logical signature of a parsed
// source CHECK after applying the same constrained grammar used by DMTX's
// PostgreSQL renderer.
func PlannedPostgresCheckSignature(
	expression Expression,
	sourceColumns []Column,
) (PostgresCheckSignature, error) {
	root, err := parsePlannedPostgresCheck(expression, sourceColumns)
	if err != nil {
		return "", err
	}
	return makePostgresCheckSignature(root), nil
}

// ParsePostgresCheckSignature parses pg_get_expr output for only DMTX's
// portable CHECK subset. PostgreSQL-inserted scalar casts and its IN-to-ANY
// rewrite are normalized back to the same logical AST as the planned CHECK.
// Text comparisons must retain an explicit built-in C collation.
func ParsePostgresCheckSignature(
	catalogExpression string,
	targetColumns []Column,
) (PostgresCheckSignature, error) {
	root, err := parsePostgresCatalogCheckRoot(
		catalogExpression,
		targetColumns,
	)
	if err != nil {
		return "", err
	}
	return makePostgresCheckSignature(root), nil
}

func parsePostgresCatalogCheckRoot(
	catalogExpression string,
	targetColumns []Column,
) (*portableCheckNode, error) {
	resolver, err := newPortableCheckColumnResolver(targetColumns)
	if err != nil {
		return nil, err
	}
	textRelabelColumns, err := postgresCheckCatalogTextRelabelColumns(
		targetColumns,
	)
	if err != nil {
		return nil, err
	}
	tokens, err := lexPostgresCheckCatalog(catalogExpression)
	if err != nil {
		return nil, err
	}
	parser := postgresCheckCatalogParser{
		tokens:             tokens,
		resolver:           resolver,
		collated:           make(map[*portableCheckNode]bool),
		nullCastFamily:     make(map[*portableCheckNode]portableCheckFamily),
		textRelabelColumns: textRelabelColumns,
		textRelabelled:     make(map[*portableCheckNode]bool),
	}
	root, err := parser.parseExpression()
	if err != nil {
		return nil, err
	}
	if parser.current().kind != postgresCheckCatalogTokenEOF {
		return nil, postgresCheckSignaturePolicy(
			"unexpected catalog CHECK token",
		)
	}
	if root.family != portableCheckBoolean {
		return nil, postgresCheckSignaturePolicy(
			"catalog CHECK root is not boolean",
		)
	}
	return root, nil
}

func postgresCheckCatalogTextRelabelColumns(
	columns []Column,
) (map[string]bool, error) {
	result := make(map[string]bool)
	for _, column := range columns {
		if column.DeclaredType == nil {
			continue
		}
		base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
		if base != "varchar" && base != "character varying" {
			continue
		}
		if _, err := renderPostgresDeclaredType(*column.DeclaredType); err != nil {
			return nil, err
		}
		result[column.Name] = true
	}
	return result, nil
}

func parsePlannedPostgresCheck(
	expression Expression,
	columns []Column,
) (*portableCheckNode, error) {
	if expression.kind != expressionCheck {
		return nil, portableCheckPolicy("non-CHECK expression")
	}
	resolver, err := newPortableCheckColumnResolver(columns)
	if err != nil {
		return nil, err
	}
	tokens, err := lexPortableCheck(expression.sql)
	if err != nil {
		return nil, err
	}
	parser := portableCheckParser{tokens: tokens, resolver: resolver}
	root, err := parser.parseExpression()
	if err != nil {
		return nil, err
	}
	if parser.current().kind != portableCheckTokenEOF {
		return nil, portableCheckPolicy("unexpected expression token")
	}
	if root.family != portableCheckBoolean {
		return nil, portableCheckPolicy("CHECK root is not boolean")
	}
	return root, nil
}

func makePostgresCheckSignature(
	root *portableCheckNode,
) PostgresCheckSignature {
	return PostgresCheckSignature(
		postgresCheckSignatureVersion + renderPostgresCheckSignature(root),
	)
}

func renderPostgresCheckSignature(node *portableCheckNode) string {
	switch node.kind {
	case portableCheckNodeColumn:
		return "column(" + portableCheckFamilyName(node.family) + "," +
			strconv.Quote(node.text) + ")"
	case portableCheckNodeNumber:
		return "number(" + strconv.Quote(
			postgresCheckSignatureNumber(node.text),
		) + ")"
	case portableCheckNodeString:
		return "string(" + strconv.Quote(node.text) + ")"
	case portableCheckNodeBoolean:
		return "boolean(" + node.text + ")"
	case portableCheckNodeNull:
		return "null"
	case portableCheckNodeComparison:
		return "compare(" + strconv.Quote(node.text) + "," +
			renderPostgresCheckSignature(node.left) + "," +
			renderPostgresCheckSignature(node.right) + ")"
	case portableCheckNodeIsNull:
		kind := "is-null"
		if node.negated {
			kind = "is-not-null"
		}
		return kind + "(" + renderPostgresCheckSignature(node.left) + ")"
	case portableCheckNodeIn:
		if len(node.children) == 1 {
			return "compare(" + strconv.Quote("=") + "," +
				renderPostgresCheckSignature(node.left) + "," +
				renderPostgresCheckSignature(node.children[0]) + ")"
		}
		children := make([]string, len(node.children))
		for index, child := range node.children {
			children[index] = renderPostgresCheckSignature(child)
		}
		return "in(" + renderPostgresCheckSignature(node.left) + ",[" +
			strings.Join(children, ",") + "])"
	case portableCheckNodeNot:
		return "not(" + renderPostgresCheckSignature(node.left) + ")"
	case portableCheckNodeAnd:
		return "and(" + renderPostgresCheckSignature(node.left) + "," +
			renderPostgresCheckSignature(node.right) + ")"
	case portableCheckNodeOr:
		return "or(" + renderPostgresCheckSignature(node.left) + "," +
			renderPostgresCheckSignature(node.right) + ")"
	default:
		return "invalid"
	}
}

func postgresCheckSignatureNumber(value string) string {
	if strings.HasPrefix(value, "-") {
		unsigned := strings.TrimPrefix(value, "-")
		if strings.Trim(unsigned, "0.") == "" {
			return unsigned
		}
	}
	return value
}

func portableCheckFamilyName(family portableCheckFamily) string {
	switch family {
	case portableCheckBoolean:
		return "boolean"
	case portableCheckNumeric:
		return "numeric"
	case portableCheckText:
		return "text"
	case portableCheckOpaque:
		return "opaque"
	case portableCheckNull:
		return "null"
	default:
		return "invalid"
	}
}

type postgresCheckCatalogTokenKind uint8

const (
	postgresCheckCatalogTokenInvalid postgresCheckCatalogTokenKind = iota
	postgresCheckCatalogTokenEOF
	postgresCheckCatalogTokenIdentifier
	postgresCheckCatalogTokenString
	postgresCheckCatalogTokenNumber
	postgresCheckCatalogTokenNull
	postgresCheckCatalogTokenTrue
	postgresCheckCatalogTokenFalse
	postgresCheckCatalogTokenLeftParen
	postgresCheckCatalogTokenRightParen
	postgresCheckCatalogTokenLeftBracket
	postgresCheckCatalogTokenRightBracket
	postgresCheckCatalogTokenComma
	postgresCheckCatalogTokenDot
	postgresCheckCatalogTokenCast
	postgresCheckCatalogTokenEqual
	postgresCheckCatalogTokenNotEqual
	postgresCheckCatalogTokenLess
	postgresCheckCatalogTokenLessEqual
	postgresCheckCatalogTokenGreater
	postgresCheckCatalogTokenGreaterEqual
	postgresCheckCatalogTokenIs
	postgresCheckCatalogTokenNot
	postgresCheckCatalogTokenAnd
	postgresCheckCatalogTokenOr
	postgresCheckCatalogTokenAny
	postgresCheckCatalogTokenArray
	postgresCheckCatalogTokenCollate
)

type postgresCheckCatalogToken struct {
	kind   postgresCheckCatalogTokenKind
	text   string
	quoted bool
}

type postgresCheckCatalogLexer struct {
	input string
	index int
}

func lexPostgresCheckCatalog(
	value string,
) ([]postgresCheckCatalogToken, error) {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return nil, postgresCheckSignaturePolicy(
			"invalid catalog CHECK text",
		)
	}
	lexer := postgresCheckCatalogLexer{input: value}
	tokens := make([]postgresCheckCatalogToken, 0, 24)
	for {
		token, err := lexer.next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if token.kind == postgresCheckCatalogTokenEOF {
			return tokens, nil
		}
	}
}

func (lexer *postgresCheckCatalogLexer) next() (
	postgresCheckCatalogToken,
	error,
) {
	for lexer.index < len(lexer.input) &&
		portableCheckWhitespace(lexer.input[lexer.index]) {
		lexer.index++
	}
	if lexer.index == len(lexer.input) {
		return postgresCheckCatalogToken{
			kind: postgresCheckCatalogTokenEOF,
		}, nil
	}
	current := lexer.input[lexer.index]
	switch current {
	case '(':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenLeftParen,
		), nil
	case ')':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenRightParen,
		), nil
	case '[':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenLeftBracket,
		), nil
	case ']':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenRightBracket,
		), nil
	case ',':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenComma,
		), nil
	case '.':
		if lexer.index+1 < len(lexer.input) &&
			portableCheckDigit(lexer.input[lexer.index+1]) {
			return lexer.number()
		}
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenDot,
		), nil
	case ':':
		if lexer.index+1 >= len(lexer.input) ||
			lexer.input[lexer.index+1] != ':' {
			return postgresCheckCatalogToken{},
				postgresCheckSignaturePolicy("invalid catalog cast")
		}
		lexer.index += 2
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenCast,
		), nil
	case '=':
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenEqual,
		), nil
	case '<':
		if lexer.peek('=') {
			lexer.index += 2
			return postgresCatalogPunctuation(
				postgresCheckCatalogTokenLessEqual,
			), nil
		}
		if lexer.peek('>') {
			lexer.index += 2
			return postgresCatalogPunctuation(
				postgresCheckCatalogTokenNotEqual,
			), nil
		}
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenLess,
		), nil
	case '>':
		if lexer.peek('=') {
			lexer.index += 2
			return postgresCatalogPunctuation(
				postgresCheckCatalogTokenGreaterEqual,
			), nil
		}
		lexer.index++
		return postgresCatalogPunctuation(
			postgresCheckCatalogTokenGreater,
		), nil
	case '\'':
		return lexer.stringLiteral(false)
	case '"':
		return lexer.quotedIdentifier()
	case ';':
		return postgresCheckCatalogToken{},
			postgresCheckSignaturePolicy(
				"catalog CHECK contains a statement boundary",
			)
	}
	if (current == 'E' || current == 'e') &&
		lexer.index+1 < len(lexer.input) &&
		lexer.input[lexer.index+1] == '\'' {
		lexer.index++
		return lexer.stringLiteral(true)
	}
	if portableCheckDigit(current) ||
		((current == '+' || current == '-') && lexer.numberFollows()) {
		return lexer.number()
	}
	if portableCheckIdentifierStart(current) {
		return lexer.identifier(), nil
	}
	return postgresCheckCatalogToken{}, postgresCheckSignaturePolicy(
		"unsupported catalog CHECK character",
	)
}

func postgresCatalogPunctuation(
	kind postgresCheckCatalogTokenKind,
) postgresCheckCatalogToken {
	return postgresCheckCatalogToken{kind: kind}
}

func (lexer *postgresCheckCatalogLexer) peek(value byte) bool {
	return lexer.index+1 < len(lexer.input) &&
		lexer.input[lexer.index+1] == value
}

func (lexer *postgresCheckCatalogLexer) numberFollows() bool {
	if lexer.index+1 >= len(lexer.input) {
		return false
	}
	next := lexer.input[lexer.index+1]
	return portableCheckDigit(next) ||
		(next == '.' && lexer.index+2 < len(lexer.input) &&
			portableCheckDigit(lexer.input[lexer.index+2]))
}

func (lexer *postgresCheckCatalogLexer) number() (
	postgresCheckCatalogToken,
	error,
) {
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
		return postgresCheckCatalogToken{},
			postgresCheckSignaturePolicy("invalid catalog numeric literal")
	}
	raw := lexer.input[start:lexer.index]
	if len(raw) > 1002 {
		return postgresCheckCatalogToken{},
			postgresCheckSignaturePolicy("catalog numeric literal is too long")
	}
	return postgresCheckCatalogToken{
		kind: postgresCheckCatalogTokenNumber,
		text: canonicalPortableCheckNumber(
			raw,
			whole,
			fraction,
			hasDecimal,
		),
	}, nil
}

func (lexer *postgresCheckCatalogLexer) identifier() postgresCheckCatalogToken {
	start := lexer.index
	lexer.index++
	for lexer.index < len(lexer.input) &&
		portableCheckIdentifierPart(lexer.input[lexer.index]) {
		lexer.index++
	}
	value := lexer.input[start:lexer.index]
	upper := strings.ToUpper(value)
	keywords := map[string]postgresCheckCatalogTokenKind{
		"NULL":    postgresCheckCatalogTokenNull,
		"TRUE":    postgresCheckCatalogTokenTrue,
		"FALSE":   postgresCheckCatalogTokenFalse,
		"IS":      postgresCheckCatalogTokenIs,
		"NOT":     postgresCheckCatalogTokenNot,
		"AND":     postgresCheckCatalogTokenAnd,
		"OR":      postgresCheckCatalogTokenOr,
		"ANY":     postgresCheckCatalogTokenAny,
		"ARRAY":   postgresCheckCatalogTokenArray,
		"COLLATE": postgresCheckCatalogTokenCollate,
	}
	if kind, ok := keywords[upper]; ok {
		return postgresCheckCatalogToken{kind: kind, text: upper}
	}
	return postgresCheckCatalogToken{
		kind: postgresCheckCatalogTokenIdentifier,
		text: value,
	}
}

func (lexer *postgresCheckCatalogLexer) quotedIdentifier() (
	postgresCheckCatalogToken,
	error,
) {
	lexer.index++
	var value strings.Builder
	for lexer.index < len(lexer.input) {
		current := lexer.input[lexer.index]
		if current != '"' {
			value.WriteByte(current)
			lexer.index++
			continue
		}
		if lexer.index+1 < len(lexer.input) &&
			lexer.input[lexer.index+1] == '"' {
			value.WriteByte('"')
			lexer.index += 2
			continue
		}
		lexer.index++
		return postgresCheckCatalogToken{
			kind:   postgresCheckCatalogTokenIdentifier,
			text:   value.String(),
			quoted: true,
		}, nil
	}
	return postgresCheckCatalogToken{}, postgresCheckSignaturePolicy(
		"unterminated catalog identifier",
	)
}

func (lexer *postgresCheckCatalogLexer) stringLiteral(
	escape bool,
) (postgresCheckCatalogToken, error) {
	lexer.index++
	var value strings.Builder
	for lexer.index < len(lexer.input) {
		current := lexer.input[lexer.index]
		if current == '\'' {
			if lexer.index+1 < len(lexer.input) &&
				lexer.input[lexer.index+1] == '\'' {
				value.WriteByte('\'')
				lexer.index += 2
				continue
			}
			lexer.index++
			return postgresCheckCatalogToken{
				kind: postgresCheckCatalogTokenString,
				text: value.String(),
			}, nil
		}
		if escape && current == '\\' {
			if lexer.index+1 >= len(lexer.input) {
				return postgresCheckCatalogToken{},
					postgresCheckSignaturePolicy(
						"unterminated catalog escape",
					)
			}
			next := lexer.input[lexer.index+1]
			switch next {
			case '\\', '\'':
				value.WriteByte(next)
			case 'n':
				value.WriteByte('\n')
			case 'r':
				value.WriteByte('\r')
			case 't':
				value.WriteByte('\t')
			default:
				return postgresCheckCatalogToken{},
					postgresCheckSignaturePolicy(
						"unsupported catalog escape",
					)
			}
			lexer.index += 2
			continue
		}
		value.WriteByte(current)
		lexer.index++
	}
	return postgresCheckCatalogToken{}, postgresCheckSignaturePolicy(
		"unterminated catalog string",
	)
}

type postgresCheckCatalogParser struct {
	tokens             []postgresCheckCatalogToken
	index              int
	resolver           portableCheckColumnResolver
	collated           map[*portableCheckNode]bool
	nullCastFamily     map[*portableCheckNode]portableCheckFamily
	textRelabelColumns map[string]bool
	textRelabelled     map[*portableCheckNode]bool
}

func (parser *postgresCheckCatalogParser) parseExpression() (
	*portableCheckNode,
	error,
) {
	return parser.parseOr()
}

func (parser *postgresCheckCatalogParser) parseOr() (
	*portableCheckNode,
	error,
) {
	left, err := parser.parseAnd()
	if err != nil {
		return nil, err
	}
	for parser.match(postgresCheckCatalogTokenOr) {
		right, err := parser.parseAnd()
		if err != nil {
			return nil, err
		}
		if left.family != portableCheckBoolean ||
			right.family != portableCheckBoolean {
			return nil, postgresCheckSignaturePolicy(
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

func (parser *postgresCheckCatalogParser) parseAnd() (
	*portableCheckNode,
	error,
) {
	left, err := parser.parseNot()
	if err != nil {
		return nil, err
	}
	for parser.match(postgresCheckCatalogTokenAnd) {
		right, err := parser.parseNot()
		if err != nil {
			return nil, err
		}
		if left.family != portableCheckBoolean ||
			right.family != portableCheckBoolean {
			return nil, postgresCheckSignaturePolicy(
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

func (parser *postgresCheckCatalogParser) parseNot() (
	*portableCheckNode,
	error,
) {
	if !parser.match(postgresCheckCatalogTokenNot) {
		return parser.parsePredicate()
	}
	child, err := parser.parseNot()
	if err != nil {
		return nil, err
	}
	if child.family != portableCheckBoolean {
		return nil, postgresCheckSignaturePolicy(
			"NOT operand must be boolean",
		)
	}
	return &portableCheckNode{
		kind:   portableCheckNodeNot,
		family: portableCheckBoolean,
		left:   child,
	}, nil
}

func (parser *postgresCheckCatalogParser) parsePredicate() (
	*portableCheckNode,
	error,
) {
	left, err := parser.parsePrimary()
	if err != nil {
		return nil, err
	}
	switch parser.current().kind {
	case postgresCheckCatalogTokenEqual,
		postgresCheckCatalogTokenNotEqual,
		postgresCheckCatalogTokenLess,
		postgresCheckCatalogTokenLessEqual,
		postgresCheckCatalogTokenGreater,
		postgresCheckCatalogTokenGreaterEqual:
		operator := parser.advance()
		if operator.kind == postgresCheckCatalogTokenEqual &&
			parser.match(postgresCheckCatalogTokenAny) {
			return parser.parseAny(left)
		}
		right, err := parser.parsePrimary()
		if err != nil {
			return nil, err
		}
		portableOperator, err := postgresCatalogPortableOperator(
			operator.kind,
		)
		if err != nil {
			return nil, err
		}
		if err := validatePortableCheckComparison(
			portableOperator,
			left,
			right,
		); err != nil {
			return nil, err
		}
		if err := parser.validateComparisonCollation(
			left,
			right,
		); err != nil {
			return nil, err
		}
		if err := parser.validateNullCast(left, right); err != nil {
			return nil, err
		}
		return &portableCheckNode{
			kind:   portableCheckNodeComparison,
			family: portableCheckBoolean,
			text:   portableCheckOperator(portableOperator),
			left:   left,
			right:  right,
		}, nil
	case postgresCheckCatalogTokenIs:
		parser.advance()
		negated := parser.match(postgresCheckCatalogTokenNot)
		if !parser.match(postgresCheckCatalogTokenNull) {
			return nil, postgresCheckSignaturePolicy(
				"IS supports only NULL",
			)
		}
		if left.kind != portableCheckNodeColumn {
			return nil, postgresCheckSignaturePolicy(
				"IS NULL requires a column",
			)
		}
		return &portableCheckNode{
			kind:    portableCheckNodeIsNull,
			family:  portableCheckBoolean,
			left:    left,
			negated: negated,
		}, nil
	default:
		return left, nil
	}
}

func (parser *postgresCheckCatalogParser) parsePrimary() (
	*portableCheckNode,
	error,
) {
	token := parser.current()
	var node *portableCheckNode
	switch token.kind {
	case postgresCheckCatalogTokenIdentifier:
		parser.advance()
		column, err := parser.resolveColumn(token)
		if err != nil {
			return nil, err
		}
		node = &portableCheckNode{
			kind:        portableCheckNodeColumn,
			family:      column.family,
			numericKind: column.numericKind,
			integral:    column.integral,
			text:        column.name,
		}
	case postgresCheckCatalogTokenNumber:
		parser.advance()
		node = &portableCheckNode{
			kind:        portableCheckNodeNumber,
			family:      portableCheckNumeric,
			numericKind: portableCheckNumericUntyped,
			text:        token.text,
		}
	case postgresCheckCatalogTokenString:
		parser.advance()
		node = &portableCheckNode{
			kind:   portableCheckNodeString,
			family: portableCheckText,
			text:   token.text,
		}
	case postgresCheckCatalogTokenTrue:
		parser.advance()
		node = &portableCheckNode{
			kind:   portableCheckNodeBoolean,
			family: portableCheckBoolean,
			text:   "TRUE",
		}
	case postgresCheckCatalogTokenFalse:
		parser.advance()
		node = &portableCheckNode{
			kind:   portableCheckNodeBoolean,
			family: portableCheckBoolean,
			text:   "FALSE",
		}
	case postgresCheckCatalogTokenNull:
		parser.advance()
		node = &portableCheckNode{
			kind:   portableCheckNodeNull,
			family: portableCheckNull,
			text:   "NULL",
		}
	case postgresCheckCatalogTokenLeftParen:
		parser.advance()
		var err error
		node, err = parser.parseExpression()
		if err != nil {
			return nil, err
		}
		if !parser.match(postgresCheckCatalogTokenRightParen) {
			return nil, postgresCheckSignaturePolicy(
				"unbalanced catalog CHECK parentheses",
			)
		}
	default:
		return nil, postgresCheckSignaturePolicy(
			"expected catalog CHECK operand",
		)
	}
	for {
		switch parser.current().kind {
		case postgresCheckCatalogTokenCast:
			parser.advance()
			cast, err := parser.parseCast(node)
			if err != nil {
				return nil, err
			}
			node = cast
		case postgresCheckCatalogTokenCollate:
			parser.advance()
			if err := parser.parseCollation(node); err != nil {
				return nil, err
			}
		default:
			return node, nil
		}
	}
}

func (parser *postgresCheckCatalogParser) resolveColumn(
	token postgresCheckCatalogToken,
) (portableCheckColumn, error) {
	for _, column := range parser.resolver.columns {
		if token.quoted {
			if token.text == column.name {
				return column, nil
			}
			continue
		}
		if strings.ToLower(token.text) == token.text &&
			column.name == token.text {
			return column, nil
		}
	}
	return portableCheckColumn{}, postgresCheckSignaturePolicy(
		"unknown catalog CHECK column",
	)
}

func (parser *postgresCheckCatalogParser) parseCast(
	node *portableCheckNode,
) (*portableCheckNode, error) {
	family, name, err := parser.parseTypeName()
	if err != nil {
		return nil, err
	}
	if parser.collated[node] && family != portableCheckText {
		return nil, postgresCheckSignaturePolicy(
			"catalog cast changes collated text family",
		)
	}
	switch node.kind {
	case portableCheckNodeNumber:
		if family != portableCheckNumeric {
			return nil, postgresCheckSignaturePolicy(
				"numeric literal has incompatible catalog cast",
			)
		}
		if node.numericKind != portableCheckNumericUntyped {
			return nil, postgresCheckSignaturePolicy(
				"repeated numeric literal casts are unsupported",
			)
		}
		node.numericKind = postgresCatalogNumericKind(name)
		if node.numericKind == portableCheckNumericInvalid {
			return nil, postgresCheckSignaturePolicy("unsupported numeric cast")
		}
	case portableCheckNodeString:
		if family == portableCheckNumeric {
			canonical, err := canonicalPostgresCatalogNumber(node.text)
			if err != nil {
				return nil, err
			}
			replacement := &portableCheckNode{
				kind:        portableCheckNodeNumber,
				family:      portableCheckNumeric,
				numericKind: postgresCatalogNumericKind(name),
				text:        canonical,
			}
			if parser.collated[node] {
				parser.collated[replacement] = true
			}
			return replacement, nil
		}
		if family != portableCheckText {
			return nil, postgresCheckSignaturePolicy(
				"string literal has incompatible catalog cast",
			)
		}
	case portableCheckNodeBoolean:
		if family != portableCheckBoolean {
			return nil, postgresCheckSignaturePolicy(
				"boolean literal has incompatible catalog cast",
			)
		}
	case portableCheckNodeNull:
		parser.nullCastFamily[node] = family
		if family == portableCheckNumeric {
			node.numericKind = postgresCatalogNumericKind(name)
		}
	case portableCheckNodeColumn:
		if name != "text" || family != portableCheckText ||
			!parser.textRelabelColumns[node.text] ||
			parser.collated[node] || parser.textRelabelled[node] {
			return nil, postgresCheckSignaturePolicy(
				"catalog column cast is unsupported",
			)
		}
		parser.textRelabelled[node] = true
	default:
		return nil, postgresCheckSignaturePolicy(
			"catalog expression casts are unsupported",
		)
	}
	return node, nil
}

func canonicalPostgresCatalogNumber(value string) (string, error) {
	tokens, err := lexPortableCheck(value)
	if err != nil || len(tokens) != 2 ||
		tokens[0].kind != portableCheckTokenNumber ||
		tokens[1].kind != portableCheckTokenEOF {
		return "", postgresCheckSignaturePolicy(
			"invalid quoted catalog numeric literal",
		)
	}
	return tokens[0].text, nil
}

func (parser *postgresCheckCatalogParser) parseTypeName() (
	portableCheckFamily,
	string,
	error,
) {
	first := parser.current()
	if first.kind != postgresCheckCatalogTokenIdentifier || first.quoted {
		return portableCheckInvalid, "", postgresCheckSignaturePolicy(
			"unsupported catalog cast type",
		)
	}
	parser.advance()
	name := strings.ToLower(first.text)
	if parser.match(postgresCheckCatalogTokenDot) {
		if name != "pg_catalog" {
			return portableCheckInvalid, "", postgresCheckSignaturePolicy(
				"catalog cast uses an unsupported namespace",
			)
		}
		second := parser.current()
		if second.kind != postgresCheckCatalogTokenIdentifier ||
			second.quoted {
			return portableCheckInvalid, "", postgresCheckSignaturePolicy(
				"unsupported catalog cast type",
			)
		}
		parser.advance()
		name = strings.ToLower(second.text)
	}
	switch name {
	case "numeric", "decimal", "smallint", "integer", "bigint", "real":
		return portableCheckNumeric, name, nil
	case "double":
		if !parser.matchUnquotedIdentifier("precision") {
			return portableCheckInvalid, "", postgresCheckSignaturePolicy(
				"unsupported catalog cast type",
			)
		}
		return portableCheckNumeric, "double precision", nil
	case "text", "varchar", "bpchar":
		return portableCheckText, name, nil
	case "character":
		if parser.matchUnquotedIdentifier("varying") {
			return portableCheckText, "character varying", nil
		}
		return portableCheckText, name, nil
	case "boolean", "bool":
		return portableCheckBoolean, name, nil
	default:
		return portableCheckInvalid, "", postgresCheckSignaturePolicy(
			"unsupported catalog cast type",
		)
	}
}

func postgresCatalogNumericKind(name string) portableCheckNumericKind {
	switch name {
	case "smallint":
		return portableCheckNumericInt16
	case "integer":
		return portableCheckNumericInt32
	case "bigint":
		return portableCheckNumericInt64
	case "numeric", "decimal":
		return portableCheckNumericExact
	case "real":
		return portableCheckNumericFloat32
	case "double precision":
		return portableCheckNumericFloat64
	default:
		return portableCheckNumericInvalid
	}
}

func (parser *postgresCheckCatalogParser) matchUnquotedIdentifier(
	value string,
) bool {
	token := parser.current()
	if token.kind != postgresCheckCatalogTokenIdentifier || token.quoted ||
		!strings.EqualFold(token.text, value) {
		return false
	}
	parser.advance()
	return true
}

func (parser *postgresCheckCatalogParser) parseCollation(
	node *portableCheckNode,
) error {
	if node.family != portableCheckText || parser.collated[node] {
		return postgresCheckSignaturePolicy(
			"catalog CHECK has invalid collation placement",
		)
	}
	first := parser.current()
	if first.kind != postgresCheckCatalogTokenIdentifier {
		return postgresCheckSignaturePolicy(
			"catalog CHECK has an invalid collation",
		)
	}
	parser.advance()
	name := first
	if parser.match(postgresCheckCatalogTokenDot) {
		if first.quoted {
			if first.text != "pg_catalog" {
				return postgresCheckSignaturePolicy(
					"catalog CHECK has an unsupported collation",
				)
			}
		} else if strings.ToLower(first.text) != "pg_catalog" {
			return postgresCheckSignaturePolicy(
				"catalog CHECK has an unsupported collation",
			)
		}
		name = parser.current()
		if name.kind != postgresCheckCatalogTokenIdentifier {
			return postgresCheckSignaturePolicy(
				"catalog CHECK has an invalid collation",
			)
		}
		parser.advance()
	}
	if !name.quoted || name.text != "C" {
		return postgresCheckSignaturePolicy(
			"catalog CHECK must retain COLLATE C",
		)
	}
	parser.collated[node] = true
	return nil
}

func (parser *postgresCheckCatalogParser) parseAny(
	left *portableCheckNode,
) (*portableCheckNode, error) {
	if left.kind != portableCheckNodeColumn {
		return nil, postgresCheckSignaturePolicy(
			"ANY requires a column",
		)
	}
	switch left.family {
	case portableCheckBoolean, portableCheckNumeric, portableCheckText:
	default:
		return nil, postgresCheckSignaturePolicy(
			"ANY is unsupported for this column type",
		)
	}
	if !parser.match(postgresCheckCatalogTokenLeftParen) {
		return nil, postgresCheckSignaturePolicy(
			"ANY requires an ARRAY literal",
		)
	}
	wrappedArray := parser.match(postgresCheckCatalogTokenLeftParen)
	if !parser.match(postgresCheckCatalogTokenArray) ||
		!parser.match(postgresCheckCatalogTokenLeftBracket) {
		return nil, postgresCheckSignaturePolicy(
			"ANY requires an ARRAY literal",
		)
	}
	if parser.current().kind == postgresCheckCatalogTokenRightBracket {
		return nil, postgresCheckSignaturePolicy(
			"ANY array cannot be empty",
		)
	}
	values := make([]*portableCheckNode, 0, 2)
	for {
		value, err := parser.parsePrimary()
		if err != nil {
			return nil, err
		}
		switch value.kind {
		case portableCheckNodeNumber,
			portableCheckNodeString,
			portableCheckNodeBoolean,
			portableCheckNodeNull:
		default:
			return nil, postgresCheckSignaturePolicy(
				"ANY array members must be literals",
			)
		}
		if parser.collated[value] {
			return nil, postgresCheckSignaturePolicy(
				"ANY array literal has a collation",
			)
		}
		if value.family == portableCheckNull {
			if cast, ok := parser.nullCastFamily[value]; ok &&
				cast != left.family {
				return nil, postgresCheckSignaturePolicy(
					"ANY NULL cast is incompatible",
				)
			}
			if cast, ok := parser.nullCastFamily[value]; ok &&
				cast == portableCheckNumeric &&
				left.family == portableCheckNumeric &&
				value.numericKind != left.numericKind {
				return nil, postgresCheckSignaturePolicy(
					"ANY numeric NULL cast does not match its column",
				)
			}
		} else if value.family != left.family {
			return nil, postgresCheckSignaturePolicy(
				"ANY contains a cross-family literal",
			)
		}
		if err := validatePortableCheckNumericOperands(left, value); err != nil {
			return nil, err
		}
		values = append(values, value)
		if !parser.match(postgresCheckCatalogTokenComma) {
			break
		}
	}
	if !parser.match(postgresCheckCatalogTokenRightBracket) {
		return nil, postgresCheckSignaturePolicy(
			"invalid ANY ARRAY literal",
		)
	}
	if wrappedArray && !parser.match(postgresCheckCatalogTokenRightParen) {
		return nil, postgresCheckSignaturePolicy(
			"invalid ANY ARRAY literal",
		)
	}
	arrayTextRelabelled := false
	if parser.match(postgresCheckCatalogTokenCast) {
		family, name, err := parser.parseTypeName()
		if err != nil {
			return nil, err
		}
		if family != portableCheckText || name != "text" ||
			!parser.match(postgresCheckCatalogTokenLeftBracket) ||
			!parser.match(postgresCheckCatalogTokenRightBracket) {
			return nil, postgresCheckSignaturePolicy(
				"unsupported ANY ARRAY cast",
			)
		}
		arrayTextRelabelled = true
	}
	if wrappedArray && !arrayTextRelabelled {
		return nil, postgresCheckSignaturePolicy(
			"unexpected ANY ARRAY wrapper",
		)
	}
	if parser.textRelabelled[left] != arrayTextRelabelled {
		return nil, postgresCheckSignaturePolicy(
			"ANY ARRAY relabel does not match its column",
		)
	}
	if !parser.match(postgresCheckCatalogTokenRightParen) {
		return nil, postgresCheckSignaturePolicy(
			"invalid ANY ARRAY literal",
		)
	}
	if left.family == portableCheckText && !parser.collated[left] {
		return nil, postgresCheckSignaturePolicy(
			"text ANY must retain COLLATE C",
		)
	}
	if left.family != portableCheckText && parser.collated[left] {
		return nil, postgresCheckSignaturePolicy(
			"non-text ANY has a collation",
		)
	}
	return &portableCheckNode{
		kind:     portableCheckNodeIn,
		family:   portableCheckBoolean,
		left:     left,
		children: values,
	}, nil
}

func (parser *postgresCheckCatalogParser) validateComparisonCollation(
	left *portableCheckNode,
	right *portableCheckNode,
) error {
	leftCollated := parser.collated[left]
	rightCollated := parser.collated[right]
	if leftCollated && left.family != portableCheckText ||
		rightCollated && right.family != portableCheckText {
		return postgresCheckSignaturePolicy(
			"non-text comparison has a collation",
		)
	}
	hasText := left.family == portableCheckText ||
		right.family == portableCheckText
	if hasText && !leftCollated && !rightCollated {
		return postgresCheckSignaturePolicy(
			"text comparison must retain COLLATE C",
		)
	}
	if !hasText && (leftCollated || rightCollated) {
		return postgresCheckSignaturePolicy(
			"non-text comparison has a collation",
		)
	}
	return nil
}

func (parser *postgresCheckCatalogParser) validateNullCast(
	left *portableCheckNode,
	right *portableCheckNode,
) error {
	if left.family == portableCheckNull {
		if cast, ok := parser.nullCastFamily[left]; ok &&
			right.family != portableCheckNull && cast != right.family {
			return postgresCheckSignaturePolicy(
				"NULL cast is incompatible with comparison",
			)
		}
		if cast, ok := parser.nullCastFamily[left]; ok &&
			cast == portableCheckNumeric &&
			right.family == portableCheckNumeric &&
			left.numericKind != right.numericKind {
			return postgresCheckSignaturePolicy(
				"numeric NULL cast does not match column subtype",
			)
		}
	}
	if right.family == portableCheckNull {
		if cast, ok := parser.nullCastFamily[right]; ok &&
			left.family != portableCheckNull && cast != left.family {
			return postgresCheckSignaturePolicy(
				"NULL cast is incompatible with comparison",
			)
		}
		if cast, ok := parser.nullCastFamily[right]; ok &&
			cast == portableCheckNumeric &&
			left.family == portableCheckNumeric &&
			right.numericKind != left.numericKind {
			return postgresCheckSignaturePolicy(
				"numeric NULL cast does not match column subtype",
			)
		}
	}
	return nil
}

func postgresCatalogPortableOperator(
	kind postgresCheckCatalogTokenKind,
) (portableCheckTokenKind, error) {
	switch kind {
	case postgresCheckCatalogTokenEqual:
		return portableCheckTokenEqual, nil
	case postgresCheckCatalogTokenNotEqual:
		return portableCheckTokenNotEqual, nil
	case postgresCheckCatalogTokenLess:
		return portableCheckTokenLess, nil
	case postgresCheckCatalogTokenLessEqual:
		return portableCheckTokenLessEqual, nil
	case postgresCheckCatalogTokenGreater:
		return portableCheckTokenGreater, nil
	case postgresCheckCatalogTokenGreaterEqual:
		return portableCheckTokenGreaterEqual, nil
	default:
		return portableCheckTokenInvalid,
			postgresCheckSignaturePolicy("unsupported comparison operator")
	}
}

func (parser *postgresCheckCatalogParser) current() postgresCheckCatalogToken {
	if parser.index >= len(parser.tokens) {
		return postgresCheckCatalogToken{kind: postgresCheckCatalogTokenEOF}
	}
	return parser.tokens[parser.index]
}

func (parser *postgresCheckCatalogParser) advance() postgresCheckCatalogToken {
	token := parser.current()
	if parser.index < len(parser.tokens) {
		parser.index++
	}
	return token
}

func (parser *postgresCheckCatalogParser) match(
	kind postgresCheckCatalogTokenKind,
) bool {
	if parser.current().kind != kind {
		return false
	}
	parser.advance()
	return true
}

func postgresCheckSignaturePolicy(reason string) error {
	return &PolicyError{
		Operation: "parse PostgreSQL CHECK signature",
		Type:      reason,
		Target:    string(Postgres),
	}
}
