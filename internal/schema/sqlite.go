package schema

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type sqliteTypeRule struct {
	canonical string
	maxArgs   int
}

var sqliteTypeRules = map[string]sqliteTypeRule{
	"int":               {canonical: "INT"},
	"integer":           {canonical: "INTEGER"},
	"tinyint":           {canonical: "TINYINT"},
	"smallint":          {canonical: "SMALLINT"},
	"mediumint":         {canonical: "MEDIUMINT"},
	"bigint":            {canonical: "BIGINT"},
	"unsigned big int":  {canonical: "UNSIGNED BIG INT"},
	"int2":              {canonical: "INT2"},
	"int8":              {canonical: "INT8"},
	"char":              {canonical: "CHAR", maxArgs: 1},
	"character":         {canonical: "CHARACTER", maxArgs: 1},
	"character varying": {canonical: "CHARACTER VARYING", maxArgs: 1},
	"varchar":           {canonical: "VARCHAR", maxArgs: 1},
	"varying character": {canonical: "VARYING CHARACTER", maxArgs: 1},
	"binary":            {canonical: "BINARY", maxArgs: 1},
	"varbinary":         {canonical: "VARBINARY", maxArgs: 1},
	"nchar":             {canonical: "NCHAR", maxArgs: 1},
	"native character":  {canonical: "NATIVE CHARACTER", maxArgs: 1},
	"nvarchar":          {canonical: "NVARCHAR", maxArgs: 1},
	"text":              {canonical: "TEXT"},
	"clob":              {canonical: "CLOB"},
	"blob":              {canonical: "BLOB"},
	"real":              {canonical: "REAL"},
	"double":            {canonical: "DOUBLE"},
	"double precision":  {canonical: "DOUBLE PRECISION"},
	"float":             {canonical: "FLOAT", maxArgs: 1},
	"numeric":           {canonical: "NUMERIC", maxArgs: 2},
	"decimal":           {canonical: "DECIMAL", maxArgs: 2},
	"boolean":           {canonical: "BOOLEAN"},
	"bool":              {canonical: "BOOL"},
	"date":              {canonical: "DATE"},
	"datetime":          {canonical: "DATETIME", maxArgs: 1},
	"timestamp":         {canonical: "TIMESTAMP", maxArgs: 1},
	"time":              {canonical: "TIME", maxArgs: 1},
	"json":              {canonical: "JSON"},
	"uuid":              {canonical: "UUID"},
	"any":               {canonical: "ANY"},
}

var sqliteDeclaredTypePattern = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*(?:[ \t]+[A-Za-z][A-Za-z0-9_]*)*)(?:[ \t]*\([ \t]*([0-9]+)(?:[ \t]*,[ \t]*([0-9]+))?[ \t]*\))?$`)
var sqliteNumberPattern = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)
var sqliteBlobPattern = regexp.MustCompile(`(?i)^x'(?:[0-9a-f]{2})*'$`)

// ParseSQLiteDeclaredType converts catalog text into a constrained structured
// declaration. Arbitrary SQLite affinity names are rejected rather than copied
// into target DDL as executable text.
func ParseSQLiteDeclaredType(value string) (*DeclaredType, error) {
	match := sqliteDeclaredTypePattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return nil, &PolicyError{Operation: "parse SQLite declared type", Type: value, Target: string(SQLite)}
	}
	base := strings.ToLower(strings.Join(strings.Fields(match[1]), " "))
	rule, ok := sqliteTypeRules[base]
	if !ok {
		return nil, &PolicyError{Operation: "parse SQLite declared type", Type: value, Target: string(SQLite)}
	}
	arguments := make([]int, 0, 2)
	for _, raw := range match[2:] {
		if raw == "" {
			continue
		}
		argument, err := strconv.Atoi(raw)
		if err != nil || argument < 0 {
			return nil, &PolicyError{Operation: "parse SQLite type modifier", Type: value, Target: string(SQLite)}
		}
		arguments = append(arguments, argument)
	}
	if len(arguments) > rule.maxArgs {
		return nil, &PolicyError{Operation: "parse SQLite type modifier", Type: value, Target: string(SQLite)}
	}
	return &DeclaredType{Base: base, Arguments: arguments}, nil
}

func renderColumnType(column Column, target Dialect) (string, error) {
	if column.DeclaredType == nil {
		return MapType(column.Type, target)
	}
	switch target {
	case SQLite:
		return renderSQLiteDeclaredType(*column.DeclaredType)
	case Postgres:
		return renderPostgresDeclaredType(*column.DeclaredType)
	}
	if len(column.DeclaredType.Arguments) > 0 {
		return "", &PolicyError{Operation: "map declared type modifiers", Type: column.Type, Target: string(target)}
	}
	return MapType(column.DeclaredType.Base, target)
}

func renderSQLiteDeclaredType(value DeclaredType) (string, error) {
	rule, ok := sqliteTypeRules[value.Base]
	if !ok || len(value.Arguments) > rule.maxArgs {
		return "", &PolicyError{Operation: "render SQLite declared type", Type: value.Base, Target: string(SQLite)}
	}
	if len(value.Arguments) == 0 {
		return rule.canonical, nil
	}
	arguments := make([]string, len(value.Arguments))
	for index, argument := range value.Arguments {
		if argument < 0 {
			return "", &PolicyError{Operation: "render SQLite type modifier", Type: value.Base, Target: string(SQLite)}
		}
		arguments[index] = strconv.Itoa(argument)
	}
	return rule.canonical + "(" + strings.Join(arguments, ",") + ")", nil
}

// ParseSQLiteDefault accepts only literal defaults and SQLite's deterministic
// current-time keywords. Other expressions fail closed.
func ParseSQLiteDefault(value string) (*Expression, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, &PolicyError{Operation: "parse SQLite default", Type: value, Target: string(SQLite)}
	}
	if hasSingleOuterParentheses(trimmed) {
		inner, err := ParseSQLiteDefault(strings.TrimSpace(trimmed[1 : len(trimmed)-1]))
		if err != nil {
			return nil, err
		}
		return &Expression{
			sql:     "(" + inner.sql + ")",
			kind:    inner.kind,
			literal: inner.literal,
		}, nil
	}
	upper := strings.ToUpper(trimmed)
	switch upper {
	case "NULL":
		return &Expression{sql: upper, kind: expressionNull}, nil
	case "TRUE", "FALSE":
		return &Expression{
			sql:     upper,
			kind:    expressionBoolean,
			literal: upper,
		}, nil
	case "CURRENT_TIME":
		return &Expression{sql: upper, kind: expressionCurrentTime}, nil
	case "CURRENT_DATE":
		return &Expression{sql: upper, kind: expressionCurrentDate}, nil
	case "CURRENT_TIMESTAMP":
		return &Expression{sql: upper, kind: expressionCurrentTimestamp}, nil
	}
	if sqliteNumberPattern.MatchString(trimmed) {
		return &Expression{
			sql:     trimmed,
			kind:    expressionNumber,
			literal: trimmed,
		}, nil
	}
	if sqliteBlobPattern.MatchString(trimmed) {
		canonical := "X" + trimmed[1:]
		return &Expression{
			sql:     canonical,
			kind:    expressionBlob,
			literal: canonical[2 : len(canonical)-1],
		}, nil
	}
	if validSingleQuotedLiteral(trimmed) {
		return &Expression{
			sql:  trimmed,
			kind: expressionString,
			literal: strings.ReplaceAll(
				trimmed[1:len(trimmed)-1],
				"''",
				"'",
			),
		}, nil
	}
	return nil, &PolicyError{Operation: "parse SQLite default expression", Type: value, Target: string(SQLite)}
}

// ParseSQLiteCheckExpression validates that an already parsed SQLite CHECK body
// cannot escape its expression boundary. SQLite has already checked grammar;
// this conservative pass rejects statement tokens, comments, parameters, and
// unknown function calls before DMTX re-renders the expression.
func ParseSQLiteCheckExpression(value string) (Expression, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return Expression{}, &PolicyError{Operation: "parse SQLite CHECK", Type: value, Target: string(SQLite)}
	}
	if err := validateSQLiteExpression(trimmed); err != nil {
		return Expression{}, err
	}
	return Expression{
		sql:  trimmed,
		kind: expressionCheck,
	}, nil
}

func rejectSQLiteOnlySchema(target Dialect, table Table) error {
	if target == Postgres {
		if table.SQLiteStrict || table.SQLiteWithoutRowID {
			return &PolicyError{Operation: "map SQLite schema objects", Type: table.Name, Target: string(target)}
		}
		_, err := postgresIdentityColumn(table)
		return err
	}
	if table.AutoIncrementColumn != "" || table.SQLiteSequence != nil || table.SQLiteStrict || table.SQLiteWithoutRowID || len(table.Indexes) > 0 || len(table.ForeignKeys) > 0 || len(table.Checks) > 0 {
		return &PolicyError{Operation: "map SQLite schema objects", Type: table.Name, Target: string(target)}
	}
	for _, column := range table.Columns {
		if target != Postgres && column.Default != nil {
			return &PolicyError{Operation: "map SQLite default", Type: column.Name, Target: string(target)}
		}
	}
	return nil
}

func renderDefault(target Dialect, column Column) (string, error) {
	if column.Default == nil {
		return "", &PolicyError{
			Operation: "render default",
			Type:      column.Name,
			Target:    string(target),
		}
	}
	switch target {
	case SQLite:
		return column.Default.sql, nil
	case Postgres:
		return renderPostgresDefault(column)
	default:
		return "", &PolicyError{
			Operation: "render default",
			Type:      column.Name,
			Target:    string(target),
		}
	}
}

// CreateIndexes renders deterministic standalone index statements. Inline
// unique constraints are emitted by CreateTable.
func CreateIndexes(target Dialect, table Table) ([]string, error) {
	if target != SQLite {
		return nil, &PolicyError{Operation: "create indexes", Type: table.Name, Target: string(target)}
	}
	statements := make([]string, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		if index.Inline {
			continue
		}
		if index.Name == "" {
			return nil, &PolicyError{Operation: "create SQLite index", Type: "empty index name", Target: string(SQLite)}
		}
		columns, err := renderSQLiteIndexColumns(index.Columns)
		if err != nil {
			return nil, err
		}
		unique := ""
		if index.Unique {
			unique = "UNIQUE "
		}
		statements = append(statements, "CREATE "+unique+"INDEX "+quote(SQLite, index.Name)+" ON "+qualified(SQLite, table.Schema, table.Name)+" ("+columns+");")
	}
	return statements, nil
}

func renderSQLiteIndexColumns(columns []IndexColumn) (string, error) {
	if len(columns) == 0 {
		return "", &PolicyError{Operation: "create SQLite index", Type: "index without columns", Target: string(SQLite)}
	}
	rendered := make([]string, len(columns))
	for index, column := range columns {
		if column.Name == "" {
			return "", &PolicyError{Operation: "create SQLite expression index", Type: "expression index", Target: string(SQLite)}
		}
		value := quote(SQLite, column.Name)
		if column.Collation != "" {
			collation := strings.ToUpper(column.Collation)
			if collation != "BINARY" && collation != "NOCASE" && collation != "RTRIM" {
				return "", &PolicyError{Operation: "create SQLite index collation", Type: column.Collation, Target: string(SQLite)}
			}
			value += " COLLATE " + collation
		}
		if column.Descending {
			value += " DESC"
		}
		rendered[index] = value
	}
	return strings.Join(rendered, ", "), nil
}

func renderSQLiteForeignKey(foreignKey ForeignKey) (string, error) {
	if len(foreignKey.Columns) == 0 || foreignKey.ReferencedTable == "" {
		return "", &PolicyError{Operation: "create SQLite foreign key", Type: "incomplete foreign key", Target: string(SQLite)}
	}
	statement := "FOREIGN KEY (" + quotedIdentifiers(foreignKey.Columns) + ") REFERENCES " + quote(SQLite, foreignKey.ReferencedTable)
	if len(foreignKey.ReferencedColumns) > 0 {
		if len(foreignKey.ReferencedColumns) != len(foreignKey.Columns) {
			return "", &PolicyError{Operation: "create SQLite foreign key", Type: "mismatched referenced columns", Target: string(SQLite)}
		}
		statement += " (" + quotedIdentifiers(foreignKey.ReferencedColumns) + ")"
	}
	actions := []struct{ label, action string }{
		{label: "ON UPDATE", action: foreignKey.OnUpdate},
		{label: "ON DELETE", action: foreignKey.OnDelete},
	}
	for _, item := range actions {
		label, action := item.label, item.action
		normalized := strings.ToUpper(strings.TrimSpace(action))
		if normalized == "" {
			normalized = "NO ACTION"
		}
		switch normalized {
		case "NO ACTION", "RESTRICT", "SET NULL", "SET DEFAULT", "CASCADE":
		default:
			return "", &PolicyError{Operation: "create SQLite foreign key action", Type: action, Target: string(SQLite)}
		}
		statement += " " + label + " " + normalized
	}
	match := strings.ToUpper(strings.TrimSpace(foreignKey.Match))
	if match != "" && match != "NONE" {
		if match != "SIMPLE" && match != "FULL" && match != "PARTIAL" {
			return "", &PolicyError{Operation: "create SQLite foreign key match", Type: foreignKey.Match, Target: string(SQLite)}
		}
		statement += " MATCH " + match
	}
	return statement, nil
}

func quotedIdentifiers(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quote(SQLite, value)
	}
	return strings.Join(quoted, ", ")
}

type Statement struct {
	SQL  string
	Args []any
}

// SQLiteSequencePlan restores the source AUTOINCREMENT frontier after explicit
// primary-key values have been loaded.
func SQLiteSequencePlan(table Table) ([]Statement, error) {
	if table.AutoIncrementColumn == "" || table.SQLiteSequence == nil {
		return nil, nil
	}
	return []Statement{
		{SQL: `DELETE FROM sqlite_sequence WHERE name = ?`, Args: []any{table.Name}},
		{SQL: `INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)`, Args: []any{table.Name, *table.SQLiteSequence}},
	}, nil
}

func validSingleQuotedLiteral(value string) bool {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		if value[index] != '\'' {
			continue
		}
		if index+1 >= len(value)-1 || value[index+1] != '\'' {
			return false
		}
		index++
	}
	return true
}

func hasSingleOuterParentheses(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}
	depth := 0
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '\'':
			next, ok := consumeQuoted(value, index, '\'', '\'')
			if !ok {
				return false
			}
			index = next
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && index != len(value)-1 {
				return false
			}
		}
	}
	return depth == 0
}

var forbiddenExpressionTokens = map[string]bool{
	"ALTER": true, "ATTACH": true, "CREATE": true, "DELETE": true,
	"DETACH": true, "DROP": true, "INSERT": true, "PRAGMA": true,
	"REINDEX": true, "REPLACE": true, "SELECT": true, "UPDATE": true,
	"VACUUM": true, "WITH": true,
}

var allowedSQLiteFunctions = map[string]bool{
	"ABS": true, "COALESCE": true, "GLOB": true, "HEX": true,
	"IFNULL": true, "JSON_VALID": true, "LENGTH": true, "LIKE": true,
	"LOWER": true, "LTRIM": true, "NULLIF": true, "PRINTF": true,
	"REPLACE": true, "ROUND": true, "RTRIM": true, "SUBSTR": true,
	"TRIM": true, "TYPEOF": true, "UNICODE": true, "UPPER": true,
}

var expressionKeywords = map[string]bool{
	"AND": true, "AS": true, "BETWEEN": true, "CASE": true, "COLLATE": true,
	"ELSE": true, "END": true, "ESCAPE": true, "FALSE": true, "GLOB": true,
	"IN": true, "IS": true, "ISNULL": true, "LIKE": true, "MATCH": true,
	"NOT": true, "NOTNULL": true, "NULL": true, "OR": true, "REGEXP": true,
	"THEN": true, "TRUE": true, "WHEN": true,
}

func validateSQLiteExpression(value string) error {
	depth := 0
	for index := 0; index < len(value); {
		current := value[index]
		if unicode.IsSpace(rune(current)) {
			index++
			continue
		}
		if current == ';' || current == '?' || current == '$' || current == '@' || current == ':' {
			return &PolicyError{Operation: "parse SQLite CHECK expression", Type: value, Target: string(SQLite)}
		}
		if index+1 < len(value) && ((current == '-' && value[index+1] == '-') || (current == '/' && value[index+1] == '*')) {
			return &PolicyError{Operation: "parse SQLite CHECK comment", Type: value, Target: string(SQLite)}
		}
		if current == '\'' || current == '"' || current == '`' {
			next, ok := consumeQuoted(value, index, current, current)
			if !ok {
				return &PolicyError{Operation: "parse SQLite CHECK quote", Type: value, Target: string(SQLite)}
			}
			index = next + 1
			continue
		}
		if current == '[' {
			next := strings.IndexByte(value[index+1:], ']')
			if next < 0 {
				return &PolicyError{Operation: "parse SQLite CHECK identifier", Type: value, Target: string(SQLite)}
			}
			index += next + 2
			continue
		}
		if current == '(' {
			depth++
			index++
			continue
		}
		if current == ')' {
			depth--
			if depth < 0 {
				return &PolicyError{Operation: "parse SQLite CHECK parentheses", Type: value, Target: string(SQLite)}
			}
			index++
			continue
		}
		if isIdentifierStart(current) {
			start := index
			for index < len(value) && isIdentifierPart(value[index]) {
				index++
			}
			token := strings.ToUpper(value[start:index])
			next := index
			for next < len(value) && unicode.IsSpace(rune(value[next])) {
				next++
			}
			isFunction := next < len(value) && value[next] == '(' && !expressionKeywords[token]
			if forbiddenExpressionTokens[token] && !(isFunction && allowedSQLiteFunctions[token]) {
				return &PolicyError{Operation: "parse SQLite CHECK statement token", Type: token, Target: string(SQLite)}
			}
			if isFunction && !allowedSQLiteFunctions[token] {
				return &PolicyError{Operation: "parse SQLite CHECK function", Type: token, Target: string(SQLite)}
			}
			continue
		}
		if strings.ContainsRune("0123456789.,+-*/%<>=!|&~", rune(current)) {
			index++
			continue
		}
		return &PolicyError{Operation: "parse SQLite CHECK character", Type: string(current), Target: string(SQLite)}
	}
	if depth != 0 {
		return &PolicyError{Operation: "parse SQLite CHECK parentheses", Type: value, Target: string(SQLite)}
	}
	return nil
}

func consumeQuoted(value string, start int, opening, closing byte) (int, bool) {
	for index := start + 1; index < len(value); index++ {
		if value[index] != closing {
			continue
		}
		if index+1 < len(value) && value[index+1] == closing {
			index++
			continue
		}
		return index, true
	}
	return 0, false
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9'
}
