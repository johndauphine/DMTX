package schema

import (
	"errors"
	"strings"
	"testing"
)

func TestRenderSQLiteCheckForPostgres(t *testing.T) {
	columns := []Column{
		{Name: "amount", Type: "numeric"},
		{Name: "status", Type: "text"},
		{Name: "enabled", Type: "boolean"},
		{Name: "archived", Type: "bool"},
		{Name: "deleted_at", Type: "datetime"},
		{Name: "Order Total", Type: "integer"},
		{Name: `quote"name`, Type: "text"},
		{Name: "select", Type: "integer"},
	}
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{
			name:       "numeric comparisons and canonical literals",
			expression: `AMOUNT >= -00.50 AND amount < +020`,
			want:       `"amount" >= -0.50 AND "amount" < 20`,
		},
		{
			name:       "text equality and inequality",
			expression: `status = 'active' OR status != 'paused'`,
			want:       `"status" COLLATE "pg_catalog"."C" = E'active' OR "status" COLLATE "pg_catalog"."C" <> E'paused'`,
		},
		{
			name:       "alternate inequality",
			expression: `status <> 'blocked'`,
			want:       `"status" COLLATE "pg_catalog"."C" <> E'blocked'`,
		},
		{
			name:       "reversed text equality",
			expression: `'active' = status`,
			want:       `E'active' = "status" COLLATE "pg_catalog"."C"`,
		},
		{
			name:       "literal text equality",
			expression: `'a' = 'b'`,
			want:       `E'a' COLLATE "pg_catalog"."C" = E'b'`,
		},
		{
			name:       "text literal list",
			expression: `status IN ('active', 'O''Reilly', 'C:\tmp', NULL)`,
			want:       `"status" COLLATE "pg_catalog"."C" IN (E'active', E'O''Reilly', E'C:\\tmp', NULL)`,
		},
		{
			name:       "numeric literal list",
			expression: `amount IN (.5, 1., +002, NULL)`,
			want:       `"amount" IN (0.5, 1.0, 2, NULL)`,
		},
		{
			name:       "boolean literal list",
			expression: `enabled IN (TRUE, false, NULL)`,
			want:       `"enabled" IN (TRUE, FALSE, NULL)`,
		},
		{
			name:       "null predicates on opaque column",
			expression: `deleted_at IS NULL OR deleted_at IS NOT NULL`,
			want:       `"deleted_at" IS NULL OR "deleted_at" IS NOT NULL`,
		},
		{
			name:       "boolean columns and precedence",
			expression: `enabled AND NOT archived OR enabled = TRUE`,
			want:       `"enabled" AND NOT "archived" OR "enabled" = TRUE`,
		},
		{
			name:       "parentheses preserve lower precedence branch",
			expression: `enabled AND (archived OR enabled = FALSE)`,
			want:       `"enabled" AND ("archived" OR "enabled" = FALSE)`,
		},
		{
			name:       "not predicate",
			expression: `NOT (amount >= 10)`,
			want:       `NOT ("amount" >= 10)`,
		},
		{
			name:       "quoted identifiers",
			expression: `"Order Total" >= 0 AND [select] = 1`,
			want:       `"Order Total" >= 0 AND "select" = 1`,
		},
		{
			name:       "escaped quoted identifier",
			expression: `"quote""name" = 'safe'`,
			want:       `"quote""name" COLLATE "pg_catalog"."C" = E'safe'`,
		},
		{
			name:       "backtick identifier",
			expression: "`Order Total` = 1",
			want:       `"Order Total" = 1`,
		},
		{
			name:       "parenthesized scalar",
			expression: `((amount)) >= 0`,
			want:       `"amount" >= 0`,
		},
		{
			name:       "literal comparison",
			expression: `1 < 2`,
			want:       `1 < 2`,
		},
		{
			name:       "boolean root",
			expression: `TRUE`,
			want:       `TRUE`,
		},
		{
			name:       "null equality remains structurally null",
			expression: `status = NULL`,
			want:       `"status" COLLATE "pg_catalog"."C" = NULL`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseSQLiteCheckExpression(test.expression)
			if err != nil {
				t.Fatalf("parse source CHECK: %v", err)
			}
			got, err := RenderSQLiteCheckForPostgres(expression, columns)
			if err != nil {
				t.Fatalf("render portable CHECK: %v", err)
			}
			if got != test.want {
				t.Fatalf("rendered = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRenderSQLiteCheckForPostgresUsesDeclaredTypeFamily(t *testing.T) {
	expression, err := ParseSQLiteCheckExpression(
		`code = 'A' AND score >= 1`,
	)
	if err != nil {
		t.Fatalf("parse source CHECK: %v", err)
	}
	got, err := RenderSQLiteCheckForPostgres(expression, []Column{
		{
			Name:         "code",
			Type:         "text",
			DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{8}},
		},
		{
			Name:         "score",
			Type:         "numeric",
			DeclaredType: &DeclaredType{Base: "decimal", Arguments: []int{7, 2}},
		},
	})
	if err != nil {
		t.Fatalf("render portable CHECK: %v", err)
	}
	want := `"code" COLLATE "pg_catalog"."C" = E'A' AND "score" >= 1`
	if got != want {
		t.Fatalf("rendered = %q, want %q", got, want)
	}
}

func TestRenderPortableCheckFailsClosedOnIntegerDecimalsAndFixedChar(
	t *testing.T,
) {
	tests := []struct {
		name       string
		expression string
		column     Column
	}{
		{
			name:       "integer decimal comparison",
			expression: `id >= 0.5`,
			column:     Column{Name: "id", Type: "integer"},
		},
		{
			name:       "integer decimal IN",
			expression: `id IN (1, 2.0)`,
			column:     Column{Name: "id", Type: "integer"},
		},
		{
			name:       "fixed char equality",
			expression: `code = 'A'`,
			column: Column{
				Name: "code",
				Type: "char",
				DeclaredType: &DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
			},
		},
		{
			name:       "fixed char IN",
			expression: `code IN ('A', 'B')`,
			column: Column{
				Name: "code",
				Type: "char",
				DeclaredType: &DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseSQLiteCheckExpression(test.expression)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RenderSQLiteCheckForPostgres(
				expression,
				[]Column{test.column},
			); err == nil {
				t.Fatal("expected CHECK to fail closed")
			}
		})
	}

	nullCheck, err := ParseSQLiteCheckExpression(`code IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RenderSQLiteCheckForPostgres(nullCheck, []Column{{
		Name: "code",
		Type: "char",
		DeclaredType: &DeclaredType{
			Base:      "char",
			Arguments: []int{4},
		},
	}}); err != nil {
		t.Fatalf("fixed CHAR IS NULL should remain portable: %v", err)
	}
}

func TestRenderSQLiteCheckForPostgresQuotesUntrustedStructure(t *testing.T) {
	columnName := `x"; DROP TABLE accounts; --`
	source := `"x""; DROP TABLE accounts; --" = 1`
	expression, err := ParseSQLiteCheckExpression(source)
	if err != nil {
		t.Fatalf("parse source CHECK: %v", err)
	}
	got, err := RenderSQLiteCheckForPostgres(expression, []Column{{
		Name: columnName,
		Type: "integer",
	}})
	if err != nil {
		t.Fatalf("render portable CHECK: %v", err)
	}
	want := `"x""; DROP TABLE accounts; --" = 1`
	if got != want {
		t.Fatalf("rendered = %q, want %q", got, want)
	}

	literal, err := ParseSQLiteCheckExpression(
		`status = 'safe''; DROP TABLE accounts; --'`,
	)
	if err != nil {
		t.Fatalf("parse source literal CHECK: %v", err)
	}
	got, err = RenderSQLiteCheckForPostgres(literal, []Column{{
		Name: "status",
		Type: "text",
	}})
	if err != nil {
		t.Fatalf("render portable literal CHECK: %v", err)
	}
	want = `"status" COLLATE "pg_catalog"."C" = E'safe''; DROP TABLE accounts; --'`
	if got != want {
		t.Fatalf("literal rendered = %q, want %q", got, want)
	}
}

func TestRenderSQLiteCheckForPostgresFailsClosed(t *testing.T) {
	columns := []Column{
		{Name: "amount", Type: "numeric"},
		{Name: "status", Type: "text"},
		{Name: "enabled", Type: "boolean"},
		{Name: "payload", Type: "blob"},
		{Name: "document", Type: "json"},
	}
	tests := []struct {
		name       string
		expression string
	}{
		{name: "unknown column", expression: `missing = 1`},
		{name: "function", expression: `length(status) > 0`},
		{name: "like", expression: `status LIKE 'a%'`},
		{name: "glob", expression: `status GLOB 'a*'`},
		{name: "collation", expression: `status COLLATE nocase = 'a'`},
		{name: "addition", expression: `amount + 1 > 0`},
		{name: "subtraction", expression: `amount - 1 > 0`},
		{name: "multiplication", expression: `amount * 2 > 0`},
		{name: "division", expression: `amount / 2 > 0`},
		{name: "concatenation", expression: `status || 'x' = 'ax'`},
		{name: "subquery", expression: `amount IN (SELECT amount FROM source)`},
		{name: "text ordering", expression: `status < 'z'`},
		{name: "boolean ordering", expression: `enabled > FALSE`},
		{name: "cross family equality", expression: `status = 1`},
		{name: "cross family column equality", expression: `status = amount`},
		{name: "boolean numeric coercion", expression: `enabled = 1`},
		{name: "opaque equality", expression: `payload = 'abc'`},
		{name: "opaque NULL equality", expression: `document = NULL`},
		{name: "opaque NULL inequality reversed", expression: `NULL <> document`},
		{name: "nonboolean numeric root", expression: `amount`},
		{name: "nonboolean text root", expression: `status`},
		{name: "null root", expression: `NULL`},
		{name: "numeric and", expression: `amount AND TRUE`},
		{name: "text or", expression: `FALSE OR status`},
		{name: "numeric not", expression: `NOT amount`},
		{name: "comparison expression operand", expression: `(amount = 1) = TRUE`},
		{name: "is true", expression: `enabled IS TRUE`},
		{name: "is literal", expression: `1 IS NULL`},
		{name: "not in syntax", expression: `amount NOT IN (1, 2)`},
		{name: "in literal left", expression: `1 IN (1, 2)`},
		{name: "in empty", expression: `amount IN ()`},
		{name: "in identifier member", expression: `amount IN (amount)`},
		{name: "in expression member", expression: `amount IN (1 + 2)`},
		{name: "in trailing comma", expression: `amount IN (1,)`},
		{name: "in cross family", expression: `amount IN (1, '2')`},
		{name: "qualified identifier", expression: `items.amount > 0`},
		{name: "between", expression: `amount BETWEEN 1 AND 2`},
		{name: "parameter", expression: `amount > ?`},
		{name: "semicolon", expression: `amount > 0; SELECT 1`},
		{name: "line comment", expression: `amount > 0 -- comment`},
		{name: "block comment", expression: `amount > 0 /* comment */`},
		{name: "double equal", expression: `amount == 1`},
		{name: "chained comparison", expression: `0 < amount < 10`},
		{name: "exponent literal", expression: `amount < 1e3`},
		{name: "spaced unary sign", expression: `amount < - 1`},
		{name: "malformed parentheses", expression: `(amount > 0`},
		{name: "extra closing parenthesis", expression: `amount > 0)`},
		{name: "unknown double quoted identifier", expression: `"missing" = 1`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rendered, err := RenderSQLiteCheckForPostgres(
				Expression{
					kind: expressionCheck,
					sql:  test.expression,
				},
				columns,
			)
			if err == nil {
				t.Fatalf(
					"rendered unsupported expression as %q",
					rendered,
				)
			}
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %T %v, want PolicyError", err, err)
			}
			if policy.Target != string(Postgres) {
				t.Fatalf(
					"policy target = %q, want %q",
					policy.Target,
					Postgres,
				)
			}
		})
	}
}

func TestRenderSQLiteCheckForPostgresRejectsInvalidMetadataAndInput(t *testing.T) {
	valid, err := ParseSQLiteCheckExpression(`amount >= 0`)
	if err != nil {
		t.Fatalf("parse source CHECK: %v", err)
	}
	tests := []struct {
		name       string
		expression Expression
		columns    []Column
	}{
		{
			name:       "non check expression",
			expression: Expression{kind: expressionNumber, literal: "1"},
			columns:    []Column{{Name: "amount", Type: "numeric"}},
		},
		{
			name:       "empty check",
			expression: Expression{kind: expressionCheck},
			columns:    []Column{{Name: "amount", Type: "numeric"}},
		},
		{
			name: "invalid utf8 expression",
			expression: Expression{
				kind: expressionCheck,
				sql:  string([]byte{0xff}),
			},
			columns: []Column{{Name: "amount", Type: "numeric"}},
		},
		{
			name: "nul expression",
			expression: Expression{
				kind: expressionCheck,
				sql:  "amount\x00 > 0",
			},
			columns: []Column{{Name: "amount", Type: "numeric"}},
		},
		{
			name:       "empty column name",
			expression: valid,
			columns:    []Column{{Name: "", Type: "numeric"}},
		},
		{
			name:       "nul column name",
			expression: valid,
			columns:    []Column{{Name: "amount\x00", Type: "numeric"}},
		},
		{
			name:       "invalid utf8 column name",
			expression: valid,
			columns: []Column{{
				Name: string([]byte{0xff}),
				Type: "numeric",
			}},
		},
		{
			name:       "case ambiguous columns",
			expression: valid,
			columns: []Column{
				{Name: "amount", Type: "numeric"},
				{Name: "AMOUNT", Type: "numeric"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rendered, err := RenderSQLiteCheckForPostgres(
				test.expression,
				test.columns,
			)
			if err == nil {
				t.Fatalf("rendered invalid input as %q", rendered)
			}
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %T %v, want PolicyError", err, err)
			}
		})
	}
}

func TestRenderSQLiteCheckForPostgresRejectsOversizedNumber(t *testing.T) {
	expression := Expression{
		kind: expressionCheck,
		sql:  "amount < " + strings.Repeat("9", 1003),
	}
	rendered, err := RenderSQLiteCheckForPostgres(expression, []Column{{
		Name: "amount",
		Type: "numeric",
	}})
	if err == nil {
		t.Fatalf("rendered oversized literal as %q", rendered)
	}
}
