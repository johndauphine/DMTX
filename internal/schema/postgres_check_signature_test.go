package schema

import (
	"errors"
	"testing"
)

func TestPostgresCheckSignaturesNormalizePortablePG16Deparse(
	t *testing.T,
) {
	t.Parallel()
	columns := postgresCheckSignatureColumns()
	tests := []struct {
		name    string
		source  string
		catalog string
	}{
		{
			name:    "numeric comparison with inserted cast",
			source:  `balance >= 0`,
			catalog: `balance >= 0::numeric`,
		},
		{
			name:    "negative zero loses its sign",
			source:  `amount >= -0.00`,
			catalog: `amount >= 0.00`,
		},
		{
			name:    "singleton numeric IN becomes equality",
			source:  `amount IN (1)`,
			catalog: `amount = 1::numeric`,
		},
		{
			name:    "singleton text IN becomes equality",
			source:  `status IN ('active')`,
			catalog: `(status COLLATE "C") = 'active'::text`,
		},
		{
			name:    "singleton varchar IN becomes equality",
			source:  `short_status IN ('active')`,
			catalog: `((short_status)::text COLLATE "C") = 'active'::text`,
		},
		{
			name:    "singleton char IN becomes equality",
			source:  `fixed_status IN ('a')`,
			catalog: `(fixed_status COLLATE "C") = 'a'::bpchar`,
		},
		{
			name:    "singleton boolean IN becomes equality",
			source:  `enabled IN (TRUE)`,
			catalog: `enabled = true`,
		},
		{
			name:    "singleton NULL IN becomes equality",
			source:  `amount IN (NULL)`,
			catalog: `amount = NULL::numeric`,
		},
		{
			name:   "text IN with NULL rewritten to ANY ARRAY",
			source: `status IN ('active', 'paused', NULL)`,
			catalog: `(status COLLATE "C") = ANY (` +
				`ARRAY['active'::text, 'paused'::text, NULL::text])`,
		},
		{
			name:   "AND OR precedence and IS NULL",
			source: `amount >= 0 AND amount <= 10 OR amount IS NULL`,
			catalog: `amount >= 0::numeric AND amount <= 10::numeric ` +
				`OR amount IS NULL`,
		},
		{
			name:    "NOT comparison",
			source:  `NOT enabled = FALSE`,
			catalog: `NOT enabled = false`,
		},
		{
			name:    "quoted identifier and quoted negative numeric",
			source:  `"Order Total" >= -1.50`,
			catalog: `"Order Total" >= '-1.50'::numeric`,
		},
		{
			name:    "schema qualified C and escaped identifier",
			source:  `"quote""name" = 'safe'`,
			catalog: `("quote""name" COLLATE pg_catalog."C") = 'safe'::text`,
		},
		{
			name:    "varchar comparison with text relabel",
			source:  `short_status = 'active'`,
			catalog: `(((short_status)::text COLLATE "C") = 'active'::text)`,
		},
		{
			name:   "varchar IN with array text relabel",
			source: `short_status IN ('active', 'paused', NULL)`,
			catalog: `(((short_status)::text COLLATE "C") = ANY (` +
				`(ARRAY['active'::character varying, ` +
				`'paused'::character varying, ` +
				`NULL::character varying])::text[]))`,
		},
		{
			name:   "opaque IS NULL and IS NOT NULL",
			source: `deleted_at IS NULL OR deleted_at IS NOT NULL`,
			catalog: `(deleted_at IS NULL) OR ` +
				`(deleted_at IS NOT NULL)`,
		},
		{
			name:    "boolean IN with typed NULL",
			source:  `enabled IN (TRUE, FALSE, NULL)`,
			catalog: `enabled = ANY (ARRAY[true, false, NULL::boolean])`,
		},
		{
			name:    "numeric IN with typed NULL",
			source:  `amount IN (.5, 2, NULL)`,
			catalog: `amount = ANY (ARRAY[0.5::numeric, 2::numeric, NULL::numeric])`,
		},
		{
			name:    "text equality to typed NULL",
			source:  `status = NULL`,
			catalog: `(status COLLATE "C") = NULL::text`,
		},
		{
			name:    "numeric equality to typed NULL",
			source:  `amount = NULL`,
			catalog: `amount = NULL::numeric`,
		},
		{
			name:    "boolean equality",
			source:  `enabled = TRUE`,
			catalog: `enabled = true::boolean`,
		},
		{
			name:    "boolean inequality to typed NULL",
			source:  `enabled <> NULL`,
			catalog: `enabled <> NULL::boolean`,
		},
		{
			name:    "literal text comparison retains C",
			source:  `'a' = 'b'`,
			catalog: `('a'::text COLLATE "C") = 'b'::text`,
		},
		{
			name:    "parentheses normalize around AST",
			source:  `enabled AND (archived OR enabled = FALSE)`,
			catalog: `(enabled AND ((archived OR (enabled = false))))`,
		},
		{
			name:    "quoted string and escape string",
			source:  `status = 'O''Reilly\path'`,
			catalog: `(status COLLATE "C") = E'O''Reilly\\path'::text`,
		},
		{
			name:    "bare boolean column",
			source:  `enabled`,
			catalog: `enabled`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			planned := mustPlannedPostgresCheckSignature(
				t,
				test.source,
				columns,
			)
			actual, err := ParsePostgresCheckSignature(
				test.catalog,
				columns,
			)
			if err != nil {
				t.Fatalf("parse catalog CHECK: %v", err)
			}
			if actual != planned {
				t.Fatalf(
					"signature mismatch:\n planned: %s\n  actual: %s",
					planned,
					actual,
				)
			}
			again, err := ParsePostgresCheckSignature(
				test.catalog,
				columns,
			)
			if err != nil || again != actual {
				t.Fatalf("catalog signature was not deterministic")
			}
		})
	}
}

func TestPlannedPostgresCheckSignatureHasStableEncoding(t *testing.T) {
	t.Parallel()
	got := mustPlannedPostgresCheckSignature(
		t,
		`balance >= 0`,
		postgresCheckSignatureColumns(),
	)
	const want = PostgresCheckSignature(
		`dmtx:postgres-check:v1:compare(">=",` +
			`column(numeric,"balance"),number("0"))`,
	)
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}

func TestPostgresCheckSignaturesDetectLogicalDrift(t *testing.T) {
	t.Parallel()
	columns := postgresCheckSignatureColumns()
	planned := mustPlannedPostgresCheckSignature(
		t,
		`amount >= 0 AND amount <= 10`,
		columns,
	)
	catalogExpressions := []struct {
		name       string
		expression string
	}{
		{
			name: "changed operator",
			expression: `amount > 0::numeric AND ` +
				`amount <= 10::numeric`,
		},
		{
			name: "changed literal",
			expression: `amount >= 1::numeric AND ` +
				`amount <= 10::numeric`,
		},
		{
			name: "changed predicate order",
			expression: `amount <= 10::numeric AND ` +
				`amount >= 0::numeric`,
		},
		{
			name: "changed boolean grouping",
			expression: `amount >= 0::numeric OR ` +
				`amount <= 10::numeric`,
		},
	}
	for _, test := range catalogExpressions {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			actual, err := ParsePostgresCheckSignature(
				test.expression,
				columns,
			)
			if err != nil {
				t.Fatalf("parse changed catalog CHECK: %v", err)
			}
			if actual == planned {
				t.Fatalf("logical drift retained signature %q", actual)
			}
		})
	}

	plannedIN := mustPlannedPostgresCheckSignature(
		t,
		`status IN ('active', 'paused')`,
		columns,
	)
	for _, expression := range []string{
		`(status COLLATE "C") = ANY (` +
			`ARRAY['paused'::text, 'active'::text])`,
		`(status COLLATE "C") = ANY (` +
			`ARRAY['active'::text, 'blocked'::text])`,
	} {
		actual, err := ParsePostgresCheckSignature(expression, columns)
		if err != nil {
			t.Fatal(err)
		}
		if actual == plannedIN {
			t.Fatalf("changed ANY list retained signature: %s", expression)
		}
	}
}

func TestParsePostgresCheckSignatureFailsClosed(t *testing.T) {
	t.Parallel()
	columns := postgresCheckSignatureColumns()
	tests := []struct {
		name       string
		expression string
	}{
		{
			name:       "text comparison without C",
			expression: `status = 'active'::text`,
		},
		{
			name:       "different collation",
			expression: `(status COLLATE "POSIX") = 'active'::text`,
		},
		{
			name:       "unquoted c collation",
			expression: `(status COLLATE c) = 'active'::text`,
		},
		{
			name:       "non catalog C namespace",
			expression: `(status COLLATE custom."C") = 'active'::text`,
		},
		{
			name:       "collation on numeric",
			expression: `(amount COLLATE "C") >= 0::numeric`,
		},
		{
			name:       "function",
			expression: `lower(status) = 'active'::text`,
		},
		{
			name:       "unsupported cast type",
			expression: `amount >= 0::money`,
		},
		{
			name:       "incompatible literal cast",
			expression: `amount >= '0'::text`,
		},
		{
			name:       "column cast",
			expression: `amount::numeric >= 0::numeric`,
		},
		{
			name: "plain text column relabel",
			expression: `((status)::text COLLATE "C") = ` +
				`'active'::text`,
		},
		{
			name: "varchar column has wrong relabel",
			expression: `((short_status)::character varying ` +
				`COLLATE "C") = 'active'::text`,
		},
		{
			name:       "quoted cast type",
			expression: `amount >= 0::"numeric"`,
		},
		{
			name:       "ANY without ARRAY",
			expression: `amount = ANY (1::numeric)`,
		},
		{
			name:       "empty ANY ARRAY",
			expression: `amount = ANY (ARRAY[])`,
		},
		{
			name:       "ANY column member",
			expression: `amount = ANY (ARRAY[amount])`,
		},
		{
			name: "ANY incompatible NULL cast",
			expression: `amount = ANY (` +
				`ARRAY[0::numeric, NULL::text])`,
		},
		{
			name: "text ANY without C",
			expression: `status = ANY (` +
				`ARRAY['active'::text])`,
		},
		{
			name: "varchar ANY without array relabel",
			expression: `((short_status)::text COLLATE "C") = ANY (` +
				`ARRAY['active'::character varying])`,
		},
		{
			name: "plain text ANY with array relabel",
			expression: `(status COLLATE "C") = ANY (` +
				`(ARRAY['active'::text])::text[])`,
		},
		{
			name:       "unknown column",
			expression: `missing >= 0::numeric`,
		},
		{
			name:       "wrong quoted identifier case",
			expression: `"BALANCE" >= 0::numeric`,
		},
		{
			name:       "qualified column",
			expression: `public.balance >= 0::numeric`,
		},
		{
			name:       "LIKE",
			expression: `(status COLLATE "C") LIKE 'a%'::text`,
		},
		{
			name:       "statement boundary",
			expression: `amount >= 0::numeric; SELECT true`,
		},
		{
			name:       "line comment",
			expression: `amount >= 0::numeric -- comment`,
		},
		{
			name:       "block comment",
			expression: `amount >= 0::numeric /* comment */`,
		},
		{
			name:       "opaque equality",
			expression: `document = NULL`,
		},
		{
			name:       "cross family comparison",
			expression: `(status COLLATE "C") = 1::numeric`,
		},
		{
			name:       "nonboolean root",
			expression: `amount`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParsePostgresCheckSignature(
				test.expression,
				columns,
			)
			if err == nil {
				t.Fatal("unsupported catalog CHECK was accepted")
			}
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error type = %T, want *PolicyError: %v", err, err)
			}
		})
	}
}

func TestPostgresCheckSignatureRejectsAmbiguousColumns(t *testing.T) {
	t.Parallel()
	expression, err := ParseSQLiteCheckExpression(`value >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	columns := []Column{
		{Name: "value", Type: "numeric"},
		{Name: "VALUE", Type: "numeric"},
	}
	if _, err := PlannedPostgresCheckSignature(
		expression,
		columns,
	); err == nil {
		t.Fatal("ambiguous planned columns were accepted")
	}
	if _, err := ParsePostgresCheckSignature(
		`value >= 0::numeric`,
		columns,
	); err == nil {
		t.Fatal("ambiguous catalog columns were accepted")
	}
}

func postgresCheckSignatureColumns() []Column {
	return []Column{
		{Name: "balance", Type: "numeric"},
		{Name: "status", Type: "text"},
		{
			Name: "short_status",
			Type: "varchar",
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{10},
			},
		},
		{
			Name: "fixed_status",
			Type: "char",
			DeclaredType: &DeclaredType{
				Base:      "char",
				Arguments: []int{10},
			},
		},
		{Name: "amount", Type: "numeric"},
		{Name: "enabled", Type: "boolean"},
		{Name: "archived", Type: "bool"},
		{Name: "deleted_at", Type: "datetime"},
		{Name: "document", Type: "json"},
		{Name: "Order Total", Type: "numeric"},
		{Name: `quote"name`, Type: "text"},
	}
}

func mustPlannedPostgresCheckSignature(
	t *testing.T,
	source string,
	columns []Column,
) PostgresCheckSignature {
	t.Helper()
	expression, err := ParseSQLiteCheckExpression(source)
	if err != nil {
		t.Fatalf("parse source CHECK: %v", err)
	}
	signature, err := PlannedPostgresCheckSignature(
		expression,
		columns,
	)
	if err != nil {
		t.Fatalf("sign planned CHECK: %v", err)
	}
	return signature
}
