package schema

import (
	"strings"
	"testing"
)

func TestParsePostgresCatalogDefaultReconstructsSafeExpression(t *testing.T) {
	tests := []struct {
		name      string
		column    Column
		catalog   string
		canonical string
	}{
		{
			name: "numeric",
			column: Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
			},
			catalog:   `'1.50'::numeric`,
			canonical: "1.5",
		},
		{
			name:      "text",
			column:    Column{Name: "status", Type: "text"},
			catalog:   `'active'::text`,
			canonical: `E'active'`,
		},
		{
			name:      "bytea",
			column:    Column{Name: "payload", Type: "bytea"},
			catalog:   `decode('00ff'::text, 'hex'::text)`,
			canonical: `X'00ff'`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParsePostgresCatalogDefault(
				test.column,
				&test.catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if expression == nil ||
				expression.CanonicalSQL() != test.canonical {
				t.Fatalf(
					"canonical default = %v, want %q",
					expression,
					test.canonical,
				)
			}
			if strings.Contains(
				expression.CanonicalSQL(),
				"::",
			) || strings.Contains(
				expression.CanonicalSQL(),
				"decode",
			) {
				t.Fatalf(
					"catalog SQL was retained: %q",
					expression.CanonicalSQL(),
				)
			}
		})
	}
	if expression, err := ParsePostgresCatalogDefault(
		Column{Name: "value", Type: "integer"},
		nil,
	); err != nil || expression != nil {
		t.Fatalf("absent default = %#v, error = %v", expression, err)
	}
}

func TestParsePostgresCatalogCheckReconstructsPortableExpression(t *testing.T) {
	columns := []Column{{
		Name: "status",
		Type: "varchar",
		DeclaredType: &DeclaredType{
			Base:      "varchar",
			Arguments: []int{16},
		},
	}}
	catalog := `((status)::text COLLATE "C") = 'active'::text`
	expression, err := ParsePostgresCatalogCheck(catalog, columns)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(expression.CanonicalSQL(), "COLLATE") ||
		strings.Contains(expression.CanonicalSQL(), "::") ||
		expression.CanonicalSQL() != `"status" = 'active'` {
		t.Fatalf(
			"catalog CHECK was not safely reconstructed: %q",
			expression.CanonicalSQL(),
		)
	}
	rendered, err := RenderSQLiteCheckForPostgres(expression, columns)
	if err != nil {
		t.Fatal(err)
	}
	if rendered !=
		`"status" COLLATE "pg_catalog"."C" = E'active'` {
		t.Fatalf("re-rendered CHECK = %q", rendered)
	}
}
