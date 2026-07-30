package schema

import (
	"errors"
	"strings"
	"testing"
)

func TestParseMariaDBCatalogDefaultReconstructsSafeScalars(t *testing.T) {
	tests := []struct {
		name        string
		column      Column
		catalog     string
		wantSQL     string
		wantKind    expressionKind
		wantLiteral string
	}{
		{
			name:        "quoted string",
			column:      Column{Name: "label", Type: "varchar"},
			catalog:     `'O''Brien\\draft\nnext'`,
			wantSQL:     "'O''Brien\\draft\nnext'",
			wantKind:    expressionString,
			wantLiteral: "O'Brien\\draft\nnext",
		},
		{
			name:        "quoted NULL remains a string",
			column:      Column{Name: "label", Type: "text"},
			catalog:     `'NULL'`,
			wantSQL:     "'NULL'",
			wantKind:    expressionString,
			wantLiteral: "NULL",
		},
		{
			name:        "boolean true",
			column:      Column{Name: "enabled", Type: "boolean"},
			catalog:     "1",
			wantSQL:     "TRUE",
			wantKind:    expressionBoolean,
			wantLiteral: "TRUE",
		},
		{
			name:        "boolean false",
			column:      Column{Name: "enabled", Type: "bool"},
			catalog:     "false",
			wantSQL:     "FALSE",
			wantKind:    expressionBoolean,
			wantLiteral: "FALSE",
		},
		{
			name:        "integer",
			column:      Column{Name: "attempts", Type: "integer"},
			catalog:     "+00042",
			wantSQL:     "42",
			wantKind:    expressionNumber,
			wantLiteral: "42",
		},
		{
			name: "numeric",
			column: Column{
				Name: "amount",
				Type: "decimal",
				DeclaredType: &DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 4},
				},
			},
			catalog:     ".5000",
			wantSQL:     "0.5",
			wantKind:    expressionNumber,
			wantLiteral: "0.5",
		},
		{
			name:        "floating point negative zero",
			column:      Column{Name: "ratio", Type: "double"},
			catalog:     "-0e2",
			wantSQL:     "0",
			wantKind:    expressionNumber,
			wantLiteral: "0",
		},
		{
			name: "BLOB hex literal",
			column: Column{
				Name:         "payload",
				Type:         "blob",
				DeclaredType: &DeclaredType{Base: "longblob"},
			},
			catalog:     "X'00fF'",
			wantSQL:     "X'00ff'",
			wantKind:    expressionBlob,
			wantLiteral: "00ff",
		},
		{
			name:     "current date function",
			column:   Column{Name: "occurred_on", Type: "date"},
			catalog:  "curdate()",
			wantSQL:  "CURRENT_DATE",
			wantKind: expressionCurrentDate,
		},
		{
			name:     "current timestamp function",
			column:   Column{Name: "created_at", Type: "timestamp"},
			catalog:  "(current_timestamp())",
			wantSQL:  "CURRENT_TIMESTAMP",
			wantKind: expressionCurrentTimestamp,
		},
		{
			name: "current timestamp exact precision",
			column: Column{
				Name: "created_at",
				Type: "datetime",
				DeclaredType: &DeclaredType{
					Base:      "datetime",
					Arguments: []int{3},
				},
			},
			catalog:  "CURRENT_TIMESTAMP(3)",
			wantSQL:  "CURRENT_TIMESTAMP",
			wantKind: expressionCurrentTimestamp,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseMariaDBCatalogDefault(
				test.column,
				&test.catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if expression == nil {
				t.Fatal("default unexpectedly absent")
			}
			if expression.CanonicalSQL() != test.wantSQL ||
				expression.kind != test.wantKind ||
				expression.literal != test.wantLiteral {
				t.Fatalf(
					"default = %#v, want SQL %q, kind %d, literal %q",
					expression,
					test.wantSQL,
					test.wantKind,
					test.wantLiteral,
				)
			}
		})
	}
}

func TestParseMariaDBCatalogDefaultNULLShapesAreAbsent(t *testing.T) {
	for _, catalog := range []*string{nil, mariaDBCatalogTestString("NULL")} {
		expression, err := ParseMariaDBCatalogDefault(
			Column{Name: "optional", Type: "text", Nullable: true},
			catalog,
		)
		if err != nil || expression != nil {
			t.Fatalf("absent default = %#v, error = %v", expression, err)
		}
	}
}

func TestParseMariaDBCatalogDefaultFailsClosed(t *testing.T) {
	invalidUTF8 := "'" + string([]byte{0xff}) + "'"
	tests := []struct {
		name    string
		column  Column
		catalog string
	}{
		{
			name:    "unquoted string",
			column:  Column{Name: "label", Type: "text"},
			catalog: "payload",
		},
		{
			name:    "unterminated string",
			column:  Column{Name: "label", Type: "text"},
			catalog: "'payload",
		},
		{
			name:    "trailing string expression",
			column:  Column{Name: "label", Type: "text"},
			catalog: "'a' || 'b'",
		},
		{
			name:    "NUL string",
			column:  Column{Name: "label", Type: "text"},
			catalog: "'a\x00b'",
		},
		{
			name:    "invalid UTF-8 string",
			column:  Column{Name: "label", Type: "text"},
			catalog: invalidUTF8,
		},
		{
			name:    "NULL text on required column",
			column:  Column{Name: "required", Type: "integer"},
			catalog: "NULL",
		},
		{
			name:    "quoted integer",
			column:  Column{Name: "attempts", Type: "integer"},
			catalog: "'42'",
		},
		{
			name:    "integer expression",
			column:  Column{Name: "attempts", Type: "integer"},
			catalog: "1 + 1",
		},
		{
			name:    "decimal exponent",
			column:  Column{Name: "amount", Type: "decimal"},
			catalog: "1e2",
		},
		{
			name:    "invalid boolean",
			column:  Column{Name: "enabled", Type: "boolean"},
			catalog: "2",
		},
		{
			name:    "wrong temporal function",
			column:  Column{Name: "occurred_on", Type: "date"},
			catalog: "current_timestamp()",
		},
		{
			name: "temporal precision mismatch",
			column: Column{
				Name: "created_at",
				Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
			catalog: "current_timestamp(6)",
		},
		{
			name:    "arbitrary function",
			column:  Column{Name: "created_at", Type: "timestamp"},
			catalog: "now()",
		},
		{
			name: "lossy VARBINARY catalog literal",
			column: Column{
				Name: "payload",
				Type: "blob",
				DeclaredType: &DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
				},
			},
			catalog: `'\\0?'`,
		},
		{
			name: "malformed BLOB hex literal",
			column: Column{
				Name:         "payload",
				Type:         "blob",
				DeclaredType: &DeclaredType{Base: "longblob"},
			},
			catalog: "X'0xz1'",
		},
		{
			name:    "unsupported type",
			column:  Column{Name: "payload", Type: "json"},
			catalog: "'{}'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseMariaDBCatalogDefault(
				test.column,
				&test.catalog,
			)
			if err == nil {
				t.Fatalf("unsafe default accepted as %#v", expression)
			}
			var policy *PolicyError
			if !errors.As(err, &policy) ||
				policy.Operation != "parse MariaDB catalog default" ||
				policy.Type != test.column.Name ||
				policy.Target != string(MySQL) {
				t.Fatalf("error = %#v, want MariaDB catalog policy", err)
			}
			if !strings.Contains(err.Error(), "MariaDB") {
				t.Fatalf("error = %q, want MariaDB context", err)
			}
		})
	}
}

func TestParseMariaDBCatalogDefaultDoesNotChangeOracleSemantics(t *testing.T) {
	column := Column{Name: "label", Type: "text"}
	oracleCatalog := "unquoted payload"
	oracle, err := ParseMySQLCatalogDefault(
		column,
		&oracleCatalog,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if oracle.literal != oracleCatalog {
		t.Fatalf("Oracle MySQL default = %#v", oracle)
	}

	mariaCatalog := "'unquoted payload'"
	maria, err := ParseMariaDBCatalogDefault(column, &mariaCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if maria.literal != oracleCatalog {
		t.Fatalf("MariaDB default = %#v", maria)
	}
}

func mariaDBCatalogTestString(value string) *string {
	return &value
}
