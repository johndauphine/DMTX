package schema

import (
	"strings"
	"testing"
)

func TestParseMySQLCatalogDefaultReconstructsSafeScalars(t *testing.T) {
	tests := []struct {
		name             string
		column           Column
		catalog          string
		defaultGenerated bool
		wantSQL          string
		wantKind         expressionKind
		wantLiteral      string
	}{
		{
			name:        "null",
			column:      Column{Name: "attempts", Type: "integer"},
			catalog:     "NULL",
			wantSQL:     "NULL",
			wantKind:    expressionNull,
			wantLiteral: "",
		},
		{
			name: "boolean one",
			column: Column{
				Name: "enabled",
				Type: "boolean",
				DeclaredType: &DeclaredType{
					Base:      "tinyint",
					Arguments: []int{1},
				},
			},
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
			name:        "string payload is re-quoted",
			column:      Column{Name: "label", Type: "varchar"},
			catalog:     "O'Brien\\draft'); DROP TABLE audit; --",
			wantSQL:     "'O''Brien\\draft''); DROP TABLE audit; --'",
			wantKind:    expressionString,
			wantLiteral: "O'Brien\\draft'); DROP TABLE audit; --",
		},
		{
			name: "generated longtext literal",
			column: Column{
				Name: "note",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base: "longtext",
				},
			},
			catalog:          `_utf8mb4\'O\\\'Brien\'`,
			defaultGenerated: true,
			wantSQL:          "'O''Brien'",
			wantKind:         expressionString,
			wantLiteral:      "O'Brien",
		},
		{
			name:             "current time",
			column:           Column{Name: "occurred_at", Type: "time"},
			catalog:          "curtime()",
			defaultGenerated: true,
			wantSQL:          "CURRENT_TIME",
			wantKind:         expressionCurrentTime,
		},
		{
			name:             "current date",
			column:           Column{Name: "occurred_on", Type: "date"},
			catalog:          "curdate()",
			defaultGenerated: true,
			wantSQL:          "CURRENT_DATE",
			wantKind:         expressionCurrentDate,
		},
		{
			name:             "current timestamp",
			column:           Column{Name: "created_at", Type: "datetime"},
			catalog:          "(current_timestamp())",
			defaultGenerated: true,
			wantSQL:          "CURRENT_TIMESTAMP",
			wantKind:         expressionCurrentTimestamp,
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
			catalog:          "CURRENT_TIMESTAMP(3)",
			defaultGenerated: true,
			wantSQL:          "CURRENT_TIMESTAMP",
			wantKind:         expressionCurrentTimestamp,
		},
		{
			name: "generated blob hexadecimal",
			column: Column{
				Name: "payload",
				Type: "blob",
				DeclaredType: &DeclaredType{
					Base: "longblob",
				},
			},
			catalog:          "0x00FF",
			defaultGenerated: true,
			wantSQL:          "X'00ff'",
			wantKind:         expressionBlob,
			wantLiteral:      "00ff",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseMySQLCatalogDefault(
				test.column,
				&test.catalog,
				test.defaultGenerated,
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

	if expression, err := ParseMySQLCatalogDefault(
		Column{Name: "optional", Type: "text"},
		nil,
		false,
	); err != nil || expression != nil {
		t.Fatalf("absent default = %#v, error = %v", expression, err)
	}
}

func TestParseMySQLCatalogDefaultStringNULLRemainsLiteral(t *testing.T) {
	catalog := "NULL"
	column := Column{Name: "label", Type: "text"}
	expression, err := ParseMySQLCatalogDefault(column, &catalog, false)
	if err != nil {
		t.Fatal(err)
	}
	if expression.kind != expressionString ||
		expression.literal != "NULL" ||
		expression.CanonicalSQL() != "'NULL'" {
		t.Fatalf("string NULL default = %#v", expression)
	}

	column.Default = expression
	rendered, err := renderPostgresDefault(column)
	if err != nil {
		t.Fatal(err)
	}
	if rendered != "E'NULL'" {
		t.Fatalf("PostgreSQL default = %q", rendered)
	}
}

func TestParseMySQLCatalogDefaultFailsClosed(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name             string
		column           Column
		catalog          *string
		defaultGenerated bool
	}{
		{
			name:             "generated string function",
			column:           Column{Name: "label", Type: "text"},
			catalog:          mysqlCatalogTestString("(concat('x', 'y'))"),
			defaultGenerated: true,
		},
		{
			name:             "generated numeric expression",
			column:           Column{Name: "attempts", Type: "integer"},
			catalog:          mysqlCatalogTestString("(1 + 1)"),
			defaultGenerated: true,
		},
		{
			name:             "generated absent value",
			column:           Column{Name: "attempts", Type: "integer"},
			defaultGenerated: true,
		},
		{
			name:             "wrong current function for type",
			column:           Column{Name: "occurred_on", Type: "date"},
			catalog:          mysqlCatalogTestString("CURRENT_TIMESTAMP"),
			defaultGenerated: true,
		},
		{
			name:             "current timestamp precision cannot be retained",
			column:           Column{Name: "created_at", Type: "timestamp"},
			catalog:          mysqlCatalogTestString("CURRENT_TIMESTAMP(6)"),
			defaultGenerated: true,
		},
		{
			name: "current timestamp precision mismatch",
			column: Column{
				Name: "created_at",
				Type: "datetime",
				DeclaredType: &DeclaredType{
					Base:      "datetime",
					Arguments: []int{3},
				},
			},
			catalog:          mysqlCatalogTestString("CURRENT_TIMESTAMP(6)"),
			defaultGenerated: true,
		},
		{
			name: "blob default must be generated",
			column: Column{
				Name: "payload",
				Type: "blob",
				DeclaredType: &DeclaredType{
					Base: "longblob",
				},
			},
			catalog: mysqlCatalogTestString("0x00ff"),
		},
		{
			name: "blob default must be even hexadecimal",
			column: Column{
				Name: "payload",
				Type: "blob",
				DeclaredType: &DeclaredType{
					Base: "longblob",
				},
			},
			catalog:          mysqlCatalogTestString("0x0fg"),
			defaultGenerated: true,
		},
		{
			name:             "function alias",
			column:           Column{Name: "created_at", Type: "timestamp"},
			catalog:          mysqlCatalogTestString("NOW()"),
			defaultGenerated: true,
		},
		{
			name:    "integer expression",
			column:  Column{Name: "attempts", Type: "integer"},
			catalog: mysqlCatalogTestString("1 + 1"),
		},
		{
			name:    "decimal exponent",
			column:  Column{Name: "amount", Type: "decimal"},
			catalog: mysqlCatalogTestString("1e2"),
		},
		{
			name:    "invalid boolean",
			column:  Column{Name: "enabled", Type: "boolean"},
			catalog: mysqlCatalogTestString("2"),
		},
		{
			name:    "static temporal default lacks a structural kind",
			column:  Column{Name: "occurred_on", Type: "date"},
			catalog: mysqlCatalogTestString("2026-07-29"),
		},
		{
			name:    "unsupported type",
			column:  Column{Name: "payload", Type: "json"},
			catalog: mysqlCatalogTestString("{}"),
		},
		{
			name:    "NUL string",
			column:  Column{Name: "label", Type: "text"},
			catalog: mysqlCatalogTestString("a\x00b"),
		},
		{
			name:    "invalid UTF-8 string",
			column:  Column{Name: "label", Type: "text"},
			catalog: &invalidUTF8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if expression, err := ParseMySQLCatalogDefault(
				test.column,
				test.catalog,
				test.defaultGenerated,
			); err == nil {
				t.Fatalf("unsafe default accepted as %#v", expression)
			}
		})
	}
}

func TestParseMySQLCatalogCheckCanonicalizesBackticks(t *testing.T) {
	columns := []Column{
		{Name: "tenant_id", Type: "integer"},
		{
			Name: "status",
			Type: "varchar",
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{16},
			},
		},
		{
			Name: "archived",
			Type: "boolean",
			DeclaredType: &DeclaredType{
				Base:      "tinyint",
				Arguments: []int{1},
			},
		},
	}
	catalog := "((`tenant_id` >= 0001) AND (`status` IN " +
		"(_utf8mb4\\'active\\', _utf8mb4\\'paused\\'))) " +
		"OR (`archived` = false)"
	expression, err := ParseMySQLCatalogCheck(catalog, columns)
	if err != nil {
		t.Fatal(err)
	}
	const want = `"tenant_id" >= 1 AND "status" IN (` +
		`'active', 'paused') OR "archived" = FALSE`
	if expression.CanonicalSQL() != want {
		t.Fatalf(
			"canonical CHECK = %q, want %q",
			expression.CanonicalSQL(),
			want,
		)
	}
	if strings.Contains(expression.CanonicalSQL(), "`") ||
		strings.Contains(expression.CanonicalSQL(), "0001") {
		t.Fatalf("catalog text was retained: %q", expression.CanonicalSQL())
	}

	rendered, err := RenderSQLiteCheckForPostgres(
		expression,
		mysqlPortableCheckColumns(columns),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		rendered,
		`"status" COLLATE "pg_catalog"."C"`,
	) {
		t.Fatalf("re-rendered CHECK lacks binary collation: %q", rendered)
	}
}

func TestParseMySQLCatalogCheckResolvesEscapedBacktickIdentifier(t *testing.T) {
	expression, err := ParseMySQLCatalogCheck(
		"`odd``name` <> 'unsafe''text'",
		[]Column{{Name: "odd`name", Type: "text"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if expression.CanonicalSQL() != `"odd`+"`"+`name" <> 'unsafe''text'` {
		t.Fatalf("canonical CHECK = %q", expression.CanonicalSQL())
	}
}

func TestParseMySQLCatalogCheckDecodesCatalogEscapedString(t *testing.T) {
	catalog := "`label` <> _utf8mb4\\'O" +
		strings.Repeat("\\", 3) + "'Brien\\'"
	expression, err := ParseMySQLCatalogCheck(
		catalog,
		[]Column{{Name: "label", Type: "text"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if expression.CanonicalSQL() != `"label" <> 'O''Brien'` {
		t.Fatalf("canonical CHECK = %q", expression.CanonicalSQL())
	}
	if strings.Contains(expression.CanonicalSQL(), "_utf8mb4") ||
		strings.Contains(expression.CanonicalSQL(), "\\") {
		t.Fatalf("catalog encoding was retained: %q", expression.CanonicalSQL())
	}
}

func TestParseMySQLCatalogCheckFailsClosed(t *testing.T) {
	columns := []Column{
		{Name: "tenant_id", Type: "integer"},
		{Name: "status", Type: "text"},
	}
	tests := []string{
		"",
		"`missing` = 1",
		"`tenant_id`",
		"`status` = 1",
		"LENGTH(`status`) > 0",
		"`tenant_id` <=> 1",
		"`tenant_id` = 1; DROP TABLE audit",
		"`tenant_id` = ?",
		"`tenant_id` = 1e2",
		"`status` = 'unterminated",
		"`status` = _ucs2\\'active\\'",
		"`status` = _utf8mb4\\'bad\\\\q\\'",
	}
	for _, catalog := range tests {
		t.Run(catalog, func(t *testing.T) {
			if expression, err := ParseMySQLCatalogCheck(
				catalog,
				columns,
			); err == nil {
				t.Fatalf("unsafe CHECK accepted as %q", expression.CanonicalSQL())
			}
		})
	}
}

func mysqlCatalogTestString(value string) *string {
	return &value
}
