package schema

import (
	"strings"
	"testing"
)

func TestParseSQLServerCatalogDefaultReconstructsTypedScalars(
	t *testing.T,
) {
	tests := []struct {
		name        string
		column      Column
		definition  string
		wantSQL     string
		wantKind    expressionKind
		wantLiteral string
	}{
		{
			name:        "Unicode string",
			column:      sqlServerCatalogTestColumn("status", "nvarchar", 16),
			definition:  `(N'O''Brien')`,
			wantSQL:     `'O''Brien'`,
			wantKind:    expressionString,
			wantLiteral: "O'Brien",
		},
		{
			name:        "ordinary string with nested parentheses",
			column:      sqlServerCatalogTestColumn("code", "varchar", 8),
			definition:  `((('A(B)')))`,
			wantSQL:     `'A(B)'`,
			wantKind:    expressionString,
			wantLiteral: "A(B)",
		},
		{
			name:        "fixed UTF-8 string is padded by source bytes",
			column:      sqlServerCatalogTestColumn("code", "char", 4),
			definition:  `('é')`,
			wantSQL:     `'é  '`,
			wantKind:    expressionString,
			wantLiteral: "é  ",
		},
		{
			name:        "fixed national string is padded by UTF-16 units",
			column:      sqlServerCatalogTestColumn("code", "nchar", 4),
			definition:  `(N'😀')`,
			wantSQL:     `'😀  '`,
			wantKind:    expressionString,
			wantLiteral: "😀  ",
		},
		{
			name:        "string payload is re-quoted",
			column:      sqlServerCatalogTestColumn("note", "nvarchar", 80),
			definition:  `(N'x''); DROP TABLE audit;--')`,
			wantSQL:     `'x''); DROP TABLE audit;--'`,
			wantKind:    expressionString,
			wantLiteral: "x'); DROP TABLE audit;--",
		},
		{
			name:        "integer",
			column:      sqlServerCatalogTestColumn("attempts", "int"),
			definition:  `((+00042))`,
			wantSQL:     `42`,
			wantKind:    expressionNumber,
			wantLiteral: "42",
		},
		{
			name:        "tinyint upper bound",
			column:      sqlServerCatalogTestColumn("priority", "tinyint"),
			definition:  `((255))`,
			wantSQL:     `255`,
			wantKind:    expressionNumber,
			wantLiteral: "255",
		},
		{
			name:        "numeric",
			column:      sqlServerCatalogTestColumn("amount", "decimal", 12, 4),
			definition:  `(((001.5000)))`,
			wantSQL:     `1.5`,
			wantKind:    expressionNumber,
			wantLiteral: "1.5",
		},
		{
			name:        "floating point",
			column:      sqlServerCatalogTestColumn("ratio", "double precision"),
			definition:  `((001.25))`,
			wantSQL:     `1.25`,
			wantKind:    expressionNumber,
			wantLiteral: "1.25",
		},
		{
			name:        "bit true",
			column:      sqlServerCatalogTestColumn("enabled", "bit"),
			definition:  `((1))`,
			wantSQL:     `TRUE`,
			wantKind:    expressionBoolean,
			wantLiteral: "TRUE",
		},
		{
			name:        "binary",
			column:      sqlServerCatalogTestColumn("digest", "binary", 2),
			definition:  `((0x00Af))`,
			wantSQL:     `X'00af'`,
			wantKind:    expressionBlob,
			wantLiteral: "00af",
		},
		{
			name:       "explicit null",
			column:     sqlServerCatalogTestColumn("status", "nvarchar", 16),
			definition: `((NULL))`,
			wantSQL:    `NULL`,
			wantKind:   expressionNull,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseSQLServerCatalogDefault(
				test.column,
				&test.definition,
			)
			if err != nil {
				t.Fatal(err)
			}
			if expression == nil ||
				expression.CanonicalSQL() != test.wantSQL ||
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
			if strings.Contains(
				expression.CanonicalSQL(),
				"GETUTCDATE",
			) || strings.Contains(
				expression.CanonicalSQL(),
				"SYSUTCDATETIME",
			) {
				t.Fatalf(
					"catalog SQL was retained: %q",
					expression.CanonicalSQL(),
				)
			}
		})
	}

	if expression, err := ParseSQLServerCatalogDefault(
		sqlServerCatalogTestColumn("optional", "int"),
		nil,
	); err != nil || expression != nil {
		t.Fatalf("absent default = %#v, error = %v", expression, err)
	}
}

func TestParseSQLServerCatalogDefaultFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		column     Column
		definition string
	}{
		{
			name:       "function",
			column:     sqlServerCatalogTestColumn("id", "nvarchar", 36),
			definition: `(newid())`,
		},
		{
			name:       "arithmetic",
			column:     sqlServerCatalogTestColumn("attempts", "int"),
			definition: `((1)+(1))`,
		},
		{
			name:       "sequence",
			column:     sqlServerCatalogTestColumn("id", "bigint"),
			definition: `(NEXT VALUE FOR [dbo].[ids])`,
		},
		{
			name:       "cast",
			column:     sqlServerCatalogTestColumn("attempts", "int"),
			definition: `(CONVERT([int],(1)))`,
		},
		{
			name:       "local timestamp",
			column:     sqlServerCatalogTestColumn("created_at", "datetime2", 0),
			definition: `(getdate())`,
		},
		{
			name:       "ANSI local timestamp",
			column:     sqlServerCatalogTestColumn("created_at", "datetime2", 0),
			definition: `(CURRENT_TIMESTAMP)`,
		},
		{
			name:       "UTC timestamp on date",
			column:     sqlServerCatalogTestColumn("created_on", "date"),
			definition: `(sysutcdatetime())`,
		},
		{
			name:       "UTC high precision timestamp",
			column:     sqlServerCatalogTestColumn("created_at", "datetime2", 3),
			definition: `((sysutcdatetime()))`,
		},
		{
			name:       "UTC legacy timestamp",
			column:     sqlServerCatalogTestColumn("created_at", "datetime"),
			definition: `( GETUTCDATE ( ) )`,
		},
		{
			name:       "string for integer",
			column:     sqlServerCatalogTestColumn("attempts", "int"),
			definition: `(N'1')`,
		},
		{
			name:       "integer expression for text",
			column:     sqlServerCatalogTestColumn("status", "nvarchar", 16),
			definition: `((1))`,
		},
		{
			name:       "UTF-8 varchar exceeds source byte length",
			column:     sqlServerCatalogTestColumn("status", "varchar", 3),
			definition: `('東京')`,
		},
		{
			name:       "nvarchar exceeds source UTF-16 length",
			column:     sqlServerCatalogTestColumn("status", "nvarchar", 1),
			definition: `(N'😀')`,
		},
		{
			name:       "bit outside domain",
			column:     sqlServerCatalogTestColumn("enabled", "bit"),
			definition: `((2))`,
		},
		{
			name:       "tinyint outside domain",
			column:     sqlServerCatalogTestColumn("priority", "tinyint"),
			definition: `((256))`,
		},
		{
			name:       "numeric exponent",
			column:     sqlServerCatalogTestColumn("amount", "decimal", 12, 4),
			definition: `((1e2))`,
		},
		{
			name:       "floating point exponent",
			column:     sqlServerCatalogTestColumn("ratio", "double precision"),
			definition: `((1e2))`,
		},
		{
			name:       "REAL default has float32 assignment semantics",
			column:     sqlServerCatalogTestColumn("ratio", "real"),
			definition: `((0.1))`,
		},
		{
			name:       "replacement rune may be a decoded surrogate",
			column:     sqlServerCatalogTestColumn("status", "varchar", 16),
			definition: "('\uFFFD')",
		},
		{
			name:       "numeric exceeds scale",
			column:     sqlServerCatalogTestColumn("amount", "decimal", 12, 4),
			definition: `((1.00001))`,
		},
		{
			name:       "odd binary",
			column:     sqlServerCatalogTestColumn("payload", "varbinary", 8),
			definition: `((0xabc))`,
		},
		{
			name:       "fixed binary length mismatch",
			column:     sqlServerCatalogTestColumn("payload", "binary", 2),
			definition: `((0xab))`,
		},
		{
			name:       "unbalanced parentheses",
			column:     sqlServerCatalogTestColumn("attempts", "int"),
			definition: `((1)`,
		},
		{
			name:       "unbalanced string",
			column:     sqlServerCatalogTestColumn("status", "nvarchar", 16),
			definition: `(N'active)`,
		},
		{
			name:       "trailing executable text",
			column:     sqlServerCatalogTestColumn("attempts", "int"),
			definition: `((1)); DROP TABLE audit`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseSQLServerCatalogDefault(
				test.column,
				&test.definition,
			)
			if err == nil {
				t.Fatal("unsafe catalog default was accepted")
			}
			if strings.Contains(err.Error(), test.definition) {
				t.Fatalf("catalog SQL leaked in error: %v", err)
			}
		})
	}
}

func TestParseSQLServerCatalogCheckReconstructsPortableExpression(
	t *testing.T,
) {
	columns := []Column{
		sqlServerCatalogTestColumn("amount", "decimal", 12, 4),
		sqlServerCatalogTestColumn("status", "nvarchar", 16),
		{
			Name:     "closing]bracket",
			Type:     "integer",
			Nullable: true,
			DeclaredType: &DeclaredType{
				Base: "int",
			},
		},
	}
	catalog := `(([amount]>=(0)) AND ([amount]<(1000.0000)))`
	expression, err := ParseSQLServerCatalogCheck(catalog, columns)
	if err != nil {
		t.Fatal(err)
	}
	want := `"amount" >= 0 AND "amount" < 1000.0000`
	if expression.CanonicalSQL() != want {
		t.Fatalf(
			"canonical CHECK = %q, want %q",
			expression.CanonicalSQL(),
			want,
		)
	}
	for _, catalogToken := range []string{"[", "]", "N'"} {
		if strings.Contains(expression.CanonicalSQL(), catalogToken) {
			t.Fatalf(
				"catalog CHECK text was retained: %q",
				expression.CanonicalSQL(),
			)
		}
	}

	escaped, err := ParseSQLServerCatalogCheck(
		`([closing]]bracket] IS NULL)`,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	if escaped.CanonicalSQL() != `"closing]bracket" IS NULL` {
		t.Fatalf(
			"escaped identifier CHECK = %q",
			escaped.CanonicalSQL(),
		)
	}

	rendered, err := RenderSQLiteCheckForPostgres(expression, columns)
	if err != nil {
		t.Fatal(err)
	}
	if rendered != `"amount" >= 0 AND "amount" < 1000.0000` {
		t.Fatalf("PostgreSQL CHECK = %q", rendered)
	}
}

func TestParseSQLServerCatalogCheckFailsClosed(t *testing.T) {
	columns := []Column{
		sqlServerCatalogTestColumn("amount", "decimal", 12, 4),
		sqlServerCatalogTestColumn("status", "nvarchar", 16),
		sqlServerCatalogTestColumn("ratio", "real"),
	}
	tests := []struct {
		name       string
		definition string
	}{
		{name: "function", definition: `(LEN([status])>(0))`},
		{
			name:       "text comparison has SQL Server blank-padding semantics",
			definition: `([status]=N'active')`,
		},
		{
			name:       "REAL comparison has float32 literal semantics",
			definition: `([ratio]<=(0.1))`,
		},
		{name: "arithmetic", definition: `([amount]+(1)>(0))`},
		{
			name:       "conversion",
			definition: `(CONVERT([decimal](12,4),[amount])>(0))`,
		},
		{
			name:       "collation",
			definition: `([status] COLLATE Latin1_General_100_BIN2=N'active')`,
		},
		{name: "unknown column", definition: `([missing]=(0))`},
		{name: "qualified column", definition: `([dbo].[amount]>(0))`},
		{name: "double quoted identifier", definition: `("amount">(0))`},
		{name: "malformed bracket", definition: `([amount>(0))`},
		{name: "unterminated string", definition: `([status]=N'active)`},
		{
			name:       "replacement rune may be a decoded surrogate",
			definition: "([\uFFFD] IS NULL)",
		},
		{name: "statement", definition: `([amount]>(0)); SELECT 1`},
		{name: "comment", definition: `([amount]>(0)) -- trusted`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseSQLServerCatalogCheck(
				test.definition,
				columns,
			)
			if err == nil {
				t.Fatal("unsafe catalog CHECK was accepted")
			}
			if strings.Contains(err.Error(), test.definition) {
				t.Fatalf("catalog SQL leaked in error: %v", err)
			}
		})
	}
}

func sqlServerCatalogTestColumn(
	name string,
	base string,
	arguments ...int,
) Column {
	column := Column{
		Name: name,
		Type: base,
		DeclaredType: &DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		},
	}
	return column
}
