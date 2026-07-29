package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresDefaultSignaturesRecognizePG16Deparse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		column     Column
		catalogSQL *string
		want       PostgresDefaultSignature
	}{
		{
			name:   "no default",
			column: Column{Name: "value", Type: "text"},
		},
		{
			name: "explicit null is absent pg_attrdef",
			column: postgresDefaultSignatureColumn(
				t,
				"value",
				"text",
				nil,
				"NULL",
			),
		},
		{
			name: "boolean",
			column: postgresDefaultSignatureColumn(
				t,
				"enabled",
				"boolean",
				nil,
				"TRUE",
			),
			catalogSQL: postgresDefaultCatalogSQL("true"),
			want:       postgresDefaultTestSignature(PostgresDefaultBoolean, "true"),
		},
		{
			name: "integer",
			column: postgresDefaultSignatureColumn(
				t,
				"offset",
				"integer",
				nil,
				"-7",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'-7'::integer`),
			want:       postgresDefaultTestSignature(PostgresDefaultInteger, "-7"),
		},
		{
			name: "bigint with integer constant",
			column: postgresDefaultSignatureColumn(
				t,
				"offset",
				"bigint",
				nil,
				"-7",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'-7'::integer`),
			want:       postgresDefaultTestSignature(PostgresDefaultBigint, "-7"),
		},
		{
			name: "bigint with bigint constant",
			column: postgresDefaultSignatureColumn(
				t,
				"offset",
				"bigint",
				nil,
				"9223372036854775807",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`'9223372036854775807'::bigint`,
			),
			want: postgresDefaultTestSignature(
				PostgresDefaultBigint,
				"9223372036854775807",
			),
		},
		{
			name: "double precision",
			column: postgresDefaultSignatureColumn(
				t,
				"ratio",
				"double",
				nil,
				"1.25",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`'1.25'::double precision`,
			),
			want: postgresDefaultTestSignature(
				PostgresDefaultDoublePrecision,
				"1.25",
			),
		},
		{
			name: "numeric",
			column: postgresDefaultSignatureColumn(
				t,
				"amount",
				"numeric",
				&DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
				"0.00",
			),
			catalogSQL: postgresDefaultCatalogSQL("0.00"),
			want:       postgresDefaultTestSignature(PostgresDefaultNumeric, "0"),
		},
		{
			name: "unmodified varchar maps to text",
			column: postgresDefaultSignatureColumn(
				t,
				"status",
				"varchar",
				nil,
				"'active'",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'active'::text`),
			want:       postgresDefaultTestSignature(PostgresDefaultText, "active"),
		},
		{
			name: "char modifier",
			column: postgresDefaultSignatureColumn(
				t,
				"code",
				"text",
				&DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
				"'AB'",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'AB'::bpchar`),
			want:       postgresDefaultTestSignature(PostgresDefaultChar, "AB"),
		},
		{
			name: "varchar modifier",
			column: postgresDefaultSignatureColumn(
				t,
				"label",
				"text",
				&DeclaredType{
					Base:      "varchar",
					Arguments: []int{40},
				},
				"'unknown'",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`'unknown'::character varying`,
			),
			want: postgresDefaultTestSignature(
				PostgresDefaultVarchar,
				"unknown",
			),
		},
		{
			name: "quoted text",
			column: postgresDefaultSignatureColumn(
				t,
				"owner",
				"text",
				nil,
				`'O''Brien'`,
			),
			catalogSQL: postgresDefaultCatalogSQL(`'O''Brien'::text`),
			want:       postgresDefaultTestSignature(PostgresDefaultText, "O'Brien"),
		},
		{
			name: "escape text",
			column: postgresDefaultSignatureColumn(
				t,
				"path",
				"text",
				nil,
				`'C:\temp'`,
			),
			catalogSQL: postgresDefaultCatalogSQL(`E'C:\\temp'::text`),
			want:       postgresDefaultTestSignature(PostgresDefaultText, `C:\temp`),
		},
		{
			name: "bytea",
			column: postgresDefaultSignatureColumn(
				t,
				"payload",
				"blob",
				nil,
				"X'00'",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`decode('00'::text, 'hex'::text)`,
			),
			want: postgresDefaultTestSignature(PostgresDefaultBytea, "00"),
		},
		{
			name: "current time",
			column: postgresDefaultSignatureColumn(
				t,
				"created_time",
				"time",
				nil,
				"CURRENT_TIME",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`date_trunc('second'::text, (statement_timestamp() AT TIME ZONE 'UTC'::text))::time without time zone`,
			),
			want: postgresDefaultTestSignature(PostgresDefaultCurrentTime, ""),
		},
		{
			name: "current date",
			column: postgresDefaultSignatureColumn(
				t,
				"created_day",
				"date",
				nil,
				"CURRENT_DATE",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`(statement_timestamp() AT TIME ZONE 'UTC'::text)::date`,
			),
			want: postgresDefaultTestSignature(PostgresDefaultCurrentDate, ""),
		},
		{
			name: "current timestamp",
			column: postgresDefaultSignatureColumn(
				t,
				"created_at",
				"timestamp",
				&DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
				"CURRENT_TIMESTAMP",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`date_trunc('second'::text, (statement_timestamp() AT TIME ZONE 'UTC'::text))`,
			),
			want: postgresDefaultTestSignature(
				PostgresDefaultCurrentTimestamp,
				"",
			),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			planned, err := PlannedPostgresDefaultSignature(test.column)
			if err != nil {
				t.Fatalf("planned signature: %v", err)
			}
			if planned != test.want {
				t.Fatalf("planned signature = %#v, want %#v", planned, test.want)
			}
			catalog, err := CatalogPostgresDefaultSignature(
				test.column,
				test.catalogSQL,
			)
			if err != nil {
				t.Fatalf("catalog signature: %v", err)
			}
			if catalog != test.want {
				t.Fatalf("catalog signature = %#v, want %#v", catalog, test.want)
			}
			matches, err := PostgresDefaultSignaturesMatch(
				test.column,
				test.catalogSQL,
			)
			if err != nil {
				t.Fatalf("match signatures: %v", err)
			}
			if !matches {
				t.Fatal("equivalent default signatures did not match")
			}
		})
	}
}

func TestPostgresDefaultSignaturesCoverRendererAliases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		aliases    []string
		defaultSQL string
		catalogSQL string
	}{
		{
			name:       "boolean",
			aliases:    []string{"bool", "boolean"},
			defaultSQL: "FALSE",
			catalogSQL: "false",
		},
		{
			name:       "integer",
			aliases:    []string{"int", "integer", "int4"},
			defaultSQL: "7",
			catalogSQL: `'7'::integer`,
		},
		{
			name:       "bigint",
			aliases:    []string{"bigint", "int8"},
			defaultSQL: "7",
			catalogSQL: `'7'::integer`,
		},
		{
			name: "double precision",
			aliases: []string{
				"real",
				"float",
				"float4",
				"double",
				"double precision",
				"float8",
			},
			defaultSQL: "1.5",
			catalogSQL: `'1.5'::double precision`,
		},
		{
			name:       "numeric",
			aliases:    []string{"numeric", "decimal"},
			defaultSQL: "1.50",
			catalogSQL: "1.50",
		},
		{
			name:       "text",
			aliases:    []string{"text", "varchar", "character varying"},
			defaultSQL: "'value'",
			catalogSQL: `'value'::text`,
		},
		{
			name: "bytea",
			aliases: []string{
				"blob",
				"binary",
				"varbinary",
				"bytea",
			},
			defaultSQL: "X'00FF'",
			catalogSQL: `decode('00ff'::text, 'hex'::text)`,
		},
		{
			name:       "timestamp",
			aliases:    []string{"timestamp", "datetime"},
			defaultSQL: "CURRENT_TIMESTAMP",
			catalogSQL: `date_trunc('second'::text, (statement_timestamp() AT TIME ZONE 'UTC'::text))`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, alias := range test.aliases {
				column := postgresDefaultSignatureColumn(
					t,
					"value",
					alias,
					nil,
					test.defaultSQL,
				)
				matches, err := PostgresDefaultSignaturesMatch(
					column,
					postgresDefaultCatalogSQL(test.catalogSQL),
				)
				if err != nil || !matches {
					t.Fatalf(
						"alias %q match = %v, error = %v",
						alias,
						matches,
						err,
					)
				}
			}
		})
	}
}

func TestPostgresDefaultSignaturesCoverDeclaredTypeAliases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		columnType   string
		declarations []DeclaredType
		defaultSQL   string
		catalogSQL   string
	}{
		{
			name:       "char",
			columnType: "text",
			declarations: []DeclaredType{
				{Base: "char", Arguments: []int{4}},
				{Base: "character", Arguments: []int{4}},
			},
			defaultSQL: "'AB'",
			catalogSQL: `'AB'::bpchar`,
		},
		{
			name:       "varchar",
			columnType: "text",
			declarations: []DeclaredType{
				{Base: "varchar", Arguments: []int{8}},
				{Base: "character varying", Arguments: []int{8}},
			},
			defaultSQL: "'value'",
			catalogSQL: `'value'::character varying`,
		},
		{
			name:       "numeric",
			columnType: "numeric",
			declarations: []DeclaredType{
				{Base: "numeric", Arguments: []int{12, 2}},
				{Base: "decimal", Arguments: []int{12, 2}},
			},
			defaultSQL: "0.00",
			catalogSQL: "0.00",
		},
		{
			name:       "timestamp",
			columnType: "timestamp",
			declarations: []DeclaredType{
				{Base: "timestamp", Arguments: []int{3}},
				{Base: "datetime", Arguments: []int{3}},
			},
			defaultSQL: "CURRENT_TIMESTAMP",
			catalogSQL: `date_trunc('second'::text, (statement_timestamp() AT TIME ZONE 'UTC'::text))`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, declaration := range test.declarations {
				declaration := declaration
				column := postgresDefaultSignatureColumn(
					t,
					"value",
					test.columnType,
					&declaration,
					test.defaultSQL,
				)
				matches, err := PostgresDefaultSignaturesMatch(
					column,
					postgresDefaultCatalogSQL(test.catalogSQL),
				)
				if err != nil || !matches {
					t.Fatalf(
						"declaration %#v match = %v, error = %v",
						declaration,
						matches,
						err,
					)
				}
			}
		})
	}
}

func TestPostgresDefaultSignaturesDetectSupportedDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		column     Column
		catalogSQL *string
	}{
		{
			name: "missing catalog default",
			column: postgresDefaultSignatureColumn(
				t, "amount", "numeric", nil, "0.00",
			),
		},
		{
			name: "unexpected catalog default",
			column: Column{
				Name: "status",
				Type: "text",
			},
			catalogSQL: postgresDefaultCatalogSQL(`'active'::text`),
		},
		{
			name: "numeric value",
			column: postgresDefaultSignatureColumn(
				t, "amount", "numeric", nil, "0.00",
			),
			catalogSQL: postgresDefaultCatalogSQL("0.01"),
		},
		{
			name: "text value",
			column: postgresDefaultSignatureColumn(
				t, "status", "text", nil, "'active'",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'paused'::text`),
		},
		{
			name: "boolean value",
			column: postgresDefaultSignatureColumn(
				t, "enabled", "boolean", nil, "TRUE",
			),
			catalogSQL: postgresDefaultCatalogSQL("false"),
		},
		{
			name: "integer value",
			column: postgresDefaultSignatureColumn(
				t, "offset", "bigint", nil, "-7",
			),
			catalogSQL: postgresDefaultCatalogSQL(`'-8'::integer`),
		},
		{
			name: "blob value",
			column: postgresDefaultSignatureColumn(
				t, "payload", "bytea", nil, "X'00'",
			),
			catalogSQL: postgresDefaultCatalogSQL(
				`decode('01'::text, 'hex'::text)`,
			),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			matches, err := PostgresDefaultSignaturesMatch(
				test.column,
				test.catalogSQL,
			)
			if err != nil {
				t.Fatalf("supported drift returned error: %v", err)
			}
			if matches {
				t.Fatal("different supported defaults matched")
			}
		})
	}
}

func TestPostgresCatalogDefaultSignatureFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		column     Column
		catalogSQL string
	}{
		{
			name:       "empty present default",
			column:     Column{Name: "value", Type: "text"},
			catalogSQL: "",
		},
		{
			name:       "explicit null catalog expression",
			column:     Column{Name: "value", Type: "text"},
			catalogSQL: "NULL::text",
		},
		{
			name:       "boolean capitalization",
			column:     Column{Name: "value", Type: "boolean"},
			catalogSQL: "TRUE",
		},
		{
			name:       "integer arithmetic",
			column:     Column{Name: "value", Type: "integer"},
			catalogSQL: "1+1",
		},
		{
			name:       "bigint invalid integer cast",
			column:     Column{Name: "value", Type: "bigint"},
			catalogSQL: `'2147483648'::integer`,
		},
		{
			name:       "double unsupported cast",
			column:     Column{Name: "value", Type: "double"},
			catalogSQL: "1.5::real",
		},
		{
			name:       "numeric executable expression",
			column:     Column{Name: "value", Type: "numeric"},
			catalogSQL: "dangerous()",
		},
		{
			name:       "text wrong cast",
			column:     Column{Name: "value", Type: "text"},
			catalogSQL: `'active'::character varying`,
		},
		{
			name: "char wrong cast",
			column: Column{
				Name: "value",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
			},
			catalogSQL: `'AB'::character varying`,
		},
		{
			name:       "unsupported escape",
			column:     Column{Name: "value", Type: "text"},
			catalogSQL: `E'C:\temp'::text`,
		},
		{
			name:       "bytea missing casts",
			column:     Column{Name: "value", Type: "bytea"},
			catalogSQL: `decode('00', 'hex')`,
		},
		{
			name:       "timestamp builtin",
			column:     Column{Name: "value", Type: "timestamp"},
			catalogSQL: "now()",
		},
		{
			name:       "timestamp renderer text not PG16 deparse",
			column:     Column{Name: "value", Type: "timestamp"},
			catalogSQL: `date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := CatalogPostgresDefaultSignature(
				test.column,
				postgresDefaultCatalogSQL(test.catalogSQL),
			)
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want *PolicyError", err)
			}
			if strings.Contains(err.Error(), test.catalogSQL) &&
				test.catalogSQL != "" {
				t.Fatalf("catalog expression leaked in error: %v", err)
			}
		})
	}
}

func TestPostgresDefaultSignaturesNormalizePG16NumericDeparse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		columnType string
		planned    string
		catalog    string
	}{
		{
			name:       "positive integer is bare",
			columnType: "integer",
			planned:    "7",
			catalog:    "7",
		},
		{
			name:       "negative integer is cast",
			columnType: "integer",
			planned:    "-7",
			catalog:    `'-7'::integer`,
		},
		{
			name:       "small bigint is bare",
			columnType: "bigint",
			planned:    "7",
			catalog:    "7",
		},
		{
			name:       "large bigint is cast",
			columnType: "bigint",
			planned:    "9223372036854775807",
			catalog:    `'9223372036854775807'::bigint`,
		},
		{
			name:       "plain double is bare",
			columnType: "double precision",
			planned:    "1.25",
			catalog:    "1.25",
		},
		{
			name:       "double exponent becomes numeric cast",
			columnType: "double precision",
			planned:    "1e2",
			catalog:    `'100'::numeric`,
		},
		{
			name:       "double negative zero is folded",
			columnType: "double precision",
			planned:    "-0",
			catalog:    "0",
		},
		{
			name:       "numeric leading zeros are removed",
			columnType: "numeric",
			planned:    "00.50",
			catalog:    "0.50",
		},
		{
			name:       "numeric unary plus is parenthesized",
			columnType: "numeric",
			planned:    "+7",
			catalog:    "(+ 7)",
		},
		{
			name:       "numeric scale is semantic",
			columnType: "numeric",
			planned:    "0.00",
			catalog:    "0.0",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			column := postgresDefaultSignatureColumn(
				t,
				"value",
				test.columnType,
				nil,
				test.planned,
			)
			matches, err := PostgresDefaultSignaturesMatch(
				column,
				postgresDefaultCatalogSQL(test.catalog),
			)
			if err != nil || !matches {
				t.Fatalf("PG16 numeric match = %v, error = %v", matches, err)
			}
		})
	}
}

func TestPostgresDefaultSignatureDoesNotMutateColumn(t *testing.T) {
	t.Parallel()
	column := postgresDefaultSignatureColumn(
		t,
		"amount",
		"numeric",
		&DeclaredType{
			Base:      "numeric",
			Arguments: []int{12, 2},
		},
		"0.00",
	)
	before := column
	declaration := *column.DeclaredType
	declaration.Arguments = append([]int(nil), column.DeclaredType.Arguments...)
	before.DeclaredType = &declaration
	expression := *column.Default
	before.Default = &expression

	if _, err := PlannedPostgresDefaultSignature(column); err != nil {
		t.Fatal(err)
	}
	if _, err := CatalogPostgresDefaultSignature(
		column,
		postgresDefaultCatalogSQL("0.00"),
	); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(column, before) {
		t.Fatalf("column mutated: got %#v, want %#v", column, before)
	}
}

func postgresDefaultSignatureColumn(
	t *testing.T,
	name string,
	columnType string,
	declaration *DeclaredType,
	defaultSQL string,
) Column {
	t.Helper()
	expression, err := ParseSQLiteDefault(defaultSQL)
	if err != nil {
		t.Fatalf("ParseSQLiteDefault(%q): %v", defaultSQL, err)
	}
	return Column{
		Name:         name,
		Type:         columnType,
		DeclaredType: declaration,
		Default:      expression,
	}
}

func postgresDefaultCatalogSQL(value string) *string {
	return &value
}

func postgresDefaultTestSignature(
	kind PostgresDefaultKind,
	value string,
) PostgresDefaultSignature {
	return PostgresDefaultSignature{
		Present: true,
		Kind:    kind,
		Value:   value,
	}
}
