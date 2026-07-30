package schema

import (
	"errors"
	"strings"
	"testing"
)

func postgresTestDefault(t *testing.T, value string) *Expression {
	t.Helper()
	expression, err := ParseSQLiteDefault(value)
	if err != nil {
		t.Fatalf("ParseSQLiteDefault(%q): %v", value, err)
	}
	return expression
}

func TestPostgresRendersDeclaredScalarTypesAndSafeDefaults(t *testing.T) {
	table := Table{
		Schema: "public",
		Name:   "scalar_defaults",
		Columns: []Column{
			{
				Name: "code",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{8},
				},
				Default: postgresTestDefault(t, "'O''Brien'"),
			},
			{
				Name: "fixed",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
				Default: postgresTestDefault(t, "'AB'"),
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
				Default: postgresTestDefault(t, "(0.00)"),
			},
			{
				Name:    "enabled",
				Type:    "boolean",
				Default: postgresTestDefault(t, "TRUE"),
			},
			{
				Name:    "payload",
				Type:    "bytea",
				Default: postgresTestDefault(t, "X'00FF'"),
			},
			{
				Name:    "event_day",
				Type:    "date",
				Default: postgresTestDefault(t, "CURRENT_DATE"),
			},
			{
				Name:    "created_at",
				Type:    "timestamp",
				Default: postgresTestDefault(t, "CURRENT_TIMESTAMP"),
			},
		},
	}

	got, err := CreateTable(Postgres, table)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	const want = `CREATE TABLE "public"."scalar_defaults" ("code" VARCHAR(8) NOT NULL DEFAULT E'O''Brien', "fixed" CHAR(4) NOT NULL DEFAULT E'AB', "amount" NUMERIC(12,2) NOT NULL DEFAULT 0.00, "enabled" BOOLEAN NOT NULL DEFAULT TRUE, "payload" BYTEA NOT NULL DEFAULT decode('00ff', 'hex'), "event_day" DATE NOT NULL DEFAULT (statement_timestamp() AT TIME ZONE 'UTC')::date, "created_at" TIMESTAMP NOT NULL DEFAULT date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'));`
	if got != want {
		t.Fatalf("PostgreSQL DDL:\n got: %s\nwant: %s", got, want)
	}
}

func TestPostgresCurrentDefaultsUseUTCStatementTime(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		defaultSQL string
		want       string
	}{
		{
			name:       "time",
			columnType: "time",
			defaultSQL: "CURRENT_TIME",
			want:       "(date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'))::time",
		},
		{
			name:       "date",
			columnType: "date",
			defaultSQL: "CURRENT_DATE",
			want:       "(statement_timestamp() AT TIME ZONE 'UTC')::date",
		},
		{
			name:       "timestamp",
			columnType: "timestamp",
			defaultSQL: "CURRENT_TIMESTAMP",
			want:       "date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := renderPostgresDefault(Column{
				Name:    "value",
				Type:    test.columnType,
				Default: postgresTestDefault(t, test.defaultSQL),
			})
			if err != nil {
				t.Fatalf("renderPostgresDefault: %v", err)
			}
			if got != test.want {
				t.Fatalf("default = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPostgresScalarRendererFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		column Column
	}{
		{
			name: "zero character length",
			column: Column{
				Name: "value",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base:      "char",
					Arguments: []int{0},
				},
			},
		},
		{
			name: "numeric scale beyond precision",
			column: Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "numeric",
					Arguments: []int{4, 5},
				},
			},
		},
		{
			name: "unsupported temporal precision",
			column: Column{
				Name: "value",
				Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base:      "timestamp",
					Arguments: []int{7},
				},
			},
		},
		{
			name: "overlong text default",
			column: Column{
				Name: "value",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{2},
				},
				Default: postgresTestDefault(t, "'abc'"),
			},
		},
		{
			name: "numeric rounding default",
			column: Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "numeric",
					Arguments: []int{4, 2},
				},
				Default: postgresTestDefault(t, "1.234"),
			},
		},
		{
			name: "current timestamp on text",
			column: Column{
				Name:    "value",
				Type:    "text",
				Default: postgresTestDefault(t, "CURRENT_TIMESTAMP"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := CreateTable(Postgres, Table{
				Name:    "unsafe_default",
				Columns: []Column{test.column},
			})
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestSQLiteDefaultParserRejectsExecutableExpressions(t *testing.T) {
	for _, value := range []string{
		"now()",
		"(SELECT secret FROM credentials)",
		"'safe' || dangerous()",
	} {
		if _, err := ParseSQLiteDefault(value); err == nil {
			t.Fatalf("default %q unexpectedly succeeded", value)
		}
	}
}

func TestPostgresStringDefaultEscapesBackslashesWithoutLeakingFailures(t *testing.T) {
	value := Column{
		Name:    "path",
		Type:    "text",
		Default: postgresTestDefault(t, `'C:\temp\O''Brien'`),
	}
	got, err := CreateTable(Postgres, Table{Name: "paths", Columns: []Column{value}})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	const want = `CREATE TABLE "paths" ("path" TEXT NOT NULL DEFAULT E'C:\\temp\\O''Brien');`
	if got != want {
		t.Fatalf("PostgreSQL escaped default = %q, want %q", got, want)
	}

	const secret = "literal-secret-marker"
	_, err = CreateTable(Postgres, Table{
		Name: "short_paths",
		Columns: []Column{{
			Name: "path",
			Type: "text",
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{1},
			},
			Default: postgresTestDefault(t, "'"+secret+"'"),
		}},
	})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("redacted default error = %v", err)
	}
}

func TestCreatePostgresTableRendersExactTemporalPrecision(t *testing.T) {
	table := Table{
		Schema: "archive",
		Name:   "events",
		Columns: []Column{
			{
				Name: "occurred_at",
				Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
			{
				Name: "received_at",
				Type: "timestamptz",
				DeclaredType: &DeclaredType{
					Base:      "timestamptz",
					Arguments: []int{0},
				},
			},
			{
				Name: "local_time",
				Type: "time",
				DeclaredType: &DeclaredType{
					Base:      "time",
					Arguments: []int{6},
				},
			},
		},
	}
	got, err := CreateTable(Postgres, table)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE TABLE "archive"."events" ("occurred_at" TIMESTAMP(3) NOT NULL, "received_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL, "local_time" TIME(6) NOT NULL);`
	if got != want {
		t.Fatalf("temporal DDL:\n got: %s\nwant: %s", got, want)
	}
	table.Columns[0].DeclaredType.Arguments = []int{7}
	if _, err := CreateTable(Postgres, table); err == nil {
		t.Fatal("precision 7 unexpectedly succeeded")
	}
	table.Columns[0].DeclaredType.Arguments = nil
	if _, err := CreateTable(Postgres, table); err == nil {
		t.Fatal("empty explicit temporal precision unexpectedly succeeded")
	}
}
