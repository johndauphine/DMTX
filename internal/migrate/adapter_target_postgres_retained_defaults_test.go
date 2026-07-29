package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidatePostgresRetainedDefaultsAcceptsLogicalCatalogShape(
	t *testing.T,
) {
	table := postgresRetainedDefaultsTestTable(t)
	actual := postgresRetainedDefaultsTestCatalog()
	if err := validatePostgresRetainedDefaults(table, actual); err != nil {
		t.Fatalf("validate retained PostgreSQL defaults: %v", err)
	}
}

func TestValidatePostgresRetainedDefaultsFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*[]postgresRetainedDefault)
	}{
		{
			name: "missing planned default",
			want: `column "amount" default differs`,
			mutate: func(values *[]postgresRetainedDefault) {
				(*values)[0].expression = nil
			},
		},
		{
			name: "unexpected default",
			want: `column "note" default differs`,
			mutate: func(values *[]postgresRetainedDefault) {
				(*values)[1].expression = postgresRetainedDefaultSQL(
					`'unexpected'::text`,
				)
			},
		},
		{
			name: "changed default",
			want: `column "amount" default differs`,
			mutate: func(values *[]postgresRetainedDefault) {
				(*values)[0].expression = postgresRetainedDefaultSQL("1.00")
			},
		},
		{
			name: "unsupported catalog expression",
			want: "cannot be proven equivalent",
			mutate: func(values *[]postgresRetainedDefault) {
				(*values)[2].expression = postgresRetainedDefaultSQL(
					"clock_timestamp()",
				)
			},
		},
		{
			name: "column order",
			want: `catalog column 1 is "note"`,
			mutate: func(values *[]postgresRetainedDefault) {
				(*values)[0], (*values)[1] = (*values)[1], (*values)[0]
			},
		},
		{
			name: "column count",
			want: "catalog returned 3 columns",
			mutate: func(values *[]postgresRetainedDefault) {
				*values = (*values)[:3]
			},
		},
	}

	table := postgresRetainedDefaultsTestTable(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := postgresRetainedDefaultsTestCatalog()
			test.mutate(&actual)
			err := validatePostgresRetainedDefaults(table, actual)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("retained-default error = %v, want %q", err, test.want)
			}
		})
	}
}

func postgresRetainedDefaultsTestTable(t *testing.T) schema.Table {
	t.Helper()
	return schema.Table{
		Schema: "archive",
		Name:   "measurements",
		Columns: []schema.Column{
			postgresRetainedDefaultColumn(
				t,
				"amount",
				"numeric",
				"0.00",
			),
			{
				Name:     "note",
				Type:     "text",
				Nullable: true,
			},
			postgresRetainedDefaultColumn(
				t,
				"created_at",
				"timestamp",
				"CURRENT_TIMESTAMP",
			),
			postgresRetainedDefaultColumn(
				t,
				"optional",
				"text",
				"NULL",
			),
		},
	}
}

func postgresRetainedDefaultsTestCatalog() []postgresRetainedDefault {
	return []postgresRetainedDefault{
		{
			columnName: "amount",
			expression: postgresRetainedDefaultSQL("0.00"),
		},
		{columnName: "note"},
		{
			columnName: "created_at",
			expression: postgresRetainedDefaultSQL(
				`date_trunc('second'::text, ` +
					`(statement_timestamp() AT TIME ZONE 'UTC'::text))`,
			),
		},
		{columnName: "optional"},
	}
}

func postgresRetainedDefaultColumn(
	t *testing.T,
	name string,
	columnType string,
	defaultSQL string,
) schema.Column {
	t.Helper()
	expression, err := schema.ParseSQLiteDefault(defaultSQL)
	if err != nil {
		t.Fatalf("parse SQLite default %q: %v", defaultSQL, err)
	}
	return schema.Column{
		Name:     name,
		Type:     columnType,
		Nullable: true,
		Default:  expression,
	}
}

func postgresRetainedDefaultSQL(value string) *string {
	return &value
}
