package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPlanPostgresRetainedChecks(t *testing.T) {
	t.Parallel()
	table := postgresRetainedCheckTestTable(t)
	planned, err := planPostgresRetainedChecks([]schema.Table{table})
	if err != nil {
		t.Fatal(err)
	}
	checks := planned[postgresRetainedTableKey(table.Schema, table.Name)]
	if len(checks) != 2 {
		t.Fatalf("planned CHECK constraints = %#v", checks)
	}
	byName, err := postgresRetainedChecksByName(checks)
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]schema.CheckConstraint{
		"accounts_balance_check": table.Checks[1],
		"accounts_status_check":  table.Checks[0],
	} {
		check, ok := byName[name]
		if !ok {
			t.Fatalf("planned CHECK %q is missing: %#v", name, checks)
		}
		want, err := schema.PlannedPostgresCheckSignature(
			source.Expression,
			table.Columns,
		)
		if err != nil {
			t.Fatal(err)
		}
		if check.signature != want ||
			!check.validated ||
			!check.local ||
			check.inheritanceCount != 0 ||
			check.noInherit ||
			check.parentConstraint {
			t.Fatalf("planned CHECK %q = %#v", name, check)
		}
	}
}

func TestValidatePostgresRetainedChecksRejectsExactnessDrift(t *testing.T) {
	t.Parallel()
	table := schema.Table{Name: "accounts"}
	expected := exactPostgresRetainedCheck()
	tests := []struct {
		name   string
		actual []postgresRetainedCheck
		want   string
	}{
		{
			name: "missing",
			want: "is missing",
		},
		{
			name: "extra",
			actual: []postgresRetainedCheck{
				expected,
				{name: "unexpected", validated: true, local: true},
			},
			want: "unexpected CHECK constraint",
		},
		{
			name: "logical signature",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.signature = "changed"
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
		{
			name: "not validated",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.validated = false
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
		{
			name: "not local",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.local = false
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
		{
			name: "inherited",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.inheritanceCount = 1
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
		{
			name: "no inherit option",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.noInherit = true
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
		{
			name: "parent constraint",
			actual: []postgresRetainedCheck{
				func() postgresRetainedCheck {
					value := expected
					value.parentConstraint = true
					return value
				}(),
			},
			want: "differs from the planned shape",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validatePostgresRetainedChecks(
				table,
				[]postgresRetainedCheck{expected},
				test.actual,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("retained CHECK error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidatePostgresRetainedChecksAcceptsExactShape(t *testing.T) {
	t.Parallel()
	check := exactPostgresRetainedCheck()
	if err := validatePostgresRetainedChecks(
		schema.Table{Name: "accounts"},
		[]postgresRetainedCheck{check},
		[]postgresRetainedCheck{check},
	); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRetainedChecksQueryUsesParsedCatalogExpression(
	t *testing.T,
) {
	t.Parallel()
	required := []string{
		"constraint_object.convalidated",
		"constraint_object.conislocal",
		"constraint_object.coninhcount",
		"constraint_object.connoinherit",
		"conparentid",
		"pg_catalog.pg_get_expr",
		"constraint_object.conbin",
		"constraint_object.conrelid",
	}
	for _, fragment := range required {
		if !strings.Contains(postgresRetainedChecksQuery, fragment) {
			t.Fatalf("CHECK catalog query is missing %q", fragment)
		}
	}
	if strings.Contains(
		postgresRetainedChecksQuery,
		"pg_get_constraintdef",
	) {
		t.Fatal("CHECK catalog query uses executable constraint definition text")
	}
}

func postgresRetainedCheckTestTable(t *testing.T) schema.Table {
	t.Helper()
	status, err := schema.ParseSQLiteCheckExpression(
		`status IN ('active', 'paused')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	balance, err := schema.ParseSQLiteCheckExpression(`balance >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	return schema.Table{
		Schema: "tenant",
		Name:   "accounts",
		Columns: []schema.Column{
			{Name: "status", Type: "text"},
			{Name: "balance", Type: "numeric"},
		},
		Checks: []schema.CheckConstraint{
			{
				Name:       "accounts_status_check",
				Expression: status,
			},
			{
				Name:       "accounts_balance_check",
				Expression: balance,
			},
		},
	}
}

func exactPostgresRetainedCheck() postgresRetainedCheck {
	return postgresRetainedCheck{
		name:      "accounts_balance_check",
		signature: "dmtx:postgres-check:v1:test",
		validated: true,
		local:     true,
	}
}
