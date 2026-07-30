package migrate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteTargetOrdersDropRecreateParentsBeforeChildren(
	t *testing.T,
) {
	tables := []schema.Table{
		adapterDependencyTable(
			"account_events",
			schema.ForeignKey{ReferencedTable: "accounts"},
		),
		adapterDependencyTable("accounts"),
	}
	ordered, err := (&sqliteTargetAdapter{}).OrderSourceTables(
		"postgres",
		tables,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := adapterDependencyNames(ordered); !reflect.DeepEqual(
		got,
		[]string{"accounts", "account_events"},
	) {
		t.Fatalf("SQLite rebuild order = %v", got)
	}
	if got := adapterDependencyNames(tables); !reflect.DeepEqual(
		got,
		[]string{"account_events", "accounts"},
	) {
		t.Fatalf("source slice was mutated: %v", got)
	}
}

func TestSQLiteTargetRejectsForeignKeyCycles(t *testing.T) {
	tables := []schema.Table{
		adapterDependencyTable(
			"alpha",
			schema.ForeignKey{ReferencedTable: "beta"},
		),
		adapterDependencyTable(
			"beta",
			schema.ForeignKey{ReferencedTable: "alpha"},
		),
	}
	_, err := (&sqliteTargetAdapter{}).OrderSourceTables(
		"postgres",
		tables,
		"drop_recreate",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "parent-before-child") ||
		!strings.Contains(err.Error(), "alpha, beta") {
		t.Fatalf("SQLite cycle error = %v", err)
	}
}

func TestValidateAdapterTargetSourceTableOrderFailsClosed(t *testing.T) {
	original := []schema.Table{
		adapterDependencyTable("alpha"),
		adapterDependencyTable("beta"),
	}
	changed := append([]schema.Table(nil), original...)
	changed[0].Columns = append(
		[]schema.Column(nil),
		changed[0].Columns...,
	)
	changed[0].Columns[0].Nullable = true

	tests := []struct {
		name      string
		requested []schema.Table
		want      string
	}{
		{
			name:      "missing table",
			requested: original[:1],
			want:      "1 source tables",
		},
		{
			name: "duplicate",
			requested: []schema.Table{
				original[0],
				original[0],
			},
			want: "duplicated",
		},
		{
			name: "unknown",
			requested: []schema.Table{
				original[0],
				adapterDependencyTable("gamma"),
			},
			want: "unknown",
		},
		{
			name:      "metadata changed",
			requested: changed,
			want:      "changed source table metadata",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateAdapterTargetSourceTableOrder(
				original,
				test.requested,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"order validation error = %v, want %q",
					err,
					test.want,
				)
			}
		})
	}
}
