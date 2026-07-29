package migrate

import (
	"database/sql"
	"math"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidatePostgresIdentitySequenceStateAcceptsExactShape(t *testing.T) {
	table := schema.Table{Name: "accounts", AutoIncrementColumn: "id"}
	if err := validatePostgresIdentitySequenceState(
		table,
		exactPostgresIdentitySequenceState(),
	); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePostgresIdentitySequenceStateFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*postgresIdentitySequenceState)
	}{
		{name: "missing OID", mutate: func(state *postgresIdentitySequenceState) {
			state.objectID = 0
		}},
		{name: "missing namespace", mutate: func(state *postgresIdentitySequenceState) {
			state.namespace = ""
		}},
		{name: "missing name", mutate: func(state *postgresIdentitySequenceState) {
			state.name = ""
		}},
		{name: "unlogged", mutate: func(state *postgresIdentitySequenceState) {
			state.persistence = "u"
		}},
		{name: "integer type", mutate: func(state *postgresIdentitySequenceState) {
			state.dataType = "integer"
		}},
		{name: "different start", mutate: func(state *postgresIdentitySequenceState) {
			state.start = 2
		}},
		{name: "different increment", mutate: func(state *postgresIdentitySequenceState) {
			state.increment = 2
		}},
		{name: "different minimum", mutate: func(state *postgresIdentitySequenceState) {
			state.minimum = 0
		}},
		{name: "different maximum", mutate: func(state *postgresIdentitySequenceState) {
			state.maximum = math.MaxInt32
		}},
		{name: "cache above one", mutate: func(state *postgresIdentitySequenceState) {
			state.cache = 2
		}},
		{name: "cycle", mutate: func(state *postgresIdentitySequenceState) {
			state.cycle = true
		}},
		{name: "cannot read", mutate: func(state *postgresIdentitySequenceState) {
			state.canRead = false
		}},
		{name: "cannot update", mutate: func(state *postgresIdentitySequenceState) {
			state.canUpdate = false
		}},
		{name: "cannot alter", mutate: func(state *postgresIdentitySequenceState) {
			state.canAlter = false
		}},
		{name: "last below bounds", mutate: func(state *postgresIdentitySequenceState) {
			state.lastValue = sql.NullInt64{Int64: 0, Valid: true}
		}},
	}
	table := schema.Table{Name: "accounts", AutoIncrementColumn: "id"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := exactPostgresIdentitySequenceState()
			test.mutate(&state)
			err := validatePostgresIdentitySequenceState(table, state)
			if err == nil ||
				!strings.Contains(err.Error(), "accounts") {
				t.Fatalf("identity sequence error = %v", err)
			}
		})
	}
}

func TestPostgresIdentitySequenceFrontierChoosesHighestObservedValue(t *testing.T) {
	value := func(number int64) *int64 { return &number }
	tests := []struct {
		name    string
		source  *int64
		target  sql.NullInt64
		current sql.NullInt64
		want    int64
		set     bool
	}{
		{name: "empty"},
		{
			name:   "source high water",
			source: value(50),
			want:   50,
			set:    true,
		},
		{
			name:   "zero source leaves fresh sequence untouched",
			source: value(0),
		},
		{
			name:   "target maximum wins",
			source: value(50),
			target: sql.NullInt64{Int64: 80, Valid: true},
			want:   80,
			set:    true,
		},
		{
			name:   "current allocated value wins",
			source: value(50),
			target: sql.NullInt64{Int64: 80, Valid: true},
			current: sql.NullInt64{
				Int64: 100,
				Valid: true,
			},
			want: 100,
			set:  true,
		},
		{
			name:   "negative explicit target keys do not lower frontier",
			target: sql.NullInt64{Int64: -7, Valid: true},
		},
		{
			name:    "maximum remains exhausted",
			source:  value(math.MaxInt64),
			current: sql.NullInt64{Int64: math.MaxInt64, Valid: true},
			want:    math.MaxInt64,
			set:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, set, err := postgresIdentitySequenceFrontier(
				test.source,
				test.target,
				test.current,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want || set != test.set {
				t.Fatalf(
					"frontier = (%d, %v), want (%d, %v)",
					got,
					set,
					test.want,
					test.set,
				)
			}
		})
	}
}

func TestPostgresIdentitySequenceFrontierRejectsNegativeSource(t *testing.T) {
	source := int64(-1)
	_, _, err := postgresIdentitySequenceFrontier(
		&source,
		sql.NullInt64{},
		sql.NullInt64{},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("negative source sequence error = %v", err)
	}
}

func TestPostgresIdentitySequenceCatalogQueryUsesOwnedSequenceOID(t *testing.T) {
	required := []string{
		"dependency.refobjsubid = attribute.attnum",
		"dependency.deptype = 'i'",
		"sequence_relation.oid = dependency.objid",
		"attribute.attidentity = 'd'",
		"has_sequence_privilege",
		"sequence_namespace.nspname",
		"sequence_relation.relname",
		"pg_has_role",
	}
	for _, fragment := range required {
		if !strings.Contains(
			postgresIdentitySequenceCatalogQuery,
			fragment,
		) {
			t.Fatalf("catalog query is missing %q", fragment)
		}
	}
}

func TestPostgresIdentitySequenceRestartDDLIsStructural(t *testing.T) {
	state := exactPostgresIdentitySequenceState()
	state.namespace = `odd"schema`
	state.name = `odd"sequence`
	if got, want := postgresIdentitySequenceLockStatement(state),
		`ALTER SEQUENCE "odd""schema"."odd""sequence" NO CYCLE`; got != want {
		t.Fatalf("lock statement = %q, want %q", got, want)
	}
	if got, want := postgresIdentitySequenceRestartStatement(state, 50),
		`ALTER SEQUENCE "odd""schema"."odd""sequence" RESTART WITH 50`; got != want {
		t.Fatalf("restart statement = %q, want %q", got, want)
	}
	table := schema.Table{
		Schema: `odd"schema`,
		Name:   `odd"table`,
	}
	if got, want := postgresIdentityTableLockStatement(table),
		`LOCK TABLE "odd""schema"."odd""table" IN SHARE ROW EXCLUSIVE MODE`; got != want {
		t.Fatalf("table lock statement = %q, want %q", got, want)
	}
}

func TestPostgresIdentitySequenceRestartUsesTransactionalSequenceLock(
	t *testing.T,
) {
	lock := postgresIdentitySequenceLockStatement(
		exactPostgresIdentitySequenceState(),
	)
	if !strings.Contains(lock, "ALTER SEQUENCE") ||
		!strings.Contains(lock, "NO CYCLE") {
		t.Fatalf("identity sequence lock = %q", lock)
	}
}

func TestValidatePostgresUpsertCatalogShapeAcceptsOnlyPlannedIdentity(t *testing.T) {
	planned := postgresUpsertPreflightPlannedTable()
	planned.AutoIncrementColumn = "id"
	actual := postgresUpsertPreflightCatalogShape()
	actual.columns[0].identity = "d"
	if err := validatePostgresUpsertCatalogShape(planned, actual); err != nil {
		t.Fatalf("validate planned identity: %v", err)
	}

	actual.columns[0].identity = ""
	err := validatePostgresUpsertCatalogShape(planned, actual)
	if err == nil ||
		!strings.Contains(err.Error(), "identity generation differs") {
		t.Fatalf("missing identity error = %v", err)
	}
	actual.columns[0].identity = "a"
	err = validatePostgresUpsertCatalogShape(planned, actual)
	if err == nil ||
		!strings.Contains(err.Error(), "identity generation differs") {
		t.Fatalf("wrong identity error = %v", err)
	}
}

func exactPostgresIdentitySequenceState() postgresIdentitySequenceState {
	return postgresIdentitySequenceState{
		objectID:    42,
		namespace:   "archive",
		name:        "accounts_id_seq",
		persistence: "p",
		dataType:    "bigint",
		start:       1,
		increment:   1,
		minimum:     1,
		maximum:     math.MaxInt64,
		cache:       1,
		canRead:     true,
		canUpdate:   true,
		canAlter:    true,
	}
}
