package migrate

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestOrderAdapterSourceTablesForUpsertParentsBeforeChildren(
	t *testing.T,
) {
	tables := []schema.Table{
		adapterDependencyTable(
			"children",
			schema.ForeignKey{ReferencedTable: "parents"},
		),
		adapterDependencyTable("parents"),
	}
	ordered, err := orderAdapterSourceTablesForMode(tables, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	if got := adapterDependencyNames(ordered); !reflect.DeepEqual(
		got,
		[]string{"parents", "children"},
	) {
		t.Fatalf("upsert order = %v", got)
	}
	if got := adapterDependencyNames(tables); !reflect.DeepEqual(
		got,
		[]string{"children", "parents"},
	) {
		t.Fatalf("source table slice was mutated: %v", got)
	}
}

func TestOrderAdapterSourceTablesForUpsertIsDeterministic(
	t *testing.T,
) {
	tables := []schema.Table{
		adapterDependencyTable("zeta"),
		adapterDependencyTable("alpha"),
		adapterDependencyTable("middle"),
	}
	ordered, err := orderAdapterSourceTablesForMode(tables, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	if got := adapterDependencyNames(ordered); !reflect.DeepEqual(
		got,
		[]string{"alpha", "middle", "zeta"},
	) {
		t.Fatalf("independent upsert order = %v", got)
	}
}

func TestOrderAdapterSourceTablesUsesQualifiedForeignKeyIdentity(
	t *testing.T,
) {
	t.Parallel()

	identityParent := adapterDependencyTable("accounts")
	identityParent.Schema = "identity"
	child := adapterDependencyTable(
		"events",
		schema.ForeignKey{
			ReferencedSchema: "identity",
			ReferencedTable:  "accounts",
		},
	)
	child.Schema = "sales"
	ownerSameName := adapterDependencyTable(
		"accounts",
		schema.ForeignKey{ReferencedTable: "events"},
	)
	ownerSameName.Schema = "sales"

	ordered, err := orderAdapterSourceTablesForMode(
		[]schema.Table{ownerSameName, child, identityParent},
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(ordered))
	for index, table := range ordered {
		got[index] = table.Schema + "." + table.Name
	}
	want := []string{
		"identity.accounts",
		"sales.events",
		"sales.accounts",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("qualified dependency order = %v, want %v", got, want)
	}
}

func TestOrderAdapterSourceTablesKeepsDropRecreateOrder(t *testing.T) {
	tables := []schema.Table{
		adapterDependencyTable(
			"children",
			schema.ForeignKey{ReferencedTable: "parents"},
		),
		adapterDependencyTable("parents"),
	}
	ordered, err := orderAdapterSourceTablesForMode(
		tables,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := adapterDependencyNames(ordered); !reflect.DeepEqual(
		got,
		[]string{"children", "parents"},
	) {
		t.Fatalf("drop/recreate order = %v", got)
	}
}

func TestRelationalStage4TargetsOrderRebuildParentsBeforeChildren(
	t *testing.T,
) {
	tables := []schema.Table{
		adapterDependencyTable(
			"children",
			schema.ForeignKey{ReferencedTable: "parents"},
		),
		adapterDependencyTable("parents"),
	}
	for name, target := range map[string]adapterTargetSourceTableOrderer{
		"postgres": &postgresTargetAdapter{},
		"mysql":    &mysqlTargetAdapter{},
		"mssql":    &sqlServerTargetAdapter{},
	} {
		t.Run(name, func(t *testing.T) {
			ordered, err := target.OrderSourceTables(
				"sqlite",
				tables,
				"drop_recreate",
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := adapterDependencyNames(ordered); !reflect.DeepEqual(
				got,
				[]string{"parents", "children"},
			) {
				t.Fatalf("rebuild order = %v", got)
			}
			if got := adapterDependencyNames(tables); !reflect.DeepEqual(
				got,
				[]string{"children", "parents"},
			) {
				t.Fatalf("source table slice was mutated: %v", got)
			}
		})
	}
}

func TestOrderAdapterSourceTablesRejectsUpsertCycles(t *testing.T) {
	tests := []struct {
		name   string
		tables []schema.Table
		want   string
	}{
		{
			name: "self reference",
			tables: []schema.Table{
				adapterDependencyTable(
					"nodes",
					schema.ForeignKey{ReferencedTable: "nodes"},
				),
			},
			want: "nodes",
		},
		{
			name: "two table cycle",
			tables: []schema.Table{
				adapterDependencyTable(
					"alpha",
					schema.ForeignKey{ReferencedTable: "beta"},
				),
				adapterDependencyTable(
					"beta",
					schema.ForeignKey{ReferencedTable: "alpha"},
				),
			},
			want: "alpha, beta",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := orderAdapterSourceTablesForMode(
				test.tables,
				"upsert",
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("cycle error = %v, want %q", err, test.want)
			}
		})
	}
}

type adapterOrderRecordingTarget struct {
	*recordingAdapterTarget
	sourceOrder []string
}

func (target *adapterOrderRecordingTarget) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	target.sourceOrder = adapterDependencyNames(sourceTables)
	return target.recordingAdapterTarget.PlanTables(
		sourceEngine,
		sourceTables,
		mode,
	)
}

func TestAdapterRunnerExecutesUpsertParentsBeforeChildren(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterDependencyTable(
				"children",
				schema.ForeignKey{ReferencedTable: "parents"},
			),
			adapterDependencyTable("parents"),
		},
	}
	target := &adapterOrderRecordingTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{
			Migration: config.Migration{TargetMode: "upsert"},
		},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if !reflect.DeepEqual(
		target.sourceOrder,
		[]string{"parents", "children"},
	) {
		t.Fatalf("target planning order = %v", target.sourceOrder)
	}
	joined := fmt.Sprint(events)
	if !strings.Contains(
		joined,
		"before_tables:parents,children",
	) {
		t.Fatalf("observer table order = %v", events)
	}
	parent := strings.Index(joined, "before:parents")
	child := strings.Index(joined, "before:children")
	if parent < 0 || child < 0 || parent >= child {
		t.Fatalf("execution order = %v", events)
	}
}

func TestAdapterRunnerRejectsUpsertCycleBeforeTargetActivity(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterDependencyTable(
				"alpha",
				schema.ForeignKey{ReferencedTable: "beta"},
			),
			adapterDependencyTable(
				"beta",
				schema.ForeignKey{ReferencedTable: "alpha"},
			),
		},
	}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{
			Migration: config.Migration{TargetMode: "upsert"},
		},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "dependency cycle") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) ||
		len(target.planned) != 0 ||
		len(target.preflighted) != 0 ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf("target activity after cycle: %#v", target)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") {
			t.Fatalf("observer ran after cycle: %v", events)
		}
	}
}

func adapterDependencyTable(
	name string,
	foreignKeys ...schema.ForeignKey,
) schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   name,
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "payload"},
		},
		ForeignKeys: foreignKeys,
	}
}

func adapterDependencyNames(tables []schema.Table) []string {
	names := make([]string, len(tables))
	for index, table := range tables {
		names[index] = table.Name
	}
	return names
}
