package migrate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteTargetPlanTableIsPureAndClearsNamespace(t *testing.T) {
	source := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "note", Type: "text", Nullable: true},
		},
		Indexes: []schema.Index{
			{
				Name: "events_note",
				Columns: []schema.IndexColumn{
					{Name: "note"},
				},
			},
		},
	}
	before := source
	before.Columns = append([]schema.Column(nil), source.Columns...)
	before.Indexes = append([]schema.Index(nil), source.Indexes...)
	before.Indexes[0].Columns = append(
		[]schema.IndexColumn(nil),
		source.Indexes[0].Columns...,
	)

	adapter := &sqliteTargetAdapter{}
	planned, err := adapter.PlanTable("postgres", source, "")
	if err != nil {
		t.Fatalf("PlanTable: %v", err)
	}
	if planned.Schema != "" || planned.Name != source.Name {
		t.Fatalf("planned table = %#v", planned)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table mutated: got %#v, want %#v", source, before)
	}
}

func TestSQLiteTargetPlanTableValidatesSourceAndRenderShape(t *testing.T) {
	adapter := &sqliteTargetAdapter{}
	table := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
		},
	}
	if _, err := adapter.PlanTable(
		"oracle",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "source engine") {
		t.Fatalf("unsupported source error = %v", err)
	}

	table.Columns[0].Type = "geography"
	if _, err := adapter.PlanTable(
		"postgres",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "plan SQLite table") {
		t.Fatalf("invalid render shape error = %v", err)
	}

	if _, err := adapter.PlanTable(
		"postgres",
		table,
		"replace",
	); err == nil || !strings.Contains(err.Error(), "target mode") {
		t.Fatalf("invalid mode error = %v", err)
	}
}
