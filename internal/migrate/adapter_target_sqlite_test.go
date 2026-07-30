package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
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
			{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
			},
			{Name: "note", Type: "text"},
		},
		Indexes: []schema.Index{
			{
				Name: "events_note",
				Columns: []schema.IndexColumn{
					{Name: "note", Collation: "BINARY"},
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
	planned, err := planSingleTargetTable(adapter, "postgres", source, "")
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
			{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
			},
		},
	}
	if _, err := planSingleTargetTable(adapter,
		"oracle",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "source engine") {
		t.Fatalf("unsupported source error = %v", err)
	}

	table.Columns[0].Type = "geography"
	if _, err := planSingleTargetTable(adapter,
		"postgres",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "map PostgreSQL") {
		t.Fatalf("invalid render shape error = %v", err)
	}

	if _, err := planSingleTargetTable(adapter,
		"postgres",
		table,
		"replace",
	); err == nil || !strings.Contains(err.Error(), "target mode") {
		t.Fatalf("invalid mode error = %v", err)
	}
}

func TestSQLiteTargetPlanTablesIsPureAndPreservesOrder(t *testing.T) {
	sources := []schema.Table{
		{
			Schema: "public",
			Name:   "parents",
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint", PrimaryKey: true,
					PrimaryKeyPosition: 1,
				},
			},
		},
		{
			Schema: "audit",
			Name:   "children",
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint", PrimaryKey: true,
					PrimaryKeyPosition: 1,
				},
				{Name: "parent_id", Type: "bigint"},
			},
		},
	}
	before := make([]schema.Table, len(sources))
	for index, table := range sources {
		before[index] = table
		before[index].Columns = append(
			[]schema.Column(nil),
			table.Columns...,
		)
	}

	adapter := &sqliteTargetAdapter{}
	planned, err := adapter.PlanTables(
		"postgres",
		sources,
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("PlanTables: %v", err)
	}
	if len(planned) != 2 ||
		planned[0].Name != "parents" ||
		planned[1].Name != "children" ||
		planned[0].Schema != "" ||
		planned[1].Schema != "" {
		t.Fatalf("planned tables = %#v", planned)
	}
	if !reflect.DeepEqual(sources, before) {
		t.Fatalf("source tables mutated: got %#v, want %#v", sources, before)
	}
}

func TestSQLiteTargetPreflightIsReadOnlyAndRequiresUpsertTable(
	t *testing.T,
) {
	database, err := sql.Open(
		"sqlite",
		sqliteTargetURI(filepath.Join(t.TempDir(), "target.db")),
	)
	if err != nil {
		t.Fatalf("open SQLite target: %v", err)
	}
	defer database.Close()

	adapter := &sqliteTargetAdapter{database: database}
	table := schema.Table{
		Name: "missing",
		Columns: []schema.Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
		},
	}
	if err := adapter.PreflightTables(
		context.Background(),
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("drop/recreate preflight: %v", err)
	}
	exists, err := tableExists(context.Background(), database, table.Name)
	if err != nil {
		t.Fatalf("inspect target: %v", err)
	}
	if exists {
		t.Fatal("drop/recreate preflight created the missing table")
	}

	err = adapter.PreflightTables(
		context.Background(),
		[]schema.Table{table},
		"upsert",
	)
	if err == nil || !strings.Contains(err.Error(), "requires an existing") {
		t.Fatalf("upsert preflight error = %v", err)
	}
	exists, existsErr := tableExists(
		context.Background(),
		database,
		table.Name,
	)
	if existsErr != nil {
		t.Fatalf("inspect target after upsert preflight: %v", existsErr)
	}
	if exists {
		t.Fatal("upsert preflight created the missing table")
	}
}
