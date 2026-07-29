package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func identityCloneTestTable(columnType string, frontier *int64) schema.Table {
	return schema.Table{
		Name: "events",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               columnType,
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
}

func assertPlannedIdentityIsolated(
	t *testing.T,
	source schema.Table,
	planned schema.Table,
	wantFrontier int64,
) {
	t.Helper()
	if planned.Identity == nil || planned.Identity.Frontier == nil {
		t.Fatalf("planned identity = %#v", planned.Identity)
	}
	planned.Identity.Column = "changed"
	*planned.Identity.Frontier = 99
	if source.Identity == nil ||
		source.Identity.Column != "id" ||
		source.Identity.Frontier == nil ||
		*source.Identity.Frontier != wantFrontier {
		t.Fatalf("planned identity aliases source: %#v", source.Identity)
	}
}

func TestPostgresTargetNetworkIdentityPlanIsMutationIsolated(t *testing.T) {
	frontier := int64(40)
	source := identityCloneTestTable("bigint", &frontier)
	adapter := &postgresTargetAdapter{namespace: "archive"}
	planned, err := adapter.PlanTables(
		"postgres",
		[]schema.Table{source},
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPlannedIdentityIsolated(t, source, planned[0], 40)
}

func TestPostgresTargetSQLiteIdentityPlanIsMutationIsolated(t *testing.T) {
	frontier := int64(50)
	source := identityCloneTestTable("integer", &frontier)
	source.Columns[0].DeclaredType = &schema.DeclaredType{Base: "integer"}
	adapter := &postgresTargetAdapter{namespace: "archive"}
	planned, err := adapter.PlanTables(
		"sqlite",
		[]schema.Table{source},
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	assertPlannedIdentityIsolated(t, source, planned[0], 50)
}

func TestSQLiteTargetIdentityPlanIsMutationIsolated(t *testing.T) {
	frontier := int64(25)
	source := identityCloneTestTable("bigint", &frontier)
	source.Schema = "public"
	adapter := &sqliteTargetAdapter{}
	planned, err := adapter.PlanTables(
		"postgres",
		[]schema.Table{source},
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if planned[0].Schema != "" {
		t.Fatalf("planned SQLite schema = %q", planned[0].Schema)
	}
	assertPlannedIdentityIsolated(t, source, planned[0], 25)
}
