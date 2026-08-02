package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4RebuildPreFinalizeTableRetainsOnlyBaseAuthority(
	t *testing.T,
) {
	identityFrontier := int64(9)
	planned := schema.Table{
		Schema:            "target",
		Name:              "events",
		MySQLCollation:    "utf8mb4_0900_bin",
		ClickHouseOrderBy: []string{"id"},
		Identity: &schema.Identity{
			Column: "id", Generation: schema.IdentityByDefault,
			Frontier: &identityFrontier,
		},
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
		Indexes: []schema.Index{{
			Name: "events_payload_idx", Columns: []schema.IndexColumn{{Name: "payload"}},
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name: "events_parent_fk", Columns: []string{"id"},
			ReferencedSchema: "target", ReferencedTable: "parents",
			ReferencedColumns: []string{"id"},
		}},
		Checks: []schema.CheckConstraint{{Name: "events_payload_check"}},
	}

	base := stage4RebuildPreFinalizeTable(planned)
	if len(base.Indexes) != 0 || len(base.ForeignKeys) != 0 || len(base.Checks) != 0 {
		t.Fatalf("pre-finalize table retains post-load objects: %#v", base)
	}
	if base.Schema != planned.Schema || base.Name != planned.Name ||
		base.MySQLCollation != planned.MySQLCollation ||
		len(base.Columns) != len(planned.Columns) ||
		base.Identity == nil || base.Identity.Column != "id" ||
		base.Identity.Frontier == nil || *base.Identity.Frontier != identityFrontier {
		t.Fatalf("pre-finalize table lost base authority: %#v", base)
	}
	base.Columns[0].Name = "mutated"
	*base.Identity.Frontier = 0
	if planned.Columns[0].Name != "id" || *planned.Identity.Frontier != identityFrontier ||
		len(planned.Indexes) != 1 || len(planned.ForeignKeys) != 1 || len(planned.Checks) != 1 {
		t.Fatalf("pre-finalize projection mutated immutable final plan: %#v", planned)
	}
}
