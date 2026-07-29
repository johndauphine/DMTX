package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPlanPostgresDropRecreateRelationNamesIncludesAllRelations(
	t *testing.T,
) {
	sequence := int64(50)
	planned, err := planPostgresDropRecreateRelationNames([]schema.Table{{
		Schema: "archive",
		Name:   "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &sequence,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "external_id", Type: "text"},
		},
		Indexes: []schema.Index{{
			Name:   "accounts_external_id_uq",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name:      "external_id",
				Collation: "BINARY",
			}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 4 {
		t.Fatalf("planned relations = %#v, want 4", planned)
	}
	got := make(map[string]string, len(planned))
	for _, relation := range planned {
		if relation.namespace != "archive" ||
			relation.table != "accounts" {
			t.Fatalf("planned relation = %#v", relation)
		}
		got[relation.name] = relation.kind
	}
	want := map[string]string{
		"accounts":                "table",
		"accounts_pkey":           "primary-key index",
		"accounts_id_seq":         "identity sequence",
		"accounts_external_id_uq": "post-load index",
	}
	for name, kind := range want {
		if got[name] != kind {
			t.Fatalf("planned relation %s = %q, want %q", name, got[name], kind)
		}
	}
}

func TestPostgresAutomaticRelationNameMirrorsServerClipping(
	t *testing.T,
) {
	if got, want := postgresAutomaticRelationName(
		strings.Repeat("a", 63),
		"",
		"pkey",
	), strings.Repeat("a", 58)+"_pkey"; got != want {
		t.Fatalf("long primary-key name = %q, want %q", got, want)
	}
	if got, want := postgresAutomaticRelationName(
		strings.Repeat("a", 40),
		strings.Repeat("b", 40),
		"seq",
	), strings.Repeat("a", 29)+"_"+
		strings.Repeat("b", 29)+"_seq"; got != want {
		t.Fatalf("long identity name = %q, want %q", got, want)
	}
	multibyte := postgresAutomaticRelationName(
		strings.Repeat("界", 30),
		"",
		"pkey",
	)
	if len(multibyte) > postgresRelationNameMaximumBytes ||
		!strings.HasSuffix(multibyte, "_pkey") {
		t.Fatalf("multibyte generated name = %q", multibyte)
	}
}

func TestPlanPostgresDropRecreateRelationNamesRejectsInternalCollision(
	t *testing.T,
) {
	_, err := planPostgresDropRecreateRelationNames([]schema.Table{
		{
			Schema: "archive",
			Name:   "accounts",
			Columns: []schema.Column{{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			}},
		},
		{
			Schema: "archive",
			Name:   "accounts_pkey",
			Columns: []schema.Column{{
				Name: "payload",
				Type: "text",
			}},
		},
	})
	if err == nil ||
		!strings.Contains(err.Error(), "share relation name") ||
		!strings.Contains(err.Error(), "accounts_pkey") {
		t.Fatalf("internal relation collision error = %v", err)
	}
}

func TestValidatePostgresDropRecreateRelationNamesAllowsSelectedOwnership(
	t *testing.T,
) {
	planned := postgresRelationCollisionPlannedNames()
	selected := map[string]struct{}{
		postgresRelationNameKey("archive", "accounts"): {},
		postgresRelationNameKey("archive", "events"):   {},
	}
	existing := []postgresExistingRelationName{
		{
			objectID:     1,
			namespace:    "archive",
			name:         "accounts",
			relationKind: "r",
		},
		{
			objectID:            2,
			namespace:           "archive",
			name:                "accounts_pkey",
			relationKind:        "i",
			indexOwnerNamespace: "archive",
			indexOwnerTable:     "accounts",
		},
		{
			objectID:               3,
			namespace:              "archive",
			name:                   "accounts_id_seq",
			relationKind:           "S",
			sequenceOwnerNamespace: "archive",
			sequenceOwnerTable:     "accounts",
		},
		{
			objectID:            4,
			namespace:           "archive",
			name:                "accounts_external_id_uq",
			relationKind:        "i",
			indexOwnerNamespace: "archive",
			indexOwnerTable:     "events",
		},
	}
	if err := validatePostgresDropRecreateRelationNames(
		planned,
		existing,
		selected,
	); err != nil {
		t.Fatalf("selected-table-owned collision rejected: %v", err)
	}
}

func TestValidatePostgresDropRecreateRelationNamesRejectsExternalRelation(
	t *testing.T,
) {
	planned := postgresRelationCollisionPlannedNames()
	selected := map[string]struct{}{
		postgresRelationNameKey("archive", "accounts"): {},
	}
	tests := []struct {
		name     string
		existing postgresExistingRelationName
		want     string
	}{
		{
			name: "identity sequence",
			existing: postgresExistingRelationName{
				objectID:     5,
				namespace:    "archive",
				name:         "accounts_id_seq",
				relationKind: "S",
			},
			want: "identity sequence",
		},
		{
			name: "post-load index",
			existing: postgresExistingRelationName{
				objectID:     6,
				namespace:    "archive",
				name:         "accounts_external_id_uq",
				relationKind: "v",
			},
			want: "post-load index",
		},
		{
			name: "selected name occupied by view",
			existing: postgresExistingRelationName{
				objectID:     7,
				namespace:    "archive",
				name:         "accounts",
				relationKind: "v",
			},
			want: "planned PostgreSQL table",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgresDropRecreateRelationNames(
				planned,
				[]postgresExistingRelationName{test.existing},
				selected,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.want) ||
				!strings.Contains(
					err.Error(),
					"outside selected target tables",
				) {
				t.Fatalf("external relation collision error = %v", err)
			}
		})
	}
}

func TestValidatePostgresDropRecreateRelationNamesIgnoresUnplannedNames(
	t *testing.T,
) {
	err := validatePostgresDropRecreateRelationNames(
		postgresRelationCollisionPlannedNames(),
		[]postgresExistingRelationName{{
			objectID:     8,
			namespace:    "archive",
			name:         "unrelated",
			relationKind: "r",
		}},
		map[string]struct{}{},
	)
	if err != nil {
		t.Fatalf("unplanned relation rejected: %v", err)
	}
}

func TestPostgresDropRecreateRelationCatalogQueryTracksDropOwnership(
	t *testing.T,
) {
	required := []string{
		"FROM pg_catalog.pg_class AS relation",
		"index_metadata.indexrelid = relation.oid",
		"index_owner.oid = index_metadata.indrelid",
		"relation.relkind = 'S'",
		"dependency.objid = relation.oid",
		"dependency.refobjsubid > 0",
		"dependency.deptype IN ('a', 'i')",
		"owner_relation.oid = dependency.refobjid",
		"WHERE namespace.nspname = $1",
		"ORDER BY relation.relname, relation.oid",
	}
	for _, fragment := range required {
		if !strings.Contains(
			postgresDropRecreateRelationCatalogQuery,
			fragment,
		) {
			t.Fatalf("relation catalog query is missing %q", fragment)
		}
	}
}

func TestPostgresTargetPlanPreflightDoesNotQueryForUpsert(t *testing.T) {
	adapter := &postgresTargetAdapter{}
	if err := adapter.PreflightTargetPlan(
		t.Context(),
		nil,
		"upsert",
	); err != nil {
		t.Fatalf("upsert relation-name preflight: %v", err)
	}
}

func postgresRelationCollisionPlannedNames() []postgresPlannedRelationName {
	return []postgresPlannedRelationName{
		{
			namespace: "archive",
			name:      "accounts",
			kind:      "table",
			table:     "accounts",
		},
		{
			namespace: "archive",
			name:      "accounts_pkey",
			kind:      "primary-key index",
			table:     "accounts",
		},
		{
			namespace: "archive",
			name:      "accounts_id_seq",
			kind:      "identity sequence",
			table:     "accounts",
		},
		{
			namespace: "archive",
			name:      "accounts_external_id_uq",
			kind:      "post-load index",
			table:     "accounts",
		},
	}
}
