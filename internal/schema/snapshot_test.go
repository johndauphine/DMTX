package schema

import (
	"bytes"
	"strings"
	"testing"
)

func TestSchemaSnapshotCanonicalizesDiscoveryOrder(t *testing.T) {
	t.Parallel()

	left, err := NewSchemaSnapshot([]Table{
		snapshotTestChildTable(),
		snapshotTestParentTable(),
	})
	if err != nil {
		t.Fatalf("build left snapshot: %v", err)
	}
	rightChild := snapshotTestChildTable()
	rightChild.Indexes[0], rightChild.Indexes[1] =
		rightChild.Indexes[1], rightChild.Indexes[0]
	rightChild.Checks[0], rightChild.Checks[1] =
		rightChild.Checks[1], rightChild.Checks[0]
	right, err := NewSchemaSnapshot([]Table{
		snapshotTestParentTable(),
		rightChild,
	})
	if err != nil {
		t.Fatalf("build right snapshot: %v", err)
	}

	equal, err := SchemaSnapshotsEqual(left, right)
	if err != nil {
		t.Fatalf("compare snapshots: %v", err)
	}
	if !equal {
		leftJSON, _ := left.CanonicalJSON()
		rightJSON, _ := right.CanonicalJSON()
		t.Fatalf("equivalent discovery order changed snapshot:\n%s\n%s", leftJSON, rightJSON)
	}
	leftDigest, err := left.Digest()
	if err != nil {
		t.Fatalf("digest left snapshot: %v", err)
	}
	rightDigest, err := right.Digest()
	if err != nil {
		t.Fatalf("digest right snapshot: %v", err)
	}
	if leftDigest != rightDigest {
		t.Fatalf("equivalent snapshot digests differ: %s != %s", leftDigest, rightDigest)
	}
}

func TestSchemaSnapshotDigestHasStableWireEncoding(t *testing.T) {
	t.Parallel()

	snapshot, err := NewSchemaSnapshot([]Table{
		snapshotTestChildTable(),
		snapshotTestParentTable(),
	})
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	digest, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest snapshot: %v", err)
	}
	const want = "9069d92181c25445f2be43c0e229e560abc913375281137a020fe77b2915898a"
	if digest != want {
		t.Fatalf("schema snapshot digest = %q, want %q", digest, want)
	}
}

func TestSchemaSnapshotPreservesStructuralDriftButIgnoresIdentityFrontier(t *testing.T) {
	t.Parallel()

	baseTable := snapshotTestParentTable()
	base, err := NewSchemaSnapshot([]Table{baseTable})
	if err != nil {
		t.Fatalf("build base snapshot: %v", err)
	}

	newFrontier := int64(999)
	frontierOnly := snapshotTestParentTable()
	frontierOnly.Identity.Frontier = &newFrontier
	frontierSnapshot, err := NewSchemaSnapshot([]Table{frontierOnly})
	if err != nil {
		t.Fatalf("build frontier snapshot: %v", err)
	}
	equal, err := SchemaSnapshotsEqual(base, frontierSnapshot)
	if err != nil {
		t.Fatalf("compare frontier snapshot: %v", err)
	}
	if !equal {
		t.Fatal("identity frontier was incorrectly treated as schema drift")
	}

	tests := []struct {
		name   string
		mutate func(*Table)
	}{
		{
			name: "column order",
			mutate: func(table *Table) {
				table.Columns[0], table.Columns[1] = table.Columns[1], table.Columns[0]
			},
		},
		{
			name: "declared type",
			mutate: func(table *Table) {
				table.Columns[1].DeclaredType.Arguments[0]++
			},
		},
		{
			name: "default",
			mutate: func(table *Table) {
				table.Columns[1].Default = snapshotTestDefault("'changed'")
			},
		},
		{
			name: "identity generation",
			mutate: func(table *Table) {
				table.Identity.Generation = IdentityGeneration("unexpected")
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			changedTable := snapshotTestParentTable()
			test.mutate(&changedTable)
			changed, changedErr := NewSchemaSnapshot([]Table{changedTable})
			if test.name == "identity generation" {
				if changedErr == nil ||
					!strings.Contains(changedErr.Error(), "unsupported identity generation") {
					t.Fatalf("unsafe identity generation error = %v", changedErr)
				}
				return
			}
			if changedErr != nil {
				t.Fatalf("build changed snapshot: %v", changedErr)
			}
			matches, compareErr := SchemaSnapshotsEqual(base, changed)
			if compareErr != nil {
				t.Fatalf("compare changed snapshot: %v", compareErr)
			}
			if matches {
				t.Fatal("structural drift retained the same snapshot")
			}
		})
	}
}

func TestSchemaSnapshotRoundTripIsStrictAndDoesNotAliasInput(t *testing.T) {
	t.Parallel()

	table := snapshotTestChildTable()
	snapshot, err := NewSchemaSnapshot([]Table{table})
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	parsed, err := ParseSchemaSnapshot(encoded)
	if err != nil {
		t.Fatalf("parse snapshot: %v", err)
	}
	reencoded, err := parsed.CanonicalJSON()
	if err != nil {
		t.Fatalf("re-encode snapshot: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("snapshot round trip changed bytes:\n%s\n%s", encoded, reencoded)
	}

	table.Columns[0].Name = "mutated"
	table.Indexes[0].Columns[0].Name = "mutated"
	afterMutation, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode after input mutation: %v", err)
	}
	if !bytes.Equal(encoded, afterMutation) {
		t.Fatal("snapshot retained aliases into source metadata")
	}

	unknownField := append([]byte(nil), encoded...)
	unknownField = bytes.Replace(
		unknownField,
		[]byte(`"version":1`),
		[]byte(`"version":1,"future":true`),
		1,
	)
	if _, err := ParseSchemaSnapshot(unknownField); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown-field error = %v", err)
	}
	if _, err := ParseSchemaSnapshot(append(encoded, []byte(` {}`)...)); err == nil ||
		!strings.Contains(err.Error(), "trailing JSON document") {
		t.Fatalf("trailing-document error = %v", err)
	}
}

func TestSchemaSnapshotRejectsAmbiguousIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data SchemaSnapshot
		want string
	}{
		{
			name: "version",
			data: SchemaSnapshot{Version: 2, Tables: []SnapshotTable{}},
			want: "unsupported schema snapshot version",
		},
		{
			name: "duplicate table",
			data: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{
					{Name: "items", Columns: []SnapshotColumn{{Name: "id"}}},
					{Name: "items", Columns: []SnapshotColumn{{Name: "id"}}},
				},
			},
			want: "duplicate table",
		},
		{
			name: "duplicate column",
			data: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{
						{Name: "id"},
						{Name: "id"},
					},
				}},
			},
			want: "duplicate column",
		},
		{
			name: "unknown identity column",
			data: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:     "items",
					Identity: &SnapshotIdentity{Column: "missing", Generation: IdentityByDefault},
					Columns:  []SnapshotColumn{{Name: "id"}},
				}},
			},
			want: "identity references unknown column",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := test.data.CanonicalJSON()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSchemaSnapshotRejectsMalformedReferencedSchema(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		schema string
	}{
		{name: "NUL", schema: "identity\x00archive"},
		{name: "invalid UTF-8", schema: string([]byte{0xff})},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			table := snapshotTestChildTable()
			table.ForeignKeys[0].ReferencedSchema = test.schema
			if _, err := NewSchemaSnapshot([]Table{table}); err == nil ||
				!strings.Contains(err.Error(), "referenced schema") {
				t.Fatalf("malformed referenced schema error = %v", err)
			}
		})
	}

	prefix := []byte(
		`{"version":1,"tables":[{"schema":"","name":"items",` +
			`"mysql_collation":"","clickhouse_order_by":null,` +
			`"columns":[],"indexes":[],"foreign_keys":[{` +
			`"name":"items_parent_fk","columns":["parent_id"],` +
			`"referenced_schema":"`,
	)
	suffix := []byte(
		`","referenced_table":"parents","referenced_columns":["id"],` +
			`"on_update":"NO ACTION","on_delete":"NO ACTION",` +
			`"match":"SIMPLE"}],"checks":[],` +
			`"sqlite_without_rowid":false,"sqlite_strict":false}]}`,
	)
	malformedJSON := append(prefix, byte(0xff))
	malformedJSON = append(malformedJSON, suffix...)
	if _, err := ParseSchemaSnapshot(malformedJSON); err == nil ||
		!strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("raw invalid UTF-8 snapshot error = %v", err)
	}
}

func snapshotTestParentTable() Table {
	frontier := int64(41)
	return Table{
		Schema: "public",
		Name:   "parents",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name:         "label",
				Type:         "varchar",
				Nullable:     true,
				DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{40}},
				Default:      snapshotTestDefault("'parent'"),
			},
		},
	}
}

func snapshotTestChildTable() Table {
	return Table{
		Schema: "public",
		Name:   "children",
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name:         "parent_id",
				Type:         "bigint",
				DeclaredType: &DeclaredType{Base: "bigint"},
			},
		},
		Indexes: []Index{
			{Name: "children_parent", Columns: []IndexColumn{{Name: "parent_id"}}},
			{Name: "children_id", Unique: true, Columns: []IndexColumn{{Name: "id"}}},
		},
		ForeignKeys: []ForeignKey{{
			Name:              "children_parent_fk",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "SIMPLE",
		}},
		Checks: []CheckConstraint{
			{Name: "children_parent_positive", Expression: *snapshotTestDefault(`"parent_id" > 0`)},
			{Name: "children_id_positive", Expression: *snapshotTestDefault(`"id" > 0`)},
		},
	}
}

func snapshotTestDefault(sql string) *Expression {
	return &Expression{sql: sql, kind: expressionString, literal: sql}
}
