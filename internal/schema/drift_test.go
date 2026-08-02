package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCompareSchemaSnapshotsIgnoresNonSemanticDiscoveryOrder(t *testing.T) {
	t.Parallel()

	previous := driftEquivalentSnapshot()
	current := driftEquivalentSnapshot()
	current.Tables[0], current.Tables[1] = current.Tables[1], current.Tables[0]
	for index := range current.Tables {
		reverseSnapshotIndexes(current.Tables[index].Indexes)
		reverseSnapshotForeignKeys(current.Tables[index].ForeignKeys)
		reverseSnapshotChecks(current.Tables[index].Checks)
	}
	facts, err := CompareSchemaSnapshots(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 0 {
		t.Fatalf("reordered equivalent snapshots produced drift: %#v", facts)
	}
}

func TestCompareSchemaSnapshotsReportsEveryStructuralClass(t *testing.T) {
	t.Parallel()

	previous, current := driftStructuralSnapshots()
	facts, err := CompareSchemaSnapshots(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	wantCounts := map[SchemaDriftChangeKind]int{
		SchemaDriftTableAdded:         1,
		SchemaDriftTableDropped:       1,
		SchemaDriftColumnAdded:        1,
		SchemaDriftColumnDropped:      1,
		SchemaDriftColumnOrderChanged: 1,
		SchemaDriftDataTypeChanged:    1,
		SchemaDriftDefaultChanged:     1,
		SchemaDriftNullabilityChanged: 1,
		SchemaDriftPrimaryKeyChanged:  1,
		SchemaDriftIdentityChanged:    1,
		SchemaDriftIndexAdded:         1,
		SchemaDriftIndexDropped:       1,
		SchemaDriftIndexChanged:       1,
		SchemaDriftForeignKeyAdded:    1,
		SchemaDriftForeignKeyDropped:  1,
		SchemaDriftForeignKeyChanged:  1,
		SchemaDriftCheckAdded:         1,
		SchemaDriftCheckDropped:       1,
		SchemaDriftCheckChanged:       1,
		SchemaDriftTableOptionChanged: 4,
	}
	gotCounts := make(map[SchemaDriftChangeKind]int)
	for _, fact := range facts {
		gotCounts[fact.ChangeKind]++
		if !json.Valid(fact.Previous) || !json.Valid(fact.Current) {
			t.Fatalf("invalid evidence in fact %#v", fact)
		}
		switch fact.ChangeKind {
		case SchemaDriftTableAdded, SchemaDriftColumnAdded,
			SchemaDriftIndexAdded, SchemaDriftForeignKeyAdded,
			SchemaDriftCheckAdded:
			if string(fact.Previous) != "null" || string(fact.Current) == "null" {
				t.Fatalf("added fact evidence = %#v", fact)
			}
		case SchemaDriftTableDropped, SchemaDriftColumnDropped,
			SchemaDriftIndexDropped, SchemaDriftForeignKeyDropped,
			SchemaDriftCheckDropped:
			if string(fact.Previous) == "null" || string(fact.Current) != "null" {
				t.Fatalf("dropped fact evidence = %#v", fact)
			}
		default:
			if string(fact.Previous) == "null" || string(fact.Current) == "null" {
				t.Fatalf("changed fact lacks both evidence sides: %#v", fact)
			}
		}
	}
	if !reflect.DeepEqual(gotCounts, wantCounts) {
		t.Fatalf("change counts = %#v, want %#v\nfacts=%#v", gotCounts, wantCounts, facts)
	}
	if len(facts) != 23 {
		t.Fatalf("facts = %d, want 23", len(facts))
	}

	assertDriftEntity(t, facts, SchemaDriftDataTypeChanged, SchemaContractDataType)
	assertDriftEntity(t, facts, SchemaDriftDefaultChanged, SchemaContractColumns)
	assertDriftEntity(t, facts, SchemaDriftPrimaryKeyChanged, SchemaContractTables)
	assertDriftEntity(t, facts, SchemaDriftIndexChanged, SchemaContractTables)
	typeFact := findDriftFact(t, facts, SchemaDriftDataTypeChanged)
	if !bytes.Contains(typeFact.Previous, []byte(`"arguments":[40]`)) ||
		!bytes.Contains(typeFact.Current, []byte(`"arguments":[80]`)) {
		t.Fatalf("data-type evidence is incomplete: %#v", typeFact)
	}

	for index := 1; index < len(facts); index++ {
		if bytes.Compare(schemaDriftFactKey(facts[index-1]), schemaDriftFactKey(facts[index])) > 0 {
			t.Fatalf("facts are not stably sorted at %d: %#v then %#v", index, facts[index-1], facts[index])
		}
	}
}

func TestCompareSchemaSnapshotsProducesDeterministicFacts(t *testing.T) {
	t.Parallel()

	previous, current := driftStructuralSnapshots()
	first, err := CompareSchemaSnapshots(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	reverseSnapshotTables(previous.Tables)
	reverseSnapshotTables(current.Tables)
	for index := range previous.Tables {
		reverseSnapshotIndexes(previous.Tables[index].Indexes)
		reverseSnapshotForeignKeys(previous.Tables[index].ForeignKeys)
		reverseSnapshotChecks(previous.Tables[index].Checks)
	}
	for index := range current.Tables {
		reverseSnapshotIndexes(current.Tables[index].Indexes)
		reverseSnapshotForeignKeys(current.Tables[index].ForeignKeys)
		reverseSnapshotChecks(current.Tables[index].Checks)
	}
	second, err := CompareSchemaSnapshots(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("fact ordering changed with discovery order:\n%s\n%s", firstJSON, secondJSON)
	}
}

func TestCompareSchemaSnapshotsPreservesSemanticOrder(t *testing.T) {
	t.Parallel()

	base := SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{{
			Schema: "public",
			Name:   "items",
			Columns: []SnapshotColumn{
				{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
				{Name: "left", Type: "text"},
				{Name: "right", Type: "text"},
			},
			Indexes: []SnapshotIndex{{
				Name: "items_pair",
				Columns: []SnapshotIndexColumn{
					{Name: "left"},
					{Name: "right"},
				},
			}},
		}},
	}
	columnReordered := driftCloneSnapshot(t, base)
	columnReordered.Tables[0].Columns[1], columnReordered.Tables[0].Columns[2] =
		columnReordered.Tables[0].Columns[2], columnReordered.Tables[0].Columns[1]
	facts, err := CompareSchemaSnapshots(base, columnReordered)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ChangeKind != SchemaDriftColumnOrderChanged {
		t.Fatalf("column reorder facts = %#v", facts)
	}

	memberReordered := driftCloneSnapshot(t, base)
	memberReordered.Tables[0].Indexes[0].Columns[0],
		memberReordered.Tables[0].Indexes[0].Columns[1] =
		memberReordered.Tables[0].Indexes[0].Columns[1],
		memberReordered.Tables[0].Indexes[0].Columns[0]
	facts, err = CompareSchemaSnapshots(base, memberReordered)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ChangeKind != SchemaDriftIndexChanged {
		t.Fatalf("ordered index member facts = %#v", facts)
	}
}

func TestCompareSchemaSnapshotsPreservesFullColumnOrderEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous []string
		current  []string
	}{
		{
			name:     "inserted",
			previous: []string{"left", "right"},
			current:  []string{"left", "middle", "right"},
		},
		{
			name:     "appended",
			previous: []string{"left", "right"},
			current:  []string{"left", "right", "tail"},
		},
		{
			name:     "dropped",
			previous: []string{"left", "middle", "right"},
			current:  []string{"left", "right"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			facts, err := CompareSchemaSnapshots(
				driftColumnOrderSnapshot(test.previous),
				driftColumnOrderSnapshot(test.current),
			)
			if err != nil {
				t.Fatal(err)
			}
			fact := findDriftFact(t, facts, SchemaDriftColumnOrderChanged)
			var previous []string
			if err := json.Unmarshal(fact.Previous, &previous); err != nil {
				t.Fatal(err)
			}
			var current []string
			if err := json.Unmarshal(fact.Current, &current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(previous, test.previous) ||
				!reflect.DeepEqual(current, test.current) {
				t.Fatalf(
					"column-order evidence = %v -> %v, want %v -> %v",
					previous,
					current,
					test.previous,
					test.current,
				)
			}
		})
	}
}

func TestCompareSchemaSnapshotsFailsClosedOnUncorrelatableUnnamedObjectMutation(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name string
		kind SchemaDriftObjectKind
	}{
		{name: "index", kind: SchemaDriftObjectIndex},
		{name: "foreign key", kind: SchemaDriftObjectForeignKey},
		{name: "check", kind: SchemaDriftObjectCheck},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := driftUnnamedObjectSnapshot()
			current := driftUnnamedObjectSnapshot()
			switch test.kind {
			case SchemaDriftObjectIndex:
				previous.Tables[0].Indexes = []SnapshotIndex{{
					Columns: []SnapshotIndexColumn{{Name: "id"}},
				}}
				current.Tables[0].Indexes = []SnapshotIndex{{
					Unique: true, Columns: []SnapshotIndexColumn{{Name: "id"}},
				}}
			case SchemaDriftObjectForeignKey:
				previous.Tables[0].ForeignKeys = []SnapshotForeignKey{{
					Columns: []string{"parent_id"}, ReferencedTable: "parents",
					ReferencedColumns: []string{"id"}, OnDelete: "NO ACTION",
				}}
				current.Tables[0].ForeignKeys = []SnapshotForeignKey{{
					Columns: []string{"parent_id"}, ReferencedTable: "parents",
					ReferencedColumns: []string{"id"}, OnDelete: "CASCADE",
				}}
			case SchemaDriftObjectCheck:
				previous.Tables[0].Checks = []SnapshotCheckConstraint{{
					Expression: `"id" > 0`,
				}}
				current.Tables[0].Checks = []SnapshotCheckConstraint{{
					Expression: `"id" >= 0`,
				}}
			default:
				t.Fatalf("unsupported test object kind %q", test.kind)
			}

			_, err := CompareSchemaSnapshots(previous, current)
			var correlationError *UncorrelatableUnnamedSchemaObjectError
			if !errors.As(err, &correlationError) {
				t.Fatalf(
					"error = %T %v, want *UncorrelatableUnnamedSchemaObjectError",
					err,
					err,
				)
			}
			if correlationError.ObjectKind != test.kind ||
				correlationError.Schema != "public" ||
				correlationError.Table != "items" ||
				correlationError.PreviousUnmatched != 1 ||
				correlationError.CurrentUnmatched != 1 {
				t.Fatalf("typed error = %#v", correlationError)
			}
			want := "schema drift: table public.items has uncorrelatable unnamed " +
				string(test.kind) +
				" metadata (1 previous unmatched, 1 current unmatched)"
			if err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestCompareSchemaSnapshotsAllowsUnambiguousUnnamedObjectAdditionAndRemoval(
	t *testing.T,
) {
	t.Parallel()

	without := driftUnnamedObjectSnapshot()
	with := driftUnnamedObjectSnapshot()
	with.Tables[0].Checks = []SnapshotCheckConstraint{{
		Expression: `"id" > 0`,
	}}

	facts, err := CompareSchemaSnapshots(without, with)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ChangeKind != SchemaDriftCheckAdded {
		t.Fatalf("unnamed addition facts = %#v", facts)
	}
	facts, err = CompareSchemaSnapshots(with, without)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ChangeKind != SchemaDriftCheckDropped {
		t.Fatalf("unnamed removal facts = %#v", facts)
	}
}

func TestCompareSchemaSnapshotsFailsClosedOnMalformedOrAmbiguousInput(t *testing.T) {
	t.Parallel()

	blankDefault := " \t "
	valid := SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{{
			Name: "items",
			Columns: []SnapshotColumn{{
				Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1,
			}},
		}},
	}
	tests := []struct {
		name     string
		snapshot SchemaSnapshot
		want     string
	}{
		{
			name:     "unsupported version",
			snapshot: SchemaSnapshot{Version: SchemaSnapshotVersion + 1},
			want:     "unsupported schema snapshot version",
		},
		{
			name: "duplicate table",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{
					{Name: "items", Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}}},
					{Name: "items", Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}}},
				},
			},
			want: "duplicate table",
		},
		{
			name: "ambiguous object name",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{
						{Name: "id", Type: "bigint"},
						{Name: "other", Type: "bigint"},
					},
					Indexes: []SnapshotIndex{
						{Name: "duplicate", Columns: []SnapshotIndexColumn{{Name: "id"}}},
						{Name: "duplicate", Columns: []SnapshotIndexColumn{{Name: "other"}}},
					},
				}},
			},
			want: "ambiguous duplicate object name",
		},
		{
			name: "primary key gap",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{
						{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 2},
					},
				}},
			},
			want: "not contiguous",
		},
		{
			name: "non-key position",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{
						{Name: "id", Type: "bigint", PrimaryKeyPosition: 1},
					},
				}},
			},
			want: "non-primary-key column",
		},
		{
			name: "missing catalog type",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id"}},
				}},
			},
			want: "has no catalog type",
		},
		{
			name: "whitespace catalog type",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id", Type: " \t "}},
				}},
			},
			want: "has no catalog type",
		},
		{
			name: "whitespace declared type",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "text",
						DeclaredType: &SnapshotDeclaredType{Base: " \t "},
					}},
				}},
			},
			want: "has an empty declared type",
		},
		{
			name: "negative declared modifier",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "varchar",
						DeclaredType: &SnapshotDeclaredType{
							Base: "varchar", Arguments: []int{-1},
						},
					}},
				}},
			},
			want: "has a negative modifier",
		},
		{
			name: "excessive declared modifiers",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "numeric",
						DeclaredType: &SnapshotDeclaredType{
							Base: "numeric", Arguments: []int{12, 2, 1},
						},
					}},
				}},
			},
			want: "has too many modifiers",
		},
		{
			name: "invalid numeric declared range",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "numeric",
						DeclaredType: &SnapshotDeclaredType{
							Base: "decimal", Arguments: []int{2, 1001},
						},
					}},
				}},
			},
			want: "has invalid modifiers",
		},
		{
			name: "invalid length declared range",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "varchar",
						DeclaredType: &SnapshotDeclaredType{
							Base: "varchar", Arguments: []int{0},
						},
					}},
				}},
			},
			want: "has invalid modifiers",
		},
		{
			name: "invalid temporal declared range",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "timestamp",
						DeclaredType: &SnapshotDeclaredType{
							Base: "timestamp", Arguments: []int{7},
						},
					}},
				}},
			},
			want: "has invalid modifiers",
		},
		{
			name: "unsupported declared modifier shape",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "text",
						DeclaredType: &SnapshotDeclaredType{
							Base: "text", Arguments: []int{1},
						},
					}},
				}},
			},
			want: "has invalid modifiers",
		},
		{
			name: "whitespace default",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{{
						Name: "id", Type: "text", Default: &blankDefault,
					}},
				}},
			},
			want: "has a blank default",
		},
		{
			name: "whitespace check expression",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
					Checks: []SnapshotCheckConstraint{{
						Name: "blank", Expression: " \t ",
					}},
				}},
			},
			want: "has an empty expression",
		},
		{
			name: "whitespace table collation",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:           "items",
					MySQLCollation: " \t ",
					Columns:        []SnapshotColumn{{Name: "id", Type: "bigint"}},
				}},
			},
			want: "has a blank MySQL collation",
		},
		{
			name: "whitespace index collation",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
					Indexes: []SnapshotIndex{{
						Name: "bad",
						Columns: []SnapshotIndexColumn{{
							Name: "id", Collation: " \t ",
						}},
					}},
				}},
			},
			want: "has a blank collation",
		},
		{
			name: "whitespace foreign key action",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name: "items",
					Columns: []SnapshotColumn{
						{Name: "id", Type: "bigint"},
						{Name: "parent_id", Type: "bigint"},
					},
					ForeignKeys: []SnapshotForeignKey{{
						Name: "bad", Columns: []string{"parent_id"},
						ReferencedTable: "parents", ReferencedColumns: []string{"id"},
						OnDelete: " \t ",
					}},
				}},
			},
			want: "has blank ON DELETE metadata",
		},
		{
			name: "unknown index column",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
					Indexes: []SnapshotIndex{{
						Name: "bad", Columns: []SnapshotIndexColumn{{Name: "missing"}},
					}},
				}},
			},
			want: "references unknown column",
		},
		{
			name: "incomplete foreign key",
			snapshot: SchemaSnapshot{
				Version: SchemaSnapshotVersion,
				Tables: []SnapshotTable{{
					Name:    "items",
					Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
					ForeignKeys: []SnapshotForeignKey{{
						Name: "bad", Columns: []string{"id"},
						ReferencedTable: "parents",
					}},
				}},
			},
			want: "incomplete column mapping",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := CompareSchemaSnapshots(test.snapshot, valid); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	validJSON, err := valid.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "unknown field",
			data: []byte(`{"version":1,"tables":[],"future":true}`),
			want: "unknown field",
		},
		{
			name: "duplicate field",
			data: []byte(`{"version":1,"version":1,"tables":[]}`),
			want: "duplicate JSON field",
		},
		{
			name: "case alias",
			data: []byte(`{"Version":1,"tables":[]}`),
			want: "non-canonical JSON field",
		},
		{
			name: "trailing document",
			data: append(append([]byte(nil), validJSON...), []byte(` {}`)...),
			want: "trailing JSON document",
		},
		{
			name: "malformed JSON",
			data: []byte(`{"version":1,"tables":[`),
			want: "decode schema snapshot",
		},
	} {
		t.Run("json "+test.name, func(t *testing.T) {
			if _, err := CompareSchemaSnapshotJSON(test.data, validJSON); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func driftEquivalentSnapshot() SchemaSnapshot {
	return SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{
			{
				Schema: "public",
				Name:   "parents",
				Columns: []SnapshotColumn{
					{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
				},
				Indexes: []SnapshotIndex{
					{Name: "parents_id_a", Columns: []SnapshotIndexColumn{{Name: "id"}}},
					{Name: "parents_id_b", Unique: true, Columns: []SnapshotIndexColumn{{Name: "id"}}},
				},
				Checks: []SnapshotCheckConstraint{
					{Name: "parents_id_positive", Expression: `"id" > 0`},
					{Name: "parents_id_bounded", Expression: `"id" < 1000`},
				},
			},
			{
				Schema: "public",
				Name:   "children",
				Columns: []SnapshotColumn{
					{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
					{Name: "parent_id", Type: "bigint"},
				},
				ForeignKeys: []SnapshotForeignKey{
					{
						Name: "children_parent_a", Columns: []string{"parent_id"},
						ReferencedTable: "parents", ReferencedColumns: []string{"id"},
						OnDelete: "CASCADE",
					},
					{
						Name: "children_parent_b", Columns: []string{"parent_id"},
						ReferencedTable: "parents", ReferencedColumns: []string{"id"},
						OnDelete: "NO ACTION",
					},
				},
			},
		},
	}
}

func driftColumnOrderSnapshot(columns []string) SchemaSnapshot {
	snapshotColumns := make([]SnapshotColumn, len(columns))
	for index, name := range columns {
		snapshotColumns[index] = SnapshotColumn{Name: name, Type: "text"}
	}
	return SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{{
			Schema: "public", Name: "items", Columns: snapshotColumns,
		}},
	}
}

func driftUnnamedObjectSnapshot() SchemaSnapshot {
	return SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{{
			Schema: "public",
			Name:   "items",
			Columns: []SnapshotColumn{
				{Name: "id", Type: "bigint"},
				{Name: "parent_id", Type: "bigint"},
			},
		}},
	}
}

func driftStructuralSnapshots() (SchemaSnapshot, SchemaSnapshot) {
	previousDefault := "'old'"
	currentDefault := "'new'"
	previous := SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{
			{
				Schema:  "public",
				Name:    "gone",
				Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
			},
			{
				Schema:             "public",
				Name:               "items",
				MySQLCollation:     "utf8mb4_bin",
				ClickHouseOrderBy:  []string{"id"},
				SQLiteWithoutRowID: false,
				SQLiteStrict:       false,
				Identity: &SnapshotIdentity{
					Column: "id", Generation: IdentityByDefault,
				},
				Columns: []SnapshotColumn{
					{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
					{
						Name: "name", Type: "varchar", Nullable: true,
						DeclaredType: &SnapshotDeclaredType{Base: "varchar", Arguments: []int{40}},
						Default:      &previousDefault,
					},
					{Name: "parent_id", Type: "bigint"},
					{Name: "legacy", Type: "text"},
				},
				Indexes: []SnapshotIndex{
					{Name: "items_changed_idx", Columns: []SnapshotIndexColumn{{Name: "name"}}},
					{Name: "items_dropped_idx", Columns: []SnapshotIndexColumn{{Name: "parent_id"}}},
				},
				ForeignKeys: []SnapshotForeignKey{
					{
						Name: "items_changed_fk", Columns: []string{"parent_id"},
						ReferencedTable: "parents", ReferencedColumns: []string{"id"},
						OnDelete: "NO ACTION",
					},
					{
						Name: "items_dropped_fk", Columns: []string{"parent_id"},
						ReferencedTable: "legacy_parents", ReferencedColumns: []string{"id"},
					},
				},
				Checks: []SnapshotCheckConstraint{
					{Name: "items_changed_check", Expression: `"name" <> ''`},
					{Name: "items_dropped_check", Expression: `"legacy" <> ''`},
				},
			},
		},
	}
	current := SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables: []SnapshotTable{
			{
				Schema:  "public",
				Name:    "added",
				Columns: []SnapshotColumn{{Name: "id", Type: "bigint"}},
			},
			{
				Schema:             "public",
				Name:               "items",
				MySQLCollation:     "utf8mb4_0900_ai_ci",
				ClickHouseOrderBy:  []string{"name", "id"},
				SQLiteWithoutRowID: true,
				SQLiteStrict:       true,
				Identity: &SnapshotIdentity{
					Column: "name", Generation: IdentityByDefault,
				},
				Columns: []SnapshotColumn{
					{Name: "parent_id", Type: "bigint"},
					{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
					{
						Name: "name", Type: "varchar", Nullable: false,
						PrimaryKey: true, PrimaryKeyPosition: 2,
						DeclaredType: &SnapshotDeclaredType{Base: "varchar", Arguments: []int{80}},
						Default:      &currentDefault,
					},
					{Name: "extra", Type: "text"},
				},
				Indexes: []SnapshotIndex{
					{Name: "items_changed_idx", Unique: true, Columns: []SnapshotIndexColumn{{Name: "id"}}},
					{Name: "items_added_idx", Columns: []SnapshotIndexColumn{{Name: "extra"}}},
				},
				ForeignKeys: []SnapshotForeignKey{
					{
						Name: "items_changed_fk", Columns: []string{"parent_id"},
						ReferencedTable: "parents", ReferencedColumns: []string{"id"},
						OnDelete: "CASCADE",
					},
					{
						Name: "items_added_fk", Columns: []string{"parent_id"},
						ReferencedTable: "new_parents", ReferencedColumns: []string{"id"},
					},
				},
				Checks: []SnapshotCheckConstraint{
					{Name: "items_changed_check", Expression: `length("name") > 0`},
					{Name: "items_added_check", Expression: `"extra" <> ''`},
				},
			},
		},
	}
	return previous, current
}

func assertDriftEntity(
	t *testing.T,
	facts []SchemaDriftFact,
	change SchemaDriftChangeKind,
	entity SchemaContractEntity,
) {
	t.Helper()
	for _, fact := range facts {
		if fact.ChangeKind == change {
			if fact.Entity != entity {
				t.Fatalf("%s entity = %s, want %s", change, fact.Entity, entity)
			}
			return
		}
	}
	t.Fatalf("missing drift change %s", change)
}

func findDriftFact(
	t *testing.T,
	facts []SchemaDriftFact,
	change SchemaDriftChangeKind,
) SchemaDriftFact {
	t.Helper()
	for _, fact := range facts {
		if fact.ChangeKind == change {
			return fact
		}
	}
	t.Fatalf("missing drift change %s", change)
	return SchemaDriftFact{}
}

func driftCloneSnapshot(t *testing.T, snapshot SchemaSnapshot) SchemaSnapshot {
	t.Helper()
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := ParseSchemaSnapshot(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return cloned
}

func reverseSnapshotTables(values []SnapshotTable) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseSnapshotIndexes(values []SnapshotIndex) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseSnapshotForeignKeys(values []SnapshotForeignKey) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseSnapshotChecks(values []SnapshotCheckConstraint) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
