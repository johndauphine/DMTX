package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestStage4CanonicalTypeMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	length := int64(80)
	precision := int64(18)
	scale := int64(4)
	fractionalZero := int64(0)
	bitWidth := int64(17)
	srid := uint32(4326)
	tables := []Table{
		{
			Schema: "identity",
			Name:   "accounts",
			Columns: []Column{{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &DeclaredType{
					Base: "bigint",
					MySQL: &MySQLTypeMetadata{
						Unsigned: true,
					},
				},
			}},
		},
		{
			Schema: "sales",
			Name:   "events",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				},
				{
					Name: "account_id",
					Type: "numeric",
					DeclaredType: &DeclaredType{
						Base:      "decimal",
						Precision: &precision,
						Scale:     &scale,
						MySQL: &MySQLTypeMetadata{
							Unsigned: true,
							Zerofill: true,
						},
					},
				},
				{
					Name: "label",
					Type: "varchar",
					DeclaredType: &DeclaredType{
						Base:   "varchar",
						Length: &length,
					},
				},
				{
					Name: "observed_at",
					Type: "timestamp",
					DeclaredType: &DeclaredType{
						Base:                      "timestamp",
						FractionalSecondPrecision: &fractionalZero,
					},
				},
				{
					Name: "flags",
					Type: "binary",
					DeclaredType: &DeclaredType{
						Base: "bit",
						MySQL: &MySQLTypeMetadata{
							BitWidth: &bitWidth,
						},
					},
				},
				{
					Name: "status",
					Type: "text",
					DeclaredType: &DeclaredType{
						Base: "enum",
						MySQL: &MySQLTypeMetadata{
							EnumMembers: []string{
								"plain",
								"comma,value",
								`quote'value`,
								`slash\value`,
								"",
							},
						},
					},
				},
				{
					Name: "tags",
					Type: "text",
					DeclaredType: &DeclaredType{
						Base: "set",
						MySQL: &MySQLTypeMetadata{
							SetMembers: []string{
								"alpha",
								`beta,gamma`,
								`quote'value`,
							},
						},
					},
				},
				{
					Name: "position",
					Type: "geometry",
					DeclaredType: &DeclaredType{
						Base: "geometry",
						Spatial: &SpatialTypeMetadata{
							Subtype: SpatialSubtypePoint,
							SRID:    &srid,
						},
					},
				},
			},
			ForeignKeys: []ForeignKey{{
				Name:              "events_account_fk",
				Columns:           []string{"account_id"},
				ReferencedSchema:  "identity",
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
				OnDelete:          "CASCADE",
			}},
		},
	}

	snapshot, err := NewSchemaSnapshot(tables)
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
		t.Fatalf("canonical round trip changed bytes:\n%s\n%s", encoded, reencoded)
	}

	events := parsed.Tables[1]
	if events.Schema != "sales" || events.Name != "events" {
		t.Fatalf("table ordering or identity = %#v", events)
	}
	if events.ForeignKeys[0].ReferencedSchema != "identity" {
		t.Fatalf("referenced schema = %#v", events.ForeignKeys[0])
	}
	if got := events.Columns[3].DeclaredType.FractionalSecondPrecision; got == nil || *got != 0 {
		t.Fatalf("explicit fractional precision = %#v", got)
	}
	if got := events.Columns[5].DeclaredType.MySQL.EnumMembers; !reflect.DeepEqual(got, tables[1].Columns[5].DeclaredType.MySQL.EnumMembers) {
		t.Fatalf("ENUM members = %#v", got)
	}
	if !bytes.Contains(encoded, []byte(`"fractional_second_precision":0`)) ||
		!bytes.Contains(encoded, []byte(`"referenced_schema":"identity"`)) {
		t.Fatalf("structured metadata absent from wire: %s", encoded)
	}
}

func TestStage4CanonicalSpatialMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	zero := uint32(0)
	table := canonicalTypeTestTable(DeclaredType{
		Base: "geography",
		Spatial: &SpatialTypeMetadata{
			Subtype: SpatialSubtypeMultiPolygon,
			SRID:    &zero,
		},
	})
	snapshot, err := NewSchemaSnapshot([]Table{table})
	if err != nil {
		t.Fatalf("build spatial snapshot: %v", err)
	}
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode spatial snapshot: %v", err)
	}
	parsed, err := ParseSchemaSnapshot(encoded)
	if err != nil {
		t.Fatalf("parse spatial snapshot: %v", err)
	}
	spatial := parsed.Tables[0].Columns[0].DeclaredType.Spatial
	if spatial == nil ||
		spatial.Subtype != SpatialSubtypeMultiPolygon ||
		spatial.SRID == nil ||
		*spatial.SRID != 0 {
		t.Fatalf("spatial metadata = %#v", spatial)
	}
}

func TestCanonicalTypeExplicitZeroAndMemberBoundariesAffectDigest(
	t *testing.T,
) {
	t.Parallel()

	unspecified, err := NewSchemaSnapshot([]Table{
		canonicalTypeTestTable(DeclaredType{Base: "timestamp"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	explicitZero, err := NewSchemaSnapshot([]Table{
		canonicalTypeTestTable(DeclaredType{
			Base:                      "timestamp",
			FractionalSecondPrecision: &zero,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	unspecifiedDigest, _ := unspecified.Digest()
	explicitDigest, _ := explicitZero.Digest()
	if unspecifiedDigest == explicitDigest {
		t.Fatalf("unspecified and explicit zero share digest %q", explicitDigest)
	}

	enumLeft, err := NewSchemaSnapshot([]Table{
		canonicalTypeTestTable(DeclaredType{
			Base: "enum",
			MySQL: &MySQLTypeMetadata{
				EnumMembers: []string{"ab", "c"},
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	enumRight, err := NewSchemaSnapshot([]Table{
		canonicalTypeTestTable(DeclaredType{
			Base: "enum",
			MySQL: &MySQLTypeMetadata{
				EnumMembers: []string{"a", "bc"},
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	leftDigest, _ := enumLeft.Digest()
	rightDigest, _ := enumRight.Digest()
	if leftDigest == rightDigest {
		t.Fatalf("member framing collision reused digest %q", leftDigest)
	}

	reordered := canonicalTypeTestTable(DeclaredType{
		Base: "enum",
		MySQL: &MySQLTypeMetadata{
			EnumMembers: []string{"c", "ab"},
		},
	})
	reorderedSnapshot, err := NewSchemaSnapshot([]Table{reordered})
	if err != nil {
		t.Fatal(err)
	}
	reorderedDigest, _ := reorderedSnapshot.Digest()
	if reorderedDigest == leftDigest {
		t.Fatalf("ordered ENUM members reused digest %q", leftDigest)
	}
}

func TestCanonicalTypeLegacyPositionalSnapshotCompatibility(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		base      string
		precision int
	}{
		{base: "datetime2", precision: 7},
		{base: "datetimeoffset", precision: 7},
		{base: "time", precision: 7},
		{base: "datetime", precision: 6},
		{base: "timestamp", precision: 6},
	} {
		test := test
		t.Run(test.base, func(t *testing.T) {
			table := canonicalTypeTestTable(DeclaredType{
				Base:      test.base,
				Arguments: []int{test.precision},
			})
			snapshot, err := NewSchemaSnapshot([]Table{table})
			if err != nil {
				t.Fatalf("legacy declaration rejected: %v", err)
			}
			encoded, err := snapshot.CanonicalJSON()
			if err != nil {
				t.Fatalf("encode legacy declaration: %v", err)
			}
			parsed, err := ParseSchemaSnapshot(encoded)
			if err != nil {
				t.Fatalf("parse legacy declaration: %v", err)
			}
			facts, err := CompareSchemaSnapshots(snapshot, parsed)
			if err != nil {
				t.Fatalf("compare legacy declaration: %v", err)
			}
			if len(facts) != 0 {
				t.Fatalf("legacy round trip drift = %#v", facts)
			}
			want := []byte(
				`"declared_type":{"base":"` + test.base +
					`","arguments":[` +
					fmt.Sprint(test.precision) + `]}`,
			)
			if !bytes.Contains(encoded, want) {
				t.Fatalf("legacy wire shape changed: %s", encoded)
			}
		})
	}
	for _, invalid := range []DeclaredType{
		{Base: "datetime2", Arguments: []int{8}},
		{Base: "datetimeoffset", Arguments: []int{8}},
		{Base: "time", Arguments: []int{8}},
		{Base: "datetime", Arguments: []int{7}},
		{Base: "timestamp", Arguments: []int{7}},
	} {
		snapshot := SchemaSnapshot{
			Version: SchemaSnapshotVersion,
			Tables: []SnapshotTable{{
				Name: "items",
				Columns: []SnapshotColumn{{
					Name: "value",
					Type: "text",
					DeclaredType: &SnapshotDeclaredType{
						Base:      invalid.Base,
						Arguments: invalid.Arguments,
					},
				}},
			}},
		}
		if _, err := CompareSchemaSnapshots(snapshot, snapshot); err == nil {
			t.Fatalf("legacy declaration %#v unexpectedly compared", invalid)
		}
	}
}

func TestCanonicalTypeValidationRejectsMalformedShapes(t *testing.T) {
	t.Parallel()

	one := int64(1)
	zero := int64(0)
	ten := int64(10)
	seven := int64(7)
	sixtyFive := int64(65)
	negativeThousandOne := int64(-1001)
	thousandOne := int64(1001)
	tests := []struct {
		name  string
		value DeclaredType
	}{
		{
			name:  "base contains declaration",
			value: DeclaredType{Base: "varchar(10)"},
		},
		{
			name: "legacy and structured modifiers",
			value: DeclaredType{
				Base: "varchar", Arguments: []int{10}, Length: &ten,
			},
		},
		{
			name:  "zero length",
			value: DeclaredType{Base: "varchar", Length: &zero},
		},
		{
			name:  "scale without precision",
			value: DeclaredType{Base: "decimal", Scale: &one},
		},
		{
			name: "legacy PostgreSQL numeric scale below range",
			value: DeclaredType{
				Base: "numeric", Arguments: []int{2, -1001},
			},
		},
		{
			name: "legacy PostgreSQL numeric scale above range",
			value: DeclaredType{
				Base: "numeric", Arguments: []int{2, 1001},
			},
		},
		{
			name: "structured PostgreSQL numeric scale below range",
			value: DeclaredType{
				Base: "numeric", Precision: &one,
				Scale: &negativeThousandOne,
			},
		},
		{
			name: "structured PostgreSQL numeric scale above range",
			value: DeclaredType{
				Base: "numeric", Precision: &one,
				Scale: &thousandOne,
			},
		},
		{
			name: "contradictory ordinary groups",
			value: DeclaredType{
				Base: "float", Precision: &ten,
				FractionalSecondPrecision: &one,
			},
		},
		{
			name: "fractional precision on non-temporal",
			value: DeclaredType{
				Base: "int", FractionalSecondPrecision: &zero,
			},
		},
		{
			name: "PostgreSQL timestamp precision above six",
			value: DeclaredType{
				Base:                      "timestamp",
				FractionalSecondPrecision: &seven,
			},
		},
		{
			name:  "bare enum",
			value: DeclaredType{Base: "enum"},
		},
		{
			name:  "bare set",
			value: DeclaredType{Base: "set"},
		},
		{
			name: "unknown spatial subtype",
			value: DeclaredType{
				Base: "geometry",
				Spatial: &SpatialTypeMetadata{
					Subtype: SpatialSubtype("triangle"),
				},
			},
		},
		{
			name: "spatial base mismatch",
			value: DeclaredType{
				Base: "point",
				Spatial: &SpatialTypeMetadata{
					Subtype: SpatialSubtypePolygon,
				},
			},
		},
		{
			name: "empty mysql metadata",
			value: DeclaredType{
				Base: "int", MySQL: &MySQLTypeMetadata{},
			},
		},
		{
			name: "zerofill without unsigned",
			value: DeclaredType{
				Base: "int",
				MySQL: &MySQLTypeMetadata{
					Zerofill: true,
				},
			},
		},
		{
			name: "unsigned text",
			value: DeclaredType{
				Base: "varchar",
				MySQL: &MySQLTypeMetadata{
					Unsigned: true,
				},
			},
		},
		{
			name: "tinyint one with wrong base",
			value: DeclaredType{
				Base: "int",
				MySQL: &MySQLTypeMetadata{
					TinyIntOne: true,
				},
			},
		},
		{
			name: "bit width out of range",
			value: DeclaredType{
				Base: "bit",
				MySQL: &MySQLTypeMetadata{
					BitWidth: &sixtyFive,
				},
			},
		},
		{
			name: "enum and set collide",
			value: DeclaredType{
				Base: "enum",
				MySQL: &MySQLTypeMetadata{
					EnumMembers: []string{"a"},
					SetMembers:  []string{"b"},
				},
			},
		},
		{
			name: "duplicate enum member",
			value: DeclaredType{
				Base: "enum",
				MySQL: &MySQLTypeMetadata{
					EnumMembers: []string{"a", "a"},
				},
			},
		},
		{
			name: "invalid enum utf8",
			value: DeclaredType{
				Base: "enum",
				MySQL: &MySQLTypeMetadata{
					EnumMembers: []string{string([]byte{0xff})},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateCatalogType(test.value); err == nil {
				t.Fatalf("ValidateCatalogType(%#v) succeeded", test.value)
			}
			if _, err := NewSchemaSnapshot([]Table{
				canonicalTypeTestTable(test.value),
			}); err == nil {
				t.Fatalf("NewSchemaSnapshot(%#v) succeeded", test.value)
			}
		})
	}
}

func TestCanonicalTypeStructuredTemporalBounds(t *testing.T) {
	t.Parallel()

	seven := int64(7)
	nine := int64(9)
	for _, value := range []DeclaredType{
		{Base: "time", FractionalSecondPrecision: &seven},
		{Base: "datetime2", FractionalSecondPrecision: &seven},
		{Base: "datetimeoffset", FractionalSecondPrecision: &seven},
		{Base: "datetime64", FractionalSecondPrecision: &nine},
	} {
		if err := ValidateCatalogType(value); err != nil {
			t.Fatalf("ValidateCatalogType(%#v): %v", value, err)
		}
	}
}

func TestCanonicalTypePreservesPostgresExtendedNumericModifiers(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		precision int
		scale     int
	}{
		{name: "negative scale", precision: 2, scale: -3},
		{name: "scale exceeds precision", precision: 3, scale: 5},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			legacy := DeclaredType{
				Base:      "numeric",
				Arguments: []int{test.precision, test.scale},
			}
			if err := ValidateCatalogType(legacy); err != nil {
				t.Fatalf("validate legacy PostgreSQL NUMERIC: %v", err)
			}
			snapshot, err := NewSchemaSnapshot([]Table{
				canonicalTypeTestTable(legacy),
			})
			if err != nil {
				t.Fatalf("snapshot legacy PostgreSQL NUMERIC: %v", err)
			}
			encoded, err := snapshot.CanonicalJSON()
			if err != nil {
				t.Fatalf("encode legacy PostgreSQL NUMERIC: %v", err)
			}
			parsed, err := ParseSchemaSnapshot(encoded)
			if err != nil {
				t.Fatalf("parse legacy PostgreSQL NUMERIC: %v", err)
			}
			if !reflect.DeepEqual(
				parsed.Tables[0].Columns[0].DeclaredType.Arguments,
				legacy.Arguments,
			) {
				t.Fatalf(
					"legacy PostgreSQL NUMERIC modifiers = %#v",
					parsed.Tables[0].Columns[0].DeclaredType.Arguments,
				)
			}

			precision := int64(test.precision)
			scale := int64(test.scale)
			structured := DeclaredType{
				Base:      "numeric",
				Precision: &precision,
				Scale:     &scale,
			}
			if err := ValidateCatalogType(structured); err != nil {
				t.Fatalf("validate structured PostgreSQL NUMERIC: %v", err)
			}
			structuredSnapshot, err := NewSchemaSnapshot([]Table{
				canonicalTypeTestTable(structured),
			})
			if err != nil {
				t.Fatalf("snapshot structured PostgreSQL NUMERIC: %v", err)
			}
			got := structuredSnapshot.Tables[0].Columns[0].DeclaredType
			if got.Precision == nil ||
				*got.Precision != precision ||
				got.Scale == nil ||
				*got.Scale != scale {
				t.Fatalf("structured PostgreSQL NUMERIC modifiers = %#v", got)
			}
		})
	}
}

func TestCanonicalTypeSnapshotAndEvolutionClonesDoNotAliasInput(
	t *testing.T,
) {
	t.Parallel()

	length := int64(40)
	bitWidth := int64(8)
	srid := uint32(3857)
	table := canonicalTypeTestTable(DeclaredType{
		Base:   "varchar",
		Length: &length,
	})
	table.Columns = append(table.Columns, Column{
		Name: "shape",
		Type: "geometry",
		DeclaredType: &DeclaredType{
			Base: "geometry",
			Spatial: &SpatialTypeMetadata{
				Subtype: SpatialSubtypeLineString,
				SRID:    &srid,
			},
		},
	})
	table.Columns = append(table.Columns, Column{
		Name: "choice",
		Type: "text",
		DeclaredType: &DeclaredType{
			Base: "enum",
			MySQL: &MySQLTypeMetadata{
				EnumMembers: []string{"a", "b"},
			},
		},
	})
	table.Columns = append(table.Columns, Column{
		Name: "bits",
		Type: "binary",
		DeclaredType: &DeclaredType{
			Base: "bit",
			MySQL: &MySQLTypeMetadata{
				BitWidth: &bitWidth,
			},
		},
	})

	snapshot, err := NewSchemaSnapshot([]Table{table})
	if err != nil {
		t.Fatal(err)
	}
	before, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	clonedColumn := cloneEvolutionColumn(table.Columns[2])

	*table.Columns[0].DeclaredType.Length = 999
	*table.Columns[1].DeclaredType.Spatial.SRID = 0
	table.Columns[2].DeclaredType.MySQL.EnumMembers[0] = "mutated"
	*table.Columns[3].DeclaredType.MySQL.BitWidth = 64
	after, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("snapshot retained an alias into source catalog metadata")
	}
	if got := clonedColumn.DeclaredType.MySQL.EnumMembers[0]; got != "a" {
		t.Fatalf("evolution clone retained member alias: %q", got)
	}
}

func TestCanonicalTypeMetadataParticipatesInDriftAndForeignKeyIdentity(
	t *testing.T,
) {
	t.Parallel()

	previous := canonicalTypeTestTable(DeclaredType{Base: "timestamp"})
	zero := int64(0)
	current := canonicalTypeTestTable(DeclaredType{
		Base:                      "timestamp",
		FractionalSecondPrecision: &zero,
	})
	previousSnapshot, err := NewSchemaSnapshot([]Table{previous})
	if err != nil {
		t.Fatal(err)
	}
	currentSnapshot, err := NewSchemaSnapshot([]Table{current})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := CompareSchemaSnapshots(previousSnapshot, currentSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 ||
		facts[0].ChangeKind != SchemaDriftDataTypeChanged {
		t.Fatalf("canonical type drift facts = %#v", facts)
	}

	previousFK := snapshotTestChildTable()
	currentFK := snapshotTestChildTable()
	currentFK.ForeignKeys[0].ReferencedSchema = "archive"
	previousSnapshot, err = NewSchemaSnapshot([]Table{previousFK})
	if err != nil {
		t.Fatal(err)
	}
	currentSnapshot, err = NewSchemaSnapshot([]Table{currentFK})
	if err != nil {
		t.Fatal(err)
	}
	facts, err = CompareSchemaSnapshots(previousSnapshot, currentSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 ||
		facts[0].ChangeKind != SchemaDriftForeignKeyChanged {
		t.Fatalf("referenced-schema drift facts = %#v", facts)
	}
}

func TestEvolutionCatalogResolvesExplicitReferencedSchema(t *testing.T) {
	t.Parallel()

	parent := Table{
		Schema: "identity",
		Name:   "accounts",
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
	}
	child := Table{
		Schema: "sales",
		Name:   "events",
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name:         "account_id",
				Type:         "bigint",
				DeclaredType: &DeclaredType{Base: "bigint"},
			},
		},
		ForeignKeys: []ForeignKey{{
			Name:              "events_account_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "identity",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		}},
	}
	if _, err := NewCompleteEvolutionCatalog([]Table{parent, child}); err != nil {
		t.Fatalf("explicit cross-schema catalog rejected: %v", err)
	}

	child.ForeignKeys[0].ReferencedSchema = ""
	if _, err := NewCompleteEvolutionCatalog([]Table{parent, child}); err == nil ||
		!strings.Contains(err.Error(), "missing or ambiguous referenced table") {
		t.Fatalf("unqualified cross-schema relation error = %v", err)
	}
}

func TestCanonicalTypeSnapshotJSONRejectsUnknownNestedFields(t *testing.T) {
	t.Parallel()

	width := int64(4)
	snapshot, err := NewSchemaSnapshot([]Table{
		canonicalTypeTestTable(DeclaredType{
			Base: "bit",
			MySQL: &MySQLTypeMetadata{
				BitWidth: &width,
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	tables := wire["tables"].([]any)
	table := tables[0].(map[string]any)
	columns := table["columns"].([]any)
	column := columns[0].(map[string]any)
	declared := column["declared_type"].(map[string]any)
	mysql := declared["mysql"].(map[string]any)
	mysql["future_shape"] = true
	unknown, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseSchemaSnapshot(unknown); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown nested field error = %v", err)
	}
}

func canonicalTypeTestTable(declared DeclaredType) Table {
	return Table{
		Schema: "public",
		Name:   "canonical_types",
		Columns: []Column{{
			Name:         "value",
			Type:         "text",
			Nullable:     true,
			DeclaredType: &declared,
		}},
	}
}
