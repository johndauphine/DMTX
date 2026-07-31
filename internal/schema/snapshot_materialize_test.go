package schema

import (
	"bytes"
	"strings"
	"testing"
)

func TestMaterializeSchemaSnapshotExactRichRoundTrip(t *testing.T) {
	t.Parallel()

	tables := snapshotMaterializationFixture(t)
	snapshot, err := NewSchemaSnapshot(tables)
	if err != nil {
		t.Fatalf("build fixture snapshot: %v", err)
	}
	before, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode fixture snapshot: %v", err)
	}
	beforeDigest, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest fixture snapshot: %v", err)
	}

	materialized, err := MaterializeSchemaSnapshot(snapshot)
	if err != nil {
		t.Fatalf("materialize fixture snapshot: %v", err)
	}
	if len(materialized) != len(tables) {
		t.Fatalf(
			"materialized tables = %d, want %d",
			len(materialized),
			len(tables),
		)
	}
	if materialized[0].Schema != "analytics" ||
		materialized[0].Name != "events" ||
		materialized[1].Schema != "identity" ||
		materialized[1].Name != "accounts" ||
		materialized[2].Schema != "local" ||
		materialized[2].Name != "audit" {
		t.Fatalf(
			"materialized canonical table order = %s.%s, %s.%s, %s.%s",
			materialized[0].Schema,
			materialized[0].Name,
			materialized[1].Schema,
			materialized[1].Name,
			materialized[2].Schema,
			materialized[2].Name,
		)
	}
	if materialized[1].Identity == nil ||
		materialized[1].Identity.Frontier != nil {
		t.Fatalf(
			"materialized identity frontier = %#v, want nil",
			materialized[1].Identity,
		)
	}
	if !materialized[2].SQLiteStrict ||
		!materialized[2].SQLiteWithoutRowID {
		t.Fatalf(
			"materialized SQLite table flags = strict:%t without-rowid:%t",
			materialized[2].SQLiteStrict,
			materialized[2].SQLiteWithoutRowID,
		)
	}

	roundTrip, err := NewSchemaSnapshot(materialized)
	if err != nil {
		t.Fatalf("rebuild materialized snapshot: %v", err)
	}
	equal, err := SchemaSnapshotsEqual(snapshot, roundTrip)
	if err != nil {
		t.Fatalf("compare materialized snapshot: %v", err)
	}
	if !equal {
		t.Fatal("materialized snapshot is not exactly equal")
	}
	after, err := roundTrip.CanonicalJSON()
	if err != nil {
		t.Fatalf("encode materialized snapshot: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf(
			"canonical round trip changed:\nbefore %s\nafter  %s",
			before,
			after,
		)
	}
	afterDigest, err := roundTrip.Digest()
	if err != nil {
		t.Fatalf("digest materialized snapshot: %v", err)
	}
	if afterDigest != beforeDigest {
		t.Fatalf(
			"materialized digest = %s, want %s",
			afterDigest,
			beforeDigest,
		)
	}

	// The returned model must not retain aliases into durable snapshot state.
	materialized[0].Columns[0].Name = "mutated"
	*materialized[0].Columns[2].DeclaredType.Spatial.SRID = 0
	materialized[0].Columns[4].
		DeclaredType.MySQL.EnumMembers[0] = "mutated"
	materialized[0].Indexes[0].Columns[0].Name = "mutated"
	materialized[0].ForeignKeys[0].Columns[0] = "mutated"
	unchanged, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatalf("re-encode source snapshot: %v", err)
	}
	if !bytes.Equal(before, unchanged) {
		t.Fatal("materialized tables alias durable snapshot state")
	}
}

func TestMaterializeSchemaSnapshotAcceptsEmptyEvidence(t *testing.T) {
	t.Parallel()

	tables, err := MaterializeSchemaSnapshot(SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables:  []SnapshotTable{},
	})
	if err != nil {
		t.Fatalf("materialize empty snapshot: %v", err)
	}
	if tables == nil || len(tables) != 0 {
		t.Fatalf("materialized empty tables = %#v", tables)
	}
}

func TestMaterializeSchemaSnapshotRejectsMalformedEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*SchemaSnapshot)
		want   string
	}{
		{
			name: "unsupported version",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Version++
			},
			want: "unsupported schema snapshot version",
		},
		{
			name: "non canonical type",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Columns[0].Type = " bigint "
			},
			want: "column type is empty or non-canonical",
		},
		{
			name: "blank default",
			mutate: func(snapshot *SchemaSnapshot) {
				value := " "
				snapshot.Tables[0].Columns[0].Default = &value
			},
			want: "default is not canonical",
		},
		{
			name: "executable default",
			mutate: func(snapshot *SchemaSnapshot) {
				value := "now()"
				snapshot.Tables[0].Columns[0].Default = &value
			},
			want: "unsupported scalar",
		},
		{
			name: "non canonical default keyword",
			mutate: func(snapshot *SchemaSnapshot) {
				value := "true"
				snapshot.Tables[0].Columns[0].Default = &value
			},
			want: "canonical scalar form",
		},
		{
			name: "non canonical Postgres escape",
			mutate: func(snapshot *SchemaSnapshot) {
				value := `E'unsafe\nescape'`
				snapshot.Tables[0].Columns[0].Default = &value
			},
			want: "non-canonical escape",
		},
		{
			name: "incomplete check grammar",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Checks[0].Expression = `"id" >`
			},
			want: "parse canonical CHECK",
		},
		{
			name: "executable check",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Checks[0].Expression =
					`"id" > 0; DROP TABLE accounts`
			},
			want: "parse canonical CHECK",
		},
		{
			name: "check missing column",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Checks[0].Expression =
					`"missing" > 0`
			},
			want: "unknown source column",
		},
		{
			name: "primary key gap",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Columns[0].
					PrimaryKeyPosition = 2
			},
			want: "primary-key positions are not contiguous",
		},
		{
			name: "identity missing column",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[1].Identity.Column = "missing"
			},
			want: "identity references unknown column",
		},
		{
			name: "index missing column",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Indexes[0].
					Columns[0].Name = "missing"
			},
			want: "unknown or case-aliased index member",
		},
		{
			name: "duplicate index member",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].Indexes[0].Columns = append(
					snapshot.Tables[0].Indexes[0].Columns,
					snapshot.Tables[0].Indexes[0].Columns[0],
				)
			},
			want: "duplicate index member",
		},
		{
			name: "duplicate index name",
			mutate: func(snapshot *SchemaSnapshot) {
				duplicate := snapshot.Tables[0].Indexes[0]
				duplicate.Columns = []SnapshotIndexColumn{{
					Name: "account_id",
				}}
				snapshot.Tables[0].Indexes = append(
					snapshot.Tables[0].Indexes,
					duplicate,
				)
			},
			want: "case-aliased index names",
		},
		{
			name: "foreign key missing local column",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					Columns[0] = "missing"
			},
			want: "unknown or case-aliased foreign-key owner member",
		},
		{
			name: "foreign key missing table",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					ReferencedTable = "missing"
			},
			want: "missing or ambiguous referenced table",
		},
		{
			name: "foreign key missing referenced column",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					ReferencedColumns[0] = "missing"
			},
			want: "unknown or case-aliased foreign-key referenced member",
		},
		{
			name: "foreign key mismatched members",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					ReferencedColumns = nil
			},
			want: "mismatched foreign-key member counts",
		},
		{
			name: "foreign key invalid action",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					OnDelete = "EXECUTE"
			},
			want: "invalid foreign-key actions",
		},
		{
			name: "foreign key non unique reference",
			mutate: func(snapshot *SchemaSnapshot) {
				snapshot.Tables[0].ForeignKeys[0].
					ReferencedColumns[0] = "display_name"
				snapshot.Tables[1].Indexes[0].Unique = false
			},
			want: "does not reference a proven unique key",
		},
		{
			name: "ambiguous case aliased table",
			mutate: func(snapshot *SchemaSnapshot) {
				duplicate := snapshot.Tables[1]
				duplicate.Name = "Accounts"
				snapshot.Tables = append(snapshot.Tables, duplicate)
			},
			want: "case-aliased table",
		},
		{
			name: "invalid declared metadata",
			mutate: func(snapshot *SchemaSnapshot) {
				zero := int64(0)
				snapshot.Tables[0].Columns[3].
					DeclaredType.Length = &zero
			},
			want: "invalid length",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot, err := NewSchemaSnapshot(
				snapshotMaterializationFixture(t),
			)
			if err != nil {
				t.Fatalf("build fixture snapshot: %v", err)
			}
			test.mutate(&snapshot)
			if materialized, err := MaterializeSchemaSnapshot(
				snapshot,
			); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"materialized = %#v, error = %v, want %q",
					materialized,
					err,
					test.want,
				)
			}
		})
	}
}

func snapshotMaterializationFixture(t *testing.T) []Table {
	t.Helper()

	frontier := int64(99)
	srid := uint32(4326)
	bitWidth := int64(16)
	nameLength := int64(80)
	statusLength := int64(16)
	precision := int64(14)
	scale := int64(3)
	fsp := int64(6)

	accountNameDefault := &Expression{
		sql:     postgresStringLiteral(`O'Brien\primary`),
		kind:    expressionString,
		literal: `O'Brien\primary`,
	}
	activeDefault, err := ParseSQLiteDefault("TRUE")
	if err != nil {
		t.Fatalf("parse active default: %v", err)
	}
	statusDefault, err := ParseSQLiteDefault("'active'")
	if err != nil {
		t.Fatalf("parse status default: %v", err)
	}
	amountDefault, err := ParseSQLiteDefault("1.5")
	if err != nil {
		t.Fatalf("parse amount default: %v", err)
	}
	payloadDefault, err := ParseSQLiteDefault("X'00ff'")
	if err != nil {
		t.Fatalf("parse payload default: %v", err)
	}
	timeDefault, err := ParseSQLiteDefault("CURRENT_TIMESTAMP")
	if err != nil {
		t.Fatalf("parse timestamp default: %v", err)
	}
	idCheck, err := ParseSQLiteCheckExpression(`"id" > 0`)
	if err != nil {
		t.Fatalf("parse id CHECK: %v", err)
	}
	statusCheck, err := ParseSQLiteCheckExpression(
		`"status" IN ('active', 'disabled')`,
	)
	if err != nil {
		t.Fatalf("parse status CHECK: %v", err)
	}

	accounts := Table{
		Schema:         "identity",
		Name:           "accounts",
		MySQLCollation: "utf8mb4_0900_bin",
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
				Name:     "display_name",
				Type:     "varchar",
				Nullable: true,
				DeclaredType: &DeclaredType{
					Base:   "varchar",
					Length: &nameLength,
				},
				Default: accountNameDefault,
			},
		},
		Indexes: []Index{{
			Name:   "accounts_display_name_uq",
			Unique: true,
			Inline: true,
			Columns: []IndexColumn{{
				Name:      "display_name",
				Collation: "BINARY",
			}},
		}},
	}

	events := Table{
		Schema:            "analytics",
		Name:              "events",
		MySQLCollation:    "utf8mb4_nopad_bin",
		ClickHouseOrderBy: []string{"id", "account_id"},
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
			{
				Name: "shape",
				Type: "geometry",
				DeclaredType: &DeclaredType{
					Base: "geometry",
					Spatial: &SpatialTypeMetadata{
						Subtype: SpatialSubtypePoint,
						SRID:    &srid,
					},
				},
				Nullable: true,
			},
			{
				Name: "status",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:   "varchar",
					Length: &statusLength,
				},
				Default: statusDefault,
			},
			{
				Name: "category",
				Type: "text",
				DeclaredType: &DeclaredType{
					Base: "enum",
					MySQL: &MySQLTypeMetadata{
						EnumMembers: []string{
							"active",
							"disabled",
						},
					},
				},
				Nullable: true,
			},
			{
				Name: "active",
				Type: "boolean",
				DeclaredType: &DeclaredType{
					Base: "tinyint",
					MySQL: &MySQLTypeMetadata{
						TinyIntOne: true,
					},
				},
				Default: activeDefault,
			},
			{
				Name: "unsigned_counter",
				Type: "bigint",
				DeclaredType: &DeclaredType{
					Base: "bigint",
					MySQL: &MySQLTypeMetadata{
						Unsigned: true,
						Zerofill: true,
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
							"red",
							"green",
							"blue",
						},
					},
				},
				Nullable: true,
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "numeric",
					Precision: &precision,
					Scale:     &scale,
				},
				Default: amountDefault,
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
				Name:         "payload",
				Type:         "blob",
				Nullable:     true,
				DeclaredType: &DeclaredType{Base: "blob"},
				Default:      payloadDefault,
			},
			{
				Name: "recorded_at",
				Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base:                      "timestamp",
					FractionalSecondPrecision: &fsp,
				},
				Default: timeDefault,
			},
		},
		Indexes: []Index{
			{
				Name:   "events_account_status_uq",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "account_id"},
					{Name: "status", Descending: true},
				},
			},
			{
				Name: "events_amount_idx",
				Columns: []IndexColumn{{
					Name:       "amount",
					Descending: true,
				}},
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
			Match:             "SIMPLE",
		}},
		Checks: []CheckConstraint{
			{Name: "events_id_positive", Expression: idCheck},
			{Name: "events_status_domain", Expression: statusCheck},
		},
	}

	audit := Table{
		Schema: "local",
		Name:   "audit",
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name:     "code",
				Type:     "varchar",
				Nullable: true,
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{32},
				},
			},
		},
		SQLiteWithoutRowID: true,
		SQLiteStrict:       true,
	}

	return []Table{accounts, events, audit}
}
