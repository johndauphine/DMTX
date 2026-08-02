package migrate

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSchemaContractOmittedReportsUnlessHardGate(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns,
		schema.SnapshotColumn{Name: "optional", Type: "text", Nullable: true},
	)

	reportPlan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{TargetMode: "upsert"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(reportPlan.Decisions) == 0 {
		t.Fatal("drift produced no decisions")
	}
	for _, decision := range reportPlan.Decisions {
		if decision.Mode != config.SchemaContractReport ||
			decision.Action != SchemaContractReport ||
			decision.Reason == "" {
			t.Fatalf("omitted-contract decision = %#v", decision)
		}
	}
	assertSchemaContractSnapshotEqual(
		t,
		reportPlan.UpsertSnapshot,
		previous,
		"report upsert projection",
	)
	assertSchemaContractSnapshotEqual(
		t,
		reportPlan.RebuildSnapshot,
		current,
		"report rebuild projection",
	)

	gatedPlan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			TargetMode:        "upsert",
			FailOnSchemaDrift: true,
		},
	)
	assertSchemaContractErrorKind(t, err, SchemaContractDriftBlocked)
	for _, decision := range gatedPlan.Decisions {
		if decision.Action != SchemaContractAbort {
			t.Fatalf("hard-gate decision = %#v", decision)
		}
	}
}

func TestSchemaContractFreezeAndReportNeverProjectUpsertMutation(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns[1].Nullable = true

	report, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractReport),
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSchemaContractSnapshotEqual(
		t,
		report.UpsertSnapshot,
		previous,
		"explicit report projection",
	)
	if report.Decisions[0].Action != SchemaContractReport {
		t.Fatalf("report action = %q", report.Decisions[0].Action)
	}

	frozen, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractFreeze),
			TargetMode: "upsert",
		},
	)
	assertSchemaContractErrorKind(t, err, SchemaContractDriftBlocked)
	assertSchemaContractSnapshotEqual(
		t,
		frozen.UpsertSnapshot,
		previous,
		"freeze projection",
	)
	if frozen.Decisions[0].Action != SchemaContractAbort {
		t.Fatalf("freeze action = %q", frozen.Decisions[0].Action)
	}
}

func TestSchemaContractEvolveSafeUpsertChanges(t *testing.T) {
	t.Parallel()

	literal := "'ready'"
	tests := []struct {
		name       string
		change     func(*schema.SchemaSnapshot)
		wantAction SchemaContractAction
	}{
		{
			name: "new table",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables = append(current.Tables, schema.SnapshotTable{
					Schema: "public",
					Name:   "new_table",
					Columns: []schema.SnapshotColumn{{
						Name: "id", Type: "bigint",
						PrimaryKey: true, PrimaryKeyPosition: 1,
					}},
				})
			},
			wantAction: SchemaContractCreateTable,
		},
		{
			name: "nullable column without default",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns = append(
					current.Tables[0].Columns,
					schema.SnapshotColumn{
						Name: "optional", Type: "text", Nullable: true,
					},
				)
			},
			wantAction: SchemaContractAddColumn,
		},
		{
			name: "nullable column with literal default",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns = append(
					current.Tables[0].Columns,
					schema.SnapshotColumn{
						Name: "state", Type: "text", Nullable: true,
						Default: &literal,
					},
				)
			},
			wantAction: SchemaContractAddColumn,
		},
		{
			name: "nullability relaxation",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1].Nullable = true
			},
			wantAction: SchemaContractRelaxNullability,
		},
		{
			name: "varchar widening",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1].DeclaredType.Arguments[0] = 80
			},
			wantAction: SchemaContractWidenType,
		},
		{
			name: "numeric widening",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[2].DeclaredType.Arguments =
					[]int{18, 4}
			},
			wantAction: SchemaContractWidenType,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			test.change(&current)
			plan, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract:   schemaContractAll(config.SchemaContractEvolve),
					TargetMode: "upsert",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !schemaContractHasAction(plan.Decisions, test.wantAction) {
				t.Fatalf(
					"actions = %v, want %q",
					schemaContractActions(plan.Decisions),
					test.wantAction,
				)
			}
			assertSchemaContractSnapshotEqual(
				t,
				plan.UpsertSnapshot,
				current,
				"safe evolve projection",
			)
		})
	}
}

func TestSchemaContractEvolveSafeTypeWideningMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous schema.SnapshotColumn
		current  schema.SnapshotColumn
		want     bool
	}{
		{
			name: "integer to bigint",
			previous: schema.SnapshotColumn{
				Type: "integer",
			},
			current: schema.SnapshotColumn{Type: "bigint"},
			want:    true,
		},
		{
			name: "mysql smallint to int",
			previous: schema.SnapshotColumn{
				Type: "integer",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "smallint",
				},
			},
			current: schema.SnapshotColumn{
				Type:         "integer",
				DeclaredType: &schema.SnapshotDeclaredType{Base: "int"},
			},
			want: true,
		},
		{
			name: "varchar length",
			previous: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{21},
				},
			},
			want: true,
		},
		{
			name: "decimal preserves integer and fractional capacity",
			previous: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "decimal", Arguments: []int{12, 2},
				},
			},
			current: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "decimal", Arguments: []int{16, 4},
				},
			},
			want: true,
		},
		{
			name: "timestamp precision",
			previous: schema.SnapshotColumn{
				Type: "timestamp",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "timestamp", Arguments: []int{3},
				},
			},
			current: schema.SnapshotColumn{
				Type: "timestamp",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
			want: true,
		},
		{
			name: "float32 to float64",
			previous: schema.SnapshotColumn{
				Type: "real",
			},
			current: schema.SnapshotColumn{Type: "double precision"},
			want:    true,
		},
		{
			name: "mysql text capacity",
			previous: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "text",
				},
			},
			current: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "longtext",
				},
			},
			want: true,
		},
		{
			name: "integer narrowing",
			previous: schema.SnapshotColumn{
				Type: "bigint",
			},
			current: schema.SnapshotColumn{Type: "integer"},
		},
		{
			name: "varchar narrowing",
			previous: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{80},
				},
			},
			current: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "decimal loses integer capacity",
			previous: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "numeric", Arguments: []int{12, 2},
				},
			},
			current: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "numeric", Arguments: []int{12, 4},
				},
			},
		},
		{
			name: "char length is not proven lossless",
			previous: schema.SnapshotColumn{
				Type: "char",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "char", Arguments: []int{20},
				},
			},
			current: schema.SnapshotColumn{
				Type: "char",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "char", Arguments: []int{40},
				},
			},
		},
		{
			name: "unrelated types",
			previous: schema.SnapshotColumn{
				Type: "text",
			},
			current: schema.SnapshotColumn{Type: "json"},
		},
		{
			name: "declared widening contradicts generic type",
			previous: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "smallint",
				},
			},
			current: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "int",
				},
			},
		},
		{
			name: "generic change contradicts declared widening",
			previous: schema.SnapshotColumn{
				Type: "integer",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "smallint",
				},
			},
			current: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "int",
				},
			},
		},
		{
			name: "generic widening with stale declaration",
			previous: schema.SnapshotColumn{
				Type: "integer",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "int",
				},
			},
			current: schema.SnapshotColumn{
				Type: "bigint",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "int",
				},
			},
		},
		{
			name: "one-sided declaration is not corroboration",
			previous: schema.SnapshotColumn{
				Type: "integer",
			},
			current: schema.SnapshotColumn{
				Type: "bigint",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "bigint",
				},
			},
		},
		{
			name: "evidence channels widen different text dimensions",
			previous: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: schema.SnapshotColumn{
				Type: "text",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "malformed variable declaration has no prior bound",
			previous: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar",
				},
			},
			current: schema.SnapshotColumn{
				Type: "varchar",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "varchar", Arguments: []int{80},
				},
			},
		},
		{
			name: "malformed numeric declaration loses capacity proof",
			previous: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "numeric", Arguments: []int{12, 2},
				},
			},
			current: schema.SnapshotColumn{
				Type: "numeric",
				DeclaredType: &schema.SnapshotDeclaredType{
					Base: "numeric", Arguments: []int{10, 20},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := safeSchemaContractTypeWidening(
				test.previous,
				test.current,
			); got != test.want {
				t.Fatalf("safe widening = %t, want %t", got, test.want)
			}
		})
	}
}

func TestSchemaContractEvolveRejectsUnsafeUpsertChanges(t *testing.T) {
	t.Parallel()

	volatileDefault := "CURRENT_TIMESTAMP"
	changedDefault := "'changed'"
	tests := []struct {
		name   string
		change func(*schema.SchemaSnapshot)
	}{
		{
			name: "non-null added column",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns = append(
					current.Tables[0].Columns,
					schema.SnapshotColumn{Name: "required", Type: "text"},
				)
			},
		},
		{
			name: "volatile added default",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns = append(
					current.Tables[0].Columns,
					schema.SnapshotColumn{
						Name: "created_at", Type: "timestamp", Nullable: true,
						Default: &volatileDefault,
					},
				)
			},
		},
		{
			name: "nullability tightening",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[3].Nullable = false
			},
		},
		{
			name: "type narrowing",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1].DeclaredType.Arguments[0] = 10
			},
		},
		{
			name: "default drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1].Default = &changedDefault
			},
		},
		{
			name: "primary key drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[0].PrimaryKey = false
				current.Tables[0].Columns[0].PrimaryKeyPosition = 0
				current.Tables[0].Columns[1].PrimaryKey = true
				current.Tables[0].Columns[1].PrimaryKeyPosition = 1
			},
		},
		{
			name: "identity drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Identity = &schema.SnapshotIdentity{
					Column:     "id",
					Generation: schema.IdentityByDefault,
				}
			},
		},
		{
			name: "index drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Indexes = append(
					current.Tables[0].Indexes,
					schema.SnapshotIndex{
						Name: "accounts_parent_code",
						Columns: []schema.SnapshotIndexColumn{{
							Name: "parent_code",
						}},
					},
				)
			},
		},
		{
			name: "table option drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].MySQLCollation = "utf8mb4_bin"
			},
		},
		{
			name: "existing column reorder",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1], current.Tables[0].Columns[2] =
					current.Tables[0].Columns[2], current.Tables[0].Columns[1]
			},
		},
		{
			name: "type widening coupled to default drift",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns[1].DeclaredType.Arguments[0] = 80
				current.Tables[0].Columns[1].Default = &changedDefault
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			test.change(&current)
			plan, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract:   schemaContractAll(config.SchemaContractEvolve),
					TargetMode: "upsert",
				},
			)
			assertSchemaContractErrorKind(
				t,
				err,
				SchemaContractUnsafeEvolution,
			)
			if !schemaContractHasAction(
				plan.Decisions,
				SchemaContractAbort,
			) {
				t.Fatalf("unsafe actions = %v", schemaContractActions(plan.Decisions))
			}
		})
	}
}

func TestSchemaContractEvolveRejectsInconsistentTypeEvidence(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	previous.Tables[0].Columns[1].Type = "text"
	previous.Tables[0].Columns[1].DeclaredType =
		&schema.SnapshotDeclaredType{Base: "smallint"}
	current.Tables[0].Columns[1].Type = "text"
	current.Tables[0].Columns[1].DeclaredType =
		&schema.SnapshotDeclaredType{Base: "int"}

	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractEvolve),
			TargetMode: "upsert",
		},
	)
	assertSchemaContractErrorKind(
		t,
		err,
		SchemaContractUnsafeEvolution,
	)
	typeDecision := schemaContractDecisionByKind(
		t,
		plan.Decisions,
		schema.SchemaDriftDataTypeChanged,
	)
	if typeDecision.Action != SchemaContractAbort ||
		!strings.Contains(typeDecision.Reason, "ambiguous") {
		t.Fatalf("inconsistent type decision = %#v", typeDecision)
	}
}

func TestSchemaContractRebuildUsesCurrentShapeSeparatelyFromUpsert(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns,
		schema.SnapshotColumn{Name: "optional", Type: "text", Nullable: true},
	)
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractEvolve),
			TargetMode: "drop_recreate",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range plan.Decisions {
		if decision.Action != SchemaContractRebuildCurrentShape {
			t.Fatalf("rebuild decision = %#v", decision)
		}
	}
	assertSchemaContractSnapshotEqual(
		t,
		plan.RebuildSnapshot,
		current,
		"rebuild projection",
	)
	assertSchemaContractSnapshotEqual(
		t,
		plan.UpsertSnapshot,
		previous,
		"separate upsert projection",
	)
}

func TestSchemaContractSourceDropsRetainUpsertTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(*schema.SchemaSnapshot)
	}{
		{
			name: "table",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables = current.Tables[:1]
			},
		},
		{
			name: "column",
			change: func(current *schema.SchemaSnapshot) {
				current.Tables[0].Columns =
					append(
						current.Tables[0].Columns[:3],
						current.Tables[0].Columns[4:]...,
					)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			test.change(&current)
			plan, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract:   schemaContractAll(config.SchemaContractEvolve),
					TargetMode: "upsert",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !schemaContractHasAction(
				plan.Decisions,
				SchemaContractRetainTarget,
			) {
				t.Fatalf("source-drop actions = %v", schemaContractActions(plan.Decisions))
			}
			assertSchemaContractSnapshotEqual(
				t,
				plan.UpsertSnapshot,
				previous,
				"source-drop upsert target",
			)
			assertSchemaContractSnapshotEqual(
				t,
				plan.RebuildSnapshot,
				previous,
				"source-drop rebuild retained target shape",
			)
		})
	}
}

func TestSchemaContractRebuildRetainsDropsWhileUsingCurrentNonDropShape(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns[:1],
		current.Tables[0].Columns[2:]...,
	)
	current.Tables[0].Columns[1].DeclaredType.Arguments = []int{18, 4}
	current.Tables[0].Indexes = current.Tables[0].Indexes[1:]
	current.Tables[0].Checks = current.Tables[0].Checks[1:]
	current.Tables[1].ForeignKeys = nil

	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractEvolve),
			TargetMode: "drop_recreate",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !schemaContractHasAction(
		plan.Decisions,
		SchemaContractRetainTarget,
	) || !schemaContractHasAction(
		plan.Decisions,
		SchemaContractRebuildCurrentShape,
	) {
		t.Fatalf("mixed rebuild actions = %v", schemaContractActions(plan.Decisions))
	}
	expected := cloneSchemaContractFixture(t, previous)
	expected.Tables[0].Columns[2].DeclaredType.Arguments = []int{18, 4}
	assertSchemaContractSnapshotEqual(
		t,
		plan.RebuildSnapshot,
		expected,
		"mixed rebuild projection",
	)
	assertSchemaContractSnapshotEqual(
		t,
		plan.TransferSnapshot,
		current,
		"mixed transfer projection",
	)
}

func TestSchemaContractRebuildDoesNotRestoreCheckAcrossDiscardedColumn(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	previous.Tables[0].Checks[0].Expression =
		`"label" <> '' AND "updated_at" IS NOT NULL`
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns[:1],
		current.Tables[0].Columns[2:]...,
	)
	current.Tables[0].Columns[2].DeclaredType.Arguments = []int{6}
	current.Tables[0].Indexes = current.Tables[0].Indexes[1:]
	current.Tables[0].Checks = current.Tables[0].Checks[1:]
	current.Tables[1].ForeignKeys = nil

	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractDiscardValue,
			},
			TargetMode: "drop_recreate",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	accounts := mustSchemaContractTable(
		t,
		plan.RebuildSnapshot,
		"public",
		"accounts",
	)
	if !schemaContractSnapshotHasColumn(accounts, "label") {
		t.Fatal("rebuild did not retain the source-dropped label")
	}
	if schemaContractSnapshotHasColumn(accounts, "updated_at") {
		t.Fatal("rebuild restored separately discarded updated_at")
	}
	for _, check := range accounts.Checks {
		if check.Name == "accounts_label_check" {
			t.Fatalf("rebuild restored mixed excluded CHECK %#v", check)
		}
	}
}

func TestSchemaContractRebuildDoesNotRestoreInboundCompositeFKAcrossDiscard(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	previous.Tables[1].Columns = append(
		previous.Tables[1].Columns,
		schema.SnapshotColumn{
			Name: "account_parent_code", Type: "text", Nullable: true,
		},
	)
	previous.Tables[1].ForeignKeys[0].Columns =
		[]string{"account_label", "account_parent_code"}
	previous.Tables[1].ForeignKeys[0].ReferencedColumns =
		[]string{"label", "parent_code"}
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns[:1],
		current.Tables[0].Columns[2:]...,
	)
	current.Tables[0].Columns[3].Type = "json"
	current.Tables[0].Indexes = current.Tables[0].Indexes[1:]
	current.Tables[0].Checks = current.Tables[0].Checks[1:]
	current.Tables[1].ForeignKeys = nil

	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractDiscardValue,
			},
			TargetMode: "drop_recreate",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	accounts := mustSchemaContractTable(
		t,
		plan.RebuildSnapshot,
		"public",
		"accounts",
	)
	if !schemaContractSnapshotHasColumn(accounts, "label") {
		t.Fatal("rebuild did not retain the source-dropped label")
	}
	if schemaContractSnapshotHasColumn(accounts, "parent_code") {
		t.Fatal("rebuild restored separately discarded parent_code")
	}
	children := mustSchemaContractTable(
		t,
		plan.RebuildSnapshot,
		"public",
		"children",
	)
	if len(children.ForeignKeys) != 0 {
		t.Fatalf("rebuild restored mixed inbound composite FK: %#v", children.ForeignKeys)
	}
}

func TestSchemaContractRebuildPrunesDependenciesFromRestoredWholeTable(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	previous.Tables[1].ForeignKeys[0].ReferencedColumns =
		[]string{"parent_code"}
	current := cloneSchemaContractFixture(t, previous)
	current.Tables = current.Tables[:1]
	current.Tables[0].Columns[4].Type = "json"

	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractDiscardValue,
			},
			TargetMode: "drop_recreate",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	accounts := mustSchemaContractTable(
		t,
		plan.RebuildSnapshot,
		"public",
		"accounts",
	)
	if schemaContractSnapshotHasColumn(accounts, "parent_code") {
		t.Fatal("rebuild retained separately discarded parent_code")
	}
	children := mustSchemaContractTable(
		t,
		plan.RebuildSnapshot,
		"public",
		"children",
	)
	if len(children.ForeignKeys) != 0 {
		t.Fatalf("whole-table restoration revived excluded FK: %#v", children.ForeignKeys)
	}
}

func TestSchemaContractReportRebuildProjectionRetainsSourceDrops(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		contract *config.SchemaContract
	}{
		{name: "omitted"},
		{
			name:     "explicit report",
			contract: schemaContractAll(config.SchemaContractReport),
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			current.Tables[0].Columns = append(
				current.Tables[0].Columns[:3],
				current.Tables[0].Columns[4:]...,
			)
			plan, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract:   test.contract,
					TargetMode: "drop_recreate",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			assertSchemaContractSnapshotEqual(
				t,
				plan.RebuildSnapshot,
				previous,
				"report rebuild retained target",
			)
		})
	}
}

func TestSchemaContractSourceColumnDropDoesNotBecomeDiscardRowOrValue(
	t *testing.T,
) {
	t.Parallel()

	for _, mode := range []config.SchemaContractMode{
		config.SchemaContractEvolve,
		config.SchemaContractDiscardRow,
		config.SchemaContractDiscardValue,
		config.SchemaContractReport,
	} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			current.Tables[0].Columns = append(
				current.Tables[0].Columns[:3],
				current.Tables[0].Columns[4:]...,
			)
			contract := schemaContractAll(config.SchemaContractReport)
			contract.Columns = mode
			plan, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract:   contract,
					TargetMode: "upsert",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, decision := range plan.Decisions {
				if decision.Entity == schema.SchemaContractColumns &&
					(decision.Action == SchemaContractDiscardRow ||
						decision.Action == SchemaContractDiscardValue) {
					t.Fatalf("source-drop decision = %#v", decision)
				}
			}
			if _, ok := findSchemaContractTable(
				plan.TransferSnapshot,
				schemaContractTableKey{
					schema: "public",
					table:  "accounts",
				},
			); !ok {
				t.Fatal("source column drop discarded the whole table")
			}
			assertSchemaContractSnapshotEqual(
				t,
				plan.UpsertSnapshot,
				previous,
				"source-drop retained target",
			)
		})
	}
}

func TestSchemaContractDiscardRowProjectsWholeRun(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns[1].Nullable = true
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractDiscardRow,
				DataType: config.SchemaContractReport,
			},
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !schemaContractHasAction(plan.Decisions, SchemaContractDiscardRow) {
		t.Fatalf("discard-row actions = %v", schemaContractActions(plan.Decisions))
	}
	for name, snapshot := range map[string]schema.SchemaSnapshot{
		"transfer":   plan.TransferSnapshot,
		"validation": plan.ValidationSnapshot,
		"successful": plan.SuccessfulSnapshot,
		"rebuild":    plan.RebuildSnapshot,
	} {
		if _, ok := findSchemaContractTable(
			snapshot,
			schemaContractTableKey{schema: "public", table: "accounts"},
		); ok {
			t.Fatalf("%s snapshot retained discarded table", name)
		}
		child, ok := findSchemaContractTable(
			snapshot,
			schemaContractTableKey{schema: "public", table: "children"},
		)
		if !ok {
			t.Fatalf("%s snapshot lost unaffected table", name)
		}
		for _, foreignKey := range child.ForeignKeys {
			if foreignKey.ReferencedTable == "accounts" {
				t.Fatalf("%s snapshot retained dangling FK %#v", name, foreignKey)
			}
		}
	}
	if _, ok := findSchemaContractTable(
		plan.UpsertSnapshot,
		schemaContractTableKey{schema: "public", table: "accounts"},
	); !ok {
		t.Fatal("discard_row destructively removed the retained upsert target")
	}
}

func TestSchemaContractReferencedTableMatchesQualifiedIdentity(
	t *testing.T,
) {
	t.Parallel()

	owner := schema.SnapshotTable{Schema: "sales", Name: "events"}
	foreignKey := schema.SnapshotForeignKey{
		ReferencedSchema: "identity",
		ReferencedTable:  "accounts",
	}
	if !schemaContractReferencedTableMatches(
		owner,
		foreignKey,
		schemaContractTableKey{schema: "identity", table: "accounts"},
	) {
		t.Fatal("qualified referenced table did not match exact identity")
	}
	if schemaContractReferencedTableMatches(
		owner,
		foreignKey,
		schemaContractTableKey{schema: "sales", table: "accounts"},
	) {
		t.Fatal("qualified reference matched owner-schema same-name table")
	}
}

func TestSchemaContractDiscardRowDominatesDependentObjectEvolution(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns,
		schema.SnapshotColumn{Name: "optional", Type: "text", Nullable: true},
	)
	current.Tables[0].Indexes = append(
		current.Tables[0].Indexes,
		schema.SnapshotIndex{
			Name: "accounts_optional",
			Columns: []schema.SnapshotIndexColumn{{
				Name: "optional",
			}},
		},
	)
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractDiscardRow,
				DataType: config.SchemaContractEvolve,
			},
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range plan.Decisions {
		if decision.Object.Table == "accounts" &&
			decision.Action != SchemaContractDiscardRow {
			t.Fatalf("discard-row dominated decision = %#v", decision)
		}
	}
	for name, snapshot := range map[string]schema.SchemaSnapshot{
		"transfer":   plan.TransferSnapshot,
		"validation": plan.ValidationSnapshot,
		"successful": plan.SuccessfulSnapshot,
		"rebuild":    plan.RebuildSnapshot,
	} {
		if _, ok := findSchemaContractTable(
			snapshot,
			schemaContractTableKey{
				schema: "public",
				table:  "accounts",
			},
		); ok {
			t.Fatalf("%s retained discard_row table", name)
		}
	}
}

func TestSchemaContractDiscardValuePrunesDependentObjectsAndRetainsTypeEvidence(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns[1].DeclaredType.Arguments[0] = 80
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractDiscardValue,
			},
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !schemaContractHasAction(plan.Decisions, SchemaContractDiscardValue) {
		t.Fatalf("discard-value actions = %v", schemaContractActions(plan.Decisions))
	}
	for name, snapshot := range map[string]schema.SchemaSnapshot{
		"transfer":   plan.TransferSnapshot,
		"validation": plan.ValidationSnapshot,
		"rebuild":    plan.RebuildSnapshot,
	} {
		accounts := mustSchemaContractTable(t, snapshot, "public", "accounts")
		if schemaContractSnapshotHasColumn(accounts, "label") {
			t.Fatalf("%s snapshot retained discarded label", name)
		}
		assertSchemaContractDependenciesPruned(t, snapshot)
	}
	successful := mustSchemaContractTable(
		t,
		plan.SuccessfulSnapshot,
		"public",
		"accounts",
	)
	label := mustSchemaContractColumn(t, successful, "label")
	if label.DeclaredType == nil ||
		!reflect.DeepEqual(label.DeclaredType.Arguments, []int{40}) {
		t.Fatalf("successful label evidence = %#v, want prior type", label)
	}
	assertSchemaContractDependenciesPruned(t, plan.SuccessfulSnapshot)
}

func TestSchemaContractDiscardValueOmitsEligibleAddedColumn(t *testing.T) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns = append(
		current.Tables[0].Columns,
		schema.SnapshotColumn{Name: "optional", Type: "text", Nullable: true},
	)
	current.Tables[0].Indexes = append(
		current.Tables[0].Indexes,
		schema.SnapshotIndex{
			Name: "accounts_optional",
			Columns: []schema.SnapshotIndexColumn{{
				Name: "optional",
			}},
		},
	)
	current.Tables[0].Checks = append(
		current.Tables[0].Checks,
		schema.SnapshotCheckConstraint{
			Name:       "accounts_optional_check",
			Expression: `"optional" IS NULL`,
		},
	)
	current.Tables[0].ForeignKeys = append(
		current.Tables[0].ForeignKeys,
		schema.SnapshotForeignKey{
			Name:              "accounts_optional_parent",
			Columns:           []string{"optional"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"label"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "NO ACTION",
		},
	)
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractDiscardValue,
				DataType: config.SchemaContractReport,
			},
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	dependentDecisions := 0
	for _, decision := range plan.Decisions {
		switch decision.ChangeKind {
		case schema.SchemaDriftIndexAdded,
			schema.SchemaDriftCheckAdded,
			schema.SchemaDriftForeignKeyAdded:
			dependentDecisions++
			if decision.Action != SchemaContractDiscardValue {
				t.Fatalf("dependent-object decision = %#v", decision)
			}
		}
	}
	if dependentDecisions != 3 {
		t.Fatalf("dependent decisions = %d, want 3", dependentDecisions)
	}
	for name, snapshot := range map[string]schema.SchemaSnapshot{
		"transfer":   plan.TransferSnapshot,
		"validation": plan.ValidationSnapshot,
		"successful": plan.SuccessfulSnapshot,
		"rebuild":    plan.RebuildSnapshot,
	} {
		accounts := mustSchemaContractTable(t, snapshot, "public", "accounts")
		if schemaContractSnapshotHasColumn(accounts, "optional") {
			t.Fatalf("%s snapshot retained discarded added column", name)
		}
		for _, index := range accounts.Indexes {
			if index.Name == "accounts_optional" {
				t.Fatalf("%s snapshot retained dependent index", name)
			}
		}
		for _, check := range accounts.Checks {
			if check.Name == "accounts_optional_check" {
				t.Fatalf("%s snapshot retained dependent check", name)
			}
		}
		for _, foreignKey := range accounts.ForeignKeys {
			if foreignKey.Name == "accounts_optional_parent" {
				t.Fatalf("%s snapshot retained dependent foreign key", name)
			}
		}
	}
}

func TestSchemaContractRejectsProtectedColumnDiscard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		column      string
		prepare     func(*schema.SchemaSnapshot, *schema.SchemaSnapshot)
		dateColumns []string
	}{
		{
			name:   "primary key",
			column: "id",
		},
		{
			name:   "identity",
			column: "id",
			prepare: func(previous, current *schema.SchemaSnapshot) {
				identity := &schema.SnapshotIdentity{
					Column:     "id",
					Generation: schema.IdentityByDefault,
				}
				previous.Tables[0].Identity = identity
				current.Tables[0].Identity = &schema.SnapshotIdentity{
					Column:     identity.Column,
					Generation: identity.Generation,
				}
			},
		},
		{
			name:        "selected date",
			column:      "updated_at",
			dateColumns: []string{"updated_at"},
		},
		{
			name:   "clickhouse ordering key",
			column: "label",
			prepare: func(previous, current *schema.SchemaSnapshot) {
				previous.Tables[0].ClickHouseOrderBy = []string{"label"}
				current.Tables[0].ClickHouseOrderBy = []string{"label"}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			previous := schemaContractFixture()
			current := cloneSchemaContractFixture(t, previous)
			if test.prepare != nil {
				test.prepare(&previous, &current)
			}
			column := mustSchemaContractColumnPointer(
				t,
				&current.Tables[0],
				test.column,
			)
			if column.DeclaredType == nil {
				column.DeclaredType = &schema.SnapshotDeclaredType{
					Base: "bigint",
				}
			}
			switch normalizeSchemaContractType(column.DeclaredType.Base) {
			case "bigint":
				column.DeclaredType.Base = "int"
				column.Type = "integer"
			case "timestamp":
				column.DeclaredType.Arguments = []int{6}
			default:
				if len(column.DeclaredType.Arguments) == 1 {
					column.DeclaredType.Arguments[0]++
				} else {
					column.DeclaredType = &schema.SnapshotDeclaredType{
						Base: "longtext",
					}
				}
			}
			_, err := BuildSchemaContractPlan(
				previous,
				current,
				SchemaContractOptions{
					Contract: &config.SchemaContract{
						Tables:   config.SchemaContractReport,
						Columns:  config.SchemaContractReport,
						DataType: config.SchemaContractDiscardValue,
					},
					TargetMode:         "upsert",
					DateUpdatedColumns: test.dateColumns,
				},
			)
			assertSchemaContractErrorKind(
				t,
				err,
				SchemaContractProtectedDiscard,
			)
		})
	}
}

func TestSchemaContractRejectsTablesDiscardValueAndInvalidModes(t *testing.T) {
	t.Parallel()

	fixture := schemaContractFixture()
	_, err := BuildSchemaContractPlan(
		fixture,
		fixture,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractDiscardValue,
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractReport,
			},
			TargetMode: "upsert",
		},
	)
	assertSchemaContractErrorKind(t, err, SchemaContractInvalidPolicy)

	_, err = BuildSchemaContractPlan(
		fixture,
		fixture,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractMode("invented"),
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractReport,
			},
			TargetMode: "upsert",
		},
	)
	assertSchemaContractErrorKind(t, err, SchemaContractInvalidPolicy)
}

func TestSchemaContractDecisionFactsAreCompleteStableAndInputImmutable(
	t *testing.T,
) {
	t.Parallel()

	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns[1].DeclaredType.Arguments[0] = 80
	current.Tables[1].Columns = append(
		current.Tables[1].Columns,
		schema.SnapshotColumn{Name: "note", Type: "text", Nullable: true},
	)
	previousBefore := mustJSON(t, previous)
	currentBefore := mustJSON(t, current)

	first, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractReport),
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range first.Decisions {
		if decision.Entity == "" ||
			decision.Mode == "" ||
			decision.ChangeKind == "" ||
			decision.Object.Kind == "" ||
			decision.Object.Table == "" ||
			!json.Valid(decision.Previous) ||
			!json.Valid(decision.Current) ||
			decision.Action == "" ||
			decision.Reason == "" {
			t.Fatalf("incomplete decision = %#v", decision)
		}
	}
	if got := mustJSON(t, previous); string(got) != string(previousBefore) {
		t.Fatalf("previous input mutated:\n%s\n%s", previousBefore, got)
	}
	if got := mustJSON(t, current); string(got) != string(currentBefore) {
		t.Fatalf("current input mutated:\n%s\n%s", currentBefore, got)
	}

	reorderedPrevious := cloneSchemaContractFixture(t, previous)
	reorderedCurrent := cloneSchemaContractFixture(t, current)
	reverseSchemaContractTables(reorderedPrevious.Tables)
	reverseSchemaContractTables(reorderedCurrent.Tables)
	for index := range reorderedPrevious.Tables {
		reverseSchemaContractIndexes(reorderedPrevious.Tables[index].Indexes)
	}
	for index := range reorderedCurrent.Tables {
		reverseSchemaContractIndexes(reorderedCurrent.Tables[index].Indexes)
	}
	second, err := BuildSchemaContractPlan(
		reorderedPrevious,
		reorderedCurrent,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractReport),
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(mustJSON(t, second)), string(mustJSON(t, first)); got != want {
		t.Fatalf("plan changed with discovery order:\n%s\n%s", want, got)
	}
}

func TestSchemaContractLiteralDefaultAdmission(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"NULL", "true", "FALSE", "0", "-1.25", ".5e+2",
		"'text'", "'it''s safe'", "X'00ff'", "x''",
	} {
		if !safeSchemaContractDefault(value) {
			t.Errorf("safe literal %q rejected", value)
		}
	}
	for _, value := range []string{
		"", "CURRENT_TIMESTAMP", "nextval('sequence')", "'unterminated",
		"'bad'quote'", "X'0'", "X'zz'", "1/2", "NaN",
	} {
		if safeSchemaContractDefault(value) {
			t.Errorf("unsafe default %q admitted", value)
		}
	}
}

func TestSchemaContractCheckDependencyLexerIsLiteralSafeAndFailClosed(
	t *testing.T,
) {
	t.Parallel()

	columns := map[string]struct{}{"label": {}}
	if !schemaContractCheckUsesAny(`"label" <> 'label'`, columns) {
		t.Fatal("quoted identifier dependency was missed")
	}
	if schemaContractCheckUsesAny(`"other" <> 'label'`, columns) {
		t.Fatal("string literal was mistaken for a dependency")
	}
	if !schemaContractCheckUsesAny(`"unterminated`, columns) {
		t.Fatal("malformed expression did not fail closed")
	}
}

func schemaContractFixture() schema.SchemaSnapshot {
	labelDefault := "'active'"
	return schema.SchemaSnapshot{
		Version: schema.SchemaSnapshotVersion,
		Tables: []schema.SnapshotTable{
			{
				Schema: "public",
				Name:   "accounts",
				Columns: []schema.SnapshotColumn{
					{
						Name: "id", Type: "bigint",
						PrimaryKey: true, PrimaryKeyPosition: 1,
					},
					{
						Name: "label", Type: "varchar",
						DeclaredType: &schema.SnapshotDeclaredType{
							Base: "varchar", Arguments: []int{40},
						},
						Default: &labelDefault,
					},
					{
						Name: "amount", Type: "numeric",
						DeclaredType: &schema.SnapshotDeclaredType{
							Base: "numeric", Arguments: []int{12, 2},
						},
					},
					{
						Name: "updated_at", Type: "timestamp", Nullable: true,
						DeclaredType: &schema.SnapshotDeclaredType{
							Base: "timestamp", Arguments: []int{3},
						},
					},
					{Name: "parent_code", Type: "text", Nullable: true},
				},
				Indexes: []schema.SnapshotIndex{
					{
						Name: "accounts_label",
						Columns: []schema.SnapshotIndexColumn{{
							Name: "label",
						}},
					},
					{
						Name: "accounts_amount",
						Columns: []schema.SnapshotIndexColumn{{
							Name: "amount",
						}},
					},
				},
				Checks: []schema.SnapshotCheckConstraint{
					{Name: "accounts_label_check", Expression: `"label" <> ''`},
					{Name: "accounts_amount_check", Expression: `"amount" >= 0`},
				},
			},
			{
				Schema: "public",
				Name:   "children",
				Columns: []schema.SnapshotColumn{
					{
						Name: "id", Type: "bigint",
						PrimaryKey: true, PrimaryKeyPosition: 1,
					},
					{Name: "account_label", Type: "varchar", Nullable: true,
						DeclaredType: &schema.SnapshotDeclaredType{
							Base: "varchar", Arguments: []int{40},
						}},
				},
				ForeignKeys: []schema.SnapshotForeignKey{{
					Name:              "children_account_label",
					Columns:           []string{"account_label"},
					ReferencedTable:   "accounts",
					ReferencedColumns: []string{"label"},
					OnUpdate:          "NO ACTION",
					OnDelete:          "NO ACTION",
				}},
			},
		},
	}
}

func schemaContractAll(mode config.SchemaContractMode) *config.SchemaContract {
	return &config.SchemaContract{Tables: mode, Columns: mode, DataType: mode}
}

func cloneSchemaContractFixture(
	t *testing.T,
	value schema.SchemaSnapshot,
) schema.SchemaSnapshot {
	t.Helper()
	encoded := mustJSON(t, value)
	var result schema.SchemaSnapshot
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertSchemaContractErrorKind(
	t *testing.T,
	err error,
	want SchemaContractErrorKind,
) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", want)
	}
	var contractError *SchemaContractError
	if !errors.As(err, &contractError) {
		t.Fatalf("error = %T %v, want *SchemaContractError", err, err)
	}
	if contractError.Kind != want {
		t.Fatalf("error kind = %q, want %q: %v", contractError.Kind, want, err)
	}
}

func assertSchemaContractSnapshotEqual(
	t *testing.T,
	got,
	want schema.SchemaSnapshot,
	description string,
) {
	t.Helper()
	gotJSON, err := got.CanonicalJSON()
	if err != nil {
		t.Fatalf("%s got snapshot: %v", description, err)
	}
	wantJSON, err := want.CanonicalJSON()
	if err != nil {
		t.Fatalf("%s want snapshot: %v", description, err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("%s mismatch:\n%s\n%s", description, wantJSON, gotJSON)
	}
}

func schemaContractHasAction(
	decisions []SchemaContractDecision,
	action SchemaContractAction,
) bool {
	for _, decision := range decisions {
		if decision.Action == action {
			return true
		}
	}
	return false
}

func schemaContractDecisionByKind(
	t *testing.T,
	decisions []SchemaContractDecision,
	kind schema.SchemaDriftChangeKind,
) SchemaContractDecision {
	t.Helper()
	for _, decision := range decisions {
		if decision.ChangeKind == kind {
			return decision
		}
	}
	t.Fatalf("missing schema contract decision %s", kind)
	return SchemaContractDecision{}
}

func schemaContractActions(
	decisions []SchemaContractDecision,
) []SchemaContractAction {
	result := make([]SchemaContractAction, len(decisions))
	for index, decision := range decisions {
		result[index] = decision.Action
	}
	return result
}

func mustSchemaContractTable(
	t *testing.T,
	snapshot schema.SchemaSnapshot,
	namespace,
	name string,
) schema.SnapshotTable {
	t.Helper()
	table, ok := findSchemaContractTable(
		snapshot,
		schemaContractTableKey{schema: namespace, table: name},
	)
	if !ok {
		t.Fatalf("missing table %s.%s", namespace, name)
	}
	return table
}

func mustSchemaContractColumn(
	t *testing.T,
	table schema.SnapshotTable,
	name string,
) schema.SnapshotColumn {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			return column
		}
	}
	t.Fatalf("missing column %s.%s", table.Name, name)
	return schema.SnapshotColumn{}
}

func mustSchemaContractColumnPointer(
	t *testing.T,
	table *schema.SnapshotTable,
	name string,
) *schema.SnapshotColumn {
	t.Helper()
	for index := range table.Columns {
		if table.Columns[index].Name == name {
			return &table.Columns[index]
		}
	}
	t.Fatalf("missing column %s.%s", table.Name, name)
	return nil
}

func schemaContractSnapshotHasColumn(
	table schema.SnapshotTable,
	name string,
) bool {
	for _, column := range table.Columns {
		if column.Name == name {
			return true
		}
	}
	return false
}

func assertSchemaContractDependenciesPruned(
	t *testing.T,
	snapshot schema.SchemaSnapshot,
) {
	t.Helper()
	accounts := mustSchemaContractTable(t, snapshot, "public", "accounts")
	for _, index := range accounts.Indexes {
		if index.Name == "accounts_label" {
			t.Fatal("dependent index was not pruned")
		}
	}
	for _, check := range accounts.Checks {
		if check.Name == "accounts_label_check" {
			t.Fatal("dependent check was not pruned")
		}
	}
	children := mustSchemaContractTable(t, snapshot, "public", "children")
	for _, foreignKey := range children.ForeignKeys {
		if foreignKey.Name == "children_account_label" {
			t.Fatal("dependent foreign key was not pruned")
		}
	}
}

func reverseSchemaContractTables(values []schema.SnapshotTable) {
	for left, right := 0, len(values)-1; left < right; left, right =
		left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseSchemaContractIndexes(values []schema.SnapshotIndex) {
	for left, right := 0, len(values)-1; left < right; left, right =
		left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestSchemaContractErrorDoesNotExposeEvidenceValues(t *testing.T) {
	t.Parallel()

	secretDefault := "'credential-like-default'"
	previous := schemaContractFixture()
	current := cloneSchemaContractFixture(t, previous)
	current.Tables[0].Columns[1].Default = &secretDefault
	_, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   schemaContractAll(config.SchemaContractEvolve),
			TargetMode: "upsert",
		},
	)
	assertSchemaContractErrorKind(t, err, SchemaContractUnsafeEvolution)
	if strings.Contains(err.Error(), "credential-like-default") {
		t.Fatalf("error leaked schema evidence value: %v", err)
	}
}
