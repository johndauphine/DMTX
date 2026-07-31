package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4TargetSchemaEvolutionProjectionPlansExactSafeUpsertEndpoints(
	t *testing.T,
) {
	t.Parallel()

	previousTables, currentTables := stage4TargetSchemaProjectionSafeTables()
	gate := stage4TargetSchemaProjectionGate(
		t,
		previousTables,
		currentTables,
		"upsert",
		false,
	)
	before := gate
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "postgres",
		targetSchema: "warehouse",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "mssql", target, "upsert",
	)

	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"mssql",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if projection.SourceEngine() != "mssql" ||
		projection.TargetEngine() != "postgres" ||
		projection.TargetMode() != "upsert" {
		t.Fatalf(
			"route = %s-to-%s mode %s",
			projection.SourceEngine(),
			projection.TargetEngine(),
			projection.TargetMode(),
		)
	}
	if projection.PriorDigest() == "" || projection.CurrentDigest() == "" ||
		projection.PriorDigest() == projection.CurrentDigest() {
		t.Fatalf(
			"prior/current digests = %q/%q",
			projection.PriorDigest(),
			projection.CurrentDigest(),
		)
	}
	if projection.SourcePriorDigest() == "" ||
		projection.SourceCurrentDigest() == "" ||
		projection.SourcePriorDigest() == projection.SourceCurrentDigest() ||
		len(projection.Decisions()) == 0 {
		t.Fatalf(
			"source digests/decisions = %q/%q/%d",
			projection.SourcePriorDigest(),
			projection.SourceCurrentDigest(),
			len(projection.Decisions()),
		)
	}
	if target.planCalls != 4 {
		t.Fatalf("PlanTables calls = %d, want 4", target.planCalls)
	}
	if !reflect.DeepEqual(
		target.sourceEngines,
		[]string{"mssql", "mssql", "mssql", "mssql"},
	) || !reflect.DeepEqual(
		target.targetModes,
		[]string{"upsert", "upsert", "upsert", "upsert"},
	) {
		t.Fatalf(
			"planner routes/modes = %#v/%#v",
			target.sourceEngines,
			target.targetModes,
		)
	}
	if !reflect.DeepEqual(gate, before) {
		t.Fatal("projection changed caller-owned gate evidence")
	}

	prior := projection.PriorTables()
	current := projection.CurrentTables()
	if len(prior) != 1 || len(current) != 2 {
		t.Fatalf("prior/current table counts = %d/%d, want 1/2", len(prior), len(current))
	}
	accounts := stage4TargetSchemaProjectionFindTable(t, current, "accounts")
	if accounts.Schema != "warehouse" {
		t.Fatalf("target accounts schema = %q, want warehouse", accounts.Schema)
	}
	if got := stage4TargetSchemaProjectionFindColumn(t, accounts, "score").Type; got != "BIGINT" {
		t.Fatalf("target widened score type = %q, want BIGINT", got)
	}
	if !stage4TargetSchemaProjectionFindColumn(t, accounts, "note").Nullable {
		t.Fatal("target note did not preserve nullability relaxation")
	}
	if got := stage4TargetSchemaProjectionFindColumn(t, accounts, "label"); !got.Nullable ||
		got.Type != "TEXT" {
		t.Fatalf("target added label = %#v", got)
	}
	if accounts.Identity == nil || accounts.Identity.Frontier != nil {
		t.Fatalf("materialized target identity = %#v, want frontier omitted", accounts.Identity)
	}
	events := stage4TargetSchemaProjectionFindTable(t, current, "events")
	if events.Schema != "warehouse" ||
		events.ForeignKeys[0].ReferencedSchema != "warehouse" {
		t.Fatalf("target events namespace/FK = %#v", events)
	}

	sourceScore := Stage4SchemaObjectIdentity{
		Schema: "source",
		Table:  "accounts",
		Column: "score",
	}
	targetScore, found := projection.TargetObject(sourceScore)
	if !found || targetScore != (Stage4SchemaObjectIdentity{
		Schema: "warehouse",
		Table:  "accounts",
		Column: "score",
	}) {
		t.Fatalf("score mapping = %#v, found %t", targetScore, found)
	}
	sourceEvents := Stage4SchemaObjectIdentity{
		Schema: "source",
		Table:  "events",
	}
	targetEvents, found := projection.TargetObject(sourceEvents)
	if !found || targetEvents.Schema != "warehouse" ||
		targetEvents.Table != "events" || targetEvents.Column != "" {
		t.Fatalf("events mapping = %#v, found %t", targetEvents, found)
	}

	// Every accessor must remain independent from caller mutation.
	current[0].Columns[0].Name = "caller mutation"
	prior[0].Schema = "caller mutation"
	mappings := projection.ObjectMappings()
	mappings[0].Target.Table = "caller mutation"
	decisions := projection.Decisions()
	decisions[0].Reason = "caller mutation"
	decisions[0].Current[0] = 'x'
	if stage4TargetSchemaProjectionFindTable(
		t,
		projection.CurrentTables(),
		"accounts",
	).Columns[0].Name == "caller mutation" ||
		projection.PriorTables()[0].Schema == "caller mutation" ||
		projection.ObjectMappings()[0].Target.Table == "caller mutation" ||
		projection.Decisions()[0].Reason == "caller mutation" ||
		projection.Decisions()[0].Current[0] == 'x' {
		t.Fatal("caller mutation changed immutable projection evidence")
	}

	repeated, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"mssql",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if projection.PriorDigest() != repeated.PriorDigest() ||
		projection.CurrentDigest() != repeated.CurrentDigest() ||
		projection.SourcePriorDigest() != repeated.SourcePriorDigest() ||
		projection.SourceCurrentDigest() != repeated.SourceCurrentDigest() ||
		!reflect.DeepEqual(projection.Decisions(), repeated.Decisions()) ||
		!reflect.DeepEqual(projection.PriorTables(), repeated.PriorTables()) ||
		!reflect.DeepEqual(projection.CurrentTables(), repeated.CurrentTables()) ||
		!reflect.DeepEqual(projection.ObjectMappings(), repeated.ObjectMappings()) {
		t.Fatal("repeated target projection changed deterministic evidence")
	}
}

func TestStage4TargetSchemaEvolutionProjectionPreservesPostgresRetainedObjectNames(
	t *testing.T,
) {
	t.Parallel()

	retained := schema.Table{
		Schema: "source_z",
		Name:   "zeta",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			{Name: "value", Type: "text"},
		},
		Indexes: []schema.Index{{
			Name:    "shared_table_scoped_idx",
			Columns: []schema.IndexColumn{{Name: "value"}},
		}},
	}
	added := cloneStage4RichTable(retained)
	added.Schema = "source_a"
	added.Name = "alpha"
	previous := []schema.Table{retained}
	current := []schema.Table{added, retained}
	gate := stage4TargetSchemaProjectionGate(
		t,
		previous,
		current,
		"upsert",
		false,
	)
	before := stage4TargetSchemaProjectionCloneGate(t, gate)
	target := &postgresTargetAdapter{namespace: "tenant"}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "postgres", target, "upsert",
	)

	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	priorRetained := stage4TargetSchemaProjectionFindTable(
		t,
		projection.PriorTables(),
		"zeta",
	)
	currentRetained := stage4TargetSchemaProjectionFindTable(
		t,
		projection.CurrentTables(),
		"zeta",
	)
	currentAdded := stage4TargetSchemaProjectionFindTable(
		t,
		projection.CurrentTables(),
		"alpha",
	)
	if priorRetained.Indexes[0].Name != "shared_table_scoped_idx" ||
		currentRetained.Indexes[0].Name != priorRetained.Indexes[0].Name {
		t.Fatalf(
			"retained index names = prior:%q current:%q",
			priorRetained.Indexes[0].Name,
			currentRetained.Indexes[0].Name,
		)
	}
	if currentAdded.Indexes[0].Name == "shared_table_scoped_idx" ||
		currentAdded.Indexes[0].Name == "" {
		t.Fatalf(
			"new colliding index name = %q, want deterministic alternate",
			currentAdded.Indexes[0].Name,
		)
	}

	ordinary, err := target.PlanTables("postgres", current, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	if stage4TargetSchemaProjectionFindTable(
		t,
		ordinary,
		"alpha",
	).Indexes[0].Name != "shared_table_scoped_idx" {
		t.Fatal("ordinary PlanTables allocation unexpectedly became prior-aware")
	}

	repeated, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projection.PriorTables(), repeated.PriorTables()) ||
		!reflect.DeepEqual(projection.CurrentTables(), repeated.CurrentTables()) ||
		projection.PriorDigest() != repeated.PriorDigest() ||
		projection.CurrentDigest() != repeated.CurrentDigest() {
		t.Fatal("prior-aware PostgreSQL projection is nondeterministic")
	}
	if !reflect.DeepEqual(gate, before) ||
		!reflect.DeepEqual(previous, []schema.Table{retained}) ||
		!reflect.DeepEqual(current, []schema.Table{added, retained}) {
		t.Fatal("prior-aware PostgreSQL projection mutated caller-owned metadata")
	}
}

func TestStage4TargetSchemaEvolutionProjectionRejectsRetainedRebuildSubobjectsWithoutDurableTargetEvidence(
	t *testing.T,
) {
	t.Parallel()

	previous := []schema.Table{
		{
			Schema: "source",
			Name:   "accounts",
			Columns: []schema.Column{
				stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
				{Name: "legacy_code", Type: "text", Nullable: true},
			},
		},
		{
			Schema: "source",
			Name:   "legacy_audit",
			Columns: []schema.Column{
				stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			},
		},
	}
	current := []schema.Table{{
		Schema: "source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
		},
	}}
	gate := stage4TargetSchemaProjectionGate(
		t,
		previous,
		current,
		"drop_recreate",
		false,
	)
	if !gate.RebuildRequiresTargetCatalog {
		t.Fatal("fixture does not require retained prior-only reconstruction")
	}
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "mssql",
		targetSchema: "dbo",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "postgres", target, "drop_recreate",
	)

	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	accounts := stage4TargetSchemaProjectionFindTable(
		t,
		projection.CurrentTables(),
		"accounts",
	)
	if stage4TargetSchemaProjectionFindColumn(
		t,
		accounts,
		"legacy_code",
	).Name != "legacy_code" {
		t.Fatal("durable target authority lost retained rebuild column")
	}
}

func TestStage4TargetSchemaEvolutionProjectionKeepsWholePriorOnlyTargetTable(
	t *testing.T,
) {
	t.Parallel()

	previous := []schema.Table{{
		Schema: "source",
		Name:   "legacy_audit",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
		},
	}}
	gate := stage4TargetSchemaProjectionGate(
		t,
		previous,
		nil,
		"drop_recreate",
		false,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "mssql",
		targetSchema: "dbo",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "postgres", target, "drop_recreate",
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	desired := projection.CurrentTables()
	if len(desired) != 1 ||
		desired[0].Name != "legacy_audit" ||
		desired[0].Schema != "dbo" {
		t.Fatalf("whole prior-only target table = %#v", desired)
	}
}

func TestStage4TargetSchemaEvolutionProjectionAcceptsExplicitFirstRunCreates(
	t *testing.T,
) {
	t.Parallel()

	_, current := stage4TargetSchemaProjectionSafeTables()
	gate := stage4TargetSchemaProjectionGate(
		t,
		nil,
		current,
		"upsert",
		true,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "mysql",
		targetSchema: "destination",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "sqlite", target, "upsert",
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"sqlite",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.PriorTables()) != 0 ||
		len(projection.CurrentTables()) != len(current) {
		t.Fatalf(
			"first-run prior/current counts = %d/%d",
			len(projection.PriorTables()),
			len(projection.CurrentTables()),
		)
	}
}

func TestStage4TargetSchemaEvolutionProjectionAuthenticatesCompatibleExistingCreate(
	t *testing.T,
) {
	t.Parallel()

	current := []schema.Table{{
		Schema: "source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			{Name: "label", Type: "text", Nullable: true},
		},
	}}
	gate := stage4TargetSchemaProjectionGate(
		t,
		nil,
		current,
		"upsert",
		true,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "postgres",
		targetSchema: "tenant",
	}
	planned, err := target.PlanTables("sqlite", current, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	compatible := cloneStage4TargetSchemaProjectionTables(planned)
	compatible[0].Columns = append(
		compatible[0].Columns,
		schema.Column{
			Name: "operator_only", Type: "text", Nullable: true,
		},
	)
	catalog, err := NewTargetSchemaEvolutionCatalog(compatible, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := stage4TargetSchemaProjectionAuthorityFromCatalog(
		t,
		gate,
		"sqlite",
		"postgres",
		"upsert",
		catalog,
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"sqlite",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projection.PriorTables(), projection.CurrentTables()) {
		t.Fatalf(
			"compatible existing create was not an authenticated no-op: prior=%#v current=%#v",
			projection.PriorTables(),
			projection.CurrentTables(),
		)
	}
	request, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		projection,
		targetSchemaEvolutionFixtureCreatePlanner{},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildTargetSchemaEvolutionPlan(request, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Complete() || plan.OperationCount() != 0 {
		t.Fatalf(
			"compatible existing create plan complete=%t operations=%d",
			plan.Complete(),
			plan.OperationCount(),
		)
	}

	incompatible := cloneStage4TargetSchemaProjectionTables(compatible)
	incompatible[0].Columns[0].Type = "text"
	incompatibleCatalog, err := NewTargetSchemaEvolutionCatalog(
		incompatible,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	incompatibleAuthority :=
		stage4TargetSchemaProjectionAuthorityFromCatalog(
			t,
			gate,
			"sqlite",
			"postgres",
			"upsert",
			incompatibleCatalog,
		)
	if _, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		incompatibleAuthority,
		"sqlite",
		target,
		"upsert",
	); err == nil ||
		!strings.Contains(err.Error(), "collides with incompatible") {
		t.Fatalf("incompatible existing create error = %v", err)
	}
}

func TestStage4TargetSchemaEvolutionProjectionRejectsRetainedUpsertSubobjectsWithoutDurableTargetEvidence(
	t *testing.T,
) {
	t.Parallel()

	previous := []schema.Table{{
		Schema: "source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			{Name: "legacy_code", Type: "text", Nullable: true},
		},
	}}
	current := []schema.Table{{
		Schema: "source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
		},
	}}
	gate := stage4TargetSchemaProjectionGate(
		t,
		previous,
		current,
		"upsert",
		false,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "postgres",
		targetSchema: "public",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "postgres", target, "upsert",
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	accounts := stage4TargetSchemaProjectionFindTable(
		t,
		projection.CurrentTables(),
		"accounts",
	)
	if stage4TargetSchemaProjectionFindColumn(
		t,
		accounts,
		"legacy_code",
	).Name != "legacy_code" {
		t.Fatal("durable target authority lost retained upsert column")
	}
}

func TestStage4TargetSchemaEvolutionProjectionFailsClosedBeforeTargetIO(
	t *testing.T,
) {
	t.Parallel()

	previousTables, currentTables := stage4TargetSchemaProjectionSafeTables()
	base := stage4TargetSchemaProjectionGate(
		t,
		previousTables,
		currentTables,
		"upsert",
		false,
	)
	tests := []struct {
		name   string
		mutate func(*Stage4SchemaGateResult, *stage4TargetSchemaProjectionTestTarget)
		want   string
		calls  int
	}{
		{
			name: "changed baseline with nonempty prior",
			mutate: func(gate *Stage4SchemaGateResult, _ *stage4TargetSchemaProjectionTestTarget) {
				gate.Baseline = true
			},
			want:  "changed baseline gate has nonempty durable prior evidence",
			calls: 0,
		},
		{
			name: "mode action mismatch",
			mutate: func(gate *Stage4SchemaGateResult, _ *stage4TargetSchemaProjectionTestTarget) {
				gate.Plan.Decisions[0].Action = SchemaContractRebuildCurrentShape
			},
			want:  `inconsistent with target mode "upsert"`,
			calls: 0,
		},
		{
			name: "valid but substituted decision evidence",
			mutate: func(gate *Stage4SchemaGateResult, _ *stage4TargetSchemaProjectionTestTarget) {
				gate.Plan.Decisions[0].Current =
					json.RawMessage(`{"valid":"but wrong"}`)
			},
			want:  "does not contain exact evidence",
			calls: 0,
		},
		{
			name: "unsupported retained object",
			mutate: func(gate *Stage4SchemaGateResult, _ *stage4TargetSchemaProjectionTestTarget) {
				gate.Plan.UpsertSnapshot.Tables = append(
					gate.Plan.UpsertSnapshot.Tables,
					schema.SnapshotTable{
						Schema: "source",
						Name:   "fabricated",
						Columns: []schema.SnapshotColumn{{
							Name:               "id",
							Type:               "bigint",
							PrimaryKey:         true,
							PrimaryKeyPosition: 1,
						}},
					},
				)
			},
			want:  "retained prior-only reconstruction is unsupported",
			calls: 0,
		},
		{
			name: "materialize malformed default",
			mutate: func(gate *Stage4SchemaGateResult, _ *stage4TargetSchemaProjectionTestTarget) {
				bad := "random()"
				gate.PreviousSnapshot.Tables[0].Columns[0].Default = &bad
				gate.CurrentSnapshot = gate.PreviousSnapshot
				gate.Plan = SchemaContractPlan{
					TransferSnapshot:   gate.PreviousSnapshot,
					SuccessfulSnapshot: gate.PreviousSnapshot,
					UpsertSnapshot:     gate.PreviousSnapshot,
					RebuildSnapshot:    gate.PreviousSnapshot,
				}
				gate.RebuildCurrentSnapshot = gate.PreviousSnapshot
				gate.RebuildRequiresTargetCatalog = false
				gate.Baseline = true
			},
			want:  "materialize durable prior snapshot",
			calls: 0,
		},
		{
			name: "planner error",
			mutate: func(_ *Stage4SchemaGateResult, target *stage4TargetSchemaProjectionTestTarget) {
				target.planErr = errors.New("injected planner failure")
			},
			want:  "injected planner failure",
			calls: 1,
		},
		{
			name: "planner input mutation",
			mutate: func(_ *Stage4SchemaGateResult, target *stage4TargetSchemaProjectionTestTarget) {
				target.mutateInput = true
			},
			want:  "mutated prior source metadata",
			calls: 1,
		},
		{
			name: "planner nondeterminism",
			mutate: func(_ *Stage4SchemaGateResult, target *stage4TargetSchemaProjectionTestTarget) {
				target.nondeterministic = true
			},
			want:  "nondeterministic for prior snapshot",
			calls: 2,
		},
		{
			name: "mismatched table identity",
			mutate: func(_ *Stage4SchemaGateResult, target *stage4TargetSchemaProjectionTestTarget) {
				target.renameFirstTable = true
			},
			want:  "changed prior source table",
			calls: 2,
		},
		{
			name: "invented identity frontier",
			mutate: func(_ *Stage4SchemaGateResult, target *stage4TargetSchemaProjectionTestTarget) {
				target.inventIdentityFrontier = true
			},
			want:  "invented dynamic identity frontier",
			calls: 2,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gate := stage4TargetSchemaProjectionCloneGate(t, base)
			target := &stage4TargetSchemaProjectionTestTarget{
				engine:       "postgres",
				targetSchema: "warehouse",
			}
			authority := stage4TargetSchemaProjectionAuthority(
				t,
				gate,
				"mssql",
				&stage4TargetSchemaProjectionTestTarget{
					engine:       "postgres",
					targetSchema: "warehouse",
				},
				"upsert",
			)
			test.mutate(&gate, target)
			if test.name == "materialize malformed default" {
				var digestErr error
				authority.sourcePriorDigest, digestErr =
					gate.PreviousSnapshot.Digest()
				if digestErr != nil {
					t.Fatal(digestErr)
				}
				authority.sourceCurrentDigest, digestErr =
					gate.CurrentSnapshot.Digest()
				if digestErr != nil {
					t.Fatal(digestErr)
				}
				authority.sourceSuccessDigest, digestErr =
					gate.Plan.SuccessfulSnapshot.Digest()
				if digestErr != nil {
					t.Fatal(digestErr)
				}
				authority.decisionDigest, digestErr =
					stage4TargetShapeDecisionDigest(
						gate.Plan.Decisions,
					)
				if digestErr != nil {
					t.Fatal(digestErr)
				}
			}
			_, err := BuildStage4TargetSchemaEvolutionProjection(
				gate,
				authority,
				"mssql",
				target,
				"upsert",
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want fragment %q", err, test.want)
			}
			if target.planCalls != test.calls {
				t.Fatalf(
					"PlanTables calls = %d, want %d",
					target.planCalls,
					test.calls,
				)
			}
			if target.ioCalls != 0 {
				t.Fatalf("target I/O calls = %d before projection failure", target.ioCalls)
			}
		})
	}
}

func TestStage4TargetSchemaEvolutionProjectionRejectsCrossEndpointAliasReplacement(
	t *testing.T,
) {
	t.Parallel()

	previous := []schema.Table{{
		Schema: "old_source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
		},
	}}
	current := []schema.Table{{
		Schema: "new_source",
		Name:   "accounts",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
		},
	}}
	gate := stage4TargetSchemaProjectionGate(
		t,
		previous,
		current,
		"upsert",
		false,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine:       "mysql",
		targetSchema: "one_database",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t, gate, "postgres", target, "upsert",
	)
	_, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err == nil || !strings.Contains(err.Error(), "collide at target table") {
		t.Fatalf("cross-endpoint alias error = %v", err)
	}
}

func stage4TargetSchemaProjectionSafeTables() ([]schema.Table, []schema.Table) {
	frontier := int64(41)
	accounts := schema.Table{
		Schema: "source",
		Name:   "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			{Name: "note", Type: "text"},
			{Name: "score", Type: "integer"},
		},
		Indexes: []schema.Index{{
			Name: "accounts_score_idx",
			Columns: []schema.IndexColumn{{
				Name: "score",
			}},
		}},
	}
	previous := []schema.Table{accounts}
	currentAccounts := cloneStage4RichTable(accounts)
	currentAccounts.Columns[1].Nullable = true
	currentAccounts.Columns[2].Type = "bigint"
	currentAccounts.Columns = append(
		currentAccounts.Columns,
		schema.Column{Name: "label", Type: "text", Nullable: true},
	)
	events := schema.Table{
		Schema: "source",
		Name:   "events",
		Columns: []schema.Column{
			stage4TargetSchemaProjectionPrimaryColumn("id", "bigint"),
			{Name: "account_id", Type: "bigint"},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_account_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "source",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "SIMPLE",
		}},
	}
	return previous, []schema.Table{currentAccounts, events}
}

func stage4TargetSchemaProjectionPrimaryColumn(
	name string,
	columnType string,
) schema.Column {
	return schema.Column{
		Name:               name,
		Type:               columnType,
		PrimaryKey:         true,
		PrimaryKeyPosition: 1,
	}
}

func stage4TargetSchemaProjectionGate(
	t *testing.T,
	previousTables []schema.Table,
	currentTables []schema.Table,
	targetMode string,
	baseline bool,
) Stage4SchemaGateResult {
	t.Helper()

	previous := stage4TargetSchemaProjectionSnapshot(t, previousTables)
	current := stage4TargetSchemaProjectionSnapshot(t, currentTables)
	contract := &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	plan, err := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:   contract,
			TargetMode: targetMode,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	equal, err := schema.SchemaSnapshotsEqual(
		plan.TransferSnapshot,
		plan.RebuildSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	return Stage4SchemaGateResult{
		Task:                         stage4SchemaGateTask,
		TopologyHash:                 "projection-fixture-topology",
		Baseline:                     baseline,
		PreviousSnapshot:             previous,
		CurrentSnapshot:              current,
		Plan:                         plan,
		RebuildCurrentSnapshot:       plan.TransferSnapshot,
		RebuildRequiresTargetCatalog: !equal,
	}
}

func stage4TargetSchemaProjectionAuthority(
	t *testing.T,
	gate Stage4SchemaGateResult,
	sourceEngine string,
	target Stage4TargetSchemaPlanner,
	targetMode string,
) Stage4TargetShapeAuthority {
	t.Helper()
	priorSource, err := schema.MaterializeSchemaSnapshot(
		gate.PreviousSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	priorTarget, err := target.PlanTables(
		sourceEngine,
		priorSource,
		targetMode,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recorder, ok := target.(*stage4TargetSchemaProjectionTestTarget); ok {
		recorder.planCalls = 0
		recorder.sourceEngines = nil
		recorder.targetModes = nil
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(priorTarget, nil)
	if err != nil {
		t.Fatal(err)
	}
	return stage4TargetSchemaProjectionAuthorityFromCatalog(
		t,
		gate,
		sourceEngine,
		target.Engine(),
		targetMode,
		catalog,
	)
}

func stage4TargetSchemaProjectionAuthorityFromCatalog(
	t *testing.T,
	gate Stage4SchemaGateResult,
	sourceEngine string,
	targetEngine string,
	targetMode string,
	catalog TargetSchemaEvolutionCatalog,
) Stage4TargetShapeAuthority {
	t.Helper()
	seed, err := NewStage4TargetShapeSeed(catalog)
	if err != nil {
		t.Fatal(err)
	}
	sourcePriorDigest, err := gate.PreviousSnapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	sourceCurrentDigest, err := gate.CurrentSnapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	sourceSuccessDigest, err := gate.Plan.SuccessfulSnapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	decisionDigest, err := stage4TargetShapeDecisionDigest(
		gate.Plan.Decisions,
	)
	if err != nil {
		t.Fatal(err)
	}
	priorDigest, err := seed.snapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return Stage4TargetShapeAuthority{
		runID:               "projection-fixture",
		task:                stage4TargetShapeTask,
		topologyHash:        "projection-fixture-topology",
		sourceEngine:        sourceEngine,
		targetEngine:        targetEngine,
		targetMode:          targetMode,
		sourcePriorDigest:   sourcePriorDigest,
		sourceCurrentDigest: sourceCurrentDigest,
		sourceSuccessDigest: sourceSuccessDigest,
		decisionDigest:      decisionDigest,
		priorEvidenceDigest: priorDigest,
		priorCatalogDigest:  seed.catalogDigest,
		priorSnapshot:       cloneSchemaSnapshot(seed.snapshot),
		priorReservations: cloneTargetSchemaEvolutionReservations(
			seed.reservations,
		),
		capturedAt: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
	}
}

func stage4TargetSchemaProjectionSnapshot(
	t *testing.T,
	tables []schema.Table,
) schema.SchemaSnapshot {
	t.Helper()
	snapshot, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func stage4TargetSchemaProjectionCloneGate(
	t *testing.T,
	gate Stage4SchemaGateResult,
) Stage4SchemaGateResult {
	t.Helper()
	encoded, err := jsonMarshalStage4TargetSchemaProjectionGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := jsonUnmarshalStage4TargetSchemaProjectionGate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return cloned
}

// Keep gate cloning local to this test without depending on unexported
// schema-contract clone helpers.
func jsonMarshalStage4TargetSchemaProjectionGate(
	gate Stage4SchemaGateResult,
) ([]byte, error) {
	type cloneWire struct {
		Task                         state.TaskKey
		TopologyHash                 string
		Baseline                     bool
		PreviousSnapshot             schema.SchemaSnapshot
		CurrentSnapshot              schema.SchemaSnapshot
		Plan                         SchemaContractPlan
		RebuildCurrentSnapshot       schema.SchemaSnapshot
		RebuildRequiresTargetCatalog bool
	}
	return json.Marshal(cloneWire{
		Task:                         gate.Task,
		TopologyHash:                 gate.TopologyHash,
		Baseline:                     gate.Baseline,
		PreviousSnapshot:             gate.PreviousSnapshot,
		CurrentSnapshot:              gate.CurrentSnapshot,
		Plan:                         gate.Plan,
		RebuildCurrentSnapshot:       gate.RebuildCurrentSnapshot,
		RebuildRequiresTargetCatalog: gate.RebuildRequiresTargetCatalog,
	})
}

func jsonUnmarshalStage4TargetSchemaProjectionGate(
	encoded []byte,
) (Stage4SchemaGateResult, error) {
	var wire struct {
		Task                         state.TaskKey
		TopologyHash                 string
		Baseline                     bool
		PreviousSnapshot             schema.SchemaSnapshot
		CurrentSnapshot              schema.SchemaSnapshot
		Plan                         SchemaContractPlan
		RebuildCurrentSnapshot       schema.SchemaSnapshot
		RebuildRequiresTargetCatalog bool
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return Stage4SchemaGateResult{}, err
	}
	return Stage4SchemaGateResult{
		Task:                         wire.Task,
		TopologyHash:                 wire.TopologyHash,
		Baseline:                     wire.Baseline,
		PreviousSnapshot:             wire.PreviousSnapshot,
		CurrentSnapshot:              wire.CurrentSnapshot,
		Plan:                         wire.Plan,
		RebuildCurrentSnapshot:       wire.RebuildCurrentSnapshot,
		RebuildRequiresTargetCatalog: wire.RebuildRequiresTargetCatalog,
	}, nil
}

func stage4TargetSchemaProjectionFindTable(
	t *testing.T,
	tables []schema.Table,
	name string,
) schema.Table {
	t.Helper()
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("table %q not found in %#v", name, tables)
	return schema.Table{}
}

func stage4TargetSchemaProjectionFindColumn(
	t *testing.T,
	table schema.Table,
	name string,
) schema.Column {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			return column
		}
	}
	t.Fatalf("column %q not found in %#v", name, table)
	return schema.Column{}
}

type stage4TargetSchemaProjectionTestTarget struct {
	engine                 string
	targetSchema           string
	planCalls              int
	ioCalls                int
	sourceEngines          []string
	targetModes            []string
	planErr                error
	mutateInput            bool
	nondeterministic       bool
	renameFirstTable       bool
	inventIdentityFrontier bool
}

func (target *stage4TargetSchemaProjectionTestTarget) Engine() string {
	return target.engine
}

func (target *stage4TargetSchemaProjectionTestTarget) PlanTables(
	sourceEngine string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	target.planCalls++
	target.sourceEngines = append(target.sourceEngines, sourceEngine)
	target.targetModes = append(target.targetModes, mode)
	if target.mutateInput && len(tables) != 0 {
		tables[0].Name = "mutated"
	}
	if target.planErr != nil {
		return nil, target.planErr
	}
	result := cloneStage4TargetSchemaProjectionTables(tables)
	for tableIndex := range result {
		result[tableIndex].Schema = target.targetSchema
		for columnIndex := range result[tableIndex].Columns {
			result[tableIndex].Columns[columnIndex].Type = strings.ToUpper(
				result[tableIndex].Columns[columnIndex].Type,
			)
		}
		for foreignKeyIndex := range result[tableIndex].ForeignKeys {
			result[tableIndex].ForeignKeys[foreignKeyIndex].ReferencedSchema =
				target.targetSchema
		}
	}
	if target.nondeterministic && target.planCalls%2 == 0 &&
		len(result) != 0 {
		result[0].Schema += "_changed"
	}
	if target.renameFirstTable && len(result) != 0 {
		result[0].Name += "_changed"
	}
	if target.inventIdentityFrontier && len(result) != 0 &&
		result[0].Identity != nil {
		frontier := int64(99)
		result[0].Identity.Frontier = &frontier
	}
	return result, nil
}

func (target *stage4TargetSchemaProjectionTestTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	target.ioCalls++
	return nil
}

func (target *stage4TargetSchemaProjectionTestTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	target.ioCalls++
	return nil
}

func (target *stage4TargetSchemaProjectionTestTarget) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	target.ioCalls++
	return WriteReceipt{}, nil
}

func (target *stage4TargetSchemaProjectionTestTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	target.ioCalls++
	return 0, nil
}

func (target *stage4TargetSchemaProjectionTestTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	target.ioCalls++
	return nil
}

func (target *stage4TargetSchemaProjectionTestTarget) Close() error {
	target.ioCalls++
	return nil
}

var _ targetAdapter = (*stage4TargetSchemaProjectionTestTarget)(nil)
