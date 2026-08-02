package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestPrepareStage4SchemaGateEstablishesBaselineOnBothBackends(t *testing.T) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
			initializeStage4LifecycleRun(t, backend, "baseline", started)
			run := stage4LifecycleRunContext(t, backend, "baseline", false)
			tables := []schema.Table{stage4LifecycleSimpleTable("public", "items")}

			result, err := PrepareStage4SchemaGate(
				run,
				tables,
				stage4LifecycleOptions(started),
			)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Baseline || len(result.Plan.Decisions) != 0 {
				t.Fatalf(
					"baseline=%v decisions=%#v",
					result.Baseline,
					result.Plan.Decisions,
				)
			}
			equal, err := schema.SchemaSnapshotsEqual(
				result.PreviousSnapshot,
				result.CurrentSnapshot,
			)
			if err != nil || !equal {
				t.Fatalf("baseline snapshots equal=%v err=%v", equal, err)
			}
			tasks, ranges, err := backend.(state.RangeBackend).ListWork("baseline")
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 || len(ranges) != 1 ||
				tasks[0].Key != stage4SchemaGateTask ||
				tasks[0].TopologyHash != result.TopologyHash {
				t.Fatalf("aggregate work = %#v %#v", tasks, ranges)
			}
			if _, found, err := backend.(state.Stage4Backend).LoadSchemaSnapshot(
				"baseline",
				stage4SchemaGateTask,
			); err != nil || found {
				t.Fatalf(
					"helper saved successful evidence: found=%v err=%v",
					found,
					err,
				)
			}
		})
	}
}

func TestPrepareStage4SchemaGateFirstUpsertEvolveAuthorizesExactCreates(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 30, 9, 15, 0, 0, time.UTC)
			initializeStage4LifecycleRun(t, backend, "first-evolve", started)
			tables := []schema.Table{
				stage4LifecycleSimpleTable("z", "later"),
				stage4LifecycleSimpleTable("a.b", "c"),
			}
			options := stage4LifecycleOptions(started)
			options.TargetMode = "upsert"
			options.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}

			result, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "first-evolve", false),
				tables,
				options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Baseline ||
				result.PreviousSnapshot.Version != schema.SchemaSnapshotVersion ||
				len(result.PreviousSnapshot.Tables) != 0 {
				t.Fatalf(
					"first evolve baseline=%v previous=%#v",
					result.Baseline,
					result.PreviousSnapshot,
				)
			}

			wantObjects := map[schema.SchemaDriftObject]bool{
				{
					Kind:   schema.SchemaDriftObjectTable,
					Schema: "z",
					Table:  "later",
				}: false,
				{
					Kind:   schema.SchemaDriftObjectTable,
					Schema: "a.b",
					Table:  "c",
				}: false,
			}
			if len(result.Plan.Decisions) != len(wantObjects) {
				t.Fatalf("create decisions = %#v", result.Plan.Decisions)
			}
			for _, decision := range result.Plan.Decisions {
				if decision.Entity != schema.SchemaContractTables ||
					decision.Mode != config.SchemaContractEvolve ||
					decision.ChangeKind != schema.SchemaDriftTableAdded ||
					decision.Action != SchemaContractCreateTable ||
					string(decision.Previous) != "null" ||
					string(decision.Current) == "null" ||
					decision.Reason == "" {
					t.Fatalf("non-exact first-run create decision = %#v", decision)
				}
				seen, ok := wantObjects[decision.Object]
				if !ok || seen {
					t.Fatalf("unexpected or duplicate create object = %#v", decision.Object)
				}
				wantObjects[decision.Object] = true
			}
			for object, seen := range wantObjects {
				if !seen {
					t.Fatalf("missing create decision for %#v", object)
				}
			}
			if equal, compareErr := schema.SchemaSnapshotsEqual(
				result.Plan.UpsertSnapshot,
				result.CurrentSnapshot,
			); compareErr != nil || !equal {
				t.Fatalf(
					"first evolve upsert projection equal=%v err=%v",
					equal,
					compareErr,
				)
			}
		})
	}
}

func TestPrepareStage4SchemaGateFirstBaselineDoesNotImplicitlyAuthorizeCreates(
	t *testing.T,
) {
	tests := []struct {
		name     string
		mode     string
		contract *config.SchemaContract
	}{
		{name: "upsert absent contract", mode: "upsert"},
		{
			name: "upsert report contract",
			mode: "upsert",
			contract: &config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractReport,
			},
		},
		{
			name: "upsert freeze contract",
			mode: "upsert",
			contract: &config.SchemaContract{
				Tables:   config.SchemaContractFreeze,
				Columns:  config.SchemaContractFreeze,
				DataType: config.SchemaContractFreeze,
			},
		},
		{
			name: "drop recreate evolve contract",
			mode: "drop_recreate",
			contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
			started := time.Date(2026, 7, 30, 9, 30, 0, 0, time.UTC)
			runID := strings.ReplaceAll(test.name, " ", "-")
			initializeStage4LifecycleRun(t, backend, runID, started)
			options := stage4LifecycleOptions(started)
			options.TargetMode = test.mode
			options.Contract = test.contract

			result, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, runID, false),
				[]schema.Table{stage4LifecycleSimpleTable("public", "items")},
				options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Baseline || len(result.Plan.Decisions) != 0 {
				t.Fatalf(
					"baseline=%v decisions=%#v",
					result.Baseline,
					result.Plan.Decisions,
				)
			}
			if equal, compareErr := schema.SchemaSnapshotsEqual(
				result.PreviousSnapshot,
				result.CurrentSnapshot,
			); compareErr != nil || !equal {
				t.Fatalf(
					"non-authorizing baseline equal=%v err=%v",
					equal,
					compareErr,
				)
			}
		})
	}
}

func TestPrepareStage4SchemaGateFirstUpsertEvolveResumeUsesStagedEvidence(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 30, 9, 45, 0, 0, time.UTC)
			initializeStage4LifecycleRun(t, backend, "first-evolve-resume", started)
			run := stage4LifecycleRunContext(
				t,
				backend,
				"first-evolve-resume",
				false,
			)
			options := stage4LifecycleOptions(started)
			options.TargetMode = "upsert"
			options.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}
			tables := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}

			first, err := PrepareStage4SchemaGate(run, tables, options)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.(state.Stage4Backend).SaveSchemaSnapshot(
				first.PendingSnapshot,
			); err != nil {
				t.Fatal(err)
			}

			run.Resume = true
			resumeOptions := options
			resumeOptions.CapturedAt = started.Add(time.Hour)
			resumed, err := PrepareStage4SchemaGate(run, tables, resumeOptions)
			if err != nil {
				t.Fatalf("resume first evolve baseline: %v", err)
			}
			if !resumed.Baseline ||
				len(resumed.PreviousSnapshot.Tables) != 0 ||
				!reflect.DeepEqual(resumed.Plan.Decisions, first.Plan.Decisions) ||
				!reflect.DeepEqual(resumed.PendingSnapshot, first.PendingSnapshot) {
				t.Fatalf(
					"resumed baseline=%v previous=%#v decisions=%#v pending=%#v; "+
						"want decisions=%#v pending=%#v",
					resumed.Baseline,
					resumed.PreviousSnapshot,
					resumed.Plan.Decisions,
					resumed.PendingSnapshot,
					first.Plan.Decisions,
					first.PendingSnapshot,
				)
			}

			changed := append(
				cloneStage4LifecycleTables(tables),
				stage4LifecycleSimpleTable("public", "new_items"),
			)
			if _, err := PrepareStage4SchemaGate(
				run,
				changed,
				resumeOptions,
			); err == nil ||
				!strings.Contains(
					err.Error(),
					"same-run successful schema projection differs",
				) {
				t.Fatalf("changed first-evolve resume error = %v", err)
			}
		})
	}
}

func TestPrepareStage4SchemaGateUsesLatestSuccessfulSnapshotAndPlansDrift(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
			previous := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			initializeStage4LifecycleRun(t, backend, "previous", started)
			previousResult, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "previous", false),
				previous,
				stage4LifecycleOptions(started),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.(state.Stage4Backend).SaveSchemaSnapshot(
				previousResult.PendingSnapshot,
			); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(state.Run{
				ID:        "previous",
				Source:    "source",
				Target:    "target",
				Outcome:   state.Success,
				Resumable: false,
				Reason:    "complete",
				StartedAt: started,
				EndedAt:   started.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}

			current := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			current[0].Columns = append(current[0].Columns, schema.Column{
				Name: "note", Type: "text", Nullable: true,
			})
			initializeStage4LifecycleRun(
				t,
				backend,
				"current",
				started.Add(2*time.Minute),
			)
			options := stage4LifecycleOptions(started.Add(2 * time.Minute))
			options.TargetMode = "upsert"
			options.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}
			result, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "current", false),
				current,
				options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.Baseline || len(result.Plan.Decisions) == 0 {
				t.Fatalf(
					"baseline=%v decisions=%#v",
					result.Baseline,
					result.Plan.Decisions,
				)
			}
			foundAdd := false
			for _, decision := range result.Plan.Decisions {
				if decision.ChangeKind == schema.SchemaDriftColumnAdded &&
					decision.Object.Schema == "public" &&
					decision.Object.Table == "items" &&
					decision.Object.Column == "note" &&
					decision.Action == SchemaContractAddColumn {
					foundAdd = true
				}
			}
			if !foundAdd {
				t.Fatalf("no add-column decision in %#v", result.Plan.Decisions)
			}
		})
	}
}

func TestPrepareStage4SchemaGateSelectsLatestSuccessfulBaselineByRunOrder(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			base := time.Date(2026, 7, 30, 10, 30, 0, 0, time.UTC)
			newer := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			newer[0].Columns = append(newer[0].Columns, schema.Column{
				Name: "newer_success", Type: "text", Nullable: true,
			})
			older := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			older[0].Columns = append(older[0].Columns, schema.Column{
				Name: "older_success", Type: "text", Nullable: true,
			})
			failed := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			failed[0].Columns = append(failed[0].Columns, schema.Column{
				Name: "failed_attempt", Type: "text", Nullable: true,
			})

			record := func(
				runID string,
				started time.Time,
				tables []schema.Table,
				outcome state.Outcome,
			) {
				t.Helper()
				initializeStage4LifecycleRun(t, backend, runID, started)
				result, err := PrepareStage4SchemaGate(
					stage4LifecycleRunContext(t, backend, runID, false),
					tables,
					stage4LifecycleOptions(started),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.(state.Stage4Backend).SaveSchemaSnapshot(
					result.PendingSnapshot,
				); err != nil {
					t.Fatal(err)
				}
				if err := backend.Append(state.Run{
					ID: runID, Source: "source", Target: "target",
					Outcome: outcome, Resumable: outcome != state.Success,
					Reason:    "fixture " + string(outcome),
					StartedAt: started, EndedAt: started.Add(time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
			}

			// Deliberately insert the newer success before the older success;
			// storage order must not decide the reusable baseline. A still
			// newer failed run must not become successful schema evidence.
			record("newer-success", base.Add(2*time.Hour), newer, state.Success)
			record("older-success", base, older, state.Success)
			record("failed-latest", base.Add(3*time.Hour), failed, state.Failed)

			initializeStage4LifecycleRun(
				t,
				backend,
				"current-order",
				base.Add(4*time.Hour),
			)
			result, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(
					t,
					backend,
					"current-order",
					false,
				),
				newer,
				stage4LifecycleOptions(base.Add(4*time.Hour)),
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.Baseline || len(result.Plan.Decisions) != 0 {
				t.Fatalf(
					"baseline=%v decisions=%#v",
					result.Baseline,
					result.Plan.Decisions,
				)
			}
			equal, err := schema.SchemaSnapshotsEqual(
				result.PreviousSnapshot,
				result.CurrentSnapshot,
			)
			if err != nil || !equal {
				t.Fatalf("latest successful baseline equal=%v err=%v", equal, err)
			}
		})
	}
}

func TestPrepareStage4SchemaGateRejectsChangedSameRunStagedSnapshot(t *testing.T) {
	backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	started := time.Date(2026, 7, 30, 11, 0, 0, 0, time.UTC)
	initializeStage4LifecycleRun(t, backend, "resume-run", started)
	run := stage4LifecycleRunContext(t, backend, "resume-run", false)
	options := stage4LifecycleOptions(started)
	first := []schema.Table{stage4LifecycleSimpleTable("a.b", "c")}
	staged, err := PrepareStage4SchemaGate(run, first, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(staged.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	changed := []schema.Table{stage4LifecycleSimpleTable("a.b", "c")}
	changed[0].Columns = append(changed[0].Columns, schema.Column{
		Name: "changed", Type: "text", Nullable: true,
	})
	if _, err := PrepareStage4SchemaGate(run, changed, options); err == nil ||
		!strings.Contains(err.Error(), "same-run successful schema projection differs") {
		t.Fatalf("same-run mismatch error = %v", err)
	}
}

func TestStage4SchemaGateTopologyExcludesDiscoveryAndBindsConfiguration(
	t *testing.T,
) {
	base := stage4LifecycleOptions(time.Time{})
	first, _, err := stage4SchemaGateTopology(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.IncludeTables = []string{"z.*", "a.b"}
	base.IncludeTables = []string{"a.b", "z.*"}
	second, _, err := stage4SchemaGateTopology(reordered)
	if err != nil {
		t.Fatal(err)
	}
	third, _, err := stage4SchemaGateTopology(base)
	if err != nil {
		t.Fatal(err)
	}
	if first == third || second != third {
		t.Fatalf(
			"topologies first=%q reordered=%q canonical=%q",
			first,
			second,
			third,
		)
	}

	for name, mutate := range map[string]func(*Stage4SchemaGateOptions){
		"route": func(value *Stage4SchemaGateOptions) {
			value.TargetEngine = "mysql"
		},
		"mode": func(value *Stage4SchemaGateOptions) {
			value.TargetMode = "upsert"
		},
		"include": func(value *Stage4SchemaGateOptions) {
			value.IncludeTables = append(value.IncludeTables, "other")
		},
		"exclude": func(value *Stage4SchemaGateOptions) {
			value.ExcludeTables = []string{"secret.*"}
		},
		"config": func(value *Stage4SchemaGateOptions) {
			value.ConfigIdentity = "configuration-b"
		},
		"contract": func(value *Stage4SchemaGateOptions) {
			value.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractFreeze,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractReport,
			}
		},
		"fail on drift": func(value *Stage4SchemaGateOptions) {
			value.FailOnSchemaDrift = true
		},
		"date columns": func(value *Stage4SchemaGateOptions) {
			value.DateUpdatedColumns = []string{"updated_at", "modified_at"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, _, err := stage4SchemaGateTopology(changed)
			if err != nil {
				t.Fatal(err)
			}
			if got == third {
				t.Fatalf("%s did not change topology", name)
			}
		})
	}

	// Discovered schema is intentionally not an input to the topology wire.
	left := []schema.Table{stage4LifecycleSimpleTable("quoted.schema", "table")}
	right := []schema.Table{stage4LifecycleSimpleTable("quoted.schema", "table")}
	right[0].Columns = append(right[0].Columns, schema.Column{
		Name: "new.column", Type: "text", Nullable: true,
	})
	if reflect.DeepEqual(left, right) {
		t.Fatal("test discovery fixtures unexpectedly equal")
	}
	stable, _, err := stage4SchemaGateTopology(base)
	if err != nil || stable != third {
		t.Fatalf("discovery affected topology: %q err=%v", stable, err)
	}
}

func TestPrepareStage4SchemaGateWritesPlanBeforeReadsAndFailsClosed(t *testing.T) {
	started := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	table := []schema.Table{stage4LifecycleSimpleTable("", "items")}
	t.Run("ordered", func(t *testing.T) {
		backend := &stage4LifecycleBackendSpy{}
		run := stage4LifecycleRunContext(t, backend, "ordered", false)
		if _, err := PrepareStage4SchemaGate(
			run,
			table,
			stage4LifecycleOptions(started),
		); err != nil {
			t.Fatal(err)
		}
		want := []string{"ensure", "load-same-run", "load-prior"}
		if !reflect.DeepEqual(backend.events, want) {
			t.Fatalf("events = %#v, want %#v", backend.events, want)
		}
	})
	t.Run("write failure", func(t *testing.T) {
		backend := &stage4LifecycleBackendSpy{
			ensureErr: errors.New("disk full"),
		}
		run := stage4LifecycleRunContext(t, backend, "write-failure", false)
		if _, err := PrepareStage4SchemaGate(
			run,
			table,
			stage4LifecycleOptions(started),
		); err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("write failure = %v", err)
		}
		if !reflect.DeepEqual(backend.events, []string{"ensure"}) {
			t.Fatalf("events after write failure = %#v", backend.events)
		}
	})
	t.Run("read failure", func(t *testing.T) {
		backend := &stage4LifecycleBackendSpy{
			loadSameRunErr: errors.New("corrupt state"),
		}
		run := stage4LifecycleRunContext(t, backend, "read-failure", false)
		if _, err := PrepareStage4SchemaGate(
			run,
			table,
			stage4LifecycleOptions(started),
		); err == nil || !strings.Contains(err.Error(), "corrupt state") {
			t.Fatalf("read failure = %v", err)
		}
		if !reflect.DeepEqual(
			backend.events,
			[]string{"ensure", "load-same-run"},
		) {
			t.Fatalf("events after read failure = %#v", backend.events)
		}
	})
}

func TestStage4SchemaGateProjectionPrunesDependenciesAndPreservesRichMetadata(
	t *testing.T,
) {
	defaultExpression, err := schema.ParseSQLiteDefault(`'kept'`)
	if err != nil {
		t.Fatal(err)
	}
	discardedCheck, err := schema.ParseSQLiteCheckExpression(
		`discarded_value IS NULL`,
	)
	if err != nil {
		t.Fatal(err)
	}
	keptCheck, err := schema.ParseSQLiteCheckExpression(`id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	rich := []schema.Table{
		{
			Schema:            "a.b",
			Name:              "c",
			MySQLCollation:    "utf8mb4_bin",
			ClickHouseOrderBy: []string{"id"},
			Identity: &schema.Identity{
				Column:     "id",
				Generation: schema.IdentityByDefault,
				Frontier:   &frontier,
			},
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint", PrimaryKey: true,
					PrimaryKeyPosition: 1,
				},
				{
					Name: "kept", Type: "text", Nullable: true,
					DeclaredType: &schema.DeclaredType{
						Base: "varchar", Arguments: []int{80},
					},
					Default: defaultExpression,
				},
				{
					Name: "discarded_value", Type: "bigint", Nullable: true,
				},
			},
			Indexes: []schema.Index{
				{
					Name: "kept.index", Columns: []schema.IndexColumn{
						{Name: "kept", Descending: true, Collation: "binary"},
					},
				},
				{
					Name:    "discarded.index",
					Columns: []schema.IndexColumn{{Name: "discarded_value"}},
				},
			},
			ForeignKeys: []schema.ForeignKey{
				{
					Name: "discarded.fk", Columns: []string{"discarded_value"},
					ReferencedTable:   "lookup.table",
					ReferencedColumns: []string{"id"},
					OnUpdate:          "NO ACTION", OnDelete: "SET NULL",
				},
			},
			Checks: []schema.CheckConstraint{
				{Name: "discarded.check", Expression: discardedCheck},
				{Name: "kept.check", Expression: keptCheck},
			},
		},
		{
			Schema: "a",
			Name:   "b.c",
			Columns: []schema.Column{{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
			}},
		},
		{
			Schema: "a.b",
			Name:   "lookup.table",
			Columns: []schema.Column{{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
			}},
		},
	}
	original := cloneStage4LifecycleTables(rich)
	previous := cloneStage4LifecycleTables(rich)
	previous[0].Columns = previous[0].Columns[:2]
	previous[0].Indexes = previous[0].Indexes[:1]
	previous[0].ForeignKeys = nil
	previous[0].Checks = previous[0].Checks[1:]

	backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	started := time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)
	initializeStage4LifecycleRun(t, backend, "previous-rich", started)
	previousResult, err := PrepareStage4SchemaGate(
		stage4LifecycleRunContext(t, backend, "previous-rich", false),
		previous,
		stage4LifecycleOptions(started),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(previousResult.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID: "previous-rich", Source: "source", Target: "target",
		Outcome: state.Success, Reason: "complete", StartedAt: started,
		EndedAt: started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	initializeStage4LifecycleRun(
		t,
		backend,
		"current-rich",
		started.Add(2*time.Minute),
	)
	options := stage4LifecycleOptions(started.Add(2 * time.Minute))
	options.TargetMode = "upsert"
	options.Contract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractDiscardValue,
		DataType: config.SchemaContractEvolve,
	}
	result, err := PrepareStage4SchemaGate(
		stage4LifecycleRunContext(t, backend, "current-rich", false),
		rich,
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rich, original) {
		t.Fatal("schema gate mutated incoming rich metadata")
	}
	for name, projection := range map[string][]schema.Table{
		"transfer":   result.TransferTables,
		"validation": result.ValidationTables,
		"rebuild":    result.RebuildCurrentTables,
	} {
		projected := stage4LifecycleFindTable(t, projection, "a.b", "c")
		if len(projected.Columns) != 2 ||
			projected.Columns[1].Name != "kept" ||
			len(projected.Indexes) != 1 ||
			projected.Indexes[0].Name != "kept.index" ||
			len(projected.ForeignKeys) != 0 ||
			len(projected.Checks) != 1 ||
			projected.Checks[0].Name != "kept.check" {
			t.Fatalf("%s projection = %#v", name, projected)
		}
		if projected.Identity == nil || projected.Identity.Frontier == nil ||
			*projected.Identity.Frontier != frontier ||
			projected.Columns[1].Default == nil ||
			projected.Columns[1].Default.CanonicalSQL() != `'kept'` ||
			projected.Columns[1].DeclaredType == nil ||
			!reflect.DeepEqual(
				projected.Columns[1].DeclaredType.Arguments,
				[]int{80},
			) {
			t.Fatalf("%s lost rich metadata: %#v", name, projected)
		}
	}
	if len(result.Plan.SuccessfulSnapshot.Tables) != 3 {
		t.Fatalf(
			"successful snapshot = %#v",
			result.Plan.SuccessfulSnapshot,
		)
	}
	if len(result.Plan.UpsertSnapshot.Tables) != 3 {
		t.Fatalf("upsert snapshot = %#v", result.Plan.UpsertSnapshot)
	}

	// Both colliding dotted display names remain distinct structural keys.
	if stage4LifecycleFindTable(t, result.TransferTables, "a", "b.c").Name !=
		"b.c" {
		t.Fatal("dotted identifier table was not projected structurally")
	}

	var mutable *schema.Table
	for index := range result.TransferTables {
		if result.TransferTables[index].Schema == "a.b" &&
			result.TransferTables[index].Name == "c" {
			mutable = &result.TransferTables[index]
			break
		}
	}
	if mutable == nil {
		t.Fatal("mutable projected table not found")
	}
	mutable.ClickHouseOrderBy[0] = "mutated"
	*mutable.Identity.Frontier = 999
	mutable.Columns[1].DeclaredType.Arguments[0] = 1
	mutable.Indexes[0].Columns[0].Name = "mutated"
	if !reflect.DeepEqual(rich, original) {
		t.Fatal("projected metadata aliases incoming rich metadata")
	}
}

func TestStage4SchemaGateTypeDiscardRetainsSuccessfulEvidenceWithoutRichProjection(
	t *testing.T,
) {
	previous := []schema.Table{stage4LifecycleSimpleTable("", "items")}
	previous[0].Columns = append(previous[0].Columns, schema.Column{
		Name: "value", Type: "bigint", Nullable: true,
	})
	current := cloneStage4LifecycleTables(previous)
	current[0].Columns[1].Type = "text"

	backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	started := time.Date(2026, 7, 30, 14, 0, 0, 0, time.UTC)
	initializeStage4LifecycleRun(t, backend, "type-previous", started)
	baseline, err := PrepareStage4SchemaGate(
		stage4LifecycleRunContext(t, backend, "type-previous", false),
		previous,
		stage4LifecycleOptions(started),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(baseline.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID: "type-previous", Source: "source", Target: "target",
		Outcome: state.Success, Reason: "complete", StartedAt: started,
		EndedAt: started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	initializeStage4LifecycleRun(
		t,
		backend,
		"type-current",
		started.Add(2*time.Minute),
	)
	options := stage4LifecycleOptions(started.Add(2 * time.Minute))
	options.TargetMode = "upsert"
	options.Contract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractDiscardValue,
	}
	currentRun := stage4LifecycleRunContext(
		t,
		backend,
		"type-current",
		false,
	)
	result, err := PrepareStage4SchemaGate(
		currentRun,
		current,
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, tables := range map[string][]schema.Table{
		"transfer": result.TransferTables, "validation": result.ValidationTables,
		"rebuild": result.RebuildCurrentTables,
	} {
		if got := stage4LifecycleFindTable(t, tables, "", "items"); len(got.Columns) != 1 {
			t.Fatalf("%s columns = %#v", name, got.Columns)
		}
	}
	successful := result.Plan.SuccessfulSnapshot.Tables[0]
	if len(successful.Columns) != 2 ||
		successful.Columns[1].Name != "value" ||
		successful.Columns[1].Type != "bigint" {
		t.Fatalf("successful retained evidence = %#v", successful.Columns)
	}
	pending, err := schema.ParseSchemaSnapshot(
		[]byte(result.PendingSnapshot.CanonicalJSON),
	)
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := schema.SchemaSnapshotsEqual(
		pending,
		result.Plan.SuccessfulSnapshot,
	); err != nil || !equal {
		t.Fatalf("pending successful projection equal=%v err=%v", equal, err)
	}
	if err := backend.SaveSchemaSnapshot(result.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	resumeOptions := options
	resumeOptions.CapturedAt = options.CapturedAt.Add(time.Hour)
	currentRun.Resume = true
	resumed, err := PrepareStage4SchemaGate(
		currentRun,
		current,
		resumeOptions,
	)
	if err != nil {
		t.Fatalf("resume staged successful projection: %v", err)
	}
	if !resumed.PendingSnapshot.CapturedAt.Equal(
		result.PendingSnapshot.CapturedAt,
	) {
		t.Fatalf(
			"resume replaced staged capture time: got %v want %v",
			resumed.PendingSnapshot.CapturedAt,
			result.PendingSnapshot.CapturedAt,
		)
	}
}

func TestSelectStage4ForeignKeysIncludesReferencedSchemaInIdentity(
	t *testing.T,
) {
	t.Parallel()

	rich := []schema.ForeignKey{
		{
			Name:              "events_accounts_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "identity",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		},
		{
			Name:              "events_accounts_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "archive",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		},
	}
	requested := []schema.SnapshotForeignKey{{
		Name:              "events_accounts_fk",
		Columns:           []string{"account_id"},
		ReferencedSchema:  "identity",
		ReferencedTable:   "accounts",
		ReferencedColumns: []string{"id"},
	}}
	selected, err := selectStage4ForeignKeys(rich, requested)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 ||
		selected[0].ReferencedSchema != "identity" {
		t.Fatalf("selected foreign keys = %#v", selected)
	}

	requested[0].ReferencedSchema = "missing"
	if _, err := selectStage4ForeignKeys(rich, requested); err == nil {
		t.Fatal("unknown qualified foreign-key identity unexpectedly selected")
	}
}

func TestCloneStage4RichColumnDoesNotAliasStructuredCatalogType(
	t *testing.T,
) {
	t.Parallel()

	length := int64(80)
	width := int64(12)
	srid := uint32(4326)
	sources := []schema.Column{
		{
			Name: "label",
			Type: "text",
			DeclaredType: &schema.DeclaredType{
				Base:   "varchar",
				Length: &length,
			},
		},
		{
			Name: "position",
			Type: "geometry",
			DeclaredType: &schema.DeclaredType{
				Base: "geometry",
				Spatial: &schema.SpatialTypeMetadata{
					Subtype: schema.SpatialSubtypePoint,
					SRID:    &srid,
				},
			},
		},
		{
			Name: "flags",
			Type: "binary",
			DeclaredType: &schema.DeclaredType{
				Base:  "bit",
				MySQL: &schema.MySQLTypeMetadata{BitWidth: &width},
			},
		},
		{
			Name: "choice",
			Type: "text",
			DeclaredType: &schema.DeclaredType{
				Base: "enum",
				MySQL: &schema.MySQLTypeMetadata{
					EnumMembers: []string{"a", "b"},
				},
			},
		},
		{
			Name: "tags",
			Type: "text",
			DeclaredType: &schema.DeclaredType{
				Base: "set",
				MySQL: &schema.MySQLTypeMetadata{
					SetMembers: []string{"x", "y"},
				},
			},
		},
	}
	clones := make([]schema.Column, len(sources))
	for index, source := range sources {
		clones[index] = cloneStage4RichColumn(source)
	}
	*clones[0].DeclaredType.Length = 1
	*clones[1].DeclaredType.Spatial.SRID = 0
	*clones[2].DeclaredType.MySQL.BitWidth = 1
	clones[3].DeclaredType.MySQL.EnumMembers[0] = "changed"
	clones[4].DeclaredType.MySQL.SetMembers[0] = "changed"
	if *sources[0].DeclaredType.Length != 80 ||
		*sources[1].DeclaredType.Spatial.SRID != 4326 ||
		*sources[2].DeclaredType.MySQL.BitWidth != 12 ||
		sources[3].DeclaredType.MySQL.EnumMembers[0] != "a" ||
		sources[4].DeclaredType.MySQL.SetMembers[0] != "x" {
		t.Fatal("Stage 4 rich-column clone retained structured metadata aliases")
	}
}

func TestStage4SchemaGateRepresentsRetainedDropsAsTargetCatalogRequirement(
	t *testing.T,
) {
	previous := []schema.Table{stage4LifecycleSimpleTable("", "items")}
	previous[0].Columns = append(previous[0].Columns, schema.Column{
		Name: "retained_target_only", Type: "text", Nullable: true,
	})
	current := []schema.Table{stage4LifecycleSimpleTable("", "items")}
	backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	started := time.Date(2026, 7, 30, 15, 0, 0, 0, time.UTC)
	initializeStage4LifecycleRun(t, backend, "drop-previous", started)
	baseline, err := PrepareStage4SchemaGate(
		stage4LifecycleRunContext(t, backend, "drop-previous", false),
		previous,
		stage4LifecycleOptions(started),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(baseline.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID: "drop-previous", Source: "source", Target: "target",
		Outcome: state.Success, Reason: "complete", StartedAt: started,
		EndedAt: started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	initializeStage4LifecycleRun(
		t,
		backend,
		"drop-current",
		started.Add(2*time.Minute),
	)
	options := stage4LifecycleOptions(started.Add(2 * time.Minute))
	options.Contract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	result, err := PrepareStage4SchemaGate(
		stage4LifecycleRunContext(t, backend, "drop-current", false),
		current,
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RebuildRequiresTargetCatalog {
		t.Fatal("retained source drop did not require the target-catalog seam")
	}
	if len(result.RebuildCurrentTables) != 1 ||
		len(result.RebuildCurrentTables[0].Columns) != 1 {
		t.Fatalf(
			"current-backed rebuild tables = %#v",
			result.RebuildCurrentTables,
		)
	}
	required := result.Plan.RebuildSnapshot.Tables[0]
	if len(required.Columns) != 2 ||
		required.Columns[1].Name != "retained_target_only" {
		t.Fatalf("required rebuild snapshot = %#v", required)
	}
}

func TestStage4SchemaGateSuccessfulBaselineCannotBeReplacedByRetainedTargetShape(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 30, 15, 30, 0, 0, time.UTC)
			contract := &config.SchemaContract{
				Tables:   config.SchemaContractFreeze,
				Columns:  config.SchemaContractDiscardRow,
				DataType: config.SchemaContractFreeze,
			}
			options := stage4LifecycleOptions(started)
			options.TargetMode = "upsert"
			options.Contract = contract

			firstTables := []schema.Table{
				stage4LifecycleSimpleTable("public", "items"),
			}
			firstTables[0].Columns = append(
				firstTables[0].Columns,
				schema.Column{
					Name: "value", Type: "text",
				},
			)
			initializeStage4LifecycleRun(t, backend, "policy-first", started)
			first, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "policy-first", false),
				firstTables,
				options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.(state.Stage4Backend).SaveSchemaSnapshot(
				first.PendingSnapshot,
			); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(state.Run{
				ID: "policy-first", Source: "source", Target: "target",
				Outcome: state.Success, Reason: "complete", StartedAt: started,
				EndedAt: started.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}

			changedTables := cloneStage4LifecycleTables(firstTables)
			changedTables[0].Columns[1].Nullable = true
			secondStarted := started.Add(2 * time.Minute)
			initializeStage4LifecycleRun(
				t,
				backend,
				"policy-discard",
				secondStarted,
			)
			options.CapturedAt = secondStarted
			second, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "policy-discard", false),
				changedTables,
				options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(second.Plan.SuccessfulSnapshot.Tables) != 0 ||
				len(second.Plan.UpsertSnapshot.Tables) != 1 {
				t.Fatalf(
					"second-run successful=%#v upsert=%#v",
					second.Plan.SuccessfulSnapshot,
					second.Plan.UpsertSnapshot,
				)
			}
			pending, err := schema.ParseSchemaSnapshot(
				[]byte(second.PendingSnapshot.CanonicalJSON),
			)
			if err != nil {
				t.Fatal(err)
			}
			if equal, compareErr := schema.SchemaSnapshotsEqual(
				pending,
				second.Plan.SuccessfulSnapshot,
			); compareErr != nil || !equal {
				t.Fatalf(
					"durable successful source baseline equal=%v err=%v",
					equal,
					compareErr,
				)
			}
			if equal, compareErr := schema.SchemaSnapshotsEqual(
				pending,
				second.Plan.UpsertSnapshot,
			); compareErr != nil || equal {
				t.Fatalf(
					"retained target shape replaced successful baseline equal=%v err=%v",
					equal,
					compareErr,
				)
			}
			if err := backend.(state.Stage4Backend).SaveSchemaSnapshot(
				second.PendingSnapshot,
			); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(state.Run{
				ID: "policy-discard", Source: "source", Target: "target",
				Outcome: state.Success, Reason: "complete",
				StartedAt: secondStarted,
				EndedAt:   secondStarted.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}

			thirdStarted := started.Add(4 * time.Minute)
			initializeStage4LifecycleRun(
				t,
				backend,
				"policy-reverted",
				thirdStarted,
			)
			options.CapturedAt = thirdStarted
			third, thirdErr := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(t, backend, "policy-reverted", false),
				firstTables,
				options,
			)
			if thirdErr == nil ||
				!strings.Contains(thirdErr.Error(), "freeze") ||
				!strings.Contains(thirdErr.Error(), "table_added") {
				t.Fatalf(
					"reverted source bypassed table freeze: plan=%#v error=%v",
					third.Plan,
					thirdErr,
				)
			}
		})
	}
}

func TestStage4RunContextValidationRejectsUnsafeSpoolsAndTypedNil(t *testing.T) {
	backend := &stage4LifecycleBackendSpy{}
	runID := "context-run"
	private := stage4LifecyclePrivateSpool(t, runID)
	if err := (Stage4RunContext{
		RunID: runID, Backend: backend, SpoolDirectory: private,
	}).Validate(); err != nil {
		t.Fatalf("valid context: %v", err)
	}

	var typedNil *stage4LifecycleBackendSpy
	for name, run := range map[string]Stage4RunContext{
		"blank run": {
			Backend: backend, SpoolDirectory: private,
		},
		"typed nil backend": {
			RunID: runID, Backend: typedNil, SpoolDirectory: private,
		},
		"relative": {
			RunID: runID, Backend: backend, SpoolDirectory: "relative",
		},
		"wrong run": {
			RunID: "other", Backend: backend, SpoolDirectory: private,
		},
		"missing": {
			RunID: runID, Backend: backend,
			SpoolDirectory: filepath.Join(filepath.Dir(private), "missing"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run.Validate(); err == nil {
				t.Fatalf("unsafe context accepted: %#v", run)
			}
		})
	}

	insecure := stage4LifecyclePrivateSpool(t, "insecure")
	if err := os.Chmod(insecure, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := (Stage4RunContext{
		RunID: "insecure", Backend: backend, SpoolDirectory: insecure,
	}).Validate(); err == nil || !strings.Contains(err.Error(), "owner-only") {
		t.Fatalf("insecure permissions error = %v", err)
	}

	inaccessible := stage4LifecyclePrivateSpool(t, "inaccessible")
	if err := os.Chmod(inaccessible, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (Stage4RunContext{
		RunID: "inaccessible", Backend: backend, SpoolDirectory: inaccessible,
	}).Validate(); err == nil || !strings.Contains(err.Error(), "owner-accessible") {
		t.Fatalf("inaccessible permissions error = %v", err)
	}

	parentRun := "insecure-parent"
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	parentSpool := filepath.Join(parent, stage4LifecycleRunDigest(parentRun))
	if err := os.Mkdir(parentSpool, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := (Stage4RunContext{
		RunID: parentRun, Backend: backend, SpoolDirectory: parentSpool,
	}).Validate(); err == nil || !strings.Contains(err.Error(), "spool parent") {
		t.Fatalf("insecure parent error = %v", err)
	}

	fileRun := "file-run"
	filePath := filepath.Join(
		filepath.Dir(private),
		stage4LifecycleRunDigest(fileRun),
	)
	if err := os.WriteFile(filePath, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (Stage4RunContext{
		RunID: fileRun, Backend: backend, SpoolDirectory: filePath,
	}).Validate(); err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("file path error = %v", err)
	}

	symlinkRun := "symlink-run"
	symlinkTarget := stage4LifecyclePrivateSpool(t, "symlink-target")
	symlinkPath := filepath.Join(
		filepath.Dir(private),
		stage4LifecycleRunDigest(symlinkRun),
	)
	if err := os.Symlink(symlinkTarget, symlinkPath); err != nil {
		t.Fatal(err)
	}
	if err := (Stage4RunContext{
		RunID: symlinkRun, Backend: backend, SpoolDirectory: symlinkPath,
	}).Validate(); err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("symlink path error = %v", err)
	}
}

func TestResolveStage4RunContextIsOptionalAndReadOnly(t *testing.T) {
	legacy := stage4LifecycleObserver{}
	if _, found, err := ResolveStage4RunContext(legacy); err != nil || found {
		t.Fatalf("legacy context found=%v err=%v", found, err)
	}
	backend := &stage4LifecycleBackendSpy{}
	run := Stage4RunContext{
		RunID:          "observed",
		Backend:        backend,
		Resume:         true,
		SpoolDirectory: stage4LifecyclePrivateSpool(t, "observed"),
	}
	observer := stage4LifecycleRunObserver{run: run}
	resolved, found, err := ResolveStage4RunContext(observer)
	if err != nil || !found || !resolved.Resume || len(backend.events) != 0 {
		t.Fatalf(
			"resolved=%#v found=%v events=%#v err=%v",
			resolved,
			found,
			backend.events,
			err,
		)
	}
}

type stage4LifecycleBackendSpy struct {
	Stage4StateBackend
	events         []string
	ensureErr      error
	loadSameRunErr error
	loadPriorErr   error
}

func (backend *stage4LifecycleBackendSpy) EnsureWorkPlan(
	state.WorkTask,
	[]state.RangeState,
) (bool, error) {
	backend.events = append(backend.events, "ensure")
	return true, backend.ensureErr
}

func (backend *stage4LifecycleBackendSpy) LoadSchemaSnapshot(
	string,
	state.TaskKey,
) (state.SchemaSnapshot, bool, error) {
	backend.events = append(backend.events, "load-same-run")
	return state.SchemaSnapshot{}, false, backend.loadSameRunErr
}

func (backend *stage4LifecycleBackendSpy) LoadLatestApplicableSchemaSnapshot(
	string,
	state.TaskKey,
) (state.SchemaSnapshot, bool, error) {
	backend.events = append(backend.events, "load-prior")
	return state.SchemaSnapshot{}, false, backend.loadPriorErr
}

type stage4LifecycleObserver struct{}

func (stage4LifecycleObserver) BeforeTable(
	_ context.Context,
	_ string,
) error {
	return nil
}

func (stage4LifecycleObserver) AfterTable(
	_ context.Context,
	_ string,
	_ int,
) error {
	return nil
}

type stage4LifecycleRunObserver struct {
	stage4LifecycleObserver
	run Stage4RunContext
	err error
}

func (observer stage4LifecycleRunObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, observer.err
}

type stage4LifecycleTestBackend interface {
	state.Backend
	Stage4StateBackend
}

func stage4LifecycleBackendFactories() map[string]func(*testing.T) stage4LifecycleTestBackend {
	return map[string]func(*testing.T) stage4LifecycleTestBackend{
		"sqlite": func(t *testing.T) stage4LifecycleTestBackend {
			return state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		},
		"yaml": func(t *testing.T) stage4LifecycleTestBackend {
			return state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		},
	}
}

func initializeStage4LifecycleRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	started time.Time,
) {
	t.Helper()
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/app",
		TargetIdentity: "postgres:target.example:5432/app",
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      started,
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
}

func stage4LifecycleRunContext(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	resume bool,
) Stage4RunContext {
	t.Helper()
	return Stage4RunContext{
		RunID:          runID,
		Backend:        backend,
		Resume:         resume,
		SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
	}
}

func stage4LifecyclePrivateSpool(t *testing.T, runID string) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, stage4LifecycleRunDigest(runID))
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func stage4LifecycleRunDigest(runID string) string {
	digest := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(digest[:])
}

func stage4LifecycleOptions(capturedAt time.Time) Stage4SchemaGateOptions {
	return Stage4SchemaGateOptions{
		SourceEngine:   "postgres",
		TargetEngine:   "postgres",
		TargetMode:     "drop_recreate",
		ConfigIdentity: "configuration-a",
		CapturedAt:     capturedAt,
	}
}

func stage4LifecycleSimpleTable(schemaName, tableName string) schema.Table {
	return schema.Table{
		Schema: schemaName,
		Name:   tableName,
		Columns: []schema.Column{{
			Name: "id", Type: "bigint", PrimaryKey: true,
			PrimaryKeyPosition: 1,
		}},
	}
}

func cloneStage4LifecycleTables(tables []schema.Table) []schema.Table {
	result := make([]schema.Table, len(tables))
	for index, table := range tables {
		result[index] = cloneStage4RichTable(table)
	}
	return result
}

func stage4LifecycleFindTable(
	t *testing.T,
	tables []schema.Table,
	schemaName string,
	tableName string,
) schema.Table {
	t.Helper()
	for _, table := range tables {
		if table.Schema == schemaName && table.Name == tableName {
			return table
		}
	}
	t.Fatalf("table (%q, %q) not found in %#v", schemaName, tableName, tables)
	return schema.Table{}
}
