package state

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestStage4AggregateReadConformance(t *testing.T) {
	factories := map[string]stage4AggregateFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend {
				return SQLiteStore{Path: path}
			}
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend {
				return YAMLStore{Path: path}
			}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Run("absent evidence reads empty", func(t *testing.T) {
				testStage4AggregateReadAbsent(t, factory)
			})
			t.Run("published evidence round-trips", func(t *testing.T) {
				testStage4AggregateReadRoundTrip(t, factory)
			})
			t.Run("reads drive run completion", func(t *testing.T) {
				testStage4AggregateReadDrivesRunCompletion(t, factory)
			})
			t.Run("completions are ordered by table", func(t *testing.T) {
				testStage4AggregateReadOrdering(t, factory)
			})
			t.Run("reads are scoped and validated", func(t *testing.T) {
				testStage4AggregateReadScope(t, factory)
			})
		})
	}
}

func testStage4AggregateReadAbsent(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	backend, _ := factory(t)
	aggregate, ok := backend.(Stage4AggregateBackend)
	if !ok {
		t.Fatalf("%T does not implement Stage4AggregateBackend", backend)
	}
	inventory, found, err := aggregate.LoadStage4TableInventory("absent-run")
	if err != nil || found {
		t.Fatalf(
			"absent inventory found=%v inventory=%#v err=%v",
			found,
			inventory,
			err,
		)
	}
	receipts, err := aggregate.LoadStage4TableCompletions("absent-run")
	if err != nil || len(receipts) != 0 {
		t.Fatalf("absent completions = %#v err=%v", receipts, err)
	}
}

func testStage4AggregateReadRoundTrip(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, true)

	// The inventory is durable before any table mutation; completions are not.
	inventory, found, err := fixture.aggregate.LoadStage4TableInventory(
		fixture.runID,
	)
	if err != nil || !found {
		t.Fatalf("inventory found=%v err=%v", found, err)
	}
	if inventory.Inventory.RunID != fixture.runID ||
		len(inventory.Inventory.Tables) != 1 ||
		inventory.Inventory.Tables[0].Task != fixture.task ||
		inventory.Inventory.Tables[0].TopologyHash != "table-topology" {
		t.Fatalf("inventory = %#v", inventory)
	}
	expectedInventory, err := newStage4TableInventoryReceipt(
		inventory.Inventory,
	)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Digest != expectedInventory.Digest {
		t.Fatalf(
			"inventory digest = %q, want %q",
			inventory.Digest,
			expectedInventory.Digest,
		)
	}
	receipts, err := fixture.aggregate.LoadStage4TableCompletions(
		fixture.runID,
	)
	if err != nil || len(receipts) != 0 {
		t.Fatalf("pre-completion receipts = %#v err=%v", receipts, err)
	}

	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}

	// A different process must recover the byte-identical completion, so read
	// through a freshly opened backend rather than the publishing one.
	restored, ok := fixture.reopen().(Stage4AggregateBackend)
	if !ok {
		t.Fatal("reopened backend lost aggregate capabilities")
	}
	receipts, err = restored.LoadStage4TableCompletions(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 1 {
		t.Fatalf("completion receipts = %#v", receipts)
	}
	normalized, err := normalizeStage4TableCompletion(fixture.completion)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(receipts[0].Completion, normalized) {
		t.Fatalf(
			"loaded completion = %#v, want %#v",
			receipts[0].Completion,
			normalized,
		)
	}
	if err := validateStage4TableCompletionReceipt(
		receipts[0],
		normalized,
	); err != nil {
		t.Fatalf("loaded receipt does not prove its own digest: %v", err)
	}
	if receipts[0].Completion.Incremental == nil {
		t.Fatal("loaded completion dropped its incremental evidence")
	}
}

// testStage4AggregateReadDrivesRunCompletion is the acceptance property for the
// read API: a process that never published a table receipt can still publish
// the run, because the exact completions it must supply are recoverable.
func testStage4AggregateReadDrivesRunCompletion(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture, completion := newStage4AggregateRunFixture(t, factory)
	restored, ok := fixture.reopen().(Stage4AggregateBackend)
	if !ok {
		t.Fatal("reopened backend lost aggregate capabilities")
	}
	inventory, found, err := restored.LoadStage4TableInventory(fixture.runID)
	if err != nil || !found {
		t.Fatalf("inventory found=%v err=%v", found, err)
	}
	receipts, err := restored.LoadStage4TableCompletions(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != len(inventory.Inventory.Tables) {
		t.Fatalf(
			"loaded %d receipts for %d inventory tables",
			len(receipts),
			len(inventory.Inventory.Tables),
		)
	}
	recovered := Stage4RunCompletion{
		RunID:       fixture.runID,
		Tables:      make([]Stage4TableCompletion, 0, len(receipts)),
		Sentinels:   completion.Sentinels,
		Reason:      completion.Reason,
		CompletedAt: completion.CompletedAt,
	}
	for _, receipt := range receipts {
		recovered.Tables = append(recovered.Tables, receipt.Completion)
	}
	if err := restored.(Stage4AggregateBackend).CompleteStage4Run(
		recovered,
	); err != nil {
		t.Fatalf("publish run from recovered receipts: %v", err)
	}
	latest, found, err := fixture.reopen().Latest()
	if err != nil || !found {
		t.Fatalf("latest run found=%v err=%v", found, err)
	}
	if latest.Outcome != Success ||
		latest.Resumable ||
		latest.Reason != completion.Reason ||
		!latest.EndedAt.Equal(completion.CompletedAt) {
		t.Fatalf("published run = %#v", latest)
	}
}

func testStage4AggregateReadOrdering(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	backend, reopen := factory(t)
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	runID := "ordering-run"
	// Publish in reverse-sorted order so insertion order cannot pass by luck.
	tables := []string{"zulu", "mike", "alpha"}
	if err := backend.InitializeRun(Run{
		ID: runID, Source: "source", Target: "target",
		SourceEngine: "postgres", SourceIdentity: "postgres:source/database",
		TargetIdentity: "postgres:target/database",
		Outcome:        Running, Resumable: true, Reason: "running",
		StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	stage4 := backend.(Stage4Backend)
	aggregate := backend.(Stage4AggregateBackend)
	snapshot := installStage4AggregateSchemaAuthority(
		t,
		backend,
		stage4,
		runID,
		started,
	)
	inventory := Stage4TableInventory{
		RunID:                runID,
		SchemaTask:           stage4SchemaContractSentinelTask,
		SchemaTopologyHash:   "schema-topology",
		SchemaSnapshotDigest: snapshot.Digest,
	}
	tasks := make([]TaskKey, len(tables))
	for index, table := range tables {
		tasks[index] = TaskKey{
			Type: "table-copy", Schema: "public", Table: table,
		}
		inventory.Tables = append(
			inventory.Tables,
			Stage4TableInventoryEntry{
				Table: table, Task: tasks[index], Strategy: "tuple_keyset",
				TopologyHash: "table-topology",
				Ranges:       []Stage4InventoryRange{{ID: "0"}},
			},
		)
	}
	if err := aggregate.EnsureStage4TableInventory(inventory); err != nil {
		t.Fatal(err)
	}
	completedAt := started.Add(time.Minute)
	for index, table := range tables {
		if err := backend.CreateTask(Task{
			RunID: runID, Table: table, StartedAt: started,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.(RangeBackend).EnsureWorkPlan(WorkTask{
			RunID: runID, Key: tasks[index], Strategy: "tuple_keyset",
			TopologyHash: "table-topology", StartedAt: started,
		}, []RangeState{{ID: "0"}}); err != nil {
			t.Fatal(err)
		}
		if err := aggregate.CompleteStage4Table(Stage4TableCompletion{
			RunID: runID, Table: table, Task: tasks[index],
			TopologyHash: "table-topology",
			Ranges:       []Stage4RangeCompletion{{ID: "0"}},
			CompletedAt:  completedAt,
		}); err != nil {
			t.Fatal(err)
		}
	}
	receipts, err := reopen().(Stage4AggregateBackend).
		LoadStage4TableCompletions(runID)
	if err != nil {
		t.Fatal(err)
	}
	loaded := make([]string, len(receipts))
	for index, receipt := range receipts {
		loaded[index] = receipt.Completion.Table
	}
	want := []string{"alpha", "mike", "zulu"}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("loaded completion order = %v, want %v", loaded, want)
	}
	// The loaded order must be the order a run completion is normalized into,
	// so the recovered slice can be supplied verbatim.
	recovered := Stage4RunCompletion{
		RunID: runID,
		Sentinels: []Stage4SentinelCompletion{{
			Task: stage4SchemaContractSentinelTask, RangeID: "aggregate-schema",
			TopologyHash: "schema-topology", Snapshot: snapshot,
		}},
		Reason:      "migration completed",
		CompletedAt: started.Add(2 * time.Minute),
	}
	for _, receipt := range receipts {
		recovered.Tables = append(recovered.Tables, receipt.Completion)
	}
	normalized, err := normalizeStage4RunCompletion(recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalized.Tables, recovered.Tables) {
		t.Fatalf(
			"normalization reordered recovered tables: %#v",
			normalized.Tables,
		)
	}
}

func testStage4AggregateReadScope(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, false)
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.aggregate.LoadStage4TableInventory(
		"  ",
	); !errors.Is(err, ErrState) {
		t.Fatalf("blank run ID inventory read = %v", err)
	}
	if _, err := fixture.aggregate.LoadStage4TableCompletions(
		"",
	); !errors.Is(err, ErrState) {
		t.Fatalf("blank run ID completion read = %v", err)
	}
	if _, found, err := fixture.aggregate.LoadStage4TableInventory(
		"other-run",
	); err != nil || found {
		t.Fatalf("foreign inventory found=%v err=%v", found, err)
	}
	receipts, err := fixture.aggregate.LoadStage4TableCompletions("other-run")
	if err != nil || len(receipts) != 0 {
		t.Fatalf("foreign completions = %#v err=%v", receipts, err)
	}
}

// TestStage4AggregateInventoryRevision pins the narrow window in which a table
// inventory may be replanned. A resumed run whose source grew must be able to
// revise its range plan, or it becomes unrecoverable rather than merely failed;
// the window must close the moment any table publishes terminal evidence.
func TestStage4AggregateInventoryRevision(t *testing.T) {
	factories := map[string]stage4AggregateFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend {
				return SQLiteStore{Path: path}
			}
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend {
				return YAMLStore{Path: path}
			}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Run("replans before terminal evidence", func(t *testing.T) {
				testStage4AggregateInventoryReplan(t, factory)
			})
			t.Run("is fixed after terminal evidence", func(t *testing.T) {
				testStage4AggregateInventoryFixed(t, factory)
			})
			t.Run("schema authority is immutable", func(t *testing.T) {
				testStage4AggregateInventorySchemaFixed(t, factory)
			})
		})
	}
}

// stage4AggregateFixtureInventory rebuilds the fixture's published inventory so
// a test can vary exactly one field.
func stage4AggregateFixtureInventory(
	t *testing.T,
	fixture stage4AggregateFixture,
) Stage4TableInventory {
	t.Helper()
	receipt, found, err := fixture.aggregate.LoadStage4TableInventory(
		fixture.runID,
	)
	if err != nil || !found {
		t.Fatalf("fixture inventory found=%v err=%v", found, err)
	}
	return receipt.Inventory
}

func testStage4AggregateInventoryReplan(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, false)
	revised := stage4AggregateFixtureInventory(t, fixture)
	revised.Tables[0].Ranges = []Stage4InventoryRange{{ID: "0"}, {ID: "1"}}
	if err := fixture.aggregate.EnsureStage4TableInventory(
		revised,
	); err != nil {
		t.Fatalf("replan before terminal evidence: %v", err)
	}
	stored := stage4AggregateFixtureInventory(t, fixture)
	if len(stored.Tables[0].Ranges) != 2 {
		t.Fatalf("revised inventory = %#v", stored)
	}
	// The revision is durable and replayable on its own terms.
	if err := fixture.aggregate.EnsureStage4TableInventory(
		revised,
	); err != nil {
		t.Fatalf("replay revised inventory: %v", err)
	}
}

func testStage4AggregateInventoryFixed(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, false)
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}
	revised := stage4AggregateFixtureInventory(t, fixture)
	revised.Tables[0].Ranges = []Stage4InventoryRange{{ID: "0"}, {ID: "1"}}
	err := fixture.aggregate.EnsureStage4TableInventory(revised)
	if !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("revision after terminal evidence = %v", err)
	}
	stored := stage4AggregateFixtureInventory(t, fixture)
	if len(stored.Tables[0].Ranges) != 1 {
		t.Fatalf("refused revision still mutated the inventory: %#v", stored)
	}
}

func testStage4AggregateInventorySchemaFixed(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, false)
	revised := stage4AggregateFixtureInventory(t, fixture)
	revised.SchemaTopologyHash = "different-schema-topology"
	revised.Tables[0].Ranges = []Stage4InventoryRange{{ID: "0"}, {ID: "1"}}
	err := fixture.aggregate.EnsureStage4TableInventory(revised)
	if !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("schema authority revision = %v", err)
	}
	stored := stage4AggregateFixtureInventory(t, fixture)
	if stored.SchemaTopologyHash != "schema-topology" ||
		len(stored.Tables[0].Ranges) != 1 {
		t.Fatalf("refused revision still mutated the inventory: %#v", stored)
	}
}

// TestStage4AggregateReadRejectsTamperedReceipt proves the read path is
// fail-closed rather than a raw decode. Both stores share the validation
// helper, so corrupting one durable document exercises it for both.
func TestStage4AggregateReadRejectsTamperedReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	store := YAMLStore{Path: path}
	factory := func(*testing.T) (Backend, func() Backend) {
		return store, func() Backend { return YAMLStore{Path: path} }
	}
	fixture := newStage4AggregateFixture(t, factory, false)
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		if len(document.Stage4TableCompletions) != 1 {
			t.Fatalf(
				"durable completions = %#v",
				document.Stage4TableCompletions,
			)
		}
		document.Stage4TableCompletions[0].Completion.RowsDone += 1
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadStage4TableCompletions(
		fixture.runID,
	); !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("tampered completion read = %v", err)
	}
}
