package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func initializeSQLiteStrictRun(
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
		SourceEngine:   "sqlite",
		SourceIdentity: "sqlite:/tmp/source.db",
		TargetIdentity: "sqlite:/tmp/target.db",
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      started,
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
}

// TestSQLiteStrictComposesWithTheCoordinator drives the SQLite opener through
// the real strict coordinator and a real state backend rather than exercising
// its methods in isolation.
//
// An opener that satisfies its own unit tests can still fail the contract the
// core actually enforces — durable work agreement, evidence shape, snapshot
// reference grammar, lifecycle ordering. This is the test that proves the
// implementation is usable, not merely self-consistent.
func TestSQLiteStrictComposesWithTheCoordinator(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Exec(
		`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT);
		 INSERT INTO items (id, payload) VALUES (1,'a'), (2,'b'), (3,'c');`,
	); err != nil {
		t.Fatal(err)
	}

	backend := state.YAMLStore{
		Path: filepath.Join(directory, "state.yaml"),
	}
	runID := "sqlite-strict-core"
	started := time.Now().UTC().Add(-time.Minute)
	// The run's source engine must be sqlite: the state layer refuses strict
	// evidence whose engine disagrees with the run, which is what stops one
	// engine's consistency proof from being credited to another.
	initializeSQLiteStrictRun(t, backend, runID, started)

	task := state.TaskKey{Type: "table-copy", Table: "items"}
	const topology = "sqlite-strict-topology"
	if _, err := backend.EnsureWorkPlan(state.WorkTask{
		RunID: runID, Key: task, Strategy: "tuple_keyset",
		TopologyHash: topology, StartedAt: started,
	}, []state.RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	workTasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var durable state.WorkTask
	for _, candidate := range workTasks {
		if candidate.Key == task {
			durable = candidate
		}
	}
	if durable.RunID == "" {
		t.Fatalf("durable work for %#v was not found: %#v", task, workTasks)
	}

	attemptID, err := BuildStrictConsistencyAttemptID(
		task,
		topology,
		durable.Attempts,
	)
	if err != nil {
		t.Fatal(err)
	}
	selected := StrictConsistencyTable{
		Task:                task,
		WorkTopologyHash:    topology,
		DurableWorkAttempts: durable.Attempts,
		AttemptID:           attemptID,
	}
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := BeginStrictConsistency(
		context.Background(),
		StrictConsistencyRequest{
			RunID:        runID,
			SourceEngine: StrictConsistencySQLite,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "epoch-1",
			State:        backend,
			Tables:       []StrictConsistencyTable{selected},
		},
		opener,
	)
	if err != nil {
		t.Fatalf("begin SQLite strict consistency: %v", err)
	}
	defer func() {
		if err := execution.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	// The coordinator must have made the same-view count durable before any
	// target work is authorized. Reading it back from the backend, rather than
	// from the execution, is what proves it survived the write.
	evidence, found, err := backend.LoadStrictSnapshotEvidence(
		runID,
		task,
		selected.AttemptID,
	)
	if err != nil || !found {
		t.Fatalf("durable strict evidence found=%v err=%v", found, err)
	}
	if evidence.ExactSourceRowCount != 3 {
		t.Fatalf("durable strict count = %d, want 3", evidence.ExactSourceRowCount)
	}
	if evidence.SnapshotReference == "" {
		t.Fatalf("durable strict evidence = %#v", evidence)
	}
}

// TestSQLiteStrictCoordinatorRejectsMigrationScope proves the refusal reaches
// the caller through the core, and that it costs nothing: no session is opened,
// so the source is never locked by a request that cannot be served.
func TestSQLiteStrictCoordinatorRejectsMigrationScope(t *testing.T) {
	directory := t.TempDir()
	source, err := sql.Open(
		"sqlite",
		filepath.Join(directory, "source.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Exec(
		`CREATE TABLE items (id INTEGER PRIMARY KEY)`,
	); err != nil {
		t.Fatal(err)
	}
	backend := state.YAMLStore{
		Path: filepath.Join(directory, "state.yaml"),
	}
	runID := "sqlite-strict-scope"
	initializeSQLiteStrictRun(
		t,
		backend,
		runID,
		time.Now().UTC().Add(-time.Minute),
	)
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := BeginStrictConsistency(
		context.Background(),
		StrictConsistencyRequest{
			RunID:        runID,
			SourceEngine: StrictConsistencySQLite,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "epoch-1",
			State:        backend,
			Tables: []StrictConsistencyTable{{
				Task:             state.TaskKey{Type: "table-copy", Table: "items"},
				WorkTopologyHash: "topology",
			}},
		},
		opener,
	)
	if execution != nil {
		_ = execution.Close(context.Background())
	}
	if err == nil {
		t.Fatal("migration scope was accepted for SQLite")
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("scope rejection class = %q: %v", ClassifyTransferError(err), err)
	}
}
