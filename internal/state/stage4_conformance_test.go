package state

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	schemapkg "github.com/johndauphine/dmtx/internal/schema"
)

type stage4BackendFactory func(*testing.T) (Backend, func() Backend)

func TestStage4BackendConformance(t *testing.T) {
	factories := map[string]stage4BackendFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend { return SQLiteStore{Path: path} }
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend { return YAMLStore{Path: path} }
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Run("schema snapshot and identity", func(t *testing.T) {
				testStage4SchemaSnapshot(t, factory)
			})
			t.Run("schema package digest compatibility", func(t *testing.T) {
				testStage4SchemaPackageDigest(t, factory)
			})
			t.Run("incremental fence and atomic completion", func(t *testing.T) {
				testStage4IncrementalAttempt(t, factory)
			})
			t.Run("delete results", func(t *testing.T) {
				testStage4DeleteReconciliation(t, factory)
			})
			t.Run("strict snapshot evidence", func(t *testing.T) {
				testStage4StrictSnapshot(t, factory)
			})
		})
	}
}

func testStage4SchemaPackageDigest(t *testing.T, factory stage4BackendFactory) {
	t.Helper()
	backend, _ := factory(t)
	stage4, runID, key, started := initializeStage4Backend(t, backend)
	schemaSnapshot, err := schemapkg.NewSchemaSnapshot([]schemapkg.Table{{
		Schema: "public",
		Name:   "items",
		Columns: []schemapkg.Column{{
			Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := schemaSnapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := schemaSnapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	record := SchemaSnapshot{
		RunID: runID, Task: key, CanonicalJSON: string(canonical),
		Digest: digest, CapturedAt: started,
	}
	if err := stage4.SaveSchemaSnapshot(record); err != nil {
		t.Fatalf("save schema package snapshot: %v", err)
	}
	restored, found, err := stage4.LoadSchemaSnapshot(runID, key)
	if err != nil || !found {
		t.Fatalf("schema package snapshot found=%v err=%v", found, err)
	}
	if restored.CanonicalJSON != string(canonical) || restored.Digest != digest {
		t.Fatalf("schema package bytes changed: %#v", restored)
	}
}

func testStage4SchemaSnapshot(t *testing.T, factory stage4BackendFactory) {
	t.Helper()
	backend, reopen := factory(t)
	stage4, runID, key, started := initializeStage4Backend(t, backend)
	snapshot := SchemaSnapshot{
		RunID:         runID,
		Task:          key,
		CanonicalJSON: `{"z":1,"table":{"name":"items","columns":["id"]}}`,
		CapturedAt:    started,
	}
	if err := stage4.SaveSchemaSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	// The exact schema-owned canonical bytes are an idempotent replay,
	// including when the caller supplies the already-computed digest.
	expectedJSON := snapshot.CanonicalJSON
	digestBytes := sha256.Sum256([]byte(expectedJSON))
	snapshot.Digest = hex.EncodeToString(digestBytes[:])
	if err := stage4.SaveSchemaSnapshot(snapshot); err != nil {
		t.Fatalf("idempotent schema snapshot: %v", err)
	}
	restored, found, err := reopen().(Stage4Backend).LoadSchemaSnapshot(runID, key)
	if err != nil || !found {
		t.Fatalf("schema snapshot found=%v err=%v", found, err)
	}
	if restored.CanonicalJSON != expectedJSON || restored.Digest != snapshot.Digest ||
		!restored.CapturedAt.Equal(started) {
		t.Fatalf("schema snapshot = %#v", restored)
	}
	changed := snapshot
	changed.CanonicalJSON = `{"table":{"name":"other"},"z":1}`
	changed.Digest = ""
	if err := stage4.SaveSchemaSnapshot(changed); !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("schema overwrite error = %v", err)
	}

	unknownRun := snapshot
	unknownRun.RunID = "missing-run"
	if err := stage4.SaveSchemaSnapshot(unknownRun); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown run error = %v", err)
	}
	unknownTask := snapshot
	unknownTask.Task = TaskKey{Type: "table-copy", Schema: "public", Table: "missing"}
	if err := stage4.SaveSchemaSnapshot(unknownTask); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown task error = %v", err)
	}
	if _, _, err := stage4.LoadSchemaSnapshot(runID, unknownTask.Task); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown task read error = %v", err)
	}
}

func testStage4IncrementalAttempt(t *testing.T, factory stage4BackendFactory) {
	t.Helper()
	backend, reopen := factory(t)
	stage4, runID, key, started := initializeStage4Backend(t, backend)
	upper := TimestampWatermark{Column: "updated_at", Value: started.Add(time.Hour)}
	attempt := IncrementalAttempt{
		RunID: runID, Task: key, AttemptID: "attempt-1",
		Mode: IncrementalBaseline, UpperFence: &upper, StartedAt: started,
	}
	stored, created, err := stage4.BeginIncrementalAttempt(attempt)
	if err != nil || !created {
		t.Fatalf("begin incremental created=%v err=%v", created, err)
	}
	if stored.Status != IncrementalRunning || stored.TableSucceeded {
		t.Fatalf("new incremental attempt = %#v", stored)
	}
	if _, created, err := stage4.BeginIncrementalAttempt(attempt); err != nil || created {
		t.Fatalf("idempotent begin created=%v err=%v", created, err)
	}
	changed := attempt
	changedUpper := *changed.UpperFence
	changedUpper.Value = changedUpper.Value.Add(time.Second)
	changed.UpperFence = &changedUpper
	if _, _, err := stage4.BeginIncrementalAttempt(changed); !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("upper fence overwrite error = %v", err)
	}

	unsafe := upper
	unsafe.Value = unsafe.Value.Add(time.Nanosecond)
	if err := backend.(RangeBackend).CompleteRange(
		runID,
		key,
		"0",
		"topology-1",
		0,
		started.Add(90*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if err := stage4.CommitIncrementalAttempt(IncrementalCommit{
		RunID: runID, Task: key, AttemptID: attempt.AttemptID,
		TopologyHash: "topology-1", Watermark: &unsafe,
		CompletedAt: started.Add(2 * time.Hour),
	}); !errors.Is(err, ErrStateTransition) {
		t.Fatalf("watermark beyond fence error = %v", err)
	}
	intermediate := upper
	intermediate.Value = intermediate.Value.Add(-time.Minute)
	if err := stage4.CommitIncrementalAttempt(IncrementalCommit{
		RunID: runID, Task: key, AttemptID: attempt.AttemptID,
		TopologyHash: "topology-1", Watermark: &intermediate,
		CompletedAt: started.Add(2 * time.Hour),
	}); !errors.Is(err, ErrStateTransition) {
		t.Fatalf("intermediate watermark error = %v", err)
	}
	stored, found, err := stage4.LoadIncrementalAttempt(runID, key, attempt.AttemptID)
	if err != nil || !found || stored.Status != IncrementalRunning ||
		stored.CommittedWatermark != nil || stored.TableSucceeded {
		t.Fatalf("failed completion changed attempt: %#v found=%v err=%v", stored, found, err)
	}

	completedAt := started.Add(2 * time.Hour)
	commit := IncrementalCommit{
		RunID: runID, Task: key, AttemptID: attempt.AttemptID,
		TopologyHash: "topology-1", Watermark: &upper, CompletedAt: completedAt,
	}
	if err := stage4.CommitIncrementalAttempt(commit); err != nil {
		t.Fatal(err)
	}
	restored, found, err := reopen().(Stage4Backend).LoadIncrementalAttempt(runID, key, attempt.AttemptID)
	if err != nil || !found {
		t.Fatalf("incremental attempt found=%v err=%v", found, err)
	}
	if restored.Status != IncrementalCompleted || !restored.TableSucceeded ||
		restored.CommittedWatermark == nil ||
		!restored.CommittedWatermark.Value.Equal(upper.Value) ||
		restored.UpperFence == nil ||
		!restored.UpperFence.Value.Equal(upper.Value) ||
		!restored.CompletedAt.Equal(completedAt) {
		t.Fatalf("completed incremental attempt = %#v", restored)
	}
	if err := stage4.CommitIncrementalAttempt(commit); err != nil {
		t.Fatalf("idempotent incremental completion: %v", err)
	}
	differentCommit := commit
	differentCommit.CompletedAt = differentCommit.CompletedAt.Add(time.Second)
	if err := stage4.CommitIncrementalAttempt(differentCommit); !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("incremental completion overwrite error = %v", err)
	}
	if err := stage4.CommitIncrementalAttempt(IncrementalCommit{
		RunID: runID, Task: key, AttemptID: "missing",
		TopologyHash: "topology-1", Watermark: &upper, CompletedAt: completedAt,
	}); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown incremental attempt error = %v", err)
	}
}

func testStage4DeleteReconciliation(t *testing.T, factory stage4BackendFactory) {
	t.Helper()
	backend, reopen := factory(t)
	stage4, runID, key, started := initializeStage4Backend(t, backend)

	notDue := DeleteReconciliation{
		RunID: runID, Task: key, AttemptID: "not-due",
		Due: false, Reason: "interval has not elapsed", StartedAt: started,
	}
	stored, created, err := stage4.BeginDeleteReconciliation(notDue)
	if err != nil || !created || stored.Status != DeleteReconciliationNotDue ||
		stored.Due || stored.CompletedAt.IsZero() {
		t.Fatalf("not-due reconciliation = %#v created=%v err=%v", stored, created, err)
	}

	due := DeleteReconciliation{
		RunID: runID, Task: key, AttemptID: "due-zero",
		Due: true, StartedAt: started.Add(time.Minute),
	}
	if _, created, err := stage4.BeginDeleteReconciliation(due); err != nil || !created {
		t.Fatalf("due reconciliation created=%v err=%v", created, err)
	}
	invalid := DeleteReconciliationResult{
		RunID: runID, Task: key, AttemptID: due.AttemptID,
		Status: DeleteReconciliationCompleted, Candidates: 1, DeletedRows: 2,
		CompletedAt: started.Add(2 * time.Minute),
	}
	if err := stage4.FinishDeleteReconciliation(invalid); err == nil {
		t.Fatal("invalid delete counts were accepted")
	}
	stored, found, err := stage4.LoadDeleteReconciliation(runID, key, due.AttemptID)
	if err != nil || !found || stored.Status != DeleteReconciliationRunning {
		t.Fatalf("failed delete result changed state: %#v found=%v err=%v", stored, found, err)
	}
	result := DeleteReconciliationResult{
		RunID: runID, Task: key, AttemptID: due.AttemptID,
		Status: DeleteReconciliationCompleted, Candidates: 0, DeletedRows: 0,
		CompletedAt: started.Add(2 * time.Minute),
	}
	if err := stage4.FinishDeleteReconciliation(result); err != nil {
		t.Fatal(err)
	}
	if err := stage4.FinishDeleteReconciliation(result); err != nil {
		t.Fatalf("idempotent delete result: %v", err)
	}
	completed, found, err := reopen().(Stage4Backend).LoadDeleteReconciliation(runID, key, due.AttemptID)
	if err != nil || !found || completed.Status != DeleteReconciliationCompleted ||
		completed.Candidates != 0 || completed.DeletedRows != 0 {
		t.Fatalf("zero-delete reconciliation = %#v found=%v err=%v", completed, found, err)
	}
	notDueStored, found, err := stage4.LoadDeleteReconciliation(runID, key, notDue.AttemptID)
	if err != nil || !found || notDueStored.Status != DeleteReconciliationNotDue {
		t.Fatalf("not-due state = %#v found=%v err=%v", notDueStored, found, err)
	}

	incomplete := DeleteReconciliation{
		RunID: runID, Task: key, AttemptID: "incomplete",
		Due: true, StartedAt: started.Add(3 * time.Minute),
	}
	if _, _, err := stage4.BeginDeleteReconciliation(incomplete); err != nil {
		t.Fatal(err)
	}
	if err := stage4.FinishDeleteReconciliation(DeleteReconciliationResult{
		RunID: runID, Task: key, AttemptID: incomplete.AttemptID,
		Status: DeleteReconciliationIncomplete, Candidates: 10, DeletedRows: 4,
		SkippedRows: 6, Reason: "source key scan failed",
		CompletedAt: started.Add(4 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	incompleteStored, found, err := stage4.LoadDeleteReconciliation(runID, key, incomplete.AttemptID)
	if err != nil || !found || incompleteStored.Status != DeleteReconciliationIncomplete ||
		incompleteStored.DeletedRows != 4 || incompleteStored.Reason == "" {
		t.Fatalf("incomplete reconciliation = %#v found=%v err=%v", incompleteStored, found, err)
	}

	dryRun := DeleteReconciliation{
		RunID: runID, Task: key, AttemptID: "dry-run",
		Due: true, DryRun: true, StartedAt: started.Add(5 * time.Minute),
	}
	if _, _, err := stage4.BeginDeleteReconciliation(dryRun); err != nil {
		t.Fatal(err)
	}
	if err := stage4.FinishDeleteReconciliation(DeleteReconciliationResult{
		RunID: runID, Task: key, AttemptID: dryRun.AttemptID,
		Status: DeleteReconciliationDryRun, Candidates: 7, DeletedRows: 0,
		CompletedAt: started.Add(6 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	dryRunStored, found, err := stage4.LoadDeleteReconciliation(runID, key, dryRun.AttemptID)
	if err != nil || !found || dryRunStored.Status != DeleteReconciliationDryRun ||
		dryRunStored.Candidates != 7 || dryRunStored.DeletedRows != 0 {
		t.Fatalf("dry-run reconciliation = %#v found=%v err=%v", dryRunStored, found, err)
	}

	for _, attempt := range []struct {
		id          string
		startedAt   time.Time
		completedAt time.Time
	}{
		{id: "latest-tie-z", startedAt: started.Add(7 * time.Minute), completedAt: started.Add(10 * time.Minute)},
		{id: "latest-tie-a", startedAt: started.Add(8 * time.Minute), completedAt: started.Add(10 * time.Minute)},
		{id: "older-appended-last", startedAt: started.Add(9 * time.Minute), completedAt: started.Add(9*time.Minute + 30*time.Second)},
	} {
		if _, _, err := stage4.BeginDeleteReconciliation(DeleteReconciliation{
			RunID: runID, Task: key, AttemptID: attempt.id,
			Due: true, StartedAt: attempt.startedAt,
		}); err != nil {
			t.Fatal(err)
		}
		if err := stage4.FinishDeleteReconciliation(DeleteReconciliationResult{
			RunID: runID, Task: key, AttemptID: attempt.id,
			Status: DeleteReconciliationCompleted, CompletedAt: attempt.completedAt,
		}); err != nil {
			t.Fatal(err)
		}
	}
	latest, found, err := reopen().(Stage4Backend).LoadLatestSuccessfulDeleteReconciliation(runID, key)
	if err != nil || !found || latest.AttemptID != "latest-tie-z" {
		t.Fatalf("latest successful delete reconciliation = %#v found=%v err=%v", latest, found, err)
	}
}

func testStage4StrictSnapshot(t *testing.T, factory stage4BackendFactory) {
	t.Helper()
	backend, reopen := factory(t)
	stage4, runID, key, started := initializeStage4Backend(t, backend)
	owner := StrictMigrationSnapshot{
		RunID: runID, EpochID: "migration-epoch-1", SourceEngine: "postgres",
		SnapshotReference: "00000003-0000001A-1", ProcessEpoch: "process-epoch-1",
		CapturedAt: started,
	}
	if err := stage4.SaveStrictMigrationSnapshot(owner); err != nil {
		t.Fatal(err)
	}
	evidence := StrictSnapshotEvidence{
		RunID: runID, Task: key, AttemptID: "strict-attempt-1", SourceEngine: "postgres",
		Scope: StrictSnapshotMigration, MigrationEpochID: owner.EpochID,
		SnapshotReference: "00000003-0000001A-1",
		ProcessEpoch:      "process-epoch-1", ExactSourceRowCount: 42,
		CapturedAt: started,
	}
	if err := stage4.SaveStrictSnapshotEvidence(evidence); err != nil {
		t.Fatal(err)
	}
	if err := stage4.SaveStrictSnapshotEvidence(evidence); err != nil {
		t.Fatalf("idempotent strict snapshot evidence: %v", err)
	}
	restored, found, err := reopen().(Stage4Backend).LoadStrictSnapshotEvidence(
		runID,
		key,
		evidence.AttemptID,
	)
	if err != nil || !found || restored.SnapshotReference != evidence.SnapshotReference ||
		restored.ProcessEpoch != evidence.ProcessEpoch ||
		restored.ExactSourceRowCount != evidence.ExactSourceRowCount {
		t.Fatalf("strict snapshot evidence = %#v found=%v err=%v", restored, found, err)
	}
	changed := evidence
	changed.ExactSourceRowCount++
	if err := stage4.SaveStrictSnapshotEvidence(changed); !errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("strict evidence overwrite error = %v", err)
	}
	nextEpoch := evidence
	nextEpoch.AttemptID = "strict-attempt-2"
	nextEpoch.SnapshotReference = "00000003-0000001B-1"
	nextEpoch.ProcessEpoch = "process-epoch-2"
	nextEpoch.MigrationEpochID = "migration-epoch-2"
	nextEpoch.CapturedAt = nextEpoch.CapturedAt.Add(time.Hour)
	if err := stage4.SaveStrictMigrationSnapshot(StrictMigrationSnapshot{
		RunID: runID, EpochID: nextEpoch.MigrationEpochID, SourceEngine: "postgres",
		SnapshotReference: nextEpoch.SnapshotReference, ProcessEpoch: nextEpoch.ProcessEpoch,
		CapturedAt: nextEpoch.CapturedAt,
	}); err != nil {
		t.Fatalf("save new strict migration snapshot: %v", err)
	}
	if err := stage4.SaveStrictSnapshotEvidence(nextEpoch); err != nil {
		t.Fatalf("save new strict process epoch: %v", err)
	}
	firstEpoch, found, err := stage4.LoadStrictSnapshotEvidence(
		runID,
		key,
		evidence.AttemptID,
	)
	if err != nil || !found || firstEpoch.ProcessEpoch != evidence.ProcessEpoch {
		t.Fatalf("first strict epoch = %#v found=%v err=%v", firstEpoch, found, err)
	}
	secondEpoch, found, err := stage4.LoadStrictSnapshotEvidence(
		runID,
		key,
		nextEpoch.AttemptID,
	)
	if err != nil || !found || secondEpoch.ProcessEpoch != nextEpoch.ProcessEpoch {
		t.Fatalf("second strict epoch = %#v found=%v err=%v", secondEpoch, found, err)
	}
}

func initializeStage4Backend(
	t *testing.T,
	backend Backend,
) (Stage4Backend, string, TaskKey, time.Time) {
	t.Helper()
	started := time.Date(2026, 7, 30, 12, 0, 0, 123456789, time.FixedZone("test", -5*60*60))
	runID := "stage4-run"
	key := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
	if err := backend.InitializeRun(Run{
		ID: runID, Source: "source", Target: "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/source",
		TargetIdentity: "postgres:target.example:5432/target", Outcome: Running,
		Resumable: true, Reason: "running", StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.(RangeBackend).EnsureWorkPlan(
		WorkTask{
			RunID: runID, Key: key, Strategy: "tuple_keyset",
			TopologyHash: "topology-1", StartedAt: started,
		},
		[]RangeState{{ID: "0"}},
	); err != nil {
		t.Fatal(err)
	}
	stage4, ok := backend.(Stage4Backend)
	if !ok {
		t.Fatalf("%T does not implement Stage4Backend", backend)
	}
	return stage4, runID, key, started.UTC()
}
