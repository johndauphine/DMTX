package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStage4IncrementalCompletionIsAtomicAcrossBackendReopen(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, reopen := factory(t)
			stage4, runID, key, started := initializeStage4Backend(t, backend)
			attempt := IncrementalAttempt{
				RunID: runID, Task: key, AttemptID: "empty-baseline",
				Mode: IncrementalBaseline, StartedAt: started,
			}
			if _, created, err := stage4.BeginIncrementalAttempt(attempt); err != nil || !created {
				t.Fatalf("begin empty baseline created=%v err=%v", created, err)
			}
			if active, found, err := stage4.LoadActiveIncrementalAttempt(runID, key); err != nil ||
				!found || active.AttemptID != attempt.AttemptID {
				t.Fatalf("active attempt = %#v found=%v err=%v", active, found, err)
			}
			if err := backend.(RangeBackend).CompleteRange(
				runID, key, "0", "topology-1", 0, started.Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}

			injected := errors.New("injected atomic completion failure")
			previousHook := stage4BeforeIncrementalCommit
			stage4BeforeIncrementalCommit = func() error { return injected }
			t.Cleanup(func() { stage4BeforeIncrementalCommit = previousHook })
			commit := IncrementalCommit{
				RunID: runID, Task: key, AttemptID: attempt.AttemptID,
				TopologyHash: "topology-1", CompletedAt: started.Add(2 * time.Minute),
			}
			if err := stage4.CommitIncrementalAttempt(commit); !errors.Is(err, injected) {
				t.Fatalf("injected completion error = %v", err)
			}

			reopened := reopen()
			reopenedStage4 := reopened.(Stage4Backend)
			stored, found, err := reopenedStage4.LoadIncrementalAttempt(
				runID,
				key,
				attempt.AttemptID,
			)
			if err != nil || !found || stored.Status != IncrementalRunning ||
				stored.TableSucceeded || stored.CommittedWatermark != nil {
				t.Fatalf("rolled-back attempt = %#v found=%v err=%v", stored, found, err)
			}
			tasks, _, err := reopened.(RangeBackend).ListWork(runID)
			if err != nil || len(tasks) != 1 || tasks[0].Status != "running" {
				t.Fatalf("rolled-back aggregate tasks=%#v err=%v", tasks, err)
			}

			stage4BeforeIncrementalCommit = previousHook
			if err := reopenedStage4.CommitIncrementalAttempt(commit); err != nil {
				t.Fatal(err)
			}
			stored, found, err = reopen().(Stage4Backend).LoadIncrementalAttempt(
				runID,
				key,
				attempt.AttemptID,
			)
			if err != nil || !found || stored.Status != IncrementalCompleted ||
				!stored.TableSucceeded || stored.CommittedWatermark != nil {
				t.Fatalf("committed empty attempt = %#v found=%v err=%v", stored, found, err)
			}
			tasks, _, err = reopen().(RangeBackend).ListWork(runID)
			if err != nil || len(tasks) != 1 || tasks[0].Status != "completed" ||
				!tasks[0].CompletedAt.Equal(commit.CompletedAt) {
				t.Fatalf("committed aggregate tasks=%#v err=%v", tasks, err)
			}
			if active, found, err := reopenedStage4.LoadActiveIncrementalAttempt(
				runID,
				key,
			); err != nil || found {
				t.Fatalf("terminal attempt remained active: %#v found=%v err=%v", active, found, err)
			}
			replayed, created, err := reopenedStage4.BeginIncrementalAttempt(attempt)
			if err != nil || created || replayed.Status != IncrementalCompleted {
				t.Fatalf("terminal begin replay = %#v created=%v err=%v", replayed, created, err)
			}
		})
	}
}

func TestStage4OneIncrementalAttemptPerRunTaskUnderConcurrency(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			stage4, runID, key, started := initializeStage4Backend(t, backend)
			type result struct {
				attempt IncrementalAttempt
				created bool
				err     error
			}
			results := make(chan result, 2)
			var ready sync.WaitGroup
			ready.Add(2)
			start := make(chan struct{})
			for _, attemptID := range []string{"parallel-a", "parallel-b"} {
				attemptID := attemptID
				go func() {
					ready.Done()
					<-start
					attempt, created, err := stage4.BeginIncrementalAttempt(IncrementalAttempt{
						RunID: runID, Task: key, AttemptID: attemptID,
						Mode: IncrementalBaseline, StartedAt: started,
					})
					results <- result{attempt: attempt, created: created, err: err}
				}()
			}
			ready.Wait()
			close(start)
			first, second := <-results, <-results
			close(results)
			successes := 0
			for _, candidate := range []result{first, second} {
				if candidate.err == nil && candidate.created {
					successes++
					continue
				}
				if !errors.Is(candidate.err, ErrImmutableEvidence) {
					t.Fatalf("parallel begin result = %#v", candidate)
				}
			}
			if successes != 1 {
				t.Fatalf("parallel begin successes = %d; results %#v %#v", successes, first, second)
			}
			active, found, err := stage4.LoadActiveIncrementalAttempt(runID, key)
			if err != nil || !found ||
				(active.AttemptID != "parallel-a" && active.AttemptID != "parallel-b") {
				t.Fatalf("active singleton = %#v found=%v err=%v", active, found, err)
			}
		})
	}
}

func TestStage4IncrementalWindowPublishesExactFenceAndPreservesZeroRowFrontier(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			started := time.Date(2026, 7, 30, 14, 0, 0, 0, time.UTC)
			key := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
			sourceIdentity := "postgres:source.example:5432/app"
			targetIdentity := "postgres:target.example:5432/app"
			baseline := initializeNamedStage4Run(
				t, backend, "baseline", key, sourceIdentity, targetIdentity, started,
			)
			baselineUpper := TimestampWatermark{
				Column: "updated_at",
				Value:  started.Add(time.Hour),
			}
			commitNamedStage4Baseline(
				t,
				backend,
				baseline,
				"baseline",
				key,
				started,
				&baselineUpper,
			)
			if err := backend.Append(Run{
				ID: "baseline", Outcome: Success, Reason: "complete",
				StartedAt: started, EndedAt: started.Add(2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			windowStart := started.Add(3 * time.Hour)
			window := initializeNamedStage4Run(
				t, backend, "window", key, sourceIdentity, targetIdentity, windowStart,
			)
			windowUpper := TimestampWatermark{
				Column: "updated_at",
				Value:  windowStart.Add(time.Hour),
			}
			attempt := IncrementalAttempt{
				RunID: "window", Task: key, AttemptID: "window-1",
				Mode: IncrementalWindow, LowerWatermark: &baselineUpper,
				UpperFence: &windowUpper, StartedAt: windowStart,
			}
			if _, created, err := window.BeginIncrementalAttempt(attempt); err != nil || !created {
				t.Fatalf("begin window created=%v err=%v", created, err)
			}
			if err := backend.(RangeBackend).CompleteRange(
				"window",
				key,
				"0",
				"topology-window",
				0,
				windowStart.Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}
			intermediate := windowUpper
			intermediate.Value = intermediate.Value.Add(-time.Minute)
			if err := window.CommitIncrementalAttempt(IncrementalCommit{
				RunID: "window", Task: key, AttemptID: attempt.AttemptID,
				TopologyHash: "topology-window", Watermark: &intermediate,
				CompletedAt: windowStart.Add(2 * time.Minute),
			}); !errors.Is(err, ErrStateTransition) {
				t.Fatalf("intermediate window watermark error = %v", err)
			}
			if err := window.CommitIncrementalAttempt(IncrementalCommit{
				RunID: "window", Task: key, AttemptID: attempt.AttemptID,
				TopologyHash: "topology-window", Watermark: &windowUpper,
				CompletedAt: windowStart.Add(2 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(Run{
				ID: "window", Outcome: Success, Reason: "complete",
				StartedAt: windowStart, EndedAt: windowStart.Add(2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			emptyStart := windowStart.Add(3 * time.Hour)
			empty := initializeNamedStage4Run(
				t, backend, "empty-window", key, sourceIdentity, targetIdentity, emptyStart,
			)
			emptyAttempt := IncrementalAttempt{
				RunID: "empty-window", Task: key, AttemptID: "empty-window-1",
				Mode: IncrementalWindow, LowerWatermark: &windowUpper,
				StartedAt: emptyStart,
			}
			if _, created, err := empty.BeginIncrementalAttempt(emptyAttempt); err != nil || !created {
				t.Fatalf("begin empty window created=%v err=%v", created, err)
			}
			if err := backend.(RangeBackend).CompleteRange(
				"empty-window",
				key,
				"0",
				"topology-empty-window",
				0,
				emptyStart.Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}
			if err := empty.CommitIncrementalAttempt(IncrementalCommit{
				RunID: "empty-window", Task: key, AttemptID: emptyAttempt.AttemptID,
				TopologyHash: "topology-empty-window", Watermark: &windowUpper,
				CompletedAt: emptyStart.Add(2 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			stored, found, err := empty.LoadIncrementalAttempt(
				"empty-window",
				key,
				emptyAttempt.AttemptID,
			)
			if err != nil || !found ||
				!reflect.DeepEqual(stored.CommittedWatermark, &windowUpper) {
				t.Fatalf("empty window frontier = %#v found=%v err=%v", stored, found, err)
			}
		})
	}
}

func TestStage4CrossRunEvidenceRequiresSuccessfulExactWorkloadIdentity(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			started := time.Date(2026, 7, 30, 15, 0, 0, 0, time.UTC)
			key := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
			sourceIdentity := "postgres:source-a.example:5432/app"
			targetIdentity := "postgres:target-a.example:5432/app"

			first := initializeNamedStage4Run(
				t, backend, "successful", key, sourceIdentity, targetIdentity, started,
			)
			firstUpper := TimestampWatermark{Column: "updated_at", Value: started.Add(time.Hour)}
			commitNamedStage4Baseline(t, backend, first, "successful", key, started, &firstUpper)
			if err := first.SaveSchemaSnapshot(SchemaSnapshot{
				RunID: "successful", Task: key, CanonicalJSON: `{"version":1}`,
				CapturedAt: started.Add(90 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			firstDelete := DeleteReconciliation{
				RunID: "successful", Task: key, AttemptID: "delete-successful",
				Due: true, StartedAt: started.Add(100 * time.Minute),
			}
			if _, _, err := first.BeginDeleteReconciliation(firstDelete); err != nil {
				t.Fatal(err)
			}
			if err := first.FinishDeleteReconciliation(DeleteReconciliationResult{
				RunID: "successful", Task: key, AttemptID: firstDelete.AttemptID,
				Status: DeleteReconciliationCompleted, Candidates: 1, DeletedRows: 1,
				CompletedAt: started.Add(101 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(Run{
				ID: "successful", Source: "app", Target: "app", Outcome: Success,
				Resumable: false, Reason: "complete", StartedAt: started,
				EndedAt: started.Add(2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			failedStart := started.Add(3 * time.Hour)
			failed := initializeNamedStage4Run(
				t, backend, "failed", key, sourceIdentity, targetIdentity, failedStart,
			)
			failedUpper := TimestampWatermark{
				Column: "updated_at",
				Value:  failedStart.Add(time.Hour),
			}
			commitNamedStage4Baseline(t, backend, failed, "failed", key, failedStart, &failedUpper)
			if err := failed.SaveSchemaSnapshot(SchemaSnapshot{
				RunID: "failed", Task: key, CanonicalJSON: `{"version":99}`,
				CapturedAt: failedStart.Add(90 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			failedDelete := DeleteReconciliation{
				RunID: "failed", Task: key, AttemptID: "delete-failed",
				Due: true, StartedAt: failedStart.Add(100 * time.Minute),
			}
			if _, _, err := failed.BeginDeleteReconciliation(failedDelete); err != nil {
				t.Fatal(err)
			}
			if err := failed.FinishDeleteReconciliation(DeleteReconciliationResult{
				RunID: "failed", Task: key, AttemptID: failedDelete.AttemptID,
				Status: DeleteReconciliationCompleted, Candidates: 2, DeletedRows: 2,
				CompletedAt: failedStart.Add(101 * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Append(Run{
				ID: "failed", Source: "app", Target: "app", Outcome: Failed,
				Resumable: false, Reason: "abandoned", StartedAt: failedStart,
				EndedAt: failedStart.Add(2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			currentStart := started.Add(6 * time.Hour)
			current := initializeNamedStage4Run(
				t, backend, "current", key, sourceIdentity, targetIdentity, currentStart,
			)
			latest, found, err := current.LoadLatestCommittedIncrementalAttempt("current", key)
			if err != nil || !found || latest.RunID != "successful" ||
				latest.CommittedWatermark == nil ||
				!latest.CommittedWatermark.Value.Equal(firstUpper.Value) {
				t.Fatalf("latest successful frontier = %#v found=%v err=%v", latest, found, err)
			}
			schemaSnapshot, found, err := current.LoadLatestApplicableSchemaSnapshot("current", key)
			if err != nil || !found || schemaSnapshot.RunID != "successful" ||
				schemaSnapshot.CanonicalJSON != `{"version":1}` {
				t.Fatalf("latest successful schema = %#v found=%v err=%v", schemaSnapshot, found, err)
			}
			deleteResult, found, err := current.LoadLatestSuccessfulDeleteReconciliation(
				"current",
				key,
			)
			if err != nil || !found || deleteResult.RunID != "successful" ||
				deleteResult.AttemptID != firstDelete.AttemptID {
				t.Fatalf("latest successful delete = %#v found=%v err=%v", deleteResult, found, err)
			}
			wrong := failedUpper
			if _, _, err := current.BeginIncrementalAttempt(IncrementalAttempt{
				RunID: "current", Task: key, AttemptID: "wrong-lower",
				Mode: IncrementalWindow, LowerWatermark: &wrong,
				UpperFence: &failedUpper, StartedAt: currentStart,
			}); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("failed-run frontier was accepted: %v", err)
			}

			other := initializeNamedStage4Run(
				t,
				backend,
				"other-host",
				key,
				"postgres:source-b.example:5432/app",
				"postgres:target-b.example:5432/app",
				currentStart.Add(time.Hour),
			)
			if latest, found, err := other.LoadLatestCommittedIncrementalAttempt(
				"other-host",
				key,
			); err != nil || found {
				t.Fatalf("foreign-host frontier = %#v found=%v err=%v", latest, found, err)
			}
			if snapshot, found, err := other.LoadLatestApplicableSchemaSnapshot(
				"other-host",
				key,
			); err != nil || found {
				t.Fatalf("foreign-host schema = %#v found=%v err=%v", snapshot, found, err)
			}
			if result, found, err := other.LoadLatestSuccessfulDeleteReconciliation(
				"other-host",
				key,
			); err != nil || found {
				t.Fatalf("foreign-host delete = %#v found=%v err=%v", result, found, err)
			}
			if _, created, err := other.BeginIncrementalAttempt(IncrementalAttempt{
				RunID: "other-host", Task: key, AttemptID: "first-on-other-host",
				Mode: IncrementalWindow, StartedAt: currentStart.Add(time.Hour),
			}); err != nil || !created {
				t.Fatalf("first other-host incremental created=%v err=%v", created, err)
			}

			legacy := initializeNamedStage4Run(
				t,
				backend,
				"legacy-identity",
				key,
				"",
				"",
				currentStart.Add(2*time.Hour),
			)
			if _, _, err := legacy.LoadLatestApplicableSchemaSnapshot(
				"legacy-identity",
				key,
			); !errors.Is(err, ErrCrossRunIdentityUnavailable) {
				t.Fatalf("legacy cross-run schema lookup error = %v", err)
			}
		})
	}
}

func TestRunAppendPreservesImmutableWorkloadIdentity(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			started := time.Date(2026, 7, 30, 16, 0, 0, 0, time.UTC)
			initial := Run{
				ID: "immutable-run", Source: "source-db", Target: "target-db",
				SourceEngine:   "postgres",
				SourceIdentity: "postgres:source.example:5432/source-db",
				TargetIdentity: "postgres:target.example:5432/target-db",
				Outcome:        Running, Resumable: true, Reason: "running", StartedAt: started,
			}
			if err := backend.InitializeRun(initial, "hash"); err != nil {
				t.Fatal(err)
			}
			mutations := []Run{
				{Source: "other-source"},
				{Target: "other-target"},
				{SourceEngine: "mssql"},
				{SourceIdentity: "postgres:other.example:5432/source-db"},
				{TargetIdentity: "postgres:other.example:5432/target-db"},
			}
			for index, mutation := range mutations {
				mutation.ID = initial.ID
				mutation.Outcome = Success
				mutation.Reason = "complete"
				mutation.StartedAt = started
				mutation.EndedAt = started.Add(time.Minute)
				if err := backend.Append(mutation); !errors.Is(err, ErrImmutableEvidence) {
					t.Fatalf("identity mutation %d error = %v", index, err)
				}
			}
			runs, err := backend.List()
			if err != nil || len(runs) != 1 {
				t.Fatalf("runs after rejected identities = %#v err=%v", runs, err)
			}
			if err := backend.Append(Run{
				ID: initial.ID, Outcome: Success, Reason: "complete",
				StartedAt: started, EndedAt: started.Add(time.Minute),
			}); err != nil {
				t.Fatalf("blank terminal identity inheritance: %v", err)
			}
			latest, found, err := backend.Latest()
			if err != nil || !found ||
				latest.Source != initial.Source ||
				latest.Target != initial.Target ||
				latest.SourceEngine != initial.SourceEngine ||
				latest.SourceIdentity != initial.SourceIdentity ||
				latest.TargetIdentity != initial.TargetIdentity {
				t.Fatalf("inherited terminal run = %#v found=%v err=%v", latest, found, err)
			}
		})
	}
}

func TestStage4ReusableEvidenceUsesBackendIndependentTotalOrder(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			started := time.Date(2026, 7, 30, 17, 0, 0, 0, time.UTC)
			key := TaskKey{
				Type: "table-copy", Schema: "public", Table: "ordered_items",
			}
			sourceIdentity := "postgres:source.example:5432/app"
			targetIdentity := "postgres:target.example:5432/app"
			for index, runID := range []string{"z-evidence", "a-evidence"} {
				stage4 := initializeNamedStage4Run(
					t,
					backend,
					runID,
					key,
					sourceIdentity,
					targetIdentity,
					started,
				)
				upper := TimestampWatermark{
					Column: "updated_at",
					Value:  started.Add(time.Duration(index+1) * time.Hour),
				}
				commitNamedStage4Baseline(
					t,
					backend,
					stage4,
					runID,
					key,
					started,
					&upper,
				)
				if err := stage4.SaveSchemaSnapshot(SchemaSnapshot{
					RunID: runID, Task: key,
					CanonicalJSON: fmt.Sprintf(`{"run":%q}`, runID),
					CapturedAt:    started.Add(time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
				deleteAttempt := DeleteReconciliation{
					RunID: runID, Task: key, AttemptID: "delete-" + runID,
					Due: true, StartedAt: started.Add(time.Minute),
				}
				if _, _, err := stage4.BeginDeleteReconciliation(
					deleteAttempt,
				); err != nil {
					t.Fatal(err)
				}
				if err := stage4.FinishDeleteReconciliation(
					DeleteReconciliationResult{
						RunID: runID, Task: key,
						AttemptID: deleteAttempt.AttemptID,
						Status:    DeleteReconciliationCompleted,
						CompletedAt: started.Add(
							2 * time.Minute,
						),
					},
				); err != nil {
					t.Fatal(err)
				}
				if err := backend.Append(Run{
					ID: runID, Outcome: Success, Reason: "complete",
					StartedAt: started, EndedAt: started.Add(3 * time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
			}

			currentStarted := started.Add(24 * time.Hour)
			current := initializeNamedStage4Run(
				t,
				backend,
				"current-evidence",
				key,
				sourceIdentity,
				targetIdentity,
				currentStarted,
			)
			incremental, found, err := current.LoadLatestCommittedIncrementalAttempt(
				"current-evidence",
				key,
			)
			if err != nil || !found || incremental.RunID != "z-evidence" {
				t.Fatalf(
					"ordered incremental = %#v found=%v err=%v",
					incremental,
					found,
					err,
				)
			}
			snapshot, found, err := current.LoadLatestApplicableSchemaSnapshot(
				"current-evidence",
				key,
			)
			if err != nil || !found || snapshot.RunID != "z-evidence" {
				t.Fatalf(
					"ordered schema = %#v found=%v err=%v",
					snapshot,
					found,
					err,
				)
			}
			deleted, found, err := current.LoadLatestSuccessfulDeleteReconciliation(
				"current-evidence",
				key,
			)
			if err != nil || !found || deleted.RunID != "z-evidence" {
				t.Fatalf(
					"ordered delete = %#v found=%v err=%v",
					deleted,
					found,
					err,
				)
			}

			for _, owner := range []StrictMigrationSnapshot{
				{
					RunID: "current-evidence", EpochID: "z-epoch",
					SourceEngine: "postgres", SnapshotReference: "snapshot-z",
					ProcessEpoch: "process-z", CapturedAt: currentStarted,
				},
				{
					RunID: "current-evidence", EpochID: "a-epoch",
					SourceEngine: "postgres", SnapshotReference: "snapshot-a",
					ProcessEpoch: "process-a", CapturedAt: currentStarted,
				},
			} {
				if err := current.SaveStrictMigrationSnapshot(owner); err != nil {
					t.Fatal(err)
				}
			}
			owner, found, err := current.LoadLatestStrictMigrationSnapshot(
				"current-evidence",
			)
			if err != nil || !found || owner.EpochID != "z-epoch" {
				t.Fatalf(
					"ordered strict owner = %#v found=%v err=%v",
					owner,
					found,
					err,
				)
			}
		})
	}
}

func TestStage4StrictEvidenceRequiresImmutableRunSourceEngine(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			started := time.Date(2026, 7, 30, 18, 0, 0, 0, time.UTC)
			key := TaskKey{
				Type: "table-copy", Schema: "public", Table: "legacy_items",
			}
			legacy := initializeNamedStage4Run(
				t,
				backend,
				"legacy-engine",
				key,
				"legacy-source",
				"legacy-target",
				started,
				"",
			)
			if err := legacy.SaveStrictSnapshotEvidence(StrictSnapshotEvidence{
				RunID: "legacy-engine", Task: key, AttemptID: "legacy-strict",
				SourceEngine: "postgres", Scope: StrictSnapshotTable,
				SnapshotReference: "snapshot", ProcessEpoch: "process",
				CapturedAt: started,
			}); !errors.Is(err, ErrRunSourceEngineUnavailable) {
				t.Fatalf("legacy strict evidence error = %v", err)
			}
			if err := backend.Append(Run{
				ID: "legacy-engine", SourceEngine: "postgres",
				Outcome: Success, Reason: "complete", StartedAt: started,
				EndedAt: started.Add(time.Minute),
			}); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("legacy source-engine backfill error = %v", err)
			}

			bound := initializeNamedStage4Run(
				t,
				backend,
				"bound-engine",
				key,
				"postgres-source",
				"postgres-target",
				started.Add(time.Hour),
			)
			if err := bound.SaveStrictSnapshotEvidence(StrictSnapshotEvidence{
				RunID: "bound-engine", Task: key, AttemptID: "mixed-strict",
				SourceEngine: "mssql", Scope: StrictSnapshotTable,
				SnapshotReference: "snapshot", ProcessEpoch: "process",
				CapturedAt: started.Add(time.Hour),
			}); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("mixed strict evidence error = %v", err)
			}
		})
	}
}

func TestStage4DeleteCompletionAndLatestSuccessAreFailClosed(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			stage4, runID, key, started := initializeStage4Backend(t, backend)
			attempt := DeleteReconciliation{
				RunID: runID, Task: key, AttemptID: "delete-all",
				Due: true, StartedAt: started,
			}
			if _, _, err := stage4.BeginDeleteReconciliation(attempt); err != nil {
				t.Fatal(err)
			}
			if err := stage4.FinishDeleteReconciliation(DeleteReconciliationResult{
				RunID: runID, Task: key, AttemptID: attempt.AttemptID,
				Status: DeleteReconciliationCompleted, Candidates: 3,
				DeletedRows: 2, SkippedRows: 1, CompletedAt: started.Add(time.Minute),
			}); !errors.Is(err, ErrStateTransition) {
				t.Fatalf("partial hard-delete completion error = %v", err)
			}
			result := DeleteReconciliationResult{
				RunID: runID, Task: key, AttemptID: attempt.AttemptID,
				Status: DeleteReconciliationCompleted, Candidates: 3,
				DeletedRows: 3, CompletedAt: started.Add(2 * time.Minute),
			}
			if err := stage4.FinishDeleteReconciliation(result); err != nil {
				t.Fatal(err)
			}
			replayed, created, err := stage4.BeginDeleteReconciliation(attempt)
			if err != nil || created || replayed.Status != DeleteReconciliationCompleted {
				t.Fatalf("terminal delete replay = %#v created=%v err=%v", replayed, created, err)
			}
			latest, found, err := stage4.LoadLatestSuccessfulDeleteReconciliation(runID, key)
			if err != nil || !found || latest.AttemptID != attempt.AttemptID {
				t.Fatalf("latest delete success = %#v found=%v err=%v", latest, found, err)
			}
		})
	}
}

func TestStage4StrictMigrationSnapshotOwnsEveryTableEvidence(t *testing.T) {
	for name, factory := range stage4Factories() {
		t.Run(name, func(t *testing.T) {
			backend, _ := factory(t)
			stage4, runID, firstKey, started := initializeStage4Backend(t, backend)
			secondKey := TaskKey{
				Type: "table-copy", Schema: "public", Table: "other_items",
			}
			if _, err := backend.(RangeBackend).EnsureWorkPlan(WorkTask{
				RunID: runID, Key: secondKey, Strategy: "tuple_keyset",
				TopologyHash: "topology-2", StartedAt: started,
			}, []RangeState{{ID: "0"}}); err != nil {
				t.Fatal(err)
			}
			owner := StrictMigrationSnapshot{
				RunID: runID, EpochID: "pg-epoch-1", SourceEngine: "postgresql",
				SnapshotReference: "00000003-0000001A-1", ProcessEpoch: "process-1",
				CapturedAt: started,
			}
			if err := stage4.SaveStrictMigrationSnapshot(owner); err != nil {
				t.Fatal(err)
			}
			owner.SourceEngine = "postgres"
			if err := stage4.SaveStrictMigrationSnapshot(owner); err != nil {
				t.Fatalf("idempotent owner replay: %v", err)
			}
			sameProcessReplacement := owner
			sameProcessReplacement.EpochID = "pg-epoch-replacement"
			sameProcessReplacement.SnapshotReference = "00000003-0000001B-1"
			if err := stage4.SaveStrictMigrationSnapshot(
				sameProcessReplacement,
			); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("same PostgreSQL process replacement error = %v", err)
			}
			reusedReference := owner
			reusedReference.EpochID = "pg-epoch-2"
			reusedReference.ProcessEpoch = "process-2"
			reusedReference.CapturedAt = reusedReference.CapturedAt.Add(time.Minute)
			if err := stage4.SaveStrictMigrationSnapshot(
				reusedReference,
			); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("cross-process PostgreSQL reference reuse error = %v", err)
			}
			nextOwner := owner
			nextOwner.EpochID = "pg-epoch-2"
			nextOwner.SnapshotReference = "00000003-0000001C-1"
			nextOwner.ProcessEpoch = "process-2"
			nextOwner.CapturedAt = nextOwner.CapturedAt.Add(time.Minute)
			if err := stage4.SaveStrictMigrationSnapshot(nextOwner); err != nil {
				t.Fatalf("new PostgreSQL process owner: %v", err)
			}
			for index, key := range []TaskKey{firstKey, secondKey} {
				evidence := StrictSnapshotEvidence{
					RunID: runID, Task: key, AttemptID: fmt.Sprintf("table-%d", index),
					SourceEngine: "pg", Scope: StrictSnapshotMigration,
					MigrationEpochID:  owner.EpochID,
					SnapshotReference: owner.SnapshotReference,
					ProcessEpoch:      owner.ProcessEpoch, ExactSourceRowCount: int64(index + 1),
					CapturedAt: started.Add(time.Duration(index+1) * time.Minute),
				}
				if err := stage4.SaveStrictSnapshotEvidence(evidence); err != nil {
					t.Fatalf("save table %d evidence: %v", index, err)
				}
			}
			mismatched := StrictSnapshotEvidence{
				RunID: runID, Task: firstKey, AttemptID: "mismatch",
				SourceEngine: "postgres", Scope: StrictSnapshotMigration,
				MigrationEpochID: owner.EpochID, SnapshotReference: "replacement",
				ProcessEpoch: owner.ProcessEpoch, ExactSourceRowCount: 1,
				CapturedAt: started.Add(3 * time.Minute),
			}
			if err := stage4.SaveStrictSnapshotEvidence(mismatched); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("mismatched migration owner error = %v", err)
			}
			tableScoped := StrictSnapshotEvidence{
				RunID: runID, Task: firstKey, AttemptID: "table-scoped",
				SourceEngine: "postgres", Scope: StrictSnapshotTable,
				SnapshotReference: "table-lock-1", ProcessEpoch: "process-2",
				ExactSourceRowCount: 5, CapturedAt: started.Add(4 * time.Minute),
			}
			if err := stage4.SaveStrictSnapshotEvidence(tableScoped); err != nil {
				t.Fatalf("save table-scoped evidence: %v", err)
			}
			wrongTableEngine := tableScoped
			wrongTableEngine.AttemptID = "table-scoped-wrong-engine"
			wrongTableEngine.SourceEngine = "mssql"
			if err := stage4.SaveStrictSnapshotEvidence(
				wrongTableEngine,
			); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("mixed table-scoped source engine error = %v", err)
			}
			for index, candidate := range []struct {
				engine string
				want   string
			}{
				{engine: "sqlserver", want: "mssql"},
				{engine: "mariadb", want: "mysql"},
				{engine: "sqlite3", want: "sqlite"},
			} {
				candidateRunID := "table-scope-" + candidate.want
				candidateKey := TaskKey{
					Type: "table-copy", Schema: "source", Table: "items",
				}
				candidateStage4 := initializeNamedStage4Run(
					t,
					backend,
					candidateRunID,
					candidateKey,
					candidate.want+":source",
					"postgres:target.example:5432/app",
					started.Add(time.Duration(index+1)*time.Hour),
					candidate.want,
				)
				evidence := StrictSnapshotEvidence{
					RunID: candidateRunID, Task: candidateKey,
					AttemptID:    fmt.Sprintf("table-scope-%s", candidate.want),
					SourceEngine: candidate.engine, Scope: StrictSnapshotTable,
					SnapshotReference:   fmt.Sprintf("table-view-%d", index),
					ProcessEpoch:        fmt.Sprintf("table-process-%d", index),
					ExactSourceRowCount: int64(index),
					CapturedAt:          started.Add(time.Duration(index+5) * time.Minute),
				}
				if err := candidateStage4.SaveStrictSnapshotEvidence(evidence); err != nil {
					t.Fatalf("save %s table evidence: %v", candidate.engine, err)
				}
				stored, found, err := candidateStage4.LoadStrictSnapshotEvidence(
					candidateRunID,
					candidateKey,
					evidence.AttemptID,
				)
				if err != nil || !found || stored.SourceEngine != candidate.want ||
					stored.Scope != StrictSnapshotTable {
					t.Fatalf(
						"%s table evidence = %#v found=%v err=%v",
						candidate.engine,
						stored,
						found,
						err,
					)
				}
			}
			if err := stage4.SaveStrictMigrationSnapshot(StrictMigrationSnapshot{
				RunID: runID, EpochID: "mysql-migration", SourceEngine: "mysql",
				SnapshotReference: "unsupported", ProcessEpoch: "unsupported",
				CapturedAt: started,
			}); err == nil {
				t.Fatal("MySQL migration-scoped owner was accepted")
			}
			if err := stage4.SaveStrictSnapshotEvidence(StrictSnapshotEvidence{
				RunID: runID, Task: firstKey, AttemptID: "sqlite-migration",
				SourceEngine: "sqlite", Scope: StrictSnapshotMigration,
				MigrationEpochID: "unsupported", SnapshotReference: "unsupported",
				ProcessEpoch: "unsupported", CapturedAt: started,
			}); err == nil {
				t.Fatal("SQLite migration-scoped evidence was accepted")
			}
			latest, found, err := stage4.LoadLatestStrictMigrationSnapshot(runID)
			if err != nil || !found || latest.EpochID != nextOwner.EpochID ||
				latest.SnapshotReference != nextOwner.SnapshotReference {
				t.Fatalf("latest strict owner = %#v found=%v err=%v", latest, found, err)
			}

			mssqlKey := TaskKey{Type: "table-copy", Schema: "dbo", Table: "items"}
			mssql := initializeNamedStage4Run(
				t,
				backend,
				"mssql-strict",
				mssqlKey,
				"mssql:source.example:1433/app",
				"postgres:target.example:5432/app",
				started.Add(10*time.Hour),
				"mssql",
			)
			firstOwner := StrictMigrationSnapshot{
				RunID: "mssql-strict", EpochID: "mssql-owner",
				SourceEngine: "mssql", SnapshotReference: "database_snapshot_1",
				ProcessEpoch: "process-a", CapturedAt: started.Add(10 * time.Hour),
			}
			if err := mssql.SaveStrictMigrationSnapshot(firstOwner); err != nil {
				t.Fatal(err)
			}
			replacement := firstOwner
			replacement.EpochID = "mssql-owner-2"
			replacement.SnapshotReference = "database_snapshot_2"
			replacement.CapturedAt = replacement.CapturedAt.Add(time.Minute)
			if err := mssql.SaveStrictMigrationSnapshot(replacement); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("SQL Server owner replacement error = %v", err)
			}
		})
	}
}

func TestStage4StateUpgradesPreserveEarlierState(t *testing.T) {
	t.Run("yaml-v2", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.yaml")
		legacy := `version: 2
runs:
  - id: legacy
    source: source
    target: target
    source_identity: "postgres:source.example:5432/app"
    target_identity: "postgres:target.example:5432/app"
    outcome: running
    resumable: true
    resumability_reason: running
    started_at: 2026-07-30T12:00:00Z
work_tasks:
  - run_id: legacy
    key:
      type: table-copy
      schema: public
      table: items
    status: running
    strategy: tuple_keyset
    topology_hash: topology-1
    started_at: 2026-07-30T12:00:00Z
    updated_at: 2026-07-30T12:00:00Z
work_ranges:
  - run_id: legacy
    task:
      type: table-copy
      schema: public
      table: items
    id: "0"
    strategy: tuple_keyset
    topology_hash: topology-1
    status: running
    updated_at: 2026-07-30T12:00:00Z
`
		if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
			t.Fatal(err)
		}
		store := YAMLStore{Path: path}
		key := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
		if err := store.SaveSchemaSnapshot(SchemaSnapshot{
			RunID: "legacy", Task: key, CanonicalJSON: `{"table":"items"}`,
			CapturedAt: time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC),
		}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(data), "version: 3\n") {
			t.Fatalf("upgraded YAML version:\n%s", data)
		}
		tasks, ranges, err := store.ListWork("legacy")
		if err != nil || len(tasks) != 1 || len(ranges) != 1 ||
			tasks[0].TopologyHash != "topology-1" || ranges[0].ID != "0" {
			t.Fatalf("legacy work tasks=%#v ranges=%#v err=%v", tasks, ranges, err)
		}
	})

	t.Run("sqlite-additive-and-future-version", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.db")
		store := SQLiteStore{Path: path}
		_, runID, key, started := initializeStage4Backend(t, store)
		if err := store.SaveSchemaSnapshot(SchemaSnapshot{
			RunID: runID, Task: key, CanonicalJSON: `{"table":"items"}`,
			CapturedAt: started,
		}); err != nil {
			t.Fatal(err)
		}
		database, err := store.Open()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(`
			UPDATE state_schema_versions SET version = 2
			WHERE component = 'stage4_state'
		`); err != nil {
			database.Close()
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.LoadSchemaSnapshot(runID, key); err == nil ||
			!strings.Contains(err.Error(), "unsupported Stage 4 state schema version") {
			t.Fatalf("future Stage 4 schema error = %v", err)
		}
	})
}

func stage4Factories() map[string]stage4BackendFactory {
	return map[string]stage4BackendFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend { return SQLiteStore{Path: path} }
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend { return YAMLStore{Path: path} }
		},
	}
}

func initializeNamedStage4Run(
	t *testing.T,
	backend Backend,
	runID string,
	key TaskKey,
	sourceIdentity string,
	targetIdentity string,
	started time.Time,
	sourceEngineOverride ...string,
) Stage4Backend {
	t.Helper()
	sourceEngine := "postgres"
	if len(sourceEngineOverride) > 1 {
		t.Fatal("initialize named Stage 4 run accepts at most one source engine")
	}
	if len(sourceEngineOverride) == 1 {
		sourceEngine = sourceEngineOverride[0]
	}
	run := Run{
		ID: runID, Source: "app", Target: "app",
		SourceEngine:   sourceEngine,
		SourceIdentity: sourceIdentity, TargetIdentity: targetIdentity,
		Outcome: Running, Resumable: true, Reason: "running", StartedAt: started,
	}
	if err := backend.InitializeRun(run, "hash-"+runID); err != nil {
		t.Fatal(err)
	}
	if stored, _, _ := backend.Latest(); stored.SourceIdentity != sourceIdentity ||
		stored.TargetIdentity != targetIdentity {
		t.Fatalf("initial endpoint identities lost: input=%#v stored=%#v", run, stored)
	}
	if _, err := backend.(RangeBackend).EnsureWorkPlan(WorkTask{
		RunID: runID, Key: key, Strategy: "tuple_keyset",
		TopologyHash: "topology-" + runID, StartedAt: started,
	}, []RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	stage4, ok := backend.(Stage4Backend)
	if !ok {
		t.Fatalf("%T does not implement Stage4Backend", backend)
	}
	return stage4
}

func commitNamedStage4Baseline(
	t *testing.T,
	backend Backend,
	stage4 Stage4Backend,
	runID string,
	key TaskKey,
	started time.Time,
	upper *TimestampWatermark,
) {
	t.Helper()
	topology := "topology-" + runID
	attempt := IncrementalAttempt{
		RunID: runID, Task: key, AttemptID: "baseline-" + runID,
		Mode: IncrementalBaseline, UpperFence: upper, StartedAt: started,
	}
	if _, created, err := stage4.BeginIncrementalAttempt(attempt); err != nil || !created {
		t.Fatalf("begin named baseline created=%v err=%v", created, err)
	}
	if err := backend.(RangeBackend).CompleteRange(
		runID,
		key,
		"0",
		topology,
		0,
		started.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if err := stage4.CommitIncrementalAttempt(IncrementalCommit{
		RunID: runID, Task: key, AttemptID: attempt.AttemptID,
		TopologyHash: topology, Watermark: upper, CompletedAt: started.Add(2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
}
