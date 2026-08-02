package state

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStage4MutationsRejectStaleLeaseGeneration(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			directory := t.TempDir()
			leaseStore := SQLiteStore{Path: filepath.Join(directory, "lease.db")}
			firstLease, err := leaseStore.AcquireLease("postgres:target", "stage4-run", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var raw Backend
			if stateKind == "sqlite" {
				raw = SQLiteStore{Path: filepath.Join(directory, "state.db")}
			} else {
				raw = YAMLStore{Path: filepath.Join(directory, "state.yaml")}
			}
			_, runID, key, started := initializeStage4Backend(t, raw)
			if err := raw.BindRunLease(runID, firstLease); err != nil {
				t.Fatalf("bind initial target lease: %v", err)
			}
			fenced := FenceBackend(raw, NewLeaseGuard(leaseStore, firstLease)).(Stage4Backend)
			upper := TimestampWatermark{Column: "updated_at", Value: started.Add(time.Hour)}
			incremental := IncrementalAttempt{
				RunID: runID, Task: key, AttemptID: "incremental-before-takeover",
				Mode: IncrementalBaseline, UpperFence: &upper, StartedAt: started,
			}
			if _, _, err := fenced.BeginIncrementalAttempt(incremental); err != nil {
				t.Fatal(err)
			}
			deleteAttempt := DeleteReconciliation{
				RunID: runID, Task: key, AttemptID: "delete-before-takeover",
				Due: true, StartedAt: started,
			}
			if _, _, err := fenced.BeginDeleteReconciliation(deleteAttempt); err != nil {
				t.Fatal(err)
			}

			database, err := leaseStore.Open()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.Exec(
				`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
				time.Unix(0, 0).UTC(),
				firstLease.Target,
			); err != nil {
				database.Close()
				t.Fatal(err)
			}
			database.Close()
			secondLease, err := leaseStore.AcquireLease("postgres:target", "new-run", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if secondLease.Generation <= firstLease.Generation {
				t.Fatalf("lease generations = %d, %d", firstLease.Generation, secondLease.Generation)
			}
			deleteDigest := strings.Repeat("a", 64)
			deletePlan := DeleteReconciliationPlan{
				RunID: runID, Task: key,
				AttemptID: deleteAttempt.AttemptID,
				PlanID:    "stale-plan", SpoolPath: "/tmp/stale-plan.db",
				EqualityProofDigest: deleteDigest,
				CandidateDigest:     deleteDigest,
				Candidates:          1, BatchSize: 1,
				BatchByteLimit: 1024, KeyWidth: 1,
				PlannedAt: started.Add(time.Second),
			}
			deleteBatch := DeleteReconciliationBatch{
				RunID: runID, Task: key,
				AttemptID: deleteAttempt.AttemptID,
				PlanID:    deletePlan.PlanID, Token: "stale-token",
				Candidates: 1, EncodedBytes: 32,
				BatchDigest: deleteDigest,
				BeganAt:     started.Add(2 * time.Second),
			}
			deleteCommit := DeleteReconciliationBatchCommit{
				RunID: runID, Task: key,
				AttemptID: deleteAttempt.AttemptID,
				PlanID:    deletePlan.PlanID, Token: deleteBatch.Token,
				FirstCandidate: deleteBatch.FirstCandidate,
				BatchDigest:    deleteDigest, Candidates: 1,
				EncodedBytes: 32, DeletedRows: 1,
				ReceiptDigest:    deleteDigest,
				FailClosedReason: DeleteReconciliationReasonTargetMutationFailed,
				CommittedAt:      started.Add(3 * time.Second),
			}

			mutations := []func() error{
				func() error {
					return fenced.SaveSchemaSnapshot(SchemaSnapshot{
						RunID: runID, Task: key, CanonicalJSON: `{"version":1}`,
						CapturedAt: started,
					})
				},
				func() error {
					other := incremental
					other.AttemptID = "incremental-after-takeover"
					_, _, err := fenced.BeginIncrementalAttempt(other)
					return err
				},
				func() error {
					return fenced.CommitIncrementalAttempt(IncrementalCommit{
						RunID: runID, Task: key, AttemptID: incremental.AttemptID,
						TopologyHash: "topology-1", Watermark: &upper,
						CompletedAt: started.Add(2 * time.Hour),
					})
				},
				func() error {
					other := deleteAttempt
					other.AttemptID = "delete-after-takeover"
					_, _, err := fenced.BeginDeleteReconciliation(other)
					return err
				},
				func() error {
					return fenced.SaveDeleteReconciliationPlan(deletePlan)
				},
				func() error {
					_, _, err := fenced.
						BeginDeleteReconciliationBatch(deleteBatch)
					return err
				},
				func() error {
					return fenced.
						CommitDeleteReconciliationBatch(deleteCommit)
				},
				func() error {
					return fenced.FinishDeleteReconciliation(DeleteReconciliationResult{
						RunID: runID, Task: key, AttemptID: deleteAttempt.AttemptID,
						Status: DeleteReconciliationCompleted, CompletedAt: started.Add(time.Minute),
					})
				},
				func() error {
					return fenced.SaveStrictSnapshotEvidence(StrictSnapshotEvidence{
						RunID: runID, Task: key, AttemptID: "strict-after-takeover",
						SourceEngine: "postgres",
						Scope:        StrictSnapshotTable, SnapshotReference: "snapshot-1",
						ProcessEpoch: "epoch-1", ExactSourceRowCount: 1, CapturedAt: started,
					})
				},
			}
			for index, mutate := range mutations {
				if err := mutate(); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("mutation %d error = %v, want lease loss", index, err)
				}
			}

			stage4 := raw.(Stage4Backend)
			if _, found, err := stage4.LoadSchemaSnapshot(runID, key); err != nil || found {
				t.Fatalf("stale owner stored schema snapshot found=%v err=%v", found, err)
			}
			storedIncremental, found, err := stage4.LoadIncrementalAttempt(runID, key, incremental.AttemptID)
			if err != nil || !found || storedIncremental.Status != IncrementalRunning ||
				storedIncremental.TableSucceeded {
				t.Fatalf("stale owner changed incremental attempt: %#v found=%v err=%v", storedIncremental, found, err)
			}
			if _, found, err := stage4.LoadIncrementalAttempt(
				runID,
				key,
				"incremental-after-takeover",
			); err != nil || found {
				t.Fatalf("stale owner added incremental attempt found=%v err=%v", found, err)
			}
			storedDelete, found, err := stage4.LoadDeleteReconciliation(runID, key, deleteAttempt.AttemptID)
			if err != nil || !found || storedDelete.Status != DeleteReconciliationRunning {
				t.Fatalf("stale owner changed delete state: %#v found=%v err=%v", storedDelete, found, err)
			}
			if _, found, err := stage4.LoadDeleteReconciliation(
				runID,
				key,
				"delete-after-takeover",
			); err != nil || found {
				t.Fatalf("stale owner added delete attempt found=%v err=%v", found, err)
			}
			if _, found, err := stage4.LoadStrictSnapshotEvidence(
				runID,
				key,
				"strict-after-takeover",
			); err != nil || found {
				t.Fatalf("stale owner stored strict evidence found=%v err=%v", found, err)
			}
		})
	}
}

func TestStage4MutationsRejectDifferentRunWithCurrentLease(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			directory := t.TempDir()
			leaseStore := SQLiteStore{Path: filepath.Join(directory, "lease.db")}
			lease, err := leaseStore.AcquireLease("postgres:target", "run-a", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			var raw Backend
			if stateKind == "sqlite" {
				raw = SQLiteStore{Path: filepath.Join(directory, "state.db")}
			} else {
				raw = YAMLStore{Path: filepath.Join(directory, "state.yaml")}
			}
			started := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
			key := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
			initializeNamedStage4Run(
				t,
				raw,
				"run-a",
				key,
				"postgres:source.example:5432/app",
				"postgres:target.example:5432/app",
				started,
			)
			rawStage4 := initializeNamedStage4Run(
				t,
				raw,
				"run-b",
				key,
				"postgres:source.example:5432/app",
				"postgres:target.example:5432/app",
				started.Add(time.Hour),
			)
			incremental := IncrementalAttempt{
				RunID: "run-b", Task: key, AttemptID: "seed-incremental",
				Mode: IncrementalBaseline, StartedAt: started.Add(time.Hour),
			}
			if _, _, err := rawStage4.BeginIncrementalAttempt(incremental); err != nil {
				t.Fatal(err)
			}
			deleteAttempt := DeleteReconciliation{
				RunID: "run-b", Task: key, AttemptID: "seed-delete",
				Due: true, StartedAt: started.Add(time.Hour),
			}
			if _, _, err := rawStage4.BeginDeleteReconciliation(deleteAttempt); err != nil {
				t.Fatal(err)
			}
			if err := raw.BindRunLease("run-a", lease); err != nil {
				t.Fatalf("bind owned run target lease: %v", err)
			}
			deleteDigest := strings.Repeat("b", 64)
			deletePlan := DeleteReconciliationPlan{
				RunID: "run-b", Task: key,
				AttemptID:           deleteAttempt.AttemptID,
				PlanID:              "cross-run-plan",
				SpoolPath:           "/tmp/cross-run-plan.db",
				EqualityProofDigest: deleteDigest,
				CandidateDigest:     deleteDigest,
				Candidates:          1, BatchSize: 1,
				BatchByteLimit: 1024, KeyWidth: 1,
				PlannedAt: started.Add(time.Hour + time.Second),
			}
			deleteBatch := DeleteReconciliationBatch{
				RunID: "run-b", Task: key,
				AttemptID: deleteAttempt.AttemptID,
				PlanID:    deletePlan.PlanID, Token: "cross-run-token",
				Candidates: 1, EncodedBytes: 32,
				BatchDigest: deleteDigest,
				BeganAt:     started.Add(time.Hour + 2*time.Second),
			}
			deleteCommit := DeleteReconciliationBatchCommit{
				RunID: "run-b", Task: key,
				AttemptID: deleteAttempt.AttemptID,
				PlanID:    deletePlan.PlanID, Token: deleteBatch.Token,
				FirstCandidate: deleteBatch.FirstCandidate,
				BatchDigest:    deleteDigest, Candidates: 1,
				EncodedBytes: 32, DeletedRows: 1,
				ReceiptDigest:    deleteDigest,
				FailClosedReason: DeleteReconciliationReasonTargetMutationFailed,
				CommittedAt:      started.Add(time.Hour + 3*time.Second),
			}

			fenced := FenceBackend(raw, NewLeaseGuard(leaseStore, lease)).(Stage4Backend)
			if err := fenced.SaveSchemaSnapshot(SchemaSnapshot{
				RunID: "run-a", Task: key, CanonicalJSON: `{"allowed":true}`,
				CapturedAt: started,
			}); err != nil {
				t.Fatalf("owned run mutation failed: %v", err)
			}
			mutations := []func() error{
				func() error {
					return fenced.SaveSchemaSnapshot(SchemaSnapshot{
						RunID: "run-b", Task: key, CanonicalJSON: `{"blocked":true}`,
						CapturedAt: started,
					})
				},
				func() error {
					_, _, err := fenced.BeginIncrementalAttempt(IncrementalAttempt{
						RunID: "run-b", Task: key, AttemptID: "blocked-incremental",
						Mode: IncrementalBaseline, StartedAt: started.Add(time.Hour),
					})
					return err
				},
				func() error {
					return fenced.CommitIncrementalAttempt(IncrementalCommit{
						RunID: "run-b", Task: key, AttemptID: incremental.AttemptID,
						TopologyHash: "topology-run-b",
						CompletedAt:  started.Add(2 * time.Hour),
					})
				},
				func() error {
					_, _, err := fenced.BeginDeleteReconciliation(DeleteReconciliation{
						RunID: "run-b", Task: key, AttemptID: "blocked-delete",
						Due: true, StartedAt: started.Add(time.Hour),
					})
					return err
				},
				func() error {
					return fenced.SaveDeleteReconciliationPlan(deletePlan)
				},
				func() error {
					_, _, err := fenced.
						BeginDeleteReconciliationBatch(deleteBatch)
					return err
				},
				func() error {
					return fenced.
						CommitDeleteReconciliationBatch(deleteCommit)
				},
				func() error {
					return fenced.FinishDeleteReconciliation(DeleteReconciliationResult{
						RunID: "run-b", Task: key, AttemptID: deleteAttempt.AttemptID,
						Status:      DeleteReconciliationCompleted,
						CompletedAt: started.Add(2 * time.Hour),
					})
				},
				func() error {
					return fenced.SaveStrictMigrationSnapshot(StrictMigrationSnapshot{
						RunID: "run-b", EpochID: "blocked-owner",
						SourceEngine: "postgres", SnapshotReference: "snapshot",
						ProcessEpoch: "process", CapturedAt: started.Add(time.Hour),
					})
				},
				func() error {
					return fenced.SaveStrictSnapshotEvidence(StrictSnapshotEvidence{
						RunID: "run-b", Task: key, AttemptID: "blocked-evidence",
						SourceEngine: "postgres", Scope: StrictSnapshotTable,
						SnapshotReference: "snapshot", ProcessEpoch: "process",
						ExactSourceRowCount: 1, CapturedAt: started.Add(time.Hour),
					})
				},
			}
			for index, mutate := range mutations {
				if err := mutate(); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("cross-run mutation %d error = %v", index, err)
				}
			}

			if _, found, err := rawStage4.LoadSchemaSnapshot("run-b", key); err != nil || found {
				t.Fatalf("cross-run schema found=%v err=%v", found, err)
			}
			storedIncremental, found, err := rawStage4.LoadIncrementalAttempt(
				"run-b",
				key,
				incremental.AttemptID,
			)
			if err != nil || !found || storedIncremental.Status != IncrementalRunning {
				t.Fatalf("cross-run incremental = %#v found=%v err=%v", storedIncremental, found, err)
			}
			storedDelete, found, err := rawStage4.LoadDeleteReconciliation(
				"run-b",
				key,
				deleteAttempt.AttemptID,
			)
			if err != nil || !found || storedDelete.Status != DeleteReconciliationRunning {
				t.Fatalf("cross-run delete = %#v found=%v err=%v", storedDelete, found, err)
			}
			if _, found, err := rawStage4.LoadStrictMigrationSnapshot(
				"run-b",
				"blocked-owner",
			); err != nil || found {
				t.Fatalf("cross-run strict owner found=%v err=%v", found, err)
			}
			if _, found, err := rawStage4.LoadStrictSnapshotEvidence(
				"run-b",
				key,
				"blocked-evidence",
			); err != nil || found {
				t.Fatalf("cross-run strict evidence found=%v err=%v", found, err)
			}
		})
	}
}
