package state

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestStrictMigrationCleanupIntentYAMLAndSQLite(t *testing.T) {
	stores := map[string]func(*testing.T) Backend{
		"yaml": func(t *testing.T) Backend {
			return YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		},
		"sqlite": func(t *testing.T) Backend {
			return SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		},
	}
	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			backend := newStore(t)
			started := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
			const runID = "mssql-cleanup-intent"
			if err := backend.InitializeRun(Run{
				ID: runID, Source: "source", Target: "target",
				SourceEngine:   "mssql",
				SourceIdentity: "mssql:source.example:1433/source",
				TargetIdentity: "sqlite:target", Outcome: Running,
				Resumable: true, Reason: "running", StartedAt: started,
			}, "cleanup-intent-config"); err != nil {
				t.Fatal(err)
			}
			stage4 := backend.(Stage4Backend)
			cleanup := backend.(StrictMigrationCleanupBackend)
			owner := StrictMigrationSnapshot{
				RunID: runID, EpochID: "mssql-epoch-1", SourceEngine: "mssql",
				SnapshotReference: "dmtx_ss_receipt", ProcessEpoch: "process-1",
				CapturedAt: started,
			}
			if err := stage4.SaveStrictMigrationSnapshot(owner); err != nil {
				t.Fatal(err)
			}
			intent := StrictMigrationCleanupIntent{
				RunID: owner.RunID, EpochID: owner.EpochID,
				SourceEngine:      owner.SourceEngine,
				SnapshotReference: owner.SnapshotReference,
				ProcessEpoch:      owner.ProcessEpoch,
				CapturedAt:        owner.CapturedAt,
				IntentAt:          started.Add(time.Minute),
			}
			if err := cleanup.SaveStrictMigrationCleanupIntent(intent); err != nil {
				t.Fatal(err)
			}
			if err := cleanup.SaveStrictMigrationCleanupIntent(intent); err != nil {
				t.Fatalf("idempotent cleanup intent: %v", err)
			}
			stored, found, err := cleanup.LoadStrictMigrationCleanupIntent(runID, owner.EpochID)
			if err != nil || !found || stored != intent {
				t.Fatalf("stored cleanup intent = %#v found=%v err=%v", stored, found, err)
			}
			changed := intent
			changed.CapturedAt = changed.CapturedAt.Add(time.Nanosecond)
			if err := cleanup.SaveStrictMigrationCleanupIntent(changed); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("changed cleanup intent error = %v", err)
			}
		})
	}
}
