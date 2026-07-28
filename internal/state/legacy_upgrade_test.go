package state

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestLegacyStateUpgradePreservesCompletedHistory(t *testing.T) {
	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state"+extension)
			backend, err := NewBackend(path)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
			completedAt := started.Add(time.Minute)
			run := Run{
				ID:        "legacy-complete",
				Source:    "source.db",
				Target:    "target.db",
				Outcome:   Running,
				Resumable: true,
				Reason:    "migration in progress",
				StartedAt: started,
			}
			if err := backend.Append(run); err != nil {
				t.Fatal(err)
			}
			if err := backend.CreateTask(Task{
				RunID: "legacy-complete", Table: "items", StartedAt: started,
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.CompleteTask(
				"legacy-complete", "items", 7, completedAt,
			); err != nil {
				t.Fatal(err)
			}
			run.Outcome = Success
			run.Resumable = false
			run.Reason = "migration completed"
			run.EndedAt = completedAt
			if err := backend.Append(run); err != nil {
				t.Fatal(err)
			}

			key := TaskKey{
				Type: "table-copy", Schema: "main", Table: "new_items",
			}
			if _, err := backend.(RangeBackend).EnsureWorkPlan(
				WorkTask{
					RunID: "legacy-complete", Key: key,
					Strategy: "integer_keyset", TopologyHash: "new-schema",
					StartedAt: completedAt,
				},
				[]RangeState{{ID: "0"}},
			); err != nil {
				t.Fatalf("add Stage 2 work schema: %v", err)
			}

			reopened, err := NewBackend(path)
			if err != nil {
				t.Fatal(err)
			}
			runs, err := reopened.List()
			if err != nil {
				t.Fatal(err)
			}
			if len(runs) != 2 ||
				runs[0].Outcome != Running ||
				runs[1].Outcome != Success {
				t.Fatalf("run history after upgrade = %#v", runs)
			}
			tasks, err := reopened.ListTasks("legacy-complete")
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 ||
				tasks[0].Status != "completed" ||
				tasks[0].RowsDone != 7 {
				t.Fatalf("completed legacy task after upgrade = %#v", tasks)
			}
		})
	}
}

func TestLegacyAmbiguitySentinelIsStable(t *testing.T) {
	wrapped := errors.Join(
		ErrState,
		errors.New("checkpoint conflict"),
		ErrAmbiguousLegacy,
	)
	if !errors.Is(wrapped, ErrAmbiguousLegacy) ||
		!errors.Is(wrapped, ErrState) {
		t.Fatalf("legacy ambiguity lost classification: %v", wrapped)
	}
}
