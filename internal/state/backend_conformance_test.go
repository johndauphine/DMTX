package state

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestBackendConformance(t *testing.T) {
	factories := map[string]func(*testing.T) Backend{
		"sqlite": func(t *testing.T) Backend {
			return SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		},
		"yaml": func(t *testing.T) Backend {
			return YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			testBackendConformance(t, factory(t))
			t.Run("failure after hard kill", func(t *testing.T) {
				testUpdateFailureFromRunning(t, factory(t))
			})
		})
	}
}

func testUpdateFailureFromRunning(t *testing.T, backend Backend) {
	t.Helper()
	started := time.Date(2026, 7, 27, 11, 0, 0, 0, time.UTC)
	run := Run{
		ID:        "hard-killed",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}
	if err := backend.InitializeRun(run, "hash"); err != nil {
		t.Fatal(err)
	}
	failedAt := started.Add(time.Minute)
	if err := backend.UpdateFailure(run.ID, "resume failed", failedAt); err != nil {
		t.Fatal(err)
	}
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Outcome != Running || runs[1].Outcome != Failed ||
		runs[1].Reason != "resume failed" || !runs[1].EndedAt.Equal(failedAt) {
		t.Fatalf("runs = %#v", runs)
	}
	resumable, found, err := backend.LatestResumableForTarget(run.Target)
	if err != nil || !found || resumable.Outcome != Failed {
		t.Fatalf("resumable = %#v, found = %v, error = %v", resumable, found, err)
	}
}

func testBackendConformance(t *testing.T, backend Backend) {
	t.Helper()
	started := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("initial runs = %#v", runs)
	}
	if _, found, err := backend.Latest(); err != nil || found {
		t.Fatalf("initial latest found = %v, error = %v", found, err)
	}

	initial := Run{
		ID:        "run-1",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}
	if err := backend.InitializeRun(initial, "hash-1"); err != nil {
		t.Fatal(err)
	}
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("latest found = %v, error = %v", found, err)
	}
	if latest.ID != initial.ID || latest.Outcome != Running || !latest.StartedAt.Equal(started) {
		t.Fatalf("latest = %#v", latest)
	}
	hash, found, err := backend.ConfigHash(initial.ID)
	if err != nil || !found || hash != "hash-1" {
		t.Fatalf("hash = %q, found = %v, error = %v", hash, found, err)
	}

	// A conflict on the second half of initialization must not leave the run.
	if err := backend.SaveConfigHash("reserved", "old-hash"); err != nil {
		t.Fatal(err)
	}
	if err := backend.InitializeRun(Run{
		ID:        "reserved",
		Source:    "source.db",
		Target:    "other.db",
		Outcome:   Running,
		Resumable: true,
		StartedAt: started.Add(time.Second),
	}, "new-hash"); err == nil {
		t.Fatal("expected initialization with an existing config hash to fail")
	}
	runs, err = backend.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		if run.ID == "reserved" {
			t.Fatalf("failed initialization left run state: %#v", run)
		}
	}
	hash, found, err = backend.ConfigHash("reserved")
	if err != nil || !found || hash != "old-hash" {
		t.Fatalf("reserved hash = %q, found = %v, error = %v", hash, found, err)
	}

	tasks := []Task{
		{RunID: initial.ID, Table: "alpha", StartedAt: started},
		{RunID: initial.ID, Table: "beta", StartedAt: started},
	}
	if err := backend.CreateTasks(tasks); err != nil {
		t.Fatal(err)
	}
	if err := backend.CreateTasks([]Task{
		{RunID: initial.ID, Table: "gamma", StartedAt: started},
		{RunID: initial.ID, Table: "alpha", StartedAt: started},
	}); err == nil {
		t.Fatal("expected duplicate bulk task creation to fail")
	}
	if err := backend.CreateTask(Task{RunID: initial.ID, Table: "zeta", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	storedTasks, err := backend.ListTasks(initial.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskNames(storedTasks); got != "alpha,beta,zeta" {
		t.Fatalf("tasks after rejected batch = %q", got)
	}

	if err := backend.AdvanceIntegerKeysetTask(initial.ID, "alpha", 10, 42); err != nil {
		t.Fatal(err)
	}
	if err := backend.AdvanceRowNumberTask(initial.ID, "beta", 20, 20); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteTask(initial.ID, "alpha", 10, started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := backend.AdvanceIntegerKeysetTask(initial.ID, "alpha", 11, 43); err == nil {
		t.Fatal("expected completed task advancement to fail")
	}
	storedTasks, err = backend.ListTasks(initial.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedTasks[0].Status != "completed" || storedTasks[0].RowsDone != 10 ||
		storedTasks[0].IntegerWatermark == nil || *storedTasks[0].IntegerWatermark != 42 {
		t.Fatalf("integer task = %#v", storedTasks[0])
	}
	if storedTasks[1].Status != "running" || storedTasks[1].RowsDone != 20 ||
		storedTasks[1].RowNumberWatermark == nil || *storedTasks[1].RowNumberWatermark != 20 {
		t.Fatalf("row-number task = %#v", storedTasks[1])
	}

	failedAt := started.Add(2 * time.Minute)
	if err := backend.Append(Run{
		ID:        initial.ID,
		Source:    initial.Source,
		Target:    initial.Target,
		Outcome:   Failed,
		Resumable: true,
		Reason:    "interrupted",
		StartedAt: started,
		EndedAt:   failedAt,
	}); err != nil {
		t.Fatal(err)
	}
	updatedAt := failedAt.Add(time.Minute)
	if err := backend.UpdateFailure(initial.ID, "retry failed", updatedAt); err != nil {
		t.Fatal(err)
	}
	resumable, found, err := backend.LatestResumableForTarget(initial.Target)
	if err != nil || !found {
		t.Fatalf("resumable found = %v, error = %v", found, err)
	}
	if resumable.Outcome != Failed || resumable.Reason != "retry failed" || !resumable.EndedAt.Equal(updatedAt) {
		t.Fatalf("resumable = %#v", resumable)
	}

	if err := backend.Append(Run{
		ID:        initial.ID,
		Source:    initial.Source,
		Target:    initial.Target,
		Outcome:   Success,
		Resumable: false,
		Reason:    "migration completed",
		StartedAt: started,
		EndedAt:   started.Add(4 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := backend.LatestResumableForTarget(initial.Target); err != nil || found {
		t.Fatalf("resumable after success found = %v, error = %v", found, err)
	}
	runs, err = backend.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 || runs[0].Outcome != Running || runs[1].Outcome != Failed || runs[2].Outcome != Success {
		t.Fatalf("runs = %#v", runs)
	}
}

func taskNames(tasks []Task) string {
	if len(tasks) == 0 {
		return ""
	}
	names := tasks[0].Table
	for _, task := range tasks[1:] {
		names = fmt.Sprintf("%s,%s", names, task.Table)
	}
	return names
}
