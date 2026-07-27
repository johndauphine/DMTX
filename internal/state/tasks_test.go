package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStoreRequiresKnownRunningTaskForCompletion(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "runs.db")}
	started := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	if err := store.CreateTask(Task{RunID: "run-1", Table: "users", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTask("run-1", "users", 2, started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTask("run-1", "users", 2, started.Add(time.Minute)); err == nil {
		t.Fatal("expected a completed task to reject a second completion")
	}
	tasks, err := store.ListTasks("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "completed" || tasks[0].RowsDone != 2 {
		t.Fatalf("tasks = %#v", tasks)
	}
}

func TestSQLiteStorePersistsRowNumberCheckpoint(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "runs.db")}
	started := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	if err := store.CreateTask(Task{RunID: "run-1", Table: "users", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceRowNumberTask("run-1", "users", 500, 500); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.ListTasks("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].RowsDone != 500 || tasks[0].RowNumberWatermark == nil || *tasks[0].RowNumberWatermark != 500 {
		t.Fatalf("tasks = %#v", tasks)
	}
}
