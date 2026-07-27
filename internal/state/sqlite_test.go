package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStorePersistsRunTransitions(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "private", "runs.db")}
	started := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	completed := started.Add(time.Minute)

	if err := store.Append(Run{ID: "run-1", Source: "source.db", Target: "target.db", Outcome: Running, Resumable: true, Reason: "migration in progress", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(Run{ID: "run-1", Source: "source.db", Target: "target.db", Outcome: Success, Resumable: false, Reason: "migration completed", StartedAt: started, EndedAt: completed}); err != nil {
		t.Fatal(err)
	}

	latest, found, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest.Outcome != Success || !latest.EndedAt.Equal(completed) {
		t.Fatalf("latest = %#v, found = %v", latest, found)
	}

	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Outcome != Running || runs[1].Outcome != Success {
		t.Fatalf("runs = %#v", runs)
	}
}
