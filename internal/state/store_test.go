package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStorePersistsLatestRun(t *testing.T) {
	store := Store{Path: filepath.Join(t.TempDir(), "private", "runs.json")}
	run := Run{ID: "run-1", Source: "source.db", Target: "target.db", Outcome: Success, StartedAt: time.Now()}
	if err := store.Append(run); err != nil {
		t.Fatal(err)
	}
	latest, found, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest.ID != run.ID || latest.Outcome != Success {
		t.Fatalf("latest = %#v, found = %v", latest, found)
	}
}
