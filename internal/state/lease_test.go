package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStoreRejectsSecondLiveTargetLease(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	lease, err := store.AcquireLease("sqlite:/target.db", "run-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease("sqlite:/target.db", "run-2", time.Minute); err == nil {
		t.Fatal("expected a second live target lease to be rejected")
	}
	if err := store.ReleaseLease(lease); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease("sqlite:/target.db", "run-2", time.Minute); err != nil {
		t.Fatal(err)
	}
}
