package state

import (
	"path/filepath"
	"testing"
)

func TestSQLiteStorePersistsConfigurationHash(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	if err := store.SaveConfigHash("run-1", "abc123"); err != nil {
		t.Fatal(err)
	}
	hash, found, err := store.ConfigHash("run-1")
	if err != nil || !found || hash != "abc123" {
		t.Fatalf("hash = %q, found = %v, error = %v", hash, found, err)
	}
}
