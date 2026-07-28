package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestReleasedLeaseRetainsMonotonicFencingGeneration(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "lease.db")}
	first, err := store.AcquireLease("sqlite:/target.db", "run-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseLease(first); err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireLease("sqlite:/target.db", "run-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("generation reset from %d to %d", first.Generation, second.Generation)
	}
	if err := store.RenewLease(first); err == nil {
		t.Fatal("released generation renewed after a new acquisition")
	}
	if err := store.ReleaseLease(first); err == nil {
		t.Fatal("released generation removed a newer acquisition")
	}
}
