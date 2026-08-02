package state

import (
	"path/filepath"
	"sync"
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

func TestSQLiteStoreRejectsRenewalFromStaleOwner(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	oldLease, err := store.AcquireLease("sqlite:/target.db", "run-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	database, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`UPDATE leases SET heartbeat_at = ? WHERE target = ?`, time.Now().Add(-time.Hour), "sqlite:/target.db"); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease("sqlite:/target.db", "run-2", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewLease(oldLease); err == nil {
		t.Fatal("expected stale lease renewal to fail")
	}
}

func TestSQLiteStoreMatchingLeaseRejectsLiveAliasAndFencesStaleOwner(
	t *testing.T,
) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	first, err := store.AcquireLeaseMatching(
		"sqlite:path:/target.db",
		"run-1",
		time.Minute,
		func(existing string) (bool, error) {
			return existing == "sqlite:file:1:2", nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLeaseMatching(
		"sqlite:file:1:2",
		"run-2",
		time.Minute,
		func(existing string) (bool, error) {
			return existing == "sqlite:path:/target.db", nil
		},
	); err == nil {
		t.Fatal("expected a live alias lease to be rejected")
	}

	database, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Now().Add(-time.Hour),
		first.Target,
	); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireLeaseMatching(
		"sqlite:file:1:2",
		"run-2",
		time.Minute,
		func(existing string) (bool, error) {
			return existing == "sqlite:path:/target.db", nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf(
			"alias takeover generation = %d, want %d",
			second.Generation,
			first.Generation+1,
		)
	}
	if err := store.RenewLease(first); err == nil {
		t.Fatal("stale aliased owner renewed after takeover")
	}
}

func TestSQLiteStoreMatchingLeaseSerializesAliasRace(t *testing.T) {
	store := SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	targets := []string{"sqlite:path:/target.db", "sqlite:file:1:2"}
	start := make(chan struct{})
	results := make(chan struct {
		lease Lease
		err   error
	}, len(targets))
	var wait sync.WaitGroup
	for index, target := range targets {
		index, target := index, target
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, err := store.AcquireLeaseMatching(
				target,
				"run-"+string(rune('1'+index)),
				time.Minute,
				func(existing string) (bool, error) {
					return existing == targets[1-index], nil
				},
			)
			results <- struct {
				lease Lease
				err   error
			}{lease: lease, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var acquired []Lease
	var rejected int
	for result := range results {
		if result.err != nil {
			rejected++
			continue
		}
		acquired = append(acquired, result.lease)
	}
	if len(acquired) != 1 || rejected != 1 {
		t.Fatalf(
			"alias race acquired=%#v rejected=%d, want one each",
			acquired,
			rejected,
		)
	}
	if err := store.ReleaseLease(acquired[0]); err != nil {
		t.Fatal(err)
	}
}
