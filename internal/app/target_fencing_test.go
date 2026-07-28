package app

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
)

func TestTargetMutationFenceSerializesTakeoverAndRejectsOldOwner(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	leaseStore := state.SQLiteStore{Path: filepath.Join(directory, "leases.db")}
	started := time.Now().UTC()
	if err := leaseStore.InitializeRun(state.Run{
		ID:        "old-run",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}, "hash"); err != nil {
		t.Fatalf("initialize run: %v", err)
	}
	oldLease, err := leaseStore.AcquireLease("sqlite:target.db", "old-run", time.Second)
	if err != nil {
		t.Fatalf("acquire old lease: %v", err)
	}
	database, err := leaseStore.Open()
	if err != nil {
		t.Fatalf("open lease database: %v", err)
	}
	if _, err := database.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Unix(0, 0).UTC(),
		oldLease.Target,
	); err != nil {
		database.Close()
		t.Fatalf("expire old lease: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close lease database: %v", err)
	}

	target, err := sql.Open("sqlite", filepath.Join(directory, "target.db"))
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer target.Close()
	if _, err := target.Exec(`CREATE TABLE writes (owner TEXT NOT NULL)`); err != nil {
		t.Fatalf("create target fixture: %v", err)
	}

	guard := state.NewLeaseGuard(leaseStore, oldLease)
	observer := tableCheckpointObserver{guard: guard}
	entered := make(chan struct{})
	allowCommit := make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- observer.ProtectTargetMutation(context.Background(), func() error {
			close(entered)
			<-allowCommit
			_, err := target.Exec(`INSERT INTO writes (owner) VALUES ('old')`)
			return err
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old owner did not enter protected target mutation")
	}

	takeoverStarted := make(chan struct{})
	takeoverDone := make(chan struct {
		lease state.Lease
		err   error
	}, 1)
	go func() {
		close(takeoverStarted)
		lease, err := leaseStore.AcquireLease(
			oldLease.Target,
			"new-run",
			time.Second,
		)
		takeoverDone <- struct {
			lease state.Lease
			err   error
		}{lease: lease, err: err}
	}()
	<-takeoverStarted
	var takeover struct {
		lease state.Lease
		err   error
	}
	takeoverReturned := false
	select {
	case takeover = <-takeoverDone:
		takeoverReturned = true
		if takeover.err == nil {
			t.Fatalf("takeover interleaved with protected mutation: lease=%#v", takeover.lease)
		}
	case <-time.After(100 * time.Millisecond):
	}

	close(allowCommit)
	if err := <-oldDone; err != nil {
		t.Fatalf("old protected commit: %v", err)
	}
	if !takeoverReturned {
		select {
		case takeover = <-takeoverDone:
		case <-time.After(5 * time.Second):
			t.Fatal("takeover did not finish after protected mutation")
		}
	}
	if takeover.err != nil {
		lease, retryErr := leaseStore.AcquireLease(
			oldLease.Target, "new-run", time.Second,
		)
		if retryErr != nil {
			t.Fatalf("take over expired lease after protected commit: %v", retryErr)
		}
		takeover.lease = lease
		takeover.err = nil
	}
	if takeover.lease.Generation != oldLease.Generation+1 {
		t.Fatalf("takeover generation = %d, want %d",
			takeover.lease.Generation, oldLease.Generation+1)
	}
	var rows int
	if err := target.QueryRow(`SELECT COUNT(*) FROM writes WHERE owner = 'old'`).Scan(&rows); err != nil {
		t.Fatalf("count protected target rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("protected target rows = %d, want 1", rows)
	}

	mutationCalled := false
	err = observer.ProtectTargetMutation(context.Background(), func() error {
		mutationCalled = true
		return nil
	})
	if !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("old target mutation error = %v, want ErrLeaseLost", err)
	}
	if mutationCalled {
		t.Fatal("old target mutation ran after takeover")
	}

	fenced := state.FenceBackend(leaseStore, guard)
	err = fenced.Append(state.Run{
		ID:        "old-run",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   state.Success,
		Resumable: false,
		Reason:    "must not persist",
		StartedAt: started,
		EndedAt:   time.Now().UTC(),
	})
	if !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("old success mutation error = %v, want ErrLeaseLost", err)
	}
	if err := leaseStore.ReleaseLease(takeover.lease); err != nil {
		t.Fatalf("release takeover lease: %v", err)
	}
}
