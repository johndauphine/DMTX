package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestLatestRunForTargetUsesCanonicalNetworkIdentity(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	started := time.Now().UTC()
	firstTarget := config.Endpoint{
		Type: "postgresql", Host: "DB.EXAMPLE.", Database: "warehouse",
		Schema: "public",
	}
	firstIdentity, err := endpointWorkloadIdentity(firstTarget)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "matching", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: firstIdentity,
		Outcome: state.Running, Resumable: true,
		Reason: "in progress", StartedAt: started,
	}, "hash-matching"); err != nil {
		t.Fatal(err)
	}
	otherIdentity, err := endpointWorkloadIdentity(config.Endpoint{
		Type: "postgres", Host: "other.example", Database: "warehouse",
		Schema: "public",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "other", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: otherIdentity,
		Outcome: state.Running, Resumable: true,
		Reason: "in progress", StartedAt: started.Add(time.Minute),
	}, "hash-other"); err != nil {
		t.Fatal(err)
	}

	selected, found, err := latestRunForTarget(store, config.Endpoint{
		Type: "pg", Host: "db.example", Port: 5432, Database: "warehouse",
		Schema: "public",
	})
	if err != nil || !found || selected.ID != "matching" {
		t.Fatalf(
			"selected = %#v, found=%v error=%v",
			selected,
			found,
			err,
		)
	}
}

func TestLatestRunForTargetRejectsNewerTerminalNonResumableAttempt(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	target := config.Endpoint{
		Type: "postgresql", Host: "db.example", Database: "warehouse",
		Schema: "public",
	}
	identity, err := endpointWorkloadIdentity(target)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if err := store.InitializeRun(state.Run{
		ID: "old-resumable", Source: "source", Target: "warehouse",
		SourceEngine: "mssql", TargetIdentity: identity,
		Outcome: state.Running, Resumable: true,
		Reason: "in progress", StartedAt: started,
	}, "old"); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateNonResumableOutcome(
		"old-resumable",
		state.Partial,
		"SQL Server migration snapshot released after graceful failure",
		started.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	selected, found, err := latestRunForTarget(store, target)
	if err != nil || found {
		t.Fatalf("selected = %#v, found=%v error=%v; want no resume candidate", selected, found, err)
	}
}

func TestLatestRunForTargetKeepsDistinctEarlierResumableAttempt(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	target := config.Endpoint{
		Type: "postgresql", Host: "db.example", Database: "warehouse",
		Schema: "public",
	}
	identity, err := endpointWorkloadIdentity(target)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if err := store.InitializeRun(state.Run{
		ID: "old-resumable", Source: "source", Target: "warehouse",
		SourceEngine: "mssql", TargetIdentity: identity,
		Outcome: state.Failed, Resumable: true,
		Reason: "temporary failure", StartedAt: started,
	}, "old"); err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "ignored-terminal", Source: "source", Target: "warehouse",
		SourceEngine: "mssql", TargetIdentity: identity,
		Outcome: state.Partial, Resumable: false,
		Reason:    "released a distinct SQL Server migration snapshot",
		StartedAt: started.Add(time.Second),
	}, "terminal"); err != nil {
		t.Fatal(err)
	}

	selected, found, err := latestRunForTarget(store, target)
	if err != nil || !found || selected.ID != "old-resumable" {
		t.Fatalf("selected = %#v, found=%v error=%v", selected, found, err)
	}
}

func TestLatestRunForTargetMatchesSQLitePathToHardlinkTransition(
	t *testing.T,
) {
	directory := t.TempDir()
	store := state.SQLiteStore{Path: filepath.Join(directory, "state.db")}
	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := endpointWorkloadIdentity(config.Endpoint{
		Type: "sqlite", Database: target,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "sqlite-alias", Source: "source.db", Target: target,
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Running, Resumable: true,
		Reason: "in progress", StartedAt: time.Now().UTC(),
	}, "hash"); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}

	selected, found, err := latestRunForTarget(store, config.Endpoint{
		Type: "sqlite3", Database: hardlink,
	})
	if err != nil || !found || selected.ID != "sqlite-alias" {
		t.Fatalf(
			"selected = %#v, found=%v error=%v",
			selected,
			found,
			err,
		)
	}
}

func TestLatestRunForTargetRejectsLegacyNetworkIdentity(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	if err := store.InitializeRun(state.Run{
		ID: "legacy-network", Source: "source", Target: "warehouse",
		Outcome: state.Running, Resumable: true,
		Reason: "legacy", StartedAt: time.Now().UTC(),
	}, "hash"); err != nil {
		t.Fatal(err)
	}
	_, found, err := latestRunForTarget(store, config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "warehouse",
	})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("legacy network run without canonical identity was selected")
	}
}

func TestLatestRunForTargetIgnoresNewerNonResumableAttempt(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	target := config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "warehouse",
		Schema: "public",
	}
	targetIdentity, err := endpointWorkloadIdentity(target)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Minute).UTC()
	if err := store.InitializeRun(state.Run{
		ID: "resumable", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Failed, Resumable: true,
		Reason: "retryable failure", StartedAt: started,
	}, "hash-resumable"); err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "accepted-partial", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Partial, Resumable: false,
		Reason: "accepted partial", StartedAt: started.Add(time.Second),
	}, "hash-partial"); err != nil {
		t.Fatal(err)
	}

	selected, found, err := latestRunForTarget(store, target)
	if err != nil || !found || selected.ID != "resumable" {
		t.Fatalf(
			"selected = %#v, found=%v error=%v",
			selected,
			found,
			err,
		)
	}
}

func TestLatestRunForTargetLaterSuccessSupersedesResumable(t *testing.T) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	target := config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "warehouse",
		Schema: "public",
	}
	targetIdentity, err := endpointWorkloadIdentity(target)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Minute).UTC()
	if err := store.InitializeRun(state.Run{
		ID: "resumable", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Failed, Resumable: true,
		Reason: "retryable failure", StartedAt: started,
	}, "hash-resumable"); err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "success", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Success, Resumable: false,
		Reason: "complete", StartedAt: started.Add(time.Second),
		EndedAt: started.Add(2 * time.Second),
	}, "hash-success"); err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID: "abandoned", Source: "source", Target: "warehouse",
		SourceEngine: "sqlite", TargetIdentity: targetIdentity,
		Outcome: state.Failed, Resumable: false,
		Reason: "abandoned", StartedAt: started.Add(3 * time.Second),
	}, "hash-abandoned"); err != nil {
		t.Fatal(err)
	}

	selected, found, err := latestRunForTarget(store, target)
	if err != nil || !found || selected.ID != "success" {
		t.Fatalf(
			"selected = %#v, found=%v error=%v",
			selected,
			found,
			err,
		)
	}
}

func TestResumeCheckpointObserverRejectsDisappearedIncompleteTask(
	t *testing.T,
) {
	store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	const runID = "resume-topology"
	if err := store.InitializeRun(state.Run{
		ID: runID, Source: "source", Target: "target",
		Outcome: state.Running, Resumable: true,
		Reason: "in progress", StartedAt: time.Now().UTC(),
	}, "hash"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(state.Task{
		RunID: runID, Table: "disappeared",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	observer := resumeCheckpointObserver{
		tableCheckpointObserver: tableCheckpointObserver{
			store: store,
			runID: runID,
		},
		existing: map[string]bool{"disappeared": true},
	}
	err := observer.BeforeTables(context.Background(), []string{"current"})
	if err == nil ||
		!strings.Contains(err.Error(), "persisted checkpoints were not rediscovered") ||
		!strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("topology drift error = %v", err)
	}
	tasks, listErr := store.ListTasks(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(tasks) != 1 || tasks[0].Table != "disappeared" {
		t.Fatalf("checkpoint validation mutated tasks: %#v", tasks)
	}
}
