package migrate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCloneDryRunSQLiteSnapshotReportsPrivateCleanupFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source.db")
	if err := os.WriteFile(path, []byte("private snapshot cleanup"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := snapshotDryRunSQLiteArtifacts(path)
	if err != nil {
		t.Fatal(err)
	}
	removeErr := errors.New("injected private snapshot removal failure")
	var removedDirectory string
	snapshot, cleanup, err := cloneDryRunSQLiteSnapshotWithRemove(
		path,
		before,
		"source",
		func(directory string) error {
			removedDirectory = directory
			return removeErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDirectory := filepath.Dir(snapshot)
	defer func() {
		if err := os.RemoveAll(snapshotDirectory); err != nil {
			t.Fatal(err)
		}
	}()
	if err := cleanup(); !errors.Is(err, removeErr) {
		t.Fatalf("snapshot cleanup error = %v, want injected removal failure", err)
	}
	if removedDirectory != snapshotDirectory {
		t.Fatalf("removed snapshot directory = %q, want %q", removedDirectory, snapshotDirectory)
	}
}

func TestCloneDryRunSQLiteSnapshotJoinsPartialCopyCleanupFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source.db")
	if err := os.WriteFile(path, []byte("partial private snapshot cleanup"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := snapshotDryRunSQLiteArtifacts(path)
	if err != nil {
		t.Fatal(err)
	}
	// Force the second copy step to fail after the base database was copied.
	// The failure must retain a cleanup failure too, rather than silently leave
	// the private directory behind.
	before["-wal"] = before[""]
	removeErr := errors.New("injected partial snapshot removal failure")
	var snapshotDirectory string
	_, _, err = cloneDryRunSQLiteSnapshotWithRemove(
		path,
		before,
		"source",
		func(directory string) error {
			snapshotDirectory = directory
			return removeErr
		},
	)
	defer func() {
		if snapshotDirectory == "" {
			return
		}
		if err := os.RemoveAll(snapshotDirectory); err != nil {
			t.Fatal(err)
		}
	}()
	if !errors.Is(err, removeErr) ||
		!strings.Contains(err.Error(), "read SQLite dry-run snapshot artifact") {
		t.Fatalf("partial snapshot error = %v, want copy and cleanup failures", err)
	}
}
