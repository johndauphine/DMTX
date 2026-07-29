package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestSQLiteLegacyAmbiguityFailsBeforeTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")

	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`
		CREATE TABLE sentinel (value TEXT NOT NULL);
		INSERT INTO sentinel VALUES ('unchanged');
	`); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = SQLiteToSQLiteResumeWithProgress(
		context.Background(),
		config.Config{
			Source: config.Endpoint{
				Type: "sqlite", Database: sourcePath,
			},
			Target: config.Endpoint{
				Type: "sqlite", Database: targetPath,
			},
			Migration: config.Migration{
				TargetMode:    "upsert",
				IncludeTables: []string{"items"},
			},
		},
		nil,
		map[string]TableProgress{
			"items": {RowsDone: 1},
		},
		nil,
	)
	if !errors.Is(err, state.ErrAmbiguousLegacy) {
		t.Fatalf("error = %v, want ErrAmbiguousLegacy", err)
	}
	if ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("error class = %q, want state", ClassifyTransferError(err))
	}

	target, err = sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var sentinel string
	if err := target.QueryRow(`SELECT value FROM sentinel`).Scan(&sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel != "unchanged" {
		t.Fatalf("sentinel = %q, want unchanged", sentinel)
	}
	var created int
	if err := target.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'items'
	`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatal("ambiguous legacy progress mutated the selected target table")
	}
}
