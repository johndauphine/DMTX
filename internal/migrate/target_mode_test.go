package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	_ "modernc.org/sqlite"
)

func TestDropRecreateReplacesExistingTargetRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
	createDatabase(t, sourcePath, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES (1, 'source')`)
	createDatabase(t, targetPath, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES (2, 'stale')`)
	cfg := config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "drop_recreate"},
	}
	if _, err := SQLiteToSQLite(context.Background(), cfg); !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf("error = %v, want destructive acknowledgement", err)
	}
	if countRowsForTest(t, targetPath, "items") != 1 {
		t.Fatal("unacknowledged drop_recreate mutated target")
	}
	var stale int
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(`SELECT id FROM items`).Scan(&stale); err != nil {
		target.Close()
		t.Fatal(err)
	}
	target.Close()
	if stale != 2 {
		t.Fatalf("unacknowledged target id = %d, want 2", stale)
	}
	cfg.Migration.DestructiveAcknowledged = true
	_, err = SQLiteToSQLite(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if countRowsForTest(t, targetPath, "items") != 1 {
		t.Fatal("drop_recreate did not replace target rows")
	}
}

func TestUpsertUpdatesExistingPrimaryKey(t *testing.T) {
	directory := t.TempDir()
	sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
	createDatabase(t, sourcePath, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES (1, 'new')`)
	createDatabase(t, targetPath, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES (1, 'old')`)
	_, err := SQLiteToSQLite(context.Background(), config.Config{Source: config.Endpoint{Type: "sqlite", Database: sourcePath}, Target: config.Endpoint{Type: "sqlite", Database: targetPath}, Migration: config.Migration{TargetMode: "upsert"}})
	if err != nil {
		t.Fatal(err)
	}
	if countRowsForTest(t, targetPath, "items") != 1 {
		t.Fatal("upsert inserted a duplicate row")
	}
}

func createDatabase(t *testing.T, path, statement string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(statement); err != nil {
		t.Fatal(err)
	}
}
func countRowsForTest(t *testing.T, path, table string) int {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var count int
	if err := database.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
