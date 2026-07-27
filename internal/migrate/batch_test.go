package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

func TestSQLiteToSQLiteTransfersAcrossWriteBatchBoundary(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= sqliteWriteBatchSize+1; id++ {
		if _, err := source.Exec(`INSERT INTO items VALUES (?, ?)`, id, "item"); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := SQLiteToSQLite(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != sqliteWriteBatchSize+1 {
		t.Fatalf("rows = %d", result.Rows)
	}
}
