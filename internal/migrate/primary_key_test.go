package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

func TestSQLiteToSQLiteRejectsTableWithoutPrimaryKeyBeforeTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE logs (message TEXT); INSERT INTO logs VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = SQLiteToSQLite(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
	})
	if err == nil || !strings.Contains(err.Error(), "has no primary key") {
		t.Fatalf("error = %v", err)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'logs'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("target table count = %d", count)
	}
}
