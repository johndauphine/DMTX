package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

func TestSQLiteToSQLiteCopiesSchemaAndRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO users (name) VALUES ('Ada'), ('Grace')`)
	if err != nil {
		t.Fatal(err)
	}
	source.Close()

	result, err := SQLiteToSQLite(context.Background(), config.Config{Source: config.Endpoint{Type: "sqlite", Database: sourcePath}, Target: config.Endpoint{Type: "sqlite", Database: targetPath}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 2 {
		t.Fatalf("result = %#v", result)
	}

	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("target rows = %d", count)
	}
}
