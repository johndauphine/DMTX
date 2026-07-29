package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	_ "modernc.org/sqlite"
)

func TestSQLiteMigrationFiltersTables(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openFilterDatabase(t, sourcePath)
	for _, statement := range []string{
		"CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO accounts VALUES (1, 'Ada')",
		"CREATE TABLE audit_log (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO audit_log VALUES (1, 'event')",
		"CREATE TABLE temp_import (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO temp_import VALUES (1, 'temporary')",
	} {
		if _, err := source.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := SQLiteToSQLite(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{"a*", "temp_*"},
			ExcludeTables: []string{"audit_*", "temp_*"},
		},
	})
	if err != nil {
		t.Fatalf("SQLiteToSQLite returned an error: %v", err)
	}
	if result.Tables != 1 || result.Rows != 1 {
		t.Fatalf("result = %+v, want one selected row", result)
	}
	target := openFilterDatabase(t, targetPath)
	defer target.Close()
	assertTableExists(t, target, "accounts", true)
	assertTableExists(t, target, "audit_log", false)
	assertTableExists(t, target, "temp_import", false)
}

func TestSQLiteMigrationRejectsEmptySelection(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openFilterDatabase(t, sourcePath)
	if _, err := source.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := SQLiteToSQLite(context.Background(), config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "drop_recreate", IncludeTables: []string{"missing_*"}},
	})
	if err == nil || !strings.Contains(err.Error(), "no source tables match") {
		t.Fatalf("SQLiteToSQLite error = %v, want no matching tables error", err)
	}
}

func openFilterDatabase(t *testing.T, file string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func assertTableExists(t *testing.T, database *sql.DB, name string, want bool) {
	t.Helper()
	var count int
	if err := database.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if got := count > 0; got != want {
		t.Fatalf("table %s exists = %v, want %v", name, got, want)
	}
}
