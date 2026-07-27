package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPreflightAcceptsDistinctSQLitePaths(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "migration.yaml")
	sourcePath := filepath.Join(directory, "source.db")
	database, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE source_probe (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	contents := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  database: " + filepath.Join(directory, "target.db") + "\n  type: sqlite\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"preflight", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("code = %d, stderr = %s", code, errors.String())
	}
	if output.String() != "[]\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestPreflightRejectsMissingSourceWithoutCreatingIt(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "missing-source.db")
	configPath := filepath.Join(directory, "migration.yaml")
	contents := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + filepath.Join(directory, "target.db") + "\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"preflight", "--config", configPath}, &output, &errors); code != ConfigurationError {
		t.Fatalf("code = %d, stderr = %s", code, errors.String())
	}
	if output.String() != "[{\"class\":\"source_missing\",\"severity\":\"error\",\"message\":\"source database does not exist\"}]\n" {
		t.Fatalf("output = %q", output.String())
	}
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("missing source was created: %v", err)
	}
}

func TestPreflightRejectsSameDatabase(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.db")
	configPath := filepath.Join(directory, "migration.yaml")
	contents := "source:\n  type: sqlite\n  database: " + database + "\ntarget:\n  type: sqlite\n  database: " + database + "\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"health-check", "--config", configPath}, &output, &errors); code != ConfigurationError {
		t.Fatalf("code = %d", code)
	}
	if output.String() != "[{\"class\":\"same_database\",\"severity\":\"error\",\"message\":\"source and target SQLite databases must differ\"}]\n" {
		t.Fatalf("output = %q", output.String())
	}
}
