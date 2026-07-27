package app

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPreflightAcceptsDistinctSQLitePaths(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "migration.yaml")
	contents := "source:\n  type: sqlite\n  database: " + filepath.Join(directory, "source.db") + "\ntarget:\n  type: sqlite\n  database: " + filepath.Join(directory, "target.db") + "\n"
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
