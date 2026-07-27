package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestRunMigratesSQLiteConfiguration(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes (body) VALUES ('first')`); err != nil {
		t.Fatal(err)
	}
	source.Close()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"run", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if output.String() != "{\"tables\":1,\"rows\":1,\"validated\":true}\n" {
		t.Fatalf("result = %q", output.String())
	}

	output.Reset()
	if code := Run([]string{"status", "--state", configPath + ".state.db"}, &output, &errors); code != Success {
		t.Fatalf("status exit code = %d, stderr = %s", code, errors.String())
	}
	if !bytes.Contains(output.Bytes(), []byte("\"outcome\":\"success\"")) {
		t.Fatalf("status result = %q", output.String())
	}
}

func TestRunDryRunDiscoversSQLiteWithoutMutatingTargetOrState(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes (body) VALUES ('first')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"run", "--config", configPath, "--dry-run"}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if !bytes.Contains(output.Bytes(), []byte(`"tables":[{"name":"notes","rows":1}]`)) {
		t.Fatalf("plan = %q", output.String())
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("dry run created target: %v", err)
	}
	if _, err := os.Stat(configPath + ".state.db"); !os.IsNotExist(err) {
		t.Fatalf("dry run created state: %v", err)
	}
}
