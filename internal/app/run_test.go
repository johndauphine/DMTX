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
}
