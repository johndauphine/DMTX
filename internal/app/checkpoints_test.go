package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/state"
	_ "modernc.org/sqlite"
)

func TestRunStoresCompletedTableCheckpoint(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)`); err != nil {
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
	if code := Run([]string{"run", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	latest, found, err := (state.SQLiteStore{Path: configPath + ".state.db"}).Latest()
	if err != nil || !found {
		t.Fatalf("latest = %#v, found = %v, error = %v", latest, found, err)
	}
	tasks, err := (state.SQLiteStore{Path: configPath + ".state.db"}).ListTasks(latest.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Table != "users" || tasks[0].Status != "completed" {
		t.Fatalf("tasks = %#v", tasks)
	}
	auditStream, err := os.ReadFile(configPath + ".audit.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(auditStream), `"type":"run_started"`) || !strings.Contains(string(auditStream), `"type":"run_succeeded"`) {
		t.Fatalf("audit stream = %q", auditStream)
	}
}
