package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
	_ "modernc.org/sqlite"
)

func TestResumeReusesValidatedCompletedTable(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createResumableDatabase(t, sourcePath)
	createResumableDatabase(t, targetPath)
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	started := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	if err := store.Append(state.Run{ID: "run-1", Source: sourcePath, Target: targetPath, Outcome: state.Running, Resumable: true, Reason: "interrupted", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(state.Run{ID: "run-1", Source: sourcePath, Target: targetPath, Outcome: state.Failed, Resumable: true, Reason: "interrupted", StartedAt: started, EndedAt: started.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(state.Task{RunID: "run-1", Table: "users", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTask("run-1", "users", 1, started.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	var output, errors bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if output.String() != "{\"tables\":1,\"rows\":1,\"validated\":true}\n" {
		t.Fatalf("result = %q", output.String())
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.Outcome != state.Success {
		t.Fatalf("latest = %#v, found = %v, error = %v", latest, found, err)
	}
}

func createResumableDatabase(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)`); err != nil {
		t.Fatal(err)
	}
}
