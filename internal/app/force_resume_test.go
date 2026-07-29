package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestForceResumeAcceptsPersistedStructuralCompatibility(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO items (id, value) VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	originalData := sqliteResumeConfig(sourcePath, targetPath, 500, nil)
	original, err := config.Parse(originalData)
	if err != nil {
		t.Fatalf("parse original config: %v", err)
	}
	originalHash, err := config.Hash(original)
	if err != nil {
		t.Fatal(err)
	}
	compatibilityHash, err := config.ResumeCompatibilityHash(original)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := state.NewBackend(statePath)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Minute).UTC()
	if err := backend.InitializeRun(state.Run{
		ID:        "compatible-resume",
		Source:    sourcePath,
		Target:    targetPath,
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}, originalHash); err != nil {
		t.Fatalf("initialize run: %v", err)
	}
	if err := backend.SaveResumeCompatibilityHash(
		"compatible-resume", compatibilityHash,
	); err != nil {
		t.Fatalf("save compatibility evidence: %v", err)
	}
	if err := backend.UpdateRecoverableOutcome(
		"compatible-resume",
		state.Failed,
		"interrupted before transfer",
		started.Add(time.Second),
	); err != nil {
		t.Fatalf("mark run recoverable: %v", err)
	}

	changedData := sqliteResumeConfig(sourcePath, targetPath, 1, nil)
	if err := os.WriteFile(configPath, changedData, 0o600); err != nil {
		t.Fatalf("write changed config: %v", err)
	}
	var stdout, stderr bytes.Buffer
	exitCode := Run([]string{
		"resume",
		"--config", configPath,
		"--state", statePath,
		"--force-resume",
		"--acknowledge-destructive",
	}, &stdout, &stderr)
	if exitCode != Success {
		t.Fatalf("force resume exit=%d stderr=%s", exitCode, stderr.String())
	}
	if got := sqliteRowCount(t, targetPath, "items"); got != 2 {
		t.Fatalf("target rows = %d, want 2", got)
	}
	changed, err := config.Parse(changedData)
	if err != nil {
		t.Fatal(err)
	}
	changedHash, err := config.Hash(changed)
	if err != nil {
		t.Fatal(err)
	}
	persistedHash, found, err := backend.ConfigHash("compatible-resume")
	if err != nil || !found || persistedHash != changedHash {
		t.Fatalf("persisted hash=%q found=%t err=%v, want %q",
			persistedHash, found, err, changedHash)
	}
	found, err = audit.HasEvent(
		configPath+".audit.ndjson",
		"compatible-resume",
		"resume_config_override",
	)
	if err != nil || !found {
		t.Fatalf("override audit found=%t err=%v", found, err)
	}
}

func TestForceResumeRejectsStructuralChangeBeforeTargetMutation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO items (id, value) VALUES (1, 'one');
	`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	originalData := sqliteResumeConfig(sourcePath, targetPath, 500, nil)
	original, err := config.Parse(originalData)
	if err != nil {
		t.Fatal(err)
	}
	originalHash, err := config.Hash(original)
	if err != nil {
		t.Fatal(err)
	}
	compatibilityHash, err := config.ResumeCompatibilityHash(original)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := state.NewBackend(statePath)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Minute).UTC()
	if err := backend.InitializeRun(state.Run{
		ID:        "structural-resume",
		Source:    sourcePath,
		Target:    targetPath,
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}, originalHash); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveResumeCompatibilityHash(
		"structural-resume", compatibilityHash,
	); err != nil {
		t.Fatal(err)
	}

	changedData := sqliteResumeConfig(
		sourcePath,
		targetPath,
		500,
		[]string{"other"},
	)
	if err := os.WriteFile(configPath, changedData, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	exitCode := Run([]string{
		"resume",
		"--config", configPath,
		"--state", statePath,
		"--force-resume",
		"--acknowledge-destructive",
	}, &stdout, &stderr)
	if exitCode != ConfigurationError {
		t.Fatalf("structural force resume exit=%d stdout=%s stderr=%s",
			exitCode, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("target was touched before structural rejection: %v", err)
	}
}

func sqliteResumeConfig(
	sourcePath,
	targetPath string,
	chunkSize int,
	include []string,
) []byte {
	includeYAML := ""
	if len(include) > 0 {
		includeYAML = "\n  include_tables: [" + include[0] + "]"
	}
	return []byte(fmt.Sprintf(`source:
  type: sqlite
  database: %q
target:
  type: sqlite
  database: %q
migration:
  target_mode: drop_recreate
  chunk_size: %d%s
`, sourcePath, targetPath, chunkSize, includeYAML))
}

func sqliteRowCount(t *testing.T, databasePath, table string) int {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var count int
	if err := database.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM "`+table+`"`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
