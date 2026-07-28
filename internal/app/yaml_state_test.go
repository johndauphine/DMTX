package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
	_ "modernc.org/sqlite"
)

func TestRunStatusAndHistoryWithExplicitYAMLState(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.yaml")
	createResumableDatabase(t, sourcePath)
	writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"run", "--config", configPath, "--state", statePath}, &stdout, &stderr); code != Success {
		t.Fatalf("run exit code = %d, stderr = %s", code, stderr.String())
	}
	if stdout.String() != "{\"tables\":1,\"rows\":1,\"validated\":true}\n" {
		t.Fatalf("run result = %q", stdout.String())
	}
	if _, err := os.Stat(configPath + ".state.db"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run created default state database: %v", err)
	}

	store := state.YAMLStore{Path: statePath}
	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Outcome != state.Running || runs[1].Outcome != state.Success {
		t.Fatalf("runs = %#v", runs)
	}
	tasks, err := store.ListTasks(runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Table != "users" || tasks[0].Status != "completed" {
		t.Fatalf("tasks = %#v", tasks)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"status", "--state", statePath}, &stdout, &stderr); code != Success {
		t.Fatalf("status exit code = %d, output = %s", code, stdout.String())
	}
	var latest state.Run
	if err := json.Unmarshal(stdout.Bytes(), &latest); err != nil {
		t.Fatal(err)
	}
	if latest.Outcome != state.Success {
		t.Fatalf("status run = %#v", latest)
	}

	stdout.Reset()
	if code := Run([]string{"history", "--state", statePath}, &stdout, &stderr); code != Success {
		t.Fatalf("history exit code = %d, output = %s", code, stdout.String())
	}
	var history []state.Run
	if err := json.Unmarshal(stdout.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Outcome != state.Running || history[1].Outcome != state.Success {
		t.Fatalf("history = %#v", history)
	}
}

func TestResumeFromExplicitYAMLState(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.yml")
	createResumableDatabase(t, sourcePath)
	cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
	hash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	store := state.YAMLStore{Path: statePath}
	if err := store.InitializeRun(state.Run{
		ID:        "yaml-interrupted",
		Source:    sourcePath,
		Target:    targetPath,
		Outcome:   state.Failed,
		Resumable: true,
		Reason:    "interrupted",
		StartedAt: started,
		EndedAt:   started,
	}, hash); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath, "--state", statePath}, &stdout, &stderr); code != Success {
		t.Fatalf("resume exit code = %d, stderr = %s", code, stderr.String())
	}
	if stdout.String() != "{\"tables\":1,\"rows\":1,\"validated\":true}\n" {
		t.Fatalf("resume result = %q", stdout.String())
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.Outcome != state.Success || latest.Resumable {
		t.Fatalf("latest = %#v, found = %v, error = %v", latest, found, err)
	}
	tasks, err := store.ListTasks("yaml-interrupted")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Table != "users" || tasks[0].Status != "completed" || tasks[0].RowsDone != 1 {
		t.Fatalf("tasks = %#v", tasks)
	}
}

func TestRunAndResumeRejectInvalidStateArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "run missing state path", args: []string{"run", "--config", "migration.yaml", "--state"}},
		{name: "run duplicate state", args: []string{"run", "--config", "migration.yaml", "--state", "one.yaml", "--state", "two.yaml"}},
		{name: "run duplicate destructive acknowledgement", args: []string{"run", "--config", "migration.yaml", "--acknowledge-destructive", "--acknowledge-destructive"}},
		{name: "resume missing state path", args: []string{"resume", "--config", "migration.yaml", "--state"}},
		{name: "resume duplicate state", args: []string{"resume", "--config", "migration.yaml", "--state", "one.yaml", "--state", "two.yaml"}},
		{name: "resume duplicate destructive acknowledgement", args: []string{"resume", "--config", "migration.yaml", "--acknowledge-destructive", "--acknowledge-destructive"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := Run(test.args, &stdout, &stderr); code != ConfigurationError {
				t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: dmt") {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

func TestRunAndResumeRejectSameSQLiteFileBeforeOperationalWrites(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createResumableDatabase(t, sourcePath)
	aliasPath := filepath.Join(directory, "unused", "..", "source.db")
	writeSQLiteStateConfig(t, configPath, sourcePath, aliasPath)

	for _, command := range []string{"run", "resume"} {
		t.Run(command, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := Run([]string{command, "--config", configPath}, &stdout, &stderr); code != ConfigurationError {
				t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "same endpoint") {
				t.Fatalf("stderr = %q", stderr.String())
			}
			for _, unexpected := range []string{
				configPath + ".state.db",
				configPath + ".audit.ndjson",
				sourcePath + ".dmtx-lease.db",
			} {
				if _, err := os.Stat(unexpected); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("operational file %s exists or cannot be checked: %v", unexpected, err)
				}
			}
		})
	}

	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var rows int
	if err := source.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("source rows = %d, want 1", rows)
	}
}

func TestResumeFindsRunThroughSQLiteHardlinkAlias(t *testing.T) {
	directory := t.TempDir()
	cache := filepath.Join(directory, "cache")
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("LOCALAPPDATA", cache)
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	targetAlias := filepath.Join(directory, "target-hardlink.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := configPath + ".state.db"
	createResumableDatabase(t, sourcePath)
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY)`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(targetPath, targetAlias); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}

	originalConfiguration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n"
	original, err := config.Parse([]byte(originalConfiguration))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := config.Hash(original)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	store := state.SQLiteStore{Path: statePath}
	if err := store.InitializeRun(state.Run{
		ID:        "hardlink-resume",
		Source:    original.Source.Database,
		Target:    original.Target.Database,
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}, hash); err != nil {
		t.Fatal(err)
	}
	aliasConfiguration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetAlias +
		"\nmigration:\n  target_mode: upsert\n"
	if err := os.WriteFile(configPath, []byte(aliasConfiguration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != Success {
		t.Fatalf("resume exit code = %d, stderr = %s", code, stderr.String())
	}
	var rows int
	target, err = sql.Open("sqlite", targetAlias)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		target.Close()
		t.Fatal(err)
	}
	target.Close()
	if rows != 1 {
		t.Fatalf("target rows = %d, want 1", rows)
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.Outcome != state.Success {
		t.Fatalf("latest = %#v, found = %v, error = %v", latest, found, err)
	}
}

func TestRunRequiresDestructiveAcknowledgementForPopulatedTarget(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createDatabaseForAppTest(t, sourcePath, 1, "source")
	createDatabaseForAppTest(t, targetPath, 2, "stale")
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: drop_recreate\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"run", "--config", configPath}, &stdout, &stderr); code != ConfigurationError {
		t.Fatalf("unacknowledged exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--acknowledge-destructive") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if id, value := readAppTestRow(t, targetPath); id != 2 || value != "stale" {
		t.Fatalf("unacknowledged target = (%d, %q)", id, value)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != ConfigurationError {
		t.Fatalf("unacknowledged resume exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--acknowledge-destructive") {
		t.Fatalf("resume stderr = %q", stderr.String())
	}
	if id, value := readAppTestRow(t, targetPath); id != 2 || value != "stale" {
		t.Fatalf("unacknowledged resume target = (%d, %q)", id, value)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"resume", "--config", configPath, "--acknowledge-destructive"}, &stdout, &stderr); code != Success {
		t.Fatalf("acknowledged resume exit code = %d, stderr = %s", code, stderr.String())
	}
	if id, value := readAppTestRow(t, targetPath); id != 1 || value != "source" {
		t.Fatalf("acknowledged resume target = (%d, %q)", id, value)
	}
}

func createDatabaseForAppTest(t *testing.T, path string, id int, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO users VALUES (?, ?)`, id, value); err != nil {
		t.Fatal(err)
	}
}

func readAppTestRow(t *testing.T, path string) (int, string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var id int
	var value string
	if err := database.QueryRow(`SELECT id, value FROM users`).Scan(&id, &value); err != nil {
		t.Fatal(err)
	}
	return id, value
}

func TestTaskInitializationFailurePrecedesTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createResumableDatabase(t, sourcePath)
	cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
	backing := state.SQLiteStore{Path: filepath.Join(directory, "state.db")}
	observer := tableCheckpointObserver{
		store: taskInitializationFailureBackend{Backend: backing},
		runID: "run-1",
	}

	_, err := migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
	if err == nil || !errors.Is(err, state.ErrState) {
		t.Fatalf("migration error = %v", err)
	}
	if code := migrationExitCode(err); code != StateError {
		t.Fatalf("exit code = %d", code)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var tables int
	if err := target.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("target table was created before durable task initialization")
	}
}

type taskInitializationFailureBackend struct {
	state.Backend
}

func (taskInitializationFailureBackend) CreateTasks([]state.Task) error {
	return errors.New("injected task initialization failure")
}

func writeSQLiteStateConfig(t *testing.T, configPath, sourcePath, targetPath string) config.Config {
	t.Helper()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
