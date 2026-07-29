package app

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

const (
	stage1BoundaryModeEnv    = "DMTX_STAGE1_BOUNDARY_MODE"
	stage1BoundaryEventEnv   = "DMTX_STAGE1_BOUNDARY_EVENT"
	stage1BoundaryProceedEnv = "DMTX_STAGE1_BOUNDARY_PROCEED"
	stage1BoundaryAttemptEnv = "DMTX_STAGE1_BOUNDARY_ATTEMPT"
)

func TestStage1SQLiteHardKillDuringRowNumberCheckpointResumesExactRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := configPath + ".state.db"
	const (
		runID     = "stage1-row-number-hard-kill"
		totalRows = 1253
	)

	createStage1RowNumberSource(t, sourcePath, totalRows)
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)
	store := state.SQLiteStore{Path: statePath}
	initializeStage1Run(t, store, cfg, runID)

	commitPath := filepath.Join(directory, "page-committed")
	child := startStage1Child(t, stage2RangeAckHelperCommand(
		configPath,
		runID,
		commitPath,
	))
	child.waitForFile(t, commitPath, "ROW_NUMBER target commit")
	if rows := stage1TargetRowCount(t, targetPath); rows != 500 {
		t.Fatalf("committed target rows before kill = %d, want 500", rows)
	}
	child.kill(t)

	tasks, err := store.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "running" || tasks[0].RowsDone != 0 ||
		tasks[0].IntegerWatermark != nil || tasks[0].RowNumberWatermark != nil {
		t.Fatalf("legacy aggregate advanced before durable range acknowledgement: %#v", tasks)
	}
	assertStage2PendingRangeIntent(t, store, runID, 500)

	resumeStage1Fixture(t, configPath, runID, totalRows)
	assertStage1RowNumberRows(t, targetPath, totalRows)
	assertStage1RowNumberSchema(t, targetPath)
	assertStage1CompletedRun(t, store, runID, totalRows)
}

func TestStage1SQLiteHardKillAtPostValidationStateBoundaries(t *testing.T) {
	const totalRows = 501
	tests := []struct {
		name           string
		mode           string
		wantTaskStatus string
		wantTaskRows   int
	}{
		{name: "task completion", mode: "task-completion", wantTaskStatus: "running", wantTaskRows: 0},
		{name: "final run outcome", mode: "run-outcome", wantTaskStatus: "completed", wantTaskRows: totalRows},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStage1IntegerFixture(t, "stage1-"+test.mode, totalRows)
			eventPath := filepath.Join(fixture.directory, "boundary-ready")
			proceedPath := filepath.Join(fixture.directory, "boundary-proceed")
			attemptPath := filepath.Join(fixture.directory, "state-write-attempt")
			child := startStage1Child(t, stage1BoundaryHelperCommand(
				test.mode,
				fixture.configPath,
				fixture.runID,
				eventPath,
				proceedPath,
				attemptPath,
			))

			child.waitForFile(t, eventPath, test.name)
			if rows := stage1TargetRowCount(t, fixture.targetPath); rows != totalRows {
				t.Fatalf("validated target rows = %d, want %d", rows, totalRows)
			}
			assertStage1IntegerSchema(t, fixture.targetPath, totalRows)
			stateLock := acquireStage1StateWriteLock(t, fixture.statePath)
			if err := os.WriteFile(proceedPath, []byte("continue"), 0o600); err != nil {
				t.Fatal(err)
			}
			child.waitForFile(t, attemptPath, test.name+" state write")
			child.kill(t)
			stateLock.release(t)

			tasks, err := fixture.store.ListTasks(fixture.runID)
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 || tasks[0].Status != test.wantTaskStatus ||
				tasks[0].RowsDone != test.wantTaskRows {
				t.Fatalf("task at %s boundary = %#v", test.name, tasks)
			}
			assertStage2CompletedRange(t, fixture.store, fixture.runID, totalRows)
			latest, found, err := fixture.store.Latest()
			if err != nil || !found || latest.Outcome != state.Running || !latest.Resumable {
				t.Fatalf("run at %s boundary = %#v, found = %v, error = %v", test.name, latest, found, err)
			}

			resumeStage1Fixture(t, fixture.configPath, fixture.runID, totalRows)
			assertStage1ExactRows(t, fixture.targetPath, totalRows)
			assertStage1IntegerSchema(t, fixture.targetPath, totalRows)
			assertStage1CompletedRun(t, fixture.store, fixture.runID, totalRows)
		})
	}
}

func TestStage1YAMLReplacementSurvivesImmediateHardKill(t *testing.T) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "migration.state.yaml")
	eventPath := filepath.Join(directory, "replacement-acknowledged")
	proceedPath := filepath.Join(directory, "never-proceed")
	attemptPath := filepath.Join(directory, "unused-attempt")
	const runID = "yaml-hard-kill"
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	store := state.YAMLStore{Path: statePath}
	if err := store.InitializeRun(state.Run{
		ID:        runID,
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}

	child := startStage1Child(t, stage1BoundaryHelperCommand(
		"yaml-replacement",
		statePath,
		runID,
		eventPath,
		proceedPath,
		attemptPath,
	))
	child.waitForFile(t, eventPath, "YAML replacement acknowledgement")
	child.kill(t)

	runs, err := store.List()
	if err != nil {
		t.Fatalf("read YAML after hard kill: %v", err)
	}
	if len(runs) != 2 || runs[0].Outcome != state.Running || runs[1].Outcome != state.Success {
		t.Fatalf("runs after hard kill = %#v", runs)
	}
	hash, found, err := store.ConfigHash(runID)
	if err != nil || !found || hash != "config-hash" {
		t.Fatalf("config hash after hard kill = %q, found = %v, error = %v", hash, found, err)
	}
}

func TestStage1BoundaryHelperProcess(t *testing.T) {
	mode := os.Getenv(stage1BoundaryModeEnv)
	if mode == "" {
		return
	}
	path := os.Getenv(stage1HelperConfigEnv)
	runID := os.Getenv(stage1HelperRunIDEnv)
	eventPath := os.Getenv(stage1BoundaryEventEnv)
	proceedPath := os.Getenv(stage1BoundaryProceedEnv)
	attemptPath := os.Getenv(stage1BoundaryAttemptEnv)

	if mode == "yaml-replacement" {
		store := state.YAMLStore{Path: path}
		run, found, err := store.Latest()
		if err != nil || !found {
			t.Fatalf("read YAML run: found = %v, error = %v", found, err)
		}
		if err := store.Append(state.Run{
			ID:        run.ID,
			Source:    run.Source,
			Target:    run.Target,
			Outcome:   state.Success,
			Resumable: false,
			Reason:    "migration completed",
			StartedAt: run.StartedAt,
			EndedAt:   time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		if err := stage1BoundaryHandshake(context.Background(), eventPath, proceedPath, attemptPath); err != nil {
			t.Fatal(err)
		}
		t.Fatal("YAML helper was not killed")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	store := state.SQLiteStore{Path: path + ".state.db"}
	base := tableCheckpointObserver{store: store, runID: runID}
	var result migrate.Result
	switch mode {
	case "task-completion":
		result, err = migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, taskCompletionBoundaryObserver{
			tableCheckpointObserver: base,
			eventPath:               eventPath,
			proceedPath:             proceedPath,
			attemptPath:             attemptPath,
		})
	case "run-outcome":
		result, err = migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, runOutcomeBoundaryObserver{
			tableCheckpointObserver: base,
			eventPath:               eventPath,
			proceedPath:             proceedPath,
			attemptPath:             attemptPath,
		})
	default:
		t.Fatalf("unknown boundary helper mode %q", mode)
	}
	if err != nil {
		t.Fatalf("migration returned before the parent killed it: %v", err)
	}
	if mode == "run-outcome" {
		run, found, readErr := store.Latest()
		if readErr != nil || !found {
			t.Fatalf("read running state: found = %v, error = %v", found, readErr)
		}
		if err := store.Append(state.Run{
			ID:        run.ID,
			Source:    run.Source,
			Target:    run.Target,
			Outcome:   state.Success,
			Resumable: false,
			Reason:    "migration completed",
			StartedAt: run.StartedAt,
			EndedAt:   time.Now().UTC(),
		}); err != nil {
			t.Fatalf("append final outcome: %v", err)
		}
	}
	t.Fatalf("boundary helper completed unexpectedly: %#v", result)
}

type taskCompletionBoundaryObserver struct {
	tableCheckpointObserver
	eventPath   string
	proceedPath string
	attemptPath string
}

func (observer taskCompletionBoundaryObserver) AfterTable(ctx context.Context, table string, rowsDone int) error {
	if err := stage1BoundaryHandshake(ctx, observer.eventPath, observer.proceedPath, observer.attemptPath); err != nil {
		return err
	}
	return observer.tableCheckpointObserver.AfterTable(ctx, table, rowsDone)
}

type runOutcomeBoundaryObserver struct {
	tableCheckpointObserver
	eventPath   string
	proceedPath string
	attemptPath string
}

func (observer runOutcomeBoundaryObserver) AfterTable(ctx context.Context, table string, rowsDone int) error {
	if err := observer.tableCheckpointObserver.AfterTable(ctx, table, rowsDone); err != nil {
		return err
	}
	return stage1BoundaryHandshake(ctx, observer.eventPath, observer.proceedPath, observer.attemptPath)
}

func (observer stage1BlockingCheckpointObserver) AfterRowNumberPage(ctx context.Context, table string, rows int, watermark int64) error {
	if err := os.WriteFile(observer.commitPath, []byte(fmt.Sprintf("%d", rows)), 0o600); err != nil {
		return err
	}
	return observer.tableCheckpointObserver.AfterRowNumberPage(ctx, table, rows, watermark)
}

func stage1BoundaryHandshake(ctx context.Context, eventPath, proceedPath, attemptPath string) error {
	if err := os.WriteFile(eventPath, []byte("ready"), 0o600); err != nil {
		return err
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(proceedPath); err == nil {
			if attemptPath == "" {
				return nil
			}
			return os.WriteFile(attemptPath, []byte("attempt"), 0o600)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func stage1BoundaryHelperCommand(mode, path, runID, eventPath, proceedPath, attemptPath string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestStage1BoundaryHelperProcess$")
	command.Env = append(os.Environ(),
		stage1BoundaryModeEnv+"="+mode,
		stage1HelperConfigEnv+"="+path,
		stage1HelperRunIDEnv+"="+runID,
		stage1BoundaryEventEnv+"="+eventPath,
		stage1BoundaryProceedEnv+"="+proceedPath,
		stage1BoundaryAttemptEnv+"="+attemptPath,
	)
	return command
}

type stage1ChildProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	wait    chan error
	reaped  bool
}

func startStage1Child(t *testing.T, command *exec.Cmd) *stage1ChildProcess {
	t.Helper()
	child := &stage1ChildProcess{command: command, wait: make(chan error, 1)}
	command.Stdout = &child.output
	command.Stderr = &child.output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { child.wait <- command.Wait() }()
	t.Cleanup(func() {
		if child.reaped {
			return
		}
		_ = child.command.Process.Kill()
		<-child.wait
		child.reaped = true
	})
	return child
}

func (child *stage1ChildProcess) waitForFile(t *testing.T, path, boundary string) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-child.wait:
			child.reaped = true
			t.Fatalf("child exited before %s: %v\n%s", boundary, err, child.output.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s\n%s", boundary, child.output.String())
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}

func (child *stage1ChildProcess) kill(t *testing.T) {
	t.Helper()
	if err := child.command.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v\n%s", err, child.output.String())
	}
	if err := <-child.wait; err == nil {
		t.Fatalf("child exited successfully instead of being killed\n%s", child.output.String())
	}
	child.reaped = true
}

type stage1StateWriteLock struct {
	database   *sql.DB
	connection *sql.Conn
	released   bool
}

func acquireStage1StateWriteLock(t *testing.T, path string) *stage1StateWriteLock {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := database.Conn(context.Background())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		_ = connection.Close()
		_ = database.Close()
		t.Fatalf("lock state checkpoint database: %v", err)
	}
	lock := &stage1StateWriteLock{database: database, connection: connection}
	t.Cleanup(func() {
		if !lock.released {
			_, _ = lock.connection.ExecContext(context.Background(), "ROLLBACK")
			_ = lock.connection.Close()
			_ = lock.database.Close()
			lock.released = true
		}
	})
	return lock
}

func (lock *stage1StateWriteLock) release(t *testing.T) {
	t.Helper()
	if lock.released {
		return
	}
	if _, err := lock.connection.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("release state checkpoint lock: %v", err)
	}
	if err := lock.connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lock.database.Close(); err != nil {
		t.Fatal(err)
	}
	lock.released = true
}

type stage1IntegerFixture struct {
	directory  string
	configPath string
	statePath  string
	targetPath string
	runID      string
	store      state.SQLiteStore
}

func newStage1IntegerFixture(t *testing.T, runID string, totalRows int) stage1IntegerFixture {
	t.Helper()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createStage1FinalizationSource(t, sourcePath, totalRows)
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	initializeStage1Run(t, store, cfg, runID)
	return stage1IntegerFixture{
		directory:  directory,
		configPath: configPath,
		statePath:  configPath + ".state.db",
		targetPath: targetPath,
		runID:      runID,
		store:      store,
	}
}

func writeStage1UpsertConfig(t *testing.T, configPath, sourcePath, targetPath string) config.Config {
	t.Helper()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func initializeStage1Run(t *testing.T, store state.Backend, cfg config.Config, runID string) {
	t.Helper()
	hash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeRun(state.Run{
		ID:        runID,
		Source:    cfg.Source.Database,
		Target:    cfg.Target.Database,
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "migration in progress",
		StartedAt: time.Now().UTC(),
	}, hash); err != nil {
		t.Fatal(err)
	}
}

func resumeStage1Fixture(t *testing.T, configPath, runID string, totalRows int) {
	t.Helper()
	command := stage1HelperCommand("resume", configPath, runID, "", "", "")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("resume helper failed: %v\n%s", err, output)
	}
	expected := fmt.Sprintf(`{"tables":1,"rows":%d,"validated":true}`, totalRows)
	if !bytes.Contains(output, []byte(expected)) {
		t.Fatalf("resume output = %q", output)
	}
}

func assertStage1CompletedRun(t *testing.T, store state.Backend, runID string, totalRows int) {
	t.Helper()
	tasks, err := store.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "completed" || tasks[0].RowsDone != totalRows {
		t.Fatalf("completed checkpoint = %#v", tasks)
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.Outcome != state.Success || latest.Resumable {
		t.Fatalf("latest run = %#v, found = %v, error = %v", latest, found, err)
	}
}

func createStage1FinalizationSource(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
		CREATE UNIQUE INDEX users_payload_uidx ON users(payload)
	`); err != nil {
		t.Fatal(err)
	}
	transaction, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	statement, err := transaction.Prepare(`INSERT INTO users (id, payload) VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= totalRows; id++ {
		if _, err := statement.Exec(id, fmt.Sprintf("payload-%04d", id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertStage1IntegerSchema(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var ddl string
	if err := database.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(ddl), "AUTOINCREMENT") {
		t.Fatalf("target DDL lost AUTOINCREMENT: %s", ddl)
	}
	var indexes int
	if err := database.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'users_payload_uidx'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatalf("target secondary index count = %d", indexes)
	}
	var sequence int
	if err := database.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'users'`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if sequence != totalRows {
		t.Fatalf("target sequence = %d, want %d", sequence, totalRows)
	}
}

func createStage1RowNumberSource(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE users (
			tenant TEXT NOT NULL,
			external_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, external_id)
		);
		CREATE INDEX users_payload_idx ON users(payload)
	`); err != nil {
		t.Fatal(err)
	}
	transaction, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	statement, err := transaction.Prepare(`INSERT INTO users (tenant, external_id, payload) VALUES (?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= totalRows; id++ {
		if _, err := statement.Exec(
			fmt.Sprintf("tenant-%02d", id%7),
			fmt.Sprintf("external-%04d", id),
			fmt.Sprintf("payload-%04d", id),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertStage1RowNumberRows(t *testing.T, path string, totalRows int) {
	t.Helper()
	expected := make(map[string]string, totalRows)
	for id := 1; id <= totalRows; id++ {
		key := fmt.Sprintf("tenant-%02d\x00external-%04d", id%7, id)
		expected[key] = fmt.Sprintf("payload-%04d", id)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rows, err := database.Query(`SELECT tenant, external_id, payload FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var tenant, externalID, payload string
		if err := rows.Scan(&tenant, &externalID, &payload); err != nil {
			t.Fatal(err)
		}
		key := tenant + "\x00" + externalID
		if want, found := expected[key]; !found || payload != want {
			t.Fatalf("unexpected target row (%q, %q, %q)", tenant, externalID, payload)
		}
		delete(expected, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(expected) != 0 {
		t.Fatalf("target is missing %d exact rows", len(expected))
	}
}

func assertStage1RowNumberSchema(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var indexes int
	if err := database.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'users_payload_idx'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatalf("target secondary index count = %d", indexes)
	}
	rows, err := database.Query(`PRAGMA table_info(users)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	primaryKeys := map[string]int{}
	for rows.Next() {
		var position, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&position, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		if primaryKey > 0 {
			primaryKeys[name] = primaryKey
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if primaryKeys["tenant"] != 1 || primaryKeys["external_id"] != 2 {
		t.Fatalf("target primary key order = %#v", primaryKeys)
	}
}
