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
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

const (
	stage1HelperModeEnv    = "DMTX_STAGE1_HELPER_MODE"
	stage1HelperConfigEnv  = "DMTX_STAGE1_HELPER_CONFIG"
	stage1HelperRunIDEnv   = "DMTX_STAGE1_HELPER_RUN_ID"
	stage1HelperReadyEnv   = "DMTX_STAGE1_HELPER_READY"
	stage1HelperProceedEnv = "DMTX_STAGE1_HELPER_PROCEED"
	stage1HelperCommitEnv  = "DMTX_STAGE1_HELPER_COMMIT"
	stage1HelperAckEnv     = "DMTX_STAGE1_HELPER_ACK"
)

func TestStage1SQLiteHardKillDuringPageCheckpointResumesExactRows(t *testing.T) {
	for _, targetMode := range []string{"upsert", "drop_recreate"} {
		targetMode := targetMode
		t.Run(targetMode, func(t *testing.T) {
			testStage1SQLiteHardKillDuringPageCheckpointResumesExactRows(t, targetMode)
		})
	}
}

func testStage1SQLiteHardKillDuringPageCheckpointResumesExactRows(t *testing.T, targetMode string) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := configPath + ".state.db"
	const totalRows = 1253

	createStage1CrashSource(t, sourcePath, totalRows)
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: " + targetMode + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}

	runID := "stage1-hard-kill-" + targetMode
	store := state.SQLiteStore{Path: statePath}
	initializeStage1Run(t, store, cfg, runID)

	readyPath := filepath.Join(directory, "table-ready")
	proceedPath := filepath.Join(directory, "continue-copy")
	commitPath := filepath.Join(directory, "page-committed")
	crashCommand := stage1HelperCommand("crash", configPath, runID, readyPath, proceedPath, commitPath)
	var crashOutput bytes.Buffer
	crashCommand.Stdout = &crashOutput
	crashCommand.Stderr = &crashOutput
	if err := crashCommand.Start(); err != nil {
		t.Fatal(err)
	}
	crashWait := make(chan error, 1)
	go func() { crashWait <- crashCommand.Wait() }()
	crashReaped := false
	t.Cleanup(func() {
		if crashReaped {
			return
		}
		_ = crashCommand.Process.Kill()
		<-crashWait
	})

	waitForStage1HelperReady(t, readyPath, crashWait, &crashReaped, &crashOutput)
	assertStage2PendingRangeIntent(t, store, runID, 500)

	if err := os.WriteFile(proceedPath, []byte("continue"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForStage2ChunkCommit(t, commitPath, crashWait, &crashReaped, &crashOutput)
	if rows := stage1TargetRowCount(t, targetPath); rows != 500 {
		t.Fatalf("committed target rows before kill = %d, want 500", rows)
	}

	if err := crashCommand.Process.Kill(); err != nil {
		t.Fatalf("kill migration helper: %v; output: %s", err, crashOutput.String())
	}
	if err := <-crashWait; err == nil {
		t.Fatalf("migration helper exited successfully instead of being killed; output: %s", crashOutput.String())
	}
	crashReaped = true

	assertStage2PendingRangeIntent(t, store, runID, 500)
	if rows := stage1TargetRowCount(t, targetPath); rows != 500 {
		t.Fatalf("committed target rows before resume = %d, want 500", rows)
	}

	resumeCommand := stage1HelperCommand("resume", configPath, runID, "", "", "")
	if targetMode == "drop_recreate" {
		resumeCommand.Env = append(resumeCommand.Env, stage1HelperAckEnv+"=1")
	}
	resumeOutput, err := resumeCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("resume helper failed: %v\n%s", err, resumeOutput)
	}
	if !bytes.Contains(resumeOutput, []byte(`{"tables":1,"rows":1253,"validated":true}`)) {
		t.Fatalf("resume output = %q", resumeOutput)
	}

	assertStage1ExactRows(t, targetPath, totalRows)
	assertStage1CrashIntegerMetadata(t, targetPath, totalRows+1000)
	assertStage2CompletedRange(t, store, runID, totalRows)
	latest, found, err := store.Latest()
	if err != nil || !found || latest.Outcome != state.Success || latest.Resumable {
		t.Fatalf("latest run = %#v, found = %v, error = %v", latest, found, err)
	}
}

func TestStage1SQLiteCrashResumeHelperProcess(t *testing.T) {
	mode := os.Getenv(stage1HelperModeEnv)
	if mode == "" {
		return
	}
	configPath := os.Getenv(stage1HelperConfigEnv)
	switch mode {
	case "crash":
		data, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Parse(data)
		if err != nil {
			t.Fatal(err)
		}
		observer := stage1BlockingCheckpointObserver{
			tableCheckpointObserver: tableCheckpointObserver{
				store: state.SQLiteStore{Path: configPath + ".state.db"},
				runID: os.Getenv(stage1HelperRunIDEnv),
			},
			readyPath:   os.Getenv(stage1HelperReadyEnv),
			proceedPath: os.Getenv(stage1HelperProceedEnv),
			commitPath:  os.Getenv(stage1HelperCommitEnv),
		}
		if _, err := migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, observer); err != nil {
			t.Fatalf("migration returned before the parent killed it: %v", err)
		}
		t.Fatal("migration completed before the parent killed it")
	case "resume":
		args := []string{"resume", "--config", configPath}
		if os.Getenv(stage1HelperAckEnv) == "1" {
			args = append(args, "--acknowledge-destructive")
		}
		os.Exit(Run(args, os.Stdout, os.Stderr))
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

type stage1BlockingCheckpointObserver struct {
	tableCheckpointObserver
	readyPath   string
	proceedPath string
	commitPath  string
}

func (observer stage1BlockingCheckpointObserver) BeforeSQLiteRangeChunk(
	ctx context.Context,
	chunk migrate.SQLiteRangeChunk,
) error {
	return observer.tableCheckpointObserver.BeforeSQLiteRangeChunk(ctx, chunk)
}

func (observer stage1BlockingCheckpointObserver) BeforeSQLiteRangeAttempt(
	ctx context.Context,
	chunk migrate.SQLiteRangeChunk,
) error {
	if err := observer.tableCheckpointObserver.BeforeSQLiteRangeAttempt(ctx, chunk); err != nil {
		return err
	}
	if err := os.WriteFile(
		observer.readyPath,
		[]byte(fmt.Sprintf("%s:%d:%d", chunk.Table, chunk.Range.ID, chunk.Sequence)),
		0o600,
	); err != nil {
		return err
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(observer.proceedPath); err == nil {
			return nil
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

func (observer stage1BlockingCheckpointObserver) AfterSQLiteRangeChunk(
	ctx context.Context,
	chunk migrate.SQLiteRangeChunk,
	receipt migrate.WriteReceipt,
	_ migrate.AckFrontier,
) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if durableRows := receipt.AcknowledgedRows(); durableRows != int64(chunk.ChunkRows) {
		return fmt.Errorf(
			"durable target receipt rows = %d, want %d",
			durableRows,
			chunk.ChunkRows,
		)
	}
	if err := os.WriteFile(observer.commitPath, []byte(fmt.Sprintf("%d", chunk.ChunkRows)), 0o600); err != nil {
		return err
	}
	return waitForParentHardKill(ctx)
}

func stage1HelperCommand(mode, configPath, runID, readyPath, proceedPath, commitPath string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestStage1SQLiteCrashResumeHelperProcess$")
	command.Env = append(os.Environ(),
		stage1HelperModeEnv+"="+mode,
		stage1HelperConfigEnv+"="+configPath,
		stage1HelperRunIDEnv+"="+runID,
		stage1HelperReadyEnv+"="+readyPath,
		stage1HelperProceedEnv+"="+proceedPath,
		stage1HelperCommitEnv+"="+commitPath,
	)
	return command
}

func waitForStage1HelperReady(t *testing.T, readyPath string, exited <-chan error, reaped *bool, output *bytes.Buffer) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			*reaped = true
			t.Fatalf("migration helper exited before the first durable range intent: %v\n%s", err, output.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for the first durable range intent\n%s", output.String())
		case <-ticker.C:
			if _, err := os.Stat(readyPath); err == nil {
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}

func waitForStage2ChunkCommit(t *testing.T, commitPath string, exited <-chan error, reaped *bool, output *bytes.Buffer) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			*reaped = true
			t.Fatalf("migration helper exited before target commit: %v\n%s", err, output.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for the committed target page\n%s", output.String())
		case <-ticker.C:
			if _, err := os.Stat(commitPath); err == nil {
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}

func createStage1CrashSource(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
		CREATE INDEX users_payload_idx ON users(payload)
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
	highWater := totalRows + 1000
	if _, err := database.Exec(
		`INSERT INTO users (id, payload) VALUES (?, ?); DELETE FROM users WHERE id = ?`,
		highWater,
		"deleted-high-water",
		highWater,
	); err != nil {
		t.Fatal(err)
	}
}

func stage1TargetRowCount(t *testing.T, path string) int {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var rows int
	if err := database.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertStage1ExactRows(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rows, err := database.Query(`SELECT id, payload FROM users ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	nextID := 1
	for rows.Next() {
		var id int
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		if id != nextID || payload != fmt.Sprintf("payload-%04d", nextID) {
			t.Fatalf("target row %d = (%d, %q)", nextID, id, payload)
		}
		nextID++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if nextID != totalRows+1 {
		t.Fatalf("target row count = %d, want %d", nextID-1, totalRows)
	}
}

func assertStage1CrashIntegerMetadata(t *testing.T, path string, wantSequence int) {
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
		t.Fatalf("target standalone index count = %d", indexes)
	}
	var sequence int
	if err := database.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'users'`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if sequence != wantSequence {
		t.Fatalf("target sequence = %d, want deleted source high-water %d", sequence, wantSequence)
	}
}
