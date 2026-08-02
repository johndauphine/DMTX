package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

const (
	stage1WriteBoundaryHelperEnv     = "DMTX_STAGE1_WRITE_BOUNDARY_HELPER"
	stage1WriteBoundaryConfigEnv     = "DMTX_STAGE1_WRITE_BOUNDARY_CONFIG"
	stage1WriteBoundaryRunIDEnv      = "DMTX_STAGE1_WRITE_BOUNDARY_RUN_ID"
	stage1WriteBoundaryNameEnv       = "DMTX_STAGE1_WRITE_BOUNDARY_NAME"
	stage1WriteBoundaryOccurrenceEnv = "DMTX_STAGE1_WRITE_BOUNDARY_OCCURRENCE"
	stage1WriteBoundaryEventEnv      = "DMTX_STAGE1_WRITE_BOUNDARY_EVENT"
)

func TestStage1SQLiteHardKillAtRemainingWriteBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		boundary    migrate.SQLiteWriteBoundary
		targetMode  string
		prepopulate bool
	}{
		{
			name:        "durable table set before first mutation",
			boundary:    migrate.SQLiteBoundaryTableSetCheckpoint,
			targetMode:  "drop_recreate",
			prepopulate: true,
		},
		{
			name:        "drop committed",
			boundary:    migrate.SQLiteBoundaryTableDropped,
			targetMode:  "drop_recreate",
			prepopulate: true,
		},
		{
			name:        "create table committed",
			boundary:    migrate.SQLiteBoundaryTableCreated,
			targetMode:  "drop_recreate",
			prepopulate: true,
		},
		{
			name:       "durable page checkpoint before next write",
			boundary:   migrate.SQLiteBoundaryPageCheckpointed,
			targetMode: "upsert",
		},
		{
			name:       "between standalone indexes",
			boundary:   migrate.SQLiteBoundaryIndexCreated,
			targetMode: "upsert",
		},
		{
			name:       "sequence reset committed",
			boundary:   migrate.SQLiteBoundarySequenceCommitted,
			targetMode: "upsert",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const totalRows = 1253
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := configPath + ".state.db"
			runID := "stage1-write-boundary-" + strings.ReplaceAll(test.name, " ", "-")

			createStage1WriteBoundarySource(t, sourcePath, totalRows)
			if test.prepopulate {
				createStage1WriteBoundaryTarget(t, targetPath)
			}
			cfg := writeStage1WriteBoundaryConfig(t, configPath, sourcePath, targetPath, test.targetMode)
			store := state.SQLiteStore{Path: statePath}
			initializeStage1Run(t, store, cfg, runID)

			eventPath := filepath.Join(directory, "write-boundary-reached")
			child := startStage1Child(t, stage1WriteBoundaryHelperCommand(
				configPath,
				runID,
				test.boundary,
				1,
				eventPath,
			))
			child.waitForFile(t, eventPath, string(test.boundary))
			assertStage1WriteBoundaryState(t, store, targetPath, runID, test.boundary, totalRows)
			child.kill(t)

			var stdout, stderr bytes.Buffer
			args := []string{"resume", "--config", configPath}
			if test.targetMode == "drop_recreate" {
				args = append(args, "--acknowledge-destructive")
			}
			if code := Run(args, &stdout, &stderr); code != Success {
				t.Fatalf("resume exit code = %d, stderr = %s", code, stderr.String())
			}
			expected := fmt.Sprintf(`{"tables":1,"rows":%d,"validated":true}`, totalRows)
			if !strings.Contains(stdout.String(), expected) {
				t.Fatalf("resume output = %q", stdout.String())
			}
			assertStage1ExactRows(t, targetPath, totalRows)
			assertStage1WriteBoundarySchema(t, targetPath, totalRows+1000)
			assertStage1CompletedRun(t, store, runID, totalRows)
		})
	}
}

func TestStage1SQLiteWriteBoundaryHelperProcess(t *testing.T) {
	if os.Getenv(stage1WriteBoundaryHelperEnv) != "1" {
		return
	}
	configPath := os.Getenv(stage1WriteBoundaryConfigEnv)
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Migration.DestructiveAcknowledged = true
	occurrence, err := strconv.Atoi(os.Getenv(stage1WriteBoundaryOccurrenceEnv))
	if err != nil || occurrence < 1 {
		t.Fatalf("invalid boundary occurrence: %q", os.Getenv(stage1WriteBoundaryOccurrenceEnv))
	}
	observer := &stage1WriteBoundaryObserver{
		tableCheckpointObserver: tableCheckpointObserver{
			store: state.SQLiteStore{Path: configPath + ".state.db"},
			runID: os.Getenv(stage1WriteBoundaryRunIDEnv),
		},
		boundary:   migrate.SQLiteWriteBoundary(os.Getenv(stage1WriteBoundaryNameEnv)),
		occurrence: occurrence,
		eventPath:  os.Getenv(stage1WriteBoundaryEventEnv),
	}
	if _, err := migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, observer); err != nil {
		t.Fatalf("migration returned before hard kill: %v", err)
	}
	t.Fatal("migration completed before hard kill")
}

type stage1WriteBoundaryObserver struct {
	tableCheckpointObserver
	boundary   migrate.SQLiteWriteBoundary
	occurrence int
	seen       int
	eventPath  string
}

func (observer *stage1WriteBoundaryObserver) AfterSQLiteWriteBoundary(
	ctx context.Context,
	boundary migrate.SQLiteWriteBoundary,
	_ string,
) error {
	if boundary != observer.boundary {
		return nil
	}
	observer.seen++
	if observer.seen != observer.occurrence {
		return nil
	}
	if err := os.WriteFile(observer.eventPath, []byte(boundary), 0o600); err != nil {
		return err
	}
	return waitForParentHardKill(ctx)
}

// waitForParentHardKill keeps crash-fixture helper processes alive after
// publishing their durable sentinel. A bare receive from Background().Done()
// leaves a helper with no timers or runnable goroutines, so newer Go runtimes
// terminate it as a deadlock before the parent can send SIGKILL.
func waitForParentHardKill(ctx context.Context) error {
	timer := time.NewTimer(24 * time.Hour)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("hard-kill test parent did not terminate helper")
	}
}

func stage1WriteBoundaryHelperCommand(
	configPath string,
	runID string,
	boundary migrate.SQLiteWriteBoundary,
	occurrence int,
	eventPath string,
) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestStage1SQLiteWriteBoundaryHelperProcess$")
	command.Env = append(os.Environ(),
		stage1WriteBoundaryHelperEnv+"=1",
		stage1WriteBoundaryConfigEnv+"="+configPath,
		stage1WriteBoundaryRunIDEnv+"="+runID,
		stage1WriteBoundaryNameEnv+"="+string(boundary),
		stage1WriteBoundaryOccurrenceEnv+"="+strconv.Itoa(occurrence),
		stage1WriteBoundaryEventEnv+"="+eventPath,
	)
	return command
}

func writeStage1WriteBoundaryConfig(
	t *testing.T,
	configPath string,
	sourcePath string,
	targetPath string,
	targetMode string,
) config.Config {
	t.Helper()
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
	return cfg
}

func createStage1WriteBoundarySource(t *testing.T, path string, totalRows int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
		CREATE INDEX users_payload_idx ON users(payload);
		CREATE UNIQUE INDEX users_payload_id_uidx ON users(payload, id DESC)
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

func createStage1WriteBoundaryTarget(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
		INSERT INTO users (id, payload) VALUES (999999, 'stale')
	`); err != nil {
		t.Fatal(err)
	}
}

func assertStage1WriteBoundaryState(
	t *testing.T,
	store state.Backend,
	targetPath string,
	runID string,
	boundary migrate.SQLiteWriteBoundary,
	totalRows int,
) {
	t.Helper()
	tasks, err := store.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "running" {
		t.Fatalf("boundary %s tasks = %#v", boundary, tasks)
	}
	database, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	switch boundary {
	case migrate.SQLiteBoundaryTableSetCheckpoint:
		if countStage1BoundaryRows(t, database) != 1 {
			t.Fatal("target changed before first mutation")
		}
	case migrate.SQLiteBoundaryTableDropped:
		if stage1BoundaryTableExists(t, database) {
			t.Fatal("target table still exists after durable drop")
		}
	case migrate.SQLiteBoundaryTableCreated:
		if countStage1BoundaryRows(t, database) != 0 {
			t.Fatal("new target table is not empty")
		}
	case migrate.SQLiteBoundaryPageCheckpointed:
		if tasks[0].RowsDone != 0 || tasks[0].IntegerWatermark != nil ||
			tasks[0].RowNumberWatermark != nil {
			t.Fatalf("legacy task advanced before table completion = %#v", tasks[0])
		}
		rangeBackend, ok := store.(state.RangeBackend)
		if !ok {
			t.Fatalf("state backend %T does not expose durable range progress", store)
		}
		assertStage2RangeFrontier(t, rangeBackend, runID, 500, 1, state.Int64Value(500))
		if countStage1BoundaryRows(t, database) != 500 {
			t.Fatal("durable page rows do not match checkpoint")
		}
	case migrate.SQLiteBoundaryIndexCreated:
		if countStage1BoundaryRows(t, database) != totalRows || countStage1BoundaryIndexes(t, database) != 1 {
			t.Fatal("first standalone index boundary is not durable")
		}
	case migrate.SQLiteBoundarySequenceCommitted:
		if countStage1BoundaryIndexes(t, database) != 2 || stage1BoundarySequence(t, database) != totalRows+1000 {
			t.Fatal("sequence boundary is not durable")
		}
	}
}

func assertStage1WriteBoundarySchema(t *testing.T, path string, wantSequence int) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var ddl string
	if err := database.QueryRow(`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'users'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(ddl), "AUTOINCREMENT") {
		t.Fatalf("target DDL lost AUTOINCREMENT: %s", ddl)
	}
	if indexes := countStage1BoundaryIndexes(t, database); indexes != 2 {
		t.Fatalf("target standalone indexes = %d, want 2", indexes)
	}
	if sequence := stage1BoundarySequence(t, database); sequence != wantSequence {
		t.Fatalf("target sequence = %d, want %d", sequence, wantSequence)
	}
}

func stage1BoundaryTableExists(t *testing.T, database *sql.DB) bool {
	t.Helper()
	var exists bool
	if err := database.QueryRow(`SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'users')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func countStage1BoundaryRows(t *testing.T, database *sql.DB) int {
	t.Helper()
	var rows int
	if err := database.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func countStage1BoundaryIndexes(t *testing.T, database *sql.DB) int {
	t.Helper()
	var indexes int
	if err := database.QueryRow(`
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'index' AND name IN ('users_payload_idx', 'users_payload_id_uidx')
	`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	return indexes
}

func stage1BoundarySequence(t *testing.T, database *sql.DB) int {
	t.Helper()
	var sequence int
	if err := database.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'users'`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	return sequence
}
