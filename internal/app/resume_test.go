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

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
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
	hash, err := config.Hash(config.Config{Source: config.Endpoint{Type: "sqlite", Database: sourcePath}, Target: config.Endpoint{Type: "sqlite", Database: targetPath}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveConfigHash("run-1", hash); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(state.Task{RunID: "run-1", Table: "users", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTask("run-1", "users", 1, started.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	var output, errors bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath, "--acknowledge-destructive"}, &output, &errors); code != Success {
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

func TestResumeCompletesAfterDurableRangeIntentInterruption(t *testing.T) {
	directory := t.TempDir()
	sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createResumableDatabase(t, sourcePath)
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	for id := 2; id <= 501; id++ {
		if _, err := source.Exec(`INSERT INTO users VALUES (?)`, id); err != nil {
			t.Fatal(err)
		}
	}
	source.Close()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\nmigration:\n  target_mode: upsert\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	started := time.Now().UTC()
	if err := store.Append(state.Run{ID: "interrupted", Source: sourcePath, Target: targetPath, Outcome: state.Failed, Resumable: true, Reason: "injected interruption", StartedAt: started, EndedAt: started}); err != nil {
		t.Fatal(err)
	}
	hash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveConfigHash("interrupted", hash); err != nil {
		t.Fatal(err)
	}
	observer := failBeforeDurableRangeAcknowledgement{
		tableCheckpointObserver: tableCheckpointObserver{
			store: store,
			runID: "interrupted",
		},
	}
	if _, err := migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, observer); err == nil {
		t.Fatal("expected interruption before durable range acknowledgement")
	}
	assertStage2PendingRangeIntent(t, store, "interrupted", 500)
	if rows := stage1TargetRowCount(t, targetPath); rows != 500 {
		t.Fatalf("committed target rows before resume = %d, want 500", rows)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("resume code = %d, stderr = %s", code, errors.String())
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var rows, distinct int
	if err := target.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT id) FROM users`).Scan(&rows, &distinct); err != nil {
		t.Fatal(err)
	}
	if rows != 501 || distinct != 501 {
		t.Fatalf("target rows = %d, distinct = %d", rows, distinct)
	}
	assertStage2CompletedRange(t, store, "interrupted", 501)
}

type failBeforeDurableRangeAcknowledgement struct{ tableCheckpointObserver }

func (observer failBeforeDurableRangeAcknowledgement) AfterSQLiteRangeChunk(
	context.Context,
	migrate.SQLiteRangeChunk,
	migrate.WriteReceipt,
	migrate.AckFrontier,
) error {
	return fmt.Errorf("injected interruption before durable range acknowledgement")
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
