package app

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

type stage2TupleRow struct {
	tenant   string
	sequence int64
	payload  string
}

func TestStage2SQLiteTupleKeysetWriteBeforeAckResumesExactRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	eventPath := filepath.Join(directory, "target-committed")
	runID := "stage2-tuple-write-before-ack"
	rows := stage2TupleRows()

	createStage2TupleSource(t, sourcePath, rows)
	configuration := fmt.Sprintf(
		"source:\n  type: sqlite\n  database: %s\n"+
			"target:\n  type: sqlite\n  database: %s\n"+
			"migration:\n  target_mode: upsert\n  chunk_size: 4\n  partitions: 1\n",
		sourcePath,
		targetPath,
	)
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	initializeStage1Run(t, store, cfg, runID)

	command := stage2RangeAckHelperCommand(configPath, runID, eventPath)
	var commandOutput bytes.Buffer
	command.Stdout = &commandOutput
	command.Stderr = &commandOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		_ = command.Process.Kill()
		<-exited
	})

	waitForStage2ChunkCommit(t, eventPath, exited, &reaped, &commandOutput)
	assertStage2TuplePendingIntent(t, store, runID)
	assertStage2TupleTargetRows(t, targetPath, rows[:4])

	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill tuple migration helper: %v; output: %s", err, commandOutput.String())
	}
	if err := <-exited; err == nil {
		t.Fatalf("tuple migration helper exited successfully instead of being killed; output: %s", commandOutput.String())
	}
	reaped = true

	assertStage2TuplePendingIntent(t, store, runID)
	assertStage2TupleTargetRows(t, targetPath, rows[:4])

	resumeStage1Fixture(t, configPath, runID, len(rows))

	assertStage2TupleTargetRows(t, targetPath, rows)
	assertStage2TupleCompletedState(t, store, runID, int64(len(rows)))
	latest, found, err := store.Latest()
	if err != nil || !found || latest.ID != runID ||
		latest.Outcome != state.Success || latest.Resumable {
		t.Fatalf("latest run = %#v, found = %v, error = %v", latest, found, err)
	}
}

func stage2TupleRows() []stage2TupleRow {
	return []stage2TupleRow{
		{tenant: "alpha", sequence: -9, payload: "alpha-negative"},
		{tenant: "alpha", sequence: 0, payload: "alpha-zero"},
		{tenant: "alpha", sequence: 1 << 53, payload: "alpha-two-to-the-53"},
		{tenant: "alpha", sequence: 1<<53 + 1, payload: "alpha-above-two-to-the-53"},
		{tenant: "alpha", sequence: math.MaxInt64, payload: "alpha-max-int64"},
		{tenant: "beta", sequence: math.MinInt64, payload: "beta-min-int64"},
		{tenant: "beta", sequence: -1, payload: "beta-negative"},
		{tenant: "beta", sequence: 7, payload: "beta-seven"},
		{tenant: "omega", sequence: 42, payload: "omega-forty-two"},
	}
}

func createStage2TupleSource(t *testing.T, path string, expected []stage2TupleRow) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE events (
			tenant TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, sequence)
		) WITHOUT ROWID
	`); err != nil {
		t.Fatal(err)
	}
	transaction, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	statement, err := transaction.Prepare(
		`INSERT INTO events (tenant, sequence, payload) VALUES (?, ?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := len(expected) - 1; index >= 0; index-- {
		row := expected[index]
		if _, err := statement.Exec(row.tenant, row.sequence, row.payload); err != nil {
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

func assertStage2TuplePendingIntent(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tuple tasks after write-before-ack interruption = %#v", tasks)
	}
	task := tasks[0]
	if task.Key != sqliteRangeTaskKey("events") ||
		task.Status != "running" ||
		task.Strategy != string(migrate.PaginationTupleKeyset) ||
		task.TopologyHash == "" ||
		task.Attempts != 1 ||
		task.Retries != 0 {
		t.Fatalf("tuple task after write-before-ack interruption = %#v", task)
	}
	if len(ranges) != 1 {
		t.Fatalf("tuple ranges after write-before-ack interruption = %#v", ranges)
	}
	workRange := ranges[0]
	expectedUpper := state.TypedTuple{
		state.TextValue("omega"),
		state.Int64Value(42),
	}
	expectedIntentFrontier := state.TypedTuple{
		state.TextValue("alpha"),
		state.Int64Value(1<<53 + 1),
	}
	if workRange.Task != task.Key ||
		workRange.ID != "0" ||
		workRange.Strategy != string(migrate.PaginationTupleKeyset) ||
		workRange.TopologyHash != task.TopologyHash ||
		len(workRange.Lower) != 0 ||
		!stage2TypedTupleEqual(workRange.Upper, expectedUpper) ||
		workRange.LowerInclusive ||
		!workRange.UpperInclusive ||
		workRange.Status != "running" ||
		workRange.FrontierValid ||
		workRange.NextSequence != 0 ||
		workRange.SequenceOffset != 0 ||
		workRange.RowsDone != 0 ||
		workRange.Attempts != 1 ||
		workRange.Retries != 0 ||
		len(workRange.Pending) != 1 {
		t.Fatalf("tuple range after write-before-ack interruption = %#v", workRange)
	}
	pending := workRange.Pending[0]
	if pending.Sequence != 0 ||
		pending.ChunkRows != 4 ||
		pending.DurableRows != 0 ||
		pending.Attempts != 1 ||
		!pending.FrontierValid ||
		!stage2TypedTupleEqual(pending.Frontier, expectedIntentFrontier) {
		t.Fatalf("tuple pending intent after write-before-ack interruption = %#v", pending)
	}
}

func assertStage2TupleCompletedState(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	expectedRows int64,
) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("completed tuple tasks = %#v", tasks)
	}
	task := tasks[0]
	if task.Status != "completed" ||
		task.Strategy != string(migrate.PaginationTupleKeyset) ||
		task.Attempts != 4 ||
		task.Retries != 1 {
		t.Fatalf("completed tuple task = %#v", task)
	}
	if len(ranges) != 1 {
		t.Fatalf("completed tuple ranges = %#v", ranges)
	}
	workRange := ranges[0]
	expectedFrontier := state.TypedTuple{
		state.TextValue("omega"),
		state.Int64Value(42),
	}
	if workRange.Status != "completed" ||
		workRange.RowsDone != expectedRows ||
		workRange.NextSequence != 3 ||
		workRange.SequenceOffset != 0 ||
		workRange.Attempts != 4 ||
		workRange.Retries != 1 ||
		!workRange.FrontierValid ||
		!stage2TypedTupleEqual(workRange.Frontier, expectedFrontier) ||
		len(workRange.Pending) != 0 {
		t.Fatalf("completed tuple range = %#v", workRange)
	}
}

func assertStage2TupleTargetRows(
	t *testing.T,
	path string,
	expected []stage2TupleRow,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	result, err := database.Query(
		`SELECT tenant, sequence, payload FROM events ORDER BY tenant, sequence`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	index := 0
	for result.Next() {
		if index >= len(expected) {
			t.Fatalf("target contains unexpected tuple row after %d expected rows", len(expected))
		}
		var actual stage2TupleRow
		if err := result.Scan(&actual.tenant, &actual.sequence, &actual.payload); err != nil {
			t.Fatal(err)
		}
		if actual != expected[index] {
			t.Fatalf("target tuple row %d = %#v, want %#v", index, actual, expected[index])
		}
		index++
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	if index != len(expected) {
		t.Fatalf("target tuple row count = %d, want %d", index, len(expected))
	}
}

func stage2TypedTupleEqual(left, right state.TypedTuple) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
