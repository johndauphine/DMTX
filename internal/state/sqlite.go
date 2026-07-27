package state

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore persists durable, queryable migration state locally.
type SQLiteStore struct {
	Path string
}

// Task is a durable table-level migration checkpoint.
type Task struct {
	RunID              string    `json:"run_id"`
	Table              string    `json:"table"`
	Status             string    `json:"status"`
	RowsDone           int       `json:"rows_done"`
	IntegerWatermark   *int64    `json:"integer_watermark,omitempty"`
	RowNumberWatermark *int64    `json:"row_number_watermark,omitempty"`
	StartedAt          time.Time `json:"started_at"`
	CompletedAt        time.Time `json:"completed_at,omitempty"`
}

// AdvanceIntegerKeysetTask records a target-acknowledged page frontier.
func (store SQLiteStore) AdvanceIntegerKeysetTask(runID, table string, rowsDone int, watermark int64) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Exec(`UPDATE tasks SET rows_done = ?, integer_watermark = ? WHERE run_id = ? AND table_name = ? AND status = 'running'`, rowsDone, watermark, runID, table)
	if err != nil {
		return fmt.Errorf("advance table checkpoint: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify table checkpoint: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("advance table checkpoint: unknown or non-running task %q", table)
	}
	return nil
}

// AdvanceRowNumberTask records a target-acknowledged row-number frontier.
func (store SQLiteStore) AdvanceRowNumberTask(runID, table string, rowsDone int, watermark int64) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Exec(`UPDATE tasks SET rows_done = ?, row_number_watermark = ? WHERE run_id = ? AND table_name = ? AND status = 'running'`, rowsDone, watermark, runID, table)
	if err != nil {
		return fmt.Errorf("advance table checkpoint: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify table checkpoint: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("advance table checkpoint: unknown or non-running task %q", table)
	}
	return nil
}

// Append records a state transition for a migration run.
func (store SQLiteStore) Append(run Run) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	_, err = database.Exec(`
		INSERT INTO runs (id, source, target, outcome, resumable, reason, started_at, ended_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, run.ID, run.Source, run.Target, run.Outcome, run.Resumable, run.Reason, run.StartedAt.UTC(), nullableTime(run.EndedAt))
	if err != nil {
		return fmt.Errorf("record run state: %w", err)
	}
	return nil
}

// CreateTask writes a table checkpoint before its target mutation begins.
func (store SQLiteStore) CreateTask(task Task) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	_, err = database.Exec(`
		INSERT INTO tasks (run_id, table_name, status, rows_done, started_at, completed_at)
		VALUES (?, ?, 'running', 0, ?, NULL)
	`, task.RunID, task.Table, task.StartedAt.UTC())
	if err != nil {
		return fmt.Errorf("create table checkpoint: %w", err)
	}
	return nil
}

// CompleteTask records the validated completion frontier for a table.
func (store SQLiteStore) CompleteTask(runID, table string, rowsDone int, completedAt time.Time) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	result, err := database.Exec(`
		UPDATE tasks
		SET status = 'completed', rows_done = ?, completed_at = ?
		WHERE run_id = ? AND table_name = ? AND status = 'running'
	`, rowsDone, completedAt.UTC(), runID, table)
	if err != nil {
		return fmt.Errorf("complete table checkpoint: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify table checkpoint: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("complete table checkpoint: unknown or non-running task %q", table)
	}
	return nil
}

// ListTasks returns a run's table checkpoints in deterministic table order.
func (store SQLiteStore) ListTasks(runID string) ([]Task, error) {
	database, err := store.Open()
	if err != nil {
		return nil, err
	}
	defer database.Close()

	rows, err := database.Query(`
		SELECT run_id, table_name, status, rows_done, integer_watermark, row_number_watermark, started_at, completed_at
		FROM tasks WHERE run_id = ? ORDER BY table_name
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list table checkpoints: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		var completedAt sql.NullTime
		var watermark sql.NullInt64
		var rowNumberWatermark sql.NullInt64
		if err := rows.Scan(&task.RunID, &task.Table, &task.Status, &task.RowsDone, &watermark, &rowNumberWatermark, &task.StartedAt, &completedAt); err != nil {
			return nil, fmt.Errorf("read table checkpoint: %w", err)
		}
		if completedAt.Valid {
			task.CompletedAt = completedAt.Time
		}
		if watermark.Valid {
			value := watermark.Int64
			task.IntegerWatermark = &value
		}
		if rowNumberWatermark.Valid {
			value := rowNumberWatermark.Int64
			task.RowNumberWatermark = &value
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table checkpoints: %w", err)
	}
	return tasks, nil
}

// Latest returns the most recently recorded run state.
func (store SQLiteStore) Latest() (Run, bool, error) {
	database, err := store.Open()
	if err != nil {
		return Run{}, false, err
	}
	defer database.Close()
	row := database.QueryRow(`SELECT id, source, target, outcome, resumable, reason, started_at, ended_at FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1`)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, false, nil
	}
	if err != nil {
		return Run{}, false, fmt.Errorf("read latest run state: %w", err)
	}
	return run, true, nil
}

// List returns migration runs in chronological order.
func (store SQLiteStore) List() ([]Run, error) {
	database, err := store.Open()
	if err != nil {
		return nil, err
	}
	defer database.Close()
	rows, err := database.Query(`SELECT id, source, target, outcome, resumable, reason, started_at, ended_at FROM runs ORDER BY started_at, rowid`)
	if err != nil {
		return nil, fmt.Errorf("list run state: %w", err)
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("read run state: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run state: %w", err)
	}
	return runs, nil
}

// Open initializes the local state database and returns a connection.
func (store SQLiteStore) Open() (*sql.DB, error) {
	if store.Path == "" {
		return nil, errors.New("state database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(store.Path), 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	database, err := sql.Open("sqlite", store.Path)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	if _, err := database.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		database.Close()
		return nil, fmt.Errorf("configure state database: %w", err)
	}
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS runs (
			id TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL, outcome TEXT NOT NULL,
			resumable INTEGER NOT NULL, reason TEXT NOT NULL, started_at DATETIME NOT NULL,
			ended_at DATETIME, PRIMARY KEY (id, outcome)
		);
		CREATE TABLE IF NOT EXISTS tasks (
			run_id TEXT NOT NULL, table_name TEXT NOT NULL, status TEXT NOT NULL,
			rows_done INTEGER NOT NULL, integer_watermark INTEGER, row_number_watermark INTEGER, started_at DATETIME NOT NULL, completed_at DATETIME,
			PRIMARY KEY (run_id, table_name)
		);
	`); err != nil {
		database.Close()
		return nil, fmt.Errorf("initialize state database: %w", err)
	}
	if _, err := database.Exec(`ALTER TABLE tasks ADD COLUMN integer_watermark INTEGER`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		database.Close()
		return nil, fmt.Errorf("upgrade task checkpoints: %w", err)
	}
	if _, err := database.Exec(`ALTER TABLE tasks ADD COLUMN row_number_watermark INTEGER`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		database.Close()
		return nil, fmt.Errorf("upgrade row-number checkpoints: %w", err)
	}
	return database, nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanRun(scanner rowScanner) (Run, error) {
	var run Run
	var endedAt sql.NullTime
	if err := scanner.Scan(&run.ID, &run.Source, &run.Target, &run.Outcome, &run.Resumable, &run.Reason, &run.StartedAt, &endedAt); err != nil {
		return Run{}, err
	}
	if endedAt.Valid {
		run.EndedAt = endedAt.Time
	}
	return run, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
