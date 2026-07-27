package state

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore persists durable, queryable migration state locally.
type SQLiteStore struct {
	Path string
}

// Append records a state transition for a migration run.
func (store SQLiteStore) Append(run Run) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	_, err = database.Exec(`
		INSERT INTO runs (
			id, source, target, outcome, resumable, reason, started_at, ended_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, run.ID, run.Source, run.Target, run.Outcome, run.Resumable, run.Reason, run.StartedAt.UTC(), nullableTime(run.EndedAt))
	if err != nil {
		return fmt.Errorf("record run state: %w", err)
	}
	return nil
}

// Latest returns the most recently recorded run state.
func (store SQLiteStore) Latest() (Run, bool, error) {
	database, err := store.Open()
	if err != nil {
		return Run{}, false, err
	}
	defer database.Close()

	row := database.QueryRow(`
		SELECT id, source, target, outcome, resumable, reason, started_at, ended_at
		FROM runs
		ORDER BY started_at DESC, rowid DESC
		LIMIT 1
	`)
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

	rows, err := database.Query(`
		SELECT id, source, target, outcome, resumable, reason, started_at, ended_at
		FROM runs
		ORDER BY started_at, rowid
	`)
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
			id TEXT NOT NULL,
			source TEXT NOT NULL,
			target TEXT NOT NULL,
			outcome TEXT NOT NULL,
			resumable INTEGER NOT NULL,
			reason TEXT NOT NULL,
			started_at DATETIME NOT NULL,
			ended_at DATETIME,
			PRIMARY KEY (id, outcome)
		)
	`); err != nil {
		database.Close()
		return nil, fmt.Errorf("initialize state database: %w", err)
	}
	return database, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(scanner rowScanner) (Run, error) {
	var run Run
	var endedAt sql.NullTime
	if err := scanner.Scan(
		&run.ID,
		&run.Source,
		&run.Target,
		&run.Outcome,
		&run.Resumable,
		&run.Reason,
		&run.StartedAt,
		&endedAt,
	); err != nil {
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
