package state

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (store SQLiteStore) UpdateRecoverableOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	if err := validateRecoverableOutcome(runID, outcome, reason, endedAt); err != nil {
		return err
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin recoverable run state: %w", err)
	}
	defer transaction.Rollback()

	var source, target string
	var startedAt time.Time
	var latest Outcome
	var resumable bool
	err = transaction.QueryRow(`
		SELECT source, target, started_at, outcome, resumable
		FROM runs WHERE id = ? ORDER BY started_at DESC, rowid DESC LIMIT 1
	`, runID).Scan(&source, &target, &startedAt, &latest, &resumable)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("update recoverable run state: unknown run %q", runID)
	}
	if err != nil {
		return fmt.Errorf("read run for recoverable state: %w", err)
	}
	if err := ensureResumableTransition(Run{
		ID: runID, Outcome: latest, Resumable: resumable,
	}, "update recoverable run state"); err != nil {
		return err
	}
	// Reinsert the one record for this outcome so its rowid represents the
	// authoritative current transition without changing the migration start.
	if _, err := transaction.Exec(`
		DELETE FROM runs WHERE id = ? AND outcome = ?
	`, runID, outcome); err != nil {
		return fmt.Errorf("replace recoverable run state: %w", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO runs (id, source, target, outcome, resumable, reason, started_at, ended_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, runID, source, target, outcome, reason, startedAt.UTC(), endedAt.UTC()); err != nil {
		return fmt.Errorf("record recoverable run state: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit recoverable run state: %w", err)
	}
	return nil
}
