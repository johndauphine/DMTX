package state

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (store SQLiteStore) AbandonRun(runID, reason string, endedAt time.Time) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" || endedAt.IsZero() {
		return fmt.Errorf("abandon run requires run ID, reason, and completion time")
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin run abandonment: %w", err)
	}
	defer transaction.Rollback()

	var rowID int64
	var run Run
	var existingEnded sql.NullTime
	err = transaction.QueryRow(`
		SELECT rowid, id, source, target, outcome, resumable, reason, started_at, ended_at
		FROM runs WHERE id = ? ORDER BY started_at DESC, rowid DESC LIMIT 1
	`, runID).Scan(
		&rowID, &run.ID, &run.Source, &run.Target, &run.Outcome,
		&run.Resumable, &run.Reason, &run.StartedAt, &existingEnded,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("abandon run: unknown run %q", runID)
	}
	if err != nil {
		return fmt.Errorf("read run for abandonment: %w", err)
	}
	if run.Outcome == Success {
		return fmt.Errorf("abandon run: successful run %q cannot be abandoned", runID)
	}
	if !resumableOutcome(run.Outcome) {
		return fmt.Errorf("abandon run: outcome %q is not abandonable", run.Outcome)
	}

	if run.Outcome == Partial {
		result, err := transaction.Exec(`
			UPDATE runs SET resumable = 0, reason = ?, ended_at = ? WHERE rowid = ?
		`, reason, endedAt.UTC(), rowID)
		if err != nil {
			return fmt.Errorf("abandon partial run: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return fmt.Errorf("abandon partial run: state changed concurrently")
		}
	} else {
		if _, err := transaction.Exec(`
			INSERT INTO runs (id, source, target, outcome, resumable, reason, started_at, ended_at)
			VALUES (?, ?, ?, ?, 0, ?, ?, ?)
			ON CONFLICT(id, outcome) DO UPDATE SET
				source = excluded.source,
				target = excluded.target,
				resumable = 0,
				reason = excluded.reason,
				started_at = excluded.started_at,
				ended_at = excluded.ended_at
		`, run.ID, run.Source, run.Target, Failed, reason, run.StartedAt.UTC(), endedAt.UTC()); err != nil {
			return fmt.Errorf("record abandoned run: %w", err)
		}
		if run.Outcome != Failed {
			if _, err := transaction.Exec(`DELETE FROM runs WHERE rowid = ?`, rowID); err != nil {
				return fmt.Errorf("remove superseded run outcome: %w", err)
			}
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit run abandonment: %w", err)
	}
	return nil
}
