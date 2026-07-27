package state

import (
	"fmt"
	"time"
)

// LatestResumableForTarget selects the newest resumable run that has not been
// superseded by a successful migration to the same target.
func (store SQLiteStore) LatestResumableForTarget(target string) (Run, bool, error) {
	runs, err := store.List()
	if err != nil {
		return Run{}, false, err
	}
	for index := len(runs) - 1; index >= 0; index-- {
		run := runs[index]
		if run.Target != target {
			continue
		}
		if run.Outcome == Success {
			return Run{}, false, nil
		}
		if run.Resumable && (run.Outcome == Running || run.Outcome == Failed) {
			return run, true, nil
		}
	}
	return Run{}, false, nil
}

// UpdateFailure records the latest recoverable error for an existing run.
func (store SQLiteStore) UpdateFailure(runID, reason string, endedAt time.Time) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	result, err := database.Exec(`
		UPDATE runs SET reason = ?, ended_at = ?, resumable = 1
		WHERE id = ? AND outcome = ?
	`, reason, endedAt.UTC(), runID, Failed)
	if err != nil {
		return fmt.Errorf("update failed run state: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify failed run state: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("update failed run state: unknown run %q", runID)
	}
	return nil
}
