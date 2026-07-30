package state

import (
	"database/sql"
	"errors"
	"fmt"
)

// ReactivateRun records that a resumable run is actively executing again.
func (store SQLiteStore) ReactivateRun(runID, reason string) error {
	if err := validateReactivation(runID, reason); err != nil {
		return err
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin run reactivation: %w", err)
	}
	defer transaction.Rollback()

	var run Run
	err = transaction.QueryRow(`
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, started_at
		FROM runs WHERE id = ? ORDER BY started_at DESC, rowid DESC LIMIT 1
	`, runID).Scan(
		&run.ID,
		&run.Source,
		&run.Target,
		&run.SourceEngine,
		&run.SourceIdentity,
		&run.TargetIdentity,
		&run.LeaseTarget,
		&run.LeaseOwnerToken,
		&run.LeaseGeneration,
		&run.Outcome,
		&run.Resumable,
		&run.StartedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("reactivate run: unknown run %q", runID)
	}
	if err != nil {
		return fmt.Errorf("read run for reactivation: %w", err)
	}
	if err := validateRunRecord(run); err != nil {
		return fmt.Errorf("read run for reactivation: %w", err)
	}
	if err := ensureResumableTransition(run, "reactivate run"); err != nil {
		return err
	}
	if _, err := transaction.Exec(`
		DELETE FROM runs WHERE id = ? AND outcome = ?
	`, runID, Running); err != nil {
		return fmt.Errorf("replace running run state: %w", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO runs (
			id, source, target, source_engine, source_identity, target_identity,
			lease_target, lease_owner_token, lease_generation,
			outcome, resumable, reason, started_at, ended_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL)
	`, run.ID, run.Source, run.Target, run.SourceEngine, run.SourceIdentity, run.TargetIdentity,
		run.LeaseTarget, run.LeaseOwnerToken, run.LeaseGeneration,
		Running, reason, run.StartedAt.UTC()); err != nil {
		return fmt.Errorf("record running run state: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit run reactivation: %w", err)
	}
	return nil
}
