package state

import (
	"errors"
	"fmt"
	"strings"
)

func validateRunLeaseBinding(runID string, lease Lease) error {
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("bind target lease requires run ID")
	}
	if lease.RunID != runID {
		return fmt.Errorf(
			"bind target lease run %q does not match lease run %q",
			runID,
			lease.RunID,
		)
	}
	run := Run{
		ID:              runID,
		LeaseTarget:     lease.Target,
		LeaseOwnerToken: lease.OwnerToken,
		LeaseGeneration: lease.Generation,
	}
	return validateRunLeaseEvidence(run)
}

func sameRunLeaseEvidence(left, right Run) bool {
	return left.LeaseTarget == right.LeaseTarget &&
		left.LeaseOwnerToken == right.LeaseOwnerToken &&
		left.LeaseGeneration == right.LeaseGeneration
}

func runWithBoundLease(run Run, lease Lease) (Run, error) {
	if err := validateRunLeaseBinding(run.ID, lease); err != nil {
		return Run{}, err
	}
	if run.LeaseTarget != "" ||
		run.LeaseOwnerToken != "" ||
		run.LeaseGeneration != 0 {
		existing, err := run.BoundLease()
		if err != nil {
			return Run{}, err
		}
		if !sameLease(existing, lease) {
			return Run{}, fmt.Errorf(
				"run %q initial target lease does not match current owner",
				run.ID,
			)
		}
	}
	run.LeaseTarget = lease.Target
	run.LeaseOwnerToken = lease.OwnerToken
	run.LeaseGeneration = lease.Generation
	if err := validateRunRecord(run); err != nil {
		return Run{}, err
	}
	return run, nil
}

func validateRunLeaseRebind(current Run, next Lease) error {
	if err := validateRunLeaseBinding(current.ID, next); err != nil {
		return err
	}
	existing, err := current.BoundLease()
	if errors.Is(err, ErrRunLeaseEvidenceUnavailable) {
		return nil
	}
	if err != nil {
		return err
	}
	if existing.Target == next.Target &&
		existing.OwnerToken == next.OwnerToken &&
		existing.Generation == next.Generation {
		return nil
	}
	if next.Generation <= existing.Generation {
		return fmt.Errorf(
			"%w: run %q target lease generation cannot move from %d to %d",
			ErrImmutableEvidence,
			current.ID,
			existing.Generation,
			next.Generation,
		)
	}
	if next.OwnerToken == existing.OwnerToken {
		return fmt.Errorf(
			"%w: run %q target lease rebind requires a new owner token",
			ErrImmutableEvidence,
			current.ID,
		)
	}
	return nil
}

// BindRunLease atomically binds every lifecycle record for one logical run to
// the currently acquired target lease. Ordinary transitions cannot change this
// tuple; resume and takeover must use this explicit operation.
func (store SQLiteStore) BindRunLease(runID string, lease Lease) error {
	if err := validateRunLeaseBinding(runID, lease); err != nil {
		return err
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin target lease binding: %w", err)
	}
	defer transaction.Rollback()

	rows, err := transaction.Query(`
		SELECT lease_target, lease_owner_token, lease_generation
		FROM runs WHERE id = ? ORDER BY started_at, rowid
	`, runID)
	if err != nil {
		return fmt.Errorf("read target lease binding: %w", err)
	}
	var current Run
	found := false
	for rows.Next() {
		candidate := Run{ID: runID}
		if err := rows.Scan(
			&candidate.LeaseTarget,
			&candidate.LeaseOwnerToken,
			&candidate.LeaseGeneration,
		); err != nil {
			rows.Close()
			return fmt.Errorf("decode target lease binding: %w", err)
		}
		if err := validateRunLeaseEvidence(candidate); err != nil {
			rows.Close()
			return fmt.Errorf("decode target lease binding: %w", err)
		}
		if found && !sameRunLeaseEvidence(current, candidate) {
			rows.Close()
			return fmt.Errorf(
				"%w: run %q has inconsistent target lease evidence",
				ErrImmutableEvidence,
				runID,
			)
		}
		current = candidate
		found = true
	}
	if err := finishSQLiteRows(
		rows,
		"iterate target lease binding",
		"close target lease binding query",
	); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	if err := validateRunLeaseRebind(current, lease); err != nil {
		return err
	}
	if _, err := transaction.Exec(`
		UPDATE runs
		SET lease_target = ?, lease_owner_token = ?, lease_generation = ?
		WHERE id = ?
	`, lease.Target, lease.OwnerToken, lease.Generation, runID); err != nil {
		return fmt.Errorf("bind target lease: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit target lease binding: %w", err)
	}
	return nil
}

// BindRunLease atomically replaces the bound lease tuple in the YAML state
// document while holding its exclusive compare-and-write lock.
func (store YAMLStore) BindRunLease(runID string, lease Lease) error {
	if err := validateRunLeaseBinding(runID, lease); err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		var current Run
		found := false
		for _, candidate := range document.Runs {
			if candidate.ID != runID {
				continue
			}
			if found && !sameRunLeaseEvidence(current, candidate) {
				return fmt.Errorf(
					"%w: run %q has inconsistent target lease evidence",
					ErrImmutableEvidence,
					runID,
				)
			}
			current = candidate
			found = true
		}
		if !found {
			return fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
		}
		if err := validateRunLeaseRebind(current, lease); err != nil {
			return err
		}
		for index := range document.Runs {
			if document.Runs[index].ID != runID {
				continue
			}
			document.Runs[index].LeaseTarget = lease.Target
			document.Runs[index].LeaseOwnerToken = lease.OwnerToken
			document.Runs[index].LeaseGeneration = lease.Generation
		}
		return nil
	})
}
