package state

import "time"

// LatestResumableForTarget selects the newest resumable run that has not been
// superseded by a successful migration to the same target.
func (store SQLiteStore) LatestResumableForTarget(target string) (Run, bool, error) {
	runs, err := store.List()
	if err != nil {
		return Run{}, false, err
	}
	run, found := latestResumableRun(runs, target)
	return run, found, nil
}

// UpdateFailure records the latest recoverable error for an existing run.
func (store SQLiteStore) UpdateFailure(runID, reason string, endedAt time.Time) error {
	return store.UpdateRecoverableOutcome(runID, Failed, reason, endedAt)
}
