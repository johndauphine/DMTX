package state

import (
	"fmt"
	"time"
)

func (store YAMLStore) UpdateRecoverableOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	if err := validateRecoverableOutcome(runID, outcome, reason, endedAt); err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		latest := latestRunIndex(document.Runs, runID)
		if latest < 0 {
			return fmt.Errorf("update recoverable run state: unknown run %q", runID)
		}
		if err := ensureResumableTransition(
			document.Runs[latest],
			"update recoverable run state",
		); err != nil {
			return err
		}
		run := document.Runs[latest]
		run.Outcome = outcome
		run.Resumable = true
		run.Reason = reason
		run.EndedAt = endedAt.UTC()
		document.Runs = appendAuthoritativeRun(document.Runs, run)
		return nil
	})
}
