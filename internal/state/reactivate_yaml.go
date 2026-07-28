package state

import (
	"fmt"
	"time"
)

// ReactivateRun records that a resumable run is actively executing again.
func (store YAMLStore) ReactivateRun(runID, reason string) error {
	if err := validateReactivation(runID, reason); err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		latest := latestRunIndex(document.Runs, runID)
		if latest < 0 {
			return fmt.Errorf("reactivate run: unknown run %q", runID)
		}
		run := document.Runs[latest]
		if err := ensureResumableTransition(run, "reactivate run"); err != nil {
			return err
		}
		run.Outcome = Running
		run.Resumable = true
		run.Reason = reason
		run.EndedAt = time.Time{}
		document.Runs = appendAuthoritativeRun(document.Runs, run)
		return nil
	})
}
