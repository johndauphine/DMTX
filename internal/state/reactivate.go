package state

import (
	"fmt"
	"strings"
)

func validateReactivation(runID, reason string) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("reactivate run requires run ID and reason")
	}
	return nil
}

func ensureResumableTransition(run Run, operation string) error {
	if run.Outcome == Success {
		return fmt.Errorf("%s: run %q is already successful", operation, run.ID)
	}
	if !run.Resumable {
		return fmt.Errorf("%s: run %q is not resumable", operation, run.ID)
	}
	if !resumableOutcome(run.Outcome) {
		return fmt.Errorf("%s: outcome %q is not resumable", operation, run.Outcome)
	}
	return nil
}

func latestRunIndex(runs []Run, runID string) int {
	latest := -1
	for index := range runs {
		if runs[index].ID != runID {
			continue
		}
		if latest < 0 ||
			runs[latest].StartedAt.Before(runs[index].StartedAt) ||
			runs[latest].StartedAt.Equal(runs[index].StartedAt) && latest < index {
			latest = index
		}
	}
	return latest
}

// appendAuthoritativeRun retains one record per run outcome while making the
// supplied transition newest among records with the same StartedAt.
func appendAuthoritativeRun(runs []Run, authoritative Run) []Run {
	updated := make([]Run, 0, len(runs))
	for _, run := range runs {
		if run.ID == authoritative.ID && run.Outcome == authoritative.Outcome {
			continue
		}
		updated = append(updated, run)
	}
	return append(updated, authoritative)
}
