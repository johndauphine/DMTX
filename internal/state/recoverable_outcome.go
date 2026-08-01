package state

import (
	"fmt"
	"strings"
	"time"
)

func validateRecoverableOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	if err := validateTerminalAttemptOutcome(runID, outcome, reason, endedAt); err != nil {
		return err
	}
	return nil
}

func validateTerminalAttemptOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" || endedAt.IsZero() {
		return fmt.Errorf("terminal outcome requires run ID, reason, and completion time")
	}
	switch outcome {
	case Failed, Cancelled, Partial:
		return nil
	default:
		return fmt.Errorf("outcome %q is not a terminal attempt", outcome)
	}
}
