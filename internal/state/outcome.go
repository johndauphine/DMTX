package state

const (
	// Cancelled is an interrupted attempt with restartable durable progress.
	Cancelled Outcome = "cancelled"
	// Partial records truthful incomplete work. Resumable distinguishes an
	// ordinary partial failure from an explicitly accepted partial outcome.
	Partial Outcome = "partial"
)

func resumableOutcome(outcome Outcome) bool {
	switch outcome {
	case Running, Failed, Cancelled, Partial:
		return true
	default:
		return false
	}
}

func latestResumableRun(runs []Run, target string) (Run, bool) {
	var selected Run
	var found bool
	for _, run := range runs {
		if run.Target != target {
			continue
		}
		if run.Outcome == Success {
			selected, found = Run{}, false
			continue
		}
		if run.Resumable && resumableOutcome(run.Outcome) {
			selected, found = run, true
		}
	}
	return selected, found
}
