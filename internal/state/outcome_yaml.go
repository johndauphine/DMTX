package state

import (
	"fmt"
	"strings"
	"time"
)

func (store YAMLStore) AbandonRun(runID, reason string, endedAt time.Time) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" || endedAt.IsZero() {
		return fmt.Errorf("abandon run requires run ID, reason, and completion time")
	}
	return store.update(func(document *yamlStateDocument) error {
		latest := -1
		for index := range document.Runs {
			if document.Runs[index].ID != runID {
				continue
			}
			if latest < 0 || document.Runs[latest].StartedAt.Before(document.Runs[index].StartedAt) ||
				document.Runs[latest].StartedAt.Equal(document.Runs[index].StartedAt) && latest < index {
				latest = index
			}
		}
		if latest < 0 {
			return fmt.Errorf("abandon run: unknown run %q", runID)
		}
		run := document.Runs[latest]
		if run.Outcome == Success {
			return fmt.Errorf("abandon run: successful run %q cannot be abandoned", runID)
		}
		if !resumableOutcome(run.Outcome) {
			return fmt.Errorf("abandon run: outcome %q is not abandonable", run.Outcome)
		}
		if run.Outcome == Partial {
			document.Runs[latest].Resumable = false
			document.Runs[latest].Reason = reason
			document.Runs[latest].EndedAt = endedAt.UTC()
			return nil
		}

		failed := -1
		for index := range document.Runs {
			if index != latest && document.Runs[index].ID == runID && document.Runs[index].Outcome == Failed {
				failed = index
				break
			}
		}
		run.Outcome = Failed
		run.Resumable = false
		run.Reason = reason
		run.EndedAt = endedAt.UTC()
		if failed >= 0 {
			document.Runs[failed] = run
			document.Runs = append(document.Runs[:latest], document.Runs[latest+1:]...)
		} else {
			document.Runs[latest] = run
		}
		return nil
	})
}
