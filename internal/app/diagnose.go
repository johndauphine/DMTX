package app

import (
	"fmt"
	"sort"

	"github.com/johndauphine/dmtx/internal/state"
)

// Diagnosis explains a run that did not finish.
//
// It is assembled from durable state alone. That is deliberate: the moment an
// operator wants a diagnosis is the moment something has gone wrong, which is
// often the moment a database is unreachable. A diagnosis that needed the
// databases would be unavailable exactly when it was needed. Forward-looking
// connectivity checks are preflight's job, and doing them here would duplicate
// it while making this command fail for reasons of its own.
type Diagnosis struct {
	Run state.Run `json:"run"`

	// Tables counts what the run got through, so an operator can see how much
	// of the work survives a resume.
	Tables TableTally `json:"tables"`

	// Incomplete names the tables that did not finish, capped so a wide
	// migration does not answer with thousands of names.
	Incomplete []string `json:"incomplete,omitempty"`
	// IncompleteTruncated says the list above is a sample rather than all of
	// it, so nobody reads a short list as a short problem.
	IncompleteTruncated bool `json:"incomplete_truncated,omitempty"`

	// Findings are the plain statements a person reads first.
	Findings []string `json:"findings"`
	// NextStep is the single thing to do, or the reason there is nothing.
	NextStep string `json:"next_step"`
}

// TableTally counts a run's tables by how far each got.
//
// The split between started and not started is read from recorded progress,
// not from task status. Every task is created with status "running" before any
// work begins, so status alone cannot tell a table that transferred half a
// million rows from one that was never reached - and a tally that claimed to
// would be confidently wrong in the direction that matters, making an
// interrupted run look further along than it was.
type TableTally struct {
	Total      int `json:"total"`
	Completed  int `json:"completed"`
	InProgress int `json:"in_progress"`
	NotStarted int `json:"not_started"`
}

// maxNamedIncompleteTables bounds the named sample. A list longer than this is
// something to scroll past rather than read.
const maxNamedIncompleteTables = 20

// executeDiagnose explains why a run did not finish and what to do about it.
func executeDiagnose(request Request) Outcome {
	out := newOutcome(request.Command)
	if request.StatePath == "" {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx diagnose --state migration.yaml.state.db [--run ID]",
		)
	}
	store, err := state.NewBackend(request.StatePath)
	if err != nil {
		return out.failWith(StateError, err.Error())
	}

	run, found, err := selectRunToDiagnose(store, request.RunID)
	if err != nil {
		return out.failWith(StateError, err.Error())
	}
	if !found {
		if request.RunID != "" {
			return out.failWith(StateError, fmt.Sprintf("no run recorded with id %q", request.RunID))
		}
		out.out("no runs recorded")
		return out.done(Success)
	}

	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		return out.failWith(StateError, err.Error())
	}

	diagnosis := diagnose(run, tasks)
	for _, line := range diagnosis.lines() {
		out.out(line)
	}
	if err := out.setPayload(PayloadDiagnosis, diagnosis); err != nil {
		return out.failWith(FileError, "write diagnosis: "+err.Error())
	}
	return out.done(Success)
}

// selectRunToDiagnose finds the run to explain: the one named, or the most
// recent that did not succeed.
//
// Not simply the latest: after a failure an operator often runs something else
// - a status, a validate - and the latest run may be the one they just did.
// The interesting run is the last one that went wrong.
func selectRunToDiagnose(store state.Backend, runID string) (state.Run, bool, error) {
	runs, err := store.List()
	if err != nil {
		return state.Run{}, false, err
	}
	if runID != "" {
		for _, run := range runs {
			if run.ID == runID {
				return run, true, nil
			}
		}
		return state.Run{}, false, nil
	}
	for index := len(runs) - 1; index >= 0; index-- {
		if runs[index].Outcome != state.Success {
			return runs[index], true, nil
		}
	}
	// Nothing went wrong. The latest run is still worth reporting, because
	// "there is nothing to diagnose" is a useful answer and an empty one is not.
	if len(runs) > 0 {
		return runs[len(runs)-1], true, nil
	}
	return state.Run{}, false, nil
}

// diagnose turns a run and its tasks into an explanation.
func diagnose(run state.Run, tasks []state.Task) Diagnosis {
	diagnosis := Diagnosis{Run: publicRun(run)}

	var incomplete []string
	for _, task := range tasks {
		diagnosis.Tables.Total++
		switch {
		case task.Status == "completed":
			diagnosis.Tables.Completed++
		case startedWork(task):
			diagnosis.Tables.InProgress++
			incomplete = append(incomplete, task.Table)
		default:
			diagnosis.Tables.NotStarted++
			incomplete = append(incomplete, task.Table)
		}
	}
	sort.Strings(incomplete)
	if len(incomplete) > maxNamedIncompleteTables {
		diagnosis.Incomplete = incomplete[:maxNamedIncompleteTables]
		diagnosis.IncompleteTruncated = true
	} else {
		diagnosis.Incomplete = incomplete
	}

	diagnosis.Findings, diagnosis.NextStep = interpret(run, diagnosis.Tables)
	return diagnosis
}

// startedWork reports whether a task got anywhere before the run stopped.
//
// Rows or a watermark are the only durable evidence of that. A task row alone
// proves the table was planned, since one is written for every table before the
// first byte moves.
func startedWork(task state.Task) bool {
	return task.RowsDone > 0 ||
		task.IntegerWatermark != nil ||
		task.RowNumberWatermark != nil
}

// interpret states what happened and what to do, in that order.
func interpret(run state.Run, tally TableTally) ([]string, string) {
	findings := []string{
		fmt.Sprintf("run %s ended as %s", run.ID, run.Outcome),
	}
	if run.Reason != "" {
		findings = append(findings, "reason: "+run.Reason)
	}
	if tally.Total > 0 {
		findings = append(findings, fmt.Sprintf(
			"%d of %d tables completed", tally.Completed, tally.Total,
		))
	}

	switch {
	case run.Outcome == state.Success:
		return findings, "nothing to do: the last recorded run succeeded"
	case run.Outcome == state.Running:
		// A run left as running was interrupted before it could record how it
		// ended - a kill, a power loss - rather than one still going: this
		// command reads state, and a live run holds the lease.
		findings = append(findings, "the run never recorded an ending, so it was interrupted rather than finished")
		return findings, "check no dmtx is still running, then: dmtx resume --config migration.yaml"
	case run.Resumable:
		return findings, "dmtx resume --config migration.yaml"
	default:
		// Saying why resume is not offered matters more than saying it is not:
		// an operator told only "not resumable" will try it anyway.
		if run.Reason != "" {
			return findings, "this run cannot be resumed (" + run.Reason +
				"); start again with: dmtx run --config migration.yaml"
		}
		return findings, "this run cannot be resumed; start again with: dmtx run --config migration.yaml"
	}
}

// lines renders the diagnosis for a terminal.
func (diagnosis Diagnosis) lines() []string {
	lines := append([]string(nil), diagnosis.Findings...)
	if len(diagnosis.Incomplete) > 0 {
		heading := "incomplete tables:"
		if diagnosis.IncompleteTruncated {
			heading = fmt.Sprintf(
				"incomplete tables (first %d of %d):",
				len(diagnosis.Incomplete),
				diagnosis.Tables.InProgress+diagnosis.Tables.NotStarted,
			)
		}
		lines = append(lines, heading)
		for _, table := range diagnosis.Incomplete {
			lines = append(lines, "  "+table)
		}
	}
	return append(lines, "next: "+diagnosis.NextStep)
}
