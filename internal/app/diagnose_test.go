package app

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// diagnosisFixture records runs and tasks the way a real migration would.
func diagnosisFixture(t *testing.T, runs []state.Run, tasks []state.Task) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "migration.state.db")
	store := state.SQLiteStore{Path: path}
	for _, run := range runs {
		if err := store.Append(run); err != nil {
			t.Fatalf("append run %s: %v", run.ID, err)
		}
	}
	if len(tasks) > 0 {
		if err := store.CreateTasks(tasks); err != nil {
			t.Fatalf("create tasks: %v", err)
		}
	}
	return path
}

func diagnosisOf(t *testing.T, request Request) (Outcome, Diagnosis) {
	t.Helper()
	outcome := executeDiagnose(request)
	if outcome.Payload == nil {
		return outcome, Diagnosis{}
	}
	var diagnosis Diagnosis
	if err := json.Unmarshal(outcome.Payload.Data, &diagnosis); err != nil {
		t.Fatalf("decode diagnosis: %v", err)
	}
	return outcome, diagnosis
}

// TestDiagnoseExplainsTheLastRunThatWentWrong pins the choice of run.
//
// Not simply the latest: after a failure an operator often runs something else
// - a status, a validate - and the newest run may be the one they just did.
// The interesting one is the last that went wrong.
func TestDiagnoseExplainsTheLastRunThatWentWrong(t *testing.T) {
	started := time.Now().UTC().Add(-time.Hour)
	path := diagnosisFixture(t, []state.Run{
		{ID: "old-failure", Outcome: state.Failed, StartedAt: started},
		{ID: "the-failure", Outcome: state.Partial, Resumable: true,
			Reason: "interrupted", StartedAt: started.Add(time.Minute)},
		{ID: "later-success", Outcome: state.Success, StartedAt: started.Add(2 * time.Minute)},
	}, nil)

	outcome, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if outcome.ExitCode != Success {
		t.Fatalf("diagnose failed: %+v", outcome.Messages)
	}
	if diagnosis.Run.ID != "the-failure" {
		t.Errorf("diagnosed %q, want the last run that did not succeed", diagnosis.Run.ID)
	}
}

// TestDiagnoseCanBePointedAtARun pins --run, since the last failure is not
// always the one being investigated.
func TestDiagnoseCanBePointedAtARun(t *testing.T) {
	started := time.Now().UTC().Add(-time.Hour)
	path := diagnosisFixture(t, []state.Run{
		{ID: "first", Outcome: state.Failed, StartedAt: started},
		{ID: "second", Outcome: state.Failed, StartedAt: started.Add(time.Minute)},
	}, nil)

	_, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path, RunID: "first"})
	if diagnosis.Run.ID != "first" {
		t.Errorf("diagnosed %q, want the run that was asked for", diagnosis.Run.ID)
	}

	outcome := executeDiagnose(Request{Command: "diagnose", StatePath: path, RunID: "invented"})
	if outcome.ExitCode == Success {
		t.Error("diagnose succeeded for a run id that does not exist")
	}
	if !strings.Contains(saidBy(outcome), "invented") {
		t.Errorf("the refusal does not name the run asked for: %q", saidBy(outcome))
	}
}

// TestDiagnoseCountsWhatSurvivesAResume pins the tally, which is the number an
// operator actually wants: how much of the work is already done.
func TestDiagnoseCountsWhatSurvivesAResume(t *testing.T) {
	path := diagnosisFixture(t,
		[]state.Run{{ID: "run", Outcome: state.Partial, Resumable: true, StartedAt: time.Now().UTC()}},
		[]state.Task{
			{RunID: "run", Table: "orders", StartedAt: time.Now().UTC()},
			{RunID: "run", Table: "customers", StartedAt: time.Now().UTC()},
			{RunID: "run", Table: "items", StartedAt: time.Now().UTC()},
		},
	)
	store := state.SQLiteStore{Path: path}
	if err := store.CompleteTask("run", "orders", 100, time.Now().UTC()); err != nil {
		t.Fatalf("complete: %v", err)
	}

	_, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if diagnosis.Tables.Total != 3 {
		t.Errorf("counted %d tables, want 3", diagnosis.Tables.Total)
	}
	if diagnosis.Tables.Completed != 1 {
		t.Errorf("counted %d completed, want 1", diagnosis.Tables.Completed)
	}
	if len(diagnosis.Incomplete) != 2 {
		t.Errorf("named %v as incomplete, want the two unfinished tables", diagnosis.Incomplete)
	}
	for _, name := range diagnosis.Incomplete {
		if name == "orders" {
			t.Error("a completed table was named as incomplete")
		}
	}
}

// TestATableWithNoRowsIsNotCountedAsStarted pins the distinction the tally
// claims to make.
//
// Every task is created with status "running" before any work begins, so status
// alone cannot separate a table that transferred rows from one that was never
// reached. Reading it from status made the tally confidently wrong in the
// direction that matters: an interrupted run looked further along than it was.
func TestATableWithNoRowsIsNotCountedAsStarted(t *testing.T) {
	now := time.Now().UTC()
	path := diagnosisFixture(t,
		[]state.Run{{ID: "run", Outcome: state.Partial, Resumable: true, StartedAt: now}},
		[]state.Task{
			{RunID: "run", Table: "done", StartedAt: now},
			{RunID: "run", Table: "partial", StartedAt: now},
			{RunID: "run", Table: "frontier_only", StartedAt: now},
			{RunID: "run", Table: "never_reached", StartedAt: now},
		},
	)
	store := state.SQLiteStore{Path: path}
	if err := store.CompleteTask("run", "done", 100, now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// Rows recorded against one table, which is the ordinary evidence that
	// work began.
	if err := store.AdvanceIntegerKeysetTask("run", "partial", 50, 500); err != nil {
		t.Fatalf("advance: %v", err)
	}
	// And a frontier acknowledged with no rows behind it, which happens when a
	// page's rows are all filtered out. The engine has been through that part
	// of the table, so the table has started even though nothing moved.
	if err := store.AdvanceIntegerKeysetTask("run", "frontier_only", 0, 900); err != nil {
		t.Fatalf("advance: %v", err)
	}

	_, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if diagnosis.Tables.Completed != 1 {
		t.Errorf("completed = %d, want 1", diagnosis.Tables.Completed)
	}
	if diagnosis.Tables.InProgress != 2 {
		t.Errorf(
			"in progress = %d, want 2 - the table with rows and the one with a "+
				"frontier acknowledged but no rows behind it",
			diagnosis.Tables.InProgress,
		)
	}
	if diagnosis.Tables.NotStarted != 1 {
		t.Errorf(
			"not started = %d, want 1 - a task row is written for every table "+
				"before any work begins, so its presence is not evidence of progress",
			diagnosis.Tables.NotStarted,
		)
	}
}

// TestDiagnoseRefusesFlagsItDoesNotKnow pins that a typo is refused rather than
// skipped.
//
// Skipping "--ruun abc" would diagnose a different run than the operator asked
// about and say nothing - a wrong answer delivered confidently, which is worse
// than the error they would have fixed in seconds.
func TestDiagnoseRefusesFlagsItDoesNotKnow(t *testing.T) {
	if _, ok := diagnoseArguments([]string{"--state", "s.db", "--run", "abc"}); !ok {
		t.Fatal("diagnose refused its own flags")
	}
	for _, refused := range [][]string{
		{"--state", "s.db", "--ruun", "abc"},
		{"--state"},
		{"--state", "s.db", "--run"},
		{"--state", "s.db", "--state", "other.db"},
		{"s.db"},
		{"--state", "s.db", "extra"},
	} {
		if _, ok := diagnoseArguments(refused); ok {
			t.Errorf("diagnose accepted %v", refused)
		}
	}
}

// TestDiagnoseSaysWhatToDoNext pins that every diagnosis ends with an action,
// because "it failed" without "so do this" leaves the operator where they
// started.
func TestDiagnoseSaysWhatToDoNext(t *testing.T) {
	now := time.Now().UTC()
	for name, expectation := range map[string]struct {
		run  state.Run
		want string
	}{
		"resumable": {
			run:  state.Run{ID: "r", Outcome: state.Partial, Resumable: true, StartedAt: now},
			want: "dmtx resume",
		},
		"not resumable": {
			run: state.Run{ID: "r", Outcome: state.Failed, Resumable: false,
				Reason: "target schema changed", StartedAt: now},
			want: "dmtx run",
		},
		"interrupted": {
			run:  state.Run{ID: "r", Outcome: state.Running, StartedAt: now},
			want: "dmtx resume",
		},
		"nothing wrong": {
			run:  state.Run{ID: "r", Outcome: state.Success, StartedAt: now},
			want: "nothing to do",
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := diagnosisFixture(t, []state.Run{expectation.run}, nil)
			outcome, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
			if outcome.ExitCode != Success {
				t.Fatalf("diagnose failed: %+v", outcome.Messages)
			}
			if diagnosis.NextStep == "" {
				t.Fatal("a diagnosis with no next step leaves the operator where they started")
			}
			if !strings.Contains(diagnosis.NextStep, expectation.want) {
				t.Errorf("next step is %q, want it to mention %q", diagnosis.NextStep, expectation.want)
			}
		})
	}
}

// TestARefusalToResumeSaysWhy pins that a run which cannot be resumed explains
// itself. Told only "not resumable", an operator tries it anyway.
func TestARefusalToResumeSaysWhy(t *testing.T) {
	path := diagnosisFixture(t, []state.Run{{
		ID: "r", Outcome: state.Failed, Resumable: false,
		Reason: "target schema changed under the run", StartedAt: time.Now().UTC(),
	}}, nil)

	_, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if !strings.Contains(diagnosis.NextStep, "target schema changed under the run") {
		t.Errorf("the next step does not carry the reason: %q", diagnosis.NextStep)
	}
}

// TestDiagnoseNamesOnlyASampleOfAWideMigration pins the cap, so a migration of
// thousands of tables does not answer with thousands of names.
func TestDiagnoseNamesOnlyASampleOfAWideMigration(t *testing.T) {
	now := time.Now().UTC()
	tasks := make([]state.Task, 0, maxNamedIncompleteTables+40)
	for index := 0; index < maxNamedIncompleteTables+40; index++ {
		tasks = append(tasks, state.Task{
			RunID: "run", Table: strings.Repeat("t", 1) + itoa(index), StartedAt: now,
		})
	}
	path := diagnosisFixture(t,
		[]state.Run{{ID: "run", Outcome: state.Partial, Resumable: true, StartedAt: now}},
		tasks,
	)

	_, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if len(diagnosis.Incomplete) > maxNamedIncompleteTables {
		t.Errorf("named %d tables, over the cap of %d",
			len(diagnosis.Incomplete), maxNamedIncompleteTables)
	}
	if !diagnosis.IncompleteTruncated {
		t.Error("the list was cut without saying so, which reads as a short problem")
	}
	// The full count must still be visible, or the cap hides the scale.
	if diagnosis.Tables.InProgress+diagnosis.Tables.NotStarted != len(tasks) {
		t.Errorf("the tally lost tables: %+v", diagnosis.Tables)
	}
}

// TestDiagnoseCarriesNoLeaseToken pins that the run in a diagnosis is redacted
// the same way status redacts it.
func TestDiagnoseCarriesNoLeaseToken(t *testing.T) {
	const token = "owner-token-should-not-appear"
	path := diagnosisFixture(t, []state.Run{{
		ID: "r", Outcome: state.Failed, StartedAt: time.Now().UTC(),
		LeaseTarget: "sqlite:target", LeaseOwnerToken: token, LeaseGeneration: 1,
	}}, nil)

	outcome, diagnosis := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	if diagnosis.Run.LeaseOwnerToken != "" {
		t.Errorf("the diagnosis carries a lease owner token: %q", diagnosis.Run.LeaseOwnerToken)
	}
	if strings.Contains(string(outcome.Payload.Data), token) {
		t.Errorf("the payload carries the token: %s", outcome.Payload.Data)
	}
}

// TestDiagnosisPayloadWireShape pins the JSON a console will read, and is the
// shape TestEveryPayloadKindIsPinned points at for PayloadDiagnosis.
func TestDiagnosisPayloadWireShape(t *testing.T) {
	const token = "owner-token-should-not-appear"
	now := time.Now().UTC()
	path := diagnosisFixture(t,
		[]state.Run{{
			ID: "run-1", Outcome: state.Partial, Resumable: true,
			Reason: "interrupted", StartedAt: now, EndedAt: now,
			Source: "sqlite:a.db", Target: "sqlite:b.db",
			LeaseTarget: "sqlite:b.db", LeaseOwnerToken: token, LeaseGeneration: 1,
		}},
		[]state.Task{{RunID: "run-1", Table: "orders", StartedAt: now}},
	)

	outcome, _ := diagnosisOf(t, Request{Command: "diagnose", StatePath: path})
	var decoded map[string]any
	if err := json.Unmarshal(outcome.Payload.Data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	present := map[string]bool{}
	collectPaths("", decoded, present)

	for _, path := range []string{
		"run", "run.id", "run.outcome", "run.resumable", "run.resumability_reason",
		"tables", "tables.total", "tables.completed", "tables.in_progress", "tables.not_started",
		"incomplete", "findings", "next_step",
	} {
		if !present[path] {
			t.Errorf("the wire shape lost %s", path)
		}
	}
	if present["run.lease_owner_token"] {
		t.Error("the wire shape carries run.lease_owner_token")
	}
	if strings.Contains(string(outcome.Payload.Data), token) {
		t.Errorf("the payload carries the lease token: %s", outcome.Payload.Data)
	}
}

// TestDiagnoseRefusesWithoutAStatePath pins the usage message.
func TestDiagnoseRefusesWithoutAStatePath(t *testing.T) {
	outcome := executeDiagnose(Request{Command: "diagnose"})
	if outcome.ExitCode == Success {
		t.Fatal("diagnose succeeded with no state to read")
	}
	if !strings.Contains(saidBy(outcome), "usage: dmtx diagnose --state") {
		t.Errorf("diagnose did not say how to call it: %q", saidBy(outcome))
	}
}

// TestDiagnoseOnAnEmptyStateSaysSo pins that a state file with no runs is an
// answer rather than an error.
func TestDiagnoseOnAnEmptyStateSaysSo(t *testing.T) {
	path := diagnosisFixture(t, nil, nil)
	outcome := executeDiagnose(Request{Command: "diagnose", StatePath: path})
	if outcome.ExitCode != Success {
		t.Fatalf("diagnose failed on an empty state: %+v", outcome.Messages)
	}
	if !strings.Contains(saidBy(outcome), "no runs recorded") {
		t.Errorf("diagnose said %q on an empty state", saidBy(outcome))
	}
}

// itoa avoids importing strconv for one call in a table name.
func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
