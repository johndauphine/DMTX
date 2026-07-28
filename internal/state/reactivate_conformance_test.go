package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRunReactivationCycleConformance(t *testing.T) {
	for name, factory := range reactivationBackendFactories() {
		t.Run(name, func(t *testing.T) {
			testRunReactivationCycles(t, factory(t))
		})
	}
}

func TestRunReactivationTerminalRejectionConformance(t *testing.T) {
	for name, factory := range reactivationBackendFactories() {
		t.Run(name, func(t *testing.T) {
			t.Run("success", func(t *testing.T) {
				testSuccessfulRunRejectsReactivation(t, factory(t))
			})
			t.Run("non-resumable", func(t *testing.T) {
				testNonResumableRunRejectsReactivation(t, factory(t))
			})
		})
	}
}

func reactivationBackendFactories() map[string]func(*testing.T) Backend {
	return map[string]func(*testing.T) Backend{
		"sqlite": func(t *testing.T) Backend {
			return SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		},
		"yaml": func(t *testing.T) Backend {
			return YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		},
	}
}

func testRunReactivationCycles(t *testing.T, backend Backend) {
	t.Helper()
	started := time.Date(2026, 7, 28, 16, 0, 0, 0, time.UTC)
	initial := Run{
		ID:        "cycle",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		Reason:    "initial attempt",
		StartedAt: started,
	}
	if err := backend.InitializeRun(initial, "hash"); err != nil {
		t.Fatal(err)
	}
	if err := backend.ReactivateRun(initial.ID, "initial running refresh"); err != nil {
		t.Fatal(err)
	}
	assertLatestRunTransition(
		t, backend, initial.ID, Running, "initial running refresh", started, time.Time{},
	)

	steps := []struct {
		outcome Outcome
		reason  string
	}{
		{outcome: Failed, reason: "first failure"},
		{outcome: Cancelled, reason: "first cancellation"},
		{outcome: Partial, reason: "first partial attempt"},
		{outcome: Failed, reason: "second failure"},
		{outcome: Cancelled, reason: "second cancellation"},
		{outcome: Partial, reason: "second partial attempt"},
	}
	for index, step := range steps {
		endedAt := started.Add(time.Duration(index+1) * time.Minute)
		var err error
		if index == 3 {
			err = backend.UpdateFailure(initial.ID, step.reason, endedAt)
		} else {
			err = backend.UpdateRecoverableOutcome(initial.ID, step.outcome, step.reason, endedAt)
		}
		if err != nil {
			t.Fatalf("record %s transition %d: %v", step.outcome, index, err)
		}
		assertLatestRunTransition(
			t, backend, initial.ID, step.outcome, step.reason, started, endedAt,
		)
		if index == len(steps)-1 {
			continue
		}
		reason := "resume after " + string(step.outcome)
		if err := backend.ReactivateRun(initial.ID, reason); err != nil {
			t.Fatalf("reactivate after %s transition %d: %v", step.outcome, index, err)
		}
		assertLatestRunTransition(
			t, backend, initial.ID, Running, reason, started, time.Time{},
		)
	}

	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	var cycle []Run
	for _, run := range runs {
		if run.ID == initial.ID {
			cycle = append(cycle, run)
		}
	}
	wantOrder := []Outcome{Failed, Cancelled, Running, Partial}
	if len(cycle) != len(wantOrder) {
		t.Fatalf("cycle history = %#v, want one record for each outcome", cycle)
	}
	for index, want := range wantOrder {
		if cycle[index].Outcome != want {
			t.Fatalf("cycle outcome %d = %q, want %q; history = %#v", index, cycle[index].Outcome, want, cycle)
		}
		if !cycle[index].StartedAt.Equal(started) {
			t.Fatalf("cycle outcome %q StartedAt = %v, want %v", want, cycle[index].StartedAt, started)
		}
	}
	selected, found, err := backend.LatestResumableForTarget(initial.Target)
	if err != nil || !found {
		t.Fatalf("latest resumable found = %v, error = %v", found, err)
	}
	if selected.ID != initial.ID || selected.Outcome != Partial || selected.Reason != "second partial attempt" {
		t.Fatalf("latest resumable = %#v", selected)
	}
}

func testSuccessfulRunRejectsReactivation(t *testing.T, backend Backend) {
	t.Helper()
	started := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	run := Run{
		ID:        "successful",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		Reason:    "running",
		StartedAt: started,
	}
	if err := backend.InitializeRun(run, "hash"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(Run{
		ID:        run.ID,
		Source:    run.Source,
		Target:    run.Target,
		Outcome:   Success,
		Resumable: false,
		Reason:    "completed",
		StartedAt: started,
		EndedAt:   started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.ReactivateRun(run.ID, "too late"); err == nil {
		t.Fatal("successful run accepted reactivation")
	}
	if err := backend.UpdateRecoverableOutcome(
		run.ID, Cancelled, "too late", started.Add(2*time.Minute),
	); err == nil {
		t.Fatal("successful run accepted recoverable transition")
	}
	assertLatestRunTransition(
		t, backend, run.ID, Success, "completed", started, started.Add(time.Minute),
	)
}

func testNonResumableRunRejectsReactivation(t *testing.T, backend Backend) {
	t.Helper()
	started := time.Date(2026, 7, 28, 18, 0, 0, 0, time.UTC)
	run := Run{
		ID:        "accepted-partial",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		Reason:    "running",
		StartedAt: started,
	}
	if err := backend.InitializeRun(run, "hash"); err != nil {
		t.Fatal(err)
	}
	partialAt := started.Add(time.Minute)
	if err := backend.UpdateRecoverableOutcome(
		run.ID, Partial, "incomplete", partialAt,
	); err != nil {
		t.Fatal(err)
	}
	acceptedAt := started.Add(2 * time.Minute)
	if err := backend.AbandonRun(run.ID, "accepted incomplete result", acceptedAt); err != nil {
		t.Fatal(err)
	}
	if err := backend.ReactivateRun(run.ID, "too late"); err == nil {
		t.Fatal("non-resumable run accepted reactivation")
	}
	if err := backend.UpdateRecoverableOutcome(
		run.ID, Failed, "too late", started.Add(3*time.Minute),
	); err == nil {
		t.Fatal("non-resumable run accepted recoverable transition")
	}
	assertLatestRunTransition(
		t, backend, run.ID, Partial, "accepted incomplete result", started, acceptedAt,
	)
	latest := latestRunTransition(t, backend, run.ID)
	if latest.Resumable {
		t.Fatalf("accepted partial run remained resumable: %#v", latest)
	}
}

func assertLatestRunTransition(
	t *testing.T,
	backend Backend,
	runID string,
	outcome Outcome,
	reason string,
	startedAt time.Time,
	endedAt time.Time,
) {
	t.Helper()
	latest := latestRunTransition(t, backend, runID)
	if latest.Outcome != outcome || latest.Reason != reason {
		t.Fatalf("latest %q transition = %#v, want outcome %q reason %q", runID, latest, outcome, reason)
	}
	if !latest.StartedAt.Equal(startedAt) {
		t.Fatalf("latest %q StartedAt = %v, want %v", runID, latest.StartedAt, startedAt)
	}
	if !latest.EndedAt.Equal(endedAt) {
		t.Fatalf("latest %q EndedAt = %v, want %v", runID, latest.EndedAt, endedAt)
	}
}

func latestRunTransition(t *testing.T, backend Backend, runID string) Run {
	t.Helper()
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	for index := len(runs) - 1; index >= 0; index-- {
		if runs[index].ID == runID {
			return runs[index]
		}
	}
	t.Fatalf("run %q was not found in %#v", runID, runs)
	return Run{}
}
