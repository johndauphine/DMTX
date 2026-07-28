package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOutcomeAndAbandonmentConformance(t *testing.T) {
	factories := map[string]func(string) Backend{
		"sqlite": func(directory string) Backend { return SQLiteStore{Path: filepath.Join(directory, "state.db")} },
		"yaml":   func(directory string) Backend { return YAMLStore{Path: filepath.Join(directory, "state.yaml")} },
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			testOutcomeSelectionAndAbandonment(t, factory)
		})
	}
}

func testOutcomeSelectionAndAbandonment(t *testing.T, factory func(string) Backend) {
	t.Helper()
	start := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	store := factory(t.TempDir())
	appendRun := func(id string, outcome Outcome, resumable bool, offset int) {
		t.Helper()
		if err := store.Append(Run{
			ID: id, Source: "source", Target: "target", Outcome: outcome,
			Resumable: resumable, Reason: string(outcome), StartedAt: start.Add(time.Duration(offset) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	appendRun("old-resumable", Failed, true, 0)
	appendRun("ignored-terminal", Failed, false, 1)
	selected, found, err := store.LatestResumableForTarget("target")
	if err != nil || !found || selected.ID != "old-resumable" {
		t.Fatalf("selection = %#v, found=%v, err=%v", selected, found, err)
	}
	appendRun("success", Success, false, 2)
	if _, found, err := store.LatestResumableForTarget("target"); err != nil || found {
		t.Fatalf("success did not supersede prior candidate: found=%v err=%v", found, err)
	}
	if err := store.UpdateRecoverableOutcome("success", Cancelled, "too late", start.Add(3*time.Minute)); err == nil {
		t.Fatal("successful run accepted a recoverable outcome")
	}
	appendRun("new-cancelled", Cancelled, true, 3)
	selected, found, err = store.LatestResumableForTarget("target")
	if err != nil || !found || selected.ID != "new-cancelled" {
		t.Fatalf("post-success selection = %#v, found=%v, err=%v", selected, found, err)
	}
	if err := store.AbandonRun("new-cancelled", "operator chose rebuild", start.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	var abandoned Run
	for _, run := range runs {
		if run.ID == "new-cancelled" {
			abandoned = run
		}
	}
	if abandoned.Outcome != Failed || abandoned.Resumable || abandoned.Reason != "operator chose rebuild" {
		t.Fatalf("abandoned cancelled run = %#v", abandoned)
	}

	partialStore := factory(t.TempDir())
	if err := partialStore.Append(Run{
		ID: "partial", Source: "source", Target: "partial-target", Outcome: Partial,
		Resumable: true, Reason: "table failed", StartedAt: start,
	}); err != nil {
		t.Fatal(err)
	}
	if err := partialStore.AbandonRun("partial", "accepted incomplete history", start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	partial, found, err := partialStore.Latest()
	if err != nil || !found {
		t.Fatal(err)
	}
	if partial.Outcome != Partial || partial.Resumable || partial.Reason != "accepted incomplete history" {
		t.Fatalf("abandoned partial = %#v", partial)
	}
	if err := partialStore.AbandonRun("missing", "reason", start); err == nil {
		t.Fatal("unknown run abandonment succeeded")
	}
}
