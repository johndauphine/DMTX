package migrate

import (
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func TestEvaluateDeleteReconciliationDueUsesDurableCompletion(t *testing.T) {
	completedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	last := state.DeleteReconciliation{
		RunID:       "run-1",
		Task:        state.TaskKey{Type: "table-copy", Table: "items"},
		AttemptID:   "attempt-1",
		Due:         true,
		Status:      state.DeleteReconciliationCompleted,
		StartedAt:   completedAt.Add(-time.Minute),
		CompletedAt: completedAt,
	}
	facts, err := EvaluateDeleteReconciliationDue(
		completedAt.Add(30*time.Minute),
		time.Hour,
		last,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Due || facts.LastSuccessfulAt == nil || facts.NextDueAt == nil {
		t.Fatalf("early due facts = %#v", facts)
	}
	if got, want := facts.NextDueAt.UTC(), completedAt.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("next due at = %s, want %s", got, want)
	}

	facts, err = EvaluateDeleteReconciliationDue(
		completedAt.Add(2*time.Hour),
		time.Hour,
		last,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !facts.Due || facts.Reason != "reconciliation interval elapsed" {
		t.Fatalf("elapsed due facts = %#v", facts)
	}
}

func TestEvaluateDeleteReconciliationDueTreatsMissingEvidenceAsDue(t *testing.T) {
	facts, err := EvaluateDeleteReconciliationDue(
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Hour,
		state.DeleteReconciliation{},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !facts.Due || facts.Reason != "no prior successful reconciliation" {
		t.Fatalf("missing evidence facts = %#v", facts)
	}
}
