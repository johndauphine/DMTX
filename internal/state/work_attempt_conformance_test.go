package state

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRangeAttemptBackendConformance(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			backend, reopen := newRangeAttemptBackend(t, stateKind)
			key, intent := initializeRangeAttemptWork(t, backend, "attempt-run")
			firstAt := time.Date(2026, 7, 28, 13, 0, 1, 0, time.UTC)
			attempt := RangeAttempt{
				RunID:        "attempt-run",
				Task:         key,
				RangeID:      "0",
				TopologyHash: "topology-a",
				Sequence:     0,
				At:           firstAt,
			}
			missingIntent := RangeAcknowledgement{
				RunID: "attempt-run", Task: key, RangeID: "0", TopologyHash: "topology-a",
				Sequence: 1, ChunkRows: 5, DurableRows: 5, At: firstAt,
			}
			assertRejectedRangeAcknowledgementDoesNotMutate(
				t, backend, missingIntent, ErrRangeOrder, "acknowledgement without intent",
			)
			zeroAttempt := RangeAcknowledgement{
				RunID: "attempt-run", Task: key, RangeID: "0", TopologyHash: "topology-a",
				Sequence: 0, ChunkRows: 5, DurableRows: 5, At: firstAt,
			}
			assertRejectedRangeAcknowledgementDoesNotMutate(
				t, backend, zeroAttempt, ErrRangeOrder, "acknowledgement without target authorization",
			)

			if err := backend.RecordRangeAttempt(attempt); err != nil {
				t.Fatalf("record first attempt: %v", err)
			}
			assertRangeAttemptCounts(t, backend, "attempt-run", 1, 0, 1)
			task, workRange := rangeAttemptSnapshot(t, backend, "attempt-run")
			if !task.UpdatedAt.Equal(firstAt) || !workRange.UpdatedAt.Equal(firstAt) {
				t.Fatalf("first attempt timestamps task=%v range=%v, want %v", task.UpdatedAt, workRange.UpdatedAt, firstAt)
			}

			// Reissuing the same durable intent must not reset its attempt count.
			intent.At = firstAt.Add(time.Second)
			if err := backend.BeginRangeChunk(intent); err != nil {
				t.Fatalf("repeat range intent: %v", err)
			}
			assertRangeAttemptCounts(t, backend, "attempt-run", 1, 0, 1)

			attempt.At = firstAt.Add(2 * time.Second)
			if err := backend.RecordRangeAttempt(attempt); err != nil {
				t.Fatalf("record retry: %v", err)
			}
			assertRangeAttemptCounts(t, backend, "attempt-run", 2, 1, 2)

			if _, err := backend.AcknowledgeRange(RangeAcknowledgement{
				RunID: "attempt-run", Task: key, RangeID: "0", TopologyHash: "topology-a",
				Sequence: 0, ChunkRows: 5, DurableRows: 5, At: firstAt.Add(3 * time.Second),
			}); err != nil {
				t.Fatalf("acknowledge first sequence: %v", err)
			}
			nextIntent := intent
			nextIntent.Sequence = 1
			nextIntent.At = firstAt.Add(4 * time.Second)
			if err := backend.BeginRangeChunk(nextIntent); err != nil {
				t.Fatalf("begin next sequence: %v", err)
			}
			attempt.Sequence = 1
			attempt.At = firstAt.Add(5 * time.Second)
			if err := backend.RecordRangeAttempt(attempt); err != nil {
				t.Fatalf("record first attempt for next sequence: %v", err)
			}
			assertRangeAttemptCounts(t, backend, "attempt-run", 3, 1, 1)

			// A freshly opened backend must classify another authorization for
			// the unresolved sequence as a retry.
			backend = reopen()
			attempt.At = firstAt.Add(6 * time.Second)
			if err := backend.RecordRangeAttempt(attempt); err != nil {
				t.Fatalf("record retry after reopen: %v", err)
			}
			assertRangeAttemptCounts(t, backend, "attempt-run", 4, 2, 2)

			if _, err := backend.AcknowledgeRange(RangeAcknowledgement{
				RunID: "attempt-run", Task: key, RangeID: "0", TopologyHash: "topology-a",
				Sequence: 1, ChunkRows: 5, DurableRows: 5, At: firstAt.Add(7 * time.Second),
			}); err != nil {
				t.Fatalf("acknowledge second sequence: %v", err)
			}

			assertRejectedRangeAttemptDoesNotMutate(t, backend, attempt, ErrRangeOrder, "resolved sequence")
			missingSequence := attempt
			missingSequence.Sequence = 2
			assertRejectedRangeAttemptDoesNotMutate(t, backend, missingSequence, ErrRangeOrder, "missing sequence")
			staleTopology := attempt
			staleTopology.TopologyHash = "topology-b"
			assertRejectedRangeAttemptDoesNotMutate(t, backend, staleTopology, ErrTopologyChanged, "stale topology")
			missingRange := attempt
			missingRange.RangeID = "missing"
			assertRejectedRangeAttemptDoesNotMutate(t, backend, missingRange, ErrUnknownWork, "missing range")
			invalidTask := attempt
			invalidTask.Task = TaskKey{Table: "items"}
			assertRejectedRangeAttemptDoesNotMutate(t, backend, invalidTask, nil, "invalid task key")

			if err := backend.CompleteRange("attempt-run", key, "0", "topology-a", 2, firstAt.Add(8*time.Second)); err != nil {
				t.Fatalf("complete range: %v", err)
			}
			if err := backend.CompleteWorkTask("attempt-run", key, "topology-a", firstAt.Add(9*time.Second)); err != nil {
				t.Fatalf("complete task: %v", err)
			}
			assertRejectedRangeAttemptDoesNotMutate(t, backend, attempt, ErrUnknownWork, "completed task")
		})
	}
}

func TestRangeAttemptConcurrentCallsLinearize(t *testing.T) {
	const calls = 16
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			backend, reopen := newRangeAttemptBackend(t, stateKind)
			key, _ := initializeRangeAttemptWork(t, backend, "concurrent-run")
			attempt := RangeAttempt{
				RunID:        "concurrent-run",
				Task:         key,
				RangeID:      "0",
				TopologyHash: "topology-a",
				Sequence:     0,
				At:           time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC),
			}
			start := make(chan struct{})
			errs := make(chan error, calls)
			var wait sync.WaitGroup
			for index := 0; index < calls; index++ {
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					errs <- backend.RecordRangeAttempt(attempt)
				}()
			}
			close(start)
			wait.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Errorf("record concurrent attempt: %v", err)
				}
			}
			if t.Failed() {
				return
			}
			assertRangeAttemptCounts(t, reopen(), "concurrent-run", calls, calls-1, calls)
		})
	}
}

func newRangeAttemptBackend(t *testing.T, stateKind string) (RangeBackend, func() RangeBackend) {
	t.Helper()
	var path string
	switch stateKind {
	case "sqlite":
		path = filepath.Join(t.TempDir(), "state.db")
		return SQLiteStore{Path: path}, func() RangeBackend { return SQLiteStore{Path: path} }
	case "yaml":
		path = filepath.Join(t.TempDir(), "state.yaml")
		return YAMLStore{Path: path}, func() RangeBackend { return YAMLStore{Path: path} }
	default:
		t.Fatalf("unknown state kind %q", stateKind)
		return nil, nil
	}
}

func initializeRangeAttemptWork(t *testing.T, backend RangeBackend, runID string) (TaskKey, RangeChunkIntent) {
	t.Helper()
	startedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	key := TaskKey{Type: "table-copy", Schema: "main", Table: "items"}
	task := WorkTask{
		RunID: runID, Key: key, Status: "running", Strategy: "integer_keyset",
		TopologyHash: "topology-a", StartedAt: startedAt,
	}
	created, err := backend.EnsureWorkPlan(task, []RangeState{{
		ID: "0", Lower: TypedTuple{Int64Value(0)}, Upper: TypedTuple{Int64Value(100)}, UpperInclusive: true,
	}})
	if err != nil || !created {
		t.Fatalf("ensure work plan created=%v err=%v", created, err)
	}
	intent := RangeChunkIntent{
		RunID: runID, Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5, EndFrontier: TypedTuple{Int64Value(5)}, FrontierValid: true, At: startedAt,
	}
	if err := backend.BeginRangeChunk(intent); err != nil {
		t.Fatalf("begin range intent: %v", err)
	}
	return key, intent
}

func rangeAttemptSnapshot(t *testing.T, backend RangeBackend, runID string) (WorkTask, RangeState) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatalf("list work: %v", err)
	}
	if len(tasks) != 1 || len(ranges) != 1 {
		t.Fatalf("work snapshot tasks=%#v ranges=%#v", tasks, ranges)
	}
	return tasks[0], ranges[0]
}

func assertRangeAttemptCounts(t *testing.T, backend RangeBackend, runID string, attempts, retries, pendingAttempts int) {
	t.Helper()
	task, workRange := rangeAttemptSnapshot(t, backend, runID)
	if task.Attempts != attempts || task.Retries != retries {
		t.Fatalf("task attempts/retries = %d/%d, want %d/%d", task.Attempts, task.Retries, attempts, retries)
	}
	if workRange.Attempts != attempts || workRange.Retries != retries {
		t.Fatalf("range attempts/retries = %d/%d, want %d/%d", workRange.Attempts, workRange.Retries, attempts, retries)
	}
	if len(workRange.Pending) != 1 || workRange.Pending[0].Attempts != pendingAttempts {
		t.Fatalf("pending attempts = %#v, want %d", workRange.Pending, pendingAttempts)
	}
}

func assertRejectedRangeAttemptDoesNotMutate(
	t *testing.T,
	backend RangeBackend,
	attempt RangeAttempt,
	want error,
	label string,
) {
	t.Helper()
	beforeTask, beforeRange := rangeAttemptSnapshot(t, backend, attempt.RunID)
	err := backend.RecordRangeAttempt(attempt)
	if want == nil {
		if err == nil {
			t.Fatalf("%s error = nil", label)
		}
	} else if !errors.Is(err, want) {
		t.Fatalf("%s error = %v, want %v", label, err, want)
	}
	afterTask, afterRange := rangeAttemptSnapshot(t, backend, attempt.RunID)
	if !reflect.DeepEqual(afterTask, beforeTask) || !reflect.DeepEqual(afterRange, beforeRange) {
		t.Fatalf("%s mutated state: before=%#v/%#v after=%#v/%#v", label, beforeTask, beforeRange, afterTask, afterRange)
	}
}

func assertRejectedRangeAcknowledgementDoesNotMutate(
	t *testing.T,
	backend RangeBackend,
	acknowledgement RangeAcknowledgement,
	want error,
	label string,
) {
	t.Helper()
	beforeTask, beforeRange := rangeAttemptSnapshot(t, backend, acknowledgement.RunID)
	if _, err := backend.AcknowledgeRange(acknowledgement); !errors.Is(err, want) {
		t.Fatalf("%s error = %v, want %v", label, err, want)
	}
	afterTask, afterRange := rangeAttemptSnapshot(t, backend, acknowledgement.RunID)
	if !reflect.DeepEqual(afterTask, beforeTask) || !reflect.DeepEqual(afterRange, beforeRange) {
		t.Fatalf(
			"%s mutated state: before=%#v/%#v after=%#v/%#v",
			label, beforeTask, beforeRange, afterTask, afterRange,
		)
	}
}
