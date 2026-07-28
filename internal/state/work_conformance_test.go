package state

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestRangeBackendConformance(t *testing.T) {
	backends := map[string]RangeBackend{
		"sqlite": SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")},
		"yaml":   YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	for name, backend := range backends {
		t.Run(name, func(t *testing.T) {
			testRangeBackend(t, backend)
		})
	}
}

func testRangeBackend(t *testing.T, backend RangeBackend) {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	key := TaskKey{Type: "table-copy", Schema: "main", Table: "items"}
	task := WorkTask{RunID: "run-1", Key: key, Strategy: "tuple_keyset", TopologyHash: "topology-a", StartedAt: now}
	ranges := []RangeState{
		{ID: "0", Lower: TypedTuple{Int64Value(-1)}, Upper: TypedTuple{Int64Value(9_007_199_254_740_993)}, UpperInclusive: true},
		{ID: "1", Lower: TypedTuple{Int64Value(9_007_199_254_740_993)}, Upper: TypedTuple{TextValue("z")}, UpperInclusive: true},
	}
	created, err := backend.EnsureWorkPlan(task, ranges)
	if err != nil || !created {
		t.Fatalf("EnsureWorkPlan created=%v err=%v", created, err)
	}
	if created, err := backend.EnsureWorkPlan(task, ranges); err != nil || created {
		t.Fatalf("idempotent ensure created=%v err=%v", created, err)
	}
	changed := task
	changed.TopologyHash = "topology-b"
	if _, err := backend.EnsureWorkPlan(changed, ranges); !errors.Is(err, ErrTopologyChanged) {
		t.Fatalf("topology error = %v", err)
	}
	if _, err := backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "missing", TopologyHash: "topology-a",
		ChunkRows: 1, DurableRows: 1,
	}); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown range error = %v", err)
	}

	if err := backend.BeginRangeChunk(RangeChunkIntent{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5,
		EndFrontier: TypedTuple{Int64Value(9_007_199_254_740_992)}, FrontierValid: true, At: now,
	}); err != nil {
		t.Fatal(err)
	}
	_, issued, err := backend.ListWork("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(issued[0].Pending) != 1 || issued[0].Pending[0].DurableRows != 0 {
		t.Fatalf("issued intent was not persisted: %#v", issued[0])
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, At: now,
	}); err != nil {
		t.Fatal(err)
	}

	// A later durable sequence is retained but cannot advance over sequence 0.
	if err := backend.BeginRangeChunk(RangeChunkIntent{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, ChunkRows: 3,
		EndFrontier: TypedTuple{Int64Value(9_007_199_254_740_993)}, FrontierValid: true, At: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, At: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	state, err := backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, ChunkRows: 3, DurableRows: 3,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_993)}, FrontierValid: true, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 0 || state.RowsDone != 0 {
		t.Fatalf("out-of-order acknowledgement advanced state: %#v", state)
	}
	state, err = backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5, DurableRows: 2,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_991)}, FrontierValid: true, At: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 0 || state.SequenceOffset != 2 || state.RowsDone != 2 {
		t.Fatalf("partial state = %#v", state)
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, At: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	state, err = backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5, AttemptOffset: 2, DurableRows: 3,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_992)}, FrontierValid: true, At: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 2 || state.SequenceOffset != 0 || state.RowsDone != 8 {
		t.Fatalf("contiguous state = %#v", state)
	}
	if err := backend.CompleteRange("run-1", key, "0", "topology-a", 2, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteRange("run-1", key, "1", "topology-a", 0, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteWorkTask("run-1", key, "topology-a", now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	tasks, restored, err := backend.ListWork("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "completed" || len(restored) != 2 {
		t.Fatalf("restored tasks=%#v ranges=%#v", tasks, restored)
	}
	value, err := restored[0].Upper[0].SQLValue()
	if err != nil || value != int64(9_007_199_254_740_993) {
		t.Fatalf("typed large integer = %#v, err=%v", value, err)
	}
}
