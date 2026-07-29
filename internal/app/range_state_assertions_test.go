package app

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/state"
)

func assertStage2PendingRangeIntent(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	expectedChunkRows int64,
) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "running" ||
		tasks[0].Attempts != 1 ||
		tasks[0].Retries != 0 {
		t.Fatalf("range tasks after write-before-ack kill = %#v", tasks)
	}
	if len(ranges) != 1 {
		t.Fatalf("ranges after write-before-ack kill = %#v", ranges)
	}
	workRange := ranges[0]
	if workRange.Status != "running" || workRange.RowsDone != 0 ||
		workRange.NextSequence != 0 || workRange.SequenceOffset != 0 ||
		workRange.Attempts != 1 || workRange.Retries != 0 ||
		len(workRange.Pending) != 1 ||
		workRange.Pending[0].Sequence != 0 ||
		workRange.Pending[0].ChunkRows != expectedChunkRows ||
		workRange.Pending[0].DurableRows != 0 ||
		workRange.Pending[0].Attempts != 1 {
		t.Fatalf("range after write-before-ack kill = %#v", workRange)
	}
}

func assertStage2CompletedRange(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	expectedRows int64,
) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "completed" {
		t.Fatalf("completed range tasks = %#v", tasks)
	}
	if len(ranges) != 1 || ranges[0].Status != "completed" ||
		ranges[0].RowsDone != expectedRows ||
		ranges[0].SequenceOffset != 0 || len(ranges[0].Pending) != 0 {
		t.Fatalf("completed ranges = %#v", ranges)
	}
}

func assertStage2RangeFrontier(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	expectedRows int64,
	expectedNextSequence uint64,
	expectedFrontier state.TypedValue,
) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "running" {
		t.Fatalf("range tasks at durable page boundary = %#v", tasks)
	}
	if len(ranges) != 1 {
		t.Fatalf("ranges at durable page boundary = %#v", ranges)
	}
	workRange := ranges[0]
	if workRange.Status != "running" ||
		workRange.RowsDone != expectedRows ||
		workRange.NextSequence != expectedNextSequence ||
		workRange.SequenceOffset != 0 ||
		len(workRange.Pending) != 0 ||
		!workRange.FrontierValid ||
		len(workRange.Frontier) != 1 ||
		workRange.Frontier[0] != expectedFrontier {
		t.Fatalf("range at durable page boundary = %#v", workRange)
	}
}
