package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

func TestResumableNetworkTransferCheckpointFrequencyPersistsContiguousFrontier(
	t *testing.T,
) {
	t.Parallel()

	plan := networkCheckpointFrequencyTestPlan(2)
	var mu sync.Mutex
	checkpoints := make([]NetworkRangeCheckpoint, 0, 2)
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: networkCheckpointFrequencyWritePage,
			RecordIssued: func(context.Context, NetworkIssuedChunk) error {
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				checkpoint NetworkRangeCheckpoint,
			) error {
				mu.Lock()
				checkpoints = append(
					checkpoints,
					cloneNetworkCheckpoint(checkpoint),
				)
				mu.Unlock()
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RunResumableNetworkTransfer: %v", err)
	}
	if result.Rows != 3 || result.CompletedRanges != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(checkpoints) != 2 {
		t.Fatalf("checkpoint count = %d, want periodic plus final", len(checkpoints))
	}
	if got := checkpoints[0]; got.Complete ||
		got.Frontier.NextSequence != 2 ||
		got.Frontier.SequenceOffset != 0 ||
		got.Frontier.Rows != 2 ||
		string(got.FrontierBytes) != "frontier-2" {
		t.Fatalf("periodic checkpoint = %#v", got)
	}
	if got := checkpoints[1]; !got.Complete ||
		got.Frontier.NextSequence != 3 ||
		got.Frontier.SequenceOffset != 0 ||
		got.Frontier.Rows != 3 ||
		string(got.FrontierBytes) != "frontier-3" {
		t.Fatalf("final checkpoint = %#v", got)
	}
}

func TestResumableNetworkTransferCheckpointFrequencyDefaultKeepsImmediateCadence(
	t *testing.T,
) {
	t.Parallel()

	plan := networkCheckpointFrequencyTestPlan(0)
	var mu sync.Mutex
	checkpoints := make([]NetworkRangeCheckpoint, 0, 4)
	_, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: networkCheckpointFrequencyWritePage,
			RecordIssued: func(context.Context, NetworkIssuedChunk) error {
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				checkpoint NetworkRangeCheckpoint,
			) error {
				mu.Lock()
				checkpoints = append(
					checkpoints,
					cloneNetworkCheckpoint(checkpoint),
				)
				mu.Unlock()
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RunResumableNetworkTransfer: %v", err)
	}
	if len(checkpoints) != 4 {
		t.Fatalf("checkpoint count = %d, want one per ack plus final", len(checkpoints))
	}
	for index := 0; index < 3; index++ {
		checkpoint := checkpoints[index]
		if checkpoint.Complete ||
			checkpoint.Frontier.NextSequence != uint64(index+1) ||
			checkpoint.Frontier.Rows != int64(index+1) {
			t.Fatalf("immediate checkpoint %d = %#v", index, checkpoint)
		}
	}
	if !checkpoints[3].Complete ||
		checkpoints[3].Frontier.NextSequence != 3 ||
		checkpoints[3].Frontier.Rows != 3 {
		t.Fatalf("final checkpoint = %#v", checkpoints[3])
	}
}

func TestResumableNetworkTransferCheckpointFrequencyStateFailureReplaysIssuedWork(
	t *testing.T,
) {
	t.Parallel()

	firstPlan := networkCheckpointFrequencyTestPlan(2)
	checkpointErr := errors.New("periodic checkpoint failure")
	var mu sync.Mutex
	issued := make([]NetworkIssuedChunk, 0, 3)
	failedCheckpoints := make([]NetworkRangeCheckpoint, 0, 1)
	durableRows := make(map[int64]string)
	write := func(
		_ context.Context,
		request NetworkWriteRequest,
	) (WriteReceipt, error) {
		if request.Mode != NetworkWriteIdempotentUpsert {
			return WriteReceipt{}, fmt.Errorf("write mode = %q", request.Mode)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, row := range request.Rows {
			key, ok := row[0].(int64)
			if !ok {
				return WriteReceipt{}, fmt.Errorf("key = %#v", row[0])
			}
			value, ok := row[1].(string)
			if !ok {
				return WriteReceipt{}, fmt.Errorf("value = %#v", row[1])
			}
			if existing, found := durableRows[key]; found && existing != value {
				return WriteReceipt{}, fmt.Errorf(
					"replay changed row %d from %q to %q",
					key,
					existing,
					value,
				)
			}
			durableRows[key] = value
		}
		return WriteReceipt{
			Certainty:     CommitDurable,
			AttemptOffset: request.AttemptOffset,
			AttemptedRows: int64(len(request.Rows)),
			CommittedRows: int64(len(request.Rows)),
		}, nil
	}
	firstResult, err := RunResumableNetworkTransfer(
		context.Background(),
		firstPlan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: write,
			RecordIssued: func(
				_ context.Context,
				chunk NetworkIssuedChunk,
			) error {
				mu.Lock()
				issued = append(issued, cloneNetworkIssuedChunk(chunk))
				mu.Unlock()
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				checkpoint NetworkRangeCheckpoint,
			) error {
				mu.Lock()
				failedCheckpoints = append(
					failedCheckpoints,
					cloneNetworkCheckpoint(checkpoint),
				)
				mu.Unlock()
				return checkpointErr
			},
		},
	)
	if !errors.Is(err, checkpointErr) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("first transfer error = %v", err)
	}
	if firstResult.Rows != 0 || firstResult.CompletedRanges != 0 {
		t.Fatalf("failed result advanced durable progress: %#v", firstResult)
	}
	if len(failedCheckpoints) != 1 ||
		failedCheckpoints[0].Complete ||
		failedCheckpoints[0].Frontier.NextSequence != 2 ||
		failedCheckpoints[0].Frontier.Rows != 2 {
		t.Fatalf("unsafe failed checkpoint = %#v", failedCheckpoints)
	}

	mu.Lock()
	restoreIssued := make([]NetworkIssuedChunk, len(issued))
	for index, chunk := range issued {
		restoreIssued[index] = cloneNetworkIssuedChunk(chunk)
	}
	mu.Unlock()
	sort.Slice(restoreIssued, func(left, right int) bool {
		return restoreIssued[left].Sequence < restoreIssued[right].Sequence
	})
	if len(restoreIssued) < 2 || restoreIssued[0].Sequence != 0 {
		t.Fatalf("durable issued work = %#v", restoreIssued)
	}

	resumePlan := networkCheckpointFrequencyTestPlan(2)
	resumePlan.Restores = []NetworkRangeRestore{{
		RangeIndex:   0,
		TopologyHash: resumePlan.Ranges[0].TopologyHash,
		Issued:       restoreIssued,
	}}
	var finalCheckpoints atomic.Int32
	resumeResult, err := RunResumableNetworkTransfer(
		context.Background(),
		resumePlan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: write,
			RecordIssued: func(context.Context, NetworkIssuedChunk) error {
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				finalCheckpoints.Add(1)
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("resume transfer: %v", err)
	}
	if resumeResult.Rows != 3 || resumeResult.CompletedRanges != 1 ||
		finalCheckpoints.Load() < 2 {
		t.Fatalf(
			"resume result=%#v final checkpoints=%d",
			resumeResult,
			finalCheckpoints.Load(),
		)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(durableRows) != 3 ||
		durableRows[1] != "value-1" ||
		durableRows[2] != "value-2" ||
		durableRows[3] != "value-3" {
		t.Fatalf("replayed durable rows = %#v", durableRows)
	}
}

func TestResumableNetworkTransferCheckpointFrequencyResumesFromPeriodicFrontier(
	t *testing.T,
) {
	t.Parallel()

	firstPlan := networkCheckpointFrequencyTestPlan(2)
	finalCheckpointErr := errors.New("final checkpoint failure")
	var mu sync.Mutex
	issued := make([]NetworkIssuedChunk, 0, 3)
	durableRows := make(map[int64]string)
	write := func(
		_ context.Context,
		request NetworkWriteRequest,
	) (WriteReceipt, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, row := range request.Rows {
			key, ok := row[0].(int64)
			if !ok {
				return WriteReceipt{}, fmt.Errorf("key = %#v", row[0])
			}
			value, ok := row[1].(string)
			if !ok {
				return WriteReceipt{}, fmt.Errorf("value = %#v", row[1])
			}
			if existing, found := durableRows[key]; found && existing != value {
				return WriteReceipt{}, fmt.Errorf(
					"replay changed row %d from %q to %q",
					key,
					existing,
					value,
				)
			}
			durableRows[key] = value
		}
		return WriteReceipt{
			Certainty:     CommitDurable,
			AttemptOffset: request.AttemptOffset,
			AttemptedRows: int64(len(request.Rows)),
			CommittedRows: int64(len(request.Rows)),
		}, nil
	}
	firstResult, err := RunResumableNetworkTransfer(
		context.Background(),
		firstPlan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: write,
			RecordIssued: func(
				_ context.Context,
				chunk NetworkIssuedChunk,
			) error {
				mu.Lock()
				issued = append(issued, cloneNetworkIssuedChunk(chunk))
				mu.Unlock()
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				checkpoint NetworkRangeCheckpoint,
			) error {
				if checkpoint.Complete {
					return finalCheckpointErr
				}
				if checkpoint.Frontier.NextSequence != 2 ||
					checkpoint.Frontier.Rows != 2 ||
					string(checkpoint.FrontierBytes) != "frontier-2" {
					return fmt.Errorf("periodic checkpoint = %#v", checkpoint)
				}
				return nil
			},
		},
	)
	if !errors.Is(err, finalCheckpointErr) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("first transfer error = %v", err)
	}
	if firstResult.Rows != 2 || firstResult.CompletedRanges != 0 {
		t.Fatalf("first result = %#v", firstResult)
	}

	mu.Lock()
	restoreIssued := make([]NetworkIssuedChunk, 0, 1)
	for _, chunk := range issued {
		if chunk.Sequence >= 2 {
			restoreIssued = append(
				restoreIssued,
				cloneNetworkIssuedChunk(chunk),
			)
		}
	}
	mu.Unlock()
	if len(restoreIssued) != 1 || restoreIssued[0].Sequence != 2 {
		t.Fatalf("unacknowledged issued suffix = %#v", restoreIssued)
	}

	resumePlan := networkCheckpointFrequencyTestPlan(2)
	resumePlan.Restores = []NetworkRangeRestore{{
		RangeIndex:   0,
		TopologyHash: resumePlan.Ranges[0].TopologyHash,
		NextSequence: 2,
		RowsDone:     2,
		Frontier:     []byte("frontier-2"),
		Issued:       restoreIssued,
	}}
	resumeResult, err := RunResumableNetworkTransfer(
		context.Background(),
		resumePlan,
		NetworkTransferCallbacks{
			ReadPage:  networkCheckpointFrequencyReadPage,
			WritePage: write,
			RecordIssued: func(context.Context, NetworkIssuedChunk) error {
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("resume transfer: %v", err)
	}
	if resumeResult.Rows != 3 || resumeResult.CompletedRanges != 1 {
		t.Fatalf("resume result = %#v", resumeResult)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(durableRows) != 3 ||
		durableRows[1] != "value-1" ||
		durableRows[2] != "value-2" ||
		durableRows[3] != "value-3" {
		t.Fatalf("replayed durable rows = %#v", durableRows)
	}
}

func TestResumableNetworkTransferRejectsUnboundedCheckpointFrequencyBeforeCallbacks(
	t *testing.T,
) {
	t.Parallel()

	plan := networkCheckpointFrequencyTestPlan(
		maximumNetworkCheckpointFrequency + 1,
	)
	var callbacks atomic.Int32
	_, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				callbacks.Add(1)
				return NetworkReadPage{}, nil
			},
			WritePage: func(
				context.Context,
				NetworkWriteRequest,
			) (WriteReceipt, error) {
				callbacks.Add(1)
				return WriteReceipt{}, nil
			},
			RecordIssued: func(context.Context, NetworkIssuedChunk) error {
				callbacks.Add(1)
				return nil
			},
			Checkpoint: func(context.Context, NetworkRangeCheckpoint) error {
				callbacks.Add(1)
				return nil
			},
		},
	)
	if !errors.Is(err, ErrInvalidNetworkTransferPlan) || callbacks.Load() != 0 {
		t.Fatalf("error=%v callbacks=%d", err, callbacks.Load())
	}
}

func networkCheckpointFrequencyTestPlan(
	frequency int,
) NetworkTransferPlan {
	plan := networkTransferTestPlan(1)
	plan.Resources.TargetMode = "upsert"
	plan.Resources.ChunkRows.Value = 1
	plan.Resources.Readers.Value = 1
	plan.Resources.Writers.Value = 1
	plan.Resources.QueueDepth.Value = 1
	plan.Resources.Workers.Value = 2
	plan.Resources.ConnectionLimit.Value = 2
	plan.ReplayMode = NetworkReplayIdempotentUpsert
	plan.CheckpointFrequency = frequency
	return plan
}

func networkCheckpointFrequencyReadPage(
	_ context.Context,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if request.Range.RangeIndex != 0 || request.Sequence > 2 ||
		request.MaxRows != 1 {
		return NetworkReadPage{}, fmt.Errorf("unexpected read request %#v", request)
	}
	wantStart := []byte(nil)
	if request.Sequence != 0 {
		wantStart = []byte(fmt.Sprintf("frontier-%d", request.Sequence))
	}
	if !bytes.Equal(request.StartFrontier, wantStart) {
		return NetworkReadPage{}, fmt.Errorf(
			"sequence %d starts at %q, want %q",
			request.Sequence,
			request.StartFrontier,
			wantStart,
		)
	}
	ordinal := request.Sequence + 1
	return networkTestPage(
		[][]any{{int64(ordinal), fmt.Sprintf("value-%d", ordinal)}},
		[]int64{32},
		fmt.Sprintf("frontier-%d", ordinal),
		fmt.Sprintf("digest-%d", ordinal),
		request.Sequence == 2,
	), nil
}

func networkCheckpointFrequencyWritePage(
	_ context.Context,
	request NetworkWriteRequest,
) (WriteReceipt, error) {
	if len(request.Rows) != 1 ||
		request.Mode != NetworkWriteIdempotentUpsert {
		return WriteReceipt{}, fmt.Errorf("unexpected write request %#v", request)
	}
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptOffset: request.AttemptOffset,
		AttemptedRows: int64(len(request.Rows)),
		CommittedRows: int64(len(request.Rows)),
	}, nil
}
