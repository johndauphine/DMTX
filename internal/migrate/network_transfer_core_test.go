package migrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestResumableNetworkTransferSingleRangeHappyPath(t *testing.T) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	var mu sync.Mutex
	var issued []NetworkIssuedChunk
	var writes []NetworkWriteRequest
	var checkpoints []NetworkRangeCheckpoint
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			_ context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			if request.Sequence != 0 ||
				request.MaxRows != plan.Resources.ChunkRows.Value ||
				request.Attempt != 1 ||
				len(request.StartFrontier) != 0 ||
				request.ReplayExpected != nil {
				t.Fatalf("read request = %#v", request)
			}
			return networkTestPage(
				[][]any{{int64(1), "one"}, {int64(2), "two"}},
				[]int64{32, 32},
				"frontier-2",
				"digest-2",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			mu.Lock()
			writes = append(writes, cloneNetworkWriteRequest(request))
			mu.Unlock()
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			_ context.Context,
			value NetworkIssuedChunk,
		) error {
			mu.Lock()
			issued = append(issued, cloneNetworkIssuedChunk(value))
			mu.Unlock()
			return nil
		},
		Checkpoint: func(
			_ context.Context,
			value NetworkRangeCheckpoint,
		) error {
			mu.Lock()
			checkpoints = append(
				checkpoints,
				cloneNetworkCheckpoint(value),
			)
			mu.Unlock()
			return nil
		},
	}

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 ||
		result.CompletedRanges != 1 ||
		result.Memory.Current != 0 ||
		result.Memory.Peak <= 0 ||
		result.HasRuntimeTuning {
		t.Fatalf("result = %#v", result)
	}
	if !reflect.DeepEqual(result.Pagination, []NetworkPaginationFact{{
		RangeIndex:   0,
		TableSchema:  "public",
		TableName:    "events_0",
		TopologyHash: "topology-0",
		Pagination:   PaginationIntegerKeyset,
	}}) {
		t.Fatalf("pagination facts = %#v", result.Pagination)
	}
	if len(result.Retries) != 0 ||
		len(issued) != 1 ||
		issued[0].Rows != 2 ||
		issued[0].Sequence != 0 ||
		issued[0].Fingerprint != "digest-2" ||
		!issued[0].Exhausted {
		t.Fatalf("issued/retry facts = %#v / %#v", issued, result.Retries)
	}
	if len(writes) != 1 ||
		writes[0].Mode != NetworkWriteFreshInsert ||
		writes[0].Attempt != 0 ||
		writes[0].AttemptOffset != 0 ||
		len(writes[0].Rows) != 2 {
		t.Fatalf("writes = %#v", writes)
	}
	if len(checkpoints) != 2 ||
		checkpoints[0].Complete ||
		checkpoints[0].Frontier.NextSequence != 1 ||
		checkpoints[0].Frontier.SequenceOffset != 0 ||
		checkpoints[0].Frontier.Rows != 2 ||
		string(checkpoints[0].FrontierBytes) != "frontier-2" ||
		!checkpoints[1].Complete ||
		checkpoints[1].Frontier.NextSequence != 1 ||
		checkpoints[1].Frontier.Rows != 2 {
		t.Fatalf("checkpoints = %#v", checkpoints)
	}
}

func TestResumableNetworkTransferRetriesExactDurablePrefixSuffix(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	var writes []NetworkWriteRequest
	var checkpoints []NetworkRangeCheckpoint
	var mu sync.Mutex
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			return networkTestPage(
				[][]any{{1}, {2}, {3}, {4}},
				[]int64{16, 16, 16, 16},
				"frontier-4",
				"digest-4",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			mu.Lock()
			writes = append(writes, cloneNetworkWriteRequest(request))
			call := len(writes)
			mu.Unlock()
			if call == 1 {
				return WriteReceipt{
						Certainty:     CommitDurablePrefix,
						AttemptOffset: 0,
						AttemptedRows: 4,
						CommittedRows: 2,
					},
					NewTransferError(
						ErrorClassTransient,
						errors.New("transient prefix sentinel"),
					)
			}
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
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
	}

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 ||
		result.CompletedRanges != 1 ||
		len(result.Retries) != 1 ||
		result.Retries[0].Operation != NetworkRetryWrite ||
		result.Retries[0].Fact.Class != ErrorClassTransient {
		t.Fatalf("result = %#v", result)
	}
	if len(writes) != 2 ||
		writes[0].AttemptOffset != 0 ||
		len(writes[0].Rows) != 4 ||
		writes[1].AttemptOffset != 2 ||
		len(writes[1].Rows) != 2 ||
		writes[1].Attempt != 1 {
		t.Fatalf("suffix writes = %#v", writes)
	}
	if len(checkpoints) != 3 ||
		checkpoints[0].Frontier.NextSequence != 0 ||
		checkpoints[0].Frontier.SequenceOffset != 2 ||
		checkpoints[0].Frontier.Rows != 2 ||
		len(checkpoints[0].FrontierBytes) != 0 ||
		checkpoints[1].Frontier.NextSequence != 1 ||
		checkpoints[1].Frontier.SequenceOffset != 0 ||
		checkpoints[1].Frontier.Rows != 4 ||
		string(checkpoints[1].FrontierBytes) != "frontier-4" ||
		!checkpoints[2].Complete {
		t.Fatalf("prefix checkpoints = %#v", checkpoints)
	}
}

func TestResumableNetworkTransferUnknownCommitFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	plan.Resources.TargetMode = "upsert"
	plan.ReplayMode = NetworkReplayIdempotentUpsert
	var writes atomic.Int32
	var issued atomic.Int32
	var checkpoints atomic.Int32
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			return networkTestPage(
				[][]any{{1}, {2}},
				[]int64{16, 16},
				"frontier-2",
				"digest-2",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			writes.Add(1)
			return WriteReceipt{
				Certainty:     CommitUnknown,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
			}, io.ErrUnexpectedEOF
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			issued.Add(1)
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			checkpoints.Add(1)
			return nil
		},
	}

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if !errors.Is(err, ErrUnknownNetworkCommit) ||
		ClassifyTransferError(err) != ErrorClassPermanent {
		t.Fatalf("unknown commit error = %v", err)
	}
	if writes.Load() != 1 ||
		issued.Load() != 1 ||
		checkpoints.Load() != 0 ||
		result.Rows != 0 ||
		result.CompletedRanges != 0 ||
		result.Memory.Current != 0 ||
		len(result.Retries) != 1 ||
		result.Retries[0].Fact.Reason != "commit_outcome_unknown" {
		t.Fatalf(
			"unknown commit result=%#v writes=%d issued=%d checkpoints=%d",
			result,
			writes.Load(),
			issued.Load(),
			checkpoints.Load(),
		)
	}
}

func TestResumableNetworkTransferReplaysIssuedChunkWithExactMode(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		targetMode string
		replayMode NetworkReplayMode
		writeMode  NetworkWriteMode
	}{
		{
			name:       "rebuild insert-only",
			targetMode: "drop_recreate",
			replayMode: NetworkReplayDuplicateSafeInsertOnly,
			writeMode:  NetworkWriteDuplicateSafeInsertOnly,
		},
		{
			name:       "upsert idempotent",
			targetMode: "upsert",
			replayMode: NetworkReplayIdempotentUpsert,
			writeMode:  NetworkWriteIdempotentUpsert,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := networkTransferTestPlan(1)
			plan.Resources.TargetMode = test.targetMode
			plan.ReplayMode = test.replayMode
			plan.Restores = []NetworkRangeRestore{{
				RangeIndex:     0,
				TopologyHash:   "topology-0",
				NextSequence:   5,
				SequenceOffset: 2,
				RowsDone:       10,
				Frontier:       []byte("frontier-10"),
				Issued: []NetworkIssuedChunk{{
					RangeIndex:    0,
					Sequence:      5,
					Rows:          4,
					StartFrontier: []byte("frontier-10"),
					EndFrontier:   []byte("frontier-14"),
					Fingerprint:   "digest-14",
					Exhausted:     true,
				}},
			}}
			var recorded atomic.Int32
			var write NetworkWriteRequest
			callbacks := NetworkTransferCallbacks{
				ReadPage: func(
					_ context.Context,
					request NetworkReadRequest,
				) (NetworkReadPage, error) {
					if request.Sequence != 5 ||
						request.ReplayExpected == nil ||
						request.ReplayExpected.Fingerprint !=
							"digest-14" {
						t.Fatalf("replay request = %#v", request)
					}
					return networkTestPage(
						[][]any{{11}, {12}, {13}, {14}},
						[]int64{16, 16, 16, 16},
						"frontier-14",
						"digest-14",
						true,
					), nil
				},
				WritePage: func(
					_ context.Context,
					request NetworkWriteRequest,
				) (WriteReceipt, error) {
					write = cloneNetworkWriteRequest(request)
					return WriteReceipt{
						Certainty:     CommitDurable,
						AttemptOffset: request.AttemptOffset,
						AttemptedRows: int64(len(request.Rows)),
						CommittedRows: int64(len(request.Rows)),
					}, nil
				},
				RecordIssued: func(
					context.Context,
					NetworkIssuedChunk,
				) error {
					recorded.Add(1)
					return nil
				},
				Checkpoint: func(
					context.Context,
					NetworkRangeCheckpoint,
				) error {
					return nil
				},
			}
			result, err := RunResumableNetworkTransfer(
				context.Background(),
				plan,
				callbacks,
			)
			if err != nil {
				t.Fatal(err)
			}
			if recorded.Load() != 0 ||
				write.Sequence != 5 ||
				write.AttemptOffset != 2 ||
				len(write.Rows) != 2 ||
				write.Mode != test.writeMode ||
				result.Rows != 14 ||
				result.CompletedRanges != 1 {
				t.Fatalf(
					"replay write=%#v result=%#v recorded=%d",
					write,
					result,
					recorded.Load(),
				)
			}
		})
	}
}

func TestResumableNetworkTransferReadRetryUsesReadOnlyBoundary(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	var reads atomic.Int32
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			if reads.Add(1) == 1 {
				return NetworkReadPage{}, io.ErrUnexpectedEOF
			}
			return networkTestPage(
				[][]any{{1}},
				[]int64{16},
				"frontier-1",
				"digest-1",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			return nil
		},
	}
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 2 ||
		len(result.Retries) != 1 ||
		result.Retries[0].Operation != NetworkRetryRead ||
		result.Retries[0].Fact.Boundary != EngineRetryReadOnly ||
		result.Retries[0].Fact.Class != ErrorClassTransient {
		t.Fatalf("read retry result = %#v reads=%d", result, reads.Load())
	}
}

func TestResumableNetworkTransferAppliesRuntimeTuningAtWriteBoundaries(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	limits := RuntimeTuningLimits{
		ProtocolMaxChunkRows:         100,
		ProtocolMaxChunkBytes:        1 << 20,
		SafetyRowWidthUpperBound:     128,
		PlannedRanges:                1,
		ExpectedColumnCount:          2,
		HistoryLimit:                 16,
		GrowthAfterHealthyBoundaries: 100,
	}
	controller, err := NewRuntimeTuningController(
		plan.Resources,
		limits,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.RuntimeTuning = controller
	plan.RowWidth = RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: 2,
		ExpectedColumnCount: 2,
		UpperBoundBytes:     128,
	}
	var writes atomic.Int32
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			return networkTestPage(
				[][]any{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}},
				[]int64{32, 32, 32, 32},
				"frontier-4",
				"digest-4",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			call := writes.Add(1)
			if call == 1 {
				return WriteReceipt{
						Certainty:     CommitNotCommitted,
						AttemptOffset: request.AttemptOffset,
						AttemptedRows: int64(len(request.Rows)),
					},
					NewTransferError(
						ErrorClassTransient,
						errors.New("retryable tuning sentinel"),
					)
			}
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			return nil
		},
	}

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	history := controller.History()
	if writes.Load() != 3 ||
		result.Rows != 4 ||
		!result.HasRuntimeTuning ||
		result.RuntimeTuning.TotalDecisions != 3 ||
		len(history) != 3 ||
		history[0].Boundary.ChunkSequence != 0 ||
		history[0].Boundary.Attempt != 0 ||
		history[1].Boundary.ChunkSequence != 0 ||
		history[1].Boundary.Attempt != 1 ||
		history[2].Boundary.Attempt != 2 ||
		history[0].After.ChunkRows.Value != 2 ||
		history[0].After.Writers.Value != 1 {
		t.Fatalf(
			"runtime tuning result=%#v history=%#v writes=%d",
			result,
			history,
			writes.Load(),
		)
	}
	assertRuntimeDecisionReasons(
		t,
		history[0],
		RuntimeReasonWriteError,
	)
}

func TestResumableNetworkTransferRejectsPrepopulatedRuntimeTuningBeforeCallbacks(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	controller := attachNetworkTestRuntimeTuning(t, &plan, 100)
	_, err := controller.ApplyChunkBoundary(
		context.Background(),
		RuntimeTuningObservation{
			Boundary: RuntimeTuningBoundary{
				Ordinal:       1,
				TableSchema:   "public",
				TableName:     "events_0",
				RangeIndex:    0,
				ChunkSequence: 0,
			},
			ObservedRows:            1,
			ObservedBytes:           16,
			CumulativeObservedRows:  1,
			CumulativeObservedBytes: 16,
			Inventory: RuntimeResourceInventory{
				Complete:        true,
				PlannedRanges:   1,
				ConnectionLimit: plan.Resources.ConnectionLimit.Value,
				ByteBudget: ByteBudgetStats{
					Limit: plan.Resources.MemoryBudget.Value,
				},
			},
			RowWidth:     plan.RowWidth,
			WriteOutcome: RuntimeWriteSucceeded,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	var callbacks atomic.Int32
	invoked := func() {
		callbacks.Add(1)
	}
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				invoked()
				return NetworkReadPage{}, nil
			},
			WritePage: func(
				context.Context,
				NetworkWriteRequest,
			) (WriteReceipt, error) {
				invoked()
				return WriteReceipt{}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				invoked()
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				invoked()
				return nil
			},
		},
	)
	if !errors.Is(err, ErrInvalidNetworkTransferPlan) ||
		callbacks.Load() != 0 ||
		result.Rows != 0 ||
		result.CompletedRanges != 0 ||
		len(result.Pagination) != 0 ||
		len(result.Retries) != 0 ||
		result.Memory != (ByteBudgetStats{}) ||
		result.HasRuntimeTuning {
		t.Fatalf(
			"error=%v callbacks=%d result=%#v",
			err,
			callbacks.Load(),
			result,
		)
	}
}

func TestResumableNetworkTransferAppliesTypedProtocolLimitBoundary(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	controller := attachNetworkTestRuntimeTuning(t, &plan, 100)
	var requests []NetworkWriteRequest
	var requestMu sync.Mutex
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{1}, {2}, {3}, {4}},
					[]int64{16, 16, 16, 16},
					"frontier-4",
					"digest-4",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				requestMu.Lock()
				requests = append(
					requests,
					cloneNetworkWriteRequest(request),
				)
				call := len(requests)
				requestMu.Unlock()
				if call == 1 {
					return WriteReceipt{
							Certainty:     CommitNotCommitted,
							AttemptOffset: request.AttemptOffset,
							AttemptedRows: int64(len(request.Rows)),
						},
						fmt.Errorf(
							"password=protocol-secret: %w",
							&NetworkWriteProtocolLimitError{},
						)
				}
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
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
		t.Fatal(err)
	}
	requestMu.Lock()
	defer requestMu.Unlock()
	history := controller.History()
	controller.mu.Lock()
	protocolCap := controller.protocolFailureChunkCap
	controller.mu.Unlock()
	if result.Rows != 4 ||
		result.CompletedRanges != 1 ||
		len(result.Retries) != 1 ||
		result.Retries[0].Fact.Class != ErrorClassTransient ||
		result.Retries[0].Fact.Code != "protocol_limit" ||
		result.Retries[0].Fact.Reason != "runtime_chunk_reduction" ||
		strings.Contains(
			fmt.Sprintf("%#v", result.Retries),
			"protocol-secret",
		) ||
		len(requests) != 3 ||
		len(requests[0].Rows) != 4 ||
		requests[0].AttemptOffset != 0 ||
		len(requests[1].Rows) != 2 ||
		requests[1].AttemptOffset != 0 ||
		len(requests[2].Rows) != 2 ||
		requests[2].AttemptOffset != 2 ||
		len(history) != 3 ||
		history[0].After.ChunkRows.Value != 2 ||
		history[1].After.ChunkRows.Value != 2 ||
		history[2].After.ChunkRows.Value != 2 ||
		protocolCap != 2 {
		t.Fatalf(
			"result=%#v requests=%#v history=%#v protocol_cap=%d",
			result,
			requests,
			history,
			protocolCap,
		)
	}
	assertRuntimeDecisionReasons(
		t,
		history[0],
		RuntimeReasonProtocolWriteError,
	)
}

func TestResumableNetworkTransferCancellationDrainsAndReleases(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var issued atomic.Int32
	var activeWrites atomic.Int32
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			return networkTestPage(
				[][]any{{1}, {2}, {3}, {4}},
				[]int64{16, 16, 16, 16},
				"frontier-4",
				"digest-4",
				true,
			), nil
		},
		WritePage: func(
			writeCtx context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			activeWrites.Add(1)
			defer activeWrites.Add(-1)
			cancel()
			<-writeCtx.Done()
			return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
			}, writeCtx.Err()
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			issued.Add(1)
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			return nil
		},
	}

	result, err := RunResumableNetworkTransfer(
		ctx,
		plan,
		callbacks,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if issued.Load() != 1 ||
		activeWrites.Load() != 0 ||
		result.Rows != 0 ||
		result.Memory.Current != 0 {
		t.Fatalf(
			"cancellation result=%#v issued=%d active=%d",
			result,
			issued.Load(),
			activeWrites.Load(),
		)
	}
}

func TestResumableNetworkTransferHonorsReaderWriterAndStateLimits(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(4)
	var activeReaders atomic.Int32
	var maxReaders atomic.Int32
	var activeWriters atomic.Int32
	var maxWriters atomic.Int32
	var activeState atomic.Int32
	var maxState atomic.Int32
	readersReady := make(chan struct{})
	writersReady := make(chan struct{})
	var readersOnce sync.Once
	var writersOnce sync.Once
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			_ context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			active := activeReaders.Add(1)
			updateNetworkTestMaximum(&maxReaders, active)
			if active == int32(plan.Resources.Readers.Value) {
				readersOnce.Do(func() { close(readersReady) })
			}
			<-readersReady
			activeReaders.Add(-1)
			index := strconv.FormatUint(
				request.Range.RangeIndex,
				10,
			)
			return networkTestPage(
				[][]any{{request.Range.RangeIndex}},
				[]int64{16},
				"frontier-"+index,
				"digest-"+index,
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			active := activeWriters.Add(1)
			updateNetworkTestMaximum(&maxWriters, active)
			if active == int32(plan.Resources.Writers.Value) {
				writersOnce.Do(func() { close(writersReady) })
			}
			<-writersReady
			activeWriters.Add(-1)
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			active := activeState.Add(1)
			updateNetworkTestMaximum(&maxState, active)
			activeState.Add(-1)
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			active := activeState.Add(1)
			updateNetworkTestMaximum(&maxState, active)
			activeState.Add(-1)
			return nil
		},
	}

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 ||
		result.CompletedRanges != 4 ||
		maxReaders.Load() != 2 ||
		maxWriters.Load() != 2 ||
		maxState.Load() != 1 ||
		activeReaders.Load() != 0 ||
		activeWriters.Load() != 0 ||
		activeState.Load() != 0 ||
		result.Memory.Current != 0 ||
		result.Memory.Peak > result.Memory.Limit {
		t.Fatalf(
			"bounded result=%#v readers=%d writers=%d state=%d",
			result,
			maxReaders.Load(),
			maxWriters.Load(),
			maxState.Load(),
		)
	}
}

func TestResumableNetworkTransferSerializesOneRangeAndCheckpointsInOrder(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{}, 1)
	var checkpointMu sync.Mutex
	var checkpoints []NetworkRangeCheckpoint
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			_ context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			switch request.Sequence {
			case 0:
				return networkTestPage(
					[][]any{{1}, {2}},
					[]int64{16, 16},
					"frontier-2",
					"digest-2",
					false,
				), nil
			case 1:
				return networkTestPage(
					[][]any{{3}, {4}},
					[]int64{16, 16},
					"frontier-4",
					"digest-4",
					true,
				), nil
			default:
				t.Fatalf("unexpected sequence %d", request.Sequence)
				return NetworkReadPage{}, nil
			}
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			if request.Sequence == 0 {
				close(firstStarted)
				<-releaseFirst
			} else {
				secondStarted <- struct{}{}
			}
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			return nil
		},
		Checkpoint: func(
			_ context.Context,
			checkpoint NetworkRangeCheckpoint,
		) error {
			checkpointMu.Lock()
			checkpoints = append(
				checkpoints,
				cloneNetworkCheckpoint(checkpoint),
			)
			checkpointMu.Unlock()
			return nil
		},
	}
	type outcome struct {
		result NetworkTransferResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := RunResumableNetworkTransfer(
			context.Background(),
			plan,
			callbacks,
		)
		done <- outcome{result: result, err: err}
	}()
	<-firstStarted
	select {
	case <-secondStarted:
		t.Fatal("second chunk wrote before the first durable frontier")
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseFirst)
	finished := <-done
	if finished.err != nil {
		t.Fatal(finished.err)
	}
	if finished.result.Rows != 4 ||
		finished.result.CompletedRanges != 1 {
		t.Fatalf("result = %#v", finished.result)
	}
	checkpointMu.Lock()
	defer checkpointMu.Unlock()
	if len(checkpoints) != 3 ||
		checkpoints[0].Frontier.NextSequence != 1 ||
		checkpoints[0].Frontier.Rows != 2 ||
		string(checkpoints[0].FrontierBytes) != "frontier-2" ||
		checkpoints[1].Frontier.NextSequence != 2 ||
		checkpoints[1].Frontier.Rows != 4 ||
		string(checkpoints[1].FrontierBytes) != "frontier-4" ||
		!checkpoints[2].Complete {
		t.Fatalf("ordered checkpoints = %#v", checkpoints)
	}
}

func TestResumableNetworkTransferRejectsMalformedPlanBeforeCallbacks(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*NetworkTransferPlan)
		target error
	}{
		{
			name: "rebuild lacks insert-only replay",
			mutate: func(plan *NetworkTransferPlan) {
				plan.ReplayMode = NetworkReplayIdempotentUpsert
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "range ordinal skips zero",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Ranges[0].RangeIndex = 1
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "topology fact token is unsafe",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Ranges[0].TopologyHash =
					"password=secret\n"
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "connections cannot cover readers and writers",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Resources.ConnectionLimit.Value = 3
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "workers cannot cover readers and writers",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Resources.Workers.Value = 3
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "memory budget exceeds detected memory",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Resources.DetectedMemoryLimit.Value =
					plan.Resources.MemoryBudget.Value - 1
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "resource provenance is unknown",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Resources.ChunkRows.Provenance =
					config.SettingProvenance("unknown")
			},
			target: ErrInvalidNetworkTransferPlan,
		},
		{
			name: "restore topology differs",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "different-topology",
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "durable offset lacks issued chunk",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:     0,
					TopologyHash:   "topology-0",
					SequenceOffset: 1,
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "completed restore has rows without a completed sequence",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					RowsDone:     1,
					Frontier:     []byte("frontier-1"),
					Complete:     true,
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "incomplete restore has rows without a completed sequence",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					RowsDone:     1,
					Frontier:     []byte("frontier-1"),
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "completed restore has a sequence without rows",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					NextSequence: 1,
					Frontier:     []byte("frontier-1"),
					Complete:     true,
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "incomplete restore completed more sequences than rows",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					NextSequence: 2,
					RowsDone:     1,
					Frontier:     []byte("frontier-1"),
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "restore rows exceed completed sequence capacity",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					NextSequence: 1,
					RowsDone: int64(
						config.MaxTransferChunkRows,
					) + 1,
					Frontier: []byte("frontier-too-many"),
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "issued chunk does not extend frontier",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Restores = []NetworkRangeRestore{{
					RangeIndex:   0,
					TopologyHash: "topology-0",
					Frontier:     []byte("frontier-a"),
					Issued: []NetworkIssuedChunk{{
						RangeIndex:    0,
						Rows:          1,
						StartFrontier: []byte("frontier-b"),
						EndFrontier:   []byte("frontier-c"),
						Fingerprint:   "digest-c",
					}},
				}}
			},
			target: ErrInvalidNetworkRestore,
		},
		{
			name: "restored row totals overflow",
			mutate: func(plan *NetworkTransferPlan) {
				plan.Ranges = append(
					plan.Ranges,
					NetworkRangePlan{
						RangeIndex:   1,
						TableSchema:  "public",
						TableName:    "events_1",
						TopologyHash: "topology-1",
						Pagination:   PaginationIntegerKeyset,
						MaxRowBytes:  128,
					},
				)
				plan.Restores = []NetworkRangeRestore{
					{
						RangeIndex:   0,
						TopologyHash: "topology-0",
						NextSequence: 1,
						RowsDone:     math.MaxInt64,
						Frontier:     []byte("frontier-max"),
						Complete:     true,
					},
					{
						RangeIndex:   1,
						TopologyHash: "topology-1",
						NextSequence: 1,
						RowsDone:     1,
						Frontier:     []byte("frontier-1"),
						Complete:     true,
					},
				}
			},
			target: ErrInvalidNetworkRestore,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := networkTransferTestPlan(1)
			test.mutate(&plan)
			var calls atomic.Int32
			callback := func() {
				calls.Add(1)
			}
			_, err := RunResumableNetworkTransfer(
				context.Background(),
				plan,
				NetworkTransferCallbacks{
					ReadPage: func(
						context.Context,
						NetworkReadRequest,
					) (NetworkReadPage, error) {
						callback()
						return NetworkReadPage{}, nil
					},
					WritePage: func(
						context.Context,
						NetworkWriteRequest,
					) (WriteReceipt, error) {
						callback()
						return WriteReceipt{}, nil
					},
					RecordIssued: func(
						context.Context,
						NetworkIssuedChunk,
					) error {
						callback()
						return nil
					},
					Checkpoint: func(
						context.Context,
						NetworkRangeCheckpoint,
					) error {
						callback()
						return nil
					},
				},
			)
			if !errors.Is(err, test.target) ||
				calls.Load() != 0 ||
				strings.Contains(
					err.Error(),
					"password=secret",
				) {
				t.Fatalf(
					"error=%v calls=%d",
					err,
					calls.Load(),
				)
			}
		})
	}
}

func TestResumableNetworkTransferRejectsMalformedPagesBeforeMutation(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name string
		page NetworkReadPage
	}{
		{
			name: "empty nonterminal",
			page: NetworkReadPage{},
		},
		{
			name: "too many rows",
			page: networkTestPage(
				[][]any{{1}, {2}, {3}, {4}, {5}},
				[]int64{1, 1, 1, 1, 1},
				"frontier-5",
				"digest-5",
				true,
			),
		},
		{
			name: "row byte inventory missing",
			page: NetworkReadPage{
				Rows:          [][]any{{1}},
				RetainedBytes: 1,
				EndFrontier:   []byte("frontier-1"),
				Fingerprint:   "digest-1",
				Exhausted:     true,
			},
		},
		{
			name: "retained sum differs",
			page: NetworkReadPage{
				Rows:          [][]any{{1}},
				RowBytes:      []int64{1},
				RetainedBytes: 2,
				EndFrontier:   []byte("frontier-1"),
				Fingerprint:   "digest-1",
				Exhausted:     true,
			},
		},
		{
			name: "frontier does not advance",
			page: networkTestPage(
				[][]any{{1}},
				[]int64{1},
				"",
				"digest-1",
				true,
			),
		},
		{
			name: "fingerprint is unbounded input",
			page: networkTestPage(
				[][]any{{1}},
				[]int64{1},
				"frontier-1",
				"driver said password=secret",
				true,
			),
		},
		{
			name: "row exceeds planned reservation",
			page: networkTestPage(
				[][]any{{1}},
				[]int64{129},
				"frontier-1",
				"digest-1",
				true,
			),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := networkTransferTestPlan(1)
			var mutations atomic.Int32
			result, err := RunResumableNetworkTransfer(
				context.Background(),
				plan,
				NetworkTransferCallbacks{
					ReadPage: func(
						context.Context,
						NetworkReadRequest,
					) (NetworkReadPage, error) {
						return test.page, nil
					},
					WritePage: func(
						context.Context,
						NetworkWriteRequest,
					) (WriteReceipt, error) {
						mutations.Add(1)
						return WriteReceipt{}, nil
					},
					RecordIssued: func(
						context.Context,
						NetworkIssuedChunk,
					) error {
						mutations.Add(1)
						return nil
					},
					Checkpoint: func(
						context.Context,
						NetworkRangeCheckpoint,
					) error {
						mutations.Add(1)
						return nil
					},
				},
			)
			if !errors.Is(err, ErrInvalidNetworkPage) ||
				mutations.Load() != 0 ||
				result.Memory.Current != 0 ||
				strings.Contains(err.Error(), "password=secret") {
				t.Fatalf(
					"error=%v mutations=%d result=%#v",
					err,
					mutations.Load(),
					result,
				)
			}
		})
	}
}

func TestResumableNetworkTransferRejectsInvalidWriteReceipts(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		receipt  WriteReceipt
		writeErr error
	}{
		{
			name: "unknown certainty",
			receipt: WriteReceipt{
				Certainty:     CommitCertainty("claimed"),
				AttemptedRows: 2,
				CommittedRows: 2,
			},
		},
		{
			name: "attempt offset differs",
			receipt: WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: 1,
				AttemptedRows: 2,
				CommittedRows: 2,
			},
		},
		{
			name: "attempt row count differs",
			receipt: WriteReceipt{
				Certainty:     CommitDurable,
				AttemptedRows: 1,
				CommittedRows: 1,
			},
		},
		{
			name: "protocol limit claims a durable commit",
			receipt: WriteReceipt{
				Certainty:     CommitDurable,
				AttemptedRows: 2,
				CommittedRows: 2,
			},
			writeErr: fmt.Errorf(
				"password=protocol-secret: %w",
				&NetworkWriteProtocolLimitError{},
			),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := networkTransferTestPlan(1)
			var issued atomic.Int32
			var writes atomic.Int32
			var checkpoints atomic.Int32
			result, err := RunResumableNetworkTransfer(
				context.Background(),
				plan,
				NetworkTransferCallbacks{
					ReadPage: func(
						context.Context,
						NetworkReadRequest,
					) (NetworkReadPage, error) {
						return networkTestPage(
							[][]any{{1}, {2}},
							[]int64{16, 16},
							"frontier-2",
							"digest-2",
							true,
						), nil
					},
					WritePage: func(
						context.Context,
						NetworkWriteRequest,
					) (WriteReceipt, error) {
						writes.Add(1)
						return test.receipt, test.writeErr
					},
					RecordIssued: func(
						context.Context,
						NetworkIssuedChunk,
					) error {
						issued.Add(1)
						return nil
					},
					Checkpoint: func(
						context.Context,
						NetworkRangeCheckpoint,
					) error {
						checkpoints.Add(1)
						return nil
					},
				},
			)
			if !errors.Is(err, ErrInvalidWriteReceipt) ||
				ClassifyTransferError(err) != ErrorClassState ||
				issued.Load() != 1 ||
				writes.Load() != 1 ||
				checkpoints.Load() != 0 ||
				result.Rows != 0 ||
				result.Memory.Current != 0 ||
				strings.Contains(err.Error(), "protocol-secret") {
				t.Fatalf(
					"error=%v result=%#v issued=%d writes=%d checkpoints=%d",
					err,
					result,
					issued.Load(),
					writes.Load(),
					checkpoints.Load(),
				)
			}
		})
	}
}

func TestResumableNetworkTransferNotCommittedWithoutErrorFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	controller, err := NewRuntimeTuningController(
		plan.Resources,
		RuntimeTuningLimits{
			ProtocolMaxChunkRows:         100,
			ProtocolMaxChunkBytes:        1 << 20,
			SafetyRowWidthUpperBound:     128,
			PlannedRanges:                1,
			ExpectedColumnCount:          1,
			HistoryLimit:                 8,
			GrowthAfterHealthyBoundaries: 100,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.RuntimeTuning = controller
	plan.RowWidth = RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: 1,
		ExpectedColumnCount: 1,
		UpperBoundBytes:     128,
	}
	var checkpoints atomic.Int32
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{1}},
					[]int64{16},
					"frontier-1",
					"digest-1",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				return WriteReceipt{
					Certainty:     CommitNotCommitted,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				checkpoints.Add(1)
				return nil
			},
		},
	)
	controller.mu.Lock()
	outcome := controller.lastObservation.WriteOutcome
	controller.mu.Unlock()
	if !errors.Is(err, ErrInvalidWriteReceipt) ||
		ClassifyTransferError(err) != ErrorClassState ||
		checkpoints.Load() != 0 ||
		result.Rows != 0 ||
		result.Memory.Current != 0 ||
		outcome != RuntimeWriteFatalError {
		t.Fatalf(
			"error=%v result=%#v checkpoints=%d outcome=%q",
			err,
			result,
			checkpoints.Load(),
			outcome,
		)
	}
}

func TestResumableNetworkTransferDurableWriteErrorIsCheckpointedAndReturned(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	writeErr := NewTransferError(
		ErrorClassTransient,
		errors.New("durable write error sentinel"),
	)
	var writes atomic.Int32
	var checkpointsMu sync.Mutex
	var checkpoints []NetworkRangeCheckpoint
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{1}, {2}},
					[]int64{16, 16},
					"frontier-2",
					"digest-2",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				writes.Add(1)
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, writeErr
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				checkpoint NetworkRangeCheckpoint,
			) error {
				checkpointsMu.Lock()
				checkpoints = append(
					checkpoints,
					cloneNetworkCheckpoint(checkpoint),
				)
				checkpointsMu.Unlock()
				return nil
			},
		},
	)
	checkpointsMu.Lock()
	defer checkpointsMu.Unlock()
	if !errors.Is(err, writeErr) ||
		ClassifyTransferError(err) != ErrorClassTransient ||
		writes.Load() != 1 ||
		len(checkpoints) != 1 ||
		checkpoints[0].Complete ||
		checkpoints[0].Frontier.NextSequence != 1 ||
		checkpoints[0].Frontier.Rows != 2 ||
		result.Rows != 2 ||
		result.CompletedRanges != 0 ||
		result.Memory.Current != 0 ||
		len(result.Retries) != 1 {
		t.Fatalf(
			"error=%v result=%#v writes=%d checkpoints=%#v",
			err,
			result,
			writes.Load(),
			checkpoints,
		)
	}
}

func TestResumableNetworkTransferRetryBoundaryMatchesReplayMode(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name          string
		targetMode    string
		replayMode    NetworkReplayMode
		wantBoundary  EngineRetryBoundary
		wantClass     TransferErrorClass
		wantWrites    int32
		wantSucceeded bool
	}{
		{
			name:          "fresh rebuild does not replay interrupted insert",
			targetMode:    "drop_recreate",
			replayMode:    NetworkReplayDuplicateSafeInsertOnly,
			wantBoundary:  EngineRetryRolledBack,
			wantClass:     ErrorClassPermanent,
			wantWrites:    1,
			wantSucceeded: false,
		},
		{
			name:          "idempotent upsert can replay interrupted write",
			targetMode:    "upsert",
			replayMode:    NetworkReplayIdempotentUpsert,
			wantBoundary:  EngineRetryIdempotent,
			wantClass:     ErrorClassTransient,
			wantWrites:    2,
			wantSucceeded: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := networkTransferTestPlan(1)
			plan.Resources.TargetMode = test.targetMode
			plan.ReplayMode = test.replayMode
			var writes atomic.Int32
			result, err := RunResumableNetworkTransfer(
				context.Background(),
				plan,
				NetworkTransferCallbacks{
					ReadPage: func(
						context.Context,
						NetworkReadRequest,
					) (NetworkReadPage, error) {
						return networkTestPage(
							[][]any{{1}, {2}},
							[]int64{16, 16},
							"frontier-2",
							"digest-2",
							true,
						), nil
					},
					WritePage: func(
						_ context.Context,
						request NetworkWriteRequest,
					) (WriteReceipt, error) {
						if writes.Add(1) == 1 {
							return WriteReceipt{
								Certainty:     CommitNotCommitted,
								AttemptOffset: request.AttemptOffset,
								AttemptedRows: int64(len(request.Rows)),
							}, io.ErrUnexpectedEOF
						}
						return WriteReceipt{
							Certainty:     CommitDurable,
							AttemptOffset: request.AttemptOffset,
							AttemptedRows: int64(len(request.Rows)),
							CommittedRows: int64(len(request.Rows)),
						}, nil
					},
					RecordIssued: func(
						context.Context,
						NetworkIssuedChunk,
					) error {
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
			if (err == nil) != test.wantSucceeded ||
				writes.Load() != test.wantWrites ||
				len(result.Retries) != 1 ||
				result.Retries[0].Fact.Boundary !=
					test.wantBoundary ||
				result.Retries[0].Fact.Class != test.wantClass ||
				result.Memory.Current != 0 {
				t.Fatalf(
					"error=%v result=%#v writes=%d",
					err,
					result,
					writes.Load(),
				)
			}
			if test.wantSucceeded {
				if result.Rows != 2 ||
					result.CompletedRanges != 1 {
					t.Fatalf("successful result = %#v", result)
				}
			} else if result.Rows != 0 ||
				result.CompletedRanges != 0 ||
				ClassifyTransferError(err) !=
					ErrorClassPermanent {
				t.Fatalf("failed result=%#v error=%v", result, err)
			}
		})
	}
}

func TestResumableNetworkTransferRejectsChangedIssuedReplay(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	plan.Restores = []NetworkRangeRestore{{
		RangeIndex:   0,
		TopologyHash: "topology-0",
		NextSequence: 5,
		RowsDone:     10,
		Frontier:     []byte("frontier-10"),
		Issued: []NetworkIssuedChunk{{
			RangeIndex:    0,
			Sequence:      5,
			Rows:          2,
			StartFrontier: []byte("frontier-10"),
			EndFrontier:   []byte("frontier-12"),
			Fingerprint:   "digest-12",
			Exhausted:     true,
		}},
	}}
	var mutations atomic.Int32
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{11}, {12}},
					[]int64{16, 16},
					"frontier-12",
					"digest-changed",
					true,
				), nil
			},
			WritePage: func(
				context.Context,
				NetworkWriteRequest,
			) (WriteReceipt, error) {
				mutations.Add(1)
				return WriteReceipt{}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				mutations.Add(1)
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				mutations.Add(1)
				return nil
			},
		},
	)
	if !errors.Is(err, ErrInvalidNetworkPage) ||
		ClassifyTransferError(err) != ErrorClassState ||
		mutations.Load() != 0 ||
		result.Rows != 10 ||
		result.CompletedRanges != 0 ||
		result.Memory.Current != 0 {
		t.Fatalf(
			"error=%v result=%#v mutations=%d",
			err,
			result,
			mutations.Load(),
		)
	}
}

func TestResumableNetworkTransferSharesOneByteBudgetAcrossReaders(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(2)
	const memory = int64(config.MinimumTransferMemoryBytes)
	plan.Resources.DetectedMemoryLimit.Value = memory
	plan.Resources.MemoryBudget.Value = memory
	for index := range plan.Ranges {
		plan.Ranges[index].MaxRowBytes = memory / 4
	}
	var activeReaders atomic.Int32
	var maxReaders atomic.Int32
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				_ context.Context,
				request NetworkReadRequest,
			) (NetworkReadPage, error) {
				active := activeReaders.Add(1)
				updateNetworkTestMaximum(&maxReaders, active)
				time.Sleep(5 * time.Millisecond)
				activeReaders.Add(-1)
				index := strconv.FormatUint(
					request.Range.RangeIndex,
					10,
				)
				return networkTestPage(
					[][]any{{request.Range.RangeIndex}},
					[]int64{16},
					"frontier-"+index,
					"digest-"+index,
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
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
		t.Fatal(err)
	}
	if result.Rows != 2 ||
		result.CompletedRanges != 2 ||
		result.Memory.Limit != memory ||
		result.Memory.Peak != memory ||
		result.Memory.Current != 0 ||
		maxReaders.Load() != 1 ||
		activeReaders.Load() != 0 {
		t.Fatalf(
			"result=%#v maxReaders=%d activeReaders=%d",
			result,
			maxReaders.Load(),
			activeReaders.Load(),
		)
	}
}

func TestResumableNetworkTransferTransfersWideBinaryOwnershipWithoutCopies(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	const retainedBytes = int64(1 << 20)
	plan.Resources.ChunkRows.Value = 1
	plan.Ranges[0].MaxRowBytes = retainedBytes
	payload := make([]byte, retainedBytes)
	payload[0] = 0x11
	payload[len(payload)-1] = 0xee
	payloadBacking := &payload[0]
	payloadLength := len(payload)
	var sameBacking atomic.Bool
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				ownedPayload := payload
				payload = nil
				return networkTestPage(
					[][]any{{ownedPayload}},
					[]int64{retainedBytes},
					"frontier-wide",
					"digest-wide",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				binary, ok := request.Rows[0][0].([]byte)
				sameBacking.Store(
					ok &&
						len(binary) == payloadLength &&
						&binary[0] == payloadBacking &&
						binary[0] == 0x11 &&
						binary[len(binary)-1] == 0xee,
				)
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
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
		t.Fatal(err)
	}
	if !sameBacking.Load() ||
		result.Rows != 1 ||
		result.CompletedRanges != 1 ||
		result.Memory.Peak != retainedBytes ||
		result.Memory.Current != 0 {
		t.Fatalf(
			"same_backing=%t result=%#v",
			sameBacking.Load(),
			result,
		)
	}
}

func TestResumableNetworkTransferRecordIssuedFailurePreventsMutation(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	stateErr := errors.New("record issued failure sentinel")
	var writes atomic.Int32
	var checkpoints atomic.Int32
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{1}},
					[]int64{16},
					"frontier-1",
					"digest-1",
					true,
				), nil
			},
			WritePage: func(
				context.Context,
				NetworkWriteRequest,
			) (WriteReceipt, error) {
				writes.Add(1)
				return WriteReceipt{}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				return stateErr
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				checkpoints.Add(1)
				return nil
			},
		},
	)
	if !errors.Is(err, stateErr) ||
		ClassifyTransferError(err) != ErrorClassState ||
		writes.Load() != 0 ||
		checkpoints.Load() != 0 ||
		result.Rows != 0 ||
		result.Memory.Current != 0 {
		t.Fatalf(
			"error=%v result=%#v writes=%d checkpoints=%d",
			err,
			result,
			writes.Load(),
			checkpoints.Load(),
		)
	}
}

func TestResumableNetworkTransferCheckpointFailureLeavesIssuedReplay(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	checkpointErr := errors.New("checkpoint failure sentinel")
	var issued atomic.Int32
	var writes atomic.Int32
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			context.Context,
			NetworkReadRequest,
		) (NetworkReadPage, error) {
			return networkTestPage(
				[][]any{{1}, {2}},
				[]int64{16, 16},
				"frontier-2",
				"digest-2",
				true,
			), nil
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			writes.Add(1)
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			context.Context,
			NetworkIssuedChunk,
		) error {
			issued.Add(1)
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			return checkpointErr
		},
	}
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		callbacks,
	)
	if !errors.Is(err, checkpointErr) ||
		issued.Load() != 1 ||
		writes.Load() != 1 ||
		result.Rows != 0 ||
		result.CompletedRanges != 0 ||
		result.Memory.Current != 0 {
		t.Fatalf(
			"checkpoint failure error=%v result=%#v issued=%d writes=%d",
			err,
			result,
			issued.Load(),
			writes.Load(),
		)
	}
}

func networkTransferTestPlan(
	rangeCount int,
) NetworkTransferPlan {
	const memory = int64(8 << 20)
	ranges := make([]NetworkRangePlan, rangeCount)
	for index := range ranges {
		ranges[index] = NetworkRangePlan{
			RangeIndex:   uint64(index),
			TableSchema:  "public",
			TableName:    "events_" + strconv.Itoa(index),
			TopologyHash: "topology-" + strconv.Itoa(index),
			Pagination:   PaginationIntegerKeyset,
			MaxRowBytes:  128,
		}
	}
	return NetworkTransferPlan{
		SourceEngine: "postgres",
		TargetEngine: "postgres",
		Resources: config.EffectiveTransferPlan{
			TargetMode: "drop_recreate",
			ConnectionLimit: config.EffectiveInt{
				Value: 4, Provenance: config.ProvenanceDerived,
			},
			DetectedMemoryLimit: config.EffectiveBytes{
				Value:      memory,
				Provenance: config.ProvenanceHostAvailable,
			},
			MemoryBudget: config.EffectiveBytes{
				Value:      memory,
				Provenance: config.ProvenanceHostAvailable,
			},
			Workers: config.EffectiveInt{
				Value: 4, Provenance: config.ProvenanceDerived,
			},
			Readers: config.EffectiveInt{
				Value: 2, Provenance: config.ProvenanceDerived,
			},
			Writers: config.EffectiveInt{
				Value: 2, Provenance: config.ProvenanceDerived,
			},
			QueueDepth: config.EffectiveInt{
				Value: 2, Provenance: config.ProvenanceDerived,
			},
			ChunkRows: config.EffectiveInt{
				Value: 4, Provenance: config.ProvenanceDerived,
			},
		},
		RetryPolicy: RetryPolicy{
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
		ReplayMode: NetworkReplayDuplicateSafeInsertOnly,
		Ranges:     ranges,
	}
}

func updateNetworkTestMaximum(
	maximum *atomic.Int32,
	value int32,
) {
	for {
		current := maximum.Load()
		if value <= current || maximum.CompareAndSwap(current, value) {
			return
		}
	}
}

func networkTestPage(
	rows [][]any,
	rowBytes []int64,
	frontier string,
	fingerprint string,
	exhausted bool,
) NetworkReadPage {
	var retained int64
	for _, value := range rowBytes {
		retained += value
	}
	return NetworkReadPage{
		Rows:          rows,
		RowBytes:      rowBytes,
		RetainedBytes: retained,
		EndFrontier:   []byte(frontier),
		Fingerprint:   fingerprint,
		Exhausted:     exhausted,
	}
}

func attachNetworkTestRuntimeTuning(
	t *testing.T,
	plan *NetworkTransferPlan,
	growthAfterHealthyBoundaries uint64,
) *RuntimeTuningController {
	t.Helper()
	controller, err := NewRuntimeTuningController(
		plan.Resources,
		RuntimeTuningLimits{
			ProtocolMaxChunkRows:         100,
			ProtocolMaxChunkBytes:        1 << 20,
			SafetyRowWidthUpperBound:     128,
			PlannedRanges:                uint64(len(plan.Ranges)),
			ExpectedColumnCount:          1,
			HistoryLimit:                 16,
			GrowthAfterHealthyBoundaries: growthAfterHealthyBoundaries,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.RuntimeTuning = controller
	plan.RowWidth = RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: 1,
		ExpectedColumnCount: 1,
		UpperBoundBytes:     128,
	}
	return controller
}

func cloneNetworkWriteRequest(
	request NetworkWriteRequest,
) NetworkWriteRequest {
	request.Rows = cloneNetworkTestRows(request.Rows)
	return request
}

func cloneNetworkTestRows(rows [][]any) [][]any {
	result := make([][]any, len(rows))
	for rowIndex, row := range rows {
		result[rowIndex] = make([]any, len(row))
		for columnIndex, value := range row {
			if binary, ok := value.([]byte); ok {
				result[rowIndex][columnIndex] =
					append([]byte(nil), binary...)
				continue
			}
			result[rowIndex][columnIndex] = value
		}
	}
	return result
}

func cloneNetworkCheckpoint(
	checkpoint NetworkRangeCheckpoint,
) NetworkRangeCheckpoint {
	checkpoint.FrontierBytes =
		cloneNetworkBytes(checkpoint.FrontierBytes)
	return checkpoint
}
