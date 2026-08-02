package migrate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// runtimeTuningHistoryRecordingSink is deliberately small: the production
// SQLite sink has its own state conformance tests, while these core tests prove
// the transfer ordering which makes a durable decision useful for recovery.
type runtimeTuningHistoryRecordingSink struct {
	mu        sync.Mutex
	snapshots []RuntimeTuningSnapshot
	decisions []RuntimeTuningDecision
	onPersist func()
	err       error
}

func (sink *runtimeTuningHistoryRecordingSink) PersistRuntimeTuningDecision(
	_ context.Context,
	snapshot RuntimeTuningSnapshot,
	decision RuntimeTuningDecision,
) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.snapshots = append(
		sink.snapshots,
		cloneRuntimeTuningSnapshot(snapshot),
	)
	sink.decisions = append(
		sink.decisions,
		cloneRuntimeTuningDecision(decision),
	)
	if sink.onPersist != nil {
		sink.onPersist()
	}
	return sink.err
}

func (sink *runtimeTuningHistoryRecordingSink) snapshot() (
	[]RuntimeTuningSnapshot,
	[]RuntimeTuningDecision,
) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	snapshots := make([]RuntimeTuningSnapshot, len(sink.snapshots))
	for index := range sink.snapshots {
		snapshots[index] = cloneRuntimeTuningSnapshot(sink.snapshots[index])
	}
	return snapshots, cloneRuntimeTuningHistory(sink.decisions)
}

func TestResumableNetworkTransferPersistsRuntimeTuningBeforeCheckpoint(
	t *testing.T,
) {
	plan := networkTransferTestPlan(1)
	controller := attachNetworkTestRuntimeTuning(t, &plan, 100)

	var eventsMu sync.Mutex
	events := make([]string, 0, 5)
	record := func(event string) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		events = append(events, event)
	}
	saw := func(event string) bool {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for _, candidate := range events {
			if candidate == event {
				return true
			}
		}
		return false
	}
	sink := &runtimeTuningHistoryRecordingSink{
		onPersist: func() { record("sink") },
	}
	plan.RuntimeTuningSink = sink

	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{int64(1)}},
					[]int64{16},
					"frontier-1",
					"fingerprint-1",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				record("write")
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
				record("issued")
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				if !saw("sink") {
					return fmt.Errorf(
						"checkpoint reached before runtime-tuning decision",
					)
				}
				record("checkpoint")
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("RunResumableNetworkTransfer: %v", err)
	}

	// The sink records only after WritePage, but before the first durable range
	// checkpoint. That preserves a decision even if a process ends between the
	// target commit and the next chunk/range transition.
	eventsMu.Lock()
	observedEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	indexOf := func(want string) int {
		for index, event := range observedEvents {
			if event == want {
				return index
			}
		}
		return -1
	}
	if write, persisted, checkpoint := indexOf("write"), indexOf("sink"), indexOf("checkpoint"); write < 0 || persisted < 0 || checkpoint < 0 ||
		write >= persisted || persisted >= checkpoint {
		t.Fatalf("runtime-tuning persistence order = %v", observedEvents)
	}
	if !result.HasRuntimeTuning || result.RuntimeTuning.TotalDecisions != 1 ||
		result.Rows != 1 || result.CompletedRanges != 1 {
		t.Fatalf("transfer result = %#v", result)
	}
	snapshots, decisions := sink.snapshot()
	if len(snapshots) != 1 || len(decisions) != 1 ||
		!snapshots[0].HasBoundary ||
		snapshots[0].LastBoundary != decisions[0].Boundary ||
		decisions[0].Boundary.Ordinal != 1 {
		t.Fatalf(
			"persisted runtime-tuning decisions snapshots=%#v decisions=%#v",
			snapshots,
			decisions,
		)
	}
	history := controller.History()
	if len(history) != 1 ||
		history[0].Boundary != decisions[0].Boundary ||
		history[0].Before != decisions[0].Before ||
		history[0].After != decisions[0].After {
		t.Fatalf("controller history=%#v persisted=%#v", history, decisions)
	}
}

func TestResumableNetworkTransferStopsBeforeCheckpointWhenRuntimeTuningPersistenceFails(
	t *testing.T,
) {
	plan := networkTransferTestPlan(1)
	controller := attachNetworkTestRuntimeTuning(t, &plan, 100)
	writeErr := errors.New("persist runtime-tuning history")
	sink := &runtimeTuningHistoryRecordingSink{err: writeErr}
	plan.RuntimeTuningSink = sink

	var writes, checkpoints int
	var callbacksMu sync.Mutex
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{int64(1)}},
					[]int64{16},
					"frontier-1",
					"fingerprint-1",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				callbacksMu.Lock()
				writes++
				callbacksMu.Unlock()
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
				callbacksMu.Lock()
				checkpoints++
				callbacksMu.Unlock()
				return nil
			},
		},
	)
	if !errors.Is(err, writeErr) {
		t.Fatalf("transfer error = %v, want history persistence error", err)
	}
	callbacksMu.Lock()
	gotWrites, gotCheckpoints := writes, checkpoints
	callbacksMu.Unlock()
	if gotWrites != 1 || gotCheckpoints != 0 {
		t.Fatalf(
			"history persistence failure writes=%d checkpoints=%d",
			gotWrites,
			gotCheckpoints,
		)
	}
	snapshots, decisions := sink.snapshot()
	if len(snapshots) != 1 || len(decisions) != 1 ||
		!result.HasRuntimeTuning || result.RuntimeTuning.TotalDecisions != 1 ||
		result.Rows != 0 || result.CompletedRanges != 0 {
		t.Fatalf(
			"failure result=%#v snapshots=%#v decisions=%#v",
			result,
			snapshots,
			decisions,
		)
	}
	if history := controller.History(); len(history) != 1 ||
		history[0].Boundary != decisions[0].Boundary {
		t.Fatalf("controller history=%#v persisted=%#v", history, decisions)
	}
}
