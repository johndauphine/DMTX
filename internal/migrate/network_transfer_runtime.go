package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/johndauphine/dmtx/internal/config"
)

// The transfer runtime: reading pages, emitting chunks, and driving a range
// to completion under its resource plan.

type networkTransferRuntime struct {
	plan       NetworkTransferPlan
	callbacks  NetworkTransferCallbacks
	budget     *ByteBudget
	queueGate  *networkAdaptiveGate
	writerGate *networkAdaptiveGate
	activity   *networkActivity
	tuning     *networkTuningCoordinator
	retryFacts *networkRetryFacts
	stateMu    sync.Mutex
}

// RunResumableNetworkTransfer executes an engine-neutral, range-restorable
// transfer. No adapter or state backend is opened by this core.
func RunResumableNetworkTransfer(
	ctx context.Context,
	plan NetworkTransferPlan,
	callbacks NetworkTransferCallbacks,
) (NetworkTransferResult, error) {
	if ctx == nil {
		return NetworkTransferResult{}, fmt.Errorf(
			"%w: nil context",
			ErrInvalidNetworkTransferPlan,
		)
	}
	states, err := validateNetworkTransferPlan(plan, callbacks)
	if err != nil {
		return NetworkTransferResult{}, err
	}
	budget, err := NewByteBudget(plan.Resources.MemoryBudget.Value)
	if err != nil {
		return NetworkTransferResult{}, err
	}
	runtime := &networkTransferRuntime{
		plan:       plan,
		callbacks:  callbacks,
		budget:     budget,
		queueGate:  newNetworkAdaptiveGate(),
		writerGate: newNetworkAdaptiveGate(),
		activity:   &networkActivity{},
		retryFacts: &networkRetryFacts{},
	}
	runtimeTuningSink := plan.RuntimeTuningSink
	if isNilInterface(runtimeTuningSink) {
		runtimeTuningSink = nil
	}
	runtime.tuning = &networkTuningCoordinator{
		controller: plan.RuntimeTuning,
		sink:       runtimeTuningSink,
		rowWidth:   plan.RowWidth,
		resources:  plan.Resources,
		ranges:     uint64(len(states)),
		budget:     budget,
		queue:      runtime.queueGate,
		writers:    runtime.writerGate,
		activity:   runtime.activity,
		progress:   make(map[uint64]networkTuningRangeProgress),
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var failureMu sync.Mutex
	var firstFailure error
	fail := func(value error) {
		if value == nil {
			return
		}
		failureMu.Lock()
		if firstFailure == nil {
			firstFailure = value
			cancel()
		}
		failureMu.Unlock()
	}
	getFailure := func() error {
		failureMu.Lock()
		defer failureMu.Unlock()
		return firstFailure
	}

	jobs := make(chan *networkRangeState)
	chunks := make(
		chan *networkBufferedChunk,
		plan.Resources.QueueDepth.Value,
	)
	go func() {
		defer close(jobs)
		for _, state := range states {
			if state.complete {
				continue
			}
			select {
			case <-pipelineCtx.Done():
				return
			case jobs <- state:
			}
		}
	}()

	readerCount := plan.Resources.Readers.Value
	if readerCount > len(states) {
		readerCount = len(states)
	}
	if readerCount < 1 {
		readerCount = 1
	}
	var readers sync.WaitGroup
	readers.Add(readerCount)
	for index := 0; index < readerCount; index++ {
		go func() {
			defer readers.Done()
			for state := range jobs {
				if err := runtime.readRange(
					pipelineCtx,
					state,
					chunks,
				); err != nil && pipelineCtx.Err() == nil {
					fail(err)
				}
			}
		}()
	}
	go func() {
		readers.Wait()
		close(chunks)
	}()

	writerCount := plan.Resources.Writers.Value
	var writers sync.WaitGroup
	writers.Add(writerCount)
	for index := 0; index < writerCount; index++ {
		go func() {
			defer writers.Done()
			for chunk := range chunks {
				runtime.queueGate.release()
				if pipelineCtx.Err() != nil {
					chunk.release()
					continue
				}
				err := runtime.processChunk(pipelineCtx, chunk)
				chunk.release()
				if err != nil && pipelineCtx.Err() == nil {
					fail(err)
				}
			}
		}()
	}
	writers.Wait()

	result, resultErr := runtime.result(states)
	if stats := budget.Stats(); stats.Current != 0 {
		leak := NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"network transfer leaked %d admitted bytes",
				stats.Current,
			),
		)
		if existing := getFailure(); existing != nil {
			return result, errors.Join(existing, resultErr, leak)
		}
		return result, errors.Join(resultErr, leak)
	}
	if err := getFailure(); err != nil {
		return result, errors.Join(err, resultErr)
	}
	if resultErr != nil {
		return result, resultErr
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for _, state := range states {
		if !state.complete {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network range %d ended without a durable completion",
					state.plan.RangeIndex,
				),
			)
		}
	}
	return result, nil
}

func validateNetworkTransferPlan(
	plan NetworkTransferPlan,
	callbacks NetworkTransferCallbacks,
) ([]*networkRangeState, error) {
	if callbacks.ReadPage == nil ||
		callbacks.WritePage == nil ||
		callbacks.RecordIssued == nil ||
		callbacks.Checkpoint == nil {
		return nil, fmt.Errorf(
			"%w: all transfer callbacks are required",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if !knownRetryEngine(plan.SourceEngine) ||
		!knownRetryEngine(plan.TargetEngine) {
		return nil, fmt.Errorf(
			"%w: source or target retry engine is unsupported",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if plan.RetryPolicy.MaxRetries < 0 ||
		plan.RetryPolicy.MaxRetries > maximumNetworkRetries ||
		plan.RetryPolicy.InitialBackoff < 0 ||
		plan.RetryPolicy.MaxBackoff < 0 {
		return nil, fmt.Errorf(
			"%w: retry policy is invalid or unbounded",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if plan.CheckpointFrequency < 0 ||
		plan.CheckpointFrequency > maximumNetworkCheckpointFrequency {
		return nil, fmt.Errorf(
			"%w: checkpoint frequency is invalid or unbounded",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if plan.UpsertMergeRows < 0 ||
		plan.UpsertMergeRows > config.MaxTransferChunkRows {
		return nil, fmt.Errorf(
			"%w: upsert merge size is invalid or unbounded",
			ErrInvalidNetworkTransferPlan,
		)
	}
	resources := plan.Resources
	if resources.TargetMode != "drop_recreate" &&
		resources.TargetMode != "upsert" ||
		resources.DetectedMemoryLimit.Value <= 0 ||
		resources.MemoryBudget.Value < config.MinimumTransferMemoryBytes ||
		resources.MemoryBudget.Value >
			resources.DetectedMemoryLimit.Value ||
		resources.ChunkRows.Value < 1 ||
		resources.ChunkRows.Value > config.MaxTransferChunkRows ||
		resources.Workers.Value < 1 ||
		resources.Workers.Value > config.MaxTransferWorkers ||
		resources.Readers.Value < 1 ||
		resources.Readers.Value > config.MaxTransferReaders ||
		resources.Writers.Value < 1 ||
		resources.Writers.Value > config.MaxTransferWriters ||
		resources.QueueDepth.Value < 1 ||
		resources.QueueDepth.Value > config.MaxTransferQueueDepth ||
		resources.ConnectionLimit.Value < 2 ||
		resources.Workers.Value < resources.Readers.Value+
			resources.Writers.Value ||
		resources.ConnectionLimit.Value <
			resources.Readers.Value+resources.Writers.Value {
		return nil, fmt.Errorf(
			"%w: effective resources are unsafe",
			ErrInvalidNetworkTransferPlan,
		)
	}
	settings := []config.EffectiveInt{
		resources.ConnectionLimit,
		resources.Workers,
		resources.Readers,
		resources.Writers,
		resources.QueueDepth,
		resources.ChunkRows,
	}
	for _, setting := range settings {
		if !validRuntimeSettingProvenance(setting.Provenance) {
			return nil, fmt.Errorf(
				"%w: effective resource provenance is unsupported",
				ErrInvalidNetworkTransferPlan,
			)
		}
	}
	if !validRuntimeDetectedMemoryProvenance(
		resources.DetectedMemoryLimit.Provenance,
	) || !validRuntimeMemoryProvenance(
		resources.MemoryBudget.Provenance,
	) {
		return nil, fmt.Errorf(
			"%w: memory provenance is unsupported",
			ErrInvalidNetworkTransferPlan,
		)
	}
	switch resources.TargetMode {
	case "drop_recreate":
		if plan.ReplayMode != NetworkReplayDuplicateSafeInsertOnly {
			return nil, fmt.Errorf(
				"%w: rebuild requires duplicate-safe insert-only replay",
				ErrInvalidNetworkTransferPlan,
			)
		}
	case "upsert":
		if plan.ReplayMode != NetworkReplayIdempotentUpsert {
			return nil, fmt.Errorf(
				"%w: upsert requires an idempotent replay path",
				ErrInvalidNetworkTransferPlan,
			)
		}
	}
	if plan.UpsertMergeRows > 0 &&
		(plan.ReplayMode != NetworkReplayIdempotentUpsert ||
			plan.UpsertMergeRows > resources.ChunkRows.Value) {
		return nil, fmt.Errorf(
			"%w: upsert merge size is incompatible with the admitted replay resources",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if len(plan.Ranges) == 0 ||
		uint64(len(plan.Ranges)) > maximumRuntimeTuningRanges {
		return nil, fmt.Errorf(
			"%w: range inventory is empty or unbounded",
			ErrInvalidNetworkTransferPlan,
		)
	}

	ranges := append([]NetworkRangePlan(nil), plan.Ranges...)
	sort.Slice(ranges, func(left, right int) bool {
		return ranges[left].RangeIndex < ranges[right].RangeIndex
	})
	for index, transferRange := range ranges {
		if transferRange.RangeIndex != uint64(index) ||
			!validNetworkIdentifier(transferRange.TableSchema, true) ||
			!validNetworkIdentifier(transferRange.TableName, false) ||
			!validNetworkFactToken(transferRange.TopologyHash) ||
			!validNetworkPagination(transferRange.Pagination) ||
			transferRange.MaxRowBytes <= 0 ||
			transferRange.MaxRowBytes >
				resources.MemoryBudget.Value {
			return nil, fmt.Errorf(
				"%w: range inventory is malformed",
				ErrInvalidNetworkTransferPlan,
			)
		}
	}

	if len(plan.Restores) > len(ranges) {
		return nil, fmt.Errorf(
			"%w: restore inventory exceeds planned ranges",
			ErrInvalidNetworkRestore,
		)
	}
	restores := make(map[uint64]NetworkRangeRestore, len(plan.Restores))
	for _, restore := range plan.Restores {
		if restore.RangeIndex >= uint64(len(ranges)) {
			return nil, fmt.Errorf(
				"%w: restore range is not planned",
				ErrInvalidNetworkRestore,
			)
		}
		if _, duplicate := restores[restore.RangeIndex]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate range restore",
				ErrInvalidNetworkRestore,
			)
		}
		expected := ranges[restore.RangeIndex]
		if restore.TopologyHash != expected.TopologyHash {
			return nil, fmt.Errorf(
				"%w: restored topology does not match the plan",
				ErrInvalidNetworkRestore,
			)
		}
		normalized, err := validateNetworkRestore(
			restore,
			expected,
			resources,
		)
		if err != nil {
			return nil, err
		}
		restores[restore.RangeIndex] = normalized
	}
	totalRestoredRows := int64(0)
	for _, restore := range restores {
		if restore.RowsDone >
			math.MaxInt64-restore.SequenceOffset ||
			totalRestoredRows >
				math.MaxInt64-restore.RowsDone-
					restore.SequenceOffset {
			return nil, fmt.Errorf(
				"%w: restored row counters overflow",
				ErrInvalidNetworkRestore,
			)
		}
		totalRestoredRows +=
			restore.RowsDone + restore.SequenceOffset
	}

	if plan.RuntimeTuning == nil && !isNilInterface(plan.RuntimeTuningSink) {
		return nil, fmt.Errorf(
			"%w: runtime-tuning history sink requires a controller",
			ErrInvalidNetworkTransferPlan,
		)
	}
	if plan.RuntimeTuning != nil {
		snapshot := plan.RuntimeTuning.Snapshot()
		if snapshot.Intent != resources ||
			plan.RuntimeTuning.limits.PlannedRanges !=
				uint64(len(ranges)) ||
			plan.RuntimeTuning.limits.ExpectedColumnCount !=
				plan.RowWidth.ExpectedColumnCount {
			return nil, fmt.Errorf(
				"%w: runtime tuning evidence differs from transfer plan",
				ErrInvalidNetworkTransferPlan,
			)
		}
		if snapshot.HasBoundary ||
			snapshot.AppliedBoundaries != 0 ||
			snapshot.TotalDecisions != 0 ||
			snapshot.RetainedDecisions != 0 {
			return nil, fmt.Errorf(
				"%w: runtime tuning controller already contains execution evidence",
				ErrInvalidNetworkTransferPlan,
			)
		}
		if plan.RowWidth.Trustworthy {
			if plan.RowWidth.CompleteColumnCount !=
				plan.RuntimeTuning.limits.ExpectedColumnCount ||
				plan.RowWidth.ExpectedColumnCount !=
					plan.RuntimeTuning.limits.ExpectedColumnCount ||
				plan.RowWidth.UpperBoundBytes >
					plan.RuntimeTuning.limits.
						SafetyRowWidthUpperBound {
				return nil, fmt.Errorf(
					"%w: runtime row-width proof is incomplete",
					ErrInvalidNetworkTransferPlan,
				)
			}
		} else if plan.RowWidth != (RuntimeRowWidthEvidence{}) {
			return nil, fmt.Errorf(
				"%w: untrusted row-width proof carries partial values",
				ErrInvalidNetworkTransferPlan,
			)
		}
		for _, transferRange := range ranges {
			if transferRange.MaxRowBytes >
				plan.RuntimeTuning.limits.
					SafetyRowWidthUpperBound {
				return nil, fmt.Errorf(
					"%w: range row reservation exceeds runtime safety evidence",
					ErrInvalidNetworkTransferPlan,
				)
			}
		}
	}

	states := make([]*networkRangeState, 0, len(ranges))
	for _, transferRange := range ranges {
		restore, exists := restores[transferRange.RangeIndex]
		if !exists {
			restore = NetworkRangeRestore{
				RangeIndex:   transferRange.RangeIndex,
				TopologyHash: transferRange.TopologyHash,
			}
		}
		state := &networkRangeState{
			plan:    transferRange,
			restore: cloneNetworkRestore(restore),
			tracker: NewContiguousAckTracker(
				fmt.Sprintf("range/%d", transferRange.RangeIndex),
				restore.NextSequence,
			),
			baseRows: restore.RowsDone,
			safeRows: restore.RowsDone + restore.SequenceOffset,
			frontier: cloneNetworkBytes(restore.Frontier),
			complete: restore.Complete,
			turn:     newNetworkSequenceTurn(restore.NextSequence),
		}
		if restore.SequenceOffset > 0 {
			issued := restore.Issued[0]
			_, err := state.tracker.Acknowledge(
				issued.Sequence,
				int64(issued.Rows),
				WriteReceipt{
					Certainty:     CommitDurablePrefix,
					AttemptOffset: 0,
					AttemptedRows: int64(issued.Rows),
					CommittedRows: restore.SequenceOffset,
				},
			)
			if err != nil {
				return nil, fmt.Errorf(
					"%w: restore durable prefix is invalid",
					ErrInvalidNetworkRestore,
				)
			}
		}
		states = append(states, state)
	}
	return states, nil
}
