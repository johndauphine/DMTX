package migrate

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/johndauphine/dmtx/internal/config"
)

// Per-range state: the turn-taking that keeps ranges ordered, plus activity
// and retry accounting.

type networkRangeState struct {
	plan                  NetworkRangePlan
	restore               NetworkRangeRestore
	tracker               *ContiguousAckTracker
	baseRows              int64
	safeRows              int64
	frontier              []byte
	pendingCheckpointAcks int
	complete              bool
	turn                  networkSequenceTurn
}

type networkSequenceTurn struct {
	mu      sync.Mutex
	next    uint64
	changed chan struct{}
}

func newNetworkSequenceTurn(next uint64) networkSequenceTurn {
	return networkSequenceTurn{
		next:    next,
		changed: make(chan struct{}),
	}
}

func (turn *networkSequenceTurn) wait(
	ctx context.Context,
	sequence uint64,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		turn.mu.Lock()
		switch {
		case sequence < turn.next:
			turn.mu.Unlock()
			return fmt.Errorf(
				"%w: range sequence is behind the writer frontier",
				ErrInvalidNetworkRestore,
			)
		case sequence == turn.next:
			turn.mu.Unlock()
			return nil
		default:
			changed := turn.changed
			turn.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
		}
	}
}

func (turn *networkSequenceTurn) advance(sequence uint64) error {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if sequence != turn.next || turn.next == math.MaxUint64 {
		return fmt.Errorf(
			"%w: range writer sequence cannot advance",
			ErrInvalidNetworkRestore,
		)
	}
	turn.next++
	changed := turn.changed
	turn.changed = make(chan struct{})
	close(changed)
	return nil
}

type networkBufferedChunk struct {
	state         *networkRangeState
	issued        NetworkIssuedChunk
	page          NetworkReadPage
	reservation   *ByteReservation
	replay        bool
	initialOffset int64
}

func (chunk *networkBufferedChunk) release() {
	if chunk == nil || chunk.reservation == nil {
		return
	}
	chunk.reservation.Release()
	chunk.reservation = nil
}

type networkAdaptiveGate struct {
	mu      sync.Mutex
	active  int
	changed chan struct{}
}

func newNetworkAdaptiveGate() *networkAdaptiveGate {
	return &networkAdaptiveGate{changed: make(chan struct{})}
}

func (gate *networkAdaptiveGate) acquire(
	ctx context.Context,
	limit func() int,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		currentLimit := limit()
		if currentLimit < 1 {
			return fmt.Errorf(
				"%w: live concurrency limit is not positive",
				ErrInvalidNetworkTransferPlan,
			)
		}
		gate.mu.Lock()
		if gate.active < currentLimit {
			gate.active++
			gate.mu.Unlock()
			return nil
		}
		changed := gate.changed
		gate.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (gate *networkAdaptiveGate) release() {
	gate.mu.Lock()
	if gate.active <= 0 {
		gate.mu.Unlock()
		panic("network adaptive gate released without ownership")
	}
	gate.active--
	changed := gate.changed
	gate.changed = make(chan struct{})
	close(changed)
	gate.mu.Unlock()
}

func (gate *networkAdaptiveGate) count() int {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.active
}

type networkActivity struct {
	mu      sync.Mutex
	readers int
}

func (activity *networkActivity) addReader(delta int) {
	activity.mu.Lock()
	activity.readers += delta
	activity.mu.Unlock()
}

func (activity *networkActivity) readerCount() int {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	return activity.readers
}

type networkRetryFacts struct {
	mu    sync.Mutex
	facts []NetworkRetryFact
}

func (facts *networkRetryFacts) append(value NetworkRetryFact) {
	facts.mu.Lock()
	facts.facts = append(facts.facts, value)
	facts.mu.Unlock()
}

func (facts *networkRetryFacts) snapshot() []NetworkRetryFact {
	facts.mu.Lock()
	defer facts.mu.Unlock()
	result := append([]NetworkRetryFact(nil), facts.facts...)
	sort.Slice(result, func(left, right int) bool {
		if result[left].RangeIndex != result[right].RangeIndex {
			return result[left].RangeIndex < result[right].RangeIndex
		}
		if result[left].Sequence != result[right].Sequence {
			return result[left].Sequence < result[right].Sequence
		}
		if result[left].Attempt != result[right].Attempt {
			return result[left].Attempt < result[right].Attempt
		}
		return result[left].Operation < result[right].Operation
	})
	return result
}

type networkTuningRangeProgress struct {
	hasSequence     bool
	networkSequence uint64
	tuningSequence  uint64
	attempt         uint32
}

type networkTuningCoordinator struct {
	mu         sync.Mutex
	controller *RuntimeTuningController
	sink       RuntimeTuningDecisionSink
	rowWidth   RuntimeRowWidthEvidence
	resources  config.EffectiveTransferPlan
	ranges     uint64
	budget     *ByteBudget
	queue      *networkAdaptiveGate
	writers    *networkAdaptiveGate
	activity   *networkActivity
	ordinal    uint64
	rows       uint64
	bytes      uint64
	progress   map[uint64]networkTuningRangeProgress
}

func (coordinator *networkTuningCoordinator) observe(
	ctx context.Context,
	chunk *networkBufferedChunk,
	rows int,
	retainedBytes int64,
	outcome RuntimeWriteOutcome,
) error {
	if coordinator == nil || coordinator.controller == nil {
		return nil
	}
	if rows <= 0 || retainedBytes <= 0 {
		return fmt.Errorf(
			"%w: runtime boundary has no observed work",
			ErrInvalidNetworkPage,
		)
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.ordinal == math.MaxUint64 ||
		coordinator.rows > math.MaxUint64-uint64(rows) ||
		coordinator.bytes > math.MaxUint64-uint64(retainedBytes) {
		return fmt.Errorf(
			"%w: runtime observation counters overflow",
			ErrInvalidNetworkTransferPlan,
		)
	}
	progress := coordinator.progress[chunk.issued.RangeIndex]
	switch {
	case !progress.hasSequence:
		progress.hasSequence = true
		progress.networkSequence = chunk.issued.Sequence
	case progress.networkSequence == chunk.issued.Sequence:
		if progress.attempt == math.MaxUint32 {
			return fmt.Errorf(
				"%w: runtime attempt counter overflow",
				ErrInvalidNetworkTransferPlan,
			)
		}
		progress.attempt++
	case progress.networkSequence != math.MaxUint64 &&
		chunk.issued.Sequence == progress.networkSequence+1:
		if progress.tuningSequence == math.MaxUint64 {
			return fmt.Errorf(
				"%w: runtime range sequence overflow",
				ErrInvalidNetworkTransferPlan,
			)
		}
		progress.networkSequence = chunk.issued.Sequence
		progress.tuningSequence++
		progress.attempt = 0
	default:
		return fmt.Errorf(
			"%w: runtime observations changed range sequence",
			ErrInvalidNetworkRestore,
		)
	}
	coordinator.progress[chunk.issued.RangeIndex] = progress
	coordinator.ordinal++
	coordinator.rows += uint64(rows)
	coordinator.bytes += uint64(retainedBytes)

	effective := coordinator.controller.Snapshot().Effective
	queueDepth := coordinator.queue.count()
	activeWriters := coordinator.writers.count()
	activeReaders := coordinator.activity.readerCount()
	openConnections := activeReaders + activeWriters
	budgetStats := coordinator.budget.Stats()
	observation := RuntimeTuningObservation{
		Boundary: RuntimeTuningBoundary{
			Ordinal:       coordinator.ordinal,
			TableSchema:   chunk.state.plan.TableSchema,
			TableName:     chunk.state.plan.TableName,
			RangeIndex:    chunk.issued.RangeIndex,
			ChunkSequence: progress.tuningSequence,
			Attempt:       progress.attempt,
		},
		ObservedRows:            uint64(rows),
		ObservedBytes:           uint64(retainedBytes),
		CumulativeObservedRows:  coordinator.rows,
		CumulativeObservedBytes: coordinator.bytes,
		Inventory: RuntimeResourceInventory{
			Complete:        true,
			PlannedRanges:   coordinator.ranges,
			ConnectionLimit: coordinator.resources.ConnectionLimit.Value,
			OpenConnections: openConnections,
			ActiveReaders:   activeReaders,
			ActiveWriters:   activeWriters,
			QueueDepth:      queueDepth,
			ByteBudget:      budgetStats,
		},
		RowWidth:     coordinator.rowWidth,
		WriteOutcome: outcome,
	}
	observation.MemoryPressure =
		budgetStats.Current >= budgetStats.Limit-budgetStats.Limit/4
	observation.QueuePressure =
		queueDepth >= effective.BufferDepth.Value
	observation.ConnectionPressure =
		openConnections >= coordinator.resources.ConnectionLimit.Value
	snapshot, decision, err := coordinator.controller.ApplyChunkBoundaryDecision(
		ctx,
		observation,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("apply runtime tuning boundary: %w", err),
		)
	}
	if coordinator.sink != nil {
		if err := coordinator.sink.PersistRuntimeTuningDecision(
			ctx,
			snapshot,
			decision,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("persist runtime tuning decision: %w", err),
			)
		}
	}
	return nil
}
