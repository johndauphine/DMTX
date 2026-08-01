package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/config"
)

var (
	ErrInvalidNetworkTransferPlan = errors.New(
		"invalid resumable network transfer plan",
	)
	ErrInvalidNetworkPage = errors.New(
		"invalid resumable network source page",
	)
	ErrInvalidNetworkRestore = errors.New(
		"invalid resumable network range restore",
	)
	ErrUnknownNetworkCommit = errors.New(
		"network target commit outcome is unknown",
	)
)

// NetworkWriteProtocolLimitError is a fixed, secret-free adapter signal that a
// target rejected a write because the attempted batch exceeded a proven
// protocol limit. Adapters must return this signal only with a
// CommitNotCommitted receipt. The core can then apply a smaller runtime-tuning
// boundary before retrying without guessing from driver text.
type NetworkWriteProtocolLimitError struct{}

func (*NetworkWriteProtocolLimitError) Error() string {
	return "network target write exceeded a protocol limit"
}

const (
	maximumNetworkFrontierBytes       = 64 << 10
	maximumNetworkFactToken           = 128
	maximumNetworkRetries             = 1 << 16
	maximumNetworkCheckpointFrequency = config.MaxTransferChunkRows
)

// NetworkReplayMode is an adapter capability promise. Rebuild routes must
// prove an insert-only duplicate-safe replay path; upsert routes must prove
// that replay is idempotent without overwriting rows outside the source page.
type NetworkReplayMode string

const (
	NetworkReplayDuplicateSafeInsertOnly NetworkReplayMode = "duplicate_safe_insert_only"
	NetworkReplayIdempotentUpsert        NetworkReplayMode = "idempotent_upsert"
)

// NetworkWriteMode tells a target callback which independently proven write
// path it must use for this attempt.
type NetworkWriteMode string

const (
	NetworkWriteFreshInsert             NetworkWriteMode = "fresh_insert"
	NetworkWriteDuplicateSafeInsertOnly NetworkWriteMode = "duplicate_safe_insert_only"
	NetworkWriteIdempotentUpsert        NetworkWriteMode = "idempotent_upsert"
)

// NetworkRangePlan is one immutable migration-global range. RangeIndex is
// contiguous across every selected table and is the durable identity used by
// state and runtime tuning. Bounds remain adapter-owned; the core carries only
// their credential-free topology hash and opaque durable frontier.
type NetworkRangePlan struct {
	RangeIndex   uint64
	TableSchema  string
	TableName    string
	TopologyHash string
	Pagination   PaginationStrategy
	MaxRowBytes  int64
}

// NetworkIssuedChunk is recorded durably before the first target mutation.
// StartFrontier and EndFrontier are opaque typed state encodings and are never
// copied into status or retry facts. Fingerprint is a stable canonical digest
// of the complete page, not raw row data.
type NetworkIssuedChunk struct {
	RangeIndex    uint64
	Sequence      uint64
	Rows          int
	StartFrontier []byte
	EndFrontier   []byte
	Fingerprint   string
	Exhausted     bool
}

// NetworkRangeRestore is the exact durable work set for one planned range.
// RowsDone counts complete sequences before NextSequence. SequenceOffset is
// the known durable prefix inside the first issued NextSequence chunk.
type NetworkRangeRestore struct {
	RangeIndex     uint64
	TopologyHash   string
	NextSequence   uint64
	SequenceOffset int64
	RowsDone       int64
	Frontier       []byte
	Complete       bool
	Issued         []NetworkIssuedChunk
}

// NetworkReadRequest bounds one source callback. The core reserves
// MaxRows*MaxRowBytes from the migration-wide ByteBudget before invoking the
// callback, so source implementations may materialize at most MaxRows owned
// rows. ReplayExpected is non-nil only for a durably issued chunk.
type NetworkReadRequest struct {
	Range          NetworkRangePlan
	Sequence       uint64
	Attempt        int
	MaxRows        int
	StartFrontier  []byte
	ReplayExpected *NetworkIssuedChunk
}

// NetworkReadPage transfers exclusive ownership of Rows and every referenced
// payload backing buffer to the core. ReadPage must not retain or mutate them
// after returning. RowBytes is the exact retained in-memory size of each row
// and RetainedBytes is their exact sum. A non-empty page must advance
// EndFrontier and carry a canonical digest.
type NetworkReadPage struct {
	Rows          [][]any
	RowBytes      []int64
	RetainedBytes int64
	EndFrontier   []byte
	Fingerprint   string
	Exhausted     bool
}

// NetworkWriteRequest is one bounded target callback. AttemptOffset is
// relative to the complete issued chunk; Rows is a borrowed, read-only view of
// the core-owned page containing only the suffix/subset represented by this
// attempt. WritePage must not mutate or retain Rows or any referenced payload
// backing buffer after returning.
type NetworkWriteRequest struct {
	Range         NetworkRangePlan
	Sequence      uint64
	Attempt       uint32
	AttemptOffset int64
	Mode          NetworkWriteMode
	Rows          [][]any
}

// NetworkRangeCheckpoint contains only the lowest contiguous durable frontier
// for one range. FrontierBytes remains at the prior complete page while
// Frontier.SequenceOffset is non-zero.
type NetworkRangeCheckpoint struct {
	RangeIndex    uint64
	TopologyHash  string
	Frontier      AckFrontier
	FrontierBytes []byte
	Complete      bool
	Memory        ByteBudgetStats
}

// NetworkTransferCallbacks are deliberately engine-neutral. ReadPage must
// close any driver cursor before returning and transfers exclusive ownership
// of its page payload to the core. WritePage receives only a borrowed,
// immutable view for the duration of the call. RecordIssued and Checkpoint must
// be durable when they return nil; the core serializes those state callbacks.
type NetworkTransferCallbacks struct {
	ReadPage     func(context.Context, NetworkReadRequest) (NetworkReadPage, error)
	WritePage    func(context.Context, NetworkWriteRequest) (WriteReceipt, error)
	RecordIssued func(context.Context, NetworkIssuedChunk) error
	Checkpoint   func(context.Context, NetworkRangeCheckpoint) error
}

// RuntimeTuningDecisionSink durably records one controller decision before the
// transfer can proceed to subsequent work. It receives only bounded,
// credential-free controller facts; source frontiers, row values, and driver
// errors remain outside this surface.
type RuntimeTuningDecisionSink interface {
	PersistRuntimeTuningDecision(
		context.Context,
		RuntimeTuningSnapshot,
		RuntimeTuningDecision,
	) error
}

// NetworkTransferPlan is immutable input for one migration-wide transfer.
// RuntimeTuning may be nil when runtime adjustment is disabled. When present,
// its immutable intent and range/row-width evidence must match this plan.
type NetworkTransferPlan struct {
	SourceEngine string
	TargetEngine string
	Resources    config.EffectiveTransferPlan
	RetryPolicy  RetryPolicy
	ReplayMode   NetworkReplayMode
	// UpsertMergeRows is an immutable write-only ceiling for an admitted
	// idempotent-upsert route. It deliberately does not affect source page
	// requests or issued-page identities: one issued page may be acknowledged
	// through several contiguous durable write prefixes. Zero preserves the
	// legacy one-write-per-live-source-page behavior.
	UpsertMergeRows int
	// CheckpointFrequency is the number of contiguous durable write
	// acknowledgements per range between periodic state checkpoints. Zero
	// preserves the legacy immediate-on-ack cadence. A final range checkpoint
	// is always synchronous regardless of this value.
	CheckpointFrequency int
	Ranges              []NetworkRangePlan
	Restores            []NetworkRangeRestore
	RuntimeTuning       *RuntimeTuningController
	RuntimeTuningSink   RuntimeTuningDecisionSink
	RowWidth            RuntimeRowWidthEvidence
}

// NetworkPaginationFact is stable, bounded, and contains no frontier or row
// values.
type NetworkPaginationFact struct {
	RangeIndex   uint64
	TableSchema  string
	TableName    string
	TopologyHash string
	Pagination   PaginationStrategy
}

// NetworkRetryOperation distinguishes source reads from target writes without
// exposing SQL text or driver messages.
type NetworkRetryOperation string

const (
	NetworkRetryRead  NetworkRetryOperation = "read"
	NetworkRetryWrite NetworkRetryOperation = "write"
)

// NetworkRetryFact pairs a secret-free engine classification with its stable
// work identity.
type NetworkRetryFact struct {
	RangeIndex uint64
	Sequence   uint64
	Attempt    uint32
	Operation  NetworkRetryOperation
	Fact       EngineRetryFact
}

// NetworkTransferResult reports only safely checkpointed work.
type NetworkTransferResult struct {
	Rows             int64
	CompletedRanges  int
	Pagination       []NetworkPaginationFact
	Retries          []NetworkRetryFact
	Memory           ByteBudgetStats
	HasRuntimeTuning bool
	RuntimeTuning    RuntimeTuningSnapshot
}

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

func validateNetworkRestore(
	restore NetworkRangeRestore,
	planned NetworkRangePlan,
	resources config.EffectiveTransferPlan,
) (NetworkRangeRestore, error) {
	if restore.RowsDone < 0 ||
		restore.SequenceOffset < 0 ||
		restore.NextSequence == math.MaxUint64 ||
		len(restore.Frontier) > maximumNetworkFrontierBytes ||
		(restore.RowsDone > 0 || restore.NextSequence > 0) &&
			len(restore.Frontier) == 0 {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: restored frontier is malformed",
			ErrInvalidNetworkRestore,
		)
	}
	if !validNetworkRestoreRowSequenceEvidence(
		restore.RowsDone,
		restore.NextSequence,
	) {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: restored rows and completed sequence count disagree",
			ErrInvalidNetworkRestore,
		)
	}
	if restore.Complete {
		if restore.SequenceOffset != 0 || len(restore.Issued) != 0 {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: completed range retains incomplete work",
				ErrInvalidNetworkRestore,
			)
		}
		return cloneNetworkRestore(restore), nil
	}
	maxIssued := resources.QueueDepth.Value + resources.Writers.Value
	if len(restore.Issued) > maxIssued {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: issued work exceeds the bounded pipeline",
			ErrInvalidNetworkRestore,
		)
	}
	if uint64(len(restore.Issued)) >
		math.MaxUint64-restore.NextSequence-1 {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: issued sequence inventory overflows",
			ErrInvalidNetworkRestore,
		)
	}
	for index, issued := range restore.Issued {
		if issued.RangeIndex != planned.RangeIndex ||
			issued.Sequence != restore.NextSequence+uint64(index) ||
			issued.Rows <= 0 ||
			issued.Rows > config.MaxTransferChunkRows ||
			len(issued.StartFrontier) >
				maximumNetworkFrontierBytes ||
			len(issued.EndFrontier) == 0 ||
			len(issued.EndFrontier) >
				maximumNetworkFrontierBytes ||
			!validNetworkFactToken(issued.Fingerprint) ||
			index > 0 && !bytes.Equal(
				issued.StartFrontier,
				restore.Issued[index-1].EndFrontier,
			) ||
			index < len(restore.Issued)-1 && issued.Exhausted {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk inventory is malformed",
				ErrInvalidNetworkRestore,
			)
		}
		if index == 0 &&
			!bytes.Equal(issued.StartFrontier, restore.Frontier) {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk does not extend the safe frontier",
				ErrInvalidNetworkRestore,
			)
		}
		if int64(issued.Rows) >
			resources.MemoryBudget.Value/planned.MaxRowBytes {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk exceeds memory budget",
				ErrInvalidNetworkRestore,
			)
		}
	}
	if restore.SequenceOffset > 0 {
		if len(restore.Issued) == 0 ||
			restore.SequenceOffset >= int64(restore.Issued[0].Rows) {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: durable prefix has no matching issued chunk",
				ErrInvalidNetworkRestore,
			)
		}
	}
	return cloneNetworkRestore(restore), nil
}

func validNetworkRestoreRowSequenceEvidence(
	rowsDone int64,
	nextSequence uint64,
) bool {
	if rowsDone == 0 {
		return nextSequence == 0
	}
	if rowsDone < 0 ||
		nextSequence == 0 ||
		nextSequence > uint64(rowsDone) {
		return false
	}
	maxRows := uint64(config.MaxTransferChunkRows)
	completedRows := uint64(rowsDone)
	minimumSequences := completedRows / maxRows
	if completedRows%maxRows != 0 {
		minimumSequences++
	}
	return nextSequence >= minimumSequences
}

func validNetworkIdentifier(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	return utf8.ValidString(value) &&
		len(value) <= 512 &&
		!strings.ContainsRune(value, '\x00')
}

func validNetworkFactToken(value string) bool {
	if value == "" || len(value) > maximumNetworkFactToken {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

func validNetworkPagination(value PaginationStrategy) bool {
	switch value {
	case PaginationIntegerKeyset,
		PaginationTupleKeyset,
		PaginationRowNumber:
		return true
	default:
		return false
	}
}

func cloneNetworkRestore(
	restore NetworkRangeRestore,
) NetworkRangeRestore {
	result := restore
	result.Frontier = cloneNetworkBytes(restore.Frontier)
	result.Issued = make([]NetworkIssuedChunk, len(restore.Issued))
	for index, issued := range restore.Issued {
		result.Issued[index] = cloneNetworkIssuedChunk(issued)
	}
	return result
}

func cloneNetworkIssuedChunk(
	issued NetworkIssuedChunk,
) NetworkIssuedChunk {
	issued.StartFrontier = cloneNetworkBytes(issued.StartFrontier)
	issued.EndFrontier = cloneNetworkBytes(issued.EndFrontier)
	return issued
}

func cloneNetworkBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func (runtime *networkTransferRuntime) liveChunkRows() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.ChunkRows.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.ChunkRows.Value
}

// liveWriteChunkRows applies an immutable explicit merge ceiling only at the
// target write boundary. Source pagination continues to use liveChunkRows so
// issued pages, source snapshots, and durable replay identities are unchanged.
func (runtime *networkTransferRuntime) liveWriteChunkRows() int {
	rows := runtime.liveChunkRows()
	if runtime.plan.UpsertMergeRows > 0 &&
		runtime.plan.UpsertMergeRows < rows {
		return runtime.plan.UpsertMergeRows
	}
	return rows
}

func (runtime *networkTransferRuntime) liveWriterLimit() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.Writers.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.Writers.Value
}

func (runtime *networkTransferRuntime) liveQueueLimit() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.QueueDepth.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.BufferDepth.Value
}

func (runtime *networkTransferRuntime) readRange(
	ctx context.Context,
	state *networkRangeState,
	output chan<- *networkBufferedChunk,
) error {
	sequence := state.restore.NextSequence
	start := cloneNetworkBytes(state.restore.Frontier)
	exhausted := false
	for index, expected := range state.restore.Issued {
		request := NetworkReadRequest{
			Range:          state.plan,
			Sequence:       sequence,
			MaxRows:        expected.Rows,
			StartFrontier:  cloneNetworkBytes(start),
			ReplayExpected: pointerToNetworkIssued(expected),
		}
		chunk, err := runtime.readPage(ctx, state, request)
		if err != nil {
			return err
		}
		chunk.replay = true
		if index == 0 {
			chunk.initialOffset = state.restore.SequenceOffset
		}
		if err := runtime.emitChunk(ctx, output, chunk); err != nil {
			return err
		}
		start = cloneNetworkBytes(expected.EndFrontier)
		sequence++
		exhausted = expected.Exhausted
	}

	for !exhausted {
		maxRows := runtime.liveChunkRows()
		memoryRows := runtime.plan.Resources.MemoryBudget.Value /
			state.plan.MaxRowBytes
		if memoryRows < int64(maxRows) {
			maxRows = int(memoryRows)
		}
		if maxRows < 1 {
			return fmt.Errorf(
				"%w: row reservation admits no source page",
				ErrInvalidNetworkTransferPlan,
			)
		}
		request := NetworkReadRequest{
			Range:         state.plan,
			Sequence:      sequence,
			MaxRows:       maxRows,
			StartFrontier: cloneNetworkBytes(start),
		}
		chunk, err := runtime.readPage(ctx, state, request)
		if err != nil {
			return err
		}
		if len(chunk.page.Rows) == 0 {
			chunk.release()
			exhausted = true
			break
		}
		if err := runtime.emitChunk(ctx, output, chunk); err != nil {
			return err
		}
		start = cloneNetworkBytes(chunk.issued.EndFrontier)
		sequence++
		exhausted = chunk.issued.Exhausted
	}
	if err := state.turn.wait(ctx, sequence); err != nil {
		return err
	}
	if err := runtime.completeRange(ctx, state); err != nil {
		return err
	}
	return state.turn.advance(sequence)
}

func pointerToNetworkIssued(
	value NetworkIssuedChunk,
) *NetworkIssuedChunk {
	cloned := cloneNetworkIssuedChunk(value)
	return &cloned
}

func (runtime *networkTransferRuntime) readPage(
	ctx context.Context,
	state *networkRangeState,
	request NetworkReadRequest,
) (*networkBufferedChunk, error) {
	if request.MaxRows <= 0 ||
		int64(request.MaxRows) >
			runtime.plan.Resources.MemoryBudget.Value/
				state.plan.MaxRowBytes {
		return nil, fmt.Errorf(
			"%w: source request exceeds memory budget",
			ErrInvalidNetworkTransferPlan,
		)
	}
	reservationBytes := int64(request.MaxRows) *
		state.plan.MaxRowBytes
	reservation, err := runtime.budget.Acquire(
		ctx,
		reservationBytes,
	)
	if err != nil {
		return nil, err
	}
	var page NetworkReadPage
	retryRequest := request
	err = RetryWithPolicy(
		ctx,
		runtime.plan.RetryPolicy,
		func(ctx context.Context, attempt int) error {
			retryRequest.Attempt = attempt
			runtime.activity.addReader(1)
			current, readErr := runtime.callbacks.ReadPage(
				ctx,
				cloneNetworkReadRequest(retryRequest),
			)
			runtime.activity.addReader(-1)
			if readErr != nil {
				fact := ClassifyEngineRetry(
					runtime.plan.SourceEngine,
					EngineRetryReadOnly,
					readErr,
				)
				runtime.retryFacts.append(NetworkRetryFact{
					RangeIndex: request.Range.RangeIndex,
					Sequence:   request.Sequence,
					Attempt:    uint32(attempt - 1),
					Operation:  NetworkRetryRead,
					Fact:       fact,
				})
				return NewTransferError(fact.Class, readErr)
			}
			page = current
			return nil
		},
	)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	normalized, err := validateNetworkPage(
		page,
		request,
		state.plan,
		reservation.Bytes(),
	)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	issued := NetworkIssuedChunk{
		RangeIndex:    state.plan.RangeIndex,
		Sequence:      request.Sequence,
		Rows:          len(normalized.Rows),
		StartFrontier: cloneNetworkBytes(request.StartFrontier),
		EndFrontier:   cloneNetworkBytes(normalized.EndFrontier),
		Fingerprint:   normalized.Fingerprint,
		Exhausted:     normalized.Exhausted,
	}
	return &networkBufferedChunk{
		state:       state,
		issued:      issued,
		page:        normalized,
		reservation: reservation,
	}, nil
}

func cloneNetworkReadRequest(
	request NetworkReadRequest,
) NetworkReadRequest {
	request.StartFrontier = cloneNetworkBytes(request.StartFrontier)
	if request.ReplayExpected != nil {
		cloned := cloneNetworkIssuedChunk(*request.ReplayExpected)
		request.ReplayExpected = &cloned
	}
	return request
}

func validateNetworkPage(
	page NetworkReadPage,
	request NetworkReadRequest,
	planned NetworkRangePlan,
	reservedBytes int64,
) (NetworkReadPage, error) {
	if len(page.Rows) > request.MaxRows ||
		len(page.RowBytes) != len(page.Rows) ||
		len(page.EndFrontier) > maximumNetworkFrontierBytes {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: page shape exceeds the bounded request",
			ErrInvalidNetworkPage,
		)
	}
	if len(page.Rows) == 0 {
		if !page.Exhausted ||
			page.RetainedBytes != 0 ||
			page.Fingerprint != "" ||
			len(page.RowBytes) != 0 ||
			request.ReplayExpected != nil {
			return NetworkReadPage{}, fmt.Errorf(
				"%w: empty page must be a new terminal read with no payload facts",
				ErrInvalidNetworkPage,
			)
		}
		page.EndFrontier = cloneNetworkBytes(request.StartFrontier)
		return page, nil
	}
	if !validNetworkFactToken(page.Fingerprint) ||
		len(page.EndFrontier) == 0 ||
		bytes.Equal(page.EndFrontier, request.StartFrontier) {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: non-empty page lacks a stable advancing frontier",
			ErrInvalidNetworkPage,
		)
	}
	total := int64(0)
	for _, retained := range page.RowBytes {
		if retained <= 0 ||
			retained > planned.MaxRowBytes ||
			total > math.MaxInt64-retained {
			return NetworkReadPage{}, fmt.Errorf(
				"%w: retained row bytes are invalid",
				ErrInvalidNetworkPage,
			)
		}
		total += retained
	}
	if total != page.RetainedBytes ||
		total > reservedBytes {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: retained bytes differ from memory admission",
			ErrInvalidNetworkPage,
		)
	}
	if request.ReplayExpected != nil {
		expected := request.ReplayExpected
		if len(page.Rows) != expected.Rows ||
			!bytes.Equal(page.EndFrontier, expected.EndFrontier) ||
			page.Fingerprint != expected.Fingerprint ||
			page.Exhausted != expected.Exhausted {
			return NetworkReadPage{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: replay page differs from durable issued intent",
					ErrInvalidNetworkPage,
				),
			)
		}
	}
	// ReadPage relinquishes exclusive ownership of the page. Keep the payload
	// in its admitted backing buffers instead of briefly retaining an
	// unaccounted duplicate beside it.
	return page, nil
}

func (runtime *networkTransferRuntime) emitChunk(
	ctx context.Context,
	output chan<- *networkBufferedChunk,
	chunk *networkBufferedChunk,
) error {
	if err := runtime.queueGate.acquire(
		ctx,
		runtime.liveQueueLimit,
	); err != nil {
		chunk.release()
		return err
	}
	select {
	case <-ctx.Done():
		runtime.queueGate.release()
		chunk.release()
		return ctx.Err()
	case output <- chunk:
		return nil
	}
}

func (runtime *networkTransferRuntime) processChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
) error {
	if err := chunk.state.turn.wait(
		ctx,
		chunk.issued.Sequence,
	); err != nil {
		return err
	}
	if !bytes.Equal(
		chunk.issued.StartFrontier,
		chunk.state.frontier,
	) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: issued chunk does not extend the durable frontier",
				ErrInvalidNetworkRestore,
			),
		)
	}
	if !chunk.replay {
		runtime.stateMu.Lock()
		err := runtime.callbacks.RecordIssued(
			ctx,
			cloneNetworkIssuedChunk(chunk.issued),
		)
		runtime.stateMu.Unlock()
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("record issued network chunk: %w", err),
			)
		}
	}
	if err := runtime.writeChunk(ctx, chunk); err != nil {
		return err
	}
	return chunk.state.turn.advance(chunk.issued.Sequence)
}

func (runtime *networkTransferRuntime) writeChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
) error {
	offset := chunk.initialOffset
	if offset < 0 || offset >= int64(len(chunk.page.Rows)) {
		return fmt.Errorf(
			"%w: chunk write offset is invalid",
			ErrInvalidNetworkRestore,
		)
	}
	var attempt uint32
	retryFailures := 0
	delay := runtime.plan.RetryPolicy.InitialBackoff
	if runtime.plan.RetryPolicy.MaxBackoff > 0 &&
		delay > runtime.plan.RetryPolicy.MaxBackoff {
		delay = runtime.plan.RetryPolicy.MaxBackoff
	}
	for offset < int64(len(chunk.page.Rows)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := len(chunk.page.Rows) - int(offset)
		attemptRows := runtime.liveWriteChunkRows()
		if attemptRows > remaining {
			attemptRows = remaining
		}
		if attemptRows < 1 {
			return fmt.Errorf(
				"%w: live chunk limit is not positive",
				ErrInvalidNetworkTransferPlan,
			)
		}
		end := int(offset) + attemptRows
		mode := runtime.writeMode(chunk)
		request := NetworkWriteRequest{
			Range:         chunk.state.plan,
			Sequence:      chunk.issued.Sequence,
			Attempt:       attempt,
			AttemptOffset: offset,
			Mode:          mode,
			Rows:          chunk.page.Rows[int(offset):end],
		}
		if err := runtime.writerGate.acquire(
			ctx,
			runtime.liveWriterLimit,
		); err != nil {
			return err
		}
		receipt, writeErr := runtime.callbacks.WritePage(
			ctx,
			request,
		)
		runtime.writerGate.release()
		protocolLimit := isNetworkWriteProtocolLimit(writeErr)
		attemptBytes, bytesErr := networkRowBytes(
			chunk.page.RowBytes[int(offset):end],
		)
		if bytesErr != nil {
			return bytesErr
		}
		if receiptErr := validateNetworkWriteReceipt(
			receipt,
			offset,
			int64(attemptRows),
		); receiptErr != nil {
			if observeErr := runtime.tuning.observe(
				ctx,
				chunk,
				attemptRows,
				attemptBytes,
				RuntimeWriteFatalError,
			); observeErr != nil {
				return observeErr
			}
			if writeErr != nil && !protocolLimit {
				receiptErr = errors.Join(receiptErr, writeErr)
			}
			return NewTransferError(
				ErrorClassState,
				receiptErr,
			)
		}
		if protocolLimit &&
			receipt.Certainty != CommitNotCommitted {
			if observeErr := runtime.tuning.observe(
				ctx,
				chunk,
				attemptRows,
				attemptBytes,
				RuntimeWriteFatalError,
			); observeErr != nil {
				return observeErr
			}
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: protocol-limit signal requires a not-committed receipt",
					ErrInvalidWriteReceipt,
				),
			)
		}

		fact, classified := runtime.classifyWrite(
			chunk,
			mode,
			receipt,
			writeErr,
		)
		if classified != nil {
			runtime.retryFacts.append(NetworkRetryFact{
				RangeIndex: chunk.issued.RangeIndex,
				Sequence:   chunk.issued.Sequence,
				Attempt:    attempt,
				Operation:  NetworkRetryWrite,
				Fact:       fact,
			})
		}
		acknowledged := receipt.AcknowledgedRows()
		completesChunk :=
			acknowledged > 0 &&
				offset+acknowledged == int64(len(chunk.page.Rows))
		outcome := RuntimeWriteSucceeded
		if classified != nil {
			if protocolLimit &&
				fact.Class == ErrorClassTransient {
				outcome = RuntimeWriteProtocolError
			} else if fact.Class == ErrorClassTransient {
				outcome = RuntimeWriteRetryableError
			} else {
				outcome = RuntimeWriteFatalError
			}
			if completesChunk {
				outcome = RuntimeWriteFatalError
			}
		} else if receipt.Certainty == CommitNotCommitted {
			outcome = RuntimeWriteFatalError
		}
		if err := runtime.tuning.observe(
			ctx,
			chunk,
			attemptRows,
			attemptBytes,
			outcome,
		); err != nil {
			return err
		}

		if acknowledged > 0 {
			frontier, err := chunk.state.tracker.Acknowledge(
				chunk.issued.Sequence,
				int64(len(chunk.page.Rows)),
				receipt,
			)
			if err != nil {
				return NewTransferError(
					ErrorClassState,
					err,
				)
			}
			if err := runtime.checkpointAcknowledgedChunk(
				ctx,
				chunk,
				frontier,
			); err != nil {
				return err
			}
			offset += acknowledged
		}
		if offset == int64(len(chunk.page.Rows)) {
			if classified != nil {
				return classified
			}
			return nil
		}

		switch receipt.Certainty {
		case CommitUnknown:
			if classified != nil {
				return classified
			}
			return NewTransferError(
				ErrorClassState,
				ErrUnknownNetworkCommit,
			)
		case CommitNotCommitted:
			if classified == nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"%w: target returned no durable progress and no error",
						ErrInvalidWriteReceipt,
					),
				)
			}
		case CommitDurable, CommitDurablePrefix:
			if classified == nil {
				if attempt == math.MaxUint32 {
					return fmt.Errorf(
						"%w: write attempt counter overflow",
						ErrInvalidNetworkTransferPlan,
					)
				}
				attempt++
				continue
			}
		}
		if classified == nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: target write made no usable progress",
					ErrInvalidWriteReceipt,
				),
			)
		}
		if !IsRetryable(classified) ||
			retryFailures >= runtime.plan.RetryPolicy.MaxRetries {
			return classified
		}
		retryFailures++
		if err := waitForRetry(ctx, delay); err != nil {
			return err
		}
		delay = nextRetryDelay(
			delay,
			runtime.plan.RetryPolicy.MaxBackoff,
		)
		if attempt == math.MaxUint32 {
			return fmt.Errorf(
				"%w: write attempt counter overflow",
				ErrInvalidNetworkTransferPlan,
			)
		}
		attempt++
	}
	return nil
}

func validateNetworkWriteReceipt(
	receipt WriteReceipt,
	offset int64,
	attempted int64,
) error {
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf(
			"%w: target returned an invalid write receipt",
			ErrInvalidWriteReceipt,
		)
	}
	if receipt.AttemptOffset != offset ||
		receipt.AttemptedRows != attempted {
		return fmt.Errorf(
			"%w: target receipt does not match the exact attempt",
			ErrInvalidWriteReceipt,
		)
	}
	return nil
}

func networkRowBytes(values []int64) (int64, error) {
	total := int64(0)
	for _, value := range values {
		if value <= 0 || total > math.MaxInt64-value {
			return 0, fmt.Errorf(
				"%w: attempt byte count is invalid",
				ErrInvalidNetworkPage,
			)
		}
		total += value
	}
	return total, nil
}

func (runtime *networkTransferRuntime) writeMode(
	chunk *networkBufferedChunk,
) NetworkWriteMode {
	if runtime.plan.Resources.TargetMode == "upsert" {
		return NetworkWriteIdempotentUpsert
	}
	if chunk.replay {
		return NetworkWriteDuplicateSafeInsertOnly
	}
	return NetworkWriteFreshInsert
}

func (runtime *networkTransferRuntime) classifyWrite(
	chunk *networkBufferedChunk,
	mode NetworkWriteMode,
	receipt WriteReceipt,
	writeErr error,
) (EngineRetryFact, error) {
	if receipt.Certainty == CommitUnknown {
		cause := error(ErrUnknownNetworkCommit)
		if writeErr != nil {
			cause = errors.Join(ErrUnknownNetworkCommit, writeErr)
		}
		fact := ClassifyEngineRetry(
			runtime.plan.TargetEngine,
			EngineRetryUnknownCommit,
			cause,
		)
		return fact, NewTransferError(fact.Class, cause)
	}
	if writeErr == nil {
		return EngineRetryFact{}, nil
	}
	boundary := EngineRetryRolledBack
	if mode == NetworkWriteIdempotentUpsert ||
		mode == NetworkWriteDuplicateSafeInsertOnly {
		boundary = EngineRetryIdempotent
	}
	if isNetworkWriteProtocolLimit(writeErr) {
		class := ErrorClassPermanent
		reason := "runtime_tuning_unavailable"
		if runtime.plan.RuntimeTuning != nil {
			class = ErrorClassTransient
			reason = "runtime_chunk_reduction"
		}
		fact := EngineRetryFact{
			Engine:   runtime.plan.TargetEngine,
			Boundary: boundary,
			Class:    class,
			Code:     "protocol_limit",
			Reason:   reason,
		}
		return fact, NewTransferError(
			class,
			&NetworkWriteProtocolLimitError{},
		)
	}
	fact := ClassifyEngineRetry(
		runtime.plan.TargetEngine,
		boundary,
		writeErr,
	)
	return fact, NewTransferError(fact.Class, writeErr)
}

func isNetworkWriteProtocolLimit(err error) bool {
	var protocolLimit *NetworkWriteProtocolLimitError
	return errors.As(err, &protocolLimit)
}

func (runtime *networkTransferRuntime) checkpointChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
	frontier AckFrontier,
) error {
	if frontier.Rows > math.MaxInt64-chunk.state.baseRows {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: checkpoint row count overflow",
				ErrInvalidAcknowledgement,
			),
		)
	}
	frontier.Rows += chunk.state.baseRows
	frontierBytes := chunk.state.frontier
	if frontier.NextSequence > chunk.issued.Sequence {
		frontierBytes = chunk.issued.EndFrontier
	}
	checkpoint := NetworkRangeCheckpoint{
		RangeIndex:    chunk.state.plan.RangeIndex,
		TopologyHash:  chunk.state.plan.TopologyHash,
		Frontier:      frontier,
		FrontierBytes: cloneNetworkBytes(frontierBytes),
		Memory:        runtime.budget.Stats(),
	}
	runtime.stateMu.Lock()
	err := runtime.callbacks.Checkpoint(ctx, checkpoint)
	runtime.stateMu.Unlock()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("persist network range checkpoint: %w", err),
		)
	}
	chunk.state.safeRows = frontier.Rows
	chunk.state.frontier = cloneNetworkBytes(frontierBytes)
	chunk.state.pendingCheckpointAcks = 0
	return nil
}

// checkpointAcknowledgedChunk applies the configured periodic cadence only
// after ContiguousAckTracker has established the safe range frontier. Deferred
// acknowledgements still advance the in-memory logical frontier so subsequent
// issued pages cannot overlap; they do not change safeRows or durable state.
// On a crash, their already-recorded issued chunks replay through the existing
// idempotent/duplicate-safe route rather than being mistaken for checkpointed
// progress.
func (runtime *networkTransferRuntime) checkpointAcknowledgedChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
	frontier AckFrontier,
) error {
	if runtime.plan.CheckpointFrequency == 0 {
		return runtime.checkpointChunk(ctx, chunk, frontier)
	}
	chunk.state.pendingCheckpointAcks++
	if chunk.state.pendingCheckpointAcks >=
		runtime.plan.CheckpointFrequency {
		return runtime.checkpointChunk(ctx, chunk, frontier)
	}
	// A partial prefix has no new typed end frontier. For a complete chunk,
	// preserve its end frontier locally so the next issued sequence proves it
	// extends the same logical source position even though persistence waits for
	// the next cadence boundary.
	if frontier.NextSequence > chunk.issued.Sequence {
		chunk.state.frontier = cloneNetworkBytes(chunk.issued.EndFrontier)
	}
	return nil
}

func (runtime *networkTransferRuntime) completeRange(
	ctx context.Context,
	state *networkRangeState,
) error {
	frontier := state.tracker.Frontier()
	if frontier.NextSequence != state.turn.next ||
		frontier.SequenceOffset != 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: range completion has an incomplete durable frontier",
				ErrInvalidAcknowledgement,
			),
		)
	}
	if frontier.Rows > math.MaxInt64-state.baseRows {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: completion row count overflow",
				ErrInvalidAcknowledgement,
			),
		)
	}
	frontier.Rows += state.baseRows
	checkpoint := NetworkRangeCheckpoint{
		RangeIndex:    state.plan.RangeIndex,
		TopologyHash:  state.plan.TopologyHash,
		Frontier:      frontier,
		FrontierBytes: cloneNetworkBytes(state.frontier),
		Complete:      true,
		Memory:        runtime.budget.Stats(),
	}
	runtime.stateMu.Lock()
	err := runtime.callbacks.Checkpoint(ctx, checkpoint)
	runtime.stateMu.Unlock()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("persist network range completion: %w", err),
		)
	}
	state.safeRows = frontier.Rows
	state.pendingCheckpointAcks = 0
	state.complete = true
	return nil
}

func (runtime *networkTransferRuntime) result(
	states []*networkRangeState,
) (NetworkTransferResult, error) {
	result := NetworkTransferResult{
		Pagination: make(
			[]NetworkPaginationFact,
			0,
			len(states),
		),
		Retries: runtime.retryFacts.snapshot(),
		Memory:  runtime.budget.Stats(),
	}
	for _, state := range states {
		if state.safeRows > math.MaxInt64-result.Rows {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: aggregate checkpoint row count overflow",
					ErrInvalidAcknowledgement,
				),
			)
		}
		result.Rows += state.safeRows
		if state.complete {
			result.CompletedRanges++
		}
		result.Pagination = append(
			result.Pagination,
			NetworkPaginationFact{
				RangeIndex:   state.plan.RangeIndex,
				TableSchema:  state.plan.TableSchema,
				TableName:    state.plan.TableName,
				TopologyHash: state.plan.TopologyHash,
				Pagination:   state.plan.Pagination,
			},
		)
	}
	if runtime.plan.RuntimeTuning != nil {
		result.HasRuntimeTuning = true
		result.RuntimeTuning =
			runtime.plan.RuntimeTuning.Snapshot()
	}
	return result, nil
}
