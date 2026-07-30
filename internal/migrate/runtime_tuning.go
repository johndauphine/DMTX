package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/config"
)

var (
	ErrInvalidRuntimeTuningPlan = errors.New(
		"invalid runtime tuning plan",
	)
	ErrInvalidRuntimeObservation = errors.New(
		"invalid runtime tuning observation",
	)
	ErrNonMonotonicRuntimeObservation = errors.New(
		"non-monotonic runtime tuning observation",
	)
)

const (
	maximumRuntimeTuningRanges  = uint64(1 << 20)
	maximumRuntimeTuningColumns = 1 << 20
)

// RuntimeTuningLimits are immutable engine/workload safety evidence. Values
// are resolved before transfer; the runtime controller never probes the host,
// rewrites configuration, or guesses a missing protocol ceiling.
type RuntimeTuningLimits struct {
	ProtocolMaxChunkRows         int
	ProtocolMaxChunkBytes        int64
	SafetyRowWidthUpperBound     int64
	PlannedRanges                uint64
	ExpectedColumnCount          int
	HistoryLimit                 int
	GrowthAfterHealthyBoundaries uint64
}

// RuntimeTuningProvenance distinguishes immutable configuration provenance
// from the cause of the current live value.
type RuntimeTuningProvenance string

const (
	RuntimeTuningInitial         RuntimeTuningProvenance = "initial"
	RuntimeTuningSafetyReduction RuntimeTuningProvenance = "safety_reduction"
	RuntimeTuningEvidenceGrowth  RuntimeTuningProvenance = "evidence_growth"
)

// RuntimeTuningValue discloses one live value without losing its immutable
// pre-run intent and provenance.
type RuntimeTuningValue struct {
	Value             int
	IntentValue       int
	IntentProvenance  config.SettingProvenance
	LiveProvenance    RuntimeTuningProvenance
	PerformancePinned bool
}

// RuntimeTuningValues are the only resources this bounded controller adjusts.
// Readers, workers, memory budget, and connection limit remain immutable.
type RuntimeTuningValues struct {
	ChunkRows   RuntimeTuningValue
	Writers     RuntimeTuningValue
	BufferDepth RuntimeTuningValue
}

// RuntimeTuningReason is a stable, secret-free explanation. Driver error text
// and row values never enter controller state or decision history.
type RuntimeTuningReason string

const (
	RuntimeReasonProtocolCeiling      RuntimeTuningReason = "protocol_ceiling"
	RuntimeReasonMemoryCeiling        RuntimeTuningReason = "memory_ceiling"
	RuntimeReasonConnectionCeiling    RuntimeTuningReason = "connection_ceiling"
	RuntimeReasonMemoryPressure       RuntimeTuningReason = "memory_pressure"
	RuntimeReasonQueuePressure        RuntimeTuningReason = "queue_pressure"
	RuntimeReasonConnectionPressure   RuntimeTuningReason = "connection_pressure"
	RuntimeReasonWriteError           RuntimeTuningReason = "write_error"
	RuntimeReasonProtocolWriteError   RuntimeTuningReason = "protocol_write_error"
	RuntimeReasonEvidenceGrowth       RuntimeTuningReason = "evidence_growth"
	RuntimeReasonInsufficientEvidence RuntimeTuningReason = "insufficient_evidence"
	RuntimeReasonHeadroomUnavailable  RuntimeTuningReason = "headroom_unavailable"
	RuntimeReasonHealthyObservation   RuntimeTuningReason = "healthy_observation"
	RuntimeReasonPinnedCeiling        RuntimeTuningReason = "pinned_ceiling"
	RuntimeReasonEffectiveCeiling     RuntimeTuningReason = "effective_ceiling"
)

// RuntimeWriteOutcome deliberately carries only a stable class. Callers keep
// raw, potentially credential-bearing driver errors outside this controller.
type RuntimeWriteOutcome string

const (
	RuntimeWriteSucceeded      RuntimeWriteOutcome = "succeeded"
	RuntimeWriteRetryableError RuntimeWriteOutcome = "retryable_error"
	RuntimeWriteProtocolError  RuntimeWriteOutcome = "protocol_error"
	RuntimeWriteFatalError     RuntimeWriteOutcome = "fatal_error"
)

// RuntimeTuningBoundary is the exact completed/failed chunk transition at
// which a decision may become effective. Ordinal is migration-global and
// contiguous. RangeIndex is the durable planned-range ordinal, avoiding an
// arbitrary caller-supplied range string in decision history.
type RuntimeTuningBoundary struct {
	Ordinal       uint64
	TableSchema   string
	TableName     string
	RangeIndex    uint64
	ChunkSequence uint64
	Attempt       uint32
}

// RuntimeResourceInventory is a complete point-in-time inventory. Zero values
// are required when Complete is false so partial evidence cannot accidentally
// authorize growth.
type RuntimeResourceInventory struct {
	Complete        bool
	PlannedRanges   uint64
	ConnectionLimit int
	OpenConnections int
	ActiveReaders   int
	ActiveWriters   int
	QueueDepth      int
	ByteBudget      ByteBudgetStats
}

// RuntimeRowWidthEvidence is a schema/converter-proven retained-row upper
// bound, not a sample average. CompleteColumnCount must equal the immutable
// selected-column inventory in RuntimeTuningLimits. Once admitted, a run
// cannot reinterpret this proof.
type RuntimeRowWidthEvidence struct {
	Trustworthy         bool
	CompleteColumnCount int
	ExpectedColumnCount int
	UpperBoundBytes     int64
}

// RuntimeTuningObservation contains bounded numeric facts for one explicit
// chunk boundary. ObservedBytes is the exact retained in-memory row payload,
// excluding transport framing. Cumulative values must equal the prior
// cumulative value plus this boundary's observed work, preventing reordered
// or dropped evidence.
type RuntimeTuningObservation struct {
	Boundary RuntimeTuningBoundary

	ObservedRows            uint64
	ObservedBytes           uint64
	CumulativeObservedRows  uint64
	CumulativeObservedBytes uint64

	Inventory RuntimeResourceInventory
	RowWidth  RuntimeRowWidthEvidence

	MemoryPressure     bool
	QueuePressure      bool
	ConnectionPressure bool
	WriteOutcome       RuntimeWriteOutcome
}

// RuntimeTuningDecision is one bounded, ordered, secret-free audit fact.
type RuntimeTuningDecision struct {
	Boundary RuntimeTuningBoundary
	Before   RuntimeTuningValues
	After    RuntimeTuningValues
	Reasons  []RuntimeTuningReason
}

// RuntimeTuningSnapshot is an atomic status view. Intent remains exactly the
// immutable EffectiveTransferPlan supplied to the constructor.
type RuntimeTuningSnapshot struct {
	Intent                config.EffectiveTransferPlan
	Effective             RuntimeTuningValues
	InitializationReasons []RuntimeTuningReason
	HasBoundary           bool
	LastBoundary          RuntimeTuningBoundary
	AppliedBoundaries     uint64
	TotalDecisions        uint64
	RetainedDecisions     int
	HealthyBoundaries     uint64
	TrustedRowWidthBytes  int64
}

type runtimeRangeProgress struct {
	identity string
	chunk    uint64
	attempt  uint32
}

// RuntimeTuningController applies deterministic changes only through
// ApplyChunkBoundary. All returned slices are copies, and every method is safe
// for concurrent use.
type RuntimeTuningController struct {
	mu sync.Mutex

	intent config.EffectiveTransferPlan
	limits RuntimeTuningLimits
	values RuntimeTuningValues

	initializationReasons []RuntimeTuningReason
	history               []RuntimeTuningDecision
	totalDecisions        uint64
	healthyBoundaries     uint64
	trustedRowWidth       int64
	trustedRowEvidence    RuntimeRowWidthEvidence

	hasObservation          bool
	lastObservation         RuntimeTuningObservation
	cumulativeRows          uint64
	cumulativeBytes         uint64
	lastBudgetPeak          int64
	rangeProgress           map[uint64]runtimeRangeProgress
	protocolFailureChunkCap int
}

// NewRuntimeTuningController validates and copies immutable pre-run intent.
// Protocol/memory/connection reductions are applied before the first chunk
// with visible safety provenance. Subsequent changes can occur only through
// ApplyChunkBoundary.
func NewRuntimeTuningController(
	plan config.EffectiveTransferPlan,
	limits RuntimeTuningLimits,
) (*RuntimeTuningController, error) {
	if err := validateRuntimeTuningPlan(plan, limits); err != nil {
		return nil, err
	}
	controller := &RuntimeTuningController{
		intent:        plan,
		limits:        limits,
		rangeProgress: make(map[uint64]runtimeRangeProgress),
		values: RuntimeTuningValues{
			ChunkRows:   newRuntimeTuningValue(plan.ChunkRows),
			Writers:     newRuntimeTuningValue(plan.Writers),
			BufferDepth: newRuntimeTuningValue(plan.QueueDepth),
		},
	}
	controller.applyInitialSafetyCeilings()
	if controller.values.ChunkRows.Value < 1 ||
		controller.values.Writers.Value < 1 ||
		controller.values.BufferDepth.Value < 1 {
		return nil, fmt.Errorf(
			"%w: safety ceilings admit no executable transfer settings",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	return controller, nil
}

// ApplyChunkBoundary validates and applies one observation atomically. An
// identical retry of the latest observation is idempotent. Any conflicting,
// skipped, reordered, or regressing observation fails without mutation.
func (controller *RuntimeTuningController) ApplyChunkBoundary(
	ctx context.Context,
	observation RuntimeTuningObservation,
) (RuntimeTuningSnapshot, error) {
	if controller == nil {
		return RuntimeTuningSnapshot{}, fmt.Errorf(
			"%w: nil controller",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if ctx == nil {
		return RuntimeTuningSnapshot{}, fmt.Errorf(
			"%w: nil context",
			ErrInvalidRuntimeObservation,
		)
	}
	if err := ctx.Err(); err != nil {
		return RuntimeTuningSnapshot{}, err
	}

	controller.mu.Lock()
	defer controller.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RuntimeTuningSnapshot{}, err
	}
	if controller.hasObservation &&
		observation.Boundary.Ordinal ==
			controller.lastObservation.Boundary.Ordinal {
		if reflect.DeepEqual(observation, controller.lastObservation) {
			return controller.snapshotLocked(), nil
		}
		return RuntimeTuningSnapshot{}, fmt.Errorf(
			"%w: boundary ordinal %d conflicts with prior evidence",
			ErrNonMonotonicRuntimeObservation,
			observation.Boundary.Ordinal,
		)
	}
	if err := controller.validateObservationLocked(observation); err != nil {
		return RuntimeTuningSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return RuntimeTuningSnapshot{}, err
	}

	before := controller.values
	reasons := controller.applyObservationLocked(observation)
	decision := RuntimeTuningDecision{
		Boundary: observation.Boundary,
		Before:   before,
		After:    controller.values,
		Reasons:  append([]RuntimeTuningReason(nil), reasons...),
	}
	controller.appendDecisionLocked(decision)
	controller.commitObservationLocked(observation)
	return controller.snapshotLocked(), nil
}

// Snapshot returns an atomic copy of immutable intent and live effective
// values.
func (controller *RuntimeTuningController) Snapshot() RuntimeTuningSnapshot {
	if controller == nil {
		return RuntimeTuningSnapshot{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.snapshotLocked()
}

// History returns the retained ordered decision suffix. Mutating the result
// cannot alter controller state.
func (controller *RuntimeTuningController) History() []RuntimeTuningDecision {
	if controller == nil {
		return nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return cloneRuntimeTuningHistory(controller.history)
}

func validateRuntimeTuningPlan(
	plan config.EffectiveTransferPlan,
	limits RuntimeTuningLimits,
) error {
	if plan.TargetMode != "drop_recreate" &&
		plan.TargetMode != "upsert" {
		return fmt.Errorf(
			"%w: effective transfer plan has unsupported target mode",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if plan.ConnectionLimit.Value < 2 ||
		plan.DetectedMemoryLimit.Value <= 0 ||
		plan.MemoryBudget.Value <
			config.MinimumTransferMemoryBytes ||
		plan.Workers.Value <= 0 ||
		plan.Workers.Value > config.MaxTransferWorkers ||
		plan.Readers.Value <= 0 ||
		plan.Readers.Value > config.MaxTransferReaders ||
		plan.Writers.Value <= 0 ||
		plan.Writers.Value > config.MaxTransferWriters ||
		plan.QueueDepth.Value <= 0 ||
		plan.QueueDepth.Value > config.MaxTransferQueueDepth ||
		plan.ChunkRows.Value <= 0 ||
		plan.ChunkRows.Value > config.MaxTransferChunkRows {
		return fmt.Errorf(
			"%w: effective transfer plan has out-of-range resources",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	settings := []struct {
		name  string
		value config.EffectiveInt
	}{
		{name: "connection limit", value: plan.ConnectionLimit},
		{name: "workers", value: plan.Workers},
		{name: "readers", value: plan.Readers},
		{name: "writers", value: plan.Writers},
		{name: "queue depth", value: plan.QueueDepth},
		{name: "chunk rows", value: plan.ChunkRows},
	}
	for _, setting := range settings {
		if !validRuntimeSettingProvenance(setting.value.Provenance) {
			return fmt.Errorf(
				"%w: %s has unsupported provenance",
				ErrInvalidRuntimeTuningPlan,
				setting.name,
			)
		}
	}
	if !validRuntimeDetectedMemoryProvenance(
		plan.DetectedMemoryLimit.Provenance,
	) {
		return fmt.Errorf(
			"%w: detected memory limit has unsupported provenance",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if !validRuntimeMemoryProvenance(plan.MemoryBudget.Provenance) {
		return fmt.Errorf(
			"%w: memory budget has unsupported provenance",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if plan.DetectedMemoryLimit.Value > 0 &&
		plan.MemoryBudget.Value > plan.DetectedMemoryLimit.Value {
		return fmt.Errorf(
			"%w: memory budget exceeds detected finite limit",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	totalConcurrency := int64(plan.Readers.Value) +
		int64(plan.Writers.Value)
	if totalConcurrency > int64(plan.ConnectionLimit.Value) ||
		totalConcurrency > int64(plan.Workers.Value) {
		return fmt.Errorf(
			"%w: readers and writers exceed connection or worker ceiling",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if limits.ProtocolMaxChunkRows <= 0 ||
		limits.ProtocolMaxChunkBytes <= 0 ||
		limits.SafetyRowWidthUpperBound <= 0 ||
		limits.PlannedRanges == 0 ||
		limits.PlannedRanges > maximumRuntimeTuningRanges ||
		limits.ExpectedColumnCount <= 0 ||
		limits.ExpectedColumnCount > maximumRuntimeTuningColumns ||
		limits.HistoryLimit <= 0 ||
		limits.HistoryLimit > 1024 ||
		limits.GrowthAfterHealthyBoundaries == 0 {
		return fmt.Errorf(
			"%w: runtime limits must be finite, positive, and bounded",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if limits.ProtocolMaxChunkRows > config.MaxTransferChunkRows {
		return fmt.Errorf(
			"%w: protocol row ceiling exceeds DMTX maximum",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if limits.SafetyRowWidthUpperBound >
		limits.ProtocolMaxChunkBytes {
		return fmt.Errorf(
			"%w: one conservatively bounded row exceeds protocol bytes",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	if limits.SafetyRowWidthUpperBound >
		plan.MemoryBudget.Value {
		return fmt.Errorf(
			"%w: one conservatively bounded row exceeds memory budget",
			ErrInvalidRuntimeTuningPlan,
		)
	}
	return nil
}

func validRuntimeDetectedMemoryProvenance(
	value config.SettingProvenance,
) bool {
	switch value {
	case config.ProvenanceHostAvailable,
		config.ProvenanceHostCapacity,
		config.ProvenanceCgroupV2Remaining,
		config.ProvenanceCgroupV1Remaining:
		return true
	default:
		return false
	}
}

func validRuntimeSettingProvenance(
	value config.SettingProvenance,
) bool {
	switch value {
	case config.ProvenanceDerived,
		config.ProvenanceRequested,
		config.ProvenanceSafetyClamped:
		return true
	default:
		return false
	}
}

func validRuntimeMemoryProvenance(
	value config.SettingProvenance,
) bool {
	switch value {
	case config.ProvenanceHostAvailable,
		config.ProvenanceHostCapacity,
		config.ProvenanceCgroupV2Remaining,
		config.ProvenanceCgroupV1Remaining,
		config.ProvenanceUserMemoryCeiling:
		return true
	default:
		return false
	}
}

func newRuntimeTuningValue(value config.EffectiveInt) RuntimeTuningValue {
	return RuntimeTuningValue{
		Value:             value.Value,
		IntentValue:       value.Value,
		IntentProvenance:  value.Provenance,
		LiveProvenance:    RuntimeTuningInitial,
		PerformancePinned: value.Provenance != config.ProvenanceDerived,
	}
}

func (controller *RuntimeTuningController) applyInitialSafetyCeilings() {
	writerCap := controller.writerHardCeiling()
	if controller.values.Writers.Value > writerCap {
		controller.setSafetyValue(
			&controller.values.Writers,
			writerCap,
		)
		controller.addInitializationReason(RuntimeReasonConnectionCeiling)
	}
	if controller.values.BufferDepth.Value >
		config.MaxTransferQueueDepth {
		controller.setSafetyValue(
			&controller.values.BufferDepth,
			config.MaxTransferQueueDepth,
		)
		controller.addInitializationReason(RuntimeReasonMemoryCeiling)
	}
	protocolCap := controller.protocolChunkCeiling(
		controller.limits.SafetyRowWidthUpperBound,
	)
	memoryCap := controller.memoryChunkCeiling(
		controller.limits.SafetyRowWidthUpperBound,
		controller.values.Writers.Value,
		controller.values.BufferDepth.Value,
	)
	chunkCap := minRuntimeInt(
		config.MaxTransferChunkRows,
		minRuntimeInt(protocolCap, memoryCap),
	)
	if controller.values.ChunkRows.Value > chunkCap {
		controller.setSafetyValue(&controller.values.ChunkRows, chunkCap)
		if chunkCap == protocolCap {
			controller.addInitializationReason(
				RuntimeReasonProtocolCeiling,
			)
		}
		if chunkCap == memoryCap {
			controller.addInitializationReason(
				RuntimeReasonMemoryCeiling,
			)
		}
	}
}

func (controller *RuntimeTuningController) addInitializationReason(
	reason RuntimeTuningReason,
) {
	controller.initializationReasons = appendRuntimeReason(
		controller.initializationReasons,
		reason,
	)
}

func (controller *RuntimeTuningController) setSafetyValue(
	value *RuntimeTuningValue,
	next int,
) {
	if next < 1 {
		next = 1
	}
	if next >= value.Value {
		return
	}
	value.Value = next
	value.LiveProvenance = RuntimeTuningSafetyReduction
}

func (controller *RuntimeTuningController) validateObservationLocked(
	observation RuntimeTuningObservation,
) error {
	expectedOrdinal := uint64(1)
	if controller.hasObservation {
		if controller.lastObservation.Boundary.Ordinal == math.MaxUint64 {
			return fmt.Errorf(
				"%w: boundary ordinal exhausted",
				ErrNonMonotonicRuntimeObservation,
			)
		}
		expectedOrdinal =
			controller.lastObservation.Boundary.Ordinal + 1
	}
	if observation.Boundary.Ordinal != expectedOrdinal {
		return fmt.Errorf(
			"%w: boundary ordinal=%d want=%d",
			ErrNonMonotonicRuntimeObservation,
			observation.Boundary.Ordinal,
			expectedOrdinal,
		)
	}
	if err := controller.validateBoundaryLocked(
		observation.Boundary,
	); err != nil {
		return err
	}
	if observation.ObservedRows == 0 ||
		observation.ObservedBytes == 0 {
		return fmt.Errorf(
			"%w: observed rows and bytes must be positive",
			ErrInvalidRuntimeObservation,
		)
	}
	if controller.cumulativeRows >
		math.MaxUint64-observation.ObservedRows ||
		controller.cumulativeBytes >
			math.MaxUint64-observation.ObservedBytes {
		return fmt.Errorf(
			"%w: cumulative counters overflow",
			ErrInvalidRuntimeObservation,
		)
	}
	if observation.CumulativeObservedRows !=
		controller.cumulativeRows+observation.ObservedRows ||
		observation.CumulativeObservedBytes !=
			controller.cumulativeBytes+observation.ObservedBytes {
		return fmt.Errorf(
			"%w: cumulative counters do not extend prior evidence exactly",
			ErrNonMonotonicRuntimeObservation,
		)
	}
	rowWidth := controller.limits.SafetyRowWidthUpperBound
	if observation.RowWidth.Trustworthy {
		rowWidth = observation.RowWidth.UpperBoundBytes
	}
	if !runtimeObservedBytesFitRowWidth(
		observation.ObservedRows,
		observation.ObservedBytes,
		rowWidth,
	) {
		return fmt.Errorf(
			"%w: observed retained bytes exceed row-width safety evidence",
			ErrInvalidRuntimeObservation,
		)
	}
	if err := controller.validateInventoryLocked(
		observation.Inventory,
	); err != nil {
		return err
	}
	if err := controller.validateRowWidthLocked(
		observation.RowWidth,
	); err != nil {
		return err
	}
	switch observation.WriteOutcome {
	case RuntimeWriteSucceeded,
		RuntimeWriteRetryableError,
		RuntimeWriteProtocolError,
		RuntimeWriteFatalError:
	default:
		return fmt.Errorf(
			"%w: unknown write outcome",
			ErrInvalidRuntimeObservation,
		)
	}
	return nil
}

func (controller *RuntimeTuningController) validateBoundaryLocked(
	boundary RuntimeTuningBoundary,
) error {
	if boundary.Ordinal == 0 ||
		boundary.TableName == "" ||
		boundary.RangeIndex >= controller.limits.PlannedRanges {
		return fmt.Errorf(
			"%w: incomplete or out-of-range execution boundary",
			ErrInvalidRuntimeObservation,
		)
	}
	identifiers := []struct {
		name  string
		value string
	}{
		{name: "schema", value: boundary.TableSchema},
		{name: "table", value: boundary.TableName},
	}
	for _, identifier := range identifiers {
		if !utf8.ValidString(identifier.value) ||
			len(identifier.value) > 512 ||
			strings.ContainsRune(identifier.value, '\x00') {
			return fmt.Errorf(
				"%w: %s identifier is invalid or unbounded",
				ErrInvalidRuntimeObservation,
				identifier.name,
			)
		}
	}
	identity := runtimeRangeIdentity(boundary)
	previous, exists := controller.rangeProgress[boundary.RangeIndex]
	if !exists {
		if uint64(len(controller.rangeProgress)) >=
			controller.limits.PlannedRanges {
			return fmt.Errorf(
				"%w: execution boundary exceeds complete range inventory",
				ErrInvalidRuntimeObservation,
			)
		}
		if boundary.ChunkSequence != 0 || boundary.Attempt != 0 {
			return fmt.Errorf(
				"%w: a range must start at chunk and attempt zero",
				ErrNonMonotonicRuntimeObservation,
			)
		}
		return nil
	}
	if previous.identity != identity {
		return fmt.Errorf(
			"%w: planned range ordinal changed identity",
			ErrNonMonotonicRuntimeObservation,
		)
	}
	switch {
	case boundary.ChunkSequence == previous.chunk:
		if previous.attempt == math.MaxUint32 ||
			boundary.Attempt != previous.attempt+1 {
			return fmt.Errorf(
				"%w: range attempt does not extend prior attempt",
				ErrNonMonotonicRuntimeObservation,
			)
		}
	case previous.chunk != math.MaxUint64 &&
		boundary.ChunkSequence == previous.chunk+1:
		if boundary.Attempt != 0 {
			return fmt.Errorf(
				"%w: a new chunk must start at attempt zero",
				ErrNonMonotonicRuntimeObservation,
			)
		}
	default:
		return fmt.Errorf(
			"%w: range chunk sequence skipped or regressed",
			ErrNonMonotonicRuntimeObservation,
		)
	}
	return nil
}

func (controller *RuntimeTuningController) validateInventoryLocked(
	inventory RuntimeResourceInventory,
) error {
	if !inventory.Complete {
		if inventory != (RuntimeResourceInventory{}) {
			return fmt.Errorf(
				"%w: incomplete inventory must not carry partial values",
				ErrInvalidRuntimeObservation,
			)
		}
		return nil
	}
	if inventory.PlannedRanges != controller.limits.PlannedRanges ||
		inventory.ConnectionLimit !=
			controller.intent.ConnectionLimit.Value ||
		inventory.ByteBudget.Limit !=
			controller.intent.MemoryBudget.Value {
		return fmt.Errorf(
			"%w: complete inventory differs from immutable plan",
			ErrInvalidRuntimeObservation,
		)
	}
	if inventory.OpenConnections < 0 ||
		inventory.ActiveReaders < 0 ||
		inventory.ActiveWriters < 0 ||
		inventory.QueueDepth < 0 ||
		inventory.OpenConnections > inventory.ConnectionLimit ||
		inventory.ActiveReaders > controller.intent.Readers.Value ||
		inventory.ActiveWriters > controller.writerHardCeiling() ||
		inventory.ActiveReaders+inventory.ActiveWriters >
			inventory.OpenConnections ||
		inventory.QueueDepth > config.MaxTransferQueueDepth {
		return fmt.Errorf(
			"%w: complete inventory contains impossible concurrency",
			ErrInvalidRuntimeObservation,
		)
	}
	budget := inventory.ByteBudget
	if budget.Current < 0 ||
		budget.Peak < budget.Current ||
		budget.Peak > budget.Limit ||
		budget.Current > budget.Limit {
		return fmt.Errorf(
			"%w: complete inventory contains impossible byte budget",
			ErrInvalidRuntimeObservation,
		)
	}
	if controller.hasObservation &&
		budget.Peak < controller.lastBudgetPeak {
		return fmt.Errorf(
			"%w: byte-budget peak regressed",
			ErrNonMonotonicRuntimeObservation,
		)
	}
	return nil
}

func (controller *RuntimeTuningController) validateRowWidthLocked(
	evidence RuntimeRowWidthEvidence,
) error {
	if !evidence.Trustworthy {
		if evidence != (RuntimeRowWidthEvidence{}) {
			return fmt.Errorf(
				"%w: untrusted row width must not carry partial values",
				ErrInvalidRuntimeObservation,
			)
		}
		return nil
	}
	if controller.trustedRowEvidence.Trustworthy &&
		evidence != controller.trustedRowEvidence {
		return fmt.Errorf(
			"%w: trustworthy row-width proof changed within the run",
			ErrNonMonotonicRuntimeObservation,
		)
	}
	if evidence.CompleteColumnCount <= 0 ||
		evidence.CompleteColumnCount != evidence.ExpectedColumnCount ||
		evidence.ExpectedColumnCount !=
			controller.limits.ExpectedColumnCount ||
		evidence.UpperBoundBytes <= 0 ||
		evidence.UpperBoundBytes >
			controller.limits.SafetyRowWidthUpperBound {
		return fmt.Errorf(
			"%w: row-width proof is incomplete or exceeds safety evidence",
			ErrInvalidRuntimeObservation,
		)
	}
	return nil
}

func (controller *RuntimeTuningController) applyObservationLocked(
	observation RuntimeTuningObservation,
) []RuntimeTuningReason {
	reasons := make([]RuntimeTuningReason, 0, 4)
	safetySignal := false
	if observation.MemoryPressure {
		safetySignal = true
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonMemoryPressure,
		)
		controller.reduceHalf(&controller.values.ChunkRows)
		controller.reduceHalf(&controller.values.BufferDepth)
		controller.reduceHalf(&controller.values.Writers)
	}
	if observation.QueuePressure {
		safetySignal = true
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonQueuePressure,
		)
		controller.reduceHalf(&controller.values.ChunkRows)
		controller.reduceHalf(&controller.values.BufferDepth)
	}
	if observation.ConnectionPressure {
		safetySignal = true
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonConnectionPressure,
		)
		controller.reduceHalf(&controller.values.Writers)
	}
	switch observation.WriteOutcome {
	case RuntimeWriteRetryableError, RuntimeWriteFatalError:
		safetySignal = true
		reasons = appendRuntimeReason(reasons, RuntimeReasonWriteError)
		controller.reduceHalf(&controller.values.ChunkRows)
		controller.reduceHalf(&controller.values.Writers)
	case RuntimeWriteProtocolError:
		safetySignal = true
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonProtocolWriteError,
		)
		next := controller.values.ChunkRows.Value / 2
		if observation.ObservedRows <= uint64(math.MaxInt) &&
			int(observation.ObservedRows) <
				controller.values.ChunkRows.Value {
			next = int(observation.ObservedRows) / 2
		}
		if next < 1 {
			next = 1
		}
		controller.setSafetyValue(&controller.values.ChunkRows, next)
		if controller.protocolFailureChunkCap == 0 ||
			controller.values.ChunkRows.Value <
				controller.protocolFailureChunkCap {
			controller.protocolFailureChunkCap =
				controller.values.ChunkRows.Value
		}
	}
	controller.enforceLiveSafetyCeilings(&reasons)

	if safetySignal {
		controller.healthyBoundaries = 0
		return reasons
	}
	if !controller.growthEvidenceComplete(observation) {
		controller.healthyBoundaries = 0
		return appendRuntimeReason(
			reasons,
			RuntimeReasonInsufficientEvidence,
		)
	}
	if !controller.hasGrowthHeadroom(observation) {
		controller.healthyBoundaries = 0
		return appendRuntimeReason(
			reasons,
			RuntimeReasonHeadroomUnavailable,
		)
	}
	if controller.healthyBoundaries <
		controller.limits.GrowthAfterHealthyBoundaries {
		controller.healthyBoundaries++
	}
	if controller.healthyBoundaries <
		controller.limits.GrowthAfterHealthyBoundaries {
		return appendRuntimeReason(
			reasons,
			RuntimeReasonHealthyObservation,
		)
	}

	grew, pinned, ceiling := controller.growWithEvidence(observation)
	if grew {
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonEvidenceGrowth,
		)
		controller.healthyBoundaries = 0
		return reasons
	}
	if pinned {
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonPinnedCeiling,
		)
	}
	if ceiling {
		reasons = appendRuntimeReason(
			reasons,
			RuntimeReasonEffectiveCeiling,
		)
	}
	if len(reasons) == 0 {
		reasons = append(reasons, RuntimeReasonHealthyObservation)
	}
	return reasons
}

func (controller *RuntimeTuningController) reduceHalf(
	value *RuntimeTuningValue,
) {
	next := value.Value / 2
	if next < 1 {
		next = 1
	}
	if next < value.Value {
		value.Value = next
		value.LiveProvenance = RuntimeTuningSafetyReduction
	}
}

func (controller *RuntimeTuningController) enforceLiveSafetyCeilings(
	reasons *[]RuntimeTuningReason,
) {
	writerCap := controller.writerHardCeiling()
	if controller.values.Writers.Value > writerCap {
		controller.setSafetyValue(&controller.values.Writers, writerCap)
		*reasons = appendRuntimeReason(
			*reasons,
			RuntimeReasonConnectionCeiling,
		)
	}
	width := controller.effectiveSafetyRowWidth()
	protocolCap := controller.protocolChunkCeiling(width)
	memoryCap := controller.memoryChunkCeiling(
		width,
		controller.values.Writers.Value,
		controller.values.BufferDepth.Value,
	)
	chunkCap := minRuntimeInt(protocolCap, memoryCap)
	if controller.values.ChunkRows.Value > chunkCap {
		controller.setSafetyValue(&controller.values.ChunkRows, chunkCap)
		if protocolCap == chunkCap {
			*reasons = appendRuntimeReason(
				*reasons,
				RuntimeReasonProtocolCeiling,
			)
		}
		if memoryCap == chunkCap {
			*reasons = appendRuntimeReason(
				*reasons,
				RuntimeReasonMemoryCeiling,
			)
		}
	}
}

func (controller *RuntimeTuningController) effectiveSafetyRowWidth() int64 {
	if controller.trustedRowWidth > 0 {
		return controller.trustedRowWidth
	}
	return controller.limits.SafetyRowWidthUpperBound
}

func (controller *RuntimeTuningController) growthEvidenceComplete(
	observation RuntimeTuningObservation,
) bool {
	return observation.Inventory.Complete &&
		observation.RowWidth.Trustworthy &&
		observation.WriteOutcome == RuntimeWriteSucceeded
}

func (controller *RuntimeTuningController) hasGrowthHeadroom(
	observation RuntimeTuningObservation,
) bool {
	inventory := observation.Inventory
	return inventory.ByteBudget.Current <=
		inventory.ByteBudget.Limit/2 &&
		inventory.QueueDepth <=
			controller.values.BufferDepth.Value/2 &&
		inventory.OpenConnections < inventory.ConnectionLimit
}

func (controller *RuntimeTuningController) growWithEvidence(
	observation RuntimeTuningObservation,
) (grew bool, pinned bool, ceiling bool) {
	width := observation.RowWidth.UpperBoundBytes
	inventory := observation.Inventory

	writerMaximum := controller.writerHardCeiling()
	if controller.values.Writers.PerformancePinned &&
		controller.values.Writers.IntentValue < writerMaximum {
		writerMaximum = controller.values.Writers.IntentValue
	}
	if inventory.OpenConnections < inventory.ConnectionLimit &&
		controller.values.Writers.Value < writerMaximum {
		candidate := controller.values.Writers.Value + 1
		if controller.values.ChunkRows.Value <=
			controller.memoryChunkCeiling(
				width,
				candidate,
				controller.values.BufferDepth.Value,
			) {
			controller.growValue(&controller.values.Writers, candidate)
			grew = true
		}
	} else if controller.values.Writers.PerformancePinned {
		pinned = true
	} else {
		ceiling = true
	}

	bufferMaximum := config.MaxTransferQueueDepth
	if controller.values.BufferDepth.PerformancePinned &&
		controller.values.BufferDepth.IntentValue < bufferMaximum {
		bufferMaximum = controller.values.BufferDepth.IntentValue
	}
	if controller.values.BufferDepth.Value < bufferMaximum {
		candidate := controller.values.BufferDepth.Value + 1
		if controller.values.ChunkRows.Value <=
			controller.memoryChunkCeiling(
				width,
				controller.values.Writers.Value,
				candidate,
			) {
			controller.growValue(
				&controller.values.BufferDepth,
				candidate,
			)
			grew = true
		}
	} else if controller.values.BufferDepth.PerformancePinned {
		pinned = true
	} else {
		ceiling = true
	}

	chunkMaximum := minRuntimeInt(
		controller.protocolChunkCeiling(width),
		controller.memoryChunkCeiling(
			width,
			controller.values.Writers.Value,
			controller.values.BufferDepth.Value,
		),
	)
	if controller.values.ChunkRows.PerformancePinned &&
		controller.values.ChunkRows.IntentValue < chunkMaximum {
		chunkMaximum = controller.values.ChunkRows.IntentValue
	}
	if controller.values.ChunkRows.Value < chunkMaximum {
		candidate := controller.values.ChunkRows.Value * 2
		if candidate < controller.values.ChunkRows.Value ||
			candidate > chunkMaximum {
			candidate = chunkMaximum
		}
		controller.growValue(&controller.values.ChunkRows, candidate)
		grew = true
	} else if controller.values.ChunkRows.PerformancePinned {
		pinned = true
	} else {
		ceiling = true
	}
	return grew, pinned, ceiling
}

func (controller *RuntimeTuningController) growValue(
	value *RuntimeTuningValue,
	next int,
) {
	if next <= value.Value {
		return
	}
	if value.PerformancePinned && next > value.IntentValue {
		next = value.IntentValue
	}
	if next <= value.Value {
		return
	}
	value.Value = next
	value.LiveProvenance = RuntimeTuningEvidenceGrowth
}

func (controller *RuntimeTuningController) writerHardCeiling() int {
	return maxRuntimeInt(
		1,
		minRuntimeInt(
			config.MaxTransferWriters,
			minRuntimeInt(
				controller.intent.ConnectionLimit.Value-
					controller.intent.Readers.Value,
				controller.intent.Workers.Value-
					controller.intent.Readers.Value,
			),
		),
	)
}

func (controller *RuntimeTuningController) protocolChunkCeiling(
	rowWidth int64,
) int {
	if rowWidth <= 0 {
		return 0
	}
	byteRows := controller.limits.ProtocolMaxChunkBytes / rowWidth
	if byteRows < 1 {
		return 0
	}
	if byteRows > int64(math.MaxInt) {
		byteRows = int64(math.MaxInt)
	}
	ceiling := minRuntimeInt(
		controller.limits.ProtocolMaxChunkRows,
		int(byteRows),
	)
	if controller.protocolFailureChunkCap > 0 {
		ceiling = minRuntimeInt(
			ceiling,
			controller.protocolFailureChunkCap,
		)
	}
	return ceiling
}

func (controller *RuntimeTuningController) memoryChunkCeiling(
	rowWidth int64,
	writers,
	buffers int,
) int {
	if rowWidth <= 0 || writers <= 0 || buffers <= 0 {
		return 0
	}
	slots := int64(writers) + int64(buffers)
	rows := controller.intent.MemoryBudget.Value / rowWidth / slots
	if rows < 1 {
		return 0
	}
	if rows > int64(math.MaxInt) {
		rows = int64(math.MaxInt)
	}
	return minRuntimeInt(config.MaxTransferChunkRows, int(rows))
}

func (controller *RuntimeTuningController) appendDecisionLocked(
	decision RuntimeTuningDecision,
) {
	controller.totalDecisions++
	if len(controller.history) == controller.limits.HistoryLimit {
		copy(controller.history, controller.history[1:])
		controller.history[len(controller.history)-1] = decision
		return
	}
	controller.history = append(controller.history, decision)
}

func (controller *RuntimeTuningController) commitObservationLocked(
	observation RuntimeTuningObservation,
) {
	controller.hasObservation = true
	controller.lastObservation = observation
	controller.cumulativeRows = observation.CumulativeObservedRows
	controller.cumulativeBytes = observation.CumulativeObservedBytes
	if observation.Inventory.Complete {
		controller.lastBudgetPeak = observation.Inventory.ByteBudget.Peak
	}
	if observation.RowWidth.Trustworthy {
		controller.trustedRowWidth =
			observation.RowWidth.UpperBoundBytes
		controller.trustedRowEvidence = observation.RowWidth
	}
	controller.rangeProgress[observation.Boundary.RangeIndex] =
		runtimeRangeProgress{
			identity: runtimeRangeIdentity(observation.Boundary),
			chunk:    observation.Boundary.ChunkSequence,
			attempt:  observation.Boundary.Attempt,
		}
}

func (controller *RuntimeTuningController) snapshotLocked() RuntimeTuningSnapshot {
	result := RuntimeTuningSnapshot{
		Intent:    controller.intent,
		Effective: controller.values,
		InitializationReasons: append(
			[]RuntimeTuningReason(nil),
			controller.initializationReasons...,
		),
		HasBoundary:          controller.hasObservation,
		AppliedBoundaries:    controller.totalDecisions,
		TotalDecisions:       controller.totalDecisions,
		RetainedDecisions:    len(controller.history),
		HealthyBoundaries:    controller.healthyBoundaries,
		TrustedRowWidthBytes: controller.trustedRowWidth,
	}
	if controller.hasObservation {
		result.LastBoundary = controller.lastObservation.Boundary
	}
	return result
}

func cloneRuntimeTuningHistory(
	history []RuntimeTuningDecision,
) []RuntimeTuningDecision {
	result := make([]RuntimeTuningDecision, len(history))
	for index, decision := range history {
		result[index] = decision
		result[index].Reasons = append(
			[]RuntimeTuningReason(nil),
			decision.Reasons...,
		)
	}
	return result
}

func runtimeRangeIdentity(boundary RuntimeTuningBoundary) string {
	return boundary.TableSchema + "\x00" +
		boundary.TableName
}

func runtimeObservedBytesFitRowWidth(
	rows uint64,
	bytes uint64,
	rowWidth int64,
) bool {
	if rows == 0 || rowWidth <= 0 {
		return false
	}
	width := uint64(rowWidth)
	if rows > math.MaxUint64/width {
		return true
	}
	return bytes <= rows*width
}

func appendRuntimeReason(
	reasons []RuntimeTuningReason,
	reason RuntimeTuningReason,
) []RuntimeTuningReason {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

func minRuntimeInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxRuntimeInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
