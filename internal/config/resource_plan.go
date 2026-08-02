package config

import (
	"context"
	"fmt"
	"runtime"
)

// CgroupLimitState describes whether a control-group memory limit is usable.
type CgroupLimitState string

const (
	CgroupLimitAbsent    CgroupLimitState = "absent"
	CgroupLimitFinite    CgroupLimitState = "finite"
	CgroupLimitUnlimited CgroupLimitState = "unlimited"
	CgroupLimitUnknown   CgroupLimitState = "unknown"
)

// CgroupMemoryEvidence is one process-scoped cgroup memory observation.
type CgroupMemoryEvidence struct {
	State        CgroupLimitState
	LimitBytes   int64
	CurrentBytes int64
}

// MemorySnapshot contains the host and process-container evidence used once
// when constructing a transfer plan.
type MemorySnapshot struct {
	HostCapacityBytes  int64
	HostAvailableBytes int64
	CgroupV2           CgroupMemoryEvidence
	CgroupV1           CgroupMemoryEvidence
}

// MemoryProbe makes host/container evidence injectable and deterministic in
// tests.
type MemoryProbe interface {
	ProbeMemory(context.Context) (MemorySnapshot, error)
}

// TransferPlanOptions permits programmatic overrides of parsed migration
// settings. Zero inherits the corresponding Migration field or a safe default.
type TransferPlanOptions struct {
	UserMemoryCeilingBytes int64
	LogicalCPUs            int
	RequestedWorkers       int
	RequestedReaders       int
	RequestedWriters       int
	RequestedQueueDepth    int
	RequestedChunkRows     int
}

// SettingProvenance explains why an effective resource value was selected.
type SettingProvenance string

const (
	ProvenanceHostAvailable     SettingProvenance = "host_available"
	ProvenanceHostCapacity      SettingProvenance = "host_capacity"
	ProvenanceCgroupV2Remaining SettingProvenance = "cgroup_v2_remaining"
	ProvenanceCgroupV1Remaining SettingProvenance = "cgroup_v1_remaining"
	ProvenanceUserMemoryCeiling SettingProvenance = "user_memory_ceiling"
	ProvenanceDerived           SettingProvenance = "derived"
	ProvenanceRequested         SettingProvenance = "requested"
	ProvenanceSafetyClamped     SettingProvenance = "safety_clamped"
)

// EffectiveBytes is a byte value with its selection provenance.
type EffectiveBytes struct {
	Value      int64
	Provenance SettingProvenance
}

// EffectiveInt is an integer resource value with its selection provenance.
type EffectiveInt struct {
	Value      int
	Provenance SettingProvenance
}

// EffectiveTransferPlan is immutable runtime resource policy derived from the
// parsed migration intent and one memory snapshot.
type EffectiveTransferPlan struct {
	TargetMode          string
	ConnectionLimit     EffectiveInt
	DetectedMemoryLimit EffectiveBytes
	MemoryBudget        EffectiveBytes
	Workers             EffectiveInt
	Readers             EffectiveInt
	Writers             EffectiveInt
	QueueDepth          EffectiveInt
	ChunkRows           EffectiveInt
}

const (
	MinimumTransferMemoryBytes int64 = 1 << 20
	TransferMemoryPerSlotBytes int64 = 8 << 20
	AssumedRetainedRowBytes    int64 = 16 << 10
	DefaultTransferChunkRows         = 500
	MaxTransferWorkers               = 32
	MaxTransferReaders               = 16
	MaxTransferWriters               = 8
	MaxTransferQueueDepth            = 64
	MaxTransferChunkRows             = 10_000
)

// ResolveSystemEffectiveTransferPlan resolves a plan with the platform memory
// probe. Unsupported or insufficient platform evidence fails closed.
func ResolveSystemEffectiveTransferPlan(ctx context.Context, migration Migration, options TransferPlanOptions) (EffectiveTransferPlan, error) {
	return ResolveEffectiveTransferPlan(ctx, migration, options, NewSystemMemoryProbe())
}

// ResolveEffectiveTransferPlan derives bounded runtime settings without
// mutating the supplied Migration intent.
func ResolveEffectiveTransferPlan(ctx context.Context, migration Migration, options TransferPlanOptions, probe MemoryProbe) (EffectiveTransferPlan, error) {
	if err := ctx.Err(); err != nil {
		return EffectiveTransferPlan{}, err
	}
	if probe == nil {
		return EffectiveTransferPlan{}, fmt.Errorf("resolve transfer resources: memory probe is required")
	}
	options = inheritMigrationTransferOptions(migration, options)
	if err := validateTransferPlanOptions(options); err != nil {
		return EffectiveTransferPlan{}, err
	}

	targetMode := migration.TargetMode
	if targetMode == "" {
		targetMode = "drop_recreate"
	}
	if targetMode != "drop_recreate" && targetMode != "upsert" {
		return EffectiveTransferPlan{}, fmt.Errorf("resolve transfer resources: invalid target mode %q", targetMode)
	}
	connectionLimit, err := effectiveConnectionLimit(migration)
	if err != nil {
		return EffectiveTransferPlan{}, err
	}

	snapshot, err := probe.ProbeMemory(ctx)
	if err != nil {
		return EffectiveTransferPlan{}, fmt.Errorf("resolve transfer resources: %w", err)
	}
	detected, err := resolveDetectedMemory(snapshot)
	if err != nil {
		return EffectiveTransferPlan{}, fmt.Errorf("resolve transfer resources: %w", err)
	}
	budget := detected
	if options.UserMemoryCeilingBytes > 0 && options.UserMemoryCeilingBytes < budget.Value {
		budget = EffectiveBytes{
			Value:      options.UserMemoryCeilingBytes,
			Provenance: ProvenanceUserMemoryCeiling,
		}
	}
	if budget.Value < MinimumTransferMemoryBytes {
		return EffectiveTransferPlan{}, fmt.Errorf(
			"resolve transfer resources: effective memory budget %d is below safe minimum %d",
			budget.Value,
			MinimumTransferMemoryBytes,
		)
	}

	logicalCPUs := options.LogicalCPUs
	if logicalCPUs == 0 {
		logicalCPUs = runtime.GOMAXPROCS(0)
	}
	if logicalCPUs < 1 {
		logicalCPUs = 1
	}
	memorySlots := boundedMemorySlots(budget.Value)

	workerCap := minInt(
		minInt(MaxTransferWorkers, memorySlots),
		connectionLimit.Value,
	)
	workers := effectiveCount(options.RequestedWorkers, minInt(logicalCPUs, workerCap), workerCap)

	readerCap := minInt(
		minInt(MaxTransferReaders, workers.Value),
		connectionLimit.Value-1,
	)

	writerCap := minInt(
		minInt(MaxTransferWriters, workers.Value),
		connectionLimit.Value-1,
	)
	defaultWriters := maxInt(
		1,
		minInt(
			minInt((logicalCPUs+1)/2, writerCap),
			connectionLimit.Value/2,
		),
	)
	defaultReaders := minInt(
		minInt(logicalCPUs, readerCap),
		connectionLimit.Value-defaultWriters,
	)
	readers := effectiveCount(
		options.RequestedReaders,
		defaultReaders,
		readerCap,
	)
	writers := effectiveCount(options.RequestedWriters, defaultWriters, writerCap)
	readers, writers = clampCombinedConcurrency(
		readers,
		writers,
		connectionLimit.Value,
	)

	queueCap := minInt(MaxTransferQueueDepth, memorySlots)
	defaultQueue := minInt(queueCap, maxInt(1, readers.Value+writers.Value))
	queueDepth := effectiveCount(options.RequestedQueueDepth, defaultQueue, queueCap)

	maxRowsByMemory := budget.Value / (int64(queueDepth.Value) * AssumedRetainedRowBytes)
	if maxRowsByMemory < 1 {
		maxRowsByMemory = 1
	}
	chunkCap := MaxTransferChunkRows
	if maxRowsByMemory < int64(chunkCap) {
		chunkCap = int(maxRowsByMemory)
	}
	chunkRows := effectiveCount(options.RequestedChunkRows, minInt(DefaultTransferChunkRows, chunkCap), chunkCap)
	if workers.Value > connectionLimit.Value ||
		readers.Value+writers.Value > connectionLimit.Value {
		return EffectiveTransferPlan{}, fmt.Errorf(
			"resolve transfer resources: effective concurrency exceeds connection limit %d",
			connectionLimit.Value,
		)
	}

	return EffectiveTransferPlan{
		TargetMode:          targetMode,
		ConnectionLimit:     connectionLimit,
		DetectedMemoryLimit: detected,
		MemoryBudget:        budget,
		Workers:             workers,
		Readers:             readers,
		Writers:             writers,
		QueueDepth:          queueDepth,
		ChunkRows:           chunkRows,
	}, nil
}

func effectiveConnectionLimit(migration Migration) (EffectiveInt, error) {
	value := migration.ConnectionLimit
	provenance := ProvenanceDerived
	if migration.fieldWasSet("connection_limit") {
		provenance = ProvenanceRequested
	}
	if value == 0 {
		value = DefaultConnectionLimit
	}
	if value < 2 {
		return EffectiveInt{}, fmt.Errorf(
			"resolve transfer resources: connection limit must permit at least one reader and one writer",
		)
	}
	return EffectiveInt{Value: value, Provenance: provenance}, nil
}

func clampCombinedConcurrency(
	readers EffectiveInt,
	writers EffectiveInt,
	limit int,
) (EffectiveInt, EffectiveInt) {
	for readers.Value+writers.Value > limit {
		readerPinned := readers.Provenance != ProvenanceDerived
		writerPinned := writers.Provenance != ProvenanceDerived
		switch {
		case readerPinned && !writerPinned && writers.Value > 1:
			writers.Value--
			writers.Provenance = ProvenanceSafetyClamped
		case writerPinned && !readerPinned && readers.Value > 1:
			readers.Value--
			readers.Provenance = ProvenanceSafetyClamped
		case readers.Value >= writers.Value && readers.Value > 1:
			readers.Value--
			readers.Provenance = ProvenanceSafetyClamped
		case writers.Value > 1:
			writers.Value--
			writers.Provenance = ProvenanceSafetyClamped
		default:
			return readers, writers
		}
	}
	return readers, writers
}

func inheritMigrationTransferOptions(migration Migration, options TransferPlanOptions) TransferPlanOptions {
	if options.UserMemoryCeilingBytes == 0 &&
		migration.fieldWasSet("memory_ceiling_bytes") {
		options.UserMemoryCeilingBytes = migration.MemoryCeilingBytes
	}
	if options.RequestedWorkers == 0 && migration.fieldWasSet("workers") {
		options.RequestedWorkers = migration.Workers
	}
	if options.RequestedReaders == 0 &&
		migration.fieldWasSet("reader_parallelism") {
		options.RequestedReaders = migration.ReaderParallelism
	}
	if options.RequestedWriters == 0 &&
		migration.fieldWasSet("writer_parallelism") {
		options.RequestedWriters = migration.WriterParallelism
	}
	if options.RequestedQueueDepth == 0 &&
		migration.fieldWasSet("read_ahead") {
		options.RequestedQueueDepth = migration.ReadAhead
	}
	if options.RequestedChunkRows == 0 &&
		migration.fieldWasSet("chunk_size") {
		options.RequestedChunkRows = migration.ChunkSize
	}
	return options
}

func validateTransferPlanOptions(options TransferPlanOptions) error {
	values := []struct {
		name  string
		value int64
	}{
		{name: "user memory ceiling", value: options.UserMemoryCeilingBytes},
		{name: "logical CPUs", value: int64(options.LogicalCPUs)},
		{name: "workers", value: int64(options.RequestedWorkers)},
		{name: "readers", value: int64(options.RequestedReaders)},
		{name: "writers", value: int64(options.RequestedWriters)},
		{name: "queue depth", value: int64(options.RequestedQueueDepth)},
		{name: "chunk rows", value: int64(options.RequestedChunkRows)},
	}
	for _, value := range values {
		if value.value < 0 {
			return fmt.Errorf("resolve transfer resources: %s must not be negative", value.name)
		}
	}
	return nil
}

func resolveDetectedMemory(snapshot MemorySnapshot) (EffectiveBytes, error) {
	if snapshot.HostCapacityBytes <= 0 || snapshot.HostAvailableBytes <= 0 {
		return EffectiveBytes{}, fmt.Errorf("safe finite host capacity and available memory evidence are required")
	}
	detected := snapshot.HostAvailableBytes
	provenance := ProvenanceHostAvailable
	if snapshot.HostCapacityBytes < detected {
		detected = snapshot.HostCapacityBytes
		provenance = ProvenanceHostCapacity
	}

	evidence := snapshot.CgroupV2
	cgroupProvenance := ProvenanceCgroupV2Remaining
	if evidence.State == "" || evidence.State == CgroupLimitAbsent {
		evidence = snapshot.CgroupV1
		cgroupProvenance = ProvenanceCgroupV1Remaining
	}
	switch evidence.State {
	case "", CgroupLimitAbsent, CgroupLimitUnlimited:
	case CgroupLimitUnknown:
		return EffectiveBytes{}, fmt.Errorf("container memory evidence is present but not safely resolvable")
	case CgroupLimitFinite:
		if evidence.LimitBytes <= 0 || evidence.CurrentBytes < 0 || evidence.CurrentBytes >= evidence.LimitBytes {
			return EffectiveBytes{}, fmt.Errorf(
				"invalid or exhausted finite cgroup memory evidence: limit=%d current=%d",
				evidence.LimitBytes,
				evidence.CurrentBytes,
			)
		}
		remaining := evidence.LimitBytes - evidence.CurrentBytes
		if remaining < detected {
			detected = remaining
			provenance = cgroupProvenance
		}
	default:
		return EffectiveBytes{}, fmt.Errorf("unknown cgroup memory state %q", evidence.State)
	}
	if detected <= 0 {
		return EffectiveBytes{}, fmt.Errorf("no safe finite memory budget is available")
	}
	return EffectiveBytes{Value: detected, Provenance: provenance}, nil
}

func boundedMemorySlots(budget int64) int {
	slots := budget / TransferMemoryPerSlotBytes
	if slots < 1 {
		return 1
	}
	if slots > int64(MaxTransferQueueDepth) {
		return MaxTransferQueueDepth
	}
	return int(slots)
}

func effectiveCount(requested, derived, cap int) EffectiveInt {
	if cap < 1 {
		cap = 1
	}
	if derived < 1 {
		derived = 1
	}
	if derived > cap {
		derived = cap
	}
	if requested == 0 {
		return EffectiveInt{Value: derived, Provenance: ProvenanceDerived}
	}
	if requested > cap {
		return EffectiveInt{Value: cap, Provenance: ProvenanceSafetyClamped}
	}
	return EffectiveInt{Value: requested, Provenance: ProvenanceRequested}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
