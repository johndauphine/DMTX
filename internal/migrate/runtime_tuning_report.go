package migrate

import "github.com/johndauphine/dmtx/internal/config"

// RuntimeTuningReport is the public, credential-free status surface for this
// invocation. It deliberately does not expose internal controller, state, or
// config wire structs: additive status remains independent of their private
// serialization and all field names use the public snake_case convention.
type RuntimeTuningReport struct {
	Enabled bool                       `json:"enabled"`
	Reason  string                     `json:"reason,omitempty"`
	Tables  []RuntimeTuningTableReport `json:"tables"`
}

// RuntimeTuningTableReport is ordered by the immutable Stage 4 table plan.
// It contains no source values, page frontiers, driver errors, or credentials.
type RuntimeTuningTableReport struct {
	Schema      string                          `json:"schema"`
	Table       string                          `json:"table"`
	Snapshot    RuntimeTuningStatusReport       `json:"snapshot"`
	Adjustments []RuntimeTuningAdjustmentReport `json:"adjustments"`
}

// RuntimeTuningSettingReport provides a quantity and the immutable provenance
// which selected it. Field names communicate whether a quantity is a count or
// byte value, avoiding an untyped config.EffectiveTransferPlan JSON leak.
type RuntimeTuningSettingReport struct {
	Value      int64  `json:"value"`
	Provenance string `json:"provenance"`
}

// RuntimeTuningIntentReport records the immutable resource envelope used to
// construct one table controller. Runtime adjustment never rewrites it.
type RuntimeTuningIntentReport struct {
	TargetMode          string                     `json:"target_mode"`
	ConnectionLimit     RuntimeTuningSettingReport `json:"connection_limit"`
	DetectedMemoryLimit RuntimeTuningSettingReport `json:"detected_memory_limit_bytes"`
	MemoryBudget        RuntimeTuningSettingReport `json:"memory_budget_bytes"`
	Workers             RuntimeTuningSettingReport `json:"workers"`
	Readers             RuntimeTuningSettingReport `json:"readers"`
	Writers             RuntimeTuningSettingReport `json:"writers"`
	BufferDepth         RuntimeTuningSettingReport `json:"buffer_depth"`
	ChunkRows           RuntimeTuningSettingReport `json:"chunk_rows"`
}

// RuntimeTuningValueReport explains an effective live tunable without losing
// its original pinned/generated intent.
type RuntimeTuningValueReport struct {
	Value             int    `json:"value"`
	IntentValue       int    `json:"intent_value"`
	IntentProvenance  string `json:"intent_provenance"`
	LiveProvenance    string `json:"live_provenance"`
	PerformancePinned bool   `json:"performance_pinned"`
}

type RuntimeTuningValuesReport struct {
	ChunkRows   RuntimeTuningValueReport `json:"chunk_rows"`
	Writers     RuntimeTuningValueReport `json:"writers"`
	BufferDepth RuntimeTuningValueReport `json:"buffer_depth"`
}

// RuntimeTuningBoundaryReport is a bounded work identity. It omits source
// frontiers and does not disclose row values.
type RuntimeTuningBoundaryReport struct {
	Ordinal       uint64 `json:"ordinal"`
	TableSchema   string `json:"table_schema"`
	TableName     string `json:"table_name"`
	RangeIndex    uint64 `json:"range_index"`
	ChunkSequence uint64 `json:"chunk_sequence"`
	Attempt       uint32 `json:"attempt"`
}

// RuntimeTuningStatusReport is the stable status projection of one controller.
// Interval is intentionally a duration string, never an implementation-sized
// nanosecond integer.
type RuntimeTuningStatusReport struct {
	Intent                RuntimeTuningIntentReport    `json:"intent"`
	Effective             RuntimeTuningValuesReport    `json:"effective"`
	InitializationReasons []string                     `json:"initialization_reasons"`
	Interval              string                       `json:"interval"`
	HasBoundary           bool                         `json:"has_boundary"`
	LastBoundary          *RuntimeTuningBoundaryReport `json:"last_boundary,omitempty"`
	AppliedBoundaries     uint64                       `json:"applied_boundaries"`
	TotalDecisions        uint64                       `json:"total_decisions"`
	RetainedDecisions     int                          `json:"retained_decisions"`
	HealthyBoundaries     uint64                       `json:"healthy_boundaries"`
	TrustedRowWidthBytes  int64                        `json:"trusted_row_width_bytes"`
}

type RuntimeTuningAdjustmentReport struct {
	Boundary RuntimeTuningBoundaryReport `json:"boundary"`
	Before   RuntimeTuningValuesReport   `json:"before"`
	After    RuntimeTuningValuesReport   `json:"after"`
	Reasons  []string                    `json:"reasons"`
}

func runtimeTuningTableReport(
	schemaName string,
	tableName string,
	snapshot RuntimeTuningSnapshot,
	adjustments []RuntimeTuningDecision,
) RuntimeTuningTableReport {
	return RuntimeTuningTableReport{
		Schema:      schemaName,
		Table:       tableName,
		Snapshot:    runtimeTuningStatusReport(snapshot),
		Adjustments: runtimeTuningAdjustmentReports(adjustments),
	}
}

func runtimeTuningStatusReport(
	snapshot RuntimeTuningSnapshot,
) RuntimeTuningStatusReport {
	result := RuntimeTuningStatusReport{
		Intent:                runtimeTuningIntentReport(snapshot.Intent),
		Effective:             runtimeTuningValuesReport(snapshot.Effective),
		InitializationReasons: runtimeTuningReasonStrings(snapshot.InitializationReasons),
		Interval:              snapshot.Interval.String(),
		HasBoundary:           snapshot.HasBoundary,
		AppliedBoundaries:     snapshot.AppliedBoundaries,
		TotalDecisions:        snapshot.TotalDecisions,
		RetainedDecisions:     snapshot.RetainedDecisions,
		HealthyBoundaries:     snapshot.HealthyBoundaries,
		TrustedRowWidthBytes:  snapshot.TrustedRowWidthBytes,
	}
	if snapshot.HasBoundary {
		boundary := runtimeTuningBoundaryReport(snapshot.LastBoundary)
		result.LastBoundary = &boundary
	}
	return result
}

func runtimeTuningIntentReport(
	plan config.EffectiveTransferPlan,
) RuntimeTuningIntentReport {
	return RuntimeTuningIntentReport{
		TargetMode:          plan.TargetMode,
		ConnectionLimit:     runtimeTuningIntSettingReport(plan.ConnectionLimit),
		DetectedMemoryLimit: runtimeTuningByteSettingReport(plan.DetectedMemoryLimit),
		MemoryBudget:        runtimeTuningByteSettingReport(plan.MemoryBudget),
		Workers:             runtimeTuningIntSettingReport(plan.Workers),
		Readers:             runtimeTuningIntSettingReport(plan.Readers),
		Writers:             runtimeTuningIntSettingReport(plan.Writers),
		BufferDepth:         runtimeTuningIntSettingReport(plan.QueueDepth),
		ChunkRows:           runtimeTuningIntSettingReport(plan.ChunkRows),
	}
}

func runtimeTuningIntSettingReport(
	value config.EffectiveInt,
) RuntimeTuningSettingReport {
	return RuntimeTuningSettingReport{
		Value:      int64(value.Value),
		Provenance: string(value.Provenance),
	}
}

func runtimeTuningByteSettingReport(
	value config.EffectiveBytes,
) RuntimeTuningSettingReport {
	return RuntimeTuningSettingReport{
		Value:      value.Value,
		Provenance: string(value.Provenance),
	}
}

func runtimeTuningValuesReport(
	values RuntimeTuningValues,
) RuntimeTuningValuesReport {
	return RuntimeTuningValuesReport{
		ChunkRows:   runtimeTuningValueReport(values.ChunkRows),
		Writers:     runtimeTuningValueReport(values.Writers),
		BufferDepth: runtimeTuningValueReport(values.BufferDepth),
	}
}

func runtimeTuningValueReport(
	value RuntimeTuningValue,
) RuntimeTuningValueReport {
	return RuntimeTuningValueReport{
		Value:             value.Value,
		IntentValue:       value.IntentValue,
		IntentProvenance:  string(value.IntentProvenance),
		LiveProvenance:    string(value.LiveProvenance),
		PerformancePinned: value.PerformancePinned,
	}
}

func runtimeTuningBoundaryReport(
	boundary RuntimeTuningBoundary,
) RuntimeTuningBoundaryReport {
	return RuntimeTuningBoundaryReport{
		Ordinal:       boundary.Ordinal,
		TableSchema:   boundary.TableSchema,
		TableName:     boundary.TableName,
		RangeIndex:    boundary.RangeIndex,
		ChunkSequence: boundary.ChunkSequence,
		Attempt:       boundary.Attempt,
	}
}

func runtimeTuningAdjustmentReports(
	adjustments []RuntimeTuningDecision,
) []RuntimeTuningAdjustmentReport {
	result := make([]RuntimeTuningAdjustmentReport, len(adjustments))
	for index, adjustment := range adjustments {
		result[index] = RuntimeTuningAdjustmentReport{
			Boundary: runtimeTuningBoundaryReport(adjustment.Boundary),
			Before:   runtimeTuningValuesReport(adjustment.Before),
			After:    runtimeTuningValuesReport(adjustment.After),
			Reasons:  runtimeTuningReasonStrings(adjustment.Reasons),
		}
	}
	return result
}

func runtimeTuningReasonStrings(
	reasons []RuntimeTuningReason,
) []string {
	result := make([]string, len(reasons))
	for index, reason := range reasons {
		result[index] = string(reason)
	}
	return result
}
