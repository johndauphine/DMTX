package migrate

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestRuntimeTuningPreservesPinnedIntentAndAppliesInitialSafety(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows = config.EffectiveInt{
		Value: 100, Provenance: config.ProvenanceRequested,
	}
	plan.Writers = config.EffectiveInt{
		Value: 3, Provenance: config.ProvenanceRequested,
	}
	plan.QueueDepth = config.EffectiveInt{
		Value: 4, Provenance: config.ProvenanceRequested,
	}
	limits := runtimeTuningTestLimits()
	limits.ProtocolMaxChunkRows = 25

	controller, err := NewRuntimeTuningController(plan, limits)
	if err != nil {
		t.Fatal(err)
	}
	plan.ChunkRows.Value = 999
	plan.Writers.Value = 999
	limits.ProtocolMaxChunkRows = 999

	snapshot := controller.Snapshot()
	if snapshot.Intent.ChunkRows.Value != 100 ||
		snapshot.Intent.Writers.Value != 3 ||
		snapshot.Effective.ChunkRows.Value != 25 ||
		snapshot.Effective.ChunkRows.IntentValue != 100 ||
		!snapshot.Effective.ChunkRows.PerformancePinned ||
		snapshot.Effective.ChunkRows.LiveProvenance !=
			RuntimeTuningSafetyReduction {
		t.Fatalf("pinned/safety snapshot = %#v", snapshot)
	}
	if !reflect.DeepEqual(
		snapshot.InitializationReasons,
		[]RuntimeTuningReason{RuntimeReasonProtocolCeiling},
	) {
		t.Fatalf(
			"initialization reasons = %v",
			snapshot.InitializationReasons,
		)
	}
	snapshot.InitializationReasons[0] = RuntimeReasonWriteError
	if got := controller.Snapshot().InitializationReasons[0]; got != RuntimeReasonProtocolCeiling {
		t.Fatalf("snapshot mutation changed controller reason to %q", got)
	}
}

func TestRuntimeTuningPressureAndWriteErrorsReduceOnlyAtBoundaries(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows.Value = 128
	plan.Writers.Value = 4
	plan.QueueDepth.Value = 4
	controller := mustRuntimeTuningController(
		t,
		plan,
		runtimeTuningTestLimits(),
	)
	before := controller.Snapshot().Effective
	if before.ChunkRows.Value != 128 ||
		before.Writers.Value != 4 ||
		before.BufferDepth.Value != 4 {
		t.Fatalf("initial values = %#v", before)
	}

	builder := newRuntimeObservationBuilder(plan, runtimeTuningTestLimits())
	memory := builder.next(controller)
	memory.MemoryPressure = true
	afterMemory, err := controller.ApplyChunkBoundary(
		context.Background(),
		memory,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterMemory.Effective.ChunkRows.Value != 64 ||
		afterMemory.Effective.Writers.Value != 2 ||
		afterMemory.Effective.BufferDepth.Value != 2 {
		t.Fatalf("memory-pressure values = %#v", afterMemory.Effective)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[0],
		RuntimeReasonMemoryPressure,
	)

	writeFailure := builder.next(controller)
	writeFailure.WriteOutcome = RuntimeWriteRetryableError
	afterWrite, err := controller.ApplyChunkBoundary(
		context.Background(),
		writeFailure,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterWrite.Effective.ChunkRows.Value != 32 ||
		afterWrite.Effective.Writers.Value != 1 ||
		afterWrite.Effective.BufferDepth.Value != 2 {
		t.Fatalf("write-error values = %#v", afterWrite.Effective)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[1],
		RuntimeReasonWriteError,
	)

	protocolFailure := builder.next(controller)
	protocolFailure.WriteOutcome = RuntimeWriteProtocolError
	afterProtocol, err := controller.ApplyChunkBoundary(
		context.Background(),
		protocolFailure,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterProtocol.Effective.ChunkRows.Value != 5 ||
		afterProtocol.Effective.Writers.Value != 1 {
		t.Fatalf(
			"protocol-error values = %#v",
			afterProtocol.Effective,
		)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[2],
		RuntimeReasonProtocolWriteError,
	)
}

func TestRuntimeTuningIntervalGatesGrowthButNeverSafetyReduction(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows.Value = 64
	plan.Writers.Value = 2
	plan.QueueDepth.Value = 2
	limits := runtimeTuningTestLimits()
	limits.GrowthAfterHealthyBoundaries = 2
	start := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	now := start
	clockCalls := 0
	controller, err := NewRuntimeTuningControllerWithOptions(
		plan,
		limits,
		RuntimeTuningOptions{
			Interval: 10 * time.Second,
			Now: func() time.Time {
				clockCalls++
				return now
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	builder := newRuntimeObservationBuilder(plan, limits)

	pressure := builder.next(controller)
	pressure.MemoryPressure = true
	afterPressure, err := controller.ApplyChunkBoundary(
		context.Background(),
		pressure,
	)
	if err != nil {
		t.Fatal(err)
	}
	if clockCalls != 0 ||
		afterPressure.Effective.ChunkRows.Value != 32 {
		t.Fatalf(
			"safety reduction clockCalls=%d values=%#v",
			clockCalls,
			afterPressure.Effective,
		)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[0],
		RuntimeReasonMemoryPressure,
	)

	now = now.Add(time.Second)
	firstHealthy := builder.next(controller)
	afterHealthy, err := controller.ApplyChunkBoundary(
		context.Background(),
		firstHealthy,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterHealthy.Effective.ChunkRows.Value != 32 ||
		afterHealthy.HealthyBoundaries != 1 {
		t.Fatalf("first eligible observation = %#v", afterHealthy)
	}

	now = now.Add(time.Second)
	gated := builder.next(controller)
	afterGated, err := controller.ApplyChunkBoundary(
		context.Background(),
		gated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterGated.Effective.ChunkRows.Value != 32 ||
		afterGated.HealthyBoundaries != 1 {
		t.Fatalf(
			"interval-gated boundary discarded eligible health: %#v",
			afterGated,
		)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[2],
		RuntimeReasonIntervalGate,
	)

	now = now.Add(time.Second)
	secondGated := builder.next(controller)
	afterSecondGated, err := controller.ApplyChunkBoundary(
		context.Background(),
		secondGated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterSecondGated.Effective.ChunkRows.Value != 32 ||
		afterSecondGated.HealthyBoundaries != 1 {
		t.Fatalf(
			"second interval-gated boundary discarded eligible health: %#v",
			afterSecondGated,
		)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[3],
		RuntimeReasonIntervalGate,
	)

	now = start.Add(11 * time.Second)
	eligible := builder.next(controller)
	afterEligible, err := controller.ApplyChunkBoundary(
		context.Background(),
		eligible,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterEligible.Effective.ChunkRows.Value != 64 {
		t.Fatalf("elapsed interval did not permit growth: %#v", afterEligible.Effective)
	}
	if afterEligible.Interval != 10*time.Second {
		t.Fatalf("runtime tuning interval = %s", afterEligible.Interval)
	}

	now = now.Add(time.Second)
	secondPressure := builder.next(controller)
	secondPressure.WriteOutcome = RuntimeWriteRetryableError
	afterSecondPressure, err := controller.ApplyChunkBoundary(
		context.Background(),
		secondPressure,
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterSecondPressure.Effective.ChunkRows.Value != 32 ||
		afterSecondPressure.HealthyBoundaries != 0 {
		t.Fatalf(
			"write-error reduction = %#v",
			afterSecondPressure,
		)
	}
}

func TestRuntimeTuningIntervalClockFailureDoesNotBlockSafetyReduction(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	controller, err := NewRuntimeTuningControllerWithOptions(
		plan,
		limits,
		RuntimeTuningOptions{
			Interval: time.Second,
			Now:      func() time.Time { return time.Time{} },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	builder := newRuntimeObservationBuilder(plan, limits)
	pressure := builder.next(controller)
	pressure.MemoryPressure = true
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		pressure,
	); err != nil {
		t.Fatalf("safety reduction was interval-gated: %v", err)
	}
	healthy := builder.next(controller)
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		healthy,
	); !errors.Is(err, ErrInvalidRuntimeObservation) {
		t.Fatalf("healthy boundary error = %v", err)
	}
}

func TestRuntimeTuningSafetyReductionOverridesPinsButRecoveryNeverExceedsThem(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows = config.EffectiveInt{
		Value: 64, Provenance: config.ProvenanceRequested,
	}
	plan.Writers = config.EffectiveInt{
		Value: 3, Provenance: config.ProvenanceRequested,
	}
	plan.QueueDepth = config.EffectiveInt{
		Value: 3, Provenance: config.ProvenanceRequested,
	}
	limits := runtimeTuningTestLimits()
	limits.GrowthAfterHealthyBoundaries = 1
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)

	pressure := builder.next(controller)
	pressure.MemoryPressure = true
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		pressure,
	); err != nil {
		t.Fatal(err)
	}
	reduced := controller.Snapshot().Effective
	if reduced.ChunkRows.Value >= 64 ||
		reduced.Writers.Value >= 3 ||
		reduced.BufferDepth.Value >= 3 {
		t.Fatalf("pinned values were not safety-reduced: %#v", reduced)
	}

	for index := 0; index < 20; index++ {
		observation := builder.next(controller)
		if _, err := controller.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
		effective := controller.Snapshot().Effective
		if effective.ChunkRows.Value > 64 ||
			effective.Writers.Value > 3 ||
			effective.BufferDepth.Value > 3 {
			t.Fatalf("growth exceeded pinned intent: %#v", effective)
		}
	}
	recovered := controller.Snapshot().Effective
	if recovered.ChunkRows.Value != 64 ||
		recovered.Writers.Value != 3 ||
		recovered.BufferDepth.Value != 3 {
		t.Fatalf("pinned values did not recover to intent: %#v", recovered)
	}
}

func TestRuntimeTuningGrowthRequiresCompleteInventoryWidthAndHeadroom(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows.Value = 16
	plan.Writers.Value = 1
	plan.QueueDepth.Value = 1
	limits := runtimeTuningTestLimits()
	limits.GrowthAfterHealthyBoundaries = 2
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	initial := controller.Snapshot().Effective

	incomplete := builder.next(controller)
	incomplete.Inventory = RuntimeResourceInventory{}
	incomplete.RowWidth = RuntimeRowWidthEvidence{}
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		incomplete,
	); err != nil {
		t.Fatal(err)
	}
	if got := controller.Snapshot().Effective; got != initial {
		t.Fatalf("incomplete evidence changed values: %#v", got)
	}

	untrusted := builder.next(controller)
	untrusted.RowWidth = RuntimeRowWidthEvidence{}
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		untrusted,
	); err != nil {
		t.Fatal(err)
	}
	if got := controller.Snapshot().Effective; got != initial {
		t.Fatalf("untrusted width changed values: %#v", got)
	}

	noHeadroom := builder.next(controller)
	noHeadroom.Inventory.ByteBudget.Current =
		noHeadroom.Inventory.ByteBudget.Limit
	noHeadroom.Inventory.ByteBudget.Peak =
		noHeadroom.Inventory.ByteBudget.Limit
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		noHeadroom,
	); err != nil {
		t.Fatal(err)
	}
	if got := controller.Snapshot().Effective; got != initial {
		t.Fatalf("missing headroom changed values: %#v", got)
	}

	firstHealthy := builder.next(controller)
	firstHealthy.Inventory.ByteBudget.Peak =
		noHeadroom.Inventory.ByteBudget.Peak
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		firstHealthy,
	); err != nil {
		t.Fatal(err)
	}
	if got := controller.Snapshot().Effective; got != initial {
		t.Fatalf("one healthy boundary changed values: %#v", got)
	}

	secondHealthy := builder.next(controller)
	secondHealthy.Inventory.ByteBudget.Peak =
		noHeadroom.Inventory.ByteBudget.Peak
	after, err := controller.ApplyChunkBoundary(
		context.Background(),
		secondHealthy,
	)
	if err != nil {
		t.Fatal(err)
	}
	if after.Effective.ChunkRows.Value <= initial.ChunkRows.Value ||
		after.Effective.Writers.Value <= initial.Writers.Value ||
		after.Effective.BufferDepth.Value <= initial.BufferDepth.Value {
		t.Fatalf("complete healthy evidence did not grow: %#v", after.Effective)
	}
	assertRuntimeDecisionReasons(
		t,
		controller.History()[len(controller.History())-1],
		RuntimeReasonEvidenceGrowth,
	)
}

func TestRuntimeTuningProtocolBytesAndRowsRemainHardCaps(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows.Value = 100
	limits := runtimeTuningTestLimits()
	limits.ProtocolMaxChunkRows = 80
	limits.ProtocolMaxChunkBytes = 3_000
	limits.SafetyRowWidthUpperBound = 100
	limits.GrowthAfterHealthyBoundaries = 1
	controller := mustRuntimeTuningController(t, plan, limits)
	if got := controller.Snapshot().Effective.ChunkRows.Value; got != 30 {
		t.Fatalf("initial protocol byte cap = %d, want 30", got)
	}

	builder := newRuntimeObservationBuilder(plan, limits)
	builder.rowWidth = 50
	for index := 0; index < 10; index++ {
		observation := builder.next(controller)
		if _, err := controller.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
	}
	if got := controller.Snapshot().Effective.ChunkRows.Value; got != 60 {
		t.Fatalf("trusted-width protocol cap = %d, want 60", got)
	}
}

func TestRuntimeTuningProtocolFailureEstablishesRunCeiling(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	plan.ChunkRows.Value = 64
	limits := runtimeTuningTestLimits()
	limits.GrowthAfterHealthyBoundaries = 1
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)

	failure := builder.next(controller)
	failure.WriteOutcome = RuntimeWriteProtocolError
	afterFailure, err := controller.ApplyChunkBoundary(
		context.Background(),
		failure,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterFailure.Effective.ChunkRows.Value; got != 5 {
		t.Fatalf("protocol failure chunk rows = %d, want 5", got)
	}

	for index := 0; index < 20; index++ {
		observation := builder.next(controller)
		if _, err := controller.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
		if got := controller.Snapshot().Effective.ChunkRows.Value; got > 5 {
			t.Fatalf(
				"chunk rows regrew past learned protocol ceiling: %d",
				got,
			)
		}
	}
}

func TestRuntimeTuningRejectsMalformedAndNonMonotonicEvidence(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*RuntimeTuningObservation)
		target error
		secret string
	}{
		{
			name: "first ordinal skips",
			mutate: func(value *RuntimeTuningObservation) {
				value.Boundary.Ordinal = 2
			},
			target: ErrNonMonotonicRuntimeObservation,
		},
		{
			name: "zero rows",
			mutate: func(value *RuntimeTuningObservation) {
				value.ObservedRows = 0
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "cumulative mismatch",
			mutate: func(value *RuntimeTuningObservation) {
				value.CumulativeObservedBytes++
			},
			target: ErrNonMonotonicRuntimeObservation,
		},
		{
			name: "partial inventory",
			mutate: func(value *RuntimeTuningObservation) {
				value.Inventory.Complete = false
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "partial row width",
			mutate: func(value *RuntimeTuningObservation) {
				value.RowWidth.Trustworthy = false
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "row width above safety proof",
			mutate: func(value *RuntimeTuningObservation) {
				value.RowWidth.UpperBoundBytes++
				value.RowWidth.UpperBoundBytes *= 100
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "self-consistent incomplete column inventory",
			mutate: func(value *RuntimeTuningObservation) {
				value.RowWidth.CompleteColumnCount = 1
				value.RowWidth.ExpectedColumnCount = 1
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "observed bytes contradict trustworthy width",
			mutate: func(value *RuntimeTuningObservation) {
				value.ObservedRows = 1
				value.ObservedBytes =
					uint64(value.RowWidth.UpperBoundBytes) + 1
				value.CumulativeObservedRows = value.ObservedRows
				value.CumulativeObservedBytes = value.ObservedBytes
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "observed bytes contradict safety width",
			mutate: func(value *RuntimeTuningObservation) {
				value.RowWidth = RuntimeRowWidthEvidence{}
				value.ObservedRows = 1
				value.ObservedBytes =
					uint64(runtimeTuningTestLimits().
						SafetyRowWidthUpperBound) + 1
				value.CumulativeObservedRows = value.ObservedRows
				value.CumulativeObservedBytes = value.ObservedBytes
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "unknown write outcome",
			mutate: func(value *RuntimeTuningObservation) {
				value.WriteOutcome = "driver said password=sentinel"
			},
			target: ErrInvalidRuntimeObservation,
			secret: "sentinel",
		},
		{
			name: "range outside inventory",
			mutate: func(value *RuntimeTuningObservation) {
				value.Boundary.RangeIndex =
					value.Inventory.PlannedRanges
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "new range starts after chunk zero",
			mutate: func(value *RuntimeTuningObservation) {
				value.Boundary.ChunkSequence = 1
			},
			target: ErrNonMonotonicRuntimeObservation,
		},
		{
			name: "impossible connections",
			mutate: func(value *RuntimeTuningObservation) {
				value.Inventory.OpenConnections =
					value.Inventory.ConnectionLimit + 1
			},
			target: ErrInvalidRuntimeObservation,
		},
		{
			name: "impossible byte budget",
			mutate: func(value *RuntimeTuningObservation) {
				value.Inventory.ByteBudget.Current =
					value.Inventory.ByteBudget.Limit + 1
			},
			target: ErrInvalidRuntimeObservation,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := runtimeTuningTestPlan()
			limits := runtimeTuningTestLimits()
			controller := mustRuntimeTuningController(t, plan, limits)
			builder := newRuntimeObservationBuilder(plan, limits)
			observation := builder.next(controller)
			test.mutate(&observation)
			before := controller.Snapshot()
			_, err := controller.ApplyChunkBoundary(
				context.Background(),
				observation,
			)
			if !errors.Is(err, test.target) {
				t.Fatalf("error = %v, want %v", err, test.target)
			}
			if test.secret != "" &&
				strings.Contains(err.Error(), test.secret) {
				t.Fatalf("error disclosed observation text: %v", err)
			}
			if after := controller.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected observation mutated state: %#v", after)
			}
		})
	}
}

func TestRuntimeTuningRejectsConflictsRegressionsAndRangeSkips(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	first := builder.next(controller)
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		first,
	); err != nil {
		t.Fatal(err)
	}

	conflict := first
	conflict.MemoryPressure = true
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		conflict,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("conflicting retry error = %v", err)
	}

	nextExpected := builder.next(controller)

	skippedOrdinal := nextExpected
	skippedOrdinal.Boundary.Ordinal++
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		skippedOrdinal,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("skipped ordinal error = %v", err)
	}

	skippedChunk := nextExpected
	skippedChunk.Boundary.ChunkSequence++
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		skippedChunk,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("skipped chunk error = %v", err)
	}

	changedWidth := nextExpected
	changedWidth.RowWidth.UpperBoundBytes--
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		changedWidth,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("changed width proof error = %v", err)
	}

	changedColumns := nextExpected
	changedColumns.RowWidth.CompleteColumnCount++
	changedColumns.RowWidth.ExpectedColumnCount++
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		changedColumns,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("changed column proof error = %v", err)
	}

	regressedPeak := nextExpected
	regressedPeak.Inventory.ByteBudget.Peak = 0
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		regressedPeak,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("regressed peak error = %v", err)
	}

	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		nextExpected,
	); err != nil {
		t.Fatalf("valid observation after rejected evidence: %v", err)
	}
}

func TestRuntimeTuningRangeOrdinalHasImmutableIdentity(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	first := builder.next(controller)
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		first,
	); err != nil {
		t.Fatal(err)
	}

	alias := builder.next(controller)
	const secretSentinel = "password_sentinel"
	alias.Boundary.TableName = secretSentinel
	alias.Boundary.ChunkSequence = 0
	before := controller.Snapshot()
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		alias,
	); !errors.Is(err, ErrNonMonotonicRuntimeObservation) {
		t.Fatalf("range ordinal alias error = %v", err)
	} else if strings.Contains(err.Error(), secretSentinel) {
		t.Fatalf("range ordinal alias error disclosed identity: %v", err)
	}
	if after := controller.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("range ordinal alias mutated controller: %#v", after)
	}

	alias.Boundary.RangeIndex = 1
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		alias,
	); err != nil {
		t.Fatalf("distinct planned ordinal was rejected: %v", err)
	}
}

func TestRuntimeTuningObservedWidthCheckDoesNotOverflow(t *testing.T) {
	t.Parallel()

	if !runtimeObservedBytesFitRowWidth(
		math.MaxUint64,
		math.MaxUint64,
		math.MaxInt64,
	) {
		t.Fatal("mathematically sufficient width was rejected after overflow")
	}
	if runtimeObservedBytesFitRowWidth(
		1,
		math.MaxUint64,
		math.MaxInt64,
	) {
		t.Fatal("insufficient single-row width was admitted")
	}
}

func TestRuntimeTuningCancellationCannotApplyBoundary(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	observation := builder.next(controller)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.ApplyChunkBoundary(
		ctx,
		observation,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled apply error = %v", err)
	}
	if snapshot := controller.Snapshot(); snapshot.HasBoundary ||
		snapshot.TotalDecisions != 0 ||
		len(controller.History()) != 0 {
		t.Fatalf("cancelled apply mutated controller: %#v", snapshot)
	}
	if _, err := controller.ApplyChunkBoundary(
		context.Background(),
		observation,
	); err != nil {
		t.Fatalf("same boundary after cancellation: %v", err)
	}
}

func TestRuntimeTuningHistoryIsBoundedOrderedAndImmutable(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	limits.HistoryLimit = 3
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	for index := 0; index < 6; index++ {
		observation := builder.next(controller)
		if index%2 == 0 {
			observation.QueuePressure = true
		}
		if _, err := controller.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
	}
	history := controller.History()
	if len(history) != 3 {
		t.Fatalf("history length = %d, want 3", len(history))
	}
	for index, wantOrdinal := range []uint64{4, 5, 6} {
		if history[index].Boundary.Ordinal != wantOrdinal ||
			history[index].Boundary.RangeIndex != 0 ||
			history[index].Boundary.ChunkSequence != wantOrdinal-1 ||
			len(history[index].Reasons) == 0 {
			t.Fatalf("history[%d] = %#v", index, history[index])
		}
	}
	history[0].Boundary.TableName = "mutated"
	history[0].Reasons[0] = RuntimeReasonWriteError
	again := controller.History()
	if again[0].Boundary.TableName == "mutated" ||
		again[0].Reasons[0] == RuntimeReasonWriteError {
		t.Fatal("returned history aliases controller state")
	}
	snapshot := controller.Snapshot()
	if snapshot.TotalDecisions != 6 ||
		snapshot.RetainedDecisions != 3 ||
		snapshot.LastBoundary.Ordinal != 6 {
		t.Fatalf("bounded history snapshot = %#v", snapshot)
	}
}

func TestRuntimeTuningReplayIsDeterministic(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	limits.HistoryLimit = 16
	left := mustRuntimeTuningController(t, plan, limits)
	right := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	observations := make([]RuntimeTuningObservation, 0, 8)
	for index := 0; index < 8; index++ {
		observation := builder.next(left)
		switch index {
		case 0:
			observation.MemoryPressure = true
		case 3:
			observation.WriteOutcome = RuntimeWriteRetryableError
		case 5:
			observation.WriteOutcome = RuntimeWriteProtocolError
		case 6:
			observation.ConnectionPressure = true
		}
		observations = append(observations, observation)
		// Builder reads only immutable plan and its own counters. Applying to
		// left here keeps inventory values realistic for later boundaries.
		if _, err := left.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
	}
	for _, observation := range observations {
		if _, err := right.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(left.Snapshot(), right.Snapshot()) ||
		!reflect.DeepEqual(left.History(), right.History()) {
		t.Fatalf(
			"deterministic replay differs:\nleft=%#v\nright=%#v",
			left.Snapshot(),
			right.Snapshot(),
		)
	}
}

func TestRuntimeTuningConcurrentIdenticalObservationsAreIdempotent(
	t *testing.T,
) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)
	observation := builder.next(controller)

	const callers = 64
	var waiters sync.WaitGroup
	waiters.Add(callers)
	errorsChannel := make(chan error, callers)
	for index := 0; index < callers; index++ {
		go func() {
			defer waiters.Done()
			_, err := controller.ApplyChunkBoundary(
				context.Background(),
				observation,
			)
			errorsChannel <- err
		}()
	}
	waiters.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	snapshot := controller.Snapshot()
	if snapshot.TotalDecisions != 1 ||
		snapshot.AppliedBoundaries != 1 ||
		len(controller.History()) != 1 {
		t.Fatalf("concurrent idempotency snapshot = %#v", snapshot)
	}
}

func TestRuntimeTuningConcurrentStatusReadsAndBoundaryWrites(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	limits := runtimeTuningTestLimits()
	limits.HistoryLimit = 128
	controller := mustRuntimeTuningController(t, plan, limits)
	builder := newRuntimeObservationBuilder(plan, limits)

	const readers = 8
	done := make(chan struct{})
	var waiters sync.WaitGroup
	waiters.Add(readers)
	for index := 0; index < readers; index++ {
		go func() {
			defer waiters.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = controller.Snapshot()
					_ = controller.History()
				}
			}
		}()
	}
	for index := 0; index < 100; index++ {
		observation := builder.next(controller)
		if _, err := controller.ApplyChunkBoundary(
			context.Background(),
			observation,
		); err != nil {
			close(done)
			waiters.Wait()
			t.Fatal(err)
		}
	}
	close(done)
	waiters.Wait()
	if got := controller.Snapshot().TotalDecisions; got != 100 {
		t.Fatalf("total decisions = %d, want 100", got)
	}
}

func TestNewRuntimeTuningControllerRejectsUnsafePlansAndLimits(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*config.EffectiveTransferPlan, *RuntimeTuningLimits)
	}{
		{
			name: "zero memory",
			mutate: func(plan *config.EffectiveTransferPlan, _ *RuntimeTuningLimits) {
				plan.MemoryBudget.Value = 0
			},
		},
		{
			name: "missing detected memory",
			mutate: func(plan *config.EffectiveTransferPlan, _ *RuntimeTuningLimits) {
				plan.DetectedMemoryLimit.Value = 0
			},
		},
		{
			name: "resource exceeds effective plan bounds",
			mutate: func(plan *config.EffectiveTransferPlan, _ *RuntimeTuningLimits) {
				plan.QueueDepth.Value =
					config.MaxTransferQueueDepth + 1
			},
		},
		{
			name: "concurrency exceeds connection limit",
			mutate: func(plan *config.EffectiveTransferPlan, _ *RuntimeTuningLimits) {
				plan.ConnectionLimit.Value = 2
			},
		},
		{
			name: "unknown provenance",
			mutate: func(plan *config.EffectiveTransferPlan, _ *RuntimeTuningLimits) {
				plan.ChunkRows.Provenance = "history_guess"
			},
		},
		{
			name: "unbounded history",
			mutate: func(_ *config.EffectiveTransferPlan, limits *RuntimeTuningLimits) {
				limits.HistoryLimit = 1025
			},
		},
		{
			name: "unbounded ranges",
			mutate: func(_ *config.EffectiveTransferPlan, limits *RuntimeTuningLimits) {
				limits.PlannedRanges = maximumRuntimeTuningRanges + 1
			},
		},
		{
			name: "missing authoritative column inventory",
			mutate: func(_ *config.EffectiveTransferPlan, limits *RuntimeTuningLimits) {
				limits.ExpectedColumnCount = 0
			},
		},
		{
			name: "one row exceeds protocol",
			mutate: func(_ *config.EffectiveTransferPlan, limits *RuntimeTuningLimits) {
				limits.ProtocolMaxChunkBytes =
					limits.SafetyRowWidthUpperBound - 1
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := runtimeTuningTestPlan()
			limits := runtimeTuningTestLimits()
			test.mutate(&plan, &limits)
			if _, err := NewRuntimeTuningController(
				plan,
				limits,
			); !errors.Is(err, ErrInvalidRuntimeTuningPlan) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

type runtimeObservationBuilder struct {
	plan     config.EffectiveTransferPlan
	limits   RuntimeTuningLimits
	ordinal  uint64
	sequence uint64
	rows     uint64
	bytes    uint64
	peak     int64
	rowWidth int64
}

func newRuntimeObservationBuilder(
	plan config.EffectiveTransferPlan,
	limits RuntimeTuningLimits,
) *runtimeObservationBuilder {
	return &runtimeObservationBuilder{
		plan:     plan,
		limits:   limits,
		peak:     2 << 10,
		rowWidth: limits.SafetyRowWidthUpperBound / 2,
	}
}

func (builder *runtimeObservationBuilder) next(
	controller *RuntimeTuningController,
) RuntimeTuningObservation {
	builder.ordinal++
	const observedRows = uint64(10)
	bytesPerRow := builder.rowWidth / 2
	if bytesPerRow < 1 {
		bytesPerRow = 1
	}
	observedBytes := observedRows * uint64(bytesPerRow)
	builder.rows += observedRows
	builder.bytes += observedBytes
	snapshot := controller.Snapshot()
	activeWriters := 1
	openConnections := builder.plan.Readers.Value + activeWriters
	if openConnections >= builder.plan.ConnectionLimit.Value {
		openConnections = builder.plan.ConnectionLimit.Value - 1
	}
	observation := RuntimeTuningObservation{
		Boundary: RuntimeTuningBoundary{
			Ordinal:       builder.ordinal,
			TableSchema:   "public",
			TableName:     "events",
			RangeIndex:    0,
			ChunkSequence: builder.sequence,
		},
		ObservedRows:            observedRows,
		ObservedBytes:           observedBytes,
		CumulativeObservedRows:  builder.rows,
		CumulativeObservedBytes: builder.bytes,
		Inventory: RuntimeResourceInventory{
			Complete:        true,
			PlannedRanges:   builder.limits.PlannedRanges,
			ConnectionLimit: builder.plan.ConnectionLimit.Value,
			OpenConnections: openConnections,
			ActiveReaders:   builder.plan.Readers.Value,
			ActiveWriters:   activeWriters,
			QueueDepth:      0,
			ByteBudget: ByteBudgetStats{
				Limit: builder.plan.MemoryBudget.Value,
				Peak:  builder.peak,
			},
		},
		RowWidth: RuntimeRowWidthEvidence{
			Trustworthy:         true,
			CompleteColumnCount: 3,
			ExpectedColumnCount: 3,
			UpperBoundBytes:     builder.rowWidth,
		},
		WriteOutcome: RuntimeWriteSucceeded,
	}
	if snapshot.Effective.Writers.Value < activeWriters {
		observation.Inventory.ActiveWriters =
			snapshot.Effective.Writers.Value
	}
	builder.sequence++
	return observation
}

func runtimeTuningTestPlan() config.EffectiveTransferPlan {
	const memory = int64(8 << 20)
	return config.EffectiveTransferPlan{
		TargetMode: "upsert",
		ConnectionLimit: config.EffectiveInt{
			Value: 8, Provenance: config.ProvenanceDerived,
		},
		DetectedMemoryLimit: config.EffectiveBytes{
			Value: memory, Provenance: config.ProvenanceHostAvailable,
		},
		MemoryBudget: config.EffectiveBytes{
			Value: memory, Provenance: config.ProvenanceHostAvailable,
		},
		Workers: config.EffectiveInt{
			Value: 8, Provenance: config.ProvenanceDerived,
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
			Value: 64, Provenance: config.ProvenanceDerived,
		},
	}
}

func runtimeTuningTestLimits() RuntimeTuningLimits {
	return RuntimeTuningLimits{
		ProtocolMaxChunkRows:         1_000,
		ProtocolMaxChunkBytes:        1 << 20,
		SafetyRowWidthUpperBound:     1 << 10,
		PlannedRanges:                4,
		ExpectedColumnCount:          3,
		HistoryLimit:                 64,
		GrowthAfterHealthyBoundaries: 2,
	}
}

func mustRuntimeTuningController(
	t *testing.T,
	plan config.EffectiveTransferPlan,
	limits RuntimeTuningLimits,
) *RuntimeTuningController {
	t.Helper()
	controller, err := NewRuntimeTuningController(plan, limits)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func assertRuntimeDecisionReasons(
	t *testing.T,
	decision RuntimeTuningDecision,
	reasons ...RuntimeTuningReason,
) {
	t.Helper()
	for _, expected := range reasons {
		found := false
		for _, actual := range decision.Reasons {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf(
				"decision reasons = %v, missing %q",
				decision.Reasons,
				expected,
			)
		}
	}
}
