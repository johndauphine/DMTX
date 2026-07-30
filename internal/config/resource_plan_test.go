package config

import (
	"context"
	"errors"
	"testing"
)

const (
	testMiB int64 = 1 << 20
	testGiB int64 = 1 << 30
)

type fakeMemoryProbe struct {
	snapshot MemorySnapshot
	err      error
}

func (probe fakeMemoryProbe) ProbeMemory(context.Context) (MemorySnapshot, error) {
	return probe.snapshot, probe.err
}

func TestResolveEffectiveTransferPlanUsesFiniteCgroupV2BudgetAndCapsConcurrency(t *testing.T) {
	const cgroupLimit = 256 * testMiB
	plan, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{TargetMode: "upsert"},
		TransferPlanOptions{
			LogicalCPUs:         256,
			RequestedWorkers:    1_000,
			RequestedReaders:    1_000,
			RequestedWriters:    1_000,
			RequestedQueueDepth: 1_000,
			RequestedChunkRows:  1_000_000,
		},
		fakeMemoryProbe{snapshot: MemorySnapshot{
			HostCapacityBytes:  64 * testGiB,
			HostAvailableBytes: 32 * testGiB,
			CgroupV2: CgroupMemoryEvidence{
				State:      CgroupLimitFinite,
				LimitBytes: cgroupLimit,
			},
			CgroupV1: CgroupMemoryEvidence{State: CgroupLimitAbsent},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.TargetMode != "upsert" {
		t.Fatalf("target mode = %q", plan.TargetMode)
	}
	if plan.DetectedMemoryLimit.Value != cgroupLimit ||
		plan.DetectedMemoryLimit.Provenance != ProvenanceCgroupV2Remaining {
		t.Fatalf("detected memory = %#v", plan.DetectedMemoryLimit)
	}
	if plan.MemoryBudget != plan.DetectedMemoryLimit {
		t.Fatalf("memory budget = %#v, detected = %#v", plan.MemoryBudget, plan.DetectedMemoryLimit)
	}
	if plan.Workers.Value > MaxTransferWorkers ||
		plan.Readers.Value > MaxTransferReaders ||
		plan.Writers.Value > MaxTransferWriters ||
		plan.QueueDepth.Value > MaxTransferQueueDepth ||
		plan.ChunkRows.Value > MaxTransferChunkRows {
		t.Fatalf("unbounded plan = %#v", plan)
	}
	if plan.ConnectionLimit.Value != DefaultConnectionLimit ||
		plan.Readers.Value+plan.Writers.Value >
			plan.ConnectionLimit.Value ||
		plan.Workers.Value > plan.ConnectionLimit.Value {
		t.Fatalf("connection-unsafe plan = %#v", plan)
	}
	if plan.Workers.Provenance != ProvenanceSafetyClamped ||
		plan.Readers.Provenance != ProvenanceSafetyClamped ||
		plan.Writers.Provenance != ProvenanceSafetyClamped ||
		plan.QueueDepth.Provenance != ProvenanceSafetyClamped ||
		plan.ChunkRows.Provenance != ProvenanceSafetyClamped {
		t.Fatalf("clamp provenance = %#v", plan)
	}
	memoryQueueCap := int(cgroupLimit / TransferMemoryPerSlotBytes)
	if plan.QueueDepth.Value > memoryQueueCap {
		t.Fatalf("queue depth %d exceeds memory-derived cap %d", plan.QueueDepth.Value, memoryQueueCap)
	}
	maxRowsByMemory := cgroupLimit / (int64(plan.QueueDepth.Value) * AssumedRetainedRowBytes)
	if int64(plan.ChunkRows.Value) > maxRowsByMemory {
		t.Fatalf("chunk rows %d exceed memory-derived cap %d", plan.ChunkRows.Value, maxRowsByMemory)
	}
}

func TestResolveEffectiveTransferPlanHonorsConnectionLimitAndProvenance(
	t *testing.T,
) {
	probe := fakeMemoryProbe{snapshot: MemorySnapshot{
		HostCapacityBytes:  8 * testGiB,
		HostAvailableBytes: 4 * testGiB,
		CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
		CgroupV1:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
	}}
	derived, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{LogicalCPUs: 32},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if derived.ConnectionLimit != (EffectiveInt{
		Value: DefaultConnectionLimit, Provenance: ProvenanceDerived,
	}) {
		t.Fatalf("derived connection limit = %#v", derived.ConnectionLimit)
	}
	if derived.Readers.Value+derived.Writers.Value >
		derived.ConnectionLimit.Value ||
		derived.Workers.Value > derived.ConnectionLimit.Value {
		t.Fatalf("derived plan exceeds connection limit: %#v", derived)
	}

	pinned, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{ConnectionLimit: 3},
		TransferPlanOptions{
			LogicalCPUs:      32,
			RequestedWorkers: 99,
			RequestedReaders: 99,
			RequestedWriters: 99,
		},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.ConnectionLimit != (EffectiveInt{
		Value: 3, Provenance: ProvenanceRequested,
	}) {
		t.Fatalf("pinned connection limit = %#v", pinned.ConnectionLimit)
	}
	if pinned.Readers.Value+pinned.Writers.Value > 3 ||
		pinned.Workers.Value > 3 {
		t.Fatalf("pinned plan exceeds connection limit: %#v", pinned)
	}
	if pinned.Readers.Provenance != ProvenanceSafetyClamped ||
		pinned.Writers.Provenance != ProvenanceSafetyClamped ||
		pinned.Workers.Provenance != ProvenanceSafetyClamped {
		t.Fatalf("connection clamps lack safety provenance: %#v", pinned)
	}

	if _, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{ConnectionLimit: 1},
		TransferPlanOptions{LogicalCPUs: 1},
		probe,
	); err == nil {
		t.Fatal("single connection admitted a reader/writer plan")
	}
}

func TestResolveEffectiveTransferPlanUserCeilingCanOnlyLowerDetectedLimit(t *testing.T) {
	probe := fakeMemoryProbe{snapshot: MemorySnapshot{
		HostCapacityBytes:  64 * testGiB,
		HostAvailableBytes: 32 * testGiB,
		CgroupV2: CgroupMemoryEvidence{
			State:      CgroupLimitFinite,
			LimitBytes: 256 * testMiB,
		},
	}}

	lowered, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{LogicalCPUs: 8, UserMemoryCeilingBytes: 128 * testMiB},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if lowered.MemoryBudget.Value != 128*testMiB ||
		lowered.MemoryBudget.Provenance != ProvenanceUserMemoryCeiling {
		t.Fatalf("lowered budget = %#v", lowered.MemoryBudget)
	}

	notRaised, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{LogicalCPUs: 8, UserMemoryCeilingBytes: 512 * testMiB},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if notRaised.MemoryBudget.Value != 256*testMiB ||
		notRaised.MemoryBudget.Provenance != ProvenanceCgroupV2Remaining {
		t.Fatalf("non-binding ceiling raised or rewrote budget: %#v", notRaised.MemoryBudget)
	}
}

func TestResolveEffectiveTransferPlanUsesFiniteCgroupV1RemainingMemory(t *testing.T) {
	plan, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{LogicalCPUs: 4},
		fakeMemoryProbe{snapshot: MemorySnapshot{
			HostCapacityBytes:  64 * testGiB,
			HostAvailableBytes: 16 * testGiB,
			CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
			CgroupV1: CgroupMemoryEvidence{
				State:        CgroupLimitFinite,
				LimitBytes:   512 * testMiB,
				CurrentBytes: 128 * testMiB,
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MemoryBudget.Value != 384*testMiB ||
		plan.MemoryBudget.Provenance != ProvenanceCgroupV1Remaining {
		t.Fatalf("v1 budget = %#v", plan.MemoryBudget)
	}
}

func TestResolveEffectiveTransferPlanUsesHostWhenCgroupIsKnownUnlimited(t *testing.T) {
	plan, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{LogicalCPUs: 4},
		fakeMemoryProbe{snapshot: MemorySnapshot{
			HostCapacityBytes:  64 * testGiB,
			HostAvailableBytes: 12 * testGiB,
			CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitUnlimited},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MemoryBudget.Value != 12*testGiB ||
		plan.MemoryBudget.Provenance != ProvenanceHostAvailable {
		t.Fatalf("unlimited cgroup budget = %#v", plan.MemoryBudget)
	}
}

func TestResolveEffectiveTransferPlanFailsClosedWithoutSafeFiniteEvidence(t *testing.T) {
	tests := []struct {
		name  string
		probe MemoryProbe
	}{
		{
			name: "unknown cgroup does not fall back to host",
			probe: fakeMemoryProbe{snapshot: MemorySnapshot{
				HostCapacityBytes:  64 * testGiB,
				HostAvailableBytes: 32 * testGiB,
				CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitUnknown},
			}},
		},
		{
			name:  "missing finite host evidence",
			probe: fakeMemoryProbe{snapshot: MemorySnapshot{}},
		},
		{
			name:  "probe failure",
			probe: fakeMemoryProbe{err: errors.New("injected probe failure")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ResolveEffectiveTransferPlan(
				context.Background(),
				Migration{},
				TransferPlanOptions{LogicalCPUs: 4},
				test.probe,
			); err == nil {
				t.Fatal("expected resource planning to fail closed")
			}
		})
	}
}

func TestResolveEffectiveTransferPlanRejectsUnsafeCeilingAndNegativeRequests(t *testing.T) {
	probe := fakeMemoryProbe{snapshot: MemorySnapshot{
		HostCapacityBytes:  8 * testGiB,
		HostAvailableBytes: 4 * testGiB,
		CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
		CgroupV1:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
	}}
	if _, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{UserMemoryCeilingBytes: MinimumTransferMemoryBytes - 1},
		probe,
	); err == nil {
		t.Fatal("expected too-small user ceiling to fail")
	}
	if _, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{},
		TransferPlanOptions{RequestedWorkers: -1},
		probe,
	); err == nil {
		t.Fatal("expected negative worker request to fail")
	}
}

func TestParsedTransferIntentPreservesRequestedAndDerivedProvenance(t *testing.T) {
	fields := []string{
		"target_mode",
		"include_tables",
		"exclude_tables",
		"date_updated_columns",
		"connection_limit",
		"workers",
		"chunk_size",
		"partitions",
		"large_table_threshold",
		"reader_parallelism",
		"writer_parallelism",
		"read_ahead",
		"upsert_merge_size",
		"memory_ceiling_bytes",
		"checkpoint_frequency",
		"max_retries",
		"strict_consistency",
		"strict_consistency_scope",
		"history_retention_days",
		"tuning",
		"runtime_tuning",
		"runtime_tuning_interval",
	}
	omitted, err := Parse([]byte("migration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		got, ok := omitted.Migration.SettingProvenance(field)
		if !ok || got != ProvenanceDerived {
			t.Fatalf("%s provenance = %q, found=%t; want derived", field, got, ok)
		}
	}
	if got, ok := omitted.Migration.SettingProvenance("not_a_setting"); ok || got != "" {
		t.Fatalf("unknown provenance = %q, found=%t", got, ok)
	}

	explicit, err := Parse([]byte(`
migration:
  target_mode: upsert
  include_tables: ["*"]
  exclude_tables: []
  date_updated_columns: [updated_at]
  connection_limit: 8
  workers: 6
  chunk_size: 321
  partitions: 2
  large_table_threshold: 1000
  reader_parallelism: 3
  writer_parallelism: 2
  read_ahead: 4
  upsert_merge_size: 200
  memory_ceiling_bytes: 134217728
  checkpoint_frequency: 0
  max_retries: 0
  strict_consistency: false
  strict_consistency_scope: table
  history_retention_days: 0
  tuning: auto
  runtime_tuning: false
  runtime_tuning_interval: 7s
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		got, ok := explicit.Migration.SettingProvenance(field)
		if !ok || got != ProvenanceRequested {
			t.Fatalf("%s provenance = %q, found=%t; want requested", field, got, ok)
		}
	}
}

func TestResourcePlanTreatsParsedOmissionsAsDerivedAndExplicitAsRequested(
	t *testing.T,
) {
	probe := fakeMemoryProbe{snapshot: MemorySnapshot{
		HostCapacityBytes:  2 * testGiB,
		HostAvailableBytes: testGiB,
		CgroupV2: CgroupMemoryEvidence{
			State:      CgroupLimitFinite,
			LimitBytes: 512 * testMiB,
		},
	}}
	omitted, err := Parse([]byte("migration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	derived, err := ResolveEffectiveTransferPlan(
		context.Background(),
		omitted.Migration,
		TransferPlanOptions{LogicalCPUs: 8},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if derived.MemoryBudget.Value != 512*testMiB ||
		derived.MemoryBudget.Provenance != ProvenanceCgroupV2Remaining {
		t.Fatalf("omitted memory ceiling became requested: %#v", derived.MemoryBudget)
	}
	for name, setting := range map[string]EffectiveInt{
		"workers": derived.Workers,
		"readers": derived.Readers,
		"writers": derived.Writers,
		"queue":   derived.QueueDepth,
		"chunk":   derived.ChunkRows,
	} {
		if setting.Provenance != ProvenanceDerived {
			t.Fatalf("%s = %#v, want derived", name, setting)
		}
	}

	explicit, err := Parse([]byte(`
migration:
  connection_limit: 8
  workers: 6
  reader_parallelism: 3
  writer_parallelism: 2
  read_ahead: 4
  chunk_size: 321
  memory_ceiling_bytes: 134217728
`))
	if err != nil {
		t.Fatal(err)
	}
	requested, err := ResolveEffectiveTransferPlan(
		context.Background(),
		explicit.Migration,
		TransferPlanOptions{LogicalCPUs: 8},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if requested.MemoryBudget.Value != 128*testMiB ||
		requested.MemoryBudget.Provenance != ProvenanceUserMemoryCeiling {
		t.Fatalf("explicit memory ceiling provenance = %#v", requested.MemoryBudget)
	}
	for name, setting := range map[string]EffectiveInt{
		"workers": requested.Workers,
		"readers": requested.Readers,
		"writers": requested.Writers,
		"queue":   requested.QueueDepth,
		"chunk":   requested.ChunkRows,
	} {
		if setting.Provenance != ProvenanceRequested {
			t.Fatalf("%s = %#v, want requested", name, setting)
		}
	}
}

func TestResourcePlanTreatsProgrammaticNonzeroValuesAsRequested(t *testing.T) {
	plan, err := ResolveEffectiveTransferPlan(
		context.Background(),
		Migration{
			Workers:           3,
			ReaderParallelism: 1,
			WriterParallelism: 1,
			ReadAhead:         2,
			ChunkSize:         123,
		},
		TransferPlanOptions{LogicalCPUs: 8},
		fakeMemoryProbe{snapshot: MemorySnapshot{
			HostCapacityBytes:  testGiB,
			HostAvailableBytes: 512 * testMiB,
			CgroupV2:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
			CgroupV1:           CgroupMemoryEvidence{State: CgroupLimitAbsent},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, setting := range map[string]EffectiveInt{
		"workers": plan.Workers,
		"readers": plan.Readers,
		"writers": plan.Writers,
		"queue":   plan.QueueDepth,
		"chunk":   plan.ChunkRows,
	} {
		if setting.Provenance != ProvenanceRequested {
			t.Fatalf("%s = %#v, want requested", name, setting)
		}
	}
}
