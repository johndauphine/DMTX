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
