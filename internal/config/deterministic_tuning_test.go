package config

import (
	"context"
	"testing"
)

// TestDeterministicTuningPreservesPinnedIntent is the Stage 4 closeout fixture
// for deterministic tuning. Derivation must never rewrite an operator's pinned
// intent into a different number, and the only permitted movement is a downward
// safety clamp that says so in its provenance.
func TestDeterministicTuningPreservesPinnedIntent(t *testing.T) {
	t.Parallel()

	t.Run("explicit fields report requested provenance", func(t *testing.T) {
		t.Parallel()
		pinned, err := Parse([]byte(`
source: {}
target: {}
migration:
  connection_limit: 8
  workers: 6
  reader_parallelism: 3
  writer_parallelism: 2
  chunk_size: 250
  max_retries: 5
`))
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{
			"connection_limit",
			"workers",
			"reader_parallelism",
			"writer_parallelism",
			"chunk_size",
			"max_retries",
		} {
			provenance, known := pinned.Migration.SettingProvenance(field)
			if !known || provenance != ProvenanceRequested {
				t.Fatalf(
					"pinned %q provenance = %q known=%v",
					field,
					provenance,
					known,
				)
			}
		}
		// An omitted tuning field must stay derived. Reporting it as requested
		// would let a later run treat a default as operator intent.
		for _, field := range []string{
			"partitions",
			"read_ahead",
			"upsert_merge_size",
			"memory_ceiling_bytes",
		} {
			provenance, known := pinned.Migration.SettingProvenance(field)
			if !known || provenance != ProvenanceDerived {
				t.Fatalf(
					"omitted %q provenance = %q known=%v",
					field,
					provenance,
					known,
				)
			}
		}
	})

	t.Run("pinned values survive derivation unchanged", func(t *testing.T) {
		t.Parallel()
		parsed, err := Parse([]byte(`
source: {}
target: {}
migration:
  connection_limit: 12
  workers: 6
  reader_parallelism: 3
  writer_parallelism: 2
  chunk_size: 250
`))
		if err != nil {
			t.Fatal(err)
		}
		plan, err := ResolveEffectiveTransferPlan(
			context.Background(),
			parsed.Migration,
			TransferPlanOptions{LogicalCPUs: 16},
			fakeMemoryProbe{snapshot: MemorySnapshot{
				HostCapacityBytes:  64 * testGiB,
				HostAvailableBytes: 32 * testGiB,
				CgroupV2: CgroupMemoryEvidence{
					State:      CgroupLimitFinite,
					LimitBytes: 8 * testGiB,
				},
			}},
		)
		if err != nil {
			t.Fatal(err)
		}
		if plan.ConnectionLimit.Value != 12 ||
			plan.ConnectionLimit.Provenance != ProvenanceRequested {
			t.Fatalf("connection limit = %#v", plan.ConnectionLimit)
		}
		if plan.Readers.Value != 3 ||
			plan.Readers.Provenance != ProvenanceRequested {
			t.Fatalf("readers = %#v", plan.Readers)
		}
		if plan.Writers.Value != 2 ||
			plan.Writers.Provenance != ProvenanceRequested {
			t.Fatalf("writers = %#v", plan.Writers)
		}
		if plan.ChunkRows.Value != 250 ||
			plan.ChunkRows.Provenance != ProvenanceRequested {
			t.Fatalf("chunk rows = %#v", plan.ChunkRows)
		}
	})

	// Unsatisfiable pinned intent is rejected outright rather than quietly
	// clamped, which is a stronger guarantee than the tuning section assumes:
	// the operator is told their numbers cannot hold instead of running under
	// different ones.
	t.Run("unsatisfiable pinned intent is rejected", func(t *testing.T) {
		t.Parallel()
		for name, document := range map[string]string{
			"concurrency above connection limit": `
source: {}
target: {}
migration:
  connection_limit: 4
  workers: 16
  reader_parallelism: 8
  writer_parallelism: 6
`,
			"workers below concurrency": `
source: {}
target: {}
migration:
  connection_limit: 16
  workers: 2
  reader_parallelism: 4
  writer_parallelism: 3
`,
		} {
			if _, err := Parse([]byte(document)); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}
	})

	t.Run("clamping only lowers and is labelled", func(t *testing.T) {
		t.Parallel()
		// Configuration rejects over-subscribed pins, so the clamp is reached
		// through caller-supplied requests, which are not parse-validated.
		parsed, err := Parse([]byte(`
source: {}
target: {}
migration:
  connection_limit: 4
`))
		if err != nil {
			t.Fatal(err)
		}
		plan, err := ResolveEffectiveTransferPlan(
			context.Background(),
			parsed.Migration,
			TransferPlanOptions{
				LogicalCPUs:      16,
				RequestedReaders: 8,
				RequestedWriters: 6,
			},
			fakeMemoryProbe{snapshot: MemorySnapshot{
				HostCapacityBytes:  64 * testGiB,
				HostAvailableBytes: 32 * testGiB,
				CgroupV2: CgroupMemoryEvidence{
					State:      CgroupLimitFinite,
					LimitBytes: 8 * testGiB,
				},
			}},
		)
		if err != nil {
			t.Fatal(err)
		}
		if plan.ConnectionLimit.Value != 4 {
			t.Fatalf("clamp raised the pinned connection limit: %#v", plan.ConnectionLimit)
		}
		if plan.Readers.Value+plan.Writers.Value > plan.ConnectionLimit.Value {
			t.Fatalf(
				"clamp left concurrency above the limit: readers=%#v writers=%#v limit=%#v",
				plan.Readers,
				plan.Writers,
				plan.ConnectionLimit,
			)
		}
		if plan.Readers.Value < 1 || plan.Writers.Value < 1 {
			t.Fatalf(
				"clamp starved a side: readers=%#v writers=%#v",
				plan.Readers,
				plan.Writers,
			)
		}
		// A clamped value must say so; silently reporting it as derived or
		// requested would misrepresent why the migration runs at that width.
		if plan.Readers.Provenance != ProvenanceSafetyClamped &&
			plan.Writers.Provenance != ProvenanceSafetyClamped {
			t.Fatalf(
				"clamped plan labelled nothing: readers=%#v writers=%#v",
				plan.Readers,
				plan.Writers,
			)
		}
	})

	t.Run("resolution is deterministic", func(t *testing.T) {
		t.Parallel()
		parsed, err := Parse([]byte(`
source: {}
target: {}
migration:
  connection_limit: 12
  workers: 8
  reader_parallelism: 4
  writer_parallelism: 3
  chunk_size: 400
`))
		if err != nil {
			t.Fatal(err)
		}
		probe := fakeMemoryProbe{snapshot: MemorySnapshot{
			HostCapacityBytes:  64 * testGiB,
			HostAvailableBytes: 32 * testGiB,
			CgroupV2: CgroupMemoryEvidence{
				State:      CgroupLimitFinite,
				LimitBytes: 8 * testGiB,
			},
		}}
		first, err := ResolveEffectiveTransferPlan(
			context.Background(),
			parsed.Migration,
			TransferPlanOptions{LogicalCPUs: 16},
			probe,
		)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 4; attempt++ {
			again, err := ResolveEffectiveTransferPlan(
				context.Background(),
				parsed.Migration,
				TransferPlanOptions{LogicalCPUs: 16},
				probe,
			)
			if err != nil {
				t.Fatal(err)
			}
			if again != first {
				t.Fatalf(
					"tuning is not deterministic: attempt %d = %#v, first = %#v",
					attempt,
					again,
					first,
				)
			}
		}
	})
}
