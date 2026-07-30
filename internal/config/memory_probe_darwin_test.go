//go:build darwin && !ios

package config

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestDarwinMemoryProbeParsesConservativeFiniteEvidence(t *testing.T) {
	t.Parallel()

	const evidence = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                      100.
Pages active:                                    200.
Pages inactive:                                  300.
Pages speculative:                                10.
Pages wired down:                                 50.
Pages purgeable:                                9000.
Pages occupied by compressor:                   8000.
`
	capacity, available, err := parseDarwinVMStat([]byte(evidence))
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(650 * 16384); capacity != want {
		t.Fatalf("capacity = %d, want %d", capacity, want)
	}
	if want := int64(400 * 16384); available != want {
		t.Fatalf("available = %d, want %d", available, want)
	}
}

func TestDarwinMemoryProbeFailsClosedOnIncompleteOrUnsafeEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		evidence string
		want     string
	}{
		{
			name:     "empty",
			evidence: "",
			want:     "empty",
		},
		{
			name: "missing category",
			evidence: `Mach Virtual Memory Statistics: (page size of 4096 bytes)
Pages free: 1.
Pages active: 2.
Pages inactive: 3.
Pages speculative: 4.
`,
			want: "missing",
		},
		{
			name: "negative pages",
			evidence: `Mach Virtual Memory Statistics: (page size of 4096 bytes)
Pages free: -1.
Pages active: 2.
Pages inactive: 3.
Pages speculative: 4.
Pages wired down: 5.
`,
			want: "Pages free",
		},
		{
			name: "byte overflow",
			evidence: `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free: 1.
Pages active: ` + "9223372036854775806" + `.
Pages inactive: 1.
Pages speculative: 1.
Pages wired down: 1.
`,
			want: "overflow",
		},
		{
			name: "speculative exceeds free",
			evidence: `Mach Virtual Memory Statistics: (page size of 4096 bytes)
Pages free: 3.
Pages active: 2.
Pages inactive: 3.
Pages speculative: 4.
Pages wired down: 5.
`,
			want: "speculative pages exceed free",
		},
		{
			name: "zero available",
			evidence: `Mach Virtual Memory Statistics: (page size of 4096 bytes)
Pages free: 0.
Pages active: 2.
Pages inactive: 0.
Pages speculative: 0.
Pages wired down: 5.
`,
			want: "not finite",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := parseDarwinVMStat([]byte(test.evidence))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDarwinMemoryProbeHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (darwinMemoryProbe{}).ProbeMemory(ctx)
	if err == nil {
		t.Fatal("canceled probe unexpectedly succeeded")
	}
}

func TestCheckedDarwinPageSumRejectsOverflow(t *testing.T) {
	t.Parallel()

	if _, err := checkedDarwinPageSum(math.MaxInt64, 1); err == nil {
		t.Fatal("overflow was accepted")
	}
}

func TestSystemDarwinMemoryProbeReturnsUsableEvidence(t *testing.T) {
	snapshot, err := NewSystemMemoryProbe().ProbeMemory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HostCapacityBytes <= 0 ||
		snapshot.HostAvailableBytes <= 0 ||
		snapshot.HostAvailableBytes > snapshot.HostCapacityBytes {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot.CgroupV1.State != CgroupLimitAbsent ||
		snapshot.CgroupV2.State != CgroupLimitAbsent {
		t.Fatalf("unexpected Darwin cgroup evidence: %#v", snapshot)
	}
}
