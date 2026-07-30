//go:build darwin && !ios

package config

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

type darwinMemoryProbe struct{}

// NewSystemMemoryProbe returns the Darwin vm_stat host-memory probe.
func NewSystemMemoryProbe() MemoryProbe {
	return darwinMemoryProbe{}
}

func (darwinMemoryProbe) ProbeMemory(ctx context.Context) (MemorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MemorySnapshot{}, err
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/vm_stat").Output()
	if err != nil {
		return MemorySnapshot{}, fmt.Errorf("read Darwin host memory evidence: %w", err)
	}
	capacity, available, err := parseDarwinVMStat(output)
	if err != nil {
		return MemorySnapshot{}, err
	}
	return MemorySnapshot{
		HostCapacityBytes:  capacity,
		HostAvailableBytes: available,
		CgroupV2: CgroupMemoryEvidence{
			State: CgroupLimitAbsent,
		},
		CgroupV1: CgroupMemoryEvidence{
			State: CgroupLimitAbsent,
		},
	}, nil
}

func parseDarwinVMStat(output []byte) (int64, int64, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	if !scanner.Scan() {
		return 0, 0, fmt.Errorf("Darwin host memory evidence is empty")
	}
	const pageSizePrefix = "Mach Virtual Memory Statistics: (page size of "
	header := strings.TrimSpace(scanner.Text())
	if !strings.HasPrefix(header, pageSizePrefix) ||
		!strings.HasSuffix(header, " bytes)") {
		return 0, 0, fmt.Errorf("parse Darwin host memory page size")
	}
	pageSizeText := strings.TrimSuffix(
		strings.TrimPrefix(header, pageSizePrefix),
		" bytes)",
	)
	pageSize, err := strconv.ParseInt(pageSizeText, 10, 64)
	if err != nil || pageSize <= 0 {
		return 0, 0, fmt.Errorf("parse Darwin host memory page size")
	}

	required := map[string]int64{
		"Pages free":        -1,
		"Pages active":      -1,
		"Pages inactive":    -1,
		"Pages speculative": -1,
		"Pages wired down":  -1,
	}
	for scanner.Scan() {
		line := scanner.Text()
		separator := strings.IndexByte(line, ':')
		if separator < 0 {
			continue
		}
		name := strings.TrimSpace(line[:separator])
		if _, wanted := required[name]; !wanted {
			continue
		}
		valueText := strings.TrimSpace(line[separator+1:])
		valueText = strings.TrimSuffix(valueText, ".")
		pages, err := strconv.ParseInt(valueText, 10, 64)
		if err != nil || pages < 0 {
			return 0, 0, fmt.Errorf(
				"parse Darwin host memory evidence field %q",
				name,
			)
		}
		required[name] = pages
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("read Darwin host memory evidence: %w", err)
	}
	for name, pages := range required {
		if pages < 0 {
			return 0, 0, fmt.Errorf(
				"Darwin host memory evidence is missing %q",
				name,
			)
		}
	}

	// Mach's free_count already includes speculative_count. Validate that
	// relationship but never add speculative pages separately. Omitting
	// purgeable and compressor statistics keeps the capacity estimate a
	// conservative lower bound. Free plus inactive pages are the currently
	// available transfer headroom.
	if required["Pages speculative"] > required["Pages free"] {
		return 0, 0, fmt.Errorf(
			"Darwin host memory speculative pages exceed free pages",
		)
	}
	capacityPages, err := checkedDarwinPageSum(
		required["Pages free"],
		required["Pages active"],
		required["Pages inactive"],
		required["Pages wired down"],
	)
	if err != nil {
		return 0, 0, err
	}
	availablePages, err := checkedDarwinPageSum(
		required["Pages free"],
		required["Pages inactive"],
	)
	if err != nil {
		return 0, 0, err
	}
	if capacityPages == 0 || availablePages == 0 ||
		capacityPages > math.MaxInt64/pageSize ||
		availablePages > math.MaxInt64/pageSize {
		return 0, 0, fmt.Errorf("Darwin host memory evidence is not finite")
	}
	return capacityPages * pageSize, availablePages * pageSize, nil
}

func checkedDarwinPageSum(values ...int64) (int64, error) {
	var sum int64
	for _, value := range values {
		if value < 0 || value > math.MaxInt64-sum {
			return 0, fmt.Errorf("Darwin host memory page counts overflow")
		}
		sum += value
	}
	return sum, nil
}
