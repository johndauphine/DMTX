//go:build linux

package config

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type linuxMemoryProbe struct{}

// NewSystemMemoryProbe returns the Linux host/cgroup memory probe.
func NewSystemMemoryProbe() MemoryProbe {
	return linuxMemoryProbe{}
}

func (linuxMemoryProbe) ProbeMemory(ctx context.Context) (MemorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MemorySnapshot{}, err
	}
	capacity, available, err := readLinuxHostMemory("/proc/meminfo")
	if err != nil {
		return MemorySnapshot{}, err
	}
	v2Path, v1Path, err := readLinuxProcessCgroups("/proc/self/cgroup")
	if err != nil {
		return MemorySnapshot{}, err
	}
	v2, err := readLinuxCgroupV2(v2Path)
	if err != nil {
		return MemorySnapshot{}, err
	}
	v1, err := readLinuxCgroupV1(v1Path)
	if err != nil {
		return MemorySnapshot{}, err
	}
	return MemorySnapshot{
		HostCapacityBytes:  capacity,
		HostAvailableBytes: available,
		CgroupV2:           v2,
		CgroupV1:           v1,
	}, nil
}

func readLinuxHostMemory(path string) (int64, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("read host memory evidence: %w", err)
	}
	defer file.Close()

	var capacity, available int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || value > math.MaxInt64/1024 {
			return 0, 0, fmt.Errorf("parse host memory evidence %q", scanner.Text())
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "MemTotal":
			capacity = value * 1024
		case "MemAvailable":
			available = value * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("read host memory evidence: %w", err)
	}
	if capacity <= 0 || available <= 0 {
		return 0, 0, fmt.Errorf("host memory evidence is missing MemTotal or MemAvailable")
	}
	return capacity, available, nil
}

func readLinuxProcessCgroups(path string) (string, string, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("read process cgroup evidence: %w", err)
	}
	defer file.Close()

	var v2Path, v1Path string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 3)
		if len(parts) != 3 {
			return "", "", fmt.Errorf("parse process cgroup evidence %q", scanner.Text())
		}
		if parts[0] == "0" && parts[1] == "" {
			v2Path = parts[2]
			continue
		}
		for _, controller := range strings.Split(parts[1], ",") {
			if controller == "memory" {
				v1Path = parts[2]
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("read process cgroup evidence: %w", err)
	}
	return v2Path, v1Path, nil
}

func readLinuxCgroupV2(processPath string) (CgroupMemoryEvidence, error) {
	if processPath == "" {
		return CgroupMemoryEvidence{State: CgroupLimitAbsent}, nil
	}
	directory := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(filepath.Clean(processPath), string(filepath.Separator)))
	limitText, err := os.ReadFile(filepath.Join(directory, "memory.max"))
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("read cgroup v2 memory limit: %w", err)
	}
	limitValue := strings.TrimSpace(string(limitText))
	if limitValue == "max" {
		return CgroupMemoryEvidence{State: CgroupLimitUnlimited}, nil
	}
	limit, err := strconv.ParseInt(limitValue, 10, 64)
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("parse cgroup v2 memory limit: %w", err)
	}
	current, err := readLinuxInt64(filepath.Join(directory, "memory.current"))
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("read cgroup v2 current memory: %w", err)
	}
	return CgroupMemoryEvidence{
		State:        CgroupLimitFinite,
		LimitBytes:   limit,
		CurrentBytes: current,
	}, nil
}

func readLinuxCgroupV1(processPath string) (CgroupMemoryEvidence, error) {
	if processPath == "" {
		return CgroupMemoryEvidence{State: CgroupLimitAbsent}, nil
	}
	directory := filepath.Join("/sys/fs/cgroup/memory", strings.TrimPrefix(filepath.Clean(processPath), string(filepath.Separator)))
	limitText, err := os.ReadFile(filepath.Join(directory, "memory.limit_in_bytes"))
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("read cgroup v1 memory limit: %w", err)
	}
	limitValue := strings.TrimSpace(string(limitText))
	unsignedLimit, err := strconv.ParseUint(limitValue, 10, 64)
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("parse cgroup v1 memory limit: %w", err)
	}
	const v1UnlimitedThreshold = uint64(math.MaxInt64 - 4095)
	if unsignedLimit >= v1UnlimitedThreshold {
		return CgroupMemoryEvidence{State: CgroupLimitUnlimited}, nil
	}
	current, err := readLinuxInt64(filepath.Join(directory, "memory.usage_in_bytes"))
	if err != nil {
		return CgroupMemoryEvidence{}, fmt.Errorf("read cgroup v1 current memory: %w", err)
	}
	return CgroupMemoryEvidence{
		State:        CgroupLimitFinite,
		LimitBytes:   int64(unsignedLimit),
		CurrentBytes: current,
	}, nil
}

func readLinuxInt64(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}
