//go:build !linux && (!darwin || ios)

package config

import (
	"context"
	"fmt"
	"runtime"
)

type unsupportedMemoryProbe struct{}

// NewSystemMemoryProbe fails closed on platforms without a trustworthy probe.
func NewSystemMemoryProbe() MemoryProbe {
	return unsupportedMemoryProbe{}
}

func (unsupportedMemoryProbe) ProbeMemory(context.Context) (MemorySnapshot, error) {
	return MemorySnapshot{}, fmt.Errorf("safe finite memory probing is unsupported on %s", runtime.GOOS)
}
