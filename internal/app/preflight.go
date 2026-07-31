package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

func preflight(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(
			stderr,
			"usage: dmtx preflight --config migration.yaml",
		)
		return ConfigurationError
	}
	data, err := os.ReadFile(args[1])
	if err != nil {
		fmt.Fprintf(stderr, "read configuration: %v\n", err)
		return FileError
	}
	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	facts, sourceSizeEvidence := probeProductionPreflight(ctx, cfg)
	report, err := composeProductionPreflightReport(
		cfg,
		facts,
		sourceSizeEvidence,
	)
	if err != nil {
		fmt.Fprintf(stderr, "preflight evidence: %v\n", err)
		return ConfigurationError
	}
	if err := json.NewEncoder(stdout).Encode(report); err != nil {
		fmt.Fprintf(stderr, "write preflight: %v\n", err)
		return FileError
	}
	if !report.Proceed {
		return ConfigurationError
	}
	return Success
}
