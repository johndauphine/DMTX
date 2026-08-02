package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

func preflight(args []string, stdout, stderr io.Writer) int {
	return preflightWithProbe(args, stdout, stderr, probeProductionPreflight)
}

func preflightWithProbe(
	args []string,
	stdout, stderr io.Writer,
	probe func(context.Context, config.Config) ([]productionPreflightFact, bool),
) int {
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
	if err := migrate.ValidateStage4ComposedConfiguration(cfg); err != nil {
		report := stage4ConfigurationPreflightReport(err)
		if encodeErr := json.NewEncoder(stdout).Encode(report); encodeErr != nil {
			fmt.Fprintf(stderr, "write preflight: %v\n", encodeErr)
			return FileError
		}
		return ConfigurationError
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	facts, sourceSizeEvidence := probe(ctx, cfg)
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

// stage4ConfigurationPreflightReport represents a policy failure that is
// known before an endpoint can be opened. Skip selectors intentionally do not
// apply: a composed Stage 4 runner will reject this configuration regardless
// of endpoint health or operator skip policy.
func stage4ConfigurationPreflightReport(err error) productionPreflightReport {
	return productionPreflightReport{
		Proceed:       false,
		SkipSelectors: []string{},
		Findings: []productionPreflightFinding{
			{
				Severity: migrate.PreflightSeverityError,
				Check:    "policy.stage4.composed_admission",
				Side:     migrate.PreflightTarget,
				Class:    preflightClassFailed,
				Message:  err.Error(),
				Remedy:   "remove the unsupported Stage 4 option or select a certified configuration",
				Evidence: "evaluated from configuration before endpoint probing",
			},
		},
	}
}
