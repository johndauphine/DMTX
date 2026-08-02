package app

import (
	"context"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

func executePreflight(ctx context.Context, request Request) Outcome {
	return executePreflightWithProbe(ctx, request, probeProductionPreflight)
}

// executePreflightWithProbe keeps the probe injectable so tests can supply
// evidence without live endpoints, exactly as before the seam existed.
func executePreflightWithProbe(
	ctx context.Context,
	request Request,
	probe func(context.Context, config.Config) ([]productionPreflightFact, bool),
) Outcome {
	out := newOutcome(request.Command)
	if request.ConfigPath == "" {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx preflight --config migration.yaml",
		)
	}
	data, err := os.ReadFile(request.ConfigPath)
	if err != nil {
		return out.failWith(FileError, "read configuration: "+err.Error())
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}
	if err := migrate.ValidateStage4ComposedConfiguration(cfg); err != nil {
		// A refused configuration still produces a report: the operator asked
		// what would happen, and "here is why not" is the answer.
		report := stage4ConfigurationPreflightReport(err)
		if encodeErr := out.setPayload(PayloadPreflightReport, report); encodeErr != nil {
			return out.failWith(FileError, "write preflight: "+encodeErr.Error())
		}
		return out.done(ConfigurationError)
	}

	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	facts, sourceSizeEvidence := probe(probeCtx, cfg)
	report, err := composeProductionPreflightReport(
		cfg,
		facts,
		sourceSizeEvidence,
	)
	if err != nil {
		return out.failWith(ConfigurationError, "preflight evidence: "+err.Error())
	}
	if err := out.setPayload(PayloadPreflightReport, report); err != nil {
		return out.failWith(FileError, "write preflight: "+err.Error())
	}
	if !report.Proceed {
		return out.done(ConfigurationError)
	}
	return out.done(Success)
}

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
