package app

import (
	"context"
	"fmt"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

// Analysis reports the resource plan a migration would run under, and why each
// value is what it is.
//
// It answers "why four workers?", which an operator asks when a migration is
// slower or heavier than they expected. The provenance on each setting is the
// answer: requested, derived from the memory envelope, or clamped for safety.
type Analysis struct {
	Path   string                 `json:"path"`
	Tuning *migrate.PlannedTuning `json:"tuning"`
}

// executeAnalyze reports the effective transfer plan without running anything.
//
// Offline by design. It reads host memory evidence and the configuration, and
// opens no database, so it answers on a laptop with the VPN down - which is
// often exactly where the question is being asked. run --dry-run discloses the
// same numbers, from the same code path, but a dry run connects to the source
// to discover a plan; this does not.
//
// DMT's analyze goes further and proposes configuration changes. That is
// deliberately not here: recommendations are only worth having when there is a
// policy behind them worth defending, and dmtx does not yet have measurements
// to justify one.
func executeAnalyze(ctx context.Context, request Request) Outcome {
	out := newOutcome(request.Command)
	if request.ConfigPath == "" {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx analyze --config migration.yaml",
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
	// Checked here as run checks it, because a plan for a migration that will
	// never start is a confident answer to a question the operator did not ask.
	// It resolves adapter roles from the configured types and opens nothing, so
	// the report stays offline.
	if err := migrate.ValidateMigration(cfg); err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}

	tuning, err := migrate.DiscloseTuning(ctx, cfg)
	if err != nil {
		return out.failWith(ConfigurationError, "analyze: "+err.Error())
	}

	analysis := Analysis{Path: request.ConfigPath, Tuning: tuning}
	for _, line := range analysis.lines() {
		out.out(line)
	}
	if err := out.setPayload(PayloadAnalysis, analysis); err != nil {
		return out.failWith(FileError, "write analysis: "+err.Error())
	}
	return out.done(Success)
}

// lines renders the analysis for a terminal.
func (analysis Analysis) lines() []string {
	lines := []string{"effective plan for " + analysis.Path}
	if analysis.Tuning == nil {
		return lines
	}
	for _, setting := range []struct {
		label string
		value migrate.PlannedSetting
		unit  string
	}{
		{"workers", analysis.Tuning.Workers, ""},
		{"readers", analysis.Tuning.Readers, ""},
		{"writers", analysis.Tuning.Writers, ""},
		{"queue depth", analysis.Tuning.QueueDepth, ""},
		{"chunk rows", analysis.Tuning.ChunkRows, ""},
		{"connection limit", analysis.Tuning.ConnectionLimit, ""},
		{"memory budget", analysis.Tuning.MemoryBudget, " bytes"},
	} {
		// The provenance is on every line rather than only the surprising ones.
		// Which value is surprising is the operator's judgement, and a report
		// that decided for them would hide the one they came to check.
		lines = append(lines, fmt.Sprintf(
			"  %-17s %d%s  (%s)",
			setting.label+":", setting.value.Value, setting.unit,
			setting.value.Provenance,
		))
	}
	return lines
}
