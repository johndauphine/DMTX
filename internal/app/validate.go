package app

import (
	"context"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

// executeValidate produces the validation outcome without deciding how it is
// shown. Every message keeps the exact text and stream the command line used,
// so the existing CLI tests remain the check that this changed no behaviour.
func executeValidate(ctx context.Context, request Request) Outcome {
	out := newOutcome(request.Command)
	if request.ConfigPath == "" {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx validate --config migration.yaml",
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
	if err := config.ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}
	result, err := migrate.ValidateSQLite(ctx, cfg)
	if err != nil {
		return out.failWith(ConfigurationError, "validation: "+err.Error())
	}
	if err := out.setPayload(PayloadResult, result); err != nil {
		return out.failWith(FileError, "write validation: "+err.Error())
	}
	if !result.Passed {
		return out.done(ValidationError)
	}
	return out.done(Success)
}
