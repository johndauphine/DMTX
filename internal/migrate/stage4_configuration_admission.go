package migrate

import (
	"errors"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// ValidateStage4ComposedConfiguration applies the configuration-only policy
// gates used by the composed Stage 4 runner. It deliberately opens neither
// endpoint and writes no state, so dry-run and preflight can report exactly
// the same unsupported-policy decision before they inspect a target.
//
// This gate is intentionally independent of observer/lifecycle admission:
// those checks need a live run and a fenced state backend, while this helper
// covers only facts determined by configuration.
func ValidateStage4ComposedConfiguration(cfg config.Config) error {
	if err := requireStage4AdapterConfigurationSeams(cfg); err != nil {
		return err
	}
	source, sourceErr := config.CanonicalEngine(cfg.Source.Type)
	target, targetErr := config.CanonicalEngine(cfg.Target.Type)
	if sourceErr != nil || targetErr != nil {
		// Route validation owns unsupported engine diagnostics. This helper is
		// deliberately a Stage 4 policy gate, not a second route registry.
		return nil
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		if !stage4AdapterIncrementalSourceEngine(source) {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 date-based incremental source engine %q is not certified",
					source,
				),
			)
		}
		if !stage4AdapterIncrementalTargetEngine(target) {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 date-based incremental target engine %q is not certified",
					target,
				),
			)
		}
	}
	if cfg.Migration.StrictConsistency {
		switch source {
		case "postgres":
			switch target {
			case "postgres", "mysql", "mssql", "sqlite":
			default:
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf("Stage 4 PostgreSQL strict consistency has no certified upsert target %q", target),
				)
			}
		case "mssql":
			if _, err := stage4SQLServerStrictScope(
				cfg.Migration.StrictConsistencyScope,
			); err != nil {
				return err
			}
			switch target {
			case "postgres", "mysql", "mssql", "sqlite":
			default:
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf("Stage 4 SQL Server strict consistency has no certified upsert target %q", target),
				)
			}
		case "mysql":
			scope, err := normalizedStrictConsistencyScope(
				cfg.Migration.StrictConsistencyScope,
			)
			if err != nil {
				return NewTransferError(ErrorClassPolicy, err)
			}
			if scope != config.StrictConsistencyTable {
				return NewTransferError(
					ErrorClassPolicy,
					errors.New("Stage 4 MySQL/MariaDB strict consistency supports table scope only"),
				)
			}
			switch target {
			case "postgres", "mysql", "mssql", "sqlite":
			default:
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf("Stage 4 MySQL/MariaDB strict consistency has no certified upsert target %q", target),
				)
			}
		case "sqlite":
			scope, err := normalizedStrictConsistencyScope(
				cfg.Migration.StrictConsistencyScope,
			)
			if err != nil {
				return NewTransferError(ErrorClassPolicy, err)
			}
			if scope != config.StrictConsistencyTable {
				return NewTransferError(
					ErrorClassPolicy,
					errors.New("Stage 4 SQLite strict consistency supports table scope only"),
				)
			}
			switch target {
			case "postgres", "mysql", "mssql", "sqlite":
			default:
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf("Stage 4 SQLite strict consistency has no certified upsert target %q", target),
				)
			}
		default:
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("Stage 4 strict consistency has no certified source composition for engine %q", source),
			)
		}
	}
	return nil
}
