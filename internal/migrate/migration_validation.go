package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// ValidateMigration rejects unsupported behavior before a caller opens state,
// acquires a lease, connects to a database, or mutates a target.
func ValidateMigration(cfg config.Config) error {
	_, err := resolveMigration(cfg, builtInAdapters)
	return err
}

func resolveMigration(
	cfg config.Config,
	registry adapterRegistry,
) (resolvedAdapterRoute, error) {
	source, target, err := registry.roles(cfg.Source.Type, cfg.Target.Type)
	if err != nil {
		return resolvedAdapterRoute{}, err
	}
	if err := requireStage4LargeTableThresholdRoute(
		cfg,
		source.engine,
		target.engine,
	); err != nil {
		return resolvedAdapterRoute{}, err
	}
	strictScope, err := normalizedStrictConsistencyScope(
		cfg.Migration.StrictConsistencyScope,
	)
	if err != nil {
		return resolvedAdapterRoute{}, err
	}
	if cfg.Migration.StrictConsistency {
		if source.engine == "mysql" && strictScope != config.StrictConsistencyTable {
			return resolvedAdapterRoute{}, fmt.Errorf(
				"Stage 4 MySQL/MariaDB strict consistency supports table scope only",
			)
		}
		if source.engine == "sqlite" && strictScope != config.StrictConsistencyTable {
			return resolvedAdapterRoute{}, fmt.Errorf(
				"Stage 4 SQLite strict consistency supports table scope only",
			)
		}
		if !certifiedStrictConsistencyComposition(
			source.engine,
			target.engine,
			cfg.Migration.TargetMode,
		) {
			return resolvedAdapterRoute{}, fmt.Errorf(
				"Stage 4 strict consistency scope %q has no certified source/target composition",
				strictScope,
			)
		}
	}
	if config.SameEndpoint(cfg.Source, cfg.Target) {
		return resolvedAdapterRoute{}, fmt.Errorf(
			"source and target resolve to the same endpoint",
		)
	}
	if cfg.Migration.TargetMode == "upsert" && !target.capability.Upsert {
		return resolvedAdapterRoute{}, fmt.Errorf(
			"target engine %s does not support upsert mode",
			cfg.Target.Type,
		)
	}
	route, err := registry.certifiedRoute(source, target)
	if err != nil {
		return resolvedAdapterRoute{}, err
	}
	if route.target.validate != nil {
		if err := route.target.validate(cfg.Target); err != nil {
			return resolvedAdapterRoute{}, err
		}
	}
	if route.source.validate != nil {
		if err := route.source.validate(cfg.Source); err != nil {
			return resolvedAdapterRoute{}, err
		}
	}
	return route, nil
}

// certifiedStrictConsistencyComposition is the configuration-time admission
// boundary. PostgreSQL and SQL Server are admitted only for the source scopes
// backed by their Stage 4 strict openers, and only to targets with composed
// keyed-upsert writer and validation paths. No other source gains strict
// support merely because it is a registered ordinary migration route.
func certifiedStrictConsistencyComposition(
	sourceEngine string,
	targetEngine string,
	targetMode string,
) bool {
	if targetMode != "upsert" {
		return false
	}
	if sourceEngine == "postgres" {
		switch targetEngine {
		case "postgres", "mysql", "mssql", "sqlite":
			return true
		}
		return false
	}
	if sourceEngine == "mssql" {
		switch targetEngine {
		case "postgres", "mysql", "mssql", "sqlite":
			return true
		}
		return false
	}
	// MySQL/MariaDB's strict primitive is table-scoped and can supply the
	// certified network targets, including SQLite's established upsert route.
	if sourceEngine == "mysql" {
		switch targetEngine {
		case "postgres", "mysql", "mssql", "sqlite":
			return true
		}
	}
	// SQLite's strict view is one read transaction and is therefore table
	// scoped. Scope admission is enforced above before this source/target
	// matrix is consulted; every listed target owns the existing keyed network
	// upsert and validation route.
	if sourceEngine == "sqlite" {
		switch targetEngine {
		case "postgres", "mysql", "mssql", "sqlite":
			return true
		}
	}
	return false
}

func normalizedStrictConsistencyScope(scope string) (string, error) {
	if scope == "" {
		return "table", nil
	}
	switch scope {
	case "table", "migration":
		return scope, nil
	default:
		return "", fmt.Errorf("invalid strict_consistency_scope %q", scope)
	}
}
