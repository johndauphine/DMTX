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
