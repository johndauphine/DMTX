package migrate

import (
	"fmt"
	"strings"
)

type adapterPair struct {
	source string
	target string
}

type adapterOverride struct {
	pair adapterPair
	run  migrationRunner
}

type resolvedAdapterRoute struct {
	source   sourceRole
	target   targetRole
	override migrationRunner
}

// adapterRegistry is immutable after construction. Production code exposes no
// registration or replacement hook, so concurrent validation cannot observe a
// partially updated capability matrix.
type adapterRegistry struct {
	sources   map[string]sourceRole
	targets   map[string]targetRole
	certified map[adapterPair]struct{}
	overrides map[adapterPair]migrationRunner
}

func newAdapterRegistry(
	sources []sourceRole,
	targets []targetRole,
	certified []adapterPair,
	overrides []adapterOverride,
) (adapterRegistry, error) {
	registry := adapterRegistry{
		sources:   make(map[string]sourceRole, len(sources)),
		targets:   make(map[string]targetRole, len(targets)),
		certified: make(map[adapterPair]struct{}, len(certified)),
		overrides: make(map[adapterPair]migrationRunner, len(overrides)),
	}
	for _, role := range sources {
		if err := validateAdapterName("source", role.engine); err != nil {
			return adapterRegistry{}, err
		}
		if _, exists := registry.sources[role.engine]; exists {
			return adapterRegistry{}, fmt.Errorf("duplicate source adapter %q", role.engine)
		}
		registry.sources[role.engine] = role
	}
	for _, role := range targets {
		if err := validateAdapterName("target", role.engine); err != nil {
			return adapterRegistry{}, err
		}
		if role.capability.BulkPath == "" {
			return adapterRegistry{}, fmt.Errorf(
				"target adapter %q has no capability declaration",
				role.engine,
			)
		}
		if _, exists := registry.targets[role.engine]; exists {
			return adapterRegistry{}, fmt.Errorf("duplicate target adapter %q", role.engine)
		}
		registry.targets[role.engine] = role
	}
	for _, pair := range certified {
		if _, exists := registry.sources[pair.source]; !exists {
			return adapterRegistry{}, fmt.Errorf(
				"migration pair %s-to-%s references an unregistered source adapter",
				pair.source,
				pair.target,
			)
		}
		if _, exists := registry.targets[pair.target]; !exists {
			return adapterRegistry{}, fmt.Errorf(
				"migration pair %s-to-%s references an unregistered target adapter",
				pair.source,
				pair.target,
			)
		}
		if _, exists := registry.certified[pair]; exists {
			return adapterRegistry{}, fmt.Errorf(
				"duplicate certified migration pair %s-to-%s",
				pair.source,
				pair.target,
			)
		}
		registry.certified[pair] = struct{}{}
	}
	for _, override := range overrides {
		if override.run == nil {
			return adapterRegistry{}, fmt.Errorf(
				"migration pair %s-to-%s has a nil override",
				override.pair.source,
				override.pair.target,
			)
		}
		if _, exists := registry.certified[override.pair]; !exists {
			return adapterRegistry{}, fmt.Errorf(
				"migration pair %s-to-%s override is not certified",
				override.pair.source,
				override.pair.target,
			)
		}
		if _, exists := registry.overrides[override.pair]; exists {
			return adapterRegistry{}, fmt.Errorf(
				"duplicate migration pair %s-to-%s override",
				override.pair.source,
				override.pair.target,
			)
		}
		registry.overrides[override.pair] = override.run
	}
	for pair := range registry.certified {
		if _, overridden := registry.overrides[pair]; overridden {
			continue
		}
		source, target := registry.sources[pair.source], registry.targets[pair.target]
		if source.open == nil || target.open == nil {
			return adapterRegistry{}, fmt.Errorf(
				"migration pair %s-to-%s has neither composable adapters nor an override",
				pair.source,
				pair.target,
			)
		}
	}
	return registry, nil
}

func validateAdapterName(role, name string) error {
	if name == "" {
		return fmt.Errorf("%s adapter engine is required", role)
	}
	if name != strings.TrimSpace(name) || name != strings.ToLower(name) {
		return fmt.Errorf("%s adapter engine %q is not canonical", role, name)
	}
	return nil
}

func (registry adapterRegistry) roles(
	source string,
	target string,
) (sourceRole, targetRole, error) {
	targetRole, exists := registry.targets[target]
	if !exists {
		return sourceRole{}, targetRole, fmt.Errorf(
			"target engine %q is not registered",
			target,
		)
	}
	sourceRole, exists := registry.sources[source]
	if !exists {
		return sourceRole, targetRole, fmt.Errorf(
			"source engine %q is not registered",
			source,
		)
	}
	return sourceRole, targetRole, nil
}

func (registry adapterRegistry) certifiedRoute(
	source sourceRole,
	target targetRole,
) (resolvedAdapterRoute, error) {
	pair := adapterPair{source: source.engine, target: target.engine}
	if _, exists := registry.certified[pair]; !exists {
		return resolvedAdapterRoute{}, fmt.Errorf(
			"migration pair %s-to-%s is not implemented",
			pair.source,
			pair.target,
		)
	}
	return resolvedAdapterRoute{
		source:   source,
		target:   target,
		override: registry.overrides[pair],
	}, nil
}

func (registry adapterRegistry) route(
	source string,
	target string,
) (resolvedAdapterRoute, error) {
	sourceRole, targetRole, err := registry.roles(source, target)
	if err != nil {
		return resolvedAdapterRoute{}, err
	}
	return registry.certifiedRoute(sourceRole, targetRole)
}
