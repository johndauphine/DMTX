package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

var testTargetCapability = engine.Capability{
	BulkPath:          "test batches",
	Upsert:            true,
	StrictConsistency: "test",
}

func unusedSourceFactory(
	context.Context,
	config.Endpoint,
) (sourceAdapter, error) {
	return nil, errors.New("unused source factory")
}

func unusedTargetFactory(
	context.Context,
	config.Endpoint,
) (targetAdapter, error) {
	return nil, errors.New("unused target factory")
}

func noOpMigrationRunner(
	context.Context,
	config.Config,
	TableObserver,
) (Result, error) {
	return Result{}, nil
}

func TestNewAdapterRegistryRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name      string
		sources   []sourceRole
		targets   []targetRole
		certified []adapterPair
		overrides []adapterOverride
		want      string
	}{
		{
			name:    "noncanonical source",
			sources: []sourceRole{{engine: "Postgres"}},
			want:    "not canonical",
		},
		{
			name: "duplicate source",
			sources: []sourceRole{
				{engine: "sqlite"},
				{engine: "sqlite"},
			},
			want: "duplicate source adapter",
		},
		{
			name:    "missing target capability",
			targets: []targetRole{{engine: "sqlite"}},
			want:    "no capability declaration",
		},
		{
			name: "duplicate target",
			targets: []targetRole{
				{engine: "sqlite", capability: testTargetCapability},
				{engine: "sqlite", capability: testTargetCapability},
			},
			want: "duplicate target adapter",
		},
		{
			name:    "missing source role",
			targets: []targetRole{{engine: "sqlite", capability: testTargetCapability}},
			certified: []adapterPair{{
				source: "postgres",
				target: "sqlite",
			}},
			want: "unregistered source adapter",
		},
		{
			name:    "missing target role",
			sources: []sourceRole{{engine: "sqlite"}},
			certified: []adapterPair{{
				source: "sqlite",
				target: "postgres",
			}},
			want: "unregistered target adapter",
		},
		{
			name: "missing implementation",
			sources: []sourceRole{{
				engine: "postgres",
			}},
			targets: []targetRole{{
				engine:     "sqlite",
				capability: testTargetCapability,
			}},
			certified: []adapterPair{{
				source: "postgres",
				target: "sqlite",
			}},
			want: "neither composable adapters nor an override",
		},
		{
			name: "uncertified override",
			sources: []sourceRole{{
				engine: "postgres",
				open:   unusedSourceFactory,
			}},
			targets: []targetRole{{
				engine:     "sqlite",
				capability: testTargetCapability,
				open:       unusedTargetFactory,
			}},
			overrides: []adapterOverride{{
				pair: adapterPair{source: "postgres", target: "sqlite"},
				run:  noOpMigrationRunner,
			}},
			want: "override is not certified",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newAdapterRegistry(
				test.sources,
				test.targets,
				test.certified,
				test.overrides,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBuiltInAdaptersPreserveCertifiedRoutes(t *testing.T) {
	type routeExpectation struct {
		override migrationRunner
		source   sourceAdapterFactory
		target   targetAdapterFactory
	}
	expected := map[adapterPair]routeExpectation{
		{source: "sqlite", target: "sqlite"}: {
			override: SQLiteToSQLiteWithObserver,
		},
		{source: "sqlite", target: "postgres"}: {
			source: openSQLiteSourceAdapter,
			target: openPostgresTargetAdapter,
		},
		{source: "sqlite", target: "mysql"}: {
			override: SQLiteToMySQLWithObserver,
		},
		{source: "sqlite", target: "mssql"}: {
			override: SQLiteToSQLServerWithObserver,
		},
		{source: "sqlite", target: "clickhouse"}: {
			source: openSQLiteSourceAdapter,
			target: openClickHouseTargetAdapter,
		},
		{source: "postgres", target: "postgres"}: {
			source: openPostgresSourceAdapter,
			target: openPostgresTargetAdapter,
		},
		{source: "postgres", target: "sqlite"}: {
			source: openPostgresSourceAdapter,
			target: openSQLiteTargetAdapter,
		},
		{source: "postgres", target: "mysql"}: {
			source: openPostgresSourceAdapter,
			target: openMySQLTargetAdapter,
		},
		{source: "postgres", target: "mssql"}: {
			source: openPostgresSourceAdapter,
			target: openSQLServerTargetAdapter,
		},
		{source: "mysql", target: "postgres"}: {
			source: openMySQLSourceAdapter,
			target: openPostgresTargetAdapter,
		},
		{source: "mysql", target: "sqlite"}: {
			source: openMySQLSourceAdapter,
			target: openSQLiteTargetAdapter,
		},
		{source: "mysql", target: "mysql"}: {
			source: openMySQLSourceAdapter,
			target: openMySQLTargetAdapter,
		},
		{source: "mssql", target: "postgres"}: {
			source: openSQLServerSourceAdapter,
			target: openPostgresTargetAdapter,
		},
		{source: "mssql", target: "sqlite"}: {
			source: openSQLServerSourceAdapter,
			target: openSQLiteTargetAdapter,
		},
		{source: "mssql", target: "mysql"}: {
			source: openSQLServerSourceAdapter,
			target: openMySQLTargetAdapter,
		},
		{source: "mssql", target: "mssql"}: {
			source: openSQLServerSourceAdapter,
			target: openSQLServerTargetAdapter,
		},
		{source: "clickhouse", target: "clickhouse"}: {
			source: openClickHouseSourceAdapter,
			target: openClickHouseTargetAdapter,
		},
	}
	if got := len(builtInAdapters.certified); got != len(expected) {
		t.Fatalf("certified route count = %d, want %d", got, len(expected))
	}
	for pair, expectation := range expected {
		route, err := builtInAdapters.route(pair.source, pair.target)
		if err != nil {
			t.Errorf("route(%s, %s): %v", pair.source, pair.target, err)
			continue
		}
		if expectation.override == nil {
			if route.override != nil {
				t.Errorf(
					"route(%s, %s) unexpectedly uses a compatibility override",
					pair.source,
					pair.target,
				)
			}
			if reflect.ValueOf(route.source.open).Pointer() !=
				reflect.ValueOf(expectation.source).Pointer() {
				t.Errorf(
					"route(%s, %s) resolved the wrong source factory",
					pair.source,
					pair.target,
				)
			}
			if reflect.ValueOf(route.target.open).Pointer() !=
				reflect.ValueOf(expectation.target).Pointer() {
				t.Errorf(
					"route(%s, %s) resolved the wrong target factory",
					pair.source,
					pair.target,
				)
			}
			continue
		}
		if route.override == nil ||
			reflect.ValueOf(route.override).Pointer() !=
				reflect.ValueOf(expectation.override).Pointer() {
			t.Errorf(
				"route(%s, %s) resolved the wrong override",
				pair.source,
				pair.target,
			)
		}
	}
	if _, err := builtInAdapters.route("clickhouse", "postgres"); err == nil ||
		!strings.Contains(err.Error(), "clickhouse-to-postgres") {
		t.Fatalf("unsupported route error = %v", err)
	}
}

func TestAdapterRegistryDistinguishesMissingRoles(t *testing.T) {
	registry, err := newAdapterRegistry(
		[]sourceRole{{engine: "postgres"}},
		[]targetRole{{engine: "sqlite", capability: testTargetCapability}},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	if _, _, err := registry.roles("sqlite", "sqlite"); err == nil ||
		!strings.Contains(err.Error(), "source engine") {
		t.Fatalf("source-role error = %v", err)
	}
	if _, _, err := registry.roles("postgres", "postgres"); err == nil ||
		!strings.Contains(err.Error(), "target engine") {
		t.Fatalf("target-role error = %v", err)
	}
}

func TestCapabilityValidationPrecedesAdapterConstruction(t *testing.T) {
	sourceOpened, targetOpened := false, false
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "postgres",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				sourceOpened = true
				return nil, errors.New("source adapter should not open")
			},
		}},
		[]targetRole{{
			engine: "clickhouse",
			capability: engine.Capability{
				BulkPath:          "test batches",
				Upsert:            false,
				StrictConsistency: "unsupported",
			},
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				targetOpened = true
				return nil, errors.New("target adapter should not open")
			},
		}},
		[]adapterPair{{source: "postgres", target: "clickhouse"}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	_, err = executeWithRegistry(context.Background(), config.Config{
		Source: config.Endpoint{Type: "postgres"},
		Target: config.Endpoint{Type: "clickhouse"},
		Migration: config.Migration{
			TargetMode: "upsert",
		},
	}, nil, registry)
	if err == nil || !strings.Contains(err.Error(), "does not support upsert") {
		t.Fatalf("error = %v", err)
	}
	if sourceOpened || targetOpened {
		t.Fatalf(
			"adapters opened before capability rejection: source=%v target=%v",
			sourceOpened,
			targetOpened,
		)
	}
}

func TestUnsupportedStrictConsistencyPrecedesAdapterConstruction(t *testing.T) {
	sourceOpened, targetOpened := false, false
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "sqlite",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				sourceOpened = true
				return nil, errors.New("source adapter should not open")
			},
		}},
		[]targetRole{{
			engine: "clickhouse",
			capability: engine.Capability{
				BulkPath:          "test batches",
				StrictConsistency: "unsupported",
			},
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				targetOpened = true
				return nil, errors.New("target adapter should not open")
			},
		}},
		[]adapterPair{{source: "sqlite", target: "clickhouse"}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	_, err = executeWithRegistry(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite"},
		Target: config.Endpoint{Type: "clickhouse"},
		Migration: config.Migration{
			StrictConsistency: true,
		},
	}, nil, registry)
	if err == nil || !strings.Contains(
		err.Error(),
		"does not support strict consistency",
	) {
		t.Fatalf("error = %v", err)
	}
	if sourceOpened || targetOpened {
		t.Fatalf(
			"adapters opened before capability rejection: source=%v target=%v",
			sourceOpened,
			targetOpened,
		)
	}
}

func TestEndpointValidationPrecedesAdapterConstruction(t *testing.T) {
	sourceOpened, targetOpened := false, false
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "postgres",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				sourceOpened = true
				return nil, errors.New("source adapter should not open")
			},
		}},
		[]targetRole{{
			engine:     "sqlite",
			capability: testTargetCapability,
			validate:   validateSQLiteTargetEndpoint,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				targetOpened = true
				return nil, errors.New("target adapter should not open")
			},
		}},
		[]adapterPair{{source: "postgres", target: "sqlite"}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	_, err = executeWithRegistry(context.Background(), config.Config{
		Source: config.Endpoint{Type: "postgres"},
		Target: config.Endpoint{Type: "sqlite"},
	}, nil, registry)
	if err == nil ||
		!strings.Contains(err.Error(), "SQLite target database path is required") {
		t.Fatalf("error = %v", err)
	}
	if sourceOpened || targetOpened {
		t.Fatalf(
			"adapters opened before endpoint rejection: source=%v target=%v",
			sourceOpened,
			targetOpened,
		)
	}
}

func TestAdapterRegistrySupportsConcurrentReads(t *testing.T) {
	const readers = 32
	var group sync.WaitGroup
	errors := make(chan error, readers)
	for range readers {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := builtInAdapters.route("postgres", "sqlite")
			errors <- err
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("concurrent route lookup: %v", err)
		}
	}
}
