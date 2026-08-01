package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

// TestStage4StrictStableViewCompositionMatrix pins the configuration-only
// boundary to the strict primitives that actually own a stable source view.
// It deliberately reaches no endpoint: a disallowed source/scope/target must
// be refused before discovery, checkpointing, or target preflight.
func TestStage4StrictStableViewCompositionMatrix(t *testing.T) {
	targets := []string{"postgres", "mysql", "mssql", "sqlite"}
	for _, source := range []string{"postgres", "mssql"} {
		for _, scope := range []string{
			config.StrictConsistencyTable,
			config.StrictConsistencyMigration,
		} {
			for _, target := range targets {
				t.Run(source+"_"+scope+"_"+target, func(t *testing.T) {
					cfg := strictMatrixConfig(source, target, scope)
					if err := ValidateMigration(cfg); err != nil {
						t.Fatalf("ValidateMigration rejected certified route: %v", err)
					}
					if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
						t.Fatalf("configuration-only gate rejected certified route: %v", err)
					}
				})
			}
		}
	}

	// MySQL-family and SQLite sources own only a one-table stable-view
	// contract, but that contract composes to the same already-certified
	// keyed-upsert target set. The registry canonicalizes MariaDB endpoints to
	// the mysql engine; keep the matrix on that canonical engine label rather
	// than multiplying the live source-contract sentinels by every target
	// writer.
	for _, source := range []string{"mysql", "sqlite"} {
		for _, target := range targets {
			t.Run(source+"_table_"+target, func(t *testing.T) {
				cfg := strictMatrixConfig(
					source,
					target,
					config.StrictConsistencyTable,
				)
				if err := ValidateMigration(cfg); err != nil {
					t.Fatalf("ValidateMigration rejected certified table route: %v", err)
				}
				if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
					t.Fatalf("configuration-only gate rejected certified table route: %v", err)
				}
			})
		}
	}

	for _, source := range []string{"mysql", "sqlite"} {
		t.Run(source+"_migration_refused", func(t *testing.T) {
			err := ValidateMigration(
				strictMatrixConfig(
					source,
					"postgres",
					config.StrictConsistencyMigration,
				),
			)
			if err == nil || !strings.Contains(err.Error(), "table scope only") {
				t.Fatalf("migration-scope policy error = %v", err)
			}
		})
	}
	for _, scope := range []string{
		config.StrictConsistencyTable,
		config.StrictConsistencyMigration,
	} {
		t.Run("clickhouse_"+scope+"_refused", func(t *testing.T) {
			err := ValidateMigration(
				strictMatrixConfig("clickhouse", "clickhouse", scope),
			)
			if err == nil || !strings.Contains(err.Error(), "strict consistency") {
				t.Fatalf("ClickHouse policy error = %v", err)
			}
		})
	}
}

func strictMatrixConfig(source, target, scope string) config.Config {
	return config.Config{
		Source: strictConsistencyTestEndpoint(source, "source"),
		Target: strictConsistencyTestEndpoint(target, "target"),
		Migration: config.Migration{
			TargetMode:             "upsert",
			StrictConsistency:      true,
			StrictConsistencyScope: scope,
		},
	}
}
