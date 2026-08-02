package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	_ "modernc.org/sqlite"
)

func TestDryRunReportsComposedStage4PolicyBeforeTargetPreflight(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	database, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, updated_at TEXT); INSERT INTO items VALUES (1, '2026-01-01')`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	for name, test := range map[string]struct {
		migration                 string
		rejected                  bool
		expectAbsentTargetFailure bool
	}{
		"delete": {
			migration: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      schedule: interval\n      interval: 1h\n",
		},
		"incremental": {
			migration: "target_mode: upsert\n  date_updated_columns: [updated_at]\n",
		},
		"strict": {
			migration: "target_mode: upsert\n  strict_consistency: true\n  strict_consistency_scope: migration\n",
			rejected:  true,
		},
		"runtime": {
			// Ordinary deferred network work now owns a bounded controller.
			// Configuration admission must therefore reach source discovery;
			// the absent SQLite target still prevents Proceed because dry-run
			// cannot perform the required target preflight without creating it.
			migration:                 "runtime_tuning: true\n",
			expectAbsentTargetFailure: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, parseErr := config.Parse([]byte(
				"source:\n  type: sqlite\n  database: " + sourcePath +
					"\ntarget:\n  type: sqlite\n  database: " + targetPath +
					"\nmigration:\n  " + test.migration,
			))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			opened := false
			plan, dryErr := dryRunWithDiscovery(
				context.Background(),
				cfg,
				func(context.Context, config.Config) (Plan, error) {
					opened = true
					return Plan{}, nil
				},
			)
			if dryErr != nil {
				t.Fatalf("DryRun: %v", dryErr)
			}
			if test.rejected {
				if plan.Proceed || plan.Admission == nil || plan.Admission.Supported || plan.Admission.Error == "" {
					t.Fatalf("dry-run policy plan = %#v", plan)
				}
				if opened || len(plan.Tables) != 0 {
					t.Fatalf("unsupported policy reached source discovery: opened=%t plan=%#v", opened, plan)
				}
				if plan.Target != nil {
					t.Fatalf("unsupported policy opened target preflight: %#v", plan.Target)
				}
			} else if !opened || plan.Admission == nil || !plan.Admission.Supported || plan.Admission.Error != "" {
				t.Fatalf("admitted policy did not reach dry-run discovery: opened=%t plan=%#v", opened, plan)
			}
			if test.expectAbsentTargetFailure {
				if plan.Proceed || plan.Target == nil ||
					plan.Target.Presence != PlannedTargetAbsent ||
					plan.Target.Preflight != PlannedTargetPreflightFailed ||
					plan.Target.Error == "" {
					t.Fatalf("runtime tuning dry-run target preflight = %#v plan=%#v", plan.Target, plan)
				}
			}
			if _, statErr := os.Stat(targetPath); !os.IsNotExist(statErr) {
				t.Fatalf("dry-run created target: %v", statErr)
			}
		})
	}
}

func TestValidateStage4ComposedConfigurationMatchesCertifiedRoutePolicy(t *testing.T) {
	for name, test := range map[string]struct {
		cfg      config.Config
		rejected bool
	}{
		"sqlite incremental": {
			cfg: config.Config{Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "sqlite"}, Migration: config.Migration{TargetMode: "upsert", DateUpdatedColumns: []string{"updated_at"}}},
		},
		"sqlite strict": {
			cfg: config.Config{Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "sqlite"}, Migration: config.Migration{TargetMode: "upsert", StrictConsistency: true}},
		},
		"sqlite strict migration scope": {
			cfg:      config.Config{Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "sqlite"}, Migration: config.Migration{TargetMode: "upsert", StrictConsistency: true, StrictConsistencyScope: "migration"}},
			rejected: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateStage4ComposedConfiguration(test.cfg)
			if test.rejected {
				if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
					(!strings.Contains(err.Error(), "no certified source composition") &&
						!strings.Contains(err.Error(), "table scope only")) {
					t.Fatalf("configuration admission error = %v", err)
				}
			} else if err != nil {
				t.Fatalf("configuration admission rejected supported route: %v", err)
			}
		})
	}
}
