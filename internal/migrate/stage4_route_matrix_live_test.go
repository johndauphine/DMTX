package migrate

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

// stage4MatrixEngines is every relational engine the Stage 4 route matrices
// enumerate. ClickHouse is included deliberately: its refusals are part of the
// certified boundary, not an omission from it.
var stage4MatrixEngines = []string{
	"postgres",
	"mysql",
	"mariadb",
	"mssql",
	"sqlite",
	"clickhouse",
}

func stage4MatrixEndpoint(engine string, role string) config.Endpoint {
	if engine == "sqlite" {
		return config.Endpoint{
			Type:     "sqlite",
			Database: "/tmp/dmtx-stage4-matrix-" + role + ".db",
		}
	}
	return config.Endpoint{
		Type:     engine,
		Host:     role + ".matrix.invalid",
		Database: "matrix_" + role,
		User:     "matrix",
		Password: "matrix",
		Schema:   "public",
	}
}

// TestStage4CertifiedRelationalDeleteRouteMatrixLive enumerates every relational
// source/target pair and pins which cells delete reconciliation is certified
// for. Exactly one cell is certified today — PostgreSQL to PostgreSQL upsert —
// and the value of the matrix is that every other cell is proven to refuse
// before any target mutation, rather than being merely undocumented.
//
// The refusals are decided by configuration and route admission, so they are
// asserted without contacting a server; the certified cell's live behaviour is
// proven by TestStage4PostgresDeleteCompositionLiveTLS and its crash-resume
// companion.
func TestStage4CertifiedRelationalDeleteRouteMatrixLive(t *testing.T) {
	for _, source := range stage4MatrixEngines {
		for _, target := range stage4MatrixEngines {
			name := source + "_to_" + target
			t.Run(name, func(t *testing.T) {
				cfg := config.Config{
					Source: stage4MatrixEndpoint(source, "source"),
					Target: stage4MatrixEndpoint(target, "target"),
					Migration: config.Migration{
						TargetMode: "upsert",
						Deletes: config.DeletePolicy{
							Mode: config.DeleteModeReconcile,
							Reconcile: config.DeleteReconcilePolicy{
								Schedule:  config.DeleteScheduleInterval,
								Interval:  time.Hour,
								BatchSize: 100,
							},
						},
						Validation: config.ValidationPolicy{
							Mode: config.ValidationCountOnly,
						},
					},
				}
				certified := source == "postgres" && target == "postgres"
				err := requireStage4AdapterConfigurationSeams(cfg)
				if certified {
					if err != nil {
						t.Fatalf(
							"certified delete cell was refused: %v",
							err,
						)
					}
					return
				}
				if err == nil {
					t.Fatal("uncertified delete cell was admitted")
				}
				if ClassifyTransferError(err) != ErrorClassPolicy {
					t.Fatalf(
						"uncertified delete refusal class = %q: %v",
						ClassifyTransferError(err),
						err,
					)
				}
				if !strings.Contains(
					err.Error(),
					"certified only for PostgreSQL-to-PostgreSQL",
				) {
					t.Fatalf("uncertified delete refusal = %v", err)
				}
			})
		}
	}
}

// TestStage4CertifiedRelationalDeleteRejectsUncertifiedModes pins the other two
// edges of the same boundary: delete reconciliation is upsert-only, and it is
// not yet certified inside a strict snapshot epoch. Both refusals must arrive
// as policy before any target work.
func TestStage4CertifiedRelationalDeleteRejectsUncertifiedModes(t *testing.T) {
	base := func() config.Config {
		return config.Config{
			Source: stage4MatrixEndpoint("postgres", "source"),
			Target: stage4MatrixEndpoint("postgres", "target"),
			Migration: config.Migration{
				TargetMode: "upsert",
				Deletes: config.DeletePolicy{
					Mode: config.DeleteModeReconcile,
					Reconcile: config.DeleteReconcilePolicy{
						Schedule:  config.DeleteScheduleInterval,
						Interval:  time.Hour,
						BatchSize: 100,
					},
				},
			},
		}
	}
	for name, test := range map[string]struct {
		mutate func(*config.Config)
		want   string
	}{
		"rebuild mode": {
			mutate: func(cfg *config.Config) {
				cfg.Migration.TargetMode = "drop_recreate"
			},
			want: "requires target mode upsert",
		},
		"strict epoch": {
			mutate: func(cfg *config.Config) {
				cfg.Migration.StrictConsistency = true
			},
			want: "not yet certified inside one strict snapshot epoch",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			test.mutate(&cfg)
			err := requireStage4AdapterConfigurationSeams(cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("refusal = %v, want %q", err, test.want)
			}
			if ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf(
					"refusal class = %q",
					ClassifyTransferError(err),
				)
			}
		})
	}
}
