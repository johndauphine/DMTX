package migrate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

// TestStage4DryRunRouteMatrixLive is the armed capability-representative proof
// that a dry run tells the truth about a real endpoint pair and changes nothing.
//
// The offline dry-run suite already covers the SQLite-local contracts — absent
// target non-proceed, shape mismatch, drift disclosure, tuning provenance —
// because those are decidable without a server. What it cannot cover is the
// network half: that discovery, counting, and target preflight actually succeed
// against verified-TLS PostgreSQL, MySQL, MariaDB, and SQL Server endpoints, and
// that reaching those endpoints leaves them untouched.
//
// Zero mutation is asserted from the target's own contents rather than from the
// absence of an error. Each fixture seeds the target with stale payloads and one
// target-only row, so a dry run that quietly performed the migration would flip
// the merged-row count from zero to two and be caught here.
func TestStage4DryRunRouteMatrixLive(t *testing.T) {
	cells := []struct {
		name  string
		setup func(*testing.T) stage4UpsertProcessKillFixture
	}{
		{name: "postgres", setup: func(t *testing.T) stage4UpsertProcessKillFixture {
			return stage4UpsertProcessKillFromDeleteFixture(
				stage4DeleteProcessKillPostgresFixture(t),
			)
		}},
		{name: "mysql80", setup: func(t *testing.T) stage4UpsertProcessKillFixture {
			return stage4UpsertProcessKillFromDeleteFixture(
				stage4DeleteProcessKillMySQLFixture(t, "mysql80"),
			)
		}},
		{name: "mariadb1011", setup: func(t *testing.T) stage4UpsertProcessKillFixture {
			return stage4UpsertProcessKillFromDeleteFixture(
				stage4DeleteProcessKillMySQLFixture(t, "mariadb1011"),
			)
		}},
		{name: "mssql", setup: func(t *testing.T) stage4UpsertProcessKillFixture {
			return stage4UpsertProcessKillFromDeleteFixture(
				stage4DeleteProcessKillSQLServerFixture(t),
			)
		}},
	}
	for _, cell := range cells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			fixture := cell.setup(t)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancel()

			plan, err := DryRun(ctx, fixture.cfg)
			if err != nil {
				t.Fatalf(
					"%s dry run failed [class=%s]",
					fixture.cell,
					ClassifyTransferError(err),
				)
			}

			if len(plan.Tables) != 1 {
				t.Fatalf("%s dry run tables = %d, want 1", fixture.cell, len(plan.Tables))
			}
			// An exact count, labelled exact. A future cheaper path that began
			// estimating through this field would change an operator's read of
			// the number without changing the number's name.
			if plan.Tables[0].Rows != 2 {
				t.Fatalf(
					"%s dry run row count = %d, want the 2 seeded source rows",
					fixture.cell,
					plan.Tables[0].Rows,
				)
			}
			if plan.Tables[0].RowsProvenance != RowCountExact {
				t.Fatalf(
					"%s dry run row provenance = %q",
					fixture.cell,
					plan.Tables[0].RowsProvenance,
				)
			}

			// Target preflight must have actually run against the live target.
			// "skipped" would mean the report promised nothing about the target
			// while still looking complete.
			if plan.Target == nil {
				t.Fatalf("%s dry run disclosed no target preflight", fixture.cell)
			}
			if plan.Target.Preflight != PlannedTargetPreflightPassed {
				t.Fatalf(
					"%s dry run target preflight = %q error=%q limitations=%v",
					fixture.cell,
					plan.Target.Preflight,
					plan.Target.Error,
					plan.Target.Limitations,
				)
			}

			if plan.Tuning == nil {
				t.Fatalf("%s dry run disclosed no tuning", fixture.cell)
			}
			if plan.Deletes == nil {
				t.Fatalf("%s dry run disclosed no delete policy", fixture.cell)
			}

			// Zero mutation, read from the target itself.
			merged, err := fixture.targetSourceRows(ctx)
			if err != nil {
				t.Fatalf("read %s target after dry run: %T", fixture.cell, err)
			}
			if merged != 0 {
				t.Fatalf(
					"%s dry run migrated %d rows into the target; a dry run must write nothing",
					fixture.cell,
					merged,
				)
			}
			retained, err := fixture.targetOnlyRows(ctx)
			if err != nil {
				t.Fatalf("read %s target-only rows after dry run: %T", fixture.cell, err)
			}
			if retained != 1 {
				t.Fatalf(
					"%s dry run disturbed the target-only row: count = %d",
					fixture.cell,
					retained,
				)
			}
		})
	}
}

// TestStage4DryRunRefusesUncertifiedRouteBeforeReachingEndpointsLive proves the
// configuration-only refusal ordering against real credentials: a route that
// Stage 4 does not certify must be refused from configuration alone, before the
// dry run contacts either endpoint.
//
// The endpoints here are live and reachable, so a refusal that arrived after
// contact would still look like a refusal in the report. The proof that it came
// first is that the target is untouched and the error is a policy class rather
// than a connection failure.
func TestStage4DryRunRefusesUncertifiedRouteBeforeReachingEndpointsLive(t *testing.T) {
	fixture := stage4UpsertProcessKillFromDeleteFixture(
		stage4DeleteProcessKillPostgresFixture(t),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := fixture.cfg
	// Strict consistency with delete reconciliation is deliberately uncertified.
	cfg.Migration.StrictConsistency = true
	cfg.Migration.Deletes = config.DeletePolicy{
		Mode:           config.DeleteModeReconcile,
		TargetBehavior: config.DeleteTargetHard,
		Reconcile: config.DeleteReconcilePolicy{
			Schedule:          config.DeleteScheduleInterval,
			Interval:          time.Hour,
			BatchSize:         1,
			RequirePrimaryKey: true,
		},
	}

	// A configuration refusal is reported as a structured non-proceed rather
	// than returned as an error: the operator gets a plan that says why it
	// cannot run, which is the whole point of asking. Asserting on the error
	// alone would pass for a dry run that happily admitted the route.
	plan, err := DryRun(ctx, cfg)
	if err != nil {
		t.Fatalf(
			"dry run errored instead of reporting non-proceed [class=%s]",
			ClassifyTransferError(err),
		)
	}
	if plan.Proceed {
		t.Fatal("dry run proceeded on an uncertified strict delete route")
	}
	if plan.Admission == nil || plan.Admission.Supported {
		t.Fatalf("uncertified route admission = %#v", plan.Admission)
	}
	// The refusal must be the real reason, not an incidental failure that
	// happens to stop the run.
	if !strings.Contains(plan.Admission.Error, "strict") {
		t.Fatalf("uncertified route admission error = %q", plan.Admission.Error)
	}
	// Nothing may have been contacted or created to decide this.
	if plan.Target != nil {
		t.Fatalf(
			"configuration refusal still ran a target preflight: %#v",
			plan.Target,
		)
	}

	merged, err := fixture.targetSourceRows(ctx)
	if err != nil {
		t.Fatalf("read target after refused dry run: %T", err)
	}
	if merged != 0 {
		t.Fatalf("refused dry run still wrote %d rows to the target", merged)
	}
}
