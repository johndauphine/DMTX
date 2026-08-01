package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestDryRunDeleteCandidateSQLiteCapabilityUsesProductionAuthority(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	for path, statement := range map[string]string{
		sourcePath: `CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'source')`,
		targetPath: `CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'source'), (2, 'stale')`,
	} {
		database, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(statement); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      schedule: interval\n      interval: 1h\n"
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	route, err := resolveMigration(cfg, builtInAdapters)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source, err := route.source.open(ctx, cfg.Source)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := route.target.open(ctx, cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	sourceTable, err := source.InspectTable(ctx, "notes")
	if err != nil {
		t.Fatal(err)
	}
	targetTables, err := target.PlanTables(source.Engine(), []schema.Table{sourceTable}, cfg.Migration.TargetMode)
	if err != nil {
		t.Fatal(err)
	}
	if len(targetTables) != 1 {
		t.Fatalf("target tables = %#v", targetTables)
	}
	if err := target.PreflightTables(ctx, targetTables, cfg.Migration.TargetMode); err != nil {
		t.Fatalf("target preflight: %v", err)
	}
	if _, err := newStage4DeleteReconciliationCapabilities(ctx, source, target, sourceTable, targetTables[0]); err != nil {
		t.Fatalf("production delete capability: %v\nsource=%#v\ntarget=%#v", err, sourceTable, targetTables[0])
	}
	if impact, err := inspectDryRunDeleteCandidateImpact(
		ctx,
		cfg,
		source,
		target,
		sourceTable,
		targetTables[0],
		PlannedDeleteTable{Table: "notes", DueStateKnown: true, Due: true},
	); err != nil || impact.candidates != 1 {
		t.Fatalf("inspect dry-run candidate impact = %#v, %v", impact, err)
	}
	plan := Plan{
		Proceed: true,
		Tables:  []PlannedTable{{Name: "notes"}},
		Target:  &PlannedTarget{Preflight: PlannedTargetPreflightPassed},
		Deletes: &PlannedDelete{DueStateKnown: true, Tables: []PlannedDeleteTable{{Table: "notes", DueStateKnown: true, Due: true}}},
	}
	ApplyDryRunDeleteCandidateImpact(ctx, cfg, &plan)
	if !plan.Proceed || len(plan.Deletes.Tables) != 1 ||
		plan.Deletes.Tables[0].CandidateImpactStatus != PlannedDeleteCandidateImpactExact ||
		plan.Deletes.Tables[0].CandidateCount == nil ||
		*plan.Deletes.Tables[0].CandidateCount != 1 {
		t.Fatalf("dry-run candidate plan = %#v table=%#v", plan, plan.Deletes.Tables[0])
	}
}
