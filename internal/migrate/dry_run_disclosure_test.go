package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func writeDryRunDisclosureSource(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(
		`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT);
		 INSERT INTO items (id, payload) VALUES (1, 'a'), (2, 'b');`,
	); err != nil {
		t.Fatal(err)
	}
}

// TestStage4DryRunDisclosesTuningAndDeletePolicy proves a supported dry run
// reports the resource plan it would actually run under, with provenance. The
// absent SQLite upsert target is inspected by
// path only and rejected without creating it.
func TestStage4DryRunDisclosesTuningAndDeletePolicy(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	writeDryRunDisclosureSource(t, sourcePath)

	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " + targetPath +
			"\nmigration:\n  target_mode: upsert\n  connection_limit: 8\n" +
			"  workers: 6\n  reader_parallelism: 3\n  writer_parallelism: 2\n",
	))
	if err != nil {
		t.Fatal(err)
	}

	plan, err := DryRun(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Tuning == nil {
		t.Fatal("dry run disclosed no tuning")
	}
	// Pinned values must be reported as requested, not silently as derived; the
	// whole point of the disclosure is telling operator intent from inference.
	if plan.Tuning.ConnectionLimit.Value != 8 ||
		plan.Tuning.ConnectionLimit.Provenance !=
			string(config.ProvenanceRequested) {
		t.Fatalf("connection limit disclosure = %#v", plan.Tuning.ConnectionLimit)
	}
	if plan.Tuning.Readers.Value != 3 ||
		plan.Tuning.Readers.Provenance != string(config.ProvenanceRequested) {
		t.Fatalf("readers disclosure = %#v", plan.Tuning.Readers)
	}
	if plan.Tuning.Writers.Value != 2 {
		t.Fatalf("writers disclosure = %#v", plan.Tuning.Writers)
	}
	if plan.Tuning.MemoryBudget.Value <= 0 ||
		plan.Tuning.MemoryBudget.Provenance == "" {
		t.Fatalf("memory budget disclosure = %#v", plan.Tuning.MemoryBudget)
	}
	if plan.Tuning.ChunkRows.Value <= 0 || plan.Tuning.QueueDepth.Value <= 0 {
		t.Fatalf(
			"incomplete tuning disclosure: chunk=%#v queue=%#v",
			plan.Tuning.ChunkRows,
			plan.Tuning.QueueDepth,
		)
	}

	// Every source path issues an exact COUNT(*), so nothing here may be
	// labelled an estimate. A duration estimate is deliberately absent: there is
	// no throughput evidence to derive one from, and inventing one would be the
	// exact failure this label guards against.
	if len(plan.Tables) != 1 || plan.Tables[0].Rows != 2 {
		t.Fatalf("plan tables = %#v", plan.Tables)
	}
	if plan.Tables[0].RowsProvenance != RowCountExact {
		t.Fatalf("row count provenance = %q", plan.Tables[0].RowsProvenance)
	}

	// The pagination strategy is the fact that tells an operator how the table
	// will actually be read; a topology hash makes two dry runs comparable.
	pagination := plan.Tables[0].Pagination
	if pagination == nil {
		t.Fatal("dry run disclosed no pagination strategy")
	}
	if pagination.Strategy == "" ||
		pagination.TopologyHash == "" ||
		pagination.Partitions < 1 {
		t.Fatalf("pagination disclosure = %#v", pagination)
	}
	if len(pagination.Keys) == 0 || pagination.Keys[0] != "id" {
		t.Fatalf("pagination keys = %#v", pagination.Keys)
	}

	if plan.Deletes == nil || plan.Deletes.Mode != string(config.DeleteModeOff) {
		t.Fatalf("delete disclosure = %#v", plan.Deletes)
	}
	if plan.Proceed {
		t.Fatal("dry run allowed upsert without an existing target")
	}
	if plan.Admission == nil || !plan.Admission.Supported ||
		plan.Admission.Error != "" {
		t.Fatalf("policy admission = %#v", plan.Admission)
	}
	if plan.Target == nil || plan.Target.Presence != PlannedTargetAbsent ||
		plan.Target.Preflight != PlannedTargetPreflightFailed {
		t.Fatalf("target preflight = %#v", plan.Target)
	}

	// The disclosure must not have created the target or any state artifact.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("dry run created the target: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "source.db" {
			t.Fatalf("dry run created %q", entry.Name())
		}
	}
}

func TestStage4DryRunRejectsAbsentSQLiteDropRecreateWithoutArtifacts(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	writeDryRunDisclosureSource(t, sourcePath)
	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DryRun(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Proceed {
		t.Fatalf("drop/recreate dry run proceeded without target preflight: %#v", plan)
	}
	if plan.Target == nil ||
		plan.Target.Presence != PlannedTargetAbsent ||
		plan.Target.Preflight != PlannedTargetPreflightFailed ||
		plan.Target.Error == "" ||
		len(plan.Target.Limitations) == 0 {
		t.Fatalf("target disclosure = %#v", plan.Target)
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("drop/recreate dry run created target: %v", err)
	}
}

func TestStage4DryRunRunsSQLiteTargetPreflightForExistingUpsert(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	writeDryRunDisclosureSource(t, sourcePath)
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT);`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " + targetPath +
			"\nmigration:\n  target_mode: upsert\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DryRun(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Proceed || plan.Target == nil ||
		plan.Target.Presence != PlannedTargetPresent ||
		plan.Target.Preflight != PlannedTargetPreflightPassed {
		t.Fatalf("existing target preflight = %#v", plan.Target)
	}
}

func TestStage4DryRunRejectsSQLiteUpsertTargetShapeMismatch(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	writeDryRunDisclosureSource(t, sourcePath)
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`CREATE TABLE items (id TEXT PRIMARY KEY, payload TEXT);`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " + targetPath +
			"\nmigration:\n  target_mode: upsert\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DryRun(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Proceed || plan.Target == nil ||
		plan.Target.Presence != PlannedTargetPresent ||
		plan.Target.Preflight != PlannedTargetPreflightFailed {
		t.Fatalf("mismatched target preflight = %#v", plan.Target)
	}
}

func TestApplyDryRunSchemaDriftDisclosesFactsAndPolicy(t *testing.T) {
	baseline, err := schema.NewSchemaSnapshot([]schema.Table{{
		Name: "items",
		Columns: []schema.Column{{
			Name: "id", Type: "integer",
			DeclaredType: &schema.DeclaredType{Base: "integer"},
			PrimaryKey:   true, PrimaryKeyPosition: 1,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	current, err := schema.NewSchemaSnapshot([]schema.Table{{
		Name: "items",
		Columns: []schema.Column{
			{Name: "id", Type: "integer", DeclaredType: &schema.DeclaredType{Base: "integer"}, PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "payload", Type: "text", DeclaredType: &schema.DeclaredType{Base: "text"}, Nullable: true},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := baseline.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := baseline.Digest()
	if err != nil {
		t.Fatal(err)
	}
	baseConfig := config.Config{Migration: config.Migration{TargetMode: "drop_recreate"}}
	for name, mutation := range map[string]func(*config.Config){
		"report only":   func(*config.Config) {},
		"fail on drift": func(cfg *config.Config) { cfg.Migration.FailOnSchemaDrift = true },
		"freeze contract": func(cfg *config.Config) {
			cfg.Migration.SchemaContract = &config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractFreeze,
				DataType: config.SchemaContractReport,
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := baseConfig
			mutation(&cfg)
			plan := Plan{
				Proceed: true, Admission: &PlannedAdmission{Supported: true},
				currentSchema: current, currentSchemaReady: true,
			}
			ApplyDryRunSchemaDrift(&plan, cfg, DryRunSchemaBaseline{
				Found: true, CanonicalJSON: string(canonical), Digest: digest,
			})
			if plan.Schema == nil || plan.Schema.Status != PlannedSchemaChanged ||
				len(plan.Schema.Facts) == 0 ||
				len(plan.Schema.Facts) != len(plan.Schema.Decisions) {
				t.Fatalf("schema drift disclosure = %#v", plan.Schema)
			}
			columnAdded := false
			for _, fact := range plan.Schema.Facts {
				if fact.Object.Table == "items" && fact.Object.Column == "payload" &&
					fact.ChangeKind == schema.SchemaDriftColumnAdded {
					columnAdded = true
				}
			}
			if !columnAdded {
				t.Fatalf("schema drift omitted added column fact: %#v", plan.Schema.Facts)
			}
			switch name {
			case "report only":
				if plan.Schema.BlocksProceed || !plan.Proceed {
					t.Fatalf("report policy = %#v", plan.Schema)
				}
				for _, decision := range plan.Schema.Decisions {
					if decision.Action != SchemaContractReport {
						t.Fatalf("report decision = %#v", decision)
					}
				}
			default:
				if !plan.Schema.BlocksProceed || plan.Proceed || plan.Schema.Error == "" {
					t.Fatalf("blocking policy = %#v", plan.Schema)
				}
				for _, decision := range plan.Schema.Decisions {
					if decision.Action != SchemaContractAbort {
						t.Fatalf("blocking decision = %#v", decision)
					}
				}
			}
		})
	}

	t.Run("baseline absent", func(t *testing.T) {
		plan := Plan{
			Proceed: true, Admission: &PlannedAdmission{Supported: true},
			currentSchema: current, currentSchemaReady: true,
		}
		ApplyDryRunSchemaDrift(&plan, baseConfig, DryRunSchemaBaseline{})
		if plan.Schema == nil || plan.Schema.Status != PlannedSchemaBaselineAbsent ||
			plan.Schema.BlocksProceed || !plan.Proceed || plan.Schema.CurrentDigest == "" {
			t.Fatalf("baseline-absent schema disclosure = %#v", plan.Schema)
		}
	})

	t.Run("unchanged", func(t *testing.T) {
		plan := Plan{
			Proceed: true, Admission: &PlannedAdmission{Supported: true},
			currentSchema: baseline, currentSchemaReady: true,
		}
		ApplyDryRunSchemaDrift(&plan, baseConfig, DryRunSchemaBaseline{
			Found: true, CanonicalJSON: string(canonical), Digest: digest,
		})
		if plan.Schema == nil || plan.Schema.Status != PlannedSchemaUnchanged ||
			plan.Schema.BlocksProceed || !plan.Proceed ||
			len(plan.Schema.Facts) != 0 || len(plan.Schema.Decisions) != 0 {
			t.Fatalf("unchanged schema disclosure = %#v", plan.Schema)
		}
	})
}

// TestStage4DryRunTuningMatchesResolvedPlan pins the disclosure to the same
// resolver the migration uses, so the reported numbers cannot drift into a
// parallel estimate that is merely plausible.
func TestStage4DryRunTuningMatchesResolvedPlan(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	writeDryRunDisclosureSource(t, sourcePath)

	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " +
			filepath.Join(directory, "target.db") + "\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DryRun(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := config.ResolveSystemEffectiveTransferPlan(
		context.Background(),
		cfg.Migration,
		config.TransferPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Tuning.ConnectionLimit.Value != int64(resolved.ConnectionLimit.Value) ||
		plan.Tuning.Workers.Value != int64(resolved.Workers.Value) ||
		plan.Tuning.Readers.Value != int64(resolved.Readers.Value) ||
		plan.Tuning.Writers.Value != int64(resolved.Writers.Value) ||
		plan.Tuning.ChunkRows.Value != int64(resolved.ChunkRows.Value) ||
		plan.Tuning.QueueDepth.Value != int64(resolved.QueueDepth.Value) {
		t.Fatalf(
			"concurrency disclosure drifted from the resolver: disclosed=%#v resolved=%#v",
			plan.Tuning,
			resolved,
		)
	}
	// The memory budget is a point-in-time reading of host-available memory, so
	// two resolutions legitimately differ by whatever the host freed or consumed
	// between them. Only its provenance and positivity are stable facts.
	if plan.Tuning.MemoryBudget.Provenance !=
		string(resolved.MemoryBudget.Provenance) ||
		plan.Tuning.MemoryBudget.Value <= 0 {
		t.Fatalf(
			"memory budget disclosure = %#v, resolver = %#v",
			plan.Tuning.MemoryBudget,
			resolved.MemoryBudget,
		)
	}
}
