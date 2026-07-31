package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
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

// TestStage4DryRunDisclosesTuningAndDeletePolicy proves a dry run reports the
// resource plan it would actually run under, with provenance, and the
// configured delete policy — without opening the target, state, lease, or audit
// log.
func TestStage4DryRunDisclosesTuningAndDeletePolicy(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	writeDryRunDisclosureSource(t, sourcePath)

	cfg, err := config.Parse([]byte(
		"source:\n  type: sqlite\n  database: " + sourcePath +
			"\ntarget:\n  type: sqlite\n  database: " + targetPath +
			"\nmigration:\n  target_mode: upsert\n  connection_limit: 8\n" +
			"  workers: 6\n  reader_parallelism: 3\n  writer_parallelism: 2\n" +
			"  deletes:\n    mode: reconcile\n    reconcile:\n" +
			"      schedule: interval\n      interval: 30m\n" +
			"      require_primary_key: true\n",
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

	if plan.Deletes == nil {
		t.Fatal("dry run disclosed no delete policy")
	}
	if plan.Deletes.Mode != string(config.DeleteModeReconcile) ||
		plan.Deletes.Schedule != string(config.DeleteScheduleInterval) ||
		plan.Deletes.IntervalSeconds != int64(30*time.Minute/time.Second) ||
		!plan.Deletes.RequirePrimaryKey {
		t.Fatalf("delete disclosure = %#v", plan.Deletes)
	}
	// Due-ness depends on durable last-success evidence, which a dry run must
	// not read. Claiming it would be worse than omitting it.
	if plan.Deletes.DueStateKnown {
		t.Fatal("dry run claimed delete due-state without opening state")
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
