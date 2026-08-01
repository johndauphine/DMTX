package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestRunMigratesSQLiteConfiguration(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes (body) VALUES ('first')`); err != nil {
		t.Fatal(err)
	}
	source.Close()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"run", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if output.String() != "{\"tables\":1,\"rows\":1,\"validated\":true}\n" {
		t.Fatalf("result = %q", output.String())
	}

	output.Reset()
	if code := Run([]string{"status", "--state", configPath + ".state.db"}, &output, &errors); code != Success {
		t.Fatalf("status exit code = %d, stderr = %s", code, errors.String())
	}
	if !bytes.Contains(output.Bytes(), []byte("\"outcome\":\"success\"")) {
		t.Fatalf("status result = %q", output.String())
	}
}

func TestRunDryRunRejectsAbsentSQLiteTargetWithoutMutatingArtifacts(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (body) VALUES ('first')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"run", "--config", configPath, "--dry-run"}, &output, &errors); code != ConfigurationError {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	// Decode rather than substring-match: the plan is an extensible JSON
	// contract, and a literal match breaks on every additive disclosure without
	// telling you whether the fields this test cares about are still right.
	var plan migrate.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatalf("decode plan %q: %v", output.String(), err)
	}
	if len(plan.Tables) != 1 ||
		plan.Tables[0].Name != "notes" ||
		plan.Tables[0].Rows != 1 {
		t.Fatalf("plan tables = %#v", plan.Tables)
	}
	if plan.Proceed || plan.Target == nil ||
		plan.Target.Presence != migrate.PlannedTargetAbsent ||
		plan.Target.Preflight != migrate.PlannedTargetPreflightFailed ||
		plan.Target.Error == "" {
		t.Fatalf("target preflight = %#v", plan.Target)
	}
	if plan.Schema == nil ||
		plan.Schema.Status != migrate.PlannedSchemaBaselineAbsent ||
		plan.Schema.BlocksProceed {
		t.Fatalf("schema disclosure = %#v", plan.Schema)
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("dry run created target: %v", err)
	}
	if _, err := os.Stat(configPath + ".state.db"); !os.IsNotExist(err) {
		t.Fatalf("dry run created state: %v", err)
	}
}

func TestRunDryRunReadsDeleteDueStateWithoutCreatingState(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	statePath := filepath.Join(directory, "migration.state.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'first')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (2, 'stale')`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	beforeState := snapshotDryRunArtifactBytes(t, statePath)
	beforeTarget := snapshotDryRunArtifactBytes(t, targetPath)
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      schedule: interval\n      interval: 30m\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, stderr bytes.Buffer
	if code := Run(
		[]string{"run", "--config", configPath, "--state", statePath, "--dry-run"},
		&output,
		&stderr,
	); code != Success {
		t.Fatalf("exit code = %d, stderr = %s, plan = %s", code, stderr.String(), output.String())
	}
	var plan migrate.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatalf("decode plan %q: %v", output.String(), err)
	}
	if !plan.Proceed || plan.Admission == nil || !plan.Admission.Supported ||
		plan.Deletes == nil || !plan.Deletes.DueStateKnown || !plan.Deletes.Due ||
		len(plan.Deletes.Tables) != 1 {
		t.Fatalf("delete policy disclosure = %#v", plan)
	}
	deletePlan := plan.Deletes.Tables[0]
	if !deletePlan.Due ||
		deletePlan.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactExact ||
		deletePlan.CandidateCount == nil || *deletePlan.CandidateCount != 1 ||
		deletePlan.CandidateDigest == "" ||
		deletePlan.CandidateEqualityProofDigest == "" ||
		deletePlan.CandidateBatchCount == nil ||
		*deletePlan.CandidateBatchCount != 1 ||
		deletePlan.CandidateProvenance !=
			migrate.PlannedDeleteCandidateImpactPrimaryKeySetDifference {
		t.Fatalf("delete candidate disclosure = %#v", deletePlan)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("dry run created state: %v", err)
	}
	assertDryRunArtifactBytesEqual(t, "state", beforeState, snapshotDryRunArtifactBytes(t, statePath))
	assertDryRunArtifactBytesEqual(t, "target", beforeTarget, snapshotDryRunArtifactBytes(t, targetPath))
}

func TestRunDryRunDisclosesApplicableSchemaDriftWithoutWrites(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	statePath := filepath.Join(directory, "migration.state.db")
	configPath := filepath.Join(directory, "migration.yaml")
	for _, path := range []string{sourcePath, targetPath} {
		database, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
			database.Close()
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n  fail_on_schema_drift: true\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
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
	canonical, err := baseline.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := baseline.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := state.TaskKey{Type: "schema-contract", Table: "aggregate-source-schema"}
	taskJSON, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	record := state.SchemaSnapshot{
		RunID: "prior-run", Task: task, CanonicalJSON: string(canonical),
		Digest: digest, CapturedAt: time.Now().UTC().Add(-time.Hour),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	stateDB, err := sql.Open("sqlite", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateDB.Exec(`CREATE TABLE runs (id TEXT, source TEXT, target TEXT, source_engine TEXT, source_identity TEXT, target_identity TEXT, lease_target TEXT, lease_owner_token TEXT, lease_generation INTEGER, outcome TEXT, resumable BOOLEAN, reason TEXT, started_at TIMESTAMP, ended_at TIMESTAMP); CREATE TABLE stage4_records (kind TEXT, run_id TEXT, task_key TEXT, record_id TEXT, payload TEXT)`); err != nil {
		stateDB.Close()
		t.Fatal(err)
	}
	if _, err := stateDB.Exec(`INSERT INTO runs VALUES (?, '', '', 'sqlite', ?, ?, '', '', 0, 'success', 0, '', ?, NULL)`, "prior-run", sourceIdentity, targetIdentity, time.Now().UTC().Add(-time.Hour)); err != nil {
		stateDB.Close()
		t.Fatal(err)
	}
	if _, err := stateDB.Exec(`INSERT INTO stage4_records VALUES ('schema_snapshot', 'prior-run', ?, ?, ?)`, string(taskJSON), digest, string(payload)); err != nil {
		stateDB.Close()
		t.Fatal(err)
	}
	if err := stateDB.Close(); err != nil {
		t.Fatal(err)
	}
	beforeState, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	beforeTarget, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}

	var output, stderr bytes.Buffer
	if code := Run([]string{"run", "--config", configPath, "--state", statePath, "--dry-run"}, &output, &stderr); code != ConfigurationError {
		t.Fatalf("exit code = %d, stderr = %s, plan = %s", code, stderr.String(), output.String())
	}
	var plan migrate.Plan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatalf("decode plan %q: %v", output.String(), err)
	}
	if plan.Proceed || plan.Schema == nil ||
		plan.Schema.Status != migrate.PlannedSchemaChanged ||
		!plan.Schema.BlocksProceed || len(plan.Schema.Facts) == 0 ||
		len(plan.Schema.Decisions) == 0 || plan.Schema.Error == "" {
		t.Fatalf("schema drift plan = %#v", plan.Schema)
	}
	afterState, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	afterTarget, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeState, afterState) || !bytes.Equal(beforeTarget, afterTarget) {
		t.Fatal("dry-run schema disclosure mutated target or state")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal", ".lock"} {
		if _, err := os.Stat(statePath + suffix); !os.IsNotExist(err) {
			t.Fatalf("dry-run schema disclosure created state artifact %s: %v", suffix, err)
		}
	}
}
