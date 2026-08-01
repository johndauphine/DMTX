package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestRunDryRunDeleteCandidatesUseReadOnlyStateAndPreserveArtifacts(t *testing.T) {
	for _, test := range []struct {
		name string
		ext  string
		due  bool
	}{
		{name: "yaml_due_zero", ext: ".yaml", due: true},
		{name: "sqlite_due_zero", ext: ".db", due: true},
		{name: "yaml_not_due", ext: ".yaml", due: false},
		{name: "sqlite_not_due", ext: ".db", due: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			statePath := filepath.Join(directory, "migration.state"+test.ext)
			configPath := filepath.Join(directory, "migration.yaml")
			createDryRunDeleteCandidateSQLiteDatabases(t, sourcePath, targetPath, !test.due)
			cfg := writeDryRunDeleteCandidateConfiguration(
				t,
				configPath,
				sourcePath,
				targetPath,
			)
			completedAt := time.Now().UTC().Add(-time.Minute)
			if test.due {
				completedAt = completedAt.Add(-2 * time.Hour)
			}
			seedDryRunDeleteReconciliationEvidence(
				t,
				statePath,
				cfg,
				completedAt,
			)
			beforeState := snapshotDryRunArtifactBytes(t, statePath)
			beforeTarget := snapshotDryRunArtifactBytes(t, targetPath)

			plan := runDryRunDeleteCandidatePlan(t, configPath, statePath, Success)
			if !plan.Proceed || plan.Target == nil ||
				plan.Target.Preflight != migrate.PlannedTargetPreflightPassed ||
				plan.Deletes == nil || !plan.Deletes.DueStateKnown ||
				len(plan.Deletes.Tables) != 1 {
				t.Fatalf("dry-run delete plan = %#v", plan)
			}
			table := plan.Deletes.Tables[0]
			if table.Due != test.due || plan.Deletes.Due != test.due {
				t.Fatalf("delete due state = %#v", plan.Deletes)
			}
			if test.due {
				if table.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactExact ||
					table.CandidateCount == nil || *table.CandidateCount != 0 ||
					table.CandidateDigest == "" ||
					table.CandidateEqualityProofDigest == "" ||
					table.CandidateBatchCount == nil || *table.CandidateBatchCount != 0 ||
					table.CandidateProvenance != migrate.PlannedDeleteCandidateImpactPrimaryKeySetDifference {
					t.Fatalf("due-zero candidate disclosure = %#v", table)
				}
			} else if table.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactNotDue ||
				table.CandidateCount != nil || table.CandidateDigest != "" ||
				table.CandidateEqualityProofDigest != "" ||
				table.CandidateBatchCount != nil ||
				table.CandidateProvenance != "" ||
				len(table.CandidateLimitations) != 0 {
				t.Fatalf("not-due candidate disclosure = %#v", table)
			}
			assertDryRunArtifactBytesEqual(t, "state", beforeState, snapshotDryRunArtifactBytes(t, statePath))
			assertDryRunArtifactBytesEqual(t, "target", beforeTarget, snapshotDryRunArtifactBytes(t, targetPath))
		})
	}
}

func TestRunDryRunDeleteCandidatesFailClosedForCorruptApplicableState(t *testing.T) {
	for _, test := range []struct {
		name    string
		ext     string
		payload []byte
	}{
		{name: "yaml", ext: ".yaml", payload: []byte("runs: [")},
		{name: "sqlite", ext: ".db", payload: []byte("not a SQLite database")},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			statePath := filepath.Join(directory, "migration.state"+test.ext)
			configPath := filepath.Join(directory, "migration.yaml")
			createDryRunDeleteCandidateSQLiteDatabases(t, sourcePath, targetPath, true)
			writeDryRunDeleteCandidateConfiguration(t, configPath, sourcePath, targetPath)
			if err := os.WriteFile(statePath, test.payload, 0o600); err != nil {
				t.Fatal(err)
			}
			beforeState := snapshotDryRunArtifactBytes(t, statePath)
			beforeTarget := snapshotDryRunArtifactBytes(t, targetPath)

			plan := runDryRunDeleteCandidatePlan(
				t,
				configPath,
				statePath,
				ConfigurationError,
			)
			if plan.Proceed || plan.Deletes == nil ||
				plan.Deletes.StateError != "durable delete due-state could not be inspected read-only" ||
				len(plan.Deletes.Tables) != 1 {
				t.Fatalf("corrupt-state dry-run plan = %#v", plan)
			}
			table := plan.Deletes.Tables[0]
			if table.DueStateKnown ||
				table.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactUnavailable ||
				len(table.CandidateLimitations) != 1 ||
				table.CandidateLimitations[0] != "durable delete due-state is unavailable; exact candidate impact was not scanned" {
				t.Fatalf("corrupt-state candidate disclosure = %#v", table)
			}
			assertDryRunArtifactBytesEqual(t, "state", beforeState, snapshotDryRunArtifactBytes(t, statePath))
			assertDryRunArtifactBytesEqual(t, "target", beforeTarget, snapshotDryRunArtifactBytes(t, targetPath))
		})
	}
}

func TestRunDryRunDeleteCandidatesDoNotCreateAbsentSQLiteTargetOrState(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "absent-target.db")
	statePath := filepath.Join(directory, "absent-state.db")
	configPath := filepath.Join(directory, "migration.yaml")
	createDryRunDeleteCandidateSQLiteSource(t, sourcePath)
	writeDryRunDeleteCandidateConfiguration(t, configPath, sourcePath, targetPath)
	beforeState := snapshotDryRunArtifactBytes(t, statePath)
	beforeTarget := snapshotDryRunArtifactBytes(t, targetPath)

	plan := runDryRunDeleteCandidatePlan(t, configPath, statePath, ConfigurationError)
	if plan.Proceed || plan.Target == nil ||
		plan.Target.Presence != migrate.PlannedTargetAbsent ||
		plan.Target.Preflight != migrate.PlannedTargetPreflightFailed ||
		plan.Deletes == nil || !plan.Deletes.DueStateKnown ||
		len(plan.Deletes.Tables) != 1 {
		t.Fatalf("absent-target dry-run plan = %#v", plan)
	}
	table := plan.Deletes.Tables[0]
	if !table.Due ||
		table.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactUnavailable ||
		len(table.CandidateLimitations) != 1 ||
		table.CandidateLimitations[0] != "target read-only preflight is unavailable; exact candidate impact was not scanned" {
		t.Fatalf("absent-target candidate disclosure = %#v", table)
	}
	assertDryRunArtifactBytesEqual(t, "state", beforeState, snapshotDryRunArtifactBytes(t, statePath))
	assertDryRunArtifactBytesEqual(t, "target", beforeTarget, snapshotDryRunArtifactBytes(t, targetPath))
}

func TestRunDryRunDeleteCandidatesPreservesActiveSQLiteWALArtifacts(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	statePath := filepath.Join(directory, "absent-state.db")
	configPath := filepath.Join(directory, "migration.yaml")

	source := openDryRunDeleteCandidateWALDatabase(t, sourcePath, false)
	defer func() {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	target := openDryRunDeleteCandidateWALDatabase(t, targetPath, true)
	defer func() {
		if err := target.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	writeDryRunDeleteCandidateConfiguration(t, configPath, sourcePath, targetPath)

	beforeSource := snapshotDryRunArtifactBytes(t, sourcePath)
	beforeTarget := snapshotDryRunArtifactBytes(t, targetPath)
	beforeState := snapshotDryRunArtifactBytes(t, statePath)
	for label, artifacts := range map[string]map[string]dryRunArtifactBytes{
		"source": beforeSource,
		"target": beforeTarget,
	} {
		if !artifacts["-wal"].exists || !artifacts["-shm"].exists {
			t.Fatalf("active WAL %s artifacts = %#v, want WAL and SHM", label, artifacts)
		}
	}

	plan := runDryRunDeleteCandidatePlan(t, configPath, statePath, Success)
	if !plan.Proceed || plan.Target == nil ||
		plan.Target.Preflight != migrate.PlannedTargetPreflightPassed ||
		plan.Deletes == nil || len(plan.Deletes.Tables) != 1 {
		t.Fatalf("active-WAL dry-run plan = %#v", plan)
	}
	table := plan.Deletes.Tables[0]
	if !table.Due ||
		table.CandidateImpactStatus != migrate.PlannedDeleteCandidateImpactExact ||
		table.CandidateCount == nil || *table.CandidateCount != 1 ||
		table.CandidateDigest == "" ||
		table.CandidateEqualityProofDigest == "" ||
		table.CandidateBatchCount == nil || *table.CandidateBatchCount != 1 {
		t.Fatalf("active-WAL delete candidate disclosure = %#v", table)
	}
	if len(plan.Tables) != 1 || plan.Tables[0].Name != "notes" ||
		plan.Tables[0].Rows != 1 {
		t.Fatalf("active-WAL source discovery = %#v", plan.Tables)
	}
	assertDryRunArtifactBytesEqual(t, "source", beforeSource, snapshotDryRunArtifactBytes(t, sourcePath))
	assertDryRunArtifactBytesEqual(t, "target", beforeTarget, snapshotDryRunArtifactBytes(t, targetPath))
	assertDryRunArtifactBytesEqual(t, "state", beforeState, snapshotDryRunArtifactBytes(t, statePath))
}

func createDryRunDeleteCandidateSQLiteDatabases(
	t *testing.T,
	sourcePath string,
	targetPath string,
	targetHasExtraRow bool,
) {
	t.Helper()
	createDryRunDeleteCandidateSQLiteSource(t, sourcePath)
	database, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'source')`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if targetHasExtraRow {
		if _, err := database.Exec(`INSERT INTO notes (id, body) VALUES (2, 'stale')`); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func createDryRunDeleteCandidateSQLiteSource(t *testing.T, sourcePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'source')`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func openDryRunDeleteCandidateWALDatabase(
	t *testing.T,
	path string,
	target bool,
) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	if _, err := database.Exec(`PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE notes (id INTEGER NOT NULL PRIMARY KEY, body TEXT); INSERT INTO notes (id, body) VALUES (1, 'source')`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if target {
		if _, err := database.Exec(`INSERT INTO notes (id, body) VALUES (2, 'stale')`); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	return database
}

func writeDryRunDeleteCandidateConfiguration(
	t *testing.T,
	configPath string,
	sourcePath string,
	targetPath string,
) config.Config {
	t.Helper()
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath +
		"\nmigration:\n  target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      schedule: interval\n      interval: 1h\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func seedDryRunDeleteReconciliationEvidence(
	t *testing.T,
	statePath string,
	cfg config.Config,
	completedAt time.Time,
) {
	t.Helper()
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.NewBackend(statePath)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := completedAt.Add(-time.Minute)
	const runID = "prior-dry-run-delete-reconciliation"
	if err := store.InitializeRun(state.Run{
		ID:             runID,
		Source:         cfg.Source.Database,
		Target:         cfg.Target.Database,
		SourceEngine:   "sqlite",
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Success,
		Reason:         "completed before dry-run inspection",
		StartedAt:      startedAt,
		EndedAt:        completedAt,
	}, "dry-run-delete-candidate-test"); err != nil {
		t.Fatal(err)
	}
	ranges, ok := store.(state.RangeBackend)
	if !ok {
		t.Fatalf("state store %T lacks work-plan support", store)
	}
	stage4, ok := store.(state.Stage4Backend)
	if !ok {
		t.Fatalf("state store %T lacks Stage 4 support", store)
	}
	task := state.TaskKey{Type: "network-table-copy", Table: "notes"}
	if _, err := ranges.EnsureWorkPlan(state.WorkTask{
		RunID:        runID,
		Key:          task,
		Strategy:     "tuple_keyset",
		TopologyHash: "dry-run-delete-candidate-test",
		StartedAt:    startedAt,
	}, []state.RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stage4.BeginDeleteReconciliation(state.DeleteReconciliation{
		RunID:     runID,
		Task:      task,
		AttemptID: "prior-delete-attempt",
		Due:       true,
		StartedAt: startedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := stage4.FinishDeleteReconciliation(state.DeleteReconciliationResult{
		RunID:       runID,
		Task:        task,
		AttemptID:   "prior-delete-attempt",
		Status:      state.DeleteReconciliationCompleted,
		Candidates:  0,
		DeletedRows: 0,
		CompletedAt: completedAt,
	}); err != nil {
		t.Fatal(err)
	}
}

func runDryRunDeleteCandidatePlan(
	t *testing.T,
	configPath string,
	statePath string,
	wantCode int,
) migrate.Plan {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := Run(
		[]string{"run", "--config", configPath, "--state", statePath, "--dry-run"},
		&stdout,
		&stderr,
	); code != wantCode {
		t.Fatalf("dry-run exit code = %d, want %d, stderr = %s, plan = %s", code, wantCode, stderr.String(), stdout.String())
	}
	var plan migrate.Plan
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
		t.Fatalf("decode dry-run plan %q: %v", stdout.String(), err)
	}
	return plan
}

type dryRunArtifactBytes struct {
	exists   bool
	data     []byte
	size     int64
	modified int64
	identity os.FileInfo
}

func snapshotDryRunArtifactBytes(t *testing.T, path string) map[string]dryRunArtifactBytes {
	t.Helper()
	artifacts := make(map[string]dryRunArtifactBytes, 5)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		artifactPath := path + suffix
		info, err := os.Stat(artifactPath)
		switch {
		case err == nil:
		case errors.Is(err, os.ErrNotExist):
			artifacts[suffix] = dryRunArtifactBytes{}
			continue
		default:
			t.Fatalf("inspect dry-run artifact %s: %v", artifactPath, err)
			continue
		}
		data, err := os.ReadFile(artifactPath)
		if err != nil {
			t.Fatalf("read dry-run artifact %s: %v", artifactPath, err)
		}
		current, err := os.Stat(artifactPath)
		if err != nil {
			t.Fatalf("reinspect dry-run artifact %s: %v", artifactPath, err)
		}
		if !os.SameFile(info, current) || info.Size() != current.Size() ||
			info.ModTime() != current.ModTime() {
			t.Fatalf("dry-run artifact %s changed while snapshotting", artifactPath)
		}
		artifacts[suffix] = dryRunArtifactBytes{
			exists:   true,
			data:     data,
			size:     info.Size(),
			modified: info.ModTime().UnixNano(),
			identity: info,
		}
	}
	return artifacts
}

func assertDryRunArtifactBytesEqual(
	t *testing.T,
	label string,
	want map[string]dryRunArtifactBytes,
	got map[string]dryRunArtifactBytes,
) {
	t.Helper()
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		before, after := want[suffix], got[suffix]
		sameIdentity := !before.exists || os.SameFile(before.identity, after.identity)
		sameBytes := bytes.Equal(before.data, after.data)
		if before.exists != after.exists || before.size != after.size ||
			before.modified != after.modified || !sameBytes || !sameIdentity {
			t.Fatalf(
				"dry-run changed %s artifact %q: before={exists:%t size:%d modified:%d} after={exists:%t size:%d modified:%d} same_identity=%t same_bytes=%t",
				label,
				suffix,
				before.exists,
				before.size,
				before.modified,
				after.exists,
				after.size,
				after.modified,
				sameIdentity,
				sameBytes,
			)
		}
	}
}
