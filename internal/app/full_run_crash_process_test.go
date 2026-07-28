package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/audit"
	"github.com/johndauphine/DMTX/internal/state"
)

const (
	fullRunCrashBoundaryEnv = "DMTX_FULL_RUN_CRASH_BOUNDARY"
	fullRunCrashConfigEnv   = "DMTX_FULL_RUN_CRASH_CONFIG"
	fullRunCrashEventEnv    = "DMTX_FULL_RUN_CRASH_EVENT"
	fullRunCrashStateEnv    = "DMTX_FULL_RUN_CRASH_STATE"
	fullRunCrashCommandEnv  = "DMTX_FULL_RUN_CRASH_COMMAND"
)

func TestStage1FullRunHardKillAfterInitializationResumesThroughLeaseTakeover(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	eventPath := filepath.Join(directory, "run-initialized")
	const totalRows = 1253
	const sourceHighWater = totalRows + 1000

	createStage1FinalizationSource(t, sourcePath, totalRows)
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(
		`INSERT INTO users (id, payload) VALUES (?, ?); DELETE FROM users WHERE id = ?`,
		sourceHighWater,
		"deleted-high-water",
		sourceHighWater,
	); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)

	command := exec.Command(os.Args[0], "-test.run=^TestStage1FullRunCrashHelperProcess$")
	command.Env = append(os.Environ(),
		fullRunCrashBoundaryEnv+"=run_initialized",
		fullRunCrashConfigEnv+"="+configPath,
		fullRunCrashEventEnv+"="+eventPath,
	)
	child := startStage1Child(t, command)
	child.waitForFile(t, eventPath, "durable run initialization")
	child.kill(t)

	if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target exists before resumed migration: %v", err)
	}
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	running, found, err := store.Latest()
	if err != nil || !found || running.Outcome != state.Running || !running.Resumable {
		t.Fatalf("running state = %#v, found = %v, error = %v", running, found, err)
	}
	if tasks, err := store.ListTasks(running.ID); err != nil || len(tasks) != 0 {
		t.Fatalf("tasks before resume = %#v, error = %v", tasks, err)
	}
	if got := readStage1AuditTypes(t, configPath+".audit.ndjson"); len(got) != 1 || got[0] != "run_started" {
		t.Fatalf("audit before resume = %#v", got)
	}

	leaseIdentity, leasePath, err := targetLeaseLocation(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	leaseStore := state.SQLiteStore{Path: leasePath}
	leaseDatabase, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	var oldLease state.Lease
	oldLease.Target = leaseIdentity
	if err := leaseDatabase.QueryRow(
		`SELECT owner_token, generation FROM leases WHERE target = ?`,
		leaseIdentity,
	).Scan(&oldLease.OwnerToken, &oldLease.Generation); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if _, err := leaseDatabase.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Now().Add(-time.Hour).UTC(),
		leaseIdentity,
	); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if err := leaseDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != Success {
		t.Fatalf("resume exit code = %d, stderr = %s", code, stderr.String())
	}
	assertStage1ExactRows(t, targetPath, totalRows)
	assertStage1IntegerSchema(t, targetPath, sourceHighWater)
	assertStage1CompletedRun(t, store, running.ID, totalRows)
	if err := leaseStore.RenewLease(oldLease); err == nil {
		t.Fatal("stale pre-crash lease renewed after takeover")
	}
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"run_started", "resume_started", "validation_completed", "resume_succeeded"}; !equalStage1Strings(got, want) {
		t.Fatalf("audit after resume = %#v, want %#v", got, want)
	}
}

func TestStage1FullRunHardKillAfterSuccessPersistenceRepairsTerminalAuditWithoutRemigration(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	eventPath := filepath.Join(directory, "run-success-persisted")
	const totalRows = 501

	createStage1FinalizationSource(t, sourcePath, totalRows)
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)
	command := exec.Command(os.Args[0], "-test.run=^TestStage1FullRunCrashHelperProcess$")
	command.Env = append(os.Environ(),
		fullRunCrashBoundaryEnv+"=run_success_persisted",
		fullRunCrashConfigEnv+"="+configPath,
		fullRunCrashEventEnv+"="+eventPath,
	)
	child := startStage1Child(t, command)
	child.waitForFile(t, eventPath, "persisted successful run")
	child.kill(t)

	store := state.SQLiteStore{Path: configPath + ".state.db"}
	succeeded, found, err := store.Latest()
	if err != nil || !found || succeeded.Outcome != state.Success || succeeded.Resumable {
		t.Fatalf("persisted success = %#v, found = %v, error = %v", succeeded, found, err)
	}
	assertStage1ExactRows(t, targetPath, totalRows)
	assertStage1IntegerSchema(t, targetPath, totalRows)
	assertStage1CompletedRun(t, store, succeeded.ID, totalRows)
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"run_started", "validation_completed"}; !equalStage1Strings(got, want) {
		t.Fatalf("audit at persisted-success crash = %#v, want %#v", got, want)
	}

	leaseIdentity, leasePath, err := targetLeaseLocation(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	leaseStore := state.SQLiteStore{Path: leasePath}
	leaseDatabase, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	var oldLease state.Lease
	oldLease.Target = leaseIdentity
	if err := leaseDatabase.QueryRow(
		`SELECT owner_token, generation FROM leases WHERE target = ?`,
		leaseIdentity,
	).Scan(&oldLease.OwnerToken, &oldLease.Generation); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if _, err := leaseDatabase.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Now().Add(-time.Hour).UTC(),
		leaseIdentity,
	); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if err := leaseDatabase.Close(); err != nil {
		t.Fatal(err)
	}
	priorRepairLease, err := leaseStore.AcquireLease(leaseIdentity, succeeded.ID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendAudit(configPath, succeeded.ID, "resume_finalization_started"); err != nil {
		_ = leaseStore.ReleaseLease(priorRepairLease)
		t.Fatal(err)
	}
	if err := leaseStore.ReleaseLease(priorRepairLease); err != nil {
		t.Fatal(err)
	}

	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`UPDATE users SET payload = 'changed-after-success' WHERE id = 1`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != Success {
		t.Fatalf("terminal repair exit code = %d, stderr = %s", code, stderr.String())
	}
	if got, want := stdout.String(), "{\"tables\":1,\"rows\":501,\"validated\":true}\n"; got != want {
		t.Fatalf("terminal repair stdout = %q, want %q", got, want)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	var targetPayload string
	if err := target.QueryRow(`SELECT payload FROM users WHERE id = 1`).Scan(&targetPayload); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if targetPayload != "payload-0001" {
		t.Fatalf("terminal repair remigrated target payload %q", targetPayload)
	}
	if err := leaseStore.RenewLease(oldLease); err == nil {
		t.Fatal("stale pre-crash lease renewed after terminal repair takeover")
	}
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"run_started", "validation_completed", "resume_finalization_started", "resume_finalization_started", "run_succeeded"}; !equalStage1Strings(got, want) {
		t.Fatalf("audit after terminal repair = %#v, want %#v", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != StateError {
		t.Fatalf("second terminal repair exit code = %d, stderr = %s", code, stderr.String())
	}
	if !bytes.Contains(stderr.Bytes(), []byte("no resumable run exists")) {
		t.Fatalf("second terminal repair stderr = %q", stderr.String())
	}
}

func TestStage1ResumeHardKillAfterSuccessPersistenceRepairsResumeAuditWithoutRemigration(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	eventPath := filepath.Join(directory, "resume-success-persisted")
	const (
		runID     = "resume-success-hard-kill"
		totalRows = 501
	)

	createStage1FinalizationSource(t, sourcePath, totalRows)
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)
	store := state.SQLiteStore{Path: configPath + ".state.db"}
	initializeStage1Run(t, store, cfg, runID)
	command := exec.Command(os.Args[0], "-test.run=^TestStage1FullRunCrashHelperProcess$")
	command.Env = append(os.Environ(),
		fullRunCrashBoundaryEnv+"=resume_success_persisted",
		fullRunCrashCommandEnv+"=resume",
		fullRunCrashConfigEnv+"="+configPath,
		fullRunCrashEventEnv+"="+eventPath,
	)
	child := startStage1Child(t, command)
	child.waitForFile(t, eventPath, "persisted successful resume")
	child.kill(t)

	succeeded, found, err := store.Latest()
	if err != nil || !found || succeeded.Outcome != state.Success || succeeded.Resumable || succeeded.Reason != resumeSuccessReason {
		t.Fatalf("persisted resume success = %#v, found = %v, error = %v", succeeded, found, err)
	}
	assertStage1ExactRows(t, targetPath, totalRows)
	assertStage1IntegerSchema(t, targetPath, totalRows)
	assertStage1CompletedRun(t, store, runID, totalRows)
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"resume_started", "validation_completed"}; !equalStage1Strings(got, want) {
		t.Fatalf("resume audit at persisted-success crash = %#v, want %#v", got, want)
	}

	leaseIdentity, leasePath, err := targetLeaseLocation(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	leaseStore := state.SQLiteStore{Path: leasePath}
	leaseDatabase, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	var oldLease state.Lease
	oldLease.Target = leaseIdentity
	if err := leaseDatabase.QueryRow(
		`SELECT owner_token, generation FROM leases WHERE target = ?`,
		leaseIdentity,
	).Scan(&oldLease.OwnerToken, &oldLease.Generation); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if _, err := leaseDatabase.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Now().Add(-time.Hour).UTC(),
		leaseIdentity,
	); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if err := leaseDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`UPDATE users SET payload = 'changed-after-resume-success' WHERE id = 1`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &stdout, &stderr); code != Success {
		t.Fatalf("resume terminal repair exit code = %d, stderr = %s", code, stderr.String())
	}
	if got, want := stdout.String(), "{\"tables\":1,\"rows\":501,\"validated\":true}\n"; got != want {
		t.Fatalf("resume terminal repair stdout = %q, want %q", got, want)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	var targetPayload string
	if err := target.QueryRow(`SELECT payload FROM users WHERE id = 1`).Scan(&targetPayload); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if targetPayload != "payload-0001" {
		t.Fatalf("resume terminal repair remigrated target payload %q", targetPayload)
	}
	if err := leaseStore.RenewLease(oldLease); err == nil {
		t.Fatal("stale resume lease renewed after terminal repair takeover")
	}
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"resume_started", "validation_completed", "resume_finalization_started", "resume_succeeded"}; !equalStage1Strings(got, want) {
		t.Fatalf("resume audit after terminal repair = %#v, want %#v", got, want)
	}
}

func TestStage1YAMLStateFullRunHardKillAfterInitializationResumes(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.yaml")
	eventPath := filepath.Join(directory, "run-initialized")
	const totalRows = 1253
	const sourceHighWater = totalRows + 1000

	createStage1FinalizationSource(t, sourcePath, totalRows)
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(
		`INSERT INTO users (id, payload) VALUES (?, ?); DELETE FROM users WHERE id = ?`,
		sourceHighWater,
		"deleted-high-water",
		sourceHighWater,
	); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := writeStage1UpsertConfig(t, configPath, sourcePath, targetPath)

	command := exec.Command(os.Args[0], "-test.run=^TestStage1FullRunCrashHelperProcess$")
	command.Env = append(os.Environ(),
		fullRunCrashBoundaryEnv+"=run_initialized",
		fullRunCrashConfigEnv+"="+configPath,
		fullRunCrashEventEnv+"="+eventPath,
		fullRunCrashStateEnv+"="+statePath,
	)
	child := startStage1Child(t, command)
	child.waitForFile(t, eventPath, "durable YAML run initialization")
	child.kill(t)

	if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target exists before resumed migration: %v", err)
	}
	store := state.YAMLStore{Path: statePath}
	running, found, err := store.Latest()
	if err != nil || !found || running.Outcome != state.Running || !running.Resumable {
		t.Fatalf("running YAML state = %#v, found = %v, error = %v", running, found, err)
	}
	if hash, found, err := store.ConfigHash(running.ID); err != nil || !found || hash == "" {
		t.Fatalf("running YAML config hash = %q, found = %v, error = %v", hash, found, err)
	}
	if tasks, err := store.ListTasks(running.ID); err != nil || len(tasks) != 0 {
		t.Fatalf("tasks before resume = %#v, error = %v", tasks, err)
	}
	if got := readStage1AuditTypes(t, configPath+".audit.ndjson"); len(got) != 1 || got[0] != "run_started" {
		t.Fatalf("audit before resume = %#v", got)
	}

	leaseIdentity, leasePath, err := targetLeaseLocation(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	leaseStore := state.SQLiteStore{Path: leasePath}
	leaseDatabase, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	var oldLease state.Lease
	oldLease.Target = leaseIdentity
	if err := leaseDatabase.QueryRow(
		`SELECT owner_token, generation FROM leases WHERE target = ?`,
		leaseIdentity,
	).Scan(&oldLease.OwnerToken, &oldLease.Generation); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if _, err := leaseDatabase.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Now().Add(-time.Hour).UTC(),
		leaseIdentity,
	); err != nil {
		leaseDatabase.Close()
		t.Fatal(err)
	}
	if err := leaseDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath, "--state", statePath}, &stdout, &stderr); code != Success {
		t.Fatalf("resume exit code = %d, stderr = %s", code, stderr.String())
	}
	if got, want := stdout.String(), "{\"tables\":1,\"rows\":1253,\"validated\":true}\n"; got != want {
		t.Fatalf("resume stdout = %q, want %q", got, want)
	}
	assertStage1ExactRows(t, targetPath, totalRows)
	assertStage1IntegerSchema(t, targetPath, sourceHighWater)
	assertStage1CompletedRun(t, store, running.ID, totalRows)
	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Outcome != state.Running || runs[1].Outcome != state.Success {
		t.Fatalf("YAML run history = %#v", runs)
	}
	if err := leaseStore.RenewLease(oldLease); err == nil {
		t.Fatal("stale pre-crash lease renewed after takeover")
	}
	if got, want := readStage1AuditTypes(t, configPath+".audit.ndjson"),
		[]string{"run_started", "resume_started", "validation_completed", "resume_succeeded"}; !equalStage1Strings(got, want) {
		t.Fatalf("audit after resume = %#v, want %#v", got, want)
	}
}

func TestStage1FullRunCrashHelperProcess(t *testing.T) {
	boundary := os.Getenv(fullRunCrashBoundaryEnv)
	if boundary == "" {
		return
	}
	eventPath := os.Getenv(fullRunCrashEventEnv)
	appLifecycleBoundary = func(reached string) error {
		if reached != boundary {
			return nil
		}
		if err := os.WriteFile(eventPath, []byte(reached), 0o600); err != nil {
			return err
		}
		select {}
	}
	command := os.Getenv(fullRunCrashCommandEnv)
	if command == "" {
		command = "run"
	}
	args := []string{command, "--config", os.Getenv(fullRunCrashConfigEnv)}
	if statePath := os.Getenv(fullRunCrashStateEnv); statePath != "" {
		args = append(args, "--state", statePath)
	}
	os.Exit(Run(args, os.Stdout, os.Stderr))
}

func readStage1AuditTypes(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	types := make([]string, 0, len(lines))
	for _, line := range lines {
		var event audit.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode audit event: %v", err)
		}
		types = append(types, event.Type)
	}
	return types
}

func equalStage1Strings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
