package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4DeleteProcessKillCaseEnv    = "DMTX_STAGE4_DELETE_PROCESS_KILL_CASE"
	stage4DeleteProcessKillStateEnv   = "DMTX_STAGE4_DELETE_PROCESS_KILL_STATE"
	stage4DeleteProcessKillPathEnv    = "DMTX_STAGE4_DELETE_PROCESS_KILL_STATE_PATH"
	stage4DeleteProcessKillConfigEnv  = "DMTX_STAGE4_DELETE_PROCESS_KILL_CONFIG_PATH"
	stage4DeleteProcessKillRunEnv     = "DMTX_STAGE4_DELETE_PROCESS_KILL_RUN_ID"
	stage4DeleteProcessKillSpoolEnv   = "DMTX_STAGE4_DELETE_PROCESS_KILL_SPOOL"
	stage4DeleteProcessKillEventEnv   = "DMTX_STAGE4_DELETE_PROCESS_KILL_EVENT"
	stage4DeleteProcessKillStatusEnv  = "DMTX_STAGE4_DELETE_PROCESS_KILL_STATUS"
	stage4DeleteProcessKillTestTimout = 45 * time.Second
)

// stage4DeleteProcessKillStore deliberately requires every durable capability
// used by an admitted delete route.  The child only intercepts the state
// acknowledgement that follows a native target receipt; all other state calls
// remain the real YAML or SQLite implementation.
type stage4DeleteProcessKillStore interface {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	state.Stage4DeleteJournalReadinessBackend
}

type stage4DeleteProcessKillBlockingBackend struct {
	stage4DeleteProcessKillStore
	eventPath string
}

// CommitDeleteReconciliationBatch is reached only after the target's atomic
// DELETE+receipt operation has returned.  Parking here gives the parent an
// OS-level kill point with a durable pending batch and native receipt but no
// state frontier or last-success acknowledgement.
func (backend *stage4DeleteProcessKillBlockingBackend) CommitDeleteReconciliationBatch(
	state.DeleteReconciliationBatchCommit,
) error {
	file, err := os.OpenFile(backend.eventPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("delete-receipt-committed\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	select {}
}

type stage4DeleteProcessKillFixture struct {
	cell               string
	cfg                config.Config
	task               state.TaskKey
	requiresReadiness  bool
	targetOnlyRows     func(context.Context) (int, error)
	targetSourceRows   func(context.Context) (int, error)
	journalReceiptRows func(context.Context, string) (int, error)
}

// TestStage4SameEngineDeleteProcessKillReplayLive is the real process-loss
// proof for every currently admitted non-strict delete reconciliation cell.
// Each child is killed after native receipt commit and before durable state
// acknowledgement, then the exact original run is resumed through the normal
// public route.  The fixtures are intentionally one table and one batch each:
// their purpose is to isolate recovery truth, not to load-test the engines.
func TestStage4SameEngineDeleteProcessKillReplayLive(t *testing.T) {
	cells := []struct {
		name  string
		setup func(*testing.T) stage4DeleteProcessKillFixture
	}{
		{name: "postgres", setup: stage4DeleteProcessKillPostgresFixture},
		{name: "mysql80", setup: func(t *testing.T) stage4DeleteProcessKillFixture {
			return stage4DeleteProcessKillMySQLFixture(t, "mysql80")
		}},
		{name: "mariadb1011", setup: func(t *testing.T) stage4DeleteProcessKillFixture {
			return stage4DeleteProcessKillMySQLFixture(t, "mariadb1011")
		}},
		{name: "mssql", setup: stage4DeleteProcessKillSQLServerFixture},
		{name: "sqlite", setup: stage4DeleteProcessKillSQLiteFixture},
	}
	for _, cell := range cells {
		cell := cell
		for _, stateKind := range []string{"yaml", "sqlite"} {
			stateKind := stateKind
			t.Run(cell.name+"/"+stateKind, func(t *testing.T) {
				fixture := cell.setup(t)
				stage4RunDeleteProcessKillReplay(t, fixture, stateKind)
			})
		}
	}
}

// TestStage4SameEngineDeleteProcessKillReplayHelperProcess is intentionally
// invoked only by the parent test above.  Its environment carries paths and a
// cell label, while the credential-bearing route configuration stays in a
// mode-0600 temporary file and is never included in test output.
func TestStage4SameEngineDeleteProcessKillReplayHelperProcess(t *testing.T) {
	cell := os.Getenv(stage4DeleteProcessKillCaseEnv)
	if cell == "" {
		return
	}
	stage4WriteDeleteProcessKillStatus("started")
	configPath := os.Getenv(stage4DeleteProcessKillConfigEnv)
	encoded, err := os.ReadFile(configPath)
	if err != nil {
		stage4WriteDeleteProcessKillStatus("read-config-failed")
		t.Fatal("read process-kill delete route configuration")
	}
	stage4WriteDeleteProcessKillStatus("config-read")
	var cfg config.Config
	if err := json.Unmarshal(encoded, &cfg); err != nil {
		stage4WriteDeleteProcessKillStatus("decode-config-failed")
		t.Fatal("decode process-kill delete route configuration")
	}
	stage4WriteDeleteProcessKillStatus("config-decoded-delete-" + string(cfg.Migration.Deletes.Mode))
	store, err := stage4DeleteProcessKillOpenStore(
		os.Getenv(stage4DeleteProcessKillStateEnv),
		os.Getenv(stage4DeleteProcessKillPathEnv),
	)
	if err != nil {
		stage4WriteDeleteProcessKillStatus("open-state-failed")
		t.Fatal("open process-kill delete state")
	}
	stage4WriteDeleteProcessKillStatus("state-open")
	blocking := &stage4DeleteProcessKillBlockingBackend{
		stage4DeleteProcessKillStore: store,
		eventPath:                    os.Getenv(stage4DeleteProcessKillEventEnv),
	}
	spool := os.Getenv(stage4DeleteProcessKillSpoolEnv)
	if spool == "" || os.Getenv(stage4DeleteProcessKillRunEnv) == "" {
		stage4WriteDeleteProcessKillStatus("missing-run-context")
		t.Fatal("process-kill delete child lacks run context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          os.Getenv(stage4DeleteProcessKillRunEnv),
			Backend:        blocking,
			SpoolDirectory: spool,
		},
	}
	stage4WriteDeleteProcessKillStatus("route-dispatched")
	result, err := stage4DeleteProcessKillExecute(ctx, cell, cfg, observer)
	if err != nil {
		stage4WriteDeleteProcessKillStatus(
			"route-error-" + string(ClassifyTransferError(err)),
		)
		t.Fatalf(
			"delete route returned before process-kill acknowledgement boundary [class=%s]",
			ClassifyTransferError(err),
		)
	}
	stage4WriteDeleteProcessKillStatus(fmt.Sprintf("route-completed-%d-%d", result.Tables, result.Rows))
	t.Fatal("delete route completed before process-kill acknowledgement boundary")
}

func stage4RunDeleteProcessKillReplay(
	t *testing.T,
	fixture stage4DeleteProcessKillFixture,
	stateKind string,
) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("resolve process-kill test directory")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal("protect process-kill test directory")
	}
	statePath := filepath.Join(root, "state."+stateKind)
	configPath := filepath.Join(root, "route.json")
	eventPath := filepath.Join(root, "delete-receipt-committed")
	statusPath := filepath.Join(root, "child-status")
	encoded, err := json.Marshal(fixture.cfg)
	if err != nil {
		t.Fatalf("encode process-kill %s route configuration: %T", fixture.cell, err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write private process-kill route configuration")
	}

	runID := "stage4-delete-process-kill-" + fixture.cell + "-" + stateKind + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	spool := stage4DeleteProcessKillSpool(t, root, runID)
	store, err := stage4DeleteProcessKillOpenStore(stateKind, statePath)
	if err != nil {
		t.Fatalf("open %s process-kill state: %T", stateKind, err)
	}
	stage4DeleteProcessKillInitializeRun(t, store, runID, fixture.cfg)

	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStage4SameEngineDeleteProcessKillReplayHelperProcess$",
	)
	command.Env = append(os.Environ(),
		stage4DeleteProcessKillCaseEnv+"="+fixture.cell,
		stage4DeleteProcessKillStateEnv+"="+stateKind,
		stage4DeleteProcessKillPathEnv+"="+statePath,
		stage4DeleteProcessKillConfigEnv+"="+configPath,
		stage4DeleteProcessKillRunEnv+"="+runID,
		stage4DeleteProcessKillSpoolEnv+"="+spool,
		stage4DeleteProcessKillEventEnv+"="+eventPath,
		stage4DeleteProcessKillStatusEnv+"="+statusPath,
	)
	var childOutput bytes.Buffer
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	if err := command.Start(); err != nil {
		t.Fatal("start process-kill delete child")
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		_ = command.Process.Kill()
		<-wait
	})
	stage4WaitForDeleteProcessKillBoundary(t, eventPath, statusPath, wait, &reaped, &childOutput)
	if err := command.Process.Kill(); err != nil {
		waitErr := <-wait
		reaped = true
		t.Fatalf("kill process-kill delete child: %T / %T", err, waitErr)
	}
	if err := <-wait; err == nil {
		reaped = true
		t.Fatal("process-kill delete child exited normally instead of being killed")
	}
	reaped = true
	_ = childOutput // Child output is intentionally withheld: it may contain driver diagnostics.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stage4AssertDeleteProcessKillPreResume(t, ctx, store, runID, spool, fixture)

	resumeEvents := make([]string, 0)
	resumeRun := Stage4RunContext{
		RunID:          runID,
		Backend:        store,
		Resume:         true,
		SpoolDirectory: spool,
	}
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    resumeRun,
	}
	result, err := ExecuteResume(
		ctx,
		fixture.cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf("resume hard-killed %s delete run: %T", fixture.cell, err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("resumed hard-killed %s result = %#v", fixture.cell, result)
	}
	stage4AssertDeleteProcessKillSentinelState(t, store, runID, false)
	published, err := PublishStage4RunCompletion(
		context.Background(),
		resumeRun,
		"migration resumed and completed",
		time.Now().UTC(),
	)
	if err != nil || !published {
		t.Fatalf(
			"publish resumed hard-killed %s run success published=%v class=%s state_transition=%t immutable=%t",
			fixture.cell,
			published,
			ClassifyTransferError(err),
			errors.Is(err, state.ErrStateTransition),
			errors.Is(err, state.ErrImmutableEvidence),
		)
	}
	stage4AssertDeleteProcessKillSentinelState(t, store, runID, true)
	stage4AssertDeleteProcessKillPostResume(t, ctx, store, runID, spool, fixture)
}

func stage4WaitForDeleteProcessKillBoundary(
	t *testing.T,
	eventPath string,
	statusPath string,
	wait <-chan error,
	reaped *bool,
	childOutput *bytes.Buffer,
) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(stage4DeleteProcessKillTestTimout)
	defer timeout.Stop()
	for {
		if _, err := os.Stat(eventPath); err == nil {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			if err := <-wait; err == nil {
				*reaped = true
				t.Fatalf("process-kill delete child completed without acknowledgement boundary: %s", stage4DeleteProcessKillChildStatus(statusPath, childOutput))
			}
			*reaped = true
			t.Fatalf("process-kill delete child exited before acknowledgement boundary: %s", stage4DeleteProcessKillChildStatus(statusPath, childOutput))
		case <-wait:
			*reaped = true
			t.Fatalf("process-kill delete child exited before acknowledgement boundary: %s", stage4DeleteProcessKillChildStatus(statusPath, childOutput))
		}
	}
}

func stage4WriteDeleteProcessKillStatus(status string) {
	path := os.Getenv(stage4DeleteProcessKillStatusEnv)
	if path == "" {
		return
	}
	_ = os.WriteFile(path, []byte(status), 0o600)
}

func stage4DeleteProcessKillChildStatus(statusPath string, output *bytes.Buffer) string {
	if value, err := os.ReadFile(statusPath); err == nil {
		return "child status=" + strings.TrimSpace(string(value))
	}
	return stage4DeleteProcessKillSafeChildOutput(output)
}

// The helper itself writes only this fixed assertion plus an error class.  Do
// not expose arbitrary driver diagnostics here: its configuration contains
// live fixture credentials.
func stage4DeleteProcessKillSafeChildOutput(output *bytes.Buffer) string {
	if output == nil {
		return "child output unavailable"
	}
	value := strings.TrimSpace(output.String())
	if value == "" {
		return "child output unavailable"
	}
	if strings.Contains(value, "delete route returned before process-kill acknowledgement boundary [class=") {
		return value
	}
	return "child output withheld"
}

func stage4AssertDeleteProcessKillPreResume(
	t *testing.T,
	ctx context.Context,
	store stage4DeleteProcessKillStore,
	runID string,
	spool string,
	fixture stage4DeleteProcessKillFixture,
) {
	t.Helper()
	runs, err := store.List()
	if err != nil || len(runs) != 1 || runs[0].ID != runID ||
		runs[0].Outcome != state.Running || !runs[0].Resumable || !runs[0].EndedAt.IsZero() {
		t.Fatalf("hard-killed %s run is not truthfully active", fixture.cell)
	}
	if fixture.requiresReadiness {
		ready, found, err := store.LoadStage4DeleteJournalReadiness(runID)
		targetEngine, engineErr := config.CanonicalEngine(fixture.cfg.Target.Type)
		if err != nil || engineErr != nil || !found ||
			ready.Readiness.RunID != runID || ready.Readiness.TargetEngine != targetEngine ||
			ready.Readiness.InventoryDigest == "" || ready.Readiness.JournalCatalogDigest == "" {
			t.Fatalf("hard-killed %s run lacks durable delete-journal readiness", fixture.cell)
		}
	}
	run := Stage4RunContext{RunID: runID, Backend: store, SpoolDirectory: spool}
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		t.Fatalf("load hard-killed %s durable work inventory: %T", fixture.cell, err)
	}
	work, found := inventory.tasks[fixture.task]
	if !found || work.Strategy == "" || work.TopologyHash == "" {
		t.Fatalf("hard-killed %s route lacks durable table work", fixture.cell)
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(runID, stage4AdapterWork{
		task: fixture.task, strategy: work.Strategy, topology: work.TopologyHash,
	})
	if err != nil {
		t.Fatalf("derive hard-killed %s delete attempt: %T", fixture.cell, err)
	}
	record, found, err := store.LoadDeleteReconciliation(runID, fixture.task, attemptID)
	if err != nil || !found || record.Status != state.DeleteReconciliationRunning ||
		record.Plan == nil || record.PendingBatch == nil || record.LastBatchCommit != nil ||
		record.Frontier != 0 || record.CommittedBatches != 0 || record.Candidates != 1 || record.DeletedRows != 0 {
		t.Fatalf("hard-killed %s reconciliation is not a durable pending receipt", fixture.cell)
	}
	if _, found, err := store.LoadLatestSuccessfulDeleteReconciliation(runID, fixture.task); err != nil || found {
		t.Fatalf("hard-killed %s run advanced last-success before acknowledgement", fixture.cell)
	}
	if rows, err := fixture.targetOnlyRows(ctx); err != nil || rows != 0 {
		t.Fatalf("hard-killed %s target-only rows = %d", fixture.cell, rows)
	}
	if rows, err := fixture.targetSourceRows(ctx); err != nil || rows != 2 {
		t.Fatalf("hard-killed %s copied target rows = %d", fixture.cell, rows)
	}
	if rows, err := fixture.journalReceiptRows(ctx, record.PendingBatch.Token); err != nil || rows != 1 {
		t.Fatalf("hard-killed %s native receipt rows = %d", fixture.cell, rows)
	}
}

func stage4AssertDeleteProcessKillPostResume(
	t *testing.T,
	ctx context.Context,
	store stage4DeleteProcessKillStore,
	runID string,
	spool string,
	fixture stage4DeleteProcessKillFixture,
) {
	t.Helper()
	run := Stage4RunContext{RunID: runID, Backend: store, SpoolDirectory: spool}
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		t.Fatalf("load resumed %s durable work inventory: %T", fixture.cell, err)
	}
	work, found := inventory.tasks[fixture.task]
	if !found {
		t.Fatalf("resumed %s route lacks durable table work", fixture.cell)
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(runID, stage4AdapterWork{
		task: fixture.task, strategy: work.Strategy, topology: work.TopologyHash,
	})
	if err != nil {
		t.Fatalf("derive resumed %s delete attempt: %T", fixture.cell, err)
	}
	record, found, err := store.LoadLatestSuccessfulDeleteReconciliation(runID, fixture.task)
	if err != nil || !found || record.Status != state.DeleteReconciliationCompleted ||
		record.AttemptID != attemptID || record.Plan == nil || record.PendingBatch != nil ||
		record.LastBatchCommit == nil || record.Candidates != 1 || record.DeletedRows != 1 ||
		record.Frontier != 1 || record.CommittedBatches != 1 {
		t.Fatalf("resumed %s reconciliation did not commit the original receipt", fixture.cell)
	}
	committed, found, err := store.LoadDeleteReconciliation(runID, fixture.task, attemptID)
	if err != nil || !found || !reflect.DeepEqual(committed, record) {
		t.Fatalf("resumed %s reconciliation did not retain one exact terminal record", fixture.cell)
	}
	if rows, err := fixture.targetOnlyRows(ctx); err != nil || rows != 0 {
		t.Fatalf("resumed %s target-only rows = %d", fixture.cell, rows)
	}
	if rows, err := fixture.targetSourceRows(ctx); err != nil || rows != 2 {
		t.Fatalf("resumed %s copied target rows = %d", fixture.cell, rows)
	}
	if rows, err := fixture.journalReceiptRows(ctx, record.LastBatchCommit.Token); err != nil || rows != 1 {
		t.Fatalf("resumed %s native receipt replay rows = %d", fixture.cell, rows)
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.ID != runID ||
		latest.Outcome != state.Success || latest.Resumable || latest.EndedAt.IsZero() {
		t.Fatalf(
			"resumed %s latest run = {id=%q outcome=%q resumable=%t ended=%t}, want original successful terminal run",
			fixture.cell,
			latest.ID,
			latest.Outcome,
			latest.Resumable,
			!latest.EndedAt.IsZero(),
		)
	}
	runs, err := store.List()
	if err != nil {
		t.Fatalf("list resumed %s run: %T", fixture.cell, err)
	}
	successes := 0
	for _, record := range runs {
		if record.ID != runID {
			t.Fatalf("resumed %s route recorded a new run %q", fixture.cell, record.ID)
		}
		if record.Outcome == state.Success {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("resumed %s route recorded %d successful original-run outcomes", fixture.cell, successes)
	}
}

func stage4AssertDeleteProcessKillSentinelState(
	t *testing.T,
	store stage4DeleteProcessKillStore,
	runID string,
	wantCompleted bool,
) {
	t.Helper()
	tasks, _, err := store.ListWork(runID)
	if err != nil {
		t.Fatalf("list %s delete schema sentinels: %T", runID, err)
	}
	found := 0
	for _, task := range tasks {
		if _, sentinel := stage4SentinelRangeID(task.Key); !sentinel {
			continue
		}
		found++
		completed := task.Status == "completed" && !task.CompletedAt.IsZero()
		if completed != wantCompleted {
			t.Fatalf(
				"delete schema sentinel %#v completed=%t, want %t",
				task.Key,
				completed,
				wantCompleted,
			)
		}
	}
	if found == 0 {
		t.Fatalf("delete route %q established no schema sentinels", runID)
	}
}

func stage4DeleteProcessKillOpenStore(
	kind string,
	path string,
) (stage4DeleteProcessKillStore, error) {
	switch kind {
	case "yaml":
		return state.YAMLStore{Path: path}, nil
	case "sqlite":
		return state.SQLiteStore{Path: path}, nil
	default:
		return nil, fmt.Errorf("unknown process-kill delete state backend")
	}
}

func stage4DeleteProcessKillSpool(t *testing.T, root, runID string) string {
	t.Helper()
	parent := filepath.Join(root, "spool")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal("create private process-kill spool parent")
	}
	path := filepath.Join(parent, stage4LifecycleRunDigest(runID))
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal("create private process-kill spool")
	}
	return path
}

func stage4DeleteProcessKillInitializeRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	cfg config.Config,
) {
	t.Helper()
	sourceEngine, err := config.CanonicalEngine(cfg.Source.Type)
	if err != nil {
		t.Fatal("identify process-kill source engine")
	}
	sourceIdentity, err := stage4DeleteProcessKillEndpointIdentity(cfg.Source)
	if err != nil {
		t.Fatal("identify process-kill source workload")
	}
	targetIdentity, err := stage4DeleteProcessKillEndpointIdentity(cfg.Target)
	if err != nil {
		t.Fatal("identify process-kill target workload")
	}
	configurationHash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal("hash process-kill route configuration")
	}
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         cfg.Source.Database,
		Target:         cfg.Target.Database,
		SourceEngine:   sourceEngine,
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      time.Now().UTC().Add(-time.Minute),
	}, configurationHash); err != nil {
		t.Fatalf("initialize process-kill run: %T", err)
	}
}

func stage4DeleteProcessKillEndpointIdentity(endpoint config.Endpoint) (string, error) {
	engineName, err := config.CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engineName == "sqlite" {
		if endpoint.Database == "" {
			return "", fmt.Errorf("SQLite database is required")
		}
		return "sqlite:" + filepath.Clean(endpoint.Database), nil
	}
	return config.NetworkEndpointWorkloadIdentity(endpoint)
}

func stage4DeleteProcessKillExecute(
	ctx context.Context,
	cell string,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	switch cell {
	case "postgres":
		return PostgresToPostgresWithObserver(ctx, cfg, observer)
	case "mysql80", "mariadb1011":
		return MySQLToMySQLWithObserver(ctx, cfg, observer)
	case "mssql":
		return SQLServerToSQLServerWithObserver(ctx, cfg, observer)
	case "sqlite":
		// SQLiteToSQLiteWithObserver intentionally remains the legacy
		// compatibility entry point. The public registry dispatcher is what
		// selects the certified Stage 4 SQLite path when reconciliation is
		// explicitly enabled.
		return Execute(ctx, cfg, observer)
	default:
		return Result{}, fmt.Errorf("unknown process-kill delete cell")
	}
}

func stage4DeleteProcessKillConfig(
	source config.Endpoint,
	target config.Endpoint,
	table string,
) config.Config {
	return config.Config{
		Source: source,
		Target: target,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{table},
			ConnectionLimit:    4,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			MemoryCeilingBytes: 64 << 20,
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationCountOnly,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes: config.DeletePolicy{
				Mode:           config.DeleteModeReconcile,
				TargetBehavior: config.DeleteTargetHard,
				Reconcile: config.DeleteReconcilePolicy{
					Schedule:          config.DeleteScheduleInterval,
					Interval:          time.Hour,
					BatchSize:         1,
					RequirePrimaryKey: true,
				},
			},
		},
	}
}

func stage4DeleteProcessKillPostgresFixture(t *testing.T) stage4DeleteProcessKillFixture {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN for PostgreSQL delete process-kill coverage")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL process-kill DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL certificate and hostname")
	}
	caPath := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	sourceDatabase, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL process-kill source: %T", err)
	}
	t.Cleanup(func() { _ = sourceDatabase.Close() })
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL process-kill source: %T", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, sourceDatabase, "process-kill source")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_kill_" + suffix
	targetName := "dmtx_s4_kill_target_" + suffix
	if _, err := sourceDatabase.ExecContext(ctx, "CREATE DATABASE "+postgresIdentifier(targetName)); err != nil {
		t.Fatalf("create PostgreSQL process-kill target: %T", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+postgresIdentifier(targetName)+" WITH (FORCE)"); err != nil {
			t.Errorf("drop PostgreSQL process-kill target: %T", err)
		}
	})
	if _, err := sourceDatabase.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
		t.Fatalf("create PostgreSQL process-kill source schema: %T", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL process-kill source schema: %T", err)
		}
	})
	sourceEndpoint := config.Endpoint{
		Type: "postgres", Host: parsed.Host, Port: int(parsed.Port), Database: parsed.Database,
		User: parsed.User, Password: parsed.Password, Schema: namespace, SSLMode: "verify-full", TLSCAFile: caPath,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetName
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build PostgreSQL process-kill target DSN: %T", err)
	}
	targetDatabase, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL process-kill target: %T", err)
	}
	t.Cleanup(func() { _ = targetDatabase.Close() })
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL process-kill target: %T", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, targetDatabase, "process-kill target")
	if _, err := targetDatabase.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
		t.Fatalf("create PostgreSQL process-kill target schema: %T", err)
	}
	const table = "items"
	qualified := postgresQualified(namespace, table)
	ddl := " (id bigint NOT NULL, payload text NOT NULL, CONSTRAINT items_pkey PRIMARY KEY (id))"
	for _, database := range []*sql.DB{sourceDatabase, targetDatabase} {
		if _, err := database.ExecContext(ctx, "CREATE TABLE "+qualified+ddl); err != nil {
			t.Fatalf("create PostgreSQL process-kill table: %T", err)
		}
	}
	if _, err := sourceDatabase.ExecContext(ctx, "INSERT INTO "+qualified+" (id, payload) VALUES (1, 'source-one'), (2, 'source-two')"); err != nil {
		t.Fatalf("seed PostgreSQL process-kill source: %T", err)
	}
	if _, err := targetDatabase.ExecContext(ctx, "INSERT INTO "+qualified+" (id, payload) VALUES (1, 'stale-one'), (2, 'stale-two'), (900, 'target-only')"); err != nil {
		t.Fatalf("seed PostgreSQL process-kill target: %T", err)
	}
	return stage4DeleteProcessKillFixture{
		cell: "postgres", cfg: stage4DeleteProcessKillConfig(sourceEndpoint, targetEndpoint, table),
		task: state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table},
		targetOnlyRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualified+" WHERE id = 900").Scan(&rows)
			return rows, err
		},
		targetSourceRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualified+" WHERE (id = 1 AND payload = 'source-one') OR (id = 2 AND payload = 'source-two')").Scan(&rows)
			return rows, err
		},
		journalReceiptRows: func(ctx context.Context, token string) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(postgresDeleteJournalSchema, postgresDeleteJournalTable)+" WHERE token = $1", token).Scan(&rows)
			return rows, err
		},
	}
}

func stage4DeleteProcessKillMySQLFixture(t *testing.T, name string) stage4DeleteProcessKillFixture {
	t.Helper()
	var fixture mysqlDeleteJournalLiveFixture
	found := false
	for _, candidate := range mysqlDeleteJournalLiveFixtures() {
		if candidate.name == name {
			fixture, found = candidate, true
			break
		}
	}
	if !found {
		t.Fatal("unknown MySQL-family delete process-kill fixture")
	}
	if fixture.targetDSN == "" || fixture.adminDSN == "" || fixture.caPath == "" {
		t.Skip("set verified-TLS target/admin/CA fixture values for " + fixture.name + " delete process-kill coverage")
	}
	registerMySQLCommonFixtureTLSNamed(t, fixture.caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(t, fixture.name+" process-kill target", fixture.targetDSN, fixture.tlsConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	sourceDSN, sourceEndpoint := mysqlTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		mysqlTargetEvolutionLiveFixture{name: "delete_kill_source_" + fixture.name, collation: fixture.collation, tlsConfig: fixture.tlsConfig, refreshInfo: fixture.refreshInfo},
		parsed,
		fixture.adminDSN,
		fixture.caPath,
		nil,
	)
	targetDSN, targetEndpoint := mysqlTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		mysqlTargetEvolutionLiveFixture{name: "delete_kill_target_" + fixture.name, collation: fixture.collation, tlsConfig: fixture.tlsConfig, refreshInfo: fixture.refreshInfo},
		parsed,
		fixture.adminDSN,
		fixture.caPath,
		nil,
	)
	sourceDatabase := openMySQLNativeLiveDatabaseForFlavor(t, ctx, fixture.name+" process-kill source", sourceDSN, fixture.refreshInfo)
	targetDatabase := openMySQLNativeLiveDatabaseForFlavor(t, ctx, fixture.name+" process-kill target", targetDSN, fixture.refreshInfo)
	table := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ddl := "CREATE TABLE " + mySQLIdentifier(table) + " (`id` BIGINT NOT NULL, `payload` VARCHAR(32) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=" + fixture.collation
	for _, database := range []*sql.DB{sourceDatabase, targetDatabase} {
		if _, err := database.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create %s process-kill table: %T", fixture.name, err)
		}
	}
	if _, err := sourceDatabase.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(table)+" (`id`, `payload`) VALUES (1, 'source-one'), (2, 'source-two')"); err != nil {
		t.Fatalf("seed %s process-kill source: %T", fixture.name, err)
	}
	if _, err := targetDatabase.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(table)+" (`id`, `payload`) VALUES (1, 'stale-one'), (2, 'stale-two'), (900, 'target-only')"); err != nil {
		t.Fatalf("seed %s process-kill target: %T", fixture.name, err)
	}
	return stage4DeleteProcessKillFixture{
		cell:              fixture.name,
		cfg:               stage4DeleteProcessKillConfig(sourceEndpoint, targetEndpoint, table),
		task:              state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: sourceEndpoint.Database, Table: table},
		requiresReadiness: true,
		targetOnlyRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLIdentifier(table)+" WHERE `id` = ?", 900).Scan(&rows)
			return rows, err
		},
		targetSourceRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLIdentifier(table)+" WHERE (`id` = 1 AND `payload` = 'source-one') OR (`id` = 2 AND `payload` = 'source-two')").Scan(&rows)
			return rows, err
		},
		journalReceiptRows: func(ctx context.Context, token string) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLIdentifier(mysqlDeleteJournalTable)+" WHERE `token` = ?", token).Scan(&rows)
			return rows, err
		},
	}
}

func stage4DeleteProcessKillSQLServerFixture(t *testing.T) stage4DeleteProcessKillFixture {
	t.Helper()
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	sourceCleanup := &sqlServerTargetEvolutionLiveCleanupEvidence{}
	targetCleanup := &sqlServerTargetEvolutionLiveCleanupEvidence{}
	sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		base,
		sourceCleanup,
	)
	targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		base,
		targetCleanup,
	)
	sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "delete process-kill source", sourceEndpoint)
	targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "delete process-kill target", targetEndpoint)
	table := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, table)
	createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, table)
	if _, err := sourceDatabase.ExecContext(ctx, "INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, table)+" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'source-one'), (1, 2, 'source-two')"); err != nil {
		t.Fatalf("seed SQL Server process-kill source: %T", err)
	}
	if _, err := targetDatabase.ExecContext(ctx, "INSERT INTO "+sqlServerQualified(targetEndpoint.Schema, table)+" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'stale-one'), (1, 2, 'stale-two'), (9, 9, 'target-only')"); err != nil {
		t.Fatalf("seed SQL Server process-kill target: %T", err)
	}
	return stage4DeleteProcessKillFixture{
		cell:              "mssql",
		cfg:               stage4DeleteProcessKillConfig(sourceEndpoint, targetEndpoint, table),
		task:              state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: sourceEndpoint.Schema, Table: table},
		requiresReadiness: true,
		targetOnlyRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, table)+" WHERE [tenant_id] = 9 AND [item_id] = 9").Scan(&rows)
			return rows, err
		},
		targetSourceRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, table)+" WHERE ([tenant_id] = 1 AND [item_id] = 1 AND [payload] = 'source-one') OR ([tenant_id] = 1 AND [item_id] = 2 AND [payload] = 'source-two')").Scan(&rows)
			return rows, err
		},
		journalReceiptRows: func(ctx context.Context, token string) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable)+" WHERE [token] = @p1", token).Scan(&rows)
			return rows, err
		},
	}
}

func stage4DeleteProcessKillSQLiteFixture(t *testing.T) stage4DeleteProcessKillFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.db")
	targetPath := filepath.Join(root, "target.db")
	sourceDatabase, err := openSQLiteTargetDatabase(ctx, sourcePath)
	if err != nil {
		t.Fatalf("open SQLite process-kill source: %T", err)
	}
	t.Cleanup(func() { _ = sourceDatabase.Close() })
	targetDatabase, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatalf("open SQLite process-kill target: %T", err)
	}
	t.Cleanup(func() { _ = targetDatabase.Close() })
	const table = "items"
	for _, database := range []*sql.DB{sourceDatabase, targetDatabase} {
		if _, err := database.ExecContext(ctx, "CREATE TABLE "+quote(table)+" (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)"); err != nil {
			t.Fatalf("create SQLite process-kill table: %T", err)
		}
	}
	if _, err := sourceDatabase.ExecContext(ctx, "INSERT INTO "+quote(table)+" (id, payload) VALUES (1, 'source-one'), (2, 'source-two')"); err != nil {
		t.Fatalf("seed SQLite process-kill source: %T", err)
	}
	if _, err := targetDatabase.ExecContext(ctx, "INSERT INTO "+quote(table)+" (id, payload) VALUES (1, 'stale-one'), (2, 'stale-two'), (900, 'target-only')"); err != nil {
		t.Fatalf("seed SQLite process-kill target: %T", err)
	}
	sourceEndpoint := config.Endpoint{Type: "sqlite", Database: sourcePath}
	targetEndpoint := config.Endpoint{Type: "sqlite", Database: targetPath}
	return stage4DeleteProcessKillFixture{
		cell: "sqlite", cfg: stage4DeleteProcessKillConfig(sourceEndpoint, targetEndpoint, table),
		task: state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: table},
		targetOnlyRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(table)+" WHERE id = 900").Scan(&rows)
			return rows, err
		},
		targetSourceRows: func(ctx context.Context) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(table)+" WHERE (id = 1 AND payload = 'source-one') OR (id = 2 AND payload = 'source-two')").Scan(&rows)
			return rows, err
		},
		journalReceiptRows: func(ctx context.Context, token string) (int, error) {
			var rows int
			err := targetDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(sqliteDeleteJournalTable)+" WHERE token = ?", token).Scan(&rows)
			return rows, err
		},
	}
}
