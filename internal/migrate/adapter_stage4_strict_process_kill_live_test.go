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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

// This file is deliberately an external-process test rather than an injected
// error test.  The child persists real strict evidence and then is hard-killed
// by its parent, which makes every source implementation prove its actual
// process-loss contract through the public resume route.
const (
	stage4StrictProcessKillCaseEnv       = "DMTX_STAGE4_STRICT_PROCESS_KILL_CASE"
	stage4StrictProcessKillStateEnv      = "DMTX_STAGE4_STRICT_PROCESS_KILL_STATE"
	stage4StrictProcessKillPathEnv       = "DMTX_STAGE4_STRICT_PROCESS_KILL_STATE_PATH"
	stage4StrictProcessKillConfigEnv     = "DMTX_STAGE4_STRICT_PROCESS_KILL_CONFIG_PATH"
	stage4StrictProcessKillRunEnv        = "DMTX_STAGE4_STRICT_PROCESS_KILL_RUN_ID"
	stage4StrictProcessKillSpoolEnv      = "DMTX_STAGE4_STRICT_PROCESS_KILL_SPOOL"
	stage4StrictProcessKillEventEnv      = "DMTX_STAGE4_STRICT_PROCESS_KILL_EVENT"
	stage4StrictProcessKillStatusEnv     = "DMTX_STAGE4_STRICT_PROCESS_KILL_STATUS"
	stage4StrictProcessKillEvidenceEnv   = "DMTX_STAGE4_STRICT_PROCESS_KILL_EVIDENCE_COUNT"
	stage4StrictProcessKillBoundaryLimit = 60 * time.Second
)

type stage4StrictProcessKillStore interface {
	state.Backend
	Stage4StateBackend
}

// stage4StrictProcessKillBlockingBackend writes to the real YAML or SQLite
// backend first.  It parks only after every expected strict-evidence record
// has become durable, while the source view is still live and before it can
// authorize target transfer.  The parent then uses an OS-level hard kill.
type stage4StrictProcessKillBlockingBackend struct {
	Stage4StateBackend
	cleanup state.StrictMigrationCleanupBackend

	eventPath string
	want      int

	mu    sync.Mutex
	saved int
}

func (backend *stage4StrictProcessKillBlockingBackend) SaveStrictMigrationCleanupIntent(intent state.StrictMigrationCleanupIntent) error {
	if backend.cleanup == nil {
		return errors.New("strict process-kill cleanup backend is unavailable")
	}
	return backend.cleanup.SaveStrictMigrationCleanupIntent(intent)
}

func (backend *stage4StrictProcessKillBlockingBackend) LoadStrictMigrationCleanupIntent(runID, epochID string) (state.StrictMigrationCleanupIntent, bool, error) {
	if backend.cleanup == nil {
		return state.StrictMigrationCleanupIntent{}, false, errors.New("strict process-kill cleanup backend is unavailable")
	}
	return backend.cleanup.LoadStrictMigrationCleanupIntent(runID, epochID)
}

func (backend *stage4StrictProcessKillBlockingBackend) SaveStrictSnapshotEvidence(
	evidence state.StrictSnapshotEvidence,
) error {
	if err := backend.Stage4StateBackend.SaveStrictSnapshotEvidence(evidence); err != nil {
		return err
	}
	backend.mu.Lock()
	backend.saved++
	saved := backend.saved
	want := backend.want
	backend.mu.Unlock()
	if saved != want {
		if saved > want {
			return fmt.Errorf("strict process-kill child saved %d evidence records, want %d", saved, want)
		}
		return nil
	}
	file, err := os.OpenFile(
		backend.eventPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("strict-evidence-durable\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	select {}
}

type stage4StrictProcessKillResumeContract string

const (
	// Table-scoped sources and PostgreSQL migration scope own a process-local
	// source view.  A hard kill necessarily requires a fresh view/evidence
	// attempt, and a committed source mutation after the kill is included.
	stage4StrictProcessKillFreshEpoch stage4StrictProcessKillResumeContract = "fresh_epoch"
	// SQL Server migration scope owns a durable database snapshot.  A hard
	// kill must reopen exactly that snapshot; a later live-source mutation is
	// intentionally excluded from resumed transfer and validation.
	stage4StrictProcessKillReuseMSSQLSnapshot stage4StrictProcessKillResumeContract = "reuse_mssql_snapshot"
)

type stage4StrictProcessKillFixture struct {
	name         string
	cfg          config.Config
	sourceEngine string
	scope        state.StrictSnapshotScope
	tables       []string
	contract     stage4StrictProcessKillResumeContract

	// Every fixture begins with these rows in each source table.  The parent
	// adds exactly one row after it has killed the child.
	initialRows int

	mutateSource      func(context.Context) error
	sourceCounts      func(context.Context) (map[string]int, error)
	targetCounts      func(context.Context) (map[string]int, error)
	beforeResume      func(context.Context, stage4StrictProcessKillEvidence) error
	afterResume       func(context.Context, stage4StrictProcessKillEvidence, stage4StrictProcessKillEvidence) error
	assertTargetEmpty func(context.Context) error
}

type stage4StrictProcessKillEvidence struct {
	work     map[state.TaskKey]state.WorkTask
	evidence map[state.TaskKey]state.StrictSnapshotEvidence
	owner    *state.StrictMigrationSnapshot
}

// TestStage4StrictProcessKillResumeMatrixLive proves the smallest reusable
// real process-loss matrix for every currently admitted strict source/scope
// contract.  It deliberately uses capability representatives rather than a
// target cross-product: writer/validation targets have independent live
// coverage, whereas this matrix owns the source-view survival rule.
//
// PostgreSQL table/migration use a verified-TLS PostgreSQL target; SQL Server
// table/migration use a verified-TLS SQL Server target; MySQL/MariaDB and
// SQLite table scope use the verified-TLS PostgreSQL writer.  Both state
// backends are included because strict evidence and migration owners are
// durable protocol authority, not merely test bookkeeping.
func TestStage4StrictProcessKillResumeMatrixLive(t *testing.T) {
	cells := []struct {
		name  string
		setup func(*testing.T) stage4StrictProcessKillFixture
	}{
		{
			name: "postgres_table",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillPostgresFixture(t, config.StrictConsistencyTable)
			},
		},
		{
			name: "postgres_migration",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillPostgresFixture(t, config.StrictConsistencyMigration)
			},
		},
		{
			name: "mssql_table",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillMSSQLFixture(t, config.StrictConsistencyTable)
			},
		},
		{
			name: "mssql_migration",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillMSSQLFixture(t, config.StrictConsistencyMigration)
			},
		},
		{
			name: "mysql_table",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillMySQLFixture(t, mysqlStrictLiveFixture{
					name:      "MySQL",
					dsnEnv:    "DMTX_TEST_MYSQL_DSN",
					caEnv:     "DMTX_TEST_MYSQL_CA",
					tlsName:   "dmtx_test",
					engine:    StrictConsistencyMySQL,
					sslServer: "localhost",
					collation: "utf8mb4_0900_bin",
				})
			},
		},
		{
			name: "mariadb_table",
			setup: func(t *testing.T) stage4StrictProcessKillFixture {
				return stage4StrictProcessKillMySQLFixture(t, mysqlStrictLiveFixture{
					name:      "MariaDB",
					dsnEnv:    "DMTX_TEST_MARIADB_DSN",
					caEnv:     "DMTX_TEST_MARIADB_CA",
					tlsName:   "dmtx_mariadb_test",
					engine:    StrictConsistencyMariaDB,
					sslServer: "localhost",
					collation: "utf8mb4_nopad_bin",
				})
			},
		},
		{
			name:  "sqlite_table",
			setup: stage4StrictProcessKillSQLiteFixture,
		},
	}
	for _, cell := range cells {
		cell := cell
		for _, stateKind := range []string{"yaml", "sqlite"} {
			stateKind := stateKind
			t.Run(cell.name+"/"+stateKind, func(t *testing.T) {
				stage4RunStrictProcessKillResume(t, cell.setup(t), stateKind)
			})
		}
	}
}

// TestStage4StrictProcessKillResumeHelperProcess is invoked only by the
// parent matrix.  Its credentials live in a 0600 JSON config file, and every
// parent diagnostic intentionally withholds arbitrary child output.
func TestStage4StrictProcessKillResumeHelperProcess(t *testing.T) {
	if os.Getenv(stage4StrictProcessKillCaseEnv) == "" {
		return
	}
	stage4WriteStrictProcessKillStatus("started")
	encoded, err := os.ReadFile(os.Getenv(stage4StrictProcessKillConfigEnv))
	if err != nil {
		stage4WriteStrictProcessKillStatus("read-config-failed")
		t.Fatal("read strict process-kill route configuration")
	}
	var cfg config.Config
	if err := json.Unmarshal(encoded, &cfg); err != nil {
		stage4WriteStrictProcessKillStatus("decode-config-failed")
		t.Fatal("decode strict process-kill route configuration")
	}
	want, err := strconv.Atoi(os.Getenv(stage4StrictProcessKillEvidenceEnv))
	if err != nil || want < 1 {
		stage4WriteStrictProcessKillStatus("invalid-evidence-count")
		t.Fatal("strict process-kill child requires a positive evidence count")
	}
	store, err := stage4StrictProcessKillOpenStore(
		os.Getenv(stage4StrictProcessKillStateEnv),
		os.Getenv(stage4StrictProcessKillPathEnv),
	)
	if err != nil {
		stage4WriteStrictProcessKillStatus("open-state-failed")
		t.Fatal("open strict process-kill state")
	}
	spool, runID := os.Getenv(stage4StrictProcessKillSpoolEnv), os.Getenv(stage4StrictProcessKillRunEnv)
	if spool == "" || runID == "" {
		stage4WriteStrictProcessKillStatus("missing-run-context")
		t.Fatal("strict process-kill child lacks run context")
	}
	blocking := &stage4StrictProcessKillBlockingBackend{
		Stage4StateBackend: store,
		eventPath:          os.Getenv(stage4StrictProcessKillEventEnv),
		want:               want,
	}
	cleanup, supported := any(store).(state.StrictMigrationCleanupBackend)
	if !supported || cleanup == nil {
		stage4WriteStrictProcessKillStatus("cleanup-state-unsupported")
		t.Fatal("strict process-kill child lacks SQL Server migration cleanup state")
	}
	blocking.cleanup = cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        blocking,
			SpoolDirectory: spool,
		},
	}
	stage4WriteStrictProcessKillStatus("route-dispatched")
	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		stage4WriteStrictProcessKillStatus("route-error-" + string(ClassifyTransferError(err)))
		t.Fatalf("strict route returned before process-kill evidence boundary [class=%s]", ClassifyTransferError(err))
	}
	stage4WriteStrictProcessKillStatus(fmt.Sprintf("route-completed-%d-%d", result.Tables, result.Rows))
	t.Fatal("strict route completed before process-kill evidence boundary")
}

func stage4RunStrictProcessKillResume(
	t *testing.T,
	fixture stage4StrictProcessKillFixture,
	stateKind string,
) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("resolve strict process-kill test directory")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal("protect strict process-kill test directory")
	}
	statePath := filepath.Join(root, "state."+stateKind)
	configPath := filepath.Join(root, "route.json")
	eventPath := filepath.Join(root, "strict-evidence-durable")
	statusPath := filepath.Join(root, "child-status")
	encoded, err := json.Marshal(fixture.cfg)
	if err != nil {
		t.Fatalf("encode strict process-kill %s config: %T", fixture.name, err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write private strict process-kill route config")
	}
	runID := "stage4-strict-process-kill-" + fixture.name + "-" + stateKind + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	spool := stage4StrictProcessKillSpool(t, root, runID)
	store, err := stage4StrictProcessKillOpenStore(stateKind, statePath)
	if err != nil {
		t.Fatalf("open %s strict process-kill state: %T", stateKind, err)
	}
	stage4StrictProcessKillInitializeRun(t, store, runID, fixture.cfg)

	command := exec.Command(os.Args[0], "-test.run=^TestStage4StrictProcessKillResumeHelperProcess$")
	command.Env = append(os.Environ(),
		stage4StrictProcessKillCaseEnv+"="+fixture.name,
		stage4StrictProcessKillStateEnv+"="+stateKind,
		stage4StrictProcessKillPathEnv+"="+statePath,
		stage4StrictProcessKillConfigEnv+"="+configPath,
		stage4StrictProcessKillRunEnv+"="+runID,
		stage4StrictProcessKillSpoolEnv+"="+spool,
		stage4StrictProcessKillEventEnv+"="+eventPath,
		stage4StrictProcessKillStatusEnv+"="+statusPath,
		stage4StrictProcessKillEvidenceEnv+"="+strconv.Itoa(len(fixture.tables)),
	)
	var childOutput bytes.Buffer
	command.Stdout, command.Stderr = &childOutput, &childOutput
	if err := command.Start(); err != nil {
		t.Fatal("start strict process-kill child")
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
	stage4WaitForStrictProcessKillBoundary(t, eventPath, statusPath, wait, &reaped, &childOutput)
	if err := command.Process.Kill(); err != nil {
		waitErr := <-wait
		reaped = true
		t.Fatalf("kill strict process-kill child: %T / %T", err, waitErr)
	}
	if err := <-wait; err == nil {
		reaped = true
		t.Fatal("strict process-kill child exited normally instead of being killed")
	}
	reaped = true
	_ = childOutput // Never print arbitrary child output: config includes fixture credentials.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stage4AssertStrictProcessKillActiveRun(t, store, runID)
	before := stage4LoadStrictProcessKillEvidence(t, store, runID, fixture, fixture.initialRows)
	if fixture.assertTargetEmpty != nil {
		if err := fixture.assertTargetEmpty(ctx); err != nil {
			t.Fatalf("hard-killed %s target was changed before resume: %v", fixture.name, err)
		}
	}
	if fixture.beforeResume != nil {
		if err := fixture.beforeResume(ctx, before); err != nil {
			t.Fatalf("hard-killed %s pre-resume authority: %v", fixture.name, err)
		}
	}
	if err := fixture.mutateSource(ctx); err != nil {
		t.Fatalf("mutate %s source after hard kill: %v", fixture.name, err)
	}
	if counts, err := fixture.sourceCounts(ctx); err != nil || !stage4StrictProcessKillEveryCount(counts, fixture.tables, fixture.initialRows+1) {
		t.Fatalf("hard-killed %s source mutation counts=%v err=%v", fixture.name, counts, err)
	}

	resumeEvents := make([]string, 0)
	resumeRun := Stage4RunContext{RunID: runID, Backend: store, Resume: true, SpoolDirectory: spool}
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    resumeRun,
	}
	result, err := ExecuteResume(ctx, fixture.cfg, CompletedTableCheckpoints{}, resumeObserver)
	if err != nil {
		t.Fatalf("resume hard-killed %s strict route: %T", fixture.name, err)
	}
	wantRows := fixture.initialRows + 1
	if fixture.contract == stage4StrictProcessKillReuseMSSQLSnapshot {
		wantRows = fixture.initialRows
	}
	wantResult := Result{Tables: len(fixture.tables), Rows: len(fixture.tables) * wantRows, Validated: true}
	if result != wantResult {
		t.Fatalf("resumed hard-killed %s strict result=%#v, want %#v", fixture.name, result, wantResult)
	}
	published, err := PublishStage4RunCompletion(context.Background(), resumeRun, "strict process-kill resume completed", time.Now().UTC())
	if err != nil {
		t.Fatalf("publish resumed hard-killed %s strict run class=%s", fixture.name, ClassifyTransferError(err))
	}
	// Strict runners own durable work/evidence but currently use the ordinary
	// application terminal-publication fallback rather than an aggregate table
	// inventory. Mirror that public lifecycle exactly: a false/nil aggregate
	// result must append a terminal success for the *same* run ID, never leave
	// an active run behind or manufacture a second run.
	if !published {
		active, found, loadErr := store.Latest()
		if loadErr != nil || !found || active.ID != runID || active.Outcome != state.Running || !active.Resumable {
			t.Fatalf("strict %s aggregate-fallback state=%#v found=%t err=%v", fixture.name, active, found, loadErr)
		}
		if err := store.Append(state.Run{
			ID: runID, Source: active.Source, Target: active.Target,
			Outcome: state.Success, Resumable: false,
			Reason: "strict process-kill resume completed", StartedAt: active.StartedAt,
			EndedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("complete resumed hard-killed %s strict run through application fallback: %v", fixture.name, err)
		}
	}
	after := stage4LoadStrictProcessKillEvidence(t, store, runID, fixture, wantRows)
	stage4AssertStrictProcessKillResumeEvidence(t, fixture, before, after)
	if counts, err := fixture.targetCounts(ctx); err != nil || !stage4StrictProcessKillEveryCount(counts, fixture.tables, wantRows) {
		t.Fatalf("resumed %s target counts=%v err=%v, want each %d", fixture.name, counts, err, wantRows)
	}
	if fixture.afterResume != nil {
		if err := fixture.afterResume(ctx, before, after); err != nil {
			t.Fatalf("resumed %s strict cleanup/evidence: %v", fixture.name, err)
		}
	}
	stage4AssertStrictProcessKillCompletedRun(t, store, runID)
}

func stage4WaitForStrictProcessKillBoundary(
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
	timeout := time.NewTimer(stage4StrictProcessKillBoundaryLimit)
	defer timeout.Stop()
	for {
		if _, err := os.Stat(eventPath); err == nil {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			select {
			case err := <-wait:
				*reaped = true
				if err == nil {
					t.Fatalf("strict process-kill child completed without evidence boundary: %s", stage4StrictProcessKillChildStatus(statusPath, childOutput))
				}
				t.Fatalf("strict process-kill child exited before evidence boundary: %s", stage4StrictProcessKillChildStatus(statusPath, childOutput))
			default:
				t.Fatalf("strict process-kill child did not reach evidence boundary within %s: %s", stage4StrictProcessKillBoundaryLimit, stage4StrictProcessKillChildStatus(statusPath, childOutput))
			}
		case <-wait:
			*reaped = true
			t.Fatalf("strict process-kill child exited before evidence boundary: %s", stage4StrictProcessKillChildStatus(statusPath, childOutput))
		}
	}
}

func stage4WriteStrictProcessKillStatus(status string) {
	if path := os.Getenv(stage4StrictProcessKillStatusEnv); path != "" {
		_ = os.WriteFile(path, []byte(status), 0o600)
	}
}

func stage4StrictProcessKillChildStatus(statusPath string, output *bytes.Buffer) string {
	if value, err := os.ReadFile(statusPath); err == nil {
		return "child status=" + strings.TrimSpace(string(value))
	}
	if output == nil || strings.TrimSpace(output.String()) == "" {
		return "child output unavailable"
	}
	// The config is credential-bearing.  The child status above is the only
	// intentionally secret-free diagnostic channel.
	return "child output withheld"
}

func stage4StrictProcessKillOpenStore(kind, path string) (stage4StrictProcessKillStore, error) {
	switch kind {
	case "yaml":
		return state.YAMLStore{Path: path}, nil
	case "sqlite":
		return state.SQLiteStore{Path: path}, nil
	default:
		return nil, fmt.Errorf("unknown strict process-kill state backend")
	}
}

func stage4StrictProcessKillSpool(t *testing.T, root, runID string) string {
	t.Helper()
	parent := filepath.Join(root, "spool")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal("create strict process-kill spool parent")
	}
	path := filepath.Join(parent, stage4LifecycleRunDigest(runID))
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal("create strict process-kill spool")
	}
	return path
}

func stage4StrictProcessKillInitializeRun(t *testing.T, backend state.Backend, runID string, cfg config.Config) {
	t.Helper()
	sourceEngine, err := config.CanonicalEngine(cfg.Source.Type)
	if err != nil {
		t.Fatal("identify strict process-kill source engine")
	}
	sourceIdentity, err := stage4StrictProcessKillEndpointIdentity(cfg.Source)
	if err != nil {
		t.Fatal("identify strict process-kill source workload")
	}
	targetIdentity, err := stage4StrictProcessKillEndpointIdentity(cfg.Target)
	if err != nil {
		t.Fatal("identify strict process-kill target workload")
	}
	configurationHash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal("hash strict process-kill route configuration")
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
		t.Fatalf("initialize strict process-kill run: %T", err)
	}
}

func stage4StrictProcessKillEndpointIdentity(endpoint config.Endpoint) (string, error) {
	engineName, err := config.CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engineName == "sqlite" {
		if endpoint.Database == "" {
			return "", errors.New("SQLite database is required")
		}
		return "sqlite:" + filepath.Clean(endpoint.Database), nil
	}
	return config.NetworkEndpointWorkloadIdentity(endpoint)
}

func stage4StrictProcessKillConfig(source, target config.Endpoint, tables []string, scope string) config.Config {
	return config.Config{
		Source: source,
		Target: target,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      append([]string(nil), tables...),
			ConnectionLimit:    4,
			Workers:            2,
			ChunkSize:          1,
			Partitions:         1,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			ReadAhead:          1,
			MemoryCeilingBytes: 64 << 20,
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationCountOnly,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes:                config.DeletePolicy{Mode: config.DeleteModeOff},
			StrictConsistency:      true,
			StrictConsistencyScope: scope,
		},
	}
}

func stage4AssertStrictProcessKillActiveRun(t *testing.T, store stage4StrictProcessKillStore, runID string) {
	t.Helper()
	runs, err := store.List()
	if err != nil || len(runs) != 1 || runs[0].ID != runID ||
		runs[0].Outcome != state.Running || !runs[0].Resumable || !runs[0].EndedAt.IsZero() {
		t.Fatalf("hard-killed strict run %q is not truthfully active", runID)
	}
}

// stage4LoadStrictProcessKillEvidence derives each opaque evidence ID from
// the *durable* work attempts.  A process-kill resume must never quietly
// assume attempt zero after work has been replayed or topology rebound.
func stage4LoadStrictProcessKillEvidence(
	t *testing.T,
	store stage4StrictProcessKillStore,
	runID string,
	fixture stage4StrictProcessKillFixture,
	wantRows int,
) stage4StrictProcessKillEvidence {
	t.Helper()
	tasks, _, err := store.ListWork(runID)
	if err != nil {
		t.Fatalf("list %s strict process-kill work: %v", fixture.name, err)
	}
	wanted := make(map[string]struct{}, len(fixture.tables))
	for _, table := range fixture.tables {
		wanted[table] = struct{}{}
	}
	result := stage4StrictProcessKillEvidence{
		work:     make(map[state.TaskKey]state.WorkTask, len(fixture.tables)),
		evidence: make(map[state.TaskKey]state.StrictSnapshotEvidence, len(fixture.tables)),
	}
	for _, item := range tasks {
		if item.Key.Type != stage4AdapterNetworkTaskType {
			continue
		}
		if _, selected := wanted[item.Key.Table]; !selected {
			continue
		}
		// WorkTask.Attempts advances while network ranges are delivered, after
		// strict evidence was already persisted.  Search the bounded durable
		// attempt domain instead of assuming attempt zero; exactly one current
		// topology record is required.  This also catches a stale or ambiguous
		// strict authority rather than silently selecting one.
		record, attempt, found, loadErr := stage4FindStrictProcessKillEvidence(store, runID, item)
		if loadErr != nil || !found || record.RunID != runID ||
			record.Task != item.Key || record.AttemptID != attempt ||
			record.SourceEngine != fixture.sourceEngine ||
			record.Scope != fixture.scope || record.ExactSourceRowCount != int64(wantRows) ||
			record.ProcessEpoch == "" || record.SnapshotReference == "" || record.CapturedAt.IsZero() {
			t.Fatalf("strict %s evidence for %s work=%#v attempt=%q record=%#v found=%t err=%v", fixture.name, item.Key.Table, item, attempt, record, found, loadErr)
		}
		if _, duplicate := result.work[item.Key]; duplicate {
			t.Fatalf("strict %s durable work duplicates %s", fixture.name, item.Key.Table)
		}
		result.work[item.Key] = item
		result.evidence[item.Key] = record
	}
	if len(result.work) != len(fixture.tables) {
		t.Fatalf("strict %s durable work has %d selected tables, want %d", fixture.name, len(result.work), len(fixture.tables))
	}
	if fixture.scope == state.StrictSnapshotMigration {
		owner, found, ownerErr := store.LoadLatestStrictMigrationSnapshot(runID)
		if ownerErr != nil || !found || owner.RunID != runID || owner.EpochID == "" ||
			owner.SnapshotReference == "" || owner.ProcessEpoch == "" || owner.CapturedAt.IsZero() {
			t.Fatalf("strict %s migration owner=%#v found=%t err=%v", fixture.name, owner, found, ownerErr)
		}
		result.owner = &owner
		for _, record := range result.evidence {
			if record.MigrationEpochID != owner.EpochID || record.SnapshotReference != owner.SnapshotReference ||
				!record.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) {
				t.Fatalf("strict %s migration evidence is not bound to its durable owner: evidence=%#v owner=%#v", fixture.name, record, owner)
			}
		}
	} else {
		for _, record := range result.evidence {
			if record.MigrationEpochID != "" {
				t.Fatalf("strict %s table evidence unexpectedly has migration epoch %q", fixture.name, record.MigrationEpochID)
			}
		}
	}
	return result
}

func stage4FindStrictProcessKillEvidence(
	store stage4StrictProcessKillStore,
	runID string,
	work state.WorkTask,
) (state.StrictSnapshotEvidence, string, bool, error) {
	if work.Attempts < 0 {
		return state.StrictSnapshotEvidence{}, "", false, errors.New("strict work attempts is negative")
	}
	var (
		foundRecord  state.StrictSnapshotEvidence
		foundAttempt string
		found        bool
	)
	for durableAttempt := 0; durableAttempt <= work.Attempts; durableAttempt++ {
		attemptID, err := BuildStrictConsistencyAttemptID(work.Key, work.TopologyHash, durableAttempt)
		if err != nil {
			return state.StrictSnapshotEvidence{}, "", false, err
		}
		record, present, err := store.LoadStrictSnapshotEvidence(runID, work.Key, attemptID)
		if err != nil {
			return state.StrictSnapshotEvidence{}, "", false, err
		}
		if !present {
			continue
		}
		if found {
			return state.StrictSnapshotEvidence{}, "", false, fmt.Errorf("strict work has multiple current-topology evidence attempts (%q and %q)", foundAttempt, attemptID)
		}
		foundRecord, foundAttempt, found = record, attemptID, true
	}
	return foundRecord, foundAttempt, found, nil
}

func stage4AssertStrictProcessKillResumeEvidence(
	t *testing.T,
	fixture stage4StrictProcessKillFixture,
	before stage4StrictProcessKillEvidence,
	after stage4StrictProcessKillEvidence,
) {
	t.Helper()
	if len(before.evidence) != len(after.evidence) {
		t.Fatalf("strict %s evidence table count changed from %d to %d", fixture.name, len(before.evidence), len(after.evidence))
	}
	switch fixture.contract {
	case stage4StrictProcessKillFreshEpoch:
		for task, oldRecord := range before.evidence {
			newRecord, found := after.evidence[task]
			if !found {
				t.Fatalf("strict %s resume lost durable table identity %s", fixture.name, task.Table)
			}
			if newRecord.ExactSourceRowCount != int64(fixture.initialRows+1) {
				t.Fatalf("strict %s resumed evidence for %s count=%d, want %d", fixture.name, task.Table, newRecord.ExactSourceRowCount, fixture.initialRows+1)
			}
			if newRecord.AttemptID == oldRecord.AttemptID ||
				newRecord.ProcessEpoch == oldRecord.ProcessEpoch ||
				newRecord.SnapshotReference == oldRecord.SnapshotReference {
				t.Fatalf("strict %s resumed %s reused dead process-local evidence: old=%#v new=%#v", fixture.name, task.Table, oldRecord, newRecord)
			}
		}
		if fixture.scope == state.StrictSnapshotMigration {
			if before.owner == nil || after.owner == nil ||
				before.owner.EpochID == after.owner.EpochID ||
				before.owner.SnapshotReference == after.owner.SnapshotReference ||
				before.owner.ProcessEpoch == after.owner.ProcessEpoch {
				t.Fatalf("strict %s migration resume did not replace the dead PostgreSQL epoch: old=%#v new=%#v", fixture.name, before.owner, after.owner)
			}
		}
	case stage4StrictProcessKillReuseMSSQLSnapshot:
		if before.owner == nil || after.owner == nil ||
			before.owner.EpochID != after.owner.EpochID ||
			before.owner.SnapshotReference != after.owner.SnapshotReference ||
			!before.owner.CapturedAt.UTC().Equal(after.owner.CapturedAt.UTC()) {
			t.Fatalf("strict %s migration resume did not retain the durable SQL Server snapshot owner: old=%#v new=%#v", fixture.name, before.owner, after.owner)
		}
		for task, oldRecord := range before.evidence {
			newRecord, found := after.evidence[task]
			if !found || newRecord.AttemptID != oldRecord.AttemptID ||
				newRecord.SnapshotReference != oldRecord.SnapshotReference ||
				newRecord.MigrationEpochID != oldRecord.MigrationEpochID ||
				!newRecord.CapturedAt.UTC().Equal(oldRecord.CapturedAt.UTC()) ||
				newRecord.ExactSourceRowCount != oldRecord.ExactSourceRowCount {
				t.Fatalf("strict %s migration resume did not reuse exact snapshot evidence for %s: old=%#v new=%#v", fixture.name, task.Table, oldRecord, newRecord)
			}
		}
	default:
		t.Fatalf("strict %s has unknown process-kill resume contract %q", fixture.name, fixture.contract)
	}
}

func stage4StrictProcessKillEveryCount(counts map[string]int, tables []string, want int) bool {
	if len(counts) != len(tables) {
		return false
	}
	for _, table := range tables {
		if counts[table] != want {
			return false
		}
	}
	return true
}

func stage4AssertStrictProcessKillCompletedRun(t *testing.T, store stage4StrictProcessKillStore, runID string) {
	t.Helper()
	latest, found, err := store.Latest()
	if err != nil || !found || latest.ID != runID || latest.Outcome != state.Success || latest.Resumable || latest.EndedAt.IsZero() {
		t.Fatalf("resumed strict process-kill run is not the original successful terminal run: %#v found=%t err=%v", latest, found, err)
	}
	runs, err := store.List()
	if err != nil || len(runs) == 0 {
		t.Fatalf("strict process-kill resume cannot read original-run history: %#v err=%v", runs, err)
	}
	successes := 0
	for _, record := range runs {
		if record.ID != runID {
			t.Fatalf("strict process-kill resume manufactured a second run: %#v", runs)
		}
		if record.Outcome == state.Success {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("strict process-kill resume recorded %d terminal-success facts for the original run: %#v", successes, runs)
	}
}

func stage4StrictProcessKillPostgresFixture(t *testing.T, scope string) stage4StrictProcessKillFixture {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN for PostgreSQL strict process-kill coverage")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL strict process-kill DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	sourceDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL strict process-kill source: %v", err)
	}
	sourceDB.SetMaxOpenConns(12)
	t.Cleanup(func() { _ = sourceDB.Close() })
	if err := sourceDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL strict process-kill source: %v", err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceSchema := "dmtx_s4_strict_kill_src_" + suffix
	targetSchema := "dmtx_s4_strict_kill_tgt_" + suffix
	targetDatabaseName := "dmtx_s4_strict_kill_" + suffix
	if _, err := sourceDB.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(sourceSchema)); err != nil {
		t.Fatalf("create PostgreSQL strict process-kill source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := sourceDB.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+postgresIdentifier(sourceSchema)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL strict process-kill source schema: %v", err)
		}
	})
	if _, err := sourceDB.ExecContext(ctx, "CREATE DATABASE "+postgresIdentifier(targetDatabaseName)); err != nil {
		t.Fatalf("create PostgreSQL strict process-kill target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if _, err := sourceDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+postgresIdentifier(targetDatabaseName)+" WITH (FORCE)"); err != nil {
			t.Errorf("drop PostgreSQL strict process-kill target database: %v", err)
		}
	})
	sourceEndpoint := config.Endpoint{
		Type: "postgres", Host: parsed.Host, Port: int(parsed.Port), Database: parsed.Database,
		User: parsed.User, Password: parsed.Password, Schema: sourceSchema,
		SSLMode: "verify-full", TLSCAFile: caFile,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database, targetEndpoint.Schema = targetDatabaseName, targetSchema
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build PostgreSQL strict process-kill target DSN: %v", err)
	}
	targetDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL strict process-kill target: %v", err)
	}
	targetDB.SetMaxOpenConns(12)
	t.Cleanup(func() { _ = targetDB.Close() })
	if err := targetDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL strict process-kill target: %v", err)
	}
	if _, err := targetDB.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(targetSchema)); err != nil {
		t.Fatalf("create PostgreSQL strict process-kill target schema: %v", err)
	}
	tables := []string{"items_" + suffix}
	if scope == config.StrictConsistencyMigration {
		tables = append(tables, "events_"+suffix)
	}
	for _, table := range tables {
		ddl := " (id bigint NOT NULL PRIMARY KEY, payload text NOT NULL)"
		for _, database := range []*sql.DB{sourceDB, targetDB} {
			schemaName := sourceSchema
			if database == targetDB {
				schemaName = targetSchema
			}
			if _, err := database.ExecContext(ctx, "CREATE TABLE "+postgresQualified(schemaName, table)+ddl); err != nil {
				t.Fatalf("create PostgreSQL strict process-kill table %s: %v", table, err)
			}
		}
		if _, err := sourceDB.ExecContext(ctx, "INSERT INTO "+postgresQualified(sourceSchema, table)+" (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three')"); err != nil {
			t.Fatalf("seed PostgreSQL strict process-kill source %s: %v", table, err)
		}
	}
	fixture := stage4StrictProcessKillFixture{
		name:         "postgres_" + scope,
		cfg:          stage4StrictProcessKillConfig(sourceEndpoint, targetEndpoint, tables, scope),
		sourceEngine: "postgres",
		scope:        state.StrictSnapshotScope(scope),
		tables:       tables,
		contract:     stage4StrictProcessKillFreshEpoch,
		initialRows:  3,
		mutateSource: func(mutateCtx context.Context) error {
			for _, table := range tables {
				if _, err := sourceDB.ExecContext(mutateCtx, "INSERT INTO "+postgresQualified(sourceSchema, table)+" (id, payload) VALUES (4, 'after-kill')"); err != nil {
					return err
				}
			}
			return nil
		},
		sourceCounts: stage4StrictProcessKillPostgresCounts(sourceDB, sourceSchema, tables),
		targetCounts: stage4StrictProcessKillPostgresCounts(targetDB, targetSchema, tables),
		assertTargetEmpty: func(checkCtx context.Context) error {
			counts, err := stage4StrictProcessKillPostgresCounts(targetDB, targetSchema, tables)(checkCtx)
			if err != nil || !stage4StrictProcessKillEveryCount(counts, tables, 0) {
				return fmt.Errorf("target counts=%v err=%v", counts, err)
			}
			return nil
		},
		afterResume: func(checkCtx context.Context, _ stage4StrictProcessKillEvidence, after stage4StrictProcessKillEvidence) error {
			for _, record := range after.evidence {
				assertPostgresStrictSnapshotReleased(t, checkCtx, sourceDB, record.SnapshotReference)
			}
			return nil
		},
	}
	return fixture
}

func stage4StrictProcessKillPostgresCounts(database *sql.DB, schemaName string, tables []string) func(context.Context) (map[string]int, error) {
	return func(ctx context.Context) (map[string]int, error) {
		counts := make(map[string]int, len(tables))
		for _, table := range tables {
			var count int
			if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(schemaName, table)).Scan(&count); err != nil {
				return nil, err
			}
			counts[table] = count
		}
		return counts, nil
	}
}

func stage4StrictProcessKillMSSQLFixture(t *testing.T, scope string) stage4StrictProcessKillFixture {
	t.Helper()
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
	// Register verification first so LIFO cleanup audits after both databases
	// have been dropped, even when a child is killed in the middle of a route.
	t.Cleanup(func() {
		assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
		assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
	})
	sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
	targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)
	sourceDB := openSQLServerNativeLiveDatabase(t, ctx, "strict process-kill source", sourceEndpoint)
	targetDB := openSQLServerNativeLiveDatabase(t, ctx, "strict process-kill target", targetEndpoint)
	tables := []string{"items_" + strconv.FormatInt(time.Now().UnixNano(), 36)}
	if scope == config.StrictConsistencyMigration {
		tables = append(tables, "events_"+strconv.FormatInt(time.Now().UnixNano(), 36))
	}
	for _, table := range tables {
		ddl := " ([id] BIGINT NOT NULL PRIMARY KEY, [payload] VARCHAR(40) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL)"
		for _, fixture := range []struct {
			database *sql.DB
			schema   string
		}{
			{sourceDB, sourceEndpoint.Schema},
			{targetDB, targetEndpoint.Schema},
		} {
			if _, err := fixture.database.ExecContext(ctx, "CREATE TABLE "+sqlServerQualified(fixture.schema, table)+ddl); err != nil {
				t.Fatalf("create SQL Server strict process-kill table %s: %v", table, err)
			}
		}
		if _, err := sourceDB.ExecContext(ctx, "INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, table)+" ([id], [payload]) VALUES (1, 'one'), (2, 'two'), (3, 'three')"); err != nil {
			t.Fatalf("seed SQL Server strict process-kill source %s: %v", table, err)
		}
	}
	contract := stage4StrictProcessKillFreshEpoch
	if scope == config.StrictConsistencyMigration {
		contract = stage4StrictProcessKillReuseMSSQLSnapshot
	}
	fixture := stage4StrictProcessKillFixture{
		name:         "mssql_" + scope,
		cfg:          stage4StrictProcessKillConfig(sourceEndpoint, targetEndpoint, tables, scope),
		sourceEngine: "mssql",
		scope:        state.StrictSnapshotScope(scope),
		tables:       tables,
		contract:     contract,
		initialRows:  3,
		mutateSource: func(mutateCtx context.Context) error {
			for _, table := range tables {
				if _, err := sourceDB.ExecContext(mutateCtx, "INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, table)+" ([id], [payload]) VALUES (4, 'after-kill')"); err != nil {
					return err
				}
			}
			return nil
		},
		sourceCounts: stage4StrictProcessKillMSSQLCounts(sourceDB, sourceEndpoint.Schema, tables),
		targetCounts: stage4StrictProcessKillMSSQLCounts(targetDB, targetEndpoint.Schema, tables),
		assertTargetEmpty: func(checkCtx context.Context) error {
			counts, err := stage4StrictProcessKillMSSQLCounts(targetDB, targetEndpoint.Schema, tables)(checkCtx)
			if err != nil || !stage4StrictProcessKillEveryCount(counts, tables, 0) {
				return fmt.Errorf("target counts=%v err=%v", counts, err)
			}
			return nil
		},
	}
	if scope == config.StrictConsistencyMigration {
		fixture.beforeResume = func(checkCtx context.Context, before stage4StrictProcessKillEvidence) error {
			if before.owner == nil {
				return errors.New("durable SQL Server migration snapshot owner is absent")
			}
			count, err := stage4StrictProcessKillMSSQLSnapshotCount(checkCtx, sourceEndpoint, before.owner.SnapshotReference)
			if err != nil || count != 1 {
				return fmt.Errorf("durable SQL Server migration snapshot count=%d err=%v", count, err)
			}
			return nil
		}
		fixture.afterResume = func(checkCtx context.Context, before, after stage4StrictProcessKillEvidence) error {
			if before.owner == nil || after.owner == nil {
				return errors.New("SQL Server migration snapshot owner missing after resume")
			}
			count, err := stage4StrictProcessKillMSSQLSnapshotCount(checkCtx, sourceEndpoint, before.owner.SnapshotReference)
			if err != nil || count != 0 {
				return fmt.Errorf("SQL Server migration snapshot cleanup count=%d err=%v", count, err)
			}
			return nil
		}
	}
	return fixture
}

func stage4StrictProcessKillMSSQLCounts(database *sql.DB, schemaName string, tables []string) func(context.Context) (map[string]int, error) {
	return func(ctx context.Context) (map[string]int, error) {
		counts := make(map[string]int, len(tables))
		for _, table := range tables {
			var count int
			if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(schemaName, table)).Scan(&count); err != nil {
				return nil, err
			}
			counts[table] = count
		}
		return counts, nil
	}
}

func stage4StrictProcessKillMSSQLSnapshotCount(ctx context.Context, source config.Endpoint, snapshot string) (int, error) {
	adminEndpoint := source
	adminEndpoint.Database = "master"
	admin, err := engine.OpenSQLServer(ctx, adminEndpoint)
	if err != nil {
		return 0, err
	}
	defer admin.Close()
	var count int
	err = admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM sys.databases WHERE name = @p1", snapshot).Scan(&count)
	return count, err
}

func stage4StrictProcessKillMySQLFixture(t *testing.T, fixture mysqlStrictLiveFixture) stage4StrictProcessKillFixture {
	t.Helper()
	if os.Getenv("DMTX_TEST_POSTGRES_DSN") == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN and MySQL-family TLS variables for strict process-kill coverage")
	}
	sourceDB, sourceNamespace, table := openMySQLStrictLiveSource(t, fixture)
	parsedSource := parseMySQLNativeTargetDSNForTLS(t, fixture.name+" strict process-kill source", os.Getenv(fixture.dsnEnv), fixture.tlsName)
	sourceEndpoint := mysqlNativeTargetEndpoint(t, parsedSource, os.Getenv(fixture.caEnv))
	// MariaDB is detected from the verified server, while the production
	// registry's canonical source role is "mysql". Keeping the canonical
	// endpoint type here makes this an Execute-path proof of the admitted
	// MariaDB flavor rather than a direct-adapter-only test.
	pgDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	parsedPG, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL strict process-kill target DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(parsedPG) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsedPG.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	targetDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL MySQL-family strict process-kill target: %v", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })
	if err := targetDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL MySQL-family strict process-kill target: %v", err)
	}
	targetNamespace := "dmtx_s4_mysql_kill_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := targetDB.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(targetNamespace)); err != nil {
		t.Fatalf("create PostgreSQL MySQL-family strict process-kill schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := targetDB.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+postgresIdentifier(targetNamespace)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL MySQL-family strict process-kill schema: %v", err)
		}
	})
	if _, err := targetDB.ExecContext(ctx, "CREATE TABLE "+postgresQualified(targetNamespace, table)+" (id bigint PRIMARY KEY, payload character varying(40) NOT NULL)"); err != nil {
		t.Fatalf("create PostgreSQL MySQL-family strict process-kill target: %v", err)
	}
	targetEndpoint := config.Endpoint{
		Type: "postgres", Host: parsedPG.Host, Port: int(parsedPG.Port), Database: parsedPG.Database,
		User: parsedPG.User, Password: parsedPG.Password, Schema: targetNamespace,
		SSLMode: "verify-full", TLSCAFile: caFile,
	}
	if sourceEndpoint.SSLMode != "verify-full" || sourceEndpoint.TLSCAFile == "" ||
		targetEndpoint.SSLMode != "verify-full" || targetEndpoint.TLSCAFile != caFile {
		t.Fatal("MySQL-family strict process-kill endpoint lost verified TLS authority")
	}
	name := "mysql_table"
	if fixture.engine == StrictConsistencyMariaDB {
		name = "mariadb_table"
	}
	quotedSource := mySQLQualified(sourceNamespace, table)
	return stage4StrictProcessKillFixture{
		name:         name,
		cfg:          stage4StrictProcessKillConfig(sourceEndpoint, targetEndpoint, []string{table}, config.StrictConsistencyTable),
		sourceEngine: "mysql",
		scope:        state.StrictSnapshotTable,
		tables:       []string{table},
		contract:     stage4StrictProcessKillFreshEpoch,
		initialRows:  3,
		mutateSource: func(mutateCtx context.Context) error {
			_, err := sourceDB.ExecContext(mutateCtx, "INSERT INTO "+quotedSource+" (id, payload) VALUES (4, 'after-kill')")
			return err
		},
		sourceCounts: stage4StrictProcessKillMySQLCounts(sourceDB, sourceNamespace, []string{table}),
		targetCounts: stage4StrictProcessKillPostgresCounts(targetDB, targetNamespace, []string{table}),
		assertTargetEmpty: func(checkCtx context.Context) error {
			counts, err := stage4StrictProcessKillPostgresCounts(targetDB, targetNamespace, []string{table})(checkCtx)
			if err != nil || counts[table] != 0 {
				return fmt.Errorf("target count=%v err=%v", counts, err)
			}
			return nil
		},
	}
}

func stage4StrictProcessKillMySQLCounts(database *sql.DB, namespace string, tables []string) func(context.Context) (map[string]int, error) {
	return func(ctx context.Context) (map[string]int, error) {
		counts := make(map[string]int, len(tables))
		for _, table := range tables {
			var count int
			if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(namespace, table)).Scan(&count); err != nil {
				return nil, err
			}
			counts[table] = count
		}
		return counts, nil
	}
}

func stage4StrictProcessKillSQLiteFixture(t *testing.T) stage4StrictProcessKillFixture {
	t.Helper()
	pgDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if pgDSN == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN for SQLite strict process-kill coverage")
	}
	parsedPG, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL SQLite strict process-kill target DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(parsedPG) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsedPG.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	sourceSetup, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open SQLite strict process-kill source setup: %v", err)
	}
	if _, err := sourceSetup.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`); err != nil {
		_ = sourceSetup.Close()
		t.Fatalf("seed SQLite strict process-kill source: %v", err)
	}
	if err := sourceSetup.Close(); err != nil {
		t.Fatalf("close SQLite strict process-kill setup source: %v", err)
	}
	sourceWriter, err := sql.Open("sqlite", sqliteSourceTestURI(sourcePath, "rw"))
	if err != nil {
		t.Fatalf("open SQLite strict process-kill writer: %v", err)
	}
	t.Cleanup(func() { _ = sourceWriter.Close() })
	targetDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL SQLite strict process-kill target: %v", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })
	if err := targetDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL SQLite strict process-kill target: %v", err)
	}
	targetNamespace := "dmtx_s4_sqlite_kill_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := targetDB.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(targetNamespace)); err != nil {
		t.Fatalf("create PostgreSQL SQLite strict process-kill schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := targetDB.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+postgresIdentifier(targetNamespace)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL SQLite strict process-kill schema: %v", err)
		}
	})
	const table = "items"
	if _, err := targetDB.ExecContext(ctx, "CREATE TABLE "+postgresQualified(targetNamespace, table)+" (id bigint PRIMARY KEY, payload text NOT NULL)"); err != nil {
		t.Fatalf("create PostgreSQL SQLite strict process-kill target table: %v", err)
	}
	sourceEndpoint := config.Endpoint{Type: "sqlite", Database: sourcePath}
	targetEndpoint := config.Endpoint{
		Type: "postgres", Host: parsedPG.Host, Port: int(parsedPG.Port), Database: parsedPG.Database,
		User: parsedPG.User, Password: parsedPG.Password, Schema: targetNamespace,
		SSLMode: "verify-full", TLSCAFile: caFile,
	}
	if targetEndpoint.SSLMode != "verify-full" || targetEndpoint.TLSCAFile != caFile {
		t.Fatal("SQLite strict process-kill target lost verified TLS authority")
	}
	return stage4StrictProcessKillFixture{
		name:         "sqlite_table",
		cfg:          stage4StrictProcessKillConfig(sourceEndpoint, targetEndpoint, []string{table}, config.StrictConsistencyTable),
		sourceEngine: "sqlite",
		scope:        state.StrictSnapshotTable,
		tables:       []string{table},
		contract:     stage4StrictProcessKillFreshEpoch,
		initialRows:  3,
		mutateSource: func(mutateCtx context.Context) error {
			_, err := sourceWriter.ExecContext(mutateCtx, "INSERT INTO items VALUES (4, 'after-kill')")
			return err
		},
		sourceCounts: stage4StrictProcessKillSQLiteCounts(sourceWriter, []string{table}),
		targetCounts: stage4StrictProcessKillPostgresCounts(targetDB, targetNamespace, []string{table}),
		assertTargetEmpty: func(checkCtx context.Context) error {
			counts, err := stage4StrictProcessKillPostgresCounts(targetDB, targetNamespace, []string{table})(checkCtx)
			if err != nil || counts[table] != 0 {
				return fmt.Errorf("target count=%v err=%v", counts, err)
			}
			return nil
		},
	}
}

func stage4StrictProcessKillSQLiteCounts(database *sql.DB, tables []string) func(context.Context) (map[string]int, error) {
	return func(ctx context.Context) (map[string]int, error) {
		counts := make(map[string]int, len(tables))
		for _, table := range tables {
			var count int
			if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stage4SQLiteIdentifier(table)).Scan(&count); err != nil {
				return nil, err
			}
			counts[table] = count
		}
		return counts, nil
	}
}
