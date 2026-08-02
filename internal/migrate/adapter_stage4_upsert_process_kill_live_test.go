package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4UpsertProcessKillCaseEnv   = "DMTX_STAGE4_UPSERT_PROCESS_KILL_CASE"
	stage4UpsertProcessKillStateEnv  = "DMTX_STAGE4_UPSERT_PROCESS_KILL_STATE"
	stage4UpsertProcessKillPathEnv   = "DMTX_STAGE4_UPSERT_PROCESS_KILL_STATE_PATH"
	stage4UpsertProcessKillConfigEnv = "DMTX_STAGE4_UPSERT_PROCESS_KILL_CONFIG_PATH"
	stage4UpsertProcessKillRunEnv    = "DMTX_STAGE4_UPSERT_PROCESS_KILL_RUN_ID"
	stage4UpsertProcessKillSpoolEnv  = "DMTX_STAGE4_UPSERT_PROCESS_KILL_SPOOL"
	stage4UpsertProcessKillEventEnv  = "DMTX_STAGE4_UPSERT_PROCESS_KILL_EVENT"
	stage4UpsertProcessKillStatusEnv = "DMTX_STAGE4_UPSERT_PROCESS_KILL_STATUS"
)

// stage4UpsertProcessKillStore requires every durable capability an ordinary
// idempotent upsert route uses. The child intercepts only the durable range
// acknowledgement; every other call remains the real YAML or SQLite store.
type stage4UpsertProcessKillStore interface {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
}

// stage4UpsertProcessKillBlockingBackend parks the child at the one boundary
// that makes replay observable. AcknowledgeRange is reached only after the
// chunk has already been written to the real target, so a kill here leaves the
// target holding rows that durable state does not yet know about. That is the
// exact state a power loss produces, and the only state in which a resume can
// be caught duplicating or overwriting rows.
type stage4UpsertProcessKillBlockingBackend struct {
	stage4UpsertProcessKillStore
	eventPath string
}

func (backend *stage4UpsertProcessKillBlockingBackend) AcknowledgeRange(
	state.RangeAcknowledgement,
) (state.RangeState, error) {
	file, err := os.OpenFile(
		backend.eventPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return state.RangeState{}, err
	}
	if _, err := file.WriteString("upsert-chunk-written\n"); err != nil {
		_ = file.Close()
		return state.RangeState{}, err
	}
	if err := file.Close(); err != nil {
		return state.RangeState{}, err
	}
	stage4ParkUntilKilled()
	return state.RangeState{}, nil
}

// stage4UpsertProcessKillSQLiteObserver is the SQLite equivalent of the
// blocking backend above, and it exists because SQLite-to-SQLite upsert does
// not use the durable range protocol at all.
//
// Verified live 2026-08-01: with reconciliation off, that pair runs the legacy
// SQLite pipeline, which reached completion without ever calling
// AcknowledgeRange. Its durable boundary is the page checkpoint reported to the
// observer instead. Blocking the backend would therefore have produced a child
// that simply finished, and a matrix cell that silently proved nothing — so the
// cell parks here, after the page reaches the target file and before the
// checkpoint is recorded.
type stage4UpsertProcessKillSQLiteObserver struct {
	stage4AdapterObserver
	eventPath string
}

// AfterIntegerKeysetPage and AfterRowNumberPage are the durable page
// checkpoints. They are also what makes the pipeline emit page boundaries at
// all: the SQLite copy loops checkpoint only when the observer satisfies
// PageObserver, so an observer that merely watched the write boundary would
// suppress the very checkpoint it was waiting for. Both park after the page has
// been written to the target file and before the progress record survives.
func (observer *stage4UpsertProcessKillSQLiteObserver) AfterIntegerKeysetPage(
	context.Context,
	string,
	int,
	int64,
) error {
	return observer.parkAfterPageWrite()
}

func (observer *stage4UpsertProcessKillSQLiteObserver) AfterRowNumberPage(
	context.Context,
	string,
	int,
	int64,
) error {
	return observer.parkAfterPageWrite()
}

func (observer *stage4UpsertProcessKillSQLiteObserver) parkAfterPageWrite() error {
	file, err := os.OpenFile(
		observer.eventPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return err
	}
	if _, err := file.WriteString("sqlite-page-written\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	stage4ParkUntilKilled()
	return nil
}

// stage4ParkUntilKilled blocks until the parent ends this process.
//
// It deliberately sleeps rather than using select{}. A bare select{} lets the Go
// runtime declare "all goroutines are asleep - deadlock!" and abort the child
// once the route's own goroutines have finished, which kills the child before
// the parent chooses to — turning an external-kill proof into a self-terminating
// one, intermittently. A sleeping goroutine is not counted as deadlocked, so the
// child stays alive until it is actually signalled.
func stage4ParkUntilKilled() {
	for {
		time.Sleep(time.Minute)
	}
}

type stage4UpsertProcessKillFixture struct {
	cell             string
	cfg              config.Config
	targetOnlyRows   func(context.Context) (int, error)
	targetSourceRows func(context.Context) (int, error)
	targetTotalRows  func(context.Context) (int, error)
}

// TestStage4UpsertProcessKillReplayLive is the external-process replay proof for
// every admitted idempotent upsert target family.
//
// The existing crash coverage either simulates failure in-process or exercises
// one route, which cannot demonstrate that a real killed process leaves a
// recoverable target. Each child here is hard-killed by the operating system
// after a chunk reaches the target and before the durable frontier advances,
// then the original run is resumed through the ordinary public route.
//
// Deletes are deliberately off: this is the plain upsert contract. The target
// is seeded with stale rows that must be overwritten exactly once and a
// target-only row that must survive, so a resume that re-applied a chunk, or
// that rebuilt rather than merged, fails rather than passing quietly.
func TestStage4UpsertProcessKillReplayLive(t *testing.T) {
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
		{name: "sqlite", setup: func(t *testing.T) stage4UpsertProcessKillFixture {
			return stage4UpsertProcessKillFromDeleteFixture(
				stage4DeleteProcessKillSQLiteFixture(t),
			)
		}},
	}
	for _, cell := range cells {
		cell := cell
		for _, stateKind := range []string{"yaml", "sqlite"} {
			stateKind := stateKind
			t.Run(cell.name+"/"+stateKind, func(t *testing.T) {
				fixture := cell.setup(t)
				stage4RunUpsertProcessKillReplay(t, fixture, stateKind)
			})
		}
	}
}

// stage4UpsertProcessKillFromDeleteFixture reuses the seeded live topology of
// the delete process-kill fixtures and turns reconciliation off. The seeding is
// exactly what an upsert replay proof needs — stale target rows to overwrite
// and one target-only row to retain — and sharing it keeps the two matrices
// from drifting apart as engines are added.
func stage4UpsertProcessKillFromDeleteFixture(
	fixture stage4DeleteProcessKillFixture,
) stage4UpsertProcessKillFixture {
	cfg := fixture.cfg
	cfg.Migration.Deletes = config.DeletePolicy{Mode: config.DeleteModeOff}
	return stage4UpsertProcessKillFixture{
		cell:             fixture.cell,
		cfg:              cfg,
		targetOnlyRows:   fixture.targetOnlyRows,
		targetSourceRows: fixture.targetSourceRows,
	}
}

// TestStage4UpsertProcessKillReplayHelperProcess is invoked only by the parent
// test. Credential-bearing configuration travels in a mode-0600 file rather
// than the environment, and the child never prints it.
func TestStage4UpsertProcessKillReplayHelperProcess(t *testing.T) {
	cell := os.Getenv(stage4UpsertProcessKillCaseEnv)
	if cell == "" {
		return
	}
	stage4WriteUpsertProcessKillStatus("started")
	encoded, err := os.ReadFile(os.Getenv(stage4UpsertProcessKillConfigEnv))
	if err != nil {
		stage4WriteUpsertProcessKillStatus("read-config-failed")
		t.Fatal("read process-kill upsert route configuration")
	}
	var cfg config.Config
	if err := json.Unmarshal(encoded, &cfg); err != nil {
		stage4WriteUpsertProcessKillStatus("decode-config-failed")
		t.Fatal("decode process-kill upsert route configuration")
	}
	stage4WriteUpsertProcessKillStatus("config-decoded")
	store, err := stage4UpsertProcessKillOpenStore(
		os.Getenv(stage4UpsertProcessKillStateEnv),
		os.Getenv(stage4UpsertProcessKillPathEnv),
	)
	if err != nil {
		stage4WriteUpsertProcessKillStatus("open-state-failed")
		t.Fatal("open process-kill upsert state")
	}
	blocking := &stage4UpsertProcessKillBlockingBackend{
		stage4UpsertProcessKillStore: store,
		eventPath:                    os.Getenv(stage4UpsertProcessKillEventEnv),
	}
	spool := os.Getenv(stage4UpsertProcessKillSpoolEnv)
	if spool == "" || os.Getenv(stage4UpsertProcessKillRunEnv) == "" {
		stage4WriteUpsertProcessKillStatus("missing-run-context")
		t.Fatal("process-kill upsert child lacks run context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          os.Getenv(stage4UpsertProcessKillRunEnv),
			Backend:        blocking,
			SpoolDirectory: spool,
		},
	}
	// Each engine is parked at its own durable boundary; see the SQLite
	// observer above for why one cell cannot use the backend seam.
	var dispatch TableObserver = observer
	if cell == "sqlite" {
		dispatch = &stage4UpsertProcessKillSQLiteObserver{
			stage4AdapterObserver: observer,
			eventPath:             os.Getenv(stage4UpsertProcessKillEventEnv),
		}
	}
	stage4WriteUpsertProcessKillStatus("route-dispatched")
	result, err := stage4DeleteProcessKillExecute(ctx, cell, cfg, dispatch)
	if err != nil {
		stage4WriteUpsertProcessKillStatus(
			"route-error-" + string(ClassifyTransferError(err)),
		)
		t.Fatalf(
			"upsert route returned before the acknowledgement boundary [class=%s]",
			ClassifyTransferError(err),
		)
	}
	stage4WriteUpsertProcessKillStatus(
		fmt.Sprintf("route-completed-%d-%d", result.Tables, result.Rows),
	)
	t.Fatal("upsert route completed before the acknowledgement boundary")
}

func stage4RunUpsertProcessKillReplay(
	t *testing.T,
	fixture stage4UpsertProcessKillFixture,
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
	eventPath := filepath.Join(root, "upsert-chunk-written")
	statusPath := filepath.Join(root, "child-status")
	encoded, err := json.Marshal(fixture.cfg)
	if err != nil {
		t.Fatalf("encode process-kill %s route configuration: %T", fixture.cell, err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write private process-kill route configuration")
	}

	runID := "stage4-upsert-process-kill-" + fixture.cell + "-" + stateKind +
		"-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	spool := stage4DeleteProcessKillSpool(t, root, runID)
	store, err := stage4UpsertProcessKillOpenStore(stateKind, statePath)
	if err != nil {
		t.Fatalf("open %s process-kill state: %T", stateKind, err)
	}
	stage4DeleteProcessKillInitializeRun(t, store, runID, fixture.cfg)

	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStage4UpsertProcessKillReplayHelperProcess$",
	)
	command.Env = append(os.Environ(),
		stage4UpsertProcessKillCaseEnv+"="+fixture.cell,
		stage4UpsertProcessKillStateEnv+"="+stateKind,
		stage4UpsertProcessKillPathEnv+"="+statePath,
		stage4UpsertProcessKillConfigEnv+"="+configPath,
		stage4UpsertProcessKillRunEnv+"="+runID,
		stage4UpsertProcessKillSpoolEnv+"="+spool,
		stage4UpsertProcessKillEventEnv+"="+eventPath,
		stage4UpsertProcessKillStatusEnv+"="+statusPath,
	)
	var childOutput bytes.Buffer
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	if err := command.Start(); err != nil {
		t.Fatal("start process-kill upsert child")
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
	stage4WaitForUpsertProcessKillBoundary(
		t,
		eventPath,
		statusPath,
		wait,
		&reaped,
		&childOutput,
	)
	if err := command.Process.Kill(); err != nil {
		waitErr := <-wait
		reaped = true
		t.Fatalf("kill process-kill upsert child: %T / %T", err, waitErr)
	}
	if err := <-wait; err == nil {
		reaped = true
		t.Fatal("process-kill upsert child exited normally instead of being killed")
	}
	reaped = true
	_ = childOutput // Withheld deliberately: it can carry driver diagnostics.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Before resuming, the target-only row must already be intact. An upsert
	// route that rebuilt the table would have destroyed it during the killed
	// attempt, and no later assertion would be able to tell that apart from a
	// correct resume.
	if fixture.targetOnlyRows != nil {
		rows, err := fixture.targetOnlyRows(ctx)
		if err != nil {
			t.Fatalf("read %s target-only rows after kill: %T", fixture.cell, err)
		}
		if rows != 1 {
			t.Fatalf(
				"%s target-only row count after kill = %d, want 1",
				fixture.cell,
				rows,
			)
		}
	}

	resumeRun := Stage4RunContext{
		RunID:          runID,
		Backend:        store,
		Resume:         true,
		SpoolDirectory: spool,
	}
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    resumeRun,
	}
	result, err := stage4UpsertProcessKillResume(
		ctx,
		fixture.cell,
		fixture.cfg,
		resumeObserver,
	)
	if err != nil {
		t.Fatalf(
			"resume hard-killed %s upsert run [class=%s]",
			fixture.cell,
			ClassifyTransferError(err),
		)
	}
	if result.Tables != 1 || !result.Validated {
		t.Fatalf("resumed hard-killed %s upsert result = %#v", fixture.cell, result)
	}

	// Aggregate publication belongs to the composed adapter route. The SQLite
	// pair runs the compatibility pipeline, which publishes no durable
	// inventory, so PublishStage4RunCompletion reports nothing to publish
	// rather than failing. Asserting the expected answer per route, instead of
	// demanding publication everywhere, keeps this cell honest about which
	// contract it actually proves.
	published, err := PublishStage4RunCompletion(
		context.Background(),
		resumeRun,
		"migration resumed and completed",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf(
			"publish resumed hard-killed %s upsert run class=%s state_transition=%t immutable=%t",
			fixture.cell,
			ClassifyTransferError(err),
			errors.Is(err, state.ErrStateTransition),
			errors.Is(err, state.ErrImmutableEvidence),
		)
	}
	if wantPublished := fixture.cell != "sqlite"; published != wantPublished {
		t.Fatalf(
			"%s aggregate publication published=%t, want %t",
			fixture.cell,
			published,
			wantPublished,
		)
	}

	stage4AssertUpsertProcessKillTarget(t, ctx, fixture)
}

// stage4AssertUpsertProcessKillTarget states the replay contract in terms of
// the target's own contents rather than the tool's reported row counts, because
// the reported counts are produced by the same code path under test.
func stage4AssertUpsertProcessKillTarget(
	t *testing.T,
	ctx context.Context,
	fixture stage4UpsertProcessKillFixture,
) {
	t.Helper()
	if fixture.targetSourceRows != nil {
		rows, err := fixture.targetSourceRows(ctx)
		if err != nil {
			t.Fatalf("read %s merged rows: %T", fixture.cell, err)
		}
		// Exactly the two source rows, each carrying the source payload. A
		// replayed chunk that inserted duplicates, or one that failed to
		// overwrite the stale payload, both fail here.
		if rows != 2 {
			t.Fatalf(
				"%s merged row count = %d, want 2 rows holding source payloads",
				fixture.cell,
				rows,
			)
		}
	}
	if fixture.targetOnlyRows != nil {
		rows, err := fixture.targetOnlyRows(ctx)
		if err != nil {
			t.Fatalf("read %s target-only rows: %T", fixture.cell, err)
		}
		if rows != 1 {
			t.Fatalf(
				"%s target-only row count = %d, want the retained row to survive upsert",
				fixture.cell,
				rows,
			)
		}
	}
	if fixture.targetTotalRows != nil {
		rows, err := fixture.targetTotalRows(ctx)
		if err != nil {
			t.Fatalf("read %s total rows: %T", fixture.cell, err)
		}
		if rows != 3 {
			t.Fatalf(
				"%s total target rows = %d, want 3 (two merged, one retained)",
				fixture.cell,
				rows,
			)
		}
	}
}

// stage4UpsertProcessKillResume routes each cell to the resume entry point its
// own route actually supports. Verified live 2026-08-01: the composed-adapter
// resume refuses the SQLite pair outright with "uses a compatibility override
// and cannot use composed-adapter resume", so that cell must resume through the
// legacy entry point it was migrated by.
func stage4UpsertProcessKillResume(
	ctx context.Context,
	cell string,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cell == "sqlite" {
		return SQLiteToSQLiteResume(
			ctx,
			cfg,
			CompletedTableCheckpoints{},
			observer,
		)
	}
	return ExecuteResume(ctx, cfg, CompletedTableCheckpoints{}, observer)
}

func stage4WaitForUpsertProcessKillBoundary(
	t *testing.T,
	eventPath string,
	statusPath string,
	wait <-chan error,
	reaped *bool,
	output *bytes.Buffer,
) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if _, err := os.Stat(eventPath); err == nil {
			return
		}
		select {
		case err := <-wait:
			*reaped = true
			t.Fatalf(
				"upsert child exited before the acknowledgement boundary: status=%s err=%T",
				stage4DeleteProcessKillChildStatus(statusPath, output),
				err,
			)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"upsert child never reached the acknowledgement boundary: status=%s",
				stage4DeleteProcessKillChildStatus(statusPath, output),
			)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func stage4WriteUpsertProcessKillStatus(status string) {
	path := os.Getenv(stage4UpsertProcessKillStatusEnv)
	if path == "" {
		return
	}
	_ = os.WriteFile(path, []byte(status), 0o600)
}

func stage4UpsertProcessKillOpenStore(
	kind string,
	path string,
) (stage4UpsertProcessKillStore, error) {
	switch kind {
	case "yaml":
		return state.YAMLStore{Path: path}, nil
	case "sqlite":
		return state.SQLiteStore{Path: path}, nil
	default:
		return nil, fmt.Errorf("unknown process-kill upsert state backend")
	}
}
