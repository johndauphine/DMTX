package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4IncrementalNetworkKillChildEnv   = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_CHILD"
	stage4IncrementalNetworkKillConfigEnv  = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_CONFIG"
	stage4IncrementalNetworkKillStateEnv   = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_STATE"
	stage4IncrementalNetworkKillBackendEnv = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_BACKEND"
	stage4IncrementalNetworkKillSpoolEnv   = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_SPOOL"
	stage4IncrementalNetworkKillRunEnv     = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_RUN"
	stage4IncrementalNetworkKillEventEnv   = "DMTX_STAGE4_INCREMENTAL_NETWORK_KILL_EVENT"
)

// stage4IncrementalNetworkKillBackend parks the child immediately after a
// network range acknowledgement has become durable.
//
// The SQLite incremental crash proof calls os.Exit at this point, which proves
// recovery from a self-terminating process. It cannot prove recovery from a
// process killed from outside, because a process that chooses when to die can
// still have chosen a convenient moment. Parking instead lets the parent send a
// real signal at a moment the child does not control, which is what the
// closeout handoff means by an external hard-kill route.
type stage4IncrementalNetworkKillBackend struct {
	stage4IncrementalLiveAggregateBackend
	eventPath string
}

func (backend *stage4IncrementalNetworkKillBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	rangeState, err := backend.stage4IncrementalTestState.AcknowledgeRange(
		acknowledgement,
	)
	if err != nil || acknowledgement.Task.Type != stage4AdapterNetworkTaskType {
		return rangeState, err
	}
	file, openErr := os.OpenFile(
		backend.eventPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if openErr != nil {
		return rangeState, openErr
	}
	if _, writeErr := file.WriteString("incremental-range-acknowledged\n"); writeErr != nil {
		_ = file.Close()
		return rangeState, writeErr
	}
	if closeErr := file.Close(); closeErr != nil {
		return rangeState, closeErr
	}
	stage4ParkUntilKilled()
	return rangeState, nil
}

// TestStage4IncrementalNetworkProcessKillResumeLive is the external hard-kill
// route for the network-backed incremental cells.
//
// The closeout handoff records this as the one remaining evidence gap in the
// incremental matrix: the 4x4 route matrix proves post-fence window semantics
// for every admitted cell, but only SQLite had a process-death proof, and that
// one is self-terminating. Each cell here runs a real composed incremental
// window against verified TLS, is killed by its parent at a moment it does not
// choose, and must then be resumable into a truthful completion.
//
// Both durable state backends run for every engine because the acknowledgement
// protocol differs between whole-document YAML replacement and SQLite
// transactional commit, and this is precisely the boundary where that
// difference would show.
func TestStage4IncrementalNetworkProcessKillResumeLive(t *testing.T) {
	if os.Getenv(stage4IncrementalNetworkKillChildEnv) == "1" {
		stage4IncrementalNetworkKillChild()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	environment := newStage4IncrementalLiveMatrixEnvironment(t, ctx)
	// The network-backed sources only. SQLite already has its own crash proof,
	// and adding it here would restate that rather than extend the matrix.
	for _, engineName := range []string{"postgres", "mysql", "mssql"} {
		engineName := engineName
		for _, backendKind := range []string{"yaml", "sqlite"} {
			backendKind := backendKind
			t.Run(engineName+"/"+backendKind, func(t *testing.T) {
				fixture := environment.newRoute(t, ctx, engineName, engineName)
				stage4RunIncrementalNetworkKill(
					t,
					ctx,
					fixture,
					engineName,
					backendKind,
				)
			})
		}
	}
}

func stage4RunIncrementalNetworkKill(
	t *testing.T,
	ctx context.Context,
	fixture *stage4IncrementalLiveRouteFixture,
	engineName string,
	backendKind string,
) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("resolve incremental kill directory")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal("protect incremental kill directory")
	}
	statePath := filepath.Join(root, "state."+backendKind)
	configPath := filepath.Join(root, "route.json")
	eventPath := filepath.Join(root, "range-acknowledged")
	spoolParent := filepath.Join(root, "spool")
	if err := os.Mkdir(spoolParent, 0o700); err != nil {
		t.Fatal("create incremental kill spool parent")
	}

	store, err := stage4IncrementalNetworkKillStore(backendKind, statePath)
	if err != nil {
		t.Fatal(err)
	}
	routeConfig := fixture.incrementalConfig()

	// A real completed Stage 4 run must supply the baseline authority. Appending
	// a success record by hand would leave the sentinel evidence
	// unrepresentative of the route being resumed.
	baselineRun := "incremental-net-kill-baseline-" + engineName + "-" + backendKind
	initializeStage4LifecycleRun(t, store, baselineRun, time.Now().Add(-time.Minute))
	baselineEvents := make([]string, 0)
	baselineSpool := filepath.Join(
		spoolParent,
		stage4LifecycleRunDigest(baselineRun),
	)
	if err := os.Mkdir(baselineSpool, 0o700); err != nil {
		t.Fatal("create incremental kill baseline spool")
	}
	baselineObserver := stage4IncrementalTestObserver{
		events:  &baselineEvents,
		backend: store,
		run: Stage4RunContext{
			RunID:          baselineRun,
			Backend:        store,
			SpoolDirectory: baselineSpool,
		},
	}
	result, err := Execute(ctx, routeConfig, baselineObserver)
	if err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("incremental kill baseline result=%#v err=%v", result, err)
	}
	publishStage4IncrementalLiveRun(t, baselineObserver.run)

	upperFence := time.Date(2026, 7, 30, 10, 2, 0, 0, time.UTC)
	if err := fixture.updateSource(ctx, 1, "window-value", upperFence); err != nil {
		t.Fatalf("prepare incremental kill window row: %v", err)
	}

	windowRun := "incremental-net-kill-window-" + engineName + "-" + backendKind
	initializeStage4LifecycleRun(t, store, windowRun, time.Now().Add(-time.Minute))
	windowSpool := filepath.Join(spoolParent, stage4LifecycleRunDigest(windowRun))
	if err := os.Mkdir(windowSpool, 0o700); err != nil {
		t.Fatal("create incremental kill window spool")
	}

	// The route configuration carries credentials, so it travels in a private
	// file rather than the environment, and the child never prints it.
	encoded, err := json.Marshal(routeConfig)
	if err != nil {
		t.Fatalf("encode incremental kill route configuration: %T", err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write private incremental kill route configuration")
	}

	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStage4IncrementalNetworkProcessKillResumeLive$",
	)
	command.Env = append(os.Environ(),
		stage4IncrementalNetworkKillChildEnv+"=1",
		stage4IncrementalNetworkKillConfigEnv+"="+configPath,
		stage4IncrementalNetworkKillStateEnv+"="+statePath,
		stage4IncrementalNetworkKillBackendEnv+"="+backendKind,
		stage4IncrementalNetworkKillSpoolEnv+"="+windowSpool,
		stage4IncrementalNetworkKillRunEnv+"="+windowRun,
		stage4IncrementalNetworkKillEventEnv+"="+eventPath,
	)
	var childOutput bytes.Buffer
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	if err := command.Start(); err != nil {
		t.Fatal("start incremental kill child")
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
	stage4WaitForIncrementalNetworkKillBoundary(t, eventPath, wait, &reaped)
	if err := command.Process.Kill(); err != nil {
		waitErr := <-wait
		reaped = true
		t.Fatalf("kill incremental child: %T / %T", err, waitErr)
	}
	if err := <-wait; err == nil {
		reaped = true
		t.Fatal("incremental kill child exited normally instead of being killed")
	}
	reaped = true
	_ = childOutput // Withheld deliberately: it can carry driver diagnostics.

	// The attempt must still be open, with its immutable upper fence already
	// durable. That combination is the whole point: the fence survived the kill,
	// so the resumed run reads the same window rather than choosing a new one.
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: fixture.sourceTable.Schema,
		Table:  fixture.sourceTable.Name,
	}
	active, found, err := store.LoadActiveIncrementalAttempt(windowRun, task)
	if err != nil || !found || active.Status != state.IncrementalRunning ||
		active.UpperFence == nil ||
		!active.UpperFence.Value.Equal(upperFence) {
		t.Fatalf(
			"killed incremental attempt=%#v found=%t err=%v",
			active,
			found,
			err,
		)
	}

	// A row written after the killed attempt's fence must never enter the
	// resumed window. Inserting it here, between the kill and the resume, is the
	// only placement that proves the resumed run honours the durable fence
	// rather than recomputing one from the current clock.
	postFence := upperFence.Add(time.Second)
	if err := fixture.insertSource(ctx, 3, "after-fence", nil, postFence); err != nil {
		t.Fatalf("insert post-kill source row: %v", err)
	}

	resumeEvents := make([]string, 0)
	resumeRun := Stage4RunContext{
		RunID:          windowRun,
		Backend:        store,
		Resume:         true,
		SpoolDirectory: windowSpool,
	}
	resumeObserver := stage4IncrementalTestObserver{
		events:  &resumeEvents,
		backend: store,
		resume:  true,
		run:     resumeRun,
	}
	result, err = ExecuteResume(
		ctx,
		routeConfig,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf(
			"resume killed %s incremental window [class=%s]",
			engineName,
			ClassifyTransferError(err),
		)
	}
	if result.Tables != 1 || !result.Validated {
		t.Fatalf("resumed killed %s incremental result=%#v", engineName, result)
	}

	if count, err := fixture.targetRowCount(ctx); err != nil || count != 2 {
		t.Fatalf("resumed %s target row count=%d err=%v", engineName, count, err)
	}
	if payload, found, err := fixture.targetPayload(ctx, 1); err != nil ||
		!found || payload != "window-value" {
		t.Fatalf(
			"resumed %s target row one payload=%q found=%t err=%v",
			engineName,
			payload,
			found,
			err,
		)
	}
	if _, found, err := fixture.targetPayload(ctx, 3); err != nil || found {
		t.Fatalf(
			"resumed %s window admitted a post-fence row found=%t err=%v",
			engineName,
			found,
			err,
		)
	}
}

func stage4WaitForIncrementalNetworkKillBoundary(
	t *testing.T,
	eventPath string,
	wait <-chan error,
	reaped *bool,
) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		if _, err := os.Stat(eventPath); err == nil {
			return
		}
		select {
		case err := <-wait:
			*reaped = true
			t.Fatalf(
				"incremental child exited before acknowledging a range: %T",
				err,
			)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("incremental child never acknowledged a network range")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func stage4IncrementalNetworkKillStore(
	kind string,
	path string,
) (stage4IncrementalTestState, error) {
	switch kind {
	case "yaml":
		return state.YAMLStore{Path: path}, nil
	case "sqlite":
		return state.SQLiteStore{Path: path}, nil
	default:
		return nil, fmt.Errorf("unknown incremental kill state backend")
	}
}

// stage4IncrementalNetworkKillChild runs the real composed incremental window
// and parks once a network range is durable. It never returns; the parent ends
// it.
func stage4IncrementalNetworkKillChild() {
	configPath := os.Getenv(stage4IncrementalNetworkKillConfigEnv)
	statePath := os.Getenv(stage4IncrementalNetworkKillStateEnv)
	spool := os.Getenv(stage4IncrementalNetworkKillSpoolEnv)
	runID := os.Getenv(stage4IncrementalNetworkKillRunEnv)
	eventPath := os.Getenv(stage4IncrementalNetworkKillEventEnv)
	if configPath == "" || statePath == "" || spool == "" || runID == "" ||
		eventPath == "" {
		os.Exit(87)
	}
	store, err := stage4IncrementalNetworkKillStore(
		os.Getenv(stage4IncrementalNetworkKillBackendEnv),
		statePath,
	)
	if err != nil {
		os.Exit(88)
	}
	encoded, err := os.ReadFile(configPath)
	if err != nil {
		os.Exit(89)
	}
	var routeConfig config.Config
	if err := json.Unmarshal(encoded, &routeConfig); err != nil {
		os.Exit(90)
	}
	parking := &stage4IncrementalNetworkKillBackend{
		stage4IncrementalLiveAggregateBackend: stage4IncrementalLiveAggregateBackend{
			stage4IncrementalTestState: store,
		},
		eventPath: eventPath,
	}
	events := make([]string, 0)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: parking,
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        parking,
			SpoolDirectory: spool,
		},
	}
	if _, err := Execute(context.Background(), routeConfig, observer); err != nil {
		// Never print the error itself; it can carry endpoint detail.
		fmt.Fprintf(
			os.Stderr,
			"incremental kill child returned before parking [class=%s]\n",
			ClassifyTransferError(err),
		)
	}
	os.Exit(91)
}
