package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

// tableCheckpointObserver persists table boundaries around migration mutation.
type tableCheckpointObserver struct {
	store          state.Backend
	runID          string
	guard          *state.LeaseGuard
	resetTopology  bool
	resume         bool
	spoolDirectory string
	configPath     string

	// progress reports where the run has got to. Nil when nobody is watching,
	// which is every CLI invocation. Reporting happens after the checkpoint
	// each hook exists to write, never before: a watcher sees what has durably
	// happened, not what is about to be attempted.
	progress *progressReporter
}

// stage4FencedStateBackend is private proof that the application wrapped the
// complete Stage 4 state surface and the ordinary run backend with the same
// lease guard. A raw backend cannot satisfy this proof by setting a flag.
type stage4FencedStateBackend struct {
	state.Backend
	migrate.Stage4StateBackend
	guard *state.LeaseGuard
	lease state.Lease
}

func newStage4FencedStateBackend(
	backend state.Backend,
	guard *state.LeaseGuard,
) (state.Backend, error) {
	if backend == nil {
		return nil, fmt.Errorf("state backend is required")
	}
	if guard == nil {
		return nil, fmt.Errorf("target lease guard is required")
	}
	lease := guard.Lease()
	if strings.TrimSpace(lease.Target) == "" ||
		strings.TrimSpace(lease.RunID) == "" ||
		strings.TrimSpace(lease.OwnerToken) == "" ||
		lease.Generation < 1 {
		return nil, fmt.Errorf("complete target lease evidence is required")
	}
	if _, ok := backend.(migrate.Stage4StateBackend); !ok {
		return nil, fmt.Errorf(
			"state backend %T lacks required Stage 4 range/evidence capabilities",
			backend,
		)
	}
	fenced := state.FenceBackend(backend, guard)
	stage4, ok := fenced.(migrate.Stage4StateBackend)
	if !ok {
		return nil, fmt.Errorf(
			"lease-fenced state backend %T lost required Stage 4 capabilities",
			fenced,
		)
	}
	return &stage4FencedStateBackend{
		Backend:            fenced,
		Stage4StateBackend: stage4,
		guard:              guard,
		lease:              lease,
	}, nil
}

// Stage4RunContext exposes only the already-fenced state backend installed by
// the run/resume lifecycle. Resolving it is read-only: spool creation and
// durable schema work are owned by later migration stages.
func (observer tableCheckpointObserver) Stage4RunContext() (
	migrate.Stage4RunContext,
	error,
) {
	if observer.guard == nil {
		return migrate.Stage4RunContext{}, fmt.Errorf(
			"Stage 4 run context requires an active target lease guard",
		)
	}
	fenced, ok := observer.store.(*stage4FencedStateBackend)
	if !ok || fenced.guard != observer.guard {
		return migrate.Stage4RunContext{}, fmt.Errorf(
			"Stage 4 run context requires the application's lease-fenced state backend",
		)
	}
	lease := observer.guard.Lease()
	if lease != fenced.lease ||
		lease.RunID != observer.runID ||
		strings.TrimSpace(lease.Target) == "" ||
		strings.TrimSpace(lease.OwnerToken) == "" ||
		lease.Generation < 1 {
		return migrate.Stage4RunContext{}, fmt.Errorf(
			"Stage 4 run context lease evidence does not own run %q",
			observer.runID,
		)
	}
	run := migrate.Stage4RunContext{
		RunID:          observer.runID,
		Backend:        fenced.Stage4StateBackend,
		Resume:         observer.resume,
		SpoolDirectory: observer.spoolDirectory,
	}
	if err := run.Validate(); err != nil {
		return migrate.Stage4RunContext{}, err
	}
	return run, nil
}

// publishStage4RunSuccess routes successful completion through one atomic
// Stage 4 publication when the route composed aggregate evidence, and reports
// false when it did not so the caller records success by its ordinary path.
//
// Callers must append their validation audit first: terminal repair refuses a
// successful run whose validation evidence is missing, so the durable success
// must never precede it. The publication itself deliberately does not observe
// the migration's cancellable context — the transfer already succeeded and its
// lease was reverified, so a late signal must not strand the outcome.
func publishStage4RunSuccess(
	observer tableCheckpointObserver,
	reason string,
) (bool, error) {
	run, err := observer.Stage4RunContext()
	if err != nil {
		return false, stateCheckpointError("resolve Stage 4 run context", err)
	}
	published, err := migrate.PublishStage4RunCompletion(
		context.Background(),
		run,
		reason,
		time.Now().UTC(),
	)
	if err != nil {
		return false, stateCheckpointError(
			"publish Stage 4 run completion",
			err,
		)
	}
	return published, nil
}

func (observer tableCheckpointObserver) ObserveStage4SchemaDecisions(
	ctx context.Context,
	report migrate.Stage4SchemaDecisionReport,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(observer.configPath) == "" {
		return fmt.Errorf("Stage 4 schema decision audit path is required")
	}
	if strings.TrimSpace(observer.runID) == "" ||
		report.RunID != observer.runID {
		return fmt.Errorf(
			"Stage 4 schema decision report run %q does not match observer run %q",
			report.RunID,
			observer.runID,
		)
	}
	return appendAuditPayload(
		observer.configPath,
		observer.runID,
		stage4SchemaDecisionsAuditEvent,
		report,
	)
}

func stage4SpoolDirectory(statePath, runID string) (string, error) {
	if strings.TrimSpace(statePath) == "" {
		return "", fmt.Errorf("state path is required for the Stage 4 spool")
	}
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(runID) != runID {
		return "", fmt.Errorf("canonical run ID is required for the Stage 4 spool")
	}
	absoluteStatePath, err := filepath.Abs(statePath)
	if err != nil {
		return "", fmt.Errorf("resolve absolute state path for Stage 4 spool: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absoluteStatePath))
	if err != nil {
		return "", fmt.Errorf("resolve stable state directory for Stage 4 spool: %w", err)
	}
	stableStatePath := filepath.Join(parent, filepath.Base(absoluteStatePath))
	if resolvedStatePath, resolveErr := filepath.EvalSymlinks(absoluteStatePath); resolveErr == nil {
		stableStatePath = resolvedStatePath
	} else if !os.IsNotExist(resolveErr) {
		return "", fmt.Errorf("resolve stable state file for Stage 4 spool: %w", resolveErr)
	} else if stateInfo, stateErr := os.Lstat(absoluteStatePath); stateErr == nil &&
		stateInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf(
			"resolve stable state file for Stage 4 spool: dangling state symlink %q",
			absoluteStatePath,
		)
	} else if stateErr != nil && !os.IsNotExist(stateErr) {
		return "", fmt.Errorf("inspect state file for Stage 4 spool: %w", stateErr)
	}
	runDigest := sha256.Sum256([]byte(runID))
	privateRoot := "." + filepath.Base(stableStatePath) + ".stage4-spool"
	root := filepath.Join(filepath.Dir(stableStatePath), privateRoot)
	if err := ensurePrivateStage4Directory(root); err != nil {
		return "", fmt.Errorf("prepare private Stage 4 spool root: %w", err)
	}
	spool := filepath.Join(root, hex.EncodeToString(runDigest[:]))
	if err := ensurePrivateStage4Directory(spool); err != nil {
		return "", fmt.Errorf("prepare run-private Stage 4 spool: %w", err)
	}
	// Revalidate the parent after child creation so a path replacement at this
	// boundary cannot be accepted merely because each component was valid at a
	// different point in time.
	if err := ensurePrivateStage4Directory(root); err != nil {
		return "", fmt.Errorf("revalidate private Stage 4 spool root: %w", err)
	}
	if err := ensurePrivateStage4Directory(spool); err != nil {
		return "", fmt.Errorf("revalidate run-private Stage 4 spool: %w", err)
	}
	return spool, nil
}

func ensurePrivateStage4Directory(path string) error {
	err := os.Mkdir(path, 0o700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%q is not a real directory", path)
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf(
			"%q permissions must be 0700 (owner-only and owner-accessible)",
			path,
		)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if resolved != path {
		return fmt.Errorf("%q traverses a symlink to %q", path, resolved)
	}
	return nil
}

func persistStage4SpoolPreparationFailure(
	store state.Backend,
	runID string,
	cause error,
) error {
	if cause == nil {
		return fmt.Errorf("Stage 4 spool preparation failure cause is required")
	}
	return store.UpdateRecoverableOutcome(
		runID,
		state.Failed,
		"Stage 4 spool preparation failed: "+cause.Error(),
		time.Now().UTC(),
	)
}

func (observer tableCheckpointObserver) BeforeTables(_ context.Context, tables []string) error {
	started := time.Now().UTC()
	tasks := make([]state.Task, 0, len(tables))
	for _, table := range tables {
		tasks = append(tasks, state.Task{RunID: observer.runID, Table: table, StartedAt: started})
	}
	if err := observer.store.CreateTasks(tasks); err != nil {
		return stateCheckpointError("create table checkpoints", err)
	}
	observer.progress.planned(tables)
	return nil
}

func (observer tableCheckpointObserver) BeforeTable(_ context.Context, table string) error {
	tasks, err := observer.store.ListTasks(observer.runID)
	if err != nil {
		return stateCheckpointError("list table checkpoints", err)
	}
	for _, task := range tasks {
		if task.Table != table {
			continue
		}
		if task.Status != "running" {
			return stateCheckpointError("reuse table checkpoint", fmt.Errorf("task %q is %s", table, task.Status))
		}
		observer.progress.starting(table)
		return nil
	}
	if err := observer.store.CreateTask(state.Task{
		RunID:     observer.runID,
		Table:     table,
		StartedAt: time.Now().UTC(),
	}); err != nil {
		return stateCheckpointError("create table checkpoint", err)
	}
	observer.progress.starting(table)
	return nil
}

func (observer tableCheckpointObserver) AfterTable(_ context.Context, table string, rowsDone int) error {
	if err := observer.store.CompleteTask(observer.runID, table, rowsDone, time.Now().UTC()); err != nil {
		return stateCheckpointError("complete table checkpoint", err)
	}
	observer.progress.finished(table, rowsDone)
	return nil
}

// AfterStage4TablePublication verifies the aggregate Stage 4 completion that
// already atomically advanced this ordinary task. It intentionally does not
// call CompleteTask again: the regular AfterTable hook owns that mutation on
// legacy routes, whereas a composed route has already committed it together
// with its structured range evidence.
func (observer tableCheckpointObserver) AfterStage4TablePublication(
	ctx context.Context,
	table string,
	rowsDone int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tasks, err := observer.store.ListTasks(observer.runID)
	if err != nil {
		return stateCheckpointError(
			"read published Stage 4 table checkpoint",
			err,
		)
	}
	for _, task := range tasks {
		if task.Table != table {
			continue
		}
		if task.Status != "completed" || task.RowsDone != rowsDone {
			return stateCheckpointError(
				"verify published Stage 4 table checkpoint",
				fmt.Errorf(
					"task %q is %s with %d rows, want completed with %d",
					table,
					task.Status,
					task.RowsDone,
					rowsDone,
				),
			)
		}
		// Reported here as well as in AfterTable, not twice for one table: the
		// two hooks are mutually exclusive, the ordinary one owning legacy
		// routes and this one composed routes that already committed the same
		// advance with their range evidence.
		observer.progress.finished(table, rowsDone)
		return nil
	}
	return stateCheckpointError(
		"verify published Stage 4 table checkpoint",
		fmt.Errorf("task %q is missing", table),
	)
}

func (observer tableCheckpointObserver) AfterIntegerKeysetPage(_ context.Context, table string, rowsDone int, watermark int64) error {
	if err := observer.store.AdvanceIntegerKeysetTask(observer.runID, table, rowsDone, watermark); err != nil {
		return stateCheckpointError("advance integer checkpoint", err)
	}
	return nil
}

func (observer tableCheckpointObserver) AfterRowNumberPage(_ context.Context, table string, rowsDone int, watermark int64) error {
	if err := observer.store.AdvanceRowNumberTask(observer.runID, table, rowsDone, watermark); err != nil {
		return stateCheckpointError("advance row-number checkpoint", err)
	}
	return nil
}

// ProtectTargetMutation holds target ownership across one complete durable
// target write. Migration adapters call this optional hook around commits.
func (observer tableCheckpointObserver) ProtectTargetMutation(ctx context.Context, operation func() error) error {
	if observer.guard == nil {
		return operation()
	}
	return observer.guard.Protect(ctx, operation)
}

func stateCheckpointError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %v", state.ErrState, action, err)
}
