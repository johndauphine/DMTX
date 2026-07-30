package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrLeaseLost = errors.New("target lease lost")

// LeaseGuard holds the canonical target ownership lock across a complete
// target or state mutation. A takeover cannot interleave between verification
// and the protected durable write.
type LeaseGuard struct {
	store SQLiteStore
	lease Lease
	mu    sync.Mutex
}

func NewLeaseGuard(store SQLiteStore, lease Lease) *LeaseGuard {
	return &LeaseGuard{store: store, lease: lease}
}

func (guard *LeaseGuard) Lease() Lease {
	if guard == nil {
		return Lease{}
	}
	return guard.lease
}

func (guard *LeaseGuard) Protect(ctx context.Context, operation func() error) (err error) {
	if guard == nil || operation == nil {
		return fmt.Errorf("%w: missing lease guard or operation", ErrLeaseLost)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()

	database, err := guard.store.Open()
	if err != nil {
		return fmt.Errorf("%w: open lease coordinator: %v", ErrLeaseLost, err)
	}
	defer database.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%w: acquire lease coordinator: %v", ErrLeaseLost, err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("%w: lock lease coordinator: %v", ErrLeaseLost, err)
	}
	locked := true
	defer func() {
		if locked {
			_, rollbackErr := connection.ExecContext(context.Background(), `ROLLBACK`)
			if rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("release lease coordinator: %w", rollbackErr))
			}
		}
	}()

	var ownerToken string
	var runID string
	var generation int64
	queryErr := connection.QueryRowContext(ctx,
		`SELECT run_id, owner_token, generation FROM leases WHERE target = ?`,
		guard.lease.Target,
	).Scan(&runID, &ownerToken, &generation)
	if errors.Is(queryErr, sql.ErrNoRows) ||
		runID != guard.lease.RunID ||
		ownerToken != guard.lease.OwnerToken ||
		generation != guard.lease.Generation {
		return fmt.Errorf("%w: target=%q generation=%d", ErrLeaseLost, guard.lease.Target, guard.lease.Generation)
	}
	if queryErr != nil {
		return fmt.Errorf("%w: verify target ownership: %v", ErrLeaseLost, queryErr)
	}
	if err := operation(); err != nil {
		return err
	}
	if _, err := connection.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("%w: commit lease coordinator: %v", ErrLeaseLost, err)
	}
	locked = false
	return nil
}

func (guard *LeaseGuard) Renew() error {
	if guard == nil {
		return fmt.Errorf("%w: missing lease guard", ErrLeaseLost)
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if err := guard.store.RenewLease(guard.lease); err != nil {
		return fmt.Errorf("%w: %v", ErrLeaseLost, err)
	}
	return nil
}

func (guard *LeaseGuard) Release() error {
	if guard == nil {
		return fmt.Errorf("%w: missing lease guard", ErrLeaseLost)
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if err := guard.store.ReleaseLease(guard.lease); err != nil {
		return fmt.Errorf("%w: %v", ErrLeaseLost, err)
	}
	return nil
}

// FenceBackend returns a backend whose every mutation is protected by the
// supplied lease generation. Reads remain available for diagnosis after loss.
func FenceBackend(backend Backend, guard *LeaseGuard) Backend {
	stage4, _ := backend.(Stage4Backend)
	return &fencedBackend{
		backend: backend,
		ranges:  backend.(RangeBackend),
		stage4:  stage4,
		guard:   guard,
	}
}

type fencedBackend struct {
	backend Backend
	ranges  RangeBackend
	stage4  Stage4Backend
	guard   *LeaseGuard
}

func (backend *fencedBackend) protect(operation func() error) error {
	return backend.guard.Protect(context.Background(), operation)
}

func sameLease(left, right Lease) bool {
	return left.Target == right.Target &&
		left.RunID == right.RunID &&
		left.OwnerToken == right.OwnerToken &&
		left.Generation == right.Generation
}

func (backend *fencedBackend) requireBoundRunLease(runID string) error {
	runs, err := backend.backend.List()
	if err != nil {
		return fmt.Errorf("verify bound target lease: %w", err)
	}
	expected := backend.guard.Lease()
	found := false
	for _, run := range runs {
		if run.ID != runID {
			continue
		}
		bound, err := run.BoundLease()
		if err != nil || !sameLease(bound, expected) {
			return fmt.Errorf(
				"%w: run %q is not bound to target=%q generation=%d",
				ErrLeaseLost,
				runID,
				expected.Target,
				expected.Generation,
			)
		}
		found = true
	}
	if !found {
		return fmt.Errorf("%w: unknown mutation run %q", ErrLeaseLost, runID)
	}
	return nil
}

func (backend *fencedBackend) protectOwnedRun(
	runID string,
	operation func() error,
) error {
	if backend.guard == nil || runID == "" || backend.guard.Lease().RunID != runID {
		return fmt.Errorf("%w: mutation run %q is not owned by the lease", ErrLeaseLost, runID)
	}
	return backend.protect(operation)
}

func (backend *fencedBackend) protectRun(runID string, operation func() error) error {
	return backend.protectOwnedRun(runID, func() error {
		if err := backend.requireBoundRunLease(runID); err != nil {
			return err
		}
		return operation()
	})
}

func (backend *fencedBackend) InitializeRun(run Run, hash string) error {
	lease := backend.guard.Lease()
	bound, err := runWithBoundLease(run, lease)
	if err != nil {
		return fmt.Errorf("%w: initialize run lease binding: %v", ErrLeaseLost, err)
	}
	return backend.protectOwnedRun(run.ID, func() error {
		return backend.backend.InitializeRun(bound, hash)
	})
}
func (backend *fencedBackend) Append(run Run) error {
	return backend.protectRun(run.ID, func() error { return backend.backend.Append(run) })
}
func (backend *fencedBackend) List() ([]Run, error) { return backend.backend.List() }
func (backend *fencedBackend) Latest() (Run, bool, error) {
	return backend.backend.Latest()
}
func (backend *fencedBackend) LatestResumableForTarget(target string) (Run, bool, error) {
	return backend.backend.LatestResumableForTarget(target)
}
func (backend *fencedBackend) BindRunLease(runID string, lease Lease) error {
	if backend.guard == nil || !sameLease(backend.guard.Lease(), lease) {
		return fmt.Errorf("%w: target lease rebind does not match current owner", ErrLeaseLost)
	}
	return backend.protectOwnedRun(runID, func() error {
		return backend.backend.BindRunLease(runID, lease)
	})
}
func (backend *fencedBackend) ReactivateRun(runID, reason string) error {
	return backend.protectRun(runID, func() error { return backend.backend.ReactivateRun(runID, reason) })
}
func (backend *fencedBackend) UpdateFailure(runID, reason string, endedAt time.Time) error {
	return backend.protectRun(runID, func() error { return backend.backend.UpdateFailure(runID, reason, endedAt) })
}
func (backend *fencedBackend) UpdateRecoverableOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.UpdateRecoverableOutcome(runID, outcome, reason, endedAt)
	})
}
func (backend *fencedBackend) AbandonRun(runID, reason string, endedAt time.Time) error {
	return backend.protectRun(runID, func() error { return backend.backend.AbandonRun(runID, reason, endedAt) })
}
func (backend *fencedBackend) CreateTask(task Task) error {
	return backend.protectRun(task.RunID, func() error { return backend.backend.CreateTask(task) })
}
func (backend *fencedBackend) CreateTasks(tasks []Task) error {
	if len(tasks) == 0 {
		return nil
	}
	runID := tasks[0].RunID
	for _, task := range tasks[1:] {
		if task.RunID != runID {
			return fmt.Errorf("%w: batch contains multiple run IDs", ErrLeaseLost)
		}
	}
	return backend.protectRun(runID, func() error { return backend.backend.CreateTasks(tasks) })
}
func (backend *fencedBackend) AdvanceIntegerKeysetTask(runID, table string, rows int, watermark int64) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.AdvanceIntegerKeysetTask(runID, table, rows, watermark)
	})
}
func (backend *fencedBackend) AdvanceRowNumberTask(runID, table string, rows int, watermark int64) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.AdvanceRowNumberTask(runID, table, rows, watermark)
	})
}
func (backend *fencedBackend) CompleteTask(runID, table string, rows int, completedAt time.Time) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.CompleteTask(runID, table, rows, completedAt)
	})
}
func (backend *fencedBackend) ListTasks(runID string) ([]Task, error) {
	return backend.backend.ListTasks(runID)
}
func (backend *fencedBackend) SaveConfigHash(runID, hash string) error {
	return backend.protectRun(runID, func() error { return backend.backend.SaveConfigHash(runID, hash) })
}
func (backend *fencedBackend) ConfigHash(runID string) (string, bool, error) {
	return backend.backend.ConfigHash(runID)
}
func (backend *fencedBackend) SaveResumeCompatibilityHash(runID, hash string) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.SaveResumeCompatibilityHash(runID, hash)
	})
}
func (backend *fencedBackend) ResumeCompatibilityHash(runID string) (string, bool, error) {
	return backend.backend.ResumeCompatibilityHash(runID)
}
func (backend *fencedBackend) AcknowledgeConfigOverride(runID, hash, compatibility string) error {
	return backend.protectRun(runID, func() error {
		return backend.backend.AcknowledgeConfigOverride(runID, hash, compatibility)
	})
}
func (backend *fencedBackend) EnsureWorkPlan(task WorkTask, ranges []RangeState) (bool, error) {
	var created bool
	err := backend.protectRun(task.RunID, func() error {
		var err error
		created, err = backend.ranges.EnsureWorkPlan(task, ranges)
		return err
	})
	return created, err
}
func (backend *fencedBackend) ResetWorkPlan(task WorkTask, ranges []RangeState) error {
	return backend.protectRun(task.RunID, func() error { return backend.ranges.ResetWorkPlan(task, ranges) })
}
func (backend *fencedBackend) ListWork(runID string) ([]WorkTask, []RangeState, error) {
	return backend.ranges.ListWork(runID)
}
func (backend *fencedBackend) BeginRangeChunk(intent RangeChunkIntent) error {
	return backend.protectRun(intent.RunID, func() error { return backend.ranges.BeginRangeChunk(intent) })
}
func (backend *fencedBackend) RecordRangeAttempt(attempt RangeAttempt) error {
	return backend.protectRun(attempt.RunID, func() error { return backend.ranges.RecordRangeAttempt(attempt) })
}
func (backend *fencedBackend) AcknowledgeRange(ack RangeAcknowledgement) (RangeState, error) {
	var updated RangeState
	err := backend.protectRun(ack.RunID, func() error {
		var err error
		updated, err = backend.ranges.AcknowledgeRange(ack)
		return err
	})
	return updated, err
}
func (backend *fencedBackend) CompleteRange(runID string, task TaskKey, rangeID, topology string, expected uint64, completedAt time.Time) error {
	return backend.protectRun(runID, func() error {
		return backend.ranges.CompleteRange(runID, task, rangeID, topology, expected, completedAt)
	})
}
func (backend *fencedBackend) CompleteWorkTask(runID string, task TaskKey, topology string, completedAt time.Time) error {
	return backend.protectRun(runID, func() error {
		return backend.ranges.CompleteWorkTask(runID, task, topology, completedAt)
	})
}
func (backend *fencedBackend) stage4Backend() (Stage4Backend, error) {
	if backend.stage4 == nil {
		return nil, fmt.Errorf("Stage 4 state is unsupported by this backend")
	}
	return backend.stage4, nil
}
func (backend *fencedBackend) SaveSchemaSnapshot(snapshot SchemaSnapshot) error {
	return backend.protectRun(snapshot.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		return stage4.SaveSchemaSnapshot(snapshot)
	})
}
func (backend *fencedBackend) LoadSchemaSnapshot(runID string, task TaskKey) (SchemaSnapshot, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	return stage4.LoadSchemaSnapshot(runID, task)
}
func (backend *fencedBackend) LoadLatestApplicableSchemaSnapshot(
	runID string,
	task TaskKey,
) (SchemaSnapshot, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	return stage4.LoadLatestApplicableSchemaSnapshot(runID, task)
}
func (backend *fencedBackend) BeginIncrementalAttempt(attempt IncrementalAttempt) (IncrementalAttempt, bool, error) {
	var stored IncrementalAttempt
	var created bool
	err := backend.protectRun(attempt.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		stored, created, err = stage4.BeginIncrementalAttempt(attempt)
		return err
	})
	return stored, created, err
}
func (backend *fencedBackend) LoadIncrementalAttempt(
	runID string,
	task TaskKey,
	attemptID string,
) (IncrementalAttempt, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	return stage4.LoadIncrementalAttempt(runID, task, attemptID)
}
func (backend *fencedBackend) LoadActiveIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	return stage4.LoadActiveIncrementalAttempt(runID, task)
}
func (backend *fencedBackend) LoadLatestCommittedIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	return stage4.LoadLatestCommittedIncrementalAttempt(runID, task)
}
func (backend *fencedBackend) CommitIncrementalAttempt(commit IncrementalCommit) error {
	return backend.protectRun(commit.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		return stage4.CommitIncrementalAttempt(commit)
	})
}
func (backend *fencedBackend) BeginDeleteReconciliation(
	record DeleteReconciliation,
) (DeleteReconciliation, bool, error) {
	var stored DeleteReconciliation
	var created bool
	err := backend.protectRun(record.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		stored, created, err = stage4.BeginDeleteReconciliation(record)
		return err
	})
	return stored, created, err
}
func (backend *fencedBackend) LoadDeleteReconciliation(
	runID string,
	task TaskKey,
	attemptID string,
) (DeleteReconciliation, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	return stage4.LoadDeleteReconciliation(runID, task, attemptID)
}
func (backend *fencedBackend) LoadLatestSuccessfulDeleteReconciliation(
	runID string,
	task TaskKey,
) (DeleteReconciliation, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	return stage4.LoadLatestSuccessfulDeleteReconciliation(runID, task)
}
func (backend *fencedBackend) FinishDeleteReconciliation(result DeleteReconciliationResult) error {
	return backend.protectRun(result.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		return stage4.FinishDeleteReconciliation(result)
	})
}
func (backend *fencedBackend) SaveStrictMigrationSnapshot(snapshot StrictMigrationSnapshot) error {
	return backend.protectRun(snapshot.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		return stage4.SaveStrictMigrationSnapshot(snapshot)
	})
}
func (backend *fencedBackend) LoadStrictMigrationSnapshot(
	runID string,
	epochID string,
) (StrictMigrationSnapshot, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	return stage4.LoadStrictMigrationSnapshot(runID, epochID)
}
func (backend *fencedBackend) LoadLatestStrictMigrationSnapshot(
	runID string,
) (StrictMigrationSnapshot, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	return stage4.LoadLatestStrictMigrationSnapshot(runID)
}
func (backend *fencedBackend) SaveStrictSnapshotEvidence(evidence StrictSnapshotEvidence) error {
	return backend.protectRun(evidence.RunID, func() error {
		stage4, err := backend.stage4Backend()
		if err != nil {
			return err
		}
		return stage4.SaveStrictSnapshotEvidence(evidence)
	})
}
func (backend *fencedBackend) LoadStrictSnapshotEvidence(
	runID string,
	task TaskKey,
	attemptID string,
) (StrictSnapshotEvidence, bool, error) {
	stage4, err := backend.stage4Backend()
	if err != nil {
		return StrictSnapshotEvidence{}, false, err
	}
	return stage4.LoadStrictSnapshotEvidence(runID, task, attemptID)
}

var (
	_ Backend       = (*fencedBackend)(nil)
	_ RangeBackend  = (*fencedBackend)(nil)
	_ Stage4Backend = (*fencedBackend)(nil)
)
