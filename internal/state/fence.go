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
	var generation int64
	queryErr := connection.QueryRowContext(ctx,
		`SELECT owner_token, generation FROM leases WHERE target = ?`,
		guard.lease.Target,
	).Scan(&ownerToken, &generation)
	if errors.Is(queryErr, sql.ErrNoRows) || ownerToken != guard.lease.OwnerToken || generation != guard.lease.Generation {
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
	return &fencedBackend{backend: backend, ranges: backend.(RangeBackend), guard: guard}
}

type fencedBackend struct {
	backend Backend
	ranges  RangeBackend
	guard   *LeaseGuard
}

func (backend *fencedBackend) protect(operation func() error) error {
	return backend.guard.Protect(context.Background(), operation)
}

func (backend *fencedBackend) InitializeRun(run Run, hash string) error {
	return backend.protect(func() error { return backend.backend.InitializeRun(run, hash) })
}
func (backend *fencedBackend) Append(run Run) error {
	return backend.protect(func() error { return backend.backend.Append(run) })
}
func (backend *fencedBackend) List() ([]Run, error) { return backend.backend.List() }
func (backend *fencedBackend) Latest() (Run, bool, error) {
	return backend.backend.Latest()
}
func (backend *fencedBackend) LatestResumableForTarget(target string) (Run, bool, error) {
	return backend.backend.LatestResumableForTarget(target)
}
func (backend *fencedBackend) ReactivateRun(runID, reason string) error {
	return backend.protect(func() error { return backend.backend.ReactivateRun(runID, reason) })
}
func (backend *fencedBackend) UpdateFailure(runID, reason string, endedAt time.Time) error {
	return backend.protect(func() error { return backend.backend.UpdateFailure(runID, reason, endedAt) })
}
func (backend *fencedBackend) UpdateRecoverableOutcome(runID string, outcome Outcome, reason string, endedAt time.Time) error {
	return backend.protect(func() error {
		return backend.backend.UpdateRecoverableOutcome(runID, outcome, reason, endedAt)
	})
}
func (backend *fencedBackend) AbandonRun(runID, reason string, endedAt time.Time) error {
	return backend.protect(func() error { return backend.backend.AbandonRun(runID, reason, endedAt) })
}
func (backend *fencedBackend) CreateTask(task Task) error {
	return backend.protect(func() error { return backend.backend.CreateTask(task) })
}
func (backend *fencedBackend) CreateTasks(tasks []Task) error {
	return backend.protect(func() error { return backend.backend.CreateTasks(tasks) })
}
func (backend *fencedBackend) AdvanceIntegerKeysetTask(runID, table string, rows int, watermark int64) error {
	return backend.protect(func() error {
		return backend.backend.AdvanceIntegerKeysetTask(runID, table, rows, watermark)
	})
}
func (backend *fencedBackend) AdvanceRowNumberTask(runID, table string, rows int, watermark int64) error {
	return backend.protect(func() error {
		return backend.backend.AdvanceRowNumberTask(runID, table, rows, watermark)
	})
}
func (backend *fencedBackend) CompleteTask(runID, table string, rows int, completedAt time.Time) error {
	return backend.protect(func() error {
		return backend.backend.CompleteTask(runID, table, rows, completedAt)
	})
}
func (backend *fencedBackend) ListTasks(runID string) ([]Task, error) {
	return backend.backend.ListTasks(runID)
}
func (backend *fencedBackend) SaveConfigHash(runID, hash string) error {
	return backend.protect(func() error { return backend.backend.SaveConfigHash(runID, hash) })
}
func (backend *fencedBackend) ConfigHash(runID string) (string, bool, error) {
	return backend.backend.ConfigHash(runID)
}
func (backend *fencedBackend) SaveResumeCompatibilityHash(runID, hash string) error {
	return backend.protect(func() error {
		return backend.backend.SaveResumeCompatibilityHash(runID, hash)
	})
}
func (backend *fencedBackend) ResumeCompatibilityHash(runID string) (string, bool, error) {
	return backend.backend.ResumeCompatibilityHash(runID)
}
func (backend *fencedBackend) AcknowledgeConfigOverride(runID, hash, compatibility string) error {
	return backend.protect(func() error {
		return backend.backend.AcknowledgeConfigOverride(runID, hash, compatibility)
	})
}
func (backend *fencedBackend) EnsureWorkPlan(task WorkTask, ranges []RangeState) (bool, error) {
	var created bool
	err := backend.protect(func() error {
		var err error
		created, err = backend.ranges.EnsureWorkPlan(task, ranges)
		return err
	})
	return created, err
}
func (backend *fencedBackend) ResetWorkPlan(task WorkTask, ranges []RangeState) error {
	return backend.protect(func() error { return backend.ranges.ResetWorkPlan(task, ranges) })
}
func (backend *fencedBackend) ListWork(runID string) ([]WorkTask, []RangeState, error) {
	return backend.ranges.ListWork(runID)
}
func (backend *fencedBackend) BeginRangeChunk(intent RangeChunkIntent) error {
	return backend.protect(func() error { return backend.ranges.BeginRangeChunk(intent) })
}
func (backend *fencedBackend) RecordRangeAttempt(attempt RangeAttempt) error {
	return backend.protect(func() error { return backend.ranges.RecordRangeAttempt(attempt) })
}
func (backend *fencedBackend) AcknowledgeRange(ack RangeAcknowledgement) (RangeState, error) {
	var updated RangeState
	err := backend.protect(func() error {
		var err error
		updated, err = backend.ranges.AcknowledgeRange(ack)
		return err
	})
	return updated, err
}
func (backend *fencedBackend) CompleteRange(runID string, task TaskKey, rangeID, topology string, expected uint64, completedAt time.Time) error {
	return backend.protect(func() error {
		return backend.ranges.CompleteRange(runID, task, rangeID, topology, expected, completedAt)
	})
}
func (backend *fencedBackend) CompleteWorkTask(runID string, task TaskKey, topology string, completedAt time.Time) error {
	return backend.protect(func() error {
		return backend.ranges.CompleteWorkTask(runID, task, topology, completedAt)
	})
}

var (
	_ Backend      = (*fencedBackend)(nil)
	_ RangeBackend = (*fencedBackend)(nil)
)
