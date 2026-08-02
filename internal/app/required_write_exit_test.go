package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// requiredWriteFailureBackend fails exactly one durable write. Each required
// write is exercised on its own so a fixture cannot pass because some earlier
// write happened to fail first.
type requiredWriteFailureBackend struct {
	state.Backend
	failCreateTasks   bool
	failCreateTask    bool
	failCompleteTask  bool
	failAdvanceInt    bool
	failAdvanceRowNum bool
}

var errInjectedRequiredWrite = errors.New("injected required-write failure")

func (backend requiredWriteFailureBackend) CreateTasks(
	tasks []state.Task,
) error {
	if backend.failCreateTasks {
		return errInjectedRequiredWrite
	}
	return backend.Backend.CreateTasks(tasks)
}

func (backend requiredWriteFailureBackend) CreateTask(
	task state.Task,
) error {
	if backend.failCreateTask {
		return errInjectedRequiredWrite
	}
	return backend.Backend.CreateTask(task)
}

func (backend requiredWriteFailureBackend) ListTasks(
	runID string,
) ([]state.Task, error) {
	if backend.failCreateTask {
		// BeforeTable lists before it creates; returning an empty set forces
		// the create path this case is about.
		return nil, nil
	}
	return backend.Backend.ListTasks(runID)
}

func (backend requiredWriteFailureBackend) CompleteTask(
	runID, table string,
	rows int,
	completedAt time.Time,
) error {
	if backend.failCompleteTask {
		return errInjectedRequiredWrite
	}
	return backend.Backend.CompleteTask(runID, table, rows, completedAt)
}

func (backend requiredWriteFailureBackend) AdvanceIntegerKeysetTask(
	runID, table string,
	rows int,
	watermark int64,
) error {
	if backend.failAdvanceInt {
		return errInjectedRequiredWrite
	}
	return backend.Backend.AdvanceIntegerKeysetTask(
		runID,
		table,
		rows,
		watermark,
	)
}

func (backend requiredWriteFailureBackend) AdvanceRowNumberTask(
	runID, table string,
	rows int,
	watermark int64,
) error {
	if backend.failAdvanceRowNum {
		return errInjectedRequiredWrite
	}
	return backend.Backend.AdvanceRowNumberTask(
		runID,
		table,
		rows,
		watermark,
	)
}

// TestStage4EveryRequiredWriteFailureReturnsStateExitSix proves the
// required-write rule at the point the application actually decides an exit
// code: an unresolved durable write must classify as a state failure, and a
// state failure must map to exit 6. A required write that classified as a
// transfer failure would let a caller retry into a fresh run instead of
// repairing and resuming.
func TestStage4EveryRequiredWriteFailureReturnsStateExitSix(t *testing.T) {
	if StateError != 6 {
		t.Fatalf("state exit code = %d, want 6", StateError)
	}
	ctx := context.Background()
	for name, test := range map[string]struct {
		backend requiredWriteFailureBackend
		invoke  func(tableCheckpointObserver) error
	}{
		"table set creation": {
			backend: requiredWriteFailureBackend{failCreateTasks: true},
			invoke: func(observer tableCheckpointObserver) error {
				return observer.BeforeTables(ctx, []string{"items"})
			},
		},
		"single table creation": {
			backend: requiredWriteFailureBackend{failCreateTask: true},
			invoke: func(observer tableCheckpointObserver) error {
				return observer.BeforeTable(ctx, "items")
			},
		},
		"table completion": {
			backend: requiredWriteFailureBackend{failCompleteTask: true},
			invoke: func(observer tableCheckpointObserver) error {
				return observer.AfterTable(ctx, "items", 2)
			},
		},
		"integer keyset checkpoint": {
			backend: requiredWriteFailureBackend{failAdvanceInt: true},
			invoke: func(observer tableCheckpointObserver) error {
				return observer.AfterIntegerKeysetPage(ctx, "items", 2, 7)
			},
		},
		"row number checkpoint": {
			backend: requiredWriteFailureBackend{failAdvanceRowNum: true},
			invoke: func(observer tableCheckpointObserver) error {
				return observer.AfterRowNumberPage(ctx, "items", 2, 7)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			observer := tableCheckpointObserver{
				store: test.backend,
				runID: "required-write-run",
			}
			err := test.invoke(observer)
			if err == nil {
				t.Fatal("required write failure was swallowed")
			}
			if !errors.Is(err, state.ErrState) {
				t.Fatalf("required write error does not classify as state: %v", err)
			}
			if !isStateOrLeaseFailure(err) {
				t.Fatalf("required write error is not a state failure: %v", err)
			}
			if code := migrationExitCode(err); code != StateError {
				t.Fatalf("required write exit code = %d, want %d", code, StateError)
			}
		})
	}
}

// TestStage4RequiredWriteFailureOutranksTransferClassification pins the
// precedence that makes the rule useful: a durable write failure must not be
// reported as a transfer error even when the transfer itself was fine.
func TestStage4RequiredWriteFailureOutranksTransferClassification(t *testing.T) {
	err := stateCheckpointError(
		"complete table checkpoint",
		errInjectedRequiredWrite,
	)
	if code := migrationExitCode(err); code != StateError {
		t.Fatalf("state failure exit code = %d, want %d", code, StateError)
	}
	if code := migrationExitCode(errInjectedRequiredWrite); code != TransferError {
		t.Fatalf(
			"unclassified failure exit code = %d, want %d",
			code,
			TransferError,
		)
	}
}
