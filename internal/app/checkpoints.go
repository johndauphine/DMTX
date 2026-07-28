package app

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
)

// tableCheckpointObserver persists table boundaries around migration mutation.
type tableCheckpointObserver struct {
	store         state.Backend
	runID         string
	guard         *state.LeaseGuard
	resetTopology bool
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
		return nil
	}
	if err := observer.store.CreateTask(state.Task{
		RunID:     observer.runID,
		Table:     table,
		StartedAt: time.Now().UTC(),
	}); err != nil {
		return stateCheckpointError("create table checkpoint", err)
	}
	return nil
}

func (observer tableCheckpointObserver) AfterTable(_ context.Context, table string, rowsDone int) error {
	if err := observer.store.CompleteTask(observer.runID, table, rowsDone, time.Now().UTC()); err != nil {
		return stateCheckpointError("complete table checkpoint", err)
	}
	return nil
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
