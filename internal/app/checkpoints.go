package app

import (
	"context"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
)

// tableCheckpointObserver persists table boundaries around migration mutation.
type tableCheckpointObserver struct {
	store state.SQLiteStore
	runID string
}

func (observer tableCheckpointObserver) BeforeTable(_ context.Context, table string) error {
	return observer.store.CreateTask(state.Task{
		RunID:     observer.runID,
		Table:     table,
		StartedAt: time.Now().UTC(),
	})
}

func (observer tableCheckpointObserver) AfterTable(_ context.Context, table string, rowsDone int) error {
	return observer.store.CompleteTask(observer.runID, table, rowsDone, time.Now().UTC())
}

func (observer tableCheckpointObserver) AfterIntegerKeysetPage(_ context.Context, table string, rowsDone int, watermark int64) error {
	return observer.store.AdvanceIntegerKeysetTask(observer.runID, table, rowsDone, watermark)
}
