package app

import (
	"context"
	"testing"

	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestTableCheckpointObserverRecordsRangeAttemptsIncludingReplay(t *testing.T) {
	t.Parallel()

	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			t.Parallel()

			backend := initializedRangeBackend(t, extension)
			observer := tableCheckpointObserver{store: backend, runID: "range-run"}
			plan := migrate.SQLiteTransferPlan{
				Table: "items",
				Pagination: migrate.PaginationPlan{
					Strategy:     migrate.PaginationIntegerKeyset,
					Keys:         []migrate.KeySpec{{Name: "id", Kind: migrate.KeyInteger}},
					Ranges:       []migrate.PaginationRange{{ID: 0}},
					TopologyHash: "attempt-topology",
				},
			}
			if err := observer.AfterSQLiteTransferPlan(context.Background(), plan); err != nil {
				t.Fatalf("create work plan: %v", err)
			}

			end := migrate.KeyTuple{migrate.IntegerKey(1)}
			chunk := migrate.SQLiteRangeChunk{
				Table:        plan.Table,
				TopologyHash: plan.Pagination.TopologyHash,
				Range:        plan.Pagination.Ranges[0],
				Sequence:     0,
				ChunkRows:    1,
				End:          &end,
			}
			if err := observer.BeforeSQLiteRangeChunk(context.Background(), chunk); err != nil {
				t.Fatalf("record issued chunk: %v", err)
			}
			if err := observer.BeforeSQLiteRangeAttempt(context.Background(), chunk); err != nil {
				t.Fatalf("record first attempt: %v", err)
			}
			chunk.Replay = true
			if err := observer.BeforeSQLiteRangeAttempt(context.Background(), chunk); err != nil {
				t.Fatalf("record replay attempt: %v", err)
			}

			tasks, ranges, err := backend.(state.RangeBackend).ListWork("range-run")
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 || tasks[0].Attempts != 2 || tasks[0].Retries != 1 {
				t.Fatalf("task attempt counters = %#v", tasks)
			}
			if len(ranges) != 1 || ranges[0].Attempts != 2 || ranges[0].Retries != 1 ||
				len(ranges[0].Pending) != 1 || ranges[0].Pending[0].Attempts != 2 {
				t.Fatalf("range attempt counters = %#v", ranges)
			}

			chunk.Sequence = 1
			if err := observer.BeforeSQLiteRangeAttempt(context.Background(), chunk); err == nil {
				t.Fatal("attempt without a durable issued intent succeeded")
			}
		})
	}
}
