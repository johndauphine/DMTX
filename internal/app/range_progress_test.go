package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

func TestTableCheckpointObserverRangeLifecycleConformance(t *testing.T) {
	t.Parallel()

	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			t.Parallel()
			backend := initializedRangeBackend(t, extension)
			observer := tableCheckpointObserver{store: backend, runID: "range-run"}
			upper := migrate.KeyTuple{migrate.IntegerKey(9_007_199_254_740_993)}
			plan := migrate.SQLiteTransferPlan{
				Table: "items",
				Pagination: migrate.PaginationPlan{
					Strategy:     migrate.PaginationIntegerKeyset,
					Keys:         []migrate.KeySpec{{Name: "id", Kind: migrate.KeyInteger}},
					Ranges:       []migrate.PaginationRange{{ID: 0, Upper: &upper}},
					TopologyHash: "topology-a",
				},
			}
			if err := observer.AfterSQLiteTransferPlan(context.Background(), plan); err != nil {
				t.Fatalf("create plan: %v", err)
			}

			firstEnd := migrate.KeyTuple{migrate.IntegerKey(3)}
			first := migrate.SQLiteRangeChunk{
				Table:        plan.Table,
				TopologyHash: plan.Pagination.TopologyHash,
				Range:        plan.Pagination.Ranges[0],
				Sequence:     0,
				ChunkRows:    3,
				End:          &firstEnd,
			}
			if err := observer.BeforeSQLiteRangeChunk(context.Background(), first); err != nil {
				t.Fatalf("record first intent: %v", err)
			}
			if err := observer.BeforeSQLiteRangeAttempt(context.Background(), first); err != nil {
				t.Fatalf("record first attempt: %v", err)
			}
			if err := observer.AfterSQLiteRangeChunk(
				context.Background(),
				first,
				migrate.WriteReceipt{
					Certainty:     migrate.CommitDurable,
					AttemptedRows: 3,
					CommittedRows: 3,
				},
				migrate.AckFrontier{
					RangeID: "items/0", NextSequence: 1, Rows: 3,
				},
			); err != nil {
				t.Fatalf("acknowledge first chunk: %v", err)
			}
			if err := observer.AfterSQLiteRangeProgress(
				context.Background(),
				migrate.SQLiteRangeProgress{
					Table:        plan.Table,
					TopologyHash: plan.Pagination.TopologyHash,
					Range:        plan.Pagination.Ranges[0],
					Frontier: migrate.AckFrontier{
						RangeID: "items/0", NextSequence: 1, Rows: 3,
					},
					Watermark: &firstEnd,
				},
			); err != nil {
				t.Fatalf("verify first progress: %v", err)
			}

			lastEnd := migrate.KeyTuple{migrate.IntegerKey(9_007_199_254_740_993)}
			last := migrate.SQLiteRangeChunk{
				Table:        plan.Table,
				TopologyHash: plan.Pagination.TopologyHash,
				Range:        plan.Pagination.Ranges[0],
				Sequence:     1,
				ChunkRows:    2,
				End:          &lastEnd,
			}
			if err := observer.BeforeSQLiteRangeChunk(context.Background(), last); err != nil {
				t.Fatalf("record last intent: %v", err)
			}
			if err := observer.BeforeSQLiteRangeAttempt(context.Background(), last); err != nil {
				t.Fatalf("record last attempt: %v", err)
			}
			if err := observer.AfterSQLiteRangeChunk(
				context.Background(),
				last,
				migrate.WriteReceipt{
					Certainty:     migrate.CommitDurable,
					AttemptedRows: 2,
					CommittedRows: 2,
				},
				migrate.AckFrontier{
					RangeID: "items/0", NextSequence: 2, Rows: 5,
				},
			); err != nil {
				t.Fatalf("acknowledge last chunk: %v", err)
			}
			if err := observer.AfterSQLiteRangeProgress(
				context.Background(),
				migrate.SQLiteRangeProgress{
					Table:        plan.Table,
					TopologyHash: plan.Pagination.TopologyHash,
					Range:        plan.Pagination.Ranges[0],
					Frontier: migrate.AckFrontier{
						RangeID: "items/0", NextSequence: 2, Rows: 5,
					},
					Watermark:            &lastEnd,
					Complete:             true,
					ExpectedNextSequence: 2,
				},
			); err != nil {
				t.Fatalf("complete range: %v", err)
			}

			tasks, ranges, err := backend.(state.RangeBackend).ListWork("range-run")
			if err != nil {
				t.Fatalf("list work: %v", err)
			}
			if len(tasks) != 1 || tasks[0].Status != "completed" {
				t.Fatalf("tasks = %#v, want one completed task", tasks)
			}
			if len(ranges) != 1 || ranges[0].Status != "completed" ||
				ranges[0].RowsDone != 5 || ranges[0].NextSequence != 2 ||
				!ranges[0].FrontierValid ||
				ranges[0].Frontier[0] != state.Int64Value(9_007_199_254_740_993) {
				t.Fatalf("ranges = %#v, want exact completed frontier", ranges)
			}
			restored, err := observer.RestoreSQLiteRanges(context.Background(), plan)
			if err != nil {
				t.Fatalf("restore completed ranges: %v", err)
			}
			if len(restored) != 1 || restored[0].NextSequence != 2 ||
				restored[0].RowsDone != 5 || restored[0].Watermark == nil ||
				(*restored[0].Watermark)[0] != migrate.IntegerKey(9_007_199_254_740_993) {
				t.Fatalf("restored = %#v, want exact large-integer frontier", restored)
			}
		})
	}
}

func TestTableCheckpointObserverRestoresIssuedRowNumberChunk(t *testing.T) {
	t.Parallel()

	backend := initializedRangeBackend(t, ".yaml")
	observer := tableCheckpointObserver{store: backend, runID: "range-run"}
	plan := migrate.SQLiteTransferPlan{
		Table: "unsafe_keys",
		Pagination: migrate.PaginationPlan{
			Strategy: migrate.PaginationRowNumber,
			Keys:     []migrate.KeySpec{{Name: "value"}},
			Ranges: []migrate.PaginationRange{{
				ID: 0, FirstRow: 1, LastRow: 10,
			}},
			TopologyHash: "row-topology",
		},
	}
	if err := observer.AfterSQLiteTransferPlan(context.Background(), plan); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	issued := migrate.SQLiteRangeChunk{
		Table:        plan.Table,
		TopologyHash: plan.Pagination.TopologyHash,
		Range:        plan.Pagination.Ranges[0],
		Sequence:     0,
		ChunkRows:    4,
		EndRow:       4,
	}
	if err := observer.BeforeSQLiteRangeChunk(context.Background(), issued); err != nil {
		t.Fatalf("record issued chunk: %v", err)
	}
	restored, err := observer.RestoreSQLiteRanges(context.Background(), plan)
	if err != nil {
		t.Fatalf("restore issued chunk: %v", err)
	}
	if len(restored) != 1 || restored[0].Issued == nil ||
		restored[0].Issued.Sequence != 0 ||
		restored[0].Issued.ChunkRows != 4 ||
		restored[0].Issued.EndRow != 4 {
		t.Fatalf("restored = %#v, want exact issued ROW_NUMBER chunk", restored)
	}
}

func TestTableCheckpointObserverResetsChangedTopology(t *testing.T) {
	t.Parallel()

	backend := initializedRangeBackend(t, ".db")
	observer := tableCheckpointObserver{
		store: backend, runID: "range-run", resetTopology: true,
	}
	first := migrate.SQLiteTransferPlan{
		Table: "items",
		Pagination: migrate.PaginationPlan{
			Strategy:     migrate.PaginationIntegerKeyset,
			Keys:         []migrate.KeySpec{{Name: "id", Kind: migrate.KeyInteger}},
			Ranges:       []migrate.PaginationRange{{ID: 0}},
			TopologyHash: "one-range",
		},
	}
	if err := observer.AfterSQLiteTransferPlan(context.Background(), first); err != nil {
		t.Fatalf("create first plan: %v", err)
	}
	end := migrate.KeyTuple{migrate.IntegerKey(1)}
	chunk := migrate.SQLiteRangeChunk{
		Table: first.Table, TopologyHash: first.Pagination.TopologyHash,
		Range: first.Pagination.Ranges[0], Sequence: 0, ChunkRows: 1, End: &end,
	}
	if err := observer.BeforeSQLiteRangeChunk(context.Background(), chunk); err != nil {
		t.Fatalf("record old topology progress: %v", err)
	}

	second := first
	second.Pagination.TopologyHash = "two-ranges"
	second.Pagination.Ranges = []migrate.PaginationRange{{ID: 0}, {ID: 1}}
	if err := observer.AfterSQLiteTransferPlan(context.Background(), second); err != nil {
		t.Fatalf("reset changed topology: %v", err)
	}
	restored, err := observer.RestoreSQLiteRanges(context.Background(), second)
	if err != nil {
		t.Fatalf("restore reset topology: %v", err)
	}
	if len(restored) != 2 {
		t.Fatalf("restored %d ranges, want 2", len(restored))
	}
	for _, workRange := range restored {
		if workRange.NextSequence != 0 || workRange.RowsDone != 0 ||
			workRange.Issued != nil {
			t.Fatalf("stale progress survived topology reset: %#v", workRange)
		}
	}
}

func initializedRangeBackend(t *testing.T, extension string) state.Backend {
	t.Helper()
	backend, err := state.NewBackend(filepath.Join(t.TempDir(), "state"+extension))
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	if err := backend.InitializeRun(state.Run{
		ID:        "range-run",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   state.Running,
		Resumable: true,
		Reason:    "test",
		StartedAt: time.Now().UTC(),
	}, "hash"); err != nil {
		t.Fatalf("initialize run: %v", err)
	}
	return backend
}
