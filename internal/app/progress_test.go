package app

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/johndauphine/dmtx/internal/state"
)

// TestAWatcherCannotStopAMigration is the property that makes this feature safe
// to have at all.
//
// Progress is reported from inside the observer hooks, and those hooks run at
// durable checkpoint boundaries where a returned error aborts the run. So a
// sink that panics - a closed channel, a nil map, a bug in a front end - must
// not unwind into the engine. Watching a migration must never be able to end
// one.
func TestAWatcherCannotStopAMigration(t *testing.T) {
	reporter := newProgressReporter(func(Progress) {
		panic("a badly behaved watcher")
	})

	// Each of these is called from a hook whose error return aborts a run.
	reporter.planned([]string{"orders", "customers"})
	reporter.starting("orders")
	reporter.finished("orders", 42)

	// Reaching here at all is the assertion: a panic would have failed the test
	// by unwinding out of the calls above.
	if reporter.done != 1 {
		t.Errorf("the tally stopped being kept: done = %d, want 1", reporter.done)
	}
}

// TestNoWatcherCostsNothing pins that the nil sink is a supported case, since
// it is the one every command-line invocation takes.
func TestNoWatcherCostsNothing(t *testing.T) {
	if reporter := newProgressReporter(nil); reporter != nil {
		t.Fatal("a nil sink produced a reporter")
	}
	// Every method has to tolerate the nil receiver, because the observer holds
	// one and calls it unconditionally.
	var reporter *progressReporter
	reporter.planned([]string{"orders"})
	reporter.starting("orders")
	reporter.finished("orders", 1)
	reporter.report(Progress{Kind: ProgressTableStarted})
}

// TestProgressCarriesTheTally pins that every report says where the run is, so
// a watcher never has to keep its own count and a client that missed events can
// still render correctly.
func TestProgressCarriesTheTally(t *testing.T) {
	var seen []Progress
	reporter := newProgressReporter(func(event Progress) {
		seen = append(seen, event)
	})

	reporter.planned([]string{"orders", "customers", "items"})
	reporter.starting("orders")
	reporter.finished("orders", 10)
	reporter.starting("customers")
	reporter.finished("customers", 20)

	if len(seen) != 5 {
		t.Fatalf("expected five reports, got %d", len(seen))
	}
	for index, event := range seen {
		if event.Total != 3 {
			t.Errorf("report %d says the run has %d tables, want 3", index, event.Total)
		}
	}
	if seen[0].Kind != ProgressTablesPlanned || len(seen[0].Tables) != 3 {
		t.Errorf("the first report is not the planned table set: %+v", seen[0])
	}
	// Done counts completed tables, so it advances on finished and nowhere else.
	for index, want := range []int{0, 0, 1, 1, 2} {
		if seen[index].Done != want {
			t.Errorf("report %d says %d done, want %d", index, seen[index].Done, want)
		}
	}
}

// TestPlannedCopiesTheTableSet pins that a delivered report does not alias the
// engine's own slice, which the engine is free to reuse or reorder afterwards.
func TestPlannedCopiesTheTableSet(t *testing.T) {
	var delivered []string
	reporter := newProgressReporter(func(event Progress) {
		if event.Kind == ProgressTablesPlanned {
			delivered = event.Tables
		}
	})

	tables := []string{"orders", "customers"}
	reporter.planned(tables)
	tables[0] = "something-else"

	if delivered[0] != "orders" {
		t.Errorf("the reported table set changed under the watcher: %v", delivered)
	}
}

// TestConcurrentReportsAreSafe pins that the tally survives tables completing on
// more than one goroutine, which the engine is free to do.
func TestConcurrentReportsAreSafe(t *testing.T) {
	var mutex sync.Mutex
	counted := 0
	reporter := newProgressReporter(func(Progress) {
		mutex.Lock()
		counted++
		mutex.Unlock()
	})
	reporter.planned(make([]string, 50))

	var group sync.WaitGroup
	for index := 0; index < 50; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			reporter.starting("t")
			reporter.finished("t", 1)
		}()
	}
	group.Wait()

	if reporter.done != 50 {
		t.Errorf("tally lost reports: done = %d, want 50", reporter.done)
	}
	if counted != 101 {
		t.Errorf("watcher saw %d reports, want 101", counted)
	}
}

// TestCheckpointHooksReportProgress pins the wiring, which is the whole point:
// the reporter is only useful if the hooks the engine actually calls reach it.
//
// It drives the observer directly with a real state backend, the way the engine
// does, rather than trusting that the calls were added.
func TestCheckpointHooksReportProgress(t *testing.T) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "migration.state.db")
	raw := state.SQLiteStore{Path: statePath}
	const runID = "progress-run"

	// The plain store, not a lease-fenced one. These three hooks only write
	// task checkpoints, and driving them needs no lease; the Stage 4 paths that
	// do are not what this test is about.
	if err := raw.Append(state.Run{ID: runID, Outcome: state.Partial}); err != nil {
		t.Fatal(err)
	}

	var seen []Progress
	var mutex sync.Mutex
	observer := tableCheckpointObserver{
		store: raw,
		runID: runID,
		progress: newProgressReporter(func(event Progress) {
			mutex.Lock()
			defer mutex.Unlock()
			seen = append(seen, event)
		}),
	}

	ctx := context.Background()
	if err := observer.BeforeTables(ctx, []string{"orders", "customers"}); err != nil {
		t.Fatalf("BeforeTables: %v", err)
	}
	if err := observer.BeforeTable(ctx, "orders"); err != nil {
		t.Fatalf("BeforeTable: %v", err)
	}
	if err := observer.AfterTable(ctx, "orders", 7); err != nil {
		t.Fatalf("AfterTable: %v", err)
	}

	kinds := make([]string, 0, len(seen))
	for _, event := range seen {
		kinds = append(kinds, event.Kind)
	}
	want := []string{ProgressTablesPlanned, ProgressTableStarted, ProgressTableFinished}
	if len(kinds) != len(want) {
		t.Fatalf("hooks reported %v, want %v", kinds, want)
	}
	for index := range want {
		if kinds[index] != want[index] {
			t.Fatalf("hooks reported %v, want %v", kinds, want)
		}
	}
	if last := seen[len(seen)-1]; last.Table != "orders" || last.Rows != 7 || last.Done != 1 {
		t.Errorf("the finished report lost its detail: %+v", last)
	}
}
