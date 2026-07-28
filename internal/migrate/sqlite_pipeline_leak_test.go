package migrate

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestSQLitePipelineRepeatedCancellationDoesNotLeakResources(t *testing.T) {
	directory := t.TempDir()
	sourcePath := directory + "/source.db"
	targetPath := directory + "/target.db"
	source := openSQLitePipelineDatabase(t, sourcePath)
	defer source.Close()
	target := openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()

	if _, err := source.Exec(`
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			payload TEXT NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 100; id++ {
		if _, err := source.Exec(
			`INSERT INTO items VALUES (?, ?)`,
			id,
			"bounded-payload",
		); err != nil {
			t.Fatal(err)
		}
	}
	table, columns, err := inspectTable(
		context.Background(),
		source,
		"items",
	)
	if err != nil {
		t.Fatal(err)
	}
	pagination, err := PlanSQLitePagination(
		context.Background(),
		source,
		table,
		4,
	)
	if err != nil {
		t.Fatal(err)
	}
	maxRowBytes, err := sqliteMaximumRowReservation(
		context.Background(),
		source,
		table.Name,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	planned := sqlitePlannedTable{
		table:       table,
		columns:     columns,
		pagination:  pagination,
		maxRowBytes: maxRowBytes,
	}
	settings := sqliteEffectiveTransferSettings{
		targetMode: "drop_recreate",
		chunkRows:  5,
		partitions: 4,
		readers:    4,
		queueDepth: 4,
		memory:     1 << 20,
		maxRetries: 0,
	}
	injected := errors.New("injected cancellation before target mutation")

	runOnce := func() {
		t.Helper()
		if _, err := prepareTargetWithStatus(
			context.Background(),
			target,
			table,
			"drop_recreate",
			nil,
		); err != nil {
			t.Fatal(err)
		}
		budget, err := NewByteBudget(settings.memory)
		if err != nil {
			t.Fatal(err)
		}
		observer := &sqlitePipelineTestObserver{
			before: func(context.Context, SQLiteRangeChunk) error {
				return injected
			},
		}
		if _, err := executeSQLiteTransferPlan(
			context.Background(),
			source,
			target,
			planned,
			settings,
			budget,
			observer,
			nil,
		); err == nil {
			t.Fatal("expected injected observer failure")
		}
		if current := budget.Stats().Current; current != 0 {
			t.Fatalf("cancelled pipeline retained %d admitted bytes", current)
		}
		if inUse := source.Stats().InUse; inUse != 0 {
			t.Fatalf("source connections still in use after cancellation: %d", inUse)
		}
		if inUse := target.Stats().InUse; inUse != 0 {
			t.Fatalf("target connections still in use after cancellation: %d", inUse)
		}
	}

	// Warm driver and database/sql one-time paths before taking the baseline.
	runOnce()
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const repetitions = 40
	for iteration := 0; iteration < repetitions; iteration++ {
		runOnce()
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if current := runtime.NumGoroutine(); current > baseline+2 {
		t.Fatalf(
			"goroutines after %d cancelled pipelines = %d, baseline = %d",
			repetitions,
			current,
			baseline,
		)
	}
}
