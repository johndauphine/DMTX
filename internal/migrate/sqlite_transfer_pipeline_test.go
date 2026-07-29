package migrate

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

type sqlitePipelineTestObserver struct {
	mu sync.Mutex

	plans      []SQLiteTransferPlan
	progress   []SQLiteRangeProgress
	tableRows  map[string]int
	restore    func(SQLiteTransferPlan) ([]SQLiteRangeRestore, error)
	beforeAck  func(context.Context, SQLiteChunkInfo, WriteReceipt) error
	beforeRead func(context.Context, SQLiteChunkInfo) error
	before     func(context.Context, SQLiteRangeChunk) error
	after      func(context.Context, SQLiteRangeChunk, WriteReceipt, AckFrontier) error
	afterTable func(context.Context, string, int) error
}

func (observer *sqlitePipelineTestObserver) BeforeTable(context.Context, string) error {
	return nil
}

func (observer *sqlitePipelineTestObserver) AfterTable(ctx context.Context, table string, rows int) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.tableRows == nil {
		observer.tableRows = make(map[string]int)
	}
	observer.tableRows[table] = rows
	if observer.afterTable != nil {
		return observer.afterTable(ctx, table, rows)
	}
	return nil
}

func (observer *sqlitePipelineTestObserver) AfterSQLiteTransferPlan(_ context.Context, plan SQLiteTransferPlan) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.plans = append(observer.plans, plan)
	return nil
}

func (observer *sqlitePipelineTestObserver) RestoreSQLiteRanges(_ context.Context, plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
	if observer.restore == nil {
		return nil, nil
	}
	return observer.restore(plan)
}

func (observer *sqlitePipelineTestObserver) AfterSQLiteRangeProgress(_ context.Context, progress SQLiteRangeProgress) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.progress = append(observer.progress, progress)
	return nil
}

func (observer *sqlitePipelineTestObserver) BeforeSQLiteChunkRead(ctx context.Context, info SQLiteChunkInfo) error {
	if observer.beforeRead == nil {
		return nil
	}
	return observer.beforeRead(ctx, info)
}

func (observer *sqlitePipelineTestObserver) BeforeSQLiteChunkAcknowledge(ctx context.Context, info SQLiteChunkInfo, receipt WriteReceipt) error {
	if observer.beforeAck == nil {
		return nil
	}
	return observer.beforeAck(ctx, info, receipt)
}

func (observer *sqlitePipelineTestObserver) BeforeSQLiteRangeChunk(ctx context.Context, chunk SQLiteRangeChunk) error {
	if observer.before == nil {
		return nil
	}
	return observer.before(ctx, chunk)
}

func (observer *sqlitePipelineTestObserver) AfterSQLiteRangeChunk(ctx context.Context, chunk SQLiteRangeChunk, receipt WriteReceipt, frontier AckFrontier) error {
	if observer.after == nil {
		return nil
	}
	return observer.after(ctx, chunk, receipt, frontier)
}

func (observer *sqlitePipelineTestObserver) snapshotPlans() []SQLiteTransferPlan {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]SQLiteTransferPlan(nil), observer.plans...)
}

func (observer *sqlitePipelineTestObserver) snapshotProgress() []SQLiteRangeProgress {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]SQLiteRangeProgress(nil), observer.progress...)
}

func sqlitePipelineTestConfig(source, target string) config.Config {
	return config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: source},
		Target: config.Endpoint{Type: "sqlite", Database: target},
		Migration: config.Migration{
			TargetMode:         "drop_recreate",
			ConnectionLimit:    5,
			Workers:            4,
			ChunkSize:          1,
			Partitions:         3,
			ReaderParallelism:  3,
			WriterParallelism:  1,
			ReadAhead:          2,
			MemoryCeilingBytes: 32 << 20,
		},
	}
}

func openSQLitePipelineDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func TestSQLiteTransferExecutesExactPlannedRanges(t *testing.T) {
	t.Run("tuple keys above two to the fifty-third", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.db")
		targetPath := filepath.Join(directory, "target.db")
		source := openSQLitePipelineDatabase(t, sourcePath)
		if _, err := source.Exec(`
			CREATE TABLE items (
				tenant INTEGER NOT NULL,
				code TEXT NOT NULL,
				payload TEXT NOT NULL,
				PRIMARY KEY (tenant, code)
			);
			INSERT INTO items VALUES
				(9007199254740993, 'a', 'one'),
				(9007199254740993, 'b', 'two'),
				(9007199254740994, 'a', 'three'),
				(9007199254740995, 'a', 'four'),
				(9007199254740996, 'z', 'five');
		`); err != nil {
			t.Fatal(err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}

		observer := &sqlitePipelineTestObserver{}
		result, err := SQLiteToSQLiteWithObserver(
			context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath), observer,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rows != 5 || result.Tables != 1 || !result.Validated {
			t.Fatalf("result = %+v", result)
		}
		plans := observer.snapshotPlans()
		if len(plans) != 1 || plans[0].Pagination.Strategy != PaginationTupleKeyset ||
			len(plans[0].Pagination.Ranges) != 3 {
			t.Fatalf("plan = %#v", plans)
		}
		firstBoundary, err := (*plans[0].Pagination.Ranges[0].Upper)[0].SQLValue()
		if err != nil || firstBoundary != int64(9_007_199_254_740_993) {
			t.Fatalf("first tuple boundary = %#v, %v", firstBoundary, err)
		}
		assertSQLitePipelineRowCount(t, targetPath, "items", 5)
	})

	t.Run("unsafe tuple uses exact row number intervals", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.db")
		targetPath := filepath.Join(directory, "target.db")
		source := openSQLitePipelineDatabase(t, sourcePath)
		if _, err := source.Exec(`
			CREATE TABLE measurements (
				score REAL NOT NULL,
				tenant INTEGER NOT NULL,
				payload TEXT NOT NULL,
				dmtx_row_number INTEGER NOT NULL,
				PRIMARY KEY (score, tenant)
			);
			INSERT INTO measurements VALUES
				(-1.5, 1, 'a', 99), (0.0, 1, 'b', 98), (0.0, 2, 'c', 97),
				(4.25, 1, 'd', 96), (100.5, 9, 'e', 95), (101.5, 9, 'f', 94);
		`); err != nil {
			t.Fatal(err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}

		observer := &sqlitePipelineTestObserver{}
		result, err := SQLiteToSQLiteWithObserver(
			context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath), observer,
		)
		if err != nil {
			t.Fatal(err)
		}
		plans := observer.snapshotPlans()
		if result.Rows != 6 || len(plans) != 1 ||
			plans[0].Pagination.Strategy != PaginationRowNumber ||
			len(plans[0].Pagination.Ranges) != 3 {
			t.Fatalf("result = %+v, plans = %#v", result, plans)
		}
		assertSQLitePipelineRowCount(t, targetPath, "measurements", 6)
	})

	t.Run("signed integer extremes and gaps", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.db")
		targetPath := filepath.Join(directory, "target.db")
		source := openSQLitePipelineDatabase(t, sourcePath)
		if _, err := source.Exec(`CREATE TABLE extremes (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		for _, id := range []int64{math.MinInt64, -5, 0, 7, math.MaxInt64} {
			if _, err := source.Exec(`INSERT INTO extremes(id, payload) VALUES (?, ?)`, id, "row"); err != nil {
				t.Fatal(err)
			}
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}

		observer := &sqlitePipelineTestObserver{}
		result, err := SQLiteToSQLiteWithObserver(
			context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath), observer,
		)
		if err != nil {
			t.Fatal(err)
		}
		plans := observer.snapshotPlans()
		if result.Rows != 5 || len(plans) != 1 ||
			plans[0].Pagination.Strategy != PaginationIntegerKeyset ||
			len(plans[0].Pagination.Ranges) != 3 {
			t.Fatalf("result = %+v, plans = %#v", result, plans)
		}
		assertSQLitePipelineRowCount(t, targetPath, "extremes", 5)
	})
}

func TestSQLiteEmptyTableHasExplicitCompletedRange(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`CREATE TABLE empty_items (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	observer := &sqlitePipelineTestObserver{}
	result, err := SQLiteToSQLiteWithObserver(
		context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath), observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	plans := observer.snapshotPlans()
	progress := observer.snapshotProgress()
	if result.Tables != 1 || result.Rows != 0 || len(plans) != 1 ||
		len(plans[0].Pagination.Ranges) != 1 || !plans[0].Pagination.Ranges[0].Empty {
		t.Fatalf("result = %+v, plans = %#v", result, plans)
	}
	if len(progress) != 1 || !progress[0].Complete || progress[0].Frontier.Rows != 0 {
		t.Fatalf("empty range progress = %#v", progress)
	}
}

func TestSQLiteStrictConsistencyFailsPolicyBeforeTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope = "table"
	result, err := SQLiteToSQLite(context.Background(), cfg)
	if err == nil {
		t.Fatal("strict SQLite migration unexpectedly succeeded")
	}
	if result != (Result{}) {
		t.Fatalf("strict preflight result = %+v", result)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("strict error class = %s, error = %v", ClassifyTransferError(err), err)
	}
	var policyError *schema.PolicyError
	if !errors.As(err, &policyError) || policyError.Operation != "enable strict consistency" {
		t.Fatalf("strict policy error = %#v, wrapped error = %v", policyError, err)
	}
	if _, statErr := os.Stat(targetPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("strict-policy rejection touched target: %v", statErr)
	}
}

func TestSQLiteNullableCompositePrimaryKeyFailsBeforeTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE unsafe_rows (
			tenant TEXT,
			item_id TEXT,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, item_id)
		);
		INSERT INTO unsafe_rows VALUES
			(NULL, 'same', 'first'),
			(NULL, 'same', 'second');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	if _, err := target.Exec(`CREATE TABLE sentinel (value TEXT NOT NULL); INSERT INTO sentinel VALUES ('untouched')`); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := SQLiteToSQLite(
		context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath),
	)
	if err == nil {
		t.Fatal("nullable composite primary key unexpectedly migrated")
	}
	if result != (Result{}) || ClassifyTransferError(err) != ErrorClassPrimaryKey ||
		!strings.Contains(err.Error(), "cannot prove deterministic duplicate-safe replay") {
		t.Fatalf("result = %+v, class = %s, error = %v", result, ClassifyTransferError(err), err)
	}
	target = openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	var sentinel string
	if err := target.QueryRow(`SELECT value FROM sentinel`).Scan(&sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel != "untouched" {
		t.Fatalf("sentinel = %q", sentinel)
	}
	var unsafeTableCount int
	if err := target.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'unsafe_rows'`).Scan(&unsafeTableCount); err != nil {
		t.Fatal(err)
	}
	if unsafeTableCount != 0 {
		t.Fatal("nullable-key rejection mutated the target schema")
	}
}

func TestSQLiteResumeSkipsCompletedRangesWithoutCompletingAgain(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.Partitions = 2
	if _, err := SQLiteToSQLite(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	if _, err := target.Exec(`UPDATE items SET payload = 'target-preserved' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.Migration.DestructiveAcknowledged = true
	observer := &sqlitePipelineTestObserver{}
	observer.restore = func(plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
		restored := make([]SQLiteRangeRestore, 0, len(plan.Pagination.Ranges))
		for _, transferRange := range plan.Pagination.Ranges {
			restored = append(restored, SQLiteRangeRestore{
				Table:        plan.Table,
				TopologyHash: plan.Pagination.TopologyHash,
				Range:        clonePaginationRange(transferRange),
				NextSequence: 2,
				Complete:     true,
				RowsDone:     2,
				Watermark:    cloneKeyTuplePointer(transferRange.Upper),
			})
		}
		return restored, nil
	}
	result, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(), cfg, nil, nil, observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 || result.Tables != 1 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	if progress := observer.snapshotProgress(); len(progress) != 0 {
		t.Fatalf("completed ranges were re-emitted: %#v", progress)
	}
	target = openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	var payload string
	if err := target.QueryRow(`SELECT payload FROM items WHERE id = 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "target-preserved" {
		t.Fatalf("completed range was reread: payload = %q", payload)
	}
}

func TestSQLiteZeroStateTopologyResetDropsStaleTargetRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	if _, err := target.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (99, 'stale');
	`); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.DestructiveAcknowledged = true
	observer := &sqlitePipelineTestObserver{}
	observer.restore = func(plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
		restored := make([]SQLiteRangeRestore, 0, len(plan.Pagination.Ranges))
		for _, transferRange := range plan.Pagination.Ranges {
			restored = append(restored, SQLiteRangeRestore{
				Table: plan.Table, TopologyHash: plan.Pagination.TopologyHash,
				Range: clonePaginationRange(transferRange),
			})
		}
		return restored, nil
	}
	result, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(), cfg, nil, nil, observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	target = openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	var stale int
	if err := target.QueryRow(`SELECT COUNT(*) FROM items WHERE id = 99`).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatal("zero-state topology reset reused the stale target")
	}
}

func TestSQLiteNonzeroResumeRequiresExistingTargetBeforeMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.Partitions = 1
	observer := &sqlitePipelineTestObserver{}
	observer.restore = func(plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
		watermark := KeyTuple{IntegerKey(1)}
		return []SQLiteRangeRestore{{
			Table:          plan.Table,
			TopologyHash:   plan.Pagination.TopologyHash,
			Range:          clonePaginationRange(plan.Pagination.Ranges[0]),
			NextSequence:   1,
			RowsDone:       1,
			Watermark:      &watermark,
			SequenceOffset: 0,
		}}, nil
	}
	result, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(), cfg, nil, nil, observer,
	)
	if err == nil || !strings.Contains(err.Error(), "resumable target table items is missing") {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result before mutation = %+v", result)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	exists, existsErr := tableExists(context.Background(), target, "items")
	if existsErr != nil {
		t.Fatal(existsErr)
	}
	if exists {
		t.Fatal("resume created a target despite missing required prefix rows")
	}
}

func TestSQLiteIssuedReplayUsesInsertOnlyConflictIgnore(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'source-now'), (2, 'second');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.Partitions = 1
	if _, err := SQLiteToSQLite(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	if _, err := target.Exec(`
		UPDATE items SET payload = 'durable-before-crash' WHERE id = 1;
		DELETE FROM items WHERE id = 2;
	`); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.Migration.DestructiveAcknowledged = true
	observer := &sqlitePipelineTestObserver{}
	observer.restore = func(plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
		transferRange := clonePaginationRange(plan.Pagination.Ranges[0])
		end := KeyTuple{IntegerKey(1)}
		issued := SQLiteRangeChunk{
			Table: plan.Table, TopologyHash: plan.Pagination.TopologyHash,
			Range: transferRange, Sequence: 0, ChunkRows: 1, End: &end,
		}
		return []SQLiteRangeRestore{{
			Table: plan.Table, TopologyHash: plan.Pagination.TopologyHash,
			Range: transferRange, Issued: &issued,
		}}, nil
	}
	result, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(), cfg, nil, nil, observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	target = openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	var first, second string
	if err := target.QueryRow(`SELECT payload FROM items WHERE id = 1`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(`SELECT payload FROM items WHERE id = 2`).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != "durable-before-crash" || second != "second" {
		t.Fatalf("replayed values = (%q, %q)", first, second)
	}
}

func TestSQLitePipelineHonorsByteBudgetForWideRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`CREATE TABLE wide_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 300_000)
	for id := 1; id <= 6; id++ {
		if _, err := source.Exec(`INSERT INTO wide_rows VALUES (?, ?)`, id, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.ChunkSize = 10
	cfg.Migration.MemoryCeilingBytes = 1 << 20
	observer := &sqlitePipelineTestObserver{}
	result, err := SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 6 {
		t.Fatalf("result = %+v", result)
	}
	var peak int64
	for _, progress := range observer.snapshotProgress() {
		if progress.Memory.Peak > peak {
			peak = progress.Memory.Peak
		}
	}
	if peak <= 0 || peak > 1<<20 {
		t.Fatalf("retained byte peak = %d, budget = %d", peak, 1<<20)
	}
}

func TestSQLiteWideRowsReserveBeforeConcurrentScan(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`CREATE TABLE wide_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("w", 4_000_000)
	for id := 1; id <= 4; id++ {
		if _, err := source.Exec(`INSERT INTO wide_rows VALUES (?, ?)`, id, payload); err != nil {
			t.Fatal(err)
		}
	}
	table, columns, err := inspectTable(context.Background(), source, "wide_rows")
	if err != nil {
		t.Fatal(err)
	}
	rowReservation, err := sqliteMaximumRowReservation(
		context.Background(), source, table.Name, columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	expectedPeak := rowReservation * 2
	memoryBudget := int64(16 << 20)

	entered := make(chan int, 2)
	release := make(chan struct{})
	var seenMu sync.Mutex
	seen := make(map[int]bool)
	observer := &sqlitePipelineTestObserver{}
	observer.beforeRead = func(ctx context.Context, info SQLiteChunkInfo) error {
		seenMu.Lock()
		firstForRange := !seen[info.RangeID]
		seen[info.RangeID] = true
		seenMu.Unlock()
		if !firstForRange {
			return nil
		}
		select {
		case entered <- info.RangeID:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.Partitions = 2
	cfg.Migration.ReaderParallelism = 2
	cfg.Migration.ChunkSize = 1
	cfg.Migration.MemoryCeilingBytes = memoryBudget
	type sqliteRunResult struct {
		result Result
		err    error
	}
	resultChannel := make(chan sqliteRunResult, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		result, runErr := SQLiteToSQLiteWithObserver(runCtx, cfg, observer)
		resultChannel <- sqliteRunResult{result: result, err: runErr}
	}()

	var first int
	select {
	case first = <-entered:
	case completed := <-resultChannel:
		t.Fatalf("migration ended before a wide-row reader entered: result=%+v error=%v", completed.result, completed.err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("first wide-row reader did not reach the admitted scan boundary")
	}
	var second int
	select {
	case second = <-entered:
	case completed := <-resultChannel:
		close(release)
		t.Fatalf("migration ended before concurrent wide-row admission: result=%+v error=%v", completed.result, completed.err)
	case <-time.After(5 * time.Second):
		close(release)
		cancel()
		t.Fatal("second wide-row reader could not reserve and scan concurrently")
	}
	if first == second {
		close(release)
		cancel()
		t.Fatalf("concurrent reads came from one range %d", first)
	}
	close(release)
	var completed sqliteRunResult
	select {
	case completed = <-resultChannel:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("migration did not finish after releasing admitted readers")
	}
	if completed.err != nil || completed.result.Rows != 4 || !completed.result.Validated {
		t.Fatalf("result = %+v, error = %v", completed.result, completed.err)
	}
	var peak int64
	for _, progress := range observer.snapshotProgress() {
		if progress.Memory.Peak > peak {
			peak = progress.Memory.Peak
		}
	}
	if peak != expectedPeak || peak > memoryBudget {
		t.Fatalf("concurrent retained peak = %d, want %d within budget %d", peak, expectedPeak, memoryBudget)
	}
}

func TestSQLitePartialResultKeepsRowsAfterAggregateCheckpointFailure(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	observer := &sqlitePipelineTestObserver{afterTable: func(context.Context, string, int) error {
		return errors.New("aggregate checkpoint failed")
	}}
	result, err := SQLiteToSQLiteWithObserver(
		context.Background(), sqlitePipelineTestConfig(sourcePath, targetPath), observer,
	)
	if err == nil || !strings.Contains(err.Error(), "aggregate checkpoint failed") {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
	if result.Rows != 2 || result.Tables != 0 || result.Validated {
		t.Fatalf("partial result = %+v", result)
	}
	assertSQLitePipelineRowCount(t, targetPath, "items", 2)
}

func TestSQLiteAcknowledgementsAreSerialized(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	var firstOnce, secondOnce sync.Once
	observer := &sqlitePipelineTestObserver{}
	observer.beforeAck = func(ctx context.Context, info SQLiteChunkInfo, _ WriteReceipt) error {
		switch info.Sequence {
		case 0:
			firstOnce.Do(func() { close(firstEntered) })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseFirst:
			}
		case 1:
			secondOnce.Do(func() { close(secondEntered) })
		}
		return nil
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.Partitions = 1
	resultChannel := make(chan error, 1)
	go func() {
		_, err := SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
		resultChannel <- err
	}()
	select {
	case <-firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first acknowledgement did not arrive")
	}
	select {
	case <-secondEntered:
		t.Fatal("later acknowledgement passed the blocked first callback")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case err := <-resultChannel:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish after releasing the first acknowledgement")
	}
}

func TestSQLitePipelineReleasesReservationsOnObserverFailure(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
	`); err != nil {
		t.Fatal(err)
	}
	table, columns, err := inspectTable(context.Background(), source, "items")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanSQLitePagination(context.Background(), source, table, 2)
	if err != nil {
		t.Fatal(err)
	}
	maxRowBytes, err := sqliteMaximumRowReservation(context.Background(), source, table.Name, columns)
	if err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	defer source.Close()
	defer target.Close()
	observer := &sqlitePipelineTestObserver{before: func(context.Context, SQLiteRangeChunk) error {
		return errors.New("stop before target mutation")
	}}
	if _, err := prepareTargetWithStatus(context.Background(), target, table, "drop_recreate", observer); err != nil {
		t.Fatal(err)
	}
	budget, err := NewByteBudget(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeSQLiteTransferPlan(
		context.Background(), source, target,
		sqlitePlannedTable{table: table, columns: columns, pagination: plan, maxRowBytes: maxRowBytes},
		sqliteEffectiveTransferSettings{
			targetMode: "drop_recreate", chunkRows: 1, partitions: 2,
			readers: 2, queueDepth: 2, memory: 1 << 20,
		},
		budget, observer, nil,
	)
	if err == nil {
		t.Fatal("expected observer failure")
	}
	if got := budget.Stats().Current; got != 0 {
		t.Fatalf("observer failure leaked %d retained bytes", got)
	}
}

func assertSQLitePipelineRowCount(t *testing.T, path, table string, want int) {
	t.Helper()
	database := openSQLitePipelineDatabase(t, path)
	defer database.Close()
	var got int
	if err := database.QueryRow(`SELECT COUNT(*) FROM ` + quote(table)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows = %d, want %d", table, got, want)
	}
}
