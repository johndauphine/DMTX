package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

var errSQLiteAttemptCheckpoint = errors.New("injected attempt checkpoint failure")

type sqliteRangeAttemptTestObserver struct {
	mu sync.Mutex

	events                  []string
	attempts                []SQLiteRangeChunk
	protections             int
	transientBeforeMutation int
	attemptErr              error
	restore                 func(SQLiteTransferPlan) ([]SQLiteRangeRestore, error)
}

func (*sqliteRangeAttemptTestObserver) BeforeTable(context.Context, string) error {
	return nil
}

func (*sqliteRangeAttemptTestObserver) AfterTable(context.Context, string, int) error {
	return nil
}

func (observer *sqliteRangeAttemptTestObserver) BeforeSQLiteRangeChunk(
	_ context.Context,
	_ SQLiteRangeChunk,
) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.events = append(observer.events, "intent")
	return nil
}

func (*sqliteRangeAttemptTestObserver) AfterSQLiteRangeChunk(
	context.Context,
	SQLiteRangeChunk,
	WriteReceipt,
	AckFrontier,
) error {
	return nil
}

func (observer *sqliteRangeAttemptTestObserver) BeforeSQLiteRangeAttempt(
	_ context.Context,
	chunk SQLiteRangeChunk,
) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.events = append(observer.events, "attempt")
	observer.attempts = append(observer.attempts, cloneSQLiteRangeChunk(chunk))
	return observer.attemptErr
}

func (observer *sqliteRangeAttemptTestObserver) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	observer.mu.Lock()
	observer.protections++
	protection := observer.protections
	observer.events = append(observer.events, "protect")
	fail := protection <= observer.transientBeforeMutation
	observer.mu.Unlock()
	if fail {
		return sqliteRetryTestError{code: 5}
	}
	return mutation()
}

func (observer *sqliteRangeAttemptTestObserver) RestoreSQLiteRanges(
	_ context.Context,
	plan SQLiteTransferPlan,
) ([]SQLiteRangeRestore, error) {
	if observer.restore == nil {
		return nil, nil
	}
	return observer.restore(plan)
}

func (observer *sqliteRangeAttemptTestObserver) snapshot() (
	[]string,
	[]SQLiteRangeChunk,
	int,
) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	events := append([]string(nil), observer.events...)
	attempts := make([]SQLiteRangeChunk, len(observer.attempts))
	for index, chunk := range observer.attempts {
		attempts[index] = cloneSQLiteRangeChunk(chunk)
	}
	return events, attempts, observer.protections
}

func TestSQLiteRangeAttemptObserverRunsBeforeEveryRetryMutation(t *testing.T) {
	sourcePath, targetPath := sqliteRangeAttemptTestDatabases(t)
	cfg := sqliteRangeAttemptTestConfig(sourcePath, targetPath, 2)
	observer := &sqliteRangeAttemptTestObserver{transientBeforeMutation: 2}

	result, err := SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 1 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	events, attempts, protections := observer.snapshot()
	wantEvents := []string{
		"intent",
		"attempt", "protect",
		"attempt", "protect",
		"attempt", "protect",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(attempts) != 3 || protections != 3 {
		t.Fatalf("attempt callbacks = %d, target protections = %d", len(attempts), protections)
	}
	for index, chunk := range attempts {
		if chunk.Replay || chunk.Sequence != 0 || chunk.ChunkRows != 1 {
			t.Fatalf("attempt %d chunk = %+v", index, chunk)
		}
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	assertSQLiteRetryRowCount(t, target, 1)
}

func TestSQLiteRangeAttemptObserverFailureStopsBeforeTargetMutation(t *testing.T) {
	sourcePath, targetPath := sqliteRangeAttemptTestDatabases(t)
	cfg := sqliteRangeAttemptTestConfig(sourcePath, targetPath, 5)
	observer := &sqliteRangeAttemptTestObserver{
		attemptErr: errors.Join(
			errSQLiteAttemptCheckpoint,
			sqliteRetryTestError{code: 5},
		),
	}

	result, err := SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
	if err == nil || !errors.Is(err, errSQLiteAttemptCheckpoint) {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
	if ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("error class = %s, error = %v", ClassifyTransferError(err), err)
	}
	if result != (Result{}) {
		t.Fatalf("result before target mutation = %+v", result)
	}
	events, attempts, protections := observer.snapshot()
	if !reflect.DeepEqual(events, []string{"intent", "attempt"}) {
		t.Fatalf("events = %#v", events)
	}
	if len(attempts) != 1 || protections != 0 {
		t.Fatalf("attempt callbacks = %d, target protections = %d", len(attempts), protections)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	assertSQLiteRetryRowCount(t, target, 0)
}

func TestSQLiteRangeAttemptObserverRunsForIssuedReplay(t *testing.T) {
	sourcePath, targetPath := sqliteRangeAttemptTestDatabases(t)
	initial := sqlitePipelineTestConfig(sourcePath, targetPath)
	initial.Migration.Partitions = 1
	if _, err := SQLiteToSQLite(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	target := openSQLitePipelineDatabase(t, targetPath)
	if _, err := target.Exec(`DELETE FROM items`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := sqliteRangeAttemptTestConfig(sourcePath, targetPath, 0)
	observer := &sqliteRangeAttemptTestObserver{}
	observer.restore = func(plan SQLiteTransferPlan) ([]SQLiteRangeRestore, error) {
		transferRange := clonePaginationRange(plan.Pagination.Ranges[0])
		end := KeyTuple{IntegerKey(1)}
		issued := SQLiteRangeChunk{
			Table:        plan.Table,
			TopologyHash: plan.Pagination.TopologyHash,
			Range:        transferRange,
			Sequence:     0,
			ChunkRows:    1,
			End:          &end,
		}
		return []SQLiteRangeRestore{{
			Table:        plan.Table,
			TopologyHash: plan.Pagination.TopologyHash,
			Range:        transferRange,
			Issued:       &issued,
		}}, nil
	}

	result, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(), cfg, nil, nil, observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 1 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	events, attempts, protections := observer.snapshot()
	if !reflect.DeepEqual(events, []string{"attempt", "protect"}) {
		t.Fatalf("replay events = %#v", events)
	}
	if len(attempts) != 1 || !attempts[0].Replay || protections != 1 {
		t.Fatalf("attempts = %+v, target protections = %d", attempts, protections)
	}
	target = openSQLitePipelineDatabase(t, targetPath)
	defer target.Close()
	assertSQLiteRetryRowCount(t, target, 1)
}

func sqliteRangeAttemptTestDatabases(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	target := openSQLitePipelineDatabase(t, targetPath)
	for _, database := range []*sql.DB{source, target} {
		if _, err := database.Exec(`
			CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
		`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Exec(`INSERT INTO items VALUES (1, 'one')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	return sourcePath, targetPath
}

func sqliteRangeAttemptTestConfig(
	sourcePath, targetPath string,
	maxRetries int,
) config.Config {
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 1
	cfg.Migration.ReaderParallelism = 1
	cfg.Migration.ChunkSize = 1
	cfg.Migration.MaxRetries = maxRetries
	return cfg
}
