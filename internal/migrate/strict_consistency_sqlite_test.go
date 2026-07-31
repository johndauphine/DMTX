package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmtx/internal/state"
)

func newSQLiteStrictSource(t *testing.T, rows int) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Exec(
		`CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)`,
	); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= rows; index++ {
		if _, err := database.Exec(
			`INSERT INTO items (id, payload) VALUES (?, 'seed')`,
			index,
		); err != nil {
			t.Fatal(err)
		}
	}
	return database, path
}

func sqliteStrictRequest(tables ...string) StrictConsistencyOpenRequest {
	selected := make([]StrictConsistencyTable, 0, len(tables))
	for _, table := range tables {
		selected = append(selected, StrictConsistencyTable{
			Task:      state.TaskKey{Type: "table-copy", Table: table},
			AttemptID: "attempt-" + table,
		})
	}
	return StrictConsistencyOpenRequest{
		RunID:        "sqlite-strict-run",
		SourceEngine: StrictConsistencySQLite,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "epoch-1",
		Tables:       selected,
	}
}

// TestSQLiteStrictTableSnapshot is the Section 10 SQLite contract: one
// serializable reader over a stable view.
//
// Unlike PostgreSQL, SQLite in its default rollback-journal mode holds a shared
// lock for the life of a read transaction, so a concurrent writer is refused
// with SQLITE_BUSY rather than committing invisibly. That is a real cost of the
// SQLite strict route and this test states it as the contract rather than
// hiding it: the view is stable *because* writers wait. The opener deliberately
// does not switch the source to WAL to soften this — journal mode is a
// persistent property of the user's database and strict consistency has no
// business silently reconfiguring the source.
func TestSQLiteStrictTableSnapshot(t *testing.T) {
	source, path := newSQLiteStrictSource(t, 3)
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(
		context.Background(),
		sqliteStrictRequest("items"),
	)
	if err != nil {
		t.Fatal(err)
	}

	writer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Exec(
		`INSERT INTO items (id, payload) VALUES (99, 'after-view')`,
	); err == nil {
		t.Fatal("a concurrent writer committed into the stable view")
	}

	capture, err := session.CaptureSameViewEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.Tables) != 1 {
		t.Fatalf("capture = %#v", capture)
	}
	if capture.Tables[0].ExactSourceRowCount != 3 {
		t.Fatalf(
			"stable view saw %d rows, want the 3 present when it opened",
			capture.Tables[0].ExactSourceRowCount,
		)
	}
	if capture.Tables[0].SnapshotReference == "" ||
		!strings.HasPrefix(capture.Tables[0].SnapshotReference, "sqlite-view-") {
		t.Fatalf("snapshot reference = %q", capture.Tables[0].SnapshotReference)
	}
	if err := validateSnapshotReference(
		capture.Tables[0].SnapshotReference,
	); err != nil {
		t.Fatalf("snapshot reference rejected by the core: %v", err)
	}
	// Table scope must leave every migration field empty.
	if capture.MigrationEpochID != "" ||
		capture.MigrationSnapshotReference != "" ||
		!capture.MigrationCapturedAt.IsZero() {
		t.Fatalf("table scope emitted migration evidence: %#v", capture)
	}

	// Closing releases the source: the previously blocked write now lands, and
	// a fresh view observes it. This is what proves the block was the view
	// holding its lock rather than an unrelated failure.
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(
		`INSERT INTO items (id, payload) VALUES (99, 'after-close')`,
	); err != nil {
		t.Fatalf("write after strict close: %v", err)
	}
	reopened, err := opener.OpenStrictConsistency(
		context.Background(),
		sqliteStrictRequest("items"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	next, err := reopened.CaptureSameViewEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Tables[0].ExactSourceRowCount != 4 {
		t.Fatalf("reopened view = %d rows, want 4", next.Tables[0].ExactSourceRowCount)
	}
	// A different view must not reuse the previous reference; the token is
	// derived from the pinned data version precisely so it moves when the
	// underlying data does.
	if next.Tables[0].SnapshotReference ==
		capture.Tables[0].SnapshotReference {
		t.Fatal("a changed view reused the prior snapshot reference")
	}
}

// TestSQLiteStrictRejectsParallelSourceReaders pins the "no parallel source
// readers" half of the contract. A second concurrent reader is refused, not
// queued, so a contract violation surfaces instead of silently serializing.
func TestSQLiteStrictRejectsParallelSourceReaders(t *testing.T) {
	source, _ := newSQLiteStrictSource(t, 2)
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := opener.OpenStrictConsistency(
		context.Background(),
		sqliteStrictRequest("items"),
	)
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*SQLiteStrictConsistencySession)
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	entered := make(chan struct{})
	release := make(chan struct{})
	var first error
	var waiting sync.WaitGroup
	waiting.Add(1)
	go func() {
		defer waiting.Done()
		first = session.RunReader(
			context.Background(),
			func(context.Context, *sql.Tx) error {
				close(entered)
				<-release
				return nil
			},
		)
	}()
	<-entered

	second := session.RunReader(
		context.Background(),
		func(context.Context, *sql.Tx) error {
			t.Error("second reader entered the stable view")
			return nil
		},
	)
	if second == nil ||
		!strings.Contains(second.Error(), "one source reader at a time") {
		t.Fatalf("parallel reader error = %v", second)
	}
	close(release)
	waiting.Wait()
	if first != nil {
		t.Fatalf("first reader = %v", first)
	}

	// The lend is released, so a later sequential reader is admitted.
	if err := session.RunReader(
		context.Background(),
		func(context.Context, *sql.Tx) error { return nil },
	); err != nil {
		t.Fatalf("sequential reader after release = %v", err)
	}
}

// TestSQLiteStrictRejectsUnsupportedRequests proves the fail-closed boundary,
// most importantly that migration scope is refused: SQLite cannot export its
// view, so promising a migration-wide snapshot would be a lie.
func TestSQLiteStrictRejectsUnsupportedRequests(t *testing.T) {
	source, _ := newSQLiteStrictSource(t, 1)
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	migrationSnapshot := &state.StrictMigrationSnapshot{}
	for name, test := range map[string]struct {
		mutate func(*StrictConsistencyOpenRequest)
		want   string
	}{
		"migration scope": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.Scope = state.StrictSnapshotMigration
			},
			want: "table scope only",
		},
		"foreign engine": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.SourceEngine = StrictConsistencyPostgres
			},
			want: "cannot serve source engine",
		},
		"reused migration snapshot": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.RequiredMigrationSnapshot = migrationSnapshot
			},
			want: "cannot reuse a durable migration snapshot",
		},
		"no tables": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.Tables = nil
			},
			want: "requires selected tables",
		},
		"blank run": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.RunID = ""
			},
			want: "run ID is required",
		},
		"partitioned task": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.Tables[0].Task.Partition = "p0"
			},
			want: "must be unpartitioned",
		},
		"duplicate task": {
			mutate: func(request *StrictConsistencyOpenRequest) {
				request.Tables = append(request.Tables, request.Tables[0])
			},
			want: "duplicated",
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := sqliteStrictRequest("items")
			test.mutate(&request)
			session, err := opener.OpenStrictConsistency(
				context.Background(),
				request,
			)
			if session != nil {
				_ = session.Close(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

// TestSQLiteStrictCloseIsIdempotentAndFinal proves cleanup is observable and
// repeatable, and that a closed session cannot hand out the view again.
func TestSQLiteStrictCloseIsIdempotentAndFinal(t *testing.T) {
	source, _ := newSQLiteStrictSource(t, 1)
	opener, err := NewSQLiteStrictConsistencyOpener(source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := opener.OpenStrictConsistency(
		context.Background(),
		sqliteStrictRequest("items"),
	)
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*SQLiteStrictConsistencySession)
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("second close = %v", err)
	}
	if _, err := session.CaptureSameViewEvidence(
		context.Background(),
	); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("capture after close = %v", err)
	}
	if err := session.RunReader(
		context.Background(),
		func(context.Context, *sql.Tx) error { return nil },
	); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("reader after close = %v", err)
	}
}
