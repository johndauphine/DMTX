package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteStage4NetworkWriterOrdersReservationProofWriteAndCommit(
	t *testing.T,
) {
	transaction := &sqliteStage4NetworkTestTransaction{}
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	table := stage4ReplayIsolationTable("", "parents")
	rows := [][]any{{int64(1), "updated"}}
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		table,
		[]string{"id", "code"},
		rows,
	)
	if err != nil {
		t.Fatalf("WriteStage4NetworkBatch: %v", err)
	}
	if got, want := transaction.operations, []string{
		"begin-immediate",
		"proof",
		"write",
		"commit",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	if transaction.proofTable.Name != "parents" {
		t.Fatalf("proof table = %#v", transaction.proofTable)
	}
	if !reflect.DeepEqual(transaction.rows, rows) {
		t.Fatalf(
			"written rows = %#v, want %#v",
			transaction.rows,
			rows,
		)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
}

func TestSQLiteStage4NetworkRebuildWriterReplayPreservesEarlierRows(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'original');
	`); err != nil {
		t.Fatalf("create rebuild replay fixture: %v", err)
	}
	writer := newSQLiteStage4NetworkWriter(database)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{{int64(1), "replayed"}, {int64(2), "new"}},
	)
	if err != nil {
		t.Fatalf("WriteStage4NetworkRebuildBatch: %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitDurable,
		2,
		2,
	)
	var existing, inserted string
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 1`,
	).Scan(&existing); err != nil {
		t.Fatalf("read replayed parent: %v", err)
	}
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 2`,
	).Scan(&inserted); err != nil {
		t.Fatalf("read inserted parent: %v", err)
	}
	if existing != "original" || inserted != "new" {
		t.Fatalf(
			"rebuild replay rows = existing=%q inserted=%q",
			existing,
			inserted,
		)
	}
}

func TestSQLiteStage4NetworkRebuildWriterFreshConflictFailsWithoutCommit(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'external');
	`); err != nil {
		t.Fatalf("create fresh conflict fixture: %v", err)
	}
	writer := newSQLiteStage4NetworkWriter(database)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "source"}, {int64(2), "must-roll-back"}},
	)
	if err == nil {
		t.Fatal("fresh rebuild conflict succeeded")
	}
	assertSQLiteStage4NetworkReceipt(t, receipt, CommitNotCommitted, 2, 0)
	var code string
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 1`,
	).Scan(&code); err != nil {
		t.Fatalf("read conflicting row: %v", err)
	}
	if code != "external" {
		t.Fatalf("fresh conflict altered target row = %q", code)
	}
	var rows int
	if err := database.QueryRow(`SELECT COUNT(*) FROM "parents"`).Scan(&rows); err != nil {
		t.Fatalf("count fresh conflict rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("fresh conflict reported partial commit: %d rows", rows)
	}
}

func TestSQLiteStage4NetworkRebuildWriterFreshInsertCompletes(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("create fresh insert fixture: %v", err)
	}
	writer := newSQLiteStage4NetworkWriter(database)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "one"}, {int64(2), "two"}},
	)
	if err != nil {
		t.Fatalf("fresh rebuild insert: %v", err)
	}
	assertSQLiteStage4NetworkReceipt(t, receipt, CommitDurable, 2, 2)
	var rows int
	if err := database.QueryRow(`SELECT COUNT(*) FROM "parents"`).Scan(&rows); err != nil {
		t.Fatalf("count fresh rows: %v", err)
	}
	if rows != 2 {
		t.Fatalf("fresh rebuild rows = %d", rows)
	}
}

func TestSQLiteStage4NetworkWriterRollsBackWithoutWriteOnReservationOrProofFailure(
	t *testing.T,
) {
	beginFailure := errors.New("begin immediate failed")
	proofFailure := errors.New("proof failed")
	tests := []struct {
		name      string
		beginErr  error
		proofErr  error
		wantOps   []string
		wantCause error
	}{
		{
			name:      "reservation",
			beginErr:  beginFailure,
			wantOps:   []string{"begin-immediate"},
			wantCause: beginFailure,
		},
		{
			name:     "proof",
			proofErr: proofFailure,
			wantOps: []string{
				"begin-immediate",
				"proof",
				"rollback",
			},
			wantCause: proofFailure,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			transaction := &sqliteStage4NetworkTestTransaction{
				beginErr: testCase.beginErr,
				proofErr: testCase.proofErr,
			}
			writer := &sqliteStage4NetworkWriter{
				connections: sqliteStage4NetworkTestProvider{
					transaction: transaction,
				},
			}
			receipt, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				stage4ReplayIsolationTable("", "parents"),
				[]string{"id", "code"},
				[][]any{{int64(1), "updated"}},
			)
			if !errors.Is(err, testCase.wantCause) {
				t.Fatalf("error = %v", err)
			}
			assertSQLiteStage4NetworkReceipt(
				t,
				receipt,
				CommitNotCommitted,
				1,
				0,
			)
			if !reflect.DeepEqual(
				transaction.operations,
				testCase.wantOps,
			) {
				t.Fatalf(
					"operations = %#v, want %#v",
					transaction.operations,
					testCase.wantOps,
				)
			}
			if transaction.writeCalls != 0 {
				t.Fatalf(
					"write calls after %s failure = %d",
					testCase.name,
					transaction.writeCalls,
				)
			}
		})
	}
}

func TestSQLiteStage4NetworkWriterPreservesFailureAndRollbackError(
	t *testing.T,
) {
	proofFailure := errors.New("proof failed")
	rollbackFailure := errors.New("rollback failed")
	transaction := &sqliteStage4NetworkTestTransaction{
		proofErr:    proofFailure,
		rollbackErr: rollbackFailure,
	}
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		stage4ReplayIsolationTable("", "parents"),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, proofFailure) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("combined error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if !transaction.discarded {
		t.Fatal("unclean SQLite Stage 4 transaction was not discarded")
	}
}

func TestSQLiteStage4NetworkWriterCancellationUsesDetachedBoundedRollback(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	rollbackFailure := errors.New("rollback failed")
	transaction := &sqliteStage4NetworkTestTransaction{
		proofErr:    context.Canceled,
		rollbackErr: rollbackFailure,
		cancelProof: cancel,
	}
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	receipt, err := writer.WriteStage4NetworkBatch(
		ctx,
		stage4ReplayIsolationTable("", "parents"),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, context.Canceled) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("canceled SQLite write error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if transaction.rollbackCtxErr != nil {
		t.Fatalf(
			"SQLite rollback inherited caller cancellation: %v",
			transaction.rollbackCtxErr,
		)
	}
	if !transaction.discarded {
		t.Fatal("failed SQLite cancellation rollback was not discarded")
	}
	if got, want := transaction.operations, []string{
		"begin-immediate",
		"proof",
		"rollback",
		"discard",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cancellation operations = %#v, want %#v", got, want)
	}
}

func TestSQLiteStage4NetworkWriterDiscardsImpossibleNilTransaction(
	t *testing.T,
) {
	transaction := &sqliteStage4NetworkTestTransaction{
		nilTransaction: true,
	}
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		stage4ReplayIsolationTable("", "parents"),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"transaction is not configured",
	) {
		t.Fatalf("nil transaction error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if !transaction.discarded {
		t.Fatal("SQLite nil transaction connection was not discarded")
	}
	if got, want := transaction.operations, []string{
		"begin-immediate",
		"discard",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nil transaction operations = %#v, want %#v", got, want)
	}
}

func TestSQLiteStage4NetworkWriterCommitFailureIsUnknownAndRollsBack(
	t *testing.T,
) {
	commitFailure := errors.New("commit failed")
	transaction := &sqliteStage4NetworkTestTransaction{
		commitErr: commitFailure,
	}
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		stage4ReplayIsolationTable("", "parents"),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("commit error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitUnknown,
		1,
		0,
	)
	if got := transaction.operations[len(transaction.operations)-2:]; !reflect.DeepEqual(
		got,
		[]string{"commit", "rollback"},
	) {
		t.Fatalf("terminal operations = %#v", got)
	}
}

func TestSQLiteStage4NetworkWriterRejectsUnsafeIncomingForeignKeyInReservedTransaction(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL UNIQUE
		);
		CREATE TABLE "external_children" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"parent_code" TEXT,
			FOREIGN KEY ("parent_code")
				REFERENCES "parents" ("code")
				ON UPDATE CASCADE
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'old');
		INSERT INTO "external_children" ("id", "parent_code")
			VALUES (1, 'old');
	`); err != nil {
		t.Fatalf("create unsafe fixture: %v", err)
	}
	writer := newSQLiteStage4NetworkWriter(database)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		[][]any{{int64(1), "new"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"ON UPDATE CASCADE on mutable column code",
	) {
		t.Fatalf("unsafe foreign-key error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	var parentCode, childCode string
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 1`,
	).Scan(&parentCode); err != nil {
		t.Fatalf("read parent after rejected write: %v", err)
	}
	if err := database.QueryRow(
		`SELECT "parent_code" FROM "external_children" WHERE "id" = 1`,
	).Scan(&childCode); err != nil {
		t.Fatalf("read child after rejected write: %v", err)
	}
	if parentCode != "old" || childCode != "old" {
		t.Fatalf(
			"rejected write changed rows: parent=%q child=%q",
			parentCode,
			childCode,
		)
	}
}

func TestSQLiteStage4NetworkWriterAllowsPrimaryKeyCascadeAndCommitsUpsert(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
		CREATE TABLE "external_children" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"parent_id" INTEGER,
			FOREIGN KEY ("parent_id")
				REFERENCES "parents" ("id")
				ON UPDATE CASCADE
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'old');
		INSERT INTO "external_children" ("id", "parent_id")
			VALUES (1, 1);
	`); err != nil {
		t.Fatalf("create legal fixture: %v", err)
	}
	writer := newSQLiteStage4NetworkWriter(database)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		[][]any{{int64(1), "new"}},
	)
	if err != nil {
		t.Fatalf("WriteStage4NetworkBatch: %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	var code string
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 1`,
	).Scan(&code); err != nil {
		t.Fatalf("read committed parent: %v", err)
	}
	if code != "new" {
		t.Fatalf("committed parent code = %q, want new", code)
	}
}

func TestSQLiteStage4NetworkWriterRejectsTriggerAddedBetweenPagesBeforeDML(
	t *testing.T,
) {
	database := openSQLiteStage4NetworkTestDatabase(t)
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
		CREATE TABLE "parent_update_audit" (
			"parent_id" INTEGER NOT NULL
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'old');
	`); err != nil {
		t.Fatalf("create SQLite between-page trigger fixture: %v", err)
	}
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	writer := newSQLiteStage4NetworkWriter(database)
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		[][]any{{int64(1), "first-page"}},
	)
	if err != nil {
		t.Fatalf("write SQLite first page: %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	if _, err := database.Exec(`
		CREATE TRIGGER "parents_update_audit"
		AFTER UPDATE ON "parents"
		BEGIN
			INSERT INTO "parent_update_audit" ("parent_id")
			VALUES (NEW."id");
		END;
	`); err != nil {
		t.Fatalf("create SQLite between-page replay trigger: %v", err)
	}
	receipt, err = writer.WriteStage4NetworkBatch(
		context.Background(),
		planned,
		[]string{"id", "code"},
		[][]any{{int64(1), "second-page"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"table trigger",
	) {
		t.Fatalf("SQLite between-page trigger replay error = %v", err)
	}
	assertSQLiteStage4NetworkReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	var code string
	if err := database.QueryRow(
		`SELECT "code" FROM "parents" WHERE "id" = 1`,
	).Scan(&code); err != nil {
		t.Fatalf("read SQLite parent after trigger rejection: %v", err)
	}
	var auditRows int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM "parent_update_audit"`,
	).Scan(&auditRows); err != nil {
		t.Fatalf("read SQLite replay-trigger audit: %v", err)
	}
	if code != "first-page" || auditRows != 0 {
		t.Fatalf(
			"rejected SQLite replay changed target: parent=%q audit=%d",
			code,
			auditRows,
		)
	}
}

func TestSQLiteStage4NetworkWriterReservationBlocksConcurrentDDL(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "target.db")
	database, err := openSQLiteTargetDatabase(
		context.Background(),
		path,
	)
	if err != nil {
		t.Fatalf("open SQLite writer target: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})
	if _, err := database.Exec(`
		CREATE TABLE "parents" (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"code" TEXT NOT NULL
		);
		INSERT INTO "parents" ("id", "code") VALUES (1, 'old');
	`); err != nil {
		t.Fatalf("create DDL fence fixture: %v", err)
	}
	ddlDatabase, err := sql.Open("sqlite", sqliteTargetURI(path))
	if err != nil {
		t.Fatalf("open concurrent SQLite target: %v", err)
	}
	t.Cleanup(func() {
		_ = ddlDatabase.Close()
	})
	ddlDatabase.SetMaxOpenConns(1)
	if _, err := ddlDatabase.Exec("PRAGMA busy_timeout = 25"); err != nil {
		t.Fatalf("configure concurrent SQLite timeout: %v", err)
	}

	proofComplete := make(chan struct{})
	releaseWrite := make(chan struct{})
	writer := &sqliteStage4NetworkWriter{
		connections: sqliteStage4PausingSQLProvider{
			database:      database,
			proofComplete: proofComplete,
			releaseWrite:  releaseWrite,
		},
	}
	type writeResult struct {
		receipt WriteReceipt
		err     error
	}
	resultChannel := make(chan writeResult, 1)
	planned := sqliteStage4NetworkPlannedTable(t, database, "parents")
	go func() {
		receipt, writeErr := writer.WriteStage4NetworkBatch(
			context.Background(),
			planned,
			[]string{"id", "code"},
			[][]any{{int64(1), "new"}},
		)
		resultChannel <- writeResult{
			receipt: receipt,
			err:     writeErr,
		}
	}()
	select {
	case <-proofComplete:
	case <-time.After(5 * time.Second):
		close(releaseWrite)
		t.Fatal("timed out waiting for reserved replay proof")
	}

	ddlContext, cancelDDL := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)
	_, ddlErr := ddlDatabase.ExecContext(
		ddlContext,
		`ALTER TABLE "parents" ADD COLUMN "injected" TEXT`,
	)
	cancelDDL()
	if ddlErr == nil {
		close(releaseWrite)
		t.Fatal("concurrent SQLite DDL crossed the immediate writer reservation")
	}
	close(releaseWrite)
	select {
	case result := <-resultChannel:
		if result.err != nil {
			t.Fatalf("reserved writer failed: %v", result.err)
		}
		assertSQLiteStage4NetworkReceipt(
			t,
			result.receipt,
			CommitDurable,
			1,
			1,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reserved writer")
	}
	var injected int
	if err := database.QueryRow(
		`SELECT COUNT(*)
		   FROM pragma_table_info('parents')
		  WHERE name = 'injected'`,
	).Scan(&injected); err != nil {
		t.Fatalf("inspect target after concurrent DDL: %v", err)
	}
	if injected != 0 {
		t.Fatal("concurrent DDL changed the target schema")
	}
}

func sqliteStage4NetworkPlannedTable(
	t *testing.T,
	database *sql.DB,
	name string,
) schema.Table {
	t.Helper()
	table, _, err := inspectSQLiteSchema(
		context.Background(),
		database,
		name,
	)
	if err != nil {
		t.Fatalf(
			"inspect SQLite Stage 4 planned table %s: %v",
			name,
			err,
		)
	}
	return table
}

func assertSQLiteStage4NetworkReceipt(
	t *testing.T,
	receipt WriteReceipt,
	certainty CommitCertainty,
	attempted int64,
	committed int64,
) {
	t.Helper()
	if receipt.Certainty != certainty ||
		receipt.AttemptedRows != attempted ||
		receipt.CommittedRows != committed {
		t.Fatalf(
			"receipt = %#v, want certainty=%s attempted=%d committed=%d",
			receipt,
			certainty,
			attempted,
			committed,
		)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("receipt validation: %v", err)
	}
}

func openSQLiteStage4NetworkTestDatabase(
	t *testing.T,
) *sql.DB {
	t.Helper()
	database, err := openSQLiteTargetDatabase(
		context.Background(),
		filepath.Join(t.TempDir(), "target.db"),
	)
	if err != nil {
		t.Fatalf("open SQLite target: %v", err)
	}
	t.Cleanup(func() {
		_ = database.Close()
	})
	return database
}

type sqliteStage4NetworkTestProvider struct {
	transaction *sqliteStage4NetworkTestTransaction
}

func (provider sqliteStage4NetworkTestProvider) WithConnection(
	ctx context.Context,
	operation func(sqliteStage4NetworkConnection) error,
) error {
	return operation(sqliteStage4NetworkTestConnection{
		transaction: provider.transaction,
	})
}

type sqliteStage4NetworkTestConnection struct {
	transaction *sqliteStage4NetworkTestTransaction
}

func (connection sqliteStage4NetworkTestConnection) BeginImmediate(
	context.Context,
) (sqliteStage4NetworkTransaction, error) {
	connection.transaction.operations = append(
		connection.transaction.operations,
		"begin-immediate",
	)
	if connection.transaction.beginErr != nil {
		return nil, connection.transaction.beginErr
	}
	if connection.transaction.nilTransaction {
		return nil, nil
	}
	return connection.transaction, nil
}

func (connection sqliteStage4NetworkTestConnection) Discard() {
	connection.transaction.operations = append(
		connection.transaction.operations,
		"discard",
	)
	connection.transaction.discarded = true
}

type sqliteStage4NetworkTestTransaction struct {
	operations     []string
	proofTable     schema.Table
	rows           [][]any
	beginErr       error
	proofErr       error
	writeErr       error
	commitErr      error
	rollbackErr    error
	rollbackCtxErr error
	cancelProof    context.CancelFunc
	nilTransaction bool
	discarded      bool
	writeCalls     int
}

func (transaction *sqliteStage4NetworkTestTransaction) ValidateStage4NetworkReplayIsolation(
	_ context.Context,
	table schema.Table,
) error {
	transaction.operations = append(
		transaction.operations,
		"proof",
	)
	transaction.proofTable = table
	if transaction.cancelProof != nil {
		transaction.cancelProof()
	}
	return transaction.proofErr
}

func (transaction *sqliteStage4NetworkTestTransaction) WriteUpsert(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) error {
	transaction.operations = append(
		transaction.operations,
		"write",
	)
	transaction.writeCalls++
	transaction.rows = clonePostgresNativeTestRows(rows)
	return transaction.writeErr
}

func (transaction *sqliteStage4NetworkTestTransaction) WriteDuplicateSafeInsertOnly(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) error {
	transaction.operations = append(transaction.operations, "rebuild-write")
	transaction.writeCalls++
	transaction.rows = clonePostgresNativeTestRows(rows)
	return transaction.writeErr
}

func (transaction *sqliteStage4NetworkTestTransaction) WriteFreshInsert(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) error {
	transaction.operations = append(transaction.operations, "rebuild-fresh-write")
	transaction.writeCalls++
	transaction.rows = clonePostgresNativeTestRows(rows)
	return transaction.writeErr
}

func (transaction *sqliteStage4NetworkTestTransaction) Commit(
	context.Context,
) error {
	transaction.operations = append(
		transaction.operations,
		"commit",
	)
	return transaction.commitErr
}

func (transaction *sqliteStage4NetworkTestTransaction) Rollback(
	ctx context.Context,
) error {
	transaction.operations = append(
		transaction.operations,
		"rollback",
	)
	transaction.rollbackCtxErr = ctx.Err()
	return transaction.rollbackErr
}

type sqliteStage4PausingSQLProvider struct {
	database      *sql.DB
	proofComplete chan struct{}
	releaseWrite  chan struct{}
}

func (provider sqliteStage4PausingSQLProvider) WithConnection(
	ctx context.Context,
	operation func(sqliteStage4NetworkConnection) error,
) error {
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	return operation(sqliteStage4PausingSQLConnection{
		delegate: sqliteStage4SQLConnection{
			connection: connection,
		},
		proofComplete: provider.proofComplete,
		releaseWrite:  provider.releaseWrite,
	})
}

type sqliteStage4PausingSQLConnection struct {
	delegate      sqliteStage4SQLConnection
	proofComplete chan struct{}
	releaseWrite  chan struct{}
}

func (connection sqliteStage4PausingSQLConnection) BeginImmediate(
	ctx context.Context,
) (sqliteStage4NetworkTransaction, error) {
	transaction, err := connection.delegate.BeginImmediate(ctx)
	if err != nil {
		return nil, err
	}
	return sqliteStage4PausingSQLTransaction{
		delegate:      transaction,
		proofComplete: connection.proofComplete,
		releaseWrite:  connection.releaseWrite,
	}, nil
}

func (connection sqliteStage4PausingSQLConnection) Discard() {
	connection.delegate.Discard()
}

type sqliteStage4PausingSQLTransaction struct {
	delegate      sqliteStage4NetworkTransaction
	proofComplete chan struct{}
	releaseWrite  chan struct{}
}

func (transaction sqliteStage4PausingSQLTransaction) ValidateStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
) error {
	if err := transaction.delegate.
		ValidateStage4NetworkReplayIsolation(
			ctx,
			table,
		); err != nil {
		return err
	}
	close(transaction.proofComplete)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-transaction.releaseWrite:
		return nil
	}
}

func (transaction sqliteStage4PausingSQLTransaction) WriteUpsert(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	return transaction.delegate.WriteUpsert(
		ctx,
		table,
		columns,
		rows,
	)
}

func (transaction sqliteStage4PausingSQLTransaction) Commit(
	ctx context.Context,
) error {
	return transaction.delegate.Commit(ctx)
}

func (transaction sqliteStage4PausingSQLTransaction) Rollback(
	ctx context.Context,
) error {
	return transaction.delegate.Rollback(ctx)
}
