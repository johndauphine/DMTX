package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresStage4NetworkWriterFencesProofAndWriteInOneTransaction(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	transaction.foreignKeys = []postgresStage4NetworkTestForeignKey{
		{
			parentNamespace:     "external",
			parentTable:         "children",
			name:                "children_parent_id_fkey",
			referencedNamespace: "public",
			referencedTable:     "parents",
			actionCode:          "c",
			referencedColumn:    "id",
		},
	}
	table := postgresStage4NetworkWriterTestTable()
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
	assertPostgresReceipt(
		t,
		receipt,
		CommitDurable,
		1,
		1,
	)
	if got, want := transaction.operations, []string{
		"begin",
		"fence",
		"proof",
		"write-exec",
		"write-copy",
		"write-exec",
		"commit",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	if got, want := transaction.statements[0],
		`LOCK TABLE "public"."parents" IN SHARE UPDATE EXCLUSIVE MODE`; got != want {
		t.Fatalf("fence statement = %q, want %q", got, want)
	}
	if transaction.proofNamespace != "public" ||
		transaction.proofTable != "parents" {
		t.Fatalf(
			"proof target = %q.%q",
			transaction.proofNamespace,
			transaction.proofTable,
		)
	}
}

func TestPostgresStage4NetworkRebuildWriterSeparatesFreshAndReplay(
	t *testing.T,
) {
	table := postgresStage4NetworkWriterTestTable()
	columns := []string{"id", "code"}
	rows := [][]any{{int64(1), "original"}, {int64(2), "new"}}

	t.Run("fresh is strict target COPY", func(t *testing.T) {
		writer, transaction := newPostgresStage4NetworkTestWriter()
		receipt, err := writer.WriteStage4NetworkRebuildBatch(
			context.Background(),
			table,
			columns,
			NetworkWriteFreshInsert,
			rows,
		)
		if err != nil {
			t.Fatalf("fresh rebuild write: %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
		if got, want := transaction.operations, []string{
			"begin",
			"fence",
			"proof",
			"write-copy",
			"commit",
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("fresh operations = %#v, want %#v", got, want)
		}
		if got, want := transaction.copyTargets,
			[][]string{{"public", "parents"}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("fresh COPY targets = %#v, want %#v", got, want)
		}
	})

	t.Run("issued replay is insert only", func(t *testing.T) {
		writer, transaction := newPostgresStage4NetworkTestWriter()
		receipt, err := writer.WriteStage4NetworkRebuildBatch(
			context.Background(),
			table,
			columns,
			NetworkWriteDuplicateSafeInsertOnly,
			rows,
		)
		if err != nil {
			t.Fatalf("replay rebuild write: %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
		if got, want := transaction.operations, []string{
			"begin",
			"fence",
			"proof",
			"write-exec",
			"write-copy",
			"write-exec",
			"commit",
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("replay operations = %#v, want %#v", got, want)
		}
		if got, want := transaction.copyTargets,
			[][]string{{"pg_temp", postgresStageTableName(table, columns)}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("replay COPY targets = %#v, want %#v", got, want)
		}
		statement := transaction.statements[len(transaction.statements)-1]
		if want := postgresInsertOnlyStageStatement(
			table,
			columns,
			postgresStageTableName(table, columns),
		); statement != want {
			t.Fatalf("replay statement = %q, want %q", statement, want)
		}
		if strings.Contains(statement, "DO UPDATE") {
			t.Fatalf("replay statement can update a row: %q", statement)
		}
	})
}

func TestPostgresStage4NetworkRebuildWriterFreshConflictDoesNotCommit(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	conflict := errors.New("duplicate key with sensitive row data")
	transaction.copyErr = conflict
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "source"}},
	)
	if !errors.Is(err, conflict) || strings.Contains(
		err.Error(),
		"sensitive row data",
	) {
		t.Fatalf("fresh conflict error = %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if got, want := transaction.operations, []string{
		"begin",
		"fence",
		"proof",
		"write-copy",
		"rollback",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fresh conflict operations = %#v, want %#v", got, want)
	}
}

func TestPostgresStage4NetworkRebuildWriterRejectsKeylessBeforeConnection(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	table := postgresStage4NetworkWriterTestTable()
	table.Columns[0].PrimaryKey = false
	table.Columns[0].PrimaryKeyPosition = 0
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		context.Background(),
		table,
		[]string{"id", "code"},
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "source"}},
	)
	if err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("keyless rebuild error = %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if len(transaction.operations) != 0 {
		t.Fatalf("keyless rebuild acquired a connection: %#v", transaction.operations)
	}
}

func TestPostgresStage4NetworkWriterRollsBackBeforeAnyWriteOnFenceOrProofFailure(
	t *testing.T,
) {
	fenceFailure := errors.New("fence failed with sensitive driver detail")
	proofFailure := errors.New("proof query failed with sensitive driver detail")
	tests := []struct {
		name       string
		configure  func(*postgresStage4NetworkTestTransaction)
		wantOps    []string
		wantCause  error
		wantPublic string
	}{
		{
			name: "fence",
			configure: func(
				transaction *postgresStage4NetworkTestTransaction,
			) {
				transaction.fenceErr = fenceFailure
			},
			wantOps: []string{
				"begin",
				"fence",
				"rollback",
			},
			wantCause:  fenceFailure,
			wantPublic: "acquire PostgreSQL Stage 4 network replay DDL fence for parents",
		},
		{
			name: "proof query",
			configure: func(
				transaction *postgresStage4NetworkTestTransaction,
			) {
				transaction.proofErr = proofFailure
			},
			wantOps: []string{
				"begin",
				"fence",
				"proof",
				"rollback",
			},
			wantCause:  proofFailure,
			wantPublic: "prove PostgreSQL Stage 4 network replay isolation for parents",
		},
		{
			name: "missing catalog iterator",
			configure: func(
				transaction *postgresStage4NetworkTestTransaction,
			) {
				transaction.nilProofRows = true
			},
			wantOps: []string{
				"begin",
				"fence",
				"proof",
				"rollback",
			},
			wantPublic: "returned no row iterator",
		},
		{
			name: "unsafe incoming foreign key",
			configure: func(
				transaction *postgresStage4NetworkTestTransaction,
			) {
				transaction.foreignKeys = []postgresStage4NetworkTestForeignKey{
					{
						parentNamespace:     "external",
						parentTable:         "children",
						name:                "children_parent_code_fkey",
						referencedNamespace: "public",
						referencedTable:     "parents",
						actionCode:          "c",
						referencedColumn:    "code",
					},
				}
			},
			wantOps: []string{
				"begin",
				"fence",
				"proof",
				"rollback",
			},
			wantPublic: "ON UPDATE CASCADE on mutable column code",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			writer, transaction := newPostgresStage4NetworkTestWriter()
			testCase.configure(transaction)
			receipt, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				postgresStage4NetworkWriterTestTable(),
				[]string{"id", "code"},
				[][]any{{int64(1), "updated"}},
			)
			if err == nil || !strings.Contains(
				err.Error(),
				testCase.wantPublic,
			) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), "sensitive driver detail") {
				t.Fatalf("error leaked driver detail: %v", err)
			}
			if testCase.wantCause != nil &&
				!errors.Is(err, testCase.wantCause) {
				t.Fatalf(
					"error does not preserve cause %v: %v",
					testCase.wantCause,
					err,
				)
			}
			assertPostgresReceipt(
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
			if transaction.copyCalls != 0 {
				t.Fatalf(
					"copy calls after isolation failure = %d",
					transaction.copyCalls,
				)
			}
		})
	}
}

func TestPostgresStage4NetworkWriterReprovesReplayHazardsUnderFence(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*postgresCatalogTableShape)
		want   string
	}{
		{
			name: "trigger",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.userTriggers = 1
			},
			want: "non-internal user triggers",
		},
		{
			name: "rewrite rule",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.userRules = 1
			},
			want: "user rewrite rules",
		},
		{
			name: "row security",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.rowSecurity = true
			},
			want: "row-level security",
		},
		{
			name: "primary key drift",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.primaryKey = []string{"code"}
			},
			want: "primary key is (code)",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			writer, transaction := newPostgresStage4NetworkTestWriter()
			shape := postgresStage4NetworkValidShape()
			testCase.mutate(&shape)
			transaction.shapeOverride = &shape
			receipt, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				postgresStage4NetworkWriterTestTable(),
				[]string{"id", "code"},
				[][]any{{int64(1), "must-not-apply"}},
			)
			if err == nil || !strings.Contains(
				err.Error(),
				testCase.want,
			) {
				t.Fatalf("locked replay-hazard error = %v", err)
			}
			assertPostgresReceipt(
				t,
				receipt,
				CommitNotCommitted,
				1,
				0,
			)
			if transaction.copyCalls != 0 {
				t.Fatalf(
					"locked replay-hazard rejection made %d writes",
					transaction.copyCalls,
				)
			}
		})
	}
}

func TestPostgresStage4NetworkWriterAmbiguousCommitFailureIsUnknownAndDiscards(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	commitFailure := errors.New("commit failed")
	transaction.commitErr = commitFailure
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("commit error = %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitUnknown,
		1,
		0,
	)
	if got := transaction.operations[len(transaction.operations)-3:]; !reflect.DeepEqual(
		got,
		[]string{"commit", "discard", "rollback"},
	) {
		t.Fatalf("terminal operations = %#v", got)
	}
	if !transaction.discarded {
		t.Fatal("ambiguous PostgreSQL commit returned its connection to the pool")
	}
}

func TestPostgresStage4NetworkWriterCommitRollbackIsNotCommittedAndReusable(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	transaction.commitErr = pgx.ErrTxCommitRollback
	transaction.rollbackErr = pgx.ErrTxClosed
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("commit rollback error = %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if transaction.discarded {
		t.Fatal("proven PostgreSQL commit rollback discarded a clean connection")
	}
	if got := transaction.operations[len(transaction.operations)-2:]; !reflect.DeepEqual(
		got,
		[]string{"commit", "rollback"},
	) {
		t.Fatalf("terminal operations = %#v", got)
	}
}

func TestPostgresStage4NetworkWriterPreservesProofAndRollbackFailures(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	proofFailure := errors.New("proof failed with sensitive driver detail")
	rollbackFailure := errors.New("rollback failed with sensitive driver detail")
	transaction.proofErr = proofFailure
	transaction.rollbackErr = rollbackFailure
	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, proofFailure) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("combined failure = %v", err)
	}
	if strings.Contains(err.Error(), "sensitive driver detail") {
		t.Fatalf("combined failure leaked driver detail: %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if !transaction.discarded {
		t.Fatal("unclean PostgreSQL Stage 4 transaction was not discarded")
	}
}

func TestPostgresStage4NetworkWriterCancellationUsesDetachedBoundedRollback(
	t *testing.T,
) {
	writer, transaction := newPostgresStage4NetworkTestWriter()
	ctx, cancel := context.WithCancel(context.Background())
	transaction.cancelProof = cancel
	transaction.proofErr = context.Canceled
	rollbackFailure := errors.New("rollback failed")
	transaction.rollbackErr = rollbackFailure
	receipt, err := writer.WriteStage4NetworkBatch(
		ctx,
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if !errors.Is(err, context.Canceled) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("canceled write error = %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
	if transaction.rollbackCtxErr != nil {
		t.Fatalf(
			"PostgreSQL rollback inherited caller cancellation: %v",
			transaction.rollbackCtxErr,
		)
	}
	if !transaction.discarded {
		t.Fatal("failed PostgreSQL cancellation rollback was not discarded")
	}
}

func TestPostgresTargetStage4NetworkWriterRequiresCertifiedDelegate(
	t *testing.T,
) {
	recorder := &postgresStage4NetworkTargetWriterRecorder{}
	adapter := &postgresTargetAdapter{batchWriter: recorder}
	receipt, err := adapter.WriteStage4NetworkBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if err != nil {
		t.Fatalf("WriteStage4NetworkBatch: %v", err)
	}
	if recorder.stage4Calls != 1 {
		t.Fatalf("Stage 4 delegate calls = %d", recorder.stage4Calls)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 1, 1)

	receipt, err = adapter.WriteStage4NetworkRebuildBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{{int64(1), "unchanged"}},
	)
	if err != nil {
		t.Fatalf("WriteStage4NetworkRebuildBatch: %v", err)
	}
	if recorder.rebuildCalls != 1 ||
		recorder.rebuildMode != NetworkWriteDuplicateSafeInsertOnly {
		t.Fatalf(
			"Stage 4 rebuild delegate = calls=%d mode=%q",
			recorder.rebuildCalls,
			recorder.rebuildMode,
		)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 1, 1)

	adapter.batchWriter = &postgresTargetWriterRecorder{}
	receipt, err = adapter.WriteStage4NetworkBatch(
		context.Background(),
		postgresStage4NetworkWriterTestTable(),
		[]string{"id", "code"},
		[][]any{{int64(1), "updated"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"Stage 4 network batch writer is not configured",
	) {
		t.Fatalf("uncertified writer error = %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
}

func newPostgresStage4NetworkTestWriter() (
	*postgresNativeWriter,
	*postgresStage4NetworkTestTransaction,
) {
	transaction := &postgresStage4NetworkTestTransaction{}
	writer := &postgresNativeWriter{
		connections: postgresStage4NetworkTestProvider{
			transaction: transaction,
		},
	}
	return writer, transaction
}

func postgresStage4NetworkWriterTestTable() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "parents",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name: "code",
				Type: "text",
			},
		},
	}
}

type postgresStage4NetworkTestProvider struct {
	transaction *postgresStage4NetworkTestTransaction
}

func (provider postgresStage4NetworkTestProvider) WithConnection(
	ctx context.Context,
	operation func(postgresNativeConnection) error,
) error {
	return operation(postgresStage4NetworkTestConnection{
		transaction: provider.transaction,
	})
}

type postgresStage4NetworkTestConnection struct {
	transaction *postgresStage4NetworkTestTransaction
}

func (connection postgresStage4NetworkTestConnection) Begin(
	context.Context,
) (postgresNativeTransaction, error) {
	connection.transaction.operations = append(
		connection.transaction.operations,
		"begin",
	)
	return connection.transaction, nil
}

func (connection postgresStage4NetworkTestConnection) Discard() {
	connection.transaction.operations = append(
		connection.transaction.operations,
		"discard",
	)
	connection.transaction.discarded = true
}

type postgresStage4NetworkTestForeignKey struct {
	parentNamespace     string
	parentTable         string
	name                string
	referencedNamespace string
	referencedTable     string
	actionCode          string
	referencedColumn    string
}

type postgresStage4NetworkTestTransaction struct {
	operations     []string
	statements     []string
	foreignKeys    []postgresStage4NetworkTestForeignKey
	proofNamespace string
	proofTable     string
	fenceErr       error
	proofErr       error
	shapeErr       error
	shapeOverride  *postgresCatalogTableShape
	missingShape   bool
	nilProofRows   bool
	commitErr      error
	rollbackErr    error
	rollbackCtxErr error
	cancelProof    context.CancelFunc
	discarded      bool
	copyCalls      int
	copyTargets    [][]string
	copyErr        error
}

func (transaction *postgresStage4NetworkTestTransaction) ReadStage4PostgresRetainedShape(
	_ context.Context,
	_ schema.Table,
) (postgresCatalogTableShape, bool, error) {
	if transaction.shapeErr != nil {
		return postgresCatalogTableShape{}, false, transaction.shapeErr
	}
	if transaction.missingShape {
		return postgresCatalogTableShape{}, false, nil
	}
	if transaction.shapeOverride != nil {
		return *transaction.shapeOverride, true, nil
	}
	return postgresStage4NetworkValidShape(), true, nil
}

func postgresStage4NetworkValidShape() postgresCatalogTableShape {
	return postgresCatalogTableShape{
		relationKind: "r",
		persistence:  "p",
		columns: []postgresCatalogColumnShape{
			{
				name: "id",
				columnType: postgresCatalogTypeShape{
					name: "int4",
				},
				notNull:          true,
				defaultCollation: true,
			},
			{
				name: "code",
				columnType: postgresCatalogTypeShape{
					name: "text",
				},
				notNull:          true,
				defaultCollation: true,
			},
		},
		primaryKey: []string{"id"},
	}
}

func (transaction *postgresStage4NetworkTestTransaction) CopyRows(
	_ context.Context,
	table []string,
	_ []string,
	rows [][]any,
) (int64, error) {
	transaction.operations = append(
		transaction.operations,
		"write-copy",
	)
	transaction.copyCalls++
	transaction.copyTargets = append(
		transaction.copyTargets,
		append([]string(nil), table...),
	)
	return int64(len(rows)), transaction.copyErr
}

func (transaction *postgresStage4NetworkTestTransaction) Exec(
	_ context.Context,
	statement string,
) (int64, error) {
	transaction.statements = append(
		transaction.statements,
		statement,
	)
	if strings.HasPrefix(statement, "LOCK TABLE ") {
		transaction.operations = append(
			transaction.operations,
			"fence",
		)
		return 0, transaction.fenceErr
	}
	transaction.operations = append(
		transaction.operations,
		"write-exec",
	)
	if strings.HasPrefix(statement, "INSERT INTO ") {
		return 1, nil
	}
	return 0, nil
}

func (transaction *postgresStage4NetworkTestTransaction) QueryStage4PostgresIncomingForeignKeys(
	_ context.Context,
	namespace string,
	table string,
) (stage4PostgresReplayCatalogRows, error) {
	transaction.operations = append(
		transaction.operations,
		"proof",
	)
	transaction.proofNamespace = namespace
	transaction.proofTable = table
	if transaction.cancelProof != nil {
		transaction.cancelProof()
	}
	if transaction.proofErr != nil {
		return nil, transaction.proofErr
	}
	if transaction.nilProofRows {
		return nil, nil
	}
	return &postgresStage4NetworkTestRows{
		foreignKeys: transaction.foreignKeys,
	}, nil
}

func (transaction *postgresStage4NetworkTestTransaction) Commit(
	context.Context,
) error {
	transaction.operations = append(
		transaction.operations,
		"commit",
	)
	return transaction.commitErr
}

func (transaction *postgresStage4NetworkTestTransaction) Rollback(
	ctx context.Context,
) error {
	transaction.operations = append(
		transaction.operations,
		"rollback",
	)
	transaction.rollbackCtxErr = ctx.Err()
	return transaction.rollbackErr
}

type postgresStage4NetworkTestRows struct {
	foreignKeys []postgresStage4NetworkTestForeignKey
	index       int
	current     *postgresStage4NetworkTestForeignKey
}

func (rows *postgresStage4NetworkTestRows) Next() bool {
	if rows.index >= len(rows.foreignKeys) {
		rows.current = nil
		return false
	}
	rows.current = &rows.foreignKeys[rows.index]
	rows.index++
	return true
}

func (rows *postgresStage4NetworkTestRows) Scan(
	destinations ...any,
) error {
	if rows.current == nil || len(destinations) != 7 {
		return fmt.Errorf(
			"unexpected PostgreSQL test catalog scan shape",
		)
	}
	values := []string{
		rows.current.parentNamespace,
		rows.current.parentTable,
		rows.current.name,
		rows.current.referencedNamespace,
		rows.current.referencedTable,
		rows.current.actionCode,
		rows.current.referencedColumn,
	}
	for index, destination := range destinations {
		value, ok := destination.(*string)
		if !ok {
			return fmt.Errorf(
				"unexpected PostgreSQL test scan destination %d",
				index,
			)
		}
		*value = values[index]
	}
	return nil
}

func (*postgresStage4NetworkTestRows) Err() error {
	return nil
}

func (*postgresStage4NetworkTestRows) Close() error {
	return nil
}

type postgresStage4NetworkTargetWriterRecorder struct {
	stage4Calls  int
	rebuildCalls int
	rebuildMode  NetworkWriteMode
}

func (writer *postgresStage4NetworkTargetWriterRecorder) WriteStage4NetworkRebuildBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	writer.rebuildCalls++
	writer.rebuildMode = mode
	attempted := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: attempted,
		CommittedRows: attempted,
	}, nil
}

func (*postgresStage4NetworkTargetWriterRecorder) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	return WriteReceipt{}, errors.New("unexpected ordinary write")
}

func (writer *postgresStage4NetworkTargetWriterRecorder) WriteStage4NetworkBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) (WriteReceipt, error) {
	writer.stage4Calls++
	attempted := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: attempted,
		CommittedRows: attempted,
	}, nil
}
