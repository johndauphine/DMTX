package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresNativeWriterDropRecreateUsesDirectCopy(t *testing.T) {
	writer, provider, transaction := newPostgresNativeTestWriter()
	table := postgresNativeTestTable()
	rows := [][]any{
		{int64(1), []byte("first")},
		{int32(2), "second"},
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id", `pay"load`},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitDurable,
		int64(len(rows)),
		int64(len(rows)),
	)
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if got, want := transaction.operations,
		[]string{"begin", "copy", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	if len(transaction.copies) != 1 {
		t.Fatalf("copy calls = %d, want 1", len(transaction.copies))
	}
	call := transaction.copies[0]
	if got, want := call.table,
		[]string{`Target Schema`, `event"data`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("COPY table = %#v, want %#v", got, want)
	}
	if got, want := call.columns,
		[]string{"id", `pay"load`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("COPY columns = %#v, want %#v", got, want)
	}
	if _, ok := call.rows[0][0].(int32); !ok {
		t.Fatalf("normalized integer type = %T, want int32", call.rows[0][0])
	}
	if got, ok := call.rows[0][1].(string); !ok || got != "first" {
		t.Fatalf("normalized text = %#v (%T)", call.rows[0][1], call.rows[0][1])
	}
}

func TestPostgresNativeWriterUpsertStagesCopiesAndMerges(t *testing.T) {
	writer, _, transaction := newPostgresNativeTestWriter()
	table := postgresNativeTestTable()
	columns := []string{"id", `pay"load`}
	rows := [][]any{{int64(1), "updated"}, {int64(2), "inserted"}}
	transaction.execResults = []postgresNativeTestExecResult{
		{rowsAffected: 0},
		{rowsAffected: int64(len(rows))},
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		columns,
		"upsert",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitDurable,
		int64(len(rows)),
		int64(len(rows)),
	)
	if got, want := transaction.operations,
		[]string{"begin", "exec", "copy", "exec", "commit"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}

	stage := postgresStageTableName(table, columns)
	wantCreate := `CREATE TEMP TABLE ` + postgresIdentifier(stage) +
		` ON COMMIT DROP AS SELECT "id", "pay""load"` +
		` FROM "Target Schema"."event""data" WITH NO DATA`
	if transaction.statements[0] != wantCreate {
		t.Fatalf(
			"create stage statement = %q, want %q",
			transaction.statements[0],
			wantCreate,
		)
	}
	if got, want := transaction.copies[0].table,
		[]string{"pg_temp", stage}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staging COPY table = %#v, want %#v", got, want)
	}
	wantMerge := `INSERT INTO "Target Schema"."event""data"` +
		` ("id", "pay""load") SELECT "id", "pay""load"` +
		` FROM "pg_temp".` + postgresIdentifier(stage) +
		` WHERE true ON CONFLICT ("id") DO UPDATE SET` +
		` "pay""load" = EXCLUDED."pay""load"`
	if transaction.statements[1] != wantMerge {
		t.Fatalf(
			"merge statement = %q, want %q",
			transaction.statements[1],
			wantMerge,
		)
	}
}

func TestPostgresNativeWriterKeyOnlyUpsertDoesNotRequireAffectedRows(t *testing.T) {
	writer, _, transaction := newPostgresNativeTestWriter()
	table := schema.Table{
		Schema: "public",
		Name:   "keys",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
		},
	}
	rows := [][]any{{int64(1)}, {int64(2)}}
	transaction.execResults = []postgresNativeTestExecResult{
		{rowsAffected: 0},
		{rowsAffected: 0},
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id"},
		"upsert",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertPostgresReceipt(
		t,
		receipt,
		CommitDurable,
		int64(len(rows)),
		int64(len(rows)),
	)
	if got := transaction.statements[1]; !strings.HasSuffix(
		got,
		`ON CONFLICT ("id") DO NOTHING`,
	) {
		t.Fatalf("key-only merge = %q", got)
	}
}

func TestPostgresNativeWriterZeroBatchIsDurableWithoutConnection(t *testing.T) {
	writer, provider, _ := newPostgresNativeTestWriter()
	receipt, err := writer.WriteBatch(
		context.Background(),
		postgresNativeTestTable(),
		[]string{"id", `pay"load`},
		"drop_recreate",
		nil,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 0, 0)
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestPostgresNativeWriterNormalizesBeforeConnectionWithoutLeakingRows(t *testing.T) {
	writer, provider, _ := newPostgresNativeTestWriter()
	const secret = "do-not-print-this-row-value"
	receipt, err := writer.WriteBatch(
		context.Background(),
		postgresNativeTestTable(),
		[]string{"id", `pay"load`},
		"drop_recreate",
		[][]any{{secret, "payload"}},
	)
	if err == nil || !strings.Contains(err.Error(), "signed 32-bit integer") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("normalization error leaked row value: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestPostgresNativeWriterFailureReceiptsAndOrdering(t *testing.T) {
	table := postgresNativeTestTable()
	columns := []string{"id", `pay"load`}
	rows := [][]any{{int64(1), "payload"}}
	forced := errors.New("forced native writer failure")

	t.Run("provider", func(t *testing.T) {
		writer, provider, transaction := newPostgresNativeTestWriter()
		provider.err = forced
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if !errors.Is(err, forced) {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
		if len(transaction.operations) != 0 {
			t.Fatalf("operations = %#v", transaction.operations)
		}
	})

	t.Run("begin", func(t *testing.T) {
		writer, _, transaction := newPostgresNativeTestWriter()
		transaction.beginErr = forced
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if !errors.Is(err, forced) {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
		assertPostgresOperations(t, transaction, "begin")
	})

	t.Run("copy", func(t *testing.T) {
		writer, _, transaction := newPostgresNativeTestWriter()
		transaction.copyErr = forced
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if !errors.Is(err, forced) {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
		assertPostgresOperations(t, transaction, "begin", "copy", "rollback")
	})

	t.Run("short copy", func(t *testing.T) {
		writer, _, transaction := newPostgresNativeTestWriter()
		transaction.copyCount = 0
		transaction.copyCountSet = true
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "acknowledged 0 rows, expected 1") {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
		assertPostgresOperations(t, transaction, "begin", "copy", "rollback")
	})

	t.Run("commit unknown", func(t *testing.T) {
		writer, _, transaction := newPostgresNativeTestWriter()
		transaction.commitErr = forced
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if !errors.Is(err, forced) {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitUnknown, 1, 0)
		assertPostgresOperations(
			t,
			transaction,
			"begin",
			"copy",
			"commit",
			"rollback",
		)
	})

	t.Run("commit rolled back", func(t *testing.T) {
		writer, _, transaction := newPostgresNativeTestWriter()
		transaction.commitErr = pgx.ErrTxCommitRollback
		receipt, err := writer.WriteBatch(
			context.Background(), table, columns, "drop_recreate", rows,
		)
		if !errors.Is(err, pgx.ErrTxCommitRollback) {
			t.Fatalf("error = %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
		assertPostgresOperations(
			t,
			transaction,
			"begin",
			"copy",
			"commit",
			"rollback",
		)
	})
}

func TestPostgresNativeWriterUpsertFailuresRollBackBeforeCommit(t *testing.T) {
	table := postgresNativeTestTable()
	columns := []string{"id", `pay"load`}
	rows := [][]any{{int64(1), "payload"}}
	forced := errors.New("forced upsert failure")

	tests := []struct {
		name       string
		configure  func(*postgresNativeTestTransaction)
		wantError  string
		operations []string
	}{
		{
			name: "create stage",
			configure: func(transaction *postgresNativeTestTransaction) {
				transaction.execResults = []postgresNativeTestExecResult{
					{err: forced},
				}
			},
			wantError:  "create PostgreSQL staging table",
			operations: []string{"begin", "exec", "rollback"},
		},
		{
			name: "copy stage",
			configure: func(transaction *postgresNativeTestTransaction) {
				transaction.execResults = []postgresNativeTestExecResult{{}}
				transaction.copyErr = forced
			},
			wantError: "copy PostgreSQL staging table",
			operations: []string{
				"begin", "exec", "copy", "rollback",
			},
		},
		{
			name: "merge",
			configure: func(transaction *postgresNativeTestTransaction) {
				transaction.execResults = []postgresNativeTestExecResult{
					{},
					{err: forced},
				}
			},
			wantError: "merge PostgreSQL staging table",
			operations: []string{
				"begin", "exec", "copy", "exec", "rollback",
			},
		},
		{
			name: "merge count",
			configure: func(transaction *postgresNativeTestTransaction) {
				transaction.execResults = []postgresNativeTestExecResult{
					{},
					{rowsAffected: 0},
				}
			},
			wantError: "affected 0 rows, expected 1",
			operations: []string{
				"begin", "exec", "copy", "exec", "rollback",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, _, transaction := newPostgresNativeTestWriter()
			test.configure(transaction)
			receipt, err := writer.WriteBatch(
				context.Background(),
				table,
				columns,
				"upsert",
				rows,
			)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v", err)
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
				test.operations,
			) {
				t.Fatalf(
					"operations = %#v, want %#v",
					transaction.operations,
					test.operations,
				)
			}
		})
	}
}

func postgresNativeTestTable() schema.Table {
	return schema.Table{
		Schema: `Target Schema`,
		Name:   `event"data`,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: `pay"load`, Type: "text"},
		},
	}
}

func assertPostgresReceipt(
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
		t.Fatalf("receipt.Validate: %v", err)
	}
}

func assertPostgresOperations(
	t *testing.T,
	transaction *postgresNativeTestTransaction,
	want ...string,
) {
	t.Helper()
	if !reflect.DeepEqual(transaction.operations, want) {
		t.Fatalf(
			"operations = %#v, want %#v",
			transaction.operations,
			want,
		)
	}
}

func newPostgresNativeTestWriter() (
	*postgresNativeWriter,
	*postgresNativeTestProvider,
	*postgresNativeTestTransaction,
) {
	transaction := &postgresNativeTestTransaction{}
	provider := &postgresNativeTestProvider{
		connection: postgresNativeTestConnection{
			transaction: transaction,
		},
	}
	return &postgresNativeWriter{connections: provider}, provider, transaction
}

type postgresNativeTestProvider struct {
	connection postgresNativeTestConnection
	err        error
	calls      int
}

func (provider *postgresNativeTestProvider) WithConnection(
	ctx context.Context,
	operation func(postgresNativeConnection) error,
) error {
	provider.calls++
	if provider.err != nil {
		return provider.err
	}
	return operation(provider.connection)
}

type postgresNativeTestConnection struct {
	transaction *postgresNativeTestTransaction
}

func (connection postgresNativeTestConnection) Begin(
	context.Context,
) (postgresNativeTransaction, error) {
	connection.transaction.operations = append(
		connection.transaction.operations,
		"begin",
	)
	if connection.transaction.beginErr != nil {
		return nil, connection.transaction.beginErr
	}
	return connection.transaction, nil
}

type postgresNativeTestExecResult struct {
	rowsAffected int64
	err          error
}

type postgresNativeTestCopyCall struct {
	table   []string
	columns []string
	rows    [][]any
}

type postgresNativeTestTransaction struct {
	operations   []string
	statements   []string
	copies       []postgresNativeTestCopyCall
	execResults  []postgresNativeTestExecResult
	execIndex    int
	beginErr     error
	copyErr      error
	copyCount    int64
	copyCountSet bool
	commitErr    error
	rollbackErr  error
}

func (transaction *postgresNativeTestTransaction) CopyRows(
	_ context.Context,
	table []string,
	columns []string,
	rows [][]any,
) (int64, error) {
	transaction.operations = append(transaction.operations, "copy")
	transaction.copies = append(
		transaction.copies,
		postgresNativeTestCopyCall{
			table:   append([]string(nil), table...),
			columns: append([]string(nil), columns...),
			rows:    clonePostgresNativeTestRows(rows),
		},
	)
	if transaction.copyErr != nil {
		return 0, transaction.copyErr
	}
	if transaction.copyCountSet {
		return transaction.copyCount, nil
	}
	return int64(len(rows)), nil
}

func (transaction *postgresNativeTestTransaction) Exec(
	_ context.Context,
	statement string,
) (int64, error) {
	transaction.operations = append(transaction.operations, "exec")
	transaction.statements = append(transaction.statements, statement)
	if transaction.execIndex >= len(transaction.execResults) {
		return 0, nil
	}
	result := transaction.execResults[transaction.execIndex]
	transaction.execIndex++
	return result.rowsAffected, result.err
}

func (transaction *postgresNativeTestTransaction) Commit(
	context.Context,
) error {
	transaction.operations = append(transaction.operations, "commit")
	return transaction.commitErr
}

func (transaction *postgresNativeTestTransaction) Rollback(
	context.Context,
) error {
	transaction.operations = append(transaction.operations, "rollback")
	return transaction.rollbackErr
}

func clonePostgresNativeTestRows(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	for index, row := range rows {
		cloned[index] = append([]any(nil), row...)
	}
	return cloned
}
