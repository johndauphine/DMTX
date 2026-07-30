package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

type recordingClickHouseProvider struct {
	transaction *recordingClickHouseTransaction
	beginErr    error
}

func (provider *recordingClickHouseProvider) Begin(
	context.Context,
) (clickHouseBatchTransaction, error) {
	if provider.beginErr != nil {
		return nil, provider.beginErr
	}
	return provider.transaction, nil
}

type recordingClickHouseTransaction struct {
	statement  *recordingClickHouseStatement
	prepareErr error
	commitErr  error
	rollback   int
	query      string
}

func (transaction *recordingClickHouseTransaction) Prepare(
	_ context.Context,
	query string,
) (clickHouseBatchStatement, error) {
	transaction.query = query
	if transaction.prepareErr != nil {
		return nil, transaction.prepareErr
	}
	return transaction.statement, nil
}

func (transaction *recordingClickHouseTransaction) Commit() error {
	return transaction.commitErr
}

func (transaction *recordingClickHouseTransaction) Rollback() error {
	transaction.rollback++
	return nil
}

type recordingClickHouseStatement struct {
	rows      [][]any
	appendErr error
	closeErr  error
}

func (statement *recordingClickHouseStatement) Append(
	_ context.Context,
	row []any,
) error {
	if statement.appendErr != nil {
		return statement.appendErr
	}
	statement.rows = append(statement.rows, append([]any(nil), row...))
	return nil
}

func (statement *recordingClickHouseStatement) Close() error {
	return statement.closeErr
}

func clickHouseWriterFixture() (
	schema.Table,
	[]string,
	[][]any,
) {
	table := schema.Table{
		Schema: "analytics",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "score", Type: "double", Nullable: true},
			{Name: "note", Type: "text"},
			{Name: "payload", Type: "blob", Nullable: true},
		},
	}
	columns := []string{"id", "score", "note", "payload"}
	rows := [][]any{
		{int64(1), float64(1.25), "snowman ☃", []byte{0, 1, 255}},
		{int64(2), nil, "", []byte{}},
	}
	return table, columns, rows
}

func TestClickHouseNativeWriterDurablySendsBoundedBatch(t *testing.T) {
	table, columns, rows := clickHouseWriterFixture()
	statement := &recordingClickHouseStatement{}
	transaction := &recordingClickHouseTransaction{statement: statement}
	writer := &clickHouseNativeWriter{
		transactions: &recordingClickHouseProvider{
			transaction: transaction,
		},
	}
	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		columns,
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptedRows != 2 ||
		receipt.CommittedRows != 2 {
		t.Fatalf("receipt = %+v", receipt)
	}
	const wantQuery = `INSERT INTO "analytics"."events" (` +
		`"id", "score", "note", "payload") VALUES (?, ?, ?, ?)`
	if transaction.query != wantQuery {
		t.Fatalf("query = %q, want %q", transaction.query, wantQuery)
	}
	if !reflect.DeepEqual(statement.rows, rows) {
		t.Fatalf("appended rows = %#v, want %#v", statement.rows, rows)
	}
	if transaction.rollback != 0 {
		t.Fatalf("rollback calls = %d", transaction.rollback)
	}
}

func TestClickHouseNativeWriterClassifiesPreSendAndCommitFailures(t *testing.T) {
	sentinel := errors.New("secret row and connection detail")
	table, columns, rows := clickHouseWriterFixture()
	tests := []struct {
		name      string
		provider  *recordingClickHouseProvider
		certainty CommitCertainty
		rollback  int
	}{
		{
			name:      "begin",
			provider:  &recordingClickHouseProvider{beginErr: sentinel},
			certainty: CommitNotCommitted,
		},
		{
			name: "prepare",
			provider: &recordingClickHouseProvider{
				transaction: &recordingClickHouseTransaction{
					prepareErr: sentinel,
				},
			},
			certainty: CommitNotCommitted,
			rollback:  1,
		},
		{
			name: "append",
			provider: &recordingClickHouseProvider{
				transaction: &recordingClickHouseTransaction{
					statement: &recordingClickHouseStatement{
						appendErr: sentinel,
					},
				},
			},
			certainty: CommitNotCommitted,
			rollback:  1,
		},
		{
			name: "close",
			provider: &recordingClickHouseProvider{
				transaction: &recordingClickHouseTransaction{
					statement: &recordingClickHouseStatement{
						closeErr: sentinel,
					},
				},
			},
			certainty: CommitNotCommitted,
			rollback:  1,
		},
		{
			name: "commit",
			provider: &recordingClickHouseProvider{
				transaction: &recordingClickHouseTransaction{
					statement: &recordingClickHouseStatement{},
					commitErr: sentinel,
				},
			},
			certainty: CommitUnknown,
			rollback:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := &clickHouseNativeWriter{
				transactions: test.provider,
			}
			receipt, err := writer.WriteBatch(
				context.Background(),
				table,
				columns,
				"drop_recreate",
				rows,
			)
			if err == nil || !errors.Is(err, sentinel) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), sentinel.Error()) {
				t.Fatalf("error exposed driver detail: %v", err)
			}
			if receipt.Certainty != test.certainty ||
				receipt.AttemptedRows != int64(len(rows)) ||
				receipt.CommittedRows != 0 {
				t.Fatalf("receipt = %+v", receipt)
			}
			if test.provider.transaction != nil &&
				test.provider.transaction.rollback != test.rollback {
				t.Fatalf(
					"rollback calls = %d, want %d",
					test.provider.transaction.rollback,
					test.rollback,
				)
			}
		})
	}
}

func TestClickHouseNativeWriterRejectsRowsBeforeMutation(t *testing.T) {
	table, columns, _ := clickHouseWriterFixture()
	tests := []struct {
		name string
		rows [][]any
		want string
	}{
		{
			name: "wrong integer type",
			rows: [][]any{{1, nil, "", []byte{}}},
			want: "unsupported Go type int",
		},
		{
			name: "null nonnullable",
			rows: [][]any{{int64(1), nil, nil, []byte{}}},
			want: "NULL but not nullable",
		},
		{
			name: "text in blob",
			rows: [][]any{{int64(1), nil, "", "not bytes"}},
			want: "unsupported Go type string",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &recordingClickHouseProvider{}
			writer := &clickHouseNativeWriter{transactions: provider}
			receipt, err := writer.WriteBatch(
				context.Background(),
				table,
				columns,
				"drop_recreate",
				test.rows,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if receipt.Certainty != CommitNotCommitted {
				t.Fatalf("receipt = %+v", receipt)
			}
		})
	}
}
