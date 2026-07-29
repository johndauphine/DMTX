package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPostgresNativeWriterRedactsUnderlyingOperationErrors(t *testing.T) {
	const secret = "secret-row-or-dsn-value"
	cause := errors.New(secret)
	table := postgresNativeTestTable()
	columns := []string{"id", `pay"load`}
	rows := [][]any{{int64(1), "payload"}}

	tests := []struct {
		name      string
		mode      string
		context   string
		certainty CommitCertainty
		configure func(
			*postgresNativeTestProvider,
			*postgresNativeTestTransaction,
		)
	}{
		{
			name:      "provider",
			mode:      "drop_recreate",
			context:   "acquire PostgreSQL native connection for " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				provider *postgresNativeTestProvider,
				_ *postgresNativeTestTransaction,
			) {
				provider.err = cause
			},
		},
		{
			name:      "begin",
			mode:      "drop_recreate",
			context:   "begin PostgreSQL native write for " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.beginErr = cause
			},
		},
		{
			name:      "copy",
			mode:      "drop_recreate",
			context:   "copy PostgreSQL table " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.copyErr = cause
			},
		},
		{
			name:      "stage",
			mode:      "upsert",
			context:   "create PostgreSQL staging table for " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.execResults = []postgresNativeTestExecResult{
					{err: cause},
				}
			},
		},
		{
			name:      "staging copy",
			mode:      "upsert",
			context:   "copy PostgreSQL staging table for " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.execResults = []postgresNativeTestExecResult{{}}
				transaction.copyErr = cause
			},
		},
		{
			name:      "merge",
			mode:      "upsert",
			context:   "merge PostgreSQL staging table for " + table.Name,
			certainty: CommitNotCommitted,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.execResults = []postgresNativeTestExecResult{
					{},
					{err: cause},
				}
			},
		},
		{
			name:      "commit",
			mode:      "drop_recreate",
			context:   "commit PostgreSQL table " + table.Name,
			certainty: CommitUnknown,
			configure: func(
				_ *postgresNativeTestProvider,
				transaction *postgresNativeTestTransaction,
			) {
				transaction.commitErr = cause
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, provider, transaction := newPostgresNativeTestWriter()
			test.configure(provider, transaction)
			receipt, err := writer.WriteBatch(
				context.Background(),
				table,
				columns,
				test.mode,
				rows,
			)
			if !errors.Is(err, cause) {
				t.Fatalf("error = %v, want errors.Is(cause)", err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("operation error leaked sensitive cause: %v", err)
			}
			if err.Error() != test.context {
				t.Fatalf(
					"error = %q, want %q",
					err.Error(),
					test.context,
				)
			}
			assertPostgresReceipt(
				t,
				receipt,
				test.certainty,
				1,
				0,
			)
		})
	}
}
