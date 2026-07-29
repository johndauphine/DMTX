package migrate

import (
	"context"
	"errors"
	"testing"
)

func TestPostgresNativeWriterRollbackFailureDoesNotMaskWriteFailure(
	t *testing.T,
) {
	writer, _, transaction := newPostgresNativeTestWriter()
	writeErr := errors.New("forced PostgreSQL COPY failure")
	rollbackErr := errors.New("forced PostgreSQL rollback failure")
	transaction.copyErr = writeErr
	transaction.rollbackErr = rollbackErr

	receipt, err := writer.WriteBatch(
		context.Background(),
		postgresNativeTestTable(),
		[]string{"id", `pay"load`},
		"drop_recreate",
		[][]any{{int64(1), "payload"}},
	)
	if !errors.Is(err, writeErr) {
		t.Fatalf("error = %v, want write failure", err)
	}
	if errors.Is(err, rollbackErr) {
		t.Fatalf("rollback failure replaced primary error: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 1, 0)
	assertPostgresOperations(
		t,
		transaction,
		"begin",
		"copy",
		"rollback",
	)
}
