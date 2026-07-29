package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

const postgresCommitErrorDriverName = "dmtx-postgres-commit-error"

var (
	registerPostgresCommitErrorDriver sync.Once
	errPostgresCommit                 = errors.New("forced PostgreSQL commit failure")
)

func TestPostgresTargetCommitFailureReturnsUnknownReceipt(t *testing.T) {
	database := newPostgresCommitErrorDatabase(t)
	adapter := &postgresTargetAdapter{
		database:  database,
		namespace: "public",
	}
	rows := [][]any{{int64(1), "first"}, {int64(2), "second"}}

	receipt, err := adapter.WriteBatch(
		context.Background(),
		schema.Table{
			Schema: "public",
			Name:   "events",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
		[]string{"id", "payload"},
		"drop_recreate",
		rows,
	)
	if !errors.Is(err, errPostgresCommit) ||
		!strings.Contains(err.Error(), "commit PostgreSQL table events") {
		t.Fatalf("error = %v", err)
	}
	if receipt.Certainty != CommitUnknown ||
		receipt.AttemptedRows != int64(len(rows)) ||
		receipt.CommittedRows != 0 {
		t.Fatalf("receipt = %#v", receipt)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("receipt.Validate: %v", err)
	}
}

func newPostgresCommitErrorDatabase(t *testing.T) *sql.DB {
	t.Helper()
	registerPostgresCommitErrorDriver.Do(func() {
		sql.Register(postgresCommitErrorDriverName, postgresCommitErrorDriver{})
	})
	database, err := sql.Open(postgresCommitErrorDriverName, "")
	if err != nil {
		t.Fatalf("open commit-error database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close commit-error database: %v", err)
		}
	})
	return database
}

type postgresCommitErrorDriver struct{}

func (postgresCommitErrorDriver) Open(string) (driver.Conn, error) {
	return &postgresCommitErrorConnection{}, nil
}

type postgresCommitErrorConnection struct{}

func (*postgresCommitErrorConnection) Prepare(string) (driver.Stmt, error) {
	return &postgresCommitErrorStatement{}, nil
}

func (*postgresCommitErrorConnection) Close() error {
	return nil
}

func (*postgresCommitErrorConnection) Begin() (driver.Tx, error) {
	return &postgresCommitErrorTransaction{}, nil
}

type postgresCommitErrorTransaction struct{}

func (*postgresCommitErrorTransaction) Commit() error {
	return errPostgresCommit
}

func (*postgresCommitErrorTransaction) Rollback() error {
	return nil
}

type postgresCommitErrorStatement struct{}

func (*postgresCommitErrorStatement) Close() error {
	return nil
}

func (*postgresCommitErrorStatement) NumInput() int {
	return -1
}

func (*postgresCommitErrorStatement) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (*postgresCommitErrorStatement) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("commit-error driver does not support queries")
}
