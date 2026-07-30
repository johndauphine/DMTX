package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLNativeWriteStatementUsesQualifiedAliasUpsert(
	t *testing.T,
) {
	table := mysqlNativeTestTable()
	statement := mySQLNativeWriteStatement(
		table,
		[]string{"id", "payload"},
		"upsert",
	)
	for _, expected := range []string{
		"INSERT INTO `target_db`.`events`",
		"VALUES (?, ?)",
		"AS `dmtx_new` ON DUPLICATE KEY UPDATE",
		"`id` = IF(`events`.`id` <=> `dmtx_new`.`id`, `events`.`id`, JSON_EXTRACT('dmtx-invalid-json', '$'))",
		"`payload` = `dmtx_new`.`payload`",
	} {
		if !strings.Contains(statement, expected) {
			t.Fatalf(
				"statement %q does not contain %q",
				statement,
				expected,
			)
		}
	}
	if strings.Contains(statement, "VALUES(`payload`)") {
		t.Fatalf("statement uses deprecated VALUES() reference: %q", statement)
	}
}

func TestMySQLNativeWriteStatementWithOnlyPrimaryKeyUsesNoOpUpdate(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "target_db",
		Name:   "events",
		Columns: []schema.Column{{
			Name:       "id",
			Type:       "bigint",
			PrimaryKey: true,
		}},
	}
	statement := mySQLNativeWriteStatement(
		table,
		[]string{"id"},
		"upsert",
	)
	if !strings.Contains(
		statement,
		"`id` = IF(`events`.`id` <=> `dmtx_new`.`id`",
	) {
		t.Fatalf("unexpected statement: %q", statement)
	}
}

func TestMySQLNativeWriteStatementAvoidsTableAliasCollision(t *testing.T) {
	table := mysqlNativeTestTable()
	table.Name = "DMTX_NEW"
	statement := mySQLNativeWriteStatement(
		table,
		[]string{"id", "payload"},
		"upsert",
	)
	if !strings.Contains(
		statement,
		"AS `dmtx_incoming` ON DUPLICATE KEY UPDATE",
	) || strings.Contains(statement, "AS `dmtx_new`") {
		t.Fatalf("unsafe row alias: %q", statement)
	}
}

func TestValidateMySQLWriteShapeRejectsUnsafeRequests(t *testing.T) {
	table := mysqlNativeTestTable()
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		mode    string
		want    string
	}{
		{
			name:    "missing namespace",
			table:   schema.Table{Name: "events"},
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "database and table name",
		},
		{
			name:    "unknown mode",
			table:   table,
			columns: []string{"id"},
			mode:    "replace",
			want:    "unsupported target mode",
		},
		{
			name:    "unknown column",
			table:   table,
			columns: []string{"missing"},
			mode:    "drop_recreate",
			want:    "not present in schema",
		},
		{
			name: "missing upsert key",
			table: schema.Table{
				Schema: "target_db",
				Name:   "events",
				Columns: []schema.Column{{
					Name: "payload",
				}},
			},
			columns: []string{"payload"},
			mode:    "upsert",
			want:    "has no primary key",
		},
		{
			name:    "omitted upsert key",
			table:   table,
			columns: []string{"payload"},
			mode:    "upsert",
			want:    "primary-key column id is not included",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMySQLWriteShape(
				test.table,
				test.columns,
				test.mode,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestMySQLNativeWriterCommitsCompleteWarningFreeBatch(t *testing.T) {
	writer, transaction := newMySQLNativeTestWriter()
	transaction.statement.affected = []int64{1, 1}
	transaction.warnings = []int64{0, 0}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{
			{int64(1), "one"},
			{int64(2), "two"},
		},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if transaction.commits != 1 || transaction.rollbacks != 0 {
		t.Fatalf(
			"transaction commits=%d rollbacks=%d",
			transaction.commits,
			transaction.rollbacks,
		)
	}
	if transaction.warningCalls != 2 ||
		len(transaction.statement.rows) != 2 ||
		!transaction.statement.closed {
		t.Fatalf("transaction = %#v", transaction)
	}
}

func TestMySQLNativeWriterRollsBackOnConversionWarning(t *testing.T) {
	writer, transaction := newMySQLNativeTestWriter()
	transaction.statement.affected = []int64{1}
	transaction.warnings = []int64{1}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "secret-row-value"}},
	)
	if err == nil || !strings.Contains(err.Error(), "conversion warnings") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), "secret-row-value") {
		t.Fatalf("error exposed a row value: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.commits != 0 || transaction.rollbacks != 1 {
		t.Fatalf(
			"transaction commits=%d rollbacks=%d",
			transaction.commits,
			transaction.rollbacks,
		)
	}
}

func TestMySQLNativeWriterReportsUnknownCommitOutcome(t *testing.T) {
	writer, transaction := newMySQLNativeTestWriter()
	transaction.statement.affected = []int64{1}
	transaction.warnings = []int64{0}
	commitErr := errors.New("driver text containing secret-row-value")
	transaction.commitErr = commitErr

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if !errors.Is(err, commitErr) {
		t.Fatalf("error = %v, want commit failure", err)
	}
	if strings.Contains(err.Error(), "secret-row-value") {
		t.Fatalf("safe error exposed driver text: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitUnknown, 1, 0)
	if transaction.commits != 1 || transaction.rollbacks != 1 {
		t.Fatalf(
			"transaction commits=%d rollbacks=%d",
			transaction.commits,
			transaction.rollbacks,
		)
	}
}

func TestMySQLNativeWriterRejectsRowWidthBeforeTransaction(t *testing.T) {
	writer, transaction := newMySQLNativeTestWriter()
	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1)}},
	)
	if err == nil || !strings.Contains(err.Error(), "has 1 values") {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.begins != 0 {
		t.Fatalf("transaction began before row validation")
	}
}

func mysqlNativeTestTable() schema.Table {
	return schema.Table{
		Schema: "target_db",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
	}
}

func assertMySQLNativeReceipt(
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
}

type mysqlNativeTestProvider struct {
	transaction *mysqlNativeTestTransaction
}

func (provider *mysqlNativeTestProvider) Begin(
	context.Context,
) (mysqlBatchTransaction, error) {
	provider.transaction.begins++
	return provider.transaction, nil
}

type mysqlNativeTestTransaction struct {
	begins       int
	commits      int
	rollbacks    int
	warningCalls int
	warnings     []int64
	commitErr    error
	statement    *mysqlNativeTestStatement
}

func (transaction *mysqlNativeTestTransaction) Prepare(
	_ context.Context,
	statement string,
) (mysqlBatchStatement, error) {
	transaction.statement.query = statement
	return transaction.statement, nil
}

func (transaction *mysqlNativeTestTransaction) WarningCount(
	context.Context,
) (int64, error) {
	index := transaction.warningCalls
	transaction.warningCalls++
	return transaction.warnings[index], nil
}

func (transaction *mysqlNativeTestTransaction) Commit() error {
	transaction.commits++
	return transaction.commitErr
}

func (transaction *mysqlNativeTestTransaction) Rollback() error {
	transaction.rollbacks++
	return nil
}

type mysqlNativeTestStatement struct {
	query    string
	rows     [][]any
	affected []int64
	closed   bool
}

func (statement *mysqlNativeTestStatement) Exec(
	_ context.Context,
	values []any,
) (int64, error) {
	index := len(statement.rows)
	statement.rows = append(statement.rows, append([]any(nil), values...))
	return statement.affected[index], nil
}

func (statement *mysqlNativeTestStatement) Close() error {
	statement.closed = true
	return nil
}

func newMySQLNativeTestWriter() (
	*mysqlNativeWriter,
	*mysqlNativeTestTransaction,
) {
	transaction := &mysqlNativeTestTransaction{
		statement: &mysqlNativeTestStatement{},
	}
	return &mysqlNativeWriter{
		transactions: &mysqlNativeTestProvider{
			transaction: transaction,
		},
	}, transaction
}
