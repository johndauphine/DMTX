package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerNativeStatementsUseExactQualifiedIdentifiersAndLocks(
	t *testing.T,
) {
	table := sqlServerNativeTestTable()
	columns := []string{"payload", "tenant_id", "id"}
	plan, err := planSQLServerNativeUpsert(table, columns)
	if err != nil {
		t.Fatalf("planSQLServerNativeUpsert: %v", err)
	}
	if got, want := plan.insertSQL,
		"INSERT INTO [Target]]Schema].[event]]data] "+
			"([payload], [tenant_id], [id]) VALUES (@p1, @p2, @p3)"; got != want {
		t.Fatalf("insert = %q, want %q", got, want)
	}
	if got, want := plan.updateSQL,
		"UPDATE [Target]]Schema].[event]]data] "+
			"WITH (UPDLOCK, HOLDLOCK) SET [payload] = @p1 "+
			"WHERE [tenant_id] = @p2 AND [id] = @p3"; got != want {
		t.Fatalf("update = %q, want %q", got, want)
	}
	if got, want := plan.updatePositions,
		[]int{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("update positions = %#v, want %#v", got, want)
	}
	if strings.Contains(strings.ToUpper(plan.updateSQL), "MERGE") {
		t.Fatalf("upsert plan uses MERGE: %q", plan.updateSQL)
	}
}

func TestSQLServerNativeKeyOnlyUpsertUsesLockedExistenceProbe(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "dbo",
		Name:   "keys",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	plan, err := planSQLServerNativeUpsert(table, []string{"id"})
	if err != nil {
		t.Fatalf("planSQLServerNativeUpsert: %v", err)
	}
	if plan.updateSQL != "" {
		t.Fatalf("key-only plan has update: %q", plan.updateSQL)
	}
	want := "SELECT CAST(CASE WHEN EXISTS (" +
		"SELECT 1 FROM [dbo].[keys] WITH (UPDLOCK, HOLDLOCK) " +
		"WHERE [id] = @p1) THEN 1 ELSE 0 END AS bit)"
	if plan.existsSQL != want {
		t.Fatalf("existence probe = %q, want %q", plan.existsSQL, want)
	}
	if strings.Contains(strings.ToUpper(plan.existsSQL), "MERGE") {
		t.Fatalf("upsert plan uses MERGE: %q", plan.existsSQL)
	}
}

func TestSQLServerNativeBulkStatementEnablesExactSafetyOptions(
	t *testing.T,
) {
	statement := sqlServerNativeBulkStatement(
		sqlServerNativeTestTable(),
		[]string{"id", "payload"},
		37,
	)
	for _, expected := range []string{
		`INSERTBULK `,
		`"TableName":"[Target]]Schema].[event]]data]"`,
		`"ColumnsName":["id","payload"]`,
		`"CheckConstraints":true`,
		`"KeepNulls":true`,
		`"RowsPerBatch":37`,
	} {
		if !strings.Contains(statement, expected) {
			t.Fatalf(
				"bulk statement %q does not contain %q",
				statement,
				expected,
			)
		}
	}
}

func TestSQLServerNativeSessionGuardPinsRequiredSettings(t *testing.T) {
	settings := []string{
		"SET NOCOUNT OFF;",
		"SET XACT_ABORT ON;",
		"SET ANSI_WARNINGS ON;",
		"SET ARITHABORT ON;",
		"SET ANSI_NULLS ON;",
		"SET QUOTED_IDENTIFIER ON;",
		"SET CONCAT_NULL_YIELDS_NULL ON;",
		"SET NUMERIC_ROUNDABORT OFF;",
	}
	position := -1
	for _, setting := range settings {
		next := strings.Index(
			sqlServerNativeSessionGuardStatement,
			setting,
		)
		if next <= position {
			t.Fatalf(
				"session guard setting %q is missing or out of order: %q",
				setting,
				sqlServerNativeSessionGuardStatement,
			)
		}
		position = next
	}
}

func TestValidateSQLServerWriteShapeRejectsUnsafeRequests(t *testing.T) {
	table := sqlServerNativeTestTable()
	identityTable := sqlServerNativeIdentityTestTable()
	compositeIdentity := table
	compositeIdentity.Identity = &schema.Identity{
		Column:     "id",
		Generation: schema.IdentityByDefault,
	}
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		mode    string
		want    string
	}{
		{
			name:    "missing schema",
			table:   schema.Table{Name: "events"},
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "schema and table name",
		},
		{
			name:    "unknown mode",
			table:   table,
			columns: []string{"id"},
			mode:    "replace",
			want:    "unsupported target mode",
		},
		{
			name: "ambiguous metadata",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					{Name: "ID"},
					{Name: "id"},
				},
			},
			columns: []string{"ID"},
			mode:    "drop_recreate",
			want:    "ambiguous columns",
		},
		{
			name:    "inexact column spelling",
			table:   table,
			columns: []string{"ID"},
			mode:    "drop_recreate",
			want:    "exact spelling",
		},
		{
			name:    "identity omitted",
			table:   identityTable,
			columns: []string{"payload"},
			mode:    "drop_recreate",
			want:    "identity column id is not included",
		},
		{
			name:    "composite identity key",
			table:   compositeIdentity,
			columns: []string{"tenant_id", "id", "payload"},
			mode:    "drop_recreate",
			want:    "identity metadata is unsupported",
		},
		{
			name: "no upsert key",
			table: schema.Table{
				Schema:  "dbo",
				Name:    "events",
				Columns: []schema.Column{{Name: "payload"}},
			},
			columns: []string{"payload"},
			mode:    "upsert",
			want:    "has no primary key",
		},
		{
			name: "nullable upsert key",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{{
					Name:       "id",
					Nullable:   true,
					PrimaryKey: true,
				}},
			},
			columns: []string{"id"},
			mode:    "upsert",
			want:    "missing or nullable",
		},
		{
			name:    "upsert key omitted",
			table:   table,
			columns: []string{"payload"},
			mode:    "upsert",
			want:    "primary-key column tenant_id is not included",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSQLServerWriteShape(
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

func TestSQLServerNativeWriterBulkCopyFlushesAndCommits(t *testing.T) {
	writer, provider, connection, transaction := newSQLServerNativeTestWriter()
	transaction.bulk.execAffected = []int64{0, 0}
	transaction.bulk.doneAffected = 2
	rows := [][]any{
		{int64(7), int64(1), "first"},
		{int64(8), int64(2), nil},
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if provider.calls != 1 || connection.beginCalls != 1 {
		t.Fatalf(
			"provider calls=%d begin calls=%d",
			provider.calls,
			connection.beginCalls,
		)
	}
	if transaction.bulk.doneCalls != 1 ||
		!transaction.bulk.closed ||
		transaction.commits != 1 ||
		transaction.rollbacks != 0 {
		t.Fatalf("transaction = %#v", transaction)
	}
	if got, want := transaction.bulk.rows, rows; !reflect.DeepEqual(got, want) {
		t.Fatalf("bulk rows = %#v, want %#v", got, want)
	}
	assertSQLServerEvents(
		t,
		transaction.events,
		"begin serializable",
		"tx exec session guard",
		"prepare bulk",
		"bulk exec",
		"bulk exec",
		"bulk done",
		"bulk close",
		"commit",
	)
}

func TestSQLServerNativeWriterTypesBinaryNullWithoutMutatingInput(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	transaction.bulk.execAffected = []int64{0}
	transaction.bulk.doneAffected = 1
	table := sqlServerNativeTestTable()
	table.Columns[2] = schema.Column{
		Name: "payload",
		Type: "blob",
		DeclaredType: &schema.DeclaredType{
			Base:      "varbinary",
			Arguments: []int{16},
		},
	}
	rows := [][]any{{int64(7), int64(1), nil}}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if rows[0][2] != nil {
		t.Fatalf("input binary NULL was mutated to %#v", rows[0][2])
	}
	got := transaction.bulk.rows[0][2]
	bytes, ok := got.([]byte)
	if !ok || bytes != nil {
		t.Fatalf("driver binary NULL = %#v, want nil []byte", got)
	}
}

func TestSQLServerNativeWriterRejectsShortBulkCompletion(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	transaction.bulk.execAffected = []int64{0}
	transaction.bulk.doneAffected = 0

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "acknowledged 0 rows; expected 1") {
		t.Fatalf("error = %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.commits != 0 || transaction.rollbacks != 1 {
		t.Fatalf("transaction = %#v", transaction)
	}
	if transaction.bulk.doneCalls != 1 {
		t.Fatalf("bulk Done calls = %d, want 1", transaction.bulk.doneCalls)
	}
}

func TestSQLServerNativeWriterIdentityUsesPinnedInsertSession(
	t *testing.T,
) {
	writer, _, connection, transaction := newSQLServerNativeTestWriter()
	table := sqlServerNativeIdentityTestTable()
	transaction.insert.execAffected = []int64{1}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "payload"}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if len(transaction.prepared) != 1 ||
		strings.HasPrefix(transaction.prepared[0], "INSERTBULK ") {
		t.Fatalf("identity path prepared %#v", transaction.prepared)
	}
	if len(connection.cleanupStatements) != 0 || connection.discarded {
		t.Fatalf("unexpected connection cleanup: %#v", connection)
	}
	assertSQLServerEvents(
		t,
		transaction.events,
		"begin serializable",
		"tx exec session guard",
		"tx exec SET IDENTITY_INSERT [Target]]Schema].[event]]data] ON",
		"prepare insert",
		"insert exec",
		"insert close",
		"tx exec SET IDENTITY_INSERT [Target]]Schema].[event]]data] OFF",
		"commit",
	)
}

func TestSQLServerNativeWriterTimeUsesPreparedInsertFallback(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	table := sqlServerNativeTestTable()
	table.Columns = append(table.Columns, schema.Column{
		Name: "local_time",
		Type: "time",
		DeclaredType: &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{6},
		},
	})
	transaction.insert.execAffected = []int64{1}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"tenant_id", "id", "payload", "local_time"},
		"drop_recreate",
		[][]any{{
			int64(7),
			int64(1),
			"payload",
			"12:34:56.123456",
		}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if len(transaction.prepared) != 1 ||
		strings.HasPrefix(transaction.prepared[0], "INSERTBULK ") {
		t.Fatalf("TIME path prepared %#v", transaction.prepared)
	}
	if transaction.bulk.doneCalls != 0 {
		t.Fatalf(
			"TIME fallback unexpectedly completed bulk copy %d times",
			transaction.bulk.doneCalls,
		)
	}
}

func TestSQLServerNativeWriterUUIDUsesPreparedInsertFallback(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	table := sqlServerNativeTestTable()
	table.Columns = append(table.Columns, schema.Column{
		Name:         "external_id",
		Type:         "uuid",
		DeclaredType: &schema.DeclaredType{Base: "uuid"},
	})
	transaction.insert.execAffected = []int64{1}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"tenant_id", "id", "payload", "external_id"},
		"drop_recreate",
		[][]any{{
			int64(7),
			int64(1),
			"payload",
			"11111111-2222-3333-4444-555555555555",
		}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if len(transaction.prepared) != 1 ||
		strings.HasPrefix(transaction.prepared[0], "INSERTBULK ") {
		t.Fatalf("UUID path prepared %#v", transaction.prepared)
	}
	if transaction.bulk.doneCalls != 0 {
		t.Fatalf(
			"UUID fallback unexpectedly completed bulk copy %d times",
			transaction.bulk.doneCalls,
		)
	}
}

func TestSQLServerNativeWriterIdentityFailureCleansOrDiscardsSession(
	t *testing.T,
) {
	writer, _, connection, transaction := newSQLServerNativeTestWriter()
	table := sqlServerNativeIdentityTestTable()
	rowFailure := errors.New("driver exposed secret-row-value")
	cleanupFailure := errors.New("driver exposed secret-dsn")
	transaction.insert.execErr = rowFailure
	connection.execErr = cleanupFailure

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "secret-row-value"}},
	)
	if !errors.Is(err, rowFailure) || !errors.Is(err, cleanupFailure) {
		t.Fatalf("error = %v, want row and cleanup failures", err)
	}
	if strings.Contains(err.Error(), "secret-row-value") ||
		strings.Contains(err.Error(), "secret-dsn") {
		t.Fatalf("safe error exposed sensitive driver text: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitUnknown, 1, 0)
	if transaction.rollbacks != 1 || !connection.discarded {
		t.Fatalf(
			"rollback=%d discarded=%t",
			transaction.rollbacks,
			connection.discarded,
		)
	}
	if got, want := connection.cleanupStatements,
		[]string{
			"SET IDENTITY_INSERT [Target]]Schema].[event]]data] OFF",
		}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cleanup statements = %#v, want %#v", got, want)
	}
}

func TestSQLServerNativeWriterAmbiguousIdentityOnAlwaysCleansOrDiscards(
	t *testing.T,
) {
	table := sqlServerNativeIdentityTestTable()
	on := sqlServerIdentityInsertStatement(table, true)
	off := sqlServerIdentityInsertStatement(table, false)
	onFailure := errors.New("driver exposed ambiguous identity response")

	t.Run("cleanup succeeds", func(t *testing.T) {
		writer, _, connection, transaction :=
			newSQLServerNativeTestWriter()
		transaction.execErrors = map[string]error{on: onFailure}

		receipt, err := writer.WriteBatch(
			context.Background(),
			table,
			[]string{"id", "payload"},
			"drop_recreate",
			[][]any{{int64(1), "payload"}},
		)
		if !errors.Is(err, onFailure) {
			t.Fatalf("error = %v, want IDENTITY_INSERT ON failure", err)
		}
		if strings.Contains(err.Error(), "ambiguous identity response") {
			t.Fatalf("safe error exposed driver text: %v", err)
		}
		assertSQLServerNativeReceipt(
			t,
			receipt,
			CommitNotCommitted,
			1,
			0,
		)
		if transaction.rollbacks != 1 ||
			connection.discarded ||
			len(transaction.prepared) != 0 {
			t.Fatalf(
				"rollback=%d discarded=%t prepared=%#v",
				transaction.rollbacks,
				connection.discarded,
				transaction.prepared,
			)
		}
		if got, want := connection.cleanupStatements,
			[]string{off}; !reflect.DeepEqual(got, want) {
			t.Fatalf("cleanup statements = %#v, want %#v", got, want)
		}
	})

	t.Run("cleanup failure discards", func(t *testing.T) {
		writer, _, connection, transaction :=
			newSQLServerNativeTestWriter()
		transaction.execErrors = map[string]error{on: onFailure}
		cleanupFailure := errors.New("driver exposed cleanup failure")
		connection.execErr = cleanupFailure

		receipt, err := writer.WriteBatch(
			context.Background(),
			table,
			[]string{"id", "payload"},
			"drop_recreate",
			[][]any{{int64(1), "payload"}},
		)
		if !errors.Is(err, onFailure) ||
			!errors.Is(err, cleanupFailure) {
			t.Fatalf("error = %v, want ON and cleanup failures", err)
		}
		assertSQLServerNativeReceipt(
			t,
			receipt,
			CommitNotCommitted,
			1,
			0,
		)
		if !connection.discarded {
			t.Fatal("ambiguous IDENTITY_INSERT session was not discarded")
		}
		if got, want := connection.cleanupStatements,
			[]string{off}; !reflect.DeepEqual(got, want) {
			t.Fatalf("cleanup statements = %#v, want %#v", got, want)
		}
	})
}

func TestSQLServerNativeWriterRollbackFailureDiscardsConnection(
	t *testing.T,
) {
	writer, _, connection, transaction := newSQLServerNativeTestWriter()
	primaryFailure := errors.New("driver exposed write failure")
	rollbackFailure := errors.New("driver exposed rollback failure")
	transaction.bulk.doneErr = primaryFailure
	transaction.rollbackErr = rollbackFailure

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if !errors.Is(err, primaryFailure) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("error = %v, want write and rollback failures", err)
	}
	if strings.Contains(err.Error(), "write failure") ||
		strings.Contains(err.Error(), "rollback failure") {
		t.Fatalf("safe error exposed driver text: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if !connection.discarded {
		t.Fatal("connection with failed rollback was not discarded")
	}
}

func TestSQLServerNativeWriterSuppressesOnlyStructuredXACTAbortRollback(
	t *testing.T,
) {
	structured := mssql.Error{
		Number:  sqlServerRollbackWithoutBeginErrorNumber,
		Message: "ROLLBACK TRANSACTION has no corresponding BEGIN",
	}
	if !sqlServerRollbackAlreadyCompletedByXACTAbort(structured) {
		t.Fatal("structured SQL Server 3903 was not recognized")
	}
	if sqlServerRollbackAlreadyCompletedByXACTAbort(
		errors.New(structured.Message),
	) {
		t.Fatal("unstructured rollback text was accepted")
	}
	if sqlServerRollbackAlreadyCompletedByXACTAbort(
		mssql.Error{Number: 1205, Message: structured.Message},
	) {
		t.Fatal("wrong SQL Server error number was accepted")
	}

	writer, _, connection, transaction := newSQLServerNativeTestWriter()
	primaryFailure := errors.New("driver exposed write failure")
	transaction.bulk.doneErr = primaryFailure
	transaction.rollbackErr = structured
	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if !errors.Is(err, primaryFailure) {
		t.Fatalf("error = %v, want write failure", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if connection.discarded {
		t.Fatal("known XACT_ABORT-completed connection was discarded")
	}
	if strings.Contains(err.Error(), "roll back SQL Server native write") {
		t.Fatalf("known XACT_ABORT rollback error was joined: %v", err)
	}
}

func TestSQLServerNativeWriterCompositeUpsertLocksThenInserts(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	transaction.match.execAffected = []int64{1, 0}
	transaction.insert.execAffected = []int64{1}
	rows := [][]any{
		{"updated", int64(7), int64(1)},
		{"inserted", int64(8), int64(2)},
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"payload", "tenant_id", "id"},
		"upsert",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if got, want := transaction.match.rows,
		[][]any{
			{"updated", int64(7), int64(1)},
			{"inserted", int64(8), int64(2)},
		}; !reflect.DeepEqual(got, want) {
		t.Fatalf("update arguments = %#v, want %#v", got, want)
	}
	if got, want := transaction.insert.rows,
		[][]any{rows[1]}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert rows = %#v, want %#v", got, want)
	}
	if transaction.commits != 1 || transaction.rollbacks != 0 {
		t.Fatalf("transaction = %#v", transaction)
	}
	assertSQLServerEventBefore(
		t,
		transaction.events,
		"tx exec session guard",
		"prepare insert",
	)
	assertSQLServerEventBefore(
		t,
		transaction.events,
		"tx exec session guard",
		"prepare match",
	)
}

func TestSQLServerNativeWriterKeyOnlyUpsertProbesBeforeInsert(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	table := schema.Table{
		Schema: "dbo",
		Name:   "keys",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	transaction.match.queryResults = []bool{true, false}
	transaction.insert.execAffected = []int64{1}

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id"},
		"upsert",
		[][]any{{int64(1)}, {int64(2)}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if got, want := transaction.match.queryRows,
		[][]any{{int64(1)}, {int64(2)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("query arguments = %#v, want %#v", got, want)
	}
	if got, want := transaction.insert.rows,
		[][]any{{int64(2)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert rows = %#v, want %#v", got, want)
	}
	assertSQLServerEventBefore(
		t,
		transaction.events,
		"tx exec session guard",
		"prepare insert",
	)
	assertSQLServerEventBefore(
		t,
		transaction.events,
		"tx exec session guard",
		"prepare match",
	)
}

func TestSQLServerNativeWriterUpsertCollisionRollsBackWithoutMerge(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	transaction.match.execAffected = []int64{0}
	collision := errors.New("driver exposed conflicting-row-value")
	transaction.insert.execErr = collision

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"payload", "tenant_id", "id"},
		"upsert",
		[][]any{{"secret", int64(7), int64(1)}},
	)
	if !errors.Is(err, collision) {
		t.Fatalf("error = %v, want collision", err)
	}
	if strings.Contains(err.Error(), "conflicting-row-value") {
		t.Fatalf("safe error exposed driver text: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.commits != 0 || transaction.rollbacks != 1 {
		t.Fatalf("transaction = %#v", transaction)
	}
	for _, statement := range transaction.prepared {
		if strings.Contains(strings.ToUpper(statement), "MERGE") {
			t.Fatalf("prepared MERGE statement: %q", statement)
		}
	}
}

func TestSQLServerNativeWriterReportsUnknownCommitWithoutDriverText(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	transaction.bulk.execAffected = []int64{0}
	transaction.bulk.doneAffected = 1
	commitFailure := errors.New("driver exposed secret-dsn")
	transaction.commitErr = commitFailure

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("error = %v, want commit failure", err)
	}
	if strings.Contains(err.Error(), "secret-dsn") {
		t.Fatalf("safe error exposed driver text: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitUnknown, 1, 0)
	if transaction.commits != 1 || transaction.rollbacks != 1 {
		t.Fatalf("transaction = %#v", transaction)
	}
}

func TestSQLServerNativeWriterUnknownIdentityCommitRepeatsSessionCleanup(
	t *testing.T,
) {
	writer, _, connection, transaction := newSQLServerNativeTestWriter()
	table := sqlServerNativeIdentityTestTable()
	transaction.insert.execAffected = []int64{1}
	commitFailure := errors.New("ambiguous commit")
	transaction.commitErr = commitFailure

	receipt, err := writer.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "payload"}},
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("error = %v, want commit failure", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitUnknown, 1, 0)
	if transaction.rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", transaction.rollbacks)
	}
	if got, want := connection.cleanupStatements,
		[]string{
			"SET IDENTITY_INSERT [Target]]Schema].[event]]data] OFF",
		}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cleanup statements = %#v, want %#v", got, want)
	}
}

func TestSQLServerNativeWriterAcquisitionFailuresAreNotCommittedAndRedacted(
	t *testing.T,
) {
	table := sqlServerNativeTestTable()
	columns := []string{"tenant_id", "id", "payload"}
	rows := [][]any{{int64(7), int64(1), "payload"}}
	secretFailure := errors.New("driver exposed secret-dsn")

	t.Run("provider", func(t *testing.T) {
		writer, provider, _, transaction :=
			newSQLServerNativeTestWriter()
		provider.err = secretFailure
		receipt, err := writer.WriteBatch(
			context.Background(),
			table,
			columns,
			"drop_recreate",
			rows,
		)
		if !errors.Is(err, secretFailure) {
			t.Fatalf("error = %v, want provider failure", err)
		}
		if strings.Contains(err.Error(), "secret-dsn") {
			t.Fatalf("safe error exposed driver text: %v", err)
		}
		assertSQLServerNativeReceipt(
			t,
			receipt,
			CommitNotCommitted,
			1,
			0,
		)
		if len(transaction.events) != 0 {
			t.Fatalf("transaction events = %#v", transaction.events)
		}
	})

	t.Run("begin", func(t *testing.T) {
		writer, _, connection, transaction :=
			newSQLServerNativeTestWriter()
		connection.beginErr = secretFailure
		receipt, err := writer.WriteBatch(
			context.Background(),
			table,
			columns,
			"drop_recreate",
			rows,
		)
		if !errors.Is(err, secretFailure) {
			t.Fatalf("error = %v, want begin failure", err)
		}
		if strings.Contains(err.Error(), "secret-dsn") {
			t.Fatalf("safe error exposed driver text: %v", err)
		}
		assertSQLServerNativeReceipt(
			t,
			receipt,
			CommitNotCommitted,
			1,
			0,
		)
		assertSQLServerEvents(t, transaction.events, "begin serializable")
	})
}

func TestSQLServerNativeWriterSessionGuardFailureIsNotCommittedAndRedacted(
	t *testing.T,
) {
	writer, _, _, transaction := newSQLServerNativeTestWriter()
	guardFailure := errors.New("driver exposed secret-session-value")
	transaction.execErr = guardFailure

	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if !errors.Is(err, guardFailure) {
		t.Fatalf("error = %v, want session guard failure", err)
	}
	if strings.Contains(err.Error(), "secret-session-value") {
		t.Fatalf("safe error exposed driver text: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if len(transaction.prepared) != 0 ||
		transaction.commits != 0 ||
		transaction.rollbacks != 1 {
		t.Fatalf("transaction = %#v", transaction)
	}
	assertSQLServerEvents(
		t,
		transaction.events,
		"begin serializable",
		"tx exec session guard",
		"rollback",
	)
}

func TestSQLServerNativeWriterValidatesBeforeConnection(t *testing.T) {
	writer, provider, _, _ := newSQLServerNativeTestWriter()
	receipt, err := writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		[][]any{{int64(7), int64(1)}},
	)
	if err == nil || !strings.Contains(err.Error(), "has 2 values") {
		t.Fatalf("error = %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if provider.calls != 0 {
		t.Fatalf("connection provider called before row validation")
	}

	receipt, err = writer.WriteBatch(
		context.Background(),
		sqlServerNativeTestTable(),
		[]string{"tenant_id", "id", "payload"},
		"drop_recreate",
		nil,
	)
	if err != nil {
		t.Fatalf("zero WriteBatch: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 0, 0)
	if provider.calls != 0 {
		t.Fatalf("connection provider called for zero batch")
	}
}

func sqlServerNativeTestTable() schema.Table {
	return schema.Table{
		Schema: "Target]Schema",
		Name:   "event]data",
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{Name: "payload", Type: "text"},
		},
	}
}

func sqlServerNativeIdentityTestTable() schema.Table {
	return schema.Table{
		Schema: "Target]Schema",
		Name:   "event]data",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
		},
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

func assertSQLServerNativeReceipt(
	t *testing.T,
	receipt WriteReceipt,
	certainty CommitCertainty,
	attempted int64,
	committed int64,
) {
	t.Helper()
	if receipt.Certainty != certainty ||
		receipt.AttemptedRows != attempted ||
		receipt.CommittedRows != committed ||
		receipt.AttemptOffset != 0 {
		t.Fatalf(
			"receipt = %+v, want certainty=%s attempted=%d committed=%d",
			receipt,
			certainty,
			attempted,
			committed,
		)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("invalid receipt: %v", err)
	}
}

type sqlServerNativeTestProvider struct {
	connection *sqlServerNativeTestConnection
	calls      int
	err        error
}

func (provider *sqlServerNativeTestProvider) WithConnection(
	_ context.Context,
	operation func(sqlServerNativeConnection) error,
) error {
	provider.calls++
	if provider.err != nil {
		return provider.err
	}
	return operation(provider.connection)
}

type sqlServerNativeTestConnection struct {
	transaction       *sqlServerNativeTestTransaction
	beginCalls        int
	beginErr          error
	execErr           error
	cleanupStatements []string
	discarded         bool
}

func (connection *sqlServerNativeTestConnection) BeginSerializable(
	_ context.Context,
) (sqlServerNativeTransaction, error) {
	connection.beginCalls++
	connection.transaction.events = append(
		connection.transaction.events,
		"begin serializable",
	)
	if connection.beginErr != nil {
		return nil, connection.beginErr
	}
	return connection.transaction, nil
}

func (connection *sqlServerNativeTestConnection) Exec(
	_ context.Context,
	statement string,
) (int64, error) {
	connection.cleanupStatements = append(
		connection.cleanupStatements,
		statement,
	)
	if connection.execErr != nil {
		return 0, connection.execErr
	}
	return 0, nil
}

func (connection *sqlServerNativeTestConnection) Discard() {
	connection.discarded = true
}

type sqlServerNativeTestTransaction struct {
	events      []string
	prepared    []string
	bulk        *sqlServerNativeTestStatement
	insert      *sqlServerNativeTestStatement
	match       *sqlServerNativeTestStatement
	execErr     error
	execErrors  map[string]error
	commitErr   error
	rollbackErr error
	commits     int
	rollbacks   int
}

func (transaction *sqlServerNativeTestTransaction) Prepare(
	_ context.Context,
	statement string,
) (sqlServerNativeStatement, error) {
	transaction.prepared = append(transaction.prepared, statement)
	var selected *sqlServerNativeTestStatement
	switch {
	case strings.HasPrefix(statement, "INSERTBULK "):
		selected = transaction.bulk
	case strings.HasPrefix(statement, "INSERT INTO "):
		selected = transaction.insert
	case strings.HasPrefix(statement, "UPDATE "),
		strings.HasPrefix(statement, "SELECT "):
		selected = transaction.match
	default:
		return nil, errors.New("unexpected test statement")
	}
	transaction.events = append(
		transaction.events,
		"prepare "+selected.label,
	)
	selected.sql = statement
	return selected, nil
}

func (transaction *sqlServerNativeTestTransaction) Exec(
	_ context.Context,
	statement string,
) (int64, error) {
	event := "tx exec " + statement
	if statement == sqlServerNativeSessionGuardStatement {
		event = "tx exec session guard"
	}
	transaction.events = append(
		transaction.events,
		event,
	)
	if err := transaction.execErrors[statement]; err != nil {
		return 0, err
	}
	if transaction.execErr != nil {
		return 0, transaction.execErr
	}
	return 0, nil
}

func (transaction *sqlServerNativeTestTransaction) Commit() error {
	transaction.commits++
	transaction.events = append(transaction.events, "commit")
	return transaction.commitErr
}

func (transaction *sqlServerNativeTestTransaction) Rollback() error {
	transaction.rollbacks++
	transaction.events = append(transaction.events, "rollback")
	return transaction.rollbackErr
}

type sqlServerNativeTestStatement struct {
	parent       *sqlServerNativeTestTransaction
	label        string
	sql          string
	rows         [][]any
	queryRows    [][]any
	execAffected []int64
	execErr      error
	execCalls    int
	queryResults []bool
	queryErr     error
	queryCalls   int
	doneAffected int64
	doneErr      error
	doneCalls    int
	closeErr     error
	closeCalls   int
	closed       bool
}

func (statement *sqlServerNativeTestStatement) Exec(
	_ context.Context,
	values []any,
) (int64, error) {
	statement.parent.events = append(
		statement.parent.events,
		statement.label+" exec",
	)
	statement.rows = append(
		statement.rows,
		append([]any(nil), values...),
	)
	index := statement.execCalls
	statement.execCalls++
	if statement.execErr != nil {
		return 0, statement.execErr
	}
	if index >= len(statement.execAffected) {
		return 0, nil
	}
	return statement.execAffected[index], nil
}

func (statement *sqlServerNativeTestStatement) QueryBool(
	_ context.Context,
	values []any,
) (bool, error) {
	statement.parent.events = append(
		statement.parent.events,
		statement.label+" query",
	)
	statement.queryRows = append(
		statement.queryRows,
		append([]any(nil), values...),
	)
	index := statement.queryCalls
	statement.queryCalls++
	if statement.queryErr != nil {
		return false, statement.queryErr
	}
	if index >= len(statement.queryResults) {
		return false, nil
	}
	return statement.queryResults[index], nil
}

func (statement *sqlServerNativeTestStatement) Done(
	_ context.Context,
) (int64, error) {
	statement.parent.events = append(
		statement.parent.events,
		statement.label+" done",
	)
	statement.doneCalls++
	return statement.doneAffected, statement.doneErr
}

func (statement *sqlServerNativeTestStatement) Close() error {
	statement.parent.events = append(
		statement.parent.events,
		statement.label+" close",
	)
	statement.closeCalls++
	statement.closed = true
	return statement.closeErr
}

func newSQLServerNativeTestWriter() (
	*sqlServerNativeWriter,
	*sqlServerNativeTestProvider,
	*sqlServerNativeTestConnection,
	*sqlServerNativeTestTransaction,
) {
	transaction := &sqlServerNativeTestTransaction{}
	transaction.bulk = &sqlServerNativeTestStatement{
		parent: transaction,
		label:  "bulk",
	}
	transaction.insert = &sqlServerNativeTestStatement{
		parent: transaction,
		label:  "insert",
	}
	transaction.match = &sqlServerNativeTestStatement{
		parent: transaction,
		label:  "match",
	}
	connection := &sqlServerNativeTestConnection{
		transaction: transaction,
	}
	provider := &sqlServerNativeTestProvider{
		connection: connection,
	}
	return &sqlServerNativeWriter{connections: provider},
		provider,
		connection,
		transaction
}

func assertSQLServerEvents(
	t *testing.T,
	got []string,
	want ...string,
) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
}

func assertSQLServerEventBefore(
	t *testing.T,
	events []string,
	first string,
	second string,
) {
	t.Helper()
	firstPosition, secondPosition := -1, -1
	for index, event := range events {
		if event == first && firstPosition < 0 {
			firstPosition = index
		}
		if event == second && secondPosition < 0 {
			secondPosition = index
		}
	}
	if firstPosition < 0 ||
		secondPosition < 0 ||
		firstPosition >= secondPosition {
		t.Fatalf(
			"event %q does not precede %q in %#v",
			first,
			second,
			events,
		)
	}
}
