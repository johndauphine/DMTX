package migrate

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/engine"
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
	const want = "INSERT INTO `target_db`.`events` (`id`, `payload`) " +
		"VALUES (?, ?) AS `dmtx_new` ON DUPLICATE KEY UPDATE " +
		"`id` = IF(`events`.`id` <=> `dmtx_new`.`id`, " +
		"`events`.`id`, JSON_EXTRACT('dmtx-invalid-json', '$')), " +
		"`payload` = `dmtx_new`.`payload`"
	if statement != want {
		t.Fatalf("Oracle MySQL statement = %q, want %q", statement, want)
	}
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

func TestMySQLNativeWriteStatementForMariaDBUsesGuardedValuesUpsert(
	t *testing.T,
) {
	table := mysqlNativeTestTable()
	statement, err := mySQLNativeWriteStatementForFlavor(
		table,
		[]string{"id", "payload"},
		"upsert",
		engine.MySQLServerFlavorMariaDB1011,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = "INSERT INTO `target_db`.`events` (`id`, `payload`) " +
		"VALUES (?, ?) ON DUPLICATE KEY UPDATE " +
		"`id` = IF(`events`.`id` <=> VALUES(`id`), " +
		"`events`.`id`, JSON_EXTRACT('dmtx-invalid-json', '$')), " +
		"`payload` = VALUES(`payload`)"
	if statement != want {
		t.Fatalf("MariaDB statement = %q, want %q", statement, want)
	}
	if strings.Contains(statement, " AS `dmtx_new`") {
		t.Fatalf("MariaDB statement uses unsupported row alias: %q", statement)
	}
}

func TestMySQLNativeWriteStatementForMariaDBGuardsCompletePrimaryKey(
	t *testing.T,
) {
	table := mysqlNativeTestTable()
	table.Columns = append(
		[]schema.Column{{
			Name:               "tenant_id",
			Type:               "integer",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		table.Columns...,
	)
	table.Columns[1].PrimaryKeyPosition = 2
	statement, err := mySQLNativeWriteStatementForFlavor(
		table,
		[]string{"tenant_id", "id", "payload"},
		"upsert",
		engine.MySQLServerFlavorMariaDB1011,
	)
	if err != nil {
		t.Fatal(err)
	}
	const guard = "`events`.`tenant_id` <=> VALUES(`tenant_id`) AND " +
		"`events`.`id` <=> VALUES(`id`)"
	if !strings.Contains(statement, guard) {
		t.Fatalf(
			"MariaDB composite-key statement %q does not contain %q",
			statement,
			guard,
		)
	}
}

func TestMySQLNativeWriterUsesConfiguredMariaDBFlavor(t *testing.T) {
	writer, transaction := newMySQLNativeTestWriterForFlavor(
		engine.MySQLServerFlavorMariaDB1011,
	)
	transaction.statement.affected = []int64{2}
	transaction.warnings = []int64{0}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"upsert",
		[][]any{{int64(1), "updated"}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if !strings.Contains(
		transaction.statement.query,
		"`payload` = VALUES(`payload`)",
	) || strings.Contains(
		transaction.statement.query,
		" AS `dmtx_new`",
	) {
		t.Fatalf(
			"writer prepared wrong MariaDB statement: %q",
			transaction.statement.query,
		)
	}
}

func TestMySQLNativeWriterRejectsUnknownFlavorBeforeTransaction(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeTestWriterForFlavor(
		engine.MySQLServerFlavorUnknown,
	)
	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"unsupported target server flavor",
	) {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.begins != 0 {
		t.Fatal("transaction began for an unsupported target flavor")
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

func TestMySQLLocalInfileStatementUsesHexVariables(t *testing.T) {
	statement, err := mySQLLocalInfileStatement(
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"dmtx_test_reader",
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = "LOAD DATA LOCAL INFILE 'Reader::dmtx_test_reader' " +
		"INTO TABLE `target_db`.`events` CHARACTER SET binary " +
		"FIELDS TERMINATED BY '\\t' ESCAPED BY '' " +
		"LINES TERMINATED BY '\\n' (@dmtx_0, @dmtx_1) SET " +
		"`id` = IF(@dmtx_0 = 'N', NULL, " +
		"UNHEX(SUBSTRING(@dmtx_0, 2))), " +
		"`payload` = IF(@dmtx_1 = 'N', NULL, " +
		"UNHEX(SUBSTRING(@dmtx_1, 2)))"
	if statement != want {
		t.Fatalf("statement = %q, want %q", statement, want)
	}
	if _, err := mySQLLocalInfileStatement(
		mysqlNativeTestTable(),
		[]string{"id"},
		"unsafe'name",
	); err == nil {
		t.Fatal("unsafe reader name was accepted")
	}
}

func TestMySQLLocalInfileReaderPreservesExactValueBoundaries(t *testing.T) {
	rows, err := normalizeMySQLLocalInfileRows([][]any{
		{
			nil,
			"",
			[]byte(nil),
			[]byte{0x00, '\t', '\n', 0xff},
			int64(-12),
			float64(1.25),
			true,
			time.Date(
				2026,
				time.July,
				30,
				12,
				34,
				56,
				123456000,
				time.FixedZone("offset", -5*60*60),
			),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(&mysqlLocalInfileReader{rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	const want = "N\tH\tN\tH00090aff\tH2d3132\tH312e3235\tH31\t" +
		"H323032362d30372d33302031373a33343a35362e313233343536\n"
	if string(payload) != want {
		t.Fatalf("payload = %q, want %q", payload, want)
	}
}

func TestMySQLLocalInfileReaderPreservesUnsignedBigint(t *testing.T) {
	rows, err := normalizeMySQLLocalInfileRows(
		[][]any{{^uint64(0)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(&mysqlLocalInfileReader{rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	const want = "H3138343436373434303733373039353531363135\n"
	if string(payload) != want {
		t.Fatalf("payload = %q, want %q", payload, want)
	}
}

func TestMySQLNativeWriterUsesProvenLocalInfile(t *testing.T) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	transaction.localInfileEnabled = []bool{true}
	transaction.executeAffected = []int64{0, 2, 0}
	transaction.counts = []int64{2}
	transaction.bulkAffected = []int64{2}
	transaction.warnings = []int64{0, 0}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{
			{int64(1), "one"},
			{int64(2), []byte{0, '\n'}},
		},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if transaction.begins != 1 ||
		transaction.rollbacks != 0 ||
		transaction.commits != 1 ||
		transaction.localInfileChecks != 1 ||
		transaction.warningCalls != 2 {
		t.Fatalf("transaction = %#v", transaction)
	}
	if len(transaction.bulkPayloads) != 1 ||
		string(transaction.bulkPayloads[0]) !=
			"H31\tH6f6e65\nH32\tH000a\n" {
		t.Fatalf("bulk payloads = %q", transaction.bulkPayloads)
	}
	if len(transaction.executeStatements) != 3 ||
		!strings.HasPrefix(
			transaction.executeStatements[0],
			"CREATE TEMPORARY TABLE `dmtx_load_",
		) ||
		!strings.HasPrefix(
			transaction.executeStatements[1],
			"INSERT INTO `target_db`.`events`",
		) ||
		!strings.HasPrefix(
			transaction.executeStatements[2],
			"DROP TEMPORARY TABLE IF EXISTS `dmtx_load_",
		) {
		t.Fatalf(
			"bulk statements = %#v",
			transaction.executeStatements,
		)
	}
}

func TestMySQLNativeWriterFallsBackOnceWhenLocalInfileDisabled(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	transaction.localInfileEnabled = []bool{false}
	transaction.statement.affected = []int64{1, 1}
	transaction.warnings = []int64{0, 0}

	for id := int64(1); id <= 2; id++ {
		receipt, err := writer.WriteBatch(
			context.Background(),
			mysqlNativeTestTable(),
			[]string{"id", "payload"},
			"drop_recreate",
			[][]any{{id, "value"}},
		)
		if err != nil {
			t.Fatalf("WriteBatch %d: %v", id, err)
		}
		assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	}
	if transaction.localInfileChecks != 1 ||
		len(transaction.bulkPayloads) != 0 ||
		transaction.begins != 3 {
		t.Fatalf("transaction = %#v", transaction)
	}
	if len(warnings) != 1 ||
		warnings[0] != mysqlLocalInfileFallbackWarning {
		t.Fatalf("fallback warnings = %#v", warnings)
	}
}

func TestMySQLNativeWriterRejectsIneligibleBulkValueBeforeTransaction(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), complex(1, 2)}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"unsupported value type",
	) {
		t.Fatalf("WriteBatch error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if transaction.begins != 0 ||
		transaction.localInfileChecks != 0 ||
		len(transaction.bulkPayloads) != 0 ||
		len(warnings) != 0 {
		t.Fatalf(
			"transaction = %#v, warnings = %#v",
			transaction,
			warnings,
		)
	}
}

func TestMySQLNativeWriterFallsBackAfterUnavailableBulkCommand(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	writer.localInfile = mysqlLocalInfileEnabled
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	transaction.localInfileEnabled = []bool{true}
	transaction.executeAffected = []int64{0, 0}
	transaction.bulkErrs = []error{&mysqlDriver.MySQLError{Number: 3948}}
	transaction.statement.affected = []int64{1}
	transaction.warnings = []int64{0}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if transaction.rollbacks != 1 ||
		transaction.commits != 1 ||
		transaction.begins != 2 ||
		len(warnings) != 1 {
		t.Fatalf(
			"transaction = %#v, warnings = %#v",
			transaction,
			warnings,
		)
	}
}

func TestMySQLNativeWriterCleansAmbiguousStagingCreateOutcome(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	createErr := errors.New("ambiguous create outcome")
	transaction.localInfileEnabled = []bool{true}
	transaction.executeErrs = []error{createErr, nil}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if !errors.Is(err, createErr) {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if len(transaction.executeStatements) != 2 {
		t.Fatalf(
			"staging statements = %#v",
			transaction.executeStatements,
		)
	}
	created := strings.TrimPrefix(
		transaction.executeStatements[0],
		"CREATE TEMPORARY TABLE ",
	)
	created = strings.SplitN(created, " LIKE ", 2)[0]
	if transaction.executeStatements[1] !=
		"DROP TEMPORARY TABLE IF EXISTS "+created {
		t.Fatalf(
			"staging cleanup = %q, want target %q",
			transaction.executeStatements[1],
			created,
		)
	}
	if transaction.rollbacks != 1 ||
		transaction.commits != 0 ||
		writer.localInfile == mysqlLocalInfileFallback {
		t.Fatalf("transaction = %#v", transaction)
	}
}

func TestMySQLNativeWriterRejectsLossyLocalInfileResult(t *testing.T) {
	tests := []struct {
		name            string
		loaded          int64
		loadWarnings    int64
		staged          int64
		merged          int64
		mergeWarnings   int64
		want            string
		wantMergeCalled bool
	}{
		{
			name:   "load missing row",
			loaded: 1,
			want:   "expected exactly 2",
		},
		{
			name:         "load conversion warning",
			loaded:       2,
			loadWarnings: 1,
			want:         "conversion warnings",
		},
		{
			name:         "staging count mismatch",
			loaded:       2,
			loadWarnings: 0,
			staged:       1,
			want:         "staging contains 1 rows",
		},
		{
			name:            "merge missing row",
			loaded:          2,
			loadWarnings:    0,
			staged:          2,
			merged:          1,
			want:            "merge affected 1 rows",
			wantMergeCalled: true,
		},
		{
			name:            "merge conversion warning",
			loaded:          2,
			loadWarnings:    0,
			staged:          2,
			merged:          2,
			mergeWarnings:   1,
			want:            "conversion warnings",
			wantMergeCalled: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, transaction := newMySQLNativeBulkTestWriter()
			writer.localInfile = mysqlLocalInfileEnabled
			transaction.localInfileEnabled = []bool{true}
			transaction.bulkAffected = []int64{test.loaded}
			transaction.warnings = []int64{test.loadWarnings}
			transaction.executeAffected = []int64{0}
			if test.loaded == 2 && test.loadWarnings == 0 {
				transaction.counts = []int64{test.staged}
			}
			if test.wantMergeCalled {
				transaction.executeAffected = append(
					transaction.executeAffected,
					test.merged,
				)
				transaction.warnings = append(
					transaction.warnings,
					test.mergeWarnings,
				)
			}
			transaction.executeAffected = append(
				transaction.executeAffected,
				0,
			)

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
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			assertMySQLNativeReceipt(
				t,
				receipt,
				CommitNotCommitted,
				2,
				0,
			)
			if transaction.rollbacks != 1 ||
				transaction.commits != 0 {
				t.Fatalf("transaction = %#v", transaction)
			}
			mergeCalled := false
			for _, statement := range transaction.executeStatements {
				if strings.HasPrefix(
					statement,
					"INSERT INTO `target_db`.`events`",
				) {
					mergeCalled = true
				}
			}
			if mergeCalled != test.wantMergeCalled {
				t.Fatalf(
					"merge called = %t, want %t; statements = %#v",
					mergeCalled,
					test.wantMergeCalled,
					transaction.executeStatements,
				)
			}
		})
	}
}

func TestMySQLNativeWriterDoesNotFallbackWhenBulkCleanupIsUncertain(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	writer.localInfile = mysqlLocalInfileEnabled
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	transaction.localInfileEnabled = []bool{true}
	transaction.executeAffected = []int64{0, 0}
	transaction.bulkErrs = []error{&mysqlDriver.MySQLError{Number: 3948}}
	transaction.rollbackErr = errors.New("rollback outcome unknown")

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if err == nil || !errors.Is(err, transaction.rollbackErr) {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if len(warnings) != 0 || writer.localInfile == mysqlLocalInfileFallback {
		t.Fatalf(
			"uncertain cleanup fallback state=%d warnings=%#v",
			writer.localInfile,
			warnings,
		)
	}
}

func TestMySQLNativeWriterReportsUnknownBulkCommitOutcome(t *testing.T) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	writer.localInfile = mysqlLocalInfileEnabled
	transaction.localInfileEnabled = []bool{true}
	transaction.executeAffected = []int64{0, 1, 0}
	transaction.bulkAffected = []int64{1}
	transaction.warnings = []int64{0, 0}
	transaction.counts = []int64{1}
	transaction.commitErr = errors.New("commit outcome unknown")

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "value"}},
	)
	if !errors.Is(err, transaction.commitErr) {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitUnknown, 1, 0)
}

func TestMySQLNativeWriterKeepsUpsertOnGuardedInsertPath(t *testing.T) {
	writer, transaction := newMySQLNativeBulkTestWriter()
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	transaction.statement.affected = []int64{2}
	transaction.warnings = []int64{0}

	receipt, err := writer.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"upsert",
		[][]any{{int64(1), "updated"}},
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if transaction.localInfileChecks != 0 ||
		len(transaction.bulkPayloads) != 0 ||
		!strings.Contains(
			transaction.statement.query,
			"ON DUPLICATE KEY UPDATE",
		) ||
		len(warnings) != 1 ||
		warnings[0] != mysqlLocalInfileUpsertFallbackWarning {
		t.Fatalf(
			"transaction = %#v, warnings = %#v",
			transaction,
			warnings,
		)
	}
}

func TestMySQLNativeWriterStage4NetworkFencesAndProvesBeforeStrictUpsert(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name   string
		flavor engine.MySQLServerFlavor
		alias  string
	}{
		{
			name:   "Oracle MySQL 8",
			flavor: engine.MySQLServerFlavorOracle80,
			alias:  "AS `dmtx_new` ON DUPLICATE KEY UPDATE",
		},
		{
			name:   "MariaDB 10.11",
			flavor: engine.MySQLServerFlavorMariaDB1011,
			alias:  "VALUES(`payload`)",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer, transaction :=
				newMySQLNativeTestWriterForFlavor(testCase.flavor)
			// A Stage 4 page must bypass both capability probing and
			// LOAD DATA even when the writer has not chosen a bulk mode.
			writer.localInfile = mysqlLocalInfileUnknown
			transaction.statement.affected = []int64{1}
			transaction.warnings = []int64{0}

			receipt, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				mysqlNativeTestTable(),
				[]string{"id", "payload"},
				[][]any{{int64(7), "safe"}},
			)
			if err != nil {
				t.Fatalf("WriteStage4NetworkBatch: %v", err)
			}
			assertMySQLNativeReceipt(
				t,
				receipt,
				CommitDurable,
				1,
				1,
			)
			wantEvents := []string{
				"begin",
				"acquire Stage 4 replay fence",
				"prove Stage 4 replay isolation",
				"prepare strict write",
				"inspect warnings",
				"commit",
			}
			if !reflect.DeepEqual(
				transaction.stage4Events,
				wantEvents,
			) {
				t.Fatalf(
					"events = %#v, want %#v",
					transaction.stage4Events,
					wantEvents,
				)
			}
			if transaction.stage4Flavor != testCase.flavor {
				t.Fatalf(
					"proof flavor = %q, want %q",
					transaction.stage4Flavor,
					testCase.flavor,
				)
			}
			if transaction.localInfileChecks != 0 ||
				len(transaction.bulkStatements) != 0 {
				t.Fatalf(
					"Stage 4 write used LOAD DATA path: checks=%d statements=%#v",
					transaction.localInfileChecks,
					transaction.bulkStatements,
				)
			}
			if !strings.Contains(
				transaction.statement.query,
				testCase.alias,
			) {
				t.Fatalf(
					"strict upsert %q lacks %q",
					transaction.statement.query,
					testCase.alias,
				)
			}
		})
	}
}

func TestMySQLNativeWriterStage4NetworkGuardFailuresRollBackBeforeWrite(
	t *testing.T,
) {
	fenceFailure := errors.New("fence failed with sensitive driver text")
	proofFailure := NewTransferError(
		ErrorClassPolicy,
		errors.New("unsafe incoming foreign key"),
	)
	for _, testCase := range []struct {
		name       string
		fenceErr   error
		proofErr   error
		want       string
		wantCause  error
		wantEvents []string
	}{
		{
			name:      "fence",
			fenceErr:  fenceFailure,
			want:      "acquire MySQL Stage 4 network replay fence for events",
			wantCause: fenceFailure,
			wantEvents: []string{
				"begin",
				"acquire Stage 4 replay fence",
				"rollback",
			},
		},
		{
			name:      "proof",
			proofErr:  proofFailure,
			want:      "unsafe incoming foreign key",
			wantCause: proofFailure,
			wantEvents: []string{
				"begin",
				"acquire Stage 4 replay fence",
				"prove Stage 4 replay isolation",
				"rollback",
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer, transaction := newMySQLNativeTestWriter()
			transaction.stage4FenceErr = testCase.fenceErr
			transaction.stage4ProofErr = testCase.proofErr

			receipt, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				mysqlNativeTestTable(),
				[]string{"id", "payload"},
				[][]any{{int64(7), "safe"}},
			)
			if err == nil ||
				!strings.Contains(err.Error(), testCase.want) ||
				!errors.Is(err, testCase.wantCause) {
				t.Fatalf("guard error = %v", err)
			}
			assertMySQLNativeReceipt(
				t,
				receipt,
				CommitNotCommitted,
				1,
				0,
			)
			if transaction.statement.query != "" {
				t.Fatalf(
					"guard failure prepared a write: %q",
					transaction.statement.query,
				)
			}
			if !reflect.DeepEqual(
				transaction.stage4Events,
				testCase.wantEvents,
			) {
				t.Fatalf(
					"events = %#v, want %#v",
					transaction.stage4Events,
					testCase.wantEvents,
				)
			}
		})
	}
}

func TestMySQLNativeWriterStage4NetworkRejectsBetweenPageRetainedShapeDriftBeforeDML(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name   string
		flavor engine.MySQLServerFlavor
		drift  string
	}{
		{
			name:   "Oracle MySQL timestamp on update",
			flavor: engine.MySQLServerFlavorOracle80,
			drift:  "column extra includes ON UPDATE CURRENT_TIMESTAMP",
		},
		{
			name:   "MariaDB system versioning",
			flavor: engine.MySQLServerFlavorMariaDB1011,
			drift:  "table options include SYSTEM VERSIONING",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer, transaction :=
				newMySQLNativeTestWriterForFlavor(testCase.flavor)
			transaction.statement.affected = []int64{1}
			transaction.warnings = []int64{0}
			table := mysqlNativeTestTable()

			first, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				table,
				[]string{"id", "payload"},
				[][]any{{int64(7), "first"}},
			)
			if err != nil {
				t.Fatalf("first page: %v", err)
			}
			assertMySQLNativeReceipt(
				t,
				first,
				CommitDurable,
				1,
				1,
			)
			writesAfterFirst := len(transaction.statement.rows)
			commitsAfterFirst := transaction.commits

			transaction.stage4ProofErr = NewTransferError(
				ErrorClassPolicy,
				errors.New(testCase.drift),
			)
			second, err := writer.WriteStage4NetworkBatch(
				context.Background(),
				table,
				[]string{"id", "payload"},
				[][]any{{int64(7), "must-not-apply"}},
			)
			if err == nil || !strings.Contains(
				err.Error(),
				testCase.drift,
			) {
				t.Fatalf("between-page drift error = %v", err)
			}
			assertMySQLNativeReceipt(
				t,
				second,
				CommitNotCommitted,
				1,
				0,
			)
			if len(transaction.statement.rows) != writesAfterFirst ||
				transaction.commits != commitsAfterFirst {
				t.Fatalf(
					"rejected drift wrote rows or committed: rows=%d/%d commits=%d/%d",
					len(transaction.statement.rows),
					writesAfterFirst,
					transaction.commits,
					commitsAfterFirst,
				)
			}
			if transaction.rollbacks != 1 {
				t.Fatalf(
					"rejected drift rollbacks = %d, want 1",
					transaction.rollbacks,
				)
			}
		})
	}
}

func TestMySQLNativeWriterStage4NetworkCommitFailureIsUnknown(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeTestWriter()
	transaction.statement.affected = []int64{1}
	transaction.warnings = []int64{0}
	transaction.commitErr = errors.New("lost commit response")

	receipt, err := writer.WriteStage4NetworkBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		[][]any{{int64(7), "safe"}},
	)
	if err == nil {
		t.Fatal("commit failure was accepted")
	}
	assertMySQLNativeReceipt(t, receipt, CommitUnknown, 1, 0)
	if transaction.rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", transaction.rollbacks)
	}
}

func TestMySQLNativeWriterStage4RollbackFailurePreservesSafetyEvidence(
	t *testing.T,
) {
	t.Run("proof failure remains not committed", func(t *testing.T) {
		writer, transaction := newMySQLNativeTestWriter()
		proofFailure := NewTransferError(
			ErrorClassPolicy,
			errors.New("unsafe incoming foreign key"),
		)
		rollbackFailure := errors.New(
			"rollback failed with secret connection detail",
		)
		transaction.stage4ProofErr = proofFailure
		transaction.rollbackErr = rollbackFailure

		receipt, err := writer.WriteStage4NetworkBatch(
			context.Background(),
			mysqlNativeTestTable(),
			[]string{"id", "payload"},
			[][]any{{int64(7), "safe"}},
		)
		if !errors.Is(err, proofFailure) ||
			!errors.Is(err, rollbackFailure) ||
			strings.Contains(err.Error(), "secret connection detail") {
			t.Fatalf("joined cleanup error = %v", err)
		}
		assertMySQLNativeReceipt(
			t,
			receipt,
			CommitNotCommitted,
			1,
			0,
		)
	})

	t.Run("post-DML failure becomes unknown", func(t *testing.T) {
		writer, transaction := newMySQLNativeTestWriter()
		transaction.statement.affected = []int64{1}
		transaction.warnings = []int64{1}
		transaction.rollbackErr = errors.New("rollback failed")

		receipt, err := writer.WriteStage4NetworkBatch(
			context.Background(),
			mysqlNativeTestTable(),
			[]string{"id", "payload"},
			[][]any{{int64(7), "unsafe conversion"}},
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"conversion warnings",
		) || !errors.Is(err, transaction.rollbackErr) {
			t.Fatalf("post-DML cleanup error = %v", err)
		}
		assertMySQLNativeReceipt(
			t,
			receipt,
			CommitUnknown,
			1,
			0,
		)
	})
}

func TestMySQLNativeWriterStage4RollbackIgnoresCanceledWriteContext(
	t *testing.T,
) {
	writer, transaction := newMySQLNativeTestWriter()
	transaction.stage4ProofErr = errors.New("proof failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := writer.WriteStage4NetworkBatch(
		ctx,
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		[][]any{{int64(7), "safe"}},
	)
	if err == nil {
		t.Fatal("proof failure was accepted")
	}
	if transaction.stage4RollbackContextErr != nil ||
		!transaction.stage4RollbackHasDeadline {
		t.Fatalf(
			"rollback context err=%v deadline=%t",
			transaction.stage4RollbackContextErr,
			transaction.stage4RollbackHasDeadline,
		)
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
	provider.transaction.stage4Events = append(
		provider.transaction.stage4Events,
		"begin",
	)
	provider.transaction.begins++
	return provider.transaction, nil
}

func (provider *mysqlNativeTestProvider) BeginStage4Network(
	context.Context,
) (mysqlStage4NetworkBatchTransaction, error) {
	provider.transaction.stage4Events = append(
		provider.transaction.stage4Events,
		"begin",
	)
	provider.transaction.begins++
	return provider.transaction, nil
}

type mysqlNativeTestTransaction struct {
	begins                    int
	commits                   int
	rollbacks                 int
	warningCalls              int
	warnings                  []int64
	commitErr                 error
	statement                 *mysqlNativeTestStatement
	localInfileChecks         int
	localInfileEnabled        []bool
	localInfileErrs           []error
	bulkAffected              []int64
	bulkErrs                  []error
	bulkStatements            []string
	bulkPayloads              [][]byte
	executeAffected           []int64
	executeErrs               []error
	executeStatements         []string
	counts                    []int64
	countErrs                 []error
	countStatements           []string
	rollbackErr               error
	stage4Events              []string
	stage4FenceErr            error
	stage4ProofErr            error
	stage4Flavor              engine.MySQLServerFlavor
	stage4Table               schema.Table
	stage4RollbackContextErr  error
	stage4RollbackHasDeadline bool
}

func (transaction *mysqlNativeTestTransaction) Prepare(
	_ context.Context,
	statement string,
) (mysqlBatchStatement, error) {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"prepare strict write",
	)
	transaction.statement.query = statement
	return transaction.statement, nil
}

func (transaction *mysqlNativeTestTransaction) WarningCount(
	context.Context,
) (int64, error) {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"inspect warnings",
	)
	index := transaction.warningCalls
	transaction.warningCalls++
	return transaction.warnings[index], nil
}

func (transaction *mysqlNativeTestTransaction) LocalInfileEnabled(
	context.Context,
) (bool, error) {
	index := transaction.localInfileChecks
	transaction.localInfileChecks++
	var err error
	if index < len(transaction.localInfileErrs) {
		err = transaction.localInfileErrs[index]
	}
	if err != nil {
		return false, err
	}
	if index >= len(transaction.localInfileEnabled) {
		return false, nil
	}
	return transaction.localInfileEnabled[index], nil
}

func (transaction *mysqlNativeTestTransaction) Execute(
	_ context.Context,
	statement string,
) (int64, error) {
	index := len(transaction.executeStatements)
	transaction.executeStatements = append(
		transaction.executeStatements,
		statement,
	)
	if index < len(transaction.executeErrs) &&
		transaction.executeErrs[index] != nil {
		return 0, transaction.executeErrs[index]
	}
	if index >= len(transaction.executeAffected) {
		return 0, nil
	}
	return transaction.executeAffected[index], nil
}

func (transaction *mysqlNativeTestTransaction) Count(
	_ context.Context,
	statement string,
) (int64, error) {
	index := len(transaction.countStatements)
	transaction.countStatements = append(
		transaction.countStatements,
		statement,
	)
	if index < len(transaction.countErrs) &&
		transaction.countErrs[index] != nil {
		return 0, transaction.countErrs[index]
	}
	if index >= len(transaction.counts) {
		return 0, nil
	}
	return transaction.counts[index], nil
}

func (transaction *mysqlNativeTestTransaction) LoadLocalInfile(
	_ context.Context,
	request mysqlLocalInfileRequest,
) (int64, error) {
	index := len(transaction.bulkStatements)
	statement, err := request.statement("dmtx_test_reader")
	if err != nil {
		return 0, err
	}
	transaction.bulkStatements = append(
		transaction.bulkStatements,
		statement,
	)
	payload, readErr := io.ReadAll(request.reader())
	transaction.bulkPayloads = append(transaction.bulkPayloads, payload)
	if readErr != nil {
		return 0, readErr
	}
	if index < len(transaction.bulkErrs) &&
		transaction.bulkErrs[index] != nil {
		return 0, transaction.bulkErrs[index]
	}
	if index >= len(transaction.bulkAffected) {
		return 0, nil
	}
	return transaction.bulkAffected[index], nil
}

func (transaction *mysqlNativeTestTransaction) Commit() error {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"commit",
	)
	transaction.commits++
	return transaction.commitErr
}

func (transaction *mysqlNativeTestTransaction) Rollback() error {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"rollback",
	)
	transaction.rollbacks++
	return transaction.rollbackErr
}

func (transaction *mysqlNativeTestTransaction) AcquireStage4NetworkReplayFence(
	_ context.Context,
	table schema.Table,
) error {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"acquire Stage 4 replay fence",
	)
	transaction.stage4Table = table
	return transaction.stage4FenceErr
}

func (transaction *mysqlNativeTestTransaction) PreflightStage4NetworkReplayIsolation(
	_ context.Context,
	table schema.Table,
	flavor engine.MySQLServerFlavor,
) error {
	transaction.stage4Events = append(
		transaction.stage4Events,
		"prove Stage 4 replay isolation",
	)
	transaction.stage4Table = table
	transaction.stage4Flavor = flavor
	return transaction.stage4ProofErr
}

func (transaction *mysqlNativeTestTransaction) RollbackStage4Network(
	ctx context.Context,
) error {
	transaction.stage4RollbackContextErr = ctx.Err()
	_, transaction.stage4RollbackHasDeadline = ctx.Deadline()
	return transaction.Rollback()
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
	return newMySQLNativeTestWriterForFlavor(
		engine.MySQLServerFlavorOracle80,
	)
}

func newMySQLNativeTestWriterForFlavor(
	flavor engine.MySQLServerFlavor,
) (
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
		flavor:      flavor,
		localInfile: mysqlLocalInfileFallback,
	}, transaction
}

func newMySQLNativeBulkTestWriter() (
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
		flavor: engine.MySQLServerFlavorOracle80,
	}, transaction
}
