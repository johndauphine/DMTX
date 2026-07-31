package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4NetworkTransactionFenceStatementsPinExactTables(
	t *testing.T,
) {
	mysqlTable := schema.Table{
		Schema: "target`db",
		Name:   "event`data",
	}
	if got, want := stage4MySQLNetworkReplayFenceStatement(mysqlTable),
		"SELECT 1 FROM `target``db`.`event``data` LIMIT 1 FOR UPDATE"; got != want {
		t.Fatalf("MySQL replay fence = %q, want %q", got, want)
	}

	sqlServerTable := schema.Table{
		Schema: "Target]Schema",
		Name:   "event]data",
	}
	if got, want := stage4SQLServerNetworkReplayFenceStatement(
		sqlServerTable,
	), "SELECT TOP (1) 1 FROM [Target]]Schema].[event]]data] "+
		"WITH (TABLOCKX, HOLDLOCK)"; got != want {
		t.Fatalf(
			"SQL Server replay fence = %q, want %q",
			got,
			want,
		)
	}
}

func TestStage4SQLServerNetworkUpdateActionFailsClosed(
	t *testing.T,
) {
	for _, testCase := range []struct {
		code        int
		description string
		want        string
	}{
		{code: 0, description: "NO_ACTION", want: "NO ACTION"},
		{code: 1, description: "CASCADE", want: "CASCADE"},
		{code: 2, description: "SET_NULL", want: "SET NULL"},
		{code: 3, description: "SET_DEFAULT", want: "SET DEFAULT"},
	} {
		actual, err := stage4SQLServerNetworkUpdateAction(
			testCase.code,
			testCase.description,
		)
		if err != nil {
			t.Fatalf(
				"action %d/%q: %v",
				testCase.code,
				testCase.description,
				err,
			)
		}
		if actual != testCase.want {
			t.Fatalf(
				"action %d/%q = %q, want %q",
				testCase.code,
				testCase.description,
				actual,
				testCase.want,
			)
		}
	}
	if _, err := stage4SQLServerNetworkUpdateAction(
		1,
		"SET_NULL",
	); err == nil || !strings.Contains(
		err.Error(),
		"conflicts",
	) {
		t.Fatalf("conflicting action error = %v", err)
	}
	if _, err := stage4SQLServerNetworkUpdateAction(
		9,
		"MAGIC",
	); err == nil || !strings.Contains(
		err.Error(),
		"unexpected",
	) {
		t.Fatalf("unknown action error = %v", err)
	}
}

func TestStage4MySQLAndSQLServerTransactionProofForeignKeySafety(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "app",
		Name:   "parents",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
		},
	}
	for _, engineName := range []string{
		"MySQL",
		"MariaDB",
		"SQL Server",
	} {
		profiles, err := stage4NetworkReplayTableProfiles(
			engineName,
			[]schema.Table{table},
			stage4ExactIdentifier,
		)
		if err != nil {
			t.Fatalf("%s profiles: %v", engineName, err)
		}
		profile := profiles[adapterSourceTableKey("app", "parents")]
		base := stage4NetworkIncomingForeignKey{
			parentNamespace:     "external",
			parentTable:         "children",
			name:                "children_parent_fkey",
			referencedNamespace: "app",
			referencedTable:     "parents",
			updateAction:        "CASCADE",
		}

		safePrimary := base
		safePrimary.referencedColumn = "id"
		if err := validateStage4NetworkIncomingForeignKey(
			engineName,
			profile,
			safePrimary,
			stage4ExactIdentifier,
		); err != nil {
			t.Fatalf("%s immutable-key cascade: %v", engineName, err)
		}

		safeNoAction := base
		safeNoAction.referencedColumn = "code"
		safeNoAction.updateAction = "NO ACTION"
		if err := validateStage4NetworkIncomingForeignKey(
			engineName,
			profile,
			safeNoAction,
			stage4ExactIdentifier,
		); err != nil {
			t.Fatalf("%s NO ACTION: %v", engineName, err)
		}

		unsafe := base
		unsafe.referencedColumn = "code"
		err = validateStage4NetworkIncomingForeignKey(
			engineName,
			profile,
			unsafe,
			stage4ExactIdentifier,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"ON UPDATE CASCADE on mutable column code",
		) {
			t.Fatalf("%s mutable cascade error = %v", engineName, err)
		}
		var transfer *TransferError
		if !errors.As(err, &transfer) ||
			transfer.Class != ErrorClassPolicy {
			t.Fatalf("%s error class = %v", engineName, err)
		}
	}
}

func TestStage4TargetAdaptersFailClosedWithoutGuardedNativeWriter(
	t *testing.T,
) {
	table := mysqlNativeTestTable()
	mysqlAdapter := &mysqlTargetAdapter{
		batchWriter: &mysqlTargetWriterRecorder{},
		namespace:   table.Schema,
	}
	receipt, err := mysqlAdapter.WriteStage4NetworkBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		[][]any{{int64(1), "payload"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"Stage 4 network batch writer is not configured",
	) {
		t.Fatalf("MySQL missing guard error = %v", err)
	}
	assertMySQLNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)

	sqlServerAdapter := &sqlServerTargetAdapter{
		batchWriter: &sqlServerTargetValueFixtureWriter{},
		namespace:   "dbo",
	}
	sqlServerTable := sqlServerNativeTestTable()
	receipt, err = sqlServerAdapter.WriteStage4NetworkBatch(
		context.Background(),
		sqlServerTable,
		[]string{"tenant_id", "id", "payload"},
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"Stage 4 network batch writer is not configured",
	) {
		t.Fatalf("SQL Server missing guard error = %v", err)
	}
	assertSQLServerNativeReceipt(
		t,
		receipt,
		CommitNotCommitted,
		1,
		0,
	)
}

func TestStage4TargetAdaptersDelegateOnlyToGuardedWriter(
	t *testing.T,
) {
	mysqlWriter := &stage4MySQLAdapterWriter{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	mysqlTable := mysqlNativeTestTable()
	mysqlAdapter := &mysqlTargetAdapter{
		batchWriter: mysqlWriter,
		namespace:   mysqlTable.Schema,
	}
	receipt, err := mysqlAdapter.WriteStage4NetworkBatch(
		context.Background(),
		mysqlTable,
		[]string{"id", "payload"},
		[][]any{{int64(1), "payload"}},
	)
	if err != nil {
		t.Fatalf("MySQL Stage 4 adapter write: %v", err)
	}
	if receipt != mysqlWriter.receipt ||
		mysqlWriter.stage4Calls != 1 ||
		mysqlWriter.ordinaryCalls != 0 {
		t.Fatalf(
			"MySQL guarded delegation = receipt %#v writer %#v",
			receipt,
			mysqlWriter,
		)
	}

	sqlServerWriter := &stage4SQLServerAdapterWriter{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	sqlServerTable := sqlServerNativeTestTable()
	sqlServerAdapter := &sqlServerTargetAdapter{
		batchWriter: sqlServerWriter,
		namespace:   sqlServerTable.Schema,
	}
	receipt, err = sqlServerAdapter.WriteStage4NetworkBatch(
		context.Background(),
		sqlServerTable,
		[]string{"tenant_id", "id", "payload"},
		[][]any{{int64(7), int64(1), "payload"}},
	)
	if err != nil {
		t.Fatalf("SQL Server Stage 4 adapter write: %v", err)
	}
	if receipt != sqlServerWriter.receipt ||
		sqlServerWriter.stage4Calls != 1 ||
		sqlServerWriter.ordinaryCalls != 0 {
		t.Fatalf(
			"SQL Server guarded delegation = receipt %#v writer %#v",
			receipt,
			sqlServerWriter,
		)
	}
}

func TestStage4NetworkTransactionProofQueriesCoverRequiredMetadata(
	t *testing.T,
) {
	for _, fragment := range []string{
		"sys.foreign_key_columns",
		"update_referential_action",
		"update_referential_action_desc",
		"referenced_column.name",
		"parent_schema.name",
		"referenced_schema.name = @p1",
		"referenced_table.name = @p2",
	} {
		if !strings.Contains(
			stage4SQLServerNetworkIncomingForeignKeysQuery,
			fragment,
		) {
			t.Fatalf(
				"SQL Server transaction proof query lacks %q",
				fragment,
			)
		}
	}
	for _, fragment := range []string{
		"sys.tables",
		"sys.triggers",
		"sys.security_predicates",
	} {
		if !strings.Contains(
			sqlServerTargetTableCatalogQuery,
			fragment,
		) {
			t.Fatalf(
				"SQL Server locked retained-target proof query lacks %q",
				fragment,
			)
		}
	}
	for _, fragment := range []string{
		"target_index.is_primary_key = 1",
		"key_column.key_ordinal",
		"target_column.name",
	} {
		if !strings.Contains(
			stage4SQLServerNetworkPrimaryKeyQuery,
			fragment,
		) {
			t.Fatalf(
				"SQL Server locked primary-key proof query lacks %q",
				fragment,
			)
		}
	}
	for label, query := range map[string]string{
		"target":  stage4MySQLReplayTargetExactQuery,
		"key":     stage4MySQLReplayPrimaryKeyExactQuery,
		"trigger": stage4MySQLReplayTriggersExactQuery,
	} {
		if !strings.Contains(query, "BINARY") {
			t.Fatalf(
				"MySQL exact %s proof query lacks binary identity matching",
				label,
			)
		}
	}
	for label, query := range map[string]string{
		"target":  stage4MySQLReplayTargetFoldedQuery,
		"key":     stage4MySQLReplayPrimaryKeyFoldedQuery,
		"trigger": stage4MySQLReplayTriggersFoldedQuery,
	} {
		if !strings.Contains(query, "LOWER(") {
			t.Fatalf(
				"MySQL folded %s proof query lacks folded identity matching",
				label,
			)
		}
	}
}

func TestStage4MySQLTriggerVisibilityFailsClosed(t *testing.T) {
	for _, engineName := range []string{"MySQL", "MariaDB"} {
		err := validateStage4MySQLTriggerMetadataVisibility(
			engineName,
			false,
			false,
			false,
			0,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"selected-table TRIGGER privilege",
		) {
			t.Fatalf("%s missing visibility error = %v", engineName, err)
		}
		var transfer *TransferError
		if !errors.As(err, &transfer) ||
			transfer.Class != ErrorClassPolicy {
			t.Fatalf("%s missing visibility class = %v", engineName, err)
		}
	}
	err := validateStage4MySQLTriggerMetadataVisibility(
		"MySQL",
		true,
		false,
		false,
		1,
	)
	if err == nil || !strings.Contains(err.Error(), "partial_revokes=0") {
		t.Fatalf("MySQL partial-revokes visibility error = %v", err)
	}
	if err := validateStage4MySQLTriggerMetadataVisibility(
		"MariaDB",
		true,
		false,
		false,
		0,
	); err != nil {
		t.Fatalf("MariaDB complete trigger visibility: %v", err)
	}
	if err := validateStage4MySQLTriggerMetadataVisibility(
		"MySQL",
		false,
		true,
		false,
		1,
	); err != nil {
		t.Fatalf(
			"MySQL direct schema trigger visibility with partial revokes: %v",
			err,
		)
	}
}

type stage4MySQLAdapterWriter struct {
	ordinaryCalls int
	stage4Calls   int
	receipt       WriteReceipt
}

func (writer *stage4MySQLAdapterWriter) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	writer.ordinaryCalls++
	return WriteReceipt{}, errors.New("ordinary writer called")
}

func (writer *stage4MySQLAdapterWriter) WriteStage4NetworkBatch(
	context.Context,
	schema.Table,
	[]string,
	[][]any,
) (WriteReceipt, error) {
	writer.stage4Calls++
	return writer.receipt, nil
}

type stage4SQLServerAdapterWriter struct {
	ordinaryCalls int
	stage4Calls   int
	receipt       WriteReceipt
}

func (writer *stage4SQLServerAdapterWriter) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	writer.ordinaryCalls++
	return WriteReceipt{}, errors.New("ordinary writer called")
}

func (writer *stage4SQLServerAdapterWriter) WriteStage4NetworkBatch(
	context.Context,
	schema.Table,
	[]string,
	[][]any,
) (WriteReceipt, error) {
	writer.stage4Calls++
	return writer.receipt, nil
}
