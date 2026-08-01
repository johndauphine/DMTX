package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func sqlServerDeleteTestTable(namespace, name string) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{
			{Name: "tenant_id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "item_id", Type: "int", PrimaryKey: true, PrimaryKeyPosition: 2},
			{Name: "payload", Type: "varchar(32)"},
		},
	}
}

func sqlServerDeleteTestEndpoint(namespace string) sqlServerDeleteEndpointIdentity {
	createdAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	return sqlServerDeleteEndpointIdentity{
		Server:     "dmtx-sqlserver",
		Database:   "dmtx_delete_test",
		DatabaseID: 42,
		CreatedAt:  createdAt,
		Collation:  "Latin1_General_100_BIN2_UTF8",
		Principal:  "dmtx",
		Major:      16,
		Version:    "16.0.1000.1",
		IdentityKey: "mssql-delete-v1 server=dmtx-sqlserver database=dmtx_delete_test database_id=42 created_at=" +
			createdAt.Format(time.RFC3339Nano) + " collation=Latin1_General_100_BIN2_UTF8 namespace=" + namespace,
	}
}

func sqlServerDeleteTestWorkloadIdentity(t *testing.T, namespace string) string {
	t.Helper()
	identity, err := config.NetworkEndpointWorkloadIdentity(config.Endpoint{
		Type:     "mssql",
		Host:     "dmtx-sqlserver",
		Port:     1433,
		Database: "dmtx_delete_test",
		Schema:   namespace,
	})
	if err != nil {
		t.Fatalf("build SQL Server delete test workload identity: %v", err)
	}
	return identity
}

func sqlServerDeleteTestAuthority(
	t *testing.T,
	table schema.Table,
	canDelete bool,
) sqlServerDeleteCatalogAuthority {
	t.Helper()
	primaryKey, err := deletePrimaryKeyColumns(table)
	if err != nil {
		t.Fatal(err)
	}
	authority := sqlServerDeleteCatalogAuthority{
		Endpoint:   sqlServerDeleteTestEndpoint(table.Schema),
		Namespace:  table.Schema,
		TableShape: cloneStage4RichTable(table),
		PrimaryKey: primaryKey,
		SchemaID:   101,
		ObjectID:   102,
		CanSelect:  true,
		CanDelete:  canDelete,
	}
	authority.CatalogDigest, err = sqlServerDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func sqlServerDeleteTestJournalCatalog(
	t *testing.T,
	headerIdentity string,
) sqlServerDeleteJournalCatalog {
	t.Helper()
	primaryIndex := func(name string) sqlServerDeleteJournalIndex {
		return sqlServerDeleteJournalIndex{
			ID: 1, Name: name, Type: 1, TypeDescription: "CLUSTERED",
			Unique: true, PrimaryKey: true, KeyColumnID: 1, KeyOrdinal: 1,
		}
	}
	catalog := sqlServerDeleteJournalCatalog{
		SchemaExists:   true,
		SchemaID:       201,
		SchemaOwner:    "dmtx",
		Exists:         true,
		ObjectID:       202,
		Columns:        sqlServerDeleteExpectedJournalColumns(),
		Index:          primaryIndex(sqlServerDeleteJournalPKConstraint),
		HeaderObjectID: 203,
		HeaderColumns:  sqlServerDeleteExpectedJournalHeaderColumns(),
		HeaderIndex:    primaryIndex(sqlServerDeleteJournalHeaderPK),
		HeaderIdentity: headerIdentity,
	}
	var err error
	catalog.CatalogDigest, err = sqlServerDeleteJournalCatalogDigest(catalog)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func sqlServerDeleteTestBatch(table schema.Table) deleteTargetBatch {
	return deleteTargetBatch{
		Table: table, Columns: []string{"tenant_id", "item_id"},
		PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
		Sequence: 7, BatchDigest: strings.Repeat("c", 64),
		Keys: [][]driver.Value{{int64(4), int64(9)}, {int64(8), int64(3)}},
	}
}

func TestSQLServerDeleteCompositeIntegerKeyAuthorityAndBounds(t *testing.T) {
	source := sqlServerDeleteTestTable("source", "items")
	target := sqlServerDeleteTestTable("target", "items")
	sourceAuthority := sqlServerDeleteTestAuthority(t, source, false)
	targetAuthority := sqlServerDeleteTestAuthority(t, target, true)
	canonicalizer, err := newSQLServerDeleteKeyCanonicalizer(
		source, target, sourceAuthority, targetAuthority,
	)
	if err != nil {
		t.Fatal(err)
	}
	sourceKey, err := deletePrimaryKeyColumns(source)
	if err != nil {
		t.Fatal(err)
	}
	targetKey, err := deletePrimaryKeyColumns(target)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(source, target, sourceKey, targetKey)
	if err != nil {
		t.Fatal(err)
	}
	for index, values := range []struct{ source, target any }{
		{int64(42), []byte("42")},
		{int32(7), int64(7)},
	} {
		left, err := canonicalizer.CanonicalizeDeleteKeyValue(
			deleteKeySourceSide, proof, index, values.source,
		)
		if err != nil {
			t.Fatal(err)
		}
		right, err := canonicalizer.CanonicalizeDeleteKeyValue(
			deleteKeyTargetSide, proof, index, values.target,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(left.Canonical, right.Canonical) || right.Parameter == nil {
			t.Fatalf("canonical key %d differs source=%#v target=%#v", index, left, right)
		}
	}

	textTarget := target
	textTarget.Columns = append([]schema.Column(nil), target.Columns...)
	textTarget.Columns[1].Type = "varchar(32)"
	if _, err := newSQLServerDeleteKeyCanonicalizer(
		source,
		textTarget,
		sourceAuthority,
		sqlServerDeleteTestAuthority(t, textTarget, true),
	); err == nil {
		t.Fatal("text primary key was admitted without a SQL Server delete equality proof")
	}

	batch := sqlServerDeleteTestBatch(target)
	keys, err := validateSQLServerDeleteBatchWithLimits("target", batch, 4, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || len(keys[0]) != 2 {
		t.Fatalf("validated composite keys = %#v", keys)
	}
	statement, err := sqlServerDeleteBatchStatement(target, batch.Columns, len(keys))
	if err != nil {
		t.Fatal(err)
	}
	want := "DELETE FROM [target].[items] WHERE ([tenant_id] = @p1 AND [item_id] = @p2) OR ([tenant_id] = @p3 AND [item_id] = @p4)"
	if statement != want {
		t.Fatalf("statement = %q, want %q", statement, want)
	}
	if _, err := validateSQLServerDeleteBatchWithLimits("target", batch, 3, 64); err == nil || !strings.Contains(err.Error(), "parameter") {
		t.Fatalf("over-parameterized composite batch was admitted: %v", err)
	}
	batch.Keys = [][]driver.Value{{strings.Repeat("x", 40), int64(1)}}
	if _, err := validateSQLServerDeleteBatchWithLimits("target", batch, 4, 32); err == nil || !strings.Contains(err.Error(), "byte") {
		t.Fatalf("over-byte-bound batch was admitted: %v", err)
	}
}

func TestSQLServerDeleteNormalizesEmptyCatalogObjectLists(t *testing.T) {
	target := sqlServerDeleteTestTable("target", "items")
	observedTarget := target
	// Native SQL Server discovery allocates these zero-length collections,
	// while a planned table may leave them nil. They are the same empty native
	// catalog fact, but nonempty object differences must remain authority drift.
	observedTarget.Indexes = []schema.Index{}
	observedTarget.ForeignKeys = []schema.ForeignKey{}
	observedTarget.Checks = []schema.CheckConstraint{}
	if !reflect.DeepEqual(
		normalizeSQLServerDeleteTableShape(target),
		normalizeSQLServerDeleteTableShape(observedTarget),
	) {
		t.Fatal("empty SQL Server native object lists did not normalize to the planned catalog shape")
	}

	drifted := observedTarget
	drifted.Indexes = []schema.Index{{Name: "unexpected", Columns: []schema.IndexColumn{{Name: "payload"}}}}
	if reflect.DeepEqual(
		normalizeSQLServerDeleteTableShape(target),
		normalizeSQLServerDeleteTableShape(drifted),
	) {
		t.Fatal("nonempty SQL Server native object drift was normalized away")
	}
}

func TestSQLServerDeleteJournalHeaderBindsReadinessAndReceipts(t *testing.T) {
	endpoint := sqlServerDeleteTestEndpoint("dbo")
	firstJournal := sqlServerDeleteTestJournalCatalog(t, strings.Repeat("d", 64))
	request := Stage4DeleteJournalReadinessRequest{
		RunID: "mssql-delete-header", InventoryDigest: strings.Repeat("a", 64),
	}
	firstReadiness, err := sqlServerDeleteReadinessFromCatalog(
		request, endpoint.IdentityKey, endpoint, firstJournal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstReadiness.Validate(); err != nil {
		t.Fatal(err)
	}

	replacement := sqlServerDeleteTestJournalCatalog(t, strings.Repeat("e", 64))
	replacement.ObjectID = firstJournal.ObjectID
	replacement.HeaderObjectID = firstJournal.HeaderObjectID
	replacement.CatalogDigest, err = sqlServerDeleteJournalCatalogDigest(replacement)
	if err != nil {
		t.Fatal(err)
	}
	replacementReadiness, err := sqlServerDeleteReadinessFromCatalog(
		request, endpoint.IdentityKey, endpoint, replacement,
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstReadiness.Equal(replacementReadiness) {
		t.Fatal("exact-DDL replacement with a new immutable journal header reused readiness authority")
	}

	table := sqlServerDeleteTestTable("dbo", "items")
	authority := sqlServerDeleteTestAuthority(t, table, true)
	batch := sqlServerDeleteTestBatch(table)
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token, Sequence: batch.Sequence,
		BatchDigest: batch.BatchDigest, Candidates: int64(len(batch.Keys)), DeletedRows: 1,
	}
	receipt.ReceiptDigest, err = sqlServerDeleteReceiptDigest(receipt, authority, firstJournal)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSQLServerDeleteReceipt(batch, receipt, authority, firstJournal); err != nil {
		t.Fatalf("validate original header-bound receipt: %v", err)
	}
	if err := validateSQLServerDeleteReceipt(batch, receipt, authority, replacement); err == nil {
		t.Fatal("receipt from replaced header was accepted")
	}
}

func TestSQLServerDeleteJournalAdmissionAndDatabaseScopedLock(t *testing.T) {
	identity := sqlServerDeleteTestEndpoint("dbo")
	catalog := sqlServerDeleteTestJournalCatalog(t, strings.Repeat("d", 64))
	allowed := sqlServerDeleteJournalPrivileges{
		ViewDefinition: true, CreateSchema: true, CreateTable: true,
		SchemaControl: true, SchemaAlter: true, SchemaSelect: true, SchemaInsert: true,
	}
	if err := validateSQLServerDeleteJournalAdmission(identity, catalog, allowed); err != nil {
		t.Fatalf("exact journal admission: %v", err)
	}
	malformed := catalog
	malformed.Columns = append([]sqlServerDeleteJournalColumn(nil), catalog.Columns...)
	malformed.Columns[0].Name = "unexpected_token"
	if err := validateSQLServerDeleteJournalColumns(malformed.Columns); err == nil {
		t.Fatal("malformed receipt journal columns were accepted")
	}
	malformedHeader := catalog
	malformedHeader.HeaderColumns = append([]sqlServerDeleteJournalColumn(nil), catalog.HeaderColumns...)
	malformedHeader.HeaderColumns[1].MaxLength = 63
	if err := validateSQLServerDeleteJournalExactColumns(
		"header", malformedHeader.HeaderColumns, sqlServerDeleteExpectedJournalHeaderColumns(),
	); err == nil {
		t.Fatal("malformed immutable journal header was accepted")
	}

	otherPrincipal := catalog
	otherPrincipal.SchemaOwner = "other_owner"
	if err := validateSQLServerDeleteJournalAdmission(identity, otherPrincipal, allowed); err == nil || !strings.Contains(err.Error(), "multi-principal") {
		t.Fatalf("shared-schema different-principal admission = %v", err)
	}
	missingPrivilege := allowed
	missingPrivilege.SchemaInsert = false
	if err := validateSQLServerDeleteJournalAdmission(identity, catalog, missingPrivilege); err == nil || !strings.Contains(err.Error(), "CONTROL") {
		t.Fatalf("journal admission without INSERT authority = %v", err)
	}

	otherSchema := identity
	otherSchema.IdentityKey = strings.Replace(otherSchema.IdentityKey, "namespace=dbo", "namespace=other_schema", 1)
	firstLock, err := sqlServerDeleteJournalLockResource(identity)
	if err != nil {
		t.Fatal(err)
	}
	secondLock, err := sqlServerDeleteJournalLockResource(otherSchema)
	if err != nil {
		t.Fatal(err)
	}
	if firstLock != secondLock {
		t.Fatalf("database-global journal locks differ by configured schema: %q != %q", firstLock, secondLock)
	}
	otherDatabase := identity
	otherDatabase.DatabaseID++
	otherLock, err := sqlServerDeleteJournalLockResource(otherDatabase)
	if err != nil {
		t.Fatal(err)
	}
	if firstLock == otherLock {
		t.Fatal("different SQL Server database incarnation reused delete-journal lock")
	}
}

func TestSQLServerDeleteRejectsSameDatabaseEndpointAcrossSchemas(t *testing.T) {
	source := sqlServerDeleteTestEndpoint("source_schema")
	target := sqlServerDeleteTestEndpoint("target_schema")
	if err := requireDistinctSQLServerDeleteEndpoints(source, target); err == nil ||
		!strings.Contains(err.Error(), "same database endpoint") {
		t.Fatalf("same-endpoint SQL Server delete admission = %v", err)
	}
	distinct := target
	distinct.DatabaseID++
	if err := requireDistinctSQLServerDeleteEndpoints(source, distinct); err != nil {
		t.Fatalf("distinct SQL Server database incarnation refused: %v", err)
	}
}

type sqlServerDeleteReceiptInsertConnection struct {
	result sql.Result
	err    error
}

func (connection sqlServerDeleteReceiptInsertConnection) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected SQL Server delete receipt query")
}

func (connection sqlServerDeleteReceiptInsertConnection) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

func (connection sqlServerDeleteReceiptInsertConnection) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return connection.result, connection.err
}

type sqlServerDeleteReceiptInsertResult struct {
	affected int64
	err      error
}

func (result sqlServerDeleteReceiptInsertResult) LastInsertId() (int64, error) {
	return 0, errors.New("SQL Server delete receipt does not expose a last insert ID")
}

func (result sqlServerDeleteReceiptInsertResult) RowsAffected() (int64, error) {
	return result.affected, result.err
}

func TestInsertSQLServerDeleteReceiptRequiresExactlyOneAffectedRow(t *testing.T) {
	table := sqlServerDeleteTestTable("dbo", "items")
	authority := sqlServerDeleteTestAuthority(t, table, true)
	journal := sqlServerDeleteTestJournalCatalog(t, strings.Repeat("d", 64))
	batch := sqlServerDeleteTestBatch(table)
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token, Sequence: batch.Sequence,
		BatchDigest: batch.BatchDigest, Candidates: int64(len(batch.Keys)), DeletedRows: 1,
	}
	var err error
	receipt.ReceiptDigest, err = sqlServerDeleteReceiptDigest(receipt, authority, journal)
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name     string
		result   sql.Result
		wantFail bool
	}{
		{name: "exactly_one", result: driver.RowsAffected(1)},
		{name: "zero", result: driver.RowsAffected(0), wantFail: true},
		{name: "multiple", result: driver.RowsAffected(2), wantFail: true},
		{name: "unknown", result: sqlServerDeleteReceiptInsertResult{err: errors.New("unavailable")}, wantFail: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := insertSQLServerDeleteReceipt(
				context.Background(),
				sqlServerDeleteReceiptInsertConnection{result: testCase.result},
				receipt,
				authority,
				journal,
			)
			if testCase.wantFail && err == nil {
				t.Fatal("non-exact receipt insertion was accepted")
			}
			if !testCase.wantFail && err != nil {
				t.Fatalf("exact receipt insertion: %v", err)
			}
		})
	}
}

func TestSQLServerDeleteTableLockQueriesNeverUseTopZero(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		exclusive bool
		hint      string
	}{
		{name: "shared", hint: "TABLOCK, HOLDLOCK"},
		{name: "exclusive", exclusive: true, hint: "TABLOCKX, HOLDLOCK"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			probe, emptyProbe, err := sqlServerDeleteTableLockQueries("[dbo].[items]", testCase.exclusive)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{probe, emptyProbe} {
				if strings.Contains(strings.ToUpper(query), "TOP (0)") || !strings.Contains(query, testCase.hint) {
					t.Fatalf("unsafe lock query %q", query)
				}
			}
			if !strings.Contains(probe, "TOP (1)") || !strings.Contains(emptyProbe, "COUNT_BIG(*)") {
				t.Fatalf("lock probes = %q / %q, want executed TOP (1) plus empty-table COUNT_BIG", probe, emptyProbe)
			}
		})
	}
}

func TestStage4DeleteCompositionAdmitsOnlySQLServerSameEngineCell(t *testing.T) {
	cfg, prepared := stage4PostgresDeleteRunnerFixture()
	if err := requireStage4AdapterPostgresDeleteComposition(cfg, "mssql", "mssql", prepared); err != nil {
		t.Fatalf("MSSQL-to-MSSQL delete cell was refused: %v", err)
	}
	if err := requireStage4AdapterPostgresDeleteComposition(cfg, "mssql", "postgres", prepared); err == nil || !strings.Contains(err.Error(), "atomic-receipt capability") {
		t.Fatalf("uncertified SQL Server cross-engine delete cell was admitted: %v", err)
	}
}

var registerSQLServerDeleteSetupDriver sync.Once

type sqlServerDeleteSetupDriverState struct {
	mu         sync.Mutex
	calls      []string
	beginErr   error
	blockSetup bool
}

func (state *sqlServerDeleteSetupDriverState) reset(beginErr error, blockSetup bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.calls = nil
	state.beginErr = beginErr
	state.blockSetup = blockSetup
}

func (state *sqlServerDeleteSetupDriverState) record(query string) (error, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.calls = append(state.calls, query)
	if strings.TrimSpace(query) == "BEGIN TRANSACTION" {
		return state.beginErr, false
	}
	if strings.TrimSpace(query) == "SET XACT_ABORT ON" {
		return nil, state.blockSetup
	}
	return nil, false
}

func (state *sqlServerDeleteSetupDriverState) snapshot() []string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]string(nil), state.calls...)
}

var sqlServerDeleteSetupState sqlServerDeleteSetupDriverState

type sqlServerDeleteSetupDriver struct{}

func (sqlServerDeleteSetupDriver) Open(string) (driver.Conn, error) {
	return sqlServerDeleteSetupConnection{}, nil
}

type sqlServerDeleteSetupConnection struct{}

func (sqlServerDeleteSetupConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected SQL Server delete setup prepare")
}

func (sqlServerDeleteSetupConnection) Close() error { return nil }

func (sqlServerDeleteSetupConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected SQL Server delete setup Begin")
}

func (sqlServerDeleteSetupConnection) ExecContext(
	ctx context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	err, block := sqlServerDeleteSetupState.record(query)
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

var _ driver.ExecerContext = sqlServerDeleteSetupConnection{}

func openSQLServerDeleteSetupDatabase(t *testing.T, beginErr error, blockSetup bool) *sql.DB {
	t.Helper()
	registerSQLServerDeleteSetupDriver.Do(func() {
		sql.Register("dmtx_mssql_delete_setup", sqlServerDeleteSetupDriver{})
	})
	sqlServerDeleteSetupState.reset(beginErr, blockSetup)
	database, err := sql.Open("dmtx_mssql_delete_setup", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func assertSQLServerDeleteBeginFailureCleanup(t *testing.T, calls []string) {
	t.Helper()
	joined := strings.Join(calls, "\n")
	for _, want := range []string{
		"SET XACT_ABORT ON",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"BEGIN TRANSACTION",
		"SET XACT_ABORT OFF",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("begin-failure cleanup calls=%q, missing %q", joined, want)
		}
	}
	if strings.Contains(joined, "ROLLBACK TRANSACTION") {
		t.Fatalf("failed BEGIN was incorrectly treated as active transaction: %q", joined)
	}
}

func TestSQLServerDeletePrepareBeginFailureUsesSetupCleanup(t *testing.T) {
	database := openSQLServerDeleteSetupDatabase(t, errors.New("begin refused"), false)
	adapter := &sqlServerTargetAdapter{
		database:         database,
		namespace:        "dbo",
		workloadIdentity: sqlServerDeleteTestWorkloadIdentity(t, "dbo"),
	}
	_, err := adapter.PrepareStage4DeleteJournalReadiness(
		context.Background(),
		Stage4DeleteJournalReadinessRequest{
			RunID: "begin-failure-prepare", InventoryDigest: strings.Repeat("a", 64),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "begin SQL Server delete journal transaction") {
		t.Fatalf("Prepare begin failure = %v", err)
	}
	assertSQLServerDeleteBeginFailureCleanup(t, sqlServerDeleteSetupState.snapshot())
}

func TestSQLServerDeleteApplyBeginFailureUsesSetupCleanup(t *testing.T) {
	database := openSQLServerDeleteSetupDatabase(t, errors.New("begin refused"), false)
	table := sqlServerDeleteTestTable("dbo", "items")
	adapter := &sqlServerTargetAdapter{database: database, namespace: "dbo"}
	capability := &sqlServerDeleteTargetCapability{
		adapter: adapter, authority: sqlServerDeleteTestAuthority(t, table, true),
	}
	_, err := capability.ApplyDeleteBatch(context.Background(), sqlServerDeleteTestBatch(table))
	if err == nil || !strings.Contains(err.Error(), "begin SQL Server delete receipt transaction") {
		t.Fatalf("Apply begin failure = %v", err)
	}
	assertSQLServerDeleteBeginFailureCleanup(t, sqlServerDeleteSetupState.snapshot())
}

func TestSQLServerDeleteSetupHonorsCallerCancellation(t *testing.T) {
	database := openSQLServerDeleteSetupDatabase(t, nil, true)
	adapter := &sqlServerTargetAdapter{
		database:         database,
		namespace:        "dbo",
		workloadIdentity: sqlServerDeleteTestWorkloadIdentity(t, "dbo"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := adapter.PrepareStage4DeleteJournalReadiness(
		ctx,
		Stage4DeleteJournalReadinessRequest{
			RunID: "cancelled-prepare", InventoryDigest: strings.Repeat("a", 64),
		},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled setup error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancelled setup was detached from caller deadline: %s", elapsed)
	}
	calls := sqlServerDeleteSetupState.snapshot()
	if strings.Contains(strings.Join(calls, "\n"), "BEGIN TRANSACTION") {
		t.Fatalf("cancelled setup reached BEGIN: %q", calls)
	}
	for _, want := range []string{"SET XACT_ABORT OFF", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"} {
		if !strings.Contains(strings.Join(calls, "\n"), want) {
			t.Fatalf("cancelled setup did not reset session: %q", calls)
		}
	}
}
