package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestSQLServerTargetSchemaEvolutionLiveTLS is deliberately database-isolated:
// the production reader treats every table in its configured schema as target
// authority, including target-only tables. Reusing the shared live database
// would therefore make this exact-catalog sentinel nondeterministic.
func TestSQLServerTargetSchemaEvolutionLiveTLS(t *testing.T) {
	endpoint := sqlServerTargetEvolutionLiveEndpoint(t)
	var cleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("isolated configured schema", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		testSQLServerTargetSchemaEvolutionLive(t, ctx, endpoint, &cleanup)
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, endpoint, cleanup)
}

// TestStage4AdapterSQLServerSchemaEvolutionComposedRouteLiveTLS proves the
// composed upsert runner invokes the real TLS-opened SQL Server target
// capability before its first data write. The direct sentinel above separately
// proves transactional DDL prefix recovery after a post-DDL/pre-ack loss.
func TestStage4AdapterSQLServerSchemaEvolutionComposedRouteLiveTLS(t *testing.T) {
	endpoint := sqlServerTargetEvolutionLiveEndpoint(t)
	var cleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("isolated configured schema", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		endpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(
			t,
			ctx,
			endpoint,
			&cleanup,
		)
		opened, err := openSQLServerTargetAdapter(ctx, endpoint)
		if err != nil {
			t.Fatalf("open composed SQL Server target: %v", err)
		}
		target, ok := opened.(*sqlServerTargetAdapter)
		if !ok {
			t.Fatalf("composed SQL Server target = %T, want *sqlServerTargetAdapter", opened)
		}
		t.Cleanup(func() { _ = target.database.Close() })

		table := stage4AdapterTestTable()
		table.Name = "dmtx_evo_route_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		var before int
		if err := target.database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			   FROM sys.tables AS target_table
			   JOIN sys.schemas AS target_schema
			     ON target_schema.schema_id = target_table.schema_id
			  WHERE target_schema.name = @p1 AND target_table.name = @p2`,
			endpoint.Schema,
			table.Name,
		).Scan(&before); err != nil {
			t.Fatalf("read composed SQL Server pre-run catalog: %v", err)
		}
		if before != 0 {
			t.Fatalf("composed SQL Server evolution table %s exists before the run", table.Name)
		}

		backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
		cfg.Target = endpoint
		cfg.Migration.TargetMode = "upsert"
		cfg.Migration.IncludeTables = []string{table.Name}
		cfg.Migration.Partitions = 1
		cfg.Migration.SchemaContract = &config.SchemaContract{
			Tables:   config.SchemaContractEvolve,
			Columns:  config.SchemaContractEvolve,
			DataType: config.SchemaContractEvolve,
		}
		runID := "stage4-mssql-evolution-live-" + table.Name
		initializeStage4LifecycleRun(t, backend, runID, time.Now().Add(-time.Minute))
		events := make([]string, 0)
		observer := stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run:                    stage4LifecycleRunContext(t, backend, runID, false),
		}
		result, err := migrateWithStage4Adapters(
			ctx,
			cfg,
			observer,
			&recordingAdapterSource{events: &events, table: table},
			target,
			"upsert",
			observer.run,
		)
		if err != nil {
			t.Fatalf("run composed SQL Server Stage 4 schema evolution: %v", err)
		}
		if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
			t.Fatalf("composed SQL Server Stage 4 result = %#v", result)
		}
		var rows int
		if err := target.database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(endpoint.Schema, table.Name),
		).Scan(&rows); err != nil {
			t.Fatalf("count composed SQL Server evolved rows: %v", err)
		}
		if rows != 2 {
			t.Fatalf("composed SQL Server evolved rows = %d, want 2", rows)
		}
		catalog, err := target.ReadTargetSchemaEvolutionCatalog(ctx)
		if err != nil {
			t.Fatalf("read composed SQL Server evolved catalog: %v", err)
		}
		if findTargetSchemaEvolutionTable(catalog.Tables(), targetSchemaEvolutionTableKey{
			schema: endpoint.Schema,
			table:  table.Name,
		}) < 0 {
			t.Fatalf("composed SQL Server catalog omits evolved table %s", table.Name)
		}
		tasks, _, err := backend.ListWork(runID)
		if err != nil {
			t.Fatal(err)
		}
		// Terminal schema sentinels are published atomically by the app after
		// validation audit persistence, not by the composed migration runner.
		for _, key := range []state.TaskKey{stage4SchemaGateTask, stage4TargetShapeTask} {
			pending := false
			for _, task := range tasks {
				if task.Key == key && task.Status == "running" {
					pending = true
				}
			}
			if !pending {
				t.Fatalf("composed SQL Server evolution did not leave %#v pending aggregate publication: %#v", key, tasks)
			}
		}
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, endpoint, cleanup)
}

func TestSQLServerTargetEvolutionTransactionFenceLiveTLS(t *testing.T) {
	endpoint := sqlServerTargetEvolutionLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := engine.OpenSQLServer2022Target(ctx, endpoint)
	if err != nil {
		t.Fatalf("open verified-TLS SQL Server transaction-fence target: %v", err)
	}
	defer database.Close()
	first, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire first SQL Server fence connection: %v", err)
	}
	defer first.Close()
	second, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire second SQL Server fence connection: %v", err)
	}
	defer second.Close()
	resource := sqlServerTargetEvolutionLockResource(
		"live_" + strconv.FormatInt(time.Now().UnixNano(), 36),
	)
	firstTransaction, err := first.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin first SQL Server fence transaction: %v", err)
	}
	if err := acquireSQLServerTargetEvolutionLock(ctx, firstTransaction, resource); err != nil {
		_ = firstTransaction.Rollback()
		t.Fatalf("acquire first transaction-owned SQL Server fence: %v", err)
	}
	secondTransaction, err := second.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		_ = firstTransaction.Rollback()
		t.Fatalf("begin competing SQL Server fence transaction: %v", err)
	}
	var contended int
	if err := secondTransaction.QueryRowContext(
		ctx,
		sqlServerTargetEvolutionAcquireLockQuery,
		resource,
		0,
	).Scan(&contended); err != nil {
		_ = secondTransaction.Rollback()
		_ = firstTransaction.Rollback()
		t.Fatalf("query competing SQL Server transaction fence: %v", err)
	}
	if contended >= 0 {
		_ = secondTransaction.Rollback()
		_ = firstTransaction.Rollback()
		t.Fatalf("competing SQL Server transaction fence result = %d, want negative", contended)
	}
	if err := secondTransaction.Rollback(); err != nil {
		_ = firstTransaction.Rollback()
		t.Fatalf("roll back contended SQL Server fence transaction: %v", err)
	}
	if err := firstTransaction.Rollback(); err != nil {
		t.Fatalf("roll back owning SQL Server fence transaction: %v", err)
	}
	thirdTransaction, err := second.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin released SQL Server fence transaction: %v", err)
	}
	if err := acquireSQLServerTargetEvolutionLock(ctx, thirdTransaction, resource); err != nil {
		_ = thirdTransaction.Rollback()
		t.Fatalf("acquire SQL Server fence after owning rollback: %v", err)
	}
	if err := thirdTransaction.Rollback(); err != nil {
		t.Fatalf("roll back released SQL Server fence transaction: %v", err)
	}
}

type sqlServerTargetEvolutionLiveCleanupEvidence struct {
	database string
	created  bool
}

func sqlServerTargetEvolutionLiveEndpoint(t *testing.T) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip("set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server target-evolution sentinel")
	}
	return sqlServerCommonFixtureEndpoint(t, dsn, caPath)
}

// sqlServerTargetEvolutionLiveTemporaryDatabase gives the actual target
// principal ownership of a brand-new database and schema. Dropping the
// database at cleanup proves no schema, user, or permission artifact survives.
func sqlServerTargetEvolutionLiveTemporaryDatabase(
	t *testing.T,
	ctx context.Context,
	target config.Endpoint,
	cleanup *sqlServerTargetEvolutionLiveCleanupEvidence,
) config.Endpoint {
	t.Helper()
	adminEndpoint := target
	adminEndpoint.Database = "master"
	admin, err := engine.OpenSQLServer(ctx, adminEndpoint)
	if err != nil {
		t.Fatalf("open verified-TLS SQL Server evolution admin connection: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close SQL Server evolution admin connection: %v", err)
		}
	})

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	databaseName := "dmtx_s4_evo_" + suffix
	quotedDatabase := sqlServerIdentifier(databaseName)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quotedDatabase); err != nil {
		t.Fatalf("create isolated SQL Server evolution database: %v", err)
	}
	cleanup.database = databaseName
	cleanup.created = true
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quotedDatabase+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedDatabase,
		); err != nil {
			t.Errorf("drop isolated SQL Server evolution database %s: %v", databaseName, err)
		}
	})
	if _, err := admin.ExecContext(
		ctx,
		"ALTER DATABASE "+quotedDatabase+" SET COMPATIBILITY_LEVEL = 160; "+
			"ALTER DATABASE "+quotedDatabase+" SET AUTO_CLOSE OFF; "+
			"ALTER DATABASE "+quotedDatabase+" SET AUTO_SHRINK OFF; "+
			"ALTER AUTHORIZATION ON DATABASE::"+quotedDatabase+" TO "+sqlServerIdentifier(target.User),
	); err != nil {
		t.Fatalf("configure isolated SQL Server evolution database: %v", err)
	}

	target.Database = databaseName
	target.Schema = "dmtx_evo_" + suffix
	setup, err := engine.OpenSQLServer(ctx, target)
	if err != nil {
		t.Fatalf("open isolated SQL Server evolution target database: %v", err)
	}
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+sqlServerIdentifier(target.Schema)+" AUTHORIZATION [dbo]",
	); err != nil {
		_ = setup.Close()
		t.Fatalf("create isolated SQL Server evolution schema: %v", err)
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("close isolated SQL Server evolution setup connection: %v", err)
	}
	return target
}

func assertSQLServerTargetEvolutionLiveDatabaseRemoved(
	t *testing.T,
	target config.Endpoint,
	cleanup sqlServerTargetEvolutionLiveCleanupEvidence,
) {
	t.Helper()
	if !cleanup.created {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminEndpoint := target
	adminEndpoint.Database = "master"
	admin, err := engine.OpenSQLServer(ctx, adminEndpoint)
	if err != nil {
		t.Fatalf("open SQL Server evolution cleanup verifier: %v", err)
	}
	defer admin.Close()
	var remaining int
	if err := admin.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		cleanup.database,
	).Scan(&remaining); err != nil {
		t.Fatalf("verify SQL Server evolution cleanup: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("isolated SQL Server evolution database %s remains after cleanup", cleanup.database)
	}
}

func testSQLServerTargetSchemaEvolutionLive(
	t *testing.T,
	ctx context.Context,
	base config.Endpoint,
	cleanup *sqlServerTargetEvolutionLiveCleanupEvidence,
) {
	t.Helper()
	endpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, cleanup)
	opened, err := openSQLServerTargetAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server target adapter: %v", err)
	}
	adapter, ok := opened.(*sqlServerTargetAdapter)
	if !ok {
		t.Fatalf("target adapter type = %T", opened)
	}
	t.Cleanup(func() { _ = adapter.database.Close() })

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "items_" + suffix
	childName := tableName + "_child"
	untouchedName := tableName + "_audit"
	qualified := sqlServerQualified(endpoint.Schema, tableName)
	if _, err := adapter.database.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s ([id] BIGINT NOT NULL, [value] VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, [must_relax] INT NOT NULL, CONSTRAINT %s PRIMARY KEY CLUSTERED ([id] ASC))",
		qualified,
		sqlServerIdentifier("pk_"+tableName),
	)); err != nil {
		t.Fatalf("create SQL Server evolution source-shaped target: %v", err)
	}
	if _, err := adapter.database.ExecContext(ctx,
		"INSERT INTO "+qualified+" ([id], [value], [must_relax]) VALUES (1, 'retained', 9)",
	); err != nil {
		t.Fatalf("seed SQL Server retained target row: %v", err)
	}
	untouchedQualified := sqlServerQualified(endpoint.Schema, untouchedName)
	if _, err := adapter.database.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s ([id] BIGINT NOT NULL, [code] VARCHAR(16) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, CONSTRAINT %s PRIMARY KEY CLUSTERED ([id] ASC), CONSTRAINT %s CHECK ([id] > (0)))",
		untouchedQualified,
		sqlServerIdentifier("pk_"+untouchedName),
		sqlServerIdentifier("ck_"+untouchedName),
	)); err != nil {
		t.Fatalf("create SQL Server target-only table: %v", err)
	}
	if _, err := adapter.database.ExecContext(ctx,
		"CREATE INDEX "+sqlServerIdentifier("ix_"+untouchedName+"_code")+" ON "+untouchedQualified+" ([code] ASC); "+
			"INSERT INTO "+untouchedQualified+" ([id], [code]) VALUES (1, 'untouched')",
	); err != nil {
		t.Fatalf("seed SQL Server target-only objects: %v", err)
	}

	priorCatalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read exact SQL Server prior catalog: %v", err)
	}
	prior := priorCatalog.Tables()
	current := cloneTargetSchemaEvolutionTables(prior)
	tableIndex := findTargetSchemaEvolutionTable(current, targetSchemaEvolutionTableKey{
		schema: endpoint.Schema,
		table:  tableName,
	})
	if tableIndex < 0 {
		t.Fatalf("prior SQL Server catalog omits %s: %#v", tableName, prior)
	}
	value := findTargetSchemaEvolutionColumnIndex(current[tableIndex], "value")
	relax := findTargetSchemaEvolutionColumnIndex(current[tableIndex], "must_relax")
	if value < 0 || relax < 0 {
		t.Fatalf("prior SQL Server columns are incomplete: %#v", current[tableIndex].Columns)
	}
	current[tableIndex].Columns[value].DeclaredType = &schema.DeclaredType{
		Base: "varchar", Arguments: []int{32},
	}
	current[tableIndex].Columns[relax].Nullable = true
	current[tableIndex].Columns = append(current[tableIndex].Columns, schema.Column{
		Name: "added", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}},
	})
	child, err := sqlServerTargetEvolutionLiveChildTable(endpoint.Schema, childName, tableName)
	if err != nil {
		t.Fatal(err)
	}
	if err := canonicalizeSQLServerTargetChecks(&child); err != nil {
		t.Fatalf("canonicalize planned SQL Server child CHECK: %v", err)
	}
	if err := canonicalizeSQLServerTargetForeignKeys(&child); err != nil {
		t.Fatalf("canonicalize planned SQL Server child foreign key: %v", err)
	}
	current = append(current, child)
	sortTargetSchemaEvolutionTables(current)
	request := sqlServerTargetEvolutionLiveRequest(
		t,
		prior,
		current,
		tableName,
		childName,
		priorCatalog.Reservations(),
		adapter.TargetSchemaEvolutionCreatePlanner(),
	)
	plan, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || plan.OperationCount() < 5 || plan.AppliedPrefix() != 0 {
		t.Fatalf("SQL Server preflight plan=%#v err=%v", plan, err)
	}

	// Simulate a process loss after transactional DDL committed but before DMTX
	// had durably acknowledged the operation. The resumed preflight admits only
	// the authenticated exact prefix, never rerunning the already committed DDL.
	first := plan.PendingOperations()[0].Statements()
	if len(first) != 1 {
		t.Fatalf("first SQL Server evolution operation statements = %#v", first)
	}
	if _, err := adapter.database.ExecContext(ctx, first[0]); err != nil {
		t.Fatalf("simulate SQL Server post-DDL/pre-ack loss: %v", err)
	}
	resumed, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || resumed.AppliedPrefix() != 1 || resumed.Complete() {
		t.Fatalf("SQL Server recovered preflight prefix=%d complete=%t err=%v", resumed.AppliedPrefix(), resumed.Complete(), err)
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, resumed); err != nil {
		actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
		var facts []schema.SchemaDriftFact
		if readErr == nil && len(resumed.states) > 2 {
			expectedSnapshot, expectedErr := schema.NewSchemaSnapshot(resumed.states[2])
			actualSnapshot, actualErr := schema.NewSchemaSnapshot(actual.Tables())
			if expectedErr == nil && actualErr == nil {
				facts, _ = schema.CompareSchemaSnapshots(expectedSnapshot, actualSnapshot)
			}
		}
		t.Fatalf(
			"apply SQL Server recovered evolution suffix: %v; catalog-read=%v facts=%#v expected-reservations=%#v actual-reservations=%#v",
			err,
			readErr,
			facts,
			resumed.reservations,
			actual.Reservations(),
		)
	}
	complete, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || !complete.Complete() || complete.AppliedPrefix() != plan.OperationCount() {
		t.Fatalf("SQL Server complete preflight prefix=%d complete=%t err=%v", complete.AppliedPrefix(), complete.Complete(), err)
	}
	var valueResult string
	var relaxed int64
	var added sql.NullString
	if err := adapter.database.QueryRowContext(ctx,
		"SELECT [value], [must_relax], [added] FROM "+qualified+" WHERE [id] = 1",
	).Scan(&valueResult, &relaxed, &added); err != nil {
		t.Fatalf("read retained SQL Server row: %v", err)
	}
	if valueResult != "retained" || relaxed != 9 || added.Valid {
		t.Fatalf("retained SQL Server row = value:%q relaxed:%d added:%#v", valueResult, relaxed, added)
	}
	finalCatalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read final SQL Server catalog: %v", err)
	}
	priorUntouched := findTargetSchemaEvolutionTable(prior, targetSchemaEvolutionTableKey{schema: endpoint.Schema, table: untouchedName})
	finalUntouched := findTargetSchemaEvolutionTable(finalCatalog.Tables(), targetSchemaEvolutionTableKey{schema: endpoint.Schema, table: untouchedName})
	if priorUntouched < 0 || finalUntouched < 0 || !reflect.DeepEqual(prior[priorUntouched], finalCatalog.Tables()[finalUntouched]) {
		t.Fatalf("SQL Server target-only table changed: before=%#v after=%#v", prior[priorUntouched], finalCatalog.Tables()[finalUntouched])
	}
	var untouchedCode string
	if err := adapter.database.QueryRowContext(ctx,
		"SELECT [code] FROM "+untouchedQualified+" WHERE [id] = 1",
	).Scan(&untouchedCode); err != nil || untouchedCode != "untouched" {
		t.Fatalf("SQL Server target-only row = %q err=%v", untouchedCode, err)
	}
	var childRows int
	if err := adapter.database.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+sqlServerQualified(endpoint.Schema, childName),
	).Scan(&childRows); err != nil || childRows != 0 {
		t.Fatalf("created SQL Server child rows = %d err=%v", childRows, err)
	}
}

func sqlServerTargetEvolutionLiveRequest(
	t *testing.T,
	prior []schema.Table,
	current []schema.Table,
	tableName string,
	childName string,
	reservations []TargetSchemaEvolutionNameReservation,
	planner TargetSchemaEvolutionCreatePlanner,
) TargetSchemaEvolutionRequest {
	t.Helper()
	if len(current) != len(prior)+1 {
		t.Fatal("live SQL Server target evolution request lacks exactly one authorized new table")
	}
	tableIndex := findTargetSchemaEvolutionTable(prior, targetSchemaEvolutionTableKey{
		schema: prior[0].Schema,
		table:  tableName,
	})
	if tableIndex < 0 {
		t.Fatalf("live SQL Server target evolution request omits table %s", tableName)
	}
	table := prior[tableIndex]
	evidence := json.RawMessage(`{}`)
	decisions := []boundTargetSchemaEvolutionDecision{
		{
			contract:     SchemaContractDecision{Entity: schema.SchemaContractTables, Mode: config.SchemaContractEvolve, ChangeKind: schema.SchemaDriftTableAdded, Object: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectTable, Schema: table.Schema, Table: childName}, Previous: evidence, Current: evidence, Action: SchemaContractCreateTable, Reason: "live complete target table"},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectTable, Schema: table.Schema, Table: childName},
		},
		{
			contract:     SchemaContractDecision{Entity: schema.SchemaContractColumns, Mode: config.SchemaContractEvolve, ChangeKind: schema.SchemaDriftColumnAdded, Object: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectColumn, Schema: table.Schema, Table: table.Name, Column: "added"}, Previous: evidence, Current: evidence, Action: SchemaContractAddColumn, Reason: "live nullable column"},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectColumn, Schema: table.Schema, Table: table.Name, Column: "added"},
		},
		{
			contract:     SchemaContractDecision{Entity: schema.SchemaContractColumns, Mode: config.SchemaContractEvolve, ChangeKind: schema.SchemaDriftNullabilityChanged, Object: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectNullability, Schema: table.Schema, Table: table.Name, Column: "must_relax"}, Previous: evidence, Current: evidence, Action: SchemaContractRelaxNullability, Reason: "live nullability relaxation"},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectNullability, Schema: table.Schema, Table: table.Name, Column: "must_relax"},
		},
		{
			contract:     SchemaContractDecision{Entity: schema.SchemaContractDataType, Mode: config.SchemaContractEvolve, ChangeKind: schema.SchemaDriftDataTypeChanged, Object: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectDataType, Schema: table.Schema, Table: table.Name, Column: "value"}, Previous: evidence, Current: evidence, Action: SchemaContractWidenType, Reason: "live safe varchar widening"},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectDataType, Schema: table.Schema, Table: table.Name, Column: "value"},
		},
	}
	request := TargetSchemaEvolutionRequest{
		target: schema.SQLServer, sourceEngine: "mssql", targetEngine: "mssql", targetMode: "upsert",
		sourcePrior: "live-prior", sourceCurrent: "live-current", projectionPrior: "live-target-prior", projectionNext: "live-target-current",
		targetAuthorityTopology: "live-topology", targetAuthorityCatalog: "live-catalog",
		targetAuthorityReservations: cloneTargetSchemaEvolutionReservations(reservations),
		decisions:                   decisions, priorTables: cloneTargetSchemaEvolutionTables(prior), currentTables: cloneTargetSchemaEvolutionTables(current), createPlanner: planner,
	}
	digest, err := digestTargetSchemaEvolutionAuthority(request)
	if err != nil {
		t.Fatalf("digest live SQL Server target evolution authority: %v", err)
	}
	request.authorityDigest = digest
	return request
}

func sqlServerTargetEvolutionLiveChildTable(
	namespace string,
	childName string,
	parentName string,
) (schema.Table, error) {
	// SQL Server catalog reconstruction deliberately rejects text comparisons
	// because their collation semantics are not portable. Keep this planned
	// CHECK numeric so the live route proves a certified round-trippable object.
	check, err := schema.ParseSQLiteCheckExpression(`id > 0`)
	if err != nil {
		return schema.Table{}, err
	}
	return schema.Table{
		Schema: namespace,
		Name:   childName,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1, DeclaredType: &schema.DeclaredType{Base: "bigint"}},
			{Name: "parent_id", Type: "bigint", DeclaredType: &schema.DeclaredType{Base: "bigint"}},
			{Name: "code", Type: "text", Nullable: true, DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}}},
		},
		Indexes:     []schema.Index{{Name: childName + "_code_idx", Columns: []schema.IndexColumn{{Name: "code"}}}},
		Checks:      []schema.CheckConstraint{{Name: childName + "_code_check", Expression: check}},
		ForeignKeys: []schema.ForeignKey{{Name: childName + "_parent_fk", Columns: []string{"parent_id"}, ReferencedSchema: namespace, ReferencedTable: parentName, ReferencedColumns: []string{"id"}, OnUpdate: "NO ACTION", OnDelete: "NO ACTION", Match: "NONE"}},
	}, nil
}
