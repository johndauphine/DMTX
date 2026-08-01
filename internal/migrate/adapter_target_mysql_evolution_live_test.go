package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestMySQLTargetSchemaEvolutionLive(t *testing.T) {
	for _, fixture := range []mysqlTargetEvolutionLiveFixture{
		{
			name: "mysql80", dsnEnv: "DMTX_TEST_MYSQL_TARGET_DSN",
			adminDSNEnv: "DMTX_TEST_MYSQL_ADMIN_DSN", caEnv: "DMTX_TEST_MYSQL_CA", tlsConfig: "dmtx_test",
			collation: "utf8mb4_0900_bin", refreshInfo: true,
		},
		{
			name: "mariadb1011", dsnEnv: "DMTX_TEST_MARIADB_TARGET_DSN",
			adminDSNEnv: "DMTX_TEST_MARIADB_ADMIN_DSN", caEnv: "DMTX_TEST_MARIADB_CA", tlsConfig: "dmtx_mariadb_test",
			collation: "utf8mb4_nopad_bin",
		},
	} {
		fixture := fixture
		var cleanup mysqlTargetEvolutionLiveCleanupEvidence
		t.Run(fixture.name, func(t *testing.T) {
			testMySQLTargetSchemaEvolutionLive(t, fixture, &cleanup)
		})
		if cleanup.granted {
			assertMySQLTargetEvolutionLiveGrantRemoved(
				t,
				fixture,
				cleanup,
			)
		}
	}
}

// TestStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS proves that the
// composed Stage 4 upsert runner reaches the production MySQL-family target
// capability—not a test seam—before its first data write. MySQL 8.0 and
// MariaDB 10.11 have separate catalog/DDL implementations and therefore run
// as separate cells. The direct native sentinel separately injects a
// post-DDL/pre-ack loss and proves each target's exact-prefix recovery
// protocol.
func TestStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS(t *testing.T) {
	fixtures := []mysqlTargetEvolutionLiveFixture{
		{
			name: "mysql80", dsnEnv: "DMTX_TEST_MYSQL_TARGET_DSN",
			adminDSNEnv: "DMTX_TEST_MYSQL_ADMIN_DSN", caEnv: "DMTX_TEST_MYSQL_CA", tlsConfig: "dmtx_test",
			collation: "utf8mb4_0900_bin", refreshInfo: true,
		},
		{
			name: "mariadb1011", dsnEnv: "DMTX_TEST_MARIADB_TARGET_DSN",
			adminDSNEnv: "DMTX_TEST_MARIADB_ADMIN_DSN", caEnv: "DMTX_TEST_MARIADB_CA", tlsConfig: "dmtx_mariadb_test",
			collation: "utf8mb4_nopad_bin",
		},
	}
	for _, fixture := range fixtures {
		fixture := fixture
		var cleanup mysqlTargetEvolutionLiveCleanupEvidence
		t.Run(fixture.name, func(t *testing.T) {
			testStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS(
				t,
				fixture,
				&cleanup,
			)
		})
		if cleanup.granted {
			assertMySQLTargetEvolutionLiveGrantRemoved(t, fixture, cleanup)
		}
	}
}

func testStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS(
	t *testing.T,
	fixture mysqlTargetEvolutionLiveFixture,
	cleanup *mysqlTargetEvolutionLiveCleanupEvidence,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	adminDSN := os.Getenv(fixture.adminDSNEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || adminDSN == "" || caPath == "" {
		t.Skip("set " + fixture.dsnEnv + ", " + fixture.adminDSNEnv + ", and " + fixture.caEnv + " to run the composed " + fixture.name + " Stage 4 evolution sentinel")
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"composed "+fixture.name+" Stage 4 evolution target",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, endpoint := mysqlTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		fixture,
		parsed,
		adminDSN,
		caPath,
		cleanup,
	)
	opened, err := openMySQLTargetAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open composed %s Stage 4 target: %v", fixture.name, err)
	}
	target, ok := opened.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("composed %s Stage 4 target = %T, want *mysqlTargetAdapter", fixture.name, opened)
	}
	t.Cleanup(func() { _ = target.database.Close() })

	table := stage4AdapterTestTable()
	table.Name = "dmtx_evo_route_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var before int
	if err := target.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM information_schema.tables
		  WHERE table_schema = ? AND table_name = ?`,
		endpoint.Database,
		table.Name,
	).Scan(&before); err != nil {
		t.Fatalf("read composed %s pre-run catalog: %v", fixture.name, err)
	}
	if before != 0 {
		t.Fatalf("composed %s evolution table %s unexpectedly exists before the run", fixture.name, table.Name)
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
	runID := "stage4-" + fixture.name + "-evolution-live-" + table.Name
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
		t.Fatalf("run composed %s Stage 4 schema evolution: %v", fixture.name, err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("composed %s Stage 4 result = %#v", fixture.name, result)
	}
	var rows int
	if err := target.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(table.Name),
	).Scan(&rows); err != nil {
		t.Fatalf("count composed %s evolved rows: %v", fixture.name, err)
	}
	if rows != 2 {
		t.Fatalf("composed %s evolved rows = %d, want 2", fixture.name, rows)
	}
	catalog, err := target.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read composed %s evolved catalog: %v", fixture.name, err)
	}
	if findTargetSchemaEvolutionTable(catalog.Tables(), targetSchemaEvolutionTableKey{
		schema: endpoint.Database,
		table:  table.Name,
	}) < 0 {
		t.Fatalf("composed %s catalog omits evolved table %s", fixture.name, table.Name)
	}
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	// PublishStage4RunCompletion owns the terminal transition for these
	// aggregate sentinels. The composed runner validates schema/data first and
	// deliberately leaves them running until the app publishes the outcome.
	for _, key := range []state.TaskKey{stage4SchemaGateTask, stage4TargetShapeTask} {
		pending := false
		for _, task := range tasks {
			if task.Key == key && task.Status == "running" {
				pending = true
			}
		}
		if !pending {
			t.Fatalf("composed %s evolution did not leave %#v pending aggregate publication: %#v", fixture.name, key, tasks)
		}
	}
}

type mysqlTargetEvolutionLiveFixture struct {
	name        string
	dsnEnv      string
	adminDSNEnv string
	caEnv       string
	tlsConfig   string
	collation   string
	refreshInfo bool
}

// mysqlTargetEvolutionLiveCleanupEvidence crosses the child-test cleanup
// boundary so its parent can prove a temporary database grant is gone only
// after the child's t.Cleanup callbacks have run.
type mysqlTargetEvolutionLiveCleanupEvidence struct {
	databaseName string
	account      string
	granted      bool
}

// mysqlTargetEvolutionLiveTemporaryDatabase prevents a complete-catalog test
// from sharing dmtx_target with other live sentinels. The production catalog
// reader deliberately inspects every base table in its configured database,
// including target-only tables, and fails closed when one cannot be rendered
// as exact target authority. A unique database is therefore part of this
// sentinel's proof, not a shortcut around that product policy.
func mysqlTargetEvolutionLiveTemporaryDatabase(
	t *testing.T,
	ctx context.Context,
	fixture mysqlTargetEvolutionLiveFixture,
	target *mysqlDriver.Config,
	adminDSN string,
	caPath string,
	cleanup *mysqlTargetEvolutionLiveCleanupEvidence,
) (string, config.Endpoint) {
	t.Helper()
	admin := parseMySQLNativeTargetDSNForTLS(
		t,
		fixture.name+" target-evolution administrator",
		adminDSN,
		fixture.tlsConfig,
	)
	if admin.Addr != target.Addr {
		t.Fatalf(
			"%s administrator address %q differs from target address %q",
			fixture.name,
			admin.Addr,
			target.Addr,
		)
	}
	administrator, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatalf("open %s target-evolution administrator: %v", fixture.name, err)
	}
	t.Cleanup(func() { _ = administrator.Close() })
	if err := administrator.PingContext(ctx); err != nil {
		t.Fatalf("ping %s target-evolution administrator: %v", fixture.name, err)
	}

	databaseName := fmt.Sprintf(
		"dmtx_s4_evo_%s_%s",
		fixture.name,
		strconv.FormatInt(time.Now().UnixNano(), 36),
	)
	if _, err := administrator.ExecContext(
		ctx,
		"CREATE DATABASE "+mySQLIdentifier(databaseName)+
			" DEFAULT CHARACTER SET=utf8mb4 COLLATE="+fixture.collation,
	); err != nil {
		t.Fatalf("create isolated %s target-evolution database: %v", fixture.name, err)
	}
	account := mysqlTargetEvolutionLiveCurrentAccount(t, ctx, target)
	if cleanup != nil {
		cleanup.databaseName = databaseName
		cleanup.account = account
	}
	granted := false
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if granted {
			if _, err := administrator.ExecContext(
				cleanupCtx,
				"REVOKE ALL PRIVILEGES ON "+mySQLIdentifier(databaseName)+".* FROM "+account,
			); err != nil {
				t.Errorf("revoke isolated %s target-evolution database grant: %v", fixture.name, err)
			}
		}
		if _, err := administrator.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+mySQLIdentifier(databaseName),
		); err != nil {
			t.Errorf("drop isolated %s target-evolution database: %v", fixture.name, err)
		}
	})
	if _, err := administrator.ExecContext(
		ctx,
		"GRANT ALL PRIVILEGES ON "+mySQLIdentifier(databaseName)+".* TO "+account,
	); err != nil {
		t.Fatalf("grant isolated %s target-evolution database: %v", fixture.name, err)
	}
	granted = true
	if cleanup != nil {
		cleanup.granted = true
	}
	targetCopy := *target
	targetCopy.DBName = databaseName
	return targetCopy.FormatDSN(), mysqlNativeTargetEndpoint(t, &targetCopy, caPath)
}

func assertMySQLTargetEvolutionLiveGrantRemoved(
	t *testing.T,
	fixture mysqlTargetEvolutionLiveFixture,
	cleanup mysqlTargetEvolutionLiveCleanupEvidence,
) {
	t.Helper()
	if cleanup.databaseName == "" || cleanup.account == "" {
		t.Fatalf("%s cleanup evidence is incomplete: %#v", fixture.name, cleanup)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	database, err := sql.Open("mysql", os.Getenv(fixture.adminDSNEnv))
	if err != nil {
		t.Fatalf("open %s grant-cleanup verifier: %v", fixture.name, err)
	}
	defer database.Close()
	var grants int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM information_schema.SCHEMA_PRIVILEGES
		  WHERE TABLE_SCHEMA = ? AND GRANTEE = ?`,
		cleanup.databaseName,
		cleanup.account,
	).Scan(&grants); err != nil {
		t.Fatalf("read %s temporary-database grant cleanup: %v", fixture.name, err)
	}
	if grants != 0 {
		t.Fatalf(
			"%s temporary database %s still has %d schema grant(s) for %s after child cleanup",
			fixture.name,
			cleanup.databaseName,
			grants,
			cleanup.account,
		)
	}
}

func mysqlTargetEvolutionLiveCurrentAccount(
	t *testing.T,
	ctx context.Context,
	target *mysqlDriver.Config,
) string {
	t.Helper()
	database, err := sql.Open("mysql", target.FormatDSN())
	if err != nil {
		t.Fatalf("open target-evolution account probe: %v", err)
	}
	defer database.Close()
	var current string
	if err := database.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&current); err != nil {
		t.Fatalf("read target-evolution current account: %v", err)
	}
	separator := strings.LastIndex(current, "@")
	if separator <= 0 || separator == len(current)-1 {
		t.Fatalf("target-evolution current account %q is not user@host", current)
	}
	user, host := current[:separator], current[separator+1:]
	return "'" + strings.ReplaceAll(user, "'", "''") + "'@'" +
		strings.ReplaceAll(host, "'", "''") + "'"
}

func testMySQLTargetSchemaEvolutionLive(
	t *testing.T,
	fixture mysqlTargetEvolutionLiveFixture,
	cleanup *mysqlTargetEvolutionLiveCleanupEvidence,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	adminDSN := os.Getenv(fixture.adminDSNEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || adminDSN == "" || caPath == "" {
		t.Skip("set " + fixture.dsnEnv + ", " + fixture.adminDSNEnv + ", and " + fixture.caEnv + " to run the " + fixture.name + " target-evolution sentinel")
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(t, "target evolution", dsn, fixture.tlsConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	temporaryDSN, endpoint := mysqlTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		fixture,
		parsed,
		adminDSN,
		caPath,
		cleanup,
	)
	database := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		"target evolution",
		temporaryDSN,
		fixture.refreshInfo,
	)
	t.Cleanup(func() { _ = database.Close() })
	tableName := "dmtx_evo_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	childName := tableName + "_child"
	untouchedName := tableName + "_untouched"
	if _, err := database.ExecContext(ctx, "CREATE TABLE "+mySQLIdentifier(tableName)+` (
		id BIGINT NOT NULL,
		value VARCHAR(10) NOT NULL,
		must_relax INT NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=`+fixture.collation+` ROW_FORMAT=DYNAMIC`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(tableName)+" (id, value, must_relax) VALUES (1, 'retained', 9)"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "CREATE TABLE "+mySQLIdentifier(untouchedName)+` (
		id BIGINT NOT NULL,
		code VARCHAR(16) NOT NULL,
		PRIMARY KEY (id),
		KEY `+mySQLIdentifier(untouchedName+"_code_idx")+` (code),
		CONSTRAINT `+mySQLIdentifier(untouchedName+"_code_check")+` CHECK (id > 0)
	) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=`+fixture.collation+` ROW_FORMAT=DYNAMIC`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(untouchedName)+" (id, code) VALUES (1, 'untouched')"); err != nil {
		t.Fatal(err)
	}
	untouchedBefore := mysqlTargetEvolutionLiveShowCreate(t, ctx, database, untouchedName)

	opened, err := openMySQLTargetAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open %s target adapter: %v", fixture.name, err)
	}
	adapter, ok := opened.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("target adapter type = %T", opened)
	}
	t.Cleanup(func() { _ = adapter.database.Close() })
	priorCatalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read %s exact prior catalog: %v", fixture.name, err)
	}
	prior := priorCatalog.Tables()
	current := cloneTargetSchemaEvolutionTables(prior)
	tableIndex := findTargetSchemaEvolutionTable(current, targetSchemaEvolutionTableKey{
		schema: endpoint.Database,
		table:  tableName,
	})
	if tableIndex < 0 {
		t.Fatalf("prior %s catalog omits %s: %#v", fixture.name, tableName, prior)
	}
	value := findTargetSchemaEvolutionColumnIndex(current[tableIndex], "value")
	relax := findTargetSchemaEvolutionColumnIndex(current[tableIndex], "must_relax")
	if value < 0 || relax < 0 {
		t.Fatalf("prior %s columns are incomplete: %#v", fixture.name, current[tableIndex].Columns)
	}
	current[tableIndex].Columns[value].DeclaredType = &schema.DeclaredType{
		Base: "varchar", Arguments: []int{32},
	}
	current[tableIndex].Columns[relax].Nullable = true
	current[tableIndex].Columns = append(current[tableIndex].Columns, schema.Column{
		Name: "added", Type: "varchar", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}},
	})
	child, err := mysqlTargetEvolutionLiveChildTable(
		endpoint.Database,
		fixture.collation,
		childName,
		tableName,
	)
	if err != nil {
		t.Fatal(err)
	}
	current = append(current, child)
	sortTargetSchemaEvolutionTables(current)
	request := mysqlTargetEvolutionLiveRequest(
		t,
		prior,
		current,
		tableName,
		childName,
		priorCatalog.Reservations(),
		adapter.TargetSchemaEvolutionCreatePlanner(),
	)
	plan, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || plan.OperationCount() != 8 || plan.AppliedPrefix() != 0 {
		t.Fatalf("%s preflight plan=%#v err=%v", fixture.name, plan, err)
	}

	// Simulate a process loss after the first auto-committing ALTER returned
	// success but before DMTX could acknowledge progress. A new preflight must
	// authenticate that exact prefix and execute only the remaining suffix.
	first := plan.PendingOperations()[0].Statements()
	if len(first) != 1 {
		t.Fatalf("%s first operation statements = %#v", fixture.name, first)
	}
	if _, err := database.ExecContext(ctx, first[0]); err != nil {
		t.Fatalf("simulate %s post-DDL/pre-ack loss: %v", fixture.name, err)
	}
	resumed, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || resumed.AppliedPrefix() != 1 || resumed.Complete() {
		actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
		var facts []schema.SchemaDriftFact
		if readErr == nil {
			expectedSnapshot, expectedErr := schema.NewSchemaSnapshot(plan.states[1])
			actualSnapshot, actualErr := schema.NewSchemaSnapshot(actual.Tables())
			if expectedErr == nil && actualErr == nil {
				facts, _ = schema.CompareSchemaSnapshots(expectedSnapshot, actualSnapshot)
			}
		}
		t.Fatalf(
			"%s recovered preflight prefix=%d complete=%t err=%v catalog-read=%v facts=%#v",
			fixture.name,
			resumed.AppliedPrefix(),
			resumed.Complete(),
			err,
			readErr,
			facts,
		)
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, resumed); err != nil {
		t.Fatalf("apply %s recovered evolution suffix: %v", fixture.name, err)
	}
	complete, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil || !complete.Complete() || complete.AppliedPrefix() != 8 {
		t.Fatalf("%s complete preflight prefix=%d complete=%t err=%v", fixture.name, complete.AppliedPrefix(), complete.Complete(), err)
	}
	var valueResult string
	var relaxed int64
	var added sql.NullString
	if err := database.QueryRowContext(ctx, "SELECT value, must_relax, added FROM "+mySQLIdentifier(tableName)+" WHERE id = 1").Scan(&valueResult, &relaxed, &added); err != nil {
		t.Fatalf("read retained %s row: %v", fixture.name, err)
	}
	if valueResult != "retained" || relaxed != 9 || added.Valid {
		t.Fatalf("retained %s row = value:%q relaxed:%#v added:%#v", fixture.name, valueResult, relaxed, added)
	}
	var childCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLIdentifier(childName)).Scan(&childCount); err != nil || childCount != 0 {
		t.Fatalf("created %s child count=%d err=%v", fixture.name, childCount, err)
	}
	untouchedAfter := mysqlTargetEvolutionLiveShowCreate(t, ctx, database, untouchedName)
	if untouchedAfter != untouchedBefore {
		t.Fatalf("target-only %s table changed during evolution:\nbefore=%s\nafter=%s", fixture.name, untouchedBefore, untouchedAfter)
	}
	var untouchedCode string
	if err := database.QueryRowContext(ctx, "SELECT code FROM "+mySQLIdentifier(untouchedName)+" WHERE id = 1").Scan(&untouchedCode); err != nil || untouchedCode != "untouched" {
		t.Fatalf("target-only %s row = %q err=%v", fixture.name, untouchedCode, err)
	}
}

func mysqlTargetEvolutionLiveRequest(
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
		t.Fatal("live target evolution request does not contain exactly one authorized new table")
	}
	tableIndex := findTargetSchemaEvolutionTable(prior, targetSchemaEvolutionTableKey{
		schema: prior[0].Schema,
		table:  tableName,
	})
	if tableIndex < 0 {
		t.Fatalf("live target evolution request omits table %s", tableName)
	}
	table := prior[tableIndex]
	evidence := json.RawMessage(`{}`)
	decisions := []boundTargetSchemaEvolutionDecision{
		{
			contract: SchemaContractDecision{
				Entity: schema.SchemaContractTables, Mode: config.SchemaContractEvolve,
				ChangeKind: schema.SchemaDriftTableAdded,
				Object: schema.SchemaDriftObject{
					Kind: schema.SchemaDriftObjectTable, Schema: table.Schema, Table: childName,
				},
				Previous: evidence, Current: evidence, Action: SchemaContractCreateTable, Reason: "live complete target table",
			},
			targetObject: schema.SchemaDriftObject{
				Kind: schema.SchemaDriftObjectTable, Schema: table.Schema, Table: childName,
			},
		},
		{
			contract: SchemaContractDecision{
				Entity: schema.SchemaContractColumns, Mode: config.SchemaContractEvolve,
				ChangeKind: schema.SchemaDriftColumnAdded,
				Object:     schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectColumn, Schema: table.Schema, Table: table.Name, Column: "added"},
				Previous:   evidence, Current: evidence, Action: SchemaContractAddColumn, Reason: "live nullable column",
			},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectColumn, Schema: table.Schema, Table: table.Name, Column: "added"},
		},
		{
			contract: SchemaContractDecision{
				Entity: schema.SchemaContractColumns, Mode: config.SchemaContractEvolve,
				ChangeKind: schema.SchemaDriftNullabilityChanged,
				Object:     schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectNullability, Schema: table.Schema, Table: table.Name, Column: "must_relax"},
				Previous:   evidence, Current: evidence, Action: SchemaContractRelaxNullability, Reason: "live nullability relaxation",
			},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectNullability, Schema: table.Schema, Table: table.Name, Column: "must_relax"},
		},
		{
			contract: SchemaContractDecision{
				Entity: schema.SchemaContractDataType, Mode: config.SchemaContractEvolve,
				ChangeKind: schema.SchemaDriftDataTypeChanged,
				Object:     schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectDataType, Schema: table.Schema, Table: table.Name, Column: "value"},
				Previous:   evidence, Current: evidence, Action: SchemaContractWidenType, Reason: "live safe varchar widening",
			},
			targetObject: schema.SchemaDriftObject{Kind: schema.SchemaDriftObjectDataType, Schema: table.Schema, Table: table.Name, Column: "value"},
		},
	}
	request := TargetSchemaEvolutionRequest{
		target: schema.MySQL, sourceEngine: "mysql", targetEngine: "mysql", targetMode: "upsert",
		sourcePrior: "live-prior", sourceCurrent: "live-current", projectionPrior: "live-target-prior", projectionNext: "live-target-current",
		targetAuthorityTopology: "live-topology", targetAuthorityCatalog: "live-catalog",
		targetAuthorityReservations: cloneTargetSchemaEvolutionReservations(reservations),
		decisions:                   decisions, priorTables: cloneTargetSchemaEvolutionTables(prior), currentTables: cloneTargetSchemaEvolutionTables(current), createPlanner: planner,
	}
	digest, err := digestTargetSchemaEvolutionAuthority(request)
	if err != nil {
		t.Fatalf("digest live MySQL target evolution authority: %v", err)
	}
	request.authorityDigest = digest
	return request
}

func mysqlTargetEvolutionLiveChildTable(
	namespace string,
	collation string,
	childName string,
	parentName string,
) (schema.Table, error) {
	check, err := schema.ParseSQLiteCheckExpression(`code <> ''`)
	if err != nil {
		return schema.Table{}, err
	}
	child := schema.Table{
		Schema:         namespace,
		Name:           childName,
		MySQLCollation: collation,
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "parent_id", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "code", Type: "varchar", Nullable: true,
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}},
			},
		},
		Indexes: []schema.Index{{
			Name: childName + "_code_idx", Columns: []schema.IndexColumn{{
				Name: "code", Collation: "BINARY",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name: childName + "_code_check", Expression: check,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name: childName + "_parent_fk", Columns: []string{"parent_id"},
			ReferencedSchema: namespace, ReferencedTable: parentName,
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION", OnDelete: "NO ACTION", Match: "NONE",
		}},
	}
	child, err = schema.AddMySQLForeignKeyIndexes(child)
	if err != nil {
		return schema.Table{}, err
	}
	if err := canonicalizeMySQLTargetChecks(&child); err != nil {
		return schema.Table{}, err
	}
	return child, nil
}

func mysqlTargetEvolutionLiveShowCreate(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	tableName string,
) string {
	t.Helper()
	var returnedName, statement string
	if err := database.QueryRowContext(ctx, "SHOW CREATE TABLE "+mySQLIdentifier(tableName)).Scan(&returnedName, &statement); err != nil {
		t.Fatalf("show create %s: %v", tableName, err)
	}
	if returnedName != tableName {
		t.Fatalf("show create returned table %q, want %q", returnedName, tableName)
	}
	return statement
}
