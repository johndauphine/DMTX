package migrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type mysqlStrictLiveFixture struct {
	name      string
	dsnEnv    string
	caEnv     string
	tlsName   string
	engine    StrictConsistencyEngine
	sslServer string
	collation string
}

// mysqlStrictPostgresLiveTarget is a test-only forwarding target that keeps
// this strict-composition sentinel out of independently tested target-schema
// evolution. It retains the real PostgreSQL planner, writer, counts, replay
// isolation, and network upsert path against the real database.
type mysqlStrictPostgresLiveTarget struct{ target *postgresTargetAdapter }

func (target *mysqlStrictPostgresLiveTarget) Engine() string { return target.target.Engine() }
func (target *mysqlStrictPostgresLiveTarget) PlanTables(source string, tables []schema.Table, mode string) ([]schema.Table, error) {
	return target.target.PlanTables(source, tables, mode)
}
func (target *mysqlStrictPostgresLiveTarget) PreflightTables(ctx context.Context, tables []schema.Table, mode string) error {
	return target.target.PreflightTables(ctx, tables, mode)
}
func (target *mysqlStrictPostgresLiveTarget) PrepareTables(ctx context.Context, tables []schema.Table, mode string) error {
	return target.target.PrepareTables(ctx, tables, mode)
}
func (target *mysqlStrictPostgresLiveTarget) WriteBatch(ctx context.Context, table schema.Table, columns []string, mode string, rows [][]any) (WriteReceipt, error) {
	return target.target.WriteBatch(ctx, table, columns, mode, rows)
}
func (target *mysqlStrictPostgresLiveTarget) CountRows(ctx context.Context, table schema.Table) (int, error) {
	return target.target.CountRows(ctx, table)
}
func (target *mysqlStrictPostgresLiveTarget) FinalizeTables(ctx context.Context, tables []schema.Table, mode string) error {
	return target.target.FinalizeTables(ctx, tables, mode)
}
func (target *mysqlStrictPostgresLiveTarget) Close() error                         { return target.target.Close() }
func (target *mysqlStrictPostgresLiveTarget) stage4NetworkIdempotentUpsertTarget() {}
func (target *mysqlStrictPostgresLiveTarget) WriteStage4NetworkBatch(ctx context.Context, table schema.Table, columns []string, rows [][]any) (WriteReceipt, error) {
	return target.target.WriteStage4NetworkBatch(ctx, table, columns, rows)
}
func (target *mysqlStrictPostgresLiveTarget) PreflightStage4NetworkReplayIsolation(ctx context.Context, tables []schema.Table) error {
	return target.target.PreflightStage4NetworkReplayIsolation(ctx, tables)
}

// TestStage4MySQLFamilyStrictComposedPostgresLiveTLS proves the aggregate
// runner, rather than just the primitive: a source commit made after durable
// strict evidence is intentionally absent from the target and validation uses
// the retained MySQL/MariaDB transaction view.
func TestStage4MySQLFamilyStrictComposedPostgresLiveTLS(t *testing.T) {
	for _, fixture := range []mysqlStrictLiveFixture{
		{name: "MySQL", dsnEnv: "DMTX_TEST_MYSQL_DSN", caEnv: "DMTX_TEST_MYSQL_CA", tlsName: "dmtx_test", engine: StrictConsistencyMySQL, sslServer: "localhost", collation: "utf8mb4_0900_bin"},
		{name: "MariaDB", dsnEnv: "DMTX_TEST_MARIADB_DSN", caEnv: "DMTX_TEST_MARIADB_CA", tlsName: "dmtx_mariadb_test", engine: StrictConsistencyMariaDB, sslServer: "localhost", collation: "utf8mb4_nopad_bin"},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) { testStage4MySQLFamilyStrictComposedPostgresLiveTLS(t, fixture) })
	}
}

func testStage4MySQLFamilyStrictComposedPostgresLiveTLS(t *testing.T, fixture mysqlStrictLiveFixture) {
	t.Helper()
	if os.Getenv("DMTX_TEST_POSTGRES_DSN") == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN and MySQL-family TLS variables to run this composed strict sentinel")
	}
	sourceDB, sourceNamespace, table := openMySQLStrictLiveSource(t, fixture)
	pgDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	pgConfig, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(pgConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	targetCAFile := stage4PostgresDeleteLiveCAFile(t, pgConfig.ConnString())
	targetDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := targetDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL target: %v", err)
	}
	targetNamespace := "dmtx_mysql_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := targetDB.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(targetNamespace)); err != nil {
		t.Fatalf("create target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := targetDB.ExecContext(cleanup, "DROP SCHEMA IF EXISTS "+postgresIdentifier(targetNamespace)+" CASCADE"); err != nil {
			t.Errorf("drop target schema: %v", err)
		}
	})
	if _, err := targetDB.ExecContext(ctx, "CREATE TABLE "+postgresQualified(targetNamespace, table)+" (id bigint PRIMARY KEY, payload character varying(40) NOT NULL)"); err != nil {
		t.Fatalf("create strict target table: %v", err)
	}
	flavor := engine.MySQLServerFlavorOracle80
	if fixture.engine == StrictConsistencyMariaDB {
		flavor = engine.MySQLServerFlavorMariaDB1011
	}
	source := &relationalSourceAdapter{spec: relationalSourceSpec{engine: "mysql", displayName: fixture.name, listTables: engine.ListMySQLTables, inspectTable: engine.InspectMySQLTable, readQuery: mySQLReadQuery, qualifiedTable: mySQLQualified, wrapRows: wrapMySQLSourceRows, preflightRows: preflightMySQLSourceRows}, database: sourceDB, namespace: sourceNamespace, mySQLFlavor: flavor}
	target := &mysqlStrictPostgresLiveTarget{target: &postgresTargetAdapter{database: targetDB, batchWriter: newPostgresNativeWriter(targetDB), namespace: targetNamespace}}
	raw := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "mysql-family-strict-" + strings.ToLower(fixture.name) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := raw.InitializeRun(state.Run{
		ID: runID, Source: "source", Target: "target", SourceEngine: "mysql",
		SourceIdentity: "mysql:strict-live", TargetIdentity: "postgres:strict-live",
		Outcome: state.Running, Resumable: true, Reason: "running", StartedAt: time.Now().Add(-time.Minute),
	}, "configuration-"+runID); err != nil {
		t.Fatalf("initialize MySQL-family strict live run: %v", err)
	}
	quoted := "`" + sourceNamespace + "`.`" + table + "`"
	backend := &stage4PostgresStrictMutationBackend{Stage4StateBackend: raw, mutate: func() error {
		_, err := sourceDB.ExecContext(ctx, "INSERT INTO "+quoted+" (id,payload) VALUES (99,'after-view')")
		return err
	}}
	events := make([]string, 0)
	observer := stage4AdapterObserver{recordingTableObserver: recordingTableObserver{events: &events}, run: stage4LifecycleRunContext(t, backend, runID, false)}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	sourceType := "mysql"
	if fixture.engine == StrictConsistencyMariaDB {
		sourceType = "mariadb"
	}
	cfg.Source = config.Endpoint{Type: sourceType, Host: "localhost", Port: 3306, Database: sourceNamespace, User: "dmtx", Schema: sourceNamespace, SSLMode: "verify-full", TLSCAFile: os.Getenv(fixture.caEnv)}
	cfg.Target = config.Endpoint{Type: "postgres", Host: pgConfig.Host, Port: int(pgConfig.Port), Database: pgConfig.Database, User: pgConfig.User, Password: pgConfig.Password, Schema: targetNamespace, SSLMode: "verify-full", TLSCAFile: targetCAFile}
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{table}
	cfg.Migration.Partitions = 2
	cfg.Migration.ReaderParallelism = 2
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	if cfg.Source.SSLMode != "verify-full" || cfg.Source.TLSCAFile == "" || cfg.Target.SSLMode != "verify-full" || cfg.Target.TLSCAFile != targetCAFile || cfg.Target.Password != pgConfig.Password {
		t.Fatal("composed endpoint lost verified-TLS authority")
	}
	result, err := migrateWithStage4Adapters(ctx, cfg, observer, source, target, "upsert", observer.run)
	if err != nil {
		t.Fatalf("run %s composed strict route: %v", fixture.name, err)
	}
	if result.Rows != 3 || !result.Validated {
		t.Fatalf("strict result = %#v", result)
	}
	var targetCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(targetNamespace, table)).Scan(&targetCount); err != nil {
		t.Fatal(err)
	}
	if targetCount != 3 {
		t.Fatalf("target count = %d, want retained strict count 3", targetCount)
	}
}

func openMySQLStrictLiveSource(
	t *testing.T,
	fixture mysqlStrictLiveFixture,
) (*sql.DB, string, string) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skipf(
			"set %s and %s to run the %s strict consistency route",
			fixture.dsnEnv,
			fixture.caEnv,
			fixture.name,
		)
	}
	registerMySQLFamilyStrictTLS(t, caPath, fixture.tlsName, fixture.sslServer)
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", fixture.dsnEnv, err)
	}
	if parsed.TLSConfig == "" || parsed.TLSConfig == "false" {
		t.Fatalf("%s must require TLS", fixture.dsnEnv)
	}
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s strict source: %v", fixture.name, err)
	}
	t.Cleanup(func() { _ = database.Close() })
	// The strict contract needs a lock holder plus readers, so the pool must
	// admit more than one connection.
	database.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping %s strict source: %v", fixture.name, err)
	}

	table := "dmtx_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quoted := "`" + parsed.DBName + "`.`" + table + "`"
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s (
			id BIGINT NOT NULL PRIMARY KEY,
			payload VARCHAR(40) CHARACTER SET utf8mb4 COLLATE %s NOT NULL
		) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=%s`,
		quoted, fixture.collation, fixture.collation,
	)); err != nil {
		t.Fatalf("create %s strict fixture: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+quoted,
		); err != nil {
			t.Errorf("drop %s strict fixture: %v", fixture.name, err)
		}
	})
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (1,'a'),(2,'b'),(3,'c')`,
		quoted,
	)); err != nil {
		t.Fatalf("seed %s strict fixture: %v", fixture.name, err)
	}
	return database, parsed.DBName, table
}

// testMySQLFamilyStrictTableSnapshotLive is the Section 10 MySQL/MariaDB
// contract proven against a real server: parallel InnoDB repeatable-read
// sessions pinned by a brief LOCK TABLES, and — the decisive property — a
// commit landing after the view opens must remain invisible to it.
func testMySQLFamilyStrictTableSnapshotLive(
	t *testing.T,
	fixture mysqlStrictLiveFixture,
) {
	source, namespace, table := openMySQLStrictLiveSource(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opener, err := NewMySQLStrictConsistencyOpener(
		source,
		namespace,
		fixture.engine,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	task := state.TaskKey{Type: "table-copy", Table: table}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mysql-strict-live",
		SourceEngine: fixture.engine,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      task,
			AttemptID: "attempt-1",
		}},
	})
	if err != nil {
		t.Fatalf("open %s strict view: %v", fixture.name, err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close %s strict view: %v", fixture.name, err)
		}
	}()

	// The lock must already be released: a writer committing now proves the
	// outage was brief, and its invisibility proves the snapshot holds.
	quoted := "`" + namespace + "`.`" + table + "`"
	if _, err := source.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (99,'after-view')`,
		quoted,
	)); err != nil {
		t.Fatalf(
			"%s strict view did not release its write lock: %v",
			fixture.name,
			err,
		)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		t.Fatalf("capture %s strict evidence: %v", fixture.name, err)
	}
	if len(capture.Tables) != 1 {
		t.Fatalf("%s capture = %#v", fixture.name, capture)
	}
	if capture.Tables[0].ExactSourceRowCount != 3 {
		t.Fatalf(
			"%s stable view saw %d rows; a later commit leaked in",
			fixture.name,
			capture.Tables[0].ExactSourceRowCount,
		)
	}
	if err := validateSnapshotReference(
		capture.Tables[0].SnapshotReference,
	); err != nil {
		t.Fatalf("%s snapshot reference rejected by the core: %v", fixture.name, err)
	}
	if capture.MigrationSnapshotReference != "" {
		t.Fatalf("%s table scope emitted migration evidence", fixture.name)
	}
}

// testMySQLFamilyStrictRejectsNonInnoDBLive proves the storage-engine gate
// against a real MyISAM table. MyISAM accepts every statement in the protocol
// while providing no snapshot at all, so refusing it is the difference between
// strict consistency and the appearance of it.
func testMySQLFamilyStrictRejectsNonInnoDBLive(
	t *testing.T,
	fixture mysqlStrictLiveFixture,
) {
	source, namespace, _ := openMySQLStrictLiveSource(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	myisam := "dmtx_strict_myisam_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	quoted := "`" + namespace + "`.`" + myisam + "`"
	if _, err := source.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY) ENGINE=MyISAM`,
		quoted,
	)); err != nil {
		t.Skipf("%s server refused a MyISAM fixture: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if _, err := source.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+quoted,
		); err != nil {
			t.Errorf("drop %s MyISAM fixture: %v", fixture.name, err)
		}
	})

	opener, err := NewMySQLStrictConsistencyOpener(
		source,
		namespace,
		fixture.engine,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mysql-strict-myisam",
		SourceEngine: fixture.engine,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      state.TaskKey{Type: "table-copy", Table: myisam},
			AttemptID: "attempt-1",
		}},
	})
	if session != nil {
		_ = session.Close(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "requires InnoDB") {
		t.Fatalf("%s MyISAM strict error = %v", fixture.name, err)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf(
			"%s MyISAM strict error class = %q",
			fixture.name,
			ClassifyTransferError(err),
		)
	}
}

func TestMySQLStrictTableSnapshotLive(t *testing.T) {
	testMySQLFamilyStrictTableSnapshotLive(t, mysqlStrictLiveFixture{
		name:      "MySQL",
		dsnEnv:    "DMTX_TEST_MYSQL_DSN",
		caEnv:     "DMTX_TEST_MYSQL_CA",
		tlsName:   "dmtx_test",
		engine:    StrictConsistencyMySQL,
		collation: "utf8mb4_0900_bin",
	})
}

// TestMySQLStrictRejectsNonInnoDBEngineLive covers the storage-engine rejection
// only. It was previously named ...RejectsEngineOrLockPrivilegeLive, which
// implied it also covered the LOCK TABLES grant; it never did, and that naming
// helped hide the fact that verifyLockPrivilege was not wired at all. The
// privilege half is covered by TestMySQLStrictRejectsMissingLockPrivilegeLive.
func TestMySQLStrictRejectsNonInnoDBEngineLive(t *testing.T) {
	testMySQLFamilyStrictRejectsNonInnoDBLive(t, mysqlStrictLiveFixture{
		name:      "MySQL",
		dsnEnv:    "DMTX_TEST_MYSQL_DSN",
		caEnv:     "DMTX_TEST_MYSQL_CA",
		tlsName:   "dmtx_test",
		engine:    StrictConsistencyMySQL,
		collation: "utf8mb4_0900_bin",
	})
}

func TestMariaDBStrictTableSnapshotLive(t *testing.T) {
	testMySQLFamilyStrictTableSnapshotLive(t, mysqlStrictLiveFixture{
		name:      "MariaDB",
		dsnEnv:    "DMTX_TEST_MARIADB_DSN",
		caEnv:     "DMTX_TEST_MARIADB_CA",
		tlsName:   "dmtx_mariadb_test",
		engine:    StrictConsistencyMariaDB,
		sslServer: "localhost",
		collation: "utf8mb4_nopad_bin",
	})
}

func registerMySQLFamilyStrictTLS(
	t *testing.T,
	caPath string,
	name string,
	serverName string,
) {
	t.Helper()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read %s: %v", caPath, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatalf("%s contains no certificates", caPath)
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if serverName != "" {
		config.ServerName = serverName
	}
	if err := mysqlDriver.RegisterTLSConfig(name, config); err != nil {
		t.Fatalf("register %s TLS config: %v", name, err)
	}
}
