package migrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"

	"github.com/johndauphine/dmtx/internal/state"
)

type mysqlStrictLiveFixture struct {
	name      string
	dsnEnv    string
	caEnv     string
	tlsName   string
	engine    StrictConsistencyEngine
	sslServer string
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
			payload VARCHAR(40) NOT NULL
		) ENGINE=InnoDB`,
		quoted,
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
		name:    "MySQL",
		dsnEnv:  "DMTX_TEST_MYSQL_DSN",
		caEnv:   "DMTX_TEST_MYSQL_CA",
		tlsName: "dmtx_test",
		engine:  StrictConsistencyMySQL,
	})
}

func TestMySQLStrictRejectsEngineOrLockPrivilegeLive(t *testing.T) {
	testMySQLFamilyStrictRejectsNonInnoDBLive(t, mysqlStrictLiveFixture{
		name:    "MySQL",
		dsnEnv:  "DMTX_TEST_MYSQL_DSN",
		caEnv:   "DMTX_TEST_MYSQL_CA",
		tlsName: "dmtx_test",
		engine:  StrictConsistencyMySQL,
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
