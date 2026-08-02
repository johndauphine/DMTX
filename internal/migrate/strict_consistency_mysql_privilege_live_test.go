package migrate

import (
	"context"
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

// TestMySQLStrictRejectsMissingLockPrivilegeLive covers the rejection that had
// no test at all.
//
// verifyLockPrivilege was implemented and documented but never wired, and the
// test whose name implied it covered this — TestMySQLStrictRejectsEngineOr
// LockPrivilegeLive, since renamed — actually exercised the storage-engine
// rejection. So the privilege half was unwired *and* uncovered, and each fact
// concealed the other.
//
// This connects as a real account holding SELECT but not LOCK TABLES and
// requires the strict route to refuse it as policy. The refusal must arrive
// before any lock is taken, which is the entire point of checking the grant up
// front rather than discovering it when LOCK TABLES fails mid-run with the
// target already touched.
func TestMySQLStrictRejectsMissingLockPrivilegeLive(t *testing.T) {
	for _, fixture := range []struct {
		mysqlStrictLiveFixture
		adminDSNEnv string
	}{
		{
			mysqlStrictLiveFixture: mysqlStrictLiveFixture{
				name:      "MySQL",
				dsnEnv:    "DMTX_TEST_MYSQL_DSN",
				caEnv:     "DMTX_TEST_MYSQL_CA",
				tlsName:   "dmtx_test",
				engine:    StrictConsistencyMySQL,
				collation: "utf8mb4_0900_bin",
			},
			adminDSNEnv: "DMTX_TEST_MYSQL_ADMIN_DSN",
		},
		{
			mysqlStrictLiveFixture: mysqlStrictLiveFixture{
				name:      "MariaDB",
				dsnEnv:    "DMTX_TEST_MARIADB_DSN",
				caEnv:     "DMTX_TEST_MARIADB_CA",
				tlsName:   "dmtx_mariadb_test",
				engine:    StrictConsistencyMySQL,
				collation: "utf8mb4_nopad_bin",
			},
			adminDSNEnv: "DMTX_TEST_MARIADB_ADMIN_DSN",
		},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			testMySQLStrictRejectsMissingLockPrivilege(
				t,
				fixture.mysqlStrictLiveFixture,
				fixture.adminDSNEnv,
			)
		})
	}
}

func testMySQLStrictRejectsMissingLockPrivilege(
	t *testing.T,
	fixture mysqlStrictLiveFixture,
	adminDSNEnv string,
) {
	t.Helper()
	adminDSN := os.Getenv(adminDSNEnv)
	if strings.TrimSpace(adminDSN) == "" {
		stage4RequireLiveFixture(t, adminDSNEnv)
	}

	// A real table on the normal account, so the refusal cannot be blamed on a
	// missing or malformed source object.
	_, namespace, table := openMySQLStrictLiveSource(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	administrator, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatalf("open %s administrator: %v", fixture.name, err)
	}
	t.Cleanup(func() { _ = administrator.Close() })
	if err := administrator.PingContext(ctx); err != nil {
		t.Fatalf("ping %s administrator: %v", fixture.name, err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	username := "dmtx_nolock_" + suffix
	password := "dmtx_nolock_test_only_" + suffix
	account := "'" + username + "'@'%'"

	if _, err := administrator.ExecContext(
		ctx,
		"CREATE USER "+account+" IDENTIFIED BY '"+password+"'",
	); err != nil {
		t.Fatalf("create %s unprivileged account: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if _, err := administrator.ExecContext(
			cleanupCtx,
			"DROP USER IF EXISTS "+account,
		); err != nil {
			t.Errorf("drop %s unprivileged account: %v", fixture.name, err)
		}
	})
	// SELECT but deliberately not LOCK TABLES. The account can read the table
	// perfectly well, so a refusal cannot be a connectivity or visibility
	// failure wearing a privilege error's message.
	if _, err := administrator.ExecContext(
		ctx,
		"GRANT SELECT ON "+mySQLIdentifier(namespace)+".* TO "+account,
	); err != nil {
		t.Fatalf("grant %s SELECT: %v", fixture.name, err)
	}
	if _, err := administrator.ExecContext(ctx, "FLUSH PRIVILEGES"); err != nil {
		t.Fatalf("flush %s privileges: %v", fixture.name, err)
	}

	parsed, err := mysqlDriver.ParseDSN(os.Getenv(fixture.dsnEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", fixture.dsnEnv, err)
	}
	parsed.User = username
	parsed.Passwd = password
	unprivileged, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatalf("open %s unprivileged source: %v", fixture.name, err)
	}
	t.Cleanup(func() { _ = unprivileged.Close() })
	unprivileged.SetMaxOpenConns(4)
	if err := unprivileged.PingContext(ctx); err != nil {
		t.Fatalf("ping %s unprivileged source: %v", fixture.name, err)
	}
	// Prove the account really can read. Without this the test would pass just
	// as happily if the grant had failed and the account could see nothing.
	var readable int
	if err := unprivileged.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", namespace, table),
	).Scan(&readable); err != nil {
		t.Fatalf(
			"%s unprivileged account cannot read the source table, so a refusal would prove nothing: %v",
			fixture.name,
			err,
		)
	}

	opener, err := NewMySQLStrictConsistencyOpener(
		unprivileged,
		namespace,
		fixture.engine,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(
		ctx,
		StrictConsistencyOpenRequest{
			RunID:        "mysql-strict-no-lock-" + suffix,
			SourceEngine: fixture.engine,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "epoch-1",
			Tables: []StrictConsistencyTable{{
				Task:      state.TaskKey{Type: "table-copy", Table: table},
				AttemptID: "attempt-1",
			}},
		},
	)
	if err == nil {
		if session != nil {
			_ = session.Close(ctx)
		}
		t.Fatalf(
			"%s strict route opened a snapshot for an account without LOCK TABLES",
			fixture.name,
		)
	}
	if session != nil {
		_ = session.Close(ctx)
		t.Fatalf(
			"%s strict route returned a session alongside its refusal",
			fixture.name,
		)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf(
			"%s missing-privilege refusal class = %q, want policy",
			fixture.name,
			ClassifyTransferError(err),
		)
	}
	// Assert the stated reason. A refusal for some unrelated cause would
	// otherwise let this pass while the privilege check was gone again.
	if !strings.Contains(err.Error(), "LOCK TABLES") {
		t.Fatalf(
			"%s refusal did not name the missing privilege: %v",
			fixture.name,
			err,
		)
	}
}
