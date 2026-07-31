package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func openSQLServerStrictLiveSource(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	if dsn == "" || os.Getenv("DMTX_TEST_MSSQL_CA") == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server strict route",
		)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MSSQL_DSN: %v", err)
	}
	if parsed.Query().Get("encrypt") != "true" {
		t.Fatal("DMTX_TEST_MSSQL_DSN must require verified TLS")
	}
	database, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open SQL Server strict source: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	database.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server strict source: %v", err)
	}

	table := "dmtx_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quoted := "[dbo].[" + table + "]"
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, payload NVARCHAR(40) NOT NULL)`,
		quoted,
	)); err != nil {
		t.Fatalf("create SQL Server strict fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+quoted,
		); err != nil {
			t.Errorf("drop SQL Server strict fixture: %v", err)
		}
	})
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (1,'a'),(2,'b'),(3,'c')`,
		quoted,
	)); err != nil {
		t.Fatalf("seed SQL Server strict fixture: %v", err)
	}
	return database, "dbo", table
}

// TestSQLServerStrictTableLockLive proves the Section 10 SQL Server table
// contract against a real server, including the part that differs from every
// other engine: writes to the locked table are expected to wait. The test
// asserts the block rather than tolerating it, because a strict table view that
// silently allowed writes would not be strict at all.
func TestSQLServerStrictTableLockLive(t *testing.T) {
	source, namespace, table := openSQLServerStrictLiveSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opener, err := NewSQLServerStrictConsistencyOpener(source, namespace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mssql-strict-live",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      state.TaskKey{Type: "table-copy", Table: table},
			AttemptID: "attempt-1",
		}},
	})
	if err != nil {
		t.Fatalf("open SQL Server strict view: %v", err)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		t.Fatalf("capture SQL Server strict evidence: %v", err)
	}
	if len(capture.Tables) != 1 ||
		capture.Tables[0].ExactSourceRowCount != 3 {
		t.Fatalf("SQL Server capture = %#v", capture)
	}
	if err := validateSnapshotReference(
		capture.Tables[0].SnapshotReference,
	); err != nil {
		t.Fatalf("snapshot reference rejected by the core: %v", err)
	}

	// A writer must wait while the shared lock is held. A short deadline turns
	// "waits" into an observable fact rather than an untested claim.
	quoted := "[" + namespace + "].[" + table + "]"
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 3*time.Second)
	_, writeErr := source.ExecContext(blockedCtx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (99,'blocked')`,
		quoted,
	))
	blockedCancel()
	if writeErr == nil {
		t.Fatal("a writer committed while the strict table lock was held")
	}
	if !errors.Is(writeErr, context.DeadlineExceeded) {
		t.Logf("writer failed with %v (accepted: it did not commit)", writeErr)
	}

	// Releasing the view must release the source, or the strict route would
	// leave the table locked for the life of the connection pool.
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close SQL Server strict view: %v", err)
	}
	releasedCtx, releasedCancel := context.WithTimeout(ctx, 20*time.Second)
	defer releasedCancel()
	if _, err := source.ExecContext(releasedCtx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (99,'after-close')`,
		quoted,
	)); err != nil {
		t.Fatalf("SQL Server strict view did not release its lock: %v", err)
	}
}

// TestSQLServerStrictRejectsMigrationScopeLive proves the table-scope opener
// refuses migration scope instead of quietly serving a table lock in place of
// the database snapshot that scope requires.
func TestSQLServerStrictRejectsMigrationScopeLive(t *testing.T) {
	source, namespace, table := openSQLServerStrictLiveSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opener, err := NewSQLServerStrictConsistencyOpener(source, namespace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mssql-strict-scope",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotMigration,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      state.TaskKey{Type: "table-copy", Table: table},
			AttemptID: "attempt-1",
		}},
	})
	if session != nil {
		_ = session.Close(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "cannot serve scope") {
		t.Fatalf("SQL Server migration-scope error = %v", err)
	}
}
