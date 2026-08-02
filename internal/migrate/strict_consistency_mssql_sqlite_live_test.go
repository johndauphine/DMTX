package migrate

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestSQLServerMigrationStrictComposedSQLiteLiveTLS is the target-generic
// migration-snapshot sentinel: source evidence comes from one durable SQL
// Server snapshot while the established SQLite keyed writer receives only the
// retained rows after a post-evidence source commit.
func TestSQLServerMigrationStrictComposedSQLiteLiveTLS(t *testing.T) {
	dsn, ca := os.Getenv("DMTX_TEST_MSSQL_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || ca == "" {
		t.Skip("set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA")
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(t, dsn, ca)
	if sourceEndpoint.SSLMode != "verify-full" || sourceEndpoint.TLSCAFile != ca {
		t.Fatal("production SQL Server source endpoint lost verified TLS authority")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("database", "master")
	parsed.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin, err := sql.Open("sqlserver", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceName := "dmtx_mssql_sqlite_strict_" + suffix
	quotedSource, err := quoteSQLServerStrictIdentifier(sourceName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quotedSource); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if _, err := admin.ExecContext(cleanup, "ALTER DATABASE "+quotedSource+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedSource); err != nil {
			t.Errorf("drop SQL Server snapshot source: %v", err)
		}
	})
	sourceEndpoint.Database = sourceName
	sourceDB, err := engine.OpenSQLServer2022Source(ctx, sourceEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })
	if _, err := sourceDB.ExecContext(ctx, "CREATE TABLE [dbo].[items] ([id] BIGINT NOT NULL PRIMARY KEY, [payload] BIGINT NOT NULL); INSERT INTO [dbo].[items] VALUES (1, 11), (2, 22), (3, 33)"); err != nil {
		t.Fatal(err)
	}

	targetEndpoint := config.Endpoint{Type: "sqlite", Database: filepath.Join(t.TempDir(), "target.db")}
	rawSource, err := openSQLServerSourceAdapter(ctx, sourceEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	setupSource := rawSource.(*relationalSourceAdapter)
	table, err := setupSource.InspectTable(ctx, "items")
	if err != nil {
		_ = setupSource.Close()
		t.Fatal(err)
	}
	rawTarget, err := openSQLiteTargetAdapter(ctx, targetEndpoint)
	if err != nil {
		_ = setupSource.Close()
		t.Fatal(err)
	}
	setupTarget := rawTarget.(*sqliteTargetAdapter)
	planned, err := setupTarget.PlanTables("mssql", []schema.Table{table}, "upsert")
	if err == nil && len(planned) == 1 {
		var ddl string
		ddl, err = schema.CreateTable(schema.SQLite, planned[0])
		if err == nil {
			_, err = setupTarget.database.ExecContext(ctx, ddl)
		}
	}
	_ = setupTarget.Close()
	_ = setupSource.Close()
	if err != nil {
		t.Fatalf("precreate SQL Server-to-SQLite strict target: %v", err)
	}

	runID := "mssql-sqlite-strict-" + suffix
	backend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	if err := backend.InitializeRun(state.Run{ID: runID, Source: sourceName, Target: targetEndpoint.Database, SourceEngine: "mssql", SourceIdentity: "mssql:" + sourceName, TargetIdentity: "sqlite:" + targetEndpoint.Database, Outcome: state.Running, Resumable: true, Reason: "running", StartedAt: time.Now().UTC()}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
	cleanupBackend, ok := any(backend).(state.StrictMigrationCleanupBackend)
	if !ok {
		t.Fatalf("%T lacks migration cleanup state", backend)
	}
	mutatingBackend := &stage4SQLServerMigrationLiveBackend{Stage4StateBackend: backend, cleanup: cleanupBackend, mutate: func() error {
		_, writeErr := sourceDB.ExecContext(ctx, "INSERT INTO [dbo].[items] VALUES (4, 44)")
		return writeErr
	}}
	events := []string{}
	observer := stage4AdapterObserver{recordingTableObserver: recordingTableObserver{events: &events}, run: stage4LifecycleRunContext(t, mutatingBackend, runID, false)}
	cfg := stage4AdapterTestConfig(t, sourceEndpoint.Password, "")
	cfg.Source, cfg.Target = sourceEndpoint, targetEndpoint
	cfg.Migration.TargetMode, cfg.Migration.IncludeTables = "upsert", []string{"items"}
	cfg.Migration.StrictConsistency, cfg.Migration.StrictConsistencyScope = true, config.StrictConsistencyMigration
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("run SQL Server migration strict SQLite route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) || mutatingBackend.mutationErr != nil {
		t.Fatalf("result=%#v source mutation=%v", result, mutatingBackend.mutationErr)
	}
	var sourceCount int
	if err := sourceDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM [dbo].[items]").Scan(&sourceCount); err != nil || sourceCount != 4 {
		t.Fatalf("source count=%d err=%v", sourceCount, err)
	}
	targetDB, err := openSQLiteTargetDatabase(ctx, targetEndpoint.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()
	var targetCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM \"items\"").Scan(&targetCount); err != nil || targetCount != 3 {
		t.Fatalf("target count=%d err=%v", targetCount, err)
	}
	work, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range work {
		if item.Key.Type != stage4AdapterNetworkTaskType {
			continue
		}
		attempt, attemptErr := BuildStrictConsistencyAttemptID(item.Key, item.TopologyHash, 0)
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		evidence, found, loadErr := backend.LoadStrictSnapshotEvidence(runID, item.Key, attempt)
		if loadErr != nil || !found || evidence.Scope != state.StrictSnapshotMigration || evidence.ExactSourceRowCount != 3 || evidence.SourceEngine != "mssql" {
			t.Fatalf("migration evidence=%#v found=%t err=%v", evidence, found, loadErr)
		}
		return
	}
	t.Fatal("missing SQL Server migration strict evidence")
}
