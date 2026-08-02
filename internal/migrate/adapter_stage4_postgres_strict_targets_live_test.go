package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4PostgresStrictCrossTargetLiveTLS exercises the real exported
// PostgreSQL snapshot composition against each newly admitted keyed-upsert
// target family. The target is pre-created from the production projection so
// this stays a strict-view sentinel rather than an evolution fixture.
func TestStage4PostgresStrictCrossTargetLiveTLS(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		dsn, ca := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN"), os.Getenv("DMTX_TEST_MYSQL_CA")
		if dsn == "" || ca == "" {
			t.Skip("set DMTX_TEST_MYSQL_TARGET_DSN and DMTX_TEST_MYSQL_CA")
		}
		registerMySQLCommonFixtureTLSNamed(t, ca, "dmtx_test")
		target := mysqlNativeTargetEndpoint(t, parseMySQLNativeTargetDSNForTLS(t, "strict target", dsn, "dmtx_test"), ca)
		testStage4PostgresStrictCrossTargetLiveTLS(t, target, config.StrictConsistencyTable)
	})
	t.Run("mssql", func(t *testing.T) {
		dsn, ca := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA")
		if dsn == "" || ca == "" {
			t.Skip("set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA")
		}
		target := sqlServerCommonFixtureEndpoint(t, dsn, ca)
		target.Schema = "dmtx_pgstrict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		database := openSQLServerNativeLiveDatabase(t, ctx, "strict schema", target)
		if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+sqlServerIdentifier(target.Schema)); err != nil {
			t.Fatalf("create SQL Server strict target schema: %v", err)
		}
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			if _, err := database.ExecContext(cleanup, "DROP SCHEMA IF EXISTS "+sqlServerIdentifier(target.Schema)); err != nil {
				t.Errorf("drop SQL Server strict target schema: %v", err)
			}
		})
		testStage4PostgresStrictCrossTargetLiveTLS(t, target, config.StrictConsistencyTable)
	})
	t.Run("sqlite", func(t *testing.T) {
		testStage4PostgresStrictCrossTargetLiveTLS(t, config.Endpoint{
			Type:     "sqlite",
			Database: filepath.Join(t.TempDir(), "target.db"),
		}, config.StrictConsistencyTable)
	})
	t.Run("sqlite_migration", func(t *testing.T) {
		testStage4PostgresStrictCrossTargetLiveTLS(t, config.Endpoint{
			Type:     "sqlite",
			Database: filepath.Join(t.TempDir(), "target.db"),
		}, config.StrictConsistencyMigration)
	})
}

func testStage4PostgresStrictCrossTargetLiveTLS(t *testing.T, targetEndpoint config.Endpoint, scope string) {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run PostgreSQL strict cross-target TLS coverage")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL strict source DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace, tableName := "dmtx_pg_strict_x_"+suffix, "items_"+suffix
	if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if _, err := database.ExecContext(cleanup, "DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE"); err != nil {
			t.Errorf("drop strict source schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	if _, err := database.ExecContext(ctx, "CREATE TABLE "+qualified+" (id bigint PRIMARY KEY, payload text NOT NULL); INSERT INTO "+qualified+" VALUES (1, 'one'), (2, 'two'), (3, 'three')"); err != nil {
		t.Fatal(err)
	}

	sourceEndpoint := config.Endpoint{Type: "postgres", Host: parsed.Host, Port: int(parsed.Port), Database: parsed.Database, User: parsed.User, Password: parsed.Password, Schema: namespace, SSLMode: "verify-full", TLSCAFile: caFile}
	if sourceEndpoint.SSLMode != "verify-full" || sourceEndpoint.TLSCAFile != caFile {
		t.Fatal("production PostgreSQL source endpoint lost verified TLS authority")
	}
	if targetEndpoint.Type != "sqlite" && (targetEndpoint.SSLMode != "verify-full" || targetEndpoint.TLSCAFile == "") {
		t.Fatalf("production %s target endpoint lacks verified TLS authority: %#v", targetEndpoint.Type, targetEndpoint)
	}

	// Precreate the exact projected target table before the strict run. This
	// avoids assigning schema-evolution coverage to a stable-view sentinel.
	rawSource, err := openPostgresSourceAdapter(ctx, sourceEndpoint)
	if err != nil {
		t.Fatalf("open strict source setup adapter: %v", err)
	}
	source := rawSource.(*relationalSourceAdapter)
	table, err := source.InspectTable(ctx, tableName)
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	rawTarget, err := builtInAdapters.targets[targetEndpoint.Type].open(ctx, targetEndpoint)
	if err != nil {
		_ = source.Close()
		t.Fatalf("open strict target setup adapter: %v", err)
	}
	target := rawTarget
	planned, err := target.PlanTables("postgres", []schema.Table{table}, "upsert")
	if err == nil && len(planned) != 1 {
		err = fmt.Errorf("strict target plan count = %d, want 1", len(planned))
	}
	if err == nil {
		switch typed := target.(type) {
		case *mysqlTargetAdapter:
			var statement string
			statement, err = schema.CreateTable(schema.MySQL, planned[0])
			if err == nil {
				_, err = typed.database.ExecContext(ctx, statement)
			}
		case *sqlServerTargetAdapter:
			var statement string
			statement, err = schema.CreateSQLServerTable(planned[0])
			if err == nil {
				_, err = typed.database.ExecContext(ctx, statement)
			}
		case *sqliteTargetAdapter:
			var statement string
			statement, err = schema.CreateTable(schema.SQLite, planned[0])
			if err == nil {
				_, err = typed.database.ExecContext(ctx, statement)
			}
		default:
			err = fmt.Errorf("strict target adapter %T has no fixture DDL handle", target)
		}
	}
	if closeErr := target.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if closeErr := source.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("precreate projected strict target: %v", err)
	}
	targetTable := planned[0]
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		opened, openErr := builtInAdapters.targets[targetEndpoint.Type].open(cleanup, targetEndpoint)
		if openErr != nil {
			t.Errorf("open strict target cleanup adapter: %v", openErr)
			return
		}
		defer opened.Close()
		switch typed := opened.(type) {
		case *mysqlTargetAdapter:
			_, openErr = typed.database.ExecContext(cleanup, "DROP TABLE IF EXISTS "+mySQLIdentifier(targetTable.Name))
		case *sqlServerTargetAdapter:
			_, openErr = typed.database.ExecContext(cleanup, "DROP TABLE IF EXISTS "+sqlServerQualified(targetTable.Schema, targetTable.Name))
		case *sqliteTargetAdapter:
			_, openErr = typed.database.ExecContext(cleanup, "DROP TABLE IF EXISTS "+quote(targetTable.Name))
		default:
			openErr = nil
		}
		if openErr != nil {
			t.Errorf("drop strict target table: %v", openErr)
		}
	})

	rawState := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "pg-strict-" + targetEndpoint.Type + "-" + suffix
	if err := rawState.InitializeRun(state.Run{ID: runID, Source: "source", Target: "target", SourceEngine: "postgres", SourceIdentity: "postgres:strict", TargetIdentity: targetEndpoint.Type + ":strict", Outcome: state.Running, Resumable: true, Reason: "running", StartedAt: time.Now().UTC()}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
	backend := &stage4PostgresStrictMutationBackend{Stage4StateBackend: rawState, mutate: func() error {
		_, writeErr := database.ExecContext(ctx, "INSERT INTO "+qualified+" VALUES (4, 'after-view')")
		return writeErr
	}}
	events := []string{}
	observer := stage4AdapterObserver{recordingTableObserver: recordingTableObserver{events: &events}, run: stage4LifecycleRunContext(t, backend, runID, false)}
	cfg := stage4AdapterTestConfig(t, parsed.Password, targetEndpoint.Password)
	cfg.Source, cfg.Target = sourceEndpoint, targetEndpoint
	cfg.Migration.TargetMode, cfg.Migration.IncludeTables = "upsert", []string{tableName}
	cfg.Migration.StrictConsistency, cfg.Migration.StrictConsistencyScope = true, scope
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("run PostgreSQL strict %s target: %v", targetEndpoint.Type, err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) || backend.err != nil {
		t.Fatalf("strict result=%#v mutation=%v", result, backend.err)
	}
	var sourceCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualified).Scan(&sourceCount); err != nil || sourceCount != 4 {
		t.Fatalf("post-view source count=%d err=%v", sourceCount, err)
	}
	countTarget, err := builtInAdapters.targets[targetEndpoint.Type].open(ctx, targetEndpoint)
	if err != nil {
		t.Fatalf("open strict target count adapter: %v", err)
	}
	targetCount, countErr := countTarget.CountRows(ctx, targetTable)
	closeErr := countTarget.Close()
	if countErr != nil || closeErr != nil || targetCount != 3 {
		t.Fatalf("post-view target count=%d countErr=%v closeErr=%v", targetCount, countErr, closeErr)
	}
	tasks, _, err := rawState.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range tasks {
		if work.Key.Type != stage4AdapterNetworkTaskType {
			continue
		}
		attempt, attemptErr := BuildStrictConsistencyAttemptID(work.Key, work.TopologyHash, 0)
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		evidence, found, loadErr := rawState.LoadStrictSnapshotEvidence(runID, work.Key, attempt)
		if loadErr != nil || !found || evidence.ExactSourceRowCount != 3 || evidence.SourceEngine != "postgres" || evidence.Scope != state.StrictSnapshotScope(scope) {
			t.Fatalf("strict evidence found=%t evidence=%#v err=%v", found, evidence, loadErr)
		}
		return
	}
	t.Fatal("strict run did not persist a network evidence task")
}
