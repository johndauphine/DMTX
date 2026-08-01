package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4IncrementalLiveFailAckBackend struct {
	state.YAMLStore
	failNext bool
}

func (backend *stage4IncrementalLiveFailAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	if backend.failNext &&
		acknowledgement.Task.Type == stage4AdapterNetworkTaskType &&
		acknowledgement.RangeID == stage4AdapterIncrementalRangeID {
		backend.failNext = false
		return state.RangeState{}, errors.New(
			"injected Stage 4 incremental acknowledgement failure",
		)
	}
	return backend.YAMLStore.AcknowledgeRange(acknowledgement)
}

func TestStage4PostgresIncrementalCompositionLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL incremental composition sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL incremental DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(
		context.Background(),
		150*time.Second,
	)
	defer cancel()
	sourceSetup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL incremental source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL incremental source: %v", err)
		}
	})
	if err := sourceSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL incremental source: %T", err)
	}
	assertStage4IncrementalPostgresTLS(
		t,
		ctx,
		sourceSetup,
		"source",
	)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_inc_" + suffix
	tableName := "events"
	targetDatabaseName := "dmtx_s4_inc_target_" + suffix
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabaseName),
	); err != nil {
		t.Fatalf("create Stage 4 incremental target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+
				postgresIdentifier(targetDatabaseName)+
				" WITH (FORCE)",
		); err != nil {
			t.Errorf("drop Stage 4 incremental target database: %v", err)
		}
	})
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 incremental source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop Stage 4 incremental source schema: %v", err)
		}
	})

	sourceEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      parsed.Host,
		Port:      int(parsed.Port),
		Database:  parsed.Database,
		User:      parsed.User,
		Password:  parsed.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: caFile,
	}
	if sourceEndpoint.SSLMode != "verify-full" ||
		sourceEndpoint.TLSCAFile != caFile {
		t.Fatal("incremental source endpoint lost verified TLS authority")
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabaseName
	if targetEndpoint.SSLMode != "verify-full" ||
		targetEndpoint.TLSCAFile != caFile {
		t.Fatal("incremental target endpoint lost verified TLS authority")
	}
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	targetSetup, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL incremental target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL incremental target: %v", err)
		}
	})
	if err := targetSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL incremental target: %v", err)
	}
	assertStage4IncrementalPostgresTLS(
		t,
		ctx,
		targetSetup,
		"target",
	)
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 incremental target schema: %v", err)
	}
	sourceTable := postgresQualified(namespace, tableName)
	targetTable := postgresQualified(namespace, tableName)
	tableDDL := ` (
		id bigint NOT NULL PRIMARY KEY,
		payload text NOT NULL,
		updated_at timestamp(3)
	)`
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE TABLE "+sourceTable+tableDDL+`;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at) VALUES
			(1, 'baseline-one', timestamp '2026-07-30 10:00:00.000'),
			(2, 'baseline-null', NULL),
			(3, 'baseline-equal', timestamp '2026-07-30 10:00:00.000')`,
	); err != nil {
		t.Fatalf("create Stage 4 incremental source table: %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE TABLE "+targetTable+tableDDL,
	); err != nil {
		t.Fatalf("create Stage 4 incremental target table: %v", err)
	}

	cfg := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{tableName},
			DateUpdatedColumns: []string{"updated_at"},
			ConnectionLimit:    4,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			MemoryCeilingBytes: 64 << 20,
			Validation: config.ValidationPolicy{
				// Exercise the incremental attempt-bound final target proof through
				// the production PostgreSQL source/target adapters: count, NULL
				// parity, and deterministic typed samples all share this route.
				Mode:                   config.ValidationSample,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
	stateStore := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: namespace,
		Table:  tableName,
	}

	baselineRun := "stage4-pg-incremental-baseline"
	initializeStage4LifecycleRun(
		t,
		stateStore,
		baselineRun,
		time.Now().Add(-time.Minute),
	)
	baselineEvents := make([]string, 0)
	baselineObserver := stage4IncrementalTestObserver{
		events:  &baselineEvents,
		backend: stateStore,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			baselineRun,
			false,
		),
	}
	result, err := PostgresToPostgresWithObserver(
		ctx,
		cfg,
		baselineObserver,
	)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL incremental baseline: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("baseline result = %#v", result)
	}
	baselineAttempt, found, err :=
		stateStore.LoadLatestCommittedIncrementalAttempt(
			baselineRun,
			task,
		)
	if err != nil || !found ||
		baselineAttempt.CommittedWatermark == nil ||
		!baselineAttempt.CommittedWatermark.Value.Equal(
			time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
		) {
		t.Fatalf(
			"baseline committed attempt found=%v attempt=%#v err=%v",
			found,
			baselineAttempt,
			err,
		)
	}
	completeStage4IncrementalTestRun(t, stateStore, baselineRun)

	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'window-one',
		        updated_at = timestamp '2026-07-30 10:01:00.000'
		  WHERE id = 1;
		 UPDATE `+sourceTable+`
		    SET payload = 'equal-lower-must-not-copy'
		  WHERE id = 3;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at)
		 VALUES (4, 'window-four', timestamp '2026-07-30 10:02:00.000')`,
	); err != nil {
		t.Fatalf("prepare Stage 4 strict-lower window: %v", err)
	}
	windowRun := "stage4-pg-incremental-window"
	initializeStage4LifecycleRun(
		t,
		stateStore,
		windowRun,
		time.Now().Add(-time.Minute),
	)
	windowEvents := make([]string, 0)
	windowObserver := stage4IncrementalTestObserver{
		events:  &windowEvents,
		backend: stateStore,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			windowRun,
			false,
		),
	}
	result, err = PostgresToPostgresWithObserver(
		ctx,
		cfg,
		windowObserver,
	)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL strict-lower window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("strict-lower result = %#v", result)
	}
	var equalLowerPayload string
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT payload FROM "+targetTable+" WHERE id = 3",
	).Scan(&equalLowerPayload); err != nil {
		t.Fatalf("read strict-lower target row: %v", err)
	}
	if equalLowerPayload != "baseline-equal" {
		t.Fatalf(
			"equal-lower row was copied: payload=%q",
			equalLowerPayload,
		)
	}

	if _, err := targetSetup.ExecContext(
		ctx,
		"UPDATE "+targetTable+
			" SET payload = 'tampered' WHERE id = 1",
	); err != nil {
		t.Fatalf("tamper completed incremental target: %v", err)
	}
	tamperEvents := make([]string, 0)
	tamperObserver := stage4IncrementalTestObserver{
		events:  &tamperEvents,
		backend: stateStore,
		resume:  true,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			windowRun,
			true,
		),
	}
	_, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{
			tableName: {Rows: 2},
		},
		tamperObserver,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"revalidate exact completed Stage 4 incremental target values",
		) {
		t.Fatalf("completed-target tamper resume error = %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		`UPDATE `+targetTable+`
		    SET payload = 'window-one',
		        updated_at = timestamp '2026-07-30 10:01:00.000'
		  WHERE id = 1`,
	); err != nil {
		t.Fatalf("repair completed incremental target: %v", err)
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{
			tableName: {Rows: 2},
		},
		tamperObserver,
	)
	if err != nil {
		t.Fatalf("revalidate repaired completed window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("completed-window reuse result = %#v", result)
	}
	completeStage4IncrementalTestRun(t, stateStore, windowRun)

	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'crash-four',
		        updated_at = timestamp '2026-07-30 10:03:00.000'
		  WHERE id = 4;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at)
		 VALUES (6, 'crash-six', timestamp '2026-07-30 10:04:00.000')`,
	); err != nil {
		t.Fatalf("prepare Stage 4 crash window: %v", err)
	}
	crashRun := "stage4-pg-incremental-crash-resume"
	failingStore := &stage4IncrementalLiveFailAckBackend{
		YAMLStore: stateStore,
		failNext:  true,
	}
	initializeStage4LifecycleRun(
		t,
		failingStore,
		crashRun,
		time.Now().Add(-time.Minute),
	)
	crashEvents := make([]string, 0)
	crashObserver := stage4IncrementalTestObserver{
		events:  &crashEvents,
		backend: failingStore,
		run: stage4LifecycleRunContext(
			t,
			failingStore,
			crashRun,
			false,
		),
	}
	result, err = PostgresToPostgresWithObserver(
		ctx,
		cfg,
		crashObserver,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"injected Stage 4 incremental acknowledgement failure",
		) {
		t.Fatalf(
			"crash-window result=%#v error=%v",
			result,
			err,
		)
	}
	active, found, err := failingStore.LoadActiveIncrementalAttempt(
		crashRun,
		task,
	)
	immutableUpper := time.Date(
		2026,
		7,
		30,
		10,
		4,
		0,
		0,
		time.UTC,
	)
	if err != nil || !found ||
		active.UpperFence == nil ||
		!active.UpperFence.Value.Equal(immutableUpper) {
		t.Fatalf(
			"active crash fence found=%v attempt=%#v err=%v",
			found,
			active,
			err,
		)
	}
	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'inserted-inside-stored-window',
		        updated_at = timestamp '2026-07-30 10:03:30.000'
		  WHERE id = 1`,
	); err != nil {
		t.Fatalf("mutate an already-passed key inside stored window: %v", err)
	}
	resumeEvents := make([]string, 0)
	resumeObserver := stage4IncrementalTestObserver{
		events:  &resumeEvents,
		backend: failingStore,
		resume:  true,
		run: stage4LifecycleRunContext(
			t,
			failingStore,
			crashRun,
			true,
		),
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf("resume full immutable PostgreSQL window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("crash-resume result = %#v", result)
	}
	committed, found, err :=
		failingStore.LoadLatestCommittedIncrementalAttempt(
			crashRun,
			task,
		)
	if err != nil || !found ||
		committed.CommittedWatermark == nil ||
		!committed.CommittedWatermark.Value.Equal(immutableUpper) {
		t.Fatalf(
			"crash-resume committed fence found=%v attempt=%#v err=%v",
			found,
			committed,
			err,
		)
	}
	var replayedPayload string
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT payload FROM "+targetTable+" WHERE id = 1",
	).Scan(&replayedPayload); err != nil {
		t.Fatalf("read full-window replayed target row: %v", err)
	}
	if replayedPayload != "inserted-inside-stored-window" {
		t.Fatalf(
			"full-window resume missed the earlier key: payload=%q",
			replayedPayload,
		)
	}
}

func assertStage4IncrementalPostgresTLS(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	label string,
) {
	t.Helper()
	var tlsActive bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect Stage 4 incremental %s TLS: %v", label, err)
	}
	if !tlsActive {
		t.Fatalf(
			"Stage 4 incremental %s established a non-TLS session",
			label,
		)
	}
}

var _ stage4IncrementalTestState = (*stage4IncrementalLiveFailAckBackend)(nil)

var _ state.Stage4AggregateBackend = (*stage4IncrementalLiveFailAckBackend)(nil)

func TestStage4IncrementalComposedCrossEngineLive(t *testing.T) {
	t.Run("sqlserver-to-postgres", func(t *testing.T) {
		sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
		caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
		targetDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
		if sourceDSN == "" || caPath == "" || targetDSN == "" {
			t.Skip("set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, and DMTX_TEST_POSTGRES_DSN")
		}
		sourceEndpoint := sqlServerCommonFixtureEndpoint(t, sourceDSN, caPath)
		parsedTarget, err := pgx.ParseConfig(targetDSN)
		if err != nil || !postgresRouteLiveRequiresTLS(parsedTarget) {
			t.Fatal("DMTX_TEST_POSTGRES_DSN must be a verified-TLS PostgreSQL DSN")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		sourceDatabase, err := sql.Open("sqlserver", sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sourceDatabase.Close() })
		targetDatabase, err := sql.Open("pgx", targetDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = targetDatabase.Close() })
		suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
		tableName := "dmtx_inc_ms_pg_" + suffix
		namespace := "dmtx_inc_ms_pg_" + suffix
		targetEndpoint := config.Endpoint{
			Type:     "postgres",
			Host:     parsedTarget.Host,
			Port:     int(parsedTarget.Port),
			Database: parsedTarget.Database,
			User:     parsedTarget.User,
			Password: parsedTarget.Password,
			Schema:   namespace,
			SSLMode:  "verify-full",
			TLSCAFile: stage4PostgresDeleteLiveCAFile(
				t,
				targetDSN,
			),
		}
		if _, err := sourceDatabase.ExecContext(ctx, `CREATE TABLE `+sqlServerQualified("dbo", tableName)+` (
			[id] BIGINT NOT NULL PRIMARY KEY,
			[payload] VARCHAR(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			[updated_at] DATETIME2(3) NOT NULL
		); INSERT INTO `+sqlServerQualified("dbo", tableName)+` VALUES
			(1, 'baseline-one', CONVERT(datetime2(3), '2026-07-30T10:00:00.000')),
			(2, 'baseline-two', CONVERT(datetime2(3), '2026-07-30T10:00:00.000'))`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = sourceDatabase.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+sqlServerQualified("dbo", tableName))
		})
		if _, err := targetDatabase.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = targetDatabase.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE")
		})
		cfg := stage4CrossEngineIncrementalConfig(sourceEndpoint, targetEndpoint, tableName)
		runStage4CrossEngineIncrementalLive(
			t, ctx, cfg, SQLServerToPostgresWithObserver, "dbo", tableName,
			func(payload string, at time.Time) error {
				_, err := sourceDatabase.ExecContext(ctx, "UPDATE "+sqlServerQualified("dbo", tableName)+" SET [payload] = @p1, [updated_at] = @p2 WHERE [id] = 1", payload, at)
				return err
			},
			func() (string, error) {
				var payload string
				err := targetDatabase.QueryRowContext(ctx, "SELECT payload FROM "+postgresQualified(namespace, tableName)+" WHERE id = 1").Scan(&payload)
				return payload, err
			},
		)
	})

	t.Run("mysql-to-sqlserver", func(t *testing.T) {
		sourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
		sourceCA := os.Getenv("DMTX_TEST_MYSQL_CA")
		targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
		targetCA := os.Getenv("DMTX_TEST_MSSQL_CA")
		if sourceDSN == "" || sourceCA == "" || targetDSN == "" || targetCA == "" {
			t.Skip("set DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_CA, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA")
		}
		registerMySQLCommonFixtureTLSNamed(t, sourceCA, "dmtx_test")
		parsedSource := parseMySQLNativeTargetDSNForTLS(t, "incremental source", sourceDSN, "dmtx_test")
		sourceEndpoint := mySQLSQLServerSourceEndpoint(t, parsedSource, sourceCA)
		targetEndpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, targetCA)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		sourceDatabase, err := sql.Open("mysql", sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sourceDatabase.Close() })
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "incremental target", targetEndpoint)
		tableName := "dmtx_inc_my_ms_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		if _, err := sourceDatabase.ExecContext(ctx, "CREATE TABLE "+mySQLIdentifier(tableName)+` (
			id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL,
			updated_at DATETIME(3) NOT NULL) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`); err != nil {
			t.Fatal(err)
		}
		if _, err := sourceDatabase.ExecContext(ctx, `INSERT INTO `+mySQLIdentifier(tableName)+` VALUES
			(1, 'baseline-one', '2026-07-30 10:00:00.000'),
			(2, 'baseline-two', '2026-07-30 10:00:00.000')`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = sourceDatabase.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+mySQLIdentifier(tableName))
		})
		cleanupSQLServerNativeTables(t, targetDatabase, tableName)
		cfg := stage4CrossEngineIncrementalConfig(sourceEndpoint, targetEndpoint, tableName)
		runStage4CrossEngineIncrementalLive(
			t, ctx, cfg, MySQLToSQLServerWithObserver, parsedSource.DBName, tableName,
			func(payload string, at time.Time) error {
				_, err := sourceDatabase.ExecContext(ctx, "UPDATE "+mySQLIdentifier(tableName)+" SET payload = ?, updated_at = ? WHERE id = 1", payload, at)
				return err
			},
			func() (string, error) {
				var payload string
				err := targetDatabase.QueryRowContext(ctx, "SELECT [payload] FROM "+sqlServerQualified("dbo", tableName)+" WHERE [id] = 1").Scan(&payload)
				return payload, err
			},
		)
	})

	t.Run("sqlite-to-mysql", func(t *testing.T) {
		targetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
		caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
		if targetDSN == "" || caPath == "" {
			t.Skip("set DMTX_TEST_MYSQL_TARGET_DSN and DMTX_TEST_MYSQL_CA")
		}
		registerMySQLCommonFixtureTLSNamed(t, caPath, "dmtx_test")
		parsedTarget := parseMySQLNativeTargetDSNForTLS(t, "incremental target", targetDSN, "dmtx_test")
		targetEndpoint := mysqlNativeTargetEndpoint(t, parsedTarget, caPath)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		sourcePath := filepath.Join(t.TempDir(), "incremental-source.sqlite")
		sourceDatabase, err := sql.Open("sqlite", sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sourceDatabase.Close() })
		targetDatabase := openMySQLNativeLiveDatabase(t, ctx, "incremental target", targetDSN)
		tableName := "dmtx_inc_sq_my_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		if _, err := sourceDatabase.ExecContext(ctx, `CREATE TABLE "`+tableName+`" (
			id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, updated_at DATETIME(3) NOT NULL,
			PRIMARY KEY (id));
			INSERT INTO "`+tableName+`" VALUES
			(1, 'baseline-one', '2026-07-30 10:00:00.000'),
			(2, 'baseline-two', '2026-07-30 10:00:00.000')`); err != nil {
			t.Fatal(err)
		}
		cleanupMySQLNativeTables(t, targetDatabase, tableName)
		cfg := stage4CrossEngineIncrementalConfig(config.Endpoint{Type: "sqlite", Database: sourcePath}, targetEndpoint, tableName)
		runStage4CrossEngineIncrementalLive(
			t, ctx, cfg, SQLiteToMySQLWithObserver, "", tableName,
			func(payload string, at time.Time) error {
				_, err := sourceDatabase.ExecContext(ctx, `UPDATE "`+tableName+`" SET payload = ?, updated_at = ? WHERE id = 1`, payload, at.Format("2006-01-02 15:04:05.000"))
				return err
			},
			func() (string, error) {
				var payload string
				err := targetDatabase.QueryRowContext(ctx, "SELECT payload FROM "+mySQLIdentifier(tableName)+" WHERE id = 1").Scan(&payload)
				return payload, err
			},
		)
	})
}

func stage4CrossEngineIncrementalConfig(source, target config.Endpoint, table string) config.Config {
	return config.Config{Source: source, Target: target, Migration: config.Migration{
		TargetMode: "upsert", IncludeTables: []string{table}, DateUpdatedColumns: []string{"updated_at"},
		ConnectionLimit: 4, ReaderParallelism: 1, WriterParallelism: 1, MemoryCeilingBytes: 64 << 20,
		Validation: config.ValidationPolicy{Mode: config.ValidationCountOnly, FailOnMismatch: true, FailOnTimeout: true, FailOnEstimateMismatch: true},
	}}
}

func runStage4CrossEngineIncrementalLive(
	t *testing.T,
	ctx context.Context,
	cfg config.Config,
	run func(context.Context, config.Config, TableObserver) (Result, error),
	schemaName string,
	table string,
	mutate func(string, time.Time) error,
	readTarget func() (string, error),
) {
	t.Helper()
	store := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: schemaName, Table: table}
	bootstrap := cfg
	bootstrap.Migration.TargetMode = "drop_recreate"
	bootstrap.Migration.DateUpdatedColumns = nil
	bootstrap.Migration.DestructiveAcknowledged = true
	if cfg.Target.Type == "postgres" {
		bootstrapRun := "stage4-cross-incremental-bootstrap"
		initializeStage4LifecycleRun(t, store, bootstrapRun, time.Now().Add(-time.Minute))
		events := make([]string, 0)
		bootstrapObserver := stage4IncrementalTestObserver{events: &events, backend: store, run: stage4LifecycleRunContext(t, store, bootstrapRun, false)}
		if result, err := run(ctx, bootstrap, bootstrapObserver); err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
			t.Fatalf("incremental target bootstrap result=%#v err=%v", result, err)
		}
		completeStage4IncrementalTestRun(t, store, bootstrapRun)
	} else if result, err := run(ctx, bootstrap, nil); err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("incremental target bootstrap result=%#v err=%v", result, err)
	}
	baseline := "stage4-cross-incremental-baseline"
	initializeStage4LifecycleRun(t, store, baseline, time.Now().Add(-time.Minute))
	events := make([]string, 0)
	baselineObserver := stage4IncrementalTestObserver{events: &events, backend: store, run: stage4LifecycleRunContext(t, store, baseline, false)}
	if result, err := run(ctx, cfg, baselineObserver); err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("incremental baseline result=%#v err=%v", result, err)
	}
	completeStage4IncrementalTestRun(t, store, baseline)
	upper := time.Date(2026, 7, 30, 10, 2, 0, 0, time.UTC)
	if err := mutate("window-value", upper); err != nil {
		t.Fatalf("mutate source window: %v", err)
	}
	crash := "stage4-cross-incremental-crash"
	failing := &stage4IncrementalLiveFailAckBackend{YAMLStore: store, failNext: true}
	initializeStage4LifecycleRun(t, failing, crash, time.Now().Add(-time.Minute))
	crashObserver := stage4IncrementalTestObserver{events: &events, backend: failing, run: stage4LifecycleRunContext(t, failing, crash, false)}
	if result, err := run(ctx, cfg, crashObserver); err == nil || !strings.Contains(err.Error(), "injected Stage 4 incremental acknowledgement failure") || result != (Result{}) {
		t.Fatalf("injected post-commit/pre-ack result=%#v err=%v", result, err)
	}
	active, found, err := failing.LoadActiveIncrementalAttempt(crash, task)
	if err != nil || !found || active.Status != state.IncrementalRunning ||
		active.CommittedWatermark != nil || active.UpperFence == nil ||
		!active.UpperFence.Value.Equal(upper) {
		t.Fatalf(
			"post-ack-failure active attempt=%#v found=%t err=%v; want durable uncommitted upper fence",
			active,
			found,
			err,
		)
	}
	frontier, found, err := failing.LoadLatestCommittedIncrementalAttempt(crash, task)
	if err != nil || !found || frontier.CommittedWatermark == nil ||
		!frontier.CommittedWatermark.Value.Equal(time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf(
			"post-ack-failure committed frontier=%#v found=%t err=%v; want baseline watermark",
			frontier,
			found,
			err,
		)
	}
	if err := mutate("replayed-window-value", upper.Add(-500*time.Millisecond)); err != nil {
		t.Fatalf("mutate source inside stored window: %v", err)
	}
	resumeObserver := stage4IncrementalTestObserver{events: &events, backend: failing, resume: true, run: stage4LifecycleRunContext(t, failing, crash, true)}
	result, err := ExecuteResume(ctx, cfg, CompletedTableCheckpoints{}, resumeObserver)
	if err != nil || result != (Result{Tables: 1, Rows: 1, Validated: true}) {
		t.Fatalf("full-window replay result=%#v err=%v", result, err)
	}
	attempt, found, err := failing.LoadLatestCommittedIncrementalAttempt(crash, task)
	if err != nil || !found || attempt.CommittedWatermark == nil || !attempt.CommittedWatermark.Value.Equal(upper) {
		t.Fatalf("committed replay attempt=%#v found=%t err=%v", attempt, found, err)
	}
	payload, err := readTarget()
	if err != nil || payload != "replayed-window-value" {
		t.Fatalf("replayed target payload=%q err=%v", payload, err)
	}
}
