package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4PostgresTLSToSQLiteNetworkCrashResumeLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 " +
				"PostgreSQL TLS-to-SQLite network resume sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL TLS DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL TLS setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL TLS setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("verify Stage 4 PostgreSQL TLS setup: %T", err)
	}
	var tlsActive bool
	if err := setup.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect Stage 4 PostgreSQL TLS session: %v", err)
	}
	if !tlsActive {
		t.Fatal("DMTX_TEST_POSTGRES_DSN established a non-TLS session")
	}

	namespace := "dmtx_stage4_resume_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "network_items"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop Stage 4 PostgreSQL source schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO ` + qualified + ` (id, payload) VALUES
			(1, 11),
			(2, 22),
			(3, 33)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create Stage 4 PostgreSQL source fixture: %v", err)
		}
	}

	targetPath := filepath.Join(t.TempDir(), "target.db")
	createStage4NetworkSQLiteTarget(
		t,
		ctx,
		targetPath,
		tableName,
	)
	cfg := stage4NetworkLifecycleLiveConfig(
		t,
		parsed,
		namespace,
		targetPath,
		tableName,
	)

	rawBackend := state.SQLiteStore{
		Path: filepath.Join(t.TempDir(), "state.db"),
	}
	runID := "stage4-pg-sqlite-network-resume"
	initializeStage4LifecycleRun(
		t,
		rawBackend,
		runID,
		time.Now().Add(-time.Minute),
	)
	backend := &stage4NetworkLifecycleFailAckBackend{
		Stage4StateBackend: rawBackend,
		failNext:           true,
	}
	run := Stage4RunContext{
		RunID:          runID,
		Backend:        backend,
		SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
	}
	freshEvents := make([]string, 0)
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &freshEvents,
		},
		run: run,
	}
	result, err := PostgresToSQLiteWithObserver(
		ctx,
		cfg,
		freshObserver,
	)
	if err == nil ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "injected durable acknowledgement failure") {
		t.Fatalf(
			"first Stage 4 network result=%#v error=%v, want injected acknowledgement failure",
			result,
			err,
		)
	}
	if result != (Result{}) {
		t.Fatalf("first Stage 4 network result = %#v, want zero", result)
	}
	assertStage4NetworkSQLiteRows(
		t,
		ctx,
		targetPath,
		tableName,
		[][2]int64{{1, 11}},
	)
	assertStage4NetworkLifecyclePending(
		t,
		rawBackend,
		runID,
		tableName,
	)

	run.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &resumeEvents,
		},
		run: run,
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf("resume Stage 4 PostgreSQL TLS-to-SQLite network run: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf(
			"resumed Stage 4 network result = %#v, want one validated three-row table",
			result,
		)
	}
	assertStage4NetworkSQLiteRows(
		t,
		ctx,
		targetPath,
		tableName,
		[][2]int64{{1, 11}, {2, 22}, {3, 33}},
	)
	assertStage4NetworkLifecycleCompleted(
		t,
		rawBackend,
		runID,
		tableName,
		3,
	)
	acknowledgements := backend.snapshotAcknowledgements()
	if len(acknowledgements) != 4 {
		t.Fatalf(
			"durable acknowledgements = %#v, want failed sequence zero plus three resumed chunks",
			acknowledgements,
		)
	}
	failed := acknowledgements[0]
	replayed := acknowledgements[1]
	if failed.RunID != replayed.RunID ||
		failed.Task != replayed.Task ||
		failed.RangeID != replayed.RangeID ||
		failed.TopologyHash != replayed.TopologyHash ||
		failed.Sequence != replayed.Sequence ||
		failed.ChunkRows != replayed.ChunkRows ||
		failed.AttemptOffset != replayed.AttemptOffset ||
		failed.DurableRows != replayed.DurableRows ||
		failed.FrontierValid != replayed.FrontierValid ||
		!reflect.DeepEqual(failed.Frontier, replayed.Frontier) {
		t.Fatalf(
			"resume did not replay the exact failed acknowledgement: failed=%#v replayed=%#v",
			failed,
			replayed,
		)
	}
}

func TestStage4PostgresTLSToSQLiteNetworkInteriorInsertReplayLive(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 " +
				"PostgreSQL TLS interior-insert replay sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL TLS DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL TLS setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL TLS setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("verify Stage 4 PostgreSQL TLS setup: %T", err)
	}
	var tlsActive bool
	if err := setup.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect Stage 4 PostgreSQL TLS session: %v", err)
	}
	if !tlsActive {
		t.Fatal("DMTX_TEST_POSTGRES_DSN established a non-TLS session")
	}

	namespace := "dmtx_stage4_interior_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "network_items"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop Stage 4 PostgreSQL source schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO ` + qualified + ` (id, payload) VALUES
			(10, 110),
			(30, 330),
			(50, 550)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create Stage 4 PostgreSQL source fixture: %v", err)
		}
	}

	targetPath := filepath.Join(t.TempDir(), "target.db")
	createStage4NetworkSQLiteTarget(
		t,
		ctx,
		targetPath,
		tableName,
	)
	cfg := stage4NetworkLifecycleLiveConfig(
		t,
		parsed,
		namespace,
		targetPath,
		tableName,
	)
	rawBackend := state.SQLiteStore{
		Path: filepath.Join(t.TempDir(), "state.db"),
	}
	runID := "stage4-pg-sqlite-network-interior-insert"
	initializeStage4LifecycleRun(
		t,
		rawBackend,
		runID,
		time.Now().Add(-time.Minute),
	)
	backend := &stage4NetworkLifecycleFailAckBackend{
		Stage4StateBackend: rawBackend,
		failAt:             3,
	}
	run := Stage4RunContext{
		RunID:          runID,
		Backend:        backend,
		SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
	}
	freshEvents := make([]string, 0)
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &freshEvents,
		},
		run: run,
	}
	result, err := PostgresToSQLiteWithObserver(
		ctx,
		cfg,
		freshObserver,
	)
	if err == nil ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(
			err.Error(),
			"injected durable acknowledgement failure",
		) {
		t.Fatalf(
			"initial progressed network result=%#v error=%v",
			result,
			err,
		)
	}
	task, workRange := stage4NetworkLifecycleWork(
		t,
		rawBackend,
		runID,
		tableName,
	)
	if task.Status != "running" ||
		workRange.Status != "running" ||
		workRange.RowsDone != 2 ||
		workRange.NextSequence != 2 ||
		!workRange.FrontierValid ||
		len(workRange.Frontier) != 1 ||
		workRange.Frontier[0] != state.Int64Value(30) ||
		len(workRange.Pending) != 1 {
		t.Fatalf(
			"successful pre-failure progress task=%#v range=%#v",
			task,
			workRange,
		)
	}
	assertStage4NetworkSQLiteRows(
		t,
		ctx,
		targetPath,
		tableName,
		[][2]int64{{10, 110}, {30, 330}, {50, 550}},
	)

	if _, err := setup.ExecContext(
		ctx,
		`INSERT INTO `+qualified+` (id, payload) VALUES (20, 220)`,
	); err != nil {
		t.Fatalf("insert row behind saved Stage 4 frontier: %v", err)
	}
	run.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &resumeEvents,
		},
		run: run,
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf(
			"resume Stage 4 after interior source insert: %v",
			err,
		)
	}
	if result != (Result{Tables: 1, Rows: 4, Validated: true}) {
		t.Fatalf("interior-insert resume result = %#v", result)
	}
	assertStage4NetworkSQLiteRows(
		t,
		ctx,
		targetPath,
		tableName,
		[][2]int64{
			{10, 110},
			{20, 220},
			{30, 330},
			{50, 550},
		},
	)
	assertStage4NetworkLifecycleCompleted(
		t,
		rawBackend,
		runID,
		tableName,
		4,
	)
}

func TestStage4MySQLStableRunnerLiveTLS(t *testing.T) {
	testStage4MySQLFamilyStableRunnerLiveTLS(
		t,
		mysqlRetainedLiveFixture{
			name:      "MySQL",
			dsnEnv:    "DMTX_TEST_MYSQL_DSN",
			caEnv:     "DMTX_TEST_MYSQL_CA",
			tlsConfig: "dmtx_test",
			collation: "utf8mb4_0900_bin",
		},
	)
}

func TestStage4MariaDBStableRunnerLiveTLS(t *testing.T) {
	testStage4MySQLFamilyStableRunnerLiveTLS(
		t,
		mysqlRetainedLiveFixture{
			name:      "MariaDB",
			dsnEnv:    "DMTX_TEST_MARIADB_DSN",
			caEnv:     "DMTX_TEST_MARIADB_CA",
			tlsConfig: "dmtx_mariadb_test",
			collation: "utf8mb4_nopad_bin",
		},
	)
}

func testStage4MySQLFamilyStableRunnerLiveTLS(
	t *testing.T,
	fixture mysqlRetainedLiveFixture,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				" Stage 4 stable-runner sentinel",
		)
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"Stage 4 stable-runner source",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s Stage 4 stable-runner setup: %T", fixture.name, err)
	}
	t.Cleanup(func() { _ = setup.Close() })
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping %s Stage 4 stable-runner setup: %T", fixture.name, err)
	}
	tableName := "dmtx_stage4_stable_" +
		strings.ToLower(fixture.name) + "_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	qualified := mySQLQualified(parsed.DBName, tableName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+qualified,
		); err != nil {
			t.Errorf(
				"drop %s Stage 4 stable-runner source: %v",
				fixture.name,
				err,
			)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
			fixture.collation,
		`INSERT INTO ` + qualified + ` VALUES
			(1, 11), (2, 22), (3, 33)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create %s Stage 4 stable-runner fixture: %v",
				fixture.name,
				err,
			)
		}
	}
	assertStage4RelationalStableRunnerLive(
		t,
		ctx,
		mysqlNativeTargetEndpoint(t, parsed, caPath),
		tableName,
		MySQLToSQLiteWithObserver,
	)
}

func TestStage4SQLServerStableRunnerLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" || caPath == "" || postgresDSN == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"and DMTX_TEST_POSTGRES_DSN to run the SQL Server " +
				"Stage 4 stable-runner sentinel",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	postgres, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL stable-runner target DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgres) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open SQL Server Stage 4 stable-runner setup: %T", err)
	}
	t.Cleanup(func() { _ = setup.Close() })
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server Stage 4 stable-runner setup: %T", err)
	}
	tableName := "dmtx_stage4_stable_mssql_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	qualified := sqlServerQualified("dbo", tableName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+qualified,
		); err != nil {
			t.Errorf(
				"drop SQL Server Stage 4 stable-runner source: %v",
				err,
			)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			[id] BIGINT NOT NULL,
			[payload] BIGINT NOT NULL,
			CONSTRAINT ` + sqlServerIdentifier(tableName+"_pk") +
			` PRIMARY KEY CLUSTERED ([id])
		)`,
		`INSERT INTO ` + qualified + ` VALUES
			(1, 11), (2, 22), (3, 33)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create SQL Server Stage 4 stable-runner fixture: %v",
				err,
			)
		}
	}
	targetDatabase, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL stable-runner target: %T", err)
	}
	t.Cleanup(func() { _ = targetDatabase.Close() })
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL stable-runner target: %T", err)
	}
	var targetTLS bool
	if err := targetDatabase.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&targetTLS); err != nil {
		t.Fatalf("inspect PostgreSQL stable-runner TLS session: %v", err)
	}
	if !targetTLS {
		t.Fatal("PostgreSQL stable-runner target established a non-TLS session")
	}
	targetSchema := "dmtx_stage4_stable_mssql_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := targetDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(targetSchema),
	); err != nil {
		t.Fatalf("create PostgreSQL stable-runner target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := targetDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(targetSchema)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL stable-runner target schema: %v",
				err,
			)
		}
	})
	targetQualified := postgresQualified(targetSchema, tableName)
	if _, err := targetDatabase.ExecContext(
		ctx,
		`CREATE TABLE `+targetQualified+` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
	); err != nil {
		t.Fatalf("create PostgreSQL stable-runner target table: %v", err)
	}
	targetEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     postgres.Host,
		Port:     int(postgres.Port),
		Database: postgres.Database,
		User:     postgres.User,
		Password: postgres.Password,
		Schema:   targetSchema,
		SSLMode:  "require",
	}
	runStage4RelationalStableRunnerLive(
		t,
		ctx,
		endpoint,
		targetEndpoint,
		tableName,
		SQLServerToPostgresWithObserver,
	)
	rows, err := targetDatabase.QueryContext(
		ctx,
		"SELECT id, payload FROM "+targetQualified+" ORDER BY id",
	)
	if err != nil {
		t.Fatalf("query PostgreSQL stable-runner rows: %v", err)
	}
	defer rows.Close()
	var actual [][2]int64
	for rows.Next() {
		var row [2]int64
		if err := rows.Scan(&row[0], &row[1]); err != nil {
			t.Fatalf("scan PostgreSQL stable-runner row: %v", err)
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL stable-runner rows: %v", err)
	}
	if !reflect.DeepEqual(
		actual,
		[][2]int64{{1, 11}, {2, 22}, {3, 33}},
	) {
		t.Fatalf(
			"PostgreSQL stable-runner rows = %#v, want three source rows",
			actual,
		)
	}
}

func assertStage4RelationalStableRunnerLive(
	t *testing.T,
	ctx context.Context,
	source config.Endpoint,
	tableName string,
	route func(
		context.Context,
		config.Config,
		TableObserver,
	) (Result, error),
) {
	t.Helper()
	targetPath := filepath.Join(t.TempDir(), "target.db")
	createStage4NetworkSQLiteTarget(
		t,
		ctx,
		targetPath,
		tableName,
	)
	runStage4RelationalStableRunnerLive(
		t,
		ctx,
		source,
		config.Endpoint{
			Type:     "sqlite",
			Database: targetPath,
		},
		tableName,
		route,
	)
	assertStage4NetworkSQLiteRows(
		t,
		ctx,
		targetPath,
		tableName,
		[][2]int64{{1, 11}, {2, 22}, {3, 33}},
	)
}

func runStage4RelationalStableRunnerLive(
	t *testing.T,
	ctx context.Context,
	source config.Endpoint,
	target config.Endpoint,
	tableName string,
	route func(
		context.Context,
		config.Config,
		TableObserver,
	) (Result, error),
) {
	t.Helper()
	cfg := stage4RelationalStableRunnerConfig(
		t,
		source,
		target,
		tableName,
	)
	backend := state.SQLiteStore{
		Path: filepath.Join(t.TempDir(), "state.db"),
	}
	runID := "stage4-stable-" + source.Type + "-" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &events,
		},
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        backend,
			SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
		},
	}
	result, err := route(ctx, cfg, observer)
	if err != nil {
		t.Fatalf(
			"run Stage 4 %s stable source with one connection: %v",
			source.Type,
			err,
		)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf(
			"Stage 4 %s stable-runner result = %#v",
			source.Type,
			result,
		)
	}
	// A returned Result only says the call succeeded. The transfer-lifecycle
	// requirement is that the run completed *through the resumable range
	// protocol*, so the durable work task and its range must both be terminal
	// with the exact row count. Without this, a route that transferred correctly
	// but left its durable evidence unfinished would pass.
	assertStage4NetworkLifecycleCompleted(
		t,
		backend,
		runID,
		tableName,
		3,
	)
}

func stage4RelationalStableRunnerConfig(
	t *testing.T,
	source config.Endpoint,
	target config.Endpoint,
	tableName string,
) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`
source:
  type: postgres
  host: placeholder.invalid
  database: placeholder
  user: placeholder
target:
  type: sqlite
  database: /tmp/dmtx-stage4-network-placeholder.db
migration:
  target_mode: upsert
  include_tables:
    - network_items
  connection_limit: 2
  workers: 2
  chunk_size: 1
  partitions: 1
  reader_parallelism: 1
  writer_parallelism: 1
  read_ahead: 1
  max_retries: 0
  runtime_tuning: false
  validation:
    mode: count_only
`))
	if err != nil {
		t.Fatalf("parse Stage 4 stable-runner config: %v", err)
	}
	cfg.Source = source
	cfg.Target = target
	cfg.Migration.IncludeTables = []string{tableName}
	return cfg
}

type stage4NetworkLifecycleFailAckBackend struct {
	Stage4StateBackend

	mu       sync.Mutex
	failNext bool
	failAt   int
	acks     []state.RangeAcknowledgement
}

func (backend *stage4NetworkLifecycleFailAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	recorded := acknowledgement
	recorded.Frontier = append(
		state.TypedTuple(nil),
		acknowledgement.Frontier...,
	)
	backend.mu.Lock()
	backend.acks = append(backend.acks, recorded)
	fail := backend.failNext ||
		backend.failAt > 0 &&
			len(backend.acks) == backend.failAt
	backend.failNext = false
	backend.mu.Unlock()
	if fail {
		return state.RangeState{}, errors.New(
			"injected durable acknowledgement failure",
		)
	}
	return backend.Stage4StateBackend.AcknowledgeRange(acknowledgement)
}

func (backend *stage4NetworkLifecycleFailAckBackend) snapshotAcknowledgements() []state.RangeAcknowledgement {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	result := make([]state.RangeAcknowledgement, len(backend.acks))
	for index := range backend.acks {
		result[index] = backend.acks[index]
		result[index].Frontier = append(
			state.TypedTuple(nil),
			backend.acks[index].Frontier...,
		)
	}
	return result
}

func stage4NetworkLifecycleLiveConfig(
	t *testing.T,
	postgres *pgx.ConnConfig,
	namespace string,
	targetPath string,
	tableName string,
) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`
source:
  type: postgres
  host: placeholder.invalid
  database: placeholder
  user: placeholder
target:
  type: sqlite
  database: /tmp/dmtx-stage4-network-placeholder.db
migration:
  target_mode: upsert
  include_tables:
    - network_items
  connection_limit: 2
  workers: 2
  chunk_size: 1
  partitions: 1
  reader_parallelism: 1
  writer_parallelism: 1
  read_ahead: 1
  max_retries: 0
  runtime_tuning: false
  validation:
    mode: count_only
`))
	if err != nil {
		t.Fatalf("parse Stage 4 network lifecycle configuration: %v", err)
	}
	cfg.Source = config.Endpoint{
		Type:     "postgres",
		Host:     postgres.Host,
		Port:     int(postgres.Port),
		Database: postgres.Database,
		User:     postgres.User,
		Password: postgres.Password,
		Schema:   namespace,
		SSLMode:  "require",
	}
	cfg.Target = config.Endpoint{
		Type:     "sqlite",
		Database: targetPath,
	}
	cfg.Migration.IncludeTables = []string{tableName}
	return cfg
}

func createStage4NetworkSQLiteTarget(
	t *testing.T,
	ctx context.Context,
	path string,
	tableName string,
) {
	t.Helper()
	database, err := openSQLiteTargetDatabase(ctx, path)
	if err != nil {
		t.Fatalf("open Stage 4 SQLite target fixture: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+quote(tableName)+` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
	); err != nil {
		t.Fatalf("create Stage 4 SQLite target fixture: %v", err)
	}
}

func assertStage4NetworkSQLiteRows(
	t *testing.T,
	ctx context.Context,
	path string,
	tableName string,
	want [][2]int64,
) {
	t.Helper()
	database, err := openSQLiteTargetDatabase(ctx, path)
	if err != nil {
		t.Fatalf("open Stage 4 SQLite target assertion: %v", err)
	}
	defer database.Close()
	rows, err := database.QueryContext(
		ctx,
		"SELECT id, payload FROM "+quote(tableName)+" ORDER BY id",
	)
	if err != nil {
		t.Fatalf("read Stage 4 SQLite target rows: %v", err)
	}
	defer rows.Close()
	got := make([][2]int64, 0, len(want))
	for rows.Next() {
		var item [2]int64
		if err := rows.Scan(&item[0], &item[1]); err != nil {
			t.Fatalf("scan Stage 4 SQLite target row: %v", err)
		}
		got = append(got, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Stage 4 SQLite target rows: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Stage 4 SQLite target rows = %#v, want %#v", got, want)
	}
}

func assertStage4NetworkLifecyclePending(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	tableName string,
) {
	t.Helper()
	task, workRange := stage4NetworkLifecycleWork(
		t,
		backend,
		runID,
		tableName,
	)
	if task.Status != "running" ||
		workRange.Status != "running" ||
		workRange.NextSequence != 0 ||
		workRange.SequenceOffset != 0 ||
		workRange.RowsDone != 0 ||
		len(workRange.Pending) != 1 ||
		workRange.Pending[0].Sequence != 0 ||
		workRange.Pending[0].ChunkRows != 1 ||
		workRange.Pending[0].Attempts != 1 {
		t.Fatalf(
			"failed Stage 4 network durability task=%#v range=%#v",
			task,
			workRange,
		)
	}
}

func assertStage4NetworkLifecycleCompleted(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	tableName string,
	rows int64,
) {
	t.Helper()
	task, workRange := stage4NetworkLifecycleWork(
		t,
		backend,
		runID,
		tableName,
	)
	if task.Status != "completed" ||
		workRange.Status != "completed" ||
		workRange.RowsDone != rows ||
		workRange.SequenceOffset != 0 ||
		len(workRange.Pending) != 0 {
		t.Fatalf(
			"completed Stage 4 network durability task=%#v range=%#v",
			task,
			workRange,
		)
	}
}

func stage4NetworkLifecycleWork(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	tableName string,
) (state.WorkTask, state.RangeState) {
	t.Helper()
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatalf("list Stage 4 network durability: %v", err)
	}
	key := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "",
		Table:  tableName,
	}
	var (
		task      state.WorkTask
		workRange state.RangeState
		taskFound bool
		rangeSeen bool
	)
	for _, candidate := range tasks {
		if candidate.Key.Type == key.Type &&
			candidate.Key.Table == key.Table {
			if taskFound {
				t.Fatalf(
					"duplicate Stage 4 network task for %s",
					tableName,
				)
			}
			task = candidate
			key = candidate.Key
			taskFound = true
		}
	}
	for _, candidate := range ranges {
		if taskFound && candidate.Task == key {
			if rangeSeen {
				t.Fatalf(
					"duplicate Stage 4 network range for %s",
					tableName,
				)
			}
			workRange = candidate
			rangeSeen = true
		}
	}
	if !taskFound || !rangeSeen {
		t.Fatalf(
			"missing Stage 4 network durability for %s: tasks=%#v ranges=%#v",
			tableName,
			tasks,
			ranges,
		)
	}
	return task, workRange
}
