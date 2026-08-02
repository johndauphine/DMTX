package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4IncrementalCertifiedRouteMatrixLiveTLS is the real-driver
// incremental matrix for every currently admitted canonical relational/SQLite
// cell. Each child uses the production composed route, precreates the exact
// production projection, and records an after-fence source insert only after
// its immutable upper fence is durable. The inserted row must be absent from
// the transferred attempt and its final sample/count/NULL-parity validation.
func TestStage4IncrementalCertifiedRouteMatrixLiveTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	environment := newStage4IncrementalLiveMatrixEnvironment(t, ctx)
	engines := []string{"postgres", "mssql", "mysql", "sqlite"}
	for _, sourceEngine := range engines {
		for _, targetEngine := range engines {
			sourceEngine, targetEngine := sourceEngine, targetEngine
			t.Run(sourceEngine+"-to-"+targetEngine, func(t *testing.T) {
				fixture := environment.newRoute(
					t,
					ctx,
					sourceEngine,
					targetEngine,
				)
				fixture.runPostFenceWindow(t, ctx)
			})
		}
	}
}

// TestStage4IncrementalMariaDBFamilyAliasLiveTLS exercises the canonical
// mysql-to-mysql cell against the separately pinned MariaDB 10.11 fixture.
// MariaDB is a public configuration alias for mysql rather than a fifth
// capability cell, so the 4x4 matrix above remains canonical.
func TestStage4IncrementalMariaDBFamilyAliasLiveTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	environment := newStage4IncrementalMariaDBFamilyEnvironment(t, ctx)
	fixture := environment.newRoute(t, ctx, "mysql", "mysql")
	fixture.runPostFenceWindow(t, ctx)
}

type stage4IncrementalLiveMatrixEnvironment struct {
	postgresDSN      string
	postgresEndpoint config.Endpoint
	postgresDatabase *sql.DB

	mySQLSourceEndpoint config.Endpoint
	mySQLSourceDatabase *sql.DB
	mySQLTargetEndpoint config.Endpoint
	mySQLTargetDatabase *sql.DB
	mySQLTableCollation string

	sqlServerSourceEndpoint config.Endpoint
	sqlServerSourceDatabase *sql.DB
	sqlServerTargetEndpoint config.Endpoint
	sqlServerTargetDatabase *sql.DB
}

func newStage4IncrementalLiveMatrixEnvironment(
	t *testing.T,
	ctx context.Context,
) *stage4IncrementalLiveMatrixEnvironment {
	t.Helper()
	if os.Getenv("DMTX_STAGE4_LIVE_REQUIRED") != "1" {
		t.Skip(
			"set DMTX_STAGE4_LIVE_REQUIRED=1 and the relational fixture variables to run the incremental live route matrix",
		)
	}
	required := []string{
		"DMTX_TEST_POSTGRES_DSN",
		"DMTX_TEST_MYSQL_DSN",
		"DMTX_TEST_MYSQL_TARGET_DSN",
		"DMTX_TEST_MYSQL_CA",
		"DMTX_TEST_MSSQL_DSN",
		"DMTX_TEST_MSSQL_TARGET_DSN",
		"DMTX_TEST_MSSQL_CA",
	}
	missing := make([]string, 0, len(required))
	for _, name := range required {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Fatalf(
			"armed incremental live route matrix is missing fixture variables: %s",
			strings.Join(missing, ", "),
		)
	}

	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	parsedPostgres, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL incremental matrix DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsedPostgres) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require verified TLS")
	}
	postgresCA := stage4PostgresDeleteLiveCAFile(t, parsedPostgres.ConnString())
	postgresEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      parsedPostgres.Host,
		Port:      int(parsedPostgres.Port),
		Database:  parsedPostgres.Database,
		User:      parsedPostgres.User,
		Password:  parsedPostgres.Password,
		Schema:    "public",
		SSLMode:   "verify-full",
		TLSCAFile: postgresCA,
	}
	if postgresEndpoint.SSLMode != "verify-full" ||
		postgresEndpoint.TLSCAFile != postgresCA {
		t.Fatal("production PostgreSQL incremental endpoint lost verified TLS authority")
	}
	postgresDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"pgx",
		postgresDSN,
		"PostgreSQL incremental matrix",
	)
	assertStage4IncrementalPostgresTLS(
		t,
		ctx,
		postgresDatabase,
		"matrix fixture",
	)

	mySQLCA := os.Getenv("DMTX_TEST_MYSQL_CA")
	registerMySQLCommonFixtureTLSNamed(t, mySQLCA, "dmtx_test")
	mySQLSourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	mySQLTargetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	parsedMySQLSource := parseMySQLNativeTargetDSNForTLS(
		t,
		"incremental matrix source",
		mySQLSourceDSN,
		"dmtx_test",
	)
	parsedMySQLTarget := parseMySQLNativeTargetDSNForTLS(
		t,
		"incremental matrix target",
		mySQLTargetDSN,
		"dmtx_test",
	)
	mySQLSourceEndpoint := mysqlNativeTargetEndpoint(
		t,
		parsedMySQLSource,
		mySQLCA,
	)
	mySQLTargetEndpoint := mysqlNativeTargetEndpoint(
		t,
		parsedMySQLTarget,
		mySQLCA,
	)
	if mySQLSourceEndpoint.SSLMode != "verify-full" ||
		mySQLSourceEndpoint.TLSCAFile != mySQLCA ||
		mySQLTargetEndpoint.SSLMode != "verify-full" ||
		mySQLTargetEndpoint.TLSCAFile != mySQLCA {
		t.Fatal("production MySQL incremental endpoint lost verified TLS authority")
	}
	mySQLSourceDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"mysql",
		mySQLSourceDSN,
		"MySQL incremental matrix source",
	)
	stage4IncrementalLiveAssertMySQLTLS(
		t,
		ctx,
		mySQLSourceDatabase,
		"source",
	)
	mySQLTargetDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"mysql",
		mySQLTargetDSN,
		"MySQL incremental matrix target",
	)
	stage4IncrementalLiveAssertMySQLTLS(
		t,
		ctx,
		mySQLTargetDatabase,
		"target",
	)

	sqlServerSourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	sqlServerTargetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	sqlServerCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	sqlServerSourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sqlServerSourceDSN,
		sqlServerCA,
	)
	sqlServerTargetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sqlServerTargetDSN,
		sqlServerCA,
	)
	if sqlServerSourceEndpoint.SSLMode != "verify-full" ||
		sqlServerSourceEndpoint.TLSCAFile != sqlServerCA ||
		sqlServerTargetEndpoint.SSLMode != "verify-full" ||
		sqlServerTargetEndpoint.TLSCAFile != sqlServerCA {
		t.Fatal("production SQL Server incremental endpoint lost verified TLS authority")
	}
	sqlServerSourceDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"sqlserver",
		sqlServerSourceDSN,
		"SQL Server incremental matrix source",
	)
	sqlServerTargetDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"sqlserver",
		sqlServerTargetDSN,
		"SQL Server incremental matrix target",
	)

	return &stage4IncrementalLiveMatrixEnvironment{
		postgresDSN:             postgresDSN,
		postgresEndpoint:        postgresEndpoint,
		postgresDatabase:        postgresDatabase,
		mySQLSourceEndpoint:     mySQLSourceEndpoint,
		mySQLSourceDatabase:     mySQLSourceDatabase,
		mySQLTargetEndpoint:     mySQLTargetEndpoint,
		mySQLTargetDatabase:     mySQLTargetDatabase,
		mySQLTableCollation:     "utf8mb4_0900_bin",
		sqlServerSourceEndpoint: sqlServerSourceEndpoint,
		sqlServerSourceDatabase: sqlServerSourceDatabase,
		sqlServerTargetEndpoint: sqlServerTargetEndpoint,
		sqlServerTargetDatabase: sqlServerTargetDatabase,
	}
}

func newStage4IncrementalMariaDBFamilyEnvironment(
	t *testing.T,
	ctx context.Context,
) *stage4IncrementalLiveMatrixEnvironment {
	t.Helper()
	if os.Getenv("DMTX_STAGE4_LIVE_REQUIRED") != "1" {
		t.Skip(
			"set DMTX_STAGE4_LIVE_REQUIRED=1 and the MariaDB fixture variables to run the incremental MariaDB family route",
		)
	}
	required := []string{
		"DMTX_TEST_MARIADB_DSN",
		"DMTX_TEST_MARIADB_TARGET_DSN",
		"DMTX_TEST_MARIADB_CA",
	}
	missing := make([]string, 0, len(required))
	for _, name := range required {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Skip(
			"armed incremental MariaDB family route is missing fixture variables: " +
				strings.Join(missing, ", "),
		)
	}

	caPath := os.Getenv("DMTX_TEST_MARIADB_CA")
	registerMySQLCommonFixtureTLSNamed(t, caPath, "dmtx_mariadb_test")
	sourceDSN := os.Getenv("DMTX_TEST_MARIADB_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MARIADB_TARGET_DSN")
	parsedSource := parseMySQLNativeTargetDSNForTLS(
		t,
		"incremental MariaDB source",
		sourceDSN,
		"dmtx_mariadb_test",
	)
	parsedTarget := parseMySQLNativeTargetDSNForTLS(
		t,
		"incremental MariaDB target",
		targetDSN,
		"dmtx_mariadb_test",
	)
	sourceDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"mysql",
		sourceDSN,
		"MariaDB incremental matrix source",
	)
	stage4IncrementalLiveAssertMySQLTLS(t, ctx, sourceDatabase, "MariaDB source")
	flavor, err := engine.DetectMySQLServerFlavor(ctx, sourceDatabase)
	if err != nil {
		t.Fatalf("detect MariaDB incremental source flavor: %v", err)
	}
	if flavor != engine.MySQLServerFlavorMariaDB1011 {
		t.Fatalf("MariaDB incremental source flavor = %v, want 10.11", flavor)
	}
	targetDatabase := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"mysql",
		targetDSN,
		"MariaDB incremental matrix target",
	)
	stage4IncrementalLiveAssertMySQLTLS(t, ctx, targetDatabase, "MariaDB target")
	if flavor, err := engine.DetectMySQLServerFlavor(ctx, targetDatabase); err != nil {
		t.Fatalf("detect MariaDB incremental target flavor: %v", err)
	} else if flavor != engine.MySQLServerFlavorMariaDB1011 {
		t.Fatalf("MariaDB incremental target flavor = %v, want 10.11", flavor)
	}
	sourceEndpoint := mysqlNativeTargetEndpoint(t, parsedSource, caPath)
	targetEndpoint := mysqlNativeTargetEndpoint(t, parsedTarget, caPath)
	if sourceEndpoint.SSLMode != "verify-full" ||
		sourceEndpoint.TLSCAFile != caPath ||
		targetEndpoint.SSLMode != "verify-full" ||
		targetEndpoint.TLSCAFile != caPath {
		t.Fatal("production MariaDB incremental endpoint lost verified TLS authority")
	}
	return &stage4IncrementalLiveMatrixEnvironment{
		mySQLSourceEndpoint: sourceEndpoint,
		mySQLSourceDatabase: sourceDatabase,
		mySQLTargetEndpoint: targetEndpoint,
		mySQLTargetDatabase: targetDatabase,
		mySQLTableCollation: "utf8mb4_nopad_bin",
	}
}

func stage4IncrementalLiveOpenDatabase(
	t *testing.T,
	ctx context.Context,
	driver string,
	dsn string,
	label string,
) *sql.DB {
	t.Helper()
	database, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %T", label, err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close %s: %v", label, err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %T", label, err)
	}
	return database
}

func stage4IncrementalLiveAssertMySQLTLS(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	label string,
) {
	t.Helper()
	var variable, cipher string
	if err := database.QueryRowContext(
		ctx,
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
	).Scan(&variable, &cipher); err != nil {
		t.Fatalf("inspect MySQL incremental matrix %s TLS: %T", label, err)
	}
	if variable != "Ssl_cipher" || cipher == "" {
		t.Fatalf("MySQL incremental matrix %s is not using TLS", label)
	}
}

type stage4IncrementalLiveRouteFixture struct {
	sourceEngine string
	targetEngine string

	sourceEndpoint         config.Endpoint
	targetEndpoint         config.Endpoint
	sourceDatabase         *sql.DB
	sourceMutationDatabase *sql.DB
	targetDatabase         *sql.DB

	sourceTable schema.Table
	targetTable schema.Table

	mySQLTableCollation string
}

func (environment *stage4IncrementalLiveMatrixEnvironment) newRoute(
	t *testing.T,
	ctx context.Context,
	sourceEngine string,
	targetEngine string,
) *stage4IncrementalLiveRouteFixture {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "dmtx_inc_" + sourceEngine + "_" + targetEngine + "_" + suffix
	fixture := &stage4IncrementalLiveRouteFixture{
		sourceEngine:        sourceEngine,
		targetEngine:        targetEngine,
		mySQLTableCollation: environment.mySQLTableCollation,
		sourceTable: schema.Table{
			Name: tableName,
		},
	}
	environment.configureSource(t, ctx, fixture, suffix)
	environment.configureTarget(t, ctx, fixture, suffix)
	if err := fixture.createSourceTable(ctx); err != nil {
		t.Fatalf("create incremental matrix %s source table: %v", sourceEngine, err)
	}
	if err := fixture.openSQLiteSourceMutationDatabase(t, ctx); err != nil {
		t.Fatalf("open incremental matrix SQLite source mutation connection: %v", err)
	}
	if err := fixture.precreateTarget(ctx); err != nil {
		t.Fatalf(
			"precreate incremental matrix %s-to-%s target: %v",
			sourceEngine,
			targetEngine,
			err,
		)
	}
	return fixture
}

func (environment *stage4IncrementalLiveMatrixEnvironment) configureSource(
	t *testing.T,
	ctx context.Context,
	fixture *stage4IncrementalLiveRouteFixture,
	suffix string,
) {
	t.Helper()
	switch fixture.sourceEngine {
	case "postgres":
		fixture.sourceEndpoint = environment.postgresEndpoint
		fixture.sourceEndpoint.Schema = "dmtx_inc_src_" + suffix
		fixture.sourceDatabase = environment.postgresDatabase
		if _, err := fixture.sourceDatabase.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(fixture.sourceEndpoint.Schema),
		); err != nil {
			t.Fatalf("create PostgreSQL incremental matrix source schema: %v", err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.sourceDatabase.ExecContext(
				cleanup,
				"DROP SCHEMA IF EXISTS "+postgresIdentifier(fixture.sourceEndpoint.Schema)+" CASCADE",
			); err != nil {
				t.Errorf("drop PostgreSQL incremental matrix source schema: %v", err)
			}
		})
	case "mysql":
		fixture.sourceEndpoint = environment.mySQLSourceEndpoint
		fixture.sourceDatabase = environment.mySQLSourceDatabase
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.sourceDatabase.ExecContext(
				cleanup,
				"DROP TABLE IF EXISTS "+fixture.sourceQualified(),
			); err != nil {
				t.Errorf("drop MySQL incremental matrix source table: %v", err)
			}
		})
	case "mssql":
		fixture.sourceEndpoint = environment.sqlServerSourceEndpoint
		fixture.sourceDatabase = environment.sqlServerSourceDatabase
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.sourceDatabase.ExecContext(
				cleanup,
				"DROP TABLE IF EXISTS "+fixture.sourceQualified(),
			); err != nil {
				t.Errorf("drop SQL Server incremental matrix source table: %v", err)
			}
		})
	case "sqlite":
		path := filepath.Join(t.TempDir(), "source.sqlite")
		fixture.sourceEndpoint = config.Endpoint{Type: "sqlite", Database: path}
		fixture.sourceDatabase = stage4IncrementalLiveOpenDatabase(
			t,
			ctx,
			"sqlite",
			path,
			"SQLite incremental matrix source",
		)
	default:
		t.Fatalf("unknown incremental matrix source engine %q", fixture.sourceEngine)
	}
}

func (environment *stage4IncrementalLiveMatrixEnvironment) configureTarget(
	t *testing.T,
	ctx context.Context,
	fixture *stage4IncrementalLiveRouteFixture,
	suffix string,
) {
	t.Helper()
	switch fixture.targetEngine {
	case "postgres":
		fixture.targetEndpoint = environment.postgresEndpoint
		fixture.targetEndpoint.Schema = "dmtx_inc_tgt_" + suffix
		fixture.targetDatabase = environment.postgresDatabase
		if fixture.sourceEngine == "postgres" {
			databaseName := "dmtx_inc_tgt_" + suffix
			if _, err := environment.postgresDatabase.ExecContext(
				ctx,
				"CREATE DATABASE "+postgresIdentifier(databaseName),
			); err != nil {
				t.Fatalf("create PostgreSQL incremental matrix target database: %v", err)
			}
			fixture.targetEndpoint.Database = databaseName
			targetDSN, err := engine.PostgresDSN(fixture.targetEndpoint)
			if err != nil {
				t.Fatalf("build PostgreSQL incremental matrix target DSN: %v", err)
			}
			fixture.targetDatabase = stage4IncrementalLiveOpenDatabase(
				t,
				ctx,
				"pgx",
				targetDSN,
				"PostgreSQL incremental matrix target",
			)
			assertStage4IncrementalPostgresTLS(
				t,
				ctx,
				fixture.targetDatabase,
				"matrix target",
			)
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if _, err := environment.postgresDatabase.ExecContext(
					cleanup,
					"DROP DATABASE IF EXISTS "+postgresIdentifier(databaseName)+" WITH (FORCE)",
				); err != nil {
					t.Errorf("drop PostgreSQL incremental matrix target database: %v", err)
				}
			})
		}
		if _, err := fixture.targetDatabase.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(fixture.targetEndpoint.Schema),
		); err != nil {
			t.Fatalf("create PostgreSQL incremental matrix target schema: %v", err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.targetDatabase.ExecContext(
				cleanup,
				"DROP SCHEMA IF EXISTS "+postgresIdentifier(fixture.targetEndpoint.Schema)+" CASCADE",
			); err != nil {
				t.Errorf("drop PostgreSQL incremental matrix target schema: %v", err)
			}
		})
	case "mysql":
		fixture.targetEndpoint = environment.mySQLTargetEndpoint
		fixture.targetDatabase = environment.mySQLTargetDatabase
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.targetDatabase.ExecContext(
				cleanup,
				"DROP TABLE IF EXISTS "+fixture.targetQualified(),
			); err != nil {
				t.Errorf("drop MySQL incremental matrix target table: %v", err)
			}
		})
	case "mssql":
		fixture.targetEndpoint = environment.sqlServerTargetEndpoint
		fixture.targetDatabase = environment.sqlServerTargetDatabase
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := fixture.targetDatabase.ExecContext(
				cleanup,
				"DROP TABLE IF EXISTS "+fixture.targetQualified(),
			); err != nil {
				t.Errorf("drop SQL Server incremental matrix target table: %v", err)
			}
		})
	case "sqlite":
		path := filepath.Join(t.TempDir(), "target.sqlite")
		fixture.targetEndpoint = config.Endpoint{Type: "sqlite", Database: path}
		fixture.targetDatabase = stage4IncrementalLiveOpenDatabase(
			t,
			ctx,
			"sqlite",
			path,
			"SQLite incremental matrix target",
		)
	default:
		t.Fatalf("unknown incremental matrix target engine %q", fixture.targetEngine)
	}
}

func (fixture *stage4IncrementalLiveRouteFixture) sourceQualified() string {
	switch fixture.sourceEngine {
	case "postgres":
		return postgresQualified(fixture.sourceEndpoint.Schema, fixture.sourceTable.Name)
	case "mysql":
		return mySQLQualified(fixture.sourceEndpoint.Database, fixture.sourceTable.Name)
	case "mssql":
		return sqlServerQualified(fixture.sourceEndpoint.Schema, fixture.sourceTable.Name)
	case "sqlite":
		return quote(fixture.sourceTable.Name)
	default:
		return ""
	}
}

func (fixture *stage4IncrementalLiveRouteFixture) targetQualified() string {
	table := fixture.targetTable
	if table.Name == "" {
		table = fixture.sourceTable
	}
	switch fixture.targetEngine {
	case "postgres":
		return postgresQualified(fixture.targetEndpoint.Schema, table.Name)
	case "mysql":
		return mySQLQualified(fixture.targetEndpoint.Database, table.Name)
	case "mssql":
		return sqlServerQualified(fixture.targetEndpoint.Schema, table.Name)
	case "sqlite":
		return quote(table.Name)
	default:
		return ""
	}
}

func (fixture *stage4IncrementalLiveRouteFixture) createSourceTable(
	ctx context.Context,
) error {
	var statement string
	switch fixture.sourceEngine {
	case "postgres":
		statement = `CREATE TABLE ` + fixture.sourceQualified() + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL,
			note TEXT NULL,
			updated_at TIMESTAMP(3) NOT NULL
		)`
	case "mysql":
		collation := fixture.mySQLTableCollation
		if collation == "" {
			collation = "utf8mb4_0900_bin"
		}
		statement = `CREATE TABLE ` + fixture.sourceQualified() + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			payload VARCHAR(64) NOT NULL,
			note VARCHAR(64) NULL,
			updated_at DATETIME(3) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` + collation
	case "mssql":
		statement = `CREATE TABLE ` + fixture.sourceQualified() + ` (
			[id] BIGINT NOT NULL PRIMARY KEY,
			[payload] VARCHAR(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			[note] VARCHAR(64) COLLATE Latin1_General_100_BIN2_UTF8 NULL,
			[updated_at] DATETIME2(3) NOT NULL
		)`
	case "sqlite":
		if _, err := fixture.sourceDatabase.ExecContext(
			ctx,
			"PRAGMA journal_mode = WAL",
		); err != nil {
			return fmt.Errorf("enable SQLite source WAL: %w", err)
		}
		statement = `CREATE TABLE ` + fixture.sourceQualified() + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL,
			note TEXT NULL,
			updated_at DATETIME(3) NOT NULL
		)`
	default:
		return fmt.Errorf("unknown source engine %q", fixture.sourceEngine)
	}
	if _, err := fixture.sourceDatabase.ExecContext(ctx, statement); err != nil {
		return err
	}
	if err := fixture.insertSource(
		ctx,
		1,
		"baseline-one",
		nil,
		time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
	); err != nil {
		return err
	}
	note := "baseline-note"
	return fixture.insertSource(
		ctx,
		2,
		"baseline-two",
		&note,
		time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
	)
}

func (fixture *stage4IncrementalLiveRouteFixture) openSQLiteSourceMutationDatabase(
	t *testing.T,
	ctx context.Context,
) error {
	if fixture.sourceEngine != "sqlite" || fixture.sourceMutationDatabase != nil {
		return nil
	}
	database, err := sql.Open(
		"sqlite",
		sqliteSourceTestURI(fixture.sourceEndpoint.Database, "rw"),
	)
	if err != nil {
		return err
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close SQLite incremental source mutation connection: %v", closeErr)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		return err
	}
	fixture.sourceMutationDatabase = database
	return nil
}

func (fixture *stage4IncrementalLiveRouteFixture) insertSource(
	ctx context.Context,
	id int64,
	payload string,
	note *string,
	updatedAt time.Time,
) error {
	noteValue := any(nil)
	if note != nil {
		noteValue = *note
	}
	query := ""
	arguments := []any{id, payload, noteValue, fixture.timestampArgument(updatedAt)}
	switch fixture.sourceEngine {
	case "postgres":
		query = "INSERT INTO " + fixture.sourceQualified() +
			" (id, payload, note, updated_at) VALUES ($1, $2, $3, $4)"
	case "mysql", "sqlite":
		query = "INSERT INTO " + fixture.sourceQualified() +
			" (id, payload, note, updated_at) VALUES (?, ?, ?, ?)"
	case "mssql":
		query = "INSERT INTO " + fixture.sourceQualified() +
			" ([id], [payload], [note], [updated_at]) VALUES (@p1, @p2, @p3, @p4)"
	default:
		return fmt.Errorf("unknown source engine %q", fixture.sourceEngine)
	}
	database := fixture.sourceDatabase
	if fixture.sourceMutationDatabase != nil {
		database = fixture.sourceMutationDatabase
	}
	_, err := database.ExecContext(ctx, query, arguments...)
	return err
}

func (fixture *stage4IncrementalLiveRouteFixture) updateSource(
	ctx context.Context,
	id int64,
	payload string,
	updatedAt time.Time,
) error {
	query := ""
	arguments := []any{payload, fixture.timestampArgument(updatedAt), id}
	switch fixture.sourceEngine {
	case "postgres":
		query = "UPDATE " + fixture.sourceQualified() +
			" SET payload = $1, updated_at = $2 WHERE id = $3"
	case "mysql", "sqlite":
		query = "UPDATE " + fixture.sourceQualified() +
			" SET payload = ?, updated_at = ? WHERE id = ?"
	case "mssql":
		query = "UPDATE " + fixture.sourceQualified() +
			" SET [payload] = @p1, [updated_at] = @p2 WHERE [id] = @p3"
	default:
		return fmt.Errorf("unknown source engine %q", fixture.sourceEngine)
	}
	database := fixture.sourceDatabase
	if fixture.sourceMutationDatabase != nil {
		database = fixture.sourceMutationDatabase
	}
	result, err := database.ExecContext(ctx, query, arguments...)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("update source id %d affected %d rows", id, changed)
	}
	return nil
}

func (fixture *stage4IncrementalLiveRouteFixture) timestampArgument(
	value time.Time,
) any {
	if fixture.sourceEngine == "sqlite" {
		return value.UTC().Format("2006-01-02 15:04:05.000")
	}
	return value.UTC()
}

func (fixture *stage4IncrementalLiveRouteFixture) precreateTarget(
	ctx context.Context,
) (resultErr error) {
	source, err := builtInAdapters.sources[fixture.sourceEngine].open(
		ctx,
		fixture.sourceEndpoint,
	)
	if err != nil {
		return fmt.Errorf("open source adapter: %w", err)
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	table, err := source.InspectTable(ctx, fixture.sourceTable.Name)
	if err != nil {
		return fmt.Errorf("inspect source table: %w", err)
	}
	target, err := builtInAdapters.targets[fixture.targetEngine].open(
		ctx,
		fixture.targetEndpoint,
	)
	if err != nil {
		return fmt.Errorf("open target adapter: %w", err)
	}
	defer func() {
		if closeErr := target.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	planned, err := target.PlanTables(
		fixture.sourceEngine,
		[]schema.Table{table},
		"upsert",
	)
	if err != nil {
		return fmt.Errorf("plan target table: %w", err)
	}
	if len(planned) != 1 {
		return fmt.Errorf("target plan has %d tables", len(planned))
	}
	statement := ""
	switch typed := target.(type) {
	case *postgresTargetAdapter:
		statement, err = schema.CreateTable(schema.Postgres, planned[0])
		if err == nil {
			_, err = typed.database.ExecContext(ctx, statement)
		}
	case *mysqlTargetAdapter:
		statement, err = schema.CreateTable(schema.MySQL, planned[0])
		if err == nil {
			_, err = typed.database.ExecContext(ctx, statement)
		}
	case *sqlServerTargetAdapter:
		statement, err = schema.CreateSQLServerTable(planned[0])
		if err == nil {
			_, err = typed.database.ExecContext(ctx, statement)
		}
	case *sqliteTargetAdapter:
		statement, err = schema.CreateTable(schema.SQLite, planned[0])
		if err == nil {
			_, err = typed.database.ExecContext(ctx, statement)
		}
	default:
		return fmt.Errorf("target adapter %T has no fixture DDL handle", target)
	}
	if err != nil {
		return fmt.Errorf("create exact projected target: %w", err)
	}
	fixture.sourceTable = table
	fixture.targetTable = planned[0]
	return nil
}

func (fixture *stage4IncrementalLiveRouteFixture) incrementalConfig() config.Config {
	return config.Config{
		Source: fixture.sourceEndpoint,
		Target: fixture.targetEndpoint,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{fixture.sourceTable.Name},
			DateUpdatedColumns: []string{"updated_at"},
			ConnectionLimit:    4,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			MemoryCeilingBytes: 64 << 20,
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationSample,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
}

func (fixture *stage4IncrementalLiveRouteFixture) runPostFenceWindow(
	t *testing.T,
	ctx context.Context,
) {
	t.Helper()
	config := fixture.incrementalConfig()
	store := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	baselineRun := "incremental-matrix-baseline-" + fixture.sourceEngine + "-" + fixture.targetEngine
	initializeStage4LifecycleRun(
		t,
		store,
		baselineRun,
		time.Now().Add(-time.Minute),
	)
	baselineEvents := make([]string, 0)
	baselineObserver := stage4IncrementalTestObserver{
		events:  &baselineEvents,
		backend: store,
		run: stage4LifecycleRunContext(
			t,
			store,
			baselineRun,
			false,
		),
	}
	result, err := Execute(ctx, config, baselineObserver)
	if err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("incremental baseline result=%#v err=%v", result, err)
	}
	publishStage4IncrementalLiveRun(t, baselineObserver.run)

	upperFence := time.Date(2026, 7, 30, 10, 2, 0, 0, time.UTC)
	if err := fixture.updateSource(ctx, 1, "window-value", upperFence); err != nil {
		t.Fatalf("prepare incremental window source row: %v", err)
	}
	postFence := upperFence.Add(time.Second)
	mutationBackend := &stage4IncrementalLivePostFenceMutationBackend{
		stage4IncrementalLiveAggregateBackend: stage4IncrementalLiveAggregateBackend{
			stage4IncrementalTestState: store,
		},
		mutate: func() error {
			return fixture.insertSource(
				ctx,
				3,
				"after-fence",
				nil,
				postFence,
			)
		},
	}
	windowRun := "incremental-matrix-window-" + fixture.sourceEngine + "-" + fixture.targetEngine
	initializeStage4LifecycleRun(
		t,
		mutationBackend,
		windowRun,
		time.Now().Add(-time.Minute),
	)
	windowEvents := make([]string, 0)
	windowContext := stage4LifecycleRunContext(
		t,
		mutationBackend,
		windowRun,
		false,
	)
	windowObserver := stage4IncrementalTestObserver{
		events:  &windowEvents,
		backend: mutationBackend,
		run:     windowContext,
	}
	result, err = Execute(ctx, config, windowObserver)
	if err != nil || result != (Result{Tables: 1, Rows: 1, Validated: true}) {
		t.Fatalf("incremental after-fence window result=%#v err=%v", result, err)
	}
	if mutationBackend.mutationErr != nil {
		t.Fatalf("insert post-fence source row: %v", mutationBackend.mutationErr)
	}
	if !mutationBackend.mutated {
		t.Fatal(
			"post-fence source row was never written, so the exclusion assertions below prove nothing",
		)
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: fixture.sourceTable.Schema,
		Table:  fixture.sourceTable.Name,
	}
	attempt, found, err := store.LoadLatestCommittedIncrementalAttempt(
		windowRun,
		task,
	)
	if err != nil || !found || attempt.UpperFence == nil ||
		!attempt.UpperFence.Value.Equal(upperFence) {
		t.Fatalf("after-fence committed attempt=%#v found=%t err=%v", attempt, found, err)
	}
	if count, err := fixture.targetRowCount(ctx); err != nil || count != 2 {
		t.Fatalf("after-fence target row count=%d err=%v", count, err)
	}
	if payload, found, err := fixture.targetPayload(ctx, 1); err != nil || !found || payload != "window-value" {
		t.Fatalf("after-fence target row one payload=%q found=%t err=%v", payload, found, err)
	}
	if _, found, err := fixture.targetPayload(ctx, 3); err != nil || found {
		t.Fatalf("post-fence source row reached target found=%t err=%v", found, err)
	}

	resumeEvents := make([]string, 0)
	windowContext.Resume = true
	resumeObserver := stage4IncrementalTestObserver{
		events:  &resumeEvents,
		backend: mutationBackend,
		resume:  true,
		run:     windowContext,
	}
	result, err = ExecuteResume(
		ctx,
		config,
		CompletedTableCheckpoints{
			fixture.sourceTable.Name: {Rows: 1},
		},
		resumeObserver,
	)
	if err != nil || result != (Result{Tables: 1, Rows: 1, Validated: true}) {
		t.Fatalf("completed post-fence window resume result=%#v err=%v", result, err)
	}
	if _, found, err := fixture.targetPayload(ctx, 3); err != nil || found {
		t.Fatalf("completed-window resume admitted post-fence source row found=%t err=%v", found, err)
	}
}

// publishStage4IncrementalLiveRun exercises the production terminal path,
// including durable schema-sentinel completion and aggregate run publication.
// Baseline source authority must be a real completed Stage 4 run; manually
// appending a success record would leave its sentinel evidence unrepresentative.
func publishStage4IncrementalLiveRun(t *testing.T, run Stage4RunContext) {
	t.Helper()
	completedAt := time.Now().UTC()
	published, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"incremental live baseline completed",
		completedAt,
	)
	if err != nil || !published {
		t.Fatalf(
			"publish incremental baseline run published=%t err=%v",
			published,
			err,
		)
	}
}

type stage4IncrementalLivePostFenceMutationBackend struct {
	stage4IncrementalLiveAggregateBackend
	once        sync.Once
	mutate      func() error
	mutationErr error
	// mutated records that the post-fence write actually happened. Without it
	// the matrix cannot distinguish "the post-fence row was correctly excluded"
	// from "the post-fence row was never written", because both leave the
	// target without row 3 and mutationErr nil. The mutation only runs when
	// BeginIncrementalAttempt reports a newly created attempt, so a route change
	// that stopped creating one would have turned every cell green while proving
	// nothing.
	mutated bool
}

func (backend *stage4IncrementalLivePostFenceMutationBackend) BeginIncrementalAttempt(
	attempt state.IncrementalAttempt,
) (state.IncrementalAttempt, bool, error) {
	stored, created, err := backend.stage4IncrementalTestState.BeginIncrementalAttempt(attempt)
	if err != nil || !created || backend.mutate == nil {
		return stored, created, err
	}
	backend.once.Do(func() {
		backend.mutationErr = backend.mutate()
		backend.mutated = backend.mutationErr == nil
	})
	if backend.mutationErr != nil {
		return state.IncrementalAttempt{}, false, backend.mutationErr
	}
	return stored, created, nil
}

// stage4IncrementalLiveAggregateBackend deliberately forwards the aggregate
// state surface through test wrappers. Embedding stage4IncrementalTestState
// alone only exposes its declared interface methods, so the production
// composed route cannot see aggregate completion on its concrete YAML/SQLite
// backend.
type stage4IncrementalLiveAggregateBackend struct {
	stage4IncrementalTestState
}

func (backend stage4IncrementalLiveAggregateBackend) aggregate() (state.Stage4AggregateBackend, error) {
	aggregate, ok := backend.stage4IncrementalTestState.(state.Stage4AggregateBackend)
	if !ok {
		return nil, fmt.Errorf("test incremental backend %T lacks aggregate state", backend.stage4IncrementalTestState)
	}
	return aggregate, nil
}

func (backend stage4IncrementalLiveAggregateBackend) EnsureStage4TableInventory(inventory state.Stage4TableInventory) error {
	aggregate, err := backend.aggregate()
	if err != nil {
		return err
	}
	return aggregate.EnsureStage4TableInventory(inventory)
}

func (backend stage4IncrementalLiveAggregateBackend) CompleteStage4Table(completion state.Stage4TableCompletion) error {
	aggregate, err := backend.aggregate()
	if err != nil {
		return err
	}
	return aggregate.CompleteStage4Table(completion)
}

func (backend stage4IncrementalLiveAggregateBackend) CompleteStage4Run(completion state.Stage4RunCompletion) error {
	aggregate, err := backend.aggregate()
	if err != nil {
		return err
	}
	return aggregate.CompleteStage4Run(completion)
}

func (backend stage4IncrementalLiveAggregateBackend) LoadStage4TableInventory(runID string) (state.Stage4TableInventoryReceipt, bool, error) {
	aggregate, err := backend.aggregate()
	if err != nil {
		return state.Stage4TableInventoryReceipt{}, false, err
	}
	return aggregate.LoadStage4TableInventory(runID)
}

func (backend stage4IncrementalLiveAggregateBackend) LoadStage4TableCompletions(runID string) ([]state.Stage4TableCompletionReceipt, error) {
	aggregate, err := backend.aggregate()
	if err != nil {
		return nil, err
	}
	return aggregate.LoadStage4TableCompletions(runID)
}

func (fixture *stage4IncrementalLiveRouteFixture) targetRowCount(
	ctx context.Context,
) (int, error) {
	var count int
	err := fixture.targetDatabase.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+fixture.targetQualified(),
	).Scan(&count)
	return count, err
}

func (fixture *stage4IncrementalLiveRouteFixture) targetPayload(
	ctx context.Context,
	id int64,
) (string, bool, error) {
	query := ""
	arguments := []any{id}
	switch fixture.targetEngine {
	case "postgres":
		query = "SELECT payload FROM " + fixture.targetQualified() + " WHERE id = $1"
	case "mysql", "sqlite":
		query = "SELECT payload FROM " + fixture.targetQualified() + " WHERE id = ?"
	case "mssql":
		query = "SELECT [payload] FROM " + fixture.targetQualified() + " WHERE [id] = @p1"
	default:
		return "", false, fmt.Errorf("unknown target engine %q", fixture.targetEngine)
	}
	var payload string
	err := fixture.targetDatabase.QueryRowContext(ctx, query, arguments...).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return payload, err == nil, err
}

const (
	stage4IncrementalProcessCrashChildEnv   = "DMTX_STAGE4_INCREMENTAL_CRASH_CHILD"
	stage4IncrementalProcessCrashStateEnv   = "DMTX_STAGE4_INCREMENTAL_CRASH_STATE"
	stage4IncrementalProcessCrashBackendEnv = "DMTX_STAGE4_INCREMENTAL_CRASH_BACKEND"
	stage4IncrementalProcessCrashSourceEnv  = "DMTX_STAGE4_INCREMENTAL_CRASH_SOURCE"
	stage4IncrementalProcessCrashTargetEnv  = "DMTX_STAGE4_INCREMENTAL_CRASH_TARGET"
	stage4IncrementalProcessCrashSpoolEnv   = "DMTX_STAGE4_INCREMENTAL_CRASH_SPOOL"
	stage4IncrementalProcessCrashRunEnv     = "DMTX_STAGE4_INCREMENTAL_CRASH_RUN"
	stage4IncrementalProcessCrashTableEnv   = "DMTX_STAGE4_INCREMENTAL_CRASH_TABLE"
	stage4IncrementalProcessCrashExitCode   = 86
)

// TestStage4IncrementalSQLiteProcessCrashResume invokes the same composed
// SQLite-to-SQLite incremental window in a child test process. The child exits
// immediately after it has durably acknowledged a target batch, so neither
// defers nor in-process fault wrappers can make recovery accidentally pass.
// YAML replacement and SQLite transactional state each receive this proof.
func TestStage4IncrementalSQLiteProcessCrashResume(t *testing.T) {
	if os.Getenv(stage4IncrementalProcessCrashChildEnv) == "1" {
		stage4IncrementalProcessCrashChild()
		return
	}
	for name, newBackend := range map[string]func(string) stage4IncrementalTestState{
		"yaml": func(path string) stage4IncrementalTestState {
			return state.YAMLStore{Path: path}
		},
		"sqlite": func(path string) stage4IncrementalTestState {
			return state.SQLiteStore{Path: path}
		},
	} {
		name, newBackend := name, newBackend
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			sourcePath := filepath.Join(t.TempDir(), "source.sqlite")
			targetPath := filepath.Join(t.TempDir(), "target.sqlite")
			statePath := filepath.Join(t.TempDir(), "state."+name)
			runID := "incremental-process-crash-" + name
			tableName := "items"
			spoolBase, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(spoolBase, 0o700); err != nil {
				t.Fatal(err)
			}
			spoolParent := filepath.Join(spoolBase, "spool")
			if err := os.Mkdir(spoolParent, 0o700); err != nil {
				t.Fatal(err)
			}
			spool := filepath.Join(spoolParent, stage4LifecycleRunDigest(runID))
			if err := os.Mkdir(spool, 0o700); err != nil {
				t.Fatal(err)
			}
			fixture := newStage4IncrementalSQLiteProcessCrashFixture(
				t,
				ctx,
				sourcePath,
				targetPath,
				tableName,
			)
			backend := newBackend(statePath)
			config := fixture.incrementalConfig()
			baselineRun := runID + "-baseline"
			initializeStage4LifecycleRun(
				t,
				backend,
				baselineRun,
				time.Now().Add(-time.Minute),
			)
			baselineEvents := make([]string, 0)
			baselineObserver := stage4IncrementalTestObserver{
				events:  &baselineEvents,
				backend: backend,
				run: Stage4RunContext{
					RunID:          baselineRun,
					Backend:        backend,
					SpoolDirectory: filepath.Join(spoolParent, stage4LifecycleRunDigest(baselineRun)),
				},
			}
			if err := os.Mkdir(baselineObserver.run.SpoolDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			result, err := Execute(ctx, config, baselineObserver)
			if err != nil || result != (Result{Tables: 1, Rows: 2, Validated: true}) {
				t.Fatalf("process-crash baseline result=%#v err=%v", result, err)
			}
			publishStage4IncrementalLiveRun(t, baselineObserver.run)
			upperFence := time.Date(2026, 7, 30, 10, 2, 0, 0, time.UTC)
			if err := fixture.updateSource(ctx, 1, "window-value", upperFence); err != nil {
				t.Fatal(err)
			}
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			command := exec.Command(
				os.Args[0],
				"-test.run=^TestStage4IncrementalSQLiteProcessCrashResume$",
			)
			command.Env = append(os.Environ(),
				stage4IncrementalProcessCrashChildEnv+"=1",
				stage4IncrementalProcessCrashStateEnv+"="+statePath,
				stage4IncrementalProcessCrashBackendEnv+"="+name,
				stage4IncrementalProcessCrashSourceEnv+"="+sourcePath,
				stage4IncrementalProcessCrashTargetEnv+"="+targetPath,
				stage4IncrementalProcessCrashSpoolEnv+"="+spool,
				stage4IncrementalProcessCrashRunEnv+"="+runID,
				stage4IncrementalProcessCrashTableEnv+"="+tableName,
			)
			output, err := command.CombinedOutput()
			exitErr, exited := err.(*exec.ExitError)
			if !exited || exitErr.ExitCode() != stage4IncrementalProcessCrashExitCode {
				t.Fatalf(
					"incremental crash child error=%T exit=%v output=%s",
					err,
					exitErr,
					strings.TrimSpace(string(output)),
				)
			}
			task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: tableName}
			active, found, err := backend.LoadActiveIncrementalAttempt(runID, task)
			if err != nil || !found || active.Status != state.IncrementalRunning ||
				active.UpperFence == nil || !active.UpperFence.Value.Equal(upperFence) {
				t.Fatalf("crashed incremental attempt=%#v found=%t err=%v", active, found, err)
			}
			if err := fixture.insertSource(
				ctx,
				3,
				"after-fence",
				nil,
				upperFence.Add(time.Second),
			); err != nil {
				t.Fatal(err)
			}
			resumeEvents := make([]string, 0)
			resumeObserver := stage4IncrementalTestObserver{
				events:  &resumeEvents,
				backend: backend,
				resume:  true,
				run: Stage4RunContext{
					RunID:          runID,
					Backend:        backend,
					Resume:         true,
					SpoolDirectory: spool,
				},
			}
			result, err = ExecuteResume(
				ctx,
				config,
				CompletedTableCheckpoints{},
				resumeObserver,
			)
			if err != nil || result != (Result{Tables: 1, Rows: 1, Validated: true}) {
				t.Fatalf("process-crash resume result=%#v err=%v", result, err)
			}
			if _, found, err := fixture.targetPayload(ctx, 3); err != nil || found {
				t.Fatalf("process-crash resume admitted post-fence source row found=%t err=%v", found, err)
			}
			entries, err := os.ReadDir(spool)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				names := make([]string, len(entries))
				for index, entry := range entries {
					names[index] = entry.Name()
				}
				t.Fatalf("process-crash resume left incremental evidence spool artifacts %q", names)
			}
		})
	}
}

func newStage4IncrementalSQLiteProcessCrashFixture(
	t *testing.T,
	ctx context.Context,
	sourcePath string,
	targetPath string,
	tableName string,
) *stage4IncrementalLiveRouteFixture {
	t.Helper()
	fixture := &stage4IncrementalLiveRouteFixture{
		sourceEngine: "sqlite",
		targetEngine: "sqlite",
		sourceEndpoint: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		targetEndpoint: config.Endpoint{
			Type:     "sqlite",
			Database: targetPath,
		},
		sourceTable: schema.Table{Name: tableName},
	}
	fixture.sourceDatabase = stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"sqlite",
		sourcePath,
		"SQLite incremental process-crash source",
	)
	fixture.targetDatabase = stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"sqlite",
		targetPath,
		"SQLite incremental process-crash target",
	)
	if err := fixture.createSourceTable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.openSQLiteSourceMutationDatabase(t, ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.precreateTarget(ctx); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func stage4IncrementalProcessCrashChild() {
	statePath := os.Getenv(stage4IncrementalProcessCrashStateEnv)
	sourcePath := os.Getenv(stage4IncrementalProcessCrashSourceEnv)
	targetPath := os.Getenv(stage4IncrementalProcessCrashTargetEnv)
	spool := os.Getenv(stage4IncrementalProcessCrashSpoolEnv)
	runID := os.Getenv(stage4IncrementalProcessCrashRunEnv)
	tableName := os.Getenv(stage4IncrementalProcessCrashTableEnv)
	if statePath == "" || sourcePath == "" || targetPath == "" || spool == "" ||
		runID == "" || tableName == "" {
		os.Exit(87)
	}
	var backend stage4IncrementalTestState
	switch os.Getenv(stage4IncrementalProcessCrashBackendEnv) {
	case "yaml":
		backend = state.YAMLStore{Path: statePath}
	case "sqlite":
		backend = state.SQLiteStore{Path: statePath}
	default:
		os.Exit(88)
	}
	crashing := &stage4IncrementalLiveProcessCrashBackend{
		stage4IncrementalLiveAggregateBackend: stage4IncrementalLiveAggregateBackend{
			stage4IncrementalTestState: backend,
		},
	}
	events := make([]string, 0)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: crashing,
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        crashing,
			SpoolDirectory: spool,
		},
	}
	config := (&stage4IncrementalLiveRouteFixture{
		sourceEngine: "sqlite",
		targetEngine: "sqlite",
		sourceEndpoint: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		targetEndpoint: config.Endpoint{
			Type:     "sqlite",
			Database: targetPath,
		},
		sourceTable: schema.Table{Name: tableName},
	}).incrementalConfig()
	if _, err := Execute(context.Background(), config, observer); err != nil {
		fmt.Fprintf(os.Stderr, "incremental crash child execute error: %v\n", err)
	}
	os.Exit(89)
}

type stage4IncrementalLiveProcessCrashBackend struct {
	stage4IncrementalLiveAggregateBackend
}

func (backend *stage4IncrementalLiveProcessCrashBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	rangeState, err := backend.stage4IncrementalTestState.AcknowledgeRange(
		acknowledgement,
	)
	if err == nil && acknowledgement.Task.Type == stage4AdapterNetworkTaskType {
		os.Exit(stage4IncrementalProcessCrashExitCode)
	}
	return rangeState, err
}
