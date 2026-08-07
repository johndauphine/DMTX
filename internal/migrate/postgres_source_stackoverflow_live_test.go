package migrate

import (
	"context"
	"database/sql"
	"net/url"
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
)

// The PostgreSQL source, against a schema dmtx did not design.
//
// postgres->mssql and postgres->mysql are registered in builtInAdapters, so
// dmtx claims both, and until this file neither had a live test of any kind.
// A certified route with no evidence behind it is the same shape of problem as
// a fixture written to match the implementation: the claim and the check come
// from the same place.
//
// The corpus is test/fixtures/postgres/so2010-minimal.sql, dmt's per-engine
// translation of the public StackOverflow schema, copied verbatim.

func TestPostgresToSQLServerStackOverflowFixtureLive(t *testing.T) {
	runPostgresStackOverflowRoute(t, "mssql")
}

func TestPostgresToMySQLStackOverflowFixtureLive(t *testing.T) {
	runPostgresStackOverflowRoute(t, "mysql")
}

// runPostgresStackOverflowRoute migrates the corpus from PostgreSQL to one
// target and checks the shape that arrives.
func runPostgresStackOverflowRoute(t *testing.T, targetEngine string) {
	t.Helper()
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if postgresDSN == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL StackOverflow route")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	namespace := "so2010_src_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceEndpoint := loadPostgresStackOverflowFixture(t, ctx, postgresDSN, namespace)

	var targetEndpoint config.Endpoint
	switch targetEngine {
	case "mssql":
		targetEndpoint = stackOverflowSQLServerTarget(t, ctx)
	case "mysql":
		targetEndpoint = stackOverflowMySQLTarget(t)
	default:
		t.Fatalf("no target builder for %q", targetEngine)
	}
	if targetEndpoint.Type == "" {
		t.Skipf("no %s endpoint configured", targetEngine)
	}

	result, err := Execute(ctx, config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
			ConnectionLimit:         4,
			ReaderParallelism:       1,
			WriterParallelism:       2,
			MemoryCeilingBytes:      256 << 20,
		},
	}, nil)
	if err != nil {
		t.Fatalf("migrate the StackOverflow corpus to %s: %v", targetEngine, err)
	}
	if result.Tables != 9 {
		t.Errorf("migrated %d tables, want 9", result.Tables)
	}
	if result.Rows == 0 {
		t.Error("migrated no rows")
	}
}

// loadPostgresStackOverflowFixture puts the corpus in its own schema and
// returns an endpoint pointing at it.
//
// A fresh schema per run rather than a fixed name: these tests can run beside
// each other, and a shared namespace would let one route's drop_recreate delete
// another's source mid-read.
func loadPostgresStackOverflowFixture(
	t *testing.T,
	ctx context.Context,
	dsn string,
	namespace string,
) config.Endpoint {
	t.Helper()
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	script, err := os.ReadFile(filepath.Join(
		"..", "..", "test", "fixtures", "postgres", "so2010-minimal.sql",
	))
	if err != nil {
		t.Fatalf("read the PostgreSQL corpus: %v", err)
	}

	quoted := postgresIdentifier(namespace)
	if _, err := database.ExecContext(
		ctx,
		"DROP SCHEMA IF EXISTS "+quoted+" CASCADE",
	); err != nil {
		t.Fatalf("reset the corpus schema: %v", err)
	}
	if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create the corpus schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+quoted+" CASCADE",
		); err != nil {
			t.Errorf("drop the corpus schema: %v", err)
		}
	})

	// One pinned connection, so search_path holds for the whole script. The
	// pool would otherwise hand the CREATE TABLEs to a connection still
	// pointing at public, and the migration would find nothing - the same way
	// the SQL Server loader failed before it was pinned.
	pinned, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("pin a PostgreSQL connection: %v", err)
	}
	defer func() { _ = pinned.Close() }()
	if _, err := pinned.ExecContext(ctx, "SET search_path TO "+quoted); err != nil {
		t.Fatalf("set the corpus search path: %v", err)
	}
	if _, err := pinned.ExecContext(
		ctx,
		stripPostgresClientDirectives(string(script)),
	); err != nil {
		t.Fatalf("load the PostgreSQL corpus: %v", err)
	}

	// Proved loaded before the migration is asked to find it, so a broken
	// loader fails here by name rather than downstream as "no source tables
	// match migration filters".
	var tables int
	if err := pinned.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1",
		namespace,
	).Scan(&tables); err != nil {
		t.Fatalf("count corpus tables: %v", err)
	}
	if tables != 9 {
		t.Fatalf("corpus loaded %d tables, want 9", tables)
	}

	return config.Endpoint{
		Type:      "postgres",
		Host:      parsed.Host,
		Port:      int(parsed.Port),
		Database:  parsed.Database,
		User:      parsed.User,
		Password:  parsed.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: postgresFixtureCAFile(dsn),
	}
}

// postgresFixtureCAFile reads the CA out of the PostgreSQL DSN.
//
// There is no DMTX_TEST_POSTGRES_CA - env.sh puts PostgreSQL's CA in the DSN's
// sslrootcert parameter, where libpq expects it, and exports a separate
// variable only for the engines whose drivers take one argument-side.
//
// This used to read DMTX_TEST_MSSQL_CA, which worked and was wrong. Every
// fixture in test/fixtures shares one generated CA, so pointing a PostgreSQL
// endpoint at SQL Server's copy verified against the right certificate by
// accident. It would have kept working until someone gave one engine its own
// CA, and then failed as a TLS error naming neither this line nor that change.
func postgresFixtureCAFile(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	return parsed.Query().Get("sslrootcert")
}

// stackOverflowSQLServerTarget makes an empty database for one route to fill.
func stackOverflowSQLServerTarget(t *testing.T, ctx context.Context) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caFile := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caFile == "" {
		return config.Endpoint{}
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, dsn, caFile)
	admin := endpoint
	admin.Database = "master"
	connection, err := engine.OpenSQLServer(ctx, admin)
	if err != nil {
		t.Fatalf("open SQL Server admin connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	name := "dmtx_so2010_tgt_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := connection.ExecContext(
		ctx,
		"CREATE DATABASE "+sqlServerIdentifier(name),
	); err != nil {
		t.Fatalf("create the SQL Server target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := connection.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+sqlServerIdentifier(name)+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+
				sqlServerIdentifier(name),
		); err != nil {
			t.Errorf("drop the SQL Server target database: %v", err)
		}
	})
	endpoint.Database = name
	return endpoint
}

// stackOverflowMySQLTarget points at a database this matrix owns alone.
//
// It used to point at the shared dmtx_target, on the reasoning that recreating
// its own tables would not disturb anything. That was wrong, and enabling
// mssql->mysql proved it: the corpus writes Badges and Users, MySQL lower-cases
// them, and every other test that validates dmtx_target then fails on a
// case-aliased table. A test that breaks its neighbours is worse than one that
// skips, so it has its own database, which provision.sh creates.
func stackOverflowMySQLTarget(t *testing.T) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	caFile := os.Getenv("DMTX_TEST_MYSQL_CA")
	if dsn == "" || caFile == "" {
		return config.Endpoint{}
	}
	// The DSN names a TLS config, and ParseDSN refuses a name the driver has
	// not been given yet - so the CA is registered under that name first.
	registerMySQLCommonFixtureTLSNamed(t, caFile, mySQLTLSConfigName(dsn))
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL target DSN: %v", err)
	}
	endpoint := mysqlNativeTargetEndpoint(t, parsed, caFile)
	const corpusTarget = "so2010_minimal_tgt"
	endpoint.Database = corpusTarget
	endpoint.Schema = corpusTarget
	return endpoint
}

// stripPostgresClientDirectives removes psql's backslash commands.
//
// The corpus ends with an \echo announcing that it loaded. That is a directive
// to the psql client, not SQL, and a driver handed it gets a syntax error - the
// same shape as SQL Server's GO, which the SQL Server loader splits on for the
// same reason.
//
// Handled here rather than by editing the file. The fixture is dmt's, kept
// byte-identical so both tools are proved against the same corpus, and a
// difference between two clients is the loader's problem rather than grounds
// to start diverging the thing being compared.
func stripPostgresClientDirectives(script string) string {
	lines := strings.Split(script, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "\\") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}
