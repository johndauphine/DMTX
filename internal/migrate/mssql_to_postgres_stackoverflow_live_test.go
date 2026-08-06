package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

// The StackOverflow route, proved against a schema dmtx did not design.
//
// Every other SQL Server live test in this package builds its own fixture, and
// that is how a fourteen-minute armed gate came to pass over a route that could
// not read an ordinary table: the fixtures wrote
// COLLATE Latin1_General_100_BIN2_UTF8 onto every text column and used
// datetime2 throughout, because those were the only spellings discovery
// accepted. A fixture shaped to fit the implementation cannot find out that the
// implementation is wrong.
//
// This one loads test/fixtures/sqlserver/so2010-minimal.sql, which is dmt's
// per-PR integration corpus copied verbatim. Its columns are nvarchar and
// datetime under the collation SQL Server installs by default, because that is
// what the public StackOverflow schema contains.
//
// The rule this test exists to keep: a fixture here must never carry a spelling
// chosen to make dmtx pass. If this test needs a COLLATE clause added to load,
// that is the finding, not the fix.

// stackOverflowFixtureDatabase is created by the fixture script itself.
const stackOverflowFixtureDatabase = "StackOverflow2010Minimal"

func TestSQLServerToPostgresStackOverflowFixtureLive(t *testing.T) {
	sqlServerDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	sqlServerCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if sqlServerDSN == "" || sqlServerCA == "" || postgresDSN == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, and DMTX_TEST_POSTGRES_DSN to run the StackOverflow fixture route",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	loadStackOverflowFixture(t, ctx, sqlServerDSN)

	sourceEndpoint := sqlServerCommonFixtureEndpoint(t, sqlServerDSN, sqlServerCA)
	sourceEndpoint.Database = stackOverflowFixtureDatabase

	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	namespace := "so2010_fixture"
	targetEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      postgresConfig.Host,
		Port:      int(postgresConfig.Port),
		Database:  postgresConfig.Database,
		User:      postgresConfig.User,
		Password:  postgresConfig.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: os.Getenv("DMTX_TEST_POSTGRES_CA"),
	}
	if targetEndpoint.TLSCAFile == "" {
		targetEndpoint.TLSCAFile = os.Getenv("DMTX_TEST_MSSQL_CA")
	}

	target := openStackOverflowPostgres(t, ctx, postgresDSN)
	if _, err := target.ExecContext(
		ctx,
		"DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE",
	); err != nil {
		t.Fatalf("reset target schema: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()
		if _, err := target.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop target schema: %v", err)
		}
	})

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
		t.Fatalf("migrate the StackOverflow fixture: %v", err)
	}

	// Nine tables is the public schema's shape, and asserting it stops a
	// migration that quietly skipped one from reading as a success.
	if result.Tables != 9 {
		t.Errorf("migrated %d tables, want 9", result.Tables)
	}
	if result.Rows == 0 {
		t.Error("migrated no rows")
	}

	assertStackOverflowTargetShape(t, ctx, target, namespace)
}

// assertStackOverflowTargetShape checks the two projections that are wrong in
// ways nothing else would notice.
//
// A row count cannot see either of them: a doubled length still loads every
// value, and a timestamp declared without its fractional digits still accepts
// every row the source had.
func assertStackOverflowTargetShape(
	t *testing.T,
	ctx context.Context,
	target *sql.DB,
	namespace string,
) {
	t.Helper()
	for _, expected := range []struct {
		column    string
		dataType  string
		maxLength int
		precision int
	}{
		// nvarchar(40) in SQL Server, where sys.columns.max_length reports 80
		// because the national types store two bytes per character. Declared as
		// varchar(80) here, every value would still fit and nothing would fail.
		{column: "DisplayName", dataType: "character varying", maxLength: 40},
		{column: "Location", dataType: "character varying", maxLength: 100},
		{column: "WebsiteUrl", dataType: "character varying", maxLength: 200},
		// nvarchar(max).
		{column: "AboutMe", dataType: "text"},
		// datetime, whose stored resolution is finer than a second, so a
		// timestamp(0) would truncate every value it carried.
		{column: "CreationDate", dataType: "timestamp without time zone", precision: 3},
		{column: "LastAccessDate", dataType: "timestamp without time zone", precision: 3},
	} {
		t.Run(expected.column, func(t *testing.T) {
			var dataType string
			var maxLength, precision sql.NullInt64
			if err := target.QueryRowContext(
				ctx,
				`SELECT data_type, character_maximum_length, datetime_precision
				 FROM information_schema.columns
				 WHERE table_schema = $1 AND table_name = 'Users' AND column_name = $2`,
				namespace,
				expected.column,
			).Scan(&dataType, &maxLength, &precision); err != nil {
				t.Fatalf("read target column: %v", err)
			}
			if dataType != expected.dataType {
				t.Errorf("data type = %q, want %q", dataType, expected.dataType)
			}
			if expected.maxLength != 0 &&
				(!maxLength.Valid || int(maxLength.Int64) != expected.maxLength) {
				t.Errorf(
					"length = %v characters, want %d - a byte count would give %d",
					maxLength,
					expected.maxLength,
					expected.maxLength*2,
				)
			}
			if expected.precision != 0 &&
				(!precision.Valid || int(precision.Int64) != expected.precision) {
				t.Errorf("precision = %v, want %d", precision, expected.precision)
			}
		})
	}
}

// loadStackOverflowFixture runs the fixture script against SQL Server.
//
// Split on GO because GO is a batch separator understood by sqlcmd and not by
// the server, so a driver handed the whole file gets a syntax error.
//
// Every batch runs on one pinned connection, and that is not a detail.
// database/sql hands out a pooled connection per call, while the script's
// USE StackOverflow2010Minimal changes the database of the connection it runs
// on and no other. Spread across the pool, the CREATE TABLE batches land in
// master, the migration finds nothing, and the failure - "no source tables
// match migration filters" - says nothing about why.
//
// The tables are dropped first rather than after. A fixture left behind by an
// earlier run would otherwise let this test pass while its own loading was
// broken, which is how the pooled-connection bug above survived a local run:
// the tables were already there from a manual load.
func loadStackOverflowFixture(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	path := filepath.Join(
		"..", "..", "test", "fixtures", "sqlserver", "so2010-minimal.sql",
	)
	script, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the StackOverflow fixture: %v", err)
	}
	database, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open SQL Server: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	pinned, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("pin a SQL Server connection: %v", err)
	}
	defer func() { _ = pinned.Close() }()

	if _, err := pinned.ExecContext(
		ctx,
		"IF DB_ID('"+stackOverflowFixtureDatabase+"') IS NOT NULL "+
			"ALTER DATABASE ["+stackOverflowFixtureDatabase+"] "+
			"SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
	); err != nil {
		t.Fatalf("quiesce any earlier fixture database: %v", err)
	}
	if _, err := pinned.ExecContext(
		ctx,
		"IF DB_ID('"+stackOverflowFixtureDatabase+"') IS NOT NULL "+
			"DROP DATABASE ["+stackOverflowFixtureDatabase+"]",
	); err != nil {
		t.Fatalf("drop any earlier fixture database: %v", err)
	}

	for index, batch := range splitSQLServerBatches(string(script)) {
		if _, err := pinned.ExecContext(ctx, batch); err != nil {
			t.Fatalf("load fixture batch %d: %v", index+1, err)
		}
	}

	// The loader proved it loaded. Without this the test can only report that
	// the migration found nothing, which is a symptom several steps downstream.
	var tables int
	if err := pinned.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM ["+stackOverflowFixtureDatabase+"].sys.tables",
	).Scan(&tables); err != nil {
		t.Fatalf("count fixture tables: %v", err)
	}
	if tables != 9 {
		t.Fatalf("fixture loaded %d tables, want 9", tables)
	}
}

// splitSQLServerBatches breaks a script on its GO lines, dropping empties.
func splitSQLServerBatches(script string) []string {
	var batches []string
	var current []string
	flush := func() {
		joined := strings.TrimSpace(strings.Join(current, "\n"))
		if joined != "" {
			batches = append(batches, joined)
		}
		current = current[:0]
	}
	for _, line := range strings.Split(script, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "GO") {
			flush()
			continue
		}
		current = append(current, line)
	}
	flush()
	return batches
}

func openStackOverflowPostgres(
	t *testing.T,
	ctx context.Context,
	dsn string,
) *sql.DB {
	t.Helper()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	return database
}
