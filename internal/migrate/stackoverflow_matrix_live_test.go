package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

// The directed-pair matrix, every pair carrying the same StackOverflow corpus.
//
// This is dmt's nightly shape: one real migration per certified route, each
// source seeded from its own translation of the public schema rather than from
// a fixture written here. dmt's integration-test-pair.sh does the same with
// four per-engine loaders and twelve pairs; the fixtures under test/fixtures
// are its, copied verbatim.
//
// Seeded independently, never chained. Migrating one engine's copy to make the
// next source would need a single seed and avoid drift, but that corpus would
// be written BY dmtx - so a wrong mapping is baked into the next hop's input
// and validated against itself. That is the shape of the defect that hid an
// unreadable nvarchar column behind fourteen minutes of green CI.
//
// The rule these fixtures exist under: none of them may carry a spelling chosen
// to make dmtx pass. If a pair only goes green after a COLLATE is added or a
// type is respelled, that is the finding, not the fix.

// stackOverflowEngine is one engine's part in the matrix: how to seed it as a
// source, and how to hand out an empty place to write as a target.
//
// Either may return a zero Endpoint, which skips the pair rather than failing
// it - an engine with no endpoint configured is absent, not broken.
type stackOverflowEngine struct {
	name   string
	source func(t *testing.T, ctx context.Context) config.Endpoint
	target func(t *testing.T, ctx context.Context) config.Endpoint
}

func stackOverflowEngines() map[string]stackOverflowEngine {
	return map[string]stackOverflowEngine{
		"mssql": {
			name:   "mssql",
			source: stackOverflowSQLServerSource,
			target: stackOverflowSQLServerTarget,
		},
		"postgres": {
			name:   "postgres",
			source: stackOverflowPostgresSource,
			target: stackOverflowPostgresTarget,
		},
		"mysql": {
			name:   "mysql",
			source: stackOverflowMySQLSource,
			target: func(t *testing.T, _ context.Context) config.Endpoint {
				return stackOverflowMySQLTarget(t)
			},
		},
	}
}

// TestStackOverflowDirectedPairMatrixLive migrates the corpus across every
// certified route the matrix covers.
//
// Table-driven rather than a file per pair, because the pairs differ only in
// which two engines they name. A bug that reaches one of them reaches all of
// them, and twelve near-copies would be twelve places to fix it.
func TestStackOverflowDirectedPairMatrixLive(t *testing.T) {
	engines := stackOverflowEngines()
	// blocked names a route the corpus cannot cross yet. It skips rather than
	// deletes, because the pair is certified in builtInAdapters - dmtx claims
	// it - and a matrix that quietly omitted the routes that fail would be the
	// same false comfort as a fixture shaped to fit the implementation.
	//
	// Both entries below were found by this matrix on its first run, minutes
	// after the equivalent SQL Server defect was fixed. They are tracked, and
	// each should be unskipped by the change that closes it.
	for _, pair := range []struct{ source, target, blocked string }{
		{source: "mssql", target: "postgres"},
		{source: "mssql", target: "mysql"},
		{source: "postgres", target: "mssql"},
		{source: "postgres", target: "mysql"},
		{source: "mysql", target: "postgres"},
		{
			source: "mysql", target: "mssql",
			// Not a defect - dmtx is right here. MySQL LONGTEXT holds 4GB and
			// SQL Server VARCHAR(MAX) holds 2GB, and nothing in the schema says
			// the data is small, so refusing is what refuse-unless-certified
			// means.
			//
			// It does expose an asymmetry worth naming: mssql->mysql maps
			// NVARCHAR(MAX) to LONGTEXT, and LONGTEXT cannot come back. The
			// corpus survives one direction and not the return, which is
			// exactly what a round-trip idempotence check would be for.
			blocked: "MySQL LONGTEXT exceeds SQL Server VARCHAR(MAX); a real " +
				"capacity limit rather than a misplaced rule",
		},
	} {
		t.Run(pair.source+"_to_"+pair.target, func(t *testing.T) {
			if pair.blocked != "" {
				t.Skip(pair.blocked)
			}
			sourceEngine, ok := engines[pair.source]
			if !ok {
				t.Fatalf("no source builder for %q", pair.source)
			}
			targetEngine, ok := engines[pair.target]
			if !ok {
				t.Fatalf("no target builder for %q", pair.target)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
			defer cancel()

			source := sourceEngine.source(t, ctx)
			if source.Type == "" {
				t.Skipf("no %s endpoint configured", pair.source)
			}
			target := targetEngine.target(t, ctx)
			if target.Type == "" {
				t.Skipf("no %s endpoint configured", pair.target)
			}

			result, err := Execute(ctx, config.Config{
				Source: source,
				Target: target,
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
				t.Fatalf("migrate %s to %s: %v", pair.source, pair.target, err)
			}
			// Nine is the public schema's shape. Asserting it stops a migration
			// that quietly skipped a table from reading as a success.
			if result.Tables != 9 {
				t.Errorf("migrated %d tables, want 9", result.Tables)
			}
			if result.Rows == 0 {
				t.Error("migrated no rows")
			}
		})
	}
}

// stackOverflowSQLServerSource seeds SQL Server and returns a source endpoint.
func stackOverflowSQLServerSource(
	t *testing.T,
	ctx context.Context,
) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caFile := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caFile == "" {
		return config.Endpoint{}
	}
	loadStackOverflowFixture(t, ctx, dsn)
	endpoint := sqlServerCommonFixtureEndpoint(t, dsn, caFile)
	endpoint.Database = stackOverflowFixtureDatabase
	return endpoint
}

// stackOverflowPostgresSource seeds PostgreSQL into a schema of its own.
func stackOverflowPostgresSource(
	t *testing.T,
	ctx context.Context,
) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		return config.Endpoint{}
	}
	namespace := "so2010_src_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	return loadPostgresStackOverflowFixture(t, ctx, dsn, namespace)
}

// stackOverflowPostgresTarget hands out an empty schema to write into.
func stackOverflowPostgresTarget(
	t *testing.T,
	ctx context.Context,
) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		return config.Endpoint{}
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %T", err)
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	namespace := "so2010_tgt_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quoted := postgresIdentifier(namespace)
	if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create the target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+quoted+" CASCADE",
		); err != nil {
			t.Errorf("drop the target schema: %v", err)
		}
	})
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

// stackOverflowMySQLSource seeds MySQL and returns a source endpoint.
//
// The corpus creates its own database and USEs it, so the script runs on one
// pinned connection - USE binds to the connection it runs on, and the pool
// would otherwise hand the CREATE TABLEs to one still pointing elsewhere.
func stackOverflowMySQLSource(
	t *testing.T,
	ctx context.Context,
) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MYSQL_DSN")
	caFile := os.Getenv("DMTX_TEST_MYSQL_CA")
	if dsn == "" || caFile == "" {
		return config.Endpoint{}
	}
	registerMySQLCommonFixtureTLSNamed(t, caFile, mySQLTLSConfigName(dsn))
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL DSN: %v", err)
	}
	// multiStatements, because the corpus is one script rather than one
	// statement and the driver refuses a batch without it.
	batched := *parsed
	batched.MultiStatements = true
	database, err := sql.Open("mysql", batched.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	script, err := os.ReadFile(filepath.Join(
		"..", "..", "test", "fixtures", "mysql", "so2010-minimal.sql",
	))
	if err != nil {
		t.Fatalf("read the MySQL corpus: %v", err)
	}
	pinned, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("pin a MySQL connection: %v", err)
	}
	defer func() { _ = pinned.Close() }()
	if _, err := pinned.ExecContext(ctx, string(script)); err != nil {
		t.Fatalf("load the MySQL corpus: %v", err)
	}

	const corpusDatabase = "so2010_minimal_src"
	var tables int
	if err := pinned.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
		corpusDatabase,
	).Scan(&tables); err != nil {
		t.Fatalf("count corpus tables: %v", err)
	}
	if tables != 9 {
		t.Fatalf("corpus loaded %d tables, want 9", tables)
	}

	endpoint := mysqlNativeTargetEndpoint(t, parsed, caFile)
	endpoint.Database = corpusDatabase
	endpoint.Schema = corpusDatabase
	return endpoint
}

// mySQLTLSConfigName reads the tls= name out of a MySQL DSN.
//
// Taken from the DSN rather than hard-coded so the fixtures stay the single
// place that decides what it is called; env.sh sets both, and a constant here
// would be a second copy free to drift from them.
func mySQLTLSConfigName(dsn string) string {
	const marker = "tls="
	start := strings.Index(dsn, marker)
	if start < 0 {
		return ""
	}
	name := dsn[start+len(marker):]
	if end := strings.IndexAny(name, "&"); end >= 0 {
		name = name[:end]
	}
	return name
}
