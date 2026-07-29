package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

const (
	sqlitePostgresUpsertPreflightLiveTable      = "numeric_guard"
	sqlitePostgresTimestampPreflightLiveTable   = "timestamp_guard"
	sqlitePostgresPersistencePreflightLiveTable = "persistence_guard"
)

func TestSQLiteToPostgresUpsertCatalogMismatchLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the live PostgreSQL upsert-preflight test",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must require TLS for the public route test",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open live PostgreSQL connection: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live PostgreSQL connection: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify live PostgreSQL connection: %T", err)
	}

	namespace := fmt.Sprintf(
		"dmtx_pg_upsert_guard_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create live PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop live PostgreSQL schema: %v", err)
		}
	})

	sourcePath := filepath.Join(t.TempDir(), "numeric-guard.sqlite")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open numeric guard SQLite source: %v", err)
	}
	if _, err := source.ExecContext(ctx, `
		CREATE TABLE numeric_guard (
			id TEXT NOT NULL,
			amount DECIMAL(7,2) NOT NULL,
			PRIMARY KEY (id)
		);
		INSERT INTO numeric_guard (id, amount) VALUES ('same-key', 1.29);

		CREATE TABLE timestamp_guard (
			id TEXT NOT NULL,
			occurred_at DATETIME NOT NULL,
			PRIMARY KEY (id)
		);
		INSERT INTO timestamp_guard (id, occurred_at)
		VALUES ('same-key', '2026-01-02 03:04:05.123456');

		CREATE TABLE persistence_guard (
			id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (id)
		);
		INSERT INTO persistence_guard (id, payload) VALUES ('same-key', 'replacement');
	`); err != nil {
		_ = source.Close()
		t.Fatalf("create numeric guard SQLite source: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close numeric guard SQLite source: %v", err)
	}

	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(
				namespace,
				sqlitePostgresUpsertPreflightLiveTable,
			)+
			` ("id" TEXT NOT NULL, "amount" NUMERIC(7,1) NOT NULL, PRIMARY KEY ("id"))`,
	); err != nil {
		t.Fatalf("create incompatible PostgreSQL target: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(
				namespace,
				sqlitePostgresUpsertPreflightLiveTable,
			)+
			` ("id", "amount") VALUES ('same-key', 5.5)`,
	); err != nil {
		t.Fatalf("seed incompatible PostgreSQL target: %v", err)
	}

	observer := &sqlitePostgresLiveObserver{}
	result, err := SQLiteToPostgresWithObserver(
		ctx,
		config.Config{
			Source: config.Endpoint{
				Type:     "sqlite",
				Database: sourcePath,
			},
			Target: config.Endpoint{
				Type:     "postgres",
				Host:     parsed.Host,
				Port:     int(parsed.Port),
				Database: parsed.Database,
				User:     parsed.User,
				Password: parsed.Password,
				Schema:   namespace,
			},
			Migration: config.Migration{
				TargetMode: "upsert",
				IncludeTables: []string{
					sqlitePostgresUpsertPreflightLiveTable,
				},
			},
		},
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "numeric(7,1)") ||
		!strings.Contains(err.Error(), "numeric(7,2)") {
		t.Fatalf("numeric scale preflight error = %v", err)
	}
	if result != (Result{}) {
		t.Fatalf("failed upsert result = %+v, want zero result", result)
	}
	if len(observer.tableSets) != 0 ||
		len(observer.before) != 0 ||
		len(observer.after) != 0 {
		t.Fatalf(
			"callbacks occurred before catalog rejection: table_sets=%#v before=%#v after=%#v",
			observer.tableSets,
			observer.before,
			observer.after,
		)
	}

	var (
		count  int
		amount string
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*), MIN(amount)::text FROM "+
			postgresQualified(
				namespace,
				sqlitePostgresUpsertPreflightLiveTable,
			),
	).Scan(&count, &amount); err != nil {
		t.Fatalf("read target after rejected upsert: %v", err)
	}
	if count != 1 || amount != "5.5" {
		t.Fatalf(
			"target after rejected upsert = (%d, %q), want untouched sentinel (1, %q)",
			count,
			amount,
			"5.5",
		)
	}
	testSQLitePostgresPrecisionAndPersistenceMismatchLive(
		t,
		ctx,
		database,
		parsed,
		namespace,
		sourcePath,
	)
}

func testSQLitePostgresPrecisionAndPersistenceMismatchLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	parsed *pgx.ConnConfig,
	namespace string,
	sourcePath string,
) {
	t.Helper()
	tests := []struct {
		name       string
		table      string
		createSQL  string
		seedSQL    string
		valueSQL   string
		sentinel   string
		wantTokens []string
	}{
		{
			name:  "timestamp precision",
			table: sqlitePostgresTimestampPreflightLiveTable,
			createSQL: "CREATE TABLE " +
				postgresQualified(
					namespace,
					sqlitePostgresTimestampPreflightLiveTable,
				) +
				` ("id" TEXT NOT NULL, "occurred_at" TIMESTAMP(0) NOT NULL, PRIMARY KEY ("id"))`,
			seedSQL: "INSERT INTO " +
				postgresQualified(
					namespace,
					sqlitePostgresTimestampPreflightLiveTable,
				) +
				` ("id", "occurred_at") VALUES ('same-key', '2000-01-01 00:00:00')`,
			valueSQL: "MIN(occurred_at)::text",
			sentinel: "2000-01-01 00:00:00",
			wantTokens: []string{
				"timestamp(0) without time zone",
				"timestamp(6) without time zone",
			},
		},
		{
			name:  "UNLOGGED persistence",
			table: sqlitePostgresPersistencePreflightLiveTable,
			createSQL: "CREATE UNLOGGED TABLE " +
				postgresQualified(
					namespace,
					sqlitePostgresPersistencePreflightLiveTable,
				) +
				` ("id" TEXT NOT NULL, "payload" TEXT NOT NULL, PRIMARY KEY ("id"))`,
			seedSQL: "INSERT INTO " +
				postgresQualified(
					namespace,
					sqlitePostgresPersistencePreflightLiveTable,
				) +
				` ("id", "payload") VALUES ('same-key', 'sentinel')`,
			valueSQL:   "MIN(payload)",
			sentinel:   "sentinel",
			wantTokens: []string{"persistence is UNLOGGED"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := database.ExecContext(ctx, test.createSQL); err != nil {
				t.Fatalf("create incompatible target: %v", err)
			}
			if _, err := database.ExecContext(ctx, test.seedSQL); err != nil {
				t.Fatalf("seed incompatible target: %v", err)
			}

			observer := &sqlitePostgresLiveObserver{}
			result, err := SQLiteToPostgresWithObserver(
				ctx,
				config.Config{
					Source: config.Endpoint{
						Type:     "sqlite",
						Database: sourcePath,
					},
					Target: config.Endpoint{
						Type:     "postgres",
						Host:     parsed.Host,
						Port:     int(parsed.Port),
						Database: parsed.Database,
						User:     parsed.User,
						Password: parsed.Password,
						Schema:   namespace,
					},
					Migration: config.Migration{
						TargetMode:    "upsert",
						IncludeTables: []string{test.table},
					},
				},
				observer,
			)
			if err == nil {
				t.Fatal("incompatible target unexpectedly passed preflight")
			}
			for _, token := range test.wantTokens {
				if !strings.Contains(err.Error(), token) {
					t.Fatalf(
						"preflight error = %v, want token %q",
						err,
						token,
					)
				}
			}
			if result != (Result{}) {
				t.Fatalf("failed upsert result = %+v, want zero result", result)
			}
			if len(observer.tableSets) != 0 ||
				len(observer.before) != 0 ||
				len(observer.after) != 0 {
				t.Fatalf(
					"callbacks occurred before catalog rejection: table_sets=%#v before=%#v after=%#v",
					observer.tableSets,
					observer.before,
					observer.after,
				)
			}

			var (
				count int
				value string
			)
			query := "SELECT COUNT(*), " + test.valueSQL + " FROM " +
				postgresQualified(namespace, test.table)
			if err := database.QueryRowContext(ctx, query).Scan(
				&count,
				&value,
			); err != nil {
				t.Fatalf("read target after rejected upsert: %v", err)
			}
			if count != 1 || value != test.sentinel {
				t.Fatalf(
					"target after rejected upsert = (%d, %q), want untouched sentinel (1, %q)",
					count,
					value,
					test.sentinel,
				)
			}
		})
	}
}
