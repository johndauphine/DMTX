package migrate

import (
	"bytes"
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

const sqlitePostgresScalarLiveTable = "scalar defaults"

type postgresScalarCatalogColumn struct {
	name       string
	dataType   string
	length     sql.NullInt64
	precision  sql.NullInt64
	scale      sql.NullInt64
	defaultSQL sql.NullString
}

func TestSQLiteToPostgresScalarSchemaFidelityLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the live SQLite-to-PostgreSQL scalar schema test",
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
		"dmtx_sqlite_pg_scalar_%d_%d",
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

	sourcePath := createSQLitePostgresScalarLiveSource(t, ctx)
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
				TargetMode: "drop_recreate",
				IncludeTables: []string{
					sqlitePostgresScalarLiveTable,
				},
			},
		},
		observer,
	)
	if err != nil {
		t.Fatalf("migrate scalar schema through composed route: %v", err)
	}
	if result.Tables != 1 || result.Rows != 1 || !result.Validated {
		t.Fatalf(
			"migration result = %+v, want 1 table, 1 row, validated",
			result,
		)
	}
	assertSQLitePostgresScalarCatalog(t, ctx, database, namespace)
	assertSQLitePostgresScalarRows(t, ctx, database, namespace)
	assertSQLitePostgresTargetDefaults(t, ctx, database, namespace)
	assertSQLitePostgresStatementTimeDefault(t, ctx, database, namespace)
}

func createSQLitePostgresScalarLiveSource(
	t *testing.T,
	ctx context.Context,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scalar-source.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open scalar SQLite source: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE "scalar defaults" (
			"code" CHAR(8) NOT NULL,
			"revision" INTEGER NOT NULL,
			"name" VARCHAR(12) NOT NULL DEFAULT 'guest',
			"amount" DECIMAL(7,2) NOT NULL DEFAULT (0.00),
			"enabled" BOOLEAN NOT NULL DEFAULT TRUE,
			"payload" BLOB DEFAULT X'00FF',
			"event_day" DATE NOT NULL DEFAULT CURRENT_DATE,
			"created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY ("code", "revision")
		);
		INSERT INTO "scalar defaults" ("code", "revision")
		VALUES ('alpha', 1);
	`); err != nil {
		_ = database.Close()
		t.Fatalf("create scalar SQLite fixture: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close scalar SQLite source: %v", err)
	}
	return path
}

func assertSQLitePostgresScalarCatalog(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, namespace, sqlitePostgresScalarLiveTable)
	if err != nil {
		t.Fatalf("query scalar PostgreSQL catalog: %v", err)
	}
	defer rows.Close()
	columns := make([]postgresScalarCatalogColumn, 0, 8)
	for rows.Next() {
		var column postgresScalarCatalogColumn
		if err := rows.Scan(
			&column.name,
			&column.dataType,
			&column.length,
			&column.precision,
			&column.scale,
			&column.defaultSQL,
		); err != nil {
			t.Fatalf("scan scalar PostgreSQL catalog: %v", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate scalar PostgreSQL catalog: %v", err)
	}
	if len(columns) != 8 {
		t.Fatalf("scalar PostgreSQL columns = %#v", columns)
	}
	wantTypes := []string{
		"character varying",
		"bigint",
		"character varying",
		"numeric",
		"boolean",
		"bytea",
		"date",
		"timestamp without time zone",
	}
	for index, column := range columns {
		if column.dataType != wantTypes[index] {
			t.Fatalf(
				"column %s type = %q, want %q",
				column.name,
				column.dataType,
				wantTypes[index],
			)
		}
	}
	if !columns[0].length.Valid || columns[0].length.Int64 != 8 ||
		!columns[2].length.Valid || columns[2].length.Int64 != 12 {
		t.Fatalf("character modifiers = %#v", columns)
	}
	if !columns[3].precision.Valid || columns[3].precision.Int64 != 7 ||
		!columns[3].scale.Valid || columns[3].scale.Int64 != 2 {
		t.Fatalf("numeric modifiers = %#v", columns[3])
	}
	defaultTokens := map[int][]string{
		2: {"guest"},
		3: {"0.00"},
		4: {"true"},
		5: {"decode", "00ff", "hex"},
		6: {"statement_timestamp", "UTC", "date"},
		7: {"date_trunc", "second", "statement_timestamp", "UTC"},
	}
	for index, tokens := range defaultTokens {
		if !columns[index].defaultSQL.Valid {
			t.Fatalf("column %s has no default", columns[index].name)
		}
		for _, token := range tokens {
			if !strings.Contains(columns[index].defaultSQL.String, token) {
				t.Fatalf(
					"column %s default = %q, want token %q",
					columns[index].name,
					columns[index].defaultSQL.String,
					token,
				)
			}
		}
	}
}

func assertSQLitePostgresScalarRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var name, amount, payload, eventDay string
	var enabled bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT name, amount::text, enabled, encode(payload, 'hex'), event_day::text FROM "+
			postgresQualified(namespace, sqlitePostgresScalarLiveTable)+
			" WHERE code = $1 AND revision = $2",
		"alpha",
		int64(1),
	).Scan(&name, &amount, &enabled, &payload, &eventDay); err != nil {
		t.Fatalf("read migrated scalar row: %v", err)
	}
	if name != "guest" || amount != "0.00" || !enabled ||
		payload != "00ff" || eventDay == "" {
		t.Fatalf(
			"migrated scalar row = name %q amount %q enabled %v payload %q day %q",
			name,
			amount,
			enabled,
			payload,
			eventDay,
		)
	}
}

func assertSQLitePostgresTargetDefaults(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var name, amount, payload string
	var enabled, utcDay, secondPrecision, utcTimestamp bool
	err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresScalarLiveTable)+
			" (code, revision) VALUES ($1, $2) "+
			"RETURNING name, amount::text, enabled, encode(payload, 'hex'), "+
			"event_day = (statement_timestamp() AT TIME ZONE 'UTC')::date, "+
			"created_at = date_trunc('second', created_at), "+
			"created_at = date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC')",
		"fallback",
		int64(2),
	).Scan(
		&name,
		&amount,
		&enabled,
		&payload,
		&utcDay,
		&secondPrecision,
		&utcTimestamp,
	)
	if err != nil {
		t.Fatalf("exercise PostgreSQL scalar defaults: %v", err)
	}
	if name != "guest" || amount != "0.00" || !enabled ||
		!bytes.Equal([]byte(payload), []byte("00ff")) ||
		!utcDay || !secondPrecision || !utcTimestamp {
		t.Fatalf(
			"target defaults = name %q amount %q enabled %v payload %q UTC-day %v second-precision %v UTC-timestamp %v",
			name,
			amount,
			enabled,
			payload,
			utcDay,
			secondPrecision,
			utcTimestamp,
		)
	}
}

func assertSQLitePostgresStatementTimeDefault(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin statement-time default transaction: %v", err)
	}
	defer transaction.Rollback()

	if _, err := transaction.ExecContext(ctx, "SELECT pg_sleep(1.1)"); err != nil {
		t.Fatalf("age statement-time default transaction: %v", err)
	}
	var createdAt, statementTime, transactionTime time.Time
	err = transaction.QueryRowContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresScalarLiveTable)+
			" (code, revision) VALUES ($1, $2) "+
			"RETURNING created_at, "+
			"date_trunc('second', statement_timestamp() AT TIME ZONE 'UTC'), "+
			"date_trunc('second', transaction_timestamp() AT TIME ZONE 'UTC')",
		"stmt",
		int64(3),
	).Scan(
		&createdAt,
		&statementTime,
		&transactionTime,
	)
	if err != nil {
		t.Fatalf("exercise statement-time default in aged transaction: %v", err)
	}
	if !createdAt.Equal(statementTime) {
		t.Fatalf(
			"created_at = %s, want statement time %s",
			createdAt.Format(time.RFC3339Nano),
			statementTime.Format(time.RFC3339Nano),
		)
	}
	if createdAt.Equal(transactionTime) {
		t.Fatalf(
			"created_at = %s, unexpectedly used transaction start %s",
			createdAt.Format(time.RFC3339Nano),
			transactionTime.Format(time.RFC3339Nano),
		)
	}
}
