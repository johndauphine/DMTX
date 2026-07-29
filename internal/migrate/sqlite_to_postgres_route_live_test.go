package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	sqlitePostgresLiveTable        = `quoted"rows`
	sqlitePostgresLiveTenantColumn = `tenant"key`
	sqlitePostgresLiveItemColumn   = "item id"
	sqlitePostgresLiveNoteColumn   = "note text"
	sqlitePostgresLiveAmountColumn = "amount"
	sqlitePostgresLiveBlobColumn   = "raw bytes"
	sqlitePostgresLiveEmptyBlob    = 1
	sqlitePostgresLiveExactNumeric = 499
	sqlitePostgresLiveUpsertRow    = 17
)

type sqlitePostgresLiveCompletion struct {
	table string
	rows  int
}

type sqlitePostgresLiveObserver struct {
	tableSets [][]string
	before    []string
	after     []sqlitePostgresLiveCompletion
}

func (observer *sqlitePostgresLiveObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	observer.tableSets = append(
		observer.tableSets,
		append([]string(nil), tables...),
	)
	return nil
}

func (observer *sqlitePostgresLiveObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	observer.before = append(observer.before, table)
	return nil
}

func (observer *sqlitePostgresLiveObserver) AfterTable(
	_ context.Context,
	table string,
	rows int,
) error {
	observer.after = append(
		observer.after,
		sqlitePostgresLiveCompletion{table: table, rows: rows},
	)
	return nil
}

func TestSQLiteToPostgresComposedRouteLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the live SQLite-to-PostgreSQL route test",
		)
	}

	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	t.Run("TLS endpoint", func(t *testing.T) {
		if !postgresRouteLiveRequiresTLS(parsed) {
			t.Fatal(
				"DMTX_TEST_POSTGRES_DSN must require TLS for the public route test",
			)
		}
		testSQLiteToPostgresComposedRouteTLS(t, dsn, parsed)
	})
}

func postgresRouteLiveRequiresTLS(parsed *pgx.ConnConfig) bool {
	if parsed.TLSConfig == nil {
		return false
	}
	for _, fallback := range parsed.Fallbacks {
		if fallback.TLSConfig == nil {
			return false
		}
	}
	return true
}

func testSQLiteToPostgresComposedRouteTLS(
	t *testing.T,
	dsn string,
	parsed *pgx.ConnConfig,
) {
	t.Helper()
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
		"dmtx_sqlite_pg_%d_%d",
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

	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	t.Run("native composed migration", func(t *testing.T) {
		testSQLiteToPostgresNativeComposedMigration(
			t,
			ctx,
			database,
			endpoint,
		)
	})
	t.Run("all-table preflight precedes mutation", func(t *testing.T) {
		testSQLiteToPostgresPreflightPrecedesMutation(
			t,
			ctx,
			database,
			endpoint,
		)
	})
}

func testSQLiteToPostgresNativeComposedMigration(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) {
	t.Helper()
	sourcePath := createSQLitePostgresLiveSource(t, ctx)
	observer := &sqlitePostgresLiveObserver{}
	result, err := SQLiteToPostgresWithObserver(
		ctx,
		config.Config{
			Source: config.Endpoint{
				Type:     "sqlite",
				Database: sourcePath,
			},
			Target: endpoint,
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: []string{sqlitePostgresLiveTable},
			},
		},
		observer,
	)
	if err != nil {
		t.Fatalf("migrate through composed SQLite-to-PostgreSQL route: %v", err)
	}
	if result.Tables != 1 || result.Rows != 501 || !result.Validated {
		t.Fatalf("migration result = %+v, want 1 table, 501 rows, validated", result)
	}
	assertSQLitePostgresLiveObserver(t, observer)
	assertSQLitePostgresLiveSchema(t, ctx, database, endpoint.Schema)
	assertSQLitePostgresLiveRows(t, ctx, database, endpoint.Schema)

	updateSQLitePostgresLiveSource(t, ctx, sourcePath)
	upsertObserver := &sqlitePostgresLiveObserver{}
	upsertResult, err := SQLiteToPostgresWithObserver(
		ctx,
		config.Config{
			Source: config.Endpoint{
				Type:     "sqlite",
				Database: sourcePath,
			},
			Target: endpoint,
			Migration: config.Migration{
				TargetMode:    "upsert",
				IncludeTables: []string{sqlitePostgresLiveTable},
			},
		},
		upsertObserver,
	)
	if err != nil {
		t.Fatalf("upsert through composed SQLite-to-PostgreSQL route: %v", err)
	}
	if upsertResult.Tables != 1 ||
		upsertResult.Rows != 501 ||
		!upsertResult.Validated {
		t.Fatalf(
			"upsert result = %+v, want 1 table, 501 rows, validated",
			upsertResult,
		)
	}
	assertSQLitePostgresLiveObserver(t, upsertObserver)
	assertSQLitePostgresLiveUpsert(t, ctx, database, endpoint.Schema)
}

func assertSQLitePostgresLiveObserver(
	t *testing.T,
	observer *sqlitePostgresLiveObserver,
) {
	t.Helper()
	wantSets := [][]string{{sqlitePostgresLiveTable}}
	if !reflect.DeepEqual(observer.tableSets, wantSets) {
		t.Fatalf(
			"BeforeTables calls = %#v, want %#v",
			observer.tableSets,
			wantSets,
		)
	}
	if !reflect.DeepEqual(observer.before, []string{sqlitePostgresLiveTable}) {
		t.Fatalf("BeforeTable calls = %#v", observer.before)
	}
	wantAfter := []sqlitePostgresLiveCompletion{{
		table: sqlitePostgresLiveTable,
		rows:  501,
	}}
	if !reflect.DeepEqual(observer.after, wantAfter) {
		t.Fatalf("AfterTable calls = %#v, want %#v", observer.after, wantAfter)
	}
}

func createSQLitePostgresLiveSource(
	t *testing.T,
	ctx context.Context,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open live SQLite source: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE "quoted""rows" (
			"tenant""key" TEXT NOT NULL,
			"item id" INTEGER NOT NULL,
			"note text" TEXT,
			"amount" NUMERIC NOT NULL,
			"raw bytes" BLOB,
			PRIMARY KEY ("tenant""key", "item id")
		)
	`); err != nil {
		_ = database.Close()
		t.Fatalf("create live SQLite source table: %v", err)
	}
	statement, err := database.PrepareContext(ctx, `
		INSERT INTO "quoted""rows" (
			"tenant""key",
			"item id",
			"note text",
			"amount",
			"raw bytes"
		) VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = database.Close()
		t.Fatalf("prepare live SQLite source rows: %v", err)
	}
	for index := 500; index >= 0; index-- {
		var note any = sqlitePostgresLiveNote(index)
		if index%11 == 0 {
			note = nil
		}
		var raw any = sqlitePostgresLiveBlob(index)
		if index%13 == 0 {
			raw = nil
		}
		if _, err := statement.ExecContext(
			ctx,
			sqlitePostgresLiveTenant(index),
			index,
			note,
			sqlitePostgresLiveAmount(index),
			raw,
		); err != nil {
			_ = statement.Close()
			_ = database.Close()
			t.Fatalf("insert live SQLite source row %d: %v", index, err)
		}
	}
	if err := statement.Close(); err != nil {
		_ = database.Close()
		t.Fatalf("close live SQLite source statement: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close live SQLite source: %v", err)
	}
	return path
}

func updateSQLitePostgresLiveSource(
	t *testing.T,
	ctx context.Context,
	path string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite source for upsert change: %v", err)
	}
	result, err := database.ExecContext(
		ctx,
		"UPDATE "+quote(sqlitePostgresLiveTable)+
			" SET "+quote(sqlitePostgresLiveNoteColumn)+" = ?"+
			" WHERE "+quote(sqlitePostgresLiveTenantColumn)+" = ?"+
			" AND "+quote(sqlitePostgresLiveItemColumn)+" = ?",
		sqlitePostgresLiveUpsertNote(),
		sqlitePostgresLiveTenant(sqlitePostgresLiveUpsertRow),
		sqlitePostgresLiveUpsertRow,
	)
	if err != nil {
		_ = database.Close()
		t.Fatalf("update SQLite source for upsert: %v", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		_ = database.Close()
		t.Fatalf("read SQLite upsert update count: %v", err)
	}
	if changed != 1 {
		_ = database.Close()
		t.Fatalf("SQLite upsert update count = %d, want 1", changed)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close SQLite source after upsert change: %v", err)
	}
}

func assertSQLitePostgresLiveUpsert(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, sqlitePostgresLiveTable),
	).Scan(&count); err != nil {
		t.Fatalf("count PostgreSQL rows after upsert: %v", err)
	}
	if count != 501 {
		t.Fatalf("PostgreSQL row count after upsert = %d, want 501", count)
	}
	var note string
	if err := database.QueryRowContext(
		ctx,
		"SELECT "+postgresIdentifier(sqlitePostgresLiveNoteColumn)+
			" FROM "+postgresQualified(namespace, sqlitePostgresLiveTable)+
			" WHERE "+
			postgresIdentifier(sqlitePostgresLiveTenantColumn)+" = $1"+
			" AND "+
			postgresIdentifier(sqlitePostgresLiveItemColumn)+" = $2",
		sqlitePostgresLiveTenant(sqlitePostgresLiveUpsertRow),
		sqlitePostgresLiveUpsertRow,
	).Scan(&note); err != nil {
		t.Fatalf("read PostgreSQL row after upsert: %v", err)
	}
	if note != sqlitePostgresLiveUpsertNote() {
		t.Fatalf(
			"PostgreSQL note after upsert = %q, want %q",
			note,
			sqlitePostgresLiveUpsertNote(),
		)
	}
}

func assertSQLitePostgresLiveSchema(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, namespace, sqlitePostgresLiveTable)
	if err != nil {
		t.Fatalf("read migrated PostgreSQL columns: %v", err)
	}
	defer rows.Close()
	type liveColumn struct {
		name     string
		dataType string
		nullable string
	}
	var got []liveColumn
	for rows.Next() {
		var column liveColumn
		if err := rows.Scan(
			&column.name,
			&column.dataType,
			&column.nullable,
		); err != nil {
			t.Fatalf("scan migrated PostgreSQL column: %v", err)
		}
		got = append(got, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated PostgreSQL columns: %v", err)
	}
	want := []liveColumn{
		{name: sqlitePostgresLiveTenantColumn, dataType: "text", nullable: "NO"},
		{name: sqlitePostgresLiveItemColumn, dataType: "bigint", nullable: "NO"},
		{name: sqlitePostgresLiveNoteColumn, dataType: "text", nullable: "YES"},
		{name: sqlitePostgresLiveAmountColumn, dataType: "numeric", nullable: "NO"},
		{name: sqlitePostgresLiveBlobColumn, dataType: "bytea", nullable: "YES"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("migrated PostgreSQL columns = %#v, want %#v", got, want)
	}
	var (
		numericPrecision int
		numericScale     int
	)
	if err := database.QueryRowContext(ctx, `
		SELECT numeric_precision, numeric_scale
		FROM information_schema.columns
		WHERE table_schema = $1
		  AND table_name = $2
		  AND column_name = $3
	`, namespace, sqlitePostgresLiveTable, sqlitePostgresLiveAmountColumn).Scan(
		&numericPrecision,
		&numericScale,
	); err != nil {
		t.Fatalf("read migrated PostgreSQL numeric shape: %v", err)
	}
	if numericPrecision != 38 || numericScale != 10 {
		t.Fatalf(
			"migrated numeric shape = (%d, %d), want (38, 10)",
			numericPrecision,
			numericScale,
		)
	}

	keyRows, err := database.QueryContext(ctx, `
		SELECT key_column_usage.column_name,
		       key_column_usage.ordinal_position
		FROM information_schema.table_constraints
		JOIN information_schema.key_column_usage
		  ON table_constraints.constraint_name =
		     key_column_usage.constraint_name
		 AND table_constraints.table_schema =
		     key_column_usage.table_schema
		 AND table_constraints.table_name =
		     key_column_usage.table_name
		WHERE table_constraints.table_schema = $1
		  AND table_constraints.table_name = $2
		  AND table_constraints.constraint_type = 'PRIMARY KEY'
		ORDER BY key_column_usage.ordinal_position
	`, namespace, sqlitePostgresLiveTable)
	if err != nil {
		t.Fatalf("read migrated PostgreSQL primary key: %v", err)
	}
	defer keyRows.Close()
	type liveKey struct {
		name     string
		position int
	}
	var keys []liveKey
	for keyRows.Next() {
		var key liveKey
		if err := keyRows.Scan(&key.name, &key.position); err != nil {
			t.Fatalf("scan migrated PostgreSQL primary key: %v", err)
		}
		keys = append(keys, key)
	}
	if err := keyRows.Err(); err != nil {
		t.Fatalf("iterate migrated PostgreSQL primary key: %v", err)
	}
	wantKeys := []liveKey{
		{name: sqlitePostgresLiveTenantColumn, position: 1},
		{name: sqlitePostgresLiveItemColumn, position: 2},
	}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("migrated PostgreSQL primary key = %#v, want %#v", keys, wantKeys)
	}
}

func assertSQLitePostgresLiveRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	query := "SELECT " +
		postgresIdentifier(sqlitePostgresLiveTenantColumn) + ", " +
		postgresIdentifier(sqlitePostgresLiveItemColumn) + ", " +
		postgresIdentifier(sqlitePostgresLiveNoteColumn) + ", " +
		postgresIdentifier(sqlitePostgresLiveAmountColumn) + "::text, " +
		postgresIdentifier(sqlitePostgresLiveBlobColumn) + ", " +
		postgresIdentifier(sqlitePostgresLiveBlobColumn) +
		" IS NULL" +
		" FROM " + postgresQualified(namespace, sqlitePostgresLiveTable) +
		" ORDER BY " +
		postgresIdentifier(sqlitePostgresLiveTenantColumn) + ", " +
		postgresIdentifier(sqlitePostgresLiveItemColumn)
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("read migrated PostgreSQL rows: %v", err)
	}
	defer rows.Close()

	seen := make([]bool, 501)
	count := 0
	previousTenant := ""
	var previousItem int64
	for rows.Next() {
		var (
			tenant  string
			item    int64
			note    sql.NullString
			amount  string
			raw     []byte
			rawNull bool
		)
		if err := rows.Scan(
			&tenant,
			&item,
			&note,
			&amount,
			&raw,
			&rawNull,
		); err != nil {
			t.Fatalf("scan migrated PostgreSQL row: %v", err)
		}
		if item < 0 || item >= int64(len(seen)) {
			t.Fatalf("migrated item id = %d, want 0 through 500", item)
		}
		index := int(item)
		if seen[index] {
			t.Fatalf("migrated item id %d appeared more than once", item)
		}
		seen[index] = true
		if tenant != sqlitePostgresLiveTenant(index) {
			t.Fatalf(
				"tenant for item %d = %q, want %q",
				item,
				tenant,
				sqlitePostgresLiveTenant(index),
			)
		}
		if count > 0 &&
			(tenant < previousTenant ||
				tenant == previousTenant && item <= previousItem) {
			t.Fatalf(
				"migrated rows are not in composite-key order: (%q, %d) after (%q, %d)",
				tenant,
				item,
				previousTenant,
				previousItem,
			)
		}
		previousTenant, previousItem = tenant, item
		if index%11 == 0 {
			if note.Valid {
				t.Fatalf("note for item %d = %q, want NULL", item, note.String)
			}
		} else if !note.Valid || note.String != sqlitePostgresLiveNote(index) {
			t.Fatalf(
				"note for item %d = %#v, want %q",
				item,
				note,
				sqlitePostgresLiveNote(index),
			)
		}
		expectedAmount := sqlitePostgresLiveExpectedAmount(index)
		if amount != expectedAmount {
			t.Fatalf(
				"amount for item %d = %q, want %q",
				item,
				amount,
				expectedAmount,
			)
		}
		if index%13 == 0 {
			if !rawNull {
				t.Fatalf("blob for item %d is non-NULL, want NULL", item)
			}
		} else {
			if rawNull {
				t.Fatalf("blob for item %d is NULL, want non-NULL", item)
			}
			expectedBlob := sqlitePostgresLiveBlob(index)
			if !bytes.Equal(raw, expectedBlob) {
				t.Fatalf(
					"blob for item %d = %v, want %v",
					item,
					raw,
					expectedBlob,
				)
			}
			if index == sqlitePostgresLiveEmptyBlob && len(raw) != 0 {
				t.Fatalf(
					"non-NULL empty blob for item %d has length %d",
					item,
					len(raw),
				)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated PostgreSQL rows: %v", err)
	}
	if count != 501 {
		t.Fatalf("migrated PostgreSQL row count = %d, want 501", count)
	}
	for index, present := range seen {
		if !present {
			t.Fatalf("migrated PostgreSQL rows are missing item %d", index)
		}
	}
}

func testSQLiteToPostgresPreflightPrecedesMutation(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "preflight.sqlite")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open preflight SQLite source: %v", err)
	}
	if _, err := source.ExecContext(ctx, `
		CREATE TABLE a_good (
			tenant TEXT NOT NULL,
			id INTEGER NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, id)
		);
		CREATE TABLE z_bad (
			tenant TEXT NOT NULL,
			id INTEGER NOT NULL,
			payload TEXT NOT NULL,
			generated TEXT GENERATED ALWAYS AS (payload || '!') STORED,
			PRIMARY KEY (tenant, id)
		);
		INSERT INTO a_good (tenant, id, payload)
		VALUES ('tenant', 1, 'replacement')
	`); err != nil {
		_ = source.Close()
		t.Fatalf("create preflight SQLite source: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close preflight SQLite source: %v", err)
	}

	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(endpoint.Schema, "a_good")+
			` ("tenant" TEXT NOT NULL, "id" BIGINT NOT NULL, "payload" TEXT NOT NULL, PRIMARY KEY ("tenant", "id"))`,
	); err != nil {
		t.Fatalf("create preflight sentinel table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(endpoint.Schema, "a_good")+
			` ("tenant", "id", "payload") VALUES ('tenant', 1, 'sentinel')`,
	); err != nil {
		t.Fatalf("seed preflight sentinel table: %v", err)
	}

	observer := &sqlitePostgresLiveObserver{}
	result, err := SQLiteToPostgresWithObserver(
		ctx,
		config.Config{
			Source: config.Endpoint{
				Type:     "sqlite",
				Database: sourcePath,
			},
			Target: endpoint,
			Migration: config.Migration{
				TargetMode: "drop_recreate",
			},
		},
		observer,
	)
	if err == nil {
		t.Fatal("unsupported generated column unexpectedly passed preflight")
	}
	var policy *schema.PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("preflight error = %T, want *schema.PolicyError", err)
	}
	if policy.Operation != "discover SQLite generated or hidden column" ||
		policy.Type != "generated" ||
		policy.Target != string(schema.SQLite) {
		t.Fatalf("preflight policy error = %+v", policy)
	}
	if result != (Result{}) {
		t.Fatalf("failed preflight result = %+v, want zero result", result)
	}
	if len(observer.tableSets) != 0 ||
		len(observer.before) != 0 ||
		len(observer.after) != 0 {
		t.Fatalf(
			"callbacks occurred before failed preflight: table_sets=%#v before=%#v after=%#v",
			observer.tableSets,
			observer.before,
			observer.after,
		)
	}

	var (
		sentinelCount int
		sentinelValue string
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*), MIN(\"payload\") FROM "+
			postgresQualified(endpoint.Schema, "a_good"),
	).Scan(&sentinelCount, &sentinelValue); err != nil {
		t.Fatalf("read preflight sentinel after rejection: %v", err)
	}
	if sentinelCount != 1 || sentinelValue != "sentinel" {
		t.Fatalf(
			"preflight sentinel = (%d, %q), want (1, %q)",
			sentinelCount,
			sentinelValue,
			"sentinel",
		)
	}
	var badTables int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM information_schema.tables
		  WHERE table_schema = $1
		    AND table_name = 'z_bad'`,
		endpoint.Schema,
	).Scan(&badTables); err != nil {
		t.Fatalf("inspect rejected PostgreSQL table: %v", err)
	}
	if badTables != 0 {
		t.Fatalf("rejected PostgreSQL tables created = %d, want 0", badTables)
	}
}

func sqlitePostgresLiveTenant(index int) string {
	return fmt.Sprintf("tenant-%02d", index%7)
}

func sqlitePostgresLiveNote(index int) string {
	switch index {
	case 1:
		return ""
	case 2:
		return "Zażółć gęślą jaźń — 東京 — 😀"
	}
	return fmt.Sprintf("note-%03d with \"quotes\"", index)
}

func sqlitePostgresLiveAmount(index int) string {
	if index == sqlitePostgresLiveExactNumeric {
		return "9007199254740993"
	}
	return fmt.Sprintf("%d.25", index)
}

func sqlitePostgresLiveExpectedAmount(index int) string {
	if index == sqlitePostgresLiveExactNumeric {
		return "9007199254740993.0000000000"
	}
	return fmt.Sprintf("%d.2500000000", index)
}

func sqlitePostgresLiveBlob(index int) []byte {
	if index == sqlitePostgresLiveEmptyBlob {
		return []byte{}
	}
	return []byte{
		byte(index % 251),
		0,
		byte((index * 3) % 251),
	}
}

func sqlitePostgresLiveUpsertNote() string {
	return "updated through staged COPY — 東京 🚀"
}
