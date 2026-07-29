package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

const (
	sqlitePostgresRichAccountsTable = "accounts"
	sqlitePostgresRichEventsTable   = "account_events"
)

func TestSQLiteToPostgresRichSchemaObjectsLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the live SQLite-to-PostgreSQL schema-object test",
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
		"dmtx_sqlite_pg_objects_%d_%d",
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

	sourcePath := createSQLitePostgresRichSource(t, ctx)
	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	result, err := SQLiteToPostgresWithObserver(
		ctx,
		sqlitePostgresRichConfig(
			sourcePath,
			endpoint,
			"drop_recreate",
		),
		nil,
	)
	if err != nil {
		t.Fatalf("migrate rich SQLite schema to PostgreSQL: %v", err)
	}
	if result.Tables != 2 || result.Rows != 5 || !result.Validated {
		t.Fatalf(
			"migration result = %+v, want 2 tables, 5 rows, validated",
			result,
		)
	}

	assertSQLitePostgresRichColumns(t, ctx, database, namespace)
	assertSQLitePostgresRichRows(t, ctx, database, namespace)
	assertSQLitePostgresRichIndexes(t, ctx, database, namespace)
	assertSQLitePostgresRichConstraints(t, ctx, database, namespace)
	assertSQLitePostgresRichIdentitySequence(
		t,
		ctx,
		database,
		namespace,
		50,
	)
	exerciseSQLitePostgresRichBehavior(
		t,
		ctx,
		database,
		namespace,
	)

	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			` (id, external_id, balance, created_at, status, label, fixed_code)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		int64(200),
		"target-only-high",
		"9.99",
		"2026-07-29 12:00:00.000",
		"active",
		"target-only",
		"TGT",
	); err != nil {
		t.Fatalf("seed target-only high identity: %v", err)
	}
	beforeObjects := snapshotSQLitePostgresRichObjects(
		t,
		ctx,
		database,
		namespace,
	)

	updateSQLitePostgresRichSource(t, ctx, sourcePath)
	upsertResult, err := SQLiteToPostgresWithObserver(
		ctx,
		sqlitePostgresRichConfig(sourcePath, endpoint, "upsert"),
		nil,
	)
	if err != nil {
		t.Fatalf("upsert rich SQLite schema into PostgreSQL: %v", err)
	}
	if upsertResult.Tables != 2 ||
		upsertResult.Rows != 7 ||
		!upsertResult.Validated {
		t.Fatalf(
			"upsert result = %+v, want 2 tables, 7 rows, validated",
			upsertResult,
		)
	}
	afterObjects := snapshotSQLitePostgresRichObjects(
		t,
		ctx,
		database,
		namespace,
	)
	if !reflect.DeepEqual(afterObjects, beforeObjects) {
		t.Fatalf(
			"PostgreSQL objects changed across upsert:\nbefore: %#v\nafter:  %#v",
			beforeObjects,
			afterObjects,
		)
	}
	assertSQLitePostgresRichUpsertRows(t, ctx, database, namespace)
	assertSQLitePostgresRichIdentitySequence(
		t,
		ctx,
		database,
		namespace,
		200,
	)
	assertSQLitePostgresRichNextIdentity(
		t,
		ctx,
		database,
		namespace,
		201,
	)
}

func sqlitePostgresRichConfig(
	sourcePath string,
	endpoint config.Endpoint,
	mode string,
) config.Config {
	return config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		Target: endpoint,
		Migration: config.Migration{
			TargetMode: mode,
			IncludeTables: []string{
				sqlitePostgresRichAccountsTable,
				sqlitePostgresRichEventsTable,
			},
		},
	}
}

func createSQLitePostgresRichSource(
	t *testing.T,
	ctx context.Context,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rich-schema.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rich SQLite source: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		PRAGMA foreign_keys = ON;
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			external_id VARCHAR(40) NOT NULL,
			balance DECIMAL(12,2) NOT NULL DEFAULT (0.00),
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
			payload BLOB DEFAULT X'00',
			status TEXT NOT NULL DEFAULT 'active',
			label VARCHAR(40) NOT NULL DEFAULT 'unknown',
			fixed_code CHAR(4) NOT NULL DEFAULT 'NONE',
			CHECK (balance >= 0),
			CHECK (status IN ('active', 'paused'))
		);
		CREATE UNIQUE INDEX accounts_external_id_uq
			ON accounts(external_id);
		CREATE INDEX accounts_status_balance_idx
			ON accounts(status, balance DESC);
		INSERT INTO accounts(
			id,
			external_id,
			balance,
			created_at,
			payload,
			status,
			label,
			fixed_code
		) VALUES
			(
				1,
				'alpha',
				12.50,
				'2026-07-27 12:00:00.123',
				X'00FF',
				'active',
				'primary',
				'A1'
			),
			(
				2,
				'beta',
				0.00,
				'2026-07-27 12:00:01.000',
				NULL,
				'paused',
				'',
				'B2'
			);
		INSERT INTO accounts(
			id,
			external_id,
			balance,
			created_at,
			status,
			label,
			fixed_code
		) VALUES (
			50,
			'deleted-high-water',
			1.00,
			'2026-07-27 12:00:02.000',
			'active',
			'deleted',
			'D50'
		);
		DELETE FROM accounts WHERE id = 50;

		CREATE TABLE account_events (
			account_id INTEGER NOT NULL,
			sequence_no INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT 'none',
			raw BLOB,
			PRIMARY KEY (sequence_no, account_id),
			UNIQUE (account_id, note),
			FOREIGN KEY (account_id)
				REFERENCES accounts(id)
				ON UPDATE CASCADE
				ON DELETE RESTRICT,
			CHECK (sequence_no > 0)
		);
		INSERT INTO account_events(
			account_id,
			sequence_no,
			note,
			raw
		) VALUES
			(1, 2, 'second', X'1020'),
			(1, 1, 'first', NULL),
			(2, 1, 'unicode-東京', X'');
	`); err != nil {
		_ = database.Close()
		t.Fatalf("create rich SQLite fixture: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close rich SQLite source: %v", err)
	}
	return path
}

func updateSQLitePostgresRichSource(
	t *testing.T,
	ctx context.Context,
	path string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rich SQLite source for upsert: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		UPDATE accounts
		   SET label = 'source-updated'
		 WHERE id = 2;
		UPDATE account_events
		   SET note = 'source-event-updated'
		 WHERE account_id = 1 AND sequence_no = 2;
		INSERT INTO accounts(
			id,
			external_id,
			balance,
			created_at,
			status,
			label,
			fixed_code
		) VALUES (
			3,
			'source-new-parent',
			3.33,
			'2026-07-29 12:30:00.123',
			'active',
			'new-parent',
			'N3'
		);
		INSERT INTO account_events(
			account_id,
			sequence_no,
			note,
			raw
		) VALUES (3, 1, 'source-new-child', X'CAFE');
	`); err != nil {
		_ = database.Close()
		t.Fatalf("update rich SQLite source for upsert: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close rich SQLite source after upsert update: %v", err)
	}
}

type sqlitePostgresRichColumn struct {
	name               string
	dataType           string
	nullable           string
	characterLength    sql.NullInt64
	numericPrecision   sql.NullInt64
	numericScale       sql.NullInt64
	timestampPrecision sql.NullInt64
	defaultSQL         sql.NullString
	identity           string
	identityGeneration sql.NullString
}

func assertSQLitePostgresRichColumns(
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
			is_nullable,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			column_default,
			is_identity,
			identity_generation
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, namespace, sqlitePostgresRichAccountsTable)
	if err != nil {
		t.Fatalf("query rich PostgreSQL columns: %v", err)
	}
	defer rows.Close()
	var columns []sqlitePostgresRichColumn
	for rows.Next() {
		var column sqlitePostgresRichColumn
		if err := rows.Scan(
			&column.name,
			&column.dataType,
			&column.nullable,
			&column.characterLength,
			&column.numericPrecision,
			&column.numericScale,
			&column.timestampPrecision,
			&column.defaultSQL,
			&column.identity,
			&column.identityGeneration,
		); err != nil {
			t.Fatalf("scan rich PostgreSQL column: %v", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rich PostgreSQL columns: %v", err)
	}
	if len(columns) != 8 {
		t.Fatalf("rich PostgreSQL columns = %#v, want 8 columns", columns)
	}
	wantNames := []string{
		"id",
		"external_id",
		"balance",
		"created_at",
		"payload",
		"status",
		"label",
		"fixed_code",
	}
	wantTypes := []string{
		"bigint",
		"character varying",
		"numeric",
		"timestamp without time zone",
		"bytea",
		"text",
		"character varying",
		"character varying",
	}
	wantNullable := []string{
		"NO", "NO", "NO", "NO", "YES", "NO", "NO", "NO",
	}
	for index, column := range columns {
		if column.name != wantNames[index] ||
			column.dataType != wantTypes[index] ||
			column.nullable != wantNullable[index] {
			t.Fatalf(
				"column %d = (%q, %q, %q), want (%q, %q, %q)",
				index+1,
				column.name,
				column.dataType,
				column.nullable,
				wantNames[index],
				wantTypes[index],
				wantNullable[index],
			)
		}
	}
	if columns[0].identity != "YES" ||
		!columns[0].identityGeneration.Valid ||
		columns[0].identityGeneration.String != "BY DEFAULT" {
		t.Fatalf("identity metadata = %#v", columns[0])
	}
	if columns[1].identity != "NO" ||
		!columns[1].characterLength.Valid ||
		columns[1].characterLength.Int64 != 40 {
		t.Fatalf("external_id metadata = %#v", columns[1])
	}
	if !columns[2].numericPrecision.Valid ||
		columns[2].numericPrecision.Int64 != 12 ||
		!columns[2].numericScale.Valid ||
		columns[2].numericScale.Int64 != 2 {
		t.Fatalf("balance metadata = %#v", columns[2])
	}
	if !columns[3].timestampPrecision.Valid ||
		columns[3].timestampPrecision.Int64 != 3 {
		t.Fatalf("created_at metadata = %#v", columns[3])
	}
	if !columns[6].characterLength.Valid ||
		columns[6].characterLength.Int64 != 40 {
		t.Fatalf("label metadata = %#v", columns[6])
	}
	if !columns[7].characterLength.Valid ||
		columns[7].characterLength.Int64 != 4 {
		t.Fatalf("fixed_code metadata = %#v", columns[7])
	}
	assertSQLitePostgresRichDefaultTokens(
		t,
		columns[2],
		"0.00",
	)
	assertSQLitePostgresRichDefaultTokens(
		t,
		columns[3],
		"date_trunc",
		"statement_timestamp",
		"utc",
	)
	assertSQLitePostgresRichDefaultTokens(
		t,
		columns[4],
		"decode",
		"00",
		"hex",
	)
	assertSQLitePostgresRichDefaultTokens(t, columns[5], "active")
	assertSQLitePostgresRichDefaultTokens(t, columns[6], "unknown")
	assertSQLitePostgresRichDefaultTokens(t, columns[7], "none")
}

func assertSQLitePostgresRichDefaultTokens(
	t *testing.T,
	column sqlitePostgresRichColumn,
	tokens ...string,
) {
	t.Helper()
	if !column.defaultSQL.Valid {
		t.Fatalf("column %s has no PostgreSQL default", column.name)
	}
	value := strings.ToLower(column.defaultSQL.String)
	for _, token := range tokens {
		if !strings.Contains(value, strings.ToLower(token)) {
			t.Fatalf(
				"column %s default = %q, want token %q",
				column.name,
				column.defaultSQL.String,
				token,
			)
		}
	}
}

func assertSQLitePostgresRichRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	rows, err := database.QueryContext(
		ctx,
		"SELECT id, external_id, balance::text, created_at, "+
			"encode(payload, 'hex'), status, label, rtrim(fixed_code::text) FROM "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" ORDER BY id",
	)
	if err != nil {
		t.Fatalf("query migrated rich PostgreSQL rows: %v", err)
	}
	defer rows.Close()
	type richRow struct {
		id        int64
		external  string
		balance   string
		createdAt time.Time
		payload   sql.NullString
		status    string
		label     string
		fixedCode string
	}
	var got []richRow
	for rows.Next() {
		var row richRow
		if err := rows.Scan(
			&row.id,
			&row.external,
			&row.balance,
			&row.createdAt,
			&row.payload,
			&row.status,
			&row.label,
			&row.fixedCode,
		); err != nil {
			t.Fatalf("scan migrated rich PostgreSQL row: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated rich PostgreSQL rows: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("migrated rich PostgreSQL rows = %#v", got)
	}
	if got[0].id != 1 ||
		got[0].external != "alpha" ||
		got[0].balance != "12.50" ||
		got[0].createdAt.Format("2006-01-02 15:04:05.000") !=
			"2026-07-27 12:00:00.123" ||
		!got[0].payload.Valid ||
		got[0].payload.String != "00ff" ||
		got[0].status != "active" ||
		got[0].label != "primary" ||
		got[0].fixedCode != "A1" {
		t.Fatalf("first migrated account = %#v", got[0])
	}
	if got[1].id != 2 ||
		got[1].external != "beta" ||
		got[1].balance != "0.00" ||
		got[1].createdAt.Format("2006-01-02 15:04:05.000") !=
			"2026-07-27 12:00:01.000" ||
		got[1].payload.Valid ||
		got[1].status != "paused" ||
		got[1].label != "" ||
		got[1].fixedCode != "B2" {
		t.Fatalf("second migrated account = %#v", got[1])
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated rich PostgreSQL accounts: %v", err)
	}

	eventRows, err := database.QueryContext(
		ctx,
		"SELECT account_id, sequence_no, note, encode(raw, 'hex') FROM "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" ORDER BY sequence_no, account_id",
	)
	if err != nil {
		t.Fatalf("query migrated rich PostgreSQL events: %v", err)
	}
	defer eventRows.Close()
	type richEvent struct {
		accountID  int64
		sequenceNo int64
		note       string
		raw        sql.NullString
	}
	var events []richEvent
	for eventRows.Next() {
		var event richEvent
		if err := eventRows.Scan(
			&event.accountID,
			&event.sequenceNo,
			&event.note,
			&event.raw,
		); err != nil {
			t.Fatalf("scan migrated rich PostgreSQL event: %v", err)
		}
		events = append(events, event)
	}
	if err := eventRows.Err(); err != nil {
		t.Fatalf("iterate migrated rich PostgreSQL events: %v", err)
	}
	wantEvents := []richEvent{
		{
			accountID:  1,
			sequenceNo: 1,
			note:       "first",
			raw:        sql.NullString{},
		},
		{
			accountID:  2,
			sequenceNo: 1,
			note:       "unicode-東京",
			raw:        sql.NullString{String: "", Valid: true},
		},
		{
			accountID:  1,
			sequenceNo: 2,
			note:       "second",
			raw:        sql.NullString{String: "1020", Valid: true},
		},
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf(
			"migrated rich PostgreSQL events = %#v, want %#v",
			events,
			wantEvents,
		)
	}
}

type sqlitePostgresRichIndex struct {
	objectID   int64
	name       string
	unique     bool
	primary    bool
	definition string
}

func assertSQLitePostgresRichIndexes(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	accounts := readSQLitePostgresRichIndexes(
		t,
		ctx,
		database,
		namespace,
		sqlitePostgresRichAccountsTable,
	)
	events := readSQLitePostgresRichIndexes(
		t,
		ctx,
		database,
		namespace,
		sqlitePostgresRichEventsTable,
	)
	if len(accounts) != 3 || len(events) != 2 {
		t.Fatalf(
			"index counts = accounts %d events %d, want 3 and 2",
			len(accounts),
			len(events),
		)
	}
	assertSQLitePostgresRichIndex(
		t,
		accounts,
		"accounts_pkey",
		true,
		true,
		"(id)",
	)
	assertSQLitePostgresRichIndex(
		t,
		accounts,
		"accounts_external_id_uq",
		true,
		false,
		"(external_id",
		"collate",
		`"c"`,
		"nulls first",
	)
	assertSQLitePostgresRichIndex(
		t,
		accounts,
		"accounts_status_balance_idx",
		false,
		false,
		"(status",
		"collate",
		`"c"`,
		"nulls first",
		"balance",
		"desc",
		"nulls last",
	)
	assertSQLitePostgresRichIndex(
		t,
		events,
		"account_events_pkey",
		true,
		true,
		"(sequence_no",
		"account_id",
	)
	assertSQLitePostgresRichIndex(
		t,
		events,
		"dmtx_account_events_account_id_note_key",
		true,
		false,
		"(account_id",
		"nulls first",
		"note",
		"collate",
		`"c"`,
		"nulls first",
	)
}

func readSQLitePostgresRichIndexes(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) map[string]sqlitePostgresRichIndex {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT
			index_relation.oid::bigint,
			index_relation.relname,
			index_metadata.indisunique,
			index_metadata.indisprimary,
			pg_catalog.pg_get_indexdef(index_relation.oid)
		FROM pg_catalog.pg_index AS index_metadata
		JOIN pg_catalog.pg_class AS table_relation
		  ON table_relation.oid = index_metadata.indrelid
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		JOIN pg_catalog.pg_class AS index_relation
		  ON index_relation.oid = index_metadata.indexrelid
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname = $2
		ORDER BY index_relation.relname
	`, namespace, table)
	if err != nil {
		t.Fatalf("query PostgreSQL indexes for %s: %v", table, err)
	}
	defer rows.Close()
	result := make(map[string]sqlitePostgresRichIndex)
	for rows.Next() {
		var index sqlitePostgresRichIndex
		if err := rows.Scan(
			&index.objectID,
			&index.name,
			&index.unique,
			&index.primary,
			&index.definition,
		); err != nil {
			t.Fatalf("scan PostgreSQL index for %s: %v", table, err)
		}
		result[index.name] = index
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL indexes for %s: %v", table, err)
	}
	return result
}

func assertSQLitePostgresRichIndex(
	t *testing.T,
	indexes map[string]sqlitePostgresRichIndex,
	name string,
	unique bool,
	primary bool,
	tokens ...string,
) {
	t.Helper()
	index, ok := indexes[name]
	if !ok {
		t.Fatalf("PostgreSQL index %s is missing: %#v", name, indexes)
	}
	if index.objectID <= 0 ||
		index.unique != unique ||
		index.primary != primary {
		t.Fatalf("PostgreSQL index %s metadata = %#v", name, index)
	}
	assertSQLitePostgresRichTokensInOrder(
		t,
		index.definition,
		tokens...,
	)
}

func assertSQLitePostgresRichTokensInOrder(
	t *testing.T,
	value string,
	tokens ...string,
) {
	t.Helper()
	lower := strings.ToLower(value)
	offset := 0
	for _, token := range tokens {
		position := strings.Index(
			lower[offset:],
			strings.ToLower(token),
		)
		if position < 0 {
			t.Fatalf("value %q does not contain ordered token %q", value, token)
		}
		offset += position + len(token)
	}
}

type sqlitePostgresRichConstraint struct {
	table      string
	name       string
	kind       string
	validated  bool
	onUpdate   string
	onDelete   string
	definition string
}

func assertSQLitePostgresRichConstraints(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT
			table_relation.relname,
			constraint_object.conname,
			constraint_object.contype::text,
			constraint_object.convalidated,
			CASE
				WHEN constraint_object.contype = 'f'
				THEN constraint_object.confupdtype::text
				ELSE ''
			END,
			CASE
				WHEN constraint_object.contype = 'f'
				THEN constraint_object.confdeltype::text
				ELSE ''
			END,
			pg_catalog.pg_get_constraintdef(
				constraint_object.oid,
				true
			)
		FROM pg_catalog.pg_constraint AS constraint_object
		JOIN pg_catalog.pg_class AS table_relation
		  ON table_relation.oid = constraint_object.conrelid
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname IN ('accounts', 'account_events')
		ORDER BY
			table_relation.relname,
			constraint_object.contype,
			constraint_object.conname
	`, namespace)
	if err != nil {
		t.Fatalf("query rich PostgreSQL constraints: %v", err)
	}
	defer rows.Close()
	var constraints []sqlitePostgresRichConstraint
	for rows.Next() {
		var constraint sqlitePostgresRichConstraint
		if err := rows.Scan(
			&constraint.table,
			&constraint.name,
			&constraint.kind,
			&constraint.validated,
			&constraint.onUpdate,
			&constraint.onDelete,
			&constraint.definition,
		); err != nil {
			t.Fatalf("scan rich PostgreSQL constraint: %v", err)
		}
		if !constraint.validated {
			t.Fatalf("PostgreSQL constraint is not validated: %#v", constraint)
		}
		constraints = append(constraints, constraint)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rich PostgreSQL constraints: %v", err)
	}
	if len(constraints) != 6 {
		t.Fatalf(
			"rich PostgreSQL constraints = %#v, want 6 constraints",
			constraints,
		)
	}
	var (
		accountsPrimary int
		accountsChecks  int
		eventsPrimary   int
		eventsChecks    int
		eventsForeign   int
	)
	for _, constraint := range constraints {
		lower := strings.ToLower(constraint.definition)
		switch {
		case constraint.table == sqlitePostgresRichAccountsTable &&
			constraint.kind == "p":
			accountsPrimary++
			assertSQLitePostgresRichTokensInOrder(
				t,
				lower,
				"primary key",
				"id",
			)
		case constraint.table == sqlitePostgresRichAccountsTable &&
			constraint.kind == "c":
			accountsChecks++
			if strings.Contains(lower, "balance") {
				assertSQLitePostgresRichTokensInOrder(
					t,
					lower,
					"balance",
					">=",
					"0",
				)
			} else {
				assertSQLitePostgresRichTokensInOrder(
					t,
					lower,
					"status",
					"active",
					"paused",
				)
			}
		case constraint.table == sqlitePostgresRichEventsTable &&
			constraint.kind == "p":
			eventsPrimary++
			assertSQLitePostgresRichTokensInOrder(
				t,
				lower,
				"primary key",
				"sequence_no",
				"account_id",
			)
		case constraint.table == sqlitePostgresRichEventsTable &&
			constraint.kind == "c":
			eventsChecks++
			assertSQLitePostgresRichTokensInOrder(
				t,
				lower,
				"sequence_no",
				">",
				"0",
			)
		case constraint.table == sqlitePostgresRichEventsTable &&
			constraint.kind == "f":
			eventsForeign++
			if constraint.onUpdate != "c" ||
				constraint.onDelete != "r" {
				t.Fatalf(
					"foreign-key action metadata = %#v",
					constraint,
				)
			}
			assertSQLitePostgresRichTokensInOrder(
				t,
				lower,
				"foreign key",
				"account_id",
				"references",
				"accounts",
				"id",
				"on update cascade",
				"on delete restrict",
			)
		default:
			t.Fatalf("unexpected PostgreSQL constraint: %#v", constraint)
		}
	}
	if accountsPrimary != 1 ||
		accountsChecks != 2 ||
		eventsPrimary != 1 ||
		eventsChecks != 1 ||
		eventsForeign != 1 {
		t.Fatalf(
			"constraint counts = accounts primary %d checks %d, events primary %d checks %d foreign %d",
			accountsPrimary,
			accountsChecks,
			eventsPrimary,
			eventsChecks,
			eventsForeign,
		)
	}
}

type sqlitePostgresRichIdentity struct {
	identity    string
	objectID    int64
	name        string
	persistence string
	dataType    string
	start       int64
	increment   int64
	minimum     int64
	maximum     int64
	cache       int64
	cycle       bool
	lastValue   sql.NullInt64
}

func assertSQLitePostgresRichIdentitySequence(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	wantLastValue int64,
) {
	t.Helper()
	var identity sqlitePostgresRichIdentity
	if err := database.QueryRowContext(ctx, `
		SELECT
			attribute.attidentity::text,
			sequence_relation.oid::bigint,
			sequence_relation.relname,
			sequence_relation.relpersistence::text,
			pg_catalog.format_type(sequence_metadata.seqtypid, NULL),
			sequence_metadata.seqstart,
			sequence_metadata.seqincrement,
			sequence_metadata.seqmin,
			sequence_metadata.seqmax,
			sequence_metadata.seqcache,
			sequence_metadata.seqcycle,
			sequence_view.last_value
		FROM pg_catalog.pg_class AS table_relation
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = table_relation.oid
		JOIN pg_catalog.pg_depend AS dependency
		  ON dependency.refclassid =
		     'pg_catalog.pg_class'::pg_catalog.regclass
		 AND dependency.refobjid = table_relation.oid
		 AND dependency.refobjsubid = attribute.attnum
		 AND dependency.classid =
		     'pg_catalog.pg_class'::pg_catalog.regclass
		 AND dependency.objsubid = 0
		 AND dependency.deptype = 'i'
		JOIN pg_catalog.pg_class AS sequence_relation
		  ON sequence_relation.oid = dependency.objid
		 AND sequence_relation.relkind = 'S'
		JOIN pg_catalog.pg_namespace AS sequence_namespace
		  ON sequence_namespace.oid = sequence_relation.relnamespace
		JOIN pg_catalog.pg_sequence AS sequence_metadata
		  ON sequence_metadata.seqrelid = sequence_relation.oid
		LEFT JOIN pg_catalog.pg_sequences AS sequence_view
		  ON sequence_view.schemaname = sequence_namespace.nspname
		 AND sequence_view.sequencename = sequence_relation.relname
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname = 'accounts'
		  AND attribute.attname = 'id'
		  AND attribute.attidentity = 'd'
	`, namespace).Scan(
		&identity.identity,
		&identity.objectID,
		&identity.name,
		&identity.persistence,
		&identity.dataType,
		&identity.start,
		&identity.increment,
		&identity.minimum,
		&identity.maximum,
		&identity.cache,
		&identity.cycle,
		&identity.lastValue,
	); err != nil {
		t.Fatalf("query rich PostgreSQL identity sequence: %v", err)
	}
	if identity.identity != "d" ||
		identity.objectID <= 0 ||
		identity.name == "" ||
		identity.persistence != "p" ||
		identity.dataType != "bigint" ||
		identity.start != 1 ||
		identity.increment != 1 ||
		identity.minimum != 1 ||
		identity.maximum != math.MaxInt64 ||
		identity.cache != 1 ||
		identity.cycle ||
		!identity.lastValue.Valid ||
		identity.lastValue.Int64 != wantLastValue {
		t.Fatalf(
			"rich PostgreSQL identity sequence = %#v, want last value %d",
			identity,
			wantLastValue,
		)
	}
}

func exerciseSQLitePostgresRichBehavior(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var (
		nextID    int64
		label     string
		balance   string
		status    string
		payload   string
		fixedCode string
		createdAt time.Time
	)
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" (external_id) VALUES ($1) "+
			"RETURNING id, label, balance::text, status, "+
			"encode(payload, 'hex'), rtrim(fixed_code::text), created_at",
		"default-row",
	).Scan(
		&nextID,
		&label,
		&balance,
		&status,
		&payload,
		&fixedCode,
		&createdAt,
	); err != nil {
		t.Fatalf("exercise PostgreSQL defaults and identity: %v", err)
	}
	if nextID != 51 ||
		label != "unknown" ||
		balance != "0.00" ||
		status != "active" ||
		payload != "00" ||
		fixedCode != "NONE" ||
		createdAt.Nanosecond() != 0 {
		t.Fatalf(
			"default PostgreSQL row = id %d label %q balance %q status %q payload %q fixed_code %q created_at %s",
			nextID,
			label,
			balance,
			status,
			payload,
			fixedCode,
			createdAt.Format(time.RFC3339Nano),
		)
	}

	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"duplicate external ID",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" (external_id) VALUES ('alpha')",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"negative balance CHECK",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" (external_id, balance) VALUES ('negative', -1.00)",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"status CHECK",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" (external_id, status) VALUES ('bad-status', 'disabled')",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"event CHECK",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" (account_id, sequence_no) VALUES (1, 0)",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"orphan foreign key",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" (account_id, sequence_no) VALUES (999, 1)",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"inline unique index",
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" (account_id, sequence_no, note) VALUES (1, 99, 'first')",
	)
	expectSQLitePostgresRichFailure(
		t,
		ctx,
		database,
		"ON DELETE RESTRICT",
		"DELETE FROM "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" WHERE id = 1",
	)

	if _, err := database.ExecContext(
		ctx,
		"UPDATE "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" SET id = 20 WHERE id = 2",
	); err != nil {
		t.Fatalf("exercise ON UPDATE CASCADE: %v", err)
	}
	var cascaded int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" WHERE account_id = 20",
	).Scan(&cascaded); err != nil {
		t.Fatalf("read cascaded foreign-key row: %v", err)
	}
	if cascaded != 1 {
		t.Fatalf("cascaded foreign-key rows = %d, want 1", cascaded)
	}
	if _, err := database.ExecContext(
		ctx,
		"UPDATE "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" SET id = 2 WHERE id = 20",
	); err != nil {
		t.Fatalf("restore account after ON UPDATE CASCADE: %v", err)
	}
}

func expectSQLitePostgresRichFailure(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	operation string,
	statement string,
) {
	t.Helper()
	if _, err := database.ExecContext(ctx, statement); err == nil {
		t.Fatalf("%s unexpectedly succeeded", operation)
	}
}

func snapshotSQLitePostgresRichObjects(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) map[string]int64 {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT
			'index'::text,
			table_relation.relname,
			index_relation.relname,
			index_relation.oid::bigint
		FROM pg_catalog.pg_index AS index_metadata
		JOIN pg_catalog.pg_class AS table_relation
		  ON table_relation.oid = index_metadata.indrelid
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		JOIN pg_catalog.pg_class AS index_relation
		  ON index_relation.oid = index_metadata.indexrelid
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname IN ('accounts', 'account_events')
		UNION ALL
		SELECT
			'constraint'::text,
			table_relation.relname,
			constraint_object.conname,
			constraint_object.oid::bigint
		FROM pg_catalog.pg_constraint AS constraint_object
		JOIN pg_catalog.pg_class AS table_relation
		  ON table_relation.oid = constraint_object.conrelid
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname IN ('accounts', 'account_events')
		UNION ALL
		SELECT
			'sequence'::text,
			table_relation.relname,
			sequence_relation.relname,
			sequence_relation.oid::bigint
		FROM pg_catalog.pg_class AS table_relation
		JOIN pg_catalog.pg_namespace AS table_namespace
		  ON table_namespace.oid = table_relation.relnamespace
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = table_relation.oid
		JOIN pg_catalog.pg_depend AS dependency
		  ON dependency.refclassid =
		     'pg_catalog.pg_class'::pg_catalog.regclass
		 AND dependency.refobjid = table_relation.oid
		 AND dependency.refobjsubid = attribute.attnum
		 AND dependency.classid =
		     'pg_catalog.pg_class'::pg_catalog.regclass
		 AND dependency.objsubid = 0
		 AND dependency.deptype = 'i'
		JOIN pg_catalog.pg_class AS sequence_relation
		  ON sequence_relation.oid = dependency.objid
		 AND sequence_relation.relkind = 'S'
		WHERE table_namespace.nspname = $1
		  AND table_relation.relname IN ('accounts', 'account_events')
		ORDER BY 1, 2, 3
	`, namespace)
	if err != nil {
		t.Fatalf("query PostgreSQL object snapshot: %v", err)
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var kind, table, name string
		var objectID int64
		if err := rows.Scan(&kind, &table, &name, &objectID); err != nil {
			t.Fatalf("scan PostgreSQL object snapshot: %v", err)
		}
		key := kind + ":" + table + ":" + name
		if _, exists := result[key]; exists {
			t.Fatalf("duplicate PostgreSQL object snapshot key %q", key)
		}
		result[key] = objectID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL object snapshot: %v", err)
	}
	if len(result) != 12 {
		t.Fatalf("PostgreSQL object snapshot = %#v, want 12 objects", result)
	}
	return result
}

func assertSQLitePostgresRichUpsertRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var label string
	if err := database.QueryRowContext(
		ctx,
		"SELECT label FROM "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" WHERE id = 2",
	).Scan(&label); err != nil {
		t.Fatalf("read source-updated account: %v", err)
	}
	if label != "source-updated" {
		t.Fatalf("source-updated label = %q", label)
	}
	var note string
	if err := database.QueryRowContext(
		ctx,
		"SELECT note FROM "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" WHERE account_id = 1 AND sequence_no = 2",
	).Scan(&note); err != nil {
		t.Fatalf("read source-updated event: %v", err)
	}
	if note != "source-event-updated" {
		t.Fatalf("source-updated event note = %q", note)
	}
	var (
		newParentLabel string
		newParentCode  string
		newChildNote   string
		newChildRaw    string
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT account.label, rtrim(account.fixed_code::text), "+
			"event.note, encode(event.raw, 'hex') "+
			"FROM "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" AS account JOIN "+
			postgresQualified(namespace, sqlitePostgresRichEventsTable)+
			" AS event ON event.account_id = account.id "+
			"WHERE account.id = 3 AND event.sequence_no = 1",
	).Scan(
		&newParentLabel,
		&newParentCode,
		&newChildNote,
		&newChildRaw,
	); err != nil {
		t.Fatalf("read new parent-child upsert rows: %v", err)
	}
	if newParentLabel != "new-parent" ||
		newParentCode != "N3" ||
		newChildNote != "source-new-child" ||
		newChildRaw != "cafe" {
		t.Fatalf(
			"new parent-child upsert rows = (%q, %q, %q, %q)",
			newParentLabel,
			newParentCode,
			newChildNote,
			newChildRaw,
		)
	}
	var retained int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" WHERE id IN (51, 200)",
	).Scan(&retained); err != nil {
		t.Fatalf("count target-only accounts after upsert: %v", err)
	}
	if retained != 2 {
		t.Fatalf("target-only accounts retained after upsert = %d, want 2", retained)
	}
}

func assertSQLitePostgresRichNextIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	want int64,
) {
	t.Helper()
	var next int64
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, sqlitePostgresRichAccountsTable)+
			" (external_id) VALUES ($1) RETURNING id",
		"after-upsert-default",
	).Scan(&next); err != nil {
		t.Fatalf("insert account after identity reseed: %v", err)
	}
	if next != want {
		t.Fatalf("identity after upsert = %d, want %d", next, want)
	}
}
