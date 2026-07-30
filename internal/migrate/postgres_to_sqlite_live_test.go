package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresToSQLiteCommonFixtureLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the " +
				"PostgreSQL-to-SQLite common fixture",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL-to-SQLite DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	sourceDatabase, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL-to-SQLite source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL-to-SQLite source: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL-to-SQLite source: %T", err)
	}

	namespace := fmt.Sprintf(
		"dmtx_pg_sqlite_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL-to-SQLite source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL-to-SQLite source schema: %v",
				err,
			)
		}
	})

	prefix := "dmtx_ps_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	createPostgresSQLiteCommonFixture(
		t,
		ctx,
		sourceDatabase,
		namespace,
		prefix,
	)
	insertPostgresSQLiteCommonFixture(
		t,
		ctx,
		sourceDatabase,
		namespace,
	)

	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedPostgresSQLitePopulatedTarget(t, ctx, targetPath)
	sourceEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: config.Endpoint{
			Type:     "sqlite",
			Database: targetPath,
		},
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{"account_events", "accounts"},
		},
	}

	result, err := PostgresToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"unacknowledged PostgreSQL-to-SQLite rebuild result = %+v, "+
				"error = %v, want %v",
			result,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	assertPostgresSQLiteTargetSentinels(t, ctx, targetPath)

	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = PostgresToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("migrate PostgreSQL-to-SQLite common fixture: %v", err)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"PostgreSQL-to-SQLite result = %+v, "+
				"want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetDatabase, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatalf("open PostgreSQL-to-SQLite target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL-to-SQLite target: %v", err)
		}
	})
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL-to-SQLite target: %v", err)
	}
	assertPostgresSQLiteCommonMetadata(
		t,
		ctx,
		targetDatabase,
		prefix,
	)
	assertPostgresSQLiteCommonRows(t, ctx, targetDatabase)
	assertPostgresSQLiteDefaultsAndIdentity(t, ctx, targetDatabase)

	if _, err := targetDatabase.ExecContext(
		ctx,
		`INSERT INTO "accounts"
		    ("id", "code", "exact_count", "enabled", "description")
		 VALUES (99, 'target-only', 77, 1, 'retained')`,
	); err != nil {
		t.Fatalf("insert retained PostgreSQL-to-SQLite target row: %v", err)
	}
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"UPDATE "+postgresQualified(namespace, "accounts")+
			` SET "exact_count" = 23 WHERE "id" = 7`,
	); err != nil {
		t.Fatalf("update PostgreSQL-to-SQLite upsert source row: %v", err)
	}
	migrationConfig.Migration.TargetMode = "upsert"
	migrationConfig.Migration.DestructiveAcknowledged = false
	result, err = PostgresToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert PostgreSQL common fixture into SQLite: %v",
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"PostgreSQL-to-SQLite retained result = %+v, "+
				"want 2 tables, 4 rows, validated",
			result,
		)
	}
	assertPostgresSQLiteRetainedUpsert(t, ctx, targetDatabase)

	createPostgresSQLiteUnsupportedLaterTable(
		t,
		ctx,
		sourceDatabase,
		namespace,
	)
	migrationConfig.Migration = config.Migration{
		TargetMode:              "drop_recreate",
		IncludeTables:           []string{"account_events", "accounts", "zz_unsupported"},
		DestructiveAcknowledged: true,
	}
	result, err = PostgresToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "timestamptz") {
		t.Fatalf(
			"unsupported later PostgreSQL table result = %+v, error = %v",
			result,
			err,
		)
	}
	assertPostgresSQLiteRetainedUpsert(t, ctx, targetDatabase)
}

func createPostgresSQLiteCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	prefix string,
) {
	t.Helper()
	accounts := postgresQualified(namespace, "accounts")
	if _, err := database.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			"id" bigint GENERATED BY DEFAULT AS IDENTITY,
			"code" varchar(24) NOT NULL DEFAULT 'guest',
			"exact_count" numeric(18,0) NOT NULL DEFAULT 0,
			"enabled" boolean NOT NULL DEFAULT TRUE,
			"description" varchar(80),
			CONSTRAINT %s PRIMARY KEY ("id"),
			CONSTRAINT %s CHECK ("exact_count" >= 0)
		)`,
		accounts,
		postgresIdentifier(prefix+"_accounts_pk"),
		postgresIdentifier(prefix+"_accounts_ck"),
	)); err != nil {
		t.Fatalf("create PostgreSQL-to-SQLite accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE UNIQUE INDEX "+
			postgresIdentifier(prefix+"_code_uq")+" ON "+accounts+
			` ("code" COLLATE "C" ASC NULLS FIRST)`,
	); err != nil {
		t.Fatalf("create PostgreSQL-to-SQLite account index: %v", err)
	}

	events := postgresQualified(namespace, "account_events")
	if _, err := database.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			"tenant_id" integer NOT NULL,
			"event_id" bigint NOT NULL,
			"account_id" bigint NOT NULL,
			"note" varchar(80) NOT NULL DEFAULT 'created',
			"exact_count" numeric(18,0) NOT NULL DEFAULT 0,
			CONSTRAINT %s PRIMARY KEY ("tenant_id", "event_id"),
			CONSTRAINT %s FOREIGN KEY ("account_id")
				REFERENCES %s ("id")
				ON UPDATE CASCADE
				ON DELETE RESTRICT,
			CONSTRAINT %s CHECK ("event_id" > 0)
		)`,
		events,
		postgresIdentifier(prefix+"_events_pk"),
		postgresIdentifier(prefix+"_account_fk"),
		accounts,
		postgresIdentifier(prefix+"_events_ck"),
	)); err != nil {
		t.Fatalf("create PostgreSQL-to-SQLite account events: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE INDEX "+postgresIdentifier(prefix+"_account_idx")+
			" ON "+events+` ("account_id" DESC NULLS LAST)`,
	); err != nil {
		t.Fatalf("create PostgreSQL-to-SQLite event index: %v", err)
	}
}

func insertPostgresSQLiteCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, "accounts")+
			` ("id", "code", "exact_count", "enabled", "description")
			 VALUES
			 (7, '東京', 9007199254740993, true,
			  'Zażółć gęślą jaźń — 東京'),
			 (11, 'emoji 😀', 0, false, NULL)`,
	); err != nil {
		t.Fatalf("insert PostgreSQL-to-SQLite accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, "account_events")+
			` ("tenant_id", "event_id", "account_id", "note", "exact_count")
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京', 9007199254740995),
			 (1, 9007199254740995, 11, 'emoji 😀', 0)`,
	); err != nil {
		t.Fatalf("insert PostgreSQL-to-SQLite account events: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		`SELECT pg_catalog.setval(
			pg_catalog.pg_get_serial_sequence($1, $2),
			41,
			true
		)`,
		namespace+".accounts",
		"id",
	); err != nil {
		t.Fatalf("set PostgreSQL-to-SQLite identity frontier: %v", err)
	}
}

func seedPostgresSQLitePopulatedTarget(
	t *testing.T,
	ctx context.Context,
	path string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open populated SQLite target: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE "accounts" (
			"id" INTEGER PRIMARY KEY,
			"marker" TEXT NOT NULL
		);
		INSERT INTO "accounts" VALUES (1, 'keep-account');
		CREATE TABLE "account_events" (
			"id" INTEGER PRIMARY KEY,
			"marker" TEXT NOT NULL
		);
		INSERT INTO "account_events" VALUES (1, 'keep-event');
	`); err != nil {
		t.Fatalf("seed populated SQLite target: %v", err)
	}
}

func assertPostgresSQLiteTargetSentinels(
	t *testing.T,
	ctx context.Context,
	path string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rejected SQLite target: %v", err)
	}
	defer database.Close()
	for table, want := range map[string]string{
		"accounts":       "keep-account",
		"account_events": "keep-event",
	} {
		var marker string
		if err := database.QueryRowContext(
			ctx,
			"SELECT \"marker\" FROM "+quote(table)+" WHERE \"id\" = 1",
		).Scan(&marker); err != nil {
			t.Fatalf("read preserved SQLite %s sentinel: %v", table, err)
		}
		if marker != want {
			t.Fatalf(
				"preserved SQLite %s sentinel = %q, want %q",
				table,
				marker,
				want,
			)
		}
	}
}

func assertPostgresSQLiteCommonMetadata(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	prefix string,
) {
	t.Helper()
	accounts, _, err := inspectSQLiteSchema(ctx, database, "accounts")
	if err != nil {
		t.Fatalf("inspect PostgreSQL-to-SQLite accounts: %v", err)
	}
	if accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 {
		t.Fatalf("SQLite accounts identity = %#v", accounts.Identity)
	}
	assertPostgresSQLiteDeclaredType(t, accounts, "id", "integer")
	assertPostgresSQLiteDeclaredType(t, accounts, "code", "varchar", 24)
	assertPostgresSQLiteDeclaredType(t, accounts, "exact_count", "bigint")
	assertPostgresSQLiteDeclaredType(t, accounts, "enabled", "boolean")
	assertPostgresSQLiteDeclaredType(
		t,
		accounts,
		"description",
		"varchar",
		80,
	)
	assertPostgresSQLiteDefault(t, accounts, "code", "'guest'")
	assertPostgresSQLiteDefault(t, accounts, "exact_count", "0")
	assertPostgresSQLiteDefault(t, accounts, "enabled", "TRUE")
	if len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != prefix+"_code_uq" ||
		!accounts.Indexes[0].Unique ||
		len(accounts.Indexes[0].Columns) != 1 ||
		accounts.Indexes[0].Columns[0].Name != "code" ||
		accounts.Indexes[0].Columns[0].Collation != "BINARY" {
		t.Fatalf("SQLite accounts indexes = %#v", accounts.Indexes)
	}
	assertPostgresSQLiteCheck(t, accounts, "exact_count", ">=")

	events, _, err := inspectSQLiteSchema(
		ctx,
		database,
		"account_events",
	)
	if err != nil {
		t.Fatalf("inspect PostgreSQL-to-SQLite events: %v", err)
	}
	if len(events.Columns) != 5 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("SQLite events columns = %#v", events.Columns)
	}
	assertPostgresSQLiteDeclaredType(t, events, "tenant_id", "integer")
	assertPostgresSQLiteDeclaredType(t, events, "event_id", "bigint")
	assertPostgresSQLiteDeclaredType(t, events, "account_id", "bigint")
	assertPostgresSQLiteDeclaredType(t, events, "note", "varchar", 80)
	assertPostgresSQLiteDeclaredType(t, events, "exact_count", "bigint")
	assertPostgresSQLiteDefault(t, events, "note", "'created'")
	assertPostgresSQLiteDefault(t, events, "exact_count", "0")
	if len(events.Indexes) != 1 ||
		events.Indexes[0].Name != prefix+"_account_idx" ||
		events.Indexes[0].Unique ||
		len(events.Indexes[0].Columns) != 1 ||
		events.Indexes[0].Columns[0].Name != "account_id" ||
		!events.Indexes[0].Columns[0].Descending {
		t.Fatalf("SQLite event indexes = %#v", events.Indexes)
	}
	if len(events.ForeignKeys) != 1 {
		t.Fatalf("SQLite event foreign keys = %#v", events.ForeignKeys)
	}
	foreignKey := events.ForeignKeys[0]
	if len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		foreignKey.ReferencedTable != "accounts" ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "NONE" {
		t.Fatalf("SQLite event foreign key = %#v", foreignKey)
	}
	assertPostgresSQLiteCheck(t, events, "event_id", ">")
}

func assertPostgresSQLiteDeclaredType(
	t *testing.T,
	table schema.Table,
	name string,
	base string,
	arguments ...int,
) {
	t.Helper()
	column := postgresSQLiteColumn(t, table, name)
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		!equalPostgresSQLiteInts(column.DeclaredType.Arguments, arguments) {
		t.Fatalf(
			"SQLite %s.%s declaration = %#v, want %s%v",
			table.Name,
			name,
			column.DeclaredType,
			base,
			arguments,
		)
	}
}

func assertPostgresSQLiteDefault(
	t *testing.T,
	table schema.Table,
	name string,
	want string,
) {
	t.Helper()
	column := postgresSQLiteColumn(t, table, name)
	if column.Default == nil || column.Default.CanonicalSQL() != want {
		t.Fatalf(
			"SQLite %s.%s default = %#v, want %q",
			table.Name,
			name,
			column.Default,
			want,
		)
	}
}

func assertPostgresSQLiteCheck(
	t *testing.T,
	table schema.Table,
	parts ...string,
) {
	t.Helper()
	for _, check := range table.Checks {
		expression := check.Expression.CanonicalSQL()
		matches := true
		for _, part := range parts {
			if !strings.Contains(expression, part) {
				matches = false
				break
			}
		}
		if matches {
			return
		}
	}
	t.Fatalf(
		"SQLite %s checks = %#v, want expression containing %q",
		table.Name,
		table.Checks,
		parts,
	)
}

func postgresSQLiteColumn(
	t *testing.T,
	table schema.Table,
	name string,
) schema.Column {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			return column
		}
	}
	t.Fatalf("SQLite table %s has no column %s", table.Name, name)
	return schema.Column{}
}

func equalPostgresSQLiteInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func assertPostgresSQLiteCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()
	var (
		code        string
		exactCount  string
		enabled     int64
		description string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", CAST("exact_count" AS TEXT), "enabled",
		        "description"
		   FROM "accounts" WHERE "id" = 7`,
	).Scan(&code, &exactCount, &enabled, &description); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite account: %v", err)
	}
	if code != "東京" ||
		exactCount != "9007199254740993" ||
		enabled != 1 ||
		description != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf(
			"SQLite account = (%q, %q, %d, %q)",
			code,
			exactCount,
			enabled,
			description,
		)
	}
	var nullableDescription bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT "description" IS NULL FROM "accounts" WHERE "id" = 11`,
	).Scan(&nullableDescription); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite NULL: %v", err)
	}
	if !nullableDescription {
		t.Fatal("PostgreSQL-to-SQLite NULL was not preserved")
	}
	var note, eventCount string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "note", CAST("exact_count" AS TEXT)
		   FROM "account_events"
		  WHERE "tenant_id" = 1 AND "event_id" = 9007199254740993`,
	).Scan(&note, &eventCount); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite event: %v", err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		eventCount != "9007199254740995" {
		t.Fatalf("SQLite event = (%q, %q)", note, eventCount)
	}
}

func assertPostgresSQLiteDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()
	var sequence int64
	if err := database.QueryRowContext(
		ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = 'accounts'`,
	).Scan(&sequence); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite identity frontier: %v", err)
	}
	if sequence != 41 {
		t.Fatalf("SQLite identity frontier = %d, want 41", sequence)
	}
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin SQLite defaults exercise: %v", err)
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(
		ctx,
		`INSERT INTO "accounts" ("description") VALUES ('defaults')`,
	)
	if err != nil {
		t.Fatalf("exercise SQLite defaults and identity: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read SQLite generated identity: %v", err)
	}
	var code, exactCount string
	var enabled int64
	if err := transaction.QueryRowContext(
		ctx,
		`SELECT "code", CAST("exact_count" AS TEXT), "enabled"
		   FROM "accounts" WHERE "id" = ?`,
		id,
	).Scan(&code, &exactCount, &enabled); err != nil {
		t.Fatalf("read SQLite defaults row: %v", err)
	}
	if id != 42 || code != "guest" ||
		exactCount != "0" || enabled != 1 {
		t.Fatalf(
			"SQLite defaults row = (%d, %q, %q, %d)",
			id,
			code,
			exactCount,
			enabled,
		)
	}
}

func assertPostgresSQLiteRetainedUpsert(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()
	var sourceCount, retainedCount string
	if err := database.QueryRowContext(
		ctx,
		`SELECT CAST("exact_count" AS TEXT)
		   FROM "accounts" WHERE "id" = 7`,
	).Scan(&sourceCount); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite upserted row: %v", err)
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT CAST("exact_count" AS TEXT)
		   FROM "accounts"
		  WHERE "id" = 99 AND "code" = 'target-only'
		    AND "description" = 'retained'`,
	).Scan(&retainedCount); err != nil {
		t.Fatalf("read PostgreSQL-to-SQLite retained row: %v", err)
	}
	if sourceCount != "23" || retainedCount != "77" {
		t.Fatalf(
			"SQLite upsert counts = (%q, %q), want (23, 77)",
			sourceCount,
			retainedCount,
		)
	}
	var accounts, events int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM "accounts"`,
	).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM "account_events"`,
	).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if accounts != 3 || events != 2 {
		t.Fatalf(
			"SQLite retained row counts = (%d, %d), want (3, 2)",
			accounts,
			events,
		)
	}
}

func createPostgresSQLiteUnsupportedLaterTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(namespace, "zz_unsupported")+
			` (
				"id" bigint PRIMARY KEY,
				"observed_at" timestamptz(6) NOT NULL
			);
			INSERT INTO `+
			postgresQualified(namespace, "zz_unsupported")+
			` VALUES (1, '2026-07-30 12:34:56.123456+00')`,
	); err != nil {
		t.Fatalf(
			"create unsupported later PostgreSQL-to-SQLite table: %v",
			err,
		)
	}
}
