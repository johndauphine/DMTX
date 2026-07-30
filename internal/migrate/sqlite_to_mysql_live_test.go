package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteToMySQLCommonFixtureLive(t *testing.T) {
	testSQLiteToMySQLFamilyCommonFixtureLive(t, mysqlNativeLiveFixture{
		name:        "MySQL",
		targetEnv:   "DMTX_TEST_MYSQL_TARGET_DSN",
		caEnv:       "DMTX_TEST_MYSQL_CA",
		tlsConfig:   "dmtx_test",
		namePrefix:  "dmtx_sm_",
		collation:   "utf8mb4_0900_bin",
		refreshInfo: true,
	})
}

func TestSQLiteToMariaDBCommonFixtureLive(t *testing.T) {
	testSQLiteToMySQLFamilyCommonFixtureLive(t, mysqlNativeLiveFixture{
		name:       "MariaDB",
		targetEnv:  "DMTX_TEST_MARIADB_TARGET_DSN",
		caEnv:      "DMTX_TEST_MARIADB_CA",
		tlsConfig:  "dmtx_mariadb_test",
		namePrefix: "dmtx_smaria_",
		collation:  "utf8mb4_nopad_bin",
	})
}

func testSQLiteToMySQLFamilyCommonFixtureLive(
	t *testing.T,
	fixture mysqlNativeLiveFixture,
) {
	t.Helper()
	targetDSN := os.Getenv(fixture.targetEnv)
	caPath := os.Getenv(fixture.caEnv)
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set " + fixture.targetEnv + " and " + fixture.caEnv +
				" to run the SQLite-to-" + fixture.name +
				" common fixture",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		caPath,
		fixture.tlsConfig,
	)
	targetConfig := parseMySQLNativeTargetDSNForTLS(
		t,
		"target",
		targetDSN,
		fixture.tlsConfig,
	)
	targetEndpoint := mysqlNativeTargetEndpoint(
		t,
		targetConfig,
		caPath,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		180*time.Second,
	)
	defer cancel()
	targetDatabase := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		"SQLite common-fixture target",
		targetDSN,
		fixture.refreshInfo,
	)

	prefix := fixture.namePrefix +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	invalidName := prefix + "_zz_invalid"
	cleanupMySQLNativeTables(
		t,
		targetDatabase,
		invalidName,
		eventsName,
		accountsName,
	)
	sourcePath := createSQLiteMySQLCommonFixture(
		t,
		ctx,
		prefix,
		accountsName,
		eventsName,
	)
	seedMySQLNativeReplacementTargets(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	migrationConfig := config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := SQLiteToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"unacknowledged SQLite-to-%s result = %+v, error = %v, want %v",
			fixture.name,
			result,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	assertSQLiteMySQLReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	for _, name := range []string{accountsName, eventsName} {
		if _, err := targetDatabase.ExecContext(
			ctx,
			"DELETE FROM "+mySQLIdentifier(name),
		); err != nil {
			t.Fatalf("empty %s target %s: %v", fixture.name, name, err)
		}
	}
	raceObserver := &sqliteMySQLDestructiveRaceObserver{
		database: targetDatabase,
		table:    accountsName,
	}
	result, err = SQLiteToMySQLWithObserver(
		ctx,
		migrationConfig,
		raceObserver,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"SQLite-to-%s acknowledgement-race result = %+v, error = %v",
			fixture.name,
			result,
			err,
		)
	}
	if raceObserver.beforeSets != 1 ||
		raceObserver.beforeTables != 0 ||
		raceObserver.afterTables != 0 {
		t.Fatalf(
			"SQLite-to-%s acknowledgement-race observer = %+v",
			fixture.name,
			raceObserver,
		)
	}
	assertSQLiteMySQLReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	assertSQLiteMySQLEmptyReplacementTarget(
		t,
		ctx,
		targetDatabase,
		eventsName,
	)

	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = SQLiteToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate SQLite common fixture into %s: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQLite-to-%s result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	assertSQLiteMySQLCommonMetadata(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		accountsName,
		eventsName,
		fixture.collation,
		41,
	)
	assertSQLiteMySQLCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		2,
		"9007199254740993",
	)
	assertSQLiteMySQLDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)

	insertSQLiteMySQLTargetOnlyAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
		99,
	)
	updateSQLiteMySQLCommonFixture(
		t,
		ctx,
		sourcePath,
		accountsName,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	migrationConfig.Migration.DestructiveAcknowledged = false
	result, err = SQLiteToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert SQLite common fixture into %s: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQLite-to-%s retained result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	assertSQLiteMySQLCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		4,
		"23",
	)
	assertSQLiteMySQLRetainedAccounts(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	assertSQLiteMySQLCommonMetadata(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		accountsName,
		eventsName,
		fixture.collation,
		99,
	)

	createSQLiteMySQLInvalidTemporalTable(
		t,
		ctx,
		sourcePath,
		invalidName,
	)
	migrationConfig.Migration.TargetMode = "drop_recreate"
	migrationConfig.Migration.DestructiveAcknowledged = true
	migrationConfig.Migration.IncludeTables = []string{
		accountsName,
		eventsName,
		invalidName,
	}
	before := sqliteMySQLAccountSnapshot(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	result, err = SQLiteToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "non-TEXT storage class") {
		t.Fatalf(
			"invalid SQLite temporal result = %+v, error = %v",
			result,
			err,
		)
	}
	after := sqliteMySQLAccountSnapshot(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf(
			"rejected SQLite-to-%s source mutated target: before=%#v after=%#v",
			fixture.name,
			before,
			after,
		)
	}
}

func createSQLiteMySQLCommonFixture(
	t *testing.T,
	ctx context.Context,
	prefix string,
	accountsName string,
	eventsName string,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sqlite-to-mysql.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if _, err := database.ExecContext(ctx, fmt.Sprintf(`
		PRAGMA foreign_keys = ON;
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			code VARCHAR(24) NOT NULL UNIQUE DEFAULT 'guest',
			exact_count DECIMAL(18,0) NOT NULL DEFAULT 0,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			description VARCHAR(80),
			created_on DATE NOT NULL DEFAULT '2026-07-30',
			occurred_at DATETIME(6) NOT NULL
				DEFAULT '2026-07-30 12:34:56.123456',
			external_id UUID NOT NULL
				DEFAULT '123e4567-e89b-12d3-a456-426614174000',
			CHECK (exact_count >= 0)
		);
		CREATE INDEX %s ON %s (exact_count DESC);
		CREATE TABLE %s (
			tenant_id INTEGER NOT NULL,
			event_id INTEGER NOT NULL,
			account_id INTEGER NOT NULL,
			note VARCHAR(80) NOT NULL DEFAULT 'created',
			exact_count DECIMAL(18,0) NOT NULL DEFAULT 0,
			payload BLOB,
			occurred_at DATETIME(0) NOT NULL
				DEFAULT '2026-07-30 12:34:56',
			PRIMARY KEY (tenant_id, event_id),
			FOREIGN KEY (account_id)
				REFERENCES %s
				ON UPDATE CASCADE
				ON DELETE NO ACTION,
			CHECK (event_id > 0)
		);
		CREATE INDEX %s ON %s (account_id);
	`,
		quote(accountsName),
		quote(prefix+"_exact_idx"),
		quote(accountsName),
		quote(eventsName),
		quote(accountsName),
		quote(prefix+"_account_idx"),
		quote(eventsName),
	)); err != nil {
		t.Fatalf("create SQLite-to-MySQL source schema: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(accountsName)+
			` (id, code, exact_count, enabled, description,
			   created_on, occurred_at, external_id)
			 VALUES
			 (7, '東京', 9007199254740993, 1,
			  'Zażółć gęślą jaźń — 東京',
			  '2026-07-30', '2026-07-30 12:34:56.123456',
			  '123e4567-e89b-12d3-a456-426614174007'),
			 (11, 'emoji 😀', 0, 0, NULL,
			  '2026-07-31', '2026-07-31 00:00:00.000000',
			  '123e4567-e89b-12d3-a456-426614174011'),
			 (41, 'deleted-frontier', 1, 1, 'deleted',
			  '2026-08-01', '2026-08-01 00:00:00.000000',
			  '123e4567-e89b-12d3-a456-426614174041')`,
	); err != nil {
		t.Fatalf("insert SQLite-to-MySQL accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"DELETE FROM "+quote(accountsName)+" WHERE id = 41",
	); err != nil {
		t.Fatalf("set SQLite-to-MySQL identity frontier: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(eventsName)+
			` (tenant_id, event_id, account_id, note, exact_count,
			   payload, occurred_at)
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京', 9007199254740995,
			  X'deadbeef', '2026-07-30 12:34:56'),
			 (1, 9007199254740995, 11, 'emoji 😀', 0,
			  NULL, '2026-07-31 00:00:00')`,
	); err != nil {
		t.Fatalf("insert SQLite-to-MySQL events: %v", err)
	}
	return path
}

func updateSQLiteMySQLCommonFixture(
	t *testing.T,
	ctx context.Context,
	path string,
	accountsName string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"UPDATE "+quote(accountsName)+
			" SET exact_count = 23 WHERE id = 7",
	); err != nil {
		t.Fatalf("update SQLite-to-MySQL source: %v", err)
	}
}

func createSQLiteMySQLInvalidTemporalTable(
	t *testing.T,
	ctx context.Context,
	path string,
	name string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+quote(name)+
			" (id BIGINT NOT NULL PRIMARY KEY, happened DATETIME(6) NOT NULL)",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(name)+
			" (id, happened) VALUES (1, X'323032362D30372D3330')",
	); err != nil {
		t.Fatal(err)
	}
}

func assertSQLiteMySQLCommonMetadata(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
	eventsName string,
	collation string,
	frontier int64,
) {
	t.Helper()
	accounts, err := engine.InspectMySQLTable(
		ctx,
		database,
		namespace,
		accountsName,
	)
	if err != nil {
		t.Fatal(err)
	}
	events, err := engine.InspectMySQLTable(
		ctx,
		database,
		namespace,
		eventsName,
	)
	if err != nil {
		t.Fatal(err)
	}
	if accounts.MySQLCollation != collation ||
		events.MySQLCollation != collation {
		t.Fatalf(
			"MySQL-family collations = %q/%q, want %q",
			accounts.MySQLCollation,
			events.MySQLCollation,
			collation,
		)
	}
	if accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != frontier ||
		accounts.Identity.Column != "id" {
		t.Fatalf("SQLite-to-MySQL identity = %#v", accounts.Identity)
	}
	assertSQLiteMySQLColumn(
		t,
		accounts,
		"id",
		"bigint",
		"bigint",
	)
	assertSQLiteMySQLColumn(
		t,
		accounts,
		"exact_count",
		"numeric",
		"decimal",
		18,
		0,
	)
	assertSQLiteMySQLColumn(
		t,
		accounts,
		"enabled",
		"integer",
		"tinyint",
		1,
	)
	assertSQLiteMySQLColumn(
		t,
		accounts,
		"occurred_at",
		"datetime",
		"datetime",
		6,
	)
	assertSQLiteMySQLColumn(
		t,
		accounts,
		"external_id",
		"varchar",
		"varchar",
		36,
	)
	if len(accounts.Indexes) != 2 ||
		len(accounts.Checks) != 2 {
		t.Fatalf(
			"accounts indexes/checks = %#v / %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}
	for _, index := range accounts.Indexes {
		if index.Name == "" {
			t.Fatalf("anonymous retained MySQL index: %#v", index)
		}
	}
	for _, check := range accounts.Checks {
		if check.Name == "" {
			t.Fatalf("anonymous retained MySQL CHECK: %#v", check)
		}
	}
	if len(events.ForeignKeys) != 1 {
		t.Fatalf("events foreign keys = %#v", events.ForeignKeys)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name == "" ||
		!reflect.DeepEqual(foreignKey.Columns, []string{"account_id"}) ||
		foreignKey.ReferencedTable != accountsName ||
		!reflect.DeepEqual(
			foreignKey.ReferencedColumns,
			[]string{"id"},
		) ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" {
		t.Fatalf("events foreign key = %#v", foreignKey)
	}
	if !sqliteMySQLSchemaIndexSupportsColumns(
		events,
		[]string{"account_id"},
	) {
		t.Fatalf("events lacks FK support index: %#v", events.Indexes)
	}
}

func assertSQLiteMySQLColumn(
	t *testing.T,
	table schema.Table,
	name string,
	semantic string,
	base string,
	arguments ...int,
) {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name != name {
			continue
		}
		if column.Type != semantic ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != base ||
			!reflect.DeepEqual(
				column.DeclaredType.Arguments,
				arguments,
			) {
			t.Fatalf(
				"MySQL column %s.%s = %#v, want %s %s%v",
				table.Name,
				name,
				column,
				semantic,
				base,
				arguments,
			)
		}
		return
	}
	t.Fatalf("MySQL table %s lacks column %s", table.Name, name)
}

func assertSQLiteMySQLCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
	accountCount int,
	exactCount string,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName),
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != accountCount {
		t.Fatalf("account count = %d, want %d", count, accountCount)
	}
	var exact, description, occurred, external string
	var enabled bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT CAST(exact_count AS CHAR), enabled, description, "+
			"DATE_FORMAT(occurred_at, '%Y-%m-%d %H:%i:%s.%f'), external_id "+
			"FROM "+mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(
		&exact,
		&enabled,
		&description,
		&occurred,
		&external,
	); err != nil {
		t.Fatal(err)
	}
	if exact != exactCount || !enabled ||
		description != "Zażółć gęślą jaźń — 東京" ||
		occurred != "2026-07-30 12:34:56.123456" ||
		external != "123e4567-e89b-12d3-a456-426614174007" {
		t.Fatalf(
			"account row = (%q,%t,%q,%q,%q)",
			exact,
			enabled,
			description,
			occurred,
			external,
		)
	}
	var eventCount int
	var eventExact string
	var payload []byte
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*), CAST(MAX(exact_count) AS CHAR), "+
			"MAX(payload) FROM "+mySQLIdentifier(eventsName),
	).Scan(&eventCount, &eventExact, &payload); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 ||
		eventExact != "9007199254740995" ||
		!reflect.DeepEqual(payload, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf(
			"event aggregate = (%d,%q,%x)",
			eventCount,
			eventExact,
			payload,
		)
	}
}

func assertSQLiteMySQLDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	result, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			" () VALUES ()",
	)
	if err != nil {
		t.Fatalf("insert SQLite-to-MySQL default row: %v", err)
	}
	identifier, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if identifier != 42 {
		t.Fatalf("next MySQL identity = %d, want 42", identifier)
	}
	var code, exact, created, occurred, external string
	var enabled bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT code, CAST(exact_count AS CHAR), enabled, "+
			"DATE_FORMAT(created_on, '%Y-%m-%d'), "+
			"DATE_FORMAT(occurred_at, '%Y-%m-%d %H:%i:%s.%f'), "+
			"external_id FROM "+mySQLIdentifier(accountsName)+" WHERE id = 42",
	).Scan(
		&code,
		&exact,
		&enabled,
		&created,
		&occurred,
		&external,
	); err != nil {
		t.Fatal(err)
	}
	if code != "guest" || exact != "0" || !enabled ||
		created != "2026-07-30" ||
		occurred != "2026-07-30 12:34:56.123456" ||
		external != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf(
			"default row = (%q,%q,%t,%q,%q,%q)",
			code,
			exact,
			enabled,
			created,
			occurred,
			external,
		)
	}
}

func insertSQLiteMySQLTargetOnlyAccount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	identifier int64,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			" (id, code, exact_count, enabled, description, external_id) "+
			"VALUES (?, 'target-only', 77, 1, 'retained', "+
			"'123e4567-e89b-12d3-a456-426614174099')",
		identifier,
	); err != nil {
		t.Fatalf("insert retained MySQL account: %v", err)
	}
}

func assertSQLiteMySQLRetainedAccounts(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	for _, identifier := range []int64{42, 99} {
		var count int
		if err := database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName)+
				" WHERE id = ?",
			identifier,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("retained account %d count = %d", identifier, count)
		}
	}
}

func assertSQLiteMySQLReplacementSentinels(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		var marker string
		if err := database.QueryRowContext(
			ctx,
			"SELECT stale_marker FROM "+mySQLIdentifier(name)+
				" WHERE stale_id = 99",
		).Scan(&marker); err != nil {
			t.Fatalf("read MySQL replacement sentinel %s: %v", name, err)
		}
		if marker != "must disappear" {
			t.Fatalf("MySQL replacement sentinel %s = %q", name, marker)
		}
	}
}

func assertSQLiteMySQLEmptyReplacementTarget(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(name),
	).Scan(&count); err != nil {
		t.Fatalf("read empty MySQL replacement target %s: %v", name, err)
	}
	if count != 0 {
		t.Fatalf("MySQL replacement target %s row count = %d, want 0", name, count)
	}
}

func sqliteMySQLAccountSnapshot(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) []string {
	t.Helper()
	rows, err := database.QueryContext(
		ctx,
		"SELECT CONCAT(id, ':', code, ':', CAST(exact_count AS CHAR)) "+
			"FROM "+mySQLIdentifier(accountsName)+" ORDER BY id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

type sqliteMySQLDestructiveRaceObserver struct {
	database     *sql.DB
	table        string
	beforeSets   int
	beforeTables int
	afterTables  int
}

func (observer *sqliteMySQLDestructiveRaceObserver) BeforeTables(
	ctx context.Context,
	_ []string,
) error {
	observer.beforeSets++
	_, err := observer.database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(observer.table)+
			" (stale_id, stale_marker) VALUES (99, 'must disappear')",
	)
	return err
}

func (observer *sqliteMySQLDestructiveRaceObserver) BeforeTable(
	_ context.Context,
	_ string,
) error {
	observer.beforeTables++
	return nil
}

func (observer *sqliteMySQLDestructiveRaceObserver) AfterTable(
	_ context.Context,
	_ string,
	_ int,
) error {
	observer.afterTables++
	return nil
}
