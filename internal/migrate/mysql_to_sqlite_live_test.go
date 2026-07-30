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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLToSQLiteCommonFixtureLive(t *testing.T) {
	testMySQLFamilyToSQLiteCommonFixtureLive(t, mysqlFamilyLiveFixture{
		name:       "MySQL",
		dsnEnv:     "DMTX_TEST_MYSQL_DSN",
		caEnv:      "DMTX_TEST_MYSQL_CA",
		tlsConfig:  "dmtx_test",
		namePrefix: "dmtx_mys_",
		collation:  "utf8mb4_0900_bin",
	})
}

func TestMariaDBToSQLiteCommonFixtureLive(t *testing.T) {
	testMySQLFamilyToSQLiteCommonFixtureLive(t, mysqlFamilyLiveFixture{
		name:       "MariaDB",
		dsnEnv:     "DMTX_TEST_MARIADB_DSN",
		caEnv:      "DMTX_TEST_MARIADB_CA",
		tlsConfig:  "dmtx_mariadb_test",
		namePrefix: "dmtx_mars_",
		collation:  "utf8mb4_nopad_bin",
	})
}

func testMySQLFamilyToSQLiteCommonFixtureLive(
	t *testing.T,
	fixture mysqlFamilyLiveFixture,
) {
	t.Helper()
	sourceDSN := os.Getenv(fixture.dsnEnv)
	sourceCA := os.Getenv(fixture.caEnv)
	if sourceDSN == "" || sourceCA == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				"-to-SQLite common fixture",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		sourceCA,
		fixture.tlsConfig,
	)
	sourceConfig, err := mysqlDriver.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatalf("parse %s-to-SQLite DSN: %T", fixture.name, err)
	}
	if sourceConfig.TLSConfig != fixture.tlsConfig &&
		sourceConfig.TLSConfig != "true" {
		t.Fatalf("%s must require verified TLS", fixture.dsnEnv)
	}
	if sourceConfig.DBName == "" {
		t.Fatalf("%s must select a database", fixture.dsnEnv)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		180*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open %s-to-SQLite source: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close %s-to-SQLite source: %v", fixture.name, err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify %s-to-SQLite source: %v", fixture.name, err)
	}
	var tlsVariable, tlsCipher string
	if err := sourceDatabase.QueryRowContext(
		ctx,
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
	).Scan(&tlsVariable, &tlsCipher); err != nil {
		t.Fatalf("inspect %s-to-SQLite TLS status: %v", fixture.name, err)
	}
	if tlsVariable != "Ssl_cipher" || tlsCipher == "" {
		t.Fatalf("%s-to-SQLite source is not using TLS", fixture.name)
	}

	prefix := fixture.namePrefix +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	unsupportedName := prefix + "_zz_unsupported"
	cleanupMySQLToSQLServerSourceTables(
		t,
		sourceDatabase,
		unsupportedName,
		eventsName,
		accountsName,
	)
	createMySQLSQLiteCommonFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
		fixture.collation,
	)
	insertMySQLSQLiteCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)

	sourceEndpoint := mySQLSQLServerSourceEndpoint(
		t,
		sourceConfig,
		sourceCA,
	)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	seedMySQLSQLiteReplacementTargets(
		t,
		ctx,
		targetPath,
		accountsName,
		eventsName,
	)
	targetDatabase, err := sql.Open("sqlite", sqliteTargetURI(targetPath))
	if err != nil {
		t.Fatalf("open %s-to-SQLite target: %v", fixture.name, err)
	}
	targetDatabase.SetMaxOpenConns(1)
	targetDatabase.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := targetDatabase.Close(); err != nil {
			t.Errorf("close %s-to-SQLite target: %v", fixture.name, err)
		}
	})
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify %s-to-SQLite target: %v", fixture.name, err)
	}
	if err := requireSQLiteForeignKeys(ctx, targetDatabase); err != nil {
		t.Fatalf(
			"verify %s-to-SQLite foreign-key enforcement: %v",
			fixture.name,
			err,
		)
	}

	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: config.Endpoint{
			Type:     "sqlite",
			Database: targetPath,
		},
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := MySQLToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"%s populated SQLite rebuild result = %+v, error = %v, want %v",
			fixture.name,
			result,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	assertMySQLSQLiteReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		map[string]string{
			accountsName: "keep-account",
			eventsName:   "keep-event",
		},
	)

	for _, name := range []string{accountsName, eventsName} {
		if _, err := targetDatabase.ExecContext(
			ctx,
			"DELETE FROM "+quote(name),
		); err != nil {
			t.Fatalf(
				"empty %s-to-SQLite race target %s: %v",
				fixture.name,
				name,
				err,
			)
		}
	}
	raceObserver := &mySQLSQLiteDestructiveRaceObserver{
		database: targetDatabase,
		table:    accountsName,
	}
	result, err = MySQLToSQLiteWithObserver(
		ctx,
		migrationConfig,
		raceObserver,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"%s SQLite row added after preflight result = %+v, error = %v, want %v",
			fixture.name,
			result,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	if raceObserver.beforeSets != 1 ||
		raceObserver.beforeTables != 0 ||
		raceObserver.afterTables != 0 {
		t.Fatalf(
			"%s-to-SQLite race observer = %+v",
			fixture.name,
			raceObserver,
		)
	}
	assertMySQLSQLiteReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		map[string]string{accountsName: "race-row"},
	)

	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = MySQLToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate %s common fixture into SQLite: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"%s-to-SQLite result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	assertMySQLSQLiteCommonMetadata(
		t,
		ctx,
		targetDatabase,
		prefix,
		accountsName,
		eventsName,
		41,
	)
	assertMySQLSQLiteCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		2,
		"9007199254740993",
	)
	assertMySQLSQLiteDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)

	insertMySQLSQLiteRetainedRow(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(accountsName)+
			" SET exact_count = 23 WHERE id = 7",
	); err != nil {
		t.Fatalf("update %s-to-SQLite upsert source: %v", fixture.name, err)
	}
	migrationConfig.Migration.TargetMode = "upsert"
	migrationConfig.Migration.DestructiveAcknowledged = false
	result, err = MySQLToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert %s common fixture into SQLite: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"%s-to-SQLite retained result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	assertMySQLSQLiteRetainedUpsert(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLSQLiteCommonMetadata(
		t,
		ctx,
		targetDatabase,
		prefix,
		accountsName,
		eventsName,
		99,
	)

	createMySQLSQLiteUnsupportedLaterTable(
		t,
		ctx,
		sourceDatabase,
		unsupportedName,
		fixture.collation,
	)
	migrationConfig.Migration = config.Migration{
		TargetMode: "drop_recreate",
		IncludeTables: []string{
			accountsName,
			eventsName,
			unsupportedName,
		},
		DestructiveAcknowledged: true,
	}
	result, err = MySQLToSQLiteWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Fatalf(
			"unsupported later %s table result = %+v, error = %v",
			fixture.name,
			result,
			err,
		)
	}
	assertMySQLSQLiteRetainedUpsert(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
}

type mySQLSQLiteDestructiveRaceObserver struct {
	database     *sql.DB
	table        string
	beforeSets   int
	beforeTables int
	afterTables  int
}

func (observer *mySQLSQLiteDestructiveRaceObserver) BeforeTables(
	ctx context.Context,
	_ []string,
) error {
	observer.beforeSets++
	_, err := observer.database.ExecContext(
		ctx,
		"INSERT INTO "+quote(observer.table)+
			` ("stale_id", "stale_marker") VALUES (99, 'race-row')`,
	)
	return err
}

func (observer *mySQLSQLiteDestructiveRaceObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.beforeTables++
	return nil
}

func (observer *mySQLSQLiteDestructiveRaceObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.afterTables++
	return nil
}

func createMySQLSQLiteCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	prefix string,
	accountsName string,
	eventsName string,
	collation string,
) {
	t.Helper()
	accountsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			code VARCHAR(24) COLLATE %s NOT NULL DEFAULT 'guest',
			exact_count DECIMAL(18,0) NOT NULL DEFAULT 0,
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			payload VARBINARY(16) NULL,
			created_at DATETIME(6) NOT NULL,
			local_time TIME(6) NOT NULL,
			description VARCHAR(80) COLLATE %s NULL,
			PRIMARY KEY (id),
			UNIQUE KEY %s (code),
			KEY %s (exact_count DESC),
			CONSTRAINT %s CHECK (exact_count >= 0)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=%s
		  ROW_FORMAT=DYNAMIC`,
		mySQLIdentifier(accountsName),
		collation,
		collation,
		mySQLIdentifier(prefix+"_code_uq"),
		mySQLIdentifier(prefix+"_count_idx"),
		mySQLIdentifier(prefix+"_account_ck"),
		collation,
	)
	if _, err := database.ExecContext(ctx, accountsDDL); err != nil {
		t.Fatalf("create MySQL-family-to-SQLite accounts: %v", err)
	}
	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT NOT NULL,
			event_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			note VARCHAR(80) COLLATE %s NOT NULL DEFAULT 'created',
			exact_count DECIMAL(18,0) NOT NULL DEFAULT 0,
			occurred_at DATETIME(6) NOT NULL,
			observed_on DATE NOT NULL,
			payload BLOB NULL,
			PRIMARY KEY (tenant_id, event_id),
			KEY %s (account_id),
			CONSTRAINT %s FOREIGN KEY (account_id)
				REFERENCES %s (id)
				ON UPDATE CASCADE
				ON DELETE RESTRICT,
			CONSTRAINT %s CHECK (event_id > 0)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=%s
		  ROW_FORMAT=DYNAMIC`,
		mySQLIdentifier(eventsName),
		collation,
		mySQLIdentifier(prefix+"_account_idx"),
		mySQLIdentifier(prefix+"_account_fk"),
		mySQLIdentifier(accountsName),
		mySQLIdentifier(prefix+"_event_ck"),
		collation,
	)
	if _, err := database.ExecContext(ctx, eventsDDL); err != nil {
		t.Fatalf("create MySQL-family-to-SQLite events: %v", err)
	}
}

func insertMySQLSQLiteCommonFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			` (id, code, exact_count, enabled, payload,
			   created_at, local_time, description)
			 VALUES
			 (7, '東京', 9007199254740993, 1, UNHEX('00ff'),
			  '2026-07-29 12:34:56.123456', '12:34:56.123456',
			  'Zażółć gęślą jaźń — 東京'),
			 (11, 'emoji 😀', 0, 0, NULL,
			  '2026-07-29 23:59:59.999999', '23:59:59.999999',
			  NULL)`,
	); err != nil {
		t.Fatalf("insert MySQL-family-to-SQLite accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+mySQLIdentifier(accountsName)+
			" AUTO_INCREMENT = 42",
	); err != nil {
		t.Fatalf("set MySQL-family-to-SQLite identity frontier: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(eventsName)+
			` (tenant_id, event_id, account_id, note, exact_count,
			   occurred_at, observed_on, payload)
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京', 9007199254740995,
			  '2026-07-29 12:34:56.123456', '2026-07-29',
			  UNHEX('deadbeef')),
			 (1, 9007199254740995, 11,
			  'emoji 😀', 0,
			  '2026-07-29 23:59:59.999999', '2026-07-30', NULL)`,
	); err != nil {
		t.Fatalf("insert MySQL-family-to-SQLite events: %v", err)
	}
}

func seedMySQLSQLiteReplacementTargets(
	t *testing.T,
	ctx context.Context,
	path string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open populated MySQL-family SQLite target: %v", err)
	}
	defer database.Close()
	for name, marker := range map[string]string{
		accountsName: "keep-account",
		eventsName:   "keep-event",
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+quote(name)+` (
				"stale_id" INTEGER PRIMARY KEY,
				"stale_marker" TEXT NOT NULL
			);
			INSERT INTO `+quote(name)+
				` VALUES (1, ?);`,
			marker,
		); err != nil {
			t.Fatalf("seed populated SQLite target %s: %v", name, err)
		}
	}
}

func assertMySQLSQLiteReplacementSentinels(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	expected map[string]string,
) {
	t.Helper()
	for name, want := range expected {
		var marker string
		if err := database.QueryRowContext(
			ctx,
			"SELECT \"stale_marker\" FROM "+quote(name)+
				" ORDER BY \"stale_id\" DESC LIMIT 1",
		).Scan(&marker); err != nil {
			t.Fatalf("read preserved SQLite target %s: %v", name, err)
		}
		if marker != want {
			t.Fatalf(
				"preserved SQLite target %s marker = %q, want %q",
				name,
				marker,
				want,
			)
		}
	}
}

func assertMySQLSQLiteCommonMetadata(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	prefix string,
	accountsName string,
	eventsName string,
	identityFrontier int64,
) {
	t.Helper()
	accounts, _, err := inspectSQLiteSchema(
		ctx,
		database,
		accountsName,
	)
	if err != nil {
		t.Fatalf("inspect MySQL-family-to-SQLite accounts: %v", err)
	}
	if accounts.Schema != "" ||
		accounts.MySQLCollation != "" ||
		accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != identityFrontier {
		t.Fatalf("SQLite accounts metadata = %#v", accounts)
	}
	assertMySQLSQLiteColumns(t, accounts, []mySQLSQLiteExpectedColumn{
		{name: "id", base: "integer", primaryKey: 1},
		{name: "code", base: "varchar", arguments: []int{24}, defaultSQL: "'guest'"},
		{name: "exact_count", base: "bigint", defaultSQL: "0"},
		{name: "enabled", base: "tinyint", defaultSQL: "1"},
		{
			name:      "payload",
			base:      "varbinary",
			arguments: []int{16},
			nullable:  true,
		},
		{name: "created_at", base: "datetime", arguments: []int{6}},
		{name: "local_time", base: "time", arguments: []int{6}},
		{name: "description", base: "varchar", arguments: []int{80}, nullable: true},
	})
	assertMySQLSQLiteIndex(
		t,
		accounts,
		prefix+"_count_idx",
		false,
		[]schema.IndexColumn{{
			Name:       "exact_count",
			Descending: true,
			Collation:  "BINARY",
		}},
	)
	assertMySQLSQLiteIndex(
		t,
		accounts,
		prefix+"_code_uq",
		true,
		[]schema.IndexColumn{{Name: "code", Collation: "BINARY"}},
	)
	assertMySQLSQLiteCheck(t, accounts, "exact_count", ">=")
	if len(accounts.Indexes) != 2 ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name != "" {
		t.Fatalf(
			"SQLite accounts objects = indexes %#v checks %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}

	events, _, err := inspectSQLiteSchema(
		ctx,
		database,
		eventsName,
	)
	if err != nil {
		t.Fatalf("inspect MySQL-family-to-SQLite events: %v", err)
	}
	if events.Schema != "" || events.MySQLCollation != "" {
		t.Fatalf("SQLite events metadata = %#v", events)
	}
	assertMySQLSQLiteColumns(t, events, []mySQLSQLiteExpectedColumn{
		{name: "tenant_id", base: "int", primaryKey: 1},
		{name: "event_id", base: "bigint", primaryKey: 2},
		{name: "account_id", base: "bigint"},
		{name: "note", base: "varchar", arguments: []int{80}, defaultSQL: "'created'"},
		{name: "exact_count", base: "bigint", defaultSQL: "0"},
		{name: "occurred_at", base: "datetime", arguments: []int{6}},
		{name: "observed_on", base: "date"},
		{name: "payload", base: "blob", nullable: true},
	})
	assertMySQLSQLiteIndex(
		t,
		events,
		prefix+"_account_idx",
		false,
		[]schema.IndexColumn{{
			Name:      "account_id",
			Collation: "BINARY",
		}},
	)
	assertMySQLSQLiteCheck(t, events, "event_id", ">")
	if len(events.Indexes) != 1 ||
		len(events.Checks) != 1 ||
		events.Checks[0].Name != "" ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"SQLite events objects = indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != "" ||
		len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		foreignKey.ReferencedTable != accountsName ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "NONE" {
		t.Fatalf("SQLite events foreign key = %#v", foreignKey)
	}
}

type mySQLSQLiteExpectedColumn struct {
	name       string
	base       string
	arguments  []int
	nullable   bool
	primaryKey int
	defaultSQL string
}

func assertMySQLSQLiteColumns(
	t *testing.T,
	table schema.Table,
	expected []mySQLSQLiteExpectedColumn,
) {
	t.Helper()
	if len(table.Columns) != len(expected) {
		t.Fatalf(
			"SQLite table %s columns = %#v, want %d",
			table.Name,
			table.Columns,
			len(expected),
		)
	}
	for index, want := range expected {
		column := table.Columns[index]
		if column.Name != want.name ||
			column.Nullable != want.nullable ||
			column.PrimaryKey != (want.primaryKey > 0) ||
			column.PrimaryKeyPosition != want.primaryKey ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != want.base ||
			fmt.Sprint(column.DeclaredType.Arguments) !=
				fmt.Sprint(want.arguments) {
			t.Fatalf(
				"SQLite table %s column %d = %#v, want %#v",
				table.Name,
				index,
				column,
				want,
			)
		}
		defaultSQL := ""
		if column.Default != nil {
			defaultSQL = column.Default.CanonicalSQL()
		}
		if defaultSQL != want.defaultSQL {
			t.Fatalf(
				"SQLite table %s column %s default = %q, want %q",
				table.Name,
				column.Name,
				defaultSQL,
				want.defaultSQL,
			)
		}
	}
}

func assertMySQLSQLiteIndex(
	t *testing.T,
	table schema.Table,
	name string,
	unique bool,
	columns []schema.IndexColumn,
) {
	t.Helper()
	for _, index := range table.Indexes {
		if index.Name != name {
			continue
		}
		if index.Unique != unique ||
			index.Inline ||
			fmt.Sprint(index.Columns) != fmt.Sprint(columns) {
			t.Fatalf(
				"SQLite table %s index %s = %#v, want unique=%t columns=%#v",
				table.Name,
				name,
				index,
				unique,
				columns,
			)
		}
		return
	}
	t.Fatalf("SQLite table %s lacks index %s", table.Name, name)
}

func assertMySQLSQLiteCheck(
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
		"SQLite table %s checks = %#v, want expression containing %q",
		table.Name,
		table.Checks,
		parts,
	)
}

func assertMySQLSQLiteCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
	expectedAccounts int,
	expectedSourceCount string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != expectedAccounts || eventCount != 2 {
		t.Fatalf(
			"SQLite common-fixture row counts = (%d, %d), want (%d, 2)",
			accountCount,
			eventCount,
			expectedAccounts,
		)
	}

	var (
		code        string
		exactCount  string
		enabled     int64
		payload     string
		createdAt   string
		localTime   string
		description string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", CAST("exact_count" AS TEXT), "enabled",
		        hex("payload"), CAST("created_at" AS TEXT),
		        CAST("local_time" AS TEXT), "description"
		   FROM `+quote(accountsName)+` WHERE "id" = 7`,
	).Scan(
		&code,
		&exactCount,
		&enabled,
		&payload,
		&createdAt,
		&localTime,
		&description,
	); err != nil {
		t.Fatalf("read MySQL-family-to-SQLite account: %v", err)
	}
	if code != "東京" ||
		exactCount != expectedSourceCount ||
		enabled != 1 ||
		payload != "00FF" ||
		createdAt != "2026-07-29 12:34:56.123456" ||
		localTime != "12:34:56.123456" ||
		description != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf(
			"SQLite account = (%q, %q, %d, %q, %q, %q, %q)",
			code,
			exactCount,
			enabled,
			payload,
			createdAt,
			localTime,
			description,
		)
	}
	var nullableDescription bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT "description" IS NULL FROM `+quote(accountsName)+
			` WHERE "id" = 11`,
	).Scan(&nullableDescription); err != nil {
		t.Fatalf("read MySQL-family-to-SQLite NULL: %v", err)
	}
	if !nullableDescription {
		t.Fatal("MySQL-family-to-SQLite NULL was not preserved")
	}

	var (
		note           string
		eventCountText string
		occurredAt     string
		observedOn     string
		eventPayload   string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT "note", CAST("exact_count" AS TEXT),
		        CAST("occurred_at" AS TEXT),
		        CAST("observed_on" AS TEXT), hex("payload")
		   FROM `+quote(eventsName)+
			` WHERE "tenant_id" = 1
			    AND "event_id" = 9007199254740993`,
	).Scan(
		&note,
		&eventCountText,
		&occurredAt,
		&observedOn,
		&eventPayload,
	); err != nil {
		t.Fatalf("read MySQL-family-to-SQLite event: %v", err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		eventCountText != "9007199254740995" ||
		occurredAt != "2026-07-29 12:34:56.123456" ||
		observedOn != "2026-07-29" ||
		eventPayload != "DEADBEEF" {
		t.Fatalf(
			"SQLite event = (%q, %q, %q, %q, %q)",
			note,
			eventCountText,
			occurredAt,
			observedOn,
			eventPayload,
		)
	}
}

func assertMySQLSQLiteDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	var sequence int64
	if err := database.QueryRowContext(
		ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = ?`,
		accountsName,
	).Scan(&sequence); err != nil {
		t.Fatalf("read MySQL-family-to-SQLite identity frontier: %v", err)
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
		"INSERT INTO "+quote(accountsName)+
			` ("created_at", "local_time", "description")
			 VALUES ('2026-07-30 00:00:00.000000',
			         '00:00:00.000000', 'defaults')`,
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
		   FROM `+quote(accountsName)+` WHERE "id" = ?`,
		id,
	).Scan(&code, &exactCount, &enabled); err != nil {
		t.Fatalf("read SQLite defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		exactCount != "0" ||
		enabled != 1 {
		t.Fatalf(
			"SQLite defaults row = (%d, %q, %q, %d)",
			id,
			code,
			exactCount,
			enabled,
		)
	}
}

func insertMySQLSQLiteRetainedRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(accountsName)+
			` ("id", "code", "exact_count", "enabled", "payload",
			   "created_at", "local_time", "description")
			 VALUES (99, 'target-only', 77, 1, X'0102',
			         '2026-07-30 01:02:03.000000',
			         '01:02:03.000000', 'retained')`,
	); err != nil {
		t.Fatalf("insert retained MySQL-family-to-SQLite target row: %v", err)
	}
}

func assertMySQLSQLiteRetainedUpsert(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	assertMySQLSQLiteCommonRows(
		t,
		ctx,
		database,
		accountsName,
		eventsName,
		3,
		"23",
	)
	var (
		exactCount  string
		description string
		payload     string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT CAST("exact_count" AS TEXT), "description", hex("payload")
		   FROM `+quote(accountsName)+
			` WHERE "id" = 99 AND "code" = 'target-only'`,
	).Scan(&exactCount, &description, &payload); err != nil {
		t.Fatalf("read retained MySQL-family-to-SQLite row: %v", err)
	}
	if exactCount != "77" ||
		description != "retained" ||
		payload != "0102" {
		t.Fatalf(
			"SQLite retained row = (%q, %q, %q)",
			exactCount,
			description,
			payload,
		)
	}
}

func createMySQLSQLiteUnsupportedLaterTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
	collation string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL,
				exact_count DECIMAL(18,0) NOT NULL,
				PRIMARY KEY (id),
				CONSTRAINT %s
					CHECK (exact_count < 9007199254740992.1)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=%s`,
			mySQLIdentifier(name),
			mySQLIdentifier(name+"_fractional_ck"),
			collation,
		),
	); err != nil {
		t.Fatalf(
			"create unsupported later MySQL-family-to-SQLite table: %v",
			err,
		)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(name)+
			" VALUES (1, 9007199254740992)",
	); err != nil {
		t.Fatalf(
			"insert unsupported later MySQL-family-to-SQLite row: %v",
			err,
		)
	}
}
