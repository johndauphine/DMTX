package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLToSQLServerCommonFixtureLive(t *testing.T) {
	testMySQLFamilyToSQLServerCommonFixtureLive(t, mysqlFamilyLiveFixture{
		name:       "Oracle MySQL",
		dsnEnv:     "DMTX_TEST_MYSQL_DSN",
		caEnv:      "DMTX_TEST_MYSQL_CA",
		tlsConfig:  "dmtx_test",
		namePrefix: "dmtx_ms_",
		collation:  "utf8mb4_0900_bin",
	})
}

func TestMariaDBToSQLServerCommonFixtureLive(t *testing.T) {
	testMySQLFamilyToSQLServerCommonFixtureLive(t, mysqlFamilyLiveFixture{
		name:       "MariaDB",
		dsnEnv:     "DMTX_TEST_MARIADB_DSN",
		caEnv:      "DMTX_TEST_MARIADB_CA",
		tlsConfig:  "dmtx_mariadb_test",
		namePrefix: "dmtx_mas_",
		collation:  "utf8mb4_nopad_bin",
	})
}

func testMySQLFamilyToSQLServerCommonFixtureLive(
	t *testing.T,
	fixture mysqlFamilyLiveFixture,
) {
	t.Helper()
	sourceDSN := os.Getenv(fixture.dsnEnv)
	sourceCA := os.Getenv(fixture.caEnv)
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	targetCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	if sourceDSN == "" || sourceCA == "" ||
		targetDSN == "" || targetCA == "" {
		t.Skip(
			"set " + fixture.dsnEnv + ", " + fixture.caEnv +
				", DMTX_TEST_MSSQL_TARGET_DSN, and " +
				"DMTX_TEST_MSSQL_CA to run the " + fixture.name +
				"-to-SQL Server common fixture",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		sourceCA,
		fixture.tlsConfig,
	)
	sourceConfig, err := mysqlDriver.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatalf("parse %s common-fixture DSN: %T", fixture.name, err)
	}
	if sourceConfig.TLSConfig != fixture.tlsConfig &&
		sourceConfig.TLSConfig != "true" {
		t.Fatalf("%s must require verified TLS", fixture.dsnEnv)
	}
	if sourceConfig.DBName == "" {
		t.Fatalf("%s must select a database", fixture.dsnEnv)
	}
	sourceEndpoint := mySQLSQLServerSourceEndpoint(
		t,
		sourceConfig,
		sourceCA,
	)
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		targetCA,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		180*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open %s common-fixture source: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close %s common-fixture source: %v", fixture.name, err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify %s common-fixture source: %v", fixture.name, err)
	}
	var tlsVariable, tlsCipher string
	if err := sourceDatabase.QueryRowContext(
		ctx,
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
	).Scan(&tlsVariable, &tlsCipher); err != nil {
		t.Fatalf("inspect %s source TLS status: %v", fixture.name, err)
	}
	if tlsVariable != "Ssl_cipher" || tlsCipher == "" {
		t.Fatalf("%s common-fixture source is not using TLS", fixture.name)
	}
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetEndpoint,
	)

	prefix := fixture.namePrefix +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupMySQLToSQLServerSourceTables(
		t,
		sourceDatabase,
		eventsName,
		accountsName,
	)
	cleanupSQLServerNativeTables(
		t,
		targetDatabase,
		eventsName,
		accountsName,
	)
	createMySQLSQLServerCommonFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
		fixture.collation,
	)
	insertMySQLSQLServerCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)
	sourceMetadata := inspectMySQLCommonFixture(
		t,
		ctx,
		sourceDatabase,
		sourceConfig.DBName,
		accountsName,
		eventsName,
	)
	assertMySQLSQLServerSourceFixtureMetadata(
		t,
		sourceMetadata,
		fixture.collation,
		accountsName,
		eventsName,
	)

	seedSQLServerNativeReplacementTargets(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	_, err = MySQLToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"%s populated SQL Server rebuild without acknowledgement error = %v, want %v",
			fixture.name,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	assertMySQLSQLServerReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	for _, name := range []string{accountsName, eventsName} {
		if _, err := targetDatabase.ExecContext(
			ctx,
			"DELETE FROM "+sqlServerQualified("dbo", name),
		); err != nil {
			t.Fatalf(
				"empty %s SQL Server acknowledgement-race target %s: %v",
				fixture.name,
				name,
				err,
			)
		}
	}
	raceObserver := &sqlServerDestructiveRaceSentinelObserver{
		database: targetDatabase,
		table:    accountsName,
	}
	result, err := MySQLToSQLServerWithObserver(
		ctx,
		migrationConfig,
		raceObserver,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"%s SQL Server row added after preflight result = %+v, error = %v, want %v",
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
			"%s SQL Server acknowledgement-race observer = %+v",
			fixture.name,
			raceObserver,
		)
	}
	assertMySQLSQLServerReplacementSentinels(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)

	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = MySQLToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate %s common fixture into SQL Server: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"%s-to-SQL Server result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	targetMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLToSQLServerCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
		41,
	)
	assertMySQLToSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		2,
		"12.34",
	)
	assertSQLServerNativeStaleTargetsWereReplaced(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	prepareMySQLToSQLServerRetainedUpsert(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		accountsName,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	migrationConfig.Migration.DestructiveAcknowledged = false
	result, err = MySQLToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert %s common fixture into SQL Server: %v",
			fixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"%s-to-SQL Server retained result = %+v, want 2 tables, 4 rows, validated",
			fixture.name,
			result,
		)
	}
	assertMySQLToSQLServerRetainedRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLToSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		3,
		"23.45",
	)
	targetMetadata = inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLToSQLServerCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
		41,
	)
	assertMySQLToSQLServerDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)

	assertMySQLToSQLServerUnsupportedShapeFailsBeforeMutation(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		migrationConfig,
		fixture.collation,
		prefix+"_unsupported",
	)
}

type sqlServerDestructiveRaceSentinelObserver struct {
	database     *sql.DB
	table        string
	beforeSets   int
	beforeTables int
	afterTables  int
}

func (observer *sqlServerDestructiveRaceSentinelObserver) BeforeTables(
	ctx context.Context,
	_ []string,
) error {
	observer.beforeSets++
	_, err := observer.database.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", observer.table)+
			" ([stale_id], [stale_marker]) "+
			"VALUES (99, 'must disappear')",
	)
	return err
}

func (observer *sqlServerDestructiveRaceSentinelObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.beforeTables++
	return nil
}

func (observer *sqlServerDestructiveRaceSentinelObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.afterTables++
	return nil
}

func mySQLSQLServerSourceEndpoint(
	t *testing.T,
	source *mysqlDriver.Config,
	caPath string,
) config.Endpoint {
	t.Helper()
	host, rawPort, err := net.SplitHostPort(source.Addr)
	if err != nil {
		t.Fatalf("parse MySQL-family common-fixture address: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse MySQL-family common-fixture port: %v", err)
	}
	return config.Endpoint{
		Type:      "mysql",
		Host:      host,
		Port:      port,
		Database:  source.DBName,
		User:      source.User,
		Password:  source.Passwd,
		Schema:    source.DBName,
		SSLMode:   "verify-full",
		TLSCAFile: caPath,
	}
}

func cleanupMySQLToSQLServerSourceTables(
	t *testing.T,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cancel()
		for _, name := range names {
			if _, err := database.ExecContext(
				ctx,
				"DROP TABLE IF EXISTS "+mySQLIdentifier(name),
			); err != nil {
				t.Errorf(
					"drop MySQL-to-SQL Server source table %s: %v",
					name,
					err,
				)
			}
		}
	})
}

func createMySQLSQLServerCommonFixture(
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
			balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
			enabled INT NOT NULL DEFAULT 1,
			payload VARBINARY(16) NULL,
			created_at DATETIME(3) NOT NULL,
			PRIMARY KEY (id),
			KEY %s (balance DESC),
			CONSTRAINT %s CHECK (balance >= 0)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=%s
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLIdentifier(accountsName),
		collation,
		mySQLIdentifier(prefix+"_balance_idx"),
		mySQLIdentifier(prefix+"_account_ck"),
		collation,
	)
	if _, err := database.ExecContext(ctx, accountsDDL); err != nil {
		t.Fatalf("create MySQL-to-SQL Server accounts: %v", err)
	}
	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT NOT NULL,
			event_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			note VARCHAR(80) COLLATE %s NOT NULL DEFAULT 'created',
			amount DECIMAL(12,3) NOT NULL DEFAULT 0.000,
			occurred_at DATETIME(6) NOT NULL,
			observed_on DATE NOT NULL,
			payload BLOB NULL,
			PRIMARY KEY (tenant_id, event_id),
			KEY %s (account_id),
			KEY %s (occurred_at DESC),
			CONSTRAINT %s FOREIGN KEY (account_id)
				REFERENCES %s (id)
				ON UPDATE CASCADE
				ON DELETE NO ACTION,
			CONSTRAINT %s CHECK (event_id > 0)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=%s
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLIdentifier(eventsName),
		collation,
		mySQLIdentifier(prefix+"_account_idx"),
		mySQLIdentifier(prefix+"_occurred_idx"),
		mySQLIdentifier(prefix+"_account_fk"),
		mySQLIdentifier(accountsName),
		mySQLIdentifier(prefix+"_event_ck"),
		collation,
	)
	if _, err := database.ExecContext(ctx, eventsDDL); err != nil {
		t.Fatalf("create MySQL-to-SQL Server events: %v", err)
	}
}

func insertMySQLSQLServerCommonFixtureRows(
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
			` (id, code, balance, enabled, payload, created_at)
			 VALUES
			 (7, '東京', 12.34, 1, UNHEX('00ff'),
			  '2026-07-29 12:34:56.123'),
			 (11, 'emoji 😀', 0.00, 0, NULL,
			  '2026-07-29 23:59:59.999')`,
	); err != nil {
		t.Fatalf("insert MySQL-to-SQL Server accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+mySQLIdentifier(accountsName)+
			" AUTO_INCREMENT = 42",
	); err != nil {
		t.Fatalf("set MySQL-to-SQL Server identity frontier: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(eventsName)+
			` (tenant_id, event_id, account_id, note, amount,
			   occurred_at, observed_on, payload)
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京', 9.125,
			  '2026-07-29 12:34:56.123456', '2026-07-29',
			  UNHEX('deadbeef')),
			 (1, 9007199254740995, 11,
			  'emoji 😀', 0.000,
			  '2026-07-29 23:59:59.999999', '2026-07-30', NULL)`,
	); err != nil {
		t.Fatalf("insert MySQL-to-SQL Server events: %v", err)
	}
}

func assertMySQLSQLServerSourceFixtureMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	collation string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts, accountsOK := tables[accountsName]
	events, eventsOK := tables[eventsName]
	if !accountsOK || !eventsOK ||
		accounts.MySQLCollation != collation ||
		events.MySQLCollation != collation ||
		accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		len(accounts.Columns) != 6 ||
		len(events.Columns) != 8 {
		t.Fatalf("MySQL-family source fixture metadata = %#v", tables)
	}
}

type mySQLSQLServerExpectedColumn struct {
	name       string
	canonical  string
	base       string
	arguments  []int
	nullable   bool
	primaryKey int
	defaultSQL string
}

func assertMySQLToSQLServerCommonMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	prefix string,
	accountsName string,
	eventsName string,
	identityFrontier int64,
) {
	t.Helper()
	accounts, accountsOK := tables[accountsName]
	events, eventsOK := tables[eventsName]
	if !accountsOK || !eventsOK ||
		accounts.Schema != "dbo" || events.Schema != "dbo" ||
		accounts.MySQLCollation != "" || events.MySQLCollation != "" {
		t.Fatalf("MySQL-to-SQL Server table metadata = %#v", tables)
	}
	if accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != identityFrontier {
		t.Fatalf(
			"MySQL-to-SQL Server identity = %#v, want frontier %d",
			accounts.Identity,
			identityFrontier,
		)
	}
	assertMySQLSQLServerColumns(t, accounts, []mySQLSQLServerExpectedColumn{
		{name: "id", canonical: "bigint", base: "bigint", primaryKey: 1},
		{
			name:       "code",
			canonical:  "text",
			base:       "varchar",
			arguments:  []int{96},
			defaultSQL: "'guest'",
		},
		{
			name:       "balance",
			canonical:  "numeric",
			base:       "decimal",
			arguments:  []int{12, 2},
			defaultSQL: "0",
		},
		{
			name:       "enabled",
			canonical:  "integer",
			base:       "int",
			defaultSQL: "1",
		},
		{
			name:      "payload",
			canonical: "blob",
			base:      "varbinary",
			arguments: []int{16},
			nullable:  true,
		},
		{
			name:      "created_at",
			canonical: "datetime",
			base:      "timestamp",
			arguments: []int{3},
		},
	})
	assertMySQLSQLServerColumns(t, events, []mySQLSQLServerExpectedColumn{
		{name: "tenant_id", canonical: "integer", base: "int", primaryKey: 1},
		{name: "event_id", canonical: "bigint", base: "bigint", primaryKey: 2},
		{name: "account_id", canonical: "bigint", base: "bigint"},
		{
			name:       "note",
			canonical:  "text",
			base:       "varchar",
			arguments:  []int{320},
			defaultSQL: "'created'",
		},
		{
			name:       "amount",
			canonical:  "numeric",
			base:       "decimal",
			arguments:  []int{12, 3},
			defaultSQL: "0",
		},
		{
			name:      "occurred_at",
			canonical: "datetime",
			base:      "timestamp",
			arguments: []int{6},
		},
		{
			name:      "observed_on",
			canonical: "date",
			base:      "date",
		},
		{name: "payload", canonical: "blob", base: "blob", nullable: true},
	})
	assertMySQLSQLServerIndex(
		t,
		accounts,
		prefix+"_balance_idx",
		false,
		[]schema.IndexColumn{{Name: "balance", Descending: true}},
	)
	if len(accounts.Indexes) != 1 ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name != prefix+"_account_ck" ||
		accounts.Checks[0].Expression.CanonicalSQL() != `"balance" >= 0` {
		t.Fatalf(
			"MySQL-to-SQL Server accounts objects = indexes %#v checks %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}
	assertMySQLSQLServerIndex(
		t,
		events,
		prefix+"_account_idx",
		false,
		[]schema.IndexColumn{{Name: "account_id"}},
	)
	assertMySQLSQLServerIndex(
		t,
		events,
		prefix+"_occurred_idx",
		false,
		[]schema.IndexColumn{{Name: "occurred_at", Descending: true}},
	)
	if len(events.Indexes) != 2 ||
		len(events.Checks) != 1 ||
		events.Checks[0].Name != prefix+"_event_ck" ||
		events.Checks[0].Expression.CanonicalSQL() != `"event_id" > 0` ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"MySQL-to-SQL Server events objects = indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != prefix+"_account_fk" ||
		len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		foreignKey.ReferencedTable != accountsName ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" ||
		foreignKey.Match != "SIMPLE" {
		t.Fatalf("MySQL-to-SQL Server foreign key = %#v", foreignKey)
	}
}

func assertMySQLSQLServerColumns(
	t *testing.T,
	table schema.Table,
	expected []mySQLSQLServerExpectedColumn,
) {
	t.Helper()
	if len(table.Columns) != len(expected) {
		t.Fatalf(
			"SQL Server table %s columns = %#v, want %d",
			table.Name,
			table.Columns,
			len(expected),
		)
	}
	for index, want := range expected {
		column := table.Columns[index]
		if column.Name != want.name ||
			column.Type != want.canonical ||
			column.Nullable != want.nullable ||
			column.PrimaryKeyPosition != want.primaryKey ||
			column.PrimaryKey != (want.primaryKey > 0) ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != want.base ||
			fmt.Sprint(column.DeclaredType.Arguments) !=
				fmt.Sprint(want.arguments) {
			t.Fatalf(
				"SQL Server table %s column %d = %#v, want %#v",
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
				"SQL Server table %s column %s default = %q, want %q",
				table.Name,
				column.Name,
				defaultSQL,
				want.defaultSQL,
			)
		}
	}
}

func assertMySQLSQLServerIndex(
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
				"SQL Server table %s index %s = %#v, want unique=%t columns=%#v",
				table.Name,
				name,
				index,
				unique,
				columns,
			)
		}
		return
	}
	t.Fatalf("SQL Server table %s lacks index %s", table.Name, name)
}

func assertMySQLSQLServerReplacementSentinels(
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
			"SELECT [stale_marker] FROM "+
				sqlServerQualified("dbo", name)+
				" WHERE [stale_id] = 99",
		).Scan(&marker); err != nil {
			t.Fatalf(
				"read SQL Server destructive-ack sentinel %s: %v",
				name,
				err,
			)
		}
		if marker != "must disappear" {
			t.Fatalf(
				"SQL Server destructive-ack sentinel %s = %q",
				name,
				marker,
			)
		}
	}
}

func assertMySQLToSQLServerCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
	expectedAccountCount int,
	expectedBalance string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != expectedAccountCount || eventCount != 2 {
		t.Fatalf(
			"MySQL-to-SQL Server row counts = (%d, %d), want (%d, 2)",
			accountCount,
			eventCount,
			expectedAccountCount,
		)
	}
	var (
		code      string
		balance   string
		enabled   int
		payload   []byte
		createdAt string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code], CONVERT(varchar(64), [balance]), [enabled],
		        [payload], CONVERT(varchar(27), [created_at], 121)
		   FROM `+sqlServerQualified("dbo", accountsName)+
			` WHERE [id] = 7`,
	).Scan(
		&code,
		&balance,
		&enabled,
		&payload,
		&createdAt,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != expectedBalance ||
		enabled != 1 ||
		!bytes.Equal(payload, []byte{0x00, 0xff}) ||
		createdAt != "2026-07-29 12:34:56.123" {
		t.Fatalf(
			"MySQL-to-SQL Server account = (%q, %q, %d, %x, %q)",
			code,
			balance,
			enabled,
			payload,
			createdAt,
		)
	}
	var (
		note       string
		amount     string
		occurredAt string
		observedOn string
		eventData  []byte
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [note], CONVERT(varchar(64), [amount]),
		        CONVERT(varchar(27), [occurred_at], 121),
		        CONVERT(varchar(10), [observed_on], 23), [payload]
		   FROM `+sqlServerQualified("dbo", eventsName)+
			` WHERE [tenant_id] = 1
			    AND [event_id] = 9007199254740993`,
	).Scan(
		&note,
		&amount,
		&occurredAt,
		&observedOn,
		&eventData,
	); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		amount != "9.125" ||
		occurredAt != "2026-07-29 12:34:56.123456" ||
		observedOn != "2026-07-29" ||
		!bytes.Equal(eventData, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf(
			"MySQL-to-SQL Server event = (%q, %q, %q, %q, %x)",
			note,
			amount,
			occurredAt,
			observedOn,
			eventData,
		)
	}
	var nullPayload any
	if err := database.QueryRowContext(
		ctx,
		"SELECT [payload] FROM "+
			sqlServerQualified("dbo", accountsName)+" WHERE [id] = 11",
	).Scan(&nullPayload); err != nil {
		t.Fatal(err)
	}
	if nullPayload != nil {
		t.Fatalf("MySQL NULL payload became %#v", nullPayload)
	}
}

func prepareMySQLToSQLServerRetainedUpsert(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	accountsName string,
) {
	t.Helper()
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(accountsName)+
			" SET balance = 23.45 WHERE id = 7",
	); err != nil {
		t.Fatalf("update MySQL retained-upsert source row: %v", err)
	}
	table := sqlServerQualified("dbo", accountsName)
	batch := fmt.Sprintf(`
		SET IDENTITY_INSERT %s ON;
		INSERT INTO %s
			([id], [code], [balance], [enabled], [payload], [created_at])
		VALUES
			(29, 'target-only', 1.00, 1, NULL,
			 CONVERT(datetime2(3), '2026-07-30T00:00:00.000'));
		SET IDENTITY_INSERT %s OFF;
	`, table, table, table)
	if _, err := target.ExecContext(ctx, batch); err != nil {
		_, _ = target.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+table+" OFF",
		)
		t.Fatalf("insert retained MySQL-to-SQL Server target row: %v", err)
	}
}

func assertMySQLToSQLServerRetainedRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var balance string
	if err := database.QueryRowContext(
		ctx,
		"SELECT CONVERT(varchar(64), [balance]) FROM "+
			sqlServerQualified("dbo", accountsName)+" WHERE [id] = 7",
	).Scan(&balance); err != nil {
		t.Fatalf("read retained-upsert source row: %v", err)
	}
	if balance != "23.45" {
		t.Fatalf("retained-upsert source balance = %q, want 23.45", balance)
	}
	var retained int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName)+
			" WHERE [id] = 29 AND [code] = 'target-only'",
	).Scan(&retained); err != nil {
		t.Fatalf("read retained-upsert target-only row: %v", err)
	}
	if retained != 1 {
		t.Fatalf("retained-upsert target-only rows = %d, want 1", retained)
	}
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 3 || eventCount != 2 {
		t.Fatalf(
			"retained-upsert row counts = (%d, %d), want (3, 2)",
			accountCount,
			eventCount,
		)
	}
}

func assertMySQLToSQLServerDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	var (
		id        int64
		code      string
		balance   string
		enabled   int
		createdAt string
	)
	err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", accountsName)+
			` ([created_at])
			 OUTPUT INSERTED.[id], INSERTED.[code],
			         CONVERT(varchar(64), INSERTED.[balance]),
			         INSERTED.[enabled],
			         CONVERT(varchar(27), INSERTED.[created_at], 121)
			 VALUES (CONVERT(datetime2(3), '2026-01-01T00:00:00.000'))`,
	).Scan(&id, &code, &balance, &enabled, &createdAt)
	if err != nil {
		t.Fatalf("insert MySQL-to-SQL Server target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		enabled != 1 ||
		createdAt != "2026-01-01 00:00:00.000" {
		t.Fatalf(
			"target defaults row = (%d, %q, %q, %d, %q)",
			id,
			code,
			balance,
			enabled,
			createdAt,
		)
	}
}

func assertMySQLToSQLServerUnsupportedShapeFailsBeforeMutation(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	migrationConfig config.Config,
	collation string,
	tableName string,
) {
	t.Helper()
	cleanupMySQLToSQLServerSourceTables(t, source, tableName)
	cleanupSQLServerNativeTables(t, target, tableName)
	if _, err := source.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLIdentifier(tableName)+
			` (
				id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=`+collation+
			" ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create unsupported MySQL-to-SQL Server source: %v", err)
	}
	seedSQLServerNativeReplacementTargets(t, ctx, target, tableName)
	migrationConfig.Migration.TargetMode = "drop_recreate"
	migrationConfig.Migration.IncludeTables = []string{tableName}
	migrationConfig.Migration.DestructiveAcknowledged = true
	_, err := MySQLToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	var policyError *schema.PolicyError
	if err == nil || !errors.As(err, &policyError) {
		t.Fatalf(
			"unsupported MySQL-to-SQL Server source error = %v, want schema policy error",
			err,
		)
	}
	assertMySQLSQLServerReplacementSentinels(
		t,
		ctx,
		target,
		tableName,
	)
}
