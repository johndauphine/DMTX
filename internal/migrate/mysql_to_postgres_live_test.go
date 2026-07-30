package migrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLToPostgresCommonFixtureLive(t *testing.T) {
	mysqlDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	mysqlCA := os.Getenv("DMTX_TEST_MYSQL_CA")
	if mysqlDSN == "" || postgresDSN == "" || mysqlCA == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_CA, and DMTX_TEST_POSTGRES_DSN to run the MySQL-to-PostgreSQL common fixture",
		)
	}
	registerMySQLCommonFixtureTLS(t, mysqlCA)
	mysqlConfig, err := mysqlDriver.ParseDSN(mysqlDSN)
	if err != nil {
		t.Fatalf("parse MySQL common-fixture DSN: %T", err)
	}
	if mysqlConfig.TLSConfig != "dmtx_test" &&
		mysqlConfig.TLSConfig != "true" {
		t.Fatal("DMTX_TEST_MYSQL_DSN must require verified TLS")
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL common-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open MySQL common-fixture source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close MySQL common-fixture source: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify MySQL common-fixture source: %T", err)
	}

	prefix := "dmtx_mc_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		for _, name := range []string{eventsName, accountsName} {
			if _, err := sourceDatabase.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+mySQLIdentifier(name),
			); err != nil {
				t.Errorf("drop MySQL common-fixture table %s: %v", name, err)
			}
		}
	})
	createMySQLCommonFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
	)
	insertMySQLCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)

	host, rawPort, err := net.SplitHostPort(mysqlConfig.Addr)
	if err != nil {
		t.Fatalf("parse MySQL common-fixture address: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse MySQL common-fixture port: %v", err)
	}
	sourceEndpoint := config.Endpoint{
		Type:      "mysql",
		Host:      host,
		Port:      port,
		Database:  mysqlConfig.DBName,
		User:      mysqlConfig.User,
		Password:  mysqlConfig.Passwd,
		Schema:    mysqlConfig.DBName,
		SSLMode:   "verify-full",
		TLSCAFile: mysqlCA,
	}
	namespace := "dmtx_mysql_common_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	targetEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     postgresConfig.Host,
		Port:     int(postgresConfig.Port),
		Database: postgresConfig.Database,
		User:     postgresConfig.User,
		Password: postgresConfig.Password,
		Schema:   namespace,
		SSLMode:  "require",
	}
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	targetDatabase, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL common-fixture target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL common-fixture target: %v", err)
		}
	})
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL common-fixture target: %v", err)
	}
	if _, err := targetDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL common-fixture target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := targetDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL common-fixture target schema: %v",
				err,
			)
		}
	})

	sourceMetadata := inspectMySQLCommonFixture(
		t,
		ctx,
		sourceDatabase,
		mysqlConfig.DBName,
		accountsName,
		eventsName,
	)
	result, err := MySQLToPostgresWithObserver(
		ctx,
		config.Config{
			Source: sourceEndpoint,
			Target: targetEndpoint,
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: []string{accountsName, eventsName},
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("migrate MySQL common fixture: %v", err)
	}
	if result.Tables != 2 ||
		result.Rows != 4 ||
		!result.Validated {
		t.Fatalf(
			"MySQL common-fixture result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetMetadata := inspectPostgresMySQLCommonFixture(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
		eventsName,
	)
	assertMySQLToPostgresCommonMetadata(
		t,
		sourceMetadata,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
	)
	assertMySQLToPostgresCommonRows(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
		eventsName,
	)
	assertMySQLToPostgresDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
	)
}

func registerMySQLCommonFixtureTLS(t *testing.T, caPath string) {
	t.Helper()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read DMTX_TEST_MYSQL_CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("DMTX_TEST_MYSQL_CA contains no certificates")
	}
	if err := mysqlDriver.RegisterTLSConfig(
		"dmtx_test",
		&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		},
	); err != nil {
		t.Fatalf("register MySQL common-fixture TLS: %v", err)
	}
}

func createMySQLCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accountsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			code VARCHAR(24) COLLATE utf8mb4_bin NOT NULL DEFAULT 'guest',
			balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			payload VARBINARY(16) NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			document JSON NULL,
			PRIMARY KEY (id),
			UNIQUE KEY %s (code),
			CONSTRAINT %s CHECK (balance >= 0 AND code <> '')
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=utf8mb4_bin
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLIdentifier(accountsName),
		mySQLIdentifier(prefix+"_code_uq"),
		mySQLIdentifier(prefix+"_account_ck"),
	)
	if _, err := database.ExecContext(ctx, accountsDDL); err != nil {
		t.Fatalf("create MySQL common-fixture accounts: %v", err)
	}
	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT NOT NULL,
			event_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			note VARCHAR(80) COLLATE utf8mb4_bin NOT NULL DEFAULT 'created',
			amount DECIMAL(12,3) NOT NULL DEFAULT 0.000,
			occurred_at DATETIME(6) NOT NULL,
			observed_on DATE NOT NULL DEFAULT (CURRENT_DATE),
			payload BLOB NULL,
			PRIMARY KEY (tenant_id, event_id),
			KEY %s (occurred_at DESC),
			CONSTRAINT %s FOREIGN KEY (account_id)
				REFERENCES %s (id)
				ON UPDATE CASCADE
				ON DELETE RESTRICT,
			CONSTRAINT %s CHECK (event_id > 0)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=utf8mb4_bin
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLIdentifier(eventsName),
		mySQLIdentifier(prefix+"_occurred_idx"),
		mySQLIdentifier(prefix+"_account_fk"),
		mySQLIdentifier(accountsName),
		mySQLIdentifier(prefix+"_event_ck"),
	)
	if _, err := database.ExecContext(ctx, eventsDDL); err != nil {
		t.Fatalf("create MySQL common-fixture events: %v", err)
	}
}

func insertMySQLCommonFixtureRows(
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
			` (id, code, balance, enabled, payload, created_at, document)
			 VALUES
			 (7, '東京', 12.34, 1, UNHEX('00ff'),
			  '2026-07-29 12:34:56', '{"active":true}'),
			 (11, 'emoji 😀', 0.00, 0, NULL,
			  '2026-07-29 23:59:59', NULL)`,
	); err != nil {
		t.Fatalf("insert MySQL common-fixture accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+mySQLIdentifier(accountsName)+
			" AUTO_INCREMENT = 42",
	); err != nil {
		t.Fatalf("set MySQL common-fixture identity frontier: %v", err)
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
		t.Fatalf("insert MySQL common-fixture events: %v", err)
	}
}

func inspectMySQLCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
	eventsName string,
) map[string]schema.Table {
	t.Helper()
	result := make(map[string]schema.Table, 2)
	for _, name := range []string{accountsName, eventsName} {
		table, err := engine.InspectMySQLTable(
			ctx,
			database,
			namespace,
			name,
		)
		if err != nil {
			t.Fatalf("inspect MySQL common-fixture table %s: %v", name, err)
		}
		result[name] = table
	}
	return result
}

func inspectPostgresMySQLCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
	eventsName string,
) map[string]schema.Table {
	t.Helper()
	names, err := engine.ListPostgresTables(ctx, database, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 ||
		!contains(names, accountsName) ||
		!contains(names, eventsName) {
		t.Fatalf("PostgreSQL common-fixture tables = %#v", names)
	}
	result := make(map[string]schema.Table, 2)
	for _, name := range names {
		table, err := engine.InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		if err != nil {
			t.Fatalf(
				"inspect PostgreSQL common-fixture table %s: %v",
				name,
				err,
			)
		}
		result[name] = table
	}
	return result
}

func assertMySQLToPostgresCommonMetadata(
	t *testing.T,
	source map[string]schema.Table,
	target map[string]schema.Table,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	sourceAccounts := source[accountsName]
	targetAccounts := target[accountsName]
	if sourceAccounts.Identity == nil ||
		sourceAccounts.Identity.Frontier == nil ||
		*sourceAccounts.Identity.Frontier != 41 ||
		targetAccounts.Identity == nil ||
		targetAccounts.Identity.Frontier == nil ||
		*targetAccounts.Identity.Frontier != 41 {
		t.Fatalf(
			"common-fixture identities = source %#v target %#v",
			sourceAccounts.Identity,
			targetAccounts.Identity,
		)
	}
	if len(targetAccounts.Columns) != 7 ||
		targetAccounts.Columns[0].Type != "bigint" ||
		targetAccounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("target accounts columns = %#v", targetAccounts.Columns)
	}
	assertPostgresCommonDeclaredType(
		t,
		targetAccounts.Columns[1],
		"varchar",
		24,
	)
	assertPostgresCommonDeclaredType(
		t,
		targetAccounts.Columns[2],
		"numeric",
		12,
		2,
	)
	if targetAccounts.Columns[3].Type != "integer" ||
		targetAccounts.Columns[4].Type != "bytea" ||
		targetAccounts.Columns[6].Type != "json" {
		t.Fatalf("target accounts mapped columns = %#v", targetAccounts.Columns)
	}
	assertPostgresCommonDeclaredType(
		t,
		targetAccounts.Columns[5],
		"timestamp",
		0,
	)
	if len(targetAccounts.Indexes) != 1 ||
		targetAccounts.Indexes[0].Name != prefix+"_code_uq" ||
		!targetAccounts.Indexes[0].Unique ||
		targetAccounts.Indexes[0].Columns[0].Collation != "BINARY" ||
		len(targetAccounts.Checks) != 1 ||
		targetAccounts.Checks[0].Name != prefix+"_account_ck" {
		t.Fatalf(
			"target accounts objects = indexes %#v checks %#v",
			targetAccounts.Indexes,
			targetAccounts.Checks,
		)
	}

	targetEvents := target[eventsName]
	if len(targetEvents.Columns) != 8 ||
		targetEvents.Columns[0].PrimaryKeyPosition != 1 ||
		targetEvents.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("target events columns = %#v", targetEvents.Columns)
	}
	assertPostgresCommonDeclaredType(
		t,
		targetEvents.Columns[3],
		"varchar",
		80,
	)
	assertPostgresCommonDeclaredType(
		t,
		targetEvents.Columns[4],
		"numeric",
		12,
		3,
	)
	assertPostgresCommonDeclaredType(
		t,
		targetEvents.Columns[5],
		"timestamp",
		6,
	)
	if len(targetEvents.Indexes) != 2 ||
		len(targetEvents.ForeignKeys) != 1 ||
		len(targetEvents.Checks) != 1 {
		t.Fatalf(
			"target events objects = indexes %#v foreign keys %#v checks %#v",
			targetEvents.Indexes,
			targetEvents.ForeignKeys,
			targetEvents.Checks,
		)
	}
	var descending bool
	for _, index := range targetEvents.Indexes {
		if index.Name == prefix+"_occurred_idx" {
			descending = len(index.Columns) == 1 &&
				index.Columns[0].Descending
		}
	}
	if !descending {
		t.Fatalf("target events indexes = %#v", targetEvents.Indexes)
	}
	foreignKey := targetEvents.ForeignKeys[0]
	if foreignKey.Name != prefix+"_account_fk" ||
		foreignKey.ReferencedTable != accountsName ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "SIMPLE" {
		t.Fatalf("target events foreign key = %#v", foreignKey)
	}
	if targetEvents.Checks[0].Name != prefix+"_event_ck" {
		t.Fatalf("target events checks = %#v", targetEvents.Checks)
	}
}

func assertPostgresCommonDeclaredType(
	t *testing.T,
	column schema.Column,
	base string,
	arguments ...int,
) {
	t.Helper()
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		len(column.DeclaredType.Arguments) != len(arguments) {
		t.Fatalf("column %s declaration = %#v", column.Name, column.DeclaredType)
	}
	for index := range arguments {
		if column.DeclaredType.Arguments[index] != arguments[index] {
			t.Fatalf(
				"column %s declaration = %#v",
				column.Name,
				column.DeclaredType,
			)
		}
	}
}

func assertMySQLToPostgresCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 2 || eventCount != 2 {
		t.Fatalf(
			"common-fixture row counts = (%d, %d)",
			accountCount,
			eventCount,
		)
	}
	var code, balance, payload, active string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", "balance"::text, encode("payload", 'hex'),
		        "document"->>'active'
		   FROM `+postgresQualified(namespace, accountsName)+
			` WHERE "id" = 7`,
	).Scan(&code, &balance, &payload, &active); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		payload != "00ff" ||
		active != "true" {
		t.Fatalf(
			"common-fixture account = (%q, %q, %q, %q)",
			code,
			balance,
			payload,
			active,
		)
	}
	var note, amount, occurred, binary string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "note", "amount"::text,
		        to_char("occurred_at", 'YYYY-MM-DD HH24:MI:SS.US'),
		        encode("payload", 'hex')
		   FROM `+postgresQualified(namespace, eventsName)+
			` WHERE "tenant_id" = 1
			    AND "event_id" = 9007199254740993`,
	).Scan(&note, &amount, &occurred, &binary); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		amount != "9.125" ||
		occurred != "2026-07-29 12:34:56.123456" ||
		binary != "deadbeef" {
		t.Fatalf(
			"common-fixture event = (%q, %q, %q, %q)",
			note,
			amount,
			occurred,
			binary,
		)
	}
}

func assertMySQLToPostgresDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
) {
	t.Helper()
	var (
		id      int64
		code    string
		balance string
		enabled int
		created bool
	)
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, accountsName)+
			` DEFAULT VALUES
			 RETURNING "id", "code", "balance"::text, "enabled",
			           "created_at" IS NOT NULL`,
	).Scan(&id, &code, &balance, &enabled, &created); err != nil {
		t.Fatalf("insert target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		enabled != 1 ||
		!created {
		t.Fatalf(
			"target defaults row = (%d, %q, %q, %d, %v)",
			id,
			code,
			balance,
			enabled,
			created,
		)
	}
}
