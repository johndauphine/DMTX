package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerToPostgresCommonFixtureLive(t *testing.T) {
	sqlServerDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	sqlServerCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if sqlServerDSN == "" || sqlServerCA == "" || postgresDSN == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, and DMTX_TEST_POSTGRES_DSN to run the SQL Server-to-PostgreSQL common fixture",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sqlServerDSN,
		sqlServerCA,
	)
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL common-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()
	sourceDatabase, err := engine.OpenSQLServer2022Source(
		ctx,
		sourceEndpoint,
	)
	if err != nil {
		t.Fatalf("open SQL Server common-fixture source: %v", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close SQL Server common-fixture source: %v", err)
		}
	})

	prefix := "dmtx_sc_" + strconv.FormatInt(time.Now().UnixNano(), 36)
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
				"DROP TABLE IF EXISTS "+
					sqlServerQualified("dbo", name),
			); err != nil {
				t.Errorf(
					"drop SQL Server common-fixture table %s: %v",
					name,
					err,
				)
			}
		}
	})
	createSQLServerCommonFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
	)
	insertSQLServerCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)

	namespace := "dmtx_mssql_common_" +
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

	sourceMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)
	assertSQLServerCommonSourceMetadata(
		t,
		sourceMetadata,
		prefix,
		accountsName,
		eventsName,
	)
	result, err := SQLServerToPostgresWithObserver(
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
		t.Fatalf("migrate SQL Server common fixture: %v", err)
	}
	if result.Tables != 2 ||
		result.Rows != 4 ||
		!result.Validated {
		t.Fatalf(
			"SQL Server common-fixture result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetMetadata := inspectPostgresSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
		eventsName,
	)
	assertSQLServerToPostgresCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
	)
	assertSQLServerToPostgresCommonRows(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
		eventsName,
	)
	assertSQLServerToPostgresDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		namespace,
		accountsName,
	)
}

func sqlServerCommonFixtureEndpoint(
	t *testing.T,
	dsn string,
	caPath string,
) config.Endpoint {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse SQL Server common-fixture DSN: %T", err)
	}
	if parsed.Scheme != "sqlserver" ||
		parsed.Hostname() == "" ||
		parsed.User == nil {
		t.Fatal("DMTX_TEST_MSSQL_DSN must be a SQL Server URI")
	}
	user := parsed.User.Username()
	password, hasPassword := parsed.User.Password()
	if user == "" || !hasPassword {
		t.Fatal("DMTX_TEST_MSSQL_DSN must include user credentials")
	}
	port := 1433
	if rawPort := parsed.Port(); rawPort != "" {
		port, err = strconv.Atoi(rawPort)
		if err != nil || port < 1 || port > 65535 {
			t.Fatal("DMTX_TEST_MSSQL_DSN has an invalid port")
		}
	}
	query := parsed.Query()
	if query.Get("database") == "" ||
		!strings.EqualFold(query.Get("encrypt"), "true") ||
		!strings.EqualFold(query.Get("guid conversion"), "true") ||
		query.Get("tlsmin") != "1.2" {
		t.Fatal(
			"DMTX_TEST_MSSQL_DSN must set database, encrypt=true, guid conversion=true, and tlsmin=1.2",
		)
	}
	certificate := query.Get("certificate")
	if certificate == "" ||
		filepath.Clean(certificate) != filepath.Clean(caPath) {
		t.Fatal(
			"DMTX_TEST_MSSQL_DSN certificate must match DMTX_TEST_MSSQL_CA",
		)
	}
	if _, err := os.ReadFile(caPath); err != nil {
		t.Fatalf("read DMTX_TEST_MSSQL_CA: %v", err)
	}
	return config.Endpoint{
		Type:      "mssql",
		Host:      parsed.Hostname(),
		Port:      port,
		Database:  query.Get("database"),
		User:      user,
		Password:  password,
		Schema:    "dbo",
		SSLMode:   "verify-full",
		TLSCAFile: caPath,
	}
}

func createSQLServerCommonFixture(
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
			[id] BIGINT IDENTITY(1,1) NOT NULL,
			[code] VARCHAR(24) COLLATE Latin1_General_100_BIN2_UTF8
				NOT NULL CONSTRAINT %s DEFAULT ('guest'),
			[balance] DECIMAL(12,2)
				NOT NULL CONSTRAINT %s DEFAULT (0.00),
			[ratio] REAL NOT NULL,
			[enabled] BIT
				NOT NULL CONSTRAINT %s DEFAULT (1),
			[payload] VARBINARY(16) NULL,
			[created_at] DATETIME2(3) NOT NULL,
			[description] VARCHAR(MAX)
				COLLATE Latin1_General_100_BIN2_UTF8 NULL,
			[external_id] UNIQUEIDENTIFIER NOT NULL,
			CONSTRAINT %s PRIMARY KEY CLUSTERED ([id] ASC),
			CONSTRAINT %s UNIQUE NONCLUSTERED ([id] ASC),
			CONSTRAINT %s CHECK ([balance] >= (0))
		)
	`,
		sqlServerQualified("dbo", accountsName),
		sqlServerIdentifier(prefix+"_code_df"),
		sqlServerIdentifier(prefix+"_balance_df"),
		sqlServerIdentifier(prefix+"_enabled_df"),
		sqlServerIdentifier(prefix+"_accounts_pk"),
		sqlServerIdentifier(prefix+"_id_uq"),
		sqlServerIdentifier(prefix+"_account_ck"),
	)
	if _, err := database.ExecContext(ctx, accountsDDL); err != nil {
		t.Fatalf("create SQL Server common-fixture accounts: %v", err)
	}
	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			[tenant_id] INT NOT NULL,
			[event_id] BIGINT NOT NULL,
			[account_id] BIGINT NOT NULL,
			[note] VARCHAR(80) COLLATE Latin1_General_100_BIN2_UTF8
				NOT NULL CONSTRAINT %s DEFAULT ('created'),
			[amount] DECIMAL(12,3)
				NOT NULL CONSTRAINT %s DEFAULT (0.000),
			[occurred_at] DATETIME2(6) NOT NULL,
			[observed_on] DATE NOT NULL,
			[local_time] TIME(6) NOT NULL,
			[payload] VARBINARY(MAX) NULL,
			CONSTRAINT %s PRIMARY KEY CLUSTERED
				([tenant_id] ASC, [event_id] ASC),
			CONSTRAINT %s FOREIGN KEY ([account_id])
				REFERENCES %s ([id])
				ON UPDATE CASCADE
				ON DELETE NO ACTION,
			CONSTRAINT %s CHECK ([event_id] > (0))
		)
	`,
		sqlServerQualified("dbo", eventsName),
		sqlServerIdentifier(prefix+"_note_df"),
		sqlServerIdentifier(prefix+"_amount_df"),
		sqlServerIdentifier(prefix+"_events_pk"),
		sqlServerIdentifier(prefix+"_account_fk"),
		sqlServerQualified("dbo", accountsName),
		sqlServerIdentifier(prefix+"_event_ck"),
	)
	if _, err := database.ExecContext(ctx, eventsDDL); err != nil {
		t.Fatalf("create SQL Server common-fixture events: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE NONCLUSTERED INDEX "+
			sqlServerIdentifier(prefix+"_occurred_idx")+
			" ON "+sqlServerQualified("dbo", eventsName)+
			" ([occurred_at] DESC)",
	); err != nil {
		t.Fatalf("create SQL Server common-fixture index: %v", err)
	}
}

func insertSQLServerCommonFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := sqlServerQualified("dbo", accountsName)
	accountsBatch := fmt.Sprintf(`
		SET IDENTITY_INSERT %s ON;
		INSERT INTO %s
			([id], [code], [balance], [ratio], [enabled], [payload],
			 [created_at], [description], [external_id])
		VALUES
			(7, N'東京', 12.34, CONVERT(real, 0.1), 1, 0x00ff,
			 CONVERT(datetime2(3), '2026-07-29T12:34:56.123'),
			 N'Zażółć gęślą jaźń — 東京',
			 CONVERT(uniqueidentifier,
			         '6F9619FF-8B86-D011-B42D-00C04FC964FF')),
			(11, N'emoji 😀', 0.00, CONVERT(real, -123.5), 0, NULL,
			 CONVERT(datetime2(3), '2026-07-29T23:59:59.999'),
			 NULL,
			 CONVERT(uniqueidentifier,
			         '00112233-4455-6677-8899-AABBCCDDEEFF'));
		SET IDENTITY_INSERT %s OFF;
		DBCC CHECKIDENT ('dbo.%s', RESEED, 41) WITH NO_INFOMSGS;
	`, accounts, accounts, accounts, accountsName)
	if _, err := database.ExecContext(ctx, accountsBatch); err != nil {
		_, _ = database.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+accounts+" OFF",
		)
		t.Fatalf("insert SQL Server common-fixture accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		fmt.Sprintf(`
			INSERT INTO %s
				([tenant_id], [event_id], [account_id], [note], [amount],
				 [occurred_at], [observed_on], [local_time], [payload])
			VALUES
				(1, 9007199254740993, 7,
				 N'Zażółć gęślą jaźń — 東京', 9.125,
				 CONVERT(datetime2(6), '2026-07-29T12:34:56.123456'),
				 CONVERT(date, '2026-07-29'),
				 CONVERT(time(6), '12:34:56.123456'), 0xdeadbeef),
				(1, 9007199254740995, 11,
				 N'emoji 😀', 0.000,
				 CONVERT(datetime2(6), '2026-07-29T23:59:59.999999'),
				 CONVERT(date, '2026-07-30'),
				 CONVERT(time(6), '23:59:59.999999'), NULL)
		`, sqlServerQualified("dbo", eventsName)),
	); err != nil {
		t.Fatalf("insert SQL Server common-fixture events: %v", err)
	}
}

func inspectSQLServerCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) map[string]schema.Table {
	t.Helper()
	result := make(map[string]schema.Table, 2)
	for _, name := range []string{accountsName, eventsName} {
		table, err := engine.InspectSQLServerTable(
			ctx,
			database,
			"dbo",
			name,
		)
		if err != nil {
			t.Fatalf(
				"inspect SQL Server common-fixture table %s: %v",
				name,
				err,
			)
		}
		result[name] = table
	}
	return result
}

func inspectPostgresSQLServerCommonFixture(
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

func assertSQLServerCommonSourceMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := tables[accountsName]
	if accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		len(accounts.Columns) != 9 ||
		accounts.Columns[0].Type != "bigint" ||
		accounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf(
			"SQL Server source accounts identity/columns = %#v %#v",
			accounts.Identity,
			accounts.Columns,
		)
	}
	assertPostgresCommonDeclaredType(t, accounts.Columns[1], "varchar", 24)
	assertPostgresCommonDeclaredType(t, accounts.Columns[2], "decimal", 12, 2)
	assertPostgresCommonDeclaredType(t, accounts.Columns[3], "real")
	assertPostgresCommonDeclaredType(t, accounts.Columns[6], "timestamp", 3)
	if accounts.Columns[3].Type != "real" ||
		accounts.Columns[4].Type != "boolean" ||
		accounts.Columns[5].Type != "blob" ||
		accounts.Columns[7].Type != "text" ||
		accounts.Columns[8].Type != "uuid" {
		t.Fatalf(
			"SQL Server source accounts mapped columns = %#v",
			accounts.Columns,
		)
	}
	if len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != prefix+"_id_uq" ||
		!accounts.Indexes[0].Unique ||
		len(accounts.Indexes[0].Columns) != 1 ||
		accounts.Indexes[0].Columns[0].Name != "id" ||
		accounts.Indexes[0].Columns[0].Collation != "" ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name != prefix+"_account_ck" {
		t.Fatalf(
			"SQL Server source accounts objects = indexes %#v checks %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}

	events := tables[eventsName]
	if len(events.Columns) != 9 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("SQL Server source events columns = %#v", events.Columns)
	}
	assertPostgresCommonDeclaredType(t, events.Columns[3], "varchar", 80)
	assertPostgresCommonDeclaredType(t, events.Columns[4], "decimal", 12, 3)
	assertPostgresCommonDeclaredType(t, events.Columns[5], "timestamp", 6)
	assertPostgresCommonDeclaredType(t, events.Columns[7], "time", 6)
	if len(events.Indexes) != 1 ||
		events.Indexes[0].Name != prefix+"_occurred_idx" ||
		len(events.Indexes[0].Columns) != 1 ||
		!events.Indexes[0].Columns[0].Descending ||
		len(events.Checks) != 1 ||
		events.Checks[0].Name != prefix+"_event_ck" ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"SQL Server source events objects = indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	assertSQLServerCommonForeignKey(
		t,
		events.ForeignKeys[0],
		prefix,
		accountsName,
	)
}

func assertSQLServerToPostgresCommonMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := tables[accountsName]
	if accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		len(accounts.Columns) != 9 ||
		accounts.Columns[0].Type != "bigint" ||
		accounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf(
			"PostgreSQL target accounts identity/columns = %#v %#v",
			accounts.Identity,
			accounts.Columns,
		)
	}
	assertPostgresCommonDeclaredType(t, accounts.Columns[1], "varchar", 24)
	assertPostgresCommonDeclaredType(t, accounts.Columns[2], "numeric", 12, 2)
	assertPostgresCommonDeclaredType(t, accounts.Columns[3], "real")
	assertPostgresCommonDeclaredType(t, accounts.Columns[6], "timestamp", 3)
	if accounts.Columns[3].Type != "real" ||
		accounts.Columns[4].Type != "boolean" ||
		accounts.Columns[5].Type != "bytea" ||
		accounts.Columns[7].Type != "text" ||
		accounts.Columns[8].Type != "uuid" ||
		len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != prefix+"_id_uq" ||
		!accounts.Indexes[0].Unique ||
		len(accounts.Indexes[0].Columns) != 1 ||
		accounts.Indexes[0].Columns[0].Name != "id" ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name != prefix+"_account_ck" {
		t.Fatalf(
			"PostgreSQL target accounts metadata = columns %#v indexes %#v checks %#v",
			accounts.Columns,
			accounts.Indexes,
			accounts.Checks,
		)
	}

	events := tables[eventsName]
	if len(events.Columns) != 9 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("PostgreSQL target events columns = %#v", events.Columns)
	}
	assertPostgresCommonDeclaredType(t, events.Columns[3], "varchar", 80)
	assertPostgresCommonDeclaredType(t, events.Columns[4], "numeric", 12, 3)
	assertPostgresCommonDeclaredType(t, events.Columns[5], "timestamp", 6)
	assertPostgresCommonDeclaredType(t, events.Columns[7], "time", 6)
	if len(events.Indexes) != 1 ||
		events.Indexes[0].Name != prefix+"_occurred_idx" ||
		len(events.Indexes[0].Columns) != 1 ||
		!events.Indexes[0].Columns[0].Descending ||
		len(events.Checks) != 1 ||
		events.Checks[0].Name != prefix+"_event_ck" ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"PostgreSQL target events objects = indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	assertSQLServerCommonForeignKey(
		t,
		events.ForeignKeys[0],
		prefix,
		accountsName,
	)
}

func assertSQLServerCommonForeignKey(
	t *testing.T,
	foreignKey schema.ForeignKey,
	prefix string,
	accountsName string,
) {
	t.Helper()
	if foreignKey.Name != prefix+"_account_fk" ||
		foreignKey.ReferencedTable != accountsName ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" ||
		foreignKey.Match != "SIMPLE" ||
		len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" {
		t.Fatalf("common-fixture foreign key = %#v", foreignKey)
	}
}

func assertSQLServerToPostgresCommonRows(
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
	var code, balance, payload, description, externalID string
	var ratio float64
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", "balance"::text, "ratio"::double precision,
		        encode("payload", 'hex'),
		        "description", "external_id"::text
		   FROM `+postgresQualified(namespace, accountsName)+
			` WHERE "id" = 7`,
	).Scan(
		&code,
		&balance,
		&ratio,
		&payload,
		&description,
		&externalID,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		ratio != float64(float32(0.1)) ||
		payload != "00ff" ||
		description != "Zażółć gęślą jaźń — 東京" ||
		externalID != "6f9619ff-8b86-d011-b42d-00c04fc964ff" {
		t.Fatalf(
			"common-fixture account = (%q, %q, %v, %q, %q, %q)",
			code,
			balance,
			ratio,
			payload,
			description,
			externalID,
		)
	}
	var accountPayloadNull, descriptionNull bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT "payload" IS NULL, "description" IS NULL
		   FROM `+postgresQualified(namespace, accountsName)+
			` WHERE "id" = 11`,
	).Scan(&accountPayloadNull, &descriptionNull); err != nil {
		t.Fatal(err)
	}
	if !accountPayloadNull || !descriptionNull {
		t.Fatal("common-fixture account NULL values were not preserved")
	}

	var note, amount, occurred, localTime, binary string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "note", "amount"::text,
		        to_char("occurred_at", 'YYYY-MM-DD HH24:MI:SS.US'),
		        to_char("local_time", 'HH24:MI:SS.US'),
		        encode("payload", 'hex')
		   FROM `+postgresQualified(namespace, eventsName)+
			` WHERE "tenant_id" = 1
			    AND "event_id" = 9007199254740993`,
	).Scan(&note, &amount, &occurred, &localTime, &binary); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		amount != "9.125" ||
		occurred != "2026-07-29 12:34:56.123456" ||
		localTime != "12:34:56.123456" ||
		binary != "deadbeef" {
		t.Fatalf(
			"common-fixture event = (%q, %q, %q, %q, %q)",
			note,
			amount,
			occurred,
			localTime,
			binary,
		)
	}
	var eventPayloadNull bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT "payload" IS NULL
		   FROM `+postgresQualified(namespace, eventsName)+
			` WHERE "tenant_id" = 1
			    AND "event_id" = 9007199254740995`,
	).Scan(&eventPayloadNull); err != nil {
		t.Fatal(err)
	}
	if !eventPayloadNull {
		t.Fatal("common-fixture event NULL payload was not preserved")
	}
}

func assertSQLServerToPostgresDefaultsAndIdentity(
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
		enabled bool
		created time.Time
	)
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, accountsName)+
			` ("external_id", "created_at", "ratio")
			 VALUES (
			   'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
			   TIMESTAMP '2026-07-29 00:00:00.000',
			   0.25
			 )
			 RETURNING "id", "code", "balance"::text, "enabled",
			           "created_at"`,
	).Scan(&id, &code, &balance, &enabled, &created); err != nil {
		t.Fatalf("insert target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		!enabled ||
		!created.Equal(time.Date(
			2026,
			time.July,
			29,
			0,
			0,
			0,
			0,
			time.UTC,
		)) {
		t.Fatalf(
			"target defaults row = (%d, %q, %q, %v, %v)",
			id,
			code,
			balance,
			enabled,
			created,
		)
	}
}
