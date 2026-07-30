package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

func TestSQLServerToSQLiteCommonFixtureLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	sourceCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	if sourceDSN == "" || sourceCA == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQL Server-to-SQLite common fixture",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sourceDSN,
		sourceCA,
	)
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
		t.Fatalf("open SQL Server-to-SQLite source: %v", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close SQL Server-to-SQLite source: %v", err)
		}
	})

	prefix := "dmtx_ss_" + strconv.FormatInt(time.Now().UnixNano(), 36)
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
					"drop SQL Server-to-SQLite source table %s: %v",
					name,
					err,
				)
			}
		}
	})
	createSQLServerSQLiteFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
	)
	insertSQLServerSQLiteFixture(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)

	targetPath := filepath.Join(t.TempDir(), "target.db")
	result, err := SQLServerToSQLiteWithObserver(
		ctx,
		config.Config{
			Source: sourceEndpoint,
			Target: config.Endpoint{
				Type:     "sqlite",
				Database: targetPath,
			},
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: []string{accountsName, eventsName},
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("migrate SQL Server-to-SQLite common fixture: %v", err)
	}
	if result.Tables != 2 ||
		result.Rows != 4 ||
		!result.Validated {
		t.Fatalf(
			"SQL Server-to-SQLite result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetDatabase, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatalf("open SQLite common-fixture target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetDatabase.Close(); err != nil {
			t.Errorf("close SQLite common-fixture target: %v", err)
		}
	})
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify SQLite common-fixture target: %v", err)
	}
	assertSQLServerSQLiteFixtureMetadata(
		t,
		ctx,
		targetDatabase,
		prefix,
		accountsName,
		eventsName,
	)
	assertSQLServerSQLiteFixtureRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertSQLServerSQLiteFixtureDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
}

func createSQLServerSQLiteFixture(
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
			[exact_count] DECIMAL(18,0)
				NOT NULL CONSTRAINT %s DEFAULT (0),
			[ratio] REAL NOT NULL,
			[enabled] BIT
				NOT NULL CONSTRAINT %s DEFAULT (1),
			[payload] VARBINARY(16) NULL,
			[created_at] DATETIME2(3) NOT NULL,
			[description] VARCHAR(MAX)
				COLLATE Latin1_General_100_BIN2_UTF8 NULL,
			[external_id] UNIQUEIDENTIFIER NOT NULL,
			CONSTRAINT %s PRIMARY KEY CLUSTERED ([id] ASC),
			CONSTRAINT %s CHECK ([exact_count] >= (0))
		)
	`,
		sqlServerQualified("dbo", accountsName),
		sqlServerIdentifier(prefix+"_code_df"),
		sqlServerIdentifier(prefix+"_count_df"),
		sqlServerIdentifier(prefix+"_enabled_df"),
		sqlServerIdentifier(prefix+"_accounts_pk"),
		sqlServerIdentifier(prefix+"_accounts_ck"),
	)
	if _, err := database.ExecContext(ctx, accountsDDL); err != nil {
		t.Fatalf("create SQL Server-to-SQLite accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE UNIQUE NONCLUSTERED INDEX "+
			sqlServerIdentifier(prefix+"_id_uq")+
			" ON "+sqlServerQualified("dbo", accountsName)+
			" ([id] ASC)",
	); err != nil {
		t.Fatalf("create SQL Server-to-SQLite account index: %v", err)
	}

	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			[tenant_id] INT NOT NULL,
			[event_id] BIGINT NOT NULL,
			[account_id] BIGINT NOT NULL,
			[note] VARCHAR(80) COLLATE Latin1_General_100_BIN2_UTF8
				NOT NULL CONSTRAINT %s DEFAULT ('created'),
			[exact_count] NUMERIC(18,0)
				NOT NULL CONSTRAINT %s DEFAULT (0),
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
		sqlServerIdentifier(prefix+"_event_count_df"),
		sqlServerIdentifier(prefix+"_events_pk"),
		sqlServerIdentifier(prefix+"_account_fk"),
		sqlServerQualified("dbo", accountsName),
		sqlServerIdentifier(prefix+"_events_ck"),
	)
	if _, err := database.ExecContext(ctx, eventsDDL); err != nil {
		t.Fatalf("create SQL Server-to-SQLite events: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE NONCLUSTERED INDEX "+
			sqlServerIdentifier(prefix+"_occurred_idx")+
			" ON "+sqlServerQualified("dbo", eventsName)+
			" ([occurred_at] DESC)",
	); err != nil {
		t.Fatalf("create SQL Server-to-SQLite event index: %v", err)
	}
}

func insertSQLServerSQLiteFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := sqlServerQualified("dbo", accountsName)
	if _, err := database.ExecContext(
		ctx,
		fmt.Sprintf(`
			SET IDENTITY_INSERT %s ON;
			INSERT INTO %s
				([id], [code], [exact_count], [ratio], [enabled],
				 [payload], [created_at], [description], [external_id])
			VALUES
				(7, N'東京', 9007199254740993, CONVERT(real, 0.1), 1,
				 0x00ff,
				 CONVERT(datetime2(3), '2026-07-29T12:34:56.123'),
				 N'Zażółć gęślą jaźń — 東京',
				 CONVERT(uniqueidentifier,
				         '6F9619FF-8B86-D011-B42D-00C04FC964FF')),
				(11, N'emoji 😀', 0, CONVERT(real, -123.5), 0,
				 NULL,
				 CONVERT(datetime2(3), '2026-07-29T23:59:59.999'),
				 NULL,
				 CONVERT(uniqueidentifier,
				         '00112233-4455-6677-8899-AABBCCDDEEFF'));
			SET IDENTITY_INSERT %s OFF;
			DBCC CHECKIDENT ('dbo.%s', RESEED, 41) WITH NO_INFOMSGS;
		`, accounts, accounts, accounts, accountsName),
	); err != nil {
		_, _ = database.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+accounts+" OFF",
		)
		t.Fatalf("insert SQL Server-to-SQLite accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		fmt.Sprintf(`
			INSERT INTO %s
				([tenant_id], [event_id], [account_id], [note],
				 [exact_count], [occurred_at], [observed_on],
				 [local_time], [payload])
			VALUES
				(1, 9007199254740993, 7,
				 N'Zażółć gęślą jaźń — 東京', 9007199254740995,
				 CONVERT(datetime2(6), '2026-07-29T12:34:56.123456'),
				 CONVERT(date, '2026-07-29'),
				 CONVERT(time(6), '12:34:56.123456'), 0xdeadbeef),
				(1, 9007199254740995, 11, N'emoji 😀', 0,
				 CONVERT(datetime2(6), '2026-07-29T23:59:59.999999'),
				 CONVERT(date, '2026-07-30'),
				 CONVERT(time(6), '23:59:59.999999'), NULL)
		`, sqlServerQualified("dbo", eventsName)),
	); err != nil {
		t.Fatalf("insert SQL Server-to-SQLite events: %v", err)
	}
}

func assertSQLServerSQLiteFixtureMetadata(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts, _, err := inspectSQLiteSchema(ctx, database, accountsName)
	if err != nil {
		t.Fatalf("inspect SQLite accounts: %v", err)
	}
	if accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		len(accounts.Columns) != 9 ||
		accounts.Columns[2].DeclaredType == nil ||
		accounts.Columns[2].DeclaredType.Base != "bigint" ||
		len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != prefix+"_id_uq" ||
		!accounts.Indexes[0].Unique ||
		len(accounts.Checks) != 2 {
		t.Fatalf("SQLite accounts metadata = %#v", accounts)
	}
	events, _, err := inspectSQLiteSchema(ctx, database, eventsName)
	if err != nil {
		t.Fatalf("inspect SQLite events: %v", err)
	}
	if len(events.Columns) != 9 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 ||
		events.Columns[4].DeclaredType == nil ||
		events.Columns[4].DeclaredType.Base != "bigint" ||
		len(events.Indexes) != 1 ||
		events.Indexes[0].Name != prefix+"_occurred_idx" ||
		!events.Indexes[0].Columns[0].Descending ||
		len(events.ForeignKeys) != 1 ||
		events.ForeignKeys[0].ReferencedTable != accountsName ||
		events.ForeignKeys[0].OnUpdate != "CASCADE" ||
		events.ForeignKeys[0].OnDelete != "NO ACTION" {
		t.Fatalf("SQLite events metadata = %#v", events)
	}
}

func assertSQLServerSQLiteFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var code, exactCount, payload, description, externalID string
	var ratio float64
	var enabled int64
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", CAST("exact_count" AS TEXT), "ratio",
		        "enabled", hex("payload"), "description", "external_id"
		   FROM `+quote(accountsName)+` WHERE "id" = 7`,
	).Scan(
		&code,
		&exactCount,
		&ratio,
		&enabled,
		&payload,
		&description,
		&externalID,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		exactCount != "9007199254740993" ||
		ratio != float64(float32(0.1)) ||
		enabled != 1 ||
		payload != "00FF" ||
		description != "Zażółć gęślą jaźń — 東京" ||
		externalID != "6f9619ff-8b86-d011-b42d-00c04fc964ff" {
		t.Fatalf(
			"SQLite account = (%q, %q, %v, %d, %q, %q, %q)",
			code,
			exactCount,
			ratio,
			enabled,
			payload,
			description,
			externalID,
		)
	}
	var accountPayloadNull, descriptionNull bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT "payload" IS NULL, "description" IS NULL
		   FROM `+quote(accountsName)+` WHERE "id" = 11`,
	).Scan(&accountPayloadNull, &descriptionNull); err != nil {
		t.Fatal(err)
	}
	if !accountPayloadNull || !descriptionNull {
		t.Fatal("SQLite account NULL values were not preserved")
	}

	var eventCount, occurred, observed, localTime, binary string
	if err := database.QueryRowContext(
		ctx,
		`SELECT CAST("exact_count" AS TEXT),
		        strftime('%Y-%m-%d %H:%M:%f', "occurred_at"),
		        strftime('%Y-%m-%d', "observed_on"),
		        "local_time", hex("payload")
		   FROM `+quote(eventsName)+
			` WHERE "tenant_id" = 1 AND "event_id" = 9007199254740993`,
	).Scan(
		&eventCount,
		&occurred,
		&observed,
		&localTime,
		&binary,
	); err != nil {
		t.Fatal(err)
	}
	if eventCount != "9007199254740995" ||
		occurred != "2026-07-29 12:34:56.123" ||
		observed != "2026-07-29" ||
		localTime != "12:34:56.123456" ||
		binary != "DEADBEEF" {
		t.Fatalf(
			"SQLite event = (%q, %q, %q, %q, %q)",
			eventCount,
			occurred,
			observed,
			localTime,
			binary,
		)
	}
}

func assertSQLServerSQLiteFixtureDefaultsAndIdentity(
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
		t.Fatalf("read SQLite identity frontier: %v", err)
	}
	if sequence != 41 {
		t.Fatalf("SQLite identity frontier = %d, want 41", sequence)
	}
	if _, err := database.ExecContext(
		ctx,
		`INSERT INTO `+quote(accountsName)+`
		    ("exact_count", "ratio", "payload", "created_at",
		     "description", "external_id")
		 VALUES
		    (5, 1.5, NULL, '2026-07-30 01:02:03.004',
		     NULL, '11111111-2222-3333-4444-555555555555')`,
	); err != nil {
		t.Fatalf("exercise SQLite defaults and identity: %v", err)
	}
	var id, enabled int64
	var code, exactCount string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "id", "code", CAST("exact_count" AS TEXT), "enabled"
		   FROM `+quote(accountsName)+` WHERE "id" = 42`,
	).Scan(&id, &code, &exactCount, &enabled); err != nil {
		t.Fatalf("read SQLite generated row: %v", err)
	}
	if id != 42 || code != "guest" ||
		exactCount != "5" || enabled != 1 {
		t.Fatalf(
			"SQLite generated row = (%d, %q, %q, %d)",
			id,
			code,
			exactCount,
			enabled,
		)
	}
}
