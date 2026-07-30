package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestInspectSQLServer2022SourceSchemaLive(t *testing.T) {
	database := openSQLServer2022SourceLive(t)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live SQL Server source: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	prefix := "dmtx_ms_src_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	unsafeName := prefix + "_computed"
	cleanupSQLServerSourceTables(
		t,
		database,
		unsafeName,
		eventsName,
		accountsName,
	)

	accountsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			[id] BIGINT IDENTITY(1,1) NOT NULL,
			[code] VARCHAR(24) COLLATE Latin1_General_100_BIN2_UTF8
				NOT NULL CONSTRAINT %s DEFAULT ('guest'),
			[balance] DECIMAL(12,2) NOT NULL
				CONSTRAINT %s DEFAULT ((0.00)),
			[ratio] REAL NOT NULL,
			[enabled] BIT NOT NULL CONSTRAINT %s DEFAULT ((1)),
			[payload] VARBINARY(16) NULL
				CONSTRAINT %s DEFAULT (0x00FF),
			[created_at] DATETIME2(3) NOT NULL,
			[note] VARCHAR(80) COLLATE Latin1_General_100_BIN2_UTF8
				NOT NULL CONSTRAINT %s DEFAULT ('hello'),
			[external_id] UNIQUEIDENTIFIER NOT NULL,
			CONSTRAINT %s PRIMARY KEY CLUSTERED ([id] ASC),
			CONSTRAINT %s CHECK ([balance] >= (0))
		)
	`,
		sqlServerQualified("dbo", accountsName),
		sqlServerIdentifier(prefix+"_code_df"),
		sqlServerIdentifier(prefix+"_balance_df"),
		sqlServerIdentifier(prefix+"_enabled_df"),
		sqlServerIdentifier(prefix+"_payload_df"),
		sqlServerIdentifier(prefix+"_note_df"),
		sqlServerIdentifier(prefix+"_accounts_pk"),
		sqlServerIdentifier(prefix+"_balance_ck"),
	)
	execSQLServerSourceLiveDDL(t, database, accountsDDL)
	execSQLServerSourceLiveDDL(
		t,
		database,
		fmt.Sprintf(
			"CREATE UNIQUE NONCLUSTERED INDEX %s ON %s ([id] ASC)",
			sqlServerIdentifier(prefix+"_id_uq"),
			sqlServerQualified("dbo", accountsName),
		),
	)

	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			[tenant_id] INT NOT NULL,
			[event_id] BIGINT NOT NULL,
			[account_id] BIGINT NOT NULL,
			[note] VARCHAR(80) COLLATE Latin1_General_100_BIN2_UTF8 NULL
				CONSTRAINT %s DEFAULT ('created'),
			[amount] NUMERIC(12,3) NOT NULL
				CONSTRAINT %s DEFAULT ((0.000)),
			[occurred_at] DATETIME2(6) NOT NULL,
			[occurred_on] DATE NOT NULL,
			[local_time] TIME(6) NOT NULL,
			[payload] VARBINARY(MAX) NULL,
			CONSTRAINT %s PRIMARY KEY CLUSTERED (
				[tenant_id] ASC,
				[event_id] ASC
			),
			CONSTRAINT %s CHECK ([event_id] > (0)),
			CONSTRAINT %s FOREIGN KEY ([account_id])
				REFERENCES %s ([id])
				ON UPDATE CASCADE
				ON DELETE NO ACTION
		)
	`,
		sqlServerQualified("dbo", eventsName),
		sqlServerIdentifier(prefix+"_event_note_df"),
		sqlServerIdentifier(prefix+"_amount_df"),
		sqlServerIdentifier(prefix+"_events_pk"),
		sqlServerIdentifier(prefix+"_event_ck"),
		sqlServerIdentifier(prefix+"_account_fk"),
		sqlServerQualified("dbo", accountsName),
	)
	execSQLServerSourceLiveDDL(t, database, eventsDDL)
	execSQLServerSourceLiveDDL(
		t,
		database,
		fmt.Sprintf(
			"CREATE NONCLUSTERED INDEX %s ON %s ([occurred_at] DESC)",
			sqlServerIdentifier(prefix+"_occurred_idx"),
			sqlServerQualified("dbo", eventsName),
		),
	)

	execSQLServerSourceLiveDDL(
		t,
		database,
		fmt.Sprintf(`
			SET IDENTITY_INSERT %s ON;
			INSERT INTO %s (
				[id],
				[code],
				[balance],
				[ratio],
				[enabled],
				[payload],
				[created_at],
				[note],
				[external_id]
			) VALUES (
				41,
				'account-41',
				1234.50,
				CONVERT(real, 0.1),
				1,
				0xDEADBEEF,
				'2026-07-29T12:34:56.123',
				N'naïve café 東京',
				CONVERT(uniqueidentifier,
				        '6F9619FF-8B86-D011-B42D-00C04FC964FF')
			);
			SET IDENTITY_INSERT %s OFF;
		`,
			sqlServerQualified("dbo", accountsName),
			sqlServerQualified("dbo", accountsName),
			sqlServerQualified("dbo", accountsName),
		),
	)
	execSQLServerSourceLiveDDL(
		t,
		database,
		fmt.Sprintf(`
			INSERT INTO %s (
				[tenant_id],
				[event_id],
				[account_id],
				[note],
				[amount],
				[occurred_at],
				[occurred_on],
				[local_time],
				[payload]
			) VALUES (
				7,
				9001,
				41,
				'created',
				9.125,
				'2026-07-29T12:34:56.123456',
				'2026-07-29',
				'12:34:56.123456',
				0xCAFE
			)
		`, sqlServerQualified("dbo", eventsName)),
	)

	if err := VerifySQLServer2022Source(ctx, database); err != nil {
		t.Fatalf("verify live SQL Server source: %v", err)
	}
	accounts, err := InspectSQLServerTable(
		ctx,
		database,
		"dbo",
		accountsName,
	)
	if err != nil {
		t.Fatalf("inspect live SQL Server accounts: %v", err)
	}
	events, err := InspectSQLServerTable(
		ctx,
		database,
		"dbo",
		eventsName,
	)
	if err != nil {
		t.Fatalf("inspect live SQL Server events: %v", err)
	}
	assertSQLServer2022AccountsDiscovery(t, accounts, prefix)
	assertSQLServer2022EventsDiscovery(
		t,
		events,
		accountsName,
		prefix,
	)

	execSQLServerSourceLiveDDL(
		t,
		database,
		fmt.Sprintf(`
			CREATE TABLE %s (
				[id] BIGINT NOT NULL,
				[doubled] AS ([id] * (2)),
				CONSTRAINT %s PRIMARY KEY ([id])
			)
		`,
			sqlServerQualified("dbo", unsafeName),
			sqlServerIdentifier(prefix+"_computed_pk"),
		),
	)
	_, err = InspectSQLServerTable(
		ctx,
		database,
		"dbo",
		unsafeName,
	)
	var policy *schema.PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("computed-column error = %v, want PolicyError", err)
	}
}

func openSQLServer2022SourceLive(t *testing.T) *sql.DB {
	t.Helper()
	rawDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if rawDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA to run SQL Server 2022 source discovery tests",
		)
	}
	parsed, err := url.Parse(rawDSN)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MSSQL_DSN: %v", err)
	}
	password, hasPassword := parsed.User.Password()
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("parse SQL Server live port: %v", err)
	}
	query := parsed.Query()
	if query.Get("encrypt") != "true" ||
		query.Get("guid conversion") != "true" ||
		query.Get("tlsmin") != "1.2" ||
		query.Get("certificate") != caPath {
		t.Fatal("DMTX_TEST_MSSQL_DSN must require verified TLS 1.2")
	}
	if parsed.User.Username() == "" || !hasPassword ||
		parsed.Hostname() == "" || query.Get("database") == "" {
		t.Fatal("DMTX_TEST_MSSQL_DSN is incomplete")
	}
	database, err := OpenSQLServer2022Source(
		context.Background(),
		config.Endpoint{
			Type:      "mssql",
			Host:      parsed.Hostname(),
			Port:      port,
			Database:  query.Get("database"),
			User:      parsed.User.Username(),
			Password:  password,
			Schema:    "dbo",
			SSLMode:   "verify-full",
			TLSCAFile: caPath,
		},
	)
	if err != nil {
		t.Fatalf("open verified SQL Server 2022 source: %v", err)
	}
	return database
}

func cleanupSQLServerSourceTables(
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
				"DROP TABLE IF EXISTS "+
					sqlServerQualified("dbo", name),
			); err != nil {
				t.Errorf("drop SQL Server source table %s: %v", name, err)
			}
		}
	})
}

func execSQLServerSourceLiveDDL(
	t *testing.T,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		context.Background(),
		statement,
	); err != nil {
		t.Fatalf("execute SQL Server source fixture DDL: %v", err)
	}
}

func assertSQLServer2022AccountsDiscovery(
	t *testing.T,
	table schema.Table,
	prefix string,
) {
	t.Helper()
	if table.Identity == nil ||
		table.Identity.Column != "id" ||
		table.Identity.Generation != schema.IdentityByDefault ||
		table.Identity.Frontier == nil ||
		*table.Identity.Frontier != 41 {
		t.Fatalf("accounts identity = %#v", table.Identity)
	}
	if len(table.Columns) != 9 ||
		!table.Columns[0].PrimaryKey ||
		table.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("accounts columns = %#v", table.Columns)
	}
	assertSQLServerLiveDeclaredType(t, table.Columns[1], "varchar", 24)
	assertSQLServerLiveDeclaredType(t, table.Columns[2], "decimal", 12, 2)
	assertSQLServerLiveDeclaredType(t, table.Columns[3], "real")
	assertSQLServerLiveDeclaredType(t, table.Columns[5], "varbinary", 16)
	assertSQLServerLiveDeclaredType(t, table.Columns[6], "timestamp", 3)
	assertSQLServerLiveDeclaredType(t, table.Columns[7], "varchar", 80)
	assertSQLServerLiveDeclaredType(t, table.Columns[8], "uuid")
	if table.Columns[3].Type != "real" ||
		sqlServerLiveDefault(table.Columns[1]) != "'guest'" ||
		sqlServerLiveDefault(table.Columns[2]) != "0" ||
		table.Columns[3].Default != nil ||
		sqlServerLiveDefault(table.Columns[4]) != "TRUE" ||
		sqlServerLiveDefault(table.Columns[5]) != "X'00ff'" ||
		table.Columns[6].Default != nil ||
		sqlServerLiveDefault(table.Columns[7]) != "'hello'" {
		t.Fatalf("accounts defaults = %#v", table.Columns)
	}
	if len(table.Indexes) != 1 ||
		table.Indexes[0].Name != prefix+"_id_uq" ||
		!table.Indexes[0].Unique ||
		len(table.Indexes[0].Columns) != 1 ||
		table.Indexes[0].Columns[0].Name != "id" ||
		table.Indexes[0].Columns[0].Collation != "" {
		t.Fatalf("accounts indexes = %#v", table.Indexes)
	}
	if len(table.Checks) != 1 ||
		table.Checks[0].Name != prefix+"_balance_ck" {
		t.Fatalf("accounts checks = %#v", table.Checks)
	}
}

func assertSQLServer2022EventsDiscovery(
	t *testing.T,
	table schema.Table,
	accountsName string,
	prefix string,
) {
	t.Helper()
	if len(table.Columns) != 9 ||
		table.Columns[0].PrimaryKeyPosition != 1 ||
		table.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("events columns = %#v", table.Columns)
	}
	assertSQLServerLiveDeclaredType(t, table.Columns[3], "varchar", 80)
	assertSQLServerLiveDeclaredType(t, table.Columns[4], "numeric", 12, 3)
	assertSQLServerLiveDeclaredType(t, table.Columns[5], "timestamp", 6)
	assertSQLServerLiveDeclaredType(t, table.Columns[7], "time", 6)
	assertSQLServerLiveDeclaredType(t, table.Columns[8], "blob")
	if len(table.Indexes) != 1 ||
		table.Indexes[0].Name != prefix+"_occurred_idx" ||
		len(table.Indexes[0].Columns) != 1 ||
		!table.Indexes[0].Columns[0].Descending {
		t.Fatalf("events indexes = %#v", table.Indexes)
	}
	if len(table.Checks) != 1 ||
		table.Checks[0].Name != prefix+"_event_ck" {
		t.Fatalf("events checks = %#v", table.Checks)
	}
	if len(table.ForeignKeys) != 1 {
		t.Fatalf("events foreign keys = %#v", table.ForeignKeys)
	}
	foreignKey := table.ForeignKeys[0]
	if foreignKey.Name != prefix+"_account_fk" ||
		len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		foreignKey.ReferencedTable != accountsName ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" ||
		foreignKey.Match != "SIMPLE" {
		t.Fatalf("events foreign key = %#v", foreignKey)
	}
}

func assertSQLServerLiveDeclaredType(
	t *testing.T,
	column schema.Column,
	base string,
	arguments ...int,
) {
	t.Helper()
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		fmt.Sprint(column.DeclaredType.Arguments) != fmt.Sprint(arguments) {
		t.Fatalf(
			"column %s declared type = %#v, want %s%v",
			column.Name,
			column.DeclaredType,
			base,
			arguments,
		)
	}
}

func sqlServerLiveDefault(column schema.Column) string {
	if column.Default == nil {
		return "<nil>"
	}
	return column.Default.CanonicalSQL()
}

func sqlServerQualified(namespace, name string) string {
	return sqlServerIdentifier(namespace) + "." +
		sqlServerIdentifier(name)
}

func sqlServerIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}
