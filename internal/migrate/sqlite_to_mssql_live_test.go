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
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteToSQLServerCommonFixtureLive(t *testing.T) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQLite-to-SQL Server common fixture",
		)
	}
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		caPath,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		180*time.Second,
	)
	defer cancel()
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"SQLite common-fixture target",
		targetEndpoint,
	)

	prefix := "dmtx_sm_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	invalidTemporalName := prefix + "_zy_invalid_temporal"
	unsupportedName := prefix + "_zz_unsupported"
	cleanupSQLServerNativeTables(
		t,
		targetDatabase,
		eventsName,
		accountsName,
	)

	sourcePath := createSQLiteSQLServerCommonFixture(
		t,
		ctx,
		prefix,
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

	result, err := SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"unacknowledged SQLite-to-SQL Server rebuild result = %+v, "+
				"error = %v, want %v",
			result,
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

	for _, name := range []string{eventsName, accountsName} {
		if _, err := targetDatabase.ExecContext(
			ctx,
			"DELETE FROM "+sqlServerQualified("dbo", name),
		); err != nil {
			t.Fatalf(
				"empty SQL Server acknowledgement-race target %s: %v",
				name,
				err,
			)
		}
	}
	raceObserver := &sqlServerDestructiveRaceSentinelObserver{
		database: targetDatabase,
		table:    accountsName,
	}
	result, err = SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		raceObserver,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"SQLite-to-SQL Server row added after preflight result = %+v, "+
				"error = %v, want %v",
			result,
			err,
			ErrDestructiveAcknowledgement,
		)
	}
	if raceObserver.beforeSets != 1 ||
		raceObserver.beforeTables != 0 ||
		raceObserver.afterTables != 0 {
		t.Fatalf(
			"SQLite-to-SQL Server acknowledgement-race observer = %+v",
			raceObserver,
		)
	}
	assertSQLiteSQLServerRaceSentinel(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate SQLite common fixture into SQL Server: %v",
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQLite-to-SQL Server result = %+v, "+
				"want 2 tables, 4 rows, validated",
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
	assertSQLiteToSQLServerCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
		41,
	)
	assertSQLiteToSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		2,
		"9007199254740993",
	)
	assertSQLServerNativeStaleTargetsWereReplaced(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertSQLiteToSQLServerDefaults(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	targetMetadata = inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertSQLiteToSQLServerCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
		41,
	)

	insertSQLiteSQLServerTargetOnlyAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
		99,
	)
	updateSQLiteSQLServerCommonFixture(
		t,
		ctx,
		sourcePath,
		accountsName,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	migrationConfig.Migration.DestructiveAcknowledged = false
	result, err = SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert SQLite common fixture into SQL Server: %v",
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQLite-to-SQL Server retained result = %+v, "+
				"want 2 tables, 4 rows, validated",
			result,
		)
	}
	assertSQLiteToSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		3,
		"23",
	)
	assertSQLiteSQLServerRetainedAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	targetMetadata = inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertSQLiteToSQLServerCommonMetadata(
		t,
		targetMetadata,
		prefix,
		accountsName,
		eventsName,
		99,
	)

	createSQLiteSQLServerInvalidTemporalLaterTable(
		t,
		ctx,
		sourcePath,
		invalidTemporalName,
	)
	beforeRejectedMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	migrationConfig.Migration = config.Migration{
		TargetMode: "drop_recreate",
		IncludeTables: []string{
			accountsName,
			eventsName,
			invalidTemporalName,
		},
		DestructiveAcknowledged: true,
	}
	rejectedObserver := &sqlServerNativePreflightObserver{}
	result, err = SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		rejectedObserver,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "non-TEXT storage class") {
		t.Fatalf(
			"invalid temporal SQLite row result = %+v, error = %v",
			result,
			err,
		)
	}
	assertSQLiteSQLServerRejectedBeforeMutation(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		beforeRejectedMetadata,
		result,
		rejectedObserver,
		"invalid temporal later SQLite table",
	)

	createSQLiteSQLServerUnsupportedLaterTable(
		t,
		ctx,
		sourcePath,
		unsupportedName,
	)
	beforeRejectedMetadata = inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
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
	rejectedObserver = &sqlServerNativePreflightObserver{}
	result, err = SQLiteToSQLServerWithObserver(
		ctx,
		migrationConfig,
		rejectedObserver,
	)
	if err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "json") {
		t.Fatalf(
			"unsupported later SQLite table result = %+v, error = %v",
			result,
			err,
		)
	}
	assertSQLiteSQLServerRejectedBeforeMutation(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		beforeRejectedMetadata,
		result,
		rejectedObserver,
		"unsupported later SQLite table",
	)
}

func assertSQLiteSQLServerRejectedBeforeMutation(
	t *testing.T,
	ctx context.Context,
	targetDatabase *sql.DB,
	accountsName string,
	eventsName string,
	beforeMetadata map[string]schema.Table,
	result Result,
	observer *sqlServerNativePreflightObserver,
	description string,
) {
	t.Helper()
	if result != (Result{}) ||
		observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"%s reached target mutation: result=%+v observer=%+v",
			description,
			result,
			observer,
		)
	}
	afterMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	if !reflect.DeepEqual(afterMetadata, beforeMetadata) {
		t.Fatalf(
			"%s changed SQL Server metadata:\nbefore: %#v\nafter:  %#v",
			description,
			beforeMetadata,
			afterMetadata,
		)
	}
	assertSQLiteToSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
		3,
		"23",
	)
	assertSQLiteSQLServerRetainedAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
}

func createSQLiteSQLServerCommonFixture(
	t *testing.T,
	ctx context.Context,
	prefix string,
	accountsName string,
	eventsName string,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sqlite-to-sql-server.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite-to-SQL Server source: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()

	if _, err := database.ExecContext(ctx, fmt.Sprintf(`
		PRAGMA foreign_keys = ON;
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			code VARCHAR(24) NOT NULL DEFAULT 'guest',
			exact_count DECIMAL(18,0) NOT NULL DEFAULT 0,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			description VARCHAR(80),
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
		t.Fatalf("create SQLite-to-SQL Server source schema: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(accountsName)+
			` (id, code, exact_count, enabled, description)
			 VALUES
			 (7, '東京', 9007199254740993, 1,
			  'Zażółć gęślą jaźń — 東京'),
			 (11, 'emoji 😀', 0, 0, NULL),
			 (41, 'deleted-frontier', 1, 1, 'deleted')`,
	); err != nil {
		t.Fatalf("insert SQLite-to-SQL Server accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"DELETE FROM "+quote(accountsName)+" WHERE id = 41",
	); err != nil {
		t.Fatalf("set SQLite-to-SQL Server identity frontier: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(eventsName)+
			` (tenant_id, event_id, account_id, note, exact_count, payload)
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京', 9007199254740995,
			  X'deadbeef'),
			 (1, 9007199254740995, 11, 'emoji 😀', 0, NULL)`,
	); err != nil {
		t.Fatalf("insert SQLite-to-SQL Server account events: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close SQLite-to-SQL Server source: %v", err)
	}
	closed = true
	return path
}

func updateSQLiteSQLServerCommonFixture(
	t *testing.T,
	ctx context.Context,
	path string,
	accountsName string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite-to-SQL Server upsert source: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"UPDATE "+quote(accountsName)+
			" SET exact_count = 23 WHERE id = 7",
	); err != nil {
		t.Fatalf("update SQLite-to-SQL Server upsert source: %v", err)
	}
}

func createSQLiteSQLServerUnsupportedLaterTable(
	t *testing.T,
	ctx context.Context,
	path string,
	name string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite unsupported-shape source: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+quote(name)+
			" (id BIGINT NOT NULL PRIMARY KEY, payload JSON NOT NULL)",
	); err != nil {
		t.Fatalf("create unsupported later SQLite table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(name)+
			` (id, payload) VALUES (1, '{"status":"unsupported"}')`,
	); err != nil {
		t.Fatalf("insert unsupported later SQLite row: %v", err)
	}
}

func createSQLiteSQLServerInvalidTemporalLaterTable(
	t *testing.T,
	ctx context.Context,
	path string,
	name string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite invalid-temporal source: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+quote(name)+
			" (id BIGINT NOT NULL PRIMARY KEY, occurred_on DATE NOT NULL)",
	); err != nil {
		t.Fatalf("create invalid temporal later SQLite table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+quote(name)+
			" (id, occurred_on) VALUES (1, X'323032362d30372d3330')",
	); err != nil {
		t.Fatalf("insert invalid temporal later SQLite row: %v", err)
	}
}

type sqliteSQLServerExpectedColumn struct {
	name       string
	canonical  string
	base       string
	arguments  []int
	nullable   bool
	primaryKey int
	defaultSQL string
}

func assertSQLiteToSQLServerCommonMetadata(
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
		accounts.Schema != "dbo" ||
		events.Schema != "dbo" ||
		accounts.SQLiteStrict ||
		accounts.SQLiteWithoutRowID ||
		events.SQLiteStrict ||
		events.SQLiteWithoutRowID ||
		accounts.MySQLCollation != "" ||
		events.MySQLCollation != "" {
		t.Fatalf(
			"SQLite-to-SQL Server table metadata = %#v",
			tables,
		)
	}
	if accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != identityFrontier {
		t.Fatalf(
			"SQLite-to-SQL Server identity = %#v, want frontier %d",
			accounts.Identity,
			identityFrontier,
		)
	}
	if events.Identity != nil {
		t.Fatalf(
			"SQLite-to-SQL Server events identity = %#v, want nil",
			events.Identity,
		)
	}
	assertSQLiteSQLServerColumns(
		t,
		accounts,
		[]sqliteSQLServerExpectedColumn{
			{
				name:       "id",
				canonical:  "bigint",
				base:       "bigint",
				primaryKey: 1,
			},
			{
				name:       "code",
				canonical:  "text",
				base:       "varchar",
				arguments:  []int{96},
				defaultSQL: "'guest'",
			},
			{
				name:       "exact_count",
				canonical:  "numeric",
				base:       "decimal",
				arguments:  []int{18, 0},
				defaultSQL: "0",
			},
			{
				name:       "enabled",
				canonical:  "boolean",
				base:       "bool",
				defaultSQL: "TRUE",
			},
			{
				name:      "description",
				canonical: "text",
				base:      "varchar",
				arguments: []int{320},
				nullable:  true,
			},
		},
	)
	assertSQLiteSQLServerColumns(
		t,
		events,
		[]sqliteSQLServerExpectedColumn{
			{
				name:       "tenant_id",
				canonical:  "bigint",
				base:       "bigint",
				primaryKey: 1,
			},
			{
				name:       "event_id",
				canonical:  "bigint",
				base:       "bigint",
				primaryKey: 2,
			},
			{
				name:      "account_id",
				canonical: "bigint",
				base:      "bigint",
			},
			{
				name:       "note",
				canonical:  "text",
				base:       "varchar",
				arguments:  []int{320},
				defaultSQL: "'created'",
			},
			{
				name:       "exact_count",
				canonical:  "numeric",
				base:       "decimal",
				arguments:  []int{18, 0},
				defaultSQL: "0",
			},
			{
				name:      "payload",
				canonical: "blob",
				base:      "blob",
				nullable:  true,
			},
		},
	)
	assertSQLiteSQLServerIndex(
		t,
		accounts,
		prefix+"_exact_idx",
		false,
		[]schema.IndexColumn{{
			Name:       "exact_count",
			Descending: true,
		}},
	)
	if len(accounts.Indexes) != 1 ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name == "" ||
		accounts.Checks[0].Expression.CanonicalSQL() !=
			`"exact_count" >= 0` {
		t.Fatalf(
			"SQLite-to-SQL Server accounts objects = "+
				"indexes %#v checks %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}
	assertSQLiteSQLServerIndex(
		t,
		events,
		prefix+"_account_idx",
		false,
		[]schema.IndexColumn{{Name: "account_id"}},
	)
	if len(events.Indexes) != 1 ||
		len(events.Checks) != 1 ||
		events.Checks[0].Name == "" ||
		events.Checks[0].Expression.CanonicalSQL() !=
			`"event_id" > 0` ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"SQLite-to-SQL Server events objects = "+
				"indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name == "" ||
		len(foreignKey.Columns) != 1 ||
		foreignKey.Columns[0] != "account_id" ||
		foreignKey.ReferencedTable != accountsName ||
		len(foreignKey.ReferencedColumns) != 1 ||
		foreignKey.ReferencedColumns[0] != "id" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" ||
		foreignKey.Match != "SIMPLE" {
		t.Fatalf(
			"SQLite-to-SQL Server foreign key = %#v",
			foreignKey,
		)
	}
}

func assertSQLiteSQLServerColumns(
	t *testing.T,
	table schema.Table,
	expected []sqliteSQLServerExpectedColumn,
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
			!reflect.DeepEqual(
				column.DeclaredType.Arguments,
				want.arguments,
			) {
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

func assertSQLiteSQLServerIndex(
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
			!reflect.DeepEqual(index.Columns, columns) {
			t.Fatalf(
				"SQL Server table %s index %s = %#v, "+
					"want unique=%t columns=%#v",
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

func assertSQLiteToSQLServerCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
	expectedAccounts int,
	expectedCount string,
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
	if accountCount != expectedAccounts || eventCount != 2 {
		t.Fatalf(
			"SQLite-to-SQL Server row counts = (%d, %d), want (%d, 2)",
			accountCount,
			eventCount,
			expectedAccounts,
		)
	}
	var (
		code        string
		exactCount  string
		enabled     bool
		description string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code],
		        CONVERT(varchar(64), [exact_count]),
		        [enabled],
		        [description]
		   FROM `+sqlServerQualified("dbo", accountsName)+
			` WHERE [id] = 7`,
	).Scan(
		&code,
		&exactCount,
		&enabled,
		&description,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		exactCount != expectedCount ||
		!enabled ||
		description != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf(
			"SQLite-to-SQL Server account = (%q, %q, %v, %q)",
			code,
			exactCount,
			enabled,
			description,
		)
	}
	var nullDescription any
	if err := database.QueryRowContext(
		ctx,
		"SELECT [description] FROM "+
			sqlServerQualified("dbo", accountsName)+
			" WHERE [id] = 11",
	).Scan(&nullDescription); err != nil {
		t.Fatal(err)
	}
	if nullDescription != nil {
		t.Fatalf(
			"SQLite NULL description became %#v",
			nullDescription,
		)
	}
	var (
		note            string
		eventCountValue string
		payload         []byte
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [note],
		        CONVERT(varchar(64), [exact_count]),
		        [payload]
		   FROM `+sqlServerQualified("dbo", eventsName)+
			` WHERE [tenant_id] = 1
			     AND [event_id] = 9007199254740993`,
	).Scan(&note, &eventCountValue, &payload); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		eventCountValue != "9007199254740995" ||
		!reflect.DeepEqual(payload, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf(
			"SQLite-to-SQL Server event = (%q, %q, %x)",
			note,
			eventCountValue,
			payload,
		)
	}
	var nullPayload any
	if err := database.QueryRowContext(
		ctx,
		"SELECT [payload] FROM "+
			sqlServerQualified("dbo", eventsName)+
			" WHERE [tenant_id] = 1 "+
			"AND [event_id] = 9007199254740995",
	).Scan(&nullPayload); err != nil {
		t.Fatal(err)
	}
	if nullPayload != nil {
		t.Fatalf("SQLite NULL payload became %#v", nullPayload)
	}
}

func assertSQLiteToSQLServerDefaults(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := sqlServerQualified("dbo", accountsName)
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve SQL Server session for defaults: %v", err)
	}
	connectionClosed := false
	defer func() {
		if !connectionClosed {
			_ = connection.Close()
		}
	}()
	if _, err := connection.ExecContext(
		ctx,
		"SET IDENTITY_INSERT "+accounts+" ON",
	); err != nil {
		t.Fatalf("enable SQL Server identity insert for defaults: %v", err)
	}
	identityEnabled := true
	defer func() {
		if identityEnabled {
			_, _ = connection.ExecContext(
				context.Background(),
				"SET IDENTITY_INSERT "+accounts+" OFF",
			)
		}
	}()
	if _, err := connection.ExecContext(
		ctx,
		"INSERT INTO "+accounts+" ([id]) VALUES (13)",
	); err != nil {
		t.Fatalf("insert SQL Server account defaults row: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		"SET IDENTITY_INSERT "+accounts+" OFF",
	); err != nil {
		t.Fatalf("disable SQL Server identity insert for defaults: %v", err)
	}
	identityEnabled = false
	if err := connection.Close(); err != nil {
		t.Fatalf("release SQL Server session for defaults: %v", err)
	}
	connectionClosed = true
	var (
		code       string
		exactCount string
		enabled    bool
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code],
		        CONVERT(varchar(64), [exact_count]),
		        [enabled]
		   FROM `+accounts+` WHERE [id] = 13`,
	).Scan(&code, &exactCount, &enabled); err != nil {
		t.Fatalf("read SQL Server account defaults row: %v", err)
	}
	if code != "guest" || exactCount != "0" || !enabled {
		t.Fatalf(
			"SQL Server account defaults = (%q, %q, %v)",
			code,
			exactCount,
			enabled,
		)
	}
	if _, err := database.ExecContext(
		ctx,
		"DELETE FROM "+accounts+" WHERE [id] = 13",
	); err != nil {
		t.Fatalf("remove SQL Server account defaults row: %v", err)
	}

	events := sqlServerQualified("dbo", eventsName)
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+events+
			" ([tenant_id], [event_id], [account_id]) "+
			"VALUES (2, 13, 7)",
	); err != nil {
		t.Fatalf("insert SQL Server event defaults row: %v", err)
	}
	var (
		note       string
		eventCount string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [note], CONVERT(varchar(64), [exact_count])
		   FROM `+events+
			` WHERE [tenant_id] = 2 AND [event_id] = 13`,
	).Scan(&note, &eventCount); err != nil {
		t.Fatalf("read SQL Server event defaults row: %v", err)
	}
	if note != "created" || eventCount != "0" {
		t.Fatalf(
			"SQL Server event defaults = (%q, %q)",
			note,
			eventCount,
		)
	}
	if _, err := database.ExecContext(
		ctx,
		"DELETE FROM "+events+
			" WHERE [tenant_id] = 2 AND [event_id] = 13",
	); err != nil {
		t.Fatalf("remove SQL Server event defaults row: %v", err)
	}
}

func insertSQLiteSQLServerTargetOnlyAccount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	id int64,
) {
	t.Helper()
	table := sqlServerQualified("dbo", accountsName)
	batch := fmt.Sprintf(`
		SET IDENTITY_INSERT %s ON;
		INSERT INTO %s
			([id], [code], [exact_count], [enabled], [description])
		VALUES
			(%d, 'target-only', 77, 1, 'retained');
		SET IDENTITY_INSERT %s OFF;
	`, table, table, id, table)
	if _, err := database.ExecContext(ctx, batch); err != nil {
		_, _ = database.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+table+" OFF",
		)
		t.Fatalf(
			"insert retained SQLite-to-SQL Server target account: %v",
			err,
		)
	}
}

func assertSQLiteSQLServerRetainedAccount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	var (
		code        string
		exactCount  string
		description string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code],
		        CONVERT(varchar(64), [exact_count]),
		        [description]
		   FROM `+sqlServerQualified("dbo", accountsName)+
			` WHERE [id] = 99`,
	).Scan(&code, &exactCount, &description); err != nil {
		t.Fatalf(
			"read retained SQLite-to-SQL Server target account: %v",
			err,
		)
	}
	if code != "target-only" ||
		exactCount != "77" ||
		description != "retained" {
		t.Fatalf(
			"retained SQLite-to-SQL Server account = (%q, %q, %q)",
			code,
			exactCount,
			description,
		)
	}
}

func assertSQLiteSQLServerRaceSentinel(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var marker string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [stale_marker] FROM "+
			sqlServerQualified("dbo", accountsName)+
			" WHERE [stale_id] = 99",
	).Scan(&marker); err != nil {
		t.Fatalf("read SQL Server race sentinel: %v", err)
	}
	if marker != "must disappear" {
		t.Fatalf("SQL Server race sentinel = %q", marker)
	}
	var events int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", eventsName),
	).Scan(&events); err != nil {
		t.Fatalf("count SQL Server race companion target: %v", err)
	}
	if events != 0 {
		t.Fatalf(
			"SQL Server race companion target rows = %d, want 0",
			events,
		)
	}
}
