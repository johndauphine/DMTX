package migrate

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerToMySQLCommonFixtureLive(t *testing.T) {
	testSQLServerToMySQLFamilyCommonFixtureLive(
		t,
		mysqlNativeLiveFixture{
			name:        "Oracle MySQL",
			targetEnv:   "DMTX_TEST_MYSQL_TARGET_DSN",
			caEnv:       "DMTX_TEST_MYSQL_CA",
			tlsConfig:   "dmtx_test",
			namePrefix:  "dmtx_sm_",
			collation:   "utf8mb4_0900_bin",
			refreshInfo: true,
		},
		engine.MySQLServerFlavorOracle80,
	)
}

func TestSQLServerToMariaDBCommonFixtureLive(t *testing.T) {
	testSQLServerToMySQLFamilyCommonFixtureLive(
		t,
		mysqlNativeLiveFixture{
			name:       "MariaDB",
			targetEnv:  "DMTX_TEST_MARIADB_TARGET_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			tlsConfig:  "dmtx_mariadb_test",
			namePrefix: "dmtx_sma_",
			collation:  "utf8mb4_nopad_bin",
		},
		engine.MySQLServerFlavorMariaDB1011,
	)
}

func testSQLServerToMySQLFamilyCommonFixtureLive(
	t *testing.T,
	targetFixture mysqlNativeLiveFixture,
	expectedFlavor engine.MySQLServerFlavor,
) {
	t.Helper()
	sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	sourceCA := os.Getenv("DMTX_TEST_MSSQL_CA")
	targetDSN := os.Getenv(targetFixture.targetEnv)
	targetCA := os.Getenv(targetFixture.caEnv)
	if sourceDSN == "" || sourceCA == "" ||
		targetDSN == "" || targetCA == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, " +
				targetFixture.targetEnv + ", and " +
				targetFixture.caEnv + " to run SQL Server-to-" +
				targetFixture.name + " common fixture",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sourceDSN,
		sourceCA,
	)
	registerMySQLCommonFixtureTLSNamed(
		t,
		targetCA,
		targetFixture.tlsConfig,
	)
	targetConfig := parseMySQLNativeTargetDSNForTLS(
		t,
		"target",
		targetDSN,
		targetFixture.tlsConfig,
	)
	targetEndpoint := mysqlNativeTargetEndpoint(
		t,
		targetConfig,
		targetCA,
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
		t.Fatalf("open SQL Server common-fixture source: %v", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close SQL Server common-fixture source: %v", err)
		}
	})
	targetDatabase := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		"target",
		targetDSN,
		targetFixture.refreshInfo,
	)
	targetFlavor, err := engine.DetectMySQLServerFlavor(
		ctx,
		targetDatabase,
	)
	if err != nil {
		t.Fatalf("detect %s target flavor: %v", targetFixture.name, err)
	}
	if targetFlavor != expectedFlavor {
		t.Fatalf(
			"%s target flavor = %q, want %q",
			targetFixture.name,
			targetFlavor,
			expectedFlavor,
		)
	}

	prefix := targetFixture.namePrefix +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupSQLServerToMySQLSourceTables(
		t,
		sourceDatabase,
		eventsName,
		accountsName,
	)
	cleanupMySQLNativeTables(
		t,
		targetDatabase,
		eventsName,
		accountsName,
	)
	seedMySQLNativeReplacementTargets(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
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

	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := SQLServerToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate SQL Server common fixture into %s: %v",
			targetFixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQL Server-to-%s result = %+v, want 2 tables, 4 rows, validated",
			targetFixture.name,
			result,
		)
	}
	targetMetadata := inspectMySQLCommonFixture(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		accountsName,
		eventsName,
	)
	assertSQLServerToMySQLMetadata(
		t,
		sourceMetadata,
		targetMetadata,
		targetConfig.DBName,
		targetFlavor,
		targetFixture.collation,
		prefix,
		accountsName,
		eventsName,
	)
	assertSQLServerToMySQLRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLNativeStaleTargetsWereReplaced(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		accountsName,
		eventsName,
	)
	assertSQLServerToMySQLDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	prepareSQLServerToMySQLRetainedUpsert(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		accountsName,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	result, err = SQLServerToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained-upsert SQL Server common fixture into %s: %v",
			targetFixture.name,
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQL Server-to-%s retained result = %+v, want 2 tables, 4 rows, validated",
			targetFixture.name,
			result,
		)
	}
	assertSQLServerToMySQLRetainedRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
	assertSQLServerToMySQLMismatchPreflight(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		migrationConfig,
		accountsName,
		eventsName,
		prefix+"_occurred_idx",
	)
	assertSQLServerToMySQLValuePreflight(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		migrationConfig,
		prefix+"_unsafe_date",
	)
}

func cleanupSQLServerToMySQLSourceTables(
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
				t.Errorf(
					"drop SQL Server-to-MySQL fixture table %s: %v",
					name,
					err,
				)
			}
		}
	})
}

func assertSQLServerToMySQLMetadata(
	t *testing.T,
	source map[string]schema.Table,
	target map[string]schema.Table,
	targetNamespace string,
	targetFlavor engine.MySQLServerFlavor,
	targetCollation string,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	assertSQLServerToMySQLKnownFixtureMetadata(
		t,
		target,
		targetNamespace,
		targetCollation,
		prefix,
		accountsName,
		eventsName,
	)
	sourceTables := make([]schema.Table, 0, len(source))
	for _, name := range []string{
		sortedSQLServerFixtureTableName(source, false),
		sortedSQLServerFixtureTableName(source, true),
	} {
		sourceTables = append(sourceTables, source[name])
	}
	adapter := &mysqlTargetAdapter{
		flavor:    targetFlavor,
		namespace: targetNamespace,
	}
	planned, err := adapter.PlanTables(
		"mssql",
		sourceTables,
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("plan expected SQL Server-to-MySQL metadata: %v", err)
	}
	for _, expected := range planned {
		actual, ok := target[expected.Name]
		if !ok {
			t.Fatalf("target metadata is missing table %s", expected.Name)
		}
		if err := validateMySQLRetainedTableShape(
			expected,
			actual,
		); err != nil {
			t.Fatalf(
				"SQL Server-to-MySQL metadata for %s differs: %v\nplanned: %#v\nactual: %#v",
				expected.Name,
				err,
				expected,
				actual,
			)
		}
	}
}

func assertSQLServerToMySQLKnownFixtureMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	namespace string,
	collation string,
	prefix string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts := tables[accountsName]
	if accounts.Schema != namespace ||
		accounts.Name != accountsName ||
		accounts.MySQLCollation != collation ||
		accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		len(accounts.Columns) != 9 ||
		accounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf(
			"MySQL target accounts identity/table = %#v %#v",
			accounts.Identity,
			accounts,
		)
	}
	assertSQLServerToMySQLColumns(t, accounts, []sqlServerToMySQLColumn{
		{name: "id", typ: "bigint", primaryKeyPosition: 1},
		{name: "code", typ: "varchar"},
		{name: "balance", typ: "numeric"},
		{name: "ratio", typ: "double precision"},
		{name: "enabled", typ: "integer"},
		{name: "payload", typ: "varbinary", nullable: true},
		{name: "created_at", typ: "datetime"},
		{name: "description", typ: "text", nullable: true},
		{name: "external_id", typ: "char"},
	})
	assertMySQLNativeDeclaredType(t, accounts.Columns[0], "bigint")
	assertMySQLNativeDeclaredType(t, accounts.Columns[1], "varchar", 24)
	assertMySQLNativeDeclaredType(t, accounts.Columns[2], "decimal", 12, 2)
	assertMySQLNativeDeclaredType(t, accounts.Columns[3], "double")
	assertMySQLNativeDeclaredType(t, accounts.Columns[4], "tinyint", 1)
	assertMySQLNativeDeclaredType(t, accounts.Columns[5], "varbinary", 16)
	assertMySQLNativeDeclaredType(t, accounts.Columns[6], "datetime", 3)
	assertMySQLNativeDeclaredType(t, accounts.Columns[7], "longtext")
	assertMySQLNativeDeclaredType(t, accounts.Columns[8], "char", 36)
	assertSQLServerToMySQLColumnDefault(
		t,
		accounts.Columns[1],
		"'guest'",
	)
	assertSQLServerToMySQLColumnDefault(
		t,
		accounts.Columns[2],
		"0",
	)
	assertSQLServerToMySQLColumnDefault(
		t,
		accounts.Columns[4],
		"1",
	)
	for _, column := range []schema.Column{
		accounts.Columns[0],
		accounts.Columns[3],
		accounts.Columns[5],
		accounts.Columns[6],
		accounts.Columns[7],
		accounts.Columns[8],
	} {
		assertSQLServerToMySQLColumnDefault(t, column, "")
	}
	if len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != prefix+"_id_uq" ||
		!accounts.Indexes[0].Unique ||
		len(accounts.Indexes[0].Columns) != 1 ||
		accounts.Indexes[0].Columns[0].Name != "id" ||
		accounts.Indexes[0].Columns[0].Descending ||
		len(accounts.Checks) != 2 ||
		!sqlServerToMySQLHasCheck(
			accounts,
			prefix+"_account_ck",
			`"balance" >= 0`,
		) ||
		!sqlServerToMySQLHasBooleanCheck(accounts, "enabled") ||
		len(accounts.ForeignKeys) != 0 {
		t.Fatalf(
			"MySQL target accounts objects = indexes %#v checks %#v foreign keys %#v",
			accounts.Indexes,
			accounts.Checks,
			accounts.ForeignKeys,
		)
	}

	events := tables[eventsName]
	if events.Schema != namespace ||
		events.Name != eventsName ||
		events.MySQLCollation != collation ||
		events.Identity != nil ||
		len(events.Columns) != 9 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("MySQL target events table/columns = %#v", events)
	}
	assertSQLServerToMySQLColumns(t, events, []sqlServerToMySQLColumn{
		{name: "tenant_id", typ: "integer", primaryKeyPosition: 1},
		{name: "event_id", typ: "bigint", primaryKeyPosition: 2},
		{name: "account_id", typ: "bigint"},
		{name: "note", typ: "varchar"},
		{name: "amount", typ: "numeric"},
		{name: "occurred_at", typ: "datetime"},
		{name: "observed_on", typ: "date"},
		{name: "local_time", typ: "time"},
		{name: "payload", typ: "blob", nullable: true},
	})
	assertMySQLNativeDeclaredType(t, events.Columns[0], "int")
	assertMySQLNativeDeclaredType(t, events.Columns[1], "bigint")
	assertMySQLNativeDeclaredType(t, events.Columns[2], "bigint")
	assertMySQLNativeDeclaredType(t, events.Columns[3], "varchar", 80)
	assertMySQLNativeDeclaredType(t, events.Columns[4], "decimal", 12, 3)
	assertMySQLNativeDeclaredType(t, events.Columns[5], "datetime", 6)
	assertMySQLNativeDeclaredType(t, events.Columns[6], "date")
	assertMySQLNativeDeclaredType(t, events.Columns[7], "time", 6)
	assertMySQLNativeDeclaredType(t, events.Columns[8], "longblob")
	assertSQLServerToMySQLColumnDefault(
		t,
		events.Columns[3],
		"'created'",
	)
	assertSQLServerToMySQLColumnDefault(
		t,
		events.Columns[4],
		"0",
	)
	for _, column := range []schema.Column{
		events.Columns[0],
		events.Columns[1],
		events.Columns[2],
		events.Columns[5],
		events.Columns[6],
		events.Columns[7],
		events.Columns[8],
	} {
		assertSQLServerToMySQLColumnDefault(t, column, "")
	}
	if len(events.Indexes) != 2 ||
		!sqlServerToMySQLHasIndex(
			events,
			prefix+"_occurred_idx",
			"occurred_at",
			true,
		) ||
		!sqlServerToMySQLHasIndex(
			events,
			"dmtx_"+eventsName+"_account_id_fkey_idx",
			"account_id",
			false,
		) ||
		len(events.Checks) != 1 ||
		!sqlServerToMySQLHasCheck(
			events,
			prefix+"_event_ck",
			`"event_id" > 0`,
		) ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"MySQL target events objects = indexes %#v checks %#v foreign keys %#v",
			events.Indexes,
			events.Checks,
			events.ForeignKeys,
		)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != prefix+"_account_fk" ||
		!reflect.DeepEqual(foreignKey.Columns, []string{"account_id"}) ||
		foreignKey.ReferencedTable != accountsName ||
		!reflect.DeepEqual(
			foreignKey.ReferencedColumns,
			[]string{"id"},
		) ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "NO ACTION" ||
		foreignKey.Match != "NONE" {
		t.Fatalf("MySQL target events foreign key = %#v", foreignKey)
	}
}

func assertSQLServerToMySQLColumnDefault(
	t *testing.T,
	column schema.Column,
	want string,
) {
	t.Helper()
	got := ""
	if column.Default != nil {
		got = column.Default.CanonicalSQL()
	}
	if got != want {
		t.Fatalf(
			"MySQL target column %s default = %q, want %q",
			column.Name,
			got,
			want,
		)
	}
}

type sqlServerToMySQLColumn struct {
	name               string
	typ                string
	nullable           bool
	primaryKeyPosition int
}

func assertSQLServerToMySQLColumns(
	t *testing.T,
	table schema.Table,
	want []sqlServerToMySQLColumn,
) {
	t.Helper()
	if len(table.Columns) != len(want) {
		t.Fatalf(
			"MySQL target table %s has %d columns, want %d",
			table.Name,
			len(table.Columns),
			len(want),
		)
	}
	for index, column := range table.Columns {
		expected := want[index]
		if column.Name != expected.name ||
			column.Type != expected.typ ||
			column.Nullable != expected.nullable ||
			column.PrimaryKey !=
				(expected.primaryKeyPosition > 0) ||
			column.PrimaryKeyPosition !=
				expected.primaryKeyPosition {
			t.Fatalf(
				"MySQL target column %s[%d] = %#v, want %#v",
				table.Name,
				index,
				column,
				expected,
			)
		}
	}
}

func sqlServerToMySQLHasIndex(
	table schema.Table,
	name string,
	column string,
	descending bool,
) bool {
	for _, index := range table.Indexes {
		if index.Name == name &&
			!index.Unique &&
			!index.Inline &&
			len(index.Columns) == 1 &&
			index.Columns[0].Name == column &&
			index.Columns[0].Collation == "" &&
			index.Columns[0].Descending == descending {
			return true
		}
	}
	return false
}

func sqlServerToMySQLHasBooleanCheck(
	table schema.Table,
	column string,
) bool {
	for _, check := range table.Checks {
		if !strings.HasPrefix(check.Name, "dmtx_bool_") {
			continue
		}
		if check.Expression.CanonicalSQL() ==
			`"`+column+`" IN (0, 1)` {
			return true
		}
	}
	return false
}

func sqlServerToMySQLHasCheck(
	table schema.Table,
	name string,
	canonical string,
) bool {
	for _, check := range table.Checks {
		if check.Name == name &&
			check.Expression.CanonicalSQL() == canonical {
			return true
		}
	}
	return false
}

func sortedSQLServerFixtureTableName(
	tables map[string]schema.Table,
	wantForeignKey bool,
) string {
	for name, table := range tables {
		if (len(table.ForeignKeys) > 0) == wantForeignKey {
			return name
		}
	}
	return ""
}

func assertSQLServerToMySQLRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var (
		code        string
		balance     string
		ratio       float64
		payload     string
		description string
		externalID  string
		created     string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT code, CAST(balance AS CHAR), ratio, LOWER(HEX(payload)),
		        description, external_id,
		        DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f')
		   FROM `+mySQLIdentifier(accountsName)+
			` WHERE id = 7`,
	).Scan(
		&code,
		&balance,
		&ratio,
		&payload,
		&description,
		&externalID,
		&created,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		ratio != float64(float32(0.1)) ||
		payload != "00ff" ||
		description != "Zażółć gęślą jaźń — 東京" ||
		externalID != "6f9619ff-8b86-d011-b42d-00c04fc964ff" ||
		created != "2026-07-29 12:34:56.123000" {
		t.Fatalf(
			"SQL Server-to-MySQL account = (%q, %q, %v, %q, %q, %q, %q)",
			code,
			balance,
			ratio,
			payload,
			description,
			externalID,
			created,
		)
	}
	var (
		note      string
		amount    string
		occurred  string
		observed  string
		localTime string
		binary    string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT note, CAST(amount AS CHAR),
		        DATE_FORMAT(occurred_at, '%Y-%m-%d %H:%i:%s.%f'),
		        DATE_FORMAT(observed_on, '%Y-%m-%d'),
		        TIME_FORMAT(local_time, '%H:%i:%s.%f'),
		        LOWER(HEX(payload))
		   FROM `+mySQLIdentifier(eventsName)+
			` WHERE tenant_id = 1
			    AND event_id = 9007199254740993`,
	).Scan(
		&note,
		&amount,
		&occurred,
		&observed,
		&localTime,
		&binary,
	); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" ||
		amount != "9.125" ||
		occurred != "2026-07-29 12:34:56.123456" ||
		observed != "2026-07-29" ||
		localTime != "12:34:56.123456" ||
		binary != "deadbeef" {
		t.Fatalf(
			"SQL Server-to-MySQL event = (%q, %q, %q, %q, %q, %q)",
			note,
			amount,
			occurred,
			observed,
			localTime,
			binary,
		)
	}
	var accountNulls, eventNull bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT payload IS NULL AND description IS NULL
		   FROM `+mySQLIdentifier(accountsName)+` WHERE id = 11`,
	).Scan(&accountNulls); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT payload IS NULL
		   FROM `+mySQLIdentifier(eventsName)+
			` WHERE tenant_id = 1
			    AND event_id = 9007199254740995`,
	).Scan(&eventNull); err != nil {
		t.Fatal(err)
	}
	if !accountNulls || !eventNull {
		t.Fatal("SQL Server-to-MySQL NULL values were not preserved")
	}
}

func assertSQLServerToMySQLDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	const externalID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			` (external_id, created_at, ratio)
			 VALUES (?, '2026-07-29 00:00:00.000', 0.25)`,
		externalID,
	); err != nil {
		t.Fatalf("insert MySQL defaults row: %v", err)
	}
	var (
		id      int64
		code    string
		balance string
		enabled bool
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT id, code, CAST(balance AS CHAR), enabled FROM "+
			mySQLIdentifier(accountsName)+" WHERE external_id = ?",
		externalID,
	).Scan(&id, &code, &balance, &enabled); err != nil {
		t.Fatal(err)
	}
	if id != 42 || code != "guest" || balance != "0.00" || !enabled {
		t.Fatalf(
			"MySQL defaults/identity row = (%d, %q, %q, %v)",
			id,
			code,
			balance,
			enabled,
		)
	}
}

func prepareSQLServerToMySQLRetainedUpsert(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	accountsName string,
) {
	t.Helper()
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+sqlServerQualified("dbo", accountsName)+
			" SET [balance] = 23.45, [enabled] = 0 WHERE [id] = 7",
	); err != nil {
		t.Fatalf("update SQL Server retained-upsert source row: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			` (id, code, external_id, created_at, ratio)
			 VALUES (
			   29, 'target-only',
			   'bbbbbbbb-cccc-dddd-eeee-ffffffffffff',
			   '2026-07-29 00:00:00.000', 0.5
			 )`,
	); err != nil {
		t.Fatalf("insert retained MySQL target row: %v", err)
	}
}

func assertSQLServerToMySQLRetainedRows(
	t *testing.T,
	ctx context.Context,
	target *sql.DB,
	accountsName string,
) {
	t.Helper()
	var balance string
	var enabled bool
	if err := target.QueryRowContext(
		ctx,
		"SELECT CAST(balance AS CHAR), enabled FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&balance, &enabled); err != nil {
		t.Fatal(err)
	}
	if balance != "23.45" || enabled {
		t.Fatalf(
			"retained-upsert row = (%q, %v), want (23.45, false)",
			balance,
			enabled,
		)
	}
	var retained int
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName)+
			" WHERE id = 29 AND code = 'target-only'",
	).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatal("retained upsert changed the target-only row")
	}
}

func assertSQLServerToMySQLMismatchPreflight(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	migrationConfig config.Config,
	accountsName string,
	eventsName string,
	indexName string,
) {
	t.Helper()
	if _, err := target.ExecContext(
		ctx,
		"DROP INDEX "+mySQLIdentifier(indexName)+" ON "+
			mySQLIdentifier(eventsName),
	); err != nil {
		t.Fatalf("drop retained MySQL index: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+sqlServerQualified("dbo", accountsName)+
			" SET [balance] = 66.66 WHERE [id] = 7",
	); err != nil {
		t.Fatalf("update source before rejected retained upsert: %v", err)
	}
	observer := &mysqlNativePreflightObserver{}
	result, err := SQLServerToMySQLWithObserver(
		ctx,
		migrationConfig,
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "retained target shape differs") {
		t.Fatalf(
			"SQL Server-to-MySQL retained mismatch result = %+v, error = %v",
			result,
			err,
		)
	}
	assertMySQLNativePreflightDidNotMutate(t, result, observer)
	var balance string
	if err := target.QueryRowContext(
		ctx,
		"SELECT CAST(balance AS CHAR) FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != "23.45" {
		t.Fatalf(
			"rejected retained upsert changed target balance to %q",
			balance,
		)
	}
}

func assertSQLServerToMySQLValuePreflight(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	migrationConfig config.Config,
	tableName string,
) {
	t.Helper()
	cleanupSQLServerToMySQLSourceTables(t, source, tableName)
	cleanupMySQLNativeTables(t, target, tableName)
	if _, err := source.ExecContext(
		ctx,
		"CREATE TABLE "+sqlServerQualified("dbo", tableName)+` (
			[id] INT NOT NULL PRIMARY KEY,
			[observed_on] DATE NOT NULL
		);
		INSERT INTO `+sqlServerQualified("dbo", tableName)+`
			([id], [observed_on])
		VALUES (1, CONVERT(date, '0999-12-31'));`,
	); err != nil {
		t.Fatalf("create unsafe SQL Server DATE fixture: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLIdentifier(tableName)+` (
			id INT NOT NULL PRIMARY KEY,
			observed_on DATE NOT NULL
		) ENGINE=InnoDB`,
	); err != nil {
		t.Fatalf("create MySQL value-preflight sentinel: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(tableName)+
			" (id, observed_on) VALUES (77, '2026-07-29')",
	); err != nil {
		t.Fatalf("seed MySQL value-preflight sentinel: %v", err)
	}

	migrationConfig.Migration.TargetMode = "drop_recreate"
	migrationConfig.Migration.IncludeTables = []string{tableName}
	observer := &mysqlNativePreflightObserver{}
	result, err := SQLServerToMySQLWithObserver(
		ctx,
		migrationConfig,
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "MySQL DATE") {
		t.Fatalf(
			"unsafe SQL Server DATE result = %+v, error = %v",
			result,
			err,
		)
	}
	assertMySQLNativePreflightDidNotMutate(t, result, observer)
	var retained int
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(tableName)+
			" WHERE id = 77 AND observed_on = '2026-07-29'",
	).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatal("unsafe SQL Server DATE preflight changed the target sentinel")
	}
}
