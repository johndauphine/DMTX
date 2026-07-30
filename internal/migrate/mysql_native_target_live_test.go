package migrate

import (
	"context"
	"database/sql"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLToMySQLCommonFixtureLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if sourceDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_TARGET_DSN, and DMTX_TEST_MYSQL_CA to run the MySQL-to-MySQL common fixture",
		)
	}
	registerMySQLCommonFixtureTLS(t, caPath)
	sourceConfig := parseMySQLNativeTargetDSN(t, "source", sourceDSN)
	targetConfig := parseMySQLNativeTargetDSN(t, "target", targetDSN)
	if sourceConfig.DBName == targetConfig.DBName &&
		sourceConfig.Addr == targetConfig.Addr {
		t.Fatal("MySQL common fixture requires distinct source and target databases")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sourceDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"source",
		sourceDSN,
	)
	targetDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetDSN,
	)

	prefix := "dmtx_mm_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupMySQLNativeTables(
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
	sourceMetadata := inspectMySQLCommonFixture(
		t,
		ctx,
		sourceDatabase,
		sourceConfig.DBName,
		accountsName,
		eventsName,
	)
	sourceEndpoint := mysqlNativeTargetEndpoint(t, sourceConfig, caPath)
	targetEndpoint := mysqlNativeTargetEndpoint(t, targetConfig, caPath)
	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("migrate MySQL common fixture into MySQL: %v", err)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"MySQL-to-MySQL common-fixture result = %+v, want 2 tables, 4 rows, validated",
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
	assertMySQLNativeExactMetadata(t, sourceMetadata, targetMetadata)
	assertMySQLNativeCommonRows(
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
	prepareMySQLNativeUpsertFixture(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		accountsName,
		eventsName,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	upsertResult, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("upsert MySQL common fixture into MySQL: %v", err)
	}
	if upsertResult.Tables != 2 ||
		upsertResult.Rows != 6 ||
		!upsertResult.Validated {
		t.Fatalf(
			"MySQL-to-MySQL upsert result = %+v, want 2 tables, 6 rows, validated",
			upsertResult,
		)
	}
	sourceMetadata = inspectMySQLCommonFixture(
		t,
		ctx,
		sourceDatabase,
		sourceConfig.DBName,
		accountsName,
		eventsName,
	)
	targetMetadata = inspectMySQLCommonFixture(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		accountsName,
		eventsName,
	)
	assertMySQLNativeExactMetadata(t, sourceMetadata, targetMetadata)
	assertMySQLNativeUpsertRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertMySQLNativeDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
		nil,
	)
	assertMySQLNativeSecondaryUniqueCollisionFails(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		migrationConfig,
		accountsName,
	)
	assertMySQLNativeMismatchPreflight(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		migrationConfig,
		accountsName,
		eventsName,
		prefix+"_occurred_idx",
	)
}

func assertMySQLNativeSecondaryUniqueCollisionFails(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	migrationConfig config.Config,
	accountsName string,
) {
	t.Helper()
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(accountsName)+
			" SET balance = 66.66 WHERE id = 7",
	); err != nil {
		t.Fatalf("prepare guarded MySQL upsert update: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			" (id, code) VALUES (31, 'target-only')",
	); err != nil {
		t.Fatalf("prepare guarded MySQL secondary collision: %v", err)
	}
	result, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "write MySQL table") {
		t.Fatalf(
			"MySQL secondary-unique collision result = %+v, error = %v",
			result,
			err,
		)
	}
	var targetBalance string
	if err := target.QueryRowContext(
		ctx,
		"SELECT CAST(balance AS CHAR) FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&targetBalance); err != nil {
		t.Fatal(err)
	}
	if targetBalance != "23.45" {
		t.Fatalf(
			"guarded MySQL upsert partially committed balance %q",
			targetBalance,
		)
	}
	var conflictingRows int
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName)+
			" WHERE (id = 29 AND code = 'target-only') OR id = 31",
	).Scan(&conflictingRows); err != nil {
		t.Fatal(err)
	}
	if conflictingRows != 1 {
		t.Fatalf(
			"guarded MySQL upsert collision rows = %d, want retained row only",
			conflictingRows,
		)
	}
}

func seedMySQLNativeReplacementTargets(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	for _, name := range []string{accountsName, eventsName} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+mySQLIdentifier(name)+
				" (stale_id BIGINT NOT NULL, stale_marker VARCHAR(24) NOT NULL, "+
				"PRIMARY KEY (stale_id)) ENGINE=InnoDB "+
				"DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin "+
				"ROW_FORMAT=DYNAMIC",
		); err != nil {
			t.Fatalf("create stale MySQL replacement target %s: %v", name, err)
		}
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+mySQLIdentifier(name)+
				" (stale_id, stale_marker) VALUES (99, 'must disappear')",
		); err != nil {
			t.Fatalf("seed stale MySQL replacement target %s: %v", name, err)
		}
	}
}

func assertMySQLNativeStaleTargetsWereReplaced(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		var staleColumns int
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			 FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA = ?
			   AND TABLE_NAME = ?
			   AND COLUMN_NAME IN ('stale_id', 'stale_marker')`,
			namespace,
			name,
		).Scan(&staleColumns); err != nil {
			t.Fatalf("inspect replaced MySQL target %s: %v", name, err)
		}
		if staleColumns != 0 {
			t.Fatalf(
				"MySQL drop/recreate retained %d stale columns on %s",
				staleColumns,
				name,
			)
		}
	}
}

func prepareMySQLNativeUpsertFixture(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			" (id, code) VALUES (29, 'target-only')",
	); err != nil {
		t.Fatalf("insert retained MySQL target row: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(accountsName)+
			" SET balance = 23.45, enabled = 0 WHERE id = 7",
	); err != nil {
		t.Fatalf("update MySQL upsert source row: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+
			` (id, code, balance, enabled, created_at, document)
			 VALUES
			 (13, 'upsert-new', 4.50, 1,
			  '2026-07-30 08:09:10', '{"upsert":true}')`,
	); err != nil {
		t.Fatalf("insert MySQL upsert source account: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(eventsName)+
			` (tenant_id, event_id, account_id, note, amount,
			   occurred_at, observed_on, payload)
			 VALUES
			 (2, 9007199254740997, 13, 'upsert event', 1.250,
			  '2026-07-30 08:09:10.123456', '2026-07-30',
			  UNHEX('cafe'))`,
	); err != nil {
		t.Fatalf("insert MySQL upsert source event: %v", err)
	}
}

func assertMySQLNativeUpsertRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 4 || eventCount != 3 {
		t.Fatalf(
			"MySQL upsert target row counts = (%d, %d), want (4, 3)",
			accountCount,
			eventCount,
		)
	}
	var balance string
	var enabled int
	if err := database.QueryRowContext(
		ctx,
		"SELECT CAST(balance AS CHAR), enabled FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&balance, &enabled); err != nil {
		t.Fatal(err)
	}
	if balance != "23.45" || enabled != 0 {
		t.Fatalf(
			"MySQL upsert updated row = (%q, %d), want (23.45, 0)",
			balance,
			enabled,
		)
	}
	var insertedCode, insertedDocument string
	if err := database.QueryRowContext(
		ctx,
		"SELECT code, JSON_UNQUOTE(JSON_EXTRACT(document, '$.upsert')) "+
			"FROM "+mySQLIdentifier(accountsName)+" WHERE id = 13",
	).Scan(&insertedCode, &insertedDocument); err != nil {
		t.Fatal(err)
	}
	if insertedCode != "upsert-new" || insertedDocument != "true" {
		t.Fatalf(
			"MySQL upsert inserted account = (%q, %q)",
			insertedCode,
			insertedDocument,
		)
	}
	var eventNote string
	if err := database.QueryRowContext(
		ctx,
		"SELECT note FROM "+mySQLIdentifier(eventsName)+
			" WHERE tenant_id = 2 AND event_id = 9007199254740997",
	).Scan(&eventNote); err != nil {
		t.Fatal(err)
	}
	if eventNote != "upsert event" {
		t.Fatalf("MySQL upsert inserted event note = %q", eventNote)
	}
	var retained int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName)+
			" WHERE id = 29 AND code = 'target-only'",
	).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatal("MySQL upsert did not retain the target-only row")
	}
}

type mysqlNativePreflightObserver struct {
	beforeSets int
	before     int
	after      int
	mutations  int
}

func (observer *mysqlNativePreflightObserver) BeforeTables(
	context.Context,
	[]string,
) error {
	observer.beforeSets++
	return nil
}

func (observer *mysqlNativePreflightObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.before++
	return nil
}

func (observer *mysqlNativePreflightObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.after++
	return nil
}

func (observer *mysqlNativePreflightObserver) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	observer.mutations++
	return mutation()
}

func assertMySQLNativeMismatchPreflight(
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
		"ALTER TABLE "+mySQLIdentifier(eventsName)+
			" DROP INDEX "+mySQLIdentifier(indexName),
	); err != nil {
		t.Fatalf("create MySQL retained-index mismatch: %v", err)
	}
	if _, err := source.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(accountsName)+
			" SET balance = 77.77 WHERE id = 7",
	); err != nil {
		t.Fatalf("update source before rejected MySQL upsert: %v", err)
	}

	observer := &mysqlNativePreflightObserver{}
	result, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "retained target shape differs") ||
		!strings.Contains(err.Error(), "index count") {
		t.Fatalf(
			"MySQL retained-index mismatch result = %+v, error = %v",
			result,
			err,
		)
	}
	if result != (Result{}) {
		t.Fatalf(
			"rejected MySQL retained-index upsert returned result %+v",
			result,
		)
	}
	if observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"MySQL mismatch reached observer/mutation callbacks: %+v",
			observer,
		)
	}

	var targetBalance string
	if err := target.QueryRowContext(
		ctx,
		"SELECT CAST(balance AS CHAR) FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&targetBalance); err != nil {
		t.Fatal(err)
	}
	if targetBalance != "23.45" {
		t.Fatalf(
			"rejected MySQL upsert changed target balance to %q",
			targetBalance,
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
		t.Fatal("rejected MySQL upsert changed the target-only sentinel")
	}
}

func TestPostgresToMySQLCommonFixtureLive(t *testing.T) {
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if postgresDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN, DMTX_TEST_MYSQL_TARGET_DSN, and DMTX_TEST_MYSQL_CA to run the PostgreSQL-to-MySQL common fixture",
		)
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL common-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	registerMySQLCommonFixtureTLS(t, caPath)
	targetConfig := parseMySQLNativeTargetDSN(t, "target", targetDSN)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sourceDatabase, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL common-fixture source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL common-fixture source: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL common-fixture source: %T", err)
	}
	targetDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetDSN,
	)
	cleanupMySQLNativeTables(
		t,
		targetDatabase,
		"account_events",
		"accounts",
	)

	namespace := "dmtx_pm_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL-to-MySQL source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL-to-MySQL source schema: %v", err)
		}
	})

	fixture := postgresCommonFixtureTables(t, namespace)
	longTextDefault, err := schema.ParseSQLiteDefault("'profile'")
	if err != nil {
		t.Fatalf("parse PostgreSQL-to-MySQL LONGTEXT default: %v", err)
	}
	numericCheck, err := schema.ParseSQLiteCheckExpression("balance >= 0")
	if err != nil {
		t.Fatalf("parse PostgreSQL-to-MySQL numeric CHECK: %v", err)
	}
	for index := range fixture {
		switch fixture[index].Name {
		case "accounts":
			fixture[index].Columns = append(
				fixture[index].Columns,
				schema.Column{
					Name:    "description",
					Type:    "text",
					Default: longTextDefault,
				},
			)
			fixture[index].Checks = []schema.CheckConstraint{{
				Name:       "accounts_balance_check",
				Expression: numericCheck,
			}}
		case "account_events":
			// PostgreSQL permits this index name to equal the FK constraint
			// name. The target must add a distinct explicit FK-support index
			// instead of relying on MySQL's colliding implicit name.
			fixture[index].Indexes = append(
				fixture[index].Indexes,
				schema.Index{
					Name: "account_events_account_fkey",
					Columns: []schema.IndexColumn{{
						Name: "event_id",
					}},
				},
			)
		}
	}
	createPostgresCommonFixture(t, ctx, sourceDatabase, fixture)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		`SELECT pg_catalog.setval(
			pg_catalog.pg_get_serial_sequence($1, $2),
			41,
			true
		)`,
		namespace+".accounts",
		"id",
	); err != nil {
		t.Fatalf("set PostgreSQL common-fixture identity frontier: %v", err)
	}
	insertPostgresCommonFixtureRows(t, ctx, sourceDatabase, namespace)

	migrationConfig := config.Config{
		Source: config.Endpoint{
			Type:      "postgres",
			Host:      postgresConfig.Host,
			Port:      int(postgresConfig.Port),
			Database:  postgresConfig.Database,
			User:      postgresConfig.User,
			Password:  postgresConfig.Password,
			Schema:    namespace,
			SSLMode:   "require",
			TLSCAFile: "",
		},
		Target: mysqlNativeTargetEndpoint(t, targetConfig, caPath),
		Migration: config.Migration{
			TargetMode: "drop_recreate",
		},
	}
	result, err := PostgresToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("migrate PostgreSQL common fixture into MySQL: %v", err)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"PostgreSQL-to-MySQL common-fixture result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetMetadata := inspectMySQLCommonFixture(
		t,
		ctx,
		targetDatabase,
		targetConfig.DBName,
		"accounts",
		"account_events",
	)
	assertPostgresToMySQLCommonMetadata(t, targetMetadata)
	assertMySQLNativeCommonRows(
		t,
		ctx,
		targetDatabase,
		"accounts",
		"account_events",
	)
	assertMySQLNativeDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		"accounts",
		mysqlNativeString("00FF"),
	)
	var sourceDocument, targetDocument string
	if err := sourceDatabase.QueryRowContext(
		ctx,
		"SELECT document::text FROM "+
			postgresQualified(namespace, "accounts")+" WHERE id = 7",
	).Scan(&sourceDocument); err != nil {
		t.Fatalf("read PostgreSQL jsonb source text: %v", err)
	}
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT document FROM "+mySQLIdentifier("accounts")+" WHERE id = 7",
	).Scan(&targetDocument); err != nil {
		t.Fatalf("read MySQL jsonb text projection: %v", err)
	}
	if targetDocument != sourceDocument {
		t.Fatalf(
			"MySQL jsonb text projection = %q, want exact %q",
			targetDocument,
			sourceDocument,
		)
	}
	migrationConfig.Migration.TargetMode = "upsert"
	retainedResult, err := PostgresToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained upsert PostgreSQL common fixture into MySQL: %v",
			err,
		)
	}
	if retainedResult.Tables != 2 ||
		retainedResult.Rows != 4 ||
		!retainedResult.Validated {
		t.Fatalf(
			"PostgreSQL-to-MySQL retained result = %+v, want 2 tables, 4 rows, validated",
			retainedResult,
		)
	}
}

func TestMySQLToMySQLRejectsLiveSameDatabaseAlias(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if sourceDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN and DMTX_TEST_MYSQL_CA to run the MySQL same-database alias guard",
		)
	}
	registerMySQLCommonFixtureTLS(t, caPath)
	sourceConfig := parseMySQLNativeTargetDSN(t, "source", sourceDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"source",
		sourceDSN,
	)
	tableName := "dmtx_alias_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, database, tableName)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLIdentifier(tableName)+
			" (id BIGINT NOT NULL, payload VARCHAR(24) NOT NULL, "+
			"PRIMARY KEY (id)) ENGINE=InnoDB "+
			"DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin "+
			"ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create MySQL alias-guard source: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(tableName)+
			" (id, payload) VALUES (1, 'must remain')",
	); err != nil {
		t.Fatalf("insert MySQL alias-guard sentinel: %v", err)
	}

	sourceEndpoint := mysqlNativeTargetEndpoint(t, sourceConfig, caPath)
	targetAlias := sourceEndpoint
	if strings.EqualFold(sourceEndpoint.Host, "localhost") {
		targetAlias.Host = "127.0.0.1"
	} else {
		targetAlias.Host = "localhost"
	}
	observer := &mysqlNativePreflightObserver{}
	result, err := MySQLToMySQLWithObserver(
		ctx,
		config.Config{
			Source: sourceEndpoint,
			Target: targetAlias,
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: []string{tableName},
			},
		},
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "distinct live source and target") {
		t.Fatalf(
			"MySQL same-database alias result = %+v, error = %v",
			result,
			err,
		)
	}
	assertMySQLNativePreflightDidNotMutate(t, result, observer)
	var payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT payload FROM "+mySQLIdentifier(tableName)+" WHERE id = 1",
	).Scan(&payload); err != nil {
		t.Fatalf("read MySQL alias-guard sentinel: %v", err)
	}
	if payload != "must remain" {
		t.Fatalf("MySQL alias-guard sentinel = %q", payload)
	}
}

func TestMySQLTargetDropPreflightLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if sourceDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_TARGET_DSN, and DMTX_TEST_MYSQL_CA to run MySQL target drop preflight tests",
		)
	}
	registerMySQLCommonFixtureTLS(t, caPath)
	sourceConfig := parseMySQLNativeTargetDSN(t, "source", sourceDSN)
	targetConfig := parseMySQLNativeTargetDSN(t, "target", targetDSN)
	if sourceConfig.DBName == targetConfig.DBName &&
		sourceConfig.Addr == targetConfig.Addr {
		t.Fatal("MySQL target preflight requires distinct source and target databases")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sourceDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"source",
		sourceDSN,
	)
	targetDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetDSN,
	)
	sourceEndpoint := mysqlNativeTargetEndpoint(t, sourceConfig, caPath)
	targetEndpoint := mysqlNativeTargetEndpoint(t, targetConfig, caPath)
	prefix := "dmtx_mp_" + strconv.FormatInt(time.Now().UnixNano(), 36)

	t.Run("selected target name occupied by view", func(t *testing.T) {
		tableName := prefix + "_view_target"
		checkName := prefix + "_view_check"
		cleanupMySQLNativeTables(t, sourceDatabase, tableName)
		cleanupMySQLNativeRelations(t, targetDatabase, tableName)
		createMySQLNativePreflightSource(
			t,
			ctx,
			sourceDatabase,
			tableName,
			checkName,
		)
		if _, err := targetDatabase.ExecContext(
			ctx,
			"CREATE VIEW "+mySQLIdentifier(tableName)+
				" AS SELECT 99 AS id, CAST(1.00 AS DECIMAL(12,2)) AS balance",
		); err != nil {
			t.Fatalf("create selected-name MySQL target view: %v", err)
		}

		observer := &mysqlNativePreflightObserver{}
		result, err := MySQLToMySQLWithObserver(
			ctx,
			config.Config{
				Source: sourceEndpoint,
				Target: targetEndpoint,
				Migration: config.Migration{
					TargetMode:    "drop_recreate",
					IncludeTables: []string{tableName},
				},
			},
			observer,
		)
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"existing target object is VIEW, not a base table",
			) {
			t.Fatalf(
				"MySQL selected-name view preflight result = %+v, error = %v",
				result,
				err,
			)
		}
		assertMySQLNativePreflightDidNotMutate(t, result, observer)
		var sentinel int
		if err := targetDatabase.QueryRowContext(
			ctx,
			"SELECT id FROM "+mySQLIdentifier(tableName),
		).Scan(&sentinel); err != nil {
			t.Fatalf("read retained MySQL target view: %v", err)
		}
		if sentinel != 99 {
			t.Fatalf("retained MySQL target view value = %d", sentinel)
		}
	})

	t.Run("case variant constraint collision", func(t *testing.T) {
		tableName := prefix + "_collision_target"
		guardName := prefix + "_collision_guard"
		checkName := prefix + "_mixed_check"
		cleanupMySQLNativeTables(t, sourceDatabase, tableName)
		cleanupMySQLNativeRelations(
			t,
			targetDatabase,
			tableName,
			guardName,
		)
		createMySQLNativePreflightSource(
			t,
			ctx,
			sourceDatabase,
			tableName,
			checkName,
		)
		if _, err := targetDatabase.ExecContext(
			ctx,
			"CREATE TABLE "+mySQLIdentifier(guardName)+
				" (id BIGINT NOT NULL, balance DECIMAL(12,2) NOT NULL, "+
				"PRIMARY KEY (id), CONSTRAINT "+
				mySQLIdentifier(strings.ToUpper(checkName))+
				" CHECK (balance >= 0)) ENGINE=InnoDB "+
				"DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin "+
				"ROW_FORMAT=DYNAMIC",
		); err != nil {
			t.Fatalf("create MySQL constraint collision guard: %v", err)
		}
		if _, err := targetDatabase.ExecContext(
			ctx,
			"INSERT INTO "+mySQLIdentifier(guardName)+
				" (id, balance) VALUES (73, 1.00)",
		); err != nil {
			t.Fatalf("insert MySQL constraint collision sentinel: %v", err)
		}

		observer := &mysqlNativePreflightObserver{}
		result, err := MySQLToMySQLWithObserver(
			ctx,
			config.Config{
				Source: sourceEndpoint,
				Target: targetEndpoint,
				Migration: config.Migration{
					TargetMode:    "drop_recreate",
					IncludeTables: []string{tableName},
				},
			},
			observer,
		)
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"retains schema-scoped constraint name",
			) {
			t.Fatalf(
				"MySQL constraint collision result = %+v, error = %v",
				result,
				err,
			)
		}
		assertMySQLNativePreflightDidNotMutate(t, result, observer)
		var sentinel int
		if err := targetDatabase.QueryRowContext(
			ctx,
			"SELECT id FROM "+mySQLIdentifier(guardName),
		).Scan(&sentinel); err != nil {
			t.Fatalf("read MySQL constraint collision sentinel: %v", err)
		}
		if sentinel != 73 {
			t.Fatalf(
				"MySQL constraint collision sentinel = %d",
				sentinel,
			)
		}
	})
}

func createMySQLNativePreflightSource(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	tableName string,
	checkName string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLIdentifier(tableName)+
			" (id BIGINT NOT NULL, balance DECIMAL(12,2) NOT NULL DEFAULT 0.00, "+
			"PRIMARY KEY (id), CONSTRAINT "+mySQLIdentifier(checkName)+
			" CHECK (balance >= 0)) ENGINE=InnoDB "+
			"DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin "+
			"ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create MySQL preflight source table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(tableName)+
			" (id, balance) VALUES (1, 1.00)",
	); err != nil {
		t.Fatalf("insert MySQL preflight source row: %v", err)
	}
}

func cleanupMySQLNativeRelations(
	t *testing.T,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, name := range names {
			var kind string
			err := database.QueryRowContext(
				ctx,
				`SELECT TABLE_TYPE
				   FROM information_schema.TABLES
				  WHERE TABLE_SCHEMA = DATABASE()
				    AND TABLE_NAME = ?`,
				name,
			).Scan(&kind)
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				t.Errorf(
					"inspect MySQL native-target relation %s: %v",
					name,
					err,
				)
				continue
			}
			kindSQL := "TABLE"
			if kind == "VIEW" {
				kindSQL = "VIEW"
			}
			if _, err := database.ExecContext(
				ctx,
				"DROP "+kindSQL+" "+mySQLIdentifier(name),
			); err != nil {
				t.Errorf(
					"drop MySQL native-target relation %s: %v",
					name,
					err,
				)
			}
		}
	})
}

func assertMySQLNativePreflightDidNotMutate(
	t *testing.T,
	result Result,
	observer *mysqlNativePreflightObserver,
) {
	t.Helper()
	if result != (Result{}) {
		t.Fatalf("rejected MySQL preflight returned result %+v", result)
	}
	if observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"MySQL preflight reached observer/mutation callbacks: %+v",
			observer,
		)
	}
}

func parseMySQLNativeTargetDSN(
	t *testing.T,
	role string,
	dsn string,
) *mysqlDriver.Config {
	t.Helper()
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL %s DSN: %T", role, err)
	}
	if parsed.Net != "tcp" || parsed.Addr == "" || parsed.DBName == "" {
		t.Fatalf("MySQL %s DSN must select one TCP database", role)
	}
	if parsed.TLSConfig != "dmtx_test" && parsed.TLSConfig != "true" {
		t.Fatalf("MySQL %s DSN must require verified TLS", role)
	}
	return parsed
}

func mysqlNativeTargetEndpoint(
	t *testing.T,
	parsed *mysqlDriver.Config,
	caPath string,
) config.Endpoint {
	t.Helper()
	host, rawPort, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatalf("parse MySQL address: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse MySQL port: %v", err)
	}
	return config.Endpoint{
		Type:      "mysql",
		Host:      host,
		Port:      port,
		Database:  parsed.DBName,
		User:      parsed.User,
		Password:  parsed.Passwd,
		Schema:    parsed.DBName,
		SSLMode:   "verify-full",
		TLSCAFile: caPath,
	}
}

func openMySQLNativeLiveDatabase(
	t *testing.T,
	ctx context.Context,
	role string,
	dsn string,
) *sql.DB {
	t.Helper()
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open MySQL %s database: %T", role, err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close MySQL %s database: %v", role, err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify MySQL %s database: %T", role, err)
	}
	if _, err := database.ExecContext(
		ctx,
		"SET SESSION information_schema_stats_expiry = 0",
	); err != nil {
		t.Fatalf(
			"configure fresh MySQL %s catalog statistics: %v",
			role,
			err,
		)
	}
	return database
}

func cleanupMySQLNativeTables(
	t *testing.T,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, name := range names {
			if _, err := database.ExecContext(
				ctx,
				"DROP TABLE IF EXISTS "+mySQLIdentifier(name),
			); err != nil {
				t.Errorf("drop MySQL native-target table %s: %v", name, err)
			}
		}
	})
}

func mysqlNativeMetadataWithoutNamespace(
	tables map[string]schema.Table,
) map[string]schema.Table {
	result := make(map[string]schema.Table, len(tables))
	for name, table := range tables {
		table.Schema = ""
		result[name] = table
	}
	return result
}

func assertMySQLNativeExactMetadata(
	t *testing.T,
	source map[string]schema.Table,
	target map[string]schema.Table,
) {
	t.Helper()
	source = mysqlNativeMetadataWithoutNamespace(source)
	target = mysqlNativeMetadataWithoutNamespace(target)
	if len(source) != len(target) {
		t.Fatalf(
			"MySQL metadata table counts differ: source=%d target=%d",
			len(source),
			len(target),
		)
	}
	for name, sourceTable := range source {
		targetTable, exists := target[name]
		if !exists {
			t.Fatalf("MySQL target metadata is missing table %s", name)
		}
		if sourceTable.Name != targetTable.Name {
			t.Fatalf(
				"MySQL table names differ: source=%q target=%q",
				sourceTable.Name,
				targetTable.Name,
			)
		}
		if !reflect.DeepEqual(sourceTable.Identity, targetTable.Identity) {
			t.Fatalf(
				"MySQL table %s identities differ: source=%#v target=%#v",
				name,
				mysqlNativeIdentityValue(sourceTable.Identity),
				mysqlNativeIdentityValue(targetTable.Identity),
			)
		}
		if len(sourceTable.Columns) != len(targetTable.Columns) {
			t.Fatalf(
				"MySQL table %s column counts differ: source=%d target=%d",
				name,
				len(sourceTable.Columns),
				len(targetTable.Columns),
			)
		}
		for index, sourceColumn := range sourceTable.Columns {
			targetColumn := targetTable.Columns[index]
			if reflect.DeepEqual(sourceColumn, targetColumn) {
				continue
			}
			t.Fatalf(
				"MySQL table %s column %d differs:\nsource=%#v declared=%#v default=%q\ntarget=%#v declared=%#v default=%q",
				name,
				index,
				sourceColumn,
				mysqlNativeDeclaredTypeValue(sourceColumn.DeclaredType),
				mysqlNativeDefaultValue(sourceColumn.Default),
				targetColumn,
				mysqlNativeDeclaredTypeValue(targetColumn.DeclaredType),
				mysqlNativeDefaultValue(targetColumn.Default),
			)
		}
		if !reflect.DeepEqual(sourceTable.Indexes, targetTable.Indexes) {
			t.Fatalf(
				"MySQL table %s indexes differ:\nsource=%#v\ntarget=%#v",
				name,
				sourceTable.Indexes,
				targetTable.Indexes,
			)
		}
		if !reflect.DeepEqual(
			sourceTable.ForeignKeys,
			targetTable.ForeignKeys,
		) {
			t.Fatalf(
				"MySQL table %s foreign keys differ:\nsource=%#v\ntarget=%#v",
				name,
				sourceTable.ForeignKeys,
				targetTable.ForeignKeys,
			)
		}
		if !reflect.DeepEqual(sourceTable.Checks, targetTable.Checks) {
			t.Fatalf(
				"MySQL table %s checks differ:\nsource=%#v\ntarget=%#v",
				name,
				sourceTable.Checks,
				targetTable.Checks,
			)
		}
		if sourceTable.SQLiteWithoutRowID != targetTable.SQLiteWithoutRowID ||
			sourceTable.SQLiteStrict != targetTable.SQLiteStrict {
			t.Fatalf(
				"MySQL table %s engine flags differ: source=(%v,%v) target=(%v,%v)",
				name,
				sourceTable.SQLiteWithoutRowID,
				sourceTable.SQLiteStrict,
				targetTable.SQLiteWithoutRowID,
				targetTable.SQLiteStrict,
			)
		}
	}
}

func mysqlNativeIdentityValue(identity *schema.Identity) any {
	if identity == nil {
		return nil
	}
	frontier := "<nil>"
	if identity.Frontier != nil {
		frontier = strconv.FormatInt(*identity.Frontier, 10)
	}
	return []any{identity.Column, identity.Generation, frontier}
}

func mysqlNativeDeclaredTypeValue(declared *schema.DeclaredType) any {
	if declared == nil {
		return nil
	}
	return *declared
}

func mysqlNativeDefaultValue(expression *schema.Expression) string {
	if expression == nil {
		return ""
	}
	return expression.CanonicalSQL()
}

func assertMySQLNativeCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 2 || eventCount != 2 {
		t.Fatalf(
			"MySQL native-target row counts = (%d, %d), want (2, 2)",
			accountCount,
			eventCount,
		)
	}

	var code, balance, payload, document string
	if err := database.QueryRowContext(
		ctx,
		"SELECT code, CAST(balance AS CHAR), HEX(payload), "+
			"JSON_UNQUOTE(JSON_EXTRACT(document, '$.active')) FROM "+
			mySQLIdentifier(accountsName)+" WHERE id = 7",
	).Scan(&code, &balance, &payload, &document); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		payload != "00FF" ||
		document != "true" {
		t.Fatalf(
			"MySQL native-target account = (%q, %q, %q, %q)",
			code,
			balance,
			payload,
			document,
		)
	}

	var note string
	if err := database.QueryRowContext(
		ctx,
		"SELECT note FROM "+mySQLIdentifier(eventsName)+
			" WHERE tenant_id = 1 AND event_id = 9007199254740993",
	).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf("MySQL native-target note = %q", note)
	}
}

func assertMySQLNativeDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	wantPayload *string,
) {
	t.Helper()
	result, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLIdentifier(accountsName)+" () VALUES ()",
	)
	if err != nil {
		t.Fatalf("insert MySQL native-target defaults row: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read MySQL native-target identity: %v", err)
	}
	var code, balance string
	var payload sql.NullString
	var enabled int
	var created bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT code, CAST(balance AS CHAR), HEX(payload), enabled, "+
			"created_at IS NOT NULL FROM "+mySQLIdentifier(accountsName)+
			" WHERE id = ?",
		id,
	).Scan(&code, &balance, &payload, &enabled, &created); err != nil {
		t.Fatalf("read MySQL native-target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		(wantPayload == nil && payload.Valid) ||
		(wantPayload != nil &&
			(!payload.Valid || payload.String != *wantPayload)) ||
		enabled != 1 ||
		!created {
		t.Fatalf(
			"MySQL native-target defaults row = (%d, %q, %q, %q, %d, %v)",
			id,
			code,
			balance,
			payload.String,
			enabled,
			created,
		)
	}
}

func mysqlNativeString(value string) *string {
	return &value
}

func assertPostgresToMySQLCommonMetadata(
	t *testing.T,
	tables map[string]schema.Table,
) {
	t.Helper()
	accounts := tables["accounts"]
	if accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 {
		t.Fatalf("MySQL target identity = %#v", accounts.Identity)
	}
	if len(accounts.Columns) != 8 ||
		accounts.Columns[0].Type != "bigint" ||
		accounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("MySQL target accounts columns = %#v", accounts.Columns)
	}
	assertMySQLNativeDeclaredType(t, accounts.Columns[1], "varchar", 24)
	assertMySQLNativeDeclaredType(t, accounts.Columns[2], "decimal", 12, 2)
	if accounts.Columns[3].Type != "integer" ||
		accounts.Columns[4].Type != "blob" ||
		accounts.Columns[6].Type != "text" {
		t.Fatalf("MySQL target mapped columns = %#v", accounts.Columns)
	}
	assertMySQLNativeDeclaredType(t, accounts.Columns[3], "tinyint", 1)
	assertMySQLNativeDeclaredType(t, accounts.Columns[4], "longblob")
	assertMySQLNativeDeclaredType(t, accounts.Columns[5], "datetime", 3)
	assertMySQLNativeDeclaredType(t, accounts.Columns[6], "longtext")
	assertMySQLNativeDeclaredType(t, accounts.Columns[7], "longtext")
	if accounts.Columns[7].Default == nil ||
		accounts.Columns[7].Default.CanonicalSQL() != "'profile'" {
		t.Fatalf(
			"MySQL target LONGTEXT default = %#v",
			accounts.Columns[7].Default,
		)
	}
	if len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != "accounts_code_uq" ||
		!accounts.Indexes[0].Unique ||
		accounts.Indexes[0].Columns[0].Collation != "BINARY" ||
		len(accounts.Checks) != 2 ||
		!mysqlNativeHasCheck(accounts, "accounts_balance_check") {
		t.Fatalf(
			"MySQL target accounts objects = indexes %#v checks %#v",
			accounts.Indexes,
			accounts.Checks,
		)
	}

	events := tables["account_events"]
	if len(events.Columns) != 4 ||
		events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("MySQL target events columns = %#v", events.Columns)
	}
	assertMySQLNativeDeclaredType(t, events.Columns[3], "varchar", 80)
	if len(events.ForeignKeys) != 1 {
		t.Fatalf("MySQL target events foreign keys = %#v", events.ForeignKeys)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != "account_events_account_fkey" ||
		foreignKey.ReferencedTable != "accounts" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "NONE" {
		t.Fatalf("MySQL target events foreign key = %#v", foreignKey)
	}
	if len(events.Indexes) != 2 {
		t.Fatalf(
			"MySQL target events indexes = %#v, want source and FK support",
			events.Indexes,
		)
	}
	var foundCollisionIndex, foundSupportIndex bool
	for _, index := range events.Indexes {
		if index.Name == "account_events_account_fkey" &&
			len(index.Columns) == 1 &&
			index.Columns[0].Name == "event_id" {
			foundCollisionIndex = true
		}
		if strings.HasPrefix(
			index.Name,
			"dmtx_account_events_account_id_fkey_idx",
		) &&
			len(index.Columns) == 1 &&
			index.Columns[0].Name == "account_id" {
			foundSupportIndex = true
		}
	}
	if !foundCollisionIndex || !foundSupportIndex {
		t.Fatalf(
			"MySQL target events index roles = %#v",
			events.Indexes,
		)
	}
}

func mysqlNativeHasCheck(table schema.Table, name string) bool {
	for _, check := range table.Checks {
		if check.Name == name {
			return true
		}
	}
	return false
}

func assertMySQLNativeDeclaredType(
	t *testing.T,
	column schema.Column,
	base string,
	arguments ...int,
) {
	t.Helper()
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		!reflect.DeepEqual(column.DeclaredType.Arguments, arguments) {
		t.Fatalf(
			"MySQL target column %s declaration = %#v, want %s%v",
			column.Name,
			column.DeclaredType,
			base,
			arguments,
		)
	}
}
