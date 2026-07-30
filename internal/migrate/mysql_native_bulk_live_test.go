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

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLNativeWriterLocalInfileFallbackLive(t *testing.T) {
	database, namespace := openMySQLNativeBulkLiveDatabase(
		t,
		"DMTX_TEST_MYSQL_TARGET_DSN",
		"DMTX_TEST_MYSQL_CA",
		"dmtx_test",
		true,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if enabled := readMySQLLocalInfileSettingLive(t, ctx, database); enabled {
		t.Fatal(
			"MySQL fallback sentinel requires @@GLOBAL.local_infile = OFF",
		)
	}

	table := createMySQLNativeBulkLiveTable(
		t,
		ctx,
		database,
		namespace,
		"dmtx_mysql_bulk_",
	)
	writer := newMySQLNativeWriterForFlavor(
		database,
		engine.MySQLServerFlavorOracle80,
	)
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	firstTime := time.Date(
		2026,
		time.July,
		30,
		12,
		34,
		56,
		123456000,
		time.UTC,
	)
	firstRows := [][]any{{
		int64(1),
		nil,
		"",
		[]byte{0x00, '\t', '\n', 0x1a, 0xff},
		firstTime,
		"123456789.123456",
	}}
	receipt, err := writer.WriteBatch(
		ctx,
		table,
		mysqlNativeBulkLiveColumns(),
		"drop_recreate",
		firstRows,
	)
	if err != nil {
		t.Fatalf("write MySQL fallback batch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)

	secondTime := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
	receipt, err = writer.WriteBatch(
		ctx,
		table,
		mysqlNativeBulkLiveColumns(),
		"drop_recreate",
		[][]any{{
			int64(2),
			"unicode café 😀",
			"x",
			[]byte{},
			secondTime,
			"-0.000001",
		}},
	)
	if err != nil {
		t.Fatalf("write second MySQL fallback batch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if writer.localInfile != mysqlLocalInfileFallback {
		t.Fatalf("MySQL local infile state = %d", writer.localInfile)
	}
	if !reflect.DeepEqual(
		warnings,
		[]string{mysqlLocalInfileFallbackWarning},
	) {
		t.Fatalf("MySQL fallback warnings = %#v", warnings)
	}
	assertMySQLNativeBulkLiveRows(
		t,
		ctx,
		database,
		table,
		[]mysqlNativeBulkLiveRow{
			{
				id:        1,
				nullable:  sql.NullString{},
				empty:     "",
				payload:   []byte{0x00, '\t', '\n', 0x1a, 0xff},
				eventTime: firstTime,
				amount:    "123456789.123456",
			},
			{
				id: 2,
				nullable: sql.NullString{
					String: "unicode café 😀",
					Valid:  true,
				},
				empty:     "x",
				payload:   []byte{},
				eventTime: secondTime,
				amount:    "-0.000001",
			},
		},
	)
}

func TestMariaDBNativeWriterLocalInfileRoundTripLive(t *testing.T) {
	database, namespace := openMySQLNativeBulkLiveDatabase(
		t,
		"DMTX_TEST_MARIADB_TARGET_DSN",
		"DMTX_TEST_MARIADB_CA",
		"dmtx_mariadb_test",
		false,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if enabled := readMySQLLocalInfileSettingLive(t, ctx, database); !enabled {
		t.Fatal(
			"MariaDB native bulk sentinel requires @@GLOBAL.local_infile = ON",
		)
	}

	table := createMySQLNativeBulkLiveTable(
		t,
		ctx,
		database,
		namespace,
		"dmtx_maria_bulk_",
	)
	writer := newMySQLNativeWriterForFlavor(
		database,
		engine.MySQLServerFlavorMariaDB1011,
	)
	var warnings []string
	writer.warn = func(message string) {
		warnings = append(warnings, message)
	}
	firstTime := time.Date(
		2026,
		time.July,
		30,
		12,
		34,
		56,
		123456000,
		time.UTC,
	)
	secondTime := time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)
	receipt, err := writer.WriteBatch(
		ctx,
		table,
		mysqlNativeBulkLiveColumns(),
		"drop_recreate",
		[][]any{
			{
				int64(1),
				nil,
				"",
				[]byte{0x00, '\t', '\n', 0x1a, 0xff},
				firstTime,
				"123456789.123456",
			},
			{
				int64(2),
				"unicode café 😀",
				"x",
				[]byte{},
				secondTime,
				"-0.000001",
			},
		},
	)
	if err != nil {
		t.Fatalf("write MariaDB native bulk batch: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 2, 2)
	if writer.localInfile != mysqlLocalInfileEnabled {
		t.Fatalf("MariaDB local infile state = %d", writer.localInfile)
	}
	if len(warnings) != 0 {
		t.Fatalf("MariaDB native bulk warnings = %#v", warnings)
	}
	assertMySQLNativeBulkLiveRows(
		t,
		ctx,
		database,
		table,
		[]mysqlNativeBulkLiveRow{
			{
				id:        1,
				nullable:  sql.NullString{},
				empty:     "",
				payload:   []byte{0x00, '\t', '\n', 0x1a, 0xff},
				eventTime: firstTime,
				amount:    "123456789.123456",
			},
			{
				id: 2,
				nullable: sql.NullString{
					String: "unicode café 😀",
					Valid:  true,
				},
				empty:     "x",
				payload:   []byte{},
				eventTime: secondTime,
				amount:    "-0.000001",
			},
		},
	)

	upsertWriter := newMySQLNativeWriterForFlavor(
		database,
		engine.MySQLServerFlavorMariaDB1011,
	)
	var upsertWarnings []string
	upsertWriter.warn = func(message string) {
		upsertWarnings = append(upsertWarnings, message)
	}
	receipt, err = upsertWriter.WriteBatch(
		ctx,
		table,
		mysqlNativeBulkLiveColumns(),
		"upsert",
		[][]any{{
			int64(1),
			"retained update",
			"",
			[]byte{0x01},
			firstTime,
			"1.000000",
		}},
	)
	if err != nil {
		t.Fatalf("write MariaDB guarded upsert: %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 1, 1)
	if !reflect.DeepEqual(
		upsertWarnings,
		[]string{mysqlLocalInfileUpsertFallbackWarning},
	) {
		t.Fatalf("MariaDB upsert warnings = %#v", upsertWarnings)
	}
	var nullable sql.NullString
	if err := database.QueryRowContext(
		ctx,
		"SELECT nullable_text FROM "+
			mySQLQualified(table.Schema, table.Name)+" WHERE id = 1",
	).Scan(&nullable); err != nil {
		t.Fatalf("read MariaDB guarded upsert: %v", err)
	}
	if !nullable.Valid || nullable.String != "retained update" {
		t.Fatalf("MariaDB guarded upsert value = %#v", nullable)
	}
}

func TestMariaDBNativeWriterLocalInfileWarningLeavesTargetUntouchedLive(
	t *testing.T,
) {
	database, namespace := openMySQLNativeBulkLiveDatabase(
		t,
		"DMTX_TEST_MARIADB_TARGET_DSN",
		"DMTX_TEST_MARIADB_CA",
		"dmtx_mariadb_test",
		false,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if enabled := readMySQLLocalInfileSettingLive(t, ctx, database); !enabled {
		t.Fatal(
			"MariaDB warning sentinel requires @@GLOBAL.local_infile = ON",
		)
	}

	name := "dmtx_maria_bulk_warning_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	table := schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "tiny_value", Type: "smallint"},
		},
	}
	cleanupMySQLNativeTables(t, database, name)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLQualified(namespace, name)+
			" (id BIGINT NOT NULL, tiny_value TINYINT UNSIGNED NOT NULL, "+
			"PRIMARY KEY (id)) ENGINE=InnoDB",
	); err != nil {
		t.Fatalf("create MariaDB warning target: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+mySQLQualified(namespace, name)+
			" (id, tiny_value) VALUES (99, 7)",
	); err != nil {
		t.Fatalf("seed MariaDB warning target: %v", err)
	}

	writer := newMySQLNativeWriterForFlavor(
		database,
		engine.MySQLServerFlavorMariaDB1011,
	)
	var fallbacks []string
	writer.warn = func(message string) {
		fallbacks = append(fallbacks, message)
	}
	receipt, err := writer.WriteBatch(
		ctx,
		table,
		[]string{"id", "tiny_value"},
		"drop_recreate",
		[][]any{{int64(1), "999"}},
	)
	if err == nil || !strings.Contains(err.Error(), "conversion warnings") {
		t.Fatalf("MariaDB lossy bulk error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
	if len(fallbacks) != 0 {
		t.Fatalf("lossy MariaDB bulk fell back: %#v", fallbacks)
	}
	var (
		count int
		id    int64
		value int64
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*), MIN(id), MIN(tiny_value) FROM "+
			mySQLQualified(namespace, name),
	).Scan(&count, &id, &value); err != nil {
		t.Fatalf("inspect MariaDB warning target: %v", err)
	}
	if count != 1 || id != 99 || value != 7 {
		t.Fatalf(
			"MariaDB warning target count=%d id=%d value=%d",
			count,
			id,
			value,
		)
	}
}

func openMySQLNativeBulkLiveDatabase(
	t *testing.T,
	dsnEnv string,
	caEnv string,
	tlsConfig string,
	refreshInformationSchema bool,
) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	caPath := os.Getenv(caEnv)
	if dsn == "" || caPath == "" {
		t.Skip("set " + dsnEnv + " and " + caEnv + " to run this live test")
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"bulk target",
		dsn,
		tlsConfig,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	database := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		"bulk target",
		dsn,
		refreshInformationSchema,
	)
	return database, parsed.DBName
}

func readMySQLLocalInfileSettingLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) bool {
	t.Helper()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin local infile setting read: %v", err)
	}
	defer transaction.Rollback()
	enabled, err := (mysqlSQLBatchTransaction{
		transaction: transaction,
	}).LocalInfileEnabled(ctx)
	if err != nil {
		t.Fatalf("read local infile setting: %v", err)
	}
	return enabled
}

func createMySQLNativeBulkLiveTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	prefix string,
) schema.Table {
	t.Helper()
	name := prefix + strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, database, name)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+mySQLQualified(namespace, name)+" ("+
			"id BIGINT NOT NULL, "+
			"nullable_text VARCHAR(255) NULL, "+
			"empty_text VARCHAR(255) NOT NULL, "+
			"payload VARBINARY(64) NOT NULL, "+
			"event_time DATETIME(6) NOT NULL, "+
			"amount DECIMAL(20,6) NOT NULL, "+
			"PRIMARY KEY (id)) ENGINE=InnoDB "+
			"DEFAULT CHARACTER SET=utf8mb4",
	); err != nil {
		t.Fatalf("create MySQL native bulk table: %v", err)
	}
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "nullable_text", Type: "varchar(255)", Nullable: true},
			{Name: "empty_text", Type: "varchar(255)"},
			{Name: "payload", Type: "varbinary(64)"},
			{Name: "event_time", Type: "datetime(6)"},
			{Name: "amount", Type: "decimal(20,6)"},
		},
	}
}

func mysqlNativeBulkLiveColumns() []string {
	return []string{
		"id",
		"nullable_text",
		"empty_text",
		"payload",
		"event_time",
		"amount",
	}
}

type mysqlNativeBulkLiveRow struct {
	id        int64
	nullable  sql.NullString
	empty     string
	payload   []byte
	eventTime time.Time
	amount    string
}

func assertMySQLNativeBulkLiveRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
	want []mysqlNativeBulkLiveRow,
) {
	t.Helper()
	rows, err := database.QueryContext(
		ctx,
		"SELECT id, nullable_text, empty_text, payload, event_time, "+
			"CAST(amount AS CHAR) FROM "+
			mySQLQualified(table.Schema, table.Name)+" ORDER BY id",
	)
	if err != nil {
		t.Fatalf("read MySQL native bulk rows: %v", err)
	}
	defer rows.Close()
	var got []mysqlNativeBulkLiveRow
	for rows.Next() {
		var row mysqlNativeBulkLiveRow
		if err := rows.Scan(
			&row.id,
			&row.nullable,
			&row.empty,
			&row.payload,
			&row.eventTime,
			&row.amount,
		); err != nil {
			t.Fatalf("scan MySQL native bulk row: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate MySQL native bulk rows: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MySQL native bulk rows = %#v, want %#v", got, want)
	}
}
