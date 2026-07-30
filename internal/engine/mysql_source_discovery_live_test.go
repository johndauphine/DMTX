package engine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestInspectMySQL80SourceSchemaLive(t *testing.T) {
	database, namespace := openMySQL80SourceLive(t)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live MySQL source: %v", err)
		}
	})

	prefix := "dmtx_src_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupMySQLSourceTables(t, database, eventsName, accountsName)

	accountsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			external_id VARCHAR(40) COLLATE utf8mb4_bin NOT NULL,
			status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL DEFAULT 'active',
			balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
			enabled TINYINT(1) NOT NULL DEFAULT 1,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			payload JSON NULL,
			raw_payload VARBINARY(16) NULL,
			PRIMARY KEY (id),
			UNIQUE KEY %s (external_id),
			CONSTRAINT %s CHECK (
				balance >= 0 AND status IN ('active', 'disabled')
			)
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET=utf8mb4
		  COLLATE=utf8mb4_bin
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLLiveIdentifier(accountsName),
		mySQLLiveIdentifier(prefix+"_external_uq"),
		mySQLLiveIdentifier(prefix+"_balance_ck"),
	)
	if _, err := database.ExecContext(context.Background(), accountsDDL); err != nil {
		t.Fatalf("create live MySQL accounts fixture: %v", err)
	}

	eventsDDL := fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT NOT NULL,
			event_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			note VARCHAR(80) COLLATE utf8mb4_bin NULL,
			amount DECIMAL(12,3) NOT NULL DEFAULT 0.000,
			occurred_at DATETIME(6) NOT NULL,
			occurred_on DATE NOT NULL DEFAULT (CURRENT_DATE),
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
		mySQLLiveIdentifier(eventsName),
		mySQLLiveIdentifier(prefix+"_occurred_idx"),
		mySQLLiveIdentifier(prefix+"_account_fk"),
		mySQLLiveIdentifier(accountsName),
		mySQLLiveIdentifier(prefix+"_event_ck"),
	)
	if _, err := database.ExecContext(context.Background(), eventsDDL); err != nil {
		t.Fatalf("create live MySQL events fixture: %v", err)
	}

	if _, err := database.ExecContext(
		context.Background(),
		fmt.Sprintf(`
			INSERT INTO %s (
				id,
				external_id,
				status,
				balance,
				enabled,
				payload,
				raw_payload
			) VALUES (
				41,
				'åccount-東京',
				'active',
				1234.50,
				1,
				JSON_OBJECT('source', 'mysql'),
				UNHEX('00ff10')
			)
		`, mySQLLiveIdentifier(accountsName)),
	); err != nil {
		t.Fatalf("seed live MySQL accounts fixture: %v", err)
	}
	if _, err := database.ExecContext(
		context.Background(),
		fmt.Sprintf(`
			INSERT INTO %s (
				tenant_id,
				event_id,
				account_id,
				note,
				amount,
				occurred_at,
				payload
			) VALUES (
				7,
				9001,
				41,
				'naïve café 東京',
				9.125,
				'2026-07-29 12:34:56.123456',
				UNHEX('deadbeef')
			)
		`, mySQLLiveIdentifier(eventsName)),
	); err != nil {
		t.Fatalf("seed live MySQL events fixture: %v", err)
	}

	ctx := context.Background()
	if err := VerifyMySQL80Source(ctx, database); err != nil {
		t.Fatalf("verify live MySQL source: %v", err)
	}
	accounts, err := InspectMySQLTable(
		ctx,
		database,
		namespace,
		accountsName,
	)
	if err != nil {
		t.Fatalf("inspect live MySQL accounts: %v", err)
	}
	events, err := InspectMySQLTable(
		ctx,
		database,
		namespace,
		eventsName,
	)
	if err != nil {
		t.Fatalf("inspect live MySQL events: %v", err)
	}
	assertMySQL80AccountsDiscovery(t, accounts, prefix)
	assertMySQL80EventsDiscovery(t, events, accountsName, prefix)

	t.Run("generated column fails closed", func(t *testing.T) {
		name := prefix + "_generated"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id INT NOT NULL,
				doubled INT GENERATED ALWAYS AS (id * 2) STORED,
				PRIMARY KEY (id)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("prefix index fails closed", func(t *testing.T) {
		name := prefix + "_prefix"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id INT NOT NULL,
				note VARCHAR(100) COLLATE utf8mb4_bin NOT NULL,
				PRIMARY KEY (id),
				KEY %s (note(10))
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_bin
			  ROW_FORMAT=DYNAMIC
		`,
			mySQLLiveIdentifier(name),
			mySQLLiveIdentifier(prefix+"_prefix_idx"),
		))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("unsigned type fails closed", func(t *testing.T) {
		name := prefix + "_unsigned"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT UNSIGNED NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
}

func openMySQL80SourceLive(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN to run MySQL 8 source discovery tests",
		)
	}
	registerMySQL80LiveTLS(t)
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MYSQL_DSN: %v", err)
	}
	if parsed.TLSConfig != "true" &&
		parsed.TLSConfig != "dmtx_test" {
		t.Fatal("DMTX_TEST_MYSQL_DSN must use verified TLS")
	}
	if parsed.DBName == "" {
		t.Fatal("DMTX_TEST_MYSQL_DSN must select a database")
	}
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open live MySQL source: %v", err)
	}
	if err := database.PingContext(context.Background()); err != nil {
		database.Close()
		t.Fatalf("ping live MySQL source: %v", err)
	}
	var variable, cipher string
	if err := database.QueryRowContext(
		context.Background(),
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
	).Scan(&variable, &cipher); err != nil {
		database.Close()
		t.Fatalf("read live MySQL TLS status: %v", err)
	}
	if variable != "Ssl_cipher" || cipher == "" {
		database.Close()
		t.Fatal("live MySQL source is not using TLS")
	}
	return database, parsed.DBName
}

func registerMySQL80LiveTLS(t *testing.T) {
	t.Helper()
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if caPath == "" {
		return
	}
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
		t.Fatalf("register MySQL live TLS config: %v", err)
	}
}

func assertMySQL80AccountsDiscovery(
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
	if len(table.Columns) != 8 ||
		!table.Columns[0].PrimaryKey ||
		table.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("accounts columns = %#v", table.Columns)
	}
	assertMySQLLiveDeclaredType(t, table.Columns[1], "varchar", 40)
	assertMySQLLiveDeclaredType(t, table.Columns[3], "decimal", 12, 2)
	assertMySQLLiveDeclaredType(t, table.Columns[4], "tinyint", 1)
	assertMySQLLiveDeclaredType(t, table.Columns[5], "timestamp", 0)
	assertMySQLLiveDeclaredType(t, table.Columns[7], "varbinary", 16)
	if table.Columns[2].Default == nil ||
		table.Columns[2].Default.CanonicalSQL() != "'active'" ||
		table.Columns[3].Default == nil ||
		table.Columns[3].Default.CanonicalSQL() != "0" ||
		table.Columns[5].Default == nil ||
		table.Columns[5].Default.CanonicalSQL() != "CURRENT_TIMESTAMP" {
		t.Fatalf(
			"accounts defaults = status %q balance %q timestamp %q",
			mySQLLiveDefaultSQL(table.Columns[2]),
			mySQLLiveDefaultSQL(table.Columns[3]),
			mySQLLiveDefaultSQL(table.Columns[5]),
		)
	}
	if len(table.Indexes) != 1 ||
		table.Indexes[0].Name != prefix+"_external_uq" ||
		!table.Indexes[0].Unique ||
		len(table.Indexes[0].Columns) != 1 ||
		table.Indexes[0].Columns[0].Collation != "BINARY" {
		t.Fatalf("accounts indexes = %#v", table.Indexes)
	}
	if len(table.Checks) != 1 ||
		table.Checks[0].Name != prefix+"_balance_ck" {
		t.Fatalf("accounts checks = %#v", table.Checks)
	}
}

func mySQLLiveDefaultSQL(column schema.Column) string {
	if column.Default == nil {
		return "<nil>"
	}
	return column.Default.CanonicalSQL()
}

func assertMySQL80EventsDiscovery(
	t *testing.T,
	table schema.Table,
	accountsName string,
	prefix string,
) {
	t.Helper()
	if len(table.Columns) != 8 ||
		!table.Columns[0].PrimaryKey ||
		table.Columns[0].PrimaryKeyPosition != 1 ||
		!table.Columns[1].PrimaryKey ||
		table.Columns[1].PrimaryKeyPosition != 2 {
		t.Fatalf("events columns = %#v", table.Columns)
	}
	assertMySQLLiveDeclaredType(t, table.Columns[4], "decimal", 12, 3)
	assertMySQLLiveDeclaredType(t, table.Columns[5], "datetime", 6)
	if table.Columns[6].Default == nil ||
		table.Columns[6].Default.CanonicalSQL() != "CURRENT_DATE" {
		t.Fatalf("events date default = %#v", table.Columns[6].Default)
	}
	if len(table.Indexes) != 2 {
		t.Fatalf("events indexes = %#v", table.Indexes)
	}
	var descending bool
	for _, index := range table.Indexes {
		if index.Name == prefix+"_occurred_idx" {
			descending = len(index.Columns) == 1 &&
				index.Columns[0].Descending
		}
	}
	if !descending {
		t.Fatalf("events descending index = %#v", table.Indexes)
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
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "NONE" {
		t.Fatalf("events foreign key = %#v", foreignKey)
	}
}

func assertMySQLLiveDeclaredType(
	t *testing.T,
	column schema.Column,
	base string,
	arguments ...int,
) {
	t.Helper()
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		!equalMySQLLiveInts(
			column.DeclaredType.Arguments,
			arguments,
		) {
		t.Fatalf(
			"column %s declaration = %#v, want %s%v",
			column.Name,
			column.DeclaredType,
			base,
			arguments,
		)
	}
}

func equalMySQLLiveInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func assertMySQLSourcePolicyError(
	t *testing.T,
	database *sql.DB,
	namespace string,
	name string,
) {
	t.Helper()
	_, err := InspectMySQLTable(
		context.Background(),
		database,
		namespace,
		name,
	)
	if err == nil {
		t.Fatalf("expected MySQL source policy error for %s", name)
	}
	if !strings.Contains(err.Error(), "schema policy:") {
		t.Fatalf("unexpected MySQL source error: %v", err)
	}
}

func execMySQLSourceLiveDDL(
	t *testing.T,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		context.Background(),
		statement,
	); err != nil {
		t.Fatalf("create live MySQL fail-closed fixture: %v", err)
	}
}

func cleanupMySQLSourceTables(
	t *testing.T,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		for _, name := range names {
			if _, err := database.ExecContext(
				context.Background(),
				"DROP TABLE IF EXISTS "+mySQLLiveIdentifier(name),
			); err != nil {
				t.Errorf("drop live MySQL fixture %s: %v", name, err)
			}
		}
	})
}

func mySQLLiveIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
