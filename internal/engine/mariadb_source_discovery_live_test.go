package engine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestInspectMariaDB1011SourceSchemaLive(t *testing.T) {
	database, namespace := openMariaDB1011SourceLive(t)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live MariaDB source: %v", err)
		}
	})

	prefix := "dmtx_maria_src_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupMySQLSourceTables(t, database, eventsName, accountsName)

	execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			external_id VARCHAR(40) COLLATE utf8mb4_nopad_bin NOT NULL,
			status VARCHAR(16) COLLATE utf8mb4_nopad_bin NOT NULL DEFAULT 'active',
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
		  COLLATE=utf8mb4_nopad_bin
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLLiveIdentifier(accountsName),
		mySQLLiveIdentifier(prefix+"_external_uq"),
		mySQLLiveIdentifier(prefix+"_balance_ck"),
	))
	execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT NOT NULL,
			event_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			note VARCHAR(80) COLLATE utf8mb4_nopad_bin NULL,
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
		  COLLATE=utf8mb4_nopad_bin
		  ROW_FORMAT=DYNAMIC
	`,
		mySQLLiveIdentifier(eventsName),
		mySQLLiveIdentifier(prefix+"_occurred_idx"),
		mySQLLiveIdentifier(prefix+"_account_fk"),
		mySQLLiveIdentifier(accountsName),
		mySQLLiveIdentifier(prefix+"_event_ck"),
	))
	execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
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
			JSON_OBJECT('source', 'mariadb'),
			UNHEX('00ff10')
		)
	`, mySQLLiveIdentifier(accountsName)))
	execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
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
	`, mySQLLiveIdentifier(eventsName)))

	ctx := context.Background()
	if err := VerifyMariaDB1011Source(ctx, database); err != nil {
		t.Fatalf("verify live MariaDB source: %v", err)
	}
	if err := VerifyMySQLSource(ctx, database); err != nil {
		t.Fatalf("dispatch live MariaDB source verification: %v", err)
	}
	if err := VerifyMySQL80Source(ctx, database); err == nil {
		t.Fatal("Oracle MySQL verifier accepted MariaDB")
	}

	accounts, err := InspectMySQLTable(
		ctx,
		database,
		namespace,
		accountsName,
	)
	if err != nil {
		t.Fatalf("inspect live MariaDB accounts: %v", err)
	}
	events, err := InspectMySQLTable(
		ctx,
		database,
		namespace,
		eventsName,
	)
	if err != nil {
		t.Fatalf("inspect live MariaDB events: %v", err)
	}
	assertMySQL80AccountsDiscovery(t, accounts, prefix)
	assertMySQL80EventsDiscovery(t, events, accountsName, prefix)
	assertMySQLLiveDeclaredType(t, accounts.Columns[6], "json")
	if accounts.Columns[6].Type != "json" {
		t.Fatalf(
			"MariaDB JSON canonical type = %q",
			accounts.Columns[6].Type,
		)
	}

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
			  COLLATE=utf8mb4_nopad_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("prefix index fails closed", func(t *testing.T) {
		name := prefix + "_prefix"
		indexName := prefix + "_prefix_idx"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id INT NOT NULL,
				note VARCHAR(100) COLLATE utf8mb4_nopad_bin NOT NULL,
				PRIMARY KEY (id),
				KEY %s (note(10))
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_nopad_bin
			  ROW_FORMAT=DYNAMIC
		`,
			mySQLLiveIdentifier(name),
			mySQLLiveIdentifier(indexName),
		))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("ignored index fails closed", func(t *testing.T) {
		name := prefix + "_ignored"
		indexName := prefix + "_ignored_idx"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id INT NOT NULL,
				code VARCHAR(24) COLLATE utf8mb4_nopad_bin NOT NULL,
				PRIMARY KEY (id),
				KEY %s (code)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_nopad_bin
			  ROW_FORMAT=DYNAMIC
		`,
			mySQLLiveIdentifier(name),
			mySQLLiveIdentifier(indexName),
		))
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(
			"ALTER TABLE %s ALTER INDEX %s IGNORED",
			mySQLLiveIdentifier(name),
			mySQLLiveIdentifier(indexName),
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
			  COLLATE=utf8mb4_nopad_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("JSON default fails closed", func(t *testing.T) {
		name := prefix + "_json_default"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL,
				document JSON NOT NULL DEFAULT '{}',
				PRIMARY KEY (id)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_nopad_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
	t.Run("padded binary collation fails closed", func(t *testing.T) {
		name := prefix + "_pad_collation"
		cleanupMySQLSourceTables(t, database, name)
		execMySQLSourceLiveDDL(t, database, fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL,
				code VARCHAR(24) COLLATE utf8mb4_bin NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB
			  DEFAULT CHARACTER SET=utf8mb4
			  COLLATE=utf8mb4_bin
			  ROW_FORMAT=DYNAMIC
		`, mySQLLiveIdentifier(name)))
		assertMySQLSourcePolicyError(t, database, namespace, name)
	})
}

func openMariaDB1011SourceLive(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MARIADB_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_MARIADB_DSN to run MariaDB 10.11 source discovery tests",
		)
	}
	caPath := os.Getenv("DMTX_TEST_MARIADB_CA")
	if caPath == "" {
		t.Skip(
			"set DMTX_TEST_MARIADB_CA to run MariaDB 10.11 source discovery tests",
		)
	}
	registerMariaDB1011LiveTLS(t, caPath)
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MARIADB_DSN: %v", err)
	}
	if parsed.TLSConfig != "dmtx_mariadb_test" {
		t.Fatal("DMTX_TEST_MARIADB_DSN must use verified TLS")
	}
	if parsed.DBName == "" {
		t.Fatal("DMTX_TEST_MARIADB_DSN must select a database")
	}
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open live MariaDB source: %v", err)
	}
	if err := database.PingContext(context.Background()); err != nil {
		database.Close()
		t.Fatalf("ping live MariaDB source: %v", err)
	}
	var variable, cipher string
	if err := database.QueryRowContext(
		context.Background(),
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
	).Scan(&variable, &cipher); err != nil {
		database.Close()
		t.Fatalf("read live MariaDB TLS status: %v", err)
	}
	if variable != "Ssl_cipher" || cipher == "" {
		database.Close()
		t.Fatal("live MariaDB source is not using TLS")
	}
	return database, parsed.DBName
}

func registerMariaDB1011LiveTLS(t *testing.T, caPath string) {
	t.Helper()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read DMTX_TEST_MARIADB_CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("DMTX_TEST_MARIADB_CA contains no certificates")
	}
	if err := mysqlDriver.RegisterTLSConfig(
		"dmtx_mariadb_test",
		&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		},
	); err != nil {
		t.Fatalf("register MariaDB live TLS config: %v", err)
	}
}
