package engine

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
)

func TestOpenMariaDB1011TargetLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MARIADB_DSN")
	caPath := os.Getenv("DMTX_TEST_MARIADB_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MARIADB_DSN and DMTX_TEST_MARIADB_CA to run MariaDB 10.11 target admission",
		)
	}
	registerMariaDB1011LiveTLS(t, caPath)
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MARIADB_DSN: %v", err)
	}
	host, portText, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatalf("parse MariaDB target address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse MariaDB target port: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, flavor, err := OpenMySQLTarget(ctx, config.Endpoint{
		Type:      "mysql",
		Host:      host,
		Port:      port,
		Database:  parsed.DBName,
		User:      parsed.User,
		Password:  parsed.Passwd,
		SSLMode:   "verify-full",
		TLSCAFile: caPath,
	})
	if err != nil {
		t.Fatalf("open live MariaDB target: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live MariaDB target: %v", err)
		}
	})
	if flavor != MySQLServerFlavorMariaDB1011 {
		t.Fatalf("MariaDB target flavor = %d", flavor)
	}
	if database.Stats().MaxOpenConnections != 1 {
		t.Fatalf(
			"MariaDB target max open connections = %d, want 1",
			database.Stats().MaxOpenConnections,
		)
	}
	verified, err := VerifyMySQLTarget(ctx, database)
	if err != nil {
		t.Fatalf("verify live MariaDB target: %v", err)
	}
	if verified != flavor {
		t.Fatalf(
			"verified MariaDB target flavor = %d, opened flavor = %d",
			verified,
			flavor,
		)
	}

	var noAutoValueOnZero, foreignKeys, uniqueKeys, checks int
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			FIND_IN_SET('NO_AUTO_VALUE_ON_ZERO', @@session.sql_mode) > 0,
			@@session.foreign_key_checks,
			@@session.unique_checks,
			@@session.check_constraint_checks`,
	).Scan(
		&noAutoValueOnZero,
		&foreignKeys,
		&uniqueKeys,
		&checks,
	); err != nil {
		t.Fatalf("read live MariaDB target session: %v", err)
	}
	if noAutoValueOnZero != 1 ||
		foreignKeys != 1 ||
		uniqueKeys != 1 ||
		checks != 1 {
		t.Fatalf(
			"MariaDB target session = no_auto_zero:%d fk:%d unique:%d checks:%d",
			noAutoValueOnZero,
			foreignKeys,
			uniqueKeys,
			checks,
		)
	}
}
