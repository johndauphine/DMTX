package engine

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// MySQLDSN builds a TLS-required MySQL connection string without logging or
// resolving password templates.
func MySQLDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("MySQL host, database, and user are required")
	}
	port := endpoint.Port
	if port == 0 {
		port = 3306
	}
	connection := mysqlDriver.NewConfig()
	connection.User = endpoint.User
	connection.Passwd = endpoint.Password
	connection.Net = "tcp"
	connection.Addr = fmt.Sprintf("%s:%d", endpoint.Host, port)
	connection.DBName = endpoint.Database
	tlsConfig, err := mySQLTLSConfig(endpoint)
	if err != nil {
		return "", err
	}
	connection.TLSConfig = tlsConfig
	connection.ParseTime = true
	return connection.FormatDSN(), nil
}

func mySQLTLSConfig(endpoint config.Endpoint) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(endpoint.SSLMode))
	switch mode {
	case "", "require", "verify-full":
	default:
		return "", fmt.Errorf(
			"MySQL ssl_mode %q is unsupported; use require or verify-full",
			endpoint.SSLMode,
		)
	}
	if endpoint.TLSCAFile == "" {
		return "true", nil
	}
	certificate, err := os.ReadFile(endpoint.TLSCAFile)
	if err != nil {
		return "", fmt.Errorf("read MySQL TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return "", fmt.Errorf("read MySQL TLS CA: file contains no certificates")
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(endpoint.Host))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(certificate)
	key := "dmtx-" + hex.EncodeToString(digest.Sum(nil)[:16])
	if err := mysqlDriver.RegisterTLSConfig(
		key,
		&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: endpoint.Host,
		},
	); err != nil {
		return "", fmt.Errorf("configure MySQL TLS CA: %w", err)
	}
	return key, nil
}

// OpenMySQL verifies a MySQL or MariaDB connection without exposing its DSN.
func OpenMySQL(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	dsn, err := MySQLDSN(endpoint)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify MySQL connection: %w", err)
	}
	return database, nil
}

// ListMySQLTables returns one database's base tables in deterministic order.
func ListMySQLTables(ctx context.Context, database *sql.DB, namespace string) ([]string, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("list MySQL tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("read MySQL table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate MySQL tables: %w", err)
	}
	return tables, nil
}

// InspectMySQLTable discovers the exact MySQL 8.0 source shape supported by
// DMTX. Unsupported catalog features fail closed before any target mutation.
func InspectMySQLTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	return inspectMySQL80Table(ctx, database, namespace, name)
}
