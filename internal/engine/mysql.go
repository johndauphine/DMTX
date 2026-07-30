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
	"sort"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// MySQLDSN builds a TLS-required MySQL connection string without logging or
// resolving password templates.
func MySQLDSN(endpoint config.Endpoint) (string, error) {
	return mySQLDSN(endpoint, false)
}

func mySQLDSN(
	endpoint config.Endpoint,
	refreshInformationSchemaStatistics bool,
) (string, error) {
	return mySQLDSNWithSessionParams(
		endpoint,
		refreshInformationSchemaStatistics,
		nil,
	)
}

func mySQLDSNWithSessionParams(
	endpoint config.Endpoint,
	refreshInformationSchemaStatistics bool,
	sessionParams map[string]string,
) (string, error) {
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
	if refreshInformationSchemaStatistics || len(sessionParams) > 0 {
		connection.Params = make(
			map[string]string,
			len(sessionParams)+1,
		)
	}
	if refreshInformationSchemaStatistics {
		// Oracle MySQL caches INFORMATION_SCHEMA table statistics for up to
		// 24 hours by default. Native source discovery and target identity
		// finalization require the current AUTO_INCREMENT value, not a
		// pre-DDL cached frontier. This variable is not sent by the generic
		// MySQL/MariaDB connection path.
		connection.Params["information_schema_stats_expiry"] = "0"
	}
	for name, value := range sessionParams {
		connection.Params[name] = value
	}
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
	return openMySQL(ctx, endpoint, false)
}

// OpenMySQL80 verifies the endpoint is an admitted Oracle MySQL 8.0 server,
// then opens the native-adapter connection with fresh INFORMATION_SCHEMA
// statistics on every pooled session.
func OpenMySQL80(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	probe, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if err := VerifyMySQL80Source(ctx, probe); err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("verify MySQL 8.0 connection: %w", err)
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf("close MySQL 8.0 verification connection: %w", err)
	}
	return openMySQL(ctx, endpoint, true)
}

// OpenMySQL80Target opens an Oracle MySQL 8.0 native target with constraint
// enforcement and zero-valued AUTO_INCREMENT identities pinned on every
// pooled session.
func OpenMySQL80Target(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	probe, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	catalog, err := readMySQL80ServerCatalog(ctx, probe)
	if err == nil {
		err = validateMySQL80SourceServerCatalog(catalog)
	}
	if err == nil {
		err = validateMySQL80TargetVersion(catalog)
	}
	if err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("verify MySQL 8.0 target connection: %w", err)
	}
	sqlMode, err := mysql80TargetSQLMode(catalog.sqlMode)
	if err != nil {
		_ = probe.Close()
		return nil, err
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf(
			"close MySQL 8.0 target verification connection: %w",
			err,
		)
	}
	return openMySQLWithSessionParams(
		ctx,
		endpoint,
		true,
		map[string]string{
			"foreign_key_checks": "1",
			"sql_mode":           sqlMode,
			"unique_checks":      "1",
		},
	)
}

func openMySQL(
	ctx context.Context,
	endpoint config.Endpoint,
	refreshInformationSchemaStatistics bool,
) (*sql.DB, error) {
	return openMySQLWithSessionParams(
		ctx,
		endpoint,
		refreshInformationSchemaStatistics,
		nil,
	)
}

func openMySQLWithSessionParams(
	ctx context.Context,
	endpoint config.Endpoint,
	refreshInformationSchemaStatistics bool,
	sessionParams map[string]string,
) (*sql.DB, error) {
	dsn, err := mySQLDSNWithSessionParams(
		endpoint,
		refreshInformationSchemaStatistics,
		sessionParams,
	)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open MySQL connection: %w", err)
	}
	if refreshInformationSchemaStatistics {
		// Native adapters are sequential and depend on one verified server
		// identity for the lifetime of the route. Keep their pool on one
		// session so discovery, safety checks, and mutation cannot fan out
		// across different load-balanced backends.
		database.SetMaxOpenConns(1)
		database.SetMaxIdleConns(1)
	}
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify MySQL connection: %w", err)
	}
	return database, nil
}

func mysql80TargetSQLMode(value string) (string, error) {
	modes := mysqlSQLModes(value)
	modes["NO_AUTO_VALUE_ON_ZERO"] = true
	names := make([]string, 0, len(modes))
	for mode := range modes {
		if mode == "" {
			continue
		}
		for _, character := range mode {
			if (character < 'A' || character > 'Z') &&
				character != '_' {
				return "", fmt.Errorf(
					"configure MySQL 8.0 target SQL mode: invalid mode %q",
					mode,
				)
			}
		}
		names = append(names, mode)
	}
	sort.Strings(names)
	return "'" + strings.Join(names, ",") + "'", nil
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

// InspectMySQLTable discovers the exact version-pinned MySQL or MariaDB source
// shape supported by DMTX. Unsupported catalog features fail closed before any
// target mutation.
func InspectMySQLTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	flavor, err := detectMySQLServerFlavor(ctx, database)
	if err != nil {
		return schema.Table{}, err
	}
	switch flavor {
	case mysqlServerFlavorOracle80:
		return inspectMySQL80Table(ctx, database, namespace, name)
	case mysqlServerFlavorMariaDB1011:
		return inspectMariaDB1011Table(ctx, database, namespace, name)
	default:
		return schema.Table{}, fmt.Errorf(
			"inspect MySQL table %s.%s: unsupported server flavor",
			namespace,
			name,
		)
	}
}
