package engine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/johndauphine/dmtx/internal/config"
)

const ClickHouse248VersionPrefix = "24.8."

// ClickHouseDSN creates a TLS-required ClickHouse URI without logging or
// resolving password templates.
func ClickHouseDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("ClickHouse host, database, and user are required")
	}
	if err := validateClickHouseTLSMode(endpoint.SSLMode); err != nil {
		return "", err
	}
	port := endpoint.Port
	if port == 0 {
		port = 9440
	}
	connection := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(endpoint.User, endpoint.Password),
		Host:   net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
		Path:   endpoint.Database,
	}
	query := url.Values{}
	query.Set("secure", "true")
	connection.RawQuery = query.Encode()
	return connection.String(), nil
}

func validateClickHouseTLSMode(mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "require", "verify-full":
		return nil
	default:
		return fmt.Errorf(
			"ClickHouse ssl_mode %q is unsupported; use require or verify-full",
			mode,
		)
	}
}

func clickHouseTLSConfig(endpoint config.Endpoint) (*tls.Config, error) {
	if err := validateClickHouseTLSMode(endpoint.SSLMode); err != nil {
		return nil, err
	}
	configuration := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: endpoint.Host,
	}
	if endpoint.TLSCAFile == "" {
		return configuration, nil
	}
	certificate, err := os.ReadFile(endpoint.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read ClickHouse TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return nil, fmt.Errorf(
			"read ClickHouse TLS CA: file contains no certificates",
		)
	}
	configuration.RootCAs = roots
	return configuration, nil
}

// OpenClickHouse verifies a ClickHouse connection without exposing its DSN.
func OpenClickHouse(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return nil, fmt.Errorf(
			"ClickHouse host, database, and user are required",
		)
	}
	tlsConfig, err := clickHouseTLSConfig(endpoint)
	if err != nil {
		return nil, err
	}
	port := endpoint.Port
	if port == 0 {
		port = 9440
	}
	database := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{
			net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
		},
		Auth: clickhouse.Auth{
			Database: endpoint.Database,
			Username: endpoint.User,
			Password: endpoint.Password,
		},
		TLS: tlsConfig,
	})
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify ClickHouse connection: %w", err)
	}
	return database, nil
}

// VerifyClickHouse248Target admits the pinned ClickHouse target contract.
// Other server lines and non-Atomic database engines remain unsupported until
// their DDL, catalog, and durability behavior has independent live evidence.
func VerifyClickHouse248Target(
	ctx context.Context,
	database *sql.DB,
	namespace string,
) error {
	return verifyClickHouse248Endpoint(
		ctx,
		database,
		namespace,
		"target",
	)
}

// VerifyClickHouse248Source pins source discovery to the same server and
// Atomic-database catalog line as the independently certified target.
func VerifyClickHouse248Source(
	ctx context.Context,
	database *sql.DB,
	namespace string,
) error {
	return verifyClickHouse248Endpoint(
		ctx,
		database,
		namespace,
		"source",
	)
}

func verifyClickHouse248Endpoint(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	role string,
) error {
	operation := "verify ClickHouse 24.8 " + role
	if database == nil {
		return fmt.Errorf("%s: database is required", operation)
	}
	if namespace == "" {
		return fmt.Errorf(
			"%s: database name is required",
			operation,
		)
	}
	var version string
	if err := database.QueryRowContext(
		ctx,
		"SELECT version()",
	).Scan(&version); err != nil {
		return fmt.Errorf(
			"%s version: %w",
			operation,
			err,
		)
	}
	if err := validateClickHouse248Version(version, role); err != nil {
		return err
	}
	var engineName string
	err := database.QueryRowContext(
		ctx,
		`SELECT engine
		 FROM system.databases
		 WHERE name = ?`,
		namespace,
	).Scan(&engineName)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"%s: database %q does not exist",
			operation,
			namespace,
		)
	}
	if err != nil {
		return fmt.Errorf(
			"%s database: %w",
			operation,
			err,
		)
	}
	if engineName != "Atomic" {
		return fmt.Errorf(
			"%s: database %q uses unsupported engine %q",
			operation,
			namespace,
			engineName,
		)
	}
	var indexGranularity string
	err = database.QueryRowContext(
		ctx,
		`SELECT value
		   FROM system.merge_tree_settings
		  WHERE name = 'index_granularity'`,
	).Scan(&indexGranularity)
	if err != nil {
		return fmt.Errorf(
			"%s MergeTree settings: %w",
			operation,
			err,
		)
	}
	if indexGranularity != "8192" {
		return fmt.Errorf(
			"%s: unsupported default index_granularity %q",
			operation,
			indexGranularity,
		)
	}
	return nil
}

func validateClickHouse248Version(version string, role string) error {
	if !strings.HasPrefix(version, ClickHouse248VersionPrefix) {
		return fmt.Errorf(
			"verify ClickHouse 24.8 %s: server version %q is unsupported",
			role,
			version,
		)
	}
	return nil
}

// ListClickHouseTables returns one database's ordinary tables in deterministic
// order. Views and system tables are intentionally excluded.
func ListClickHouseTables(ctx context.Context, database *sql.DB, namespace string) ([]string, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT name
		FROM system.tables
		WHERE database = ? AND is_temporary = 0 AND engine NOT IN ('View', 'MaterializedView', 'LiveView')
		ORDER BY name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("list ClickHouse tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("read ClickHouse table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ClickHouse tables: %w", err)
	}
	return tables, nil
}
