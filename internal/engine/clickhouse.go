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
	"github.com/johndauphine/dmtx/internal/schema"
)

const ClickHouseTargetVersionPrefix = "24.8."

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
	if database == nil {
		return fmt.Errorf("verify ClickHouse 24.8 target: database is required")
	}
	if namespace == "" {
		return fmt.Errorf(
			"verify ClickHouse 24.8 target: database name is required",
		)
	}
	var version string
	if err := database.QueryRowContext(
		ctx,
		"SELECT version()",
	).Scan(&version); err != nil {
		return fmt.Errorf(
			"verify ClickHouse 24.8 target version: %w",
			err,
		)
	}
	if !strings.HasPrefix(version, ClickHouseTargetVersionPrefix) {
		return fmt.Errorf(
			"verify ClickHouse 24.8 target: server version %q is unsupported",
			version,
		)
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
			"verify ClickHouse 24.8 target: database %q does not exist",
			namespace,
		)
	}
	if err != nil {
		return fmt.Errorf(
			"verify ClickHouse 24.8 target database: %w",
			err,
		)
	}
	if engineName != "Atomic" {
		return fmt.Errorf(
			"verify ClickHouse 24.8 target: database %q uses unsupported engine %q",
			namespace,
			engineName,
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

// InspectClickHouseTable discovers ordered columns and primary/order-key
// membership without falsely treating ClickHouse keys as relational uniqueness.
func InspectClickHouseTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT name, type, is_in_primary_key
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`, namespace, name)
	if err != nil {
		return schema.Table{}, fmt.Errorf("list ClickHouse columns: %w", err)
	}
	defer rows.Close()
	var columns []schema.Column
	for rows.Next() {
		var column schema.Column
		var inPrimaryKey uint8
		if err := rows.Scan(&column.Name, &column.Type, &inPrimaryKey); err != nil {
			return schema.Table{}, fmt.Errorf("read ClickHouse column: %w", err)
		}
		rawType := column.Type
		column.Type = normalizeClickHouseType(column.Type)
		column.Nullable = strings.HasPrefix(strings.ToLower(rawType), "nullable(")
		// ClickHouse's primary key is an ordering key, not relational uniqueness.
		column.PrimaryKey = false
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, fmt.Errorf("iterate ClickHouse columns: %w", err)
	}
	if len(columns) == 0 {
		return schema.Table{}, fmt.Errorf("ClickHouse table %s.%s does not exist", namespace, name)
	}
	return schema.Table{Schema: namespace, Name: name, Columns: columns}, nil
}

func normalizeClickHouseType(value string) string {
	value = strings.ToLower(value)
	value = strings.TrimPrefix(value, "nullable(")
	value = strings.TrimSuffix(value, ")")
	switch value {
	case "int8", "int16", "int32", "uint8", "uint16", "uint32":
		return "integer"
	case "int64", "uint64":
		return "bigint"
	case "string", "fixedstring":
		return "text"
	case "bool", "boolean":
		return "boolean"
	case "datetime", "datetime64":
		return "timestamp"
	default:
		return value
	}
}
