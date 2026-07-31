// Package engine owns database-engine connection and discovery adapters.
package engine

import (
	"context"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// PostgresCatalogQueryer is the read-only catalog surface used by PostgreSQL
// 16 source discovery. Both *sql.DB and *sql.Tx implement it, which lets a
// table-stable reader rerun the complete version and catalog contract through
// the exact transaction that owns its REPEATABLE READ snapshot.
type PostgresCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// PostgresDSN creates a URI connection string without logging or resolving
// password templates. Callers must keep its value out of operator output.
func PostgresDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("PostgreSQL host, database, and user are required")
	}
	sslMode, tlsCAFile, err := postgresTLSOptions(endpoint)
	if err != nil {
		return "", err
	}
	port := endpoint.Port
	if port == 0 {
		port = 5432
	}
	connection := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(endpoint.User, endpoint.Password),
		Host:   net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
		Path:   endpoint.Database,
	}
	query := url.Values{}
	query.Set("sslmode", sslMode)
	if tlsCAFile != "" {
		query.Set("sslrootcert", tlsCAFile)
	}
	connection.RawQuery = query.Encode()
	return connection.String(), nil
}

func postgresTLSOptions(endpoint config.Endpoint) (string, string, error) {
	mode := strings.ToLower(strings.TrimSpace(endpoint.SSLMode))
	if mode == "" {
		mode = "require"
	}
	switch mode {
	case "require":
		if endpoint.TLSCAFile != "" {
			return "", "", fmt.Errorf(
				"PostgreSQL tls_ca_file requires ssl_mode verify-ca or verify-full",
			)
		}
		return mode, "", nil
	case "verify-ca", "verify-full":
	default:
		return "", "", fmt.Errorf(
			"PostgreSQL ssl_mode is unsupported; use require, verify-ca, or verify-full",
		)
	}

	if strings.TrimSpace(endpoint.TLSCAFile) == "" {
		return "", "", fmt.Errorf(
			"PostgreSQL ssl_mode %s requires tls_ca_file",
			mode,
		)
	}
	certificate, err := os.ReadFile(endpoint.TLSCAFile)
	if err != nil {
		return "", "", fmt.Errorf("read PostgreSQL TLS CA: file is unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return "", "", fmt.Errorf(
			"read PostgreSQL TLS CA: file contains no certificates",
		)
	}
	return mode, endpoint.TLSCAFile, nil
}

// OpenPostgres verifies a PostgreSQL connection without exposing its DSN.
func OpenPostgres(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	dsn, err := PostgresDSN(endpoint)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify PostgreSQL connection: %w", err)
	}
	return database, nil
}

// ListPostgresTables returns one schema's base tables in deterministic order.
func ListPostgresTables(ctx context.Context, database *sql.DB, schema string) ([]string, error) {
	if schema == "" {
		schema = "public"
	}
	rows, err := database.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("read PostgreSQL table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL tables: %w", err)
	}
	return tables, nil
}

// InspectPostgresTable discovers the complete PostgreSQL 16 source schema
// contract supported by DMTX. Unsupported or ambiguous catalog shapes fail
// closed before a target adapter is opened.
func InspectPostgresTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	return InspectPostgresTableWithQueryer(
		ctx,
		database,
		namespace,
		name,
	)
}

// InspectPostgresTableWithQueryer runs the complete version-pinned PostgreSQL
// 16 table/column/identity/index/CHECK/foreign-key discovery through a
// caller-supplied queryer.
func InspectPostgresTableWithQueryer(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	namespace string,
	name string,
) (schema.Table, error) {
	if queryer == nil {
		return schema.Table{}, fmt.Errorf(
			"inspect PostgreSQL table %s.%s: catalog queryer is required",
			namespace,
			name,
		)
	}
	if namespace == "" {
		namespace = "public"
	}
	return inspectPostgres16Table(ctx, queryer, namespace, name)
}
