package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "github.com/microsoft/go-mssqldb"
)

// SQLServerDSN creates an encrypted SQL Server URI without logging or
// resolving password templates.
func SQLServerDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("SQL Server host, database, and user are required")
	}
	port := endpoint.Port
	if port == 0 {
		port = 1433
	}
	connection := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(endpoint.User, endpoint.Password),
		Host:   endpoint.Host + ":" + strconv.Itoa(port),
	}
	query := url.Values{}
	query.Set("database", endpoint.Database)
	query.Set("encrypt", "true")
	query.Set("guid conversion", "true")
	query.Set("tlsmin", "1.2")
	if endpoint.TLSCAFile != "" {
		query.Set("certificate", endpoint.TLSCAFile)
	}
	connection.RawQuery = query.Encode()
	return connection.String(), nil
}

// OpenSQLServer verifies an encrypted SQL Server connection without exposing
// its DSN.
func OpenSQLServer(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	dsn, err := SQLServerDSN(endpoint)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQL Server connection: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify SQL Server connection: %w", err)
	}
	return database, nil
}

// OpenSQLServer2022Source opens a verified SQL Server 2022 source pool with a
// single connection slot. Discovery independently checks stable object
// identities and catalog equality rather than assuming database/sql can never
// replace a failed physical connection. This opener does not provide the
// run-scoped snapshot needed for concurrent source DDL, writes, or listener
// failover; that is the separate strict-consistency contract.
func OpenSQLServer2022Source(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	database, err := OpenSQLServer(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := VerifySQLServer2022Source(ctx, database); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"verify SQL Server 2022 source: %w (close: %v)",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("verify SQL Server 2022 source: %w", err)
	}
	return database, nil
}

// ListSQLServerTables returns one schema's base tables in deterministic order.
func ListSQLServerTables(ctx context.Context, database *sql.DB, namespace string) ([]string, error) {
	if namespace == "" {
		namespace = "dbo"
	}
	rows, err := database.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = @p1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("list SQL Server tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("read SQL Server table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQL Server tables: %w", err)
	}
	return tables, nil
}

// InspectSQLServerTable discovers deterministic column and ordered primary-key
// metadata for a base table.
func InspectSQLServerTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	if namespace == "" {
		namespace = "dbo"
	}
	return inspectSQLServer2022Table(ctx, database, namespace, name)
}
