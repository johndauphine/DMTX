// Package engine owns database-engine connection and discovery adapters.
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/DMTX/internal/config"
)

// PostgresDSN creates a URI connection string without logging or resolving
// password templates. Callers must keep its value out of operator output.
func PostgresDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("PostgreSQL host, database, and user are required")
	}
	port := endpoint.Port
	if port == 0 {
		port = 5432
	}
	connection := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(endpoint.User, endpoint.Password),
		Host:   endpoint.Host + ":" + strconv.Itoa(port),
		Path:   endpoint.Database,
	}
	query := url.Values{}
	query.Set("sslmode", "require")
	connection.RawQuery = query.Encode()
	return connection.String(), nil
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
