// Package engine owns database-engine connection and discovery adapters.
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/schema"
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

// InspectPostgresTable discovers deterministic column and primary-key metadata.
func InspectPostgresTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	if namespace == "" {
		namespace = "public"
	}
	rows, err := database.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, namespace, name)
	if err != nil {
		return schema.Table{}, fmt.Errorf("list PostgreSQL columns: %w", err)
	}
	defer rows.Close()
	var columns []schema.Column
	for rows.Next() {
		var column schema.Column
		var nullable string
		if err := rows.Scan(&column.Name, &column.Type, &nullable); err != nil {
			return schema.Table{}, fmt.Errorf("read PostgreSQL column: %w", err)
		}
		column.Type = normalizePostgresType(column.Type)
		column.Nullable = nullable == "YES"
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, fmt.Errorf("iterate PostgreSQL columns: %w", err)
	}
	if len(columns) == 0 {
		return schema.Table{}, fmt.Errorf("PostgreSQL table %s.%s does not exist", namespace, name)
	}
	primaryKeys, err := postgresPrimaryKeys(ctx, database, namespace, name)
	if err != nil {
		return schema.Table{}, err
	}
	return buildPostgresTable(namespace, name, columns, primaryKeys), nil
}

func postgresPrimaryKeys(ctx context.Context, database *sql.DB, namespace, name string) ([]string, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT key_column_usage.column_name
		FROM information_schema.table_constraints
		JOIN information_schema.key_column_usage
		  ON table_constraints.constraint_name = key_column_usage.constraint_name
		 AND table_constraints.table_schema = key_column_usage.table_schema
		WHERE table_constraints.table_schema = $1
		  AND table_constraints.table_name = $2
		  AND table_constraints.constraint_type = 'PRIMARY KEY'
		ORDER BY key_column_usage.ordinal_position
	`, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL primary key: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("read PostgreSQL primary key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL primary key: %w", err)
	}
	return keys, nil
}

func buildPostgresTable(namespace, name string, columns []schema.Column, primaryKeys []string) schema.Table {
	keys := make(map[string]bool, len(primaryKeys))
	for _, key := range primaryKeys {
		keys[key] = true
	}
	for index := range columns {
		columns[index].PrimaryKey = keys[columns[index].Name]
	}
	return schema.Table{Schema: namespace, Name: name, Columns: columns}
}

func normalizePostgresType(value string) string {
	switch strings.ToLower(value) {
	case "timestamp without time zone", "timestamp with time zone":
		return "timestamp"
	case "character varying":
		return "varchar"
	default:
		return value
	}
}
