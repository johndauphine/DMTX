package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

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
	rows, err := database.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = @p1 AND table_name = @p2
		ORDER BY ordinal_position
	`, namespace, name)
	if err != nil {
		return schema.Table{}, fmt.Errorf("list SQL Server columns: %w", err)
	}
	defer rows.Close()
	var columns []schema.Column
	for rows.Next() {
		var column schema.Column
		var nullable string
		if err := rows.Scan(&column.Name, &column.Type, &nullable); err != nil {
			return schema.Table{}, fmt.Errorf("read SQL Server column: %w", err)
		}
		column.Type = normalizeSQLServerType(column.Type)
		column.Nullable = nullable == "YES"
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, fmt.Errorf("iterate SQL Server columns: %w", err)
	}
	if len(columns) == 0 {
		return schema.Table{}, fmt.Errorf("SQL Server table %s.%s does not exist", namespace, name)
	}
	keys, err := sqlServerPrimaryKeys(ctx, database, namespace, name)
	if err != nil {
		return schema.Table{}, err
	}
	return buildSQLServerTable(namespace, name, columns, keys), nil
}

func sqlServerPrimaryKeys(ctx context.Context, database *sql.DB, namespace, name string) ([]string, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT key_column_usage.column_name
		FROM information_schema.table_constraints
		JOIN information_schema.key_column_usage
		  ON table_constraints.constraint_name = key_column_usage.constraint_name
		 AND table_constraints.table_schema = key_column_usage.table_schema
		WHERE table_constraints.table_schema = @p1
		  AND table_constraints.table_name = @p2
		  AND table_constraints.constraint_type = 'PRIMARY KEY'
		ORDER BY key_column_usage.ordinal_position
	`, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("list SQL Server primary key: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("read SQL Server primary key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQL Server primary key: %w", err)
	}
	return keys, nil
}

func buildSQLServerTable(namespace, name string, columns []schema.Column, primaryKeys []string) schema.Table {
	keys := make(map[string]bool, len(primaryKeys))
	for _, key := range primaryKeys {
		keys[key] = true
	}
	for index := range columns {
		columns[index].PrimaryKey = keys[columns[index].Name]
	}
	return schema.Table{Schema: namespace, Name: name, Columns: columns}
}

func normalizeSQLServerType(value string) string {
	switch strings.ToLower(value) {
	case "int", "smallint", "tinyint":
		return "integer"
	case "bigint":
		return "bigint"
	case "nvarchar", "varchar", "nchar", "char":
		return "text"
	case "bit":
		return "boolean"
	case "datetime", "datetime2", "datetimeoffset":
		return "datetime"
	default:
		return strings.ToLower(value)
	}
}
