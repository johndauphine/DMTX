package engine

import (
	"context"
	"database/sql"
	"fmt"
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
	connection.TLSConfig = "true"
	connection.ParseTime = true
	return connection.FormatDSN(), nil
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

// InspectMySQLTable discovers deterministic column and ordered primary-key
// metadata. Detailed modifiers remain in the adapter contract as it expands.
func InspectMySQLTable(ctx context.Context, database *sql.DB, namespace, name string) (schema.Table, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`, namespace, name)
	if err != nil {
		return schema.Table{}, fmt.Errorf("list MySQL columns: %w", err)
	}
	defer rows.Close()
	var columns []schema.Column
	for rows.Next() {
		var column schema.Column
		var nullable string
		if err := rows.Scan(&column.Name, &column.Type, &nullable); err != nil {
			return schema.Table{}, fmt.Errorf("read MySQL column: %w", err)
		}
		column.Type = normalizeMySQLType(column.Type)
		column.Nullable = nullable == "YES"
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, fmt.Errorf("iterate MySQL columns: %w", err)
	}
	if len(columns) == 0 {
		return schema.Table{}, fmt.Errorf("MySQL table %s.%s does not exist", namespace, name)
	}
	keys, err := mySQLPrimaryKeys(ctx, database, namespace, name)
	if err != nil {
		return schema.Table{}, err
	}
	return buildMySQLTable(namespace, name, columns, keys), nil
}

func mySQLPrimaryKeys(ctx context.Context, database *sql.DB, namespace, name string) ([]string, error) {
	rows, err := database.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY'
		ORDER BY seq_in_index
	`, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("list MySQL primary key: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("read MySQL primary key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate MySQL primary key: %w", err)
	}
	return keys, nil
}

func buildMySQLTable(namespace, name string, columns []schema.Column, primaryKeys []string) schema.Table {
	keys := make(map[string]bool, len(primaryKeys))
	for _, key := range primaryKeys {
		keys[key] = true
	}
	for index := range columns {
		columns[index].PrimaryKey = keys[columns[index].Name]
	}
	return schema.Table{Schema: namespace, Name: name, Columns: columns}
}

func normalizeMySQLType(value string) string {
	switch strings.ToLower(value) {
	case "int", "mediumint", "smallint":
		return "integer"
	case "bigint":
		return "bigint"
	case "varchar", "char", "tinytext", "mediumtext", "longtext":
		return "text"
	case "datetime":
		return "datetime"
	case "timestamp":
		return "timestamp"
	default:
		return strings.ToLower(value)
	}
}
