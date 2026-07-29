package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// ClickHouseDSN creates a TLS-required ClickHouse URI without logging or
// resolving password templates.
func ClickHouseDSN(endpoint config.Endpoint) (string, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return "", fmt.Errorf("ClickHouse host, database, and user are required")
	}
	port := endpoint.Port
	if port == 0 {
		port = 9440
	}
	connection := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(endpoint.User, endpoint.Password),
		Host:   endpoint.Host + ":" + strconv.Itoa(port),
		Path:   endpoint.Database,
	}
	query := url.Values{}
	query.Set("secure", "true")
	connection.RawQuery = query.Encode()
	return connection.String(), nil
}

// OpenClickHouse verifies a ClickHouse connection without exposing its DSN.
func OpenClickHouse(ctx context.Context, endpoint config.Endpoint) (*sql.DB, error) {
	dsn, err := ClickHouseDSN(endpoint)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse connection: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("verify ClickHouse connection: %w", err)
	}
	return database, nil
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
