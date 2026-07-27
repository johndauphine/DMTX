// Package schema is the sole deterministic renderer for migrated-schema DDL.
package schema

import (
	"fmt"
	"strings"
)

type Dialect string

const (
	Postgres   Dialect = "postgres"
	SQLServer  Dialect = "mssql"
	MySQL      Dialect = "mysql"
	SQLite     Dialect = "sqlite"
	ClickHouse Dialect = "clickhouse"
)

type Column struct {
	Name, Type string
	Nullable   bool
	PrimaryKey bool
}
type Table struct {
	Schema, Name string
	Columns      []Column
}

type PolicyError struct{ Operation, Type, Target string }

func (e *PolicyError) Error() string {
	return fmt.Sprintf("schema policy: %s type %q is unsupported for %s", e.Operation, e.Type, e.Target)
}

func quote(d Dialect, ident string) string {
	if d == MySQL {
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
	if d == SQLServer {
		return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
func qualified(d Dialect, schema, name string) string {
	if schema == "" {
		return quote(d, name)
	}
	return quote(d, schema) + "." + quote(d, name)
}

func MapType(source string, target Dialect) (string, error) {
	switch strings.ToLower(source) {
	case "int", "integer", "int4":
		if target == ClickHouse {
			return "Int32", nil
		}
		return "INTEGER", nil
	case "bigint", "int8":
		if target == ClickHouse {
			return "Int64", nil
		}
		return "BIGINT", nil
	case "text", "varchar", "character varying":
		return "TEXT", nil
	case "bool", "boolean":
		if target == SQLServer {
			return "BIT", nil
		}
		if target == MySQL {
			return "BOOLEAN", nil
		}
		return "BOOLEAN", nil
	case "timestamp", "datetime":
		if target == SQLServer {
			return "DATETIME2", nil
		}
		return "TIMESTAMP", nil
	default:
		return "", &PolicyError{Operation: "map type", Type: source, Target: string(target)}
	}
}

func CreateTable(target Dialect, table Table) (string, error) {
	if len(table.Columns) == 0 {
		return "", &PolicyError{Operation: "create table", Type: "empty table", Target: string(target)}
	}
	parts, pk := make([]string, 0, len(table.Columns)+1), make([]string, 0)
	for _, column := range table.Columns {
		typ, err := MapType(column.Type, target)
		if err != nil {
			return "", err
		}
		nullability := " NOT NULL"
		if column.Nullable {
			nullability = ""
		}
		parts = append(parts, quote(target, column.Name)+" "+typ+nullability)
		if column.PrimaryKey {
			pk = append(pk, quote(target, column.Name))
		}
	}
	if target == ClickHouse {
		orderBy := "tuple()"
		if len(pk) > 0 {
			orderBy = "(" + strings.Join(pk, ", ") + ")"
		}
		return "CREATE TABLE " + qualified(target, table.Schema, table.Name) + " (" + strings.Join(parts, ", ") + ") ENGINE = MergeTree ORDER BY " + orderBy + ";", nil
	}
	if len(pk) > 0 {
		parts = append(parts, "PRIMARY KEY ("+strings.Join(pk, ", ")+")")
	}
	return "CREATE TABLE " + qualified(target, table.Schema, table.Name) + " (" + strings.Join(parts, ", ") + ");", nil
}
