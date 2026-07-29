// Package schema is the sole deterministic renderer for migrated-schema DDL.
package schema

import (
	"fmt"
	"sort"
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
	Name, Type         string
	Nullable           bool
	PrimaryKey         bool
	PrimaryKeyPosition int
	DeclaredType       *DeclaredType
	Default            *Expression
}

// DeclaredType preserves a validated catalog type declaration without carrying
// executable catalog text. Arguments are numeric SQLite type modifiers such as
// VARCHAR(40) or DECIMAL(12,2).
type DeclaredType struct {
	Base      string
	Arguments []int
}

// Expression is SQL text accepted by a dialect-specific conservative parser.
type Expression struct {
	sql string
}

func (expression Expression) CanonicalSQL() string {
	return expression.sql
}

type IndexColumn struct {
	Name       string
	Descending bool
	Collation  string
}

type Index struct {
	Name    string
	Unique  bool
	Inline  bool
	Columns []IndexColumn
}

type ForeignKey struct {
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnUpdate          string
	OnDelete          string
	Match             string
}

type CheckConstraint struct {
	Name       string
	Expression Expression
}

type Table struct {
	Schema, Name        string
	Columns             []Column
	Indexes             []Index
	ForeignKeys         []ForeignKey
	Checks              []CheckConstraint
	AutoIncrementColumn string
	SQLiteSequence      *int64
	SQLiteWithoutRowID  bool
	SQLiteStrict        bool
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
	case "real", "float", "float4", "double", "double precision", "float8":
		switch target {
		case Postgres:
			return "DOUBLE PRECISION", nil
		case SQLServer:
			return "FLOAT", nil
		case MySQL:
			return "DOUBLE", nil
		case ClickHouse:
			return "Float64", nil
		default:
			return "REAL", nil
		}
	case "decimal", "numeric":
		switch target {
		case ClickHouse:
			return "Decimal(38, 10)", nil
		case SQLite:
			return "NUMERIC", nil
		default:
			return "DECIMAL(38, 10)", nil
		}
	case "text", "varchar", "character varying":
		return "TEXT", nil
	case "uuid":
		switch target {
		case Postgres, ClickHouse:
			return "UUID", nil
		case SQLServer:
			return "UNIQUEIDENTIFIER", nil
		case MySQL:
			return "CHAR(36)", nil
		default:
			return "TEXT", nil
		}
	case "blob", "binary", "varbinary", "bytea":
		switch target {
		case Postgres:
			return "BYTEA", nil
		case SQLServer:
			return "VARBINARY(MAX)", nil
		case MySQL:
			return "LONGBLOB", nil
		case ClickHouse:
			return "String", nil
		default:
			return "BLOB", nil
		}
	case "json":
		switch target {
		case Postgres:
			return "JSON", nil
		case SQLServer:
			return "NVARCHAR(MAX)", nil
		case MySQL:
			return "JSON", nil
		case ClickHouse:
			return "String", nil
		default:
			return "TEXT", nil
		}
	case "jsonb":
		switch target {
		case Postgres:
			return "JSONB", nil
		case SQLServer:
			return "NVARCHAR(MAX)", nil
		case MySQL:
			return "JSON", nil
		case ClickHouse:
			return "String", nil
		default:
			return "TEXT", nil
		}
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
	case "timestamptz":
		if target == Postgres {
			return "TIMESTAMP WITH TIME ZONE", nil
		}
		return "", &PolicyError{
			Operation: "map type", Type: source, Target: string(target)}
	case "date":
		return "DATE", nil
	default:
		return "", &PolicyError{Operation: "map type", Type: source, Target: string(target)}
	}
}

func CreateTable(target Dialect, table Table) (string, error) {
	if len(table.Columns) == 0 {
		return "", &PolicyError{Operation: "create table", Type: "empty table", Target: string(target)}
	}
	if target != SQLite {
		if err := rejectSQLiteOnlySchema(target, table); err != nil {
			return "", err
		}
	}
	parts, pk := make([]string, 0, len(table.Columns)+1), make([]string, 0)
	for _, column := range table.Columns {
		typ, err := renderColumnType(column, target)
		if err != nil {
			return "", err
		}
		nullability := " NOT NULL"
		if column.Nullable {
			nullability = ""
		}
		definition := quote(target, column.Name) + " " + typ + nullability
		if target == SQLite && column.Default != nil {
			definition += " DEFAULT " + column.Default.sql
		}
		if target == SQLite && column.Name == table.AutoIncrementColumn {
			definition += " PRIMARY KEY AUTOINCREMENT"
		}
		parts = append(parts, definition)
	}
	for _, column := range orderedPrimaryKeyColumns(table) {
		if target == SQLite && column.Name == table.AutoIncrementColumn {
			continue
		}
		pk = append(pk, quote(target, column.Name))
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
	if target == SQLite {
		for _, index := range table.Indexes {
			if !index.Inline {
				continue
			}
			columns, err := renderSQLiteIndexColumns(index.Columns)
			if err != nil {
				return "", err
			}
			constraint := ""
			if index.Name != "" {
				constraint = "CONSTRAINT " + quote(SQLite, index.Name) + " "
			}
			parts = append(parts, constraint+"UNIQUE ("+columns+")")
		}
		for _, foreignKey := range table.ForeignKeys {
			rendered, err := renderSQLiteForeignKey(foreignKey)
			if err != nil {
				return "", err
			}
			parts = append(parts, rendered)
		}
		for _, check := range table.Checks {
			constraint := ""
			if check.Name != "" {
				constraint = "CONSTRAINT " + quote(SQLite, check.Name) + " "
			}
			parts = append(parts, constraint+"CHECK ("+check.Expression.sql+")")
		}
	}
	statement := "CREATE TABLE " + qualified(target, table.Schema, table.Name) + " (" + strings.Join(parts, ", ") + ")"
	if target == SQLite {
		options := make([]string, 0, 2)
		if table.SQLiteStrict {
			options = append(options, "STRICT")
		}
		if table.SQLiteWithoutRowID {
			options = append(options, "WITHOUT ROWID")
		}
		if len(options) > 0 {
			statement += " " + strings.Join(options, ", ")
		}
	}
	return statement + ";", nil
}

func orderedPrimaryKeyColumns(table Table) []Column {
	columns := make([]Column, 0)
	fallback := len(table.Columns) + 1
	for _, column := range table.Columns {
		if column.PrimaryKeyPosition > 0 {
			columns = append(columns, column)
			continue
		}
		if column.PrimaryKey {
			column.PrimaryKeyPosition = fallback
			fallback++
			columns = append(columns, column)
		}
	}
	sort.SliceStable(columns, func(left, right int) bool {
		return columns[left].PrimaryKeyPosition < columns[right].PrimaryKeyPosition
	})
	return columns
}
