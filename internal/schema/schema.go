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

// IdentityGeneration describes how a target generates values when an
// identity column is omitted from an INSERT. Stage 3 supports only BY DEFAULT
// identities because migrations must remain able to load explicit source keys.
type IdentityGeneration string

const (
	IdentityByDefault IdentityGeneration = "by_default"
)

// Identity is engine-neutral metadata for the narrow identity shape DMTX can
// preserve exactly across supported engines. Frontier is the greatest value
// already allocated by the source generator, even when no row currently owns
// that value.
type Identity struct {
	Column     string
	Generation IdentityGeneration
	Frontier   *int64
}

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

type expressionKind uint8

const (
	expressionUnknown expressionKind = iota
	expressionNull
	expressionBoolean
	expressionNumber
	expressionString
	expressionBlob
	expressionCurrentTime
	expressionCurrentDate
	expressionCurrentTimestamp
	expressionCheck
)

// Expression is a value accepted by a dialect-specific conservative parser.
// The original SQL is retained for same-dialect rendering while kind and
// literal keep cross-dialect rendering structural instead of copying catalog
// text into target DDL.
type Expression struct {
	sql     string
	kind    expressionKind
	literal string
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
	Name              string
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
	Schema, Name   string
	MySQLCollation string
	// ClickHouseOrderBy is physical ordering metadata, never a relational
	// primary-key or uniqueness claim.
	ClickHouseOrderBy  []string
	Identity           *Identity
	Columns            []Column
	Indexes            []Index
	ForeignKeys        []ForeignKey
	Checks             []CheckConstraint
	SQLiteWithoutRowID bool
	SQLiteStrict       bool
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
		if target == ClickHouse {
			return "String", nil
		}
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
		if target == ClickHouse {
			return "Bool", nil
		}
		return "BOOLEAN", nil
	case "timestamp", "datetime":
		if target == SQLServer {
			return "DATETIME2", nil
		}
		if target == ClickHouse {
			return "DateTime64(6)", nil
		}
		return "TIMESTAMP", nil
	case "timestamptz":
		if target == Postgres {
			return "TIMESTAMP WITH TIME ZONE", nil
		}
		return "", &PolicyError{
			Operation: "map type", Type: source, Target: string(target)}
	case "date":
		if target == ClickHouse {
			return "Date", nil
		}
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
	postgresIdentity := ""
	if target == Postgres {
		var err error
		postgresIdentity, err = postgresIdentityColumn(table)
		if err != nil {
			return "", err
		}
	}
	mysqlIdentity := ""
	mysqlCollation := ""
	if target == MySQL {
		var err error
		mysqlIdentity, err = mysqlIdentityColumn(table)
		if err != nil {
			return "", err
		}
		if err := validateMySQLDeclaredRowSize(table); err != nil {
			return "", err
		}
		mysqlCollation, err = mysqlTableCollation(table)
		if err != nil {
			return "", err
		}
	}
	sqliteIdentity := ""
	if target == SQLite {
		var err error
		sqliteIdentity, err = sqliteIdentityColumn(table)
		if err != nil {
			return "", err
		}
	}
	parts, pk := make([]string, 0, len(table.Columns)+1), make([]string, 0)
	for _, column := range table.Columns {
		var typ string
		if target == SQLite && column.Name == sqliteIdentity {
			typ = "INTEGER"
		} else {
			var err error
			typ, err = renderColumnType(column, target)
			if err != nil {
				return "", err
			}
		}
		nullability := " NOT NULL"
		if target == ClickHouse {
			nullability = ""
			if column.Nullable {
				typ = "Nullable(" + typ + ")"
			}
		} else if column.Nullable {
			nullability = ""
		}
		identity := ""
		if target == Postgres && column.Name == postgresIdentity {
			identity = " GENERATED BY DEFAULT AS IDENTITY"
		}
		if target == MySQL && column.Name == mysqlIdentity {
			identity = " AUTO_INCREMENT"
		}
		definition := quote(target, column.Name) + " " + typ + identity + nullability
		if column.Default != nil {
			renderedDefault, err := renderDefault(target, column)
			if err != nil {
				return "", err
			}
			definition += " DEFAULT " + renderedDefault
		}
		if target == SQLite && column.Name == sqliteIdentity {
			definition += " PRIMARY KEY AUTOINCREMENT"
		}
		parts = append(parts, definition)
	}
	for _, column := range orderedPrimaryKeyColumns(table) {
		if target == SQLite && column.Name == sqliteIdentity {
			continue
		}
		pk = append(pk, quote(target, column.Name))
	}
	if target == ClickHouse {
		orderBy := "tuple()"
		clickHouseOrder, err := clickHouseOrderByColumns(table)
		if err != nil {
			return "", err
		}
		if len(clickHouseOrder) > 0 {
			orderBy = "(" + strings.Join(clickHouseOrder, ", ") + ")"
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
	if target == MySQL {
		statement += " ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4" +
			" COLLATE=" + mysqlCollation + " ROW_FORMAT=DYNAMIC"
	}
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

func clickHouseOrderByColumns(table Table) ([]string, error) {
	if len(table.ClickHouseOrderBy) == 0 {
		return nil, nil
	}
	columns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"ClickHouse table %s has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := columns[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"ClickHouse table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		columns[column.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(table.ClickHouseOrderBy))
	orderBy := make([]string, len(table.ClickHouseOrderBy))
	for index, name := range table.ClickHouseOrderBy {
		if _, exists := columns[name]; !exists {
			return nil, fmt.Errorf(
				"ClickHouse table %s ordering column %s is absent",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"ClickHouse table %s has duplicate ordering column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
		orderBy[index] = quote(ClickHouse, name)
	}
	return orderBy, nil
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
