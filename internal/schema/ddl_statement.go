package schema

import (
	"fmt"
	"strings"
)

// DDLStatement is one opaque statement emitted by DMTX's deterministic schema
// renderer. Its SQL and dialect are intentionally private: adapter planners
// may compose renderer results, but cannot turn caller-supplied SQL into an
// executable schema-evolution boundary.
type DDLStatement struct {
	dialect Dialect
	sql     string
}

// RenderDDLStatement returns the exact renderer-owned SQL only for its bound
// dialect. The zero value and cross-dialect reuse fail closed.
func RenderDDLStatement(
	statement DDLStatement,
	target Dialect,
) (string, error) {
	if statement.dialect == "" ||
		statement.dialect != target ||
		strings.TrimSpace(statement.sql) == "" {
		return "", fmt.Errorf(
			"schema DDL statement is zero, empty, or bound to a different dialect",
		)
	}
	return statement.sql, nil
}

// CreateTableDDL renders one complete base-table statement through DMTX's sole
// deterministic create-table renderer and seals it as an opaque boundary.
func CreateTableDDL(
	target Dialect,
	table Table,
) (DDLStatement, error) {
	sql, err := CreateTable(target, table)
	if err != nil {
		return DDLStatement{}, err
	}
	return newDDLStatement(target, sql)
}

// PostgresObjectDDL seals one statement returned by
// PlanPostgresDropRecreateObjects. It accepts only the planner's additive
// index/CHECK/foreign-key object classes.
func PostgresObjectDDL(
	object PostgresObjectStatement,
) (DDLStatement, error) {
	if !object.plannerAuthentic() {
		return DDLStatement{}, fmt.Errorf(
			"PostgreSQL schema object statement was not emitted by the planner",
		)
	}
	switch object.Kind() {
	case PostgresIndexObject,
		PostgresCheckObject,
		PostgresForeignKeyObject:
	default:
		return DDLStatement{}, fmt.Errorf(
			"unsupported PostgreSQL schema object kind %d",
			object.Kind(),
		)
	}
	if strings.TrimSpace(object.Schema()) == "" ||
		strings.TrimSpace(object.Table()) == "" ||
		strings.TrimSpace(object.Name()) == "" {
		return DDLStatement{}, fmt.Errorf(
			"PostgreSQL schema object statement has incomplete identity",
		)
	}
	return newDDLStatement(Postgres, object.SQL())
}

func newDDLStatement(
	target Dialect,
	sql string,
) (DDLStatement, error) {
	switch target {
	case Postgres, SQLServer, MySQL, SQLite, ClickHouse:
	default:
		return DDLStatement{}, fmt.Errorf(
			"schema DDL statement has unsupported dialect %q",
			target,
		)
	}
	if strings.TrimSpace(sql) == "" {
		return DDLStatement{}, fmt.Errorf(
			"schema DDL statement renderer returned empty SQL",
		)
	}
	return DDLStatement{
		dialect: target,
		sql:     sql,
	}, nil
}
