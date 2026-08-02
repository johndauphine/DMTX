package schema

import (
	"fmt"
	"reflect"
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

// SQLitePlannedIndexDDL seals one standalone SQLite index emitted by the
// deterministic CreateIndexes renderer. SQLite keeps ordinary indexes outside
// CREATE TABLE, so a complete evolution create bundle must authenticate them
// separately rather than accept adapter-supplied SQL.
func SQLitePlannedIndexDDL(table Table, index Index) (DDLStatement, error) {
	statements, err := CreateIndexes(SQLite, table)
	if err != nil {
		return DDLStatement{}, err
	}
	for position, candidate := range sqliteStandaloneIndexPositions(table.Indexes) {
		if !reflect.DeepEqual(candidate.index, index) {
			continue
		}
		for _, later := range sqliteStandaloneIndexPositions(table.Indexes)[position+1:] {
			if reflect.DeepEqual(later.index, index) {
				return DDLStatement{}, fmt.Errorf(
					"SQLite deterministic index renderer has duplicate indistinguishable index metadata",
				)
			}
		}
		if position >= len(statements) {
			break
		}
		return newDDLStatement(SQLite, statements[position])
	}
	return DDLStatement{}, fmt.Errorf(
		"SQLite index statement was not emitted by the deterministic index renderer",
	)
}

type sqliteStandaloneIndexPosition struct{ index Index }

func sqliteStandaloneIndexPositions(indexes []Index) []sqliteStandaloneIndexPosition {
	result := make([]sqliteStandaloneIndexPosition, 0, len(indexes))
	for _, index := range indexes {
		if !index.Inline {
			result = append(result, sqliteStandaloneIndexPosition{index: index})
		}
	}
	return result
}

// SQLServerCreateTableDDL seals the SQL Server-specific base-table renderer.
// SQL Server keeps its primary-key naming and rich object validation in the
// target-owned renderer rather than the generic cross-dialect CreateTable.
func SQLServerCreateTableDDL(table Table) (DDLStatement, error) {
	sql, err := CreateSQLServerTable(table)
	if err != nil {
		return DDLStatement{}, err
	}
	return newDDLStatement(SQLServer, sql)
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

// MySQLPlannedObjectDDL seals one statement from a complete deterministic
// MySQL object plan. MySQLObjectStatement has public fields for existing
// planner consumers, so the source table set is supplied again and the object
// is authenticated by recomputing the exact plan before its SQL may become an
// executable DDL boundary.
func MySQLPlannedObjectDDL(
	tables []Table,
	object MySQLObjectStatement,
) (DDLStatement, error) {
	planned, err := PlanMySQLDropRecreateObjects(tables)
	if err != nil {
		return DDLStatement{}, fmt.Errorf(
			"plan MySQL object statement: %w",
			err,
		)
	}
	for _, candidate := range planned {
		if candidate == object {
			return newDDLStatement(MySQL, candidate.SQL)
		}
	}
	return DDLStatement{}, fmt.Errorf(
		"MySQL schema object statement was not emitted by the complete object planner",
	)
}

// SQLServerPlannedObjectDDL seals one statement from a complete deterministic
// SQL Server object plan. SQLServerObjectStatement is descriptive planner
// output rather than executable authority, so recompute the full plan before
// allowing one object statement to cross the DDL boundary.
func SQLServerPlannedObjectDDL(
	tables []Table,
	object SQLServerObjectStatement,
) (DDLStatement, error) {
	planned, err := PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		return DDLStatement{}, fmt.Errorf(
			"plan SQL Server object statement: %w",
			err,
		)
	}
	for _, candidate := range planned {
		if candidate == object {
			return newDDLStatement(SQLServer, candidate.SQL)
		}
	}
	return DDLStatement{}, fmt.Errorf(
		"SQL Server schema object statement was not emitted by the complete object planner",
	)
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
