package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// MySQLToSQLiteWithObserver migrates MySQL or MariaDB base tables through the
// shared source/target adapter runner.
func MySQLToSQLiteWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf(
			"MySQL-to-SQLite requires source.type mysql and target.type sqlite",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mysql", target: "sqlite"},
	)
}

func mySQLReadQuery(
	namespace string,
	table schema.Table,
	columns []string,
) string {
	return "SELECT " + mySQLReadProjection(table, columns) +
		" FROM " + mySQLQualified(namespace, table.Name) +
		" ORDER BY " + mySQLQuotedColumns(primaryKeyColumns(table))
}

// mySQLReadProjection retains temporal catalog text until the MySQL source
// adapter has validated it. Without this projection go-sql-driver/mysql may
// normalize zero dates to time.Time{}, making them indistinguishable from a
// legitimate finite value in another engine.
func mySQLReadProjection(table schema.Table, columns []string) string {
	types := make(map[string]string, len(table.Columns))
	for _, column := range table.Columns {
		types[column.Name] = column.Type
	}
	projected := make([]string, len(columns))
	for index, column := range columns {
		identifier := mySQLIdentifier(column)
		if isMySQLTemporalType(types[column]) {
			projected[index] = "CAST(" + identifier + " AS CHAR) AS " +
				identifier
			continue
		}
		projected[index] = identifier
	}
	return strings.Join(projected, ", ")
}

func mySQLQualified(namespace, name string) string {
	return mySQLIdentifier(namespace) + "." + mySQLIdentifier(name)
}

func mySQLQuotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = mySQLIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

func mySQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
