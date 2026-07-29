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

func mysqlColumnNames(table schema.Table) []string {
	return adapterColumnNames(table)
}

func mySQLReadQuery(
	namespace string,
	table schema.Table,
	columns []string,
) string {
	return "SELECT " + mySQLQuotedColumns(columns) +
		" FROM " + mySQLQualified(namespace, table.Name) +
		" ORDER BY " + mySQLQuotedColumns(primaryKeyColumns(table))
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
