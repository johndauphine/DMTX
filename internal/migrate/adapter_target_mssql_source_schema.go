package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectSQLServerTargetTable converts one already-discovered source table
// into the conservative shape accepted by the native SQL Server 2022 target.
// It never copies executable catalog text.
func projectSQLServerTargetTable(
	sourceEngine string,
	source schema.Table,
) (schema.Table, error) {
	switch sourceEngine {
	case "mssql":
		return cloneSQLServerTargetTable(source), nil
	case "postgres":
		return projectPostgresTableForSQLServer(source)
	case "mysql":
		return projectMySQLTableForSQLServer(source)
	case "sqlite":
		return projectSQLiteTableForSQLServer(source)
	default:
		return schema.Table{}, fmt.Errorf(
			"SQL Server target does not support source engine %q",
			sourceEngine,
		)
	}
}

func cloneSQLServerTargetTable(source schema.Table) schema.Table {
	cloned := source
	cloned.Identity = cloneSchemaIdentity(source.Identity)
	cloned.Columns = append([]schema.Column(nil), source.Columns...)
	for index := range cloned.Columns {
		if source.Columns[index].DeclaredType != nil {
			declaration := *source.Columns[index].DeclaredType
			declaration.Arguments = append(
				[]int(nil),
				source.Columns[index].DeclaredType.Arguments...,
			)
			cloned.Columns[index].DeclaredType = &declaration
		}
		cloned.Columns[index].Default = cloneSchemaExpression(
			source.Columns[index].Default,
		)
	}
	cloned.Indexes = clonePostgresProjectionIndexes(source.Indexes)
	cloned.ForeignKeys = clonePostgresProjectionForeignKeys(
		source.ForeignKeys,
	)
	cloned.Checks = append([]schema.CheckConstraint(nil), source.Checks...)
	return cloned
}

func projectPostgresTableForSQLServer(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID ||
		source.MySQLCollation != "" {
		return schema.Table{}, sqlServerProjectionPolicy(
			"map PostgreSQL table metadata",
			source.Name,
		)
	}
	projected := cloneSQLServerTargetTable(source)
	for index, column := range source.Columns {
		target, err := projectPostgresColumnForSQLServer(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL column %s.%s to SQL Server: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		sourceColumns[column.Name] = column
		if column.PrimaryKeyPosition > 0 &&
			postgresTextColumnForSQLServer(column) {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL text primary-key collation",
				source.Name+"."+column.Name,
			)
		}
		if column.PrimaryKeyPosition > 0 &&
			postgresUUIDColumnForSQLServer(column) {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL UUID primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
	}
	for _, index := range source.Indexes {
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL index column",
					source.Name+"."+indexed.Name,
				)
			}
			if postgresTextColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if postgresUUIDColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}
	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "SIMPLE"
		default:
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, name := range foreignKey.Columns {
			if postgresTextColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text foreign key collation",
					source.Name+"."+name,
				)
			}
			if postgresUUIDColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID foreign-key comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	for _, check := range source.Checks {
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, err
		}
		for _, name := range referenced {
			if postgresTextColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text CHECK collation",
					source.Name+"."+name,
				)
			}
			if postgresUUIDColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID CHECK comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	return projected, nil
}

func projectPostgresColumnForSQLServer(
	source schema.Column,
) (schema.Column, error) {
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	base := strings.ToLower(strings.TrimSpace(source.Type))
	declaredBase := base
	var arguments []int
	if source.DeclaredType != nil {
		declaredBase = strings.ToLower(strings.TrimSpace(
			source.DeclaredType.Base,
		))
		arguments = append(
			[]int(nil),
			source.DeclaredType.Arguments...,
		)
	}
	if declaredBase != base {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL declared type",
			source.Name,
		)
	}
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
	}
	requireNoArguments := func() error {
		if len(arguments) != 0 {
			return sqlServerProjectionPolicy(
				"map PostgreSQL type modifier",
				source.Name,
			)
		}
		return nil
	}

	switch base {
	case "integer":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "integer"
		declaration("int")
	case "bigint":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "bigint"
		declaration("bigint")
	case "real":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "real"
		declaration("real")
	case "double precision":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "double precision"
		declaration("double precision")
	case "numeric":
		if len(arguments) != 2 ||
			arguments[0] < 1 || arguments[0] > 38 ||
			arguments[1] < 0 || arguments[1] > arguments[0] {
			return schema.Column{}, sqlServerProjectionPolicy(
				"map PostgreSQL numeric modifier",
				source.Name,
			)
		}
		target.Type = "numeric"
		declaration("decimal", arguments...)
	case "char":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL character type",
			"fixed-width blank-padding semantics cannot be preserved",
		)
	case "varchar":
		if len(arguments) != 1 ||
			arguments[0] < 1 || arguments[0] > 2_000 {
			return schema.Column{}, sqlServerProjectionPolicy(
				"map PostgreSQL character modifier",
				source.Name,
			)
		}
		target.Type = "text"
		declaration("varchar", arguments[0]*4)
	case "text", "jsonb":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "text"
		declaration("text")
	case "json":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL type",
			"json source text is normalized differently by drivers",
		)
	case "bytea":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "blob"
		declaration("blob")
	case "boolean":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "boolean"
		declaration("bool")
	case "date":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "date"
		declaration("date")
	case "time":
		precision, err := postgresTemporalPrecisionForSQLServer(
			source.Name,
			arguments,
		)
		if err != nil {
			return schema.Column{}, err
		}
		target.Type = "time"
		declaration("time", precision)
	case "timestamp":
		precision, err := postgresTemporalPrecisionForSQLServer(
			source.Name,
			arguments,
		)
		if err != nil {
			return schema.Column{}, err
		}
		target.Type = "datetime"
		declaration("timestamp", precision)
	case "uuid":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "uuid"
		declaration("uuid")
	case "timestamptz":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL type",
			base,
		)
	default:
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL type",
			base,
		)
	}
	if target.Default != nil {
		switch strings.ToUpper(strings.TrimSpace(
			target.Default.CanonicalSQL(),
		)) {
		case "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
			return schema.Column{}, sqlServerProjectionPolicy(
				"map PostgreSQL clock default",
				source.Name,
			)
		}
	}
	return target, nil
}

func postgresTemporalPrecisionForSQLServer(
	column string,
	arguments []int,
) (int, error) {
	precision := 6
	if len(arguments) == 1 {
		precision = arguments[0]
	} else if len(arguments) != 0 {
		return 0, sqlServerProjectionPolicy(
			"map PostgreSQL temporal modifier",
			column,
		)
	}
	if precision < 0 || precision > 6 {
		return 0, sqlServerProjectionPolicy(
			"map PostgreSQL temporal modifier",
			column,
		)
	}
	return precision, nil
}

func postgresTextColumnForSQLServer(column schema.Column) bool {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "char", "varchar", "text", "json", "jsonb":
		return true
	default:
		return false
	}
}

func postgresUUIDColumnForSQLServer(column schema.Column) bool {
	return strings.EqualFold(strings.TrimSpace(column.Type), "uuid")
}

func sqlServerProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLServer),
	}
}
