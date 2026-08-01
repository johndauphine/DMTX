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
	var target schema.Table
	var err error
	switch sourceEngine {
	case "mssql":
		target = cloneSQLServerTargetTable(source)
	case "postgres":
		target, err = projectPostgresTableForSQLServer(source)
	case "mysql":
		target, err = projectMySQLTableForSQLServer(source)
	case "sqlite":
		target, err = projectSQLiteTableForSQLServer(source)
	default:
		return schema.Table{}, fmt.Errorf(
			"SQL Server target does not support source engine %q",
			sourceEngine,
		)
	}
	if err != nil {
		return schema.Table{}, err
	}
	if err := canonicalizeSQLServerTargetChecks(&target); err != nil {
		return schema.Table{}, err
	}
	if err := canonicalizeSQLServerTargetForeignKeys(&target); err != nil {
		return schema.Table{}, err
	}
	return target, nil
}

// canonicalizeSQLServerTargetChecks freezes a portable CHECK in exactly the
// AST form returned by SQL Server's catalog after DMTX renders it. SQL Server
// always brackets identifiers, so retaining a source spelling such as id > 0
// would otherwise make an immediately reread post-DDL catalog look like mixed
// drift despite identical CHECK semantics.
func canonicalizeSQLServerTargetChecks(table *schema.Table) error {
	if table == nil {
		return fmt.Errorf("SQL Server target CHECK canonicalization table is nil")
	}
	for index := range table.Checks {
		rendered, err := schema.RenderPortableCheckForSQLServer(
			table.Checks[index].Expression,
			table.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"canonicalize SQL Server target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		expression, err := schema.ParseSQLServerCatalogCheck(
			rendered,
			table.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"parse planned SQL Server target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		table.Checks[index].Expression = expression
	}
	return nil
}

// canonicalizeSQLServerTargetForeignKeys freezes the SQL Server catalog's
// only supported MATCH spelling. SQL Server implements SIMPLE semantics and
// reports that canonical form even when a portable source model uses NONE.
func canonicalizeSQLServerTargetForeignKeys(table *schema.Table) error {
	if table == nil {
		return fmt.Errorf("SQL Server target foreign-key canonicalization table is nil")
	}
	for index := range table.ForeignKeys {
		foreignKey := &table.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "SIMPLE"
		default:
			return fmt.Errorf(
				"canonicalize SQL Server target foreign key %s.%s: unsupported MATCH %q",
				table.Name,
				foreignKey.Name,
				foreignKey.Match,
			)
		}
	}
	return nil
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
