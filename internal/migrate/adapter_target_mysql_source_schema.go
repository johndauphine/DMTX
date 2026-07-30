package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectMySQLTargetTable converts one already-discovered source table into
// the exact, conservative shape accepted by the native MySQL 8 target. The
// result contains no executable catalog text: defaults and CHECKs remain
// structured schema expressions.
func projectMySQLTargetTable(
	sourceEngine string,
	sourceTable schema.Table,
) (schema.Table, error) {
	switch sourceEngine {
	case "mysql":
		return cloneMySQLTargetTable(sourceTable), nil
	case "postgres":
		return projectPostgresTableForMySQL(sourceTable)
	default:
		return schema.Table{}, fmt.Errorf(
			"MySQL target does not support source engine %q",
			sourceEngine,
		)
	}
}

func cloneMySQLTargetTable(source schema.Table) schema.Table {
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

func projectPostgresTableForMySQL(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID {
		return schema.Table{}, mysqlProjectionPolicy(
			"map PostgreSQL table metadata",
			source.Name,
		)
	}
	projected := cloneMySQLTargetTable(source)
	projected.MySQLCollation = "utf8mb4_0900_bin"
	for index, column := range source.Columns {
		target, err := projectPostgresColumnForMySQL(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL column %s.%s to MySQL: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}
	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, mysqlProjectionPolicy(
				"map PostgreSQL foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			if strings.EqualFold(strings.TrimSpace(action), "SET DEFAULT") {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL foreign-key action",
					source.Name+"."+foreignKey.Name,
				)
			}
		}
	}
	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		sourceColumns[column.Name] = column
		if column.PrimaryKeyPosition > 0 &&
			postgresTextColumnForMySQL(column) {
			return schema.Table{}, mysqlProjectionPolicy(
				"map PostgreSQL text primary key collation",
				source.Name+"."+column.Name,
			)
		}
	}
	for _, index := range source.Indexes {
		for _, indexed := range index.Columns {
			if postgresTextColumnForMySQL(sourceColumns[indexed.Name]) &&
				!strings.EqualFold(indexed.Collation, "BINARY") {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text index collation",
					source.Name+"."+indexed.Name,
				)
			}
		}
	}
	for _, foreignKey := range source.ForeignKeys {
		for _, name := range foreignKey.Columns {
			if postgresTextColumnForMySQL(sourceColumns[name]) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text foreign key collation",
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
			if postgresTextColumnForMySQL(sourceColumns[name]) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text CHECK collation",
					source.Name+"."+name,
				)
			}
		}
	}
	for _, column := range projected.Columns {
		if !mySQLProjectedBoolean(column) {
			continue
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			mySQLIdentifier(column.Name)+" IN (0, 1)",
			projected.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"plan MySQL boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(projected.Checks, schema.CheckConstraint{
			Name:       mySQLBooleanCheckName(source, column.Name),
			Expression: expression,
		})
	}
	return projected, nil
}

func postgresTextColumnForMySQL(column schema.Column) bool {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "char", "varchar", "text":
		return true
	default:
		return false
	}
}

func mySQLBooleanCheckName(table schema.Table, column string) string {
	digest := sha256.Sum256([]byte(
		table.Schema + "\x00" + table.Name + "\x00" + column,
	))
	return "dmtx_bool_" + hex.EncodeToString(digest[:8])
}

func projectPostgresColumnForMySQL(
	source schema.Column,
) (schema.Column, error) {
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	base := strings.ToLower(strings.TrimSpace(source.Type))
	arguments := []int(nil)
	declaredBase := base
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
		return schema.Column{}, mysqlProjectionPolicy(
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
			return mysqlProjectionPolicy(
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
	case "double precision":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "double precision"
		declaration("double")
	case "numeric":
		if len(arguments) != 2 ||
			arguments[0] < 1 || arguments[0] > 65 ||
			arguments[1] < 0 || arguments[1] > 30 ||
			arguments[1] > arguments[0] {
			return schema.Column{}, mysqlProjectionPolicy(
				"map PostgreSQL numeric modifier",
				source.Name,
			)
		}
		target.Type = "numeric"
		declaration("decimal", arguments...)
	case "char":
		return schema.Column{}, mysqlProjectionPolicy(
			"map PostgreSQL character type",
			"fixed-width blank-padding semantics cannot be preserved",
		)
	case "varchar":
		if len(arguments) != 1 ||
			arguments[0] < 1 || arguments[0] > 16_383 {
			return schema.Column{}, mysqlProjectionPolicy(
				"map PostgreSQL character modifier",
				source.Name,
			)
		}
		target.Type = "varchar"
		declaration("varchar", arguments...)
	case "text":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "text"
		declaration("longtext")
	case "bytea":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "blob"
		declaration("longblob")
	case "jsonb":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		// PostgreSQL jsonb has a deterministic textual form with numeric
		// precision beyond MySQL's binary JSON number domain. Preserve that
		// canonical source representation as LONGTEXT instead of allowing a
		// warning-free numeric rewrite inside MySQL JSON.
		target.Type = "text"
		declaration("longtext")
	case "json":
		return schema.Column{}, mysqlProjectionPolicy(
			"map PostgreSQL type",
			"json preserves source text that MySQL JSON normalizes",
		)
	case "boolean":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "integer"
		declaration("tinyint", 1)
		if source.Default != nil {
			value := strings.ToUpper(strings.TrimSpace(
				source.Default.CanonicalSQL(),
			))
			switch value {
			case "TRUE":
				value = "1"
			case "FALSE":
				value = "0"
			default:
				return schema.Column{}, mysqlProjectionPolicy(
					"map PostgreSQL boolean default",
					source.Name,
				)
			}
			expression, err := schema.ParseMySQLCatalogDefault(
				target,
				&value,
				false,
			)
			if err != nil {
				return schema.Column{}, err
			}
			target.Default = expression
		}
	case "date":
		if err := requireNoArguments(); err != nil {
			return schema.Column{}, err
		}
		target.Type = "date"
		declaration("date")
	case "timestamp":
		precision := 6
		if len(arguments) == 1 {
			precision = arguments[0]
		} else if len(arguments) != 0 {
			return schema.Column{}, mysqlProjectionPolicy(
				"map PostgreSQL temporal modifier",
				source.Name,
			)
		}
		if precision < 0 || precision > 6 {
			return schema.Column{}, mysqlProjectionPolicy(
				"map PostgreSQL temporal modifier",
				source.Name,
			)
		}
		target.Type = "datetime"
		declaration("datetime", precision)
	case "uuid", "timestamptz":
		return schema.Column{}, mysqlProjectionPolicy(
			"map PostgreSQL type",
			base,
		)
	default:
		return schema.Column{}, mysqlProjectionPolicy(
			"map PostgreSQL type",
			base,
		)
	}
	if target.Default != nil {
		normalized, err := schema.NormalizeMySQLDefault(target)
		if err != nil {
			return schema.Column{}, fmt.Errorf(
				"normalize MySQL default for %s: %w",
				source.Name,
				err,
			)
		}
		target.Default = normalized
	}
	return target, nil
}

func mySQLProjectedBoolean(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	return strings.EqualFold(column.DeclaredType.Base, "tinyint") &&
		len(column.DeclaredType.Arguments) == 1 &&
		column.DeclaredType.Arguments[0] == 1
}

func mysqlProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}
