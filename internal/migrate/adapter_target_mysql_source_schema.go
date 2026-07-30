package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

// projectMySQLTargetTable converts one already-discovered source table into the
// exact, conservative shape accepted by the selected native MySQL-family
// target. The result contains no executable catalog text: defaults and CHECKs
// remain structured schema expressions.
func projectMySQLTargetTable(
	sourceEngine string,
	sourceTable schema.Table,
	targetFlavor engine.MySQLServerFlavor,
) (schema.Table, error) {
	var target schema.Table
	var err error
	switch sourceEngine {
	case "mysql":
		target = cloneMySQLTargetTable(sourceTable)
	case "postgres":
		target, err = projectPostgresTableForMySQL(sourceTable)
	case "mssql":
		target, err = projectSQLServerTableForMySQL(sourceTable)
	case "sqlite":
		target, err = projectSQLiteTableForMySQL(sourceTable)
	default:
		return schema.Table{}, fmt.Errorf(
			"MySQL target does not support source engine %q",
			sourceEngine,
		)
	}
	if err != nil {
		return schema.Table{}, err
	}
	if err := normalizeMySQLTargetCollation(
		sourceEngine,
		&target,
		targetFlavor,
	); err != nil {
		return schema.Table{}, err
	}
	return target, nil
}

func normalizeMySQLTargetCollation(
	sourceEngine string,
	table *schema.Table,
	targetFlavor engine.MySQLServerFlavor,
) error {
	collation := strings.ToLower(strings.TrimSpace(table.MySQLCollation))
	switch targetFlavor {
	case engine.MySQLServerFlavorOracle80:
		if sourceEngine == "postgres" ||
			sourceEngine == "mssql" ||
			sourceEngine == "sqlite" {
			table.MySQLCollation = "utf8mb4_0900_bin"
			return nil
		}
		switch collation {
		case "utf8mb4_bin", "utf8mb4_0900_bin":
			table.MySQLCollation = collation
			return nil
		}
	case engine.MySQLServerFlavorMariaDB1011:
		if sourceEngine == "postgres" ||
			sourceEngine == "mssql" ||
			sourceEngine == "sqlite" {
			table.MySQLCollation = "utf8mb4_nopad_bin"
			return nil
		}
		if collation == "utf8mb4_nopad_bin" {
			table.MySQLCollation = collation
			return nil
		}
	default:
		return fmt.Errorf("unsupported MySQL target flavor")
	}
	return mysqlProjectionPolicy(
		"map MySQL-family table collation",
		table.Name+"."+table.MySQLCollation,
	)
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

// projectSQLServerTableForMySQL maps only the SQL Server 2022 source shape
// whose stored values and relational objects have an exact MySQL-family
// representation. Scalar text is safe because both admitted targets use a
// deterministic binary/no-pad collation, but text, binary, and UUID
// comparison roles remain fail-closed because their engine equality and
// padding contracts differ.
func projectSQLServerTableForMySQL(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID ||
		strings.TrimSpace(source.MySQLCollation) != "" {
		return schema.Table{}, mysqlProjectionPolicy(
			"map SQL Server table metadata",
			source.Name,
		)
	}
	projected := cloneMySQLTargetTable(source)
	for index, column := range source.Columns {
		target, err := projectSQLServerColumnForMySQL(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQL Server column %s.%s to MySQL: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(
		map[string]schema.Column,
		len(source.Columns),
	)
	for _, column := range source.Columns {
		if column.Name == "" {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server column",
				source.Name,
			)
		}
		if _, exists := sourceColumns[column.Name]; exists {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server columns",
				source.Name+"."+column.Name,
			)
		}
		sourceColumns[column.Name] = column
		if (column.PrimaryKey ||
			column.PrimaryKeyPosition > 0) &&
			sqlServerMySQLNonportableComparison(column) {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
	}

	for _, index := range source.Indexes {
		if index.Inline {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server index shape",
				source.Name+"."+index.Name,
			)
		}
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists ||
				strings.TrimSpace(indexed.Collation) != "" ||
				sqlServerMySQLNonportableComparison(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server index comparison",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			if strings.EqualFold(
				strings.TrimSpace(action),
				"SET DEFAULT",
			) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server foreign-key action",
					source.Name+"."+foreignKey.Name,
				)
			}
		}
		for _, name := range foreignKey.Columns {
			column, exists := sourceColumns[name]
			if !exists ||
				sqlServerMySQLNonportableComparison(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server foreign-key comparison",
					source.Name+"."+foreignKey.Name+"."+name,
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
			return schema.Table{}, fmt.Errorf(
				"map SQL Server CHECK %s.%s to MySQL: %w",
				source.Name,
				check.Name,
				err,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists ||
				sqlServerMySQLNonportableComparison(column) ||
				sqlServerMySQLRealColumn(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server CHECK comparison",
					source.Name+"."+check.Name+"."+name,
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
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{
				Name: mySQLBooleanCheckName(
					source,
					column.Name,
				),
				Expression: expression,
			},
		)
	}
	return projected, nil
}

func projectSQLServerColumnForMySQL(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server declared type",
			source.Name,
		)
	}
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	base := strings.ToLower(strings.Join(
		strings.Fields(source.DeclaredType.Base),
		" ",
	))
	arguments := append(
		[]int(nil),
		source.DeclaredType.Arguments...,
	)
	sourceType := strings.ToLower(strings.Join(
		strings.Fields(source.Type),
		" ",
	))
	mapped := false
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
		mapped = true
	}
	noArguments := func() bool {
		return len(arguments) == 0
	}

	switch base {
	case "tinyint":
		// SQL Server TINYINT is unsigned (0..255); MySQL TINYINT is signed.
		if sourceType != "integer" || !noArguments() {
			break
		}
		target.Type = "integer"
		declaration("smallint")
	case "smallint":
		if sourceType != "integer" || !noArguments() {
			break
		}
		target.Type = "integer"
		declaration("smallint")
	case "int", "integer":
		if sourceType != "integer" || !noArguments() {
			break
		}
		target.Type = "integer"
		declaration("int")
	case "bigint":
		if sourceType != "bigint" || !noArguments() {
			break
		}
		target.Type = "bigint"
		declaration("bigint")
	case "bool", "boolean":
		if sourceType != "boolean" || !noArguments() {
			break
		}
		target.Type = "integer"
		declaration("tinyint", 1)
	case "decimal", "numeric":
		if sourceType != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 38 ||
			arguments[1] < 0 ||
			arguments[1] > 30 ||
			arguments[1] > arguments[0] {
			break
		}
		target.Type = "numeric"
		declaration("decimal", arguments...)
	case "real":
		// Every IEEE-754 binary32 value is represented exactly by DOUBLE.
		// A source REAL default is rejected because re-evaluating its decimal
		// token as DOUBLE would not reproduce SQL Server's binary32 rounding.
		if sourceType != "real" ||
			!noArguments() ||
			source.Default != nil {
			break
		}
		target.Type = "double precision"
		declaration("double")
	case "double precision":
		if sourceType != "double precision" || !noArguments() {
			break
		}
		target.Type = "double precision"
		declaration("double")
	case "char", "varchar":
		// SQL Server's modifier is a UTF-8 byte limit while MySQL's is a
		// character limit. VARCHAR is a safe widening that also retains the
		// padding already present in admitted SQL Server CHAR rows/defaults.
		if sourceType != "text" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		target.Type = "varchar"
		declaration("varchar", arguments[0])
	case "text":
		if sourceType != "text" || !noArguments() {
			break
		}
		target.Type = "text"
		declaration("longtext")
	case "binary", "varbinary":
		if sourceType != "blob" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		target.Type = base
		declaration(base, arguments[0])
	case "blob":
		if sourceType != "blob" || !noArguments() {
			break
		}
		target.Type = "blob"
		declaration("longblob")
	case "date":
		if sourceType != "date" || !noArguments() {
			break
		}
		target.Type = "date"
		declaration("date")
	case "time":
		if sourceType != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		target.Type = "time"
		declaration("time", arguments[0])
	case "timestamp":
		if sourceType != "datetime" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		target.Type = "datetime"
		declaration("datetime", arguments[0])
	case "smalldatetime":
		if sourceType != "datetime" || !noArguments() {
			break
		}
		target.Type = "datetime"
		declaration("datetime", 0)
	case "uuid":
		if sourceType != "uuid" || !noArguments() {
			break
		}
		target.Type = "char"
		declaration("char", 36)
	default:
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server type",
			base,
		)
	}
	if !mapped {
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server type",
			source.Name+"."+base,
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

func sqlServerMySQLNonportableComparison(
	column schema.Column,
) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.Join(
			strings.Fields(column.DeclaredType.Base),
			" ",
		))
	}
	switch base {
	case "char", "varchar", "text",
		"binary", "varbinary", "blob",
		"uuid", "uniqueidentifier":
		return true
	default:
		return false
	}
}

func sqlServerMySQLRealColumn(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	return strings.EqualFold(
		strings.TrimSpace(column.DeclaredType.Base),
		"real",
	)
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
