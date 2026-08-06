package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

func projectPostgresSourceTable(
	sourceEngine string,
	sourceTable schema.Table,
) (schema.Table, error) {
	switch sourceEngine {
	case "postgres":
		projected := sourceTable
		projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
		return projected, nil
	case "mssql":
		return projectSQLServerTableForPostgres(sourceTable)
	case "mysql":
		return projectMySQLTableForPostgres(sourceTable)
	case "sqlite":
		return projectSQLiteTableForPostgres(sourceTable)
	default:
		return schema.Table{}, fmt.Errorf(
			"PostgreSQL target does not support source engine %q",
			sourceEngine,
		)
	}
}

func projectMySQLTableForPostgres(
	sourceTable schema.Table,
) (schema.Table, error) {
	if sourceTable.SQLiteStrict || sourceTable.SQLiteWithoutRowID {
		return schema.Table{}, postgresMySQLPolicy(
			"map MySQL table metadata",
			sourceTable.Name,
		)
	}
	projected := sourceTable
	// MySQL collation is source-only admission evidence. PostgreSQL has no
	// catalog representation for it, so it must not become part of the
	// projected target authority.
	//
	// The binary-collation gate that stood here has been removed rather than
	// widened. It read the TABLE's collation, which is only the default its
	// columns inherit, and refused every ordinary MySQL table -
	// utf8mb4_0900_ai_ci is 8.0's own default. It also disagreed with the set
	// discovery certified: this accepted utf8mb4_0900_bin and
	// utf8mb4_nopad_bin, discovery accepted utf8mb4_bin and utf8mb4_0900_bin,
	// so utf8mb4_bin passed one and failed the other. Two copies of one fact,
	// already inconsistent, neither of them checking the thing that matters.
	//
	// What matters is ordering, and ordering is a property of the columns a
	// paged read is ordered by. checkMySQLSourceKeyCollations asks it of those
	// columns, once, where key membership is known. A data column's collation
	// changes nothing here: the value transfers byte for byte.
	projected.MySQLCollation = ""
	projected.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	projected.Indexes = clonePostgresProjectionIndexes(sourceTable.Indexes)
	projected.ForeignKeys = clonePostgresProjectionForeignKeys(
		sourceTable.ForeignKeys,
	)
	for index := range projected.ForeignKeys {
		if strings.EqualFold(
			strings.TrimSpace(projected.ForeignKeys[index].Match),
			"NONE",
		) {
			projected.ForeignKeys[index].Match = "SIMPLE"
		}
	}
	projected.Checks = append(
		[]schema.CheckConstraint(nil),
		sourceTable.Checks...,
	)
	projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
	for index, sourceColumn := range sourceTable.Columns {
		targetType, declaration, operation, err := projectMySQLColumnForPostgres(
			sourceColumn,
		)
		if err != nil {
			value := sourceTable.Name + "." + sourceColumn.Name
			if operation == "map MySQL type" &&
				sourceColumn.DeclaredType != nil {
				value = strings.TrimSpace(sourceColumn.DeclaredType.Base)
			}
			return schema.Table{}, postgresMySQLPolicy(operation, value)
		}
		projected.Columns[index].Type = targetType
		projected.Columns[index].DeclaredType = declaration
		projected.Columns[index].Default = cloneSchemaExpression(
			sourceColumn.Default,
		)
	}
	return projected, nil
}

// projectMySQLColumnForPostgres accepts only the structured output of MySQL
// catalog discovery. Integer widths that PostgreSQL's portable writer does not
// expose are widened to INTEGER. MySQL CHAR uses VARCHAR so COPY does not add
// padding, and bounded binary values use BYTEA because PostgreSQL has no
// length-modified binary scalar; both conversions preserve every source value.
func projectMySQLColumnForPostgres(
	source schema.Column,
) (string, *schema.DeclaredType, string, error) {
	if source.DeclaredType == nil {
		return "", nil, "map MySQL declared type",
			fmt.Errorf("missing declared type")
	}
	base := strings.ToLower(strings.TrimSpace(source.DeclaredType.Base))
	sourceType := strings.ToLower(strings.TrimSpace(source.Type))
	arguments := source.DeclaredType.Arguments
	typeMatches := func(candidates ...string) bool {
		for _, candidate := range candidates {
			if sourceType == candidate {
				return true
			}
		}
		return false
	}
	noArguments := func() error {
		if len(arguments) != 0 {
			return fmt.Errorf("unexpected type modifiers")
		}
		return nil
	}
	modifierError := func() (string, *schema.DeclaredType, string, error) {
		return "", nil, "map MySQL type modifier",
			fmt.Errorf("invalid type modifiers")
	}
	canonicalError := func() (string, *schema.DeclaredType, string, error) {
		return "", nil, "map MySQL declared type",
			fmt.Errorf("declared type does not match canonical type")
	}

	switch base {
	case "tinyint":
		if len(arguments) > 1 ||
			len(arguments) == 1 && arguments[0] != 1 {
			return modifierError()
		}
		if len(arguments) == 1 &&
			typeMatches("bool", "boolean") {
			return "boolean", nil, "", nil
		}
		if len(arguments) == 0 &&
			typeMatches("bool", "boolean") {
			return canonicalError()
		}
		if !typeMatches("tinyint", "smallint", "integer", "int") {
			return canonicalError()
		}
		return "integer", nil, "", nil
	case "bool", "boolean":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("bool", "boolean") {
			return canonicalError()
		}
		return "boolean", nil, "", nil
	case "smallint":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("smallint", "integer", "int") {
			return canonicalError()
		}
		return "integer", nil, "", nil
	case "mediumint", "int", "integer":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("mediumint", "int", "integer") {
			return canonicalError()
		}
		return "integer", nil, "", nil
	case "bigint":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("bigint", "int8") {
			return canonicalError()
		}
		return "bigint", nil, "", nil
	case "double", "double precision":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("double", "double precision", "float8") {
			return canonicalError()
		}
		return "double precision", nil, "", nil
	case "decimal", "numeric":
		if !typeMatches("decimal", "numeric") {
			return canonicalError()
		}
		if len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 65 ||
			arguments[1] < 0 ||
			arguments[1] > arguments[0] {
			return modifierError()
		}
		return "numeric", &schema.DeclaredType{
			Base:      "numeric",
			Arguments: append([]int(nil), arguments...),
		}, "", nil
	case "char", "character":
		if !typeMatches("char", "character", "text") {
			return canonicalError()
		}
		if len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 255 {
			return modifierError()
		}
		return "varchar", &schema.DeclaredType{
			Base:      "varchar",
			Arguments: append([]int(nil), arguments...),
		}, "", nil
	case "varchar", "character varying":
		if !typeMatches(
			"varchar",
			"character varying",
			"text",
		) {
			return canonicalError()
		}
		if len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 65_535 {
			return modifierError()
		}
		return "varchar", &schema.DeclaredType{
			Base:      "varchar",
			Arguments: append([]int(nil), arguments...),
		}, "", nil
	case "tinytext", "text", "mediumtext", "longtext":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches(
			"tinytext",
			"text",
			"mediumtext",
			"longtext",
		) {
			return canonicalError()
		}
		return "text", nil, "", nil
	case "binary":
		if !typeMatches("binary", "bytea") {
			return canonicalError()
		}
		if len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 255 {
			return modifierError()
		}
		return "bytea", nil, "", nil
	case "varbinary":
		if !typeMatches("varbinary", "bytea") {
			return canonicalError()
		}
		if len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 65_535 {
			return modifierError()
		}
		return "bytea", nil, "", nil
	case "tinyblob", "blob", "mediumblob", "longblob":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches(
			"tinyblob",
			"blob",
			"mediumblob",
			"longblob",
			"bytea",
		) {
			return canonicalError()
		}
		return "bytea", nil, "", nil
	case "date":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("date") {
			return canonicalError()
		}
		return "date", nil, "", nil
	case "datetime", "timestamp":
		if !typeMatches("datetime", "timestamp") {
			return canonicalError()
		}
		precision := 0
		switch len(arguments) {
		case 0:
		case 1:
			precision = arguments[0]
		default:
			return modifierError()
		}
		if precision < 0 || precision > 6 {
			return modifierError()
		}
		return "timestamp", &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{precision},
		}, "", nil
	case "json":
		if err := noArguments(); err != nil {
			return modifierError()
		}
		if !typeMatches("json") {
			return canonicalError()
		}
		return "json", nil, "", nil
	default:
		return "", nil, "map MySQL type",
			fmt.Errorf("unsupported MySQL type")
	}
}

func cloneSchemaExpression(
	expression *schema.Expression,
) *schema.Expression {
	if expression == nil {
		return nil
	}
	cloned := *expression
	return &cloned
}

func postgresMySQLPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}

func projectSQLiteTableForPostgres(
	sourceTable schema.Table,
) (schema.Table, error) {
	if sourceTable.Schema != "" {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite schema namespace",
			sourceTable.Schema,
		)
	}
	if sourceTable.SQLiteStrict {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite STRICT table",
			sourceTable.Name,
		)
	}
	if sourceTable.SQLiteWithoutRowID {
		return schema.Table{}, postgresSQLitePolicy(
			"map SQLite WITHOUT ROWID table",
			sourceTable.Name,
		)
	}
	if sourceTable.Identity == nil {
		if column, ok := sqliteImplicitRowIDAlias(sourceTable); ok {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite implicit rowid identity",
				sourceTable.Name+"."+column,
			)
		}
	}

	projected := sourceTable
	projected.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	projected.Indexes = clonePostgresProjectionIndexes(sourceTable.Indexes)
	projected.ForeignKeys = clonePostgresProjectionForeignKeys(sourceTable.ForeignKeys)
	projected.Checks = append([]schema.CheckConstraint(nil), sourceTable.Checks...)
	projected.Identity = cloneSchemaIdentity(sourceTable.Identity)
	for index, sourceColumn := range sourceTable.Columns {
		if sourceColumn.DeclaredType == nil {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite declared type",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		base := strings.ToLower(strings.TrimSpace(
			sourceColumn.DeclaredType.Base,
		))
		if base == "" ||
			base != strings.ToLower(strings.TrimSpace(sourceColumn.Type)) {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite declared type",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		mapped, ok := postgresSQLiteType(base)
		if !ok {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type",
				base,
			)
		}
		projected.Columns[index].Type = mapped
		declaration, err := projectSQLiteDeclaredTypeForPostgres(
			*sourceColumn.DeclaredType,
		)
		if err != nil {
			return schema.Table{}, postgresSQLitePolicy(
				"map SQLite type modifier",
				sourceTable.Name+"."+sourceColumn.Name,
			)
		}
		projected.Columns[index].DeclaredType = declaration
	}
	return projected, nil
}

// projectSQLiteDeclaredTypeForPostgres retains only modifiers whose source
// values can be checked exactly before COPY. SQLite ignores character and
// decimal modifiers, so the writer must reject values that PostgreSQL would
// otherwise truncate or round. CHAR-family declarations use VARCHAR to avoid
// PostgreSQL's trailing-space padding while preserving the declared limit.
func projectSQLiteDeclaredTypeForPostgres(
	value schema.DeclaredType,
) (*schema.DeclaredType, error) {
	if len(value.Arguments) == 0 {
		return nil, nil
	}
	base := strings.ToLower(strings.TrimSpace(value.Base))
	arguments := append([]int(nil), value.Arguments...)
	switch base {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar":
		if len(arguments) != 1 ||
			arguments[0] <= 0 ||
			arguments[0] > 10_485_760 {
			return nil, fmt.Errorf("invalid character length")
		}
		return &schema.DeclaredType{
			Base:      "varchar",
			Arguments: arguments,
		}, nil
	case "numeric", "decimal":
		if len(arguments) < 1 || len(arguments) > 2 ||
			arguments[0] < 1 || arguments[0] > 1000 {
			return nil, fmt.Errorf("invalid numeric precision")
		}
		scale := 0
		if len(arguments) == 2 {
			scale = arguments[1]
		}
		if scale < 0 || scale > arguments[0] {
			return nil, fmt.Errorf("invalid numeric scale")
		}
		return &schema.DeclaredType{
			Base:      "numeric",
			Arguments: arguments,
		}, nil
	case "datetime", "timestamp":
		if len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return nil, fmt.Errorf(
				"invalid temporal precision",
			)
		}
		return &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: arguments,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported modifier")
	}
}

func sqliteImplicitRowIDAlias(table schema.Table) (string, bool) {
	keyIndex := -1
	for index, column := range table.Columns {
		if !column.PrimaryKey && column.PrimaryKeyPosition == 0 {
			continue
		}
		if keyIndex >= 0 {
			return "", false
		}
		keyIndex = index
	}
	if keyIndex < 0 {
		return "", false
	}
	key := table.Columns[keyIndex]
	if key.DeclaredType == nil ||
		len(key.DeclaredType.Arguments) != 0 {
		return "", false
	}
	if strings.ToLower(strings.TrimSpace(key.DeclaredType.Base)) !=
		"integer" {
		return "", false
	}
	return key.Name, true
}

func postgresSQLiteType(source string) (string, bool) {
	switch source {
	case "int", "integer", "tinyint", "smallint", "mediumint",
		"bigint", "int2", "int8":
		return "bigint", true
	case "real", "double", "double precision", "float":
		return "double precision", true
	case "numeric", "decimal":
		return "numeric", true
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		return "text", true
	case "blob", "binary", "varbinary":
		return "bytea", true
	case "bool", "boolean":
		return "boolean", true
	case "date":
		return "date", true
	case "datetime", "timestamp":
		return "timestamp", true
	case "json":
		return "json", true
	case "uuid":
		return "uuid", true
	default:
		return "", false
	}
}

func postgresSQLitePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}
