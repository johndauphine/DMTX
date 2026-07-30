package migrate

import (
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectMySQLTableForSQLServer maps only the shared Oracle MySQL 8.0 and
// MariaDB 10.11 source shape whose stored values and relational objects can be
// reconstructed by the native SQL Server 2022 target. In particular, MySQL
// NO-PAD text comparison is not claimed to be equivalent to SQL Server's
// UTF-8 BIN2 comparison, so text remains a scalar-only cross-engine type.
func projectMySQLTableForSQLServer(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict ||
		source.SQLiteWithoutRowID ||
		len(source.ClickHouseOrderBy) != 0 {
		return schema.Table{}, sqlServerProjectionPolicy(
			"map MySQL table metadata",
			source.Name,
		)
	}
	switch strings.ToLower(strings.TrimSpace(source.MySQLCollation)) {
	case "utf8mb4_0900_bin", "utf8mb4_nopad_bin":
	default:
		return schema.Table{}, sqlServerProjectionPolicy(
			"map MySQL collation",
			source.MySQLCollation,
		)
	}

	projected := cloneSQLServerTargetTable(source)
	projected.MySQLCollation = ""
	for index, column := range source.Columns {
		target, err := projectMySQLColumnForSQLServer(column)
		if err != nil {
			return schema.Table{}, err
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		sourceColumns[column.Name] = column
		if column.PrimaryKeyPosition > 0 &&
			mySQLTextColumnForSQLServer(column) {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map MySQL text primary-key collation",
				source.Name+"."+column.Name,
			)
		}
	}

	for _, index := range source.Indexes {
		if index.Inline || index.Name == "" ||
			len(index.Columns) == 0 {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map MySQL index shape",
				source.Name+"."+index.Name,
			)
		}
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL index column",
					source.Name+"."+indexed.Name,
				)
			}
			if mySQLTextColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL text index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if strings.TrimSpace(indexed.Collation) != "" {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL index collation",
					source.Name+"."+index.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "NONE":
			foreignKey.Match = "SIMPLE"
		default:
			return schema.Table{}, sqlServerProjectionPolicy(
				"map MySQL foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, name := range foreignKey.Columns {
			column, exists := sourceColumns[name]
			if !exists {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL foreign-key column",
					source.Name+"."+name,
				)
			}
			if mySQLTextColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL text foreign-key comparison",
					source.Name+"."+name,
				)
			}
		}
		if err := projectMySQLForeignKeyActionForSQLServer(
			foreignKey.OnUpdate,
		); err != nil {
			return schema.Table{}, err
		}
		if err := projectMySQLForeignKeyActionForSQLServer(
			foreignKey.OnDelete,
		); err != nil {
			return schema.Table{}, err
		}
	}

	for _, check := range source.Checks {
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map MySQL CHECK expression",
				source.Name+"."+check.Name,
			)
		}
		for _, name := range referenced {
			if mySQLTextColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map MySQL text CHECK comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	return projected, nil
}

func projectMySQLColumnForSQLServer(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL declared type",
			source.Name,
		)
	}
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	base := strings.ToLower(strings.Join(
		strings.Fields(source.DeclaredType.Base),
		" ",
	))
	sourceType := strings.ToLower(strings.Join(
		strings.Fields(source.Type),
		" ",
	))
	arguments := append(
		[]int(nil),
		source.DeclaredType.Arguments...,
	)
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
		if sourceType != "integer" ||
			len(arguments) > 1 ||
			len(arguments) == 1 && arguments[0] != 1 {
			break
		}
		// MySQL TINYINT is signed while SQL Server TINYINT is unsigned.
		target.Type = "integer"
		declaration("smallint")
	case "smallint":
		if sourceType != "integer" || !noArguments() {
			break
		}
		target.Type = "integer"
		declaration("smallint")
	case "mediumint", "int", "integer":
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
	case "double", "double precision":
		if sourceType != "double precision" || !noArguments() {
			break
		}
		target.Type = "double precision"
		declaration("double precision")
	case "decimal", "numeric":
		if sourceType != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 38 ||
			arguments[1] < 0 ||
			arguments[1] > arguments[0] {
			break
		}
		target.Type = "numeric"
		declaration("decimal", arguments...)
	case "char", "character":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL character type",
			"fixed-width blank-padding semantics cannot be preserved",
		)
	case "varchar", "character varying":
		if sourceType != "varchar" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 2_000 {
			break
		}
		// MySQL's modifier counts utf8mb4 characters; SQL Server's admitted
		// UTF-8 VARCHAR modifier counts bytes.
		target.Type = "text"
		declaration("varchar", arguments[0]*4)
	case "tinytext", "text", "mediumtext":
		if sourceType != "text" || !noArguments() {
			break
		}
		target.Type = "text"
		declaration("text")
	case "longtext":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type",
			"LONGTEXT capacity exceeds SQL Server VARCHAR(MAX)",
		)
	case "binary":
		if sourceType != "binary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		target.Type = "blob"
		declaration("binary", arguments[0])
	case "varbinary":
		if sourceType != "varbinary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		target.Type = "blob"
		declaration("varbinary", arguments[0])
	case "tinyblob", "blob", "mediumblob":
		if sourceType != "blob" || !noArguments() {
			break
		}
		target.Type = "blob"
		declaration("blob")
	case "longblob":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type",
			"LONGBLOB capacity exceeds SQL Server VARBINARY(MAX)",
		)
	case "date":
		if sourceType != "date" || !noArguments() {
			break
		}
		target.Type = "date"
		declaration("date")
	case "datetime", "timestamp":
		if sourceType != base ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		target.Type = "datetime"
		declaration("timestamp", arguments[0])
	case "time":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type",
			"TIME includes signed durations outside SQL Server TIME",
		)
	case "json":
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type",
			"JSON storage and validation semantics cannot be preserved",
		)
	default:
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type",
			base,
		)
	}
	if !mapped {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL type modifier",
			source.Name+"."+base,
		)
	}
	if target.Default != nil {
		if _, err := schema.RenderSQLServerDefault(target); err != nil {
			return schema.Column{}, sqlServerProjectionPolicy(
				"map MySQL default",
				source.Name,
			)
		}
	}
	return target, nil
}

func projectMySQLForeignKeyActionForSQLServer(value string) error {
	switch strings.ToUpper(strings.Join(strings.Fields(value), " ")) {
	case "NO ACTION", "CASCADE", "SET NULL":
		return nil
	default:
		return sqlServerProjectionPolicy(
			"map MySQL foreign-key action",
			value,
		)
	}
}

func mySQLTextColumnForSQLServer(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	switch strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	)) {
	case "char", "character", "varchar", "character varying",
		"tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}
