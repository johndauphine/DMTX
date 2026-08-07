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
	projected := cloneSQLServerTargetTable(source)
	// The table's MySQL collation is dropped rather than gated. It is the
	// default its columns inherit, not an ordering anything is read by, and the
	// gate that stood here refused every ordinary MySQL table because
	// utf8mb4_0900_ai_ci is 8.0's own default.
	//
	// Ordering is asked of the columns a paged read is ordered by, in
	// checkMySQLSourceKeyCollations at discovery, once. The third copy of that
	// certified set lived here and named a different pair than discovery's -
	// utf8mb4_bin passed discovery and failed this - so the copies were already
	// inconsistent before any of them was widened.
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

// projectMySQLColumnForSQLServer routes through the canonical type.
//
// The third route to leave the pairwise projection behind, and the first whose
// source engine declares facts the canonical type had no room for. Two kinds
// were added for it, both because the information exists at the source and
// throwing it away costs something real:
//
//	KindSmallInt  MySQL names tinyint, smallint, mediumint and int alike
//	              "integer", so an ordinary TINYINT arrived as INT and spent
//	              four bytes a row where one was declared.
//	KindBinary    binary(n) is fixed and zero-padded, varbinary(n) is not, and
//	              the canonical type had one KindBlob carrying neither the
//	              width nor the distinction.
//
// Every accepted shape was compared against the pairwise projection before the
// swap, whole declaration at a time, and they agree - tinyint through smallint,
// decimal's two positional arguments, binary and varbinary at their widths,
// datetime's fractional precision. The pins in
// TestMySQLToSQLServerDeclarations are what survives that comparison, because a
// differential test stops testing anything the moment the code it differs
// against is deleted.
//
// The route deliberately accepts two shapes the pairwise projection refused,
// and refuses five it also refused. Both lists are in
// refuseMySQLTypeSQLServerCannotHold, with the reason each one is on the list
// it is on.
func projectMySQLColumnForSQLServer(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL declared type",
			source.Name,
		)
	}
	if err := refuseMySQLTypeSQLServerCannotHold(source); err != nil {
		return schema.Column{}, err
	}

	// Flavour and collation are not consulted, and isKey is false, because this
	// projects a column's SHAPE rather than deciding whether it may order a
	// paged read. That question is asked at discovery, in
	// checkMySQLSourceKeyCollations, where key membership is known - and asking
	// it here with neither the collation nor the key set to hand is how the old
	// rule came to be applied to every text column in the table.
	canonical, err := schema.CanonicalFromMySQL(
		source,
		schema.MySQLFlavorUnknown,
		"",
		false,
	)
	if err != nil {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map MySQL declared type",
			source.Name+": "+err.Error(),
		)
	}
	targetType, declared, err := schema.CanonicalToDeclared(
		canonical,
		schema.SQLServer,
	)
	if err != nil {
		return schema.Column{}, err
	}

	target := source
	target.Default = cloneSchemaExpression(source.Default)
	target.Type = targetType
	target.DeclaredType = declared

	// The default has to be renderable on the target, which is a property of
	// the DEFAULT rather than of the type and so stays here.
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

// refuseMySQLTypeSQLServerCannotHold keeps the refusals that are about VALUES.
//
// The canonical type says what a column holds; these say what SQL Server cannot
// hold, which is a different question and one the canonical layer should not be
// taught to answer per pair. Each names its reason, because "unsupported type"
// tells an operator nothing they can act on.
//
// What is NOT here is as deliberate. The pairwise projection also refused
// varchar past 2000 characters and varbinary past 8000 bytes, on the grounds
// that four times 2000 exceeds what SQL Server's varchar can declare. Those are
// now accepted and widened to the MAX form, because widening refuses nothing
// the source can hold and the alternative is refusing an ordinary column
// outright. It is also what the postgres -> mssql route has been doing since
// before the canonical type existed, so the two routes now agree rather than
// each having a local opinion - which is the entire point of the lattice.
func refuseMySQLTypeSQLServerCannotHold(source schema.Column) error {
	switch strings.ToLower(strings.Join(
		strings.Fields(source.DeclaredType.Base),
		" ",
	)) {
	case "char", "character":
		return sqlServerProjectionPolicy(
			"map MySQL character type",
			"fixed-width blank-padding semantics cannot be preserved",
		)
	case "longtext":
		return sqlServerProjectionPolicy(
			"map MySQL type",
			"LONGTEXT capacity exceeds SQL Server VARCHAR(MAX)",
		)
	case "longblob":
		return sqlServerProjectionPolicy(
			"map MySQL type",
			"LONGBLOB capacity exceeds SQL Server VARBINARY(MAX)",
		)
	case "time":
		return sqlServerProjectionPolicy(
			"map MySQL type",
			"TIME includes signed durations outside SQL Server TIME",
		)
	case "json":
		return sqlServerProjectionPolicy(
			"map MySQL type",
			"JSON storage and validation semantics cannot be preserved",
		)
	}
	return nil
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
