package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	sqlServerIdentifierMaximumCharacters = 128
	sqlServerMaximumColumns              = 1024
	sqlServerMaximumIndexColumns         = 32
	sqlServerMaximumIndexes              = 999
	sqlServerMaximumClusteredKeyBytes    = 900
	sqlServerMaximumNonclusteredKeyBytes = 1700
	sqlServerMaximumDeclaredRowBytes     = 8060
	sqlServerPortableTextCollation       = "Latin1_General_100_BIN2_UTF8"
)

// SQLServerObjectKind identifies one deterministic post-load SQL Server
// object. Plans order every index before every CHECK and every CHECK before
// every foreign key.
type SQLServerObjectKind uint8

const (
	SQLServerIndexObject SQLServerObjectKind = iota + 1
	SQLServerCheckObject
	SQLServerForeignKeyObject
)

// SQLServerObjectStatement is one fully rendered post-load DDL statement.
// Object identity remains separate from SQL so callers never have to parse a
// statement to report a failure.
type SQLServerObjectStatement struct {
	Kind   SQLServerObjectKind
	Schema string
	Table  string
	Name   string
	SQL    string
}

type sqlServerTargetTable struct {
	source  Table
	columns map[string]Column
}

type sqlServerObjectSpec struct {
	kind       SQLServerObjectKind
	table      *sqlServerTargetTable
	index      Index
	check      CheckConstraint
	foreignKey ForeignKey
	referenced *sqlServerTargetTable
	name       string
	sortKey    string
}

// ValidateSQLServerTable validates the complete base-table shape without
// performing catalog I/O. Cross-table foreign-key validation belongs to
// PlanSQLServerDropRecreateObjects.
func ValidateSQLServerTable(table Table) error {
	_, err := validateSQLServerTargetTable(table)
	return err
}

// SQLServerPrimaryKeyConstraintName returns the deterministic schema-scoped
// primary-key constraint name for a fully validated table. Tables without a
// primary key return an empty name.
func SQLServerPrimaryKeyConstraintName(table Table) (string, error) {
	if _, err := validateSQLServerTargetTable(table); err != nil {
		return "", err
	}
	if len(orderedPrimaryKeyColumns(table)) == 0 {
		return "", nil
	}
	return sqlServerPrimaryKeyName(table), nil
}

// CreateSQLServerTable renders a base table with a named clustered primary
// key. Secondary indexes, CHECKs, and foreign keys are deliberately post-load
// objects and are planned by PlanSQLServerDropRecreateObjects.
func CreateSQLServerTable(table Table) (string, error) {
	if _, err := validateSQLServerTargetTable(table); err != nil {
		return "", err
	}
	identityColumn, err := sqlServerIdentityColumn(table)
	if err != nil {
		return "", err
	}

	definitions := make([]string, 0, len(table.Columns)+1)
	for _, column := range table.Columns {
		renderedType, err := renderSQLServerDeclaredColumn(column)
		if err != nil {
			return "", err
		}
		identity := ""
		if column.Name == identityColumn {
			identity = " IDENTITY(1,1)"
		}
		nullability := " NOT NULL"
		if column.Nullable {
			nullability = " NULL"
		}
		definition := quote(SQLServer, column.Name) + " " +
			renderedType + identity + nullability
		if column.Default != nil {
			renderedDefault, err := RenderSQLServerDefault(column)
			if err != nil {
				return "", err
			}
			definition += " DEFAULT " + renderedDefault
		}
		definitions = append(definitions, definition)
	}

	primaryKey := orderedPrimaryKeyColumns(table)
	if len(primaryKey) > 0 {
		columns := make([]string, len(primaryKey))
		for index, column := range primaryKey {
			columns[index] = quote(SQLServer, column.Name) + " ASC"
		}
		definitions = append(
			definitions,
			"CONSTRAINT "+
				quote(SQLServer, sqlServerPrimaryKeyName(table))+
				" PRIMARY KEY CLUSTERED ("+
				strings.Join(columns, ", ")+")",
		)
	}

	return "CREATE TABLE " +
		qualified(SQLServer, table.Schema, table.Name) +
		" (" + strings.Join(definitions, ", ") + ");", nil
}

// DropSQLServerTable renders an exact, fully qualified table drop.
func DropSQLServerTable(table Table) (string, error) {
	if err := validateSQLServerIdentifier("schema", table.Schema, false); err != nil {
		return "", err
	}
	if err := validateSQLServerIdentifier("table", table.Name, false); err != nil {
		return "", err
	}
	return "DROP TABLE IF EXISTS " +
		qualified(SQLServer, table.Schema, table.Name) + ";", nil
}

// PlanSQLServerDropRecreateObjects builds the pure post-load object plan.
// Input ordering cannot affect output ordering or generated names.
func PlanSQLServerDropRecreateObjects(
	tables []Table,
) ([]SQLServerObjectStatement, error) {
	planned, err := planSQLServerTargetTables(tables)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err :=
		collectSQLServerObjectSpecs(planned)
	if err != nil {
		return nil, err
	}
	if err := validateSQLServerObjectNameScopes(
		planned,
		indexes,
		checks,
		foreignKeys,
	); err != nil {
		return nil, err
	}
	if err := validateSQLServerCascadeTopology(
		planned,
		foreignKeys,
	); err != nil {
		return nil, err
	}

	statements := make(
		[]SQLServerObjectStatement,
		0,
		len(indexes)+len(checks)+len(foreignKeys),
	)
	for _, spec := range indexes {
		statements = append(statements, planSQLServerIndex(spec))
	}
	for _, spec := range checks {
		statement, err := planSQLServerCheck(spec)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	for _, spec := range foreignKeys {
		statements = append(
			statements,
			planSQLServerForeignKey(spec),
		)
	}
	return statements, nil
}

func validateSQLServerTargetTable(
	table Table,
) (sqlServerTargetTable, error) {
	if err := validateSQLServerIdentifier(
		"schema",
		table.Schema,
		false,
	); err != nil {
		return sqlServerTargetTable{}, err
	}
	if err := validateSQLServerIdentifier(
		"table",
		table.Name,
		false,
	); err != nil {
		return sqlServerTargetTable{}, err
	}
	if table.SQLiteStrict || table.SQLiteWithoutRowID ||
		table.MySQLCollation != "" {
		return sqlServerTargetTable{}, sqlServerTargetPolicy(
			"create SQL Server table",
			"foreign table options on "+table.Name,
		)
	}
	if len(table.Columns) == 0 ||
		len(table.Columns) > sqlServerMaximumColumns {
		return sqlServerTargetTable{}, sqlServerTargetPolicy(
			"create SQL Server table",
			"invalid column count on "+table.Name,
		)
	}

	columns := make(map[string]Column, len(table.Columns))
	columnNames := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if err := validateSQLServerIdentifier(
			"column",
			column.Name,
			false,
		); err != nil {
			return sqlServerTargetTable{}, err
		}
		if sqlServerEqualFoldExists(columnNames, column.Name) {
			return sqlServerTargetTable{}, sqlServerTargetPolicy(
				"create SQL Server table",
				"duplicate column "+table.Name+"."+column.Name,
			)
		}
		if _, err := renderSQLServerDeclaredColumn(column); err != nil {
			return sqlServerTargetTable{}, err
		}
		if column.Default != nil {
			if _, err := RenderSQLServerDefault(column); err != nil {
				return sqlServerTargetTable{}, err
			}
		}
		columnNames = append(columnNames, column.Name)
		columns[column.Name] = column
	}
	if err := validateSQLServerDeclaredRowSize(table); err != nil {
		return sqlServerTargetTable{}, err
	}
	if err := validateSQLServerPrimaryKey(table, columns); err != nil {
		return sqlServerTargetTable{}, err
	}
	if _, err := sqlServerIdentityColumn(table); err != nil {
		return sqlServerTargetTable{}, err
	}
	return sqlServerTargetTable{
		source:  table,
		columns: columns,
	}, nil
}

func renderSQLServerDeclaredColumn(column Column) (string, error) {
	if column.DeclaredType == nil {
		return "", sqlServerTargetPolicy(
			"render SQL Server declared type",
			column.Name+" has no declared type",
		)
	}
	value := *column.DeclaredType
	base := strings.ToLower(strings.Join(strings.Fields(value.Base), " "))
	arguments := value.Arguments
	noArguments := func() bool { return len(arguments) == 0 }
	oneArgument := func(minimum, maximum int) (int, bool) {
		if len(arguments) != 1 ||
			arguments[0] < minimum ||
			arguments[0] > maximum {
			return 0, false
		}
		return arguments[0], true
	}
	canonical := strings.ToLower(strings.Join(strings.Fields(column.Type), " "))
	requires := func(values ...string) bool {
		for _, value := range values {
			if canonical == value {
				return true
			}
		}
		return false
	}

	switch base {
	case "tinyint":
		if noArguments() && requires("integer", "int") {
			return "TINYINT", nil
		}
	case "smallint":
		if noArguments() && requires("integer", "int") {
			return "SMALLINT", nil
		}
	case "int", "integer":
		if noArguments() && requires("integer", "int") {
			return "INT", nil
		}
	case "bigint":
		if noArguments() && requires("bigint") {
			return "BIGINT", nil
		}
	case "bit", "bool", "boolean":
		if noArguments() && requires("bit", "bool", "boolean") {
			return "BIT", nil
		}
	case "decimal", "numeric":
		if len(arguments) == 2 &&
			arguments[0] >= 1 && arguments[0] <= 38 &&
			arguments[1] >= 0 &&
			arguments[1] <= arguments[0] &&
			requires("decimal", "numeric") {
			return strings.ToUpper(base) + "(" +
				strconv.Itoa(arguments[0]) + "," +
				strconv.Itoa(arguments[1]) + ")", nil
		}
	case "real":
		if noArguments() && requires("real") {
			return "REAL", nil
		}
	case "float", "double", "double precision":
		if noArguments() &&
			requires("float", "double", "double precision") {
			return "FLOAT(53)", nil
		}
	case "char":
		if length, ok := oneArgument(1, 8_000); ok &&
			requires("text", "char") {
			return "CHAR(" + strconv.Itoa(length) + ") COLLATE " +
				sqlServerPortableTextCollation, nil
		}
	case "varchar":
		if length, ok := oneArgument(1, 8_000); ok &&
			requires("text", "varchar") {
			return "VARCHAR(" + strconv.Itoa(length) + ") COLLATE " +
				sqlServerPortableTextCollation, nil
		}
	case "text":
		if noArguments() && requires("text") {
			return "VARCHAR(MAX) COLLATE " +
				sqlServerPortableTextCollation, nil
		}
	case "binary":
		if length, ok := oneArgument(1, 8_000); ok &&
			requires("blob", "binary") {
			return "BINARY(" + strconv.Itoa(length) + ")", nil
		}
	case "varbinary":
		if length, ok := oneArgument(1, 8_000); ok &&
			requires("blob", "binary", "varbinary") {
			return "VARBINARY(" + strconv.Itoa(length) + ")", nil
		}
	case "blob":
		if noArguments() && requires("blob", "binary", "varbinary") {
			return "VARBINARY(MAX)", nil
		}
	case "date":
		if noArguments() && requires("date") {
			return "DATE", nil
		}
	case "time":
		if precision, ok := oneArgument(0, 6); ok &&
			requires("time") {
			return "TIME(" + strconv.Itoa(precision) + ")", nil
		}
	case "timestamp", "datetime", "datetime2":
		if precision, ok := oneArgument(0, 6); ok &&
			requires("timestamp", "datetime") {
			return "DATETIME2(" + strconv.Itoa(precision) + ")", nil
		}
	case "smalldatetime":
		if noArguments() && requires("timestamp", "datetime") {
			return "SMALLDATETIME", nil
		}
	case "uuid", "uniqueidentifier":
		if noArguments() && requires("uuid") {
			return "UNIQUEIDENTIFIER", nil
		}
	}
	return "", sqlServerTargetPolicy(
		"render SQL Server declared type",
		column.Name+" "+declaredTypeDescription(value),
	)
}

// RenderSQLServerDefault reconstructs a typed scalar default without copying
// executable catalog text.
func RenderSQLServerDefault(column Column) (string, error) {
	expression := column.Default
	if expression == nil {
		return "", sqlServerTargetPolicy(
			"render SQL Server default",
			column.Name,
		)
	}
	if _, err := renderSQLServerDeclaredColumn(column); err != nil {
		return "", err
	}
	switch expression.kind {
	case expressionNull:
		return "NULL", nil
	case expressionBoolean:
		if sqlServerCatalogDefaultTargetForColumn(column) !=
			sqlServerCatalogDefaultBoolean {
			break
		}
		switch strings.ToUpper(expression.literal) {
		case "TRUE":
			return "1", nil
		case "FALSE":
			return "0", nil
		}
	case expressionNumber:
		var (
			canonical string
			err       error
		)
		switch sqlServerCatalogDefaultTargetForColumn(column) {
		case sqlServerCatalogDefaultInteger:
			canonical, err = canonicalSQLServerIntegerDefault(
				column,
				expression.literal,
			)
		case sqlServerCatalogDefaultNumeric:
			canonical, err = canonicalSQLServerNumericDefault(
				column,
				expression.literal,
			)
		case sqlServerCatalogDefaultFloat:
			canonical, err = canonicalSQLServerFloatDefault(
				expression.literal,
			)
		default:
			err = fmt.Errorf("incompatible number")
		}
		if err == nil && canonical == expression.literal {
			return canonical, nil
		}
	case expressionString:
		if sqlServerCatalogDefaultTargetForColumn(column) !=
			sqlServerCatalogDefaultString {
			break
		}
		canonical, ok := canonicalSQLServerCatalogString(
			column,
			expression.literal,
		)
		if ok && canonical == expression.literal {
			return sqlServerUnicodeStringLiteral(canonical), nil
		}
	case expressionBlob:
		canonical, err := canonicalSQLServerBinaryDefault(
			column,
			"0x"+strings.ToLower(expression.literal),
		)
		if err == nil && canonical == strings.ToLower(expression.literal) {
			return "0x" + canonical, nil
		}
	}
	return "", sqlServerTargetPolicy(
		"render SQL Server default",
		column.Name,
	)
}

func sqlServerIdentityColumn(table Table) (string, error) {
	if table.Identity == nil {
		return "", nil
	}
	identity := table.Identity
	if identity.Generation != IdentityByDefault ||
		identity.Column == "" {
		return "", sqlServerTargetPolicy(
			"render SQL Server identity",
			table.Name,
		)
	}
	if identity.Frontier != nil &&
		(*identity.Frontier < 0 ||
			*identity.Frontier == math.MaxInt64) {
		// A negative frontier is outside the admitted IDENTITY(1,1)
		// lifecycle. MaxInt64 cannot be admitted here because the neutral
		// schema does not carry whether the source table is empty; an empty
		// exhausted generator cannot be reproduced by the target primer.
		return "", sqlServerTargetPolicy(
			"render SQL Server identity frontier",
			table.Name,
		)
	}
	var found *Column
	for index := range table.Columns {
		if table.Columns[index].Name == identity.Column {
			found = &table.Columns[index]
			break
		}
	}
	primaryKey := orderedPrimaryKeyColumns(table)
	if found == nil ||
		found.Nullable ||
		found.Default != nil ||
		len(primaryKey) != 1 ||
		primaryKey[0].Name != identity.Column ||
		found.DeclaredType == nil ||
		!strings.EqualFold(found.DeclaredType.Base, "bigint") ||
		len(found.DeclaredType.Arguments) != 0 ||
		!strings.EqualFold(found.Type, "bigint") {
		return "", sqlServerTargetPolicy(
			"render SQL Server identity",
			table.Name+"."+identity.Column,
		)
	}
	return identity.Column, nil
}

func validateSQLServerPrimaryKey(
	table Table,
	columns map[string]Column,
) error {
	primaryKey := orderedPrimaryKeyColumns(table)
	if len(primaryKey) == 0 {
		return nil
	}
	if len(primaryKey) > sqlServerMaximumIndexColumns {
		return sqlServerTargetPolicy(
			"create SQL Server primary key",
			"too many columns on "+table.Name,
		)
	}
	seenNames := make([]string, 0, len(primaryKey))
	seenPositions := make(map[int]bool, len(primaryKey))
	keyBytes := 0
	for expected, key := range primaryKey {
		column, exists := columns[key.Name]
		if !exists ||
			column.Nullable ||
			key.PrimaryKeyPosition != expected+1 ||
			seenPositions[key.PrimaryKeyPosition] ||
			sqlServerEqualFoldExists(seenNames, key.Name) {
			return sqlServerTargetPolicy(
				"create SQL Server primary key",
				"invalid key shape on "+table.Name,
			)
		}
		size, err := sqlServerIndexColumnBytes(column)
		if err != nil {
			return err
		}
		keyBytes += size
		if keyBytes > sqlServerMaximumClusteredKeyBytes {
			return sqlServerTargetPolicy(
				"create SQL Server primary key",
				"clustered key exceeds 900 bytes on "+table.Name,
			)
		}
		seenNames = append(seenNames, key.Name)
		seenPositions[key.PrimaryKeyPosition] = true
	}
	return nil
}

func planSQLServerTargetTables(
	tables []Table,
) ([]sqlServerTargetTable, error) {
	planned := make([]sqlServerTargetTable, 0, len(tables))
	for _, table := range tables {
		validated, err := validateSQLServerTargetTable(table)
		if err != nil {
			return nil, err
		}
		for _, existing := range planned {
			if strings.EqualFold(existing.source.Schema, table.Schema) &&
				strings.EqualFold(existing.source.Name, table.Name) {
				return nil, sqlServerTargetPolicy(
					"plan SQL Server tables",
					"duplicate target table "+table.Schema+"."+table.Name,
				)
			}
		}
		planned = append(planned, validated)
	}
	sort.Slice(planned, func(left, right int) bool {
		return sqlServerTableSortKey(planned[left].source) <
			sqlServerTableSortKey(planned[right].source)
	})
	return planned, nil
}

func collectSQLServerObjectSpecs(
	tables []sqlServerTargetTable,
) (
	[]sqlServerObjectSpec,
	[]sqlServerObjectSpec,
	[]sqlServerObjectSpec,
	error,
) {
	var indexes, checks, foreignKeys []sqlServerObjectSpec
	for tableIndex := range tables {
		table := &tables[tableIndex]
		if len(table.source.Indexes) > sqlServerMaximumIndexes {
			return nil, nil, nil, sqlServerTargetPolicy(
				"create SQL Server indexes",
				"too many indexes on "+table.source.Name,
			)
		}
		for _, index := range table.source.Indexes {
			name, err := validateSQLServerIndex(table, index)
			if err != nil {
				return nil, nil, nil, err
			}
			indexes = append(indexes, sqlServerObjectSpec{
				kind:    SQLServerIndexObject,
				table:   table,
				index:   index,
				name:    name,
				sortKey: sqlServerIndexSortKey(index),
			})
		}
		for _, check := range table.source.Checks {
			name, err := validateSQLServerCheck(table, check)
			if err != nil {
				return nil, nil, nil, err
			}
			checks = append(checks, sqlServerObjectSpec{
				kind:    SQLServerCheckObject,
				table:   table,
				check:   check,
				name:    name,
				sortKey: sqlServerCheckSortKey(check),
			})
		}
	}
	for tableIndex := range tables {
		table := &tables[tableIndex]
		for _, foreignKey := range table.source.ForeignKeys {
			referenced, name, err := validateSQLServerForeignKey(
				table,
				foreignKey,
				tables,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			foreignKeys = append(foreignKeys, sqlServerObjectSpec{
				kind:       SQLServerForeignKeyObject,
				table:      table,
				foreignKey: foreignKey,
				referenced: referenced,
				name:       name,
				sortKey:    sqlServerForeignKeySortKey(foreignKey),
			})
		}
	}
	sortSQLServerObjectSpecs(indexes)
	sortSQLServerObjectSpecs(checks)
	sortSQLServerObjectSpecs(foreignKeys)
	return indexes, checks, foreignKeys, nil
}

func validateSQLServerIndex(
	table *sqlServerTargetTable,
	index Index,
) (string, error) {
	if index.Inline ||
		len(index.Columns) == 0 ||
		len(index.Columns) > sqlServerMaximumIndexColumns {
		return "", sqlServerTargetPolicy(
			"create SQL Server index",
			"invalid index shape on "+table.source.Name,
		)
	}
	name := index.Name
	if name == "" {
		name = sqlServerGeneratedUniqueObjectName(
			"dmtx_"+table.source.Name+"_index",
			table.source.Schema+"\x00"+table.source.Name+"\x00"+
				sqlServerIndexSortKey(index),
		)
	}
	if err := validateSQLServerIdentifier("index", name, false); err != nil {
		return "", err
	}
	seen := make([]string, 0, len(index.Columns))
	keyBytes := 0
	for _, indexed := range index.Columns {
		if indexed.Collation != "" {
			return "", sqlServerTargetPolicy(
				"create SQL Server index",
				"per-index collation on "+name,
			)
		}
		column, exists := table.columns[indexed.Name]
		if !exists ||
			sqlServerEqualFoldExists(seen, indexed.Name) {
			return "", sqlServerTargetPolicy(
				"create SQL Server index",
				"invalid column "+indexed.Name+" on "+name,
			)
		}
		if index.Unique && column.Nullable {
			return "", sqlServerTargetPolicy(
				"create SQL Server unique index",
				"nullable column "+indexed.Name+" on "+name,
			)
		}
		size, err := sqlServerIndexColumnBytes(column)
		if err != nil {
			return "", err
		}
		keyBytes += size
		if keyBytes > sqlServerMaximumNonclusteredKeyBytes {
			return "", sqlServerTargetPolicy(
				"create SQL Server index",
				"key exceeds 1700 bytes on "+name,
			)
		}
		seen = append(seen, indexed.Name)
	}
	return name, nil
}

func validateSQLServerCheck(
	table *sqlServerTargetTable,
	check CheckConstraint,
) (string, error) {
	name := check.Name
	if name == "" {
		name = sqlServerGeneratedUniqueObjectName(
			"dmtx_"+table.source.Name+"_check",
			table.source.Schema+"\x00"+table.source.Name+"\x00"+
				sqlServerCheckSortKey(check),
		)
	}
	if err := validateSQLServerIdentifier("CHECK constraint", name, false); err != nil {
		return "", err
	}
	if _, err := RenderPortableCheckForSQLServer(
		check.Expression,
		table.source.Columns,
	); err != nil {
		return "", err
	}
	return name, nil
}

func validateSQLServerForeignKey(
	table *sqlServerTargetTable,
	foreignKey ForeignKey,
	tables []sqlServerTargetTable,
) (*sqlServerTargetTable, string, error) {
	name := foreignKey.Name
	if name == "" {
		name = sqlServerGeneratedUniqueObjectName(
			"dmtx_"+table.source.Name+"_fkey",
			table.source.Schema+"\x00"+table.source.Name+"\x00"+
				sqlServerForeignKeySortKey(foreignKey),
		)
	}
	if err := validateSQLServerIdentifier("foreign key", name, false); err != nil {
		return nil, "", err
	}
	if len(foreignKey.Columns) == 0 ||
		len(foreignKey.Columns) > sqlServerMaximumIndexColumns {
		return nil, "", sqlServerTargetPolicy(
			"create SQL Server foreign key",
			"invalid column count on "+name,
		)
	}
	var referenced *sqlServerTargetTable
	for index := range tables {
		if strings.EqualFold(tables[index].source.Schema, table.source.Schema) &&
			strings.EqualFold(
				tables[index].source.Name,
				foreignKey.ReferencedTable,
			) {
			referenced = &tables[index]
			break
		}
	}
	if referenced == nil {
		return nil, "", sqlServerTargetPolicy(
			"create SQL Server foreign key",
			"referenced table is not selected for "+name,
		)
	}
	referencedColumns := append(
		[]string(nil),
		foreignKey.ReferencedColumns...,
	)
	if len(referencedColumns) == 0 {
		for _, column := range orderedPrimaryKeyColumns(
			referenced.source,
		) {
			referencedColumns = append(referencedColumns, column.Name)
		}
	}
	if len(referencedColumns) != len(foreignKey.Columns) {
		return nil, "", sqlServerTargetPolicy(
			"create SQL Server foreign key",
			"column counts differ on "+name,
		)
	}
	localSeen, referencedSeen := []string{}, []string{}
	for index, localName := range foreignKey.Columns {
		referencedName := referencedColumns[index]
		localColumn, localExists := table.columns[localName]
		referencedColumn, referencedExists :=
			referenced.columns[referencedName]
		if !localExists ||
			!referencedExists ||
			sqlServerEqualFoldExists(localSeen, localName) ||
			sqlServerEqualFoldExists(referencedSeen, referencedName) {
			return nil, "", sqlServerTargetPolicy(
				"create SQL Server foreign key",
				"invalid columns on "+name,
			)
		}
		localType, localErr :=
			renderSQLServerDeclaredColumn(localColumn)
		referencedType, referencedErr :=
			renderSQLServerDeclaredColumn(referencedColumn)
		if localErr != nil ||
			referencedErr != nil ||
			localType != referencedType {
			return nil, "", sqlServerTargetPolicy(
				"create SQL Server foreign key",
				"incompatible columns on "+name,
			)
		}
		localSeen = append(localSeen, localName)
		referencedSeen = append(referencedSeen, referencedName)
	}
	if !sqlServerColumnsAreUnique(
		referenced.source,
		referencedColumns,
	) {
		return nil, "", sqlServerTargetPolicy(
			"create SQL Server foreign key",
			"referenced columns are not unique on "+name,
		)
	}
	onUpdate, err := normalizeSQLServerForeignKeyAction(
		foreignKey.OnUpdate,
	)
	if err != nil {
		return nil, "", err
	}
	onDelete, err := normalizeSQLServerForeignKeyAction(
		foreignKey.OnDelete,
	)
	if err != nil {
		return nil, "", err
	}
	if onUpdate == "SET NULL" || onDelete == "SET NULL" {
		for _, column := range foreignKey.Columns {
			if !table.columns[column].Nullable {
				return nil, "", sqlServerTargetPolicy(
					"create SQL Server foreign key",
					"SET NULL column is required "+column,
				)
			}
		}
	}
	if onUpdate == "SET DEFAULT" || onDelete == "SET DEFAULT" {
		for _, column := range foreignKey.Columns {
			if table.columns[column].Default == nil {
				return nil, "", sqlServerTargetPolicy(
					"create SQL Server foreign key",
					"SET DEFAULT column has no default "+column,
				)
			}
		}
	}
	if err := validateSQLServerForeignKeyMatch(foreignKey.Match); err != nil {
		return nil, "", err
	}
	return referenced, name, nil
}

func validateSQLServerObjectNameScopes(
	tables []sqlServerTargetTable,
	indexes, checks, foreignKeys []sqlServerObjectSpec,
) error {
	type scopedName struct {
		scope string
		name  string
		kind  string
		table string
	}
	var constraintNames, indexNames []scopedName
	add := func(
		values *[]scopedName,
		candidate scopedName,
	) error {
		for _, existing := range *values {
			if strings.EqualFold(existing.scope, candidate.scope) &&
				strings.EqualFold(existing.name, candidate.name) {
				return sqlServerTargetPolicy(
					"plan SQL Server object names",
					fmt.Sprintf(
						"%s %s on %s collides with %s %s on %s",
						candidate.kind,
						candidate.name,
						candidate.table,
						existing.kind,
						existing.name,
						existing.table,
					),
				)
			}
		}
		*values = append(*values, candidate)
		return nil
	}
	for _, table := range tables {
		// Tables and constraints are schema-scoped objects in SQL Server and
		// therefore share one case-insensitive name namespace.
		if err := add(&constraintNames, scopedName{
			scope: table.source.Schema,
			name:  table.source.Name,
			kind:  "table",
			table: table.source.Name,
		}); err != nil {
			return err
		}
	}
	for _, table := range tables {
		name, err := SQLServerPrimaryKeyConstraintName(table.source)
		if err != nil {
			return err
		}
		if name == "" {
			continue
		}
		if err := add(&constraintNames, scopedName{
			scope: table.source.Schema,
			name:  name,
			kind:  "primary key",
			table: table.source.Name,
		}); err != nil {
			return err
		}
		if err := add(&indexNames, scopedName{
			scope: sqlServerTableSortKey(table.source),
			name:  name,
			kind:  "primary-key index",
			table: table.source.Name,
		}); err != nil {
			return err
		}
	}
	for _, spec := range indexes {
		if err := add(&indexNames, scopedName{
			scope: sqlServerTableSortKey(spec.table.source),
			name:  spec.name,
			kind:  "index",
			table: spec.table.source.Name,
		}); err != nil {
			return err
		}
	}
	for _, spec := range append(
		append([]sqlServerObjectSpec(nil), checks...),
		foreignKeys...,
	) {
		kind := "CHECK"
		if spec.kind == SQLServerForeignKeyObject {
			kind = "foreign key"
		}
		if err := add(&constraintNames, scopedName{
			scope: spec.table.source.Schema,
			name:  spec.name,
			kind:  kind,
			table: spec.table.source.Name,
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLServerCascadeTopology(
	tables []sqlServerTargetTable,
	foreignKeys []sqlServerObjectSpec,
) error {
	type cascadeEdge struct {
		child int
		name  string
	}
	positions := make(
		map[*sqlServerTargetTable]int,
		len(tables),
	)
	for index := range tables {
		positions[&tables[index]] = index
	}
	adjacency := make([][]cascadeEdge, len(tables))
	incoming := make([]string, len(tables))
	for _, spec := range foreignKeys {
		onUpdate, err := normalizeSQLServerForeignKeyAction(
			spec.foreignKey.OnUpdate,
		)
		if err != nil {
			return err
		}
		onDelete, err := normalizeSQLServerForeignKeyAction(
			spec.foreignKey.OnDelete,
		)
		if err != nil {
			return err
		}
		if onUpdate == "NO ACTION" && onDelete == "NO ACTION" {
			continue
		}
		parent, parentExists := positions[spec.referenced]
		child, childExists := positions[spec.table]
		if !parentExists || !childExists {
			return sqlServerTargetPolicy(
				"plan SQL Server cascade topology",
				"foreign key "+spec.name+" has an unknown endpoint",
			)
		}
		if incoming[child] != "" {
			return sqlServerTargetPolicy(
				"plan SQL Server cascade topology",
				"table "+tables[child].source.Name+
					" has multiple cascading paths through "+
					incoming[child]+" and "+spec.name,
			)
		}
		incoming[child] = spec.name
		adjacency[parent] = append(adjacency[parent], cascadeEdge{
			child: child,
			name:  spec.name,
		})
	}

	// The admitted subset is a directed forest over all DELETE and UPDATE
	// cascading actions. A forest is deliberately stricter than SQL Server's
	// event-specific rule, but proves that no table can occur twice in any
	// cascading action tree and that no cycle can be introduced after load.
	const (
		cascadeUnvisited uint8 = iota
		cascadeVisiting
		cascadeVisited
	)
	state := make([]uint8, len(tables))
	var visit func(int) error
	visit = func(parent int) error {
		state[parent] = cascadeVisiting
		for _, edge := range adjacency[parent] {
			switch state[edge.child] {
			case cascadeVisiting:
				return sqlServerTargetPolicy(
					"plan SQL Server cascade topology",
					"foreign key "+edge.name+
						" creates a cascade cycle at "+
						tables[edge.child].source.Name,
				)
			case cascadeUnvisited:
				if err := visit(edge.child); err != nil {
					return err
				}
			}
		}
		state[parent] = cascadeVisited
		return nil
	}
	for index := range tables {
		if state[index] == cascadeUnvisited {
			if err := visit(index); err != nil {
				return err
			}
		}
	}
	return nil
}

func planSQLServerIndex(
	spec sqlServerObjectSpec,
) SQLServerObjectStatement {
	columns := make([]string, len(spec.index.Columns))
	for index, column := range spec.index.Columns {
		direction := " ASC"
		if column.Descending {
			direction = " DESC"
		}
		columns[index] = quote(SQLServer, column.Name) + direction
	}
	unique := ""
	if spec.index.Unique {
		unique = "UNIQUE "
	}
	return SQLServerObjectStatement{
		Kind:   SQLServerIndexObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   spec.name,
		SQL: "CREATE " + unique + "NONCLUSTERED INDEX " +
			quote(SQLServer, spec.name) + " ON " +
			qualified(
				SQLServer,
				spec.table.source.Schema,
				spec.table.source.Name,
			) + " (" + strings.Join(columns, ", ") + ");",
	}
}

func planSQLServerCheck(
	spec sqlServerObjectSpec,
) (SQLServerObjectStatement, error) {
	expression, err := RenderPortableCheckForSQLServer(
		spec.check.Expression,
		spec.table.source.Columns,
	)
	if err != nil {
		return SQLServerObjectStatement{}, err
	}
	return SQLServerObjectStatement{
		Kind:   SQLServerCheckObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   spec.name,
		SQL: "ALTER TABLE " + qualified(
			SQLServer,
			spec.table.source.Schema,
			spec.table.source.Name,
		) + " WITH CHECK ADD CONSTRAINT " +
			quote(SQLServer, spec.name) +
			" CHECK (" + expression + ");",
	}, nil
}

func planSQLServerForeignKey(
	spec sqlServerObjectSpec,
) SQLServerObjectStatement {
	foreignKey := spec.foreignKey
	referencedColumns := append(
		[]string(nil),
		foreignKey.ReferencedColumns...,
	)
	if len(referencedColumns) == 0 {
		for _, column := range orderedPrimaryKeyColumns(
			spec.referenced.source,
		) {
			referencedColumns = append(referencedColumns, column.Name)
		}
	}
	onUpdate, _ := normalizeSQLServerForeignKeyAction(
		foreignKey.OnUpdate,
	)
	onDelete, _ := normalizeSQLServerForeignKeyAction(
		foreignKey.OnDelete,
	)
	return SQLServerObjectStatement{
		Kind:   SQLServerForeignKeyObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   spec.name,
		SQL: "ALTER TABLE " + qualified(
			SQLServer,
			spec.table.source.Schema,
			spec.table.source.Name,
		) + " WITH CHECK ADD CONSTRAINT " +
			quote(SQLServer, spec.name) +
			" FOREIGN KEY (" +
			sqlServerQuotedIdentifiers(foreignKey.Columns) +
			") REFERENCES " + qualified(
			SQLServer,
			spec.referenced.source.Schema,
			spec.referenced.source.Name,
		) + " (" +
			sqlServerQuotedIdentifiers(referencedColumns) + ")" +
			" ON UPDATE " + onUpdate +
			" ON DELETE " + onDelete + ";",
	}
}

// RenderPortableCheckForSQLServer reconstructs the portable CHECK subset with
// bracket identifiers, Unicode string literals, and BIT-compatible booleans.
func RenderPortableCheckForSQLServer(
	expression Expression,
	columns []Column,
) (string, error) {
	if expression.kind != expressionCheck {
		return "", sqlServerTargetPolicy(
			"render SQL Server CHECK",
			"non-CHECK expression",
		)
	}
	resolver, err := newPortableCheckColumnResolver(columns)
	if err != nil {
		return "", err
	}
	tokens, err := lexPortableCheck(expression.sql)
	if err != nil {
		return "", err
	}
	parser := portableCheckParser{tokens: tokens, resolver: resolver}
	root, err := parser.parseExpression()
	if err != nil {
		return "", err
	}
	if parser.current().kind != portableCheckTokenEOF ||
		root.family != portableCheckBoolean {
		return "", sqlServerTargetPolicy(
			"render SQL Server CHECK",
			"invalid CHECK expression",
		)
	}
	if err := validateSQLServerCheckNumberLiterals(root); err != nil {
		return "", err
	}
	return renderSQLServerPortableCheck(
		root,
		portableCheckPrecedenceLowest,
	), nil
}

func renderSQLServerPortableCheck(
	node *portableCheckNode,
	parentPrecedence int,
) string {
	precedence := portableCheckNodePrecedence(node)
	var rendered string
	switch node.kind {
	case portableCheckNodeColumn:
		rendered = quote(SQLServer, node.text)
	case portableCheckNodeNumber, portableCheckNodeNull:
		rendered = node.text
	case portableCheckNodeBoolean:
		if strings.EqualFold(node.text, "TRUE") {
			rendered = "1"
		} else {
			rendered = "0"
		}
	case portableCheckNodeString:
		rendered = sqlServerUnicodeStringLiteral(node.text)
	case portableCheckNodeComparison:
		rendered = renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " " + node.text + " " + renderSQLServerPortableCheck(
			node.right,
			portableCheckPrecedencePredicate,
		)
	case portableCheckNodeIsNull:
		rendered = renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " IS "
		if node.negated {
			rendered += "NOT "
		}
		rendered += "NULL"
	case portableCheckNodeIn:
		values := make([]string, len(node.children))
		for index, child := range node.children {
			values[index] = renderSQLServerPortableCheck(
				child,
				portableCheckPrecedenceLowest,
			)
		}
		rendered = renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " IN (" + strings.Join(values, ", ") + ")"
	case portableCheckNodeNot:
		child := renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedenceLowest,
		)
		if !node.left.isScalar() {
			child = "(" + child + ")"
		}
		rendered = "NOT " + child
	case portableCheckNodeAnd:
		rendered = renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedenceAnd,
		) + " AND " + renderSQLServerPortableCheck(
			node.right,
			portableCheckPrecedenceAnd,
		)
	case portableCheckNodeOr:
		rendered = renderSQLServerPortableCheck(
			node.left,
			portableCheckPrecedenceOr,
		) + " OR " + renderSQLServerPortableCheck(
			node.right,
			portableCheckPrecedenceOr,
		)
	}
	if precedence < parentPrecedence {
		return "(" + rendered + ")"
	}
	return rendered
}

func validateSQLServerCheckNumberLiterals(
	node *portableCheckNode,
) error {
	if node == nil {
		return nil
	}
	if node.kind == portableCheckNodeNumber {
		value := strings.TrimPrefix(node.text, "-")
		parts := strings.SplitN(value, ".", 2)
		digits := len(parts[0])
		scale := 0
		if len(parts) == 2 {
			digits += len(parts[1])
			scale = len(parts[1])
		}
		if digits > 38 || scale > 38 {
			return sqlServerTargetPolicy(
				"render SQL Server CHECK",
				"numeric literal exceeds DECIMAL(38,38)",
			)
		}
	}
	if err := validateSQLServerCheckNumberLiterals(node.left); err != nil {
		return err
	}
	if err := validateSQLServerCheckNumberLiterals(node.right); err != nil {
		return err
	}
	for _, child := range node.children {
		if err := validateSQLServerCheckNumberLiterals(child); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSQLServerForeignKeyAction(
	value string,
) (string, error) {
	normalized := strings.ToUpper(strings.Join(strings.Fields(value), " "))
	if normalized == "" {
		normalized = "NO ACTION"
	}
	switch normalized {
	case "NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT":
		return normalized, nil
	default:
		return "", sqlServerTargetPolicy(
			"create SQL Server foreign-key action",
			value,
		)
	}
}

func validateSQLServerForeignKeyMatch(value string) error {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "NONE", "SIMPLE":
		return nil
	default:
		return sqlServerTargetPolicy(
			"create SQL Server foreign-key match",
			value,
		)
	}
}

func sqlServerColumnsAreUnique(
	table Table,
	columns []string,
) bool {
	primaryKey := orderedPrimaryKeyColumns(table)
	primaryNames := make([]string, len(primaryKey))
	for index, column := range primaryKey {
		primaryNames[index] = column.Name
	}
	if sqlServerEqualIdentifierLists(primaryNames, columns) {
		return true
	}
	for _, index := range table.Indexes {
		if !index.Unique || len(index.Columns) != len(columns) {
			continue
		}
		names := make([]string, len(index.Columns))
		for position, column := range index.Columns {
			names[position] = column.Name
		}
		if sqlServerEqualIdentifierLists(names, columns) {
			return true
		}
	}
	return false
}

func sqlServerIndexColumnBytes(column Column) (int, error) {
	if column.DeclaredType == nil {
		return 0, sqlServerTargetPolicy(
			"create SQL Server index",
			"missing declared type for "+column.Name,
		)
	}
	value := *column.DeclaredType
	base := strings.ToLower(strings.Join(strings.Fields(value.Base), " "))
	switch base {
	case "tinyint", "bit", "bool", "boolean":
		return 1, nil
	case "smallint":
		return 2, nil
	case "int", "integer", "real":
		return 4, nil
	case "bigint", "float", "double", "double precision":
		return 8, nil
	case "decimal", "numeric":
		if len(value.Arguments) != 2 {
			break
		}
		switch {
		case value.Arguments[0] <= 9:
			return 5, nil
		case value.Arguments[0] <= 19:
			return 9, nil
		case value.Arguments[0] <= 28:
			return 13, nil
		default:
			return 17, nil
		}
	case "char", "varchar", "binary", "varbinary":
		if len(value.Arguments) == 1 &&
			value.Arguments[0] >= 1 &&
			value.Arguments[0] <= 8_000 {
			return value.Arguments[0], nil
		}
	case "date":
		return 3, nil
	case "time":
		if len(value.Arguments) == 1 {
			switch {
			case value.Arguments[0] <= 2:
				return 3, nil
			case value.Arguments[0] <= 4:
				return 4, nil
			default:
				return 5, nil
			}
		}
	case "timestamp", "datetime", "datetime2":
		if len(value.Arguments) == 1 {
			switch {
			case value.Arguments[0] <= 2:
				return 6, nil
			case value.Arguments[0] <= 4:
				return 7, nil
			default:
				return 8, nil
			}
		}
	case "smalldatetime":
		return 4, nil
	case "uuid", "uniqueidentifier":
		return 16, nil
	case "text", "blob":
		return 0, sqlServerTargetPolicy(
			"create SQL Server index",
			"MAX column "+column.Name,
		)
	}
	return 0, sqlServerTargetPolicy(
		"create SQL Server index",
		"invalid declared type for "+column.Name,
	)
}

// validateSQLServerDeclaredRowSize applies a conservative in-row admission
// bound. SQL Server can move some variable-width payloads to row-overflow
// pages, but accepting a declaration that depends on that rewrite would leave
// inserts vulnerable to data-dependent error 511. DMTX therefore admits only
// shapes whose declared maximum, row header, null bitmap, and variable-column
// directory fit within the 8,060-byte record limit.
func validateSQLServerDeclaredRowSize(table Table) error {
	fixedBytes := 7 // Conservative ordinary-record header.
	variableBytes := 0
	variableColumns := 0
	bitColumns := 0
	for _, column := range table.Columns {
		if column.DeclaredType == nil {
			return sqlServerTargetPolicy(
				"render SQL Server declared row",
				"missing declared type on "+table.Name+"."+column.Name,
			)
		}
		value := *column.DeclaredType
		base := strings.ToLower(
			strings.Join(strings.Fields(value.Base), " "),
		)
		switch base {
		case "bit", "bool", "boolean":
			bitColumns++
		case "varchar", "varbinary":
			if len(value.Arguments) != 1 {
				return sqlServerTargetPolicy(
					"render SQL Server declared row",
					"invalid variable column "+column.Name,
				)
			}
			variableColumns++
			variableBytes += value.Arguments[0]
		case "text", "blob":
			// A non-null (MAX) value can consume a 24-byte in-row root.
			variableColumns++
			variableBytes += 24
		default:
			bytes, err := sqlServerIndexColumnBytes(column)
			if err != nil {
				return err
			}
			fixedBytes += bytes
		}
	}
	fixedBytes += (bitColumns + 7) / 8
	// Every record carries a two-byte column count followed by a nullable
	// bitmap. Include it even when no columns are nullable.
	fixedBytes += 2 + (len(table.Columns)+7)/8
	if variableColumns > 0 {
		// Variable-count word plus one two-byte end offset per variable field.
		fixedBytes += 2 + 2*variableColumns
	}
	if fixedBytes+variableBytes > sqlServerMaximumDeclaredRowBytes {
		return sqlServerTargetPolicy(
			"render SQL Server declared row",
			table.Name+" exceeds the 8060-byte in-row limit",
		)
	}
	return nil
}

func sortSQLServerObjectSpecs(specs []sqlServerObjectSpec) {
	sort.Slice(specs, func(left, right int) bool {
		leftTable := sqlServerTableSortKey(specs[left].table.source)
		rightTable := sqlServerTableSortKey(specs[right].table.source)
		if leftTable != rightTable {
			return leftTable < rightTable
		}
		if specs[left].sortKey != specs[right].sortKey {
			return specs[left].sortKey < specs[right].sortKey
		}
		return specs[left].name < specs[right].name
	})
}

func sqlServerPrimaryKeyName(table Table) string {
	return sqlServerGeneratedObjectName(
		"dmtx_"+table.Name+"_pk",
		table.Schema+"\x00"+table.Name+"\x00primary-key",
	)
}

func sqlServerGeneratedObjectName(
	preferred string,
	seed string,
) string {
	if validateSQLServerIdentifier("generated object", preferred, false) == nil {
		return preferred
	}
	digest := sha256.Sum256([]byte(seed))
	suffix := "_" + hex.EncodeToString(digest[:8])
	maximum := sqlServerIdentifierMaximumCharacters -
		sqlServerUTF16Length(suffix)
	return sqlServerTruncateUTF16(preferred, maximum) + suffix
}

func sqlServerGeneratedUniqueObjectName(
	preferred string,
	seed string,
) string {
	digest := sha256.Sum256([]byte(seed))
	suffix := "_" + hex.EncodeToString(digest[:8])
	maximum := sqlServerIdentifierMaximumCharacters -
		sqlServerUTF16Length(suffix)
	return sqlServerTruncateUTF16(preferred, maximum) + suffix
}

func sqlServerIndexSortKey(index Index) string {
	columns := make([]string, len(index.Columns))
	for position, column := range index.Columns {
		columns[position] = column.Name + "\x00" +
			strconv.FormatBool(column.Descending) + "\x00" +
			column.Collation
	}
	return strings.Join([]string{
		index.Name,
		strconv.FormatBool(index.Unique),
		strconv.FormatBool(index.Inline),
		strings.Join(columns, "\x01"),
	}, "\x00")
}

func sqlServerCheckSortKey(check CheckConstraint) string {
	return check.Name + "\x00" + check.Expression.CanonicalSQL()
}

func sqlServerForeignKeySortKey(foreignKey ForeignKey) string {
	return strings.Join([]string{
		foreignKey.Name,
		strings.Join(foreignKey.Columns, "\x01"),
		foreignKey.ReferencedTable,
		strings.Join(foreignKey.ReferencedColumns, "\x01"),
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnUpdate,
		), " ")),
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnDelete,
		), " ")),
		strings.ToUpper(strings.TrimSpace(foreignKey.Match)),
	}, "\x00")
}

func sqlServerTableSortKey(table Table) string {
	return strings.ToLower(table.Schema) + "\x00" +
		strings.ToLower(table.Name) + "\x00" +
		table.Schema + "\x00" + table.Name
}

func validateSQLServerIdentifier(
	kind string,
	value string,
	allowEmpty bool,
) error {
	if value == "" && allowEmpty {
		return nil
	}
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		strings.ContainsRune(value, '\uFFFD') ||
		strings.HasSuffix(value, " ") ||
		sqlServerUTF16Length(value) >
			sqlServerIdentifierMaximumCharacters {
		return sqlServerTargetPolicy(
			"validate SQL Server "+kind+" identifier",
			value,
		)
	}
	return nil
}

func sqlServerUTF16Length(value string) int {
	return len(utf16.Encode([]rune(value)))
}

func sqlServerTruncateUTF16(value string, maximum int) string {
	if maximum <= 0 {
		return ""
	}
	var result strings.Builder
	used := 0
	for _, character := range value {
		units := 1
		if character > 0xffff {
			units = 2
		}
		if used+units > maximum {
			break
		}
		result.WriteRune(character)
		used += units
	}
	return result.String()
}

func sqlServerEqualFoldExists(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(value, candidate) {
			return true
		}
	}
	return false
}

func sqlServerEqualIdentifierLists(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !strings.EqualFold(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sqlServerQuotedIdentifiers(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quote(SQLServer, value)
	}
	return strings.Join(quoted, ", ")
}

func sqlServerUnicodeStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlServerTargetPolicy(operation, value string) error {
	return &PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(SQLServer),
	}
}
