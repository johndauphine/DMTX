package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MySQLObjectKind identifies one post-load MySQL schema object.
type MySQLObjectKind uint8

const (
	MySQLIndexObject MySQLObjectKind = iota + 1
	MySQLCheckObject
	MySQLForeignKeyObject
)

// MySQLObjectStatement is one deterministic post-load DDL statement.
type MySQLObjectStatement struct {
	Kind   MySQLObjectKind
	Schema string
	Table  string
	Name   string
	SQL    string
}

type mysqlObjectTable struct {
	source  Table
	columns map[string]Column
}

type mysqlObjectSpec struct {
	kind       MySQLObjectKind
	table      *mysqlObjectTable
	index      Index
	check      CheckConstraint
	foreignKey ForeignKey
	referenced *mysqlObjectTable
	sortKey    string
	occurrence int
}

// AddMySQLForeignKeyIndexes makes every target-side foreign-key dependency
// explicit. InnoDB otherwise creates a hidden side-effect index named after
// the constraint; that shape cannot be predicted safely when names collide
// and would make a subsequent retained-table preflight reject DMTX's own
// freshly created target.
func AddMySQLForeignKeyIndexes(table Table) (Table, error) {
	result := table
	result.Indexes = append([]Index(nil), table.Indexes...)
	names := newMySQLNameAllocator()
	scope := mysqlObjectTableKey(table.Schema, table.Name)
	names.reserve(scope, "PRIMARY")
	for _, index := range result.Indexes {
		if index.Name != "" {
			names.reserve(scope, index.Name)
		}
	}

	foreignKeys := append([]ForeignKey(nil), result.ForeignKeys...)
	sort.Slice(foreignKeys, func(left, right int) bool {
		return postgresForeignKeySortKey(foreignKeys[left]) <
			postgresForeignKeySortKey(foreignKeys[right])
	})
	for _, foreignKey := range foreignKeys {
		if mysqlIndexSupportsColumns(result, foreignKey.Columns) {
			continue
		}
		if len(foreignKey.Columns) == 0 {
			return Table{}, mysqlObjectPolicy(
				"plan MySQL foreign-key index",
				"foreign key has no columns",
			)
		}
		preferred := "dmtx_" + table.Name + "_" +
			strings.Join(foreignKey.Columns, "_") + "_fkey_idx"
		seed := table.Schema + "\x00" + table.Name + "\x00" +
			postgresForeignKeySortKey(foreignKey)
		name, err := names.allocate(
			scope,
			preferred,
			seed,
			true,
		)
		if err != nil {
			return Table{}, err
		}
		columns := make([]IndexColumn, len(foreignKey.Columns))
		for index, column := range foreignKey.Columns {
			columns[index] = IndexColumn{Name: column}
			for _, tableColumn := range table.Columns {
				if tableColumn.Name == column &&
					mysqlTextBase(mysqlColumnBase(tableColumn)) {
					columns[index].Collation = "BINARY"
					break
				}
			}
		}
		result.Indexes = append(result.Indexes, Index{
			Name:    name,
			Columns: columns,
		})
	}
	return result, nil
}

// PlanMySQLDropRecreateObjects plans indexes first, CHECK constraints second,
// and foreign keys last. The plan is pure and independent of input ordering.
func PlanMySQLDropRecreateObjects(
	tables []Table,
) ([]MySQLObjectStatement, error) {
	planned, err := planMySQLObjectTables(tables)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err := collectMySQLObjectSpecs(planned)
	if err != nil {
		return nil, err
	}

	indexNames := newMySQLNameAllocator()
	constraintNames := newMySQLNameAllocator()
	for index := range planned {
		table := &planned[index]
		indexNames.reserve(
			mysqlObjectTableKey(table.source.Schema, table.source.Name),
			"PRIMARY",
		)
	}

	statements := make(
		[]MySQLObjectStatement,
		0,
		len(indexes)+len(checks)+len(foreignKeys),
	)
	for _, spec := range indexes {
		statement, err := planMySQLIndex(spec, indexNames)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	for _, spec := range checks {
		statement, err := planMySQLCheck(spec, constraintNames)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	for _, spec := range foreignKeys {
		statement, err := planMySQLForeignKey(spec, constraintNames)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

func planMySQLObjectTables(tables []Table) ([]mysqlObjectTable, error) {
	planned := make([]mysqlObjectTable, 0, len(tables))
	seen := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		if err := validateMySQLIdentifier("schema", table.Schema, true); err != nil {
			return nil, err
		}
		if err := validateMySQLIdentifier("table", table.Name, false); err != nil {
			return nil, err
		}
		key := mysqlObjectTableKey(table.Schema, table.Name)
		if _, exists := seen[key]; exists {
			return nil, mysqlObjectPolicy(
				"plan MySQL objects",
				"duplicate table "+table.Name,
			)
		}
		seen[key] = struct{}{}
		columns := make(map[string]Column, len(table.Columns))
		normalized := make(map[string]bool, len(table.Columns))
		for _, column := range table.Columns {
			if err := validateMySQLIdentifier(
				"column",
				column.Name,
				false,
			); err != nil {
				return nil, err
			}
			folded := strings.ToLower(column.Name)
			if normalized[folded] {
				return nil, mysqlObjectPolicy(
					"plan MySQL objects",
					"duplicate column "+table.Name+"."+column.Name,
				)
			}
			normalized[folded] = true
			columns[column.Name] = column
		}
		if err := validateMySQLPrimaryKey(table, columns); err != nil {
			return nil, err
		}
		if _, err := mysqlIdentityColumn(table); err != nil {
			return nil, err
		}
		planned = append(planned, mysqlObjectTable{
			source:  table,
			columns: columns,
		})
	}
	sort.Slice(planned, func(left, right int) bool {
		return mysqlObjectTableKey(
			planned[left].source.Schema,
			planned[left].source.Name,
		) < mysqlObjectTableKey(
			planned[right].source.Schema,
			planned[right].source.Name,
		)
	})
	return planned, nil
}

func validateMySQLPrimaryKey(
	table Table,
	columns map[string]Column,
) error {
	keys := orderedPrimaryKeyColumns(table)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) > 16 {
		return mysqlObjectPolicy(
			"create MySQL primary key",
			"primary key exceeds 16 columns",
		)
	}
	positions := make(map[int]bool, len(keys))
	keyBytes := 0
	for expected, key := range keys {
		if key.PrimaryKeyPosition > 0 {
			if positions[key.PrimaryKeyPosition] ||
				key.PrimaryKeyPosition != expected+1 {
				return mysqlObjectPolicy(
					"create MySQL primary key",
					"invalid primary-key column order for table "+table.Name,
				)
			}
			positions[key.PrimaryKeyPosition] = true
		}
		column, exists := columns[key.Name]
		if !exists || column.Nullable {
			return mysqlObjectPolicy(
				"create MySQL primary key",
				"invalid primary-key column "+key.Name,
			)
		}
		columnBytes, err := mysqlIndexColumnMaximumBytes(column)
		if err != nil {
			return err
		}
		keyBytes += columnBytes
		if keyBytes > 3072 {
			return mysqlObjectPolicy(
				"create MySQL primary key",
				"primary key exceeds the 3072-byte InnoDB key limit",
			)
		}
	}
	return nil
}

func collectMySQLObjectSpecs(
	tables []mysqlObjectTable,
) (
	[]mysqlObjectSpec,
	[]mysqlObjectSpec,
	[]mysqlObjectSpec,
	error,
) {
	byName := make(map[string]*mysqlObjectTable, len(tables))
	for index := range tables {
		table := &tables[index]
		byName[mysqlObjectTableKey(
			table.source.Schema,
			table.source.Name,
		)] = table
	}

	var indexes, checks, foreignKeys []mysqlObjectSpec
	for index := range tables {
		table := &tables[index]
		indexCount := len(table.source.Indexes)
		for _, foreignKey := range table.source.ForeignKeys {
			if !mysqlIndexSupportsColumns(
				table.source,
				foreignKey.Columns,
			) {
				// InnoDB creates a supporting index as part of ADD FOREIGN
				// KEY. Include that deterministic side effect in the
				// admission limit even though it has no separate statement.
				indexCount++
			}
		}
		if indexCount > 64 {
			return nil, nil, nil, mysqlObjectPolicy(
				"create MySQL indexes",
				"table "+table.source.Name+" exceeds 64 indexes",
			)
		}
		for _, source := range table.source.Indexes {
			if err := validateMySQLIndex(table, source); err != nil {
				return nil, nil, nil, err
			}
			indexes = append(indexes, mysqlObjectSpec{
				kind:    MySQLIndexObject,
				table:   table,
				index:   source,
				sortKey: postgresIndexSortKey(source),
			})
		}
		for _, source := range table.source.Checks {
			if err := validateMySQLCheck(table, source); err != nil {
				return nil, nil, nil, err
			}
			checks = append(checks, mysqlObjectSpec{
				kind:    MySQLCheckObject,
				table:   table,
				check:   source,
				sortKey: postgresCheckSortKey(source),
			})
		}
		for _, source := range table.source.ForeignKeys {
			referenced, err := validateMySQLForeignKey(
				table,
				source,
				byName,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			foreignKeys = append(foreignKeys, mysqlObjectSpec{
				kind:       MySQLForeignKeyObject,
				table:      table,
				foreignKey: source,
				referenced: referenced,
				sortKey:    postgresForeignKeySortKey(source),
			})
		}
	}
	sortMySQLObjectSpecs(indexes)
	sortMySQLObjectSpecs(checks)
	sortMySQLObjectSpecs(foreignKeys)
	return indexes, checks, foreignKeys, nil
}

func sortMySQLObjectSpecs(specs []mysqlObjectSpec) {
	sort.Slice(specs, func(left, right int) bool {
		leftTable := mysqlObjectTableKey(
			specs[left].table.source.Schema,
			specs[left].table.source.Name,
		)
		rightTable := mysqlObjectTableKey(
			specs[right].table.source.Schema,
			specs[right].table.source.Name,
		)
		if leftTable != rightTable {
			return leftTable < rightTable
		}
		return specs[left].sortKey < specs[right].sortKey
	})
	var previous string
	occurrence := 0
	for index := range specs {
		key := mysqlObjectTableKey(
			specs[index].table.source.Schema,
			specs[index].table.source.Name,
		) + "\x00" + specs[index].sortKey
		if index == 0 || key != previous {
			occurrence = 0
			previous = key
		} else {
			occurrence++
		}
		specs[index].occurrence = occurrence
	}
}

func validateMySQLIndex(table *mysqlObjectTable, index Index) error {
	if index.Name != "" {
		if err := validateMySQLIdentifier("index", index.Name, false); err != nil {
			return err
		}
		if strings.EqualFold(index.Name, "PRIMARY") {
			return mysqlObjectPolicy(
				"create MySQL index",
				"reserved index name PRIMARY",
			)
		}
	}
	if len(index.Columns) == 0 || len(index.Columns) > 16 {
		return mysqlObjectPolicy(
			"create MySQL index",
			"index must have between 1 and 16 columns",
		)
	}
	seen := make(map[string]bool, len(index.Columns))
	keyBytes := 0
	for _, indexed := range index.Columns {
		if err := validateMySQLIdentifier(
			"indexed column",
			indexed.Name,
			false,
		); err != nil {
			return err
		}
		column, exists := table.columns[indexed.Name]
		if !exists {
			return mysqlObjectPolicy(
				"create MySQL index",
				"unknown column "+table.source.Name+"."+indexed.Name,
			)
		}
		if seen[strings.ToLower(indexed.Name)] {
			return mysqlObjectPolicy(
				"create MySQL index",
				"duplicate column "+indexed.Name,
			)
		}
		seen[strings.ToLower(indexed.Name)] = true
		collation := strings.ToUpper(strings.TrimSpace(indexed.Collation))
		if collation != "" && collation != "BINARY" {
			return mysqlObjectPolicy(
				"create MySQL index collation",
				indexed.Collation,
			)
		}
		if collation == "BINARY" && !mysqlTextBase(mysqlColumnBase(column)) {
			return mysqlObjectPolicy(
				"create MySQL index collation",
				indexed.Name,
			)
		}
		columnBytes, err := mysqlIndexColumnMaximumBytes(column)
		if err != nil {
			return err
		}
		keyBytes += columnBytes
		if keyBytes > 3072 {
			return mysqlObjectPolicy(
				"create MySQL index",
				"index exceeds the 3072-byte InnoDB key limit",
			)
		}
	}
	return nil
}

func mysqlIndexColumnMaximumBytes(column Column) (int, error) {
	base := mysqlColumnBase(column)
	arguments := []int(nil)
	if column.DeclaredType != nil {
		arguments = column.DeclaredType.Arguments
	}
	switch base {
	case "tinyint", "bool", "boolean":
		return 1, nil
	case "smallint":
		return 2, nil
	case "mediumint", "date":
		return 3, nil
	case "int", "integer":
		return 4, nil
	case "bigint", "double", "double precision":
		return 8, nil
	case "time", "datetime", "timestamp":
		value := DeclaredType{Base: base, Arguments: arguments}
		precision, err := mysqlDeclaredTemporalPrecision(value)
		if err != nil {
			return 0, err
		}
		baseBytes := 3
		if base == "datetime" {
			baseBytes = 5
		} else if base == "timestamp" {
			baseBytes = 4
		}
		return baseBytes + mysqlFractionalStorageBytes(precision), nil
	case "decimal", "numeric":
		if len(arguments) != 2 {
			return 0, mysqlObjectPolicy(
				"create MySQL index",
				"DECIMAL column has no exact modifiers",
			)
		}
		// Packed DECIMAL uses no more than one byte per two digits plus sign
		// overhead. This bound is deliberately conservative.
		return (arguments[0]+2)/2 + 1, nil
	case "char", "character", "varchar", "character varying":
		if len(arguments) != 1 {
			return 0, mysqlObjectPolicy(
				"create MySQL index",
				"character column has no exact length",
			)
		}
		return arguments[0] * 4, nil
	case "binary", "varbinary":
		if len(arguments) != 1 {
			return 0, mysqlObjectPolicy(
				"create MySQL index",
				"binary column has no exact length",
			)
		}
		return arguments[0], nil
	case "tinytext", "text", "mediumtext", "longtext",
		"tinyblob", "blob", "mediumblob", "longblob", "json":
		return 0, mysqlObjectPolicy(
			"create MySQL index",
			"column "+column.Name+" requires an unsupported index prefix",
		)
	default:
		// A projection should normally provide a declared MySQL type. Keep
		// common modifier-free portable scalars safe for direct schema use.
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "uuid":
			return 36 * 4, nil
		case "integer", "int4":
			return 4, nil
		case "int8":
			return 8, nil
		}
		return 0, mysqlObjectPolicy(
			"create MySQL index",
			"unsupported indexed type "+base,
		)
	}
}

func validateMySQLCheck(
	table *mysqlObjectTable,
	check CheckConstraint,
) error {
	if check.Name != "" {
		if err := validateMySQLIdentifier(
			"CHECK constraint",
			check.Name,
			false,
		); err != nil {
			return err
		}
	}
	_, err := RenderPortableCheckForMySQL(
		check.Expression,
		table.source.Columns,
	)
	if err != nil {
		return fmt.Errorf(
			"render MySQL CHECK for table %s: %w",
			table.source.Name,
			err,
		)
	}
	if table.source.Identity != nil {
		referenced, err := mysqlCheckReferencedColumns(
			check.Expression,
			table.source.Columns,
		)
		if err != nil {
			return err
		}
		if referenced[table.source.Identity.Column] {
			return mysqlObjectPolicy(
				"create MySQL CHECK",
				"CHECK references AUTO_INCREMENT column "+
					table.source.Identity.Column,
			)
		}
	}
	return nil
}

func validateMySQLForeignKey(
	table *mysqlObjectTable,
	foreignKey ForeignKey,
	tables map[string]*mysqlObjectTable,
) (*mysqlObjectTable, error) {
	if foreignKey.Name != "" {
		if err := validateMySQLIdentifier(
			"foreign-key constraint",
			foreignKey.Name,
			false,
		); err != nil {
			return nil, err
		}
	}
	if len(foreignKey.Columns) == 0 ||
		foreignKey.ReferencedTable == "" {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"incomplete foreign key for table "+table.source.Name,
		)
	}
	referencedSchema := foreignKey.ReferencedSchema
	if referencedSchema == "" {
		referencedSchema = table.source.Schema
	} else if err := validateMySQLIdentifier(
		"referenced schema",
		referencedSchema,
		false,
	); err != nil {
		return nil, err
	}
	referenced := tables[mysqlObjectTableKey(
		referencedSchema,
		foreignKey.ReferencedTable,
	)]
	if referenced == nil {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"unknown referenced table "+
				referencedSchema+"."+foreignKey.ReferencedTable,
		)
	}
	referencedColumns := append(
		[]string(nil),
		foreignKey.ReferencedColumns...,
	)
	if len(referencedColumns) == 0 {
		for _, column := range orderedPrimaryKeyColumns(referenced.source) {
			referencedColumns = append(referencedColumns, column.Name)
		}
	}
	if len(referencedColumns) != len(foreignKey.Columns) {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"mismatched columns for table "+table.source.Name,
		)
	}
	if referenced == table &&
		equalIdentifierLists(foreignKey.Columns, referencedColumns) {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"self-reference uses the same local and referenced columns",
		)
	}
	if err := validateMySQLForeignKeyColumns(
		table,
		foreignKey.Columns,
	); err != nil {
		return nil, err
	}
	if err := validateMySQLForeignKeyColumns(
		referenced,
		referencedColumns,
	); err != nil {
		return nil, err
	}
	if len(foreignKey.Columns) > 16 {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"foreign key exceeds 16 columns",
		)
	}
	keyBytes := 0
	for index, name := range foreignKey.Columns {
		columnBytes, err := mysqlIndexColumnMaximumBytes(table.columns[name])
		if err != nil {
			return nil, err
		}
		keyBytes += columnBytes
		if keyBytes > 3072 {
			return nil, mysqlObjectPolicy(
				"create MySQL foreign key",
				"foreign key exceeds the 3072-byte InnoDB key limit",
			)
		}
		localType, err := renderColumnType(table.columns[name], MySQL)
		if err != nil {
			return nil, err
		}
		referencedType, err := renderColumnType(
			referenced.columns[referencedColumns[index]],
			MySQL,
		)
		if err != nil {
			return nil, err
		}
		if localType != referencedType {
			return nil, mysqlObjectPolicy(
				"create MySQL foreign key",
				"incompatible column types for table "+table.source.Name,
			)
		}
	}
	if !mysqlColumnsAreUnique(referenced.source, referencedColumns) {
		return nil, mysqlObjectPolicy(
			"create MySQL foreign key",
			"referenced columns are not a known unique key",
		)
	}
	onUpdate, err := normalizeMySQLForeignKeyAction(foreignKey.OnUpdate)
	if err != nil {
		return nil, err
	}
	onDelete, err := normalizeMySQLForeignKeyAction(foreignKey.OnDelete)
	if err != nil {
		return nil, err
	}
	if onUpdate == "SET NULL" || onDelete == "SET NULL" {
		for _, name := range foreignKey.Columns {
			if !table.columns[name].Nullable {
				return nil, mysqlObjectPolicy(
					"create MySQL foreign key",
					"SET NULL column is not nullable "+name,
				)
			}
		}
	}
	if onUpdate == "CASCADE" ||
		onUpdate == "SET NULL" ||
		onDelete == "CASCADE" ||
		onDelete == "SET NULL" {
		foreignColumns := make(map[string]bool, len(foreignKey.Columns))
		for _, name := range foreignKey.Columns {
			foreignColumns[name] = true
		}
		for _, check := range table.source.Checks {
			referencedColumns, err := mysqlCheckReferencedColumns(
				check.Expression,
				table.source.Columns,
			)
			if err != nil {
				return nil, err
			}
			for name := range referencedColumns {
				if foreignColumns[name] {
					return nil, mysqlObjectPolicy(
						"create MySQL foreign key",
						"referential action conflicts with CHECK column "+name,
					)
				}
			}
		}
	}
	if err := validateMySQLForeignKeyMatch(foreignKey.Match); err != nil {
		return nil, err
	}
	return referenced, nil
}

func mysqlCheckReferencedColumns(
	expression Expression,
	columns []Column,
) (map[string]bool, error) {
	if expression.kind != expressionCheck {
		return nil, mysqlObjectPolicy(
			"inspect MySQL CHECK",
			"non-CHECK expression",
		)
	}
	resolver, err := newPortableCheckColumnResolver(columns)
	if err != nil {
		return nil, err
	}
	tokens, err := lexPortableCheck(expression.sql)
	if err != nil {
		return nil, err
	}
	parser := portableCheckParser{tokens: tokens, resolver: resolver}
	root, err := parser.parseExpression()
	if err != nil {
		return nil, err
	}
	if parser.current().kind != portableCheckTokenEOF ||
		root.family != portableCheckBoolean {
		return nil, mysqlObjectPolicy(
			"inspect MySQL CHECK",
			"invalid CHECK expression",
		)
	}
	result := make(map[string]bool)
	var collect func(*portableCheckNode)
	collect = func(node *portableCheckNode) {
		if node == nil {
			return
		}
		if node.kind == portableCheckNodeColumn {
			result[node.text] = true
		}
		collect(node.left)
		collect(node.right)
		for _, child := range node.children {
			collect(child)
		}
	}
	collect(root)
	return result, nil
}

// ReferencedCheckColumns returns the validated portable column references in
// deterministic order. Cross-engine target projections use it to fail closed
// when a source collation-dependent predicate cannot be preserved.
func ReferencedCheckColumns(
	expression Expression,
	columns []Column,
) ([]string, error) {
	referenced, err := mysqlCheckReferencedColumns(expression, columns)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(referenced))
	for name := range referenced {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func validateMySQLForeignKeyColumns(
	table *mysqlObjectTable,
	columns []string,
) error {
	seen := make(map[string]bool, len(columns))
	for _, name := range columns {
		if _, exists := table.columns[name]; !exists {
			return mysqlObjectPolicy(
				"create MySQL foreign key",
				"unknown column "+table.source.Name+"."+name,
			)
		}
		folded := strings.ToLower(name)
		if seen[folded] {
			return mysqlObjectPolicy(
				"create MySQL foreign key",
				"duplicate column "+name,
			)
		}
		seen[folded] = true
	}
	return nil
}

func mysqlColumnsAreUnique(table Table, columns []string) bool {
	primaryKey := orderedPrimaryKeyColumns(table)
	keyNames := make([]string, len(primaryKey))
	for index, column := range primaryKey {
		keyNames[index] = column.Name
	}
	if equalIdentifierLists(keyNames, columns) {
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
		if equalIdentifierLists(names, columns) {
			return true
		}
	}
	return false
}

func mysqlIndexSupportsColumns(table Table, columns []string) bool {
	if len(columns) == 0 {
		return false
	}
	primaryKey := orderedPrimaryKeyColumns(table)
	if len(primaryKey) >= len(columns) {
		matches := true
		for index := range columns {
			if primaryKey[index].Name != columns[index] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	for _, index := range table.Indexes {
		if len(index.Columns) < len(columns) {
			continue
		}
		matches := true
		for position := range columns {
			if index.Columns[position].Name != columns[position] ||
				index.Columns[position].Descending {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func planMySQLIndex(
	spec mysqlObjectSpec,
	names *mysqlNameAllocator,
) (MySQLObjectStatement, error) {
	preferred := spec.index.Name
	generated := preferred == ""
	if generated {
		suffix := "idx"
		if spec.index.Unique {
			suffix = "key"
		}
		preferred = "dmtx_" + spec.table.source.Name + "_" +
			mysqlIndexComponents(spec.index.Columns) + "_" + suffix
	}
	name, err := names.allocate(
		mysqlObjectTableKey(
			spec.table.source.Schema,
			spec.table.source.Name,
		),
		preferred,
		mysqlObjectNameSeed(spec),
		generated,
	)
	if err != nil {
		return MySQLObjectStatement{}, err
	}
	columns := make([]string, len(spec.index.Columns))
	for index, column := range spec.index.Columns {
		direction := " ASC"
		if column.Descending {
			direction = " DESC"
		}
		columns[index] = quote(MySQL, column.Name) + direction
	}
	unique := ""
	if spec.index.Unique {
		unique = "UNIQUE "
	}
	sql := "CREATE " + unique + "INDEX " + quote(MySQL, name) +
		" ON " + qualified(
		MySQL,
		spec.table.source.Schema,
		spec.table.source.Name,
	) + " (" + strings.Join(columns, ", ") + ");"
	return MySQLObjectStatement{
		Kind:   MySQLIndexObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func planMySQLCheck(
	spec mysqlObjectSpec,
	names *mysqlNameAllocator,
) (MySQLObjectStatement, error) {
	preferred := spec.check.Name
	generated := preferred == ""
	if generated {
		preferred = "dmtx_" + spec.table.source.Name + "_check"
	}
	name, err := names.allocate(
		spec.table.source.Schema,
		preferred,
		mysqlObjectNameSeed(spec),
		generated,
	)
	if err != nil {
		return MySQLObjectStatement{}, err
	}
	expression, err := RenderPortableCheckForMySQL(
		spec.check.Expression,
		spec.table.source.Columns,
	)
	if err != nil {
		return MySQLObjectStatement{}, err
	}
	sql := "ALTER TABLE " + qualified(
		MySQL,
		spec.table.source.Schema,
		spec.table.source.Name,
	) + " ADD CONSTRAINT " + quote(MySQL, name) +
		" CHECK (" + expression + ");"
	return MySQLObjectStatement{
		Kind:   MySQLCheckObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func planMySQLForeignKey(
	spec mysqlObjectSpec,
	names *mysqlNameAllocator,
) (MySQLObjectStatement, error) {
	foreignKey := spec.foreignKey
	preferred := foreignKey.Name
	generated := preferred == ""
	if generated {
		preferred = "dmtx_" + spec.table.source.Name + "_" +
			strings.Join(foreignKey.Columns, "_") + "_fkey"
	}
	name, err := names.allocate(
		spec.table.source.Schema,
		preferred,
		mysqlObjectNameSeed(spec),
		generated,
	)
	if err != nil {
		return MySQLObjectStatement{}, err
	}
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
	onUpdate, _ := normalizeMySQLForeignKeyAction(foreignKey.OnUpdate)
	onDelete, _ := normalizeMySQLForeignKeyAction(foreignKey.OnDelete)
	sql := "ALTER TABLE " + qualified(
		MySQL,
		spec.table.source.Schema,
		spec.table.source.Name,
	) + " ADD CONSTRAINT " + quote(MySQL, name) +
		" FOREIGN KEY (" + mysqlQuotedIdentifiers(foreignKey.Columns) +
		") REFERENCES " + qualified(
		MySQL,
		spec.referenced.source.Schema,
		spec.referenced.source.Name,
	) + " (" + mysqlQuotedIdentifiers(referencedColumns) + ")" +
		" ON UPDATE " + onUpdate +
		" ON DELETE " + onDelete + ";"
	return MySQLObjectStatement{
		Kind:   MySQLForeignKeyObject,
		Schema: spec.table.source.Schema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func normalizeMySQLForeignKeyAction(value string) (string, error) {
	normalized := strings.ToUpper(strings.Join(strings.Fields(value), " "))
	if normalized == "" {
		normalized = "NO ACTION"
	}
	switch normalized {
	case "NO ACTION", "RESTRICT", "CASCADE", "SET NULL":
		return normalized, nil
	default:
		return "", mysqlObjectPolicy(
			"create MySQL foreign key action",
			value,
		)
	}
}

func validateMySQLForeignKeyMatch(value string) error {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "NONE", "SIMPLE":
		return nil
	default:
		return mysqlObjectPolicy(
			"create MySQL foreign key match",
			value,
		)
	}
}

// RenderPortableCheckForMySQL reconstructs the portable CHECK subset using
// MySQL identifiers and literals. The table renderer fixes utf8mb4_bin as the
// default collation, so no catalog collation text is copied into the result.
func RenderPortableCheckForMySQL(
	expression Expression,
	columns []Column,
) (string, error) {
	if expression.kind != expressionCheck {
		return "", mysqlObjectPolicy(
			"render MySQL CHECK",
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
		return "", mysqlObjectPolicy(
			"render MySQL CHECK",
			"invalid CHECK expression",
		)
	}
	if err := validateMySQLCheckNumberLiterals(root); err != nil {
		return "", err
	}
	return renderMySQLPortableCheck(
		root,
		portableCheckPrecedenceLowest,
	), nil
}

func validateMySQLCheckNumberLiterals(node *portableCheckNode) error {
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
		// MySQL parses exact numeric literals through its DECIMAL domain.
		// Values outside DECIMAL(65,30) can be rounded or rewritten while
		// installing a CHECK, so reject them before target DDL is emitted.
		if digits > 65 || scale > 30 {
			return mysqlObjectPolicy(
				"render MySQL CHECK",
				"numeric literal exceeds DECIMAL(65,30)",
			)
		}
	}
	if err := validateMySQLCheckNumberLiterals(node.left); err != nil {
		return err
	}
	if err := validateMySQLCheckNumberLiterals(node.right); err != nil {
		return err
	}
	for _, child := range node.children {
		if err := validateMySQLCheckNumberLiterals(child); err != nil {
			return err
		}
	}
	return nil
}

func renderMySQLPortableCheck(
	node *portableCheckNode,
	parentPrecedence int,
) string {
	precedence := portableCheckNodePrecedence(node)
	var rendered string
	switch node.kind {
	case portableCheckNodeColumn:
		rendered = quote(MySQL, node.text)
	case portableCheckNodeNumber, portableCheckNodeBoolean,
		portableCheckNodeNull:
		rendered = node.text
	case portableCheckNodeString:
		rendered = mysqlStringLiteral(node.text)
	case portableCheckNodeComparison:
		rendered = renderMySQLPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " " + node.text + " " + renderMySQLPortableCheck(
			node.right,
			portableCheckPrecedencePredicate,
		)
	case portableCheckNodeIsNull:
		rendered = renderMySQLPortableCheck(
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
			values[index] = renderMySQLPortableCheck(
				child,
				portableCheckPrecedenceLowest,
			)
		}
		rendered = renderMySQLPortableCheck(
			node.left,
			portableCheckPrecedencePredicate,
		) + " IN (" + strings.Join(values, ", ") + ")"
	case portableCheckNodeNot:
		child := renderMySQLPortableCheck(
			node.left,
			portableCheckPrecedenceLowest,
		)
		if !node.left.isScalar() {
			child = "(" + child + ")"
		}
		rendered = "NOT " + child
	case portableCheckNodeAnd:
		rendered = renderMySQLPortableCheck(
			node.left,
			portableCheckPrecedenceAnd,
		) + " AND " + renderMySQLPortableCheck(
			node.right,
			portableCheckPrecedenceAnd,
		)
	case portableCheckNodeOr:
		rendered = renderMySQLPortableCheck(
			node.left,
			portableCheckPrecedenceOr,
		) + " OR " + renderMySQLPortableCheck(
			node.right,
			portableCheckPrecedenceOr,
		)
	}
	if precedence < parentPrecedence {
		return "(" + rendered + ")"
	}
	return rendered
}

func mysqlQuotedIdentifiers(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quote(MySQL, value)
	}
	return strings.Join(quoted, ", ")
}

func mysqlIndexComponents(columns []IndexColumn) string {
	parts := make([]string, len(columns))
	for index, column := range columns {
		parts[index] = column.Name
	}
	return strings.Join(parts, "_")
}

func mysqlObjectNameSeed(spec mysqlObjectSpec) string {
	return strconv.Itoa(int(spec.kind)) + "\x00" +
		spec.table.source.Schema + "\x00" +
		spec.table.source.Name + "\x00" +
		spec.sortKey + "\x00" +
		strconv.Itoa(spec.occurrence)
}

func mysqlObjectTableKey(schema, table string) string {
	return schema + "\x00" + table
}

func validateMySQLIdentifier(kind, value string, allowEmpty bool) error {
	if value == "" && allowEmpty {
		return nil
	}
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		strings.IndexFunc(value, func(value rune) bool {
			return value > '\uFFFF'
		}) >= 0 ||
		strings.HasSuffix(value, " ") ||
		utf8.RuneCountInString(value) > mysqlIdentifierMaximumCharacters {
		return mysqlObjectPolicy(
			"validate MySQL "+kind+" identifier",
			"invalid identifier",
		)
	}
	return nil
}

type mysqlNameAllocator struct {
	used map[string]map[string]bool
}

func newMySQLNameAllocator() *mysqlNameAllocator {
	return &mysqlNameAllocator{used: make(map[string]map[string]bool)}
}

func (allocator *mysqlNameAllocator) reserve(scope, name string) {
	if allocator.used[scope] == nil {
		allocator.used[scope] = make(map[string]bool)
	}
	allocator.used[scope][strings.ToLower(name)] = true
}

func (allocator *mysqlNameAllocator) allocate(
	scope string,
	preferred string,
	seed string,
	generated bool,
) (string, error) {
	if !generated {
		if err := validateMySQLIdentifier(
			"object",
			preferred,
			false,
		); err != nil {
			return "", err
		}
		if allocator.taken(scope, preferred) {
			return "", mysqlObjectPolicy(
				"allocate MySQL object name",
				"duplicate explicit name "+preferred,
			)
		}
		allocator.reserve(scope, preferred)
		return preferred, nil
	}

	candidate := mysqlBoundedGeneratedName(preferred, "")
	if !allocator.taken(scope, candidate) {
		allocator.reserve(scope, candidate)
		return candidate, nil
	}
	sum := sha256.Sum256([]byte(seed))
	suffix := "_" + hex.EncodeToString(sum[:6])
	candidate = mysqlBoundedGeneratedName(preferred, suffix)
	for counter := 0; allocator.taken(scope, candidate); counter++ {
		candidate = mysqlBoundedGeneratedName(
			preferred,
			suffix+"_"+strconv.Itoa(counter+1),
		)
	}
	allocator.reserve(scope, candidate)
	return candidate, nil
}

func (allocator *mysqlNameAllocator) taken(scope, name string) bool {
	return allocator.used[scope] != nil &&
		allocator.used[scope][strings.ToLower(name)]
}

func mysqlBoundedGeneratedName(preferred, suffix string) string {
	runes := []rune(preferred)
	available := mysqlIdentifierMaximumCharacters - utf8.RuneCountInString(suffix)
	if available < 1 {
		available = 1
	}
	if len(runes) > available {
		runes = runes[:available]
	}
	return string(runes) + suffix
}

func mysqlObjectPolicy(operation, value string) error {
	return &PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(MySQL),
	}
}
