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

const postgresIdentifierMaximumBytes = 63

// PostgresObjectKind identifies the dependency class of a post-load object.
// Plans always order indexes before checks and checks before foreign keys.
type PostgresObjectKind uint8

const (
	PostgresIndexObject PostgresObjectKind = iota + 1
	PostgresCheckObject
	PostgresForeignKeyObject
)

// PostgresObjectStatement is one deterministic, fully rendered post-load DDL
// statement. Schema, Table, and Name are retained separately so an executor can
// report an object without parsing SQL.
type PostgresObjectStatement struct {
	Kind   PostgresObjectKind
	Schema string
	Table  string
	Name   string
	SQL    string
}

// PostgresNamespaceMapper maps a source namespace to its target PostgreSQL
// namespace. A nil mapper maps an empty source namespace to public and
// otherwise keeps the source namespace.
type PostgresNamespaceMapper func(sourceSchema string) (string, error)

// PostgresObjectPlanOptions supplies namespace policy external to structural
// object rendering. CHECK translation always uses DMTX's sole deterministic
// portable renderer.
type PostgresObjectPlanOptions struct {
	MapNamespace PostgresNamespaceMapper
}

type postgresObjectTable struct {
	source       Table
	targetSchema string
	columns      map[string]Column
}

type postgresObjectSpec struct {
	kind       PostgresObjectKind
	table      *postgresObjectTable
	index      Index
	check      CheckConstraint
	foreignKey ForeignKey
	referenced *postgresObjectTable
	sortKey    string
	occurrence int
}

// PlanPostgresDropRecreateObjects builds the pure post-load part of a
// drop/recreate migration. It performs no catalog reads and makes no changes.
//
// The plan deliberately creates every index first, every CHECK second, and
// every foreign key last. This lets all referenced unique indexes exist before
// foreign keys are added and makes the result independent of input ordering.
func PlanPostgresDropRecreateObjects(
	tables []Table,
	options PostgresObjectPlanOptions,
) ([]PostgresObjectStatement, error) {
	plannedTables, err := planPostgresObjectTables(
		tables,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err := collectPostgresObjectSpecs(
		plannedTables,
	)
	if err != nil {
		return nil, err
	}
	relationNames, constraintNames, err := reservePostgresObjectNames(
		plannedTables,
	)
	if err != nil {
		return nil, err
	}
	statements := make(
		[]PostgresObjectStatement,
		0,
		len(indexes)+len(checks)+len(foreignKeys),
	)
	for _, spec := range indexes {
		statement, err := planPostgresIndex(spec, relationNames)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	for _, spec := range checks {
		statement, err := planPostgresCheck(
			spec,
			constraintNames,
		)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	for _, spec := range foreignKeys {
		statement, err := planPostgresForeignKey(
			spec,
			constraintNames,
		)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

func planPostgresObjectTables(
	tables []Table,
	mapNamespace PostgresNamespaceMapper,
) ([]postgresObjectTable, error) {
	if mapNamespace == nil {
		mapNamespace = func(sourceSchema string) (string, error) {
			if sourceSchema == "" {
				return "public", nil
			}
			return sourceSchema, nil
		}
	}

	planned := make([]postgresObjectTable, 0, len(tables))
	sourceNames := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		if err := validateSourceObjectIdentifier(
			"table namespace",
			table.Schema,
			true,
		); err != nil {
			return nil, err
		}
		if err := validatePostgresObjectIdentifier(
			"table",
			table.Name,
		); err != nil {
			return nil, err
		}
		sourceKey := postgresSourceTableKey(table.Schema, table.Name)
		if _, exists := sourceNames[sourceKey]; exists {
			return nil, postgresObjectPolicy(
				"plan PostgreSQL objects",
				"duplicate source table "+table.Name,
			)
		}
		sourceNames[sourceKey] = struct{}{}

		targetSchema, err := mapNamespace(table.Schema)
		if err != nil {
			return nil, fmt.Errorf(
				"map PostgreSQL namespace for table %s: %w",
				table.Name,
				err,
			)
		}
		if err := validatePostgresObjectIdentifier(
			"target namespace",
			targetSchema,
		); err != nil {
			return nil, err
		}
		columns := make(map[string]Column, len(table.Columns))
		for _, column := range table.Columns {
			if err := validatePostgresObjectIdentifier(
				"column",
				column.Name,
			); err != nil {
				return nil, err
			}
			if _, exists := columns[column.Name]; exists {
				return nil, postgresObjectPolicy(
					"plan PostgreSQL objects",
					"duplicate column "+table.Name+"."+column.Name,
				)
			}
			columns[column.Name] = column
		}
		planned = append(planned, postgresObjectTable{
			source:       table,
			targetSchema: targetSchema,
			columns:      columns,
		})
	}

	sort.Slice(planned, func(left, right int) bool {
		return postgresTargetTableKey(
			planned[left].targetSchema,
			planned[left].source.Name,
		) < postgresTargetTableKey(
			planned[right].targetSchema,
			planned[right].source.Name,
		)
	})
	targetNames := make(map[string]string, len(planned))
	for index := range planned {
		targetKey := postgresTargetTableKey(
			planned[index].targetSchema,
			planned[index].source.Name,
		)
		sourceKey := postgresSourceTableKey(
			planned[index].source.Schema,
			planned[index].source.Name,
		)
		if existing, exists := targetNames[targetKey]; exists {
			return nil, postgresObjectPolicy(
				"map PostgreSQL namespace",
				"tables "+existing+" and "+sourceKey+
					" map to the same target",
			)
		}
		targetNames[targetKey] = sourceKey
	}
	return planned, nil
}

func collectPostgresObjectSpecs(
	tables []postgresObjectTable,
) (
	[]postgresObjectSpec,
	[]postgresObjectSpec,
	[]postgresObjectSpec,
	error,
) {
	bySourceName := make(map[string]*postgresObjectTable, len(tables))
	for index := range tables {
		table := &tables[index]
		bySourceName[postgresSourceTableKey(
			table.source.Schema,
			table.source.Name,
		)] = table
	}

	var indexes, checks, foreignKeys []postgresObjectSpec
	for index := range tables {
		table := &tables[index]
		for _, sourceIndex := range table.source.Indexes {
			if err := validatePostgresIndex(table, sourceIndex); err != nil {
				return nil, nil, nil, err
			}
			indexes = append(indexes, postgresObjectSpec{
				kind:    PostgresIndexObject,
				table:   table,
				index:   sourceIndex,
				sortKey: postgresIndexSortKey(sourceIndex),
			})
		}
		for _, check := range table.source.Checks {
			if err := validatePostgresCheck(table, check); err != nil {
				return nil, nil, nil, err
			}
			checks = append(checks, postgresObjectSpec{
				kind:    PostgresCheckObject,
				table:   table,
				check:   check,
				sortKey: postgresCheckSortKey(check),
			})
		}
		for _, foreignKey := range table.source.ForeignKeys {
			referenced, err := validatePostgresForeignKey(
				table,
				foreignKey,
				bySourceName,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			foreignKeys = append(foreignKeys, postgresObjectSpec{
				kind:       PostgresForeignKeyObject,
				table:      table,
				foreignKey: foreignKey,
				referenced: referenced,
				sortKey:    postgresForeignKeySortKey(foreignKey),
			})
		}
	}
	sortPostgresObjectSpecs(indexes)
	sortPostgresObjectSpecs(checks)
	sortPostgresObjectSpecs(foreignKeys)
	return indexes, checks, foreignKeys, nil
}

func sortPostgresObjectSpecs(specs []postgresObjectSpec) {
	sort.Slice(specs, func(left, right int) bool {
		leftTable := postgresTargetTableKey(
			specs[left].table.targetSchema,
			specs[left].table.source.Name,
		)
		rightTable := postgresTargetTableKey(
			specs[right].table.targetSchema,
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
		key := postgresTargetTableKey(
			specs[index].table.targetSchema,
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

func validatePostgresIndex(
	table *postgresObjectTable,
	index Index,
) error {
	if index.Inline && !index.Unique {
		return postgresObjectPolicy(
			"create PostgreSQL index",
			"inline index is not unique",
		)
	}
	if !index.Inline && index.Name == "" {
		return postgresObjectPolicy(
			"create PostgreSQL index",
			"standalone index has no name",
		)
	}
	if index.Name != "" {
		if err := validateSourceObjectIdentifier(
			"index",
			index.Name,
			false,
		); err != nil {
			return err
		}
	}
	if len(index.Columns) == 0 {
		return postgresObjectPolicy(
			"create PostgreSQL index",
			"index has no columns",
		)
	}
	for _, indexedColumn := range index.Columns {
		if indexedColumn.Name == "" {
			return postgresObjectPolicy(
				"create PostgreSQL expression index",
				indexDisplayName(table.source, index),
			)
		}
		if err := validatePostgresObjectIdentifier(
			"indexed column",
			indexedColumn.Name,
		); err != nil {
			return err
		}
		if _, exists := table.columns[indexedColumn.Name]; !exists {
			return postgresObjectPolicy(
				"create PostgreSQL index",
				"unknown column "+table.source.Name+"."+
					indexedColumn.Name,
			)
		}
		collation := strings.ToUpper(strings.TrimSpace(
			indexedColumn.Collation,
		))
		if collation != "" && collation != "BINARY" {
			return postgresObjectPolicy(
				"map SQLite index collation",
				indexedColumn.Collation,
			)
		}
	}
	return nil
}

func validatePostgresCheck(
	table *postgresObjectTable,
	check CheckConstraint,
) error {
	if check.Name != "" {
		if err := validateSourceObjectIdentifier(
			"CHECK constraint",
			check.Name,
			false,
		); err != nil {
			return err
		}
	}
	if check.Expression.kind != expressionCheck ||
		strings.TrimSpace(check.Expression.sql) == "" {
		return postgresObjectPolicy(
			"create PostgreSQL CHECK",
			"invalid CHECK metadata for table "+table.source.Name,
		)
	}
	return nil
}

func validatePostgresForeignKey(
	table *postgresObjectTable,
	foreignKey ForeignKey,
	tables map[string]*postgresObjectTable,
) (*postgresObjectTable, error) {
	if len(foreignKey.Columns) == 0 ||
		foreignKey.ReferencedTable == "" {
		return nil, postgresObjectPolicy(
			"create PostgreSQL foreign key",
			"incomplete foreign key for table "+table.source.Name,
		)
	}
	if err := validatePostgresObjectIdentifier(
		"referenced table",
		foreignKey.ReferencedTable,
	); err != nil {
		return nil, err
	}
	if err := validatePostgresForeignKeyColumns(
		table,
		foreignKey.Columns,
		"foreign-key column",
	); err != nil {
		return nil, err
	}
	referenced := tables[postgresSourceTableKey(
		table.source.Schema,
		foreignKey.ReferencedTable,
	)]
	if referenced == nil {
		return nil, postgresObjectPolicy(
			"create PostgreSQL foreign key",
			"unknown referenced table "+foreignKey.ReferencedTable,
		)
	}

	referencedColumns := foreignKey.ReferencedColumns
	if len(referencedColumns) == 0 {
		primaryKey := orderedPrimaryKeyColumns(referenced.source)
		referencedColumns = make([]string, len(primaryKey))
		for index, column := range primaryKey {
			referencedColumns[index] = column.Name
		}
	}
	if len(referencedColumns) != len(foreignKey.Columns) {
		return nil, postgresObjectPolicy(
			"create PostgreSQL foreign key",
			"mismatched foreign-key columns for table "+
				table.source.Name,
		)
	}
	if err := validatePostgresForeignKeyColumns(
		referenced,
		referencedColumns,
		"referenced column",
	); err != nil {
		return nil, err
	}
	if err := validatePostgresForeignKeyTypes(
		table,
		foreignKey.Columns,
		referenced,
		referencedColumns,
	); err != nil {
		return nil, err
	}
	if !postgresColumnsAreUnique(referenced.source, referencedColumns) {
		return nil, postgresObjectPolicy(
			"create PostgreSQL foreign key",
			"referenced columns are not a known unique key",
		)
	}
	if _, err := normalizePostgresForeignKeyAction(
		foreignKey.OnUpdate,
	); err != nil {
		return nil, err
	}
	if _, err := normalizePostgresForeignKeyAction(
		foreignKey.OnDelete,
	); err != nil {
		return nil, err
	}
	if _, err := normalizePostgresForeignKeyMatch(
		foreignKey.Match,
	); err != nil {
		return nil, err
	}
	return referenced, nil
}

func validatePostgresForeignKeyColumns(
	table *postgresObjectTable,
	columns []string,
	kind string,
) error {
	seen := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if err := validatePostgresObjectIdentifier(kind, name); err != nil {
			return err
		}
		if _, exists := table.columns[name]; !exists {
			return postgresObjectPolicy(
				"create PostgreSQL foreign key",
				"unknown "+kind+" "+table.source.Name+"."+name,
			)
		}
		if _, exists := seen[name]; exists {
			return postgresObjectPolicy(
				"create PostgreSQL foreign key",
				"duplicate "+kind+" "+name,
			)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validatePostgresForeignKeyTypes(
	table *postgresObjectTable,
	columns []string,
	referenced *postgresObjectTable,
	referencedColumns []string,
) error {
	for index, name := range columns {
		columnType, err := renderColumnType(
			table.columns[name],
			Postgres,
		)
		if err != nil {
			return err
		}
		referencedType, err := renderColumnType(
			referenced.columns[referencedColumns[index]],
			Postgres,
		)
		if err != nil {
			return err
		}
		// PostgreSQL accepts some cross-type foreign keys through compatible
		// operator classes. DMTX uses the stricter exact-rendered-type rule so
		// planning never defers type compatibility to post-load ALTER TABLE.
		if columnType != referencedType {
			return postgresObjectPolicy(
				"create PostgreSQL foreign key",
				"incompatible column types for table "+
					table.source.Name,
			)
		}
	}
	return nil
}

func postgresColumnsAreUnique(table Table, columns []string) bool {
	primaryKey := orderedPrimaryKeyColumns(table)
	primaryKeyNames := make([]string, len(primaryKey))
	for index, column := range primaryKey {
		primaryKeyNames[index] = column.Name
	}
	if equalIdentifierLists(primaryKeyNames, columns) {
		return true
	}
	for _, index := range table.Indexes {
		if !index.Unique || len(index.Columns) != len(columns) {
			continue
		}
		indexNames := make([]string, len(index.Columns))
		for position, column := range index.Columns {
			if column.Name == "" {
				indexNames = nil
				break
			}
			indexNames[position] = column.Name
		}
		if equalIdentifierLists(indexNames, columns) {
			return true
		}
	}
	return false
}

func equalIdentifierLists(left, right []string) bool {
	if len(left) == 0 || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func reservePostgresObjectNames(
	tables []postgresObjectTable,
) (
	*postgresNameAllocator,
	*postgresNameAllocator,
	error,
) {
	relations := newPostgresNameAllocator()
	constraints := newPostgresNameAllocator()
	for index := range tables {
		table := &tables[index]
		relationScope := table.targetSchema
		constraintScope := postgresTargetTableKey(
			table.targetSchema,
			table.source.Name,
		)
		relations.reserve(relationScope, table.source.Name)
		if len(orderedPrimaryKeyColumns(table.source)) > 0 {
			primaryKeyName := postgresGeneratedRelationName(
				table.source.Name,
				"",
				"pkey",
			)
			relations.reserve(relationScope, primaryKeyName)
			constraints.reserve(constraintScope, primaryKeyName)
		}
		identityColumn, err := postgresIdentityColumn(table.source)
		if err != nil {
			return nil, nil, err
		}
		if identityColumn != "" {
			relations.reserve(
				relationScope,
				postgresGeneratedRelationName(
					table.source.Name,
					identityColumn,
					"seq",
				),
			)
		}
	}
	return relations, constraints, nil
}

func planPostgresIndex(
	spec postgresObjectSpec,
	names *postgresNameAllocator,
) (PostgresObjectStatement, error) {
	preferredName := spec.index.Name
	if preferredName == "" {
		preferredName = "dmtx_" + spec.table.source.Name + "_" +
			postgresIdentifierComponents(spec.index.Columns) + "_key"
	}
	name := names.allocate(
		spec.table.targetSchema,
		preferredName,
		postgresObjectNameSeed(spec),
	)

	columns := make([]string, len(spec.index.Columns))
	for index, sourceColumn := range spec.index.Columns {
		column := spec.table.columns[sourceColumn.Name]
		columns[index] = renderPostgresObjectIndexColumn(
			column,
			sourceColumn,
		)
	}
	unique := ""
	if spec.index.Unique {
		unique = "UNIQUE "
	}
	sql := "CREATE " + unique + "INDEX " + quote(Postgres, name) +
		" ON " + qualified(
		Postgres,
		spec.table.targetSchema,
		spec.table.source.Name,
	) + " (" + strings.Join(columns, ", ") + ");"
	return PostgresObjectStatement{
		Kind:   PostgresIndexObject,
		Schema: spec.table.targetSchema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func renderPostgresObjectIndexColumn(
	column Column,
	indexColumn IndexColumn,
) string {
	rendered := quote(Postgres, indexColumn.Name)
	if postgresObjectColumnIsText(column) {
		rendered += " COLLATE " +
			qualified(Postgres, "pg_catalog", "C")
	}
	if indexColumn.Descending {
		return rendered + " DESC NULLS LAST"
	}
	return rendered + " ASC NULLS FIRST"
}

func postgresObjectColumnIsText(column Column) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(
			column.DeclaredType.Base,
		))
	}
	switch base {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		return true
	default:
		return false
	}
}

func planPostgresCheck(
	spec postgresObjectSpec,
	names *postgresNameAllocator,
) (PostgresObjectStatement, error) {
	preferredName := spec.check.Name
	if preferredName == "" {
		preferredName = "dmtx_" + spec.table.source.Name + "_check"
	}
	name := names.allocate(
		postgresTargetTableKey(
			spec.table.targetSchema,
			spec.table.source.Name,
		),
		preferredName,
		postgresObjectNameSeed(spec),
	)
	expression, err := RenderSQLiteCheckForPostgres(
		spec.check.Expression,
		spec.table.source.Columns,
	)
	if err != nil {
		return PostgresObjectStatement{}, fmt.Errorf(
			"render PostgreSQL CHECK for table %s: %w",
			spec.table.source.Name,
			err,
		)
	}
	expression = strings.TrimSpace(expression)
	if err := validateRenderedPostgresCheck(expression); err != nil {
		return PostgresObjectStatement{}, postgresObjectPolicy(
			"render PostgreSQL CHECK",
			checkDisplayName(spec.table.source, spec.check),
		)
	}
	sql := "ALTER TABLE " + qualified(
		Postgres,
		spec.table.targetSchema,
		spec.table.source.Name,
	) + " ADD CONSTRAINT " + quote(Postgres, name) +
		" CHECK (" + expression + ");"
	return PostgresObjectStatement{
		Kind:   PostgresCheckObject,
		Schema: spec.table.targetSchema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func validateRenderedPostgresCheck(expression string) error {
	if expression == "" ||
		!utf8.ValidString(expression) ||
		strings.ContainsRune(expression, '\x00') {
		return fmt.Errorf("invalid expression")
	}
	depth := 0
	for index := 0; index < len(expression); index++ {
		current := expression[index]
		if current < 0x20 &&
			current != '\t' &&
			current != '\n' &&
			current != '\r' {
			return fmt.Errorf("control character")
		}
		if current == ';' || current == '$' {
			return fmt.Errorf("expression boundary token")
		}
		if index+1 < len(expression) &&
			((current == '-' && expression[index+1] == '-') ||
				(current == '/' && expression[index+1] == '*') ||
				(current == '*' && expression[index+1] == '/')) {
			return fmt.Errorf("comment token")
		}
		switch current {
		case '\'', '"':
			next, ok := consumeQuoted(
				expression,
				index,
				current,
				current,
			)
			if !ok {
				return fmt.Errorf("unterminated quote")
			}
			index = next
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced parentheses")
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unbalanced parentheses")
	}
	return nil
}

func planPostgresForeignKey(
	spec postgresObjectSpec,
	names *postgresNameAllocator,
) (PostgresObjectStatement, error) {
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
	preferredName := "dmtx_" + spec.table.source.Name + "_" +
		strings.Join(foreignKey.Columns, "_") + "_fkey"
	name := names.allocate(
		postgresTargetTableKey(
			spec.table.targetSchema,
			spec.table.source.Name,
		),
		preferredName,
		postgresObjectNameSeed(spec),
	)
	onUpdate, _ := normalizePostgresForeignKeyAction(foreignKey.OnUpdate)
	onDelete, _ := normalizePostgresForeignKeyAction(foreignKey.OnDelete)
	match, _ := normalizePostgresForeignKeyMatch(foreignKey.Match)
	sql := "ALTER TABLE " + qualified(
		Postgres,
		spec.table.targetSchema,
		spec.table.source.Name,
	) + " ADD CONSTRAINT " + quote(Postgres, name) +
		" FOREIGN KEY (" +
		postgresQuotedIdentifiers(foreignKey.Columns) +
		") REFERENCES " + qualified(
		Postgres,
		spec.referenced.targetSchema,
		spec.referenced.source.Name,
	) + " (" + postgresQuotedIdentifiers(referencedColumns) + ")" +
		" MATCH " + match +
		" ON UPDATE " + onUpdate +
		" ON DELETE " + onDelete + ";"
	return PostgresObjectStatement{
		Kind:   PostgresForeignKeyObject,
		Schema: spec.table.targetSchema,
		Table:  spec.table.source.Name,
		Name:   name,
		SQL:    sql,
	}, nil
}

func normalizePostgresForeignKeyAction(value string) (string, error) {
	normalized := strings.ToUpper(strings.Join(strings.Fields(value), " "))
	if normalized == "" {
		normalized = "NO ACTION"
	}
	switch normalized {
	case "NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT":
		return normalized, nil
	default:
		return "", postgresObjectPolicy(
			"create PostgreSQL foreign key action",
			value,
		)
	}
}

func normalizePostgresForeignKeyMatch(value string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch normalized {
	case "", "NONE", "SIMPLE":
		return "SIMPLE", nil
	case "FULL":
		return "FULL", nil
	default:
		return "", postgresObjectPolicy(
			"create PostgreSQL foreign key match",
			value,
		)
	}
}

func postgresQuotedIdentifiers(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quote(Postgres, value)
	}
	return strings.Join(quoted, ", ")
}

func postgresIdentifierComponents(columns []IndexColumn) string {
	components := make([]string, len(columns))
	for index, column := range columns {
		components[index] = column.Name
	}
	return strings.Join(components, "_")
}

func postgresObjectNameSeed(spec postgresObjectSpec) string {
	return strconv.Itoa(int(spec.kind)) + "\x00" +
		spec.table.source.Schema + "\x00" +
		spec.table.source.Name + "\x00" +
		spec.sortKey + "\x00" +
		strconv.Itoa(spec.occurrence)
}

func postgresIndexSortKey(index Index) string {
	parts := []string{
		index.Name,
		strconv.FormatBool(index.Unique),
		strconv.FormatBool(index.Inline),
	}
	for _, column := range index.Columns {
		parts = append(
			parts,
			column.Name,
			strconv.FormatBool(column.Descending),
			strings.ToUpper(strings.TrimSpace(column.Collation)),
		)
	}
	return strings.Join(parts, "\x00")
}

func postgresCheckSortKey(check CheckConstraint) string {
	return check.Name + "\x00" + check.Expression.CanonicalSQL()
}

func postgresForeignKeySortKey(foreignKey ForeignKey) string {
	parts := append([]string(nil), foreignKey.Columns...)
	parts = append(parts, foreignKey.ReferencedTable)
	parts = append(parts, foreignKey.ReferencedColumns...)
	parts = append(
		parts,
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnUpdate,
		), " ")),
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnDelete,
		), " ")),
		strings.ToUpper(strings.TrimSpace(foreignKey.Match)),
	)
	return strings.Join(parts, "\x00")
}

func indexDisplayName(table Table, index Index) string {
	if index.Name != "" {
		return index.Name
	}
	return table.Name
}

func checkDisplayName(table Table, check CheckConstraint) string {
	if check.Name != "" {
		return check.Name
	}
	return table.Name
}

func postgresSourceTableKey(schema, table string) string {
	return schema + "\x00" + table
}

func postgresTargetTableKey(schema, table string) string {
	return schema + "\x00" + table
}

func validateSourceObjectIdentifier(
	kind string,
	value string,
	allowEmpty bool,
) error {
	if value == "" && allowEmpty {
		return nil
	}
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return postgresObjectPolicy(
			"validate PostgreSQL "+kind+" identifier",
			"invalid identifier",
		)
	}
	return nil
}

func validatePostgresObjectIdentifier(kind, value string) error {
	if err := validateSourceObjectIdentifier(
		kind,
		value,
		false,
	); err != nil {
		return err
	}
	if len(value) > postgresIdentifierMaximumBytes {
		return postgresObjectPolicy(
			"validate PostgreSQL "+kind+" identifier",
			"identifier exceeds 63 bytes",
		)
	}
	return nil
}

func postgresObjectPolicy(operation, value string) error {
	return &PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(Postgres),
	}
}

type postgresNameAllocator struct {
	used map[string]map[string]struct{}
}

func newPostgresNameAllocator() *postgresNameAllocator {
	return &postgresNameAllocator{
		used: make(map[string]map[string]struct{}),
	}
}

func (allocator *postgresNameAllocator) reserve(scope, name string) {
	if allocator.used[scope] == nil {
		allocator.used[scope] = make(map[string]struct{})
	}
	allocator.used[scope][name] = struct{}{}
}

func (allocator *postgresNameAllocator) allocate(
	scope string,
	preferred string,
	seed string,
) string {
	if len(preferred) <= postgresIdentifierMaximumBytes &&
		!allocator.contains(scope, preferred) {
		allocator.reserve(scope, preferred)
		return preferred
	}
	for attempt := 0; ; attempt++ {
		candidate := postgresHashedIdentifier(
			preferred,
			seed,
			attempt,
		)
		if allocator.contains(scope, candidate) {
			continue
		}
		allocator.reserve(scope, candidate)
		return candidate
	}
}

func (allocator *postgresNameAllocator) contains(
	scope string,
	name string,
) bool {
	_, exists := allocator.used[scope][name]
	return exists
}

func postgresHashedIdentifier(
	preferred string,
	seed string,
	attempt int,
) string {
	if preferred == "" {
		preferred = "dmtx"
	}
	digest := sha256.Sum256([]byte(
		seed + "\x00" + strconv.Itoa(attempt),
	))
	suffix := "_" + hex.EncodeToString(digest[:6])
	prefix := truncatePostgresIdentifier(
		preferred,
		postgresIdentifierMaximumBytes-len(suffix),
	)
	if prefix == "" {
		prefix = "dmtx"
	}
	return prefix + suffix
}

func truncatePostgresIdentifier(value string, maximumBytes int) string {
	if len(value) <= maximumBytes {
		return value
	}
	end := maximumBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

// postgresGeneratedRelationName mirrors PostgreSQL's conservative
// makeObjectName shape: separators and the label are retained while the two
// user-controlled components are clipped evenly to the 63-byte identifier
// limit. This is used only to reserve server-generated relation names.
func postgresGeneratedRelationName(
	first string,
	second string,
	label string,
) string {
	firstBytes := len(first)
	secondBytes := len(second)
	overhead := len(label) + 1
	if second != "" {
		overhead++
	}
	available := postgresIdentifierMaximumBytes - overhead
	for firstBytes+secondBytes > available {
		if firstBytes > secondBytes {
			firstBytes--
		} else {
			secondBytes--
		}
	}
	first = truncatePostgresIdentifier(first, firstBytes)
	second = truncatePostgresIdentifier(second, secondBytes)
	name := first
	if second != "" {
		name += "_" + second
	}
	return name + "_" + label
}
