package schema

import (
	"reflect"
	"strconv"
	"strings"
)

// PostgresObjectNameReservation records one authenticated PostgreSQL relation
// name which is outside the logical table catalog but still participates in
// the namespace-wide relation collision domain.
type PostgresObjectNameReservation struct {
	Namespace string
	Name      string
}

// MaterializePostgresObjectNames returns a deep copy of tables with every
// deterministic PostgreSQL index, CHECK, and foreign-key name written into its
// canonical object metadata. It uses the exact planner specs and name
// allocators that render target DDL, so an unnamed portable source object can
// be compared exactly with the name PostgreSQL will expose through pg_catalog.
func MaterializePostgresObjectNames(
	tables []Table,
	options PostgresObjectPlanOptions,
) ([]Table, error) {
	planned, err := planPostgresObjectTables(
		tables,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err := collectPostgresObjectSpecs(planned)
	if err != nil {
		return nil, err
	}
	relationNames, constraintNames, err :=
		reservePostgresObjectNames(planned)
	if err != nil {
		return nil, err
	}

	materialized := make([]Table, len(tables))
	for index, table := range tables {
		materialized[index] = cloneEvolutionTable(table)
	}
	for _, spec := range indexes {
		statement, err := planPostgresIndex(spec, relationNames)
		if err != nil {
			return nil, err
		}
		if err := materializePostgresIndexName(
			materialized,
			spec,
			statement.Name(),
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range checks {
		statement, err := planPostgresCheck(spec, constraintNames)
		if err != nil {
			return nil, err
		}
		if err := materializePostgresCheckName(
			materialized,
			spec,
			statement.Name(),
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range foreignKeys {
		statement, err := planPostgresForeignKey(
			spec,
			constraintNames,
		)
		if err != nil {
			return nil, err
		}
		if err := materializePostgresForeignKeyName(
			materialized,
			spec,
			statement.Name(),
		); err != nil {
			return nil, err
		}
	}
	if _, err := PlanPostgresDropRecreateObjects(
		materialized,
		options,
	); err != nil {
		return nil, err
	}
	return materialized, nil
}

// MaterializePostgresObjectNamesAfterPrior preserves the exact names assigned
// to structurally retained prior objects before allocating names to newly
// added objects. priorTables is the pre-materialization prior shape and
// priorMaterialized is the authenticated target-ready result previously
// produced from it.
//
// Ordinary MaterializePostgresObjectNames allocation remains independent of
// prior state. This paired form is only for incremental schema projection.
func MaterializePostgresObjectNamesAfterPrior(
	tables []Table,
	priorTables []Table,
	priorMaterialized []Table,
	options PostgresObjectPlanOptions,
) ([]Table, error) {
	expectedPrior, err := MaterializePostgresObjectNames(
		priorTables,
		options,
	)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(expectedPrior, priorMaterialized) {
		return nil, postgresObjectPolicy(
			"materialize PostgreSQL object names after prior",
			"prior target evidence does not match its exact source projection",
		)
	}
	return materializePostgresObjectNamesAfterPriorAuthority(
		tables,
		priorTables,
		priorMaterialized,
		nil,
		options,
	)
}

// MaterializePostgresObjectNamesAfterPriorAuthority preserves source-backed
// prior names while reserving every relation and constraint name from the
// authenticated full target authority. This lets a new source object allocate
// deterministically around target-only objects which remain present after the
// projection.
func MaterializePostgresObjectNamesAfterPriorAuthority(
	tables []Table,
	priorTables []Table,
	authority []Table,
	reservations []PostgresObjectNameReservation,
	options PostgresObjectPlanOptions,
) ([]Table, error) {
	return materializePostgresObjectNamesAfterPriorAuthority(
		tables,
		priorTables,
		authority,
		reservations,
		options,
	)
}

func materializePostgresObjectNamesAfterPriorAuthority(
	tables []Table,
	priorTables []Table,
	authority []Table,
	reservations []PostgresObjectNameReservation,
	options PostgresObjectPlanOptions,
) ([]Table, error) {
	priorNames, err := authenticatePostgresPriorObjectNames(
		priorTables,
		authority,
		options,
	)
	if err != nil {
		return nil, err
	}

	planned, err := planPostgresObjectTables(
		tables,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err := collectPostgresObjectSpecs(planned)
	if err != nil {
		return nil, err
	}
	currentSpecs := appendPostgresObjectSpecs(
		nil,
		indexes,
		checks,
		foreignKeys,
	)
	currentKeys := make(
		map[postgresRetainedObjectKey]struct{},
		len(currentSpecs),
	)
	for _, spec := range currentSpecs {
		currentKeys[postgresObjectRetentionKey(spec)] = struct{}{}
	}

	authorityPlanned, err := planPostgresObjectTables(
		authority,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	authorityIndexes, authorityChecks, authorityForeignKeys, err :=
		collectPostgresObjectSpecs(authorityPlanned)
	if err != nil {
		return nil, err
	}
	rigidTables := append(
		[]postgresObjectTable(nil),
		authorityPlanned...,
	)
	authorityTableKeys := make(map[string]struct{}, len(authorityPlanned))
	for _, table := range authorityPlanned {
		authorityTableKeys[postgresTargetTableKey(
			table.targetSchema,
			table.source.Name,
		)] = struct{}{}
	}
	for _, table := range planned {
		key := postgresTargetTableKey(
			table.targetSchema,
			table.source.Name,
		)
		if _, exists := authorityTableKeys[key]; exists {
			continue
		}
		rigidTables = append(rigidTables, table)
	}
	relationNames, constraintNames, relationOwners, constraintOwners, err :=
		reserveStrictPostgresRigidNames(rigidTables)
	if err != nil {
		return nil, err
	}
	if err := reservePostgresAuthorityObjectNames(
		authorityIndexes,
		authorityChecks,
		authorityForeignKeys,
		relationNames,
		constraintNames,
		relationOwners,
		constraintOwners,
	); err != nil {
		return nil, err
	}
	if err := reservePostgresAuthorityRelationNames(
		reservations,
		relationNames,
		relationOwners,
	); err != nil {
		return nil, err
	}
	retainedNames := make(
		map[postgresRetainedObjectKey]string,
		len(priorNames),
	)
	for _, prior := range priorNames {
		if _, retained := currentKeys[prior.key]; !retained {
			continue
		}
		var allocator *postgresNameAllocator
		var owners map[string]string
		var scope string
		var class string
		if prior.key.kind == PostgresIndexObject {
			allocator = relationNames
			owners = relationOwners
			scope = prior.key.schema
			class = "relation"
		} else {
			allocator = constraintNames
			owners = constraintOwners
			scope = postgresTargetTableKey(
				prior.key.schema,
				prior.key.table,
			)
			class = "constraint"
		}
		if !allocator.contains(scope, prior.name) {
			if err := reserveStrictPostgresName(
				allocator,
				owners,
				scope,
				prior.name,
				"retained "+postgresObjectKindDescription(prior.key.kind)+
					" on "+prior.key.schema+"."+prior.key.table,
				class,
			); err != nil {
				return nil, err
			}
		}
		retainedNames[prior.key] = prior.name
	}

	materialized := make([]Table, len(tables))
	for index, table := range tables {
		materialized[index] = cloneEvolutionTable(table)
	}
	for _, spec := range indexes {
		name := retainedNames[postgresObjectRetentionKey(spec)]
		if name == "" {
			name = allocatePostgresObjectSpecName(spec, relationNames)
		}
		if err := materializePostgresIndexName(
			materialized,
			spec,
			name,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range checks {
		name := retainedNames[postgresObjectRetentionKey(spec)]
		if name == "" {
			name = allocatePostgresObjectSpecName(spec, constraintNames)
		}
		if err := materializePostgresCheckName(
			materialized,
			spec,
			name,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range foreignKeys {
		name := retainedNames[postgresObjectRetentionKey(spec)]
		if name == "" {
			name = allocatePostgresObjectSpecName(spec, constraintNames)
		}
		if err := materializePostgresForeignKeyName(
			materialized,
			spec,
			name,
		); err != nil {
			return nil, err
		}
	}
	if _, err := PlanPostgresDropRecreateObjects(
		materialized,
		options,
	); err != nil {
		return nil, err
	}
	return materialized, nil
}

func authenticatePostgresPriorObjectNames(
	prior []Table,
	authority []Table,
	options PostgresObjectPlanOptions,
) ([]postgresPlannedObjectName, error) {
	priorPlanned, err := planPostgresObjectTables(
		prior,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	authorityPlanned, err := planPostgresObjectTables(
		authority,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	authorityByTable := make(
		map[string]Table,
		len(authorityPlanned),
	)
	for _, table := range authorityPlanned {
		authorityByTable[postgresTargetTableKey(
			table.targetSchema,
			table.source.Name,
		)] = table.source
	}
	for _, expected := range priorPlanned {
		key := postgresTargetTableKey(
			expected.targetSchema,
			expected.source.Name,
		)
		actual, found := authorityByTable[key]
		if !found ||
			expected.source.MySQLCollation != actual.MySQLCollation ||
			!reflect.DeepEqual(
				expected.source.ClickHouseOrderBy,
				actual.ClickHouseOrderBy,
			) ||
			!reflect.DeepEqual(expected.source.Identity, actual.Identity) ||
			expected.source.SQLiteWithoutRowID != actual.SQLiteWithoutRowID ||
			expected.source.SQLiteStrict != actual.SQLiteStrict ||
			!postgresMaterializedValuesContained(
				expected.source.Columns,
				actual.Columns,
			) {
			return nil, postgresObjectPolicy(
				"materialize PostgreSQL object names after prior",
				"full target authority does not contain the exact source-backed prior table "+key,
			)
		}
	}

	priorIndexes, priorChecks, priorForeignKeys, err :=
		collectPostgresObjectSpecs(priorPlanned)
	if err != nil {
		return nil, err
	}
	authorityIndexes, authorityChecks, authorityForeignKeys, err :=
		collectPostgresObjectSpecs(authorityPlanned)
	if err != nil {
		return nil, err
	}
	var result []postgresPlannedObjectName
	for _, pair := range []struct {
		expected []postgresObjectSpec
		actual   []postgresObjectSpec
	}{
		{expected: priorIndexes, actual: authorityIndexes},
		{expected: priorChecks, actual: authorityChecks},
		{expected: priorForeignKeys, actual: authorityForeignKeys},
	} {
		names, err := authenticatePostgresPriorObjectGroup(
			pair.expected,
			pair.actual,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, names...)
	}
	return result, nil
}

func authenticatePostgresPriorObjectGroup(
	expected []postgresObjectSpec,
	actual []postgresObjectSpec,
) ([]postgresPlannedObjectName, error) {
	used := make([]bool, len(actual))
	result := make([]postgresPlannedObjectName, 0, len(expected))
	for _, wanted := range expected {
		match := -1
		for index, candidate := range actual {
			if used[index] ||
				!postgresObjectSpecsStructurallyEqual(wanted, candidate) {
				continue
			}
			if match >= 0 {
				return nil, postgresObjectPolicy(
					"materialize PostgreSQL object names after prior",
					"full target authority ambiguously represents a source-backed prior "+
						postgresObjectKindDescription(wanted.kind),
				)
			}
			match = index
		}
		if match < 0 {
			return nil, postgresObjectPolicy(
				"materialize PostgreSQL object names after prior",
				"full target authority is missing a source-backed prior "+
					postgresObjectKindDescription(wanted.kind),
			)
		}
		name := postgresObjectSpecExactName(actual[match])
		if err := validatePostgresObjectIdentifier(
			postgresObjectKindDescription(wanted.kind),
			name,
		); err != nil {
			return nil, err
		}
		used[match] = true
		result = append(result, postgresPlannedObjectName{
			key:  postgresObjectRetentionKey(wanted),
			name: name,
		})
	}
	return result, nil
}

func postgresObjectSpecsStructurallyEqual(
	left postgresObjectSpec,
	right postgresObjectSpec,
) bool {
	if left.kind != right.kind ||
		left.table.targetSchema != right.table.targetSchema ||
		left.table.source.Name != right.table.source.Name {
		return false
	}
	switch left.kind {
	case PostgresIndexObject:
		leftValue := left.index
		rightValue := right.index
		leftValue.Name = ""
		rightValue.Name = ""
		return reflect.DeepEqual(leftValue, rightValue)
	case PostgresCheckObject:
		leftValue := left.check
		rightValue := right.check
		leftValue.Name = ""
		rightValue.Name = ""
		return reflect.DeepEqual(leftValue, rightValue)
	case PostgresForeignKeyObject:
		leftValue := left.foreignKey
		rightValue := right.foreignKey
		leftValue.Name = ""
		rightValue.Name = ""
		return reflect.DeepEqual(leftValue, rightValue)
	default:
		return false
	}
}

func postgresObjectSpecExactName(spec postgresObjectSpec) string {
	switch spec.kind {
	case PostgresIndexObject:
		return spec.index.Name
	case PostgresCheckObject:
		return spec.check.Name
	case PostgresForeignKeyObject:
		return spec.foreignKey.Name
	default:
		return ""
	}
}

func postgresMaterializedValuesContained[T any](
	expected []T,
	actual []T,
) bool {
	for _, wanted := range expected {
		found := false
		for _, candidate := range actual {
			if reflect.DeepEqual(wanted, candidate) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func reservePostgresAuthorityObjectNames(
	indexes []postgresObjectSpec,
	checks []postgresObjectSpec,
	foreignKeys []postgresObjectSpec,
	relations *postgresNameAllocator,
	constraints *postgresNameAllocator,
	relationOwners map[string]string,
	constraintOwners map[string]string,
) error {
	for _, spec := range indexes {
		name := spec.index.Name
		if name == "" {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names after prior",
				"full target authority contains an unnamed index",
			)
		}
		if err := validatePostgresObjectIdentifier("index", name); err != nil {
			return err
		}
		if err := reserveStrictPostgresName(
			relations,
			relationOwners,
			spec.table.targetSchema,
			name,
			"target-authority index on "+
				spec.table.targetSchema+"."+spec.table.source.Name,
			"relation",
		); err != nil {
			return err
		}
	}
	for _, group := range [][]postgresObjectSpec{
		checks,
		foreignKeys,
	} {
		for _, spec := range group {
			name := spec.check.Name
			if spec.kind == PostgresForeignKeyObject {
				name = spec.foreignKey.Name
			}
			if name == "" {
				return postgresObjectPolicy(
					"materialize PostgreSQL object names after prior",
					"full target authority contains an unnamed constraint",
				)
			}
			if err := validatePostgresObjectIdentifier(
				"constraint",
				name,
			); err != nil {
				return err
			}
			if err := reserveStrictPostgresName(
				constraints,
				constraintOwners,
				postgresTargetTableKey(
					spec.table.targetSchema,
					spec.table.source.Name,
				),
				name,
				"target-authority "+
					postgresObjectKindDescription(spec.kind)+
					" on "+spec.table.targetSchema+"."+
					spec.table.source.Name,
				"constraint",
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func reservePostgresAuthorityRelationNames(
	reservations []PostgresObjectNameReservation,
	relations *postgresNameAllocator,
	relationOwners map[string]string,
) error {
	seen := make(map[string]struct{}, len(reservations))
	for index, reservation := range reservations {
		if strings.TrimSpace(reservation.Namespace) == "" ||
			reservation.Namespace != strings.TrimSpace(
				reservation.Namespace,
			) ||
			strings.TrimSpace(reservation.Name) == "" ||
			reservation.Name != strings.TrimSpace(reservation.Name) {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names after prior",
				"target-authority relation reservation "+
					strconv.Itoa(index)+" is non-canonical",
			)
		}
		if err := validatePostgresObjectIdentifier(
			"reservation namespace",
			reservation.Namespace,
		); err != nil {
			return err
		}
		if err := validatePostgresObjectIdentifier(
			"reserved relation",
			reservation.Name,
		); err != nil {
			return err
		}
		key := postgresTargetTableKey(
			reservation.Namespace,
			reservation.Name,
		)
		if _, duplicate := seen[key]; duplicate {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names after prior",
				"duplicate target-authority relation reservation "+
					reservation.Namespace+"."+reservation.Name,
			)
		}
		seen[key] = struct{}{}
		if err := reserveStrictPostgresName(
			relations,
			relationOwners,
			reservation.Namespace,
			reservation.Name,
			"target-authority unmodeled relation reservation",
			"relation",
		); err != nil {
			return err
		}
	}
	return nil
}

type postgresRetainedObjectKey struct {
	kind       PostgresObjectKind
	schema     string
	table      string
	sortKey    string
	occurrence int
}

type postgresPlannedObjectName struct {
	key  postgresRetainedObjectKey
	name string
}

func postgresObjectRetentionKey(
	spec postgresObjectSpec,
) postgresRetainedObjectKey {
	return postgresRetainedObjectKey{
		kind:       spec.kind,
		schema:     spec.table.targetSchema,
		table:      spec.table.source.Name,
		sortKey:    spec.sortKey,
		occurrence: spec.occurrence,
	}
}

func appendPostgresObjectSpecs(
	destination []postgresObjectSpec,
	groups ...[]postgresObjectSpec,
) []postgresObjectSpec {
	for _, group := range groups {
		destination = append(destination, group...)
	}
	return destination
}

func plannedPostgresObjectNames(
	tables []postgresObjectTable,
	indexes []postgresObjectSpec,
	checks []postgresObjectSpec,
	foreignKeys []postgresObjectSpec,
) ([]postgresPlannedObjectName, error) {
	relations, constraints, err := reservePostgresObjectNames(tables)
	if err != nil {
		return nil, err
	}
	result := make(
		[]postgresPlannedObjectName,
		0,
		len(indexes)+len(checks)+len(foreignKeys),
	)
	for _, spec := range indexes {
		statement, err := planPostgresIndex(spec, relations)
		if err != nil {
			return nil, err
		}
		result = append(result, postgresPlannedObjectName{
			key:  postgresObjectRetentionKey(spec),
			name: statement.Name(),
		})
	}
	for _, spec := range checks {
		statement, err := planPostgresCheck(spec, constraints)
		if err != nil {
			return nil, err
		}
		result = append(result, postgresPlannedObjectName{
			key:  postgresObjectRetentionKey(spec),
			name: statement.Name(),
		})
	}
	for _, spec := range foreignKeys {
		statement, err := planPostgresForeignKey(spec, constraints)
		if err != nil {
			return nil, err
		}
		result = append(result, postgresPlannedObjectName{
			key:  postgresObjectRetentionKey(spec),
			name: statement.Name(),
		})
	}
	return result, nil
}

func reserveStrictPostgresRigidNames(
	tables []postgresObjectTable,
) (
	*postgresNameAllocator,
	*postgresNameAllocator,
	map[string]string,
	map[string]string,
	error,
) {
	relations := newPostgresNameAllocator()
	constraints := newPostgresNameAllocator()
	relationOwners := make(map[string]string)
	constraintOwners := make(map[string]string)
	for index := range tables {
		table := &tables[index]
		schemaName := table.targetSchema
		tableName := table.source.Name
		tableOwner := "table " + schemaName + "." + tableName
		if err := reserveStrictPostgresName(
			relations,
			relationOwners,
			schemaName,
			tableName,
			tableOwner,
			"relation",
		); err != nil {
			return nil, nil, nil, nil, err
		}
		if len(orderedPrimaryKeyColumns(table.source)) > 0 {
			primaryKeyName := postgresGeneratedRelationName(
				tableName,
				"",
				"pkey",
			)
			if err := reserveStrictPostgresName(
				relations,
				relationOwners,
				schemaName,
				primaryKeyName,
				"primary key on "+schemaName+"."+tableName,
				"relation",
			); err != nil {
				return nil, nil, nil, nil, err
			}
			if err := reserveStrictPostgresName(
				constraints,
				constraintOwners,
				postgresTargetTableKey(schemaName, tableName),
				primaryKeyName,
				"primary key on "+schemaName+"."+tableName,
				"constraint",
			); err != nil {
				return nil, nil, nil, nil, err
			}
		}
		identityColumn, err := postgresIdentityColumn(table.source)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if identityColumn != "" {
			sequenceName := postgresGeneratedRelationName(
				tableName,
				identityColumn,
				"seq",
			)
			if err := reserveStrictPostgresName(
				relations,
				relationOwners,
				schemaName,
				sequenceName,
				"identity sequence on "+schemaName+"."+tableName,
				"relation",
			); err != nil {
				return nil, nil, nil, nil, err
			}
		}
	}
	return relations, constraints, relationOwners, constraintOwners, nil
}

func reserveStrictPostgresName(
	allocator *postgresNameAllocator,
	owners map[string]string,
	scope string,
	name string,
	owner string,
	class string,
) error {
	key := strconv.Itoa(len(scope)) + ":" + scope + name
	if existing, exists := owners[key]; exists {
		return postgresObjectPolicy(
			"materialize PostgreSQL object names after prior",
			class+" name "+name+" collides between "+existing+" and "+owner,
		)
	}
	allocator.reserve(scope, name)
	owners[key] = owner
	return nil
}

func allocatePostgresObjectSpecName(
	spec postgresObjectSpec,
	allocator *postgresNameAllocator,
) string {
	var scope string
	var preferred string
	switch spec.kind {
	case PostgresIndexObject:
		scope = spec.table.targetSchema
		preferred = spec.index.Name
		if preferred == "" {
			preferred = "dmtx_" + spec.table.source.Name + "_" +
				postgresIdentifierComponents(spec.index.Columns) + "_key"
		}
	case PostgresCheckObject:
		scope = postgresTargetTableKey(
			spec.table.targetSchema,
			spec.table.source.Name,
		)
		preferred = spec.check.Name
		if preferred == "" {
			preferred = "dmtx_" + spec.table.source.Name + "_check"
		}
	case PostgresForeignKeyObject:
		scope = postgresTargetTableKey(
			spec.table.targetSchema,
			spec.table.source.Name,
		)
		preferred = spec.foreignKey.Name
		if preferred == "" {
			preferred = "dmtx_" + spec.table.source.Name + "_" +
				joinPostgresForeignKeyNameColumns(
					spec.foreignKey.Columns,
				) + "_fkey"
		}
	}
	return allocator.allocate(
		scope,
		preferred,
		postgresObjectNameSeed(spec),
	)
}

func joinPostgresForeignKeyNameColumns(columns []string) string {
	result := ""
	for index, column := range columns {
		if index > 0 {
			result += "_"
		}
		result += column
	}
	return result
}

func postgresObjectKindDescription(kind PostgresObjectKind) string {
	switch kind {
	case PostgresIndexObject:
		return "index"
	case PostgresCheckObject:
		return "CHECK constraint"
	case PostgresForeignKeyObject:
		return "foreign-key constraint"
	default:
		return "object"
	}
}

func materializePostgresIndexName(
	tables []Table,
	spec postgresObjectSpec,
	name string,
) error {
	table, err := postgresMaterializedObjectTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Indexes {
		if postgresIndexSortKey(table.Indexes[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names",
				"ambiguous index on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return postgresObjectPolicy(
			"materialize PostgreSQL object names",
			"missing index on "+table.Name,
		)
	}
	table.Indexes[matched].Name = name
	return nil
}

func materializePostgresCheckName(
	tables []Table,
	spec postgresObjectSpec,
	name string,
) error {
	table, err := postgresMaterializedObjectTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Checks {
		if postgresCheckSortKey(table.Checks[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names",
				"ambiguous CHECK on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return postgresObjectPolicy(
			"materialize PostgreSQL object names",
			"missing CHECK on "+table.Name,
		)
	}
	table.Checks[matched].Name = name
	return nil
}

func materializePostgresForeignKeyName(
	tables []Table,
	spec postgresObjectSpec,
	name string,
) error {
	table, err := postgresMaterializedObjectTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.ForeignKeys {
		if postgresForeignKeySortKey(
			table.ForeignKeys[index],
		) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return postgresObjectPolicy(
				"materialize PostgreSQL object names",
				"ambiguous foreign key on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return postgresObjectPolicy(
			"materialize PostgreSQL object names",
			"missing foreign key on "+table.Name,
		)
	}
	table.ForeignKeys[matched].Name = name
	return nil
}

func postgresMaterializedObjectTable(
	tables []Table,
	spec postgresObjectSpec,
) (*Table, error) {
	for index := range tables {
		if tables[index].Schema == spec.table.source.Schema &&
			tables[index].Name == spec.table.source.Name {
			return &tables[index], nil
		}
	}
	return nil, postgresObjectPolicy(
		"materialize PostgreSQL object names",
		"missing table "+spec.table.source.Schema+"."+
			spec.table.source.Name,
	)
}
