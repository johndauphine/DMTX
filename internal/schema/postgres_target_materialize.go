package schema

import (
	"reflect"
	"strconv"
)

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

	priorPlanned, err := planPostgresObjectTables(
		priorTables,
		options.MapNamespace,
	)
	if err != nil {
		return nil, err
	}
	priorIndexes, priorChecks, priorForeignKeys, err :=
		collectPostgresObjectSpecs(priorPlanned)
	if err != nil {
		return nil, err
	}
	priorNames, err := plannedPostgresObjectNames(
		priorPlanned,
		priorIndexes,
		priorChecks,
		priorForeignKeys,
	)
	if err != nil {
		return nil, err
	}

	relationNames, constraintNames, relationOwners, constraintOwners, err :=
		reserveStrictPostgresRigidNames(planned)
	if err != nil {
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
