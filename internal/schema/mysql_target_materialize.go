package schema

// MaterializeMySQLObjectNames returns a deep copy of tables with every
// deterministic MySQL index, CHECK, and foreign-key name written into its
// canonical object metadata. It invokes the same planner functions that
// render target DDL, so retained-object comparison cannot drift from the
// naming algorithm used during drop/recreate.
func MaterializeMySQLObjectNames(tables []Table) ([]Table, error) {
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
			mysqlObjectTableKey(
				table.source.Schema,
				table.source.Name,
			),
			"PRIMARY",
		)
	}

	materialized := cloneMySQLMaterializedTables(tables)
	for _, spec := range indexes {
		statement, err := planMySQLIndex(spec, indexNames)
		if err != nil {
			return nil, err
		}
		if err := materializeMySQLIndexName(
			materialized,
			spec,
			statement.Name,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range checks {
		statement, err := planMySQLCheck(spec, constraintNames)
		if err != nil {
			return nil, err
		}
		if err := materializeMySQLCheckName(
			materialized,
			spec,
			statement.Name,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range foreignKeys {
		statement, err := planMySQLForeignKey(
			spec,
			constraintNames,
		)
		if err != nil {
			return nil, err
		}
		if err := materializeMySQLForeignKeyName(
			materialized,
			spec,
			statement.Name,
		); err != nil {
			return nil, err
		}
	}
	if _, err := PlanMySQLDropRecreateObjects(materialized); err != nil {
		return nil, err
	}
	return materialized, nil
}

func materializeMySQLIndexName(
	tables []Table,
	spec mysqlObjectSpec,
	name string,
) error {
	table, err := mySQLMaterializedTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Indexes {
		if postgresIndexSortKey(table.Indexes[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return mysqlObjectPolicy(
				"materialize MySQL object names",
				"ambiguous index on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return mysqlObjectPolicy(
			"materialize MySQL object names",
			"missing index on "+table.Name,
		)
	}
	table.Indexes[matched].Name = name
	return nil
}

func materializeMySQLCheckName(
	tables []Table,
	spec mysqlObjectSpec,
	name string,
) error {
	table, err := mySQLMaterializedTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Checks {
		if postgresCheckSortKey(table.Checks[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return mysqlObjectPolicy(
				"materialize MySQL object names",
				"ambiguous CHECK on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return mysqlObjectPolicy(
			"materialize MySQL object names",
			"missing CHECK on "+table.Name,
		)
	}
	table.Checks[matched].Name = name
	return nil
}

func materializeMySQLForeignKeyName(
	tables []Table,
	spec mysqlObjectSpec,
	name string,
) error {
	table, err := mySQLMaterializedTable(tables, spec)
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
			return mysqlObjectPolicy(
				"materialize MySQL object names",
				"ambiguous foreign key on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return mysqlObjectPolicy(
			"materialize MySQL object names",
			"missing foreign key on "+table.Name,
		)
	}
	table.ForeignKeys[matched].Name = name
	return nil
}

func mySQLMaterializedTable(
	tables []Table,
	spec mysqlObjectSpec,
) (*Table, error) {
	for index := range tables {
		if tables[index].Schema == spec.table.source.Schema &&
			tables[index].Name == spec.table.source.Name {
			return &tables[index], nil
		}
	}
	return nil, mysqlObjectPolicy(
		"materialize MySQL object names",
		"missing table "+spec.table.source.Schema+"."+
			spec.table.source.Name,
	)
}

func cloneMySQLMaterializedTables(source []Table) []Table {
	cloned := make([]Table, len(source))
	for tableIndex := range source {
		table := source[tableIndex]
		cloned[tableIndex] = table
		cloned[tableIndex].ClickHouseOrderBy = append(
			[]string(nil),
			table.ClickHouseOrderBy...,
		)
		if table.Identity != nil {
			identity := *table.Identity
			if table.Identity.Frontier != nil {
				frontier := *table.Identity.Frontier
				identity.Frontier = &frontier
			}
			cloned[tableIndex].Identity = &identity
		}
		cloned[tableIndex].Columns = append(
			[]Column(nil),
			table.Columns...,
		)
		for columnIndex := range table.Columns {
			column := table.Columns[columnIndex]
			if column.DeclaredType != nil {
				declaration := *column.DeclaredType
				declaration.Arguments = append(
					[]int(nil),
					column.DeclaredType.Arguments...,
				)
				cloned[tableIndex].Columns[columnIndex].
					DeclaredType = &declaration
			}
			if column.Default != nil {
				expression := *column.Default
				cloned[tableIndex].Columns[columnIndex].
					Default = &expression
			}
		}
		if table.Indexes != nil {
			cloned[tableIndex].Indexes = make(
				[]Index,
				len(table.Indexes),
			)
			for index := range table.Indexes {
				cloned[tableIndex].Indexes[index] =
					table.Indexes[index]
				cloned[tableIndex].Indexes[index].Columns = append(
					[]IndexColumn(nil),
					table.Indexes[index].Columns...,
				)
			}
		}
		cloned[tableIndex].Checks = append(
			[]CheckConstraint(nil),
			table.Checks...,
		)
		if table.ForeignKeys != nil {
			cloned[tableIndex].ForeignKeys = make(
				[]ForeignKey,
				len(table.ForeignKeys),
			)
			for index := range table.ForeignKeys {
				cloned[tableIndex].ForeignKeys[index] =
					table.ForeignKeys[index]
				cloned[tableIndex].ForeignKeys[index].Columns = append(
					[]string(nil),
					table.ForeignKeys[index].Columns...,
				)
				cloned[tableIndex].ForeignKeys[index].
					ReferencedColumns = append(
					[]string(nil),
					table.ForeignKeys[index].
						ReferencedColumns...,
				)
			}
		}
	}
	return cloned
}
