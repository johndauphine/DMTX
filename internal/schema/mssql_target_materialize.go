package schema

// MaterializeSQLServerObjectNames returns a deep copy of tables with every
// deterministic SQL Server index, CHECK, and foreign-key name written into its
// canonical object metadata. It uses the same planner specs that render DDL, so
// target preflight and retained-object comparison never duplicate the naming
// algorithm or parse rendered SQL.
func MaterializeSQLServerObjectNames(
	tables []Table,
) ([]Table, error) {
	if _, err := PlanSQLServerDropRecreateObjects(tables); err != nil {
		return nil, err
	}
	planned, err := planSQLServerTargetTables(tables)
	if err != nil {
		return nil, err
	}
	indexes, checks, foreignKeys, err :=
		collectSQLServerObjectSpecs(planned)
	if err != nil {
		return nil, err
	}

	materialized := cloneSQLServerMaterializedTables(tables)
	for _, spec := range indexes {
		if err := materializeSQLServerIndexName(
			materialized,
			spec,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range checks {
		if err := materializeSQLServerCheckName(
			materialized,
			spec,
		); err != nil {
			return nil, err
		}
	}
	for _, spec := range foreignKeys {
		if err := materializeSQLServerForeignKeyName(
			materialized,
			spec,
		); err != nil {
			return nil, err
		}
	}
	if _, err := PlanSQLServerDropRecreateObjects(materialized); err != nil {
		return nil, err
	}
	return materialized, nil
}

func materializeSQLServerIndexName(
	tables []Table,
	spec sqlServerObjectSpec,
) error {
	table, err := sqlServerMaterializedTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Indexes {
		if sqlServerIndexSortKey(table.Indexes[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return sqlServerTargetPolicy(
				"materialize SQL Server object names",
				"ambiguous index on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return sqlServerTargetPolicy(
			"materialize SQL Server object names",
			"missing index on "+table.Name,
		)
	}
	table.Indexes[matched].Name = spec.name
	return nil
}

func materializeSQLServerCheckName(
	tables []Table,
	spec sqlServerObjectSpec,
) error {
	table, err := sqlServerMaterializedTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.Checks {
		if sqlServerCheckSortKey(table.Checks[index]) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return sqlServerTargetPolicy(
				"materialize SQL Server object names",
				"ambiguous CHECK on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return sqlServerTargetPolicy(
			"materialize SQL Server object names",
			"missing CHECK on "+table.Name,
		)
	}
	table.Checks[matched].Name = spec.name
	return nil
}

func materializeSQLServerForeignKeyName(
	tables []Table,
	spec sqlServerObjectSpec,
) error {
	table, err := sqlServerMaterializedTable(tables, spec)
	if err != nil {
		return err
	}
	matched := -1
	for index := range table.ForeignKeys {
		if sqlServerForeignKeySortKey(
			table.ForeignKeys[index],
		) != spec.sortKey {
			continue
		}
		if matched >= 0 {
			return sqlServerTargetPolicy(
				"materialize SQL Server object names",
				"ambiguous foreign key on "+table.Name,
			)
		}
		matched = index
	}
	if matched < 0 {
		return sqlServerTargetPolicy(
			"materialize SQL Server object names",
			"missing foreign key on "+table.Name,
		)
	}
	table.ForeignKeys[matched].Name = spec.name
	return nil
}

func sqlServerMaterializedTable(
	tables []Table,
	spec sqlServerObjectSpec,
) (*Table, error) {
	for index := range tables {
		if tables[index].Schema == spec.table.source.Schema &&
			tables[index].Name == spec.table.source.Name {
			return &tables[index], nil
		}
	}
	return nil, sqlServerTargetPolicy(
		"materialize SQL Server object names",
		"missing table "+spec.table.source.Schema+"."+
			spec.table.source.Name,
	)
}

func cloneSQLServerMaterializedTables(source []Table) []Table {
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
