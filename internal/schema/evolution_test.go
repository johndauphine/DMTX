package schema

import (
	"reflect"
	"strings"
	"testing"
)

func TestColumnEvolutionRendersExactDialectDDL(t *testing.T) {
	t.Parallel()

	defaultValue := mustEvolutionDefault(t, `'O''Reilly'`)
	added := evolutionFixtureColumn(40, true, defaultValue)
	addTable := evolutionFixtureTable(added)
	addition, err := PlanAddNullableColumn(
		evolutionSnapshotBeforeColumn(addTable, added.Name),
		addTable,
		added,
	)
	if err != nil {
		t.Fatal(err)
	}
	if addition.Kind() != AddNullableColumnEvolution {
		t.Fatalf("addition kind = %d", addition.Kind())
	}

	required := evolutionFixtureColumn(40, false, defaultValue)
	nullable := evolutionFixtureColumn(40, true, defaultValue)
	relaxTable := evolutionFixtureTable(nullable)
	relaxation, err := PlanRelaxNullability(
		relaxTable,
		required,
		nullable,
	)
	if err != nil {
		t.Fatal(err)
	}
	if relaxation.Kind() != RelaxNullabilityEvolution {
		t.Fatalf("relaxation kind = %d", relaxation.Kind())
	}

	narrower := evolutionFixtureColumn(20, false, defaultValue)
	wider := evolutionFixtureColumn(40, false, defaultValue)
	widenTable := evolutionFixtureTable(wider)
	widenCatalog := mustCompleteEvolutionCatalog(t, widenTable)
	widening, err := PlanSafeTypeWidening(
		widenCatalog,
		widenTable,
		narrower,
		wider,
	)
	if err != nil {
		t.Fatal(err)
	}
	if widening.Kind() != WidenTypeEvolution {
		t.Fatalf("widening kind = %d", widening.Kind())
	}

	tests := []struct {
		name   string
		target Dialect
		add    string
		relax  string
		widen  string
	}{
		{
			name:   "PostgreSQL",
			target: Postgres,
			add: "ALTER TABLE \"tenant\"\"].`west\"." +
				"\"orders\"\"].`archive\" ADD COLUMN " +
				"\"note\"\"].`value\" VARCHAR(40) NULL " +
				"DEFAULT E'O''Reilly';",
			relax: "ALTER TABLE \"tenant\"\"].`west\"." +
				"\"orders\"\"].`archive\" ALTER COLUMN " +
				"\"note\"\"].`value\" DROP NOT NULL;",
			widen: "ALTER TABLE \"tenant\"\"].`west\"." +
				"\"orders\"\"].`archive\" ALTER COLUMN " +
				"\"note\"\"].`value\" TYPE VARCHAR(40);",
		},
		{
			name:   "SQL Server",
			target: SQLServer,
			add: "ALTER TABLE [tenant\"]].`west].[orders\"]].`archive] " +
				"ADD [note\"]].`value] VARCHAR(40) COLLATE " +
				"Latin1_General_100_BIN2_UTF8 NULL " +
				"DEFAULT N'O''Reilly';",
			relax: "ALTER TABLE [tenant\"]].`west].[orders\"]].`archive] " +
				"ALTER COLUMN [note\"]].`value] VARCHAR(40) COLLATE " +
				"Latin1_General_100_BIN2_UTF8 NULL;",
			widen: "ALTER TABLE [tenant\"]].`west].[orders\"]].`archive] " +
				"ALTER COLUMN [note\"]].`value] VARCHAR(40) COLLATE " +
				"Latin1_General_100_BIN2_UTF8 NOT NULL;",
		},
		{
			name:   "MySQL and MariaDB",
			target: MySQL,
			add: "ALTER TABLE `tenant\"].``west`.`orders\"].``archive` " +
				"ADD COLUMN `note\"].``value` VARCHAR(40) NULL " +
				"DEFAULT 'O''Reilly';",
			relax: "ALTER TABLE `tenant\"].``west`.`orders\"].``archive` " +
				"MODIFY COLUMN `note\"].``value` VARCHAR(40) NULL " +
				"DEFAULT 'O''Reilly';",
			widen: "ALTER TABLE `tenant\"].``west`.`orders\"].``archive` " +
				"MODIFY COLUMN `note\"].``value` VARCHAR(40) NOT NULL " +
				"DEFAULT 'O''Reilly';",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, operation := range []struct {
				name  string
				proof ColumnEvolution
				want  string
			}{
				{name: "add", proof: addition, want: test.add},
				{name: "relax", proof: relaxation, want: test.relax},
				{name: "widen", proof: widening, want: test.widen},
			} {
				operation := operation
				t.Run(operation.name, func(t *testing.T) {
					t.Parallel()
					for iteration := 0; iteration < 5; iteration++ {
						got, err := RenderColumnEvolution(
							test.target,
							operation.proof,
						)
						if err != nil {
							t.Fatal(err)
						}
						if got != operation.want {
							t.Fatalf(
								"statement = %q, want %q",
								got,
								operation.want,
							)
						}
					}
				})
			}
		})
	}
}

func TestColumnEvolutionProofDoesNotAliasInputs(t *testing.T) {
	t.Parallel()

	defaultValue := mustEvolutionDefault(t, `'stable'`)
	previous := evolutionFixtureColumn(20, false, defaultValue)
	current := evolutionFixtureColumn(40, false, defaultValue)
	table := evolutionFixtureTable(current)
	catalog := mustCompleteEvolutionCatalog(t, table)
	operation, err := PlanSafeTypeWidening(
		catalog,
		table,
		previous,
		current,
	)
	if err != nil {
		t.Fatal(err)
	}

	table.Schema = "mutated"
	table.Name = "mutated"
	table.Columns[1].Name = "mutated"
	table.Columns[1].DeclaredType.Arguments[0] = 1
	current.Name = "mutated"
	current.DeclaredType.Arguments[0] = 1
	previous.Name = "mutated"
	previous.DeclaredType.Arguments[0] = 1

	got, err := RenderColumnEvolution(Postgres, operation)
	if err != nil {
		t.Fatal(err)
	}
	want := `ALTER TABLE "tenant""].` +
		"`west" + `"."orders""].` +
		"`archive" + `" ALTER COLUMN "note""].` +
		"`value" + `" TYPE VARCHAR(40);`
	if got != want {
		t.Fatalf("statement after input mutation = %q, want %q", got, want)
	}
}

func TestPlanAddNullableColumnAdmission(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"NULL",
		"TRUE",
		"-12.5",
		`'literal'`,
		`X'00ff'`,
	} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			column := evolutionFixtureColumn(
				40,
				true,
				mustEvolutionDefault(t, value),
			)
			if _, err := PlanAddNullableColumn(
				evolutionSnapshotBeforeColumn(
					evolutionFixtureTable(column),
					column.Name,
				),
				evolutionFixtureTable(column),
				column,
			); err != nil {
				t.Fatalf("literal %s was rejected: %v", value, err)
			}
		})
	}

	withoutDefault := evolutionFixtureColumn(40, true, nil)
	withoutDefaultTable := evolutionFixtureTable(withoutDefault)
	operation, err := PlanAddNullableColumn(
		evolutionSnapshotBeforeColumn(
			withoutDefaultTable,
			withoutDefault.Name,
		),
		withoutDefaultTable,
		withoutDefault,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RenderColumnEvolution(MySQL, operation)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, " DEFAULT ") {
		t.Fatalf("absent default was invented: %q", got)
	}
}

func TestPlanAddNullableColumnRejectsUnsafeShapes(t *testing.T) {
	t.Parallel()

	volatileDefault := mustEvolutionDefault(t, "CURRENT_TIMESTAMP")
	base := evolutionFixtureColumn(40, true, nil)
	tests := []struct {
		name   string
		table  Table
		column Column
	}{
		{
			name:   "empty table",
			table:  Table{Columns: []Column{base}},
			column: base,
		},
		{
			name: "empty column",
			table: evolutionFixtureTable(Column{
				Type: "text", Nullable: true,
			}),
			column: Column{Type: "text", Nullable: true},
		},
		{
			name:   "missing current column",
			table:  evolutionFixtureTable(base),
			column: evolutionRenamedColumn(base, "other"),
		},
		{
			name: "duplicate current column",
			table: func() Table {
				table := evolutionFixtureTable(base)
				table.Columns = append(table.Columns, cloneEvolutionColumn(base))
				return table
			}(),
			column: base,
		},
		{
			name: "table evidence mismatch",
			table: func() Table {
				table := evolutionFixtureTable(base)
				table.Columns[len(table.Columns)-1].Nullable = false
				return table
			}(),
			column: base,
		},
		{
			name: "not nullable",
			table: func() Table {
				column := base
				column.Nullable = false
				return evolutionFixtureTable(column)
			}(),
			column: func() Column {
				column := base
				column.Nullable = false
				return column
			}(),
		},
		{
			name: "primary key",
			table: func() Table {
				column := base
				column.PrimaryKey = true
				column.PrimaryKeyPosition = 1
				return evolutionFixtureTable(column)
			}(),
			column: func() Column {
				column := base
				column.PrimaryKey = true
				column.PrimaryKeyPosition = 1
				return column
			}(),
		},
		{
			name: "primary key position without membership",
			table: func() Table {
				column := base
				column.PrimaryKeyPosition = 1
				return evolutionFixtureTable(column)
			}(),
			column: func() Column {
				column := base
				column.PrimaryKeyPosition = 1
				return column
			}(),
		},
		{
			name: "identity",
			table: func() Table {
				table := evolutionFixtureTable(base)
				table.Identity = &Identity{
					Column:     base.Name,
					Generation: IdentityByDefault,
				}
				return table
			}(),
			column: base,
		},
		{
			name: "malformed identity evidence",
			table: func() Table {
				table := evolutionFixtureTable(base)
				table.Identity = &Identity{Column: "missing"}
				return table
			}(),
			column: base,
		},
		{
			name: "volatile default",
			table: func() Table {
				column := base
				column.Default = volatileDefault
				return evolutionFixtureTable(column)
			}(),
			column: func() Column {
				column := base
				column.Default = volatileDefault
				return column
			}(),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := PlanAddNullableColumn(
				evolutionSnapshotBeforeColumn(
					test.table,
					test.column.Name,
				),
				test.table,
				test.column,
			); err == nil {
				t.Fatal("unsafe add-column shape was admitted")
			}
		})
	}
}

func TestPlanAddNullableColumnRequiresExactPriorAbsenceEvidence(
	t *testing.T,
) {
	t.Parallel()

	column := evolutionFixtureColumn(40, true, nil)
	current := evolutionFixtureTable(column)
	validPrevious := evolutionSnapshotBeforeColumn(current, column.Name)
	for _, test := range []struct {
		name     string
		previous SnapshotTable
	}{
		{
			name: "column already existed",
			previous: func() SnapshotTable {
				value := validPrevious
				value.Columns = append(
					value.Columns,
					SnapshotColumn{Name: column.Name, Type: column.Type},
				)
				return value
			}(),
		},
		{
			name: "schema identity changed",
			previous: func() SnapshotTable {
				value := validPrevious
				value.Schema = "other"
				return value
			}(),
		},
		{
			name: "table identity changed",
			previous: func() SnapshotTable {
				value := validPrevious
				value.Name = "other"
				return value
			}(),
		},
		{
			name: "prior identity is malformed",
			previous: func() SnapshotTable {
				value := validPrevious
				value.Identity = &SnapshotIdentity{
					Column:     "missing",
					Generation: IdentityByDefault,
				}
				return value
			}(),
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := PlanAddNullableColumn(
				test.previous,
				current,
				column,
			); err == nil {
				t.Fatal("invalid prior-absence evidence was admitted")
			}
		})
	}
}

func TestRenderAddNullableColumnRejectsCaseAliasedPriorIdentifiers(
	t *testing.T,
) {
	t.Parallel()

	column := Column{
		Name: "value", Type: "text", Nullable: true,
		DeclaredType: &DeclaredType{
			Base: "varchar", Arguments: []int{40},
		},
	}
	table := Table{
		Schema:  "target",
		Name:    "items",
		Columns: []Column{column},
	}
	previous := SnapshotTable{
		Schema: table.Schema,
		Name:   table.Name,
		Columns: []SnapshotColumn{{
			Name: "VALUE", Type: "text", Nullable: true,
			DeclaredType: &SnapshotDeclaredType{
				Base: "varchar", Arguments: []int{20},
			},
		}},
	}
	operation, err := PlanAddNullableColumn(previous, table, column)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []Dialect{SQLServer, MySQL} {
		if _, err := RenderColumnEvolution(
			target,
			operation,
		); err == nil || !strings.Contains(err.Error(), "prior-table") {
			t.Fatalf(
				"case-aliased prior identifier for %s error = %v",
				target,
				err,
			)
		}
	}
}

func TestPlanRelaxNullabilityRejectsUnsafeShapes(t *testing.T) {
	t.Parallel()

	required := evolutionFixtureColumn(40, false, nil)
	nullable := evolutionFixtureColumn(40, true, nil)
	changedDefault := mustEvolutionDefault(t, `'changed'`)
	tests := []struct {
		name     string
		previous Column
		current  Column
		table    func(Column) Table
	}{
		{
			name:     "already nullable",
			previous: nullable,
			current:  nullable,
		},
		{
			name:     "tightening",
			previous: nullable,
			current:  required,
		},
		{
			name:     "different name",
			previous: evolutionRenamedColumn(required, "prior"),
			current:  nullable,
		},
		{
			name:     "coupled type",
			previous: evolutionFixtureColumn(20, false, nil),
			current:  nullable,
		},
		{
			name:     "coupled default",
			previous: required,
			current: func() Column {
				column := nullable
				column.Default = changedDefault
				return column
			}(),
		},
		{
			name: "primary key",
			previous: func() Column {
				column := required
				column.PrimaryKey = true
				column.PrimaryKeyPosition = 1
				return column
			}(),
			current: func() Column {
				column := nullable
				column.PrimaryKey = true
				column.PrimaryKeyPosition = 1
				return column
			}(),
		},
		{
			name:     "identity",
			previous: required,
			current:  nullable,
			table: func(column Column) Table {
				table := evolutionFixtureTable(column)
				table.Identity = &Identity{
					Column:     column.Name,
					Generation: IdentityByDefault,
				}
				return table
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := evolutionFixtureTable(test.current)
			if test.table != nil {
				table = test.table(test.current)
			}
			if _, err := PlanRelaxNullability(
				table,
				test.previous,
				test.current,
			); err == nil {
				t.Fatal("unsafe nullability change was admitted")
			}
		})
	}
}

func TestPlanSafeTypeWideningRejectsDependentObjects(t *testing.T) {
	t.Parallel()

	previous := Column{
		Name: "value", Type: "varchar",
		DeclaredType: &DeclaredType{
			Base: "varchar", Arguments: []int{20},
		},
	}
	current := cloneEvolutionColumn(previous)
	current.DeclaredType.Arguments[0] = 40
	baseTable := Table{
		Schema: "target",
		Name:   "items",
		Columns: []Column{
			{Name: "id", Type: "bigint"},
			current,
		},
	}
	checkExpression, err := ParseSQLiteCheckExpression(`"id" > 0`)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		target func() Table
		others func() []Table
	}{
		{
			name: "secondary index",
			target: func() Table {
				table := cloneEvolutionTable(baseTable)
				table.Indexes = []Index{{
					Name: "value_idx",
					Columns: []IndexColumn{{
						Name: current.Name,
					}},
				}}
				return table
			},
		},
		{
			name: "outgoing foreign key",
			target: func() Table {
				table := cloneEvolutionTable(baseTable)
				table.ForeignKeys = []ForeignKey{{
					Name:              "value_fk",
					Columns:           []string{current.Name},
					ReferencedTable:   "parents",
					ReferencedColumns: []string{"id"},
				}}
				return table
			},
			others: func() []Table {
				return []Table{{
					Schema: "target",
					Name:   "parents",
					Columns: []Column{{
						Name: "id", Type: "varchar",
						PrimaryKey: true, PrimaryKeyPosition: 1,
					}},
				}}
			},
		},
		{
			name: "incoming foreign key",
			target: func() Table {
				table := cloneEvolutionTable(baseTable)
				table.Indexes = []Index{{
					Name: "value_key", Unique: true,
					Columns: []IndexColumn{{Name: current.Name}},
				}}
				return table
			},
			others: func() []Table {
				return []Table{{
					Schema: "target",
					Name:   "children",
					Columns: []Column{
						{Name: "id", Type: "bigint"},
						{Name: "item_value", Type: "varchar"},
					},
					ForeignKeys: []ForeignKey{{
						Name:              "item_value_fk",
						Columns:           []string{"item_value"},
						ReferencedTable:   baseTable.Name,
						ReferencedColumns: []string{current.Name},
					}},
				}}
			},
		},
		{
			name: "unprovable CHECK dependency",
			target: func() Table {
				table := cloneEvolutionTable(baseTable)
				table.Checks = []CheckConstraint{{
					Name:       "id_positive",
					Expression: checkExpression,
				}}
				return table
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := test.target()
			tables := []Table{table}
			if test.others != nil {
				tables = append(tables, test.others()...)
			}
			catalog := mustCompleteEvolutionCatalog(t, tables...)
			if _, err := PlanSafeTypeWidening(
				catalog,
				table,
				previous,
				current,
			); err == nil {
				t.Fatal("dependent standalone type ALTER was admitted")
			}
		})
	}
}

func TestPlanSafeTypeWideningRequiresMatchingCompleteCatalog(t *testing.T) {
	t.Parallel()

	previous := Column{Name: "value", Type: "integer"}
	current := Column{Name: "value", Type: "bigint"}
	table := Table{
		Schema:  "target",
		Name:    "items",
		Columns: []Column{current},
	}
	other := Table{
		Schema:  "target",
		Name:    "other",
		Columns: []Column{{Name: "id", Type: "bigint"}},
	}
	mismatched := cloneEvolutionTable(table)
	mismatched.Columns[0].Nullable = true
	for _, test := range []struct {
		name    string
		catalog CompleteEvolutionCatalog
	}{
		{
			name:    "target missing",
			catalog: mustCompleteEvolutionCatalog(t, other),
		},
		{
			name:    "target evidence differs",
			catalog: mustCompleteEvolutionCatalog(t, mismatched),
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := PlanSafeTypeWidening(
				test.catalog,
				table,
				previous,
				current,
			); err == nil {
				t.Fatal("incomplete or mismatched catalog was admitted")
			}
		})
	}
}

func TestCompleteEvolutionCatalogAcceptsCompleteUnnamedRelationEvidence(
	t *testing.T,
) {
	t.Parallel()

	tables := completeEvolutionRelationFixture(t)
	tables = append(tables, Table{
		Schema: "target",
		Name:   "counters",
		Columns: []Column{{
			Name: "id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		}},
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
		},
	})
	if _, err := NewCompleteEvolutionCatalog(tables); err != nil {
		t.Fatalf("complete relation evidence was rejected: %v", err)
	}
}

func TestCompleteEvolutionCatalogDoesNotAliasRelationEvidence(t *testing.T) {
	t.Parallel()

	tables := completeEvolutionRelationFixture(t)
	current := Column{
		Name: "label", Type: "varchar", Nullable: true,
		DeclaredType: &DeclaredType{
			Base: "varchar", Arguments: []int{40},
		},
	}
	previous := cloneEvolutionColumn(current)
	previous.DeclaredType.Arguments[0] = 20
	tables[0].Columns[1] = current
	table := cloneEvolutionTable(tables[0])
	catalog := mustCompleteEvolutionCatalog(t, tables...)

	tables[0].Indexes[0].Columns[0].Name = current.Name
	tables[1].ForeignKeys[0].ReferencedColumns[0] = current.Name
	if _, err := PlanSafeTypeWidening(
		catalog,
		table,
		previous,
		current,
	); err != nil {
		t.Fatalf("caller mutation changed catalog proof: %v", err)
	}
}

func TestCompleteEvolutionCatalogRejectsIncompleteRelationEvidence(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func([]Table) []Table
	}{
		{
			name: "table without columns",
			mutate: func(tables []Table) []Table {
				tables[0].Columns = nil
				return tables
			},
		},
		{
			name: "column without type",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].Type = ""
				return tables
			},
		},
		{
			name: "case-aliased table in same schema",
			mutate: func(tables []Table) []Table {
				aliased := cloneEvolutionTable(tables[0])
				aliased.Name = "PARENTS"
				return append(tables, aliased)
			},
		},
		{
			name: "case-aliased schema and table",
			mutate: func(tables []Table) []Table {
				aliased := cloneEvolutionTable(tables[0])
				aliased.Schema = "TARGET"
				aliased.Name = "PARENTS"
				return append(tables, aliased)
			},
		},
		{
			name: "case-aliased column",
			mutate: func(tables []Table) []Table {
				tables[0].Columns = append(
					tables[0].Columns,
					Column{Name: "ID", Type: "bigint"},
				)
				return tables
			},
		},
		{
			name: "primary-key flag without position",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].PrimaryKeyPosition = 0
				return tables
			},
		},
		{
			name: "primary-key position without flag",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].PrimaryKey = false
				return tables
			},
		},
		{
			name: "non-contiguous primary-key position",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].PrimaryKeyPosition = 2
				return tables
			},
		},
		{
			name: "nullable primary-key member",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].Nullable = true
				return tables
			},
		},
		{
			name: "identity is not sole primary key",
			mutate: func(tables []Table) []Table {
				tables[0].Identity = &Identity{
					Column:     "label",
					Generation: IdentityByDefault,
				}
				return tables
			},
		},
		{
			name: "negative identity frontier",
			mutate: func(tables []Table) []Table {
				frontier := int64(-1)
				tables[0].Identity = &Identity{
					Column:     "id",
					Generation: IdentityByDefault,
					Frontier:   &frontier,
				}
				return tables
			},
		},
		{
			name: "identity has non-bigint type",
			mutate: func(tables []Table) []Table {
				tables[0].Columns[0].Type = "integer"
				tables[0].Identity = &Identity{
					Column:     "id",
					Generation: IdentityByDefault,
				}
				return tables
			},
		},
		{
			name: "empty index members",
			mutate: func(tables []Table) []Table {
				tables[0].Indexes[0].Columns = nil
				return tables
			},
		},
		{
			name: "unknown index member",
			mutate: func(tables []Table) []Table {
				tables[0].Indexes[0].Columns[0].Name = "missing"
				return tables
			},
		},
		{
			name: "duplicate index member",
			mutate: func(tables []Table) []Table {
				tables[0].Indexes[0].Columns = append(
					tables[0].Indexes[0].Columns,
					IndexColumn{Name: "id"},
				)
				return tables
			},
		},
		{
			name: "case-aliased index names",
			mutate: func(tables []Table) []Table {
				tables[0].Indexes[0].Name = "parent_key"
				duplicate := tables[0].Indexes[0]
				duplicate.Name = "PARENT_KEY"
				tables[0].Indexes = append(
					tables[0].Indexes,
					duplicate,
				)
				return tables
			},
		},
		{
			name: "non-unique inline index",
			mutate: func(tables []Table) []Table {
				tables[0].Indexes[0].Inline = true
				tables[0].Indexes[0].Unique = false
				return tables
			},
		},
		{
			name: "invalid check expression",
			mutate: func(tables []Table) []Table {
				tables[1].Checks[0].Expression = Expression{
					kind: expressionCheck,
				}
				return tables
			},
		},
		{
			name: "case-aliased check names",
			mutate: func(tables []Table) []Table {
				tables[1].Checks[0].Name = "parent_positive"
				duplicate := tables[1].Checks[0]
				duplicate.Name = "PARENT_POSITIVE"
				tables[1].Checks = append(
					tables[1].Checks,
					duplicate,
				)
				return tables
			},
		},
		{
			name: "empty foreign-key owner members",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Columns = nil
				tables[1].ForeignKeys[0].ReferencedColumns = nil
				return tables
			},
		},
		{
			name: "unknown foreign-key owner member",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Columns[0] = "missing"
				return tables
			},
		},
		{
			name: "case-aliased foreign-key owner member",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Columns[0] = "PARENT_ID"
				return tables
			},
		},
		{
			name: "empty referenced table",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedTable = ""
				return tables
			},
		},
		{
			name: "missing referenced table",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedTable = "missing"
				return tables
			},
		},
		{
			name: "case-aliased referenced table",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedTable = "PARENTS"
				return tables
			},
		},
		{
			name: "empty referenced members",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedColumns = nil
				return tables
			},
		},
		{
			name: "mismatched foreign-key member counts",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedColumns =
					[]string{"id", "label"}
				return tables
			},
		},
		{
			name: "unknown referenced member",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedColumns[0] =
					"missing"
				return tables
			},
		},
		{
			name: "case-aliased referenced member",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedColumns[0] = "ID"
				return tables
			},
		},
		{
			name: "duplicate foreign-key owner member",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Columns =
					[]string{"parent_id", "parent_id"}
				tables[1].ForeignKeys[0].ReferencedColumns =
					[]string{"id", "id"}
				return tables
			},
		},
		{
			name: "duplicate foreign-key referenced member",
			mutate: func(tables []Table) []Table {
				tables[1].Columns = append(
					tables[1].Columns,
					Column{Name: "other_parent_id", Type: "bigint"},
				)
				tables[1].ForeignKeys[0].Columns =
					[]string{"parent_id", "other_parent_id"}
				tables[1].ForeignKeys[0].ReferencedColumns =
					[]string{"id", "id"}
				return tables
			},
		},
		{
			name: "referenced members are not unique",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].ReferencedColumns[0] =
					"label"
				return tables
			},
		},
		{
			name: "invalid foreign-key action",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].OnDelete = "EXECUTE"
				return tables
			},
		},
		{
			name: "invalid foreign-key match",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Match = "FUZZY"
				return tables
			},
		},
		{
			name: "case-aliased foreign-key names",
			mutate: func(tables []Table) []Table {
				tables[1].ForeignKeys[0].Name = "parent_fk"
				duplicate := tables[1].ForeignKeys[0]
				duplicate.Name = "PARENT_FK"
				tables[1].ForeignKeys = append(
					tables[1].ForeignKeys,
					duplicate,
				)
				return tables
			},
		},
		{
			name: "unqualified cross-schema reference",
			mutate: func(tables []Table) []Table {
				tables[0].Schema = "other"
				return tables
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tables := test.mutate(completeEvolutionRelationFixture(t))
			if _, err := NewCompleteEvolutionCatalog(tables); err == nil {
				t.Fatal("incomplete relation evidence was admitted")
			}
		})
	}
}

func TestPlanSafeTypeWideningResolvesForeignKeysInOwnerSchema(
	t *testing.T,
) {
	t.Parallel()

	tables := completeEvolutionRelationFixture(t)
	previous := Column{
		Name: "label", Type: "varchar", Nullable: true,
		DeclaredType: &DeclaredType{
			Base: "varchar", Arguments: []int{20},
		},
	}
	current := cloneEvolutionColumn(previous)
	current.DeclaredType.Arguments[0] = 40
	otherSchemaTable := cloneEvolutionTable(tables[0])
	otherSchemaTable.Schema = "other"
	otherSchemaTable.Columns[1] = current
	tables = append(tables, otherSchemaTable)

	catalog := mustCompleteEvolutionCatalog(t, tables...)
	if _, err := PlanSafeTypeWidening(
		catalog,
		otherSchemaTable,
		previous,
		current,
	); err != nil {
		t.Fatalf(
			"same-named table in another schema caused a false FK dependency: %v",
			err,
		)
	}
}

func TestPlanSafeTypeWideningAllowsProvenUnrelatedObjects(t *testing.T) {
	t.Parallel()

	previous := Column{
		Name: "value", Type: "varchar",
		DeclaredType: &DeclaredType{
			Base: "varchar", Arguments: []int{20},
		},
	}
	current := cloneEvolutionColumn(previous)
	current.DeclaredType.Arguments[0] = 40
	table := Table{
		Schema: "target",
		Name:   "items",
		Columns: []Column{
			{Name: "id", Type: "bigint"},
			current,
		},
		Indexes: []Index{{
			Name:    "id_idx",
			Unique:  true,
			Columns: []IndexColumn{{Name: "id"}},
		}},
		ForeignKeys: []ForeignKey{{
			Name:              "id_fk",
			Columns:           []string{"id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
		}},
	}
	other := Table{
		Schema: "target",
		Name:   "children",
		Columns: []Column{
			{Name: "item_id", Type: "bigint"},
		},
		ForeignKeys: []ForeignKey{{
			Name:              "item_id_fk",
			Columns:           []string{"item_id"},
			ReferencedTable:   table.Name,
			ReferencedColumns: []string{"id"},
		}},
	}
	parent := Table{
		Schema: "target",
		Name:   "parents",
		Columns: []Column{{
			Name: "id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		}},
	}
	catalog := mustCompleteEvolutionCatalog(t, table, other, parent)
	if _, err := PlanSafeTypeWidening(
		catalog,
		table,
		previous,
		current,
	); err != nil {
		t.Fatalf("unrelated dependent objects blocked widening: %v", err)
	}
}

func TestPlanSafeTypeWideningAdmissionMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous Column
		current  Column
	}{
		{
			name:     "integer to bigint",
			previous: Column{Name: "value", Type: "integer"},
			current:  Column{Name: "value", Type: "bigint"},
		},
		{
			name: "smallint to int",
			previous: Column{
				Name: "value", Type: "integer",
				DeclaredType: &DeclaredType{Base: "smallint"},
			},
			current: Column{
				Name: "value", Type: "integer",
				DeclaredType: &DeclaredType{Base: "int"},
			},
		},
		{
			name: "varchar length",
			previous: Column{
				Name: "value", Type: "varchar",
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: Column{
				Name: "value", Type: "varchar",
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "numeric capacity",
			previous: Column{
				Name: "value", Type: "numeric",
				DeclaredType: &DeclaredType{
					Base: "decimal", Arguments: []int{12, 2},
				},
			},
			current: Column{
				Name: "value", Type: "numeric",
				DeclaredType: &DeclaredType{
					Base: "numeric", Arguments: []int{16, 4},
				},
			},
		},
		{
			name: "temporal precision",
			previous: Column{
				Name: "value", Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base: "timestamp", Arguments: []int{3},
				},
			},
			current: Column{
				Name: "value", Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
		},
		{
			name:     "real to double",
			previous: Column{Name: "value", Type: "real"},
			current:  Column{Name: "value", Type: "double"},
		},
		{
			name: "MySQL text capacity",
			previous: Column{
				Name: "value", Type: "text",
				DeclaredType: &DeclaredType{Base: "text"},
			},
			current: Column{
				Name: "value", Type: "text",
				DeclaredType: &DeclaredType{Base: "longtext"},
			},
		},
		{
			name: "MySQL blob capacity",
			previous: Column{
				Name: "value", Type: "blob",
				DeclaredType: &DeclaredType{Base: "tinyblob"},
			},
			current: Column{
				Name: "value", Type: "blob",
				DeclaredType: &DeclaredType{Base: "longblob"},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := Table{
				Schema:  "target",
				Name:    "items",
				Columns: []Column{test.current},
			}
			catalog := mustCompleteEvolutionCatalog(t, table)
			if _, err := PlanSafeTypeWidening(
				catalog,
				table,
				test.previous,
				test.current,
			); err != nil {
				t.Fatalf("safe widening rejected: %v", err)
			}
		})
	}
}

func TestPlanSafeTypeWideningRejectsUnsafeShapes(t *testing.T) {
	t.Parallel()

	defaultValue := mustEvolutionDefault(t, `'stable'`)
	changedDefault := mustEvolutionDefault(t, `'changed'`)
	tests := []struct {
		name     string
		previous Column
		current  Column
		identity bool
	}{
		{
			name:     "integer narrowing",
			previous: Column{Name: "value", Type: "bigint"},
			current:  Column{Name: "value", Type: "integer"},
		},
		{
			name:     "unchanged",
			previous: Column{Name: "value", Type: "integer"},
			current:  Column{Name: "value", Type: "integer"},
		},
		{
			name: "varchar narrowing",
			previous: Column{
				Name: "value", Type: "varchar",
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{80},
				},
			},
			current: Column{
				Name: "value", Type: "varchar",
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "numeric loses integer capacity",
			previous: Column{
				Name: "value", Type: "numeric",
				DeclaredType: &DeclaredType{
					Base: "numeric", Arguments: []int{12, 2},
				},
			},
			current: Column{
				Name: "value", Type: "numeric",
				DeclaredType: &DeclaredType{
					Base: "numeric", Arguments: []int{12, 4},
				},
			},
		},
		{
			name: "char is not variable-width",
			previous: Column{
				Name: "value", Type: "char",
				DeclaredType: &DeclaredType{
					Base: "char", Arguments: []int{20},
				},
			},
			current: Column{
				Name: "value", Type: "char",
				DeclaredType: &DeclaredType{
					Base: "char", Arguments: []int{40},
				},
			},
		},
		{
			name:     "unrelated type",
			previous: Column{Name: "value", Type: "text"},
			current:  Column{Name: "value", Type: "json"},
		},
		{
			name: "one sided declaration",
			previous: Column{
				Name: "value", Type: "integer",
			},
			current: Column{
				Name: "value", Type: "bigint",
				DeclaredType: &DeclaredType{Base: "bigint"},
			},
		},
		{
			name: "coupled default",
			previous: Column{
				Name: "value", Type: "varchar", Default: defaultValue,
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: Column{
				Name: "value", Type: "varchar", Default: changedDefault,
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "coupled nullability",
			previous: Column{
				Name: "value", Type: "varchar",
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: Column{
				Name: "value", Type: "varchar", Nullable: true,
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "primary key",
			previous: Column{
				Name: "value", Type: "integer",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			current: Column{
				Name: "value", Type: "bigint",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
		},
		{
			name:     "identity",
			previous: Column{Name: "value", Type: "integer"},
			current:  Column{Name: "value", Type: "bigint"},
			identity: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			previous := test.previous
			current := test.current
			if test.identity {
				previous.PrimaryKey = true
				previous.PrimaryKeyPosition = 1
				current.PrimaryKey = true
				current.PrimaryKeyPosition = 1
			}
			table := Table{
				Schema:  "target",
				Name:    "items",
				Columns: []Column{current},
			}
			if test.identity {
				table.Identity = &Identity{
					Column:     current.Name,
					Generation: IdentityByDefault,
				}
			}
			catalog := mustCompleteEvolutionCatalog(t, table)
			if _, err := PlanSafeTypeWidening(
				catalog,
				table,
				previous,
				current,
			); err == nil {
				t.Fatal("unsafe widening was admitted")
			}
		})
	}
}

func TestRenderColumnEvolutionFailsClosedForUnsupportedTargetsAndInputs(
	t *testing.T,
) {
	t.Parallel()

	column := evolutionFixtureColumn(40, true, nil)
	table := evolutionFixtureTable(column)
	operation, err := PlanAddNullableColumn(
		evolutionSnapshotBeforeColumn(table, column.Name),
		table,
		column,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		target Dialect
		reason string
	}{
		{target: SQLite, reason: "in-place evolution"},
		{target: ClickHouse, reason: "requires rebuild"},
		{target: Dialect("oracle"), reason: "unknown target"},
	} {
		if _, err := RenderColumnEvolution(
			test.target,
			operation,
		); err == nil || !strings.Contains(err.Error(), test.reason) {
			t.Fatalf(
				"target %q error = %v, want %q",
				test.target,
				err,
				test.reason,
			)
		}
	}
	if _, err := RenderColumnEvolution(
		Postgres,
		ColumnEvolution{},
	); err == nil || !strings.Contains(err.Error(), "unproved") {
		t.Fatalf("zero proof error = %v", err)
	}
}

func TestRenderColumnEvolutionRevalidatesTargetTypesAndDefaults(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		target Dialect
		column Column
	}{
		{
			name:   "SQL Server requires exact declaration",
			target: SQLServer,
			column: Column{Name: "value", Type: "text", Nullable: true},
		},
		{
			name:   "PostgreSQL rejects invalid modifier",
			target: Postgres,
			column: Column{
				Name: "value", Type: "text", Nullable: true,
				DeclaredType: &DeclaredType{
					Base: "varchar", Arguments: []int{0},
				},
			},
		},
		{
			name:   "MySQL rejects incompatible default",
			target: MySQL,
			column: Column{
				Name: "value", Type: "integer", Nullable: true,
				DeclaredType: &DeclaredType{Base: "int"},
				Default:      mustEvolutionDefault(t, `'not-a-number'`),
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := Table{
				Schema:  "target",
				Name:    "items",
				Columns: []Column{test.column},
			}
			operation, err := PlanAddNullableColumn(
				evolutionSnapshotBeforeColumn(table, test.column.Name),
				table,
				test.column,
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RenderColumnEvolution(
				test.target,
				operation,
			); err == nil {
				t.Fatal("invalid target-ready shape was rendered")
			}
		})
	}
}

func TestMySQLWideningPreservesLargeObjectDefault(t *testing.T) {
	t.Parallel()

	defaultValue := mustEvolutionDefault(t, `'retained'`)
	previous := Column{
		Name: "payload", Type: "text", Nullable: true,
		DeclaredType: &DeclaredType{Base: "text"},
		Default:      defaultValue,
	}
	current := cloneEvolutionColumn(previous)
	current.DeclaredType.Base = "longtext"
	table := Table{
		Schema: "target", Name: "items",
		Columns: []Column{current},
	}
	catalog := mustCompleteEvolutionCatalog(t, table)
	operation, err := PlanSafeTypeWidening(
		catalog,
		table,
		previous,
		current,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RenderColumnEvolution(MySQL, operation)
	if err != nil {
		t.Fatal(err)
	}
	const want = "ALTER TABLE `target`.`items` MODIFY COLUMN `payload` " +
		"LONGTEXT NULL DEFAULT ('retained');"
	if got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestColumnEvolutionPlanningDoesNotMutateEvidence(t *testing.T) {
	t.Parallel()

	defaultValue := mustEvolutionDefault(t, `'stable'`)
	previous := evolutionFixtureColumn(20, false, defaultValue)
	current := evolutionFixtureColumn(40, false, defaultValue)
	table := evolutionFixtureTable(current)
	catalog := mustCompleteEvolutionCatalog(t, table)
	beforeTable := cloneEvolutionTable(table)
	beforePrevious := cloneEvolutionColumn(previous)
	beforeCurrent := cloneEvolutionColumn(current)

	if _, err := PlanSafeTypeWidening(
		catalog,
		table,
		previous,
		current,
	); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(table, beforeTable) ||
		!reflect.DeepEqual(previous, beforePrevious) ||
		!reflect.DeepEqual(current, beforeCurrent) {
		t.Fatal("planning mutated input evidence")
	}
}

func evolutionFixtureTable(column Column) Table {
	return Table{
		Schema: "tenant\"].`west",
		Name:   "orders\"].`archive",
		Columns: []Column{
			{
				Name:         "existing_id",
				Type:         "bigint",
				DeclaredType: &DeclaredType{Base: "bigint"},
			},
			column,
		},
	}
}

func evolutionFixtureColumn(
	length int,
	nullable bool,
	defaultValue *Expression,
) Column {
	return Column{
		Name:     "note\"].`value",
		Type:     "text",
		Nullable: nullable,
		DeclaredType: &DeclaredType{
			Base:      "varchar",
			Arguments: []int{length},
		},
		Default: defaultValue,
	}
}

func evolutionRenamedColumn(column Column, name string) Column {
	column.Name = name
	return column
}

func evolutionSnapshotBeforeColumn(
	table Table,
	columnName string,
) SnapshotTable {
	previous := SnapshotTable{
		Schema: table.Schema,
		Name:   table.Name,
	}
	columns := make([]SnapshotColumn, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == columnName {
			continue
		}
		snapshotColumn := SnapshotColumn{
			Name:               column.Name,
			Type:               column.Type,
			Nullable:           column.Nullable,
			PrimaryKey:         column.PrimaryKey,
			PrimaryKeyPosition: column.PrimaryKeyPosition,
		}
		if column.DeclaredType != nil {
			snapshotColumn.DeclaredType = &SnapshotDeclaredType{
				Base: column.DeclaredType.Base,
				Arguments: append(
					[]int(nil),
					column.DeclaredType.Arguments...,
				),
			}
		}
		if column.Default != nil {
			value := column.Default.CanonicalSQL()
			snapshotColumn.Default = &value
		}
		columns = append(columns, snapshotColumn)
	}
	previous.Columns = columns
	if table.Identity != nil &&
		table.Identity.Column != columnName {
		previous.Identity = &SnapshotIdentity{
			Column:     table.Identity.Column,
			Generation: table.Identity.Generation,
		}
	}
	return previous
}

func completeEvolutionRelationFixture(t *testing.T) []Table {
	t.Helper()
	check, err := ParseSQLiteCheckExpression(`"parent_id" >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	return []Table{
		{
			Schema: "target",
			Name:   "parents",
			Columns: []Column{
				{
					Name: "id", Type: "bigint",
					PrimaryKey: true, PrimaryKeyPosition: 1,
				},
				{Name: "label", Type: "text", Nullable: true},
			},
			// Empty names are legal evidence when the structural members
			// themselves are complete.
			Indexes: []Index{{
				Unique:  true,
				Columns: []IndexColumn{{Name: "id"}},
			}},
		},
		{
			Schema: "target",
			Name:   "children",
			Columns: []Column{
				{
					Name: "id", Type: "bigint",
					PrimaryKey: true, PrimaryKeyPosition: 1,
				},
				{Name: "parent_id", Type: "bigint", Nullable: true},
			},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parents",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
				OnDelete:          "SET NULL",
				Match:             "SIMPLE",
			}},
			Checks: []CheckConstraint{{
				Expression: check,
			}},
		},
	}
}

func mustCompleteEvolutionCatalog(
	t *testing.T,
	tables ...Table,
) CompleteEvolutionCatalog {
	t.Helper()
	catalog, err := NewCompleteEvolutionCatalog(tables)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func mustEvolutionDefault(t *testing.T, value string) *Expression {
	t.Helper()
	expression, err := ParseSQLiteDefault(value)
	if err != nil {
		t.Fatal(err)
	}
	return expression
}
