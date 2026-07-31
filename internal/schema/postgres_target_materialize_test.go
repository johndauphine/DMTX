package schema

import (
	"reflect"
	"strings"
	"testing"
)

func TestMaterializePostgresObjectNamesUsesDDLPlannerNames(t *testing.T) {
	t.Parallel()

	check, err := ParseSQLiteCheckExpression(`code <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	tables := []Table{
		{
			Schema: "tenant",
			Name:   "parents",
			Columns: []Column{{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			}},
		},
		{
			Schema: "tenant",
			Name:   "children",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{Name: "parent_id", Type: "bigint"},
				{Name: "code", Type: "text"},
			},
			Indexes: []Index{{
				Unique: true,
				Inline: true,
				Columns: []IndexColumn{{
					Name: "code",
				}},
			}},
			Checks: []CheckConstraint{{
				Expression: check,
			}},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"parent_id"},
				ReferencedSchema:  "tenant",
				ReferencedTable:   "parents",
				ReferencedColumns: []string{"id"},
			}},
		},
	}

	first, err := MaterializePostgresObjectNames(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MaterializePostgresObjectNames(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("PostgreSQL object-name materialization is not deterministic")
	}
	idempotent, err := MaterializePostgresObjectNames(
		first,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, idempotent) {
		t.Fatal("PostgreSQL object-name materialization is not idempotent")
	}
	if tables[1].Indexes[0].Name != "" ||
		tables[1].Checks[0].Name != "" ||
		tables[1].ForeignKeys[0].Name != "" {
		t.Fatal("materialization mutated caller-owned metadata")
	}
	child := first[1]
	if child.Indexes[0].Name == "" ||
		child.Checks[0].Name == "" ||
		child.ForeignKeys[0].Name == "" {
		t.Fatalf(
			"materialized names = index:%q check:%q foreign_key:%q",
			child.Indexes[0].Name,
			child.Checks[0].Name,
			child.ForeignKeys[0].Name,
		)
	}
	objects, err := PlanPostgresDropRecreateObjects(
		first,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	gotNames := make(map[PostgresObjectKind]string)
	for _, object := range objects {
		if object.Table() == "children" {
			gotNames[object.Kind()] = object.Name()
		}
	}
	if gotNames[PostgresIndexObject] != child.Indexes[0].Name ||
		gotNames[PostgresCheckObject] != child.Checks[0].Name ||
		gotNames[PostgresForeignKeyObject] !=
			child.ForeignKeys[0].Name {
		t.Fatalf("planner names = %#v, materialized child = %#v", gotNames, child)
	}
}

func TestMaterializePostgresObjectNamesRejectsAmbiguousObjects(t *testing.T) {
	t.Parallel()

	check, err := ParseSQLiteCheckExpression(`id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	table := Table{
		Schema: "tenant",
		Name:   "events",
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		Checks: []CheckConstraint{
			{Expression: check},
			{Expression: check},
		},
	}
	if _, err := MaterializePostgresObjectNames(
		[]Table{table},
		PostgresObjectPlanOptions{},
	); err == nil {
		t.Fatal("ambiguous unnamed PostgreSQL CHECKs were accepted")
	}
}

func TestMaterializePostgresObjectNamesAfterPriorRejectsRigidNameCollisions(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name          string
		retainedIndex string
		added         Table
	}{
		{
			name:          "table",
			retainedIndex: "reserved_relation",
			added: Table{
				Schema:  "tenant",
				Name:    "reserved_relation",
				Columns: []Column{{Name: "id", Type: "bigint"}},
			},
		},
		{
			name:          "primary key",
			retainedIndex: "orders_pkey",
			added: Table{
				Schema: "tenant",
				Name:   "orders",
				Columns: []Column{{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				}},
			},
		},
		{
			name:          "identity sequence",
			retainedIndex: "orders_id_seq",
			added: Table{
				Schema: "tenant",
				Name:   "orders",
				Identity: &Identity{
					Column:     "id",
					Generation: IdentityByDefault,
				},
				Columns: []Column{{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				}},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			retained := Table{
				Schema: "tenant",
				Name:   "zeta",
				Columns: []Column{
					{
						Name:               "id",
						Type:               "bigint",
						PrimaryKey:         true,
						PrimaryKeyPosition: 1,
					},
					{Name: "value", Type: "text"},
				},
				Indexes: []Index{{
					Name: test.retainedIndex,
					Columns: []IndexColumn{{
						Name: "value",
					}},
				}},
			}
			prior := []Table{retained}
			priorMaterialized, err := MaterializePostgresObjectNames(
				prior,
				PostgresObjectPlanOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			current := []Table{test.added, retained}
			priorBefore := clonePostgresMaterializeTestTables(prior)
			priorMaterializedBefore := clonePostgresMaterializeTestTables(
				priorMaterialized,
			)
			currentBefore := clonePostgresMaterializeTestTables(current)

			_, err = MaterializePostgresObjectNamesAfterPrior(
				current,
				prior,
				priorMaterialized,
				PostgresObjectPlanOptions{},
			)
			if err == nil || !strings.Contains(
				err.Error(),
				"collides between",
			) {
				t.Fatalf("rigid name collision error = %v", err)
			}
			if !reflect.DeepEqual(prior, priorBefore) ||
				!reflect.DeepEqual(
					priorMaterialized,
					priorMaterializedBefore,
				) ||
				!reflect.DeepEqual(current, currentBefore) {
				t.Fatal("prior-aware materialization mutated its inputs")
			}
		})
	}
}

func clonePostgresMaterializeTestTables(tables []Table) []Table {
	result := make([]Table, len(tables))
	for index, table := range tables {
		result[index] = cloneEvolutionTable(table)
	}
	return result
}
