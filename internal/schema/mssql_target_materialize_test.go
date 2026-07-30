package schema

import (
	"reflect"
	"sort"
	"testing"
)

func TestMaterializeSQLServerObjectNamesUsesPlannerIdentityWithoutMutation(
	t *testing.T,
) {
	check, err := ParseSQLiteCheckExpression("balance >= 0")
	if err != nil {
		t.Fatal(err)
	}
	tables := []Table{
		{
			Schema: "dbo",
			Name:   "accounts",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
					DeclaredType: &DeclaredType{
						Base: "bigint",
					},
				},
				{
					Name: "balance",
					Type: "numeric",
					DeclaredType: &DeclaredType{
						Base:      "decimal",
						Arguments: []int{18, 0},
					},
				},
			},
			Indexes: []Index{{
				Unique: true,
				Columns: []IndexColumn{{
					Name: "balance",
				}},
			}},
			Checks: []CheckConstraint{{
				Expression: check,
			}},
		},
		{
			Schema: "dbo",
			Name:   "events",
			Columns: []Column{
				{
					Name:               "sequence_no",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
					DeclaredType: &DeclaredType{
						Base: "bigint",
					},
				},
				{
					Name: "account_id",
					Type: "bigint",
					DeclaredType: &DeclaredType{
						Base: "bigint",
					},
				},
			},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"account_id"},
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
				OnDelete:          "NO ACTION",
				Match:             "SIMPLE",
			}},
		},
	}
	before := cloneSQLServerMaterializedTables(tables)
	originalPlan, err := PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		t.Fatalf("plan anonymous objects: %v", err)
	}

	materialized, err := MaterializeSQLServerObjectNames(tables)
	if err != nil {
		t.Fatalf("materialize object names: %v", err)
	}
	if !reflect.DeepEqual(tables, before) {
		t.Fatalf("input tables mutated:\n got %#v\nwant %#v", tables, before)
	}
	if materialized[0].Indexes[0].Name == "" ||
		materialized[0].Checks[0].Name == "" ||
		materialized[1].ForeignKeys[0].Name == "" {
		t.Fatalf("materialized objects = %#v", materialized)
	}
	materializedPlan, err :=
		PlanSQLServerDropRecreateObjects(materialized)
	if err != nil {
		t.Fatalf("plan materialized objects: %v", err)
	}
	sort.Slice(originalPlan, func(left, right int) bool {
		return originalPlan[left].SQL < originalPlan[right].SQL
	})
	sort.Slice(materializedPlan, func(left, right int) bool {
		return materializedPlan[left].SQL <
			materializedPlan[right].SQL
	})
	if !reflect.DeepEqual(originalPlan, materializedPlan) {
		t.Fatalf(
			"materialized plan changed:\n got %#v\nwant %#v",
			materializedPlan,
			originalPlan,
		)
	}

	reversed := []Table{tables[1], tables[0]}
	reversedMaterialized, err :=
		MaterializeSQLServerObjectNames(reversed)
	if err != nil {
		t.Fatalf("materialize reversed tables: %v", err)
	}
	if reversedMaterialized[0].ForeignKeys[0].Name !=
		materialized[1].ForeignKeys[0].Name ||
		reversedMaterialized[1].Indexes[0].Name !=
			materialized[0].Indexes[0].Name ||
		reversedMaterialized[1].Checks[0].Name !=
			materialized[0].Checks[0].Name {
		t.Fatalf(
			"materialized names depend on input order: %#v / %#v",
			materialized,
			reversedMaterialized,
		)
	}

	materialized[0].Columns[1].DeclaredType.Arguments[0] = 1
	materialized[0].Indexes[0].Columns[0].Name = "id"
	materialized[1].ForeignKeys[0].Columns[0] = "sequence_no"
	if tables[0].Columns[1].DeclaredType.Arguments[0] != 18 ||
		tables[0].Indexes[0].Columns[0].Name != "balance" ||
		tables[1].ForeignKeys[0].Columns[0] != "account_id" {
		t.Fatal("materialized tables alias input metadata")
	}
}
