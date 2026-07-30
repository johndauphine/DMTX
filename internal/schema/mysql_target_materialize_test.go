package schema

import (
	"reflect"
	"testing"
)

func TestMaterializeMySQLObjectNamesUsesPlannerNamesAndIsPure(
	t *testing.T,
) {
	check, err := ParseSQLiteCheckExpression("balance >= 0")
	if err != nil {
		t.Fatal(err)
	}
	tables := []Table{
		{
			Schema:         "target_db",
			Name:           "accounts",
			MySQLCollation: "utf8mb4_0900_bin",
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
					Name: "code",
					Type: "varchar",
					DeclaredType: &DeclaredType{
						Base:      "varchar",
						Arguments: []int{24},
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
					Name:      "code",
					Collation: "BINARY",
				}},
			}},
			Checks: []CheckConstraint{{
				Expression: check,
			}},
		},
		{
			Schema:         "target_db",
			Name:           "events",
			MySQLCollation: "utf8mb4_0900_bin",
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
					Name: "account_id",
					Type: "bigint",
					DeclaredType: &DeclaredType{
						Base: "bigint",
					},
				},
			},
			ForeignKeys: []ForeignKey{{
				Columns:         []string{"account_id"},
				ReferencedTable: "accounts",
				OnUpdate:        "CASCADE",
				OnDelete:        "NO ACTION",
				Match:           "NONE",
			}},
		},
	}
	before := cloneMySQLMaterializedTables(tables)

	first, err := MaterializeMySQLObjectNames(tables)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MaterializeMySQLObjectNames(tables)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf(
			"materialized names are nondeterministic:\n%#v\n%#v",
			first,
			second,
		)
	}
	if !reflect.DeepEqual(tables, before) {
		t.Fatalf("source tables were mutated:\n%#v", tables)
	}
	if first[0].Indexes[0].Name == "" ||
		first[0].Checks[0].Name == "" ||
		first[1].ForeignKeys[0].Name == "" {
		t.Fatalf("materialized objects = %#v", first)
	}

	plan, err := PlanMySQLDropRecreateObjects(first)
	if err != nil {
		t.Fatal(err)
	}
	plannedNames := make(map[MySQLObjectKind]map[string]bool)
	for _, statement := range plan {
		if plannedNames[statement.Kind] == nil {
			plannedNames[statement.Kind] = make(map[string]bool)
		}
		plannedNames[statement.Kind][statement.Name] = true
	}
	if !plannedNames[MySQLIndexObject][first[0].Indexes[0].Name] ||
		!plannedNames[MySQLCheckObject][first[0].Checks[0].Name] ||
		!plannedNames[MySQLForeignKeyObject][first[1].ForeignKeys[0].Name] {
		t.Fatalf(
			"materialized names do not match planner: tables=%#v plan=%#v",
			first,
			plan,
		)
	}

	first[0].Columns[0].DeclaredType.Base = "changed"
	first[0].Indexes[0].Columns[0].Name = "changed"
	first[1].ForeignKeys[0].Columns[0] = "changed"
	if !reflect.DeepEqual(tables, before) {
		t.Fatal("materialized result aliases source metadata")
	}
}

func TestMaterializeMySQLObjectNamesRejectsAmbiguousObjects(
	t *testing.T,
) {
	check, err := ParseSQLiteCheckExpression("id > 0")
	if err != nil {
		t.Fatal(err)
	}
	table := Table{
		Schema: "target_db",
		Name:   "events",
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
		Checks: []CheckConstraint{
			{Expression: check},
			{Expression: check},
		},
	}
	if _, err := MaterializeMySQLObjectNames(
		[]Table{table},
	); err == nil {
		t.Fatal("ambiguous anonymous CHECKs were accepted")
	}
}
