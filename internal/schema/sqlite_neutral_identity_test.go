package schema

import (
	"reflect"
	"testing"
)

func TestCreateSQLiteTableRendersNeutralIdentityAndFrontierPlan(t *testing.T) {
	frontier := int64(50)
	table := Table{
		Name: "events",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
	ddl, err := CreateTable(SQLite, table)
	if err != nil {
		t.Fatal(err)
	}
	const wantDDL = `CREATE TABLE "events" ("id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "note" TEXT);`
	if ddl != wantDDL {
		t.Fatalf("DDL = %q, want %q", ddl, wantDDL)
	}
	plan, err := SQLiteSequencePlan(table)
	if err != nil {
		t.Fatal(err)
	}
	wantPlan := []Statement{
		{SQL: `DELETE FROM sqlite_sequence WHERE name = ?`, Args: []any{"events"}},
		{SQL: `INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)`, Args: []any{"events", int64(50)}},
	}
	if !reflect.DeepEqual(plan, wantPlan) {
		t.Fatalf("sequence plan = %#v, want %#v", plan, wantPlan)
	}

	smallint := table
	smallint.Columns = append([]Column(nil), table.Columns...)
	smallint.Columns[0].Type = "smallint"
	smallint.Columns[0].DeclaredType = &DeclaredType{Base: "smallint"}
	ddl, err = CreateTable(SQLite, smallint)
	if err != nil {
		t.Fatalf("render smallint network identity: %v", err)
	}
	if ddl != wantDDL {
		t.Fatalf("smallint identity DDL = %q, want %q", ddl, wantDDL)
	}
}

func TestCreateSQLiteTableRejectsInvalidNeutralIdentityShapes(t *testing.T) {
	base := Table{
		Name: "events",
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
	}
	negative := int64(-1)
	tests := []struct {
		name   string
		mutate func(*Table)
	}{
		{name: "generation", mutate: func(table *Table) {
			table.Identity.Generation = "always"
		}},
		{name: "negative frontier", mutate: func(table *Table) {
			table.Identity.Frontier = &negative
		}},
		{name: "missing column", mutate: func(table *Table) {
			table.Identity.Column = "missing"
		}},
		{name: "non integer", mutate: func(table *Table) {
			table.Columns[0].Type = "text"
		}},
		{name: "default", mutate: func(table *Table) {
			table.Columns[0].Default = &Expression{
				kind: expressionNumber,
				sql:  "1",
			}
		}},
		{name: "composite primary key", mutate: func(table *Table) {
			table.Columns = append(table.Columns, Column{
				Name:               "tenant",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			})
		}},
		{name: "without rowid", mutate: func(table *Table) {
			table.SQLiteWithoutRowID = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base
			identity := *base.Identity
			table.Identity = &identity
			table.Columns = append([]Column(nil), base.Columns...)
			test.mutate(&table)
			if ddl, err := CreateTable(SQLite, table); err == nil {
				t.Fatalf("invalid identity rendered as %q", ddl)
			}
			if plan, err := SQLiteSequencePlan(table); err == nil {
				t.Fatalf("invalid identity sequence plan = %#v", plan)
			}
		})
	}
}
