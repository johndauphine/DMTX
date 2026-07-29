package schema

import "testing"

func TestDropTableQuotesIdentifiers(t *testing.T) {
	statement, err := DropTable(SQLite, Table{Name: `order"items`})
	if err != nil {
		t.Fatal(err)
	}
	if statement != `DROP TABLE IF EXISTS "order""items";` {
		t.Fatalf("statement = %q", statement)
	}
}

func TestDropPostgresTablesIsDeterministicAndDoesNotCascade(t *testing.T) {
	tables := []Table{
		{Schema: `tenant"west`, Name: "parents"},
		{Schema: `tenant"west`, Name: "children"},
	}
	statement, err := DropTables(Postgres, tables)
	if err != nil {
		t.Fatal(err)
	}
	const want = `DROP TABLE IF EXISTS "tenant""west"."children", "tenant""west"."parents";`
	if statement != want {
		t.Fatalf("statement = %q, want %q", statement, want)
	}
	reversed, err := DropTables(Postgres, []Table{tables[1], tables[0]})
	if err != nil {
		t.Fatal(err)
	}
	if reversed != want {
		t.Fatalf("reversed statement = %q, want %q", reversed, want)
	}
}

func TestDropPostgresTablesRejectsUnsafeShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		target Dialect
		tables []Table
	}{
		{name: "unsupported dialect", target: SQLite, tables: []Table{{Name: "items"}}},
		{name: "empty set", target: Postgres},
		{name: "empty name", target: Postgres, tables: []Table{{Schema: "public"}}},
		{name: "duplicate", target: Postgres, tables: []Table{{Name: "items"}, {Name: "items"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DropTables(test.target, test.tables); err == nil {
				t.Fatal("DropTables unexpectedly succeeded")
			}
		})
	}
}
