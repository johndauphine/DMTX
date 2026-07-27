package schema

import "testing"

func TestCreateTableIsDeterministicAcrossDialects(t *testing.T) {
	table := Table{Schema: "public", Name: "users", Columns: []Column{{Name: "id", Type: "bigint", PrimaryKey: true}, {Name: "name", Type: "text", Nullable: true}}}
	for _, dialect := range []Dialect{Postgres, SQLServer, MySQL, SQLite, ClickHouse} {
		first, err := CreateTable(dialect, table)
		if err != nil {
			t.Fatalf("%s: %v", dialect, err)
		}
		second, _ := CreateTable(dialect, table)
		if first != second {
			t.Fatalf("%s renderer was non-deterministic", dialect)
		}
	}
}

func TestMapTypeSupportsCommonPortableTypes(t *testing.T) {
	cases := []struct {
		source string
		target Dialect
		want   string
	}{
		{"decimal", Postgres, "DECIMAL(38, 10)"},
		{"decimal", ClickHouse, "Decimal(38, 10)"},
		{"double precision", SQLite, "REAL"},
		{"uuid", SQLServer, "UNIQUEIDENTIFIER"},
		{"bytea", Postgres, "BYTEA"},
		{"jsonb", MySQL, "JSON"},
		{"date", ClickHouse, "DATE"},
	}
	for _, test := range cases {
		got, err := MapType(test.source, test.target)
		if err != nil || got != test.want {
			t.Fatalf("MapType(%q, %q) = %q, %v; want %q", test.source, test.target, got, err, test.want)
		}
	}
}

func TestUnknownTypeIsClassifiable(t *testing.T) {
	_, err := MapType("mystery", Postgres)
	if _, ok := err.(*PolicyError); !ok {
		t.Fatalf("expected policy error, got %T", err)
	}
}
