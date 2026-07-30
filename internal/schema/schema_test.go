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
		{"json", Postgres, "JSON"},
		{"jsonb", Postgres, "JSONB"},
		{"date", ClickHouse, "Date"},
		{"text", ClickHouse, "String"},
		{"boolean", ClickHouse, "Bool"},
		{"timestamp", ClickHouse, "DateTime64(6)"},
		{"timestamptz", Postgres, "TIMESTAMP WITH TIME ZONE"},
	}
	for _, test := range cases {
		got, err := MapType(test.source, test.target)
		if err != nil || got != test.want {
			t.Fatalf("MapType(%q, %q) = %q, %v; want %q", test.source, test.target, got, err, test.want)
		}
	}
}

func TestMapTypeRejectsTimestamptzOutsidePostgres(t *testing.T) {
	for _, target := range []Dialect{
		SQLite,
		MySQL,
		SQLServer,
		ClickHouse,
	} {
		if _, err := MapType("timestamptz", target); err == nil {
			t.Fatalf("timestamptz unexpectedly mapped to %s", target)
		} else if _, ok := err.(*PolicyError); !ok {
			t.Fatalf("timestamptz to %s error type = %T", target, err)
		}
	}
}

func TestUnknownTypeIsClassifiable(t *testing.T) {
	_, err := MapType("mystery", Postgres)
	if _, ok := err.(*PolicyError); !ok {
		t.Fatalf("expected policy error, got %T", err)
	}
}

func TestClickHouseOrderByIsIndependentFromRelationalPrimaryKey(t *testing.T) {
	table := Table{
		Schema:            "analytics",
		Name:              "events",
		ClickHouseOrderBy: []string{"tenant_id", "event_id"},
		Columns: []Column{
			{Name: "payload", Type: "text"},
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_id", Type: "bigint"},
		},
	}
	statement, err := CreateTable(ClickHouse, table)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE TABLE "analytics"."events" (` +
		`"payload" String, "tenant_id" Int64, "event_id" Int64) ` +
		`ENGINE = MergeTree ORDER BY ("tenant_id", "event_id");`
	if statement != want {
		t.Fatalf("ClickHouse DDL:\n got: %s\nwant: %s", statement, want)
	}
	for _, column := range table.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
			t.Fatalf("ordering metadata became relational key: %#v", column)
		}
	}
}

func TestClickHouseRendererDoesNotInferOrderFromRelationalPrimaryKey(
	t *testing.T,
) {
	table := Table{
		Schema: "analytics",
		Name:   "events",
		Columns: []Column{{
			Name:       "id",
			Type:       "bigint",
			PrimaryKey: true,
		}},
	}
	statement, err := CreateTable(ClickHouse, table)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE TABLE "analytics"."events" ("id" Int64) ` +
		`ENGINE = MergeTree ORDER BY tuple();`
	if statement != want {
		t.Fatalf("ClickHouse DDL:\n got: %s\nwant: %s", statement, want)
	}
}

func TestClickHouseOrderByRejectsMissingOrDuplicateColumns(t *testing.T) {
	base := Table{
		Name:              "events",
		ClickHouseOrderBy: []string{"id"},
		Columns:           []Column{{Name: "id", Type: "bigint"}},
	}
	tests := []Table{
		func() Table {
			value := base
			value.ClickHouseOrderBy = []string{"missing"}
			return value
		}(),
		func() Table {
			value := base
			value.ClickHouseOrderBy = []string{"id", "id"}
			return value
		}(),
	}
	for _, table := range tests {
		if _, err := CreateTable(ClickHouse, table); err == nil {
			t.Fatalf("unsafe ClickHouse ordering accepted: %#v", table)
		}
	}
}

func TestSQLiteForeignKeyClauseOrderIsDeterministic(t *testing.T) {
	table := Table{
		Name: "children",
		Columns: []Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "parent_id", Type: "integer"},
		},
		ForeignKeys: []ForeignKey{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "RESTRICT",
		}},
	}
	const want = `CREATE TABLE "children" ("id" INTEGER NOT NULL, "parent_id" INTEGER NOT NULL, PRIMARY KEY ("id"), FOREIGN KEY ("parent_id") REFERENCES "parents" ("id") ON UPDATE CASCADE ON DELETE RESTRICT);`
	for attempt := 0; attempt < 100; attempt++ {
		got, err := CreateTable(SQLite, table)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("attempt %d:\n got: %s\nwant: %s", attempt, got, want)
		}
	}
}
