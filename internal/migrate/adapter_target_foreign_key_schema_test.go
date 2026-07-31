package migrate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestRebaseProjectedForeignKeySchemasPreservesExactReference(
	t *testing.T,
) {
	t.Parallel()

	source := schema.Table{
		Schema: "source",
		Name:   "events",
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_accounts_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "source",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		}},
	}
	projected := source
	if err := rebaseProjectedForeignKeySchemas(
		source.Schema,
		"target",
		"PostgreSQL",
		&projected,
	); err != nil {
		t.Fatal(err)
	}
	if projected.ForeignKeys[0].ReferencedSchema != "target" {
		t.Fatalf("projected foreign key = %#v", projected.ForeignKeys[0])
	}
	if source.ForeignKeys[0].ReferencedSchema != "source" {
		t.Fatal("projection mutated source foreign-key metadata")
	}

	sqlite := source
	if err := rebaseProjectedForeignKeySchemas(
		source.Schema,
		"",
		"SQLite",
		&sqlite,
	); err != nil {
		t.Fatal(err)
	}
	if sqlite.ForeignKeys[0].ReferencedSchema != "" {
		t.Fatalf("SQLite reference remained qualified: %#v", sqlite.ForeignKeys[0])
	}
}

func TestRebaseProjectedForeignKeySchemasQualifiesOwnerRelativeReference(
	t *testing.T,
) {
	t.Parallel()

	source := schema.Table{
		Name: "events",
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_accounts_fk",
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		}},
	}
	projected := source
	if err := rebaseProjectedForeignKeySchemas(
		source.Schema,
		"target",
		"MySQL",
		&projected,
	); err != nil {
		t.Fatal(err)
	}
	if projected.ForeignKeys[0].ReferencedSchema != "target" {
		t.Fatalf("projected foreign key = %#v", projected.ForeignKeys[0])
	}
	if source.ForeignKeys[0].ReferencedSchema != "" {
		t.Fatal("projection mutated owner-relative source metadata")
	}

	sqlite := source
	if err := rebaseProjectedForeignKeySchemas(
		source.Schema,
		"",
		"SQLite",
		&sqlite,
	); err != nil {
		t.Fatal(err)
	}
	if sqlite.ForeignKeys[0].ReferencedSchema != "" {
		t.Fatalf("SQLite reference became qualified: %#v", sqlite.ForeignKeys[0])
	}
}

func TestRebaseProjectedForeignKeySchemasRejectsCrossSchemaAlias(
	t *testing.T,
) {
	t.Parallel()

	table := schema.Table{
		Schema: "sales",
		Name:   "events",
		ForeignKeys: []schema.ForeignKey{{
			Name:             "events_accounts_fk",
			ReferencedSchema: "identity",
			ReferencedTable:  "accounts",
		}},
	}
	before := table
	before.ForeignKeys = append(
		[]schema.ForeignKey(nil),
		table.ForeignKeys...,
	)
	err := rebaseProjectedForeignKeySchemas(
		table.Schema,
		"target",
		"MySQL",
		&table,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "cross-schema reference identity.accounts") {
		t.Fatalf("cross-schema projection error = %v", err)
	}
	if !reflect.DeepEqual(table.ForeignKeys, before.ForeignKeys) {
		t.Fatal("failed projection rewrote foreign-key identity")
	}
}
