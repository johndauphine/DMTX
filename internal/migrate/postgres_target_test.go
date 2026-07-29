package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresWriteStatementUsesNativePlaceholdersAndUpsert(t *testing.T) {
	table := schema.Table{Schema: "public", Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}, {Name: "note"}}}
	statement := postgresWriteStatement(table, []string{"id", "note"}, "upsert")
	for _, expected := range []string{`INSERT INTO "public"."events"`, `VALUES ($1, $2)`, `ON CONFLICT ("id")`, `"note" = EXCLUDED."note"`} {
		if !strings.Contains(statement, expected) {
			t.Fatalf("statement %q does not contain %q", statement, expected)
		}
	}
	if strings.Contains(statement, "?") {
		t.Fatalf("PostgreSQL statement must not contain SQLite placeholders: %q", statement)
	}
}

func TestPostgresWriteStatementOnlyPrimaryKeyDoesNothingOnConflict(t *testing.T) {
	table := schema.Table{Schema: "public", Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}}}
	statement := postgresWriteStatement(table, []string{"id"}, "upsert")
	if !strings.HasSuffix(statement, `ON CONFLICT ("id") DO NOTHING`) {
		t.Fatalf("unexpected statement: %q", statement)
	}
}

func TestPostgresPlaceholders(t *testing.T) {
	if got, want := postgresPlaceholders(3), "$1, $2, $3"; got != want {
		t.Fatalf("postgresPlaceholders() = %q, want %q", got, want)
	}
}
