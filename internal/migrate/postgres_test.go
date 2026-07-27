package migrate

import "testing"

func TestPostgresReadQueryUsesDeterministicPrimaryKeyOrder(t *testing.T) {
	query := postgresReadQueryForColumns("public", "events", []string{"tenant", "id", "note"}, []string{"tenant", "id"})
	want := `SELECT "tenant", "id", "note" FROM "public"."events" ORDER BY "tenant", "id"`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestPostgresIdentifierEscapesQuotes(t *testing.T) {
	if got, want := postgresIdentifier(`public"name`), `"public""name"`; got != want {
		t.Fatalf("identifier = %q, want %q", got, want)
	}
}
