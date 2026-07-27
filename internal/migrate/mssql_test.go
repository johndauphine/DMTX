package migrate

import (
	"testing"

	"github.com/johndauphine/DMTX/internal/schema"
)

func TestSQLServerReadQueryUsesBracketEscapingAndPrimaryKeyOrder(t *testing.T) {
	query := sqlServerReadQuery("dbo", schema.Table{Name: "events", Columns: []schema.Column{
		{Name: "tenant", PrimaryKey: true}, {Name: "id", PrimaryKey: true}, {Name: "note"},
	}}, []string{"tenant", "id", "note"})
	want := "SELECT [tenant], [id], [note] FROM [dbo].[events] ORDER BY [tenant], [id]"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestSQLServerIdentifierEscapesClosingBracket(t *testing.T) {
	if got, want := sqlServerIdentifier("a]b"), "[a]]b]"; got != want {
		t.Fatalf("identifier = %q, want %q", got, want)
	}
}
