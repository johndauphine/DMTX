package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/schema"
)

func TestSQLServerWriteStatementUsesNativeParametersAndMerge(t *testing.T) {
	table := schema.Table{Schema: "dbo", Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}, {Name: "note"}}}
	statement := sqlServerWriteStatement(table, []string{"id", "note"}, "upsert")
	for _, expected := range []string{"MERGE INTO [dbo].[events]", "VALUES (@p1, @p2)", "target.[id] = source.[id]", "WHEN MATCHED THEN UPDATE SET", "WHEN NOT MATCHED THEN INSERT"} {
		if !strings.Contains(statement, expected) {
			t.Fatalf("statement %q does not contain %q", statement, expected)
		}
	}
}

func TestSQLServerWriteStatementWithoutUpsertUsesInsert(t *testing.T) {
	table := schema.Table{Schema: "dbo", Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}}}
	statement := sqlServerWriteStatement(table, []string{"id"}, "drop_recreate")
	if got, want := statement, "INSERT INTO [dbo].[events] ([id]) VALUES (@p1)"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}
