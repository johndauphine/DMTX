package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/schema"
)

func TestMySQLWriteStatementUsesMySQLUpsert(t *testing.T) {
	table := schema.Table{Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}, {Name: "note"}}}
	statement := mySQLWriteStatement(table, []string{"id", "note"}, "upsert")
	for _, expected := range []string{"INSERT INTO `events`", "VALUES (?, ?)", "ON DUPLICATE KEY UPDATE", "`note` = VALUES(`note`)"} {
		if !strings.Contains(statement, expected) {
			t.Fatalf("statement %q does not contain %q", statement, expected)
		}
	}
}

func TestMySQLWriteStatementWithOnlyPrimaryKeyIsNoOpUpdate(t *testing.T) {
	table := schema.Table{Name: "events", Columns: []schema.Column{{Name: "id", PrimaryKey: true}}}
	statement := mySQLWriteStatement(table, []string{"id"}, "upsert")
	if !strings.HasSuffix(statement, "`id` = `id`") {
		t.Fatalf("unexpected statement: %q", statement)
	}
}
