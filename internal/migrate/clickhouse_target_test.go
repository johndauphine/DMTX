package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestClickHouseDDLUsesMergeTreeAndPrimaryKeyOrder(t *testing.T) {
	table := schema.Table{Schema: "analytics", Name: "events", Columns: []schema.Column{{Name: "id", Type: "integer", PrimaryKey: true}, {Name: "note", Type: "text"}}}
	statement, err := schema.CreateTable(schema.ClickHouse, table)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`CREATE TABLE "analytics"."events"`, "ENGINE = MergeTree", `ORDER BY ("id")`} {
		if !strings.Contains(statement, expected) {
			t.Fatalf("statement %q does not contain %q", statement, expected)
		}
	}
}
