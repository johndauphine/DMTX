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
