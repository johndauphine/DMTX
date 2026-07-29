package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLReadQueryUsesQuotedDeterministicPrimaryKeyOrder(t *testing.T) {
	query := mySQLReadQuery("crm", schema.Table{Name: "events", Columns: []schema.Column{
		{Name: "tenant", PrimaryKey: true}, {Name: "id", PrimaryKey: true}, {Name: "note"},
	}}, []string{"tenant", "id", "note"})
	want := "SELECT `tenant`, `id`, `note` FROM `crm`.`events` ORDER BY `tenant`, `id`"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestMySQLReadQueryProjectsTemporalColumnsAsRawText(t *testing.T) {
	table := schema.Table{
		Name: "event`data",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "event`date", Type: "date"},
			{Name: "occurred_at", Type: "datetime"},
			{Name: "updated_at", Type: "timestamp"},
			{Name: "payload", Type: "text"},
		},
	}
	query := mySQLReadQuery(
		"crm",
		table,
		[]string{
			"id",
			"event`date",
			"occurred_at",
			"updated_at",
			"payload",
		},
	)
	want := "SELECT `id`, " +
		"CAST(`event``date` AS CHAR) AS `event``date`, " +
		"CAST(`occurred_at` AS CHAR) AS `occurred_at`, " +
		"CAST(`updated_at` AS CHAR) AS `updated_at`, `payload` " +
		"FROM `crm`.`event``data` ORDER BY `id`"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestMySQLIdentifierEscapesBackticks(t *testing.T) {
	if got, want := mySQLIdentifier("a`b"), "`a``b`"; got != want {
		t.Fatalf("identifier = %q, want %q", got, want)
	}
}
