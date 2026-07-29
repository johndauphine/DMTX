package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresDSNUsesSecureDefaultAndEscapesCredentials(t *testing.T) {
	dsn, err := PostgresDSN(config.Endpoint{
		Host: "db.example.test", Database: "dmtx", User: "a@user", Password: "p@ss word",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "sslmode=require") || !strings.Contains(dsn, "a%40user") || !strings.Contains(dsn, "p%40ss%20word") {
		t.Fatalf("unexpected DSN encoding")
	}
}

func TestPostgresDSNRequiresConnectionIdentity(t *testing.T) {
	if _, err := PostgresDSN(config.Endpoint{}); err == nil {
		t.Fatal("expected incomplete endpoint to be rejected")
	}
}

func TestBuildPostgresTableMarksCompositePrimaryKeyAndNormalizesTypes(t *testing.T) {
	table := buildPostgresTable("public", "events", []schema.Column{
		{Name: "tenant_id", Type: normalizePostgresType("integer")},
		{Name: "occurred_at", Type: normalizePostgresType("timestamp with time zone")},
		{Name: "note", Type: normalizePostgresType("character varying"), Nullable: true},
	}, []string{"tenant_id", "occurred_at"})
	if table.Schema != "public" || table.Name != "events" || !table.Columns[0].PrimaryKey || !table.Columns[1].PrimaryKey || table.Columns[2].PrimaryKey {
		t.Fatalf("table = %#v", table)
	}
	if table.Columns[1].Type != "timestamp" || table.Columns[2].Type != "varchar" {
		t.Fatalf("columns = %#v", table.Columns)
	}
}
