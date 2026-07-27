package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/schema"
)

func TestMySQLDSNRequiresTLSAndEscapesCredentials(t *testing.T) {
	dsn, err := MySQLDSN(config.Endpoint{Host: "db.example.test", Database: "dmtx", User: "a:user", Password: "p@ss word"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "tls=true") || !strings.Contains(dsn, "a:user:p@ss word@tcp(db.example.test:3306)") {
		t.Fatalf("unexpected MySQL DSN %q", dsn)
	}
}

func TestMySQLDSNRequiresConnectionIdentity(t *testing.T) {
	if _, err := MySQLDSN(config.Endpoint{}); err == nil {
		t.Fatal("expected incomplete endpoint to be rejected")
	}
}

func TestBuildMySQLTablePreservesCompositePrimaryKeyOrder(t *testing.T) {
	table := buildMySQLTable("crm", "events", []schema.Column{
		{Name: "tenant_id", Type: normalizeMySQLType("int")},
		{Name: "event_id", Type: normalizeMySQLType("bigint")},
		{Name: "message", Type: normalizeMySQLType("varchar"), Nullable: true},
	}, []string{"tenant_id", "event_id"})
	if table.Schema != "crm" || !table.Columns[0].PrimaryKey || !table.Columns[1].PrimaryKey || table.Columns[2].PrimaryKey {
		t.Fatalf("table = %#v", table)
	}
	if table.Columns[0].Type != "integer" || table.Columns[2].Type != "text" {
		t.Fatalf("columns = %#v", table.Columns)
	}
}
