package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerDSNRequiresEncryptionAndEscapesCredentials(t *testing.T) {
	dsn, err := SQLServerDSN(config.Endpoint{Host: "db.example.test", Database: "dmtx", User: "a@user", Password: "p@ss word"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "encrypt=true") || !strings.Contains(dsn, "a%40user") || !strings.Contains(dsn, "p%40ss%20word") {
		t.Fatalf("unexpected SQL Server DSN %q", dsn)
	}
}

func TestBuildSQLServerTablePreservesCompositePrimaryKey(t *testing.T) {
	table := buildSQLServerTable("dbo", "events", []schema.Column{
		{Name: "tenant", Type: normalizeSQLServerType("int")},
		{Name: "id", Type: normalizeSQLServerType("bigint")},
		{Name: "active", Type: normalizeSQLServerType("bit")},
	}, []string{"tenant", "id"})
	if !table.Columns[0].PrimaryKey || !table.Columns[1].PrimaryKey || table.Columns[2].PrimaryKey {
		t.Fatalf("table = %#v", table)
	}
	if table.Columns[0].Type != "integer" || table.Columns[2].Type != "boolean" {
		t.Fatalf("columns = %#v", table.Columns)
	}
}
