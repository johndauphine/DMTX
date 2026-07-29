package app

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestNetworkLeaseIdentityNormalizesHostAndDefaultPort(t *testing.T) {
	identity, path, err := targetLeaseLocation(config.Endpoint{Type: "postgres", Host: "DB.EXAMPLE", Database: "warehouse", Schema: "public"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := identity, "postgres:db.example:5432:warehouse:public"; got != want {
		t.Fatalf("identity = %q, want %q", got, want)
	}
	if !strings.HasSuffix(path, ".db") || !strings.Contains(path, "dmtx") {
		t.Fatalf("unexpected lease path %q", path)
	}
}

func TestSQLiteLeaseStaysAdjacentToTarget(t *testing.T) {
	identity, path, err := targetLeaseLocation(config.Endpoint{Type: "sqlite", Database: "target.db"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(identity, "sqlite:") || !strings.HasSuffix(path, "target.db.dmtx-lease.db") {
		t.Fatalf("identity = %q, path = %q", identity, path)
	}
}
