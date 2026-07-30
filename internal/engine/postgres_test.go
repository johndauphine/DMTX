package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
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
