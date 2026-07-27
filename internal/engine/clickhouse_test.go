package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestClickHouseDSNRequiresTLSAndEscapesCredentials(t *testing.T) {
	dsn, err := ClickHouseDSN(config.Endpoint{Host: "db.example.test", Database: "dmtx", User: "a@user", Password: "p@ss word"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "secure=true") || !strings.Contains(dsn, "a%40user") || !strings.Contains(dsn, "p%40ss%20word") {
		t.Fatalf("unexpected ClickHouse DSN %q", dsn)
	}
}

func TestNormalizeClickHouseTypeUnwrapsNullable(t *testing.T) {
	if got, want := normalizeClickHouseType("Nullable(Int32)"), "integer"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
}
