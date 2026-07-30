package engine

import (
	"crypto/tls"
	"os"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
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

func TestClickHouseDSNSupportsIPv6AndRejectsWeakTLSModes(t *testing.T) {
	dsn, err := ClickHouseDSN(config.Endpoint{
		Host:     "::1",
		Database: "dmtx",
		User:     "tester",
		SSLMode:  "verify-full",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "@[::1]:9440/") {
		t.Fatalf("IPv6 ClickHouse DSN = %q", dsn)
	}
	_, err = ClickHouseDSN(config.Endpoint{
		Host:     "localhost",
		Database: "dmtx",
		User:     "tester",
		SSLMode:  "disable",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("weak TLS mode error = %v", err)
	}
}

func TestClickHouseTLSConfigRequiresTLS12AndValidCA(t *testing.T) {
	configuration, err := clickHouseTLSConfig(config.Endpoint{
		Host: "db.example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if configuration.MinVersion != tls.VersionTLS12 ||
		configuration.ServerName != "db.example.test" ||
		configuration.InsecureSkipVerify {
		t.Fatalf("TLS configuration = %+v", configuration)
	}

	path := t.TempDir() + "/invalid-ca.pem"
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = clickHouseTLSConfig(config.Endpoint{
		Host:      "db.example.test",
		TLSCAFile: path,
	})
	if err == nil || !strings.Contains(err.Error(), "no certificates") {
		t.Fatalf("invalid CA error = %v", err)
	}
}

func TestNormalizeClickHouseTypeUnwrapsNullable(t *testing.T) {
	if got, want := normalizeClickHouseType("Nullable(Int32)"), "integer"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
}
