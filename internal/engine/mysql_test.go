package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
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

func TestMySQLDSNRejectsUnsafeTLSConfiguration(t *testing.T) {
	base := config.Endpoint{
		Host:     "db.example.test",
		Database: "dmtx",
		User:     "reader",
	}
	unsupported := base
	unsupported.SSLMode = "disable"
	if _, err := MySQLDSN(unsupported); err == nil {
		t.Fatal("expected disabled MySQL TLS to be rejected")
	}
	missingCA := base
	missingCA.TLSCAFile = "/path/that/does/not/exist/dmtx-ca.pem"
	if _, err := MySQLDSN(missingCA); err == nil {
		t.Fatal("expected unreadable MySQL TLS CA to be rejected")
	}
}

func TestParseMySQLVersion(t *testing.T) {
	major, minor, patch, ok := parseMySQLVersion("8.0.46")
	if !ok || major != 8 || minor != 0 || patch != 46 {
		t.Fatalf(
			"version = %d.%d.%d ok=%t",
			major,
			minor,
			patch,
			ok,
		)
	}
	for _, value := range []string{"8.4.0", "10.11.8-MariaDB"} {
		if _, _, _, ok := parseMySQLVersion(value); !ok {
			t.Fatalf("expected syntactically valid version %q", value)
		}
	}
	if _, _, _, ok := parseMySQLVersion("8.0"); ok {
		t.Fatal("expected incomplete version to be rejected")
	}
}
