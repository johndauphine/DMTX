package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLServerDSNRequiresEncryptionAndEscapesCredentials(t *testing.T) {
	dsn, err := SQLServerDSN(config.Endpoint{
		Host:      "db.example.test",
		Database:  "dmtx",
		User:      "a@user",
		Password:  "p@ss word",
		TLSCAFile: "/etc/dmtx/sql server ca.pem",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "encrypt=true") ||
		!strings.Contains(dsn, "guid+conversion=true") ||
		!strings.Contains(dsn, "tlsmin=1.2") ||
		!strings.Contains(
			dsn,
			"certificate=%2Fetc%2Fdmtx%2Fsql+server+ca.pem",
		) ||
		!strings.Contains(dsn, "a%40user") ||
		!strings.Contains(dsn, "p%40ss%20word") {
		t.Fatalf("unexpected SQL Server DSN %q", dsn)
	}
}
