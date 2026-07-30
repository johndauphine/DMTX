package engine

import (
	"strings"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
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
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL DSN: %v", err)
	}
	if _, present := parsed.Params["information_schema_stats_expiry"]; present {
		t.Fatalf(
			"generic MySQL/MariaDB DSN unexpectedly sets information_schema_stats_expiry = %q",
			parsed.Params["information_schema_stats_expiry"],
		)
	}
}

func TestMySQL80DSNRefreshesInformationSchemaStatistics(t *testing.T) {
	dsn, err := mySQLDSN(config.Endpoint{
		Host:     "db.example.test",
		Database: "dmtx",
		User:     "reader",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL 8.0 DSN: %v", err)
	}
	if parsed.Params["information_schema_stats_expiry"] != "0" {
		t.Fatalf(
			"MySQL 8.0 DSN information_schema_stats_expiry = %q, want 0",
			parsed.Params["information_schema_stats_expiry"],
		)
	}
}

func TestMySQL80TargetDSNPinsSessionSafety(t *testing.T) {
	sqlMode, err := mysql80TargetSQLMode(
		"STRICT_TRANS_TABLES,NO_ZERO_DATE",
	)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := mySQLDSNWithSessionParams(
		config.Endpoint{
			Host:     "db.example.test",
			Database: "dmtx",
			User:     "writer",
		},
		true,
		map[string]string{
			"foreign_key_checks": "1",
			"sql_mode":           sqlMode,
			"unique_checks":      "1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MySQL target DSN: %v", err)
	}
	for name, want := range map[string]string{
		"foreign_key_checks":              "1",
		"information_schema_stats_expiry": "0",
		"sql_mode":                        "'NO_AUTO_VALUE_ON_ZERO,NO_ZERO_DATE,STRICT_TRANS_TABLES'",
		"unique_checks":                   "1",
	} {
		if parsed.Params[name] != want {
			t.Fatalf(
				"MySQL target DSN %s = %q, want %q",
				name,
				parsed.Params[name],
				want,
			)
		}
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
