package engine

import (
	"strings"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/config"
)

func validMariaDB1011TargetServerCatalog() mariaDB1011TargetServerCatalog {
	source := validMariaDB1011ServerCatalog()
	source.sqlMode += ",NO_AUTO_VALUE_ON_ZERO"
	return mariaDB1011TargetServerCatalog{
		source:                source,
		foreignKeyChecks:      1,
		uniqueChecks:          1,
		checkConstraintChecks: 1,
		innodbPageSize:        16_384,
		innodbForcePrimaryKey: 0,
	}
}

func TestValidateMariaDB1011TargetServerCatalog(t *testing.T) {
	if err := validateMariaDB1011TargetServerCatalog(
		validMariaDB1011TargetServerCatalog(),
	); err != nil {
		t.Fatalf("valid MariaDB target catalog: %v", err)
	}

	tests := map[string]func(*mariaDB1011TargetServerCatalog){
		"source boundary": func(value *mariaDB1011TargetServerCatalog) {
			value.source.sessionTimeZone = "-05:00"
		},
		"zero identity mode": func(value *mariaDB1011TargetServerCatalog) {
			value.source.sqlMode = strings.ReplaceAll(
				value.source.sqlMode,
				",NO_AUTO_VALUE_ON_ZERO",
				"",
			)
		},
		"foreign key enforcement": func(value *mariaDB1011TargetServerCatalog) {
			value.foreignKeyChecks = 0
		},
		"unique enforcement": func(value *mariaDB1011TargetServerCatalog) {
			value.uniqueChecks = 0
		},
		"CHECK enforcement": func(value *mariaDB1011TargetServerCatalog) {
			value.checkConstraintChecks = 0
		},
		"InnoDB page size": func(value *mariaDB1011TargetServerCatalog) {
			value.innodbPageSize = 8_192
		},
		"forced primary keys": func(value *mariaDB1011TargetServerCatalog) {
			value.innodbForcePrimaryKey = 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validMariaDB1011TargetServerCatalog()
			mutate(&value)
			if err := validateMariaDB1011TargetServerCatalog(
				value,
			); err == nil {
				t.Fatal("unsafe MariaDB target session was accepted")
			}
		})
	}
}

func TestMariaDB1011TargetDSNPinsSessionWithoutOracleParameter(
	t *testing.T,
) {
	sqlMode, err := mariaDB1011TargetSQLMode(
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
		false,
		map[string]string{
			"check_constraint_checks": "1",
			"foreign_key_checks":      "1",
			"sql_mode":                sqlMode,
			"unique_checks":           "1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse MariaDB target DSN: %v", err)
	}
	for name, want := range map[string]string{
		"check_constraint_checks": "1",
		"foreign_key_checks":      "1",
		"sql_mode": "'NO_AUTO_VALUE_ON_ZERO,NO_ZERO_DATE," +
			"STRICT_TRANS_TABLES'",
		"unique_checks": "1",
	} {
		if parsed.Params[name] != want {
			t.Fatalf(
				"MariaDB target DSN %s = %q, want %q",
				name,
				parsed.Params[name],
				want,
			)
		}
	}
	if value, present := parsed.Params["information_schema_stats_expiry"]; present {
		t.Fatalf(
			"MariaDB target DSN unexpectedly sets information_schema_stats_expiry = %q",
			value,
		)
	}
}

func TestExportedMySQLServerFlavorConstantsPreserveInternalAliases(
	t *testing.T,
) {
	if mysqlServerFlavorOracle80 != MySQLServerFlavorOracle80 ||
		mysqlServerFlavorMariaDB1011 != MySQLServerFlavorMariaDB1011 ||
		mysqlServerFlavorUnknown != MySQLServerFlavorUnknown {
		t.Fatal("internal and exported MySQL flavor constants differ")
	}
}
