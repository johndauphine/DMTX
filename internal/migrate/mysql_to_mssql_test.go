package migrate

import (
	"context"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestMySQLToSQLServerRejectsWrongTypesBeforeOpening(t *testing.T) {
	tests := []config.Config{
		{
			Source: config.Endpoint{Type: "postgres"},
			Target: config.Endpoint{Type: "mssql"},
		},
		{
			Source: config.Endpoint{Type: "mysql"},
			Target: config.Endpoint{Type: "postgres"},
		},
	}
	const want = "MySQL-to-SQL Server requires source.type mysql and target.type mssql"
	for _, cfg := range tests {
		_, err := MySQLToSQLServerWithObserver(
			context.Background(),
			cfg,
			nil,
		)
		if err == nil || err.Error() != want {
			t.Fatalf(
				"MySQLToSQLServerWithObserver(%s, %s) error = %v, want %q",
				cfg.Source.Type,
				cfg.Target.Type,
				err,
				want,
			)
		}
	}
}

func TestMySQLToSQLServerRouteIsCertifiedForMySQLFamilySource(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source: config.Endpoint{
			Type:     "mysql",
			Host:     "source.example.test",
			Database: "source",
			User:     "reader",
		},
		Target: config.Endpoint{
			Type:     "mssql",
			Host:     "target.example.test",
			Database: "target",
			User:     "writer",
		},
	})
	if err != nil {
		t.Fatalf("ValidateMigration(MySQL/MariaDB-to-SQL Server): %v", err)
	}
}
