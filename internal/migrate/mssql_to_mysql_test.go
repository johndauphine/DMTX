package migrate

import (
	"context"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLServerToMySQLRejectsWrongTypesBeforeOpening(t *testing.T) {
	tests := []config.Config{
		{
			Source: config.Endpoint{Type: "postgres"},
			Target: config.Endpoint{Type: "mysql"},
		},
		{
			Source: config.Endpoint{Type: "mssql"},
			Target: config.Endpoint{Type: "postgres"},
		},
	}
	const want = "SQL Server-to-MySQL requires source.type mssql and target.type mysql"
	for _, cfg := range tests {
		_, err := SQLServerToMySQLWithObserver(
			context.Background(),
			cfg,
			nil,
		)
		if err == nil || err.Error() != want {
			t.Fatalf(
				"SQLServerToMySQLWithObserver(%s, %s) error = %v, want %q",
				cfg.Source.Type,
				cfg.Target.Type,
				err,
				want,
			)
		}
	}
}

func TestSQLServerToMySQLRouteIsCertified(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source: config.Endpoint{
			Type:     "mssql",
			Host:     "source.example.test",
			Database: "source",
			User:     "reader",
		},
		Target: config.Endpoint{
			Type:     "mysql",
			Host:     "target.example.test",
			Database: "target",
			User:     "writer",
		},
	})
	if err != nil {
		t.Fatalf("ValidateMigration(SQL Server-to-MySQL): %v", err)
	}
}
