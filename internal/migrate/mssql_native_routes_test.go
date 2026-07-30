package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLServerToSQLServerRejectsSameConfiguredDatabase(t *testing.T) {
	endpoint := config.Endpoint{
		Type:     "mssql",
		Host:     "db.example",
		Database: "production",
		User:     "dmtx",
	}
	_, err := SQLServerToSQLServerWithObserver(
		context.Background(),
		config.Config{Source: endpoint, Target: endpoint},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("same-database error = %v", err)
	}
}

func TestSameConfiguredSQLServerDatabaseUsesDefaultPortAndCaseFold(t *testing.T) {
	source := config.Endpoint{
		Host:     " DB.EXAMPLE ",
		Database: "Production",
	}
	alias := config.Endpoint{
		Host:     "db.example",
		Port:     1433,
		Database: "production",
	}
	if !sameConfiguredSQLServerDatabase(source, alias) {
		t.Fatal("same configured SQL Server database was not detected")
	}
	alias.Database = "staging"
	if sameConfiguredSQLServerDatabase(source, alias) {
		t.Fatal("distinct SQL Server databases were treated as identical")
	}
	alias.Database = source.Database
	alias.Port = 1434
	if sameConfiguredSQLServerDatabase(source, alias) {
		t.Fatal("distinct SQL Server ports were treated as identical")
	}
}

func TestSameSQLServerDatabaseIdentityUsesDatabaseGUID(t *testing.T) {
	source := sqlServerDatabaseIdentity{
		databaseGUID: "9606DA0B-1E3C-48A0-9C4E-CFCE63746E05",
		database:     "production",
	}
	alias := sqlServerDatabaseIdentity{
		databaseGUID: "9606da0b-1e3c-48a0-9c4e-cfce63746e05",
		database:     "production_alias",
	}
	if !sameSQLServerDatabaseIdentity(source, alias) {
		t.Fatal("same live SQL Server database GUID was not detected")
	}
	other := alias
	other.databaseGUID = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
	if sameSQLServerDatabaseIdentity(source, other) {
		t.Fatal("different SQL Server database GUIDs were treated as identical")
	}
}

func TestSQLServerNativeRoutesValidatePairBeforeOpening(t *testing.T) {
	tests := []struct {
		name string
		run  func(config.Config) (Result, error)
		want string
	}{
		{
			name: "sql server to sql server",
			run: func(cfg config.Config) (Result, error) {
				return SQLServerToSQLServerWithObserver(
					context.Background(),
					cfg,
					nil,
				)
			},
			want: "SQL Server-to-SQL Server",
		},
		{
			name: "postgres to sql server",
			run: func(cfg config.Config) (Result, error) {
				return PostgresToSQLServerWithObserver(
					context.Background(),
					cfg,
					nil,
				)
			},
			want: "PostgreSQL-to-SQL Server",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.run(config.Config{
				Source: config.Endpoint{Type: "sqlite"},
				Target: config.Endpoint{Type: "mssql"},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("route error = %v", err)
			}
		})
	}
}

func TestSQLServerNativeRoutesAreCertified(t *testing.T) {
	tests := []config.Config{
		{
			Source: config.Endpoint{
				Type:     "mssql",
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
		},
		{
			Source: config.Endpoint{
				Type:     "postgres",
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
		},
	}
	for _, cfg := range tests {
		if err := ValidateMigration(cfg); err != nil {
			t.Fatalf(
				"ValidateMigration(%s-to-%s): %v",
				cfg.Source.Type,
				cfg.Target.Type,
				err,
			)
		}
	}
}
