package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestMySQLToMySQLRejectsSameConfiguredDatabase(t *testing.T) {
	endpoint := config.Endpoint{
		Type:     "mysql",
		Host:     "db.example",
		Database: "production",
		User:     "dmtx",
	}
	_, err := MySQLToMySQLWithObserver(
		context.Background(),
		config.Config{Source: endpoint, Target: endpoint},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("same-database error = %v", err)
	}
}

func TestSameMySQLDatabaseIdentityUsesServerUUIDAndDatabase(t *testing.T) {
	source := mysqlDatabaseIdentity{
		serverUUID: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
		database:   "production",
	}
	alias := mysqlDatabaseIdentity{
		serverUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		database:   "production",
	}
	if !sameMySQLDatabaseIdentity(source, alias) {
		t.Fatal("same live server/database identity was not detected")
	}
	otherDatabase := alias
	otherDatabase.database = "staging"
	if sameMySQLDatabaseIdentity(source, otherDatabase) {
		t.Fatal("different selected databases were treated as identical")
	}
	otherServer := alias
	otherServer.serverUUID = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	if sameMySQLDatabaseIdentity(source, otherServer) {
		t.Fatal("different server UUIDs were treated as identical")
	}
}

func TestMySQLNativeRoutesValidatePairBeforeOpening(t *testing.T) {
	tests := []struct {
		name string
		run  func(config.Config) (Result, error)
		want string
	}{
		{
			name: "mysql to mysql",
			run: func(cfg config.Config) (Result, error) {
				return MySQLToMySQLWithObserver(
					context.Background(),
					cfg,
					nil,
				)
			},
			want: "MySQL-to-MySQL",
		},
		{
			name: "postgres to mysql",
			run: func(cfg config.Config) (Result, error) {
				return PostgresToMySQLWithObserver(
					context.Background(),
					cfg,
					nil,
				)
			},
			want: "PostgreSQL-to-MySQL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.run(config.Config{
				Source: config.Endpoint{Type: "sqlite"},
				Target: config.Endpoint{Type: "mysql"},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("route error = %v", err)
			}
		})
	}
}
