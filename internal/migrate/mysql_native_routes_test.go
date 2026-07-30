package migrate

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
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
		flavor:         engine.MySQLServerFlavorOracle80,
		serverIdentity: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
		database:       "production",
	}
	alias := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorOracle80,
		serverIdentity: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		database:       "production",
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
	otherServer.serverIdentity = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	if sameMySQLDatabaseIdentity(source, otherServer) {
		t.Fatal("different server UUIDs were treated as identical")
	}
}

func TestSameMySQLDatabaseIdentityUsesExactMariaDBServerUID(t *testing.T) {
	source := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorMariaDB1011,
		serverIdentity: "4uZvX4oQl/FA0eDc4MsOlOTawj8=",
		database:       "production",
	}
	alias := source
	if !sameMySQLDatabaseIdentity(source, alias) {
		t.Fatal("same MariaDB server UID and database were not detected")
	}

	caseVariant := alias
	caseVariant.serverIdentity = "4UzvX4oQl/FA0eDc4MsOlOTawj8="
	if sameMySQLDatabaseIdentity(source, caseVariant) {
		t.Fatal("case-distinct MariaDB server UIDs were treated as equal")
	}

	otherDatabase := alias
	otherDatabase.database = "staging"
	if sameMySQLDatabaseIdentity(source, otherDatabase) {
		t.Fatal("different MariaDB databases were treated as identical")
	}
}

func TestSameMySQLDatabaseIdentityRejectsFlavorMismatch(t *testing.T) {
	oracle := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorOracle80,
		serverIdentity: "shared-looking-identity",
		database:       "production",
	}
	mariaDB := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorMariaDB1011,
		serverIdentity: "shared-looking-identity",
		database:       "production",
	}
	if sameMySQLDatabaseIdentity(oracle, mariaDB) {
		t.Fatal("different MySQL server flavors were treated as one server")
	}
}

func TestValidateMariaDBReplicationStatusColumns(t *testing.T) {
	valid := []string{
		"Connection_name",
		"Slave_SQL_State",
		"Slave_IO_State",
		"Master_Host",
		"Slave_IO_Running",
		"Slave_SQL_Running",
	}
	if err := validateMariaDBReplicationStatusColumns(valid); err != nil {
		t.Fatalf("valid MariaDB replication shape: %v", err)
	}

	tests := []struct {
		name    string
		columns []string
	}{
		{
			name: "missing connection name",
			columns: []string{
				"Master_Host",
				"Slave_IO_Running",
				"Slave_SQL_Running",
			},
		},
		{
			name: "missing running state",
			columns: []string{
				"Connection_name",
				"Master_Host",
				"Slave_IO_Running",
			},
		},
		{
			name: "duplicate column",
			columns: append(
				append([]string(nil), valid...),
				"master_host",
			),
		},
		{
			name: "empty column",
			columns: append(
				append([]string(nil), valid...),
				"",
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateMariaDBReplicationStatusColumns(
				test.columns,
			); err == nil {
				t.Fatal("unexpected MariaDB replication shape was accepted")
			}
		})
	}
}

func TestMariaDBDatabaseIdentityFromCatalogFailsClosed(t *testing.T) {
	validUID := sql.NullString{
		String: "4uZvX4oQl/FA0eDc4MsOlOTawj8=",
		Valid:  true,
	}
	validDatabase := sql.NullString{String: "production", Valid: true}
	identity, err := mariaDBDatabaseIdentityFromCatalog(
		validUID,
		validDatabase,
		0,
		0,
	)
	if err != nil {
		t.Fatalf("valid MariaDB identity: %v", err)
	}
	if identity.flavor != engine.MySQLServerFlavorMariaDB1011 ||
		identity.serverIdentity != validUID.String ||
		identity.database != validDatabase.String {
		t.Fatalf("MariaDB identity = %#v", identity)
	}

	tests := []struct {
		name                string
		uid                 sql.NullString
		database            sql.NullString
		wsrepOn             int
		replicationChannels int
	}{
		{
			name:     "missing server UID",
			uid:      sql.NullString{},
			database: validDatabase,
		},
		{
			name:     "empty server UID",
			uid:      sql.NullString{Valid: true},
			database: validDatabase,
		},
		{
			name: "missing database",
			uid:  validUID,
		},
		{
			name:     "wsrep cluster",
			uid:      validUID,
			database: validDatabase,
			wsrepOn:  1,
		},
		{
			name:                "replication channel",
			uid:                 validUID,
			database:            validDatabase,
			replicationChannels: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := mariaDBDatabaseIdentityFromCatalog(
				test.uid,
				test.database,
				test.wsrepOn,
				test.replicationChannels,
			); err == nil {
				t.Fatal("unsafe MariaDB identity catalog was accepted")
			}
		})
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
