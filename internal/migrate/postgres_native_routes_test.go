package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestPostgresToPostgresRejectsSameConfiguredDatabase(t *testing.T) {
	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     "db.example",
		Database: "production",
		User:     "dmtx",
	}
	_, err := PostgresToPostgresWithObserver(
		context.Background(),
		config.Config{Source: endpoint, Target: endpoint},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("same-database error = %v", err)
	}
}

func TestSameConfiguredPostgresDatabaseUsesDefaultPort(t *testing.T) {
	source := config.Endpoint{
		Host:     " DB.EXAMPLE ",
		Database: "production",
	}
	target := config.Endpoint{
		Host:     "db.example",
		Port:     5432,
		Database: "production",
	}
	if !sameConfiguredPostgresDatabase(source, target) {
		t.Fatal("same configured PostgreSQL database was not detected")
	}
	target.Database = "staging"
	if sameConfiguredPostgresDatabase(source, target) {
		t.Fatal("distinct PostgreSQL databases were treated as identical")
	}
	target.Database = source.Database
	target.Port = 5433
	if sameConfiguredPostgresDatabase(source, target) {
		t.Fatal("distinct PostgreSQL ports were treated as identical")
	}
}

func TestSamePostgresDatabaseIdentityUsesSystemIdentifierAndOID(
	t *testing.T,
) {
	source := postgresDatabaseIdentity{
		systemIdentifier: "7668098435510087718",
		databaseOID:      16384,
		database:         "production",
	}
	alias := postgresDatabaseIdentity{
		systemIdentifier: "7668098435510087718",
		databaseOID:      16384,
		database:         "renamed_production",
	}
	if !samePostgresDatabaseIdentity(source, alias) {
		t.Fatal("same live PostgreSQL database identity was not detected")
	}
	otherDatabase := alias
	otherDatabase.databaseOID++
	if samePostgresDatabaseIdentity(source, otherDatabase) {
		t.Fatal("different PostgreSQL database OIDs were treated as identical")
	}
	otherCluster := alias
	otherCluster.systemIdentifier = "8668098435510087718"
	if samePostgresDatabaseIdentity(source, otherCluster) {
		t.Fatal("different PostgreSQL clusters were treated as identical")
	}
}
