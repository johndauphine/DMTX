package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestClickHouseToClickHouseRejectsSameConfiguredDatabase(t *testing.T) {
	endpoint := config.Endpoint{
		Type:     "clickhouse",
		Host:     "db.example",
		Database: "production",
		User:     "dmtx",
	}
	_, err := ClickHouseToClickHouseWithObserver(
		context.Background(),
		config.Config{Source: endpoint, Target: endpoint},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("same-database error = %v", err)
	}
}

func TestSameClickHouseDatabaseIdentityUsesAtomicDatabaseUUID(
	t *testing.T,
) {
	source := clickHouseDatabaseIdentity{
		uuid:     "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
		database: "source",
	}
	alias := clickHouseDatabaseIdentity{
		uuid:     "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		database: "source_alias",
	}
	if !sameClickHouseDatabaseIdentity(source, alias) {
		t.Fatal("same live ClickHouse database UUID was not detected")
	}
	other := alias
	other.uuid = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	if sameClickHouseDatabaseIdentity(source, other) {
		t.Fatal("different ClickHouse database UUIDs were treated as equal")
	}
}

func TestSameConfiguredClickHouseDatabaseUsesDefaultTLSPort(t *testing.T) {
	source := config.Endpoint{
		Host:     "DB.EXAMPLE",
		Database: "analytics",
	}
	target := config.Endpoint{
		Host:     "db.example",
		Port:     9440,
		Database: "analytics",
	}
	if !sameConfiguredClickHouseDatabase(source, target) {
		t.Fatal("default ClickHouse TLS port was not canonicalized")
	}
	target.Database = "other"
	if sameConfiguredClickHouseDatabase(source, target) {
		t.Fatal("distinct ClickHouse databases were treated as equal")
	}
}

func TestClickHouseNativeRouteFailsUnsupportedCapabilitiesBeforeOpen(
	t *testing.T,
) {
	base := config.Config{
		Source: config.Endpoint{
			Type:     "clickhouse",
			Host:     "source.example",
			Database: "source",
			User:     "dmtx",
		},
		Target: config.Endpoint{
			Type:     "clickhouse",
			Host:     "target.example",
			Database: "target",
			User:     "dmtx",
		},
	}
	upsert := base
	upsert.Migration.TargetMode = "upsert"
	if err := ValidateMigration(upsert); err == nil ||
		!strings.Contains(err.Error(), "does not support upsert") {
		t.Fatalf("upsert error = %v", err)
	}
	strict := base
	strict.Migration.StrictConsistency = true
	if err := ValidateMigration(strict); err == nil ||
		!strings.Contains(err.Error(), "strict consistency") {
		t.Fatalf("strict consistency error = %v", err)
	}
}
