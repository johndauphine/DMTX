package app

import (
	"context"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

func TestPostgresDropPreflightRequiresExistingTableOwnership(
	t *testing.T,
) {
	t.Parallel()

	write, create := admitPostgresTargetPrivileges(
		"drop_recreate",
		true,
		true,
		true,
		true,
		true,
		true,
		false,
	)
	if write || !create {
		t.Fatalf(
			"non-owner drop privileges = write:%v create:%v",
			write,
			create,
		)
	}
	write, create = admitPostgresTargetPrivileges(
		"drop_recreate",
		true,
		true,
		true,
		true,
		true,
		true,
		true,
	)
	if !write || !create {
		t.Fatalf(
			"owner drop privileges = write:%v create:%v",
			write,
			create,
		)
	}
	write, _ = admitPostgresTargetPrivileges(
		"upsert",
		false,
		true,
		true,
		true,
		false,
		false,
		false,
	)
	if !write {
		t.Fatal("upsert incorrectly required table ownership")
	}
	write, _ = admitPostgresTargetPrivileges(
		"upsert",
		true,
		true,
		true,
		false,
		true,
		true,
		true,
	)
	if write {
		t.Fatal("PostgreSQL upsert accepted INSERT and SELECT without UPDATE")
	}
}

func TestSQLiteDiskCapacityKnownShortfallFailsClosed(t *testing.T) {
	t.Parallel()

	shortfall := sqliteTargetDiskCapacityFact(101, 100, true)
	if shortfall.Finding.Severity != migrate.PreflightSeverityError ||
		shortfall.Class != preflightClassFailed ||
		shortfall.Evidence != "required_bytes=101;free_bytes=100" {
		t.Fatalf("known disk shortfall = %#v", shortfall)
	}
	enough := sqliteTargetDiskCapacityFact(100, 100, true)
	if enough.Finding.Severity != migrate.PreflightSeverityInfo ||
		enough.Class != preflightClassPassed {
		t.Fatalf("known sufficient capacity = %#v", enough)
	}
	unknown := sqliteTargetDiskCapacityFact(100, 0, false)
	if unknown.Finding.Severity != migrate.PreflightSeverityWarning ||
		unknown.Class != preflightClassUnverified {
		t.Fatalf("unknown capacity = %#v", unknown)
	}
}

func TestMissingSQLiteTargetUnknownWriteAccessFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	probe := productionEndpointProbe{
		endpoint: config.Endpoint{
			Type:     "sqlite",
			Database: "missing.db",
		},
		side:           migrate.PreflightTarget,
		databaseAbsent: true,
	}
	for _, fact := range []productionPreflightFact{
		targetWriteFact(probe),
		targetSchemaCreateFact(probe),
		targetDeletePrivilegeFact(probe),
	} {
		if fact.Finding.Severity != migrate.PreflightSeverityError ||
			fact.Class != preflightClassUnverified {
			t.Fatalf("unknown filesystem access fact = %#v", fact)
		}
	}
}

func TestDestructiveOccupancyCatalogFailureFailsClosed(t *testing.T) {
	t.Parallel()

	fact := targetDestructiveAcknowledgementFact(
		context.Background(),
		config.Config{},
		productionEndpointProbe{selectedTables: nil},
		productionEndpointProbe{
			endpoint: config.Endpoint{Type: "sqlite"},
			side:     migrate.PreflightTarget,
		},
	)
	if fact.Finding.Severity != migrate.PreflightSeverityError ||
		fact.Class != preflightClassUnverified ||
		fact.Evidence != "target_occupancy_unverified" {
		t.Fatalf("catalog failure fact = %#v", fact)
	}
}

func TestMySQLBulkCapabilityRequiresLiveLocalInfileEvidence(
	t *testing.T,
) {
	t.Parallel()

	missing := mySQLBulkPathFact(productionEndpointProbe{})
	if missing.Finding.Severity != migrate.PreflightSeverityError ||
		missing.Class != preflightClassFailed {
		t.Fatalf("missing local-infile evidence = %#v", missing)
	}
	fallback := mySQLBulkPathFact(productionEndpointProbe{
		localInfileKnown: true,
		localInfile:      false,
	})
	if fallback.Finding.Severity != migrate.PreflightSeverityInfo ||
		fallback.Class != preflightClassPassed ||
		fallback.Evidence !=
			"bounded_insert_fallback_certified;local_infile=off" {
		t.Fatalf("bounded fallback fact = %#v", fallback)
	}
}
