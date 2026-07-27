package engine

import (
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestTargetCapabilitiesMatchRequiredDifferences(t *testing.T) {
	sqlite, ok := TargetCapability("sqlite")
	if !ok || !sqlite.Upsert || sqlite.PostLoadConstraints {
		t.Fatalf("SQLite capability = %#v", sqlite)
	}
	clickhouse, ok := TargetCapability("clickhouse")
	if !ok || clickhouse.Upsert || clickhouse.SequenceReset {
		t.Fatalf("ClickHouse capability = %#v", clickhouse)
	}
}

func TestValidateMigrationRejectsUnsupportedTargetModeBeforePairSelection(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "clickhouse"},
		Migration: config.Migration{TargetMode: "upsert"},
	})
	if err == nil || !strings.Contains(err.Error(), "does not support upsert") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateMigrationAcceptsImplementedPairs(t *testing.T) {
	for _, cfg := range []config.Config{
		{Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "sqlite"}},
		{Source: config.Endpoint{Type: "postgres"}, Target: config.Endpoint{Type: "sqlite"}},
	} {
		if err := ValidateMigration(cfg); err != nil {
			t.Fatalf("ValidateMigration(%#v) = %v", cfg, err)
		}
	}
}
