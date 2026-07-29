package engine

import (
	"testing"
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
