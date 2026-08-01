package migrate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

// clickHouseStage4RejectionConfig builds a real ClickHouse-endpoint migration so
// the refusals below are proven against a configured live endpoint rather than
// a placeholder. The refusal must still arrive before any connection is opened.
func clickHouseStage4RejectionConfig(
	t *testing.T,
	source config.Endpoint,
) config.Config {
	t.Helper()
	// ClickHouse is only a source in the clickhouse-to-clickhouse pair, so the
	// target must be another ClickHouse database. A SQLite target would be
	// refused by route resolution instead, which would prove nothing about the
	// Stage 4 gates under test.
	target := source
	target.Database = "dmtx_target"
	return config.Config{
		Source: source,
		Target: target,
		Migration: config.Migration{
			TargetMode:    "upsert",
			IncludeTables: []string{"events"},
			Validation: config.ValidationPolicy{
				Mode: config.ValidationCountOnly,
			},
		},
	}
}

// TestClickHouseIncrementalRejectedBeforeMutationLive proves a real ClickHouse
// source is refused for date-based incremental transfer before anything is
// opened or written. ClickHouse has no certified incremental window, and the
// refusal is the contract — a silent fallback to a full transfer would move far
// more data than the operator asked for.
func TestClickHouseIncrementalRejectedBeforeMutationLive(t *testing.T) {
	source := clickHouseLiveEndpoint(t)
	cfg := clickHouseStage4RejectionConfig(t, source)
	cfg.Migration.DateUpdatedColumns = []string{"updated_at"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events := make([]string, 0)
	_, err := Execute(ctx, cfg, &recordingTableObserver{events: &events})
	if err == nil {
		t.Fatal("a live ClickHouse source was admitted for incremental transfer")
	}
	if len(events) != 0 {
		t.Fatalf(
			"ClickHouse incremental refusal reached the table lifecycle: %v",
			events,
		)
	}
	// The refusal arrives one layer earlier than this fixture's name suggests:
	// ClickHouse is a rebuild-only target and does not support upsert at all,
	// and date-based incremental requires upsert. So the route is unreachable by
	// construction rather than gated by an incremental-specific check. That is a
	// stronger guarantee, not a weaker one, and asserting the real reason keeps
	// the test honest about which layer protects the operator.
	if !strings.Contains(err.Error(), "does not support upsert mode") {
		t.Fatalf("ClickHouse incremental refusal = %v", err)
	}
}

// TestClickHouseDeleteReconcileRejectedBeforeMutationLive proves the same for
// delete reconciliation. Deleting target rows on the strength of an uncertified
// key comparison is the most destructive thing this tool can be asked to do, so
// the refusal must precede every connection.
func TestClickHouseDeleteReconcileRejectedBeforeMutationLive(t *testing.T) {
	source := clickHouseLiveEndpoint(t)
	cfg := clickHouseStage4RejectionConfig(t, source)
	cfg.Migration.Deletes = config.DeletePolicy{
		Mode:           config.DeleteModeReconcile,
		TargetBehavior: config.DeleteTargetHard,
		Reconcile: config.DeleteReconcilePolicy{
			Schedule:          config.DeleteScheduleInterval,
			Interval:          time.Hour,
			BatchSize:         100,
			RequirePrimaryKey: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events := make([]string, 0)
	_, err := Execute(ctx, cfg, &recordingTableObserver{events: &events})
	if err == nil {
		t.Fatal("a live ClickHouse source was admitted for delete reconciliation")
	}
	if len(events) != 0 {
		t.Fatalf(
			"ClickHouse delete refusal reached the table lifecycle: %v",
			events,
		)
	}
	// As with incremental, the refusal is that ClickHouse cannot be an upsert
	// target at all, which delete reconciliation requires. Deleting target rows
	// on an uncertified comparison is the most destructive thing this tool can
	// do, so being unreachable by construction is the outcome to want.
	if !strings.Contains(err.Error(), "does not support upsert mode") {
		t.Fatalf("ClickHouse delete refusal = %v", err)
	}
}
