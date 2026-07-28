package migrate

import (
	"context"
	"testing"

	"github.com/johndauphine/DMTX/internal/schema"
)

func TestNotifySQLiteTransferPlansSkipsLegacySingleWatermarkProgress(t *testing.T) {
	integerWatermark := int64(42)
	rowNumberWatermark := int64(17)
	plans := []sqlitePlannedTable{
		{table: schema.Table{Name: "integer_legacy"}},
		{table: schema.Table{Name: "row_number_legacy"}},
		{table: schema.Table{Name: "pristine"}},
		{table: schema.Table{Name: "completed"}},
	}
	observer := &sqlitePipelineTestObserver{}

	err := notifySQLiteTransferPlans(
		context.Background(),
		observer,
		plans,
		sqliteEffectiveTransferSettings{targetMode: "upsert"},
		map[string]int{"completed": 9},
		map[string]TableProgress{
			"integer_legacy": {
				RowsDone:         42,
				IntegerWatermark: &integerWatermark,
			},
			"row_number_legacy": {
				RowsDone:           17,
				RowNumberWatermark: &rowNumberWatermark,
			},
			"pristine": {},
		},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}

	notified := observer.snapshotPlans()
	if len(notified) != 1 || notified[0].Table != "pristine" {
		t.Fatalf("notified plans = %#v, want only pristine table", notified)
	}
}

func TestNotifySQLiteTransferPlansIncludesAllFreshTables(t *testing.T) {
	plans := []sqlitePlannedTable{
		{table: schema.Table{Name: "items"}},
		{table: schema.Table{Name: "events"}},
	}
	observer := &sqlitePipelineTestObserver{}

	err := notifySQLiteTransferPlans(
		context.Background(),
		observer,
		plans,
		sqliteEffectiveTransferSettings{},
		nil,
		map[string]TableProgress{
			"items": {RowsDone: 99},
		},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	notified := observer.snapshotPlans()
	if len(notified) != len(plans) {
		t.Fatalf("notified %d fresh plans, want %d", len(notified), len(plans))
	}
}
