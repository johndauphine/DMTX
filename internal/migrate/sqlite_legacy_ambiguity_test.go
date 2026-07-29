package migrate

import (
	"errors"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestValidateSQLiteLegacyProgressRequiresMatchingUnambiguousFrontier(t *testing.T) {
	integer := int64(-7)
	rowNumber := int64(3)
	plans := []sqlitePlannedTable{
		{
			table: schema.Table{Name: "integer_items"},
			pagination: PaginationPlan{
				Strategy: PaginationIntegerKeyset,
			},
		},
		{
			table: schema.Table{Name: "numbered_items"},
			pagination: PaginationPlan{
				Strategy: PaginationRowNumber,
			},
		},
		{
			table: schema.Table{Name: "tuple_items"},
			pagination: PaginationPlan{
				Strategy: PaginationTupleKeyset,
			},
		},
	}
	settings := sqliteEffectiveTransferSettings{
		targetMode: "upsert",
		partitions: 1,
	}

	valid := map[string]TableProgress{
		"integer_items": {
			RowsDone:         2,
			IntegerWatermark: &integer,
		},
		"numbered_items": {
			RowsDone:           3,
			RowNumberWatermark: &rowNumber,
		},
	}
	if err := validateSQLiteLegacyProgress(plans[:2], valid, settings, true); err != nil {
		t.Fatalf("valid legacy progress rejected: %v", err)
	}

	tests := []struct {
		name     string
		plan     sqlitePlannedTable
		progress TableProgress
	}{
		{
			name:     "row count without a frontier",
			plan:     plans[0],
			progress: TableProgress{RowsDone: 2},
		},
		{
			name: "frontier without a row count",
			plan: plans[0],
			progress: TableProgress{
				IntegerWatermark: &integer,
			},
		},
		{
			name: "conflicting frontier kinds",
			plan: plans[0],
			progress: TableProgress{
				RowsDone:           3,
				IntegerWatermark:   &integer,
				RowNumberWatermark: &rowNumber,
			},
		},
		{
			name: "wrong frontier kind",
			plan: plans[0],
			progress: TableProgress{
				RowsDone:           3,
				RowNumberWatermark: &rowNumber,
			},
		},
		{
			name: "row number count mismatch",
			plan: plans[1],
			progress: TableProgress{
				RowsDone:           2,
				RowNumberWatermark: &rowNumber,
			},
		},
		{
			name: "tuple frontier is not representable",
			plan: plans[2],
			progress: TableProgress{
				RowsDone:           3,
				RowNumberWatermark: &rowNumber,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSQLiteLegacyProgress(
				[]sqlitePlannedTable{test.plan},
				map[string]TableProgress{
					test.plan.table.Name: test.progress,
				},
				settings,
				true,
			)
			if !errors.Is(err, state.ErrAmbiguousLegacy) {
				t.Fatalf("error = %v, want ErrAmbiguousLegacy", err)
			}
			if ClassifyTransferError(err) != ErrorClassState {
				t.Fatalf("error class = %q, want state", ClassifyTransferError(err))
			}
		})
	}
}

func TestValidateSQLiteLegacyProgressRebuildIgnoresOldFrontier(t *testing.T) {
	plan := sqlitePlannedTable{
		table: schema.Table{Name: "items"},
		pagination: PaginationPlan{
			Strategy: PaginationTupleKeyset,
		},
	}
	if err := validateSQLiteLegacyProgress(
		[]sqlitePlannedTable{plan},
		map[string]TableProgress{
			"items": {RowsDone: 9},
		},
		sqliteEffectiveTransferSettings{
			targetMode: "drop_recreate",
			partitions: 4,
		},
		true,
	); err != nil {
		t.Fatalf("rebuild should restart instead of interpreting legacy progress: %v", err)
	}
}
