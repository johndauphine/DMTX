package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// OrderSourceTables keeps referenced parents ahead of children because SQLite
// foreign-key enforcement is active while both rebuild and retained-upsert
// rows are written. Cycles fail closed: disabling enforcement, guessing row
// order, or temporarily carrying invalid target data would weaken the target
// contract.
func (*sqliteTargetAdapter) OrderSourceTables(
	_ string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	if _, err := normalizeAdapterTargetMode(mode); err != nil {
		return nil, err
	}
	ordered, err := orderAdapterSourceTablesForMode(tables, "upsert")
	if err != nil {
		return nil, fmt.Errorf(
			"SQLite target requires an acyclic parent-before-child table order while foreign keys are enforced: %w",
			err,
		)
	}
	return ordered, nil
}
