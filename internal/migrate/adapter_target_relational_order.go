package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// OrderSourceTables gives every relational rebuild target the same explicit
// parent-before-child order that the deferred network runner verifies before
// it freezes work inventory. Native drop/recreate finalizers install foreign
// keys after the set-wide data plane, but the durable table sequence still
// must be dependency-safe: it is reused verbatim on replay and by targets
// whose table creation keeps foreign keys active. The generic source helper
// intentionally preserves drop/recreate order for legacy callers, so this
// target capability is the narrow composed-route seam.
func orderRelationalStage4TargetSourceTables(
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	if _, err := normalizeAdapterTargetMode(mode); err != nil {
		return nil, err
	}
	ordered, err := orderAdapterSourceTablesForMode(tables, "upsert")
	if err != nil {
		return nil, fmt.Errorf(
			"relational target requires an acyclic parent-before-child table order: %w",
			err,
		)
	}
	return ordered, nil
}

func (*postgresTargetAdapter) OrderSourceTables(
	_ string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	return orderRelationalStage4TargetSourceTables(tables, mode)
}

func (*mysqlTargetAdapter) OrderSourceTables(
	_ string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	return orderRelationalStage4TargetSourceTables(tables, mode)
}

func (*sqlServerTargetAdapter) OrderSourceTables(
	_ string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	return orderRelationalStage4TargetSourceTables(tables, mode)
}
