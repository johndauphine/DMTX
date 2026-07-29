package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

type targetTablesPlanner interface {
	PlanTables(
		string,
		[]schema.Table,
		string,
	) ([]schema.Table, error)
}

func planSingleTargetTable(
	planner targetTablesPlanner,
	sourceEngine string,
	sourceTable schema.Table,
	mode string,
) (schema.Table, error) {
	tables, err := planner.PlanTables(
		sourceEngine,
		[]schema.Table{sourceTable},
		mode,
	)
	if err != nil {
		return schema.Table{}, err
	}
	if len(tables) != 1 {
		return schema.Table{}, fmt.Errorf(
			"planned %d target tables, want 1",
			len(tables),
		)
	}
	return tables[0], nil
}
