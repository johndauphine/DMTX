package app

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

func (observer tableCheckpointObserver) AfterSQLiteTransferPlan(_ context.Context, plan migrate.SQLiteTransferPlan) error {
	backend, ok := observer.store.(state.RangeBackend)
	if !ok {
		return stateCheckpointError("create range checkpoints", fmt.Errorf("state backend does not support range checkpoints"))
	}
	taskKey := state.TaskKey{Type: "table-copy", Schema: "main", Table: plan.Table}
	task := state.WorkTask{
		RunID:        observer.runID,
		Key:          taskKey,
		Strategy:     string(plan.Pagination.Strategy),
		TopologyHash: plan.Pagination.TopologyHash,
		StartedAt:    time.Now().UTC(),
	}
	ranges := make([]state.RangeState, 0, len(plan.Pagination.Ranges))
	for _, planned := range plan.Pagination.Ranges {
		lower, err := stateTuple(planned.Lower)
		if err != nil {
			return stateCheckpointError("encode range lower bound", err)
		}
		upper, err := stateTuple(planned.Upper)
		if err != nil {
			return stateCheckpointError("encode range upper bound", err)
		}
		rowsTotal := int64(0)
		if planned.FirstRow > 0 && planned.LastRow >= planned.FirstRow {
			rowsTotal = planned.LastRow - planned.FirstRow + 1
		}
		ranges = append(ranges, state.RangeState{
			ID:             strconv.Itoa(planned.ID),
			Lower:          lower,
			Upper:          upper,
			LowerInclusive: false,
			UpperInclusive: planned.Upper != nil,
			FirstRow:       planned.FirstRow,
			LastRow:        planned.LastRow,
			RowsTotal:      rowsTotal,
		})
	}
	_, err := backend.EnsureWorkPlan(task, ranges)
	if errors.Is(err, state.ErrTopologyChanged) && observer.resetTopology {
		if resetErr := backend.ResetWorkPlan(task, ranges); resetErr != nil {
			return stateCheckpointError("reset stale range checkpoints", resetErr)
		}
		return nil
	}
	if err != nil {
		return stateCheckpointError("create range checkpoints", err)
	}
	return nil
}

func stateTuple(tuple *migrate.KeyTuple) (state.TypedTuple, error) {
	if tuple == nil {
		return nil, nil
	}
	converted := make(state.TypedTuple, len(*tuple))
	for index, value := range *tuple {
		sqlValue, err := value.SQLValue()
		if err != nil {
			return nil, err
		}
		switch typed := sqlValue.(type) {
		case int64:
			converted[index] = state.Int64Value(typed)
		case string:
			converted[index] = state.TextValue(typed)
		case []byte:
			converted[index] = state.BytesValue(typed)
		default:
			return nil, fmt.Errorf("unsupported pagination key type %T", sqlValue)
		}
	}
	return converted, nil
}
