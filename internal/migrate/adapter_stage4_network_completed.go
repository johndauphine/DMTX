package migrate

import (
	"context"
	"fmt"
	"math"

	"github.com/johndauphine/dmtx/internal/state"
)

// Completed-table admission: proving a table the caller says is already done
// actually agrees with the target before it is skipped.

func (execution *stage4AdapterNetworkExecution) advanceCompletedTable(
	ctx context.Context,
	planIndex int,
	checkpointRows int,
) error {
	_, err := execution.validateCompletedTable(
		ctx,
		planIndex,
		checkpointRows,
		true,
	)
	return err
}

func (execution *stage4AdapterNetworkExecution) validateCompletedTable(
	ctx context.Context,
	planIndex int,
	checkpointRows int,
	advance bool,
) (uint64, error) {
	if execution == nil || !execution.deferred ||
		planIndex < 0 || planIndex >= len(execution.prepared.work) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed network table inventory is unavailable",
			),
		)
	}
	inventory, err := loadStage4WorkInventory(
		ctx,
		execution.prepared.run,
	)
	if err != nil {
		return 0, err
	}
	if err := execution.validateDurableTableTaskInventory(
		inventory,
	); err != nil {
		return 0, err
	}
	base := execution.prepared.work[planIndex]
	task, found := inventory.tasks[base.task]
	if !found || task.Status != "completed" ||
		task.RunID != execution.prepared.run.RunID ||
		task.Error != "" ||
		task.StartedAt.IsZero() ||
		task.UpdatedAt.IsZero() ||
		task.CompletedAt.IsZero() ||
		!task.UpdatedAt.Equal(task.CompletedAt) ||
		task.CompletedAt.Before(task.StartedAt) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s lacks exact durable network work",
				base.task.Table,
			),
		)
	}
	item, err := execution.reconstructCompletedTableWork(
		planIndex,
		inventory,
	)
	if err != nil {
		return 0, err
	}
	task, ranges, _, err := exactStage4AdapterWork(
		inventory,
		item,
		false,
	)
	if err != nil {
		return 0, err
	}
	if task.Status != "completed" {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has incomplete durable work",
				item.task.Table,
			),
		)
	}

	var rows int64
	for index, workRange := range ranges {
		restore, restoreErr := networkRestoreFromState(
			networkStateRangeBinding{
				RangeIndex: uint64(index),
				Task:       item.task,
				Initial:    item.ranges[index],
			},
			workRange,
		)
		if restoreErr != nil ||
			workRange.Status != "completed" ||
			workRange.RunID != execution.prepared.run.RunID ||
			workRange.Task != item.task ||
			workRange.Error != "" ||
			workRange.CommittedPrefix != 0 ||
			workRange.UpdatedAt.IsZero() ||
			workRange.CompletedAt.IsZero() ||
			!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
			workRange.CompletedAt.Before(task.StartedAt) ||
			workRange.CompletedAt.After(task.CompletedAt) ||
			!restore.Complete ||
			restore.SequenceOffset != 0 ||
			len(restore.Issued) != 0 ||
			workRange.RowsDone > math.MaxInt64-rows {
			if restoreErr == nil {
				restoreErr = fmt.Errorf(
					"completed range retains incomplete or overflowing work",
				)
			}
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s has invalid durable range evidence: %w",
					item.task.Table,
					restoreErr,
				),
			)
		}
		if err := validateCompletedStage4AdapterRange(
			task,
			item,
			index,
			workRange,
			restore,
		); err != nil {
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q has invalid strategy evidence: %w",
					item.task.Table,
					workRange.ID,
					err,
				),
			)
		}
		rows += workRange.RowsDone
	}
	if checkpointRows < 0 || rows != int64(checkpointRows) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s checkpoint differs from durable ranges",
				item.task.Table,
			),
		)
	}
	rangeCount := uint64(len(ranges))
	if !advance {
		return rangeCount, nil
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if rangeCount >
		maximumRuntimeTuningRanges-execution.nextGlobalRange {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 global network range inventory is unbounded"),
		)
	}
	execution.nextGlobalRange += rangeCount
	return rangeCount, nil
}

func validateCompletedStage4AdapterRange(
	task state.WorkTask,
	item stage4AdapterWork,
	rangeIndex int,
	workRange state.RangeState,
	restore NetworkRangeRestore,
) error {
	if rangeIndex < 0 ||
		rangeIndex >= len(item.pagination.Ranges) {
		return fmt.Errorf("completed range is outside pagination")
	}
	planned := item.pagination.Ranges[rangeIndex]
	if planned.Empty {
		if workRange.RowsDone != 0 ||
			workRange.NextSequence != 0 ||
			workRange.SequenceOffset != 0 ||
			workRange.CommittedPrefix != 0 ||
			workRange.FrontierValid ||
			len(workRange.Frontier) != 0 ||
			len(workRange.Pending) != 0 ||
			workRange.Attempts != 0 ||
			workRange.Retries != 0 ||
			task.Attempts != 0 ||
			task.Retries != 0 ||
			len(restore.Frontier) != 0 {
			return fmt.Errorf(
				"empty pagination range retains progress evidence",
			)
		}
		return nil
	}

	switch item.pagination.Strategy {
	case PaginationRowNumber:
		if planned.FirstRow < 1 ||
			planned.LastRow < planned.FirstRow {
			return fmt.Errorf(
				"ROW_NUMBER pagination interval is invalid",
			)
		}
		span := planned.LastRow - planned.FirstRow
		if span < 0 ||
			span == math.MaxInt64 {
			return fmt.Errorf(
				"ROW_NUMBER pagination span overflows",
			)
		}
		expectedRows := span + 1
		if workRange.RowsDone != expectedRows ||
			!workRange.FrontierValid ||
			len(workRange.Frontier) != 1 ||
			workRange.Frontier[0] !=
				state.Int64Value(planned.LastRow) {
			return fmt.Errorf(
				"ROW_NUMBER completion differs from its exact interval",
			)
		}
		return nil
	case PaginationIntegerKeyset, PaginationTupleKeyset:
		if workRange.RowsDone == 0 {
			if workRange.NextSequence != 0 ||
				workRange.FrontierValid ||
				len(workRange.Frontier) != 0 ||
				len(restore.Frontier) != 0 ||
				workRange.Attempts != 0 ||
				workRange.Retries != 0 {
				return fmt.Errorf(
					"empty keyset result retains progress evidence",
				)
			}
			return nil
		}
		frontier, err := stage4AdapterKeyTupleFromState(
			workRange.Frontier,
		)
		if err != nil || frontier == nil ||
			!workRange.FrontierValid {
			if err == nil {
				err = fmt.Errorf("completed keyset frontier is missing")
			}
			return err
		}
		if err := validateStage4AdapterKeyTuple(
			frontier,
			item.pagination.Keys,
		); err != nil {
			return err
		}
		if planned.Lower != nil &&
			!adapterPaginationKeyTupleAfter(
				*frontier,
				*planned.Lower,
			) {
			return fmt.Errorf(
				"completed keyset frontier does not follow its lower bound",
			)
		}
		if planned.Upper != nil &&
			adapterPaginationKeyTupleAfter(
				*frontier,
				*planned.Upper,
			) {
			return fmt.Errorf(
				"completed keyset frontier exceeds its upper bound",
			)
		}
		return nil
	default:
		return fmt.Errorf(
			"completed pagination strategy %q is unsupported",
			item.pagination.Strategy,
		)
	}
}

func (
	execution *stage4AdapterNetworkExecution,
) prevalidateCompletedTables(
	ctx context.Context,
	completed map[string]int,
) error {
	if err := execution.validateDurableTableTaskSet(ctx); err != nil {
		return err
	}
	execution.mu.Lock()
	rangeCount := execution.nextGlobalRange
	execution.mu.Unlock()
	for planIndex, plan := range execution.prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		tableRangeCount, err := execution.validateCompletedTable(
			ctx,
			planIndex,
			rows,
			false,
		)
		if err != nil {
			return err
		}
		if tableRangeCount >
			maximumRuntimeTuningRanges-rangeCount {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 completed global network range inventory is unbounded",
				),
			)
		}
		rangeCount += tableRangeCount
	}
	return nil
}

func (
	execution *stage4AdapterNetworkExecution,
) validateDurableTableTaskSet(ctx context.Context) error {
	if execution == nil || !execution.deferred {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 durable table-task inventory is unavailable",
			),
		)
	}
	inventory, err := loadStage4WorkInventory(
		ctx,
		execution.prepared.run,
	)
	if err != nil {
		return err
	}
	return execution.validateDurableTableTaskInventory(inventory)
}

func (
	execution *stage4AdapterNetworkExecution,
) validateDurableTableTaskInventory(
	inventory stage4WorkInventory,
) error {
	expected := make(
		map[state.TaskKey]struct{},
		len(execution.prepared.work),
	)
	for _, item := range execution.prepared.work {
		expected[item.task] = struct{}{}
	}
	for key := range inventory.tasks {
		if !stage4AdapterDurableTableTaskType(key.Type) {
			continue
		}
		if _, ok := expected[key]; !ok {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"unexpected stale Stage 4 table work task %#v before resume",
					key,
				),
			)
		}
	}
	for key, ranges := range inventory.ranges {
		if len(ranges) == 0 ||
			!stage4AdapterDurableTableTaskType(key.Type) {
			continue
		}
		if _, ok := expected[key]; !ok {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"unexpected stale Stage 4 table work range for task %#v before resume",
					key,
				),
			)
		}
		if _, taskFound := inventory.tasks[key]; !taskFound {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"orphaned Stage 4 table work ranges exist for task %#v before resume",
					key,
				),
			)
		}
	}
	return nil
}
