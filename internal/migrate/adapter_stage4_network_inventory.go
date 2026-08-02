package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/johndauphine/dmtx/internal/state"
)

// Durable network inventory admission, and the page writer that turns an
// issued range into rows on the target.

func admitStage4AdapterNetworkInventory(
	prepared stage4AdapterPrepared,
	sourceEngine string,
	stableSource bool,
) ([]stage4AdapterNetworkRange, []networkStateRangeBinding, error) {
	if len(prepared.plans) == 0 {
		if len(prepared.work) != 0 || prepared.network != nil {
			return nil, nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("empty Stage 4 transfer carries network work"),
			)
		}
		return []stage4AdapterNetworkRange{},
			[]networkStateRangeBinding{},
			nil
	}
	if len(prepared.work) != len(prepared.plans) ||
		prepared.network == nil {
		return nil, nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network inventory is incomplete"),
		)
	}
	var count uint64
	for index, work := range prepared.work {
		plan := prepared.plans[index]
		if work.task.Schema != plan.source.Schema ||
			work.task.Table != plan.source.Name ||
			work.task.Type != stage4AdapterNetworkTaskType ||
			work.strategy != stage4AdapterCopyStrategy ||
			work.topology == "" ||
			!validNetworkFactToken(work.topology) ||
			len(work.ranges) == 0 ||
			len(work.ranges) != len(work.pagination.Ranges) {
			return nil, nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network work differs from table plan %d",
					index,
				),
			)
		}
		switch work.pagination.Strategy {
		case PaginationIntegerKeyset:
			if len(work.pagination.Keys) != 1 ||
				work.pagination.Keys[0].Kind != KeyInteger {
				return nil, nil, NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 integer keyset requires one exact key",
					),
				)
			}
		case PaginationTupleKeyset:
			if len(work.pagination.Keys) < 1 {
				return nil, nil, NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 source pagination %q has no bounded network reader",
						work.pagination.Strategy,
					),
				)
			}
			for _, key := range work.pagination.Keys {
				if key.Kind != KeyInteger &&
					key.Kind != KeyBytes {
					return nil, nil, NewTransferError(
						ErrorClassPolicy,
						fmt.Errorf(
							"Stage 4 tuple pagination key %q has no exact bounded network order",
							key.Name,
						),
					)
				}
			}
		case PaginationRowNumber:
			if !stableSource {
				return nil, nil, NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 ROW_NUMBER pagination requires one table-stable source view",
					),
				)
			}
		default:
			return nil, nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 source pagination %q has no bounded network reader",
					work.pagination.Strategy,
				),
			)
		}
		if uint64(len(work.ranges)) >
			maximumRuntimeTuningRanges-count {
			return nil, nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("Stage 4 network range inventory is unbounded"),
			)
		}
		count += uint64(len(work.ranges))
	}
	if count == 0 ||
		count != uint64(len(prepared.network.bindings)) ||
		len(prepared.network.byIndex) != len(prepared.network.bindings) {
		return nil, nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 global network range inventory is inconsistent",
			),
		)
	}

	ranges := make([]stage4AdapterNetworkRange, 0, int(count))
	bindings := make([]networkStateRangeBinding, 0, int(count))
	var global uint64
	for planIndex, work := range prepared.work {
		for localIndex, planned := range work.pagination.Ranges {
			expectedRange, err := stage4AdapterStateRange(
				planned,
				work.topology,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"validate Stage 4 network range %d for table %s: %w",
					localIndex,
					work.task.Table,
					err,
				)
			}
			if !reflect.DeepEqual(work.ranges[localIndex], expectedRange) {
				return nil, nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 range %d for table %s differs from immutable pagination",
						localIndex,
						work.task.Table,
					),
				)
			}
			expected := networkStateRangeBinding{
				RangeIndex: global,
				Task:       work.task,
				Initial:    expectedRange,
			}
			actual := prepared.network.bindings[global]
			indexed, exists := prepared.network.byIndex[global]
			if !exists ||
				!reflect.DeepEqual(actual, expected) ||
				!reflect.DeepEqual(indexed, expected) {
				return nil, nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 global range %d changed after admission",
						global,
					),
				)
			}
			ranges = append(ranges, stage4AdapterNetworkRange{
				globalIndex: global,
				planIndex:   planIndex,
				localIndex:  localIndex,
				plan: cloneStage4AdapterNetworkTablePlan(
					prepared.plans[planIndex],
				),
				work:      cloneStage4AdapterNetworkWork(work),
				rangePlan: clonePaginationRange(planned),
				durable: networkStateRangeBinding{
					RangeIndex: expected.RangeIndex,
					Task:       expected.Task,
					Initial: cloneInitialNetworkStateRange(
						expected.Initial,
					),
				},
			})
			bindings = append(bindings, networkStateRangeBinding{
				RangeIndex: expected.RangeIndex,
				Task:       expected.Task,
				Initial: cloneInitialNetworkStateRange(
					expected.Initial,
				),
			})
			global++
		}
	}
	return ranges, bindings, nil
}

func cloneStage4AdapterNetworkTablePlan(
	value adapterTablePlan,
) adapterTablePlan {
	return adapterTablePlan{
		source:  cloneStage4RichTable(value.source),
		target:  cloneStage4RichTable(value.target),
		columns: append([]string(nil), value.columns...),
	}
}

func cloneStage4AdapterNetworkWork(
	value stage4AdapterWork,
) stage4AdapterWork {
	result := value
	result.ranges = make([]state.RangeState, len(value.ranges))
	for index := range value.ranges {
		result.ranges[index] = cloneInitialNetworkStateRange(
			value.ranges[index],
		)
	}
	result.pagination = cloneStage4AdapterPagination(value.pagination)
	return result
}

func exactStage4AdapterNetworkRange(
	ranges []stage4AdapterNetworkRange,
	request NetworkRangePlan,
) (stage4AdapterNetworkRange, error) {
	if len(ranges) == 0 ||
		request.RangeIndex < ranges[0].globalIndex {
		return stage4AdapterNetworkRange{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 callback references an unknown global range"),
		)
	}
	localIndex := request.RangeIndex - ranges[0].globalIndex
	if localIndex >= uint64(len(ranges)) {
		return stage4AdapterNetworkRange{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 callback references an unknown global range"),
		)
	}
	binding := ranges[localIndex]
	if request.RangeIndex != binding.globalIndex ||
		request.TableSchema != binding.plan.source.Schema ||
		request.TableName != binding.plan.source.Name ||
		request.TopologyHash != binding.work.topology ||
		request.Pagination != binding.work.pagination.Strategy ||
		request.MaxRowBytes != binding.maxRowBytes {
		return stage4AdapterNetworkRange{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 callback range differs from immutable admission",
			),
		)
	}
	return binding, nil
}

func writeStage4AdapterNetworkPage(
	ctx context.Context,
	target targetAdapter,
	ranges []stage4AdapterNetworkRange,
	replayMode NetworkReplayMode,
	request NetworkWriteRequest,
) (WriteReceipt, error) {
	failed := networkStateFailedReceipt(request)
	switch replayMode {
	case NetworkReplayIdempotentUpsert:
		if request.Mode != NetworkWriteIdempotentUpsert {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 upsert network plan received incompatible write mode %q",
					request.Mode,
				),
			)
		}
	case NetworkReplayDuplicateSafeInsertOnly:
		if request.Mode != NetworkWriteFreshInsert &&
			request.Mode != NetworkWriteDuplicateSafeInsertOnly {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 rebuild network plan received incompatible write mode %q",
					request.Mode,
				),
			)
		}
	default:
		return failed, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network plan has invalid replay mode %q",
				replayMode,
			),
		)
	}
	binding, err := exactStage4AdapterNetworkRange(
		ranges,
		request.Range,
	)
	if err != nil {
		return failed, err
	}
	var (
		receipt  WriteReceipt
		writeErr error
	)
	switch request.Mode {
	case NetworkWriteIdempotentUpsert:
		upsertTarget, ok := target.(adapterStage4NetworkUpsertTarget)
		if !ok || isNilInterface(upsertTarget) {
			return failed, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 network target rejected idempotent upsert without a certified writer",
				),
			)
		}
		receipt, writeErr = upsertTarget.WriteStage4NetworkBatch(
			ctx,
			binding.plan.target,
			binding.plan.columns,
			request.Rows,
		)
	case NetworkWriteFreshInsert, NetworkWriteDuplicateSafeInsertOnly:
		rebuildTarget, ok := target.(adapterStage4NetworkRebuildTarget)
		if !ok || isNilInterface(rebuildTarget) {
			return failed, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 network target rejected rebuild write mode %q without a certified duplicate-safe writer",
					request.Mode,
				),
			)
		}
		receipt, writeErr = rebuildTarget.WriteStage4NetworkRebuildBatch(
			ctx,
			binding.plan.target,
			binding.plan.columns,
			request.Mode,
			request.Rows,
		)
	default:
		return failed, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network target rejected write mode %q",
				request.Mode,
			),
		)
	}
	if receiptErr := receipt.Validate(); receiptErr != nil ||
		receipt.AttemptOffset != 0 ||
		receipt.AttemptedRows != int64(len(request.Rows)) {
		cause := receiptErr
		if cause == nil {
			cause = ErrInvalidWriteReceipt
		}
		if writeErr != nil {
			cause = errors.Join(cause, writeErr)
		}
		return failed, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 target returned an invalid local write receipt: %w",
				cause,
			),
		)
	}
	receipt.AttemptOffset = request.AttemptOffset
	return receipt, writeErr
}

func durableStage4AdapterNetworkTotals(
	ctx context.Context,
	coordinator *networkStateCoordinator,
	ranges []stage4AdapterNetworkRange,
	tableCount int,
) ([]int, int64, error) {
	snapshot, err := coordinator.loadSnapshot(ctx)
	if err != nil {
		return nil, 0, err
	}
	totals := make([]int64, tableCount)
	aggregate := int64(0)
	for _, binding := range ranges {
		workRange, exactErr := snapshot.exact(binding.durable)
		if exactErr != nil {
			return nil, 0, exactErr
		}
		restore, restoreErr := networkRestoreFromState(
			binding.durable,
			workRange,
		)
		if restoreErr != nil || !restore.Complete ||
			restore.SequenceOffset != 0 ||
			len(restore.Issued) != 0 {
			if restoreErr == nil {
				restoreErr = fmt.Errorf(
					"range is not durably complete",
				)
			}
			return nil, 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"read durable Stage 4 total for global range %d: %w",
					binding.globalIndex,
					restoreErr,
				),
			)
		}
		if restore.RowsDone > math.MaxInt64-totals[binding.planIndex] ||
			restore.RowsDone > math.MaxInt64-aggregate {
			return nil, 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 durable row total overflows"),
			)
		}
		totals[binding.planIndex] += restore.RowsDone
		aggregate += restore.RowsDone
	}
	result := make([]int, len(totals))
	for index, total := range totals {
		converted := int(total)
		if int64(converted) != total {
			return nil, 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 durable row total for table %d exceeds platform int",
					index,
				),
			)
		}
		result[index] = converted
	}
	return result, aggregate, nil
}
