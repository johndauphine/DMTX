package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4AdapterAnalyticalCopyStrategy = "stage4_adapter_analytical_copy_v1"
	stage4AdapterAnalyticalCopyRangeID  = "full-copy"
)

// bindStage4AdapterPagination turns source-approved pagination into the exact
// state topology used by the durable network coordinator. Target/configuration
// identity is folded into the pagination digest so a range plan can never be
// reused for a different target projection or lifecycle mode.
func bindStage4AdapterPagination(
	ctx context.Context,
	source sourceAdapter,
	requestedPartitions int,
	work []stage4AdapterWork,
	plans []adapterTablePlan,
) ([]stage4AdapterWork, error) {
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 pagination context is required"),
		)
	}
	if len(work) != len(plans) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 pagination work inventory is inconsistent"),
		)
	}
	if len(work) == 0 {
		return []stage4AdapterWork{}, nil
	}
	if requestedPartitions == 0 {
		requestedPartitions = config.DefaultPartitions
	}
	if requestedPartitions < 1 ||
		uint64(requestedPartitions) > maximumRuntimeTuningRanges {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 pagination partition count %d is outside 1..%d",
				requestedPartitions,
				maximumRuntimeTuningRanges,
			),
		)
	}
	if source == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 pagination source is required"),
		)
	}
	if source.Engine() == "clickhouse" {
		return bindStage4AnalyticalWork(work), nil
	}
	planner, err := requirePaginationSourceAdapter(source)
	if err != nil {
		return nil, fmt.Errorf(
			"admit Stage 4 relational source pagination: %w",
			err,
		)
	}

	result := make([]stage4AdapterWork, len(work))
	for index, item := range work {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pagination, err := planner.PlanPagination(
			ctx,
			plans[index].source,
			requestedPartitions,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"plan Stage 4 network ranges for %s: %w",
				plans[index].source.Name,
				err,
			)
		}
		if err := validateStage4AdapterPagination(
			source.Engine(),
			plans[index].source,
			requestedPartitions,
			pagination,
		); err != nil {
			return nil, fmt.Errorf(
				"admit Stage 4 network ranges for %s: %w",
				plans[index].source.Name,
				err,
			)
		}
		topology, err := stage4AdapterNetworkTopology(
			item.topology,
			requestedPartitions,
			pagination,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"bind Stage 4 network topology for %s: %w",
				plans[index].source.Name,
				err,
			)
		}
		item.strategy = stage4AdapterCopyStrategy
		item.topology = topology
		item.pagination = cloneStage4AdapterPagination(pagination)
		item.ranges = make(
			[]state.RangeState,
			len(pagination.Ranges),
		)
		for rangeIndex, planned := range pagination.Ranges {
			workRange, err := stage4AdapterStateRange(
				planned,
				topology,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"bind Stage 4 network range %d for %s: %w",
					rangeIndex,
					plans[index].source.Name,
					err,
				)
			}
			item.ranges[rangeIndex] = workRange
		}
		result[index] = item
	}
	return result, nil
}

func bindStage4AnalyticalWork(
	work []stage4AdapterWork,
) []stage4AdapterWork {
	result := make([]stage4AdapterWork, len(work))
	for index, item := range work {
		item.task.Type = "analytical-table-copy"
		item.strategy = stage4AdapterAnalyticalCopyStrategy
		item.pagination = PaginationPlan{}
		item.ranges = []state.RangeState{{
			ID:           stage4AdapterAnalyticalCopyRangeID,
			Strategy:     item.strategy,
			TopologyHash: item.topology,
		}}
		result[index] = item
	}
	return result
}

func validateStage4AdapterPagination(
	sourceEngine string,
	table schema.Table,
	requestedPartitions int,
	plan PaginationPlan,
) error {
	if !validNetworkPagination(plan.Strategy) ||
		!validNetworkFactToken(plan.TopologyHash) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("pagination strategy or topology hash is invalid"),
		)
	}
	if len(plan.Ranges) == 0 ||
		uint64(len(plan.Ranges)) > maximumRuntimeTuningRanges {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("pagination range inventory is empty or unbounded"),
		)
	}
	keys, err := adapterPaginationPrimaryKey(
		sourceEngine,
		table.Schema,
		table,
	)
	if err != nil {
		return err
	}
	if len(plan.Keys) == 0 || len(plan.Keys) != len(keys) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"pagination key inventory differs from source primary key",
			),
		)
	}
	seenKeys := make(map[string]struct{}, len(plan.Keys))
	evidence := make(
		[]adapterPaginationKeyEvidence,
		len(keys),
	)
	for index, key := range plan.Keys {
		if err := validateAdapterPaginationIdentifier(
			sourceEngine,
			"pagination key",
			key.Name,
		); err != nil {
			return err
		}
		if _, duplicate := seenKeys[key.Name]; duplicate {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("pagination key %q is duplicated", key.Name),
			)
		}
		seenKeys[key.Name] = struct{}{}
		expectedKind := adapterPaginationKeyKind(
			sourceEngine,
			keys[index],
		)
		if key.Name != keys[index].Name ||
			key.Kind != expectedKind {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"pagination key %d differs from source primary-key order",
					index,
				),
			)
		}
		switch key.Kind {
		case KeyInteger, KeyText, KeyBytes:
		case "":
			if plan.Strategy != PaginationRowNumber {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"pagination key %q has no restorable key kind",
						key.Name,
					),
				)
			}
		default:
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"pagination key %q has unsupported kind %q",
					key.Name,
					key.Kind,
				),
			)
		}
		evidence[index] = adapterPaginationKeyEvidence{
			Name:     keys[index].Name,
			Type:     keys[index].Type,
			Nullable: keys[index].Nullable,
			Position: keys[index].PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(
				keys[index].DeclaredType,
			),
		}
	}
	if plan.Strategy != adapterPaginationStrategy(
		sourceEngine,
		table,
		keys,
	) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"pagination strategy differs from source key evidence",
			),
		)
	}
	expectedTopology, err := adapterPaginationTopologyHash(
		sourceEngine,
		table,
		requestedPartitions,
		evidence,
		plan,
	)
	if err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	if plan.TopologyHash != expectedTopology {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"pagination topology hash differs from approved source plan",
			),
		)
	}

	empty := false
	var previousUpper *KeyTuple
	var previousLast int64
	for index, planned := range plan.Ranges {
		if planned.ID != index {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"pagination range IDs must be contiguous from zero",
				),
			)
		}
		if planned.Empty {
			if len(plan.Ranges) != 1 || index != 0 ||
				planned.Lower != nil || planned.Upper != nil ||
				planned.LastRow != 0 ||
				(plan.Strategy == PaginationRowNumber &&
					planned.FirstRow != 1) ||
				(plan.Strategy != PaginationRowNumber &&
					planned.FirstRow != 0) {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf("empty pagination range is malformed"),
				)
			}
			empty = true
			continue
		}
		switch plan.Strategy {
		case PaginationIntegerKeyset, PaginationTupleKeyset:
			for _, key := range plan.Keys {
				if key.Kind != KeyInteger {
					return NewTransferError(
						ErrorClassPolicy,
						fmt.Errorf(
							"keyset pagination requires exact signed-integer keys",
						),
					)
				}
			}
			if planned.FirstRow != 0 || planned.LastRow != 0 ||
				planned.Upper == nil ||
				index == 0 && planned.Lower != nil ||
				index > 0 && planned.Lower == nil {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"keyset pagination range %d is malformed",
						index,
					),
				)
			}
			if index > 0 &&
				!stage4AdapterKeyTupleEqual(
					planned.Lower,
					previousUpper,
				) {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"keyset pagination range %d is not contiguous",
						index,
					),
				)
			}
			if err := validateStage4AdapterKeyTuple(
				planned.Upper,
				plan.Keys,
			); err != nil {
				return fmt.Errorf(
					"pagination range %d upper bound: %w",
					index,
					err,
				)
			}
			if planned.Lower != nil {
				if err := validateStage4AdapterKeyTuple(
					planned.Lower,
					plan.Keys,
				); err != nil {
					return fmt.Errorf(
						"pagination range %d lower bound: %w",
						index,
						err,
					)
				}
			}
			if previousUpper != nil &&
				!stage4AdapterIntegerTupleAfter(
					planned.Upper,
					previousUpper,
				) {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"keyset pagination range %d does not advance",
						index,
					),
				)
			}
			upper := append(KeyTuple(nil), (*planned.Upper)...)
			previousUpper = &upper
		case PaginationRowNumber:
			if planned.Lower != nil || planned.Upper != nil ||
				planned.FirstRow < 1 ||
				planned.LastRow < planned.FirstRow ||
				index == 0 && planned.FirstRow != 1 ||
				index > 0 && planned.FirstRow != previousLast+1 {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"row-number pagination range %d is malformed",
						index,
					),
				)
			}
			previousLast = planned.LastRow
		}
	}
	if empty && len(plan.Ranges) != 1 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("empty pagination inventory contains extra ranges"),
		)
	}
	return nil
}

func stage4AdapterIntegerTupleAfter(
	value *KeyTuple,
	previous *KeyTuple,
) bool {
	if value == nil || previous == nil ||
		len(*value) != len(*previous) {
		return false
	}
	for index := range *value {
		current, currentErr := strconv.ParseInt(
			(*value)[index].Encoded,
			10,
			64,
		)
		prior, priorErr := strconv.ParseInt(
			(*previous)[index].Encoded,
			10,
			64,
		)
		if currentErr != nil || priorErr != nil {
			return false
		}
		switch {
		case current > prior:
			return true
		case current < prior:
			return false
		}
	}
	return false
}

func validateStage4AdapterKeyTuple(
	tuple *KeyTuple,
	keys []KeySpec,
) error {
	if tuple == nil || len(*tuple) != len(keys) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("key tuple width differs from pagination keys"),
		)
	}
	for index, value := range *tuple {
		if value.Kind != keys[index].Kind {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"key tuple kind %q differs from %q",
					value.Kind,
					keys[index].Kind,
				),
			)
		}
		if _, err := stage4AdapterStateValue(value); err != nil {
			return err
		}
	}
	return nil
}

func stage4AdapterKeyTupleEqual(
	left *KeyTuple,
	right *KeyTuple,
) bool {
	if left == nil || right == nil || len(*left) != len(*right) {
		return false
	}
	for index := range *left {
		if (*left)[index] != (*right)[index] {
			return false
		}
	}
	return true
}

func stage4AdapterNetworkTopology(
	base string,
	requestedPartitions int,
	pagination PaginationPlan,
) (string, error) {
	wire := struct {
		Version             int            `json:"version"`
		BaseTopology        string         `json:"base_topology"`
		RequestedPartitions int            `json:"requested_partitions"`
		Pagination          PaginationPlan `json:"pagination"`
	}{
		Version:             1,
		BaseTopology:        base,
		RequestedPartitions: requestedPartitions,
		Pagination:          pagination,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stage4AdapterStateRange(
	planned PaginationRange,
	topology string,
) (state.RangeState, error) {
	result := state.RangeState{
		ID:             "range/" + strconv.Itoa(planned.ID),
		Strategy:       stage4AdapterCopyStrategy,
		TopologyHash:   topology,
		UpperInclusive: planned.Upper != nil,
		FirstRow:       planned.FirstRow,
		LastRow:        planned.LastRow,
	}
	var err error
	if planned.Lower != nil {
		result.Lower, err = stage4AdapterStateTuple(*planned.Lower)
		if err != nil {
			return state.RangeState{}, fmt.Errorf("lower bound: %w", err)
		}
		if _, err := encodeNetworkStateFrontier(
			result.Lower,
			true,
		); err != nil {
			return state.RangeState{}, fmt.Errorf(
				"lower bound is not a network frontier: %w",
				err,
			)
		}
	}
	if planned.Upper != nil {
		result.Upper, err = stage4AdapterStateTuple(*planned.Upper)
		if err != nil {
			return state.RangeState{}, fmt.Errorf("upper bound: %w", err)
		}
		if _, err := encodeNetworkStateFrontier(
			result.Upper,
			true,
		); err != nil {
			return state.RangeState{}, fmt.Errorf(
				"upper bound is not a network frontier: %w",
				err,
			)
		}
	}
	return result, nil
}

func stage4AdapterStateTuple(
	tuple KeyTuple,
) (state.TypedTuple, error) {
	result := make(state.TypedTuple, len(tuple))
	for index, value := range tuple {
		converted, err := stage4AdapterStateValue(value)
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", index, err)
		}
		result[index] = converted
	}
	if err := result.Validate(false); err != nil {
		return nil, err
	}
	return result, nil
}

func stage4AdapterStateValue(
	value KeyValue,
) (state.TypedValue, error) {
	switch value.Kind {
	case KeyInteger:
		decoded, err := value.SQLValue()
		if err != nil {
			return state.TypedValue{}, err
		}
		integer, ok := decoded.(int64)
		if !ok {
			return state.TypedValue{}, fmt.Errorf(
				"integer key decoded as %T",
				decoded,
			)
		}
		if strconv.FormatInt(integer, 10) != value.Encoded {
			return state.TypedValue{}, fmt.Errorf(
				"integer key encoding is non-canonical",
			)
		}
		return state.Int64Value(integer), nil
	case KeyText:
		if !utf8.ValidString(value.Encoded) {
			return state.TypedValue{}, fmt.Errorf(
				"text key contains invalid UTF-8",
			)
		}
		return state.TextValue(value.Encoded), nil
	case KeyBytes:
		decoded, err := value.SQLValue()
		if err != nil {
			return state.TypedValue{}, err
		}
		bytesValue, ok := decoded.([]byte)
		if !ok {
			return state.TypedValue{}, fmt.Errorf(
				"byte key decoded as %T",
				decoded,
			)
		}
		converted := state.BytesValue(bytesValue)
		if converted.Encoded != value.Encoded {
			return state.TypedValue{}, fmt.Errorf(
				"byte key encoding is non-canonical",
			)
		}
		return converted, nil
	default:
		return state.TypedValue{}, fmt.Errorf(
			"unsupported key kind %q",
			value.Kind,
		)
	}
}

func cloneStage4AdapterPagination(
	value PaginationPlan,
) PaginationPlan {
	result := value
	result.Keys = append([]KeySpec(nil), value.Keys...)
	result.Ranges = make([]PaginationRange, len(value.Ranges))
	for index, planned := range value.Ranges {
		result.Ranges[index] = planned
		if planned.Lower != nil {
			lower := append(KeyTuple(nil), (*planned.Lower)...)
			result.Ranges[index].Lower = &lower
		}
		if planned.Upper != nil {
			upper := append(KeyTuple(nil), (*planned.Upper)...)
			result.Ranges[index].Upper = &upper
		}
	}
	return result
}

func newStage4AdapterNetworkCoordinator(
	run Stage4RunContext,
	work []stage4AdapterWork,
) (*networkStateCoordinator, error) {
	if len(work) == 0 {
		return nil, nil
	}
	var rangeCount uint64
	for _, item := range work {
		if len(item.ranges) == 0 {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network work for %s has no ranges",
					item.task.Table,
				),
			)
		}
		if uint64(len(item.ranges)) >
			maximumRuntimeTuningRanges-rangeCount {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("Stage 4 network range inventory is unbounded"),
			)
		}
		rangeCount += uint64(len(item.ranges))
	}
	bindings := make(
		[]networkStateRangeBinding,
		0,
		int(rangeCount),
	)
	var rangeIndex uint64
	for _, item := range work {
		for _, workRange := range item.ranges {
			initial := workRange
			initial.Strategy = item.strategy
			initial.TopologyHash = item.topology
			bindings = append(bindings, networkStateRangeBinding{
				RangeIndex: rangeIndex,
				Task:       item.task,
				Initial:    initial,
			})
			rangeIndex++
		}
	}
	coordinator, err := newNetworkStateCoordinator(run, bindings)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("bind Stage 4 network state: %w", err),
		)
	}
	return coordinator, nil
}

func exactStage4AdapterWork(
	inventory stage4WorkInventory,
	item stage4AdapterWork,
	allowMissing bool,
) (state.WorkTask, []state.RangeState, bool, error) {
	task, found := inventory.tasks[item.task]
	persisted := inventory.ranges[item.task]
	if !found {
		if len(persisted) != 0 {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"orphaned Stage 4 network ranges exist for missing task %#v",
					item.task,
				),
			)
		}
		if allowMissing {
			return state.WorkTask{}, nil, false, nil
		}
		return state.WorkTask{}, nil, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("missing Stage 4 work task %#v", item.task),
		)
	}
	if task.Strategy != item.strategy ||
		task.TopologyHash != item.topology {
		return state.WorkTask{}, nil, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work topology changed for task %#v",
				item.task,
			),
		)
	}
	if task.Status != "running" && task.Status != "completed" ||
		task.Attempts < 0 || task.Retries < 0 {
		return state.WorkTask{}, nil, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work task %#v has unsafe status or counters",
				item.task,
			),
		)
	}
	if len(persisted) != len(item.ranges) {
		return state.WorkTask{}, nil, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work task %#v has an unsafe range set",
				item.task,
			),
		)
	}
	byID := make(map[string]state.RangeState, len(persisted))
	for _, workRange := range persisted {
		if _, duplicate := byID[workRange.ID]; duplicate {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 network range %q for task %#v",
					workRange.ID,
					item.task,
				),
			)
		}
		byID[workRange.ID] = workRange
	}
	result := make([]state.RangeState, len(item.ranges))
	for index, initial := range item.ranges {
		workRange, ok := byID[initial.ID]
		if !ok || !stage4AdapterRangeTopologyEqual(
			initial,
			workRange,
		) {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work topology changed for range %q of task %#v",
					initial.ID,
					item.task,
				),
			)
		}
		if task.Status == "completed" &&
			workRange.Status != "completed" {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 work task has non-completed range %q",
					initial.ID,
				),
			)
		}
		if workRange.Status != "running" &&
			workRange.Status != "completed" {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work range %q has unsafe status %q",
					initial.ID,
					workRange.Status,
				),
			)
		}
		if workRange.Attempts < 0 || workRange.Retries < 0 {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work range %q has negative attempt counters",
					initial.ID,
				),
			)
		}
		if _, err := networkRestoreFromState(
			networkStateRangeBinding{
				RangeIndex: uint64(index),
				Task:       item.task,
				Initial:    initial,
			},
			workRange,
		); err != nil {
			return state.WorkTask{}, nil, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work range %q contains unsafe network evidence: %w",
					initial.ID,
					err,
				),
			)
		}
		result[index] = workRange
		delete(byID, initial.ID)
	}
	if len(byID) != 0 {
		return state.WorkTask{}, nil, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work task %#v contains unexpected ranges",
				item.task,
			),
		)
	}
	return task, result, true, nil
}

func stage4AdapterRangeTopologyEqual(
	initial state.RangeState,
	persisted state.RangeState,
) bool {
	return initial.ID == persisted.ID &&
		initial.Strategy == persisted.Strategy &&
		initial.TopologyHash == persisted.TopologyHash &&
		initial.LowerInclusive == persisted.LowerInclusive &&
		initial.UpperInclusive == persisted.UpperInclusive &&
		initial.FirstRow == persisted.FirstRow &&
		initial.LastRow == persisted.LastRow &&
		initial.RowsTotal == persisted.RowsTotal &&
		stage4AdapterStateTupleEqual(initial.Lower, persisted.Lower) &&
		stage4AdapterStateTupleEqual(initial.Upper, persisted.Upper)
}

func stage4AdapterStateTupleEqual(
	left state.TypedTuple,
	right state.TypedTuple,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func completeStage4AdapterWorkItem(
	ctx context.Context,
	run Stage4RunContext,
	item stage4AdapterWork,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		return err
	}
	task, ranges, _, err := exactStage4AdapterWork(
		inventory,
		item,
		false,
	)
	if err != nil {
		return err
	}
	if task.Status == "completed" {
		return nil
	}
	now := time.Now().UTC()
	for _, workRange := range ranges {
		if workRange.Status == "completed" {
			continue
		}
		if len(workRange.Pending) != 0 ||
			workRange.SequenceOffset != 0 {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work range %q retains unresolved network chunks",
					workRange.ID,
				),
			)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := run.Backend.CompleteRange(
			run.RunID,
			item.task,
			workRange.ID,
			item.topology,
			workRange.NextSequence,
			now,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"complete Stage 4 network range %q: %w",
					workRange.ID,
					err,
				),
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run.Backend.CompleteWorkTask(
		run.RunID,
		item.task,
		item.topology,
		now,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("complete Stage 4 network task: %w", err),
		)
	}
	return nil
}
