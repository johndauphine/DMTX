package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// networkStateRangeBinding maps one migration-global network range to the
// durable task/range identity used by both state backends. Initial must contain
// only immutable range topology; progress fields are always loaded from state.
type networkStateRangeBinding struct {
	RangeIndex uint64
	Task       state.TaskKey
	Initial    state.RangeState
}

// networkStateCoordinator is the exact durability bridge for the engine-
// neutral network core. It preserves issued-page replay evidence, records
// target attempt authorization before the driver call, and advances state only
// to the core's lowest contiguous acknowledgement frontier.
type networkStateCoordinator struct {
	run      Stage4RunContext
	bindings []networkStateRangeBinding
	byIndex  map[uint64]networkStateRangeBinding
	now      func() time.Time
}

func newNetworkStateCoordinator(
	run Stage4RunContext,
	bindings []networkStateRangeBinding,
) (*networkStateCoordinator, error) {
	if err := run.Validate(); err != nil {
		return nil, fmt.Errorf("network state context: %w", err)
	}
	if len(bindings) == 0 {
		return nil, fmt.Errorf("network state bindings are required")
	}
	result := &networkStateCoordinator{
		run:      run,
		bindings: make([]networkStateRangeBinding, len(bindings)),
		byIndex:  make(map[uint64]networkStateRangeBinding, len(bindings)),
		now:      func() time.Time { return time.Now().UTC() },
	}
	copy(result.bindings, bindings)
	sort.Slice(result.bindings, func(left, right int) bool {
		return result.bindings[left].RangeIndex <
			result.bindings[right].RangeIndex
	})
	taskTopology := make(map[state.TaskKey]state.RangeState)
	for index, binding := range result.bindings {
		if binding.RangeIndex != uint64(index) {
			return nil, fmt.Errorf(
				"network state range indexes must be contiguous from zero",
			)
		}
		if err := binding.Task.Validate(); err != nil {
			return nil, fmt.Errorf(
				"network state range %d task: %w",
				binding.RangeIndex,
				err,
			)
		}
		if err := validateInitialNetworkStateRange(binding.Initial); err != nil {
			return nil, fmt.Errorf(
				"network state range %d: %w",
				binding.RangeIndex,
				err,
			)
		}
		if prior, ok := taskTopology[binding.Task]; ok &&
			(prior.Strategy != binding.Initial.Strategy ||
				prior.TopologyHash != binding.Initial.TopologyHash) {
			return nil, fmt.Errorf(
				"network state task %#v has inconsistent topology",
				binding.Task,
			)
		}
		taskTopology[binding.Task] = binding.Initial
		binding.Initial = cloneInitialNetworkStateRange(binding.Initial)
		result.bindings[index] = binding
		result.byIndex[binding.RangeIndex] = binding
	}
	return result, nil
}

func validateInitialNetworkStateRange(value state.RangeState) error {
	if value.ID == "" || value.Strategy == "" ||
		!validNetworkFactToken(value.TopologyHash) {
		return fmt.Errorf(
			"range ID, strategy, and bounded topology hash are required",
		)
	}
	if len(value.Lower) != 0 {
		if err := value.Lower.Validate(false); err != nil {
			return fmt.Errorf("lower bound: %w", err)
		}
	}
	if len(value.Upper) != 0 {
		if err := value.Upper.Validate(false); err != nil {
			return fmt.Errorf("upper bound: %w", err)
		}
	}
	if value.RunID != "" || value.Task != (state.TaskKey{}) ||
		value.Status != "" || value.NextSequence != 0 ||
		value.SequenceOffset != 0 || value.RowsDone != 0 ||
		value.RowsTotal != 0 || value.CommittedPrefix != 0 ||
		value.Attempts != 0 || value.Retries != 0 ||
		value.Error != "" || len(value.Frontier) != 0 ||
		value.FrontierValid || len(value.Pending) != 0 ||
		!value.UpdatedAt.IsZero() || !value.CompletedAt.IsZero() {
		return fmt.Errorf("initial range contains mutable progress evidence")
	}
	return nil
}

func cloneInitialNetworkStateRange(
	value state.RangeState,
) state.RangeState {
	value.Lower = append(state.TypedTuple(nil), value.Lower...)
	value.Upper = append(state.TypedTuple(nil), value.Upper...)
	return value
}

func (coordinator *networkStateCoordinator) ensurePlans(
	ctx context.Context,
) error {
	if coordinator == nil {
		return fmt.Errorf("network state coordinator is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	type taskPlan struct {
		task   state.WorkTask
		ranges []state.RangeState
	}
	plans := make(map[state.TaskKey]*taskPlan)
	keys := make([]state.TaskKey, 0)
	startedAt := coordinator.now().UTC()
	for _, binding := range coordinator.bindings {
		plan, ok := plans[binding.Task]
		if !ok {
			plan = &taskPlan{
				task: state.WorkTask{
					RunID:        coordinator.run.RunID,
					Key:          binding.Task,
					Strategy:     binding.Initial.Strategy,
					TopologyHash: binding.Initial.TopologyHash,
					StartedAt:    startedAt,
				},
			}
			plans[binding.Task] = plan
			keys = append(keys, binding.Task)
		}
		plan.ranges = append(
			plan.ranges,
			cloneInitialNetworkStateRange(binding.Initial),
		)
	}
	sort.Slice(keys, func(left, right int) bool {
		return networkTaskKeyLess(keys[left], keys[right])
	})
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		plan := plans[key]
		if _, err := coordinator.run.Backend.EnsureWorkPlan(
			plan.task,
			plan.ranges,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"ensure durable network work plan for %#v: %w",
					key,
					err,
				),
			)
		}
	}
	return ctx.Err()
}

func networkTaskKeyLess(left, right state.TaskKey) bool {
	if left.Type != right.Type {
		return left.Type < right.Type
	}
	if left.Schema != right.Schema {
		return left.Schema < right.Schema
	}
	if left.Table != right.Table {
		return left.Table < right.Table
	}
	return left.Partition < right.Partition
}

func (coordinator *networkStateCoordinator) loadRestores(
	ctx context.Context,
) ([]NetworkRangeRestore, error) {
	snapshot, err := coordinator.loadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	restores, err := coordinator.restoresFromSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	if _, err := coordinator.recoverCompletedTasks(
		ctx,
		snapshot,
	); err != nil {
		return nil, err
	}
	return restores, nil
}

func (coordinator *networkStateCoordinator) restoresFromSnapshot(
	snapshot networkStateSnapshot,
) ([]NetworkRangeRestore, error) {
	restores := make([]NetworkRangeRestore, len(coordinator.bindings))
	for index, binding := range coordinator.bindings {
		workRange, err := snapshot.exact(binding)
		if err != nil {
			return nil, err
		}
		restore, err := networkRestoreFromState(binding, workRange)
		if err != nil {
			return nil, fmt.Errorf(
				"restore durable network range %d: %w",
				binding.RangeIndex,
				err,
			)
		}
		restores[index] = restore
	}
	return restores, nil
}

// recoverCompletedTasks closes the narrow crash window between durable range
// completion and durable task completion. Without this repair, a core restore
// containing only completed ranges would correctly perform no callbacks but
// the enclosing run could never truthfully finalize its task.
func (coordinator *networkStateCoordinator) recoverCompletedTasks(
	ctx context.Context,
	snapshot networkStateSnapshot,
) (networkStateSnapshot, error) {
	byTask := make(map[state.TaskKey][]networkStateRangeBinding)
	keys := make([]state.TaskKey, 0)
	for _, binding := range coordinator.bindings {
		if _, exists := byTask[binding.Task]; !exists {
			keys = append(keys, binding.Task)
		}
		byTask[binding.Task] = append(
			byTask[binding.Task],
			binding,
		)
	}
	sort.Slice(keys, func(left, right int) bool {
		return networkTaskKeyLess(keys[left], keys[right])
	})
	recovered := false
	for _, key := range keys {
		task, found := snapshot.tasks[key]
		if !found {
			return networkStateSnapshot{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("missing durable network task %#v", key),
			)
		}
		if task.Status == "completed" {
			continue
		}
		allComplete := true
		var topology string
		for _, binding := range byTask[key] {
			workRange, err := snapshot.exact(binding)
			if err != nil {
				return networkStateSnapshot{}, err
			}
			if workRange.Status != "completed" {
				allComplete = false
				break
			}
			if _, err := networkRestoreFromState(
				binding,
				workRange,
			); err != nil {
				return networkStateSnapshot{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"validate completed durable network range %d before task recovery: %w",
						binding.RangeIndex,
						err,
					),
				)
			}
			topology = binding.Initial.TopologyHash
		}
		if !allComplete {
			continue
		}
		if err := ctx.Err(); err != nil {
			return networkStateSnapshot{}, err
		}
		if err := coordinator.run.Backend.CompleteWorkTask(
			coordinator.run.RunID,
			key,
			topology,
			coordinator.now().UTC(),
		); err != nil {
			return networkStateSnapshot{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"recover completed durable network task: %w",
					err,
				),
			)
		}
		recovered = true
	}
	if !recovered {
		return snapshot, nil
	}
	return coordinator.loadSnapshot(ctx)
}

type networkStateSnapshot struct {
	tasks  map[state.TaskKey]state.WorkTask
	ranges map[state.TaskKey]map[string]state.RangeState
}

func (coordinator *networkStateCoordinator) loadSnapshot(
	ctx context.Context,
) (networkStateSnapshot, error) {
	result := networkStateSnapshot{
		tasks:  make(map[state.TaskKey]state.WorkTask),
		ranges: make(map[state.TaskKey]map[string]state.RangeState),
	}
	if coordinator == nil {
		return result, fmt.Errorf("network state coordinator is nil")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	tasks, ranges, err := coordinator.run.Backend.ListWork(
		coordinator.run.RunID,
	)
	if err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("load durable network work: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for _, task := range tasks {
		if _, duplicate := result.tasks[task.Key]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf("duplicate durable network task %#v", task.Key),
			)
		}
		result.tasks[task.Key] = task
	}
	for _, workRange := range ranges {
		byID := result.ranges[workRange.Task]
		if byID == nil {
			byID = make(map[string]state.RangeState)
			result.ranges[workRange.Task] = byID
		}
		if _, duplicate := byID[workRange.ID]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate durable network range %q for %#v",
					workRange.ID,
					workRange.Task,
				),
			)
		}
		byID[workRange.ID] = workRange
	}
	return result, nil
}

func (snapshot networkStateSnapshot) exact(
	binding networkStateRangeBinding,
) (state.RangeState, error) {
	task, found := snapshot.tasks[binding.Task]
	if !found {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("missing durable network task %#v", binding.Task),
		)
	}
	workRange, found := snapshot.ranges[binding.Task][binding.Initial.ID]
	if !found {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"missing durable network range %q for %#v",
				binding.Initial.ID,
				binding.Task,
			),
		)
	}
	if task.Strategy != binding.Initial.Strategy ||
		task.TopologyHash != binding.Initial.TopologyHash ||
		workRange.Strategy != binding.Initial.Strategy ||
		workRange.TopologyHash != binding.Initial.TopologyHash {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"durable network topology changed for range %d",
				binding.RangeIndex,
			),
		)
	}
	if task.Status != "running" && task.Status != "completed" ||
		workRange.Status != "running" &&
			workRange.Status != "completed" ||
		task.Status == "completed" &&
			workRange.Status != "completed" {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"durable network status is inconsistent for range %d",
				binding.RangeIndex,
			),
		)
	}
	if task.Attempts < 0 || task.Retries < 0 ||
		workRange.Attempts < 0 || workRange.Retries < 0 {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"durable network attempt counters are negative for range %d",
				binding.RangeIndex,
			),
		)
	}
	return workRange, nil
}

func networkRestoreFromState(
	binding networkStateRangeBinding,
	workRange state.RangeState,
) (NetworkRangeRestore, error) {
	if workRange.RowsDone < 0 || workRange.SequenceOffset < 0 ||
		workRange.RowsDone < workRange.SequenceOffset ||
		workRange.SequenceOffset > int64(config.MaxTransferChunkRows) {
		return NetworkRangeRestore{}, fmt.Errorf(
			"durable row counters are inconsistent",
		)
	}
	frontier, err := encodeNetworkStateFrontier(
		workRange.Frontier,
		workRange.FrontierValid,
	)
	if err != nil {
		return NetworkRangeRestore{}, fmt.Errorf(
			"durable frontier: %w",
			err,
		)
	}
	result := NetworkRangeRestore{
		RangeIndex:     binding.RangeIndex,
		TopologyHash:   binding.Initial.TopologyHash,
		NextSequence:   workRange.NextSequence,
		SequenceOffset: workRange.SequenceOffset,
		RowsDone:       workRange.RowsDone - workRange.SequenceOffset,
		Frontier:       frontier,
		Complete:       workRange.Status == "completed",
		Issued:         make([]NetworkIssuedChunk, len(workRange.Pending)),
	}
	if (result.RowsDone > 0 || result.NextSequence > 0) &&
		len(result.Frontier) == 0 {
		return NetworkRangeRestore{}, fmt.Errorf(
			"progressed durable range is missing its frontier",
		)
	}
	if !validNetworkRestoreRowSequenceEvidence(
		result.RowsDone,
		result.NextSequence,
	) {
		return NetworkRangeRestore{}, fmt.Errorf(
			"durable rows and completed sequence count disagree",
		)
	}
	for index, pending := range workRange.Pending {
		if pending.ChunkRows <= 0 ||
			pending.ChunkRows > int64(config.MaxTransferChunkRows) ||
			pending.DurableRows < 0 ||
			pending.DurableRows > pending.ChunkRows ||
			pending.Attempts < 0 ||
			pending.Sequence < workRange.NextSequence ||
			pending.Fingerprint == "" ||
			!validNetworkFactToken(pending.Fingerprint) {
			return NetworkRangeRestore{}, fmt.Errorf(
				"pending issued chunk %d is malformed",
				pending.Sequence,
			)
		}
		start, err := encodeNetworkStateFrontier(
			pending.StartFrontier,
			pending.StartFrontierValid,
		)
		if err != nil {
			return NetworkRangeRestore{}, fmt.Errorf(
				"pending sequence %d start frontier: %w",
				pending.Sequence,
				err,
			)
		}
		end, err := encodeNetworkStateFrontier(
			pending.IssuedEndFrontier,
			pending.IssuedEndValid,
		)
		if err != nil || len(end) == 0 {
			if err == nil {
				err = fmt.Errorf("issued end frontier is missing")
			}
			return NetworkRangeRestore{}, fmt.Errorf(
				"pending sequence %d end frontier: %w",
				pending.Sequence,
				err,
			)
		}
		if pending.Sequence == workRange.NextSequence &&
			pending.DurableRows != workRange.SequenceOffset {
			return NetworkRangeRestore{}, fmt.Errorf(
				"pending first sequence durable prefix differs from range",
			)
		}
		if pending.Sequence > workRange.NextSequence &&
			pending.DurableRows != 0 {
			return NetworkRangeRestore{}, fmt.Errorf(
				"pending later sequence has an unrepresentable durable prefix",
			)
		}
		result.Issued[index] = NetworkIssuedChunk{
			RangeIndex:    binding.RangeIndex,
			Sequence:      pending.Sequence,
			Rows:          int(pending.ChunkRows),
			StartFrontier: start,
			EndFrontier:   end,
			Fingerprint:   pending.Fingerprint,
			Exhausted:     pending.Exhausted,
		}
	}
	sort.Slice(result.Issued, func(left, right int) bool {
		return result.Issued[left].Sequence <
			result.Issued[right].Sequence
	})
	return result, nil
}

func encodeNetworkStateFrontier(
	tuple state.TypedTuple,
	valid bool,
) ([]byte, error) {
	if !valid {
		if len(tuple) != 0 {
			return nil, fmt.Errorf(
				"invalid frontier carries typed values",
			)
		}
		return nil, nil
	}
	if err := tuple.Validate(false); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(tuple)
	if err != nil {
		return nil, fmt.Errorf("encode typed frontier: %w", err)
	}
	if len(encoded) == 0 ||
		len(encoded) > maximumNetworkFrontierBytes {
		return nil, fmt.Errorf("typed frontier exceeds its durable bound")
	}
	return encoded, nil
}

func decodeNetworkStateFrontier(
	encoded []byte,
) (state.TypedTuple, bool, error) {
	if len(encoded) == 0 {
		return nil, false, nil
	}
	if len(encoded) > maximumNetworkFrontierBytes {
		return nil, false, fmt.Errorf(
			"typed frontier exceeds its durable bound",
		)
	}
	var tuple state.TypedTuple
	if err := json.Unmarshal(encoded, &tuple); err != nil {
		return nil, false, fmt.Errorf("decode typed frontier: %w", err)
	}
	if err := tuple.Validate(false); err != nil {
		return nil, false, err
	}
	canonical, err := json.Marshal(tuple)
	if err != nil {
		return nil, false, fmt.Errorf(
			"re-encode typed frontier: %w",
			err,
		)
	}
	if !bytes.Equal(canonical, encoded) {
		return nil, false, fmt.Errorf(
			"typed frontier encoding is not canonical",
		)
	}
	return tuple, true, nil
}

func (coordinator *networkStateCoordinator) recordIssued(
	ctx context.Context,
	issued NetworkIssuedChunk,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, ok := coordinator.byIndex[issued.RangeIndex]
	if !ok {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"issued network chunk references an unknown range",
			),
		)
	}
	if issued.Rows <= 0 ||
		issued.Rows > config.MaxTransferChunkRows ||
		issued.Fingerprint == "" ||
		!validNetworkFactToken(issued.Fingerprint) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("issued network chunk is malformed"),
		)
	}
	start, startValid, err := decodeNetworkStateFrontier(
		issued.StartFrontier,
	)
	if err != nil {
		return NewTransferError(ErrorClassState, err)
	}
	end, endValid, err := decodeNetworkStateFrontier(
		issued.EndFrontier,
	)
	if err != nil || !endValid {
		if err == nil {
			err = fmt.Errorf("issued network end frontier is required")
		}
		return NewTransferError(ErrorClassState, err)
	}
	err = coordinator.run.Backend.BeginRangeChunk(
		state.RangeChunkIntent{
			RunID:              coordinator.run.RunID,
			Task:               binding.Task,
			RangeID:            binding.Initial.ID,
			TopologyHash:       binding.Initial.TopologyHash,
			Sequence:           issued.Sequence,
			ChunkRows:          int64(issued.Rows),
			StartFrontier:      start,
			StartFrontierValid: startValid,
			EndFrontier:        end,
			FrontierValid:      endValid,
			Fingerprint:        issued.Fingerprint,
			Exhausted:          issued.Exhausted,
			At:                 coordinator.now().UTC(),
		},
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("record durable network issued chunk: %w", err),
		)
	}
	return ctx.Err()
}

func (coordinator *networkStateCoordinator) wrapWrite(
	observer TableObserver,
	delegate func(
		context.Context,
		NetworkWriteRequest,
	) (WriteReceipt, error),
) func(context.Context, NetworkWriteRequest) (WriteReceipt, error) {
	return func(
		ctx context.Context,
		request NetworkWriteRequest,
	) (WriteReceipt, error) {
		failed := networkStateFailedReceipt(request)
		if delegate == nil {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf("network target writer is unavailable"),
			)
		}
		protector, protected := observer.(adapterTargetMutationProtector)
		if !protected || networkMutationProtectorIsNil(protector) {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network target writer requires a lease-fenced mutation protector",
				),
			)
		}
		if err := ctx.Err(); err != nil {
			return failed, err
		}
		binding, ok := coordinator.byIndex[request.Range.RangeIndex]
		if !ok ||
			request.Range.TopologyHash !=
				binding.Initial.TopologyHash {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network write references an unknown durable range",
				),
			)
		}
		if err := coordinator.run.Backend.RecordRangeAttempt(
			state.RangeAttempt{
				RunID:        coordinator.run.RunID,
				Task:         binding.Task,
				RangeID:      binding.Initial.ID,
				TopologyHash: binding.Initial.TopologyHash,
				Sequence:     request.Sequence,
				At:           coordinator.now().UTC(),
			},
		); err != nil {
			return failed, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"authorize durable network target attempt: %w",
					err,
				),
			)
		}
		if err := ctx.Err(); err != nil {
			return failed, err
		}
		receipt := failed
		_, err := protectAdapterTargetMutationOnce(
			ctx,
			observer,
			"network page write",
			func() error {
				var writeErr error
				receipt, writeErr = delegate(ctx, request)
				return writeErr
			},
		)
		return receipt, err
	}
}

func networkMutationProtectorIsNil(
	protector adapterTargetMutationProtector,
) bool {
	if protector == nil {
		return true
	}
	value := reflect.ValueOf(protector)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (coordinator *networkStateCoordinator) checkpoint(
	ctx context.Context,
	checkpoint NetworkRangeCheckpoint,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, ok := coordinator.byIndex[checkpoint.RangeIndex]
	if !ok ||
		checkpoint.TopologyHash != binding.Initial.TopologyHash ||
		checkpoint.Frontier.RangeID !=
			fmt.Sprintf("range/%d", checkpoint.RangeIndex) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("network checkpoint references an unknown range"),
		)
	}
	if checkpoint.Frontier.Rows < 0 ||
		checkpoint.Frontier.SequenceOffset < 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("network checkpoint row counters are negative"),
		)
	}
	durableFrontier, frontierValid, err :=
		decodeNetworkStateFrontier(checkpoint.FrontierBytes)
	if err != nil {
		return NewTransferError(ErrorClassState, err)
	}
	snapshot, err := coordinator.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	workRange, err := snapshot.exact(binding)
	if err != nil {
		return err
	}
	workRange, err = coordinator.advanceRangeToCheckpoint(
		binding,
		workRange,
		checkpoint,
		durableFrontier,
		frontierValid,
	)
	if err != nil {
		return err
	}
	if workRange.RowsDone != checkpoint.Frontier.Rows ||
		workRange.NextSequence != checkpoint.Frontier.NextSequence ||
		workRange.SequenceOffset !=
			checkpoint.Frontier.SequenceOffset {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"durable network checkpoint did not reach the requested frontier",
			),
		)
	}
	encoded, encodeErr := encodeNetworkStateFrontier(
		workRange.Frontier,
		workRange.FrontierValid,
	)
	if encodeErr != nil || !bytes.Equal(
		encoded,
		checkpoint.FrontierBytes,
	) {
		if encodeErr == nil {
			encodeErr = fmt.Errorf(
				"durable frontier differs from network checkpoint",
			)
		}
		return NewTransferError(ErrorClassState, encodeErr)
	}
	if !checkpoint.Complete {
		return ctx.Err()
	}
	if len(workRange.Pending) != 0 ||
		workRange.SequenceOffset != 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"network range completion retains unresolved chunks",
			),
		)
	}
	if workRange.Status != "completed" {
		if err := coordinator.run.Backend.CompleteRange(
			coordinator.run.RunID,
			binding.Task,
			binding.Initial.ID,
			binding.Initial.TopologyHash,
			checkpoint.Frontier.NextSequence,
			coordinator.now().UTC(),
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"complete durable network range: %w",
					err,
				),
			)
		}
	}
	return coordinator.completeTaskIfReady(ctx, binding)
}

func (coordinator *networkStateCoordinator) advanceRangeToCheckpoint(
	binding networkStateRangeBinding,
	workRange state.RangeState,
	checkpoint NetworkRangeCheckpoint,
	durableFrontier state.TypedTuple,
	frontierValid bool,
) (state.RangeState, error) {
	target := checkpoint.Frontier
	if target.NextSequence < workRange.NextSequence ||
		target.NextSequence == workRange.NextSequence &&
			target.SequenceOffset < workRange.SequenceOffset ||
		target.Rows < workRange.RowsDone {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("network checkpoint regressed durable state"),
		)
	}
	for workRange.NextSequence < target.NextSequence {
		pending, found := networkPending(
			workRange,
			workRange.NextSequence,
		)
		if !found || pending.Attempts <= 0 ||
			pending.DurableRows > pending.ChunkRows {
			return state.RangeState{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network checkpoint crosses an unauthorized issued chunk",
				),
			)
		}
		if pending.DurableRows == pending.ChunkRows {
			// A prior out-of-order acknowledgement becomes contiguous when an
			// earlier call advances state. Re-read through a zero-new-row
			// transition is impossible in RangeBackend, so this shape must
			// have advanced automatically with that earlier acknowledgement.
			return state.RangeState{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"durable network pending frontier failed to advance",
				),
			)
		}
		end, endValid := networkPendingIssuedEnd(pending)
		if !endValid {
			return state.RangeState{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network issued end frontier is unavailable",
				),
			)
		}
		next, err := coordinator.run.Backend.AcknowledgeRange(
			state.RangeAcknowledgement{
				RunID:         coordinator.run.RunID,
				Task:          binding.Task,
				RangeID:       binding.Initial.ID,
				TopologyHash:  binding.Initial.TopologyHash,
				Sequence:      pending.Sequence,
				ChunkRows:     pending.ChunkRows,
				AttemptOffset: pending.DurableRows,
				DurableRows: pending.ChunkRows -
					pending.DurableRows,
				Frontier:      end,
				FrontierValid: true,
				At:            coordinator.now().UTC(),
			},
		)
		if err != nil {
			return state.RangeState{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"advance durable network acknowledgement: %w",
					err,
				),
			)
		}
		workRange = next
	}
	if workRange.NextSequence != target.NextSequence {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("network checkpoint sequence is unreachable"),
		)
	}
	if target.SequenceOffset > 0 {
		pending, found := networkPending(
			workRange,
			target.NextSequence,
		)
		if !found || pending.Attempts <= 0 ||
			target.SequenceOffset > pending.ChunkRows ||
			target.SequenceOffset < pending.DurableRows {
			return state.RangeState{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"network partial checkpoint is inconsistent",
				),
			)
		}
		if target.SequenceOffset > pending.DurableRows {
			next, err := coordinator.run.Backend.AcknowledgeRange(
				state.RangeAcknowledgement{
					RunID:         coordinator.run.RunID,
					Task:          binding.Task,
					RangeID:       binding.Initial.ID,
					TopologyHash:  binding.Initial.TopologyHash,
					Sequence:      pending.Sequence,
					ChunkRows:     pending.ChunkRows,
					AttemptOffset: pending.DurableRows,
					DurableRows: target.SequenceOffset -
						pending.DurableRows,
					Frontier:      durableFrontier,
					FrontierValid: frontierValid,
					At:            coordinator.now().UTC(),
				},
			)
			if err != nil {
				return state.RangeState{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"advance durable network partial acknowledgement: %w",
						err,
					),
				)
			}
			workRange = next
		}
	} else if workRange.SequenceOffset != 0 {
		return state.RangeState{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"network checkpoint omitted a durable partial prefix",
			),
		)
	}
	return workRange, nil
}

func networkPending(
	workRange state.RangeState,
	sequence uint64,
) (state.PendingAcknowledgement, bool) {
	for _, pending := range workRange.Pending {
		if pending.Sequence == sequence {
			return pending, true
		}
	}
	return state.PendingAcknowledgement{}, false
}

func networkPendingIssuedEnd(
	pending state.PendingAcknowledgement,
) (state.TypedTuple, bool) {
	if pending.IssuedEndValid {
		return append(
			state.TypedTuple(nil),
			pending.IssuedEndFrontier...,
		), true
	}
	if pending.FrontierValid && pending.DurableRows == 0 {
		return append(
			state.TypedTuple(nil),
			pending.Frontier...,
		), true
	}
	return nil, false
}

func (coordinator *networkStateCoordinator) completeTaskIfReady(
	ctx context.Context,
	binding networkStateRangeBinding,
) error {
	snapshot, err := coordinator.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	task := snapshot.tasks[binding.Task]
	if task.Status == "completed" {
		return nil
	}
	ranges := snapshot.ranges[binding.Task]
	if len(ranges) == 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("durable network task has no ranges"),
		)
	}
	for _, workRange := range ranges {
		if workRange.Status != "completed" {
			return nil
		}
	}
	if err := coordinator.run.Backend.CompleteWorkTask(
		coordinator.run.RunID,
		binding.Task,
		binding.Initial.TopologyHash,
		coordinator.now().UTC(),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("complete durable network task: %w", err),
		)
	}
	return ctx.Err()
}

func networkStateFailedReceipt(
	request NetworkWriteRequest,
) WriteReceipt {
	rows := int64(len(request.Rows))
	return WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: rows,
		AttemptOffset: request.AttemptOffset,
	}
}
