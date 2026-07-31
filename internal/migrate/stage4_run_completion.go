package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// stage4SentinelRangeID reports the single range a Stage 4 schema sentinel owns.
// Only these two tasks are sentinels; every other structured task is table work.
func stage4SentinelRangeID(task state.TaskKey) (string, bool) {
	switch task {
	case stage4SchemaGateTask:
		return stage4SchemaGateRangeID, true
	case stage4TargetShapeTask:
		return stage4TargetShapeRangeID, true
	default:
		return "", false
	}
}

// PublishStage4RunCompletion completes every durable Stage 4 schema sentinel and
// publishes the running migration as successful in one backend mutation, closing
// the window where table evidence is terminal but the run outcome is not.
//
// Everything it supplies is recovered from durable state rather than carried in
// memory, so a resumed process publishes exactly what the original one would
// have. The caller owns the success reason and completion time because those are
// application lifecycle facts, and terminal repair reads the reason back.
//
// It reports false without mutating state when the run has no durable table
// inventory. That absence is the marker that this route never composed aggregate
// evidence, and the caller must then record success by its ordinary path. Once a
// route does publish an inventory, an incomplete composition fails closed here
// rather than silently falling back.
func PublishStage4RunCompletion(
	ctx context.Context,
	run Stage4RunContext,
	reason string,
	completedAt time.Time,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := run.Validate(); err != nil {
		return false, NewTransferError(ErrorClassState, err)
	}
	if reason == "" {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 run completion reason is required"),
		)
	}
	if completedAt.IsZero() {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 run completion time is required"),
		)
	}
	aggregate, ok := run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return false, nil
	}
	if _, found, err := aggregate.LoadStage4TableInventory(
		run.RunID,
	); err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 table inventory before run publication: %w",
				err,
			),
		)
	} else if !found {
		return false, nil
	}
	completion, err := buildStage4RunCompletion(
		run,
		aggregate,
		reason,
		completedAt,
	)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := aggregate.CompleteStage4Run(completion); err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("atomically publish Stage 4 run completion: %w", err),
		)
	}
	return true, nil
}

func buildStage4RunCompletion(
	run Stage4RunContext,
	aggregate state.Stage4AggregateBackend,
	reason string,
	completedAt time.Time,
) (state.Stage4RunCompletion, error) {
	receipts, err := aggregate.LoadStage4TableCompletions(run.RunID)
	if err != nil {
		return state.Stage4RunCompletion{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 table completions before run publication: %w",
				err,
			),
		)
	}
	tables := make([]state.Stage4TableCompletion, 0, len(receipts))
	for _, receipt := range receipts {
		tables = append(tables, receipt.Completion)
	}
	sentinels, err := stage4DurableSentinelCompletions(run)
	if err != nil {
		return state.Stage4RunCompletion{}, err
	}
	return state.Stage4RunCompletion{
		RunID:       run.RunID,
		Tables:      tables,
		Sentinels:   sentinels,
		Reason:      reason,
		CompletedAt: completedAt.UTC(),
	}, nil
}

// stage4DurableSentinelCompletions rebuilds every schema sentinel from the work
// plan that already exists, so a sentinel established by an earlier process is
// never silently omitted from the run publication.
func stage4DurableSentinelCompletions(
	run Stage4RunContext,
) ([]state.Stage4SentinelCompletion, error) {
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 work before run publication: %w",
				err,
			),
		)
	}
	sentinels := make([]state.Stage4SentinelCompletion, 0, 2)
	seen := make(map[state.TaskKey]struct{}, 2)
	for _, task := range tasks {
		rangeID, sentinel := stage4SentinelRangeID(task.Key)
		if !sentinel {
			continue
		}
		if _, duplicate := seen[task.Key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 sentinel work task %#v",
					task.Key,
				),
			)
		}
		seen[task.Key] = struct{}{}
		nextSequence, err := stage4SentinelNextSequence(
			run.RunID,
			task.Key,
			rangeID,
			ranges,
		)
		if err != nil {
			return nil, err
		}
		snapshot, found, err := run.Backend.LoadSchemaSnapshot(
			run.RunID,
			task.Key,
		)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"read Stage 4 sentinel snapshot %#v: %w",
					task.Key,
					err,
				),
			)
		}
		if !found {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 sentinel %#v has no durable schema snapshot",
					task.Key,
				),
			)
		}
		sentinels = append(sentinels, state.Stage4SentinelCompletion{
			Task:         task.Key,
			RangeID:      rangeID,
			TopologyHash: task.TopologyHash,
			NextSequence: nextSequence,
			Snapshot:     snapshot,
		})
	}
	if len(sentinels) == 0 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 run %q has no durable schema sentinel",
				run.RunID,
			),
		)
	}
	return sentinels, nil
}

func stage4SentinelNextSequence(
	runID string,
	task state.TaskKey,
	rangeID string,
	ranges []state.RangeState,
) (uint64, error) {
	var matched *state.RangeState
	for index := range ranges {
		candidate := &ranges[index]
		if candidate.RunID != runID || candidate.Task != task {
			continue
		}
		if candidate.ID != rangeID {
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 sentinel %#v owns unexpected range %q",
					task,
					candidate.ID,
				),
			)
		}
		if matched != nil {
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 sentinel range %q",
					rangeID,
				),
			)
		}
		matched = candidate
	}
	if matched == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 sentinel %#v has no durable range %q",
				task,
				rangeID,
			),
		)
	}
	return matched.NextSequence, nil
}
