package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// Stage 4 durable work: the inventory a run is bound to, its checkpoints,
// and the schema-gate sentinels that close it out.

func buildStage4AdapterWork(
	configDigest string,
	mode string,
	plans []adapterTablePlan,
) ([]stage4AdapterWork, error) {
	result := make([]stage4AdapterWork, len(plans))
	seen := make(map[state.TaskKey]struct{}, len(plans))
	for index, plan := range plans {
		task := state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: plan.source.Schema,
			Table:  plan.source.Name,
		}
		if _, duplicate := seen[task]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 table work task for (%q, %q)",
					task.Schema,
					task.Table,
				),
			)
		}
		seen[task] = struct{}{}
		sourceSnapshot, err := schema.NewSchemaSnapshot(
			[]schema.Table{plan.source},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize Stage 4 source work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		sourceCanonical, err := sourceSnapshot.CanonicalJSON()
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 source work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		targetSnapshot, err := schema.NewSchemaSnapshot(
			[]schema.Table{plan.target},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize Stage 4 target work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		targetCanonical, err := targetSnapshot.CanonicalJSON()
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 target work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		var sourceIdentityFrontier, targetIdentityFrontier *int64
		if mode != "upsert" {
			sourceIdentityFrontier = stage4IdentityFrontier(plan.source)
			targetIdentityFrontier = stage4IdentityFrontier(plan.target)
		}
		wire := struct {
			Version                int      `json:"version"`
			ConfigDigest           string   `json:"config_digest"`
			Mode                   string   `json:"mode"`
			SourceCanonical        string   `json:"source_canonical"`
			TargetCanonical        string   `json:"target_canonical"`
			SourceIdentityFrontier *int64   `json:"source_identity_frontier"`
			TargetIdentityFrontier *int64   `json:"target_identity_frontier"`
			Projection             []string `json:"projection"`
		}{
			Version:                1,
			ConfigDigest:           configDigest,
			Mode:                   mode,
			SourceCanonical:        string(sourceCanonical),
			TargetCanonical:        string(targetCanonical),
			SourceIdentityFrontier: sourceIdentityFrontier,
			TargetIdentityFrontier: targetIdentityFrontier,
			Projection:             append([]string(nil), plan.columns...),
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 table work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		digest := sha256.Sum256(encoded)
		result[index] = stage4AdapterWork{
			task:     task,
			strategy: stage4AdapterCopyStrategy,
			topology: hex.EncodeToString(digest[:]),
		}
	}
	return result, nil
}

func stage4IdentityFrontier(table schema.Table) *int64 {
	if table.Identity == nil || table.Identity.Frontier == nil {
		return nil
	}
	value := *table.Identity.Frontier
	return &value
}

type stage4WorkInventory struct {
	tasks  map[state.TaskKey]state.WorkTask
	ranges map[state.TaskKey][]state.RangeState
}

func loadStage4WorkInventory(
	ctx context.Context,
	run Stage4RunContext,
) (stage4WorkInventory, error) {
	result := stage4WorkInventory{
		tasks:  make(map[state.TaskKey]state.WorkTask),
		ranges: make(map[state.TaskKey][]state.RangeState),
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 work evidence: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for _, task := range tasks {
		if _, duplicate := result.tasks[task.Key]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 work task %#v",
					task.Key,
				),
			)
		}
		result.tasks[task.Key] = task
	}
	for _, workRange := range ranges {
		result.ranges[workRange.Task] = append(
			result.ranges[workRange.Task],
			workRange,
		)
	}
	return result, nil
}

func (inventory stage4WorkInventory) exact(
	key state.TaskKey,
	rangeID string,
	strategy string,
	topology string,
	allowMissing bool,
) (state.WorkTask, state.RangeState, bool, error) {
	task, found := inventory.tasks[key]
	taskRanges := inventory.ranges[key]
	if !found {
		if len(taskRanges) != 0 {
			return state.WorkTask{}, state.RangeState{}, false,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"orphaned Stage 4 work ranges exist for missing task %#v",
						key,
					),
				)
		}
		if allowMissing {
			return state.WorkTask{}, state.RangeState{}, false, nil
		}
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf("missing Stage 4 work task %#v", key),
			)
	}
	if len(taskRanges) != 1 || taskRanges[0].ID != rangeID {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work task %#v has an unsafe range set",
					key,
				),
			)
	}
	workRange := taskRanges[0]
	if task.Strategy != strategy ||
		task.TopologyHash != topology ||
		workRange.Strategy != strategy ||
		workRange.TopologyHash != topology {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work topology changed for task %#v",
					key,
				),
			)
	}
	if err := validateStage4CoarseWorkState(
		task,
		workRange,
	); err != nil {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work state is unsafe for task %#v: %w",
					key,
					err,
				),
			)
	}
	return task, workRange, true, nil
}

func validateStage4CoarseWorkState(
	task state.WorkTask,
	workRange state.RangeState,
) error {
	switch task.Status {
	case "running", "completed":
	default:
		return fmt.Errorf("task status is %q", task.Status)
	}
	switch workRange.Status {
	case "running", "completed":
	default:
		return fmt.Errorf("range status is %q", workRange.Status)
	}
	if task.Status == "completed" && workRange.Status != "completed" {
		return fmt.Errorf(
			"completed task has non-completed range status %q",
			workRange.Status,
		)
	}
	if task.Attempts != 0 || task.Retries != 0 || task.Error != "" {
		return fmt.Errorf("coarse task contains unexpected retry evidence")
	}
	if len(workRange.Lower) != 0 ||
		len(workRange.Upper) != 0 ||
		workRange.LowerInclusive ||
		workRange.UpperInclusive ||
		workRange.FirstRow != 0 ||
		workRange.LastRow != 0 ||
		len(workRange.Frontier) != 0 ||
		workRange.FrontierValid ||
		workRange.NextSequence != 0 ||
		workRange.SequenceOffset != 0 ||
		workRange.RowsDone != 0 ||
		workRange.RowsTotal != 0 ||
		workRange.CommittedPrefix != 0 ||
		workRange.Attempts != 0 ||
		workRange.Retries != 0 ||
		workRange.Error != "" ||
		len(workRange.Pending) != 0 {
		return fmt.Errorf(
			"coarse range contains unexpected progress or retry evidence",
		)
	}
	return nil
}

func verifyStage4SchemaSentinelEvidence(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
) error {
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		return err
	}
	task, workRange, _, err := inventory.exact(
		gate.Task,
		stage4SchemaGateRangeID,
		stage4SchemaGateStrategy,
		gate.TopologyHash,
		false,
	)
	if err != nil {
		return fmt.Errorf(
			"verify Stage 4 schema sentinel before target planning: %w",
			err,
		)
	}
	snapshot, found, err := run.Backend.LoadSchemaSnapshot(
		run.RunID,
		gate.Task,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 schema sentinel evidence before target planning: %w",
				err,
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !found {
		if task.Status != "running" ||
			workRange.Status != "running" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 schema sentinel is complete without its prior validated snapshot",
				),
			)
		}
		return nil
	}
	pending := gate.PendingSnapshot
	if snapshot.RunID != pending.RunID ||
		snapshot.Task != pending.Task ||
		snapshot.CanonicalJSON != pending.CanonicalJSON ||
		snapshot.Digest != pending.Digest ||
		!snapshot.CapturedAt.Equal(pending.CapturedAt) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 schema sentinel snapshot changed after policy evaluation",
			),
		)
	}
	return nil
}

func checkpointStage4AdapterWork(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
) error {
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network admission requires a lease-fenced target mutation protector",
			),
		)
	}
	setObserver, err := requireStage4TableSetObserver(observer)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := setObserver.BeforeTables(
		ctx,
		append([]string(nil), prepared.names...),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint Stage 4 table set: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared.network == nil {
		if len(prepared.work) == 0 {
			return nil
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network admission is unavailable"),
		)
	}
	if err := prepared.network.ensurePlans(ctx); err != nil {
		return fmt.Errorf(
			"checkpoint Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	if _, err := prepared.network.loadRestores(ctx); err != nil {
		return fmt.Errorf(
			"verify Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	return nil
}

func requireStage4TableSetObserver(
	observer TableObserver,
) (TableSetObserver, error) {
	setObserver, ok := observer.(TableSetObserver)
	if !ok || isNilAdapterResumeObserver(observer) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 composed-adapter migration requires a table-set observer so ordinary checkpoints exist before target preparation",
			),
		)
	}
	return setObserver, nil
}

func ensureStage4AdapterWork(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
) error {
	if len(work) == 0 {
		return ctx.Err()
	}
	coordinator, err := newStage4AdapterNetworkCoordinator(run, work)
	if err != nil {
		return err
	}
	if err := coordinator.ensurePlans(ctx); err != nil {
		return fmt.Errorf(
			"checkpoint Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	_, err = coordinator.loadRestores(ctx)
	if err != nil {
		return fmt.Errorf(
			"verify Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	return nil
}

func verifyStage4ResumeWorkEvidence(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
	validated map[string]int,
	allowMissingIncomplete bool,
) error {
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		return fmt.Errorf(
			"read Stage 4 table work before resume mutation: %w",
			err,
		)
	}
	expected := make(map[state.TaskKey]struct{}, len(work))
	for _, item := range work {
		expected[item.task] = struct{}{}
	}
	for key := range inventory.tasks {
		if key.Type != stage4AdapterNetworkTaskType &&
			key.Type != "table-copy" &&
			key.Type != "analytical-table-copy" {
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
		for range ranges {
			if key.Type != stage4AdapterNetworkTaskType &&
				key.Type != "table-copy" &&
				key.Type != "analytical-table-copy" {
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
		}
	}
	for _, item := range work {
		checkpointRows, checkpointComplete := validated[item.task.Table]
		task, ranges, found, err := exactStage4AdapterWork(
			inventory,
			item,
			allowMissingIncomplete && !checkpointComplete,
		)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if task.Status == "completed" && !checkpointComplete {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 structured work marks table %s complete but its ordinary checkpoint is not reusable",
					item.task.Table,
				),
			)
		}
		if checkpointComplete {
			var structuredRows int64
			for _, workRange := range ranges {
				if workRange.Status != "completed" {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 ordinary checkpoint marks table %s complete but structured range %s is not complete",
							item.task.Table,
							workRange.ID,
						),
					)
				}
				if workRange.RowsDone >
					math.MaxInt64-structuredRows {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 structured row total overflows for table %s",
							item.task.Table,
						),
					)
				}
				structuredRows += workRange.RowsDone
			}
			if structuredRows != int64(checkpointRows) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 ordinary checkpoint row total differs from completed structured ranges for table %s",
						item.task.Table,
					),
				)
			}
		}
	}
	return nil
}

func completeStage4AdapterWork(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
) error {
	for _, item := range work {
		if err := completeStage4AdapterWorkItem(
			ctx,
			run,
			item,
		); err != nil {
			return fmt.Errorf(
				"complete Stage 4 work for %s: %w",
				item.task.Table,
				err,
			)
		}
	}
	return nil
}

func stageStage4SchemaGateSnapshots(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run.Backend.SaveSchemaSnapshot(
		gate.PendingSnapshot,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"save validated Stage 4 schema snapshot before sentinel completion: %w",
				err,
			),
		)
	}
	if evolution != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := run.Backend.SaveSchemaSnapshot(
			evolution.pending,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"save validated Stage 4 target-shape snapshot before schema DDL: %w",
					err,
				),
			)
		}
	}
	return ctx.Err()
}

// completeStage4AdapterTerminalSchemaGateSentinels leaves schema sentinels
// running for every route that has already published immutable aggregate table
// inventory. PublishStage4RunCompletion owns their one terminal timestamp and
// atomically records it with the successful run outcome. A backend without
// aggregate support, or a legacy route that never published inventory, keeps
// the older direct-completion path.
func completeStage4AdapterTerminalSchemaGateSentinels(
	ctx context.Context,
	prepared stage4AdapterPrepared,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	aggregate, ok := prepared.run.Backend.(state.Stage4AggregateBackend)
	if ok && !nilStage4AggregateBackend(aggregate) {
		_, found, err := aggregate.LoadStage4TableInventory(
			prepared.run.RunID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"read Stage 4 table inventory before terminal sentinel completion: %w",
					err,
				),
			)
		}
		if found {
			return nil
		}
	}
	return completeStage4SchemaGateSentinels(
		ctx,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	)
}

func completeStage4SchemaGateSentinels(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if evolution != nil {
		if err := completeStage4WorkTask(
			ctx,
			run,
			evolution.authority.Task(),
			stage4TargetShapeRangeID,
			evolution.authority.TopologyHash(),
		); err != nil {
			return fmt.Errorf(
				"complete validated Stage 4 target-shape sentinel: %w",
				err,
			)
		}
	}
	if err := completeStage4WorkTask(
		ctx,
		run,
		gate.Task,
		stage4SchemaGateRangeID,
		gate.TopologyHash,
	); err != nil {
		return fmt.Errorf(
			"complete validated Stage 4 schema sentinel: %w",
			err,
		)
	}
	return nil
}

func completeStage4WorkTask(
	ctx context.Context,
	run Stage4RunContext,
	task state.TaskKey,
	rangeID string,
	topology string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 work before completion: %w", err),
		)
	}
	var matchedTask *state.WorkTask
	for index := range tasks {
		if tasks[index].Key != task {
			continue
		}
		if matchedTask != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("duplicate Stage 4 work task %#v", task),
			)
		}
		matchedTask = &tasks[index]
	}
	if matchedTask == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("missing Stage 4 work task %#v", task),
		)
	}
	if matchedTask.TopologyHash != topology {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work topology changed for task %#v",
				task,
			),
		)
	}
	var matchedRange *state.RangeState
	for index := range ranges {
		if ranges[index].Task != task ||
			ranges[index].ID != rangeID {
			continue
		}
		if matchedRange != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 work range %q for task %#v",
					rangeID,
					task,
				),
			)
		}
		matchedRange = &ranges[index]
	}
	if matchedRange == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"missing Stage 4 work range %q for task %#v",
				rangeID,
				task,
			),
		)
	}
	if matchedRange.TopologyHash != topology {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work range topology changed for task %#v",
				task,
			),
		)
	}
	if matchedTask.Status == "completed" {
		if matchedRange.Status != "completed" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 work task has non-completed range status %q",
					matchedRange.Status,
				),
			)
		}
		return nil
	}
	if matchedTask.Status != "running" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work task has unsafe status %q",
				matchedTask.Status,
			),
		)
	}
	now := time.Now().UTC()
	switch matchedRange.Status {
	case "completed":
	case "running":
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := run.Backend.CompleteRange(
			run.RunID,
			task,
			rangeID,
			topology,
			matchedRange.NextSequence,
			now,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("complete Stage 4 work range: %w", err),
			)
		}
	default:
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work range %q has unsafe status %q",
				rangeID,
				matchedRange.Status,
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run.Backend.CompleteWorkTask(
		run.RunID,
		task,
		topology,
		now,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("complete Stage 4 work task: %w", err),
		)
	}
	return nil
}
