package migrate

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4RebuildRecoveryKind describes the only three restart boundaries the
// destructive runner admits.  The distinction is deliberately durable: a
// process never infers target ownership merely from an earlier process having
// reached PrepareTables.
type stage4RebuildRecoveryKind uint8

const (
	stage4RebuildRerun stage4RebuildRecoveryKind = iota
	stage4RebuildReplay
	stage4RebuildPublication
)

type stage4RebuildRecovery struct {
	kind                stage4RebuildRecoveryKind
	aggregate           state.Stage4AggregateBackend
	ready               state.Stage4RebuildRecoveryBackend
	inventory           state.Stage4TableInventoryReceipt
	finalizationStarted bool
	work                []stage4AdapterWork
	receipts            map[string]state.Stage4TableCompletionReceipt
	rows                map[string]int
}

type stage4RebuildTaskReader interface {
	ListTasks(string) ([]state.Task, error)
}

// runStage4AdapterStableNetworkRebuildTables owns the set-wide destructive
// lifecycle and its recovery protocol.  A fresh run and a recovery that has
// no durable target-write authority both prepare the whole selected set before
// any transfer.  Once a range has durable issued/checkpoint evidence, recovery
// instead restores that exact range and invokes the duplicate-safe rebuild
// writer; it never drops pages that may already belong to this run.
func runStage4AdapterStableNetworkRebuildTables(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (Result, error) {
	if execution == nil || !execution.deferred {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild requires deferred stable network execution"),
		)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if !resume {
		if len(completed) != 0 {
			return Result{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("fresh Stage 4 rebuild has completed table evidence"),
			)
		}
		return runStage4AdapterFreshNetworkRebuild(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
		)
	}

	if err := completeStage4AdapterNetworkRebuildCheckpointPrefix(
		ctx,
		observer,
		execution,
		completed,
	); err != nil {
		return Result{}, err
	}
	recovery, err := classifyStage4AdapterNetworkRebuildRecovery(
		ctx,
		execution,
		completed,
	)
	if err != nil {
		return Result{}, err
	}
	switch recovery.kind {
	case stage4RebuildRerun:
		return runStage4AdapterRecoveredNetworkRebuild(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
			recovery,
			true,
		)
	case stage4RebuildReplay:
		return runStage4AdapterRecoveredNetworkRebuild(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
			recovery,
			false,
		)
	case stage4RebuildPublication:
		return publishStage4AdapterRecoveredNetworkRebuild(
			ctx,
			cfg,
			observer,
			execution.source,
			target,
			prepared,
			recovery,
		)
	default:
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild recovery state is unknown"),
		)
	}
}

// completeStage4AdapterNetworkRebuildCheckpointPrefix repairs only a fresh
// run's provably pre-mutation checkpoint prefix. The fresh order is inventory,
// ordinary table set, then one immutable WorkPlan per table; PrepareTables is
// unreachable until that whole sequence returns successfully. Consequently a
// missing member of that exact sequence, combined with pristine earlier
// evidence and no terminal/write authority, proves that target mutation could
// not yet have been authorized by this runner.
//
// A complete checkpoint set is left untouched for the ordinary recovery
// classifier. Any issued range, progress, terminal receipt, changed topology,
// or non-prefix work set fails closed instead of being reinterpreted as
// pre-mutation evidence.
func completeStage4AdapterNetworkRebuildCheckpointPrefix(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
) error {
	backends, err := stage4AdapterNetworkRebuildBackends(execution)
	if err != nil {
		return err
	}
	work, err := stage4AdapterPlanNetworkRebuildRecoveryWork(ctx, execution)
	if err != nil {
		return err
	}
	inventory, inventoryFound, err := backends.aggregate.LoadStage4TableInventory(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table inventory before checkpoint recovery: %w", err),
		)
	}
	if inventoryFound {
		if err := validateStage4AdapterNetworkRebuildInventory(inventory, work); err != nil {
			return err
		}
	}

	durable, err := loadStage4WorkInventory(ctx, execution.prepared.run)
	if err != nil {
		return err
	}
	if err := execution.validateDurableTableTaskInventory(durable); err != nil {
		return err
	}
	ordinary, err := stage4AdapterNetworkRebuildOrdinaryTaskSubset(execution)
	if err != nil {
		return err
	}

	type checkpointWorkEvidence struct {
		item   stage4AdapterWork
		task   state.WorkTask
		ranges []state.RangeState
	}
	foundWork := make([]checkpointWorkEvidence, 0, len(work))
	missingWork := false
	workFound := 0
	for _, item := range work {
		task, ranges, found, exactErr := exactStage4AdapterWork(
			durable,
			item,
			true,
		)
		if exactErr != nil {
			return exactErr
		}
		if !found {
			missingWork = true
			continue
		}
		if missingWork {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 rebuild pre-mutation work is not an exact checkpoint prefix at %s",
					item.task.Table,
				),
			)
		}
		workFound++
		foundWork = append(foundWork, checkpointWorkEvidence{
			item:   item,
			task:   task,
			ranges: ranges,
		})
	}

	ordinaryMissing := len(ordinary) != len(execution.prepared.plans)
	if inventoryFound && !missingWork && !ordinaryMissing {
		// The complete checkpoint boundary may already carry transfer or
		// publication authority. The normal classifier owns that decision and
		// this helper must not mutate or reject it as a partial prefix.
		return nil
	}
	if inventoryFound && !missingWork && ordinaryMissing {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 rebuild ordinary task evidence is incomplete after the full work checkpoint; target safety cannot be inferred",
			),
		)
	}
	if err := validateStage4AdapterNetworkRebuildPristineOrdinaryTasks(ordinary); err != nil {
		return err
	}
	for _, evidence := range foundWork {
		if err := validateStage4AdapterNetworkRebuildPristineWork(
			evidence.item,
			evidence.task,
			evidence.ranges,
		); err != nil {
			return err
		}
	}
	if len(completed) != 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild pre-mutation checkpoint is incomplete but completed table evidence exists"),
		)
	}
	ready, readyFound, err := backends.ready.LoadStage4RebuildReady(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild terminal readiness before checkpoint recovery: %w", err),
		)
	}
	if readyFound || ready.Ready.RunID != "" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild terminal readiness cannot accompany an incomplete pre-mutation checkpoint"),
		)
	}
	for _, phase := range []state.Stage4RebuildFinalizationPhase{
		state.Stage4RebuildFinalizationPlanned,
		state.Stage4RebuildFinalizationStarted,
	} {
		receipt, found, err := backends.ready.LoadStage4RebuildFinalization(
			execution.prepared.run.RunID,
			phase,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("read Stage 4 rebuild finalization evidence before checkpoint recovery: %w", err),
			)
		}
		if found || receipt.Finalization.RunID != "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild finalization evidence cannot accompany an incomplete pre-mutation checkpoint"),
			)
		}
	}
	receipts, err := backends.aggregate.LoadStage4TableCompletions(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table completions before checkpoint recovery: %w", err),
		)
	}
	if len(receipts) != 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild table completion cannot accompany an incomplete pre-mutation checkpoint"),
		)
	}
	if !inventoryFound {
		if len(ordinary) != 0 || workFound != 0 {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild lacks immutable inventory but already has ordinary or table work"),
			)
		}
		if err := publishStage4AdapterNetworkWorkInventory(
			ctx,
			execution,
			work,
		); err != nil {
			return err
		}
		inventory, inventoryFound, err = backends.aggregate.LoadStage4TableInventory(
			execution.prepared.run.RunID,
		)
		if err != nil || !inventoryFound {
			if err == nil {
				err = fmt.Errorf("inventory write returned without durable evidence")
			}
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("verify Stage 4 rebuild table inventory after checkpoint recovery: %w", err),
			)
		}
		if err := validateStage4AdapterNetworkRebuildInventory(inventory, work); err != nil {
			return err
		}
	}

	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		execution.prepared.names,
	); err != nil {
		return err
	}
	ordinary, err = stage4AdapterNetworkRebuildOrdinaryTaskSubset(execution)
	if err != nil {
		return err
	}
	if len(ordinary) != len(execution.prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild ordinary task checkpoint remains incomplete after pre-mutation recovery"),
		)
	}
	if err := validateStage4AdapterNetworkRebuildPristineOrdinaryTasks(ordinary); err != nil {
		return err
	}
	if err := ensureStage4AdapterWork(ctx, execution.prepared.run, work); err != nil {
		return err
	}
	return verifyStage4AdapterNetworkRebuildPristineWork(ctx, execution, work)
}

func stage4AdapterNetworkRebuildOrdinaryTaskSubset(
	execution *stage4AdapterNetworkExecution,
) (map[string]state.Task, error) {
	reader, ok := execution.prepared.run.Backend.(stage4RebuildTaskReader)
	if !ok || isNilInterface(reader) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild recovery requires read-only table task evidence"),
		)
	}
	tasks, err := reader.ListTasks(execution.prepared.run.RunID)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild ordinary tasks: %w", err),
		)
	}
	expected := make(map[string]struct{}, len(execution.prepared.plans))
	for _, plan := range execution.prepared.plans {
		expected[plan.source.Name] = struct{}{}
	}
	result := make(map[string]state.Task, len(tasks))
	for _, task := range tasks {
		if task.RunID != execution.prepared.run.RunID {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild ordinary task has a different run"),
			)
		}
		if _, found := expected[task.Table]; !found {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild has unexpected ordinary task %s", task.Table),
			)
		}
		if _, duplicate := result[task.Table]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild duplicates ordinary task %s", task.Table),
			)
		}
		result[task.Table] = task
	}
	return result, nil
}

func validateStage4AdapterNetworkRebuildPristineOrdinaryTasks(
	tasks map[string]state.Task,
) error {
	for _, task := range tasks {
		if task.Status != "running" || task.RowsDone != 0 ||
			task.IntegerWatermark != nil || task.RowNumberWatermark != nil ||
			task.CompletedAt.After(time.Time{}) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild ordinary task %s is not pristine pre-mutation evidence", task.Table),
			)
		}
	}
	return nil
}

func validateStage4AdapterNetworkRebuildPristineWork(
	item stage4AdapterWork,
	task state.WorkTask,
	ranges []state.RangeState,
) error {
	if task.Status != "running" || task.Attempts != 0 || task.Retries != 0 ||
		task.Error != "" || task.CompletedAt.After(time.Time{}) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild work for %s carries transfer authority", item.task.Table),
		)
	}
	for _, workRange := range ranges {
		if workRange.Status != "running" || workRange.Error != "" ||
			workRange.CompletedAt.After(time.Time{}) ||
			stage4AdapterRebuildRangeHasTransferAuthority(workRange) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild range %q for %s carries transfer authority", workRange.ID, item.task.Table),
			)
		}
	}
	return nil
}

func verifyStage4AdapterNetworkRebuildPristineWork(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	work []stage4AdapterWork,
) error {
	durable, err := loadStage4WorkInventory(ctx, execution.prepared.run)
	if err != nil {
		return err
	}
	if err := execution.validateDurableTableTaskInventory(durable); err != nil {
		return err
	}
	for _, item := range work {
		task, ranges, found, err := exactStage4AdapterWork(durable, item, false)
		if err != nil {
			return err
		}
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild checkpoint recovery did not persist work for %s", item.task.Table),
			)
		}
		if err := validateStage4AdapterNetworkRebuildPristineWork(item, task, ranges); err != nil {
			return err
		}
	}
	return nil
}

func runStage4AdapterFreshNetworkRebuild(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
) (Result, error) {
	_, aggregate := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if aggregate {
		// A fresh destructive run needs the terminal-ready receipt before it
		// records any mutable table callback or drops anything. Discovering that
		// the backend cannot persist publication repair after transfer/finalization
		// would otherwise strand a correct target with no admitted restart
		// protocol.
		if _, err := stage4AdapterNetworkRebuildBackends(execution); err != nil {
			return Result{}, err
		}
	}
	for _, plan := range prepared.plans {
		if err := observer.BeforeTable(ctx, plan.source.Name); err != nil {
			return Result{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("checkpoint before %s: %w", plan.source.Name, err),
			)
		}
	}
	if !aggregate {
		// Non-aggregate contexts predate the recovery protocol. They can still
		// execute one fresh set-wide rebuild, but cannot safely admit a later
		// resume; the resume classifier fails closed when its receipts are absent.
		return runStage4AdapterFreshNetworkRebuildWithoutRecovery(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
		)
	}
	backends, err := stage4AdapterNetworkRebuildBackends(execution)
	if err != nil {
		return Result{}, err
	}
	if _, err := stage4AdapterEnsureNetworkRebuildFinalization(
		prepared.run,
		backends,
		state.Stage4RebuildFinalizationPlanned,
	); err != nil {
		return Result{}, err
	}
	return runStage4AdapterNetworkRebuildDataPlane(
		ctx,
		cfg,
		observer,
		target,
		prepared,
		execution,
		nil,
		true,
		false,
	)
}

func runStage4AdapterFreshNetworkRebuildWithoutRecovery(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
) (result Result, resultErr error) {
	targetTables := stage4AdapterRebuildTargetTables(prepared)
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare complete Stage 4 rebuild table set",
		func() error { return target.PrepareTables(ctx, targetTables, prepared.mode) },
	); err != nil {
		return Result{}, stage4AdapterRebuildRerunError(err)
	}
	boundWork := make([]stage4AdapterWork, len(prepared.plans))
	copiedRows := make([]int, len(prepared.plans))
	for planIndex := range prepared.plans {
		tableExecution, err := execution.openTable(ctx, planIndex, false)
		if err != nil {
			return Result{}, stage4AdapterRebuildRerunError(err)
		}
		copied, err := runStage4AdapterStableNetworkRebuildTableData(
			ctx,
			observer,
			tableExecution,
		)
		if err != nil {
			return Result{}, err
		}
		copiedRows[planIndex] = copied
		boundWork[planIndex] = cloneStage4AdapterNetworkWork(tableExecution.work)
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"finalize complete Stage 4 rebuild table set",
		func() error { return target.FinalizeTables(ctx, targetTables, prepared.mode) },
	); err != nil {
		return Result{}, err
	}
	for planIndex := range prepared.plans {
		if err := validateStage4AdapterStableTable(
			ctx,
			cfg,
			observer,
			execution.source,
			execution.source,
			target,
			prepared,
			planIndex,
		); err != nil {
			return Result{}, err
		}
	}
	for planIndex, plan := range prepared.plans {
		if err := completeStage4AdapterNetworkTable(
			ctx,
			observer,
			prepared.run,
			nil,
			boundWork[planIndex],
			plan.source.Name,
			copiedRows[planIndex],
		); err != nil {
			return result, fmt.Errorf("complete Stage 4 work for %s: %w", plan.source.Name, err)
		}
		result.Tables++
		result.Rows += copiedRows[planIndex]
	}
	result.Validated = true
	return result, nil
}

func runStage4AdapterRecoveredNetworkRebuild(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	recovery stage4RebuildRecovery,
	prepare bool,
) (Result, error) {
	return runStage4AdapterNetworkRebuildDataPlane(
		ctx,
		cfg,
		observer,
		target,
		prepared,
		execution,
		&recovery,
		prepare,
		true,
	)
}

// runStage4AdapterNetworkRebuildDataPlane has exactly one set-wide prepare
// boundary.  It deliberately never invokes ResetWorkPlan: existing durable
// ranges either prove an exact duplicate-safe replay or prove that no target
// transfer authority exists and a fresh set-wide prepare is required.
func runStage4AdapterNetworkRebuildDataPlane(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	recovery *stage4RebuildRecovery,
	prepare bool,
	resume bool,
) (result Result, resultErr error) {
	// Keep this check at the first data-plane boundary as well as at fresh-run
	// admission.  Recovered callers should never be able to reach PrepareTables
	// through a future path that skipped the fresh admission helper.
	backends, err := stage4AdapterNetworkRebuildBackends(execution)
	if err != nil {
		return Result{}, err
	}
	if _, err := stage4AdapterEnsureNetworkRebuildFinalization(
		execution.prepared.run,
		backends,
		state.Stage4RebuildFinalizationPlanned,
	); err != nil {
		return Result{}, err
	}
	mutationObserver := observer
	if resume {
		mutationObserver = adapterResumeMutationGuard{
			ctx:      ctx,
			delegate: observer,
			boundary: "mutate resumed Stage 4 rebuild",
		}
	}
	targetTables := stage4AdapterRebuildTargetTables(prepared)
	preparedTarget := false
	if prepare {
		if _, err := protectAdapterTargetMutationOnce(
			ctx,
			mutationObserver,
			"prepare complete Stage 4 rebuild table set",
			func() error {
				return target.PrepareTables(ctx, targetTables, prepared.mode)
			},
		); err != nil {
			return Result{}, stage4AdapterRebuildRerunError(err)
		}
		preparedTarget = true
	}

	copiedRows := make([]int, len(prepared.plans))
	boundWork := make([]stage4AdapterWork, len(prepared.plans))
	for planIndex := range prepared.plans {
		tableExecution, err := execution.openTable(ctx, planIndex, false)
		if err != nil {
			return Result{}, stage4AdapterRebuildDataPlaneError(
				execution,
				err,
				preparedTarget,
			)
		}
		copied, err := runStage4AdapterStableNetworkRebuildTableData(
			ctx,
			mutationObserver,
			tableExecution,
		)
		if err != nil {
			return Result{}, stage4AdapterRebuildDataPlaneError(
				execution,
				err,
				preparedTarget,
			)
		}
		copiedRows[planIndex] = copied
		boundWork[planIndex] = cloneStage4AdapterNetworkWork(
			tableExecution.work,
		)
	}

	finalizationStarted := recovery != nil && recovery.finalizationStarted
	if finalizationStarted {
		if err := stage4AdapterAuthenticateNetworkRebuildFinalization(
			ctx,
			mutationObserver,
			target,
			targetTables,
		); err != nil {
			return Result{}, err
		}
	} else {
		if _, err := stage4AdapterEnsureNetworkRebuildFinalization(
			execution.prepared.run,
			backends,
			state.Stage4RebuildFinalizationStarted,
		); err != nil {
			return Result{}, err
		}
		if _, err := protectAdapterTargetMutationOnce(
			ctx,
			mutationObserver,
			"finalize complete Stage 4 rebuild table set",
			func() error {
				return target.FinalizeTables(ctx, targetTables, prepared.mode)
			},
		); err != nil {
			return Result{}, stage4AdapterRebuildDataPlaneError(
				execution,
				err,
				preparedTarget,
			)
		}
	}
	for planIndex := range prepared.plans {
		if err := validateStage4AdapterStableTable(
			ctx,
			cfg,
			observer,
			execution.source,
			execution.source,
			target,
			prepared,
			planIndex,
		); err != nil {
			return Result{}, stage4AdapterRebuildDataPlaneError(
				execution,
				err,
				preparedTarget,
			)
		}
	}

	if err := stage4AdapterSaveNetworkRebuildReady(
		execution.prepared.run,
		backends,
	); err != nil {
		return Result{}, err
	}
	if recovery == nil {
		recovery = &stage4RebuildRecovery{
			aggregate: backends.aggregate,
			ready:     backends.ready,
			work:      boundWork,
			receipts:  make(map[string]state.Stage4TableCompletionReceipt),
		}
	}
	// A terminal-ready receipt is intentionally persisted before this loop.  If
	// either the aggregate mutation or its callback fails, a later process can
	// authenticate the finished target and replay the exact stored receipt
	// instead of re-dropping correct tables.
	recovery.work = boundWork
	recovery.rows = make(map[string]int, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		recovery.rows[plan.source.Name] = copiedRows[planIndex]
	}
	return publishStage4AdapterRecoveredNetworkRebuild(
		ctx,
		cfg,
		observer,
		execution.source,
		target,
		prepared,
		*recovery,
	)
}

type stage4AdapterRebuildBackends struct {
	aggregate state.Stage4AggregateBackend
	ready     state.Stage4RebuildRecoveryBackend
}

// admitStage4AdapterNetworkRebuildRecovery is a read-only capability and
// evidence check. Aggregate-composed rebuilds must prove that terminal-ready
// evidence can be read before any ordinary task checkpoint is created. A
// backend wrapper that advertises the interface but cannot read its underlying
// evidence is rejected here as well, rather than after target preparation.
// Legacy non-aggregate test contexts retain their fresh-only path.
func admitStage4AdapterNetworkRebuildRecovery(run Stage4RunContext) error {
	aggregate, ok := run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil
	}
	ready, ok := run.Backend.(state.Stage4RebuildRecoveryBackend)
	if !ok || isNilInterface(ready) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild requires durable terminal-recovery evidence"),
		)
	}
	if _, _, err := ready.LoadStage4RebuildFinalization(
		run.RunID,
		state.Stage4RebuildFinalizationPlanned,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 rebuild finalization evidence during admission: %w",
				err,
			),
		)
	}
	if _, _, err := ready.LoadStage4RebuildFinalization(
		run.RunID,
		state.Stage4RebuildFinalizationStarted,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 rebuild finalization evidence during admission: %w",
				err,
			),
		)
	}
	if _, _, err := ready.LoadStage4RebuildReady(run.RunID); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 rebuild terminal-recovery evidence during admission: %w",
				err,
			),
		)
	}
	return nil
}

func stage4AdapterNetworkRebuildBackends(
	execution *stage4AdapterNetworkExecution,
) (stage4AdapterRebuildBackends, error) {
	if execution == nil {
		return stage4AdapterRebuildBackends{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild execution is unavailable"),
		)
	}
	aggregate, ok := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return stage4AdapterRebuildBackends{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild requires aggregate table evidence"),
		)
	}
	ready, ok := execution.prepared.run.Backend.(state.Stage4RebuildRecoveryBackend)
	if !ok || isNilInterface(ready) {
		return stage4AdapterRebuildBackends{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild requires durable terminal-recovery evidence"),
		)
	}
	execution.aggregate = aggregate
	return stage4AdapterRebuildBackends{aggregate: aggregate, ready: ready}, nil
}

func stage4AdapterRebuildTargetTables(
	prepared stage4AdapterPrepared,
) []schema.Table {
	tables := make([]schema.Table, len(prepared.targetTables))
	for index, table := range prepared.targetTables {
		tables[index] = cloneStage4RichTable(table)
	}
	return tables
}

func stage4AdapterSaveNetworkRebuildReady(
	run Stage4RunContext,
	backends stage4AdapterRebuildBackends,
) error {
	inventory, found, err := backends.aggregate.LoadStage4TableInventory(run.RunID)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table inventory: %w", err),
		)
	}
	if !found {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild is missing durable table inventory"),
		)
	}
	started, startedFound, err := backends.ready.LoadStage4RebuildFinalization(
		run.RunID,
		state.Stage4RebuildFinalizationStarted,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild finalization boundary: %w", err),
		)
	}
	if !startedFound ||
		started.Finalization.InventoryDigest != inventory.Digest {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 rebuild terminal readiness requires matching finalization evidence",
			),
		)
	}
	if err := backends.ready.SaveStage4RebuildReady(state.Stage4RebuildReady{
		RunID:           run.RunID,
		InventoryDigest: inventory.Digest,
		ValidatedAt:     time.Now().UTC(),
	}); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("record Stage 4 rebuild terminal readiness: %w", err),
		)
	}
	return nil
}

// stage4AdapterEnsureNetworkRebuildFinalization persists a phase before the
// corresponding target boundary. The backend creates its own immutable
// timestamp so a recovery sees the original receipt rather than a newly
// minted authority.
func stage4AdapterEnsureNetworkRebuildFinalization(
	run Stage4RunContext,
	backends stage4AdapterRebuildBackends,
	phase state.Stage4RebuildFinalizationPhase,
) (state.Stage4RebuildFinalizationReceipt, error) {
	inventory, found, err := backends.aggregate.LoadStage4TableInventory(run.RunID)
	if err != nil {
		return state.Stage4RebuildFinalizationReceipt{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table inventory: %w", err),
		)
	}
	if !found {
		return state.Stage4RebuildFinalizationReceipt{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild is missing durable table inventory"),
		)
	}
	finalization, err := state.NewStage4RebuildFinalization(
		run.RunID,
		inventory.Digest,
		phase,
	)
	if err != nil {
		return state.Stage4RebuildFinalizationReceipt{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("build Stage 4 rebuild finalization evidence: %w", err),
		)
	}
	receipt, _, err := backends.ready.EnsureStage4RebuildFinalization(
		finalization,
	)
	if err != nil {
		return state.Stage4RebuildFinalizationReceipt{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("record Stage 4 rebuild finalization evidence: %w", err),
		)
	}
	return receipt, nil
}

// stage4AdapterAuthenticateNetworkRebuildFinalization is deliberately
// read-only. A durable "started" receipt means FinalizeTables may have
// committed before the prior process died. Exact retained-shape preflight is
// the only admitted proof that allows the runner to skip that non-idempotent
// call. Any other result fails closed rather than attempting finalization a
// second time.
func stage4AdapterAuthenticateNetworkRebuildFinalization(
	ctx context.Context,
	observer TableObserver,
	target targetAdapter,
	targetTables []schema.Table,
) error {
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"authenticate finalized Stage 4 rebuild table set",
		func() error {
			return target.PreflightTables(ctx, targetTables, "upsert")
		},
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 rebuild finalization is uncertain after its durable start boundary; the exact finalized target state could not be authenticated: %w; do not automatically rerun finalization or drop/recreate, explicitly verify or invalidate the target before rerunning the rebuild",
				err,
			),
		)
	}
	return nil
}

func stage4AdapterRebuildRerunError(err error) error {
	return fmt.Errorf(
		"%w; target preparation may be partial and rerunning drop_recreate mode is the recovery path",
		err,
	)
}

func stage4AdapterRebuildDataPlaneError(
	execution *stage4AdapterNetworkExecution,
	err error,
	preparedTarget bool,
) error {
	if !preparedTarget {
		return err
	}
	authority, authorityErr := stage4AdapterRebuildHasTransferAuthority(execution)
	if authorityErr != nil {
		return fmt.Errorf("%w; inspect durable rebuild recovery evidence: %v", err, authorityErr)
	}
	if !authority {
		return stage4AdapterRebuildRerunError(err)
	}
	return fmt.Errorf(
		"%w; same-run durable issued/checkpoint evidence was retained, so resume uses exact duplicate-safe rebuild replay without re-dropping the target set",
		err,
	)
}

func stage4AdapterRebuildHasTransferAuthority(
	execution *stage4AdapterNetworkExecution,
) (bool, error) {
	inventory, err := loadStage4WorkInventory(context.Background(), execution.prepared.run)
	if err != nil {
		return false, err
	}
	if err := execution.validateDurableTableTaskInventory(inventory); err != nil {
		return false, err
	}
	for key, task := range inventory.tasks {
		if !stage4AdapterDurableTableTaskType(key.Type) {
			continue
		}
		if stage4AdapterRebuildTaskHasTransferAuthority(task) {
			return true, nil
		}
		for _, workRange := range inventory.ranges[key] {
			if stage4AdapterRebuildRangeHasTransferAuthority(workRange) {
				return true, nil
			}
		}
	}
	return false, nil
}

func stage4AdapterRebuildTaskHasTransferAuthority(task state.WorkTask) bool {
	return task.Status == "completed" || task.Attempts != 0 || task.Retries != 0
}

func stage4AdapterRebuildRangeHasTransferAuthority(workRange state.RangeState) bool {
	return workRange.Status == "completed" ||
		workRange.NextSequence != 0 ||
		workRange.SequenceOffset != 0 ||
		workRange.RowsDone != 0 ||
		workRange.CommittedPrefix != 0 ||
		workRange.FrontierValid ||
		len(workRange.Frontier) != 0 ||
		len(workRange.Pending) != 0 ||
		workRange.Attempts != 0 ||
		workRange.Retries != 0
}

func classifyStage4AdapterNetworkRebuildRecovery(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
) (stage4RebuildRecovery, error) {
	backends, err := stage4AdapterNetworkRebuildBackends(execution)
	if err != nil {
		return stage4RebuildRecovery{}, err
	}
	work, err := stage4AdapterPlanNetworkRebuildRecoveryWork(ctx, execution)
	if err != nil {
		return stage4RebuildRecovery{}, err
	}
	inventory, found, err := backends.aggregate.LoadStage4TableInventory(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table inventory: %w", err),
		)
	}
	if !found {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild resume lacks immutable table inventory"),
		)
	}
	if err := validateStage4AdapterNetworkRebuildInventory(inventory, work); err != nil {
		return stage4RebuildRecovery{}, err
	}
	durable, err := loadStage4WorkInventory(ctx, execution.prepared.run)
	if err != nil {
		return stage4RebuildRecovery{}, err
	}
	if err := execution.validateDurableTableTaskInventory(durable); err != nil {
		return stage4RebuildRecovery{}, err
	}

	rows := make(map[string]int, len(work))
	hasAuthority := false
	allComplete := true
	for _, item := range work {
		task, ranges, found, exactErr := exactStage4AdapterWork(
			durable,
			item,
			false,
		)
		if exactErr != nil {
			return stage4RebuildRecovery{}, exactErr
		}
		if !found {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild resume is missing durable work for %s", item.task.Table),
			)
		}
		if task.RunID != execution.prepared.run.RunID {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild work for %s belongs to another run", item.task.Table),
			)
		}
		for _, workRange := range ranges {
			if workRange.RunID != execution.prepared.run.RunID ||
				workRange.Task != item.task {
				return stage4RebuildRecovery{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf("Stage 4 rebuild range for %s belongs to another run", item.task.Table),
				)
			}
		}
		tableRows, complete, authority, stateErr :=
			stage4AdapterNetworkRebuildTableState(item, task, ranges)
		if stateErr != nil {
			return stage4RebuildRecovery{}, stateErr
		}
		rows[item.task.Table] = tableRows
		allComplete = allComplete && complete
		hasAuthority = hasAuthority || authority
	}

	ordinary, err := stage4AdapterNetworkRebuildOrdinaryTasks(execution)
	if err != nil {
		return stage4RebuildRecovery{}, err
	}
	receipts, err := stage4AdapterNetworkRebuildReceipts(
		backends.aggregate,
		execution.prepared.run.RunID,
		work,
		durable,
		ordinary,
		rows,
		completed,
	)
	if err != nil {
		return stage4RebuildRecovery{}, err
	}

	readyReceipt, terminalReady, err := backends.ready.LoadStage4RebuildReady(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild terminal readiness: %w", err),
		)
	}
	plannedReceipt, finalizationPlanned, err :=
		backends.ready.LoadStage4RebuildFinalization(
			execution.prepared.run.RunID,
			state.Stage4RebuildFinalizationPlanned,
		)
	if err != nil {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild finalization plan: %w", err),
		)
	}
	startedReceipt, finalizationStarted, err :=
		backends.ready.LoadStage4RebuildFinalization(
			execution.prepared.run.RunID,
			state.Stage4RebuildFinalizationStarted,
		)
	if err != nil {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild finalization start: %w", err),
		)
	}
	if finalizationPlanned &&
		plannedReceipt.Finalization.InventoryDigest != inventory.Digest {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild finalization plan differs from current inventory"),
		)
	}
	if finalizationStarted &&
		startedReceipt.Finalization.InventoryDigest != inventory.Digest {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild finalization start differs from current inventory"),
		)
	}
	if terminalReady {
		if readyReceipt.Ready.RunID != execution.prepared.run.RunID ||
			readyReceipt.Ready.InventoryDigest != inventory.Digest {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild terminal readiness differs from current inventory"),
			)
		}
		if !allComplete {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild terminal readiness has incomplete durable ranges"),
			)
		}
		return stage4RebuildRecovery{
			kind:      stage4RebuildPublication,
			aggregate: backends.aggregate,
			ready:     backends.ready,
			inventory: inventory,
			work:      work,
			receipts:  receipts,
			rows:      rows,
		}, nil
	}
	if finalizationStarted && !finalizationPlanned {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild finalization start lacks its planned boundary"),
		)
	}
	if finalizationStarted && !allComplete {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild finalization start has incomplete durable ranges"),
		)
	}
	if len(receipts) != 0 {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild has table publication without terminal readiness"),
		)
	}
	for _, item := range work {
		if durable.tasks[item.task].Status != "running" {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild work task %s is terminal without aggregate publication", item.task.Table),
			)
		}
	}
	for _, task := range ordinary {
		if task.Status != "running" || task.RowsDone != 0 ||
			!task.CompletedAt.IsZero() {
			return stage4RebuildRecovery{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild ordinary task %s is unsafe for recovery", task.Table),
			)
		}
	}
	if hasAuthority && !finalizationPlanned {
		return stage4RebuildRecovery{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 rebuild has transfer authority but lacks the durable finalization plan; target finalization cannot be safely classified",
			),
		)
	}

	kind := stage4RebuildRerun
	if hasAuthority {
		kind = stage4RebuildReplay
	}
	return stage4RebuildRecovery{
		kind:                kind,
		aggregate:           backends.aggregate,
		ready:               backends.ready,
		inventory:           inventory,
		finalizationStarted: finalizationStarted,
		work:                work,
		receipts:            receipts,
		rows:                rows,
	}, nil
}

// stage4AdapterPlanNetworkRebuildRecoveryWork recomputes the exact stable
// source plans without mutating state.  It runs before openTable(false), whose
// EnsureWorkPlan operation is intentionally idempotent but must never be used
// to manufacture missing recovery evidence.
func stage4AdapterPlanNetworkRebuildRecoveryWork(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
) ([]stage4AdapterWork, error) {
	if execution == nil || !execution.deferred {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild recovery has no deferred execution"),
		)
	}
	execution.mu.Lock()
	if execution.binding || execution.bound || execution.started {
		execution.mu.Unlock()
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild recovery execution is already active"),
		)
	}
	execution.nextGlobalRange = 0
	execution.mu.Unlock()
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	work := make([]stage4AdapterWork, len(execution.prepared.plans))
	for planIndex := range execution.prepared.plans {
		tableExecution, err := execution.planTableOnce(ctx, planIndex)
		if err != nil {
			return nil, err
		}
		work[planIndex] = cloneStage4AdapterNetworkWork(tableExecution.work)
		if closeErr := tableExecution.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"close Stage 4 stable source after recovery planning %s: %w",
				execution.prepared.plans[planIndex].source.Name,
				closeErr,
			)
		}
	}
	return work, nil
}

func validateStage4AdapterNetworkRebuildInventory(
	receipt state.Stage4TableInventoryReceipt,
	work []stage4AdapterWork,
) error {
	if len(receipt.Inventory.Tables) != len(work) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild table inventory size changed"),
		)
	}
	entries := make(map[state.TaskKey]state.Stage4TableInventoryEntry, len(work))
	for _, entry := range receipt.Inventory.Tables {
		if _, duplicate := entries[entry.Task]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild table inventory duplicates %#v", entry.Task),
			)
		}
		entries[entry.Task] = entry
	}
	for _, item := range work {
		entry, found := entries[item.task]
		if !found || entry.Table != item.task.Table ||
			entry.Strategy != item.strategy || entry.TopologyHash != item.topology ||
			len(entry.Ranges) != len(item.ranges) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild table inventory changed for %s", item.task.Table),
			)
		}
		expected := make([]string, len(item.ranges))
		actual := make([]string, len(entry.Ranges))
		for index, workRange := range item.ranges {
			expected[index] = workRange.ID
		}
		for index, workRange := range entry.Ranges {
			actual[index] = workRange.ID
		}
		sort.Strings(expected)
		sort.Strings(actual)
		for index := range expected {
			if expected[index] != actual[index] {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("Stage 4 rebuild range inventory changed for %s", item.task.Table),
				)
			}
		}
	}
	return nil
}

func stage4AdapterNetworkRebuildTableState(
	item stage4AdapterWork,
	task state.WorkTask,
	ranges []state.RangeState,
) (int, bool, bool, error) {
	if task.RunID == "" || task.Error != "" {
		return 0, false, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild work task %s is invalid", item.task.Table),
		)
	}
	if len(ranges) != len(item.ranges) {
		return 0, false, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild ranges changed for %s", item.task.Table),
		)
	}
	rows := int64(0)
	complete := true
	authority := stage4AdapterRebuildTaskHasTransferAuthority(task)
	for index, workRange := range ranges {
		restore, err := networkRestoreFromState(networkStateRangeBinding{
			RangeIndex: uint64(index),
			Task:       item.task,
			Initial:    item.ranges[index],
		}, workRange)
		if err != nil {
			return 0, false, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild range %q is invalid: %w", workRange.ID, err),
			)
		}
		if workRange.Error != "" || workRange.RowsDone > math.MaxInt64-rows {
			return 0, false, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild range %q has unsafe durable state", workRange.ID),
			)
		}
		rows += workRange.RowsDone
		terminal := workRange.Status == "completed" && restore.Complete &&
			restore.SequenceOffset == 0 && len(restore.Issued) == 0 &&
			workRange.CommittedPrefix == 0 && workRange.CompletedAt.After(time.Time{}) &&
			workRange.UpdatedAt.Equal(workRange.CompletedAt)
		complete = complete && terminal
		authority = authority || stage4AdapterRebuildRangeHasTransferAuthority(workRange)
	}
	if rows > int64(math.MaxInt) {
		return 0, false, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild row total overflows for %s", item.task.Table),
		)
	}
	return int(rows), complete, authority, nil
}

func stage4AdapterNetworkRebuildOrdinaryTasks(
	execution *stage4AdapterNetworkExecution,
) (map[string]state.Task, error) {
	reader, ok := execution.prepared.run.Backend.(stage4RebuildTaskReader)
	if !ok || isNilInterface(reader) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild recovery requires read-only table task evidence"),
		)
	}
	tasks, err := reader.ListTasks(execution.prepared.run.RunID)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild ordinary tasks: %w", err),
		)
	}
	expected := make(map[string]struct{}, len(execution.prepared.plans))
	for _, plan := range execution.prepared.plans {
		expected[plan.source.Name] = struct{}{}
	}
	result := make(map[string]state.Task, len(tasks))
	for _, task := range tasks {
		if task.RunID != execution.prepared.run.RunID {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild ordinary task has a different run"),
			)
		}
		if _, expected := expected[task.Table]; !expected {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild has unexpected ordinary task %s", task.Table),
			)
		}
		if _, duplicate := result[task.Table]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild duplicates ordinary task %s", task.Table),
			)
		}
		result[task.Table] = task
	}
	if len(result) != len(expected) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild ordinary task inventory is incomplete"),
		)
	}
	return result, nil
}

func stage4AdapterNetworkRebuildReceipts(
	aggregate state.Stage4AggregateBackend,
	runID string,
	work []stage4AdapterWork,
	durable stage4WorkInventory,
	ordinary map[string]state.Task,
	rows map[string]int,
	completed map[string]int,
) (map[string]state.Stage4TableCompletionReceipt, error) {
	stored, err := aggregate.LoadStage4TableCompletions(runID)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 rebuild table completions: %w", err),
		)
	}
	expected := make(map[string]stage4AdapterWork, len(work))
	for _, item := range work {
		expected[item.task.Table] = item
	}
	result := make(map[string]state.Stage4TableCompletionReceipt, len(stored))
	for _, receipt := range stored {
		completion := receipt.Completion
		item, found := expected[completion.Table]
		if !found || completion.RunID != runID || completion.Task != item.task ||
			completion.TopologyHash != item.topology || completion.RowsDone < 0 ||
			completion.CompletedAt.IsZero() {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild completion receipt is outside the current inventory"),
			)
		}
		if _, duplicate := result[completion.Table]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild duplicates completion receipt for %s", completion.Table),
			)
		}
		if completion.RowsDone != rows[completion.Table] ||
			!stage4AdapterNetworkRebuildCompletionRangesEqual(
				item,
				durable.ranges[item.task],
				completion.Ranges,
			) {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild completion receipt differs from durable ranges for %s", completion.Table),
			)
		}
		ordinaryTask, found := ordinary[completion.Table]
		if !found || ordinaryTask.Status != "completed" ||
			ordinaryTask.RowsDone != completion.RowsDone ||
			ordinaryTask.CompletedAt.IsZero() {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild completion receipt differs from ordinary task %s", completion.Table),
			)
		}
		result[completion.Table] = receipt
	}
	for table, task := range ordinary {
		_, hasReceipt := result[table]
		if task.Status == "completed" != hasReceipt {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild ordinary completion differs from aggregate receipt for %s", table),
			)
		}
		checkpoint, passed := completed[table]
		if hasReceipt {
			if !passed || checkpoint != result[table].Completion.RowsDone {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf("Stage 4 rebuild supplied completion differs from durable receipt for %s", table),
				)
			}
		} else if passed {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild supplied completion lacks aggregate receipt for %s", table),
			)
		}
	}
	for table := range completed {
		if _, exists := ordinary[table]; !exists {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild supplied completion is outside the current selection: %s", table),
			)
		}
	}
	return result, nil
}

func stage4AdapterNetworkRebuildCompletionRangesEqual(
	item stage4AdapterWork,
	durable []state.RangeState,
	completed []state.Stage4RangeCompletion,
) bool {
	if len(durable) != len(item.ranges) || len(completed) != len(item.ranges) {
		return false
	}
	expected := make(map[string]uint64, len(item.ranges))
	for _, workRange := range durable {
		if workRange.Task != item.task {
			return false
		}
		if _, duplicate := expected[workRange.ID]; duplicate {
			return false
		}
		expected[workRange.ID] = workRange.NextSequence
	}
	for _, completion := range completed {
		nextSequence, found := expected[completion.ID]
		if !found || completion.NextSequence != nextSequence {
			return false
		}
		delete(expected, completion.ID)
	}
	return len(expected) == 0
}

// publishStage4AdapterRecoveredNetworkRebuild first authenticates each target
// count against its own durable range total.  It then reuses any existing
// aggregate receipt verbatim, preserving its CompletedAt across a callback
// failure, and creates receipts only for tables that have not yet been
// atomically published.
func publishStage4AdapterRecoveredNetworkRebuild(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	recovery stage4RebuildRecovery,
) (Result, error) {
	if len(recovery.work) != len(prepared.plans) {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild publication lacks durable work inventory"),
		)
	}
	rows := make(map[string]int, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		item := recovery.work[planIndex]
		rowsDone, found := recovery.rows[plan.source.Name]
		if !found {
			var err error
			rowsDone, err = stage4AdapterNetworkRebuildDurableRows(
				prepared.run,
				item,
			)
			if err != nil {
				return Result{}, err
			}
		}
		targetRows, err := target.CountRows(ctx, plan.target)
		if err != nil {
			return Result{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("authenticate Stage 4 rebuild target %s: %w", plan.source.Name, err),
			)
		}
		if targetRows != rowsDone {
			return Result{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild target %s has %d rows, durable ranges require %d", plan.source.Name, targetRows, rowsDone),
			)
		}
		if err := validateStage4AdapterStableTable(
			ctx,
			cfg,
			observer,
			source,
			source,
			target,
			prepared,
			planIndex,
		); err != nil {
			return Result{}, err
		}
		rows[plan.source.Name] = rowsDone
	}

	result := Result{Validated: true}
	for planIndex, plan := range prepared.plans {
		rowsDone := rows[plan.source.Name]
		if receipt, found := recovery.receipts[plan.source.Name]; found {
			if err := publishStage4AdapterNetworkTableCompletion(
				ctx,
				observer,
				recovery.aggregate,
				receipt.Completion,
			); err != nil {
				return result, fmt.Errorf("complete Stage 4 work for %s: %w", plan.source.Name, err)
			}
		} else if err := completeStage4AdapterNetworkTable(
			ctx,
			observer,
			prepared.run,
			recovery.aggregate,
			recovery.work[planIndex],
			plan.source.Name,
			rowsDone,
		); err != nil {
			return result, fmt.Errorf("complete Stage 4 work for %s: %w", plan.source.Name, err)
		}
		result.Tables++
		result.Rows += rowsDone
	}
	return result, nil
}

func stage4AdapterNetworkRebuildDurableRows(
	run Stage4RunContext,
	item stage4AdapterWork,
) (int, error) {
	inventory, err := loadStage4WorkInventory(context.Background(), run)
	if err != nil {
		return 0, err
	}
	_, ranges, _, err := exactStage4AdapterWork(inventory, item, false)
	if err != nil {
		return 0, err
	}
	var rows int64
	for index, workRange := range ranges {
		restore, restoreErr := networkRestoreFromState(networkStateRangeBinding{
			RangeIndex: uint64(index),
			Task:       item.task,
			Initial:    item.ranges[index],
		}, workRange)
		if restoreErr != nil || !restore.Complete || restore.SequenceOffset != 0 ||
			len(restore.Issued) != 0 || workRange.Status != "completed" ||
			workRange.Error != "" || workRange.RowsDone > math.MaxInt64-rows {
			if restoreErr == nil {
				restoreErr = fmt.Errorf("range is not terminal")
			}
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 rebuild range %q is not publishable: %w", workRange.ID, restoreErr),
			)
		}
		rows += workRange.RowsDone
	}
	if rows > int64(math.MaxInt) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 rebuild row total overflows for %s", item.task.Table),
		)
	}
	return int(rows), nil
}
