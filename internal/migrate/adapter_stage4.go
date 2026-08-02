package migrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func migrateWithStage4Adapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	run Stage4RunContext,
) (Result, error) {
	if _, err := requireStage4TableSetObserver(observer); err != nil {
		return Result{}, err
	}
	if err := requireStage4StrictRoute(
		cfg,
		source,
		target,
		mode,
	); err != nil {
		return Result{}, err
	}
	prepared, err := prepareStage4AdapterRun(
		ctx,
		cfg,
		observer,
		source,
		target,
		mode,
		run,
	)
	if err != nil {
		return Result{}, err
	}
	if prepared.incremental != nil {
		result, runErr := migrateWithStage4IncrementalAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			false,
			nil,
		)
		if result.RuntimeTuning == nil {
			result.RuntimeTuning =
				stage4GeneratedIncrementalRuntimeTuningReport(
					cfg.Migration,
				)
		}
		return result, runErr
	}
	var networkOptions []stage4AdapterNetworkAdmissionOption
	if cfg.Migration.StrictConsistency {
		networkOptions = append(
			networkOptions,
			withStage4StrictSnapshotComposition(),
		)
	}
	if prepared.deletes != nil {
		networkOptions = append(
			networkOptions,
			withStage4DeleteReconciliationComposition(),
		)
	}
	networkExecution, err := admitStage4AdapterNetworkTransfer(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		nil,
		networkOptions...,
	)
	if err != nil {
		return Result{}, err
	}
	if cfg.Migration.StrictConsistency {
		var (
			result Result
			runErr error
		)
		switch source.Engine() {
		case "postgres":
			result, runErr = migrateWithStage4PostgresStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		case "mssql":
			result, runErr = migrateWithStage4SQLServerStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		case "mysql":
			result, runErr = migrateWithStage4MySQLStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, false, nil,
			)
		case "sqlite":
			result, runErr = migrateWithStage4SQLiteStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, false, nil,
			)
		default:
			return Result{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 strict consistency has no composed runner for source engine %q",
					source.Engine(),
				),
			)
		}
		attachStage4AdapterRuntimeTuningReport(&result, networkExecution)
		return result, runErr
	}
	if networkExecution != nil && networkExecution.deferred {
		if err := checkpointStage4AdapterStableNetworkWork(
			ctx,
			observer,
			networkExecution,
			false,
			nil,
		); err != nil {
			return Result{}, err
		}
		if err := applyStage4AdapterTargetSchema(
			ctx,
			observer,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		); err != nil {
			return Result{}, err
		}
		if err := preflightStage4AdapterDesiredTargetAfterEvolution(
			ctx,
			target,
			prepared,
		); err != nil {
			return Result{}, err
		}
		if err := activateStage4AdapterPostgresDeleteComposition(
			ctx,
			prepared,
		); err != nil {
			return Result{}, err
		}
		if err := prevalidateStage4AdapterPostgresDeleteCompletedTargets(
			ctx,
			target,
			prepared,
			networkExecution,
		); err != nil {
			return Result{}, err
		}
		if err := ensureStage4AdapterDeleteJournalReadiness(
			ctx,
			observer,
			prepared,
		); err != nil {
			return Result{}, err
		}
		var result Result
		if prepared.deletes != nil {
			result, err = runStage4AdapterPostgresDeleteNetworkTables(
				ctx,
				cfg,
				observer,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		} else {
			result, err = runStage4AdapterStableNetworkTables(
				ctx,
				cfg,
				observer,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		}
		if err != nil {
			return result, err
		}
		if err := completeStage4AdapterTerminalSchemaGateSentinels(
			ctx,
			prepared,
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	if err := checkpointStage4AdapterWork(
		ctx,
		observer,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := applyStage4AdapterTargetSchema(
		ctx,
		observer,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return Result{}, err
	}
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(
		ctx,
		target,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := activateStage4AdapterPostgresDeleteComposition(
		ctx,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := ensureStage4AdapterDeleteJournalReadiness(
		ctx,
		observer,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if networkExecution != nil {
		if err := bindStage4AdapterNetworkRestoresAndValidate(
			ctx,
			observer,
			networkExecution,
		); err != nil {
			return Result{}, err
		}
	}

	networkTransferStarted := false
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare Stage 4 tables",
		func() error {
			return target.PrepareTables(
				ctx,
				prepared.targetTables,
				mode,
			)
		},
	); err != nil {
		return Result{}, err
	}

	copiedRows := make([]int, len(prepared.plans))
	if networkExecution != nil {
		for _, plan := range prepared.plans {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			if err := observer.BeforeTable(
				ctx,
				plan.source.Name,
			); err != nil {
				return Result{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"checkpoint before %s: %w",
						plan.source.Name,
						err,
					),
				)
			}
		}
		networkTransferStarted = true
		copiedRows, err = runStage4AdapterNetworkTransfer(
			ctx,
			observer,
			networkExecution,
		)
		if err != nil {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
			return Result{}, err
		}
	} else {
		for index, plan := range prepared.plans {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			if observer != nil {
				if err := observer.BeforeTable(
					ctx,
					plan.source.Name,
				); err != nil {
					return Result{}, NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"checkpoint before %s: %w",
							plan.source.Name,
							err,
						),
					)
				}
			}
			copied, copyErr := copyAdapterRows(
				ctx,
				observer,
				source,
				target,
				plan.source,
				plan.target,
				plan.columns,
				mode,
			)
			if copyErr != nil {
				return Result{}, copyErr
			}
			copiedRows[index] = copied
		}
	}

	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"finalize Stage 4 tables",
		func() error {
			return target.FinalizeTables(
				ctx,
				prepared.targetTables,
				mode,
			)
		},
	); err != nil {
		if networkTransferStarted {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
		}
		return Result{}, err
	}
	if err := validateStage4AdapterRun(
		ctx,
		cfg,
		source,
		target,
		prepared,
	); err != nil {
		if networkTransferStarted {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
		}
		return Result{}, err
	}

	result := Result{}
	for index, plan := range prepared.plans {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		copied := copiedRows[index]
		if observer != nil {
			if err := observer.AfterTable(
				ctx,
				plan.source.Name,
				copied,
			); err != nil {
				return result, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"checkpoint after %s: %w",
						plan.source.Name,
						err,
					),
				)
			}
		}
		result.Tables++
		result.Rows += copied
	}
	if err := completeStage4AdapterWork(
		ctx,
		prepared.run,
		prepared.work,
	); err != nil {
		return result, err
	}
	if err := completeStage4AdapterTerminalSchemaGateSentinels(
		ctx,
		prepared,
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

func checkpointStage4AdapterTableSet(
	ctx context.Context,
	observer TableObserver,
	names []string,
) error {
	setObserver, err := requireStage4TableSetObserver(observer)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := setObserver.BeforeTables(
		ctx,
		append([]string(nil), names...),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint Stage 4 table set: %w", err),
		)
	}
	return ctx.Err()
}

// checkpointStage4AdapterStableNetworkWork materializes and checkpoints every
// table's exact stable pagination plan before schema DDL is permitted. The
// stable sessions are intentionally closed after checkpointing; execution
// reopens each table and requires the newly observed plan to match the durable
// one before preparing or writing that table.
func checkpointStage4AdapterStableNetworkWork(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (resultErr error) {
	if execution == nil || !execution.deferred {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 stable network work checkpoint requires a deferred execution",
			),
		)
	}
	if !resume && len(completed) == 0 {
		return checkpointStage4AdapterFreshStableNetworkWork(
			ctx,
			observer,
			execution,
		)
	}
	if err := adoptStage4AdapterNetworkInventory(execution); err != nil {
		return err
	}
	if err := reviseStage4AdapterNetworkInventoryOnResume(
		ctx,
		execution,
		completed,
	); err != nil {
		return err
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		execution.prepared.names,
	); err != nil {
		return err
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	for planIndex, plan := range execution.prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if _, err := execution.validateCompletedTable(
				ctx,
				planIndex,
				rows,
				false,
			); err != nil {
				return err
			}
			if execution.prepared.deletes != nil {
				bound, found, err :=
					execution.classifyStage4AdapterPostgresDeleteTransferredTable(
						ctx,
						planIndex,
					)
				if err != nil {
					return err
				}
				if !found || !bound.taskCompleted || bound.rows != rows {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"completed Stage 4 PostgreSQL delete table %s lacks exact durable transfer evidence",
							plan.source.Name,
						),
					)
				}
				bound.ordinaryCompleted = true
				if stage4AdapterPostgresDeleteAuthorityActivated(
					execution.prepared.deletes,
				) {
					strict, err :=
						authenticateStage4AdapterPostgresDeleteTerminal(
							ctx,
							execution.prepared.deletes,
							planIndex,
							bound.work,
						)
					if err != nil {
						return err
					}
					bound.terminalAuthenticated = true
					bound.terminalStrict = strict
				}
				if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(
					planIndex,
					bound,
				); err != nil {
					return err
				}
			}
			continue
		}
		if resume && execution.prepared.deletes != nil {
			bound, found, err :=
				execution.classifyStage4AdapterPostgresDeleteTransferredTable(
					ctx,
					planIndex,
				)
			if err != nil {
				return err
			}
			if found {
				if bound.taskCompleted &&
					stage4AdapterPostgresDeleteAuthorityActivated(
						execution.prepared.deletes,
					) {
					strict, err := authenticateStage4AdapterPostgresDeleteTerminal(
						ctx,
						execution.prepared.deletes,
						planIndex,
						bound.work,
					)
					if err != nil {
						return err
					}
					bound.terminalAuthenticated = true
					bound.terminalStrict = strict
				}
				if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(
					planIndex,
					bound,
				); err != nil {
					return err
				}
				continue
			}
			if err := execution.rejectStage4AdapterPostgresDeleteAttemptBeforeReplay(
				ctx,
				planIndex,
			); err != nil {
				return err
			}
		}
		tableExecution, err := execution.openTable(
			ctx,
			planIndex,
			resume,
		)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after checkpointing %s: %w",
				plan.source.Name,
				closeErr,
			)
		}
	}
	return ctx.Err()
}

// checkpointStage4AdapterFreshStableNetworkWork establishes the exact Stage 4
// table inventory before any ordinary task or table work exists, which is the
// only order EnsureStage4TableInventory accepts. Planning is therefore kept
// apart from the durable write: every table is planned and its stable session
// closed, the inventory is published, the ordinary table set is checkpointed,
// and only then is each planned work plan committed. Execution still reopens
// each table and requires the newly observed plan to match the durable one.
func checkpointStage4AdapterFreshStableNetworkWork(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
) error {
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	planned := make(
		[]*stage4AdapterNetworkTableExecution,
		len(execution.prepared.plans),
	)
	for planIndex := range execution.prepared.plans {
		tableExecution, err := execution.planTableOnce(ctx, planIndex)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.session.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after planning %s: %w",
				execution.prepared.plans[planIndex].source.Name,
				closeErr,
			)
		}
		planned[planIndex] = tableExecution
	}
	if err := publishStage4AdapterNetworkInventory(
		ctx,
		execution,
		planned,
	); err != nil {
		return err
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		execution.prepared.names,
	); err != nil {
		return err
	}
	for planIndex, tableExecution := range planned {
		if err := tableExecution.resetOrEnsurePlan(ctx, false); err != nil {
			return err
		}
		if err := tableExecution.bindRestoresAndValidate(ctx); err != nil {
			return err
		}
		// Keep the bound plan separate from prepared.work. The latter is the
		// immutable seed for reopening a stable source and recomputing this exact
		// topology; replacing it with the derived topology would rehash it on the
		// next open and falsely look like an unsafe replan.
		execution.recordBoundWork(planIndex, tableExecution.work)
	}
	return ctx.Err()
}

// publishStage4AdapterNetworkInventory records the exact selected table set as
// immutable pre-mutation authority. A backend without aggregate completion
// leaves the run on the older separate-mutation path rather than failing, so
// the route stays usable wherever aggregate state is unavailable.
func publishStage4AdapterNetworkInventory(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	planned []*stage4AdapterNetworkTableExecution,
) error {
	work := make([]stage4AdapterWork, len(planned))
	for index, tableExecution := range planned {
		if tableExecution == nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network table inventory is missing planned work for %s",
					execution.prepared.plans[index].source.Name,
				),
			)
		}
		work[index] = cloneStage4AdapterNetworkWork(tableExecution.work)
	}
	return publishStage4AdapterNetworkWorkInventory(ctx, execution, work)
}

// publishStage4AdapterNetworkWorkInventory persists an already recomputed
// immutable work set. Recovery uses this form only when the absence of every
// ordinary/table checkpoint proves the original fresh checkpoint sequence
// stopped before PrepareTables could run.
func publishStage4AdapterNetworkWorkInventory(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	work []stage4AdapterWork,
) error {
	aggregate, ok := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil
	}
	if len(work) != len(execution.prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network table inventory is incomplete"),
		)
	}
	// The inventory binds itself to the validated source schema, so that
	// snapshot must already be durable. Staging is idempotent; the target
	// schema stage restages the identical evidence later.
	if err := stageStage4SchemaGateSnapshots(
		ctx,
		execution.prepared.run,
		execution.prepared.gate,
		execution.prepared.evolution,
	); err != nil {
		return err
	}
	inventory := state.Stage4TableInventory{
		RunID:                execution.prepared.run.RunID,
		SchemaTask:           execution.prepared.gate.Task,
		SchemaTopologyHash:   execution.prepared.gate.TopologyHash,
		SchemaSnapshotDigest: execution.prepared.gate.PendingSnapshot.Digest,
		Tables: make(
			[]state.Stage4TableInventoryEntry,
			len(work),
		),
	}
	for index, item := range work {
		ranges := make(
			[]state.Stage4InventoryRange,
			len(item.ranges),
		)
		for rangeIndex, workRange := range item.ranges {
			ranges[rangeIndex] = state.Stage4InventoryRange{
				ID: workRange.ID,
			}
		}
		inventory.Tables[index] = state.Stage4TableInventoryEntry{
			Table:        item.task.Table,
			Task:         item.task,
			Strategy:     item.strategy,
			TopologyHash: item.topology,
			Ranges:       ranges,
		}
	}
	if err := aggregate.EnsureStage4TableInventory(inventory); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"publish immutable Stage 4 network table inventory before table checkpoints: %w",
				err,
			),
		)
	}
	execution.aggregate = aggregate
	return nil
}

// reviseStage4AdapterNetworkInventoryOnResume republishes the table inventory
// when a resumed run legitimately replans. A source that grew during an outage
// yields a different partition count, and the durable inventory pins the exact
// range identities a table completion is validated against, so it must follow
// the replan. The revision window is enforced by the state layer and closes as
// soon as any table publishes terminal evidence, which is why a resume that
// already carries completed tables keeps the inventory it was given.
func reviseStage4AdapterNetworkInventoryOnResume(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
) error {
	if execution.aggregate == nil || len(completed) != 0 {
		return nil
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	planned := make(
		[]*stage4AdapterNetworkTableExecution,
		len(execution.prepared.plans),
	)
	for planIndex := range execution.prepared.plans {
		tableExecution, err := execution.planTableOnce(ctx, planIndex)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.session.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after replanning %s: %w",
				execution.prepared.plans[planIndex].source.Name,
				closeErr,
			)
		}
		planned[planIndex] = tableExecution
	}
	return publishStage4AdapterNetworkInventory(ctx, execution, planned)
}

// adoptStage4AdapterNetworkInventory binds a resumed run to the inventory its
// original attempt published. A run without one predates aggregate composition
// and must keep completing tables through the older separate mutations.
func adoptStage4AdapterNetworkInventory(
	execution *stage4AdapterNetworkExecution,
) error {
	aggregate, ok := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil
	}
	_, found, err := aggregate.LoadStage4TableInventory(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 network table inventory before resume: %w",
				err,
			),
		)
	}
	if found {
		execution.aggregate = aggregate
	}
	return nil
}

func runStage4AdapterStableNetworkTables(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	defer func() {
		attachStage4AdapterRuntimeTuningReport(&result, execution)
	}()
	if prepared.mode == "drop_recreate" {
		result, resultErr = runStage4AdapterStableNetworkRebuildTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
			resume,
			completed,
		)
		return result, resultErr
	}
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := execution.advanceCompletedTable(
				ctx,
				planIndex,
				rows,
			); err != nil {
				return result, err
			}
			result.Tables++
			result.Rows += rows
			continue
		}
		tableExecution, err := execution.openTable(
			ctx,
			planIndex,
			resume,
		)
		if err != nil {
			return result, err
		}
		copied, err := runStage4AdapterStableNetworkTable(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			planIndex,
			tableExecution,
			resume,
		)
		if err != nil {
			return result, err
		}
		result.Tables++
		result.Rows += copied
	}
	return result, nil
}

// resetStage4AdapterUnpublishedNetworkWork clears durable page completion
// facts when a target has not passed validation and finalization. A restart
// must replay from the first page, not mistake a partially validated target
// for completed work. The admitted network writer owns replay safety, so this
// reset is conservative for both upsert and rebuild paths.
func resetStage4AdapterUnpublishedNetworkWork(
	execution *stage4AdapterNetworkExecution,
) error {
	if execution == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("reset unpublished Stage 4 network work: execution is unavailable"),
		)
	}
	work, err := execution.snapshotBoundWork()
	if err != nil {
		return err
	}
	for _, work := range work {
		task := state.WorkTask{
			RunID:        execution.prepared.run.RunID,
			Key:          work.task,
			Strategy:     work.strategy,
			TopologyHash: work.topology,
			StartedAt:    time.Now().UTC(),
		}
		ranges := make([]state.RangeState, len(work.ranges))
		for index := range work.ranges {
			ranges[index] = cloneInitialNetworkStateRange(work.ranges[index])
		}
		if err := execution.prepared.run.Backend.ResetWorkPlan(task, ranges); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"reset unpublished Stage 4 network work for %s: %w",
					work.task.Table,
					err,
				),
			)
		}
	}
	return nil
}

// runStage4AdapterStableNetworkRebuildTableData transfers one table after the
// full target set has been recreated. Set-wide finalization occurs before
// validation, matching the public lifecycle; completion remains deferred until
// both phases succeed.
func runStage4AdapterStableNetworkRebuildTableData(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkTableExecution,
) (_ int, resultErr error) {
	if execution == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable rebuild table execution is unavailable"),
		)
	}
	defer func() {
		if closeErr := execution.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}
	}()
	copied, err := execution.run(ctx, observer)
	if err != nil {
		return 0, err
	}
	return copied, nil
}

func runStage4AdapterStableNetworkTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
	execution *stage4AdapterNetworkTableExecution,
	resume bool,
) (_ int, resultErr error) {
	if execution == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table execution is unavailable"),
		)
	}
	defer func() {
		if closeErr := execution.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}
	}()
	plan := prepared.plans[planIndex]
	name := plan.source.Name
	if resume {
		if err := checkAdapterResumeContext(
			ctx,
			"checkpoint before "+name,
		); err != nil {
			return 0, err
		}
	}
	if err := observer.BeforeTable(ctx, name); err != nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint before %s: %w", name, err),
		)
	}
	mutationObserver := observer
	if resume {
		mutationObserver = adapterResumeMutationGuard{
			ctx:      ctx,
			delegate: observer,
			boundary: "mutate resumed Stage 4 table " + name,
		}
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"prepare Stage 4 table "+name,
		func() error {
			return target.PrepareTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(plan.target),
				},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, err
	}
	copied, err := execution.run(ctx, mutationObserver)
	if err != nil {
		return 0, err
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"finalize Stage 4 table "+name,
		func() error {
			return target.FinalizeTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(plan.target),
				},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, err
	}
	if err := validateStage4AdapterStableTable(
		ctx,
		cfg,
		observer,
		execution.parent.source,
		execution.source,
		target,
		prepared,
		planIndex,
	); err != nil {
		return 0, err
	}
	if err := completeStage4AdapterNetworkTable(
		ctx,
		observer,
		prepared.run,
		execution.parent.aggregate,
		execution.work,
		name,
		copied,
	); err != nil {
		return 0, fmt.Errorf(
			"complete Stage 4 work for %s: %w",
			name,
			err,
		)
	}
	return copied, nil
}

func validateStage4AdapterStableTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	providerSource sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
) error {
	plan := cloneStage4AdapterNetworkTablePlan(
		prepared.plans[planIndex],
	)
	probe, err := stage4AdapterValidationProbe(
		cfg,
		observer,
		source,
		target,
		[]adapterTablePlan{plan},
		providerSource,
	)
	if err != nil {
		return err
	}
	tablePrepared := prepared
	tablePrepared.plans = []adapterTablePlan{plan}
	tablePrepared.validation = probe
	tablePrepared.gate.ValidationTables = []schema.Table{
		cloneStage4RichTable(plan.source),
	}
	return validateStage4AdapterRun(
		ctx,
		cfg,
		source,
		target,
		tablePrepared,
	)
}
