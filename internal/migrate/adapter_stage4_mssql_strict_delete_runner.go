package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4SQLServerStrictDeleteReader is intentionally a callback-only bridge.
// It exposes one retained strict source view for the specified table and never
// exposes the ordinary relational source pool to delete reconciliation.
type stage4SQLServerStrictDeleteReader func(
	context.Context,
	int,
	func(context.Context, adapterStableNetworkSource) error,
) error

// migrateWithStage4SQLServerStrictDeleteAdapters is the SQL Server-only
// strict/delete composition. It deliberately keeps the existing non-strict
// delete lifecycle and every PostgreSQL/MySQL/SQLite strict route untouched.
// The route is bounded to MSSQL-to-MSSQL by the ordinary delete capability
// admission; this runner merely changes the source side from a live scanner
// to the retained strict reader.
func migrateWithStage4SQLServerStrictDeleteAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (Result, error) {
	if prepared.deletes == nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete composition is unavailable"),
		)
	}
	if networkExecution == nil || !networkExecution.deferred {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete composition requires deferred network work"),
		)
	}
	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational == nil || relational.database == nil ||
		relational.spec.engine != "mssql" || target == nil ||
		target.Engine() != "mssql" {
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server strict delete reconciliation is certified only for the production MSSQL-to-MSSQL route"),
		)
	}
	scope, err := stage4SQLServerStrictScope(
		cfg.Migration.StrictConsistencyScope,
	)
	if err != nil {
		return Result{}, err
	}
	if scope != state.StrictSnapshotTable &&
		scope != state.StrictSnapshotMigration {
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server strict delete reconciliation has an unsupported strict scope"),
		)
	}
	// A table-scoped strict view is process-local, so a partial ordinary-table
	// checkpoint set cannot safely be combined with a newly opened lock epoch.
	// Migration scope is different: its durable SQL Server database snapshot is
	// explicitly reopened on resume and has a dedicated terminal-repair path
	// below.  Do not conflate the two recovery contracts.
	if scope == state.StrictSnapshotTable && len(completed) != 0 && len(completed) != len(prepared.plans) {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server table-strict delete reconciliation refuses a partial completed-table set because a new process cannot reuse the prior held lock view"),
		)
	}
	completedBinding, err := validateCompletedStage4SQLServerStrictEvidence(
		ctx,
		prepared,
		scope,
		completed,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if len(completedBinding.tables) != 0 {
		networkExecution.finalizeWork = completedBinding.finalizeWork
	}
	if err := networkExecution.prevalidateCompletedTables(ctx, completed); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	selected := make([]state.TaskKey, 0, len(prepared.plans))
	for index, plan := range prepared.plans {
		if _, complete := completed[plan.source.Name]; !complete {
			selected = append(selected, prepared.work[index].task)
		}
	}
	if len(selected) == 0 {
		return completeStage4SQLServerStrictDeleteNoWork(
			ctx,
			observer,
			relational,
			target,
			prepared,
			networkExecution,
			scope,
			resume,
			completed,
		)
	}
	processEpoch, err := newStage4SQLServerProcessEpoch()
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	switch scope {
	case state.StrictSnapshotTable:
		// The ordinary SQL Server source opener deliberately caps its pool at
		// one connection. A table-strict epoch has one lock owner and one
		// transaction-bound reader, so retain exactly those two slots here.
		// This route serializes its strict readers; widening further would add
		// resource use without strengthening the source proof.
		relational.database.SetMaxOpenConns(2)
		relational.database.SetMaxIdleConns(2)
		opener, err := NewSQLServerStrictConsistencyOpener(
			relational.database,
			relational.namespace,
		)
		if err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
		return migrateWithStage4SQLServerTableStrictDeleteAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			networkExecution,
			opener,
			processEpoch,
			completedBinding,
			resume,
			completed,
			selected,
		)
	case state.StrictSnapshotMigration:
		cleanupBackend, supported := prepared.run.Backend.(state.StrictMigrationCleanupBackend)
		if !supported || isNilInterface(cleanupBackend) {
			return resultForValidatedAdapterCheckpoints(completed), NewTransferError(
				ErrorClassPolicy,
				errors.New("SQL Server migration-scope strict delete reconciliation requires durable cleanup-intent state support"),
			)
		}
		opener, err := NewSQLServerMigrationSnapshotOpener(
			relational.database,
			relational.endpoint,
			relational.namespace,
		)
		if err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
		return migrateWithStage4SQLServerMigrationStrictDeleteAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			networkExecution,
			opener,
			processEpoch,
			completedBinding,
			resume,
			completed,
			selected,
			cleanupBackend,
		)
	default:
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server strict delete reconciliation has an unsupported strict scope"),
		)
	}
}

func migrateWithStage4SQLServerTableStrictDeleteAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	opener *SQLServerStrictConsistencyOpener,
	processEpoch string,
	completedBinding stage4SQLServerStrictEpochBinding,
	resume bool,
	completed map[string]int,
	selected []state.TaskKey,
) (result Result, resultErr error) {
	strictExecution, err := BeginPlannedStrictConsistency(
		ctx,
		PlannedStrictConsistencyRequest{
			RunID:        prepared.run.RunID,
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotTable,
			Resume:       resume,
			ProcessEpoch: processEpoch,
			State:        prepared.run.Backend,
			Tasks:        append([]state.TaskKey(nil), selected...),
		},
		opener,
		func(
			planCtx context.Context,
			rawSession StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			session, ok := rawSession.(*SQLServerStrictConsistencySession)
			if !ok || session == nil {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New("SQL Server strict delete planner received an unexpected table session"),
				)
			}
			currentBinding, err := newStage4SQLServerStrictEpochBinding(
				processEpoch,
				capture,
			)
			if err != nil {
				return nil, err
			}
			binding, err := completedBinding.merge(currentBinding)
			if err != nil {
				return nil, err
			}
			networkExecution.finalizeWork = binding.finalizeWork
			return planStage4SQLServerStrictDeleteNetworkWork(
				planCtx,
				observer,
				prepared,
				networkExecution,
				resume,
				completed,
				func(
					readerCtx context.Context,
					planIndex int,
					work func(context.Context, adapterStableNetworkSource) error,
				) error {
					return RunSQLServerAdapterStableNetworkReader(
						readerCtx,
						session,
						prepared.work[planIndex].task,
						source,
						prepared.plans[planIndex].source,
						work,
					)
				},
			)
		},
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	strictPrepared := prepared
	strictPrepared.strictSourceRows, err = stage4SQLServerStrictSourceRows(
		strictExecution.Evidence(),
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	currentBinding, err := newStage4SQLServerStrictEpochBinding(
		processEpoch,
		strictConsistencyCaptureFromEvidence(strictExecution.Evidence()),
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	nextBinding, err := completedBinding.merge(currentBinding)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	resultErr = strictExecution.Run(
		ctx,
		func(runCtx context.Context, rawSession StrictConsistencySession) error {
			session, ok := rawSession.(*SQLServerStrictConsistencySession)
			if !ok || session == nil {
				return NewTransferError(
					ErrorClassState,
					errors.New("SQL Server strict delete execution received an unexpected table session"),
				)
			}
			return runStage4SQLServerStrictDeleteEpoch(
				runCtx,
				cfg,
				observer,
				source,
				target,
				strictPrepared,
				networkExecution,
				resume,
				completed,
				func(
					readerCtx context.Context,
					planIndex int,
					work func(context.Context, adapterStableNetworkSource) error,
				) error {
					return RunSQLServerAdapterStableNetworkReader(
						readerCtx,
						session,
						prepared.work[planIndex].task,
						source,
						prepared.plans[planIndex].source,
						work,
					)
				},
				&result,
			)
		},
	)
	if resultErr != nil {
		return result, resultErr
	}
	if err := completeStage4SchemaGateSentinels(
		ctx,
		strictPrepared.run,
		strictPrepared.gate,
		strictPrepared.evolution,
	); err != nil {
		return result, err
	}
	result.Validated = true
	networkExecution.finalizeWork = nextBinding.finalizeWork
	return result, resultErr
}

func migrateWithStage4SQLServerMigrationStrictDeleteAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	opener *SQLServerMigrationSnapshotOpener,
	processEpoch string,
	completedBinding stage4SQLServerStrictEpochBinding,
	resume bool,
	completed map[string]int,
	selected []state.TaskKey,
	cleanupBackend state.StrictMigrationCleanupBackend,
) (result Result, resultErr error) {
	partialCompletion := len(completed) != 0 && len(completed) != len(prepared.plans)
	if partialCompletion && !resume {
		return resultForValidatedAdapterCheckpoints(completed), NewTransferError(
			ErrorClassState,
			errors.New("partial SQL Server migration-strict delete completion requires explicit resume of the durable database snapshot"),
		)
	}
	strictExecution, err := BeginPlannedStrictConsistency(
		ctx,
		PlannedStrictConsistencyRequest{
			RunID:        prepared.run.RunID,
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			Resume:       resume,
			ProcessEpoch: processEpoch,
			State:        prepared.run.Backend,
			Tasks:        append([]state.TaskKey(nil), selected...),
		},
		opener,
		func(
			planCtx context.Context,
			rawSession StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			session, ok := rawSession.(*SQLServerMigrationSnapshotSession)
			if !ok || session == nil {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New("SQL Server strict delete planner received an unexpected migration snapshot session"),
				)
			}
			currentBinding, err := newStage4SQLServerMigrationEpochBinding(capture)
			if err != nil {
				return nil, err
			}
			binding, err := completedBinding.merge(currentBinding)
			if err != nil {
				return nil, err
			}
			networkExecution.finalizeWork = binding.finalizeWork
			if partialCompletion {
				return planStage4SQLServerMigrationStrictDeletePartialCompletion(
					planCtx,
					observer,
					prepared,
					networkExecution,
					completed,
					selected,
				)
			}
			return planStage4SQLServerStrictDeleteNetworkWork(
				planCtx,
				observer,
				prepared,
				networkExecution,
				resume,
				completed,
				func(
					readerCtx context.Context,
					planIndex int,
					work func(context.Context, adapterStableNetworkSource) error,
				) error {
					return RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
						readerCtx,
						session,
						prepared.work[planIndex].task,
						source,
						prepared.plans[planIndex].source,
						work,
					)
				},
			)
		},
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	owner, found := strictExecution.MigrationSnapshot()
	if !found || owner.SourceEngine != "mssql" || owner.SnapshotReference == "" || owner.EpochID == "" {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, NewTransferError(
				ErrorClassState,
				errors.New("SQL Server strict delete execution lacks durable migration snapshot ownership"),
			))
	}
	strictPrepared := prepared
	strictPrepared.strictSourceRows, err = stage4SQLServerMigrationStrictSourceRows(
		owner,
		strictExecution.Evidence(),
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	currentBinding, err := stage4SQLServerMigrationBindingFromEvidence(
		owner,
		strictExecution.Evidence(),
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	nextBinding, err := completedBinding.merge(currentBinding)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed),
			closeStage4SQLServerStrictDeleteExecution(ctx, strictExecution, err)
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	resultErr = strictExecution.Run(
		ctx,
		func(runCtx context.Context, rawSession StrictConsistencySession) error {
			session, ok := rawSession.(*SQLServerMigrationSnapshotSession)
			if !ok || session == nil {
				return NewTransferError(
					ErrorClassState,
					errors.New("SQL Server strict delete execution received an unexpected migration snapshot session"),
				)
			}
			reader := func(
				readerCtx context.Context,
				planIndex int,
				work func(context.Context, adapterStableNetworkSource) error,
			) error {
				return RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
					readerCtx,
					session,
					strictPrepared.work[planIndex].task,
					source,
					strictPrepared.plans[planIndex].source,
					work,
				)
			}
			if partialCompletion {
				if err := resumeStage4SQLServerMigrationStrictDeletePartialCompletion(
					runCtx,
					observer,
					source,
					target,
					strictPrepared,
					networkExecution,
					session,
					completed,
					reader,
					&result,
				); err != nil {
					return err
				}
			} else if err := runStage4SQLServerMigrationStrictDeleteEpoch(
				runCtx,
				cfg,
				observer,
				source,
				target,
				strictPrepared,
				networkExecution,
				resume,
				completed,
				reader,
				session,
				&result,
			); err != nil {
				return err
			}
			// The completion/finalization boundary is handled after the strict
			// transfer epoch returns so ordinary transfer failures release the
			// owned snapshot as terminal.
			if err := completeStage4SchemaGateSentinels(
				runCtx,
				strictPrepared.run,
				strictPrepared.gate,
				strictPrepared.evolution,
			); err != nil {
				return err
			}
			if err := cleanupBackend.SaveStrictMigrationCleanupIntent(
				state.StrictMigrationCleanupIntent{
					RunID:             owner.RunID,
					EpochID:           owner.EpochID,
					SourceEngine:      owner.SourceEngine,
					SnapshotReference: owner.SnapshotReference,
					ProcessEpoch:      owner.ProcessEpoch,
					CapturedAt:        owner.CapturedAt,
					IntentAt:          time.Now().UTC(),
				},
			); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("persist SQL Server migration snapshot cleanup intent after strict delete validation: %w", err),
				)
			}
			if err := session.AuthorizeSnapshotCleanup(); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("authorize SQL Server migration snapshot cleanup after strict delete validation: %w", err),
				)
			}
			result.Validated = true
			return nil
		},
	)
	if resultErr == nil {
		networkExecution.finalizeWork = nextBinding.finalizeWork
	}
	return result, resultErr
}

// planStage4SQLServerStrictDeleteNetworkWork mirrors the ordinary deferred
// planning order, but every source catalog/pagination/width read is supplied
// by a retained strict reader. It publishes the full immutable inventory
// before ordinary work or table checkpoints so the existing journal-readiness
// protocol can safely execute after this planner returns.
func planStage4SQLServerStrictDeleteNetworkWork(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
	reader stage4SQLServerStrictDeleteReader,
) (_ []StrictConsistencyTable, resultErr error) {
	if execution == nil || !execution.deferred || reader == nil {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete planner is unavailable"),
		)
	}
	if len(completed) != 0 {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete planner requires no partial completed-table work"),
		)
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	planned := make([]*stage4AdapterNetworkTableExecution, len(prepared.plans))
	for planIndex := range prepared.plans {
		if err := reader(ctx, planIndex, func(
			readerCtx context.Context,
			stable adapterStableNetworkSource,
		) error {
			borrowed := &adapterStableNetworkTableSession{
				source: stable,
				// A strict delete epoch intentionally uses one reader at a time.
				// The complete source-key scan and validation then have the same
				// direct transaction-bound proof as transfer pagination.
				readerLimit: 1,
			}
			tableExecution, err := planStage4SQLServerStrictDeleteTable(
				readerCtx,
				execution,
				planIndex,
				borrowed,
			)
			if err != nil {
				return err
			}
			if closeErr := tableExecution.Close(); closeErr != nil {
				return fmt.Errorf("close retained SQL Server strict source after planning %s: %w", prepared.plans[planIndex].source.Name, closeErr)
			}
			planned[planIndex] = tableExecution
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if err := publishStage4AdapterNetworkInventory(ctx, execution, planned); err != nil {
		return nil, err
	}
	if err := checkpointStage4AdapterTableSet(ctx, observer, prepared.names); err != nil {
		return nil, err
	}
	result := make([]StrictConsistencyTable, 0, len(planned))
	for planIndex, tableExecution := range planned {
		if tableExecution == nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server strict delete planner has no work for %s", prepared.plans[planIndex].source.Name),
			)
		}
		if err := tableExecution.resetOrEnsurePlan(ctx, resume); err != nil {
			return nil, err
		}
		if err := tableExecution.bindRestoresAndValidate(ctx); err != nil {
			return nil, err
		}
		execution.recordBoundWork(planIndex, tableExecution.work)
		attemptID, err := BuildStrictConsistencyAttemptID(
			tableExecution.work.task,
			tableExecution.work.topology,
			0,
		)
		if err != nil {
			return nil, fmt.Errorf("build SQL Server strict delete work attempt for %s: %w", prepared.plans[planIndex].source.Name, err)
		}
		result = append(result, StrictConsistencyTable{
			Task:                tableExecution.work.task,
			AttemptID:           attemptID,
			WorkTopologyHash:    tableExecution.work.topology,
			DurableWorkAttempts: 0,
		})
	}
	return result, nil
}

// planStage4SQLServerMigrationStrictDeletePartialCompletion reopens a durable
// migration snapshot solely to authenticate the unfinished ordinary table
// publications. All selected tables must already have complete range and
// delete-terminal evidence; choosing fresh pagination or a live source here
// would silently replace the durable source instant after a crash.
func planStage4SQLServerMigrationStrictDeletePartialCompletion(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
	selected []state.TaskKey,
) ([]StrictConsistencyTable, error) {
	if execution == nil || !execution.deferred || len(completed) == 0 ||
		len(completed) >= len(prepared.plans) {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete partial-completion planner has an invalid table checkpoint set"),
		)
	}
	if err := checkpointStage4AdapterTableSet(ctx, observer, prepared.names); err != nil {
		return nil, err
	}
	durable, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return nil, err
	}
	if err := execution.validateDurableTableTaskInventory(durable); err != nil {
		return nil, err
	}
	selectedByTask := make(map[state.TaskKey]struct{}, len(selected))
	for _, task := range selected {
		if err := task.Validate(); err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("validate SQL Server migration strict delete partial task: %w", err),
			)
		}
		if _, duplicate := selectedByTask[task]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete partial planner duplicates task %s.%s", task.Schema, task.Table),
			)
		}
		selectedByTask[task] = struct{}{}
	}
	result := make([]StrictConsistencyTable, 0, len(selected))
	for planIndex, plan := range prepared.plans {
		task := prepared.work[planIndex].task
		rows, checkpointed := completed[plan.source.Name]
		if checkpointed {
			if _, selected := selectedByTask[task]; selected {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf("SQL Server migration strict delete partial planner selected completed table %s", plan.source.Name),
				)
			}
			if err := execution.advanceCompletedTable(ctx, planIndex, rows); err != nil {
				return nil, err
			}
			continue
		}
		if _, selected := selectedByTask[task]; !selected {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete partial planner omitted unfinished table %s", plan.source.Name),
			)
		}
		bound, found, err := execution.classifyStage4AdapterPostgresDeleteTransferredTable(ctx, planIndex)
		if err != nil {
			return nil, err
		}
		if !found || bound.taskCompleted {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete partial table %s lacks exact running terminal work", plan.source.Name),
			)
		}
		durableTask, found := durable.tasks[bound.work.task]
		if !found {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete partial table %s lacks durable work identity", plan.source.Name),
			)
		}
		strictTable, err := newStage4SQLServerMigrationStrictDeletePartialEvidenceTable(
			durableTask,
			bound.work,
		)
		if err != nil {
			return nil, fmt.Errorf("build SQL Server migration strict delete partial work attempt for %s: %w", plan.source.Name, err)
		}
		result = append(result, strictTable)
	}
	if len(result) != len(selectedByTask) {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete partial planner task set is incomplete"),
		)
	}
	return result, nil
}

// newStage4SQLServerMigrationStrictDeletePartialEvidenceTable binds a resumed
// strict proof to the current durable work identity. The original evidence was
// recorded before delivery began, so a partial publication repair must not
// reuse its zero attempt counter after completed range deliveries advanced the
// persisted task. It re-attests through the same migration snapshot instead.
func newStage4SQLServerMigrationStrictDeletePartialEvidenceTable(
	durable state.WorkTask,
	work stage4AdapterWork,
) (StrictConsistencyTable, error) {
	if durable.Key != work.task || durable.TopologyHash != work.topology ||
		durable.Status != "running" || durable.Attempts < 0 {
		return StrictConsistencyTable{}, errors.New("SQL Server migration strict delete partial durable work identity is invalid")
	}
	attemptID, err := BuildStrictConsistencyAttemptID(
		work.task,
		work.topology,
		durable.Attempts,
	)
	if err != nil {
		return StrictConsistencyTable{}, err
	}
	return StrictConsistencyTable{
		Task:                work.task,
		AttemptID:           attemptID,
		WorkTopologyHash:    work.topology,
		DurableWorkAttempts: durable.Attempts,
	}, nil
}

func planStage4SQLServerStrictDeleteTable(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	planIndex int,
	borrowed *adapterStableNetworkTableSession,
) (_ *stage4AdapterNetworkTableExecution, resultErr error) {
	if execution == nil || borrowed == nil {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete table planner is unavailable"),
		)
	}
	execution.mu.Lock()
	if execution.binding || execution.bound || execution.started {
		execution.mu.Unlock()
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete table planner is already active or consumed"),
		)
	}
	execution.binding = true
	globalOffset := execution.nextGlobalRange
	execution.mu.Unlock()
	defer func() {
		execution.mu.Lock()
		execution.binding = false
		execution.mu.Unlock()
	}()
	tableExecution, err := execution.planTable(
		ctx,
		planIndex,
		globalOffset,
		borrowed,
	)
	if err != nil {
		return nil, err
	}
	execution.mu.Lock()
	execution.nextGlobalRange += uint64(len(tableExecution.ranges))
	execution.mu.Unlock()
	return tableExecution, nil
}

// runStage4SQLServerStrictDeleteEpoch performs transfer, reconciliation, and
// validation in the one order the delete contract permits: all range writes
// finish first, then hard deletes use the retained strict source view, then
// post-delete validation and ordinary completion publish.
func runStage4SQLServerStrictDeleteEpoch(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
	reader stage4SQLServerStrictDeleteReader,
	result *Result,
) error {
	return runStage4SQLServerStrictDeleteEpochWithCompletion(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		execution,
		resume,
		completed,
		reader,
		nil,
		result,
	)
}

// runStage4SQLServerMigrationStrictDeleteEpoch retains the one durable SQL
// Server database snapshot only after transfer, delete reconciliation, and
// validation have all completed, but before the first ordinary table
// completion is published. That narrowly covers the partial-publication crash
// window without making an ordinary transfer error resumable.
func runStage4SQLServerMigrationStrictDeleteEpoch(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
	reader stage4SQLServerStrictDeleteReader,
	session *SQLServerMigrationSnapshotSession,
	result *Result,
) error {
	if session == nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete snapshot session is unavailable"),
		)
	}
	return runStage4SQLServerStrictDeleteEpochWithCompletion(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		execution,
		resume,
		completed,
		reader,
		func() error {
			if err := session.PreserveSnapshotForResume(); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("preserve SQL Server migration snapshot before strict delete table completion: %w", err),
				)
			}
			return nil
		},
		result,
	)
}

func runStage4SQLServerStrictDeleteEpochWithCompletion(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
	reader stage4SQLServerStrictDeleteReader,
	beforeCompletion func() error,
	result *Result,
) error {
	if prepared.deletes == nil || execution == nil || reader == nil || result == nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete epoch is unavailable"),
		)
	}
	if len(completed) != 0 {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete epoch refuses partial completed-table work"),
		)
	}
	if err := applyStage4AdapterTargetSchema(
		ctx,
		observer,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return err
	}
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(
		ctx,
		target,
		prepared,
	); err != nil {
		return err
	}
	if err := activateStage4AdapterPostgresDeleteComposition(ctx, prepared); err != nil {
		return err
	}
	// Journal DDL is deliberately later than immutable inventory binding and
	// target-shape activation, but still before the first PrepareTables or row
	// write. The target-owned readiness protocol preserves that exact boundary.
	if err := ensureStage4AdapterDeleteJournalReadiness(ctx, observer, prepared); err != nil {
		return err
	}
	finalWork := make([]stage4AdapterWork, len(prepared.plans))
	rows := make([]int, len(prepared.plans))
	for planIndex := range prepared.plans {
		copied, work, err := runStage4SQLServerStrictDeleteTransferTable(
			ctx,
			observer,
			target,
			prepared,
			execution,
			planIndex,
			resume,
			reader,
		)
		if err != nil {
			return err
		}
		if copied < 0 || result.Rows > math.MaxInt-copied {
			return NewTransferError(
				ErrorClassState,
				errors.New("SQL Server strict delete result row total overflows"),
			)
		}
		finalWork[planIndex] = work
		rows[planIndex] = copied
		result.Tables++
		result.Rows += copied
	}
	deleteResult, err := reconcileStage4SQLServerStrictDeletes(
		ctx,
		prepared.deletes,
		finalWork,
		reader,
	)
	if err != nil {
		if ClassifyTransferError(err) == ErrorClassPermanent {
			err = NewTransferError(ErrorClassState, err)
		}
		return err
	}
	prepared.deleteReconciliationStrict = deleteResult.strictByTable
	if err := validateStage4AdapterPostgresDeleteCheckpointRows(
		ctx,
		target,
		prepared,
		rows,
	); err != nil {
		return err
	}
	for planIndex := range prepared.plans {
		if err := reader(ctx, planIndex, func(
			readerCtx context.Context,
			stable adapterStableNetworkSource,
		) error {
			return validateStage4AdapterStableTable(
				readerCtx,
				cfg,
				observer,
				source,
				stable,
				target,
				prepared,
				planIndex,
			)
		}); err != nil {
			return err
		}
	}
	if beforeCompletion != nil {
		if err := beforeCompletion(); err != nil {
			return err
		}
	}
	for planIndex, plan := range prepared.plans {
		if err := completeStage4AdapterNetworkTable(
			ctx,
			observer,
			prepared.run,
			execution.aggregate,
			finalWork[planIndex],
			plan.source.Name,
			rows[planIndex],
		); err != nil {
			return fmt.Errorf("complete SQL Server strict delete work for %s: %w", plan.source.Name, err)
		}
	}
	return nil
}

// resumeStage4SQLServerMigrationStrictDeletePartialCompletion repairs only
// the narrow crash window after every transfer/delete/validation result is
// durable but before every ordinary table-completion receipt was published.
// The migration snapshot is reopened by BeginPlannedStrictConsistency before
// this function runs; unfinished-table count/catalog authority is therefore
// observed through that same snapshot, never an ordinary source reader.
func resumeStage4SQLServerMigrationStrictDeletePartialCompletion(
	ctx context.Context,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *SQLServerMigrationSnapshotSession,
	completed map[string]int,
	reader stage4SQLServerStrictDeleteReader,
	result *Result,
) error {
	if prepared.deletes == nil || execution == nil || session == nil || reader == nil || result == nil ||
		len(completed) == 0 || len(completed) >= len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete partial completion is unavailable"),
		)
	}
	// This path must not apply schema evolution or open a replacement source
	// view. The target shape and journal are only reauthenticated against the
	// durable terminal evidence before any missing ordinary completion is
	// published.
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(ctx, target, prepared); err != nil {
		return err
	}
	if err := activateStage4AdapterPostgresDeleteComposition(ctx, prepared); err != nil {
		return err
	}
	if err := ensureStage4AdapterDeleteJournalReadiness(ctx, observer, prepared); err != nil {
		return err
	}

	terminal := make([]stage4AdapterPostgresDeleteTransferredTable, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		bound, found, err := execution.classifyStage4AdapterPostgresDeleteTransferredTable(ctx, planIndex)
		if err != nil {
			return err
		}
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete partial table %s lacks complete durable range work", plan.source.Name),
			)
		}
		checkpointRows, checkpointed := completed[plan.source.Name]
		if checkpointed {
			if !bound.taskCompleted || bound.rows != checkpointRows {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("SQL Server migration strict delete completed table %s differs from its durable range work", plan.source.Name),
				)
			}
		} else if bound.taskCompleted {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete table %s is completed in durable work but missing its ordinary checkpoint", plan.source.Name),
			)
		}

		var strict bool
		// A migration-scope resume must authenticate every terminal delete
		// plan against the reopened durable database snapshot.  In particular,
		// a completed ordinary-table receipt cannot authorize a fresh live
		// source catalog read after the original strict evidence was captured.
		// The unfinished table additionally re-attests its strict evidence at
		// its current durable range-attempt count before reaching this point.
		terminalReader := reader
		if checkpointed {
			terminalReader = func(
				readerCtx context.Context,
				requestedPlanIndex int,
				work func(context.Context, adapterStableNetworkSource) error,
			) error {
				if requestedPlanIndex != planIndex {
					return NewTransferError(
						ErrorClassState,
						errors.New("SQL Server migration strict delete terminal reader received a mismatched table index"),
					)
				}
				return runStage4SQLServerMigrationStrictDeleteTerminalReader(
					readerCtx,
					session,
					source,
					prepared,
					planIndex,
					work,
				)
			}
		}
		strict, err = authenticateStage4SQLServerStrictDeleteTerminal(
			ctx,
			prepared.deletes,
			planIndex,
			bound.work,
			terminalReader,
		)
		if err != nil {
			return err
		}
		if !strict {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete terminal for table %s is not exact", plan.source.Name),
			)
		}
		targetRows, err := target.CountRows(ctx, plan.target)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("count SQL Server migration strict delete target table %s: %w", plan.source.Name, err),
			)
		}
		if targetRows != bound.rows {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete terminal for table %s has %d target rows, want %d", plan.source.Name, targetRows, bound.rows),
			)
		}
		bound.ordinaryCompleted = checkpointed
		bound.terminalAuthenticated = true
		bound.terminalStrict = true
		if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(planIndex, bound); err != nil {
			return err
		}
		terminal[planIndex] = bound
	}

	// Every mutable target operation is already terminal. Preserve before the
	// first missing ordinary-completion publication so another callback/state
	// fault continues to reuse this exact snapshot rather than dropping it.
	if err := session.PreserveSnapshotForResume(); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("preserve SQL Server migration snapshot before partial strict delete completion: %w", err),
		)
	}
	for planIndex, plan := range prepared.plans {
		if _, checkpointed := completed[plan.source.Name]; checkpointed {
			continue
		}
		bound := terminal[planIndex]
		if bound.rows < 0 || result.Rows > math.MaxInt-bound.rows {
			return NewTransferError(
				ErrorClassState,
				errors.New("SQL Server migration strict delete partial result row total overflows"),
			)
		}
		if err := completeStage4AdapterNetworkTable(
			ctx,
			observer,
			prepared.run,
			execution.aggregate,
			bound.work,
			plan.source.Name,
			bound.rows,
		); err != nil {
			return fmt.Errorf("complete SQL Server migration strict delete partial work for %s: %w", plan.source.Name, err)
		}
		result.Tables++
		result.Rows += bound.rows
	}
	return nil
}

// runStage4SQLServerMigrationStrictDeleteTerminalReader temporarily admits an
// already checkpointed terminal table to the reopened migration snapshot. The
// planned strict task set intentionally contains only unfinished work, because
// strict evidence can only be re-attested for a running durable task. A
// checkpointed table nevertheless needs its terminal delete plan checked
// against the same durable snapshot, never against the ordinary live source.
// The caller has already validated its immutable strict evidence and durable
// terminal range work before this narrow reader is used.
func runStage4SQLServerMigrationStrictDeleteTerminalReader(
	ctx context.Context,
	session *SQLServerMigrationSnapshotSession,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil || planIndex < 0 || planIndex >= len(prepared.plans) ||
		planIndex >= len(prepared.work) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete terminal reader is unavailable"),
		)
	}
	task := prepared.work[planIndex].task
	table := prepared.plans[planIndex].source
	if task.Schema != table.Schema || task.Table != table.Name {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete terminal task differs from its table catalog"),
		)
	}

	// RunReader owns reader cancellation and Close coordination. Its selected
	// task set normally contains only tasks that are eligible for new strict
	// evidence. Grant this verified terminal table a callback-scoped admission
	// solely for catalog/key authority authentication against the same snapshot.
	session.mu.Lock()
	if session.closed || session.selected == nil {
		session.mu.Unlock()
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete terminal snapshot is closed"),
		)
	}
	_, alreadySelected := session.selected[task]
	if !alreadySelected {
		session.selected[task] = struct{}{}
	}
	session.mu.Unlock()
	if !alreadySelected {
		defer func() {
			session.mu.Lock()
			delete(session.selected, task)
			session.mu.Unlock()
		}()
	}
	return RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
		ctx,
		session,
		task,
		source,
		table,
		work,
	)
}

// authenticateStage4SQLServerStrictDeleteTerminal authenticates a terminal
// delete attempt using a strict retained reader. Migration recovery uses it for
// every terminal table: both a completed ordinary-table receipt and an
// unfinished publication must remain bound to the reopened durable snapshot,
// rather than inspecting the ordinary live source after a crash.
func authenticateStage4SQLServerStrictDeleteTerminal(
	ctx context.Context,
	composition *stage4AdapterPostgresDeletePrepared,
	planIndex int,
	work stage4AdapterWork,
	reader stage4SQLServerStrictDeleteReader,
) (strict bool, resultErr error) {
	if composition == nil || reader == nil || planIndex < 0 || planIndex >= len(composition.entries) {
		return false, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete terminal reader authority is unavailable"),
		)
	}
	entry := composition.entries[planIndex]
	if err := reader(ctx, planIndex, func(
		readerCtx context.Context,
		stable adapterStableNetworkSource,
	) error {
		current, err := newSQLServerStrictDeleteReconciliationCapabilities(
			readerCtx,
			stable,
			entry.capabilities,
			entry.source,
			entry.target,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("derive retained SQL Server strict delete terminal authority for table %s: %w", entry.source.Name, err),
			)
		}
		reconciler, request, err := composition.requestFor(entry, work)
		if err != nil {
			return NewTransferError(ErrorClassState, err)
		}
		reconciler.source = current.source
		reconciler.target = current.target
		reconciler.canonicalizer = current.canonicalizer
		record, found, err := composition.run.Backend.LoadDeleteReconciliation(
			request.RunID,
			request.Task,
			request.AttemptID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("load retained SQL Server strict delete terminal for table %s: %w", entry.source.Name, err),
			)
		}
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQL Server migration strict delete table %s lacks terminal reconciliation evidence", entry.source.Name),
			)
		}
		outcome, err := terminalDeleteReconcileOutcome(record)
		if err != nil {
			return NewTransferError(ErrorClassState, err)
		}
		if err := composition.authenticateTerminalAuthority(
			readerCtx,
			entry,
			reconciler,
			request,
			outcome,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("authenticate retained SQL Server strict delete terminal for table %s: %w", entry.source.Name, err),
			)
		}
		strict, err = stage4AdapterPostgresDeleteTerminalStrictness(outcome)
		if err != nil {
			return NewTransferError(ErrorClassState, err)
		}
		return nil
	}); err != nil {
		return false, err
	}
	return strict, nil
}

func runStage4SQLServerStrictDeleteTransferTable(
	ctx context.Context,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	planIndex int,
	resume bool,
	reader stage4SQLServerStrictDeleteReader,
) (copied int, work stage4AdapterWork, resultErr error) {
	if err := reader(ctx, planIndex, func(
		readerCtx context.Context,
		stable adapterStableNetworkSource,
	) error {
		borrowed := &adapterStableNetworkTableSession{
			source: stable, readerLimit: 1,
		}
		tableExecution, err := execution.openTable(
			readerCtx,
			planIndex,
			resume,
			borrowed,
		)
		if err != nil {
			return err
		}
		copied, work, err = runStage4AdapterPostgresDeleteNetworkTable(
			readerCtx,
			observer,
			target,
			prepared,
			planIndex,
			tableExecution,
			resume,
		)
		return err
	}); err != nil {
		return 0, stage4AdapterWork{}, err
	}
	return copied, cloneStage4AdapterNetworkWork(work), resultErr
}

// reconcileStage4SQLServerStrictDeletes is a strict-reader variant of the
// generic Stage 4 delete bridge. It reuses the durable candidate/receipt core
// and the native SQL Server target journal, but substitutes a source capability
// that cannot query the ordinary live source.
func reconcileStage4SQLServerStrictDeletes(
	ctx context.Context,
	composition *stage4AdapterPostgresDeletePrepared,
	finalWork []stage4AdapterWork,
	reader stage4SQLServerStrictDeleteReader,
) (stage4AdapterPostgresDeleteResult, error) {
	if composition == nil || reader == nil {
		return stage4AdapterPostgresDeleteResult{}, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete reconciliation is unavailable"),
		)
	}
	result := stage4AdapterPostgresDeleteResult{
		tables:        make([]stage4AdapterPostgresDeleteTableOutcome, 0, len(composition.entries)),
		strictByTable: make(map[stage4RichTableKey]bool, len(composition.entries)),
	}
	bound, err := composition.bindFinalWork(finalWork)
	if err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	inventory, err := loadStage4WorkInventory(ctx, composition.run)
	if err != nil {
		return result, err
	}
	if err := composition.authenticateFinalWork(bound, inventory); err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	deleteOrder, err := stage4AdapterPostgresDeleteReversePlanIndexes(len(composition.entries))
	if err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	for _, order := range deleteOrder {
		entry := composition.entries[order]
		work := bound[entry.planIndex]
		var outcome deleteReconcileOutcome
		if err := reader(ctx, entry.planIndex, func(
			readerCtx context.Context,
			stable adapterStableNetworkSource,
		) error {
			current, capabilityErr := newSQLServerStrictDeleteReconciliationCapabilities(
				readerCtx,
				stable,
				entry.capabilities,
				entry.source,
				entry.target,
			)
			if capabilityErr != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("derive retained SQL Server strict delete authority for table %s: %w", entry.source.Name, capabilityErr),
				)
			}
			reconciler, request, requestErr := composition.requestFor(entry, work)
			if requestErr != nil {
				return NewTransferError(ErrorClassState, requestErr)
			}
			reconciler.source = current.source
			reconciler.target = current.target
			reconciler.canonicalizer = current.canonicalizer
			var reconcileErr error
			outcome, reconcileErr = reconciler.reconcile(readerCtx, request)
			if reconcileErr != nil {
				return fmt.Errorf("reconcile retained SQL Server strict target-only rows for table %s: %w", entry.source.Name, reconcileErr)
			}
			if authorityErr := composition.authenticateTerminalAuthority(
				readerCtx,
				entry,
				reconciler,
				request,
				outcome,
			); authorityErr != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("authenticate retained SQL Server strict delete authority for table %s: %w", entry.source.Name, authorityErr),
				)
			}
			return nil
		}); err != nil {
			return result, err
		}
		strict, strictErr := stage4AdapterPostgresDeleteTerminalStrictness(outcome)
		if strictErr != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf("authenticate retained SQL Server strict delete evidence for table %s: %w", entry.source.Name, strictErr),
			)
		}
		key := stage4RichTableKey{schema: entry.source.Schema, table: entry.source.Name}
		if _, duplicate := result.strictByTable[key]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf("duplicate retained SQL Server strict delete outcome for table (%q, %q)", key.schema, key.table),
			)
		}
		result.tables = append(result.tables, stage4AdapterPostgresDeleteTableOutcome{
			planIndex: entry.planIndex,
			work:      cloneStage4AdapterNetworkWork(work),
			outcome:   outcome,
		})
		result.strictByTable[key] = strict
	}
	if len(result.strictByTable) != len(composition.entries) {
		return result, NewTransferError(
			ErrorClassState,
			errors.New("retained SQL Server strict delete outcomes do not cover every selected table"),
		)
	}
	return result, nil
}

func completeStage4SQLServerStrictDeleteNoWork(
	ctx context.Context,
	observer TableObserver,
	relational *relationalSourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	scope state.StrictSnapshotScope,
	resume bool,
	completed map[string]int,
) (Result, error) {
	if len(completed) != len(prepared.plans) {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete completion has incomplete table checkpoints"),
		)
	}
	if scope == state.StrictSnapshotMigration {
		return completeStage4SQLServerMigrationStrictDeleteNoWork(
			ctx,
			observer,
			relational,
			target,
			prepared,
			execution,
			resume,
			completed,
		)
	}
	if err := activateStage4AdapterPostgresDeleteComposition(ctx, prepared); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if err := validateCompletedStage4SQLServerStrictDeleteTerminals(
		ctx,
		target,
		prepared,
		execution,
		completed,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result := resultForValidatedAdapterCheckpoints(completed)
	if err := completeStage4SchemaGateSentinels(
		ctx,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

// completeStage4SQLServerMigrationStrictDeleteNoWork handles the narrow
// cleanup-only resume after every ordinary table completion is durable. The
// source may have changed after the original migration snapshot was captured,
// so terminal delete authority is reauthenticated exclusively through that
// reopened snapshot rather than through the ordinary source adapter.
func completeStage4SQLServerMigrationStrictDeleteNoWork(
	ctx context.Context,
	observer TableObserver,
	relational *relationalSourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	result = resultForValidatedAdapterCheckpoints(completed)
	if !resume {
		return result, NewTransferError(
			ErrorClassState,
			errors.New("completed SQL Server migration-strict delete work requires explicit resume for snapshot cleanup"),
		)
	}
	if err := activateStage4AdapterPostgresDeleteComposition(ctx, prepared); err != nil {
		return result, err
	}
	owner, found, err := prepared.run.Backend.LoadLatestStrictMigrationSnapshot(prepared.run.RunID)
	if err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("load completed SQL Server migration strict delete snapshot cleanup owner: %w", err),
		)
	}
	if !found {
		return result, NewTransferError(
			ErrorClassState,
			errors.New("completed SQL Server migration strict delete work lacks a durable database snapshot cleanup owner"),
		)
	}
	cleanupBackend, supported := prepared.run.Backend.(state.StrictMigrationCleanupBackend)
	if !supported || isNilInterface(cleanupBackend) {
		return result, NewTransferError(
			ErrorClassPolicy,
			errors.New("completed SQL Server migration strict delete work lacks durable cleanup-intent state support"),
		)
	}
	intent, intentFound, err := cleanupBackend.LoadStrictMigrationCleanupIntent(
		prepared.run.RunID,
		owner.EpochID,
	)
	if err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("load completed SQL Server migration strict delete cleanup intent: %w", err),
		)
	}
	if intentFound && !stage4SQLServerMigrationCleanupIntentMatchesOwner(intent, owner) {
		return result, NewTransferError(
			ErrorClassState,
			errors.New("completed SQL Server migration strict delete cleanup intent differs from durable owner"),
		)
	}
	opener, err := NewSQLServerMigrationSnapshotOpener(
		relational.database,
		relational.endpoint,
		relational.namespace,
	)
	if err != nil {
		return result, err
	}
	rawSession, absent, err := opener.OpenCompletedMigrationSnapshot(
		ctx,
		prepared.run.RunID,
		owner,
		intentFound,
	)
	if err != nil {
		if errors.Is(err, errSQLServerMigrationSnapshotMissing) {
			return result, errors.Join(err, ErrSQLServerMigrationSnapshotNotResumable)
		}
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("reopen SQL Server migration strict delete snapshot for terminal authentication: %w", err),
		)
	}
	if absent {
		if !intentFound {
			return result, NewTransferError(
				ErrorClassState,
				errors.New("completed SQL Server migration strict delete snapshot is absent before durable cleanup intent"),
			)
		}
		if err := completeStage4SchemaGateSentinels(
			ctx,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	session, ok := rawSession.(*SQLServerMigrationSnapshotSession)
	if !ok || session == nil {
		return result, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete terminal authentication received an unexpected snapshot session"),
		)
	}
	if err := session.PreserveSnapshotForResume(); err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("preserve SQL Server migration strict delete snapshot before terminal authentication: %w", err),
		)
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			strictConsistencyCleanupTimeout,
		)
		defer cancel()
		if closeErr := session.Close(cleanupCtx); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf(
				"preserve SQL Server migration strict delete snapshot after terminal authentication failure: %w",
				closeErr,
			))
		}
	}()
	if err := validateCompletedStage4SQLServerMigrationStrictDeleteTerminals(
		ctx,
		target,
		prepared,
		execution,
		completed,
		session,
	); err != nil {
		return result, err
	}
	resultErr = finalizeStage4SQLServerMigrationSnapshotCleanup(
		ctx,
		session,
		owner,
		intent,
		intentFound,
		cleanupBackend,
		func() error {
			return completeStage4SchemaGateSentinels(
				ctx,
				prepared.run,
				prepared.gate,
				prepared.evolution,
			)
		},
	)
	closed = true
	if resultErr != nil {
		return result, resultErr
	}
	result.Validated = true
	return result, nil
}

func validateCompletedStage4SQLServerStrictDeleteTerminals(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
) error {
	if prepared.deletes == nil || execution == nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server strict delete terminal validation is unavailable"),
		)
	}
	strictByTable := make(map[stage4RichTableKey]bool, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		rows, found := completed[plan.source.Name]
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server strict delete terminal is missing table %s", plan.source.Name),
			)
		}
		bound, found, err := execution.classifyStage4AdapterPostgresDeleteTransferredTable(ctx, planIndex)
		if err != nil {
			return err
		}
		if !found || !bound.taskCompleted || bound.rows != rows {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server strict delete table %s lacks authenticated transfer evidence", plan.source.Name),
			)
		}
		strict, err := authenticateStage4AdapterPostgresDeleteTerminal(
			ctx,
			prepared.deletes,
			planIndex,
			bound.work,
		)
		if err != nil {
			return err
		}
		// The completed checkpoint is the durable ordinary-table completion
		// authority in this no-work resume branch.  classify... intentionally
		// reconstructs only transfer state, so bind that independent fact here
		// rather than requiring an in-memory flag from the prior process.
		bound.ordinaryCompleted = true
		bound.terminalAuthenticated = true
		bound.terminalStrict = strict
		if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(planIndex, bound); err != nil {
			return err
		}
		strictByTable[stage4RichTableKey{schema: plan.source.Schema, table: plan.source.Name}] = strict
	}
	prepared.deleteReconciliationStrict = strictByTable
	if err := prevalidateStage4AdapterPostgresDeleteCompletedTargets(
		ctx,
		target,
		prepared,
		execution,
	); err != nil {
		return err
	}
	return validateStage4AdapterPostgresDeleteCheckpointRows(
		ctx,
		target,
		prepared,
		func() []int {
			rows := make([]int, len(prepared.plans))
			for index, plan := range prepared.plans {
				rows[index] = completed[plan.source.Name]
			}
			return rows
		}(),
	)
}

// validateCompletedStage4SQLServerMigrationStrictDeleteTerminals is the
// migration-snapshot counterpart of the ordinary no-work terminal validator.
// It deliberately substitutes the reopened snapshot for every source-side
// catalog/key authority read, so a post-evidence source mutation cannot turn a
// cleanup-only resume into a live-source validation.
func validateCompletedStage4SQLServerMigrationStrictDeleteTerminals(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
	session *SQLServerMigrationSnapshotSession,
) error {
	if prepared.deletes == nil || execution == nil || session == nil ||
		isNilInterface(prepared.deletes.source) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict delete terminal validation is unavailable"),
		)
	}
	reader := func(
		readerCtx context.Context,
		planIndex int,
		work func(context.Context, adapterStableNetworkSource) error,
	) error {
		return runStage4SQLServerMigrationStrictDeleteTerminalReader(
			readerCtx,
			session,
			prepared.deletes.source,
			prepared,
			planIndex,
			work,
		)
	}
	strictByTable := make(map[stage4RichTableKey]bool, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		rows, found := completed[plan.source.Name]
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server migration strict delete terminal is missing table %s", plan.source.Name),
			)
		}
		bound, found, err := execution.classifyStage4AdapterPostgresDeleteTransferredTable(ctx, planIndex)
		if err != nil {
			return err
		}
		if !found || !bound.taskCompleted || bound.rows != rows {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server migration strict delete table %s lacks authenticated transfer evidence", plan.source.Name),
			)
		}
		strict, err := authenticateStage4SQLServerStrictDeleteTerminal(
			ctx,
			prepared.deletes,
			planIndex,
			bound.work,
			reader,
		)
		if err != nil {
			return err
		}
		bound.ordinaryCompleted = true
		bound.terminalAuthenticated = true
		bound.terminalStrict = strict
		if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(planIndex, bound); err != nil {
			return err
		}
		strictByTable[stage4RichTableKey{schema: plan.source.Schema, table: plan.source.Name}] = strict
	}
	prepared.deleteReconciliationStrict = strictByTable
	return validateStage4AdapterPostgresDeleteCheckpointRows(
		ctx,
		target,
		prepared,
		func() []int {
			rows := make([]int, len(prepared.plans))
			for index, plan := range prepared.plans {
				rows[index] = completed[plan.source.Name]
			}
			return rows
		}(),
	)
}

func closeStage4SQLServerStrictDeleteExecution(
	ctx context.Context,
	execution *StrictConsistencyExecution,
	primary error,
) error {
	if execution == nil {
		return primary
	}
	if closeErr := execution.Close(ctx); closeErr != nil {
		return errors.Join(primary, closeErr)
	}
	return primary
}
