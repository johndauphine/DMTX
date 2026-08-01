package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const stage4PostgresStrictWorkVersion = 1

type stage4PostgresStrictEpochBinding struct {
	tables map[state.TaskKey]stage4PostgresStrictTableBinding
}

type stage4PostgresStrictTableBinding struct {
	scope             state.StrictSnapshotScope
	processEpoch      string
	snapshotReference string
}

type stage4PostgresStrictParallelSource struct {
	adapterStableNetworkSource

	readerSlots   chan struct{}
	primaryReader chan struct{}
	openSecondary func(
		context.Context,
		func(adapterStableNetworkSource) error,
	) error
}

func newStage4PostgresStrictParallelSource(
	primary adapterStableNetworkSource,
	readerLimit int,
	openSecondary func(
		context.Context,
		func(adapterStableNetworkSource) error,
	) error,
) (*stage4PostgresStrictParallelSource, error) {
	if isNilInterface(primary) {
		return nil, errors.New(
			"PostgreSQL strict parallel source requires its primary imported snapshot reader",
		)
	}
	if readerLimit < 1 {
		return nil, errors.New(
			"PostgreSQL strict parallel source reader limit must be positive",
		)
	}
	if readerLimit > 1 && isNilInterface(openSecondary) {
		return nil, errors.New(
			"PostgreSQL strict parallel source requires an imported secondary reader opener",
		)
	}
	source := &stage4PostgresStrictParallelSource{
		adapterStableNetworkSource: primary,
		readerSlots:                make(chan struct{}, readerLimit),
		primaryReader:              make(chan struct{}, 1),
		openSecondary:              openSecondary,
	}
	source.primaryReader <- struct{}{}
	return source, nil
}

func (
	source *stage4PostgresStrictParallelSource,
) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	rangePlan PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if source == nil || isNilInterface(source.adapterStableNetworkSource) {
		return NetworkReadPage{}, errors.New(
			"PostgreSQL strict parallel source is unavailable",
		)
	}
	if ctx == nil {
		return NetworkReadPage{}, errors.New(
			"PostgreSQL strict parallel source context is required",
		)
	}
	select {
	case <-ctx.Done():
		return NetworkReadPage{}, ctx.Err()
	case source.readerSlots <- struct{}{}:
	}
	defer func() {
		<-source.readerSlots
	}()

	select {
	case <-source.primaryReader:
		defer func() {
			source.primaryReader <- struct{}{}
		}()
		return source.adapterStableNetworkSource.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			pagination,
			rangePlan,
			request,
		)
	default:
	}
	if isNilInterface(source.openSecondary) {
		return NetworkReadPage{}, errors.New(
			"PostgreSQL strict parallel source has no secondary reader",
		)
	}
	var page NetworkReadPage
	err := source.openSecondary(
		ctx,
		func(secondary adapterStableNetworkSource) error {
			if isNilInterface(secondary) {
				return errors.New(
					"PostgreSQL strict secondary reader is unavailable",
				)
			}
			var err error
			page, err = secondary.ReadNetworkRangePage(
				ctx,
				table,
				columns,
				pagination,
				rangePlan,
				request,
			)
			return err
		},
	)
	return page, err
}

func (
	source *stage4PostgresStrictParallelSource,
) Stage4ValidationProbe(
	routeSource sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if source == nil || isNilInterface(source.adapterStableNetworkSource) {
		return nil, errors.New(
			"PostgreSQL strict parallel validation source is unavailable",
		)
	}
	provider, ok := source.adapterStableNetworkSource.(adapterStage4ValidationProbeProvider)
	if !ok || isNilInterface(provider) {
		return nil, errors.New(
			"PostgreSQL strict primary reader lacks its stable validation seam",
		)
	}
	return provider.Stage4ValidationProbe(routeSource, target, plans)
}

func inheritStage4PostgresStrictSameSnapshotProofs(
	primary adapterStableNetworkSource,
	secondary adapterStableNetworkSource,
) error {
	primaryView, ok := primary.(*adapterRetainedStableRelationalView)
	if !ok || primaryView == nil {
		return errors.New(
			"PostgreSQL strict primary reader has an unexpected stable-view type",
		)
	}
	secondaryView, ok :=
		secondary.(*adapterRetainedStableRelationalView)
	if !ok || secondaryView == nil {
		return errors.New(
			"PostgreSQL strict secondary reader has an unexpected stable-view type",
		)
	}
	primaryView.mu.Lock()
	if primaryView.source == nil ||
		primaryView.tableScope == nil ||
		primaryView.tableCatalog == nil {
		primaryView.mu.Unlock()
		return errors.New(
			"PostgreSQL strict primary reader lacks its table authority",
		)
	}
	source := primaryView.source
	scope := *primaryView.tableScope
	catalog := cloneStage4RichTable(*primaryView.tableCatalog)
	retained := make(
		map[string]int64,
		len(primaryView.retainedRowBounds),
	)
	for identity, bound := range primaryView.retainedRowBounds {
		retained[identity] = bound
	}
	pagination := make(
		map[string]PaginationPlan,
		len(primaryView.paginationPlans),
	)
	for identity, plan := range primaryView.paginationPlans {
		pagination[identity] = clonePaginationPlan(plan)
	}
	primaryView.mu.Unlock()
	if len(retained) == 0 || len(pagination) == 0 {
		return errors.New(
			"PostgreSQL strict primary reader lacks exact stable planning proofs",
		)
	}

	secondaryView.mu.Lock()
	defer secondaryView.mu.Unlock()
	if secondaryView.source != source ||
		secondaryView.tableScope == nil ||
		*secondaryView.tableScope != scope ||
		secondaryView.tableCatalog == nil {
		return errors.New(
			"PostgreSQL strict secondary reader differs from the primary table authority",
		)
	}
	sameCatalog, err := stage4AdapterNetworkCatalogEqual(
		*secondaryView.tableCatalog,
		catalog,
	)
	if err != nil {
		return err
	}
	if !sameCatalog {
		return errors.New(
			"PostgreSQL strict secondary reader catalog differs from the primary stable view",
		)
	}
	secondaryView.retainedRowBounds = retained
	secondaryView.paginationPlans = pagination
	return nil
}

func requireStage4PostgresStrictRoute(
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	mode string,
) error {
	if !cfg.Migration.StrictConsistency {
		return nil
	}
	if mode != "upsert" ||
		isNilInterface(source) ||
		isNilInterface(target) ||
		source.Engine() != "postgres" ||
		!stage4AdapterNetworkRelationalEngine(target.Engine()) {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"Stage 4 PostgreSQL strict consistency requires an upsert route to a certified relational or SQLite target",
			),
		)
	}
	if relational, ok := source.(*relationalSourceAdapter); !ok ||
		relational == nil || relational.database == nil {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"PostgreSQL strict composition requires the production relational source adapter",
			),
		)
	}
	if upsertTarget, ok := target.(adapterStage4NetworkUpsertTarget); !ok ||
		isNilInterface(upsertTarget) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 PostgreSQL strict target engine %q has no certified idempotent network upsert path",
				target.Engine(),
			),
		)
	}
	return nil
}

func migrateWithStage4PostgresStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	if !cfg.Migration.StrictConsistency {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New(
				"PostgreSQL strict composition requires strict consistency",
			),
		)
	}
	if prepared.mode != "upsert" ||
		source.Engine() != "postgres" ||
		!stage4AdapterNetworkRelationalEngine(target.Engine()) {
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 PostgreSQL strict network composition requires an upsert route to a certified relational or SQLite target",
			),
		)
	}
	if networkExecution == nil || !networkExecution.deferred {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New(
				"PostgreSQL strict composition requires deferred network work",
			),
		)
	}
	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational == nil || relational.database == nil {
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"PostgreSQL strict composition requires the production relational source adapter",
			),
		)
	}
	scope, err := stage4PostgresStrictScope(
		cfg.Migration.StrictConsistencyScope,
	)
	if err != nil {
		return Result{}, err
	}
	completedBinding, err :=
		validateCompletedStage4PostgresStrictEvidence(
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
	if err := networkExecution.prevalidateCompletedTables(
		ctx,
		completed,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}

	selectedTasks := make([]state.TaskKey, 0, len(prepared.work))
	for index, plan := range prepared.plans {
		if _, complete := completed[plan.source.Name]; complete {
			continue
		}
		selectedTasks = append(
			selectedTasks,
			prepared.work[index].task,
		)
	}
	if len(selectedTasks) == 0 {
		result = resultForValidatedAdapterCheckpoints(completed)
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

	processEpoch, err := newStage4PostgresProcessEpoch()
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	opener, err := NewPostgresStrictConsistencyOpener(
		relational.database,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if scope == state.StrictSnapshotTable {
		return migrateWithStage4PostgresTableStrictAdapters(
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
		)
	}
	strictExecution, err := BeginPlannedStrictConsistency(
		ctx,
		PlannedStrictConsistencyRequest{
			RunID:        prepared.run.RunID,
			SourceEngine: StrictConsistencyPostgres,
			Scope:        scope,
			Resume:       resume,
			ProcessEpoch: processEpoch,
			State:        prepared.run.Backend,
			Tasks:        selectedTasks,
		},
		opener,
		func(
			planCtx context.Context,
			rawSession StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			session, ok := rawSession.(*PostgresStrictConsistencySession)
			if !ok || session == nil {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New(
						"PostgreSQL planned strict session has an unexpected type",
					),
				)
			}
			currentBinding, err :=
				newStage4PostgresStrictEpochBinding(
					scope,
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
			return planStage4PostgresStrictNetworkWork(
				planCtx,
				observer,
				source,
				prepared,
				networkExecution,
				session,
				resume,
				completed,
			)
		},
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}

	strictPrepared := prepared
	strictPrepared.strictSourceRows, err =
		stage4PostgresStrictSourceRows(
			strictExecution.Evidence(),
		)
	if err != nil {
		closeErr := strictExecution.Close(ctx)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	resultErr = strictExecution.Run(
		ctx,
		func(
			runCtx context.Context,
			rawSession StrictConsistencySession,
		) error {
			session, ok := rawSession.(*PostgresStrictConsistencySession)
			if !ok || session == nil {
				return NewTransferError(
					ErrorClassState,
					errors.New(
						"PostgreSQL strict execution session has an unexpected type",
					),
				)
			}
			if err := applyStage4AdapterTargetSchema(
				runCtx,
				observer,
				strictPrepared.run,
				strictPrepared.gate,
				strictPrepared.evolution,
			); err != nil {
				return err
			}
			if err := preflightStage4AdapterDesiredTargetAfterEvolution(
				runCtx,
				target,
				strictPrepared,
			); err != nil {
				return err
			}
			for planIndex, plan := range strictPrepared.plans {
				if rows, complete := completed[plan.source.Name]; complete {
					if err := networkExecution.advanceCompletedTable(
						runCtx,
						planIndex,
						rows,
					); err != nil {
						return err
					}
					continue
				}
				copied, err := runStage4PostgresStrictTable(
					runCtx,
					cfg,
					observer,
					source,
					target,
					strictPrepared,
					networkExecution,
					session,
					planIndex,
					resume,
				)
				if err != nil {
					return err
				}
				if copied < 0 ||
					result.Rows > math.MaxInt-copied {
					return NewTransferError(
						ErrorClassState,
						errors.New(
							"PostgreSQL strict result row total overflows",
						),
					)
				}
				result.Tables++
				result.Rows += copied
			}
			if err := completeStage4SchemaGateSentinels(
				runCtx,
				strictPrepared.run,
				strictPrepared.gate,
				strictPrepared.evolution,
			); err != nil {
				return err
			}
			result.Validated = true
			return nil
		},
	)
	return result, resultErr
}

func migrateWithStage4PostgresTableStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	opener *PostgresStrictConsistencyOpener,
	processEpoch string,
	completedBinding stage4PostgresStrictEpochBinding,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		prepared.names,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	schemaApplied := false
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := networkExecution.advanceCompletedTable(
				ctx,
				planIndex,
				rows,
			); err != nil {
				return result, err
			}
			continue
		}
		task := prepared.work[planIndex].task
		strictExecution, err := BeginPlannedStrictConsistency(
			ctx,
			PlannedStrictConsistencyRequest{
				RunID:        prepared.run.RunID,
				SourceEngine: StrictConsistencyPostgres,
				Scope:        state.StrictSnapshotTable,
				Resume:       resume,
				ProcessEpoch: processEpoch,
				State:        prepared.run.Backend,
				Tasks:        []state.TaskKey{task},
			},
			opener,
			func(
				planCtx context.Context,
				rawSession StrictConsistencySession,
				capture StrictConsistencyCapture,
			) ([]StrictConsistencyTable, error) {
				session, ok := rawSession.(*PostgresStrictConsistencySession)
				if !ok || session == nil {
					return nil, NewTransferError(
						ErrorClassState,
						errors.New(
							"PostgreSQL planned table-strict session has an unexpected type",
						),
					)
				}
				currentBinding, err :=
					newStage4PostgresStrictEpochBinding(
						state.StrictSnapshotTable,
						processEpoch,
						capture,
					)
				if err != nil {
					return nil, err
				}
				binding, err := completedBinding.merge(
					currentBinding,
				)
				if err != nil {
					return nil, err
				}
				networkExecution.finalizeWork = binding.finalizeWork
				table, err :=
					planStage4PostgresStrictNetworkTable(
						planCtx,
						source,
						prepared,
						networkExecution,
						session,
						planIndex,
						resume,
					)
				if err != nil {
					return nil, err
				}
				return []StrictConsistencyTable{table}, nil
			},
		)
		if err != nil {
			return result, err
		}
		strictPrepared := prepared
		strictPrepared.strictSourceRows, err =
			stage4PostgresStrictSourceRows(
				strictExecution.Evidence(),
			)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		currentBinding, err :=
			newStage4PostgresStrictEpochBinding(
				state.StrictSnapshotTable,
				processEpoch,
				strictConsistencyCaptureFromEvidence(
					strictExecution.Evidence(),
				),
			)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		nextBinding, err := completedBinding.merge(currentBinding)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		resultErr = strictExecution.Run(
			ctx,
			func(
				runCtx context.Context,
				rawSession StrictConsistencySession,
			) error {
				session, ok := rawSession.(*PostgresStrictConsistencySession)
				if !ok || session == nil {
					return NewTransferError(
						ErrorClassState,
						errors.New(
							"PostgreSQL table-strict execution session has an unexpected type",
						),
					)
				}
				if !schemaApplied {
					if err := applyStage4AdapterTargetSchema(
						runCtx,
						observer,
						strictPrepared.run,
						strictPrepared.gate,
						strictPrepared.evolution,
					); err != nil {
						return err
					}
					if err := preflightStage4AdapterDesiredTargetAfterEvolution(
						runCtx,
						target,
						strictPrepared,
					); err != nil {
						return err
					}
					schemaApplied = true
				}
				copied, err := runStage4PostgresStrictTable(
					runCtx,
					cfg,
					observer,
					source,
					target,
					strictPrepared,
					networkExecution,
					session,
					planIndex,
					resume,
				)
				if err != nil {
					return err
				}
				if copied < 0 || result.Rows > math.MaxInt-copied {
					return NewTransferError(
						ErrorClassState,
						errors.New(
							"PostgreSQL table-strict result row total overflows",
						),
					)
				}
				result.Tables++
				result.Rows += copied
				return nil
			},
		)
		if resultErr != nil {
			return result, resultErr
		}
		completedBinding = nextBinding
		networkExecution.finalizeWork =
			completedBinding.finalizeWork
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

func strictConsistencyCaptureFromEvidence(
	evidence []state.StrictSnapshotEvidence,
) StrictConsistencyCapture {
	capture := StrictConsistencyCapture{
		Tables: make(
			[]StrictConsistencyTableCapture,
			len(evidence),
		),
	}
	for index, record := range evidence {
		capture.Tables[index] = StrictConsistencyTableCapture{
			Task:                record.Task,
			AttemptID:           record.AttemptID,
			SnapshotReference:   record.SnapshotReference,
			ExactSourceRowCount: record.ExactSourceRowCount,
			CapturedAt:          record.CapturedAt,
		}
	}
	return capture
}

func planStage4PostgresStrictNetworkTable(
	ctx context.Context,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *PostgresStrictConsistencySession,
	planIndex int,
	resume bool,
) (_ StrictConsistencyTable, resultErr error) {
	plan := prepared.plans[planIndex]
	execution.mu.Lock()
	startGlobalRange := execution.nextGlobalRange
	execution.mu.Unlock()
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = startGlobalRange
		execution.mu.Unlock()
	}()

	var work stage4AdapterWork
	err := RunPostgresAdapterStableNetworkReader(
		ctx,
		session,
		prepared.work[planIndex].task,
		source,
		plan.source,
		func(
			readerCtx context.Context,
			stable adapterStableNetworkSource,
		) error {
			borrowed := &adapterStableNetworkTableSession{
				source: stable,
				readerLimit: execution.resources.
					Readers.Value,
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
			defer func() {
				if closeErr := tableExecution.Close(); closeErr != nil {
					resultErr = errors.Join(resultErr, closeErr)
				}
			}()
			work = cloneStage4AdapterNetworkWork(
				tableExecution.work,
			)
			return nil
		},
	)
	if err != nil {
		return StrictConsistencyTable{}, err
	}
	attemptID, err := BuildStrictConsistencyAttemptID(
		work.task,
		work.topology,
		0,
	)
	if err != nil {
		return StrictConsistencyTable{}, fmt.Errorf(
			"build PostgreSQL table-strict work attempt for %s: %w",
			plan.source.Name,
			err,
		)
	}
	return StrictConsistencyTable{
		Task:                work.task,
		AttemptID:           attemptID,
		WorkTopologyHash:    work.topology,
		DurableWorkAttempts: 0,
	}, resultErr
}

func planStage4PostgresStrictNetworkWork(
	ctx context.Context,
	observer TableObserver,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *PostgresStrictConsistencySession,
	resume bool,
	completed map[string]int,
) (_ []StrictConsistencyTable, resultErr error) {
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		prepared.names,
	); err != nil {
		return nil, err
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	result := make(
		[]StrictConsistencyTable,
		0,
		len(prepared.plans),
	)
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := execution.advanceCompletedTable(
				ctx,
				planIndex,
				rows,
			); err != nil {
				return nil, err
			}
			continue
		}
		var work stage4AdapterWork
		err := RunPostgresAdapterStableNetworkReader(
			ctx,
			session,
			prepared.work[planIndex].task,
			source,
			plan.source,
			func(
				readerCtx context.Context,
				stable adapterStableNetworkSource,
			) error {
				borrowed := &adapterStableNetworkTableSession{
					source: stable,
					readerLimit: execution.resources.
						Readers.Value,
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
				defer func() {
					if closeErr := tableExecution.Close(); closeErr != nil {
						resultErr = errors.Join(
							resultErr,
							closeErr,
						)
					}
				}()
				work = cloneStage4AdapterNetworkWork(
					tableExecution.work,
				)
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
		attemptID, err := BuildStrictConsistencyAttemptID(
			work.task,
			work.topology,
			0,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"build PostgreSQL strict work attempt for %s: %w",
				plan.source.Name,
				err,
			)
		}
		result = append(result, StrictConsistencyTable{
			Task:                work.task,
			AttemptID:           attemptID,
			WorkTopologyHash:    work.topology,
			DurableWorkAttempts: 0,
		})
	}
	return result, resultErr
}

func runStage4PostgresStrictTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	session *PostgresStrictConsistencySession,
	planIndex int,
	resume bool,
) (copied int, resultErr error) {
	plan := prepared.plans[planIndex]
	err := RunPostgresAdapterStableNetworkReader(
		ctx,
		session,
		prepared.work[planIndex].task,
		source,
		plan.source,
		func(
			readerCtx context.Context,
			stable adapterStableNetworkSource,
		) error {
			parallel, err :=
				newStage4PostgresStrictParallelSource(
					stable,
					networkExecution.resources.Readers.Value,
					func(
						secondaryCtx context.Context,
						work func(
							adapterStableNetworkSource,
						) error,
					) error {
						return RunPostgresAdapterStableNetworkReader(
							secondaryCtx,
							session,
							prepared.work[planIndex].task,
							source,
							plan.source,
							func(
								_ context.Context,
								secondary adapterStableNetworkSource,
							) error {
								if err := inheritStage4PostgresStrictSameSnapshotProofs(
									stable,
									secondary,
								); err != nil {
									return err
								}
								return work(secondary)
							},
						)
					},
				)
			if err != nil {
				return err
			}
			borrowed := &adapterStableNetworkTableSession{
				source: parallel,
				readerLimit: networkExecution.resources.
					Readers.Value,
			}
			tableExecution, err := networkExecution.openTable(
				readerCtx,
				planIndex,
				false,
				borrowed,
			)
			if err != nil {
				return err
			}
			copied, err = runStage4AdapterStableNetworkTable(
				readerCtx,
				cfg,
				observer,
				target,
				prepared,
				planIndex,
				tableExecution,
				resume,
			)
			return err
		},
	)
	if err != nil {
		return 0, err
	}
	return copied, resultErr
}

func newStage4PostgresStrictEpochBinding(
	scope state.StrictSnapshotScope,
	processEpoch string,
	capture StrictConsistencyCapture,
) (stage4PostgresStrictEpochBinding, error) {
	if err := validateCredentialFreeIdentifier(
		"PostgreSQL strict process epoch",
		processEpoch,
	); err != nil {
		return stage4PostgresStrictEpochBinding{}, err
	}
	tables := make(
		map[state.TaskKey]stage4PostgresStrictTableBinding,
		len(capture.Tables),
	)
	for _, table := range capture.Tables {
		if _, duplicate := tables[table.Task]; duplicate {
			return stage4PostgresStrictEpochBinding{}, errors.New(
				"PostgreSQL strict capture duplicates a table reference",
			)
		}
		if err := validateSnapshotReference(
			table.SnapshotReference,
		); err != nil {
			return stage4PostgresStrictEpochBinding{}, err
		}
		tables[table.Task] = stage4PostgresStrictTableBinding{
			scope:             scope,
			processEpoch:      processEpoch,
			snapshotReference: table.SnapshotReference,
		}
	}
	if len(tables) == 0 {
		return stage4PostgresStrictEpochBinding{}, errors.New(
			"PostgreSQL strict capture has no table references",
		)
	}
	return stage4PostgresStrictEpochBinding{
		tables: tables,
	}, nil
}

func (
	binding stage4PostgresStrictEpochBinding,
) merge(
	other stage4PostgresStrictEpochBinding,
) (stage4PostgresStrictEpochBinding, error) {
	tables := make(
		map[state.TaskKey]stage4PostgresStrictTableBinding,
		len(binding.tables)+len(other.tables),
	)
	for task, table := range binding.tables {
		tables[task] = table
	}
	for task, table := range other.tables {
		if _, duplicate := tables[task]; duplicate {
			return stage4PostgresStrictEpochBinding{}, fmt.Errorf(
				"PostgreSQL strict epoch binding duplicates task %s.%s",
				task.Schema,
				task.Table,
			)
		}
		tables[task] = table
	}
	return stage4PostgresStrictEpochBinding{tables: tables}, nil
}

func (binding stage4PostgresStrictEpochBinding) finalizeWork(
	work stage4AdapterWork,
) (stage4AdapterWork, error) {
	table, ok := binding.tables[work.task]
	if !ok {
		return stage4AdapterWork{}, fmt.Errorf(
			"strict epoch has no snapshot reference for %s.%s",
			work.task.Schema,
			work.task.Table,
		)
	}
	wire := struct {
		Version           int                       `json:"version"`
		BaseTopology      string                    `json:"base_topology"`
		Scope             state.StrictSnapshotScope `json:"scope"`
		ProcessEpoch      string                    `json:"process_epoch"`
		SnapshotReference string                    `json:"snapshot_reference"`
	}{
		Version:           stage4PostgresStrictWorkVersion,
		BaseTopology:      work.topology,
		Scope:             table.scope,
		ProcessEpoch:      table.processEpoch,
		SnapshotReference: table.snapshotReference,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return stage4AdapterWork{}, fmt.Errorf(
			"encode PostgreSQL strict work identity: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	work.topology = hex.EncodeToString(digest[:])
	for index := range work.ranges {
		work.ranges[index].TopologyHash = work.topology
	}
	return work, nil
}

func stage4PostgresStrictScope(
	value string,
) (state.StrictSnapshotScope, error) {
	normalized, err := normalizedStrictConsistencyScope(value)
	if err != nil {
		return "", NewTransferError(ErrorClassPolicy, err)
	}
	switch normalized {
	case config.StrictConsistencyTable:
		return state.StrictSnapshotTable, nil
	case config.StrictConsistencyMigration:
		return state.StrictSnapshotMigration, nil
	default:
		return "", NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"PostgreSQL strict consistency scope %q is unsupported",
				normalized,
			),
		)
	}
}

func newStage4PostgresProcessEpoch() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"generate PostgreSQL strict process epoch: %w",
				err,
			),
		)
	}
	return "pg-process-" + hex.EncodeToString(value[:]), nil
}

func stage4PostgresStrictSourceRows(
	evidence []state.StrictSnapshotEvidence,
) (map[stage4RichTableKey]int64, error) {
	result := make(
		map[stage4RichTableKey]int64,
		len(evidence),
	)
	for _, record := range evidence {
		if record.SourceEngine != "postgres" ||
			record.ExactSourceRowCount < 0 {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New(
					"PostgreSQL strict execution returned invalid count evidence",
				),
			)
		}
		key := stage4RichTableKey{
			schema: record.Task.Schema,
			table:  record.Task.Table,
		}
		if _, duplicate := result[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"PostgreSQL strict execution duplicates count evidence for %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		result[key] = record.ExactSourceRowCount
	}
	return result, nil
}

func validateCompletedStage4PostgresStrictEvidence(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	scope state.StrictSnapshotScope,
	completed map[string]int,
) (stage4PostgresStrictEpochBinding, error) {
	binding := stage4PostgresStrictEpochBinding{
		tables: make(
			map[state.TaskKey]stage4PostgresStrictTableBinding,
			len(completed),
		),
	}
	if len(completed) == 0 {
		return binding, ctx.Err()
	}
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return stage4PostgresStrictEpochBinding{}, err
	}
	for index, plan := range prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		base := prepared.work[index]
		task, found := inventory.tasks[base.task]
		if !found || task.Status != "completed" ||
			task.TopologyHash == "" {
			return stage4PostgresStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed PostgreSQL strict table %s lacks exact durable work",
					plan.source.Name,
				),
			)
		}
		attemptID, err := BuildStrictConsistencyAttemptID(
			task.Key,
			task.TopologyHash,
			0,
		)
		if err != nil {
			return stage4PostgresStrictEpochBinding{}, err
		}
		evidence, found, err := prepared.run.Backend.
			LoadStrictSnapshotEvidence(
				prepared.run.RunID,
				task.Key,
				attemptID,
			)
		if err != nil {
			return stage4PostgresStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"load completed PostgreSQL strict evidence for %s: %w",
					plan.source.Name,
					err,
				),
			)
		}
		if !found ||
			evidence.RunID != prepared.run.RunID ||
			evidence.Task != task.Key ||
			evidence.AttemptID != attemptID ||
			evidence.SourceEngine != "postgres" ||
			evidence.Scope != scope ||
			evidence.ProcessEpoch == "" ||
			evidence.SnapshotReference == "" ||
			evidence.ExactSourceRowCount != int64(rows) ||
			evidence.CapturedAt.IsZero() {
			return stage4PostgresStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed PostgreSQL strict table %s lacks matching immutable snapshot evidence",
					plan.source.Name,
				),
			)
		}
		if scope == state.StrictSnapshotMigration {
			owner, found, err := prepared.run.Backend.
				LoadStrictMigrationSnapshot(
					prepared.run.RunID,
					evidence.MigrationEpochID,
				)
			if err != nil {
				return stage4PostgresStrictEpochBinding{}, err
			}
			if !found ||
				owner.RunID != evidence.RunID ||
				owner.SourceEngine != "postgres" ||
				owner.EpochID != evidence.MigrationEpochID ||
				owner.SnapshotReference !=
					evidence.SnapshotReference ||
				owner.ProcessEpoch != evidence.ProcessEpoch ||
				!owner.CapturedAt.Equal(evidence.CapturedAt) {
				return stage4PostgresStrictEpochBinding{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"completed PostgreSQL strict table %s has no matching migration epoch owner",
						plan.source.Name,
					),
				)
			}
		} else if evidence.MigrationEpochID != "" {
			return stage4PostgresStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed table-scoped PostgreSQL strict table %s claims a migration epoch",
					plan.source.Name,
				),
			)
		}
		if err := validateCredentialFreeIdentifier(
			"completed PostgreSQL strict process epoch",
			evidence.ProcessEpoch,
		); err != nil {
			return stage4PostgresStrictEpochBinding{}, err
		}
		if err := validateSnapshotReference(
			evidence.SnapshotReference,
		); err != nil {
			return stage4PostgresStrictEpochBinding{}, err
		}
		binding.tables[task.Key] = stage4PostgresStrictTableBinding{
			scope:             evidence.Scope,
			processEpoch:      evidence.ProcessEpoch,
			snapshotReference: evidence.SnapshotReference,
		}
	}
	return binding, ctx.Err()
}
