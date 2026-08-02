package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Stage 4 network transfer execution: binding durable restores to a run,
// opening and planning each table, and reporting runtime tuning.

func bindStage4AdapterNetworkRestoresAndValidate(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
) error {
	if execution == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network execution is unavailable"),
		)
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network restore context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network target writes require a lease-fenced mutation protector",
			),
		)
	}
	execution.mu.Lock()
	if execution.binding || execution.bound || execution.started {
		execution.mu.Unlock()
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network restores are already bound or executing",
			),
		)
	}
	execution.binding = true
	execution.mu.Unlock()
	succeeded := false
	defer func() {
		execution.mu.Lock()
		execution.binding = false
		if succeeded {
			execution.bound = true
		}
		execution.mu.Unlock()
	}()
	if len(execution.ranges) == 0 {
		succeeded = true
		return nil
	}
	restores, err := execution.coordinator.loadRestores(ctx)
	if err != nil {
		return fmt.Errorf(
			"load Stage 4 durable network restores: %w",
			err,
		)
	}
	plan := execution.plan
	plan.Ranges = append([]NetworkRangePlan(nil), execution.plan.Ranges...)
	plan.Restores = make([]NetworkRangeRestore, len(restores))
	for index := range restores {
		plan.Restores[index] = cloneNetworkRestore(restores[index])
	}
	callbacks := execution.callbacks(observer)
	states, err := validateNetworkTransferPlan(plan, callbacks)
	if err != nil {
		return fmt.Errorf("validate Stage 4 network execution plan: %w", err)
	}
	if len(states) != len(execution.ranges) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 validated network state inventory is incomplete",
			),
		)
	}
	waves, err := buildStage4AdapterNetworkWaves(
		plan,
		execution.ranges,
		execution.tableCount,
	)
	if err != nil {
		return fmt.Errorf("build Stage 4 network dependency waves: %w", err)
	}
	for index := range waves {
		mapped, mapErr := waves[index].callbacks(callbacks)
		if mapErr != nil {
			return fmt.Errorf(
				"bind Stage 4 network dependency wave %d: %w",
				index,
				mapErr,
			)
		}
		if _, validateErr := validateNetworkTransferPlan(
			waves[index].plan,
			mapped,
		); validateErr != nil {
			return fmt.Errorf(
				"validate Stage 4 network dependency wave %d: %w",
				index,
				validateErr,
			)
		}
	}
	execution.mu.Lock()
	execution.plan = plan
	execution.waves = waves
	execution.mu.Unlock()
	succeeded = true
	return nil
}

// runStage4AdapterNetworkTransfer consumes one restore-bound execution exactly
// once, after target preparation and all ordinary BeforeTable checkpoints.
func runStage4AdapterNetworkTransfer(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
) ([]int, error) {
	if execution == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network execution is unavailable"),
		)
	}
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network execution context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network target writes require a lease-fenced mutation protector",
			),
		)
	}
	execution.mu.Lock()
	if execution.binding || !execution.bound || execution.started {
		execution.mu.Unlock()
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network execution is unbound, binding, or already used",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		execution.mu.Unlock()
		return nil, err
	}
	execution.started = true
	execution.mu.Unlock()
	if len(execution.ranges) == 0 {
		return make([]int, execution.tableCount), nil
	}
	callbacks := execution.callbacks(observer)
	networkRows := int64(0)
	completedRanges := 0
	for waveIndex := range execution.waves {
		wave := execution.waves[waveIndex]
		mapped, err := wave.callbacks(callbacks)
		if err != nil {
			return nil, err
		}
		result, err := RunResumableNetworkTransfer(
			ctx,
			wave.plan,
			mapped,
		)
		if err != nil {
			return nil, err
		}
		if result.HasRuntimeTuning !=
			(wave.plan.RuntimeTuning != nil) ||
			result.CompletedRanges != len(wave.global) ||
			len(result.Pagination) != len(wave.global) {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network dependency wave %d result is incomplete",
					waveIndex,
				),
			)
		}
		for localIndex, fact := range result.Pagination {
			expected := wave.plan.Ranges[localIndex]
			if fact.RangeIndex != expected.RangeIndex ||
				fact.TableSchema != expected.TableSchema ||
				fact.TableName != expected.TableName ||
				fact.TopologyHash != expected.TopologyHash ||
				fact.Pagination != expected.Pagination {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 network result changed dependency-wave range identity",
					),
				)
			}
		}
		if result.Rows > math.MaxInt64-networkRows {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network dependency-wave row total overflows",
				),
			)
		}
		networkRows += result.Rows
		completedRanges += result.CompletedRanges
	}
	if completedRanges != len(execution.ranges) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network result is incomplete"),
		)
	}
	totals, durableRows, err := durableStage4AdapterNetworkTotals(
		ctx,
		execution.coordinator,
		execution.ranges,
		execution.tableCount,
	)
	if err != nil {
		return nil, err
	}
	if durableRows != networkRows {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 durable table row totals differ from the completed network result",
			),
		)
	}
	return totals, nil
}

func (execution *stage4AdapterNetworkExecution) callbacks(
	observer TableObserver,
) NetworkTransferCallbacks {
	return NetworkTransferCallbacks{
		ReadPage: func(
			readCtx context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			binding, lookupErr := exactStage4AdapterNetworkRange(
				execution.ranges,
				request.Range,
			)
			if lookupErr != nil {
				return NetworkReadPage{}, lookupErr
			}
			return execution.reader.ReadNetworkRangePage(
				readCtx,
				binding.plan.source,
				binding.plan.columns,
				binding.work.pagination,
				binding.rangePlan,
				request,
			)
		},
		WritePage: execution.coordinator.wrapWrite(
			observer,
			func(
				writeCtx context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				return writeStage4AdapterNetworkPage(
					writeCtx,
					execution.target,
					execution.ranges,
					execution.plan.ReplayMode,
					request,
				)
			},
		),
		RecordIssued: execution.coordinator.recordIssued,
		Checkpoint:   execution.coordinator.checkpoint,
	}
}

func (execution *stage4AdapterNetworkExecution) openTable(
	ctx context.Context,
	planIndex int,
	resume bool,
	stableSessions ...*adapterStableNetworkTableSession,
) (_ *stage4AdapterNetworkTableExecution, resultErr error) {
	if execution == nil || !execution.deferred {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 deferred network execution is unavailable",
			),
		)
	}
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if planIndex < 0 || planIndex >= len(execution.prepared.plans) ||
		planIndex >= len(execution.prepared.work) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table index is outside the plan"),
		)
	}
	if len(stableSessions) > 1 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 stable table accepts at most one owned source session",
			),
		)
	}
	execution.mu.Lock()
	if execution.binding || execution.bound || execution.started {
		execution.mu.Unlock()
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 deferred network execution is already active or consumed",
			),
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
		stableSessions...,
	)
	if err != nil {
		return nil, err
	}
	// planTable hands back an open session, so the durable write below owns
	// closing it on failure exactly as the single-phase form used to.
	defer func() {
		if resultErr == nil {
			return
		}
		if closeErr := tableExecution.session.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if err := tableExecution.resetOrEnsurePlan(ctx, resume); err != nil {
		return nil, err
	}
	if err := tableExecution.bindRestoresAndValidate(ctx); err != nil {
		return nil, err
	}
	// A full local SQLite state backend records the controller session before
	// this table can reach PrepareTables. The optional sink then persists every
	// decision at its chunk boundary; YAML intentionally retains only the
	// bounded invocation report.
	if err := tableExecution.bindRuntimeTuningHistory(ctx); err != nil {
		return nil, err
	}
	execution.recordBoundWork(planIndex, tableExecution.work)
	execution.mu.Lock()
	execution.nextGlobalRange += uint64(len(tableExecution.ranges))
	execution.mu.Unlock()
	return tableExecution, nil
}

func (execution *stage4AdapterNetworkExecution) recordBoundWork(
	planIndex int,
	work stage4AdapterWork,
) {
	if execution == nil {
		return
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if len(execution.boundWork) != len(execution.prepared.work) {
		execution.boundWork = make(
			[]stage4AdapterWork,
			len(execution.prepared.work),
		)
	}
	if planIndex >= 0 && planIndex < len(execution.boundWork) {
		execution.boundWork[planIndex] = cloneStage4AdapterNetworkWork(work)
	}
}

// recordRuntimeTuningResult captures only the controller's bounded,
// credential-free status after the core returns, including an incomplete
// transfer that already applied a safety decision. The result is intentionally
// indexed by the immutable prepared plan rather than completion order, so
// serial resume/status output stays deterministic.
func (execution *stage4AdapterNetworkExecution) recordRuntimeTuningResult(
	planIndex int,
	snapshot RuntimeTuningSnapshot,
	adjustments []RuntimeTuningDecision,
) {
	if execution == nil || planIndex < 0 ||
		planIndex >= len(execution.prepared.plans) {
		return
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if len(execution.runtimeTuningReports) != len(execution.prepared.plans) {
		execution.runtimeTuningReports = make(
			[]stage4AdapterRuntimeTuningTableReport,
			len(execution.prepared.plans),
		)
	}
	execution.runtimeTuningReports[planIndex] =
		stage4AdapterRuntimeTuningTableReport{
			recorded:    true,
			snapshot:    cloneRuntimeTuningSnapshot(snapshot),
			adjustments: cloneRuntimeTuningHistory(adjustments),
		}
}

// runtimeTuningReport returns a new status value. It deliberately carries no
// page frontiers, source values, driver messages, or credentials: adjustment
// history uses only the bounded RuntimeTuningDecision identity surface.
func (execution *stage4AdapterNetworkExecution) runtimeTuningReport() *RuntimeTuningReport {
	if execution == nil || !execution.runtimeTuning {
		return nil
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	report := &RuntimeTuningReport{
		Enabled: true,
		Tables: make(
			[]RuntimeTuningTableReport,
			0,
			len(execution.runtimeTuningReports),
		),
	}
	for planIndex, entry := range execution.runtimeTuningReports {
		if !entry.recorded {
			continue
		}
		plan := execution.prepared.plans[planIndex]
		report.Tables = append(
			report.Tables,
			runtimeTuningTableReport(
				plan.source.Schema,
				plan.source.Name,
				entry.snapshot,
				entry.adjustments,
			),
		)
	}
	if len(report.Tables) == 0 {
		report.Reason = "runtime tuning was enabled, but this invocation had no uncompleted table transfer"
	}
	return report
}

func attachStage4AdapterRuntimeTuningReport(
	result *Result,
	execution *stage4AdapterNetworkExecution,
) {
	if result == nil || result.RuntimeTuning != nil {
		return
	}
	result.RuntimeTuning = execution.runtimeTuningReport()
}

func (execution *stage4AdapterNetworkExecution) snapshotBoundWork() (
	[]stage4AdapterWork,
	error,
) {
	if execution == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 bounded network execution is unavailable"),
		)
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if len(execution.boundWork) != len(execution.prepared.work) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 bounded network work is incomplete"),
		)
	}
	result := make([]stage4AdapterWork, len(execution.boundWork))
	for index, work := range execution.boundWork {
		if len(work.ranges) == 0 {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 bounded network work for %s is unavailable",
					execution.prepared.plans[index].source.Name,
				),
			)
		}
		result[index] = cloneStage4AdapterNetworkWork(work)
	}
	return result, nil
}

// planTableOnce plans one table under the same single-binding guard openTable
// uses, and advances the shared global range offset so the next table's ranges
// keep migration-global identities. The caller owns the returned session and
// commits the durable work plan later.
func (execution *stage4AdapterNetworkExecution) planTableOnce(
	ctx context.Context,
	planIndex int,
) (*stage4AdapterNetworkTableExecution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if planIndex < 0 || planIndex >= len(execution.prepared.plans) ||
		planIndex >= len(execution.prepared.work) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table index is outside the plan"),
		)
	}
	execution.mu.Lock()
	if execution.binding || execution.bound || execution.started {
		execution.mu.Unlock()
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 deferred network execution is already active or consumed",
			),
		)
	}
	execution.binding = true
	globalOffset := execution.nextGlobalRange
	execution.mu.Unlock()

	tableExecution, err := execution.planTable(ctx, planIndex, globalOffset)

	execution.mu.Lock()
	execution.binding = false
	if err == nil {
		execution.nextGlobalRange += uint64(len(tableExecution.ranges))
	}
	execution.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return tableExecution, nil
}

// planTable materializes one table's exact stable pagination plan, range
// inventory, and transfer plan without writing any durable work. Keeping the
// durable write separate lets a caller establish the complete Stage 4 table
// inventory while no table work or ordinary task exists yet, which is what
// EnsureStage4TableInventory requires. The caller owns the returned session.
func (execution *stage4AdapterNetworkExecution) planTable(
	ctx context.Context,
	planIndex int,
	globalOffset uint64,
	stableSessions ...*adapterStableNetworkTableSession,
) (_ *stage4AdapterNetworkTableExecution, resultErr error) {
	plan := cloneStage4AdapterNetworkTablePlan(
		execution.prepared.plans[planIndex],
	)
	var session *adapterStableNetworkTableSession
	if len(stableSessions) == 1 {
		session = stableSessions[0]
		if session == nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 supplied stable source session for %s is unavailable",
					plan.source.Name,
				),
			)
		}
	} else {
		var err error
		session, err = OpenAdapterStableNetworkTableSource(
			ctx,
			execution.source,
			plan.source,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"open Stage 4 stable source view for %s: %w",
				plan.source.Name,
				err,
			)
		}
	}
	defer func() {
		if resultErr == nil {
			return
		}
		if closeErr := session.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	stable, err := session.Source()
	if err != nil {
		return nil, err
	}
	if _, ok := stable.(adapterNetworkStableRangePageSource); !ok {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 table %s source lacks a stable range-read proof",
				plan.source.Name,
			),
		)
	}
	// The exact catalog recheck must use the same stable view that owns
	// pagination and row reads. A second pool connection can both deadlock
	// when the source pool is intentionally capped at one and observe catalog
	// state from a different engine snapshot.
	catalog, err := stable.InspectTable(
		ctx,
		plan.source.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"inspect Stage 4 stable source table %s: %w",
			plan.source.Name,
			err,
		)
	}
	expectedCatalog, ok := execution.prepared.sourceCatalog[stage4RichTableKey{
		schema: plan.source.Schema,
		table:  plan.source.Name,
	}]
	if !ok {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 source catalog for table %s is missing from static admission",
				plan.source.Name,
			),
		)
	}
	sameCatalog, err := stage4AdapterNetworkCatalogEqual(
		catalog,
		expectedCatalog,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"canonicalize Stage 4 stable source table %s: %w",
			plan.source.Name,
			err,
		)
	}
	if !sameCatalog {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 source schema changed before stable planning for table %s",
				plan.source.Name,
			),
		)
	}

	work := cloneStage4AdapterNetworkWork(
		execution.prepared.work[planIndex],
	)
	partitions := execution.partitions
	if execution.largeTableThreshold != 0 {
		decision, decisionErr :=
			stage4AdapterLargeTableDecisionForStableSource(
				ctx,
				stable,
				plan.source,
				execution.largeTableThreshold,
				partitions,
			)
		if decisionErr != nil {
			return nil, decisionErr
		}
		work.topology, decisionErr = stage4AdapterLargeTableTopology(
			work.topology,
			decision,
		)
		if decisionErr != nil {
			return nil, fmt.Errorf(
				"bind Stage 4 large-table topology for %s: %w",
				plan.source.Name,
				decisionErr,
			)
		}
		partitions = decision.effectivePartitions
	}
	bound, err := bindStage4AdapterPagination(
		ctx,
		stable,
		partitions,
		[]stage4AdapterWork{
			work,
		},
		[]adapterTablePlan{plan},
	)
	if err != nil {
		return nil, err
	}
	if execution.finalizeWork != nil {
		finalized, finalizeErr := execution.finalizeWork(bound[0])
		if finalizeErr != nil {
			return nil, fmt.Errorf(
				"finalize Stage 4 stable work identity for %s: %w",
				plan.source.Name,
				finalizeErr,
			)
		}
		bound[0] = finalized
	}
	coordinator, err := newStage4AdapterNetworkCoordinator(
		execution.prepared.run,
		bound,
	)
	if err != nil {
		return nil, err
	}
	localPrepared := stage4AdapterPrepared{
		run:     execution.prepared.run,
		mode:    execution.prepared.mode,
		plans:   []adapterTablePlan{plan},
		work:    bound,
		network: coordinator,
	}
	ranges, _, err := admitStage4AdapterNetworkInventory(
		localPrepared,
		execution.source.Engine(),
		true,
	)
	if err != nil {
		return nil, err
	}
	if err := bindStage4AdapterNetworkRangeTargetAuthority(
		ranges,
		execution.prepared.evolution,
		[]schema.Table{plan.target},
	); err != nil {
		return nil, err
	}
	if len(ranges) == 0 ||
		uint64(len(ranges)) >
			maximumRuntimeTuningRanges-globalOffset {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 global network range inventory is empty or unbounded",
			),
		)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		ctx,
		plan.source,
		plan.columns,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"prove Stage 4 retained row width for table %s: %w",
			plan.source.Name,
			err,
		)
	}
	if !evidence.Trustworthy ||
		evidence.CompleteColumnCount != len(plan.columns) ||
		evidence.ExpectedColumnCount != len(plan.columns) ||
		evidence.UpperBoundBytes <= 0 {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 retained row proof for table %s is incomplete",
				plan.source.Name,
			),
		)
	}
	resources, err := clampStage4AdapterNetworkReaders(
		execution.resources,
		session.ReaderLimit(),
	)
	if err != nil {
		return nil, err
	}
	if evidence.UpperBoundBytes > resources.MemoryBudget.Value {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 retained row bound for table %s exceeds the migration memory budget",
				plan.source.Name,
			),
		)
	}

	globalPlans := make([]NetworkRangePlan, len(ranges))
	localPlans := make([]NetworkRangePlan, len(ranges))
	for index := range ranges {
		globalIndex := globalOffset + uint64(index)
		ranges[index].globalIndex = globalIndex
		ranges[index].maxRowBytes = evidence.UpperBoundBytes
		globalPlans[index] = NetworkRangePlan{
			RangeIndex:   globalIndex,
			TableSchema:  plan.source.Schema,
			TableName:    plan.source.Name,
			TopologyHash: bound[0].topology,
			Pagination:   bound[0].pagination.Strategy,
			MaxRowBytes:  evidence.UpperBoundBytes,
		}
		localPlans[index] = globalPlans[index]
		localPlans[index].RangeIndex = uint64(index)
	}
	tableExecution := &stage4AdapterNetworkTableExecution{
		parent:      execution,
		session:     session,
		source:      stable,
		planIndex:   planIndex,
		work:        bound[0],
		ranges:      ranges,
		coordinator: coordinator,
		global:      globalPlans,
		corePlan: NetworkTransferPlan{
			SourceEngine:        execution.source.Engine(),
			TargetEngine:        execution.target.Engine(),
			Resources:           resources,
			RetryPolicy:         execution.retryPolicy,
			ReplayMode:          stage4AdapterNetworkReplayMode(execution.prepared.mode),
			UpsertMergeRows:     execution.upsertMergeRows,
			CheckpointFrequency: execution.checkpointFrequency,
			Ranges:              localPlans,
		},
	}
	if execution.runtimeTuning {
		controller, tuningErr := newStage4AdapterTableRuntimeTuning(
			resources,
			evidence,
			len(localPlans),
			execution.runtimeTuningInterval,
			nil,
		)
		if tuningErr != nil {
			return nil, tuningErr
		}
		tableExecution.corePlan.RuntimeTuning = controller
		tableExecution.corePlan.RowWidth = evidence
	}
	return tableExecution, nil
}
