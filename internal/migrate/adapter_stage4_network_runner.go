package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// adapterStage4NetworkUpsertTarget is the explicit route-owned proof that the
// target's ordinary "upsert" WriteBatch path is safe to replay after a durable
// issued-page record. ClickHouse deliberately does not implement this marker.
type adapterStage4NetworkUpsertTarget interface {
	targetAdapter
	stage4NetworkIdempotentUpsertTarget()
	WriteStage4NetworkBatch(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) (WriteReceipt, error)
}

func (*postgresTargetAdapter) stage4NetworkIdempotentUpsertTarget() {}
func (*mysqlTargetAdapter) stage4NetworkIdempotentUpsertTarget()    {}
func (*sqlServerTargetAdapter) stage4NetworkIdempotentUpsertTarget() {
}
func (*sqliteTargetAdapter) stage4NetworkIdempotentUpsertTarget() {}

type stage4AdapterNetworkRange struct {
	globalIndex uint64
	planIndex   int
	localIndex  int
	plan        adapterTablePlan
	work        stage4AdapterWork
	rangePlan   PaginationRange
	durable     networkStateRangeBinding
	maxRowBytes int64
}

// stage4AdapterNetworkExecution is an admitted, single-use transfer. Admission
// resolves all source reads, target replay capability, FK scheduling, retained
// row widths, resources, and global range identities before PrepareTables is
// allowed to mutate the target. Durable restores are bound after
// checkpointStage4AdapterWork has ensured the plans and before PrepareTables.
type stage4AdapterNetworkExecution struct {
	mu          sync.Mutex
	binding     bool
	bound       bool
	started     bool
	deferred    bool
	source      sourceAdapter
	reader      adapterNetworkRangePageSource
	target      adapterStage4NetworkUpsertTarget
	coordinator *networkStateCoordinator
	ranges      []stage4AdapterNetworkRange
	plan        NetworkTransferPlan
	waves       []stage4AdapterNetworkWave
	tableCount  int
	prepared    stage4AdapterPrepared
	partitions  int
	resources   config.EffectiveTransferPlan
	retryPolicy RetryPolicy

	nextGlobalRange   uint64
	finalizeWork      func(stage4AdapterWork) (stage4AdapterWork, error)
	deleteTransferred map[int]stage4AdapterPostgresDeleteTransferredTable

	// aggregate is non-nil only once this run owns a durable Stage 4 table
	// inventory, which is the proof that every table must publish its terminal
	// evidence through one atomic completion instead of the separate ordinary
	// and structured mutations. Runs that predate the inventory keep the older
	// pair so an in-flight resume is never stranded.
	aggregate state.Stage4AggregateBackend
}

// stage4AdapterNetworkWave is one table's exact range set. Tables execute in
// the already validated parent-before-child adapter plan order, while ranges
// inside a table retain bounded parallelism. The core requires local contiguous
// range indexes, so callbacks translate them back to immutable global durable
// identities at the adapter boundary.
type stage4AdapterNetworkWave struct {
	plan   NetworkTransferPlan
	global []NetworkRangePlan
}

type stage4AdapterNetworkTableExecution struct {
	parent      *stage4AdapterNetworkExecution
	session     *adapterStableNetworkTableSession
	source      adapterStableNetworkSource
	planIndex   int
	work        stage4AdapterWork
	ranges      []stage4AdapterNetworkRange
	coordinator *networkStateCoordinator
	corePlan    NetworkTransferPlan
	global      []NetworkRangePlan
}

type stage4AdapterNetworkAdmissionOptions struct {
	strictSnapshotComposition       bool
	deleteReconciliationComposition bool
}

type stage4AdapterNetworkAdmissionOption func(
	*stage4AdapterNetworkAdmissionOptions,
)

func withStage4StrictSnapshotComposition() stage4AdapterNetworkAdmissionOption {
	return func(options *stage4AdapterNetworkAdmissionOptions) {
		options.strictSnapshotComposition = true
	}
}

func withStage4DeleteReconciliationComposition() stage4AdapterNetworkAdmissionOption {
	return func(options *stage4AdapterNetworkAdmissionOptions) {
		options.deleteReconciliationComposition = true
	}
}

// admitStage4AdapterNetworkTransfer is read-only. The caller invokes it before
// checkpointing or target.PrepareTables, then passes the returned execution to
// runStage4AdapterNetworkTransfer after all ordinary BeforeTable callbacks.
// A non-nil resource override supports deterministic tests and a route that
// already resolved immutable resources.
func admitStage4AdapterNetworkTransfer(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	resourceOverride *config.EffectiveTransferPlan,
	optionValues ...stage4AdapterNetworkAdmissionOption,
) (*stage4AdapterNetworkExecution, error) {
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network admission context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := stage4AdapterNetworkAdmissionOptions{}
	for _, option := range optionValues {
		if option == nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network admission option is unavailable",
				),
			)
		}
		option(&options)
	}
	if err := requireStage4AdapterNetworkMode(
		cfg,
		prepared,
		options,
	); err != nil {
		return nil, err
	}
	if err := prepared.run.Validate(); err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("validate Stage 4 network run context: %w", err),
		)
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
	upsertTarget, ok := target.(adapterStage4NetworkUpsertTarget)
	if !ok || isNilInterface(upsertTarget) {
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified idempotent network upsert path",
				engine,
			),
		)
	}
	if source.Engine() == "clickhouse" ||
		upsertTarget.Engine() == "clickhouse" {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 analytical ClickHouse transfer requires its dedicated bounded runner",
			),
		)
	}
	if !stage4AdapterNetworkRelationalEngine(source.Engine()) ||
		!stage4AdapterNetworkRelationalEngine(upsertTarget.Engine()) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network route %s-to-%s is unsupported",
				source.Engine(),
				upsertTarget.Engine(),
			),
		)
	}
	targetTables := make(
		[]schema.Table,
		len(prepared.plans),
	)
	for index := range prepared.plans {
		targetTables[index] =
			cloneStage4RichTable(prepared.plans[index].target)
	}
	if prepared.evolution != nil {
		existingTargetTables, existingErr :=
			stage4AdapterExistingEvolutionTargetTables(
				prepared.evolution,
				targetTables,
			)
		if existingErr != nil {
			return nil, existingErr
		}
		targetTables = existingTargetTables
	}
	if err := preflightStage4NetworkReplayIsolation(
		ctx,
		upsertTarget,
		targetTables,
	); err != nil {
		return nil, err
	}

	resources, err := stage4AdapterNetworkResources(
		ctx,
		cfg,
		source.Engine(),
		upsertTarget.Engine(),
		resourceOverride,
	)
	if err != nil {
		return nil, err
	}
	if options.strictSnapshotComposition {
		resources, err =
			reserveStage4AdapterStrictSnapshotOwner(resources)
		if err != nil {
			return nil, err
		}
	}
	if err := validateStage4AdapterNetworkDependencyOrder(
		prepared.plans,
	); err != nil {
		return nil, err
	}
	retryPolicy := DefaultRetryPolicy()
	retryPolicy.MaxRetries = cfg.Migration.MaxRetries

	// Production relational admission is intentionally static. Pagination,
	// retained width, durable topology, and the range reader are bound later
	// from one table-scoped stable source view. Older direct runner fixtures can
	// still supply an already-bound coordinator to exercise the core in
	// isolation; migrate/resume never use that compatibility path.
	if prepared.network == nil {
		if err := validateStage4AdapterDeferredNetworkInventory(
			prepared,
		); err != nil {
			return nil, err
		}
		if !canOpenAdapterStableNetworkTableSource(source) {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 source engine %q has no table-stable network lifecycle",
					source.Engine(),
				),
			)
		}
		return &stage4AdapterNetworkExecution{
			deferred:    true,
			source:      source,
			target:      upsertTarget,
			tableCount:  len(prepared.plans),
			prepared:    cloneStage4AdapterNetworkPrepared(prepared),
			partitions:  cfg.Migration.Partitions,
			resources:   resources,
			retryPolicy: retryPolicy,
		}, nil
	}

	ranges, bindings, err := admitStage4AdapterNetworkInventory(
		prepared,
		source.Engine(),
		false,
	)
	if err != nil {
		return nil, err
	}
	if err := bindStage4AdapterNetworkRangeTargetAuthority(
		ranges,
		prepared.evolution,
		prepared.targetTables,
	); err != nil {
		return nil, err
	}
	reader, err := requireAdapterNetworkRangePageSource(source)
	if err != nil {
		return nil, fmt.Errorf(
			"admit Stage 4 bounded network source reader: %w",
			err,
		)
	}
	if len(ranges) == 0 {
		return &stage4AdapterNetworkExecution{
			reader:     reader,
			target:     upsertTarget,
			tableCount: len(prepared.plans),
		}, nil
	}

	widths := make([]int64, len(prepared.plans))
	for index, plan := range prepared.plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		evidence, proofErr := planAdapterSourceRetainedRowWidth(
			ctx,
			source,
			plan.source,
			plan.columns,
		)
		if proofErr != nil {
			return nil, fmt.Errorf(
				"prove Stage 4 retained row width for table %s: %w",
				plan.source.Name,
				proofErr,
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
		if evidence.UpperBoundBytes > resources.MemoryBudget.Value {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 retained row bound for table %s exceeds the migration memory budget",
					plan.source.Name,
				),
			)
		}
		widths[index] = evidence.UpperBoundBytes
	}
	for index := range ranges {
		ranges[index].maxRowBytes = widths[ranges[index].planIndex]
	}

	// Range completion must remain distinct from ordinary AfterTable and final
	// schema publication. Reconstruct from the already-admitted exact bindings
	// with immutable deferred task completion.
	coordinator, err := newNetworkStateCoordinator(
		prepared.run,
		bindings,
		withDeferredNetworkTaskCompletion(),
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("construct deferred Stage 4 network coordinator: %w", err),
		)
	}

	transferRanges := make([]NetworkRangePlan, len(ranges))
	for index, binding := range ranges {
		transferRanges[index] = NetworkRangePlan{
			RangeIndex:   binding.globalIndex,
			TableSchema:  binding.plan.source.Schema,
			TableName:    binding.plan.source.Name,
			TopologyHash: binding.work.topology,
			Pagination:   binding.work.pagination.Strategy,
			MaxRowBytes:  binding.maxRowBytes,
		}
	}
	return &stage4AdapterNetworkExecution{
		reader:      reader,
		target:      upsertTarget,
		coordinator: coordinator,
		ranges:      ranges,
		tableCount:  len(prepared.plans),
		plan: NetworkTransferPlan{
			SourceEngine: source.Engine(),
			TargetEngine: upsertTarget.Engine(),
			Resources:    resources,
			RetryPolicy:  retryPolicy,
			ReplayMode:   NetworkReplayIdempotentUpsert,
			Ranges:       transferRanges,
		},
	}, nil
}

// bindStage4AdapterNetworkRestoresAndValidate runs after the ordinary network
// plans are checkpointed but before PrepareTables. It freezes the exact durable
// restore set and proves the complete core plan without executing callbacks.
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
		if result.HasRuntimeTuning ||
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
	execution.mu.Lock()
	execution.nextGlobalRange += uint64(len(tableExecution.ranges))
	execution.mu.Unlock()
	return tableExecution, nil
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

	bound, err := bindStage4AdapterPagination(
		ctx,
		stable,
		execution.partitions,
		[]stage4AdapterWork{
			cloneStage4AdapterNetworkWork(
				execution.prepared.work[planIndex],
			),
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
			SourceEngine: execution.source.Engine(),
			TargetEngine: execution.target.Engine(),
			Resources:    resources,
			RetryPolicy:  execution.retryPolicy,
			ReplayMode:   NetworkReplayIdempotentUpsert,
			Ranges:       localPlans,
		},
	}
	return tableExecution, nil
}

func stage4AdapterNetworkCatalogEqual(
	actual schema.Table,
	expected schema.Table,
) (bool, error) {
	actualSnapshot, err := schema.NewSchemaSnapshot(
		[]schema.Table{actual},
	)
	if err != nil {
		return false, err
	}
	expectedSnapshot, err := schema.NewSchemaSnapshot(
		[]schema.Table{expected},
	)
	if err != nil {
		return false, err
	}
	actualCanonical, err := actualSnapshot.CanonicalJSON()
	if err != nil {
		return false, err
	}
	expectedCanonical, err := expectedSnapshot.CanonicalJSON()
	if err != nil {
		return false, err
	}
	return string(actualCanonical) == string(expectedCanonical), nil
}

func clampStage4AdapterNetworkReaders(
	resources config.EffectiveTransferPlan,
	limit int,
) (config.EffectiveTransferPlan, error) {
	if limit < 1 {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 stable source reader limit is invalid"),
		)
	}
	if resources.Readers.Value > limit {
		resources.Readers = config.EffectiveInt{
			Value:      limit,
			Provenance: config.ProvenanceSafetyClamped,
		}
	}
	if resources.Workers.Value <
		resources.Readers.Value+resources.Writers.Value ||
		resources.ConnectionLimit.Value <
			resources.Readers.Value+resources.Writers.Value {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 stable source resources cannot schedule admitted readers and writers",
			),
		)
	}
	return resources, nil
}

// reserveStage4AdapterStrictSnapshotOwner keeps the PostgreSQL snapshot
// exporter inside the same admitted connection envelope as imported readers
// and writers. Prefer retaining reader parallelism because independent
// imported readers are the strict route's source-side concurrency mechanism.
func reserveStage4AdapterStrictSnapshotOwner(
	resources config.EffectiveTransferPlan,
) (config.EffectiveTransferPlan, error) {
	if resources.ConnectionLimit.Value >=
		resources.Readers.Value+resources.Writers.Value+1 {
		return resources, nil
	}
	switch {
	case resources.Writers.Value > 1:
		resources.Writers = config.EffectiveInt{
			Value:      resources.Writers.Value - 1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	case resources.Readers.Value > 1:
		resources.Readers = config.EffectiveInt{
			Value:      resources.Readers.Value - 1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	default:
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency requires connection_limit of at least 3 for one snapshot owner, one reader, and one writer",
			),
		)
	}
	if resources.ConnectionLimit.Value <
		resources.Readers.Value+resources.Writers.Value+1 {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency cannot reserve its snapshot owner inside the admitted connection limit",
			),
		)
	}
	return resources, nil
}

func (execution *stage4AdapterNetworkTableExecution) resetOrEnsurePlan(
	ctx context.Context,
	resume bool,
) error {
	if execution == nil || execution.coordinator == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 table network coordinator is unavailable"),
		)
	}
	if !resume {
		if err := execution.coordinator.ensurePlans(ctx); err != nil {
			return err
		}
		return nil
	}
	inventory, err := loadStage4WorkInventory(
		ctx,
		execution.parent.prepared.run,
	)
	if err != nil {
		return err
	}
	existing, found := inventory.tasks[execution.work.task]
	if !found {
		if err := execution.coordinator.ensurePlans(ctx); err != nil {
			return err
		}
		return nil
	}
	switch existing.Status {
	case "running", "completed":
	default:
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 incomplete table %s has unsafe durable network task status %q",
				execution.work.task.Table,
				existing.Status,
			),
		)
	}
	task := state.WorkTask{
		RunID:        execution.parent.prepared.run.RunID,
		Key:          execution.work.task,
		Strategy:     execution.work.strategy,
		TopologyHash: execution.work.topology,
		StartedAt:    time.Now().UTC(),
	}
	ranges := make([]state.RangeState, len(execution.work.ranges))
	for index := range execution.work.ranges {
		ranges[index] = cloneInitialNetworkStateRange(
			execution.work.ranges[index],
		)
	}
	if err := execution.parent.prepared.run.Backend.ResetWorkPlan(
		task,
		ranges,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reset stale Stage 4 network plan for table %s before replay: %w",
				execution.work.task.Table,
				err,
			),
		)
	}
	return ctx.Err()
}

func (execution *stage4AdapterNetworkTableExecution) bindRestoresAndValidate(
	ctx context.Context,
) error {
	restores, err := execution.coordinator.loadRestores(ctx)
	if err != nil {
		return fmt.Errorf(
			"load Stage 4 durable network restores for %s: %w",
			execution.work.task.Table,
			err,
		)
	}
	execution.corePlan.Restores = make(
		[]NetworkRangeRestore,
		len(restores),
	)
	for index := range restores {
		execution.corePlan.Restores[index] =
			cloneNetworkRestore(restores[index])
	}
	if _, err := validateNetworkTransferPlan(
		execution.corePlan,
		execution.callbacks(nil),
	); err != nil {
		return fmt.Errorf(
			"validate Stage 4 stable table execution for %s: %w",
			execution.work.task.Table,
			err,
		)
	}
	return nil
}

func (execution *stage4AdapterNetworkTableExecution) callbacks(
	observer TableObserver,
) NetworkTransferCallbacks {
	globalRequest := func(
		local NetworkRangePlan,
	) (NetworkRangePlan, error) {
		if local.RangeIndex >= uint64(len(execution.global)) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 table callback references an unknown local range",
				),
			)
		}
		expected := execution.corePlan.Ranges[local.RangeIndex]
		if !reflect.DeepEqual(local, expected) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 table callback range differs from immutable admission",
				),
			)
		}
		return execution.global[local.RangeIndex], nil
	}
	write := execution.coordinator.wrapWrite(
		observer,
		func(
			writeCtx context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			global, err := globalRequest(request.Range)
			if err != nil {
				return networkStateFailedReceipt(request), err
			}
			request.Range = global
			return writeStage4AdapterNetworkPage(
				writeCtx,
				execution.parent.target,
				execution.ranges,
				request,
			)
		},
	)
	return NetworkTransferCallbacks{
		ReadPage: func(
			readCtx context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			global, err := globalRequest(request.Range)
			if err != nil {
				return NetworkReadPage{}, err
			}
			localIndex := request.Range.RangeIndex
			request.Range = global
			if request.ReplayExpected != nil {
				if request.ReplayExpected.RangeIndex != localIndex {
					return NetworkReadPage{}, NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 table replay identity changed",
						),
					)
				}
				replay := *request.ReplayExpected
				replay.RangeIndex = global.RangeIndex
				request.ReplayExpected = &replay
			}
			binding, err := exactStage4AdapterNetworkRange(
				execution.ranges,
				global,
			)
			if err != nil {
				return NetworkReadPage{}, err
			}
			return execution.source.ReadNetworkRangePage(
				readCtx,
				binding.plan.source,
				binding.plan.columns,
				binding.work.pagination,
				binding.rangePlan,
				request,
			)
		},
		WritePage:    write,
		RecordIssued: execution.coordinator.recordIssued,
		Checkpoint:   execution.coordinator.checkpoint,
	}
}

func (execution *stage4AdapterNetworkTableExecution) run(
	ctx context.Context,
	observer TableObserver,
) (int, error) {
	if execution == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table execution is unavailable"),
		)
	}
	result, err := RunResumableNetworkTransfer(
		ctx,
		execution.corePlan,
		execution.callbacks(observer),
	)
	if err != nil {
		return 0, err
	}
	if result.HasRuntimeTuning ||
		result.CompletedRanges != len(execution.ranges) ||
		len(result.Pagination) != len(execution.ranges) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 stable table network result is incomplete",
			),
		)
	}
	totals, durableRows, err := durableStage4AdapterNetworkTotals(
		ctx,
		execution.coordinator,
		execution.ranges,
		1,
	)
	if err != nil {
		return 0, err
	}
	if len(totals) != 1 || durableRows != result.Rows {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 stable table durable rows differ from the network result",
			),
		)
	}
	return totals[0], nil
}

func bindStage4AdapterNetworkRangeTargetAuthority(
	ranges []stage4AdapterNetworkRange,
	evolution *stage4AdapterTargetSchemaEvolution,
	transfer []schema.Table,
) error {
	if evolution == nil {
		return nil
	}
	current, err := stage4AdapterCurrentEvolutionTargetTables(
		evolution,
		transfer,
	)
	if err != nil {
		return err
	}
	byIdentity := make(
		map[targetSchemaEvolutionTableKey]schema.Table,
		len(current),
	)
	for _, table := range current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		byIdentity[key] = cloneStage4RichTable(table)
	}
	for index := range ranges {
		key := targetSchemaEvolutionTableKey{
			schema: ranges[index].plan.target.Schema,
			table:  ranges[index].plan.target.Name,
		}
		authenticated, found := byIdentity[key]
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network range target %s.%s is missing from the authenticated current target shape",
					key.schema,
					key.table,
				),
			)
		}
		ranges[index].plan.target =
			cloneStage4RichTable(authenticated)
	}
	return nil
}

func (execution *stage4AdapterNetworkTableExecution) Close() error {
	if execution == nil || execution.session == nil {
		return nil
	}
	return execution.session.Close()
}

func (execution *stage4AdapterNetworkExecution) advanceCompletedTable(
	ctx context.Context,
	planIndex int,
	checkpointRows int,
) error {
	_, err := execution.validateCompletedTable(
		ctx,
		planIndex,
		checkpointRows,
		true,
	)
	return err
}

func (execution *stage4AdapterNetworkExecution) validateCompletedTable(
	ctx context.Context,
	planIndex int,
	checkpointRows int,
	advance bool,
) (uint64, error) {
	if execution == nil || !execution.deferred ||
		planIndex < 0 || planIndex >= len(execution.prepared.work) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed network table inventory is unavailable",
			),
		)
	}
	inventory, err := loadStage4WorkInventory(
		ctx,
		execution.prepared.run,
	)
	if err != nil {
		return 0, err
	}
	if err := execution.validateDurableTableTaskInventory(
		inventory,
	); err != nil {
		return 0, err
	}
	base := execution.prepared.work[planIndex]
	task, found := inventory.tasks[base.task]
	if !found || task.Status != "completed" ||
		task.RunID != execution.prepared.run.RunID ||
		task.Error != "" ||
		task.StartedAt.IsZero() ||
		task.UpdatedAt.IsZero() ||
		task.CompletedAt.IsZero() ||
		!task.UpdatedAt.Equal(task.CompletedAt) ||
		task.CompletedAt.Before(task.StartedAt) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s lacks exact durable network work",
				base.task.Table,
			),
		)
	}
	item, err := execution.reconstructCompletedTableWork(
		planIndex,
		inventory,
	)
	if err != nil {
		return 0, err
	}
	task, ranges, _, err := exactStage4AdapterWork(
		inventory,
		item,
		false,
	)
	if err != nil {
		return 0, err
	}
	if task.Status != "completed" {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has incomplete durable work",
				item.task.Table,
			),
		)
	}

	var rows int64
	for index, workRange := range ranges {
		restore, restoreErr := networkRestoreFromState(
			networkStateRangeBinding{
				RangeIndex: uint64(index),
				Task:       item.task,
				Initial:    item.ranges[index],
			},
			workRange,
		)
		if restoreErr != nil ||
			workRange.Status != "completed" ||
			workRange.RunID != execution.prepared.run.RunID ||
			workRange.Task != item.task ||
			workRange.Error != "" ||
			workRange.CommittedPrefix != 0 ||
			workRange.UpdatedAt.IsZero() ||
			workRange.CompletedAt.IsZero() ||
			!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
			workRange.CompletedAt.Before(task.StartedAt) ||
			workRange.CompletedAt.After(task.CompletedAt) ||
			!restore.Complete ||
			restore.SequenceOffset != 0 ||
			len(restore.Issued) != 0 ||
			workRange.RowsDone > math.MaxInt64-rows {
			if restoreErr == nil {
				restoreErr = fmt.Errorf(
					"completed range retains incomplete or overflowing work",
				)
			}
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s has invalid durable range evidence: %w",
					item.task.Table,
					restoreErr,
				),
			)
		}
		if err := validateCompletedStage4AdapterRange(
			task,
			item,
			index,
			workRange,
			restore,
		); err != nil {
			return 0, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q has invalid strategy evidence: %w",
					item.task.Table,
					workRange.ID,
					err,
				),
			)
		}
		rows += workRange.RowsDone
	}
	if checkpointRows < 0 || rows != int64(checkpointRows) {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s checkpoint differs from durable ranges",
				item.task.Table,
			),
		)
	}
	rangeCount := uint64(len(ranges))
	if !advance {
		return rangeCount, nil
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if rangeCount >
		maximumRuntimeTuningRanges-execution.nextGlobalRange {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 global network range inventory is unbounded"),
		)
	}
	execution.nextGlobalRange += rangeCount
	return rangeCount, nil
}

func validateCompletedStage4AdapterRange(
	task state.WorkTask,
	item stage4AdapterWork,
	rangeIndex int,
	workRange state.RangeState,
	restore NetworkRangeRestore,
) error {
	if rangeIndex < 0 ||
		rangeIndex >= len(item.pagination.Ranges) {
		return fmt.Errorf("completed range is outside pagination")
	}
	planned := item.pagination.Ranges[rangeIndex]
	if planned.Empty {
		if workRange.RowsDone != 0 ||
			workRange.NextSequence != 0 ||
			workRange.SequenceOffset != 0 ||
			workRange.CommittedPrefix != 0 ||
			workRange.FrontierValid ||
			len(workRange.Frontier) != 0 ||
			len(workRange.Pending) != 0 ||
			workRange.Attempts != 0 ||
			workRange.Retries != 0 ||
			task.Attempts != 0 ||
			task.Retries != 0 ||
			len(restore.Frontier) != 0 {
			return fmt.Errorf(
				"empty pagination range retains progress evidence",
			)
		}
		return nil
	}

	switch item.pagination.Strategy {
	case PaginationRowNumber:
		if planned.FirstRow < 1 ||
			planned.LastRow < planned.FirstRow {
			return fmt.Errorf(
				"ROW_NUMBER pagination interval is invalid",
			)
		}
		span := planned.LastRow - planned.FirstRow
		if span < 0 ||
			span == math.MaxInt64 {
			return fmt.Errorf(
				"ROW_NUMBER pagination span overflows",
			)
		}
		expectedRows := span + 1
		if workRange.RowsDone != expectedRows ||
			!workRange.FrontierValid ||
			len(workRange.Frontier) != 1 ||
			workRange.Frontier[0] !=
				state.Int64Value(planned.LastRow) {
			return fmt.Errorf(
				"ROW_NUMBER completion differs from its exact interval",
			)
		}
		return nil
	case PaginationIntegerKeyset, PaginationTupleKeyset:
		if workRange.RowsDone == 0 {
			if workRange.NextSequence != 0 ||
				workRange.FrontierValid ||
				len(workRange.Frontier) != 0 ||
				len(restore.Frontier) != 0 ||
				workRange.Attempts != 0 ||
				workRange.Retries != 0 {
				return fmt.Errorf(
					"empty keyset result retains progress evidence",
				)
			}
			return nil
		}
		frontier, err := stage4AdapterKeyTupleFromState(
			workRange.Frontier,
		)
		if err != nil || frontier == nil ||
			!workRange.FrontierValid {
			if err == nil {
				err = fmt.Errorf("completed keyset frontier is missing")
			}
			return err
		}
		if err := validateStage4AdapterKeyTuple(
			frontier,
			item.pagination.Keys,
		); err != nil {
			return err
		}
		if planned.Lower != nil &&
			!adapterPaginationKeyTupleAfter(
				*frontier,
				*planned.Lower,
			) {
			return fmt.Errorf(
				"completed keyset frontier does not follow its lower bound",
			)
		}
		if planned.Upper != nil &&
			adapterPaginationKeyTupleAfter(
				*frontier,
				*planned.Upper,
			) {
			return fmt.Errorf(
				"completed keyset frontier exceeds its upper bound",
			)
		}
		return nil
	default:
		return fmt.Errorf(
			"completed pagination strategy %q is unsupported",
			item.pagination.Strategy,
		)
	}
}

func (
	execution *stage4AdapterNetworkExecution,
) prevalidateCompletedTables(
	ctx context.Context,
	completed map[string]int,
) error {
	if err := execution.validateDurableTableTaskSet(ctx); err != nil {
		return err
	}
	execution.mu.Lock()
	rangeCount := execution.nextGlobalRange
	execution.mu.Unlock()
	for planIndex, plan := range execution.prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		tableRangeCount, err := execution.validateCompletedTable(
			ctx,
			planIndex,
			rows,
			false,
		)
		if err != nil {
			return err
		}
		if tableRangeCount >
			maximumRuntimeTuningRanges-rangeCount {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 completed global network range inventory is unbounded",
				),
			)
		}
		rangeCount += tableRangeCount
	}
	return nil
}

func (
	execution *stage4AdapterNetworkExecution,
) validateDurableTableTaskSet(ctx context.Context) error {
	if execution == nil || !execution.deferred {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 durable table-task inventory is unavailable",
			),
		)
	}
	inventory, err := loadStage4WorkInventory(
		ctx,
		execution.prepared.run,
	)
	if err != nil {
		return err
	}
	return execution.validateDurableTableTaskInventory(inventory)
}

func (
	execution *stage4AdapterNetworkExecution,
) validateDurableTableTaskInventory(
	inventory stage4WorkInventory,
) error {
	expected := make(
		map[state.TaskKey]struct{},
		len(execution.prepared.work),
	)
	for _, item := range execution.prepared.work {
		expected[item.task] = struct{}{}
	}
	for key := range inventory.tasks {
		if !stage4AdapterDurableTableTaskType(key.Type) {
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
		if len(ranges) == 0 ||
			!stage4AdapterDurableTableTaskType(key.Type) {
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
		if _, taskFound := inventory.tasks[key]; !taskFound {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"orphaned Stage 4 table work ranges exist for task %#v before resume",
					key,
				),
			)
		}
	}
	return nil
}

func stage4AdapterDurableTableTaskType(value string) bool {
	switch value {
	case stage4AdapterNetworkTaskType,
		"table-copy",
		"analytical-table-copy":
		return true
	default:
		return false
	}
}

func (
	execution *stage4AdapterNetworkExecution,
) reconstructCompletedTableWork(
	planIndex int,
	inventory stage4WorkInventory,
) (stage4AdapterWork, error) {
	base := cloneStage4AdapterNetworkWork(
		execution.prepared.work[planIndex],
	)
	table := cloneStage4RichTable(
		execution.prepared.plans[planIndex].source,
	)
	persisted := inventory.ranges[base.task]
	if len(persisted) == 0 ||
		uint64(len(persisted)) > maximumRuntimeTuningRanges {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has no bounded durable ranges",
				base.task.Table,
			),
		)
	}
	requestedPartitions := execution.partitions
	if requestedPartitions == 0 {
		requestedPartitions = config.DefaultPartitions
	}
	if requestedPartitions < 1 ||
		uint64(requestedPartitions) > maximumRuntimeTuningRanges {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has an invalid partition count",
				base.task.Table,
			),
		)
	}

	byID := make(map[string]state.RangeState, len(persisted))
	for _, workRange := range persisted {
		if _, duplicate := byID[workRange.ID]; duplicate {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s duplicates durable range %q",
					base.task.Table,
					workRange.ID,
				),
			)
		}
		byID[workRange.ID] = workRange
	}

	keyColumns, err := adapterPaginationPrimaryKey(
		execution.source.Engine(),
		table.Schema,
		table,
	)
	if err != nil {
		return stage4AdapterWork{}, err
	}
	keys := make([]KeySpec, len(keyColumns))
	evidence := make(
		[]adapterPaginationKeyEvidence,
		len(keyColumns),
	)
	for index, column := range keyColumns {
		keys[index] = KeySpec{
			Name: column.Name,
			Kind: adapterPaginationKeyKind(
				execution.source.Engine(),
				column,
			),
		}
		evidence[index] = adapterPaginationKeyEvidence{
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
			Position: column.PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(
				column.DeclaredType,
			),
		}
	}
	strategy := adapterPaginationStrategy(
		execution.source.Engine(),
		table,
		keyColumns,
	)
	plannedRanges := make(
		[]PaginationRange,
		len(persisted),
	)
	for index := range plannedRanges {
		rangeID := fmt.Sprintf("range/%d", index)
		workRange, ok := byID[rangeID]
		if !ok {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s lacks exact durable range %q",
					base.task.Table,
					rangeID,
				),
			)
		}
		delete(byID, rangeID)
		lower, tupleErr := stage4AdapterKeyTupleFromState(
			workRange.Lower,
		)
		if tupleErr != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q lower bound: %w",
					base.task.Table,
					rangeID,
					tupleErr,
				),
			)
		}
		upper, tupleErr := stage4AdapterKeyTupleFromState(
			workRange.Upper,
		)
		if tupleErr != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q upper bound: %w",
					base.task.Table,
					rangeID,
					tupleErr,
				),
			)
		}
		plannedRanges[index] = PaginationRange{
			ID:       index,
			Lower:    lower,
			Upper:    upper,
			FirstRow: workRange.FirstRow,
			LastRow:  workRange.LastRow,
			Empty: len(persisted) == 1 &&
				lower == nil && upper == nil &&
				(strategy == PaginationRowNumber &&
					workRange.FirstRow == 1 &&
					workRange.LastRow == 0 ||
					strategy != PaginationRowNumber &&
						workRange.FirstRow == 0 &&
						workRange.LastRow == 0),
		}
	}
	if len(byID) != 0 {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s contains unexpected durable ranges",
				base.task.Table,
			),
		)
	}

	pagination := PaginationPlan{
		Strategy: strategy,
		Keys:     keys,
		Ranges:   plannedRanges,
	}
	pagination.TopologyHash, err = adapterPaginationTopologyHash(
		execution.source.Engine(),
		table,
		requestedPartitions,
		evidence,
		pagination,
	)
	if err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reconstruct Stage 4 completed pagination for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	if err := validateStage4AdapterPagination(
		execution.source.Engine(),
		table,
		requestedPartitions,
		pagination,
	); err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"validate Stage 4 completed pagination for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	base.topology, err = stage4AdapterNetworkTopology(
		base.topology,
		requestedPartitions,
		pagination,
	)
	if err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reconstruct Stage 4 completed topology for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	base.pagination = cloneStage4AdapterPagination(pagination)
	base.ranges = make([]state.RangeState, len(plannedRanges))
	for index, planned := range plannedRanges {
		base.ranges[index], err = stage4AdapterStateRange(
			planned,
			base.topology,
		)
		if err != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"reconstruct Stage 4 completed range %d for %s: %w",
					index,
					base.task.Table,
					err,
				),
			)
		}
	}
	if execution.finalizeWork != nil {
		base, err = execution.finalizeWork(base)
		if err != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"restore Stage 4 completed strict topology for %s: %w",
					base.task.Table,
					err,
				),
			)
		}
	}
	return base, nil
}

func stage4AdapterKeyTupleFromState(
	tuple state.TypedTuple,
) (*KeyTuple, error) {
	if len(tuple) == 0 {
		return nil, nil
	}
	if err := tuple.Validate(false); err != nil {
		return nil, err
	}
	result := make(KeyTuple, len(tuple))
	for index, value := range tuple {
		switch value.Kind {
		case state.ValueInt64:
			result[index] = KeyValue{
				Kind:    KeyInteger,
				Encoded: value.Encoded,
			}
		case state.ValueText:
			result[index] = KeyValue{
				Kind:    KeyText,
				Encoded: value.Encoded,
			}
		case state.ValueBytes:
			result[index] = KeyValue{
				Kind:    KeyBytes,
				Encoded: value.Encoded,
			}
		default:
			return nil, fmt.Errorf(
				"typed key %d has unsupported kind %q",
				index,
				value.Kind,
			)
		}
	}
	return &result, nil
}

func requireStage4AdapterNetworkMode(
	cfg config.Config,
	prepared stage4AdapterPrepared,
	options stage4AdapterNetworkAdmissionOptions,
) error {
	if cfg.Migration.TargetMode != "upsert" ||
		prepared.mode != "upsert" {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network runner currently requires target_mode upsert",
			),
		)
	}
	if cfg.Migration.StrictConsistency &&
		!options.strictSnapshotComposition {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency requires a composed network snapshot runner",
			),
		)
	}
	if !cfg.Migration.StrictConsistency &&
		options.strictSnapshotComposition {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 strict network snapshot composition was enabled without strict consistency",
			),
		)
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 incremental windows require a composed network incremental runner",
			),
		)
	}
	deleteEnabled := false
	switch cfg.Migration.Deletes.Mode {
	case "", config.DeleteModeOff:
	case config.DeleteModeReconcile:
		deleteEnabled = true
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network runner received unsupported delete mode %q",
				cfg.Migration.Deletes.Mode,
			),
		)
	}
	if deleteEnabled !=
		options.deleteReconciliationComposition {
		class := ErrorClassPolicy
		message := "Stage 4 delete reconciliation requires a composed network delete runner"
		if !deleteEnabled {
			class = ErrorClassState
			message = "Stage 4 network delete composition was enabled without delete reconciliation"
		}
		return NewTransferError(
			class,
			fmt.Errorf("%s", message),
		)
	}
	if deleteEnabled != (prepared.deletes != nil) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 prepared delete reconciliation differs from network admission",
			),
		)
	}
	if cfg.Migration.RuntimeTuning {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 runtime tuning requires a composed network chunk-boundary controller",
			),
		)
	}
	if cfg.Migration.MaxRetries < 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 max retries must not be negative"),
		)
	}
	return nil
}

func canOpenAdapterStableNetworkTableSource(source sourceAdapter) bool {
	if isNilInterface(source) {
		return false
	}
	if _, ok := source.(adapterStableNetworkTableSessionOpener); ok {
		return true
	}
	switch source.(type) {
	case *relationalSourceAdapter, *sqliteSourceAdapter:
		return true
	default:
		return false
	}
}

func validateStage4AdapterDeferredNetworkInventory(
	prepared stage4AdapterPrepared,
) error {
	if prepared.network != nil ||
		len(prepared.work) != len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 deferred network inventory is inconsistent",
			),
		)
	}
	for index, item := range prepared.work {
		plan := prepared.plans[index]
		if item.task.Schema != plan.source.Schema ||
			item.task.Table != plan.source.Name ||
			item.task.Type != stage4AdapterNetworkTaskType ||
			item.strategy != stage4AdapterCopyStrategy ||
			!validNetworkFactToken(item.topology) ||
			len(item.ranges) != 0 ||
			!reflect.DeepEqual(item.pagination, PaginationPlan{}) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 deferred network work differs from table plan %d",
					index,
				),
			)
		}
	}
	return nil
}

func cloneStage4AdapterNetworkPrepared(
	value stage4AdapterPrepared,
) stage4AdapterPrepared {
	result := value
	result.names = append([]string(nil), value.names...)
	result.targetTables = make([]schema.Table, len(value.targetTables))
	for index := range value.targetTables {
		result.targetTables[index] =
			cloneStage4RichTable(value.targetTables[index])
	}
	result.plans = make([]adapterTablePlan, len(value.plans))
	for index := range value.plans {
		result.plans[index] =
			cloneStage4AdapterNetworkTablePlan(value.plans[index])
	}
	result.work = make([]stage4AdapterWork, len(value.work))
	for index := range value.work {
		result.work[index] =
			cloneStage4AdapterNetworkWork(value.work[index])
	}
	result.sourceCatalog = make(
		map[stage4RichTableKey]schema.Table,
		len(value.sourceCatalog),
	)
	for key, table := range value.sourceCatalog {
		result.sourceCatalog[key] = cloneStage4RichTable(table)
	}
	if value.validationPrimaryKeyEqualityProofs != nil {
		result.validationPrimaryKeyEqualityProofs = make(
			map[stage4RichTableKey]string,
			len(value.validationPrimaryKeyEqualityProofs),
		)
		for key, proof := range value.validationPrimaryKeyEqualityProofs {
			result.validationPrimaryKeyEqualityProofs[key] = proof
		}
	}
	return result
}

func stage4AdapterNetworkRelationalEngine(engine string) bool {
	switch engine {
	case "postgres", "mysql", "mariadb", "mssql", "sqlite":
		return true
	default:
		return false
	}
}

func stage4AdapterNetworkResources(
	ctx context.Context,
	cfg config.Config,
	sourceEngine string,
	targetEngine string,
	override *config.EffectiveTransferPlan,
) (config.EffectiveTransferPlan, error) {
	var (
		resources config.EffectiveTransferPlan
		err       error
	)
	if override == nil {
		resources, err = config.ResolveSystemEffectiveTransferPlan(
			ctx,
			cfg.Migration,
			config.TransferPlanOptions{},
		)
		if err != nil {
			return resources, fmt.Errorf(
				"resolve Stage 4 network resources: %w",
				err,
			)
		}
	} else {
		resources = *override
	}
	if resources.TargetMode != "upsert" {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network resources require target mode upsert",
			),
		)
	}
	if sourceEngine == "sqlite" && resources.Readers.Value != 1 {
		resources.Readers = config.EffectiveInt{
			Value:      1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	}
	if targetEngine == "sqlite" && resources.Writers.Value != 1 {
		resources.Writers = config.EffectiveInt{
			Value:      1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	}
	requiredWorkers := resources.Readers.Value + resources.Writers.Value
	if resources.Workers.Value < requiredWorkers ||
		resources.ConnectionLimit.Value < requiredWorkers {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network resources cannot safely schedule the admitted readers and writers",
			),
		)
	}
	return resources, nil
}

func buildStage4AdapterNetworkWaves(
	plan NetworkTransferPlan,
	ranges []stage4AdapterNetworkRange,
	tableCount int,
) ([]stage4AdapterNetworkWave, error) {
	if tableCount < 0 ||
		len(plan.Ranges) != len(ranges) ||
		(tableCount == 0) != (len(ranges) == 0) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 dependency-wave inventory is inconsistent",
			),
		)
	}
	if plan.RuntimeTuning != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 dependency waves require a composed migration-wide runtime tuning controller",
			),
		)
	}
	restores := make(
		map[uint64]NetworkRangeRestore,
		len(plan.Restores),
	)
	for _, restore := range plan.Restores {
		if _, duplicate := restores[restore.RangeIndex]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency-wave restore %d is duplicated",
					restore.RangeIndex,
				),
			)
		}
		restores[restore.RangeIndex] = cloneNetworkRestore(restore)
	}

	waves := make([]stage4AdapterNetworkWave, 0, tableCount)
	cursor := 0
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		first := cursor
		for cursor < len(ranges) &&
			ranges[cursor].planIndex == tableIndex {
			if ranges[cursor].globalIndex != uint64(cursor) ||
				plan.Ranges[cursor].RangeIndex != uint64(cursor) {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency-wave global range order changed",
					),
				)
			}
			cursor++
		}
		if first == cursor {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency wave for table %d has no ranges",
					tableIndex,
				),
			)
		}
		globalRanges := append(
			[]NetworkRangePlan(nil),
			plan.Ranges[first:cursor]...,
		)
		localPlan := plan
		localPlan.Ranges = make(
			[]NetworkRangePlan,
			len(globalRanges),
		)
		localPlan.Restores = make(
			[]NetworkRangeRestore,
			0,
			len(globalRanges),
		)
		for localIndex, globalRange := range globalRanges {
			localRange := globalRange
			localRange.RangeIndex = uint64(localIndex)
			localPlan.Ranges[localIndex] = localRange
			if restore, exists := restores[globalRange.RangeIndex]; exists {
				delete(restores, globalRange.RangeIndex)
				for issuedIndex := range restore.Issued {
					if restore.Issued[issuedIndex].RangeIndex !=
						globalRange.RangeIndex {
						return nil, NewTransferError(
							ErrorClassState,
							fmt.Errorf(
								"Stage 4 dependency-wave issued restore changed global range identity",
							),
						)
					}
					restore.Issued[issuedIndex].RangeIndex =
						uint64(localIndex)
				}
				restore.RangeIndex = uint64(localIndex)
				localPlan.Restores = append(
					localPlan.Restores,
					restore,
				)
			}
		}
		waves = append(waves, stage4AdapterNetworkWave{
			plan:   localPlan,
			global: globalRanges,
		})
	}
	if cursor != len(ranges) || len(restores) != 0 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 dependency waves do not cover the global range inventory",
			),
		)
	}
	return waves, nil
}

func (wave stage4AdapterNetworkWave) callbacks(
	base NetworkTransferCallbacks,
) (NetworkTransferCallbacks, error) {
	if len(wave.global) == 0 ||
		len(wave.plan.Ranges) != len(wave.global) {
		return NetworkTransferCallbacks{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 dependency wave is empty or incomplete"),
		)
	}
	globalRange := func(
		local NetworkRangePlan,
	) (NetworkRangePlan, error) {
		if local.RangeIndex >= uint64(len(wave.global)) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency wave references an unknown local range",
				),
			)
		}
		expected := wave.plan.Ranges[local.RangeIndex]
		if !reflect.DeepEqual(local, expected) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency-wave range differs from immutable admission",
				),
			)
		}
		return wave.global[local.RangeIndex], nil
	}
	return NetworkTransferCallbacks{
		ReadPage: func(
			ctx context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			global, err := globalRange(request.Range)
			if err != nil {
				return NetworkReadPage{}, err
			}
			localIndex := request.Range.RangeIndex
			request.Range = global
			if request.ReplayExpected != nil {
				if request.ReplayExpected.RangeIndex != localIndex {
					return NetworkReadPage{}, NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 dependency-wave replay identity changed",
						),
					)
				}
				replay := *request.ReplayExpected
				replay.RangeIndex = global.RangeIndex
				request.ReplayExpected = &replay
			}
			return base.ReadPage(ctx, request)
		},
		WritePage: func(
			ctx context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			global, err := globalRange(request.Range)
			if err != nil {
				return networkStateFailedReceipt(request), err
			}
			request.Range = global
			return base.WritePage(ctx, request)
		},
		RecordIssued: func(
			ctx context.Context,
			issued NetworkIssuedChunk,
		) error {
			if issued.RangeIndex >= uint64(len(wave.global)) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency wave issued an unknown local range",
					),
				)
			}
			issued.RangeIndex =
				wave.global[issued.RangeIndex].RangeIndex
			return base.RecordIssued(ctx, issued)
		},
		Checkpoint: func(
			ctx context.Context,
			checkpoint NetworkRangeCheckpoint,
		) error {
			if checkpoint.RangeIndex >= uint64(len(wave.global)) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency wave checkpointed an unknown local range",
					),
				)
			}
			globalIndex :=
				wave.global[checkpoint.RangeIndex].RangeIndex
			checkpoint.RangeIndex = globalIndex
			checkpoint.Frontier.RangeID =
				fmt.Sprintf("range/%d", globalIndex)
			return base.Checkpoint(ctx, checkpoint)
		},
	}, nil
}

func validateStage4AdapterNetworkDependencyOrder(
	plans []adapterTablePlan,
) error {
	selected := make(
		map[stage4RichTableKey]int,
		len(plans),
	)
	for index, plan := range plans {
		key := stage4RichTableKey{
			schema: plan.target.Schema,
			table:  plan.target.Name,
		}
		if _, duplicate := selected[key]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network target table (%q, %q) is duplicated",
					key.schema,
					key.table,
				),
			)
		}
		selected[key] = index
	}
	for childIndex, plan := range plans {
		for _, foreignKey := range plan.target.ForeignKeys {
			referencedSchema := foreignKey.ReferencedSchema
			if referencedSchema == "" {
				referencedSchema = plan.target.Schema
			}
			referenced := stage4RichTableKey{
				schema: referencedSchema,
				table:  foreignKey.ReferencedTable,
			}
			parentIndex, inScope, err :=
				stage4AdapterNetworkSelectedTableIndex(
					selected,
					referenced,
				)
			if err != nil {
				return err
			}
			if inScope && parentIndex >= childIndex {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 network table %s is not ordered after foreign-key parent %s",
						plan.target.Name,
						foreignKey.ReferencedTable,
					),
				)
			}
		}
	}
	return nil
}

func stage4AdapterNetworkSelectedTableIndex(
	selected map[stage4RichTableKey]int,
	referenced stage4RichTableKey,
) (int, bool, error) {
	if index, exists := selected[referenced]; exists {
		return index, true, nil
	}
	matched := -1
	for key, index := range selected {
		if strings.EqualFold(key.schema, referenced.schema) &&
			strings.EqualFold(key.table, referenced.table) {
			if matched >= 0 {
				return 0, false, NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 network foreign-key identity (%q, %q) is ambiguous under target identifier folding",
						referenced.schema,
						referenced.table,
					),
				)
			}
			matched = index
		}
	}
	if matched >= 0 {
		return matched, true, nil
	}
	return 0, false, nil
}

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
	target adapterStage4NetworkUpsertTarget,
	ranges []stage4AdapterNetworkRange,
	request NetworkWriteRequest,
) (WriteReceipt, error) {
	failed := networkStateFailedReceipt(request)
	if request.Mode != NetworkWriteIdempotentUpsert {
		return failed, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network target rejected write mode %q",
				request.Mode,
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
	receipt, writeErr := target.WriteStage4NetworkBatch(
		ctx,
		binding.plan.target,
		binding.plan.columns,
		request.Rows,
	)
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
