package migrate

import (
	"context"
	"fmt"
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

// adapterStage4NetworkRebuildTarget is the explicit target-owned proof that a
// rebuild page can be replayed after its durable issued-page record. Unlike an
// ordinary fresh insert, this writer must leave an already inserted matching
// primary-key row untouched while refusing to turn a conflicting unique-key
// row into an update. A route without this proof must not start a destructive
// Stage 4 transfer.
type adapterStage4NetworkRebuildTarget interface {
	targetAdapter
	stage4NetworkDuplicateSafeRebuildTarget()
	PreflightStage4NetworkRebuild(
		context.Context,
		[]schema.Table,
	) error
	WriteStage4NetworkRebuildBatch(
		context.Context,
		schema.Table,
		[]string,
		NetworkWriteMode,
		[][]any,
	) (WriteReceipt, error)
}

func (*postgresTargetAdapter) stage4NetworkIdempotentUpsertTarget() {}
func (*mysqlTargetAdapter) stage4NetworkIdempotentUpsertTarget()    {}
func (*sqlServerTargetAdapter) stage4NetworkIdempotentUpsertTarget() {
}
func (*sqliteTargetAdapter) stage4NetworkIdempotentUpsertTarget() {}

func (*postgresTargetAdapter) stage4NetworkDuplicateSafeRebuildTarget() {}
func (*mysqlTargetAdapter) stage4NetworkDuplicateSafeRebuildTarget()    {}
func (*sqlServerTargetAdapter) stage4NetworkDuplicateSafeRebuildTarget() {
}
func (*sqliteTargetAdapter) stage4NetworkDuplicateSafeRebuildTarget() {}

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
	target      targetAdapter
	coordinator *networkStateCoordinator
	ranges      []stage4AdapterNetworkRange
	plan        NetworkTransferPlan
	waves       []stage4AdapterNetworkWave
	tableCount  int
	prepared    stage4AdapterPrepared
	// boundWork contains the exact durable pagination plan for tables whose
	// stable view has been admitted. prepared.work deliberately remains the
	// immutable pre-pagination seed: rebinding a seed that was replaced with
	// its own derived topology would hash a different plan on every reopen.
	boundWork   []stage4AdapterWork
	partitions  int
	resources   config.EffectiveTransferPlan
	retryPolicy RetryPolicy
	// upsertMergeRows is zero unless an operator explicitly requested a
	// capability-backed write-only merge ceiling. It is carried into every
	// table-local core plan without changing source pagination.
	upsertMergeRows int
	// largeTableThreshold is zero for the generated compatibility default. An
	// explicit positive value is consumed during retained-view planning before
	// a table's durable range inventory is created.
	largeTableThreshold int64
	// checkpointFrequency is an explicit per-range acknowledgement cadence.
	// Zero retains immediate-on-ack checkpoints for generated defaults.
	checkpointFrequency int
	runtimeTuning       bool
	// runtimeTuningInterval is immutable config intent. Each serial table gets
	// a fresh controller, but all controllers use this same migration setting.
	runtimeTuningInterval time.Duration
	// runtimeTuningReports are populated only after a controller has completed
	// a table transfer. They are retained in immutable plan order for the
	// credential-free Result status surface.
	runtimeTuningReports []stage4AdapterRuntimeTuningTableReport
	// runtimeTuningHistory retains invocation-local session IDs and receipt
	// chains. It is populated only when the run backend resolves the optional
	// lease-fenced SQLite history capability; YAML/current-run state remains
	// intentionally history-free.
	runtimeTuningHistory map[int]*stage4AdapterRuntimeTuningHistorySession

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

type stage4AdapterRuntimeTuningTableReport struct {
	recorded    bool
	snapshot    RuntimeTuningSnapshot
	adjustments []RuntimeTuningDecision
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
	// Normal entry points already run the configuration-only gate before this
	// admission. Keep the setting-specific check here too so direct callers
	// cannot bypass the fail-closed incremental/strict/delete boundary.
	if err := requireStage4CheckpointFrequencyComposition(cfg); err != nil {
		return nil, err
	}
	if err := requireStage4LargeTableThresholdComposition(cfg); err != nil {
		return nil, err
	}
	if err := requireStage4AdapterNetworkMode(
		cfg,
		prepared,
		options,
	); err != nil {
		return nil, err
	}
	checkpointFrequency, err := stage4AdapterNetworkCheckpointFrequency(
		cfg.Migration,
	)
	if err != nil {
		return nil, err
	}
	largeTableThreshold, err := stage4AdapterLargeTableThreshold(
		cfg.Migration,
	)
	if err != nil {
		return nil, err
	}
	if err := prepared.run.Validate(); err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("validate Stage 4 network run context: %w", err),
		)
	}
	// The compatibility path below accepts a prebound multi-table range plan
	// and fans it into dependency waves. It has no migration-wide controller
	// session that could preserve one ordered adjustment history across waves.
	// Production adapters bind one stable table at a time instead, so reject an
	// enabled controller here rather than silently leaving it inert.
	if cfg.Migration.RuntimeTuning && prepared.network != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 runtime tuning requires the deferred stable-table network path; prebound dependency waves have no composed controller session",
			),
		)
	}
	if prepared.mode == "drop_recreate" {
		if err := admitStage4AdapterNetworkRebuildRecovery(
			prepared.run,
		); err != nil {
			return nil, err
		}
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
	var networkTarget targetAdapter
	var rebuildTarget adapterStage4NetworkRebuildTarget
	switch prepared.mode {
	case "upsert":
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
		networkTarget = upsertTarget
	case "drop_recreate":
		certifiedRebuildTarget, ok := target.(adapterStage4NetworkRebuildTarget)
		if !ok || isNilInterface(certifiedRebuildTarget) {
			engine := ""
			if !isNilInterface(target) {
				engine = target.Engine()
			}
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 target engine %q has no certified duplicate-safe rebuild replay path",
					engine,
				),
			)
		}
		rebuildTarget = certifiedRebuildTarget
		networkTarget = certifiedRebuildTarget
	default:
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network runner received unsupported target mode %q",
				prepared.mode,
			),
		)
	}
	if isNilInterface(networkTarget) {
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified network replay path",
				engine,
			),
		)
	}
	if source.Engine() == "clickhouse" ||
		networkTarget.Engine() == "clickhouse" {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 analytical ClickHouse transfer requires its dedicated bounded runner",
			),
		)
	}
	if !stage4AdapterNetworkRelationalEngine(source.Engine()) ||
		!stage4AdapterNetworkRelationalEngine(networkTarget.Engine()) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network route %s-to-%s is unsupported",
				source.Engine(),
				networkTarget.Engine(),
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
	if prepared.mode == "drop_recreate" {
		if err := rebuildTarget.PreflightStage4NetworkRebuild(
			ctx,
			targetTables,
		); err != nil {
			return nil, err
		}
	} else {
		if err := preflightStage4NetworkReplayIsolation(
			ctx,
			networkTarget,
			targetTables,
		); err != nil {
			return nil, err
		}
	}

	resources, err := stage4AdapterNetworkResources(
		ctx,
		cfg,
		source.Engine(),
		networkTarget.Engine(),
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
	upsertMergeRows, err := stage4AdapterExplicitUpsertMergeRows(
		ctx,
		cfg.Migration,
		prepared.mode,
		networkTarget,
		resources.ChunkRows.Value,
	)
	if err != nil {
		return nil, err
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
			deferred:              true,
			source:                source,
			target:                networkTarget,
			tableCount:            len(prepared.plans),
			prepared:              cloneStage4AdapterNetworkPrepared(prepared),
			partitions:            cfg.Migration.Partitions,
			resources:             resources,
			retryPolicy:           retryPolicy,
			upsertMergeRows:       upsertMergeRows,
			largeTableThreshold:   largeTableThreshold,
			checkpointFrequency:   checkpointFrequency,
			runtimeTuning:         cfg.Migration.RuntimeTuning,
			runtimeTuningInterval: cfg.Migration.RuntimeTuningInterval,
		}, nil
	}
	if largeTableThreshold != 0 {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold requires deferred table-stable network planning",
			),
		)
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
			target:     networkTarget,
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
		target:      networkTarget,
		coordinator: coordinator,
		ranges:      ranges,
		tableCount:  len(prepared.plans),
		plan: NetworkTransferPlan{
			SourceEngine:        source.Engine(),
			TargetEngine:        networkTarget.Engine(),
			Resources:           resources,
			RetryPolicy:         retryPolicy,
			ReplayMode:          stage4AdapterNetworkReplayMode(prepared.mode),
			UpsertMergeRows:     upsertMergeRows,
			CheckpointFrequency: checkpointFrequency,
			Ranges:              transferRanges,
		},
	}, nil
}

// bindStage4AdapterNetworkRestoresAndValidate runs after the ordinary network
// plans are checkpointed but before PrepareTables. It freezes the exact durable
// restore set and proves the complete core plan without executing callbacks.
