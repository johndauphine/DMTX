package migrate

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// Per-table Stage 4 network state: tuning, catalog equality, reader clamps,
// strict-snapshot ownership, and the plan/restore lifecycle of one table.

func newStage4AdapterTableRuntimeTuning(
	resources config.EffectiveTransferPlan,
	evidence RuntimeRowWidthEvidence,
	ranges int,
	interval time.Duration,
	now func() time.Time,
) (*RuntimeTuningController, error) {
	if ranges < 1 || !evidence.Trustworthy ||
		evidence.UpperBoundBytes < 1 ||
		evidence.ExpectedColumnCount < 1 {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 runtime tuning requires complete table range and row-width evidence",
			),
		)
	}
	if interval <= 0 {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 runtime tuning requires a positive chunk-boundary interval",
			),
		)
	}
	limits := RuntimeTuningLimits{
		// No adapter currently returns an authenticated target transport
		// ceiling at this generic boundary. Leave protocol ceilings unknown
		// rather than relabeling the DMTX row cap or memory budget as target
		// protocol evidence; a typed protocol write failure can still impose
		// a durable in-controller reduction at the next safe boundary.
		ProtocolMaxChunkRows:         0,
		ProtocolMaxChunkBytes:        0,
		SafetyRowWidthUpperBound:     evidence.UpperBoundBytes,
		PlannedRanges:                uint64(ranges),
		ExpectedColumnCount:          evidence.ExpectedColumnCount,
		HistoryLimit:                 128,
		GrowthAfterHealthyBoundaries: 4,
	}
	controller, err := NewRuntimeTuningControllerWithOptions(
		resources,
		limits,
		RuntimeTuningOptions{Interval: interval, Now: now},
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"construct Stage 4 table runtime tuning controller: %w",
				err,
			),
		)
	}
	return controller, nil
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
				execution.corePlan.ReplayMode,
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
	// The core returns its status snapshot even when the transfer itself
	// failed. Capture that bounded safety evidence before propagating the
	// failure: a protocol/write reduction is most useful precisely on an
	// incomplete run that needs diagnosis or safe resume.
	if result.HasRuntimeTuning && execution.corePlan.RuntimeTuning != nil {
		execution.parent.recordRuntimeTuningResult(
			execution.planIndex,
			result.RuntimeTuning,
			execution.corePlan.RuntimeTuning.History(),
		)
	}
	if err != nil {
		return 0, err
	}
	if result.HasRuntimeTuning !=
		(execution.corePlan.RuntimeTuning != nil) ||
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
