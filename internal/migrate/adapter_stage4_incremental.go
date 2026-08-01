package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4AdapterIncrementalStrategy = "stage4_adapter_incremental_window_v1"
	stage4AdapterIncrementalRangeID  = "incremental-window"
)

type stage4AdapterIncrementalPrepared struct {
	source                             incrementalSourceAdapter
	target                             adapterStage4NetworkUpsertTarget
	validator                          adapterStage4IncrementalValidationTarget
	aggregate                          state.Stage4AggregateBackend
	validation                         *stage4AdapterIncrementalValidationEvidence
	validationPrimaryKeyEqualityProofs map[stage4RichTableKey]string
	deletes                            *stage4AdapterSQLiteIncrementalDeletePrepared
	tables                             []stage4AdapterIncrementalTable
	upsertMergeRows                    int
}

type stage4AdapterIncrementalTable struct {
	plan      IncrementalTablePlan
	work      stage4AdapterWork
	planIndex int
	attemptID string
}

type stage4AdapterIncrementalProgress struct {
	rows         int
	nextSequence uint64
}

// prepareStage4AdapterIncremental admits only relational/SQLite sources and
// targets with an explicit bounded-window writer and exact target-value
// validation. The shared incremental state machine owns the immutable upper
// fence and full-window replay; target-specific admission must finish before
// it creates ordinary table work or permits any target mutation.
func prepareStage4AdapterIncremental(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) (*stage4AdapterIncrementalPrepared, []stage4AdapterWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if prepared.mode != "upsert" {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 date-based incremental migration requires target mode upsert",
			),
		)
	}
	if !stage4AdapterIncrementalSourceEngine(source.Engine()) {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 date-based incremental source engine %q is not certified",
				source.Engine(),
			),
		)
	}
	if !stage4AdapterIncrementalTargetEngine(target.Engine()) {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 date-based incremental target engine %q is not certified",
				target.Engine(),
			),
		)
	}
	validationPlan, validationErr := BuildValidationPlan(
		cfg.Migration.Validation.Mode,
	)
	if validationErr != nil {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 date-based incremental validation mode %q is unsupported: %w",
				cfg.Migration.Validation.Mode,
				validationErr,
			),
		)
	}
	incrementalSource, err := requireIncrementalSourceAdapter(source)
	if err != nil {
		return nil, nil, err
	}
	upsertTarget, ok := target.(adapterStage4NetworkUpsertTarget)
	if !ok || isNilInterface(upsertTarget) {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified idempotent incremental upsert path",
				target.Engine(),
			),
		)
	}
	validator, ok := target.(adapterStage4IncrementalValidationTarget)
	if !ok || isNilInterface(validator) {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified exact incremental window validation path",
				target.Engine(),
			),
		)
	}
	aggregate, ok := prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil, nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 date-based incremental migration requires aggregate table completion state",
			),
		)
	}
	if len(prepared.plans) != len(prepared.work) {
		return nil, nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 incremental table and durable work inventories differ",
			),
		)
	}
	validationPrimaryKeyEqualityProofs := make(
		map[stage4RichTableKey]string,
		len(prepared.plans),
	)
	for _, adapterPlan := range prepared.plans {
		_, _, proof, proofErr := adapterValidationCrossEqualityProof(
			source.Engine(),
			target.Engine(),
			adapterPlan,
		)
		if proofErr != nil {
			return nil, nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"admit Stage 4 incremental primary-key equality for %s-to-%s table %s: %w",
					source.Engine(),
					target.Engine(),
					adapterPlan.source.Name,
					proofErr,
				),
			)
		}
		key := stage4RichTableKey{
			schema: adapterPlan.source.Schema,
			table:  adapterPlan.source.Name,
		}
		if _, exists := validationPrimaryKeyEqualityProofs[key]; exists {
			return nil, nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental primary-key equality inventory duplicates table (%q, %q)",
					key.schema,
					key.table,
				),
			)
		}
		validationPrimaryKeyEqualityProofs[key] = proof
	}
	existingTargets, err := stage4AdapterExistingEvolutionTargetTables(
		prepared.evolution,
		prepared.targetTables,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := preflightStage4NetworkReplayIsolation(
		ctx,
		upsertTarget,
		existingTargets,
	); err != nil {
		return nil, nil, err
	}
	if err := preflightStage4IncrementalUpsert(
		ctx,
		upsertTarget,
		existingTargets,
	); err != nil {
		return nil, nil, err
	}
	upsertMergeRows := 0
	mergeRequested, mergeRequestErr := stage4AdapterUpsertMergeRequested(
		cfg.Migration,
	)
	if mergeRequestErr != nil {
		return nil, nil, mergeRequestErr
	}
	if mergeRequested {
		resources, resourceErr := stage4AdapterNetworkResources(
			ctx,
			cfg,
			source.Engine(),
			upsertTarget.Engine(),
			nil,
		)
		if resourceErr != nil {
			return nil, nil, resourceErr
		}
		resourceRows := stage4MinimumUpsertMergeRows(
			resources.ChunkRows.Value,
			sqliteWriteBatchSize,
		)
		upsertMergeRows, mergeRequestErr =
			stage4AdapterExplicitUpsertMergeRows(
				ctx,
				cfg.Migration,
				prepared.mode,
				upsertTarget,
				resourceRows,
			)
		if mergeRequestErr != nil {
			return nil, nil, mergeRequestErr
		}
	}
	validationEvidence, err := newStage4AdapterIncrementalValidationEvidence(
		validationPlan.Mode,
		prepared.plans,
		target,
		prepared.run.SpoolDirectory,
	)
	if err != nil {
		return nil, nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("admit Stage 4 incremental validation evidence: %w", err),
		)
	}

	result := &stage4AdapterIncrementalPrepared{
		source:                             incrementalSource,
		target:                             upsertTarget,
		validator:                          validator,
		aggregate:                          aggregate,
		validation:                         validationEvidence,
		validationPrimaryKeyEqualityProofs: validationPrimaryKeyEqualityProofs,
		tables:                             make([]stage4AdapterIncrementalTable, len(prepared.plans)),
		upsertMergeRows:                    upsertMergeRows,
	}
	work := make([]stage4AdapterWork, len(prepared.work))
	for index, adapterPlan := range prepared.plans {
		table, mapErr := incrementalSource.IncrementalTable(
			adapterPlan.source,
		)
		if mapErr != nil {
			return nil, nil, fmt.Errorf(
				"map Stage 4 incremental table %s: %w",
				adapterPlan.source.Name,
				mapErr,
			)
		}
		plan, planErr := BuildIncrementalTablePlan(
			table,
			cfg.Migration.DateUpdatedColumns,
		)
		if planErr != nil {
			return nil, nil, fmt.Errorf(
				"plan Stage 4 incremental table %s: %w",
				adapterPlan.source.Name,
				planErr,
			)
		}
		topology, topologyErr := stage4AdapterIncrementalTopology(
			prepared.work[index],
			plan,
		)
		if topologyErr != nil {
			return nil, nil, topologyErr
		}
		item := prepared.work[index]
		item.strategy = stage4AdapterIncrementalStrategy
		item.topology = topology
		item.pagination = PaginationPlan{}
		item.ranges = []state.RangeState{{
			ID:           stage4AdapterIncrementalRangeID,
			Strategy:     stage4AdapterIncrementalStrategy,
			TopologyHash: topology,
		}}
		attemptID := stage4AdapterIncrementalAttemptID(
			prepared.run.RunID,
			item.task,
			topology,
		)
		work[index] = item
		result.tables[index] = stage4AdapterIncrementalTable{
			plan:      plan,
			work:      item,
			planIndex: index,
			attemptID: attemptID,
		}
	}
	return result, work, nil
}

func stage4AdapterIncrementalSourceEngine(engine string) bool {
	switch engine {
	case "postgres", "mssql", "mysql", "sqlite":
		return true
	default:
		return false
	}
}

func stage4AdapterIncrementalTargetEngine(engine string) bool {
	switch engine {
	case "postgres", "mssql", "mysql", "sqlite":
		return true
	default:
		return false
	}
}

func nilStage4AggregateBackend(backend state.Stage4AggregateBackend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func stage4AdapterIncrementalTopology(
	base stage4AdapterWork,
	plan IncrementalTablePlan,
) (string, error) {
	wire := struct {
		Version      int    `json:"version"`
		BaseTopology string `json:"base_topology"`
		PlanHash     string `json:"plan_hash"`
		Strategy     string `json:"strategy"`
		RangeID      string `json:"range_id"`
	}{
		Version:      1,
		BaseTopology: base.topology,
		PlanHash:     plan.PlanHash,
		Strategy:     stage4AdapterIncrementalStrategy,
		RangeID:      stage4AdapterIncrementalRangeID,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf(
			"encode Stage 4 incremental topology for %s: %w",
			plan.Table.Name,
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stage4AdapterIncrementalAttemptID(
	runID string,
	task state.TaskKey,
	topology string,
) string {
	encoded, _ := json.Marshal(struct {
		Version  int           `json:"version"`
		RunID    string        `json:"run_id"`
		Task     state.TaskKey `json:"task"`
		Topology string        `json:"topology"`
	}{
		Version:  1,
		RunID:    runID,
		Task:     task,
		Topology: topology,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func migrateWithStage4IncrementalAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	incremental := prepared.incremental
	if incremental == nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 incremental execution is unavailable"),
		)
	}
	if incremental.validation == nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 incremental validation evidence is unavailable"),
		)
	}
	defer func() {
		cleanupErr := incremental.validation.Close()
		if cleanupErr == nil {
			return
		}
		cleanupErr = NewTransferError(
			ErrorClassState,
			fmt.Errorf("cleanup Stage 4 incremental validation evidence: %w", cleanupErr),
		)
		if resultErr == nil {
			result.Validated = false
			resultErr = cleanupErr
			return
		}
		resultErr = errors.Join(resultErr, cleanupErr)
	}()
	// A resumed completed transfer may not construct a new source-key delete
	// plan without its original retained view.  This is state-only and belongs
	// ahead of schema staging, ordinary checkpoints, or private journal DDL.
	if err := preflightStage4AdapterSQLiteIncrementalDeleteResume(
		ctx,
		prepared,
		resume,
	); err != nil {
		return Result{}, err
	}
	if err := stageStage4SchemaGateSnapshots(
		ctx,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return Result{}, err
	}
	if err := incremental.aggregate.EnsureStage4TableInventory(
		stage4AdapterIncrementalInventory(prepared),
	); err != nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"publish immutable Stage 4 incremental table inventory before table checkpoints: %w",
				err,
			),
		)
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		prepared.names,
	); err != nil {
		return Result{}, err
	}
	if resume {
		if err := resetStage4AdapterIncrementalWork(
			ctx,
			prepared,
			completed,
		); err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
	}
	incomplete := stage4AdapterIncompleteWork(prepared, completed)
	if len(incomplete) != 0 {
		if err := ensureStage4AdapterWork(
			ctx,
			prepared.run,
			incomplete,
		); err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
	}
	for _, table := range incremental.tables {
		name := prepared.plans[table.planIndex].source.Name
		if rows, complete := completed[name]; complete {
			if err := verifyCompletedStage4AdapterIncrementalTable(
				ctx,
				prepared,
				table,
				rows,
			); err != nil {
				return resultForValidatedAdapterCheckpoints(completed), err
			}
			continue
		}
		if err := observer.BeforeTable(ctx, name); err != nil {
			return resultForValidatedAdapterCheckpoints(completed),
				NewTransferError(
					ErrorClassState,
					fmt.Errorf("checkpoint before %s: %w", name, err),
				)
		}
	}

	if err := applyStage4AdapterTargetSchema(
		ctx,
		observer,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(
		ctx,
		target,
		prepared,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	// Delete authority is intentionally bound only after target evolution has
	// made the exact target table shape available.  This remains before any
	// incremental PrepareTables or row write, so an unsupported SQLite delete
	// capability cannot be discovered after data mutation.
	if err := activateStage4AdapterPostgresDeleteComposition(ctx, prepared); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	// Private delete-journal DDL follows schema application and exact table
	// authority activation, but still precedes any incremental attempt, target
	// PrepareTables call, or row write.
	if err := ensureStage4AdapterDeleteJournalReadiness(
		ctx,
		observer,
		prepared,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	currentTargets, err := stage4AdapterCurrentEvolutionTargetTables(
		prepared.evolution,
		prepared.targetTables,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if err := preflightStage4NetworkReplayIsolation(
		ctx,
		incremental.target,
		currentTargets,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}

	// Arm every selected timestamp table only after target schema authority,
	// delete activation, and private-journal readiness are durable. An active
	// attempt is reused verbatim on resume, so its upper fence can never move
	// forward after this point.
	for _, table := range incremental.tables {
		name := prepared.plans[table.planIndex].source.Name
		if _, complete := completed[name]; complete {
			continue
		}
		if _, err := ExecuteIncrementalTable(
			ctx,
			stage4AdapterIncrementalRequest(
				prepared,
				table,
				true,
				nil,
				nil,
			),
		); err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
	}

	result = resultForValidatedAdapterCheckpoints(completed)
	for _, table := range incremental.tables {
		adapterPlan := prepared.plans[table.planIndex]
		if _, complete := completed[adapterPlan.source.Name]; complete {
			continue
		}
		mutationObserver := observer
		if resume {
			mutationObserver = adapterResumeMutationGuard{
				ctx:      ctx,
				delegate: observer,
				boundary: "mutate resumed Stage 4 incremental table " +
					adapterPlan.source.Name,
			}
		}
		progress := stage4AdapterIncrementalProgress{}
		transfer := func(
			transferCtx context.Context,
			read IncrementalReadPlan,
		) error {
			next, transferErr := transferStage4AdapterIncrementalTable(
				transferCtx,
				mutationObserver,
				prepared,
				table,
				read,
			)
			progress = next
			return transferErr
		}
		publisher := func(
			publishCtx context.Context,
			commit state.IncrementalCommit,
		) error {
			if err := publishCtx.Err(); err != nil {
				return err
			}
			return incremental.aggregate.CompleteStage4Table(
				stage4AdapterIncrementalCompletion(
					prepared,
					table,
					progress,
					commit.CompletedAt,
					&commit,
				),
			)
		}
		request := stage4AdapterIncrementalRequest(
			prepared,
			table,
			false,
			transfer,
			publisher,
		)
		execution, executeErr := ExecuteIncrementalTable(ctx, request)
		if executeErr != nil {
			return result, executeErr
		}
		if table.plan.FullTableUpsert {
			completedAt := time.Now().UTC()
			if err := incremental.aggregate.CompleteStage4Table(
				stage4AdapterIncrementalCompletion(
					prepared,
					table,
					progress,
					completedAt,
					nil,
				),
			); err != nil {
				return result, incrementalPostTransferStateError(
					"atomically complete full-table incremental fallback",
					err,
				)
			}
		} else if !execution.Completed {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental table %s did not publish terminal evidence",
					adapterPlan.source.Name,
				),
			)
		}
		result.Tables++
		result.Rows += progress.rows
	}
	if err := reconcileStage4AdapterSQLiteIncrementalDeletes(
		ctx,
		&prepared,
		resume,
	); err != nil {
		return result, err
	}
	if err := validateStage4AdapterIncrementalRun(
		ctx,
		cfg,
		source,
		target,
		prepared,
	); err != nil {
		return result, err
	}
	// The schema sentinels are deliberately left running. This route publishes a
	// durable table inventory and per-table receipts, so PublishStage4RunCompletion
	// completes the sentinels and the run outcome in one mutation once the caller
	// has recorded its validation evidence. Completing them here would make that
	// publication fail closed on already-terminal sentinels.
	result.Validated = true
	return result, nil
}

// validateStage4AdapterIncrementalRun executes the inclusive §12 validation
// plan over the exact transfer attempt, not over a later live whole-source
// read. The per-batch target proof is complete-key/full-row canonical equality;
// the evidence probe aggregates source count, NULL, and deterministic
// PK-sample facts, then re-queries the actual target through the private exact
// transferred-key spool. It admits neither post-attempt source rows nor
// target-only upsert rows into the final validation scope.
func validateStage4AdapterIncrementalRun(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) error {
	if prepared.incremental == nil || prepared.incremental.validation == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 incremental validation evidence is unavailable"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		return err
	}
	report, err := RunValidationCore(
		ctx,
		ValidationCoreOptions{
			Mode:                   cfg.Migration.Validation.Mode,
			TargetMode:             prepared.mode,
			FailOnMismatch:         cfg.Migration.Validation.FailOnMismatch,
			FailOnTimeout:          cfg.Migration.Validation.FailOnTimeout,
			FailOnEstimateMismatch: cfg.Migration.Validation.FailOnEstimateMismatch,
			ExactCountTimeout:      30 * time.Second,
			TableTimeout:           2 * time.Minute,
			TableConcurrency:       stage4ValidationConcurrency(len(specs)),
			SampleLimit:            stage4AdapterIncrementalValidationSampleLimit,
		},
		specs,
		prepared.incremental.validation,
	)
	if err != nil {
		return fmt.Errorf("run Stage 4 incremental validation core: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !report.Passed {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"Stage 4 incremental post-window validation failed for route %s-to-%s",
				source.Engine(),
				target.Engine(),
			),
		)
	}
	return nil
}

func stage4AdapterIncrementalInventory(
	prepared stage4AdapterPrepared,
) state.Stage4TableInventory {
	inventory := state.Stage4TableInventory{
		RunID:                prepared.run.RunID,
		SchemaTask:           prepared.gate.Task,
		SchemaTopologyHash:   prepared.gate.TopologyHash,
		SchemaSnapshotDigest: prepared.gate.PendingSnapshot.Digest,
		Tables: make(
			[]state.Stage4TableInventoryEntry,
			len(prepared.work),
		),
	}
	for index, work := range prepared.work {
		ranges := make(
			[]state.Stage4InventoryRange,
			len(work.ranges),
		)
		for rangeIndex, workRange := range work.ranges {
			ranges[rangeIndex] = state.Stage4InventoryRange{
				ID: workRange.ID,
			}
		}
		inventory.Tables[index] = state.Stage4TableInventoryEntry{
			Table:        work.task.Table,
			Task:         work.task,
			Strategy:     work.strategy,
			TopologyHash: work.topology,
			Ranges:       ranges,
		}
	}
	return inventory
}

func stage4AdapterIncompleteWork(
	prepared stage4AdapterPrepared,
	completed map[string]int,
) []stage4AdapterWork {
	work := make([]stage4AdapterWork, 0, len(prepared.work))
	for _, item := range prepared.work {
		if _, complete := completed[item.task.Table]; complete {
			continue
		}
		work = append(work, item)
	}
	return work
}

func resetStage4AdapterIncrementalWork(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	completed map[string]int,
) error {
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return err
	}
	expected := make(map[state.TaskKey]stage4AdapterWork, len(prepared.work))
	for _, work := range prepared.work {
		expected[work.task] = work
	}
	type sentinelAuthority struct {
		rangeID  string
		strategy string
		topology string
	}
	sentinels := map[state.TaskKey]sentinelAuthority{
		prepared.gate.Task: {
			rangeID:  stage4SchemaGateRangeID,
			strategy: stage4SchemaGateStrategy,
			topology: prepared.gate.TopologyHash,
		},
	}
	if prepared.evolution != nil {
		sentinels[prepared.evolution.authority.Task()] =
			sentinelAuthority{
				rangeID:  stage4TargetShapeRangeID,
				strategy: stage4TargetShapeStrategy,
				topology: prepared.evolution.authority.TopologyHash(),
			}
	}
	for key := range inventory.tasks {
		if _, found := expected[key]; found {
			continue
		}
		if _, found := sentinels[key]; found {
			continue
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"unexpected run-scoped Stage 4 work %#v before incremental replay",
				key,
			),
		)
	}
	for key := range inventory.ranges {
		if _, found := expected[key]; found {
			continue
		}
		if _, found := sentinels[key]; found {
			continue
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"unexpected run-scoped Stage 4 range work %#v before incremental replay",
				key,
			),
		)
	}
	for key, authority := range sentinels {
		if _, _, _, err := inventory.exact(
			key,
			authority.rangeID,
			authority.strategy,
			authority.topology,
			false,
		); err != nil {
			return fmt.Errorf(
				"verify exact Stage 4 sentinel %#v before incremental replay: %w",
				key,
				err,
			)
		}
	}
	for _, work := range prepared.work {
		task, found := inventory.tasks[work.task]
		ranges := inventory.ranges[work.task]
		checkpointRows, complete := completed[work.task.Table]
		if !found {
			if len(ranges) != 0 || complete {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 incremental table %s has partial completed work evidence",
						work.task.Table,
					),
				)
			}
			continue
		}
		if task.RunID != prepared.run.RunID ||
			task.Key != work.task ||
			task.Strategy != work.strategy ||
			task.TopologyHash != work.topology ||
			len(ranges) != 1 ||
			ranges[0].ID != stage4AdapterIncrementalRangeID ||
			ranges[0].Strategy != work.strategy ||
			ranges[0].TopologyHash != work.topology {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental work topology changed for table %s",
					work.task.Table,
				),
			)
		}
		if complete {
			if err := validateCompletedStage4AdapterIncrementalWork(
				task,
				ranges[0],
				checkpointRows,
			); err != nil {
				return err
			}
			continue
		}
		if task.Status != "running" ||
			ranges[0].Status != "running" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"incomplete Stage 4 incremental table %s has terminal or unsafe structured work",
					work.task.Table,
				),
			)
		}
		resetTask := state.WorkTask{
			RunID:        prepared.run.RunID,
			Key:          work.task,
			Strategy:     work.strategy,
			TopologyHash: work.topology,
			StartedAt:    time.Now().UTC(),
		}
		resetRanges := []state.RangeState{
			cloneInitialNetworkStateRange(work.ranges[0]),
		}
		if err := prepared.run.Backend.ResetWorkPlan(
			resetTask,
			resetRanges,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"discard positional progress and reset the full incremental window for %s before replay: %w",
					work.task.Table,
					err,
				),
			)
		}
	}
	return ctx.Err()
}

func validateCompletedStage4AdapterIncrementalWork(
	task state.WorkTask,
	workRange state.RangeState,
	checkpointRows int,
) error {
	if task.Status != "completed" ||
		workRange.Status != "completed" ||
		task.Error != "" ||
		workRange.Error != "" ||
		len(workRange.Pending) != 0 ||
		workRange.SequenceOffset != 0 ||
		checkpointRows < 0 ||
		workRange.RowsDone != int64(checkpointRows) ||
		task.CompletedAt.IsZero() ||
		workRange.CompletedAt.IsZero() {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"completed Stage 4 incremental table %s lacks exact aggregate work evidence",
				task.Key.Table,
			),
		)
	}
	return nil
}

func verifyCompletedStage4AdapterIncrementalTable(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	checkpointRows int,
) error {
	if table.plan.FullTableUpsert {
		inventory, err := loadStage4WorkInventory(ctx, prepared.run)
		if err != nil {
			return err
		}
		task := inventory.tasks[table.work.task]
		ranges := inventory.ranges[table.work.task]
		if len(ranges) != 1 {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 incremental fallback %s has an unsafe range set",
					table.work.task.Table,
				),
			)
		}
		if err := validateCompletedStage4AdapterIncrementalWork(
			task,
			ranges[0],
			checkpointRows,
		); err != nil {
			return err
		}
		rows, err := validateCompletedStage4AdapterIncrementalRead(
			ctx,
			prepared,
			table,
			incrementalFullTableRead(table.plan, true),
		)
		if err != nil {
			return err
		}
		if rows != checkpointRows {
			return NewTransferError(
				ErrorClassValidation,
				fmt.Errorf(
					"completed Stage 4 incremental fallback %s now has %d exact source rows, not the aggregate count %d",
					table.work.task.Table,
					rows,
					checkpointRows,
				),
			)
		}
		return nil
	}
	request := stage4AdapterIncrementalRequest(
		prepared,
		table,
		true,
		nil,
		nil,
	)
	request.VerifyCompletedTable = func(
		verifyCtx context.Context,
		attempt state.IncrementalAttempt,
	) error {
		read, err := incrementalAttemptRead(
			table.plan,
			attempt,
			true,
		)
		if err != nil {
			return err
		}
		rows, err := validateCompletedStage4AdapterIncrementalRead(
			verifyCtx,
			prepared,
			table,
			read,
		)
		if err != nil {
			return err
		}
		if rows != checkpointRows {
			return fmt.Errorf(
				"exact completed incremental source window has %d rows, not aggregate count %d",
				rows,
				checkpointRows,
			)
		}
		return nil
	}
	result, err := ExecuteIncrementalTable(ctx, request)
	if err != nil {
		return err
	}
	if !result.AlreadyCompleted {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"completed Stage 4 incremental table %s lacks a committed attempt",
				table.work.task.Table,
			),
		)
	}
	return nil
}

func validateCompletedStage4AdapterIncrementalRead(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	read IncrementalReadPlan,
) (int, error) {
	adapterPlan := prepared.plans[table.planIndex]
	rows, err := prepared.incremental.source.OpenIncrementalRows(
		ctx,
		adapterPlan.source,
		adapterPlan.columns,
		read,
	)
	if err != nil {
		return 0, err
	}
	values := make([]any, len(adapterPlan.columns))
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	batch := make([][]any, 0, sqliteWriteBatchSize)
	total := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := prepared.incremental.validator.
			ValidateStage4IncrementalBatch(
				ctx,
				adapterPlan.target,
				adapterPlan.columns,
				batch,
			); err != nil {
			return NewTransferError(
				ErrorClassValidation,
				fmt.Errorf(
					"revalidate exact completed Stage 4 incremental target values for %s: %w",
					adapterPlan.source.Name,
					err,
				),
			)
		}
		if err := prepared.incremental.validation.RecordExactBatch(
			ctx,
			adapterPlan.source,
			adapterPlan.columns,
			batch,
		); err != nil {
			return NewTransferError(
				ErrorClassValidation,
				fmt.Errorf(
					"record exact completed Stage 4 incremental validation evidence for %s: %w",
					adapterPlan.source.Name,
					err,
				),
			)
		}
		if len(batch) > math.MaxInt-total {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 incremental validation row total overflows for %s",
					adapterPlan.source.Name,
				),
			)
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}
	var readErr error
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			readErr = fmt.Errorf(
				"read completed PostgreSQL incremental table %s: %w",
				adapterPlan.source.Name,
				err,
			)
			break
		}
		batch = append(batch, cloneAdapterRow(values))
		if len(batch) == sqliteWriteBatchSize {
			if err := flush(); err != nil {
				readErr = err
				break
			}
		}
	}
	if readErr == nil {
		if err := rows.Err(); err != nil {
			readErr = fmt.Errorf(
				"read completed PostgreSQL incremental table %s: %w",
				adapterPlan.source.Name,
				err,
			)
		}
	}
	if readErr == nil {
		readErr = flush()
	}
	closeErr := rows.Close()
	if readErr != nil {
		if closeErr != nil {
			return total, errors.Join(readErr, closeErr)
		}
		return total, readErr
	}
	if closeErr != nil {
		return total, fmt.Errorf(
			"close completed Stage 4 incremental source rows for %s: %w",
			adapterPlan.source.Name,
			closeErr,
		)
	}
	return total, ctx.Err()
}

func stage4AdapterIncrementalRequest(
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	armOnly bool,
	transfer IncrementalTransfer,
	publisher IncrementalCompletionPublisher,
) IncrementalExecutionRequest {
	sourceTable := cloneStage4RichTable(
		prepared.plans[table.planIndex].source,
	)
	return IncrementalExecutionRequest{
		State:        prepared.run.Backend,
		RunID:        prepared.run.RunID,
		Task:         table.work.task,
		AttemptID:    table.attemptID,
		TopologyHash: table.work.topology,
		StartedAt:    time.Now().UTC(),
		Plan:         table.plan,
		SampleUpperFence: func(
			ctx context.Context,
			_ IncrementalTable,
			column IncrementalColumn,
		) (*time.Time, error) {
			return prepared.incremental.source.SampleIncrementalUpperFence(
				ctx,
				sourceTable,
				column,
			)
		},
		VerifyDurableBinding: func(
			ctx context.Context,
			_ state.IncrementalAttempt,
			planHash string,
			topology string,
		) error {
			if planHash != table.plan.PlanHash ||
				topology != table.work.topology {
				return fmt.Errorf(
					"requested incremental plan/topology differs from admission",
				)
			}
			inventory, err := loadStage4WorkInventory(ctx, prepared.run)
			if err != nil {
				return err
			}
			task, found := inventory.tasks[table.work.task]
			ranges := inventory.ranges[table.work.task]
			if !found ||
				task.TopologyHash != table.work.topology ||
				task.Strategy != table.work.strategy ||
				len(ranges) != 1 ||
				ranges[0].ID != stage4AdapterIncrementalRangeID ||
				ranges[0].TopologyHash != table.work.topology ||
				ranges[0].Strategy != table.work.strategy {
				return fmt.Errorf(
					"durable incremental work is not bound to the admitted plan",
				)
			}
			return nil
		},
		Transfer:          transfer,
		PublishCompletion: publisher,
		ArmOnly:           armOnly,
	}
}

func transferStage4AdapterIncrementalTable(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	read IncrementalReadPlan,
) (stage4AdapterIncrementalProgress, error) {
	adapterPlan := prepared.plans[table.planIndex]
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare Stage 4 incremental table "+adapterPlan.source.Name,
		func() error {
			return prepared.incremental.target.PrepareTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(adapterPlan.target),
				},
				"upsert",
			)
		},
	); err != nil {
		return stage4AdapterIncrementalProgress{}, err
	}
	rows, err := prepared.incremental.source.OpenIncrementalRows(
		ctx,
		adapterPlan.source,
		adapterPlan.columns,
		read,
	)
	if err != nil {
		return stage4AdapterIncrementalProgress{}, err
	}
	progress, transferErr := streamStage4AdapterIncrementalRows(
		ctx,
		observer,
		prepared,
		table,
		rows,
	)
	closeErr := rows.Close()
	if transferErr != nil {
		if closeErr != nil {
			return progress, errors.Join(transferErr, closeErr)
		}
		return progress, transferErr
	}
	if closeErr != nil {
		return progress, fmt.Errorf(
			"close Stage 4 incremental source rows for %s: %w",
			adapterPlan.source.Name,
			closeErr,
		)
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"finalize Stage 4 incremental table "+adapterPlan.source.Name,
		func() error {
			return prepared.incremental.target.FinalizeTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(adapterPlan.target),
				},
				"upsert",
			)
		},
	); err != nil {
		return progress, err
	}
	return progress, nil
}

func streamStage4AdapterIncrementalRows(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	rows adapterRows,
) (stage4AdapterIncrementalProgress, error) {
	if rows == nil {
		return stage4AdapterIncrementalProgress{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 incremental source returned nil rows"),
		)
	}
	adapterPlan := prepared.plans[table.planIndex]
	values := make([]any, len(adapterPlan.columns))
	pointers := make([]any, len(values))
	for index := range values {
		pointers[index] = &values[index]
	}
	batch := make([][]any, 0, sqliteWriteBatchSize)
	progress := stage4AdapterIncrementalProgress{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if progress.nextSequence == math.MaxUint64 {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental sequence overflow for %s",
					adapterPlan.source.Name,
				),
			)
		}
		if err := writeStage4AdapterIncrementalBatch(
			ctx,
			observer,
			prepared,
			table,
			progress.nextSequence,
			batch,
		); err != nil {
			return err
		}
		if len(batch) > math.MaxInt-progress.rows {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental row total overflows for %s",
					adapterPlan.source.Name,
				),
			)
		}
		progress.rows += len(batch)
		progress.nextSequence++
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return progress, fmt.Errorf(
				"read PostgreSQL incremental table %s: %w",
				adapterPlan.source.Name,
				err,
			)
		}
		batch = append(batch, cloneAdapterRow(values))
		if len(batch) == sqliteWriteBatchSize {
			if err := flush(); err != nil {
				return progress, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return progress, fmt.Errorf(
			"read PostgreSQL incremental table %s: %w",
			adapterPlan.source.Name,
			err,
		)
	}
	if err := flush(); err != nil {
		return progress, err
	}
	return progress, nil
}

func writeStage4AdapterIncrementalBatch(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	sequence uint64,
	rows [][]any,
) error {
	adapterPlan := prepared.plans[table.planIndex]
	chunkRows := int64(len(rows))
	now := time.Now().UTC()
	intent := state.RangeChunkIntent{
		RunID:        prepared.run.RunID,
		Task:         table.work.task,
		RangeID:      stage4AdapterIncrementalRangeID,
		TopologyHash: table.work.topology,
		Sequence:     sequence,
		ChunkRows:    chunkRows,
		At:           now,
	}
	if err := prepared.run.Backend.BeginRangeChunk(intent); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"persist Stage 4 incremental batch intent for %s: %w",
				adapterPlan.source.Name,
				err,
			),
		)
	}
	if err := prepared.run.Backend.RecordRangeAttempt(
		state.RangeAttempt{
			RunID:        prepared.run.RunID,
			Task:         table.work.task,
			RangeID:      stage4AdapterIncrementalRangeID,
			TopologyHash: table.work.topology,
			Sequence:     sequence,
			At:           time.Now().UTC(),
		},
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"authorize Stage 4 incremental target attempt for %s: %w",
				adapterPlan.source.Name,
				err,
			),
		)
	}
	for offset := 0; offset < len(rows); {
		limit := stage4AdapterIncrementalWriteLimit(prepared.incremental, len(rows)-offset)
		if limit < 1 {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental upsert merge limit is invalid for %s",
					adapterPlan.source.Name,
				),
			)
		}
		end := offset + limit
		if err := writeStage4AdapterIncrementalFragment(
			ctx,
			observer,
			prepared.incremental,
			adapterPlan,
			rows[offset:end],
		); err != nil {
			return err
		}
		offset = end
	}
	if err := prepared.incremental.validator.ValidateStage4IncrementalBatch(
		ctx,
		adapterPlan.target,
		adapterPlan.columns,
		rows,
	); err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"validate exact Stage 4 incremental target values for %s before advancing durable progress: %w",
				adapterPlan.source.Name,
				err,
			),
		)
	}
	if err := prepared.incremental.validation.RecordExactBatch(
		ctx,
		adapterPlan.source,
		adapterPlan.columns,
		rows,
	); err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"record exact Stage 4 incremental validation evidence for %s: %w",
				adapterPlan.source.Name,
				err,
			),
		)
	}
	if _, err := prepared.run.Backend.AcknowledgeRange(
		state.RangeAcknowledgement{
			RunID:        prepared.run.RunID,
			Task:         table.work.task,
			RangeID:      stage4AdapterIncrementalRangeID,
			TopologyHash: table.work.topology,
			Sequence:     sequence,
			ChunkRows:    chunkRows,
			DurableRows:  chunkRows,
			At:           time.Now().UTC(),
		},
	); err != nil {
		return incrementalPostTransferStateError(
			"acknowledge durable incremental batch; resume must reset and replay the full stored window",
			err,
		)
	}
	return nil
}

// stage4AdapterIncrementalWriteLimit splits only a durable target operation;
// the caller retains one immutable full-window intent and acknowledges it only
// after every fragment has committed and the original window is validated.
func stage4AdapterIncrementalWriteLimit(
	incremental *stage4AdapterIncrementalPrepared,
	remaining int,
) int {
	if remaining < 1 {
		return 0
	}
	if incremental == nil || incremental.upsertMergeRows == 0 ||
		incremental.upsertMergeRows >= remaining {
		return remaining
	}
	if incremental.upsertMergeRows < 0 {
		return 0
	}
	return incremental.upsertMergeRows
}

func writeStage4AdapterIncrementalFragment(
	ctx context.Context,
	observer TableObserver,
	incremental *stage4AdapterIncrementalPrepared,
	adapterPlan adapterTablePlan,
	rows [][]any,
) error {
	if incremental == nil || isNilInterface(incremental.target) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 incremental upsert target is unavailable"),
		)
	}
	attempted := int64(len(rows))
	receipt := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	_, writeErr := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"write Stage 4 incremental batch "+adapterPlan.source.Name,
		func() error {
			var err error
			receipt, err = incremental.target.WriteStage4NetworkBatch(
				ctx,
				adapterPlan.target,
				adapterPlan.columns,
				rows,
			)
			return err
		},
	)
	if receiptErr := receipt.Validate(); receiptErr != nil {
		if writeErr != nil {
			receiptErr = errors.Join(receiptErr, writeErr)
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 incremental target returned an invalid receipt for %s: %w",
				adapterPlan.source.Name,
				receiptErr,
			),
		)
	}
	if writeErr != nil {
		switch receipt.Certainty {
		case CommitNotCommitted:
			return writeErr
		case CommitUnknown:
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental write outcome for %s is unknown; reset and replay the full stored window: %w",
					adapterPlan.source.Name,
					writeErr,
				),
			)
		default:
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 incremental write for %s failed after reporting durable work; reset and replay the full stored window: %w",
					adapterPlan.source.Name,
					writeErr,
				),
			)
		}
	}
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptOffset != 0 ||
		receipt.AttemptedRows != attempted ||
		receipt.CommittedRows != attempted {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 incremental target did not durably commit the complete bounded write for %s",
				adapterPlan.source.Name,
			),
		)
	}
	return nil
}

func stage4AdapterIncrementalCompletion(
	prepared stage4AdapterPrepared,
	table stage4AdapterIncrementalTable,
	progress stage4AdapterIncrementalProgress,
	completedAt time.Time,
	commit *state.IncrementalCommit,
) state.Stage4TableCompletion {
	var incremental *state.IncrementalCommit
	if commit != nil {
		copy := *commit
		incremental = &copy
	}
	return state.Stage4TableCompletion{
		RunID:        prepared.run.RunID,
		Table:        table.work.task.Table,
		Task:         table.work.task,
		TopologyHash: table.work.topology,
		Ranges: []state.Stage4RangeCompletion{{
			ID:           stage4AdapterIncrementalRangeID,
			NextSequence: progress.nextSequence,
		}},
		RowsDone:    progress.rows,
		Incremental: incremental,
		CompletedAt: completedAt.UTC(),
	}
}
