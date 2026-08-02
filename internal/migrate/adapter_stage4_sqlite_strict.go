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
	"github.com/johndauphine/dmtx/internal/state"
)

// SQLite strict consistency is deliberately table scoped. SQLite cannot hand
// its read transaction to another connection, so every planning and transfer
// read for one table is borrowed from exactly that one transaction.
const stage4SQLiteStrictWorkVersion = 1

type stage4SQLiteStrictTableBinding struct {
	processEpoch string
	reference    string
}

type stage4SQLiteStrictEpochBinding struct {
	tables map[state.TaskKey]stage4SQLiteStrictTableBinding
}

func requireStage4SQLiteStrictRoute(
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	mode string,
) error {
	if !cfg.Migration.StrictConsistency {
		return nil
	}
	if mode != "upsert" || isNilInterface(source) ||
		isNilInterface(target) || source.Engine() != "sqlite" ||
		!stage4AdapterNetworkRelationalEngine(target.Engine()) {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("Stage 4 SQLite strict consistency requires an upsert route to a certified relational or SQLite target"),
		)
	}
	if _, err := stage4SQLiteStrictScope(
		cfg.Migration.StrictConsistencyScope,
	); err != nil {
		return err
	}
	return nil
}

func stage4SQLiteStrictScope(value string) (state.StrictSnapshotScope, error) {
	scope, err := normalizedStrictConsistencyScope(value)
	if err != nil {
		return "", NewTransferError(ErrorClassPolicy, err)
	}
	if scope != config.StrictConsistencyTable {
		return "", NewTransferError(
			ErrorClassPolicy,
			errors.New("Stage 4 SQLite strict consistency supports table scope only"),
		)
	}
	return state.StrictSnapshotTable, nil
}

func newStage4SQLiteProcessEpoch() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", NewTransferError(
			ErrorClassState,
			fmt.Errorf("generate SQLite strict process epoch: %w", err),
		)
	}
	return "sqlite-process-" + hex.EncodeToString(value[:]), nil
}

func newStage4SQLiteStrictEpochBinding(
	epoch string,
	capture StrictConsistencyCapture,
) (stage4SQLiteStrictEpochBinding, error) {
	if err := validateCredentialFreeIdentifier(
		"SQLite strict process epoch",
		epoch,
	); err != nil {
		return stage4SQLiteStrictEpochBinding{}, err
	}
	result := stage4SQLiteStrictEpochBinding{
		tables: make(map[state.TaskKey]stage4SQLiteStrictTableBinding, len(capture.Tables)),
	}
	for _, record := range capture.Tables {
		if _, exists := result.tables[record.Task]; exists {
			return stage4SQLiteStrictEpochBinding{}, errors.New("SQLite strict capture duplicates a table reference")
		}
		if err := validateSnapshotReference(record.SnapshotReference); err != nil {
			return stage4SQLiteStrictEpochBinding{}, err
		}
		result.tables[record.Task] = stage4SQLiteStrictTableBinding{
			processEpoch: epoch,
			reference:    record.SnapshotReference,
		}
	}
	if len(result.tables) == 0 {
		return stage4SQLiteStrictEpochBinding{}, errors.New("SQLite strict capture has no table references")
	}
	return result, nil
}

func (binding stage4SQLiteStrictEpochBinding) merge(
	other stage4SQLiteStrictEpochBinding,
) (stage4SQLiteStrictEpochBinding, error) {
	result := stage4SQLiteStrictEpochBinding{
		tables: make(map[state.TaskKey]stage4SQLiteStrictTableBinding, len(binding.tables)+len(other.tables)),
	}
	for task, value := range binding.tables {
		result.tables[task] = value
	}
	for task, value := range other.tables {
		if _, exists := result.tables[task]; exists {
			return stage4SQLiteStrictEpochBinding{}, fmt.Errorf("SQLite strict epoch binding duplicates task %s", task.Table)
		}
		result.tables[task] = value
	}
	return result, nil
}

func (binding stage4SQLiteStrictEpochBinding) finalizeWork(
	work stage4AdapterWork,
) (stage4AdapterWork, error) {
	table, ok := binding.tables[work.task]
	if !ok {
		return stage4AdapterWork{}, fmt.Errorf("SQLite strict epoch has no snapshot reference for %s", work.task.Table)
	}
	wire := struct {
		Version   int    `json:"version"`
		Base      string `json:"base_topology"`
		Epoch     string `json:"process_epoch"`
		Reference string `json:"snapshot_reference"`
	}{stage4SQLiteStrictWorkVersion, work.topology, table.processEpoch, table.reference}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return stage4AdapterWork{}, fmt.Errorf("encode SQLite strict work identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	work.topology = hex.EncodeToString(digest[:])
	for index := range work.ranges {
		work.ranges[index].TopologyHash = work.topology
	}
	return work, nil
}

func stage4SQLiteStrictSourceRows(
	evidence []state.StrictSnapshotEvidence,
) (map[stage4RichTableKey]int64, error) {
	result := make(map[stage4RichTableKey]int64, len(evidence))
	for _, record := range evidence {
		if record.SourceEngine != "sqlite" ||
			record.Scope != state.StrictSnapshotTable ||
			record.ExactSourceRowCount < 0 {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New("SQLite strict execution returned invalid count evidence"),
			)
		}
		key := stage4RichTableKey{schema: record.Task.Schema, table: record.Task.Table}
		if _, exists := result[key]; exists {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("SQLite strict execution duplicates count evidence for %s", key.table),
			)
		}
		result[key] = record.ExactSourceRowCount
	}
	return result, nil
}

func validateCompletedStage4SQLiteStrictEvidence(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	completed map[string]int,
) (stage4SQLiteStrictEpochBinding, error) {
	binding := stage4SQLiteStrictEpochBinding{
		tables: make(map[state.TaskKey]stage4SQLiteStrictTableBinding, len(completed)),
	}
	if len(completed) == 0 {
		return binding, ctx.Err()
	}
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return stage4SQLiteStrictEpochBinding{}, err
	}
	for index, plan := range prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		task, found := inventory.tasks[prepared.work[index].task]
		if !found || task.Status != "completed" || task.TopologyHash == "" {
			return stage4SQLiteStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("completed SQLite strict table %s lacks exact durable work", plan.source.Name))
		}
		attempt, err := BuildStrictConsistencyAttemptID(task.Key, task.TopologyHash, 0)
		if err != nil {
			return stage4SQLiteStrictEpochBinding{}, err
		}
		evidence, found, err := prepared.run.Backend.LoadStrictSnapshotEvidence(prepared.run.RunID, task.Key, attempt)
		if err != nil {
			return stage4SQLiteStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("load completed SQLite strict evidence for %s: %w", plan.source.Name, err))
		}
		if !found || evidence.RunID != prepared.run.RunID || evidence.Task != task.Key || evidence.AttemptID != attempt || evidence.SourceEngine != "sqlite" || evidence.Scope != state.StrictSnapshotTable || evidence.MigrationEpochID != "" || evidence.ProcessEpoch == "" || evidence.SnapshotReference == "" || evidence.ExactSourceRowCount != int64(rows) || evidence.CapturedAt.IsZero() {
			return stage4SQLiteStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("completed SQLite strict table %s lacks matching immutable snapshot evidence", plan.source.Name))
		}
		if err := validateCredentialFreeIdentifier("completed SQLite strict process epoch", evidence.ProcessEpoch); err != nil {
			return stage4SQLiteStrictEpochBinding{}, err
		}
		if err := validateSnapshotReference(evidence.SnapshotReference); err != nil {
			return stage4SQLiteStrictEpochBinding{}, err
		}
		binding.tables[task.Key] = stage4SQLiteStrictTableBinding{processEpoch: evidence.ProcessEpoch, reference: evidence.SnapshotReference}
	}
	return binding, ctx.Err()
}

func stage4SQLiteStrictPlanningRangeCursor(
	execution *stage4AdapterNetworkExecution,
) func() {
	execution.mu.Lock()
	start := execution.nextGlobalRange
	execution.mu.Unlock()
	return func() {
		execution.mu.Lock()
		execution.nextGlobalRange = start
		execution.mu.Unlock()
	}
}

func planStage4SQLiteStrictNetworkTable(
	ctx context.Context,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *SQLiteStrictConsistencySession,
	planIndex int,
	resume bool,
) (_ StrictConsistencyTable, resultErr error) {
	plan := prepared.plans[planIndex]
	defer stage4SQLiteStrictPlanningRangeCursor(execution)()
	var work stage4AdapterWork
	err := RunSQLiteAdapterStableNetworkReader(ctx, session, prepared.work[planIndex].task, source, plan.source, func(readerCtx context.Context, stable adapterStableNetworkSource) error {
		tableExecution, err := execution.openTable(readerCtx, planIndex, resume, &adapterStableNetworkTableSession{source: stable, readerLimit: 1})
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := tableExecution.Close(); closeErr != nil {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}()
		work = cloneStage4AdapterNetworkWork(tableExecution.work)
		return nil
	})
	if err != nil {
		return StrictConsistencyTable{}, err
	}
	attempt, err := BuildStrictConsistencyAttemptID(work.task, work.topology, 0)
	if err != nil {
		return StrictConsistencyTable{}, fmt.Errorf("build SQLite table-strict work attempt for %s: %w", plan.source.Name, err)
	}
	return StrictConsistencyTable{Task: work.task, AttemptID: attempt, WorkTopologyHash: work.topology}, resultErr
}

func runStage4SQLiteStrictTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *SQLiteStrictConsistencySession,
	planIndex int,
	resume bool,
) (copied int, resultErr error) {
	plan := prepared.plans[planIndex]
	err := RunSQLiteAdapterStableNetworkReader(ctx, session, prepared.work[planIndex].task, source, plan.source, func(readerCtx context.Context, stable adapterStableNetworkSource) error {
		tableExecution, err := execution.openTable(readerCtx, planIndex, false, &adapterStableNetworkTableSession{source: stable, readerLimit: 1})
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := tableExecution.Close(); closeErr != nil {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}()
		copied, err = runStage4AdapterStableNetworkTable(readerCtx, cfg, observer, target, prepared, planIndex, tableExecution, resume)
		return err
	})
	if err != nil {
		return 0, err
	}
	return copied, resultErr
}

func migrateWithStage4SQLiteStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	if err := requireStage4SQLiteStrictRoute(cfg, source, target, prepared.mode); err != nil {
		return Result{}, err
	}
	if execution == nil || !execution.deferred {
		return Result{}, NewTransferError(ErrorClassState, errors.New("SQLite strict composition requires deferred network work"))
	}
	sqlite, ok := source.(*sqliteSourceAdapter)
	if !ok || sqlite == nil || sqlite.database == nil {
		return Result{}, NewTransferError(ErrorClassPolicy, errors.New("SQLite strict composition requires the production SQLite source adapter"))
	}
	if _, err := stage4SQLiteStrictScope(cfg.Migration.StrictConsistencyScope); err != nil {
		return Result{}, err
	}
	opener, err := NewSQLiteStrictConsistencyOpener(sqlite.database)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	// Source capability admission precedes every checkpoint and every target
	// mutation. The longer-lived transaction is still opened again per table
	// immediately before same-view planning.
	if err := opener.Preflight(ctx); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	binding, err := validateCompletedStage4SQLiteStrictEvidence(ctx, prepared, completed)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if len(binding.tables) != 0 {
		execution.finalizeWork = binding.finalizeWork
	}
	if err := execution.prevalidateCompletedTables(ctx, completed); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if len(completed) == len(prepared.plans) {
		result = resultForValidatedAdapterCheckpoints(completed)
		if err := completeStage4SchemaGateSentinels(ctx, prepared.run, prepared.gate, prepared.evolution); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	epoch, err := newStage4SQLiteProcessEpoch()
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if err := checkpointStage4AdapterTableSet(ctx, observer, prepared.names); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	schemaApplied := false
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := execution.advanceCompletedTable(ctx, planIndex, rows); err != nil {
				return result, err
			}
			continue
		}
		strictExecution, err := BeginPlannedStrictConsistency(ctx, PlannedStrictConsistencyRequest{RunID: prepared.run.RunID, SourceEngine: StrictConsistencySQLite, Scope: state.StrictSnapshotTable, Resume: resume, ProcessEpoch: epoch, State: prepared.run.Backend, Tasks: []state.TaskKey{prepared.work[planIndex].task}}, opener, func(planCtx context.Context, raw StrictConsistencySession, capture StrictConsistencyCapture) ([]StrictConsistencyTable, error) {
			session, ok := raw.(*SQLiteStrictConsistencySession)
			if !ok || session == nil {
				return nil, NewTransferError(ErrorClassState, errors.New("SQLite planned table-strict session has an unexpected type"))
			}
			current, err := newStage4SQLiteStrictEpochBinding(epoch, capture)
			if err != nil {
				return nil, err
			}
			next, err := binding.merge(current)
			if err != nil {
				return nil, err
			}
			execution.finalizeWork = next.finalizeWork
			table, err := planStage4SQLiteStrictNetworkTable(planCtx, source, prepared, execution, session, planIndex, resume)
			if err != nil {
				return nil, err
			}
			return []StrictConsistencyTable{table}, nil
		})
		if err != nil {
			return result, err
		}
		strictPrepared := prepared
		strictPrepared.strictSourceRows, err = stage4SQLiteStrictSourceRows(strictExecution.Evidence())
		if err != nil {
			return result, errors.Join(err, strictExecution.Close(ctx))
		}
		current, err := newStage4SQLiteStrictEpochBinding(epoch, strictConsistencyCaptureFromEvidence(strictExecution.Evidence()))
		if err != nil {
			return result, errors.Join(err, strictExecution.Close(ctx))
		}
		next, err := binding.merge(current)
		if err != nil {
			return result, errors.Join(err, strictExecution.Close(ctx))
		}
		resultErr = strictExecution.Run(ctx, func(runCtx context.Context, raw StrictConsistencySession) error {
			session, ok := raw.(*SQLiteStrictConsistencySession)
			if !ok || session == nil {
				return NewTransferError(ErrorClassState, errors.New("SQLite table-strict execution session has an unexpected type"))
			}
			if !schemaApplied {
				if err := applyStage4AdapterTargetSchema(runCtx, observer, strictPrepared.run, strictPrepared.gate, strictPrepared.evolution); err != nil {
					return err
				}
				if err := preflightStage4AdapterDesiredTargetAfterEvolution(runCtx, target, strictPrepared); err != nil {
					return err
				}
				schemaApplied = true
			}
			copied, err := runStage4SQLiteStrictTable(runCtx, cfg, observer, source, target, strictPrepared, execution, session, planIndex, resume)
			if err != nil {
				return err
			}
			if copied < 0 || result.Rows > math.MaxInt-copied {
				return NewTransferError(ErrorClassState, errors.New("SQLite strict result row total overflows"))
			}
			result.Tables++
			result.Rows += copied
			return nil
		})
		if resultErr != nil {
			return result, resultErr
		}
		binding = next
		execution.finalizeWork = binding.finalizeWork
	}
	if err := completeStage4SchemaGateSentinels(ctx, prepared.run, prepared.gate, prepared.evolution); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}
