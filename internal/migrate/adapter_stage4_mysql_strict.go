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
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

// MySQL/MariaDB strict consistency is deliberately table-scoped.  The source
// primitive pins all reader transactions together during LOCK TABLES; this
// adapter layer is responsible for ensuring no ordinary pool read can escape
// that set of transactions.
const stage4MySQLStrictWorkVersion = 1

type stage4MySQLStrictTableBinding struct {
	processEpoch string
	reference    string
}
type stage4MySQLStrictEpochBinding struct {
	tables map[state.TaskKey]stage4MySQLStrictTableBinding
}

func requireStage4MySQLStrictRoute(cfg config.Config, source sourceAdapter, target targetAdapter, mode string) error {
	if !cfg.Migration.StrictConsistency {
		return nil
	}
	if mode != "upsert" || isNilInterface(source) || isNilInterface(target) || source.Engine() != "mysql" || !stage4AdapterNetworkRelationalEngine(target.Engine()) {
		return NewTransferError(ErrorClassPolicy, errors.New("Stage 4 MySQL/MariaDB strict consistency requires an upsert route to a certified relational or SQLite target"))
	}
	scope, err := stage4MySQLStrictScope(cfg.Migration.StrictConsistencyScope)
	if err != nil {
		return err
	}
	if scope != state.StrictSnapshotTable {
		return NewTransferError(ErrorClassPolicy, errors.New("Stage 4 MySQL/MariaDB strict consistency supports table scope only"))
	}
	return nil
}

func stage4MySQLStrictScope(value string) (state.StrictSnapshotScope, error) {
	scope, err := normalizedStrictConsistencyScope(value)
	if err != nil {
		return "", NewTransferError(ErrorClassPolicy, err)
	}
	if scope != config.StrictConsistencyTable {
		return "", NewTransferError(ErrorClassPolicy, errors.New("Stage 4 MySQL/MariaDB strict consistency supports table scope only"))
	}
	return state.StrictSnapshotTable, nil
}

func newStage4MySQLProcessEpoch() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", NewTransferError(ErrorClassState, fmt.Errorf("generate MySQL strict process epoch: %w", err))
	}
	return "mysql-process-" + hex.EncodeToString(value[:]), nil
}

func newStage4MySQLStrictEpochBinding(epoch string, capture StrictConsistencyCapture) (stage4MySQLStrictEpochBinding, error) {
	if err := validateCredentialFreeIdentifier("MySQL strict process epoch", epoch); err != nil {
		return stage4MySQLStrictEpochBinding{}, err
	}
	result := stage4MySQLStrictEpochBinding{tables: make(map[state.TaskKey]stage4MySQLStrictTableBinding, len(capture.Tables))}
	for _, record := range capture.Tables {
		if _, exists := result.tables[record.Task]; exists {
			return stage4MySQLStrictEpochBinding{}, errors.New("MySQL strict capture duplicates a table reference")
		}
		if err := validateSnapshotReference(record.SnapshotReference); err != nil {
			return stage4MySQLStrictEpochBinding{}, err
		}
		result.tables[record.Task] = stage4MySQLStrictTableBinding{processEpoch: epoch, reference: record.SnapshotReference}
	}
	if len(result.tables) == 0 {
		return stage4MySQLStrictEpochBinding{}, errors.New("MySQL strict capture has no table references")
	}
	return result, nil
}

func (binding stage4MySQLStrictEpochBinding) merge(other stage4MySQLStrictEpochBinding) (stage4MySQLStrictEpochBinding, error) {
	result := stage4MySQLStrictEpochBinding{tables: make(map[state.TaskKey]stage4MySQLStrictTableBinding, len(binding.tables)+len(other.tables))}
	for task, value := range binding.tables {
		result.tables[task] = value
	}
	for task, value := range other.tables {
		if _, exists := result.tables[task]; exists {
			return stage4MySQLStrictEpochBinding{}, fmt.Errorf("MySQL strict epoch binding duplicates task %s.%s", task.Schema, task.Table)
		}
		result.tables[task] = value
	}
	return result, nil
}

func (binding stage4MySQLStrictEpochBinding) finalizeWork(work stage4AdapterWork) (stage4AdapterWork, error) {
	table, ok := binding.tables[work.task]
	if !ok {
		return stage4AdapterWork{}, fmt.Errorf("MySQL strict epoch has no snapshot reference for %s.%s", work.task.Schema, work.task.Table)
	}
	wire := struct {
		Version   int    `json:"version"`
		Base      string `json:"base_topology"`
		Epoch     string `json:"process_epoch"`
		Reference string `json:"snapshot_reference"`
	}{stage4MySQLStrictWorkVersion, work.topology, table.processEpoch, table.reference}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return stage4AdapterWork{}, fmt.Errorf("encode MySQL strict work identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	work.topology = hex.EncodeToString(digest[:])
	for index := range work.ranges {
		work.ranges[index].TopologyHash = work.topology
	}
	return work, nil
}

func stage4MySQLStrictSourceRows(evidence []state.StrictSnapshotEvidence) (map[stage4RichTableKey]int64, error) {
	result := make(map[stage4RichTableKey]int64, len(evidence))
	for _, record := range evidence {
		if record.SourceEngine != "mysql" || record.Scope != state.StrictSnapshotTable || record.ExactSourceRowCount < 0 {
			return nil, NewTransferError(ErrorClassState, errors.New("MySQL strict execution returned invalid count evidence"))
		}
		key := stage4RichTableKey{schema: record.Task.Schema, table: record.Task.Table}
		if _, exists := result[key]; exists {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf("MySQL strict execution duplicates count evidence for %s.%s", key.schema, key.table))
		}
		result[key] = record.ExactSourceRowCount
	}
	return result, nil
}

func validateCompletedStage4MySQLStrictEvidence(ctx context.Context, prepared stage4AdapterPrepared, completed map[string]int) (stage4MySQLStrictEpochBinding, error) {
	binding := stage4MySQLStrictEpochBinding{tables: make(map[state.TaskKey]stage4MySQLStrictTableBinding, len(completed))}
	if len(completed) == 0 {
		return binding, ctx.Err()
	}
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return stage4MySQLStrictEpochBinding{}, err
	}
	for index, plan := range prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		task, found := inventory.tasks[prepared.work[index].task]
		if !found || task.Status != "completed" || task.TopologyHash == "" {
			return stage4MySQLStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("completed MySQL strict table %s lacks exact durable work", plan.source.Name))
		}
		attempt, err := BuildStrictConsistencyAttemptID(task.Key, task.TopologyHash, 0)
		if err != nil {
			return stage4MySQLStrictEpochBinding{}, err
		}
		evidence, found, err := prepared.run.Backend.LoadStrictSnapshotEvidence(prepared.run.RunID, task.Key, attempt)
		if err != nil {
			return stage4MySQLStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("load completed MySQL strict evidence for %s: %w", plan.source.Name, err))
		}
		if !found || evidence.RunID != prepared.run.RunID || evidence.Task != task.Key || evidence.AttemptID != attempt || evidence.SourceEngine != "mysql" || evidence.Scope != state.StrictSnapshotTable || evidence.MigrationEpochID != "" || evidence.ProcessEpoch == "" || evidence.SnapshotReference == "" || evidence.ExactSourceRowCount != int64(rows) || evidence.CapturedAt.IsZero() {
			return stage4MySQLStrictEpochBinding{}, NewTransferError(ErrorClassState, fmt.Errorf("completed MySQL strict table %s lacks matching immutable snapshot evidence", plan.source.Name))
		}
		if err := validateCredentialFreeIdentifier("completed MySQL strict process epoch", evidence.ProcessEpoch); err != nil {
			return stage4MySQLStrictEpochBinding{}, err
		}
		if err := validateSnapshotReference(evidence.SnapshotReference); err != nil {
			return stage4MySQLStrictEpochBinding{}, err
		}
		binding.tables[task.Key] = stage4MySQLStrictTableBinding{processEpoch: evidence.ProcessEpoch, reference: evidence.SnapshotReference}
	}
	return binding, ctx.Err()
}

func planStage4MySQLStrictNetworkTable(ctx context.Context, source sourceAdapter, prepared stage4AdapterPrepared, execution *stage4AdapterNetworkExecution, session *MySQLStrictConsistencySession, planIndex int, resume bool) (_ StrictConsistencyTable, resultErr error) {
	plan := prepared.plans[planIndex]
	defer stage4MySQLStrictPlanningRangeCursor(execution)()
	var work stage4AdapterWork
	err := RunMySQLAdapterStableNetworkReader(ctx, session, prepared.work[planIndex].task, source, plan.source, func(readerCtx context.Context, stable adapterStableNetworkSource) error {
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
		return StrictConsistencyTable{}, fmt.Errorf("build MySQL table-strict work attempt for %s: %w", plan.source.Name, err)
	}
	return StrictConsistencyTable{Task: work.task, AttemptID: attempt, WorkTopologyHash: work.topology}, resultErr
}

// stage4MySQLStrictPlanningRangeCursor isolates topology planning from the
// migration-global range cursor. Planning opens the same stable table as the
// later transfer, so its temporary range allocation must never consume a
// durable global index.
func stage4MySQLStrictPlanningRangeCursor(execution *stage4AdapterNetworkExecution) func() {
	execution.mu.Lock()
	start := execution.nextGlobalRange
	execution.mu.Unlock()
	return func() {
		execution.mu.Lock()
		execution.nextGlobalRange = start
		execution.mu.Unlock()
	}
}

func runStage4MySQLStrictTable(ctx context.Context, cfg config.Config, observer TableObserver, source sourceAdapter, target targetAdapter, prepared stage4AdapterPrepared, execution *stage4AdapterNetworkExecution, session *MySQLStrictConsistencySession, planIndex int, resume bool) (copied int, resultErr error) {
	plan := prepared.plans[planIndex]
	err := RunMySQLAdapterStableNetworkReader(ctx, session, prepared.work[planIndex].task, source, plan.source, func(readerCtx context.Context, stable adapterStableNetworkSource) error {
		// The shared parallel wrapper limits active reads to resource readers;
		// each secondary callback obtains a distinct pre-opened snapshot lease.
		parallel, err := newStage4PostgresStrictParallelSource(stable, execution.resources.Readers.Value, func(secondaryCtx context.Context, work func(adapterStableNetworkSource) error) error {
			return RunMySQLAdapterStableNetworkReader(secondaryCtx, session, prepared.work[planIndex].task, source, plan.source, func(_ context.Context, secondary adapterStableNetworkSource) error {
				if err := inheritStage4PostgresStrictSameSnapshotProofs(stable, secondary); err != nil {
					return err
				}
				return work(secondary)
			})
		})
		if err != nil {
			return err
		}
		tableExecution, err := execution.openTable(readerCtx, planIndex, false, &adapterStableNetworkTableSession{source: parallel, readerLimit: execution.resources.Readers.Value})
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

func migrateWithStage4MySQLStrictAdapters(ctx context.Context, cfg config.Config, observer TableObserver, source sourceAdapter, target targetAdapter, prepared stage4AdapterPrepared, execution *stage4AdapterNetworkExecution, resume bool, completed map[string]int) (result Result, resultErr error) {
	if err := requireStage4MySQLStrictRoute(cfg, source, target, prepared.mode); err != nil {
		return Result{}, err
	}
	if execution == nil || !execution.deferred {
		return Result{}, NewTransferError(ErrorClassState, errors.New("MySQL strict composition requires deferred network work"))
	}
	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational == nil || relational.database == nil || relational.spec.engine != "mysql" {
		return Result{}, NewTransferError(ErrorClassPolicy, errors.New("MySQL strict composition requires the production MySQL-family source adapter"))
	}
	var strictEngine StrictConsistencyEngine
	switch relational.mySQLFlavor {
	case engine.MySQLServerFlavorOracle80:
		strictEngine = StrictConsistencyMySQL
	case engine.MySQLServerFlavorMariaDB1011:
		strictEngine = StrictConsistencyMariaDB
	default:
		return Result{}, NewTransferError(ErrorClassPolicy, errors.New("MySQL strict composition requires a verified MySQL 8.0 or MariaDB 10.11 source"))
	}
	if _, err := stage4MySQLStrictScope(cfg.Migration.StrictConsistencyScope); err != nil {
		return Result{}, err
	}
	if err := configureStage4MySQLTableStrictSourcePool(
		relational,
		execution.resources,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	binding, err := validateCompletedStage4MySQLStrictEvidence(ctx, prepared, completed)
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
	epoch, err := newStage4MySQLProcessEpoch()
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	opener, err := NewMySQLStrictConsistencyOpener(relational.database, relational.namespace, strictEngine, execution.resources.Readers.Value)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	preflightTables := make([]StrictConsistencyTable, 0, len(prepared.work))
	for index, plan := range prepared.plans {
		if _, complete := completed[plan.source.Name]; !complete {
			preflightTables = append(preflightTables, StrictConsistencyTable{Task: prepared.work[index].task})
		}
	}
	if err := opener.Preflight(ctx, preflightTables); err != nil {
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
		strictExecution, err := BeginPlannedStrictConsistency(ctx, PlannedStrictConsistencyRequest{RunID: prepared.run.RunID, SourceEngine: strictEngine, Scope: state.StrictSnapshotTable, Resume: resume, ProcessEpoch: epoch, State: prepared.run.Backend, Tasks: []state.TaskKey{prepared.work[planIndex].task}}, opener, func(planCtx context.Context, raw StrictConsistencySession, capture StrictConsistencyCapture) ([]StrictConsistencyTable, error) {
			session, ok := raw.(*MySQLStrictConsistencySession)
			if !ok || session == nil {
				return nil, NewTransferError(ErrorClassState, errors.New("MySQL planned table-strict session has an unexpected type"))
			}
			current, err := newStage4MySQLStrictEpochBinding(epoch, capture)
			if err != nil {
				return nil, err
			}
			next, err := binding.merge(current)
			if err != nil {
				return nil, err
			}
			execution.finalizeWork = next.finalizeWork
			table, err := planStage4MySQLStrictNetworkTable(planCtx, source, prepared, execution, session, planIndex, resume)
			if err != nil {
				return nil, err
			}
			return []StrictConsistencyTable{table}, nil
		})
		if err != nil {
			return result, err
		}
		strictPrepared := prepared
		strictPrepared.strictSourceRows, err = stage4MySQLStrictSourceRows(strictExecution.Evidence())
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			return result, errors.Join(err, closeErr)
		}
		current, err := newStage4MySQLStrictEpochBinding(epoch, strictConsistencyCaptureFromEvidence(strictExecution.Evidence()))
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			return result, errors.Join(err, closeErr)
		}
		next, err := binding.merge(current)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			return result, errors.Join(err, closeErr)
		}
		resultErr = strictExecution.Run(ctx, func(runCtx context.Context, raw StrictConsistencySession) error {
			session, ok := raw.(*MySQLStrictConsistencySession)
			if !ok || session == nil {
				return NewTransferError(ErrorClassState, errors.New("MySQL table-strict execution session has an unexpected type"))
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
			copied, err := runStage4MySQLStrictTable(runCtx, cfg, observer, source, target, strictPrepared, execution, session, planIndex, resume)
			if err != nil {
				return err
			}
			if copied < 0 || result.Rows > math.MaxInt-copied {
				return NewTransferError(ErrorClassState, errors.New("MySQL strict result row total overflows"))
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

// configureStage4MySQLTableStrictSourcePool raises the deliberately
// conservative ordinary MySQL-family source pool only for a table-strict
// epoch.  MySQL/MariaDB need one physical connection to hold LOCK TABLES and
// one retained consistent-snapshot connection for every admitted reader. The
// target has its own pool, so this source-local envelope must stay within the
// resource plan's source connection limit.
func configureStage4MySQLTableStrictSourcePool(
	source *relationalSourceAdapter,
	resources config.EffectiveTransferPlan,
) error {
	if source == nil || source.database == nil || source.spec.engine != "mysql" {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("MySQL table-strict source pool requires the production MySQL-family source adapter"),
		)
	}
	if resources.Readers.Value < 1 {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("MySQL table-strict source pool requires at least one admitted reader"),
		)
	}
	ownerAndReaders := resources.Readers.Value + 1
	if resources.ConnectionLimit.Value < ownerAndReaders {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"MySQL table-strict source pool requires %d connections for its lock holder and admitted readers within connection_limit",
				ownerAndReaders,
			),
		)
	}
	source.database.SetMaxOpenConns(ownerAndReaders)
	source.database.SetMaxIdleConns(ownerAndReaders)
	return nil
}
