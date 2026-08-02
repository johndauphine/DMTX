package migrate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4RebuildNoResetYAMLBackend turns an accidental cleanup of terminal or
// issued work into a visible failure.  Rebuild recovery must either preserve
// exact ranges or rerun the target lifecycle; it never resets work plans.
type stage4RebuildNoResetYAMLBackend struct {
	state.YAMLStore
}

func (backend stage4RebuildNoResetYAMLBackend) ResetWorkPlan(
	state.WorkTask,
	[]state.RangeState,
) error {
	return errors.New("rebuild recovery attempted ResetWorkPlan")
}

// stage4RebuildAggregateWithoutReady deliberately exposes the old aggregate
// surface while hiding the new terminal-ready receipt. It models an external
// state implementation that has not upgraded to the rebuild-recovery
// protocol; a destructive fresh run must reject it before PrepareTables.
type stage4RebuildAggregateWithoutReady struct {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
}

// stage4RebuildCheckpointFaultBackend injects failures only into the fresh
// pre-mutation checkpoint sequence while preserving every capability of the
// underlying YAML or SQLite backend. It lets the same recovery contract run
// against both durable formats.
type stage4RebuildCheckpointFaultBackend struct {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	state.Stage4RebuildRecoveryBackend

	err               error
	failInventory     bool
	failNetworkWorkAt int
	networkWorkCalls  int
}

func (backend *stage4RebuildCheckpointFaultBackend) EnsureStage4TableInventory(
	inventory state.Stage4TableInventory,
) error {
	if backend.failInventory {
		backend.failInventory = false
		return backend.err
	}
	return backend.Stage4AggregateBackend.EnsureStage4TableInventory(inventory)
}

func (backend *stage4RebuildCheckpointFaultBackend) EnsureWorkPlan(
	task state.WorkTask,
	ranges []state.RangeState,
) (bool, error) {
	if task.Key.Type == stage4AdapterNetworkTaskType {
		backend.networkWorkCalls++
		if backend.failNetworkWorkAt > 0 &&
			backend.networkWorkCalls == backend.failNetworkWorkAt {
			backend.failNetworkWorkAt = 0
			return false, backend.err
		}
	}
	return backend.Stage4StateBackend.EnsureWorkPlan(task, ranges)
}

type stage4RebuildBeforeTablesCommitThenFailObserver struct {
	stage4AdapterObserver
	err    error
	failed bool
}

func (observer *stage4RebuildBeforeTablesCommitThenFailObserver) BeforeTables(
	ctx context.Context,
	tables []string,
) error {
	if err := observer.stage4AdapterObserver.BeforeTables(ctx, tables); err != nil {
		return err
	}
	if !observer.failed {
		observer.failed = true
		return observer.err
	}
	return nil
}

// stage4RebuildReadyCommitThenFailBackend models the uncertain state-write
// boundary where durable storage accepted the terminal-ready receipt but the
// caller received an error. Resume must authenticate the committed receipt
// and publish aggregates without rebuilding the already validated target.
type stage4RebuildReadyCommitThenFailBackend struct {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	recovery state.Stage4RebuildRecoveryBackend
	failed   bool
	err      error
}

func (backend *stage4RebuildReadyCommitThenFailBackend) EnsureStage4RebuildFinalization(
	finalization state.Stage4RebuildFinalization,
) (state.Stage4RebuildFinalizationReceipt, bool, error) {
	return backend.recovery.EnsureStage4RebuildFinalization(finalization)
}

func (backend *stage4RebuildReadyCommitThenFailBackend) LoadStage4RebuildFinalization(
	runID string,
	phase state.Stage4RebuildFinalizationPhase,
) (state.Stage4RebuildFinalizationReceipt, bool, error) {
	return backend.recovery.LoadStage4RebuildFinalization(runID, phase)
}

func (backend *stage4RebuildReadyCommitThenFailBackend) SaveStage4RebuildReady(
	ready state.Stage4RebuildReady,
) error {
	if err := backend.recovery.SaveStage4RebuildReady(ready); err != nil {
		return err
	}
	if !backend.failed {
		backend.failed = true
		return backend.err
	}
	return nil
}

func (backend *stage4RebuildReadyCommitThenFailBackend) LoadStage4RebuildReady(
	runID string,
) (state.Stage4RebuildReadyReceipt, bool, error) {
	return backend.recovery.LoadStage4RebuildReady(runID)
}

type stage4RebuildCommitThenFailTarget struct {
	*recordingAdapterTarget
	failNext bool
	err      error
}

// stage4RebuildFailOpenSource faults only the first data-plane stable view.
// Fresh rebuild checkpointing has already persisted the complete inventory and
// work before that view opens, which lets the regression distinguish partial
// target preparation from a target that has a durable write authority.
type stage4RebuildFailOpenSource struct {
	*recordingAdapterSource
	opens  int
	failOn int
	err    error
}

func (source *stage4RebuildFailOpenSource) openStableNetworkTableSource(
	ctx context.Context,
	table schema.Table,
) (*adapterStableNetworkTableSession, error) {
	source.opens++
	if source.opens == source.failOn {
		return nil, source.err
	}
	return source.recordingAdapterSource.openStableNetworkTableSource(ctx, table)
}

// The real SQLite target supplies this orderer. The recorder used by adapter
// tests is intentionally generic, so this wrapper makes the FK fixture model
// the route's parent-before-child write contract without changing unrelated
// recorder tests.
type stage4RebuildFKTarget struct {
	*recordingAdapterTarget
}

func (*stage4RebuildFKTarget) OrderSourceTables(
	_ string,
	tables []schema.Table,
	_ string,
) ([]schema.Table, error) {
	return orderAdapterSourceTablesForMode(tables, "upsert")
}

func (*stage4RebuildCommitThenFailTarget) OrderSourceTables(
	_ string,
	tables []schema.Table,
	_ string,
) ([]schema.Table, error) {
	return orderAdapterSourceTablesForMode(tables, "upsert")
}

func (target *stage4RebuildCommitThenFailTarget) WriteStage4NetworkRebuildBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	receipt, err := target.recordingAdapterTarget.
		WriteStage4NetworkRebuildBatch(ctx, table, columns, mode, rows)
	if err != nil {
		return receipt, err
	}
	if target.failNext && mode == NetworkWriteFreshInsert {
		target.failNext = false
		return receipt, target.err
	}
	return receipt, nil
}

type stage4RebuildToggleValidationTarget struct {
	*recordingAdapterTarget
	short bool
}

// stage4RebuildFinalizationFaultTarget models the one target boundary that
// cannot be replayed blindly: native finalization may have committed even
// though its caller saw an error. Its upsert preflight is the independent,
// read-only authentication used by the recovery protocol.
type stage4RebuildFinalizationFaultTarget struct {
	*recordingAdapterTarget
	finalizedTarget  bool
	failBeforeCommit bool
	failAfterCommit  bool
	failure          error
}

func (*stage4RebuildFinalizationFaultTarget) OrderSourceTables(
	_ string,
	tables []schema.Table,
	_ string,
) ([]schema.Table, error) {
	return orderAdapterSourceTablesForMode(tables, "upsert")
}

func (target *stage4RebuildFinalizationFaultTarget) PreflightTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	if err := target.recordingAdapterTarget.PreflightTables(ctx, tables, mode); err != nil {
		return err
	}
	if mode == "upsert" && !target.finalizedTarget {
		return errors.New("target finalization is not authenticated")
	}
	return nil
}

func (target *stage4RebuildFinalizationFaultTarget) FinalizeTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	if err := target.recordingAdapterTarget.FinalizeTables(ctx, tables, mode); err != nil {
		return err
	}
	if target.failBeforeCommit {
		return target.failure
	}
	target.finalizedTarget = true
	if target.failAfterCommit {
		return target.failure
	}
	return nil
}

func (*stage4RebuildToggleValidationTarget) OrderSourceTables(
	_ string,
	tables []schema.Table,
	_ string,
) ([]schema.Table, error) {
	return orderAdapterSourceTablesForMode(tables, "upsert")
}

func (target *stage4RebuildToggleValidationTarget) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	rows, err := target.recordingAdapterTarget.CountRows(ctx, table)
	if err == nil && target.short && rows > 0 {
		return rows - 1, nil
	}
	return rows, err
}

type stage4RebuildPublicationFaultObserver struct {
	stage4AdapterObserver
	failTable string
	err       error
	failed    bool
}

func (observer *stage4RebuildPublicationFaultObserver) AfterStage4TablePublication(
	ctx context.Context,
	table string,
	rows int,
) error {
	if err := observer.stage4AdapterObserver.AfterStage4TablePublication(
		ctx,
		table,
		rows,
	); err != nil {
		return err
	}
	if !observer.failed && table == observer.failTable {
		observer.failed = true
		return observer.err
	}
	return nil
}

func stage4RebuildStateBackends(
	t *testing.T,
) []struct {
	name string
	new  func(*testing.T) Stage4StateBackend
} {
	t.Helper()
	return []struct {
		name string
		new  func(*testing.T) Stage4StateBackend
	}{
		{
			name: "yaml",
			new: func(t *testing.T) Stage4StateBackend {
				return stage4RebuildNoResetYAMLBackend{YAMLStore: state.YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}}
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) Stage4StateBackend {
				return state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
			},
		},
	}
}

func stage4RebuildCheckpointFaultStateBackend(
	t *testing.T,
	underlying Stage4StateBackend,
	fault error,
) *stage4RebuildCheckpointFaultBackend {
	t.Helper()
	ordinary, ok := underlying.(state.Backend)
	if !ok {
		t.Fatalf("Stage 4 backend %T lacks ordinary task state", underlying)
	}
	aggregate, ok := underlying.(state.Stage4AggregateBackend)
	if !ok {
		t.Fatalf("Stage 4 backend %T lacks aggregate evidence", underlying)
	}
	recovery, ok := underlying.(state.Stage4RebuildRecoveryBackend)
	if !ok {
		t.Fatalf("Stage 4 backend %T lacks rebuild recovery evidence", underlying)
	}
	return &stage4RebuildCheckpointFaultBackend{
		Backend:                      ordinary,
		Stage4StateBackend:           underlying,
		Stage4AggregateBackend:       aggregate,
		Stage4RebuildRecoveryBackend: recovery,
		err:                          fault,
	}
}

func stage4RebuildNetworkWorkCount(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
) int {
	t.Helper()
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, task := range tasks {
		if task.Key.Type == stage4AdapterNetworkTaskType {
			count++
		}
	}
	return count
}

func stage4RebuildFixtureTables() (schema.Table, schema.Table) {
	parent := stage4AdapterTestTable()
	parent.Name = "parents"
	child := stage4AdapterTestTable()
	child.Name = "children"
	child.ForeignKeys = []schema.ForeignKey{{
		Name:              "children_parent_fk",
		Columns:           []string{"id"},
		ReferencedTable:   parent.Name,
		ReferencedColumns: []string{"id"},
	}}
	return parent, child
}

func stage4RebuildFixtureSource(events *[]string) *recordingAdapterSource {
	parent, child := stage4RebuildFixtureTables()
	return &recordingAdapterSource{
		events: events,
		// The source catalog is already topologically ordered, matching the
		// rebuild contract for a target that re-enables FK enforcement on load.
		tables: []schema.Table{parent, child},
		rows:   []string{"one", "two"},
	}
}

func stage4RebuildFixtureConfig(
	t *testing.T,
) config.Config {
	t.Helper()
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "drop_recreate"
	cfg.Migration.Partitions = 2
	cfg.Migration.MaxRetries = 0
	return cfg
}

func stage4RebuildStateBackend(
	t *testing.T,
	backend Stage4StateBackend,
) state.Backend {
	t.Helper()
	result, ok := backend.(state.Backend)
	if !ok {
		t.Fatalf("Stage 4 backend %T does not expose ordinary task state", backend)
	}
	return result
}

func stage4RebuildCompletedCheckpoints(
	t *testing.T,
	backend state.Backend,
	runID string,
) CompletedTableCheckpoints {
	t.Helper()
	tasks, err := backend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	result := make(CompletedTableCheckpoints)
	for _, task := range tasks {
		if task.Status == "completed" {
			result[task.Table] = CompletedTableCheckpoint{Rows: task.RowsDone}
		}
	}
	return result
}

func stage4RebuildResume(
	t *testing.T,
	cfg config.Config,
	backend Stage4StateBackend,
	runID string,
	source sourceAdapter,
	target targetAdapter,
	events *[]string,
) (Result, error) {
	t.Helper()
	ordinary := stage4RebuildStateBackend(t, backend)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: events},
		run:                    stage4LifecycleRunContext(t, backend, runID, true),
	}
	return resumeWithAdapters(
		context.Background(),
		cfg,
		stage4RebuildCompletedCheckpoints(t, ordinary, runID),
		observer,
		observer,
		source,
		target,
	)
}

func stage4RebuildAssertCompleted(
	t *testing.T,
	backend state.Backend,
	runID string,
) {
	t.Helper()
	tasks, err := backend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("ordinary rebuild tasks = %#v", tasks)
	}
	for _, task := range tasks {
		if task.Status != "completed" || task.RowsDone != 2 {
			t.Fatalf("ordinary rebuild task = %#v", task)
		}
	}
	aggregate, ok := backend.(state.Stage4AggregateBackend)
	if !ok {
		t.Fatalf("backend %T lacks aggregate reads", backend)
	}
	receipts, err := aggregate.LoadStage4TableCompletions(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 2 {
		t.Fatalf("rebuild table receipts = %#v", receipts)
	}
}

func TestStage4AdapterRebuildDefersTerminalSentinelsToAggregatePublication(
	t *testing.T,
) {
	for _, backendCase := range stage4RebuildStateBackends(t) {
		backendCase := backendCase
		t.Run(backendCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := backendCase.new(t)
			ordinary := stage4RebuildStateBackend(t, backend)
			runID := "stage4-rebuild-terminal-publication-" + backendCase.name
			initializeStage4LifecycleRun(
				t,
				ordinary,
				runID,
				time.Now().UTC().Add(-time.Minute),
			)
			run := stage4LifecycleRunContext(t, backend, runID, false)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			target := &stage4RebuildFKTarget{
				recordingAdapterTarget: &recordingAdapterTarget{events: &events},
			}
			result, err := migrateWithAdapters(
				context.Background(),
				stage4RebuildFixtureConfig(t),
				observer,
				stage4RebuildFixtureSource(&events),
				target,
			)
			if err != nil {
				t.Fatalf("complete aggregate rebuild route: %v", err)
			}
			if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("aggregate rebuild result = %#v", result)
			}
			assertStage4TerminalSchemaSentinels(t, backend, runID, false)

			published, err := PublishStage4RunCompletion(
				context.Background(),
				run,
				"aggregate rebuild completed",
				time.Now().UTC(),
			)
			if err != nil || !published {
				t.Fatalf(
					"publish aggregate rebuild completion published=%t err=%v",
					published,
					err,
				)
			}
			assertStage4TerminalSchemaSentinels(t, backend, runID, true)
		})
	}
}

func stage4RebuildPreparedNames(t *testing.T, tables []schema.Table) []string {
	t.Helper()
	result := make([]string, len(tables))
	for index, table := range tables {
		result[index] = table.Name
	}
	return result
}

func TestStage4AdapterRebuildRerunsWholeFKSetAfterPartialPrepare(
	t *testing.T,
) {
	for _, testCase := range stage4RebuildStateBackends(t) {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := testCase.new(t)
			ordinary := stage4RebuildStateBackend(t, backend)
			runID := "stage4-rebuild-partial-prepare-" + testCase.name
			initializeStage4LifecycleRun(
				t,
				ordinary,
				runID,
				time.Now().Add(-time.Minute),
			)
			source := stage4RebuildFixtureSource(&events)
			baseTarget := &recordingAdapterTarget{
				events:      &events,
				rowsByTable: map[string]int{"parents": 9, "children": 9},
				prepareErr:  errors.New("forced partial set-wide prepare failure"),
			}
			target := &stage4RebuildFKTarget{recordingAdapterTarget: baseTarget}
			cfg := stage4RebuildFixtureConfig(t)
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithAdapters(
				context.Background(), cfg, freshObserver, source, target,
			); err == nil || !containsString(err.Error(), "rerunning drop_recreate mode") {
				t.Fatalf("partial prepare error = %v", err)
			}
			if len(target.preparedSets) != 1 || !reflect.DeepEqual(
				stage4RebuildPreparedNames(t, target.preparedSets[0]),
				[]string{"parents", "children"},
			) {
				t.Fatalf("initial rebuild prepare sets = %#v", target.preparedSets)
			}
			target.prepareErr = nil
			events = events[:0]
			result, err := stage4RebuildResume(
				t, cfg, backend, runID, source, target, &events,
			)
			if err != nil {
				t.Fatalf("rerun rebuild after partial prepare: %v", err)
			}
			if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("rerun result = %#v", result)
			}
			if len(target.preparedSets) != 2 || !reflect.DeepEqual(
				stage4RebuildPreparedNames(t, target.preparedSets[1]),
				[]string{"parents", "children"},
			) {
				t.Fatalf("recovery did not reprepare the full FK set: %#v", target.preparedSets)
			}
			if target.rowsByTable["parents"] != 2 || target.rowsByTable["children"] != 2 {
				t.Fatalf("rerun rows = %#v", target.rowsByTable)
			}
			stage4RebuildAssertCompleted(t, ordinary, runID)
		})
	}
}

func TestStage4AdapterRebuildRequiresTerminalRecoveryEvidenceBeforePrepare(
	t *testing.T,
) {
	events := make([]string, 0)
	raw := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	backend := stage4RebuildAggregateWithoutReady{
		Backend:                raw,
		Stage4StateBackend:     raw,
		Stage4AggregateBackend: raw,
	}
	runID := "stage4-rebuild-no-terminal-recovery"
	initializeStage4LifecycleRun(t, raw, runID, time.Now().Add(-time.Minute))
	source := stage4RebuildFixtureSource(&events)
	target := &stage4RebuildFKTarget{recordingAdapterTarget: &recordingAdapterTarget{
		events: &events,
	}}
	cfg := stage4RebuildFixtureConfig(t)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, false),
	}
	if _, err := migrateWithAdapters(
		context.Background(), cfg, observer, source, target,
	); err == nil || !containsString(err.Error(), "terminal-recovery evidence") {
		t.Fatalf("fresh rebuild without terminal recovery evidence = %v", err)
	}
	if len(target.preparedSets) != 0 {
		t.Fatalf("missing terminal recovery evidence reached PrepareTables: %#v", target.preparedSets)
	}
	if tasks, err := raw.ListTasks(runID); err != nil || len(tasks) != 0 {
		t.Fatalf("missing terminal recovery evidence mutated ordinary tasks=%#v err=%v", tasks, err)
	}
}

func TestStage4AdapterRebuildCompletesExactPreMutationCheckpointPrefix(
	t *testing.T,
) {
	for _, backendCase := range stage4RebuildStateBackends(t) {
		for _, boundary := range []struct {
			name              string
			inventoryFailure  bool
			beforeTablesFault bool
			workFailureAt     int
			wantInventory     bool
			wantOrdinary      int
			wantNetworkWork   int
		}{
			{
				name:             "before-inventory",
				inventoryFailure: true,
				wantInventory:    false,
			},
			{
				name:              "after-before-tables-commit",
				beforeTablesFault: true,
				wantInventory:     true,
				wantOrdinary:      2,
			},
			{
				name:            "midway-work-plans",
				workFailureAt:   2,
				wantInventory:   true,
				wantOrdinary:    2,
				wantNetworkWork: 1,
			},
		} {
			t.Run(backendCase.name+"/"+boundary.name, func(t *testing.T) {
				events := make([]string, 0)
				underlying := backendCase.new(t)
				fault := errors.New("forced pre-mutation checkpoint boundary failure")
				backend := stage4RebuildCheckpointFaultStateBackend(
					t,
					underlying,
					fault,
				)
				backend.failInventory = boundary.inventoryFailure
				backend.failNetworkWorkAt = boundary.workFailureAt
				runID := "stage4-rebuild-checkpoint-prefix-" +
					backendCase.name + "-" + boundary.name
				initializeStage4LifecycleRun(
					t,
					backend.Backend,
					runID,
					time.Now().Add(-time.Minute),
				)
				source := stage4RebuildFixtureSource(&events)
				target := &stage4RebuildFKTarget{
					recordingAdapterTarget: &recordingAdapterTarget{
						events:      &events,
						rowsByTable: map[string]int{"parents": 9, "children": 9},
					},
				}
				cfg := stage4RebuildFixtureConfig(t)
				baseObserver := stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{events: &events},
					run: stage4LifecycleRunContext(
						t,
						backend,
						runID,
						false,
					),
				}
				var freshObserver TableObserver = baseObserver
				if boundary.beforeTablesFault {
					freshObserver = &stage4RebuildBeforeTablesCommitThenFailObserver{
						stage4AdapterObserver: baseObserver,
						err:                   fault,
					}
				}
				if _, err := migrateWithAdapters(
					context.Background(),
					cfg,
					freshObserver,
					source,
					target,
				); err == nil || !errors.Is(err, fault) {
					t.Fatalf("fresh checkpoint boundary error = %v", err)
				}
				if len(target.preparedSets) != 0 ||
					target.rowsByTable["parents"] != 9 ||
					target.rowsByTable["children"] != 9 {
					t.Fatalf(
						"pre-mutation checkpoint failure reached target: prepared=%#v rows=%#v",
						target.preparedSets,
						target.rowsByTable,
					)
				}
				_, inventoryFound, err := backend.LoadStage4TableInventory(runID)
				if err != nil || inventoryFound != boundary.wantInventory {
					t.Fatalf(
						"inventory found=%t want=%t err=%v",
						inventoryFound,
						boundary.wantInventory,
						err,
					)
				}
				ordinaryTasks, err := backend.ListTasks(runID)
				if err != nil || len(ordinaryTasks) != boundary.wantOrdinary {
					t.Fatalf(
						"ordinary tasks=%#v want=%d err=%v",
						ordinaryTasks,
						boundary.wantOrdinary,
						err,
					)
				}
				if got := stage4RebuildNetworkWorkCount(t, backend, runID); got != boundary.wantNetworkWork {
					t.Fatalf("network work count=%d want=%d", got, boundary.wantNetworkWork)
				}

				events = events[:0]
				result, err := stage4RebuildResume(
					t,
					cfg,
					backend,
					runID,
					source,
					target,
					&events,
				)
				if err != nil {
					t.Fatalf("resume exact pre-mutation checkpoint prefix: %v", err)
				}
				if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
					t.Fatalf("checkpoint-prefix recovery result = %#v", result)
				}
				if len(target.preparedSets) != 1 ||
					target.rowsByTable["parents"] != 2 ||
					target.rowsByTable["children"] != 2 {
					t.Fatalf(
						"checkpoint-prefix recovery lifecycle: prepared=%#v rows=%#v",
						target.preparedSets,
						target.rowsByTable,
					)
				}
				stage4RebuildAssertCompleted(t, backend.Backend, runID)
			})
		}
	}
}

func TestStage4AdapterRebuildRejectsIncompleteCheckpointPrefixWithWriteAuthority(
	t *testing.T,
) {
	for _, backendCase := range stage4RebuildStateBackends(t) {
		t.Run(backendCase.name, func(t *testing.T) {
			events := make([]string, 0)
			underlying := backendCase.new(t)
			fault := errors.New("forced second work-plan failure")
			backend := stage4RebuildCheckpointFaultStateBackend(t, underlying, fault)
			backend.failNetworkWorkAt = 2
			runID := "stage4-rebuild-checkpoint-authority-" + backendCase.name
			initializeStage4LifecycleRun(
				t,
				backend.Backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			source := stage4RebuildFixtureSource(&events)
			target := &stage4RebuildFKTarget{
				recordingAdapterTarget: &recordingAdapterTarget{events: &events},
			}
			cfg := stage4RebuildFixtureConfig(t)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithAdapters(
				context.Background(), cfg, observer, source, target,
			); err == nil || !errors.Is(err, fault) {
				t.Fatalf("fresh partial work-plan error = %v", err)
			}
			workTasks, workRanges, err := backend.ListWork(runID)
			if err != nil {
				t.Fatal(err)
			}
			var networkTask state.WorkTask
			var networkRange state.RangeState
			for _, task := range workTasks {
				if task.Key.Type == stage4AdapterNetworkTaskType {
					networkTask = task
					break
				}
			}
			for _, workRange := range workRanges {
				if workRange.Task == networkTask.Key {
					networkRange = workRange
					break
				}
			}
			if networkTask.RunID == "" || networkRange.ID == "" {
				t.Fatalf("missing partial network evidence: task=%#v range=%#v", networkTask, networkRange)
			}
			if err := backend.BeginRangeChunk(state.RangeChunkIntent{
				RunID:         runID,
				Task:          networkTask.Key,
				RangeID:       networkRange.ID,
				TopologyHash:  networkTask.TopologyHash,
				Sequence:      0,
				ChunkRows:     1,
				EndFrontier:   state.TypedTuple{state.Int64Value(1)},
				FrontierValid: true,
				Fingerprint:   "issued-page-0",
				At:            time.Now().UTC(),
			}); err != nil {
				t.Fatalf("seed issued write authority: %v", err)
			}

			events = events[:0]
			if _, err := stage4RebuildResume(
				t, cfg, backend, runID, source, target, &events,
			); err == nil || !containsString(err.Error(), "carries transfer authority") {
				t.Fatalf("resume incomplete prefix with authority = %v", err)
			}
			if len(target.preparedSets) != 0 {
				t.Fatalf("unsafe prefix reached target preparation: %#v", target.preparedSets)
			}
			if got := stage4RebuildNetworkWorkCount(t, backend, runID); got != 1 {
				t.Fatalf("unsafe prefix manufactured missing work: %d", got)
			}
		})
	}
}

func TestStage4AdapterRebuildRerunsWholeFKSetAfterOpenBeforeFirstWrite(
	t *testing.T,
) {
	for _, testCase := range stage4RebuildStateBackends(t) {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := testCase.new(t)
			ordinary := stage4RebuildStateBackend(t, backend)
			runID := "stage4-rebuild-open-before-write-" + testCase.name
			initializeStage4LifecycleRun(
				t, ordinary, runID, time.Now().Add(-time.Minute),
			)
			baseSource := stage4RebuildFixtureSource(&events)
			source := &stage4RebuildFailOpenSource{
				recordingAdapterSource: baseSource,
				// The fresh checkpoint opens two table-scoped views. The third
				// open is the first post-prepare data-plane view.
				failOn: 3,
				err:    errors.New("forced open after complete rebuild preparation"),
			}
			target := &stage4RebuildFKTarget{recordingAdapterTarget: &recordingAdapterTarget{
				events:      &events,
				rowsByTable: map[string]int{"parents": 9, "children": 9},
			}}
			cfg := stage4RebuildFixtureConfig(t)
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithAdapters(
				context.Background(), cfg, freshObserver, source, target,
			); err == nil || !containsString(err.Error(), "rerunning drop_recreate mode") {
				t.Fatalf("post-prepare open error = %v", err)
			}
			if len(target.preparedSets) != 1 || target.rowsByTable["parents"] != 0 ||
				target.rowsByTable["children"] != 0 {
				t.Fatalf("post-prepare state = prepared=%#v rows=%#v", target.preparedSets, target.rowsByTable)
			}

			source.failOn = 0
			events = events[:0]
			result, err := stage4RebuildResume(
				t, cfg, backend, runID, source, target, &events,
			)
			if err != nil {
				t.Fatalf("rerun after post-prepare source open failure: %v", err)
			}
			if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("rerun result = %#v", result)
			}
			if len(target.preparedSets) != 2 || !reflect.DeepEqual(
				stage4RebuildPreparedNames(t, target.preparedSets[1]),
				[]string{"parents", "children"},
			) {
				t.Fatalf("rerun did not reprepare full FK set: %#v", target.preparedSets)
			}
			stage4RebuildAssertCompleted(t, ordinary, runID)
		})
	}
}

func TestStage4AdapterRebuildReplaysIssuedRangeWithoutRedrop(
	t *testing.T,
) {
	for _, testCase := range stage4RebuildStateBackends(t) {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := testCase.new(t)
			ordinary := stage4RebuildStateBackend(t, backend)
			runID := "stage4-rebuild-issued-replay-" + testCase.name
			initializeStage4LifecycleRun(
				t,
				ordinary,
				runID,
				time.Now().Add(-time.Minute),
			)
			source := stage4RebuildFixtureSource(&events)
			target := &stage4RebuildCommitThenFailTarget{
				recordingAdapterTarget: &recordingAdapterTarget{events: &events},
				failNext:               true,
				err:                    errors.New("forced post-commit write failure"),
			}
			cfg := stage4RebuildFixtureConfig(t)
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithAdapters(
				context.Background(), cfg, freshObserver, source, target,
			); err == nil || !containsString(err.Error(), "duplicate-safe rebuild replay") {
				t.Fatalf("issued range error = %v", err)
			}
			if len(target.preparedSets) != 1 {
				t.Fatalf("initial prepare sets = %#v", target.preparedSets)
			}
			events = events[:0]
			result, err := stage4RebuildResume(
				t, cfg, backend, runID, source, target, &events,
			)
			if err != nil {
				t.Fatalf("replay issued rebuild page: %v", err)
			}
			if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("issued replay result = %#v", result)
			}
			if len(target.preparedSets) != 1 {
				t.Fatalf("issued replay re-dropped target set: %#v", target.preparedSets)
			}
			if target.rowsByTable["parents"] != 2 || target.rowsByTable["children"] != 2 {
				t.Fatalf("issued replay rows = %#v", target.rowsByTable)
			}
			stage4RebuildAssertCompleted(t, ordinary, runID)
		})
	}
}

func TestStage4AdapterRebuildRejectsChangedRecoveryIdentityBeforeMutation(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *config.Config)
	}{
		{
			name: "pagination topology",
			mutate: func(_ *testing.T, cfg *config.Config) {
				cfg.Migration.Partitions++
			},
		},
		{
			name: "target identity",
			mutate: func(t *testing.T, cfg *config.Config) {
				cfg.Target.Database = filepath.Join(t.TempDir(), "other-target.db")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := stage4RebuildNoResetYAMLBackend{YAMLStore: state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}}
			ordinary := stage4RebuildStateBackend(t, backend)
			runID := "stage4-rebuild-identity-" + testCase.name
			initializeStage4LifecycleRun(
				t, ordinary, runID, time.Now().Add(-time.Minute),
			)
			source := stage4RebuildFixtureSource(&events)
			target := &stage4RebuildFKTarget{recordingAdapterTarget: &recordingAdapterTarget{
				events:     &events,
				prepareErr: errors.New("forced partial prepare before recovery admission"),
			}}
			cfg := stage4RebuildFixtureConfig(t)
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithAdapters(
				context.Background(), cfg, freshObserver, source, target,
			); err == nil {
				t.Fatal("partial rebuild unexpectedly succeeded")
			}
			if len(target.preparedSets) != 1 {
				t.Fatalf("initial prepare sets = %#v", target.preparedSets)
			}

			changed := cfg
			testCase.mutate(t, &changed)
			events = events[:0]
			if _, err := stage4RebuildResume(
				t, changed, backend, runID, source, target, &events,
			); err == nil {
				t.Fatalf("changed %s resume unexpectedly succeeded", testCase.name)
			}
			if len(target.preparedSets) != 1 {
				t.Fatalf(
					"changed %s reached recovery target mutation: %#v",
					testCase.name,
					target.preparedSets,
				)
			}
		})
	}
}

func TestStage4AdapterRebuildPublicationRecoveryReusesCommittedReceipt(
	t *testing.T,
) {
	for _, testCase := range stage4RebuildStateBackends(t) {
		for _, failedTable := range []string{"parents", "children"} {
			t.Run(testCase.name+"/"+failedTable, func(t *testing.T) {
				events := make([]string, 0)
				backend := testCase.new(t)
				ordinary := stage4RebuildStateBackend(t, backend)
				runID := "stage4-rebuild-publication-replay-" + testCase.name
				initializeStage4LifecycleRun(
					t,
					ordinary,
					runID,
					time.Now().Add(-time.Minute),
				)
				source := stage4RebuildFixtureSource(&events)
				target := &stage4RebuildFKTarget{
					recordingAdapterTarget: &recordingAdapterTarget{events: &events},
				}
				cfg := stage4RebuildFixtureConfig(t)
				faultObserver := &stage4RebuildPublicationFaultObserver{
					stage4AdapterObserver: stage4AdapterObserver{
						recordingTableObserver: recordingTableObserver{events: &events},
						run:                    stage4LifecycleRunContext(t, backend, runID, false),
					},
					failTable: failedTable,
					err:       errors.New("forced post-aggregate publication callback failure"),
				}
				if _, err := migrateWithAdapters(
					context.Background(), cfg, faultObserver, source, target,
				); err == nil || !containsString(err.Error(), "publication callback failure") {
					t.Fatalf("publication callback error = %v", err)
				}
				aggregate := ordinary.(state.Stage4AggregateBackend)
				before, err := aggregate.LoadStage4TableCompletions(runID)
				wantReceipts := 1
				if failedTable == "children" {
					wantReceipts = 2
				}
				if err != nil || len(before) != wantReceipts {
					t.Fatalf("aggregate receipts before callback replay = %#v err=%v", before, err)
				}
				completedAt := time.Time{}
				for _, receipt := range before {
					if receipt.Completion.Table == failedTable {
						completedAt = receipt.Completion.CompletedAt
					}
				}
				if completedAt.IsZero() {
					t.Fatalf("missing committed receipt for failed callback table %s: %#v", failedTable, before)
				}
				time.Sleep(2 * time.Millisecond)
				events = events[:0]
				result, err := stage4RebuildResume(
					t, cfg, backend, runID, source, target, &events,
				)
				if err != nil {
					t.Fatalf("publication-only rebuild recovery: %v", err)
				}
				if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
					t.Fatalf("publication recovery result = %#v", result)
				}
				if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
					t.Fatalf("publication recovery mutated lifecycle: prepared=%#v finalized=%#v", target.preparedSets, target.finalized)
				}
				after, err := aggregate.LoadStage4TableCompletions(runID)
				if err != nil || len(after) != 2 {
					t.Fatalf("published receipts = %#v err=%v", after, err)
				}
				for _, receipt := range after {
					if receipt.Completion.Table == failedTable && !receipt.Completion.CompletedAt.Equal(completedAt) {
						t.Fatalf("replayed %s receipt changed completion time: before=%s after=%s", failedTable, completedAt, receipt.Completion.CompletedAt)
					}
				}
				stage4RebuildAssertCompleted(t, ordinary, runID)
			})
		}
	}
}

func TestStage4AdapterRebuildRecognizesCommittedReadyReceiptAfterWriteError(
	t *testing.T,
) {
	for _, testCase := range stage4RebuildStateBackends(t) {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			underlying := testCase.new(t)
			ordinary := stage4RebuildStateBackend(t, underlying)
			aggregate, ok := underlying.(state.Stage4AggregateBackend)
			if !ok {
				t.Fatalf("backend %T lacks aggregate evidence", underlying)
			}
			recovery, ok := underlying.(state.Stage4RebuildRecoveryBackend)
			if !ok {
				t.Fatalf("backend %T lacks rebuild-ready evidence", underlying)
			}
			backend := &stage4RebuildReadyCommitThenFailBackend{
				Backend:                ordinary,
				Stage4StateBackend:     underlying,
				Stage4AggregateBackend: aggregate,
				recovery:               recovery,
				err: errors.New(
					"forced acknowledgement loss after rebuild-ready commit",
				),
			}
			runID := "stage4-rebuild-ready-commit-error-" + testCase.name
			initializeStage4LifecycleRun(
				t,
				ordinary,
				runID,
				time.Now().Add(-time.Minute),
			)
			source := stage4RebuildFixtureSource(&events)
			target := &stage4RebuildFKTarget{
				recordingAdapterTarget: &recordingAdapterTarget{events: &events},
			}
			cfg := stage4RebuildFixtureConfig(t)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run: stage4LifecycleRunContext(
					t,
					backend,
					runID,
					false,
				),
			}
			if _, err := migrateWithAdapters(
				context.Background(),
				cfg,
				observer,
				source,
				target,
			); err == nil || !errors.Is(err, backend.err) {
				t.Fatalf("committed-ready write error = %v", err)
			}
			if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
				t.Fatalf(
					"terminal-ready fault lifecycle = prepared=%#v finalized=%#v",
					target.preparedSets,
					target.finalized,
				)
			}
			if _, found, err := recovery.LoadStage4RebuildReady(runID); err != nil || !found {
				t.Fatalf("committed rebuild-ready receipt found=%t err=%v", found, err)
			}

			events = events[:0]
			result, err := stage4RebuildResume(
				t,
				cfg,
				backend,
				runID,
				source,
				target,
				&events,
			)
			if err != nil {
				t.Fatalf("resume committed rebuild-ready receipt: %v", err)
			}
			if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("publication recovery result = %#v", result)
			}
			if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
				t.Fatalf(
					"publication recovery reran target lifecycle: prepared=%#v finalized=%#v",
					target.preparedSets,
					target.finalized,
				)
			}
			stage4RebuildAssertCompleted(t, ordinary, runID)
		})
	}
}

func TestStage4AdapterRebuildFinalizationBoundaryRecovery(
	t *testing.T,
) {
	for _, backendCase := range stage4RebuildStateBackends(t) {
		for _, boundary := range []struct {
			name             string
			failBeforeCommit bool
			failAfterCommit  bool
			resume           bool
		}{
			{
				name:            "committed_finalizer_is_authenticated_and_not_replayed",
				failAfterCommit: true,
				resume:          true,
			},
			{
				name:             "uncertain_finalizer_refuses_without_replay",
				failBeforeCommit: true,
			},
		} {
			t.Run(backendCase.name+"/"+boundary.name, func(t *testing.T) {
				events := make([]string, 0)
				backend := backendCase.new(t)
				ordinary := stage4RebuildStateBackend(t, backend)
				recovery, ok := backend.(state.Stage4RebuildRecoveryBackend)
				if !ok {
					t.Fatalf("backend %T lacks rebuild recovery evidence", backend)
				}
				runID := "stage4-rebuild-finalization-" + backendCase.name + "-" + boundary.name
				initializeStage4LifecycleRun(
					t,
					ordinary,
					runID,
					time.Now().Add(-time.Minute),
				)
				source := stage4RebuildFixtureSource(&events)
				failure := errors.New("forced finalizer acknowledgement failure")
				target := &stage4RebuildFinalizationFaultTarget{
					recordingAdapterTarget: &recordingAdapterTarget{events: &events},
					failBeforeCommit:       boundary.failBeforeCommit,
					failAfterCommit:        boundary.failAfterCommit,
					failure:                failure,
				}
				cfg := stage4RebuildFixtureConfig(t)
				observer := stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{events: &events},
					run:                    stage4LifecycleRunContext(t, backend, runID, false),
				}
				if _, err := migrateWithAdapters(
					context.Background(), cfg, observer, source, target,
				); err == nil || !errors.Is(err, failure) {
					t.Fatalf("fresh finalizer fault = %v", err)
				}
				for _, phase := range []state.Stage4RebuildFinalizationPhase{
					state.Stage4RebuildFinalizationPlanned,
					state.Stage4RebuildFinalizationStarted,
				} {
					receipt, found, err := recovery.LoadStage4RebuildFinalization(
						runID,
						phase,
					)
					if err != nil || !found ||
						receipt.Finalization.InventoryDigest == "" {
						t.Fatalf(
							"%s receipt found=%t receipt=%#v err=%v",
							phase,
							found,
							receipt,
							err,
						)
					}
				}
				if _, found, err := recovery.LoadStage4RebuildReady(runID); err != nil || found {
					t.Fatalf("terminal-ready after finalizer fault found=%t err=%v", found, err)
				}
				if len(target.finalized) != 1 {
					t.Fatalf("fresh finalizer calls = %#v", target.finalized)
				}

				events = events[:0]
				result, err := stage4RebuildResume(
					t,
					cfg,
					backend,
					runID,
					source,
					target,
					&events,
				)
				if !boundary.resume {
					if err == nil || !containsString(err.Error(), "could not be authenticated") {
						t.Fatalf("uncertain finalizer resume error = %v", err)
					}
					if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
						t.Fatalf(
							"uncertain finalizer retried target lifecycle: prepared=%#v finalized=%#v",
							target.preparedSets,
							target.finalized,
						)
					}
					return
				}
				if err != nil {
					t.Fatalf("resume after committed finalizer: %v", err)
				}
				if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
					t.Fatalf("authenticated finalization recovery result = %#v", result)
				}
				if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
					t.Fatalf(
						"authenticated finalizer replayed target lifecycle: prepared=%#v finalized=%#v",
						target.preparedSets,
						target.finalized,
					)
				}
				if !containsString(fmt.Sprint(target.preflighted), "upsert") {
					t.Fatalf("resume did not authenticate finalized target: preflight=%#v", target.preflighted)
				}
				stage4RebuildAssertCompleted(t, ordinary, runID)
			})
		}
	}
}

func TestStage4AdapterRebuildRecoversFinalizeValidationAndAggregateFaults(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name string
		run  func(*testing.T, *recordingAdapterSource, *recordingAdapterTarget, state.YAMLStore, config.Config, string, *[]string)
	}{
		{
			name: "finalize",
			run: func(t *testing.T, source *recordingAdapterSource, target *recordingAdapterTarget, backend state.YAMLStore, cfg config.Config, runID string, events *[]string) {
				target.finalizeErr = errors.New("forced rebuild finalize failure")
				fkTarget := &stage4RebuildFKTarget{recordingAdapterTarget: target}
				observer := stage4AdapterObserver{recordingTableObserver: recordingTableObserver{events: events}, run: stage4LifecycleRunContext(t, backend, runID, false)}
				if _, err := migrateWithAdapters(context.Background(), cfg, observer, source, fkTarget); err == nil {
					t.Fatal("finalize failure unexpectedly succeeded")
				}
				target.finalizeErr = nil
				*events = (*events)[:0]
				result, err := stage4RebuildResume(t, cfg, backend, runID, source, fkTarget, events)
				if err != nil || result != (Result{Tables: 2, Rows: 4, Validated: true}) {
					t.Fatalf("finalize recovery result=%#v err=%v", result, err)
				}
			},
		},
		{
			name: "validation",
			run: func(t *testing.T, source *recordingAdapterSource, target *recordingAdapterTarget, backend state.YAMLStore, cfg config.Config, runID string, events *[]string) {
				validationTarget := &stage4RebuildToggleValidationTarget{recordingAdapterTarget: target, short: true}
				observer := stage4AdapterObserver{recordingTableObserver: recordingTableObserver{events: events}, run: stage4LifecycleRunContext(t, backend, runID, false)}
				if _, err := migrateWithAdapters(context.Background(), cfg, observer, source, validationTarget); err == nil {
					t.Fatal("validation failure unexpectedly succeeded")
				}
				validationTarget.short = false
				*events = (*events)[:0]
				result, err := stage4RebuildResume(t, cfg, backend, runID, source, validationTarget, events)
				if err != nil || result != (Result{Tables: 2, Rows: 4, Validated: true}) {
					t.Fatalf("validation recovery result=%#v err=%v", result, err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
			runID := "stage4-rebuild-" + testCase.name
			initializeStage4LifecycleRun(t, backend, runID, time.Now().Add(-time.Minute))
			source := stage4RebuildFixtureSource(&events)
			target := &recordingAdapterTarget{events: &events}
			cfg := stage4RebuildFixtureConfig(t)
			testCase.run(t, source, target, backend, cfg, runID, &events)
			if len(target.preparedSets) != 1 {
				t.Fatalf("%s recovery re-dropped completed transfer: %#v", testCase.name, target.preparedSets)
			}
			stage4RebuildAssertCompleted(t, backend, runID)
		})
	}

	for _, failedTable := range []string{"parents", "children"} {
		t.Run("aggregate-"+failedTable, func(t *testing.T) {
			events := make([]string, 0)
			raw := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
			runID := "stage4-rebuild-aggregate-" + failedTable
			initializeStage4LifecycleRun(t, raw, runID, time.Now().Add(-time.Minute))
			faultBackend := stage4NetworkFailingAggregateBackend{
				YAMLStore: raw,
				err:       fmt.Errorf("forced %s aggregate completion failure", failedTable),
				table:     failedTable,
			}
			source := stage4RebuildFixtureSource(&events)
			target := &stage4RebuildFKTarget{
				recordingAdapterTarget: &recordingAdapterTarget{events: &events},
			}
			cfg := stage4RebuildFixtureConfig(t)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, faultBackend, runID, false),
			}
			if _, err := migrateWithAdapters(context.Background(), cfg, observer, source, target); err == nil || !containsString(err.Error(), "aggregate completion failure") {
				t.Fatalf("aggregate failure error = %v", err)
			}
			events = events[:0]
			result, err := stage4RebuildResume(t, cfg, raw, runID, source, target, &events)
			if err != nil || result != (Result{Tables: 2, Rows: 4, Validated: true}) {
				t.Fatalf("aggregate recovery result=%#v err=%v", result, err)
			}
			if len(target.preparedSets) != 1 || len(target.finalized) != 1 {
				t.Fatalf("aggregate recovery mutated lifecycle: prepared=%#v finalized=%#v", target.preparedSets, target.finalized)
			}
			stage4RebuildAssertCompleted(t, raw, runID)
		})
	}
}

func containsString(value string, expected string) bool {
	return len(expected) == 0 || (len(value) >= len(expected) &&
		containsStage4RebuildString(value, expected))
}

func containsStage4RebuildString(value string, expected string) bool {
	for index := 0; index+len(expected) <= len(value); index++ {
		if value[index:index+len(expected)] == expected {
			return true
		}
	}
	return false
}
