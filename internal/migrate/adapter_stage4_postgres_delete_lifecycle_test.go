package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4PostgresDeleteLifecycleOrdersGlobalPhasesAndPreservesTransferredResume(
	t *testing.T,
) {
	fixture := newStage4PostgresDeleteLifecycleFixture(
		t,
		"delete-lifecycle-order",
		[]string{"parent", "child"},
	)
	fixture.completeAllRanges(t)
	fixture.log.clear()

	if err := checkpointStage4AdapterStableNetworkWork(
		context.Background(),
		fixture.observer,
		fixture.execution,
		true,
		nil,
	); err != nil {
		t.Fatalf("checkpoint fully transferred resume: %v", err)
	}
	if fixture.sourceStableOpens() != 0 ||
		fixture.backend.resetCalls() != 0 ||
		fixture.target.prepareCalls() != 0 ||
		fixture.target.writeCalls() != 0 {
		t.Fatalf(
			"fully transferred resume reopened/reset/recopied: events=%v opens=%d resets=%d prepare=%d writes=%d",
			fixture.log.snapshot(),
			fixture.sourceStableOpens(),
			fixture.backend.resetCalls(),
			fixture.target.prepareCalls(),
			fixture.target.writeCalls(),
		)
	}

	result, err := runStage4AdapterPostgresDeleteNetworkTables(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.target,
		fixture.prepared,
		fixture.execution,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("run resumed PostgreSQL delete lifecycle: %v", err)
	}
	if result != (Result{Tables: 2, Rows: 0}) {
		t.Fatalf("result = %#v", result)
	}
	if fixture.target.prepareCalls() != 0 ||
		fixture.target.writeCalls() != 0 {
		t.Fatalf(
			"preserved resume recopied target: prepare=%d writes=%d",
			fixture.target.prepareCalls(),
			fixture.target.writeCalls(),
		)
	}

	events := fixture.log.snapshot()
	assertStage4DeleteLifecycleBefore(
		t,
		events,
		"finalize:parent",
		"finalize:child",
	)
	assertStage4DeleteLifecycleBefore(
		t,
		events,
		"finalize:child",
		"delete-source-open:child",
	)
	assertStage4DeleteLifecycleBefore(
		t,
		events,
		"delete-target-close:child",
		"delete-source-open:parent",
	)
	firstValidation := firstStage4DeleteLifecycleEvent(
		events,
		"validation:",
	)
	if firstValidation < 0 {
		t.Fatalf("validation did not run: %v", events)
	}
	for _, event := range []string{
		"delete-target-close:child",
		"delete-target-close:parent",
	} {
		index := stage4DeleteLifecycleEventIndex(events, event)
		if index < 0 || index >= firstValidation {
			t.Fatalf("delete %q did not precede validation: %v", event, events)
		}
	}
	for _, table := range []string{"parent", "child"} {
		complete := stage4DeleteLifecycleEventIndex(
			events,
			"work-complete:"+table,
		)
		after := stage4DeleteLifecycleEventIndex(events, "after:"+table)
		if complete <= firstValidation || after <= complete {
			t.Fatalf(
				"validation/work/AfterTable order for %s is unsafe: %v",
				table,
				events,
			)
		}
	}
}

func TestStage4PostgresDeleteLifecycleAuthenticatesCurrentAuthorityBeforeRefinalize(
	t *testing.T,
) {
	tests := map[string]struct {
		terminalSpool bool
		seed          func(
			*testing.T,
			Stage4StateBackend,
			deleteReconcileRequest,
			deleteKeyPlan,
		)
	}{
		"missing attempt": {
			seed: func(
				*testing.T,
				Stage4StateBackend,
				deleteReconcileRequest,
				deleteKeyPlan,
			) {
			},
		},
		"running attempt": {
			seed: func(
				t *testing.T,
				backend Stage4StateBackend,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				stage4PostgresDeleteRunnerSeedRunningAuthority(
					t,
					backend,
					request,
					request.AttemptID,
					keyPlan,
					request.Now.Add(-2*time.Minute),
				)
			},
		},
		"completed attempt": {
			terminalSpool: true,
			seed: func(
				t *testing.T,
				backend Stage4StateBackend,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				stage4PostgresDeleteRunnerSeedCompletedAuthority(
					t,
					backend,
					request,
					request.AttemptID,
					keyPlan,
					request.Now.Add(-time.Minute),
				)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newStage4PostgresDeleteLifecycleFixture(
				t,
				"delete-lifecycle-pre-finalize-"+
					strings.ReplaceAll(name, " ", "-"),
				[]string{"items"},
			)
			fixture.completeAllRanges(t)
			fixture.bindAllTransferred(t)
			bound, found := fixture.execution.
				stage4AdapterPostgresDeleteTransferredTable(0)
			if !found || bound.taskCompleted {
				t.Fatalf("running transferred binding = %#v, found=%v", bound, found)
			}
			entry := fixture.prepared.deletes.entries[0]
			_, request, err := fixture.prepared.deletes.requestFor(
				entry,
				bound.work,
			)
			if err != nil {
				t.Fatal(err)
			}
			originalPlan, err := validateDeleteReconcileRequest(
				request,
				entry.capabilities.canonicalizer,
			)
			if err != nil {
				t.Fatal(err)
			}
			test.seed(t, fixture.backend, request, originalPlan)
			terminalSpoolPath := request.SpoolDirectory +
				"/plan-" + request.AttemptID
			if test.terminalSpool {
				if err := os.WriteFile(
					terminalSpoolPath,
					[]byte("crash-leftover"),
					0o600,
				); err != nil {
					t.Fatal(err)
				}
			}

			fixture.prepared.deletes.entries[0].currentAuthority = func(
				context.Context,
			) (deleteKeyCanonicalizer, error) {
				return &stage4PostgresDeleteRunnerAuthorityCanonicalizer{
					id: "postgres-test-authority-recreated-before-finalize",
				}, nil
			}
			fixture.log.clear()

			result, err := runStage4AdapterPostgresDeleteNetworkTables(
				context.Background(),
				fixture.cfg,
				fixture.observer,
				fixture.target,
				fixture.prepared,
				fixture.execution,
				true,
				nil,
			)
			if err == nil ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), "authority") {
				t.Fatalf(
					"pre-finalize authority drift error = %v; result=%#v",
					err,
					result,
				)
			}
			if result != (Result{}) {
				t.Fatalf("pre-finalize authority drift result = %#v", result)
			}
			if test.terminalSpool {
				if _, statErr := os.Stat(terminalSpoolPath); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf(
						"pre-finalize terminal spool was not cleaned: %v",
						statErr,
					)
				}
			}
			if fixture.observer.mutationCalls() != 0 ||
				fixture.target.prepareCalls() != 0 ||
				fixture.target.writeCalls() != 0 {
				t.Fatalf(
					"pre-finalize rejection mutated target: protector=%d prepare=%d write=%d",
					fixture.observer.mutationCalls(),
					fixture.target.prepareCalls(),
					fixture.target.writeCalls(),
				)
			}
			events := fixture.log.snapshot()
			for _, forbidden := range []string{
				"before:",
				"finalize:",
				"delete-source-open:",
				"delete-target-open:",
				"checkpoint-count:",
				"validation:",
			} {
				if firstStage4DeleteLifecycleEvent(events, forbidden) >= 0 {
					t.Fatalf(
						"pre-finalize authority rejection reached %q: %v",
						forbidden,
						events,
					)
				}
			}
		})
	}
}

func TestStage4PostgresDeleteLifecycleAllowsPlanlessRunningAttemptAfterFreshAuthority(
	t *testing.T,
) {
	fixture := newStage4PostgresDeleteLifecycleFixture(
		t,
		"delete-lifecycle-pre-finalize-planless-running",
		[]string{"items"},
	)
	fixture.completeAllRanges(t)
	fixture.bindAllTransferred(t)
	bound, found := fixture.execution.
		stage4AdapterPostgresDeleteTransferredTable(0)
	if !found || bound.taskCompleted {
		t.Fatalf("running transferred binding = %#v, found=%v", bound, found)
	}
	entry := fixture.prepared.deletes.entries[0]
	_, request, err := fixture.prepared.deletes.requestFor(
		entry,
		bound.work,
	)
	if err != nil {
		t.Fatal(err)
	}
	stored, created, err := fixture.backend.BeginDeleteReconciliation(
		state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			Due:       true,
			StartedAt: request.Now.Add(-2 * time.Minute),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !created || stored.Status != state.DeleteReconciliationRunning ||
		stored.Plan != nil {
		t.Fatalf("planless running attempt = %#v, created=%v", stored, created)
	}
	fixture.log.clear()

	result, err := runStage4AdapterPostgresDeleteNetworkTables(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.target,
		fixture.prepared,
		fixture.execution,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("resume planless running delete attempt: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 0}) {
		t.Fatalf("planless running resume result = %#v", result)
	}
	if fixture.observer.mutationCalls() == 0 {
		t.Fatal("planless running resume did not reach protected finalization")
	}
	events := fixture.log.snapshot()
	assertStage4DeleteLifecycleBefore(t, events, "before:items", "finalize:items")
	assertStage4DeleteLifecycleBefore(
		t,
		events,
		"finalize:items",
		"delete-source-open:items",
	)
	terminal := fixture.loadDeleteRecord(t, 0)
	if terminal.Status != state.DeleteReconciliationCompleted ||
		terminal.Plan == nil {
		t.Fatalf("planless running attempt was not completed safely: %#v", terminal)
	}
}

func TestStage4PostgresDeleteLifecyclePropagatesCompletedAndNotDueStrictness(
	t *testing.T,
) {
	t.Run("completed and not-due pass their exact count contracts", func(t *testing.T) {
		fixture := newStage4PostgresDeleteLifecycleFixture(
			t,
			"delete-lifecycle-strictness",
			[]string{"not_due", "completed"},
		)
		fixture.completeAllRanges(t)
		fixture.bindAllTransferred(t)
		fixture.seedNotDue(t, 0)
		fixture.validation.targetRows["not_due"] = 1
		fixture.target.rows["not_due"] = 1

		result, err := runStage4AdapterPostgresDeleteNetworkTables(
			context.Background(),
			fixture.cfg,
			fixture.observer,
			fixture.target,
			fixture.prepared,
			fixture.execution,
			false,
			nil,
		)
		if err != nil {
			t.Fatalf("run mixed terminal outcomes: %v", err)
		}
		if result != (Result{Tables: 2, Rows: 0}) {
			t.Fatalf("result = %#v", result)
		}
		notDue := fixture.loadDeleteRecord(t, 0)
		completed := fixture.loadDeleteRecord(t, 1)
		if notDue.Status != state.DeleteReconciliationNotDue ||
			completed.Status != state.DeleteReconciliationCompleted {
			t.Fatalf(
				"terminal statuses = %q, %q",
				notDue.Status,
				completed.Status,
			)
		}
	})

	t.Run("completed outcome makes upsert validation exact", func(t *testing.T) {
		fixture := newStage4PostgresDeleteLifecycleFixture(
			t,
			"delete-lifecycle-completed-exact",
			[]string{"completed"},
		)
		fixture.completeAllRanges(t)
		fixture.bindAllTransferred(t)
		fixture.validation.targetRows["completed"] = 1

		_, err := runStage4AdapterPostgresDeleteNetworkTables(
			context.Background(),
			fixture.cfg,
			fixture.observer,
			fixture.target,
			fixture.prepared,
			fixture.execution,
			false,
			nil,
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassValidation {
			t.Fatalf(
				"completed reconciliation did not require exact validation: %v",
				err,
			)
		}
	})
}

func TestStage4PostgresDeleteLifecycleRecoversCompletedTaskMissingAfterTable(
	t *testing.T,
) {
	fixture := newStage4PostgresDeleteLifecycleFixture(
		t,
		"delete-lifecycle-missing-after",
		[]string{"items"},
	)
	fixture.completeAllRanges(t)
	bound := fixture.classifyTransferred(t, 0)
	result, err := fixture.prepared.deletes.reconcile(
		context.Background(),
		[]stage4AdapterWork{bound.work},
	)
	if err != nil {
		t.Fatalf("seed terminal delete pass: %v", err)
	}
	if !result.strictByTable[stage4RichTableKey{
		schema: "public",
		table:  "items",
	}] {
		t.Fatalf("seeded terminal result = %#v", result)
	}
	if err := fixture.backend.CompleteWorkTask(
		fixture.run.RunID,
		bound.work.task,
		bound.work.topology,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	fixture.log.clear()

	if err := checkpointStage4AdapterStableNetworkWork(
		context.Background(),
		fixture.observer,
		fixture.execution,
		true,
		nil,
	); err != nil {
		t.Fatalf("authenticate completed task without AfterTable: %v", err)
	}
	got, err := runStage4AdapterPostgresDeleteNetworkTables(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.target,
		fixture.prepared,
		fixture.execution,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("recover completed task without AfterTable: %v", err)
	}
	if got != (Result{Tables: 1, Rows: 0}) {
		t.Fatalf("result = %#v", got)
	}
	events := fixture.log.snapshot()
	if !reflect.DeepEqual(
		stage4DeleteLifecycleEventsWithPrefix(events, "after:"),
		[]string{"after:items"},
	) {
		t.Fatalf("AfterTable recovery events = %v", events)
	}
	for _, forbidden := range []string{
		"stable-open:",
		"work-reset:",
		"finalize:",
		"delete-source-open:",
		"target-prepare:",
		"target-write:",
	} {
		if firstStage4DeleteLifecycleEvent(events, forbidden) >= 0 {
			t.Fatalf("missing-After recovery repeated %q work: %v", forbidden, events)
		}
	}
}

func TestStage4PostgresDeleteLifecycleRejectsOrdinaryCompletedWithoutUsableTerminalEvidence(
	t *testing.T,
) {
	tests := map[string]struct {
		status state.DeleteReconciliationStatus
		found  bool
		want   string
	}{
		"missing": {
			found: false,
			want:  "lacks terminal delete reconciliation evidence",
		},
		"running": {
			found:  true,
			status: state.DeleteReconciliationRunning,
			want:   "remains running",
		},
		"incomplete": {
			found:  true,
			status: state.DeleteReconciliationIncomplete,
			want:   "already incomplete",
		},
		"dry run": {
			found:  true,
			status: state.DeleteReconciliationDryRun,
			want:   "dry-run evidence",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newStage4PostgresDeleteLifecycleFixture(
				t,
				"delete-lifecycle-terminal-"+strings.ReplaceAll(name, " ", "-"),
				[]string{"items"},
			)
			fixture.completeAllRanges(t)
			bound := fixture.classifyTransferred(t, 0)
			if err := fixture.backend.CompleteWorkTask(
				fixture.run.RunID,
				bound.work.task,
				bound.work.topology,
				time.Now().UTC(),
			); err != nil {
				t.Fatal(err)
			}
			attemptID, err := stage4AdapterPostgresDeleteAttemptID(
				fixture.run.RunID,
				bound.work,
			)
			if err != nil {
				t.Fatal(err)
			}
			fixture.backend.injectDeleteRecord(
				attemptID,
				stage4DeleteLifecycleTerminalRecord(
					fixture.run.RunID,
					bound.work.task,
					attemptID,
					test.status,
				),
				test.found,
			)
			fixture.log.clear()

			err = checkpointStage4AdapterStableNetworkWork(
				context.Background(),
				fixture.observer,
				fixture.execution,
				true,
				map[string]int{"items": 0},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("terminal evidence error = %v; want %q", err, test.want)
			}
			if fixture.sourceStableOpens() != 0 ||
				fixture.backend.resetCalls() != 0 {
				t.Fatalf(
					"terminal rejection reopened/reset work: %v",
					fixture.log.snapshot(),
				)
			}
		})
	}
}

func TestStage4PostgresDeleteLifecyclePartialWorkResetBoundary(t *testing.T) {
	t.Run("partial work without delete attempt is reset", func(t *testing.T) {
		fixture := newStage4PostgresDeleteLifecycleFixture(
			t,
			"delete-lifecycle-partial-reset",
			[]string{"items"},
		)
		fixture.log.clear()
		if err := checkpointStage4AdapterStableNetworkWork(
			context.Background(),
			fixture.observer,
			fixture.execution,
			true,
			nil,
		); err != nil {
			t.Fatalf("reset partial work: %v", err)
		}
		if fixture.backend.resetCalls() != 1 ||
			fixture.sourceStableOpens() != 1 {
			t.Fatalf(
				"partial reset calls: opens=%d resets=%d events=%v",
				fixture.sourceStableOpens(),
				fixture.backend.resetCalls(),
				fixture.log.snapshot(),
			)
		}
	})

	t.Run("partial work with delete attempt fails before reset", func(t *testing.T) {
		fixture := newStage4PostgresDeleteLifecycleFixture(
			t,
			"delete-lifecycle-partial-attempt",
			[]string{"items"},
		)
		inventory, err := loadStage4WorkInventory(
			context.Background(),
			fixture.run,
		)
		if err != nil {
			t.Fatal(err)
		}
		work, err := fixture.execution.reconstructCompletedTableWork(
			0,
			inventory,
		)
		if err != nil {
			t.Fatal(err)
		}
		attemptID, err := stage4AdapterPostgresDeleteAttemptID(
			fixture.run.RunID,
			work,
		)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now().UTC().Add(-time.Minute)
		if _, _, err := fixture.backend.BeginDeleteReconciliation(
			state.DeleteReconciliation{
				RunID: fixture.run.RunID,
				Task:  work.task, AttemptID: attemptID,
				Due: true, StartedAt: started,
			},
		); err != nil {
			t.Fatal(err)
		}
		fixture.log.clear()

		err = checkpointStage4AdapterStableNetworkWork(
			context.Background(),
			fixture.observer,
			fixture.execution,
			true,
			nil,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "already has delete attempt") ||
			!strings.Contains(err.Error(), "orphan durable delete evidence") {
			t.Fatalf("partial delete-attempt error = %v", err)
		}
		if fixture.backend.resetCalls() != 0 ||
			fixture.sourceStableOpens() != 0 {
			t.Fatalf(
				"unsafe partial attempt was reset/reopened: %v",
				fixture.log.snapshot(),
			)
		}
	})
}

type stage4DeleteLifecycleFixture struct {
	t          *testing.T
	run        Stage4RunContext
	cfg        config.Config
	log        *stage4DeleteLifecycleLog
	backend    *stage4DeleteLifecycleBackend
	source     *recordingAdapterSource
	target     *stage4DeleteLifecycleTarget
	observer   *stage4DeleteLifecycleObserver
	validation *stage4DeleteLifecycleValidationProbe
	prepared   stage4AdapterPrepared
	execution  *stage4AdapterNetworkExecution
}

func newStage4PostgresDeleteLifecycleFixture(
	t *testing.T,
	runID string,
	names []string,
) *stage4DeleteLifecycleFixture {
	t.Helper()
	if len(names) == 0 {
		t.Fatal("delete lifecycle fixture requires tables")
	}
	log := &stage4DeleteLifecycleLog{}
	run := newNetworkStateTestRun(t, "sqlite", runID)
	durableRunBackend, ok := run.Backend.(state.Backend)
	if !ok {
		t.Fatal("delete lifecycle fixture backend cannot initialize a run")
	}
	initializeStage4LifecycleRun(
		t,
		durableRunBackend,
		runID,
		time.Now().UTC().Add(-time.Hour),
	)
	backend := &stage4DeleteLifecycleBackend{
		Stage4StateBackend: run.Backend,
		log:                log,
		injected: make(
			map[string]stage4DeleteLifecycleInjectedRecord,
		),
	}
	run.Backend = backend

	sourceTables := make([]schema.Table, len(names))
	targetTables := make([]schema.Table, len(names))
	plans := make([]adapterTablePlan, len(names))
	sourceCatalog := make(
		map[stage4RichTableKey]schema.Table,
		len(names),
	)
	for index, name := range names {
		sourceTable := stage4AdapterTestTable()
		sourceTable.Name = name
		targetTable := cloneStage4RichTable(sourceTable)
		targetTable.Schema = "target"
		sourceTables[index] = sourceTable
		targetTables[index] = targetTable
		plans[index] = adapterTablePlan{
			source:  sourceTable,
			target:  targetTable,
			columns: adapterColumnNames(sourceTable),
		}
		sourceCatalog[stage4RichTableKey{
			schema: sourceTable.Schema,
			table:  sourceTable.Name,
		}] = cloneStage4RichTable(sourceTable)
	}
	work, err := buildStage4AdapterWork(
		strings.Repeat("a", 64),
		"upsert",
		plans,
	)
	if err != nil {
		t.Fatal(err)
	}
	discardedSourceEvents := make([]string, 0)
	source := &recordingAdapterSource{
		events: &discardedSourceEvents,
		tables: sourceTables,
		rows:   []string{},
		beforeStableOpen: func(source *recordingAdapterSource) {
			definitions := source.definitions()
			if len(definitions) == 0 {
				log.add("stable-open:unknown")
				return
			}
			// openStableNetworkTableSource invokes this hook before table
			// selection. Infer the next table from prior stable-open events.
			opened := len(log.withPrefix("stable-open:"))
			name := definitions[opened%len(definitions)].Name
			log.add("stable-open:" + name)
		},
	}
	target := &stage4DeleteLifecycleTarget{
		log:  log,
		rows: make(map[string]int, len(names)),
	}
	observer := &stage4DeleteLifecycleObserver{log: log}
	validation := &stage4DeleteLifecycleValidationProbe{
		log:        log,
		sourceRows: make(map[string]int64, len(names)),
		targetRows: make(map[string]int64, len(names)),
	}
	prepared := stage4AdapterPrepared{
		run:           run,
		gate:          Stage4SchemaGateResult{ValidationTables: sourceTables},
		configDigest:  strings.Repeat("a", 64),
		mode:          "upsert",
		plans:         plans,
		names:         append([]string(nil), names...),
		targetTables:  targetTables,
		validation:    validation,
		sourceCatalog: sourceCatalog,
		work:          work,
	}
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.Partitions = 1
	cfg.Migration.MemoryCeilingBytes = 64 << 20
	cfg.Migration.Validation.FailOnMismatch = true
	cfg.Migration.Deletes = config.DeletePolicy{
		Mode:           config.DeleteModeReconcile,
		TargetBehavior: config.DeleteTargetHard,
		Reconcile: config.DeleteReconcilePolicy{
			Schedule:          config.DeleteScheduleInterval,
			Interval:          24 * time.Hour,
			BatchSize:         100,
			RequirePrimaryKey: true,
		},
	}
	entries := make([]stage4AdapterPostgresDeleteEntry, len(plans))
	for index, plan := range plans {
		canonicalizer := &deleteTestCanonicalizer{}
		entry := stage4AdapterPostgresDeleteEntry{
			planIndex: index,
			source:    cloneStage4RichTable(plan.source),
			target:    cloneStage4RichTable(plan.target),
			capabilities: postgresDeleteReconciliationCapabilities{
				source: &stage4DeleteLifecycleKeySource{
					table: plan.source.Name,
					log:   log,
				},
				target: &stage4DeleteLifecycleKeyTarget{
					table: plan.source.Name,
					log:   log,
				},
				canonicalizer: canonicalizer,
			},
		}
		entry.currentAuthority = func(
			context.Context,
		) (deleteKeyCanonicalizer, error) {
			return canonicalizer, nil
		}
		entries[index] = entry
	}
	fixedNow := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	prepared.deletes = &stage4AdapterPostgresDeletePrepared{
		run:           run,
		policy:        cfg.Migration.Deletes,
		maxBatchBytes: 8 << 20,
		protector:     observer,
		entries:       entries,
		now:           func() time.Time { return fixedNow },
	}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		prepared,
		&resources,
		withStage4DeleteReconciliationComposition(),
	)
	if err != nil {
		t.Fatalf("admit delete lifecycle fixture: %v", err)
	}
	if !execution.deferred {
		t.Fatal("delete lifecycle fixture did not use deferred stable work")
	}
	if err := checkpointStage4AdapterStableNetworkWork(
		context.Background(),
		observer,
		execution,
		false,
		nil,
	); err != nil {
		t.Fatalf("checkpoint initial delete lifecycle work: %v", err)
	}
	log.clear()
	return &stage4DeleteLifecycleFixture{
		t: t, run: run, cfg: cfg, log: log, backend: backend,
		source: source, target: target, observer: observer,
		validation: validation, prepared: execution.prepared,
		execution: execution,
	}
}

func (fixture *stage4DeleteLifecycleFixture) completeAllRanges(t *testing.T) {
	t.Helper()
	tasks, ranges, err := fixture.backend.ListWork(fixture.run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != len(fixture.prepared.plans) ||
		len(ranges) != len(fixture.prepared.plans) {
		t.Fatalf("initial work inventory: tasks=%#v ranges=%#v", tasks, ranges)
	}
	completedAt := time.Now().UTC()
	for _, workRange := range ranges {
		if err := fixture.backend.CompleteRange(
			fixture.run.RunID,
			workRange.Task,
			workRange.ID,
			workRange.TopologyHash,
			workRange.NextSequence,
			completedAt,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func (fixture *stage4DeleteLifecycleFixture) classifyTransferred(
	t *testing.T,
	planIndex int,
) stage4AdapterPostgresDeleteTransferredTable {
	t.Helper()
	bound, found, err :=
		fixture.execution.classifyStage4AdapterPostgresDeleteTransferredTable(
			context.Background(),
			planIndex,
		)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("table %d was not classified as fully transferred", planIndex)
	}
	return bound
}

func (fixture *stage4DeleteLifecycleFixture) bindAllTransferred(t *testing.T) {
	t.Helper()
	for index := range fixture.prepared.plans {
		bound := fixture.classifyTransferred(t, index)
		if err := fixture.execution.bindStage4AdapterPostgresDeleteTransferredTable(
			index,
			bound,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func (fixture *stage4DeleteLifecycleFixture) seedNotDue(
	t *testing.T,
	planIndex int,
) {
	t.Helper()
	bound, found := fixture.execution.
		stage4AdapterPostgresDeleteTransferredTable(planIndex)
	if !found {
		t.Fatalf("table %d is not bound", planIndex)
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(
		fixture.run.RunID,
		bound.work,
	)
	if err != nil {
		t.Fatal(err)
	}
	entry := fixture.prepared.deletes.entries[planIndex]
	_, request, err := fixture.prepared.deletes.requestFor(
		entry,
		bound.work,
	)
	if err != nil {
		t.Fatal(err)
	}
	keyPlan, err := validateDeleteReconcileRequest(
		request,
		entry.capabilities.canonicalizer,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4PostgresDeleteRunnerSeedCompletedAuthority(
		t,
		fixture.backend,
		request,
		"prior-success-"+attemptID,
		keyPlan,
		fixture.prepared.deletes.now().Add(-time.Hour),
	)
	started := fixture.prepared.deletes.now().Add(-time.Minute)
	stored, created, err := fixture.backend.BeginDeleteReconciliation(
		state.DeleteReconciliation{
			RunID: fixture.run.RunID,
			Task:  bound.work.task, AttemptID: attemptID,
			Due:       false,
			Reason:    "reconciliation interval has not elapsed",
			StartedAt: started,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !created || stored.Status != state.DeleteReconciliationNotDue {
		t.Fatalf("seed not-due record = %#v, created=%v", stored, created)
	}
}

func (fixture *stage4DeleteLifecycleFixture) loadDeleteRecord(
	t *testing.T,
	planIndex int,
) state.DeleteReconciliation {
	t.Helper()
	bound, found := fixture.execution.
		stage4AdapterPostgresDeleteTransferredTable(planIndex)
	if !found {
		t.Fatalf("table %d is not bound", planIndex)
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(
		fixture.run.RunID,
		bound.work,
	)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := fixture.backend.Stage4StateBackend.
		LoadDeleteReconciliation(
			fixture.run.RunID,
			bound.work.task,
			attemptID,
		)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("delete record %s was not stored", attemptID)
	}
	return record
}

func (fixture *stage4DeleteLifecycleFixture) sourceStableOpens() int {
	return len(fixture.log.withPrefix("stable-open:"))
}

type stage4DeleteLifecycleLog struct {
	mu     sync.Mutex
	events []string
}

func (log *stage4DeleteLifecycleLog) add(event string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = append(log.events, event)
}

func (log *stage4DeleteLifecycleLog) clear() {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = nil
}

func (log *stage4DeleteLifecycleLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

func (log *stage4DeleteLifecycleLog) withPrefix(prefix string) []string {
	return stage4DeleteLifecycleEventsWithPrefix(log.snapshot(), prefix)
}

type stage4DeleteLifecycleInjectedRecord struct {
	record state.DeleteReconciliation
	found  bool
}

type stage4DeleteLifecycleBackend struct {
	Stage4StateBackend
	log *stage4DeleteLifecycleLog

	mu       sync.Mutex
	resets   int
	injected map[string]stage4DeleteLifecycleInjectedRecord
}

func (backend *stage4DeleteLifecycleBackend) ResetWorkPlan(
	task state.WorkTask,
	ranges []state.RangeState,
) error {
	backend.mu.Lock()
	backend.resets++
	backend.mu.Unlock()
	backend.log.add("work-reset:" + task.Key.Table)
	return backend.Stage4StateBackend.ResetWorkPlan(task, ranges)
}

func (backend *stage4DeleteLifecycleBackend) CompleteWorkTask(
	runID string,
	key state.TaskKey,
	topology string,
	at time.Time,
) error {
	backend.log.add("work-complete:" + key.Table)
	return backend.Stage4StateBackend.CompleteWorkTask(
		runID,
		key,
		topology,
		at,
	)
}

func (backend *stage4DeleteLifecycleBackend) LoadDeleteReconciliation(
	runID string,
	task state.TaskKey,
	attemptID string,
) (state.DeleteReconciliation, bool, error) {
	backend.mu.Lock()
	injected, ok := backend.injected[attemptID]
	backend.mu.Unlock()
	if ok {
		return injected.record, injected.found, nil
	}
	return backend.Stage4StateBackend.LoadDeleteReconciliation(
		runID,
		task,
		attemptID,
	)
}

func (backend *stage4DeleteLifecycleBackend) injectDeleteRecord(
	attemptID string,
	record state.DeleteReconciliation,
	found bool,
) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.injected[attemptID] = stage4DeleteLifecycleInjectedRecord{
		record: record,
		found:  found,
	}
}

func (backend *stage4DeleteLifecycleBackend) resetCalls() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.resets
}

type stage4DeleteLifecycleObserver struct {
	log *stage4DeleteLifecycleLog

	mu        sync.Mutex
	mutations int
}

func (observer *stage4DeleteLifecycleObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	observer.log.add("before-tables:" + strings.Join(tables, ","))
	return nil
}

func (observer *stage4DeleteLifecycleObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	observer.log.add("before:" + table)
	return nil
}

func (observer *stage4DeleteLifecycleObserver) AfterTable(
	_ context.Context,
	table string,
	_ int,
) error {
	observer.log.add("after:" + table)
	return nil
}

func (observer *stage4DeleteLifecycleObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	observer.mu.Lock()
	observer.mutations++
	observer.mu.Unlock()
	return mutation()
}

func (observer *stage4DeleteLifecycleObserver) mutationCalls() int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.mutations
}

type stage4DeleteLifecycleTarget struct {
	log  *stage4DeleteLifecycleLog
	rows map[string]int

	mu       sync.Mutex
	prepares int
	writes   int
}

func (*stage4DeleteLifecycleTarget) Engine() string { return "postgres" }

func (*stage4DeleteLifecycleTarget) stage4NetworkIdempotentUpsertTarget() {}

func (*stage4DeleteLifecycleTarget) PreflightStage4NetworkReplayIsolation(
	context.Context,
	[]schema.Table,
) error {
	return nil
}

func (*stage4DeleteLifecycleTarget) PlanTables(
	string,
	[]schema.Table,
	string,
) ([]schema.Table, error) {
	return nil, errors.New("unexpected PlanTables")
}

func (*stage4DeleteLifecycleTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return errors.New("unexpected PreflightTables")
}

func (target *stage4DeleteLifecycleTarget) PrepareTables(
	_ context.Context,
	tables []schema.Table,
	_ string,
) error {
	target.mu.Lock()
	target.prepares++
	target.mu.Unlock()
	for _, table := range tables {
		target.log.add("target-prepare:" + table.Name)
	}
	return nil
}

func (target *stage4DeleteLifecycleTarget) WriteBatch(
	_ context.Context,
	table schema.Table,
	_ []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	target.mu.Lock()
	target.writes++
	target.rows[table.Name] += len(rows)
	target.mu.Unlock()
	target.log.add("target-write:" + table.Name)
	count := int64(len(rows))
	return WriteReceipt{
		Certainty: CommitDurable, AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *stage4DeleteLifecycleTarget) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	return target.WriteBatch(ctx, table, columns, "upsert", rows)
}

func (target *stage4DeleteLifecycleTarget) CountRows(
	_ context.Context,
	table schema.Table,
) (int, error) {
	target.mu.Lock()
	rows := target.rows[table.Name]
	target.mu.Unlock()
	target.log.add("checkpoint-count:" + table.Name)
	return rows, nil
}

func (target *stage4DeleteLifecycleTarget) FinalizeTables(
	_ context.Context,
	tables []schema.Table,
	_ string,
) error {
	if len(tables) != 1 {
		return fmt.Errorf("finalize table count = %d", len(tables))
	}
	target.log.add("finalize:" + tables[0].Name)
	return nil
}

func (*stage4DeleteLifecycleTarget) Close() error { return nil }

func (target *stage4DeleteLifecycleTarget) prepareCalls() int {
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.prepares
}

func (target *stage4DeleteLifecycleTarget) writeCalls() int {
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.writes
}

type stage4DeleteLifecycleValidationProbe struct {
	log        *stage4DeleteLifecycleLog
	sourceRows map[string]int64
	targetRows map[string]int64
}

func (probe *stage4DeleteLifecycleValidationProbe) ExactCount(
	_ context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	probe.log.add("validation:" + string(side) + ":" + table.Name)
	if side == ValidationSource {
		return probe.sourceRows[table.Name], nil
	}
	return probe.targetRows[table.Name], nil
}

func (*stage4DeleteLifecycleValidationProbe) EstimateCount(
	context.Context,
	ValidationSide,
	schema.Table,
) (int64, error) {
	return 0, errors.New("unexpected EstimateCount")
}

func (*stage4DeleteLifecycleValidationProbe) NullCounts(
	context.Context,
	ValidationSide,
	schema.Table,
	[]string,
	ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	return ValidationNullCountEvidence{}, errors.New("unexpected NullCounts")
}

func (*stage4DeleteLifecycleValidationProbe) SampleSourceRows(
	context.Context,
	schema.Table,
	[]string,
	int,
) ([]ValidationSampleRow, error) {
	return nil, errors.New("unexpected SampleSourceRows")
}

func (*stage4DeleteLifecycleValidationProbe) SampleTargetRows(
	context.Context,
	schema.Table,
	[]string,
	[]ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	return nil, errors.New("unexpected SampleTargetRows")
}

type stage4DeleteLifecycleKeySource struct {
	table string
	log   *stage4DeleteLifecycleLog
}

func (source *stage4DeleteLifecycleKeySource) OpenDeletePrimaryKeys(
	context.Context,
	schema.Table,
	[]string,
) (deleteKeyRows, error) {
	source.log.add("delete-source-open:" + source.table)
	return &stage4DeleteLifecycleKeyRows{
		table: source.table,
		side:  "source",
		log:   source.log,
	}, nil
}

type stage4DeleteLifecycleKeyTarget struct {
	table string
	log   *stage4DeleteLifecycleLog
}

func (target *stage4DeleteLifecycleKeyTarget) OpenDeletePrimaryKeys(
	context.Context,
	schema.Table,
	[]string,
) (deleteKeyRows, error) {
	target.log.add("delete-target-open:" + target.table)
	return &stage4DeleteLifecycleKeyRows{
		table: target.table,
		side:  "target",
		log:   target.log,
	}, nil
}

func (*stage4DeleteLifecycleKeyTarget) MaxDeleteParameters() int {
	return 100
}

func (target *stage4DeleteLifecycleKeyTarget) ApplyDeleteBatch(
	_ context.Context,
	batch deleteTargetBatch,
) (deleteTargetBatchReceipt, error) {
	target.log.add("delete-apply:" + target.table)
	digest := sha256.Sum256([]byte(batch.Token))
	return deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token,
		Sequence: batch.Sequence, BatchDigest: batch.BatchDigest,
		Candidates:    int64(len(batch.Keys)),
		DeletedRows:   int64(len(batch.Keys)),
		ReceiptDigest: hex.EncodeToString(digest[:]),
	}, nil
}

type stage4DeleteLifecycleKeyRows struct {
	table  string
	side   string
	log    *stage4DeleteLifecycleLog
	rows   [][]any
	index  int
	closed bool
}

func (rows *stage4DeleteLifecycleKeyRows) Next() bool {
	if rows.index >= len(rows.rows) {
		return false
	}
	rows.index++
	return true
}

func (rows *stage4DeleteLifecycleKeyRows) Values() ([]any, error) {
	if rows.index < 1 || rows.index > len(rows.rows) {
		return nil, errors.New("delete row is not positioned")
	}
	return append([]any(nil), rows.rows[rows.index-1]...), nil
}

func (*stage4DeleteLifecycleKeyRows) Err() error { return nil }

func (rows *stage4DeleteLifecycleKeyRows) Close() error {
	if rows.closed {
		return errors.New("delete rows closed twice")
	}
	rows.closed = true
	rows.log.add("delete-" + rows.side + "-close:" + rows.table)
	return nil
}

func stage4DeleteLifecycleTerminalRecord(
	runID string,
	task state.TaskKey,
	attemptID string,
	status state.DeleteReconciliationStatus,
) state.DeleteReconciliation {
	started := time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)
	record := state.DeleteReconciliation{
		RunID: runID, Task: task, AttemptID: attemptID,
		Due: true, Status: status, StartedAt: started,
	}
	switch status {
	case state.DeleteReconciliationRunning:
	case state.DeleteReconciliationIncomplete:
		record.Reason = state.DeleteReconciliationReasonCancelled
		record.CompletedAt = started.Add(time.Minute)
	case state.DeleteReconciliationDryRun:
		record.DryRun = true
		record.CompletedAt = started.Add(time.Minute)
	case state.DeleteReconciliationCompleted:
		record.CompletedAt = started.Add(time.Minute)
	case state.DeleteReconciliationNotDue:
		record.Due = false
		record.Reason = "reconciliation interval has not elapsed"
		record.CompletedAt = started
	}
	return record
}

func stage4DeleteLifecycleEventIndex(events []string, want string) int {
	for index, event := range events {
		if event == want {
			return index
		}
	}
	return -1
}

func firstStage4DeleteLifecycleEvent(events []string, prefix string) int {
	for index, event := range events {
		if strings.HasPrefix(event, prefix) {
			return index
		}
	}
	return -1
}

func stage4DeleteLifecycleEventsWithPrefix(
	events []string,
	prefix string,
) []string {
	result := make([]string, 0)
	for _, event := range events {
		if strings.HasPrefix(event, prefix) {
			result = append(result, event)
		}
	}
	return result
}

func assertStage4DeleteLifecycleBefore(
	t *testing.T,
	events []string,
	first string,
	second string,
) {
	t.Helper()
	firstIndex := stage4DeleteLifecycleEventIndex(events, first)
	secondIndex := stage4DeleteLifecycleEventIndex(events, second)
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf("event %q did not precede %q: %v", first, second, events)
	}
}
