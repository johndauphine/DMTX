package migrate

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4NetworkFaultObserver struct {
	stage4AdapterObserver
	beforeErr    error
	afterErr     error
	aggregateErr error
}

// stage4NetworkFailingAggregateBackend fails the atomic table completion that
// replaced the separate ordinary checkpoint on a composed route, so the stable
// session must still be closed at that terminal failure point.
type stage4NetworkFailingAggregateBackend struct {
	state.YAMLStore
	err error
	// table scopes the failure to one ordinary table, which is how a baseline
	// leaves an earlier table durably complete and a later one not.
	table string
}

func (backend stage4NetworkFailingAggregateBackend) CompleteStage4Table(
	completion state.Stage4TableCompletion,
) error {
	if backend.table != "" && completion.Table != backend.table {
		return backend.YAMLStore.CompleteStage4Table(completion)
	}
	return backend.err
}

type stage4NetworkShortCountTarget struct {
	*recordingAdapterTarget
}

type stage4NetworkLifecycleFaultBackend struct {
	Stage4StateBackend

	resetErr        error
	completeTaskErr error
	mutateList      func(*[]state.WorkTask, *[]state.RangeState)
}

func (backend *stage4NetworkLifecycleFaultBackend) ResetWorkPlan(
	task state.WorkTask,
	ranges []state.RangeState,
) error {
	if backend.resetErr != nil &&
		task.Key.Type == stage4AdapterNetworkTaskType {
		return backend.resetErr
	}
	return backend.Stage4StateBackend.ResetWorkPlan(task, ranges)
}

func (backend *stage4NetworkLifecycleFaultBackend) CompleteWorkTask(
	runID string,
	key state.TaskKey,
	topologyHash string,
	at time.Time,
) error {
	if backend.completeTaskErr != nil &&
		key.Type == stage4AdapterNetworkTaskType {
		return backend.completeTaskErr
	}
	return backend.Stage4StateBackend.CompleteWorkTask(
		runID,
		key,
		topologyHash,
		at,
	)
}

func (backend *stage4NetworkLifecycleFaultBackend) ListWork(
	runID string,
) ([]state.WorkTask, []state.RangeState, error) {
	tasks, ranges, err := backend.Stage4StateBackend.ListWork(runID)
	if err != nil {
		return nil, nil, err
	}
	tasks = append([]state.WorkTask(nil), tasks...)
	clonedRanges := make([]state.RangeState, len(ranges))
	for index, workRange := range ranges {
		clonedRanges[index] = workRange
		clonedRanges[index].Lower = append(
			state.TypedTuple(nil),
			workRange.Lower...,
		)
		clonedRanges[index].Upper = append(
			state.TypedTuple(nil),
			workRange.Upper...,
		)
		clonedRanges[index].Frontier = append(
			state.TypedTuple(nil),
			workRange.Frontier...,
		)
		clonedRanges[index].Pending = append(
			[]state.PendingAcknowledgement(nil),
			workRange.Pending...,
		)
	}
	if backend.mutateList != nil {
		backend.mutateList(&tasks, &clonedRanges)
	}
	return tasks, clonedRanges, nil
}

func (target *stage4NetworkShortCountTarget) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	count, err := target.recordingAdapterTarget.CountRows(ctx, table)
	if count > 0 {
		count--
	}
	return count, err
}

func (observer stage4NetworkFaultObserver) BeforeTable(
	ctx context.Context,
	table string,
) error {
	if err := observer.stage4AdapterObserver.BeforeTable(
		ctx,
		table,
	); err != nil {
		return err
	}
	return observer.beforeErr
}

func (observer stage4NetworkFaultObserver) AfterTable(
	ctx context.Context,
	table string,
	rows int,
) error {
	if err := observer.stage4AdapterObserver.AfterTable(
		ctx,
		table,
		rows,
	); err != nil {
		return err
	}
	return observer.afterErr
}

func TestStage4AdapterNetworkFreshCheckpointsEveryTableBeforeAnyWrite(
	t *testing.T,
) {
	events := make([]string, 0)
	readRanges := make([]uint64, 0)
	items := stage4AdapterTestTable()
	widgets := stage4AdapterTestTable()
	widgets.Name = "widgets"
	source := &recordingAdapterSource{
		events:     &events,
		tables:     []schema.Table{items, widgets},
		readRanges: &readRanges,
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-all-before"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2

	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrate Stage 4 network tables: %v", err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	seenRanges := make(map[uint64]int, len(readRanges))
	for _, rangeIndex := range readRanges {
		seenRanges[rangeIndex]++
	}
	for rangeIndex := uint64(0); rangeIndex < 4; rangeIndex++ {
		if seenRanges[rangeIndex] != 1 {
			t.Fatalf(
				"migration-global range %d reads = %d; all reads=%v",
				rangeIndex,
				seenRanges[rangeIndex],
				readRanges,
			)
		}
	}

	beforeTables := stage4AdapterEventIndex(
		events,
		"before_tables:items,widgets",
	)
	firstWrite := stage4AdapterEventIndex(events, "target_write")
	if beforeTables < 0 || firstWrite < 0 {
		t.Fatalf("network lifecycle is incomplete: %v", events)
	}
	if beforeTables >= firstWrite {
		t.Fatalf("global table-set admission followed mutation: %v", events)
	}
	// A composed route publishes each table's terminal evidence through one
	// aggregate completion, so the execution segment closes on the table's own
	// stable close rather than on a separate ordinary AfterTable checkpoint.
	previousClose := beforeTables
	for _, name := range []string{"items", "widgets"} {
		before := stage4AdapterEventIndex(events, "before:"+name)
		if stage4AdapterEventIndex(events, "after:"+name) >= 0 {
			t.Fatalf(
				"composed table %s published a separate ordinary checkpoint: %v",
				name,
				events,
			)
		}
		closeIndex := -1
		for index := before + 1; index < len(events); index++ {
			if events[index] == "source_stable_close:"+name {
				closeIndex = index
				break
			}
		}
		if before <= previousClose || closeIndex <= before {
			t.Fatalf(
				"table %s did not own one complete execution stable lifecycle after its durable planning prepass: %v",
				name,
				events,
			)
		}
		segment := events[before:closeIndex]
		prepare := stage4AdapterEventIndex(segment, "target_prepare")
		write := stage4AdapterEventIndex(segment, "target_write")
		finalize := stage4AdapterEventIndex(segment, "target_finalize")
		sourceCount := stage4AdapterEventIndex(segment, "source_count")
		targetCount := stage4AdapterEventIndex(segment, "target_count")
		if prepare < 0 || write <= prepare || finalize <= write ||
			sourceCount <= finalize || targetCount <= finalize {
			t.Fatalf(
				"table %s lifecycle order is incomplete: %v",
				name,
				events,
			)
		}
		previousClose = closeIndex
	}

	// The composed route must leave the exact aggregate evidence a run
	// publication requires: one immutable inventory and one receipt per table,
	// with the ordinary task made terminal by that same mutation.
	inventory, found, err := backend.LoadStage4TableInventory(runID)
	if err != nil || !found {
		t.Fatalf("durable table inventory found=%v err=%v", found, err)
	}
	if len(inventory.Inventory.Tables) != 2 {
		t.Fatalf("table inventory = %#v", inventory.Inventory)
	}
	receipts, err := backend.LoadStage4TableCompletions(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 2 {
		t.Fatalf("aggregate table receipts = %#v", receipts)
	}
	ordinary, err := backend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ordinary) != 2 {
		t.Fatalf("ordinary tasks = %#v", ordinary)
	}
	for _, task := range ordinary {
		if task.Status != "completed" || task.RowsDone != 2 {
			t.Fatalf("ordinary task = %#v", task)
		}
	}

	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"items", "widgets"} {
		key := state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: "public",
			Table:  name,
		}
		var (
			completedTask bool
			rowsDone      int64
			rangeCount    int
		)
		for _, task := range tasks {
			if task.Key == key {
				completedTask = task.Status == "completed"
			}
		}
		for _, workRange := range ranges {
			if workRange.Task != key {
				continue
			}
			rangeCount++
			if workRange.Status != "completed" {
				t.Fatalf(
					"table %s range %s status = %q",
					name,
					workRange.ID,
					workRange.Status,
				)
			}
			rowsDone += workRange.RowsDone
		}
		if !completedTask || rangeCount != 2 || rowsDone != 2 {
			t.Fatalf(
				"table %s durable work task=%v ranges=%d rows=%d",
				name,
				completedTask,
				rangeCount,
				rowsDone,
			)
		}
	}
}

func TestStage4AdapterNetworkResumeAdmissionPrecedesDurableTableSet(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{
		events:       &events,
		isolationErr: context.DeadlineExceeded,
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-resume-admission"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"

	if _, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{},
		observer,
		observer,
		source,
		target,
	); err == nil {
		t.Fatal("unsafe replay isolation unexpectedly admitted resume")
	}
	for _, forbidden := range []string{
		"before_tables:items",
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"static resume admission crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Key.Type == stage4AdapterNetworkTaskType {
			t.Fatalf(
				"static resume admission created network task: %#v",
				task,
			)
		}
	}
}

func TestStage4AdapterNetworkCompletedResumeIgnoresLaterSourceGrowth(
	t *testing.T,
) {
	events := make([]string, 0)
	identityFrontier := int64(2)
	table := stage4AdapterTestTable()
	table.Identity = &schema.Identity{
		Column:     "id",
		Generation: schema.IdentityByDefault,
		Frontier:   &identityFrontier,
	}
	source := &recordingAdapterSource{
		events: &events,
		table:  table,
		rows:   []string{"first", "second"},
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-completed-source-growth"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	freshObserver := stage4AdapterObserver{
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
		freshObserver,
		source,
		target,
	); err != nil {
		t.Fatalf("complete initial stable table: %v", err)
	}

	source.rows = append(source.rows, "later-source-row")
	laterIdentityFrontier := int64(3)
	source.table.Identity.Frontier = &laterIdentityFrontier
	target.rowsByTable["items"] = 3
	events = events[:0]
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			true,
		),
	}
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("resume completed table after source growth: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("completed resume result = %#v", result)
	}
	for _, forbidden := range []string{
		"source_count",
		"source_pagination",
		"source_stable_close:items",
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
		"after:items",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"completed table reopened or replanned at %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterNetworkCompletedResumeRejectsCorruptDurableTopology(
	t *testing.T,
) {
	testCases := []struct {
		name              string
		withoutCheckpoint bool
		mutate            func(*[]state.WorkTask, *[]state.RangeState)
	}{
		{
			name: "extra range",
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for _, workRange := range *ranges {
					if workRange.Task.Type !=
						stage4AdapterNetworkTaskType {
						continue
					}
					extra := workRange
					extra.ID = "range/99"
					*ranges = append(*ranges, extra)
					return
				}
			},
		},
		{
			name: "duplicate range",
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for _, workRange := range *ranges {
					if workRange.Task.Type !=
						stage4AdapterNetworkTaskType {
						continue
					}
					*ranges = append(*ranges, workRange)
					return
				}
			},
		},
		{
			name: "changed bounds",
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for index := range *ranges {
					if (*ranges)[index].Task.Type ==
						stage4AdapterNetworkTaskType &&
						len((*ranges)[index].Upper) != 0 {
						(*ranges)[index].Upper[0] =
							state.Int64Value(999)
						return
					}
				}
			},
		},
		{
			name: "changed topology",
			mutate: func(
				tasks *[]state.WorkTask,
				_ *[]state.RangeState,
			) {
				for index := range *tasks {
					if (*tasks)[index].Key.Type ==
						stage4AdapterNetworkTaskType {
						(*tasks)[index].TopologyHash =
							strings.Repeat("a", 64)
						return
					}
				}
			},
		},
		{
			name: "completed mutable residue",
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for index := range *ranges {
					if (*ranges)[index].Task.Type ==
						stage4AdapterNetworkTaskType {
						(*ranges)[index].CommittedPrefix = 1
						return
					}
				}
			},
		},
		{
			name: "frontier outside keyset bounds",
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for index := range *ranges {
					if (*ranges)[index].Task.Type ==
						stage4AdapterNetworkTaskType {
						(*ranges)[index].Frontier =
							state.TypedTuple{
								state.Int64Value(999),
							}
						(*ranges)[index].FrontierValid = true
						return
					}
				}
			},
		},
		{
			name:              "stale table task",
			withoutCheckpoint: true,
			mutate: func(
				tasks *[]state.WorkTask,
				_ *[]state.RangeState,
			) {
				for _, task := range *tasks {
					if task.Key.Type !=
						stage4AdapterNetworkTaskType {
						continue
					}
					stale := task
					stale.Key.Table = "stale_items"
					*tasks = append(*tasks, stale)
					return
				}
			},
		},
		{
			name:              "orphan table range",
			withoutCheckpoint: true,
			mutate: func(
				_ *[]state.WorkTask,
				ranges *[]state.RangeState,
			) {
				for _, workRange := range *ranges {
					if workRange.Task.Type !=
						stage4AdapterNetworkTaskType {
						continue
					}
					orphan := workRange
					orphan.Task.Table = "orphan_items"
					*ranges = append(*ranges, orphan)
					return
				}
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  stage4AdapterTestTable(),
			}
			target := &recordingAdapterTarget{events: &events}
			rawBackend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "stage4-network-corrupt-" +
				strings.ReplaceAll(testCase.name, " ", "-")
			initializeStage4LifecycleRun(
				t,
				rawBackend,
				runID,
				time.Now().Add(-time.Minute),
			)
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Migration.TargetMode = "upsert"
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &events,
				},
				run: stage4LifecycleRunContext(
					t,
					rawBackend,
					runID,
					false,
				),
			}
			if _, err := migrateWithAdapters(
				context.Background(),
				cfg,
				freshObserver,
				source,
				target,
			); err != nil {
				t.Fatalf("complete baseline migration: %v", err)
			}

			faultBackend := &stage4NetworkLifecycleFaultBackend{
				Stage4StateBackend: rawBackend,
				mutateList: func(
					tasks *[]state.WorkTask,
					ranges *[]state.RangeState,
				) {
					testCase.mutate(tasks, ranges)
				},
			}
			events = events[:0]
			resumeObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &events,
				},
				run: stage4LifecycleRunContext(
					t,
					faultBackend,
					runID,
					true,
				),
			}
			completed := CompletedTableCheckpoints{
				"items": {Rows: 2},
			}
			if testCase.withoutCheckpoint {
				completed = CompletedTableCheckpoints{}
			}
			_, err := resumeWithAdapters(
				context.Background(),
				cfg,
				completed,
				resumeObserver,
				resumeObserver,
				source,
				target,
			)
			if err == nil {
				t.Fatal("corrupt completed work unexpectedly resumed")
			}
			for _, forbidden := range []string{
				"before_tables:items",
				"before:items",
				"source_pagination",
				"target_prepare",
				"target_write",
				"target_finalize",
			} {
				if stage4AdapterEventIndex(events, forbidden) >= 0 {
					t.Fatalf(
						"corrupt completed work crossed %s: %v",
						forbidden,
						events,
					)
				}
			}
		})
	}
}

func TestStage4AdapterNetworkRejectsMissingStaticSourceCatalog(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-missing-source-catalog"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	delete(
		prepared.sourceCatalog,
		stage4RichTableKey{schema: "public", table: "items"},
	)
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointStage4AdapterTableSet(
		context.Background(),
		observer,
		prepared.names,
	); err != nil {
		t.Fatal(err)
	}
	_, err = execution.openTable(
		context.Background(),
		0,
		false,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"is missing from static admission",
		) {
		t.Fatalf("missing static source catalog error = %v", err)
	}
	if stage4AdapterEventIndex(
		events,
		"source_stable_close:items",
	) < 0 {
		t.Fatalf("missing-catalog session was not closed: %v", events)
	}
	for _, forbidden := range []string{
		"source_pagination",
		"before:items",
		"target_prepare",
		"target_write",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"missing source catalog crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterNetworkRejectsCatalogDriftAfterStableOpen(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	source.beforeStableOpen = func(source *recordingAdapterSource) {
		source.beforeStableOpen = nil
		source.table.Columns = append(
			source.table.Columns,
			schema.Column{
				Name:     "appeared_after_discovery",
				Type:     "text",
				Nullable: true,
			},
		)
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-catalog-drift"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"

	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"source schema changed before stable planning",
		) {
		t.Fatalf("stable catalog drift error = %v", err)
	}
	if stage4AdapterEventIndex(
		events,
		"source_stable_close:items",
	) < 0 {
		t.Fatalf("catalog-drift session was not closed: %v", events)
	}
	for _, forbidden := range []string{
		"source_pagination",
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"catalog drift crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Key.Type == stage4AdapterNetworkTaskType {
			t.Fatalf("catalog drift created durable network work: %#v", task)
		}
	}
}

func TestStage4AdapterNetworkStableSessionClampsReaders(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events:            &events,
		table:             stage4AdapterTestTable(),
		stableReaderLimit: 1,
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-reader-clamp"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatal(err)
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
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointStage4AdapterTableSet(
		context.Background(),
		observer,
		prepared.names,
	); err != nil {
		t.Fatal(err)
	}
	tableExecution, err := execution.openTable(
		context.Background(),
		0,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tableExecution.Close(); err != nil {
			t.Error(err)
		}
	}()
	readers := tableExecution.corePlan.Resources.Readers
	if readers.Value != 1 ||
		readers.Provenance != config.ProvenanceSafetyClamped {
		t.Fatalf("stable reader resources = %#v", readers)
	}
	if stage4AdapterEventIndex(events, "before_tables:items") >=
		stage4AdapterEventIndex(events, "source_pagination") {
		t.Fatalf("stable plan preceded global table set: %v", events)
	}
	if len(target.written) != 0 ||
		len(target.prepared) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf("reader admission mutated target: %#v", target)
	}
}

func TestStage4AdapterNetworkClosesStableSessionOnEveryTableFailure(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name      string
		configure func(
			*stage4NetworkFaultObserver,
			*recordingAdapterTarget,
		) targetAdapter
	}{
		{
			name: "before table",
			configure: func(
				observer *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				observer.beforeErr = errors.New("forced before-table failure")
				return target
			},
		},
		{
			name: "prepare",
			configure: func(
				_ *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				target.prepareErr = errors.New("forced prepare failure")
				return target
			},
		},
		{
			name: "write",
			configure: func(
				_ *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				target.writeErr = errors.New("forced write failure")
				return target
			},
		},
		{
			name: "finalize",
			configure: func(
				_ *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				target.finalizeErr = errors.New("forced finalize failure")
				return target
			},
		},
		{
			name: "validation",
			configure: func(
				_ *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				return &stage4NetworkShortCountTarget{
					recordingAdapterTarget: target,
				}
			},
		},
		{
			name: "aggregate completion",
			configure: func(
				observer *stage4NetworkFaultObserver,
				target *recordingAdapterTarget,
			) targetAdapter {
				observer.aggregateErr = errors.New(
					"forced aggregate table completion failure",
				)
				return target
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  stage4AdapterTestTable(),
			}
			baseTarget := &recordingAdapterTarget{events: &events}
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "stage4-network-close-" +
				strings.ReplaceAll(testCase.name, " ", "-")
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			observer := stage4NetworkFaultObserver{
				stage4AdapterObserver: stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{
						events: &events,
					},
					run: stage4LifecycleRunContext(
						t,
						backend,
						runID,
						false,
					),
				},
			}
			target := testCase.configure(&observer, baseTarget)
			if observer.aggregateErr != nil {
				observer.run = stage4LifecycleRunContext(
					t,
					stage4NetworkFailingAggregateBackend{
						YAMLStore: backend,
						err:       observer.aggregateErr,
					},
					runID,
					false,
				)
			}
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.MaxRetries = 0
			if _, err := migrateWithAdapters(
				context.Background(),
				cfg,
				observer,
				source,
				target,
			); err == nil {
				t.Fatal("forced table failure unexpectedly succeeded")
			}
			if stage4AdapterEventIndex(
				events,
				"source_stable_close:items",
			) < 0 {
				t.Fatalf(
					"stable session leaked after %s: %v",
					testCase.name,
					events,
				)
			}
		})
	}
}

func TestStage4AdapterNetworkClosesStableSessionOnWorkTaskCompletionFailure(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	rawBackend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-close-work-task-failure"
	initializeStage4LifecycleRun(
		t,
		rawBackend,
		runID,
		time.Now().Add(-time.Minute),
	)
	faultBackend := &stage4NetworkLifecycleFaultBackend{
		Stage4StateBackend: rawBackend,
		completeTaskErr:    errors.New("forced work-task completion failure"),
	}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			faultBackend,
			runID,
			false,
		),
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"

	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"forced work-task completion failure",
		) {
		t.Fatalf("work-task completion error = %v", err)
	}
	if stage4AdapterEventIndex(
		events,
		"source_stable_close:items",
	) < 0 {
		t.Fatalf("work-task completion failure leaked session: %v", events)
	}
	if stage4AdapterEventIndex(events, "after:items") >= 0 {
		t.Fatalf("ordinary completion followed failed work task: %v", events)
	}
}

func TestStage4AdapterNetworkMissingAggregateCheckpointResetsWholeTable(
	t *testing.T,
) {
	events := make([]string, 0)
	readRanges := make([]uint64, 0)
	source := &recordingAdapterSource{
		events:     &events,
		table:      stage4AdapterTestTable(),
		readRanges: &readRanges,
	}
	target := &recordingAdapterTarget{events: &events}
	rawBackend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-missing-aggregate-reset"
	initializeStage4LifecycleRun(
		t,
		rawBackend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	failingObserver := stage4NetworkFaultObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				stage4NetworkFailingAggregateBackend{
					YAMLStore: rawBackend,
					err: errors.New(
						"forced aggregate checkpoint failure",
					),
				},
				runID,
				false,
			),
		},
	}
	if _, err := migrateWithAdapters(
		context.Background(),
		cfg,
		failingObserver,
		source,
		target,
	); err == nil ||
		!strings.Contains(err.Error(), "aggregate checkpoint failure") {
		t.Fatalf("aggregate checkpoint failure = %v", err)
	}
	tasks, ranges, err := rawBackend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	// A composed route publishes the ordinary task, the structured task, and the
	// receipt in one mutation, so a failed completion must leave no terminal
	// table evidence at all rather than a structurally complete table whose
	// ordinary checkpoint never landed.
	networkTaskComplete := false
	for _, task := range tasks {
		if task.Key.Type == stage4AdapterNetworkTaskType {
			networkTaskComplete = task.Status == "completed"
		}
	}
	if networkTaskComplete {
		t.Fatalf(
			"failed aggregate checkpoint left partial structured completion: tasks=%#v ranges=%#v",
			tasks,
			ranges,
		)
	}
	receipts, err := rawBackend.LoadStage4TableCompletions(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 0 {
		t.Fatalf("failed aggregate checkpoint published receipts: %#v", receipts)
	}
	ordinary, err := rawBackend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range ordinary {
		if task.Status == "completed" {
			t.Fatalf(
				"failed aggregate checkpoint completed an ordinary task: %#v",
				task,
			)
		}
	}

	faultBackend := &stage4NetworkLifecycleFaultBackend{
		Stage4StateBackend: rawBackend,
		resetErr:           errors.New("forced reset failure"),
	}
	events = events[:0]
	readRanges = readRanges[:0]
	resetObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			faultBackend,
			runID,
			true,
		),
	}
	_, err = resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{},
		resetObserver,
		resetObserver,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "forced reset failure") {
		t.Fatalf("reset failure = %v", err)
	}
	if stage4AdapterEventIndex(
		events,
		"source_stable_close:items",
	) < 0 {
		t.Fatalf("reset failure leaked stable session: %v", events)
	}
	for _, forbidden := range []string{
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf("reset failure crossed %s: %v", forbidden, events)
		}
	}

	events = events[:0]
	readRanges = readRanges[:0]
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			rawBackend,
			runID,
			true,
		),
	}
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("replay table after missing aggregate checkpoint: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("replayed result = %#v", result)
	}
	if target.rowsByTable["items"] != 2 {
		t.Fatalf(
			"idempotent whole-table replay rows = %d",
			target.rowsByTable["items"],
		)
	}
	seenRanges := make(map[uint64]int)
	for _, rangeIndex := range readRanges {
		seenRanges[rangeIndex]++
	}
	if seenRanges[0] != 1 || seenRanges[1] != 1 ||
		len(seenRanges) != 2 {
		t.Fatalf("whole-table replay ranges = %v", readRanges)
	}
}

func TestStage4AdapterNetworkCloseFailureDefersGlobalPublicationAndResumes(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	stableOpens := 0
	source.beforeStableOpen = func(source *recordingAdapterSource) {
		stableOpens++
		source.stableCloseErr = nil
		if stableOpens > 1 {
			source.stableCloseErr =
				errors.New("forced stable close failure")
		}
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-close-failure-recovery"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		freshObserver,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "forced stable close failure") {
		t.Fatalf("stable close error = %v", err)
	}
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var schemaStatus, networkStatus string
	for _, task := range tasks {
		switch task.Key {
		case stage4SchemaGateTask:
			schemaStatus = task.Status
		default:
			if task.Key.Type == stage4AdapterNetworkTaskType {
				networkStatus = task.Status
			}
		}
	}
	if schemaStatus == "completed" || networkStatus != "completed" {
		t.Fatalf(
			"close failure publication state schema=%q network=%q",
			schemaStatus,
			networkStatus,
		)
	}

	source.beforeStableOpen = nil
	source.stableCloseErr = nil
	events = events[:0]
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			true,
		),
	}
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("resume completed table after close failure: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("close-failure resume result = %#v", result)
	}
	for _, forbidden := range []string{
		"source_pagination",
		"source_stable_close:items",
		"before:items",
		"target_prepare",
		"target_write",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"close-failure resume reopened completed table at %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterNetworkResumeOffsetsAfterCompletedTable(
	t *testing.T,
) {
	events := make([]string, 0)
	readRanges := make([]uint64, 0)
	items := stage4AdapterTestTable()
	widgets := stage4AdapterTestTable()
	widgets.Name = "widgets"
	source := &recordingAdapterSource{
		events:     &events,
		tables:     []schema.Table{items, widgets},
		readRanges: &readRanges,
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-completed-offset"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	// The baseline must leave the first table durably complete and the second
	// not, which is the only shape a production resume observes: the completed
	// set is derived from the ordinary task status it is about to be given.
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			stage4NetworkFailingAggregateBackend{
				YAMLStore: backend,
				err:       errors.New("forced widgets completion failure"),
				table:     "widgets",
			},
			runID,
			false,
		),
	}
	if _, err := migrateWithAdapters(
		context.Background(),
		cfg,
		freshObserver,
		source,
		target,
	); err == nil {
		t.Fatal("baseline unexpectedly completed the second table")
	}

	events = events[:0]
	readRanges = readRanges[:0]
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			true,
		),
	}
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("resume after completed first table: %v", err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("offset resume result = %#v", result)
	}
	seenRanges := make(map[uint64]int)
	for _, rangeIndex := range readRanges {
		seenRanges[rangeIndex]++
	}
	if seenRanges[2] != 1 || seenRanges[3] != 1 ||
		len(seenRanges) != 2 {
		t.Fatalf(
			"second table did not retain migration-global offsets: %v",
			readRanges,
		)
	}
	for _, forbidden := range []string{
		"before:items",
		"source_stable_close:items",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf("completed first table reopened at %s: %v", forbidden, events)
		}
	}
}

func TestStage4AdapterNetworkCompletedRangeBudgetFailsBeforeTableSet(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-completed-range-budget"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	freshObserver := stage4AdapterObserver{
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
		freshObserver,
		source,
		target,
	); err != nil {
		t.Fatalf("complete range-budget baseline: %v", err)
	}

	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			true,
		),
	}
	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		resumeObserver,
		source,
		target,
		"upsert",
		resumeObserver.run,
	)
	if err != nil {
		t.Fatal(err)
	}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		resumeObserver,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	execution.nextGlobalRange = maximumRuntimeTuningRanges
	events = events[:0]
	err = execution.prevalidateCompletedTables(
		context.Background(),
		map[string]int{"items": 2},
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"completed global network range inventory is unbounded",
		) {
		t.Fatalf("completed range-budget error = %v", err)
	}
	for _, forbidden := range []string{
		"before_tables:items",
		"before:items",
		"source_pagination",
		"target_prepare",
		"target_write",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"completed range-budget failure crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterNetworkResumeReplansChangedRangeCount(
	t *testing.T,
) {
	events := make([]string, 0)
	readRanges := make([]uint64, 0)
	source := &recordingAdapterSource{
		events:     &events,
		table:      stage4AdapterTestTable(),
		rows:       []string{"first"},
		ids:        []int64{10},
		readRanges: &readRanges,
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-replan-range-count"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	failingObserver := stage4NetworkFaultObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				stage4NetworkFailingAggregateBackend{
					YAMLStore: backend,
					err: errors.New(
						"forced aggregate checkpoint failure",
					),
				},
				runID,
				false,
			),
		},
	}
	if _, err := migrateWithAdapters(
		context.Background(),
		cfg,
		failingObserver,
		source,
		target,
	); err == nil {
		t.Fatal("initial aggregate checkpoint failure unexpectedly succeeded")
	}

	source.rows = []string{"first", "middle", "last"}
	source.ids = []int64{10, 30, 50}
	events = events[:0]
	readRanges = readRanges[:0]
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			true,
		),
	}
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("resume changed range count: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("changed-range resume result = %#v", result)
	}
	_, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	networkRanges := 0
	for _, workRange := range ranges {
		if workRange.Task.Type == stage4AdapterNetworkTaskType {
			networkRanges++
		}
	}
	if networkRanges != 2 {
		t.Fatalf("replanned durable range count = %d", networkRanges)
	}
	seenRanges := make(map[uint64]int)
	for _, rangeIndex := range readRanges {
		seenRanges[rangeIndex]++
	}
	if seenRanges[0] != 1 || seenRanges[1] != 1 ||
		len(seenRanges) != 2 {
		t.Fatalf("changed-range callbacks = %v", readRanges)
	}
}
