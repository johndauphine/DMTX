package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4AdapterDurablyAdmitsPaginationBeforeTargetMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
		rows:   []string{"first", "second"},
	}
	rawTarget := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-admission"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	protected := false
	rawTarget.protected = &protected
	target := &stage4NetworkAdmissionTarget{
		recordingAdapterTarget: rawTarget,
		backend:                backend,
		runID:                  runID,
	}
	observer := &stage4NetworkAdmissionObserver{
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
		protected: &protected,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.Partitions = 2

	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if !target.sawDurableAdmission {
		t.Fatal("target preparation did not observe durable network ranges")
	}
	if observer.protectionCalls < 3 {
		t.Fatalf(
			"mutation protection calls = %d, want prepare/write/finalize",
			observer.protectionCalls,
		)
	}
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_pagination",
		"before_tables:items",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"before_tables:items",
		"target_prepare",
	)

	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var tableTask *state.WorkTask
	tableRanges := make([]state.RangeState, 0, 2)
	for index := range tasks {
		if tasks[index].Key == (state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: "public",
			Table:  "items",
		}) {
			tableTask = &tasks[index]
		}
	}
	for _, workRange := range ranges {
		if workRange.Task.Type == stage4AdapterNetworkTaskType &&
			workRange.Task.Schema == "public" &&
			workRange.Task.Table == "items" {
			tableRanges = append(tableRanges, workRange)
		}
	}
	if tableTask == nil ||
		tableTask.Strategy != stage4AdapterCopyStrategy ||
		tableTask.Status != "completed" ||
		len(tableRanges) != 2 {
		t.Fatalf(
			"table task=%#v ranges=%#v",
			tableTask,
			tableRanges,
		)
	}
	if tableRanges[0].ID != "range/0" ||
		tableRanges[1].ID != "range/1" ||
		tableRanges[0].Status != "completed" ||
		tableRanges[1].Status != "completed" ||
		len(tableRanges[0].Lower) != 0 ||
		len(tableRanges[0].Upper) != 1 ||
		len(tableRanges[1].Lower) != 1 ||
		len(tableRanges[1].Upper) != 1 ||
		tableRanges[0].Upper[0] != state.Int64Value(1) ||
		tableRanges[1].Lower[0] != state.Int64Value(1) ||
		tableRanges[1].Upper[0] != state.Int64Value(2) {
		t.Fatalf("durable pagination ranges = %#v", tableRanges)
	}
	for _, workRange := range tableRanges {
		if workRange.ID == "full-copy" {
			t.Fatal("coarse full-copy range survived network admission")
		}
	}
}

func TestStage4AdapterRejectsChangedPaginationBeforeTargetMutationUntilReset(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
		rows:   []string{"first", "second"},
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-topology-change"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: run,
		},
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.Partitions = 2
	initial, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"drop_recreate",
		run,
	)
	if err != nil {
		t.Fatalf("prepare initial topology: %v", err)
	}
	if err := ensureStage4AdapterWork(
		context.Background(),
		run,
		initial.work,
	); err != nil {
		t.Fatalf("persist initial topology: %v", err)
	}

	source.rows = []string{"first", "second", "third"}
	events = events[:0]
	_, err = migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "topology") {
		t.Fatalf("changed pagination error = %v", err)
	}
	for _, forbidden := range []string{
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"changed topology reached %s before reset: %v",
				forbidden,
				events,
			)
		}
	}

	changed, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"drop_recreate",
		run,
	)
	if err != nil {
		t.Fatalf("prepare changed topology for reset: %v", err)
	}
	if len(changed.work) != 1 {
		t.Fatalf("changed work = %#v", changed.work)
	}
	item := changed.work[0]
	if err := backend.ResetWorkPlan(
		state.WorkTask{
			RunID:        runID,
			Key:          item.task,
			Strategy:     item.strategy,
			TopologyHash: item.topology,
		},
		append([]state.RangeState(nil), item.ranges...),
	); err != nil {
		t.Fatalf("explicitly reset changed network topology: %v", err)
	}
	if err := ensureStage4AdapterWork(
		context.Background(),
		run,
		changed.work,
	); err != nil {
		t.Fatalf("admit explicitly reset topology: %v", err)
	}
}

func TestStage4AdapterPaginationAdmissionRejectsUnapprovedShapes(
	t *testing.T,
) {
	events := make([]string, 0)
	table := stage4AdapterTestTable()
	source := &recordingAdapterSource{
		events: &events,
		table:  table,
		rows:   []string{"first", "second"},
	}
	valid, err := source.PlanPagination(
		context.Background(),
		table,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStage4AdapterPagination(
		"postgres",
		table,
		2,
		valid,
	); err != nil {
		t.Fatalf("valid pagination: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*PaginationPlan)
		want   string
	}{
		{
			name: "non-canonical integer",
			mutate: func(plan *PaginationPlan) {
				(*plan.Ranges[0].Upper)[0].Encoded = "+1"
			},
			want: "non-canonical",
		},
		{
			name: "range does not advance",
			mutate: func(plan *PaginationPlan) {
				upper := append(
					KeyTuple(nil),
					(*plan.Ranges[0].Upper)...,
				)
				plan.Ranges[1].Upper = &upper
			},
			want: "does not advance",
		},
		{
			name: "primary key changed",
			mutate: func(plan *PaginationPlan) {
				plan.Keys[0].Name = "payload"
			},
			want: "source primary-key order",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := cloneStage4AdapterPagination(valid)
			test.mutate(&plan)
			stage4AdapterRehashPagination(
				t,
				"postgres",
				table,
				2,
				&plan,
			)
			err := validateStage4AdapterPagination(
				"postgres",
				table,
				2,
				plan,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("pagination error = %v, want %q", err, test.want)
			}
		})
	}

	rowNumberTable := schema.Table{
		Schema: "public",
		Name:   "text_items",
		Columns: []schema.Column{{
			Name:               "code",
			Type:               "text",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	rowNumber := PaginationPlan{
		Strategy: PaginationRowNumber,
		Keys:     []KeySpec{{Name: "code", Kind: KeyText}},
		Ranges: []PaginationRange{{
			ID:       0,
			FirstRow: 2,
			LastRow:  2,
		}},
	}
	stage4AdapterRehashPagination(
		t,
		"postgres",
		rowNumberTable,
		1,
		&rowNumber,
	)
	if err := validateStage4AdapterPagination(
		"postgres",
		rowNumberTable,
		1,
		rowNumber,
	); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("row-number gap error = %v", err)
	}
}

func stage4AdapterRehashPagination(
	t *testing.T,
	sourceEngine string,
	table schema.Table,
	requestedPartitions int,
	plan *PaginationPlan,
) {
	t.Helper()
	keys, err := adapterPaginationPrimaryKey(
		sourceEngine,
		table.Schema,
		table,
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence := make(
		[]adapterPaginationKeyEvidence,
		len(keys),
	)
	for index, key := range keys {
		evidence[index] = adapterPaginationKeyEvidence{
			Name:     key.Name,
			Type:     key.Type,
			Nullable: key.Nullable,
			Position: key.PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(
				key.DeclaredType,
			),
		}
	}
	plan.TopologyHash, err = adapterPaginationTopologyHash(
		sourceEngine,
		table,
		requestedPartitions,
		evidence,
		*plan,
	)
	if err != nil {
		t.Fatal(err)
	}
}

type stage4NetworkAdmissionObserver struct {
	stage4AdapterObserver
	protected       *bool
	protectionCalls int
}

func (observer *stage4NetworkAdmissionObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	observer.protectionCalls++
	if observer.protected != nil {
		if *observer.protected {
			return fmt.Errorf("nested target mutation protection")
		}
		*observer.protected = true
		defer func() {
			*observer.protected = false
		}()
	}
	return mutation()
}

type stage4NetworkAdmissionTarget struct {
	*recordingAdapterTarget
	backend             state.RangeBackend
	runID               string
	sawDurableAdmission bool
}

func (target *stage4NetworkAdmissionTarget) PrepareTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	tasks, ranges, err := target.backend.ListWork(target.runID)
	if err != nil {
		return fmt.Errorf("inspect durable admission: %w", err)
	}
	for _, task := range tasks {
		if task.Key != (state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: "public",
			Table:  "items",
		}) ||
			task.Strategy != stage4AdapterCopyStrategy ||
			task.Status != "running" {
			continue
		}
		count := 0
		for _, workRange := range ranges {
			if workRange.Task == task.Key &&
				workRange.Strategy == stage4AdapterCopyStrategy &&
				workRange.TopologyHash == task.TopologyHash &&
				workRange.Status == "running" {
				count++
			}
		}
		target.sawDurableAdmission = count == 2
	}
	if !target.sawDurableAdmission {
		return fmt.Errorf(
			"target preparation ran before durable network admission",
		)
	}
	return target.recordingAdapterTarget.PrepareTables(
		ctx,
		tables,
		mode,
	)
}

var _ targetAdapter = (*stage4NetworkAdmissionTarget)(nil)
var _ adapterTargetMutationProtector = (*stage4NetworkAdmissionObserver)(nil)
