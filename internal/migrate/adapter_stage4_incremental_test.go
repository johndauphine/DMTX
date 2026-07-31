package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4IncrementalTestRow struct {
	id      int64
	payload string
	updated *time.Time
}

type stage4IncrementalTestSource struct {
	events *[]string
	table  schema.Table
	rows   []stage4IncrementalTestRow
}

func (*stage4IncrementalTestSource) Engine() string      { return "postgres" }
func (*stage4IncrementalTestSource) DisplayName() string { return "PostgreSQL" }

func (source *stage4IncrementalTestSource) ListTables(
	context.Context,
) ([]string, error) {
	*source.events = append(*source.events, "source_list")
	return []string{source.table.Name}, nil
}

func (source *stage4IncrementalTestSource) InspectTable(
	_ context.Context,
	name string,
) (schema.Table, error) {
	*source.events = append(*source.events, "source_inspect")
	if name != source.table.Name {
		return schema.Table{}, fmt.Errorf("unexpected table %q", name)
	}
	return cloneStage4RichTable(source.table), nil
}

func (source *stage4IncrementalTestSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, fmt.Errorf("ordinary source reads are forbidden")
}

func (source *stage4IncrementalTestSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return len(source.rows), nil
}

func (*stage4IncrementalTestSource) Close() error { return nil }

func (source *stage4IncrementalTestSource) IncrementalTable(
	table schema.Table,
) (IncrementalTable, error) {
	return buildAdapterIncrementalTable("postgres", "public", table)
}

func (source *stage4IncrementalTestSource) SampleIncrementalUpperFence(
	_ context.Context,
	_ schema.Table,
	column IncrementalColumn,
) (*time.Time, error) {
	*source.events = append(*source.events, "source_sample_fence")
	if column.Name != "updated_at" {
		return nil, fmt.Errorf("unexpected fence column %q", column.Name)
	}
	var maximum *time.Time
	for _, row := range source.rows {
		if row.updated == nil {
			continue
		}
		if maximum == nil || row.updated.After(*maximum) {
			copy := row.updated.UTC()
			maximum = &copy
		}
	}
	return maximum, nil
}

func (source *stage4IncrementalTestSource) OpenIncrementalRows(
	_ context.Context,
	_ schema.Table,
	columns []string,
	read IncrementalReadPlan,
) (adapterRows, error) {
	*source.events = append(*source.events, "source_incremental_rows")
	if len(columns) != 3 ||
		columns[0] != "id" ||
		columns[1] != "payload" ||
		columns[2] != "updated_at" {
		return nil, fmt.Errorf("unexpected projection %#v", columns)
	}
	selected := make([]stage4IncrementalTestRow, 0, len(source.rows))
	for _, row := range source.rows {
		if read.Scope == IncrementalReadWindow &&
			!read.Window.Contains(row.updated) {
			continue
		}
		selected = append(selected, row)
	}
	sort.Slice(selected, func(left, right int) bool {
		leftTime, rightTime := selected[left].updated, selected[right].updated
		if leftTime == nil || rightTime == nil {
			if leftTime == nil && rightTime == nil {
				return selected[left].id < selected[right].id
			}
			return leftTime == nil
		}
		if leftTime.Equal(*rightTime) {
			return selected[left].id < selected[right].id
		}
		return leftTime.Before(*rightTime)
	})
	values := make([][]any, len(selected))
	for index, row := range selected {
		var updated any
		if row.updated != nil {
			updated = row.updated.UTC()
		}
		values[index] = []any{row.id, row.payload, updated}
	}
	return &stage4IncrementalTestRows{values: values, index: -1}, nil
}

type stage4IncrementalTestRows struct {
	values [][]any
	index  int
}

func (rows *stage4IncrementalTestRows) Next() bool {
	rows.index++
	return rows.index < len(rows.values)
}

func (rows *stage4IncrementalTestRows) Scan(destinations ...any) error {
	if rows.index < 0 || rows.index >= len(rows.values) {
		return fmt.Errorf("row index is outside the stream")
	}
	if len(destinations) != len(rows.values[rows.index]) {
		return fmt.Errorf("destination count differs")
	}
	for index, value := range rows.values[rows.index] {
		destination, ok := destinations[index].(*any)
		if !ok {
			return fmt.Errorf("destination %d has type %T", index, destinations[index])
		}
		*destination = value
	}
	return nil
}

func (*stage4IncrementalTestRows) Err() error   { return nil }
func (*stage4IncrementalTestRows) Close() error { return nil }

type stage4IncrementalTestTarget struct {
	events *[]string
	rows   map[int64][]any
}

func (*stage4IncrementalTestTarget) Engine() string                       { return "postgres" }
func (*stage4IncrementalTestTarget) stage4NetworkIdempotentUpsertTarget() {}

func (target *stage4IncrementalTestTarget) PlanTables(
	sourceEngine string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	*target.events = append(*target.events, "target_plan")
	if sourceEngine != "postgres" || mode != "upsert" {
		return nil, fmt.Errorf("unexpected route %s/%s", sourceEngine, mode)
	}
	result := make([]schema.Table, len(tables))
	for index := range tables {
		result[index] = cloneStage4RichTable(tables[index])
	}
	return result, nil
}

func (target *stage4IncrementalTestTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_preflight")
	return nil
}

func (target *stage4IncrementalTestTarget) PreflightStage4NetworkReplayIsolation(
	context.Context,
	[]schema.Table,
) error {
	*target.events = append(*target.events, "target_isolation")
	return nil
}

func (target *stage4IncrementalTestTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_prepare")
	return nil
}

func (target *stage4IncrementalTestTarget) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	return target.WriteStage4NetworkBatch(ctx, table, columns, rows)
}

func (target *stage4IncrementalTestTarget) WriteStage4NetworkBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	if target.rows == nil {
		target.rows = make(map[int64][]any)
	}
	for _, row := range rows {
		id, ok := row[0].(int64)
		if !ok {
			return WriteReceipt{}, fmt.Errorf("unexpected key type %T", row[0])
		}
		target.rows[id] = cloneAdapterRow(row)
	}
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *stage4IncrementalTestTarget) ValidateStage4IncrementalBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) error {
	*target.events = append(*target.events, "target_validate")
	for _, row := range rows {
		id, ok := row[0].(int64)
		if !ok {
			return fmt.Errorf("unexpected validation key type %T", row[0])
		}
		stored, found := target.rows[id]
		if !found || !reflect.DeepEqual(stored, row) {
			return fmt.Errorf("target row differs for a transferred key")
		}
	}
	return nil
}

func (target *stage4IncrementalTestTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	*target.events = append(*target.events, "target_count")
	return len(target.rows), nil
}

func (target *stage4IncrementalTestTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_finalize")
	return nil
}

func (*stage4IncrementalTestTarget) Close() error { return nil }

type stage4IncrementalTestObserver struct {
	events  *[]string
	backend stage4IncrementalTestState
	run     Stage4RunContext
	resume  bool
}

type stage4IncrementalTestState interface {
	state.Backend
	Stage4StateBackend
}

func (observer stage4IncrementalTestObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func (stage4IncrementalTestObserver) ObserveStage4SchemaDecisions(
	context.Context,
	Stage4SchemaDecisionReport,
) error {
	return nil
}

func (observer stage4IncrementalTestObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutation()
}

func (observer stage4IncrementalTestObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(*observer.events, "before_tables")
	if observer.resume {
		return nil
	}
	tasks := make([]state.Task, len(tables))
	startedAt := time.Now().UTC()
	for index, table := range tables {
		tasks[index] = state.Task{
			RunID:     observer.run.RunID,
			Table:     table,
			StartedAt: startedAt,
		}
	}
	return observer.backend.CreateTasks(tasks)
}

func (observer stage4IncrementalTestObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	*observer.events = append(*observer.events, "before:"+table)
	return nil
}

func (observer stage4IncrementalTestObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return fmt.Errorf("aggregate Stage 4 completion must not call AfterTable")
}

func TestStage4AdapterIncrementalPersistsFenceBeforeTargetMutationAndPublishesAggregate(
	t *testing.T,
) {
	events := make([]string, 0)
	first := time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	second := first.Add(time.Hour)
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	source := &stage4IncrementalTestSource{
		events: &events,
		table:  table,
		rows: []stage4IncrementalTestRow{
			{id: 1, payload: "first", updated: &first},
			{id: 2, payload: "second", updated: &second},
		},
	}
	target := &stage4IncrementalTestTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-incremental-fresh"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	cfg := config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     "source.example",
			Port:     5432,
			Database: "source",
		},
		Target: config.Endpoint{
			Type:     "postgres",
			Host:     "target.example",
			Port:     5432,
			Database: "target",
		},
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode:           config.ValidationCountOnly,
				FailOnMismatch: true,
				FailOnTimeout:  true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
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
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_sample_fence",
		"target_prepare",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_sample_fence",
		"target_write",
	)
	if len(target.rows) != 2 {
		t.Fatalf("target rows = %d", len(target.rows))
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	attempt, found, err := backend.LoadLatestCommittedIncrementalAttempt(
		runID,
		task,
	)
	if err != nil || !found {
		t.Fatalf("committed attempt found=%v err=%v", found, err)
	}
	if attempt.Status != state.IncrementalCompleted ||
		attempt.CommittedWatermark == nil ||
		attempt.CommittedWatermark.Column != "updated_at" ||
		!attempt.CommittedWatermark.Value.Equal(second) {
		t.Fatalf("committed attempt = %#v", attempt)
	}
	tasks, err := backend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 ||
		tasks[0].Status != "completed" ||
		tasks[0].RowsDone != 2 {
		t.Fatalf("ordinary tasks = %#v", tasks)
	}

	pristine := make(map[int64][]any, len(target.rows))
	for id, row := range target.rows {
		pristine[id] = cloneAdapterRow(row)
	}
	for _, test := range []struct {
		name   string
		tamper func(map[int64][]any)
		want   string
	}{
		{
			name: "delete",
			tamper: func(rows map[int64][]any) {
				delete(rows, 1)
			},
			want: "checkpoint has 2 rows and target has 1",
		},
		{
			name: "value tamper",
			tamper: func(rows map[int64][]any) {
				rows[1][1] = "tampered"
			},
			want: "revalidate exact completed Stage 4 incremental target values",
		},
	} {
		t.Run("resume rejects "+test.name, func(t *testing.T) {
			target.rows = make(map[int64][]any, len(pristine))
			for id, row := range pristine {
				target.rows[id] = cloneAdapterRow(row)
			}
			test.tamper(target.rows)
			resumeObserver := stage4IncrementalTestObserver{
				events:  &events,
				backend: backend,
				resume:  true,
				run: stage4LifecycleRunContext(
					t,
					backend,
					runID,
					true,
				),
			}
			_, err := resumeWithAdapters(
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
			if err == nil ||
				!containsStage4AdapterIncrementalText(
					err.Error(),
					test.want,
				) {
				t.Fatalf("resume error = %v", err)
			}
		})
	}

	target.rows = make(map[int64][]any, len(pristine))
	for id, row := range pristine {
		target.rows[id] = cloneAdapterRow(row)
	}
	completeStage4IncrementalTestRun(t, backend, runID)
	nextRunID := "stage4-incremental-unchanged-window"
	initializeStage4LifecycleRun(
		t,
		backend,
		nextRunID,
		time.Now().Add(-time.Minute),
	)
	delete(target.rows, 2)
	nextObserver := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run: stage4LifecycleRunContext(
			t,
			backend,
			nextRunID,
			false,
		),
	}
	_, err = migrateWithAdapters(
		context.Background(),
		cfg,
		nextObserver,
		source,
		target,
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassValidation {
		t.Fatalf(
			"unchanged-window historical-row validation error = %v",
			err,
		)
	}
}

func TestPrepareStage4AdapterIncrementalFailsClosedForUncertifiedRoute(
	t *testing.T,
) {
	cfg := config.Config{
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
		},
	}
	events := make([]string, 0)
	source := &stage4IncrementalTestSource{events: &events}
	target := &recordingAdapterTarget{events: &events}
	_, _, err := prepareStage4AdapterIncremental(
		context.Background(),
		cfg,
		source,
		target,
		stage4AdapterPrepared{mode: "upsert"},
	)
	if err == nil ||
		!stage4AdapterIncrementalErrorHas(
			err,
			"only postgres-to-postgres is currently admitted",
		) {
		t.Fatalf("error = %v", err)
	}
}

func stage4AdapterIncrementalErrorHas(err error, text string) bool {
	return err != nil && len(text) != 0 &&
		fmt.Sprintf("%v", err) != "" &&
		containsStage4AdapterIncrementalText(err.Error(), text)
}

func containsStage4AdapterIncrementalText(value, text string) bool {
	for index := 0; index+len(text) <= len(value); index++ {
		if value[index:index+len(text)] == text {
			return true
		}
	}
	return false
}

func completeStage4IncrementalTestRun(
	t *testing.T,
	backend state.Backend,
	runID string,
) {
	t.Helper()
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	var startedAt time.Time
	for _, run := range runs {
		if run.ID == runID && run.Outcome == state.Running {
			startedAt = run.StartedAt
		}
	}
	if startedAt.IsZero() {
		t.Fatalf("running state for %s was not found", runID)
	}
	if err := backend.Append(state.Run{
		ID:        runID,
		Outcome:   state.Success,
		Resumable: false,
		Reason:    "test migration completed",
		StartedAt: startedAt,
		EndedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
}
