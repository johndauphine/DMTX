package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func adapterLifecycleTable(name string) schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   name,
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "payload"},
		},
	}
}

func TestAdapterRunnerOrdersAllTableLifecycle(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterLifecycleTable("parents"),
			adapterLifecycleTable("children"),
		},
	}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	wantEvents := []string{
		"source_list",
		"source_inspect",
		"source_inspect",
		"target_plan",
		"target_preflight",
		"before_tables:parents,children",
		"target_prepare",
		"before:parents",
		"source_rows",
		"target_write",
		"rows_close",
		"source_count",
		"target_count",
		"before:children",
		"source_rows",
		"target_write",
		"rows_close",
		"source_count",
		"target_count",
		"target_finalize",
		"after:parents",
		"after:children",
	}
	if fmt.Sprint(events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(target.prepared) != 1 || len(target.finalized) != 1 {
		t.Fatalf(
			"lifecycle calls: prepare=%v finalize=%v",
			target.prepared,
			target.finalized,
		)
	}
}

func TestAdapterRunnerPreflightFailurePreventsTasksAndMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced read-only target preflight failure")
	target := &recordingAdapterTarget{
		events:       &events,
		preflightErr: forced,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		validAdapterRunnerSource(&events),
		target,
	)
	if !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	if len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf(
			"target mutated after preflight failure: prepare=%v write=%v finalize=%v",
			target.prepared,
			target.written,
			target.finalized,
		)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") ||
			strings.HasPrefix(event, "after") {
			t.Fatalf(
				"task callback ran after preflight failure: %v",
				events,
			)
		}
	}
}

func TestAdapterRunnerFinalizeFailurePreventsAllCompletion(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced target finalization failure")
	observer := &adapterMutationTestObserver{}
	target := &recordingAdapterTarget{
		events:      &events,
		finalizeErr: forced,
		protected:   &observer.active,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		observer,
		validAdapterRunnerSource(&events),
		target,
	)
	if !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	if observer.calls != 3 {
		t.Fatalf("protected mutation calls = %d, want 3", observer.calls)
	}
	if observer.after != 0 {
		t.Fatalf("finalization failure completed %d tables", observer.after)
	}
	if len(target.prepared) != 1 ||
		len(target.written) != 1 ||
		len(target.finalized) != 1 {
		t.Fatalf(
			"lifecycle calls: prepare=%v write=%v finalize=%v",
			target.prepared,
			target.written,
			target.finalized,
		)
	}
}

type failLaterAfterTableObserver struct {
	after []string
	err   error
}

func (*failLaterAfterTableObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (observer *failLaterAfterTableObserver) AfterTable(
	_ context.Context,
	table string,
	_ int,
) error {
	observer.after = append(observer.after, table)
	if len(observer.after) == 2 {
		return observer.err
	}
	return nil
}

func TestAdapterRunnerReturnsOnlyCompletedProgressWhenLaterCheckpointFails(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced second table checkpoint failure")
	observer := &failLaterAfterTableObserver{err: forced}
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterLifecycleTable("parents"),
			adapterLifecycleTable("children"),
		},
	}
	target := &recordingAdapterTarget{events: &events}

	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		observer,
		source,
		target,
	)
	if !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	want := Result{Tables: 1, Rows: 2}
	if result != want {
		t.Fatalf("partial result = %#v, want %#v", result, want)
	}
	if fmt.Sprint(observer.after) != "[parents children]" {
		t.Fatalf("AfterTable calls = %v", observer.after)
	}
}

func TestAdapterRunnerUpsertAllowsTargetOnlyRows(t *testing.T) {
	events := make([]string, 0)
	target := &recordingAdapterTarget{
		events: &events,
		rowsByTable: map[string]int{
			"items": 1,
		},
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{
			Migration: config.Migration{TargetMode: "upsert"},
		},
		recordingTableObserver{events: &events},
		validAdapterRunnerSource(&events),
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if target.rowsByTable["items"] != 3 {
		t.Fatalf(
			"target rows = %d, want target-only row plus two source rows",
			target.rowsByTable["items"],
		)
	}
}
