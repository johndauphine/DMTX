package migrate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type recordingAdapterSource struct {
	events *[]string
	table  schema.Table
	rows   []string
}

func (source *recordingAdapterSource) payloads() []string {
	if source.rows == nil {
		return []string{"first", "later"}
	}
	return source.rows
}

func (source *recordingAdapterSource) Engine() string {
	return "postgres"
}

func (source *recordingAdapterSource) DisplayName() string {
	return "PostgreSQL"
}

func (source *recordingAdapterSource) ListTables(
	context.Context,
) ([]string, error) {
	*source.events = append(*source.events, "source_list")
	return []string{source.table.Name}, nil
}

func (source *recordingAdapterSource) InspectTable(
	_ context.Context,
	name string,
) (schema.Table, error) {
	*source.events = append(*source.events, "source_inspect")
	if name != source.table.Name {
		return schema.Table{}, fmt.Errorf("unexpected table %s", name)
	}
	return source.table, nil
}

func (source *recordingAdapterSource) OpenRows(
	_ context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	*source.events = append(*source.events, "source_rows")
	if table.Schema != "public" {
		return nil, fmt.Errorf("source schema changed to %q", table.Schema)
	}
	if len(columns) != 2 || columns[0] != "id" || columns[1] != "payload" {
		return nil, fmt.Errorf("unexpected columns %#v", columns)
	}
	payloads := source.payloads()
	bufferSize := 0
	for _, payload := range payloads {
		if len(payload) > bufferSize {
			bufferSize = len(payload)
		}
	}
	return &reusingAdapterRows{
		events:   source.events,
		index:    -1,
		buffer:   make([]byte, bufferSize),
		payloads: payloads,
	}, nil
}

func (source *recordingAdapterSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	*source.events = append(*source.events, "source_count")
	return len(source.payloads()), nil
}

func (source *recordingAdapterSource) Close() error {
	*source.events = append(*source.events, "source_close")
	return nil
}

type reusingAdapterRows struct {
	events   *[]string
	index    int
	buffer   []byte
	payloads []string
}

func (rows *reusingAdapterRows) Next() bool {
	rows.index++
	return rows.index < len(rows.payloads)
}

func (rows *reusingAdapterRows) Scan(destinations ...any) error {
	if len(destinations) != 2 {
		return fmt.Errorf("destination count = %d", len(destinations))
	}
	id, ok := destinations[0].(*any)
	if !ok {
		return fmt.Errorf("id destination has type %T", destinations[0])
	}
	payload, ok := destinations[1].(*any)
	if !ok {
		return fmt.Errorf("payload destination has type %T", destinations[1])
	}
	copy(rows.buffer, rows.payloads[rows.index])
	*id = int64(rows.index + 1)
	*payload = rows.buffer
	return nil
}

func (rows *reusingAdapterRows) Err() error {
	return nil
}

func (rows *reusingAdapterRows) Close() error {
	*rows.events = append(*rows.events, "rows_close")
	return nil
}

type recordingAdapterTarget struct {
	events   *[]string
	captured [][]any
	prepared []string
	written  []string
	batches  []int
	receipt  *WriteReceipt
}

func (target *recordingAdapterTarget) Engine() string {
	return "sqlite"
}

func (target *recordingAdapterTarget) PrepareTable(
	_ context.Context,
	sourceTable schema.Table,
	mode string,
) (schema.Table, error) {
	*target.events = append(*target.events, "target_prepare")
	target.prepared = append(target.prepared, mode)
	if sourceTable.Schema != "public" {
		return schema.Table{}, fmt.Errorf(
			"prepare received source schema %q",
			sourceTable.Schema,
		)
	}
	targetTable := sourceTable
	targetTable.Schema = ""
	return targetTable, nil
}

func (target *recordingAdapterTarget) WriteBatch(
	_ context.Context,
	table schema.Table,
	_ []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	target.written = append(target.written, mode)
	target.batches = append(target.batches, len(rows))
	if table.Schema != "" {
		return WriteReceipt{}, fmt.Errorf(
			"target schema was not cleared: %q",
			table.Schema,
		)
	}
	target.captured = append(target.captured, rows...)
	if target.receipt != nil {
		return *target.receipt, nil
	}
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *recordingAdapterTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	*target.events = append(*target.events, "target_count")
	return len(target.captured), nil
}

func (target *recordingAdapterTarget) Close() error {
	*target.events = append(*target.events, "target_close")
	return nil
}

type recordingTableObserver struct {
	events *[]string
}

func (observer recordingTableObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	*observer.events = append(*observer.events, "before:"+table)
	return nil
}

func (observer recordingTableObserver) AfterTable(
	_ context.Context,
	table string,
	_ int,
) error {
	*observer.events = append(*observer.events, "after:"+table)
	return nil
}

func TestExecuteComposesIndependentSourceAndTargetAdapters(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
	target := &recordingAdapterTarget{events: &events}
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "postgres",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				events = append(events, "source_open")
				return source, nil
			},
		}},
		[]targetRole{{
			engine:     "sqlite",
			capability: testTargetCapability,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				events = append(events, "target_open")
				return target, nil
			},
		}},
		[]adapterPair{{source: "postgres", target: "sqlite"}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	result, err := executeWithRegistry(
		context.Background(),
		config.Config{
			Source: config.Endpoint{
				Type: "postgres",
			},
			Target: config.Endpoint{
				Type:     "sqlite",
				Database: "target.db",
			},
			Migration: config.Migration{
				TargetMode: "drop_recreate",
			},
		},
		recordingTableObserver{events: &events},
		registry,
	)
	if err != nil {
		t.Fatalf("executeWithRegistry: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if fmt.Sprint(target.prepared) != "[drop_recreate]" ||
		fmt.Sprint(target.written) != "[drop_recreate]" {
		t.Fatalf(
			"target modes: prepare=%v write=%v",
			target.prepared,
			target.written,
		)
	}
	if got := string(target.captured[0][1].([]byte)); got != "first" {
		t.Fatalf("first retained payload = %q, want first", got)
	}
	if got := string(target.captured[1][1].([]byte)); got != "later" {
		t.Fatalf("second retained payload = %q, want later", got)
	}
	wantEvents := []string{
		"source_open",
		"target_open",
		"source_list",
		"before:items",
		"source_inspect",
		"target_prepare",
		"source_rows",
		"target_write",
		"rows_close",
		"source_count",
		"target_count",
		"after:items",
		"target_close",
		"source_close",
	}
	if fmt.Sprint(events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
}

func TestAdapterRunnerRejectsMissingPrimaryKeyBeforeTargetMutation(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id"},
				{Name: "payload"},
			},
		},
	}
	target := &recordingAdapterTarget{events: &events}
	_, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		nil,
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "has no primary key") {
		t.Fatalf("error = %v", err)
	}
	if len(target.prepared) != 0 || len(target.written) != 0 {
		t.Fatalf(
			"target mutated before primary-key rejection: prepare=%v write=%v",
			target.prepared,
			target.written,
		)
	}
}

func TestAdapterRunnerForwardsUpsertMode(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
	target := &recordingAdapterTarget{events: &events}
	_, err := migrateWithAdapters(
		context.Background(),
		config.Config{Migration: config.Migration{TargetMode: "upsert"}},
		nil,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if fmt.Sprint(target.prepared) != "[upsert]" ||
		fmt.Sprint(target.written) != "[upsert]" {
		t.Fatalf(
			"target modes: prepare=%v write=%v",
			target.prepared,
			target.written,
		)
	}
}

func TestAdapterRunnerUsesBoundedBatches(t *testing.T) {
	events := make([]string, 0)
	payloads := make([]string, sqliteWriteBatchSize+1)
	for index := range payloads {
		payloads[index] = "value"
	}
	source := &recordingAdapterSource{
		events: &events,
		rows:   payloads,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		nil,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result.Rows != sqliteWriteBatchSize+1 {
		t.Fatalf("rows = %d", result.Rows)
	}
	if fmt.Sprint(target.batches) != fmt.Sprint(
		[]int{sqliteWriteBatchSize, 1},
	) {
		t.Fatalf("batch sizes = %v", target.batches)
	}
}

func TestAdapterRunnerRejectsNonDurableReceipt(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
	receipt := WriteReceipt{
		Certainty:     CommitUnknown,
		AttemptedRows: 2,
	}
	target := &recordingAdapterTarget{
		events:  &events,
		receipt: &receipt,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		nil,
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "did not durably commit") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
}
