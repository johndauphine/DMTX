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

type recordingAdapterSource struct {
	events *[]string
	table  schema.Table
	tables []schema.Table
	rows   []string
}

func (source *recordingAdapterSource) definitions() []schema.Table {
	if source.tables != nil {
		return source.tables
	}
	return []schema.Table{source.table}
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
	tables := source.definitions()
	names := make([]string, len(tables))
	for index, table := range tables {
		names[index] = table.Name
	}
	return names, nil
}

func (source *recordingAdapterSource) InspectTable(
	_ context.Context,
	name string,
) (schema.Table, error) {
	*source.events = append(*source.events, "source_inspect")
	for _, table := range source.definitions() {
		if name == table.Name {
			return table, nil
		}
	}
	return schema.Table{}, fmt.Errorf("unexpected table %s", name)
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
	events       *[]string
	captured     [][]any
	rowsByTable  map[string]int
	prepared     []string
	preflighted  []string
	written      []string
	planned      []string
	finalized    []string
	batches      []int
	receipt      *WriteReceipt
	writeErr     error
	preflightErr error
	prepareErr   error
	finalizeErr  error
	protected    *bool
}

func (target *recordingAdapterTarget) Engine() string {
	return "sqlite"
}

func (target *recordingAdapterTarget) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	*target.events = append(*target.events, "target_plan")
	if sourceEngine != "postgres" {
		return nil, fmt.Errorf(
			"unexpected source engine %q",
			sourceEngine,
		)
	}
	targetTables := make([]schema.Table, 0, len(sourceTables))
	for _, sourceTable := range sourceTables {
		target.planned = append(target.planned, mode)
		if sourceTable.Schema != "public" {
			return nil, fmt.Errorf(
				"plan received source schema %q",
				sourceTable.Schema,
			)
		}
		targetTable := sourceTable
		targetTable.Schema = ""
		targetTables = append(targetTables, targetTable)
	}
	return targetTables, nil
}

func (target *recordingAdapterTarget) PreflightTables(
	_ context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	*target.events = append(*target.events, "target_preflight")
	target.preflighted = append(target.preflighted, mode)
	for _, targetTable := range targetTables {
		if targetTable.Schema != "" {
			return fmt.Errorf(
				"target schema was not cleared: %q",
				targetTable.Schema,
			)
		}
	}
	return target.preflightErr
}

func (target *recordingAdapterTarget) PrepareTables(
	_ context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	*target.events = append(*target.events, "target_prepare")
	target.prepared = append(target.prepared, mode)
	if target.protected != nil && !*target.protected {
		return fmt.Errorf("prepare was not protected")
	}
	for _, targetTable := range targetTables {
		if targetTable.Schema != "" {
			return fmt.Errorf(
				"target schema was not cleared: %q",
				targetTable.Schema,
			)
		}
	}
	return target.prepareErr
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
	if target.protected != nil && !*target.protected {
		return WriteReceipt{}, fmt.Errorf("write was not protected")
	}
	if table.Schema != "" {
		return WriteReceipt{}, fmt.Errorf(
			"target schema was not cleared: %q",
			table.Schema,
		)
	}
	target.captured = append(target.captured, rows...)
	if target.rowsByTable == nil {
		target.rowsByTable = make(map[string]int)
	}
	target.rowsByTable[table.Name] += len(rows)
	if target.receipt != nil {
		return *target.receipt, target.writeErr
	}
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, target.writeErr
}

func (target *recordingAdapterTarget) CountRows(
	_ context.Context,
	table schema.Table,
) (int, error) {
	*target.events = append(*target.events, "target_count")
	if target.rowsByTable == nil {
		return 0, nil
	}
	return target.rowsByTable[table.Name], nil
}

func (target *recordingAdapterTarget) FinalizeTables(
	_ context.Context,
	_ []schema.Table,
	mode string,
) error {
	*target.events = append(*target.events, "target_finalize")
	target.finalized = append(target.finalized, mode)
	if target.protected != nil && !*target.protected {
		return fmt.Errorf("finalize was not protected")
	}
	return target.finalizeErr
}

func (target *recordingAdapterTarget) Close() error {
	*target.events = append(*target.events, "target_close")
	return nil
}

type destructivePreflightRecordingTarget struct {
	*recordingAdapterTarget
	migrations []config.Migration
	err        error
}

func (target *destructivePreflightRecordingTarget) PreflightDestructive(
	_ context.Context,
	_ []schema.Table,
	migration config.Migration,
) error {
	*target.events = append(
		*target.events,
		"target_destructive_preflight",
	)
	target.migrations = append(target.migrations, migration)
	return target.err
}

type recordingTableObserver struct {
	events *[]string
}

func (observer recordingTableObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(
		*observer.events,
		"before_tables:"+strings.Join(tables, ","),
	)
	return nil
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
		fmt.Sprint(target.written) != "[drop_recreate]" ||
		fmt.Sprint(target.finalized) != "[drop_recreate]" {
		t.Fatalf(
			"target modes: prepare=%v write=%v finalize=%v",
			target.prepared,
			target.written,
			target.finalized,
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
		"source_inspect",
		"target_plan",
		"target_preflight",
		"before_tables:items",
		"target_prepare",
		"before:items",
		"source_rows",
		"target_write",
		"rows_close",
		"source_count",
		"target_count",
		"target_finalize",
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

func TestAdapterRunnerRunsDestructivePreflightBeforeCheckpointOrMutation(
	t *testing.T,
) {
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
	baseTarget := &recordingAdapterTarget{events: &events}
	target := &destructivePreflightRecordingTarget{
		recordingAdapterTarget: baseTarget,
		err:                    ErrDestructiveAcknowledgement,
	}
	cfg := config.Config{
		Migration: config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
		},
	}
	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf("destructive preflight error = %v", err)
	}
	if len(target.migrations) != 1 ||
		target.migrations[0].TargetMode != "drop_recreate" ||
		!target.migrations[0].DestructiveAcknowledged {
		t.Fatalf(
			"destructive preflight migrations = %+v",
			target.migrations,
		)
	}
	if len(baseTarget.prepared) != 0 ||
		len(baseTarget.written) != 0 ||
		len(baseTarget.finalized) != 0 {
		t.Fatalf(
			"destructive preflight failure mutated target: prepare=%v write=%v finalize=%v",
			baseTarget.prepared,
			baseTarget.written,
			baseTarget.finalized,
		)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") {
			t.Fatalf(
				"checkpoint ran before destructive preflight: %v",
				events,
			)
		}
	}
	wantEvents := []string{
		"source_list",
		"source_inspect",
		"target_plan",
		"target_preflight",
		"target_destructive_preflight",
	}
	if fmt.Sprint(events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
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
		fmt.Sprint(target.preflighted) != "[upsert]" ||
		fmt.Sprint(target.written) != "[upsert]" ||
		fmt.Sprint(target.finalized) != "[upsert]" {
		t.Fatalf(
			"target modes: preflight=%v prepare=%v write=%v finalize=%v",
			target.preflighted,
			target.prepared,
			target.written,
			target.finalized,
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

func TestAdapterRunnerPlansAllTablesBeforeTargetMutation(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			{
				Schema: "public",
				Name:   "first",
				Columns: []schema.Column{
					{Name: "id", PrimaryKey: true},
				},
			},
			{
				Schema:  "public",
				Name:    "invalid",
				Columns: []schema.Column{{Name: "id"}},
			},
		},
	}
	target := &recordingAdapterTarget{events: &events}
	_, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid has no primary key") {
		t.Fatalf("error = %v", err)
	}
	if len(target.planned) != 0 {
		t.Fatalf("target planning ran before source inspection completed: %v", target.planned)
	}
	if len(target.prepared) != 0 || len(target.written) != 0 {
		t.Fatalf(
			"target mutated during preflight: prepare=%v write=%v",
			target.prepared,
			target.written,
		)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") {
			t.Fatalf("checkpoint created before preflight completed: %v", events)
		}
	}
}

type adapterMutationTestObserver struct {
	active   bool
	calls    int
	after    int
	failCall int
	failErr  error
}

func (*adapterMutationTestObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (observer *adapterMutationTestObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.after++
	return nil
}

func (observer *adapterMutationTestObserver) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	observer.calls++
	observer.active = true
	err := mutation()
	observer.active = false
	if err != nil {
		return err
	}
	if observer.calls == observer.failCall {
		return observer.failErr
	}
	return nil
}

func TestAdapterRunnerFencesPrepareAndWrite(t *testing.T) {
	events := make([]string, 0)
	observer := &adapterMutationTestObserver{}
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
	target := &recordingAdapterTarget{
		events:    &events,
		protected: &observer.active,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
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
	if observer.calls != 3 || observer.after != 1 {
		t.Fatalf(
			"observer calls = %d, after = %d",
			observer.calls,
			observer.after,
		)
	}
}

func TestAdapterRunnerUnknownReceiptPreservesCauseWithoutCheckpoint(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced commit response loss")
	receipt := WriteReceipt{
		Certainty:     CommitUnknown,
		AttemptedRows: 2,
	}
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
	target := &recordingAdapterTarget{
		events:   &events,
		receipt:  &receipt,
		writeErr: forced,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if !errors.Is(err, forced) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "commit outcome is unknown") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	for _, event := range events {
		if event == "after:items" {
			t.Fatalf("unknown commit was checkpointed: %v", events)
		}
	}
}

func TestAdapterRunnerFenceFailureAfterDurableWriteDoesNotCheckpoint(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced lease coordinator commit failure")
	observer := &adapterMutationTestObserver{
		failCall: 2,
		failErr:  forced,
	}
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
	target := &recordingAdapterTarget{
		events:    &events,
		protected: &observer.active,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		observer,
		source,
		target,
	)
	if !errors.Is(err, forced) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "after reporting commit certainty durable") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if observer.after != 0 {
		t.Fatalf("durable write with failed fence was checkpointed")
	}
	if len(target.captured) != 2 {
		t.Fatalf("captured rows = %d, want durable target write", len(target.captured))
	}
}

func TestCloneAdapterRowPreservesNonNullEmptyBlob(t *testing.T) {
	source := make([]byte, 0)
	cloned := cloneAdapterRow([]any{source, nil})
	blob, ok := cloned[0].([]byte)
	if !ok || blob == nil || len(blob) != 0 {
		t.Fatalf("cloned empty blob = %#v (%T)", cloned[0], cloned[0])
	}
	if cloned[1] != nil {
		t.Fatalf("SQL NULL changed to %#v", cloned[1])
	}
}
