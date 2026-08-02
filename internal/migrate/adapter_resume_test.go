package migrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type resumeLifecycleTarget struct {
	*recordingAdapterTarget
	preparedTables  [][]string
	finalizedTables [][]string
}

func (target *resumeLifecycleTarget) PrepareTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	target.preparedTables = append(
		target.preparedTables,
		resumeTableNames(tables),
	)
	return target.recordingAdapterTarget.PrepareTables(
		ctx,
		tables,
		mode,
	)
}

func (target *resumeLifecycleTarget) FinalizeTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	target.finalizedTables = append(
		target.finalizedTables,
		resumeTableNames(tables),
	)
	return target.recordingAdapterTarget.FinalizeTables(
		ctx,
		tables,
		mode,
	)
}

func resumeTableNames(tables []schema.Table) []string {
	names := make([]string, len(tables))
	for index, table := range tables {
		names[index] = table.Name
	}
	return names
}

func newResumeTestRegistry(
	t *testing.T,
	source sourceAdapter,
	target targetAdapter,
) adapterRegistry {
	t.Helper()
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: source.Engine(),
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				if events := resumeTestEvents(source, target); events != nil {
					*events = append(*events, "source_open")
				}
				return source, nil
			},
		}},
		[]targetRole{{
			engine:     target.Engine(),
			capability: testTargetCapability,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				if events := resumeTestEvents(source, target); events != nil {
					*events = append(*events, "target_open")
				}
				return target, nil
			},
		}},
		[]adapterPair{{
			source: source.Engine(),
			target: target.Engine(),
		}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	return registry
}

func resumeTestEvents(
	source sourceAdapter,
	target targetAdapter,
) *[]string {
	if recording, ok := source.(*recordingAdapterSource); ok {
		return recording.events
	}
	if recording, ok := target.(*resumeLifecycleTarget); ok {
		return recording.events
	}
	return nil
}

func resumeTestConfig() config.Config {
	return config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     "source.example.test",
			Database: "source",
		},
		Target: config.Endpoint{
			Type:     "sqlite",
			Database: "target.db",
		},
		Migration: config.Migration{
			TargetMode: "upsert",
		},
	}
}

func TestAdapterResumeReplaysOnlyIncompleteTables(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterLifecycleTable("parents"),
			adapterLifecycleTable("children"),
		},
	}
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
			rowsByTable: map[string]int{
				"parents": 2,
			},
		},
	}
	observer := recordingTableObserver{events: &events}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		CompletedTableCheckpoints{
			"parents": {Rows: 2},
		},
		observer,
		newResumeTestRegistry(t, source, target),
	)
	if err != nil {
		t.Fatalf("executeResumeWithRegistry: %v", err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if fmt.Sprint(target.preparedTables) != "[[children]]" {
		t.Fatalf("prepared tables = %v", target.preparedTables)
	}
	if fmt.Sprint(target.finalizedTables) != "[[children]]" {
		t.Fatalf("finalized tables = %v", target.finalizedTables)
	}
	wantEvents := []string{
		"source_open",
		"target_open",
		"source_list",
		"source_inspect",
		"source_inspect",
		"target_plan",
		"target_preflight",
		"source_count",
		"target_count",
		"before_tables:children,parents",
		"target_prepare",
		"before:children",
		"source_rows",
		"target_write",
		"rows_close",
		"source_count",
		"target_count",
		"target_finalize",
		"after:children",
		"target_close",
		"source_close",
	}
	if fmt.Sprint(events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
}

func TestAdapterResumeValidatesCompletedCountsBeforeTaskOrMutation(
	t *testing.T,
) {
	tests := []struct {
		name       string
		checkpoint int
		targetRows int
		want       string
	}{
		{
			name:       "source drift with target superset",
			checkpoint: 1,
			targetRows: 3,
			want:       "checkpoint has 1 rows, source has 2 rows, target has 3 rows",
		},
		{
			name:       "target mismatch",
			checkpoint: 2,
			targetRows: 1,
			want:       "checkpoint has 2 rows, source has 2 rows, target has 1 rows",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			source := validAdapterRunnerSource(&events)
			target := &resumeLifecycleTarget{
				recordingAdapterTarget: &recordingAdapterTarget{
					events: &events,
					rowsByTable: map[string]int{
						"items": test.targetRows,
					},
				},
			}
			result, err := executeResumeWithRegistry(
				context.Background(),
				resumeTestConfig(),
				CompletedTableCheckpoints{
					"items": {Rows: test.checkpoint},
				},
				recordingTableObserver{events: &events},
				newResumeTestRegistry(t, source, target),
			)
			if result != (Result{}) ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("result = %#v, error = %v", result, err)
			}
			for _, event := range events {
				if strings.HasPrefix(event, "before") ||
					event == "target_prepare" ||
					event == "target_write" ||
					event == "target_finalize" {
					t.Fatalf(
						"checkpoint mismatch reached task or mutation: %v",
						events,
					)
				}
			}
		})
	}
}

func TestAdapterResumeCompletedUpsertAllowsTargetSuperset(t *testing.T) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
			rowsByTable: map[string]int{
				"items": 3,
			},
		},
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if err != nil {
		t.Fatalf("executeResumeWithRegistry: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if len(target.preparedTables) != 0 ||
		len(target.written) != 0 ||
		len(target.finalizedTables) != 0 {
		t.Fatalf(
			"completed superset resume mutated target: prepare=%v write=%v finalize=%v",
			target.preparedTables,
			target.written,
			target.finalizedTables,
		)
	}
}

func TestAdapterResumeStrictReconciliationRejectsTargetSuperset(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
			rowsByTable: map[string]int{
				"items": 3,
			},
		},
	}
	cfg := resumeTestConfig()
	cfg.Migration.Deletes.Mode = config.DeleteModeReconcile

	result, err := executeResumeWithRegistry(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(
			err.Error(),
			"checkpoint has 2 rows, source has 2 rows, target has 3 rows",
		) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") ||
			event == "target_prepare" ||
			event == "target_write" ||
			event == "target_finalize" {
			t.Fatalf(
				"strict checkpoint mismatch reached task or mutation: %v",
				events,
			)
		}
	}
}

func TestAdapterResumeReplaysWholeIncompleteUpsertTable(t *testing.T) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &keyedResumeTarget{
		resumeLifecycleTarget: &resumeLifecycleTarget{
			recordingAdapterTarget: &recordingAdapterTarget{
				events: &events,
			},
		},
		rows: map[int64]string{
			1: "stale",
		},
	}
	registry := newResumeTestRegistry(t, source, target)
	observer := recordingTableObserver{events: &events}

	for attempt := 1; attempt <= 2; attempt++ {
		result, err := executeResumeWithRegistry(
			context.Background(),
			resumeTestConfig(),
			nil,
			observer,
			registry,
		)
		if err != nil {
			t.Fatalf(
				"executeResumeWithRegistry attempt %d: %v",
				attempt,
				err,
			)
		}
		if result != (Result{
			Tables:    1,
			Rows:      2,
			Validated: true,
		}) {
			t.Fatalf("attempt %d result = %#v", attempt, result)
		}
		wantRows := map[int64]string{
			1: "first",
			2: "later",
		}
		if !reflect.DeepEqual(target.rows, wantRows) {
			t.Fatalf(
				"attempt %d keyed rows = %#v, want %#v",
				attempt,
				target.rows,
				wantRows,
			)
		}
	}
	if fmt.Sprint(target.batches) != "[2 2]" {
		t.Fatalf(
			"replayed batches = %v, want both full source replays",
			target.batches,
		)
	}
	if fmt.Sprint(target.written) != "[upsert upsert]" {
		t.Fatalf("write modes = %v", target.written)
	}
}

type keyedResumeTarget struct {
	*resumeLifecycleTarget
	rows map[int64]string
}

func (target *keyedResumeTarget) WriteBatch(
	_ context.Context,
	table schema.Table,
	_ []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	target.written = append(target.written, mode)
	target.batches = append(target.batches, len(rows))
	if mode != "upsert" {
		return WriteReceipt{}, fmt.Errorf(
			"keyed resume target received mode %q",
			mode,
		)
	}
	if table.Name != "items" {
		return WriteReceipt{}, fmt.Errorf(
			"keyed resume target received table %q",
			table.Name,
		)
	}
	for _, row := range rows {
		if len(row) != 2 {
			return WriteReceipt{}, fmt.Errorf(
				"keyed resume row has %d values",
				len(row),
			)
		}
		key, ok := row[0].(int64)
		if !ok {
			return WriteReceipt{}, fmt.Errorf(
				"keyed resume key has type %T",
				row[0],
			)
		}
		var value string
		switch payload := row[1].(type) {
		case []byte:
			value = string(payload)
		case string:
			value = payload
		default:
			return WriteReceipt{}, fmt.Errorf(
				"keyed resume payload has type %T",
				row[1],
			)
		}
		target.rows[key] = value
	}
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *keyedResumeTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	*target.events = append(*target.events, "target_count")
	return len(target.rows), nil
}

func TestAdapterResumeAllCompletedCheckpointsSkipLifecycle(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
			rowsByTable: map[string]int{
				"items": 2,
			},
		},
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if err != nil {
		t.Fatalf("executeResumeWithRegistry: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if len(target.preparedTables) != 0 ||
		len(target.written) != 0 ||
		len(target.finalizedTables) != 0 {
		t.Fatalf(
			"completed-only resume mutated target: prepare=%v write=%v finalize=%v",
			target.preparedTables,
			target.written,
			target.finalizedTables,
		)
	}
	if !containsResumeEvent(events, "before_tables:items") {
		t.Fatalf("table set was not checkpointed: %v", events)
	}
}

func TestAdapterResumeRejectsCheckpointOutsideCurrentSelection(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		CompletedTableCheckpoints{
			"removed_table": {Rows: 0},
		},
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(
			err.Error(),
			"outside the current selection",
		) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if containsResumeEvent(events, "target_prepare") {
		t.Fatalf("unknown checkpoint reached mutation: %v", events)
	}
}

func TestAdapterResumeRejectsConfiguredSameEngineAliasBeforeOpen(
	t *testing.T,
) {
	opened := 0
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "postgres",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				opened++
				return nil, errors.New("must not open")
			},
		}},
		[]targetRole{{
			engine:     "postgres",
			capability: testTargetCapability,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				opened++
				return nil, errors.New("must not open")
			},
		}},
		[]adapterPair{{source: "postgres", target: "postgres"}},
		nil,
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	cfg := resumeTestConfig()
	cfg.Source.Host = "DB.EXAMPLE.TEST"
	cfg.Source.Database = "same"
	cfg.Target = config.Endpoint{
		Type:     "postgres",
		Host:     "db.example.test",
		Port:     5432,
		Database: "same",
	}

	_, err = executeResumeWithRegistry(
		context.Background(),
		cfg,
		nil,
		recordingTableObserver{events: &[]string{}},
		registry,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "same endpoint") {
		t.Fatalf("same-alias error = %v", err)
	}
	if opened != 0 {
		t.Fatalf("same configured alias opened %d adapters", opened)
	}
}

type postgresResumeTarget struct {
	*resumeLifecycleTarget
}

func (*postgresResumeTarget) Engine() string {
	return "postgres"
}

func TestAdapterResumeSameEngineFailsClosedWithoutLiveIdentity(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &postgresResumeTarget{
		resumeLifecycleTarget: &resumeLifecycleTarget{
			recordingAdapterTarget: &recordingAdapterTarget{
				events: &events,
			},
		},
	}
	cfg := resumeTestConfig()
	cfg.Target = config.Endpoint{
		Type:     "postgres",
		Host:     "target.example.test",
		Database: "target",
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		cfg,
		nil,
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		err == nil ||
		!strings.Contains(
			err.Error(),
			"cannot verify distinct live source and target databases",
		) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if containsResumeEvent(events, "source_list") {
		t.Fatalf("unverified same-engine route reached discovery: %v", events)
	}
}

func TestRequireDistinctLiveSQLiteDatabasesRejectsPhysicalAliases(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createSQLiteSourceTestDatabase(
		t,
		sourcePath,
		`CREATE TABLE items (id INTEGER PRIMARY KEY); INSERT INTO items VALUES (1)`,
	)
	distinctPath := filepath.Join(directory, "target.db")
	createSQLiteSourceTestDatabase(
		t,
		distinctPath,
		`CREATE TABLE items (id INTEGER PRIMARY KEY); INSERT INTO items VALUES (2)`,
	)
	symlinkPath := filepath.Join(directory, "source-symlink.db")
	if err := os.Symlink(sourcePath, symlinkPath); err != nil {
		t.Fatalf("create SQLite source symlink: %v", err)
	}
	hardLinkPath := filepath.Join(directory, "source-hardlink.db")
	if err := os.Link(sourcePath, hardLinkPath); err != nil {
		t.Fatalf("create SQLite source hard link: %v", err)
	}

	for _, test := range []struct {
		name       string
		targetPath string
		wantReject bool
	}{
		{name: "same path", targetPath: sourcePath, wantReject: true},
		{name: "symlink alias", targetPath: symlinkPath, wantReject: true},
		{name: "hard-link alias", targetPath: hardLinkPath, wantReject: true},
		{name: "distinct file", targetPath: distinctPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawSource, err := openSQLiteSourceAdapter(
				ctx,
				config.Endpoint{Type: "sqlite", Database: sourcePath},
			)
			if err != nil {
				t.Fatalf("open source: %v", err)
			}
			source := rawSource.(*sqliteSourceAdapter)
			t.Cleanup(func() {
				if closeErr := source.Close(); closeErr != nil {
					t.Errorf("close source: %v", closeErr)
				}
			})
			rawTarget, err := openSQLiteTargetAdapter(
				ctx,
				config.Endpoint{Type: "sqlite", Database: test.targetPath},
			)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			target := rawTarget.(*sqliteTargetAdapter)
			t.Cleanup(func() {
				if closeErr := target.Close(); closeErr != nil {
					t.Errorf("close target: %v", closeErr)
				}
			})

			err = requireDistinctLiveSQLiteDatabases(ctx, source, target)
			if test.wantReject {
				if err == nil || !strings.Contains(err.Error(), "requires distinct") {
					t.Fatalf("alias guard error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("distinct databases rejected: %v", err)
			}
		})
	}
}

type tableOnlyResumeObserver struct{}

func (tableOnlyResumeObserver) BeforeTable(context.Context, string) error {
	return nil
}

func (tableOnlyResumeObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func TestAdapterResumeRequiresTableSetCheckpointObserver(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		tableOnlyResumeObserver{},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "table-set observer") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

type typedNilResumeObserver struct{}

func (*typedNilResumeObserver) BeforeTables(
	context.Context,
	[]string,
) error {
	return nil
}

func (*typedNilResumeObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (*typedNilResumeObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func TestAdapterResumeRejectsNilObserversWithoutOpeningAdapters(
	t *testing.T,
) {
	tests := []struct {
		name     string
		observer TableObserver
	}{
		{name: "nil"},
		{
			name:     "typed nil",
			observer: (*typedNilResumeObserver)(nil),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			source := validAdapterRunnerSource(&events)
			target := &resumeLifecycleTarget{
				recordingAdapterTarget: &recordingAdapterTarget{
					events: &events,
				},
			}
			result, err := executeResumeWithRegistry(
				context.Background(),
				resumeTestConfig(),
				nil,
				test.observer,
				newResumeTestRegistry(t, source, target),
			)
			if result != (Result{}) ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), "table-set observer") {
				t.Fatalf("result = %#v, error = %v", result, err)
			}
			if len(events) != 0 {
				t.Fatalf("nil observer opened adapters: %v", events)
			}
		})
	}
}

func TestAdapterResumePreCanceledContextOpensNothing(t *testing.T) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := executeResumeWithRegistry(
		ctx,
		resumeTestConfig(),
		nil,
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, context.Canceled) ||
		ClassifyTransferError(err) != ErrorClassCanceled {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(events) != 0 {
		t.Fatalf("pre-canceled resume opened adapters: %v", events)
	}
}

type cancelAfterTableSetResumeObserver struct {
	events *[]string
	cancel context.CancelFunc
}

func (observer *cancelAfterTableSetResumeObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(
		*observer.events,
		"before_tables:"+strings.Join(tables, ","),
	)
	observer.cancel()
	return nil
}

func (*cancelAfterTableSetResumeObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (*cancelAfterTableSetResumeObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func TestAdapterResumeCancellationAfterTaskSetPreventsMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	observer := &cancelAfterTableSetResumeObserver{
		events: &events,
		cancel: cancel,
	}

	result, err := executeResumeWithRegistry(
		ctx,
		resumeTestConfig(),
		nil,
		observer,
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, context.Canceled) ||
		ClassifyTransferError(err) != ErrorClassCanceled {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(target.preparedTables) != 0 ||
		len(target.written) != 0 ||
		len(target.finalizedTables) != 0 {
		t.Fatalf(
			"post-task cancellation mutated target: prepare=%v write=%v finalize=%v",
			target.preparedTables,
			target.written,
			target.finalizedTables,
		)
	}
}

type cancelAfterFinalizeResumeTarget struct {
	*resumeLifecycleTarget
	cancel context.CancelFunc
}

func (target *cancelAfterFinalizeResumeTarget) FinalizeTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	err := target.resumeLifecycleTarget.FinalizeTables(
		ctx,
		tables,
		mode,
	)
	target.cancel()
	return err
}

func TestAdapterResumeCancellationAfterFinalizeCannotCompleteTasks(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	ctx, cancel := context.WithCancel(context.Background())
	target := &cancelAfterFinalizeResumeTarget{
		resumeLifecycleTarget: &resumeLifecycleTarget{
			recordingAdapterTarget: &recordingAdapterTarget{
				events: &events,
			},
		},
		cancel: cancel,
	}

	result, err := executeResumeWithRegistry(
		ctx,
		resumeTestConfig(),
		nil,
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, context.Canceled) ||
		ClassifyTransferError(err) != ErrorClassCanceled {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if !containsResumeEvent(events, "target_finalize") {
		t.Fatalf("finalization did not run: %v", events)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "after:") {
			t.Fatalf(
				"post-finalize cancellation completed a task: %v",
				events,
			)
		}
	}
}

func TestAdapterResumeRejectsDropRecreateWithoutOpeningAdapters(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	cfg := resumeTestConfig()
	cfg.Migration.TargetMode = "drop_recreate"
	result, err := executeResumeWithRegistry(
		context.Background(),
		cfg,
		nil,
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(
			err.Error(),
			"requires a duplicate-safe rebuild replay protocol",
		) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(events) != 0 {
		t.Fatalf("drop/recreate resume opened adapters: %v", events)
	}
}

func TestAdapterResumeRejectsCompatibilityOverride(t *testing.T) {
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "sqlite",
		}},
		[]targetRole{{
			engine:     "sqlite",
			capability: testTargetCapability,
		}},
		[]adapterPair{{source: "sqlite", target: "sqlite"}},
		[]adapterOverride{{
			pair: adapterPair{source: "sqlite", target: "sqlite"},
			run:  noOpMigrationRunner,
		}},
	)
	if err != nil {
		t.Fatalf("newAdapterRegistry: %v", err)
	}
	cfg := config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: "source.db",
		},
		Target: config.Endpoint{
			Type:     "sqlite",
			Database: "target.db",
		},
		Migration: config.Migration{
			TargetMode: "upsert",
		},
	}
	result, err := executeResumeWithRegistry(
		context.Background(),
		cfg,
		nil,
		recordingTableObserver{events: &[]string{}},
		registry,
	)
	if result != (Result{}) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "compatibility override") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestAdapterResumeTaskCheckpointFailurePreventsMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	forced := errors.New("forced task-set checkpoint failure")
	observer := &failingResumeTaskObserver{
		events: &events,
		err:    forced,
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		observer,
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, forced) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(target.preparedTables) != 0 ||
		len(target.written) != 0 ||
		len(target.finalizedTables) != 0 {
		t.Fatalf(
			"task checkpoint failure mutated target: prepare=%v write=%v finalize=%v",
			target.preparedTables,
			target.written,
			target.finalizedTables,
		)
	}
}

type tableCheckpointFailureResumeObserver struct {
	events    *[]string
	beforeErr error
	afterErr  error
	after     []string
}

func (observer *tableCheckpointFailureResumeObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(
		*observer.events,
		"before_tables:"+strings.Join(tables, ","),
	)
	return nil
}

func (observer *tableCheckpointFailureResumeObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	*observer.events = append(*observer.events, "before:"+table)
	return observer.beforeErr
}

func (observer *tableCheckpointFailureResumeObserver) AfterTable(
	_ context.Context,
	table string,
	_ int,
) error {
	observer.after = append(observer.after, table)
	*observer.events = append(*observer.events, "after:"+table)
	if len(observer.after) == 2 {
		return observer.afterErr
	}
	return nil
}

func TestAdapterResumeBeforeTableFailureIsStateError(t *testing.T) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	forced := errors.New("forced before-table checkpoint failure")
	observer := &tableCheckpointFailureResumeObserver{
		events:    &events,
		beforeErr: forced,
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		observer,
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, forced) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(target.written) != 0 ||
		len(target.finalizedTables) != 0 {
		t.Fatalf(
			"before-table checkpoint failure advanced lifecycle: write=%v finalize=%v",
			target.written,
			target.finalizedTables,
		)
	}
}

func TestAdapterResumeAfterTableFailureReturnsTruthfulPartialResult(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			adapterLifecycleTable("parents"),
			adapterLifecycleTable("children"),
		},
	}
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	forced := errors.New("forced second completion checkpoint failure")
	observer := &tableCheckpointFailureResumeObserver{
		events:   &events,
		afterErr: forced,
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		observer,
		newResumeTestRegistry(t, source, target),
	)
	want := Result{Tables: 1, Rows: 2}
	if result != want ||
		!errors.Is(err, forced) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf(
			"result = %#v, want %#v, error = %v",
			result,
			want,
			err,
		)
	}
	if fmt.Sprint(observer.after) != "[children parents]" {
		t.Fatalf("AfterTable calls = %v", observer.after)
	}
}

func TestAdapterResumeCheckpointCancellationPreservesIdentity(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events: &events,
		},
	}
	observer := &failingResumeTaskObserver{
		events: &events,
		err:    context.Canceled,
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		observer,
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) ||
		!errors.Is(err, context.Canceled) ||
		ClassifyTransferError(err) != ErrorClassCanceled {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if len(target.preparedTables) != 0 {
		t.Fatalf(
			"canceled task checkpoint prepared target: %v",
			target.preparedTables,
		)
	}
}

func TestAdapterResumeRerunsPreflightBeforeTaskOrMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := validAdapterRunnerSource(&events)
	forced := errors.New("forced resume preflight failure")
	target := &resumeLifecycleTarget{
		recordingAdapterTarget: &recordingAdapterTarget{
			events:       &events,
			preflightErr: forced,
		},
	}

	result, err := executeResumeWithRegistry(
		context.Background(),
		resumeTestConfig(),
		nil,
		recordingTableObserver{events: &events},
		newResumeTestRegistry(t, source, target),
	)
	if result != (Result{}) || !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if !containsResumeEvent(events, "target_preflight") {
		t.Fatalf("target preflight was not rerun: %v", events)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") ||
			event == "target_prepare" ||
			event == "target_write" ||
			event == "target_finalize" {
			t.Fatalf(
				"preflight failure reached task or mutation: %v",
				events,
			)
		}
	}
}

type failingResumeTaskObserver struct {
	events *[]string
	err    error
}

func (observer *failingResumeTaskObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(
		*observer.events,
		"before_tables:"+strings.Join(tables, ","),
	)
	return observer.err
}

func (*failingResumeTaskObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (*failingResumeTaskObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func containsResumeEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
