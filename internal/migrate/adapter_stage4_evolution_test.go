package migrate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4AdapterEvolutionSource struct {
	*recordingAdapterSource
}

func (*stage4AdapterEvolutionSource) Engine() string {
	return "postgres"
}

type stage4AdapterEvolutionTarget struct {
	events *[]string

	catalog                     TargetSchemaEvolutionCatalog
	evolutionPreflightCalls     int
	applyCalls                  int
	reverifyFailuresRemaining   int
	reverifyErr                 error
	applyErr                    error
	substituteReverifyDigest    bool
	returnTypedNilCreatePlanner bool
	preflightCalls              int
	preflightTables             [][]schema.Table
	postEvolutionPreflightErr   error
	isolationTables             [][]schema.Table
	writeTables                 []schema.Table

	rows int
	keys map[string]struct{}
}

func (*stage4AdapterEvolutionTarget) Engine() string {
	return "postgres"
}

func (*stage4AdapterEvolutionTarget) stage4NetworkIdempotentUpsertTarget() {}

func (target *stage4AdapterEvolutionTarget) PreflightStage4NetworkReplayIsolation(
	_ context.Context,
	tables []schema.Table,
) error {
	target.isolationTables = append(
		target.isolationTables,
		cloneTargetSchemaEvolutionTables(tables),
	)
	return nil
}

func (target *stage4AdapterEvolutionTarget) PlanTables(
	_ string,
	sourceTables []schema.Table,
	_ string,
) ([]schema.Table, error) {
	*target.events = append(*target.events, "target_plan")
	result := make([]schema.Table, len(sourceTables))
	for index, table := range sourceTables {
		result[index] = cloneStage4RichTable(table)
		result[index].Schema = "tenant"
	}
	return result, nil
}

func (target *stage4AdapterEvolutionTarget) PreflightTables(
	_ context.Context,
	tables []schema.Table,
	_ string,
) error {
	target.preflightCalls++
	target.preflightTables = append(
		target.preflightTables,
		cloneTargetSchemaEvolutionTables(tables),
	)
	*target.events = append(*target.events, "target_preflight")
	if target.preflightCalls > 1 &&
		target.postEvolutionPreflightErr != nil {
		return target.postEvolutionPreflightErr
	}
	return nil
}

func (target *stage4AdapterEvolutionTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_prepare")
	return nil
}

func (target *stage4AdapterEvolutionTarget) WriteBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	target.rows += len(rows)
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *stage4AdapterEvolutionTarget) WriteStage4NetworkBatch(
	_ context.Context,
	table schema.Table,
	_ []string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	target.writeTables = append(
		target.writeTables,
		cloneStage4RichTable(table),
	)
	if target.keys == nil {
		target.keys = make(map[string]struct{})
	}
	committed := int64(0)
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		key := fmt.Sprintf("%T:%v", row[0], row[0])
		if _, exists := target.keys[key]; exists {
			continue
		}
		target.keys[key] = struct{}{}
		committed++
	}
	target.rows += int(committed)
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: int64(len(rows)),
		CommittedRows: int64(len(rows)),
	}, nil
}

func (target *stage4AdapterEvolutionTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	recordingAdapterCountEventsMu.Lock()
	defer recordingAdapterCountEventsMu.Unlock()
	*target.events = append(*target.events, "target_count")
	return target.rows, nil
}

func (target *stage4AdapterEvolutionTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_finalize")
	return nil
}

func (*stage4AdapterEvolutionTarget) Close() error {
	return nil
}

func (*stage4AdapterEvolutionTarget) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.Postgres
}

type stage4AdapterTypedNilCreatePlanner struct{}

func (*stage4AdapterTypedNilCreatePlanner) PlanCompleteTargetSchemaCreates(
	schema.Dialect,
	[]schema.Table,
	[]schema.Table,
	TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	panic("typed-nil create planner must not be invoked")
}

func (target *stage4AdapterEvolutionTarget) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	if target.returnTypedNilCreatePlanner {
		return (*stage4AdapterTypedNilCreatePlanner)(nil)
	}
	return postgresTargetSchemaEvolutionCreatePlanner{}
}

func (target *stage4AdapterEvolutionTarget) ReadTargetSchemaEvolutionCatalog(
	context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	return cloneTargetSchemaEvolutionCatalog(target.catalog), nil
}

func (target *stage4AdapterEvolutionTarget) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	target.evolutionPreflightCalls++
	*target.events = append(
		*target.events,
		fmt.Sprintf(
			"evolution_preflight:%d",
			target.evolutionPreflightCalls,
		),
	)
	if target.evolutionPreflightCalls > 1 &&
		target.reverifyFailuresRemaining > 0 {
		target.reverifyFailuresRemaining--
		return TargetSchemaEvolutionPlan{}, target.reverifyErr
	}
	plan, err := PreflightTargetSchemaEvolution(ctx, request, target)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if target.evolutionPreflightCalls > 1 &&
		target.substituteReverifyDigest {
		plan.digest = strings.Repeat("f", len(plan.digest))
	}
	return plan, nil
}

func (target *stage4AdapterEvolutionTarget) ApplyTargetSchemaEvolutionPlan(
	_ context.Context,
	plan TargetSchemaEvolutionPlan,
) error {
	target.applyCalls++
	*target.events = append(*target.events, "evolution_apply")
	if target.applyErr != nil {
		return target.applyErr
	}
	if !plan.valid() || plan.Complete() {
		return fmt.Errorf("fixture received a non-applicable evolution plan")
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(
		plan.states[len(plan.states)-1],
		plan.reservations,
	)
	if err != nil {
		return err
	}
	target.catalog = catalog
	return nil
}

type stage4AdapterEvolutionObserver struct {
	stage4AdapterObserver
	doubleMutation bool
}

func (observer stage4AdapterEvolutionObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := mutation(); err != nil {
		return err
	}
	if observer.doubleMutation {
		return mutation()
	}
	return nil
}

type stage4AdapterEvolutionBackend struct {
	Stage4StateBackend
	events        *[]string
	failSave      bool
	failTableWork bool
}

func (backend *stage4AdapterEvolutionBackend) SaveSchemaSnapshot(
	snapshot state.SchemaSnapshot,
) error {
	*backend.events = append(*backend.events, "snapshot_save")
	if backend.failSave {
		return errors.New("injected evolution snapshot save failure")
	}
	return backend.Stage4StateBackend.SaveSchemaSnapshot(snapshot)
}

func (backend *stage4AdapterEvolutionBackend) EnsureWorkPlan(
	task state.WorkTask,
	ranges []state.RangeState,
) (bool, error) {
	if task.Key.Type == stage4AdapterNetworkTaskType {
		*backend.events = append(*backend.events, "table_work_ensure")
		if backend.failTableWork {
			return false, errors.New(
				"injected evolution table work checkpoint failure",
			)
		}
	}
	return backend.Stage4StateBackend.EnsureWorkPlan(task, ranges)
}

type stage4AdapterEvolutionFixture struct {
	cfg      config.Config
	source   *stage4AdapterEvolutionSource
	target   *stage4AdapterEvolutionTarget
	observer stage4AdapterEvolutionObserver
	raw      state.YAMLStore
	backend  *stage4AdapterEvolutionBackend
	runID    string
	events   *[]string
}

func newStage4AdapterEvolutionFixture(
	t *testing.T,
) stage4AdapterEvolutionFixture {
	return newStage4AdapterEvolutionFixtureWithTargetPrior(t, nil)
}

func newStage4AdapterEvolutionFixtureWithTargetPrior(
	t *testing.T,
	customize func(*schema.Table),
) stage4AdapterEvolutionFixture {
	t.Helper()
	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	previous.Columns[1].Nullable = false
	current := cloneStage4RichTable(previous)
	current.Columns[1].Nullable = true
	targetPrevious := cloneStage4RichTable(previous)
	targetPrevious.Schema = "tenant"
	if customize != nil {
		customize(&targetPrevious)
	}

	raw := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	stage4AdapterInstallEvolutionBaseline(
		t,
		raw,
		"stage4-evolution-baseline",
		started,
		previous,
		targetPrevious,
	)
	runID := "stage4-evolution-current"
	initializeStage4AdapterEvolutionRun(
		t,
		raw,
		runID,
		started.Add(90*time.Second),
	)
	backend := &stage4AdapterEvolutionBackend{
		Stage4StateBackend: raw,
		events:             &events,
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(
		[]schema.Table{targetPrevious},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := &stage4AdapterEvolutionSource{
		recordingAdapterSource: &recordingAdapterSource{
			events: &events,
			table:  current,
		},
	}
	target := &stage4AdapterEvolutionTarget{
		events:  &events,
		catalog: catalog,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	observer := stage4AdapterEvolutionObserver{
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
	return stage4AdapterEvolutionFixture{
		cfg:      cfg,
		source:   source,
		target:   target,
		observer: observer,
		raw:      raw,
		backend:  backend,
		runID:    runID,
		events:   &events,
	}
}

func TestStage4AdapterDropRecreateDoesNotEnterInPlaceEvolution(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &stage4AdapterEvolutionSource{
		recordingAdapterSource: &recordingAdapterSource{
			events: &events,
			table:  stage4AdapterTestTable(),
		},
	}
	emptyCatalog, err := NewTargetSchemaEvolutionCatalog(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	target := &stage4AdapterEvolutionTarget{
		events:  &events,
		catalog: emptyCatalog,
	}
	raw := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-drop-recreate-no-in-place-evolution"
	initializeStage4AdapterEvolutionRun(
		t,
		raw,
		runID,
		time.Now().Add(-time.Minute),
	)
	backend := &stage4AdapterEvolutionBackend{
		Stage4StateBackend: raw,
		events:             &events,
	}
	observer := stage4AdapterEvolutionObserver{
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
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "drop_recreate"
	result, err := migrateWithStage4Adapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"drop_recreate",
		observer.run,
	)
	if err != nil {
		t.Fatalf("drop/recreate composed route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("drop/recreate result = %#v", result)
	}
	if target.evolutionPreflightCalls != 0 || target.applyCalls != 0 {
		t.Fatalf(
			"drop/recreate entered in-place evolution: preflight=%d apply=%d events=%v",
			target.evolutionPreflightCalls,
			target.applyCalls,
			events,
		)
	}
	tasks, _, listErr := raw.ListWork(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	for _, task := range tasks {
		if task.Key == stage4TargetShapeTask {
			t.Fatalf(
				"drop/recreate created target-shape sentinel: %#v",
				task,
			)
		}
	}
}

func TestStage4AdapterEvolutionPostDDLPreflightUsesFullAuthenticatedTargetShape(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression(`owner_id >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newStage4AdapterEvolutionFixtureWithTargetPrior(
		t,
		func(table *schema.Table) {
			table.Columns = append(table.Columns, schema.Column{
				Name:     "owner_id",
				Type:     "bigint",
				Nullable: true,
			})
			table.Indexes = append(table.Indexes, schema.Index{
				Name: "items_owner_id_idx",
				Columns: []schema.IndexColumn{{
					Name: "owner_id",
				}},
			})
			table.Checks = append(table.Checks, schema.CheckConstraint{
				Name:       "items_owner_id_check",
				Expression: check,
			})
			table.ForeignKeys = append(
				table.ForeignKeys,
				schema.ForeignKey{
					Name:              "items_owner_id_fk",
					Columns:           []string{"owner_id"},
					ReferencedSchema:  "tenant",
					ReferencedTable:   "items",
					ReferencedColumns: []string{"id"},
					OnUpdate:          "NO ACTION",
					OnDelete:          "NO ACTION",
				},
			)
		},
	)
	result, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err != nil {
		t.Fatalf("migrate retained target shape: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("retained target result = %#v", result)
	}
	if len(fixture.target.preflightTables) != 2 {
		t.Fatalf(
			"target preflight calls = %#v",
			fixture.target.preflightTables,
		)
	}
	for index, tables := range fixture.target.preflightTables {
		assertStage4AdapterRetainedTargetShape(t, tables, index)
	}
	if len(fixture.target.isolationTables) != 2 {
		t.Fatalf(
			"replay isolation calls = %#v",
			fixture.target.isolationTables,
		)
	}
	for index, tables := range fixture.target.isolationTables {
		assertStage4AdapterRetainedTargetShape(t, tables, index)
	}
	if len(fixture.target.writeTables) == 0 {
		t.Fatal("retained target route wrote no network page")
	}
	for index, table := range fixture.target.writeTables {
		assertStage4AdapterRetainedTargetShape(
			t,
			[]schema.Table{table},
			index,
		)
	}
}

func assertStage4AdapterRetainedTargetShape(
	t *testing.T,
	tables []schema.Table,
	call int,
) {
	t.Helper()
	if len(tables) != 1 {
		t.Fatalf(
			"retained target call %d tables = %#v",
			call,
			tables,
		)
	}
	table := tables[0]
	if len(table.Columns) != 3 ||
		table.Columns[2].Name != "owner_id" ||
		len(table.Indexes) != 1 ||
		table.Indexes[0].Name != "items_owner_id_idx" ||
		len(table.Checks) != 1 ||
		table.Checks[0].Name != "items_owner_id_check" ||
		len(table.ForeignKeys) != 1 ||
		table.ForeignKeys[0].Name != "items_owner_id_fk" {
		t.Fatalf(
			"retained target call %d lost authenticated objects: %#v",
			call,
			table,
		)
	}
}

func TestStage4AdapterEvolutionStagesAfterPreflightAndAppliesOnceBeforeData(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	result, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err != nil {
		t.Fatalf("migrateWithStage4Adapters: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if fixture.target.applyCalls != 1 ||
		fixture.target.evolutionPreflightCalls != 2 {
		t.Fatalf(
			"evolution apply=%d preflight=%d events=%v",
			fixture.target.applyCalls,
			fixture.target.evolutionPreflightCalls,
			*fixture.events,
		)
	}
	if len(fixture.target.preflightTables) != 2 ||
		len(fixture.target.preflightTables[0]) != 1 ||
		len(fixture.target.preflightTables[1]) != 1 ||
		fixture.target.preflightTables[0][0].Columns[1].Nullable ||
		!fixture.target.preflightTables[1][0].Columns[1].Nullable {
		t.Fatalf(
			"target preflight did not use prior then desired shape: %#v",
			fixture.target.preflightTables,
		)
	}
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"evolution_preflight:1",
		"target_preflight",
	)
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"target_preflight",
		"before_tables:items",
	)
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"before_tables:items",
		"snapshot_save",
	)
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"snapshot_save",
		"evolution_apply",
	)
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"evolution_apply",
		"evolution_preflight:2",
	)
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"evolution_preflight:2",
		"target_prepare",
	)
}

func TestStage4AdapterEvolutionFailuresNeverReachDataCallbacks(
	t *testing.T,
) {
	tests := []struct {
		name       string
		configure  func(*stage4AdapterEvolutionFixture)
		want       string
		wantApply  int
		wantStaged bool
	}{
		{
			name: "snapshot stage",
			configure: func(fixture *stage4AdapterEvolutionFixture) {
				fixture.backend.failSave = true
			},
			want:      "snapshot save failure",
			wantApply: 0,
		},
		{
			name: "apply",
			configure: func(fixture *stage4AdapterEvolutionFixture) {
				fixture.target.applyErr =
					errors.New("injected evolution apply failure")
			},
			want:       "injected evolution apply failure",
			wantApply:  1,
			wantStaged: true,
		},
		{
			name: "reverify",
			configure: func(fixture *stage4AdapterEvolutionFixture) {
				fixture.target.reverifyFailuresRemaining = 1
				fixture.target.reverifyErr =
					errors.New("injected evolution reverify failure")
			},
			want:       "injected evolution reverify failure",
			wantApply:  1,
			wantStaged: true,
		},
		{
			name: "post evolution target preflight",
			configure: func(fixture *stage4AdapterEvolutionFixture) {
				fixture.target.postEvolutionPreflightErr =
					errors.New("injected post-evolution preflight failure")
			},
			want:       "injected post-evolution preflight failure",
			wantApply:  1,
			wantStaged: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStage4AdapterEvolutionFixture(t)
			test.configure(&fixture)
			_, err := migrateWithStage4Adapters(
				context.Background(),
				fixture.cfg,
				fixture.observer,
				fixture.source,
				fixture.target,
				"upsert",
				fixture.observer.run,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if fixture.target.applyCalls != test.wantApply {
				t.Fatalf(
					"apply calls = %d, want %d",
					fixture.target.applyCalls,
					test.wantApply,
				)
			}
			for _, task := range []state.TaskKey{
				stage4SchemaGateTask,
				stage4TargetShapeTask,
			} {
				snapshot, found, loadErr :=
					fixture.raw.LoadSchemaSnapshot(
						fixture.runID,
						task,
					)
				if loadErr != nil ||
					found != test.wantStaged ||
					test.wantStaged && snapshot.Digest == "" {
					t.Fatalf(
						"staged evidence for %#v found=%v snapshot=%#v err=%v want=%v",
						task,
						found,
						snapshot,
						loadErr,
						test.wantStaged,
					)
				}
			}
			for _, forbidden := range []string{
				"before:items",
				"target_prepare",
				"target_write",
				"target_finalize",
			} {
				if stage4AdapterEventIndex(
					*fixture.events,
					forbidden,
				) >= 0 {
					t.Fatalf(
						"failure crossed %s: %v",
						forbidden,
						*fixture.events,
					)
				}
			}
		})
	}
}

func TestStage4AdapterEvolutionTaskCheckpointFailurePrecedesSnapshotAndDDL(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.backend.failTableWork = true
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "table work checkpoint failure") {
		t.Fatalf("task checkpoint error = %v", err)
	}
	if stage4AdapterEventIndex(*fixture.events, "table_work_ensure") < 0 ||
		stage4AdapterEventIndex(*fixture.events, "snapshot_save") >= 0 ||
		fixture.target.applyCalls != 0 {
		t.Fatalf(
			"task checkpoint ordering events=%v apply=%d",
			*fixture.events,
			fixture.target.applyCalls,
		)
	}
}

func TestStage4AdapterEvolutionSnapshotFailureLeavesTasksButNoDDL(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.backend.failSave = true
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil || !strings.Contains(err.Error(), "snapshot save failure") {
		t.Fatalf("snapshot failure error = %v", err)
	}
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"table_work_ensure",
		"snapshot_save",
	)
	if fixture.target.applyCalls != 0 {
		t.Fatalf("snapshot failure applied DDL %d times", fixture.target.applyCalls)
	}
	tasks, _, listErr := fixture.raw.ListWork(fixture.runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	var tableTask bool
	for _, task := range tasks {
		if task.Key.Type == stage4AdapterNetworkTaskType {
			tableTask = true
			if task.Status != "running" {
				t.Fatalf("table task status = %q", task.Status)
			}
		}
	}
	if !tableTask {
		t.Fatalf("snapshot failure left no durable table task: %#v", tasks)
	}
}

func TestStage4AdapterEvolutionRerunAfterApplySkipsDuplicateMutation(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.target.reverifyFailuresRemaining = 1
	fixture.target.reverifyErr =
		errors.New("injected crash after evolution apply")
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil || !strings.Contains(err.Error(), "injected crash") {
		t.Fatalf("first run error = %v", err)
	}
	if fixture.target.applyCalls != 1 {
		t.Fatalf("first run apply calls = %d", fixture.target.applyCalls)
	}
	firstEvents := len(*fixture.events)
	result, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("rerun result = %#v", result)
	}
	if fixture.target.applyCalls != 1 {
		t.Fatalf(
			"rerun duplicated evolution apply: calls=%d events=%v",
			fixture.target.applyCalls,
			(*fixture.events)[firstEvents:],
		)
	}
}

func TestStage4AdapterEvolutionRejectsReverifiedDigestSubstitution(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.target.substituteReverifyDigest = true
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil || !strings.Contains(err.Error(), "plan digest changed") {
		t.Fatalf("digest substitution error = %v", err)
	}
	if fixture.target.applyCalls != 1 {
		t.Fatalf("apply calls = %d", fixture.target.applyCalls)
	}
	if stage4AdapterEventIndex(*fixture.events, "before:items") >= 0 ||
		stage4AdapterEventIndex(*fixture.events, "target_prepare") >= 0 ||
		stage4AdapterEventIndex(*fixture.events, "target_write") >= 0 {
		t.Fatalf(
			"digest substitution crossed data boundary: %v",
			*fixture.events,
		)
	}
}

func TestStage4AdapterEvolutionRejectsTypedNilCreatePlannerBeforeStaging(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.target.returnTypedNilCreatePlanner = true
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil || !strings.Contains(err.Error(), "nil create planner") {
		t.Fatalf("typed-nil planner error = %v", err)
	}
	if stage4AdapterEventIndex(*fixture.events, "snapshot_save") >= 0 ||
		fixture.target.applyCalls != 0 {
		t.Fatalf(
			"typed-nil planner crossed staging: events=%v apply=%d",
			*fixture.events,
			fixture.target.applyCalls,
		)
	}
}

func TestStage4AdapterEvolutionProtectorCannotInvokeApplyTwice(
	t *testing.T,
) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.observer.doubleMutation = true
	_, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		fixture.target,
		"upsert",
		fixture.observer.run,
	)
	if err == nil || !strings.Contains(err.Error(), "multiple times") {
		t.Fatalf("double protector error = %v", err)
	}
	if fixture.target.applyCalls != 1 {
		t.Fatalf(
			"double protector invoked live evolution %d times",
			fixture.target.applyCalls,
		)
	}
	if stage4AdapterEventIndex(*fixture.events, "before:items") >= 0 {
		t.Fatalf(
			"double protector crossed data callbacks: %v",
			*fixture.events,
		)
	}
}

func TestStage4AdapterTargetShapeAuthorityCarriesUnchangedBaselineIntoLaterEvolution(
	t *testing.T,
) {
	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	previous.Columns[1].Nullable = false
	raw := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-3 * time.Minute).UTC()
	firstRunID := "stage4-authority-unchanged"
	initializeStage4AdapterEvolutionRun(
		t,
		raw,
		firstRunID,
		started,
	)
	backend := &stage4AdapterEvolutionBackend{
		Stage4StateBackend: raw,
		events:             &events,
	}
	targetPrevious := cloneStage4RichTable(previous)
	targetPrevious.Schema = "tenant"
	catalog, err := NewTargetSchemaEvolutionCatalog(
		[]schema.Table{targetPrevious},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := &stage4AdapterEvolutionSource{
		recordingAdapterSource: &recordingAdapterSource{
			events: &events,
			table:  previous,
		},
	}
	target := &stage4AdapterEvolutionTarget{
		events:  &events,
		catalog: catalog,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractReport,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	firstObserver := stage4AdapterEvolutionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				backend,
				firstRunID,
				false,
			),
		},
	}
	first, err := migrateWithStage4Adapters(
		context.Background(),
		cfg,
		firstObserver,
		source,
		target,
		"upsert",
		firstObserver.run,
	)
	if err != nil {
		t.Fatalf("unchanged baseline: %v", err)
	}
	if !first.Validated || target.applyCalls != 0 {
		t.Fatalf(
			"unchanged baseline result=%#v apply=%d",
			first,
			target.applyCalls,
		)
	}
	targetSnapshot, found, err := raw.LoadSchemaSnapshot(
		firstRunID,
		stage4TargetShapeTask,
	)
	if err != nil || !found || targetSnapshot.Digest == "" {
		t.Fatalf(
			"unchanged baseline target authority found=%v snapshot=%#v err=%v",
			found,
			targetSnapshot,
			err,
		)
	}
	if err := raw.Append(state.Run{
		ID:             firstRunID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/app",
		TargetIdentity: "postgres:target.example:5432/app",
		Outcome:        state.Success,
		Resumable:      true,
		Reason:         "complete",
		StartedAt:      started,
		EndedAt:        started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	current := cloneStage4RichTable(previous)
	current.Columns[1].Nullable = true
	source.table = current
	secondRunID := "stage4-authority-later-evolve"
	initializeStage4AdapterEvolutionRun(
		t,
		raw,
		secondRunID,
		started.Add(2*time.Minute),
	)
	secondObserver := stage4AdapterEvolutionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				backend,
				secondRunID,
				false,
			),
		},
	}
	second, err := migrateWithStage4Adapters(
		context.Background(),
		cfg,
		secondObserver,
		source,
		target,
		"upsert",
		secondObserver.run,
	)
	if err != nil {
		t.Fatalf("later evolve: %v", err)
	}
	if !second.Validated || target.applyCalls != 1 {
		t.Fatalf(
			"later evolve result=%#v apply=%d",
			second,
			target.applyCalls,
		)
	}
}

func stage4AdapterInstallEvolutionBaseline(
	t *testing.T,
	backend state.YAMLStore,
	runID string,
	started time.Time,
	table schema.Table,
	targetTable schema.Table,
) {
	t.Helper()
	initializeStage4AdapterEvolutionRun(
		t,
		backend,
		runID,
		started,
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	options := Stage4SchemaGateOptions{
		SourceEngine:   "postgres",
		TargetEngine:   "postgres",
		TargetMode:     "upsert",
		ConfigIdentity: "stage4-evolution-baseline",
		CapturedAt:     started,
	}
	gate, err := PrepareStage4SchemaGate(
		run,
		[]schema.Table{table},
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	targetCatalog, err := NewTargetSchemaEvolutionCatalog(
		[]schema.Table{targetTable},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewStage4TargetShapeSeed(targetCatalog)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := PrepareStage4TargetShapeAuthority(
		run,
		gate,
		options,
		seed,
	)
	if err != nil {
		t.Fatal(err)
	}
	baselineEvents := make([]string, 0)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		&stage4AdapterEvolutionTarget{events: &baselineEvents},
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	targetPending, err := BindStage4TargetShapeProjection(
		authority,
		projection,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(gate.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(targetPending); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		run,
		authority.Task(),
		stage4TargetShapeRangeID,
		authority.TopologyHash(),
	); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		run,
		gate.Task,
		stage4SchemaGateRangeID,
		gate.TopologyHash,
	); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/app",
		TargetIdentity: "postgres:target.example:5432/app",
		Outcome:        state.Success,
		Resumable:      true,
		Reason:         "complete",
		StartedAt:      started,
		EndedAt:        started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
}

func initializeStage4AdapterEvolutionRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	started time.Time,
) {
	t.Helper()
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/app",
		TargetIdentity: "postgres:target.example:5432/app",
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      started,
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
}
