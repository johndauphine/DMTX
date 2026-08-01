package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestSQLiteTargetEvolutionCatalogPreservesTargetOnlyObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE managed (id INTEGER NOT NULL, PRIMARY KEY (id));
		CREATE TABLE target_only (id INTEGER NOT NULL, value TEXT, PRIMARY KEY (id));
		CREATE INDEX target_only_value_idx ON target_only(value);
		CREATE VIEW target_only_view AS SELECT id, value FROM target_only;
		CREATE TRIGGER target_only_audit AFTER INSERT ON target_only
		BEGIN SELECT NEW.id; END;`); err != nil {
		t.Fatal(err)
	}
	catalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if findTargetSchemaEvolutionTable(catalog.Tables(), targetSchemaEvolutionTableKey{table: "managed"}) < 0 ||
		findTargetSchemaEvolutionTable(catalog.Tables(), targetSchemaEvolutionTableKey{table: "target_only"}) < 0 {
		t.Fatalf("catalog omitted target-only table: %#v", catalog.Tables())
	}
	if !sqliteTargetEvolutionReservationPresent(catalog, "relation", "target_only_view") ||
		!sqliteTargetEvolutionReservationPresent(catalog, "trigger", "target_only_audit") ||
		!sqliteTargetEvolutionDefinitionPresent(catalog, "view_definition", "target_only_view@") ||
		!sqliteTargetEvolutionDefinitionPresent(catalog, "trigger_definition", "target_only_audit@") {
		t.Fatalf("catalog omitted target-only object reservations: %#v", catalog.Reservations())
	}
	before := catalog
	if _, err := adapter.database.ExecContext(ctx, `DROP TRIGGER target_only_audit; CREATE TRIGGER target_only_audit AFTER INSERT ON target_only BEGIN SELECT NEW.value; END;`); err != nil {
		t.Fatal(err)
	}
	after, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := matchTargetSchemaEvolutionState(
		[][]schema.Table{before.Tables()}, before.Reservations(), after,
	); err == nil || !strings.Contains(err.Error(), "reservations changed") {
		t.Fatalf("same-name trigger rewrite was accepted as catalog state: %v", err)
	}
}

func TestSQLiteTargetEvolutionCreatePlannerSealsIndexesAndRejectsReservations(t *testing.T) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression(`value <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	table := sqliteTargetEvolutionTestTable("events")
	table.Columns = append(table.Columns, schema.Column{
		Name: "value", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "text"},
	})
	table.Indexes = []schema.Index{{
		Name: "events_value_idx", Columns: []schema.IndexColumn{{Name: "value"}},
	}}
	table.Checks = []schema.CheckConstraint{{Expression: check}}
	catalog, err := NewTargetSchemaEvolutionCatalog(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := (sqliteTargetSchemaEvolutionCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(schema.SQLite, []schema.Table{table}, []schema.Table{table}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.steps) != 2 ||
		!strings.HasPrefix(bundle.steps[0].statement, `CREATE TABLE "events"`) ||
		!strings.HasPrefix(bundle.steps[1].statement, `CREATE INDEX "events_value_idx"`) {
		t.Fatalf("SQLite complete create steps = %#v", bundle.steps)
	}
	reserved, err := NewTargetSchemaEvolutionCatalog(nil, []TargetSchemaEvolutionNameReservation{{
		Scope: "relation", Namespace: sqliteTargetEvolutionNamespace, Name: "events",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (sqliteTargetSchemaEvolutionCreatePlanner{}).PlanCompleteTargetSchemaCreates(
		schema.SQLite, []schema.Table{table}, []schema.Table{table}, reserved,
	); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("reserved SQLite relation was admitted: %v", err)
	}
}

func TestSQLiteTargetEvolutionCreatePlannerRejectsCaseInsensitiveObjectCollisions(t *testing.T) {
	t.Parallel()
	existing := sqliteTargetEvolutionTestTable("retained")
	existing.Indexes = []schema.Index{{
		Name: "TakenIndex", Columns: []schema.IndexColumn{{Name: "id"}},
	}}
	catalog, err := NewTargetSchemaEvolutionCatalog([]schema.Table{existing}, []TargetSchemaEvolutionNameReservation{
		{Scope: "relation", Namespace: sqliteTargetEvolutionNamespace, Name: "TakenView"},
		{Scope: "trigger", Namespace: sqliteTargetEvolutionNamespace, Name: "TakenTrigger"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		table schema.Table
	}{
		{name: "table_view", table: sqliteTargetEvolutionTestTable("takenview")},
		{name: "table_trigger", table: sqliteTargetEvolutionTestTable("TAKENTRIGGER")},
		{name: "index_table", table: func() schema.Table {
			table := sqliteTargetEvolutionTestTable("events")
			table.Indexes = []schema.Index{{
				Name: "takenindex", Columns: []schema.IndexColumn{{Name: "id"}},
			}}
			return table
		}()},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := (sqliteTargetSchemaEvolutionCreatePlanner{}).
				PlanCompleteTargetSchemaCreates(
					schema.SQLite, []schema.Table{test.table}, []schema.Table{test.table}, catalog,
				)
			if err == nil || !strings.Contains(err.Error(), "collides") {
				t.Fatalf("case-insensitive SQLite collision = %v", err)
			}
		})
	}
}

func TestSQLiteTargetEvolutionRejectsBlankPersistentObjectAuthority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"view", "trigger"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			_, err := appendSQLiteTargetEvolutionObjectReservations(
				nil, "relation", kind, "target_only", sql.NullString{},
			)
			if err == nil || !strings.Contains(err.Error(), "no exact sqlite_schema SQL authority") {
				t.Fatalf("blank SQLite %s authority = %v", kind, err)
			}
		})
	}
}

func TestSQLiteTargetEvolutionTransactionalApplyAndPostCommitRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	var journalMode string
	if err := adapter.database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %q, want WAL", journalMode)
	}
	before := sqliteTargetEvolutionTestTable("events")
	createBefore, err := schema.CreateTable(schema.SQLite, before)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.database.ExecContext(ctx, createBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.database.ExecContext(ctx, `
		INSERT INTO events(id) VALUES (1);
		CREATE TABLE target_only (id INTEGER NOT NULL, value TEXT, PRIMARY KEY (id));
		INSERT INTO target_only(id, value) VALUES (7, 'preserve');
		CREATE INDEX target_only_value_idx ON target_only(value);`); err != nil {
		t.Fatal(err)
	}
	after := cloneStage4RichTable(before)
	after.Columns = append(after.Columns, schema.Column{
		Name: "note", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "text"},
	})
	addStatement := `ALTER TABLE "events" ADD COLUMN "note" TEXT NULL;`
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeTables := baseline.Tables()
	afterTables := replaceSQLiteTargetEvolutionTable(t, beforeTables, after)
	plan := TargetSchemaEvolutionPlan{
		target:          schema.SQLite,
		operations:      []TargetSchemaEvolutionOperation{{action: SchemaContractAddColumn, statements: []string{addStatement}}},
		states:          [][]schema.Table{beforeTables, afterTables},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-test-authority",
		digest:          "sqlite-test-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err != nil {
		t.Fatalf("apply SQLite evolution: %v", err)
	}
	var rows int
	if err := adapter.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE id = 1 AND note IS NULL").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("retained managed row after add = %d, %v", rows, err)
	}
	var targetOnly string
	if err := adapter.database.QueryRowContext(ctx, "SELECT value FROM target_only WHERE id = 7").Scan(&targetOnly); err != nil || targetOnly != "preserve" {
		t.Fatalf("target-only row after add = %q, %v", targetOnly, err)
	}
	// Model a process loss after durable COMMIT and before aggregate state
	// acknowledgement. A fresh preflight would observe this exact final prefix;
	// the resumed immutable plan must perform no DDL and verify it independently.
	resumed := plan
	resumed.observedPrefix = 1
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, resumed); err != nil {
		t.Fatalf("resume SQLite post-commit evolution: %v", err)
	}
	finalCatalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := classifySQLiteTargetEvolutionCommitCatalog(
		plan,
		finalCatalog,
		nil,
		errors.New("injected SQLite commit acknowledgement loss"),
	); err != nil {
		t.Fatalf("exact final SQLite catalog did not recover commit ambiguity: %v", err)
	}
	if _, err := adapter.database.ExecContext(ctx, "INSERT INTO events(id, note) VALUES (2, 'ok')"); err != nil {
		t.Fatalf("write after SQLite evolution: %v", err)
	}
}

func TestStage4AdapterSQLiteTargetEvolutionComposedWAL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "target.sqlite")
	opened, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: path})
	if err != nil {
		t.Fatal(err)
	}
	target := opened.(*sqliteTargetAdapter)
	t.Cleanup(func() { _ = target.database.Close() })
	var journalMode string
	if err := target.database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %q, want WAL", journalMode)
	}
	if _, err := target.database.ExecContext(ctx, `
		CREATE TABLE target_only (id INTEGER NOT NULL, value TEXT, PRIMARY KEY (id));
		CREATE INDEX target_only_value_idx ON target_only(value);
		INSERT INTO target_only(id, value) VALUES (7, 'preserve');
		CREATE VIEW target_only_view AS SELECT id, value FROM target_only;`); err != nil {
		t.Fatal(err)
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Target = config.Endpoint{Type: "sqlite", Database: path}
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{"items"}
	cfg.Migration.Partitions = 1
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	installSQLiteTargetEvolutionBaseline(
		t, ctx, backend, target, cfg, "sqlite-evolution-baseline", started,
	)
	runID := "sqlite-evolution-current"
	initializeStage4LifecycleRun(t, backend, runID, started.Add(time.Minute))
	events := make([]string, 0)
	source := &recordingAdapterSource{events: &events, table: stage4AdapterTestTable()}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, false),
	}
	result, err := migrateWithStage4Adapters(
		ctx, cfg, observer, source, target, "upsert", observer.run,
	)
	if err != nil {
		t.Fatalf("run composed SQLite schema evolution: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("composed SQLite schema evolution result = %#v", result)
	}
	var itemRows int
	if err := target.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&itemRows); err != nil || itemRows != 2 {
		t.Fatalf("evolved SQLite items rows = %d, %v", itemRows, err)
	}
	var targetOnly string
	if err := target.database.QueryRowContext(ctx, "SELECT value FROM target_only WHERE id = 7").Scan(&targetOnly); err != nil || targetOnly != "preserve" {
		t.Fatalf("composed SQLite target-only row = %q, %v", targetOnly, err)
	}
	var viewCount int
	if err := target.database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'view' AND name = 'target_only_view'`).Scan(&viewCount); err != nil || viewCount != 1 {
		t.Fatalf("composed SQLite target-only view = %d, %v", viewCount, err)
	}
}

func TestStage4AdapterSQLiteTargetEvolutionRelaxResumeWAL(t *testing.T) {
	ctx := context.Background()
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "target.sqlite")
			opened, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: path})
			if err != nil {
				t.Fatal(err)
			}
			adapter := opened.(*sqliteTargetAdapter)
			t.Cleanup(func() { _ = adapter.database.Close() })
			var journalMode string
			if err := adapter.database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
				t.Fatalf("enable WAL: %v", err)
			}
			if !strings.EqualFold(journalMode, "wal") {
				t.Fatalf("journal mode = %q, want WAL", journalMode)
			}

			prior := stage4AdapterTestTable()
			prior.Columns[1].Nullable = false
			current := cloneStage4RichTable(prior)
			current.Columns[1].Nullable = true
			planned, err := adapter.PlanTables("postgres", []schema.Table{prior}, "upsert")
			if err != nil || len(planned) != 1 {
				t.Fatalf("plan prior SQLite target table = %#v, %v", planned, err)
			}
			create, err := schema.CreateTable(schema.SQLite, planned[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.database.ExecContext(ctx, create); err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.database.ExecContext(ctx, `
				ALTER TABLE items ADD COLUMN target_only BLOB;
				INSERT INTO items(id, payload, target_only) VALUES (1, 'old', X'00FF');`); err != nil {
				t.Fatal(err)
			}

			backend := newBackend(t)
			cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
			cfg.Target = config.Endpoint{Type: "sqlite", Database: path}
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.IncludeTables = []string{"items"}
			cfg.Migration.Partitions = 1
			cfg.Migration.SchemaContract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}
			started := time.Now().Add(-2 * time.Minute).UTC()
			installSQLiteTargetEvolutionBaselineWithSource(
				t,
				ctx,
				backend,
				adapter,
				cfg,
				"sqlite-relax-baseline-"+backendName,
				started,
				[]schema.Table{prior},
			)

			runID := "sqlite-relax-current-" + backendName
			initializeStage4LifecycleRun(t, backend, runID, started.Add(time.Minute))
			events := make([]string, 0)
			source := &recordingAdapterSource{events: &events, table: current}
			faultTarget := &sqliteTargetEvolutionReverifyFaultTarget{
				sqliteTargetAdapter: adapter,
				failReverify:        true,
			}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			if _, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				faultTarget,
				"upsert",
				observer.run,
			); err == nil || !strings.Contains(err.Error(), "injected SQLite evolution reverify failure") {
				t.Fatalf("fault after committed SQLite evolution = %v", err)
			}
			catalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
			if err != nil {
				t.Fatal(err)
			}
			itemsIndex := findTargetSchemaEvolutionTable(catalog.Tables(), targetSchemaEvolutionTableKey{table: "items"})
			if itemsIndex < 0 {
				t.Fatal("items missing after committed copy/swap")
			}
			payloadIndex := findTargetSchemaEvolutionColumnIndex(catalog.Tables()[itemsIndex], "payload")
			if payloadIndex < 0 || !catalog.Tables()[itemsIndex].Columns[payloadIndex].Nullable {
				t.Fatalf("committed copy/swap did not relax items.payload: %#v", catalog.Tables()[itemsIndex])
			}
			resumedEvents := make([]string, 0)
			resumedObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &resumedEvents},
				run:                    stage4LifecycleRunContext(t, backend, runID, true),
			}
			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				resumedObserver,
				&recordingAdapterSource{events: &resumedEvents, table: current},
				adapter,
				"upsert",
				resumedObserver.run,
			)
			if err != nil {
				t.Fatalf("resume after committed SQLite evolution: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
				t.Fatalf("resumed SQLite evolution result = %#v", result)
			}
			var payload string
			var targetOnly []byte
			if err := adapter.database.QueryRowContext(
				ctx,
				`SELECT payload, target_only FROM items WHERE id = 1`,
			).Scan(&payload, &targetOnly); err != nil || payload != "first" || string(targetOnly) != string([]byte{0x00, 0xff}) {
				t.Fatalf("retained target-only data after resumed transfer = (%q, %x), %v", payload, targetOnly, err)
			}
			var rows int
			if err := adapter.database.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&rows); err != nil || rows != 2 {
				t.Fatalf("resumed SQLite evolution target rows = %d, %v", rows, err)
			}
		})
	}
}

func TestStage4AdapterExistingEvolutionTargetTablesUsesAuthenticatedAppliedPrefix(t *testing.T) {
	prior := sqliteTargetEvolutionTestTable("items")
	prior.Columns = append(prior.Columns, schema.Column{
		Name: "payload", Type: "text", Nullable: false,
		DeclaredType: &schema.DeclaredType{Base: "text"},
	})
	final := cloneStage4RichTable(prior)
	final.Columns[1].Nullable = true
	evolution := &stage4AdapterTargetSchemaEvolution{
		prior:   []schema.Table{prior},
		current: []schema.Table{final},
		plan: TargetSchemaEvolutionPlan{
			target:          schema.SQLite,
			operations:      []TargetSchemaEvolutionOperation{{}},
			states:          [][]schema.Table{{prior}, {final}},
			observedPrefix:  1,
			authorityDigest: "applied-prefix-authority",
			digest:          "applied-prefix-plan",
		},
	}
	tables, err := stage4AdapterExistingEvolutionTargetTables(
		evolution,
		[]schema.Table{final},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || len(tables[0].Columns) != 2 || !tables[0].Columns[1].Nullable {
		t.Fatalf("existing tables did not use authenticated applied prefix: %#v", tables)
	}
}

type sqliteTargetEvolutionReverifyFaultTarget struct {
	*sqliteTargetAdapter
	failReverify bool
	preflights   int
}

func (target *sqliteTargetEvolutionReverifyFaultTarget) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	target.preflights++
	plan, err := target.sqliteTargetAdapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if target.failReverify && target.preflights == 2 {
		return TargetSchemaEvolutionPlan{}, errors.New("injected SQLite evolution reverify failure")
	}
	return plan, nil
}

func TestSQLiteTargetEvolutionAdmitsRelaxAndWidenForCopySwap(t *testing.T) {
	t.Parallel()
	for _, action := range []SchemaContractAction{
		SchemaContractRelaxNullability,
		SchemaContractWidenType,
	} {
		action := action
		t.Run(string(action), func(t *testing.T) {
			request := TargetSchemaEvolutionRequest{decisions: []boundTargetSchemaEvolutionDecision{{
				contract: SchemaContractDecision{Action: action},
			}}}
			if err := validateSQLiteTargetEvolutionRequest(request); err != nil {
				t.Fatalf("%s copy/swap admission = %v", action, err)
			}
		})
	}
}

func TestSQLiteTargetEvolutionPreflightBuildsProvedCopySwapPlan(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		action SchemaContractAction
		mutate func(*schema.Table, int)
	}{
		{
			name:   "relax_nullability",
			action: SchemaContractRelaxNullability,
			mutate: func(table *schema.Table, valueIndex int) {
				table.Columns[valueIndex].Nullable = true
			},
		},
		{
			name:   "safe_widen_type",
			action: SchemaContractWidenType,
			mutate: func(table *schema.Table, valueIndex int) {
				table.Columns[valueIndex].Type = "text"
				table.Columns[valueIndex].DeclaredType = &schema.DeclaredType{Base: "text"}
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
			if _, err := adapter.database.ExecContext(ctx, `
				CREATE TABLE events (
					id INTEGER NOT NULL PRIMARY KEY,
					value VARCHAR(8) NOT NULL
				);
				INSERT INTO events(id, value) VALUES (1, 'retained');`); err != nil {
				t.Fatal(err)
			}
			baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
			if err != nil {
				t.Fatal(err)
			}
			prior := baseline.Tables()
			eventsIndex := findTargetSchemaEvolutionTable(
				prior,
				targetSchemaEvolutionTableKey{table: "events"},
			)
			if eventsIndex < 0 {
				t.Fatal("events missing from SQLite evolution catalog")
			}
			currentEvents := cloneStage4RichTable(prior[eventsIndex])
			valueIndex := findTargetSchemaEvolutionColumnIndex(currentEvents, "value")
			if valueIndex < 0 {
				t.Fatal("events.value missing from SQLite evolution catalog")
			}
			test.mutate(&currentEvents, valueIndex)
			current := replaceSQLiteTargetEvolutionTable(t, prior, currentEvents)
			priorSnapshot, err := schema.NewSchemaSnapshot(prior)
			if err != nil {
				t.Fatal(err)
			}
			currentSnapshot, err := schema.NewSchemaSnapshot(current)
			if err != nil {
				t.Fatal(err)
			}
			contract, err := BuildSchemaContractPlan(
				priorSnapshot,
				currentSnapshot,
				SchemaContractOptions{
					Contract: &config.SchemaContract{
						Tables:   config.SchemaContractEvolve,
						Columns:  config.SchemaContractEvolve,
						DataType: config.SchemaContractEvolve,
					},
					TargetMode: "upsert",
				},
			)
			if err != nil {
				t.Fatalf("build %s schema contract: %v", test.action, err)
			}
			if len(contract.Decisions) != 1 || contract.Decisions[0].Action != test.action {
				t.Fatalf("%s contract decisions = %#v", test.action, contract.Decisions)
			}
			projection := targetSchemaEvolutionFixtureProjection(
				t,
				prior,
				current,
				contract.Decisions,
			)
			projection.sourceEngine = "sqlite"
			projection.targetEngine = "sqlite"
			projection.targetAuthorityReservations = baseline.Reservations()
			projection.targetAuthorityCatalogDigest, err = stage4TargetShapeCatalogDigest(
				priorSnapshot,
				baseline.Reservations(),
			)
			if err != nil {
				t.Fatal(err)
			}
			request, err := NewTargetSchemaEvolutionRequest(
				schema.SQLite,
				projection,
				adapter.TargetSchemaEvolutionCreatePlanner(),
			)
			if err != nil {
				t.Fatalf("authorize %s SQLite evolution: %v", test.action, err)
			}
			plan, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
			if err != nil {
				t.Fatalf("preflight %s SQLite evolution: %v", test.action, err)
			}
			pending := plan.PendingOperations()
			if len(pending) != 1 || pending[0].Action() != test.action ||
				!reflect.DeepEqual(pending[0].Statements(), []string{sqliteTargetEvolutionCopySwapStatement}) {
				t.Fatalf("%s SQLite copy/swap plan = %#v", test.action, pending)
			}
		})
	}
}

func TestSQLiteTargetEvolutionCopySwapPreservesRetainedRowsAndAuthority(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	var journalMode string
	if err := adapter.database.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %q, want WAL", journalMode)
	}
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE lookup (
			id INTEGER NOT NULL PRIMARY KEY
		);
		CREATE TABLE parent (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			value VARCHAR(8) NOT NULL,
			lookup_id INTEGER NOT NULL REFERENCES lookup(id),
			target_only BLOB,
			CHECK (value <> '')
		);
		CREATE INDEX parent_value_idx ON parent(value);
		CREATE VIEW parent_values AS SELECT id, value, target_only FROM parent;
		INSERT INTO lookup(id) VALUES (1);
		INSERT INTO parent(id, value, lookup_id, target_only) VALUES (40, 'retained', 1, X'00FF');`); err != nil {
		t.Fatal(err)
	}
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeTables := baseline.Tables()
	parentIndex := findTargetSchemaEvolutionTable(
		beforeTables,
		targetSchemaEvolutionTableKey{table: "parent"},
	)
	if parentIndex < 0 {
		t.Fatal("parent missing from catalog")
	}
	afterRelax := cloneStage4RichTable(beforeTables[parentIndex])
	valueIndex := findTargetSchemaEvolutionColumnIndex(afterRelax, "value")
	if valueIndex < 0 {
		t.Fatal("parent.value missing from catalog")
	}
	afterRelax.Columns[valueIndex].Nullable = true
	relaxedTables := replaceSQLiteTargetEvolutionTable(t, beforeTables, afterRelax)
	afterWiden := cloneStage4RichTable(afterRelax)
	afterWiden.Columns[valueIndex].Type = "text"
	afterWiden.Columns[valueIndex].DeclaredType = &schema.DeclaredType{Base: "text"}
	widenedTables := replaceSQLiteTargetEvolutionTable(t, relaxedTables, afterWiden)
	plan := TargetSchemaEvolutionPlan{
		target: schema.SQLite,
		operations: []TargetSchemaEvolutionOperation{
			{
				action: SchemaContractRelaxNullability,
				objects: []schema.SchemaDriftObject{{
					Kind: schema.SchemaDriftObjectNullability, Table: "parent", Column: "value",
				}},
				statements: []string{sqliteTargetEvolutionCopySwapStatement},
			},
			{
				action: SchemaContractWidenType,
				objects: []schema.SchemaDriftObject{{
					Kind: schema.SchemaDriftObjectDataType, Table: "parent", Column: "value",
				}},
				statements: []string{sqliteTargetEvolutionCopySwapStatement},
			},
		},
		states:          [][]schema.Table{beforeTables, relaxedTables, widenedTables},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-copy-swap-authority",
		digest:          "sqlite-copy-swap-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err != nil {
		t.Fatalf("apply SQLite copy/swap evolution: %v", err)
	}
	var value string
	var targetOnly []byte
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT value, target_only FROM parent WHERE id = 40`,
	).Scan(&value, &targetOnly); err != nil || value != "retained" || string(targetOnly) != string([]byte{0x00, 0xff}) {
		t.Fatalf("retained parent row = (%q, %x), %v", value, targetOnly, err)
	}
	var outboundForeignKeys int
	if err := adapter.database.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_foreign_key_list('parent') WHERE "table" = 'lookup'`).Scan(&outboundForeignKeys); err != nil || outboundForeignKeys != 1 {
		t.Fatalf("retained outbound FK after copy/swap = %d, %v", outboundForeignKeys, err)
	}
	var viewValue string
	if err := adapter.database.QueryRowContext(ctx, `SELECT value FROM parent_values WHERE id = 40`).Scan(&viewValue); err != nil || viewValue != "retained" {
		t.Fatalf("retained view after copy/swap = %q, %v", viewValue, err)
	}
	var indexCount int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = 'parent_value_idx'`,
	).Scan(&indexCount); err != nil || indexCount != 1 {
		t.Fatalf("retained target-owned index = %d, %v", indexCount, err)
	}
	var sequence int64
	if err := adapter.database.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = 'parent'`).Scan(&sequence); err != nil || sequence != 40 {
		t.Fatalf("restored sqlite_sequence = %d, %v, want 40", sequence, err)
	}
	if _, err := adapter.database.ExecContext(ctx, `INSERT INTO parent(value, lookup_id) VALUES ('next', 1)`); err != nil {
		t.Fatalf("insert after restored sqlite_sequence: %v", err)
	}
	var nextID int64
	if err := adapter.database.QueryRowContext(ctx, `SELECT id FROM parent WHERE value = 'next'`).Scan(&nextID); err != nil || nextID != 41 {
		t.Fatalf("next AUTOINCREMENT id = %d, %v, want 41", nextID, err)
	}
	var temporaryObjects int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporaryObjects); err != nil || temporaryObjects != 0 {
		t.Fatalf("copy/swap temporary schema objects = %d, %v", temporaryObjects, err)
	}
	var temporarySequences int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_sequence WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporarySequences); err != nil || temporarySequences != 0 {
		t.Fatalf("copy/swap temporary sequence objects = %d, %v", temporarySequences, err)
	}
	if err := preflightSQLiteForeignKeyIntegrity(ctx, adapter.database, ""); err != nil {
		t.Fatalf("foreign-key integrity after copy/swap: %v", err)
	}
	resumed := plan
	resumed.observedPrefix = len(plan.operations)
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, resumed); err != nil {
		t.Fatalf("resume completed SQLite copy/swap evolution: %v", err)
	}
}

func TestSQLiteTargetEvolutionCopySwapWithoutAutoincrementNeedsNoSequenceTable(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER NOT NULL PRIMARY KEY,
			value VARCHAR(8) NOT NULL
		);
		INSERT INTO events(id, value) VALUES (1, 'retained');`); err != nil {
		t.Fatal(err)
	}
	var sequenceTables int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'`,
	).Scan(&sequenceTables); err != nil || sequenceTables != 0 {
		t.Fatalf("sqlite_sequence before non-identity evolution = %d, %v", sequenceTables, err)
	}
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := baseline.Tables()
	eventsIndex := findTargetSchemaEvolutionTable(before, targetSchemaEvolutionTableKey{table: "events"})
	if eventsIndex < 0 {
		t.Fatal("events missing from catalog")
	}
	after := cloneStage4RichTable(before[eventsIndex])
	valueIndex := findTargetSchemaEvolutionColumnIndex(after, "value")
	after.Columns[valueIndex].Nullable = true
	states := [][]schema.Table{before, replaceSQLiteTargetEvolutionTable(t, before, after)}
	plan := TargetSchemaEvolutionPlan{
		target: schema.SQLite,
		operations: []TargetSchemaEvolutionOperation{{
			action: SchemaContractRelaxNullability,
			objects: []schema.SchemaDriftObject{{
				Kind: schema.SchemaDriftObjectNullability, Table: "events", Column: "value",
			}},
			statements: []string{sqliteTargetEvolutionCopySwapStatement},
		}},
		states:          states,
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-no-sequence-authority",
		digest:          "sqlite-no-sequence-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err != nil {
		t.Fatalf("apply non-AUTOINCREMENT copy/swap: %v", err)
	}
	var retained string
	if err := adapter.database.QueryRowContext(ctx, `SELECT value FROM events WHERE id = 1`).Scan(&retained); err != nil || retained != "retained" {
		t.Fatalf("retained non-AUTOINCREMENT row = %q, %v", retained, err)
	}
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'`,
	).Scan(&sequenceTables); err != nil || sequenceTables != 0 {
		t.Fatalf("sqlite_sequence after non-identity evolution = %d, %v", sequenceTables, err)
	}
}

func TestSQLiteTargetEvolutionCopySwapRejectsIncomingForeignKeysBeforeMutation(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name     string
		onDelete string
		childDDL string
	}{
		{
			name:     "cascade",
			onDelete: "CASCADE",
			childDDL: `CREATE TABLE child (
				id INTEGER NOT NULL PRIMARY KEY,
				parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE
			);`,
		},
		{
			name:     "set_null",
			onDelete: "SET NULL",
			childDDL: `CREATE TABLE child (
				id INTEGER NOT NULL PRIMARY KEY,
				parent_id INTEGER REFERENCES parent(id) ON DELETE SET NULL
			);`,
		},
		{
			name:     "set_default",
			onDelete: "SET DEFAULT",
			childDDL: `CREATE TABLE child (
				id INTEGER NOT NULL PRIMARY KEY,
				parent_id INTEGER NOT NULL DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT
			);`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
			if _, err := adapter.database.ExecContext(ctx, `
				CREATE TABLE parent (
					id INTEGER NOT NULL PRIMARY KEY,
					value TEXT NOT NULL
				);
				INSERT INTO parent(id, value) VALUES (0, 'default'), (1, 'retained');
			`+test.childDDL+`
				INSERT INTO child(id, parent_id) VALUES (1, 1);`); err != nil {
				t.Fatal(err)
			}
			plan := sqliteTargetEvolutionCopySwapRelaxPlan(t, ctx, adapter, "parent", "value")

			// Preflight is strictly read-only. Its refusal must happen before
			// BEGIN IMMEDIATE/DDL so none of SQLite's DROP-triggered actions can
			// touch the dependent row.
			err := adapter.validateSQLiteTargetEvolutionCopySwapPlan(ctx, plan)
			if err == nil || !strings.Contains(err.Error(), "incoming foreign-key") ||
				!strings.Contains(err.Error(), "on_delete="+test.onDelete) {
				t.Fatalf("copy/swap preflight incoming %s FK = %v", test.onDelete, err)
			}
			if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err == nil ||
				!strings.Contains(err.Error(), "incoming foreign-key") {
				t.Fatalf("copy/swap apply incoming %s FK = %v", test.onDelete, err)
			}

			var parentValue string
			if err := adapter.database.QueryRowContext(
				ctx,
				`SELECT value FROM parent WHERE id = 1`,
			).Scan(&parentValue); err != nil || parentValue != "retained" {
				t.Fatalf("parent row after rejected copy/swap = %q, %v", parentValue, err)
			}
			var childParent sql.NullInt64
			if err := adapter.database.QueryRowContext(
				ctx,
				`SELECT parent_id FROM child WHERE id = 1`,
			).Scan(&childParent); err != nil || !childParent.Valid || childParent.Int64 != 1 {
				t.Fatalf("child row after rejected copy/swap = %#v, %v", childParent, err)
			}
			var temporary int
			if err := adapter.database.QueryRowContext(
				ctx,
				`SELECT COUNT(*) FROM sqlite_schema WHERE lower(name) LIKE '__dmtx_evolve_%'`,
			).Scan(&temporary); err != nil || temporary != 0 {
				t.Fatalf("temporary copy/swap objects after rejected admission = %d, %v", temporary, err)
			}
		})
	}
}

func TestSQLiteTargetEvolutionCopySwapScreensEveryPendingOperationBeforeBegin(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE preparation (
			id INTEGER NOT NULL PRIMARY KEY
		);
		CREATE TABLE parent (
			id INTEGER NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE TABLE child (
			id INTEGER NOT NULL PRIMARY KEY,
			parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE
		);
		INSERT INTO parent(id, value) VALUES (1, 'retained');
		INSERT INTO child(id, parent_id) VALUES (1, 1);`); err != nil {
		t.Fatal(err)
	}
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := baseline.Tables()
	preparationIndex := findTargetSchemaEvolutionTable(
		before,
		targetSchemaEvolutionTableKey{table: "preparation"},
	)
	parentIndex := findTargetSchemaEvolutionTable(
		before,
		targetSchemaEvolutionTableKey{table: "parent"},
	)
	if preparationIndex < 0 || parentIndex < 0 {
		t.Fatalf("copy/swap test catalog missing preparation/parent: %#v", before)
	}
	afterAdd := cloneStage4RichTable(before[preparationIndex])
	afterAdd.Columns = append(afterAdd.Columns, schema.Column{
		Name: "note", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "text"},
	})
	afterAddTables := replaceSQLiteTargetEvolutionTable(t, before, afterAdd)
	afterRelax := cloneStage4RichTable(afterAddTables[parentIndex])
	valueIndex := findTargetSchemaEvolutionColumnIndex(afterRelax, "value")
	if valueIndex < 0 {
		t.Fatal("parent.value missing from catalog")
	}
	afterRelax.Columns[valueIndex].Nullable = true
	plan := TargetSchemaEvolutionPlan{
		target: schema.SQLite,
		operations: []TargetSchemaEvolutionOperation{
			{
				action:     SchemaContractAddColumn,
				statements: []string{`ALTER TABLE "preparation" ADD COLUMN "note" TEXT NULL;`},
			},
			{
				action: SchemaContractRelaxNullability,
				objects: []schema.SchemaDriftObject{{
					Kind: schema.SchemaDriftObjectNullability, Table: "parent", Column: "value",
				}},
				statements: []string{sqliteTargetEvolutionCopySwapStatement},
			},
		},
		states: [][]schema.Table{
			before,
			afterAddTables,
			replaceSQLiteTargetEvolutionTable(t, afterAddTables, afterRelax),
		},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-copy-swap-all-pending-authority",
		digest:          "sqlite-copy-swap-all-pending-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err == nil ||
		!strings.Contains(err.Error(), "incoming foreign-key") {
		t.Fatalf("later pending copy/swap admission = %v", err)
	}
	var noteColumns int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM pragma_table_info('preparation') WHERE name = 'note'`,
	).Scan(&noteColumns); err != nil || noteColumns != 0 {
		t.Fatalf("earlier direct ADD mutated before later copy/swap refusal = %d, %v", noteColumns, err)
	}
	var childParent int64
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT parent_id FROM child WHERE id = 1`,
	).Scan(&childParent); err != nil || childParent != 1 {
		t.Fatalf("child row after all-pending refusal = %d, %v", childParent, err)
	}
}

func TestSQLiteTargetEvolutionCopySwapRejectsPlannedIncomingForeignKeyBeforeBegin(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE parent (
			id INTEGER NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		);
		INSERT INTO parent(id, value) VALUES (1, 'retained');`); err != nil {
		t.Fatal(err)
	}
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := baseline.Tables()
	parentIndex := findTargetSchemaEvolutionTable(before, targetSchemaEvolutionTableKey{table: "parent"})
	if parentIndex < 0 {
		t.Fatal("parent missing from catalog")
	}
	futureChild := schema.Table{
		Name: "future_child",
		Columns: []schema.Column{
			{Name: "id", Type: "integer", Nullable: false, PrimaryKey: true, PrimaryKeyPosition: 1, DeclaredType: &schema.DeclaredType{Base: "integer"}},
			{Name: "parent_id", Type: "integer", Nullable: false, DeclaredType: &schema.DeclaredType{Base: "integer"}},
		},
		ForeignKeys: []schema.ForeignKey{{
			Columns: []string{"parent_id"}, ReferencedTable: "parent", ReferencedColumns: []string{"id"},
			OnUpdate: "NO ACTION", OnDelete: "SET DEFAULT", Match: "NONE",
		}},
	}
	stateAfterCreate := append(cloneTargetSchemaEvolutionTables(before), futureChild)
	sortTargetSchemaEvolutionTables(stateAfterCreate)
	parentAfterCreateIndex := findTargetSchemaEvolutionTable(
		stateAfterCreate,
		targetSchemaEvolutionTableKey{table: "parent"},
	)
	if parentAfterCreateIndex < 0 {
		t.Fatal("parent missing from planned post-create catalog")
	}
	afterRelax := cloneStage4RichTable(stateAfterCreate[parentAfterCreateIndex])
	valueIndex := findTargetSchemaEvolutionColumnIndex(afterRelax, "value")
	if valueIndex < 0 {
		t.Fatal("parent.value missing from planned catalog")
	}
	afterRelax.Columns[valueIndex].Nullable = true
	plan := TargetSchemaEvolutionPlan{
		target: schema.SQLite,
		operations: []TargetSchemaEvolutionOperation{
			{
				action:     SchemaContractCreateTable,
				statements: []string{`CREATE TABLE "future_child" ("id" INTEGER NOT NULL PRIMARY KEY, "parent_id" INTEGER NOT NULL REFERENCES "parent"("id") ON DELETE SET DEFAULT);`},
			},
			{
				action: SchemaContractRelaxNullability,
				objects: []schema.SchemaDriftObject{{
					Kind: schema.SchemaDriftObjectNullability, Table: "parent", Column: "value",
				}},
				statements: []string{sqliteTargetEvolutionCopySwapStatement},
			},
		},
		states: [][]schema.Table{
			before,
			stateAfterCreate,
			replaceSQLiteTargetEvolutionTable(t, stateAfterCreate, afterRelax),
		},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-copy-swap-planned-incoming-authority",
		digest:          "sqlite-copy-swap-planned-incoming-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err == nil ||
		!strings.Contains(err.Error(), "planned incoming foreign-key") {
		t.Fatalf("planned incoming copy/swap admission = %v", err)
	}
	var futureChildCount int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'future_child'`,
	).Scan(&futureChildCount); err != nil || futureChildCount != 0 {
		t.Fatalf("future child created before planned copy/swap refusal = %d, %v", futureChildCount, err)
	}
}

func TestSQLiteTargetEvolutionCopySwapRefusesOwnedTriggerBeforeMutation(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE TABLE audit (value TEXT NOT NULL);
		CREATE TRIGGER events_audit AFTER INSERT ON events
		BEGIN INSERT INTO audit(value) VALUES (NEW.value); END;
		INSERT INTO events(id, value) VALUES (1, 'retained');`); err != nil {
		t.Fatal(err)
	}
	plan := sqliteTargetEvolutionCopySwapRelaxPlan(t, ctx, adapter, "events", "value")
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err == nil ||
		!strings.Contains(err.Error(), "owns trigger") {
		t.Fatalf("trigger-owned copy/swap admission = %v", err)
	}
	var value string
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT value FROM events WHERE id = 1`,
	).Scan(&value); err != nil || value != "retained" {
		t.Fatalf("row after trigger refusal = %q, %v", value, err)
	}
	var triggerCount int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger' AND name = 'events_audit'`,
	).Scan(&triggerCount); err != nil || triggerCount != 1 {
		t.Fatalf("trigger after refusal = %d, %v", triggerCount, err)
	}
}

func TestSQLiteTargetEvolutionCopySwapRejectsConcurrentTemporaryNameCollision(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		);
		INSERT INTO events(id, value) VALUES (1, 'retained');`); err != nil {
		t.Fatal(err)
	}
	plan := sqliteTargetEvolutionCopySwapRelaxPlan(t, ctx, adapter, "events", "value")
	_, _, temporary, err := sqliteTargetEvolutionCopySwapOperation(
		plan,
		0,
		plan.operations[0],
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.database.ExecContext(
		ctx,
		`CREATE TABLE `+quote(temporary)+` (id INTEGER NOT NULL PRIMARY KEY)`,
	); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err == nil ||
		!strings.Contains(err.Error(), "deterministic temporary name") {
		t.Fatalf("concurrent temporary collision admission = %v", err)
	}
	var value string
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT value FROM events WHERE id = 1`,
	).Scan(&value); err != nil || value != "retained" {
		t.Fatalf("row after temporary collision refusal = %q, %v", value, err)
	}
}

func TestSQLiteTargetEvolutionReadSequenceRejectsAmbiguousAuthority(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value TEXT NOT NULL
		);
		INSERT INTO events(value) VALUES ('retained');
		INSERT INTO sqlite_sequence(name, seq) VALUES ('EVENTS', 99);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteTargetEvolutionReadSequence(ctx, adapter.database, "events", true); err == nil ||
		!strings.Contains(err.Error(), "ambiguous or invalid") {
		t.Fatalf("ambiguous sqlite_sequence authority = %v", err)
	}
}

func TestSQLiteTargetEvolutionPostCommitVerificationRejectsRetainedDataMismatch(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER NOT NULL PRIMARY KEY,
			value TEXT NOT NULL,
			payload BLOB
		);
		INSERT INTO events(id, value, payload) VALUES (1, 'retained', X'00FF');`); err != nil {
		t.Fatal(err)
	}
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eventsIndex := findTargetSchemaEvolutionTable(
		baseline.Tables(),
		targetSchemaEvolutionTableKey{table: "events"},
	)
	if eventsIndex < 0 {
		t.Fatal("events missing from catalog")
	}
	retained, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
		ctx,
		adapter.database,
		baseline.Tables()[eventsIndex],
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := TargetSchemaEvolutionPlan{
		target:          schema.SQLite,
		states:          [][]schema.Table{baseline.Tables()},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-post-commit-retained-data-authority",
		digest:          "sqlite-post-commit-retained-data-plan",
	}
	if _, err := adapter.database.ExecContext(
		ctx,
		`UPDATE events SET value = 'changed', payload = X'FF00' WHERE id = 1`,
	); err != nil {
		t.Fatal(err)
	}
	actual, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.verifySQLiteTargetEvolutionCommittedAuthority(
		ctx,
		plan,
		[]sqliteTargetEvolutionRetainedDataAuthority{retained},
		actual,
		nil,
	); err == nil || !strings.Contains(err.Error(), "retained rows") {
		t.Fatalf("post-commit retained data mismatch = %v", err)
	}
}

func TestSQLiteTargetEvolutionCopySwapCommitAckRecoveryAuthenticatesRetainedAuthority(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value TEXT NOT NULL,
			payload BLOB
		);
		INSERT INTO events(id, value, payload) VALUES (40, 'retained', X'00FF');`); err != nil {
		t.Fatal(err)
	}
	plan := sqliteTargetEvolutionCopySwapRelaxPlan(t, ctx, adapter, "events", "value")
	committed := false
	adapter.evolutionCommit = func(
		commitCtx context.Context,
		connection *sql.Conn,
	) (sql.Result, error) {
		result, err := connection.ExecContext(commitCtx, "COMMIT")
		if err != nil {
			return result, err
		}
		committed = true
		return result, errors.New("injected SQLite COMMIT acknowledgement loss")
	}
	// This calls the production ApplyTargetSchemaEvolutionPlan branch. The
	// underlying COMMIT succeeds, then the simulated lost acknowledgement must
	// be resolved by detached catalog, retained-row, and sequence authority.
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err != nil {
		t.Fatalf("recover committed copy/swap after lost acknowledgement: %v", err)
	}
	if !committed {
		t.Fatal("test COMMIT hook did not commit before reporting acknowledgement loss")
	}
	var value string
	var payload []byte
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT value, payload FROM events WHERE id = 40`,
	).Scan(&value, &payload); err != nil || value != "retained" || string(payload) != string([]byte{0x00, 0xff}) {
		t.Fatalf("retained row after commit recovery = (%q, %x), %v", value, payload, err)
	}
	var sequence int64
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = 'events'`,
	).Scan(&sequence); err != nil || sequence != 40 {
		t.Fatalf("AUTOINCREMENT frontier after commit recovery = %d, %v", sequence, err)
	}
	var temporarySchema, temporarySequence int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporarySchema); err != nil || temporarySchema != 0 {
		t.Fatalf("temporary schema artifacts after commit recovery = %d, %v", temporarySchema, err)
	}
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_sequence WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporarySequence); err != nil || temporarySequence != 0 {
		t.Fatalf("temporary sequence artifacts after commit recovery = %d, %v", temporarySequence, err)
	}
}

func TestSQLiteTargetEvolutionCopySwapCommitAckRecoveryRefusesRetainedDataDrift(t *testing.T) {
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	if _, err := adapter.database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value TEXT NOT NULL,
			payload BLOB
		);
		INSERT INTO events(id, value, payload) VALUES (40, 'retained', X'00FF');`); err != nil {
		t.Fatal(err)
	}
	plan := sqliteTargetEvolutionCopySwapRelaxPlan(t, ctx, adapter, "events", "value")
	// The pinned mutation connection consumes the target adapter's normal
	// single pool slot. Let this test use a second connection to model an
	// independent post-COMMIT writer: the production post-ack verification
	// must fail closed if such a write changes retained data before its fresh
	// authority read.
	adapter.database.SetMaxOpenConns(2)
	adapter.database.SetMaxIdleConns(2)
	adapter.evolutionCommit = func(
		commitCtx context.Context,
		connection *sql.Conn,
	) (sql.Result, error) {
		result, err := connection.ExecContext(commitCtx, "COMMIT")
		if err != nil {
			return result, err
		}
		if _, err := adapter.database.ExecContext(
			commitCtx,
			`UPDATE events SET value = 'changed', payload = X'FF00' WHERE id = 40`,
		); err != nil {
			return result, err
		}
		return result, errors.New("injected SQLite COMMIT acknowledgement loss")
	}
	err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan)
	if err == nil || !strings.Contains(err.Error(), "retained rows") {
		t.Fatalf("commit acknowledgement recovery accepted retained-data drift: %v", err)
	}
	var value string
	var payload []byte
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT value, payload FROM events WHERE id = 40`,
	).Scan(&value, &payload); err != nil || value != "changed" || string(payload) != string([]byte{0xff, 0x00}) {
		t.Fatalf("external post-commit retained-row drift = (%q, %x), %v", value, payload, err)
	}
	var sequence int64
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = 'events'`,
	).Scan(&sequence); err != nil || sequence != 40 {
		t.Fatalf("AUTOINCREMENT frontier after failed commit recovery = %d, %v", sequence, err)
	}
	var temporarySchema, temporarySequence int
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporarySchema); err != nil || temporarySchema != 0 {
		t.Fatalf("temporary schema artifacts after failed commit recovery = %d, %v", temporarySchema, err)
	}
	if err := adapter.database.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_sequence WHERE lower(name) LIKE '__dmtx_evolve_%'`,
	).Scan(&temporarySequence); err != nil || temporarySequence != 0 {
		t.Fatalf("temporary sequence artifacts after failed commit recovery = %d, %v", temporarySequence, err)
	}
}

func sqliteTargetEvolutionCopySwapRelaxPlan(
	t *testing.T,
	ctx context.Context,
	adapter *sqliteTargetAdapter,
	table string,
	column string,
) TargetSchemaEvolutionPlan {
	t.Helper()
	baseline, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := baseline.Tables()
	index := findTargetSchemaEvolutionTable(before, targetSchemaEvolutionTableKey{table: table})
	if index < 0 {
		t.Fatalf("copy/swap plan table %s is absent", table)
	}
	after := cloneStage4RichTable(before[index])
	columnIndex := findTargetSchemaEvolutionColumnIndex(after, column)
	if columnIndex < 0 {
		t.Fatalf("copy/swap plan column %s.%s is absent", table, column)
	}
	after.Columns[columnIndex].Nullable = true
	return TargetSchemaEvolutionPlan{
		target: schema.SQLite,
		operations: []TargetSchemaEvolutionOperation{{
			action: SchemaContractRelaxNullability,
			objects: []schema.SchemaDriftObject{{
				Kind: schema.SchemaDriftObjectNullability, Table: table, Column: column,
			}},
			statements: []string{sqliteTargetEvolutionCopySwapStatement},
		}},
		states:          [][]schema.Table{before, replaceSQLiteTargetEvolutionTable(t, before, after)},
		reservations:    baseline.Reservations(),
		authorityDigest: "sqlite-copy-swap-relax-authority-" + table,
		digest:          "sqlite-copy-swap-relax-plan-" + table,
	}
}

func openSQLiteTargetEvolutionTestAdapter(t *testing.T, ctx context.Context) *sqliteTargetAdapter {
	t.Helper()
	opened, err := openSQLiteTargetAdapter(ctx, config.Endpoint{
		Database: filepath.Join(t.TempDir(), "target.sqlite"),
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, ok := opened.(*sqliteTargetAdapter)
	if !ok {
		t.Fatalf("opened SQLite target = %T", opened)
	}
	t.Cleanup(func() { _ = adapter.database.Close() })
	return adapter
}

func sqliteTargetEvolutionTestTable(name string) schema.Table {
	return schema.Table{
		Name: name,
		Columns: []schema.Column{{
			Name: "id", Type: "integer", Nullable: false,
			PrimaryKey: true, PrimaryKeyPosition: 1,
			DeclaredType: &schema.DeclaredType{Base: "integer"},
		}},
	}
}

func sqliteTargetEvolutionReservationPresent(catalog TargetSchemaEvolutionCatalog, scope, name string) bool {
	for _, reservation := range catalog.Reservations() {
		if reservation.Scope == scope && reservation.Name == name {
			return true
		}
	}
	return false
}

func sqliteTargetEvolutionDefinitionPresent(catalog TargetSchemaEvolutionCatalog, scope, prefix string) bool {
	for _, reservation := range catalog.Reservations() {
		if reservation.Scope == scope && strings.HasPrefix(reservation.Name, prefix) {
			return true
		}
	}
	return false
}

func replaceSQLiteTargetEvolutionTable(
	t *testing.T,
	tables []schema.Table,
	replacement schema.Table,
) []schema.Table {
	t.Helper()
	result := cloneTargetSchemaEvolutionTables(tables)
	index := findTargetSchemaEvolutionTable(result, targetSchemaEvolutionTableKey{table: replacement.Name})
	if index < 0 {
		t.Fatalf("replace SQLite evolution table %s: missing", replacement.Name)
	}
	result[index] = replacement
	sortTargetSchemaEvolutionTables(result)
	return result
}

func installSQLiteTargetEvolutionBaseline(
	t *testing.T,
	ctx context.Context,
	backend state.YAMLStore,
	target *sqliteTargetAdapter,
	cfg config.Config,
	runID string,
	started time.Time,
) {
	installSQLiteTargetEvolutionBaselineWithSource(
		t,
		ctx,
		backend,
		target,
		cfg,
		runID,
		started,
		nil,
	)
}

func installSQLiteTargetEvolutionBaselineWithSource(
	t *testing.T,
	ctx context.Context,
	backend stage4LifecycleTestBackend,
	target *sqliteTargetAdapter,
	cfg config.Config,
	runID string,
	started time.Time,
	sourceTables []schema.Table,
) {
	t.Helper()
	initializeStage4LifecycleRun(t, backend, runID, started)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	configDigest, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	options := Stage4SchemaGateOptions{
		SourceEngine:       "postgres",
		TargetEngine:       "sqlite",
		TargetMode:         "upsert",
		IncludeTables:      cfg.Migration.IncludeTables,
		ExcludeTables:      cfg.Migration.ExcludeTables,
		ConfigIdentity:     configDigest,
		Contract:           cfg.Migration.SchemaContract,
		FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
		DateUpdatedColumns: cfg.Migration.DateUpdatedColumns,
		CapturedAt:         started,
	}
	gate, err := PrepareStage4SchemaGate(run, sourceTables, options)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := target.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewStage4TargetShapeSeed(catalog)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := PrepareStage4TargetShapeAuthority(run, gate, options, seed)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate, authority, "postgres", target, "upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	pendingTarget, err := BindStage4TargetShapeProjection(authority, projection)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(gate.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(pendingTarget); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(ctx, run, authority.Task(), stage4TargetShapeRangeID, authority.TopologyHash()); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(ctx, run, gate.Task, stage4SchemaGateRangeID, gate.TopologyHash); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID: runID, Source: "source", Target: "target", SourceEngine: "postgres",
		SourceIdentity: "postgres:source.example:5432/app", TargetIdentity: "postgres:target.example:5432/app",
		Outcome: state.Success, Resumable: true, Reason: "complete", StartedAt: started, EndedAt: started.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
}
