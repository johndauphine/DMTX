package migrate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestSQLServerSQLiteUpsertCompositeKeyPreflightAndReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	source := sqlServerSQLiteUpsertCompositeTable()
	planned, err := adapter.PlanTables("mssql", []schema.Table{source}, "upsert")
	if err != nil {
		t.Fatalf("plan SQL Server-to-SQLite upsert: %v", err)
	}
	if !adapter.sqlServerRoute || adapter.sourceEngine != "mssql" || len(planned) != 1 {
		t.Fatalf("SQL Server-to-SQLite upsert route = %#v", adapter)
	}
	create, err := schema.CreateTable(schema.SQLite, planned[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.database.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	catalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read projected SQL Server composite-key target catalog: %v", err)
	}
	var actual schema.Table
	for _, table := range catalog.Tables() {
		if table.Name == source.Name {
			actual = table
			break
		}
	}
	if len(actual.Columns) != len(planned[0].Columns) ||
		actual.Columns[0].Type != "int" ||
		actual.Columns[0].DeclaredType == nil ||
		actual.Columns[0].DeclaredType.Base != "int" {
		t.Fatalf("projected SQL Server INT composite-key catalog = %#v", actual)
	}
	contains, err := stage4TargetAuthorityTableContains(actual, planned[0])
	if err != nil || !contains {
		t.Fatalf(
			"projected SQL Server composite-key authority mismatch: contains=%v err=%v authority=%#v desired=%#v",
			contains, err, actual, planned[0],
		)
	}
	if err := adapter.PreflightTables(ctx, planned, "upsert"); err != nil {
		t.Fatalf("preflight projected SQL Server upsert target: %v", err)
	}
	if err := adapter.PreflightStage4NetworkReplayIsolation(ctx, planned); err != nil {
		t.Fatalf("preflight projected SQL Server replay target: %v", err)
	}
	columns := adapterColumnNames(source)
	first := [][]any{{
		int64(7), int64(11), "first",
		time.Date(2026, 8, 1, 12, 34, 56, 123456000, time.UTC),
	}}
	if receipt, err := adapter.WriteStage4NetworkBatch(ctx, planned[0], columns, first); err != nil ||
		receipt.Certainty != CommitDurable || receipt.CommittedRows != 1 {
		t.Fatalf("initial SQL Server-to-SQLite upsert receipt=%#v err=%v", receipt, err)
	}
	// A durable issued page can be replayed through SQLite's idempotent upsert
	// writer. The same composite key remains a single row and values are bound
	// using the target's normalized timestamp representation.
	if receipt, err := adapter.WriteStage4NetworkBatch(ctx, planned[0], columns, first); err != nil ||
		receipt.Certainty != CommitDurable || receipt.CommittedRows != 1 {
		t.Fatalf("replayed SQL Server-to-SQLite upsert receipt=%#v err=%v", receipt, err)
	}
	changed := [][]any{{
		int64(7), int64(11), "changed",
		time.Date(2026, 8, 1, 12, 34, 57, 0, time.UTC),
	}}
	if receipt, err := adapter.WriteStage4NetworkBatch(ctx, planned[0], columns, changed); err != nil ||
		receipt.Certainty != CommitDurable {
		t.Fatalf("ordinary SQL Server-to-SQLite idempotent update receipt=%#v err=%v", receipt, err)
	}
	var count int
	var payload, occurredAt string
	if err := adapter.database.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(payload), MAX(occurred_at)
		  FROM orders
		 WHERE tenant_id = 7 AND order_id = 11`,
	).Scan(&count, &payload, &occurredAt); err != nil {
		t.Fatal(err)
	}
	if count != 1 || payload != "changed" || occurredAt != "2026-08-01 12:34:57.000000" {
		t.Fatalf("SQL Server-to-SQLite composite key row=(%d,%q,%q)", count, payload, occurredAt)
	}
}

func TestSQLServerSQLiteIdentityProjectionRoundTripsTargetCatalog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	source := sqlServerSQLiteUpsertTable()
	source.Identity = &schema.Identity{
		Column: "id", Generation: schema.IdentityByDefault,
	}
	planned, err := adapter.PlanTables("mssql", []schema.Table{source}, "upsert")
	if err != nil {
		t.Fatalf("plan SQL Server identity upsert target: %v", err)
	}
	create, err := schema.CreateTable(schema.SQLite, planned[0])
	if err != nil {
		t.Fatalf("render projected SQLite identity: %v", err)
	}
	if _, err := adapter.database.ExecContext(ctx, create); err != nil {
		t.Fatalf("create projected SQLite identity: %v", err)
	}
	discovered, _, err := inspectSQLiteSchema(ctx, adapter.database, source.Name)
	if err != nil {
		t.Fatalf("rediscover physical SQLite identity: %v", err)
	}
	if discovered.Identity == nil ||
		discovered.Columns[0].DeclaredType == nil ||
		discovered.Columns[0].DeclaredType.Base != "integer" ||
		!discovered.Columns[0].PrimaryKey || discovered.Columns[0].Nullable {
		t.Fatalf("physical SQLite identity discovery = %#v", discovered)
	}
	if err := validateSQLiteRetainedTable(planned[0], discovered); err != nil {
		t.Fatalf("projected SQL Server identity did not retain exact SQLite shape: %v", err)
	}
	catalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read canonical SQLite evolution catalog: %v", err)
	}
	var canonical schema.Table
	for _, table := range catalog.Tables() {
		if table.Name == source.Name {
			canonical = table
			break
		}
	}
	if canonical.Identity == nil || len(canonical.Columns) != 2 ||
		canonical.Columns[0].Type != "bigint" ||
		canonical.Columns[0].DeclaredType == nil ||
		canonical.Columns[0].DeclaredType.Base != "bigint" ||
		canonical.Columns[0].Nullable {
		t.Fatalf("canonical SQLite evolution identity = %#v", canonical)
	}
}

func TestSQLServerSQLiteTargetShapeSnapshotMaterializesBooleanCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := openSQLiteTargetEvolutionTestAdapter(t, ctx)
	source := sqlServerSQLiteUpsertTable()
	source.Name = "flags"
	source.Identity = &schema.Identity{
		Column: "id", Generation: schema.IdentityByDefault,
	}
	source.Columns = append(source.Columns, schema.Column{
		Name: "enabled", Type: "boolean",
		DeclaredType: &schema.DeclaredType{Base: "bool"},
	})
	planned, err := adapter.PlanTables("mssql", []schema.Table{source}, "upsert")
	if err != nil {
		t.Fatalf("plan SQL Server boolean upsert target: %v", err)
	}
	create, err := schema.CreateTable(schema.SQLite, planned[0])
	if err != nil {
		t.Fatalf("render projected SQLite boolean target: %v", err)
	}
	if _, err := adapter.database.ExecContext(ctx, create); err != nil {
		t.Fatalf("create projected SQLite boolean target: %v", err)
	}
	catalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read SQLite boolean target catalog: %v", err)
	}
	snapshot, err := schema.NewSchemaSnapshot(catalog.Tables())
	if err != nil {
		t.Fatalf("snapshot SQLite boolean target catalog: %v", err)
	}
	materialized, err := stage4MaterializeTargetShapeSnapshot(snapshot, "sqlite")
	if err != nil {
		t.Fatalf("materialize durable SQLite boolean target shape: %v", err)
	}
	if len(materialized) != 1 || len(materialized[0].Checks) != 1 ||
		!strings.Contains(materialized[0].Checks[0].Expression.CanonicalSQL(), "IN") {
		t.Fatalf("materialized SQLite boolean target shape = %#v", materialized)
	}
	contains, err := stage4TargetAuthorityTableContains(
		materialized[0], planned[0],
	)
	if err != nil || !contains {
		t.Fatalf(
			"canonical SQLite authority does not contain projected SQL Server shape: contains=%v err=%v authority=%#v desired=%#v",
			contains,
			err,
			materialized[0],
			planned[0],
		)
	}
}

func TestStage4SQLServerSQLiteUpsertReplaysIssuedPage(t *testing.T) {
	ctx := context.Background()
	targetPath := filepath.Join(t.TempDir(), "target.sqlite")
	opened, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	target := opened.(*sqliteTargetAdapter)
	t.Cleanup(func() { _ = target.database.Close() })
	sourceTable := sqlServerSQLiteUpsertTable()
	planned, err := target.PlanTables("mssql", []schema.Table{sourceTable}, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	create, err := schema.CreateTable(schema.SQLite, planned[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.database.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	backendPath := filepath.Join(t.TempDir(), "state.yaml")
	rawBackend := state.YAMLStore{Path: backendPath}
	runID := "stage4-mssql-sqlite-upsert-replay"
	initializeStage4LifecycleRun(t, rawBackend, runID, time.Now().Add(-time.Minute))
	backend := &sqlServerSQLiteFailAckBackend{
		Stage4StateBackend: rawBackend,
		failNext:           true,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{sourceTable.Name}
	cfg.Migration.Partitions = 1
	cfg.Migration.ChunkSize = 1
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	events := make([]string, 0)
	source := &sqlServerSQLiteRecordingSource{recordingAdapterSource: &recordingAdapterSource{
		events: &events,
		table:  sourceTable,
		ids:    []int64{1, 2},
		rows:   []string{"first", "later"},
	}}
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	result, err := migrateWithStage4Adapters(ctx, cfg, observer, source, target, "upsert", run)
	if err == nil || !strings.Contains(err.Error(), "injected SQL Server-to-SQLite acknowledgement failure") {
		t.Fatalf("first SQL Server-to-SQLite issued-page result=%#v err=%v", result, err)
	}
	var firstCount int
	if err := target.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&firstCount); err != nil || firstCount != 1 {
		t.Fatalf("first issued SQL Server-to-SQLite page count=%d err=%v", firstCount, err)
	}

	run.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    run,
	}
	result, err = resumeWithStage4Adapters(
		ctx, cfg, CompletedTableCheckpoints{}, resumeObserver, resumeObserver,
		source, target, "upsert", run,
	)
	if err != nil {
		t.Fatalf("resume SQL Server-to-SQLite issued page: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("resumed SQL Server-to-SQLite result=%#v", result)
	}
	var count int
	if err := target.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != 2 {
		t.Fatalf("resumed SQL Server-to-SQLite rows=%d err=%v", count, err)
	}
	if backend.calls < 2 {
		t.Fatalf("SQL Server-to-SQLite replay did not retry issued acknowledgement: calls=%d", backend.calls)
	}
}

type sqlServerSQLiteRecordingSource struct{ *recordingAdapterSource }

func (*sqlServerSQLiteRecordingSource) Engine() string { return "mssql" }

// The shared recording source deliberately asserts PostgreSQL's public schema.
// This wrapper keeps its conservative row/value behavior but exposes the SQL
// Server dbo catalog that the SQLite projection must admit.
func (source *sqlServerSQLiteRecordingSource) OpenRows(
	_ context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	if table.Schema != "dbo" || len(columns) != 2 ||
		columns[0] != "id" || columns[1] != "payload" {
		return nil, errSQLServerSQLiteSourceShape
	}
	rows := make([][]any, len(source.ids))
	for index, id := range source.identifiers() {
		rows[index] = []any{id, source.payloads()[index]}
	}
	return &sqlServerSQLiteFixtureRows{rows: rows, index: -1}, nil
}

func (source *sqlServerSQLiteRecordingSource) openStableNetworkTableSource(
	ctx context.Context,
	table schema.Table,
) (*adapterStableNetworkTableSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	catalog, err := source.InspectTable(ctx, table.Name)
	if err != nil {
		return nil, err
	}
	if catalog.Schema != table.Schema {
		return nil, errSQLServerSQLiteSourceShape
	}
	return &adapterStableNetworkTableSession{
		source:      source,
		readerLimit: 1,
		closeFn:     func() error { return nil },
	}, nil
}

func (source *sqlServerSQLiteRecordingSource) PlanPagination(
	ctx context.Context,
	table schema.Table,
	requestedPartitions int,
) (PaginationPlan, error) {
	plan, err := source.recordingAdapterSource.PlanPagination(
		ctx, table, requestedPartitions,
	)
	if err != nil {
		return PaginationPlan{}, err
	}
	keys, err := adapterPaginationPrimaryKey("mssql", table.Schema, table)
	if err != nil {
		return PaginationPlan{}, err
	}
	evidence := make([]adapterPaginationKeyEvidence, len(keys))
	for index, key := range keys {
		evidence[index] = adapterPaginationKeyEvidence{
			Name:        key.Name,
			Type:        key.Type,
			Nullable:    key.Nullable,
			Position:    key.PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(key.DeclaredType),
		}
	}
	plan.TopologyHash, err = adapterPaginationTopologyHash(
		"mssql", table, requestedPartitions, evidence, plan,
	)
	if err != nil {
		return PaginationPlan{}, err
	}
	return plan, nil
}

func (source *sqlServerSQLiteRecordingSource) PlanRetainedRowWidth(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	table.Schema = "public"
	return source.recordingAdapterSource.PlanRetainedRowWidth(
		ctx, table, columns,
	)
}

func (source *sqlServerSQLiteRecordingSource) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	// The shared recorder is intentionally PostgreSQL-shaped internally. Give
	// it an equivalent private pagination proof while the outer source retains
	// the SQL Server topology that production admission binds and persists.
	recorderTable := cloneStage4RichTable(table)
	// PostgreSQL's test recorder models its integer pagination evidence as an
	// unadorned type. This is private fixture state; production SQL Server
	// planning continues to bind the declared bigint evidence above.
	recorderTable.Columns[0].DeclaredType = nil
	recorderPagination, err := source.recordingAdapterSource.PlanPagination(
		ctx, recorderTable, len(pagination.Ranges),
	)
	if err != nil {
		return NetworkReadPage{}, err
	}
	page, err := source.recordingAdapterSource.ReadNetworkRangePage(
		ctx, recorderTable, columns, recorderPagination, plannedRange, request,
	)
	if err != nil {
		return NetworkReadPage{}, err
	}
	for rowIndex := range page.Rows {
		for columnIndex, value := range page.Rows[rowIndex] {
			if bytes, ok := value.([]byte); ok {
				page.Rows[rowIndex][columnIndex] = string(bytes)
			}
		}
	}
	return page, nil
}

var errSQLServerSQLiteSourceShape = &sqlServerSQLiteSourceShapeError{}

type sqlServerSQLiteSourceShapeError struct{}

func (*sqlServerSQLiteSourceShapeError) Error() string {
	return "SQL Server-to-SQLite fixture source shape is invalid"
}

type sqlServerSQLiteFixtureRows struct {
	rows  [][]any
	index int
}

func (rows *sqlServerSQLiteFixtureRows) Next() bool {
	rows.index++
	return rows.index < len(rows.rows)
}

func (rows *sqlServerSQLiteFixtureRows) Scan(destinations ...any) error {
	if rows.index < 0 || rows.index >= len(rows.rows) ||
		len(destinations) != len(rows.rows[rows.index]) {
		return errSQLServerSQLiteSourceShape
	}
	for index, destination := range destinations {
		pointer, ok := destination.(*any)
		if !ok {
			return errSQLServerSQLiteSourceShape
		}
		*pointer = rows.rows[rows.index][index]
	}
	return nil
}

func (*sqlServerSQLiteFixtureRows) Err() error   { return nil }
func (*sqlServerSQLiteFixtureRows) Close() error { return nil }

type sqlServerSQLiteFailAckBackend struct {
	Stage4StateBackend
	failNext bool
	calls    int
}

func (backend *sqlServerSQLiteFailAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	backend.calls++
	if backend.failNext {
		backend.failNext = false
		return state.RangeState{}, errSQLServerSQLiteAcknowledgement
	}
	return backend.Stage4StateBackend.AcknowledgeRange(acknowledgement)
}

var errSQLServerSQLiteAcknowledgement = &sqlServerSQLiteAcknowledgementError{}

type sqlServerSQLiteAcknowledgementError struct{}

func (*sqlServerSQLiteAcknowledgementError) Error() string {
	return "injected SQL Server-to-SQLite acknowledgement failure"
}

func sqlServerSQLiteUpsertTable() schema.Table {
	return schema.Table{
		Schema: "dbo",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "payload", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
		},
	}
}

func sqlServerSQLiteUpsertCompositeTable() schema.Table {
	table := sqlServerSQLiteUpsertTable()
	table.Name = "orders"
	table.Columns = []schema.Column{
		{
			Name: "tenant_id", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 1,
			DeclaredType: &schema.DeclaredType{Base: "int"},
		},
		{
			Name: "order_id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 2,
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		{
			Name: "payload", Type: "text", DeclaredType: &schema.DeclaredType{Base: "text"},
		},
		{
			Name: "occurred_at", Type: "datetime",
			DeclaredType: &schema.DeclaredType{Base: "timestamp", Arguments: []int{6}},
		},
	}
	return table
}
