package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestAdapterNetworkRangePageQueryUsesExactEngineSemantics(
	t *testing.T,
) {
	t.Parallel()

	keys := []KeySpec{
		{Name: "tenant", Kind: KeyInteger},
		{Name: "id", Kind: KeyInteger},
	}
	table := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "tenant"},
			{Name: "id"},
			{Name: "payload"},
		},
	}
	tests := []struct {
		name      string
		engine    string
		namespace string
		columns   []string
		keys      []KeySpec
		effective []int64
		upper     []int64
		wantSQL   string
		wantArgs  []any
	}{
		{
			name:      "PostgreSQL tuple",
			engine:    "postgres",
			namespace: "public",
			columns:   []string{"tenant", "id", "payload"},
			keys:      keys,
			effective: []int64{1, 2},
			upper:     []int64{9, 10},
			wantSQL: `SELECT "tenant", "id", "payload" FROM ` +
				`"public"."events" WHERE ("tenant", "id") > ($1, $2)` +
				` AND ("tenant", "id") <= ($3, $4)` +
				` ORDER BY "tenant" ASC, "id" ASC LIMIT $5`,
			wantArgs: []any{int64(1), int64(2), int64(9), int64(10), 3},
		},
		{
			name:      "MySQL family tuple",
			engine:    "mysql",
			namespace: "source",
			columns:   []string{"tenant", "id", "payload"},
			keys:      keys,
			effective: []int64{1, 2},
			upper:     []int64{9, 10},
			wantSQL: "SELECT `tenant`, `id`, `payload` FROM " +
				"`source`.`events` WHERE (`tenant`, `id`) > (?, ?)" +
				" AND (`tenant`, `id`) <= (?, ?)" +
				" ORDER BY `tenant` ASC, `id` ASC LIMIT ?",
			wantArgs: []any{int64(1), int64(2), int64(9), int64(10), 3},
		},
		{
			name:      "SQLite tuple",
			engine:    "sqlite",
			namespace: "",
			columns:   []string{"tenant", "id", "payload"},
			keys:      keys,
			effective: []int64{1, 2},
			upper:     []int64{9, 10},
			wantSQL: `SELECT "tenant", "id", "payload" FROM "events"` +
				` WHERE ("tenant", "id") > (?, ?)` +
				` AND ("tenant", "id") <= (?, ?)` +
				` ORDER BY "tenant" ASC, "id" ASC LIMIT ?`,
			wantArgs: []any{int64(1), int64(2), int64(9), int64(10), 3},
		},
		{
			name:      "SQL Server single integer",
			engine:    "mssql",
			namespace: "dbo",
			columns:   []string{"id", "payload"},
			keys:      []KeySpec{{Name: "id", Kind: KeyInteger}},
			effective: []int64{2},
			upper:     []int64{10},
			wantSQL: "SELECT TOP (@p1) [id], [payload] FROM " +
				"[dbo].[events] WHERE [id] > @p2 AND [id] <= @p3 " +
				"ORDER BY [id] ASC",
			wantArgs: []any{
				3,
				int64(2), int64(10),
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testTable := table
			testTable.Schema = test.namespace
			admission := adapterRangePageAdmission{
				engine:       test.engine,
				namespace:    test.namespace,
				table:        testTable,
				columnNames:  test.columns,
				keys:         test.keys,
				effective:    test.effective,
				hasEffective: true,
				upper:        test.upper,
				request: NetworkReadRequest{
					MaxRows: 3,
				},
			}
			query, err := buildAdapterNetworkRangePageQuery(admission)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantSQL != "" && query.SQL != test.wantSQL {
				t.Fatalf("query =\n%s\nwant\n%s", query.SQL, test.wantSQL)
			}
			if !reflect.DeepEqual(query.Args, test.wantArgs) {
				t.Fatalf("arguments = %#v, want %#v", query.Args, test.wantArgs)
			}
		})
	}
}

func TestAdapterNetworkRangePageFailsClosedBeforeQuery(
	t *testing.T,
) {
	t.Parallel()

	signed := adapterRangePagePostgresTable()
	good := adapterRangePageIntegerPlan(10)
	baseRequest := adapterRangePageRequest(signed, good, 0, 2)
	tests := []struct {
		name      string
		table     schema.Table
		plan      PaginationPlan
		rangePlan PaginationRange
		request   NetworkReadRequest
		wantClass TransferErrorClass
		secret    string
	}{
		{
			name: "ROW_NUMBER",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{{
					Name: "id", Type: "text", Nullable: false,
					PrimaryKey: true, PrimaryKeyPosition: 1,
				}},
			},
			plan: PaginationPlan{
				Strategy: PaginationRowNumber,
				Keys:     []KeySpec{{Name: "id", Kind: KeyText}},
				Ranges: []PaginationRange{{
					ID: 0, FirstRow: 1, LastRow: 10,
				}},
				TopologyHash: strings.Repeat("a", 64),
			},
			rangePlan: PaginationRange{
				ID: 0, FirstRow: 1, LastRow: 10,
			},
			request: NetworkReadRequest{
				Range: NetworkRangePlan{
					TableSchema:  "public",
					TableName:    "events",
					TopologyHash: "network-topology",
					Pagination:   PaginationRowNumber,
					MaxRowBytes:  4096,
				},
				MaxRows: 2,
			},
			wantClass: ErrorClassPolicy,
		},
		{
			name:  "text key injected into integer plan",
			table: signed,
			plan: func() PaginationPlan {
				plan := good
				plan.Keys = append([]KeySpec(nil), good.Keys...)
				plan.Ranges = append([]PaginationRange(nil), good.Ranges...)
				plan.Keys = []KeySpec{{Name: "id", Kind: KeyText}}
				upper := KeyTuple{TextKey("sensitive-upper")}
				plan.Ranges[0].Upper = &upper
				return plan
			}(),
			rangePlan: func() PaginationRange {
				value := PaginationRange{ID: 0}
				upper := KeyTuple{TextKey("sensitive-upper")}
				value.Upper = &upper
				return value
			}(),
			request:   baseRequest,
			wantClass: ErrorClassPolicy,
			secret:    "sensitive-upper",
		},
		{
			name:      "malformed typed frontier",
			table:     signed,
			plan:      good,
			rangePlan: good.Ranges[0],
			request: func() NetworkReadRequest {
				request := baseRequest
				request.StartFrontier = []byte(
					`[{"kind":"int64","encoded":"sensitive-frontier"}]`,
				)
				return request
			}(),
			wantClass: ErrorClassState,
			secret:    "sensitive-frontier",
		},
		{
			name:      "malformed durable replay end",
			table:     signed,
			plan:      good,
			rangePlan: good.Ranges[0],
			request: func() NetworkReadRequest {
				request := baseRequest
				request.ReplayExpected = &NetworkIssuedChunk{
					RangeIndex: 0,
					Sequence:   0,
					Rows:       2,
					EndFrontier: []byte(
						`[{"kind":"int64","encoded":"sensitive-replay"}]`,
					),
					Fingerprint: strings.Repeat("c", 64),
				}
				return request
			}(),
			wantClass: ErrorClassState,
			secret:    "sensitive-replay",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			queryer := &adapterRangePageRejectingQueryer{}
			_, err := readAdapterNetworkRangePage(
				context.Background(),
				"postgres",
				"public",
				queryer,
				nil,
				test.table,
				[]string{"id"},
				test.plan,
				test.rangePlan,
				test.request,
			)
			if err == nil ||
				ClassifyTransferError(err) != test.wantClass {
				t.Fatalf("error = %v, want class %s", err, test.wantClass)
			}
			if queryer.calls != 0 {
				t.Fatalf("query calls = %d, want zero", queryer.calls)
			}
			if test.secret != "" &&
				strings.Contains(err.Error(), test.secret) {
				t.Fatalf("error leaked a row key: %v", err)
			}
		})
	}
}

func TestSQLiteNetworkRangePageFeedsResumableCoreAndReplays(
	t *testing.T,
) {
	t.Parallel()

	source, table := openAdapterRangePageSQLiteFixture(t)
	plan := adapterRangePageTuplePlan(3, 1)
	plannedRange := plan.Ranges[0]
	networkPlan := networkTransferTestPlan(1)
	networkPlan.SourceEngine = "sqlite"
	networkPlan.TargetEngine = "sqlite"
	networkPlan.Resources.ChunkRows.Value = 2
	networkPlan.Resources.Readers.Value = 1
	networkPlan.Resources.Writers.Value = 1
	networkPlan.Resources.QueueDepth.Value = 1
	networkPlan.Ranges[0] = NetworkRangePlan{
		RangeIndex:   0,
		TableName:    table.Name,
		TopologyHash: "network-topology",
		Pagination:   PaginationTupleKeyset,
		MaxRowBytes:  4096,
	}

	var written [][]any
	var issued []NetworkIssuedChunk
	callbacks := NetworkTransferCallbacks{
		ReadPage: func(
			ctx context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			return source.ReadNetworkRangePage(
				ctx,
				table,
				[]string{"tenant", "id", "payload"},
				plan,
				plannedRange,
				request,
			)
		},
		WritePage: func(
			_ context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			written = append(written, cloneNetworkTestRows(request.Rows)...)
			return WriteReceipt{
				Certainty:     CommitDurable,
				AttemptOffset: request.AttemptOffset,
				AttemptedRows: int64(len(request.Rows)),
				CommittedRows: int64(len(request.Rows)),
			}, nil
		},
		RecordIssued: func(
			_ context.Context,
			value NetworkIssuedChunk,
		) error {
			issued = append(issued, cloneNetworkIssuedChunk(value))
			return nil
		},
		Checkpoint: func(
			context.Context,
			NetworkRangeCheckpoint,
		) error {
			return nil
		},
	}
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		networkPlan,
		callbacks,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 || result.CompletedRanges != 1 ||
		len(written) != 4 || len(issued) != 2 ||
		issued[0].Exhausted || !issued[1].Exhausted {
		t.Fatalf(
			"result=%#v written=%#v issued=%#v",
			result,
			written,
			issued,
		)
	}
	if got := [][2]int64{
		{written[0][0].(int64), written[0][1].(int64)},
		{written[1][0].(int64), written[1][1].(int64)},
		{written[2][0].(int64), written[2][1].(int64)},
		{written[3][0].(int64), written[3][1].(int64)},
	}; !reflect.DeepEqual(got, [][2]int64{
		{-2, 9}, {1, 1}, {1, 4}, {3, 1},
	}) {
		t.Fatalf("ordered keys = %#v", got)
	}
	for index, row := range written {
		measured, err := measureAdapterRetainedRowBytes(row)
		if err != nil {
			t.Fatal(err)
		}
		pageIndex := index / 2
		rowIndex := index % 2
		request := adapterRangePageRequest(table, plan, 0, 2)
		if pageIndex == 1 {
			request.Sequence = 1
			request.StartFrontier = issued[0].EndFrontier
		}
		page, err := source.ReadNetworkRangePage(
			context.Background(),
			table,
			[]string{"tenant", "id", "payload"},
			plan,
			plannedRange,
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if page.RowBytes[rowIndex] != measured {
			t.Fatalf(
				"row %d retained bytes = %d, want %d",
				index,
				page.RowBytes[rowIndex],
				measured,
			)
		}
	}

	replay := adapterRangePageRequest(table, plan, 0, 2)
	replay.ReplayExpected = &issued[0]
	replayed, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plannedRange,
		replay,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Fingerprint != issued[0].Fingerprint ||
		!bytes.Equal(replayed.EndFrontier, issued[0].EndFrontier) ||
		replayed.Exhausted {
		t.Fatalf("replayed page = %#v", replayed)
	}
	replay.ReplayExpected = &NetworkIssuedChunk{
		RangeIndex:    issued[0].RangeIndex,
		Sequence:      issued[0].Sequence,
		Rows:          issued[0].Rows,
		StartFrontier: issued[0].StartFrontier,
		EndFrontier:   issued[0].EndFrontier,
		Fingerprint:   strings.Repeat("f", 64),
		Exhausted:     issued[0].Exhausted,
	}
	if _, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plannedRange,
		replay,
	); err == nil || ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("changed replay error = %v", err)
	}

	// The source cursor and exhaustion probe must both be closed before return.
	if _, err := source.snapshot.ExecContext(
		context.Background(),
		"SELECT 1",
	); err != nil {
		t.Fatalf("source snapshot remained cursor-blocked: %v", err)
	}
}

func TestAdapterNetworkRangePageRejectsRowBeyondMemoryProof(
	t *testing.T,
) {
	t.Parallel()

	source, table := openAdapterRangePageSQLiteFixture(t)
	plan := adapterRangePageTuplePlan(3, 1)
	request := adapterRangePageRequest(table, plan, 0, 1)
	request.Range.MaxRowBytes = 1
	_, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		request,
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("memory proof error = %v", err)
	}
	if strings.Contains(err.Error(), "first-secret") {
		t.Fatalf("memory proof error leaked a row value: %v", err)
	}
}

func TestAdapterNetworkRangePageExactlyFullFinalPageUsesEmptyTerminalRead(
	t *testing.T,
) {
	t.Parallel()

	source, table := openAdapterRangePageSQLiteFixture(t)
	// This models rows deleted after immutable range planning: the approved
	// upper bound remains above the greatest row still present.
	plan := adapterRangePageTuplePlan(9, 9)
	request := adapterRangePageRequest(table, plan, 0, 2)
	first, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Rows) != 2 || first.Exhausted {
		t.Fatalf("first page = %#v", first)
	}
	request.Sequence++
	request.StartFrontier = first.EndFrontier
	second, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 2 || second.Exhausted {
		t.Fatalf("exactly-full final page = %#v", second)
	}
	request.Sequence++
	request.StartFrontier = second.EndFrontier
	terminal, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal.Rows) != 0 ||
		!terminal.Exhausted ||
		terminal.Fingerprint != "" ||
		!bytes.Equal(terminal.EndFrontier, request.StartFrontier) {
		t.Fatalf("terminal page = %#v", terminal)
	}
}

func TestAdapterNetworkRangePageReplaysShortTerminalFactWithoutSecondView(
	t *testing.T,
) {
	t.Parallel()

	source, table := openAdapterRangePageSQLiteFixture(t)
	plan := adapterRangePageTuplePlan(9, 9)
	originalRequest := adapterRangePageRequest(table, plan, 0, 10)
	original, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		originalRequest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(original.Rows) != 4 || !original.Exhausted {
		t.Fatalf("short terminal page = %#v", original)
	}
	replay := adapterRangePageRequest(
		table,
		plan,
		0,
		len(original.Rows),
	)
	replay.ReplayExpected = &NetworkIssuedChunk{
		RangeIndex:    0,
		Sequence:      0,
		Rows:          len(original.Rows),
		StartFrontier: nil,
		EndFrontier:   cloneNetworkBytes(original.EndFrontier),
		Fingerprint:   original.Fingerprint,
		Exhausted:     true,
	}
	replayed, err := source.ReadNetworkRangePage(
		context.Background(),
		table,
		[]string{"tenant", "id", "payload"},
		plan,
		plan.Ranges[0],
		replay,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Exhausted ||
		replayed.Fingerprint != original.Fingerprint ||
		!bytes.Equal(replayed.EndFrontier, original.EndFrontier) {
		t.Fatalf("short terminal replay = %#v", replayed)
	}
}

func adapterRangePagePostgresTable() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", Nullable: false,
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text", Nullable: true},
		},
	}
}

func adapterRangePageIntegerPlan(upperValue int64) PaginationPlan {
	upper := KeyTuple{IntegerKey(upperValue)}
	return PaginationPlan{
		Strategy:     PaginationIntegerKeyset,
		Keys:         []KeySpec{{Name: "id", Kind: KeyInteger}},
		Ranges:       []PaginationRange{{ID: 0, Upper: &upper}},
		TopologyHash: strings.Repeat("a", 64),
	}
}

func adapterRangePageTuplePlan(
	upperFirst int64,
	upperSecond int64,
) PaginationPlan {
	upper := KeyTuple{
		IntegerKey(upperFirst),
		IntegerKey(upperSecond),
	}
	return PaginationPlan{
		Strategy: PaginationTupleKeyset,
		Keys: []KeySpec{
			{Name: "tenant", Kind: KeyInteger},
			{Name: "id", Kind: KeyInteger},
		},
		Ranges:       []PaginationRange{{ID: 0, Upper: &upper}},
		TopologyHash: strings.Repeat("b", 64),
	}
}

func adapterRangePageRequest(
	table schema.Table,
	plan PaginationPlan,
	rangeIndex uint64,
	maxRows int,
) NetworkReadRequest {
	return NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   rangeIndex,
			TableSchema:  table.Schema,
			TableName:    table.Name,
			TopologyHash: "network-topology",
			Pagination:   plan.Strategy,
			MaxRowBytes:  4096,
		},
		MaxRows: maxRows,
	}
}

func openAdapterRangePageSQLiteFixture(
	t *testing.T,
) (*sqliteSourceAdapter, schema.Table) {
	t.Helper()
	database, err := sql.Open(
		"sqlite",
		"file:"+strings.ReplaceAll(t.Name(), "/", "_")+
			"?mode=memory&cache=shared",
	)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = database.Close()
	})
	if _, err := database.Exec(`
		CREATE TABLE events (
			tenant INTEGER NOT NULL,
			id INTEGER NOT NULL,
			payload BLOB,
			PRIMARY KEY (tenant, id)
		) STRICT;
		INSERT INTO events (tenant, id, payload) VALUES
			(-2, 9, X'66697273742D736563726574'),
			(1, 1, X'7365636F6E64'),
			(1, 4, X'7468697264'),
			(3, 1, X'666F75727468');
	`); err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.BeginTx(
		context.Background(),
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = snapshot.Rollback()
	})
	table := schema.Table{
		Name:         "events",
		SQLiteStrict: true,
		Columns: []schema.Column{
			{
				Name: "tenant", Type: "integer", Nullable: false,
				PrimaryKey: true, PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
			{
				Name: "id", Type: "integer", Nullable: false,
				PrimaryKey: true, PrimaryKeyPosition: 2,
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
			{
				Name: "payload", Type: "blob", Nullable: true,
				DeclaredType: &schema.DeclaredType{Base: "blob"},
			},
		},
	}
	return &sqliteSourceAdapter{
		database: database,
		snapshot: snapshot,
	}, table
}

type adapterRangePageRejectingQueryer struct {
	calls int
}

func (queryer *adapterRangePageRejectingQueryer) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	queryer.calls++
	return nil, errors.New("query must not run")
}

func TestAdapterNetworkRangePageCapabilityRejectsUnsupportedSource(
	t *testing.T,
) {
	t.Parallel()
	if _, err := requireAdapterNetworkRangePageSource(
		adapterPaginationUnsupportedSource{},
	); err == nil || ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("unsupported source error = %v", err)
	}
}

func TestAdapterNetworkRangePageRequestBoundUsesConfiguredMaximum(
	t *testing.T,
) {
	t.Parallel()
	table := adapterRangePagePostgresTable()
	plan := adapterRangePageIntegerPlan(10)
	request := adapterRangePageRequest(
		table,
		plan,
		0,
		config.MaxTransferChunkRows+1,
	)
	queryer := &adapterRangePageRejectingQueryer{}
	if _, err := readAdapterNetworkRangePage(
		context.Background(),
		"postgres",
		"public",
		queryer,
		nil,
		table,
		[]string{"id", "payload"},
		plan,
		plan.Ranges[0],
		request,
	); err == nil || ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("oversized request error = %v", err)
	}
	if queryer.calls != 0 {
		t.Fatalf("query calls = %d, want zero", queryer.calls)
	}
}
