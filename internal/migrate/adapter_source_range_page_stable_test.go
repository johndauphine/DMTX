package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestSQLiteStableNetworkRangePagesCoverRowNumberAndBinaryKeys(
	t *testing.T,
) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "stable-source.db")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close SQLite stable setup: %v", err)
		}
	}()
	if _, err := setup.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE text_ids (
			code TEXT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		) STRICT;
		INSERT INTO text_ids VALUES
			('a', 'one'),
			('c', 'three'),
			('e', 'five');

		CREATE TABLE binary_ids (
			digest BLOB NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		) STRICT;
		INSERT INTO binary_ids VALUES
			(X'00FF', 'one'),
			(X'0100', 'two'),
			(X'FF00', 'three');
	`); err != nil {
		t.Fatal(err)
	}

	raw, err := openSQLiteSourceAdapter(
		ctx,
		config.Endpoint{Type: "sqlite", Database: path},
	)
	if err != nil {
		t.Fatal(err)
	}
	source := raw.(*sqliteSourceAdapter)
	defer func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQLite stable source: %v", err)
		}
	}()

	t.Run("ROW_NUMBER remains tied to opening snapshot", func(t *testing.T) {
		table, err := source.InspectTable(ctx, "text_ids")
		if err != nil {
			t.Fatal(err)
		}
		session, err := OpenAdapterStableNetworkTableSource(
			ctx,
			source,
			table,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := session.Close(); err != nil {
				t.Errorf("close borrowed SQLite stable session: %v", err)
			}
		}()
		stable, err := session.Source()
		if err != nil {
			t.Fatal(err)
		}
		if session.ReaderLimit() != 1 {
			t.Fatalf(
				"SQLite stable reader limit = %d, want 1",
				session.ReaderLimit(),
			)
		}
		if _, ok := any(stable).(adapterNetworkStableRangePageSource); !ok {
			t.Fatal("SQLite stable source lacks admission marker")
		}
		columns := adapterColumnNames(table)
		plan, err := stable.PlanPagination(ctx, table, 2)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Strategy != PaginationRowNumber ||
			len(plan.Ranges) != 2 ||
			plan.Ranges[0].FirstRow != 1 ||
			plan.Ranges[0].LastRow != 2 {
			t.Fatalf("ROW_NUMBER plan = %#v", plan)
		}
		width, err := stable.PlanRetainedRowWidth(ctx, table, columns)
		if err != nil {
			t.Fatal(err)
		}

		// This row sorts inside the first interval. A mutable re-numbering
		// would change the second page from c to b; the opening SQLite
		// snapshot must continue to return the original a,c interval.
		if _, err := setup.ExecContext(
			ctx,
			`INSERT INTO text_ids VALUES ('b', 'two')`,
		); err != nil {
			t.Fatalf("mutate WAL source after stable planning: %v", err)
		}

		request := stableRangePageRequest(
			table.Schema,
			table.Name,
			plan,
			0,
			width.UpperBoundBytes,
			1,
		)
		first, err := stable.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[0],
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Rows) != 1 ||
			first.Rows[0][0] != "a" ||
			first.Exhausted {
			t.Fatalf("first ROW_NUMBER page = %#v", first)
		}
		assertOrdinalFrontier(t, first.EndFrontier, 1)

		replayRequest := request
		replayRequest.ReplayExpected = &NetworkIssuedChunk{
			RangeIndex:    0,
			Sequence:      0,
			Rows:          1,
			StartFrontier: nil,
			EndFrontier:   cloneNetworkBytes(first.EndFrontier),
			Fingerprint:   first.Fingerprint,
			Exhausted:     false,
		}
		replayed, err := stable.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[0],
			replayRequest,
		)
		if err != nil {
			t.Fatal(err)
		}
		if replayed.Fingerprint != first.Fingerprint ||
			!bytes.Equal(replayed.EndFrontier, first.EndFrontier) {
			t.Fatalf("ROW_NUMBER replay = %#v, want %#v", replayed, first)
		}

		secondRequest := request
		secondRequest.Sequence = 1
		secondRequest.StartFrontier = cloneNetworkBytes(first.EndFrontier)
		second, err := stable.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[0],
			secondRequest,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Rows) != 1 ||
			second.Rows[0][0] != "c" ||
			!second.Exhausted {
			t.Fatalf("second ROW_NUMBER page = %#v", second)
		}
		assertOrdinalFrontier(t, second.EndFrontier, 2)

		lastRequest := stableRangePageRequest(
			table.Schema,
			table.Name,
			plan,
			1,
			width.UpperBoundBytes,
			2,
		)
		last, err := stable.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[1],
			lastRequest,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(last.Rows) != 1 ||
			last.Rows[0][0] != "e" ||
			!last.Exhausted {
			t.Fatalf("last ROW_NUMBER page = %#v", last)
		}
		assertOrdinalFrontier(t, last.EndFrontier, 3)
	})

	t.Run("binary scalar keyset has typed replay frontier", func(t *testing.T) {
		table, err := source.InspectTable(ctx, "binary_ids")
		if err != nil {
			t.Fatal(err)
		}
		columns := adapterColumnNames(table)
		plan, err := source.PlanPagination(ctx, table, 1)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Strategy != PaginationTupleKeyset ||
			len(plan.Keys) != 1 ||
			plan.Keys[0].Kind != KeyBytes {
			t.Fatalf("binary key plan = %#v", plan)
		}
		width, err := source.PlanRetainedRowWidth(ctx, table, columns)
		if err != nil {
			t.Fatal(err)
		}
		request := stableRangePageRequest(
			table.Schema,
			table.Name,
			plan,
			0,
			width.UpperBoundBytes,
			2,
		)
		first, err := source.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[0],
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Rows) != 2 || first.Exhausted {
			t.Fatalf("first binary page = %#v", first)
		}
		frontier, valid, err := decodeNetworkStateFrontier(
			first.EndFrontier,
		)
		if err != nil || !valid ||
			len(frontier) != 1 ||
			frontier[0].Kind != state.ValueBytes {
			t.Fatalf(
				"binary frontier = %#v, valid=%v, err=%v",
				frontier,
				valid,
				err,
			)
		}
		decoded, err := frontier[0].SQLValue()
		if err != nil || !bytes.Equal(decoded.([]byte), []byte{0x01, 0x00}) {
			t.Fatalf("binary frontier value = %#v, %v", decoded, err)
		}

		secondRequest := request
		secondRequest.Sequence = 1
		secondRequest.StartFrontier = cloneNetworkBytes(first.EndFrontier)
		second, err := source.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			plan,
			plan.Ranges[0],
			secondRequest,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Rows) != 1 ||
			!bytes.Equal(second.Rows[0][0].([]byte), []byte{0xff, 0x00}) ||
			!second.Exhausted {
			t.Fatalf("second binary page = %#v", second)
		}
	})
}

func stableRangePageRequest(
	tableSchema string,
	tableName string,
	plan PaginationPlan,
	rangeIndex uint64,
	maxRowBytes int64,
	maxRows int,
) NetworkReadRequest {
	return NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   rangeIndex,
			TableSchema:  tableSchema,
			TableName:    tableName,
			TopologyHash: "stable-network-topology",
			Pagination:   plan.Strategy,
			MaxRowBytes:  maxRowBytes,
		},
		MaxRows: maxRows,
	}
}

func assertOrdinalFrontier(
	t *testing.T,
	frontier []byte,
	want int64,
) {
	t.Helper()
	value, valid, err := adapterRangePageOrdinalFrontier(frontier)
	if err != nil || !valid || value != want {
		t.Fatalf(
			"ROW_NUMBER frontier = %d, valid=%v, err=%v; want %d",
			value,
			valid,
			err,
			want,
		)
	}
}
