package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestSplitIntegerRangeCoversSignedExtremesWithoutOverlap(t *testing.T) {
	ranges := SplitIntegerRange(math.MinInt64, math.MaxInt64, 7)
	if len(ranges) != 7 {
		t.Fatalf("ranges = %d", len(ranges))
	}
	if got, _ := (*ranges[0].Upper)[0].SQLValue(); got == nil {
		t.Fatal("missing first upper bound")
	}
	firstLower := ranges[0].Lower
	if firstLower != nil {
		t.Fatalf("first lower = %#v", firstLower)
	}
	lastUpper, err := (*ranges[len(ranges)-1].Upper)[0].SQLValue()
	if err != nil || lastUpper != int64(math.MaxInt64) {
		t.Fatalf("last upper = %#v, err = %v", lastUpper, err)
	}
	for index := 1; index < len(ranges); index++ {
		previous, _ := (*ranges[index-1].Upper)[0].SQLValue()
		lower, _ := (*ranges[index].Lower)[0].SQLValue()
		if previous != lower {
			t.Fatalf("range %d lower %v does not equal prior inclusive upper %v", index, lower, previous)
		}
	}
}

func TestSplitIntegerRangeCapsPartitionsAtCardinality(t *testing.T) {
	ranges := SplitIntegerRange(-1, 1, 10)
	if len(ranges) != 3 {
		t.Fatalf("ranges = %#v", ranges)
	}
	for index, want := range []int64{-1, 0, 1} {
		got, _ := (*ranges[index].Upper)[0].SQLValue()
		if got != want {
			t.Fatalf("range %d upper = %v, want %d", index, got, want)
		}
	}
}

func TestKeyValueRoundTripAboveTwoToTheFiftyThird(t *testing.T) {
	want := int64(9_007_199_254_740_993)
	encoded, err := json.Marshal(IntegerKey(want))
	if err != nil {
		t.Fatal(err)
	}
	var restored KeyValue
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	got, err := restored.SQLValue()
	if err != nil || got != want {
		t.Fatalf("got %#v, err = %v, JSON = %s", got, err, encoded)
	}
}

func TestSplitRowNumberRangeCoversExactlyOnce(t *testing.T) {
	ranges := SplitRowNumberRange(11, 4)
	var got []int64
	for _, work := range ranges {
		for row := work.FirstRow; row <= work.LastRow; row++ {
			got = append(got, row)
		}
	}
	want := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v", got)
	}
}

func TestPlanSQLitePaginationSelectsTupleAndStableTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE items (tenant INTEGER NOT NULL, code TEXT NOT NULL, value TEXT, PRIMARY KEY (tenant, code));
		INSERT INTO items VALUES
			(9007199254740993, 'a', 'one'),
			(9007199254740993, 'b', 'two'),
			(9007199254740994, 'a', 'three'),
			(9007199254740995, 'a', 'four');
	`); err != nil {
		t.Fatal(err)
	}
	table, _, err := inspectTable(context.Background(), database, "items")
	if err != nil {
		t.Fatal(err)
	}
	first, err := PlanSQLitePagination(context.Background(), database, table, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanSQLitePagination(context.Background(), database, table, 3)
	if err != nil {
		t.Fatal(err)
	}
	if first.Strategy != PaginationTupleKeyset || len(first.Ranges) != 3 {
		t.Fatalf("plan = %#v", first)
	}
	if first.TopologyHash == "" || first.TopologyHash != second.TopologyHash {
		t.Fatalf("topology hashes = %q, %q", first.TopologyHash, second.TopologyHash)
	}
	value, err := (*first.Ranges[0].Upper)[0].SQLValue()
	if err != nil {
		t.Fatal(err)
	}
	if value != int64(9_007_199_254_740_993) {
		t.Fatalf("large integer boundary = %#v", value)
	}
}

func TestPlanSQLitePaginationTopologyIncludesRequestedPartitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE empty_items (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	table, _, err := inspectTable(context.Background(), database, "empty_items")
	if err != nil {
		t.Fatal(err)
	}
	one, err := PlanSQLitePagination(context.Background(), database, table, 1)
	if err != nil {
		t.Fatal(err)
	}
	four, err := PlanSQLitePagination(context.Background(), database, table, 4)
	if err != nil {
		t.Fatal(err)
	}
	if one.TopologyHash == four.TopologyHash {
		t.Fatalf("partition-policy change reused topology hash %q", one.TopologyHash)
	}
}

func TestSQLiteUnsafeTupleFallsBackToRowNumber(t *testing.T) {
	table := schema.Table{
		Name: "unsafe",
		Columns: []schema.Column{
			{Name: "id", Type: "text", Nullable: true, PrimaryKey: true, PrimaryKeyPosition: 1, DeclaredType: &schema.DeclaredType{Base: "text"}},
		},
	}
	keys := sqliteKeySpecs(table)
	if got := sqlitePaginationStrategy(table, keys); got != PaginationRowNumber {
		t.Fatalf("strategy = %s", got)
	}
}
