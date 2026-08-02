package migrate

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestAdapterPaginationStrategyIsConservative(t *testing.T) {
	t.Parallel()

	primary := func(
		name string,
		typ string,
		position int,
		declaration *schema.DeclaredType,
	) schema.Column {
		return schema.Column{
			Name:               name,
			Type:               typ,
			PrimaryKey:         true,
			PrimaryKeyPosition: position,
			DeclaredType:       declaration,
		}
	}
	tests := []struct {
		name     string
		engine   string
		table    schema.Table
		strategy PaginationStrategy
		kinds    []KeyKind
	}{
		{
			name:   "PostgreSQL signed bigint",
			engine: "postgres",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					primary("id", "bigint", 1, nil),
				},
			},
			strategy: PaginationIntegerKeyset,
			kinds:    []KeyKind{KeyInteger},
		},
		{
			name:   "PostgreSQL signed composite",
			engine: "postgres",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					primary("tenant", "bigint", 1, nil),
					primary("id", "integer", 2, nil),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyInteger, KeyInteger},
		},
		{
			name:   "PostgreSQL bytea scalar",
			engine: "postgres",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					primary("digest", "bytea", 1, nil),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyBytes},
		},
		{
			name:   "MySQL signed bigint",
			engine: "mysql",
			table: schema.Table{
				Schema: "source",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"id",
						"bigint",
						1,
						&schema.DeclaredType{Base: "bigint"},
					),
				},
			},
			strategy: PaginationIntegerKeyset,
			kinds:    []KeyKind{KeyInteger},
		},
		{
			name:   "MySQL unsigned falls back",
			engine: "mysql",
			table: schema.Table{
				Schema: "source",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"id",
						"bigint",
						1,
						&schema.DeclaredType{
							Base: "bigint",
							MySQL: &schema.MySQLTypeMetadata{
								Unsigned: true,
							},
						},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{""},
		},
		{
			name:   "MySQL tinyint one falls back",
			engine: "mysql",
			table: schema.Table{
				Schema: "source",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"id",
						"integer",
						1,
						&schema.DeclaredType{
							Base:      "tinyint",
							Arguments: []int{1},
						},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{""},
		},
		{
			name:   "MariaDB signed composite uses MySQL proof",
			engine: "mysql",
			table: schema.Table{
				Schema: "source",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "int"},
					),
					primary(
						"id",
						"bigint",
						2,
						&schema.DeclaredType{Base: "bigint"},
					),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyInteger, KeyInteger},
		},
		{
			name:   "SQL Server signed bigint",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"id",
						"bigint",
						1,
						&schema.DeclaredType{Base: "bigint"},
					),
				},
			},
			strategy: PaginationIntegerKeyset,
			kinds:    []KeyKind{KeyInteger},
		},
		{
			name:   "SQL Server tinyint is not signed",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"id",
						"integer",
						1,
						&schema.DeclaredType{Base: "tinyint"},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{""},
		},
		{
			name:   "SQL Server signed composite uses tuple keyset",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "int"},
					),
					primary(
						"id",
						"bigint",
						2,
						&schema.DeclaredType{Base: "bigint"},
					),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyInteger, KeyInteger},
		},
		{
			name:   "SQL Server varbinary composite uses ROW_NUMBER",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "int"},
					),
					primary(
						"digest",
						"blob",
						2,
						&schema.DeclaredType{
							Base:      "varbinary",
							Arguments: []int{16},
						},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{KeyInteger, KeyBytes},
		},
		{
			name:   "SQL Server fixed binary composite uses tuple keyset",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "int"},
					),
					primary(
						"digest",
						"blob",
						2,
						&schema.DeclaredType{
							Base:      "binary",
							Arguments: []int{16},
						},
					),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyInteger, KeyBytes},
		},
		{
			name:   "converter touched temporal key uses ROW_NUMBER",
			engine: "mssql",
			table: schema.Table{
				Schema: "dbo",
				Name:   "events",
				Columns: []schema.Column{
					primary(
						"created_at",
						"datetime",
						1,
						&schema.DeclaredType{
							Base:      "datetime2",
							Arguments: []int{6},
						},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{""},
		},
		{
			name:   "text collation key uses ROW_NUMBER",
			engine: "mysql",
			table: schema.Table{
				Schema:         "source",
				Name:           "events",
				MySQLCollation: "utf8mb4_0900_bin",
				Columns: []schema.Column{
					primary(
						"code",
						"varchar",
						1,
						&schema.DeclaredType{
							Base:      "varchar",
							Arguments: []int{32},
						},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{KeyText},
		},
		{
			name:   "SQLite rowid integer",
			engine: "sqlite",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{
					primary(
						"id",
						"integer",
						1,
						&schema.DeclaredType{Base: "integer"},
					),
				},
			},
			strategy: PaginationIntegerKeyset,
			kinds:    []KeyKind{KeyInteger},
		},
		{
			name:   "SQLite strict signed composite",
			engine: "sqlite",
			table: schema.Table{
				Name:         "events",
				SQLiteStrict: true,
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "integer"},
					),
					primary(
						"id",
						"integer",
						2,
						&schema.DeclaredType{Base: "integer"},
					),
				},
			},
			strategy: PaginationTupleKeyset,
			kinds:    []KeyKind{KeyInteger, KeyInteger},
		},
		{
			name:   "SQLite dynamic composite uses ROW_NUMBER",
			engine: "sqlite",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{
					primary(
						"tenant",
						"integer",
						1,
						&schema.DeclaredType{Base: "integer"},
					),
					primary(
						"id",
						"integer",
						2,
						&schema.DeclaredType{Base: "integer"},
					),
				},
			},
			strategy: PaginationRowNumber,
			kinds:    []KeyKind{KeyInteger, KeyInteger},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			namespace := test.table.Schema
			keys, err := adapterPaginationPrimaryKey(
				test.engine,
				namespace,
				test.table,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := adapterPaginationStrategy(
				test.engine,
				test.table,
				keys,
			); got != test.strategy {
				t.Fatalf("strategy = %q, want %q", got, test.strategy)
			}
			kinds := make([]KeyKind, len(keys))
			for index, key := range keys {
				kinds[index] = adapterPaginationKeyKind(
					test.engine,
					key,
				)
			}
			if !reflect.DeepEqual(kinds, test.kinds) {
				t.Fatalf("key kinds = %v, want %v", kinds, test.kinds)
			}
		})
	}
}

func TestAdapterPaginationCatalogFailsClosed(t *testing.T) {
	t.Parallel()

	base := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	tests := []struct {
		name   string
		mutate func(*schema.Table)
		class  TransferErrorClass
	}{
		{
			name: "no primary key",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKey = false
				table.Columns[0].PrimaryKeyPosition = 0
			},
			class: ErrorClassPrimaryKey,
		},
		{
			name: "nullable primary key",
			mutate: func(table *schema.Table) {
				table.Columns[0].Nullable = true
			},
			class: ErrorClassPrimaryKey,
		},
		{
			name: "contradictory primary key flag",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKey = false
			},
			class: ErrorClassPolicy,
		},
		{
			name: "negative non-key position",
			mutate: func(table *schema.Table) {
				table.Columns = append(
					table.Columns,
					schema.Column{
						Name:               "payload",
						Type:               "text",
						PrimaryKeyPosition: -1,
					},
				)
			},
			class: ErrorClassPolicy,
		},
		{
			name: "primary key position gap",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKeyPosition = 2
			},
			class: ErrorClassPolicy,
		},
		{
			name: "duplicate column",
			mutate: func(table *schema.Table) {
				table.Columns = append(
					table.Columns,
					table.Columns[0],
				)
			},
			class: ErrorClassPolicy,
		},
		{
			name: "schema mismatch",
			mutate: func(table *schema.Table) {
				table.Schema = "other"
			},
			class: ErrorClassPolicy,
		},
		{
			name: "invalid schema identifier",
			mutate: func(table *schema.Table) {
				table.Schema = "public\x00"
			},
			class: ErrorClassPolicy,
		},
		{
			name: "invalid identifier",
			mutate: func(table *schema.Table) {
				table.Columns[0].Name = "bad\x00name"
			},
			class: ErrorClassPolicy,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := base
			table.Columns = append([]schema.Column(nil), base.Columns...)
			test.mutate(&table)
			_, err := adapterPaginationPrimaryKey(
				"postgres",
				"public",
				table,
			)
			if err == nil ||
				ClassifyTransferError(err) != test.class {
				t.Fatalf(
					"error = %v, class = %q, want %q",
					err,
					ClassifyTransferError(err),
					test.class,
				)
			}
		})
	}
}

func TestAdapterPaginationRowNumberSplitAvoidsOverflow(t *testing.T) {
	t.Parallel()

	ranges := splitAdapterPaginationRowNumberRange(
		math.MaxInt64,
		7,
	)
	if len(ranges) != 7 ||
		ranges[0].FirstRow != 1 ||
		ranges[len(ranges)-1].LastRow != math.MaxInt64 {
		t.Fatalf("ranges = %#v", ranges)
	}
	for index := 1; index < len(ranges); index++ {
		if ranges[index].FirstRow !=
			ranges[index-1].LastRow+1 {
			t.Fatalf(
				"range %d starts at %d after %d",
				index,
				ranges[index].FirstRow,
				ranges[index-1].LastRow,
			)
		}
	}
}

func TestAdapterPaginationIntegerDriverValuesNeverUseFloat(t *testing.T) {
	t.Parallel()

	const aboveFloatExactness = int64(9_007_199_254_740_993)
	for _, value := range []any{
		aboveFloatExactness,
		"9007199254740993",
		[]byte("9007199254740993"),
	} {
		got, err := adapterPaginationInt64(value)
		if err != nil || got != aboveFloatExactness {
			t.Fatalf("value %#v = %d, %v", value, got, err)
		}
	}
	for _, value := range []any{
		float64(aboveFloatExactness),
		uint64(math.MaxInt64) + 1,
		"9007199254740993.0",
	} {
		if _, err := adapterPaginationInt64(value); err == nil {
			t.Fatalf("unsafe integer boundary %#v was admitted", value)
		}
	}
}

func TestAdapterPaginationTupleQueriesQuoteCompleteKey(t *testing.T) {
	t.Parallel()

	keys := []schema.Column{
		{Name: " tenant ", PrimaryKey: true, PrimaryKeyPosition: 1},
		{
			Name:               "dmtx_pagination_bucket",
			PrimaryKey:         true,
			PrimaryKeyPosition: 2,
		},
	}
	tests := []struct {
		engine      string
		namespace   string
		want        []string
		notExpected string
	}{
		{
			engine:    "postgres",
			namespace: " source ",
			want: []string{
				`" source "." events "`,
				`NTILE($1)`,
				`" tenant " ASC`,
				`"dmtx_pagination_bucket" ASC`,
				`"dmtx_pagination_bucket_1"`,
			},
		},
		{
			engine:    "mysql",
			namespace: " source ",
			want: []string{
				"` source `.` events `",
				"NTILE(?)",
				"` tenant ` ASC",
				"`dmtx_pagination_bucket` ASC",
				"`dmtx_pagination_bucket_1`",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.engine, func(t *testing.T) {
			t.Parallel()
			query, err := adapterPaginationTupleBoundaryQuery(
				test.engine,
				test.namespace,
				" events ",
				keys,
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				if !strings.Contains(query, want) {
					t.Fatalf("query %q lacks %q", query, want)
				}
			}
		})
	}
}

func TestAdapterPaginationAliasRemainsBoundedUnderCollisions(t *testing.T) {
	t.Parallel()

	const base = "dmtx_pagination_boundary_rank"
	columns := []string{base}
	for index := 1; index <= 100; index++ {
		columns = append(
			columns,
			base+"_"+strconv.Itoa(index),
		)
	}
	alias := adapterPaginationAlias(columns, base)
	if alias != base+"_101" || len(alias) > 63 {
		t.Fatalf("alias = %q", alias)
	}
	for _, column := range columns {
		if strings.EqualFold(column, alias) {
			t.Fatalf("alias %q collides with column %q", alias, column)
		}
	}
}

func TestAdapterPaginationTopologyIsStableAndScoped(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Schema: "public",
		Name:   "events",
	}
	evidence := []adapterPaginationKeyEvidence{{
		Name:     "id",
		Type:     "bigint",
		Position: 1,
	}}
	plan := PaginationPlan{
		Strategy: PaginationIntegerKeyset,
		Keys:     []KeySpec{{Name: "id", Kind: KeyInteger}},
		Ranges: []PaginationRange{{
			ID: 0,
			Upper: func() *KeyTuple {
				value := KeyTuple{IntegerKey(9_007_199_254_740_993)}
				return &value
			}(),
		}},
	}
	first, err := adapterPaginationTopologyHash(
		"postgres",
		table,
		2,
		evidence,
		plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapterPaginationTopologyHash(
		"postgres",
		table,
		2,
		evidence,
		plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	otherEngine, err := adapterPaginationTopologyHash(
		"mysql",
		table,
		2,
		evidence,
		plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	otherPartitions, err := adapterPaginationTopologyHash(
		"postgres",
		table,
		3,
		evidence,
		plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 64 ||
		first != second ||
		first == otherEngine ||
		first == otherPartitions {
		t.Fatalf(
			"hashes stable=%q/%q engine=%q partitions=%q",
			first,
			second,
			otherEngine,
			otherPartitions,
		)
	}
}

func TestSQLiteSourceAdapterPlansPaginationLive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(`
		CREATE TABLE integer_ids (
			id INTEGER PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO integer_ids VALUES
			(9007199254740993, 'one'),
			(9007199254740994, 'two'),
			(9007199254741000, 'three');

		CREATE TABLE strict_pairs (
			tenant INTEGER NOT NULL,
			id INTEGER NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, id)
		) STRICT;
		INSERT INTO strict_pairs VALUES
			(1, 1, 'one'),
			(1, 2, 'two'),
			(2, 1, 'three'),
			(2, 2, 'four'),
			(3, 1, 'five');

		CREATE TABLE text_ids (
			code TEXT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO text_ids VALUES
			('a', 'one'),
			('b', 'two'),
			('c', 'three');

		CREATE TABLE binary_ids (
			digest BLOB NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		) STRICT;
		INSERT INTO binary_ids VALUES
			(X'00FF', 'one'),
			(X'0100', 'two'),
			(X'FF00', 'three');

		CREATE TABLE empty_text_ids (
			code TEXT NOT NULL PRIMARY KEY
		);
	`); err != nil {
		_ = setup.Close()
		t.Fatal(err)
	}
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}

	source, err := openSQLiteSourceAdapter(
		context.Background(),
		config.Endpoint{Type: "sqlite", Database: path},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQLite pagination source: %v", err)
		}
	})
	planner, err := requirePaginationSourceAdapter(source)
	if err != nil {
		t.Fatal(err)
	}

	integerTable, err := source.InspectTable(
		context.Background(),
		"integer_ids",
	)
	if err != nil {
		t.Fatal(err)
	}
	integerPlan, err := planner.PlanPagination(
		context.Background(),
		integerTable,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	if integerPlan.Strategy != PaginationIntegerKeyset ||
		len(integerPlan.Ranges) != 3 {
		t.Fatalf("integer plan = %#v", integerPlan)
	}
	lastInteger, err := (*integerPlan.Ranges[2].Upper)[0].SQLValue()
	if err != nil ||
		lastInteger != int64(9_007_199_254_741_000) {
		t.Fatalf(
			"last integer bound = %#v, %v",
			lastInteger,
			err,
		)
	}

	tupleTable, err := source.InspectTable(
		context.Background(),
		"strict_pairs",
	)
	if err != nil {
		t.Fatal(err)
	}
	firstTuplePlan, err := planner.PlanPagination(
		context.Background(),
		tupleTable,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondTuplePlan, err := planner.PlanPagination(
		context.Background(),
		tupleTable,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstTuplePlan.Strategy != PaginationTupleKeyset ||
		len(firstTuplePlan.Ranges) != 3 ||
		firstTuplePlan.TopologyHash != secondTuplePlan.TopologyHash {
		t.Fatalf(
			"tuple plans = %#v / %#v",
			firstTuplePlan,
			secondTuplePlan,
		)
	}
	wantTupleBounds := [][]int64{{1, 2}, {2, 2}, {3, 1}}
	for rangeIndex, want := range wantTupleBounds {
		upper := *firstTuplePlan.Ranges[rangeIndex].Upper
		for keyIndex, wantValue := range want {
			got, err := upper[keyIndex].SQLValue()
			if err != nil || got != wantValue {
				t.Fatalf(
					"tuple range %d key %d = %#v, %v; want %d",
					rangeIndex,
					keyIndex,
					got,
					err,
					wantValue,
				)
			}
		}
	}

	textTable, err := source.InspectTable(
		context.Background(),
		"text_ids",
	)
	if err != nil {
		t.Fatal(err)
	}
	textPlan, err := planner.PlanPagination(
		context.Background(),
		textTable,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if textPlan.Strategy != PaginationRowNumber ||
		len(textPlan.Ranges) != 2 ||
		textPlan.Ranges[0].FirstRow != 1 ||
		textPlan.Ranges[0].LastRow != 2 ||
		textPlan.Ranges[1].FirstRow != 3 ||
		textPlan.Ranges[1].LastRow != 3 ||
		!reflect.DeepEqual(
			textPlan.Keys,
			[]KeySpec{{Name: "code", Kind: KeyText}},
		) {
		t.Fatalf("text plan = %#v", textPlan)
	}

	binaryTable, err := source.InspectTable(
		context.Background(),
		"binary_ids",
	)
	if err != nil {
		t.Fatal(err)
	}
	binaryPlan, err := planner.PlanPagination(
		context.Background(),
		binaryTable,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if binaryPlan.Strategy != PaginationTupleKeyset ||
		len(binaryPlan.Ranges) != 2 ||
		len(binaryPlan.Keys) != 1 ||
		binaryPlan.Keys[0].Kind != KeyBytes {
		t.Fatalf("binary plan = %#v", binaryPlan)
	}
	binaryUpper, err := (*binaryPlan.Ranges[1].Upper)[0].SQLValue()
	if err != nil ||
		!reflect.DeepEqual(binaryUpper, []byte{0xff, 0x00}) {
		t.Fatalf("binary upper = %#v, %v", binaryUpper, err)
	}

	emptyTable, err := source.InspectTable(
		context.Background(),
		"empty_text_ids",
	)
	if err != nil {
		t.Fatal(err)
	}
	emptyPlan, err := planner.PlanPagination(
		context.Background(),
		emptyTable,
		4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if emptyPlan.Strategy != PaginationRowNumber ||
		len(emptyPlan.Ranges) != 1 ||
		!emptyPlan.Ranges[0].Empty ||
		emptyPlan.Ranges[0].ID != 0 ||
		emptyPlan.Ranges[0].FirstRow != 1 {
		t.Fatalf("empty plan = %#v", emptyPlan)
	}
}

func TestRequirePaginationSourceAdapterFailsClosed(t *testing.T) {
	t.Parallel()

	for _, source := range []sourceAdapter{
		nil,
		&clickHouseSourceAdapter{},
		adapterPaginationUnsupportedSource{},
	} {
		if _, err := requirePaginationSourceAdapter(source); err == nil ||
			ClassifyTransferError(err) != ErrorClassPolicy {
			t.Fatalf("source %#v error = %v", source, err)
		}
	}
}

type adapterPaginationUnsupportedSource struct{}

func (adapterPaginationUnsupportedSource) Engine() string { return "postgres" }
func (adapterPaginationUnsupportedSource) DisplayName() string {
	return "unsupported"
}
func (adapterPaginationUnsupportedSource) ListTables(
	context.Context,
) ([]string, error) {
	return nil, errors.New("not implemented")
}
func (adapterPaginationUnsupportedSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return schema.Table{}, errors.New("not implemented")
}
func (adapterPaginationUnsupportedSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, errors.New("not implemented")
}
func (adapterPaginationUnsupportedSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, errors.New("not implemented")
}
func (adapterPaginationUnsupportedSource) Close() error { return nil }
