package migrate

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizeSQLiteMySQLValuesPreservesExactDomains(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		column schema.Column
		input  any
		want   any
	}{
		{
			name:   "signed integer",
			column: sqliteMySQLTestColumn("v", "bigint", "bigint"),
			input:  int64(math.MinInt64),
			want:   int64(math.MinInt64),
		},
		{
			name:   "finite double",
			column: sqliteMySQLTestColumn("v", "double precision", "double"),
			input:  1.25,
			want:   1.25,
		},
		{
			name: "integral decimal",
			column: sqliteMySQLTestColumn(
				"v",
				"numeric",
				"decimal",
				18,
				0,
			),
			input: "9007199254740993",
			want:  "9007199254740993",
		},
		{
			name: "boolean",
			column: sqliteMySQLTestColumn(
				"v",
				"integer",
				"tinyint",
				1,
			),
			input: int64(1),
			want:  true,
		},
		{
			name: "unicode varchar uses characters",
			column: sqliteMySQLTestColumn(
				"v",
				"varchar",
				"varchar",
				3,
			),
			input: "a😀界",
			want:  "a😀界",
		},
		{
			name:   "long text",
			column: sqliteMySQLTestColumn("v", "text", "longtext"),
			input:  "Zażółć — 東京",
			want:   "Zażółć — 東京",
		},
		{
			name: "bounded binary",
			column: sqliteMySQLTestColumn(
				"v",
				"blob",
				"varbinary",
				4,
			),
			input: []byte{0, 1, 2, 3},
			want:  []byte{0, 1, 2, 3},
		},
		{
			name:   "unbounded binary",
			column: sqliteMySQLTestColumn("v", "blob", "longblob"),
			input:  []byte{},
			want:   []byte{},
		},
		{
			name:   "date",
			column: sqliteMySQLTestColumn("v", "date", "date"),
			input:  "2026-07-30",
			want:   time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "datetime",
			column: sqliteMySQLTestColumn(
				"v",
				"datetime",
				"datetime",
				6,
			),
			input: "2026-07-30 12:34:56.123456",
			want: time.Date(
				2026,
				7,
				30,
				12,
				34,
				56,
				123456000,
				time.UTC,
			),
		},
		{
			name: "uuid",
			column: sqliteMySQLTestColumn(
				"v",
				"uuid",
				"varchar",
				36,
			),
			input: "123e4567-e89b-12d3-a456-426614174000",
			want:  "123e4567-e89b-12d3-a456-426614174000",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			metadata, err := sqliteMySQLValueColumnFromSchema(
				0,
				test.column,
			)
			if err != nil {
				t.Fatal(err)
			}
			got, err := normalizeSQLiteMySQLValue(
				metadata,
				test.input,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("normalized value = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestNormalizeSQLiteMySQLValuesRejectsLossyShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		column schema.Column
		input  any
		want   string
	}{
		{
			name:   "integer storage",
			column: sqliteMySQLTestColumn("v", "bigint", "bigint"),
			input:  1.0,
			want:   "exact SQLite INTEGER",
		},
		{
			name:   "nonfinite double",
			column: sqliteMySQLTestColumn("v", "double precision", "double"),
			input:  math.Inf(1),
			want:   "finite SQLite REAL",
		},
		{
			name: "fractional decimal",
			column: sqliteMySQLTestColumn(
				"v",
				"numeric",
				"decimal",
				18,
				0,
			),
			input: "12.5",
			want:  "exact DECIMAL",
		},
		{
			name: "decimal precision",
			column: sqliteMySQLTestColumn(
				"v",
				"numeric",
				"decimal",
				3,
				0,
			),
			input: "1000",
			want:  "exact DECIMAL",
		},
		{
			name: "boolean domain",
			column: sqliteMySQLTestColumn(
				"v",
				"integer",
				"tinyint",
				1,
			),
			input: int64(2),
			want:  "boolean 0 or 1",
		},
		{
			name: "varchar character length",
			column: sqliteMySQLTestColumn(
				"v",
				"varchar",
				"varchar",
				2,
			),
			input: "a😀界",
			want:  "character length 2",
		},
		{
			name:   "text NUL",
			column: sqliteMySQLTestColumn("v", "text", "longtext"),
			input:  "a\x00b",
			want:   "NUL-free UTF-8",
		},
		{
			name: "binary length",
			column: sqliteMySQLTestColumn(
				"v",
				"blob",
				"varbinary",
				2,
			),
			input: []byte{1, 2, 3},
			want:  "byte length 2",
		},
		{
			name:   "date lower bound",
			column: sqliteMySQLTestColumn("v", "date", "date"),
			input:  "0999-12-31",
			want:   "MySQL DATE",
		},
		{
			name: "datetime spelling",
			column: sqliteMySQLTestColumn(
				"v",
				"datetime",
				"datetime",
				3,
			),
			input: "2026-07-30T12:34:56.123",
			want:  "MySQL DATETIME(3)",
		},
		{
			name: "datetime precision",
			column: sqliteMySQLTestColumn(
				"v",
				"datetime",
				"datetime",
				3,
			),
			input: "2026-07-30 12:34:56.1230",
			want:  "MySQL DATETIME(3)",
		},
		{
			name: "noncanonical uuid",
			column: sqliteMySQLTestColumn(
				"v",
				"uuid",
				"varchar",
				36,
			),
			input: "123E4567-E89B-12D3-A456-426614174000",
			want:  "canonical lowercase UUID",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			metadata, err := sqliteMySQLValueColumnFromSchema(
				0,
				test.column,
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = normalizeSQLiteMySQLValue(metadata, test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestNormalizeSQLiteMySQLBatchOwnsBinaryAndRejectsNull(t *testing.T) {
	t.Parallel()
	table := schema.Table{
		Name: "payloads",
		Columns: []schema.Column{
			sqliteMySQLTestColumn("id", "bigint", "bigint"),
			sqliteMySQLTestColumn("payload", "blob", "longblob"),
		},
	}
	table.Columns[0].Nullable = false
	table.Columns[1].Nullable = false
	sourceBytes := []byte{1, 2, 3}
	normalized, err := normalizeSQLiteMySQLBatch(
		table,
		[]string{"id", "payload"},
		[][]any{{int64(1), sourceBytes}},
	)
	if err != nil {
		t.Fatal(err)
	}
	sourceBytes[0] = 9
	if got := normalized[0][1].([]byte); !reflect.DeepEqual(
		got,
		[]byte{1, 2, 3},
	) {
		t.Fatalf("normalized BLOB aliases source: %#v", got)
	}
	_, err = normalizeSQLiteMySQLBatch(
		table,
		[]string{"id", "payload"},
		[][]any{{int64(1), nil}},
	)
	if err == nil || !strings.Contains(err.Error(), "NULL violates") {
		t.Fatalf("NULL error = %v", err)
	}
}

func TestMySQLTargetWriteBatchNormalizesSQLiteValuesBeforeWriter(
	t *testing.T,
) {
	t.Parallel()
	writer := &mysqlTargetWriterRecorder{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	adapter := &mysqlTargetAdapter{
		batchWriter:                 writer,
		normalizeSQLiteSourceValues: true,
	}
	table := schema.Table{
		Schema: "target_db",
		Name:   "events",
		Columns: []schema.Column{
			sqliteMySQLTestColumn("id", "bigint", "bigint"),
			sqliteMySQLTestColumn("enabled", "integer", "tinyint", 1),
			sqliteMySQLTestColumn("payload", "blob", "longblob"),
		},
	}
	for index := range table.Columns {
		table.Columns[index].Nullable = false
	}
	payload := []byte{1, 2, 3}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "enabled", "payload"},
		"drop_recreate",
		[][]any{{int64(1), int64(1), payload}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != writer.receipt || writer.calls != 1 {
		t.Fatalf("receipt/writer = %#v / %#v", receipt, writer)
	}
	payload[0] = 9
	if got := writer.rows[0]; got[1] != true ||
		!reflect.DeepEqual(got[2], []byte{1, 2, 3}) {
		t.Fatalf("writer values = %#v", got)
	}
}

func TestMySQLTargetPreflightSelectsSQLiteValueContract(t *testing.T) {
	t.Parallel()
	adapter := &mysqlTargetAdapter{
		validateSQLServerSourceValues: true,
	}
	source := &sqliteSQLServerValueFixtureSource{}
	if err := adapter.PreflightSourceData(
		context.Background(),
		source,
		nil,
		"drop_recreate",
	); err != nil {
		t.Fatal(err)
	}
	if !adapter.normalizeSQLiteSourceValues ||
		adapter.validateSQLServerSourceValues {
		t.Fatalf(
			"SQLite value flags = normalize:%t sqlserver:%t",
			adapter.normalizeSQLiteSourceValues,
			adapter.validateSQLServerSourceValues,
		)
	}
}

func TestPlanSQLiteMySQLSourceProbesQuotesCatalogNames(t *testing.T) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression(`"amount" >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	plans := []adapterTablePlan{{
		source: schema.Table{
			Name: `orders'quoted`,
			Columns: []schema.Column{
				sqliteMySQLTestColumn(
					"amount",
					"numeric",
					"decimal",
					18,
					0,
				),
				sqliteMySQLTestColumn(
					"occurred",
					"datetime",
					"datetime",
					6,
				),
			},
			Indexes: []schema.Index{{
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name:      "amount",
					Collation: "BINARY",
				}},
			}},
			Checks: []schema.CheckConstraint{{
				Expression: check,
			}},
			ForeignKeys: []schema.ForeignKey{{
				Columns:         []string{"amount"},
				ReferencedTable: "parents",
			}},
		},
	}}
	probes, err := planSQLiteMySQLSourceProbes(plans)
	if err != nil {
		t.Fatal(err)
	}
	if len(probes) != 5 {
		t.Fatalf("probe count = %d, want 5: %#v", len(probes), probes)
	}
	for _, probe := range probes {
		if strings.Contains(probe.query, `orders'quoted`) &&
			!strings.Contains(probe.query, `"orders'quoted"`) &&
			!strings.Contains(probe.query, `'orders''quoted'`) {
			t.Fatalf("unquoted source identifier/literal: %s", probe.query)
		}
	}
}

func TestRunSQLiteMySQLSourceProbesFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind sqliteMySQLSourceProbeKind
		want string
	}{
		{sqliteMySQLSourceProbeNumericStorage, "non-INTEGER"},
		{sqliteMySQLSourceProbeTemporalStorage, "non-TEXT"},
		{sqliteMySQLSourceProbeCheck, "historical rows"},
		{sqliteMySQLSourceProbeForeignKey, "orphan rows"},
		{sqliteMySQLSourceProbeUnique, "duplicate"},
	}
	for _, test := range tests {
		runner := &sqliteMySQLProbeRunnerStub{invalid: true}
		err := runSQLiteMySQLSourceProbes(
			context.Background(),
			runner,
			[]sqliteMySQLSourceProbe{{
				kind:   test.kind,
				table:  "events",
				object: "constraint",
				query:  "SELECT TRUE",
			}},
		)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("kind %d error = %v, want %q", test.kind, err, test.want)
		}
	}
}

func TestSQLiteMySQLValueColumnsRejectsUnexpectedPlan(t *testing.T) {
	t.Parallel()
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteMySQLTestColumn("payload", "json", "json"),
		},
	}
	_, err := sqliteMySQLValueColumns(table, []string{"payload"})
	if err == nil || !strings.Contains(err.Error(), "no exact") {
		t.Fatalf("unexpected-plan error = %v", err)
	}
}

type sqliteMySQLProbeRunnerStub struct {
	invalid bool
	err     error
	queries []string
}

func (runner *sqliteMySQLProbeRunnerStub) hasInvalidSQLiteMySQLSourceRow(
	_ context.Context,
	query string,
) (bool, error) {
	runner.queries = append(runner.queries, query)
	return runner.invalid, runner.err
}

func sqliteMySQLTestColumn(
	name string,
	semantic string,
	base string,
	arguments ...int,
) schema.Column {
	return schema.Column{
		Name:     name,
		Type:     semantic,
		Nullable: true,
		DeclaredType: &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		},
	}
}
