package migrate

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizeSQLiteSQLServerBatchExactAndOwned(
	t *testing.T,
) {
	table := sqliteSQLServerValueTestTable()
	columns := adapterColumnNames(table)
	payload := []byte{0x00, 0xff}
	unbounded := []byte{}
	input := [][]any{{
		int64(7),
		float64(1.25),
		"9007199254740993",
		int64(1),
		"東京",
		nil,
		payload,
		unbounded,
		"2026-07-30",
		"2026-07-30 12:34:56.123",
		"00112233-4455-6677-8899-AABBCCDDEEFF",
	}}

	normalized, err := normalizeSQLiteSQLServerBatch(
		table,
		columns,
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]any{{
		int64(7),
		float64(1.25),
		"9007199254740993",
		true,
		"東京",
		nil,
		[]byte{0x00, 0xff},
		[]byte{},
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		time.Date(
			2026, 7, 30, 12, 34, 56, 123_000_000, time.UTC,
		),
		"00112233-4455-6677-8899-aabbccddeeff",
	}}
	if !reflect.DeepEqual(normalized, want) {
		t.Fatalf("normalized = %#v, want %#v", normalized, want)
	}
	if &normalized[0][0] == &input[0][0] {
		t.Fatal("normalized row aliases the input row")
	}
	normalized[0][6].([]byte)[0] = 0xee
	if !reflect.DeepEqual(payload, []byte{0x00, 0xff}) {
		t.Fatalf("normalized BLOB aliases source bytes: %#v", payload)
	}
}

func TestSQLServerTargetWriteBatchNormalizesSQLiteValuesBeforeWriter(
	t *testing.T,
) {
	table := schema.Table{
		Name: "values",
		Columns: []schema.Column{
			sqliteSQLServerValueTestColumnDef(
				"enabled",
				"boolean",
				"bool",
			),
			sqliteSQLServerValueTestColumnDef(
				"payload",
				"blob",
				"blob",
			),
		},
	}
	payload := []byte{0x01, 0x02}
	writer := &sqliteSQLServerCapturingWriter{}
	adapter := &sqlServerTargetAdapter{
		batchWriter:  writer,
		sourceEngine: "sqlite",
	}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"enabled", "payload"},
		"drop_recreate",
		[][]any{{int64(1), payload}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptedRows != 1 ||
		receipt.CommittedRows != 1 ||
		writer.calls != 1 {
		t.Fatalf("receipt/writer = %#v / %#v", receipt, writer)
	}
	if got := writer.rows[0][0]; got != true {
		t.Fatalf("writer boolean = %#v, want true", got)
	}
	payload[0] = 0xff
	if !reflect.DeepEqual(
		writer.rows[0][1],
		[]byte{0x01, 0x02},
	) {
		t.Fatalf("writer BLOB aliases source: %#v", writer.rows)
	}

	writer.calls = 0
	receipt, err = adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"enabled", "payload"},
		"drop_recreate",
		[][]any{{int64(2), []byte{}}},
	)
	if err == nil || !strings.Contains(err.Error(), "boolean 0 or 1") {
		t.Fatalf("invalid boolean error = %v", err)
	}
	if writer.calls != 0 ||
		receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 {
		t.Fatalf("rejected receipt/writer = %#v / %#v", receipt, writer)
	}
}

func TestNormalizeSQLiteSQLServerBatchRejectsDynamicValueLoss(
	t *testing.T,
) {
	tests := []struct {
		name      string
		column    schema.Column
		value     any
		wantError string
	}{
		{
			name:      "integer storage",
			column:    sqliteSQLServerValueTestColumnDef("value", "bigint", "bigint"),
			value:     float64(7),
			wantError: "SQLite INTEGER",
		},
		{
			name:      "nonfinite real",
			column:    sqliteSQLServerValueTestColumnDef("value", "double precision", "double precision"),
			value:     math.Inf(1),
			wantError: "finite SQLite REAL",
		},
		{
			name:      "decimal fraction",
			column:    sqliteSQLServerValueTestColumnDef("value", "numeric", "decimal", 18, 0),
			value:     "12.0",
			wantError: "DECIMAL(18,0)",
		},
		{
			name:      "decimal noncanonical",
			column:    sqliteSQLServerValueTestColumnDef("value", "numeric", "decimal", 18, 0),
			value:     "0012",
			wantError: "DECIMAL(18,0)",
		},
		{
			name:      "decimal precision",
			column:    sqliteSQLServerValueTestColumnDef("value", "numeric", "decimal", 3, 0),
			value:     "1000",
			wantError: "DECIMAL(3,0)",
		},
		{
			name:      "decimal beyond int64",
			column:    sqliteSQLServerValueTestColumnDef("value", "numeric", "decimal", 18, 0),
			value:     "9999999999999999999",
			wantError: "DECIMAL(18,0)",
		},
		{
			name:      "boolean",
			column:    sqliteSQLServerValueTestColumnDef("value", "boolean", "bool"),
			value:     int64(2),
			wantError: "boolean 0 or 1",
		},
		{
			name:      "invalid utf8",
			column:    sqliteSQLServerValueTestColumnDef("value", "text", "text"),
			value:     string([]byte{0xff}),
			wantError: "UTF-8",
		},
		{
			name:      "text nul",
			column:    sqliteSQLServerValueTestColumnDef("value", "text", "text"),
			value:     "secret\x00suffix",
			wantError: "NUL-free",
		},
		{
			name:      "utf8 byte limit",
			column:    sqliteSQLServerValueTestColumnDef("value", "text", "varchar", 3),
			value:     "東京",
			wantError: "byte length 3",
		},
		{
			name:      "binary storage",
			column:    sqliteSQLServerValueTestColumnDef("value", "blob", "blob"),
			value:     "bytes",
			wantError: "BLOB bytes",
		},
		{
			name:      "binary length",
			column:    sqliteSQLServerValueTestColumnDef("value", "blob", "varbinary", 2),
			value:     []byte{1, 2, 3},
			wantError: "byte length 2",
		},
		{
			name:      "date shape",
			column:    sqliteSQLServerValueTestColumnDef("value", "date", "date"),
			value:     "2026-07-30 00:00:00",
			wantError: "SQL Server DATE",
		},
		{
			name:      "year zero",
			column:    sqliteSQLServerValueTestColumnDef("value", "date", "date"),
			value:     "0000-01-01",
			wantError: "SQL Server DATE",
		},
		{
			name:      "datetime precision",
			column:    sqliteSQLServerValueTestColumnDef("value", "datetime", "timestamp", 3),
			value:     "2026-07-30 12:34:56.1234",
			wantError: "DATETIME2(3)",
		},
		{
			name:      "datetime short precision",
			column:    sqliteSQLServerValueTestColumnDef("value", "datetime", "timestamp", 3),
			value:     "2026-07-30 12:34:56.12",
			wantError: "DATETIME2(3)",
		},
		{
			name:      "datetime alternate separator",
			column:    sqliteSQLServerValueTestColumnDef("value", "datetime", "timestamp", 3),
			value:     "2026-07-30T12:34:56.123",
			wantError: "DATETIME2(3)",
		},
		{
			name:      "datetime zone",
			column:    sqliteSQLServerValueTestColumnDef("value", "datetime", "timestamp", 6),
			value:     "2026-07-30T12:34:56-05:00",
			wantError: "DATETIME2(6)",
		},
		{
			name:      "uuid",
			column:    sqliteSQLServerValueTestColumnDef("value", "uuid", "uuid"),
			value:     "not-a-uuid",
			wantError: "valid UUID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := schema.Table{
				Name:    "values",
				Columns: []schema.Column{test.column},
			}
			_, err := normalizeSQLiteSQLServerBatch(
				table,
				[]string{"value"},
				[][]any{{test.value}},
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaks source value: %v", err)
			}
		})
	}
}

func TestNormalizeSQLiteSQLServerBatchRejectsInvalidShape(
	t *testing.T,
) {
	table := schema.Table{
		Name: "values",
		Columns: []schema.Column{
			sqliteSQLServerValueTestColumnDef(
				"id",
				"bigint",
				"bigint",
			),
		},
	}
	if _, err := normalizeSQLiteSQLServerBatch(
		table,
		[]string{"id"},
		[][]any{{}},
	); err == nil || !strings.Contains(err.Error(), "row width") {
		t.Fatalf("row-width error = %v", err)
	}
	if _, err := normalizeSQLiteSQLServerBatch(
		table,
		[]string{"missing"},
		nil,
	); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("missing-column error = %v", err)
	}
	if _, err := normalizeSQLiteSQLServerBatch(
		table,
		[]string{"id", "id"},
		nil,
	); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate-selection error = %v", err)
	}
	table.Columns[0].DeclaredType = &schema.DeclaredType{
		Base:      "decimal",
		Arguments: []int{18, 2},
	}
	table.Columns[0].Type = "numeric"
	if _, err := normalizeSQLiteSQLServerBatch(
		table,
		[]string{"id"},
		nil,
	); err == nil || !strings.Contains(
		err.Error(),
		"no exact SQLite source-value contract",
	) {
		t.Fatalf("unsupported-domain error = %v", err)
	}
}

func TestPlanSQLiteSQLServerSourceProbesBuildsSafeQueries(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression(`"amount" >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	source := schema.Table{
		Name: `odd'"table`,
		Columns: []schema.Column{
			{
				Name: "id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
			{
				Name: "amount", Type: "decimal",
				DeclaredType: &schema.DeclaredType{
					Base: "decimal", Arguments: []int{18, 0},
				},
			},
			{
				Name: "occurred_at", Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "datetime",
					Arguments: []int{3},
				},
			},
		},
		Checks: []schema.CheckConstraint{{Expression: check}},
		ForeignKeys: []schema.ForeignKey{{
			Columns:           []string{"id"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
		}},
		Indexes: []schema.Index{{
			Name:   "amount_uq",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name: "amount", Collation: "BINARY",
			}},
		}},
	}
	probes, err := planSQLiteSQLServerSourceProbes(
		[]adapterTablePlan{{source: source}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(probes) != 5 {
		t.Fatalf("probes = %#v, want 5", probes)
	}
	joined := probes[0].query + probes[1].query +
		probes[2].query + probes[3].query + probes[4].query
	for _, fragment := range []string{
		`"odd'""table"`,
		`typeof("amount") <> 'integer'`,
		`typeof("occurred_at") <> 'text'`,
		`NOT ("amount" >= 0)`,
		`pragma_foreign_key_check('odd''"table')`,
		`GROUP BY "amount" COLLATE BINARY`,
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("queries %#v do not contain %q", probes, fragment)
		}
	}
}

func TestRunSQLiteSQLServerSourceProbesRejectsTemporalStorage(
	t *testing.T,
) {
	runner := &sqliteSQLServerProbeTestRunner{
		results: []sqliteSQLServerProbeTestResult{{invalid: true}},
	}
	err := runSQLiteSQLServerSourceProbes(
		context.Background(),
		runner,
		[]sqliteSQLServerSourceProbe{{
			kind:   sqliteSQLServerSourceProbeTemporalStorage,
			table:  "events",
			object: "occurred_at",
			query:  "temporal",
		}},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "non-TEXT storage class") ||
		!strings.Contains(err.Error(), "occurred_at") {
		t.Fatalf("temporal storage error = %v", err)
	}
}

func TestRunSQLiteSQLServerSourceProbesStopsAtViolation(
	t *testing.T,
) {
	runner := &sqliteSQLServerProbeTestRunner{
		results: []sqliteSQLServerProbeTestResult{
			{},
			{invalid: true},
			{err: errors.New("must not run")},
		},
	}
	err := runSQLiteSQLServerSourceProbes(
		context.Background(),
		runner,
		[]sqliteSQLServerSourceProbe{
			{
				kind:  sqliteSQLServerSourceProbeNumericStorage,
				table: "events", object: "amount", query: "first",
			},
			{
				kind:  sqliteSQLServerSourceProbeCheck,
				table: "events", object: "1", query: "second",
			},
			{
				kind:  sqliteSQLServerSourceProbeForeignKey,
				table: "events", object: "foreign keys",
				query: "third",
			},
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"CHECK 1 is violated",
	) {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(runner.queries, []string{"first", "second"}) {
		t.Fatalf("queries = %#v", runner.queries)
	}
}

func TestPreflightSQLiteSQLServerSourceDataChecksAllRowsAndCloses(
	t *testing.T,
) {
	sourceTable := schema.Table{
		Name: "events",
		Columns: []schema.Column{{
			Name: "note", Type: "text",
			DeclaredType: &schema.DeclaredType{Base: "text"},
		}},
	}
	targetTable := sourceTable
	fixture := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows: [][]any{
			{"safe"},
			{"secret-value\x00must-not-leak"},
		},
	}
	source := &sqliteSQLServerValueFixtureSource{
		sqlServerTargetValueFixtureSource: fixture,
	}
	err := preflightSQLiteSQLServerSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"note"},
		}},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "row 2") ||
		!strings.Contains(err.Error(), "column note") {
		t.Fatalf("preflight error = %v", err)
	}
	if strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("preflight leaked a source value: %v", err)
	}
	if fixture.opens != 1 || fixture.closes != 1 {
		t.Fatalf(
			"source opens/closes = %d/%d, want 1/1",
			fixture.opens,
			fixture.closes,
		)
	}
}

func sqliteSQLServerValueTestTable() schema.Table {
	column := func(
		name string,
		semantic string,
		base string,
		arguments ...int,
	) schema.Column {
		return sqliteSQLServerValueTestColumnDef(
			name,
			semantic,
			base,
			arguments...,
		)
	}
	optional := column("optional", "text", "text")
	optional.Nullable = true
	return schema.Table{
		Name: "values",
		Columns: []schema.Column{
			column("id", "bigint", "bigint"),
			column("ratio", "double precision", "double precision"),
			column("amount", "numeric", "decimal", 18, 0),
			column("enabled", "boolean", "bool"),
			column("code", "text", "varchar", 16),
			optional,
			column("payload", "blob", "varbinary", 4),
			column("raw", "blob", "blob"),
			column("day", "date", "date"),
			column("occurred", "datetime", "timestamp", 3),
			column("external_id", "uuid", "uuid"),
		},
	}
}

func sqliteSQLServerValueTestColumnDef(
	name string,
	semantic string,
	base string,
	arguments ...int,
) schema.Column {
	return schema.Column{
		Name: name,
		Type: semantic,
		DeclaredType: &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		},
	}
}

type sqliteSQLServerProbeTestResult struct {
	invalid bool
	err     error
}

type sqliteSQLServerProbeTestRunner struct {
	results []sqliteSQLServerProbeTestResult
	queries []string
}

func (runner *sqliteSQLServerProbeTestRunner) hasInvalidSQLiteSQLServerSourceRow(
	_ context.Context,
	query string,
) (bool, error) {
	runner.queries = append(runner.queries, query)
	if len(runner.results) == 0 {
		return false, errors.New("unexpected probe")
	}
	result := runner.results[0]
	runner.results = runner.results[1:]
	return result.invalid, result.err
}

type sqliteSQLServerValueFixtureSource struct {
	*sqlServerTargetValueFixtureSource
}

func (*sqliteSQLServerValueFixtureSource) Engine() string {
	return "sqlite"
}

func (*sqliteSQLServerValueFixtureSource) DisplayName() string {
	return "SQLite"
}

type sqliteSQLServerCapturingWriter struct {
	calls int
	rows  [][]any
}

func (writer *sqliteSQLServerCapturingWriter) WriteBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	writer.calls++
	writer.rows = rows
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}
