package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizeMySQLSQLiteBatchPreservesMySQLAndMariaDBValues(
	t *testing.T,
) {
	for _, flavor := range []string{"MySQL 8.0", "MariaDB 10.11"} {
		t.Run(flavor, func(t *testing.T) {
			table := mySQLSQLiteValueTestTable()
			columns := adapterColumnNames(table)
			text := []byte("héllo")
			fixed := []byte{0x00, 0x7f, 0xff}
			varying := []byte{0xde, 0xad}
			payload := []byte{0xbe, 0xef}
			row := []any{
				int64(-128),
				int64(32_767),
				int64(8_388_607),
				int64(-2_147_483_648),
				int64(-9_223_372_036_854_775_808),
				[]byte("999999999999999999"),
				text,
				[]byte("long text"),
				fixed,
				varying,
				payload,
				time.Date(
					2026, time.July, 30, 0, 0, 0, 0, time.UTC,
				),
				"23:59:59.123456",
				time.Date(
					2026,
					time.July,
					30,
					12,
					34,
					56,
					123_000_000,
					time.UTC,
				),
				time.Date(
					2026,
					time.July,
					30,
					12,
					34,
					56,
					0,
					time.UTC,
				),
				nil,
			}

			normalized, err := normalizeMySQLSQLiteBatch(
				table,
				columns,
				[][]any{row},
			)
			if err != nil {
				t.Fatalf("normalize admitted %s row: %v", flavor, err)
			}
			want := [][]any{{
				int64(-128),
				int64(32_767),
				int64(8_388_607),
				int64(-2_147_483_648),
				int64(-9_223_372_036_854_775_808),
				int64(999_999_999_999_999_999),
				"héllo",
				"long text",
				[]byte{0x00, 0x7f, 0xff},
				[]byte{0xde, 0xad},
				[]byte{0xbe, 0xef},
				"2026-07-30",
				"23:59:59.123456",
				"2026-07-30 12:34:56.123",
				"2026-07-30 12:34:56",
				nil,
			}}
			if !reflect.DeepEqual(normalized, want) {
				t.Fatalf(
					"normalized %s row = %#v, want %#v",
					flavor,
					normalized,
					want,
				)
			}

			normalized[0][8].([]byte)[0] = 0xee
			normalized[0][9].([]byte)[0] = 0xee
			normalized[0][10].([]byte)[0] = 0xee
			if !reflect.DeepEqual(fixed, []byte{0x00, 0x7f, 0xff}) ||
				!reflect.DeepEqual(varying, []byte{0xde, 0xad}) ||
				!reflect.DeepEqual(payload, []byte{0xbe, 0xef}) {
				t.Fatal("normalization retained an alias to source binary data")
			}
			if !reflect.DeepEqual(text, []byte("héllo")) ||
				!reflect.DeepEqual(
					row[5],
					[]byte("999999999999999999"),
				) {
				t.Fatalf("normalization mutated source row: %#v", row)
			}
		})
	}
}

func TestNormalizeMySQLSQLiteBatchFailsClosed(t *testing.T) {
	column := func(
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
	tests := []struct {
		name   string
		column schema.Column
		value  any
		want   string
	}{
		{
			name:   "unexpected integer driver shape",
			column: column("id", "integer", "int"),
			value:  "12",
			want:   "source-width integer",
		},
		{
			name:   "tinyint overflow",
			column: column("id", "integer", "tinyint"),
			value:  int64(128),
			want:   "source-width integer",
		},
		{
			name:   "mediumint underflow",
			column: column("id", "integer", "mediumint"),
			value:  int64(-8_388_609),
			want:   "source-width integer",
		},
		{
			name:   "fractional decimal",
			column: column("amount", "numeric", "bigint"),
			value:  []byte("12.25"),
			want:   "exact SQLite INTEGER",
		},
		{
			name:   "decimal precision",
			column: column("amount", "numeric", "bigint"),
			value:  []byte("1000000000000000000"),
			want:   "exact SQLite INTEGER",
		},
		{
			name:   "unexpected decimal driver shape",
			column: column("amount", "numeric", "bigint"),
			value:  "12",
			want:   "exact SQLite INTEGER",
		},
		{
			name:   "unexpected text driver shape",
			column: column("note", "text", "text"),
			value:  "secret-text-shape",
			want:   "UTF-8 text",
		},
		{
			name:   "invalid UTF-8",
			column: column("note", "text", "text"),
			value:  []byte{0xff},
			want:   "UTF-8 text",
		},
		{
			name:   "embedded NUL",
			column: column("note", "text", "text"),
			value:  []byte("secret\x00suffix"),
			want:   "UTF-8 text",
		},
		{
			name:   "varchar rune length",
			column: column("code", "varchar", "varchar", 2),
			value:  []byte("éab"),
			want:   "VARCHAR length 2",
		},
		{
			name:   "fixed binary length",
			column: column("payload", "binary", "binary", 3),
			value:  []byte{0x01, 0x02},
			want:   "binary length",
		},
		{
			name:   "varbinary length",
			column: column("payload", "varbinary", "varbinary", 2),
			value:  []byte{0x01, 0x02, 0x03},
			want:   "binary length",
		},
		{
			name:   "unexpected binary driver shape",
			column: column("payload", "blob", "blob"),
			value:  "not-binary",
			want:   "binary length",
		},
		{
			name:   "date carries time",
			column: column("day", "date", "date"),
			value: time.Date(
				2026, time.July, 30, 1, 0, 0, 0, time.UTC,
			),
			want: "SQLite DATE",
		},
		{
			name:   "date before MySQL finite range",
			column: column("day", "date", "date"),
			value: time.Date(
				999, time.December, 31, 0, 0, 0, 0, time.UTC,
			),
			want: "SQLite DATE",
		},
		{
			name:   "date offset",
			column: column("day", "date", "date"),
			value: time.Date(
				2026,
				time.July,
				30,
				0,
				0,
				0,
				0,
				time.FixedZone("unexpected", 3600),
			),
			want: "SQLite DATE",
		},
		{
			name:   "duration TIME",
			column: column("clock", "time", "time", 0),
			value:  "24:00:00",
			want:   "canonical MySQL TIME",
		},
		{
			name:   "TIME precision",
			column: column("clock", "time", "time", 3),
			value:  "12:34:56.1234",
			want:   "canonical MySQL TIME",
		},
		{
			name:   "unexpected TIME driver shape",
			column: column("clock", "time", "time", 0),
			value:  []byte("12:34:56"),
			want:   "canonical MySQL TIME",
		},
		{
			name: "datetime precision",
			column: column(
				"occurred_at",
				"datetime",
				"datetime",
				3,
			),
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				0,
				0,
				123_001_000,
				time.UTC,
			),
			want: "temporal precision",
		},
		{
			name: "timestamp offset",
			column: column(
				"occurred_at",
				"timestamp",
				"timestamp",
				0,
			),
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				0,
				0,
				0,
				time.FixedZone("unexpected", -3600),
			),
			want: "temporal precision",
		},
		{
			name:   "NULL non-null",
			column: column("id", "bigint", "bigint"),
			value:  nil,
			want:   "NULL",
		},
		{
			name:   "unadmitted double",
			column: column("ratio", "double precision", "real"),
			value:  0.5,
			want:   "no exact SQLite value contract",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := schema.Table{
				Name:    "events",
				Columns: []schema.Column{test.column},
			}
			_, err := normalizeMySQLSQLiteBatch(
				table,
				[]string{test.column.Name},
				[][]any{{test.value}},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"normalization error = %v, want %q",
					err,
					test.want,
				)
			}
			for _, secret := range []string{
				"secret-text-shape",
				"secret",
			} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf(
						"normalization leaked source value: %v",
						err,
					)
				}
			}
		})
	}
}

func TestNormalizeMySQLSQLiteBatchRejectsMalformedPlanAndRows(
	t *testing.T,
) {
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{{
			Name: "id", Type: "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		}},
	}
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		rows    [][]any
		want    string
	}{
		{
			name:    "row width",
			table:   table,
			columns: []string{"id"},
			rows:    [][]any{{int64(1), int64(2)}},
			want:    "row has 2 values",
		},
		{
			name:    "unknown selected column",
			table:   table,
			columns: []string{"missing"},
			rows:    [][]any{{int64(1)}},
			want:    "absent",
		},
		{
			name:    "duplicate selected column",
			table:   table,
			columns: []string{"id", "id"},
			rows:    [][]any{{int64(1), int64(1)}},
			want:    "duplicated",
		},
		{
			name: "missing declaration",
			table: schema.Table{
				Name:    "events",
				Columns: []schema.Column{{Name: "id", Type: "bigint"}},
			},
			columns: []string{"id"},
			rows:    [][]any{{int64(1)}},
			want:    "declared type is missing",
		},
		{
			name: "invalid mapper pair",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{{
					Name: "id", Type: "integer",
					DeclaredType: &schema.DeclaredType{Base: "integer"},
				}},
			},
			columns: []string{"id"},
			rows:    [][]any{{int64(1)}},
			want:    "invalid integer projection",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeMySQLSQLiteBatch(
				test.table,
				test.columns,
				test.rows,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"normalization error = %v, want %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestAddMySQLSQLiteRowPayloadKeepsRecordHeadroom(t *testing.T) {
	got, ok := addMySQLSQLiteRowPayload(
		mySQLSQLiteMaximumRowPayloadBytes-2,
		"a",
	)
	if !ok || got != mySQLSQLiteMaximumRowPayloadBytes-1 {
		t.Fatalf("payload total = %d, %t", got, ok)
	}
	if _, ok := addMySQLSQLiteRowPayload(got, "ab"); ok {
		t.Fatal("aggregate payload above the conservative limit was admitted")
	}
	if _, ok := addMySQLSQLiteRowPayload(-1, int64(1)); ok {
		t.Fatal("negative aggregate payload was admitted")
	}
}

func TestPlanMySQLSQLiteConstraintProbesBuildsExactQueries(
	t *testing.T,
) {
	columns := []schema.Column{
		{
			Name: "id", Type: "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		{
			Name: "amount", Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base: "decimal", Arguments: []int{12, 0},
			},
		},
		{
			Name: "parent_id", Type: "bigint", Nullable: true,
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
	}
	check, err := schema.ParseMySQLCatalogCheck(
		"`amount` >= 0",
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := schema.Table{
		Schema:  "app`data",
		Name:    "event`items",
		Columns: columns,
		Checks: []schema.CheckConstraint{{
			Name: "ck_amount", Expression: check,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "fk_parent",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parent`items",
			ReferencedColumns: []string{"id"},
		}},
		Indexes: []schema.Index{{
			Name:   "ux_amount",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name: "amount",
			}},
		}},
	}
	target := source
	target.Schema = ""

	got, err := planMySQLSQLiteConstraintProbes(
		[]adapterTablePlan{{
			source: source,
			target: target,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []mySQLSQLiteConstraintProbe{
		{
			kind:   mySQLSQLiteConstraintProbeCheck,
			table:  "event`items",
			object: "ck_amount",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE NOT (`amount` >= 0))",
		},
		{
			kind:   mySQLSQLiteConstraintProbeForeignKey,
			table:  "event`items",
			object: "fk_parent",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" AS `dmtx_child` LEFT JOIN `app``data`.`parent``items`" +
				" AS `dmtx_parent` ON `dmtx_child`.`parent_id` =" +
				" `dmtx_parent`.`id` WHERE" +
				" `dmtx_child`.`parent_id` IS NOT NULL AND" +
				" `dmtx_parent`.`id` IS NULL)",
		},
		{
			kind:   mySQLSQLiteConstraintProbeUniqueIndex,
			table:  "event`items",
			object: "ux_amount",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE `amount` IS NOT NULL GROUP BY `amount`" +
				" HAVING COUNT(*) > 1)",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("constraint probes = %#v, want %#v", got, want)
	}
}

func TestRunMySQLSQLiteConstraintProbesReportsSafeViolations(
	t *testing.T,
) {
	tests := []struct {
		name    string
		kind    mySQLSQLiteConstraintProbeKind
		object  string
		wantErr string
	}{
		{
			name:    "CHECK",
			kind:    mySQLSQLiteConstraintProbeCheck,
			object:  "ck_amount",
			wantErr: "CHECK ck_amount is violated by historical rows",
		},
		{
			name:    "foreign key",
			kind:    mySQLSQLiteConstraintProbeForeignKey,
			object:  "fk_parent",
			wantErr: "foreign key fk_parent has orphan rows",
		},
		{
			name:    "unique index",
			kind:    mySQLSQLiteConstraintProbeUniqueIndex,
			object:  "ux_amount",
			wantErr: "unique index ux_amount has duplicate fully-nonnull keys",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &mySQLSQLiteProbeTestRunner{
				results: []mySQLSQLiteProbeTestResult{{
					invalid: true,
				}},
			}
			err := runMySQLSQLiteConstraintProbes(
				context.Background(),
				runner,
				[]mySQLSQLiteConstraintProbe{{
					kind:   test.kind,
					table:  "events",
					object: test.object,
					query:  "probe-query",
				}},
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("probe error = %v", err)
			}
			if strings.Contains(err.Error(), "secret-row-value") {
				t.Fatalf("probe leaked a row value: %v", err)
			}
		})
	}
}

func TestRunMySQLSQLiteConstraintProbesRunsInOrderAndStopsOnError(
	t *testing.T,
) {
	forced := errors.New("catalog connection failed")
	runner := &mySQLSQLiteProbeTestRunner{
		results: []mySQLSQLiteProbeTestResult{
			{},
			{err: forced},
			{},
		},
	}
	err := runMySQLSQLiteConstraintProbes(
		context.Background(),
		runner,
		[]mySQLSQLiteConstraintProbe{
			{
				kind:   mySQLSQLiteConstraintProbeCheck,
				table:  "events",
				object: "ck_events",
				query:  "first",
			},
			{
				kind:   mySQLSQLiteConstraintProbeForeignKey,
				table:  "events",
				object: "fk_events",
				query:  "second",
			},
			{
				kind:   mySQLSQLiteConstraintProbeUniqueIndex,
				table:  "events",
				object: "ux_events",
				query:  "third",
			},
		},
	)
	if !errors.Is(err, forced) ||
		!strings.Contains(err.Error(), "foreign key fk_events") {
		t.Fatalf("probe error = %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(
		runner.queries,
		want,
	) {
		t.Fatalf("queries = %#v, want %#v", runner.queries, want)
	}
}

func TestPreflightMySQLSQLiteSourceDataChecksAllRowsAndCloses(
	t *testing.T,
) {
	sourceTable := schema.Table{
		Schema: "source_database",
		Name:   "events",
		Columns: []schema.Column{{
			Name: "note", Type: "text",
			DeclaredType: &schema.DeclaredType{Base: "text"},
		}},
	}
	targetTable := sourceTable
	targetTable.Schema = ""
	fixture := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows: [][]any{
			{[]byte("safe")},
			{[]byte("secret-value\x00must-not-leak")},
		},
	}
	source := &mySQLServerTargetValueFixtureSource{
		sqlServerTargetValueFixtureSource: fixture,
	}
	err := preflightMySQLSQLiteSourceData(
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

func mySQLSQLiteValueTestTable() schema.Table {
	column := func(
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
	optional := column("optional_note", "text", "text")
	optional.Nullable = true
	return schema.Table{
		Name: "values",
		Columns: []schema.Column{
			column("tiny_number", "integer", "tinyint"),
			column("small_number", "integer", "smallint"),
			column("medium_number", "integer", "mediumint"),
			column("regular_number", "integer", "int"),
			column("large_number", "bigint", "bigint"),
			column("amount", "numeric", "bigint"),
			column("code", "varchar", "varchar", 20),
			column("note", "text", "text"),
			column("fixed_bytes", "binary", "binary", 3),
			column("varying_bytes", "varbinary", "varbinary", 4),
			column("payload", "blob", "blob"),
			column("day", "date", "date"),
			column("clock", "time", "time", 6),
			column("occurred_at", "datetime", "datetime", 3),
			column("updated_at", "timestamp", "timestamp", 0),
			optional,
		},
	}
}

type mySQLSQLiteProbeTestResult struct {
	invalid bool
	err     error
}

type mySQLSQLiteProbeTestRunner struct {
	results []mySQLSQLiteProbeTestResult
	queries []string
}

func (runner *mySQLSQLiteProbeTestRunner) hasInvalidRow(
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
