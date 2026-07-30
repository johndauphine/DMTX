package migrate

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizePostgresSQLiteBatchPreservesAdmittedValues(t *testing.T) {
	table := postgresSQLiteValueTestTable()
	columns := adapterColumnNames(table)
	binary := []byte{0x00, 0x7f, 0xff}
	row := []any{
		int64(-41),
		"9223372036854775807.000",
		[]byte{1},
		"héllo",
		binary,
		time.Date(2026, time.July, 30, 0, 0, 0, 0, time.UTC),
		"24:00:00.000000",
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
		nil,
	}

	normalized, err := normalizePostgresSQLiteBatch(
		table,
		columns,
		[][]any{row},
	)
	if err != nil {
		t.Fatalf("normalize admitted PostgreSQL row: %v", err)
	}
	want := [][]any{{
		int64(-41),
		int64(math.MaxInt64),
		true,
		"héllo",
		[]byte{0x00, 0x7f, 0xff},
		"2026-07-30",
		"24:00:00.000000",
		"2026-07-30 12:34:56.123",
		nil,
	}}
	if !reflect.DeepEqual(normalized, want) {
		t.Fatalf("normalized row = %#v, want %#v", normalized, want)
	}
	normalized[0][4].([]byte)[0] = 0xee
	if binary[0] != 0x00 {
		t.Fatal("normalization retained an alias to source binary data")
	}
	if row[1] != "9223372036854775807.000" ||
		!reflect.DeepEqual(row[4], []byte{0x00, 0x7f, 0xff}) {
		t.Fatalf("normalization mutated the source row: %#v", row)
	}
}

func TestNormalizePostgresSQLiteBatchFailsClosed(t *testing.T) {
	baseTable := func(column schema.Column) schema.Table {
		return schema.Table{
			Name:    "events",
			Columns: []schema.Column{column},
		}
	}
	tests := []struct {
		name   string
		column schema.Column
		value  any
		want   string
	}{
		{
			name: "fractional numeric",
			column: schema.Column{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			value: "12.25",
			want:  "exact SQLite INTEGER",
		},
		{
			name: "integer overflow",
			column: schema.Column{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			value: "9223372036854775808",
			want:  "exact SQLite INTEGER",
		},
		{
			name: "loose boolean",
			column: schema.Column{
				Name: "enabled", Type: "boolean",
				DeclaredType: &schema.DeclaredType{Base: "boolean"},
			},
			value: int64(2),
			want:  "strict boolean",
		},
		{
			name: "invalid UTF-8",
			column: schema.Column{
				Name: "note", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			value: []byte{0xff},
			want:  "UTF-8",
		},
		{
			name: "text NUL",
			column: schema.Column{
				Name: "note", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			value: "hidden\x00suffix",
			want:  "UTF-8",
		},
		{
			name: "varchar rune length",
			column: schema.Column{
				Name: "code", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{2},
				},
			},
			value: "éab",
			want:  "VARCHAR length 2",
		},
		{
			name: "non-binary blob",
			column: schema.Column{
				Name: "payload", Type: "bytea",
				DeclaredType: &schema.DeclaredType{Base: "blob"},
			},
			value: "not bytes",
			want:  "binary",
		},
		{
			name: "infinite date",
			column: schema.Column{
				Name: "day", Type: "date",
				DeclaredType: &schema.DeclaredType{Base: "date"},
			},
			value: pgtype.Date{
				Valid:            true,
				InfinityModifier: pgtype.Infinity,
			},
			want: "SQLite DATE",
		},
		{
			name: "date carries time",
			column: schema.Column{
				Name: "day", Type: "date",
				DeclaredType: &schema.DeclaredType{Base: "date"},
			},
			value: time.Date(
				2026, time.July, 30, 1, 0, 0, 0, time.UTC,
			),
			want: "SQLite DATE",
		},
		{
			name: "timestamp infinity",
			column: schema.Column{
				Name: "occurred_at", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
			value: pgtype.Timestamp{
				Valid:            true,
				InfinityModifier: pgtype.NegativeInfinity,
			},
			want: "SQLite TIMESTAMP",
		},
		{
			name: "timestamp precision",
			column: schema.Column{
				Name: "occurred_at", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{3},
				},
			},
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
			want: "TIMESTAMP precision",
		},
		{
			name: "timestamp offset",
			column: schema.Column{
				Name: "occurred_at", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{Base: "timestamp"},
			},
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				0,
				0,
				0,
				time.FixedZone("unexpected", 3600),
			),
			want: "TIMESTAMP precision",
		},
		{
			name: "time precision",
			column: schema.Column{
				Name: "clock", Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base: "time", Arguments: []int{3},
				},
			},
			value: "12:34:56.123001",
			want:  "TIME precision",
		},
		{
			name: "non-zero end-of-day fraction",
			column: schema.Column{
				Name: "clock", Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base: "time", Arguments: []int{6},
				},
			},
			value: "24:00:00.000001",
			want:  "TIME precision",
		},
		{
			name: "NULL non-null",
			column: schema.Column{
				Name: "id", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			value: nil,
			want:  "NULL",
		},
		{
			name: "unadmitted source type",
			column: schema.Column{
				Name: "document", Type: "jsonb",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			value: "{}",
			want:  "no exact SQLite value contract",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := baseTable(test.column)
			_, err := normalizePostgresSQLiteBatch(
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
		})
	}
}

func TestNormalizePostgresSQLiteBatchRejectsMalformedPlanAndRows(
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizePostgresSQLiteBatch(
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

func TestPreflightPostgresSQLiteSourceDataChecksAllRowsAndCloses(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{{
			Name: "note", Type: "text",
			DeclaredType: &schema.DeclaredType{Base: "text"},
		}},
	}
	target := cloneSQLiteTargetTable(table)
	target.Schema = ""
	source := &sqlServerTargetValueFixtureSource{
		table: table,
		rows: [][]any{
			{"safe"},
			{"secret-value\x00must-not-leak"},
		},
	}
	err := preflightPostgresSQLiteSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  table,
			target:  target,
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
	if source.opens != 1 || source.closes != 1 {
		t.Fatalf(
			"source opens/closes = %d/%d, want 1/1",
			source.opens,
			source.closes,
		)
	}
}

func postgresSQLiteValueTestTable() schema.Table {
	return schema.Table{
		Name: "values",
		Columns: []schema.Column{
			{
				Name: "id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
			{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "enabled", Type: "boolean",
				DeclaredType: &schema.DeclaredType{Base: "boolean"},
			},
			{
				Name: "note", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			{
				Name: "payload", Type: "bytea",
				DeclaredType: &schema.DeclaredType{Base: "blob"},
			},
			{
				Name: "day", Type: "date",
				DeclaredType: &schema.DeclaredType{Base: "date"},
			},
			{
				Name: "clock", Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base: "time", Arguments: []int{6},
				},
			},
			{
				Name: "occurred_at", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{3},
				},
			},
			{
				Name: "optional_note", Type: "text", Nullable: true,
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
		},
	}
}
