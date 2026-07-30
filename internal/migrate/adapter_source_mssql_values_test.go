package migrate

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerSourceRowsNormalizeNumericAndUUID(t *testing.T) {
	source := &sqlServerSourceFixtureRows{values: []any{
		[]byte("-12345678901234567890.123456"),
		[]byte{
			0xff, 0x19, 0x96, 0x6f,
			0x86, 0x8b,
			0x11, 0xd0,
			0xb4, 0x2d,
			0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff,
		},
		nil,
		[]byte{0x00, 0xff},
	}}
	table := schema.Table{Columns: []schema.Column{
		{
			Name: "amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{30, 6},
			},
		},
		{
			Name: "record_id",
			Type: "uuid",
			DeclaredType: &schema.DeclaredType{
				Base: "uuid",
			},
		},
		{
			Name: "optional_amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "numeric",
				Arguments: []int{8, 2},
			},
		},
		{
			Name: "payload",
			Type: "blob",
		},
	}}
	rows := wrapSQLServerSourceRows(
		source,
		table,
		[]string{"amount", "record_id", "optional_amount", "payload"},
	)
	destinations := make([]any, 4)
	pointers := make([]any, 4)
	for index := range destinations {
		pointers[index] = &destinations[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		t.Fatalf("scan normalized values: %v", err)
	}
	if got, want := destinations[0], "-12345678901234567890.123456"; got != want {
		t.Fatalf("numeric = %#v, want %q", got, want)
	}
	if got, want := destinations[1], "ff19966f-868b-11d0-b42d-00c04fc964ff"; got != want {
		t.Fatalf("UUID = %#v, want %q", got, want)
	}
	if destinations[2] != nil {
		t.Fatalf("NULL numeric = %#v", destinations[2])
	}
	payload, ok := destinations[3].([]byte)
	if !ok || len(payload) != 2 || payload[0] != 0 || payload[1] != 0xff {
		t.Fatalf("binary payload = %#v", destinations[3])
	}
}

func TestSQLServerSourceRowsNormalizeTemporalDriverValues(t *testing.T) {
	zeroOffset := time.FixedZone("driver-zero-offset", 0)
	date := time.Date(2026, time.July, 30, 0, 0, 0, 0, zeroOffset)
	clock := time.Date(
		1,
		time.January,
		1,
		23,
		59,
		58,
		123456000,
		zeroOffset,
	)
	wholeSecondClock := time.Date(
		1,
		time.January,
		1,
		8,
		30,
		0,
		0,
		time.UTC,
	)
	timestamp := time.Date(
		2026,
		time.July,
		30,
		12,
		34,
		56,
		123000000,
		zeroOffset,
	)
	minute := time.Date(
		2026,
		time.July,
		30,
		12,
		34,
		0,
		0,
		time.UTC,
	)
	source := &sqlServerSourceFixtureRows{values: []any{
		date,
		clock,
		wholeSecondClock,
		timestamp,
		minute,
		nil,
	}}
	table := schema.Table{Columns: []schema.Column{
		sqlServerTemporalFixtureColumn("observed_on", "date", "date"),
		sqlServerTemporalFixtureColumn("local_time", "time", "time", 6),
		sqlServerTemporalFixtureColumn("whole_time", "time", "time", 0),
		sqlServerTemporalFixtureColumn(
			"occurred_at",
			"datetime",
			"timestamp",
			3,
		),
		sqlServerTemporalFixtureColumn(
			"minute_at",
			"datetime",
			"smalldatetime",
		),
		sqlServerTemporalFixtureColumn(
			"optional_time",
			"time",
			"time",
			6,
		),
	}}
	rows := wrapSQLServerSourceRows(
		source,
		table,
		[]string{
			"observed_on",
			"local_time",
			"whole_time",
			"occurred_at",
			"minute_at",
			"optional_time",
		},
	)
	destinations := make([]any, 6)
	pointers := make([]any, len(destinations))
	for index := range destinations {
		pointers[index] = &destinations[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		t.Fatalf("scan normalized temporal values: %v", err)
	}
	if got, want := destinations[1], "23:59:58.123456"; got != want {
		t.Fatalf("TIME(6) = %#v, want %q", got, want)
	}
	if got, want := destinations[2], "08:30:00"; got != want {
		t.Fatalf("TIME(0) = %#v, want %q", got, want)
	}
	for _, check := range []struct {
		index int
		want  time.Time
	}{
		{index: 0, want: date},
		{index: 3, want: timestamp},
		{index: 4, want: minute},
	} {
		got, ok := destinations[check.index].(time.Time)
		if !ok || !got.Equal(check.want) {
			t.Fatalf(
				"temporal value %d = %#v, want %#v",
				check.index,
				got,
				check.want,
			)
		}
		if got.Location() != time.UTC {
			t.Fatalf(
				"temporal value %d location = %v, want UTC",
				check.index,
				got.Location(),
			)
		}
	}
	if destinations[5] != nil {
		t.Fatalf("NULL TIME = %#v", destinations[5])
	}
}

func TestSQLServerSourceRowsRejectInvalidTemporalDriverValues(t *testing.T) {
	tests := []struct {
		name       string
		column     schema.Column
		value      any
		wantReason string
	}{
		{
			name:       "date text instead of driver time",
			column:     sqlServerTemporalFixtureColumn("value", "date", "date"),
			value:      "2026-07-30",
			wantReason: "invalid date",
		},
		{
			name:   "date has time component",
			column: sqlServerTemporalFixtureColumn("value", "date", "date"),
			value: time.Date(
				2026,
				time.July,
				30,
				0,
				0,
				1,
				0,
				time.UTC,
			),
			wantReason: "invalid date",
		},
		{
			name:   "date has nonzero offset",
			column: sqlServerTemporalFixtureColumn("value", "date", "date"),
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
			wantReason: "invalid date",
		},
		{
			name:       "time text instead of driver time",
			column:     sqlServerTemporalFixtureColumn("value", "time", "time", 6),
			value:      "12:34:56.123456",
			wantReason: "invalid time",
		},
		{
			name:   "time has date component",
			column: sqlServerTemporalFixtureColumn("value", "time", "time", 6),
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				34,
				56,
				123456000,
				time.UTC,
			),
			wantReason: "invalid time",
		},
		{
			name:   "time exceeds declared precision",
			column: sqlServerTemporalFixtureColumn("value", "time", "time", 3),
			value: time.Date(
				1,
				time.January,
				1,
				12,
				34,
				56,
				123400000,
				time.UTC,
			),
			wantReason: "invalid time",
		},
		{
			name: "datetime bytes instead of driver time",
			column: sqlServerTemporalFixtureColumn(
				"value",
				"datetime",
				"timestamp",
				6,
			),
			value:      []byte("2026-07-30 12:34:56.123456"),
			wantReason: "invalid datetime",
		},
		{
			name: "datetime exceeds declared precision",
			column: sqlServerTemporalFixtureColumn(
				"value",
				"datetime",
				"timestamp",
				3,
			),
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				34,
				56,
				123400000,
				time.UTC,
			),
			wantReason: "invalid datetime",
		},
		{
			name: "smalldatetime has seconds",
			column: sqlServerTemporalFixtureColumn(
				"value",
				"datetime",
				"smalldatetime",
			),
			value: time.Date(
				2026,
				time.July,
				30,
				12,
				34,
				1,
				0,
				time.UTC,
			),
			wantReason: "invalid datetime",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := scanSQLServerSourceFixture(test.column, test.value)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("error = %v, want reason %q", err, test.wantReason)
			}
			if strings.Contains(err.Error(), fmt.Sprint(test.value)) {
				t.Fatalf("error leaked source value: %v", err)
			}
		})
	}
}

func TestSQLServerSourceRowsRejectUnexpectedValueShapes(t *testing.T) {
	tests := []struct {
		name       string
		column     schema.Column
		value      any
		wantReason string
	}{
		{
			name: "numeric text instead of driver bytes",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{8, 2},
				},
			},
			value:      "12.34",
			wantReason: "unexpected value shape",
		},
		{
			name: "numeric exponent",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{8, 2},
				},
			},
			value:      []byte("1e2"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "numeric scale mismatch",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{8, 2},
				},
			},
			value:      []byte("12.3"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "numeric integer overflow",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{5, 2},
				},
			},
			value:      []byte("1234.56"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "short UUID",
			column: schema.Column{
				Name: "record_id",
				Type: "uuid",
				DeclaredType: &schema.DeclaredType{
					Base: "uuid",
				},
			},
			value:      []byte{1, 2, 3},
			wantReason: "invalid UUID",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := scanSQLServerSourceFixture(test.column, test.value)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("error = %v, want reason %q", err, test.wantReason)
			}
			if strings.Contains(err.Error(), fmt.Sprint(test.value)) {
				t.Fatalf("error leaked source value: %v", err)
			}
		})
	}
}

func TestSQLServerSourceRowsRejectInvalidMetadataAndMissingColumn(
	t *testing.T,
) {
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		value   any
		reason  string
	}{
		{
			name: "numeric modifiers absent",
			table: schema.Table{Columns: []schema.Column{{
				Name: "amount",
				Type: "numeric",
			}}},
			columns: []string{"amount"},
			value:   []byte("1.00"),
			reason:  "numeric declaration is missing",
		},
		{
			name: "UUID declaration mismatch",
			table: schema.Table{Columns: []schema.Column{{
				Name: "record_id",
				Type: "uuid",
				DeclaredType: &schema.DeclaredType{
					Base: "uniqueidentifier",
				},
			}}},
			columns: []string{"record_id"},
			value:   make([]byte, 16),
			reason:  "UUID declaration is invalid",
		},
		{
			name: "date declaration mismatch",
			table: schema.Table{Columns: []schema.Column{{
				Name: "observed_on",
				Type: "date",
				DeclaredType: &schema.DeclaredType{
					Base:      "date",
					Arguments: []int{0},
				},
			}}},
			columns: []string{"observed_on"},
			value:   time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			reason:  "date declaration is invalid",
		},
		{
			name: "time declaration mismatch",
			table: schema.Table{Columns: []schema.Column{{
				Name: "local_time",
				Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base:      "time",
					Arguments: []int{7},
				},
			}}},
			columns: []string{"local_time"},
			value:   time.Date(1, 1, 1, 12, 0, 0, 0, time.UTC),
			reason:  "time declaration is invalid",
		},
		{
			name: "datetime declaration absent",
			table: schema.Table{Columns: []schema.Column{{
				Name: "occurred_at",
				Type: "datetime",
			}}},
			columns: []string{"occurred_at"},
			value:   time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
			reason:  "datetime declaration is missing",
		},
		{
			name: "datetime declaration mismatch",
			table: schema.Table{Columns: []schema.Column{{
				Name: "occurred_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "datetime2",
				},
			}}},
			columns: []string{"occurred_at"},
			value:   time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
			reason:  "datetime declaration is invalid",
		},
		{
			name:    "selected column absent",
			columns: []string{"missing"},
			value:   int64(1),
			reason:  "column is absent from discovered schema",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &sqlServerSourceFixtureRows{
				values: []any{test.value},
			}
			rows := wrapSQLServerSourceRows(
				source,
				test.table,
				test.columns,
			)
			var got any
			err := rows.Scan(&got)
			if err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("error = %v, want reason %q", err, test.reason)
			}
		})
	}
}

func TestSQLServerSourceRowsLeaveOtherTypesUnwrapped(t *testing.T) {
	source := &sqlServerSourceFixtureRows{values: []any{[]byte{1}}}
	rows := wrapSQLServerSourceRows(
		source,
		schema.Table{Columns: []schema.Column{{
			Name: "payload",
			Type: "blob",
		}}},
		[]string{"payload"},
	)
	if rows != source {
		t.Fatal("non-converted SQL Server rows were unnecessarily wrapped")
	}
}

func sqlServerTemporalFixtureColumn(
	name string,
	columnType string,
	declaredBase string,
	arguments ...int,
) schema.Column {
	return schema.Column{
		Name: name,
		Type: columnType,
		DeclaredType: &schema.DeclaredType{
			Base:      declaredBase,
			Arguments: append([]int(nil), arguments...),
		},
	}
}

func scanSQLServerSourceFixture(
	column schema.Column,
	value any,
) error {
	source := &sqlServerSourceFixtureRows{values: []any{value}}
	rows := wrapSQLServerSourceRows(
		source,
		schema.Table{Columns: []schema.Column{column}},
		[]string{column.Name},
	)
	var got any
	return rows.Scan(&got)
}

type sqlServerSourceFixtureRows struct {
	values []any
}

func (rows *sqlServerSourceFixtureRows) Next() bool {
	return true
}

func (rows *sqlServerSourceFixtureRows) Scan(destinations ...any) error {
	if len(destinations) != len(rows.values) {
		return fmt.Errorf(
			"fixture destination count = %d, want %d",
			len(destinations),
			len(rows.values),
		)
	}
	for index, destination := range destinations {
		pointer, ok := destination.(*any)
		if !ok {
			return fmt.Errorf(
				"fixture destination %d has type %T",
				index,
				destination,
			)
		}
		*pointer = rows.values[index]
	}
	return nil
}

func (rows *sqlServerSourceFixtureRows) Err() error {
	return nil
}

func (rows *sqlServerSourceFixtureRows) Close() error {
	return nil
}
