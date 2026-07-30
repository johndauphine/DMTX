package migrate

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLTemporalRowsRejectZeroPartialZeroAndMalformedText(
	t *testing.T,
) {
	tests := []struct {
		name       string
		columnType string
		value      any
	}{
		{"zero date", "date", "0000-00-00"},
		{"zero month date", "date", []byte("2010-00-00")},
		{"zero day date", "date", "2010-01-00"},
		{"zero datetime", "datetime", "0000-00-00 00:00:00"},
		{"zero month datetime", "datetime", "2010-00-00 12:34:56"},
		{"zero day datetime", "datetime", []byte("2010-01-00 12:34:56")},
		{"partial zero timestamp", "timestamp", "2010-00-01 00:00:00"},
		{"malformed timestamp", "timestamp", "2010-01-01 24:00:00"},
		{"excess fractional precision", "datetime", "2010-01-01 00:00:00.1234567"},
		{"driver zero time", "date", time.Time{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scanMySQLTemporalFixture(
				test.columnType,
				test.value,
			)
			want := "MySQL source column occurred_at contains an invalid " +
				test.columnType + " value"
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			if strings.Contains(err.Error(), "PostgreSQL") {
				t.Fatalf("error is target-specific: %v", err)
			}
			raw := mysqlTemporalFixtureText(test.value)
			if raw != "" && strings.Contains(err.Error(), raw) {
				t.Fatalf("error leaked source value: %v", err)
			}
		})
	}
}

func TestMySQLTemporalRowsParseValidTextAndPreserveNull(t *testing.T) {
	predecoded := time.Date(
		2026,
		time.July,
		29,
		8,
		30,
		0,
		0,
		time.UTC,
	)
	tests := []struct {
		name       string
		columnType string
		value      any
		want       time.Time
		wantNull   bool
	}{
		{
			name:       "date text",
			columnType: "date",
			value:      "2010-01-02",
			want:       time.Date(2010, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "datetime fractional bytes",
			columnType: "datetime",
			value:      []byte("2010-01-02 03:04:05.123456"),
			want: time.Date(
				2010, 1, 2, 3, 4, 5, 123456000, time.UTC,
			),
		},
		{
			name:       "timestamp text",
			columnType: "timestamp",
			value:      "2010-01-02 03:04:05",
			want: time.Date(
				2010, 1, 2, 3, 4, 5, 0, time.UTC,
			),
		},
		{
			name:       "predecoded finite",
			columnType: "datetime",
			value:      predecoded,
			want:       predecoded,
		},
		{
			name:       "null",
			columnType: "datetime",
			wantNull:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := scanMySQLTemporalFixture(
				test.columnType,
				test.value,
			)
			if err != nil {
				t.Fatalf("value type %T: %v", test.value, err)
			}
			if test.wantNull {
				if got != nil {
					t.Fatalf("NULL became %#v", got)
				}
				return
			}
			if !got.(time.Time).Equal(test.want) {
				t.Fatalf("finite temporal = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestMySQLTemporalRowsPreserveExactClockTime(t *testing.T) {
	tests := []struct {
		name      string
		precision int
		value     any
		want      any
	}{
		{
			name:      "precision zero text",
			precision: 0,
			value:     "00:00:00",
			want:      "00:00:00",
		},
		{
			name:      "fractional bytes",
			precision: 6,
			value:     []byte("23:59:59.123456"),
			want:      "23:59:59.123456",
		},
		{
			name:      "null",
			precision: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := scanMySQLTimeFixture(
				test.precision,
				test.value,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("TIME value = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestMySQLTemporalRowsRejectNoncanonicalOrDurationTime(t *testing.T) {
	tests := []struct {
		name      string
		precision int
		value     any
	}{
		{"missing fraction", 3, "12:34:56"},
		{"short fraction", 3, "12:34:56.12"},
		{"excess fraction", 3, "12:34:56.1234"},
		{"fraction at precision zero", 0, "12:34:56.0"},
		{"single-digit hour", 0, "1:02:03"},
		{"end of day", 0, "24:00:00"},
		{"duration hour", 3, "100:02:03.123"},
		{"negative duration", 3, "-01:02:03.123"},
		{"positive sign", 3, "+01:02:03.123"},
		{"invalid minute", 3, "12:60:00.123"},
		{"invalid second", 3, "12:00:60.123"},
		{"unexpected time value", 0, time.Date(
			2026, time.July, 30, 12, 34, 56, 0, time.UTC,
		)},
		{"invalid UTF-8", 0, []byte{0xff, 0xfe}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scanMySQLTimeFixture(
				test.precision,
				test.value,
			)
			const want = "MySQL source column local_time contains an invalid time value"
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			raw := mysqlTemporalFixtureText(test.value)
			if raw != "" && strings.Contains(err.Error(), raw) {
				t.Fatalf("error leaked source value: %v", err)
			}
		})
	}
}

func TestMySQLTemporalRowsRejectTimeWithoutExactDeclaredPrecision(
	t *testing.T,
) {
	tests := []struct {
		name     string
		declared *schema.DeclaredType
	}{
		{name: "missing declaration"},
		{
			name:     "missing precision",
			declared: &schema.DeclaredType{Base: "time"},
		},
		{
			name: "wrong base",
			declared: &schema.DeclaredType{
				Base:      "datetime",
				Arguments: []int{0},
			},
		},
		{
			name: "excess precision",
			declared: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{7},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scanMySQLTemporalColumnFixture(
				schema.Column{
					Name:         "local_time",
					Type:         "time",
					DeclaredType: test.declared,
				},
				"12:34:56",
			)
			if err == nil {
				t.Fatal("expected TIME declaration to fail closed")
			}
		})
	}
}

func TestMySQLTemporalRowsWrapOnlyTemporalColumns(t *testing.T) {
	source := &mysqlTemporalFixtureRows{value: time.Time{}}
	rows := wrapMySQLSourceRows(
		source,
		schema.Table{Columns: []schema.Column{{
			Name: "payload",
			Type: "text",
		}}},
		[]string{"payload"},
	)
	if rows != source {
		t.Fatal("non-temporal MySQL rows were unnecessarily wrapped")
	}
	var value any
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("non-temporal zero time: %v", err)
	}
	if !value.(time.Time).IsZero() {
		t.Fatalf("non-temporal value = %#v", value)
	}
}

func scanMySQLTemporalFixture(
	columnType string,
	value any,
) (any, error) {
	return scanMySQLTemporalColumnFixture(
		schema.Column{
			Name: "occurred_at",
			Type: columnType,
		},
		value,
	)
}

func scanMySQLTimeFixture(
	precision int,
	value any,
) (any, error) {
	return scanMySQLTemporalColumnFixture(
		schema.Column{
			Name: "local_time",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{precision},
			},
		},
		value,
	)
}

func scanMySQLTemporalColumnFixture(
	column schema.Column,
	value any,
) (any, error) {
	source := &mysqlTemporalFixtureRows{value: value}
	rows := wrapMySQLSourceRows(
		source,
		schema.Table{Columns: []schema.Column{column}},
		[]string{column.Name},
	)
	var got any
	err := rows.Scan(&got)
	return got, err
}

func mysqlTemporalFixtureText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

type mysqlTemporalFixtureRows struct {
	value any
}

func (rows *mysqlTemporalFixtureRows) Next() bool {
	return true
}

func (rows *mysqlTemporalFixtureRows) Scan(destinations ...any) error {
	if len(destinations) != 1 {
		return fmt.Errorf(
			"fixture destination count = %d",
			len(destinations),
		)
	}
	destination, ok := destinations[0].(*any)
	if !ok {
		return fmt.Errorf(
			"fixture destination has type %T",
			destinations[0],
		)
	}
	*destination = rows.value
	return nil
}

func (rows *mysqlTemporalFixtureRows) Err() error {
	return nil
}

func (rows *mysqlTemporalFixtureRows) Close() error {
	return nil
}
