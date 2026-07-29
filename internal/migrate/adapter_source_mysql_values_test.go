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
	source := &mysqlTemporalFixtureRows{value: value}
	rows := wrapMySQLSourceRows(
		source,
		schema.Table{Columns: []schema.Column{{
			Name: "occurred_at",
			Type: columnType,
		}}},
		[]string{"occurred_at"},
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
