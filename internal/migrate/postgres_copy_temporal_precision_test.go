package migrate

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizePostgresTimestampTextRequiresZoneLessInput(
	t *testing.T,
) {
	for _, source := range []any{
		"2026-07-29 12:34:56.123456",
		"2026-07-29T12:34:56",
		[]byte("2026-07-29 12:34:56"),
	} {
		value, err := normalizePostgresValue("timestamp", source)
		if err != nil {
			t.Fatalf("zone-less timestamp %T: %v", source, err)
		}
		if !value.(pgtype.Timestamp).Valid {
			t.Fatalf("zone-less timestamp %T is invalid", source)
		}
	}

	for _, source := range []string{
		"2026-07-29T12:34:56Z",
		"2026-07-29T12:34:56-05:00",
		"2026-07-29 12:34:56.123456+02:30",
	} {
		_, err := normalizePostgresValue("timestamp", source)
		if err == nil {
			t.Fatalf("zoned timestamp %q unexpectedly succeeded", source)
		}
		if strings.Contains(err.Error(), source) {
			t.Fatalf("timestamp error leaked source text: %v", err)
		}
	}

	if _, err := normalizePostgresValue(
		"timestamptz",
		"2026-07-29T12:34:56-05:00",
	); err != nil {
		t.Fatalf("timezone-aware timestamptz: %v", err)
	}
}

func TestNormalizePostgresTimestampRequiresMicrosecondPrecision(
	t *testing.T,
) {
	const exact = "2026-07-29 12:34:56.123456"
	value, err := normalizePostgresValue("timestamp", exact)
	if err != nil {
		t.Fatalf("exact timestamp text: %v", err)
	}
	if got := value.(pgtype.Timestamp).Time.Nanosecond(); got != 123456000 {
		t.Fatalf("timestamp nanoseconds = %d, want 123456000", got)
	}

	exactTime := time.Date(
		2026,
		time.July,
		29,
		12,
		34,
		56,
		123456000,
		time.UTC,
	)
	if _, err := normalizePostgresValue(
		"timestamp",
		exactTime,
	); err != nil {
		t.Fatalf("exact timestamp time.Time: %v", err)
	}

	const subMicrosecond = "2026-07-29 12:34:56.123456789"
	_, err = normalizePostgresValue("timestamp", subMicrosecond)
	if err == nil {
		t.Fatal("sub-microsecond timestamp text unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), subMicrosecond) {
		t.Fatalf("timestamp precision error leaked source text: %v", err)
	}

	subMicrosecondTime := time.Date(
		2026,
		time.July,
		29,
		12,
		34,
		56,
		123456789,
		time.UTC,
	)
	if _, err := normalizePostgresValue(
		"timestamp",
		subMicrosecondTime,
	); err == nil {
		t.Fatal("sub-microsecond timestamp time.Time unexpectedly succeeded")
	}
	if _, err := normalizePostgresValue(
		"timestamp",
		pgtype.Timestamp{
			Time:  subMicrosecondTime,
			Valid: true,
		},
	); err == nil {
		t.Fatal("sub-microsecond pgtype.Timestamp unexpectedly succeeded")
	}
}

func TestNormalizePostgresTimestamptzRequiresMicrosecondPrecision(
	t *testing.T,
) {
	const exact = "2026-07-29T12:34:56.123456-05:00"
	value, err := normalizePostgresValue("timestamptz", exact)
	if err != nil {
		t.Fatalf("exact timestamptz text: %v", err)
	}
	if got := value.(pgtype.Timestamptz).Time.Nanosecond(); got != 123456000 {
		t.Fatalf("timestamptz nanoseconds = %d, want 123456000", got)
	}

	exactTime := time.Date(
		2026,
		time.July,
		29,
		12,
		34,
		56,
		123456000,
		time.FixedZone("fixture", -5*60*60),
	)
	if _, err := normalizePostgresValue(
		"timestamptz",
		exactTime,
	); err != nil {
		t.Fatalf("exact timestamptz time.Time: %v", err)
	}

	const subMicrosecond = "2026-07-29T12:34:56.123456789-05:00"
	_, err = normalizePostgresValue("timestamptz", subMicrosecond)
	if err == nil {
		t.Fatal("sub-microsecond timestamptz text unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), subMicrosecond) {
		t.Fatalf("timestamptz precision error leaked source text: %v", err)
	}

	subMicrosecondTime := time.Date(
		2026,
		time.July,
		29,
		12,
		34,
		56,
		123456789,
		time.FixedZone("fixture", -5*60*60),
	)
	if _, err := normalizePostgresValue(
		"timestamptz",
		subMicrosecondTime,
	); err == nil {
		t.Fatal("sub-microsecond timestamptz time.Time unexpectedly succeeded")
	}
	if _, err := normalizePostgresValue(
		"timestamptz",
		pgtype.Timestamptz{
			Time:  subMicrosecondTime,
			Valid: true,
		},
	); err == nil {
		t.Fatal("sub-microsecond pgtype.Timestamptz unexpectedly succeeded")
	}
}

func TestNormalizePostgresTimePreservesTimeOfDayAndPrecision(
	t *testing.T,
) {
	column := schema.Column{
		Name: "local_time",
		Type: "time",
		DeclaredType: &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{6},
		},
	}
	const want = int64(
		12*time.Hour/time.Microsecond +
			34*time.Minute/time.Microsecond +
			56*time.Second/time.Microsecond +
			123456,
	)
	for _, source := range []any{
		"12:34:56.123456",
		[]byte("12:34:56.123456"),
		time.Date(
			1,
			time.January,
			1,
			12,
			34,
			56,
			123456000,
			time.UTC,
		),
		pgtype.Time{Microseconds: want, Valid: true},
	} {
		value, err := normalizePostgresColumnValue(column, source)
		if err != nil {
			t.Fatalf("normalize time %T: %v", source, err)
		}
		got := value.(pgtype.Time)
		if !got.Valid || got.Microseconds != want {
			t.Fatalf("normalized time %T = %#v, want %d", source, got, want)
		}
	}

	for _, source := range []any{
		"12:34:56.123456789",
		"2026-07-29 12:34:56",
		"12:34:56Z",
		time.Date(
			2026,
			time.July,
			29,
			12,
			34,
			56,
			0,
			time.UTC,
		),
		time.Date(
			1,
			time.January,
			1,
			12,
			34,
			56,
			0,
			time.FixedZone("unsafe", 60*60),
		),
		pgtype.Time{Microseconds: -1, Valid: true},
		pgtype.Time{Microseconds: 0, Valid: false},
	} {
		if _, err := normalizePostgresColumnValue(
			column,
			source,
		); err == nil {
			t.Fatalf("unsafe time %T unexpectedly succeeded", source)
		}
	}

	column.DeclaredType.Arguments = []int{3}
	if _, err := normalizePostgresColumnValue(
		column,
		"12:34:56.123",
	); err != nil {
		t.Fatalf("millisecond time: %v", err)
	}
	if _, err := normalizePostgresColumnValue(
		column,
		"12:34:56.1234",
	); err == nil {
		t.Fatal("over-precise time unexpectedly fit time(3)")
	}
}

func TestNormalizePostgresTimestampHonorsDeclaredPrecision(t *testing.T) {
	column := schema.Column{
		Name: "occurred_at",
		Type: "timestamp",
		DeclaredType: &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		},
	}
	for _, value := range []any{
		"2026-07-29 12:34:56.123",
		time.Date(
			2026,
			time.July,
			29,
			12,
			34,
			56,
			123000000,
			time.UTC,
		),
	} {
		if _, err := normalizePostgresColumnValue(column, value); err != nil {
			t.Fatalf("millisecond value %T: %v", value, err)
		}
	}
	for _, value := range []any{
		"2026-07-29 12:34:56.1234",
		time.Date(
			2026,
			time.July,
			29,
			12,
			34,
			56,
			123400000,
			time.UTC,
		),
	} {
		if _, err := normalizePostgresColumnValue(column, value); err == nil {
			t.Fatalf("over-precise value %T unexpectedly succeeded", value)
		}
	}

	column.DeclaredType.Arguments = []int{0}
	if _, err := normalizePostgresColumnValue(
		column,
		"2026-07-29 12:34:56",
	); err != nil {
		t.Fatalf("whole-second value: %v", err)
	}
	if _, err := normalizePostgresColumnValue(
		column,
		"2026-07-29 12:34:56.1",
	); err == nil {
		t.Fatal("fractional value unexpectedly fit timestamp(0)")
	}
}
