package migrate

import (
	"bytes"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestNormalizePostgresIntegersChecksExactRanges(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      any
		want       any
	}{
		{"int32 minimum", "integer", "-2147483648", int32(math.MinInt32)},
		{"int32 maximum", "int4", []byte("2147483647"), int32(math.MaxInt32)},
		{"int64 minimum", "bigint", "-9223372036854775808", int64(math.MinInt64)},
		{"int64 maximum", "int8", []byte("9223372036854775807"), int64(math.MaxInt64)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizePostgresValue(test.columnType, test.value)
			if err != nil {
				t.Fatalf("normalizePostgresValue: %v", err)
			}
			if got != test.want {
				t.Fatalf("value = %#v (%T), want %#v (%T)", got, got, test.want, test.want)
			}
		})
	}

	for _, test := range []struct {
		columnType string
		value      any
	}{
		{"integer", "2147483648"},
		{"integer", "-2147483649"},
		{"bigint", uint64(math.MaxUint64)},
		{"bigint", "1.0"},
		{"bigint", " 1"},
	} {
		if _, err := normalizePostgresValue(
			test.columnType,
			test.value,
		); err == nil {
			t.Fatalf(
				"normalizePostgresValue(%q, %#v) unexpectedly succeeded",
				test.columnType,
				test.value,
			)
		}
	}
}

func TestNormalizePostgresNumericNeverUsesFloatConversion(t *testing.T) {
	const exact = "-1234567890123456789012345678.1234567890"
	value, err := normalizePostgresValue("numeric", []byte(exact))
	if err != nil {
		t.Fatalf("normalizePostgresValue: %v", err)
	}
	numeric, ok := value.(pgtype.Numeric)
	if !ok {
		t.Fatalf("numeric type = %T", value)
	}
	if !numeric.Valid ||
		numeric.Int.String() != "-12345678901234567890123456781234567890" ||
		numeric.Exp != -10 {
		t.Fatalf("numeric = %#v", numeric)
	}

	integerValue, err := normalizePostgresValue(
		"decimal",
		int64(9007199254740993),
	)
	if err != nil {
		t.Fatalf("integer numeric: %v", err)
	}
	integerNumeric := integerValue.(pgtype.Numeric)
	if integerNumeric.Int.String() != "9007199254740993" ||
		integerNumeric.Exp != 0 {
		t.Fatalf("integer numeric = %#v", integerNumeric)
	}

	for _, invalid := range []any{
		float64(1.25),
		"1e3",
		"Infinity",
		"1 2",
	} {
		if _, err := normalizePostgresValue("numeric", invalid); err == nil {
			t.Fatalf("numeric %#v unexpectedly succeeded", invalid)
		}
	}
}

func TestNormalizePostgresNumericNaNFidelity(t *testing.T) {
	inputs := []any{
		"NaN",
		[]byte("NaN"),
		pgtype.Numeric{NaN: true, Valid: true},
	}
	for _, input := range inputs {
		value, err := normalizePostgresValue("numeric", input)
		if err != nil {
			t.Fatalf("normalize numeric NaN from %T: %v", input, err)
		}
		numeric, ok := value.(pgtype.Numeric)
		if !ok {
			t.Fatalf("numeric NaN type = %T", value)
		}
		if !numeric.Valid ||
			!numeric.NaN ||
			numeric.Int != nil ||
			numeric.Exp != 0 ||
			numeric.InfinityModifier != pgtype.Finite {
			t.Fatalf("numeric NaN = %#v", numeric)
		}
	}

	invalid := []any{
		"nan",
		"+NaN",
		"-NaN",
		"Infinity",
		"+Infinity",
		"-Infinity",
		pgtype.Numeric{
			NaN: true,
		},
		pgtype.Numeric{
			Int:   big.NewInt(1),
			NaN:   true,
			Valid: true,
		},
		pgtype.Numeric{
			Exp:   -1,
			NaN:   true,
			Valid: true,
		},
		pgtype.Numeric{
			NaN:              true,
			InfinityModifier: pgtype.Infinity,
			Valid:            true,
		},
		pgtype.Numeric{
			InfinityModifier: pgtype.Infinity,
			Valid:            true,
		},
		pgtype.Numeric{
			InfinityModifier: pgtype.NegativeInfinity,
			Valid:            true,
		},
	}
	for _, input := range invalid {
		if _, err := normalizePostgresValue(
			"numeric",
			input,
		); err == nil {
			t.Fatalf("numeric special input %T unexpectedly succeeded", input)
		}
	}

	const secret = "not-a-number-secret-marker"
	_, err := normalizePostgresRows(
		schema.Table{
			Schema: "public",
			Name:   "events",
			Columns: []schema.Column{
				{Name: "amount", Type: "numeric"},
			},
		},
		[]string{"amount"},
		[][]any{{secret}},
	)
	if err == nil {
		t.Fatal("invalid secret numeric unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("numeric normalization error leaked row value: %v", err)
	}
}

func TestNormalizePostgresNumericFitsDecimal38Scale10Exactly(t *testing.T) {
	const maximum = "9999999999999999999999999999.9999999999"
	for _, boundary := range []struct {
		source      string
		coefficient string
	}{
		{maximum, "99999999999999999999999999999999999999"},
		{"-" + maximum, "-99999999999999999999999999999999999999"},
	} {
		value, err := normalizePostgresValue("numeric", boundary.source)
		if err != nil {
			t.Fatalf("boundary DECIMAL(38,10): %v", err)
		}
		numeric := value.(pgtype.Numeric)
		if numeric.Int.String() != boundary.coefficient ||
			numeric.Exp != -10 {
			t.Fatalf("boundary numeric = %#v", numeric)
		}
	}

	value, err := normalizePostgresValue(
		"decimal",
		"1.123456789000",
	)
	if err != nil {
		t.Fatalf("exact trailing-zero fit: %v", err)
	}
	numeric := value.(pgtype.Numeric)
	if numeric.Int.String() != "11234567890" || numeric.Exp != -10 {
		t.Fatalf("trailing-zero numeric = %#v", numeric)
	}

	value, err = normalizePostgresValue("numeric", pgtype.Numeric{
		Int:   big.NewInt(12345678900),
		Exp:   -12,
		Valid: true,
	})
	if err != nil {
		t.Fatalf("pgtype trailing-zero fit: %v", err)
	}
	numeric = value.(pgtype.Numeric)
	if numeric.Int.String() != "123456789" || numeric.Exp != -10 {
		t.Fatalf("pgtype trailing-zero numeric = %#v", numeric)
	}

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "text excess scale",
			value: "0.00000000001",
			want:  "DECIMAL(38,10) scale",
		},
		{
			name:  "text excess integer precision",
			value: "10000000000000000000000000000",
			want:  "DECIMAL(38,10) integer precision",
		},
		{
			name: "pgtype excess scale",
			value: pgtype.Numeric{
				Int:   big.NewInt(1),
				Exp:   -11,
				Valid: true,
			},
			want: "DECIMAL(38,10) scale",
		},
		{
			name: "pgtype excess integer precision",
			value: pgtype.Numeric{
				Int:   big.NewInt(1),
				Exp:   28,
				Valid: true,
			},
			want: "DECIMAL(38,10) integer precision",
		},
		{
			name: "pgtype malformed",
			value: pgtype.Numeric{
				Valid: true,
			},
			want: "finite exact numeric",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := normalizePostgresValue(
				"numeric",
				test.value,
			); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestNormalizePostgresUUIDAcceptsTextAndRawBytes(t *testing.T) {
	const text = "00112233-4455-6677-8899-aabbccddeeff"
	value, err := normalizePostgresValue("uuid", text)
	if err != nil {
		t.Fatalf("text UUID: %v", err)
	}
	if got := value.(pgtype.UUID).String(); got != text {
		t.Fatalf("UUID = %q, want %q", got, text)
	}

	raw := []byte{
		0x00, 0x11, 0x22, 0x33,
		0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xaa, 0xbb,
		0xcc, 0xdd, 0xee, 0xff,
	}
	value, err = normalizePostgresValue("uuid", raw)
	if err != nil {
		t.Fatalf("raw UUID: %v", err)
	}
	if got := value.(pgtype.UUID).String(); got != text {
		t.Fatalf("raw UUID = %q, want %q", got, text)
	}

	for _, invalid := range []any{
		"not-a-uuid",
		[]byte{0xff, 0xfe},
		int64(1),
		pgtype.UUID{},
	} {
		if _, err := normalizePostgresValue("uuid", invalid); err == nil {
			t.Fatalf("UUID %#v unexpectedly succeeded", invalid)
		}
	}
}

func TestNormalizePostgresTextJSONAndByteaAreStrictAndOwned(t *testing.T) {
	text, err := normalizePostgresValue("text", []byte("hello"))
	if err != nil || text != "hello" {
		t.Fatalf("text = %#v, error = %v", text, err)
	}
	if _, err := normalizePostgresValue(
		"text",
		[]byte{0xff},
	); err == nil {
		t.Fatal("invalid UTF-8 text unexpectedly succeeded")
	}
	for _, invalid := range []any{
		"before\x00after",
		[]byte{'b', 'e', 'f', 'o', 'r', 'e', 0, 'a', 'f', 't', 'e', 'r'},
	} {
		if _, err := normalizePostgresValue(
			"text",
			invalid,
		); err == nil || !strings.Contains(err.Error(), "cannot contain NUL") {
			t.Fatalf("NUL text error = %v", err)
		}
	}

	document := []byte(`{"key":"value"}`)
	normalizedJSON, err := normalizePostgresValue("jsonb", document)
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	jsonBytes := normalizedJSON.([]byte)
	document[2] = 'X'
	if !bytes.Equal(jsonBytes, []byte(`{"key":"value"}`)) {
		t.Fatalf("normalized JSON aliases input: %q", jsonBytes)
	}
	for _, invalid := range []any{
		`{"missing":`,
		[]byte{'"', 0xff, '"'},
		int64(1),
	} {
		if _, err := normalizePostgresValue("json", invalid); err == nil {
			t.Fatalf("JSON %#v unexpectedly succeeded", invalid)
		}
	}

	binary := []byte{0, 1, 2, 3}
	normalizedBinary, err := normalizePostgresValue("bytea", binary)
	if err != nil {
		t.Fatalf("bytea: %v", err)
	}
	binaryCopy := normalizedBinary.([]byte)
	binary[0] = 9
	if !bytes.Equal(binaryCopy, []byte{0, 1, 2, 3}) {
		t.Fatalf("normalized bytea aliases input: %#v", binaryCopy)
	}

	empty := make([]byte, 0)
	normalizedEmpty, err := normalizePostgresValue("bytea", empty)
	if err != nil {
		t.Fatalf("empty bytea: %v", err)
	}
	emptyCopy, ok := normalizedEmpty.([]byte)
	if !ok || emptyCopy == nil || len(emptyCopy) != 0 {
		t.Fatalf("normalized empty bytea = %#v (%T)", normalizedEmpty, normalizedEmpty)
	}

	if _, err := normalizePostgresValue("bytea", "not bytes"); err == nil {
		t.Fatal("string bytea unexpectedly succeeded")
	}
}

func TestNormalizePostgresBooleanIsStrict(t *testing.T) {
	tests := []struct {
		value any
		want  bool
	}{
		{true, true},
		{false, false},
		{int64(1), true},
		{uint8(0), false},
		{"true", true},
		{"0", false},
		{[]byte("false"), false},
		{[]byte{1}, true},
	}
	for _, test := range tests {
		value, err := normalizePostgresValue("boolean", test.value)
		if err != nil {
			t.Fatalf("boolean %#v: %v", test.value, err)
		}
		if value != test.want {
			t.Fatalf("boolean %#v = %#v, want %v", test.value, value, test.want)
		}
	}
	for _, invalid := range []any{
		int64(2),
		"TRUE",
		"yes",
		[]byte{2},
		float64(1),
	} {
		if _, err := normalizePostgresValue("boolean", invalid); err == nil {
			t.Fatalf("boolean %#v unexpectedly succeeded", invalid)
		}
	}
}

func TestNormalizePostgresFloatPreservesSpecialValues(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		wantNaN bool
		wantInf int
		want    float64
	}{
		{name: "finite", value: "1.25", want: 1.25},
		{name: "stdlib NaN", value: math.NaN(), wantNaN: true},
		{name: "stdlib positive infinity", value: math.Inf(1), wantInf: 1},
		{name: "stdlib negative infinity", value: math.Inf(-1), wantInf: -1},
		{
			name:    "positive infinity float32",
			value:   float32(math.Inf(1)),
			wantInf: 1,
		},
		{name: "negative infinity bytes", value: []byte("-Infinity"), wantInf: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := normalizePostgresValue("double precision", test.value)
			if err != nil {
				t.Fatalf("normalizePostgresValue: %v", err)
			}
			number := value.(float64)
			switch {
			case test.wantNaN && !math.IsNaN(number):
				t.Fatalf("number = %v, want NaN", number)
			case test.wantInf != 0 && !math.IsInf(number, test.wantInf):
				t.Fatalf("number = %v, want infinity sign %d", number, test.wantInf)
			case !test.wantNaN && test.wantInf == 0 && number != test.want:
				t.Fatalf("number = %v, want %v", number, test.want)
			}
		})
	}

	const secret = "do-not-leak-malformed-float"
	for _, invalid := range []any{
		secret,
		[]byte("1.2.3"),
		int64(1),
	} {
		_, err := normalizePostgresValue("double precision", invalid)
		if err == nil {
			t.Fatalf("malformed float type %T unexpectedly succeeded", invalid)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("float error leaked source value: %v", err)
		}
	}
}

func TestNormalizePostgresRealRequiresExactFloat32(t *testing.T) {
	want := float32(0.1)
	for _, source := range []any{want, float64(want)} {
		value, err := normalizePostgresValue("real", source)
		if err != nil {
			t.Fatalf("normalize REAL %T: %v", source, err)
		}
		if got := value.(float32); got != want {
			t.Fatalf("REAL = %v, want %v", got, want)
		}
	}
	for _, source := range []any{float64(0.1), "0.1"} {
		if _, err := normalizePostgresValue(
			"real",
			source,
		); err == nil {
			t.Fatalf("inexact REAL %T was accepted", source)
		}
	}
}

func TestNormalizePostgresTemporalValuesAcceptValidGoZeroTime(t *testing.T) {
	timestamp := time.Date(
		2026,
		time.July,
		28,
		21,
		15,
		30,
		123000000,
		time.UTC,
	)
	if value, err := normalizePostgresValue(
		"timestamp",
		timestamp,
	); err != nil ||
		!value.(pgtype.Timestamp).Valid ||
		!value.(pgtype.Timestamp).Time.Equal(timestamp) {
		t.Fatalf("timestamp = %#v, error = %v", value, err)
	}
	if value, err := normalizePostgresValue(
		"timestamp",
		time.Time{},
	); err != nil ||
		!value.(pgtype.Timestamp).Valid ||
		!value.(pgtype.Timestamp).Time.IsZero() {
		t.Fatalf("valid year-one timestamp = %#v, error = %v", value, err)
	}
	if value, err := normalizePostgresValue(
		"timestamptz",
		timestamp,
	); err != nil ||
		!value.(pgtype.Timestamptz).Valid ||
		!value.(pgtype.Timestamptz).Time.Equal(timestamp) {
		t.Fatalf("timestamptz = %#v, error = %v", value, err)
	}
	if value, err := normalizePostgresValue(
		"datetime",
		"2026-07-28 21:15:30",
	); err != nil ||
		!value.(pgtype.Timestamp).Valid ||
		value.(pgtype.Timestamp).Time.IsZero() {
		t.Fatalf("text timestamp = %#v, error = %v", value, err)
	}
	if value, err := normalizePostgresValue(
		"date",
		"2026-07-28",
	); err != nil ||
		!value.(pgtype.Date).Valid ||
		value.(pgtype.Date).Time.Format("2006-01-02") != "2026-07-28" {
		t.Fatalf("date = %#v, error = %v", value, err)
	}
	if value, err := normalizePostgresValue(
		"date",
		time.Time{},
	); err != nil ||
		!value.(pgtype.Date).Valid ||
		!value.(pgtype.Date).Time.IsZero() {
		t.Fatalf("valid year-one date = %#v, error = %v", value, err)
	}
	for _, invalid := range []struct {
		columnType string
		value      string
	}{
		{"date", "0000-00-00"},
		{"datetime", "0000-00-00 00:00:00"},
	} {
		if _, err := normalizePostgresValue(
			invalid.columnType,
			invalid.value,
		); err == nil {
			t.Fatalf(
				"invalid %s %q unexpectedly succeeded",
				invalid.columnType,
				invalid.value,
			)
		}
	}
}

func TestNormalizePostgresTemporalInfinityUsesMatchingPGXType(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      any
		want       pgtype.InfinityModifier
	}{
		{
			name:       "date stdlib positive",
			columnType: "date",
			value:      "infinity",
			want:       pgtype.Infinity,
		},
		{
			name:       "date pgtype negative",
			columnType: "date",
			value: pgtype.Date{
				InfinityModifier: pgtype.NegativeInfinity,
				Valid:            true,
			},
			want: pgtype.NegativeInfinity,
		},
		{
			name:       "timestamp stdlib negative",
			columnType: "timestamp",
			value:      "-infinity",
			want:       pgtype.NegativeInfinity,
		},
		{
			name:       "timestamp pgtype positive",
			columnType: "timestamp",
			value: pgtype.Timestamp{
				InfinityModifier: pgtype.Infinity,
				Valid:            true,
			},
			want: pgtype.Infinity,
		},
		{
			name:       "timestamptz stdlib positive",
			columnType: "timestamptz",
			value:      "infinity",
			want:       pgtype.Infinity,
		},
		{
			name:       "timestamptz pgtype negative",
			columnType: "timestamptz",
			value: pgtype.Timestamptz{
				InfinityModifier: pgtype.NegativeInfinity,
				Valid:            true,
			},
			want: pgtype.NegativeInfinity,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := normalizePostgresValue(
				test.columnType,
				test.value,
			)
			if err != nil {
				t.Fatalf("normalizePostgresValue: %v", err)
			}
			var got pgtype.InfinityModifier
			switch typed := value.(type) {
			case pgtype.Date:
				if test.columnType != "date" {
					t.Fatalf("date returned for %s", test.columnType)
				}
				got = typed.InfinityModifier
			case pgtype.Timestamp:
				if test.columnType != "timestamp" {
					t.Fatalf("timestamp returned for %s", test.columnType)
				}
				got = typed.InfinityModifier
			case pgtype.Timestamptz:
				if test.columnType != "timestamptz" {
					t.Fatalf("timestamptz returned for %s", test.columnType)
				}
				got = typed.InfinityModifier
			default:
				t.Fatalf("normalized type = %T", value)
			}
			if got != test.want {
				t.Fatalf("infinity modifier = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNormalizePostgresTemporalRejectsMalformedAndCrossTypedValues(
	t *testing.T,
) {
	const secret = "do-not-leak-malformed-temporal"
	tests := []struct {
		name       string
		columnType string
		value      any
	}{
		{"malformed date", "date", secret},
		{"malformed timestamp", "timestamp", secret},
		{"malformed timestamptz", "timestamptz", secret},
		{
			"date given timestamp",
			"date",
			pgtype.Timestamp{Time: time.Now(), Valid: true},
		},
		{
			"timestamp given date",
			"timestamp",
			pgtype.Date{Time: time.Now(), Valid: true},
		},
		{
			"timestamptz given timestamp",
			"timestamptz",
			pgtype.Timestamp{Time: time.Now(), Valid: true},
		},
		{
			"invalid date",
			"date",
			pgtype.Date{},
		},
		{
			"invalid timestamp modifier",
			"timestamp",
			pgtype.Timestamp{
				InfinityModifier: pgtype.InfinityModifier(2),
				Valid:            true,
			},
		},
		{
			"invalid timestamptz",
			"timestamptz",
			pgtype.Timestamptz{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizePostgresValue(
				test.columnType,
				test.value,
			)
			if err == nil {
				t.Fatal("malformed temporal unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("temporal error leaked source value: %v", err)
			}
		})
	}
}

func TestNormalizePostgresRowsValidatesWidthAndNullability(t *testing.T) {
	table := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "integer"},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
	rows, err := normalizePostgresRows(
		table,
		[]string{"id", "note"},
		[][]any{{int64(1), nil}},
	)
	if err != nil {
		t.Fatalf("normalizePostgresRows: %v", err)
	}
	if rows[0][1] != nil {
		t.Fatalf("nullable value = %#v, want nil", rows[0][1])
	}

	if _, err := normalizePostgresRows(
		table,
		[]string{"id"},
		[][]any{{nil}},
	); err == nil || !strings.Contains(err.Error(), "NULL is not allowed") {
		t.Fatalf("non-null error = %v", err)
	}
	if _, err := normalizePostgresRows(
		table,
		[]string{"id", "note"},
		[][]any{{int64(1)}},
	); err == nil || !strings.Contains(err.Error(), "got 1 values for 2 columns") {
		t.Fatalf("width error = %v", err)
	}
}
