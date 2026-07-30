package migrate

import (
	"database/sql"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestCanonicalValidationRowNormalizesDriverShapesBySemanticType(t *testing.T) {
	t.Parallel()

	descriptor := validationDescriptorFixture(t)
	instant := time.Date(2026, 7, 30, 17, 4, 5, 123456789, time.UTC)
	central := instant.In(time.FixedZone("central", -5*60*60))
	uuidBytes := []byte{
		0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}
	left, err := canonicalValidationRow(descriptor, []any{
		int8(7),
		[]byte("123.4500"),
		float32(1.5),
		[]byte("hello"),
		[]byte{0, 1, 2},
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.FixedZone("west", -8*60*60)),
		"12:03:04.12",
		instant,
		uuidBytes,
		int64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalValidationRow(descriptor, []any{
		uint64(7),
		"123.45",
		float64(float32(1.5)),
		"hello",
		sql.RawBytes{0, 1, 2},
		"2026-07-30",
		time.Date(1, 1, 1, 12, 3, 4, 120_000_000, time.UTC),
		central,
		"550e8400-e29b-41d4-a716-446655440000",
		true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Fatalf("equal cross-driver values differ:\nleft  %q\nright %q", left, right)
	}
}

func TestCanonicalValidationFloatUsesOneInjectiveDomain(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "value", Kind: validationFloat}},
	}
	float32Value := float32(0.1)
	fromFloat32, err := canonicalValidationRow(descriptor, []any{float32Value})
	if err != nil {
		t.Fatal(err)
	}
	promoted, err := canonicalValidationRow(descriptor, []any{float64(float32Value)})
	if err != nil {
		t.Fatal(err)
	}
	if string(fromFloat32) != string(promoted) {
		t.Fatal("float32 differs from the same value promoted to float64")
	}
	distinctFloat64, err := canonicalValidationRow(descriptor, []any{float64(0.1)})
	if err != nil {
		t.Fatal(err)
	}
	if string(fromFloat32) == string(distinctFloat64) {
		t.Fatal("distinct float32 and float64 values collided")
	}
}

func TestCanonicalValidationRowKeepsSemanticTypesCollisionFree(t *testing.T) {
	t.Parallel()

	values := []struct {
		kind  validationValueKind
		value any
	}{
		{validationText, ""},
		{validationBytes, []byte{}},
		{validationText, "null"},
		{validationBytes, []byte("null")},
		{validationBoolean, false},
		{validationInteger, int64(0)},
		{validationDecimal, "0"},
		{validationFloat, float64(0)},
		{validationText, "0"},
		{validationBytes, []byte("0")},
	}
	seen := make(map[string]struct{}, len(values)+1)
	nullDescriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "value", Kind: validationText}},
	}
	nullEncoded, err := canonicalValidationRow(nullDescriptor, []any{nil})
	if err != nil {
		t.Fatal(err)
	}
	seen[string(nullEncoded)] = struct{}{}
	for _, test := range values {
		descriptor := validationSampleDescriptor{
			Columns: []validationColumnDescriptor{{Name: "value", Kind: test.kind}},
		}
		encoded, err := canonicalValidationRow(descriptor, []any{test.value})
		if err != nil {
			t.Fatal(err)
		}
		key := string(encoded)
		if _, exists := seen[key]; exists {
			t.Fatalf("canonical validation collision for kind %s type %T", test.kind, test.value)
		}
		seen[key] = struct{}{}
	}
}

func TestCanonicalValidationRowLengthFramesCannotCollide(t *testing.T) {
	t.Parallel()

	twoColumns := validationSampleDescriptor{Columns: []validationColumnDescriptor{
		{Name: "left", Kind: validationText},
		{Name: "right", Kind: validationText},
	}}
	left, err := canonicalValidationRow(twoColumns, []any{"ab", "c"})
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalValidationRow(twoColumns, []any{"a", "bc"})
	if err != nil {
		t.Fatal(err)
	}
	if string(left) == string(right) {
		t.Fatal("different column boundaries produced the same canonical row")
	}

	oneColumn := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "value", Kind: validationText}},
	}
	framedPayload, err := canonicalValidationRow(oneColumn, []any{"a4:text1:b"})
	if err != nil {
		t.Fatal(err)
	}
	if string(left) == string(framedPayload) {
		t.Fatal("payload resembling a frame changed row boundaries")
	}
}

func TestCanonicalValidationRowPreservesLargeIntegersWithoutFloat(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "id", Kind: validationInteger}},
	}
	const aboveTwoToTheFiftyThird = uint64(9_007_199_254_740_993)
	unsigned, err := canonicalValidationRow(descriptor, []any{aboveTwoToTheFiftyThird})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := canonicalValidationRow(descriptor, []any{int64(aboveTwoToTheFiftyThird)})
	if err != nil {
		t.Fatal(err)
	}
	if string(unsigned) != string(signed) {
		t.Fatal("equal signed and unsigned large integers differ")
	}
	if !strings.Contains(string(unsigned), "9007199254740993") {
		t.Fatalf("large integer lost precision: %q", unsigned)
	}

	maximum, err := canonicalValidationRow(descriptor, []any{uint64(math.MaxUint64)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(maximum), "18446744073709551615") {
		t.Fatalf("uint64 maximum lost precision: %q", maximum)
	}
}

func TestCanonicalValidationRowNormalizesFloatSpecialValues(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "value", Kind: validationFloat}},
	}
	positiveZero, err := canonicalValidationRow(descriptor, []any{float64(0)})
	if err != nil {
		t.Fatal(err)
	}
	negativeZero, err := canonicalValidationRow(descriptor, []any{math.Copysign(0, -1)})
	if err != nil {
		t.Fatal(err)
	}
	if string(positiveZero) != string(negativeZero) {
		t.Fatal("positive and negative zero differ")
	}

	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := canonicalValidationRow(descriptor, []any{value}); err != nil {
			t.Fatalf("canonicalize %v: %v", value, err)
		}
	}
}

func TestValidationSampleDescriptorRequiresCompletePrimaryKeyAndProjection(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Name: "items",
		Columns: []schema.Column{
			{Name: "tenant_id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "item_id", Type: "uuid", PrimaryKey: true, PrimaryKeyPosition: 2},
			{Name: "note", Type: "text"},
		},
	}
	descriptor, err := newValidationSampleDescriptor(
		table,
		[]string{"tenant_id", "item_id", "note"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptor.Columns) != 3 ||
		descriptor.Columns[1].Kind != validationUUID {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	for _, test := range []struct {
		name       string
		projection []string
		want       string
	}{
		{name: "missing key", projection: []string{"tenant_id", "note"}, want: "omits primary-key"},
		{name: "missing payload", projection: []string{"tenant_id", "item_id"}, want: "omits transfer columns note"},
		{name: "duplicate", projection: []string{"tenant_id", "item_id", "item_id"}, want: "duplicates"},
		{name: "unknown", projection: []string{"tenant_id", "item_id", "missing"}, want: "unknown column"},
		{name: "empty", want: "is empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newValidationSampleDescriptor(table, test.projection); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	if _, err := canonicalValidationRow(descriptor, []any{int64(1)}); err == nil ||
		!strings.Contains(err.Error(), "values for") {
		t.Fatalf("row width error = %v", err)
	}
}

func TestCanonicalValidationSQLiteANYPreservesRuntimeStorageClass(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "value", Kind: validationDynamic}},
	}
	values := []any{nil, int64(1), float64(1), "1", []byte("1")}
	encodings := make(map[string]struct{}, len(values))
	for _, value := range values {
		encoded, err := canonicalValidationRow(descriptor, []any{value})
		if err != nil {
			t.Fatalf("canonicalize %T: %v", value, err)
		}
		key := string(encoded)
		if _, collision := encodings[key]; collision {
			t.Fatalf("runtime storage class %T collided", value)
		}
		encodings[key] = struct{}{}
	}

	if _, err := canonicalValidationRow(descriptor, []any{true}); err == nil ||
		!strings.Contains(err.Error(), "unexpected dynamic validation value type bool") {
		t.Fatalf("ambiguous dynamic boolean error = %v", err)
	}
	if _, err := canonicalValidationRow(
		descriptor,
		[]any{sql.RawBytes("ambiguous")},
	); err == nil || !strings.Contains(err.Error(), "unexpected dynamic") {
		t.Fatalf("ambiguous RawBytes error = %v", err)
	}
}

func TestValidationKindSupportsSQLiteANY(t *testing.T) {
	t.Parallel()

	kind, err := validationKindForColumn(schema.Column{Name: "value", Type: "ANY"})
	if err != nil {
		t.Fatal(err)
	}
	if kind != validationDynamic {
		t.Fatalf("kind = %q, want %q", kind, validationDynamic)
	}
}

func TestValidationSampleDescriptorRejectsMalformedPrimaryKeyMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		columns []schema.Column
		want    string
	}{
		{
			name: "position without membership",
			columns: []schema.Column{
				{Name: "id", Type: "integer", PrimaryKeyPosition: 1},
			},
			want: "non-primary-key",
		},
		{
			name: "membership without position",
			columns: []schema.Column{
				{Name: "id", Type: "integer", PrimaryKey: true},
			},
			want: "no positive position",
		},
		{
			name: "duplicate position",
			columns: []schema.Column{
				{Name: "tenant", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 1},
				{Name: "id", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 1},
			},
			want: "position 1 is shared",
		},
		{
			name: "position gap",
			columns: []schema.Column{
				{Name: "tenant", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 1},
				{Name: "id", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 3},
			},
			want: "not contiguous",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := schema.Table{Name: "items", Columns: test.columns}
			projection := make([]string, len(test.columns))
			for index, column := range test.columns {
				projection[index] = column.Name
			}
			_, err := newValidationSampleDescriptor(table, projection)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCanonicalValidationDecimalRejectsNonSQLRationalSyntax(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "amount", Kind: validationDecimal}},
	}
	half, err := canonicalValidationRow(descriptor, []any{"0.5"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalValidationRow(descriptor, []any{"1/2"}); err == nil {
		t.Fatal("non-SQL rational syntax was accepted")
	}
	exponent, err := canonicalValidationRow(descriptor, []any{"5e-1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(half) != string(exponent) {
		t.Fatal("equal admitted decimal exponent syntax did not normalize")
	}
}

func TestCanonicalValidationUUIDRequiresCanonicalHyphenPositions(t *testing.T) {
	t.Parallel()

	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "id", Kind: validationUUID}},
	}
	valid, err := canonicalValidationRow(
		descriptor,
		[]any{"550e8400-e29b-41d4-a716-446655440000"},
	)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := canonicalValidationRow(
		descriptor,
		[]any{"550e8400e29b41d4a716446655440000"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(valid) != string(compact) {
		t.Fatal("canonical and compact UUID forms differ")
	}
	if _, err := canonicalValidationRow(
		descriptor,
		[]any{"5-50e8400e29b41d4a716446655440000"},
	); err == nil {
		t.Fatal("misplaced UUID hyphen was accepted")
	}
}

func TestValidationKindCoversSupportedSQLiteDeclaredAliases(t *testing.T) {
	t.Parallel()

	tests := map[string]validationValueKind{
		"tinyint":           validationInteger,
		"mediumint":         validationInteger,
		"int2":              validationInteger,
		"unsigned big int":  validationInteger,
		"character":         validationText,
		"character varying": validationText,
		"nvarchar":          validationText,
		"clob":              validationText,
	}
	for declared, want := range tests {
		declared, want := declared, want
		t.Run(declared, func(t *testing.T) {
			t.Parallel()
			got, err := validationKindForColumn(schema.Column{Type: declared})
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("kind = %q, want %q", got, want)
			}
		})
	}
}

func TestCanonicalValidationDateAndTimeRejectDiscardedComponents(t *testing.T) {
	t.Parallel()

	date := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "day", Kind: validationDate}},
	}
	if _, err := canonicalValidationRow(
		date,
		[]any{time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC)},
	); err == nil {
		t.Fatal("DATE silently discarded a clock component")
	}

	clock := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "clock", Kind: validationTime}},
	}
	if _, err := canonicalValidationRow(
		clock,
		[]any{time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC)},
	); err == nil {
		t.Fatal("TIME silently discarded a date component")
	}
}

func TestCanonicalValidationRowRejectsUnexpectedShapeWithoutValue(t *testing.T) {
	t.Parallel()

	type sentinel struct{ Secret string }
	const secret = "STAGE4-ROW-SECRET"
	descriptor := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "payload", Kind: validationBytes}},
	}
	_, err := canonicalValidationRow(descriptor, []any{sentinel{Secret: secret}})
	if err == nil {
		t.Fatal("unsupported value shape was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("canonicalization error leaked row content: %v", err)
	}
	if !strings.Contains(err.Error(), "migrate.sentinel") {
		t.Fatalf("canonicalization error lacks safe type context: %v", err)
	}
}

func TestCanonicalValidationRowRejectsTextBinaryConfusion(t *testing.T) {
	t.Parallel()

	binary := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "payload", Kind: validationBytes}},
	}
	if _, err := canonicalValidationRow(binary, []any{"not binary"}); err == nil {
		t.Fatal("string was accepted for a binary column")
	}
	text := validationSampleDescriptor{
		Columns: []validationColumnDescriptor{{Name: "payload", Kind: validationText}},
	}
	if _, err := canonicalValidationRow(text, []any{[]byte{0xff}}); err == nil {
		t.Fatal("invalid UTF-8 was accepted for a text column")
	}
}

func validationDescriptorFixture(t *testing.T) validationSampleDescriptor {
	t.Helper()
	table := schema.Table{
		Name: "values",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "amount", Type: "numeric"},
			{Name: "ratio", Type: "double"},
			{Name: "note", Type: "text"},
			{Name: "payload", Type: "blob"},
			{Name: "day", Type: "date"},
			{Name: "clock", Type: "time"},
			{Name: "occurred_at", Type: "timestamp"},
			{Name: "uid", Type: "uuid"},
			{Name: "enabled", Type: "boolean"},
		},
	}
	projection := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		projection[index] = column.Name
	}
	descriptor, err := newValidationSampleDescriptor(table, projection)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
