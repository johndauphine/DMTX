package migrate

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

// validationValueEncodingVersion makes process-local sample comparisons fail
// closed if the canonical representation ever changes.
const validationValueEncodingVersion = "dmtx-validation-value-v1"

var validationDecimalPattern = regexp.MustCompile(
	`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`,
)

type validationValueKind string

const (
	validationBoolean   validationValueKind = "boolean"
	validationInteger   validationValueKind = "integer"
	validationDecimal   validationValueKind = "decimal"
	validationFloat     validationValueKind = "float"
	validationText      validationValueKind = "text"
	validationBytes     validationValueKind = "bytes"
	validationDate      validationValueKind = "date"
	validationTime      validationValueKind = "time"
	validationTimestamp validationValueKind = "timestamp"
	validationUUID      validationValueKind = "uuid"
	validationDynamic   validationValueKind = "dynamic"
)

type validationColumnDescriptor struct {
	Name string
	Kind validationValueKind
}

// validationSampleDescriptor binds sampled values to one validated projection.
// The projection must contain the complete primary key, so a future sampler
// cannot accidentally compare unstable or mismatched row identities.
type validationSampleDescriptor struct {
	Columns []validationColumnDescriptor
}

func newValidationSampleDescriptor(
	table schema.Table,
	projection []string,
) (validationSampleDescriptor, error) {
	if len(projection) == 0 {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation projection for table %s is empty",
			table.Name,
		)
	}
	metadata := make(map[string]schema.Column, len(table.Columns))
	requiredKeys := make(map[string]struct{})
	requiredColumns := make(map[string]struct{}, len(table.Columns))
	primaryKeyPositions := make(map[int]string)
	for _, column := range table.Columns {
		if column.Name == "" {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
		requiredColumns[column.Name] = struct{}{}
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return validationSampleDescriptor{}, fmt.Errorf(
					"validation table %s non-primary-key column %s has position %d",
					table.Name,
					column.Name,
					column.PrimaryKeyPosition,
				)
			}
			continue
		}
		if column.Nullable {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s primary-key column %s is nullable",
				table.Name,
				column.Name,
			)
		}
		if column.PrimaryKeyPosition <= 0 {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s primary-key column %s has no positive position",
				table.Name,
				column.Name,
			)
		}
		if previous, duplicate := primaryKeyPositions[column.PrimaryKeyPosition]; duplicate {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s primary-key position %d is shared by %s and %s",
				table.Name,
				column.PrimaryKeyPosition,
				previous,
				column.Name,
			)
		}
		primaryKeyPositions[column.PrimaryKeyPosition] = column.Name
		requiredKeys[column.Name] = struct{}{}
	}
	if len(requiredKeys) == 0 {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation table %s has no primary key",
			table.Name,
		)
	}
	for position := 1; position <= len(primaryKeyPositions); position++ {
		if _, exists := primaryKeyPositions[position]; !exists {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s primary-key positions are not contiguous from one",
				table.Name,
			)
		}
	}

	descriptor := validationSampleDescriptor{
		Columns: make([]validationColumnDescriptor, len(projection)),
	}
	seen := make(map[string]struct{}, len(projection))
	for index, name := range projection {
		column, exists := metadata[name]
		if !exists {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation projection for table %s contains unknown column %s",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation projection for table %s duplicates column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
		delete(requiredKeys, name)
		delete(requiredColumns, name)
		kind, err := validationKindForColumn(column)
		if err != nil {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation projection for table %s column %s: %w",
				table.Name,
				name,
				err,
			)
		}
		descriptor.Columns[index] = validationColumnDescriptor{
			Name: name,
			Kind: kind,
		}
	}
	if len(requiredKeys) != 0 {
		missing := make([]string, 0, len(requiredKeys))
		for name := range requiredKeys {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation projection for table %s omits primary-key columns %s",
			table.Name,
			strings.Join(missing, ", "),
		)
	}
	if len(requiredColumns) != 0 {
		missing := make([]string, 0, len(requiredColumns))
		for name := range requiredColumns {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation projection for table %s omits transfer columns %s",
			table.Name,
			strings.Join(missing, ", "),
		)
	}
	return descriptor, nil
}

func validationKindForColumn(column schema.Column) (validationValueKind, error) {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if opening := strings.IndexByte(base, '('); opening >= 0 {
		base = strings.TrimSpace(base[:opening])
	}
	switch base {
	case "bool", "boolean":
		return validationBoolean, nil
	case "tinyint", "smallint", "mediumint", "int", "integer", "int2",
		"int4", "bigint", "int8", "unsigned big int", "year":
		return validationInteger, nil
	case "decimal", "numeric", "money", "smallmoney":
		return validationDecimal, nil
	case "real", "float", "float4", "float8", "double", "double precision":
		return validationFloat, nil
	case "char", "character", "varchar", "character varying",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "ntext", "tinytext", "mediumtext", "longtext", "clob",
		"json", "jsonb", "xml", "enum", "set":
		return validationText, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob",
		"bytea", "image", "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection":
		return validationBytes, nil
	case "date":
		return validationDate, nil
	case "time":
		return validationTime, nil
	case "datetime", "datetime2", "smalldatetime", "timestamp",
		"timestamptz", "datetimeoffset":
		return validationTimestamp, nil
	case "uuid", "uniqueidentifier":
		return validationUUID, nil
	case "any":
		return validationDynamic, nil
	default:
		return "", fmt.Errorf(
			"unsupported validation column type %q",
			column.Type,
		)
	}
}

// canonicalValidationRow returns a typed, length-delimited representation of
// one sampled row. The bytes may contain row data: they are process-local
// comparison material and must never be logged, audited, persisted, or sent
// to an advisory service.
func canonicalValidationRow(
	descriptor validationSampleDescriptor,
	values []any,
) ([]byte, error) {
	if len(descriptor.Columns) == 0 {
		return nil, fmt.Errorf("validation sample descriptor is empty")
	}
	if len(values) != len(descriptor.Columns) {
		return nil, fmt.Errorf(
			"validation row has %d values for %d projected columns",
			len(values),
			len(descriptor.Columns),
		)
	}
	encoded := make([]byte, 0, len(values)*24)
	encoded = appendFrame(encoded, "version", []byte(validationValueEncodingVersion))
	encoded = appendFrame(encoded, "columns", []byte(strconv.Itoa(len(values))))
	for index, value := range values {
		column := descriptor.Columns[index]
		if column.Name == "" {
			return nil, fmt.Errorf(
				"validation sample descriptor column %d has no name",
				index,
			)
		}
		kind, payload, err := canonicalValidationValue(column.Kind, value)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize validation column %d (%s): %w",
				index,
				column.Name,
				err,
			)
		}
		encoded = appendFrame(encoded, string(kind), payload)
	}
	return encoded, nil
}

func canonicalValidationValue(
	kind validationValueKind,
	value any,
) (validationValueKind, []byte, error) {
	if value == nil {
		return "null", nil, nil
	}
	switch kind {
	case validationBoolean:
		payload, err := canonicalValidationBoolean(value)
		return kind, payload, err
	case validationInteger:
		payload, err := canonicalValidationInteger(value)
		return kind, payload, err
	case validationDecimal:
		payload, err := canonicalValidationDecimal(value)
		return kind, payload, err
	case validationFloat:
		payload, err := canonicalValidationFloat(value)
		return kind, payload, err
	case validationText:
		payload, err := canonicalValidationText(value)
		return kind, payload, err
	case validationBytes:
		payload, err := canonicalValidationBytes(value)
		return kind, payload, err
	case validationDate:
		payload, err := canonicalValidationDate(value)
		return kind, payload, err
	case validationTime:
		payload, err := canonicalValidationTime(value)
		return kind, payload, err
	case validationTimestamp:
		payload, err := canonicalValidationTimestamp(value)
		return kind, payload, err
	case validationUUID:
		payload, err := canonicalValidationUUID(value)
		return kind, payload, err
	case validationDynamic:
		return canonicalValidationDynamic(value)
	default:
		return "", nil, fmt.Errorf("unsupported validation semantic kind %q", kind)
	}
}

// canonicalValidationDynamic preserves SQLite ANY's runtime storage class.
// SQLite ANY may hold only NULL (handled by canonicalValidationValue),
// INTEGER, REAL, TEXT, or BLOB. Each admitted class uses the same canonical
// payload as a statically typed column but retains a distinct frame kind.
func canonicalValidationDynamic(value any) (validationValueKind, []byte, error) {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		payload, err := canonicalValidationInteger(value)
		return validationInteger, payload, err
	case float32, float64:
		payload, err := canonicalValidationFloat(value)
		return validationFloat, payload, err
	case string:
		payload, err := canonicalValidationText(value)
		return validationText, payload, err
	case []byte:
		payload, err := canonicalValidationBytes(value)
		return validationBytes, payload, err
	default:
		return "", nil, unexpectedValidationShape(validationDynamic, value)
	}
}

// compareValidationPrimaryKeyValues compares complete primary-key values in
// the ascending semantic order required from a source sample adapter. It does
// not compare the canonical wire bytes: integer, decimal, and floating-point
// payloads must be compared by mathematical value, and composite keys must be
// compared one component at a time.
func compareValidationPrimaryKeyValues(
	descriptor validationSampleDescriptor,
	left []any,
	right []any,
) (int, error) {
	if len(descriptor.Columns) == 0 {
		return 0, fmt.Errorf("validation primary-key descriptor is empty")
	}
	if len(left) != len(descriptor.Columns) ||
		len(right) != len(descriptor.Columns) {
		return 0, fmt.Errorf(
			"validation primary-key value count does not match its descriptor",
		)
	}
	for index, column := range descriptor.Columns {
		comparison, err := compareValidationOrderValue(
			column.Kind,
			left[index],
			right[index],
		)
		if err != nil {
			return 0, fmt.Errorf(
				"compare validation primary-key column %d (%s): %w",
				index,
				column.Name,
				err,
			)
		}
		if comparison != 0 {
			return comparison, nil
		}
	}
	return 0, nil
}

func compareValidationOrderValue(
	declaredKind validationValueKind,
	left any,
	right any,
) (int, error) {
	leftKind, leftPayload, err := canonicalValidationValue(
		declaredKind,
		left,
	)
	if err != nil {
		return 0, err
	}
	rightKind, rightPayload, err := canonicalValidationValue(
		declaredKind,
		right,
	)
	if err != nil {
		return 0, err
	}
	if leftKind == "null" || rightKind == "null" {
		return 0, fmt.Errorf(
			"primary-key ordering cannot admit NULL values",
		)
	}
	if declaredKind == validationDynamic {
		return compareDynamicValidationOrder(
			leftKind,
			leftPayload,
			rightKind,
			rightPayload,
		)
	}
	if leftKind != declaredKind || rightKind != declaredKind {
		return 0, fmt.Errorf(
			"canonical primary-key kind does not match its descriptor",
		)
	}
	switch declaredKind {
	case validationInteger, validationDecimal, validationFloat:
		return compareCanonicalValidationNumbers(
			declaredKind,
			leftPayload,
			declaredKind,
			rightPayload,
		)
	case validationBoolean, validationText, validationBytes,
		validationDate, validationTime, validationTimestamp,
		validationUUID:
		return bytes.Compare(leftPayload, rightPayload), nil
	default:
		return 0, fmt.Errorf(
			"unsupported primary-key ordering kind %q",
			declaredKind,
		)
	}
}

func compareDynamicValidationOrder(
	leftKind validationValueKind,
	leftPayload []byte,
	rightKind validationValueKind,
	rightPayload []byte,
) (int, error) {
	leftRank, err := dynamicValidationOrderRank(leftKind)
	if err != nil {
		return 0, err
	}
	rightRank, err := dynamicValidationOrderRank(rightKind)
	if err != nil {
		return 0, err
	}
	if leftRank < rightRank {
		return -1, nil
	}
	if leftRank > rightRank {
		return 1, nil
	}
	if leftRank == 0 {
		return compareCanonicalValidationNumbers(
			leftKind,
			leftPayload,
			rightKind,
			rightPayload,
		)
	}
	return bytes.Compare(leftPayload, rightPayload), nil
}

func dynamicValidationOrderRank(kind validationValueKind) (int, error) {
	switch kind {
	case validationInteger, validationFloat:
		return 0, nil
	case validationText:
		return 1, nil
	case validationBytes:
		return 2, nil
	default:
		return 0, fmt.Errorf(
			"unsupported dynamic primary-key ordering kind %q",
			kind,
		)
	}
}

type canonicalValidationNumber struct {
	infinity int
	value    *big.Rat
}

func compareCanonicalValidationNumbers(
	leftKind validationValueKind,
	leftPayload []byte,
	rightKind validationValueKind,
	rightPayload []byte,
) (int, error) {
	left, err := parseCanonicalValidationNumber(leftKind, leftPayload)
	if err != nil {
		return 0, err
	}
	right, err := parseCanonicalValidationNumber(rightKind, rightPayload)
	if err != nil {
		return 0, err
	}
	if left.infinity < right.infinity {
		return -1, nil
	}
	if left.infinity > right.infinity {
		return 1, nil
	}
	if left.infinity != 0 {
		return 0, nil
	}
	return left.value.Cmp(right.value), nil
}

func parseCanonicalValidationNumber(
	kind validationValueKind,
	payload []byte,
) (canonicalValidationNumber, error) {
	switch kind {
	case validationInteger:
		integer, ok := new(big.Int).SetString(string(payload), 10)
		if !ok {
			return canonicalValidationNumber{}, fmt.Errorf(
				"canonical integer has an unsupported shape",
			)
		}
		return canonicalValidationNumber{
			value: new(big.Rat).SetInt(integer),
		}, nil
	case validationDecimal:
		rational, ok := new(big.Rat).SetString(string(payload))
		if !ok {
			return canonicalValidationNumber{}, fmt.Errorf(
				"canonical decimal has an unsupported shape",
			)
		}
		return canonicalValidationNumber{value: rational}, nil
	case validationFloat:
		switch string(payload) {
		case "-inf":
			return canonicalValidationNumber{infinity: -1}, nil
		case "+inf":
			return canonicalValidationNumber{infinity: 1}, nil
		case "nan":
			return canonicalValidationNumber{}, fmt.Errorf(
				"NaN has no strict primary-key order",
			)
		}
		floating, err := strconv.ParseFloat(string(payload), 64)
		if err != nil {
			return canonicalValidationNumber{}, fmt.Errorf(
				"canonical float has an unsupported shape",
			)
		}
		rational := new(big.Rat).SetFloat64(floating)
		if rational == nil {
			return canonicalValidationNumber{}, fmt.Errorf(
				"canonical float has no finite mathematical value",
			)
		}
		return canonicalValidationNumber{value: rational}, nil
	default:
		return canonicalValidationNumber{}, fmt.Errorf(
			"unsupported numeric primary-key ordering kind %q",
			kind,
		)
	}
}

func canonicalValidationBoolean(value any) ([]byte, error) {
	switch typed := value.(type) {
	case bool:
		return []byte(strconv.FormatBool(typed)), nil
	case int:
		return canonicalBooleanInteger(int64(typed))
	case int8:
		return canonicalBooleanInteger(int64(typed))
	case int16:
		return canonicalBooleanInteger(int64(typed))
	case int32:
		return canonicalBooleanInteger(int64(typed))
	case int64:
		return canonicalBooleanInteger(typed)
	case uint:
		if uint64(typed) > 1 {
			break
		}
		return []byte(strconv.FormatBool(typed == 1)), nil
	case uint8:
		return canonicalBooleanInteger(int64(typed))
	case uint16:
		return canonicalBooleanInteger(int64(typed))
	case uint32:
		return canonicalBooleanInteger(int64(typed))
	case uint64:
		if typed > 1 {
			break
		}
		return []byte(strconv.FormatBool(typed == 1)), nil
	case string:
		return canonicalBooleanText(typed)
	case []byte:
		return canonicalBooleanText(string(typed))
	case sql.RawBytes:
		return canonicalBooleanText(string(typed))
	}
	return nil, unexpectedValidationShape(validationBoolean, value)
}

func canonicalBooleanInteger(value int64) ([]byte, error) {
	if value != 0 && value != 1 {
		return nil, fmt.Errorf("boolean integer must be zero or one")
	}
	return []byte(strconv.FormatBool(value == 1)), nil
}

func canonicalBooleanText(value string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "t", "1":
		return []byte("true"), nil
	case "false", "f", "0":
		return []byte("false"), nil
	default:
		return nil, fmt.Errorf("boolean text has an unsupported shape")
	}
}

func canonicalValidationInteger(value any) ([]byte, error) {
	switch typed := value.(type) {
	case int:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(typed, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(typed, 10)), nil
	case string:
		return canonicalIntegerText(typed)
	case []byte:
		return canonicalIntegerText(string(typed))
	case sql.RawBytes:
		return canonicalIntegerText(string(typed))
	default:
		return nil, unexpectedValidationShape(validationInteger, value)
	}
}

func canonicalIntegerText(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	integer, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf("integer text has an unsupported shape")
	}
	return []byte(integer.String()), nil
}

func canonicalValidationDecimal(value any) ([]byte, error) {
	switch typed := value.(type) {
	case int:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(typed, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(typed, 10)), nil
	case string:
		return canonicalDecimalText(typed)
	case []byte:
		return canonicalDecimalText(string(typed))
	case sql.RawBytes:
		return canonicalDecimalText(string(typed))
	default:
		return nil, unexpectedValidationShape(validationDecimal, value)
	}
}

func canonicalDecimalText(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if !validationDecimalPattern.MatchString(value) {
		return nil, fmt.Errorf("decimal text has an unsupported shape")
	}
	rational, ok := new(big.Rat).SetString(value)
	if !ok {
		return nil, fmt.Errorf("decimal text has an unsupported shape")
	}
	return []byte(rational.RatString()), nil
}

func canonicalValidationFloat(value any) ([]byte, error) {
	var floating float64
	switch typed := value.(type) {
	case float32:
		floating = float64(typed)
	case float64:
		floating = typed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return nil, fmt.Errorf("float text has an unsupported shape")
		}
		floating = parsed
	case []byte:
		return canonicalValidationFloat(string(typed))
	case sql.RawBytes:
		return canonicalValidationFloat(string(typed))
	default:
		return nil, unexpectedValidationShape(validationFloat, value)
	}
	switch {
	case math.IsNaN(floating):
		return []byte("nan"), nil
	case math.IsInf(floating, 1):
		return []byte("+inf"), nil
	case math.IsInf(floating, -1):
		return []byte("-inf"), nil
	case floating == 0:
		return []byte("0"), nil
	default:
		// One float64 domain makes float32(v) equal to float64(float32(v))
		// without colliding it with a distinct float64 value.
		return []byte(strconv.FormatFloat(floating, 'g', -1, 64)), nil
	}
}

func canonicalValidationText(value any) ([]byte, error) {
	var text []byte
	switch typed := value.(type) {
	case string:
		text = []byte(typed)
	case []byte:
		text = typed
	case sql.RawBytes:
		text = typed
	default:
		return nil, unexpectedValidationShape(validationText, value)
	}
	if !utf8.Valid(text) {
		return nil, fmt.Errorf("text value is not valid UTF-8")
	}
	return append([]byte(nil), text...), nil
}

func canonicalValidationBytes(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...), nil
	case sql.RawBytes:
		return append([]byte(nil), typed...), nil
	default:
		return nil, unexpectedValidationShape(validationBytes, value)
	}
}

func canonicalValidationDate(value any) ([]byte, error) {
	switch typed := value.(type) {
	case time.Time:
		if typed.Hour() != 0 || typed.Minute() != 0 ||
			typed.Second() != 0 || typed.Nanosecond() != 0 {
			return nil, fmt.Errorf("date time.Time contains a clock component")
		}
		return []byte(typed.Format("2006-01-02")), nil
	case string:
		parsed, err := time.Parse("2006-01-02", strings.TrimSpace(typed))
		if err != nil {
			return nil, fmt.Errorf("date text has an unsupported shape")
		}
		return []byte(parsed.Format("2006-01-02")), nil
	case []byte:
		return canonicalValidationDate(string(typed))
	case sql.RawBytes:
		return canonicalValidationDate(string(typed))
	default:
		return nil, unexpectedValidationShape(validationDate, value)
	}
}

func canonicalValidationTime(value any) ([]byte, error) {
	switch typed := value.(type) {
	case time.Time:
		if (typed.Year() != 0 && typed.Year() != 1) ||
			typed.Month() != time.January || typed.Day() != 1 {
			return nil, fmt.Errorf("time time.Time contains a date component")
		}
		return []byte(typed.Format("15:04:05.000000000")), nil
	case string:
		parsed, err := time.Parse("15:04:05.999999999", strings.TrimSpace(typed))
		if err != nil {
			return nil, fmt.Errorf("time text has an unsupported shape")
		}
		return []byte(parsed.Format("15:04:05.000000000")), nil
	case []byte:
		return canonicalValidationTime(string(typed))
	case sql.RawBytes:
		return canonicalValidationTime(string(typed))
	default:
		return nil, unexpectedValidationShape(validationTime, value)
	}
}

func canonicalValidationTimestamp(value any) ([]byte, error) {
	var timestamp time.Time
	switch typed := value.(type) {
	case time.Time:
		timestamp = typed
	case string:
		parsed, err := parseValidationTimestamp(strings.TrimSpace(typed))
		if err != nil {
			return nil, err
		}
		timestamp = parsed
	case []byte:
		return canonicalValidationTimestamp(string(typed))
	case sql.RawBytes:
		return canonicalValidationTimestamp(string(typed))
	default:
		return nil, unexpectedValidationShape(validationTimestamp, value)
	}
	normalized := timestamp.Round(0).UTC()
	return []byte(normalized.Format("2006-01-02T15:04:05.000000000Z07:00")), nil
}

func parseValidationTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
	} {
		var (
			parsed time.Time
			err    error
		)
		if strings.Contains(layout, "Z07:00") {
			parsed, err = time.Parse(layout, value)
		} else {
			parsed, err = time.ParseInLocation(layout, value, time.UTC)
		}
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp text has an unsupported shape")
}

func canonicalValidationUUID(value any) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return canonicalUUIDText(typed)
	case []byte:
		if len(typed) == 16 {
			encoded := make([]byte, hex.EncodedLen(len(typed)))
			hex.Encode(encoded, typed)
			return encoded, nil
		}
		return canonicalUUIDText(string(typed))
	case sql.RawBytes:
		return canonicalValidationUUID([]byte(typed))
	default:
		return nil, unexpectedValidationShape(validationUUID, value)
	}
}

func canonicalUUIDText(value string) ([]byte, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	switch len(value) {
	case 32:
	case 36:
		if value[8] != '-' || value[13] != '-' ||
			value[18] != '-' || value[23] != '-' {
			return nil, fmt.Errorf("UUID text has an unsupported shape")
		}
		value = value[:8] + value[9:13] + value[14:18] +
			value[19:23] + value[24:]
	default:
		return nil, fmt.Errorf("UUID text has an unsupported shape")
	}
	decoded := make([]byte, 16)
	if _, err := hex.Decode(decoded, []byte(value)); err != nil {
		return nil, fmt.Errorf("UUID text has an unsupported shape")
	}
	return []byte(value), nil
}

func unexpectedValidationShape(
	kind validationValueKind,
	value any,
) error {
	return fmt.Errorf(
		"unexpected %s validation value type %s",
		kind,
		reflect.TypeOf(value),
	)
}

func appendFrame(destination []byte, kind string, payload []byte) []byte {
	destination = strconv.AppendInt(destination, int64(len(kind)), 10)
	destination = append(destination, ':')
	destination = append(destination, kind...)
	destination = strconv.AppendInt(destination, int64(len(payload)), 10)
	destination = append(destination, ':')
	destination = append(destination, payload...)
	return destination
}
