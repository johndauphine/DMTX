package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// PaginationStrategy is the durable ordering algorithm chosen for a table.
type PaginationStrategy string

const (
	PaginationIntegerKeyset PaginationStrategy = "integer_keyset"
	PaginationTupleKeyset   PaginationStrategy = "tuple_keyset"
	PaginationRowNumber     PaginationStrategy = "row_number"
)

// KeyKind is an exact bindable primary-key representation. Integer values are
// encoded as decimal strings so values above 2^53 never pass through float64.
type KeyKind string

const (
	KeyInteger KeyKind = "int64"
	KeyText    KeyKind = "text"
	KeyBytes   KeyKind = "bytes"
)

type KeyValue struct {
	Kind    KeyKind `json:"kind"`
	Encoded string  `json:"encoded"`
}

func IntegerKey(value int64) KeyValue {
	return KeyValue{Kind: KeyInteger, Encoded: strconv.FormatInt(value, 10)}
}

func TextKey(value string) KeyValue {
	return KeyValue{Kind: KeyText, Encoded: value}
}

func BytesKey(value []byte) KeyValue {
	return KeyValue{Kind: KeyBytes, Encoded: base64.StdEncoding.EncodeToString(value)}
}

func (value KeyValue) SQLValue() (any, error) {
	switch value.Kind {
	case KeyInteger:
		parsed, err := strconv.ParseInt(value.Encoded, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode integer key: %w", err)
		}
		return parsed, nil
	case KeyText:
		return value.Encoded, nil
	case KeyBytes:
		decoded, err := base64.StdEncoding.DecodeString(value.Encoded)
		if err != nil {
			return nil, fmt.Errorf("decode byte key: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported key kind %q", value.Kind)
	}
}

type KeySpec struct {
	Name string  `json:"name"`
	Kind KeyKind `json:"kind"`
}

type KeyTuple []KeyValue

// PaginationRange is an immutable, exactly restorable source work envelope.
// Lower is exclusive and Upper is inclusive. A nil bound is unbounded.
type PaginationRange struct {
	ID       int       `json:"id"`
	Lower    *KeyTuple `json:"lower,omitempty"`
	Upper    *KeyTuple `json:"upper,omitempty"`
	FirstRow int64     `json:"first_row,omitempty"`
	LastRow  int64     `json:"last_row,omitempty"`
	Empty    bool      `json:"empty,omitempty"`
}

type PaginationPlan struct {
	Strategy     PaginationStrategy `json:"strategy"`
	Keys         []KeySpec          `json:"keys"`
	Ranges       []PaginationRange  `json:"ranges"`
	TopologyHash string             `json:"topology_hash"`
}

// PlanSQLitePagination selects the strongest proven SQLite strategy and
// materializes stable partition boundaries before target mutation.
func PlanSQLitePagination(ctx context.Context, source *sql.DB, table schema.Table, requestedPartitions int) (PaginationPlan, error) {
	if requestedPartitions <= 0 {
		return PaginationPlan{}, fmt.Errorf("partition count must be positive")
	}
	keys := sqliteKeySpecs(table)
	if len(keys) == 0 {
		return PaginationPlan{}, fmt.Errorf("table %s has no primary key", table.Name)
	}
	strategy := sqlitePaginationStrategy(table, keys)
	var ranges []PaginationRange
	var err error
	switch strategy {
	case PaginationIntegerKeyset:
		ranges, err = sqliteIntegerRanges(ctx, source, table.Name, keys[0].Name, requestedPartitions)
	case PaginationTupleKeyset:
		ranges, err = sqliteTupleRanges(ctx, source, table.Name, keys, requestedPartitions)
	default:
		ranges, err = sqliteRowNumberRanges(ctx, source, table.Name, requestedPartitions)
	}
	if err != nil {
		return PaginationPlan{}, err
	}
	if len(ranges) == 0 {
		ranges = []PaginationRange{{ID: 0, FirstRow: 1, Empty: true}}
	}
	plan := PaginationPlan{Strategy: strategy, Keys: keys, Ranges: ranges}
	encoded, err := json.Marshal(struct {
		Strategy            PaginationStrategy `json:"strategy"`
		Keys                []KeySpec          `json:"keys"`
		Ranges              []PaginationRange  `json:"ranges"`
		RequestedPartitions int                `json:"requested_partitions"`
	}{plan.Strategy, plan.Keys, plan.Ranges, requestedPartitions})
	if err != nil {
		return PaginationPlan{}, fmt.Errorf("encode pagination topology: %w", err)
	}
	digest := sha256.Sum256(encoded)
	plan.TopologyHash = hex.EncodeToString(digest[:])
	return plan, nil
}

func sqlitePaginationStrategy(table schema.Table, keys []KeySpec) PaginationStrategy {
	if len(keys) == 1 && keys[0].Kind == KeyInteger {
		column := primaryKeyColumn(table, keys[0].Name)
		if column != nil && (column.DeclaredType != nil && column.DeclaredType.Base == "integer" || !column.Nullable || table.SQLiteWithoutRowID) {
			return PaginationIntegerKeyset
		}
	}
	if len(keys) > 0 {
		for _, key := range keys {
			column := primaryKeyColumn(table, key.Name)
			if column == nil || column.Nullable && !table.SQLiteWithoutRowID || key.Kind == "" {
				return PaginationRowNumber
			}
		}
		return PaginationTupleKeyset
	}
	return PaginationRowNumber
}

func sqliteKeySpecs(table schema.Table) []KeySpec {
	names := primaryKeyColumns(table)
	specs := make([]KeySpec, 0, len(names))
	for _, name := range names {
		column := primaryKeyColumn(table, name)
		if column == nil {
			return nil
		}
		kind := sqliteKeyKind(*column)
		if kind == "" {
			// Preserve the key name so the planner can choose ROW_NUMBER.
			specs = append(specs, KeySpec{Name: name})
			continue
		}
		specs = append(specs, KeySpec{Name: name, Kind: kind})
	}
	return specs
}

func primaryKeyColumn(table schema.Table, name string) *schema.Column {
	for index := range table.Columns {
		if table.Columns[index].Name == name {
			return &table.Columns[index]
		}
	}
	return nil
}

func sqliteKeyKind(column schema.Column) KeyKind {
	base := strings.ToLower(column.Type)
	if column.DeclaredType != nil {
		base = strings.ToLower(column.DeclaredType.Base)
	}
	switch base {
	case "int", "integer", "tinyint", "smallint", "mediumint", "bigint", "int2", "int8":
		return KeyInteger
	case "char", "character", "character varying", "varchar", "varying character", "nchar", "native character", "nvarchar", "text", "clob":
		return KeyText
	case "blob", "binary", "varbinary":
		return KeyBytes
	default:
		return ""
	}
}

func sqliteIntegerRanges(ctx context.Context, source *sql.DB, table, key string, partitions int) ([]PaginationRange, error) {
	var minimum, maximum sql.NullInt64
	query := "SELECT MIN(" + quote(key) + "), MAX(" + quote(key) + ") FROM " + quote(table)
	if err := source.QueryRowContext(ctx, query).Scan(&minimum, &maximum); err != nil {
		return nil, fmt.Errorf("inspect integer range for %s: %w", table, err)
	}
	if !minimum.Valid {
		return nil, nil
	}
	return SplitIntegerRange(minimum.Int64, maximum.Int64, partitions), nil
}

// SplitIntegerRange covers [minimum, maximum] exactly once without performing
// signed fixed-width arithmetic on the domain width.
func SplitIntegerRange(minimum, maximum int64, partitions int) []PaginationRange {
	if partitions <= 0 || minimum > maximum {
		return nil
	}
	minimumBig, maximumBig := big.NewInt(minimum), big.NewInt(maximum)
	cardinality := new(big.Int).Sub(maximumBig, minimumBig)
	cardinality.Add(cardinality, big.NewInt(1))
	partCount := big.NewInt(int64(partitions))
	if cardinality.Cmp(partCount) < 0 {
		partCount.Set(cardinality)
	}
	count := int(partCount.Int64())
	width, remainder := new(big.Int), new(big.Int)
	width.QuoRem(cardinality, partCount, remainder)
	cursor := new(big.Int).Set(minimumBig)
	ranges := make([]PaginationRange, 0, count)
	for index := 0; index < count; index++ {
		size := new(big.Int).Set(width)
		if int64(index) < remainder.Int64() {
			size.Add(size, big.NewInt(1))
		}
		upper := new(big.Int).Add(cursor, new(big.Int).Sub(size, big.NewInt(1)))
		lowerValue, upperValue := cursor.Int64(), upper.Int64()
		upperTuple := KeyTuple{IntegerKey(upperValue)}
		var lowerTuple *KeyTuple
		if index > 0 {
			previous := KeyTuple{IntegerKey(lowerValue - 1)}
			lowerTuple = &previous
		}
		ranges = append(ranges, PaginationRange{ID: index, Lower: lowerTuple, Upper: &upperTuple})
		cursor = new(big.Int).Add(upper, big.NewInt(1))
	}
	return ranges
}

func sqliteTupleRanges(ctx context.Context, source *sql.DB, table string, keys []KeySpec, partitions int) ([]PaginationRange, error) {
	var total int64
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(table)).Scan(&total); err != nil {
		return nil, fmt.Errorf("count tuple range for %s: %w", table, err)
	}
	if total == 0 {
		return nil, nil
	}
	if int64(partitions) > total {
		partitions = int(total)
	}
	ranges := make([]PaginationRange, 0, partitions)
	var lower *KeyTuple
	baseSize := total / int64(partitions)
	extraRows := total % int64(partitions)
	var nextOrdinal int64
	for index := 0; index < partitions; index++ {
		size := baseSize
		if int64(index) < extraRows {
			size++
		}
		lastOrdinal := nextOrdinal + size - 1
		nextOrdinal = lastOrdinal + 1
		upper, err := sqliteTupleAtOffset(ctx, source, table, keys, lastOrdinal)
		if err != nil {
			return nil, err
		}
		upperCopy := append(KeyTuple(nil), upper...)
		ranges = append(ranges, PaginationRange{ID: index, Lower: lower, Upper: &upperCopy})
		lowerCopy := append(KeyTuple(nil), upper...)
		lower = &lowerCopy
	}
	return ranges, nil
}

func sqliteTupleAtOffset(ctx context.Context, source *sql.DB, table string, keys []KeySpec, offset int64) (KeyTuple, error) {
	names := make([]string, len(keys))
	for index, key := range keys {
		names[index] = key.Name
	}
	query := "SELECT " + quotedColumns(names) + " FROM " + quote(table) + " ORDER BY " + quotedColumns(names) + " LIMIT 1 OFFSET ?"
	rows, err := source.QueryContext(ctx, query, offset)
	if err != nil {
		return nil, fmt.Errorf("read tuple boundary for %s: %w", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("tuple boundary %d for %s disappeared", offset, table)
	}
	values, pointers := make([]any, len(keys)), make([]any, len(keys))
	for index := range pointers {
		pointers[index] = &values[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, fmt.Errorf("scan tuple boundary for %s: %w", table, err)
	}
	tuple := make(KeyTuple, len(keys))
	for index, key := range keys {
		value, err := sqliteTypedKey(values[index], key.Kind)
		if err != nil {
			return nil, fmt.Errorf("scan tuple boundary for %s key %s: %w", table, key.Name, err)
		}
		tuple[index] = value
	}
	return tuple, nil
}

func sqliteTypedKey(value any, kind KeyKind) (KeyValue, error) {
	if value == nil {
		return KeyValue{}, fmt.Errorf("NULL tuple keys are unsafe")
	}
	switch kind {
	case KeyInteger:
		integer, err := sqliteIntegerValue(value)
		if err != nil {
			return KeyValue{}, err
		}
		return IntegerKey(integer), nil
	case KeyText:
		switch typed := value.(type) {
		case string:
			return TextKey(typed), nil
		case []byte:
			return TextKey(string(typed)), nil
		default:
			return KeyValue{}, fmt.Errorf("text key has unexpected value type %T", value)
		}
	case KeyBytes:
		bytes, ok := value.([]byte)
		if !ok {
			return KeyValue{}, fmt.Errorf("byte key has unexpected value type %T", value)
		}
		return BytesKey(bytes), nil
	default:
		return KeyValue{}, fmt.Errorf("key type is not tuple-safe")
	}
}

func sqliteRowNumberRanges(ctx context.Context, source *sql.DB, table string, partitions int) ([]PaginationRange, error) {
	var total int64
	if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(table)).Scan(&total); err != nil {
		return nil, fmt.Errorf("count row-number range for %s: %w", table, err)
	}
	return SplitRowNumberRange(total, partitions), nil
}

func SplitRowNumberRange(total int64, partitions int) []PaginationRange {
	if total <= 0 || partitions <= 0 {
		return nil
	}
	if int64(partitions) > total {
		partitions = int(total)
	}
	ranges := make([]PaginationRange, 0, partitions)
	var first int64 = 1
	for index := 0; index < partitions; index++ {
		last := int64(index+1) * total / int64(partitions)
		ranges = append(ranges, PaginationRange{ID: index, FirstRow: first, LastRow: last})
		first = last + 1
	}
	return ranges
}
