package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

// paginationSourceAdapter is a pre-mutation source capability. It resolves a
// deterministic strategy, exact range bounds, and a stable topology hash
// without opening a target or transferring rows.
type paginationSourceAdapter interface {
	PlanPagination(
		context.Context,
		schema.Table,
		int,
	) (PaginationPlan, error)
}

var (
	_ paginationSourceAdapter = (*relationalSourceAdapter)(nil)
	_ paginationSourceAdapter = (*sqliteSourceAdapter)(nil)
)

type adapterPaginationQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type adapterPaginationKeyEvidence struct {
	Name        string               `json:"name"`
	Type        string               `json:"type"`
	Nullable    bool                 `json:"nullable"`
	Position    int                  `json:"position"`
	Declaration *schema.DeclaredType `json:"declaration,omitempty"`
}

func requirePaginationSourceAdapter(
	source sourceAdapter,
) (paginationSourceAdapter, error) {
	if source == nil {
		return nil, adapterPaginationPolicy(
			"select capability",
			errors.New("source adapter is required"),
		)
	}
	if source.Engine() == "clickhouse" {
		return nil, adapterPaginationPolicy(
			"select capability",
			errors.New(
				"ClickHouse source does not support relational range pagination",
			),
		)
	}
	capability, ok := source.(paginationSourceAdapter)
	if !ok {
		return nil, adapterPaginationPolicy(
			"select capability",
			fmt.Errorf(
				"%s source does not implement deterministic range pagination",
				source.DisplayName(),
			),
		)
	}
	return capability, nil
}

func (adapter *relationalSourceAdapter) PlanPagination(
	ctx context.Context,
	table schema.Table,
	requestedPartitions int,
) (PaginationPlan, error) {
	if adapter == nil || adapter.database == nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"plan",
			errors.New("relational source adapter is not open"),
		)
	}
	return planAdapterSourcePagination(
		ctx,
		adapter.spec.engine,
		adapter.namespace,
		adapter.database,
		table,
		requestedPartitions,
	)
}

func (adapter *sqliteSourceAdapter) PlanPagination(
	ctx context.Context,
	table schema.Table,
	requestedPartitions int,
) (PaginationPlan, error) {
	if adapter == nil || adapter.snapshot == nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"plan",
			errors.New("SQLite source adapter is not open"),
		)
	}
	return planAdapterSourcePagination(
		ctx,
		"sqlite",
		"",
		adapter.snapshot,
		table,
		requestedPartitions,
	)
}

func planAdapterSourcePagination(
	ctx context.Context,
	sourceEngine string,
	namespace string,
	queryer adapterPaginationQueryer,
	table schema.Table,
	requestedPartitions int,
) (PaginationPlan, error) {
	if ctx == nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"plan",
			errors.New("context is required"),
		)
	}
	if queryer == nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"plan",
			errors.New("source query capability is required"),
		)
	}
	if requestedPartitions <= 0 ||
		uint64(requestedPartitions) > maximumRuntimeTuningRanges {
		return PaginationPlan{}, adapterPaginationPolicy(
			"plan",
			fmt.Errorf(
				"partition count %d is outside 1..%d",
				requestedPartitions,
				maximumRuntimeTuningRanges,
			),
		)
	}

	keys, err := adapterPaginationPrimaryKey(
		sourceEngine,
		namespace,
		table,
	)
	if err != nil {
		return PaginationPlan{}, err
	}
	specs := make([]KeySpec, len(keys))
	evidence := make([]adapterPaginationKeyEvidence, len(keys))
	for index, column := range keys {
		specs[index] = KeySpec{
			Name: column.Name,
			Kind: adapterPaginationKeyKind(sourceEngine, column),
		}
		evidence[index] = adapterPaginationKeyEvidence{
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
			Position: column.PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(
				column.DeclaredType,
			),
		}
	}

	strategy := adapterPaginationStrategy(sourceEngine, table, keys)
	var ranges []PaginationRange
	switch strategy {
	case PaginationIntegerKeyset:
		ranges, err = adapterPaginationIntegerRanges(
			ctx,
			sourceEngine,
			namespace,
			queryer,
			table,
			keys[0],
			requestedPartitions,
		)
	case PaginationTupleKeyset:
		ranges, err = adapterPaginationTupleRanges(
			ctx,
			sourceEngine,
			namespace,
			queryer,
			table,
			keys,
			requestedPartitions,
		)
	case PaginationRowNumber:
		ranges, err = adapterPaginationRowNumberRanges(
			ctx,
			sourceEngine,
			namespace,
			queryer,
			table,
			requestedPartitions,
		)
	default:
		err = fmt.Errorf("unsupported pagination strategy %q", strategy)
	}
	if err != nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"materialize ranges",
			err,
		)
	}
	if len(ranges) == 0 {
		empty := PaginationRange{ID: 0, Empty: true}
		if strategy == PaginationRowNumber {
			empty.FirstRow = 1
		}
		ranges = []PaginationRange{empty}
	}

	plan := PaginationPlan{
		Strategy: strategy,
		Keys:     specs,
		Ranges:   ranges,
	}
	hash, err := adapterPaginationTopologyHash(
		sourceEngine,
		table,
		requestedPartitions,
		evidence,
		plan,
	)
	if err != nil {
		return PaginationPlan{}, adapterPaginationPolicy(
			"hash topology",
			err,
		)
	}
	plan.TopologyHash = hash
	return plan, nil
}

func adapterPaginationPrimaryKey(
	sourceEngine string,
	namespace string,
	table schema.Table,
) ([]schema.Column, error) {
	if err := validateAdapterPaginationIdentifier(
		sourceEngine,
		"table",
		table.Name,
	); err != nil {
		return nil, adapterPaginationPolicy("validate catalog", err)
	}
	switch sourceEngine {
	case "postgres", "mysql", "mssql":
		if err := validateAdapterPaginationIdentifier(
			sourceEngine,
			"schema",
			namespace,
		); err != nil {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				err,
			)
		}
		if table.Schema != namespace {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s has schema %q, want %q",
					table.Name,
					table.Schema,
					namespace,
				),
			)
		}
	case "sqlite":
		if table.Schema != "" {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"SQLite source table %s has unexpected schema %q",
					table.Name,
					table.Schema,
				),
			)
		}
	default:
		return nil, adapterPaginationPolicy(
			"validate catalog",
			fmt.Errorf(
				"source engine %q has no pagination admission",
				sourceEngine,
			),
		)
	}

	seenNames := make(map[string]struct{}, len(table.Columns))
	positions := make(map[int]schema.Column)
	for index, column := range table.Columns {
		if err := validateAdapterPaginationIdentifier(
			sourceEngine,
			"column",
			column.Name,
		); err != nil {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf("column %d: %w", index, err),
			)
		}
		if _, duplicate := seenNames[column.Name]; duplicate {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s has duplicate column %q",
					table.Name,
					column.Name,
				),
			)
		}
		seenNames[column.Name] = struct{}{}
		if column.PrimaryKeyPosition < 0 {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s column %s has negative primary-key position %d",
					table.Name,
					column.Name,
					column.PrimaryKeyPosition,
				),
			)
		}
		if column.PrimaryKey != (column.PrimaryKeyPosition > 0) {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s column %s has contradictory primary-key metadata",
					table.Name,
					column.Name,
				),
			)
		}
		if !column.PrimaryKey {
			continue
		}
		if _, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s has duplicate primary-key position %d",
					table.Name,
					column.PrimaryKeyPosition,
				),
			)
		}
		if !adapterPaginationKeyNonNull(
			sourceEngine,
			table,
			column,
		) {
			return nil, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"source table %s primary-key column %s is not proven non-null",
					table.Name,
					column.Name,
				),
			)
		}
		positions[column.PrimaryKeyPosition] = column
	}
	if len(positions) == 0 {
		return nil, NewTransferError(
			ErrorClassPrimaryKey,
			fmt.Errorf(
				"source table %s has no primary key for deterministic pagination",
				table.Name,
			),
		)
	}
	keys := make([]schema.Column, len(positions))
	for position := 1; position <= len(keys); position++ {
		column, ok := positions[position]
		if !ok {
			return nil, adapterPaginationPolicy(
				"validate catalog",
				fmt.Errorf(
					"source table %s primary-key positions are not contiguous",
					table.Name,
				),
			)
		}
		keys[position-1] = column
	}
	return keys, nil
}

func adapterPaginationKeyNonNull(
	sourceEngine string,
	table schema.Table,
	column schema.Column,
) bool {
	if !column.Nullable {
		return true
	}
	if sourceEngine != "sqlite" {
		return false
	}
	if table.SQLiteWithoutRowID || table.SQLiteStrict {
		return true
	}
	return len(adapterPaginationKeyColumns(table)) == 1 &&
		adapterPaginationExactSignedInteger("sqlite", column)
}

func adapterPaginationKeyColumns(table schema.Table) []schema.Column {
	result := make([]schema.Column, 0)
	for _, column := range table.Columns {
		if column.PrimaryKey {
			result = append(result, column)
		}
	}
	return result
}

func adapterPaginationStrategy(
	sourceEngine string,
	table schema.Table,
	keys []schema.Column,
) PaginationStrategy {
	if len(keys) == 1 &&
		adapterPaginationExactSignedInteger(sourceEngine, keys[0]) &&
		(sourceEngine != "sqlite" ||
			!table.SQLiteWithoutRowID &&
				adapterPaginationExactSQLiteRowID(keys[0])) {
		return PaginationIntegerKeyset
	}
	if len(keys) > 1 &&
		adapterPaginationTupleComparisonProven(
			sourceEngine,
			table,
			keys,
		) {
		return PaginationTupleKeyset
	}
	return PaginationRowNumber
}

func adapterPaginationTupleComparisonProven(
	sourceEngine string,
	table schema.Table,
	keys []schema.Column,
) bool {
	switch sourceEngine {
	case "postgres", "mysql":
	case "sqlite":
		if !table.SQLiteStrict {
			return false
		}
	default:
		// SQL Server has no native row-value comparison. The row-number
		// fallback keeps the complete primary key ordering without inventing
		// an unproven disjunctive comparison seam.
		return false
	}
	for _, column := range keys {
		if !adapterPaginationExactSignedInteger(sourceEngine, column) {
			return false
		}
	}
	return true
}

func adapterPaginationExactSignedInteger(
	sourceEngine string,
	column schema.Column,
) bool {
	if column.Nullable && sourceEngine != "sqlite" {
		return false
	}
	switch sourceEngine {
	case "postgres":
		return (column.Type == "integer" || column.Type == "bigint") &&
			column.DeclaredType == nil
	case "mysql":
		if column.DeclaredType == nil ||
			(column.Type != "integer" && column.Type != "bigint") {
			return false
		}
		switch column.DeclaredType.Base {
		case "tinyint", "smallint", "mediumint", "int", "bigint":
		default:
			return false
		}
		return adapterPaginationPlainDeclaration(
			column.DeclaredType,
			true,
		)
	case "mssql":
		if column.DeclaredType == nil ||
			(column.Type != "integer" && column.Type != "bigint") {
			return false
		}
		switch column.DeclaredType.Base {
		case "smallint", "int", "bigint":
		default:
			return false
		}
		return adapterPaginationPlainDeclaration(
			column.DeclaredType,
			false,
		)
	case "sqlite":
		return adapterPaginationExactSQLiteRowID(column)
	default:
		return false
	}
}

func adapterPaginationExactSQLiteRowID(column schema.Column) bool {
	return column.Type == "integer" &&
		column.DeclaredType != nil &&
		column.DeclaredType.Base == "integer" &&
		adapterPaginationPlainDeclaration(
			column.DeclaredType,
			false,
		)
}

func adapterPaginationPlainDeclaration(
	declaration *schema.DeclaredType,
	allowEmptyMySQL bool,
) bool {
	if declaration == nil ||
		len(declaration.Arguments) != 0 ||
		declaration.Length != nil ||
		declaration.Precision != nil ||
		declaration.Scale != nil ||
		declaration.FractionalSecondPrecision != nil ||
		declaration.Spatial != nil {
		return false
	}
	if declaration.MySQL == nil {
		return true
	}
	if !allowEmptyMySQL {
		return false
	}
	mysql := declaration.MySQL
	return !mysql.Unsigned &&
		!mysql.Zerofill &&
		!mysql.TinyIntOne &&
		mysql.BitWidth == nil &&
		len(mysql.EnumMembers) == 0 &&
		len(mysql.SetMembers) == 0
}

func adapterPaginationKeyKind(
	sourceEngine string,
	column schema.Column,
) KeyKind {
	if adapterPaginationExactSignedInteger(sourceEngine, column) {
		return KeyInteger
	}
	switch column.Type {
	case "char", "varchar", "text", "uuid", "json":
		return KeyText
	case "blob", "bytea", "binary", "varbinary":
		return KeyBytes
	default:
		return ""
	}
}

func adapterPaginationIntegerRanges(
	ctx context.Context,
	sourceEngine string,
	namespace string,
	queryer adapterPaginationQueryer,
	table schema.Table,
	key schema.Column,
	requestedPartitions int,
) ([]PaginationRange, error) {
	qualified, err := adapterPaginationQualified(
		sourceEngine,
		namespace,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	identifier := adapterPaginationIdentifier(sourceEngine, key.Name)
	query := "SELECT MIN(" + identifier + "), MAX(" +
		identifier + ") FROM " + qualified
	var minimum, maximum sql.NullInt64
	if err := queryer.QueryRowContext(ctx, query).Scan(
		&minimum,
		&maximum,
	); err != nil {
		return nil, fmt.Errorf(
			"inspect integer range for %s: %w",
			table.Name,
			err,
		)
	}
	if minimum.Valid != maximum.Valid {
		return nil, fmt.Errorf(
			"integer range for %s returned contradictory NULL bounds",
			table.Name,
		)
	}
	if !minimum.Valid {
		return nil, nil
	}
	if minimum.Int64 > maximum.Int64 {
		return nil, fmt.Errorf(
			"integer range for %s regressed",
			table.Name,
		)
	}
	return SplitIntegerRange(
		minimum.Int64,
		maximum.Int64,
		requestedPartitions,
	), nil
}

func adapterPaginationTupleRanges(
	ctx context.Context,
	sourceEngine string,
	namespace string,
	queryer adapterPaginationQueryer,
	table schema.Table,
	keys []schema.Column,
	requestedPartitions int,
) ([]PaginationRange, error) {
	query, err := adapterPaginationTupleBoundaryQuery(
		sourceEngine,
		namespace,
		table.Name,
		keys,
	)
	if err != nil {
		return nil, err
	}
	rows, err := queryer.QueryContext(
		ctx,
		query,
		requestedPartitions,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read tuple boundaries for %s: %w",
			table.Name,
			err,
		)
	}
	defer rows.Close()

	ranges := make([]PaginationRange, 0, requestedPartitions)
	var previous *KeyTuple
	var previousValues []int64
	for rows.Next() {
		values := make([]any, len(keys))
		destinations := make([]any, len(keys)+1)
		var bucketValue any
		destinations[0] = &bucketValue
		for index := range values {
			destinations[index+1] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf(
				"scan tuple boundary for %s: %w",
				table.Name,
				err,
			)
		}
		bucket, err := adapterPaginationInt64(bucketValue)
		if err != nil ||
			bucket != int64(len(ranges)+1) ||
			bucket > int64(requestedPartitions) {
			return nil, fmt.Errorf(
				"tuple boundary for %s has invalid bucket",
				table.Name,
			)
		}
		tuple := make(KeyTuple, len(keys))
		integers := make([]int64, len(keys))
		for index, value := range values {
			integer, err := adapterPaginationInt64(value)
			if err != nil {
				return nil, fmt.Errorf(
					"scan tuple boundary for %s key %s: %w",
					table.Name,
					keys[index].Name,
					err,
				)
			}
			integers[index] = integer
			tuple[index] = IntegerKey(integer)
		}
		if len(previousValues) > 0 &&
			!adapterPaginationTupleAfter(
				integers,
				previousValues,
			) {
			return nil, fmt.Errorf(
				"tuple boundaries for %s are not strictly ordered",
				table.Name,
			)
		}
		upper := append(KeyTuple(nil), tuple...)
		ranges = append(ranges, PaginationRange{
			ID:    len(ranges),
			Lower: cloneAdapterPaginationTuple(previous),
			Upper: &upper,
		})
		previousCopy := append(KeyTuple(nil), tuple...)
		previous = &previousCopy
		previousValues = append([]int64(nil), integers...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate tuple boundaries for %s: %w",
			table.Name,
			err,
		)
	}
	return ranges, nil
}

func adapterPaginationRowNumberRanges(
	ctx context.Context,
	sourceEngine string,
	namespace string,
	queryer adapterPaginationQueryer,
	table schema.Table,
	requestedPartitions int,
) ([]PaginationRange, error) {
	qualified, err := adapterPaginationQualified(
		sourceEngine,
		namespace,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	count := "COUNT(*)"
	if sourceEngine == "mssql" {
		count = "COUNT_BIG(*)"
	}
	var total int64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT "+count+" FROM "+qualified,
	).Scan(&total); err != nil {
		return nil, fmt.Errorf(
			"count row-number range for %s: %w",
			table.Name,
			err,
		)
	}
	if total < 0 {
		return nil, fmt.Errorf(
			"row-number range for %s returned a negative count",
			table.Name,
		)
	}
	return splitAdapterPaginationRowNumberRange(
		total,
		requestedPartitions,
	), nil
}

func splitAdapterPaginationRowNumberRange(
	total int64,
	requestedPartitions int,
) []PaginationRange {
	if total <= 0 || requestedPartitions <= 0 {
		return nil
	}
	partitions := requestedPartitions
	if int64(partitions) > total {
		partitions = int(total)
	}
	width := total / int64(partitions)
	extra := total % int64(partitions)
	first := int64(1)
	ranges := make([]PaginationRange, 0, partitions)
	for index := 0; index < partitions; index++ {
		size := width
		if int64(index) < extra {
			size++
		}
		last := first + size - 1
		ranges = append(ranges, PaginationRange{
			ID:       index,
			FirstRow: first,
			LastRow:  last,
		})
		if index+1 < partitions {
			first = last + 1
		}
	}
	return ranges
}

func adapterPaginationTupleBoundaryQuery(
	sourceEngine string,
	namespace string,
	tableName string,
	keys []schema.Column,
) (string, error) {
	if len(keys) < 2 {
		return "", errors.New(
			"tuple boundary query requires a composite primary key",
		)
	}
	qualified, err := adapterPaginationQualified(
		sourceEngine,
		namespace,
		tableName,
	)
	if err != nil {
		return "", err
	}
	names := make([]string, len(keys))
	for index, key := range keys {
		names[index] = key.Name
	}
	projection := adapterPaginationQuotedColumns(
		sourceEngine,
		names,
	)
	orderAscending := adapterPaginationOrderBy(
		sourceEngine,
		names,
		false,
	)
	orderDescending := adapterPaginationOrderBy(
		sourceEngine,
		names,
		true,
	)
	bucketName := adapterPaginationAlias(
		names,
		"dmtx_pagination_bucket",
	)
	rankName := adapterPaginationAlias(
		append(names, bucketName),
		"dmtx_pagination_boundary_rank",
	)
	bucket := adapterPaginationIdentifier(sourceEngine, bucketName)
	rank := adapterPaginationIdentifier(sourceEngine, rankName)
	placeholder := adapterPaginationPlaceholder(sourceEngine, 1)
	return "WITH dmtx_pagination_ranked AS (" +
		"SELECT " + projection + ", NTILE(" + placeholder +
		") OVER (ORDER BY " + orderAscending + ") AS " + bucket +
		" FROM " + qualified +
		"), dmtx_pagination_boundaries AS (" +
		"SELECT " + projection + ", " + bucket +
		", ROW_NUMBER() OVER (PARTITION BY " + bucket +
		" ORDER BY " + orderDescending + ") AS " + rank +
		" FROM dmtx_pagination_ranked" +
		") SELECT " + bucket + ", " + projection +
		" FROM dmtx_pagination_boundaries" +
		" WHERE " + rank + " = 1 ORDER BY " + bucket, nil
}

func adapterPaginationTupleAfter(
	value []int64,
	previous []int64,
) bool {
	if len(value) != len(previous) {
		return false
	}
	for index := range value {
		switch {
		case value[index] > previous[index]:
			return true
		case value[index] < previous[index]:
			return false
		}
	}
	return false
}

func adapterPaginationInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case int32:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int:
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, errors.New("integer boundary exceeds int64")
		}
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, errors.New("integer boundary exceeds int64")
		}
		return int64(typed), nil
	case string:
		integer, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("decode integer boundary: %w", err)
		}
		return integer, nil
	case []byte:
		integer, err := strconv.ParseInt(string(typed), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("decode integer boundary: %w", err)
		}
		return integer, nil
	default:
		return 0, fmt.Errorf(
			"integer boundary has unexpected driver type %T",
			value,
		)
	}
}

func adapterPaginationTopologyHash(
	sourceEngine string,
	table schema.Table,
	requestedPartitions int,
	evidence []adapterPaginationKeyEvidence,
	plan PaginationPlan,
) (string, error) {
	wire := struct {
		Version             int                            `json:"version"`
		Engine              string                         `json:"engine"`
		Schema              string                         `json:"schema"`
		Table               string                         `json:"table"`
		SQLiteStrict        bool                           `json:"sqlite_strict"`
		SQLiteWithoutRowID  bool                           `json:"sqlite_without_rowid"`
		MySQLCollation      string                         `json:"mysql_collation"`
		RequestedPartitions int                            `json:"requested_partitions"`
		KeyEvidence         []adapterPaginationKeyEvidence `json:"key_evidence"`
		Strategy            PaginationStrategy             `json:"strategy"`
		Keys                []KeySpec                      `json:"keys"`
		Ranges              []PaginationRange              `json:"ranges"`
	}{
		Version:             1,
		Engine:              sourceEngine,
		Schema:              table.Schema,
		Table:               table.Name,
		SQLiteStrict:        table.SQLiteStrict,
		SQLiteWithoutRowID:  table.SQLiteWithoutRowID,
		MySQLCollation:      table.MySQLCollation,
		RequestedPartitions: requestedPartitions,
		KeyEvidence:         evidence,
		Strategy:            plan.Strategy,
		Keys:                plan.Keys,
		Ranges:              plan.Ranges,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode pagination topology: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func cloneAdapterPaginationDeclaration(
	value *schema.DeclaredType,
) *schema.DeclaredType {
	if value == nil {
		return nil
	}
	cloned := *value
	cloned.Arguments = append([]int(nil), value.Arguments...)
	if value.MySQL != nil {
		mysql := *value.MySQL
		mysql.EnumMembers = append(
			[]string(nil),
			value.MySQL.EnumMembers...,
		)
		mysql.SetMembers = append(
			[]string(nil),
			value.MySQL.SetMembers...,
		)
		cloned.MySQL = &mysql
	}
	return &cloned
}

func cloneAdapterPaginationTuple(
	value *KeyTuple,
) *KeyTuple {
	if value == nil {
		return nil
	}
	cloned := append(KeyTuple(nil), (*value)...)
	return &cloned
}

func validateAdapterPaginationIdentifier(
	sourceEngine string,
	kind string,
	value string,
) error {
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("source %s has an empty or invalid name", kind)
	}
	validLength := true
	switch sourceEngine {
	case "postgres":
		validLength = len(value) <= 63
	case "mysql":
		validLength = utf8.RuneCountInString(value) <= 64
	case "mssql":
		validLength = len(utf16.Encode([]rune(value))) <= 128
	case "sqlite":
	default:
		return fmt.Errorf(
			"source engine %q has no identifier admission",
			sourceEngine,
		)
	}
	if !validLength {
		return fmt.Errorf(
			"source %s name exceeds the %s identifier limit",
			kind,
			sourceEngine,
		)
	}
	return nil
}

func adapterPaginationQualified(
	sourceEngine string,
	namespace string,
	table string,
) (string, error) {
	switch sourceEngine {
	case "postgres":
		return postgresQualified(namespace, table), nil
	case "mysql":
		return mySQLQualified(namespace, table), nil
	case "mssql":
		return sqlServerQualified(namespace, table), nil
	case "sqlite":
		return quote(table), nil
	default:
		return "", fmt.Errorf(
			"source engine %q has no pagination table quoting",
			sourceEngine,
		)
	}
}

func adapterPaginationIdentifier(
	sourceEngine string,
	value string,
) string {
	switch sourceEngine {
	case "postgres":
		return postgresIdentifier(value)
	case "mysql":
		return mySQLIdentifier(value)
	case "mssql":
		return sqlServerIdentifier(value)
	default:
		return quote(value)
	}
}

func adapterPaginationQuotedColumns(
	sourceEngine string,
	values []string,
) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = adapterPaginationIdentifier(
			sourceEngine,
			value,
		)
	}
	return strings.Join(quoted, ", ")
}

func adapterPaginationOrderBy(
	sourceEngine string,
	values []string,
	descending bool,
) string {
	order := " ASC"
	if descending {
		order = " DESC"
	}
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = adapterPaginationIdentifier(
			sourceEngine,
			value,
		) + order
	}
	return strings.Join(quoted, ", ")
}

func adapterPaginationPlaceholder(
	sourceEngine string,
	position int,
) string {
	switch sourceEngine {
	case "postgres":
		return "$" + strconv.Itoa(position)
	case "mssql":
		return "@p" + strconv.Itoa(position)
	default:
		return "?"
	}
}

func adapterPaginationAlias(
	columns []string,
	base string,
) string {
	if !slices.ContainsFunc(columns, func(column string) bool {
		return strings.EqualFold(column, base)
	}) {
		return base
	}
	for attempt := 1; ; attempt++ {
		alias := base + "_" + strconv.Itoa(attempt)
		if !slices.ContainsFunc(columns, func(column string) bool {
			return strings.EqualFold(column, alias)
		}) {
			return alias
		}
	}
}

func adapterPaginationPolicy(
	operation string,
	err error,
) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"plan source pagination %s: %w",
			operation,
			err,
		),
	)
}
