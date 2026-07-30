package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

// modernc SQLite is compiled with SQLITE_MAX_LENGTH=1,000,000,000. Keep one
// megabyte of headroom for the record header and serial-type varints so a row
// admitted by preflight cannot cross the engine limit only when encoded.
const mySQLSQLiteMaximumRowPayloadBytes int64 = 999_000_000

type mySQLSQLiteValueKind uint8

const (
	mySQLSQLiteValueInvalid mySQLSQLiteValueKind = iota
	mySQLSQLiteValueInteger
	mySQLSQLiteValueNumericInteger
	mySQLSQLiteValueText
	mySQLSQLiteValueBinary
	mySQLSQLiteValueDate
	mySQLSQLiteValueTime
	mySQLSQLiteValueDateTime
)

type mySQLSQLiteValueColumn struct {
	column       schema.Column
	kind         mySQLSQLiteValueKind
	minimum      int64
	maximum      int64
	maxRunes     int
	minimumBytes int
	maximumBytes int
	precision    int
}

type mySQLSQLiteConstraintProbeKind uint8

const (
	mySQLSQLiteConstraintProbeInvalid mySQLSQLiteConstraintProbeKind = iota
	mySQLSQLiteConstraintProbeCheck
	mySQLSQLiteConstraintProbeForeignKey
	mySQLSQLiteConstraintProbeUniqueIndex
)

type mySQLSQLiteConstraintProbe struct {
	kind   mySQLSQLiteConstraintProbeKind
	table  string
	object string
	query  string
}

// preflightMySQLSQLiteSourceData proves both the row-value projection and the
// historical constraint state before SQLite's first mutation. MySQL and
// MariaDB can retain rows inserted while CHECK, foreign-key, or unique checks
// were disabled; SQLite will enforce the reconstructed objects during writes.
// Values are checked again at the write boundary to close the value-shape race.
func preflightMySQLSQLiteSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
) error {
	if source == nil {
		return fmt.Errorf(
			"preflight MySQL source data for SQLite: source adapter is required",
		)
	}
	if source.Engine() != "mysql" {
		return fmt.Errorf(
			"preflight MySQL source data for SQLite: source engine is %q",
			source.Engine(),
		)
	}

	probes, err := planMySQLSQLiteConstraintProbes(plans)
	if err != nil {
		return err
	}
	if len(probes) != 0 {
		provider, ok := source.(mysqlDatabaseHandleProvider)
		if !ok || provider.mySQLDatabaseHandle() == nil {
			return fmt.Errorf(
				"preflight MySQL source constraints for SQLite: source database is not available",
			)
		}
		if err := runMySQLSQLiteConstraintProbes(
			ctx,
			postgresMySQLSourceDataDatabase{
				database: provider.mySQLDatabaseHandle(),
			},
			probes,
		); err != nil {
			return err
		}
	}

	for _, plan := range plans {
		if err := preflightMySQLSQLiteTableValues(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightMySQLSQLiteTableValues(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	ordered, err := mySQLSQLiteValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open MySQL table %s for SQLite value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf(
				"close MySQL table %s SQLite value preflight: %w",
				plan.source.Name,
				closeErr,
			)
			if result == nil {
				result = closeErr
			} else {
				result = errors.Join(result, closeErr)
			}
		}
	}()

	values := make([]any, len(plan.columns))
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	rowNumber := int64(0)
	for rows.Next() {
		rowNumber++
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf(
				"read MySQL table %s row %d during SQLite value preflight: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
		if _, err := normalizeMySQLSQLiteRow(ordered, values); err != nil {
			return fmt.Errorf(
				"preflight MySQL table %s row %d for SQLite: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MySQL table %s during SQLite value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	return nil
}

func normalizeMySQLSQLiteBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	ordered, err := mySQLSQLiteValueColumns(table, columns)
	if err != nil {
		return nil, err
	}
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex], err = normalizeMySQLSQLiteRow(
			ordered,
			row,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite table %s row %d: %w",
				table.Name,
				rowIndex+1,
				err,
			)
		}
	}
	return normalized, nil
}

func mySQLSQLiteValueColumns(
	table schema.Table,
	names []string,
) ([]mySQLSQLiteValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite table %s: planned schema has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite table %s: planned schema has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
	}

	ordered := make([]mySQLSQLiteValueColumn, len(names))
	selected := make(map[string]struct{}, len(names))
	for index, name := range names {
		if _, duplicate := selected[name]; duplicate {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite table %s: selected column %s is duplicated",
				table.Name,
				name,
			)
		}
		selected[name] = struct{}{}
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		contract, err := mySQLSQLiteValueColumnFromSchema(column)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize MySQL-to-SQLite column %s.%s: %w",
				table.Name,
				name,
				err,
			)
		}
		ordered[index] = contract
	}
	return ordered, nil
}

func mySQLSQLiteValueColumnFromSchema(
	column schema.Column,
) (mySQLSQLiteValueColumn, error) {
	result := mySQLSQLiteValueColumn{column: column}
	if column.DeclaredType == nil {
		return result, fmt.Errorf("planned declared type is missing")
	}
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
	arguments := column.DeclaredType.Arguments
	noArguments := len(arguments) == 0

	switch semantic {
	case "integer":
		if !noArguments {
			return result, fmt.Errorf("invalid integer projection")
		}
		result.kind = mySQLSQLiteValueInteger
		switch base {
		case "tinyint":
			result.minimum, result.maximum = math.MinInt8, math.MaxInt8
		case "smallint":
			result.minimum, result.maximum = math.MinInt16, math.MaxInt16
		case "mediumint":
			result.minimum, result.maximum = -8_388_608, 8_388_607
		case "int":
			result.minimum, result.maximum = math.MinInt32, math.MaxInt32
		default:
			return result, fmt.Errorf("invalid integer projection")
		}
	case "bigint":
		if base != "bigint" || !noArguments {
			return result, fmt.Errorf("invalid BIGINT projection")
		}
		result.kind = mySQLSQLiteValueInteger
		result.minimum, result.maximum = math.MinInt64, math.MaxInt64
	case "numeric":
		if base != "bigint" || !noArguments {
			return result, fmt.Errorf("invalid exact DECIMAL projection")
		}
		result.kind = mySQLSQLiteValueNumericInteger
	case "varchar":
		if base != "varchar" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 16_383 {
			return result, fmt.Errorf("invalid VARCHAR projection")
		}
		result.kind = mySQLSQLiteValueText
		result.maxRunes = arguments[0]
	case "text":
		if base != "text" || !noArguments {
			return result, fmt.Errorf("invalid TEXT projection")
		}
		result.kind = mySQLSQLiteValueText
	case "binary":
		if base != "binary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 255 {
			return result, fmt.Errorf("invalid BINARY projection")
		}
		result.kind = mySQLSQLiteValueBinary
		result.minimumBytes = arguments[0]
		result.maximumBytes = arguments[0]
	case "varbinary":
		if base != "varbinary" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 65_535 {
			return result, fmt.Errorf("invalid VARBINARY projection")
		}
		result.kind = mySQLSQLiteValueBinary
		result.maximumBytes = arguments[0]
	case "blob":
		if base != "blob" || !noArguments {
			return result, fmt.Errorf("invalid BLOB projection")
		}
		result.kind = mySQLSQLiteValueBinary
		result.maximumBytes = int(mySQLSQLiteMaximumRowPayloadBytes)
	case "date":
		if base != "date" || !noArguments {
			return result, fmt.Errorf("invalid DATE projection")
		}
		result.kind = mySQLSQLiteValueDate
	case "time":
		if base != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return result, fmt.Errorf("invalid TIME projection")
		}
		result.kind = mySQLSQLiteValueTime
		result.precision = arguments[0]
	case "datetime", "timestamp":
		if base != semantic ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			return result, fmt.Errorf(
				"invalid %s projection",
				strings.ToUpper(semantic),
			)
		}
		result.kind = mySQLSQLiteValueDateTime
		result.precision = arguments[0]
	default:
		return result, fmt.Errorf(
			"source type %q has no exact SQLite value contract",
			semantic,
		)
	}
	return result, nil
}

func normalizeMySQLSQLiteRow(
	columns []mySQLSQLiteValueColumn,
	row []any,
) ([]any, error) {
	if len(row) != len(columns) {
		return nil, fmt.Errorf(
			"row has %d values for %d selected columns",
			len(row),
			len(columns),
		)
	}
	normalized := make([]any, len(row))
	var payloadBytes int64
	for index, value := range row {
		column := columns[index]
		if value == nil {
			if !column.column.Nullable {
				return nil, fmt.Errorf(
					"column %s: NULL violates the planned non-null contract",
					column.column.Name,
				)
			}
			payloadBytes++
			continue
		}
		converted, err := normalizeMySQLSQLiteValue(column, value)
		if err != nil {
			return nil, fmt.Errorf(
				"column %s: %w",
				column.column.Name,
				err,
			)
		}
		nextPayload, ok := addMySQLSQLiteRowPayload(
			payloadBytes,
			converted,
		)
		if !ok {
			return nil, fmt.Errorf(
				"column %s: row exceeds the conservative SQLite storage limit",
				column.column.Name,
			)
		}
		payloadBytes = nextPayload
		normalized[index] = converted
	}
	return normalized, nil
}

func normalizeMySQLSQLiteValue(
	column mySQLSQLiteValueColumn,
	value any,
) (any, error) {
	switch column.kind {
	case mySQLSQLiteValueInteger:
		number, ok := value.(int64)
		if !ok ||
			number < column.minimum ||
			number > column.maximum {
			return nil, fmt.Errorf(
				"value is not an exact signed source-width integer",
			)
		}
		return number, nil
	case mySQLSQLiteValueNumericInteger:
		return normalizeMySQLSQLiteDecimal(value)
	case mySQLSQLiteValueText:
		bytes, ok := value.([]byte)
		if !ok ||
			!utf8.Valid(bytes) ||
			bytesContainNUL(bytes) ||
			int64(len(bytes)) > mySQLSQLiteMaximumRowPayloadBytes {
			return nil, fmt.Errorf(
				"value is not admitted MySQL UTF-8 text",
			)
		}
		text := string(bytes)
		if column.maxRunes > 0 &&
			utf8.RuneCountInString(text) > column.maxRunes {
			return nil, fmt.Errorf(
				"value exceeds the planned VARCHAR length %d",
				column.maxRunes,
			)
		}
		return text, nil
	case mySQLSQLiteValueBinary:
		bytes, ok := value.([]byte)
		if !ok ||
			len(bytes) < column.minimumBytes ||
			column.maximumBytes > 0 &&
				len(bytes) > column.maximumBytes {
			return nil, fmt.Errorf(
				"value is outside the planned binary length domain",
			)
		}
		return append([]byte(nil), bytes...), nil
	case mySQLSQLiteValueDate:
		return normalizeMySQLSQLiteDate(value)
	case mySQLSQLiteValueTime:
		text, ok := value.(string)
		if !ok || !validMySQLClockTime(text, column.precision) {
			return nil, fmt.Errorf(
				"value is not canonical MySQL TIME text at the planned precision",
			)
		}
		return text, nil
	case mySQLSQLiteValueDateTime:
		return normalizeMySQLSQLiteDateTime(
			value,
			column.precision,
		)
	default:
		return nil, fmt.Errorf("planned value contract is unsupported")
	}
}

func normalizeMySQLSQLiteDecimal(value any) (int64, error) {
	bytes, ok := value.([]byte)
	if !ok || !utf8.Valid(bytes) {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	numeric, err := normalizePostgresNumericWithModifiers(
		bytes,
		18,
		0,
	)
	if err != nil ||
		!numeric.Valid ||
		numeric.NaN ||
		numeric.InfinityModifier != pgtype.Finite ||
		numeric.Int == nil {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	integer := new(big.Int).Set(numeric.Int)
	if numeric.Exp > 0 {
		integer.Mul(
			integer,
			new(big.Int).Exp(
				big.NewInt(10),
				big.NewInt(int64(numeric.Exp)),
				nil,
			),
		)
	} else if numeric.Exp < 0 {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	if !integer.IsInt64() {
		return 0, fmt.Errorf(
			"value does not fit exact SQLite INTEGER storage",
		)
	}
	return integer.Int64(), nil
}

func normalizeMySQLSQLiteDate(value any) (string, error) {
	temporal, ok := value.(time.Time)
	if !ok ||
		!mySQLSQLiteUTC(temporal) ||
		temporal.Year() < 1000 ||
		temporal.Year() > 9999 ||
		temporal.Hour() != 0 ||
		temporal.Minute() != 0 ||
		temporal.Second() != 0 ||
		temporal.Nanosecond() != 0 {
		return "", fmt.Errorf(
			"value is not exactly representable as SQLite DATE text",
		)
	}
	return temporal.Format("2006-01-02"), nil
}

func normalizeMySQLSQLiteDateTime(
	value any,
	precision int,
) (string, error) {
	temporal, ok := value.(time.Time)
	if !ok ||
		!mySQLSQLiteUTC(temporal) ||
		temporal.Year() < 1000 ||
		temporal.Year() > 9999 ||
		!mySQLSQLiteFractionExact(
			temporal.Nanosecond(),
			precision,
		) {
		return "", fmt.Errorf(
			"value is not exactly representable at the planned SQLite temporal precision",
		)
	}
	layout := "2006-01-02 15:04:05"
	if precision > 0 {
		layout += "." + strings.Repeat("0", precision)
	}
	return temporal.Format(layout), nil
}

func mySQLSQLiteUTC(value time.Time) bool {
	_, offset := value.Zone()
	return offset == 0
}

func mySQLSQLiteFractionExact(
	nanoseconds int,
	precision int,
) bool {
	if nanoseconds < 0 ||
		nanoseconds >= int(time.Second) ||
		precision < 0 ||
		precision > 6 {
		return false
	}
	unit := int(time.Second)
	for digit := 0; digit < precision; digit++ {
		unit /= 10
	}
	return nanoseconds%unit == 0
}

func bytesContainNUL(value []byte) bool {
	for _, current := range value {
		if current == 0 {
			return true
		}
	}
	return false
}

func addMySQLSQLiteRowPayload(
	current int64,
	value any,
) (int64, bool) {
	var size int64
	switch typed := value.(type) {
	case string:
		size = int64(len(typed))
	case []byte:
		size = int64(len(typed))
	default:
		// INTEGER values and record serial types need at most a handful of
		// bytes. Sixteen is a deliberately conservative charge.
		size = 16
	}
	if current < 0 ||
		size < 0 ||
		current > mySQLSQLiteMaximumRowPayloadBytes-size {
		return 0, false
	}
	return current + size, true
}

func planMySQLSQLiteConstraintProbes(
	plans []adapterTablePlan,
) ([]mySQLSQLiteConstraintProbe, error) {
	probes := make([]mySQLSQLiteConstraintProbe, 0)
	for _, plan := range plans {
		tableProbes, err := planMySQLSQLiteTableConstraintProbes(plan)
		if err != nil {
			return nil, err
		}
		probes = append(probes, tableProbes...)
	}
	return probes, nil
}

func planMySQLSQLiteTableConstraintProbes(
	plan adapterTablePlan,
) ([]mySQLSQLiteConstraintProbe, error) {
	source := plan.source
	if source.Schema == "" || source.Name == "" {
		return nil, fmt.Errorf(
			"plan MySQL source constraint preflight for SQLite: source table identity is incomplete",
		)
	}
	if plan.target.Name != source.Name {
		return nil, fmt.Errorf(
			"plan MySQL source constraint preflight for SQLite table %s: target table name is %q",
			source.Name,
			plan.target.Name,
		)
	}
	columns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: source column name is empty",
				source.Name,
			)
		}
		if _, duplicate := columns[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: duplicate source column %s",
				source.Name,
				column.Name,
			)
		}
		columns[column.Name] = column
	}

	qualified := mySQLQualified(source.Schema, source.Name)
	probes := make([]mySQLSQLiteConstraintProbe, 0)
	checkNames := make(map[string]struct{}, len(source.Checks))
	for _, check := range source.Checks {
		if check.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: CHECK name is empty",
				source.Name,
			)
		}
		if _, duplicate := checkNames[check.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: duplicate CHECK %s",
				source.Name,
				check.Name,
			)
		}
		checkNames[check.Name] = struct{}{}
		rendered, err := schema.RenderPortableCheckForMySQL(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite CHECK %s.%s: %w",
				source.Name,
				check.Name,
				err,
			)
		}
		probes = append(probes, mySQLSQLiteConstraintProbe{
			kind:   mySQLSQLiteConstraintProbeCheck,
			table:  source.Name,
			object: check.Name,
			query: "SELECT EXISTS (SELECT 1 FROM " + qualified +
				" WHERE NOT (" + rendered + "))",
		})
	}

	foreignKeyNames := make(map[string]struct{}, len(source.ForeignKeys))
	for _, foreignKey := range source.ForeignKeys {
		if foreignKey.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: foreign key name is empty",
				source.Name,
			)
		}
		if _, duplicate := foreignKeyNames[foreignKey.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: duplicate foreign key %s",
				source.Name,
				foreignKey.Name,
			)
		}
		foreignKeyNames[foreignKey.Name] = struct{}{}
		query, err := postgresMySQLForeignKeyProbeQuery(
			source,
			columns,
			foreignKey,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite: %w",
				err,
			)
		}
		probes = append(probes, mySQLSQLiteConstraintProbe{
			kind:   mySQLSQLiteConstraintProbeForeignKey,
			table:  source.Name,
			object: foreignKey.Name,
			query:  query,
		})
	}

	indexNames := make(map[string]struct{}, len(source.Indexes))
	for _, index := range source.Indexes {
		if !index.Unique {
			continue
		}
		if index.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: unique index name is empty",
				source.Name,
			)
		}
		if _, duplicate := indexNames[index.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite table %s: duplicate unique index %s",
				source.Name,
				index.Name,
			)
		}
		indexNames[index.Name] = struct{}{}
		query, err := postgresMySQLUniqueIndexProbeQuery(
			source,
			columns,
			index,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL source constraint preflight for SQLite: %w",
				err,
			)
		}
		probes = append(probes, mySQLSQLiteConstraintProbe{
			kind:   mySQLSQLiteConstraintProbeUniqueIndex,
			table:  source.Name,
			object: index.Name,
			query:  query,
		})
	}
	return probes, nil
}

func runMySQLSQLiteConstraintProbes(
	ctx context.Context,
	runner postgresMySQLSourceDataProbeRunner,
	probes []mySQLSQLiteConstraintProbe,
) error {
	if runner == nil {
		return fmt.Errorf(
			"preflight MySQL source constraints for SQLite: probe runner is required",
		)
	}
	for _, probe := range probes {
		description, err := mySQLSQLiteConstraintProbeDescription(probe)
		if err != nil {
			return err
		}
		invalid, err := runner.hasInvalidRow(ctx, probe.query)
		if err != nil {
			return fmt.Errorf(
				"inspect MySQL table %s for SQLite %s preflight: %w",
				probe.table,
				description,
				err,
			)
		}
		if !invalid {
			continue
		}
		switch probe.kind {
		case mySQLSQLiteConstraintProbeCheck:
			return fmt.Errorf(
				"preflight MySQL table %s for SQLite: CHECK %s is violated by historical rows",
				probe.table,
				probe.object,
			)
		case mySQLSQLiteConstraintProbeForeignKey:
			return fmt.Errorf(
				"preflight MySQL table %s for SQLite: foreign key %s has orphan rows",
				probe.table,
				probe.object,
			)
		case mySQLSQLiteConstraintProbeUniqueIndex:
			return fmt.Errorf(
				"preflight MySQL table %s for SQLite: unique index %s has duplicate fully-nonnull keys",
				probe.table,
				probe.object,
			)
		default:
			return fmt.Errorf(
				"preflight MySQL table %s for SQLite: unsupported constraint probe kind %d",
				probe.table,
				probe.kind,
			)
		}
	}
	return nil
}

func mySQLSQLiteConstraintProbeDescription(
	probe mySQLSQLiteConstraintProbe,
) (string, error) {
	if probe.table == "" || probe.object == "" || probe.query == "" {
		return "", fmt.Errorf(
			"preflight MySQL source constraints for SQLite: incomplete probe metadata",
		)
	}
	switch probe.kind {
	case mySQLSQLiteConstraintProbeCheck:
		return "CHECK " + probe.object, nil
	case mySQLSQLiteConstraintProbeForeignKey:
		return "foreign key " + probe.object, nil
	case mySQLSQLiteConstraintProbeUniqueIndex:
		return "unique index " + probe.object, nil
	default:
		return "", fmt.Errorf(
			"preflight MySQL table %s for SQLite: unsupported constraint probe kind %d",
			probe.table,
			probe.kind,
		)
	}
}
