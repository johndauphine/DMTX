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

type sqliteQueryerProvider interface {
	sqliteSourceQueryer() sqliteQueryer
}

var _ sqliteQueryerProvider = (*sqliteSourceAdapter)(nil)

func (adapter *sqliteSourceAdapter) sqliteSourceQueryer() sqliteQueryer {
	if adapter == nil {
		return nil
	}
	return adapter.snapshot
}

type sqliteSQLServerValueKind uint8

const (
	sqliteSQLServerValueInvalid sqliteSQLServerValueKind = iota
	sqliteSQLServerValueInteger
	sqliteSQLServerValueFloat
	sqliteSQLServerValueNumericInteger
	sqliteSQLServerValueBoolean
	sqliteSQLServerValueText
	sqliteSQLServerValueBinary
	sqliteSQLServerValueDate
	sqliteSQLServerValueDateTime
	sqliteSQLServerValueUUID
)

type sqliteSQLServerValueColumn struct {
	index     int
	name      string
	kind      sqliteSQLServerValueKind
	nullable  bool
	precision int
	maxBytes  int
}

type sqliteSQLServerSourceProbeKind uint8

const (
	sqliteSQLServerSourceProbeInvalid sqliteSQLServerSourceProbeKind = iota
	sqliteSQLServerSourceProbeNumericStorage
	sqliteSQLServerSourceProbeTemporalStorage
	sqliteSQLServerSourceProbeCheck
	sqliteSQLServerSourceProbeForeignKey
	sqliteSQLServerSourceProbeUnique
)

type sqliteSQLServerSourceProbe struct {
	kind   sqliteSQLServerSourceProbeKind
	table  string
	object string
	query  string
}

type sqliteSQLServerSourceProbeRunner interface {
	hasInvalidSQLiteSQLServerSourceRow(context.Context, string) (bool, error)
}

type sqliteSQLServerSourceDatabase struct {
	database sqliteQueryer
}

func (runner sqliteSQLServerSourceDatabase) hasInvalidSQLiteSQLServerSourceRow(
	ctx context.Context,
	query string,
) (bool, error) {
	if runner.database == nil {
		return false, fmt.Errorf("SQLite source database is not configured")
	}
	var invalid bool
	if err := runner.database.QueryRowContext(ctx, query).Scan(&invalid); err != nil {
		return false, err
	}
	return invalid, nil
}

// preflightSQLiteSQLServerSourceData proves the complete SQLite value and
// historical-constraint state before SQL Server's first mutation. SQLite's
// dynamic typing means catalog declarations alone are not a value contract.
// The same normalization is repeated by WriteBatch to close the source-change
// race at the target boundary.
func preflightSQLiteSQLServerSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
) error {
	if source == nil {
		return fmt.Errorf(
			"preflight SQLite source data for SQL Server: source adapter is required",
		)
	}
	if source.Engine() != "sqlite" {
		return fmt.Errorf(
			"preflight SQLite source data for SQL Server: source engine is %q",
			source.Engine(),
		)
	}

	probes, err := planSQLiteSQLServerSourceProbes(plans)
	if err != nil {
		return err
	}
	if len(probes) != 0 {
		provider, ok := source.(sqliteQueryerProvider)
		if !ok || provider.sqliteSourceQueryer() == nil {
			return fmt.Errorf(
				"preflight SQLite source constraints for SQL Server: source database is not available",
			)
		}
		if err := runSQLiteSQLServerSourceProbes(
			ctx,
			sqliteSQLServerSourceDatabase{
				database: provider.sqliteSourceQueryer(),
			},
			probes,
		); err != nil {
			return err
		}
	}

	for _, plan := range plans {
		if err := preflightSQLiteSQLServerTableValues(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightSQLiteSQLServerTableValues(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	ordered, err := sqliteSQLServerValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open SQLite table %s for SQL Server value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf(
				"close SQLite table %s SQL Server value preflight: %w",
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
				"read SQLite table %s row %d during SQL Server value preflight: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
		if _, err := normalizeSQLiteSQLServerRow(
			ordered,
			values,
		); err != nil {
			return fmt.Errorf(
				"preflight SQLite table %s row %d for SQL Server: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQLite table %s during SQL Server value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	return nil
}

func normalizeSQLiteSQLServerBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	ordered, err := sqliteSQLServerValueColumns(table, columns)
	if err != nil {
		return nil, err
	}
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex], err = normalizeSQLiteSQLServerRow(
			ordered,
			row,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server table %s row %d: %w",
				table.Name,
				rowIndex+1,
				err,
			)
		}
	}
	return normalized, nil
}

func sqliteSQLServerValueColumns(
	table schema.Table,
	names []string,
) ([]sqliteSQLServerValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server table %s: planned schema has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server table %s: planned schema has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
	}

	ordered := make([]sqliteSQLServerValueColumn, len(names))
	selected := make(map[string]struct{}, len(names))
	for index, name := range names {
		if _, duplicate := selected[name]; duplicate {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server table %s: selected column %s is duplicated",
				table.Name,
				name,
			)
		}
		selected[name] = struct{}{}
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		value, err := sqliteSQLServerValueColumnFromSchema(
			index,
			column,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize SQLite-to-SQL-Server value domain for %s.%s: %w",
				table.Name,
				name,
				err,
			)
		}
		ordered[index] = value
	}
	return ordered, nil
}

func sqliteSQLServerValueColumnFromSchema(
	index int,
	column schema.Column,
) (sqliteSQLServerValueColumn, error) {
	result := sqliteSQLServerValueColumn{
		index:    index,
		name:     column.Name,
		nullable: column.Nullable,
	}
	if column.DeclaredType == nil {
		return result, fmt.Errorf("planned declared type is missing")
	}
	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	arguments := column.DeclaredType.Arguments
	noArguments := len(arguments) == 0

	switch base {
	case "bigint":
		if semantic != "bigint" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueInteger
	case "double", "double precision", "float":
		if semantic != "double precision" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueFloat
	case "decimal", "numeric":
		if semantic != "numeric" ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 18 ||
			arguments[1] != 0 {
			break
		}
		result.kind = sqliteSQLServerValueNumericInteger
		result.precision = arguments[0]
	case "bool", "boolean", "bit":
		if semantic != "boolean" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueBoolean
	case "varchar":
		if semantic != "text" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		result.kind = sqliteSQLServerValueText
		result.maxBytes = arguments[0]
	case "text":
		if semantic != "text" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueText
	case "varbinary":
		if semantic != "blob" ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 8_000 {
			break
		}
		result.kind = sqliteSQLServerValueBinary
		result.maxBytes = arguments[0]
	case "blob":
		if semantic != "blob" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueBinary
	case "date":
		if semantic != "date" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueDate
	case "timestamp", "datetime", "datetime2":
		if semantic != "datetime" ||
			len(arguments) != 1 ||
			arguments[0] < 0 ||
			arguments[0] > 6 {
			break
		}
		result.kind = sqliteSQLServerValueDateTime
		result.precision = arguments[0]
	case "uuid", "uniqueidentifier":
		if semantic != "uuid" || !noArguments {
			break
		}
		result.kind = sqliteSQLServerValueUUID
	}
	if result.kind == sqliteSQLServerValueInvalid {
		return result, fmt.Errorf(
			"planned type %q with semantic type %q has no exact SQLite source-value contract",
			base,
			semantic,
		)
	}
	return result, nil
}

func normalizeSQLiteSQLServerRow(
	columns []sqliteSQLServerValueColumn,
	values []any,
) ([]any, error) {
	if len(values) != len(columns) {
		return nil, fmt.Errorf(
			"row width %d does not match selected column count %d",
			len(values),
			len(columns),
		)
	}
	normalized := make([]any, len(values))
	for _, column := range columns {
		value := values[column.index]
		if value == nil {
			if !column.nullable {
				return nil, fmt.Errorf(
					"column %s: NULL violates the planned non-null contract",
					column.name,
				)
			}
			normalized[column.index] = nil
			continue
		}
		converted, err := normalizeSQLiteSQLServerValue(column, value)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", column.name, err)
		}
		normalized[column.index] = converted
	}
	return normalized, nil
}

func normalizeSQLiteSQLServerValue(
	column sqliteSQLServerValueColumn,
	value any,
) (any, error) {
	switch column.kind {
	case sqliteSQLServerValueInteger:
		number, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf(
				"value is not an exact SQLite INTEGER",
			)
		}
		return number, nil
	case sqliteSQLServerValueFloat:
		number, ok := value.(float64)
		if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, fmt.Errorf(
				"value is not a finite SQLite REAL",
			)
		}
		return number, nil
	case sqliteSQLServerValueNumericInteger:
		text, ok := value.(string)
		if !ok ||
			!canonicalSQLiteSQLServerInteger(text) ||
			!sqliteSQLServerIntegerFitsPrecision(text, column.precision) {
			return nil, fmt.Errorf(
				"value is not an exact DECIMAL(%d,0) SQLite INTEGER",
				column.precision,
			)
		}
		return text, nil
	case sqliteSQLServerValueBoolean:
		number, ok := value.(int64)
		if !ok || number != 0 && number != 1 {
			return nil, fmt.Errorf("value is not SQLite boolean 0 or 1")
		}
		return number == 1, nil
	case sqliteSQLServerValueText:
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) ||
			strings.IndexByte(text, 0) >= 0 {
			return nil, fmt.Errorf(
				"value is not admitted NUL-free UTF-8 text",
			)
		}
		if column.maxBytes > 0 && len(text) > column.maxBytes {
			return nil, fmt.Errorf(
				"UTF-8 text exceeds planned SQL Server byte length %d",
				column.maxBytes,
			)
		}
		return text, nil
	case sqliteSQLServerValueBinary:
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite BLOB bytes")
		}
		if column.maxBytes > 0 && len(bytes) > column.maxBytes {
			return nil, fmt.Errorf(
				"BLOB exceeds planned SQL Server byte length %d",
				column.maxBytes,
			)
		}
		owned := make([]byte, len(bytes))
		copy(owned, bytes)
		return owned, nil
	case sqliteSQLServerValueDate:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite DATE text")
		}
		date, err := time.Parse("2006-01-02", text)
		if err != nil ||
			date.Year() < 1 ||
			date.Year() > 9999 ||
			date.Format("2006-01-02") != text {
			return nil, fmt.Errorf(
				"value is not exactly representable as SQL Server DATE",
			)
		}
		return date.UTC(), nil
	case sqliteSQLServerValueDateTime:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite DATETIME text")
		}
		timestamp, ok := parseSQLiteSQLServerDateTime(
			text,
			column.precision,
		)
		if !ok {
			return nil, fmt.Errorf(
				"value is not exactly representable at SQL Server DATETIME2(%d)",
				column.precision,
			)
		}
		return timestamp, nil
	case sqliteSQLServerValueUUID:
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) {
			return nil, fmt.Errorf("value is not UUID text")
		}
		var identifier pgtype.UUID
		if err := identifier.Scan(text); err != nil || !identifier.Valid {
			return nil, fmt.Errorf("value is not a valid UUID")
		}
		return identifier.String(), nil
	default:
		return nil, fmt.Errorf("unsupported SQLite source-value domain")
	}
}

func canonicalSQLiteSQLServerInteger(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value == "-0" || value[0] == '+' {
		return false
	}
	digits := value
	if value[0] == '-' {
		digits = value[1:]
	}
	if digits == "" || digits[0] == '0' {
		return false
	}
	for _, digit := range []byte(digits) {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	integer, ok := new(big.Int).SetString(value, 10)
	return ok && integer.IsInt64()
}

func sqliteSQLServerIntegerFitsPrecision(
	value string,
	precision int,
) bool {
	digits := value
	if strings.HasPrefix(digits, "-") {
		digits = digits[1:]
	}
	return precision >= 1 && len(digits) <= precision
}

func parseSQLiteSQLServerDateTime(
	value string,
	precision int,
) (time.Time, bool) {
	if precision < 0 || precision > 6 {
		return time.Time{}, false
	}
	layout := "2006-01-02 15:04:05"
	if precision > 0 {
		layout += "." + strings.Repeat("0", precision)
	}
	// One canonical source spelling is required. Accepting both a space and
	// T separator, or multiple equivalent fractional spellings, would let
	// distinct SQLite TEXT keys collapse to one SQL Server DATETIME2 value.
	timestamp, err := time.Parse(layout, value)
	if err != nil ||
		timestamp.Year() < 1 ||
		timestamp.Year() > 9999 ||
		timestamp.Format(layout) != value {
		return time.Time{}, false
	}
	return timestamp.UTC(), true
}

func planSQLiteSQLServerSourceProbes(
	plans []adapterTablePlan,
) ([]sqliteSQLServerSourceProbe, error) {
	probes := make([]sqliteSQLServerSourceProbe, 0)
	tables := make(map[string]struct{}, len(plans))
	for _, plan := range plans {
		source := plan.source
		if source.Name == "" {
			return nil, fmt.Errorf(
				"plan SQLite source preflight for SQL Server: source table name is empty",
			)
		}
		if _, duplicate := tables[source.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan SQLite source preflight for SQL Server: duplicate table %s",
				source.Name,
			)
		}
		tables[source.Name] = struct{}{}

		for _, column := range source.Columns {
			if column.DeclaredType == nil {
				return nil, fmt.Errorf(
					"plan SQLite source preflight for SQL Server table %s: column %s has no declared type",
					source.Name,
					column.Name,
				)
			}
			base := strings.ToLower(strings.Join(
				strings.Fields(column.DeclaredType.Base),
				" ",
			))
			if base != "numeric" && base != "decimal" {
				if base != "date" &&
					base != "datetime" &&
					base != "timestamp" {
					continue
				}
				quoted := quote(column.Name)
				probes = append(
					probes,
					sqliteSQLServerSourceProbe{
						kind:   sqliteSQLServerSourceProbeTemporalStorage,
						table:  source.Name,
						object: column.Name,
						query: "SELECT EXISTS(SELECT 1 FROM " +
							quote(source.Name) + " WHERE " + quoted +
							" IS NOT NULL AND typeof(" + quoted +
							") <> 'text' LIMIT 1)",
					},
				)
				continue
			}
			quoted := quote(column.Name)
			probes = append(probes, sqliteSQLServerSourceProbe{
				kind:   sqliteSQLServerSourceProbeNumericStorage,
				table:  source.Name,
				object: column.Name,
				query: "SELECT EXISTS(SELECT 1 FROM " +
					quote(source.Name) + " WHERE " + quoted +
					" IS NOT NULL AND typeof(" + quoted +
					") <> 'integer' LIMIT 1)",
			})
		}
		for index, check := range source.Checks {
			canonical := strings.TrimSpace(
				check.Expression.CanonicalSQL(),
			)
			if canonical == "" {
				return nil, fmt.Errorf(
					"plan SQLite source CHECK preflight for SQL Server table %s: CHECK %d is empty",
					source.Name,
					index+1,
				)
			}
			probes = append(probes, sqliteSQLServerSourceProbe{
				kind:   sqliteSQLServerSourceProbeCheck,
				table:  source.Name,
				object: fmt.Sprintf("%d", index+1),
				query: "SELECT EXISTS(SELECT 1 FROM " +
					quote(source.Name) + " WHERE NOT (" +
					canonical + ") LIMIT 1)",
			})
		}
		if len(source.ForeignKeys) != 0 {
			probes = append(probes, sqliteSQLServerSourceProbe{
				kind:   sqliteSQLServerSourceProbeForeignKey,
				table:  source.Name,
				object: "foreign keys",
				query: "SELECT EXISTS(SELECT 1 FROM " +
					"pragma_foreign_key_check(" +
					sqliteSQLServerSourceString(source.Name) +
					") LIMIT 1)",
			})
		}
		for index, unique := range source.Indexes {
			if !unique.Unique {
				continue
			}
			query, err := sqliteSQLServerUniqueProbeQuery(
				source,
				unique,
			)
			if err != nil {
				return nil, err
			}
			object := unique.Name
			if object == "" {
				object = fmt.Sprintf("inline-%d", index+1)
			}
			probes = append(probes, sqliteSQLServerSourceProbe{
				kind:   sqliteSQLServerSourceProbeUnique,
				table:  source.Name,
				object: object,
				query:  query,
			})
		}
	}
	return probes, nil
}

func sqliteSQLServerUniqueProbeQuery(
	table schema.Table,
	index schema.Index,
) (string, error) {
	if len(index.Columns) == 0 {
		return "", fmt.Errorf(
			"plan SQLite unique preflight for SQL Server table %s: unique index has no columns",
			table.Name,
		)
	}
	nonNull := make([]string, len(index.Columns))
	grouped := make([]string, len(index.Columns))
	for position, indexed := range index.Columns {
		if indexed.Name == "" {
			return "", fmt.Errorf(
				"plan SQLite unique preflight for SQL Server table %s: unique index has an empty column",
				table.Name,
			)
		}
		if collation := strings.ToUpper(strings.TrimSpace(
			indexed.Collation,
		)); collation != "" && collation != "BINARY" {
			return "", fmt.Errorf(
				"plan SQLite unique preflight for SQL Server table %s: unique index column %s is not BINARY-collated",
				table.Name,
				indexed.Name,
			)
		}
		quoted := quote(indexed.Name)
		nonNull[position] = quoted + " IS NOT NULL"
		grouped[position] = quoted + " COLLATE BINARY"
	}
	return "SELECT EXISTS(SELECT 1 FROM " + quote(table.Name) +
		" WHERE " + strings.Join(nonNull, " AND ") +
		" GROUP BY " + strings.Join(grouped, ", ") +
		" HAVING COUNT(*) > 1 LIMIT 1)", nil
}

func sqliteSQLServerSourceString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func runSQLiteSQLServerSourceProbes(
	ctx context.Context,
	runner sqliteSQLServerSourceProbeRunner,
	probes []sqliteSQLServerSourceProbe,
) error {
	if runner == nil {
		return fmt.Errorf(
			"preflight SQLite source constraints for SQL Server: probe runner is required",
		)
	}
	for _, probe := range probes {
		if probe.table == "" || probe.object == "" ||
			probe.query == "" {
			return fmt.Errorf(
				"preflight SQLite source constraints for SQL Server: incomplete probe metadata",
			)
		}
		invalid, err := runner.hasInvalidSQLiteSQLServerSourceRow(
			ctx,
			probe.query,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQLite table %s for SQL Server source preflight: %w",
				probe.table,
				err,
			)
		}
		if !invalid {
			continue
		}
		switch probe.kind {
		case sqliteSQLServerSourceProbeNumericStorage:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: DECIMAL column %s contains a non-INTEGER storage class",
				probe.table,
				probe.object,
			)
		case sqliteSQLServerSourceProbeTemporalStorage:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: temporal column %s contains a non-TEXT storage class",
				probe.table,
				probe.object,
			)
		case sqliteSQLServerSourceProbeCheck:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: CHECK %s is violated by historical rows",
				probe.table,
				probe.object,
			)
		case sqliteSQLServerSourceProbeForeignKey:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: foreign keys have orphan rows",
				probe.table,
			)
		case sqliteSQLServerSourceProbeUnique:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: unique index %s has duplicate fully-nonnull keys",
				probe.table,
				probe.object,
			)
		default:
			return fmt.Errorf(
				"preflight SQLite table %s for SQL Server: unsupported source probe kind %d",
				probe.table,
				probe.kind,
			)
		}
	}
	return nil
}
