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

type sqliteMySQLValueKind uint8

const (
	sqliteMySQLValueInvalid sqliteMySQLValueKind = iota
	sqliteMySQLValueInteger
	sqliteMySQLValueFloat
	sqliteMySQLValueNumericInteger
	sqliteMySQLValueBoolean
	sqliteMySQLValueText
	sqliteMySQLValueBinary
	sqliteMySQLValueDate
	sqliteMySQLValueDateTime
	sqliteMySQLValueUUID
)

type sqliteMySQLValueColumn struct {
	index     int
	name      string
	kind      sqliteMySQLValueKind
	nullable  bool
	precision int
	maxRunes  int
	maxBytes  int
}

type sqliteMySQLSourceProbeKind uint8

const (
	sqliteMySQLSourceProbeInvalid sqliteMySQLSourceProbeKind = iota
	sqliteMySQLSourceProbeNumericStorage
	sqliteMySQLSourceProbeTemporalStorage
	sqliteMySQLSourceProbeCheck
	sqliteMySQLSourceProbeForeignKey
	sqliteMySQLSourceProbeUnique
)

type sqliteMySQLSourceProbe struct {
	kind   sqliteMySQLSourceProbeKind
	table  string
	object string
	query  string
}

type sqliteMySQLSourceProbeRunner interface {
	hasInvalidSQLiteMySQLSourceRow(context.Context, string) (bool, error)
}

type sqliteMySQLSourceDatabase struct {
	database sqliteQueryer
}

func (runner sqliteMySQLSourceDatabase) hasInvalidSQLiteMySQLSourceRow(
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

// preflightSQLiteMySQLSourceData proves every dynamic SQLite value and the
// historical relational state before the MySQL-family target is mutated.
// WriteBatch repeats the same value normalization at the target boundary.
func preflightSQLiteMySQLSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
) error {
	if source == nil {
		return fmt.Errorf(
			"preflight SQLite source data for MySQL target: source adapter is required",
		)
	}
	if source.Engine() != "sqlite" {
		return fmt.Errorf(
			"preflight SQLite source data for MySQL target: source engine is %q",
			source.Engine(),
		)
	}

	probes, err := planSQLiteMySQLSourceProbes(plans)
	if err != nil {
		return err
	}
	if len(probes) != 0 {
		provider, ok := source.(sqliteQueryerProvider)
		if !ok || provider.sqliteSourceQueryer() == nil {
			return fmt.Errorf(
				"preflight SQLite source constraints for MySQL target: source database is not available",
			)
		}
		if err := runSQLiteMySQLSourceProbes(
			ctx,
			sqliteMySQLSourceDatabase{
				database: provider.sqliteSourceQueryer(),
			},
			probes,
		); err != nil {
			return err
		}
	}

	for _, plan := range plans {
		if err := preflightSQLiteMySQLTableValues(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightSQLiteMySQLTableValues(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	ordered, err := sqliteMySQLValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open SQLite table %s for MySQL value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf(
				"close SQLite table %s MySQL value preflight: %w",
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
				"read SQLite table %s row %d during MySQL value preflight: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
		if _, err := normalizeSQLiteMySQLRow(ordered, values); err != nil {
			return fmt.Errorf(
				"preflight SQLite table %s row %d for MySQL target: %w",
				plan.source.Name,
				rowNumber,
				err,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQLite table %s during MySQL value preflight: %w",
			plan.source.Name,
			err,
		)
	}
	return nil
}

func normalizeSQLiteMySQLBatch(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	ordered, err := sqliteMySQLValueColumns(table, columns)
	if err != nil {
		return nil, err
	}
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex], err = normalizeSQLiteMySQLRow(
			ordered,
			row,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL table %s row %d: %w",
				table.Name,
				rowIndex+1,
				err,
			)
		}
	}
	return normalized, nil
}

func sqliteMySQLValueColumns(
	table schema.Table,
	names []string,
) ([]sqliteMySQLValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL table %s: planned schema has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL table %s: planned schema has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
	}

	ordered := make([]sqliteMySQLValueColumn, len(names))
	selected := make(map[string]struct{}, len(names))
	for index, name := range names {
		if _, duplicate := selected[name]; duplicate {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL table %s: selected column %s is duplicated",
				table.Name,
				name,
			)
		}
		selected[name] = struct{}{}
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		value, err := sqliteMySQLValueColumnFromSchema(index, column)
		if err != nil {
			return nil, fmt.Errorf(
				"normalize SQLite-to-MySQL value domain for %s.%s: %w",
				table.Name,
				name,
				err,
			)
		}
		ordered[index] = value
	}
	return ordered, nil
}

func sqliteMySQLValueColumnFromSchema(
	index int,
	column schema.Column,
) (sqliteMySQLValueColumn, error) {
	result := sqliteMySQLValueColumn{
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
		if semantic == "bigint" && noArguments {
			result.kind = sqliteMySQLValueInteger
		}
	case "double":
		if semantic == "double precision" && noArguments {
			result.kind = sqliteMySQLValueFloat
		}
	case "decimal", "numeric":
		if semantic == "numeric" &&
			len(arguments) == 2 &&
			arguments[0] >= 1 &&
			arguments[0] <= 18 &&
			arguments[1] == 0 {
			result.kind = sqliteMySQLValueNumericInteger
			result.precision = arguments[0]
		}
	case "tinyint":
		if semantic == "integer" &&
			len(arguments) == 1 &&
			arguments[0] == 1 {
			result.kind = sqliteMySQLValueBoolean
		}
	case "varchar":
		if (semantic == "varchar" || semantic == "text") &&
			len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 16_383 {
			result.kind = sqliteMySQLValueText
			result.maxRunes = arguments[0]
		}
		if semantic == "uuid" &&
			len(arguments) == 1 &&
			arguments[0] == 36 {
			result.kind = sqliteMySQLValueUUID
			result.maxRunes = 36
		}
	case "char":
		if semantic == "uuid" &&
			len(arguments) == 1 &&
			arguments[0] == 36 {
			result.kind = sqliteMySQLValueUUID
			result.maxRunes = 36
		}
	case "longtext":
		if semantic == "text" && noArguments {
			result.kind = sqliteMySQLValueText
		}
	case "varbinary":
		if (semantic == "blob" || semantic == "varbinary") &&
			len(arguments) == 1 &&
			arguments[0] >= 1 &&
			arguments[0] <= 65_535 {
			result.kind = sqliteMySQLValueBinary
			result.maxBytes = arguments[0]
		}
	case "longblob":
		if semantic == "blob" && noArguments {
			result.kind = sqliteMySQLValueBinary
		}
	case "date":
		if semantic == "date" && noArguments {
			result.kind = sqliteMySQLValueDate
		}
	case "datetime":
		if semantic == "datetime" &&
			len(arguments) == 1 &&
			arguments[0] >= 0 &&
			arguments[0] <= 6 {
			result.kind = sqliteMySQLValueDateTime
			result.precision = arguments[0]
		}
	}
	if result.kind == sqliteMySQLValueInvalid {
		return result, fmt.Errorf(
			"planned type %q with semantic type %q has no exact SQLite source-value contract",
			base,
			semantic,
		)
	}
	return result, nil
}

func normalizeSQLiteMySQLRow(
	columns []sqliteMySQLValueColumn,
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
			continue
		}
		converted, err := normalizeSQLiteMySQLValue(column, value)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", column.name, err)
		}
		normalized[column.index] = converted
	}
	return normalized, nil
}

func normalizeSQLiteMySQLValue(
	column sqliteMySQLValueColumn,
	value any,
) (any, error) {
	switch column.kind {
	case sqliteMySQLValueInteger:
		number, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value is not an exact SQLite INTEGER")
		}
		return number, nil
	case sqliteMySQLValueFloat:
		number, ok := value.(float64)
		if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, fmt.Errorf("value is not a finite SQLite REAL")
		}
		return number, nil
	case sqliteMySQLValueNumericInteger:
		text, ok := value.(string)
		if !ok ||
			!canonicalSQLiteMySQLInteger(text) ||
			!sqliteMySQLIntegerFitsPrecision(text, column.precision) {
			return nil, fmt.Errorf(
				"value is not an exact DECIMAL(%d,0) SQLite INTEGER",
				column.precision,
			)
		}
		return text, nil
	case sqliteMySQLValueBoolean:
		number, ok := value.(int64)
		if !ok || number != 0 && number != 1 {
			return nil, fmt.Errorf("value is not SQLite boolean 0 or 1")
		}
		return number == 1, nil
	case sqliteMySQLValueText:
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) ||
			strings.IndexByte(text, 0) >= 0 {
			return nil, fmt.Errorf(
				"value is not admitted NUL-free UTF-8 text",
			)
		}
		if column.maxRunes > 0 &&
			utf8.RuneCountInString(text) > column.maxRunes {
			return nil, fmt.Errorf(
				"UTF-8 text exceeds planned MySQL character length %d",
				column.maxRunes,
			)
		}
		return text, nil
	case sqliteMySQLValueBinary:
		bytes, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite BLOB bytes")
		}
		if column.maxBytes > 0 && len(bytes) > column.maxBytes {
			return nil, fmt.Errorf(
				"BLOB exceeds planned MySQL byte length %d",
				column.maxBytes,
			)
		}
		owned := make([]byte, len(bytes))
		copy(owned, bytes)
		return owned, nil
	case sqliteMySQLValueDate:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite DATE text")
		}
		date, err := time.Parse("2006-01-02", text)
		if err != nil ||
			date.Year() < 1000 ||
			date.Year() > 9999 ||
			date.Format("2006-01-02") != text {
			return nil, fmt.Errorf(
				"value is not exactly representable as MySQL DATE",
			)
		}
		return date.UTC(), nil
	case sqliteMySQLValueDateTime:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not SQLite DATETIME text")
		}
		timestamp, ok := parseSQLiteMySQLDateTime(
			text,
			column.precision,
		)
		if !ok {
			return nil, fmt.Errorf(
				"value is not exactly representable at MySQL DATETIME(%d)",
				column.precision,
			)
		}
		return timestamp, nil
	case sqliteMySQLValueUUID:
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) {
			return nil, fmt.Errorf("value is not UUID text")
		}
		var identifier pgtype.UUID
		if err := identifier.Scan(text); err != nil ||
			!identifier.Valid ||
			identifier.String() != text {
			return nil, fmt.Errorf(
				"value is not canonical lowercase UUID text",
			)
		}
		return text, nil
	default:
		return nil, fmt.Errorf("unsupported SQLite source-value domain")
	}
}

func canonicalSQLiteMySQLInteger(value string) bool {
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

func sqliteMySQLIntegerFitsPrecision(value string, precision int) bool {
	digits := value
	if strings.HasPrefix(digits, "-") {
		digits = digits[1:]
	}
	return precision >= 1 && len(digits) <= precision
}

func parseSQLiteMySQLDateTime(
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
	timestamp, err := time.Parse(layout, value)
	if err != nil ||
		timestamp.Year() < 1000 ||
		timestamp.Year() > 9999 ||
		timestamp.Format(layout) != value {
		return time.Time{}, false
	}
	return timestamp.UTC(), true
}

func planSQLiteMySQLSourceProbes(
	plans []adapterTablePlan,
) ([]sqliteMySQLSourceProbe, error) {
	probes := make([]sqliteMySQLSourceProbe, 0)
	tables := make(map[string]struct{}, len(plans))
	for _, plan := range plans {
		source := plan.source
		if source.Name == "" {
			return nil, fmt.Errorf(
				"plan SQLite source preflight for MySQL target: source table name is empty",
			)
		}
		if _, duplicate := tables[source.Name]; duplicate {
			return nil, fmt.Errorf(
				"plan SQLite source preflight for MySQL target: duplicate table %s",
				source.Name,
			)
		}
		tables[source.Name] = struct{}{}

		for _, column := range source.Columns {
			if column.DeclaredType == nil {
				return nil, fmt.Errorf(
					"plan SQLite source preflight for MySQL table %s: column %s has no declared type",
					source.Name,
					column.Name,
				)
			}
			base := strings.ToLower(strings.Join(
				strings.Fields(column.DeclaredType.Base),
				" ",
			))
			quoted := quote(column.Name)
			switch base {
			case "numeric", "decimal":
				probes = append(probes, sqliteMySQLSourceProbe{
					kind:   sqliteMySQLSourceProbeNumericStorage,
					table:  source.Name,
					object: column.Name,
					query: "SELECT EXISTS(SELECT 1 FROM " +
						quote(source.Name) + " WHERE " + quoted +
						" IS NOT NULL AND typeof(" + quoted +
						") <> 'integer' LIMIT 1)",
				})
			case "date", "datetime", "timestamp":
				probes = append(probes, sqliteMySQLSourceProbe{
					kind:   sqliteMySQLSourceProbeTemporalStorage,
					table:  source.Name,
					object: column.Name,
					query: "SELECT EXISTS(SELECT 1 FROM " +
						quote(source.Name) + " WHERE " + quoted +
						" IS NOT NULL AND typeof(" + quoted +
						") <> 'text' LIMIT 1)",
				})
			}
		}
		for index, check := range source.Checks {
			canonical := strings.TrimSpace(
				check.Expression.CanonicalSQL(),
			)
			if canonical == "" {
				return nil, fmt.Errorf(
					"plan SQLite source CHECK preflight for MySQL table %s: CHECK %d is empty",
					source.Name,
					index+1,
				)
			}
			probes = append(probes, sqliteMySQLSourceProbe{
				kind:   sqliteMySQLSourceProbeCheck,
				table:  source.Name,
				object: fmt.Sprintf("%d", index+1),
				query: "SELECT EXISTS(SELECT 1 FROM " +
					quote(source.Name) + " WHERE NOT (" +
					canonical + ") LIMIT 1)",
			})
		}
		if len(source.ForeignKeys) != 0 {
			probes = append(probes, sqliteMySQLSourceProbe{
				kind:   sqliteMySQLSourceProbeForeignKey,
				table:  source.Name,
				object: "foreign keys",
				query: "SELECT EXISTS(SELECT 1 FROM " +
					"pragma_foreign_key_check(" +
					sqliteMySQLSourceString(source.Name) +
					") LIMIT 1)",
			})
		}
		for index, unique := range source.Indexes {
			if !unique.Unique {
				continue
			}
			query, err := sqliteMySQLUniqueProbeQuery(source, unique)
			if err != nil {
				return nil, err
			}
			object := unique.Name
			if object == "" {
				object = fmt.Sprintf("inline-%d", index+1)
			}
			probes = append(probes, sqliteMySQLSourceProbe{
				kind:   sqliteMySQLSourceProbeUnique,
				table:  source.Name,
				object: object,
				query:  query,
			})
		}
	}
	return probes, nil
}

func sqliteMySQLUniqueProbeQuery(
	table schema.Table,
	index schema.Index,
) (string, error) {
	if len(index.Columns) == 0 {
		return "", fmt.Errorf(
			"plan SQLite unique preflight for MySQL table %s: unique index has no columns",
			table.Name,
		)
	}
	nonNull := make([]string, len(index.Columns))
	grouped := make([]string, len(index.Columns))
	for position, indexed := range index.Columns {
		if indexed.Name == "" {
			return "", fmt.Errorf(
				"plan SQLite unique preflight for MySQL table %s: unique index has an empty column",
				table.Name,
			)
		}
		if collation := strings.ToUpper(strings.TrimSpace(
			indexed.Collation,
		)); collation != "" && collation != "BINARY" {
			return "", fmt.Errorf(
				"plan SQLite unique preflight for MySQL table %s: unique index column %s is not BINARY-collated",
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

func sqliteMySQLSourceString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func runSQLiteMySQLSourceProbes(
	ctx context.Context,
	runner sqliteMySQLSourceProbeRunner,
	probes []sqliteMySQLSourceProbe,
) error {
	if runner == nil {
		return fmt.Errorf(
			"preflight SQLite source constraints for MySQL target: probe runner is required",
		)
	}
	for _, probe := range probes {
		if probe.table == "" || probe.object == "" ||
			probe.query == "" {
			return fmt.Errorf(
				"preflight SQLite source constraints for MySQL target: incomplete probe metadata",
			)
		}
		invalid, err := runner.hasInvalidSQLiteMySQLSourceRow(
			ctx,
			probe.query,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQLite table %s for MySQL source preflight: %w",
				probe.table,
				err,
			)
		}
		if !invalid {
			continue
		}
		switch probe.kind {
		case sqliteMySQLSourceProbeNumericStorage:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: DECIMAL column %s contains a non-INTEGER storage class",
				probe.table,
				probe.object,
			)
		case sqliteMySQLSourceProbeTemporalStorage:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: temporal column %s contains a non-TEXT storage class",
				probe.table,
				probe.object,
			)
		case sqliteMySQLSourceProbeCheck:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: CHECK %s is violated by historical rows",
				probe.table,
				probe.object,
			)
		case sqliteMySQLSourceProbeForeignKey:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: foreign keys have orphan rows",
				probe.table,
			)
		case sqliteMySQLSourceProbeUnique:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: unique index %s has duplicate fully-nonnull keys",
				probe.table,
				probe.object,
			)
		default:
			return fmt.Errorf(
				"preflight SQLite table %s for MySQL target: unsupported source probe kind %d",
				probe.table,
				probe.kind,
			)
		}
	}
	return nil
}
