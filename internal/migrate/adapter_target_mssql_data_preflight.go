package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

type sqlServerTargetValueKind uint8

const (
	sqlServerTargetValueInvalid sqlServerTargetValueKind = iota
	sqlServerTargetValueDate
	sqlServerTargetValueTime
	sqlServerTargetValueDateTime2
	sqlServerTargetValueReal
	sqlServerTargetValueFloat
)

type sqlServerTargetValueColumn struct {
	index     int
	name      string
	kind      sqlServerTargetValueKind
	nullable  bool
	precision int
}

// PreflightSourceData admits any empty-table identity lifecycle before target
// mutation, then reads cross-engine values whose domains are wider than their
// admitted SQL Server projections. Same-engine values are already constrained
// by SQL Server itself and are checked again at the write boundary.
func (adapter *sqlServerTargetAdapter) PreflightSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
	mode string,
) error {
	if err := preflightSQLServerIdentityPrimers(
		ctx,
		source,
		plans,
		mode,
		adapter.database,
	); err != nil {
		return err
	}
	switch source.Engine() {
	case "postgres", "mysql":
	case "sqlite":
		return preflightSQLiteSQLServerSourceData(
			ctx,
			source,
			plans,
		)
	default:
		return nil
	}
	for _, plan := range plans {
		if err := preflightSourceTableValuesForSQLServer(
			ctx,
			source,
			plan,
		); err != nil {
			return err
		}
	}
	return nil
}

func preflightSQLServerIdentityPrimers(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
	mode string,
	database *sql.DB,
) error {
	targetTables := make([]schema.Table, len(plans))
	for index := range plans {
		targetTables[index] = plans[index].target
	}
	for _, plan := range plans {
		identity := plan.target.Identity
		if identity == nil {
			continue
		}
		count, err := source.CountRows(ctx, plan.source)
		if err != nil {
			return fmt.Errorf(
				"count source table %s for SQL Server identity preflight: %w",
				plan.source.Name,
				err,
			)
		}
		if count < 0 {
			return fmt.Errorf(
				"count source table %s for SQL Server identity preflight: negative row count",
				plan.source.Name,
			)
		}
		if count != 0 || identity.Frontier == nil {
			continue
		}
		if mode == "upsert" {
			if database == nil {
				return fmt.Errorf(
					"preflight SQL Server identity for %s: target database is not configured",
					plan.target.Name,
				)
			}
			if err := preflightSQLServerEmptyUpsertIdentity(
				ctx,
				database,
				plan.target,
			); err != nil {
				return err
			}
			continue
		}
		if mode != "drop_recreate" {
			return fmt.Errorf(
				"preflight SQL Server identity for %s: unsupported target mode %q",
				plan.target.Name,
				mode,
			)
		}
		if err := validateSQLServerEmptyIdentityPrimer(
			plan.target,
			targetTables,
		); err != nil {
			return fmt.Errorf(
				"preflight SQL Server identity for %s: %w",
				plan.target.Name,
				err,
			)
		}
	}
	return nil
}

func preflightSQLServerEmptyUpsertIdentity(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	table schema.Table,
) error {
	if table.Identity == nil || table.Identity.Column == "" {
		return fmt.Errorf(
			"preflight SQL Server identity for %s: identity metadata is missing",
			table.Name,
		)
	}
	var maximum sql.NullInt64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT MAX("+sqlServerIdentifier(table.Identity.Column)+
			") FROM "+
			sqlServerQualified(table.Schema, table.Name),
	).Scan(&maximum); err != nil {
		return newSQLServerSafeOperationError(
			"inspect retained SQL Server identity row maximum for",
			table.Name,
			err,
		)
	}
	var current sql.NullInt64
	if err := queryer.QueryRowContext(
		ctx,
		`SELECT TRY_CONVERT(bigint, identity_column.last_value)
		   FROM sys.identity_columns AS identity_column
		   JOIN sys.tables AS identity_table
		     ON identity_table.object_id = identity_column.object_id
		   JOIN sys.schemas AS identity_schema
		     ON identity_schema.schema_id = identity_table.schema_id
		  WHERE identity_schema.name = @p1
		    AND identity_table.name = @p2
		    AND identity_column.name = @p3`,
		table.Schema,
		table.Name,
		table.Identity.Column,
	).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf(
				"preflight SQL Server identity for %s: retained identity column is missing",
				table.Name,
			)
		}
		return newSQLServerSafeOperationError(
			"inspect retained SQL Server identity frontier for",
			table.Name,
			err,
		)
	}
	if !maximum.Valid && !current.Valid {
		return fmt.Errorf(
			"preflight SQL Server identity for %s: empty source with an allocated frontier cannot be combined with uncalled retained target generator state exactly",
			table.Name,
		)
	}
	return nil
}

func preflightSourceTableValuesForSQLServer(
	ctx context.Context,
	source sourceAdapter,
	plan adapterTablePlan,
) (result error) {
	sourceName := source.DisplayName()
	checked, err := sqlServerTargetValueColumns(
		plan.target,
		plan.columns,
	)
	if err != nil {
		return err
	}
	if len(checked) == 0 {
		return nil
	}
	rows, err := source.OpenRows(ctx, plan.source, plan.columns)
	if err != nil {
		return fmt.Errorf(
			"open %s table %s for SQL Server value preflight: %w",
			sourceName,
			plan.source.Name,
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf(
				"close %s table %s SQL Server value preflight: %w",
				sourceName,
				plan.source.Name,
				closeErr,
			)
			if result != nil {
				result = errors.Join(result, closeErr)
			} else {
				result = closeErr
			}
		}
	}()

	values := make([]any, len(plan.columns))
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf(
				"read %s table %s during SQL Server value preflight: %w",
				sourceName,
				plan.source.Name,
				err,
			)
		}
		if err := validateSQLServerTargetValues(
			plan.target,
			checked,
			values,
		); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate %s table %s during SQL Server value preflight: %w",
			sourceName,
			plan.source.Name,
			err,
		)
	}
	return nil
}

func validateSQLServerTargetBatchValues(
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	checked, err := sqlServerTargetValueColumns(table, columns)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf(
				"validate SQL Server target values for table %s: row width does not match selected columns",
				table.Name,
			)
		}
		if err := validateSQLServerTargetValues(
			table,
			checked,
			row,
		); err != nil {
			return err
		}
	}
	return nil
}

func sqlServerTargetValueColumns(
	table schema.Table,
	columns []string,
) ([]sqlServerTargetValueColumn, error) {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	checked := make([]sqlServerTargetValueColumn, 0)
	for index, name := range columns {
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"validate SQL Server target values for table %s: selected column %s is absent from the planned schema",
				table.Name,
				name,
			)
		}
		value, ok, err := sqlServerTargetValueColumnFromSchema(
			index,
			column,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"validate SQL Server target value domain for %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		if ok {
			checked = append(checked, value)
		}
	}
	return checked, nil
}

func sqlServerTargetValueColumnFromSchema(
	index int,
	column schema.Column,
) (sqlServerTargetValueColumn, bool, error) {
	result := sqlServerTargetValueColumn{
		index:    index,
		name:     column.Name,
		nullable: column.Nullable,
	}
	if column.DeclaredType == nil {
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "date", "time", "datetime", "timestamp", "real", "float",
			"double", "double precision":
			return result, false, fmt.Errorf(
				"tracked value domain has no declared type",
			)
		default:
			return result, false, nil
		}
	}

	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	arguments := column.DeclaredType.Arguments
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	switch base {
	case "date":
		if semantic != "date" || len(arguments) != 0 {
			return result, false, fmt.Errorf(
				"invalid DATE declaration",
			)
		}
		result.kind = sqlServerTargetValueDate
	case "time":
		if semantic != "time" ||
			len(arguments) != 1 ||
			arguments[0] < 0 || arguments[0] > 6 {
			return result, false, fmt.Errorf(
				"invalid TIME declaration",
			)
		}
		result.kind = sqlServerTargetValueTime
		result.precision = arguments[0]
	case "timestamp", "datetime", "datetime2":
		if (semantic != "timestamp" && semantic != "datetime") ||
			len(arguments) != 1 ||
			arguments[0] < 0 || arguments[0] > 6 {
			return result, false, fmt.Errorf(
				"invalid DATETIME2 declaration",
			)
		}
		result.kind = sqlServerTargetValueDateTime2
		result.precision = arguments[0]
	case "real":
		if semantic != "real" || len(arguments) != 0 {
			return result, false, fmt.Errorf(
				"invalid REAL declaration",
			)
		}
		result.kind = sqlServerTargetValueReal
	case "float", "double", "double precision":
		if (semantic != "float" &&
			semantic != "double" &&
			semantic != "double precision") ||
			len(arguments) != 0 {
			return result, false, fmt.Errorf(
				"invalid FLOAT declaration",
			)
		}
		result.kind = sqlServerTargetValueFloat
	case "smalldatetime":
		// PostgreSQL projection never emits SMALLDATETIME. Same-engine
		// values already belong to its narrower native domain.
		return result, false, nil
	default:
		return result, false, nil
	}
	return result, true, nil
}

func validateSQLServerTargetValues(
	table schema.Table,
	columns []sqlServerTargetValueColumn,
	values []any,
) error {
	for _, column := range columns {
		if column.index >= len(values) {
			return sqlServerTargetValueError(
				table,
				column,
				"row is missing the selected column",
			)
		}
		value := values[column.index]
		if value == nil {
			if column.nullable {
				continue
			}
			return sqlServerTargetValueError(
				table,
				column,
				"NULL is outside the non-nullable target domain",
			)
		}
		switch column.kind {
		case sqlServerTargetValueDate:
			timestamp, ok := value.(time.Time)
			if !ok ||
				timestamp.Year() < 1 || timestamp.Year() > 9999 ||
				timestamp.Hour() != 0 ||
				timestamp.Minute() != 0 ||
				timestamp.Second() != 0 ||
				timestamp.Nanosecond() != 0 {
				return sqlServerTargetValueError(
					table,
					column,
					"value is not exactly representable as SQL Server DATE",
				)
			}
		case sqlServerTargetValueTime:
			if !sqlServerTimeValueExact(value, column.precision) {
				return sqlServerTargetValueError(
					table,
					column,
					"value is not exactly representable at the planned SQL Server TIME precision",
				)
			}
		case sqlServerTargetValueDateTime2:
			timestamp, ok := value.(time.Time)
			if !ok ||
				timestamp.Year() < 1 || timestamp.Year() > 9999 ||
				!sqlServerDateTime2PrecisionExact(
					timestamp,
					column.precision,
				) {
				return sqlServerTargetValueError(
					table,
					column,
					"value is not exactly representable at the planned SQL Server DATETIME2 precision",
				)
			}
		case sqlServerTargetValueReal:
			number, ok := sqlServerTargetFloat64(value)
			converted := float32(number)
			if !ok ||
				math.IsNaN(number) ||
				math.IsInf(number, 0) ||
				math.IsInf(float64(converted), 0) ||
				float64(converted) != number {
				return sqlServerTargetValueError(
					table,
					column,
					"value is not a finite exact SQL Server REAL",
				)
			}
		case sqlServerTargetValueFloat:
			number, ok := sqlServerTargetFloat64(value)
			if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
				return sqlServerTargetValueError(
					table,
					column,
					"value is not a finite SQL Server FLOAT",
				)
			}
		default:
			return sqlServerTargetValueError(
				table,
				column,
				"unsupported target value domain",
			)
		}
	}
	return nil
}

func sqlServerTimeValueExact(value any, precision int) bool {
	switch typed := value.(type) {
	case string:
		microseconds, ok := parsePostgresDriverTime(typed)
		if !ok {
			return false
		}
		return sqlServerTemporalFractionExact(
			microseconds*int64(time.Microsecond),
			precision,
		)
	case time.Time:
		// go-mssqldb represents TIME as a time.Time anchored to 0001-01-01.
		// Requiring that exact anchor fails closed if a timestamp-shaped value
		// is accidentally supplied at the write boundary.
		return typed.Year() == 1 &&
			typed.Month() == time.January &&
			typed.Day() == 1 &&
			sqlServerTemporalFractionExact(
				int64(typed.Nanosecond()),
				precision,
			)
	default:
		return false
	}
}

// parsePostgresDriverTime accepts the exact text domain exposed by pgx's
// database/sql codec for PostgreSQL TIME. Extended-query binary results are
// rendered with six fractional digits, while text protocol results may omit
// the fraction or trailing zeroes. PostgreSQL's distinct 24:00:00 value is
// deliberately rejected because SQL Server TIME ends before 24:00:00.
func parsePostgresDriverTime(value string) (int64, bool) {
	if len(value) < 8 || len(value) > 15 ||
		value[2] != ':' || value[5] != ':' {
		return 0, false
	}
	hour, ok := sqlServerTimeTwoDigits(value[0:2])
	if !ok || hour > 23 {
		return 0, false
	}
	minute, ok := sqlServerTimeTwoDigits(value[3:5])
	if !ok || minute > 59 {
		return 0, false
	}
	second, ok := sqlServerTimeTwoDigits(value[6:8])
	if !ok || second > 59 {
		return 0, false
	}

	fraction := 0
	if len(value) > 8 {
		if value[8] != '.' || len(value) == 9 {
			return 0, false
		}
		scale := 100_000
		for index := 9; index < len(value); index++ {
			digit := value[index]
			if digit < '0' || digit > '9' {
				return 0, false
			}
			fraction += int(digit-'0') * scale
			scale /= 10
		}
	}
	seconds := int64((hour*60+minute)*60 + second)
	return seconds*1_000_000 + int64(fraction), true
}

func sqlServerTimeTwoDigits(value string) (int, bool) {
	if len(value) != 2 ||
		value[0] < '0' || value[0] > '9' ||
		value[1] < '0' || value[1] > '9' {
		return 0, false
	}
	return int(value[0]-'0')*10 + int(value[1]-'0'), true
}

func sqlServerDateTime2PrecisionExact(
	value time.Time,
	precision int,
) bool {
	return sqlServerTemporalFractionExact(
		int64(value.Nanosecond()),
		precision,
	)
}

func sqlServerTemporalFractionExact(
	nanoseconds int64,
	precision int,
) bool {
	if precision < 0 || precision > 6 {
		return false
	}
	unit := int64(time.Second)
	for index := 0; index < precision; index++ {
		unit /= 10
	}
	return nanoseconds%unit == 0
}

func sqlServerTargetFloat64(value any) (float64, bool) {
	switch number := value.(type) {
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}

func sqlServerTargetValueError(
	table schema.Table,
	column sqlServerTargetValueColumn,
	reason string,
) error {
	return fmt.Errorf(
		"SQL Server target column %s.%s rejects a source value: %s",
		table.Name,
		column.name,
		reason,
	)
}
