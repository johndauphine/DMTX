package migrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

// incrementalSourceAdapter is deliberately separate from sourceAdapter. A
// source is not eligible for date-based incremental transfer merely because it
// can perform an ordered full-table read.
type incrementalSourceAdapter interface {
	IncrementalTable(schema.Table) (IncrementalTable, error)
	SampleIncrementalUpperFence(
		context.Context,
		schema.Table,
		IncrementalColumn,
	) (*time.Time, error)
	OpenIncrementalRows(
		context.Context,
		schema.Table,
		[]string,
		IncrementalReadPlan,
	) (adapterRows, error)
}

var (
	_ incrementalSourceAdapter = (*relationalSourceAdapter)(nil)
	_ incrementalSourceAdapter = (*sqliteSourceAdapter)(nil)
)

// requireIncrementalSourceAdapter is the pre-mutation capability gate used by
// later route composition. ClickHouse is intentionally not in the supported
// set: Stage 4 incremental sync requires a relational, duplicate-safe target
// lifecycle.
func requireIncrementalSourceAdapter(
	source sourceAdapter,
) (incrementalSourceAdapter, error) {
	if source == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental source adapter is required"),
		)
	}
	if source.Engine() == "clickhouse" {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"ClickHouse source does not support date-based incremental transfer",
			),
		)
	}
	capability, ok := source.(incrementalSourceAdapter)
	if !ok {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"%s source does not implement date-based incremental transfer",
				source.DisplayName(),
			),
		)
	}
	return capability, nil
}

func (adapter *relationalSourceAdapter) IncrementalTable(
	table schema.Table,
) (IncrementalTable, error) {
	if adapter == nil {
		return IncrementalTable{}, incrementalSourcePolicy(
			"map catalog",
			errors.New("relational source adapter is required"),
		)
	}
	return buildAdapterIncrementalTable(
		adapter.spec.engine,
		adapter.namespace,
		table,
	)
}

func (adapter *sqliteSourceAdapter) IncrementalTable(
	table schema.Table,
) (IncrementalTable, error) {
	return buildAdapterIncrementalTable("sqlite", "", table)
}

type adapterIncrementalTemporal struct {
	kind      IncrementalTemporalKind
	base      string
	precision int
	zoneAware bool
}

func buildAdapterIncrementalTable(
	sourceEngine string,
	namespace string,
	table schema.Table,
) (IncrementalTable, error) {
	if err := validateAdapterIncrementalIdentifier(
		sourceEngine,
		"table",
		table.Name,
	); err != nil {
		return IncrementalTable{}, incrementalSourcePolicy(
			"map catalog",
			err,
		)
	}
	switch sourceEngine {
	case "postgres", "mysql", "mssql":
		if err := validateAdapterIncrementalIdentifier(
			sourceEngine,
			"schema",
			namespace,
		); err != nil {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				err,
			)
		}
		if table.Schema != namespace {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
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
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				fmt.Errorf(
					"SQLite source table %s has unexpected schema %q",
					table.Name,
					table.Schema,
				),
			)
		}
	default:
		return IncrementalTable{}, incrementalSourcePolicy(
			"map catalog",
			fmt.Errorf(
				"source engine %q has no incremental catalog admission",
				sourceEngine,
			),
		)
	}

	result := IncrementalTable{
		Schema:  table.Schema,
		Name:    table.Name,
		Columns: make([]IncrementalColumn, 0, len(table.Columns)),
	}
	seen := make(map[string]struct{}, len(table.Columns))
	for index, column := range table.Columns {
		if err := validateAdapterIncrementalIdentifier(
			sourceEngine,
			"column",
			column.Name,
		); err != nil {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				fmt.Errorf(
					"source table %s column %d: %w",
					table.Name,
					index,
					err,
				),
			)
		}
		if _, exists := seen[column.Name]; exists {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				fmt.Errorf(
					"source table %s has duplicate column %q",
					table.Name,
					column.Name,
				),
			)
		}
		seen[column.Name] = struct{}{}
		if column.PrimaryKey != (column.PrimaryKeyPosition > 0) {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				fmt.Errorf(
					"source table %s column %s has contradictory primary-key metadata",
					table.Name,
					column.Name,
				),
			)
		}
		if column.PrimaryKey && column.Nullable {
			return IncrementalTable{}, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"incremental table %s primary-key column %s is nullable",
					table.Name,
					column.Name,
				),
			)
		}
		temporal, admitted, err := adapterIncrementalTemporalColumn(
			sourceEngine,
			column,
		)
		if err != nil {
			return IncrementalTable{}, incrementalSourcePolicy(
				"map catalog",
				fmt.Errorf(
					"source table %s column %s: %w",
					table.Name,
					column.Name,
					err,
				),
			)
		}
		mapped := IncrementalColumn{
			Name:               column.Name,
			Nullable:           column.Nullable,
			PrimaryKeyPosition: column.PrimaryKeyPosition,
		}
		if admitted {
			mapped.TemporalKind = temporal.kind
			mapped.OrderAdmission = IncrementalOrderExact
		}
		if column.PrimaryKey {
			// Stage 3 source discovery has already admitted the exact native
			// primary-key order. Incremental resume never restores a
			// positional cursor, but a complete deterministic tie-breaker is
			// still mandatory within each timestamp.
			mapped.OrderAdmission = IncrementalOrderExact
		}
		result.Columns = append(result.Columns, mapped)
	}
	if _, err := BuildIncrementalTablePlan(result, nil); err != nil {
		return IncrementalTable{}, err
	}
	return result, nil
}

func validateAdapterIncrementalIdentifier(
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

func adapterIncrementalTemporalColumn(
	sourceEngine string,
	column schema.Column,
) (adapterIncrementalTemporal, bool, error) {
	if column.Type != strings.ToLower(strings.TrimSpace(column.Type)) {
		return adapterIncrementalTemporal{}, false, fmt.Errorf(
			"catalog type %q is not canonical",
			column.Type,
		)
	}
	switch sourceEngine {
	case "postgres":
		return postgresIncrementalTemporal(column)
	case "mysql":
		return mySQLIncrementalTemporal(column)
	case "mssql":
		return sqlServerIncrementalTemporal(column)
	case "sqlite":
		return sqliteIncrementalTemporal(column)
	default:
		return adapterIncrementalTemporal{}, false, fmt.Errorf(
			"source engine %q is unsupported",
			sourceEngine,
		)
	}
}

func postgresIncrementalTemporal(
	column schema.Column,
) (adapterIncrementalTemporal, bool, error) {
	switch column.Type {
	case "date":
		if column.DeclaredType != nil {
			return adapterIncrementalTemporal{}, false, errors.New(
				"PostgreSQL date has an unexpected declaration",
			)
		}
		return adapterIncrementalTemporal{
			kind: IncrementalTemporalDate,
			base: "date",
		}, true, nil
	case "timestamp", "timestamptz":
		precision, err := incrementalFractionalPrecision(
			column.DeclaredType,
			column.Type,
			6,
			6,
			true,
			false,
		)
		if err != nil {
			return adapterIncrementalTemporal{}, false, err
		}
		return adapterIncrementalTemporal{
			kind:      IncrementalTemporalTimestamp,
			base:      column.Type,
			precision: precision,
			zoneAware: column.Type == "timestamptz",
		}, true, nil
	default:
		return adapterIncrementalTemporal{}, false, nil
	}
}

func mySQLIncrementalTemporal(
	column schema.Column,
) (adapterIncrementalTemporal, bool, error) {
	switch column.Type {
	case "date":
		if !incrementalExactDeclaration(column.DeclaredType, "date") {
			return adapterIncrementalTemporal{}, false, errors.New(
				"MySQL date declaration is missing or invalid",
			)
		}
		return adapterIncrementalTemporal{
			kind: IncrementalTemporalDate,
			base: "date",
		}, true, nil
	case "datetime", "timestamp":
		precision, err := incrementalFractionalPrecision(
			column.DeclaredType,
			column.Type,
			0,
			6,
			false,
			false,
		)
		if err != nil {
			return adapterIncrementalTemporal{}, false, err
		}
		return adapterIncrementalTemporal{
			kind:      IncrementalTemporalTimestamp,
			base:      column.Type,
			precision: precision,
			zoneAware: column.Type == "timestamp",
		}, true, nil
	default:
		return adapterIncrementalTemporal{}, false, nil
	}
}

func sqlServerIncrementalTemporal(
	column schema.Column,
) (adapterIncrementalTemporal, bool, error) {
	switch column.Type {
	case "date":
		if !incrementalExactDeclaration(column.DeclaredType, "date") {
			return adapterIncrementalTemporal{}, false, errors.New(
				"SQL Server date declaration is missing or invalid",
			)
		}
		return adapterIncrementalTemporal{
			kind: IncrementalTemporalDate,
			base: "date",
		}, true, nil
	case "datetime":
		if incrementalExactDeclaration(column.DeclaredType, "smalldatetime") {
			return adapterIncrementalTemporal{
				kind:      IncrementalTemporalTimestamp,
				base:      "smalldatetime",
				precision: 0,
			}, true, nil
		}
		precision, err := incrementalFractionalPrecision(
			column.DeclaredType,
			"timestamp",
			0,
			6,
			false,
			false,
		)
		if err != nil {
			return adapterIncrementalTemporal{}, false, err
		}
		return adapterIncrementalTemporal{
			kind:      IncrementalTemporalTimestamp,
			base:      "timestamp",
			precision: precision,
		}, true, nil
	default:
		return adapterIncrementalTemporal{}, false, nil
	}
}

func sqliteIncrementalTemporal(
	column schema.Column,
) (adapterIncrementalTemporal, bool, error) {
	switch column.Type {
	case "date":
		if !incrementalExactDeclaration(column.DeclaredType, "date") {
			return adapterIncrementalTemporal{}, false, errors.New(
				"SQLite date declaration is missing or invalid",
			)
		}
		return adapterIncrementalTemporal{
			kind: IncrementalTemporalDate,
			base: "date",
		}, true, nil
	case "datetime", "timestamp":
		precision, err := incrementalFractionalPrecision(
			column.DeclaredType,
			column.Type,
			0,
			9,
			false,
			true,
		)
		if err != nil {
			return adapterIncrementalTemporal{}, false, err
		}
		return adapterIncrementalTemporal{
			kind:      IncrementalTemporalTimestamp,
			base:      column.Type,
			precision: precision,
		}, true, nil
	default:
		return adapterIncrementalTemporal{}, false, nil
	}
}

func incrementalExactDeclaration(
	declaration *schema.DeclaredType,
	base string,
) bool {
	return declaration != nil &&
		declaration.Base == base &&
		len(declaration.Arguments) == 0 &&
		!incrementalHasStructuredModifiers(declaration)
}

func incrementalFractionalPrecision(
	declaration *schema.DeclaredType,
	base string,
	defaultPrecision int,
	maxPrecision int,
	allowMissing bool,
	allowImplicit bool,
) (int, error) {
	if declaration == nil {
		if allowMissing {
			return defaultPrecision, nil
		}
		return 0, fmt.Errorf("%s declaration is missing", base)
	}
	if declaration.Base != base {
		return 0, fmt.Errorf(
			"%s declaration has base %q",
			base,
			declaration.Base,
		)
	}
	if declaration.Length != nil ||
		declaration.Precision != nil ||
		declaration.Scale != nil ||
		declaration.Spatial != nil ||
		declaration.MySQL != nil {
		return 0, fmt.Errorf("%s declaration has incompatible modifiers", base)
	}
	var precision int
	switch {
	case len(declaration.Arguments) == 1 &&
		declaration.FractionalSecondPrecision == nil:
		precision = declaration.Arguments[0]
	case len(declaration.Arguments) == 0 &&
		declaration.FractionalSecondPrecision != nil:
		fixed := *declaration.FractionalSecondPrecision
		if fixed < 0 || fixed > int64(maxPrecision) {
			return 0, fmt.Errorf(
				"%s precision %d is outside 0..%d",
				base,
				fixed,
				maxPrecision,
			)
		}
		precision = int(fixed)
	case len(declaration.Arguments) == 0:
		if !allowImplicit {
			return 0, fmt.Errorf(
				"%s declaration is missing explicit precision",
				base,
			)
		}
		precision = defaultPrecision
	default:
		return 0, fmt.Errorf(
			"%s declaration has an invalid precision shape",
			base,
		)
	}
	if precision < 0 || precision > maxPrecision {
		return 0, fmt.Errorf(
			"%s precision %d is outside 0..%d",
			base,
			precision,
			maxPrecision,
		)
	}
	return precision, nil
}

func incrementalHasStructuredModifiers(
	declaration *schema.DeclaredType,
) bool {
	return declaration.Length != nil ||
		declaration.Precision != nil ||
		declaration.Scale != nil ||
		declaration.FractionalSecondPrecision != nil ||
		declaration.Spatial != nil ||
		declaration.MySQL != nil
}

type adapterIncrementalQuery struct {
	SQL  string
	Args []any
}

func buildAdapterIncrementalFenceQuery(
	sourceEngine string,
	namespace string,
	table schema.Table,
	column schema.Column,
) (adapterIncrementalQuery, error) {
	temporal, admitted, err := adapterIncrementalTemporalColumn(
		sourceEngine,
		column,
	)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	if !admitted {
		return adapterIncrementalQuery{}, fmt.Errorf(
			"column %s is not an admitted date or timestamp",
			column.Name,
		)
	}
	identifier := adapterIncrementalIdentifier(sourceEngine, column.Name)
	qualified, err := adapterIncrementalQualified(
		sourceEngine,
		namespace,
		table.Name,
	)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	if sourceEngine == "sqlite" {
		valid := sqliteIncrementalTemporalValidity(identifier, temporal)
		return adapterIncrementalQuery{
			SQL: "SELECT COALESCE(MAX(CASE WHEN " + valid +
				" THEN 0 ELSE 1 END), 0), MAX(CAST(" +
				identifier + " AS TEXT)) FROM " + qualified +
				" WHERE " + identifier + " IS NOT NULL",
		}, nil
	}
	projection := "MAX(" + identifier + ")"
	if sourceEngine == "mysql" {
		projection = "CAST(MAX(" + identifier + ") AS CHAR)"
	}
	return adapterIncrementalQuery{
		SQL: "SELECT " + projection + " FROM " + qualified +
			" WHERE " + identifier + " IS NOT NULL",
	}, nil
}

func buildAdapterIncrementalReadQuery(
	sourceEngine string,
	namespace string,
	table schema.Table,
	columns []string,
	read IncrementalReadPlan,
) (adapterIncrementalQuery, error) {
	mapped, err := buildAdapterIncrementalTable(
		sourceEngine,
		namespace,
		table,
	)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	if !equalIncrementalTables(mapped, read.Table) {
		return adapterIncrementalQuery{}, incrementalSourcePolicy(
			"build read",
			errors.New(
				"incremental read plan does not match the discovered source table",
			),
		)
	}
	if read.PositionalRestoreAllowed {
		return adapterIncrementalQuery{}, incrementalSourcePolicy(
			"build read",
			errors.New("incremental reads cannot restore a positional cursor"),
		)
	}
	if read.Resumed && !read.ReplayFromLowerWatermark {
		return adapterIncrementalQuery{}, incrementalSourcePolicy(
			"build read",
			errors.New(
				"resumed incremental read must replay from the durable lower watermark",
			),
		)
	}
	if err := validateAdapterIncrementalColumns(table, columns); err != nil {
		return adapterIncrementalQuery{}, err
	}
	if err := validateAdapterIncrementalOrdering(mapped, read); err != nil {
		return adapterIncrementalQuery{}, err
	}
	projection, err := adapterIncrementalProjection(
		sourceEngine,
		table,
		columns,
	)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	qualified, err := adapterIncrementalQualified(
		sourceEngine,
		namespace,
		table.Name,
	)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	orderBy, err := adapterIncrementalOrderBy(sourceEngine, read.Ordering)
	if err != nil {
		return adapterIncrementalQuery{}, err
	}
	query := adapterIncrementalQuery{
		SQL: "SELECT " + projection + " FROM " + qualified,
	}
	switch read.Scope {
	case IncrementalReadFullTable:
		if read.Window != nil {
			return adapterIncrementalQuery{}, incrementalSourcePolicy(
				"build read",
				errors.New("full-table incremental read has a window"),
			)
		}
	case IncrementalReadWindow:
		predicate, args, err := adapterIncrementalWindowPredicate(
			sourceEngine,
			table,
			read,
		)
		if err != nil {
			return adapterIncrementalQuery{}, err
		}
		query.SQL += " WHERE " + predicate
		query.Args = args
	default:
		return adapterIncrementalQuery{}, incrementalSourcePolicy(
			"build read",
			fmt.Errorf(
				"incremental read has unknown scope %q",
				read.Scope,
			),
		)
	}
	query.SQL += " ORDER BY " + orderBy
	return query, nil
}

func validateAdapterIncrementalColumns(
	table schema.Table,
	columns []string,
) error {
	if len(columns) == 0 {
		return incrementalSourcePolicy(
			"build read",
			errors.New("incremental read requires at least one column"),
		)
	}
	known := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		known[column.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if _, ok := known[name]; !ok {
			return incrementalSourcePolicy(
				"build read",
				fmt.Errorf(
					"source table %s has no column %q",
					table.Name,
					name,
				),
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return incrementalSourcePolicy(
				"build read",
				fmt.Errorf("incremental projection duplicates column %q", name),
			)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateAdapterIncrementalOrdering(
	table IncrementalTable,
	read IncrementalReadPlan,
) error {
	primaryKey := make([]IncrementalColumn, 0)
	columns := make(map[string]IncrementalColumn, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
		if column.PrimaryKeyPosition > 0 {
			primaryKey = append(primaryKey, column)
		}
	}
	slices.SortFunc(primaryKey, func(left, right IncrementalColumn) int {
		return left.PrimaryKeyPosition - right.PrimaryKeyPosition
	})
	expected := make([]IncrementalOrderTerm, 0, len(primaryKey)+1)
	if read.Scope == IncrementalReadWindow {
		if read.Window == nil {
			return incrementalSourcePolicy(
				"build read",
				errors.New("incremental window read is missing its window"),
			)
		}
		column, ok := columns[read.Window.Column]
		if !ok ||
			(column.TemporalKind != IncrementalTemporalDate &&
				column.TemporalKind != IncrementalTemporalTimestamp) ||
			column.OrderAdmission != IncrementalOrderExact {
			return incrementalSourcePolicy(
				"build read",
				fmt.Errorf(
					"incremental window column %q lacks exact temporal admission",
					read.Window.Column,
				),
			)
		}
		expected = append(expected, IncrementalOrderTerm{
			Column: read.Window.Column,
			Role:   IncrementalOrderUpdateColumn,
			Nulls:  IncrementalNullsExcluded,
		})
	} else if len(read.Ordering) > 0 &&
		read.Ordering[0].Role == IncrementalOrderUpdateColumn {
		column, ok := columns[read.Ordering[0].Column]
		if !ok ||
			(column.TemporalKind != IncrementalTemporalDate &&
				column.TemporalKind != IncrementalTemporalTimestamp) ||
			column.OrderAdmission != IncrementalOrderExact {
			return incrementalSourcePolicy(
				"build read",
				fmt.Errorf(
					"incremental baseline column %q lacks exact temporal admission",
					read.Ordering[0].Column,
				),
			)
		}
		expected = append(expected, IncrementalOrderTerm{
			Column: read.Ordering[0].Column,
			Role:   IncrementalOrderUpdateColumn,
			Nulls:  IncrementalNullsFirst,
		})
	}
	for _, column := range primaryKey {
		expected = append(expected, IncrementalOrderTerm{
			Column: column.Name,
			Role:   IncrementalOrderPrimaryKey,
		})
	}
	if !slices.Equal(expected, read.Ordering) {
		return incrementalSourcePolicy(
			"build read",
			errors.New(
				"incremental read order is not selected timestamp plus complete primary key",
			),
		)
	}
	return nil
}

func adapterIncrementalWindowPredicate(
	sourceEngine string,
	table schema.Table,
	read IncrementalReadPlan,
) (string, []any, error) {
	window := read.Window
	if window == nil {
		return "", nil, incrementalSourcePolicy(
			"build read",
			errors.New("incremental window is required"),
		)
	}
	if !window.LowerExclusive ||
		!window.UpperInclusive ||
		!window.ExcludeNull {
		return "", nil, incrementalSourcePolicy(
			"build read",
			errors.New(
				"incremental window must be strict-lower, inclusive-upper, and NULL-excluding",
			),
		)
	}
	metadata, ok := adapterIncrementalSchemaColumn(table, window.Column)
	if !ok {
		return "", nil, incrementalSourcePolicy(
			"build read",
			fmt.Errorf("incremental window column %q is absent", window.Column),
		)
	}
	temporal, admitted, err := adapterIncrementalTemporalColumn(
		sourceEngine,
		metadata,
	)
	if err != nil || !admitted {
		if err == nil {
			err = errors.New("column is not date or timestamp")
		}
		return "", nil, incrementalSourcePolicy("build read", err)
	}
	identifier := adapterIncrementalIdentifier(sourceEngine, window.Column)
	if window.Empty {
		var lower, upper *time.Time
		if window.Lower != nil {
			canonical, err := validateAdapterIncrementalTemporal(
				temporal,
				*window.Lower,
			)
			if err != nil {
				return "", nil, incrementalSourcePolicy(
					"build read",
					err,
				)
			}
			lower = &canonical
		}
		if window.Upper != nil {
			canonical, err := validateAdapterIncrementalTemporal(
				temporal,
				*window.Upper,
			)
			if err != nil {
				return "", nil, incrementalSourcePolicy(
					"build read",
					err,
				)
			}
			upper = &canonical
		}
		if upper != nil &&
			(lower == nil || !upper.Equal(*lower)) {
			return "", nil, incrementalSourcePolicy(
				"build read",
				errors.New("empty incremental window has contradictory bounds"),
			)
		}
		return "1 = 0", nil, nil
	}
	if window.Upper == nil {
		return "", nil, incrementalSourcePolicy(
			"build read",
			errors.New("non-empty incremental window has no upper fence"),
		)
	}
	upperCanonical, err := validateAdapterIncrementalTemporal(
		temporal,
		*window.Upper,
	)
	if err != nil {
		return "", nil, incrementalSourcePolicy("build read", err)
	}
	if window.Lower != nil {
		lowerCanonical, lowerErr := validateAdapterIncrementalTemporal(
			temporal,
			*window.Lower,
		)
		if lowerErr != nil {
			return "", nil, incrementalSourcePolicy(
				"build read",
				lowerErr,
			)
		}
		if !upperCanonical.After(lowerCanonical) {
			return "", nil, incrementalSourcePolicy(
				"build read",
				errors.New(
					"non-empty incremental window has contradictory bounds",
				),
			)
		}
	}
	args := make([]any, 0, 2)
	terms := []string{identifier + " IS NOT NULL"}
	argument := 1
	if window.Lower != nil {
		value, err := adapterIncrementalBoundValue(
			sourceEngine,
			temporal,
			*window.Lower,
		)
		if err != nil {
			return "", nil, incrementalSourcePolicy("build read", err)
		}
		terms = append(
			terms,
			identifier+" > "+
				adapterIncrementalPlaceholder(sourceEngine, argument),
		)
		args = append(args, value)
		argument++
	}
	upper, err := adapterIncrementalBoundValue(
		sourceEngine,
		temporal,
		*window.Upper,
	)
	if err != nil {
		return "", nil, incrementalSourcePolicy("build read", err)
	}
	terms = append(
		terms,
		identifier+" <= "+
			adapterIncrementalPlaceholder(sourceEngine, argument),
	)
	args = append(args, upper)
	return strings.Join(terms, " AND "), args, nil
}

func adapterIncrementalProjection(
	sourceEngine string,
	table schema.Table,
	columns []string,
) (string, error) {
	switch sourceEngine {
	case "postgres":
		return quotedColumns(columns), nil
	case "mysql":
		return mySQLReadProjection(table, columns), nil
	case "mssql":
		return sqlServerQuotedColumns(columns), nil
	case "sqlite":
		return sqliteSourceProjection(table, columns)
	default:
		return "", fmt.Errorf(
			"source engine %q has no incremental projection",
			sourceEngine,
		)
	}
}

func adapterIncrementalOrderBy(
	sourceEngine string,
	ordering []IncrementalOrderTerm,
) (string, error) {
	if len(ordering) == 0 {
		return "", incrementalSourcePolicy(
			"build read",
			errors.New("incremental read order is empty"),
		)
	}
	terms := make([]string, 0, len(ordering)+1)
	for _, term := range ordering {
		identifier := adapterIncrementalIdentifier(sourceEngine, term.Column)
		switch term.Role {
		case IncrementalOrderUpdateColumn:
			switch term.Nulls {
			case IncrementalNullsFirst:
				if sourceEngine == "postgres" {
					terms = append(terms, identifier+" ASC NULLS FIRST")
				} else {
					terms = append(
						terms,
						"CASE WHEN "+identifier+
							" IS NULL THEN 0 ELSE 1 END ASC",
						identifier+" ASC",
					)
				}
			case IncrementalNullsExcluded:
				terms = append(terms, identifier+" ASC")
			default:
				return "", incrementalSourcePolicy(
					"build read",
					fmt.Errorf(
						"update column %s has unknown NULL order %q",
						term.Column,
						term.Nulls,
					),
				)
			}
		case IncrementalOrderPrimaryKey:
			if term.Nulls != "" {
				return "", incrementalSourcePolicy(
					"build read",
					fmt.Errorf(
						"primary-key column %s has a NULL order",
						term.Column,
					),
				)
			}
			terms = append(terms, identifier+" ASC")
		default:
			return "", incrementalSourcePolicy(
				"build read",
				fmt.Errorf(
					"column %s has unknown incremental order role %q",
					term.Column,
					term.Role,
				),
			)
		}
	}
	return strings.Join(terms, ", "), nil
}

func adapterIncrementalIdentifier(sourceEngine, value string) string {
	switch sourceEngine {
	case "mysql":
		return mySQLIdentifier(value)
	case "mssql":
		return sqlServerIdentifier(value)
	case "postgres":
		return postgresIdentifier(value)
	default:
		return quote(value)
	}
}

func adapterIncrementalQualified(
	sourceEngine string,
	namespace string,
	name string,
) (string, error) {
	switch sourceEngine {
	case "postgres":
		return postgresQualified(namespace, name), nil
	case "mysql":
		return mySQLQualified(namespace, name), nil
	case "mssql":
		return sqlServerQualified(namespace, name), nil
	case "sqlite":
		return quote(name), nil
	default:
		return "", fmt.Errorf(
			"source engine %q has no incremental table quoting",
			sourceEngine,
		)
	}
}

func adapterIncrementalPlaceholder(sourceEngine string, position int) string {
	switch sourceEngine {
	case "postgres":
		return "$" + strconv.Itoa(position)
	case "mssql":
		return "@p" + strconv.Itoa(position)
	default:
		return "?"
	}
}

func adapterIncrementalBoundValue(
	sourceEngine string,
	temporal adapterIncrementalTemporal,
	value time.Time,
) (any, error) {
	canonical, err := validateAdapterIncrementalTemporal(temporal, value)
	if err != nil {
		return nil, err
	}
	switch sourceEngine {
	case "mysql", "sqlite":
		return formatAdapterIncrementalTemporal(temporal, canonical), nil
	default:
		return canonical, nil
	}
}

func validateAdapterIncrementalTemporal(
	temporal adapterIncrementalTemporal,
	value time.Time,
) (time.Time, error) {
	canonical := value.UTC()
	if !temporal.zoneAware {
		canonical = time.Date(
			value.Year(),
			value.Month(),
			value.Day(),
			value.Hour(),
			value.Minute(),
			value.Second(),
			value.Nanosecond(),
			time.UTC,
		)
	}
	if temporal.kind == IncrementalTemporalDate {
		if canonical.Hour() != 0 ||
			canonical.Minute() != 0 ||
			canonical.Second() != 0 ||
			canonical.Nanosecond() != 0 {
			return time.Time{}, errors.New(
				"incremental date fence is not UTC midnight",
			)
		}
		return canonical, nil
	}
	if temporal.base == "smalldatetime" &&
		(canonical.Second() != 0 || canonical.Nanosecond() != 0) {
		return time.Time{}, errors.New(
			"incremental smalldatetime fence is not minute-exact",
		)
	}
	if !validAdapterIncrementalTemporalPrecision(
		canonical.Nanosecond(),
		temporal.precision,
	) {
		return time.Time{}, fmt.Errorf(
			"incremental timestamp is not exact at precision %d",
			temporal.precision,
		)
	}
	return canonical, nil
}

func validAdapterIncrementalTemporalPrecision(
	nanosecond int,
	precision int,
) bool {
	if nanosecond < 0 ||
		nanosecond >= int(time.Second) ||
		precision < 0 ||
		precision > 9 {
		return false
	}
	unit := 1
	for digits := precision; digits < 9; digits++ {
		unit *= 10
	}
	return nanosecond%unit == 0
}

func formatAdapterIncrementalTemporal(
	temporal adapterIncrementalTemporal,
	value time.Time,
) string {
	if temporal.kind == IncrementalTemporalDate {
		return value.Format("2006-01-02")
	}
	result := value.Format("2006-01-02 15:04:05")
	if temporal.precision == 0 {
		return result
	}
	unit := 1
	for digits := temporal.precision; digits < 9; digits++ {
		unit *= 10
	}
	return result + fmt.Sprintf(
		".%0*d",
		temporal.precision,
		value.Nanosecond()/unit,
	)
}

func sqliteIncrementalTemporalValidity(
	identifier string,
	temporal adapterIncrementalTemporal,
) string {
	if temporal.kind == IncrementalTemporalDate {
		return "typeof(" + identifier + ") = 'text'" +
			" AND length(" + identifier + ") = 10" +
			" AND " + identifier + " GLOB '" +
			"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'" +
			" AND CAST(substr(" + identifier +
			", 1, 4) AS INTEGER) BETWEEN 1 AND 9999" +
			" AND CAST(substr(" + identifier +
			", 6, 2) AS INTEGER) BETWEEN 1 AND 12" +
			" AND CAST(substr(" + identifier +
			", 9, 2) AS INTEGER) BETWEEN 1 AND 31" +
			" AND date(" + identifier + ") = " + identifier
	}
	length := 19
	fraction := ""
	if temporal.precision > 0 {
		length += 1 + temporal.precision
		fraction = " AND substr(" + identifier + ", 20, 1) = '.'" +
			" AND substr(" + identifier + ", 21) GLOB '" +
			strings.Repeat("[0-9]", temporal.precision) + "'"
	}
	return "typeof(" + identifier + ") = 'text'" +
		" AND length(" + identifier + ") = " + strconv.Itoa(length) +
		" AND substr(" + identifier + ", 1, 19) GLOB '" +
		"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " +
		"[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'" +
		" AND CAST(substr(" + identifier +
		", 1, 4) AS INTEGER) BETWEEN 1 AND 9999" +
		" AND CAST(substr(" + identifier +
		", 6, 2) AS INTEGER) BETWEEN 1 AND 12" +
		" AND CAST(substr(" + identifier +
		", 9, 2) AS INTEGER) BETWEEN 1 AND 31" +
		" AND CAST(substr(" + identifier +
		", 12, 2) AS INTEGER) BETWEEN 0 AND 23" +
		" AND CAST(substr(" + identifier +
		", 15, 2) AS INTEGER) BETWEEN 0 AND 59" +
		" AND CAST(substr(" + identifier +
		", 18, 2) AS INTEGER) BETWEEN 0 AND 59" +
		" AND datetime(substr(" + identifier + ", 1, 19))" +
		" = substr(" + identifier + ", 1, 19)" +
		fraction
}

func (adapter *relationalSourceAdapter) SampleIncrementalUpperFence(
	ctx context.Context,
	table schema.Table,
	column IncrementalColumn,
) (*time.Time, error) {
	mapped, err := adapter.IncrementalTable(table)
	if err != nil {
		return nil, err
	}
	if err := adapter.requireIncrementalMySQLFlavor(ctx); err != nil {
		return nil, err
	}
	if adapter.spec.engine == "mysql" {
		if err := preflightMySQLSourceRows(
			ctx,
			adapter.database,
			adapter.namespace,
			[]schema.Table{table},
		); err != nil {
			return nil, err
		}
	}
	if err := validateAdapterIncrementalFenceColumn(mapped, column); err != nil {
		return nil, err
	}
	metadata, ok := adapterIncrementalSchemaColumn(table, column.Name)
	if !ok {
		return nil, incrementalSourcePolicy(
			"sample upper fence",
			fmt.Errorf("source table %s has no column %q", table.Name, column.Name),
		)
	}
	query, err := buildAdapterIncrementalFenceQuery(
		adapter.spec.engine,
		adapter.namespace,
		table,
		metadata,
	)
	if err != nil {
		return nil, incrementalSourcePolicy("sample upper fence", err)
	}
	var raw any
	if err := adapter.database.QueryRowContext(
		ctx,
		query.SQL,
		query.Args...,
	).Scan(&raw); err != nil {
		return nil, fmt.Errorf(
			"sample %s incremental upper fence for %s: %w",
			adapter.spec.displayName,
			table.Name,
			err,
		)
	}
	return normalizeAdapterIncrementalFence(
		adapter.spec.engine,
		metadata,
		raw,
	)
}

func (adapter *sqliteSourceAdapter) SampleIncrementalUpperFence(
	ctx context.Context,
	table schema.Table,
	column IncrementalColumn,
) (*time.Time, error) {
	mapped, err := adapter.IncrementalTable(table)
	if err != nil {
		return nil, err
	}
	if err := validateAdapterIncrementalFenceColumn(mapped, column); err != nil {
		return nil, err
	}
	metadata, ok := adapterIncrementalSchemaColumn(table, column.Name)
	if !ok {
		return nil, incrementalSourcePolicy(
			"sample upper fence",
			fmt.Errorf("source table %s has no column %q", table.Name, column.Name),
		)
	}
	query, err := buildAdapterIncrementalFenceQuery(
		"sqlite",
		"",
		table,
		metadata,
	)
	if err != nil {
		return nil, incrementalSourcePolicy("sample upper fence", err)
	}
	var invalid int
	var raw any
	if err := adapter.snapshot.QueryRowContext(ctx, query.SQL).Scan(
		&invalid,
		&raw,
	); err != nil {
		return nil, fmt.Errorf(
			"sample SQLite incremental upper fence for %s: %w",
			table.Name,
			err,
		)
	}
	if invalid != 0 {
		return nil, incrementalSourcePolicy(
			"sample upper fence",
			fmt.Errorf(
				"SQLite source column %s contains a value outside its exact declared temporal shape",
				column.Name,
			),
		)
	}
	return normalizeAdapterIncrementalFence("sqlite", metadata, raw)
}

func normalizeAdapterIncrementalFence(
	sourceEngine string,
	column schema.Column,
	raw any,
) (*time.Time, error) {
	if raw == nil {
		return nil, nil
	}
	temporal, admitted, err := adapterIncrementalTemporalColumn(
		sourceEngine,
		column,
	)
	if err != nil || !admitted {
		if err == nil {
			err = errors.New("column is not date or timestamp")
		}
		return nil, incrementalSourcePolicy("normalize upper fence", err)
	}
	var value time.Time
	switch sourceEngine {
	case "postgres":
		var ok bool
		value, ok = raw.(time.Time)
		if !ok {
			return nil, incrementalSourcePolicy(
				"normalize upper fence",
				errors.New("PostgreSQL driver returned an unexpected temporal shape"),
			)
		}
	case "mssql":
		var metadata sqlServerSourceValueColumn
		if temporal.kind == IncrementalTemporalDate {
			metadata = sqlServerDateSourceColumn(0, column)
		} else {
			metadata = sqlServerDateTimeSourceColumn(0, column)
		}
		normalized, normalizeErr := normalizeSQLServerSourceValue(metadata, raw)
		if normalizeErr != nil {
			return nil, incrementalSourcePolicy(
				"normalize upper fence",
				normalizeErr,
			)
		}
		var ok bool
		value, ok = normalized.(time.Time)
		if !ok {
			return nil, incrementalSourcePolicy(
				"normalize upper fence",
				errors.New("SQL Server driver returned an unexpected temporal shape"),
			)
		}
	case "mysql", "sqlite":
		text, ok := adapterIncrementalText(raw)
		if !ok {
			return nil, incrementalSourcePolicy(
				"normalize upper fence",
				fmt.Errorf(
					"%s driver returned an unexpected temporal shape",
					sourceEngine,
				),
			)
		}
		value, err = parseAdapterIncrementalTemporal(temporal, text)
		if err != nil {
			return nil, incrementalSourcePolicy("normalize upper fence", err)
		}
	default:
		return nil, incrementalSourcePolicy(
			"normalize upper fence",
			fmt.Errorf("source engine %q is unsupported", sourceEngine),
		)
	}
	canonical, err := validateAdapterIncrementalTemporal(temporal, value)
	if err != nil {
		return nil, incrementalSourcePolicy("normalize upper fence", err)
	}
	return &canonical, nil
}

func adapterIncrementalText(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func parseAdapterIncrementalTemporal(
	temporal adapterIncrementalTemporal,
	value string,
) (time.Time, error) {
	layout := "2006-01-02"
	if temporal.kind == IncrementalTemporalTimestamp {
		layout = "2006-01-02 15:04:05"
		if temporal.precision > 0 {
			layout += "." + strings.Repeat("0", temporal.precision)
		}
	}
	if len(value) != len(layout) {
		return time.Time{}, fmt.Errorf(
			"source temporal value does not match declared precision %d",
			temporal.precision,
		)
	}
	parsed, err := time.ParseInLocation(layout, value, time.UTC)
	if err != nil || parsed.Year() < 1 {
		return time.Time{}, errors.New(
			"source temporal value is outside its exact declared shape",
		)
	}
	return parsed, nil
}

func (adapter *relationalSourceAdapter) OpenIncrementalRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
	read IncrementalReadPlan,
) (adapterRows, error) {
	if err := adapter.requireIncrementalMySQLFlavor(ctx); err != nil {
		return nil, err
	}
	query, err := buildAdapterIncrementalReadQuery(
		adapter.spec.engine,
		adapter.namespace,
		table,
		columns,
		read,
	)
	if err != nil {
		return nil, err
	}
	rows, err := adapter.database.QueryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return nil, fmt.Errorf(
			"read %s incremental table %s: %w",
			adapter.spec.displayName,
			table.Name,
			err,
		)
	}
	var result adapterRows = rows
	if adapter.spec.wrapRows != nil {
		result = adapter.spec.wrapRows(result, table, columns)
	}
	return result, nil
}

func (adapter *sqliteSourceAdapter) OpenIncrementalRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
	read IncrementalReadPlan,
) (adapterRows, error) {
	query, err := buildAdapterIncrementalReadQuery(
		"sqlite",
		"",
		table,
		columns,
		read,
	)
	if err != nil {
		return nil, err
	}
	rows, err := adapter.snapshot.QueryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return nil, fmt.Errorf(
			"read SQLite incremental table %s: %w",
			table.Name,
			err,
		)
	}
	return rows, nil
}

func (adapter *relationalSourceAdapter) requireIncrementalMySQLFlavor(
	ctx context.Context,
) error {
	if adapter.spec.engine != "mysql" {
		return nil
	}
	flavor, err := engine.DetectMySQLServerFlavor(ctx, adapter.database)
	if err != nil {
		return incrementalSourcePolicy(
			"verify MySQL-family flavor",
			err,
		)
	}
	switch flavor {
	case engine.MySQLServerFlavorOracle80,
		engine.MySQLServerFlavorMariaDB1011:
		return nil
	default:
		return incrementalSourcePolicy(
			"verify MySQL-family flavor",
			errors.New("unsupported MySQL-family source flavor"),
		)
	}
}

func adapterIncrementalSchemaColumn(
	table schema.Table,
	name string,
) (schema.Column, bool) {
	for _, column := range table.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return schema.Column{}, false
}

func validateAdapterIncrementalFenceColumn(
	table IncrementalTable,
	column IncrementalColumn,
) error {
	for _, mapped := range table.Columns {
		if mapped.Name != column.Name {
			continue
		}
		if mapped != column ||
			(mapped.TemporalKind != IncrementalTemporalDate &&
				mapped.TemporalKind != IncrementalTemporalTimestamp) ||
			mapped.OrderAdmission != IncrementalOrderExact {
			return incrementalSourcePolicy(
				"sample upper fence",
				fmt.Errorf(
					"column %q does not match its exact admitted temporal catalog",
					column.Name,
				),
			)
		}
		return nil
	}
	return incrementalSourcePolicy(
		"sample upper fence",
		fmt.Errorf(
			"column %q is absent from the admitted incremental table",
			column.Name,
		),
	)
}

func equalIncrementalTables(left, right IncrementalTable) bool {
	return left.Schema == right.Schema &&
		left.Name == right.Name &&
		slices.Equal(left.Columns, right.Columns)
}

func incrementalSourcePolicy(operation string, err error) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf("incremental source %s: %w", operation, err),
	)
}
