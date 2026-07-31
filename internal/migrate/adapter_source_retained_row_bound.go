package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	"github.com/johndauphine/dmtx/internal/schema"
)

// adapterSourceRetainedRowBounder proves a finite retained-memory upper bound
// for the exact source columns selected by a network range. Implementations
// may inspect source values, but must remain read-only.
type adapterSourceRetainedRowBounder interface {
	PlanRetainedRowWidth(
		context.Context,
		schema.Table,
		[]string,
	) (RuntimeRowWidthEvidence, error)
}

type adapterRetainedLengthQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// adapterRetainedStableViewQueryer is deliberately narrower than *sql.DB.
// Dynamic length evidence is trustworthy only while the same verified
// snapshot/lock view remains alive for every corresponding source page read.
// Callers must not copy evidence out of that view's execution lifetime.
type adapterRetainedStableViewQueryer interface {
	adapterRetainedLengthQueryer
	retainedStableViewEngine() string
}

// postgresAdapterRetainedStableView is created by Stage 4 orchestration inside
// PostgresStrictConsistencySession.RunReader. The imported transaction is
// rolled back as soon as that callback returns, which bounds this capability's
// useful lifetime even if a caller incorrectly retains the wrapper.
type postgresAdapterRetainedStableView struct {
	queryer PostgresStrictSnapshotQueryer
}

type adapterRetainedStableRelationalView struct {
	source *relationalSourceAdapter
	view   adapterRetainedStableViewQueryer
}

func newAdapterRetainedStableRelationalView(
	source sourceAdapter,
	view adapterRetainedStableViewQueryer,
) (*adapterRetainedStableRelationalView, error) {
	adapter, ok := source.(*relationalSourceAdapter)
	if !ok || adapter == nil || adapter.database == nil {
		return nil, errors.New(
			"stable retained-row view requires an open relational source adapter",
		)
	}
	if isNilInterface(view) ||
		view.retainedStableViewEngine() != adapter.spec.engine {
		return nil, fmt.Errorf(
			"retained stable view does not match source engine %q",
			adapter.spec.engine,
		)
	}
	return &adapterRetainedStableRelationalView{
		source: adapter,
		view:   view,
	}, nil
}

func newPostgresAdapterRetainedStableRelationalView(
	source sourceAdapter,
	queryer PostgresStrictSnapshotQueryer,
) (*adapterRetainedStableRelationalView, error) {
	if isNilInterface(queryer) {
		return nil, errors.New(
			"PostgreSQL retained-row stable view is required",
		)
	}
	return newAdapterRetainedStableRelationalView(
		source,
		&postgresAdapterRetainedStableView{queryer: queryer},
	)
}

func (view *postgresAdapterRetainedStableView) QueryContext(
	ctx context.Context,
	query string,
	arguments ...any,
) (*sql.Rows, error) {
	if view == nil || isNilInterface(view.queryer) {
		return nil, errors.New(
			"PostgreSQL retained-row stable view is unavailable",
		)
	}
	return view.queryer.QueryContext(ctx, query, arguments...)
}

func (*postgresAdapterRetainedStableView) retainedStableViewEngine() string {
	return "postgres"
}

// OpenRows intentionally has no fallback to source.database. The view object
// and every width proof it creates are valid only while the owning strict
// session keeps this exact queryer alive.
func (view *adapterRetainedStableRelationalView) OpenRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	if view == nil || view.source == nil || isNilInterface(view.view) {
		return nil, errors.New(
			"stable retained-row source view is unavailable",
		)
	}
	rows, err := view.view.QueryContext(
		ctx,
		view.source.spec.readQuery(
			view.source.namespace,
			table,
			columns,
		),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read %s stable source table %s: %w",
			view.source.spec.displayName,
			table.Name,
			err,
		)
	}
	var result adapterRows = rows
	if view.source.spec.wrapRows != nil {
		result = view.source.spec.wrapRows(result, table, columns)
	}
	return result, nil
}

type adapterRetainedColumnBound struct {
	name           string
	fixedBytes     int64
	liveExpression string
	liveOverhead   int64
	liveMultiplier int64
}

type adapterRetainedRowBoundPlan struct {
	engine    string
	table     string
	baseBytes int64
	columns   []adapterRetainedColumnBound
	query     string
	aliases   []string
}

// planAdapterSourceRetainedRowWidth is the common admission entry point. Its
// result can be copied directly into NetworkRangePlan.MaxRowBytes and the
// migration-wide RuntimeRowWidthEvidence.
func planAdapterSourceRetainedRowWidth(
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	if ctx == nil {
		return RuntimeRowWidthEvidence{}, errors.New(
			"retained source row context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	if isNilInterface(source) {
		return RuntimeRowWidthEvidence{}, errors.New(
			"retained source row adapter is required",
		)
	}
	bounder, ok := source.(adapterSourceRetainedRowBounder)
	if !ok || isNilInterface(bounder) {
		return RuntimeRowWidthEvidence{}, fmt.Errorf(
			"source engine %q does not prove a retained row upper bound",
			source.Engine(),
		)
	}
	evidence, err := bounder.PlanRetainedRowWidth(ctx, table, columns)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	if !evidence.Trustworthy ||
		evidence.CompleteColumnCount != len(columns) ||
		evidence.ExpectedColumnCount != len(columns) ||
		evidence.UpperBoundBytes <= 0 {
		return RuntimeRowWidthEvidence{}, errors.New(
			"source returned incomplete retained row width evidence",
		)
	}
	return evidence, nil
}

func (adapter *relationalSourceAdapter) PlanRetainedRowWidth(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	plan, columnCount, err := adapter.planRetainedRowBound(
		table,
		columns,
	)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	if plan.query != "" {
		return RuntimeRowWidthEvidence{}, fmt.Errorf(
			"%s retained row planning requires an active stable source view for dynamic column lengths",
			adapter.spec.displayName,
		)
	}
	upper, err := executeAdapterRetainedRowBound(
		ctx,
		adapter.database,
		plan,
	)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	return RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: columnCount,
		ExpectedColumnCount: columnCount,
		UpperBoundBytes:     upper,
	}, nil
}

// PlanRetainedRowWidth may be called only inside this view's execution
// lifetime. All page reads admitted by its result must use OpenRows (or the
// range-page method implemented on this same object), never source.database.
// Mutable relational adapters reject every dynamic term.
func (view *adapterRetainedStableRelationalView) PlanRetainedRowWidth(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	if ctx == nil {
		return RuntimeRowWidthEvidence{}, errors.New(
			"retained source row context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	if view == nil || view.source == nil || isNilInterface(view.view) {
		return RuntimeRowWidthEvidence{}, errors.New(
			"retained source adapter and active stable view are required",
		)
	}
	if view.view.retainedStableViewEngine() != view.source.spec.engine {
		return RuntimeRowWidthEvidence{}, fmt.Errorf(
			"retained stable view engine %q differs from source engine %q",
			view.view.retainedStableViewEngine(),
			view.source.spec.engine,
		)
	}
	plan, columnCount, err := view.source.planRetainedRowBound(
		table,
		columns,
	)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	upper, err := executeAdapterRetainedRowBound(ctx, view.view, plan)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	return RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: columnCount,
		ExpectedColumnCount: columnCount,
		UpperBoundBytes:     upper,
	}, nil
}

func (adapter *relationalSourceAdapter) planRetainedRowBound(
	table schema.Table,
	columns []string,
) (adapterRetainedRowBoundPlan, int, error) {
	if adapter == nil || adapter.database == nil {
		return adapterRetainedRowBoundPlan{}, 0, errors.New(
			"relational source is unavailable for retained row planning",
		)
	}
	ordered, err := exactAdapterRetainedColumns(
		adapter.spec.engine,
		table,
		columns,
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, 0, err
	}
	var plan adapterRetainedRowBoundPlan
	switch adapter.spec.engine {
	case "postgres":
		plan, err = planPostgresRetainedRowBound(
			adapter.namespace,
			table,
			ordered,
		)
	case "mysql":
		plan, err = planMySQLRetainedRowBound(
			adapter.namespace,
			table,
			ordered,
		)
	case "mssql":
		plan, err = planSQLServerRetainedRowBound(
			adapter.namespace,
			table,
			ordered,
		)
	default:
		err = fmt.Errorf(
			"source engine %q does not prove retained row widths",
			adapter.spec.engine,
		)
	}
	if err != nil {
		return adapterRetainedRowBoundPlan{}, 0, err
	}
	return plan, len(ordered), nil
}

func (adapter *sqliteSourceAdapter) PlanRetainedRowWidth(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	if adapter == nil || adapter.snapshot == nil {
		return RuntimeRowWidthEvidence{}, errors.New(
			"SQLite source snapshot is unavailable for retained row planning",
		)
	}
	ordered, err := exactAdapterRetainedColumns(
		"sqlite",
		table,
		columns,
	)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	plan, err := planSQLiteRetainedRowBound(table, ordered)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	upper, err := executeAdapterRetainedRowBound(
		ctx,
		adapter.snapshot,
		plan,
	)
	if err != nil {
		return RuntimeRowWidthEvidence{}, err
	}
	return RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: len(ordered),
		ExpectedColumnCount: len(columns),
		UpperBoundBytes:     upper,
	}, nil
}

func exactAdapterRetainedColumns(
	sourceEngine string,
	table schema.Table,
	selected []string,
) ([]schema.Column, error) {
	if table.Schema != "" {
		if sourceEngine == "sqlite" {
			return nil, errors.New(
				"SQLite retained row planning does not accept a source schema",
			)
		}
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"schema",
			table.Schema,
		); err != nil {
			return nil, err
		}
	}
	if err := validateAdapterRetainedIdentifier(
		sourceEngine,
		"table",
		table.Name,
	); err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf(
			"retained row planning for table %s requires selected columns",
			table.Name,
		)
	}
	metadata := make(map[string]schema.Column, len(table.Columns))
	for index, column := range table.Columns {
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"column",
			column.Name,
		); err != nil {
			return nil, fmt.Errorf(
				"retained row planning for table %s has an invalid catalog column at index %d: %w",
				table.Name,
				index,
				err,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"retained row planning for table %s has duplicate catalog column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = column
	}
	ordered := make([]schema.Column, len(selected))
	seen := make(map[string]struct{}, len(selected))
	for index, name := range selected {
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"selected column",
			name,
		); err != nil {
			return nil, err
		}
		column, exists := metadata[name]
		if !exists {
			return nil, fmt.Errorf(
				"retained row planning for table %s selected unknown column %s",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"retained row planning for table %s selected duplicate column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
		ordered[index] = column
	}
	return ordered, nil
}

func newAdapterRetainedRowBoundPlan(
	engine string,
	table string,
	columnCount int,
) (adapterRetainedRowBoundPlan, error) {
	if columnCount <= 0 {
		return adapterRetainedRowBoundPlan{}, errors.New(
			"retained row bound requires selected columns",
		)
	}
	sliceBytes := int64(unsafe.Sizeof([]any{}))
	slotBytes := int64(unsafe.Sizeof(any(nil)))
	slots, err := multiplyAdapterRetainedBytes(
		int64(columnCount),
		slotBytes,
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	base, err := addAdapterRetainedBytes(sliceBytes, slots)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	return adapterRetainedRowBoundPlan{
		engine:    engine,
		table:     table,
		baseBytes: base,
		columns:   make([]adapterRetainedColumnBound, 0, columnCount),
	}, nil
}

func planPostgresRetainedRowBound(
	namespace string,
	table schema.Table,
	columns []schema.Column,
) (adapterRetainedRowBoundPlan, error) {
	if err := validateAdapterRetainedPlanIdentifiers(
		"postgres",
		namespace,
		table,
		columns,
	); err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	plan, err := newAdapterRetainedRowBoundPlan(
		"postgres",
		postgresQualified(namespace, table.Name),
		len(columns),
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	for _, column := range columns {
		bound, err := postgresRetainedColumnBound(column)
		if err != nil {
			return adapterRetainedRowBoundPlan{}, fmt.Errorf(
				"plan PostgreSQL retained column %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		plan.columns = append(plan.columns, bound)
	}
	return finalizeAdapterRetainedLengthQuery(
		plan,
		postgresIdentifier,
	), nil
}

func postgresRetainedColumnBound(
	column schema.Column,
) (adapterRetainedColumnBound, error) {
	bound := adapterRetainedColumnBound{name: column.Name}
	switch column.Type {
	case "integer", "bigint":
		if column.DeclaredType != nil {
			return bound, errors.New("integer declaration must be implicit")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(int64(0)))
	case "real":
		if !exactAdapterDeclaredType(column.DeclaredType, "real") {
			return bound, errors.New("real declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(float64(0)))
	case "double precision":
		if column.DeclaredType != nil {
			return bound, errors.New(
				"double precision declaration must be implicit",
			)
		}
		bound.fixedBytes = int64(unsafe.Sizeof(float64(0)))
	case "boolean":
		if column.DeclaredType != nil {
			return bound, errors.New("boolean declaration must be implicit")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(false))
	case "char", "varchar":
		length, ok := exactAdapterDeclaredLength(
			column.DeclaredType,
			column.Type,
		)
		if !ok || length > 10_485_760 {
			return bound, errors.New("character declaration is invalid")
		}
		payload, err := multiplyAdapterRetainedBytes(length, 4)
		if err != nil {
			return bound, err
		}
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof("")),
			payload,
		)
		if err != nil {
			return bound, err
		}
	case "numeric":
		precision, scale, ok := exactAdapterDecimal(column.DeclaredType, "numeric")
		if !ok || precision > 1000 ||
			scale < -1000 || scale > 1000 {
			return bound, errors.New("numeric declaration is invalid")
		}
		payload, err := adapterDecimalTextBytes(precision, scale)
		if err != nil {
			return bound, err
		}
		if payload < int64(len("-Infinity")) {
			payload = int64(len("-Infinity"))
		}
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			payload,
		)
		if err != nil {
			return bound, err
		}
	case "uuid":
		if column.DeclaredType != nil {
			return bound, errors.New("UUID declaration must be implicit")
		}
		bound.fixedBytes = int64(unsafe.Sizeof("")) + 36
	case "date", "timestamp", "timestamptz":
		if column.Type == "date" {
			if column.DeclaredType != nil {
				return bound, errors.New("date declaration must be implicit")
			}
		} else if !validAdapterOptionalPrecision(
			column.DeclaredType,
			column.Type,
		) {
			return bound, errors.New("timestamp declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(time.Time{}))
		infinityBytes := int64(unsafe.Sizeof("")) +
			int64(len("-infinity"))
		if infinityBytes > bound.fixedBytes {
			bound.fixedBytes = infinityBytes
		}
	case "time":
		if !validAdapterOptionalPrecision(column.DeclaredType, "time") {
			return bound, errors.New("time declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof("")) + 15
	case "text":
		if column.DeclaredType != nil {
			return bound, errors.New("text declaration must be implicit")
		}
		bound.liveExpression = "octet_length(" +
			postgresIdentifier(column.Name) + ")::bigint"
		bound.liveOverhead = int64(unsafe.Sizeof(""))
		bound.liveMultiplier = 1
	case "bytea":
		if column.DeclaredType != nil {
			return bound, errors.New("bytea declaration must be implicit")
		}
		bound.liveExpression = "octet_length(" +
			postgresIdentifier(column.Name) + ")::bigint"
		bound.liveOverhead = int64(unsafe.Sizeof([]byte(nil)))
		bound.liveMultiplier = 1
	case "json", "jsonb":
		if column.DeclaredType != nil {
			return bound, errors.New("JSON declaration must be implicit")
		}
		bound.liveExpression = "octet_length((" +
			postgresIdentifier(column.Name) + ")::text)::bigint"
		bound.liveOverhead = int64(unsafe.Sizeof([]byte(nil)))
		bound.liveMultiplier = 1
	default:
		return bound, fmt.Errorf(
			"unsupported PostgreSQL retained type %q",
			column.Type,
		)
	}
	return bound, nil
}

func planMySQLRetainedRowBound(
	namespace string,
	table schema.Table,
	columns []schema.Column,
) (adapterRetainedRowBoundPlan, error) {
	if err := validateAdapterRetainedPlanIdentifiers(
		"mysql",
		namespace,
		table,
		columns,
	); err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	plan, err := newAdapterRetainedRowBoundPlan(
		"mysql",
		mySQLQualified(namespace, table.Name),
		len(columns),
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	for _, column := range columns {
		bound, err := mySQLRetainedColumnBound(column)
		if err != nil {
			return adapterRetainedRowBoundPlan{}, fmt.Errorf(
				"plan MySQL-family retained column %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		plan.columns = append(plan.columns, bound)
	}
	return finalizeAdapterRetainedLengthQuery(
		plan,
		mySQLIdentifier,
	), nil
}

func mySQLRetainedColumnBound(
	column schema.Column,
) (adapterRetainedColumnBound, error) {
	bound := adapterRetainedColumnBound{name: column.Name}
	switch column.Type {
	case "integer":
		if !validMySQLRetainedIntegerDeclaration(column.DeclaredType) {
			return bound, errors.New("integer declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(int64(0)))
	case "bigint":
		if !exactAdapterDeclaredType(column.DeclaredType, "bigint") {
			return bound, errors.New("bigint declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(int64(0)))
	case "numeric":
		precision, scale, ok := exactAdapterDecimal(
			column.DeclaredType,
			"decimal",
		)
		if !ok || precision > 65 ||
			scale < 0 || scale > 30 || scale > precision {
			return bound, errors.New("decimal declaration is invalid")
		}
		payload, err := adapterDecimalTextBytes(precision, scale)
		if err != nil {
			return bound, err
		}
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			payload,
		)
		if err != nil {
			return bound, err
		}
	case "double precision":
		if !exactAdapterDeclaredType(column.DeclaredType, "double") {
			return bound, errors.New("double declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(float64(0)))
	case "char", "varchar":
		length, ok := exactAdapterDeclaredLength(
			column.DeclaredType,
			column.Type,
		)
		if !ok || length > 16_383 {
			return bound, errors.New("character declaration is invalid")
		}
		payload, err := multiplyAdapterRetainedBytes(length, 4)
		if err != nil {
			return bound, err
		}
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			payload,
		)
		if err != nil {
			return bound, err
		}
	case "binary", "varbinary":
		length, ok := exactAdapterDeclaredLength(
			column.DeclaredType,
			column.Type,
		)
		if !ok || length > 65_535 {
			return bound, errors.New("binary declaration is invalid")
		}
		var err error
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			length,
		)
		if err != nil {
			return bound, err
		}
	case "text":
		if !exactAdapterDeclaredTypeIn(
			column.DeclaredType,
			"tinytext",
			"text",
			"mediumtext",
			"longtext",
		) {
			return bound, errors.New("text declaration is invalid")
		}
		bound = mySQLLiveRetainedColumnBound(column.Name)
	case "blob":
		if !exactAdapterDeclaredTypeIn(
			column.DeclaredType,
			"tinyblob",
			"blob",
			"mediumblob",
			"longblob",
		) {
			return bound, errors.New("blob declaration is invalid")
		}
		bound = mySQLLiveRetainedColumnBound(column.Name)
	case "json":
		if !exactAdapterDeclaredType(column.DeclaredType, "json") {
			return bound, errors.New("JSON declaration is invalid")
		}
		bound = mySQLLiveRetainedColumnBound(column.Name)
	case "date":
		if !exactAdapterDeclaredType(column.DeclaredType, "date") {
			return bound, errors.New("date declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof([]byte(nil))) +
			int64(len("2006-01-02"))
	case "time":
		if !validAdapterRequiredPrecision(column.DeclaredType, "time") {
			return bound, errors.New("time declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof([]byte(nil))) + 15
	case "datetime", "timestamp":
		if !validAdapterRequiredPrecision(
			column.DeclaredType,
			column.Type,
		) {
			return bound, errors.New("datetime declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof([]byte(nil))) +
			int64(len("2006-01-02 15:04:05.000000"))
	default:
		return bound, fmt.Errorf(
			"unsupported MySQL-family retained type %q",
			column.Type,
		)
	}
	return bound, nil
}

func mySQLLiveRetainedColumnBound(
	name string,
) adapterRetainedColumnBound {
	return adapterRetainedColumnBound{
		name: name,
		liveExpression: "CAST(OCTET_LENGTH(" +
			mySQLIdentifier(name) + ") AS SIGNED)",
		liveOverhead:   int64(unsafe.Sizeof([]byte(nil))),
		liveMultiplier: 1,
	}
}

func validMySQLRetainedIntegerDeclaration(
	declaration *schema.DeclaredType,
) bool {
	if exactAdapterDeclaredTypeIn(
		declaration,
		"tinyint",
		"smallint",
		"mediumint",
		"int",
	) {
		return true
	}
	return declaration != nil &&
		declaration.Base == "tinyint" &&
		len(declaration.Arguments) == 1 &&
		declaration.Arguments[0] == 1 &&
		emptyAdapterNamedTypeFields(declaration)
}

func planSQLServerRetainedRowBound(
	namespace string,
	table schema.Table,
	columns []schema.Column,
) (adapterRetainedRowBoundPlan, error) {
	if err := validateAdapterRetainedPlanIdentifiers(
		"mssql",
		namespace,
		table,
		columns,
	); err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	plan, err := newAdapterRetainedRowBoundPlan(
		"mssql",
		sqlServerQualified(namespace, table.Name),
		len(columns),
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	for _, column := range columns {
		bound, err := sqlServerRetainedColumnBound(column)
		if err != nil {
			return adapterRetainedRowBoundPlan{}, fmt.Errorf(
				"plan SQL Server retained column %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		plan.columns = append(plan.columns, bound)
	}
	return finalizeAdapterRetainedLengthQuery(
		plan,
		sqlServerIdentifier,
	), nil
}

func sqlServerRetainedColumnBound(
	column schema.Column,
) (adapterRetainedColumnBound, error) {
	bound := adapterRetainedColumnBound{name: column.Name}
	switch column.Type {
	case "integer":
		if !exactAdapterDeclaredTypeIn(
			column.DeclaredType,
			"tinyint",
			"smallint",
			"int",
		) {
			return bound, errors.New("integer declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(int64(0)))
	case "bigint":
		if !exactAdapterDeclaredType(column.DeclaredType, "bigint") {
			return bound, errors.New("bigint declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(int64(0)))
	case "boolean":
		if !exactAdapterDeclaredType(column.DeclaredType, "bool") {
			return bound, errors.New("boolean declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(false))
	case "numeric":
		precision, scale, ok := exactAdapterDecimalIn(
			column.DeclaredType,
			"decimal",
			"numeric",
		)
		if !ok || precision > 38 ||
			scale < 0 || scale > precision {
			return bound, errors.New("numeric declaration is invalid")
		}
		payload, err := adapterDecimalTextBytes(precision, scale)
		if err != nil {
			return bound, err
		}
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			payload,
		)
		if err != nil {
			return bound, err
		}
	case "real":
		if !exactAdapterDeclaredType(column.DeclaredType, "real") {
			return bound, errors.New("real declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(float64(0)))
	case "double precision":
		if !exactAdapterDeclaredType(
			column.DeclaredType,
			"double precision",
		) {
			return bound, errors.New(
				"double precision declaration is invalid",
			)
		}
		bound.fixedBytes = int64(unsafe.Sizeof(float64(0)))
	case "text":
		if exactAdapterDeclaredType(column.DeclaredType, "text") {
			bound = sqlServerLiveRetainedColumnBound(
				column.Name,
				int64(unsafe.Sizeof("")),
			)
			break
		}
		length, ok := exactAdapterDeclaredLengthIn(
			column.DeclaredType,
			"char",
			"varchar",
		)
		if !ok || length > 8_000 {
			return bound, errors.New("text declaration is invalid")
		}
		var err error
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof("")),
			length,
		)
		if err != nil {
			return bound, err
		}
	case "blob":
		if exactAdapterDeclaredType(column.DeclaredType, "blob") {
			bound = sqlServerLiveRetainedColumnBound(
				column.Name,
				int64(unsafe.Sizeof([]byte(nil))),
			)
			break
		}
		length, ok := exactAdapterDeclaredLengthIn(
			column.DeclaredType,
			"binary",
			"varbinary",
		)
		if !ok || length > 8_000 {
			return bound, errors.New("binary declaration is invalid")
		}
		var err error
		bound.fixedBytes, err = addAdapterRetainedBytes(
			int64(unsafe.Sizeof([]byte(nil))),
			length,
		)
		if err != nil {
			return bound, err
		}
	case "date":
		if !exactAdapterDeclaredType(column.DeclaredType, "date") {
			return bound, errors.New("date declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(time.Time{}))
	case "time":
		if !validAdapterRequiredPrecision(column.DeclaredType, "time") {
			return bound, errors.New("time declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof("")) + 15
	case "datetime":
		if exactAdapterDeclaredType(
			column.DeclaredType,
			"smalldatetime",
		) {
			bound.fixedBytes = int64(unsafe.Sizeof(time.Time{}))
			break
		}
		if !validAdapterRequiredPrecision(
			column.DeclaredType,
			"timestamp",
		) {
			return bound, errors.New("datetime declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof(time.Time{}))
	case "uuid":
		if !exactAdapterDeclaredType(column.DeclaredType, "uuid") {
			return bound, errors.New("UUID declaration is invalid")
		}
		bound.fixedBytes = int64(unsafe.Sizeof("")) + 36
	default:
		return bound, fmt.Errorf(
			"unsupported SQL Server retained type %q",
			column.Type,
		)
	}
	return bound, nil
}

func sqlServerLiveRetainedColumnBound(
	name string,
	overhead int64,
) adapterRetainedColumnBound {
	return adapterRetainedColumnBound{
		name: name,
		liveExpression: "CONVERT(bigint, DATALENGTH(" +
			sqlServerIdentifier(name) + "))",
		liveOverhead:   overhead,
		liveMultiplier: 1,
	}
}

func planSQLiteRetainedRowBound(
	table schema.Table,
	columns []schema.Column,
) (adapterRetainedRowBoundPlan, error) {
	if err := validateAdapterRetainedPlanIdentifiers(
		"sqlite",
		"",
		table,
		columns,
	); err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	plan, err := newAdapterRetainedRowBoundPlan(
		"sqlite",
		quote(table.Name),
		len(columns),
	)
	if err != nil {
		return adapterRetainedRowBoundPlan{}, err
	}
	for _, column := range columns {
		if !validSQLiteRetainedColumn(column) {
			return adapterRetainedRowBoundPlan{}, fmt.Errorf(
				"plan SQLite retained column %s.%s: declaration is invalid",
				table.Name,
				column.Name,
			)
		}
		plan.columns = append(plan.columns, adapterRetainedColumnBound{
			name: column.Name,
			liveExpression: "length(CAST(" + quote(column.Name) +
				" AS BLOB))",
			// The established SQLite transfer proof reserves a fixed scalar
			// allowance plus both the driver value and DMTX-owned copy.
			liveOverhead:   64,
			liveMultiplier: 2,
		})
	}
	return finalizeAdapterRetainedLengthQuery(plan, quote), nil
}

func validateAdapterRetainedPlanIdentifiers(
	sourceEngine string,
	namespace string,
	table schema.Table,
	columns []schema.Column,
) error {
	if sourceEngine == "sqlite" {
		if namespace != "" || table.Schema != "" {
			return errors.New(
				"SQLite retained row planning does not accept a source schema",
			)
		}
	} else {
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"schema",
			namespace,
		); err != nil {
			return err
		}
		if table.Schema == "" {
			return fmt.Errorf(
				"%s retained row planning requires the discovered source schema",
				sourceEngine,
			)
		}
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"schema",
			table.Schema,
		); err != nil {
			return err
		}
		if table.Schema != namespace {
			return fmt.Errorf(
				"%s retained row planning has a mismatched source schema",
				sourceEngine,
			)
		}
	}
	if err := validateAdapterRetainedIdentifier(
		sourceEngine,
		"table",
		table.Name,
	); err != nil {
		return err
	}
	for index, column := range columns {
		if err := validateAdapterRetainedIdentifier(
			sourceEngine,
			"column",
			column.Name,
		); err != nil {
			return fmt.Errorf(
				"retained row planning for table %s has an invalid selected column at index %d: %w",
				table.Name,
				index,
				err,
			)
		}
	}
	return nil
}

func validateAdapterRetainedIdentifier(
	sourceEngine string,
	kind string,
	value string,
) error {
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return fmt.Errorf(
			"source %s has an empty or invalid name",
			kind,
		)
	}

	valid := true
	switch sourceEngine {
	case "postgres":
		valid = len(value) <= 63
	case "mysql":
		valid = !strings.HasSuffix(value, " ") &&
			utf8.RuneCountInString(value) <= 64
		if valid {
			for _, character := range value {
				if character > '\uFFFF' {
					valid = false
					break
				}
			}
		}
	case "mssql":
		valid = !strings.HasSuffix(value, " ") &&
			!strings.ContainsRune(value, '\uFFFD') &&
			len(utf16.Encode([]rune(value))) <= 128
	case "sqlite":
	default:
		return fmt.Errorf(
			"source engine %q has no retained identifier admission",
			sourceEngine,
		)
	}
	if !valid {
		return fmt.Errorf(
			"source %s name violates the %s identifier contract",
			kind,
			sourceEngine,
		)
	}
	return nil
}

func validSQLiteRetainedColumn(column schema.Column) bool {
	declaration := column.DeclaredType
	if declaration == nil ||
		declaration.Base != column.Type ||
		!emptyAdapterNamedTypeFields(declaration) {
		return false
	}
	maximumArguments, supported := map[string]int{
		"int": 0, "integer": 0, "tinyint": 0, "smallint": 0,
		"mediumint": 0, "bigint": 0, "unsigned big int": 0,
		"int2": 0, "int8": 0, "char": 1, "character": 1,
		"character varying": 1, "varchar": 1,
		"varying character": 1, "binary": 1, "varbinary": 1,
		"nchar": 1, "native character": 1, "nvarchar": 1,
		"text": 0, "clob": 0, "blob": 0, "real": 0,
		"double": 0, "double precision": 0, "float": 1,
		"numeric": 2, "decimal": 2, "boolean": 0, "bool": 0,
		"date": 0, "datetime": 1, "timestamp": 1, "time": 1,
		"json": 0, "uuid": 0, "any": 0,
	}[declaration.Base]
	if !supported || len(declaration.Arguments) > maximumArguments {
		return false
	}
	for _, argument := range declaration.Arguments {
		if argument < 0 {
			return false
		}
	}
	return true
}

func finalizeAdapterRetainedLengthQuery(
	plan adapterRetainedRowBoundPlan,
	quoteIdentifier func(string) string,
) adapterRetainedRowBoundPlan {
	terms := make([]string, 0, len(plan.columns))
	for index, column := range plan.columns {
		if column.liveExpression == "" {
			continue
		}
		alias := fmt.Sprintf("dmtx_retained_%d", index)
		plan.aliases = append(plan.aliases, alias)
		terms = append(
			terms,
			"MAX(CASE WHEN "+quoteIdentifier(column.name)+
				" IS NULL THEN NULL ELSE "+column.liveExpression+
				" END) AS "+quoteIdentifier(alias),
		)
	}
	if len(terms) != 0 {
		plan.query = "SELECT " + strings.Join(terms, ", ") +
			" FROM " + plan.table
	}
	return plan
}

func executeAdapterRetainedRowBound(
	ctx context.Context,
	queryer adapterRetainedLengthQueryer,
	plan adapterRetainedRowBoundPlan,
) (result int64, resultErr error) {
	if ctx == nil {
		return 0, errors.New("retained row bound context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if queryer == nil || plan.engine == "" || plan.table == "" ||
		plan.baseBytes <= 0 || len(plan.columns) == 0 {
		return 0, errors.New("retained row bound plan is incomplete")
	}
	maximums := make([]sql.NullInt64, len(plan.aliases))
	if len(plan.aliases) != 0 {
		rows, err := queryer.QueryContext(ctx, plan.query)
		if err != nil {
			return 0, fmt.Errorf(
				"measure %s retained row lengths for %s: %w",
				plan.engine,
				plan.table,
				err,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				result = 0
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf(
						"close %s retained length result: %w",
						plan.engine,
						closeErr,
					),
				)
			}
		}()
		actual, err := rows.Columns()
		if err != nil {
			return 0, fmt.Errorf(
				"inspect %s retained length result: %w",
				plan.engine,
				err,
			)
		}
		if len(actual) != len(plan.aliases) {
			return 0, fmt.Errorf(
				"%s retained length query returned %d columns, expected %d",
				plan.engine,
				len(actual),
				len(plan.aliases),
			)
		}
		for index := range actual {
			if actual[index] != plan.aliases[index] {
				return 0, fmt.Errorf(
					"%s retained length query column %d is %q, expected %q",
					plan.engine,
					index,
					actual[index],
					plan.aliases[index],
				)
			}
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return 0, fmt.Errorf(
					"read %s retained length result: %w",
					plan.engine,
					err,
				)
			}
			return 0, fmt.Errorf(
				"%s retained length query returned no aggregate row",
				plan.engine,
			)
		}
		destinations := make([]any, len(maximums))
		for index := range maximums {
			destinations[index] = &maximums[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return 0, fmt.Errorf(
				"scan %s retained length result: %w",
				plan.engine,
				err,
			)
		}
		if rows.Next() {
			return 0, fmt.Errorf(
				"%s retained length query returned multiple aggregate rows",
				plan.engine,
			)
		}
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf(
				"iterate %s retained length result: %w",
				plan.engine,
				err,
			)
		}
	}

	total := plan.baseBytes
	liveIndex := 0
	for _, column := range plan.columns {
		retained := column.fixedBytes
		if column.liveExpression != "" {
			if liveIndex >= len(maximums) ||
				column.liveOverhead <= 0 ||
				column.liveMultiplier <= 0 {
				return 0, errors.New(
					"retained row bound live evidence is incomplete",
				)
			}
			maximum := maximums[liveIndex]
			liveIndex++
			payload := int64(0)
			if maximum.Valid {
				if maximum.Int64 < 0 {
					return 0, fmt.Errorf(
						"%s retained length for column %s is negative",
						plan.engine,
						column.name,
					)
				}
				var err error
				payload, err = multiplyAdapterRetainedBytes(
					maximum.Int64,
					column.liveMultiplier,
				)
				if err != nil {
					return 0, fmt.Errorf(
						"%s retained length for column %s: %w",
						plan.engine,
						column.name,
						err,
					)
				}
			}
			var err error
			retained, err = addAdapterRetainedBytes(
				column.liveOverhead,
				payload,
			)
			if err != nil {
				return 0, err
			}
		}
		if retained <= 0 {
			return 0, fmt.Errorf(
				"%s retained bound for column %s is invalid",
				plan.engine,
				column.name,
			)
		}
		var err error
		total, err = addAdapterRetainedBytes(total, retained)
		if err != nil {
			return 0, fmt.Errorf(
				"%s retained row bound: %w",
				plan.engine,
				err,
			)
		}
	}
	if liveIndex != len(maximums) {
		return 0, errors.New(
			"retained row bound did not consume exact live evidence",
		)
	}
	// A range reader owns the already-cloned rows while database/sql scans the
	// next row. During conversion and []byte cloning, the raw row and its owned
	// form can coexist; the scan destination slice also remains live. Reserving
	// two complete maximum-sized rows plus one interface destination inventory
	// safely covers maxRows=1 and composes conservatively as
	// maxRows*UpperBoundBytes for larger pages.
	rawAndOwned, err := multiplyAdapterRetainedBytes(total, 2)
	if err != nil {
		return 0, fmt.Errorf(
			"%s retained row transient bound: %w",
			plan.engine,
			err,
		)
	}
	peak, err := addAdapterRetainedBytes(
		rawAndOwned,
		plan.baseBytes,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"%s retained row transient bound: %w",
			plan.engine,
			err,
		)
	}
	return peak, nil
}

func exactAdapterDeclaredType(
	declaration *schema.DeclaredType,
	base string,
) bool {
	return declaration != nil &&
		declaration.Base == base &&
		len(declaration.Arguments) == 0 &&
		emptyAdapterNamedTypeFields(declaration)
}

func exactAdapterDeclaredTypeIn(
	declaration *schema.DeclaredType,
	bases ...string,
) bool {
	if declaration == nil || len(declaration.Arguments) != 0 ||
		!emptyAdapterNamedTypeFields(declaration) {
		return false
	}
	for _, base := range bases {
		if declaration.Base == base {
			return true
		}
	}
	return false
}

func exactAdapterDeclaredLength(
	declaration *schema.DeclaredType,
	base string,
) (int64, bool) {
	if declaration == nil ||
		declaration.Base != base ||
		len(declaration.Arguments) != 1 ||
		declaration.Arguments[0] <= 0 ||
		!emptyAdapterNamedTypeFields(declaration) {
		return 0, false
	}
	return int64(declaration.Arguments[0]), true
}

func exactAdapterDeclaredLengthIn(
	declaration *schema.DeclaredType,
	bases ...string,
) (int64, bool) {
	if declaration == nil ||
		len(declaration.Arguments) != 1 ||
		declaration.Arguments[0] <= 0 ||
		!emptyAdapterNamedTypeFields(declaration) {
		return 0, false
	}
	for _, base := range bases {
		if declaration.Base == base {
			return int64(declaration.Arguments[0]), true
		}
	}
	return 0, false
}

func exactAdapterDecimal(
	declaration *schema.DeclaredType,
	base string,
) (int64, int64, bool) {
	if declaration == nil ||
		declaration.Base != base ||
		len(declaration.Arguments) != 2 ||
		declaration.Arguments[0] <= 0 ||
		!emptyAdapterNamedTypeFields(declaration) {
		return 0, 0, false
	}
	return int64(declaration.Arguments[0]),
		int64(declaration.Arguments[1]),
		true
}

func exactAdapterDecimalIn(
	declaration *schema.DeclaredType,
	bases ...string,
) (int64, int64, bool) {
	if declaration == nil ||
		len(declaration.Arguments) != 2 ||
		declaration.Arguments[0] <= 0 ||
		!emptyAdapterNamedTypeFields(declaration) {
		return 0, 0, false
	}
	for _, base := range bases {
		if declaration.Base == base {
			return int64(declaration.Arguments[0]),
				int64(declaration.Arguments[1]),
				true
		}
	}
	return 0, 0, false
}

func validAdapterOptionalPrecision(
	declaration *schema.DeclaredType,
	base string,
) bool {
	if declaration == nil {
		return true
	}
	return declaration.Base == base &&
		len(declaration.Arguments) == 1 &&
		declaration.Arguments[0] >= 0 &&
		declaration.Arguments[0] <= 6 &&
		emptyAdapterNamedTypeFields(declaration)
}

func validAdapterRequiredPrecision(
	declaration *schema.DeclaredType,
	base string,
) bool {
	return declaration != nil &&
		declaration.Base == base &&
		len(declaration.Arguments) == 1 &&
		declaration.Arguments[0] >= 0 &&
		declaration.Arguments[0] <= 6 &&
		emptyAdapterNamedTypeFields(declaration)
}

func emptyAdapterNamedTypeFields(
	declaration *schema.DeclaredType,
) bool {
	return declaration != nil &&
		declaration.Length == nil &&
		declaration.Precision == nil &&
		declaration.Scale == nil &&
		declaration.FractionalSecondPrecision == nil &&
		declaration.Spatial == nil &&
		declaration.MySQL == nil
}

func adapterDecimalTextBytes(
	precision int64,
	scale int64,
) (int64, error) {
	if precision <= 0 {
		return 0, errors.New("decimal precision is invalid")
	}
	if scale <= 0 {
		digits, err := addAdapterRetainedBytes(precision, -scale)
		if err != nil {
			return 0, err
		}
		return addAdapterRetainedBytes(1, digits)
	}
	integerDigits := precision - scale
	if integerDigits < 1 {
		integerDigits = 1
	}
	result, err := addAdapterRetainedBytes(1, integerDigits)
	if err != nil {
		return 0, err
	}
	result, err = addAdapterRetainedBytes(result, 1)
	if err != nil {
		return 0, err
	}
	return addAdapterRetainedBytes(result, scale)
}

func addAdapterRetainedBytes(left int64, right int64) (int64, error) {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return 0, errors.New("retained row byte count overflow")
	}
	return left + right, nil
}

func multiplyAdapterRetainedBytes(
	left int64,
	right int64,
) (int64, error) {
	if left < 0 || right < 0 ||
		left != 0 && right > math.MaxInt64/left {
		return 0, errors.New("retained row byte count overflow")
	}
	return left * right, nil
}

func measureAdapterRetainedRowBytes(values []any) (int64, error) {
	size, err := newAdapterRetainedRowBoundPlan(
		"measurement",
		"row",
		len(values),
	)
	if err != nil {
		return 0, err
	}
	total := size.baseBytes
	for _, value := range values {
		var retained int64
		switch typed := value.(type) {
		case nil:
		case []byte:
			retained = int64(unsafe.Sizeof([]byte(nil))) + int64(len(typed))
		case string:
			retained = int64(unsafe.Sizeof("")) + int64(len(typed))
		case int64:
			retained = int64(unsafe.Sizeof(typed))
		case int32:
			retained = int64(unsafe.Sizeof(typed))
		case int:
			retained = int64(unsafe.Sizeof(typed))
		case float64:
			retained = int64(unsafe.Sizeof(typed))
		case float32:
			retained = int64(unsafe.Sizeof(typed))
		case bool:
			retained = int64(unsafe.Sizeof(typed))
		case time.Time:
			retained = int64(unsafe.Sizeof(typed))
		default:
			return 0, fmt.Errorf(
				"source row has unsupported retained value type %T",
				value,
			)
		}
		var addErr error
		total, addErr = addAdapterRetainedBytes(total, retained)
		if addErr != nil {
			return 0, addErr
		}
	}
	return total, nil
}
