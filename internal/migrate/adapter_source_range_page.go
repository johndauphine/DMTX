package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// adapterNetworkRangePageSource is the relational source seam consumed by
// RunResumableNetworkTransfer. The caller supplies the immutable pagination
// plan and the one range bound to request.Range.
type adapterNetworkRangePageSource interface {
	ReadNetworkRangePage(
		context.Context,
		schema.Table,
		[]string,
		PaginationPlan,
		PaginationRange,
		NetworkReadRequest,
	) (NetworkReadPage, error)
}

var (
	_ adapterNetworkRangePageSource = (*relationalSourceAdapter)(nil)
	_ adapterNetworkRangePageSource = (*adapterRetainedStableRelationalView)(nil)
	_ adapterNetworkRangePageSource = (*sqliteSourceAdapter)(nil)
)

type adapterRangePageQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type adapterRangePageAdmission struct {
	engine       string
	namespace    string
	table        schema.Table
	columns      []schema.Column
	columnNames  []string
	keys         []KeySpec
	keyIndexes   []int
	lower        []int64
	hasLower     bool
	upper        []int64
	start        []int64
	hasStart     bool
	effective    []int64
	hasEffective bool
	keyLower     KeyTuple
	keyUpper     KeyTuple
	keyStart     KeyTuple
	keyEffective KeyTuple
	firstRow     int64
	lastRow      int64
	effectiveRow int64
	rowNumber    bool
	terminal     bool
	rangePlan    PaginationRange
	pagination   PaginationPlan
	request      NetworkReadRequest
}

type adapterRangePageQuery struct {
	SQL  string
	Args []any
}

func requireAdapterNetworkRangePageSource(
	source sourceAdapter,
) (adapterNetworkRangePageSource, error) {
	if isNilInterface(source) {
		return nil, adapterRangePagePolicy(
			errors.New("source adapter is required"),
		)
	}
	capability, ok := source.(adapterNetworkRangePageSource)
	if !ok || isNilInterface(capability) {
		return nil, adapterRangePagePolicy(fmt.Errorf(
			"%s source does not implement bounded network range reads",
			source.DisplayName(),
		))
	}
	return capability, nil
}

func (adapter *relationalSourceAdapter) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if adapter == nil || adapter.database == nil {
		return NetworkReadPage{}, adapterRangePagePolicy(
			errors.New("relational source adapter is not open"),
		)
	}
	return readAdapterNetworkRangePage(
		ctx,
		adapter.spec.engine,
		adapter.namespace,
		adapter.database,
		adapter.spec.wrapRows,
		table,
		columns,
		pagination,
		plannedRange,
		request,
	)
}

// ReadNetworkRangePage deliberately routes through the exact stable queryer
// that produced this view's dynamic retained-width evidence. It must never
// fall back to source.database: doing so could observe a wider mutable row
// after memory admission.
func (view *adapterRetainedStableRelationalView) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if view == nil || view.source == nil || isNilInterface(view.view) {
		return NetworkReadPage{}, adapterRangePagePolicy(
			errors.New("stable relational source view is unavailable"),
		)
	}
	if view.view.retainedStableViewEngine() != view.source.spec.engine {
		return NetworkReadPage{}, adapterRangePagePolicy(fmt.Errorf(
			"stable source view engine differs from source engine %q",
			view.source.spec.engine,
		))
	}
	if err := view.admitNetworkRangeRead(
		table,
		columns,
		pagination,
		request.Range.MaxRowBytes,
	); err != nil {
		return NetworkReadPage{}, adapterRangePagePolicy(err)
	}
	return readStableAdapterNetworkRangePage(
		ctx,
		view.source.spec.engine,
		view.source.namespace,
		view.view,
		view.source.spec.wrapRows,
		table,
		columns,
		pagination,
		plannedRange,
		request,
	)
}

func (adapter *sqliteSourceAdapter) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if adapter == nil || adapter.snapshot == nil {
		return NetworkReadPage{}, adapterRangePagePolicy(
			errors.New("SQLite source snapshot is not open"),
		)
	}
	return readStableAdapterNetworkRangePage(
		ctx,
		"sqlite",
		"",
		adapter.snapshot,
		nil,
		table,
		columns,
		pagination,
		plannedRange,
		request,
	)
}

func readAdapterNetworkRangePage(
	ctx context.Context,
	engine string,
	namespace string,
	queryer adapterRangePageQueryer,
	wrapRows func(adapterRows, schema.Table, []string) adapterRows,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	return readAdapterNetworkRangePageWithStability(
		ctx,
		engine,
		namespace,
		queryer,
		wrapRows,
		table,
		columns,
		pagination,
		plannedRange,
		request,
		false,
	)
}

func readStableAdapterNetworkRangePage(
	ctx context.Context,
	engine string,
	namespace string,
	queryer adapterRangePageQueryer,
	wrapRows func(adapterRows, schema.Table, []string) adapterRows,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	return readAdapterNetworkRangePageWithStability(
		ctx,
		engine,
		namespace,
		queryer,
		wrapRows,
		table,
		columns,
		pagination,
		plannedRange,
		request,
		true,
	)
}

func readAdapterNetworkRangePageWithStability(
	ctx context.Context,
	engine string,
	namespace string,
	queryer adapterRangePageQueryer,
	wrapRows func(adapterRows, schema.Table, []string) adapterRows,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
	stableView bool,
) (NetworkReadPage, error) {
	admission, err := admitAdapterNetworkRangePageWithStability(
		ctx,
		engine,
		namespace,
		table,
		columns,
		pagination,
		plannedRange,
		request,
		stableView,
	)
	if err != nil {
		return NetworkReadPage{}, err
	}
	if admission.terminal {
		return NetworkReadPage{
			EndFrontier: cloneNetworkBytes(request.StartFrontier),
			Exhausted:   true,
		}, nil
	}

	query, err := buildAdapterNetworkRangePageQuery(admission)
	if err != nil {
		return NetworkReadPage{}, adapterRangePagePolicy(err)
	}
	rows, err := queryer.QueryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return NetworkReadPage{}, fmt.Errorf(
			"read %s network range for table %s: %w",
			adapterRangePageDisplayName(engine),
			table.Name,
			err,
		)
	}
	var stream adapterRows = rows
	if wrapRows != nil {
		stream = wrapRows(stream, table, admission.columnNames)
	}
	page, last, scanErr := scanAdapterNetworkRangePage(
		stream,
		admission,
	)
	closeErr := stream.Close()
	if scanErr != nil {
		if closeErr != nil {
			scanErr = errors.Join(
				scanErr,
				fmt.Errorf("close source range cursor: %w", closeErr),
			)
		}
		return NetworkReadPage{}, scanErr
	}
	if closeErr != nil {
		return NetworkReadPage{}, fmt.Errorf(
			"close %s network range cursor for table %s: %w",
			adapterRangePageDisplayName(engine),
			table.Name,
			closeErr,
		)
	}
	if len(page.Rows) == 0 {
		if request.ReplayExpected != nil {
			return NetworkReadPage{}, adapterRangePageState(
				errors.New("replayed network range page disappeared"),
			)
		}
		if admission.rowNumber && !admission.terminal {
			return NetworkReadPage{}, adapterRangePageState(
				errors.New(
					"stable ROW_NUMBER range disappeared inside its immutable interval",
				),
			)
		}
		page.EndFrontier = cloneNetworkBytes(request.StartFrontier)
		page.Exhausted = true
		return page, nil
	}

	if admission.rowNumber {
		page.Exhausted = admission.effectiveRow+
			int64(len(page.Rows)) == admission.lastRow
	} else {
		page.Exhausted = len(page.Rows) < request.MaxRows ||
			adapterRangePageKeyTupleCompare(last, admission.keyUpper) == 0
	}
	if request.ReplayExpected != nil {
		// Replay MaxRows is the durable issued row count, not necessarily the
		// larger limit used by the original short terminal read. Preserve the
		// issued terminal fact while re-proving the complete row payload,
		// end frontier, and canonical fingerprint below.
		page.Exhausted = request.ReplayExpected.Exhausted
	}
	page.Fingerprint, err = fingerprintAdapterNetworkRangePage(
		admission,
		page,
	)
	if err != nil {
		return NetworkReadPage{}, NewTransferError(
			ErrorClassConversion,
			fmt.Errorf("fingerprint source range page: %w", err),
		)
	}
	if err := verifyAdapterNetworkRangeReplay(request, page); err != nil {
		return NetworkReadPage{}, err
	}
	return page, nil
}

func admitAdapterNetworkRangePage(
	ctx context.Context,
	engine string,
	namespace string,
	table schema.Table,
	selected []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (adapterRangePageAdmission, error) {
	return admitAdapterNetworkRangePageWithStability(
		ctx,
		engine,
		namespace,
		table,
		selected,
		pagination,
		plannedRange,
		request,
		false,
	)
}

func admitAdapterNetworkRangePageWithStability(
	ctx context.Context,
	engine string,
	namespace string,
	table schema.Table,
	selected []string,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
	stableView bool,
) (adapterRangePageAdmission, error) {
	if ctx == nil {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return adapterRangePageAdmission{}, err
	}
	switch engine {
	case "postgres", "mysql", "mssql", "sqlite":
	default:
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			fmt.Errorf("source engine %q has no network range reader", engine),
		)
	}
	if request.MaxRows < 1 ||
		request.MaxRows > config.MaxTransferChunkRows ||
		request.Range.MaxRowBytes < 1 ||
		request.Range.TableSchema != table.Schema ||
		request.Range.TableName != table.Name ||
		request.Range.Pagination != pagination.Strategy ||
		!validNetworkFactToken(request.Range.TopologyHash) {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("network range request differs from source plan"),
		)
	}
	if request.Attempt < 0 {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("network range request has a negative attempt"),
		)
	}
	if err := validateAdapterRangeReplayRequest(request); err != nil {
		return adapterRangePageAdmission{}, err
	}
	ordered, err := exactAdapterRetainedColumns(
		engine,
		table,
		selected,
	)
	if err != nil {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(err)
	}
	keys, err := adapterPaginationPrimaryKey(engine, namespace, table)
	if err != nil {
		return adapterRangePageAdmission{}, err
	}
	if pagination.Strategy != adapterPaginationStrategy(
		engine,
		table,
		keys,
	) {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("pagination strategy differs from exact source key evidence"),
		)
	}
	switch pagination.Strategy {
	case PaginationIntegerKeyset:
		if len(keys) != 1 {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New("integer keyset requires one complete primary-key column"),
			)
		}
	case PaginationTupleKeyset:
		if len(keys) < 1 ||
			!adapterPaginationTupleComparisonProven(
				engine,
				table,
				keys,
			) {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New("tuple keyset lacks complete order-equivalence evidence"),
			)
		}
	case PaginationRowNumber:
		if !stableView {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New(
					"ROW_NUMBER range reads require the stable source view that materialized their boundaries",
				),
			)
		}
	default:
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			fmt.Errorf(
				"pagination strategy %q has no bounded network range reader",
				pagination.Strategy,
			),
		)
	}
	if len(pagination.Keys) != len(keys) {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("pagination key inventory is incomplete"),
		)
	}
	for index, key := range pagination.Keys {
		if key.Name != keys[index].Name ||
			key.Kind != adapterPaginationKeyKind(engine, keys[index]) {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				fmt.Errorf(
					"pagination key %d differs from exact primary-key order evidence",
					index,
				),
			)
		}
		if pagination.Strategy == PaginationIntegerKeyset &&
			(key.Kind != KeyInteger ||
				!adapterPaginationExactSignedInteger(
					engine,
					keys[index],
				)) {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New(
					"integer keyset is not an exact signed-integer primary key",
				),
			)
		}
	}
	if !adapterRangePageTopologyToken(pagination.TopologyHash) {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("pagination topology hash is not canonical"),
		)
	}
	if err := validateAdapterRangePageInventory(
		pagination,
		plannedRange,
	); err != nil {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(err)
	}

	indexes := make([]int, len(pagination.Keys))
	for keyIndex, key := range pagination.Keys {
		indexes[keyIndex] = -1
		for columnIndex, column := range ordered {
			if column.Name == key.Name {
				indexes[keyIndex] = columnIndex
				break
			}
		}
		if indexes[keyIndex] < 0 {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				fmt.Errorf(
					"selected source columns omit primary-key column %s",
					key.Name,
				),
			)
		}
	}
	if pagination.Strategy == PaginationRowNumber {
		return admitAdapterRowNumberRangePage(
			engine,
			namespace,
			table,
			ordered,
			selected,
			keys,
			indexes,
			pagination,
			plannedRange,
			request,
		)
	}

	lower, hasLower, err := adapterRangePageKeyBound(
		plannedRange.Lower,
		pagination.Keys,
	)
	if err != nil {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("network range lower bound is malformed"),
		)
	}
	upper, hasUpper, err := adapterRangePageKeyBound(
		plannedRange.Upper,
		pagination.Keys,
	)
	if err != nil || !plannedRange.Empty && !hasUpper {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("network range upper bound is malformed"),
		)
	}
	start, hasStart, err := adapterRangePageKeyFrontier(
		request.StartFrontier,
		pagination.Keys,
	)
	if err != nil {
		return adapterRangePageAdmission{}, adapterRangePageState(
			errors.New("network start frontier is malformed"),
		)
	}
	if plannedRange.Empty {
		if hasLower || hasUpper || hasStart ||
			request.ReplayExpected != nil {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New("empty network range carries bounds or replay state"),
			)
		}
		return adapterRangePageAdmission{
			engine:      engine,
			namespace:   namespace,
			table:       table,
			columns:     ordered,
			columnNames: append([]string(nil), selected...),
			keys:        append([]KeySpec(nil), pagination.Keys...),
			keyIndexes:  indexes,
			terminal:    true,
			rangePlan:   plannedRange,
			pagination:  pagination,
			request:     request,
		}, nil
	}
	if hasLower && adapterRangePageKeyTupleCompare(lower, upper) >= 0 {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("network range bounds do not advance"),
		)
	}
	if hasStart && hasLower &&
		adapterRangePageKeyTupleCompare(start, lower) < 0 {
		return adapterRangePageAdmission{}, adapterRangePageState(
			errors.New("network start frontier precedes its immutable range"),
		)
	}
	if hasStart && adapterRangePageKeyTupleCompare(start, upper) > 0 {
		return adapterRangePageAdmission{}, adapterRangePageState(
			errors.New("network start frontier exceeds its immutable range"),
		)
	}
	effective := lower
	hasEffective := hasLower
	if hasStart {
		effective = start
		hasEffective = true
	}
	terminal := hasStart &&
		adapterRangePageKeyTupleCompare(start, upper) == 0
	if terminal && request.ReplayExpected != nil {
		return adapterRangePageAdmission{}, adapterRangePageState(
			errors.New("replay starts at an exhausted range frontier"),
		)
	}
	if request.ReplayExpected != nil {
		expectedEnd, valid, err := adapterRangePageKeyFrontier(
			request.ReplayExpected.EndFrontier,
			pagination.Keys,
		)
		if err != nil || !valid ||
			hasEffective &&
				adapterRangePageKeyTupleCompare(
					expectedEnd,
					effective,
				) <= 0 ||
			adapterRangePageKeyTupleCompare(expectedEnd, upper) > 0 ||
			adapterRangePageKeyTupleCompare(expectedEnd, upper) == 0 &&
				!request.ReplayExpected.Exhausted {
			return adapterRangePageAdmission{}, adapterRangePageState(
				errors.New("durable replay end frontier is malformed"),
			)
		}
	}
	lowerIntegers, _ := adapterRangePageIntegerTuple(lower)
	upperIntegers, _ := adapterRangePageIntegerTuple(upper)
	startIntegers, _ := adapterRangePageIntegerTuple(start)
	effectiveIntegers, _ := adapterRangePageIntegerTuple(effective)
	return adapterRangePageAdmission{
		engine:       engine,
		namespace:    namespace,
		table:        table,
		columns:      ordered,
		columnNames:  append([]string(nil), selected...),
		keys:         append([]KeySpec(nil), pagination.Keys...),
		keyIndexes:   indexes,
		lower:        lowerIntegers,
		hasLower:     hasLower,
		upper:        upperIntegers,
		start:        startIntegers,
		hasStart:     hasStart,
		effective:    effectiveIntegers,
		hasEffective: hasEffective,
		keyLower:     append(KeyTuple(nil), lower...),
		keyUpper:     append(KeyTuple(nil), upper...),
		keyStart:     append(KeyTuple(nil), start...),
		keyEffective: append(KeyTuple(nil), effective...),
		terminal:     terminal,
		rangePlan:    plannedRange,
		pagination:   pagination,
		request:      request,
	}, nil
}

func admitAdapterRowNumberRangePage(
	engine string,
	namespace string,
	table schema.Table,
	columns []schema.Column,
	columnNames []string,
	keys []schema.Column,
	keyIndexes []int,
	pagination PaginationPlan,
	plannedRange PaginationRange,
	request NetworkReadRequest,
) (adapterRangePageAdmission, error) {
	if plannedRange.Lower != nil || plannedRange.Upper != nil {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("ROW_NUMBER range carries tuple bounds"),
		)
	}
	if plannedRange.Empty {
		if plannedRange.FirstRow != 1 ||
			plannedRange.LastRow != 0 ||
			len(pagination.Ranges) != 1 ||
			request.StartFrontier != nil ||
			request.ReplayExpected != nil {
			return adapterRangePageAdmission{}, adapterRangePagePolicy(
				errors.New("empty ROW_NUMBER range is malformed"),
			)
		}
		return adapterRangePageAdmission{
			engine:       engine,
			namespace:    namespace,
			table:        table,
			columns:      columns,
			columnNames:  append([]string(nil), columnNames...),
			keys:         append([]KeySpec(nil), pagination.Keys...),
			keyIndexes:   append([]int(nil), keyIndexes...),
			firstRow:     1,
			effectiveRow: 0,
			rowNumber:    true,
			terminal:     true,
			rangePlan:    plannedRange,
			pagination:   pagination,
			request:      request,
		}, nil
	}
	if plannedRange.FirstRow < 1 ||
		plannedRange.LastRow < plannedRange.FirstRow {
		return adapterRangePageAdmission{}, adapterRangePagePolicy(
			errors.New("ROW_NUMBER range interval is malformed"),
		)
	}
	start, hasStart, err := adapterRangePageOrdinalFrontier(
		request.StartFrontier,
	)
	if err != nil {
		return adapterRangePageAdmission{}, adapterRangePageState(err)
	}
	effective := plannedRange.FirstRow - 1
	if hasStart {
		if start < effective || start > plannedRange.LastRow {
			return adapterRangePageAdmission{}, adapterRangePageState(
				errors.New(
					"ROW_NUMBER start frontier is outside its immutable interval",
				),
			)
		}
		effective = start
	}
	terminal := effective == plannedRange.LastRow
	if terminal && request.ReplayExpected != nil {
		return adapterRangePageAdmission{}, adapterRangePageState(
			errors.New("replay starts at an exhausted ROW_NUMBER interval"),
		)
	}
	if expected := request.ReplayExpected; expected != nil {
		end, valid, endErr := adapterRangePageOrdinalFrontier(
			expected.EndFrontier,
		)
		if endErr != nil || !valid ||
			end <= effective ||
			end > plannedRange.LastRow ||
			expected.Exhausted != (end == plannedRange.LastRow) {
			return adapterRangePageAdmission{}, adapterRangePageState(
				errors.New(
					"durable ROW_NUMBER replay end frontier is malformed",
				),
			)
		}
	}
	_ = keys
	return adapterRangePageAdmission{
		engine:       engine,
		namespace:    namespace,
		table:        table,
		columns:      columns,
		columnNames:  append([]string(nil), columnNames...),
		keys:         append([]KeySpec(nil), pagination.Keys...),
		keyIndexes:   append([]int(nil), keyIndexes...),
		firstRow:     plannedRange.FirstRow,
		lastRow:      plannedRange.LastRow,
		effectiveRow: effective,
		rowNumber:    true,
		terminal:     terminal,
		rangePlan:    plannedRange,
		pagination:   pagination,
		request:      request,
	}, nil
}

func validateAdapterRangeReplayRequest(
	request NetworkReadRequest,
) error {
	expected := request.ReplayExpected
	if expected == nil {
		return nil
	}
	if expected.RangeIndex != request.Range.RangeIndex ||
		expected.Sequence != request.Sequence ||
		expected.Rows != request.MaxRows ||
		expected.Rows < 1 ||
		expected.Rows > config.MaxTransferChunkRows ||
		!bytes.Equal(expected.StartFrontier, request.StartFrontier) ||
		len(expected.EndFrontier) == 0 ||
		!validNetworkFactToken(expected.Fingerprint) {
		return adapterRangePageState(
			errors.New("durable replay evidence differs from the source request"),
		)
	}
	return nil
}

func validateAdapterRangePageInventory(
	plan PaginationPlan,
	selected PaginationRange,
) error {
	if len(plan.Ranges) == 0 ||
		uint64(len(plan.Ranges)) > maximumRuntimeTuningRanges ||
		selected.ID < 0 ||
		selected.ID >= len(plan.Ranges) ||
		!adapterRangePageRangeEqual(plan.Ranges[selected.ID], selected) {
		return errors.New("pagination range is absent from the immutable plan")
	}
	var previous KeyTuple
	var previousLast int64
	for index, planned := range plan.Ranges {
		if planned.ID != index {
			return errors.New("pagination range IDs are not contiguous")
		}
		if planned.Empty {
			if len(plan.Ranges) != 1 || index != 0 ||
				planned.Lower != nil || planned.Upper != nil ||
				planned.LastRow != 0 ||
				plan.Strategy == PaginationRowNumber &&
					planned.FirstRow != 1 ||
				plan.Strategy != PaginationRowNumber &&
					planned.FirstRow != 0 {
				return errors.New("empty pagination range is malformed")
			}
			continue
		}
		if plan.Strategy == PaginationRowNumber {
			if planned.Lower != nil || planned.Upper != nil ||
				planned.FirstRow < 1 ||
				planned.LastRow < planned.FirstRow ||
				index == 0 && planned.FirstRow != 1 ||
				index > 0 && planned.FirstRow != previousLast+1 {
				return errors.New(
					"ROW_NUMBER pagination range is malformed",
				)
			}
			previousLast = planned.LastRow
			continue
		}
		if planned.FirstRow != 0 || planned.LastRow != 0 ||
			planned.Upper == nil ||
			index == 0 && planned.Lower != nil ||
			index > 0 && planned.Lower == nil {
			return errors.New("keyset pagination range is malformed")
		}
		lower, hasLower, err := adapterRangePageKeyBound(
			planned.Lower,
			plan.Keys,
		)
		if err != nil {
			return errors.New("pagination lower bound is malformed")
		}
		upper, _, err := adapterRangePageKeyBound(
			planned.Upper,
			plan.Keys,
		)
		if err != nil {
			return errors.New("pagination upper bound is malformed")
		}
		if index > 0 &&
			(!hasLower ||
				adapterRangePageKeyTupleCompare(lower, previous) != 0) {
			return errors.New("pagination ranges are not contiguous")
		}
		if hasLower &&
			adapterRangePageKeyTupleCompare(upper, lower) <= 0 {
			return errors.New("pagination range does not advance")
		}
		previous = upper
	}
	return nil
}

func adapterRangePageRangeEqual(
	left PaginationRange,
	right PaginationRange,
) bool {
	return left.ID == right.ID &&
		left.FirstRow == right.FirstRow &&
		left.LastRow == right.LastRow &&
		left.Empty == right.Empty &&
		adapterRangePageKeyTupleEqual(left.Lower, right.Lower) &&
		adapterRangePageKeyTupleEqual(left.Upper, right.Upper)
}

func adapterRangePageKeyTupleEqual(
	left *KeyTuple,
	right *KeyTuple,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if len(*left) != len(*right) {
		return false
	}
	for index := range *left {
		if (*left)[index] != (*right)[index] {
			return false
		}
	}
	return true
}

func adapterRangePageKeyBound(
	bound *KeyTuple,
	keys []KeySpec,
) (KeyTuple, bool, error) {
	if bound == nil {
		return nil, false, nil
	}
	if len(*bound) != len(keys) {
		return nil, false, errors.New("bound width differs from key width")
	}
	result := make(KeyTuple, len(keys))
	for index, value := range *bound {
		if keys[index].Kind == "" ||
			value.Kind != keys[index].Kind {
			return nil, false, errors.New("bound kind differs from key kind")
		}
		if _, ok := adapterPaginationKeyCompare(value, value); !ok {
			return nil, false, errors.New(
				"bound key encoding is not canonical",
			)
		}
		result[index] = value
	}
	return result, true, nil
}

func adapterRangePageKeyFrontier(
	encoded []byte,
	keys []KeySpec,
) (KeyTuple, bool, error) {
	tuple, valid, err := decodeNetworkStateFrontier(encoded)
	if err != nil {
		return nil, false, errors.New("typed frontier is invalid")
	}
	if !valid {
		return nil, false, nil
	}
	if len(tuple) != len(keys) {
		return nil, false, errors.New("typed frontier width differs from key width")
	}
	result := make(KeyTuple, len(keys))
	for index, value := range tuple {
		switch keys[index].Kind {
		case KeyInteger:
			if value.Kind != state.ValueInt64 {
				return nil, false, errors.New(
					"typed frontier kind differs from pagination key",
				)
			}
			result[index] = KeyValue{
				Kind:    KeyInteger,
				Encoded: value.Encoded,
			}
		case KeyText:
			if value.Kind != state.ValueText {
				return nil, false, errors.New(
					"typed frontier kind differs from pagination key",
				)
			}
			result[index] = KeyValue{
				Kind:    KeyText,
				Encoded: value.Encoded,
			}
		case KeyBytes:
			if value.Kind != state.ValueBytes {
				return nil, false, errors.New(
					"typed frontier kind differs from pagination key",
				)
			}
			result[index] = KeyValue{
				Kind:    KeyBytes,
				Encoded: value.Encoded,
			}
		default:
			return nil, false, errors.New(
				"pagination key has no typed frontier representation",
			)
		}
		if _, ok := adapterPaginationKeyCompare(
			result[index],
			result[index],
		); !ok {
			return nil, false, errors.New(
				"typed frontier key is not canonical",
			)
		}
	}
	return result, true, nil
}

func adapterRangePageOrdinalFrontier(
	encoded []byte,
) (int64, bool, error) {
	tuple, valid, err := adapterRangePageKeyFrontier(
		encoded,
		[]KeySpec{{Name: "row_number", Kind: KeyInteger}},
	)
	if err != nil || !valid {
		return 0, valid, err
	}
	value, err := strconv.ParseInt(tuple[0].Encoded, 10, 64)
	if err != nil {
		return 0, false, errors.New(
			"ROW_NUMBER frontier is not a signed integer",
		)
	}
	return value, true, nil
}

func adapterRangePageTopologyToken(value string) bool {
	if len(value) != sha256.Size*2 ||
		value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func buildAdapterNetworkRangePageQuery(
	admission adapterRangePageAdmission,
) (adapterRangePageQuery, error) {
	if admission.rowNumber {
		return buildAdapterRowNumberRangePageQuery(admission)
	}
	projection, err := adapterRangePageProjection(
		admission.engine,
		admission.table,
		admission.columnNames,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	qualified, err := adapterPaginationQualified(
		admission.engine,
		admission.namespace,
		admission.table.Name,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	limit := admission.request.MaxRows
	position := 1
	args := make([]any, 0, len(admission.keys)*2+1)
	query := "SELECT "
	if admission.engine == "mssql" {
		query += "TOP (" +
			adapterPaginationPlaceholder(admission.engine, position) +
			") "
		args = append(args, limit)
		position++
	}
	query += projection + " FROM " + qualified
	predicates := make([]string, 0, 2)
	if admission.hasEffective {
		effective, err := adapterRangePageAdmissionTuple(
			admission.keyEffective,
			admission.effective,
			admission.keys,
		)
		if err != nil {
			return adapterRangePageQuery{}, err
		}
		predicate, values, next, err := adapterRangePageKeyPredicate(
			admission.engine,
			admission.keys,
			">",
			effective,
			position,
		)
		if err != nil {
			return adapterRangePageQuery{}, err
		}
		predicates = append(predicates, predicate)
		args = append(args, values...)
		position = next
	}
	upper, err := adapterRangePageAdmissionTuple(
		admission.keyUpper,
		admission.upper,
		admission.keys,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	predicate, values, next, err := adapterRangePageKeyPredicate(
		admission.engine,
		admission.keys,
		"<=",
		upper,
		position,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	predicates = append(predicates, predicate)
	args = append(args, values...)
	position = next
	query += " WHERE " + strings.Join(predicates, " AND ")
	keyNames := make([]string, len(admission.keys))
	for index, key := range admission.keys {
		keyNames[index] = key.Name
	}
	query += " ORDER BY " +
		adapterPaginationOrderBy(admission.engine, keyNames, false)
	if admission.engine != "mssql" {
		query += " LIMIT " +
			adapterPaginationPlaceholder(admission.engine, position)
		args = append(args, limit)
	}
	return adapterRangePageQuery{SQL: query, Args: args}, nil
}

func buildAdapterRowNumberRangePageQuery(
	admission adapterRangePageAdmission,
) (adapterRangePageQuery, error) {
	if admission.firstRow < 1 ||
		admission.lastRow < admission.firstRow ||
		admission.effectiveRow < admission.firstRow-1 ||
		admission.effectiveRow >= admission.lastRow {
		return adapterRangePageQuery{}, errors.New(
			"ROW_NUMBER query has an invalid immutable interval",
		)
	}
	projection, err := adapterRangePageProjection(
		admission.engine,
		admission.table,
		admission.columnNames,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	qualified, err := adapterPaginationQualified(
		admission.engine,
		admission.namespace,
		admission.table.Name,
	)
	if err != nil {
		return adapterRangePageQuery{}, err
	}
	keyNames := make([]string, len(admission.keys))
	for index, key := range admission.keys {
		keyNames[index] = key.Name
	}
	if len(keyNames) == 0 {
		return adapterRangePageQuery{}, errors.New(
			"ROW_NUMBER query requires a complete primary key",
		)
	}
	aliasName := adapterPaginationAlias(
		append(
			append([]string(nil), admission.columnNames...),
			keyNames...,
		),
		"dmtx_network_row_number",
	)
	alias := adapterPaginationIdentifier(admission.engine, aliasName)
	order := adapterPaginationOrderBy(
		admission.engine,
		keyNames,
		false,
	)
	outerProjection := adapterPaginationQuotedColumns(
		admission.engine,
		admission.columnNames,
	)
	position := 1
	args := make([]any, 0, 3)
	top := ""
	if admission.engine == "mssql" {
		top = "TOP (" +
			adapterPaginationPlaceholder(admission.engine, position) +
			") "
		args = append(args, admission.request.MaxRows)
		position++
	}
	lower := adapterPaginationPlaceholder(admission.engine, position)
	args = append(args, admission.effectiveRow)
	position++
	upper := adapterPaginationPlaceholder(admission.engine, position)
	args = append(args, admission.lastRow)
	position++
	query := "WITH dmtx_network_ranked AS (" +
		"SELECT " + projection + ", ROW_NUMBER() OVER (ORDER BY " +
		order + ") AS " + alias + " FROM " + qualified +
		") SELECT " + top + outerProjection +
		" FROM dmtx_network_ranked WHERE " + alias + " > " + lower +
		" AND " + alias + " <= " + upper +
		" ORDER BY " + alias
	if admission.engine != "mssql" {
		query += " LIMIT " +
			adapterPaginationPlaceholder(admission.engine, position)
		args = append(args, admission.request.MaxRows)
	}
	return adapterRangePageQuery{SQL: query, Args: args}, nil
}

func adapterRangePageAdmissionTuple(
	typed KeyTuple,
	integers []int64,
	keys []KeySpec,
) (KeyTuple, error) {
	if len(typed) == len(keys) {
		return append(KeyTuple(nil), typed...), nil
	}
	if len(integers) != len(keys) {
		return nil, errors.New(
			"range query bound differs from pagination key width",
		)
	}
	result := make(KeyTuple, len(integers))
	for index, value := range integers {
		if keys[index].Kind != KeyInteger {
			return nil, errors.New(
				"range query lacks its typed key bound",
			)
		}
		result[index] = IntegerKey(value)
	}
	return result, nil
}

func adapterRangePageProjection(
	engine string,
	table schema.Table,
	columns []string,
) (string, error) {
	switch engine {
	case "postgres":
		return adapterPaginationQuotedColumns(engine, columns), nil
	case "mysql":
		return mySQLReadProjection(table, columns), nil
	case "mssql":
		return adapterPaginationQuotedColumns(engine, columns), nil
	case "sqlite":
		return sqliteSourceProjection(table, columns)
	default:
		return "", fmt.Errorf(
			"source engine %q has no range-page projection",
			engine,
		)
	}
}

func adapterRangePagePredicate(
	engine string,
	keys []KeySpec,
	operator string,
	values []int64,
	position int,
) (string, []any, int, error) {
	typed := make(KeyTuple, len(values))
	for index, value := range values {
		typed[index] = IntegerKey(value)
	}
	return adapterRangePageKeyPredicate(
		engine,
		keys,
		operator,
		typed,
		position,
	)
}

func adapterRangePageKeyPredicate(
	engine string,
	keys []KeySpec,
	operator string,
	values KeyTuple,
	position int,
) (string, []any, int, error) {
	if len(keys) == 0 || len(keys) != len(values) ||
		(operator != ">" && operator != "<=") {
		return "", nil, position, errors.New(
			"range predicate has an invalid key shape",
		)
	}
	arguments := make([]any, len(values))
	for index, key := range keys {
		if key.Kind == "" || values[index].Kind != key.Kind {
			return "", nil, position, errors.New(
				"range predicate key kind differs from its bound",
			)
		}
		value, err := values[index].SQLValue()
		if err != nil {
			return "", nil, position, errors.New(
				"range predicate key encoding is invalid",
			)
		}
		arguments[index] = value
	}
	if len(keys) == 1 {
		placeholder := adapterPaginationPlaceholder(engine, position)
		return adapterPaginationIdentifier(engine, keys[0].Name) +
				" " + operator + " " + placeholder,
			[]any{arguments[0]},
			position + 1,
			nil
	}
	if engine == "mssql" {
		return adapterRangePageSQLServerTuplePredicate(
			keys,
			operator,
			arguments,
			position,
		)
	}
	names := make([]string, len(keys))
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for index, key := range keys {
		names[index] = adapterPaginationIdentifier(engine, key.Name)
		placeholders[index] = adapterPaginationPlaceholder(
			engine,
			position+index,
		)
		args[index] = arguments[index]
	}
	return "(" + strings.Join(names, ", ") + ") " + operator +
			" (" + strings.Join(placeholders, ", ") + ")",
		args,
		position + len(keys),
		nil
}

func adapterRangePageSQLServerTuplePredicate(
	keys []KeySpec,
	operator string,
	values []any,
	position int,
) (string, []any, int, error) {
	if len(keys) < 2 || len(keys) != len(values) ||
		(operator != ">" && operator != "<=") {
		return "", nil, position, errors.New(
			"SQL Server tuple predicate has an invalid key shape",
		)
	}
	terms := make([]string, len(keys))
	arguments := make([]any, 0, len(keys)*(len(keys)+1)/2)
	next := position
	for termIndex := range keys {
		parts := make([]string, 0, termIndex+1)
		for equalIndex := 0; equalIndex < termIndex; equalIndex++ {
			parts = append(
				parts,
				adapterPaginationIdentifier(
					"mssql",
					keys[equalIndex].Name,
				)+" = "+
					adapterPaginationPlaceholder("mssql", next),
			)
			arguments = append(arguments, values[equalIndex])
			next++
		}
		comparison := operator
		if operator == "<=" && termIndex < len(keys)-1 {
			comparison = "<"
		}
		parts = append(
			parts,
			adapterPaginationIdentifier(
				"mssql",
				keys[termIndex].Name,
			)+" "+comparison+" "+
				adapterPaginationPlaceholder("mssql", next),
		)
		arguments = append(arguments, values[termIndex])
		next++
		terms[termIndex] = "(" + strings.Join(parts, " AND ") + ")"
	}
	return "(" + strings.Join(terms, " OR ") + ")",
		arguments,
		next,
		nil
}

func scanAdapterNetworkRangePage(
	rows adapterRows,
	admission adapterRangePageAdmission,
) (NetworkReadPage, KeyTuple, error) {
	page := NetworkReadPage{
		Rows:     make([][]any, 0, admission.request.MaxRows),
		RowBytes: make([]int64, 0, admission.request.MaxRows),
	}
	var previous KeyTuple
	if admission.hasEffective && !admission.rowNumber {
		var err error
		previous, err = adapterRangePageAdmissionTuple(
			admission.keyEffective,
			admission.effective,
			admission.keys,
		)
		if err != nil {
			return NetworkReadPage{}, nil, adapterRangePagePolicy(err)
		}
	}
	for len(page.Rows) < admission.request.MaxRows && rows.Next() {
		values := make([]any, len(admission.columnNames))
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return NetworkReadPage{}, nil, NewTransferError(
				ErrorClassConversion,
				fmt.Errorf(
					"scan source network range row for table %s: %w",
					admission.table.Name,
					err,
				),
			)
		}
		owned := cloneAdapterRow(values)
		if !admission.rowNumber {
			frontier, err := adapterRangePageRowKeyFrontier(
				owned,
				admission,
			)
			if err != nil {
				return NetworkReadPage{}, nil, err
			}
			if previous != nil &&
				adapterRangePageKeyTupleCompare(frontier, previous) <= 0 {
				return NetworkReadPage{}, nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"source network range order did not advance for table %s",
						admission.table.Name,
					),
				)
			}
			if adapterRangePageKeyTupleCompare(
				frontier,
				admission.keyUpper,
			) > 0 {
				return NetworkReadPage{}, nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"source network range exceeded its immutable upper bound for table %s",
						admission.table.Name,
					),
				)
			}
			previous = frontier
		}
		retained, err := measureAdapterRetainedRowBytes(owned)
		if err != nil {
			return NetworkReadPage{}, nil, NewTransferError(
				ErrorClassConversion,
				fmt.Errorf(
					"measure source network range row for table %s: %w",
					admission.table.Name,
					err,
				),
			)
		}
		if retained < 1 ||
			retained > admission.request.Range.MaxRowBytes ||
			page.RetainedBytes > math.MaxInt64-retained {
			return NetworkReadPage{}, nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"source network range row exceeds its admitted memory bound for table %s",
					admission.table.Name,
				),
			)
		}
		page.Rows = append(page.Rows, owned)
		page.RowBytes = append(page.RowBytes, retained)
		page.RetainedBytes += retained
	}
	if err := rows.Err(); err != nil {
		return NetworkReadPage{}, nil, fmt.Errorf(
			"iterate source network range for table %s: %w",
			admission.table.Name,
			err,
		)
	}
	if len(page.Rows) == 0 {
		return page, nil, nil
	}
	var frontier []byte
	var err error
	if admission.rowNumber {
		expected := admission.lastRow - admission.effectiveRow
		if expected > int64(admission.request.MaxRows) {
			expected = int64(admission.request.MaxRows)
		}
		if int64(len(page.Rows)) != expected {
			return NetworkReadPage{}, nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"stable ROW_NUMBER range for table %s returned an incomplete immutable interval",
					admission.table.Name,
				),
			)
		}
		frontier, err = adapterRangePageEncodeFrontier(
			[]int64{
				admission.effectiveRow + int64(len(page.Rows)),
			},
		)
	} else {
		frontier, err = adapterRangePageEncodeKeyFrontier(previous)
	}
	if err != nil {
		return NetworkReadPage{}, nil, NewTransferError(
			ErrorClassState,
			errors.New("encode source network range frontier"),
		)
	}
	page.EndFrontier = frontier
	return page, previous, nil
}

func adapterRangePageRowKeyFrontier(
	row []any,
	admission adapterRangePageAdmission,
) (KeyTuple, error) {
	result := make(KeyTuple, len(admission.keyIndexes))
	for index, columnIndex := range admission.keyIndexes {
		if columnIndex < 0 || columnIndex >= len(row) {
			return nil, adapterRangePagePolicy(
				errors.New("source pagination key is absent from the selected row"),
			)
		}
		value, err := adapterPaginationTypedKey(
			row[columnIndex],
			admission.keys[index].Kind,
		)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassConversion,
				fmt.Errorf(
					"source pagination key %s has an unexpected driver value shape",
					admission.keys[index].Name,
				),
			)
		}
		result[index] = value
	}
	return result, nil
}

func adapterRangePageRowFrontier(
	row []any,
	admission adapterRangePageAdmission,
) ([]int64, error) {
	typed, err := adapterRangePageRowKeyFrontier(row, admission)
	if err != nil {
		return nil, err
	}
	result, ok := adapterRangePageIntegerTuple(typed)
	if !ok {
		return nil, NewTransferError(
			ErrorClassConversion,
			errors.New(
				"source pagination frontier is not a signed-integer tuple",
			),
		)
	}
	return result, nil
}

func adapterRangePageInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int:
		return int64(typed), strconv.IntSize == 64 ||
			int64(typed) >= math.MinInt32 &&
				int64(typed) <= math.MaxInt32
	case []byte:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil &&
			strconv.FormatInt(parsed, 10) == string(typed)
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil &&
			strconv.FormatInt(parsed, 10) == typed
	default:
		return 0, false
	}
}

func adapterRangePageEncodeFrontier(values []int64) ([]byte, error) {
	tuple := make(state.TypedTuple, len(values))
	for index, value := range values {
		tuple[index] = state.Int64Value(value)
	}
	return encodeNetworkStateFrontier(tuple, true)
}

func adapterRangePageEncodeKeyFrontier(
	values KeyTuple,
) ([]byte, error) {
	tuple := make(state.TypedTuple, len(values))
	for index, value := range values {
		switch value.Kind {
		case KeyInteger:
			parsed, err := strconv.ParseInt(value.Encoded, 10, 64)
			if err != nil ||
				strconv.FormatInt(parsed, 10) != value.Encoded {
				return nil, errors.New(
					"pagination integer frontier is not canonical",
				)
			}
			tuple[index] = state.Int64Value(parsed)
		case KeyText:
			if !utf8.ValidString(value.Encoded) {
				return nil, errors.New(
					"pagination text frontier is invalid UTF-8",
				)
			}
			tuple[index] = state.TextValue(value.Encoded)
		case KeyBytes:
			decoded, err := value.SQLValue()
			if err != nil {
				return nil, errors.New(
					"pagination byte frontier is not canonical",
				)
			}
			bytesValue, ok := decoded.([]byte)
			if !ok || BytesKey(bytesValue).Encoded != value.Encoded {
				return nil, errors.New(
					"pagination byte frontier is not canonical",
				)
			}
			tuple[index] = state.BytesValue(bytesValue)
		default:
			return nil, fmt.Errorf(
				"pagination frontier has unsupported kind %q",
				value.Kind,
			)
		}
	}
	return encodeNetworkStateFrontier(tuple, true)
}

func adapterRangePageTupleCompare(left, right []int64) int {
	if len(left) != len(right) {
		return 0
	}
	for index := range left {
		switch {
		case left[index] < right[index]:
			return -1
		case left[index] > right[index]:
			return 1
		}
	}
	return 0
}

func adapterRangePageKeyTupleCompare(left, right KeyTuple) int {
	if len(left) != len(right) {
		return 0
	}
	for index := range left {
		comparison, ok := adapterPaginationKeyCompare(
			left[index],
			right[index],
		)
		if !ok {
			return 0
		}
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func adapterRangePageIntegerTuple(value KeyTuple) ([]int64, bool) {
	result := make([]int64, len(value))
	for index, key := range value {
		if key.Kind != KeyInteger {
			return nil, false
		}
		parsed, err := strconv.ParseInt(key.Encoded, 10, 64)
		if err != nil || strconv.FormatInt(parsed, 10) != key.Encoded {
			return nil, false
		}
		result[index] = parsed
	}
	return result, true
}

func fingerprintAdapterNetworkRangePage(
	admission adapterRangePageAdmission,
	page NetworkReadPage,
) (string, error) {
	digest := sha256.New()
	adapterRangePageHashBytes(
		digest,
		[]byte("dmtx-network-range-page-v1"),
	)
	adapterRangePageHashBytes(
		digest,
		[]byte(admission.pagination.TopologyHash),
	)
	adapterRangePageHashBytes(
		digest,
		[]byte(admission.request.Range.TopologyHash),
	)
	adapterRangePageHashUint64(
		digest,
		admission.request.Range.RangeIndex,
	)
	adapterRangePageHashUint64(digest, admission.request.Sequence)
	adapterRangePageHashBytes(digest, admission.request.StartFrontier)
	adapterRangePageHashBytes(digest, page.EndFrontier)
	if page.Exhausted {
		digest.Write([]byte{1})
	} else {
		digest.Write([]byte{0})
	}
	adapterRangePageHashUint64(digest, uint64(len(admission.columnNames)))
	for _, column := range admission.columnNames {
		adapterRangePageHashBytes(digest, []byte(column))
	}
	adapterRangePageHashUint64(digest, uint64(len(page.Rows)))
	for _, row := range page.Rows {
		if len(row) != len(admission.columnNames) {
			return "", errors.New("source row width differs from selected columns")
		}
		for _, value := range row {
			if err := adapterRangePageHashValue(digest, value); err != nil {
				return "", err
			}
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func adapterRangePageHashValue(digest hash.Hash, value any) error {
	switch typed := value.(type) {
	case nil:
		digest.Write([]byte{0})
	case []byte:
		digest.Write([]byte{1})
		adapterRangePageHashBytes(digest, typed)
	case string:
		digest.Write([]byte{2})
		adapterRangePageHashBytes(digest, []byte(typed))
	case int64:
		digest.Write([]byte{3})
		adapterRangePageHashInt64(digest, typed)
	case int32:
		digest.Write([]byte{3})
		adapterRangePageHashInt64(digest, int64(typed))
	case int:
		digest.Write([]byte{3})
		adapterRangePageHashInt64(digest, int64(typed))
	case float64:
		digest.Write([]byte{4})
		adapterRangePageHashUint64(digest, math.Float64bits(typed))
	case float32:
		digest.Write([]byte{4})
		adapterRangePageHashUint64(
			digest,
			math.Float64bits(float64(typed)),
		)
	case bool:
		if typed {
			digest.Write([]byte{5, 1})
		} else {
			digest.Write([]byte{5, 0})
		}
	case time.Time:
		digest.Write([]byte{6})
		encoded, err := typed.MarshalBinary()
		if err != nil {
			return errors.New("source time value has no canonical encoding")
		}
		adapterRangePageHashBytes(digest, encoded)
	default:
		return fmt.Errorf(
			"source row has unsupported fingerprint value type %T",
			value,
		)
	}
	return nil
}

func adapterRangePageHashBytes(digest hash.Hash, value []byte) {
	adapterRangePageHashUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

func adapterRangePageHashInt64(digest hash.Hash, value int64) {
	adapterRangePageHashUint64(digest, uint64(value))
}

func adapterRangePageHashUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func verifyAdapterNetworkRangeReplay(
	request NetworkReadRequest,
	page NetworkReadPage,
) error {
	expected := request.ReplayExpected
	if expected == nil {
		return nil
	}
	if len(page.Rows) != expected.Rows ||
		!bytes.Equal(page.EndFrontier, expected.EndFrontier) ||
		page.Fingerprint != expected.Fingerprint ||
		page.Exhausted != expected.Exhausted {
		return adapterRangePageState(
			errors.New("source network range replay differs from durable intent"),
		)
	}
	return nil
}

func adapterRangePageDisplayName(engine string) string {
	switch engine {
	case "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL-family"
	case "mssql":
		return "SQL Server"
	case "sqlite":
		return "SQLite"
	default:
		return "relational"
	}
}

func adapterRangePagePolicy(err error) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf("read source network range: %w", err),
	)
}

func adapterRangePageState(err error) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf("restore source network range: %w", err),
	)
}
