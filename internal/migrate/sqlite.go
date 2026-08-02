// Package migrate contains database-to-database migration services.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

type Result struct {
	Tables        int                  `json:"tables"`
	Rows          int                  `json:"rows"`
	Validated     bool                 `json:"validated"`
	RuntimeTuning *RuntimeTuningReport `json:"runtime_tuning,omitempty"`
}

const sqliteWriteBatchSize = 500

const sqliteInsertOnlyReplayMode = "sqlite_insert_only_replay"

type TableObserver interface {
	BeforeTable(context.Context, string) error
	AfterTable(context.Context, string, int) error
}

// TableSetObserver receives the complete deterministic table set before the
// first target mutation. It is optional so existing per-table observers remain
// source compatible.
type TableSetObserver interface {
	BeforeTables(context.Context, []string) error
}

// Stage4TablePublicationObserver observes a table only after the composed
// Stage 4 aggregate transaction has durably published its ordinary checkpoint,
// structured work, and range evidence. It is deliberately separate from
// TableObserver.AfterTable: the latter owns the ordinary checkpoint on legacy
// routes, while calling it again after aggregate completion would attempt a
// second, non-idempotent state mutation.
type Stage4TablePublicationObserver interface {
	AfterStage4TablePublication(context.Context, string, int) error
}

// PageObserver records target-acknowledged page frontiers. It is optional so
// table-level observers remain compatible.
type PageObserver interface {
	AfterIntegerKeysetPage(context.Context, string, int, int64) error
	AfterRowNumberPage(context.Context, string, int, int64) error
}

// SQLiteWriteBoundary identifies a durable SQLite target or checkpoint write.
// Boundary observers are optional and are used by fault-injection validation.
type SQLiteWriteBoundary string

const (
	SQLiteBoundaryTableSetCheckpoint SQLiteWriteBoundary = "table_set_checkpointed"
	SQLiteBoundaryTableDropped       SQLiteWriteBoundary = "table_dropped"
	SQLiteBoundaryTableCreated       SQLiteWriteBoundary = "table_created"
	SQLiteBoundaryPageCommitted      SQLiteWriteBoundary = "page_committed"
	SQLiteBoundaryPageCheckpointed   SQLiteWriteBoundary = "page_checkpointed"
	SQLiteBoundaryIndexCreated       SQLiteWriteBoundary = "index_created"
	SQLiteBoundarySequenceCommitted  SQLiteWriteBoundary = "sequence_committed"
)

// SQLiteWriteBoundaryObserver receives a synchronous notification immediately
// after a durable SQLite write boundary. Returning an error stops the migration.
type SQLiteWriteBoundaryObserver interface {
	AfterSQLiteWriteBoundary(context.Context, SQLiteWriteBoundary, string) error
}

func notifySQLiteWriteBoundary(ctx context.Context, observer TableObserver, boundary SQLiteWriteBoundary, table string) error {
	if boundaryObserver, ok := observer.(SQLiteWriteBoundaryObserver); ok {
		if err := boundaryObserver.AfterSQLiteWriteBoundary(ctx, boundary, table); err != nil {
			return fmt.Errorf("observe SQLite write boundary %s for %s: %w", boundary, table, err)
		}
	}
	return nil
}

// TableProgress is the durable, reusable portion of an incomplete table.
type TableProgress struct {
	RowsDone           int
	IntegerWatermark   *int64
	RowNumberWatermark *int64
}

func SQLiteToSQLite(ctx context.Context, cfg config.Config) (Result, error) {
	return SQLiteToSQLiteWithObserver(ctx, cfg, nil)
}

func SQLiteToSQLiteWithObserver(ctx context.Context, cfg config.Config, observer TableObserver) (Result, error) {
	return runSQLiteToSQLite(ctx, cfg, nil, nil, observer, false)
}

func selectedTables(names []string, cfg config.Config) ([]string, error) {
	selected, err := config.SelectTables(names, cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables)
	if err != nil {
		return nil, fmt.Errorf("select source tables: %w", err)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no source tables match migration filters")
	}
	return selected, nil
}

func userTables(ctx context.Context, database *sql.DB) ([]string, error) {
	rows, err := database.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND lower(name) <> 'dmtx_internal_delete_batch_receipts' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source tables: %w", err)
	}
	return names, nil
}

func copyTable(ctx context.Context, source, target *sql.DB, name, mode string, observer TableObserver, progress TableProgress, resumeExisting bool) (int, error) {
	table, columns, err := inspectTable(ctx, source, name)
	if err != nil {
		return 0, err
	}
	if !hasPrimaryKey(table) {
		return 0, fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
	}
	created, err := prepareTargetWithStatus(ctx, target, table, mode, observer)
	if err != nil {
		return 0, err
	}
	finalizeExisting := false
	if !created && resumeExisting {
		finalizeExisting, err = sqliteTableMatchesPlannedDefinition(ctx, target, table)
		if err != nil {
			return 0, err
		}
	}
	var copied int
	if key, ok := integerPrimaryKey(table); ok {
		copied, err = copyIntegerKeyset(ctx, source, target, table, columns, key, mode, observer, progress)
	} else {
		copied, err = copyOrderedRows(ctx, source, target, table, columns, mode, observer, progress)
	}
	if err != nil {
		return 0, err
	}
	if created || finalizeExisting {
		if err := finalizeSQLiteTarget(ctx, target, table, observer); err != nil {
			return 0, err
		}
	}
	return copied, nil
}

func copyIntegerKeyset(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, key string, mode string, observer TableObserver, progress TableProgress) (int, error) {
	keyIndex := columnIndex(columns, key)
	if keyIndex == -1 {
		return 0, fmt.Errorf("integer primary key %s is not a selected column", key)
	}

	var lowerBound int64
	hasLowerBound := progress.IntegerWatermark != nil
	if hasLowerBound {
		lowerBound = *progress.IntegerWatermark
	}
	count := 0
	for {
		query := "SELECT " + quotedColumns(columns) + " FROM " + quote(table.Name)
		arguments := make([]any, 0, 2)
		if hasLowerBound {
			query += " WHERE " + quote(key) + " > ?"
			arguments = append(arguments, lowerBound)
		}
		query += " ORDER BY " + quote(key) + " LIMIT ?"
		arguments = append(arguments, sqliteWriteBatchSize)

		rows, err := source.QueryContext(ctx, query, arguments...)
		if err != nil {
			return 0, fmt.Errorf("read %s keyset page: %w", table.Name, err)
		}
		batch, lastKey, err := scanPage(rows, len(columns), keyIndex)
		rows.Close()
		if err != nil {
			return 0, fmt.Errorf("read %s keyset page: %w", table.Name, err)
		}
		if len(batch) == 0 {
			return count, nil
		}
		if err := writeBatchWithObserver(ctx, target, table, columns, mode, batch, observer); err != nil {
			return 0, err
		}
		count += len(batch)
		if pageObserver, ok := observer.(PageObserver); ok {
			if err := pageObserver.AfterIntegerKeysetPage(ctx, table.Name, progress.RowsDone+count, lastKey); err != nil {
				return 0, fmt.Errorf("checkpoint page for %s: %w", table.Name, err)
			}
			if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryPageCheckpointed, table.Name); err != nil {
				return 0, err
			}
		}
		lowerBound = lastKey
		hasLowerBound = true
	}
}

func copyOrderedRows(ctx context.Context, source, target *sql.DB, table schema.Table, columns []string, mode string, observer TableObserver, progress TableProgress) (int, error) {
	count := 0
	lowerRow := int64(0)
	if progress.RowNumberWatermark != nil {
		lowerRow = *progress.RowNumberWatermark
	}
	for {
		batch, err := readRowNumberPage(ctx, source, table, columns, lowerRow)
		if err != nil {
			return 0, err
		}
		if len(batch) == 0 {
			return count, nil
		}
		if err := writeBatchWithObserver(ctx, target, table, columns, mode, batch, observer); err != nil {
			return 0, err
		}
		count += len(batch)
		lowerRow += int64(len(batch))
		if pageObserver, ok := observer.(PageObserver); ok {
			if err := pageObserver.AfterRowNumberPage(ctx, table.Name, progress.RowsDone+count, lowerRow); err != nil {
				return 0, fmt.Errorf("checkpoint page for %s: %w", table.Name, err)
			}
			if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryPageCheckpointed, table.Name); err != nil {
				return 0, err
			}
		}
	}
}

func readRowNumberPage(ctx context.Context, source *sql.DB, table schema.Table, columns []string, lowerRow int64) ([][]any, error) {
	order := quotedColumns(primaryKeyColumns(table))
	query := "SELECT " + quotedColumns(columns) + " FROM (SELECT " + quotedColumns(columns) + ", ROW_NUMBER() OVER (ORDER BY " + order + ") AS dmtx_row_number FROM " + quote(table.Name) + ") WHERE dmtx_row_number > ? AND dmtx_row_number <= ? ORDER BY dmtx_row_number"
	rows, err := source.QueryContext(ctx, query, lowerRow, lowerRow+sqliteWriteBatchSize)
	if err != nil {
		return nil, fmt.Errorf("read %s row-number page: %w", table.Name, err)
	}
	defer rows.Close()
	batch, _, err := scanPage(rows, len(columns), -1)
	if err != nil {
		return nil, fmt.Errorf("read %s row-number page: %w", table.Name, err)
	}
	return batch, nil
}

func scanPage(rows *sql.Rows, columnCount, keyIndex int) ([][]any, int64, error) {
	values, pointers := make([]any, columnCount), make([]any, columnCount)
	for index := range values {
		pointers[index] = &values[index]
	}
	batch := make([][]any, 0, sqliteWriteBatchSize)
	var lastKey int64
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return nil, 0, err
		}
		var key int64
		if keyIndex >= 0 {
			var conversionErr error
			key, conversionErr = sqliteIntegerValue(values[keyIndex])
			if conversionErr != nil {
				return nil, 0, conversionErr
			}
		}
		rowValues := make([]any, len(values))
		for index, value := range values {
			rowValues[index] = cloneSQLiteValue(value)
		}
		batch = append(batch, rowValues)
		lastKey = key
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return batch, lastKey, nil
}

func cloneSQLiteValue(value any) any {
	bytes, ok := value.([]byte)
	if !ok {
		return value
	}
	cloned := make([]byte, len(bytes))
	copy(cloned, bytes)
	return cloned
}

func integerPrimaryKey(table schema.Table) (string, bool) {
	keys := primaryKeyColumns(table)
	if len(keys) != 1 {
		return "", false
	}
	for _, column := range table.Columns {
		if column.Name == keys[0] && strings.Contains(strings.ToUpper(column.Type), "INT") {
			return column.Name, true
		}
	}
	return "", false
}

func columnIndex(columns []string, name string) int {
	for index, column := range columns {
		if column == name {
			return index
		}
	}
	return -1
}

func sqliteIntegerValue(value any) (int64, error) {
	switch number := value.(type) {
	case int64:
		return number, nil
	case int:
		return int64(number), nil
	case int32:
		return int64(number), nil
	case uint64:
		if number > math.MaxInt64 {
			return 0, fmt.Errorf("integer primary key exceeds signed 64-bit range")
		}
		return int64(number), nil
	default:
		return 0, fmt.Errorf("integer primary key has unexpected value type %T", value)
	}
}

type sqliteErrorCoder interface {
	Code() int
}

func sqliteRetryPolicy(maxRetries int) RetryPolicy {
	policy := DefaultRetryPolicy()
	policy.MaxRetries = maxRetries
	return policy
}

func sqliteDefinitelyNotCommittedError(err error) error {
	if err == nil || ClassifyTransferError(err) == ErrorClassCanceled {
		return err
	}
	var classified transferErrorClassifier
	if errors.As(err, &classified) {
		return err
	}
	var coded sqliteErrorCoder
	if errors.As(err, &coded) {
		// SQLite extended result codes retain the primary result in the low byte.
		switch coded.Code() & 0xff {
		case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
			return NewTransferError(ErrorClassTransient, err)
		}
	}
	return err
}

func retrySQLiteDefinitelyNotCommitted(
	ctx context.Context,
	policy RetryPolicy,
	operation RetryOperation,
) error {
	return RetryWithPolicy(ctx, policy, func(ctx context.Context, attempt int) error {
		return sqliteDefinitelyNotCommittedError(operation(ctx, attempt))
	})
}

func querySQLiteRowsWithRetry(
	ctx context.Context,
	database *sql.DB,
	policy RetryPolicy,
	query string,
	arguments ...any,
) (*sql.Rows, error) {
	var rows *sql.Rows
	err := retrySQLiteDefinitelyNotCommitted(ctx, policy, func(ctx context.Context, _ int) error {
		result, queryErr := database.QueryContext(ctx, query, arguments...)
		if queryErr != nil {
			return queryErr
		}
		rows = result
		return nil
	})
	return rows, err
}

type sqliteWriteAttempt func(context.Context, int) (WriteReceipt, error)

func retrySQLiteWriteAttempts(
	ctx context.Context,
	policy RetryPolicy,
	operation sqliteWriteAttempt,
) (WriteReceipt, error) {
	if operation == nil {
		return WriteReceipt{}, ErrNilRetryOperation
	}
	var receipt WriteReceipt
	err := RetryWithPolicy(ctx, policy, func(ctx context.Context, attempt int) error {
		current, attemptErr := operation(ctx, attempt)
		receipt = current
		if err := current.Validate(); err != nil {
			return NewTransferError(ErrorClassState, fmt.Errorf("invalid SQLite write attempt receipt: %w", err))
		}
		if attemptErr == nil {
			if current.Certainty != CommitDurable {
				return NewTransferError(ErrorClassState, fmt.Errorf(
					"successful SQLite write reported commit certainty %s", current.Certainty,
				))
			}
			return nil
		}
		switch current.Certainty {
		case CommitNotCommitted:
			return sqliteDefinitelyNotCommittedError(attemptErr)
		case CommitUnknown:
			return NewTransferError(ErrorClassState, fmt.Errorf(
				"SQLite write commit outcome is unknown; refusing retry: %w", attemptErr,
			))
		default:
			return NewTransferError(ErrorClassState, fmt.Errorf(
				"SQLite write failed after reporting commit certainty %s; refusing retry: %w",
				current.Certainty, attemptErr,
			))
		}
	})
	return receipt, err
}

func writeBatchWithObserver(ctx context.Context, target *sql.DB, table schema.Table, columns []string, mode string, rows [][]any, observer TableObserver) error {
	_, err := writeSQLiteBatchReceipt(ctx, target, table, columns, mode, rows, observer, nil)
	return err
}

func writeSQLiteBatchReceipt(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	observer TableObserver,
	observerMu *sync.Mutex,
) (WriteReceipt, error) {
	return writeSQLiteBatchReceiptWithPolicy(
		ctx, target, table, columns, mode, rows, observer, observerMu,
		RetryPolicy{MaxRetries: 0},
	)
}

func writeSQLiteBatchReceiptWithPolicy(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	observer TableObserver,
	observerMu *sync.Mutex,
	policy RetryPolicy,
) (WriteReceipt, error) {
	return writeSQLiteBatchReceiptWithRangePolicy(
		ctx, target, table, columns, mode, rows, nil, observer, observerMu, policy,
	)
}

func writeSQLiteRangeBatchReceiptWithPolicy(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	chunk SQLiteRangeChunk,
	observer TableObserver,
	observerMu *sync.Mutex,
	policy RetryPolicy,
) (WriteReceipt, error) {
	cloned := cloneSQLiteRangeChunk(chunk)
	return writeSQLiteBatchReceiptWithRangePolicy(
		ctx, target, table, columns, mode, rows, &cloned, observer, observerMu, policy,
	)
}

func writeSQLiteBatchReceiptWithRangePolicy(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	chunk *SQLiteRangeChunk,
	observer TableObserver,
	observerMu *sync.Mutex,
	policy RetryPolicy,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	zero := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	receipt, err := retrySQLiteWriteAttempts(ctx, policy, func(ctx context.Context, _ int) (WriteReceipt, error) {
		current := zero
		if chunk != nil {
			if err := notifySQLiteRangeAttempt(ctx, observer, observerMu, *chunk); err != nil {
				return zero, err
			}
		}
		mutationCalled := false
		guardErr := protectSQLiteTargetMutation(ctx, observer, func() error {
			mutationCalled = true
			var attemptErr error
			current, attemptErr = writeSQLiteTransactionAttempt(ctx, target, table, columns, mode, rows)
			return attemptErr
		})
		if guardErr == nil {
			return current, nil
		}
		if !mutationCalled {
			return zero, guardErr
		}
		return current, guardErr
	})
	if err != nil {
		return receipt, err
	}
	if observerMu != nil {
		observerMu.Lock()
		defer observerMu.Unlock()
	}
	if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryPageCommitted, table.Name); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func notifySQLiteRangeAttempt(
	ctx context.Context,
	observer TableObserver,
	observerMu *sync.Mutex,
	chunk SQLiteRangeChunk,
) error {
	attemptObserver, ok := observer.(SQLiteRangeAttemptObserver)
	if !ok {
		return nil
	}
	if observerMu != nil {
		observerMu.Lock()
		defer observerMu.Unlock()
	}
	if err := attemptObserver.BeforeSQLiteRangeAttempt(
		ctx, cloneSQLiteRangeChunk(chunk),
	); err != nil {
		return NewTransferError(ErrorClassState, fmt.Errorf(
			"record SQLite range attempt %s range %d sequence %d: %w",
			chunk.Table, chunk.Range.ID, chunk.Sequence, err,
		))
	}
	return nil
}

func writeSQLiteTransactionAttempt(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{Certainty: CommitNotCommitted, AttemptedRows: attempted}
	unknown := WriteReceipt{Certainty: CommitUnknown, AttemptedRows: attempted}
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return notCommitted, fmt.Errorf("begin write for %s: %w", table.Name, err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, writeStatement(table, columns, mode))
	if err != nil {
		return notCommitted, fmt.Errorf("prepare write for %s: %w", table.Name, err)
	}
	defer statement.Close()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return notCommitted, fmt.Errorf("write %s: %w", table.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return unknown, fmt.Errorf("commit %s: %w", table.Name, err)
	}
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: attempted,
		CommittedRows: attempted,
	}, nil
}

func writeStatement(table schema.Table, columns []string, mode string) string {
	statement := "INSERT INTO " + quote(table.Name) + " (" + quotedColumns(columns) + ") VALUES (" + placeholders(len(columns)) + ")"
	if mode == sqliteInsertOnlyReplayMode {
		return statement + " ON CONFLICT (" + quotedColumns(primaryKeyColumns(table)) + ") DO NOTHING"
	}
	if mode != "upsert" {
		return statement
	}
	keys := primaryKeyColumns(table)
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(updates, quote(column)+" = excluded."+quote(column))
		}
	}
	if len(updates) == 0 {
		return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO NOTHING"
	}
	return statement + " ON CONFLICT (" + quotedColumns(keys) + ") DO UPDATE SET " + strings.Join(updates, ", ")
}

func primaryKeyColumns(table schema.Table) []string {
	type orderedKey struct {
		name     string
		position int
	}
	keys := make([]orderedKey, 0)
	fallback := len(table.Columns) + 1
	for _, column := range table.Columns {
		if column.PrimaryKey {
			position := column.PrimaryKeyPosition
			if position == 0 {
				position = fallback
				fallback++
			}
			keys = append(keys, orderedKey{name: column.Name, position: position})
		}
	}
	sort.SliceStable(keys, func(left, right int) bool { return keys[left].position < keys[right].position })
	names := make([]string, len(keys))
	for index, key := range keys {
		names[index] = key.name
	}
	return names
}
func hasPrimaryKey(table schema.Table) bool { return len(primaryKeyColumns(table)) > 0 }
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func prepareTargetWithStatus(ctx context.Context, target *sql.DB, table schema.Table, mode string, observer TableObserver) (bool, error) {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.SQLite, table)
		if err != nil {
			return false, err
		}
		err = protectSQLiteTargetMutation(ctx, observer, func() error {
			_, mutationErr := target.ExecContext(ctx, drop)
			return mutationErr
		})
		if err != nil {
			return false, fmt.Errorf("drop %s: %w", table.Name, err)
		}
		if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryTableDropped, table.Name); err != nil {
			return false, err
		}
	}
	exists, err := tableExists(ctx, target, table.Name)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	ddl, err := schema.CreateTable(schema.SQLite, table)
	if err != nil {
		return false, fmt.Errorf("plan %s: %w", table.Name, err)
	}
	err = protectSQLiteTargetMutation(ctx, observer, func() error {
		_, mutationErr := target.ExecContext(ctx, ddl)
		return mutationErr
	})
	if err != nil {
		return false, fmt.Errorf("create %s: %w", table.Name, err)
	}
	if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryTableCreated, table.Name); err != nil {
		return false, err
	}
	return true, nil
}

func finalizeSQLiteTarget(ctx context.Context, target *sql.DB, table schema.Table, observer TableObserver) error {
	indexes, err := schema.CreateIndexes(schema.SQLite, table)
	if err != nil {
		return fmt.Errorf("plan indexes for %s: %w", table.Name, err)
	}
	existing, err := inspectSQLiteIndexes(ctx, target, table.Name)
	if err != nil {
		return fmt.Errorf("inspect target indexes for %s: %w", table.Name, err)
	}
	byName := make(map[string]schema.Index, len(existing))
	for _, index := range existing {
		if !index.Inline {
			byName[index.Name] = index
		}
	}
	position := 0
	for _, index := range table.Indexes {
		if index.Inline {
			continue
		}
		statement := indexes[position]
		position++
		if targetIndex, ok := byName[index.Name]; ok {
			if !sameSQLiteIndex(index, targetIndex) {
				return &schema.PolicyError{Operation: "finalize SQLite index", Type: table.Name + "." + index.Name, Target: string(schema.SQLite)}
			}
			continue
		}
		err = protectSQLiteTargetMutation(ctx, observer, func() error {
			_, mutationErr := target.ExecContext(ctx, statement)
			return mutationErr
		})
		if err != nil {
			return fmt.Errorf("create index for %s: %w", table.Name, err)
		}
		if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryIndexCreated, table.Name); err != nil {
			return err
		}
	}
	return resetSQLiteSequence(ctx, target, table, observer)
}

func resetSQLiteSequence(ctx context.Context, target *sql.DB, table schema.Table, observer TableObserver) error {
	if table.Identity == nil || table.Identity.Frontier == nil {
		return nil
	}
	current, err := inspectSQLiteSequence(ctx, target, table.Name)
	if err != nil {
		return err
	}
	desired := *table.Identity.Frontier
	if current != nil && *current >= desired {
		return nil
	}
	plannedTable := table
	identity := *table.Identity
	identity.Frontier = &desired
	plannedTable.Identity = &identity
	plan, err := schema.SQLiteSequencePlan(plannedTable)
	if err != nil {
		return err
	}
	err = protectSQLiteTargetMutation(ctx, observer, func() error {
		tx, mutationErr := target.BeginTx(ctx, nil)
		if mutationErr != nil {
			return fmt.Errorf("begin sequence reset for %s: %w", table.Name, mutationErr)
		}
		defer tx.Rollback()
		for _, statement := range plan {
			if _, mutationErr := tx.ExecContext(ctx, statement.SQL, statement.Args...); mutationErr != nil {
				return fmt.Errorf("reset sequence for %s: %w", table.Name, mutationErr)
			}
		}
		if mutationErr := tx.Commit(); mutationErr != nil {
			return fmt.Errorf("commit sequence reset for %s: %w", table.Name, mutationErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundarySequenceCommitted, table.Name); err != nil {
		return err
	}
	return nil
}
func tableExists(ctx context.Context, database *sql.DB, name string) (bool, error) {
	var count int
	err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&count)
	return count > 0, err
}
func validateCount(ctx context.Context, source, target *sql.DB, name, mode string) error {
	sourceCount, err := countRows(ctx, source, name)
	if err != nil {
		return err
	}
	targetCount, err := countRows(ctx, target, name)
	if err != nil {
		return err
	}
	if !countsMatch(mode, sourceCount, targetCount) {
		return fmt.Errorf("validation failed for %s: source has %d rows, target has %d", name, sourceCount, targetCount)
	}
	return nil
}
func countRows(ctx context.Context, database *sql.DB, name string) (int, error) {
	var count int
	err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(name)).Scan(&count)
	return count, err
}
func inspectTable(ctx context.Context, database *sql.DB, name string) (schema.Table, []string, error) {
	if strings.EqualFold(name, sqliteDeleteJournalTable) {
		return schema.Table{}, nil, fmt.Errorf(
			"SQLite table %s is private DMTX delete receipt state and is not a migratable source table",
			sqliteDeleteJournalTable,
		)
	}
	return inspectSQLiteSchema(ctx, database, name)
}
func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func quotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quote(column)
	}
	return strings.Join(quoted, ", ")
}

func sqliteTableMatchesPlannedDefinition(ctx context.Context, target *sql.DB, table schema.Table) (bool, error) {
	existing, err := sqliteCreateTableSQL(ctx, target, table.Name)
	if err != nil {
		return false, fmt.Errorf("inspect resumable target table %s: %w", table.Name, err)
	}
	planned, err := schema.CreateTable(schema.SQLite, table)
	if err != nil {
		return false, fmt.Errorf("plan resumable target table %s: %w", table.Name, err)
	}
	normalize := func(statement string) string {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}
	return normalize(existing) == normalize(planned), nil
}
func placeholders(count int) string {
	values := make([]string, count)
	for i := range values {
		values[i] = "?"
	}
	return strings.Join(values, ", ")
}

func sameSQLiteIndex(left, right schema.Index) bool {
	if left.Name != right.Name || left.Unique != right.Unique || left.Inline != right.Inline || len(left.Columns) != len(right.Columns) {
		return false
	}
	for position := range left.Columns {
		leftColumn, rightColumn := left.Columns[position], right.Columns[position]
		if leftColumn.Name != rightColumn.Name || leftColumn.Descending != rightColumn.Descending || !strings.EqualFold(leftColumn.Collation, rightColumn.Collation) {
			return false
		}
	}
	return true
}
