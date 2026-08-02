package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

const mysqlStage4NetworkCleanupTimeout = 5 * time.Second

// mysqlBatchWriter is the target adapter's durable bounded-insert dependency.
// Production construction always installs mysqlNativeWriter.
type mysqlBatchWriter interface {
	WriteBatch(
		context.Context,
		schema.Table,
		[]string,
		string,
		[][]any,
	) (WriteReceipt, error)
}

type mysqlStage4NetworkBatchWriter interface {
	WriteStage4NetworkBatch(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) (WriteReceipt, error)
}

type mysqlStage4NetworkRebuildBatchWriter interface {
	WriteStage4NetworkRebuildBatch(
		context.Context,
		schema.Table,
		[]string,
		NetworkWriteMode,
		[][]any,
	) (WriteReceipt, error)
}

type mysqlTransactionProvider interface {
	Begin(context.Context) (mysqlBatchTransaction, error)
}

type mysqlStage4NetworkTransactionProvider interface {
	BeginStage4Network(
		context.Context,
	) (mysqlStage4NetworkBatchTransaction, error)
}

type mysqlBatchTransaction interface {
	Prepare(context.Context, string) (mysqlBatchStatement, error)
	Execute(context.Context, string) (int64, error)
	Count(context.Context, string) (int64, error)
	LocalInfileEnabled(context.Context) (bool, error)
	LoadLocalInfile(
		context.Context,
		mysqlLocalInfileRequest,
	) (int64, error)
	WarningCount(context.Context) (int64, error)
	Commit() error
	Rollback() error
}

// mysqlStage4NetworkBatchTransaction is the per-page transaction capability
// required by crash-resumable network writes. The fence and catalog proof must
// run on the same transaction that performs the upsert so target DDL cannot
// invalidate admission between the proof and the page mutation.
type mysqlStage4NetworkBatchTransaction interface {
	mysqlBatchTransaction
	AcquireStage4NetworkReplayFence(
		context.Context,
		schema.Table,
	) error
	PreflightStage4NetworkReplayIsolation(
		context.Context,
		schema.Table,
		engine.MySQLServerFlavor,
	) error
	RollbackStage4Network(context.Context) error
}

type mysqlBatchStatement interface {
	Exec(context.Context, []any) (int64, error)
	Close() error
}

// mysqlSafeOperationError retains a driver error for errors.Is/errors.As while
// keeping driver-provided text, which can contain values, out of operator
// output.
type mysqlSafeOperationError struct {
	operation string
	table     string
	cause     error
}

func (operationError *mysqlSafeOperationError) Error() string {
	return operationError.operation + " " + operationError.table
}

func (operationError *mysqlSafeOperationError) Unwrap() error {
	return operationError.cause
}

func newMySQLSafeOperationError(
	operation string,
	table string,
	cause error,
) error {
	var safeError *mysqlSafeOperationError
	if errors.As(cause, &safeError) {
		return safeError
	}
	return &mysqlSafeOperationError{
		operation: operation,
		table:     table,
		cause:     cause,
	}
}

type mysqlNativeWriter struct {
	transactions mysqlTransactionProvider
	flavor       engine.MySQLServerFlavor
	mu           sync.Mutex
	localInfile  mysqlLocalInfileState
	warn         func(string)
}

func newMySQLNativeWriter(database *sql.DB) *mysqlNativeWriter {
	return newMySQLNativeWriterForFlavor(
		database,
		engine.MySQLServerFlavorOracle80,
	)
}

func newMySQLNativeWriterForFlavor(
	database *sql.DB,
	flavor engine.MySQLServerFlavor,
) *mysqlNativeWriter {
	return &mysqlNativeWriter{
		transactions: mysqlSQLTransactionProvider{database: database},
		flavor:       flavor,
		warn:         defaultMySQLNativeWriterWarning,
	}
}

func (writer *mysqlNativeWriter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	if err := validateMySQLWriteShape(table, columns, mode); err != nil {
		return notCommitted, err
	}
	for index, row := range rows {
		if len(row) != len(columns) {
			return notCommitted, fmt.Errorf(
				"write MySQL table %s: row %d has %d values for %d columns",
				table.Name,
				index,
				len(row),
				len(columns),
			)
		}
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if writer == nil || writer.transactions == nil {
		return notCommitted, fmt.Errorf(
			"write MySQL table %s: transaction provider is not configured",
			table.Name,
		)
	}
	writeStatement, err := mySQLNativeWriteStatementForFlavor(
		table,
		columns,
		mode,
		writer.flavor,
	)
	if err != nil {
		return notCommitted, err
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if mode == "upsert" &&
		writer.localInfile != mysqlLocalInfileFallback {
		writer.useMySQLStrictInsertFallback(
			mysqlLocalInfileUpsertFallbackWarning,
		)
	}
	if mode == "drop_recreate" &&
		writer.localInfile != mysqlLocalInfileFallback {
		localRows, normalizeErr := normalizeMySQLLocalInfileRows(rows)
		if normalizeErr != nil {
			return notCommitted, fmt.Errorf(
				"prepare MySQL native bulk data for table %s: %w",
				table.Name,
				normalizeErr,
			)
		}
		receipt, fallback, bulkErr := writer.writeMySQLLocalInfileBatch(
			ctx,
			table,
			columns,
			localRows,
		)
		if bulkErr != nil || !fallback {
			return receipt, bulkErr
		}
	}

	return writer.writeMySQLStrictBatch(
		ctx,
		table,
		mode,
		rows,
		writeStatement,
	)
}

// WriteStage4NetworkBatch writes one replayable network page. It deliberately
// bypasses LOAD DATA even for a newly-created target: only the strict,
// primary-key-guarded upsert path has the idempotence and receipt semantics
// required by durable range replay.
func (writer *mysqlNativeWriter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	if err := validateMySQLWriteShape(
		table,
		columns,
		"upsert",
	); err != nil {
		return notCommitted, err
	}
	for index, row := range rows {
		if len(row) != len(columns) {
			return notCommitted, fmt.Errorf(
				"write MySQL table %s: row %d has %d values for %d columns",
				table.Name,
				index,
				len(row),
				len(columns),
			)
		}
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if writer == nil || writer.transactions == nil {
		return notCommitted, fmt.Errorf(
			"write MySQL table %s: transaction provider is not configured",
			table.Name,
		)
	}
	writeStatement, err := mySQLNativeWriteStatementForFlavor(
		table,
		columns,
		"upsert",
		writer.flavor,
	)
	if err != nil {
		return notCommitted, err
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writeMySQLStrictBatchWithGuard(
		ctx,
		table,
		"upsert",
		rows,
		writeStatement,
		writer.stage4NetworkGuard(table),
	)
}

// WriteStage4NetworkRebuildBatch keeps the initial empty-target insert strict
// while allowing an issued page to be replayed without updating any row. The
// replay statement is scoped to a complete primary-key match; a collision on
// another UNIQUE key deliberately fails instead of being ignored or updated.
func (writer *mysqlNativeWriter) WriteStage4NetworkRebuildBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	if mode != NetworkWriteFreshInsert &&
		mode != NetworkWriteDuplicateSafeInsertOnly {
		return notCommitted, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"MySQL Stage 4 rebuild writer received unsupported mode %q",
				mode,
			),
		)
	}
	// Every page can become an issued replay, so admission to either mode
	// requires the complete primary key even though the fresh SQL is strict.
	if err := validateMySQLWriteShape(table, columns, "upsert"); err != nil {
		return notCommitted, err
	}
	for index, row := range rows {
		if len(row) != len(columns) {
			return notCommitted, fmt.Errorf(
				"write MySQL table %s: row %d has %d values for %d columns",
				table.Name,
				index,
				len(row),
				len(columns),
			)
		}
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if writer == nil || writer.transactions == nil {
		return notCommitted, fmt.Errorf(
			"write MySQL table %s: transaction provider is not configured",
			table.Name,
		)
	}

	affectedMode := "drop_recreate"
	writeStatement, err := mySQLNativeWriteStatementForFlavor(
		table,
		columns,
		"drop_recreate",
		writer.flavor,
	)
	if mode == NetworkWriteDuplicateSafeInsertOnly {
		affectedMode = "upsert"
		writeStatement, err = mySQLNativeInsertOnlyStatementForFlavor(
			table,
			columns,
			writer.flavor,
		)
	}
	if err != nil {
		return notCommitted, err
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writeMySQLStrictBatchWithGuard(
		ctx,
		table,
		affectedMode,
		rows,
		writeStatement,
		writer.stage4RebuildNetworkGuard(table),
	)
}

func (writer *mysqlNativeWriter) stage4NetworkGuard(
	table schema.Table,
) func(context.Context, mysqlBatchTransaction) error {
	return writer.stage4NetworkGuardForProof(table, table)
}

// stage4RebuildNetworkGuard keeps the page transaction's fence on the full
// immutable table identity while proving the load-time catalog shape. The
// final table's secondary objects do not exist until the set-wide finalizer;
// requiring them here would reject every correctly prepared FK graph before
// its first transfer page.
func (writer *mysqlNativeWriter) stage4RebuildNetworkGuard(
	table schema.Table,
) func(context.Context, mysqlBatchTransaction) error {
	return writer.stage4NetworkGuardForProof(
		table,
		stage4RebuildPreFinalizeTable(table),
	)
}

func (writer *mysqlNativeWriter) stage4NetworkGuardForProof(
	fenceTable schema.Table,
	proofTable schema.Table,
) func(context.Context, mysqlBatchTransaction) error {
	return func(
		ctx context.Context,
		transaction mysqlBatchTransaction,
	) error {
		stage4Transaction, ok :=
			transaction.(mysqlStage4NetworkBatchTransaction)
		if !ok || isNilInterface(stage4Transaction) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"write MySQL Stage 4 network table %s: transaction has no replay-isolation fence",
					fenceTable.Name,
				),
			)
		}
		if err := stage4Transaction.AcquireStage4NetworkReplayFence(
			ctx,
			fenceTable,
		); err != nil {
			return newMySQLSafeOperationError(
				"acquire MySQL Stage 4 network replay fence for",
				fenceTable.Name,
				err,
			)
		}
		if err := stage4Transaction.PreflightStage4NetworkReplayIsolation(
			ctx,
			proofTable,
			writer.flavor,
		); err != nil {
			return fmt.Errorf(
				"prove MySQL Stage 4 network replay isolation for table %s: %w",
				fenceTable.Name,
				err,
			)
		}
		return nil
	}
}

func (writer *mysqlNativeWriter) writeMySQLStrictBatch(
	ctx context.Context,
	table schema.Table,
	mode string,
	rows [][]any,
	writeStatement string,
) (WriteReceipt, error) {
	return writer.writeMySQLStrictBatchWithGuard(
		ctx,
		table,
		mode,
		rows,
		writeStatement,
		nil,
	)
}

func (writer *mysqlNativeWriter) writeMySQLStrictBatchWithGuard(
	ctx context.Context,
	table schema.Table,
	mode string,
	rows [][]any,
	writeStatement string,
	guard func(context.Context, mysqlBatchTransaction) error,
) (receipt WriteReceipt, operationError error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	var transaction mysqlBatchTransaction
	var err error
	if guard == nil {
		transaction, err = writer.transactions.Begin(ctx)
	} else {
		stage4Provider, ok :=
			writer.transactions.(mysqlStage4NetworkTransactionProvider)
		if !ok || isNilInterface(stage4Provider) {
			return notCommitted, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"write MySQL Stage 4 network table %s: transaction provider has no pinned replay transaction",
					table.Name,
				),
			)
		}
		transaction, err = stage4Provider.BeginStage4Network(ctx)
	}
	if err != nil {
		return notCommitted, newMySQLSafeOperationError(
			"begin MySQL write for",
			table.Name,
			err,
		)
	}
	committed := false
	writeMayHaveChangedTarget := false
	defer func() {
		if committed {
			return
		}
		if guard == nil {
			_ = transaction.Rollback()
			return
		}
		stage4Transaction, ok :=
			transaction.(mysqlStage4NetworkBatchTransaction)
		if !ok || isNilInterface(stage4Transaction) {
			operationError = errors.Join(
				operationError,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"clean up MySQL Stage 4 network table %s: transaction has no bounded rollback",
						table.Name,
					),
				),
			)
			if writeMayHaveChangedTarget {
				receipt.Certainty = CommitUnknown
			}
			return
		}
		cleanupContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			mysqlStage4NetworkCleanupTimeout,
		)
		defer cancel()
		if rollbackErr := stage4Transaction.RollbackStage4Network(
			cleanupContext,
		); rollbackErr != nil &&
			!errors.Is(rollbackErr, sql.ErrTxDone) {
			if writeMayHaveChangedTarget {
				receipt.Certainty = CommitUnknown
			}
			operationError = errors.Join(
				operationError,
				newMySQLSafeOperationError(
					"roll back MySQL Stage 4 network write for",
					table.Name,
					rollbackErr,
				),
			)
		}
	}()

	if guard != nil {
		if err := guard(ctx, transaction); err != nil {
			return notCommitted, err
		}
	}

	statement, err := transaction.Prepare(
		ctx,
		writeStatement,
	)
	if err != nil {
		return notCommitted, newMySQLSafeOperationError(
			"prepare MySQL write for",
			table.Name,
			err,
		)
	}
	statementClosed := false
	defer func() {
		if !statementClosed {
			_ = statement.Close()
		}
	}()

	for _, row := range rows {
		// The server can apply the DML and lose the response. From this point a
		// failed rollback makes the target state unknown.
		writeMayHaveChangedTarget = true
		affected, err := statement.Exec(ctx, row)
		if err != nil {
			return notCommitted, newMySQLSafeOperationError(
				"write MySQL table",
				table.Name,
				err,
			)
		}
		if err := validateMySQLAffectedRows(mode, affected); err != nil {
			return notCommitted, fmt.Errorf(
				"write MySQL table %s: %w",
				table.Name,
				err,
			)
		}
		warnings, err := transaction.WarningCount(ctx)
		if err != nil {
			return notCommitted, newMySQLSafeOperationError(
				"inspect MySQL write warnings for",
				table.Name,
				err,
			)
		}
		if warnings != 0 {
			return notCommitted, fmt.Errorf(
				"write MySQL table %s produced %d conversion warnings",
				table.Name,
				warnings,
			)
		}
	}
	if err := statement.Close(); err != nil {
		return notCommitted, newMySQLSafeOperationError(
			"close MySQL write statement for",
			table.Name,
			err,
		)
	}
	statementClosed = true

	if err := transaction.Commit(); err != nil {
		return WriteReceipt{
				Certainty:     CommitUnknown,
				AttemptedRows: attempted,
			}, newMySQLSafeOperationError(
				"commit MySQL table",
				table.Name,
				err,
			)
	}
	committed = true
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: attempted,
		CommittedRows: attempted,
	}, nil
}

func validateMySQLWriteShape(
	table schema.Table,
	columns []string,
	mode string,
) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf(
			"MySQL target database and table name are required",
		)
	}
	if mode != "drop_recreate" && mode != "upsert" {
		return fmt.Errorf(
			"write MySQL table %s: unsupported target mode %q",
			table.Name,
			mode,
		)
	}
	if len(columns) == 0 {
		return fmt.Errorf(
			"write MySQL table %s: at least one column is required",
			table.Name,
		)
	}

	metadata := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return fmt.Errorf(
				"write MySQL table %s: schema contains an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return fmt.Errorf(
				"write MySQL table %s: schema contains duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = struct{}{}
	}

	requested := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf(
				"write MySQL table %s: requested column name is empty",
				table.Name,
			)
		}
		if _, duplicate := requested[column]; duplicate {
			return fmt.Errorf(
				"write MySQL table %s: requested column %s is duplicated",
				table.Name,
				column,
			)
		}
		if _, exists := metadata[column]; !exists {
			return fmt.Errorf(
				"write MySQL table %s: requested column %s is not present in schema",
				table.Name,
				column,
			)
		}
		requested[column] = struct{}{}
	}

	if mode != "upsert" {
		return nil
	}
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return fmt.Errorf(
			"table %s has no primary key; MySQL upsert requires a primary key",
			table.Name,
		)
	}
	for _, key := range keys {
		if _, included := requested[key]; !included {
			return fmt.Errorf(
				"write MySQL table %s: upsert primary-key column %s is not included",
				table.Name,
				key,
			)
		}
	}
	return nil
}

func validateMySQLAffectedRows(mode string, affected int64) error {
	if mode == "drop_recreate" && affected != 1 {
		return fmt.Errorf(
			"insert affected %d rows; expected exactly 1",
			affected,
		)
	}
	if mode == "upsert" && (affected < 0 || affected > 2) {
		return fmt.Errorf(
			"upsert affected %d rows; expected 0, 1, or 2",
			affected,
		)
	}
	return nil
}

func mySQLNativeWriteStatement(
	table schema.Table,
	columns []string,
	mode string,
) string {
	statement := "INSERT INTO " +
		mySQLQualified(table.Schema, table.Name) +
		" (" + mySQLQuotedColumns(columns) + ") VALUES (" +
		placeholders(len(columns)) + ")"
	if mode != "upsert" {
		return statement
	}

	keys := primaryKeyColumns(table)
	incoming := "dmtx_new"
	if strings.EqualFold(table.Name, incoming) {
		incoming = "dmtx_incoming"
	}
	keyMatches := make([]string, len(keys))
	for index, key := range keys {
		keyMatches[index] = mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(key) + " <=> " +
			mySQLIdentifier(incoming) + "." +
			mySQLIdentifier(key)
	}
	// MySQL's ON DUPLICATE KEY clause has no conflict target. Guard the first
	// assignment so a secondary UNIQUE collision on a different primary key
	// raises a deterministic expression error instead of updating the wrong
	// retained row. The false branch is not evaluated for the intended PK
	// conflict.
	guardKey := keys[0]
	updates := []string{
		mySQLIdentifier(guardKey) + " = IF(" +
			strings.Join(keyMatches, " AND ") + ", " +
			mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(guardKey) + ", " +
			"JSON_EXTRACT('dmtx-invalid-json', '$'))",
	}
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(
				updates,
				mySQLIdentifier(column)+" = "+
					mySQLIdentifier(incoming)+"."+
					mySQLIdentifier(column),
			)
		}
	}
	return statement + " AS " + mySQLIdentifier(incoming) +
		" ON DUPLICATE KEY UPDATE " +
		strings.Join(updates, ", ")
}

func mySQLNativeWriteStatementForFlavor(
	table schema.Table,
	columns []string,
	mode string,
	flavor engine.MySQLServerFlavor,
) (string, error) {
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return mySQLNativeWriteStatement(table, columns, mode), nil
	case engine.MySQLServerFlavorMariaDB1011:
		return mySQLNativeMariaDBWriteStatement(
			table,
			columns,
			mode,
		), nil
	default:
		return "", fmt.Errorf(
			"write MySQL table %s: unsupported target server flavor",
			table.Name,
		)
	}
}

func mySQLNativeMariaDBWriteStatement(
	table schema.Table,
	columns []string,
	mode string,
) string {
	statement := "INSERT INTO " +
		mySQLQualified(table.Schema, table.Name) +
		" (" + mySQLQuotedColumns(columns) + ") VALUES (" +
		placeholders(len(columns)) + ")"
	if mode != "upsert" {
		return statement
	}

	keys := primaryKeyColumns(table)
	keyMatches := make([]string, len(keys))
	for index, key := range keys {
		keyMatches[index] = mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(key) + " <=> VALUES(" +
			mySQLIdentifier(key) + ")"
	}
	// MariaDB 10.11 does not accept Oracle MySQL's row-alias syntax after
	// VALUES. Its VALUES(column) form still lets the first assignment prove
	// that the duplicate row matched the complete primary key. A collision
	// through another UNIQUE key evaluates the invalid JSON branch; assigning
	// its NULL result to the NOT NULL primary key fails or warns, and the
	// writer rolls the whole transaction back in either case.
	guardKey := keys[0]
	updates := []string{
		mySQLIdentifier(guardKey) + " = IF(" +
			strings.Join(keyMatches, " AND ") + ", " +
			mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(guardKey) + ", " +
			"JSON_EXTRACT('dmtx-invalid-json', '$'))",
	}
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(
				updates,
				mySQLIdentifier(column)+" = VALUES("+
					mySQLIdentifier(column)+")",
			)
		}
	}
	return statement + " ON DUPLICATE KEY UPDATE " +
		strings.Join(updates, ", ")
}

func mySQLNativeInsertOnlyStatementForFlavor(
	table schema.Table,
	columns []string,
	flavor engine.MySQLServerFlavor,
) (string, error) {
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return mySQLNativeInsertOnlyStatement(table, columns), nil
	case engine.MySQLServerFlavorMariaDB1011:
		return mySQLNativeMariaDBInsertOnlyStatement(
			table,
			columns,
		), nil
	default:
		return "", fmt.Errorf(
			"write MySQL table %s: unsupported target server flavor",
			table.Name,
		)
	}
}

func mySQLNativeInsertOnlyStatement(
	table schema.Table,
	columns []string,
) string {
	statement := "INSERT INTO " +
		mySQLQualified(table.Schema, table.Name) +
		" (" + mySQLQuotedColumns(columns) + ") VALUES (" +
		placeholders(len(columns)) + ")"
	keys := primaryKeyColumns(table)
	incoming := "dmtx_new"
	if strings.EqualFold(table.Name, incoming) {
		incoming = "dmtx_incoming"
	}
	keyMatches := make([]string, len(keys))
	for index, key := range keys {
		keyMatches[index] = mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(key) + " <=> " +
			mySQLIdentifier(incoming) + "." +
			mySQLIdentifier(key)
	}
	guardKey := keys[0]
	return statement + " AS " + mySQLIdentifier(incoming) +
		" ON DUPLICATE KEY UPDATE " +
		mySQLIdentifier(guardKey) + " = IF(" +
		strings.Join(keyMatches, " AND ") + ", " +
		mySQLIdentifier(table.Name) + "." +
		mySQLIdentifier(guardKey) + ", " +
		"JSON_EXTRACT('dmtx-invalid-json', '$'))"
}

func mySQLNativeMariaDBInsertOnlyStatement(
	table schema.Table,
	columns []string,
) string {
	statement := "INSERT INTO " +
		mySQLQualified(table.Schema, table.Name) +
		" (" + mySQLQuotedColumns(columns) + ") VALUES (" +
		placeholders(len(columns)) + ")"
	keys := primaryKeyColumns(table)
	keyMatches := make([]string, len(keys))
	for index, key := range keys {
		keyMatches[index] = mySQLIdentifier(table.Name) + "." +
			mySQLIdentifier(key) + " <=> VALUES(" +
			mySQLIdentifier(key) + ")"
	}
	guardKey := keys[0]
	return statement + " ON DUPLICATE KEY UPDATE " +
		mySQLIdentifier(guardKey) + " = IF(" +
		strings.Join(keyMatches, " AND ") + ", " +
		mySQLIdentifier(table.Name) + "." +
		mySQLIdentifier(guardKey) + ", " +
		"JSON_EXTRACT('dmtx-invalid-json', '$'))"
}

type mysqlSQLTransactionProvider struct {
	database *sql.DB
}

func (provider mysqlSQLTransactionProvider) Begin(
	ctx context.Context,
) (mysqlBatchTransaction, error) {
	transaction, err := provider.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return mysqlSQLBatchTransaction{transaction: transaction}, nil
}

func (provider mysqlSQLTransactionProvider) BeginStage4Network(
	ctx context.Context,
) (mysqlStage4NetworkBatchTransaction, error) {
	if provider.database == nil {
		return nil, fmt.Errorf("MySQL database is not configured")
	}
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return nil, err
	}
	transaction, err := connection.BeginTx(ctx, nil)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	return &mysqlSQLStage4NetworkTransaction{
		mysqlSQLBatchTransaction: mysqlSQLBatchTransaction{
			transaction: transaction,
		},
		connection: connection,
	}, nil
}

type mysqlSQLBatchTransaction struct {
	transaction *sql.Tx
}

func (transaction mysqlSQLBatchTransaction) AcquireStage4NetworkReplayFence(
	ctx context.Context,
	table schema.Table,
) error {
	// A table reference inside an explicit MySQL-family transaction acquires a
	// shared metadata lock that is retained through commit/rollback. Reading
	// at most one constant avoids row materialization while preventing ALTER,
	// DROP, RENAME, and foreign-key DDL from crossing the catalog proof.
	rows, err := transaction.transaction.QueryContext(
		ctx,
		stage4MySQLNetworkReplayFenceStatement(table),
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		// At most one constant row is returned; no target value is observed.
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func (transaction mysqlSQLBatchTransaction) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
	flavor engine.MySQLServerFlavor,
) error {
	return preflightStage4MySQLNetworkReplayIsolation(
		ctx,
		transaction.transaction,
		[]schema.Table{table},
		flavor,
	)
}

func (transaction mysqlSQLBatchTransaction) Prepare(
	ctx context.Context,
	statement string,
) (mysqlBatchStatement, error) {
	prepared, err := transaction.transaction.PrepareContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	return mysqlSQLBatchStatement{statement: prepared}, nil
}

func (transaction mysqlSQLBatchTransaction) Execute(
	ctx context.Context,
	statement string,
) (int64, error) {
	result, err := transaction.transaction.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (transaction mysqlSQLBatchTransaction) Count(
	ctx context.Context,
	statement string,
) (int64, error) {
	var count int64
	err := transaction.transaction.QueryRowContext(ctx, statement).Scan(&count)
	return count, err
}

func (transaction mysqlSQLBatchTransaction) LocalInfileEnabled(
	ctx context.Context,
) (bool, error) {
	var value string
	if err := transaction.transaction.QueryRowContext(
		ctx,
		"SELECT @@GLOBAL.local_infile",
	).Scan(&value); err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "on":
		return true, nil
	case "0", "off":
		return false, nil
	default:
		return false, fmt.Errorf(
			"unexpected MySQL local_infile value",
		)
	}
}

func (transaction mysqlSQLBatchTransaction) WarningCount(
	ctx context.Context,
) (int64, error) {
	var warnings int64
	err := transaction.transaction.QueryRowContext(
		ctx,
		"SHOW COUNT(*) WARNINGS",
	).Scan(&warnings)
	return warnings, err
}

func (transaction mysqlSQLBatchTransaction) Commit() error {
	return transaction.transaction.Commit()
}

func (transaction mysqlSQLBatchTransaction) Rollback() error {
	return transaction.transaction.Rollback()
}

type mysqlSQLStage4NetworkTransaction struct {
	mysqlSQLBatchTransaction
	connection *sql.Conn
}

func (transaction *mysqlSQLStage4NetworkTransaction) Commit() error {
	commitErr := transaction.mysqlSQLBatchTransaction.Commit()
	if commitErr != nil {
		transaction.discard()
	}
	closeErr := transaction.connection.Close()
	return errors.Join(commitErr, closeErr)
}

func (transaction *mysqlSQLStage4NetworkTransaction) RollbackStage4Network(
	ctx context.Context,
) error {
	if ctx == nil {
		return fmt.Errorf(
			"MySQL Stage 4 rollback context is required",
		)
	}
	type rollbackResult struct {
		rollback error
		close    error
	}
	result := make(chan rollbackResult, 1)
	go func() {
		rollbackErr :=
			transaction.mysqlSQLBatchTransaction.Rollback()
		if rollbackErr != nil &&
			!errors.Is(rollbackErr, sql.ErrTxDone) {
			transaction.discard()
		}
		result <- rollbackResult{
			rollback: rollbackErr,
			close:    transaction.connection.Close(),
		}
	}()
	select {
	case completed := <-result:
		return errors.Join(completed.rollback, completed.close)
	case <-ctx.Done():
		// The pinned *sql.Conn remains unavailable to the pool. The cleanup
		// goroutine will discard it on a rollback failure and close it only
		// after rollback completes, so timeout never exposes a transaction
		// that might still be active to a later caller.
		return fmt.Errorf(
			"MySQL Stage 4 rollback did not complete before cleanup deadline: %w",
			ctx.Err(),
		)
	}
}

func (transaction *mysqlSQLStage4NetworkTransaction) discard() {
	if transaction == nil || transaction.connection == nil {
		return
	}
	_ = transaction.connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

type mysqlSQLBatchStatement struct {
	statement *sql.Stmt
}

func (statement mysqlSQLBatchStatement) Exec(
	ctx context.Context,
	values []any,
) (int64, error) {
	result, err := statement.statement.ExecContext(ctx, values...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (statement mysqlSQLBatchStatement) Close() error {
	return statement.statement.Close()
}
