package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

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

type mysqlTransactionProvider interface {
	Begin(context.Context) (mysqlBatchTransaction, error)
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

func (writer *mysqlNativeWriter) writeMySQLStrictBatch(
	ctx context.Context,
	table schema.Table,
	mode string,
	rows [][]any,
	writeStatement string,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	transaction, err := writer.transactions.Begin(ctx)
	if err != nil {
		return notCommitted, newMySQLSafeOperationError(
			"begin MySQL write for",
			table.Name,
			err,
		)
	}
	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

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

type mysqlSQLBatchTransaction struct {
	transaction *sql.Tx
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
