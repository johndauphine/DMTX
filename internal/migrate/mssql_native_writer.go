package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/johndauphine/dmtx/internal/schema"
)

const sqlServerIdentityCleanupTimeout = 5 * time.Second

const sqlServerRollbackWithoutBeginErrorNumber int32 = 3903

const sqlServerNativeSessionGuardStatement = `SET NOCOUNT OFF;
SET XACT_ABORT ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;`

// sqlServerBatchWriter is the SQL Server target adapter's durable bounded
// write dependency. Production construction always installs
// sqlServerNativeWriter.
type sqlServerBatchWriter interface {
	WriteBatch(
		context.Context,
		schema.Table,
		[]string,
		string,
		[][]any,
	) (WriteReceipt, error)
}

type sqlServerStage4NetworkBatchWriter interface {
	WriteStage4NetworkBatch(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) (WriteReceipt, error)
}

// sqlServerSafeOperationError preserves the driver cause for errors.Is and
// errors.As without exposing driver-provided text, which can contain row
// values or connection details, in operator output.
type sqlServerSafeOperationError struct {
	operation string
	table     string
	cause     error
}

func (operationError *sqlServerSafeOperationError) Error() string {
	return operationError.operation + " " + operationError.table
}

func (operationError *sqlServerSafeOperationError) Unwrap() error {
	return operationError.cause
}

func newSQLServerSafeOperationError(
	operation string,
	table string,
	cause error,
) error {
	var safeError *sqlServerSafeOperationError
	if errors.As(cause, &safeError) {
		return safeError
	}
	return &sqlServerSafeOperationError{
		operation: operation,
		table:     table,
		cause:     cause,
	}
}

// The narrow interfaces below keep connection affinity and transaction
// semantics explicit while allowing failure and cleanup behavior to be tested
// without a live server.
type sqlServerNativeConnectionProvider interface {
	WithConnection(
		context.Context,
		func(sqlServerNativeConnection) error,
	) error
}

type sqlServerNativeConnection interface {
	BeginSerializable(context.Context) (sqlServerNativeTransaction, error)
	Exec(context.Context, string) (int64, error)
	Discard()
}

type sqlServerNativeTransaction interface {
	Prepare(context.Context, string) (sqlServerNativeStatement, error)
	Exec(context.Context, string) (int64, error)
	Commit() error
	Rollback() error
}

// sqlServerStage4NetworkTransaction binds the schema-stability proof to the
// exact transaction that mutates a replayable page.
type sqlServerStage4NetworkTransaction interface {
	sqlServerNativeTransaction
	AcquireStage4NetworkReplayFence(
		context.Context,
		schema.Table,
	) error
	PreflightStage4NetworkReplayIsolation(
		context.Context,
		schema.Table,
	) error
	// RollbackStage4Network must quarantine the physical connection before it
	// returns a timeout or rollback error.
	RollbackStage4Network(context.Context) error
}

type sqlServerNativeStatement interface {
	Exec(context.Context, []any) (int64, error)
	QueryBool(context.Context, []any) (bool, error)
	Done(context.Context) (int64, error)
	Close() error
}

type sqlServerNativeWriter struct {
	connections sqlServerNativeConnectionProvider
}

func newSQLServerNativeWriter(database *sql.DB) *sqlServerNativeWriter {
	return &sqlServerNativeWriter{
		connections: sqlServerSQLConnectionProvider{database: database},
	}
}

func (writer *sqlServerNativeWriter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	return writer.writeBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
		false,
	)
}

// WriteStage4NetworkBatch forces the transactionally-fenced strict upsert
// path required for durable page replay.
func (writer *sqlServerNativeWriter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	return writer.writeBatch(
		ctx,
		table,
		columns,
		"upsert",
		rows,
		true,
	)
}

func (writer *sqlServerNativeWriter) writeBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	stage4Network bool,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	receipt := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	if err := validateSQLServerWriteShape(table, columns, mode); err != nil {
		return receipt, err
	}
	for index, row := range rows {
		if len(row) != len(columns) {
			return receipt, fmt.Errorf(
				"write SQL Server table %s: row %d has %d values for %d columns",
				table.Name,
				index,
				len(row),
				len(columns),
			)
		}
	}
	normalizedRows, err := normalizeSQLServerWriteRows(
		table,
		columns,
		rows,
	)
	if err != nil {
		return receipt, err
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if writer == nil || writer.connections == nil {
		return receipt, fmt.Errorf(
			"write SQL Server table %s: connection provider is not configured",
			table.Name,
		)
	}

	var callbackError error
	providerError := writer.connections.WithConnection(
		ctx,
		func(connection sqlServerNativeConnection) error {
			callbackError = writer.writeTransaction(
				ctx,
				connection,
				table,
				columns,
				mode,
				normalizedRows,
				&receipt,
				stage4Network,
			)
			return callbackError
		},
	)
	if callbackError != nil {
		return receipt, callbackError
	}
	if providerError != nil {
		return receipt, newSQLServerSafeOperationError(
			"acquire SQL Server native connection for",
			table.Name,
			providerError,
		)
	}
	return receipt, nil
}

// go-mssqldb encodes an untyped nil parameter as NVARCHAR NULL. SQL Server
// deliberately has no implicit NVARCHAR-to-VARBINARY conversion, even for a
// NULL value. Preserve source NULL semantics while pinning the parameter type
// with a nil []byte for binary target columns.
//
// MySQL-family drivers return exact DECIMAL and character values as []byte.
// Preserve binary values verbatim, but bind textual and decimal bytes as
// strings so both SQL Server bulk copy and prepared upserts use the planned
// character/numeric target type instead of VARBINARY. Invalid UTF-8 is rejected
// before a target connection or transaction is acquired.
func normalizeSQLServerWriteRows(
	table schema.Table,
	columns []string,
	rows [][]any,
) ([][]any, error) {
	binary := make([]bool, len(columns))
	stringBytes := make([]bool, len(columns))
	for index, name := range columns {
		column, exists := sqlServerTableColumn(table, name)
		if !exists {
			return nil, fmt.Errorf(
				"write SQL Server table %s: column %s is absent from schema",
				table.Name,
				name,
			)
		}
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "blob", "binary", "varbinary":
			binary[index] = true
		case "text", "char", "varchar", "numeric", "decimal":
			stringBytes[index] = true
		}
	}

	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex] = append([]any(nil), row...)
		for columnIndex := range columns {
			if binary[columnIndex] &&
				normalized[rowIndex][columnIndex] == nil {
				normalized[rowIndex][columnIndex] = []byte(nil)
				continue
			}
			if !stringBytes[columnIndex] {
				continue
			}
			value, ok := normalized[rowIndex][columnIndex].([]byte)
			if !ok {
				continue
			}
			if !utf8.Valid(value) {
				return nil, fmt.Errorf(
					"write SQL Server table %s: row %d column %s contains invalid UTF-8",
					table.Name,
					rowIndex,
					columns[columnIndex],
				)
			}
			normalized[rowIndex][columnIndex] = string(value)
		}
	}
	return normalized, nil
}

func (writer *sqlServerNativeWriter) writeTransaction(
	ctx context.Context,
	connection sqlServerNativeConnection,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	receipt *WriteReceipt,
	stage4Network bool,
) (operationError error) {
	transaction, err := connection.BeginSerializable(ctx)
	if err != nil {
		return newSQLServerSafeOperationError(
			"begin SQL Server native write for",
			table.Name,
			err,
		)
	}

	committed := false
	identityInsertEnabled := false
	identityFrontierMayHaveChanged := false
	writeMayHaveChangedTarget := false
	defer func() {
		rollbackUnsafe := false
		stage4ConnectionQuarantined := false
		if !committed {
			var rollbackError error
			if stage4Network {
				stage4Transaction, ok :=
					transaction.(sqlServerStage4NetworkTransaction)
				if !ok || isNilInterface(stage4Transaction) {
					rollbackError = NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"clean up SQL Server Stage 4 network table %s: transaction has no bounded rollback",
							table.Name,
						),
					)
					connection.Discard()
				} else {
					cleanupContext, cancel := context.WithTimeout(
						context.WithoutCancel(ctx),
						sqlServerIdentityCleanupTimeout,
					)
					rollbackError =
						stage4Transaction.RollbackStage4Network(
							cleanupContext,
						)
					cancel()
					stage4ConnectionQuarantined = true
				}
			} else {
				rollbackError = transaction.Rollback()
			}
			if rollbackError != nil &&
				!errors.Is(rollbackError, sql.ErrTxDone) {
				if !sqlServerRollbackAlreadyCompletedByXACTAbort(
					rollbackError,
				) {
					if stage4Network &&
						writeMayHaveChangedTarget {
						receipt.Certainty = CommitUnknown
					}
					if !stage4ConnectionQuarantined {
						connection.Discard()
					}
					rollbackUnsafe = true
					operationError = errors.Join(
						operationError,
						newSQLServerSafeOperationError(
							"roll back SQL Server native write for",
							table.Name,
							rollbackError,
						),
					)
				}
			}
		}
		if stage4ConnectionQuarantined {
			// The Stage 4 cleanup transaction owns and closes its detached
			// physical connection. Session-scoped IDENTITY_INSERT state
			// cannot escape through the pool, even after a timeout.
			return
		}
		if !identityInsertEnabled {
			return
		}
		if rollbackUnsafe {
			// The connection cannot be proven usable and has already been
			// excluded from the pool. Do not attempt another command on it.
			return
		}
		cleanupContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			sqlServerIdentityCleanupTimeout,
		)
		defer cancel()
		if _, cleanupError := connection.Exec(
			cleanupContext,
			sqlServerIdentityInsertStatement(table, false),
		); cleanupError != nil {
			connection.Discard()
			operationError = errors.Join(
				operationError,
				newSQLServerSafeOperationError(
					"disable SQL Server identity insert after",
					table.Name,
					cleanupError,
				),
			)
		}
	}()

	if _, err := transaction.Exec(
		ctx,
		sqlServerNativeSessionGuardStatement,
	); err != nil {
		return newSQLServerSafeOperationError(
			"configure SQL Server native write session for",
			table.Name,
			err,
		)
	}

	if stage4Network {
		stage4Transaction, ok :=
			transaction.(sqlServerStage4NetworkTransaction)
		if !ok || isNilInterface(stage4Transaction) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"write SQL Server Stage 4 network table %s: transaction has no replay-isolation fence",
					table.Name,
				),
			)
		}
		if err := stage4Transaction.
			AcquireStage4NetworkReplayFence(
				ctx,
				table,
			); err != nil {
			return newSQLServerSafeOperationError(
				"acquire SQL Server Stage 4 network replay fence for",
				table.Name,
				err,
			)
		}
		if err := stage4Transaction.
			PreflightStage4NetworkReplayIsolation(
				ctx,
				table,
			); err != nil {
			return fmt.Errorf(
				"prove SQL Server Stage 4 network replay isolation for table %s: %w",
				table.Name,
				err,
			)
		}
	}

	if table.Identity != nil {
		// The server can apply this session setting and lose the response.
		// Mark cleanup necessary before sending ON so every ambiguous response
		// is followed by OFF or by discarding the physical connection.
		identityInsertEnabled = true
		if _, err := transaction.Exec(
			ctx,
			sqlServerIdentityInsertStatement(table, true),
		); err != nil {
			return newSQLServerSafeOperationError(
				"enable SQL Server identity insert for",
				table.Name,
				err,
			)
		}
		// Explicit identity values can advance SQL Server's identity frontier
		// even when the surrounding row transaction later rolls back. The
		// writer cannot restore that database-level side effect without a race
		// after releasing its transaction lock, so any subsequent failure is
		// reported as unknown target state.
		identityFrontierMayHaveChanged = true
	}

	switch {
	case mode == "upsert":
		writeMayHaveChangedTarget = true
		err = writeSQLServerUpsertRows(
			ctx,
			transaction,
			table,
			columns,
			rows,
		)
	case table.Identity != nil ||
		sqlServerTableRequiresPreparedInsert(table, columns):
		writeMayHaveChangedTarget = true
		err = writeSQLServerInsertRows(
			ctx,
			transaction,
			table,
			columns,
			rows,
		)
	default:
		writeMayHaveChangedTarget = true
		err = writeSQLServerBulkRows(
			ctx,
			transaction,
			table,
			columns,
			rows,
		)
	}
	if err != nil {
		if identityFrontierMayHaveChanged {
			receipt.Certainty = CommitUnknown
		}
		return err
	}

	if identityInsertEnabled {
		if _, err := transaction.Exec(
			ctx,
			sqlServerIdentityInsertStatement(table, false),
		); err != nil {
			if identityFrontierMayHaveChanged {
				receipt.Certainty = CommitUnknown
			}
			return newSQLServerSafeOperationError(
				"disable SQL Server identity insert for",
				table.Name,
				err,
			)
		}
		identityInsertEnabled = false
	}

	if err := transaction.Commit(); err != nil {
		receipt.Certainty = CommitUnknown
		// IDENTITY_INSERT is session-scoped. Even though OFF succeeded before
		// Commit, repeat the cleanup after the failed transaction is rolled
		// back so an ambiguous commit cannot return a poisoned pooled session.
		if table.Identity != nil {
			identityInsertEnabled = true
		}
		return newSQLServerSafeOperationError(
			"commit SQL Server table",
			table.Name,
			err,
		)
	}
	committed = true
	receipt.Certainty = CommitDurable
	receipt.CommittedRows = receipt.AttemptedRows
	return nil
}

type sqlServerNumberedError interface {
	SQLErrorNumber() int32
}

func sqlServerRollbackAlreadyCompletedByXACTAbort(err error) bool {
	var numbered sqlServerNumberedError
	return errors.As(err, &numbered) &&
		numbered.SQLErrorNumber() ==
			sqlServerRollbackWithoutBeginErrorNumber
}

func writeSQLServerBulkRows(
	ctx context.Context,
	transaction sqlServerNativeTransaction,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	statement, err := transaction.Prepare(
		ctx,
		sqlServerNativeBulkStatement(table, columns, len(rows)),
	)
	if err != nil {
		return newSQLServerSafeOperationError(
			"prepare SQL Server bulk copy for",
			table.Name,
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			_ = statement.Close()
		}
	}()
	for _, row := range rows {
		affected, err := statement.Exec(ctx, row)
		if err != nil {
			return newSQLServerSafeOperationError(
				"write SQL Server bulk row for",
				table.Name,
				err,
			)
		}
		if affected != 0 {
			return fmt.Errorf(
				"write SQL Server table %s: bulk row acknowledged %d rows before completion; expected 0",
				table.Name,
				affected,
			)
		}
	}
	affected, err := statement.Done(ctx)
	if err != nil {
		return newSQLServerSafeOperationError(
			"complete SQL Server bulk copy for",
			table.Name,
			err,
		)
	}
	if affected != int64(len(rows)) {
		return fmt.Errorf(
			"write SQL Server table %s: bulk copy acknowledged %d rows; expected %d",
			table.Name,
			affected,
			len(rows),
		)
	}
	if err := statement.Close(); err != nil {
		return newSQLServerSafeOperationError(
			"close SQL Server bulk copy for",
			table.Name,
			err,
		)
	}
	closed = true
	return nil
}

func writeSQLServerInsertRows(
	ctx context.Context,
	transaction sqlServerNativeTransaction,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	statement, err := transaction.Prepare(
		ctx,
		sqlServerNativeInsertStatement(table, columns),
	)
	if err != nil {
		return newSQLServerSafeOperationError(
			"prepare SQL Server insert for",
			table.Name,
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			_ = statement.Close()
		}
	}()
	for _, row := range rows {
		affected, err := statement.Exec(ctx, row)
		if err != nil {
			return newSQLServerSafeOperationError(
				"write SQL Server table",
				table.Name,
				err,
			)
		}
		if affected != 1 {
			return fmt.Errorf(
				"write SQL Server table %s: insert affected %d rows; expected exactly 1",
				table.Name,
				affected,
			)
		}
	}
	if err := statement.Close(); err != nil {
		return newSQLServerSafeOperationError(
			"close SQL Server insert for",
			table.Name,
			err,
		)
	}
	closed = true
	return nil
}

// go-mssqldb v1.9's native BCP metadata is rejected by SQL Server 2022 for
// TIME columns at the 0..6 precision admitted by DMTX ("Invalid column
// attribute from bcp client"). Its bulk UNIQUEIDENTIFIER encoder also accepts
// only []byte, while source normalization deliberately emits canonical UUID
// strings that prepared parameters accept exactly. Keep bulk copy for other
// tables and use the deterministic affected-row-checked path for these known
// driver boundaries.
func sqlServerTableRequiresPreparedInsert(
	table schema.Table,
	columns []string,
) bool {
	for _, name := range columns {
		column, exists := sqlServerTableColumn(table, name)
		if !exists {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "time", "uuid", "uniqueidentifier":
			return true
		}
	}
	return false
}

type sqlServerNativeUpsertPlan struct {
	insertSQL       string
	updateSQL       string
	existsSQL       string
	updatePositions []int
	keyPositions    []int
}

func writeSQLServerUpsertRows(
	ctx context.Context,
	transaction sqlServerNativeTransaction,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	plan, err := planSQLServerNativeUpsert(table, columns)
	if err != nil {
		return err
	}
	insert, err := transaction.Prepare(ctx, plan.insertSQL)
	if err != nil {
		return newSQLServerSafeOperationError(
			"prepare SQL Server upsert insert for",
			table.Name,
			err,
		)
	}
	insertClosed := false
	defer func() {
		if !insertClosed {
			_ = insert.Close()
		}
	}()

	matchSQL := plan.updateSQL
	if matchSQL == "" {
		matchSQL = plan.existsSQL
	}
	match, err := transaction.Prepare(ctx, matchSQL)
	if err != nil {
		return newSQLServerSafeOperationError(
			"prepare SQL Server locked upsert match for",
			table.Name,
			err,
		)
	}
	matchClosed := false
	defer func() {
		if !matchClosed {
			_ = match.Close()
		}
	}()

	for _, row := range rows {
		found := false
		if plan.updateSQL != "" {
			arguments := sqlServerRowPositions(
				row,
				plan.updatePositions,
			)
			affected, err := match.Exec(ctx, arguments)
			if err != nil {
				return newSQLServerSafeOperationError(
					"update SQL Server upsert row for",
					table.Name,
					err,
				)
			}
			if affected < 0 || affected > 1 {
				return fmt.Errorf(
					"write SQL Server table %s: locked upsert update affected %d rows; expected 0 or 1",
					table.Name,
					affected,
				)
			}
			found = affected == 1
		} else {
			arguments := sqlServerRowPositions(row, plan.keyPositions)
			found, err = match.QueryBool(ctx, arguments)
			if err != nil {
				return newSQLServerSafeOperationError(
					"inspect SQL Server locked upsert row for",
					table.Name,
					err,
				)
			}
		}
		if found {
			continue
		}
		affected, err := insert.Exec(ctx, row)
		if err != nil {
			return newSQLServerSafeOperationError(
				"insert SQL Server upsert row for",
				table.Name,
				err,
			)
		}
		if affected != 1 {
			return fmt.Errorf(
				"write SQL Server table %s: upsert insert affected %d rows; expected exactly 1",
				table.Name,
				affected,
			)
		}
	}

	if err := match.Close(); err != nil {
		return newSQLServerSafeOperationError(
			"close SQL Server locked upsert match for",
			table.Name,
			err,
		)
	}
	matchClosed = true
	if err := insert.Close(); err != nil {
		return newSQLServerSafeOperationError(
			"close SQL Server upsert insert for",
			table.Name,
			err,
		)
	}
	insertClosed = true
	return nil
}

func validateSQLServerWriteShape(
	table schema.Table,
	columns []string,
	mode string,
) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf(
			"SQL Server target schema and table name are required",
		)
	}
	if mode != "drop_recreate" && mode != "upsert" {
		return fmt.Errorf(
			"write SQL Server table %s: unsupported target mode %q",
			table.Name,
			mode,
		)
	}
	if len(columns) == 0 {
		return fmt.Errorf(
			"write SQL Server table %s: at least one column is required",
			table.Name,
		)
	}

	metadata := make(map[string]string, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return fmt.Errorf(
				"write SQL Server table %s: schema contains an empty column name",
				table.Name,
			)
		}
		key := strings.ToLower(column.Name)
		if previous, duplicate := metadata[key]; duplicate {
			return fmt.Errorf(
				"write SQL Server table %s: schema contains ambiguous columns %s and %s",
				table.Name,
				previous,
				column.Name,
			)
		}
		metadata[key] = column.Name
	}

	requested := make(map[string]int, len(columns))
	for index, column := range columns {
		if column == "" {
			return fmt.Errorf(
				"write SQL Server table %s: requested column name is empty",
				table.Name,
			)
		}
		key := strings.ToLower(column)
		if _, duplicate := requested[key]; duplicate {
			return fmt.Errorf(
				"write SQL Server table %s: requested column %s is duplicated",
				table.Name,
				column,
			)
		}
		canonical, exists := metadata[key]
		if !exists || canonical != column {
			return fmt.Errorf(
				"write SQL Server table %s: requested column %s is not present in schema with exact spelling",
				table.Name,
				column,
			)
		}
		requested[key] = index
	}

	if table.Identity != nil {
		if table.Identity.Column == "" {
			return fmt.Errorf(
				"write SQL Server table %s: identity column is empty",
				table.Name,
			)
		}
		identityColumn, exists := sqlServerTableColumn(
			table,
			table.Identity.Column,
		)
		identityKeys := primaryKeyColumns(table)
		if !exists ||
			table.Identity.Generation != schema.IdentityByDefault ||
			identityColumn.Nullable ||
			strings.ToLower(strings.TrimSpace(identityColumn.Type)) !=
				"bigint" ||
			len(identityKeys) != 1 ||
			identityKeys[0] != table.Identity.Column {
			return fmt.Errorf(
				"write SQL Server table %s: identity metadata is unsupported",
				table.Name,
			)
		}
		requestedPosition, exists := requested[strings.ToLower(
			table.Identity.Column,
		)]
		if !exists ||
			columns[requestedPosition] != table.Identity.Column {
			return fmt.Errorf(
				"write SQL Server table %s: identity column %s is not included",
				table.Name,
				table.Identity.Column,
			)
		}
	}
	if mode != "upsert" {
		return nil
	}
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return fmt.Errorf(
			"table %s has no primary key; SQL Server upsert requires a primary key",
			table.Name,
		)
	}
	for _, key := range keys {
		column, exists := sqlServerTableColumn(table, key)
		if !exists || column.Nullable {
			return fmt.Errorf(
				"write SQL Server table %s: primary-key column %s is missing or nullable",
				table.Name,
				key,
			)
		}
		if _, included := requested[strings.ToLower(key)]; !included {
			return fmt.Errorf(
				"write SQL Server table %s: upsert primary-key column %s is not included",
				table.Name,
				key,
			)
		}
	}
	return nil
}

func planSQLServerNativeUpsert(
	table schema.Table,
	columns []string,
) (sqlServerNativeUpsertPlan, error) {
	if err := validateSQLServerWriteShape(table, columns, "upsert"); err != nil {
		return sqlServerNativeUpsertPlan{}, err
	}
	keys := primaryKeyColumns(table)
	positions := make(map[string]int, len(columns))
	for index, column := range columns {
		positions[strings.ToLower(column)] = index
	}
	keySet := make(map[string]struct{}, len(keys))
	keyPositions := make([]int, len(keys))
	for index, key := range keys {
		folded := strings.ToLower(key)
		keySet[folded] = struct{}{}
		keyPositions[index] = positions[folded]
	}

	nonKeyPositions := make([]int, 0, len(columns)-len(keys))
	for index, column := range columns {
		if _, key := keySet[strings.ToLower(column)]; !key {
			nonKeyPositions = append(nonKeyPositions, index)
		}
	}
	plan := sqlServerNativeUpsertPlan{
		insertSQL:    sqlServerNativeInsertStatement(table, columns),
		keyPositions: keyPositions,
	}

	conditions := make([]string, len(keys))
	parameter := 1
	if len(nonKeyPositions) > 0 {
		assignments := make([]string, len(nonKeyPositions))
		for index, position := range nonKeyPositions {
			assignments[index] = sqlServerIdentifier(columns[position]) +
				" = " + fmt.Sprintf("@p%d", parameter)
			parameter++
		}
		for index, key := range keys {
			conditions[index] = sqlServerIdentifier(key) +
				" = " + fmt.Sprintf("@p%d", parameter)
			parameter++
		}
		plan.updatePositions = append(
			append([]int(nil), nonKeyPositions...),
			keyPositions...,
		)
		plan.updateSQL = "UPDATE " +
			sqlServerQualified(table.Schema, table.Name) +
			" WITH (UPDLOCK, HOLDLOCK) SET " +
			strings.Join(assignments, ", ") +
			" WHERE " + strings.Join(conditions, " AND ")
		return plan, nil
	}

	for index, key := range keys {
		conditions[index] = sqlServerIdentifier(key) +
			" = " + fmt.Sprintf("@p%d", index+1)
	}
	plan.existsSQL = "SELECT CAST(CASE WHEN EXISTS (" +
		"SELECT 1 FROM " +
		sqlServerQualified(table.Schema, table.Name) +
		" WITH (UPDLOCK, HOLDLOCK) WHERE " +
		strings.Join(conditions, " AND ") +
		") THEN 1 ELSE 0 END AS bit)"
	return plan, nil
}

func sqlServerNativeBulkStatement(
	table schema.Table,
	columns []string,
	rows int,
) string {
	return mssql.CopyIn(
		sqlServerQualified(table.Schema, table.Name),
		mssql.BulkOptions{
			CheckConstraints: true,
			KeepNulls:        true,
			RowsPerBatch:     rows,
		},
		columns...,
	)
}

func sqlServerNativeInsertStatement(
	table schema.Table,
	columns []string,
) string {
	return "INSERT INTO " +
		sqlServerQualified(table.Schema, table.Name) +
		" (" + sqlServerQuotedColumns(columns) + ") VALUES (" +
		sqlServerPlaceholders(len(columns)) + ")"
}

func sqlServerIdentityInsertStatement(
	table schema.Table,
	enabled bool,
) string {
	state := "OFF"
	if enabled {
		state = "ON"
	}
	return "SET IDENTITY_INSERT " +
		sqlServerQualified(table.Schema, table.Name) + " " + state
}

func sqlServerRowPositions(row []any, positions []int) []any {
	values := make([]any, len(positions))
	for index, position := range positions {
		values[index] = row[position]
	}
	return values
}

func sqlServerTableColumn(
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

type sqlServerSQLConnectionProvider struct {
	database *sql.DB
}

func (provider sqlServerSQLConnectionProvider) WithConnection(
	ctx context.Context,
	operation func(sqlServerNativeConnection) error,
) error {
	if provider.database == nil {
		return fmt.Errorf("SQL Server database is not configured")
	}
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return err
	}
	wrapped := &sqlServerSQLConnection{connection: connection}
	defer func() {
		if !wrapped.detached.Load() {
			_ = connection.Close()
		}
	}()
	return operation(wrapped)
}

type sqlServerSQLConnection struct {
	connection *sql.Conn
	detached   atomic.Bool
}

func (connection *sqlServerSQLConnection) BeginSerializable(
	ctx context.Context,
) (sqlServerNativeTransaction, error) {
	transaction, err := connection.connection.BeginTx(
		ctx,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		return nil, err
	}
	return &sqlServerSQLTransaction{
		transaction: transaction,
		owner:       connection,
	}, nil
}

func (connection *sqlServerSQLConnection) Exec(
	ctx context.Context,
	statement string,
) (int64, error) {
	result, err := connection.connection.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (connection *sqlServerSQLConnection) Discard() {
	_ = connection.connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

type sqlServerSQLTransaction struct {
	transaction *sql.Tx
	owner       *sqlServerSQLConnection
}

func (transaction *sqlServerSQLTransaction) AcquireStage4NetworkReplayFence(
	ctx context.Context,
	table schema.Table,
) error {
	// TABLOCKX is transaction-owned and blocks Sch-M/ALTER while remaining
	// compatible with this transaction's later DML. TOP (1), rather than
	// TOP (0), prevents the optimizer from removing the table access for an
	// empty-result constant plan; an empty table still acquires the table lock.
	rows, err := transaction.transaction.QueryContext(
		ctx,
		stage4SQLServerNetworkReplayFenceStatement(table),
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

func (transaction *sqlServerSQLTransaction) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
) error {
	return preflightStage4SQLServerNetworkReplayIsolationTransaction(
		ctx,
		transaction.transaction,
		table,
	)
}

func (transaction *sqlServerSQLTransaction) Prepare(
	ctx context.Context,
	statement string,
) (sqlServerNativeStatement, error) {
	prepared, err := transaction.transaction.PrepareContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	return &sqlServerSQLStatement{statement: prepared}, nil
}

func (transaction *sqlServerSQLTransaction) Exec(
	ctx context.Context,
	statement string,
) (int64, error) {
	result, err := transaction.transaction.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (transaction *sqlServerSQLTransaction) Commit() error {
	return transaction.transaction.Commit()
}

func (transaction *sqlServerSQLTransaction) Rollback() error {
	return transaction.transaction.Rollback()
}

func (transaction *sqlServerSQLTransaction) RollbackStage4Network(
	ctx context.Context,
) error {
	if ctx == nil {
		return fmt.Errorf(
			"SQL Server Stage 4 rollback context is required",
		)
	}
	if transaction.owner == nil ||
		transaction.owner.connection == nil {
		return fmt.Errorf(
			"SQL Server Stage 4 rollback has no pinned connection",
		)
	}
	// Detach before starting rollback so the provider can return on the
	// cleanup deadline without putting an active transaction back in the
	// pool. The goroutine is the sole owner from this point onward.
	transaction.owner.detached.Store(true)
	result := make(chan error, 1)
	go func() {
		rollbackErr := transaction.transaction.Rollback()
		if rollbackErr != nil &&
			!errors.Is(rollbackErr, sql.ErrTxDone) {
			transaction.owner.Discard()
		}
		closeErr := transaction.owner.connection.Close()
		result <- errors.Join(rollbackErr, closeErr)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf(
			"SQL Server Stage 4 rollback did not complete before cleanup deadline: %w",
			ctx.Err(),
		)
	}
}

type sqlServerSQLStatement struct {
	statement *sql.Stmt
}

func (statement *sqlServerSQLStatement) Exec(
	ctx context.Context,
	values []any,
) (int64, error) {
	result, err := statement.statement.ExecContext(ctx, values...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (statement *sqlServerSQLStatement) QueryBool(
	ctx context.Context,
	values []any,
) (bool, error) {
	var result bool
	err := statement.statement.QueryRowContext(ctx, values...).Scan(&result)
	return result, err
}

func (statement *sqlServerSQLStatement) Done(
	ctx context.Context,
) (int64, error) {
	result, err := statement.statement.ExecContext(ctx)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (statement *sqlServerSQLStatement) Close() error {
	return statement.statement.Close()
}
