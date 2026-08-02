package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

type sqliteStage4NetworkBatchWriter interface {
	WriteStage4NetworkBatch(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) (WriteReceipt, error)
}

// sqliteStage4NetworkRebuildBatchWriter is intentionally separate from the
// upsert writer so a target cannot accidentally advertise duplicate-safe
// rebuild replay merely by implementing the ordinary Stage 4 upsert path.
type sqliteStage4NetworkRebuildBatchWriter interface {
	WriteStage4NetworkRebuildBatch(
		context.Context,
		schema.Table,
		[]string,
		NetworkWriteMode,
		[][]any,
	) (WriteReceipt, error)
}

type sqliteStage4NetworkConnectionProvider interface {
	WithConnection(
		context.Context,
		func(sqliteStage4NetworkConnection) error,
	) error
}

type sqliteStage4NetworkConnection interface {
	BeginImmediate(
		context.Context,
	) (sqliteStage4NetworkTransaction, error)
	Discard()
}

const sqliteStage4NetworkCleanupTimeout = 5 * time.Second

type sqliteStage4NetworkTransaction interface {
	ValidateStage4NetworkReplayIsolation(
		context.Context,
		schema.Table,
	) error
	WriteUpsert(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

type sqliteStage4NetworkRebuildTransaction interface {
	WriteFreshInsert(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) error
	WriteDuplicateSafeInsertOnly(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) error
}

type sqliteStage4NetworkWriter struct {
	connections sqliteStage4NetworkConnectionProvider
}

func newSQLiteStage4NetworkWriter(
	database *sql.DB,
) *sqliteStage4NetworkWriter {
	return &sqliteStage4NetworkWriter{
		connections: sqliteStage4SQLConnectionProvider{
			database: database,
		},
	}
}

func (writer *sqliteStage4NetworkWriter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	return writer.writeStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
		NetworkWriteIdempotentUpsert,
	)
}

// WriteStage4NetworkRebuildBatch is the bounded rebuild counterpart to the
// upsert page writer. Fresh writes use a strict INSERT so an external primary
// key conflict stops the migration. Only a durable issued-page replay may use
// the insert-only primary-key conflict handler.
func (writer *sqliteStage4NetworkWriter) WriteStage4NetworkRebuildBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	return writer.writeStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
		mode,
	)
}

func (writer *sqliteStage4NetworkWriter) writeStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
	mode NetworkWriteMode,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	if err := validateSQLiteStage4NetworkWriteShape(
		table,
		columns,
		rows,
	); err != nil {
		return notCommitted, err
	}
	switch mode {
	case NetworkWriteIdempotentUpsert,
		NetworkWriteFreshInsert,
		NetworkWriteDuplicateSafeInsertOnly:
	default:
		return notCommitted, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network write received unsupported mode %q",
				mode,
			),
		)
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if ctx == nil {
		return notCommitted, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network write context is required",
			),
		)
	}
	if writer == nil || writer.connections == nil {
		return notCommitted, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network connection provider is not configured",
			),
		)
	}

	receipt := notCommitted
	callbackCalled := false
	err := writer.connections.WithConnection(
		ctx,
		func(
			connection sqliteStage4NetworkConnection,
		) (operationError error) {
			callbackCalled = true
			if connection == nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"SQLite Stage 4 network connection is not configured",
					),
				)
			}
			transaction, beginErr := connection.BeginImmediate(ctx)
			if beginErr != nil {
				return fmt.Errorf(
					"begin SQLite Stage 4 network write for %s with an immediate writer reservation: %w",
					table.Name,
					beginErr,
				)
			}
			if transaction == nil {
				connection.Discard()
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"SQLite Stage 4 network transaction is not configured for table %s",
						table.Name,
					),
				)
			}
			committed := false
			defer func() {
				if committed {
					return
				}
				cleanupContext, cancelCleanup :=
					context.WithTimeout(
						context.WithoutCancel(ctx),
						sqliteStage4NetworkCleanupTimeout,
					)
				defer cancelCleanup()
				if rollbackErr := transaction.Rollback(
					cleanupContext,
				); rollbackErr != nil {
					connection.Discard()
					operationError = errors.Join(
						operationError,
						fmt.Errorf(
							"rollback SQLite Stage 4 network write for %s: %w",
							table.Name,
							rollbackErr,
						),
					)
				}
			}()

			if proofErr := transaction.
				ValidateStage4NetworkReplayIsolation(
					ctx,
					table,
				); proofErr != nil {
				return fmt.Errorf(
					"prove SQLite Stage 4 network replay isolation for %s: %w",
					table.Name,
					proofErr,
				)
			}
			var writeErr error
			switch mode {
			case NetworkWriteIdempotentUpsert:
				writeErr = transaction.WriteUpsert(
					ctx,
					table,
					columns,
					rows,
				)
			case NetworkWriteFreshInsert,
				NetworkWriteDuplicateSafeInsertOnly:
				rebuildTransaction, ok := transaction.(sqliteStage4NetworkRebuildTransaction)
				if !ok || rebuildTransaction == nil {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"SQLite Stage 4 rebuild transaction is not configured for table %s",
							table.Name,
						),
					)
				}
				if mode == NetworkWriteFreshInsert {
					writeErr = rebuildTransaction.WriteFreshInsert(
						ctx,
						table,
						columns,
						rows,
					)
				} else {
					writeErr = rebuildTransaction.WriteDuplicateSafeInsertOnly(
						ctx,
						table,
						columns,
						rows,
					)
				}
			}
			if writeErr != nil {
				return fmt.Errorf(
					"write SQLite Stage 4 network page for %s: %w",
					table.Name,
					writeErr,
				)
			}
			if commitErr := transaction.Commit(ctx); commitErr != nil {
				receipt.Certainty = CommitUnknown
				return fmt.Errorf(
					"commit SQLite Stage 4 network page for %s: %w",
					table.Name,
					commitErr,
				)
			}
			committed = true
			receipt = WriteReceipt{
				Certainty:     CommitDurable,
				AttemptedRows: attempted,
				CommittedRows: attempted,
			}
			return nil
		},
	)
	if err == nil {
		return receipt, nil
	}
	if callbackCalled {
		return receipt, err
	}
	return receipt, fmt.Errorf(
		"acquire SQLite Stage 4 network connection for %s: %w",
		table.Name,
		err,
	)
}

func validateSQLiteStage4NetworkWriteShape(
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	if table.Schema != "" || table.Name == "" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network target requires an unqualified non-empty table name",
			),
		)
	}
	if len(columns) == 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network write for %s requires at least one column",
				table.Name,
			),
		)
	}
	metadata := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network table %s contains an empty column name",
					table.Name,
				),
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network table %s contains duplicate column %s",
					table.Name,
					column.Name,
				),
			)
		}
		metadata[column.Name] = struct{}{}
	}
	requested := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network write for %s contains an empty requested column",
					table.Name,
				),
			)
		}
		if _, duplicate := requested[column]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network write for %s repeats column %s",
					table.Name,
					column,
				),
			)
		}
		if _, exists := metadata[column]; !exists {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network write for %s requests unknown column %s",
					table.Name,
					column,
				),
			)
		}
		requested[column] = struct{}{}
	}
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return NewTransferError(
			ErrorClassPrimaryKey,
			fmt.Errorf(
				"table %s has no primary key; SQLite Stage 4 network replay requires an idempotent upsert key",
				table.Name,
			),
		)
	}
	for _, key := range keys {
		if _, included := requested[key]; !included {
			return NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"SQLite Stage 4 network write for %s omits primary-key column %s",
					table.Name,
					key,
				),
			)
		}
	}
	if err := requireSQLiteReplaySafePrimaryKey(table); err != nil {
		return err
	}
	for index, row := range rows {
		if len(row) != len(columns) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQLite Stage 4 network write for %s row %d has %d values, want %d",
					table.Name,
					index,
					len(row),
					len(columns),
				),
			)
		}
	}
	return nil
}

type sqliteStage4SQLConnectionProvider struct {
	database *sql.DB
}

func (provider sqliteStage4SQLConnectionProvider) WithConnection(
	ctx context.Context,
	operation func(sqliteStage4NetworkConnection) error,
) (operationError error) {
	if provider.database == nil {
		return fmt.Errorf("SQLite target database is not configured")
	}
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			operationError = errors.Join(
				operationError,
				fmt.Errorf(
					"close SQLite Stage 4 network connection: %w",
					closeErr,
				),
			)
		}
	}()
	return operation(sqliteStage4SQLConnection{
		connection: connection,
	})
}

type sqliteStage4SQLConnection struct {
	connection *sql.Conn
}

func (connection sqliteStage4SQLConnection) BeginImmediate(
	ctx context.Context,
) (sqliteStage4NetworkTransaction, error) {
	if connection.connection == nil {
		return nil, fmt.Errorf("SQLite SQL connection is not configured")
	}
	if _, err := connection.connection.ExecContext(
		ctx,
		"BEGIN IMMEDIATE",
	); err != nil {
		return nil, err
	}
	return sqliteStage4SQLTransaction{
		connection: connection.connection,
	}, nil
}

func (connection sqliteStage4SQLConnection) Discard() {
	if connection.connection == nil {
		return
	}
	_ = connection.connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

type sqliteStage4SQLTransaction struct {
	connection *sql.Conn
}

func (transaction sqliteStage4SQLTransaction) ValidateStage4NetworkReplayIsolation(
	ctx context.Context,
	table schema.Table,
) error {
	if err := validateStage4SQLiteRetainedReplayTarget(
		ctx,
		transaction.connection,
		table,
	); err != nil {
		return err
	}
	return preflightStage4SQLiteNetworkReplayIsolation(
		ctx,
		transaction.connection,
		[]schema.Table{table},
	)
}

func (transaction sqliteStage4SQLTransaction) WriteUpsert(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	statement, err := transaction.connection.PrepareContext(
		ctx,
		writeStatement(table, columns, "upsert"),
	)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = statement.Close()
		}
	}()
	for _, values := range rows {
		if _, err := statement.ExecContext(
			ctx,
			values...,
		); err != nil {
			return err
		}
	}
	if err := statement.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func (transaction sqliteStage4SQLTransaction) WriteDuplicateSafeInsertOnly(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	statement, err := transaction.connection.PrepareContext(
		ctx,
		writeStatement(table, columns, sqliteInsertOnlyReplayMode),
	)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = statement.Close()
		}
	}()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return err
		}
	}
	if err := statement.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func (transaction sqliteStage4SQLTransaction) WriteFreshInsert(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	statement, err := transaction.connection.PrepareContext(
		ctx,
		writeStatement(table, columns, "drop_recreate"),
	)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = statement.Close()
		}
	}()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return err
		}
	}
	if err := statement.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func (transaction sqliteStage4SQLTransaction) Commit(
	ctx context.Context,
) error {
	_, err := transaction.connection.ExecContext(ctx, "COMMIT")
	return err
}

func (transaction sqliteStage4SQLTransaction) Rollback(
	ctx context.Context,
) error {
	_, err := transaction.connection.ExecContext(ctx, "ROLLBACK")
	return err
}
