package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/schema"
)

// postgresBatchWriter is the target adapter's durable PostgreSQL write
// dependency. Production construction always installs postgresNativeWriter.
type postgresBatchWriter interface {
	WriteBatch(
		context.Context,
		schema.Table,
		[]string,
		string,
		[][]any,
	) (WriteReceipt, error)
}

// postgresSafeOperationError preserves an underlying driver error for
// errors.Is/errors.As without including its potentially sensitive text in
// operator output.
type postgresSafeOperationError struct {
	operation string
	table     string
	cause     error
}

func (operationError *postgresSafeOperationError) Error() string {
	return operationError.operation + " " + operationError.table
}

func (operationError *postgresSafeOperationError) Unwrap() error {
	return operationError.cause
}

func newPostgresSafeOperationError(
	operation string,
	table string,
	cause error,
) error {
	var safeError *postgresSafeOperationError
	if errors.As(cause, &safeError) {
		return safeError
	}
	return &postgresSafeOperationError{
		operation: operation,
		table:     table,
		cause:     cause,
	}
}

// postgresNativeConnectionProvider pins a physical pgx stdlib connection for
// the entire native transaction. The callback must finish before WithConnection
// returns because database/sql does not permit retaining a raw driver
// connection.
type postgresNativeConnectionProvider interface {
	WithConnection(
		context.Context,
		func(postgresNativeConnection) error,
	) error
}

type postgresNativeConnection interface {
	Begin(context.Context) (postgresNativeTransaction, error)
}

// postgresNativeTransaction is deliberately narrower than pgx.Tx. It contains
// only the native operations required for one durable PostgreSQL batch.
type postgresNativeTransaction interface {
	CopyRows(
		context.Context,
		[]string,
		[]string,
		[][]any,
	) (int64, error)
	Exec(context.Context, string) (int64, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type postgresNativeWriter struct {
	connections postgresNativeConnectionProvider
}

func newPostgresNativeWriter(database *sql.DB) *postgresNativeWriter {
	return &postgresNativeWriter{
		connections: postgresStdlibConnectionProvider{database: database},
	}
}

func (writer *postgresNativeWriter) WriteBatch(
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
	if err := validatePostgresWriteShape(table, columns, mode); err != nil {
		return notCommitted, err
	}
	if len(rows) == 0 {
		return WriteReceipt{
			Certainty: CommitDurable,
		}, nil
	}
	if writer == nil || writer.connections == nil {
		return notCommitted, fmt.Errorf(
			"write PostgreSQL table %s: native connection provider is not configured",
			table.Name,
		)
	}

	normalizedRows, err := normalizePostgresRows(table, columns, rows)
	if err != nil {
		return notCommitted, err
	}

	receipt := notCommitted
	var callbackError error
	err = writer.connections.WithConnection(
		ctx,
		func(
			connection postgresNativeConnection,
		) (operationError error) {
			defer func() {
				callbackError = operationError
			}()
			transaction, beginErr := connection.Begin(ctx)
			if beginErr != nil {
				return newPostgresSafeOperationError(
					"begin PostgreSQL native write for",
					table.Name,
					beginErr,
				)
			}
			committed := false
			defer func() {
				if !committed {
					_ = transaction.Rollback(ctx)
				}
			}()

			if mode == "drop_recreate" {
				if copyErr := copyPostgresRows(
					ctx,
					transaction,
					[]string{table.Schema, table.Name},
					columns,
					normalizedRows,
					"copy PostgreSQL table",
					table.Name,
				); copyErr != nil {
					return copyErr
				}
			} else {
				if upsertErr := stageAndUpsertPostgresRows(
					ctx,
					transaction,
					table,
					columns,
					normalizedRows,
				); upsertErr != nil {
					return upsertErr
				}
			}

			if commitErr := transaction.Commit(ctx); commitErr != nil {
				if !errors.Is(commitErr, pgx.ErrTxCommitRollback) {
					receipt.Certainty = CommitUnknown
				}
				return newPostgresSafeOperationError(
					"commit PostgreSQL table",
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
	if callbackError != nil {
		return receipt, callbackError
	}
	if err != nil {
		return receipt, newPostgresSafeOperationError(
			"acquire PostgreSQL native connection for",
			table.Name,
			err,
		)
	}
	return receipt, nil
}

func copyPostgresRows(
	ctx context.Context,
	transaction postgresNativeTransaction,
	tableIdentifier []string,
	columns []string,
	rows [][]any,
	operation string,
	tableName string,
) error {
	copied, err := transaction.CopyRows(
		ctx,
		tableIdentifier,
		columns,
		rows,
	)
	if err != nil {
		return newPostgresSafeOperationError(
			operation,
			tableName,
			err,
		)
	}
	if copied != int64(len(rows)) {
		return fmt.Errorf(
			"native COPY acknowledged %d rows, expected %d",
			copied,
			len(rows),
		)
	}
	return nil
}

func stageAndUpsertPostgresRows(
	ctx context.Context,
	transaction postgresNativeTransaction,
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	stage := postgresStageTableName(table, columns)
	if _, err := transaction.Exec(
		ctx,
		postgresCreateStageStatement(table, columns, stage),
	); err != nil {
		return newPostgresSafeOperationError(
			"create PostgreSQL staging table for",
			table.Name,
			err,
		)
	}
	if err := copyPostgresRows(
		ctx,
		transaction,
		[]string{"pg_temp", stage},
		columns,
		rows,
		"copy PostgreSQL staging table for",
		table.Name,
	); err != nil {
		return err
	}

	statement, updates, err := postgresMergeStageStatement(
		table,
		columns,
		stage,
	)
	if err != nil {
		return err
	}
	merged, err := transaction.Exec(ctx, statement)
	if err != nil {
		return newPostgresSafeOperationError(
			"merge PostgreSQL staging table for",
			table.Name,
			err,
		)
	}
	if updates && merged != int64(len(rows)) {
		return fmt.Errorf(
			"merge PostgreSQL table %s affected %d rows, expected %d",
			table.Name,
			merged,
			len(rows),
		)
	}
	return nil
}

type postgresStdlibConnectionProvider struct {
	database *sql.DB
}

func (provider postgresStdlibConnectionProvider) WithConnection(
	ctx context.Context,
	operation func(postgresNativeConnection) error,
) error {
	if provider.database == nil {
		return fmt.Errorf("PostgreSQL database is not configured")
	}
	connection, err := provider.database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire PostgreSQL native connection: %w", err)
	}
	defer connection.Close()

	return connection.Raw(func(driverConnection any) error {
		stdlibConnection, ok := driverConnection.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf(
				"PostgreSQL native COPY requires the pgx stdlib driver",
			)
		}
		return operation(postgresPGXConnection{
			connection: stdlibConnection.Conn(),
		})
	})
}

type postgresPGXConnection struct {
	connection *pgx.Conn
}

func (connection postgresPGXConnection) Begin(
	ctx context.Context,
) (postgresNativeTransaction, error) {
	transaction, err := connection.connection.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return postgresPGXTransaction{transaction: transaction}, nil
}

type postgresPGXTransaction struct {
	transaction pgx.Tx
}

func (transaction postgresPGXTransaction) CopyRows(
	ctx context.Context,
	tableIdentifier []string,
	columns []string,
	rows [][]any,
) (int64, error) {
	return transaction.transaction.CopyFrom(
		ctx,
		pgx.Identifier(tableIdentifier),
		columns,
		pgx.CopyFromRows(rows),
	)
}

func (transaction postgresPGXTransaction) Exec(
	ctx context.Context,
	statement string,
) (int64, error) {
	tag, err := transaction.transaction.Exec(ctx, statement)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (transaction postgresPGXTransaction) Commit(ctx context.Context) error {
	return transaction.transaction.Commit(ctx)
}

func (transaction postgresPGXTransaction) Rollback(ctx context.Context) error {
	return transaction.transaction.Rollback(ctx)
}
