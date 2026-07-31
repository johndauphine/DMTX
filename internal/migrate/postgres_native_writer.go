package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

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

type postgresStage4NetworkBatchWriter interface {
	WriteStage4NetworkBatch(
		context.Context,
		schema.Table,
		[]string,
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

type postgresNativeDiscardableConnection interface {
	Discard()
}

const postgresStage4NetworkRollbackTimeout = 5 * time.Second

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
	return writer.writeBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
		false,
	)
}

// WriteStage4NetworkBatch is the only PostgreSQL page-write path certified for
// Stage 4 crash replay. It always uses idempotent upsert and proves that an
// update cannot escape the page through an incoming foreign key while the same
// transaction holds a DDL fence on the target table.
func (writer *postgresNativeWriter) WriteStage4NetworkBatch(
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

func (writer *postgresNativeWriter) writeBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
	stage4NetworkReplay bool,
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
	if stage4NetworkReplay && ctx == nil {
		return notCommitted, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL Stage 4 network write context is required",
			),
		)
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
			var discardable postgresNativeDiscardableConnection
			if stage4NetworkReplay {
				var discardableOK bool
				discardable, discardableOK =
					connection.(postgresNativeDiscardableConnection)
				if !discardableOK {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"PostgreSQL Stage 4 network connection cannot discard an unclean transaction for table %s",
							table.Name,
						),
					)
				}
			}
			transaction, beginErr := connection.Begin(ctx)
			if beginErr != nil {
				return newPostgresSafeOperationError(
					"begin PostgreSQL native write for",
					table.Name,
					beginErr,
				)
			}
			if transaction == nil {
				if discardable != nil {
					discardable.Discard()
				}
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"PostgreSQL native transaction is not configured for table %s",
						table.Name,
					),
				)
			}
			committed := false
			defer func() {
				if !committed {
					rollbackContext := ctx
					cancelRollback := func() {}
					if stage4NetworkReplay {
						rollbackContext, cancelRollback =
							context.WithTimeout(
								context.WithoutCancel(ctx),
								postgresStage4NetworkRollbackTimeout,
							)
					}
					defer cancelRollback()
					if rollbackErr := transaction.Rollback(
						rollbackContext,
					); rollbackErr != nil &&
						!errors.Is(rollbackErr, pgx.ErrTxClosed) {
						if stage4NetworkReplay {
							discardable.Discard()
							operationError = errors.Join(
								operationError,
								newPostgresSafeOperationError(
									"rollback PostgreSQL native write for",
									table.Name,
									rollbackErr,
								),
							)
						}
					}
				}
			}()

			if stage4NetworkReplay {
				if isolationErr := fenceAndValidateStage4PostgresNetworkReplay(
					ctx,
					transaction,
					table,
				); isolationErr != nil {
					return isolationErr
				}
			}

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
					if stage4NetworkReplay {
						// A failed COMMIT can mean the server committed but
						// the acknowledgement was lost. pgx then commonly
						// reports ErrTxClosed from the deferred rollback,
						// which proves nothing about the physical session.
						// Quarantine it immediately; only
						// ErrTxCommitRollback proves the transaction rolled
						// back instead of committing.
						discardable.Discard()
					}
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

func fenceAndValidateStage4PostgresNetworkReplay(
	ctx context.Context,
	transaction postgresNativeTransaction,
	table schema.Table,
) error {
	reader, ok := transaction.(stage4PostgresReplayCatalogReader)
	if !ok {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL native transaction does not provide the Stage 4 network replay catalog proof for table %s",
				table.Name,
			),
		)
	}
	if _, err := transaction.Exec(
		ctx,
		"LOCK TABLE "+
			postgresQualified(table.Schema, table.Name)+
			" IN SHARE UPDATE EXCLUSIVE MODE",
	); err != nil {
		return newPostgresSafeOperationError(
			"acquire PostgreSQL Stage 4 network replay DDL fence for",
			table.Name,
			err,
		)
	}
	err := validateStage4PostgresNetworkReplayIsolation(
		ctx,
		reader,
		[]schema.Table{table},
	)
	if err == nil {
		return nil
	}
	var transferError *TransferError
	if errors.As(err, &transferError) {
		return err
	}
	return newPostgresSafeOperationError(
		"prove PostgreSQL Stage 4 network replay isolation for",
		table.Name,
		err,
	)
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

	var operationError error
	rawError := connection.Raw(func(driverConnection any) error {
		stdlibConnection, ok := driverConnection.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf(
				"PostgreSQL native COPY requires the pgx stdlib driver",
			)
		}
		discard := false
		operationError = operation(postgresPGXConnection{
			connection: stdlibConnection.Conn(),
			discard:    &discard,
		})
		if discard {
			return driver.ErrBadConn
		}
		return operationError
	})
	if operationError != nil && errors.Is(rawError, driver.ErrBadConn) {
		return operationError
	}
	return rawError
}

type postgresPGXConnection struct {
	connection *pgx.Conn
	discard    *bool
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

func (connection postgresPGXConnection) Discard() {
	if connection.discard != nil {
		*connection.discard = true
	}
}

type postgresPGXTransaction struct {
	transaction pgx.Tx
}

type postgresPGXReplayCatalogRows struct {
	rows pgx.Rows
}

func (rows postgresPGXReplayCatalogRows) Next() bool {
	return rows.rows.Next()
}

func (rows postgresPGXReplayCatalogRows) Scan(destinations ...any) error {
	return rows.rows.Scan(destinations...)
}

func (rows postgresPGXReplayCatalogRows) Err() error {
	return rows.rows.Err()
}

func (rows postgresPGXReplayCatalogRows) Close() error {
	rows.rows.Close()
	return nil
}

func (transaction postgresPGXTransaction) ReadStage4PostgresRetainedShape(
	ctx context.Context,
	table schema.Table,
) (postgresCatalogTableShape, bool, error) {
	var result postgresCatalogTableShape
	err := transaction.transaction.QueryRow(
		ctx,
		`SELECT
			relation.relkind::text,
			relation.relpersistence::text,
			relation.relrowsecurity OR relation.relforcerowsecurity
		   FROM pg_catalog.pg_class AS relation
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2`,
		table.Schema,
		table.Name,
	).Scan(
		&result.relationKind,
		&result.persistence,
		&result.rowSecurity,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return postgresCatalogTableShape{}, false, nil
	}
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	if result.relationKind != "r" {
		return result, true, nil
	}

	rows, err := transaction.transaction.Query(
		ctx,
		`SELECT
			attribute.attname,
			column_type.typname,
			attribute.atttypmod,
			attribute.attnotnull,
			attribute.attgenerated::text,
			attribute.attidentity::text,
			attribute.attcollation = column_type.typcollation
		   FROM pg_catalog.pg_attribute AS attribute
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = attribute.attrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		   JOIN pg_catalog.pg_type AS column_type
		     ON column_type.oid = attribute.atttypid
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND relation.relkind = 'r'
		    AND attribute.attnum > 0
		    AND NOT attribute.attisdropped
		  ORDER BY attribute.attnum`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	for rows.Next() {
		var (
			column   postgresCatalogColumnShape
			typeName string
			typeMode int32
		)
		if err := rows.Scan(
			&column.name,
			&typeName,
			&typeMode,
			&column.notNull,
			&column.generated,
			&column.identity,
			&column.defaultCollation,
		); err != nil {
			rows.Close()
			return postgresCatalogTableShape{}, false, err
		}
		column.columnType = postgresCatalogTypeFromModifier(
			typeName,
			typeMode,
		)
		result.columns = append(result.columns, column)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return postgresCatalogTableShape{}, false, err
	}
	rows.Close()

	keyRows, err := transaction.transaction.Query(
		ctx,
		`SELECT attribute.attname
		   FROM pg_catalog.pg_index AS index
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = index.indrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		   CROSS JOIN LATERAL unnest(index.indkey)
		     WITH ORDINALITY AS key_column(attnum, position)
		   JOIN pg_catalog.pg_attribute AS attribute
		     ON attribute.attrelid = relation.oid
		    AND attribute.attnum = key_column.attnum
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND index.indisprimary
		  ORDER BY key_column.position`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	for keyRows.Next() {
		var name string
		if err := keyRows.Scan(&name); err != nil {
			keyRows.Close()
			return postgresCatalogTableShape{}, false, err
		}
		result.primaryKey = append(result.primaryKey, name)
	}
	if err := keyRows.Err(); err != nil {
		keyRows.Close()
		return postgresCatalogTableShape{}, false, err
	}
	keyRows.Close()

	if err := transaction.transaction.QueryRow(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_trigger AS trigger
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = trigger.tgrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND NOT trigger.tgisinternal`,
		table.Schema,
		table.Name,
	).Scan(&result.userTriggers); err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	if err := transaction.transaction.QueryRow(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_rewrite AS rewrite_rule
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = rewrite_rule.ev_class
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2`,
		table.Schema,
		table.Name,
	).Scan(&result.userRules); err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	return result, true, nil
}

func (transaction postgresPGXTransaction) QueryStage4PostgresIncomingForeignKeys(
	ctx context.Context,
	namespace string,
	table string,
) (stage4PostgresReplayCatalogRows, error) {
	rows, err := transaction.transaction.Query(
		ctx,
		stage4PostgresIncomingForeignKeysQuery,
		namespace,
		table,
	)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, fmt.Errorf(
			"PostgreSQL replay catalog query returned no row iterator",
		)
	}
	return postgresPGXReplayCatalogRows{rows: rows}, nil
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
