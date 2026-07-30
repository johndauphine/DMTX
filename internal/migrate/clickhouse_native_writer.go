package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type clickHouseBatchWriter interface {
	WriteBatch(
		context.Context,
		schema.Table,
		[]string,
		string,
		[][]any,
	) (WriteReceipt, error)
}

type clickHouseTransactionProvider interface {
	Begin(context.Context) (clickHouseBatchTransaction, error)
}

type clickHouseBatchTransaction interface {
	Prepare(context.Context, string) (clickHouseBatchStatement, error)
	Commit() error
	Rollback() error
}

type clickHouseBatchStatement interface {
	Append(context.Context, []any) error
	Close() error
}

// clickHouseSafeOperationError retains a driver cause for errors.Is/errors.As
// without including driver text, which can contain connection or row data, in
// operator output.
type clickHouseSafeOperationError struct {
	operation string
	table     string
	cause     error
}

func (operationError *clickHouseSafeOperationError) Error() string {
	return operationError.operation + " " + operationError.table
}

func (operationError *clickHouseSafeOperationError) Unwrap() error {
	return operationError.cause
}

func newClickHouseSafeOperationError(
	operation string,
	table string,
	cause error,
) error {
	var safeError *clickHouseSafeOperationError
	if errors.As(cause, &safeError) {
		return safeError
	}
	return &clickHouseSafeOperationError{
		operation: operation,
		table:     table,
		cause:     cause,
	}
}

type clickHouseNativeWriter struct {
	transactions clickHouseTransactionProvider
}

func newClickHouseNativeWriter(
	database *sql.DB,
) *clickHouseNativeWriter {
	return &clickHouseNativeWriter{
		transactions: clickHouseSQLTransactionProvider{
			database: database,
		},
	}
}

func (writer *clickHouseNativeWriter) WriteBatch(
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
	if err := validateClickHouseWriteShape(
		table,
		columns,
		mode,
	); err != nil {
		return notCommitted, err
	}
	if err := validateClickHouseRows(table, columns, rows); err != nil {
		return notCommitted, err
	}
	if len(rows) == 0 {
		return WriteReceipt{Certainty: CommitDurable}, nil
	}
	if writer == nil || writer.transactions == nil {
		return notCommitted, fmt.Errorf(
			"write ClickHouse table %s: transaction provider is not configured",
			table.Name,
		)
	}

	transaction, err := writer.transactions.Begin(ctx)
	if err != nil {
		return notCommitted, newClickHouseSafeOperationError(
			"begin ClickHouse write for",
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
		clickHouseInsertStatement(table, columns),
	)
	if err != nil {
		return notCommitted, newClickHouseSafeOperationError(
			"prepare ClickHouse write for",
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
		if err := statement.Append(ctx, row); err != nil {
			return notCommitted, newClickHouseSafeOperationError(
				"append ClickHouse table",
				table.Name,
				err,
			)
		}
	}
	if err := statement.Close(); err != nil {
		return notCommitted, newClickHouseSafeOperationError(
			"close ClickHouse write statement for",
			table.Name,
			err,
		)
	}
	statementClosed = true

	if err := transaction.Commit(); err != nil {
		return WriteReceipt{
				Certainty:     CommitUnknown,
				AttemptedRows: attempted,
			}, newClickHouseSafeOperationError(
				"commit ClickHouse table",
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

func validateClickHouseWriteShape(
	table schema.Table,
	columns []string,
	mode string,
) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf(
			"ClickHouse target database and table name are required",
		)
	}
	if mode != "drop_recreate" {
		return fmt.Errorf(
			"write ClickHouse table %s: unsupported target mode %q",
			table.Name,
			mode,
		)
	}
	if len(columns) == 0 {
		return fmt.Errorf(
			"write ClickHouse table %s: at least one column is required",
			table.Name,
		)
	}
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return fmt.Errorf(
				"write ClickHouse table %s: schema contains an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return fmt.Errorf(
				"write ClickHouse table %s: schema contains duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "bigint", "double", "text", "blob":
		default:
			return clickHouseTargetPolicy(
				"write target type",
				table.Name+"."+column.Name+" "+column.Type,
			)
		}
		metadata[column.Name] = column
	}
	if len(columns) != len(table.Columns) {
		return fmt.Errorf(
			"write ClickHouse table %s: requested %d columns for %d-column schema",
			table.Name,
			len(columns),
			len(table.Columns),
		)
	}
	requested := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if _, exists := metadata[name]; !exists {
			return fmt.Errorf(
				"write ClickHouse table %s: column %s is absent from schema",
				table.Name,
				name,
			)
		}
		if _, duplicate := requested[name]; duplicate {
			return fmt.Errorf(
				"write ClickHouse table %s: requested duplicate column %s",
				table.Name,
				name,
			)
		}
		requested[name] = struct{}{}
	}
	return nil
}

func validateClickHouseRows(
	table schema.Table,
	columns []string,
	rows [][]any,
) error {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	for rowIndex, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf(
				"write ClickHouse table %s: row %d has %d values for %d columns",
				table.Name,
				rowIndex,
				len(row),
				len(columns),
			)
		}
		for columnIndex, value := range row {
			column := metadata[columns[columnIndex]]
			if value == nil {
				if !column.Nullable {
					return fmt.Errorf(
						"write ClickHouse table %s: row %d column %s is NULL but not nullable",
						table.Name,
						rowIndex,
						column.Name,
					)
				}
				continue
			}
			valid := false
			switch strings.ToLower(strings.TrimSpace(column.Type)) {
			case "bigint":
				_, valid = value.(int64)
			case "double":
				_, valid = value.(float64)
			case "text":
				_, valid = value.(string)
			case "blob":
				_, valid = value.([]byte)
			}
			if !valid {
				return fmt.Errorf(
					"write ClickHouse table %s: row %d column %s has unsupported Go type %s",
					table.Name,
					rowIndex,
					column.Name,
					reflect.TypeOf(value),
				)
			}
		}
	}
	return nil
}

func clickHouseInsertStatement(
	table schema.Table,
	columns []string,
) string {
	return "INSERT INTO " +
		clickHouseQualified(table.Schema, table.Name) +
		" (" + quotedColumns(columns) + ") VALUES (" +
		placeholders(len(columns)) + ")"
}

type clickHouseSQLTransactionProvider struct {
	database *sql.DB
}

func (provider clickHouseSQLTransactionProvider) Begin(
	ctx context.Context,
) (clickHouseBatchTransaction, error) {
	transaction, err := provider.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return clickHouseSQLBatchTransaction{transaction: transaction}, nil
}

type clickHouseSQLBatchTransaction struct {
	transaction *sql.Tx
}

func (transaction clickHouseSQLBatchTransaction) Prepare(
	ctx context.Context,
	query string,
) (clickHouseBatchStatement, error) {
	statement, err := transaction.transaction.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return clickHouseSQLBatchStatement{statement: statement}, nil
}

func (transaction clickHouseSQLBatchTransaction) Commit() error {
	return transaction.transaction.Commit()
}

func (transaction clickHouseSQLBatchTransaction) Rollback() error {
	return transaction.transaction.Rollback()
}

type clickHouseSQLBatchStatement struct {
	statement *sql.Stmt
}

func (statement clickHouseSQLBatchStatement) Append(
	ctx context.Context,
	values []any,
) error {
	_, err := statement.statement.ExecContext(ctx, values...)
	return err
}

func (statement clickHouseSQLBatchStatement) Close() error {
	return statement.statement.Close()
}
