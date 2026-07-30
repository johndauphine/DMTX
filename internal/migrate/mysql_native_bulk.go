package migrate

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmtx/internal/schema"
)

type mysqlLocalInfileState uint8

const (
	mysqlLocalInfileUnknown mysqlLocalInfileState = iota
	mysqlLocalInfileEnabled
	mysqlLocalInfileFallback
)

const mysqlLocalInfileFallbackWarning = "warning: MySQL-family " +
	"LOAD DATA LOCAL INFILE is unavailable; using strict bounded inserts"
const mysqlLocalInfileUpsertFallbackWarning = "warning: MySQL-family " +
	"upsert uses strict bounded inserts because LOAD DATA cannot preserve " +
	"guarded conflict semantics"

type mysqlLocalInfileRequest struct {
	statement func(string) (string, error)
	reader    func() io.Reader
}

func defaultMySQLNativeWriterWarning(message string) {
	log.Print(message)
}

func (writer *mysqlNativeWriter) writeMySQLLocalInfileBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, bool, error) {
	attempted := int64(len(rows))
	notCommitted := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	transaction, err := writer.transactions.Begin(ctx)
	if err != nil {
		return notCommitted, false, newMySQLSafeOperationError(
			"begin MySQL bulk write for",
			table.Name,
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			_ = transaction.Rollback()
		}
	}()

	enabled, err := transaction.LocalInfileEnabled(ctx)
	if err != nil {
		return notCommitted, false, newMySQLSafeOperationError(
			"inspect MySQL local infile setting for",
			table.Name,
			err,
		)
	}
	if !enabled {
		if err := transaction.Rollback(); err != nil {
			closed = true
			return notCommitted, false, newMySQLSafeOperationError(
				"close disabled MySQL bulk write for",
				table.Name,
				err,
			)
		}
		closed = true
		writer.useMySQLStrictInsertFallback(
			mysqlLocalInfileFallbackWarning,
		)
		return notCommitted, true, nil
	}

	stagingName, err := newMySQLLocalInfileObjectName("dmtx_load_")
	if err != nil {
		return notCommitted, false, fmt.Errorf(
			"prepare MySQL bulk write for table %s: %w",
			table.Name,
			err,
		)
	}
	if _, err := transaction.Execute(
		ctx,
		mySQLCreateLocalInfileStagingStatement(table, stagingName),
	); err != nil {
		cleanupErr := cleanupMySQLLocalInfileTransaction(
			ctx,
			transaction,
			stagingName,
		)
		closed = true
		if cleanupErr != nil {
			return notCommitted, false, newMySQLSafeOperationError(
				"clean up MySQL bulk staging for",
				table.Name,
				errors.Join(err, cleanupErr),
			)
		}
		if isMySQLLocalInfileSetupUnavailable(err) {
			writer.useMySQLStrictInsertFallback(
				mysqlLocalInfileFallbackWarning,
			)
			return notCommitted, true, nil
		}
		return notCommitted, false, newMySQLSafeOperationError(
			"create MySQL bulk staging for",
			table.Name,
			err,
		)
	}

	loaded, err := transaction.LoadLocalInfile(
		ctx,
		newMySQLLocalInfileRequest(
			mySQLIdentifier(stagingName),
			columns,
			rows,
		),
	)
	if err != nil {
		cleanupErr := cleanupMySQLLocalInfileTransaction(
			ctx,
			transaction,
			stagingName,
		)
		closed = true
		if cleanupErr != nil {
			return notCommitted, false, newMySQLSafeOperationError(
				"clean up MySQL bulk staging for",
				table.Name,
				errors.Join(err, cleanupErr),
			)
		}
		if isMySQLLocalInfileUnavailable(err) {
			writer.useMySQLStrictInsertFallback(
				mysqlLocalInfileFallbackWarning,
			)
			return notCommitted, true, nil
		}
		return notCommitted, false, newMySQLSafeOperationError(
			"load MySQL bulk staging for",
			table.Name,
			err,
		)
	}
	if loaded != attempted {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			false,
			fmt.Errorf(
				"bulk load staged %d rows; expected exactly %d",
				loaded,
				attempted,
			),
			&closed,
		)
	}
	warnings, err := transaction.WarningCount(ctx)
	if err != nil {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			false,
			newMySQLSafeOperationError(
				"inspect MySQL bulk load warnings for",
				table.Name,
				err,
			),
			&closed,
		)
	}
	if warnings != 0 {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			false,
			fmt.Errorf(
				"bulk load MySQL table %s produced %d conversion warnings",
				table.Name,
				warnings,
			),
			&closed,
		)
	}
	staged, err := transaction.Count(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(stagingName),
	)
	if err != nil {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			false,
			newMySQLSafeOperationError(
				"count MySQL bulk staging for",
				table.Name,
				err,
			),
			&closed,
		)
	}
	if staged != attempted {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			false,
			fmt.Errorf(
				"bulk staging contains %d rows; expected exactly %d",
				staged,
				attempted,
			),
			&closed,
		)
	}
	writer.localInfile = mysqlLocalInfileEnabled

	affected, err := transaction.Execute(
		ctx,
		mySQLMergeLocalInfileStagingStatement(
			table,
			stagingName,
			columns,
		),
	)
	if err != nil {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			true,
			newMySQLSafeOperationError(
				"merge MySQL bulk staging for",
				table.Name,
				err,
			),
			&closed,
		)
	}
	if affected != attempted {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			true,
			fmt.Errorf(
				"bulk merge affected %d rows; expected exactly %d",
				affected,
				attempted,
			),
			&closed,
		)
	}
	warnings, err = transaction.WarningCount(ctx)
	if err != nil {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			true,
			newMySQLSafeOperationError(
				"inspect MySQL bulk merge warnings for",
				table.Name,
				err,
			),
			&closed,
		)
	}
	if warnings != 0 {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			true,
			fmt.Errorf(
				"bulk merge MySQL table %s produced %d conversion warnings",
				table.Name,
				warnings,
			),
			&closed,
		)
	}
	if _, err := transaction.Execute(
		ctx,
		mySQLDropLocalInfileStagingStatement(stagingName),
	); err != nil {
		return writer.failMySQLStagedBulkWrite(
			ctx,
			transaction,
			table,
			stagingName,
			attempted,
			true,
			newMySQLSafeOperationError(
				"drop MySQL bulk staging for",
				table.Name,
				err,
			),
			&closed,
		)
	}
	err = transaction.Commit()
	closed = true
	if err != nil {
		return WriteReceipt{
				Certainty:     CommitUnknown,
				AttemptedRows: attempted,
			}, false, newMySQLSafeOperationError(
				"commit MySQL bulk write for",
				table.Name,
				err,
			)
	}
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: attempted,
		CommittedRows: attempted,
	}, false, nil
}

func (writer *mysqlNativeWriter) failMySQLStagedBulkWrite(
	ctx context.Context,
	transaction mysqlBatchTransaction,
	table schema.Table,
	stagingName string,
	attempted int64,
	targetTouched bool,
	cause error,
	closed *bool,
) (WriteReceipt, bool, error) {
	if err := cleanupMySQLLocalInfileTransaction(
		ctx,
		transaction,
		stagingName,
	); err != nil {
		*closed = true
		certainty := CommitNotCommitted
		if targetTouched {
			certainty = CommitUnknown
		}
		return WriteReceipt{
				Certainty:     certainty,
				AttemptedRows: attempted,
			}, false, newMySQLSafeOperationError(
				"clean up MySQL bulk write for",
				table.Name,
				errors.Join(cause, err),
			)
	}
	*closed = true
	return WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}, false, cause
}

func cleanupMySQLLocalInfileTransaction(
	ctx context.Context,
	transaction mysqlBatchTransaction,
	stagingName string,
) error {
	cleanupContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		mysqlTargetCleanupTimeout,
	)
	defer cancel()
	var cleanupErr error
	if stagingName != "" {
		if _, err := transaction.Execute(
			cleanupContext,
			mySQLDropLocalInfileStagingStatement(stagingName),
		); err != nil {
			cleanupErr = err
		}
	}
	if err := transaction.Rollback(); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	return cleanupErr
}

func (writer *mysqlNativeWriter) useMySQLStrictInsertFallback(
	warning string,
) {
	if writer.localInfile == mysqlLocalInfileFallback {
		return
	}
	writer.localInfile = mysqlLocalInfileFallback
	if writer.warn != nil {
		writer.warn(warning)
	}
}

func isMySQLLocalInfileSetupUnavailable(err error) bool {
	if isMySQLLocalInfileUnavailable(err) {
		return true
	}
	var serverError *mysqlDriver.MySQLError
	if !errors.As(err, &serverError) {
		return false
	}
	switch serverError.Number {
	case 1044, 1142:
		return true
	default:
		return false
	}
}

func isMySQLLocalInfileUnavailable(err error) bool {
	var serverError *mysqlDriver.MySQLError
	if !errors.As(err, &serverError) {
		return false
	}
	switch serverError.Number {
	case 1148, 3948, 4166:
		return true
	default:
		return false
	}
}

func newMySQLLocalInfileObjectName(prefix string) (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate MySQL local infile object name")
	}
	return prefix + hex.EncodeToString(entropy[:]), nil
}

func mySQLCreateLocalInfileStagingStatement(
	table schema.Table,
	stagingName string,
) string {
	return "CREATE TEMPORARY TABLE " + mySQLIdentifier(stagingName) +
		" LIKE " + mySQLQualified(table.Schema, table.Name)
}

func mySQLDropLocalInfileStagingStatement(stagingName string) string {
	return "DROP TEMPORARY TABLE IF EXISTS " +
		mySQLIdentifier(stagingName)
}

func mySQLMergeLocalInfileStagingStatement(
	table schema.Table,
	stagingName string,
	columns []string,
) string {
	quoted := mySQLQuotedColumns(columns)
	return "INSERT INTO " + mySQLQualified(table.Schema, table.Name) +
		" (" + quoted + ") SELECT " + quoted +
		" FROM " + mySQLIdentifier(stagingName)
}

func newMySQLLocalInfileRequest(
	target string,
	columns []string,
	rows [][]any,
) mysqlLocalInfileRequest {
	return mysqlLocalInfileRequest{
		statement: func(readerName string) (string, error) {
			return mySQLLocalInfileStatementForTarget(
				target,
				columns,
				readerName,
			)
		},
		reader: func() io.Reader {
			return &mysqlLocalInfileReader{rows: rows}
		},
	}
}

func mySQLLocalInfileStatement(
	table schema.Table,
	columns []string,
	readerName string,
) (string, error) {
	return mySQLLocalInfileStatementForTarget(
		mySQLQualified(table.Schema, table.Name),
		columns,
		readerName,
	)
}

func mySQLLocalInfileStatementForTarget(
	target string,
	columns []string,
	readerName string,
) (string, error) {
	if !validMySQLLocalInfileReaderName(readerName) {
		return "", fmt.Errorf("invalid MySQL local infile reader name")
	}
	variables := make([]string, len(columns))
	assignments := make([]string, len(columns))
	for index, column := range columns {
		variables[index] = fmt.Sprintf("@dmtx_%d", index)
		assignments[index] = mySQLIdentifier(column) +
			" = IF(" + variables[index] + " = 'N', NULL, " +
			"UNHEX(SUBSTRING(" + variables[index] + ", 2)))"
	}
	return "LOAD DATA LOCAL INFILE 'Reader::" + readerName + "' INTO TABLE " +
		target +
		" CHARACTER SET binary FIELDS TERMINATED BY '\\t' ESCAPED BY '' " +
		"LINES TERMINATED BY '\\n' (" +
		strings.Join(variables, ", ") + ") SET " +
		strings.Join(assignments, ", "), nil
}

func validMySQLLocalInfileReaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character != '_' &&
			(character < '0' || character > '9') &&
			(character < 'a' || character > 'z') {
			return false
		}
	}
	return true
}

func normalizeMySQLLocalInfileRows(rows [][]any) ([][]any, error) {
	normalized := make([][]any, len(rows))
	for rowIndex, row := range rows {
		normalized[rowIndex] = make([]any, len(row))
		for columnIndex, value := range row {
			converted, err := normalizeMySQLLocalInfileValue(value)
			if err != nil {
				return nil, fmt.Errorf(
					"row %d column %d has unsupported value type",
					rowIndex,
					columnIndex,
				)
			}
			if err := validateMySQLLocalInfileValue(converted); err != nil {
				return nil, fmt.Errorf(
					"row %d column %d: %w",
					rowIndex,
					columnIndex,
					err,
				)
			}
			normalized[rowIndex][columnIndex] = converted
		}
	}
	return normalized, nil
}

func normalizeMySQLLocalInfileValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() == reflect.Ptr && reflected.IsNil() {
		return nil, nil
	}
	if valuer, ok := value.(driver.Valuer); ok {
		converted, err := valuer.Value()
		if err != nil {
			return nil, err
		}
		if unsigned, ok := converted.(uint64); ok {
			return unsigned, nil
		}
		return driver.DefaultParameterConverter.ConvertValue(converted)
	}
	switch reflected.Kind() {
	case reflect.Ptr:
		return normalizeMySQLLocalInfileValue(
			reflected.Elem().Interface(),
		)
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return reflected.Uint(), nil
	}
	return driver.DefaultParameterConverter.ConvertValue(value)
}

func validateMySQLLocalInfileValue(value any) error {
	switch value := value.(type) {
	case nil, int64, uint64, float64, bool, string:
		return nil
	case []byte:
		return nil
	case time.Time:
		if value.IsZero() {
			return nil
		}
		year := value.In(time.UTC).Year()
		if year < 1 || year > 9999 {
			return fmt.Errorf("time year is outside MySQL range")
		}
		return nil
	default:
		return fmt.Errorf("unsupported normalized value type %T", value)
	}
}

type mysqlLocalInfileReader struct {
	rows    [][]any
	index   int
	pending []byte
}

func (reader *mysqlLocalInfileReader) Read(target []byte) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(target) {
		if len(reader.pending) == 0 {
			if reader.index == len(reader.rows) {
				if written == 0 {
					return 0, io.EOF
				}
				return written, nil
			}
			reader.pending = appendMySQLLocalInfileRow(
				reader.pending[:0],
				reader.rows[reader.index],
			)
			reader.index++
		}
		copied := copy(target[written:], reader.pending)
		written += copied
		reader.pending = reader.pending[copied:]
	}
	return written, nil
}

func appendMySQLLocalInfileRow(target []byte, row []any) []byte {
	for index, value := range row {
		if index > 0 {
			target = append(target, '\t')
		}
		target = appendMySQLLocalInfileValue(target, value)
	}
	return append(target, '\n')
}

func appendMySQLLocalInfileValue(target []byte, value any) []byte {
	if value == nil {
		return append(target, 'N')
	}
	if bytes, ok := value.([]byte); ok && bytes == nil {
		return append(target, 'N')
	}
	target = append(target, 'H')
	switch value := value.(type) {
	case int64:
		text := strconv.AppendInt(nil, value, 10)
		return hex.AppendEncode(target, text)
	case uint64:
		text := strconv.AppendUint(nil, value, 10)
		return hex.AppendEncode(target, text)
	case float64:
		text := strconv.AppendFloat(nil, value, 'g', -1, 64)
		return hex.AppendEncode(target, text)
	case bool:
		if value {
			return append(target, '3', '1')
		}
		return append(target, '3', '0')
	case []byte:
		return hex.AppendEncode(target, value)
	case string:
		return hex.AppendEncode(target, []byte(value))
	case time.Time:
		return hex.AppendEncode(
			target,
			[]byte(formatMySQLLocalInfileTime(value)),
		)
	default:
		panic(fmt.Sprintf(
			"unvalidated MySQL local infile value type %T",
			value,
		))
	}
}

func formatMySQLLocalInfileTime(value time.Time) string {
	if value.IsZero() {
		return "0000-00-00"
	}
	value = value.In(time.UTC)
	if value.Hour() == 0 &&
		value.Minute() == 0 &&
		value.Second() == 0 &&
		value.Nanosecond() == 0 {
		return value.Format("2006-01-02")
	}
	return value.Format("2006-01-02 15:04:05.999999999")
}

func (transaction mysqlSQLBatchTransaction) LoadLocalInfile(
	ctx context.Context,
	request mysqlLocalInfileRequest,
) (int64, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return 0, fmt.Errorf("generate MySQL local infile reader name")
	}
	name := "dmtx_" + hex.EncodeToString(entropy[:])
	statement, err := request.statement(name)
	if err != nil {
		return 0, err
	}
	if request.reader == nil {
		return 0, fmt.Errorf("MySQL local infile reader is required")
	}
	var opened atomic.Bool
	mysqlDriver.RegisterReaderHandler(name, func() io.Reader {
		if !opened.CompareAndSwap(false, true) {
			return nil
		}
		return request.reader()
	})
	defer mysqlDriver.DeregisterReaderHandler(name)

	result, err := transaction.transaction.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
