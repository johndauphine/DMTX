package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

// sqliteDeleteJournalTable is deliberately a single private object rather
// than a per-user-table receipt table.  A batch receipt and its DELETE are
// committed in one SQLite transaction, which is the evidence required to
// resolve a lost commit acknowledgement without counting a replay twice.
const sqliteDeleteJournalTable = "dmtx_internal_delete_batch_receipts"

const sqliteDeleteJournalCreateSQL = "CREATE TABLE \"dmtx_internal_delete_batch_receipts\" (\"token\" TEXT NOT NULL PRIMARY KEY, \"plan_id\" TEXT NOT NULL, \"sequence\" INTEGER NOT NULL, \"batch_digest\" TEXT NOT NULL, \"candidates\" INTEGER NOT NULL, \"deleted_rows\" INTEGER NOT NULL, \"receipt_digest\" TEXT NOT NULL)"

type sqliteDeleteSourceCapability struct {
	adapter *sqliteSourceAdapter
	table   schema.Table
}

type sqliteDeleteTargetCapability struct {
	adapter *sqliteTargetAdapter
	table   schema.Table
}

type sqliteDeleteKeyRows struct {
	rows  *sql.Rows
	tx    *sql.Tx
	width int
}

func newSQLiteDeleteReconciliationCapabilities(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
) (postgresDeleteReconciliationCapabilities, error) {
	sourceAdapter, ok := source.(*sqliteSourceAdapter)
	if !ok || sourceAdapter == nil || sourceAdapter.snapshot == nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("delete reconciliation requires a live SQLite source snapshot")
	}
	targetAdapter, ok := target.(*sqliteTargetAdapter)
	if !ok || targetAdapter == nil || targetAdapter.database == nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("delete reconciliation requires a live SQLite target adapter")
	}
	if sourceTable.Schema != "" || targetTable.Schema != "" {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("SQLite delete reconciliation requires unqualified source and target tables")
	}
	if sourceTable.Name == sqliteDeleteJournalTable || targetTable.Name == sqliteDeleteJournalTable {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("SQLite table %s is reserved for DMTX delete receipt evidence", sqliteDeleteJournalTable)
	}
	if err := validateSQLiteDeleteTableAuthority(ctx, sourceAdapter.snapshot, sourceTable); err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("validate SQLite delete source catalog: %w", err)
	}
	if err := validateSQLiteDeleteTableAuthority(ctx, targetAdapter.database, targetTable); err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("validate SQLite delete target catalog: %w", err)
	}
	if err := preflightSQLiteDeleteReceiptJournal(ctx, targetAdapter.database); err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("preflight SQLite delete receipt journal: %w", err)
	}
	if sourceAdapter.database == targetAdapter.database {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf("SQLite delete reconciliation rejects an identical source and target database")
	}
	canonicalizer, err := newSQLiteDeleteKeyCanonicalizer(sourceTable, targetTable)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	return postgresDeleteReconciliationCapabilities{
		source:        &sqliteDeleteSourceCapability{adapter: sourceAdapter, table: sourceTable},
		target:        &sqliteDeleteTargetCapability{adapter: targetAdapter, table: targetTable},
		canonicalizer: canonicalizer,
	}, nil
}

func validateSQLiteDeleteTableAuthority(ctx context.Context, queryer sqliteQueryer, expected schema.Table) error {
	if queryer == nil || expected.Schema != "" || strings.TrimSpace(expected.Name) == "" {
		return fmt.Errorf("SQLite delete catalog identity is incomplete")
	}
	live, _, err := inspectSQLiteSchema(ctx, queryer, expected.Name)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(live, expected) {
		return fmt.Errorf("SQLite delete catalog shape changed after discovery")
	}
	if _, err := deletePrimaryKeyColumns(live); err != nil {
		return err
	}
	return nil
}

func (rows *sqliteDeleteKeyRows) Next() bool {
	return rows != nil && rows.rows != nil && rows.rows.Next()
}

func (rows *sqliteDeleteKeyRows) Values() ([]any, error) {
	if rows == nil || rows.rows == nil || rows.width < 1 {
		return nil, errors.New("SQLite delete key reader is closed")
	}
	values := make([]any, rows.width)
	destinations := make([]any, rows.width)
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.rows.Scan(destinations...); err != nil {
		return nil, err
	}
	return values, nil
}

func (rows *sqliteDeleteKeyRows) Err() error {
	if rows == nil || rows.rows == nil {
		return nil
	}
	return rows.rows.Err()
}

func (rows *sqliteDeleteKeyRows) Close() error {
	if rows == nil {
		return nil
	}
	var result error
	if rows.rows != nil {
		result = rows.rows.Close()
		rows.rows = nil
	}
	if rows.tx != nil {
		err := rows.tx.Rollback()
		if errors.Is(err, sql.ErrTxDone) {
			err = nil
		}
		result = errors.Join(result, err)
		rows.tx = nil
	}
	return result
}

func openSQLiteDeletePrimaryKeys(ctx context.Context, queryer sqliteQueryer, table schema.Table, columns []string) (deleteKeyRows, error) {
	if err := validateSQLiteDeleteTableAuthority(ctx, queryer, table); err != nil {
		return nil, err
	}
	key, err := deletePrimaryKeyColumns(table)
	if err != nil {
		return nil, err
	}
	if len(columns) != len(key) {
		return nil, fmt.Errorf("SQLite delete key request width differs from the primary key")
	}
	for index := range key {
		if columns[index] != key[index].Name {
			return nil, fmt.Errorf("SQLite delete key request is not in exact primary-key order")
		}
	}
	rows, err := queryer.QueryContext(ctx, "SELECT "+quotedColumns(columns)+" FROM "+quote(table.Name)+" ORDER BY "+quotedColumns(columns))
	if err != nil {
		return nil, fmt.Errorf("open SQLite delete primary keys for %s: %w", table.Name, err)
	}
	return &sqliteDeleteKeyRows{rows: rows, width: len(columns)}, nil
}

func (capability *sqliteDeleteSourceCapability) OpenDeletePrimaryKeys(ctx context.Context, table schema.Table, columns []string) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.snapshot == nil || !reflect.DeepEqual(table, capability.table) {
		return nil, errors.New("SQLite delete source authority is unavailable")
	}
	return openSQLiteDeletePrimaryKeys(ctx, capability.adapter.snapshot, table, columns)
}

func (capability *sqliteDeleteTargetCapability) OpenDeletePrimaryKeys(ctx context.Context, table schema.Table, columns []string) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil || !reflect.DeepEqual(table, capability.table) {
		return nil, errors.New("SQLite delete target authority is unavailable")
	}
	return openSQLiteDeletePrimaryKeys(ctx, capability.adapter.database, table, columns)
}

func (*sqliteDeleteTargetCapability) MaxDeleteParameters() int {
	return adapterValidationSQLiteParameterLimit
}

type sqliteDeleteKeyCanonicalizer struct {
	source, target schema.Table
	proof          deleteKeyEqualityProof
}

func newSQLiteDeleteKeyCanonicalizer(source, target schema.Table) (*sqliteDeleteKeyCanonicalizer, error) {
	sourceKey, targetKey, digest, err := adapterValidationCrossEqualityProof(adapterValidationSQLite, adapterValidationSQLite, adapterTablePlan{source: source, target: target})
	if err != nil {
		return nil, fmt.Errorf("certify SQLite delete primary-key equality: %w", err)
	}
	proof := deleteKeyEqualityProof{CanonicalizerID: "sqlite-exact-primary-key-v1:" + digest, Columns: make([]deleteKeyColumnProof, len(sourceKey))}
	proof.SourceFingerprint, err = deleteKeyMetadataFingerprint(source, sourceKey)
	if err != nil {
		return nil, err
	}
	proof.TargetFingerprint, err = deleteKeyMetadataFingerprint(target, targetKey)
	if err != nil {
		return nil, err
	}
	for index, column := range sourceKey {
		kind, kindErr := validationKindForColumn(column)
		if kindErr != nil {
			return nil, kindErr
		}
		switch kind {
		case validationInteger:
			proof.Columns[index].Semantics = "integer"
		case validationBytes:
			proof.Columns[index].Semantics = "binary"
		default:
			return nil, fmt.Errorf("SQLite delete key column %s lacks a certified exact equality domain", column.Name)
		}
	}
	if _, err := validateDeleteKeyEqualityProof(proof, source, target, sourceKey, targetKey); err != nil {
		return nil, err
	}
	return &sqliteDeleteKeyCanonicalizer{source: source, target: target, proof: proof}, nil
}

func (canonicalizer *sqliteDeleteKeyCanonicalizer) ProveDeleteKeyEquality(source, target schema.Table, sourceKey, targetKey []schema.Column) (deleteKeyEqualityProof, error) {
	if canonicalizer == nil || !reflect.DeepEqual(source, canonicalizer.source) || !reflect.DeepEqual(target, canonicalizer.target) {
		return deleteKeyEqualityProof{}, errors.New("SQLite delete key proof was requested for different tables")
	}
	if _, err := validateDeleteKeyEqualityProof(canonicalizer.proof, source, target, sourceKey, targetKey); err != nil {
		return deleteKeyEqualityProof{}, err
	}
	return canonicalizer.proof, nil
}

func (canonicalizer *sqliteDeleteKeyCanonicalizer) CanonicalizeDeleteKeyValue(side deleteKeySide, proof deleteKeyEqualityProof, index int, value any) (deleteCanonicalValue, error) {
	if canonicalizer == nil || !reflect.DeepEqual(proof, canonicalizer.proof) || index < 0 || index >= len(proof.Columns) {
		return deleteCanonicalValue{}, errors.New("SQLite delete key canonicalization proof differs")
	}
	var canonical []byte
	var err error
	switch proof.Columns[index].Semantics {
	case "integer":
		canonical, err = canonicalValidationInteger(value)
	case "binary":
		canonical, err = canonicalValidationBytes(value)
	default:
		err = errors.New("SQLite delete key semantics are unsupported")
	}
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	parameter, err := stableDeleteParameter(value)
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	return deleteCanonicalValue{Canonical: canonical, Parameter: parameter}, nil
}

func preflightSQLiteDeleteReceiptJournal(ctx context.Context, queryer sqliteQueryer) error {
	exists, err := inspectSQLiteDeleteReceiptJournal(ctx, queryer)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return nil
}

// inspectSQLiteDeleteReceiptJournal deliberately checks more than the receipt
// columns. A pre-existing same-name object must not be able to return an
// ambiguous token row after a commit-acknowledgement loss.
func inspectSQLiteDeleteReceiptJournal(ctx context.Context, queryer sqliteQueryer) (bool, error) {
	if queryer == nil {
		return false, errors.New("SQLite delete receipt catalog is unavailable")
	}
	rows, err := queryer.QueryContext(ctx, "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE lower(name) = lower(?)", sqliteDeleteJournalTable)
	if err != nil {
		return false, fmt.Errorf("inspect SQLite delete receipt object: %w", err)
	}
	defer rows.Close()
	type object struct {
		kind, name, table string
		sql               sql.NullString
	}
	var objects []object
	for rows.Next() {
		var item object
		if err := rows.Scan(&item.kind, &item.name, &item.table, &item.sql); err != nil {
			return false, err
		}
		objects = append(objects, item)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(objects) == 0 {
		return false, nil
	}
	if len(objects) != 1 || objects[0].kind != "table" || objects[0].name != sqliteDeleteJournalTable || objects[0].table != sqliteDeleteJournalTable || !objects[0].sql.Valid || objects[0].sql.String != sqliteDeleteJournalCreateSQL {
		return false, errors.New("SQLite delete receipt object collides with or differs from the exact DMTX journal")
	}
	tableInfo, err := queryer.QueryContext(ctx, "PRAGMA table_xinfo("+quote(sqliteDeleteJournalTable)+")")
	if err != nil {
		return false, err
	}
	defer tableInfo.Close()
	expected := []struct {
		name, typ                   string
		notNull, primaryKey, hidden int
	}{
		{"token", "TEXT", 1, 1, 0}, {"plan_id", "TEXT", 1, 0, 0}, {"sequence", "INTEGER", 1, 0, 0},
		{"batch_digest", "TEXT", 1, 0, 0}, {"candidates", "INTEGER", 1, 0, 0}, {"deleted_rows", "INTEGER", 1, 0, 0}, {"receipt_digest", "TEXT", 1, 0, 0},
	}
	index := 0
	for tableInfo.Next() {
		var cid, notNull, primaryKey, hidden int
		var name, typ string
		var defaultValue any
		if err := tableInfo.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			return false, err
		}
		if index >= len(expected) || cid != index || name != expected[index].name || typ != expected[index].typ || notNull != expected[index].notNull || primaryKey != expected[index].primaryKey || hidden != expected[index].hidden || defaultValue != nil {
			return false, errors.New("SQLite delete receipt journal column authority differs")
		}
		index++
	}
	if err := tableInfo.Err(); err != nil {
		return false, err
	}
	if index != len(expected) {
		return false, errors.New("SQLite delete receipt journal column count differs")
	}
	indexes, err := queryer.QueryContext(ctx, "PRAGMA index_list("+quote(sqliteDeleteJournalTable)+")")
	if err != nil {
		return false, err
	}
	defer indexes.Close()
	indexCount := 0
	for indexes.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := indexes.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			return false, err
		}
		if sequence != 0 || unique != 1 || origin != "pk" || partial != 0 {
			return false, errors.New("SQLite delete receipt journal has unexpected index authority")
		}
		indexCount++
	}
	if err := indexes.Err(); err != nil {
		return false, err
	}
	if indexCount != 1 {
		return false, errors.New("SQLite delete receipt journal lacks an exact token primary-key index")
	}
	triggers, err := queryer.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ?", sqliteDeleteJournalTable)
	if err != nil {
		return false, err
	}
	defer triggers.Close()
	if triggers.Next() {
		return false, errors.New("SQLite delete receipt journal has an unexpected trigger")
	}
	if err := triggers.Err(); err != nil {
		return false, err
	}
	return true, nil
}

type sqliteDeleteReceiptMutationConnection interface {
	sqliteQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func ensureSQLiteDeleteReceiptJournal(ctx context.Context, connection sqliteDeleteReceiptMutationConnection) error {
	if connection == nil {
		return errors.New("SQLite delete receipt mutation connection is unavailable")
	}
	exists, err := inspectSQLiteDeleteReceiptJournal(ctx, connection)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := connection.ExecContext(ctx, sqliteDeleteJournalCreateSQL); err != nil {
			return fmt.Errorf("create SQLite delete receipt journal: %w", err)
		}
		if _, err := inspectSQLiteDeleteReceiptJournal(ctx, connection); err != nil {
			return fmt.Errorf("verify created SQLite delete receipt journal: %w", err)
		}
	}
	return nil
}

func sqliteDeleteReceiptDigest(receipt deleteTargetBatchReceipt) (string, error) {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func loadSQLiteDeleteReceipt(
	ctx context.Context,
	queryer sqliteQueryer,
	token string,
) (deleteTargetBatchReceipt, bool, error) {
	if queryer == nil || strings.TrimSpace(token) == "" {
		return deleteTargetBatchReceipt{}, false, errors.New("SQLite delete receipt lookup authority is unavailable")
	}
	var stored deleteTargetBatchReceipt
	err := queryer.QueryRowContext(ctx, "SELECT plan_id, sequence, batch_digest, candidates, deleted_rows, receipt_digest FROM "+quote(sqliteDeleteJournalTable)+" WHERE token = ?", token).Scan(&stored.PlanID, &stored.Sequence, &stored.BatchDigest, &stored.Candidates, &stored.DeletedRows, &stored.ReceiptDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return deleteTargetBatchReceipt{}, false, nil
	}
	if err != nil {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf("load SQLite delete receipt: %w", err)
	}
	stored.Token = token
	return stored, true, nil
}

func sqliteDeleteDetachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
}

func rollbackSQLiteDeleteReceiptTransaction(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := sqliteDeleteDetachedContext(ctx)
	defer cancel()
	_, err := connection.ExecContext(cleanupCtx, "ROLLBACK")
	return err
}

func discardSQLiteDeleteReceiptConnection(connection *sql.Conn) {
	if connection == nil {
		return
	}
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
}

func (adapter *sqliteTargetAdapter) commitSQLiteDeleteReceipt(
	ctx context.Context,
	connection *sql.Conn,
) (sql.Result, error) {
	if adapter != nil && adapter.deleteCommit != nil {
		return adapter.deleteCommit(ctx, connection)
	}
	return connection.ExecContext(ctx, "COMMIT")
}

func (capability *sqliteDeleteTargetCapability) classifySQLiteDeleteCommitAmbiguity(
	ctx context.Context,
	connection *sql.Conn,
	closed *bool,
	batch deleteTargetBatch,
	commitErr error,
) (deleteTargetBatchReceipt, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil || connection == nil || closed == nil {
		return deleteTargetBatchReceipt{}, errors.Join(commitErr, errors.New("SQLite delete commit ambiguity authority is unavailable"))
	}
	discardSQLiteDeleteReceiptConnection(connection)
	closeErr := connection.Close()
	*closed = true
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	verificationCtx, cancel := sqliteDeleteDetachedContext(ctx)
	defer cancel()
	verifyErr := validateSQLiteDeleteTableAuthority(
		verificationCtx,
		capability.adapter.database,
		capability.table,
	)
	if verifyErr == nil {
		var exists bool
		exists, verifyErr = inspectSQLiteDeleteReceiptJournal(
			verificationCtx,
			capability.adapter.database,
		)
		if verifyErr == nil && !exists {
			verifyErr = errors.New("SQLite delete receipt journal is absent after commit acknowledgement failure")
		}
	}
	var stored deleteTargetBatchReceipt
	var found bool
	if verifyErr == nil {
		stored, found, verifyErr = loadSQLiteDeleteReceipt(
			verificationCtx,
			capability.adapter.database,
			batch.Token,
		)
		if verifyErr == nil && !found {
			verifyErr = errors.New("SQLite delete receipt is absent after commit acknowledgement failure")
		}
	}
	if verifyErr == nil {
		verifyErr = validateSQLiteDeleteReceipt(batch, stored)
	}
	if verifyErr == nil && closeErr == nil {
		return stored, nil
	}
	return deleteTargetBatchReceipt{}, errors.Join(
		fmt.Errorf("SQLite delete commit outcome is unknown; resume the existing run with the same pending batch token: %w", commitErr),
		closeErr,
		verifyErr,
	)
}

func (capability *sqliteDeleteTargetCapability) ApplyDeleteBatch(ctx context.Context, batch deleteTargetBatch) (result deleteTargetBatchReceipt, resultErr error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil {
		return result, errors.New("SQLite delete target is unavailable")
	}
	if !reflect.DeepEqual(batch.Table, capability.table) {
		return result, errors.New("SQLite delete batch table differs from admitted authority")
	}
	keys, err := validateSQLiteDeleteBatch(batch)
	if err != nil {
		return result, err
	}
	connection, err := capability.adapter.database.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire pinned SQLite delete receipt connection: %w", err)
	}
	active := false
	closed := false
	defer func() {
		if active {
			if rollbackErr := rollbackSQLiteDeleteReceiptTransaction(ctx, connection); rollbackErr != nil {
				discardSQLiteDeleteReceiptConnection(connection)
				result = deleteTargetBatchReceipt{}
				resultErr = errors.Join(resultErr, fmt.Errorf("roll back SQLite delete receipt transaction: %w", rollbackErr))
			}
		}
		if !closed {
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
				result = deleteTargetBatchReceipt{}
				resultErr = errors.Join(resultErr, fmt.Errorf("close pinned SQLite delete receipt connection: %w", closeErr))
			}
		}
	}()
	if _, err := connection.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return result, fmt.Errorf("acquire SQLite BEGIN IMMEDIATE delete writer reservation: %w", err)
	}
	active = true
	if err := validateSQLiteDeleteTableAuthority(ctx, connection, capability.table); err != nil {
		return result, fmt.Errorf("revalidate writer-reserved SQLite delete target catalog: %w", err)
	}
	if err := ensureSQLiteDeleteReceiptJournal(ctx, connection); err != nil {
		return result, err
	}
	if capability.adapter.deleteAfterReservation != nil {
		if err := capability.adapter.deleteAfterReservation(ctx, connection); err != nil {
			return result, fmt.Errorf("exercise SQLite delete writer reservation: %w", err)
		}
	}
	stored, found, err := loadSQLiteDeleteReceipt(ctx, connection, batch.Token)
	if err != nil {
		return result, err
	}
	if found {
		if err := validateSQLiteDeleteReceipt(batch, stored); err != nil {
			return result, err
		}
		if _, commitErr := capability.adapter.commitSQLiteDeleteReceipt(ctx, connection); commitErr != nil {
			active = false
			return capability.classifySQLiteDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
		}
		active = false
		return stored, nil
	}
	predicate, args, err := sqliteDeletePredicate(batch.Columns, keys)
	if err != nil {
		return result, err
	}
	deleteResult, err := connection.ExecContext(ctx, "DELETE FROM "+quote(batch.Table.Name)+" WHERE "+predicate, args...)
	if err != nil {
		return result, fmt.Errorf("SQLite delete batch rolled back with no receipt: %w", err)
	}
	deleted, err := deleteResult.RowsAffected()
	if err != nil || deleted < 0 || deleted > int64(len(keys)) {
		return result, fmt.Errorf("SQLite delete batch returned unsafe affected-row count: affected=%d err=%w", deleted, err)
	}
	receipt := deleteTargetBatchReceipt{PlanID: batch.PlanID, Token: batch.Token, Sequence: batch.Sequence, BatchDigest: batch.BatchDigest, Candidates: int64(len(keys)), DeletedRows: deleted}
	receipt.ReceiptDigest, err = sqliteDeleteReceiptDigest(receipt)
	if err != nil {
		return result, err
	}
	if _, err := connection.ExecContext(ctx, "INSERT INTO "+quote(sqliteDeleteJournalTable)+" (token, plan_id, sequence, batch_digest, candidates, deleted_rows, receipt_digest) VALUES (?, ?, ?, ?, ?, ?, ?)", receipt.Token, receipt.PlanID, receipt.Sequence, receipt.BatchDigest, receipt.Candidates, receipt.DeletedRows, receipt.ReceiptDigest); err != nil {
		return result, fmt.Errorf("persist SQLite delete receipt: %w", err)
	}
	if _, commitErr := capability.adapter.commitSQLiteDeleteReceipt(ctx, connection); commitErr != nil {
		active = false
		return capability.classifySQLiteDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
	}
	active = false
	return receipt, nil
}

func sqliteDeletePredicate(columns []string, keys [][]driver.Value) (string, []any, error) {
	if len(columns) == 0 || len(keys) == 0 {
		return "", nil, errors.New("SQLite delete predicate is empty")
	}
	clauses := make([]string, len(keys))
	args := make([]any, 0, len(keys)*len(columns))
	for keyIndex, key := range keys {
		if len(key) != len(columns) {
			return "", nil, fmt.Errorf("SQLite delete key %d has invalid width", keyIndex)
		}
		terms := make([]string, len(columns))
		for index := range columns {
			if key[index] == nil {
				return "", nil, errors.New("SQLite delete key contains NULL")
			}
			terms[index] = quote(columns[index]) + " = ?"
			args = append(args, key[index])
		}
		clauses[keyIndex] = "(" + strings.Join(terms, " AND ") + ")"
	}
	return "(" + strings.Join(clauses, " OR ") + ")", args, nil
}

func validateSQLiteDeleteBatch(batch deleteTargetBatch) ([][]driver.Value, error) {
	if batch.Table.Schema != "" || strings.TrimSpace(batch.Table.Name) == "" ||
		len(batch.Columns) == 0 || len(batch.Keys) == 0 ||
		strings.TrimSpace(batch.PlanID) == "" || batch.Sequence < 0 {
		return nil, errors.New("SQLite delete batch identity is incomplete")
	}
	if _, err := hex.DecodeString(batch.PlanID); err != nil || len(batch.PlanID) != 32 || batch.PlanID != strings.ToLower(batch.PlanID) {
		return nil, errors.New("SQLite delete batch plan ID must be 32 lowercase hexadecimal characters")
	}
	if err := validateLowerSHA256("SQLite delete batch token", batch.Token); err != nil {
		return nil, err
	}
	if err := validateLowerSHA256("SQLite delete batch digest", batch.BatchDigest); err != nil {
		return nil, err
	}
	if len(batch.Keys) > adapterValidationSQLiteParameterLimit/len(batch.Columns) {
		return nil, errors.New("SQLite delete batch exceeds the parameter limit")
	}
	result := make([][]driver.Value, len(batch.Keys))
	for rowIndex, key := range batch.Keys {
		if len(key) != len(batch.Columns) {
			return nil, fmt.Errorf("SQLite delete key %d width differs", rowIndex)
		}
		result[rowIndex] = make([]driver.Value, len(key))
		for columnIndex, value := range key {
			stable, err := stableDeleteParameter(value)
			if err != nil || stable == nil {
				return nil, fmt.Errorf("SQLite delete key %d column %d is not parameter-safe", rowIndex, columnIndex)
			}
			result[rowIndex][columnIndex] = stable
		}
	}
	return result, nil
}

func validateSQLiteDeleteReceipt(batch deleteTargetBatch, receipt deleteTargetBatchReceipt) error {
	if receipt.PlanID != batch.PlanID || receipt.Token != batch.Token ||
		receipt.Sequence != batch.Sequence || receipt.BatchDigest != batch.BatchDigest ||
		receipt.Candidates != int64(len(batch.Keys)) || receipt.DeletedRows < 0 || receipt.DeletedRows > receipt.Candidates || receipt.FailClosedReason != "" {
		return errors.New("SQLite delete receipt differs from the pending batch")
	}
	digest, err := sqliteDeleteReceiptDigest(deleteTargetBatchReceipt{PlanID: receipt.PlanID, Token: receipt.Token, Sequence: receipt.Sequence, BatchDigest: receipt.BatchDigest, Candidates: receipt.Candidates, DeletedRows: receipt.DeletedRows})
	if err != nil || receipt.ReceiptDigest != digest {
		return errors.New("SQLite delete receipt digest differs from the durable receipt")
	}
	return nil
}

var _ deleteKeySource = (*sqliteDeleteSourceCapability)(nil)
var _ deleteKeyTarget = (*sqliteDeleteTargetCapability)(nil)
var _ deleteKeyCanonicalizer = (*sqliteDeleteKeyCanonicalizer)(nil)
