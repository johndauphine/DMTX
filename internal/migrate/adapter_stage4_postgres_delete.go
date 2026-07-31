package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	postgresDeleteMaximumParameters = 65535
	postgresDeleteMaximumBatchBytes = 64 << 20
)

// postgresDeleteReconciliationCapabilities is deliberately not part of the
// general adapter contracts yet. Stage 4 composition can request this
// PostgreSQL-only capability after source/target discovery without making
// unsupported routes appear delete-capable.
type postgresDeleteReconciliationCapabilities struct {
	source        deleteKeySource
	target        deleteKeyTarget
	canonicalizer deleteKeyCanonicalizer
}

type postgresDeleteSourceCapability struct {
	adapter   *relationalSourceAdapter
	authority postgresDeleteCatalogAuthority
}

type postgresDeleteTargetCapability struct {
	adapter   *postgresTargetAdapter
	authority postgresDeleteCatalogAuthority
}

func newPostgresDeleteReconciliationCapabilities(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
) (postgresDeleteReconciliationCapabilities, error) {
	sourceAdapter, ok := source.(*relationalSourceAdapter)
	if !ok || sourceAdapter == nil ||
		sourceAdapter.spec.engine != "postgres" ||
		sourceAdapter.database == nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"delete reconciliation requires a live PostgreSQL relational source adapter",
		)
	}
	targetAdapter, ok := target.(*postgresTargetAdapter)
	if !ok || targetAdapter == nil || targetAdapter.database == nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"delete reconciliation requires a live PostgreSQL target adapter",
		)
	}
	if sourceTable.Schema != sourceAdapter.namespace ||
		targetTable.Schema != targetAdapter.namespace {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"delete reconciliation table namespaces differ from their PostgreSQL adapters",
		)
	}
	if sourceAdapter.namespace == postgresDeleteJournalSchema ||
		targetAdapter.namespace == postgresDeleteJournalSchema {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"PostgreSQL source and target namespace %s is reserved for DMTX receipt evidence",
			postgresDeleteJournalSchema,
		)
	}
	sourceAuthority, err := inspectPostgresDeleteCatalogAuthority(
		ctx,
		sourceAdapter.database,
		sourceAdapter.namespace,
		sourceTable,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"validate PostgreSQL delete source catalog: %w",
			err,
		)
	}
	targetAuthority, err := inspectPostgresDeleteCatalogAuthority(
		ctx,
		targetAdapter.database,
		targetAdapter.namespace,
		targetTable,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"validate PostgreSQL delete target catalog: %w",
			err,
		)
	}
	if !targetAuthority.CanDelete {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"PostgreSQL delete target requires exact table DELETE privilege",
		)
	}
	if samePostgresDeleteRelation(
		sourceAuthority,
		targetAuthority,
	) {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"PostgreSQL delete reconciliation rejects an identical source and target relation",
		)
	}
	if err := validatePostgresDeleteKeyPair(
		sourceAuthority.PrimaryKey,
		targetAuthority.PrimaryKey,
	); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	if err := preflightPostgresDeleteReceiptJournal(
		ctx,
		targetAdapter.database,
	); err != nil {
		return postgresDeleteReconciliationCapabilities{}, fmt.Errorf(
			"preflight PostgreSQL delete receipt journal: %w",
			err,
		)
	}
	canonicalizer, err := newPostgresDeleteKeyCanonicalizer(
		sourceTable,
		targetTable,
		sourceAuthority,
		targetAuthority,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	return postgresDeleteReconciliationCapabilities{
		source: &postgresDeleteSourceCapability{
			adapter:   sourceAdapter,
			authority: sourceAuthority,
		},
		target: &postgresDeleteTargetCapability{
			adapter:   targetAdapter,
			authority: targetAuthority,
		},
		canonicalizer: canonicalizer,
	}, nil
}

func validatePostgresDeleteKeyPair(
	sourceKey []schema.Column,
	targetKey []schema.Column,
) error {
	if len(sourceKey) == 0 || len(sourceKey) != len(targetKey) {
		return fmt.Errorf(
			"PostgreSQL delete source and target primary-key widths differ",
		)
	}
	for index := range sourceKey {
		source := sourceKey[index]
		target := targetKey[index]
		if source.Name != target.Name ||
			source.PrimaryKeyPosition != index+1 ||
			target.PrimaryKeyPosition != index+1 ||
			source.Nullable || target.Nullable ||
			!reflect.DeepEqual(source, target) {
			return fmt.Errorf(
				"PostgreSQL delete primary-key column %d is not preserved exactly",
				index+1,
			)
		}
		if _, err := postgresDeleteProofSemantics(source); err != nil {
			return fmt.Errorf(
				"PostgreSQL delete primary-key column %s: %w",
				source.Name,
				err,
			)
		}
	}
	return nil
}

type postgresDeleteKeyRows struct {
	rows  *sql.Rows
	tx    *sql.Tx
	width int
}

func (rows *postgresDeleteKeyRows) Next() bool {
	return rows != nil && rows.rows != nil && rows.rows.Next()
}

func (rows *postgresDeleteKeyRows) Values() ([]any, error) {
	if rows == nil || rows.rows == nil || rows.width <= 0 {
		return nil, fmt.Errorf("PostgreSQL delete key reader is closed")
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

func (rows *postgresDeleteKeyRows) Err() error {
	if rows == nil || rows.rows == nil {
		return nil
	}
	return rows.rows.Err()
}

func (rows *postgresDeleteKeyRows) Close() error {
	if rows == nil {
		return nil
	}
	var closeErr error
	if rows.rows != nil {
		closeErr = rows.rows.Close()
		rows.rows = nil
	}
	if rows.tx != nil {
		rollbackErr := rows.tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		closeErr = errors.Join(closeErr, rollbackErr)
		rows.tx = nil
	}
	return closeErr
}

func openPostgresDeletePrimaryKeys(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table schema.Table,
	columns []string,
	expectedAuthority postgresDeleteCatalogAuthority,
) (deleteKeyRows, error) {
	tx, err := database.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"begin PostgreSQL delete key snapshot: %w",
			err,
		)
	}
	if _, err := tx.ExecContext(
		ctx,
		"LOCK TABLE "+
			postgresQualified(namespace, table.Name)+
			" IN ACCESS SHARE MODE",
	); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf(
			"lock PostgreSQL delete key catalog authority: %w",
			err,
		)
	}
	liveAuthority, err := inspectPostgresDeleteCatalogAuthority(
		ctx,
		tx,
		namespace,
		table,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if !samePostgresDeleteCatalogAuthority(
		expectedAuthority,
		liveAuthority,
	) {
		_ = tx.Rollback()
		return nil, fmt.Errorf(
			"PostgreSQL delete key relation/catalog authority changed after plan admission",
		)
	}
	if len(columns) != len(liveAuthority.PrimaryKey) {
		_ = tx.Rollback()
		return nil, fmt.Errorf(
			"PostgreSQL delete key request width differs from the live primary key",
		)
	}
	for index := range liveAuthority.PrimaryKey {
		if columns[index] != liveAuthority.PrimaryKey[index].Name {
			_ = tx.Rollback()
			return nil, fmt.Errorf(
				"PostgreSQL delete key request is not in exact primary-key order",
			)
		}
	}
	query := postgresDeletePrimaryKeyQuery(namespace, table.Name, columns)
	sqlRows, err := tx.QueryContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf(
			"open PostgreSQL delete primary keys for %s.%s: %w",
			namespace,
			table.Name,
			err,
		)
	}
	return &postgresDeleteKeyRows{
		rows:  sqlRows,
		tx:    tx,
		width: len(columns),
	}, nil
}

func postgresDeletePrimaryKeyQuery(
	namespace string,
	table string,
	columns []string,
) string {
	quoted := quotedColumns(columns)
	return "SELECT " + quoted +
		" FROM " + postgresQualified(namespace, table) +
		" ORDER BY " + quoted
}

func (capability *postgresDeleteSourceCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil ||
		capability.adapter.spec.engine != "postgres" {
		return nil, fmt.Errorf(
			"delete primary-key scans are available only on PostgreSQL sources",
		)
	}
	return openPostgresDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
	)
}

func (capability *postgresDeleteTargetCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil {
		return nil, fmt.Errorf("PostgreSQL delete target is unavailable")
	}
	return openPostgresDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
	)
}

func (*postgresDeleteTargetCapability) MaxDeleteParameters() int {
	return postgresDeleteMaximumParameters
}

type postgresDeleteKeyCanonicalizer struct {
	sourceTable schema.Table
	targetTable schema.Table
	proof       deleteKeyEqualityProof
}

func newPostgresDeleteKeyCanonicalizer(
	sourceTable schema.Table,
	targetTable schema.Table,
	sourceAuthority postgresDeleteCatalogAuthority,
	targetAuthority postgresDeleteCatalogAuthority,
) (*postgresDeleteKeyCanonicalizer, error) {
	sourceKey := sourceAuthority.PrimaryKey
	targetKey := targetAuthority.PrimaryKey
	if err := validatePostgresDeleteKeyPair(sourceKey, targetKey); err != nil {
		return nil, err
	}
	for side, authority := range map[string]postgresDeleteCatalogAuthority{
		"source": sourceAuthority,
		"target": targetAuthority,
	} {
		expectedDigest, err :=
			postgresDeleteAuthorityDigestValue(authority)
		if err != nil {
			return nil, err
		}
		if err := validateLowerSHA256(
			"PostgreSQL delete "+side+" catalog authority digest",
			authority.CatalogDigest,
		); err != nil || authority.CatalogDigest != expectedDigest {
			return nil, fmt.Errorf(
				"PostgreSQL delete %s catalog authority digest differs from its exact catalog evidence",
				side,
			)
		}
		if len(authority.IndexKeys) != len(authority.PrimaryKey) {
			return nil, fmt.Errorf(
				"PostgreSQL delete %s primary-key index authority width differs",
				side,
			)
		}
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		sourceTable,
		sourceKey,
	)
	if err != nil {
		return nil, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		targetTable,
		targetKey,
	)
	if err != nil {
		return nil, err
	}
	routeAuthority := sha256.Sum256([]byte(
		sourceAuthority.CatalogDigest + "\x00" +
			targetAuthority.CatalogDigest,
	))
	proof := deleteKeyEqualityProof{
		CanonicalizerID: "postgres-exact-primary-key-v2:" +
			hex.EncodeToString(routeAuthority[:]),
		SourceFingerprint: sourceFingerprint,
		TargetFingerprint: targetFingerprint,
		Columns: make(
			[]deleteKeyColumnProof,
			len(sourceKey),
		),
	}
	for index, column := range sourceKey {
		semantics, err := postgresDeleteProofSemantics(column)
		if err != nil {
			return nil, err
		}
		sourceIndex := sourceAuthority.IndexKeys[index]
		targetIndex := targetAuthority.IndexKeys[index]
		if sourceIndex.Position != index+1 ||
			targetIndex.Position != index+1 ||
			sourceIndex.Column != column.Name ||
			targetIndex.Column != targetKey[index].Name ||
			sourceIndex.OperatorClassNamespace != "pg_catalog" ||
			targetIndex.OperatorClassNamespace != "pg_catalog" ||
			strings.TrimSpace(sourceIndex.OperatorClass) == "" ||
			sourceIndex.OperatorClass != targetIndex.OperatorClass {
			return nil, fmt.Errorf(
				"PostgreSQL delete primary-key column %s lacks matching exact backing-index operator-class authority",
				column.Name,
			)
		}
		proof.Columns[index].Semantics = semantics
		if semantics == "binary_text" {
			if sourceIndex.CollationOID <= 0 ||
				targetIndex.CollationOID <= 0 ||
				!sourceIndex.CollationDeterministic ||
				!targetIndex.CollationDeterministic ||
				strings.TrimSpace(sourceIndex.CollationNamespace) == "" ||
				strings.TrimSpace(targetIndex.CollationNamespace) == "" ||
				strings.TrimSpace(sourceIndex.Collation) == "" ||
				strings.TrimSpace(targetIndex.Collation) == "" ||
				strings.TrimSpace(sourceIndex.CollationProvider) == "" ||
				strings.TrimSpace(targetIndex.CollationProvider) == "" {
				return nil, fmt.Errorf(
					"PostgreSQL text delete key %s lacks deterministic backing-index collation authority",
					column.Name,
				)
			}
			proof.Columns[index].CollationEvidence =
				postgresDeleteIndexCollationProof(
					sourceIndex,
					targetIndex,
				)
		}
		if semantics == "uuid_binary_text" {
			proof.Columns[index].CollationEvidence =
				"PostgreSQL uuid equality is binary and collation-independent"
		}
	}
	if _, err := validateDeleteKeyEqualityProof(
		proof,
		sourceTable,
		targetTable,
		sourceKey,
		targetKey,
	); err != nil {
		return nil, err
	}
	return &postgresDeleteKeyCanonicalizer{
		sourceTable: sourceTable,
		targetTable: targetTable,
		proof:       proof,
	}, nil
}

func postgresDeleteIndexCollationProof(
	source postgresDeleteIndexKeyAuthority,
	target postgresDeleteIndexKeyAuthority,
) string {
	return fmt.Sprintf(
		"postgres-pk-btree-deterministic-v2 source=%d/%s.%s/%s/%s target=%d/%s.%s/%s/%s",
		source.CollationOID,
		source.CollationNamespace,
		source.Collation,
		source.OperatorClassNamespace,
		source.OperatorClass,
		target.CollationOID,
		target.CollationNamespace,
		target.Collation,
		target.OperatorClassNamespace,
		target.OperatorClass,
	)
}

func postgresDeleteProofSemantics(
	column schema.Column,
) (string, error) {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if opening := strings.IndexByte(base, '('); opening >= 0 {
		base = strings.TrimSpace(base[:opening])
	}
	switch base {
	case "char", "character", "bpchar":
		return "", fmt.Errorf(
			"fixed-width character keys have trailing-space equality and are unsupported",
		)
	case "json", "jsonb":
		return "", fmt.Errorf(
			"JSON primary keys do not share PostgreSQL byte equality and are unsupported",
		)
	case "boolean", "integer", "bigint", "numeric", "real",
		"double precision", "text", "varchar", "bytea", "date",
		"time", "timestamp", "timestamptz", "uuid":
	default:
		return "", fmt.Errorf(
			"unsupported PostgreSQL delete key type %q",
			column.Type,
		)
	}
	kind, err := validationKindForColumn(column)
	if err != nil {
		return "", err
	}
	switch kind {
	case validationBoolean:
		return "boolean", nil
	case validationInteger:
		return "integer", nil
	case validationDecimal:
		return "decimal", nil
	case validationFloat:
		return "float_exact", nil
	case validationText:
		return "binary_text", nil
	case validationBytes:
		return "binary", nil
	case validationDate:
		return "date", nil
	case validationTime:
		return "time", nil
	case validationTimestamp:
		return "timestamp", nil
	case validationUUID:
		return "uuid_binary_text", nil
	default:
		return "", fmt.Errorf(
			"unsupported PostgreSQL delete key type %q",
			column.Type,
		)
	}
}

func (canonicalizer *postgresDeleteKeyCanonicalizer) ProveDeleteKeyEquality(
	sourceTable schema.Table,
	targetTable schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (deleteKeyEqualityProof, error) {
	if canonicalizer == nil {
		return deleteKeyEqualityProof{}, fmt.Errorf(
			"PostgreSQL delete key proof was requested for different tables",
		)
	}
	sourceMatches, err := samePostgresDeleteStableTableShape(
		sourceTable,
		canonicalizer.sourceTable,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, fmt.Errorf(
			"compare PostgreSQL delete source proof table: %w",
			err,
		)
	}
	targetMatches, err := samePostgresDeleteStableTableShape(
		targetTable,
		canonicalizer.targetTable,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, fmt.Errorf(
			"compare PostgreSQL delete target proof table: %w",
			err,
		)
	}
	if !sourceMatches || !targetMatches {
		return deleteKeyEqualityProof{}, fmt.Errorf(
			"PostgreSQL delete key proof was requested for different tables",
		)
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		sourceTable,
		sourcePrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		targetTable,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	if sourceFingerprint != canonicalizer.proof.SourceFingerprint ||
		targetFingerprint != canonicalizer.proof.TargetFingerprint {
		return deleteKeyEqualityProof{}, fmt.Errorf(
			"PostgreSQL delete primary-key metadata changed after capability admission",
		)
	}
	proof := canonicalizer.proof
	proof.Columns = append(
		[]deleteKeyColumnProof(nil),
		canonicalizer.proof.Columns...,
	)
	return proof, nil
}

func (canonicalizer *postgresDeleteKeyCanonicalizer) CanonicalizeDeleteKeyValue(
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	index int,
	value any,
) (deleteCanonicalValue, error) {
	if canonicalizer == nil ||
		proof.CanonicalizerID != canonicalizer.proof.CanonicalizerID ||
		!reflect.DeepEqual(proof, canonicalizer.proof) ||
		index < 0 || index >= len(proof.Columns) {
		return deleteCanonicalValue{}, fmt.Errorf(
			"PostgreSQL delete key canonicalization proof differs",
		)
	}
	var canonical []byte
	var err error
	switch proof.Columns[index].Semantics {
	case "boolean":
		canonical, err = canonicalValidationBoolean(value)
	case "integer":
		canonical, err = canonicalValidationInteger(value)
	case "decimal":
		canonical, err = canonicalValidationDecimal(value)
	case "float_exact":
		canonical, err = canonicalValidationFloat(value)
	case "binary_text":
		canonical, err = canonicalValidationText(value)
	case "uuid_binary_text":
		canonical, err = canonicalValidationUUID(value)
	case "binary":
		canonical, err = canonicalValidationBytes(value)
	case "date":
		canonical, err = canonicalValidationDate(value)
	case "time":
		canonical, err = canonicalValidationTime(value)
	case "timestamp":
		canonical, err = canonicalValidationTimestamp(value)
	default:
		err = fmt.Errorf(
			"unsupported PostgreSQL delete key semantics %q",
			proof.Columns[index].Semantics,
		)
	}
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	result := deleteCanonicalValue{
		Canonical: append([]byte(nil), canonical...),
	}
	if side == deleteKeyTargetSide {
		parameter, err := driver.DefaultParameterConverter.
			ConvertValue(value)
		if err != nil {
			return deleteCanonicalValue{}, fmt.Errorf(
				"convert PostgreSQL delete parameter: %w",
				err,
			)
		}
		result.Parameter, err = stableDeleteParameter(parameter)
		if err != nil {
			return deleteCanonicalValue{}, err
		}
	} else if side != deleteKeySourceSide {
		return deleteCanonicalValue{}, fmt.Errorf(
			"unknown PostgreSQL delete key side %q",
			side,
		)
	}
	return result, nil
}

func validatePostgresDeleteBatch(
	namespace string,
	batch deleteTargetBatch,
) ([][]driver.Value, error) {
	return validatePostgresDeleteBatchWithLimits(
		namespace,
		batch,
		postgresDeleteMaximumParameters,
		postgresDeleteMaximumBatchBytes,
	)
}

func validatePostgresDeleteBatchWithLimits(
	namespace string,
	batch deleteTargetBatch,
	maximumParameters int,
	maximumBytes int64,
) ([][]driver.Value, error) {
	if strings.TrimSpace(namespace) == "" ||
		batch.Table.Schema != namespace ||
		strings.TrimSpace(batch.Table.Name) == "" ||
		len(batch.Columns) == 0 ||
		len(batch.Keys) == 0 ||
		strings.TrimSpace(batch.PlanID) == "" ||
		batch.Sequence < 0 {
		return nil, fmt.Errorf(
			"PostgreSQL delete batch identity is incomplete",
		)
	}
	if _, err := hex.DecodeString(batch.PlanID); err != nil ||
		len(batch.PlanID) != 32 ||
		batch.PlanID != strings.ToLower(batch.PlanID) {
		return nil, fmt.Errorf(
			"PostgreSQL delete batch plan ID must be 32 lowercase hexadecimal characters",
		)
	}
	if err := validateLowerSHA256(
		"PostgreSQL delete batch token",
		batch.Token,
	); err != nil {
		return nil, err
	}
	if err := validateLowerSHA256(
		"PostgreSQL delete batch digest",
		batch.BatchDigest,
	); err != nil {
		return nil, err
	}
	if maximumParameters <= 0 || maximumBytes <= 0 {
		return nil, fmt.Errorf(
			"PostgreSQL delete batch limits must be positive",
		)
	}
	if len(batch.Keys) > maximumParameters/len(batch.Columns) {
		return nil, fmt.Errorf(
			"PostgreSQL delete batch exceeds the %d-parameter limit",
			maximumParameters,
		)
	}
	var encodedBytes int64
	for rowIndex, key := range batch.Keys {
		if len(key) != len(batch.Columns) {
			return nil, fmt.Errorf(
				"PostgreSQL delete key %d width differs",
				rowIndex,
			)
		}
		for columnIndex, value := range key {
			if value == nil || !driver.IsValue(value) {
				return nil, fmt.Errorf(
					"PostgreSQL delete key %d column %d is not parameter-safe",
					rowIndex,
					columnIndex,
				)
			}
			var valueBytes int64
			switch typed := value.(type) {
			case string:
				valueBytes = int64(len(typed))
			case []byte:
				valueBytes = int64(len(typed))
			default:
				valueBytes = 16
			}
			if valueBytes > maximumBytes-encodedBytes {
				return nil, fmt.Errorf(
					"PostgreSQL delete batch exceeds the %d-byte adapter ceiling",
					maximumBytes,
				)
			}
			encodedBytes += valueBytes
		}
	}
	arguments := make([][]driver.Value, len(batch.Keys))
	for rowIndex, key := range batch.Keys {
		arguments[rowIndex] = make([]driver.Value, len(key))
		for columnIndex, value := range key {
			stable, err := stableDeleteParameter(value)
			if err != nil || stable == nil {
				return nil, fmt.Errorf(
					"PostgreSQL delete key %d column %d is not parameter-safe",
					rowIndex,
					columnIndex,
				)
			}
			arguments[rowIndex][columnIndex] = stable
		}
	}
	return arguments, nil
}

func postgresDeleteBatchStatement(
	table schema.Table,
	columns []string,
	rowCount int,
) (string, error) {
	if table.Schema == "" || table.Name == "" ||
		len(columns) == 0 || rowCount <= 0 {
		return "", fmt.Errorf(
			"PostgreSQL delete statement shape is incomplete",
		)
	}
	tuples := make([]string, rowCount)
	parameter := 1
	for row := range tuples {
		placeholders := make([]string, len(columns))
		for column := range placeholders {
			placeholders[column] = fmt.Sprintf("$%d", parameter)
			parameter++
		}
		tuples[row] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	return "DELETE FROM " +
		postgresQualified(table.Schema, table.Name) +
		" WHERE (" + quotedColumns(columns) + ") IN (" +
		strings.Join(tuples, ", ") + ")", nil
}

var (
	_ deleteKeySource        = (*postgresDeleteSourceCapability)(nil)
	_ deleteKeyTarget        = (*postgresDeleteTargetCapability)(nil)
	_ deleteKeyCanonicalizer = (*postgresDeleteKeyCanonicalizer)(nil)
)
