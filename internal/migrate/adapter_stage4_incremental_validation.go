package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// adapterStage4IncrementalValidationTarget is the route-owned proof that every
// row in one bounded source window batch can be fetched from the target by its
// complete primary key and compared under the same canonical value semantics
// as Stage 4 deep validation.
type adapterStage4IncrementalValidationTarget interface {
	ValidateStage4IncrementalBatch(
		context.Context,
		schema.Table,
		[]string,
		[][]any,
	) error
}

var _ adapterStage4IncrementalValidationTarget = (*postgresTargetAdapter)(nil)
var _ adapterStage4IncrementalValidationTarget = (*sqlServerTargetAdapter)(nil)
var _ adapterStage4IncrementalValidationTarget = (*mysqlTargetAdapter)(nil)
var _ adapterStage4IncrementalValidationTarget = (*sqliteTargetAdapter)(nil)

func (adapter *postgresTargetAdapter) ValidateStage4IncrementalBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalValidationTargetUnavailable("PostgreSQL")
	}
	return validateStage4IncrementalSQLTarget(
		ctx,
		adapter,
		"PostgreSQL",
		table,
		projection,
		sourceRows,
	)
}

func (adapter *sqlServerTargetAdapter) ValidateStage4IncrementalBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalValidationTargetUnavailable("SQL Server")
	}
	return validateStage4IncrementalSQLTarget(
		ctx,
		adapter,
		"SQL Server",
		table,
		projection,
		sourceRows,
	)
}

func (adapter *mysqlTargetAdapter) ValidateStage4IncrementalBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalValidationTargetUnavailable("MySQL")
	}
	return validateStage4IncrementalSQLTarget(
		ctx,
		adapter,
		"MySQL",
		table,
		projection,
		sourceRows,
	)
}

func (adapter *sqliteTargetAdapter) ValidateStage4IncrementalBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalValidationTargetUnavailable("SQLite")
	}
	return validateStage4IncrementalSQLTarget(
		ctx,
		adapter,
		"SQLite",
		table,
		projection,
		sourceRows,
	)
}

func stage4IncrementalValidationTargetUnavailable(engine string) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf("%s incremental validation target is not configured", engine),
	)
}

// validateStage4IncrementalSQLTarget is deliberately endpoint-generic. The
// four relational/SQLite target adapters only supply their configured query
// endpoint; key construction, identifier quoting, placeholder binding, and
// canonical comparison remain shared with the database validation probe.
func validateStage4IncrementalSQLTarget(
	ctx context.Context,
	target targetAdapter,
	label string,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	endpoint, err := adapterValidationTargetEndpoint(target)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("resolve %s incremental validation target: %w", label, err),
		)
	}
	if endpoint.engine == adapterValidationSQLite {
		if table.Schema != "" {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"admit %s incremental validation table: SQLite target table %s has schema %q",
					label,
					table.Name,
					table.Schema,
				),
			)
		}
	} else if table.Schema != endpoint.namespace {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"admit %s incremental validation table: planned schema %q differs from target namespace %q",
				label,
				table.Schema,
				endpoint.namespace,
			),
		)
	}
	descriptor, err := validateValidationCoreProjection(
		table,
		projection,
		true,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"admit %s incremental validation projection: %w",
				label,
				err,
			),
		)
	}
	keyIndexes, keyDescriptor, err := validationPrimaryKeyDescriptor(
		table,
		descriptor,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"admit %s incremental validation primary key: %w",
				label,
				err,
			),
		)
	}
	primaryKey, err := adapterValidationPrimaryKey(table)
	if err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"map %s incremental validation primary key: %w",
				label,
				err,
			),
		)
	}
	sourceSamples := make(
		[]ValidationSampleRow,
		len(sourceRows),
	)
	keys := make([]ValidationPrimaryKey, len(sourceRows))
	for rowIndex, row := range sourceRows {
		sourceSamples[rowIndex] = ValidationSampleRow{
			Values: cloneAdapterRow(row),
		}
		key := make([]any, len(keyIndexes))
		for keyIndex, projectionIndex := range keyIndexes {
			if projectionIndex < 0 || projectionIndex >= len(row) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"%s incremental source batch has an incomplete primary key",
						label,
					),
				)
			}
			key[keyIndex] = cloneAdapterValidationValue(
				row[projectionIndex],
			)
		}
		keys[rowIndex] = ValidationPrimaryKey{Values: key}
	}
	canonicalSource, err := canonicalizeValidationSamples(
		descriptor,
		keyDescriptor,
		keyIndexes,
		sourceSamples,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"canonicalize %s incremental source batch: %w",
				label,
				err,
			),
		)
	}
	if duplicateCanonicalValidationKey(canonicalSource) {
		return NewTransferError(
			ErrorClassValidation,
			errors.New(
				label+" incremental source batch contains duplicate complete primary keys",
			),
		)
	}
	requested, err := adapterValidationCanonicalKeySet(
		primaryKey,
		keys,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"canonicalize %s incremental validation keys: %w",
				label,
				err,
			),
		)
	}
	batchSize, err := adapterValidationKeyBatchSize(
		endpoint.parameterLimit,
		len(primaryKey),
	)
	if err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	targetSamples := make(
		[]ValidationSampleRow,
		0,
		len(sourceRows),
	)
	for offset := 0; offset < len(keys); offset += batchSize {
		end := offset + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		predicate, arguments, predicateErr :=
			adapterValidationKeyPredicate(
				endpoint,
				primaryKey,
				keys[offset:end],
			)
		if predicateErr != nil {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"build %s incremental validation key predicate: %w",
					label,
					predicateErr,
				),
			)
		}
		query := "SELECT " + adapterValidationQuotedColumns(
			endpoint,
			projection,
		) + " FROM " + endpoint.qualified(table) +
			" WHERE " + predicate
		rows, queryErr := endpoint.queryer.QueryContext(
			ctx,
			query,
			arguments...,
		)
		if queryErr != nil {
			return fmt.Errorf(
				"fetch %s incremental target rows for table %s failed with error type %T",
				label,
				table.Name,
				queryErr,
			)
		}
		fetched, scanErr := scanAdapterValidationRows(
			rows,
			len(projection),
			end-offset,
			label+" incremental target validation",
		)
		if scanErr != nil {
			return scanErr
		}
		targetSamples = append(targetSamples, fetched...)
	}
	if err := validateAdapterValidationTargetRows(
		table,
		projection,
		primaryKey,
		requested,
		targetSamples,
	); err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"validate %s incremental target key set: %w",
				label,
				err,
			),
		)
	}
	canonicalTarget, err := canonicalizeValidationSamples(
		descriptor,
		keyDescriptor,
		keyIndexes,
		targetSamples,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"canonicalize %s incremental target batch: %w",
				label,
				err,
			),
		)
	}
	targetByKey := make(map[string][]byte, len(canonicalTarget))
	for _, row := range canonicalTarget {
		key := string(row.key)
		if _, duplicate := targetByKey[key]; duplicate {
			return NewTransferError(
				ErrorClassValidation,
				errors.New(
					label+" incremental target returned a duplicate complete primary key",
				),
			)
		}
		targetByKey[key] = row.row
	}
	if len(targetByKey) != len(canonicalSource) {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"%s incremental target returned %d rows for %d exact source keys",
				label,
				len(targetByKey),
				len(canonicalSource),
			),
		)
	}
	for _, row := range canonicalSource {
		targetRow, found := targetByKey[string(row.key)]
		if !found || !bytes.Equal(row.row, targetRow) {
			return NewTransferError(
				ErrorClassValidation,
				fmt.Errorf(
					"%s incremental target row differs for a transferred complete primary key",
					label,
				),
			)
		}
	}
	return ctx.Err()
}
