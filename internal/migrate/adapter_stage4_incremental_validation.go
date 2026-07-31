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

func (adapter *postgresTargetAdapter) ValidateStage4IncrementalBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	sourceRows [][]any,
) error {
	if adapter == nil || adapter.database == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL incremental validation target is not configured",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
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
				"admit PostgreSQL incremental validation projection: %w",
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
				"admit PostgreSQL incremental validation primary key: %w",
				err,
			),
		)
	}
	primaryKey, err := adapterValidationPrimaryKey(table)
	if err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"map PostgreSQL incremental validation primary key: %w",
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
						"PostgreSQL incremental source batch has an incomplete primary key",
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
				"canonicalize PostgreSQL incremental source batch: %w",
				err,
			),
		)
	}
	if duplicateCanonicalValidationKey(canonicalSource) {
		return NewTransferError(
			ErrorClassValidation,
			errors.New(
				"PostgreSQL incremental source batch contains duplicate complete primary keys",
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
				"canonicalize PostgreSQL incremental validation keys: %w",
				err,
			),
		)
	}
	endpoint := adapterValidationSQLEndpoint{
		engine:         adapterValidationPostgres,
		namespace:      table.Schema,
		queryer:        adapter.database,
		database:       adapter.database,
		parameterLimit: adapterValidationPostgresParameterLimit,
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
					"build PostgreSQL incremental validation key predicate: %w",
					predicateErr,
				),
			)
		}
		query := "SELECT " + adapterValidationQuotedColumns(
			endpoint,
			projection,
		) + " FROM " + endpoint.qualified(table) +
			" WHERE " + predicate
		rows, queryErr := adapter.database.QueryContext(
			ctx,
			query,
			arguments...,
		)
		if queryErr != nil {
			return fmt.Errorf(
				"fetch PostgreSQL incremental target rows for table %s failed with error type %T",
				table.Name,
				queryErr,
			)
		}
		fetched, scanErr := scanAdapterValidationRows(
			rows,
			len(projection),
			end-offset,
			"PostgreSQL incremental target validation",
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
				"validate PostgreSQL incremental target key set: %w",
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
				"canonicalize PostgreSQL incremental target batch: %w",
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
					"PostgreSQL incremental target returned a duplicate complete primary key",
				),
			)
		}
		targetByKey[key] = row.row
	}
	if len(targetByKey) != len(canonicalSource) {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"PostgreSQL incremental target returned %d rows for %d exact source keys",
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
					"PostgreSQL incremental target row differs for a transferred complete primary key",
				),
			)
		}
	}
	return ctx.Err()
}
