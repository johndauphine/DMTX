package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Rebuild admission is deliberately static: the selected tables may not yet
// exist, and inspecting retained-table replay hazards would incorrectly reject
// a valid drop/recreate route. PrepareTables creates the complete set before
// transfer, and each native page transaction then fences and re-proves the
// exact recreated table catalog before writing.

func (adapter *postgresTargetAdapter) PreflightStage4NetworkRebuild(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4NetworkRebuildAdmissionContext(
		ctx,
		"PostgreSQL",
	); err != nil {
		return err
	}
	if adapter == nil {
		return stage4NetworkRebuildAdmissionStateError(
			"PostgreSQL",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(postgresStage4NetworkRebuildBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4NetworkRebuildAdmissionStateError(
			"PostgreSQL",
			"certified rebuild writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4NetworkRebuildAdmissionPolicyError(
				"PostgreSQL",
				table.Name,
				fmt.Errorf(
					"planned schema %q differs from target schema %q",
					table.Schema,
					adapter.namespace,
				),
			)
		}
		if err := validatePostgresWriteShape(
			table,
			adapterColumnNames(table),
			"upsert",
		); err != nil {
			return stage4NetworkRebuildAdmissionPolicyError(
				"PostgreSQL",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *mysqlTargetAdapter) PreflightStage4NetworkRebuild(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4NetworkRebuildAdmissionContext(
		ctx,
		"MySQL",
	); err != nil {
		return err
	}
	if adapter == nil {
		return stage4NetworkRebuildAdmissionStateError(
			"MySQL",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(mysqlStage4NetworkRebuildBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4NetworkRebuildAdmissionStateError(
			"MySQL",
			"certified rebuild writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4NetworkRebuildAdmissionPolicyError(
				"MySQL",
				table.Name,
				fmt.Errorf(
					"planned database %q differs from target database %q",
					table.Schema,
					adapter.namespace,
				),
			)
		}
		if err := validateMySQLWriteShape(
			table,
			adapterColumnNames(table),
			"upsert",
		); err != nil {
			return stage4NetworkRebuildAdmissionPolicyError(
				"MySQL",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *sqlServerTargetAdapter) PreflightStage4NetworkRebuild(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4NetworkRebuildAdmissionContext(
		ctx,
		"SQL Server",
	); err != nil {
		return err
	}
	if adapter == nil {
		return stage4NetworkRebuildAdmissionStateError(
			"SQL Server",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(sqlServerStage4NetworkRebuildBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4NetworkRebuildAdmissionStateError(
			"SQL Server",
			"certified rebuild writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4NetworkRebuildAdmissionPolicyError(
				"SQL Server",
				table.Name,
				fmt.Errorf(
					"planned schema %q differs from target schema %q",
					table.Schema,
					adapter.namespace,
				),
			)
		}
		if err := validateSQLServerWriteShape(
			table,
			adapterColumnNames(table),
			"upsert",
		); err != nil {
			return stage4NetworkRebuildAdmissionPolicyError(
				"SQL Server",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) PreflightStage4NetworkRebuild(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4NetworkRebuildAdmissionContext(
		ctx,
		"SQLite",
	); err != nil {
		return err
	}
	if adapter == nil {
		return stage4NetworkRebuildAdmissionStateError(
			"SQLite",
			"target adapter is not configured",
		)
	}
	writer := adapter.stage4BatchWriter
	if writer == nil && adapter.database != nil {
		writer = newSQLiteStage4NetworkWriter(adapter.database)
	}
	if rebuildWriter, ok := writer.(sqliteStage4NetworkRebuildBatchWriter); !ok || isNilInterface(rebuildWriter) {
		return stage4NetworkRebuildAdmissionStateError(
			"SQLite",
			"certified rebuild writer is not configured",
		)
	}
	for _, table := range tables {
		if err := validateSQLiteStage4NetworkWriteShape(
			table,
			adapterColumnNames(table),
			nil,
		); err != nil {
			return stage4NetworkRebuildAdmissionPolicyError(
				"SQLite",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func validateStage4NetworkRebuildAdmissionContext(
	ctx context.Context,
	engine string,
) error {
	if ctx == nil {
		return stage4NetworkRebuildAdmissionStateError(
			engine,
			"context is required",
		)
	}
	return ctx.Err()
}

func stage4NetworkRebuildAdmissionStateError(
	engine string,
	detail string,
) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf(
			"%s Stage 4 rebuild admission failed: %s",
			engine,
			detail,
		),
	)
}

func stage4NetworkRebuildAdmissionPolicyError(
	engine string,
	table string,
	cause error,
) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"%s Stage 4 rebuild table %s is not replay-safe: %w",
			engine,
			table,
			cause,
		),
	)
}
