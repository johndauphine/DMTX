package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// adapterStage4IncrementalUpsertTarget proves that the target owns the native
// page writer used for an incremental window. The ordinary network-upsert
// marker alone is insufficient: a partially constructed adapter could carry
// that marker while its native writer is absent, which would defer a required
// admission failure until after durable work is created.
type adapterStage4IncrementalUpsertTarget interface {
	adapterStage4NetworkUpsertTarget
	PreflightStage4IncrementalUpsert(context.Context, []schema.Table) error
}

func preflightStage4IncrementalUpsert(
	ctx context.Context,
	target targetAdapter,
	tables []schema.Table,
) error {
	capability, ok := target.(adapterStage4IncrementalUpsertTarget)
	if !ok || isNilInterface(capability) {
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q has no certified incremental upsert writer",
				engine,
			),
		)
	}
	if err := capability.PreflightStage4IncrementalUpsert(
		ctx,
		append([]schema.Table(nil), tables...),
	); err != nil {
		return fmt.Errorf(
			"preflight Stage 4 incremental upsert writer for target %s: %w",
			capability.Engine(),
			err,
		)
	}
	return nil
}

func (adapter *postgresTargetAdapter) PreflightStage4IncrementalUpsert(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4IncrementalAdmissionContext(ctx, "PostgreSQL"); err != nil {
		return err
	}
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalAdmissionStateError(
			"PostgreSQL",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(postgresStage4NetworkBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4IncrementalAdmissionStateError(
			"PostgreSQL",
			"certified incremental upsert writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4IncrementalAdmissionPolicyError(
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
			return stage4IncrementalAdmissionPolicyError(
				"PostgreSQL",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *mysqlTargetAdapter) PreflightStage4IncrementalUpsert(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4IncrementalAdmissionContext(ctx, "MySQL"); err != nil {
		return err
	}
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalAdmissionStateError(
			"MySQL",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(mysqlStage4NetworkBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4IncrementalAdmissionStateError(
			"MySQL",
			"certified incremental upsert writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4IncrementalAdmissionPolicyError(
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
			return stage4IncrementalAdmissionPolicyError(
				"MySQL",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *sqlServerTargetAdapter) PreflightStage4IncrementalUpsert(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4IncrementalAdmissionContext(ctx, "SQL Server"); err != nil {
		return err
	}
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalAdmissionStateError(
			"SQL Server",
			"target adapter is not configured",
		)
	}
	writer, ok := adapter.batchWriter.(sqlServerStage4NetworkBatchWriter)
	if !ok || isNilInterface(writer) {
		return stage4IncrementalAdmissionStateError(
			"SQL Server",
			"certified incremental upsert writer is not configured",
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return stage4IncrementalAdmissionPolicyError(
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
			return stage4IncrementalAdmissionPolicyError(
				"SQL Server",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) PreflightStage4IncrementalUpsert(
	ctx context.Context,
	tables []schema.Table,
) error {
	if err := validateStage4IncrementalAdmissionContext(ctx, "SQLite"); err != nil {
		return err
	}
	if adapter == nil || adapter.database == nil {
		return stage4IncrementalAdmissionStateError(
			"SQLite",
			"target adapter is not configured",
		)
	}
	writer := adapter.stage4BatchWriter
	if writer == nil {
		writer = newSQLiteStage4NetworkWriter(adapter.database)
	}
	if isNilInterface(writer) {
		return stage4IncrementalAdmissionStateError(
			"SQLite",
			"certified incremental upsert writer is not configured",
		)
	}
	for _, table := range tables {
		if err := validateSQLiteStage4NetworkWriteShape(
			table,
			adapterColumnNames(table),
			nil,
		); err != nil {
			return stage4IncrementalAdmissionPolicyError(
				"SQLite",
				table.Name,
				err,
			)
		}
	}
	return nil
}

func validateStage4IncrementalAdmissionContext(
	ctx context.Context,
	engine string,
) error {
	if ctx == nil {
		return stage4IncrementalAdmissionStateError(engine, "context is required")
	}
	return ctx.Err()
}

func stage4IncrementalAdmissionStateError(
	engine string,
	detail string,
) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf("%s Stage 4 incremental admission failed: %s", engine, detail),
	)
}

func stage4IncrementalAdmissionPolicyError(
	engine string,
	table string,
	cause error,
) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"%s Stage 4 incremental table %s is not upsert-safe: %w",
			engine,
			table,
			cause,
		),
	)
}
