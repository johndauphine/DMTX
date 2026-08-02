package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// adapterStage4UpsertMergeTarget proves that its Stage 4 idempotent-upsert
// writer executes exactly the rows passed to each write call in one bounded
// native merge/transaction, without coalescing later calls. It is consulted
// only for an explicitly requested migration.upsert_merge_size.
//
// The returned ceiling is a target-native, per-call row limit. It must be
// positive and no greater than the DMTX hard transfer cap; the route combines
// it with its immutable resource ceiling before any target mutation.
type adapterStage4UpsertMergeTarget interface {
	targetAdapter
	PreflightStage4UpsertMerge(context.Context) (int, error)
}

// stage4UpsertMergeNativeWriter is deliberately separate from the ordinary
// Stage 4 network-writer interfaces. A custom writer cannot accidentally gain
// an explicit merge-size capability merely by being replay-safe.
type stage4UpsertMergeNativeWriter interface {
	Stage4UpsertMergeMaximumRows() int
}

// stage4AdapterExplicitUpsertMergeRows turns the explicit public setting into
// a write-only core ceiling. Omitted/default intent remains zero so existing
// routes retain their exact legacy source-page/write-page behavior. The helper
// is read-only and runs during route admission before table checkpoints or
// target mutation.
func stage4AdapterExplicitUpsertMergeRows(
	ctx context.Context,
	migration config.Migration,
	mode string,
	target targetAdapter,
	resourceRows int,
) (int, error) {
	requested, err := stage4AdapterUpsertMergeRequested(migration)
	if err != nil {
		return 0, err
	}
	if !requested {
		return 0, nil
	}
	if ctx == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 upsert merge admission context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if mode != "upsert" {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"migration.upsert_merge_size requires target mode upsert",
			),
		)
	}
	if migration.UpsertMergeSize < 1 {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("migration.upsert_merge_size must be positive"),
		)
	}
	if resourceRows < 1 || resourceRows > config.MaxTransferChunkRows {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 upsert merge resource row ceiling is invalid"),
		)
	}
	preflighter, ok := target.(adapterStage4UpsertMergeTarget)
	if !ok || isNilInterface(preflighter) {
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target engine %q cannot prove an explicit upsert merge limit",
				engine,
			),
		)
	}
	nativeRows, err := preflighter.PreflightStage4UpsertMerge(ctx)
	if err != nil {
		return 0, fmt.Errorf(
			"preflight Stage 4 upsert merge limit for target %s: %w",
			preflighter.Engine(),
			err,
		)
	}
	if nativeRows < 1 || nativeRows > config.MaxTransferChunkRows {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 target engine %q returned an invalid upsert merge limit",
				preflighter.Engine(),
			),
		)
	}
	return stage4MinimumUpsertMergeRows(
		migration.UpsertMergeSize,
		nativeRows,
		resourceRows,
		config.MaxTransferChunkRows,
	), nil
}

func stage4AdapterUpsertMergeRequested(
	migration config.Migration,
) (bool, error) {
	provenance, known := migration.SettingProvenance("upsert_merge_size")
	if !known {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 upsert merge setting provenance is unavailable"),
		)
	}
	return provenance == config.ProvenanceRequested, nil
}

// stage4AdapterUpsertMergeExplicitlyRequested is the configuration-only
// counterpart used while routing legacy compatibility paths. Those paths must
// either enter the composed Stage 4 runner (where the target can prove its
// bound) or be rejected; they may never silently ignore an explicit cap.
func stage4AdapterUpsertMergeExplicitlyRequested(
	migration config.Migration,
) bool {
	provenance, known := migration.SettingProvenance("upsert_merge_size")
	return known && provenance == config.ProvenanceRequested
}

func requireStage4UpsertMergeComposition(
	cfg config.Config,
	stage4Enabled bool,
) error {
	if !stage4AdapterUpsertMergeExplicitlyRequested(cfg.Migration) ||
		stage4Enabled {
		return nil
	}
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"migration.upsert_merge_size requires the composed Stage 4 network runner so the target can prove its bounded upsert writer",
		),
	)
}

func stage4MinimumUpsertMergeRows(values ...int) int {
	minimum := 0
	for _, value := range values {
		if value < 1 {
			return 0
		}
		if minimum == 0 || value < minimum {
			minimum = value
		}
	}
	return minimum
}

// The certified native writers below execute every Stage 4 upsert call as one
// transaction and never retain rows to combine with a later call. The core is
// therefore the sole splitter, and its hard cap is also the writers' proven
// per-call protocol ceiling. Targets with a different native ceiling can
// expose a narrower value without changing the route protocol.
func (*postgresNativeWriter) Stage4UpsertMergeMaximumRows() int {
	return config.MaxTransferChunkRows
}

func (*mysqlNativeWriter) Stage4UpsertMergeMaximumRows() int {
	return config.MaxTransferChunkRows
}

func (*sqlServerNativeWriter) Stage4UpsertMergeMaximumRows() int {
	return config.MaxTransferChunkRows
}

func (*sqliteStage4NetworkWriter) Stage4UpsertMergeMaximumRows() int {
	return config.MaxTransferChunkRows
}

func (adapter *postgresTargetAdapter) PreflightStage4UpsertMerge(
	ctx context.Context,
) (int, error) {
	if err := stage4UpsertMergePreflightContext(ctx); err != nil {
		return 0, err
	}
	if adapter == nil {
		return 0, stage4UpsertMergeWriterUnavailable("PostgreSQL")
	}
	writer, network := adapter.batchWriter.(postgresStage4NetworkBatchWriter)
	limited, bounded := writer.(stage4UpsertMergeNativeWriter)
	if !network || !bounded || isNilInterface(writer) ||
		isNilInterface(limited) {
		return 0, stage4UpsertMergeWriterUnavailable("PostgreSQL")
	}
	return limited.Stage4UpsertMergeMaximumRows(), nil
}

func (adapter *mysqlTargetAdapter) PreflightStage4UpsertMerge(
	ctx context.Context,
) (int, error) {
	if err := stage4UpsertMergePreflightContext(ctx); err != nil {
		return 0, err
	}
	if adapter == nil {
		return 0, stage4UpsertMergeWriterUnavailable("MySQL")
	}
	writer, network := adapter.batchWriter.(mysqlStage4NetworkBatchWriter)
	limited, bounded := writer.(stage4UpsertMergeNativeWriter)
	if !network || !bounded || isNilInterface(writer) ||
		isNilInterface(limited) {
		return 0, stage4UpsertMergeWriterUnavailable("MySQL")
	}
	return limited.Stage4UpsertMergeMaximumRows(), nil
}

func (adapter *sqlServerTargetAdapter) PreflightStage4UpsertMerge(
	ctx context.Context,
) (int, error) {
	if err := stage4UpsertMergePreflightContext(ctx); err != nil {
		return 0, err
	}
	if adapter == nil {
		return 0, stage4UpsertMergeWriterUnavailable("SQL Server")
	}
	writer, network := adapter.batchWriter.(sqlServerStage4NetworkBatchWriter)
	limited, bounded := writer.(stage4UpsertMergeNativeWriter)
	if !network || !bounded || isNilInterface(writer) ||
		isNilInterface(limited) {
		return 0, stage4UpsertMergeWriterUnavailable("SQL Server")
	}
	return limited.Stage4UpsertMergeMaximumRows(), nil
}

func (adapter *sqliteTargetAdapter) PreflightStage4UpsertMerge(
	ctx context.Context,
) (int, error) {
	if err := stage4UpsertMergePreflightContext(ctx); err != nil {
		return 0, err
	}
	if adapter == nil {
		return 0, stage4UpsertMergeWriterUnavailable("SQLite")
	}
	writer := adapter.stage4BatchWriter
	if writer == nil && adapter.database != nil {
		writer = newSQLiteStage4NetworkWriter(adapter.database)
	}
	limited, bounded := writer.(stage4UpsertMergeNativeWriter)
	if writer == nil || !bounded || isNilInterface(writer) ||
		isNilInterface(limited) {
		return 0, stage4UpsertMergeWriterUnavailable("SQLite")
	}
	return limited.Stage4UpsertMergeMaximumRows(), nil
}

func stage4UpsertMergePreflightContext(ctx context.Context) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 upsert merge preflight context is required"),
		)
	}
	return ctx.Err()
}

func stage4UpsertMergeWriterUnavailable(engine string) error {
	return NewTransferError(
		ErrorClassPolicy,
		fmt.Errorf(
			"%s Stage 4 native writer cannot prove an explicit upsert merge limit",
			engine,
		),
	)
}
