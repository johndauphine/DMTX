package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// stage4LargeTableThresholdExplicitlyRequested distinguishes the documented
// parser default from an operator request. The generated default must retain
// the established partition behavior; only an explicit setting selects the
// exact-size planning policy below.
func stage4LargeTableThresholdExplicitlyRequested(
	migration config.Migration,
) bool {
	provenance, found := migration.SettingProvenance(
		"large_table_threshold",
	)
	return found && provenance == config.ProvenanceRequested
}

// stage4AdapterLargeTableThreshold returns zero when the generated
// compatibility default remains inactive. A positive result is an operator
// request that must be consumed by retained-view planning, never silently
// accepted by a route without that authority.
func stage4AdapterLargeTableThreshold(
	migration config.Migration,
) (int64, error) {
	if !stage4LargeTableThresholdExplicitlyRequested(migration) {
		return 0, nil
	}
	if migration.LargeTableThreshold <= 0 {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold must be positive",
			),
		)
	}
	return migration.LargeTableThreshold, nil
}

// requireStage4LargeTableThresholdRoute keeps an explicit threshold out of
// every Stage 4 route that cannot bind its exact table-size decision to the
// same retained view used for pagination and row reads. It is intentionally
// configuration-only so unsupported combinations fail before endpoint opens,
// checkpoints, or target mutation.
func requireStage4LargeTableThresholdRoute(
	cfg config.Config,
	sourceEngine string,
	targetEngine string,
) error {
	threshold, err := stage4AdapterLargeTableThreshold(cfg.Migration)
	if err != nil || threshold == 0 {
		return err
	}
	switch {
	case cfg.Migration.StrictConsistency:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold is not yet composed with strict consistency",
			),
		)
	case len(cfg.Migration.DateUpdatedColumns) != 0:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold is not yet composed with date-based incremental transfer",
			),
		)
	case cfg.Migration.Deletes.Mode == config.DeleteModeReconcile:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold is not yet composed with delete reconciliation",
			),
		)
	case sourceEngine == "sqlite" && targetEngine == "sqlite":
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold requires the composed table-stable network runner; SQLite-to-SQLite compatibility routing has no certified consumer",
			),
		)
	case !stage4AdapterNetworkRelationalEngine(sourceEngine) ||
		!stage4AdapterNetworkRelationalEngine(targetEngine):
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold requires a certified composed relational or SQLite network route, got %s-to-%s",
				sourceEngine,
				targetEngine,
			),
		)
	}
	return nil
}

func requireStage4LargeTableThresholdComposition(
	cfg config.Config,
) error {
	if !stage4LargeTableThresholdExplicitlyRequested(cfg.Migration) {
		return nil
	}
	sourceEngine, sourceErr := config.CanonicalEngine(cfg.Source.Type)
	targetEngine, targetErr := config.CanonicalEngine(cfg.Target.Type)
	if sourceErr != nil || targetErr != nil {
		// Route validation owns malformed or unsupported engine diagnostics.
		return nil
	}
	return requireStage4LargeTableThresholdRoute(
		cfg,
		sourceEngine,
		targetEngine,
	)
}

type stage4AdapterLargeTableDecision struct {
	threshold           int64
	exactSourceRows     int64
	requestedPartitions int
	effectivePartitions int
}

// stage4AdapterLargeTableDecisionForStableSource gets an exact count only
// from the complete stable source surface. Both production implementations of
// that surface use the same retained transaction for CountRows,
// PlanPagination, retained-width evidence, and range reads. A mutable source
// pool or catalog estimate is deliberately not accepted here.
func stage4AdapterLargeTableDecisionForStableSource(
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
	threshold int64,
	requestedPartitions int,
) (stage4AdapterLargeTableDecision, error) {
	if threshold <= 0 {
		return stage4AdapterLargeTableDecision{}, nil
	}
	if ctx == nil {
		return stage4AdapterLargeTableDecision{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 large-table planning context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return stage4AdapterLargeTableDecision{}, err
	}
	stable, ok := source.(adapterStableNetworkSource)
	if !ok || isNilInterface(stable) {
		return stage4AdapterLargeTableDecision{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large_table_threshold requires exact retained source table-size authority for %s",
				table.Name,
			),
		)
	}
	if requestedPartitions == 0 {
		requestedPartitions = config.DefaultPartitions
	}
	if requestedPartitions < 1 ||
		uint64(requestedPartitions) > maximumRuntimeTuningRanges {
		return stage4AdapterLargeTableDecision{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 large-table partition count %d is outside 1..%d",
				requestedPartitions,
				maximumRuntimeTuningRanges,
			),
		)
	}
	rows, err := stable.CountRows(ctx, table)
	if err != nil {
		return stage4AdapterLargeTableDecision{}, fmt.Errorf(
			"authenticate retained source row count for Stage 4 table %s: %w",
			table.Name,
			err,
		)
	}
	if rows < 0 {
		return stage4AdapterLargeTableDecision{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 retained source row count for table %s is negative",
				table.Name,
			),
		)
	}
	decision := stage4AdapterLargeTableDecision{
		threshold:           threshold,
		exactSourceRows:     int64(rows),
		requestedPartitions: requestedPartitions,
		effectivePartitions: requestedPartitions,
	}
	if decision.exactSourceRows < threshold {
		decision.effectivePartitions = 1
	}
	return decision, nil
}

// stage4AdapterLargeTableTopology folds the exact retained-view fact and the
// threshold decision into the pre-pagination topology seed. The ordinary
// pagination digest then commits the concrete range bounds. A resume therefore
// cannot reuse work planned with a different exact count or partition choice.
func stage4AdapterLargeTableTopology(
	base string,
	decision stage4AdapterLargeTableDecision,
) (string, error) {
	if base == "" || decision.threshold <= 0 ||
		decision.exactSourceRows < 0 ||
		decision.requestedPartitions < 1 ||
		decision.effectivePartitions < 1 {
		return "", fmt.Errorf("large-table topology evidence is incomplete")
	}
	wire := struct {
		Version             int    `json:"version"`
		BaseTopology        string `json:"base_topology"`
		Threshold           int64  `json:"threshold"`
		ExactSourceRows     int64  `json:"exact_source_rows"`
		RequestedPartitions int    `json:"requested_partitions"`
		EffectivePartitions int    `json:"effective_partitions"`
	}{
		Version:             1,
		BaseTopology:        base,
		Threshold:           decision.threshold,
		ExactSourceRows:     decision.exactSourceRows,
		RequestedPartitions: decision.requestedPartitions,
		EffectivePartitions: decision.effectivePartitions,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
