package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// stage4CheckpointFrequencyExplicitlyRequested distinguishes the documented
// parser default from an operator request. The default keeps the established
// immediate-on-ack checkpoint cadence; only an explicit value selects the
// periodic cadence below.
func stage4CheckpointFrequencyExplicitlyRequested(
	migration config.Migration,
) bool {
	provenance, found := migration.SettingProvenance(
		"checkpoint_frequency",
	)
	return found && provenance == config.ProvenanceRequested
}

// stage4AdapterNetworkCheckpointFrequency returns the core cadence expressed
// as contiguous durable write acknowledgements per range. Zero is the
// explicit immediate-on-ack setting. It is deliberately bounded by the
// maximum core chunk count so a malformed configuration cannot create an
// unbounded in-memory checkpoint interval.
func stage4AdapterNetworkCheckpointFrequency(
	migration config.Migration,
) (int, error) {
	if !stage4CheckpointFrequencyExplicitlyRequested(migration) {
		// Preserve the pre-existing default behavior. The parser's generated
		// compatibility default was never a cadence consumer.
		return 0, nil
	}
	if migration.CheckpointFrequency < 0 ||
		migration.CheckpointFrequency > maximumNetworkCheckpointFrequency {
		return 0, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 checkpoint_frequency must be between 0 and %d contiguous acknowledgements",
				maximumNetworkCheckpointFrequency,
			),
		)
	}
	return migration.CheckpointFrequency, nil
}

// requireStage4CheckpointFrequencyComposition keeps the setting fail-closed
// on routes whose state machine does not use NetworkTransferPlan. It runs
// before endpoint opening, ordinary task creation, or target mutation.
func requireStage4CheckpointFrequencyComposition(
	cfg config.Config,
) error {
	if !stage4CheckpointFrequencyExplicitlyRequested(cfg.Migration) {
		return nil
	}
	if _, err := stage4AdapterNetworkCheckpointFrequency(
		cfg.Migration,
	); err != nil {
		return err
	}
	switch {
	case len(cfg.Migration.DateUpdatedColumns) != 0:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 checkpoint_frequency is not yet composed with date-based incremental transfer",
			),
		)
	case cfg.Migration.StrictConsistency:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 checkpoint_frequency is not yet composed with strict consistency",
			),
		)
	case cfg.Migration.Deletes.Mode == config.DeleteModeReconcile:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 checkpoint_frequency is not yet composed with delete reconciliation",
			),
		)
	default:
		return nil
	}
}
