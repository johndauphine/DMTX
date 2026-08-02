package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Stage4RebuildRecoveryBackend is the additive terminal-recovery surface for
// a drop/recreate run. The receipt is written only after the complete selected
// target set has transferred, finalized, and validated, but before the first
// table aggregate is published. It lets a later process distinguish a safe
// publication repair from a rebuild that still needs data-plane recovery.
//
// This stays separate from Stage4AggregateBackend so integrations that have
// not implemented the durable rebuild protocol continue to compile; the
// composed rebuild runner fails closed when the receipt surface is absent.
type Stage4RebuildRecoveryBackend interface {
	// EnsureStage4RebuildFinalization records the two immutable boundaries
	// around a potentially non-idempotent target finalizer. See
	// Stage4RebuildFinalization for the phase contract.
	EnsureStage4RebuildFinalization(Stage4RebuildFinalization) (Stage4RebuildFinalizationReceipt, bool, error)
	LoadStage4RebuildFinalization(string, Stage4RebuildFinalizationPhase) (Stage4RebuildFinalizationReceipt, bool, error)
	SaveStage4RebuildReady(Stage4RebuildReady) error
	LoadStage4RebuildReady(string) (Stage4RebuildReadyReceipt, bool, error)
}

// Stage4RebuildReady binds a terminal set-wide rebuild pass to the immutable
// table inventory it validated. Its existence is durable evidence of the
// finalization/validation boundary, not merely evidence that rows were copied.
type Stage4RebuildReady struct {
	RunID           string    `json:"run_id" yaml:"run_id"`
	InventoryDigest string    `json:"inventory_digest" yaml:"inventory_digest"`
	ValidatedAt     time.Time `json:"validated_at" yaml:"validated_at"`
}

// Stage4RebuildReadyReceipt makes the terminal-ready boundary immutable and
// self-validating across YAML replacement and SQLite record storage.
type Stage4RebuildReadyReceipt struct {
	Ready  Stage4RebuildReady `json:"ready" yaml:"ready"`
	Digest string             `json:"digest" yaml:"digest"`
}

func normalizeStage4RebuildReady(
	ready Stage4RebuildReady,
) (Stage4RebuildReady, error) {
	if strings.TrimSpace(ready.RunID) == "" || ready.RunID != strings.TrimSpace(ready.RunID) {
		return Stage4RebuildReady{}, fmt.Errorf("rebuild-ready run ID is required")
	}
	digest, err := hex.DecodeString(ready.InventoryDigest)
	if err != nil || len(digest) != sha256.Size {
		return Stage4RebuildReady{}, fmt.Errorf(
			"rebuild-ready inventory digest must be a SHA-256 digest",
		)
	}
	if ready.ValidatedAt.IsZero() {
		return Stage4RebuildReady{}, fmt.Errorf(
			"rebuild-ready validation time is required",
		)
	}
	ready.InventoryDigest = strings.ToLower(ready.InventoryDigest)
	ready.ValidatedAt = ready.ValidatedAt.UTC()
	return ready, nil
}

func newStage4RebuildReadyReceipt(
	ready Stage4RebuildReady,
) (Stage4RebuildReadyReceipt, error) {
	normalized, err := normalizeStage4RebuildReady(ready)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, fmt.Errorf(
			"encode rebuild-ready receipt: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return Stage4RebuildReadyReceipt{
		Ready:  normalized,
		Digest: hex.EncodeToString(digest[:]),
	}, nil
}

func normalizeStoredStage4RebuildReady(
	stored Stage4RebuildReadyReceipt,
) (Stage4RebuildReadyReceipt, error) {
	normalized, err := normalizeStage4RebuildReady(stored.Ready)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, err
	}
	expected, err := newStage4RebuildReadyReceipt(normalized)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, err
	}
	if stored.Digest != expected.Digest ||
		!reflect.DeepEqual(stored.Ready, normalized) {
		return Stage4RebuildReadyReceipt{}, fmt.Errorf(
			"%w: rebuild-ready receipt differs",
			ErrImmutableEvidence,
		)
	}
	stored.Ready = normalized
	return stored, nil
}

func validateStage4RebuildReadyReceipt(
	stored Stage4RebuildReadyReceipt,
	requested Stage4RebuildReady,
) error {
	normalizedStored, err := normalizeStoredStage4RebuildReady(stored)
	if err != nil {
		return err
	}
	normalizedRequested, err := normalizeStage4RebuildReady(requested)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalizedStored.Ready, normalizedRequested) {
		return fmt.Errorf(
			"%w: rebuild-ready receipt differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

// validateStage4RebuildReadyAuthority proves that a terminal-ready receipt is
// never minted from a partial rebuild. The aggregate table records must still
// be absent: after this boundary, they are the only remaining durable work.
func validateStage4RebuildReadyAuthority(
	inventoryReceipt Stage4TableInventoryReceipt,
	ready Stage4RebuildReady,
	ordinary []Task,
	workTasks []WorkTask,
	workRanges []RangeState,
) error {
	inventoryReceipt, err := normalizeStoredStage4TableInventory(
		inventoryReceipt,
	)
	if err != nil {
		return err
	}
	if ready.RunID != inventoryReceipt.Inventory.RunID ||
		ready.InventoryDigest != inventoryReceipt.Digest {
		return fmt.Errorf(
			"%w: rebuild-ready receipt differs from table inventory",
			ErrImmutableEvidence,
		)
	}
	if len(ordinary) != len(inventoryReceipt.Inventory.Tables) {
		return fmt.Errorf(
			"%w: rebuild-ready ordinary task inventory differs",
			ErrImmutableEvidence,
		)
	}
	ordinaryByTable := make(map[string]Task, len(ordinary))
	for _, task := range ordinary {
		if task.RunID != ready.RunID {
			return fmt.Errorf(
				"%w: rebuild-ready ordinary task run differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := ordinaryByTable[task.Table]; duplicate {
			return fmt.Errorf(
				"%w: duplicate rebuild-ready ordinary task %q",
				ErrImmutableEvidence,
				task.Table,
			)
		}
		ordinaryByTable[task.Table] = task
	}
	taskIndexes, rangesByTask, err := indexStage4AggregateWork(
		ready.RunID,
		workTasks,
		workRanges,
	)
	if err != nil {
		return err
	}
	expected := make(map[TaskKey]struct{}, len(inventoryReceipt.Inventory.Tables))
	for _, entry := range inventoryReceipt.Inventory.Tables {
		expected[entry.Task] = struct{}{}
		ordinaryTask, found := ordinaryByTable[entry.Table]
		if !found || ordinaryTask.Status != "running" ||
			ordinaryTask.RowsDone != 0 || !ordinaryTask.CompletedAt.IsZero() ||
			ordinaryTask.StartedAt.IsZero() ||
			ordinaryTask.IntegerWatermark != nil ||
			ordinaryTask.RowNumberWatermark != nil {
			return fmt.Errorf(
				"%w: rebuild-ready ordinary task %q is not pending publication",
				ErrStateTransition,
				entry.Table,
			)
		}
		index, found := taskIndexes[entry.Task]
		if !found {
			return fmt.Errorf(
				"%w: rebuild-ready structured task %#v",
				ErrUnknownWork,
				entry.Task,
			)
		}
		workTask := workTasks[index]
		if workTask.Strategy != entry.Strategy ||
			workTask.TopologyHash != entry.TopologyHash ||
			workTask.Status != "running" ||
			workTask.StartedAt.IsZero() ||
			!workTask.CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: rebuild-ready structured task %#v is not pending publication",
				ErrStateTransition,
				entry.Task,
			)
		}
		ranges := rangesByTask[entry.Task]
		if len(ranges) != len(entry.Ranges) {
			return fmt.Errorf(
				"%w: rebuild-ready ranges for table %q differ",
				ErrImmutableEvidence,
				entry.Table,
			)
		}
		for index, plannedRange := range entry.Ranges {
			workRange := ranges[index]
			if workRange.ID != plannedRange.ID ||
				workRange.Strategy != entry.Strategy ||
				workRange.TopologyHash != entry.TopologyHash ||
				workRange.Status != "completed" ||
				workRange.Error != "" ||
				workRange.SequenceOffset != 0 ||
				workRange.CommittedPrefix != 0 ||
				len(workRange.Pending) != 0 ||
				workRange.CompletedAt.IsZero() ||
				!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
				workRange.CompletedAt.Before(workTask.StartedAt) {
				return fmt.Errorf(
					"%w: rebuild-ready range %q for table %q is incomplete",
					ErrStateTransition,
					plannedRange.ID,
					entry.Table,
				)
			}
		}
	}
	for key := range taskIndexes {
		if !stage4TableWorkType(key.Type) {
			continue
		}
		if _, found := expected[key]; !found {
			return fmt.Errorf(
				"%w: rebuild-ready has unexpected structured task %#v",
				ErrImmutableEvidence,
				key,
			)
		}
	}
	return nil
}
