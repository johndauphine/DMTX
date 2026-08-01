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

// Stage4RebuildFinalizationVersion identifies the durable two-boundary
// protocol around a destructive rebuild finalizer.  The planned boundary is
// persisted before PrepareTables can mutate the target.  The started boundary
// is persisted immediately before FinalizeTables.  A resume may therefore
// distinguish an unfinished transfer (safe to finalize) from a finalizer that
// may already have committed (which must be authenticated, never retried).
const Stage4RebuildFinalizationVersion = 1

type Stage4RebuildFinalizationPhase string

const (
	Stage4RebuildFinalizationPlanned Stage4RebuildFinalizationPhase = "planned"
	Stage4RebuildFinalizationStarted Stage4RebuildFinalizationPhase = "started"
)

// Stage4RebuildFinalization is the immutable authority for one side of the
// rebuild finalization protocol.  It intentionally carries only the run and
// table-inventory identity: run identity already binds the canonical target
// endpoint and configuration, while the inventory binds every selected table
// and stable range.
type Stage4RebuildFinalization struct {
	Version         int                            `json:"version" yaml:"version"`
	RunID           string                         `json:"run_id" yaml:"run_id"`
	InventoryDigest string                         `json:"inventory_digest" yaml:"inventory_digest"`
	Phase           Stage4RebuildFinalizationPhase `json:"phase" yaml:"phase"`
}

// Stage4RebuildFinalizationReceipt makes each phase durable and
// self-validating. RecordedAt is created by the state backend, so idempotent
// Ensure calls do not manufacture a conflicting timestamp.
type Stage4RebuildFinalizationReceipt struct {
	Finalization Stage4RebuildFinalization `json:"finalization" yaml:"finalization"`
	RecordedAt   time.Time                 `json:"recorded_at" yaml:"recorded_at"`
	Digest       string                    `json:"digest" yaml:"digest"`
}

func NewStage4RebuildFinalization(
	runID string,
	inventoryDigest string,
	phase Stage4RebuildFinalizationPhase,
) (Stage4RebuildFinalization, error) {
	return normalizeStage4RebuildFinalization(Stage4RebuildFinalization{
		Version:         Stage4RebuildFinalizationVersion,
		RunID:           runID,
		InventoryDigest: inventoryDigest,
		Phase:           phase,
	})
}

func (finalization Stage4RebuildFinalization) Clone() Stage4RebuildFinalization {
	return finalization
}

func (finalization Stage4RebuildFinalization) Equal(
	other Stage4RebuildFinalization,
) bool {
	left, leftErr := normalizeStage4RebuildFinalization(finalization)
	right, rightErr := normalizeStage4RebuildFinalization(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (finalization Stage4RebuildFinalization) Validate() error {
	_, err := normalizeStage4RebuildFinalization(finalization)
	return err
}

func (receipt Stage4RebuildFinalizationReceipt) Clone() Stage4RebuildFinalizationReceipt {
	receipt.Finalization = receipt.Finalization.Clone()
	return receipt
}

func (receipt Stage4RebuildFinalizationReceipt) Equal(
	other Stage4RebuildFinalizationReceipt,
) bool {
	left, leftErr := normalizeStoredStage4RebuildFinalization(receipt)
	right, rightErr := normalizeStoredStage4RebuildFinalization(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (receipt Stage4RebuildFinalizationReceipt) Validate() error {
	_, err := normalizeStoredStage4RebuildFinalization(receipt)
	return err
}

func normalizeStage4RebuildFinalization(
	finalization Stage4RebuildFinalization,
) (Stage4RebuildFinalization, error) {
	if finalization.Version != Stage4RebuildFinalizationVersion {
		return Stage4RebuildFinalization{}, fmt.Errorf(
			"rebuild finalization version %d is unsupported",
			finalization.Version,
		)
	}
	if strings.TrimSpace(finalization.RunID) == "" ||
		finalization.RunID != strings.TrimSpace(finalization.RunID) {
		return Stage4RebuildFinalization{}, fmt.Errorf(
			"rebuild finalization run ID is required",
		)
	}
	digest, err := hex.DecodeString(finalization.InventoryDigest)
	if err != nil || len(digest) != sha256.Size {
		return Stage4RebuildFinalization{}, fmt.Errorf(
			"rebuild finalization inventory digest must be a SHA-256 digest",
		)
	}
	switch finalization.Phase {
	case Stage4RebuildFinalizationPlanned, Stage4RebuildFinalizationStarted:
	default:
		return Stage4RebuildFinalization{}, fmt.Errorf(
			"rebuild finalization phase %q is unsupported",
			finalization.Phase,
		)
	}
	finalization.InventoryDigest = strings.ToLower(finalization.InventoryDigest)
	return finalization, nil
}

func newStage4RebuildFinalizationReceipt(
	finalization Stage4RebuildFinalization,
	recordedAt time.Time,
) (Stage4RebuildFinalizationReceipt, error) {
	normalized, err := normalizeStage4RebuildFinalization(finalization)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, err
	}
	if recordedAt.IsZero() {
		return Stage4RebuildFinalizationReceipt{}, fmt.Errorf(
			"rebuild finalization receipt time is required",
		)
	}
	recordedAt = recordedAt.UTC()
	encoded, err := json.Marshal(struct {
		Finalization Stage4RebuildFinalization `json:"finalization"`
		RecordedAt   time.Time                 `json:"recorded_at"`
	}{
		Finalization: normalized,
		RecordedAt:   recordedAt,
	})
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, fmt.Errorf(
			"encode rebuild finalization receipt: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return Stage4RebuildFinalizationReceipt{
		Finalization: normalized,
		RecordedAt:   recordedAt,
		Digest:       hex.EncodeToString(digest[:]),
	}, nil
}

func normalizeStoredStage4RebuildFinalization(
	stored Stage4RebuildFinalizationReceipt,
) (Stage4RebuildFinalizationReceipt, error) {
	normalized, err := normalizeStage4RebuildFinalization(stored.Finalization)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, err
	}
	expected, err := newStage4RebuildFinalizationReceipt(
		normalized,
		stored.RecordedAt,
	)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, err
	}
	if stored.Digest != expected.Digest ||
		!reflect.DeepEqual(stored.Finalization, normalized) {
		return Stage4RebuildFinalizationReceipt{}, fmt.Errorf(
			"%w: rebuild finalization receipt differs",
			ErrImmutableEvidence,
		)
	}
	stored.Finalization = normalized
	stored.RecordedAt = stored.RecordedAt.UTC()
	return stored, nil
}

func validateStage4RebuildFinalizationReceipt(
	stored Stage4RebuildFinalizationReceipt,
	requested Stage4RebuildFinalization,
) error {
	normalizedStored, err := normalizeStoredStage4RebuildFinalization(stored)
	if err != nil {
		return err
	}
	normalizedRequested, err := normalizeStage4RebuildFinalization(requested)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalizedStored.Finalization, normalizedRequested) {
		return fmt.Errorf(
			"%w: rebuild finalization authority differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func validateStage4RebuildFinalizationAuthority(
	inventoryReceipt Stage4TableInventoryReceipt,
	finalization Stage4RebuildFinalization,
	ordinary []Task,
	workTasks []WorkTask,
	workRanges []RangeState,
) error {
	inventoryReceipt, err := normalizeStoredStage4TableInventory(inventoryReceipt)
	if err != nil {
		return err
	}
	if finalization.RunID != inventoryReceipt.Inventory.RunID ||
		finalization.InventoryDigest != inventoryReceipt.Digest {
		return fmt.Errorf(
			"%w: rebuild finalization differs from table inventory",
			ErrImmutableEvidence,
		)
	}
	if len(ordinary) != len(inventoryReceipt.Inventory.Tables) {
		return fmt.Errorf(
			"%w: rebuild finalization ordinary task inventory differs",
			ErrImmutableEvidence,
		)
	}
	ordinaryByTable := make(map[string]Task, len(ordinary))
	for _, task := range ordinary {
		if task.RunID != finalization.RunID {
			return fmt.Errorf(
				"%w: rebuild finalization ordinary task run differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := ordinaryByTable[task.Table]; duplicate {
			return fmt.Errorf(
				"%w: duplicate rebuild finalization ordinary task %q",
				ErrImmutableEvidence,
				task.Table,
			)
		}
		ordinaryByTable[task.Table] = task
	}
	taskIndexes, rangesByTask, err := indexStage4AggregateWork(
		finalization.RunID,
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
				"%w: rebuild finalization ordinary task %q is not pending publication",
				ErrStateTransition,
				entry.Table,
			)
		}
		index, found := taskIndexes[entry.Task]
		if !found {
			return fmt.Errorf(
				"%w: rebuild finalization structured task %#v",
				ErrUnknownWork,
				entry.Task,
			)
		}
		workTask := workTasks[index]
		if workTask.Strategy != entry.Strategy ||
			workTask.TopologyHash != entry.TopologyHash ||
			workTask.Status != "running" || workTask.StartedAt.IsZero() ||
			!workTask.CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: rebuild finalization structured task %#v is not pending publication",
				ErrStateTransition,
				entry.Task,
			)
		}
		ranges := rangesByTask[entry.Task]
		if len(ranges) != len(entry.Ranges) {
			return fmt.Errorf(
				"%w: rebuild finalization ranges for table %q differ",
				ErrImmutableEvidence,
				entry.Table,
			)
		}
		for index, plannedRange := range entry.Ranges {
			workRange := ranges[index]
			if workRange.ID != plannedRange.ID ||
				workRange.Strategy != entry.Strategy ||
				workRange.TopologyHash != entry.TopologyHash {
				return fmt.Errorf(
					"%w: rebuild finalization range %q for table %q differs",
					ErrImmutableEvidence,
					plannedRange.ID,
					entry.Table,
				)
			}
			switch finalization.Phase {
			case Stage4RebuildFinalizationPlanned:
				if workTask.Attempts != 0 || workTask.Retries != 0 ||
					workTask.Error != "" || workRange.Status != "running" ||
					workRange.Error != "" || workRange.NextSequence != 0 ||
					workRange.SequenceOffset != 0 || workRange.RowsDone != 0 ||
					workRange.CommittedPrefix != 0 || len(workRange.Pending) != 0 ||
					len(workRange.Frontier) != 0 || workRange.FrontierValid ||
					workRange.Attempts != 0 || workRange.Retries != 0 ||
					!workRange.CompletedAt.IsZero() {
					return fmt.Errorf(
						"%w: rebuild finalization plan for range %q carries transfer authority",
						ErrStateTransition,
						plannedRange.ID,
					)
				}
			case Stage4RebuildFinalizationStarted:
				if workTask.Error != "" || workRange.Status != "completed" ||
					workRange.Error != "" || workRange.SequenceOffset != 0 ||
					workRange.CommittedPrefix != 0 || len(workRange.Pending) != 0 ||
					workRange.CompletedAt.IsZero() ||
					!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
					workRange.CompletedAt.Before(workTask.StartedAt) {
					return fmt.Errorf(
						"%w: rebuild finalization range %q is incomplete",
						ErrStateTransition,
						plannedRange.ID,
					)
				}
			}
		}
	}
	for key := range taskIndexes {
		if !stage4TableWorkType(key.Type) {
			continue
		}
		if _, found := expected[key]; !found {
			return fmt.Errorf(
				"%w: rebuild finalization has unexpected structured task %#v",
				ErrImmutableEvidence,
				key,
			)
		}
	}
	return nil
}
