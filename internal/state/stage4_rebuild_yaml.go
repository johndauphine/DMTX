package state

import (
	"fmt"
	"strings"
)

func (store YAMLStore) SaveStage4RebuildReady(
	ready Stage4RebuildReady,
) error {
	normalized, err := normalizeStage4RebuildReady(ready)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		var stored *Stage4RebuildReadyReceipt
		for index := range document.Stage4RebuildReadiness {
			candidate := &document.Stage4RebuildReadiness[index]
			if candidate.Ready.RunID != normalized.RunID {
				continue
			}
			if stored != nil {
				return false, fmt.Errorf(
					"%w: duplicate rebuild-ready receipt",
					ErrImmutableEvidence,
				)
			}
			stored = candidate
		}
		if stored != nil {
			return false, validateStage4RebuildReadyReceipt(
				*stored,
				normalized,
			)
		}

		var runs []Run
		for _, run := range document.Runs {
			if run.ID == normalized.RunID {
				runs = append(runs, run)
			}
		}
		latest, err := validateStage4RunIdentity(runs, normalized.RunID)
		if err != nil {
			return false, err
		}
		if latest.Outcome != Running || !latest.Resumable {
			return false, fmt.Errorf(
				"%w: rebuild-ready receipt requires an active resumable run",
				ErrStateTransition,
			)
		}

		var inventory *Stage4TableInventoryReceipt
		for index := range document.Stage4TableInventories {
			candidate := &document.Stage4TableInventories[index]
			if candidate.Inventory.RunID != normalized.RunID {
				continue
			}
			if inventory != nil {
				return false, fmt.Errorf(
					"%w: duplicate Stage 4 table inventory",
					ErrImmutableEvidence,
				)
			}
			inventory = candidate
		}
		if inventory == nil {
			return false, fmt.Errorf(
				"%w: rebuild-ready table inventory",
				ErrUnknownWork,
			)
		}
		if strings.ToLower(normalized.InventoryDigest) != inventory.Digest {
			return false, fmt.Errorf(
				"%w: rebuild-ready inventory digest differs",
				ErrImmutableEvidence,
			)
		}
		for _, completion := range document.Stage4TableCompletions {
			if completion.Completion.RunID == normalized.RunID {
				return false, fmt.Errorf(
					"%w: rebuild-ready receipt follows table publication",
					ErrStateTransition,
				)
			}
		}
		ordinary := make([]Task, 0, len(document.Tasks))
		for _, task := range document.Tasks {
			if task.RunID == normalized.RunID {
				ordinary = append(ordinary, task)
			}
		}
		workTasks := make([]WorkTask, 0, len(document.WorkTasks))
		for _, task := range document.WorkTasks {
			if task.RunID == normalized.RunID {
				workTasks = append(workTasks, task)
			}
		}
		workRanges := make([]RangeState, 0, len(document.WorkRanges))
		for _, workRange := range document.WorkRanges {
			if workRange.RunID == normalized.RunID {
				workRanges = append(workRanges, workRange)
			}
		}
		if err := validateStage4RebuildReadyAuthority(
			*inventory,
			normalized,
			ordinary,
			workTasks,
			workRanges,
		); err != nil {
			return false, err
		}
		receipt, err := newStage4RebuildReadyReceipt(normalized)
		if err != nil {
			return false, err
		}
		document.Stage4RebuildReadiness = append(
			document.Stage4RebuildReadiness,
			receipt,
		)
		return true, nil
	})
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	return nil
}

func (store YAMLStore) LoadStage4RebuildReady(
	runID string,
) (Stage4RebuildReadyReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			fmt.Errorf("run ID is required"),
		)
	}
	var result Stage4RebuildReadyReceipt
	found := false
	err := store.read(func(document yamlStateDocument) error {
		for _, candidate := range document.Stage4RebuildReadiness {
			if candidate.Ready.RunID != runID {
				continue
			}
			if found {
				return fmt.Errorf(
					"%w: duplicate rebuild-ready receipt",
					ErrImmutableEvidence,
				)
			}
			normalized, err := normalizeStoredStage4RebuildReady(candidate)
			if err != nil {
				return err
			}
			if normalized.Ready.RunID != runID {
				return fmt.Errorf(
					"%w: rebuild-ready run identity differs",
					ErrImmutableEvidence,
				)
			}
			result, found = normalized, true
		}
		return nil
	})
	if err != nil {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			err,
		)
	}
	return result, found, nil
}

var _ Stage4RebuildRecoveryBackend = YAMLStore{}
