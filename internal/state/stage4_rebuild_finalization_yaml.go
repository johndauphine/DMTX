package state

import (
	"fmt"
	"strings"
	"time"
)

func (store YAMLStore) EnsureStage4RebuildFinalization(
	finalization Stage4RebuildFinalization,
) (Stage4RebuildFinalizationReceipt, bool, error) {
	normalized, err := normalizeStage4RebuildFinalization(finalization)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	var result Stage4RebuildFinalizationReceipt
	created := false
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		stored, found, err := findYAMLStage4RebuildFinalization(
			*document,
			normalized.RunID,
			normalized.Phase,
		)
		if err != nil {
			return false, err
		}
		if found {
			if err := validateStage4RebuildFinalizationReceipt(
				stored,
				normalized,
			); err != nil {
				return false, err
			}
			result = stored.Clone()
			return false, nil
		}

		latest, inventory, ordinary, workTasks, workRanges, err :=
			stage4RebuildFinalizationAuthorityYAML(*document, normalized)
		if err != nil {
			return false, err
		}
		if latest.Outcome != Running || !latest.Resumable {
			return false, fmt.Errorf(
				"%w: rebuild finalization requires an active resumable run",
				ErrStateTransition,
			)
		}
		if err := validateStage4RebuildFinalizationAuthority(
			inventory,
			normalized,
			ordinary,
			workTasks,
			workRanges,
		); err != nil {
			return false, err
		}
		if normalized.Phase == Stage4RebuildFinalizationStarted {
			planned, plannedFound, err := findYAMLStage4RebuildFinalization(
				*document,
				normalized.RunID,
				Stage4RebuildFinalizationPlanned,
			)
			if err != nil {
				return false, err
			}
			if !plannedFound ||
				planned.Finalization.InventoryDigest != normalized.InventoryDigest {
				return false, fmt.Errorf(
					"%w: rebuild finalization start lacks matching planned boundary",
					ErrImmutableEvidence,
				)
			}
		}
		receipt, err := newStage4RebuildFinalizationReceipt(
			normalized,
			time.Now().UTC(),
		)
		if err != nil {
			return false, err
		}
		document.Stage4RebuildFinalizations = append(
			document.Stage4RebuildFinalizations,
			receipt,
		)
		result = receipt.Clone()
		created = true
		return true, nil
	})
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	return result.Clone(), created, nil
}

func (store YAMLStore) LoadStage4RebuildFinalization(
	runID string,
	phase Stage4RebuildFinalizationPhase,
) (Stage4RebuildFinalizationReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" || runID != strings.TrimSpace(runID) {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			fmt.Errorf("run ID is required"),
		)
	}
	if _, err := normalizeStage4RebuildFinalization(
		Stage4RebuildFinalization{
			Version:         Stage4RebuildFinalizationVersion,
			RunID:           runID,
			InventoryDigest: strings.Repeat("0", 64),
			Phase:           phase,
		},
	); err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			err,
		)
	}
	var result Stage4RebuildFinalizationReceipt
	found := false
	err := store.read(func(document yamlStateDocument) error {
		var err error
		result, found, err = findYAMLStage4RebuildFinalization(
			document,
			runID,
			phase,
		)
		return err
	})
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			err,
		)
	}
	return result.Clone(), found, nil
}

func findYAMLStage4RebuildFinalization(
	document yamlStateDocument,
	runID string,
	phase Stage4RebuildFinalizationPhase,
) (Stage4RebuildFinalizationReceipt, bool, error) {
	var result Stage4RebuildFinalizationReceipt
	found := false
	for _, candidate := range document.Stage4RebuildFinalizations {
		if candidate.Finalization.RunID != runID ||
			candidate.Finalization.Phase != phase {
			continue
		}
		normalized, err := normalizeStoredStage4RebuildFinalization(candidate)
		if err != nil {
			return Stage4RebuildFinalizationReceipt{}, false, err
		}
		if found {
			return Stage4RebuildFinalizationReceipt{}, false, fmt.Errorf(
				"%w: duplicate rebuild finalization receipt",
				ErrImmutableEvidence,
			)
		}
		result, found = normalized.Clone(), true
	}
	return result, found, nil
}

func stage4RebuildFinalizationAuthorityYAML(
	document yamlStateDocument,
	finalization Stage4RebuildFinalization,
) (Run, Stage4TableInventoryReceipt, []Task, []WorkTask, []RangeState, error) {
	var runs []Run
	for _, run := range document.Runs {
		if run.ID == finalization.RunID {
			runs = append(runs, run)
		}
	}
	latest, err := validateStage4RunIdentity(runs, finalization.RunID)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	var inventory *Stage4TableInventoryReceipt
	for index := range document.Stage4TableInventories {
		candidate := &document.Stage4TableInventories[index]
		if candidate.Inventory.RunID != finalization.RunID {
			continue
		}
		if inventory != nil {
			return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
				"%w: duplicate Stage 4 table inventory",
				ErrImmutableEvidence,
			)
		}
		inventory = candidate
	}
	if inventory == nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"%w: rebuild finalization table inventory",
			ErrUnknownWork,
		)
	}
	if finalization.InventoryDigest != inventory.Digest {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"%w: rebuild finalization inventory digest differs",
			ErrImmutableEvidence,
		)
	}
	for _, completion := range document.Stage4TableCompletions {
		if completion.Completion.RunID == finalization.RunID {
			return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
				"%w: rebuild finalization follows table publication",
				ErrStateTransition,
			)
		}
	}
	ordinary := make([]Task, 0, len(document.Tasks))
	for _, task := range document.Tasks {
		if task.RunID == finalization.RunID {
			ordinary = append(ordinary, task)
		}
	}
	workTasks := make([]WorkTask, 0, len(document.WorkTasks))
	for _, task := range document.WorkTasks {
		if task.RunID == finalization.RunID {
			workTasks = append(workTasks, task)
		}
	}
	workRanges := make([]RangeState, 0, len(document.WorkRanges))
	for _, workRange := range document.WorkRanges {
		if workRange.RunID == finalization.RunID {
			workRanges = append(workRanges, workRange)
		}
	}
	return latest, *inventory, ordinary, workTasks, workRanges, nil
}

var _ Stage4RebuildRecoveryBackend = YAMLStore{}
