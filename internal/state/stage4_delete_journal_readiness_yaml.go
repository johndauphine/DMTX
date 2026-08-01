package state

import (
	"fmt"
	"strings"
)

func (store YAMLStore) SaveStage4DeleteJournalReadiness(
	ready Stage4DeleteJournalReadiness,
) error {
	_, _, err := store.EnsureStage4DeleteJournalReadiness(ready)
	return err
}

func (store YAMLStore) EnsureStage4DeleteJournalReadiness(
	ready Stage4DeleteJournalReadiness,
) (Stage4DeleteJournalReadinessReceipt, bool, error) {
	normalized, err := normalizeStage4DeleteJournalReadiness(ready)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	var result Stage4DeleteJournalReadinessReceipt
	created := false
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		var stored *Stage4DeleteJournalReadinessReceipt
		for index := range document.Stage4DeleteJournalReadiness {
			candidate := document.Stage4DeleteJournalReadiness[index]
			if _, err := normalizeStoredStage4DeleteJournalReadiness(
				candidate,
			); err != nil {
				return false, fmt.Errorf(
					"validate delete-journal readiness receipt: %w",
					err,
				)
			}
			if candidate.Readiness.RunID != normalized.RunID {
				continue
			}
			if stored != nil {
				return false, fmt.Errorf(
					"%w: duplicate delete-journal readiness receipt",
					ErrImmutableEvidence,
				)
			}
			stored = &document.Stage4DeleteJournalReadiness[index]
		}
		if stored != nil {
			if err := validateStage4DeleteJournalReadinessReceipt(
				*stored,
				normalized,
			); err != nil {
				return false, err
			}
			result = stored.Clone()
			return false, nil
		}

		latest, err := validateStage4DeleteJournalReadinessBoundaryYAML(
			*document,
			Stage4DeleteJournalReadinessBoundary{
				RunID:           normalized.RunID,
				InventoryDigest: normalized.InventoryDigest,
			},
		)
		if err != nil {
			return false, err
		}
		if latest.TargetIdentity != normalized.TargetIdentity ||
			normalized.ReadyAt.Before(latest.StartedAt) {
			return false, fmt.Errorf(
				"%w: delete-journal readiness target authority differs from run identity",
				ErrImmutableEvidence,
			)
		}
		receipt, err := newStage4DeleteJournalReadinessReceipt(normalized)
		if err != nil {
			return false, err
		}
		document.Stage4DeleteJournalReadiness = append(
			document.Stage4DeleteJournalReadiness,
			receipt,
		)
		result = receipt.Clone()
		created = true
		return true, nil
	})
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	return result.Clone(), created, nil
}

func (store YAMLStore) LoadStage4DeleteJournalReadiness(
	runID string,
) (Stage4DeleteJournalReadinessReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" || runID != strings.TrimSpace(runID) {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			fmt.Errorf("run ID is required"),
		)
	}
	var result Stage4DeleteJournalReadinessReceipt
	found := false
	err := store.read(func(document yamlStateDocument) error {
		for _, candidate := range document.Stage4DeleteJournalReadiness {
			normalized, err := normalizeStoredStage4DeleteJournalReadiness(candidate)
			if err != nil {
				return fmt.Errorf(
					"validate delete-journal readiness receipt: %w",
					err,
				)
			}
			if normalized.Readiness.RunID != runID {
				continue
			}
			if found {
				return fmt.Errorf(
					"%w: duplicate delete-journal readiness receipt",
					ErrImmutableEvidence,
				)
			}
			result, found = normalized.Clone(), true
		}
		return nil
	})
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			err,
		)
	}
	return result.Clone(), found, nil
}

func (store YAMLStore) ValidateStage4DeleteJournalReadinessBoundary(
	boundary Stage4DeleteJournalReadinessBoundary,
) error {
	normalized, err := normalizeStage4DeleteJournalReadinessBoundary(boundary)
	if err != nil {
		return stage4AggregateError("delete-journal readiness boundary", err)
	}
	err = store.read(func(document yamlStateDocument) error {
		_, err := validateStage4DeleteJournalReadinessBoundaryYAML(
			document,
			normalized,
		)
		return err
	})
	if err != nil {
		return stage4AggregateError("delete-journal readiness boundary", err)
	}
	return nil
}

func validateStage4DeleteJournalReadinessBoundaryYAML(
	document yamlStateDocument,
	boundary Stage4DeleteJournalReadinessBoundary,
) (Run, error) {
	var runs []Run
	for _, run := range document.Runs {
		if run.ID == boundary.RunID {
			runs = append(runs, run)
		}
	}
	latest, err := validateStage4RunIdentity(runs, boundary.RunID)
	if err != nil {
		return Run{}, err
	}
	if latest.Outcome != Running || !latest.Resumable {
		return Run{}, fmt.Errorf(
			"%w: delete-journal readiness requires an active resumable run",
			ErrStateTransition,
		)
	}
	var inventory *Stage4TableInventoryReceipt
	for index := range document.Stage4TableInventories {
		candidate := &document.Stage4TableInventories[index]
		if candidate.Inventory.RunID != boundary.RunID {
			continue
		}
		if inventory != nil {
			return Run{}, fmt.Errorf(
				"%w: duplicate Stage 4 table inventory",
				ErrImmutableEvidence,
			)
		}
		inventory = candidate
	}
	if inventory == nil {
		return Run{}, fmt.Errorf(
			"%w: delete-journal readiness table inventory",
			ErrUnknownWork,
		)
	}
	for _, completion := range document.Stage4TableCompletions {
		if completion.Completion.RunID == boundary.RunID {
			return Run{}, fmt.Errorf(
				"%w: delete-journal readiness follows table publication",
				ErrStateTransition,
			)
		}
	}
	for _, attempt := range document.IncrementalAttempts {
		if attempt.RunID == boundary.RunID {
			return Run{}, fmt.Errorf(
				"%w: delete-journal readiness follows table mutation evidence",
				ErrStateTransition,
			)
		}
	}
	for _, record := range document.DeleteReconciliations {
		if record.RunID == boundary.RunID {
			return Run{}, fmt.Errorf(
				"%w: delete-journal readiness follows delete mutation evidence",
				ErrStateTransition,
			)
		}
	}
	ordinary := make([]Task, 0, len(document.Tasks))
	for _, task := range document.Tasks {
		if task.RunID == boundary.RunID {
			ordinary = append(ordinary, task)
		}
	}
	workTasks := make([]WorkTask, 0, len(document.WorkTasks))
	for _, task := range document.WorkTasks {
		if task.RunID == boundary.RunID {
			workTasks = append(workTasks, task)
		}
	}
	workRanges := make([]RangeState, 0, len(document.WorkRanges))
	for _, workRange := range document.WorkRanges {
		if workRange.RunID == boundary.RunID {
			workRanges = append(workRanges, workRange)
		}
	}
	if err := validateStage4DeleteJournalReadinessAuthority(
		*inventory,
		boundary,
		ordinary,
		workTasks,
		workRanges,
	); err != nil {
		return Run{}, err
	}
	return latest, nil
}

var _ Stage4DeleteJournalReadinessBackend = YAMLStore{}
