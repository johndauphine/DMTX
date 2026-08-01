package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const stage4RebuildFinalizationRecord = "rebuild_finalization"

func (store SQLiteStore) EnsureStage4RebuildFinalization(
	finalization Stage4RebuildFinalization,
) (Stage4RebuildFinalizationReceipt, bool, error) {
	normalized, err := normalizeStage4RebuildFinalization(finalization)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			fmt.Errorf("begin rebuild finalization receipt: %w", err),
		)
	}
	defer transaction.Rollback()

	stored, found, err := readSQLiteStage4RebuildFinalization(
		transaction,
		normalized.RunID,
		normalized.Phase,
	)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	if found {
		if err := validateStage4RebuildFinalizationReceipt(
			stored,
			normalized,
		); err != nil {
			return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
				"rebuild finalization receipt",
				err,
			)
		}
		return stored.Clone(), false, nil
	}

	latest, inventory, ordinary, workTasks, workRanges, err :=
		stage4RebuildFinalizationAuthoritySQLite(transaction, normalized)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	if latest.Outcome != Running || !latest.Resumable {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			fmt.Errorf(
				"%w: rebuild finalization requires an active resumable run",
				ErrStateTransition,
			),
		)
	}
	if err := validateStage4RebuildFinalizationAuthority(
		inventory,
		normalized,
		ordinary,
		workTasks,
		workRanges,
	); err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	if normalized.Phase == Stage4RebuildFinalizationStarted {
		planned, plannedFound, err := readSQLiteStage4RebuildFinalization(
			transaction,
			normalized.RunID,
			Stage4RebuildFinalizationPlanned,
		)
		if err != nil {
			return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
				"rebuild finalization receipt",
				err,
			)
		}
		if !plannedFound ||
			planned.Finalization.InventoryDigest != normalized.InventoryDigest {
			return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
				"rebuild finalization receipt",
				fmt.Errorf(
					"%w: rebuild finalization start lacks matching planned boundary",
					ErrImmutableEvidence,
				),
			)
		}
	}
	receipt, err := newStage4RebuildFinalizationReceipt(
		normalized,
		time.Now().UTC(),
	)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			fmt.Errorf("encode rebuild finalization receipt: %w", err),
		)
	}
	inserted, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4RebuildFinalizationRecord,
		normalized.RunID,
		stage4MigrationTaskKey,
		string(normalized.Phase),
		string(payload),
	)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			err,
		)
	}
	if !inserted {
		var existing Stage4RebuildFinalizationReceipt
		if err := json.Unmarshal([]byte(existingPayload), &existing); err != nil {
			return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
				"rebuild finalization receipt",
				fmt.Errorf("decode concurrently stored rebuild finalization receipt: %w", err),
			)
		}
		if err := validateStage4RebuildFinalizationReceipt(
			existing,
			normalized,
		); err != nil {
			return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
				"rebuild finalization receipt",
				err,
			)
		}
		return existing.Clone(), false, nil
	}
	if err := transaction.Commit(); err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt",
			fmt.Errorf("commit rebuild finalization receipt: %w", err),
		)
	}
	return receipt.Clone(), true, nil
}

func (store SQLiteStore) LoadStage4RebuildFinalization(
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
	database, err := store.openStage4()
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			err,
		)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			fmt.Errorf("begin rebuild finalization receipt read: %w", err),
		)
	}
	defer transaction.Rollback()
	receipt, found, err := readSQLiteStage4RebuildFinalization(
		transaction,
		runID,
		phase,
	)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, stage4AggregateError(
			"rebuild finalization receipt read",
			err,
		)
	}
	return receipt.Clone(), found, nil
}

func readSQLiteStage4RebuildFinalization(
	transaction *sql.Tx,
	runID string,
	phase Stage4RebuildFinalizationPhase,
) (Stage4RebuildFinalizationReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4RebuildFinalizationRecord,
		runID,
		stage4MigrationTaskKey,
		string(phase),
	)
	if err != nil || !found {
		return Stage4RebuildFinalizationReceipt{}, found, err
	}
	var stored Stage4RebuildFinalizationReceipt
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, fmt.Errorf(
			"decode rebuild finalization receipt: %w",
			err,
		)
	}
	normalized, err := normalizeStoredStage4RebuildFinalization(stored)
	if err != nil {
		return Stage4RebuildFinalizationReceipt{}, false, err
	}
	if normalized.Finalization.RunID != runID ||
		normalized.Finalization.Phase != phase {
		return Stage4RebuildFinalizationReceipt{}, false, fmt.Errorf(
			"%w: rebuild finalization receipt identity differs",
			ErrImmutableEvidence,
		)
	}
	return normalized.Clone(), true, nil
}

func stage4RebuildFinalizationAuthoritySQLite(
	transaction *sql.Tx,
	finalization Stage4RebuildFinalization,
) (Run, Stage4TableInventoryReceipt, []Task, []WorkTask, []RangeState, error) {
	runs, err := readSQLiteAggregateRuns(transaction, finalization.RunID)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	latest, err := validateStage4RunIdentity(runs, finalization.RunID)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	inventory, _, found, err := readSQLiteStage4TableInventory(
		transaction,
		finalization.RunID,
	)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	if !found {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"%w: rebuild finalization table inventory",
			ErrUnknownWork,
		)
	}
	inventory, err = normalizeStoredStage4TableInventory(inventory)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	if finalization.InventoryDigest != inventory.Digest {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"%w: rebuild finalization inventory digest differs",
			ErrImmutableEvidence,
		)
	}
	var completionCount int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind = ?
	`, finalization.RunID, stage4AggregateTableRecord).Scan(&completionCount); err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"inspect rebuild finalization table publications: %w",
			err,
		)
	}
	if completionCount != 0 {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, fmt.Errorf(
			"%w: rebuild finalization follows table publication",
			ErrStateTransition,
		)
	}
	ordinary, err := readSQLiteRebuildReadyOrdinaryTasks(
		transaction,
		finalization.RunID,
	)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	workTasks, workRanges, err := readSQLiteAggregateWork(
		transaction,
		finalization.RunID,
	)
	if err != nil {
		return Run{}, Stage4TableInventoryReceipt{}, nil, nil, nil, err
	}
	return latest, inventory, ordinary, workTasks, workRanges, nil
}

var _ Stage4RebuildRecoveryBackend = SQLiteStore{}
