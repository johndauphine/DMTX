package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const stage4DeleteJournalReadinessRecord = "delete_journal_readiness"

func (store SQLiteStore) SaveStage4DeleteJournalReadiness(
	ready Stage4DeleteJournalReadiness,
) error {
	_, _, err := store.EnsureStage4DeleteJournalReadiness(ready)
	return err
}

func (store SQLiteStore) EnsureStage4DeleteJournalReadiness(
	ready Stage4DeleteJournalReadiness,
) (Stage4DeleteJournalReadinessReceipt, bool, error) {
	normalized, err := normalizeStage4DeleteJournalReadiness(ready)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			fmt.Errorf("begin delete-journal readiness receipt: %w", err),
		)
	}
	defer transaction.Rollback()

	stored, found, err := readSQLiteStage4DeleteJournalReadiness(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	if found {
		if err := validateStage4DeleteJournalReadinessReceipt(
			stored,
			normalized,
		); err != nil {
			return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
				"delete-journal readiness receipt",
				err,
			)
		}
		return stored.Clone(), false, nil
	}

	latest, err := validateSQLiteStage4DeleteJournalReadinessBoundary(
		transaction,
		Stage4DeleteJournalReadinessBoundary{
			RunID:           normalized.RunID,
			InventoryDigest: normalized.InventoryDigest,
		},
	)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	if latest.TargetIdentity != normalized.TargetIdentity ||
		normalized.ReadyAt.Before(latest.StartedAt) {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			fmt.Errorf(
				"%w: delete-journal readiness target authority differs from run identity",
				ErrImmutableEvidence,
			),
		)
	}
	receipt, err := newStage4DeleteJournalReadinessReceipt(normalized)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			fmt.Errorf("encode delete-journal readiness receipt: %w", err),
		)
	}
	inserted, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4DeleteJournalReadinessRecord,
		normalized.RunID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
		string(payload),
	)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			err,
		)
	}
	if !inserted {
		var existing Stage4DeleteJournalReadinessReceipt
		if err := json.Unmarshal([]byte(existingPayload), &existing); err != nil {
			return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
				"delete-journal readiness receipt",
				fmt.Errorf("decode concurrently stored delete-journal readiness receipt: %w", err),
			)
		}
		if err := validateStage4DeleteJournalReadinessReceipt(
			existing,
			normalized,
		); err != nil {
			return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
				"delete-journal readiness receipt",
				err,
			)
		}
		return existing.Clone(), false, nil
	}
	if err := transaction.Commit(); err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt",
			fmt.Errorf("commit delete-journal readiness receipt: %w", err),
		)
	}
	return receipt.Clone(), true, nil
}

func (store SQLiteStore) LoadStage4DeleteJournalReadiness(
	runID string,
) (Stage4DeleteJournalReadinessReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" || runID != strings.TrimSpace(runID) {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			fmt.Errorf("run ID is required"),
		)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			err,
		)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			fmt.Errorf("begin delete-journal readiness receipt read: %w", err),
		)
	}
	defer transaction.Rollback()
	receipt, found, err := readSQLiteStage4DeleteJournalReadiness(
		transaction,
		runID,
	)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, stage4AggregateError(
			"delete-journal readiness receipt read",
			err,
		)
	}
	return receipt.Clone(), found, nil
}

func (store SQLiteStore) ValidateStage4DeleteJournalReadinessBoundary(
	boundary Stage4DeleteJournalReadinessBoundary,
) error {
	normalized, err := normalizeStage4DeleteJournalReadinessBoundary(boundary)
	if err != nil {
		return stage4AggregateError("delete-journal readiness boundary", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return stage4AggregateError("delete-journal readiness boundary", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return stage4AggregateError("delete-journal readiness boundary", fmt.Errorf(
			"begin delete-journal readiness boundary read: %w",
			err,
		))
	}
	defer transaction.Rollback()
	if _, err := validateSQLiteStage4DeleteJournalReadinessBoundary(
		transaction,
		normalized,
	); err != nil {
		return stage4AggregateError("delete-journal readiness boundary", err)
	}
	return nil
}

func validateSQLiteStage4DeleteJournalReadinessBoundary(
	transaction *sql.Tx,
	boundary Stage4DeleteJournalReadinessBoundary,
) (Run, error) {
	runs, err := readSQLiteAggregateRuns(transaction, boundary.RunID)
	if err != nil {
		return Run{}, err
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
	inventory, _, inventoryFound, err := readSQLiteStage4TableInventory(
		transaction,
		boundary.RunID,
	)
	if err != nil {
		return Run{}, err
	}
	if !inventoryFound {
		return Run{}, fmt.Errorf(
			"%w: delete-journal readiness table inventory",
			ErrUnknownWork,
		)
	}
	inventory, err = normalizeStoredStage4TableInventory(inventory)
	if err != nil {
		return Run{}, err
	}
	var completionCount int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind = ?
	`, boundary.RunID, stage4AggregateTableRecord).Scan(&completionCount); err != nil {
		return Run{}, fmt.Errorf("inspect delete-journal table publications: %w", err)
	}
	if completionCount != 0 {
		return Run{}, fmt.Errorf(
			"%w: delete-journal readiness follows table publication",
			ErrStateTransition,
		)
	}
	var mutationCount int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind IN (?, ?)
	`, boundary.RunID, stage4IncrementalRecord, stage4DeleteRecord).Scan(&mutationCount); err != nil {
		return Run{}, fmt.Errorf("inspect delete-journal mutation evidence: %w", err)
	}
	if mutationCount != 0 {
		return Run{}, fmt.Errorf(
			"%w: delete-journal readiness follows table mutation evidence",
			ErrStateTransition,
		)
	}
	ordinary, err := readSQLiteDeleteJournalReadinessOrdinaryTasks(
		transaction,
		boundary.RunID,
	)
	if err != nil {
		return Run{}, err
	}
	workTasks, workRanges, err := readSQLiteAggregateWork(
		transaction,
		boundary.RunID,
	)
	if err != nil {
		return Run{}, err
	}
	if err := validateStage4DeleteJournalReadinessAuthority(
		inventory,
		boundary,
		ordinary,
		workTasks,
		workRanges,
	); err != nil {
		return Run{}, err
	}
	return latest, nil
}

func readSQLiteStage4DeleteJournalReadiness(
	transaction *sql.Tx,
	runID string,
) (Stage4DeleteJournalReadinessReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4DeleteJournalReadinessRecord,
		runID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
	)
	if err != nil || !found {
		return Stage4DeleteJournalReadinessReceipt{}, found, err
	}
	var stored Stage4DeleteJournalReadinessReceipt
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, fmt.Errorf(
			"decode delete-journal readiness receipt: %w",
			err,
		)
	}
	normalized, err := normalizeStoredStage4DeleteJournalReadiness(stored)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, false, err
	}
	if normalized.Readiness.RunID != runID {
		return Stage4DeleteJournalReadinessReceipt{}, false, fmt.Errorf(
			"%w: delete-journal readiness run identity differs",
			ErrImmutableEvidence,
		)
	}
	return normalized.Clone(), true, nil
}

func readSQLiteDeleteJournalReadinessOrdinaryTasks(
	transaction *sql.Tx,
	runID string,
) ([]Task, error) {
	rows, err := transaction.Query(`
		SELECT run_id, table_name, status, rows_done,
		       integer_watermark, row_number_watermark,
		       started_at, completed_at
		FROM tasks WHERE run_id = ? ORDER BY table_name
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("read delete-journal readiness ordinary tasks: %w", err)
	}
	defer rows.Close()
	result := make([]Task, 0)
	for rows.Next() {
		var (
			task               Task
			integer, rowNumber sql.NullInt64
			completed          sql.NullTime
		)
		if err := rows.Scan(
			&task.RunID,
			&task.Table,
			&task.Status,
			&task.RowsDone,
			&integer,
			&rowNumber,
			&task.StartedAt,
			&completed,
		); err != nil {
			return nil, fmt.Errorf("scan delete-journal readiness ordinary task: %w", err)
		}
		if integer.Valid {
			value := integer.Int64
			task.IntegerWatermark = &value
		}
		if rowNumber.Valid {
			value := rowNumber.Int64
			task.RowNumberWatermark = &value
		}
		if completed.Valid {
			task.CompletedAt = completed.Time.UTC()
		}
		task.StartedAt = task.StartedAt.UTC()
		result = append(result, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delete-journal readiness ordinary tasks: %w", err)
	}
	return result, nil
}

var _ Stage4DeleteJournalReadinessBackend = SQLiteStore{}
