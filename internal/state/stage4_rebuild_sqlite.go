package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const stage4RebuildReadyRecord = "rebuild_terminal_ready"

func (store SQLiteStore) SaveStage4RebuildReady(
	ready Stage4RebuildReady,
) error {
	normalized, err := normalizeStage4RebuildReady(ready)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return stage4AggregateError(
			"rebuild-ready receipt",
			fmt.Errorf("begin rebuild-ready receipt: %w", err),
		)
	}
	defer transaction.Rollback()

	stored, found, err := readSQLiteStage4RebuildReady(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if found {
		return stage4AggregateError(
			"rebuild-ready receipt",
			validateStage4RebuildReadyReceipt(stored, normalized),
		)
	}

	runs, err := readSQLiteAggregateRuns(transaction, normalized.RunID)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	latest, err := validateStage4RunIdentity(runs, normalized.RunID)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if latest.Outcome != Running || !latest.Resumable {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"%w: rebuild-ready receipt requires an active resumable run",
			ErrStateTransition,
		))
	}
	inventory, _, inventoryFound, err := readSQLiteStage4TableInventory(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if !inventoryFound {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"%w: rebuild-ready table inventory",
			ErrUnknownWork,
		))
	}
	inventory, err = normalizeStoredStage4TableInventory(inventory)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if normalized.InventoryDigest != inventory.Digest {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"%w: rebuild-ready inventory digest differs",
			ErrImmutableEvidence,
		))
	}
	var completionCount int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind = ?
	`, normalized.RunID, stage4AggregateTableRecord).Scan(&completionCount); err != nil {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"inspect rebuild table publications: %w", err,
		))
	}
	if completionCount != 0 {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"%w: rebuild-ready receipt follows table publication",
			ErrStateTransition,
		))
	}
	ordinary, err := readSQLiteRebuildReadyOrdinaryTasks(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	workTasks, workRanges, err := readSQLiteAggregateWork(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if err := validateStage4RebuildReadyAuthority(
		inventory,
		normalized,
		ordinary,
		workTasks,
		workRanges,
	); err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	receipt, err := newStage4RebuildReadyReceipt(normalized)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"encode rebuild-ready receipt: %w", err,
		))
	}
	inserted, _, err := insertSQLiteStage4Record(
		transaction,
		stage4RebuildReadyRecord,
		normalized.RunID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
		string(payload),
	)
	if err != nil {
		return stage4AggregateError("rebuild-ready receipt", err)
	}
	if !inserted {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"%w: rebuild-ready receipt already exists",
			ErrStateTransition,
		))
	}
	if err := transaction.Commit(); err != nil {
		return stage4AggregateError("rebuild-ready receipt", fmt.Errorf(
			"commit rebuild-ready receipt: %w", err,
		))
	}
	return nil
}

func (store SQLiteStore) LoadStage4RebuildReady(
	runID string,
) (Stage4RebuildReadyReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			fmt.Errorf("run ID is required"),
		)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			err,
		)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			fmt.Errorf("begin rebuild-ready receipt read: %w", err),
		)
	}
	defer transaction.Rollback()
	receipt, found, err := readSQLiteStage4RebuildReady(transaction, runID)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, false, stage4AggregateError(
			"rebuild-ready receipt read",
			err,
		)
	}
	return receipt, found, nil
}

func readSQLiteStage4RebuildReady(
	transaction *sql.Tx,
	runID string,
) (Stage4RebuildReadyReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4RebuildReadyRecord,
		runID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
	)
	if err != nil || !found {
		return Stage4RebuildReadyReceipt{}, found, err
	}
	var stored Stage4RebuildReadyReceipt
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		return Stage4RebuildReadyReceipt{}, false, fmt.Errorf(
			"decode rebuild-ready receipt: %w",
			err,
		)
	}
	normalized, err := normalizeStoredStage4RebuildReady(stored)
	if err != nil {
		return Stage4RebuildReadyReceipt{}, false, err
	}
	if normalized.Ready.RunID != runID {
		return Stage4RebuildReadyReceipt{}, false, fmt.Errorf(
			"%w: rebuild-ready run identity differs",
			ErrImmutableEvidence,
		)
	}
	return normalized, true, nil
}

func readSQLiteRebuildReadyOrdinaryTasks(
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
		return nil, fmt.Errorf("read rebuild-ready ordinary tasks: %w", err)
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
			return nil, fmt.Errorf("scan rebuild-ready ordinary task: %w", err)
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
		return nil, fmt.Errorf("iterate rebuild-ready ordinary tasks: %w", err)
	}
	return result, nil
}

var _ Stage4RebuildRecoveryBackend = SQLiteStore{}
