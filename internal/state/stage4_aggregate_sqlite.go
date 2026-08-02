package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	stage4AggregateTableRecord     = "aggregate_table_completion"
	stage4AggregateInventoryRecord = "aggregate_table_inventory"
)

func (store SQLiteStore) EnsureStage4TableInventory(
	inventory Stage4TableInventory,
) error {
	normalized, err := normalizeStage4TableInventory(inventory)
	if err != nil {
		return stage4AggregateError("table inventory", err)
	}
	if err := store.ensureStage4TableInventory(normalized); err != nil {
		return stage4AggregateError("table inventory", err)
	}
	return nil
}

func (store SQLiteStore) ensureStage4TableInventory(
	inventory Stage4TableInventory,
) error {
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin Stage 4 table inventory: %w", err)
	}
	defer transaction.Rollback()

	runs, err := readSQLiteAggregateRuns(transaction, inventory.RunID)
	if err != nil {
		return err
	}
	latest, err := validateStage4RunIdentity(runs, inventory.RunID)
	if err != nil {
		return err
	}
	stored, storedPayload, found, err := readSQLiteStage4TableInventory(
		transaction,
		inventory.RunID,
	)
	if err != nil {
		return err
	}
	if latest.Outcome != Running || !latest.Resumable {
		return fmt.Errorf(
			"%w: table inventory requires an active resumable run",
			ErrStateTransition,
		)
	}
	if found {
		if validateStage4TableInventoryReceipt(
			stored,
			inventory,
		) == nil {
			return nil
		}
		return reviseSQLiteStage4TableInventory(
			transaction,
			stored,
			storedPayload,
			inventory,
		)
	}
	if err := requireSQLiteStage4InventoryPrerequisites(
		transaction,
		inventory,
	); err != nil {
		return err
	}
	receipt, err := newStage4TableInventoryReceipt(inventory)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode Stage 4 table inventory: %w", err)
	}
	inserted, _, err := insertSQLiteStage4Record(
		transaction,
		stage4AggregateInventoryRecord,
		inventory.RunID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf(
			"%w: Stage 4 table inventory already exists",
			ErrStateTransition,
		)
	}
	if err := stage4BeforeInventoryCommit(); err != nil {
		return fmt.Errorf("prepare Stage 4 table inventory commit: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit Stage 4 table inventory: %w", err)
	}
	return nil
}

func readSQLiteStage4TableInventory(
	transaction *sql.Tx,
	runID string,
) (Stage4TableInventoryReceipt, string, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4AggregateInventoryRecord,
		runID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
	)
	if err != nil || !found {
		return Stage4TableInventoryReceipt{}, "", found, err
	}
	var receipt Stage4TableInventoryReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return Stage4TableInventoryReceipt{}, "", false, fmt.Errorf(
			"decode Stage 4 table inventory: %w",
			err,
		)
	}
	return receipt, payload, true, nil
}

// reviseSQLiteStage4TableInventory replaces a pre-mutation inventory in place.
// The window is guarded by validateStage4InventoryRevision; the snapshot
// authority is revalidated because the revision still binds itself to it.
func reviseSQLiteStage4TableInventory(
	transaction *sql.Tx,
	stored Stage4TableInventoryReceipt,
	storedPayload string,
	inventory Stage4TableInventory,
) error {
	var receipts, completedTables int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind = ?
	`, inventory.RunID, stage4AggregateTableRecord).Scan(&receipts); err != nil {
		return fmt.Errorf("inspect aggregate table receipts: %w", err)
	}
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM tasks
		WHERE run_id = ? AND status = 'completed'
	`, inventory.RunID).Scan(&completedTables); err != nil {
		return fmt.Errorf("inspect completed ordinary tables: %w", err)
	}
	if err := validateStage4InventoryRevision(
		stored,
		inventory,
		receipts,
		completedTables,
	); err != nil {
		return err
	}
	snapshots, err := readSQLiteAggregateSnapshots(
		transaction,
		inventory.RunID,
	)
	if err != nil {
		return err
	}
	if err := validateStage4InventorySnapshot(inventory, snapshots); err != nil {
		return err
	}
	receipt, err := newStage4TableInventoryReceipt(inventory)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode Stage 4 table inventory: %w", err)
	}
	if err := updateSQLiteStage4Record(
		transaction,
		stage4AggregateInventoryRecord,
		inventory.RunID,
		stage4MigrationTaskKey,
		stage4SingletonRecordID,
		storedPayload,
		string(payload),
	); err != nil {
		return err
	}
	if err := stage4BeforeInventoryCommit(); err != nil {
		return fmt.Errorf("prepare Stage 4 table inventory revision: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit Stage 4 table inventory revision: %w", err)
	}
	return nil
}

func requireSQLiteStage4InventoryPrerequisites(
	transaction *sql.Tx,
	inventory Stage4TableInventory,
) error {
	var ordinaryCount int
	if err := transaction.QueryRow(
		`SELECT COUNT(*) FROM tasks WHERE run_id = ?`,
		inventory.RunID,
	).Scan(&ordinaryCount); err != nil {
		return fmt.Errorf("inspect ordinary table evidence: %w", err)
	}
	if ordinaryCount != 0 {
		return fmt.Errorf(
			"%w: table inventory must precede ordinary table evidence",
			ErrStateTransition,
		)
	}
	workTasks, workRanges, err := readSQLiteAggregateWork(
		transaction,
		inventory.RunID,
	)
	if err != nil {
		return err
	}
	if err := validateStage4InventoryAuthority(
		inventory,
		workTasks,
		workRanges,
	); err != nil {
		return err
	}
	snapshots, err := readSQLiteAggregateSnapshots(
		transaction,
		inventory.RunID,
	)
	if err != nil {
		return err
	}
	if err := validateStage4InventorySnapshot(inventory, snapshots); err != nil {
		return err
	}
	var mutationCount int
	if err := transaction.QueryRow(`
		SELECT COUNT(*) FROM stage4_records
		WHERE run_id = ? AND kind IN (?, ?, ?)
	`, inventory.RunID, stage4AggregateTableRecord,
		stage4IncrementalRecord, stage4DeleteRecord,
	).Scan(&mutationCount); err != nil {
		return fmt.Errorf("inspect table mutation evidence: %w", err)
	}
	if mutationCount != 0 {
		return fmt.Errorf(
			"%w: table inventory must precede table mutation evidence",
			ErrStateTransition,
		)
	}
	return nil
}

func (store SQLiteStore) LoadStage4TableInventory(
	runID string,
) (Stage4TableInventoryReceipt, bool, error) {
	if strings.TrimSpace(runID) == "" {
		return Stage4TableInventoryReceipt{}, false, stage4AggregateError(
			"table inventory read",
			fmt.Errorf("run ID is required"),
		)
	}
	receipt, found, err := store.loadStage4TableInventory(runID)
	if err != nil {
		return Stage4TableInventoryReceipt{}, false, stage4AggregateError(
			"table inventory read",
			err,
		)
	}
	return receipt, found, nil
}

func (store SQLiteStore) loadStage4TableInventory(
	runID string,
) (Stage4TableInventoryReceipt, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return Stage4TableInventoryReceipt{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4TableInventoryReceipt{}, false, fmt.Errorf(
			"begin Stage 4 table inventory read: %w",
			err,
		)
	}
	defer transaction.Rollback()

	stored, _, found, err := readSQLiteStage4TableInventory(transaction, runID)
	if err != nil || !found {
		return Stage4TableInventoryReceipt{}, found, err
	}
	receipt, err := normalizeStoredStage4TableInventory(stored)
	if err != nil {
		return Stage4TableInventoryReceipt{}, false, err
	}
	if receipt.Inventory.RunID != runID {
		return Stage4TableInventoryReceipt{}, false, fmt.Errorf(
			"%w: Stage 4 table inventory run identity differs",
			ErrImmutableEvidence,
		)
	}
	return receipt, true, nil
}

func (store SQLiteStore) LoadStage4TableCompletions(
	runID string,
) ([]Stage4TableCompletionReceipt, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, stage4AggregateError(
			"table completion read",
			fmt.Errorf("run ID is required"),
		)
	}
	receipts, err := store.loadStage4TableCompletions(runID)
	if err != nil {
		return nil, stage4AggregateError("table completion read", err)
	}
	return receipts, nil
}

func (store SQLiteStore) loadStage4TableCompletions(
	runID string,
) ([]Stage4TableCompletionReceipt, error) {
	database, err := store.openStage4()
	if err != nil {
		return nil, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return nil, fmt.Errorf(
			"begin aggregate table completion read: %w",
			err,
		)
	}
	defer transaction.Rollback()

	stored, err := readSQLiteAggregateTableReceipts(transaction, runID)
	if err != nil {
		return nil, err
	}
	receipts := make([]Stage4TableCompletionReceipt, 0, len(stored))
	for _, receipt := range stored {
		receipts = append(receipts, receipt)
	}
	return normalizeStoredStage4TableCompletions(runID, receipts)
}

func (store SQLiteStore) CompleteStage4Table(
	completion Stage4TableCompletion,
) error {
	normalized, err := normalizeStage4TableCompletion(completion)
	if err != nil {
		return stage4AggregateError("table completion", err)
	}
	if err := store.completeStage4Table(normalized); err != nil {
		return stage4AggregateError("table completion", err)
	}
	return nil
}

func (store SQLiteStore) completeStage4Table(
	completion Stage4TableCompletion,
) error {
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin aggregate table completion: %w", err)
	}
	defer transaction.Rollback()

	runs, err := readSQLiteAggregateRuns(transaction, completion.RunID)
	if err != nil {
		return err
	}
	ordinary, err := readSQLiteAggregateTask(
		transaction,
		completion.RunID,
		completion.Table,
	)
	if err != nil {
		return err
	}
	if err := validateStage4TableRun(
		runs,
		completion,
		ordinary.Status == "completed",
	); err != nil {
		return err
	}
	inventory, _, inventoryFound, err := readSQLiteStage4TableInventory(
		transaction,
		completion.RunID,
	)
	if err != nil {
		return err
	}
	if !inventoryFound {
		return fmt.Errorf(
			"%w: durable Stage 4 table inventory",
			ErrUnknownWork,
		)
	}
	workTask, ranges, found, err := readSQLiteWorkPlan(
		transaction,
		completion.RunID,
		completion.Task,
	)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: structured table task", ErrUnknownWork)
	}
	if err := validateStage4InventoryAuthorizesTable(
		inventory,
		completion,
		workTask,
		ranges,
	); err != nil {
		return err
	}
	taskKey, _ := completion.Task.canonical()
	receipt, receiptFound, err := readSQLiteAggregateTableReceipt(
		transaction,
		completion.RunID,
		taskKey,
	)
	if err != nil {
		return err
	}
	attempt, recordID, previousAttempt, err :=
		readSQLiteAggregateIncremental(
			transaction,
			completion.RunID,
			taskKey,
		)
	if err != nil {
		return err
	}
	replay := ordinary.Status == "completed"
	nextOrdinary, nextWork, nextRanges, nextAttempt, err :=
		applyStage4TableCompletion(
			completion,
			ordinary,
			workTask,
			ranges,
			attempt,
		)
	if err != nil {
		return err
	}
	if replay {
		if !receiptFound {
			return fmt.Errorf(
				"%w: completed table lacks an aggregate receipt",
				ErrStateTransition,
			)
		}
		return validateStage4TableCompletionReceipt(receipt, completion)
	}
	if receiptFound {
		return fmt.Errorf(
			"%w: running table already has an aggregate receipt",
			ErrStateTransition,
		)
	}

	if err := updateSQLiteAggregateWork(
		transaction,
		completion.RunID,
		taskKey,
		nextWork,
		nextRanges,
	); err != nil {
		return err
	}
	result, err := transaction.Exec(`
		UPDATE tasks
		SET status = ?, rows_done = ?, completed_at = ?
		WHERE run_id = ? AND table_name = ? AND status = 'running'
	`, nextOrdinary.Status, nextOrdinary.RowsDone,
		nextOrdinary.CompletedAt, completion.RunID, completion.Table)
	if err != nil {
		return fmt.Errorf("update ordinary table completion: %w", err)
	}
	if err := requireOneSQLiteMutation(
		result,
		"ordinary table completion",
	); err != nil {
		return err
	}
	if nextAttempt != nil {
		nextPayload, err := json.Marshal(nextAttempt)
		if err != nil {
			return fmt.Errorf("encode aggregate incremental completion: %w", err)
		}
		if string(nextPayload) != previousAttempt {
			if err := updateSQLiteStage4Record(
				transaction,
				stage4IncrementalRecord,
				completion.RunID,
				taskKey,
				recordID,
				previousAttempt,
				string(nextPayload),
			); err != nil {
				return err
			}
		}
	}
	nextReceipt, err := newStage4TableCompletionReceipt(completion)
	if err != nil {
		return err
	}
	receiptPayload, err := json.Marshal(nextReceipt)
	if err != nil {
		return fmt.Errorf("encode aggregate table receipt: %w", err)
	}
	inserted, _, err := insertSQLiteStage4Record(
		transaction,
		stage4AggregateTableRecord,
		completion.RunID,
		taskKey,
		stage4SingletonRecordID,
		string(receiptPayload),
	)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf(
			"%w: aggregate table receipt already exists",
			ErrStateTransition,
		)
	}
	if err := stage4BeforeAggregateTableCommit(); err != nil {
		return fmt.Errorf("prepare aggregate table commit: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit aggregate table completion: %w", err)
	}
	return nil
}

func readSQLiteAggregateTableReceipt(
	transaction *sql.Tx,
	runID string,
	taskKey string,
) (Stage4TableCompletionReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4AggregateTableRecord,
		runID,
		taskKey,
		stage4SingletonRecordID,
	)
	if err != nil || !found {
		return Stage4TableCompletionReceipt{}, found, err
	}
	var receipt Stage4TableCompletionReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return Stage4TableCompletionReceipt{}, false, fmt.Errorf(
			"decode aggregate table receipt: %w",
			err,
		)
	}
	return receipt, true, nil
}

func readSQLiteAggregateTableReceipts(
	transaction *sql.Tx,
	runID string,
) (map[TaskKey]Stage4TableCompletionReceipt, error) {
	rows, err := transaction.Query(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ?
		ORDER BY task_key, record_id
	`, stage4AggregateTableRecord, runID)
	if err != nil {
		return nil, fmt.Errorf("read aggregate table receipts: %w", err)
	}
	defer rows.Close()
	receipts := make(map[TaskKey]Stage4TableCompletionReceipt)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var receipt Stage4TableCompletionReceipt
		if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
			return nil, fmt.Errorf("decode aggregate table receipt: %w", err)
		}
		if receipt.Completion.RunID != runID {
			return nil, fmt.Errorf(
				"%w: aggregate table receipt run identity differs",
				ErrImmutableEvidence,
			)
		}
		task := receipt.Completion.Task
		if _, duplicate := receipts[task]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate aggregate table receipt for task %#v",
				ErrImmutableEvidence,
				task,
			)
		}
		receipts[task] = receipt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return receipts, nil
}

func (store SQLiteStore) CompleteStage4Run(
	completion Stage4RunCompletion,
) error {
	normalized, err := normalizeStage4RunCompletion(completion)
	if err != nil {
		return stage4AggregateError("run completion", err)
	}
	if err := store.completeStage4Run(normalized); err != nil {
		return stage4AggregateError("run completion", err)
	}
	return nil
}

func (store SQLiteStore) completeStage4Run(
	completion Stage4RunCompletion,
) error {
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin aggregate run completion: %w", err)
	}
	defer transaction.Rollback()

	runs, err := readSQLiteAggregateRuns(transaction, completion.RunID)
	if err != nil {
		return err
	}
	latest, err := validateStage4RunIdentity(runs, completion.RunID)
	if err != nil {
		return err
	}
	replay := latest.Outcome == Success
	switch {
	case replay && !stage4RunReplayMatches(latest, completion):
		return fmt.Errorf("%w: successful run publication differs", ErrImmutableEvidence)
	case !replay && (latest.Outcome != Running || !latest.Resumable):
		return fmt.Errorf(
			"%w: run is not an active resumable migration",
			ErrStateTransition,
		)
	case completion.CompletedAt.Before(latest.StartedAt):
		return fmt.Errorf("%w: run completion precedes start", ErrStateTransition)
	}
	inventory, _, inventoryFound, err := readSQLiteStage4TableInventory(
		transaction,
		completion.RunID,
	)
	if err != nil {
		return err
	}
	if !inventoryFound {
		return fmt.Errorf(
			"%w: durable Stage 4 table inventory",
			ErrUnknownWork,
		)
	}
	if err := validateStage4RunInventory(inventory, completion); err != nil {
		return err
	}

	ordinary, err := readSQLiteAggregateTasks(transaction, completion.RunID)
	if err != nil {
		return err
	}
	if err := validateStage4RunRowsComplete(
		completion.RunID,
		completion.CompletedAt,
		ordinary,
	); err != nil {
		return err
	}
	workTasks, workRanges, err :=
		readSQLiteAggregateWork(transaction, completion.RunID)
	if err != nil {
		return err
	}
	if err := validateStage4KnownWorkInventory(workTasks); err != nil {
		return err
	}
	taskIndexes, rangesByTask, err := indexStage4AggregateWork(
		completion.RunID,
		workTasks,
		workRanges,
	)
	if err != nil {
		return err
	}
	if err := validateStage4DurableInventoryWork(
		inventory,
		workTasks,
		rangesByTask,
	); err != nil {
		return err
	}
	if err := validateStage4SentinelInventory(
		completion,
		workTasks,
		rangesByTask,
	); err != nil {
		return err
	}
	snapshots, err := readSQLiteAggregateSnapshots(
		transaction,
		completion.RunID,
	)
	if err != nil {
		return err
	}
	if len(snapshots) != len(completion.Sentinels) {
		return fmt.Errorf(
			"%w: exact schema sentinel inventory differs",
			ErrImmutableEvidence,
		)
	}
	for _, sentinel := range completion.Sentinels {
		taskIndex, ok := taskIndexes[sentinel.Task]
		if !ok {
			return fmt.Errorf(
				"%w: schema sentinel task %#v",
				ErrUnknownWork,
				sentinel.Task,
			)
		}
		storedSnapshot, found := snapshots[sentinel.Task]
		if !found {
			return fmt.Errorf(
				"%w: schema snapshot for sentinel %#v",
				ErrUnknownWork,
				sentinel.Task,
			)
		}
		if !stage4SnapshotMatches(storedSnapshot, sentinel.Snapshot) {
			return fmt.Errorf(
				"%w: schema snapshot for sentinel %#v differs",
				ErrImmutableEvidence,
				sentinel.Task,
			)
		}
		nextTask, nextRanges, wasComplete, err :=
			applyStage4StructuredCompletion(
				completion.RunID,
				sentinel.Task,
				sentinel.TopologyHash,
				[]Stage4RangeCompletion{{
					ID: sentinel.RangeID, NextSequence: sentinel.NextSequence,
				}},
				completion.CompletedAt,
				workTasks[taskIndex],
				rangesByTask[sentinel.Task],
				true,
			)
		if err != nil {
			return err
		}
		if replay != wasComplete {
			return fmt.Errorf(
				"%w: schema sentinel and run publication are partial",
				ErrStateTransition,
			)
		}
		workTasks[taskIndex] = nextTask
		rangesByTask[sentinel.Task] = nextRanges
	}
	if err := validateStage4AggregateWorkComplete(
		workTasks,
		rangesByTask,
		completion.CompletedAt,
	); err != nil {
		return err
	}
	incremental, deletes, err := readSQLiteAggregateTerminalRecords(
		transaction,
		completion.RunID,
	)
	if err != nil {
		return err
	}
	receipts, err := readSQLiteAggregateTableReceipts(
		transaction,
		completion.RunID,
	)
	if err != nil {
		return err
	}
	if err := validateStage4ExpectedTableInventory(
		completion,
		ordinary,
		workTasks,
		rangesByTask,
		incremental,
		receipts,
	); err != nil {
		return err
	}
	if err := validateStage4TerminalRecords(
		incremental,
		deletes,
		completion,
	); err != nil {
		return err
	}
	if replay {
		return nil
	}

	for _, sentinel := range completion.Sentinels {
		taskKey, _ := sentinel.Task.canonical()
		taskIndex := taskIndexes[sentinel.Task]
		if err := updateSQLiteAggregateWork(
			transaction,
			completion.RunID,
			taskKey,
			workTasks[taskIndex],
			rangesByTask[sentinel.Task],
		); err != nil {
			return err
		}
	}
	success := latest
	success.Outcome = Success
	success.Resumable = false
	success.Reason = completion.Reason
	success.EndedAt = completion.CompletedAt
	if _, err := transaction.Exec(`
		INSERT INTO runs (
			id, source, target, source_engine, source_identity, target_identity,
			lease_target, lease_owner_token, lease_generation,
			outcome, resumable, reason, started_at, ended_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, success.ID, success.Source, success.Target, success.SourceEngine,
		success.SourceIdentity, success.TargetIdentity,
		success.LeaseTarget, success.LeaseOwnerToken, success.LeaseGeneration,
		success.Outcome, success.Resumable, success.Reason,
		success.StartedAt, success.EndedAt); err != nil {
		return fmt.Errorf("publish successful run outcome: %w", err)
	}
	if err := stage4BeforeAggregateRunCommit(); err != nil {
		return fmt.Errorf("prepare aggregate run commit: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit aggregate run completion: %w", err)
	}
	return nil
}

func readSQLiteAggregateTask(
	transaction *sql.Tx,
	runID string,
	table string,
) (Task, error) {
	var task Task
	var integerWatermark, rowNumberWatermark sql.NullInt64
	var completedAt sql.NullTime
	err := transaction.QueryRow(`
		SELECT run_id, table_name, status, rows_done,
		       integer_watermark, row_number_watermark,
		       started_at, completed_at
		FROM tasks WHERE run_id = ? AND table_name = ?
	`, runID, table).Scan(
		&task.RunID, &task.Table, &task.Status, &task.RowsDone,
		&integerWatermark, &rowNumberWatermark,
		&task.StartedAt, &completedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: ordinary task %q", ErrUnknownWork, table)
	}
	if err != nil {
		return Task{}, fmt.Errorf("read ordinary table task: %w", err)
	}
	if integerWatermark.Valid {
		value := integerWatermark.Int64
		task.IntegerWatermark = &value
	}
	if rowNumberWatermark.Valid {
		value := rowNumberWatermark.Int64
		task.RowNumberWatermark = &value
	}
	if completedAt.Valid {
		task.CompletedAt = completedAt.Time
	}
	return task, nil
}

func readSQLiteAggregateIncremental(
	transaction *sql.Tx,
	runID string,
	taskKey string,
) (*IncrementalAttempt, string, string, error) {
	rows, err := transaction.Query(`
		SELECT record_id, payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
		ORDER BY record_id
	`, stage4IncrementalRecord, runID, taskKey)
	if err != nil {
		return nil, "", "", fmt.Errorf("read aggregate incremental attempt: %w", err)
	}
	defer rows.Close()
	var attempt *IncrementalAttempt
	var recordID, payload string
	for rows.Next() {
		if attempt != nil {
			return nil, "", "", fmt.Errorf(
				"%w: multiple incremental attempts for table",
				ErrStateTransition,
			)
		}
		if err := rows.Scan(&recordID, &payload); err != nil {
			return nil, "", "", err
		}
		decoded, err := incrementalAttemptFromPayload(payload)
		if err != nil {
			return nil, "", "", err
		}
		attempt = &decoded
	}
	if err := rows.Err(); err != nil {
		return nil, "", "", err
	}
	return attempt, recordID, payload, nil
}

func updateSQLiteAggregateWork(
	transaction *sql.Tx,
	runID string,
	taskKey string,
	task WorkTask,
	ranges []RangeState,
) error {
	taskPayload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("encode aggregate work task: %w", err)
	}
	result, err := transaction.Exec(
		`UPDATE work_tasks SET payload = ? WHERE run_id = ? AND task_key = ?`,
		string(taskPayload), runID, taskKey,
	)
	if err != nil {
		return fmt.Errorf("update aggregate work task: %w", err)
	}
	if err := requireOneSQLiteMutation(result, "aggregate work task"); err != nil {
		return err
	}
	for _, workRange := range ranges {
		payload, err := json.Marshal(workRange)
		if err != nil {
			return fmt.Errorf("encode aggregate work range %q: %w", workRange.ID, err)
		}
		result, err := transaction.Exec(`
			UPDATE work_ranges SET payload = ?
			WHERE run_id = ? AND task_key = ? AND range_id = ?
		`, string(payload), runID, taskKey, workRange.ID)
		if err != nil {
			return fmt.Errorf("update aggregate work range %q: %w", workRange.ID, err)
		}
		if err := requireOneSQLiteMutation(
			result,
			"aggregate work range",
		); err != nil {
			return err
		}
	}
	return nil
}

func requireOneSQLiteMutation(result sql.Result, operation string) error {
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify %s: %w", operation, err)
	}
	if updated != 1 {
		return fmt.Errorf("%w: %s changed concurrently", ErrStateTransition, operation)
	}
	return nil
}

func readSQLiteAggregateRuns(
	transaction *sql.Tx,
	runID string,
) ([]Run, error) {
	rows, err := transaction.Query(`
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, reason, started_at, ended_at
		FROM runs WHERE id = ? ORDER BY started_at, rowid
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("read aggregate run identity: %w", err)
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("decode aggregate run identity: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func readSQLiteAggregateTasks(
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
		return nil, fmt.Errorf("list aggregate ordinary tasks: %w", err)
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var task Task
		var integerWatermark, rowNumberWatermark sql.NullInt64
		var completedAt sql.NullTime
		if err := rows.Scan(
			&task.RunID, &task.Table, &task.Status, &task.RowsDone,
			&integerWatermark, &rowNumberWatermark,
			&task.StartedAt, &completedAt,
		); err != nil {
			return nil, err
		}
		if integerWatermark.Valid {
			value := integerWatermark.Int64
			task.IntegerWatermark = &value
		}
		if rowNumberWatermark.Valid {
			value := rowNumberWatermark.Int64
			task.RowNumberWatermark = &value
		}
		if completedAt.Valid {
			task.CompletedAt = completedAt.Time
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tasks, nil
}

func readSQLiteAggregateWork(
	transaction *sql.Tx,
	runID string,
) ([]WorkTask, []RangeState, error) {
	taskRows, err := transaction.Query(`
		SELECT payload FROM work_tasks WHERE run_id = ? ORDER BY task_key
	`, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("list aggregate work tasks: %w", err)
	}
	var tasks []WorkTask
	for taskRows.Next() {
		var payload string
		if err := taskRows.Scan(&payload); err != nil {
			taskRows.Close()
			return nil, nil, err
		}
		var task WorkTask
		if err := json.Unmarshal([]byte(payload), &task); err != nil {
			taskRows.Close()
			return nil, nil, fmt.Errorf("decode aggregate work task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := finishSQLiteRows(
		taskRows,
		"iterate aggregate work tasks",
		"close aggregate work task query",
	); err != nil {
		return nil, nil, err
	}
	rangeRows, err := transaction.Query(`
		SELECT payload FROM work_ranges
		WHERE run_id = ? ORDER BY task_key, range_id
	`, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("list aggregate work ranges: %w", err)
	}
	var ranges []RangeState
	for rangeRows.Next() {
		var payload string
		if err := rangeRows.Scan(&payload); err != nil {
			rangeRows.Close()
			return nil, nil, err
		}
		var workRange RangeState
		if err := json.Unmarshal([]byte(payload), &workRange); err != nil {
			rangeRows.Close()
			return nil, nil, fmt.Errorf("decode aggregate work range: %w", err)
		}
		ranges = append(ranges, workRange)
	}
	if err := finishSQLiteRows(
		rangeRows,
		"iterate aggregate work ranges",
		"close aggregate work range query",
	); err != nil {
		return nil, nil, err
	}
	return tasks, ranges, nil
}

func readSQLiteAggregateSnapshots(
	transaction *sql.Tx,
	runID string,
) (map[TaskKey]SchemaSnapshot, error) {
	rows, err := transaction.Query(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ?
		ORDER BY task_key, record_id
	`, stage4SchemaRecord, runID)
	if err != nil {
		return nil, fmt.Errorf("read aggregate schema snapshots: %w", err)
	}
	defer rows.Close()
	snapshots := make(map[TaskKey]SchemaSnapshot)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var snapshot SchemaSnapshot
		if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
			return nil, fmt.Errorf("decode aggregate schema snapshot: %w", err)
		}
		if snapshot.RunID != runID {
			return nil, fmt.Errorf(
				"%w: schema snapshot run identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := snapshots[snapshot.Task]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate schema snapshot for task %#v",
				ErrImmutableEvidence,
				snapshot.Task,
			)
		}
		snapshots[snapshot.Task] = snapshot
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func readSQLiteAggregateTerminalRecords(
	transaction *sql.Tx,
	runID string,
) ([]IncrementalAttempt, []DeleteReconciliation, error) {
	rows, err := transaction.Query(`
		SELECT kind, payload FROM stage4_records
		WHERE run_id = ? AND kind IN (?, ?)
		ORDER BY kind, task_key, record_id
	`, runID, stage4IncrementalRecord, stage4DeleteRecord)
	if err != nil {
		return nil, nil, fmt.Errorf("read aggregate terminal records: %w", err)
	}
	defer rows.Close()
	var incremental []IncrementalAttempt
	var deletes []DeleteReconciliation
	for rows.Next() {
		var kind, payload string
		if err := rows.Scan(&kind, &payload); err != nil {
			return nil, nil, err
		}
		switch kind {
		case stage4IncrementalRecord:
			record, err := incrementalAttemptFromPayload(payload)
			if err != nil {
				return nil, nil, err
			}
			incremental = append(incremental, record)
		case stage4DeleteRecord:
			record, err := deleteReconciliationFromPayload(payload)
			if err != nil {
				return nil, nil, err
			}
			deletes = append(deletes, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return incremental, deletes, nil
}

func indexStage4AggregateWork(
	runID string,
	tasks []WorkTask,
	ranges []RangeState,
) (map[TaskKey]int, map[TaskKey][]RangeState, error) {
	taskIndexes := make(map[TaskKey]int, len(tasks))
	for index, task := range tasks {
		if task.RunID != runID {
			return nil, nil, fmt.Errorf(
				"%w: structured task run identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := taskIndexes[task.Key]; duplicate {
			return nil, nil, fmt.Errorf(
				"%w: duplicate structured task %#v",
				ErrImmutableEvidence,
				task.Key,
			)
		}
		taskIndexes[task.Key] = index
	}
	rangesByTask := make(map[TaskKey][]RangeState, len(tasks))
	for _, workRange := range ranges {
		if workRange.RunID != runID {
			return nil, nil, fmt.Errorf(
				"%w: structured range run identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, known := taskIndexes[workRange.Task]; !known {
			return nil, nil, fmt.Errorf(
				"%w: orphan structured range %q",
				ErrUnknownWork,
				workRange.ID,
			)
		}
		rangesByTask[workRange.Task] = append(
			rangesByTask[workRange.Task],
			workRange,
		)
	}
	for task := range rangesByTask {
		sort.Slice(rangesByTask[task], func(left, right int) bool {
			return rangesByTask[task][left].ID <
				rangesByTask[task][right].ID
		})
	}
	return taskIndexes, rangesByTask, nil
}

func validateStage4AggregateWorkComplete(
	tasks []WorkTask,
	rangesByTask map[TaskKey][]RangeState,
	completedAt time.Time,
) error {
	for _, task := range tasks {
		if strings.TrimSpace(task.Strategy) == "" ||
			strings.TrimSpace(task.TopologyHash) == "" ||
			task.StartedAt.IsZero() ||
			task.Status != "completed" ||
			task.CompletedAt.IsZero() ||
			task.CompletedAt.Before(task.StartedAt) ||
			task.CompletedAt.After(completedAt) ||
			!task.UpdatedAt.Equal(task.CompletedAt) ||
			len(rangesByTask[task.Key]) == 0 {
			return fmt.Errorf(
				"%w: structured task %#v is not durably complete",
				ErrStateTransition,
				task.Key,
			)
		}
		for _, workRange := range rangesByTask[task.Key] {
			if workRange.Strategy != task.Strategy ||
				workRange.TopologyHash != task.TopologyHash ||
				workRange.Status != "completed" ||
				workRange.CompletedAt.IsZero() ||
				workRange.CompletedAt.After(task.CompletedAt) ||
				workRange.CompletedAt.After(completedAt) ||
				!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
				workRange.SequenceOffset != 0 ||
				len(workRange.Pending) != 0 {
				return fmt.Errorf(
					"%w: structured range %q is not durably complete",
					ErrStateTransition,
					workRange.ID,
				)
			}
		}
	}
	return nil
}

var _ Stage4AggregateBackend = SQLiteStore{}
