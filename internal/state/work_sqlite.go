package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const sqliteWorkSchemaVersion = 1

func ensureSQLiteWorkSchema(database *sql.DB) error {
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin work-state schema: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.Exec(`
		CREATE TABLE IF NOT EXISTS state_schema_versions (
			component TEXT PRIMARY KEY,
			version INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS work_tasks (
			run_id TEXT NOT NULL,
			task_key TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (run_id, task_key)
		);
		CREATE TABLE IF NOT EXISTS work_ranges (
			run_id TEXT NOT NULL,
			task_key TEXT NOT NULL,
			range_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (run_id, task_key, range_id)
		);
	`); err != nil {
		return fmt.Errorf("initialize work-state schema: %w", err)
	}
	var version int
	err = transaction.QueryRow(`SELECT version FROM state_schema_versions WHERE component = 'stage2_work'`).Scan(&version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := transaction.Exec(`INSERT INTO state_schema_versions(component, version) VALUES ('stage2_work', ?)`, sqliteWorkSchemaVersion); err != nil {
			return fmt.Errorf("record work-state schema: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read work-state schema: %w", err)
	case version != sqliteWorkSchemaVersion:
		return fmt.Errorf("unsupported work-state schema version %d", version)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit work-state schema: %w", err)
	}
	return nil
}

func (store SQLiteStore) EnsureWorkPlan(task WorkTask, ranges []RangeState) (bool, error) {
	task, ranges, err := validateWorkPlan(task, ranges)
	if err != nil {
		return false, err
	}
	database, err := store.Open()
	if err != nil {
		return false, err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return false, err
	}
	transaction, err := database.Begin()
	if err != nil {
		return false, fmt.Errorf("begin ensure work plan: %w", err)
	}
	defer transaction.Rollback()
	key, _ := task.Key.canonical()
	existingTask, existingRanges, found, err := readSQLiteWorkPlan(transaction, task.RunID, task.Key)
	if err != nil {
		return false, err
	}
	if found {
		if !workPlanEqual(existingTask, existingRanges, task, ranges) {
			return false, fmt.Errorf("%w for %s", ErrTopologyChanged, key)
		}
		return false, nil
	}
	taskPayload, err := json.Marshal(task)
	if err != nil {
		return false, fmt.Errorf("encode work task: %w", err)
	}
	if _, err := transaction.Exec(`INSERT INTO work_tasks(run_id, task_key, payload) VALUES (?, ?, ?)`, task.RunID, key, string(taskPayload)); err != nil {
		return false, fmt.Errorf("create work task: %w", err)
	}
	for _, workRange := range ranges {
		payload, err := json.Marshal(workRange)
		if err != nil {
			return false, fmt.Errorf("encode work range %s: %w", workRange.ID, err)
		}
		if _, err := transaction.Exec(`INSERT INTO work_ranges(run_id, task_key, range_id, payload) VALUES (?, ?, ?, ?)`, task.RunID, key, workRange.ID, string(payload)); err != nil {
			return false, fmt.Errorf("create work range %s: %w", workRange.ID, err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit work plan: %w", err)
	}
	return true, nil
}

func (store SQLiteStore) ResetWorkPlan(task WorkTask, ranges []RangeState) error {
	task, ranges, err := validateWorkPlan(task, ranges)
	if err != nil {
		return err
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin reset work plan: %w", err)
	}
	defer transaction.Rollback()
	key, _ := task.Key.canonical()
	var existing string
	if err := transaction.QueryRow(`SELECT payload FROM work_tasks WHERE run_id = ? AND task_key = ?`, task.RunID, key).Scan(&existing); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: task %s", ErrUnknownWork, key)
	} else if err != nil {
		return fmt.Errorf("read work task: %w", err)
	}
	if _, err := transaction.Exec(`DELETE FROM work_ranges WHERE run_id = ? AND task_key = ?`, task.RunID, key); err != nil {
		return fmt.Errorf("clear stale work ranges: %w", err)
	}
	taskPayload, _ := json.Marshal(task)
	if _, err := transaction.Exec(`UPDATE work_tasks SET payload = ? WHERE run_id = ? AND task_key = ?`, string(taskPayload), task.RunID, key); err != nil {
		return fmt.Errorf("replace work task: %w", err)
	}
	for _, workRange := range ranges {
		payload, _ := json.Marshal(workRange)
		if _, err := transaction.Exec(`INSERT INTO work_ranges(run_id, task_key, range_id, payload) VALUES (?, ?, ?, ?)`, task.RunID, key, workRange.ID, string(payload)); err != nil {
			return fmt.Errorf("replace work range %s: %w", workRange.ID, err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit reset work plan: %w", err)
	}
	return nil
}

func (store SQLiteStore) ListWork(runID string) ([]WorkTask, []RangeState, error) {
	database, err := store.Open()
	if err != nil {
		return nil, nil, err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return nil, nil, err
	}
	taskRows, err := database.Query(`SELECT payload FROM work_tasks WHERE run_id = ? ORDER BY task_key`, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("list work tasks: %w", err)
	}
	var tasks []WorkTask
	for taskRows.Next() {
		var payload string
		var task WorkTask
		if err := taskRows.Scan(&payload); err != nil {
			taskRows.Close()
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(payload), &task); err != nil {
			taskRows.Close()
			return nil, nil, fmt.Errorf("decode work task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := taskRows.Close(); err != nil {
		return nil, nil, err
	}
	rangeRows, err := database.Query(`SELECT payload FROM work_ranges WHERE run_id = ? ORDER BY task_key, range_id`, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("list work ranges: %w", err)
	}
	defer rangeRows.Close()
	var ranges []RangeState
	for rangeRows.Next() {
		var payload string
		var workRange RangeState
		if err := rangeRows.Scan(&payload); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(payload), &workRange); err != nil {
			return nil, nil, fmt.Errorf("decode work range: %w", err)
		}
		ranges = append(ranges, workRange)
	}
	if err := rangeRows.Err(); err != nil {
		return nil, nil, err
	}
	return tasks, ranges, nil
}

func (store SQLiteStore) BeginRangeChunk(intent RangeChunkIntent) error {
	return store.mutateSQLiteRange(intent.RunID, intent.Task, intent.RangeID, func(workRange *RangeState) error {
		updated, err := applyRangeChunkIntent(*workRange, intent)
		*workRange = updated
		return err
	})
}

func (store SQLiteStore) RecordRangeAttempt(attempt RangeAttempt) (err error) {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return err
	}
	ctx := context.Background()
	connection, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire range attempt connection: %w", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("begin range attempt: %w", err)
	}
	transactionOpen := true
	defer func() {
		if !transactionOpen {
			return
		}
		if _, rollbackErr := connection.ExecContext(ctx, `ROLLBACK`); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback range attempt: %w", rollbackErr))
		}
	}()

	key, err := attempt.Task.canonical()
	if err != nil {
		return err
	}
	var taskPayload string
	queryErr := connection.QueryRowContext(ctx,
		`SELECT payload FROM work_tasks WHERE run_id = ? AND task_key = ?`,
		attempt.RunID, key,
	).Scan(&taskPayload)
	if errors.Is(queryErr, sql.ErrNoRows) {
		return fmt.Errorf("%w: task %s", ErrUnknownWork, key)
	}
	if queryErr != nil {
		return fmt.Errorf("read work task for range attempt: %w", queryErr)
	}
	var rangePayload string
	queryErr = connection.QueryRowContext(ctx,
		`SELECT payload FROM work_ranges WHERE run_id = ? AND task_key = ? AND range_id = ?`,
		attempt.RunID, key, attempt.RangeID,
	).Scan(&rangePayload)
	if errors.Is(queryErr, sql.ErrNoRows) {
		return fmt.Errorf("%w: range %q", ErrUnknownWork, attempt.RangeID)
	}
	if queryErr != nil {
		return fmt.Errorf("read work range for attempt: %w", queryErr)
	}
	var workTask WorkTask
	if err := json.Unmarshal([]byte(taskPayload), &workTask); err != nil {
		return fmt.Errorf("decode work task for range attempt: %w", err)
	}
	var workRange RangeState
	if err := json.Unmarshal([]byte(rangePayload), &workRange); err != nil {
		return fmt.Errorf("decode work range for attempt: %w", err)
	}
	updatedTask, updatedRange, err := applyRangeAttempt(workTask, workRange, attempt)
	if err != nil {
		return err
	}
	taskPayloadBytes, err := json.Marshal(updatedTask)
	if err != nil {
		return fmt.Errorf("encode work task for range attempt: %w", err)
	}
	rangePayloadBytes, err := json.Marshal(updatedRange)
	if err != nil {
		return fmt.Errorf("encode work range for attempt: %w", err)
	}
	taskResult, err := connection.ExecContext(ctx,
		`UPDATE work_tasks SET payload = ? WHERE run_id = ? AND task_key = ?`,
		string(taskPayloadBytes), attempt.RunID, key,
	)
	if err != nil {
		return fmt.Errorf("persist work task range attempt: %w", err)
	}
	if count, countErr := taskResult.RowsAffected(); countErr != nil || count != 1 {
		if countErr != nil {
			return fmt.Errorf("verify work task range attempt: %w", countErr)
		}
		return fmt.Errorf("%w: task changed", ErrUnknownWork)
	}
	rangeResult, err := connection.ExecContext(ctx,
		`UPDATE work_ranges SET payload = ? WHERE run_id = ? AND task_key = ? AND range_id = ?`,
		string(rangePayloadBytes), attempt.RunID, key, attempt.RangeID,
	)
	if err != nil {
		return fmt.Errorf("persist work range attempt: %w", err)
	}
	if count, countErr := rangeResult.RowsAffected(); countErr != nil || count != 1 {
		if countErr != nil {
			return fmt.Errorf("verify work range attempt: %w", countErr)
		}
		return fmt.Errorf("%w: range %q changed", ErrUnknownWork, attempt.RangeID)
	}
	if _, err := connection.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit range attempt: %w", err)
	}
	transactionOpen = false
	return nil
}

func (store SQLiteStore) AcknowledgeRange(acknowledgement RangeAcknowledgement) (RangeState, error) {
	database, err := store.Open()
	if err != nil {
		return RangeState{}, err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return RangeState{}, err
	}
	transaction, err := database.Begin()
	if err != nil {
		return RangeState{}, fmt.Errorf("begin range acknowledgement: %w", err)
	}
	defer transaction.Rollback()
	key, err := acknowledgement.Task.canonical()
	if err != nil {
		return RangeState{}, err
	}
	var payload string
	err = transaction.QueryRow(`SELECT payload FROM work_ranges WHERE run_id = ? AND task_key = ? AND range_id = ?`, acknowledgement.RunID, key, acknowledgement.RangeID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return RangeState{}, fmt.Errorf("%w: range %q", ErrUnknownWork, acknowledgement.RangeID)
	}
	if err != nil {
		return RangeState{}, fmt.Errorf("read work range: %w", err)
	}
	var workRange RangeState
	if err := json.Unmarshal([]byte(payload), &workRange); err != nil {
		return RangeState{}, fmt.Errorf("decode work range: %w", err)
	}
	updated, err := applyRangeAcknowledgement(workRange, acknowledgement)
	if err != nil {
		return RangeState{}, err
	}
	encoded, _ := json.Marshal(updated)
	result, err := transaction.Exec(`UPDATE work_ranges SET payload = ? WHERE run_id = ? AND task_key = ? AND range_id = ?`, string(encoded), acknowledgement.RunID, key, acknowledgement.RangeID)
	if err != nil {
		return RangeState{}, fmt.Errorf("persist range acknowledgement: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return RangeState{}, fmt.Errorf("%w: range %q changed", ErrUnknownWork, acknowledgement.RangeID)
	}
	if err := transaction.Commit(); err != nil {
		return RangeState{}, fmt.Errorf("commit range acknowledgement: %w", err)
	}
	return updated, nil
}

func (store SQLiteStore) CompleteRange(runID string, task TaskKey, rangeID, topologyHash string, expectedNext uint64, completedAt time.Time) error {
	return store.mutateSQLiteRange(runID, task, rangeID, func(workRange *RangeState) error {
		if workRange.TopologyHash != topologyHash {
			return ErrTopologyChanged
		}
		if workRange.Status != "running" || workRange.NextSequence != expectedNext || workRange.SequenceOffset != 0 || len(workRange.Pending) != 0 {
			return fmt.Errorf("%w: range %q has incomplete acknowledgements", ErrRangeOrder, rangeID)
		}
		workRange.Status = "completed"
		workRange.CompletedAt = completedAt.UTC()
		workRange.UpdatedAt = completedAt.UTC()
		return nil
	})
}

func (store SQLiteStore) CompleteWorkTask(runID string, task TaskKey, topologyHash string, completedAt time.Time) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	key, err := task.canonical()
	if err != nil {
		return err
	}
	var payload string
	if err := transaction.QueryRow(`SELECT payload FROM work_tasks WHERE run_id = ? AND task_key = ?`, runID, key).Scan(&payload); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: task", ErrUnknownWork)
	} else if err != nil {
		return err
	}
	rows, err := transaction.Query(`SELECT payload FROM work_ranges WHERE run_id = ? AND task_key = ?`, runID, key)
	if err != nil {
		return err
	}
	for rows.Next() {
		var rangePayload string
		var workRange RangeState
		if err := rows.Scan(&rangePayload); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(rangePayload), &workRange); err != nil {
			rows.Close()
			return err
		}
		if workRange.TopologyHash != topologyHash || workRange.Status != "completed" {
			rows.Close()
			return fmt.Errorf("%w: task has incomplete or stale ranges", ErrRangeOrder)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var workTask WorkTask
	if err := json.Unmarshal([]byte(payload), &workTask); err != nil {
		return err
	}
	if workTask.TopologyHash != topologyHash || workTask.Status != "running" {
		return fmt.Errorf("%w: task topology or status", ErrTopologyChanged)
	}
	workTask.Status = "completed"
	workTask.CompletedAt, workTask.UpdatedAt = completedAt.UTC(), completedAt.UTC()
	encoded, _ := json.Marshal(workTask)
	if _, err := transaction.Exec(`UPDATE work_tasks SET payload = ? WHERE run_id = ? AND task_key = ?`, string(encoded), runID, key); err != nil {
		return err
	}
	return transaction.Commit()
}

func (store SQLiteStore) mutateSQLiteRange(runID string, task TaskKey, rangeID string, mutate func(*RangeState) error) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	key, err := task.canonical()
	if err != nil {
		return err
	}
	var payload string
	if err := transaction.QueryRow(`SELECT payload FROM work_ranges WHERE run_id = ? AND task_key = ? AND range_id = ?`, runID, key, rangeID).Scan(&payload); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: range %q", ErrUnknownWork, rangeID)
	} else if err != nil {
		return err
	}
	var workRange RangeState
	if err := json.Unmarshal([]byte(payload), &workRange); err != nil {
		return err
	}
	if err := mutate(&workRange); err != nil {
		return err
	}
	encoded, _ := json.Marshal(workRange)
	if _, err := transaction.Exec(`UPDATE work_ranges SET payload = ? WHERE run_id = ? AND task_key = ? AND range_id = ?`, string(encoded), runID, key, rangeID); err != nil {
		return err
	}
	return transaction.Commit()
}

func readSQLiteWorkPlan(transaction *sql.Tx, runID string, task TaskKey) (WorkTask, []RangeState, bool, error) {
	key, err := task.canonical()
	if err != nil {
		return WorkTask{}, nil, false, err
	}
	var payload string
	err = transaction.QueryRow(`SELECT payload FROM work_tasks WHERE run_id = ? AND task_key = ?`, runID, key).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkTask{}, nil, false, nil
	}
	if err != nil {
		return WorkTask{}, nil, false, fmt.Errorf("read work task: %w", err)
	}
	var workTask WorkTask
	if err := json.Unmarshal([]byte(payload), &workTask); err != nil {
		return WorkTask{}, nil, false, fmt.Errorf("decode work task: %w", err)
	}
	rows, err := transaction.Query(`SELECT payload FROM work_ranges WHERE run_id = ? AND task_key = ? ORDER BY range_id`, runID, key)
	if err != nil {
		return WorkTask{}, nil, false, err
	}
	defer rows.Close()
	var ranges []RangeState
	for rows.Next() {
		var rangePayload string
		var workRange RangeState
		if err := rows.Scan(&rangePayload); err != nil {
			return WorkTask{}, nil, false, err
		}
		if err := json.Unmarshal([]byte(rangePayload), &workRange); err != nil {
			return WorkTask{}, nil, false, err
		}
		ranges = append(ranges, workRange)
	}
	sort.Slice(ranges, func(left, right int) bool { return ranges[left].ID < ranges[right].ID })
	return workTask, ranges, true, rows.Err()
}
