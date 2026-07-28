package app

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

func sqliteRangeTaskKey(table string) state.TaskKey {
	return state.TaskKey{Type: "table-copy", Schema: "main", Table: table}
}

func (observer tableCheckpointObserver) sqliteRangeBackend() (state.RangeBackend, error) {
	backend, ok := observer.store.(state.RangeBackend)
	if !ok {
		return nil, stateCheckpointError(
			"use range checkpoints",
			fmt.Errorf("state backend does not support range checkpoints"),
		)
	}
	return backend, nil
}

func (observer tableCheckpointObserver) BeforeSQLiteRangeAttempt(
	_ context.Context,
	chunk migrate.SQLiteRangeChunk,
) error {
	backend, err := observer.sqliteRangeBackend()
	if err != nil {
		return err
	}
	if err := backend.RecordRangeAttempt(state.RangeAttempt{
		RunID:        observer.runID,
		Task:         sqliteRangeTaskKey(chunk.Table),
		RangeID:      strconv.Itoa(chunk.Range.ID),
		TopologyHash: chunk.TopologyHash,
		Sequence:     chunk.Sequence,
		At:           time.Now().UTC(),
	}); err != nil {
		return stateCheckpointError("record range attempt", err)
	}
	return nil
}

func (observer tableCheckpointObserver) BeforeSQLiteRangeChunk(
	_ context.Context,
	chunk migrate.SQLiteRangeChunk,
) error {
	if chunk.Replay {
		return nil
	}
	backend, err := observer.sqliteRangeBackend()
	if err != nil {
		return err
	}
	frontier, valid, err := stateChunkFrontier(chunk)
	if err != nil {
		return stateCheckpointError("encode issued range frontier", err)
	}
	if err := backend.BeginRangeChunk(state.RangeChunkIntent{
		RunID:         observer.runID,
		Task:          sqliteRangeTaskKey(chunk.Table),
		RangeID:       strconv.Itoa(chunk.Range.ID),
		TopologyHash:  chunk.TopologyHash,
		Sequence:      chunk.Sequence,
		ChunkRows:     int64(chunk.ChunkRows),
		EndFrontier:   frontier,
		FrontierValid: valid,
		At:            time.Now().UTC(),
	}); err != nil {
		return stateCheckpointError("record issued range chunk", err)
	}
	return nil
}

func (observer tableCheckpointObserver) AfterSQLiteRangeChunk(
	_ context.Context,
	chunk migrate.SQLiteRangeChunk,
	receipt migrate.WriteReceipt,
	frontier migrate.AckFrontier,
) error {
	if err := receipt.Validate(); err != nil {
		return stateCheckpointError("validate durable range receipt", err)
	}
	durableRows := receipt.AcknowledgedRows()
	if durableRows <= 0 {
		return stateCheckpointError(
			"acknowledge range chunk",
			fmt.Errorf("receipt is not durably acknowledged"),
		)
	}
	backend, err := observer.sqliteRangeBackend()
	if err != nil {
		return err
	}
	typedFrontier, valid, err := stateChunkFrontier(chunk)
	if err != nil {
		return stateCheckpointError("encode acknowledged range frontier", err)
	}
	updated, err := backend.AcknowledgeRange(state.RangeAcknowledgement{
		RunID:         observer.runID,
		Task:          sqliteRangeTaskKey(chunk.Table),
		RangeID:       strconv.Itoa(chunk.Range.ID),
		TopologyHash:  chunk.TopologyHash,
		Sequence:      chunk.Sequence,
		ChunkRows:     int64(chunk.ChunkRows),
		AttemptOffset: receipt.AttemptOffset,
		DurableRows:   durableRows,
		Frontier:      typedFrontier,
		FrontierValid: valid,
		At:            time.Now().UTC(),
	})
	if err != nil {
		return stateCheckpointError("acknowledge range chunk", err)
	}
	if updated.NextSequence != frontier.NextSequence ||
		updated.SequenceOffset != frontier.SequenceOffset ||
		updated.RowsDone != frontier.Rows {
		return stateCheckpointError("verify acknowledged range frontier", fmt.Errorf(
			"state frontier (%d,%d,%d) differs from writer frontier (%d,%d,%d)",
			updated.NextSequence,
			updated.SequenceOffset,
			updated.RowsDone,
			frontier.NextSequence,
			frontier.SequenceOffset,
			frontier.Rows,
		))
	}
	return nil
}

func (observer tableCheckpointObserver) AfterSQLiteRangeProgress(
	_ context.Context,
	progress migrate.SQLiteRangeProgress,
) error {
	backend, err := observer.sqliteRangeBackend()
	if err != nil {
		return err
	}
	taskKey := sqliteRangeTaskKey(progress.Table)
	task, workRange, err := observer.loadSQLiteRangeState(
		backend,
		taskKey,
		strconv.Itoa(progress.Range.ID),
	)
	if err != nil {
		return stateCheckpointError("read range progress", err)
	}
	if err := verifySQLiteRangeProgress(task, workRange, progress); err != nil {
		return stateCheckpointError("verify range progress", err)
	}
	if !progress.Complete {
		return nil
	}
	if workRange.Status == "running" {
		if err := backend.CompleteRange(
			observer.runID,
			taskKey,
			workRange.ID,
			progress.TopologyHash,
			progress.ExpectedNextSequence,
			time.Now().UTC(),
		); err != nil {
			return stateCheckpointError("complete transfer range", err)
		}
	} else if workRange.Status != "completed" {
		return stateCheckpointError("complete transfer range", fmt.Errorf(
			"range %q is %s", workRange.ID, workRange.Status,
		))
	}

	tasks, ranges, err := backend.ListWork(observer.runID)
	if err != nil {
		return stateCheckpointError("read completed transfer ranges", err)
	}
	var refreshedTask *state.WorkTask
	allComplete := true
	foundRange := false
	for index := range tasks {
		if tasks[index].Key == taskKey {
			candidate := tasks[index]
			refreshedTask = &candidate
			break
		}
	}
	for _, candidate := range ranges {
		if candidate.Task != taskKey {
			continue
		}
		foundRange = true
		if candidate.TopologyHash != progress.TopologyHash ||
			candidate.Status != "completed" {
			allComplete = false
		}
	}
	if refreshedTask == nil || !foundRange {
		return stateCheckpointError("complete transfer task", state.ErrUnknownWork)
	}
	if !allComplete {
		return nil
	}
	if refreshedTask.Status == "completed" {
		return nil
	}
	if refreshedTask.Status != "running" {
		return stateCheckpointError("complete transfer task", fmt.Errorf(
			"task %q is %s", progress.Table, refreshedTask.Status,
		))
	}
	if err := backend.CompleteWorkTask(
		observer.runID,
		taskKey,
		progress.TopologyHash,
		time.Now().UTC(),
	); err != nil {
		return stateCheckpointError("complete transfer task", err)
	}
	return nil
}

func (observer tableCheckpointObserver) RestoreSQLiteRanges(
	_ context.Context,
	plan migrate.SQLiteTransferPlan,
) ([]migrate.SQLiteRangeRestore, error) {
	backend, err := observer.sqliteRangeBackend()
	if err != nil {
		return nil, err
	}
	tasks, ranges, err := backend.ListWork(observer.runID)
	if err != nil {
		return nil, stateCheckpointError("read range checkpoints", err)
	}
	taskKey := sqliteRangeTaskKey(plan.Table)
	var task *state.WorkTask
	for index := range tasks {
		if tasks[index].Key == taskKey {
			candidate := tasks[index]
			task = &candidate
			break
		}
	}
	if task == nil {
		return nil, stateCheckpointError("restore range checkpoints", state.ErrUnknownWork)
	}
	if task.TopologyHash != plan.Pagination.TopologyHash ||
		task.Strategy != string(plan.Pagination.Strategy) {
		return nil, stateCheckpointError(
			"restore range checkpoints",
			state.ErrTopologyChanged,
		)
	}

	savedByID := make(map[string]state.RangeState)
	for _, workRange := range ranges {
		if workRange.Task != taskKey {
			continue
		}
		if _, duplicate := savedByID[workRange.ID]; duplicate {
			return nil, stateCheckpointError(
				"restore range checkpoints",
				fmt.Errorf("duplicate range %q", workRange.ID),
			)
		}
		savedByID[workRange.ID] = workRange
	}
	restored := make([]migrate.SQLiteRangeRestore, 0, len(plan.Pagination.Ranges))
	for _, plannedRange := range plan.Pagination.Ranges {
		rangeID := strconv.Itoa(plannedRange.ID)
		saved, ok := savedByID[rangeID]
		if !ok {
			return nil, stateCheckpointError(
				"restore range checkpoints",
				fmt.Errorf("%w: range %q", state.ErrUnknownWork, rangeID),
			)
		}
		delete(savedByID, rangeID)
		restore, err := sqliteRangeRestore(plan, plannedRange, saved)
		if err != nil {
			return nil, stateCheckpointError("restore range checkpoints", err)
		}
		restored = append(restored, restore)
	}
	if len(savedByID) != 0 {
		return nil, stateCheckpointError(
			"restore range checkpoints",
			fmt.Errorf("saved range set differs from current plan"),
		)
	}
	return restored, nil
}

func sqliteRangeRestore(
	plan migrate.SQLiteTransferPlan,
	plannedRange migrate.PaginationRange,
	saved state.RangeState,
) (migrate.SQLiteRangeRestore, error) {
	if saved.TopologyHash != plan.Pagination.TopologyHash ||
		saved.Strategy != string(plan.Pagination.Strategy) {
		return migrate.SQLiteRangeRestore{}, state.ErrTopologyChanged
	}
	if saved.SequenceOffset != 0 {
		return migrate.SQLiteRangeRestore{}, fmt.Errorf(
			"SQLite range %q has unsupported partial sequence offset %d",
			saved.ID,
			saved.SequenceOffset,
		)
	}
	if saved.RowsDone < 0 {
		return migrate.SQLiteRangeRestore{}, fmt.Errorf(
			"SQLite range %q has negative progress", saved.ID,
		)
	}
	if saved.Status != "running" && saved.Status != "completed" {
		return migrate.SQLiteRangeRestore{}, fmt.Errorf(
			"SQLite range %q has invalid status %q", saved.ID, saved.Status,
		)
	}
	restore := migrate.SQLiteRangeRestore{
		Table:          plan.Table,
		TopologyHash:   plan.Pagination.TopologyHash,
		Range:          plannedRange,
		NextSequence:   saved.NextSequence,
		SequenceOffset: saved.SequenceOffset,
		RowsDone:       saved.RowsDone,
		Complete:       saved.Status == "completed",
	}
	if saved.FrontierValid {
		watermark, err := migrateTuple(saved.Frontier)
		if err != nil {
			return migrate.SQLiteRangeRestore{}, err
		}
		restore.Watermark = &watermark
	}
	if plan.Pagination.Strategy == migrate.PaginationRowNumber {
		restore.RowNumberWatermark = plannedRange.FirstRow - 1 + saved.RowsDone
		if plannedRange.Empty {
			restore.RowNumberWatermark = 0
		}
		if !plannedRange.Empty &&
			restore.RowNumberWatermark > plannedRange.LastRow {
			return migrate.SQLiteRangeRestore{}, fmt.Errorf(
				"SQLite range %q row-number frontier exceeds its bound",
				saved.ID,
			)
		}
	}
	if len(saved.Pending) > 1 {
		return migrate.SQLiteRangeRestore{}, fmt.Errorf(
			"SQLite range %q has multiple pending receipts", saved.ID,
		)
	}
	if restore.Complete && len(saved.Pending) != 0 {
		return migrate.SQLiteRangeRestore{}, fmt.Errorf(
			"completed SQLite range %q retains a pending receipt", saved.ID,
		)
	}
	if len(saved.Pending) == 1 {
		pending := saved.Pending[0]
		if pending.Sequence != saved.NextSequence || pending.DurableRows != 0 ||
			pending.ChunkRows <= 0 {
			return migrate.SQLiteRangeRestore{}, fmt.Errorf(
				"SQLite range %q has a non-restorable pending receipt",
				saved.ID,
			)
		}
		issued := migrate.SQLiteRangeChunk{
			Table:        plan.Table,
			TopologyHash: plan.Pagination.TopologyHash,
			Range:        plannedRange,
			Sequence:     pending.Sequence,
			ChunkRows:    int(pending.ChunkRows),
		}
		if pending.FrontierValid {
			end, err := migrateTuple(pending.Frontier)
			if err != nil {
				return migrate.SQLiteRangeRestore{}, err
			}
			issued.End = &end
		}
		if plan.Pagination.Strategy == migrate.PaginationRowNumber {
			if pending.FrontierValid {
				return migrate.SQLiteRangeRestore{}, fmt.Errorf(
					"ROW_NUMBER range %q has a typed issued frontier", saved.ID,
				)
			}
			issued.EndRow = restore.RowNumberWatermark + pending.ChunkRows
			if !plannedRange.Empty && issued.EndRow > plannedRange.LastRow {
				return migrate.SQLiteRangeRestore{}, fmt.Errorf(
					"ROW_NUMBER issued range %q exceeds its bound", saved.ID,
				)
			}
		} else if !pending.FrontierValid {
			return migrate.SQLiteRangeRestore{}, fmt.Errorf(
				"keyset range %q is missing its issued frontier", saved.ID,
			)
		}
		restore.Issued = &issued
	}
	return restore, nil
}

func stateChunkFrontier(
	chunk migrate.SQLiteRangeChunk,
) (state.TypedTuple, bool, error) {
	if chunk.End == nil {
		return nil, false, nil
	}
	frontier, err := stateTuple(chunk.End)
	if err != nil {
		return nil, false, err
	}
	return frontier, true, nil
}

func migrateTuple(tuple state.TypedTuple) (migrate.KeyTuple, error) {
	converted := make(migrate.KeyTuple, len(tuple))
	for index, value := range tuple {
		if err := value.Validate(); err != nil {
			return nil, err
		}
		switch value.Kind {
		case state.ValueInt64:
			converted[index] = migrate.KeyValue{
				Kind: migrate.KeyInteger, Encoded: value.Encoded,
			}
		case state.ValueText:
			converted[index] = migrate.KeyValue{
				Kind: migrate.KeyText, Encoded: value.Encoded,
			}
		case state.ValueBytes:
			converted[index] = migrate.KeyValue{
				Kind: migrate.KeyBytes, Encoded: value.Encoded,
			}
		default:
			return nil, fmt.Errorf(
				"unsupported range frontier kind %q", value.Kind,
			)
		}
	}
	return converted, nil
}

func (observer tableCheckpointObserver) loadSQLiteRangeState(
	backend state.RangeBackend,
	taskKey state.TaskKey,
	rangeID string,
) (state.WorkTask, state.RangeState, error) {
	tasks, ranges, err := backend.ListWork(observer.runID)
	if err != nil {
		return state.WorkTask{}, state.RangeState{}, err
	}
	var task state.WorkTask
	var workRange state.RangeState
	taskFound, rangeFound := false, false
	for _, candidate := range tasks {
		if candidate.Key == taskKey {
			task, taskFound = candidate, true
			break
		}
	}
	for _, candidate := range ranges {
		if candidate.Task == taskKey && candidate.ID == rangeID {
			workRange, rangeFound = candidate, true
			break
		}
	}
	if !taskFound || !rangeFound {
		return state.WorkTask{}, state.RangeState{}, state.ErrUnknownWork
	}
	return task, workRange, nil
}

func verifySQLiteRangeProgress(
	task state.WorkTask,
	workRange state.RangeState,
	progress migrate.SQLiteRangeProgress,
) error {
	if task.TopologyHash != progress.TopologyHash ||
		workRange.TopologyHash != progress.TopologyHash {
		return state.ErrTopologyChanged
	}
	if workRange.NextSequence != progress.Frontier.NextSequence ||
		workRange.SequenceOffset != progress.Frontier.SequenceOffset ||
		workRange.RowsDone != progress.Frontier.Rows {
		return fmt.Errorf(
			"durable range frontier (%d,%d,%d) differs from reported frontier (%d,%d,%d)",
			workRange.NextSequence,
			workRange.SequenceOffset,
			workRange.RowsDone,
			progress.Frontier.NextSequence,
			progress.Frontier.SequenceOffset,
			progress.Frontier.Rows,
		)
	}
	if progress.Watermark != nil {
		expected, err := stateTuple(progress.Watermark)
		if err != nil {
			return err
		}
		if !workRange.FrontierValid || !reflect.DeepEqual(workRange.Frontier, expected) {
			return fmt.Errorf("durable typed watermark differs from reported watermark")
		}
	}
	if progress.RowNumberWatermark != 0 {
		expected := progress.Range.FirstRow - 1 + workRange.RowsDone
		if expected != progress.RowNumberWatermark {
			return fmt.Errorf(
				"durable row-number watermark %d differs from reported watermark %d",
				expected,
				progress.RowNumberWatermark,
			)
		}
	}
	return nil
}
