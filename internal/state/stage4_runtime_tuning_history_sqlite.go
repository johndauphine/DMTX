package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const (
	stage4RuntimeTuningSessionRecord  = "runtime_tuning_session"
	stage4RuntimeTuningDecisionRecord = "runtime_tuning_decision"
	stage4RuntimeTuningCursorRecord   = "runtime_tuning_cursor"
	stage4RuntimeTuningCursorID       = "cursor"
)

var stage4RuntimeTuningHistoryNow = func() time.Time { return time.Now().UTC() }

func (store SQLiteStore) EnsureStage4RuntimeTuningSession(
	session Stage4RuntimeTuningSession,
) (Stage4RuntimeTuningSessionReceipt, bool, error) {
	normalized, err := normalizeStage4RuntimeTuningSession(session)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", fmt.Errorf(
				"begin runtime-tuning session: %w", err,
			))
	}
	defer transaction.Rollback()

	targetDigest, err := validateSQLiteStage4RuntimeTuningSessionAuthority(
		transaction,
		normalized,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	existing, found, err := readSQLiteStage4RuntimeTuningSession(
		transaction,
		normalized.RunID,
		normalized.SessionID,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	if found {
		if err := validateStage4RuntimeTuningStoredSession(
			existing,
			normalized,
			targetDigest,
		); err != nil {
			return Stage4RuntimeTuningSessionReceipt{}, false,
				stage4AggregateError("runtime-tuning session", err)
		}
		return existing.Clone(), false, nil
	}

	sessions, err := readSQLiteStage4RuntimeTuningSessions(
		transaction,
		normalized.RunID,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	if err := validateStage4RuntimeTuningSessionContinuity(
		sessions,
		normalized,
		targetDigest,
	); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	receipt, err := newStage4RuntimeTuningSessionReceipt(
		normalized,
		targetDigest,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", fmt.Errorf(
				"encode runtime-tuning session: %w", err,
			))
	}
	inserted, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningSessionRecord,
		normalized.RunID,
		stage4MigrationTaskKey,
		normalized.SessionID,
		string(payload),
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	if !inserted {
		var concurrent Stage4RuntimeTuningSessionReceipt
		if err := json.Unmarshal([]byte(existingPayload), &concurrent); err != nil {
			return Stage4RuntimeTuningSessionReceipt{}, false,
				stage4AggregateError("runtime-tuning session", fmt.Errorf(
					"decode concurrently stored runtime-tuning session: %w", err,
				))
		}
		if err := validateStage4RuntimeTuningStoredSession(
			concurrent,
			normalized,
			targetDigest,
		); err != nil {
			return Stage4RuntimeTuningSessionReceipt{}, false,
				stage4AggregateError("runtime-tuning session", err)
		}
		return concurrent.Clone(), false, nil
	}
	sessions = append(sessions, receipt)
	if err := pruneSQLiteStage4RuntimeTuningSessions(
		transaction,
		normalized.RunID,
		normalized.Task,
		normalized.SessionID,
		sessions,
	); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", err)
	}
	if err := transaction.Commit(); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session", fmt.Errorf(
				"commit runtime-tuning session: %w", err,
			))
	}
	return receipt.Clone(), true, nil
}

func (store SQLiteStore) LoadStage4RuntimeTuningSession(
	runID, sessionID string,
) (Stage4RuntimeTuningSessionReceipt, bool, error) {
	if err := validateStage4RuntimeTuningRunID(runID); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session read", err)
	}
	if _, err := normalizeStage4RuntimeTuningSessionID(
		"session ID", sessionID,
	); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session read", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session read", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session read", fmt.Errorf(
				"begin runtime-tuning session read: %w", err,
			))
	}
	defer transaction.Rollback()
	receipt, found, err := readSQLiteStage4RuntimeTuningSession(
		transaction, runID, sessionID,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false,
			stage4AggregateError("runtime-tuning session read", err)
	}
	return receipt.Clone(), found, nil
}

func (store SQLiteStore) EnsureStage4RuntimeTuningDecision(
	decision Stage4RuntimeTuningDecision,
) (Stage4RuntimeTuningDecisionReceipt, bool, error) {
	normalized, err := normalizeStage4RuntimeTuningDecision(decision)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", fmt.Errorf(
				"begin runtime-tuning decision: %w", err,
			))
	}
	defer transaction.Rollback()

	session, found, err := readSQLiteStage4RuntimeTuningSession(
		transaction,
		normalized.RunID,
		normalized.SessionID,
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if !found {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", fmt.Errorf(
				"%w: runtime-tuning decision session is unknown", ErrUnknownWork,
			))
	}
	if _, err := validateSQLiteStage4RuntimeTuningSessionAuthority(
		transaction,
		session.Session,
	); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if err := validateStage4RuntimeTuningDecisionSession(
		normalized,
		session.Session,
	); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	recordID := stage4RuntimeTuningDecisionRecordID(
		normalized.Boundary.Ordinal,
	)
	existing, existingFound, err := readSQLiteStage4RuntimeTuningDecision(
		transaction,
		normalized.RunID,
		normalized.SessionID,
		recordID,
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if existingFound {
		if !existing.Decision.Equal(normalized) {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", fmt.Errorf(
					"%w: runtime-tuning decision replay differs", ErrImmutableEvidence,
				))
		}
		return existing.Clone(), false, nil
	}
	cursor, cursorPayload, cursorFound, err := readSQLiteStage4RuntimeTuningCursor(
		transaction,
		normalized.RunID,
		normalized.SessionID,
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if err := validateStage4RuntimeTuningNextDecision(
		normalized,
		cursor,
		cursorFound,
	); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	receipt, err := newStage4RuntimeTuningDecisionReceipt(
		normalized,
		stage4RuntimeTuningHistoryNow(),
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", fmt.Errorf(
				"encode runtime-tuning decision: %w", err,
			))
	}
	inserted, concurrentPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningDecisionRecord,
		normalized.RunID,
		normalized.SessionID,
		recordID,
		string(payload),
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if !inserted {
		var concurrent Stage4RuntimeTuningDecisionReceipt
		if err := json.Unmarshal([]byte(concurrentPayload), &concurrent); err != nil {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", fmt.Errorf(
					"decode concurrently stored runtime-tuning decision: %w", err,
				))
		}
		concurrent, err = normalizeStoredStage4RuntimeTuningDecision(concurrent)
		if err != nil {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", err)
		}
		if !concurrent.Decision.Equal(normalized) {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", fmt.Errorf(
					"%w: runtime-tuning decision replay differs", ErrImmutableEvidence,
				))
		}
		return concurrent.Clone(), false, nil
	}
	nextCursor := stage4RuntimeTuningNextCursor(
		normalized,
		receipt.Digest,
		cursor,
		cursorFound,
		session.Session.DecisionLimit,
	)
	nextCursorReceipt, err := newStage4RuntimeTuningCursorReceipt(nextCursor)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	nextCursorPayload, err := json.Marshal(nextCursorReceipt)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", fmt.Errorf(
				"encode runtime-tuning cursor: %w", err,
			))
	}
	if !cursorFound {
		cursorInserted, _, err := insertSQLiteStage4Record(
			transaction,
			stage4RuntimeTuningCursorRecord,
			normalized.RunID,
			normalized.SessionID,
			stage4RuntimeTuningCursorID,
			string(nextCursorPayload),
		)
		if err != nil {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", err)
		}
		if !cursorInserted {
			return Stage4RuntimeTuningDecisionReceipt{}, false,
				stage4AggregateError("runtime-tuning decision", fmt.Errorf(
					"%w: runtime-tuning cursor appeared concurrently", ErrStateTransition,
				))
		}
	} else if err := updateSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningCursorRecord,
		normalized.RunID,
		normalized.SessionID,
		stage4RuntimeTuningCursorID,
		cursorPayload,
		string(nextCursorPayload),
	); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if err := pruneSQLiteStage4RuntimeTuningDecisions(
		transaction,
		normalized.RunID,
		normalized.SessionID,
		nextCursor.RetainedFromOrdinal,
	); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", err)
	}
	if err := transaction.Commit(); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false,
			stage4AggregateError("runtime-tuning decision", fmt.Errorf(
				"commit runtime-tuning decision: %w", err,
			))
	}
	return receipt.Clone(), true, nil
}

func (store SQLiteStore) LoadStage4RuntimeTuningDecisions(
	runID, sessionID string,
) ([]Stage4RuntimeTuningDecisionReceipt, error) {
	if err := validateStage4RuntimeTuningRunID(runID); err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	if _, err := normalizeStage4RuntimeTuningSessionID(
		"session ID", sessionID,
	); err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	database, err := store.openStage4()
	if err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", fmt.Errorf(
			"begin runtime-tuning decisions read: %w", err,
		))
	}
	defer transaction.Rollback()
	if _, found, err := readSQLiteStage4RuntimeTuningSession(
		transaction, runID, sessionID,
	); err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	} else if !found {
		return nil, stage4AggregateError("runtime-tuning decisions read", fmt.Errorf(
			"%w: runtime-tuning decision session is unknown", ErrUnknownWork,
		))
	}
	decisions, err := readSQLiteStage4RuntimeTuningDecisions(
		transaction, runID, sessionID,
	)
	if err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	cursor, _, cursorFound, err := readSQLiteStage4RuntimeTuningCursor(
		transaction, runID, sessionID,
	)
	if err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	if err := validateStage4RuntimeTuningDecisionHistory(
		decisions, cursor, cursorFound,
	); err != nil {
		return nil, stage4AggregateError("runtime-tuning decisions read", err)
	}
	result := make([]Stage4RuntimeTuningDecisionReceipt, len(decisions))
	for index := range decisions {
		result[index] = decisions[index].Clone()
	}
	return result, nil
}

func validateSQLiteStage4RuntimeTuningSessionAuthority(
	transaction *sql.Tx,
	session Stage4RuntimeTuningSession,
) (string, error) {
	runs, err := readSQLiteAggregateRuns(transaction, session.RunID)
	if err != nil {
		return "", err
	}
	latest, err := validateStage4RunIdentity(runs, session.RunID)
	if err != nil {
		return "", err
	}
	if latest.Outcome != Running || !latest.Resumable {
		return "", fmt.Errorf(
			"%w: runtime-tuning session requires an active resumable run",
			ErrStateTransition,
		)
	}
	if latest.SourceEngine != session.SourceEngine {
		return "", fmt.Errorf(
			"%w: runtime-tuning source engine differs from run identity",
			ErrImmutableEvidence,
		)
	}
	targetDigest, err := stage4RuntimeTuningTargetIdentityDigest(
		latest.TargetIdentity,
	)
	if err != nil {
		return "", err
	}
	if session.StartedAt.Before(latest.StartedAt) {
		return "", fmt.Errorf(
			"%w: runtime-tuning session precedes run start", ErrStateTransition,
		)
	}
	inventory, _, found, err := readSQLiteStage4TableInventory(
		transaction, session.RunID,
	)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf(
			"%w: runtime-tuning session table inventory", ErrUnknownWork,
		)
	}
	inventory, err = normalizeStoredStage4TableInventory(inventory)
	if err != nil {
		return "", err
	}
	var entry *Stage4TableInventoryEntry
	for index := range inventory.Inventory.Tables {
		candidate := &inventory.Inventory.Tables[index]
		if candidate.Task == session.Task {
			if entry != nil {
				return "", fmt.Errorf(
					"%w: duplicate runtime-tuning table inventory entry",
					ErrImmutableEvidence,
				)
			}
			entry = candidate
		}
	}
	if entry == nil || entry.TopologyHash != session.TopologyHash {
		return "", fmt.Errorf(
			"%w: runtime-tuning session differs from table inventory",
			ErrImmutableEvidence,
		)
	}
	ordinary, err := readSQLiteAggregateTasks(transaction, session.RunID)
	if err != nil {
		return "", err
	}
	ordinaryFound := false
	for _, task := range ordinary {
		if task.Table != session.Task.Table {
			continue
		}
		if ordinaryFound || task.RunID != session.RunID || task.Status != "running" ||
			task.RowsDone != 0 || !task.CompletedAt.IsZero() {
			return "", fmt.Errorf(
				"%w: runtime-tuning ordinary task authority differs",
				ErrStateTransition,
			)
		}
		ordinaryFound = true
	}
	if !ordinaryFound {
		return "", fmt.Errorf(
			"%w: runtime-tuning ordinary task is unknown", ErrUnknownWork,
		)
	}
	workTasks, workRanges, err := readSQLiteAggregateWork(
		transaction, session.RunID,
	)
	if err != nil {
		return "", err
	}
	indexes, rangesByTask, err := indexStage4AggregateWork(
		session.RunID, workTasks, workRanges,
	)
	if err != nil {
		return "", err
	}
	index, found := indexes[session.Task]
	if !found {
		return "", fmt.Errorf(
			"%w: runtime-tuning structured task is unknown", ErrUnknownWork,
		)
	}
	work := workTasks[index]
	if work.Status != "running" || work.TopologyHash != session.TopologyHash ||
		work.Strategy != entry.Strategy || work.RunID != session.RunID {
		return "", fmt.Errorf(
			"%w: runtime-tuning structured task authority differs",
			ErrImmutableEvidence,
		)
	}
	ranges := rangesByTask[session.Task]
	if len(ranges) != len(entry.Ranges) || len(ranges) == 0 {
		return "", fmt.Errorf(
			"%w: runtime-tuning range inventory differs", ErrImmutableEvidence,
		)
	}
	byID := make(map[string]RangeState, len(ranges))
	for _, workRange := range ranges {
		if workRange.RunID != session.RunID ||
			workRange.Task != session.Task ||
			workRange.TopologyHash != session.TopologyHash ||
			workRange.Strategy != entry.Strategy {
			return "", fmt.Errorf(
				"%w: runtime-tuning range authority differs", ErrImmutableEvidence,
			)
		}
		if _, duplicate := byID[workRange.ID]; duplicate {
			return "", fmt.Errorf(
				"%w: duplicate runtime-tuning range", ErrImmutableEvidence,
			)
		}
		byID[workRange.ID] = workRange
	}
	for _, expected := range entry.Ranges {
		if _, found := byID[expected.ID]; !found {
			return "", fmt.Errorf(
				"%w: runtime-tuning range inventory differs", ErrImmutableEvidence,
			)
		}
	}
	return targetDigest, nil
}

func validateStage4RuntimeTuningStoredSession(
	stored Stage4RuntimeTuningSessionReceipt,
	requested Stage4RuntimeTuningSession,
	targetDigest string,
) error {
	stored, err := normalizeStoredStage4RuntimeTuningSession(stored)
	if err != nil {
		return err
	}
	requested, err = normalizeStage4RuntimeTuningSession(requested)
	if err != nil {
		return err
	}
	if stored.TargetIdentityDigest != targetDigest ||
		!stored.Session.Equal(requested) {
		return fmt.Errorf(
			"%w: runtime-tuning session authority differs", ErrImmutableEvidence,
		)
	}
	return nil
}

func validateStage4RuntimeTuningSessionContinuity(
	sessions []Stage4RuntimeTuningSessionReceipt,
	requested Stage4RuntimeTuningSession,
	targetDigest string,
) error {
	for _, receipt := range sessions {
		receipt, err := normalizeStoredStage4RuntimeTuningSession(receipt)
		if err != nil {
			return err
		}
		if receipt.Session.Task != requested.Task {
			continue
		}
		if receipt.TargetIdentityDigest != targetDigest ||
			receipt.Session.TopologyHash != requested.TopologyHash ||
			receipt.Session.SourceEngine != requested.SourceEngine ||
			receipt.Session.TargetEngine != requested.TargetEngine ||
			receipt.Session.IntentDigest != requested.IntentDigest ||
			receipt.Session.IntervalNanos != requested.IntervalNanos ||
			receipt.Session.DecisionLimit != requested.DecisionLimit {
			return fmt.Errorf(
				"%w: runtime-tuning resume session authority differs",
				ErrImmutableEvidence,
			)
		}
	}
	return nil
}

func validateStage4RuntimeTuningDecisionSession(
	decision Stage4RuntimeTuningDecision,
	session Stage4RuntimeTuningSession,
) error {
	if decision.RunID != session.RunID || decision.SessionID != session.SessionID ||
		decision.Boundary.TableSchema != session.Task.Schema ||
		decision.Boundary.TableName != session.Task.Table {
		return fmt.Errorf(
			"%w: runtime-tuning decision differs from its session authority",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func validateStage4RuntimeTuningNextDecision(
	decision Stage4RuntimeTuningDecision,
	cursor Stage4RuntimeTuningHistoryCursorReceipt,
	found bool,
) error {
	if !found {
		if decision.Boundary.Ordinal != 1 || decision.PreviousDigest != "" {
			return fmt.Errorf(
				"%w: runtime-tuning first decision is not ordinal one",
				ErrStateTransition,
			)
		}
		return nil
	}
	cursor, err := normalizeStoredStage4RuntimeTuningCursor(cursor)
	if err != nil {
		return err
	}
	if decision.RunID != cursor.Cursor.RunID ||
		decision.SessionID != cursor.Cursor.SessionID ||
		cursor.Cursor.LastOrdinal == ^uint64(0) ||
		decision.Boundary.Ordinal != cursor.Cursor.LastOrdinal+1 ||
		decision.PreviousDigest != cursor.Cursor.LastDigest {
		return fmt.Errorf(
			"%w: runtime-tuning decision order differs", ErrImmutableEvidence,
		)
	}
	return nil
}

func stage4RuntimeTuningNextCursor(
	decision Stage4RuntimeTuningDecision,
	digest string,
	previous Stage4RuntimeTuningHistoryCursorReceipt,
	found bool,
	limit int,
) Stage4RuntimeTuningHistoryCursor {
	next := Stage4RuntimeTuningHistoryCursor{
		Version:             Stage4RuntimeTuningHistoryVersion,
		RunID:               decision.RunID,
		SessionID:           decision.SessionID,
		LastOrdinal:         decision.Boundary.Ordinal,
		LastDigest:          digest,
		TotalDecisions:      decision.Boundary.Ordinal,
		RetainedFromOrdinal: decision.Boundary.Ordinal,
		RetainedDecisions:   1,
	}
	if !found {
		return next
	}
	next.RetainedFromOrdinal = previous.Cursor.RetainedFromOrdinal
	next.RetainedDecisions = previous.Cursor.RetainedDecisions + 1
	if next.RetainedDecisions > limit {
		next.RetainedDecisions = limit
		next.RetainedFromOrdinal++
	}
	return next
}

func validateStage4RuntimeTuningDecisionHistory(
	decisions []Stage4RuntimeTuningDecisionReceipt,
	cursor Stage4RuntimeTuningHistoryCursorReceipt,
	cursorFound bool,
) error {
	if !cursorFound {
		if len(decisions) != 0 {
			return fmt.Errorf(
				"%w: runtime-tuning decisions have no cursor", ErrImmutableEvidence,
			)
		}
		return nil
	}
	cursor, err := normalizeStoredStage4RuntimeTuningCursor(cursor)
	if err != nil {
		return err
	}
	if len(decisions) != cursor.Cursor.RetainedDecisions {
		return fmt.Errorf(
			"%w: runtime-tuning retained decision count differs",
			ErrImmutableEvidence,
		)
	}
	for index := range decisions {
		decisions[index], err = normalizeStoredStage4RuntimeTuningDecision(
			decisions[index],
		)
		if err != nil {
			return err
		}
		if decisions[index].Decision.RunID != cursor.Cursor.RunID ||
			decisions[index].Decision.SessionID != cursor.Cursor.SessionID ||
			decisions[index].Decision.Boundary.Ordinal !=
				cursor.Cursor.RetainedFromOrdinal+uint64(index) {
			return fmt.Errorf(
				"%w: runtime-tuning retained decision order differs",
				ErrImmutableEvidence,
			)
		}
		if index == 0 {
			continue
		}
		if decisions[index].Decision.PreviousDigest != decisions[index-1].Digest {
			return fmt.Errorf(
				"%w: runtime-tuning decision chain differs", ErrImmutableEvidence,
			)
		}
	}
	if len(decisions) == 0 ||
		decisions[len(decisions)-1].Digest != cursor.Cursor.LastDigest ||
		decisions[len(decisions)-1].Decision.Boundary.Ordinal != cursor.Cursor.LastOrdinal {
		return fmt.Errorf(
			"%w: runtime-tuning cursor tail differs", ErrImmutableEvidence,
		)
	}
	return nil
}

func readSQLiteStage4RuntimeTuningSession(
	transaction *sql.Tx,
	runID, sessionID string,
) (Stage4RuntimeTuningSessionReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningSessionRecord,
		runID,
		stage4MigrationTaskKey,
		sessionID,
	)
	if err != nil || !found {
		return Stage4RuntimeTuningSessionReceipt{}, found, err
	}
	var receipt Stage4RuntimeTuningSessionReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false, fmt.Errorf(
			"decode runtime-tuning session: %w", err,
		)
	}
	receipt, err = normalizeStoredStage4RuntimeTuningSession(receipt)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, false, err
	}
	if receipt.Session.RunID != runID || receipt.Session.SessionID != sessionID {
		return Stage4RuntimeTuningSessionReceipt{}, false, fmt.Errorf(
			"%w: runtime-tuning session record identity differs",
			ErrImmutableEvidence,
		)
	}
	return receipt, true, nil
}

func readSQLiteStage4RuntimeTuningSessions(
	transaction *sql.Tx,
	runID string,
) ([]Stage4RuntimeTuningSessionReceipt, error) {
	rows, err := transaction.Query(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
		ORDER BY record_id
	`, stage4RuntimeTuningSessionRecord, runID, stage4MigrationTaskKey)
	if err != nil {
		return nil, fmt.Errorf("read runtime-tuning sessions: %w", err)
	}
	defer rows.Close()
	result := make([]Stage4RuntimeTuningSessionReceipt, 0)
	seen := make(map[string]struct{})
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var receipt Stage4RuntimeTuningSessionReceipt
		if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
			return nil, fmt.Errorf("decode runtime-tuning session: %w", err)
		}
		receipt, err = normalizeStoredStage4RuntimeTuningSession(receipt)
		if err != nil {
			return nil, err
		}
		if receipt.Session.RunID != runID {
			return nil, fmt.Errorf(
				"%w: runtime-tuning session run identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := seen[receipt.Session.SessionID]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate runtime-tuning session", ErrImmutableEvidence,
			)
		}
		seen[receipt.Session.SessionID] = struct{}{}
		result = append(result, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime-tuning sessions: %w", err)
	}
	return result, nil
}

func readSQLiteStage4RuntimeTuningDecision(
	transaction *sql.Tx,
	runID, sessionID, recordID string,
) (Stage4RuntimeTuningDecisionReceipt, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningDecisionRecord,
		runID,
		sessionID,
		recordID,
	)
	if err != nil || !found {
		return Stage4RuntimeTuningDecisionReceipt{}, found, err
	}
	var receipt Stage4RuntimeTuningDecisionReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false, fmt.Errorf(
			"decode runtime-tuning decision: %w", err,
		)
	}
	receipt, err = normalizeStoredStage4RuntimeTuningDecision(receipt)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, false, err
	}
	if receipt.Decision.RunID != runID || receipt.Decision.SessionID != sessionID ||
		recordID != stage4RuntimeTuningDecisionRecordID(
			receipt.Decision.Boundary.Ordinal,
		) {
		return Stage4RuntimeTuningDecisionReceipt{}, false, fmt.Errorf(
			"%w: runtime-tuning decision record identity differs",
			ErrImmutableEvidence,
		)
	}
	return receipt, true, nil
}

func readSQLiteStage4RuntimeTuningDecisions(
	transaction *sql.Tx,
	runID, sessionID string,
) ([]Stage4RuntimeTuningDecisionReceipt, error) {
	rows, err := transaction.Query(`
		SELECT record_id, payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
		ORDER BY record_id
	`, stage4RuntimeTuningDecisionRecord, runID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("read runtime-tuning decisions: %w", err)
	}
	defer rows.Close()
	result := make([]Stage4RuntimeTuningDecisionReceipt, 0)
	for rows.Next() {
		var recordID, payload string
		if err := rows.Scan(&recordID, &payload); err != nil {
			return nil, err
		}
		var receipt Stage4RuntimeTuningDecisionReceipt
		if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
			return nil, fmt.Errorf("decode runtime-tuning decision: %w", err)
		}
		receipt, err = normalizeStoredStage4RuntimeTuningDecision(receipt)
		if err != nil {
			return nil, err
		}
		if receipt.Decision.RunID != runID ||
			receipt.Decision.SessionID != sessionID ||
			recordID != stage4RuntimeTuningDecisionRecordID(
				receipt.Decision.Boundary.Ordinal,
			) {
			return nil, fmt.Errorf(
				"%w: runtime-tuning decision record identity differs",
				ErrImmutableEvidence,
			)
		}
		result = append(result, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime-tuning decisions: %w", err)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Decision.Boundary.Ordinal <
			result[right].Decision.Boundary.Ordinal
	})
	return result, nil
}

func readSQLiteStage4RuntimeTuningCursor(
	transaction *sql.Tx,
	runID, sessionID string,
) (Stage4RuntimeTuningHistoryCursorReceipt, string, bool, error) {
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4RuntimeTuningCursorRecord,
		runID,
		sessionID,
		stage4RuntimeTuningCursorID,
	)
	if err != nil || !found {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, "", found, err
	}
	var receipt Stage4RuntimeTuningHistoryCursorReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, "", false, fmt.Errorf(
			"decode runtime-tuning cursor: %w", err,
		)
	}
	receipt, err = normalizeStoredStage4RuntimeTuningCursor(receipt)
	if err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, "", false, err
	}
	if receipt.Cursor.RunID != runID || receipt.Cursor.SessionID != sessionID {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, "", false, fmt.Errorf(
			"%w: runtime-tuning cursor identity differs", ErrImmutableEvidence,
		)
	}
	return receipt, payload, true, nil
}

func pruneSQLiteStage4RuntimeTuningSessions(
	transaction *sql.Tx,
	runID string,
	task TaskKey,
	currentSessionID string,
	sessions []Stage4RuntimeTuningSessionReceipt,
) error {
	matching := make([]Stage4RuntimeTuningSessionReceipt, 0, len(sessions))
	for _, receipt := range sessions {
		if receipt.Session.Task == task {
			matching = append(matching, receipt)
		}
	}
	if len(matching) <= Stage4RuntimeTuningSessionRetention {
		return nil
	}
	stage4RuntimeTuningSessionsByAge(matching)
	remaining := len(matching)
	for _, receipt := range matching {
		if remaining <= Stage4RuntimeTuningSessionRetention {
			break
		}
		if receipt.Session.SessionID == currentSessionID {
			continue
		}
		if err := deleteSQLiteStage4RuntimeTuningSession(
			transaction, runID, receipt.Session.SessionID,
		); err != nil {
			return err
		}
		remaining--
	}
	if remaining > Stage4RuntimeTuningSessionRetention {
		return fmt.Errorf(
			"%w: runtime-tuning session retention cannot remove current session",
			ErrStateTransition,
		)
	}
	return nil
}

func deleteSQLiteStage4RuntimeTuningSession(
	transaction *sql.Tx,
	runID, sessionID string,
) error {
	for _, kind := range []string{
		stage4RuntimeTuningDecisionRecord,
		stage4RuntimeTuningCursorRecord,
	} {
		if _, err := transaction.Exec(`
			DELETE FROM stage4_records
			WHERE kind = ? AND run_id = ? AND task_key = ?
		`, kind, runID, sessionID); err != nil {
			return fmt.Errorf("prune %s runtime-tuning history: %w", kind, err)
		}
	}
	result, err := transaction.Exec(`
		DELETE FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ? AND record_id = ?
	`, stage4RuntimeTuningSessionRecord, runID,
		stage4MigrationTaskKey, sessionID)
	if err != nil {
		return fmt.Errorf("prune runtime-tuning session: %w", err)
	}
	if err := requireOneSQLiteMutation(result, "runtime-tuning session"); err != nil {
		return err
	}
	return nil
}

func pruneSQLiteStage4RuntimeTuningDecisions(
	transaction *sql.Tx,
	runID, sessionID string,
	retainedFrom uint64,
) error {
	decisions, err := readSQLiteStage4RuntimeTuningDecisions(
		transaction, runID, sessionID,
	)
	if err != nil {
		return err
	}
	for _, receipt := range decisions {
		if receipt.Decision.Boundary.Ordinal >= retainedFrom {
			continue
		}
		if _, err := transaction.Exec(`
			DELETE FROM stage4_records
			WHERE kind = ? AND run_id = ? AND task_key = ? AND record_id = ?
		`, stage4RuntimeTuningDecisionRecord, runID, sessionID,
			stage4RuntimeTuningDecisionRecordID(receipt.Decision.Boundary.Ordinal)); err != nil {
			return fmt.Errorf("prune runtime-tuning decision: %w", err)
		}
	}
	return nil
}

func stage4RuntimeTuningDecisionRecordID(ordinal uint64) string {
	return fmt.Sprintf("%020d", ordinal)
}
