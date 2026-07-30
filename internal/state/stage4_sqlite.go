package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	sqliteStage4SchemaVersion = 1

	stage4SchemaRecord          = "schema_snapshot"
	stage4IncrementalRecord     = "incremental_attempt"
	stage4DeleteRecord          = "delete_reconciliation"
	stage4StrictRecord          = "strict_snapshot"
	stage4StrictMigrationRecord = "strict_migration_snapshot"
	stage4SingletonRecordID     = "table"
	stage4MigrationTaskKey      = "migration"
)

var (
	sqliteStage4IncrementalBeginMu sync.Mutex
	stage4BeforeIncrementalCommit  = func() error { return nil }
)

func ensureSQLiteStage4Schema(database *sql.DB) error {
	if err := ensureSQLiteWorkSchema(database); err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin Stage 4 state schema: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.Exec(`
		CREATE TABLE IF NOT EXISTS stage4_records (
			kind TEXT NOT NULL,
			run_id TEXT NOT NULL,
			task_key TEXT NOT NULL,
			record_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (kind, run_id, task_key, record_id)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS stage4_one_incremental_attempt_per_task
		ON stage4_records(run_id, task_key)
		WHERE kind = 'incremental_attempt';
	`); err != nil {
		return fmt.Errorf("initialize Stage 4 state schema: %w", err)
	}
	var version int
	err = transaction.QueryRow(`SELECT version FROM state_schema_versions WHERE component = 'stage4_state'`).Scan(&version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := transaction.Exec(
			`INSERT INTO state_schema_versions(component, version) VALUES ('stage4_state', ?)`,
			sqliteStage4SchemaVersion,
		); err != nil {
			return fmt.Errorf("record Stage 4 state schema: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read Stage 4 state schema: %w", err)
	case version != sqliteStage4SchemaVersion:
		return fmt.Errorf("unsupported Stage 4 state schema version %d", version)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit Stage 4 state schema: %w", err)
	}
	return nil
}

func (store SQLiteStore) openStage4() (*sql.DB, error) {
	database, err := store.Open()
	if err != nil {
		return nil, err
	}
	if err := ensureSQLiteStage4Schema(database); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func requireSQLiteStage4Identity(transaction *sql.Tx, runID string, task TaskKey) (string, error) {
	if err := validateStage4Identity(runID, task); err != nil {
		return "", err
	}
	var exists int
	if err := transaction.QueryRow(`SELECT 1 FROM runs WHERE id = ? LIMIT 1`, runID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	} else if err != nil {
		return "", fmt.Errorf("read Stage 4 run identity: %w", err)
	}
	taskKey, err := task.canonical()
	if err != nil {
		return "", err
	}
	if err := transaction.QueryRow(
		`SELECT 1 FROM work_tasks WHERE run_id = ? AND task_key = ?`,
		runID,
		taskKey,
	).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: task %s", ErrUnknownWork, taskKey)
	} else if err != nil {
		return "", fmt.Errorf("read Stage 4 task identity: %w", err)
	}
	return taskKey, nil
}

func requireSQLiteRun(transaction *sql.Tx, runID string) (Run, error) {
	if runID == "" {
		return Run{}, fmt.Errorf("Stage 4 run ID is required")
	}
	row := transaction.QueryRow(`
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, reason, started_at, ended_at
		FROM runs WHERE id = ? ORDER BY started_at DESC, rowid DESC LIMIT 1
	`, runID)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	if err != nil {
		return Run{}, fmt.Errorf("read Stage 4 run identity: %w", err)
	}
	return run, nil
}

func loadSQLiteRunIdentity(transaction *sql.Tx, runID string) (string, string, bool, error) {
	var sourceIdentity, targetIdentity string
	err := transaction.QueryRow(
		`SELECT source_identity, target_identity
		 FROM runs WHERE id = ? ORDER BY started_at DESC, rowid DESC LIMIT 1`,
		runID,
	).Scan(&sourceIdentity, &targetIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("read Stage 4 run identity: %w", err)
	}
	return sourceIdentity, targetIdentity, true, nil
}

func incrementalAttemptFromPayload(payload string) (IncrementalAttempt, error) {
	var attempt IncrementalAttempt
	if err := json.Unmarshal([]byte(payload), &attempt); err != nil {
		return IncrementalAttempt{}, fmt.Errorf("decode incremental attempt: %w", err)
	}
	return attempt, nil
}

func deleteReconciliationFromPayload(payload string) (DeleteReconciliation, error) {
	var record DeleteReconciliation
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return DeleteReconciliation{}, fmt.Errorf("decode delete reconciliation: %w", err)
	}
	return record, nil
}

func loadSQLiteIncrementalForTask(
	transaction *sql.Tx,
	runID string,
	taskKey string,
) (IncrementalAttempt, bool, error) {
	var payload string
	err := transaction.QueryRow(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
		LIMIT 1
	`, stage4IncrementalRecord, runID, taskKey).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return IncrementalAttempt{}, false, nil
	}
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("read incremental attempt: %w", err)
	}
	attempt, err := incrementalAttemptFromPayload(payload)
	return attempt, true, err
}

func loadSQLiteLatestCommittedIncremental(
	transaction *sql.Tx,
	runID string,
	taskKey string,
) (IncrementalAttempt, bool, error) {
	sourceIdentity, targetIdentity, found, err := loadSQLiteRunIdentity(transaction, runID)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	if !found {
		return IncrementalAttempt{}, false, fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	rows, err := transaction.Query(`
		SELECT record.payload, record.run_id, run.source_identity, run.target_identity,
		       run.outcome, run.started_at
		FROM stage4_records AS record
		JOIN runs AS run ON run.id = record.run_id
		WHERE record.kind = ? AND record.task_key = ?
		  AND run.rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		  )
	`, stage4IncrementalRecord, taskKey)
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("query committed incremental attempts: %w", err)
	}
	defer rows.Close()
	var latest IncrementalAttempt
	var latestFound bool
	var sameRun IncrementalAttempt
	var sameRunFound bool
	var identityUnavailable bool
	var latestOrder, sameRunOrder stage4EvidenceOrder
	for rows.Next() {
		var payload, candidateRunID, candidateSourceIdentity, candidateTargetIdentity string
		var candidateOutcome Outcome
		var candidateRunStartedAt time.Time
		if err := rows.Scan(
			&payload,
			&candidateRunID,
			&candidateSourceIdentity,
			&candidateTargetIdentity,
			&candidateOutcome,
			&candidateRunStartedAt,
		); err != nil {
			return IncrementalAttempt{}, false, fmt.Errorf("read committed incremental attempt: %w", err)
		}
		attempt, err := incrementalAttemptFromPayload(payload)
		if err != nil {
			return IncrementalAttempt{}, false, err
		}
		if attempt.Status != IncrementalCompleted || !attempt.TableSucceeded {
			continue
		}
		candidateOrder := incrementalEvidenceOrder(
			candidateRunStartedAt,
			attempt,
		)
		if candidateRunID == runID {
			if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
				sameRun, sameRunFound = attempt, true
				sameRunOrder = candidateOrder
			}
			continue
		}
		if candidateRunID != runID {
			if candidateOutcome != Success {
				continue
			}
			if sourceIdentity == "" || targetIdentity == "" ||
				candidateSourceIdentity == "" || candidateTargetIdentity == "" {
				identityUnavailable = true
				continue
			}
			if candidateSourceIdentity != sourceIdentity || candidateTargetIdentity != targetIdentity {
				continue
			}
		}
		if !latestFound || laterStage4Evidence(candidateOrder, latestOrder) {
			latest, latestFound = attempt, true
			latestOrder = candidateOrder
		}
	}
	if err := rows.Err(); err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("iterate committed incremental attempts: %w", err)
	}
	if sameRunFound {
		return sameRun, true, nil
	}
	if identityUnavailable {
		return IncrementalAttempt{}, false, fmt.Errorf(
			"%w: cannot select an incremental frontier for run %q",
			ErrCrossRunIdentityUnavailable,
			runID,
		)
	}
	if latestFound {
		return latest, true, nil
	}
	return IncrementalAttempt{}, false, nil
}

func loadSQLiteLatestSuccessfulDelete(
	transaction *sql.Tx,
	runID string,
	taskKey string,
) (DeleteReconciliation, bool, error) {
	sourceIdentity, targetIdentity, found, err := loadSQLiteRunIdentity(transaction, runID)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	if !found {
		return DeleteReconciliation{}, false, fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	rows, err := transaction.Query(`
		SELECT record.payload, record.run_id, run.source_identity, run.target_identity,
		       run.outcome, run.started_at
		FROM stage4_records AS record
		JOIN runs AS run ON run.id = record.run_id
		WHERE record.kind = ? AND record.task_key = ?
		  AND run.rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		  )
	`, stage4DeleteRecord, taskKey)
	if err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("query successful delete reconciliations: %w", err)
	}
	defer rows.Close()
	var latest DeleteReconciliation
	var latestFound bool
	var sameRun DeleteReconciliation
	var sameRunFound bool
	var identityUnavailable bool
	var latestOrder, sameRunOrder stage4EvidenceOrder
	for rows.Next() {
		var payload, candidateRunID, candidateSourceIdentity, candidateTargetIdentity string
		var candidateOutcome Outcome
		var candidateRunStartedAt time.Time
		if err := rows.Scan(
			&payload,
			&candidateRunID,
			&candidateSourceIdentity,
			&candidateTargetIdentity,
			&candidateOutcome,
			&candidateRunStartedAt,
		); err != nil {
			return DeleteReconciliation{}, false, fmt.Errorf("read successful delete reconciliation: %w", err)
		}
		record, err := deleteReconciliationFromPayload(payload)
		if err != nil {
			return DeleteReconciliation{}, false, err
		}
		if record.Status != DeleteReconciliationCompleted {
			continue
		}
		candidateOrder := deleteEvidenceOrder(
			candidateRunStartedAt,
			record,
		)
		if candidateRunID == runID {
			if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
				sameRun, sameRunFound = record, true
				sameRunOrder = candidateOrder
			}
			continue
		}
		if candidateRunID != runID {
			if candidateOutcome != Success {
				continue
			}
			if sourceIdentity == "" || targetIdentity == "" ||
				candidateSourceIdentity == "" || candidateTargetIdentity == "" {
				identityUnavailable = true
				continue
			}
			if candidateSourceIdentity != sourceIdentity || candidateTargetIdentity != targetIdentity {
				continue
			}
		}
		if !latestFound || laterStage4Evidence(candidateOrder, latestOrder) {
			latest, latestFound = record, true
			latestOrder = candidateOrder
		}
	}
	if err := rows.Err(); err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("iterate successful delete reconciliations: %w", err)
	}
	if sameRunFound {
		return sameRun, true, nil
	}
	if identityUnavailable {
		return DeleteReconciliation{}, false, fmt.Errorf(
			"%w: cannot select delete reconciliation history for run %q",
			ErrCrossRunIdentityUnavailable,
			runID,
		)
	}
	if latestFound {
		return latest, true, nil
	}
	return DeleteReconciliation{}, false, nil
}

func loadSQLiteStage4Record(
	transaction *sql.Tx,
	kind string,
	runID string,
	taskKey string,
	recordID string,
) (string, bool, error) {
	var payload string
	err := transaction.QueryRow(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ? AND record_id = ?
	`, kind, runID, taskKey, recordID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read %s state: %w", kind, err)
	}
	return payload, true, nil
}

func insertSQLiteStage4Record(
	transaction *sql.Tx,
	kind string,
	runID string,
	taskKey string,
	recordID string,
	payload string,
) (bool, string, error) {
	result, err := transaction.Exec(`
		INSERT INTO stage4_records(kind, run_id, task_key, record_id, payload)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(kind, run_id, task_key, record_id) DO NOTHING
	`, kind, runID, taskKey, recordID, payload)
	if err != nil {
		return false, "", fmt.Errorf("write %s state: %w", kind, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, "", fmt.Errorf("verify %s state write: %w", kind, err)
	}
	if inserted == 1 {
		return true, "", nil
	}
	existing, found, err := loadSQLiteStage4Record(transaction, kind, runID, taskKey, recordID)
	if err != nil {
		return false, "", err
	}
	if !found {
		return false, "", fmt.Errorf("%w: %s record disappeared during insert", ErrStateTransition, kind)
	}
	return false, existing, nil
}

func updateSQLiteStage4Record(
	transaction *sql.Tx,
	kind string,
	runID string,
	taskKey string,
	recordID string,
	previous string,
	next string,
) error {
	result, err := transaction.Exec(`
		UPDATE stage4_records SET payload = ?
		WHERE kind = ? AND run_id = ? AND task_key = ? AND record_id = ? AND payload = ?
	`, next, kind, runID, taskKey, recordID, previous)
	if err != nil {
		return fmt.Errorf("update %s state: %w", kind, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify %s state update: %w", kind, err)
	}
	if updated != 1 {
		return fmt.Errorf("%w: %s state changed concurrently", ErrStateTransition, kind)
	}
	return nil
}

func (store SQLiteStore) SaveSchemaSnapshot(snapshot SchemaSnapshot) error {
	snapshot, err := normalizeSchemaSnapshot(snapshot)
	if err != nil {
		return err
	}
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin schema snapshot: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, snapshot.RunID, snapshot.Task)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode schema snapshot: %w", err)
	}
	created, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4SchemaRecord,
		snapshot.RunID,
		taskKey,
		stage4SingletonRecordID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if !created {
		var existing SchemaSnapshot
		if err := json.Unmarshal([]byte(existingPayload), &existing); err != nil {
			return fmt.Errorf("decode schema snapshot: %w", err)
		}
		if !reflect.DeepEqual(existing, snapshot) {
			return fmt.Errorf("%w: schema snapshot", ErrImmutableEvidence)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit schema snapshot: %w", err)
	}
	return nil
}

func (store SQLiteStore) LoadSchemaSnapshot(runID string, task TaskKey) (SchemaSnapshot, bool, error) {
	var snapshot SchemaSnapshot
	found, err := store.loadStage4Record(
		stage4SchemaRecord,
		runID,
		task,
		stage4SingletonRecordID,
		&snapshot,
	)
	return snapshot, found, err
}

func (store SQLiteStore) LoadLatestApplicableSchemaSnapshot(
	runID string,
	task TaskKey,
) (SchemaSnapshot, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("begin applicable schema snapshot read: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, runID, task)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	sourceIdentity, targetIdentity, _, err := loadSQLiteRunIdentity(transaction, runID)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	rows, err := transaction.Query(`
		SELECT record.payload, record.run_id, run.source_identity, run.target_identity,
		       run.started_at
		FROM stage4_records AS record
		JOIN runs AS run ON run.id = record.run_id
		WHERE record.kind = ? AND record.task_key = ? AND run.outcome = ?
		  AND run.rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		  )
	`, stage4SchemaRecord, taskKey, Success)
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("query applicable schema snapshots: %w", err)
	}
	var latest SchemaSnapshot
	var found, identityUnavailable bool
	var sameRun SchemaSnapshot
	var sameRunFound bool
	var latestOrder, sameRunOrder stage4EvidenceOrder
	for rows.Next() {
		var payload, candidateRunID, candidateSourceIdentity, candidateTargetIdentity string
		var candidateRunStartedAt time.Time
		if err := rows.Scan(
			&payload,
			&candidateRunID,
			&candidateSourceIdentity,
			&candidateTargetIdentity,
			&candidateRunStartedAt,
		); err != nil {
			rows.Close()
			return SchemaSnapshot{}, false, fmt.Errorf("read applicable schema snapshot: %w", err)
		}
		var candidate SchemaSnapshot
		if err := json.Unmarshal([]byte(payload), &candidate); err != nil {
			rows.Close()
			return SchemaSnapshot{}, false, fmt.Errorf("decode applicable schema snapshot: %w", err)
		}
		candidateOrder := schemaEvidenceOrder(
			candidateRunStartedAt,
			candidate,
		)
		if candidateRunID == runID {
			if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
				sameRun, sameRunFound = candidate, true
				sameRunOrder = candidateOrder
			}
			continue
		}
		if sourceIdentity == "" || targetIdentity == "" ||
			candidateSourceIdentity == "" || candidateTargetIdentity == "" {
			identityUnavailable = true
			continue
		}
		if candidateSourceIdentity != sourceIdentity || candidateTargetIdentity != targetIdentity {
			continue
		}
		if !found || laterStage4Evidence(candidateOrder, latestOrder) {
			latest, found = candidate, true
			latestOrder = candidateOrder
		}
	}
	if err := finishSQLiteRows(
		rows,
		"iterate applicable schema snapshots",
		"close applicable schema snapshot query",
	); err != nil {
		return SchemaSnapshot{}, false, err
	}
	if sameRunFound {
		if err := transaction.Commit(); err != nil {
			return SchemaSnapshot{}, false, fmt.Errorf("commit applicable schema snapshot read: %w", err)
		}
		return sameRun, true, nil
	}
	if identityUnavailable {
		return SchemaSnapshot{}, false, fmt.Errorf(
			"%w: cannot select schema history for run %q",
			ErrCrossRunIdentityUnavailable,
			runID,
		)
	}
	if found {
		if err := transaction.Commit(); err != nil {
			return SchemaSnapshot{}, false, fmt.Errorf("commit applicable schema snapshot read: %w", err)
		}
		return latest, true, nil
	}
	if err := transaction.Commit(); err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("commit applicable schema snapshot read: %w", err)
	}
	return SchemaSnapshot{}, false, nil
}

func (store SQLiteStore) BeginIncrementalAttempt(attempt IncrementalAttempt) (IncrementalAttempt, bool, error) {
	attempt, err := normalizeIncrementalAttempt(attempt)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	sqliteStage4IncrementalBeginMu.Lock()
	defer sqliteStage4IncrementalBeginMu.Unlock()
	database, err := store.openStage4()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("begin incremental attempt: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, attempt.RunID, attempt.Task)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	existing, found, err := loadSQLiteIncrementalForTask(transaction, attempt.RunID, taskKey)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	if found {
		if !incrementalBeginMatches(existing, attempt) {
			return IncrementalAttempt{}, false, fmt.Errorf(
				"%w: one immutable incremental attempt already exists for this run and task",
				ErrImmutableEvidence,
			)
		}
		if err := transaction.Commit(); err != nil {
			return IncrementalAttempt{}, false, fmt.Errorf("commit incremental attempt replay: %w", err)
		}
		return existing, false, nil
	}
	if attempt.Mode == IncrementalWindow {
		latest, latestFound, err := loadSQLiteLatestCommittedIncremental(
			transaction,
			attempt.RunID,
			taskKey,
		)
		if err != nil {
			return IncrementalAttempt{}, false, err
		}
		if err := validateIncrementalLowerWatermark(attempt, latest, latestFound); err != nil {
			return IncrementalAttempt{}, false, err
		}
	}
	payload, err := json.Marshal(attempt)
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("encode incremental attempt: %w", err)
	}
	created, _, err := insertSQLiteStage4Record(
		transaction,
		stage4IncrementalRecord,
		attempt.RunID,
		taskKey,
		attempt.AttemptID,
		string(payload),
	)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	if !created {
		existing, found, loadErr := loadSQLiteIncrementalForTask(transaction, attempt.RunID, taskKey)
		if loadErr != nil {
			return IncrementalAttempt{}, false, loadErr
		}
		if !found || !incrementalBeginMatches(existing, attempt) {
			return IncrementalAttempt{}, false, fmt.Errorf("%w: concurrent incremental attempt", ErrImmutableEvidence)
		}
		attempt = existing
	}
	if err := transaction.Commit(); err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("commit incremental attempt: %w", err)
	}
	return attempt, created, nil
}

func (store SQLiteStore) LoadIncrementalAttempt(
	runID string,
	task TaskKey,
	attemptID string,
) (IncrementalAttempt, bool, error) {
	var attempt IncrementalAttempt
	found, err := store.loadStage4Record(
		stage4IncrementalRecord,
		runID,
		task,
		attemptID,
		&attempt,
	)
	return attempt, found, err
}

func (store SQLiteStore) LoadActiveIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("begin active incremental attempt read: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, runID, task)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	attempt, found, err := loadSQLiteIncrementalForTask(transaction, runID, taskKey)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	if found && attempt.Status != IncrementalRunning {
		found = false
		attempt = IncrementalAttempt{}
	}
	if err := transaction.Commit(); err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("commit active incremental attempt read: %w", err)
	}
	return attempt, found, nil
}

func (store SQLiteStore) LoadLatestCommittedIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("begin committed incremental attempt read: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, runID, task)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	attempt, found, err := loadSQLiteLatestCommittedIncremental(transaction, runID, taskKey)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	if err := transaction.Commit(); err != nil {
		return IncrementalAttempt{}, false, fmt.Errorf("commit committed incremental attempt read: %w", err)
	}
	return attempt, found, nil
}

func (store SQLiteStore) CommitIncrementalAttempt(commit IncrementalCommit) error {
	if err := validateStage4Identity(commit.RunID, commit.Task); err != nil {
		return err
	}
	if commit.AttemptID == "" {
		return fmt.Errorf("incremental attempt ID is required")
	}
	sqliteStage4IncrementalBeginMu.Lock()
	defer sqliteStage4IncrementalBeginMu.Unlock()
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin incremental completion: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, commit.RunID, commit.Task)
	if err != nil {
		return err
	}
	previous, found, err := loadSQLiteStage4Record(
		transaction,
		stage4IncrementalRecord,
		commit.RunID,
		taskKey,
		commit.AttemptID,
	)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: incremental attempt %q", ErrUnknownWork, commit.AttemptID)
	}
	var attempt IncrementalAttempt
	if err := json.Unmarshal([]byte(previous), &attempt); err != nil {
		return fmt.Errorf("decode incremental attempt: %w", err)
	}
	completed, err := applyIncrementalCommit(attempt, commit)
	if err != nil {
		return err
	}
	workTask, ranges, workFound, err := readSQLiteWorkPlan(transaction, commit.RunID, commit.Task)
	if err != nil {
		return err
	}
	if !workFound {
		return fmt.Errorf("%w: incremental aggregate work task", ErrUnknownWork)
	}
	if workTask.TopologyHash != commit.TopologyHash {
		return fmt.Errorf("%w: incremental aggregate work task", ErrTopologyChanged)
	}
	for _, workRange := range ranges {
		if workRange.TopologyHash != commit.TopologyHash || workRange.Status != "completed" {
			return fmt.Errorf("%w: incremental task has incomplete or stale ranges", ErrRangeOrder)
		}
	}
	switch {
	case attempt.Status == IncrementalRunning && workTask.Status == "running":
		workTask.Status = "completed"
		workTask.CompletedAt = commit.CompletedAt.UTC()
		workTask.UpdatedAt = commit.CompletedAt.UTC()
	case attempt.Status == IncrementalCompleted && workTask.Status == "completed":
		if !workTask.CompletedAt.Equal(commit.CompletedAt.UTC()) {
			return fmt.Errorf("%w: incremental aggregate completion differs", ErrImmutableEvidence)
		}
	default:
		return fmt.Errorf(
			"%w: incremental attempt %q and aggregate task %q are inconsistent",
			ErrStateTransition,
			attempt.Status,
			workTask.Status,
		)
	}
	next, err := json.Marshal(completed)
	if err != nil {
		return fmt.Errorf("encode incremental completion: %w", err)
	}
	if string(next) != previous {
		if err := updateSQLiteStage4Record(
			transaction,
			stage4IncrementalRecord,
			commit.RunID,
			taskKey,
			commit.AttemptID,
			previous,
			string(next),
		); err != nil {
			return err
		}
	}
	encodedWorkTask, err := json.Marshal(workTask)
	if err != nil {
		return fmt.Errorf("encode incremental aggregate work task: %w", err)
	}
	result, err := transaction.Exec(
		`UPDATE work_tasks SET payload = ? WHERE run_id = ? AND task_key = ?`,
		string(encodedWorkTask),
		commit.RunID,
		taskKey,
	)
	if err != nil {
		return fmt.Errorf("update incremental aggregate work task: %w", err)
	}
	if updated, rowsErr := result.RowsAffected(); rowsErr != nil || updated != 1 {
		if rowsErr != nil {
			return fmt.Errorf("verify incremental aggregate work task: %w", rowsErr)
		}
		return fmt.Errorf("%w: incremental aggregate work task changed", ErrStateTransition)
	}
	if err := stage4BeforeIncrementalCommit(); err != nil {
		return fmt.Errorf("prepare atomic incremental completion: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit incremental completion: %w", err)
	}
	return nil
}

func (store SQLiteStore) BeginDeleteReconciliation(record DeleteReconciliation) (DeleteReconciliation, bool, error) {
	record, err := normalizeDeleteReconciliation(record)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	database, err := store.openStage4()
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("begin delete reconciliation: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, record.RunID, record.Task)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("encode delete reconciliation: %w", err)
	}
	created, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4DeleteRecord,
		record.RunID,
		taskKey,
		record.AttemptID,
		string(payload),
	)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	stored := record
	if !created {
		if err := json.Unmarshal([]byte(existingPayload), &stored); err != nil {
			return DeleteReconciliation{}, false, fmt.Errorf("decode delete reconciliation: %w", err)
		}
		if !deleteReconciliationBeginMatches(stored, record) {
			return DeleteReconciliation{}, false, fmt.Errorf("%w: delete reconciliation", ErrImmutableEvidence)
		}
	}
	if err := transaction.Commit(); err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("commit delete reconciliation: %w", err)
	}
	return stored, created, nil
}

func (store SQLiteStore) LoadDeleteReconciliation(
	runID string,
	task TaskKey,
	attemptID string,
) (DeleteReconciliation, bool, error) {
	var record DeleteReconciliation
	found, err := store.loadStage4Record(
		stage4DeleteRecord,
		runID,
		task,
		attemptID,
		&record,
	)
	return record, found, err
}

func (store SQLiteStore) LoadLatestSuccessfulDeleteReconciliation(
	runID string,
	task TaskKey,
) (DeleteReconciliation, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("begin successful delete reconciliation read: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, runID, task)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	record, found, err := loadSQLiteLatestSuccessfulDelete(transaction, runID, taskKey)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	if err := transaction.Commit(); err != nil {
		return DeleteReconciliation{}, false, fmt.Errorf("commit successful delete reconciliation read: %w", err)
	}
	return record, found, nil
}

func (store SQLiteStore) FinishDeleteReconciliation(result DeleteReconciliationResult) error {
	if err := validateStage4Identity(result.RunID, result.Task); err != nil {
		return err
	}
	if result.AttemptID == "" {
		return fmt.Errorf("delete reconciliation attempt ID is required")
	}
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin delete reconciliation result: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, result.RunID, result.Task)
	if err != nil {
		return err
	}
	previous, found, err := loadSQLiteStage4Record(
		transaction,
		stage4DeleteRecord,
		result.RunID,
		taskKey,
		result.AttemptID,
	)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: delete reconciliation %q", ErrUnknownWork, result.AttemptID)
	}
	var record DeleteReconciliation
	if err := json.Unmarshal([]byte(previous), &record); err != nil {
		return fmt.Errorf("decode delete reconciliation: %w", err)
	}
	completed, err := applyDeleteReconciliationResult(record, result)
	if err != nil {
		return err
	}
	next, err := json.Marshal(completed)
	if err != nil {
		return fmt.Errorf("encode delete reconciliation result: %w", err)
	}
	if string(next) != previous {
		if err := updateSQLiteStage4Record(
			transaction,
			stage4DeleteRecord,
			result.RunID,
			taskKey,
			result.AttemptID,
			previous,
			string(next),
		); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit delete reconciliation result: %w", err)
	}
	return nil
}

func (store SQLiteStore) SaveStrictMigrationSnapshot(snapshot StrictMigrationSnapshot) error {
	snapshot, err := normalizeStrictMigrationSnapshot(snapshot)
	if err != nil {
		return err
	}
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin strict migration snapshot: %w", err)
	}
	defer transaction.Rollback()
	run, err := requireSQLiteRun(transaction, snapshot.RunID)
	if err != nil {
		return err
	}
	if err := requireStrictRunSourceEngine(run, snapshot.SourceEngine); err != nil {
		return err
	}
	rows, err := transaction.Query(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
	`, stage4StrictMigrationRecord, snapshot.RunID, stage4MigrationTaskKey)
	if err != nil {
		return fmt.Errorf("query strict migration snapshots: %w", err)
	}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return fmt.Errorf("read strict migration snapshot: %w", err)
		}
		var existing StrictMigrationSnapshot
		if err := json.Unmarshal([]byte(payload), &existing); err != nil {
			rows.Close()
			return fmt.Errorf("decode strict migration snapshot: %w", err)
		}
		if existing.SourceEngine != snapshot.SourceEngine {
			rows.Close()
			return fmt.Errorf("%w: strict migration source engine differs", ErrImmutableEvidence)
		}
		if existing.SourceEngine == "postgres" {
			if existing.ProcessEpoch == snapshot.ProcessEpoch &&
				!reflect.DeepEqual(existing, snapshot) {
				rows.Close()
				return fmt.Errorf(
					"%w: PostgreSQL process epoch already owns a different migration snapshot",
					ErrImmutableEvidence,
				)
			}
			if existing.SnapshotReference == snapshot.SnapshotReference &&
				existing.ProcessEpoch != snapshot.ProcessEpoch {
				rows.Close()
				return fmt.Errorf(
					"%w: PostgreSQL snapshot reference cannot cross process epochs",
					ErrImmutableEvidence,
				)
			}
		}
		if existing.SourceEngine == "mssql" &&
			(existing.EpochID != snapshot.EpochID || !reflect.DeepEqual(existing, snapshot)) {
			rows.Close()
			return fmt.Errorf(
				"%w: SQL Server migration snapshot must be reused for the entire run",
				ErrImmutableEvidence,
			)
		}
	}
	if err := finishSQLiteRows(
		rows,
		"iterate strict migration snapshots",
		"close strict migration snapshot query",
	); err != nil {
		return err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode strict migration snapshot: %w", err)
	}
	created, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4StrictMigrationRecord,
		snapshot.RunID,
		stage4MigrationTaskKey,
		snapshot.EpochID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if !created {
		var existing StrictMigrationSnapshot
		if err := json.Unmarshal([]byte(existingPayload), &existing); err != nil {
			return fmt.Errorf("decode strict migration snapshot: %w", err)
		}
		if !reflect.DeepEqual(existing, snapshot) {
			return fmt.Errorf("%w: strict migration snapshot", ErrImmutableEvidence)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit strict migration snapshot: %w", err)
	}
	return nil
}

func (store SQLiteStore) LoadStrictMigrationSnapshot(
	runID string,
	epochID string,
) (StrictMigrationSnapshot, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("begin strict migration snapshot read: %w", err)
	}
	defer transaction.Rollback()
	if _, err := requireSQLiteRun(transaction, runID); err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	payload, found, err := loadSQLiteStage4Record(
		transaction,
		stage4StrictMigrationRecord,
		runID,
		stage4MigrationTaskKey,
		epochID,
	)
	if err != nil || !found {
		return StrictMigrationSnapshot{}, found, err
	}
	var snapshot StrictMigrationSnapshot
	if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("decode strict migration snapshot: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("commit strict migration snapshot read: %w", err)
	}
	return snapshot, true, nil
}

func (store SQLiteStore) LoadLatestStrictMigrationSnapshot(
	runID string,
) (StrictMigrationSnapshot, bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("begin latest strict migration snapshot read: %w", err)
	}
	defer transaction.Rollback()
	if _, err := requireSQLiteRun(transaction, runID); err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	rows, err := transaction.Query(`
		SELECT payload FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
	`, stage4StrictMigrationRecord, runID, stage4MigrationTaskKey)
	if err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("query strict migration snapshots: %w", err)
	}
	var latest StrictMigrationSnapshot
	var found bool
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return StrictMigrationSnapshot{}, false, fmt.Errorf("read strict migration snapshot: %w", err)
		}
		var candidate StrictMigrationSnapshot
		if err := json.Unmarshal([]byte(payload), &candidate); err != nil {
			rows.Close()
			return StrictMigrationSnapshot{}, false, fmt.Errorf("decode strict migration snapshot: %w", err)
		}
		if !found || laterStrictMigrationSnapshot(candidate, latest) {
			latest, found = candidate, true
		}
	}
	if err := finishSQLiteRows(
		rows,
		"iterate strict migration snapshots",
		"close strict migration snapshot query",
	); err != nil {
		return StrictMigrationSnapshot{}, false, err
	}
	if err := transaction.Commit(); err != nil {
		return StrictMigrationSnapshot{}, false, fmt.Errorf("commit latest strict migration snapshot read: %w", err)
	}
	return latest, found, nil
}

func (store SQLiteStore) SaveStrictSnapshotEvidence(evidence StrictSnapshotEvidence) error {
	evidence, err := normalizeStrictSnapshotEvidence(evidence)
	if err != nil {
		return err
	}
	database, err := store.openStage4()
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin strict snapshot evidence: %w", err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, evidence.RunID, evidence.Task)
	if err != nil {
		return err
	}
	run, err := requireSQLiteRun(transaction, evidence.RunID)
	if err != nil {
		return err
	}
	if err := requireStrictRunSourceEngine(run, evidence.SourceEngine); err != nil {
		return err
	}
	if evidence.Scope == StrictSnapshotMigration {
		payload, found, err := loadSQLiteStage4Record(
			transaction,
			stage4StrictMigrationRecord,
			evidence.RunID,
			stage4MigrationTaskKey,
			evidence.MigrationEpochID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: strict migration snapshot %q", ErrUnknownWork, evidence.MigrationEpochID)
		}
		var owner StrictMigrationSnapshot
		if err := json.Unmarshal([]byte(payload), &owner); err != nil {
			return fmt.Errorf("decode strict migration snapshot: %w", err)
		}
		if owner.SourceEngine != evidence.SourceEngine ||
			owner.SnapshotReference != evidence.SnapshotReference ||
			owner.ProcessEpoch != evidence.ProcessEpoch {
			return fmt.Errorf("%w: strict table evidence differs from its migration snapshot", ErrImmutableEvidence)
		}
	}
	payload, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("encode strict snapshot evidence: %w", err)
	}
	created, existingPayload, err := insertSQLiteStage4Record(
		transaction,
		stage4StrictRecord,
		evidence.RunID,
		taskKey,
		evidence.AttemptID,
		string(payload),
	)
	if err != nil {
		return err
	}
	if !created {
		var existing StrictSnapshotEvidence
		if err := json.Unmarshal([]byte(existingPayload), &existing); err != nil {
			return fmt.Errorf("decode strict snapshot evidence: %w", err)
		}
		if !reflect.DeepEqual(existing, evidence) {
			return fmt.Errorf("%w: strict snapshot evidence", ErrImmutableEvidence)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit strict snapshot evidence: %w", err)
	}
	return nil
}

func (store SQLiteStore) LoadStrictSnapshotEvidence(
	runID string,
	task TaskKey,
	attemptID string,
) (StrictSnapshotEvidence, bool, error) {
	var evidence StrictSnapshotEvidence
	found, err := store.loadStage4Record(
		stage4StrictRecord,
		runID,
		task,
		attemptID,
		&evidence,
	)
	return evidence, found, err
}

func (store SQLiteStore) loadStage4Record(
	kind string,
	runID string,
	task TaskKey,
	recordID string,
	destination any,
) (bool, error) {
	database, err := store.openStage4()
	if err != nil {
		return false, err
	}
	defer database.Close()
	transaction, err := database.Begin()
	if err != nil {
		return false, fmt.Errorf("begin %s read: %w", kind, err)
	}
	defer transaction.Rollback()
	taskKey, err := requireSQLiteStage4Identity(transaction, runID, task)
	if err != nil {
		return false, err
	}
	payload, found, err := loadSQLiteStage4Record(transaction, kind, runID, taskKey, recordID)
	if err != nil || !found {
		return found, err
	}
	if err := json.Unmarshal([]byte(payload), destination); err != nil {
		return false, fmt.Errorf("decode %s state: %w", kind, err)
	}
	if err := transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit %s read: %w", kind, err)
	}
	return true, nil
}
