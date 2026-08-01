package state

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

// DeleteReconciliationReadScope is the workload identity a read-only caller
// must prove before it can reuse delete scheduling evidence. Delete state is
// per source table, and a state file can contain unrelated targets and runs;
// selecting its newest record globally is therefore never safe.
type DeleteReconciliationReadScope struct {
	SourceIdentity string
	TargetIdentity string
	Tasks          []TaskKey
}

// ScopedDeleteReconciliation contains the latest completed evidence for one
// requested table, if any. Results preserve the caller's task order.
type ScopedDeleteReconciliation struct {
	Task   TaskKey
	Record DeleteReconciliation
	Found  bool
}

// SchemaSnapshotReadScope identifies the exact completed workload whose
// aggregate source-schema evidence may be reused by an advisory reader.
// Unlike the stateful Stage4Backend method, it does not require the caller to
// invent a new run solely to inspect historical evidence.
type SchemaSnapshotReadScope struct {
	SourceIdentity string
	TargetIdentity string
	Task           TaskKey
}

// ReadOnlyLatestSuccessfulSchemaSnapshot returns the newest valid successful
// aggregate schema snapshot for one exact source/target workload. It never
// creates state, locks, directories, SQLite journals, or a synthetic run.
// Missing state is a legitimate absence of baseline; ambiguous or corrupted
// selected evidence fails closed.
func ReadOnlyLatestSuccessfulSchemaSnapshot(
	path string,
	scope SchemaSnapshotReadScope,
) (SchemaSnapshot, bool, error) {
	normalized, err := normalizeSchemaSnapshotReadScope(scope)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	if strings.TrimSpace(path) == "" {
		return SchemaSnapshot{}, false, errors.New("state path is required")
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return readOnlyYAMLSchemaSnapshot(path, normalized)
	default:
		return readOnlySQLiteSchemaSnapshot(path, normalized)
	}
}

// ReadOnlyLatestSuccessfulDeleteReconciliations returns the newest valid
// completed delete reconciliation for every requested workload task without
// creating a state file, lock file, directory, schema, or SQLite journal.
// Missing state and state created before Stage 4 both mean that no evidence is
// available. The source and target identities are mandatory so a history entry
// from another migration cannot make the current workload appear not due.
func ReadOnlyLatestSuccessfulDeleteReconciliations(
	path string,
	scope DeleteReconciliationReadScope,
) ([]ScopedDeleteReconciliation, error) {
	normalized, err := normalizeDeleteReconciliationReadScope(scope)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("state path is required")
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return readOnlyYAMLScopedDeleteReconciliationEvidence(path, normalized)
	default:
		return readOnlySQLiteScopedDeleteReconciliationEvidence(path, normalized)
	}
}

type readOnlySQLiteDeleteRecord struct {
	payload string
	runID   string
	taskKey string
}

type readOnlySchemaSnapshotRecord struct {
	snapshot      SchemaSnapshot
	payload       string
	runID         string
	storageScoped bool
}

func readOnlyYAMLScopedDeleteReconciliationEvidence(
	path string,
	scope DeleteReconciliationReadScope,
) ([]ScopedDeleteReconciliation, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return emptyScopedDeleteReconciliationEvidence(scope), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read YAML state: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return emptyScopedDeleteReconciliationEvidence(scope), nil
	}
	var document yamlStateDocument
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("decode YAML state: %w", err)
	}
	if document.Version < 0 || document.Version > yamlStateVersion {
		return nil, fmt.Errorf(
			"decode YAML state: unsupported version %d",
			document.Version,
		)
	}
	return scopedReadOnlyDeleteEvidence(
		document.DeleteReconciliations,
		document.Runs,
		scope,
	)
}

func readOnlyYAMLSchemaSnapshot(
	path string,
	scope SchemaSnapshotReadScope,
) (SchemaSnapshot, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return SchemaSnapshot{}, false, nil
	}
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("read YAML state: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return SchemaSnapshot{}, false, nil
	}
	var document yamlStateDocument
	if err := yaml.Unmarshal(data, &document); err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("decode YAML state: %w", err)
	}
	if document.Version < 0 || document.Version > yamlStateVersion {
		return SchemaSnapshot{}, false, fmt.Errorf(
			"decode YAML state: unsupported version %d",
			document.Version,
		)
	}
	return selectReadOnlySchemaSnapshot(
		readOnlySchemaSnapshotRecords(document.SchemaSnapshots),
		document.Runs,
		scope,
	)
}

// ReadOnlyLatestSuccessfulDeleteReconciliation returns the newest valid
// completed delete reconciliation in a state file without creating a state
// file, lock file, directory, schema, or SQLite journal. It is retained for
// callers that deliberately need an unscoped diagnostic view; migration and
// dry-run scheduling must use ReadOnlyLatestSuccessfulDeleteReconciliations.
func ReadOnlyLatestSuccessfulDeleteReconciliation(
	path string,
) (DeleteReconciliation, bool, error) {
	if strings.TrimSpace(path) == "" {
		return DeleteReconciliation{}, false, errors.New("state path is required")
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return readOnlyYAMLDeleteReconciliation(path)
	default:
		return readOnlySQLiteDeleteReconciliation(path)
	}
}

func readOnlyYAMLDeleteReconciliation(
	path string,
) (DeleteReconciliation, bool, error) {
	records, _, err := readOnlyYAMLDeleteReconciliationRecords(path)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	return latestReadOnlyDeleteEvidence(records)
}

func readOnlyYAMLDeleteReconciliationRecords(
	path string,
) ([]DeleteReconciliation, map[string]Run, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, map[string]Run{}, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read YAML state: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, map[string]Run{}, nil
	}
	var document yamlStateDocument
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, nil, fmt.Errorf("decode YAML state: %w", err)
	}
	if document.Version < 0 || document.Version > yamlStateVersion {
		return nil, nil, fmt.Errorf(
			"decode YAML state: unsupported version %d",
			document.Version,
		)
	}
	runs, err := readOnlyRunIndex(document.Runs)
	if err != nil {
		return nil, nil, fmt.Errorf("decode YAML run identities: %w", err)
	}
	return append([]DeleteReconciliation(nil), document.DeleteReconciliations...), runs, nil
}

func readOnlySQLiteDeleteReconciliation(
	path string,
) (DeleteReconciliation, bool, error) {
	records, _, err := readOnlySQLiteDeleteReconciliationRecords(path)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	return latestReadOnlyDeleteEvidence(records)
}

func readOnlySQLiteDeleteReconciliationRecords(
	path string,
) ([]DeleteReconciliation, map[string]Run, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"resolve SQLite state path: %w",
			err,
		)
	}
	info, err := os.Stat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		return nil, map[string]Run{}, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("inspect SQLite state: %w", err)
	}
	if info.IsDir() {
		return nil, nil, errors.New("SQLite state path is a directory")
	}
	database, err := sql.Open("sqlite", sqliteReadOnlyStateURI(absolute))
	if err != nil {
		return nil, nil, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		return nil, nil, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	var tableName string
	err = database.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'stage4_records'`,
	).Scan(&tableName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, map[string]Run{}, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf(
			"inspect SQLite Stage 4 state schema: %w",
			err,
		)
	}
	rows, err := database.Query(
		`SELECT payload FROM stage4_records WHERE kind = ?`,
		stage4DeleteRecord,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"read SQLite delete reconciliation evidence: %w",
			err,
		)
	}
	defer rows.Close()
	records := make([]DeleteReconciliation, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, nil, fmt.Errorf(
				"read SQLite delete reconciliation evidence: %w",
				err,
			)
		}
		var record DeleteReconciliation
		if err := json.Unmarshal([]byte(payload), &record); err != nil {
			return nil, nil, fmt.Errorf(
				"decode SQLite delete reconciliation evidence: %w",
				err,
			)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf(
			"iterate SQLite delete reconciliation evidence: %w",
			err,
		)
	}
	runs, err := readOnlySQLiteRunIndex(database)
	if err != nil {
		return nil, nil, err
	}
	return records, runs, nil
}

// readOnlySQLiteScopedDeleteReconciliationEvidence selects by canonical task
// key and workload identity in SQLite before decoding payloads. This prevents
// malformed records or run histories belonging to another workload from
// poisoning the selected dry-run schedule.
func readOnlySQLiteScopedDeleteReconciliationEvidence(
	path string,
	scope DeleteReconciliationReadScope,
) (result []ScopedDeleteReconciliation, resultErr error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite state path: %w", err)
	}
	info, err := os.Stat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		return emptyScopedDeleteReconciliationEvidence(scope), nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite state: %w", err)
	}
	if info.IsDir() {
		return nil, errors.New("SQLite state path is a directory")
	}
	before, err := snapshotSQLiteReadOnlyArtifacts(absolute)
	if err != nil {
		return nil, err
	}
	snapshot, cleanup, err := cloneSQLiteReadOnlySnapshot(absolute, before)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	database, err := sql.Open("sqlite", sqliteReadOnlyStateURI(snapshot))
	if err != nil {
		return nil, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	defer func() {
		closeErr := database.Close()
		after, artifactErr := snapshotSQLiteReadOnlyArtifacts(absolute)
		if artifactErr == nil {
			artifactErr = verifySQLiteReadOnlyArtifacts(before, after)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("close SQLite state read-only: %w", closeErr)
		}
		if artifactErr != nil {
			artifactErr = fmt.Errorf("verify SQLite state read-only artifacts: %w", artifactErr)
		}
		if closeErr != nil || artifactErr != nil {
			result = nil
			resultErr = errors.Join(resultErr, closeErr, artifactErr)
		}
	}()
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		return nil, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	if _, err := database.Exec(`PRAGMA query_only = ON`); err != nil {
		return nil, fmt.Errorf("configure SQLite state read-only: %w", err)
	}
	keys, err := canonicalScopedDeleteTaskKeys(scope.Tasks)
	if err != nil {
		return nil, err
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(keys)), ",")
	query := `SELECT payload, run_id, task_key FROM stage4_records WHERE kind = ? AND task_key IN (` + placeholders + `)`
	arguments := make([]any, 0, len(keys)+1)
	arguments = append(arguments, stage4DeleteRecord)
	for _, key := range keys {
		arguments = append(arguments, key)
	}
	rows, err := database.Query(query, arguments...)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return emptyScopedDeleteReconciliationEvidence(scope), nil
		}
		return nil, fmt.Errorf("read SQLite delete reconciliation evidence: %w", err)
	}
	defer rows.Close()
	rawRecords := make([]readOnlySQLiteDeleteRecord, 0)
	for rows.Next() {
		var record readOnlySQLiteDeleteRecord
		if err := rows.Scan(&record.payload, &record.runID, &record.taskKey); err != nil {
			return nil, fmt.Errorf("read SQLite delete reconciliation evidence: %w", err)
		}
		rawRecords = append(rawRecords, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite delete reconciliation evidence: %w", err)
	}
	wantedRuns := make(map[string]struct{}, len(rawRecords))
	for _, record := range rawRecords {
		wantedRuns[record.runID] = struct{}{}
	}
	runRecords, err := readOnlySQLiteScopedRunIndex(database, wantedRuns, scope)
	if err != nil {
		return nil, err
	}
	runs := latestScopedReadOnlyRuns(runRecords, wantedRuns)
	records := make([]DeleteReconciliation, 0, len(rawRecords))
	for _, raw := range rawRecords {
		_, found := runs[raw.runID]
		if !found {
			// The storage key proves this record's task, but only the durable run
			// identity proves its source and target. A nonmatching or legacy run
			// therefore contributes no scheduling evidence.
			continue
		}
		var record DeleteReconciliation
		if err := json.Unmarshal([]byte(raw.payload), &record); err != nil {
			return nil, fmt.Errorf("decode SQLite delete reconciliation evidence: %w", err)
		}
		key, keyErr := record.Task.canonical()
		if keyErr != nil || record.RunID != raw.runID || key != raw.taskKey {
			return nil, errors.New("SQLite delete reconciliation record identity does not match its storage key")
		}
		records = append(records, record)
	}
	result, err = scopedReadOnlyDeleteEvidence(records, runRecords, scope)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func readOnlySQLiteSchemaSnapshot(
	path string,
	scope SchemaSnapshotReadScope,
) (result SchemaSnapshot, found bool, resultErr error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf(
			"resolve SQLite state path: %w",
			err,
		)
	}
	info, err := os.Stat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		return SchemaSnapshot{}, false, nil
	}
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("inspect SQLite state: %w", err)
	}
	if info.IsDir() {
		return SchemaSnapshot{}, false, errors.New("SQLite state path is a directory")
	}
	before, err := snapshotSQLiteReadOnlyArtifacts(absolute)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	snapshot, cleanup, err := cloneSQLiteReadOnlySnapshot(absolute, before)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	defer cleanup()
	database, err := sql.Open("sqlite", sqliteReadOnlyStateURI(snapshot))
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	defer func() {
		closeErr := database.Close()
		after, artifactErr := snapshotSQLiteReadOnlyArtifacts(absolute)
		if artifactErr == nil {
			artifactErr = verifySQLiteReadOnlyArtifacts(before, after)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("close SQLite state read-only: %w", closeErr)
		}
		if artifactErr != nil {
			artifactErr = fmt.Errorf("verify SQLite state read-only artifacts: %w", artifactErr)
		}
		if closeErr != nil || artifactErr != nil {
			result = SchemaSnapshot{}
			found = false
			resultErr = errors.Join(resultErr, closeErr, artifactErr)
		}
	}()
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("open SQLite state read-only: %w", err)
	}
	if _, err := database.Exec(`PRAGMA query_only = ON`); err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("configure SQLite state read-only: %w", err)
	}
	taskKey, err := scope.Task.canonical()
	if err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("canonical schema snapshot task: %w", err)
	}
	rows, err := database.Query(
		`SELECT payload, run_id FROM stage4_records WHERE kind = ? AND task_key = ?`,
		stage4SchemaRecord,
		taskKey,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return SchemaSnapshot{}, false, nil
		}
		return SchemaSnapshot{}, false, fmt.Errorf("read SQLite schema snapshot evidence: %w", err)
	}
	defer rows.Close()
	snapshots := make([]readOnlySchemaSnapshotRecord, 0)
	runIDs := make(map[string]struct{})
	for rows.Next() {
		var payload, runID string
		if err := rows.Scan(&payload, &runID); err != nil {
			return SchemaSnapshot{}, false, fmt.Errorf("read SQLite schema snapshot evidence: %w", err)
		}
		snapshots = append(snapshots, readOnlySchemaSnapshotRecord{
			payload:       payload,
			runID:         runID,
			storageScoped: true,
		})
		runIDs[runID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return SchemaSnapshot{}, false, fmt.Errorf("iterate SQLite schema snapshot evidence: %w", err)
	}
	runs, err := readOnlySQLiteSchemaSnapshotRuns(database, runIDs)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	return selectReadOnlySchemaSnapshot(snapshots, runs, scope)
}

func readOnlySQLiteSchemaSnapshotRuns(
	database *sql.DB,
	ids map[string]struct{},
) ([]Run, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	placeholders := strings.TrimRight(strings.Repeat("?,", len(ordered)), ",")
	arguments := make([]any, len(ordered))
	for index := range ordered {
		arguments[index] = ordered[index]
	}
	rows, err := database.Query(`
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, reason, started_at, ended_at
		FROM runs AS run
		WHERE id IN (`+placeholders+`)
		  AND rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		  )`, arguments...)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return nil, nil
		}
		return nil, fmt.Errorf("read SQLite schema snapshot run identities: %w", err)
	}
	defer rows.Close()
	result := make([]Run, 0, len(ids))
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("read SQLite schema snapshot run identity: %w", scanErr)
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite schema snapshot run identities: %w", err)
	}
	return result, nil
}

func readOnlySchemaSnapshotRecords(
	snapshots []SchemaSnapshot,
) []readOnlySchemaSnapshotRecord {
	result := make([]readOnlySchemaSnapshotRecord, len(snapshots))
	for index := range snapshots {
		result[index] = readOnlySchemaSnapshotRecord{
			snapshot: snapshots[index],
			runID:    snapshots[index].RunID,
		}
	}
	return result
}

func selectReadOnlySchemaSnapshot(
	records []readOnlySchemaSnapshotRecord,
	runs []Run,
	scope SchemaSnapshotReadScope,
) (SchemaSnapshot, bool, error) {
	wantedRuns := make(map[string]struct{})
	for _, record := range records {
		if !record.storageScoped && record.snapshot.Task != scope.Task {
			continue
		}
		wantedRuns[record.runID] = struct{}{}
	}
	runIndex, err := readOnlySchemaSnapshotRunIndex(runs, wantedRuns)
	if err != nil {
		return SchemaSnapshot{}, false, err
	}
	var latest SchemaSnapshot
	var found bool
	var latestOrder stage4EvidenceOrder
	for _, record := range records {
		if !record.storageScoped && record.snapshot.Task != scope.Task {
			continue
		}
		run, runFound := runIndex[record.runID]
		if !runFound {
			return SchemaSnapshot{}, false, fmt.Errorf(
				"schema snapshot %q has no readable run identity",
				record.runID,
			)
		}
		if run.Outcome != Success {
			continue
		}
		if run.SourceIdentity != scope.SourceIdentity ||
			run.TargetIdentity != scope.TargetIdentity {
			if run.SourceIdentity != "" && run.TargetIdentity != "" {
				continue
			}
			return SchemaSnapshot{}, false, fmt.Errorf(
				"%w: cannot select schema history for run %q",
				ErrCrossRunIdentityUnavailable,
				run.ID,
			)
		}
		if err := validateRunRecord(run); err != nil {
			return SchemaSnapshot{}, false, fmt.Errorf(
				"schema snapshot run %q: %w",
				run.ID,
				err,
			)
		}
		candidate := record.snapshot
		if record.payload != "" {
			if err := json.Unmarshal([]byte(record.payload), &candidate); err != nil {
				return SchemaSnapshot{}, false, fmt.Errorf(
					"decode SQLite schema snapshot evidence: %w",
					err,
				)
			}
		}
		if candidate.RunID != record.runID || candidate.Task != scope.Task {
			return SchemaSnapshot{}, false, errors.New(
				"schema snapshot identity does not match its storage scope",
			)
		}
		candidate, err = normalizeSchemaSnapshot(candidate)
		if err != nil {
			return SchemaSnapshot{}, false, fmt.Errorf(
				"validate applicable schema snapshot: %w",
				err,
			)
		}
		order := schemaEvidenceOrder(run.StartedAt, candidate)
		if !found || laterStage4Evidence(order, latestOrder) {
			latest, found, latestOrder = candidate, true, order
		}
	}
	return latest, found, nil
}

func readOnlySchemaSnapshotRunIndex(
	runs []Run,
	wanted map[string]struct{},
) (map[string]Run, error) {
	result := make(map[string]Run, len(wanted))
	for _, candidate := range runs {
		if _, selected := wanted[candidate.ID]; !selected {
			continue
		}
		if strings.TrimSpace(candidate.ID) == "" {
			return nil, errors.New("schema snapshot run identity is missing an ID")
		}
		current, found := result[candidate.ID]
		if !found || candidate.StartedAt.After(current.StartedAt) ||
			candidate.StartedAt.Equal(current.StartedAt) {
			result[candidate.ID] = candidate
		}
	}
	return result, nil
}

func normalizeSchemaSnapshotReadScope(
	scope SchemaSnapshotReadScope,
) (SchemaSnapshotReadScope, error) {
	if strings.TrimSpace(scope.SourceIdentity) == "" ||
		strings.TrimSpace(scope.TargetIdentity) == "" {
		return SchemaSnapshotReadScope{}, errors.New(
			"schema snapshot read scope requires source and target identities",
		)
	}
	if err := scope.Task.Validate(); err != nil {
		return SchemaSnapshotReadScope{}, fmt.Errorf(
			"schema snapshot read scope task: %w",
			err,
		)
	}
	return scope, nil
}

func canonicalScopedDeleteTaskKeys(tasks []TaskKey) ([]string, error) {
	keys := make([]string, len(tasks))
	for index, task := range tasks {
		key, err := task.canonical()
		if err != nil {
			return nil, fmt.Errorf("canonical delete reconciliation task: %w", err)
		}
		keys[index] = key
	}
	return keys, nil
}

func readOnlySQLiteScopedRunIndex(
	database *sql.DB,
	wanted map[string]struct{},
	scope DeleteReconciliationReadScope,
) ([]Run, error) {
	if len(wanted) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(wanted))
	for id := range wanted {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	query := `
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, reason, started_at, ended_at
		FROM runs AS run
		WHERE id IN (` + placeholders + `)
		  AND source_identity = ? AND target_identity = ?
		  AND rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		  )`
	arguments := make([]any, 0, len(ids)+2)
	for _, id := range ids {
		arguments = append(arguments, id)
	}
	arguments = append(arguments, scope.SourceIdentity, scope.TargetIdentity)
	rows, err := database.Query(query, arguments...)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return nil, nil
		}
		return nil, fmt.Errorf("read SQLite run identities: %w", err)
	}
	defer rows.Close()
	runs := make([]Run, 0, len(ids))
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("read SQLite run identity: %w", scanErr)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite run identities: %w", err)
	}
	return runs, nil
}

func normalizeDeleteReconciliationReadScope(
	scope DeleteReconciliationReadScope,
) (DeleteReconciliationReadScope, error) {
	if strings.TrimSpace(scope.SourceIdentity) == "" ||
		strings.TrimSpace(scope.TargetIdentity) == "" {
		return DeleteReconciliationReadScope{}, errors.New(
			"delete reconciliation read scope requires source and target identities",
		)
	}
	if len(scope.Tasks) == 0 {
		return DeleteReconciliationReadScope{}, errors.New(
			"delete reconciliation read scope requires at least one task",
		)
	}
	result := DeleteReconciliationReadScope{
		SourceIdentity: scope.SourceIdentity,
		TargetIdentity: scope.TargetIdentity,
		Tasks:          make([]TaskKey, len(scope.Tasks)),
	}
	seen := make(map[TaskKey]struct{}, len(scope.Tasks))
	for index, task := range scope.Tasks {
		if err := task.Validate(); err != nil {
			return DeleteReconciliationReadScope{}, fmt.Errorf(
				"delete reconciliation read scope task %d: %w",
				index,
				err,
			)
		}
		if _, duplicate := seen[task]; duplicate {
			return DeleteReconciliationReadScope{}, fmt.Errorf(
				"delete reconciliation read scope duplicates task %s.%s",
				task.Schema,
				task.Table,
			)
		}
		seen[task] = struct{}{}
		result.Tasks[index] = task
	}
	return result, nil
}

func readOnlyRunIndex(records []Run) (map[string]Run, error) {
	runs := make(map[string]Run, len(records))
	for _, candidate := range records {
		if strings.TrimSpace(candidate.ID) == "" {
			return nil, errors.New("run identity is missing an ID")
		}
		if err := validateRunRecord(candidate); err != nil {
			return nil, fmt.Errorf("run %q: %w", candidate.ID, err)
		}
		current, found := runs[candidate.ID]
		if found {
			var err error
			candidate, err = inheritRunWorkloadIdentity(current, candidate)
			if err != nil {
				return nil, err
			}
		}
		if !found || candidate.StartedAt.After(current.StartedAt) ||
			candidate.StartedAt.Equal(current.StartedAt) {
			runs[candidate.ID] = candidate
		}
	}
	return runs, nil
}

func readOnlySQLiteRunIndex(database *sql.DB) (map[string]Run, error) {
	rows, err := database.Query(`
		SELECT id, source, target, source_engine, source_identity, target_identity,
		       lease_target, lease_owner_token, lease_generation,
		       outcome, resumable, reason, started_at, ended_at
		FROM runs AS run
		WHERE rowid = (
			SELECT latest.rowid FROM runs AS latest
			WHERE latest.id = run.id
			ORDER BY latest.started_at DESC, latest.rowid DESC LIMIT 1
		)
	`)
	if err != nil {
		// A pre-Stage 4 state file can legitimately lack runs. Its delete
		// records cannot be scoped safely, so it contributes no evidence.
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return map[string]Run{}, nil
		}
		return nil, fmt.Errorf("read SQLite run identities: %w", err)
	}
	defer rows.Close()
	result := make(map[string]Run)
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("read SQLite run identity: %w", scanErr)
		}
		if err := validateRunRecord(run); err != nil {
			return nil, fmt.Errorf("read SQLite run identity %q: %w", run.ID, err)
		}
		result[run.ID] = run
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite run identities: %w", err)
	}
	return result, nil
}

func scopedReadOnlyDeleteEvidence(
	records []DeleteReconciliation,
	runRecords []Run,
	scope DeleteReconciliationReadScope,
) ([]ScopedDeleteReconciliation, error) {
	result := make([]ScopedDeleteReconciliation, len(scope.Tasks))
	indexes := make(map[TaskKey]int, len(scope.Tasks))
	for index, task := range scope.Tasks {
		result[index].Task = task
		indexes[task] = index
	}
	// Only runs referenced by selected, completed table evidence participate in
	// validation. A state file may contain malformed histories for other
	// workloads; they cannot affect this schedule and therefore must not make a
	// scoped, read-only query fail.
	wantedRuns := make(map[string]struct{})
	for _, record := range records {
		if _, selected := indexes[record.Task]; selected &&
			record.Status == DeleteReconciliationCompleted {
			wantedRuns[record.RunID] = struct{}{}
		}
	}
	runs := latestScopedReadOnlyRuns(runRecords, wantedRuns)
	orders := make([]stage4EvidenceOrder, len(scope.Tasks))
	for _, record := range records {
		index, selected := indexes[record.Task]
		if !selected || record.Status != DeleteReconciliationCompleted {
			continue
		}
		run, found := runs[record.RunID]
		if !found {
			// A run for another source/target is unrelated to this workload.
			// If no readable matching run exists at all, selected evidence cannot
			// be trusted and the read must fail closed.
			if hasReadOnlyRunID(runRecords, record.RunID) {
				continue
			}
			return nil, fmt.Errorf("completed delete reconciliation %q has no readable run identity", record.AttemptID)
		}
		if run.SourceIdentity == "" || run.TargetIdentity == "" {
			return nil, fmt.Errorf(
				"completed delete reconciliation %q has no workload identity",
				record.AttemptID,
			)
		}
		if run.SourceIdentity != scope.SourceIdentity ||
			run.TargetIdentity != scope.TargetIdentity {
			continue
		}
		if err := validateRunRecord(run); err != nil {
			return nil, fmt.Errorf("read delete reconciliation run identity %q: %w", run.ID, err)
		}
		if run.Outcome != Success {
			continue
		}
		if err := ValidateDeleteReconciliationEvidence(record); err != nil {
			return nil, fmt.Errorf(
				"validate completed delete reconciliation evidence: %w",
				err,
			)
		}
		order := deleteEvidenceOrder(run.StartedAt, record)
		if !result[index].Found || laterStage4Evidence(order, orders[index]) {
			result[index].Record = record
			result[index].Found = true
			orders[index] = order
		}
	}
	return result, nil
}

func emptyScopedDeleteReconciliationEvidence(
	scope DeleteReconciliationReadScope,
) []ScopedDeleteReconciliation {
	result := make([]ScopedDeleteReconciliation, len(scope.Tasks))
	for index, task := range scope.Tasks {
		result[index].Task = task
	}
	return result
}

func latestScopedReadOnlyRuns(
	runRecords []Run,
	wanted map[string]struct{},
) map[string]Run {
	runs := make(map[string]Run, len(wanted))
	for _, candidate := range runRecords {
		if _, selected := wanted[candidate.ID]; !selected {
			continue
		}
		current, found := runs[candidate.ID]
		if !found || candidate.StartedAt.After(current.StartedAt) ||
			candidate.StartedAt.Equal(current.StartedAt) {
			runs[candidate.ID] = candidate
		}
	}
	return runs
}

func hasReadOnlyRunID(runs []Run, id string) bool {
	for _, run := range runs {
		if run.ID == id {
			return true
		}
	}
	return false
}

func latestReadOnlyDeleteEvidence(
	records []DeleteReconciliation,
) (DeleteReconciliation, bool, error) {
	valid := make([]DeleteReconciliation, 0, len(records))
	for _, record := range records {
		if record.Status != DeleteReconciliationCompleted {
			continue
		}
		if err := ValidateDeleteReconciliationEvidence(record); err != nil {
			return DeleteReconciliation{}, false, fmt.Errorf(
				"validate completed delete reconciliation evidence: %w",
				err,
			)
		}
		valid = append(valid, record)
	}
	if len(valid) == 0 {
		return DeleteReconciliation{}, false, nil
	}
	sort.SliceStable(valid, func(left, right int) bool {
		if !valid[left].CompletedAt.Equal(valid[right].CompletedAt) {
			return valid[left].CompletedAt.Before(valid[right].CompletedAt)
		}
		if !valid[left].StartedAt.Equal(valid[right].StartedAt) {
			return valid[left].StartedAt.Before(valid[right].StartedAt)
		}
		if valid[left].RunID != valid[right].RunID {
			return valid[left].RunID < valid[right].RunID
		}
		return valid[left].AttemptID < valid[right].AttemptID
	})
	return valid[len(valid)-1], true, nil
}

func sqliteReadOnlyStateURI(path string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{Scheme: "file", Path: normalized}
	query := location.Query()
	query.Set("mode", "ro")
	location.RawQuery = query.Encode()
	return location.String()
}

type sqliteReadOnlyArtifact struct {
	exists   bool
	size     int64
	modified int64
	identity os.FileInfo
	digest   [sha256.Size]byte
}

func snapshotSQLiteReadOnlyArtifacts(
	path string,
) (map[string]sqliteReadOnlyArtifact, error) {
	result := make(map[string]sqliteReadOnlyArtifact, 5)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		info, err := os.Stat(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			result[suffix] = sqliteReadOnlyArtifact{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect SQLite state %s artifact: %w", suffix, err)
		}
		digest, digestErr := digestSQLiteReadOnlyArtifact(path+suffix, info)
		if digestErr != nil {
			return nil, digestErr
		}
		result[suffix] = sqliteReadOnlyArtifact{exists: true, size: info.Size(), modified: info.ModTime().UnixNano(), identity: info, digest: digest}
	}
	return result, nil
}

func verifySQLiteReadOnlyArtifacts(
	before, after map[string]sqliteReadOnlyArtifact,
) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		left, right := before[suffix], after[suffix]
		if left.exists != right.exists || left.size != right.size ||
			left.modified != right.modified || left.digest != right.digest ||
			(left.exists && !os.SameFile(left.identity, right.identity)) {
			return fmt.Errorf("SQLite state %s artifact changed", suffix)
		}
	}
	return nil
}

func digestSQLiteReadOnlyArtifact(
	path string,
	expected os.FileInfo,
) ([sha256.Size]byte, error) {
	input, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open SQLite state artifact: %w", err)
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("inspect opened SQLite state artifact: %w", err)
	}
	if !os.SameFile(expected, opened) || expected.Size() != opened.Size() ||
		expected.ModTime() != opened.ModTime() {
		return [sha256.Size]byte{}, errors.New("SQLite state artifact changed while snapshotting")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, input); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("digest SQLite state artifact: %w", err)
	}
	current, err := os.Stat(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("reinspect SQLite state artifact: %w", err)
	}
	if !os.SameFile(opened, current) || opened.Size() != current.Size() ||
		opened.ModTime() != current.ModTime() {
		return [sha256.Size]byte{}, errors.New("SQLite state artifact changed while snapshotting")
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

// cloneSQLiteReadOnlySnapshot keeps SQLite's WAL bookkeeping away from the
// real state path. A mode=ro connection may still update a WAL index with some
// drivers; opening a private copy preserves WAL visibility while the before
// and after source snapshots prove the original database, sidecars, and lock
// name remained untouched. If a writer changes the source while it is copied,
// the read fails closed rather than interpreting a torn snapshot.
func cloneSQLiteReadOnlySnapshot(
	path string,
	before map[string]sqliteReadOnlyArtifact,
) (string, func(), error) {
	directory, err := os.MkdirTemp("", "dmtx-state-readonly-")
	if err != nil {
		return "", nil, fmt.Errorf("create SQLite read-only snapshot: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	snapshot := filepath.Join(directory, "state.db")
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if !before[suffix].exists {
			continue
		}
		if err := copySQLiteReadOnlyArtifact(path+suffix, snapshot+suffix); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	after, err := snapshotSQLiteReadOnlyArtifacts(path)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if err := verifySQLiteReadOnlyArtifacts(before, after); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("SQLite state changed while capturing read-only snapshot: %w", err)
	}
	return snapshot, cleanup, nil
}

func copySQLiteReadOnlyArtifact(source, target string) error {
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("read SQLite state snapshot artifact: %w", err)
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create SQLite state snapshot artifact: %w", err)
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		return fmt.Errorf("copy SQLite state snapshot artifact: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("close SQLite state snapshot artifact: %w", err)
	}
	return nil
}
