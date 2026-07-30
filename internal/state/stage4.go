package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

var (
	// ErrImmutableEvidence reports an attempt to replace durable evidence that
	// resume correctness requires to remain unchanged.
	ErrImmutableEvidence = errors.New("immutable state evidence differs")
	// ErrStateTransition reports a transition that could expose an incomplete
	// or contradictory Stage 4 checkpoint.
	ErrStateTransition = errors.New("invalid state transition")
	// ErrCrossRunIdentityUnavailable prevents reuse of evidence when legacy
	// state lacks the credential-free canonical endpoint identities needed to
	// distinguish equal database names on different servers.
	ErrCrossRunIdentityUnavailable = errors.New("cross-run workload identity unavailable")
	// ErrRunSourceEngineUnavailable prevents strict snapshot evidence from
	// being attached to a legacy run whose immutable source engine is absent.
	ErrRunSourceEngineUnavailable = errors.New("run source engine identity unavailable")
)

// Stage4Backend is the additive restartability surface for schema contracts,
// incremental windows, delete convergence, and strict source snapshots.
//
// Backend remains unchanged so existing integrations keep compiling. Both
// built-in stores, and the lease-fenced wrapper returned by FenceBackend,
// implement this interface.
type Stage4Backend interface {
	SaveSchemaSnapshot(SchemaSnapshot) error
	LoadSchemaSnapshot(string, TaskKey) (SchemaSnapshot, bool, error)
	LoadLatestApplicableSchemaSnapshot(string, TaskKey) (SchemaSnapshot, bool, error)

	BeginIncrementalAttempt(IncrementalAttempt) (IncrementalAttempt, bool, error)
	LoadIncrementalAttempt(string, TaskKey, string) (IncrementalAttempt, bool, error)
	LoadActiveIncrementalAttempt(string, TaskKey) (IncrementalAttempt, bool, error)
	LoadLatestCommittedIncrementalAttempt(string, TaskKey) (IncrementalAttempt, bool, error)
	CommitIncrementalAttempt(IncrementalCommit) error

	BeginDeleteReconciliation(DeleteReconciliation) (DeleteReconciliation, bool, error)
	LoadDeleteReconciliation(string, TaskKey, string) (DeleteReconciliation, bool, error)
	LoadLatestSuccessfulDeleteReconciliation(string, TaskKey) (DeleteReconciliation, bool, error)
	FinishDeleteReconciliation(DeleteReconciliationResult) error

	SaveStrictMigrationSnapshot(StrictMigrationSnapshot) error
	LoadStrictMigrationSnapshot(string, string) (StrictMigrationSnapshot, bool, error)
	LoadLatestStrictMigrationSnapshot(string) (StrictMigrationSnapshot, bool, error)
	SaveStrictSnapshotEvidence(StrictSnapshotEvidence) error
	LoadStrictSnapshotEvidence(string, TaskKey, string) (StrictSnapshotEvidence, bool, error)
}

var (
	_ Stage4Backend = SQLiteStore{}
	_ Stage4Backend = YAMLStore{}
)

// SchemaSnapshot is the deterministic canonical schema observed for one
// structured table task. CanonicalJSON contains data only, never executable
// catalog SQL. Digest is the lowercase SHA-256 digest of CanonicalJSON.
type SchemaSnapshot struct {
	RunID         string    `json:"run_id" yaml:"run_id"`
	Task          TaskKey   `json:"task" yaml:"task"`
	CanonicalJSON string    `json:"canonical_json" yaml:"canonical_json"`
	Digest        string    `json:"digest" yaml:"digest"`
	CapturedAt    time.Time `json:"captured_at" yaml:"captured_at"`
}

// TimestampWatermark preserves a selected source timestamp and its column
// identity without converting through an untyped serialization.
type TimestampWatermark struct {
	Column string    `json:"column" yaml:"column"`
	Value  time.Time `json:"value" yaml:"value"`
}

type IncrementalStatus string

const (
	IncrementalRunning   IncrementalStatus = "running"
	IncrementalCompleted IncrementalStatus = "completed"
)

type IncrementalAttemptMode string

const (
	IncrementalBaseline IncrementalAttemptMode = "baseline"
	IncrementalWindow   IncrementalAttemptMode = "incremental"
)

// IncrementalAttempt durably binds a table attempt to its lower watermark and
// immutable sampled upper fence. Completion stores the new safe watermark and
// aggregate table-success evidence in the same atomic record replacement.
type IncrementalAttempt struct {
	RunID              string                 `json:"run_id" yaml:"run_id"`
	Task               TaskKey                `json:"task" yaml:"task"`
	AttemptID          string                 `json:"attempt_id" yaml:"attempt_id"`
	Mode               IncrementalAttemptMode `json:"mode" yaml:"mode"`
	LowerWatermark     *TimestampWatermark    `json:"lower_watermark,omitempty" yaml:"lower_watermark,omitempty"`
	UpperFence         *TimestampWatermark    `json:"upper_fence,omitempty" yaml:"upper_fence,omitempty"`
	Status             IncrementalStatus      `json:"status" yaml:"status"`
	CommittedWatermark *TimestampWatermark    `json:"committed_watermark,omitempty" yaml:"committed_watermark,omitempty"`
	TableSucceeded     bool                   `json:"table_succeeded" yaml:"table_succeeded"`
	StartedAt          time.Time              `json:"started_at" yaml:"started_at"`
	CompletedAt        time.Time              `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
}

// IncrementalCommit atomically completes the attempt and publishes its safe
// source-derived watermark. A nil watermark is valid only when the attempt had
// no lower watermark, such as a baseline table containing no non-NULL values.
type IncrementalCommit struct {
	RunID        string
	Task         TaskKey
	AttemptID    string
	TopologyHash string
	Watermark    *TimestampWatermark
	CompletedAt  time.Time
}

type DeleteReconciliationStatus string

const (
	DeleteReconciliationRunning    DeleteReconciliationStatus = "running"
	DeleteReconciliationNotDue     DeleteReconciliationStatus = "not_due"
	DeleteReconciliationCompleted  DeleteReconciliationStatus = "completed"
	DeleteReconciliationIncomplete DeleteReconciliationStatus = "incomplete"
	DeleteReconciliationDryRun     DeleteReconciliationStatus = "dry_run"
)

// DeleteReconciliation distinguishes a reconciliation that was not due from a
// due pass that ran and found zero candidates. Terminal records are immutable,
// so incomplete work can never be mistaken for the durable last success.
type DeleteReconciliation struct {
	RunID       string                     `json:"run_id" yaml:"run_id"`
	Task        TaskKey                    `json:"task" yaml:"task"`
	AttemptID   string                     `json:"attempt_id" yaml:"attempt_id"`
	Due         bool                       `json:"due" yaml:"due"`
	DryRun      bool                       `json:"dry_run" yaml:"dry_run"`
	Status      DeleteReconciliationStatus `json:"status" yaml:"status"`
	Candidates  int64                      `json:"candidates" yaml:"candidates"`
	DeletedRows int64                      `json:"deleted_rows" yaml:"deleted_rows"`
	SkippedRows int64                      `json:"skipped_rows" yaml:"skipped_rows"`
	Reason      string                     `json:"reason,omitempty" yaml:"reason,omitempty"`
	StartedAt   time.Time                  `json:"started_at" yaml:"started_at"`
	CompletedAt time.Time                  `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
}

// DeleteReconciliationResult completes one due reconciliation. Completed,
// incomplete, and dry-run are distinct durable outcomes.
type DeleteReconciliationResult struct {
	RunID       string
	Task        TaskKey
	AttemptID   string
	Status      DeleteReconciliationStatus
	Candidates  int64
	DeletedRows int64
	SkippedRows int64
	Reason      string
	CompletedAt time.Time
}

// laterDeleteReconciliation reports whether candidate is the deterministic
// successor to current within one run/task. Completion time is the durable
// ordering boundary; attempt ID makes equal timestamps stable across backends.
type stage4EvidenceOrder struct {
	runStartedAt time.Time
	runID        string
	recordAt     time.Time
	recordID     string
}

// laterStage4Evidence is the backend-independent total ordering for reusable
// cross-run evidence. Storage insertion order and SQLite rowid are never part
// of the correctness decision.
func laterStage4Evidence(candidate, current stage4EvidenceOrder) bool {
	if !candidate.runStartedAt.Equal(current.runStartedAt) {
		return candidate.runStartedAt.After(current.runStartedAt)
	}
	if candidate.runID != current.runID {
		return candidate.runID > current.runID
	}
	if !candidate.recordAt.Equal(current.recordAt) {
		return candidate.recordAt.After(current.recordAt)
	}
	return candidate.recordID > current.recordID
}

func schemaEvidenceOrder(runStartedAt time.Time, snapshot SchemaSnapshot) stage4EvidenceOrder {
	return stage4EvidenceOrder{
		runStartedAt: runStartedAt,
		runID:        snapshot.RunID,
		recordAt:     snapshot.CapturedAt,
		recordID:     snapshot.Digest,
	}
}

func incrementalEvidenceOrder(runStartedAt time.Time, attempt IncrementalAttempt) stage4EvidenceOrder {
	return stage4EvidenceOrder{
		runStartedAt: runStartedAt,
		runID:        attempt.RunID,
		recordAt:     attempt.CompletedAt,
		recordID:     attempt.AttemptID,
	}
}

func deleteEvidenceOrder(runStartedAt time.Time, record DeleteReconciliation) stage4EvidenceOrder {
	return stage4EvidenceOrder{
		runStartedAt: runStartedAt,
		runID:        record.RunID,
		recordAt:     record.CompletedAt,
		recordID:     record.AttemptID,
	}
}

func laterStrictMigrationSnapshot(candidate, current StrictMigrationSnapshot) bool {
	if !candidate.CapturedAt.Equal(current.CapturedAt) {
		return candidate.CapturedAt.After(current.CapturedAt)
	}
	return candidate.EpochID > current.EpochID
}

type StrictSnapshotScope string

const (
	StrictSnapshotTable     StrictSnapshotScope = "table"
	StrictSnapshotMigration StrictSnapshotScope = "migration"
)

// StrictMigrationSnapshot owns one migration-scoped source view. Per-table
// count evidence refers to EpochID so every task is provably read from the
// same engine snapshot and process epoch.
type StrictMigrationSnapshot struct {
	RunID             string    `json:"run_id" yaml:"run_id"`
	EpochID           string    `json:"epoch_id" yaml:"epoch_id"`
	SourceEngine      string    `json:"source_engine" yaml:"source_engine"`
	SnapshotReference string    `json:"snapshot_reference" yaml:"snapshot_reference"`
	ProcessEpoch      string    `json:"process_epoch" yaml:"process_epoch"`
	CapturedAt        time.Time `json:"captured_at" yaml:"captured_at"`
}

// StrictSnapshotEvidence is the immutable source-view evidence used by resume
// and validation. SnapshotReference is an engine-owned opaque identifier;
// ProcessEpoch makes PostgreSQL's new-process boundary explicit.
type StrictSnapshotEvidence struct {
	RunID               string              `json:"run_id" yaml:"run_id"`
	Task                TaskKey             `json:"task" yaml:"task"`
	AttemptID           string              `json:"attempt_id" yaml:"attempt_id"`
	SourceEngine        string              `json:"source_engine" yaml:"source_engine"`
	Scope               StrictSnapshotScope `json:"scope" yaml:"scope"`
	MigrationEpochID    string              `json:"migration_epoch_id,omitempty" yaml:"migration_epoch_id,omitempty"`
	SnapshotReference   string              `json:"snapshot_reference" yaml:"snapshot_reference"`
	ProcessEpoch        string              `json:"process_epoch" yaml:"process_epoch"`
	ExactSourceRowCount int64               `json:"exact_source_row_count" yaml:"exact_source_row_count"`
	CapturedAt          time.Time           `json:"captured_at" yaml:"captured_at"`
}

func normalizeSchemaSnapshot(snapshot SchemaSnapshot) (SchemaSnapshot, error) {
	if err := validateStage4Identity(snapshot.RunID, snapshot.Task); err != nil {
		return SchemaSnapshot{}, err
	}
	if snapshot.CapturedAt.IsZero() {
		return SchemaSnapshot{}, fmt.Errorf("schema snapshot capture time is required")
	}
	canonical, err := canonicalJSON(snapshot.CanonicalJSON)
	if err != nil {
		return SchemaSnapshot{}, fmt.Errorf("canonical schema snapshot: %w", err)
	}
	digestBytes := sha256.Sum256([]byte(canonical))
	digest := hex.EncodeToString(digestBytes[:])
	if snapshot.Digest != "" && !strings.EqualFold(snapshot.Digest, digest) {
		return SchemaSnapshot{}, fmt.Errorf("schema snapshot digest does not match canonical payload")
	}
	snapshot.CanonicalJSON = canonical
	snapshot.Digest = digest
	snapshot.CapturedAt = snapshot.CapturedAt.UTC()
	return snapshot, nil
}

func canonicalJSON(encoded string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", fmt.Errorf("multiple JSON values")
		}
		return "", err
	}
	// Canonical schema bytes are owned by internal/schema. Re-marshalling an
	// opaque snapshot here would impose a second field-ordering contract and
	// invalidate the schema package's digest.
	return encoded, nil
}

func normalizeTimestamp(value TimestampWatermark) (TimestampWatermark, error) {
	if strings.TrimSpace(value.Column) == "" {
		return TimestampWatermark{}, fmt.Errorf("watermark column is required")
	}
	if value.Value.IsZero() {
		return TimestampWatermark{}, fmt.Errorf("watermark value is required")
	}
	value.Value = value.Value.UTC()
	return value, nil
}

func normalizeOptionalTimestamp(value *TimestampWatermark) (*TimestampWatermark, error) {
	if value == nil {
		return nil, nil
	}
	normalized, err := normalizeTimestamp(*value)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func normalizeIncrementalAttempt(attempt IncrementalAttempt) (IncrementalAttempt, error) {
	if err := validateStage4Identity(attempt.RunID, attempt.Task); err != nil {
		return IncrementalAttempt{}, err
	}
	if strings.TrimSpace(attempt.AttemptID) == "" {
		return IncrementalAttempt{}, fmt.Errorf("incremental attempt ID is required")
	}
	if attempt.StartedAt.IsZero() {
		return IncrementalAttempt{}, fmt.Errorf("incremental attempt start time is required")
	}
	if attempt.Status != "" && attempt.Status != IncrementalRunning {
		return IncrementalAttempt{}, fmt.Errorf("%w: new incremental attempt is %q", ErrStateTransition, attempt.Status)
	}
	if attempt.CommittedWatermark != nil || attempt.TableSucceeded || !attempt.CompletedAt.IsZero() {
		return IncrementalAttempt{}, fmt.Errorf("%w: new incremental attempt contains completion evidence", ErrStateTransition)
	}
	if attempt.Mode != IncrementalBaseline && attempt.Mode != IncrementalWindow {
		return IncrementalAttempt{}, fmt.Errorf("unknown incremental attempt mode %q", attempt.Mode)
	}
	lower, err := normalizeOptionalTimestamp(attempt.LowerWatermark)
	if err != nil {
		return IncrementalAttempt{}, fmt.Errorf("incremental lower watermark: %w", err)
	}
	upper, err := normalizeOptionalTimestamp(attempt.UpperFence)
	if err != nil {
		return IncrementalAttempt{}, fmt.Errorf("incremental upper fence: %w", err)
	}
	if attempt.Mode == IncrementalBaseline && lower != nil {
		return IncrementalAttempt{}, fmt.Errorf("%w: baseline attempt cannot have a lower watermark", ErrStateTransition)
	}
	if lower != nil && upper != nil {
		if lower.Column != upper.Column {
			return IncrementalAttempt{}, fmt.Errorf("incremental lower watermark and upper fence use different columns")
		}
		if lower.Value.After(upper.Value) {
			return IncrementalAttempt{}, fmt.Errorf("incremental lower watermark exceeds upper fence")
		}
	}
	attempt.LowerWatermark = lower
	attempt.UpperFence = upper
	attempt.Status = IncrementalRunning
	attempt.StartedAt = attempt.StartedAt.UTC()
	return attempt, nil
}

func validateIncrementalLowerWatermark(
	attempt IncrementalAttempt,
	latest IncrementalAttempt,
	found bool,
) error {
	if attempt.Mode == IncrementalBaseline {
		return nil
	}
	var expected *TimestampWatermark
	if found {
		if latest.Status != IncrementalCompleted || !latest.TableSucceeded {
			return fmt.Errorf("%w: latest incremental frontier is not committed", ErrStateTransition)
		}
		expected = latest.CommittedWatermark
	}
	if !reflect.DeepEqual(attempt.LowerWatermark, expected) {
		return fmt.Errorf("%w: incremental lower watermark does not match latest committed frontier", ErrImmutableEvidence)
	}
	return nil
}

func incrementalBeginMatches(stored, requested IncrementalAttempt) bool {
	return stored.RunID == requested.RunID &&
		stored.Task == requested.Task &&
		stored.AttemptID == requested.AttemptID &&
		stored.Mode == requested.Mode &&
		reflect.DeepEqual(stored.LowerWatermark, requested.LowerWatermark) &&
		reflect.DeepEqual(stored.UpperFence, requested.UpperFence) &&
		stored.StartedAt.Equal(requested.StartedAt)
}

func applyIncrementalCommit(attempt IncrementalAttempt, commit IncrementalCommit) (IncrementalAttempt, error) {
	if commit.RunID != attempt.RunID || commit.Task != attempt.Task || commit.AttemptID != attempt.AttemptID {
		return IncrementalAttempt{}, fmt.Errorf("%w: incremental attempt identity differs", ErrImmutableEvidence)
	}
	if commit.CompletedAt.IsZero() {
		return IncrementalAttempt{}, fmt.Errorf("incremental completion time is required")
	}
	if commit.CompletedAt.Before(attempt.StartedAt) {
		return IncrementalAttempt{}, fmt.Errorf("incremental completion precedes attempt start")
	}
	if strings.TrimSpace(commit.TopologyHash) == "" {
		return IncrementalAttempt{}, fmt.Errorf("incremental completion topology hash is required")
	}
	watermark, err := normalizeOptionalTimestamp(commit.Watermark)
	if err != nil {
		return IncrementalAttempt{}, fmt.Errorf("incremental committed watermark: %w", err)
	}
	if attempt.UpperFence == nil {
		if !reflect.DeepEqual(watermark, attempt.LowerWatermark) {
			return IncrementalAttempt{}, fmt.Errorf("%w: absent upper fence cannot advance or discard the watermark", ErrStateTransition)
		}
	} else if !reflect.DeepEqual(watermark, attempt.UpperFence) {
		return IncrementalAttempt{}, fmt.Errorf(
			"%w: successful incremental completion must publish its exact immutable upper fence",
			ErrStateTransition,
		)
	}
	if watermark != nil && attempt.LowerWatermark != nil {
		if watermark.Column != attempt.LowerWatermark.Column ||
			watermark.Value.Before(attempt.LowerWatermark.Value) {
			return IncrementalAttempt{}, fmt.Errorf(
				"%w: committed watermark regresses below lower watermark",
				ErrStateTransition,
			)
		}
	}
	completed := attempt
	completed.Status = IncrementalCompleted
	completed.CommittedWatermark = watermark
	completed.TableSucceeded = true
	completed.CompletedAt = commit.CompletedAt.UTC()
	if attempt.Status == IncrementalCompleted {
		if reflect.DeepEqual(attempt, completed) {
			return attempt, nil
		}
		return IncrementalAttempt{}, fmt.Errorf("%w: incremental completion differs", ErrImmutableEvidence)
	}
	if attempt.Status != IncrementalRunning {
		return IncrementalAttempt{}, fmt.Errorf("%w: incremental attempt is %q", ErrStateTransition, attempt.Status)
	}
	return completed, nil
}

func normalizeDeleteReconciliation(record DeleteReconciliation) (DeleteReconciliation, error) {
	if err := validateStage4Identity(record.RunID, record.Task); err != nil {
		return DeleteReconciliation{}, err
	}
	if strings.TrimSpace(record.AttemptID) == "" {
		return DeleteReconciliation{}, fmt.Errorf("delete reconciliation attempt ID is required")
	}
	if record.StartedAt.IsZero() {
		return DeleteReconciliation{}, fmt.Errorf("delete reconciliation start time is required")
	}
	if record.Candidates != 0 || record.DeletedRows != 0 || record.SkippedRows != 0 {
		return DeleteReconciliation{}, fmt.Errorf("%w: new delete reconciliation contains result counts", ErrStateTransition)
	}
	record.StartedAt = record.StartedAt.UTC()
	switch {
	case !record.Due:
		if record.DryRun {
			return DeleteReconciliation{}, fmt.Errorf("not-due reconciliation cannot be a dry run")
		}
		if record.Status != "" && record.Status != DeleteReconciliationNotDue {
			return DeleteReconciliation{}, fmt.Errorf("%w: not-due reconciliation is %q", ErrStateTransition, record.Status)
		}
		if strings.TrimSpace(record.Reason) == "" {
			return DeleteReconciliation{}, fmt.Errorf("not-due reconciliation reason is required")
		}
		record.Status = DeleteReconciliationNotDue
		if record.CompletedAt.IsZero() {
			record.CompletedAt = record.StartedAt
		} else {
			record.CompletedAt = record.CompletedAt.UTC()
			if record.CompletedAt.Before(record.StartedAt) {
				return DeleteReconciliation{}, fmt.Errorf("delete reconciliation completion precedes attempt start")
			}
		}
	default:
		if record.Status != "" && record.Status != DeleteReconciliationRunning {
			return DeleteReconciliation{}, fmt.Errorf("%w: new due reconciliation is %q", ErrStateTransition, record.Status)
		}
		if !record.CompletedAt.IsZero() || record.Reason != "" {
			return DeleteReconciliation{}, fmt.Errorf("%w: new due reconciliation contains terminal evidence", ErrStateTransition)
		}
		record.Status = DeleteReconciliationRunning
	}
	return record, nil
}

func deleteReconciliationBeginMatches(stored, requested DeleteReconciliation) bool {
	return stored.RunID == requested.RunID &&
		stored.Task == requested.Task &&
		stored.AttemptID == requested.AttemptID &&
		stored.Due == requested.Due &&
		stored.DryRun == requested.DryRun &&
		stored.Reason == requested.Reason &&
		stored.StartedAt.Equal(requested.StartedAt)
}

func applyDeleteReconciliationResult(record DeleteReconciliation, result DeleteReconciliationResult) (DeleteReconciliation, error) {
	if result.RunID != record.RunID || result.Task != record.Task || result.AttemptID != record.AttemptID {
		return DeleteReconciliation{}, fmt.Errorf("%w: delete reconciliation identity differs", ErrImmutableEvidence)
	}
	if result.CompletedAt.IsZero() {
		return DeleteReconciliation{}, fmt.Errorf("delete reconciliation completion time is required")
	}
	if result.CompletedAt.Before(record.StartedAt) {
		return DeleteReconciliation{}, fmt.Errorf("delete reconciliation completion precedes attempt start")
	}
	if result.Candidates < 0 || result.DeletedRows < 0 || result.SkippedRows < 0 {
		return DeleteReconciliation{}, fmt.Errorf("delete reconciliation counts must not be negative")
	}
	if result.DeletedRows > result.Candidates {
		return DeleteReconciliation{}, fmt.Errorf("deleted rows exceed delete candidates")
	}
	if result.SkippedRows > result.Candidates-result.DeletedRows {
		return DeleteReconciliation{}, fmt.Errorf("deleted and skipped rows exceed delete candidates")
	}
	switch result.Status {
	case DeleteReconciliationCompleted:
		if record.DryRun {
			return DeleteReconciliation{}, fmt.Errorf("%w: dry run cannot complete as a mutating reconciliation", ErrStateTransition)
		}
		if result.DeletedRows != result.Candidates || result.SkippedRows != 0 {
			return DeleteReconciliation{}, fmt.Errorf(
				"%w: completed reconciliation must delete every candidate without skips",
				ErrStateTransition,
			)
		}
	case DeleteReconciliationDryRun:
		if !record.DryRun || result.DeletedRows != 0 {
			return DeleteReconciliation{}, fmt.Errorf("%w: invalid dry-run result", ErrStateTransition)
		}
	case DeleteReconciliationIncomplete:
		if strings.TrimSpace(result.Reason) == "" {
			return DeleteReconciliation{}, fmt.Errorf("incomplete delete reconciliation reason is required")
		}
	default:
		return DeleteReconciliation{}, fmt.Errorf("%w: terminal delete reconciliation status %q", ErrStateTransition, result.Status)
	}
	completed := record
	completed.Status = result.Status
	completed.Candidates = result.Candidates
	completed.DeletedRows = result.DeletedRows
	completed.SkippedRows = result.SkippedRows
	completed.Reason = result.Reason
	completed.CompletedAt = result.CompletedAt.UTC()
	if record.Status != DeleteReconciliationRunning {
		if reflect.DeepEqual(record, completed) {
			return record, nil
		}
		return DeleteReconciliation{}, fmt.Errorf("%w: delete reconciliation result differs", ErrImmutableEvidence)
	}
	return completed, nil
}

func normalizeStrictSourceEngine(engine string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "postgres", "postgresql", "pg":
		return "postgres", nil
	case "mssql", "sqlserver", "sql-server":
		return "mssql", nil
	case "mysql", "mariadb", "maria":
		return "mysql", nil
	case "sqlite", "sqlite3", "sqlitedb":
		return "sqlite", nil
	default:
		return "", fmt.Errorf("strict snapshots are unsupported for source engine %q", engine)
	}
}

func requireStrictRunSourceEngine(run Run, engine string) error {
	if run.SourceEngine == "" {
		return fmt.Errorf(
			"%w: strict evidence for run %q",
			ErrRunSourceEngineUnavailable,
			run.ID,
		)
	}
	if err := validateRunSourceEngine(run.SourceEngine); err != nil {
		return fmt.Errorf("%w: %v", ErrImmutableEvidence, err)
	}
	if run.SourceEngine != engine {
		return fmt.Errorf(
			"%w: run %q source engine %q differs from strict evidence engine %q",
			ErrImmutableEvidence,
			run.ID,
			run.SourceEngine,
			engine,
		)
	}
	return nil
}

func normalizeStrictMigrationSnapshot(snapshot StrictMigrationSnapshot) (StrictMigrationSnapshot, error) {
	if strings.TrimSpace(snapshot.RunID) == "" {
		return StrictMigrationSnapshot{}, fmt.Errorf("strict migration snapshot run ID is required")
	}
	if strings.TrimSpace(snapshot.EpochID) == "" ||
		strings.TrimSpace(snapshot.SnapshotReference) == "" ||
		strings.TrimSpace(snapshot.ProcessEpoch) == "" {
		return StrictMigrationSnapshot{}, fmt.Errorf("strict migration epoch, reference, and process epoch are required")
	}
	engine, err := normalizeStrictSourceEngine(snapshot.SourceEngine)
	if err != nil {
		return StrictMigrationSnapshot{}, err
	}
	if engine != "postgres" && engine != "mssql" {
		return StrictMigrationSnapshot{}, fmt.Errorf(
			"migration-scoped strict snapshots are unsupported for source engine %q",
			engine,
		)
	}
	if snapshot.CapturedAt.IsZero() {
		return StrictMigrationSnapshot{}, fmt.Errorf("strict migration snapshot capture time is required")
	}
	snapshot.SourceEngine = engine
	snapshot.CapturedAt = snapshot.CapturedAt.UTC()
	return snapshot, nil
}

func normalizeStrictSnapshotEvidence(evidence StrictSnapshotEvidence) (StrictSnapshotEvidence, error) {
	if err := validateStage4Identity(evidence.RunID, evidence.Task); err != nil {
		return StrictSnapshotEvidence{}, err
	}
	if strings.TrimSpace(evidence.AttemptID) == "" ||
		strings.TrimSpace(evidence.SnapshotReference) == "" ||
		strings.TrimSpace(evidence.ProcessEpoch) == "" {
		return StrictSnapshotEvidence{}, fmt.Errorf("strict snapshot attempt, reference, and process epoch are required")
	}
	engine, err := normalizeStrictSourceEngine(evidence.SourceEngine)
	if err != nil {
		return StrictSnapshotEvidence{}, err
	}
	if evidence.Scope != StrictSnapshotTable && evidence.Scope != StrictSnapshotMigration {
		return StrictSnapshotEvidence{}, fmt.Errorf("unknown strict snapshot scope %q", evidence.Scope)
	}
	if evidence.Scope == StrictSnapshotTable && strings.TrimSpace(evidence.MigrationEpochID) != "" {
		return StrictSnapshotEvidence{}, fmt.Errorf("%w: table-scoped evidence cannot reference a migration epoch", ErrStateTransition)
	}
	if evidence.Scope == StrictSnapshotMigration && strings.TrimSpace(evidence.MigrationEpochID) == "" {
		return StrictSnapshotEvidence{}, fmt.Errorf("migration-scoped strict evidence requires a migration epoch")
	}
	if evidence.Scope == StrictSnapshotMigration && engine != "postgres" && engine != "mssql" {
		return StrictSnapshotEvidence{}, fmt.Errorf(
			"migration-scoped strict evidence is unsupported for source engine %q",
			engine,
		)
	}
	if evidence.ExactSourceRowCount < 0 {
		return StrictSnapshotEvidence{}, fmt.Errorf("strict snapshot row count must not be negative")
	}
	if evidence.CapturedAt.IsZero() {
		return StrictSnapshotEvidence{}, fmt.Errorf("strict snapshot capture time is required")
	}
	evidence.SourceEngine = engine
	evidence.CapturedAt = evidence.CapturedAt.UTC()
	return evidence, nil
}

func validateStage4Identity(runID string, task TaskKey) error {
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("stage 4 run ID is required")
	}
	if err := task.Validate(); err != nil {
		return fmt.Errorf("stage 4 task identity: %w", err)
	}
	return nil
}
