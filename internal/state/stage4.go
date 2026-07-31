package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
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
	SaveDeleteReconciliationPlan(DeleteReconciliationPlan) error
	BeginDeleteReconciliationBatch(DeleteReconciliationBatch) (DeleteReconciliationBatch, bool, error)
	CommitDeleteReconciliationBatch(DeleteReconciliationBatchCommit) error
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

// Incomplete reconciliation reasons are stable, non-sensitive codes. Driver
// errors and row values belong in the caller's diagnostic error path, never in
// durable state.
const (
	DeleteReconciliationReasonCancelled                     = "cancelled"
	DeleteReconciliationReasonKeyReadersUnavailable         = "key_readers_unavailable"
	DeleteReconciliationReasonMutationProtectionUnavailable = "mutation_protection_unavailable"
	DeleteReconciliationReasonLeaseLost                     = "lease_lost"
	DeleteReconciliationReasonUnsafeBatchLimits             = "unsafe_batch_limits"
	DeleteReconciliationReasonPlanCreationFailed            = "plan_creation_failed"
	DeleteReconciliationReasonKeyScanFailed                 = "key_scan_failed"
	DeleteReconciliationReasonClockInvalid                  = "clock_invalid"
	DeleteReconciliationReasonDurablePlanMismatch           = "durable_plan_mismatch"
	DeleteReconciliationReasonSpoolUnavailable              = "spool_unavailable"
	DeleteReconciliationReasonSpoolVerificationFailed       = "spool_verification_failed"
	DeleteReconciliationReasonTargetMutationFailed          = "target_mutation_failed"
	DeleteReconciliationReasonTargetReceiptIncomplete       = "target_receipt_incomplete"
)

// DeleteReconciliation distinguishes a reconciliation that was not due from a
// due pass that ran and found zero candidates. Terminal records are immutable,
// so incomplete work can never be mistaken for the durable last success.
type DeleteReconciliation struct {
	RunID            string                           `json:"run_id" yaml:"run_id"`
	Task             TaskKey                          `json:"task" yaml:"task"`
	AttemptID        string                           `json:"attempt_id" yaml:"attempt_id"`
	Due              bool                             `json:"due" yaml:"due"`
	DryRun           bool                             `json:"dry_run" yaml:"dry_run"`
	Status           DeleteReconciliationStatus       `json:"status" yaml:"status"`
	Candidates       int64                            `json:"candidates" yaml:"candidates"`
	DeletedRows      int64                            `json:"deleted_rows" yaml:"deleted_rows"`
	SkippedRows      int64                            `json:"skipped_rows" yaml:"skipped_rows"`
	Reason           string                           `json:"reason,omitempty" yaml:"reason,omitempty"`
	StartedAt        time.Time                        `json:"started_at" yaml:"started_at"`
	CompletedAt      time.Time                        `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
	Plan             *DeleteReconciliationPlan        `json:"plan,omitempty" yaml:"plan,omitempty"`
	Frontier         int64                            `json:"frontier,omitempty" yaml:"frontier,omitempty"`
	CommittedBatches int64                            `json:"committed_batches,omitempty" yaml:"committed_batches,omitempty"`
	PendingBatch     *DeleteReconciliationBatch       `json:"pending_batch,omitempty" yaml:"pending_batch,omitempty"`
	LastBatchCommit  *DeleteReconciliationBatchCommit `json:"last_batch_commit,omitempty" yaml:"last_batch_commit,omitempty"`
}

// DeleteReconciliationPlan binds a due attempt to one immutable, disk-backed
// target-only candidate set before the first target mutation. CandidateDigest
// covers the complete ordered key/parameter stream; EqualityProofDigest binds
// it to the route-specific key equality proof used to construct that stream.
type DeleteReconciliationPlan struct {
	RunID               string    `json:"run_id" yaml:"run_id"`
	Task                TaskKey   `json:"task" yaml:"task"`
	AttemptID           string    `json:"attempt_id" yaml:"attempt_id"`
	PlanID              string    `json:"plan_id" yaml:"plan_id"`
	SpoolPath           string    `json:"spool_path" yaml:"spool_path"`
	EqualityProofDigest string    `json:"equality_proof_digest" yaml:"equality_proof_digest"`
	CandidateDigest     string    `json:"candidate_digest" yaml:"candidate_digest"`
	Candidates          int64     `json:"candidates" yaml:"candidates"`
	BatchSize           int       `json:"batch_size" yaml:"batch_size"`
	BatchByteLimit      int64     `json:"batch_byte_limit" yaml:"batch_byte_limit"`
	KeyWidth            int       `json:"key_width" yaml:"key_width"`
	PlannedAt           time.Time `json:"planned_at" yaml:"planned_at"`
}

// DeleteReconciliationBatch is persisted before its target mutation. Token is
// the idempotency key that a target must journal atomically with the deletes.
type DeleteReconciliationBatch struct {
	RunID          string    `json:"run_id" yaml:"run_id"`
	Task           TaskKey   `json:"task" yaml:"task"`
	AttemptID      string    `json:"attempt_id" yaml:"attempt_id"`
	PlanID         string    `json:"plan_id" yaml:"plan_id"`
	Token          string    `json:"token" yaml:"token"`
	Sequence       int64     `json:"sequence" yaml:"sequence"`
	FirstCandidate int64     `json:"first_candidate" yaml:"first_candidate"`
	Candidates     int64     `json:"candidates" yaml:"candidates"`
	EncodedBytes   int64     `json:"encoded_bytes" yaml:"encoded_bytes"`
	BatchDigest    string    `json:"batch_digest" yaml:"batch_digest"`
	BeganAt        time.Time `json:"began_at" yaml:"began_at"`
}

// DeleteReconciliationBatchCommit records the target's durable idempotent
// receipt and advances the candidate frontier atomically in the state store.
type DeleteReconciliationBatchCommit struct {
	RunID            string    `json:"run_id" yaml:"run_id"`
	Task             TaskKey   `json:"task" yaml:"task"`
	AttemptID        string    `json:"attempt_id" yaml:"attempt_id"`
	PlanID           string    `json:"plan_id" yaml:"plan_id"`
	Token            string    `json:"token" yaml:"token"`
	Sequence         int64     `json:"sequence" yaml:"sequence"`
	FirstCandidate   int64     `json:"first_candidate" yaml:"first_candidate"`
	BatchDigest      string    `json:"batch_digest" yaml:"batch_digest"`
	Candidates       int64     `json:"candidates" yaml:"candidates"`
	EncodedBytes     int64     `json:"encoded_bytes" yaml:"encoded_bytes"`
	DeletedRows      int64     `json:"deleted_rows" yaml:"deleted_rows"`
	ReceiptDigest    string    `json:"receipt_digest" yaml:"receipt_digest"`
	FailClosedReason string    `json:"fail_closed_reason,omitempty" yaml:"fail_closed_reason,omitempty"`
	CommittedAt      time.Time `json:"committed_at" yaml:"committed_at"`
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
	if record.Plan != nil || record.Frontier != 0 ||
		record.CommittedBatches != 0 || record.PendingBatch != nil ||
		record.LastBatchCommit != nil {
		return DeleteReconciliation{}, fmt.Errorf(
			"%w: new delete reconciliation contains plan or batch evidence",
			ErrStateTransition,
		)
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

func validateDeleteDigest(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size ||
		value != strings.ToLower(value) {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", label)
	}
	return nil
}

func validateDeleteReconciliationIncompleteReason(reason string) error {
	switch reason {
	case DeleteReconciliationReasonCancelled,
		DeleteReconciliationReasonKeyReadersUnavailable,
		DeleteReconciliationReasonMutationProtectionUnavailable,
		DeleteReconciliationReasonLeaseLost,
		DeleteReconciliationReasonUnsafeBatchLimits,
		DeleteReconciliationReasonPlanCreationFailed,
		DeleteReconciliationReasonKeyScanFailed,
		DeleteReconciliationReasonClockInvalid,
		DeleteReconciliationReasonDurablePlanMismatch,
		DeleteReconciliationReasonSpoolUnavailable,
		DeleteReconciliationReasonSpoolVerificationFailed,
		DeleteReconciliationReasonTargetMutationFailed,
		DeleteReconciliationReasonTargetReceiptIncomplete:
		return nil
	default:
		return fmt.Errorf(
			"delete reconciliation incomplete reason %q is not a stable code",
			reason,
		)
	}
}

func validateDeleteReconciliationPlan(
	record DeleteReconciliation,
	plan DeleteReconciliationPlan,
) (DeleteReconciliationPlan, error) {
	if record.Status != DeleteReconciliationRunning ||
		!record.Due || record.DryRun {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"%w: delete plan requires a running mutating reconciliation",
			ErrStateTransition,
		)
	}
	if plan.RunID != record.RunID || plan.Task != record.Task ||
		plan.AttemptID != record.AttemptID {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"%w: delete plan identity differs",
			ErrImmutableEvidence,
		)
	}
	if strings.TrimSpace(plan.PlanID) == "" {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation plan ID is required",
		)
	}
	if strings.TrimSpace(plan.SpoolPath) == "" {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation spool path is required",
		)
	}
	if !filepath.IsAbs(plan.SpoolPath) {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation spool path must be absolute",
		)
	}
	if err := validateDeleteDigest(
		"delete reconciliation equality proof digest",
		plan.EqualityProofDigest,
	); err != nil {
		return DeleteReconciliationPlan{}, err
	}
	if err := validateDeleteDigest(
		"delete reconciliation candidate digest",
		plan.CandidateDigest,
	); err != nil {
		return DeleteReconciliationPlan{}, err
	}
	if plan.Candidates < 0 {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation candidate count must not be negative",
		)
	}
	if plan.BatchSize <= 0 || plan.BatchByteLimit <= 0 ||
		plan.KeyWidth <= 0 {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation plan batch size, byte limit, and key width must be positive",
		)
	}
	if plan.PlannedAt.IsZero() ||
		plan.PlannedAt.Before(record.StartedAt) {
		return DeleteReconciliationPlan{}, fmt.Errorf(
			"delete reconciliation plan time is absent or precedes the attempt",
		)
	}
	plan.PlannedAt = plan.PlannedAt.UTC()
	return plan, nil
}

func applyDeleteReconciliationPlan(
	record DeleteReconciliation,
	plan DeleteReconciliationPlan,
) (DeleteReconciliation, error) {
	if err := ValidateDeleteReconciliationEvidence(record); err != nil {
		return DeleteReconciliation{}, fmt.Errorf(
			"invalid stored delete reconciliation: %w",
			err,
		)
	}
	plan, err := validateDeleteReconciliationPlan(record, plan)
	if err != nil {
		return DeleteReconciliation{}, err
	}
	if record.Plan != nil {
		if reflect.DeepEqual(*record.Plan, plan) {
			return record, nil
		}
		return DeleteReconciliation{}, fmt.Errorf(
			"%w: delete reconciliation plan differs",
			ErrImmutableEvidence,
		)
	}
	if record.Frontier != 0 || record.CommittedBatches != 0 ||
		record.PendingBatch != nil || record.LastBatchCommit != nil ||
		record.Candidates != 0 || record.DeletedRows != 0 ||
		record.SkippedRows != 0 {
		return DeleteReconciliation{}, fmt.Errorf(
			"%w: delete reconciliation already contains progress",
			ErrStateTransition,
		)
	}
	next := record
	next.Plan = &plan
	next.Candidates = plan.Candidates
	return next, nil
}

func validateDeleteReconciliationBatch(
	record DeleteReconciliation,
	batch DeleteReconciliationBatch,
) (DeleteReconciliationBatch, error) {
	if record.Status != DeleteReconciliationRunning ||
		record.Plan == nil {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"%w: delete batch requires a running planned reconciliation",
			ErrStateTransition,
		)
	}
	if record.LastBatchCommit != nil &&
		record.LastBatchCommit.FailClosedReason != "" {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"%w: delete reconciliation is fail-closed after its last target receipt",
			ErrStateTransition,
		)
	}
	if batch.RunID != record.RunID || batch.Task != record.Task ||
		batch.AttemptID != record.AttemptID ||
		batch.PlanID != record.Plan.PlanID {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"%w: delete batch identity differs",
			ErrImmutableEvidence,
		)
	}
	if strings.TrimSpace(batch.Token) == "" {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"delete reconciliation batch token is required",
		)
	}
	if batch.Sequence != record.CommittedBatches ||
		batch.FirstCandidate != record.Frontier {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"%w: delete batch does not begin at the durable frontier",
			ErrStateTransition,
		)
	}
	if batch.Candidates <= 0 ||
		batch.Candidates > int64(record.Plan.BatchSize) ||
		batch.EncodedBytes <= 0 ||
		batch.EncodedBytes > record.Plan.BatchByteLimit ||
		batch.FirstCandidate < 0 ||
		batch.FirstCandidate > record.Candidates ||
		batch.Candidates >
			record.Candidates-batch.FirstCandidate {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"%w: delete batch candidate range is invalid",
			ErrStateTransition,
		)
	}
	if err := validateDeleteDigest(
		"delete reconciliation batch digest",
		batch.BatchDigest,
	); err != nil {
		return DeleteReconciliationBatch{}, err
	}
	if batch.BeganAt.IsZero() ||
		batch.BeganAt.Before(record.Plan.PlannedAt) {
		return DeleteReconciliationBatch{}, fmt.Errorf(
			"delete reconciliation batch time is absent or precedes its plan",
		)
	}
	batch.BeganAt = batch.BeganAt.UTC()
	return batch, nil
}

func applyBeginDeleteReconciliationBatch(
	record DeleteReconciliation,
	batch DeleteReconciliationBatch,
) (DeleteReconciliation, DeleteReconciliationBatch, bool, error) {
	if err := ValidateDeleteReconciliationEvidence(record); err != nil {
		return DeleteReconciliation{}, DeleteReconciliationBatch{}, false,
			fmt.Errorf(
				"invalid stored delete reconciliation: %w",
				err,
			)
	}
	batch, err := validateDeleteReconciliationBatch(record, batch)
	if err != nil {
		return DeleteReconciliation{}, DeleteReconciliationBatch{}, false, err
	}
	if record.PendingBatch != nil {
		if reflect.DeepEqual(*record.PendingBatch, batch) {
			return record, *record.PendingBatch, false, nil
		}
		return DeleteReconciliation{}, DeleteReconciliationBatch{}, false,
			fmt.Errorf(
				"%w: pending delete reconciliation batch differs",
				ErrImmutableEvidence,
			)
	}
	next := record
	next.PendingBatch = &batch
	return next, batch, true, nil
}

func validateDeleteReconciliationBatchCommit(
	record DeleteReconciliation,
	commit DeleteReconciliationBatchCommit,
) (DeleteReconciliationBatchCommit, error) {
	if record.Status != DeleteReconciliationRunning ||
		record.Plan == nil || record.PendingBatch == nil {
		return DeleteReconciliationBatchCommit{}, fmt.Errorf(
			"%w: delete batch commit requires a pending batch",
			ErrStateTransition,
		)
	}
	pending := record.PendingBatch
	if commit.RunID != record.RunID || commit.Task != record.Task ||
		commit.AttemptID != record.AttemptID ||
		commit.PlanID != record.Plan.PlanID ||
		commit.Token != pending.Token ||
		commit.Sequence != pending.Sequence ||
		commit.FirstCandidate != pending.FirstCandidate ||
		commit.BatchDigest != pending.BatchDigest ||
		commit.Candidates != pending.Candidates ||
		commit.EncodedBytes != pending.EncodedBytes {
		return DeleteReconciliationBatchCommit{}, fmt.Errorf(
			"%w: delete batch receipt differs from its pending intent",
			ErrImmutableEvidence,
		)
	}
	if commit.DeletedRows < 0 ||
		commit.DeletedRows > commit.Candidates {
		return DeleteReconciliationBatchCommit{}, fmt.Errorf(
			"delete batch receipt has invalid deleted-row count",
		)
	}
	if commit.FailClosedReason != "" {
		if err := validateDeleteReconciliationIncompleteReason(
			commit.FailClosedReason,
		); err != nil {
			return DeleteReconciliationBatchCommit{}, err
		}
	}
	if commit.DeletedRows != commit.Candidates &&
		commit.FailClosedReason == "" {
		return DeleteReconciliationBatchCommit{}, fmt.Errorf(
			"%w: partial delete receipt must fail closed atomically with its frontier",
			ErrStateTransition,
		)
	}
	if err := validateDeleteDigest(
		"delete reconciliation receipt digest",
		commit.ReceiptDigest,
	); err != nil {
		return DeleteReconciliationBatchCommit{}, err
	}
	if commit.CommittedAt.IsZero() ||
		commit.CommittedAt.Before(pending.BeganAt) {
		return DeleteReconciliationBatchCommit{}, fmt.Errorf(
			"delete batch receipt time is absent or precedes its intent",
		)
	}
	commit.CommittedAt = commit.CommittedAt.UTC()
	return commit, nil
}

func applyDeleteReconciliationBatchCommit(
	record DeleteReconciliation,
	commit DeleteReconciliationBatchCommit,
) (DeleteReconciliation, error) {
	if err := ValidateDeleteReconciliationEvidence(record); err != nil {
		return DeleteReconciliation{}, fmt.Errorf(
			"invalid stored delete reconciliation: %w",
			err,
		)
	}
	if !commit.CommittedAt.IsZero() {
		commit.CommittedAt = commit.CommittedAt.UTC()
	}
	if record.PendingBatch == nil && record.LastBatchCommit != nil &&
		reflect.DeepEqual(*record.LastBatchCommit, commit) {
		return record, nil
	}
	commit, err := validateDeleteReconciliationBatchCommit(record, commit)
	if err != nil {
		return DeleteReconciliation{}, err
	}
	next := record
	next.Frontier += commit.Candidates
	next.CommittedBatches++
	next.DeletedRows += commit.DeletedRows
	next.PendingBatch = nil
	next.LastBatchCommit = &commit
	return next, nil
}

// ValidateDeleteReconciliationEvidence rejects malformed loaded state before a
// caller uses terminal status for scheduling/validation or resumes mutation.
func ValidateDeleteReconciliationEvidence(
	record DeleteReconciliation,
) error {
	if err := validateStage4Identity(record.RunID, record.Task); err != nil {
		return err
	}
	if strings.TrimSpace(record.AttemptID) == "" ||
		record.StartedAt.IsZero() {
		return fmt.Errorf(
			"delete reconciliation identity and start time are required",
		)
	}
	if record.Candidates < 0 || record.DeletedRows < 0 ||
		record.SkippedRows < 0 ||
		record.DeletedRows+record.SkippedRows > record.Candidates {
		return fmt.Errorf(
			"delete reconciliation contains invalid result counts",
		)
	}
	if record.Plan == nil {
		if record.Frontier != 0 || record.CommittedBatches != 0 ||
			record.PendingBatch != nil || record.LastBatchCommit != nil {
			return fmt.Errorf(
				"delete reconciliation has progress without a plan",
			)
		}
	} else {
		plan, err := validateDeleteReconciliationPlan(
			DeleteReconciliation{
				RunID: record.RunID, Task: record.Task,
				AttemptID: record.AttemptID, Due: true,
				Status:    DeleteReconciliationRunning,
				StartedAt: record.StartedAt,
			},
			*record.Plan,
		)
		if err != nil {
			return err
		}
		if plan.Candidates != record.Candidates ||
			record.Frontier < 0 ||
			record.Frontier > record.Candidates ||
			record.DeletedRows > record.Frontier ||
			record.CommittedBatches < 0 ||
			record.CommittedBatches > record.Frontier {
			return fmt.Errorf(
				"delete reconciliation plan progress is inconsistent",
			)
		}
		if record.PendingBatch != nil {
			if _, err := validateDeleteReconciliationBatch(
				record,
				*record.PendingBatch,
			); err != nil {
				return err
			}
		}
		if record.Frontier == 0 {
			if record.CommittedBatches != 0 ||
				record.LastBatchCommit != nil {
				return fmt.Errorf(
					"delete reconciliation has batch evidence before its frontier",
				)
			}
		} else {
			if record.CommittedBatches <= 0 ||
				record.LastBatchCommit == nil {
				return fmt.Errorf(
					"delete reconciliation frontier lacks batch evidence",
				)
			}
			last := record.LastBatchCommit
			if last.RunID != record.RunID ||
				last.Task != record.Task ||
				last.AttemptID != record.AttemptID ||
				last.PlanID != record.Plan.PlanID ||
				strings.TrimSpace(last.Token) == "" ||
				last.Sequence != record.CommittedBatches-1 ||
				last.Candidates <= 0 ||
				last.Candidates > int64(record.Plan.BatchSize) ||
				last.FirstCandidate < 0 ||
				last.Candidates > record.Frontier ||
				last.FirstCandidate !=
					record.Frontier-last.Candidates ||
				last.EncodedBytes <= 0 ||
				last.EncodedBytes >
					record.Plan.BatchByteLimit ||
				last.DeletedRows < 0 ||
				last.DeletedRows > last.Candidates ||
				last.CommittedAt.IsZero() ||
				last.CommittedAt.Before(record.Plan.PlannedAt) {
				return fmt.Errorf(
					"delete reconciliation last batch evidence is invalid",
				)
			}
			if err := validateDeleteDigest(
				"delete reconciliation last batch digest",
				last.BatchDigest,
			); err != nil {
				return err
			}
			if err := validateDeleteDigest(
				"delete reconciliation last receipt digest",
				last.ReceiptDigest,
			); err != nil {
				return err
			}
			if last.FailClosedReason != "" {
				if err := validateDeleteReconciliationIncompleteReason(
					last.FailClosedReason,
				); err != nil {
					return err
				}
			}
			if last.DeletedRows != last.Candidates &&
				last.FailClosedReason == "" {
				return fmt.Errorf(
					"delete reconciliation partial receipt is not fail-closed",
				)
			}
		}
	}
	switch record.Status {
	case DeleteReconciliationRunning:
		if !record.Due || !record.CompletedAt.IsZero() ||
			record.Reason != "" || record.SkippedRows != 0 {
			return fmt.Errorf(
				"running delete reconciliation has terminal evidence",
			)
		}
	case DeleteReconciliationNotDue:
		if record.Due || record.DryRun || record.Reason == "" ||
			record.CompletedAt.IsZero() || record.Plan != nil ||
			record.Candidates != 0 || record.DeletedRows != 0 ||
			record.SkippedRows != 0 {
			return fmt.Errorf(
				"not-due delete reconciliation evidence is invalid",
			)
		}
	case DeleteReconciliationCompleted:
		if !record.Due || record.DryRun ||
			record.CompletedAt.IsZero() || record.PendingBatch != nil ||
			record.DeletedRows != record.Candidates ||
			record.SkippedRows != 0 ||
			record.Plan != nil && record.Frontier != record.Candidates ||
			record.LastBatchCommit != nil &&
				record.LastBatchCommit.FailClosedReason != "" {
			return fmt.Errorf(
				"completed delete reconciliation evidence is invalid",
			)
		}
	case DeleteReconciliationIncomplete:
		if !record.Due || record.CompletedAt.IsZero() ||
			strings.TrimSpace(record.Reason) == "" ||
			record.PendingBatch != nil ||
			record.SkippedRows != record.Candidates-record.DeletedRows {
			return fmt.Errorf(
				"incomplete delete reconciliation evidence is invalid",
			)
		}
		if err := validateDeleteReconciliationIncompleteReason(
			record.Reason,
		); err != nil {
			return err
		}
		if record.LastBatchCommit != nil &&
			record.LastBatchCommit.FailClosedReason != "" &&
			record.Reason != record.LastBatchCommit.FailClosedReason {
			return fmt.Errorf(
				"incomplete delete reconciliation reason differs from its fail-closed receipt",
			)
		}
	case DeleteReconciliationDryRun:
		if !record.Due || !record.DryRun ||
			record.CompletedAt.IsZero() || record.DeletedRows != 0 ||
			record.Plan != nil {
			return fmt.Errorf(
				"dry-run delete reconciliation evidence is invalid",
			)
		}
	default:
		return fmt.Errorf(
			"delete reconciliation has invalid status %q",
			record.Status,
		)
	}
	if !record.CompletedAt.IsZero() &&
		record.CompletedAt.Before(record.StartedAt) {
		return fmt.Errorf(
			"delete reconciliation completion precedes its start",
		)
	}
	return nil
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
	if err := ValidateDeleteReconciliationEvidence(record); err != nil {
		return DeleteReconciliation{}, fmt.Errorf(
			"invalid stored delete reconciliation: %w",
			err,
		)
	}
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
	if record.Plan != nil {
		if result.Candidates != record.Candidates ||
			result.DeletedRows != record.DeletedRows {
			return DeleteReconciliation{}, fmt.Errorf(
				"%w: delete result differs from durable plan progress",
				ErrImmutableEvidence,
			)
		}
		if result.Status == DeleteReconciliationCompleted &&
			(record.PendingBatch != nil ||
				record.Frontier != record.Candidates ||
				record.LastBatchCommit != nil &&
					record.LastBatchCommit.FailClosedReason != "") {
			return DeleteReconciliation{}, fmt.Errorf(
				"%w: delete plan is not fully committed",
				ErrStateTransition,
			)
		}
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
		if err := validateDeleteReconciliationIncompleteReason(
			result.Reason,
		); err != nil {
			return DeleteReconciliation{}, err
		}
		if record.LastBatchCommit != nil &&
			record.LastBatchCommit.FailClosedReason != "" &&
			result.Reason != record.LastBatchCommit.FailClosedReason {
			return DeleteReconciliation{}, fmt.Errorf(
				"%w: incomplete result differs from its fail-closed receipt",
				ErrImmutableEvidence,
			)
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
	if err := ValidateDeleteReconciliationEvidence(completed); err != nil {
		return DeleteReconciliation{}, fmt.Errorf(
			"invalid delete reconciliation result: %w",
			err,
		)
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
