package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Stage4RuntimeTuningHistoryVersion is the private SQLite wire version for
// Stage 4 runtime-tuning history. YAML deliberately does not implement this
// optional capability: it remains the portable current-run state backend, not
// the full local history backend required to retain long-lived tuning facts.
const Stage4RuntimeTuningHistoryVersion = 1

// Stage4RuntimeTuningSessionRetention and
// Stage4RuntimeTuningDecisionRetention bound the full-local history retained
// for one run/table. They intentionally do not consume
// history_retention_days, whose Stage 5 consumer remains deferred.
const (
	Stage4RuntimeTuningSessionRetention  = 16
	Stage4RuntimeTuningDecisionRetention = 128
)

// Stage4RuntimeTuningHistoryBackend is an optional full-local state
// capability. It records immutable controller-session authority and ordered
// decision receipts; callers must obtain it through
// ResolveFencedStage4RuntimeTuningHistory so a raw SQLite store is never
// mistaken for lease-fenced production authority.
type Stage4RuntimeTuningHistoryBackend interface {
	EnsureStage4RuntimeTuningSession(Stage4RuntimeTuningSession) (Stage4RuntimeTuningSessionReceipt, bool, error)
	LoadStage4RuntimeTuningSession(string, string) (Stage4RuntimeTuningSessionReceipt, bool, error)
	EnsureStage4RuntimeTuningDecision(Stage4RuntimeTuningDecision) (Stage4RuntimeTuningDecisionReceipt, bool, error)
	LoadStage4RuntimeTuningDecisions(string, string) ([]Stage4RuntimeTuningDecisionReceipt, error)
}

// Stage4RuntimeTuningHistoryResolver is implemented only by the lease-fenced
// wrapper. Keeping resolution distinct from the raw history capability makes
// production composition fail closed rather than writing mutable history
// through an unfenced SQLiteStore value.
type Stage4RuntimeTuningHistoryResolver interface {
	ResolveStage4RuntimeTuningHistory() (Stage4RuntimeTuningHistoryBackend, bool)
}

// ResolveFencedStage4RuntimeTuningHistory returns the optional durable history
// writer only when backend is a live lease-fenced wrapper around a backend
// that actually supports the capability. Reads and legacy/YAML runs remain
// usable without it.
func ResolveFencedStage4RuntimeTuningHistory(
	backend any,
) (Stage4RuntimeTuningHistoryBackend, bool) {
	resolver, ok := backend.(Stage4RuntimeTuningHistoryResolver)
	if !ok || stage4RuntimeTuningHistoryResolverIsNil(resolver) {
		return nil, false
	}
	history, ok := resolver.ResolveStage4RuntimeTuningHistory()
	if !ok || stage4RuntimeTuningHistoryBackendIsNil(history) {
		return nil, false
	}
	return history, true
}

// Stage4RuntimeTuningSession is immutable per fresh/resume controller
// instance. It captures only credential-free plan identity. A resume starts a
// new controller/session and never rehydrates prior adaptive values.
type Stage4RuntimeTuningSession struct {
	Version       int       `json:"version"`
	RunID         string    `json:"run_id"`
	SessionID     string    `json:"session_id"`
	Resume        bool      `json:"resume"`
	Task          TaskKey   `json:"task"`
	TopologyHash  string    `json:"topology_hash"`
	SourceEngine  string    `json:"source_engine"`
	TargetEngine  string    `json:"target_engine"`
	IntentDigest  string    `json:"intent_digest"`
	IntervalNanos int64     `json:"interval_nanos"`
	DecisionLimit int       `json:"decision_limit"`
	StartedAt     time.Time `json:"started_at"`
}

// Stage4RuntimeTuningSessionReceipt adds the run-owned canonical target
// identity digest and a digest over the complete receipt. The target identity
// itself never appears in tuning history.
type Stage4RuntimeTuningSessionReceipt struct {
	Session              Stage4RuntimeTuningSession `json:"session"`
	TargetIdentityDigest string                     `json:"target_identity_digest"`
	Digest               string                     `json:"digest"`
}

// Stage4RuntimeTuningBoundary is a secret-free completed chunk identity. The
// session binds its table and topology; the boundary supplies the exact range
// and attempt order inside that session.
type Stage4RuntimeTuningBoundary struct {
	Ordinal       uint64 `json:"ordinal"`
	TableSchema   string `json:"table_schema"`
	TableName     string `json:"table_name"`
	RangeIndex    uint64 `json:"range_index"`
	ChunkSequence uint64 `json:"chunk_sequence"`
	Attempt       uint32 `json:"attempt"`
}

// Stage4RuntimeTuningValue records one effective controller value without
// losing the immutable input value/provenance which selected it.
type Stage4RuntimeTuningValue struct {
	Value             int    `json:"value"`
	IntentValue       int    `json:"intent_value"`
	IntentProvenance  string `json:"intent_provenance"`
	LiveProvenance    string `json:"live_provenance"`
	PerformancePinned bool   `json:"performance_pinned"`
}

type Stage4RuntimeTuningValues struct {
	ChunkRows   Stage4RuntimeTuningValue `json:"chunk_rows"`
	Writers     Stage4RuntimeTuningValue `json:"writers"`
	BufferDepth Stage4RuntimeTuningValue `json:"buffer_depth"`
}

// Stage4RuntimeTuningDecision is one exact controller transition. Reasons are
// a closed, secret-free vocabulary; raw driver errors and row/page values are
// intentionally excluded. PreviousDigest chains decisions even after the
// bounded retained prefix has been pruned.
type Stage4RuntimeTuningDecision struct {
	Version        int                         `json:"version"`
	RunID          string                      `json:"run_id"`
	SessionID      string                      `json:"session_id"`
	Boundary       Stage4RuntimeTuningBoundary `json:"boundary"`
	Before         Stage4RuntimeTuningValues   `json:"before"`
	After          Stage4RuntimeTuningValues   `json:"after"`
	Reasons        []string                    `json:"reasons"`
	PreviousDigest string                      `json:"previous_digest,omitempty"`
}

// Stage4RuntimeTuningDecisionReceipt is immutable once it exists. RecordedAt
// is created by the state backend, so a retry compares exact decision content
// without needing to reproduce wall-clock time.
type Stage4RuntimeTuningDecisionReceipt struct {
	Decision   Stage4RuntimeTuningDecision `json:"decision"`
	RecordedAt time.Time                   `json:"recorded_at"`
	Digest     string                      `json:"digest"`
}

// Stage4RuntimeTuningHistoryCursor is the mutable, fenced checkpoint for one
// immutable decision chain. It is separate from receipts because retention
// removes old records while the cursor must preserve the exact next ordinal
// and previous digest.
type Stage4RuntimeTuningHistoryCursor struct {
	Version             int    `json:"version"`
	RunID               string `json:"run_id"`
	SessionID           string `json:"session_id"`
	LastOrdinal         uint64 `json:"last_ordinal"`
	LastDigest          string `json:"last_digest"`
	TotalDecisions      uint64 `json:"total_decisions"`
	RetainedFromOrdinal uint64 `json:"retained_from_ordinal"`
	RetainedDecisions   int    `json:"retained_decisions"`
}

type Stage4RuntimeTuningHistoryCursorReceipt struct {
	Cursor Stage4RuntimeTuningHistoryCursor `json:"cursor"`
	Digest string                           `json:"digest"`
}

func (session Stage4RuntimeTuningSession) Clone() Stage4RuntimeTuningSession {
	return session
}

func (session Stage4RuntimeTuningSession) Equal(
	other Stage4RuntimeTuningSession,
) bool {
	left, leftErr := normalizeStage4RuntimeTuningSession(session)
	right, rightErr := normalizeStage4RuntimeTuningSession(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (session Stage4RuntimeTuningSession) Validate() error {
	_, err := normalizeStage4RuntimeTuningSession(session)
	return err
}

func (receipt Stage4RuntimeTuningSessionReceipt) Clone() Stage4RuntimeTuningSessionReceipt {
	receipt.Session = receipt.Session.Clone()
	return receipt
}

func (receipt Stage4RuntimeTuningSessionReceipt) Equal(
	other Stage4RuntimeTuningSessionReceipt,
) bool {
	left, leftErr := normalizeStoredStage4RuntimeTuningSession(receipt)
	right, rightErr := normalizeStoredStage4RuntimeTuningSession(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (receipt Stage4RuntimeTuningSessionReceipt) Validate() error {
	_, err := normalizeStoredStage4RuntimeTuningSession(receipt)
	return err
}

func (decision Stage4RuntimeTuningDecision) Clone() Stage4RuntimeTuningDecision {
	decision.Reasons = append([]string(nil), decision.Reasons...)
	return decision
}

func (decision Stage4RuntimeTuningDecision) Equal(
	other Stage4RuntimeTuningDecision,
) bool {
	left, leftErr := normalizeStage4RuntimeTuningDecision(decision)
	right, rightErr := normalizeStage4RuntimeTuningDecision(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (decision Stage4RuntimeTuningDecision) Validate() error {
	_, err := normalizeStage4RuntimeTuningDecision(decision)
	return err
}

func (receipt Stage4RuntimeTuningDecisionReceipt) Clone() Stage4RuntimeTuningDecisionReceipt {
	receipt.Decision = receipt.Decision.Clone()
	return receipt
}

func (receipt Stage4RuntimeTuningDecisionReceipt) Equal(
	other Stage4RuntimeTuningDecisionReceipt,
) bool {
	left, leftErr := normalizeStoredStage4RuntimeTuningDecision(receipt)
	right, rightErr := normalizeStoredStage4RuntimeTuningDecision(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

func (receipt Stage4RuntimeTuningDecisionReceipt) Validate() error {
	_, err := normalizeStoredStage4RuntimeTuningDecision(receipt)
	return err
}

func (receipt Stage4RuntimeTuningHistoryCursorReceipt) Clone() Stage4RuntimeTuningHistoryCursorReceipt {
	return receipt
}

func (receipt Stage4RuntimeTuningHistoryCursorReceipt) Validate() error {
	_, err := normalizeStoredStage4RuntimeTuningCursor(receipt)
	return err
}

func normalizeStage4RuntimeTuningSession(
	session Stage4RuntimeTuningSession,
) (Stage4RuntimeTuningSession, error) {
	if session.Version != Stage4RuntimeTuningHistoryVersion {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning session version %d is unsupported", session.Version,
		)
	}
	if err := validateStage4RuntimeTuningRunID(session.RunID); err != nil {
		return Stage4RuntimeTuningSession{}, err
	}
	var err error
	if session.SessionID, err = normalizeStage4RuntimeTuningSessionID(
		"session ID", session.SessionID,
	); err != nil {
		return Stage4RuntimeTuningSession{}, err
	}
	if err := session.Task.Validate(); err != nil ||
		!stage4TableWorkType(session.Task.Type) ||
		session.Task.Partition != "" {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"%w: runtime-tuning session task is unsupported", ErrImmutableEvidence,
		)
	}
	if session.TopologyHash, err = normalizeStage4RuntimeTuningToken(
		"topology hash", session.TopologyHash, 256,
	); err != nil {
		return Stage4RuntimeTuningSession{}, err
	}
	if session.SourceEngine, err = normalizeStage4RuntimeTuningEngine(
		session.SourceEngine,
	); err != nil {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning source %w", err,
		)
	}
	if session.TargetEngine, err = normalizeStage4RuntimeTuningEngine(
		session.TargetEngine,
	); err != nil {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning target %w", err,
		)
	}
	if session.IntentDigest, err = normalizeStage4RuntimeTuningDigest(
		"intent digest", session.IntentDigest,
	); err != nil {
		return Stage4RuntimeTuningSession{}, err
	}
	if session.IntervalNanos <= 0 {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning session interval must be positive",
		)
	}
	if session.DecisionLimit < 1 ||
		session.DecisionLimit > Stage4RuntimeTuningDecisionRetention {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning session decision retention is invalid",
		)
	}
	if session.StartedAt.IsZero() {
		return Stage4RuntimeTuningSession{}, fmt.Errorf(
			"runtime-tuning session start time is required",
		)
	}
	session.StartedAt = session.StartedAt.UTC()
	return session, nil
}

func normalizeStoredStage4RuntimeTuningSession(
	receipt Stage4RuntimeTuningSessionReceipt,
) (Stage4RuntimeTuningSessionReceipt, error) {
	normalized, err := normalizeStage4RuntimeTuningSession(receipt.Session)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, err
	}
	if receipt.TargetIdentityDigest, err = normalizeStage4RuntimeTuningDigest(
		"target identity digest", receipt.TargetIdentityDigest,
	); err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, err
	}
	expected, err := newStage4RuntimeTuningSessionReceipt(
		normalized,
		receipt.TargetIdentityDigest,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, err
	}
	if receipt.Digest != expected.Digest ||
		!reflect.DeepEqual(receipt.Session, normalized) {
		return Stage4RuntimeTuningSessionReceipt{}, fmt.Errorf(
			"%w: runtime-tuning session receipt differs", ErrImmutableEvidence,
		)
	}
	receipt.Session = normalized
	return receipt, nil
}

func newStage4RuntimeTuningSessionReceipt(
	session Stage4RuntimeTuningSession,
	targetIdentityDigest string,
) (Stage4RuntimeTuningSessionReceipt, error) {
	normalized, err := normalizeStage4RuntimeTuningSession(session)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, err
	}
	targetIdentityDigest, err = normalizeStage4RuntimeTuningDigest(
		"target identity digest", targetIdentityDigest,
	)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, err
	}
	base := struct {
		Session              Stage4RuntimeTuningSession `json:"session"`
		TargetIdentityDigest string                     `json:"target_identity_digest"`
	}{
		Session: normalized, TargetIdentityDigest: targetIdentityDigest,
	}
	payload, err := json.Marshal(base)
	if err != nil {
		return Stage4RuntimeTuningSessionReceipt{}, fmt.Errorf(
			"encode runtime-tuning session receipt: %w", err,
		)
	}
	digest := sha256.Sum256(payload)
	return Stage4RuntimeTuningSessionReceipt{
		Session: normalized, TargetIdentityDigest: targetIdentityDigest,
		Digest: hex.EncodeToString(digest[:]),
	}, nil
}

func normalizeStage4RuntimeTuningDecision(
	decision Stage4RuntimeTuningDecision,
) (Stage4RuntimeTuningDecision, error) {
	if decision.Version != Stage4RuntimeTuningHistoryVersion {
		return Stage4RuntimeTuningDecision{}, fmt.Errorf(
			"runtime-tuning decision version %d is unsupported", decision.Version,
		)
	}
	if err := validateStage4RuntimeTuningRunID(decision.RunID); err != nil {
		return Stage4RuntimeTuningDecision{}, err
	}
	var err error
	if decision.SessionID, err = normalizeStage4RuntimeTuningSessionID(
		"decision session ID", decision.SessionID,
	); err != nil {
		return Stage4RuntimeTuningDecision{}, err
	}
	if decision.Boundary.Ordinal == 0 ||
		decision.Boundary.TableName == "" ||
		decision.Boundary.TableName != strings.TrimSpace(decision.Boundary.TableName) ||
		decision.Boundary.TableSchema != strings.TrimSpace(decision.Boundary.TableSchema) ||
		len(decision.Boundary.TableName) > 1024 ||
		len(decision.Boundary.TableSchema) > 1024 {
		return Stage4RuntimeTuningDecision{}, fmt.Errorf(
			"runtime-tuning decision boundary is invalid",
		)
	}
	if err := validateStage4RuntimeTuningValues(decision.Before); err != nil {
		return Stage4RuntimeTuningDecision{}, fmt.Errorf(
			"runtime-tuning decision before values: %w", err,
		)
	}
	if err := validateStage4RuntimeTuningValues(decision.After); err != nil {
		return Stage4RuntimeTuningDecision{}, fmt.Errorf(
			"runtime-tuning decision after values: %w", err,
		)
	}
	if len(decision.Reasons) == 0 || len(decision.Reasons) > 32 {
		return Stage4RuntimeTuningDecision{}, fmt.Errorf(
			"runtime-tuning decision reasons are invalid",
		)
	}
	seen := make(map[string]struct{}, len(decision.Reasons))
	for index, reason := range decision.Reasons {
		if !knownStage4RuntimeTuningReason(reason) {
			return Stage4RuntimeTuningDecision{}, fmt.Errorf(
				"runtime-tuning decision reason %d is unsupported", index,
			)
		}
		if _, duplicate := seen[reason]; duplicate {
			return Stage4RuntimeTuningDecision{}, fmt.Errorf(
				"runtime-tuning decision reasons are duplicated",
			)
		}
		seen[reason] = struct{}{}
	}
	if decision.Boundary.Ordinal == 1 {
		if decision.PreviousDigest != "" {
			return Stage4RuntimeTuningDecision{}, fmt.Errorf(
				"runtime-tuning first decision has previous digest",
			)
		}
	} else if decision.PreviousDigest, err = normalizeStage4RuntimeTuningDigest(
		"previous decision digest", decision.PreviousDigest,
	); err != nil {
		return Stage4RuntimeTuningDecision{}, err
	}
	return decision, nil
}

func normalizeStoredStage4RuntimeTuningDecision(
	receipt Stage4RuntimeTuningDecisionReceipt,
) (Stage4RuntimeTuningDecisionReceipt, error) {
	normalized, err := normalizeStage4RuntimeTuningDecision(receipt.Decision)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, err
	}
	if receipt.RecordedAt.IsZero() {
		return Stage4RuntimeTuningDecisionReceipt{}, fmt.Errorf(
			"runtime-tuning decision receipt time is required",
		)
	}
	receipt.RecordedAt = receipt.RecordedAt.UTC()
	expected, err := newStage4RuntimeTuningDecisionReceipt(
		normalized,
		receipt.RecordedAt,
	)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, err
	}
	if receipt.Digest != expected.Digest ||
		!reflect.DeepEqual(receipt.Decision, normalized) {
		return Stage4RuntimeTuningDecisionReceipt{}, fmt.Errorf(
			"%w: runtime-tuning decision receipt differs", ErrImmutableEvidence,
		)
	}
	receipt.Decision = normalized
	return receipt, nil
}

func newStage4RuntimeTuningDecisionReceipt(
	decision Stage4RuntimeTuningDecision,
	recordedAt time.Time,
) (Stage4RuntimeTuningDecisionReceipt, error) {
	normalized, err := normalizeStage4RuntimeTuningDecision(decision)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, err
	}
	if recordedAt.IsZero() {
		return Stage4RuntimeTuningDecisionReceipt{}, fmt.Errorf(
			"runtime-tuning decision receipt time is required",
		)
	}
	recordedAt = recordedAt.UTC()
	base := struct {
		Decision   Stage4RuntimeTuningDecision `json:"decision"`
		RecordedAt time.Time                   `json:"recorded_at"`
	}{Decision: normalized, RecordedAt: recordedAt}
	payload, err := json.Marshal(base)
	if err != nil {
		return Stage4RuntimeTuningDecisionReceipt{}, fmt.Errorf(
			"encode runtime-tuning decision receipt: %w", err,
		)
	}
	digest := sha256.Sum256(payload)
	return Stage4RuntimeTuningDecisionReceipt{
		Decision: normalized, RecordedAt: recordedAt,
		Digest: hex.EncodeToString(digest[:]),
	}, nil
}

func normalizeStoredStage4RuntimeTuningCursor(
	receipt Stage4RuntimeTuningHistoryCursorReceipt,
) (Stage4RuntimeTuningHistoryCursorReceipt, error) {
	cursor := receipt.Cursor
	if cursor.Version != Stage4RuntimeTuningHistoryVersion ||
		validateStage4RuntimeTuningRunID(cursor.RunID) != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, fmt.Errorf(
			"runtime-tuning history cursor is invalid",
		)
	}
	var err error
	if cursor.SessionID, err = normalizeStage4RuntimeTuningSessionID(
		"cursor session ID", cursor.SessionID,
	); err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, err
	}
	if cursor.LastOrdinal == 0 || cursor.TotalDecisions != cursor.LastOrdinal ||
		cursor.RetainedFromOrdinal == 0 ||
		cursor.RetainedFromOrdinal > cursor.LastOrdinal ||
		cursor.RetainedDecisions < 1 ||
		cursor.RetainedDecisions > Stage4RuntimeTuningDecisionRetention ||
		uint64(cursor.RetainedDecisions) != cursor.LastOrdinal-cursor.RetainedFromOrdinal+1 {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, fmt.Errorf(
			"runtime-tuning history cursor counters are invalid",
		)
	}
	if cursor.LastDigest, err = normalizeStage4RuntimeTuningDigest(
		"cursor last digest", cursor.LastDigest,
	); err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, err
	}
	base := struct {
		Cursor Stage4RuntimeTuningHistoryCursor `json:"cursor"`
	}{Cursor: cursor}
	payload, err := json.Marshal(base)
	if err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, fmt.Errorf(
			"encode runtime-tuning history cursor: %w", err,
		)
	}
	digest := sha256.Sum256(payload)
	if receipt.Digest != hex.EncodeToString(digest[:]) ||
		!reflect.DeepEqual(receipt.Cursor, cursor) {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, fmt.Errorf(
			"%w: runtime-tuning history cursor differs", ErrImmutableEvidence,
		)
	}
	receipt.Cursor = cursor
	return receipt, nil
}

func newStage4RuntimeTuningCursorReceipt(
	cursor Stage4RuntimeTuningHistoryCursor,
) (Stage4RuntimeTuningHistoryCursorReceipt, error) {
	base := Stage4RuntimeTuningHistoryCursorReceipt{Cursor: cursor}
	// Normalize through the stored form after calculating the content digest.
	payload, err := json.Marshal(struct {
		Cursor Stage4RuntimeTuningHistoryCursor `json:"cursor"`
	}{Cursor: cursor})
	if err != nil {
		return Stage4RuntimeTuningHistoryCursorReceipt{}, fmt.Errorf(
			"encode runtime-tuning history cursor: %w", err,
		)
	}
	digest := sha256.Sum256(payload)
	base.Digest = hex.EncodeToString(digest[:])
	return normalizeStoredStage4RuntimeTuningCursor(base)
}

func validateStage4RuntimeTuningValues(values Stage4RuntimeTuningValues) error {
	for _, item := range []struct {
		name  string
		value Stage4RuntimeTuningValue
	}{
		{name: "chunk rows", value: values.ChunkRows},
		{name: "writers", value: values.Writers},
		{name: "buffer depth", value: values.BufferDepth},
	} {
		if item.value.Value < 1 || item.value.Value > 1<<20 ||
			item.value.IntentValue < 1 || item.value.IntentValue > 1<<20 ||
			!knownStage4RuntimeTuningConfigProvenance(item.value.IntentProvenance) ||
			!knownStage4RuntimeTuningLiveProvenance(item.value.LiveProvenance) {
			return fmt.Errorf("runtime-tuning %s is invalid", item.name)
		}
	}
	return nil
}

func validateStage4RuntimeTuningRunID(runID string) error {
	if strings.TrimSpace(runID) == "" || runID != strings.TrimSpace(runID) ||
		len(runID) > 512 {
		return fmt.Errorf("runtime-tuning run ID is required")
	}
	return nil
}

func normalizeStage4RuntimeTuningToken(
	label string,
	value string,
	maximum int,
) (string, error) {
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) ||
		len(value) > maximum {
		return "", fmt.Errorf("runtime-tuning %s is invalid", label)
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return "", fmt.Errorf(
			"runtime-tuning %s character %d is invalid", label, index,
		)
	}
	return value, nil
}

func normalizeStage4RuntimeTuningSessionID(
	label, value string,
) (string, error) {
	normalized, err := normalizeStage4RuntimeTuningToken(label, value, 32)
	if err != nil {
		return "", err
	}
	if len(normalized) != 32 {
		return "", fmt.Errorf("runtime-tuning %s is invalid", label)
	}
	decoded, err := hex.DecodeString(normalized)
	if err != nil || len(decoded) != 16 {
		return "", fmt.Errorf("runtime-tuning %s is not hexadecimal", label)
	}
	return normalized, nil
}

func normalizeStage4RuntimeTuningDigest(
	label, value string,
) (string, error) {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return "", fmt.Errorf("runtime-tuning %s must be a SHA-256 digest", label)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("runtime-tuning %s must be a SHA-256 digest", label)
	}
	return value, nil
}

func normalizeStage4RuntimeTuningEngine(value string) (string, error) {
	switch value {
	case "postgres", "mysql", "mssql", "sqlite":
		return value, nil
	default:
		return "", fmt.Errorf("engine %q is unsupported", value)
	}
}

func knownStage4RuntimeTuningConfigProvenance(value string) bool {
	switch value {
	case "host_available", "host_capacity", "cgroup_v2_remaining",
		"cgroup_v1_remaining", "user_memory_ceiling", "derived",
		"requested", "safety_clamped":
		return true
	default:
		return false
	}
}

func knownStage4RuntimeTuningLiveProvenance(value string) bool {
	switch value {
	case "initial", "safety_reduction", "evidence_growth":
		return true
	default:
		return false
	}
}

func knownStage4RuntimeTuningReason(value string) bool {
	switch value {
	case "protocol_ceiling", "memory_ceiling", "connection_ceiling",
		"memory_pressure", "queue_pressure", "connection_pressure",
		"write_error", "protocol_write_error", "evidence_growth",
		"insufficient_evidence", "headroom_unavailable",
		"healthy_observation", "interval_gate", "pinned_ceiling",
		"effective_ceiling":
		return true
	default:
		return false
	}
}

func stage4RuntimeTuningTargetIdentityDigest(identity string) (string, error) {
	if strings.TrimSpace(identity) == "" || identity != strings.TrimSpace(identity) {
		return "", fmt.Errorf("runtime-tuning target identity is unavailable")
	}
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:]), nil
}

func stage4RuntimeTuningSessionsByAge(
	receipts []Stage4RuntimeTuningSessionReceipt,
) {
	sort.Slice(receipts, func(left, right int) bool {
		if !receipts[left].Session.StartedAt.Equal(receipts[right].Session.StartedAt) {
			return receipts[left].Session.StartedAt.Before(receipts[right].Session.StartedAt)
		}
		return receipts[left].Session.SessionID < receipts[right].Session.SessionID
	})
}

func stage4RuntimeTuningHistoryBackendIsNil(
	backend Stage4RuntimeTuningHistoryBackend,
) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func stage4RuntimeTuningHistoryResolverIsNil(
	resolver Stage4RuntimeTuningHistoryResolver,
) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ Stage4RuntimeTuningHistoryBackend = SQLiteStore{}
