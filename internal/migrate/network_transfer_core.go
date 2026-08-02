package migrate

import (
	"context"
	"errors"

	"github.com/johndauphine/dmtx/internal/config"
)

var (
	ErrInvalidNetworkTransferPlan = errors.New(
		"invalid resumable network transfer plan",
	)
	ErrInvalidNetworkPage = errors.New(
		"invalid resumable network source page",
	)
	ErrInvalidNetworkRestore = errors.New(
		"invalid resumable network range restore",
	)
	ErrUnknownNetworkCommit = errors.New(
		"network target commit outcome is unknown",
	)
)

// NetworkWriteProtocolLimitError is a fixed, secret-free adapter signal that a
// target rejected a write because the attempted batch exceeded a proven
// protocol limit. Adapters must return this signal only with a
// CommitNotCommitted receipt. The core can then apply a smaller runtime-tuning
// boundary before retrying without guessing from driver text.
type NetworkWriteProtocolLimitError struct{}

func (*NetworkWriteProtocolLimitError) Error() string {
	return "network target write exceeded a protocol limit"
}

const (
	maximumNetworkFrontierBytes       = 64 << 10
	maximumNetworkFactToken           = 128
	maximumNetworkRetries             = 1 << 16
	maximumNetworkCheckpointFrequency = config.MaxTransferChunkRows
)

// NetworkReplayMode is an adapter capability promise. Rebuild routes must
// prove an insert-only duplicate-safe replay path; upsert routes must prove
// that replay is idempotent without overwriting rows outside the source page.
type NetworkReplayMode string

const (
	NetworkReplayDuplicateSafeInsertOnly NetworkReplayMode = "duplicate_safe_insert_only"
	NetworkReplayIdempotentUpsert        NetworkReplayMode = "idempotent_upsert"
)

// NetworkWriteMode tells a target callback which independently proven write
// path it must use for this attempt.
type NetworkWriteMode string

const (
	NetworkWriteFreshInsert             NetworkWriteMode = "fresh_insert"
	NetworkWriteDuplicateSafeInsertOnly NetworkWriteMode = "duplicate_safe_insert_only"
	NetworkWriteIdempotentUpsert        NetworkWriteMode = "idempotent_upsert"
)

// NetworkRangePlan is one immutable migration-global range. RangeIndex is
// contiguous across every selected table and is the durable identity used by
// state and runtime tuning. Bounds remain adapter-owned; the core carries only
// their credential-free topology hash and opaque durable frontier.
type NetworkRangePlan struct {
	RangeIndex   uint64
	TableSchema  string
	TableName    string
	TopologyHash string
	Pagination   PaginationStrategy
	MaxRowBytes  int64
}

// NetworkIssuedChunk is recorded durably before the first target mutation.
// StartFrontier and EndFrontier are opaque typed state encodings and are never
// copied into status or retry facts. Fingerprint is a stable canonical digest
// of the complete page, not raw row data.
type NetworkIssuedChunk struct {
	RangeIndex    uint64
	Sequence      uint64
	Rows          int
	StartFrontier []byte
	EndFrontier   []byte
	Fingerprint   string
	Exhausted     bool
}

// NetworkRangeRestore is the exact durable work set for one planned range.
// RowsDone counts complete sequences before NextSequence. SequenceOffset is
// the known durable prefix inside the first issued NextSequence chunk.
type NetworkRangeRestore struct {
	RangeIndex     uint64
	TopologyHash   string
	NextSequence   uint64
	SequenceOffset int64
	RowsDone       int64
	Frontier       []byte
	Complete       bool
	Issued         []NetworkIssuedChunk
}

// NetworkReadRequest bounds one source callback. The core reserves
// MaxRows*MaxRowBytes from the migration-wide ByteBudget before invoking the
// callback, so source implementations may materialize at most MaxRows owned
// rows. ReplayExpected is non-nil only for a durably issued chunk.
type NetworkReadRequest struct {
	Range          NetworkRangePlan
	Sequence       uint64
	Attempt        int
	MaxRows        int
	StartFrontier  []byte
	ReplayExpected *NetworkIssuedChunk
}

// NetworkReadPage transfers exclusive ownership of Rows and every referenced
// payload backing buffer to the core. ReadPage must not retain or mutate them
// after returning. RowBytes is the exact retained in-memory size of each row
// and RetainedBytes is their exact sum. A non-empty page must advance
// EndFrontier and carry a canonical digest.
type NetworkReadPage struct {
	Rows          [][]any
	RowBytes      []int64
	RetainedBytes int64
	EndFrontier   []byte
	Fingerprint   string
	Exhausted     bool
}

// NetworkWriteRequest is one bounded target callback. AttemptOffset is
// relative to the complete issued chunk; Rows is a borrowed, read-only view of
// the core-owned page containing only the suffix/subset represented by this
// attempt. WritePage must not mutate or retain Rows or any referenced payload
// backing buffer after returning.
type NetworkWriteRequest struct {
	Range         NetworkRangePlan
	Sequence      uint64
	Attempt       uint32
	AttemptOffset int64
	Mode          NetworkWriteMode
	Rows          [][]any
}

// NetworkRangeCheckpoint contains only the lowest contiguous durable frontier
// for one range. FrontierBytes remains at the prior complete page while
// Frontier.SequenceOffset is non-zero.
type NetworkRangeCheckpoint struct {
	RangeIndex    uint64
	TopologyHash  string
	Frontier      AckFrontier
	FrontierBytes []byte
	Complete      bool
	Memory        ByteBudgetStats
}

// NetworkTransferCallbacks are deliberately engine-neutral. ReadPage must
// close any driver cursor before returning and transfers exclusive ownership
// of its page payload to the core. WritePage receives only a borrowed,
// immutable view for the duration of the call. RecordIssued and Checkpoint must
// be durable when they return nil; the core serializes those state callbacks.
type NetworkTransferCallbacks struct {
	ReadPage     func(context.Context, NetworkReadRequest) (NetworkReadPage, error)
	WritePage    func(context.Context, NetworkWriteRequest) (WriteReceipt, error)
	RecordIssued func(context.Context, NetworkIssuedChunk) error
	Checkpoint   func(context.Context, NetworkRangeCheckpoint) error
}

// RuntimeTuningDecisionSink durably records one controller decision before the
// transfer can proceed to subsequent work. It receives only bounded,
// credential-free controller facts; source frontiers, row values, and driver
// errors remain outside this surface.
type RuntimeTuningDecisionSink interface {
	PersistRuntimeTuningDecision(
		context.Context,
		RuntimeTuningSnapshot,
		RuntimeTuningDecision,
	) error
}

// NetworkTransferPlan is immutable input for one migration-wide transfer.
// RuntimeTuning may be nil when runtime adjustment is disabled. When present,
// its immutable intent and range/row-width evidence must match this plan.
type NetworkTransferPlan struct {
	SourceEngine string
	TargetEngine string
	Resources    config.EffectiveTransferPlan
	RetryPolicy  RetryPolicy
	ReplayMode   NetworkReplayMode
	// UpsertMergeRows is an immutable write-only ceiling for an admitted
	// idempotent-upsert route. It deliberately does not affect source page
	// requests or issued-page identities: one issued page may be acknowledged
	// through several contiguous durable write prefixes. Zero preserves the
	// legacy one-write-per-live-source-page behavior.
	UpsertMergeRows int
	// CheckpointFrequency is the number of contiguous durable write
	// acknowledgements per range between periodic state checkpoints. Zero
	// preserves the legacy immediate-on-ack cadence. A final range checkpoint
	// is always synchronous regardless of this value.
	CheckpointFrequency int
	Ranges              []NetworkRangePlan
	Restores            []NetworkRangeRestore
	RuntimeTuning       *RuntimeTuningController
	RuntimeTuningSink   RuntimeTuningDecisionSink
	RowWidth            RuntimeRowWidthEvidence
}

// NetworkPaginationFact is stable, bounded, and contains no frontier or row
// values.
type NetworkPaginationFact struct {
	RangeIndex   uint64
	TableSchema  string
	TableName    string
	TopologyHash string
	Pagination   PaginationStrategy
}

// NetworkRetryOperation distinguishes source reads from target writes without
// exposing SQL text or driver messages.
type NetworkRetryOperation string

const (
	NetworkRetryRead  NetworkRetryOperation = "read"
	NetworkRetryWrite NetworkRetryOperation = "write"
)

// NetworkRetryFact pairs a secret-free engine classification with its stable
// work identity.
type NetworkRetryFact struct {
	RangeIndex uint64
	Sequence   uint64
	Attempt    uint32
	Operation  NetworkRetryOperation
	Fact       EngineRetryFact
}

// NetworkTransferResult reports only safely checkpointed work.
type NetworkTransferResult struct {
	Rows             int64
	CompletedRanges  int
	Pagination       []NetworkPaginationFact
	Retries          []NetworkRetryFact
	Memory           ByteBudgetStats
	HasRuntimeTuning bool
	RuntimeTuning    RuntimeTuningSnapshot
}
