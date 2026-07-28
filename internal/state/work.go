package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

var (
	ErrUnknownWork     = errors.New("unknown migration work")
	ErrTopologyChanged = errors.New("pagination topology changed")
	ErrRangeOrder      = errors.New("invalid range acknowledgement order")
	ErrAmbiguousLegacy = errors.New("ambiguous legacy progress")
)

type TaskKey struct {
	Type      string `json:"type" yaml:"type"`
	Schema    string `json:"schema,omitempty" yaml:"schema,omitempty"`
	Table     string `json:"table" yaml:"table"`
	Partition string `json:"partition,omitempty" yaml:"partition,omitempty"`
}

func (key TaskKey) Validate() error {
	if key.Type == "" || key.Table == "" {
		return fmt.Errorf("task type and table are required")
	}
	return nil
}

func (key TaskKey) canonical() (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("encode task key: %w", err)
	}
	return string(encoded), nil
}

type ValueKind string

const (
	ValueInt64 ValueKind = "int64"
	ValueText  ValueKind = "text"
	ValueBytes ValueKind = "bytes"
	ValueNull  ValueKind = "null"
)

// TypedValue preserves key values without a floating-point intermediate.
type TypedValue struct {
	Kind    ValueKind `json:"kind" yaml:"kind"`
	Encoded string    `json:"encoded,omitempty" yaml:"encoded,omitempty"`
}

func Int64Value(value int64) TypedValue {
	return TypedValue{Kind: ValueInt64, Encoded: strconv.FormatInt(value, 10)}
}

func TextValue(value string) TypedValue {
	return TypedValue{Kind: ValueText, Encoded: value}
}

func BytesValue(value []byte) TypedValue {
	return TypedValue{Kind: ValueBytes, Encoded: base64.StdEncoding.EncodeToString(value)}
}

func (value TypedValue) Validate() error {
	switch value.Kind {
	case ValueInt64:
		if _, err := strconv.ParseInt(value.Encoded, 10, 64); err != nil {
			return fmt.Errorf("invalid int64 value: %w", err)
		}
	case ValueText:
	case ValueBytes:
		if _, err := base64.StdEncoding.DecodeString(value.Encoded); err != nil {
			return fmt.Errorf("invalid byte value: %w", err)
		}
	case ValueNull:
		if value.Encoded != "" {
			return fmt.Errorf("null value must not carry data")
		}
	default:
		return fmt.Errorf("unknown typed value kind %q", value.Kind)
	}
	return nil
}

func (value TypedValue) SQLValue() (any, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	switch value.Kind {
	case ValueInt64:
		return strconv.ParseInt(value.Encoded, 10, 64)
	case ValueText:
		return value.Encoded, nil
	case ValueBytes:
		return base64.StdEncoding.DecodeString(value.Encoded)
	case ValueNull:
		return nil, nil
	default:
		panic("validated typed value has unsupported kind")
	}
}

type TypedTuple []TypedValue

func (tuple TypedTuple) Validate(allowNull bool) error {
	if len(tuple) == 0 {
		return fmt.Errorf("typed tuple must not be empty")
	}
	for index, value := range tuple {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("tuple value %d: %w", index, err)
		}
		if value.Kind == ValueNull && !allowNull {
			return fmt.Errorf("tuple value %d: NULL ordering is not proven safe", index)
		}
	}
	return nil
}

type PendingAcknowledgement struct {
	Sequence      uint64     `json:"sequence" yaml:"sequence"`
	ChunkRows     int64      `json:"chunk_rows" yaml:"chunk_rows"`
	DurableRows   int64      `json:"durable_rows" yaml:"durable_rows"`
	Attempts      int        `json:"attempts" yaml:"attempts"`
	Frontier      TypedTuple `json:"frontier,omitempty" yaml:"frontier,omitempty"`
	FrontierValid bool       `json:"frontier_valid,omitempty" yaml:"frontier_valid,omitempty"`
}

type WorkTask struct {
	RunID        string    `json:"run_id" yaml:"run_id"`
	Key          TaskKey   `json:"key" yaml:"key"`
	Status       string    `json:"status" yaml:"status"`
	Strategy     string    `json:"strategy" yaml:"strategy"`
	TopologyHash string    `json:"topology_hash" yaml:"topology_hash"`
	Attempts     int       `json:"attempts" yaml:"attempts"`
	Retries      int       `json:"retries" yaml:"retries"`
	Error        string    `json:"error,omitempty" yaml:"error,omitempty"`
	StartedAt    time.Time `json:"started_at" yaml:"started_at"`
	UpdatedAt    time.Time `json:"updated_at" yaml:"updated_at"`
	CompletedAt  time.Time `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
}

type RangeState struct {
	RunID           string                   `json:"run_id" yaml:"run_id"`
	Task            TaskKey                  `json:"task" yaml:"task"`
	ID              string                   `json:"id" yaml:"id"`
	Strategy        string                   `json:"strategy" yaml:"strategy"`
	TopologyHash    string                   `json:"topology_hash" yaml:"topology_hash"`
	Lower           TypedTuple               `json:"lower,omitempty" yaml:"lower,omitempty"`
	Upper           TypedTuple               `json:"upper,omitempty" yaml:"upper,omitempty"`
	LowerInclusive  bool                     `json:"lower_inclusive" yaml:"lower_inclusive"`
	UpperInclusive  bool                     `json:"upper_inclusive" yaml:"upper_inclusive"`
	FirstRow        int64                    `json:"first_row,omitempty" yaml:"first_row,omitempty"`
	LastRow         int64                    `json:"last_row,omitempty" yaml:"last_row,omitempty"`
	Frontier        TypedTuple               `json:"frontier,omitempty" yaml:"frontier,omitempty"`
	FrontierValid   bool                     `json:"frontier_valid" yaml:"frontier_valid"`
	NextSequence    uint64                   `json:"next_sequence" yaml:"next_sequence"`
	SequenceOffset  int64                    `json:"sequence_offset" yaml:"sequence_offset"`
	RowsDone        int64                    `json:"rows_done" yaml:"rows_done"`
	RowsTotal       int64                    `json:"rows_total" yaml:"rows_total"`
	CommittedPrefix int64                    `json:"committed_prefix" yaml:"committed_prefix"`
	Attempts        int                      `json:"attempts" yaml:"attempts"`
	Retries         int                      `json:"retries" yaml:"retries"`
	Status          string                   `json:"status" yaml:"status"`
	Error           string                   `json:"error,omitempty" yaml:"error,omitempty"`
	UpdatedAt       time.Time                `json:"updated_at" yaml:"updated_at"`
	CompletedAt     time.Time                `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
	Pending         []PendingAcknowledgement `json:"pending,omitempty" yaml:"pending,omitempty"`
}

type RangeAcknowledgement struct {
	RunID         string
	Task          TaskKey
	RangeID       string
	TopologyHash  string
	Sequence      uint64
	ChunkRows     int64
	AttemptOffset int64
	DurableRows   int64
	Frontier      TypedTuple
	FrontierValid bool
	At            time.Time
}

// RangeChunkIntent is persisted before its associated target mutation. If the
// process stops after commit but before acknowledgement, resume can select the
// insert-only replay path for this exact sequence.
type RangeChunkIntent struct {
	RunID         string
	Task          TaskKey
	RangeID       string
	TopologyHash  string
	Sequence      uint64
	ChunkRows     int64
	EndFrontier   TypedTuple
	FrontierValid bool
	At            time.Time
}

// RangeAttempt authorizes one target-driver invocation for an unresolved
// durable range intent. Recording the authorization before the driver call
// makes retry accounting survive a process stop, even if the stop happens
// before the driver is entered.
type RangeAttempt struct {
	RunID        string
	Task         TaskKey
	RangeID      string
	TopologyHash string
	Sequence     uint64
	At           time.Time
}

// RangeBackend is the Stage 2 restartability surface implemented by both
// durable state formats.
type RangeBackend interface {
	EnsureWorkPlan(WorkTask, []RangeState) (bool, error)
	ResetWorkPlan(WorkTask, []RangeState) error
	ListWork(string) ([]WorkTask, []RangeState, error)
	BeginRangeChunk(RangeChunkIntent) error
	RecordRangeAttempt(RangeAttempt) error
	AcknowledgeRange(RangeAcknowledgement) (RangeState, error)
	CompleteRange(string, TaskKey, string, string, uint64, time.Time) error
	CompleteWorkTask(string, TaskKey, string, time.Time) error
}

var (
	_ RangeBackend = SQLiteStore{}
	_ RangeBackend = YAMLStore{}
)

func validateWorkPlan(task WorkTask, ranges []RangeState) (WorkTask, []RangeState, error) {
	if task.RunID == "" {
		return WorkTask{}, nil, fmt.Errorf("work run ID is required")
	}
	if err := task.Key.Validate(); err != nil {
		return WorkTask{}, nil, err
	}
	if task.Strategy == "" || task.TopologyHash == "" {
		return WorkTask{}, nil, fmt.Errorf("work strategy and topology hash are required")
	}
	if len(ranges) == 0 {
		return WorkTask{}, nil, fmt.Errorf("work plan requires at least one range")
	}
	if task.Status == "" {
		task.Status = "running"
	}
	if task.Status != "running" {
		return WorkTask{}, nil, fmt.Errorf("new work task must be running")
	}
	now := task.StartedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	task.StartedAt, task.UpdatedAt = now.UTC(), now.UTC()
	seen := make(map[string]bool, len(ranges))
	validated := make([]RangeState, len(ranges))
	for index, workRange := range ranges {
		if workRange.ID == "" || seen[workRange.ID] {
			return WorkTask{}, nil, fmt.Errorf("range identifiers must be non-empty and unique")
		}
		seen[workRange.ID] = true
		if workRange.RunID != "" && workRange.RunID != task.RunID {
			return WorkTask{}, nil, fmt.Errorf("range %s has mismatched run ID", workRange.ID)
		}
		if workRange.Task != (TaskKey{}) && workRange.Task != task.Key {
			return WorkTask{}, nil, fmt.Errorf("range %s has mismatched task", workRange.ID)
		}
		if workRange.TopologyHash != "" && workRange.TopologyHash != task.TopologyHash {
			return WorkTask{}, nil, fmt.Errorf("range %s has mismatched topology", workRange.ID)
		}
		if workRange.Strategy != "" && workRange.Strategy != task.Strategy {
			return WorkTask{}, nil, fmt.Errorf("range %s has mismatched strategy", workRange.ID)
		}
		if len(workRange.Lower) > 0 {
			if err := workRange.Lower.Validate(false); err != nil {
				return WorkTask{}, nil, fmt.Errorf("range %s lower bound: %w", workRange.ID, err)
			}
		}
		if len(workRange.Upper) > 0 {
			if err := workRange.Upper.Validate(false); err != nil {
				return WorkTask{}, nil, fmt.Errorf("range %s upper bound: %w", workRange.ID, err)
			}
		}
		workRange.RunID = task.RunID
		workRange.Task = task.Key
		workRange.TopologyHash = task.TopologyHash
		workRange.Strategy = task.Strategy
		workRange.Status = "running"
		workRange.UpdatedAt = now.UTC()
		validated[index] = workRange
	}
	sort.Slice(validated, func(left, right int) bool { return validated[left].ID < validated[right].ID })
	return task, validated, nil
}

func applyRangeChunkIntent(workRange RangeState, intent RangeChunkIntent) (RangeState, error) {
	if workRange.Status != "running" {
		return RangeState{}, fmt.Errorf("%w: range %q is %s", ErrUnknownWork, workRange.ID, workRange.Status)
	}
	if intent.TopologyHash != workRange.TopologyHash {
		return RangeState{}, fmt.Errorf("%w: range %q", ErrTopologyChanged, workRange.ID)
	}
	if intent.ChunkRows <= 0 || intent.Sequence < workRange.NextSequence {
		return RangeState{}, fmt.Errorf("%w: invalid issued sequence", ErrRangeOrder)
	}
	if intent.FrontierValid {
		if err := intent.EndFrontier.Validate(false); err != nil {
			return RangeState{}, fmt.Errorf("%w: invalid issued frontier: %v", ErrRangeOrder, err)
		}
	}
	for _, pending := range workRange.Pending {
		if pending.Sequence != intent.Sequence {
			continue
		}
		if pending.ChunkRows == intent.ChunkRows && pending.DurableRows == 0 &&
			pending.FrontierValid == intent.FrontierValid && typedTupleEqual(pending.Frontier, intent.EndFrontier) {
			return workRange, nil
		}
		return RangeState{}, fmt.Errorf("%w: sequence %d was already issued differently", ErrRangeOrder, intent.Sequence)
	}
	workRange.Pending = append(workRange.Pending, PendingAcknowledgement{
		Sequence:      intent.Sequence,
		ChunkRows:     intent.ChunkRows,
		Frontier:      append(TypedTuple(nil), intent.EndFrontier...),
		FrontierValid: intent.FrontierValid,
	})
	sort.Slice(workRange.Pending, func(left, right int) bool {
		return workRange.Pending[left].Sequence < workRange.Pending[right].Sequence
	})
	if intent.At.IsZero() {
		intent.At = time.Now().UTC()
	}
	workRange.UpdatedAt = intent.At.UTC()
	return workRange, nil
}

func applyRangeAttempt(workTask WorkTask, workRange RangeState, attempt RangeAttempt) (WorkTask, RangeState, error) {
	if workTask.Status != "running" {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: task %q is %s", ErrUnknownWork, workTask.Key.Table, workTask.Status)
	}
	if workRange.Status != "running" {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: range %q is %s", ErrUnknownWork, workRange.ID, workRange.Status)
	}
	if attempt.TopologyHash != workTask.TopologyHash || attempt.TopologyHash != workRange.TopologyHash {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: range %q", ErrTopologyChanged, workRange.ID)
	}
	if attempt.Sequence < workRange.NextSequence {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: sequence %d is behind %d", ErrRangeOrder, attempt.Sequence, workRange.NextSequence)
	}
	pendingIndex := -1
	for index := range workRange.Pending {
		if workRange.Pending[index].Sequence == attempt.Sequence {
			pendingIndex = index
			break
		}
	}
	if pendingIndex < 0 {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: sequence %d has no pending intent", ErrRangeOrder, attempt.Sequence)
	}
	pending := &workRange.Pending[pendingIndex]
	if pending.ChunkRows <= 0 || pending.DurableRows < 0 || pending.DurableRows >= pending.ChunkRows {
		return WorkTask{}, RangeState{}, fmt.Errorf("%w: sequence %d is already resolved", ErrRangeOrder, attempt.Sequence)
	}
	retry := pending.Attempts > 0
	pending.Attempts++
	workRange.Attempts++
	workTask.Attempts++
	if retry {
		workRange.Retries++
		workTask.Retries++
	}
	if attempt.At.IsZero() {
		attempt.At = time.Now().UTC()
	}
	workTask.UpdatedAt = attempt.At.UTC()
	workRange.UpdatedAt = attempt.At.UTC()
	return workTask, workRange, nil
}

func applyRangeAcknowledgement(workRange RangeState, acknowledgement RangeAcknowledgement) (RangeState, error) {
	if workRange.Status != "running" {
		return RangeState{}, fmt.Errorf("%w: range %q is %s", ErrUnknownWork, workRange.ID, workRange.Status)
	}
	if acknowledgement.TopologyHash != workRange.TopologyHash {
		return RangeState{}, fmt.Errorf("%w: range %q", ErrTopologyChanged, workRange.ID)
	}
	if acknowledgement.ChunkRows <= 0 || acknowledgement.AttemptOffset < 0 || acknowledgement.DurableRows <= 0 ||
		acknowledgement.AttemptOffset+acknowledgement.DurableRows > acknowledgement.ChunkRows {
		return RangeState{}, fmt.Errorf("%w: invalid durable prefix", ErrRangeOrder)
	}
	if acknowledgement.FrontierValid {
		if err := acknowledgement.Frontier.Validate(false); err != nil {
			return RangeState{}, fmt.Errorf("%w: invalid frontier: %v", ErrRangeOrder, err)
		}
	}
	if acknowledgement.Sequence < workRange.NextSequence {
		return RangeState{}, fmt.Errorf("%w: sequence %d is behind %d", ErrRangeOrder, acknowledgement.Sequence, workRange.NextSequence)
	}
	index := -1
	for candidate := range workRange.Pending {
		if workRange.Pending[candidate].Sequence == acknowledgement.Sequence {
			index = candidate
			break
		}
	}
	if index < 0 {
		return RangeState{}, fmt.Errorf("%w: sequence %d has no pending intent", ErrRangeOrder, acknowledgement.Sequence)
	}
	pending := &workRange.Pending[index]
	if pending.Attempts <= 0 {
		return RangeState{}, fmt.Errorf("%w: sequence %d has no authorized target attempt", ErrRangeOrder, acknowledgement.Sequence)
	}
	if pending.ChunkRows != acknowledgement.ChunkRows || pending.DurableRows != acknowledgement.AttemptOffset {
		return RangeState{}, fmt.Errorf("%w: sequence %d expected offset %d", ErrRangeOrder, acknowledgement.Sequence, pending.DurableRows)
	}
	pending.DurableRows += acknowledgement.DurableRows
	pending.Frontier = acknowledgement.Frontier
	pending.FrontierValid = acknowledgement.FrontierValid
	sort.Slice(workRange.Pending, func(left, right int) bool {
		return workRange.Pending[left].Sequence < workRange.Pending[right].Sequence
	})
	for len(workRange.Pending) > 0 {
		pending := workRange.Pending[0]
		if pending.Sequence != workRange.NextSequence {
			break
		}
		if pending.DurableRows < workRange.SequenceOffset {
			return RangeState{}, fmt.Errorf("%w: durable offset regressed", ErrRangeOrder)
		}
		workRange.RowsDone += pending.DurableRows - workRange.SequenceOffset
		workRange.CommittedPrefix = pending.DurableRows
		workRange.SequenceOffset = pending.DurableRows
		if pending.FrontierValid {
			workRange.Frontier = append(TypedTuple(nil), pending.Frontier...)
			workRange.FrontierValid = true
		}
		if pending.DurableRows != pending.ChunkRows {
			break
		}
		workRange.NextSequence++
		workRange.SequenceOffset = 0
		workRange.CommittedPrefix = 0
		workRange.Pending = workRange.Pending[1:]
	}
	if acknowledgement.At.IsZero() {
		acknowledgement.At = time.Now().UTC()
	}
	workRange.UpdatedAt = acknowledgement.At.UTC()
	return workRange, nil
}

func workPlanEqual(leftTask WorkTask, leftRanges []RangeState, rightTask WorkTask, rightRanges []RangeState) bool {
	if leftTask.Strategy != rightTask.Strategy || leftTask.TopologyHash != rightTask.TopologyHash || leftTask.Key != rightTask.Key {
		return false
	}
	if len(leftRanges) != len(rightRanges) {
		return false
	}
	for index := range leftRanges {
		left, right := leftRanges[index], rightRanges[index]
		if left.ID != right.ID || left.Strategy != right.Strategy || left.TopologyHash != right.TopologyHash ||
			left.LowerInclusive != right.LowerInclusive || left.UpperInclusive != right.UpperInclusive ||
			left.FirstRow != right.FirstRow || left.LastRow != right.LastRow ||
			!typedTupleEqual(left.Lower, right.Lower) || !typedTupleEqual(left.Upper, right.Upper) {
			return false
		}
	}
	return true
}

func typedTupleEqual(left, right TypedTuple) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
