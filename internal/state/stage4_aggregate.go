package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Stage4AggregateBackend publishes the final evidence for a table or run in
// one backend mutation. It is deliberately separate from Stage4Backend so
// integrations that have not adopted aggregate publication keep compiling.
type Stage4AggregateBackend interface {
	EnsureStage4TableInventory(Stage4TableInventory) error
	CompleteStage4Table(Stage4TableCompletion) error
	CompleteStage4Run(Stage4RunCompletion) error

	// LoadStage4TableInventory and LoadStage4TableCompletions read back the
	// durable aggregate evidence a run publication must reproduce exactly.
	// A process that did not publish a receipt itself — every resume — has no
	// other way to recover the byte-identical completion it must supply.
	LoadStage4TableInventory(string) (Stage4TableInventoryReceipt, bool, error)
	LoadStage4TableCompletions(string) ([]Stage4TableCompletionReceipt, error)
}

// Stage4RangeCompletion identifies the exact acknowledged range frontier that
// may be made terminal by aggregate table completion.
type Stage4RangeCompletion struct {
	ID           string `json:"id" yaml:"id"`
	NextSequence uint64 `json:"next_sequence" yaml:"next_sequence"`
}

// Stage4TableCompletion atomically reconciles the legacy table checkpoint,
// structured work/ranges, and an optional incremental watermark publication.
type Stage4TableCompletion struct {
	RunID        string                  `json:"run_id" yaml:"run_id"`
	Table        string                  `json:"table" yaml:"table"`
	Task         TaskKey                 `json:"task" yaml:"task"`
	TopologyHash string                  `json:"topology_hash" yaml:"topology_hash"`
	Ranges       []Stage4RangeCompletion `json:"ranges" yaml:"ranges"`
	RowsDone     int                     `json:"rows_done" yaml:"rows_done"`
	Incremental  *IncrementalCommit      `json:"incremental,omitempty" yaml:"incremental,omitempty"`
	CompletedAt  time.Time               `json:"completed_at" yaml:"completed_at"`
}

// Stage4TableCompletionReceipt is the immutable proof that every table-level
// terminal field was published by one aggregate backend mutation.
type Stage4TableCompletionReceipt struct {
	Completion Stage4TableCompletion `json:"completion" yaml:"completion"`
	Digest     string                `json:"digest" yaml:"digest"`
}

// Stage4TableInventory is the immutable pre-mutation authority for the exact
// selected table set. Calling EnsureStage4TableInventory with an empty Tables
// slice is the durable proof of a deliberate zero-table migration.
type Stage4TableInventory struct {
	RunID                string                      `json:"run_id" yaml:"run_id"`
	SchemaTask           TaskKey                     `json:"schema_task" yaml:"schema_task"`
	SchemaTopologyHash   string                      `json:"schema_topology_hash" yaml:"schema_topology_hash"`
	SchemaSnapshotDigest string                      `json:"schema_snapshot_digest" yaml:"schema_snapshot_digest"`
	Tables               []Stage4TableInventoryEntry `json:"tables" yaml:"tables"`
}

type Stage4TableInventoryEntry struct {
	Table        string                 `json:"table" yaml:"table"`
	Task         TaskKey                `json:"task" yaml:"task"`
	Strategy     string                 `json:"strategy" yaml:"strategy"`
	TopologyHash string                 `json:"topology_hash" yaml:"topology_hash"`
	Ranges       []Stage4InventoryRange `json:"ranges" yaml:"ranges"`
}

// Stage4InventoryRange contains only immutable plan identity. Mutable
// acknowledgement frontiers belong to Stage4TableCompletion, not the
// pre-mutation inventory receipt.
type Stage4InventoryRange struct {
	ID string `json:"id" yaml:"id"`
}

type Stage4TableInventoryReceipt struct {
	Inventory Stage4TableInventory `json:"inventory" yaml:"inventory"`
	Digest    string               `json:"digest" yaml:"digest"`
}

// Stage4SentinelCompletion binds a schema or target-shape work sentinel to the
// exact immutable snapshot that authorizes its completion.
type Stage4SentinelCompletion struct {
	Task         TaskKey        `json:"task" yaml:"task"`
	RangeID      string         `json:"range_id" yaml:"range_id"`
	TopologyHash string         `json:"topology_hash" yaml:"topology_hash"`
	NextSequence uint64         `json:"next_sequence" yaml:"next_sequence"`
	Snapshot     SchemaSnapshot `json:"snapshot" yaml:"snapshot"`
}

// Stage4RunCompletion atomically completes all supplied schema sentinels and
// publishes the existing running migration as successful.
type Stage4RunCompletion struct {
	RunID       string                     `json:"run_id" yaml:"run_id"`
	Tables      []Stage4TableCompletion    `json:"tables,omitempty" yaml:"tables,omitempty"`
	Sentinels   []Stage4SentinelCompletion `json:"sentinels" yaml:"sentinels"`
	Reason      string                     `json:"reason" yaml:"reason"`
	CompletedAt time.Time                  `json:"completed_at" yaml:"completed_at"`
}

var (
	stage4SchemaContractSentinelTask = TaskKey{
		Type: "schema-contract", Table: "aggregate-source-schema",
	}
	stage4TargetShapeSentinelTask = TaskKey{
		Type: "target-schema-shape", Table: "aggregate-target-schema",
	}
)

var (
	stage4BeforeAggregateTableCommit = func() error { return nil }
	stage4BeforeAggregateRunCommit   = func() error { return nil }
	stage4BeforeInventoryCommit      = func() error { return nil }
)

func stage4AggregateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(
		"%w: Stage 4 %s failed; repair durable state and resume: %w",
		ErrState,
		operation,
		err,
	)
}

func normalizeStage4TableInventory(
	inventory Stage4TableInventory,
) (Stage4TableInventory, error) {
	if strings.TrimSpace(inventory.RunID) == "" {
		return Stage4TableInventory{}, fmt.Errorf("inventory run ID is required")
	}
	if inventory.SchemaTask != stage4SchemaContractSentinelTask ||
		strings.TrimSpace(inventory.SchemaTopologyHash) == "" {
		return Stage4TableInventory{}, fmt.Errorf(
			"%w: inventory schema authority is invalid",
			ErrImmutableEvidence,
		)
	}
	decodedDigest, err := hex.DecodeString(inventory.SchemaSnapshotDigest)
	if err != nil || len(decodedDigest) != sha256.Size {
		return Stage4TableInventory{}, fmt.Errorf(
			"inventory schema snapshot digest must be a SHA-256 digest",
		)
	}
	inventory.SchemaSnapshotDigest =
		strings.ToLower(inventory.SchemaSnapshotDigest)
	tables := make([]Stage4TableInventoryEntry, len(inventory.Tables))
	copy(tables, inventory.Tables)
	inventory.Tables = tables
	for index := range inventory.Tables {
		entry := &inventory.Tables[index]
		if err := entry.Task.Validate(); err != nil {
			return Stage4TableInventory{}, fmt.Errorf(
				"table inventory entry %d task: %w",
				index,
				err,
			)
		}
		if strings.TrimSpace(entry.Table) == "" ||
			entry.Task.Table != entry.Table ||
			entry.Task.Partition != "" ||
			!stage4TableWorkType(entry.Task.Type) ||
			strings.TrimSpace(entry.Strategy) == "" ||
			strings.TrimSpace(entry.TopologyHash) == "" ||
			len(entry.Ranges) == 0 {
			return Stage4TableInventory{}, fmt.Errorf(
				"%w: table inventory entry %d is invalid",
				ErrImmutableEvidence,
				index,
			)
		}
		entry.Ranges = append([]Stage4InventoryRange(nil), entry.Ranges...)
		sort.Slice(entry.Ranges, func(left, right int) bool {
			return entry.Ranges[left].ID < entry.Ranges[right].ID
		})
		for rangeIndex, workRange := range entry.Ranges {
			if strings.TrimSpace(workRange.ID) == "" ||
				rangeIndex > 0 &&
					entry.Ranges[rangeIndex-1].ID == workRange.ID {
				return Stage4TableInventory{}, fmt.Errorf(
					"%w: table inventory range identity is invalid",
					ErrImmutableEvidence,
				)
			}
		}
	}
	sort.Slice(inventory.Tables, func(left, right int) bool {
		return inventory.Tables[left].Table < inventory.Tables[right].Table
	})
	seenTasks := make(map[TaskKey]struct{}, len(inventory.Tables))
	for index, entry := range inventory.Tables {
		if index > 0 &&
			inventory.Tables[index-1].Table == entry.Table {
			return Stage4TableInventory{}, fmt.Errorf(
				"%w: duplicate inventory table %q",
				ErrImmutableEvidence,
				entry.Table,
			)
		}
		if _, duplicate := seenTasks[entry.Task]; duplicate {
			return Stage4TableInventory{}, fmt.Errorf(
				"%w: duplicate inventory task %#v",
				ErrImmutableEvidence,
				entry.Task,
			)
		}
		seenTasks[entry.Task] = struct{}{}
	}
	return inventory, nil
}

// validateStage4InventoryRevision permits replacing a table inventory that no
// table has yet built terminal evidence on. A resumed run may legitimately
// replan its range set — a source that grew during an outage yields a different
// partition count — and freezing the first plan forever would make such a run
// unrecoverable rather than merely failed.
//
// The revision window is deliberately narrow. The schema authority is immutable
// across it, and it closes permanently the moment any table publishes an
// aggregate receipt or completes its ordinary checkpoint, because from then on
// the inventory is the authority those terminal records were validated against.
func validateStage4InventoryRevision(
	stored Stage4TableInventoryReceipt,
	next Stage4TableInventory,
	receipts int,
	completedTables int,
) error {
	receipt, err := normalizeStoredStage4TableInventory(stored)
	if err != nil {
		return err
	}
	if receipts != 0 || completedTables != 0 {
		return fmt.Errorf(
			"%w: Stage 4 table inventory is fixed once a table published terminal evidence",
			ErrImmutableEvidence,
		)
	}
	current := receipt.Inventory
	if current.RunID != next.RunID ||
		current.SchemaTask != next.SchemaTask ||
		current.SchemaTopologyHash != next.SchemaTopologyHash ||
		current.SchemaSnapshotDigest != next.SchemaSnapshotDigest {
		return fmt.Errorf(
			"%w: Stage 4 table inventory schema authority cannot be revised",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func newStage4TableInventoryReceipt(
	inventory Stage4TableInventory,
) (Stage4TableInventoryReceipt, error) {
	encoded, err := json.Marshal(inventory)
	if err != nil {
		return Stage4TableInventoryReceipt{}, fmt.Errorf(
			"encode Stage 4 table inventory: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return Stage4TableInventoryReceipt{
		Inventory: inventory,
		Digest:    hex.EncodeToString(digest[:]),
	}, nil
}

func validateStage4TableInventoryReceipt(
	stored Stage4TableInventoryReceipt,
	expected Stage4TableInventory,
) error {
	canonical, err := newStage4TableInventoryReceipt(stored.Inventory)
	if err != nil {
		return err
	}
	requested, err := newStage4TableInventoryReceipt(expected)
	if err != nil {
		return err
	}
	if stored.Digest != canonical.Digest ||
		stored.Digest != requested.Digest ||
		!reflect.DeepEqual(stored.Inventory, expected) {
		return fmt.Errorf(
			"%w: Stage 4 table inventory receipt differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func validateStage4InventoryAuthority(
	inventory Stage4TableInventory,
	workTasks []WorkTask,
	workRanges []RangeState,
) error {
	_, rangesByTask, err := indexStage4AggregateWork(
		inventory.RunID,
		workTasks,
		workRanges,
	)
	if err != nil {
		return err
	}
	schemaFound := false
	for _, task := range workTasks {
		rangeID, strategy, sentinel := stage4SentinelDefinition(task.Key)
		if !sentinel {
			return fmt.Errorf(
				"%w: table inventory must precede table work %#v",
				ErrStateTransition,
				task.Key,
			)
		}
		ranges := rangesByTask[task.Key]
		if task.Strategy != strategy ||
			task.Status != "running" ||
			task.StartedAt.IsZero() ||
			!task.CompletedAt.IsZero() ||
			len(ranges) != 1 ||
			ranges[0].ID != rangeID ||
			ranges[0].Strategy != strategy ||
			ranges[0].TopologyHash != task.TopologyHash ||
			ranges[0].Status != "running" ||
			!ranges[0].CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: Stage 4 sentinel %#v has an unexpected pre-mutation shape",
				ErrImmutableEvidence,
				task.Key,
			)
		}
		if task.Key == stage4SchemaContractSentinelTask {
			schemaFound = true
			if task.TopologyHash != inventory.SchemaTopologyHash {
				return fmt.Errorf(
					"%w: inventory schema topology differs",
					ErrTopologyChanged,
				)
			}
		}
	}
	if !schemaFound {
		return fmt.Errorf(
			"%w: inventory schema authority task",
			ErrUnknownWork,
		)
	}
	return nil
}

func validateStage4InventorySnapshot(
	inventory Stage4TableInventory,
	snapshots map[TaskKey]SchemaSnapshot,
) error {
	for task := range snapshots {
		if _, _, known := stage4SentinelDefinition(task); !known {
			return fmt.Errorf(
				"%w: unexpected schema snapshot authority %#v",
				ErrImmutableEvidence,
				task,
			)
		}
	}
	snapshot, found := snapshots[inventory.SchemaTask]
	if !found {
		return fmt.Errorf(
			"%w: inventory schema snapshot",
			ErrUnknownWork,
		)
	}
	normalized, err := normalizeSchemaSnapshot(snapshot)
	if err != nil {
		return err
	}
	if !stage4SnapshotMatches(snapshot, normalized) ||
		snapshot.RunID != inventory.RunID ||
		snapshot.Task != inventory.SchemaTask ||
		snapshot.Digest != inventory.SchemaSnapshotDigest {
		return fmt.Errorf(
			"%w: inventory schema snapshot differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func normalizeStoredStage4TableInventory(
	stored Stage4TableInventoryReceipt,
) (Stage4TableInventoryReceipt, error) {
	normalized, err := normalizeStage4TableInventory(stored.Inventory)
	if err != nil {
		return Stage4TableInventoryReceipt{}, err
	}
	if err := validateStage4TableInventoryReceipt(
		stored,
		normalized,
	); err != nil {
		return Stage4TableInventoryReceipt{}, err
	}
	stored.Inventory = normalized
	return stored, nil
}

// normalizeStoredStage4TableCompletion re-derives one stored table receipt and
// proves it still matches its own digest, so a caller can never rebuild a run
// publication from tampered or partially written completion evidence.
func normalizeStoredStage4TableCompletion(
	stored Stage4TableCompletionReceipt,
) (Stage4TableCompletionReceipt, error) {
	normalized, err := normalizeStage4TableCompletion(stored.Completion)
	if err != nil {
		return Stage4TableCompletionReceipt{}, err
	}
	if err := validateStage4TableCompletionReceipt(
		stored,
		normalized,
	); err != nil {
		return Stage4TableCompletionReceipt{}, err
	}
	stored.Completion = normalized
	return stored, nil
}

// normalizeStoredStage4TableCompletions returns every validated receipt for one
// run ordered by ordinary table, which is the exact order
// normalizeStage4RunCompletion imposes on Stage4RunCompletion.Tables.
func normalizeStoredStage4TableCompletions(
	runID string,
	stored []Stage4TableCompletionReceipt,
) ([]Stage4TableCompletionReceipt, error) {
	receipts := make([]Stage4TableCompletionReceipt, 0, len(stored))
	tables := make(map[string]struct{}, len(stored))
	tasks := make(map[TaskKey]struct{}, len(stored))
	for _, candidate := range stored {
		receipt, err := normalizeStoredStage4TableCompletion(candidate)
		if err != nil {
			return nil, err
		}
		completion := receipt.Completion
		if completion.RunID != runID {
			return nil, fmt.Errorf(
				"%w: aggregate table receipt run identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := tables[completion.Table]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate aggregate table receipt for table %q",
				ErrImmutableEvidence,
				completion.Table,
			)
		}
		if _, duplicate := tasks[completion.Task]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate aggregate table receipt for task %#v",
				ErrImmutableEvidence,
				completion.Task,
			)
		}
		tables[completion.Table] = struct{}{}
		tasks[completion.Task] = struct{}{}
		receipts = append(receipts, receipt)
	}
	sort.Slice(receipts, func(left, right int) bool {
		return receipts[left].Completion.Table <
			receipts[right].Completion.Table
	})
	return receipts, nil
}

func validateStage4InventoryAuthorizesTable(
	stored Stage4TableInventoryReceipt,
	completion Stage4TableCompletion,
	workTask WorkTask,
	ranges []RangeState,
) error {
	receipt, err := normalizeStoredStage4TableInventory(stored)
	if err != nil {
		return err
	}
	var authority *Stage4TableInventoryEntry
	for index := range receipt.Inventory.Tables {
		candidate := &receipt.Inventory.Tables[index]
		if candidate.Table == completion.Table ||
			candidate.Task == completion.Task {
			if authority != nil ||
				candidate.Table != completion.Table ||
				candidate.Task != completion.Task {
				return fmt.Errorf(
					"%w: table inventory identity is ambiguous",
					ErrImmutableEvidence,
				)
			}
			authority = candidate
		}
	}
	if authority == nil {
		return fmt.Errorf(
			"%w: table completion is absent from the durable inventory",
			ErrUnknownWork,
		)
	}
	if authority.TopologyHash != completion.TopologyHash ||
		workTask.RunID != completion.RunID ||
		workTask.Key != completion.Task ||
		workTask.Strategy != authority.Strategy ||
		workTask.TopologyHash != authority.TopologyHash {
		return fmt.Errorf(
			"%w: table completion differs from durable inventory authority",
			ErrTopologyChanged,
		)
	}
	if len(authority.Ranges) != len(completion.Ranges) ||
		len(ranges) != len(authority.Ranges) {
		return fmt.Errorf(
			"%w: table range inventory differs",
			ErrTopologyChanged,
		)
	}
	rangeByID := make(map[string]RangeState, len(ranges))
	for _, workRange := range ranges {
		if _, duplicate := rangeByID[workRange.ID]; duplicate {
			return fmt.Errorf(
				"%w: duplicate durable table range %q",
				ErrImmutableEvidence,
				workRange.ID,
			)
		}
		rangeByID[workRange.ID] = workRange
	}
	for index, planned := range authority.Ranges {
		if completion.Ranges[index].ID != planned.ID {
			return fmt.Errorf(
				"%w: table completion range identity differs",
				ErrTopologyChanged,
			)
		}
		workRange, found := rangeByID[planned.ID]
		if !found ||
			workRange.RunID != completion.RunID ||
			workRange.Task != completion.Task ||
			workRange.Strategy != authority.Strategy ||
			workRange.TopologyHash != authority.TopologyHash {
			return fmt.Errorf(
				"%w: durable table range %q differs from inventory authority",
				ErrTopologyChanged,
				planned.ID,
			)
		}
	}
	return nil
}

func validateStage4RunInventory(
	stored Stage4TableInventoryReceipt,
	completion Stage4RunCompletion,
) error {
	receipt, err := normalizeStoredStage4TableInventory(stored)
	if err != nil {
		return err
	}
	inventory := receipt.Inventory
	if inventory.RunID != completion.RunID ||
		len(inventory.Tables) != len(completion.Tables) {
		return fmt.Errorf(
			"%w: exact durable table inventory differs",
			ErrImmutableEvidence,
		)
	}
	schemaFound := false
	for _, sentinel := range completion.Sentinels {
		if sentinel.Task != inventory.SchemaTask {
			continue
		}
		if schemaFound ||
			sentinel.TopologyHash != inventory.SchemaTopologyHash ||
			sentinel.Snapshot.Digest != inventory.SchemaSnapshotDigest {
			return fmt.Errorf(
				"%w: schema authority differs from table inventory",
				ErrImmutableEvidence,
			)
		}
		schemaFound = true
	}
	if !schemaFound {
		return fmt.Errorf(
			"%w: table inventory schema authority",
			ErrUnknownWork,
		)
	}
	for index, entry := range inventory.Tables {
		table := completion.Tables[index]
		if entry.Table != table.Table ||
			entry.Task != table.Task ||
			entry.TopologyHash != table.TopologyHash ||
			len(entry.Ranges) != len(table.Ranges) {
			return fmt.Errorf(
				"%w: completed table inventory differs",
				ErrImmutableEvidence,
			)
		}
		for rangeIndex := range entry.Ranges {
			if entry.Ranges[rangeIndex].ID != table.Ranges[rangeIndex].ID {
				return fmt.Errorf(
					"%w: completed table range inventory differs",
					ErrImmutableEvidence,
				)
			}
		}
	}
	return nil
}

func validateStage4DurableInventoryWork(
	stored Stage4TableInventoryReceipt,
	workTasks []WorkTask,
	rangesByTask map[TaskKey][]RangeState,
) error {
	receipt, err := normalizeStoredStage4TableInventory(stored)
	if err != nil {
		return err
	}
	tableTasks := make(map[TaskKey]WorkTask, len(receipt.Inventory.Tables))
	for _, task := range workTasks {
		if !stage4TableWorkType(task.Key.Type) {
			continue
		}
		if _, duplicate := tableTasks[task.Key]; duplicate {
			return fmt.Errorf(
				"%w: duplicate durable table task %#v",
				ErrImmutableEvidence,
				task.Key,
			)
		}
		tableTasks[task.Key] = task
	}
	if len(tableTasks) != len(receipt.Inventory.Tables) {
		return fmt.Errorf(
			"%w: exact durable table work inventory differs",
			ErrImmutableEvidence,
		)
	}
	for _, entry := range receipt.Inventory.Tables {
		task, found := tableTasks[entry.Task]
		if !found ||
			task.RunID != receipt.Inventory.RunID ||
			task.Key.Table != entry.Table ||
			task.Strategy != entry.Strategy ||
			task.TopologyHash != entry.TopologyHash {
			return fmt.Errorf(
				"%w: durable table task differs from inventory authority",
				ErrImmutableEvidence,
			)
		}
		ranges := append(
			[]RangeState(nil),
			rangesByTask[entry.Task]...,
		)
		sort.Slice(ranges, func(left, right int) bool {
			return ranges[left].ID < ranges[right].ID
		})
		if len(ranges) != len(entry.Ranges) {
			return fmt.Errorf(
				"%w: durable table range inventory differs",
				ErrImmutableEvidence,
			)
		}
		for index, planned := range entry.Ranges {
			workRange := ranges[index]
			if workRange.RunID != receipt.Inventory.RunID ||
				workRange.Task != entry.Task ||
				workRange.ID != planned.ID ||
				workRange.Strategy != entry.Strategy ||
				workRange.TopologyHash != entry.TopologyHash {
				return fmt.Errorf(
					"%w: durable table range differs from inventory authority",
					ErrImmutableEvidence,
				)
			}
		}
	}
	return nil
}

func validateStage4KnownWorkInventory(workTasks []WorkTask) error {
	for _, task := range workTasks {
		if stage4TableWorkType(task.Key.Type) {
			continue
		}
		if _, _, sentinel := stage4SentinelDefinition(task.Key); sentinel {
			continue
		}
		return fmt.Errorf(
			"%w: unexpected run-scoped structured task %#v",
			ErrImmutableEvidence,
			task.Key,
		)
	}
	return nil
}

func normalizeStage4TableCompletion(
	completion Stage4TableCompletion,
) (Stage4TableCompletion, error) {
	if strings.TrimSpace(completion.RunID) == "" {
		return Stage4TableCompletion{}, fmt.Errorf("run ID is required")
	}
	if strings.TrimSpace(completion.Table) == "" {
		return Stage4TableCompletion{}, fmt.Errorf("ordinary table identity is required")
	}
	if err := completion.Task.Validate(); err != nil {
		return Stage4TableCompletion{}, err
	}
	if !stage4TableWorkType(completion.Task.Type) {
		return Stage4TableCompletion{}, fmt.Errorf(
			"%w: unsupported aggregate table task type %q",
			ErrImmutableEvidence,
			completion.Task.Type,
		)
	}
	if completion.Task.Table != completion.Table ||
		completion.Task.Partition != "" {
		return Stage4TableCompletion{}, fmt.Errorf(
			"%w: ordinary and structured table identities differ",
			ErrImmutableEvidence,
		)
	}
	if strings.TrimSpace(completion.TopologyHash) == "" {
		return Stage4TableCompletion{}, fmt.Errorf("topology hash is required")
	}
	if completion.RowsDone < 0 {
		return Stage4TableCompletion{}, fmt.Errorf("completed row count cannot be negative")
	}
	if completion.CompletedAt.IsZero() {
		return Stage4TableCompletion{}, fmt.Errorf("completion time is required")
	}
	if len(completion.Ranges) == 0 {
		return Stage4TableCompletion{}, fmt.Errorf("at least one range is required")
	}
	completion.CompletedAt = completion.CompletedAt.UTC()
	completion.Ranges = append([]Stage4RangeCompletion(nil), completion.Ranges...)
	sort.Slice(completion.Ranges, func(left, right int) bool {
		return completion.Ranges[left].ID < completion.Ranges[right].ID
	})
	for index, workRange := range completion.Ranges {
		if strings.TrimSpace(workRange.ID) == "" {
			return Stage4TableCompletion{}, fmt.Errorf("range identity is required")
		}
		if index > 0 && completion.Ranges[index-1].ID == workRange.ID {
			return Stage4TableCompletion{}, fmt.Errorf(
				"%w: duplicate range %q",
				ErrImmutableEvidence,
				workRange.ID,
			)
		}
	}
	if completion.Incremental != nil {
		incremental := *completion.Incremental
		if incremental.RunID != completion.RunID ||
			incremental.Task != completion.Task ||
			incremental.TopologyHash != completion.TopologyHash ||
			!incremental.CompletedAt.Equal(completion.CompletedAt) {
			return Stage4TableCompletion{}, fmt.Errorf(
				"%w: incremental completion differs from table completion",
				ErrImmutableEvidence,
			)
		}
		if strings.TrimSpace(incremental.AttemptID) == "" {
			return Stage4TableCompletion{}, fmt.Errorf(
				"incremental attempt ID is required",
			)
		}
		if incremental.Watermark != nil {
			watermark, err := normalizeOptionalTimestamp(
				incremental.Watermark,
			)
			if err != nil {
				return Stage4TableCompletion{}, fmt.Errorf(
					"incremental committed watermark: %w",
					err,
				)
			}
			incremental.Watermark = watermark
		}
		completion.Incremental = &incremental
	}
	return completion, nil
}

func newStage4TableCompletionReceipt(
	completion Stage4TableCompletion,
) (Stage4TableCompletionReceipt, error) {
	encoded, err := json.Marshal(completion)
	if err != nil {
		return Stage4TableCompletionReceipt{}, fmt.Errorf(
			"encode aggregate table receipt: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return Stage4TableCompletionReceipt{
		Completion: completion,
		Digest:     hex.EncodeToString(digest[:]),
	}, nil
}

func validateStage4TableCompletionReceipt(
	stored Stage4TableCompletionReceipt,
	expected Stage4TableCompletion,
) error {
	canonical, err := newStage4TableCompletionReceipt(stored.Completion)
	if err != nil {
		return err
	}
	requested, err := newStage4TableCompletionReceipt(expected)
	if err != nil {
		return err
	}
	if stored.Digest != canonical.Digest ||
		stored.Digest != requested.Digest ||
		!reflect.DeepEqual(stored.Completion, expected) {
		return fmt.Errorf(
			"%w: aggregate table completion receipt differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func normalizeStage4RunCompletion(
	completion Stage4RunCompletion,
) (Stage4RunCompletion, error) {
	if strings.TrimSpace(completion.RunID) == "" {
		return Stage4RunCompletion{}, fmt.Errorf("run ID is required")
	}
	if strings.TrimSpace(completion.Reason) == "" {
		return Stage4RunCompletion{}, fmt.Errorf("success reason is required")
	}
	if completion.CompletedAt.IsZero() {
		return Stage4RunCompletion{}, fmt.Errorf("completion time is required")
	}
	if len(completion.Sentinels) == 0 {
		return Stage4RunCompletion{}, fmt.Errorf(
			"at least one schema sentinel is required",
		)
	}
	completion.CompletedAt = completion.CompletedAt.UTC()
	completion.Tables = append(
		[]Stage4TableCompletion(nil),
		completion.Tables...,
	)
	for index := range completion.Tables {
		table, err := normalizeStage4TableCompletion(
			completion.Tables[index],
		)
		if err != nil {
			return Stage4RunCompletion{}, fmt.Errorf(
				"expected table completion: %w",
				err,
			)
		}
		if table.RunID != completion.RunID {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: expected table belongs to another run",
				ErrImmutableEvidence,
			)
		}
		if table.CompletedAt.After(completion.CompletedAt) {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: run completion precedes expected table completion",
				ErrStateTransition,
			)
		}
		completion.Tables[index] = table
	}
	sort.Slice(completion.Tables, func(left, right int) bool {
		return completion.Tables[left].Table <
			completion.Tables[right].Table
	})
	seenTasks := make(map[TaskKey]struct{}, len(completion.Tables))
	for index, table := range completion.Tables {
		if index > 0 &&
			completion.Tables[index-1].Table == table.Table {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: duplicate expected ordinary table %q",
				ErrImmutableEvidence,
				table.Table,
			)
		}
		if _, duplicate := seenTasks[table.Task]; duplicate {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: duplicate expected structured table %#v",
				ErrImmutableEvidence,
				table.Task,
			)
		}
		seenTasks[table.Task] = struct{}{}
	}
	completion.Sentinels = append(
		[]Stage4SentinelCompletion(nil),
		completion.Sentinels...,
	)
	for index := range completion.Sentinels {
		sentinel := &completion.Sentinels[index]
		if err := sentinel.Task.Validate(); err != nil {
			return Stage4RunCompletion{}, err
		}
		if strings.TrimSpace(sentinel.RangeID) == "" ||
			strings.TrimSpace(sentinel.TopologyHash) == "" {
			return Stage4RunCompletion{}, fmt.Errorf(
				"sentinel range and topology are required",
			)
		}
		expectedRange, _, known := stage4SentinelDefinition(sentinel.Task)
		if !known || sentinel.RangeID != expectedRange {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: unknown or malformed Stage 4 sentinel task %#v",
				ErrImmutableEvidence,
				sentinel.Task,
			)
		}
		if sentinel.Snapshot.RunID != completion.RunID ||
			sentinel.Snapshot.Task != sentinel.Task {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: sentinel snapshot identity differs",
				ErrImmutableEvidence,
			)
		}
		normalized, err := normalizeSchemaSnapshot(sentinel.Snapshot)
		if err != nil {
			return Stage4RunCompletion{}, err
		}
		if normalized.CapturedAt.After(completion.CompletedAt) {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: sentinel snapshot was captured after run completion",
				ErrStateTransition,
			)
		}
		sentinel.Snapshot = normalized
	}
	sort.Slice(completion.Sentinels, func(left, right int) bool {
		leftKey, _ := completion.Sentinels[left].Task.canonical()
		rightKey, _ := completion.Sentinels[right].Task.canonical()
		return leftKey < rightKey
	})
	for index := 1; index < len(completion.Sentinels); index++ {
		if completion.Sentinels[index-1].Task ==
			completion.Sentinels[index].Task {
			return Stage4RunCompletion{}, fmt.Errorf(
				"%w: duplicate sentinel task",
				ErrImmutableEvidence,
			)
		}
	}
	foundSchema := false
	for _, sentinel := range completion.Sentinels {
		if sentinel.Task == stage4SchemaContractSentinelTask {
			foundSchema = true
		}
	}
	if !foundSchema {
		return Stage4RunCompletion{}, fmt.Errorf(
			"%w: aggregate source-schema sentinel is required",
			ErrUnknownWork,
		)
	}
	return completion, nil
}

func stage4SentinelDefinition(
	task TaskKey,
) (rangeID string, strategy string, found bool) {
	switch task {
	case stage4SchemaContractSentinelTask:
		return "aggregate-schema", "stage4_aggregate_schema_contract_v1", true
	case stage4TargetShapeSentinelTask:
		return "aggregate-target-shape", "stage4_target_shape_authority_v1", true
	default:
		return "", "", false
	}
}

func validateStage4SentinelInventory(
	completion Stage4RunCompletion,
	workTasks []WorkTask,
	rangesByTask map[TaskKey][]RangeState,
) error {
	requested := make(map[TaskKey]Stage4SentinelCompletion, len(completion.Sentinels))
	for _, sentinel := range completion.Sentinels {
		requested[sentinel.Task] = sentinel
	}
	for _, task := range workTasks {
		rangeID, strategy, sentinel := stage4SentinelDefinition(task.Key)
		if !sentinel {
			continue
		}
		request, supplied := requested[task.Key]
		if !supplied {
			return fmt.Errorf(
				"%w: durable Stage 4 sentinel %#v was omitted",
				ErrImmutableEvidence,
				task.Key,
			)
		}
		ranges := rangesByTask[task.Key]
		if task.Strategy != strategy ||
			request.RangeID != rangeID ||
			len(ranges) != 1 ||
			ranges[0].ID != rangeID ||
			ranges[0].Strategy != strategy {
			return fmt.Errorf(
				"%w: durable Stage 4 sentinel %#v has an unexpected shape",
				ErrImmutableEvidence,
				task.Key,
			)
		}
	}
	for task := range requested {
		found := false
		for _, workTask := range workTasks {
			if workTask.Key == task {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf(
				"%w: supplied Stage 4 sentinel %#v",
				ErrUnknownWork,
				task,
			)
		}
	}
	return nil
}

func exactStage4Ranges(
	runID string,
	task TaskKey,
	topology string,
	ranges []RangeState,
	expected []Stage4RangeCompletion,
) error {
	if len(ranges) != len(expected) {
		return fmt.Errorf(
			"%w: exact range inventory differs for task %#v",
			ErrTopologyChanged,
			task,
		)
	}
	sort.Slice(ranges, func(left, right int) bool {
		return ranges[left].ID < ranges[right].ID
	})
	for index := range ranges {
		workRange := ranges[index]
		requested := expected[index]
		if workRange.RunID != runID ||
			workRange.Task != task ||
			workRange.ID != requested.ID {
			return fmt.Errorf(
				"%w: range identity differs for task %#v",
				ErrImmutableEvidence,
				task,
			)
		}
		if workRange.TopologyHash != topology ||
			workRange.NextSequence != requested.NextSequence {
			return fmt.Errorf(
				"%w: range %q frontier differs",
				ErrTopologyChanged,
				requested.ID,
			)
		}
		if workRange.SequenceOffset != 0 || len(workRange.Pending) != 0 {
			return fmt.Errorf(
				"%w: range %q has incomplete acknowledgements",
				ErrRangeOrder,
				requested.ID,
			)
		}
		switch workRange.Status {
		case "running":
			if !workRange.CompletedAt.IsZero() {
				return fmt.Errorf(
					"%w: running range %q has completion evidence",
					ErrStateTransition,
					requested.ID,
				)
			}
		case "completed":
			if workRange.CompletedAt.IsZero() ||
				workRange.UpdatedAt.IsZero() ||
				!workRange.UpdatedAt.Equal(workRange.CompletedAt) {
				return fmt.Errorf(
					"%w: completed range %q has invalid completion evidence",
					ErrStateTransition,
					requested.ID,
				)
			}
		default:
			return fmt.Errorf(
				"%w: range %q has unsafe status %q",
				ErrStateTransition,
				requested.ID,
				workRange.Status,
			)
		}
	}
	return nil
}

func applyStage4StructuredCompletion(
	runID string,
	taskKey TaskKey,
	topology string,
	expected []Stage4RangeCompletion,
	completedAt time.Time,
	workTask WorkTask,
	ranges []RangeState,
	requireAllRunning bool,
) (WorkTask, []RangeState, bool, error) {
	if workTask.RunID != runID || workTask.Key != taskKey {
		return WorkTask{}, nil, false, fmt.Errorf(
			"%w: structured task identity differs",
			ErrImmutableEvidence,
		)
	}
	if completedAt.Before(workTask.StartedAt) {
		return WorkTask{}, nil, false, fmt.Errorf(
			"%w: structured completion precedes task start",
			ErrStateTransition,
		)
	}
	if workTask.TopologyHash != topology {
		return WorkTask{}, nil, false, fmt.Errorf(
			"%w: structured task topology differs",
			ErrTopologyChanged,
		)
	}
	ranges = append([]RangeState(nil), ranges...)
	if err := exactStage4Ranges(
		runID,
		taskKey,
		topology,
		ranges,
		expected,
	); err != nil {
		return WorkTask{}, nil, false, err
	}
	switch workTask.Status {
	case "running":
		if !workTask.CompletedAt.IsZero() {
			return WorkTask{}, nil, false, fmt.Errorf(
				"%w: running structured task has completion evidence",
				ErrStateTransition,
			)
		}
		for index := range ranges {
			if requireAllRunning && ranges[index].Status != "running" {
				return WorkTask{}, nil, false, fmt.Errorf(
					"%w: schema sentinel is partially complete",
					ErrStateTransition,
				)
			}
			if ranges[index].Status == "running" {
				ranges[index].Status = "completed"
				ranges[index].CompletedAt = completedAt
				ranges[index].UpdatedAt = completedAt
			}
		}
		workTask.Status = "completed"
		workTask.CompletedAt = completedAt
		workTask.UpdatedAt = completedAt
		return workTask, ranges, false, nil
	case "completed":
		if !workTask.CompletedAt.Equal(completedAt) ||
			!workTask.UpdatedAt.Equal(completedAt) {
			return WorkTask{}, nil, false, fmt.Errorf(
				"%w: structured completion differs",
				ErrImmutableEvidence,
			)
		}
		for _, workRange := range ranges {
			if workRange.Status != "completed" {
				return WorkTask{}, nil, false, fmt.Errorf(
					"%w: completed task has incomplete ranges",
					ErrStateTransition,
				)
			}
			if requireAllRunning &&
				(!workRange.CompletedAt.Equal(completedAt) ||
					!workRange.UpdatedAt.Equal(completedAt)) {
				return WorkTask{}, nil, false, fmt.Errorf(
					"%w: schema sentinel completion differs",
					ErrImmutableEvidence,
				)
			}
		}
		return workTask, ranges, true, nil
	default:
		return WorkTask{}, nil, false, fmt.Errorf(
			"%w: structured task has unsafe status %q",
			ErrStateTransition,
			workTask.Status,
		)
	}
}

func applyStage4TableCompletion(
	completion Stage4TableCompletion,
	ordinary Task,
	workTask WorkTask,
	ranges []RangeState,
	attempt *IncrementalAttempt,
) (Task, WorkTask, []RangeState, *IncrementalAttempt, error) {
	if ordinary.RunID != completion.RunID ||
		ordinary.Table != completion.Table {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: ordinary task identity differs",
			ErrImmutableEvidence,
		)
	}
	if ordinary.StartedAt.IsZero() ||
		workTask.StartedAt.IsZero() {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: task start evidence is incomplete",
			ErrStateTransition,
		)
	}
	if completion.CompletedAt.Before(ordinary.StartedAt) ||
		completion.CompletedAt.Before(workTask.StartedAt) {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: table completion precedes task start",
			ErrStateTransition,
		)
	}
	if ordinary.IntegerWatermark != nil ||
		ordinary.RowNumberWatermark != nil {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: ordinary task contains legacy frontier evidence",
			ErrStateTransition,
		)
	}
	completedWork, completedRanges, replay, err :=
		applyStage4StructuredCompletion(
			completion.RunID,
			completion.Task,
			completion.TopologyHash,
			completion.Ranges,
			completion.CompletedAt,
			workTask,
			ranges,
			false,
		)
	if err != nil {
		return Task{}, WorkTask{}, nil, nil, err
	}
	switch {
	case ordinary.Status == "running" && !replay:
		if ordinary.RowsDone != 0 || !ordinary.CompletedAt.IsZero() {
			return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
				"%w: running ordinary task has terminal evidence",
				ErrStateTransition,
			)
		}
		ordinary.Status = "completed"
		ordinary.RowsDone = completion.RowsDone
		ordinary.CompletedAt = completion.CompletedAt
	case ordinary.Status == "completed" && replay:
		if ordinary.RowsDone != completion.RowsDone ||
			!ordinary.CompletedAt.Equal(completion.CompletedAt) {
			return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
				"%w: ordinary completion differs",
				ErrImmutableEvidence,
			)
		}
	default:
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: ordinary and structured completion evidence is partial",
			ErrStateTransition,
		)
	}

	var rowsDone int64
	for _, workRange := range completedRanges {
		if workRange.CompletedAt.After(completion.CompletedAt) {
			return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
				"%w: range completion follows table completion",
				ErrStateTransition,
			)
		}
		if workRange.RowsDone < 0 ||
			workRange.RowsDone > math.MaxInt64-rowsDone {
			return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
				"%w: structured row count is invalid",
				ErrStateTransition,
			)
		}
		rowsDone += workRange.RowsDone
	}
	if rowsDone > int64(math.MaxInt) ||
		int(rowsDone) != completion.RowsDone {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: ordinary and structured row totals differ",
			ErrImmutableEvidence,
		)
	}

	if completion.Incremental == nil {
		if attempt != nil {
			return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
				"%w: incremental attempt exists without completion evidence",
				ErrStateTransition,
			)
		}
		return ordinary, completedWork, completedRanges, nil, nil
	}
	if attempt == nil {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: incremental attempt %q",
			ErrUnknownWork,
			completion.Incremental.AttemptID,
		)
	}
	completedAttempt, err := applyIncrementalCommit(
		*attempt,
		*completion.Incremental,
	)
	if err != nil {
		return Task{}, WorkTask{}, nil, nil, err
	}
	if replay != (attempt.Status == IncrementalCompleted) {
		return Task{}, WorkTask{}, nil, nil, fmt.Errorf(
			"%w: incremental and task completion evidence is partial",
			ErrStateTransition,
		)
	}
	return ordinary, completedWork, completedRanges, &completedAttempt, nil
}

func validateStage4RunRowsComplete(
	runID string,
	completedAt time.Time,
	tasks []Task,
) error {
	seen := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		if task.RunID != runID ||
			strings.TrimSpace(task.Table) == "" {
			return fmt.Errorf(
				"%w: ordinary task identity differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := seen[task.Table]; duplicate {
			return fmt.Errorf(
				"%w: duplicate ordinary task %q",
				ErrImmutableEvidence,
				task.Table,
			)
		}
		seen[task.Table] = struct{}{}
		if task.Status != "completed" ||
			task.CompletedAt.IsZero() ||
			task.RowsDone < 0 ||
			task.IntegerWatermark != nil ||
			task.RowNumberWatermark != nil ||
			task.CompletedAt.After(completedAt) {
			return fmt.Errorf(
				"%w: ordinary task %q is not durably complete",
				ErrStateTransition,
				task.Table,
			)
		}
	}
	return nil
}

func validateStage4TerminalRecords(
	incremental []IncrementalAttempt,
	deletes []DeleteReconciliation,
	completion Stage4RunCompletion,
) error {
	expectedTasks := make(map[TaskKey]struct{}, len(completion.Tables))
	for _, table := range completion.Tables {
		expectedTasks[table.Task] = struct{}{}
	}
	for _, attempt := range incremental {
		if _, expected := expectedTasks[attempt.Task]; !expected {
			return fmt.Errorf(
				"%w: incremental attempt %q belongs to an unexpected table",
				ErrImmutableEvidence,
				attempt.AttemptID,
			)
		}
		if attempt.Status != IncrementalCompleted ||
			!attempt.TableSucceeded ||
			attempt.CompletedAt.IsZero() ||
			attempt.CompletedAt.After(completion.CompletedAt) {
			return fmt.Errorf(
				"%w: incremental attempt %q is not durably complete",
				ErrStateTransition,
				attempt.AttemptID,
			)
		}
		if _, err := applyIncrementalCommit(
			attempt,
			IncrementalCommit{
				RunID:        attempt.RunID,
				Task:         attempt.Task,
				AttemptID:    attempt.AttemptID,
				TopologyHash: "durable-terminal-validation",
				Watermark:    attempt.CommittedWatermark,
				CompletedAt:  attempt.CompletedAt,
			},
		); err != nil {
			return fmt.Errorf(
				"%w: incremental attempt %q is malformed: %v",
				ErrStateTransition,
				attempt.AttemptID,
				err,
			)
		}
	}
	for _, record := range deletes {
		if _, expected := expectedTasks[record.Task]; !expected {
			return fmt.Errorf(
				"%w: delete reconciliation %q belongs to an unexpected table",
				ErrImmutableEvidence,
				record.AttemptID,
			)
		}
		if err := ValidateDeleteReconciliationEvidence(record); err != nil {
			return fmt.Errorf(
				"%w: invalid delete reconciliation %q: %v",
				ErrStateTransition,
				record.AttemptID,
				err,
			)
		}
		switch record.Status {
		case DeleteReconciliationNotDue,
			DeleteReconciliationCompleted,
			DeleteReconciliationDryRun:
			if record.CompletedAt.IsZero() ||
				record.CompletedAt.After(completion.CompletedAt) ||
				record.PendingBatch != nil {
				return fmt.Errorf(
					"%w: delete reconciliation %q has incomplete terminal evidence",
					ErrStateTransition,
					record.AttemptID,
				)
			}
		default:
			return fmt.Errorf(
				"%w: delete reconciliation %q is not terminal",
				ErrStateTransition,
				record.AttemptID,
			)
		}
	}
	return nil
}

func stage4RunReplayMatches(
	run Run,
	completion Stage4RunCompletion,
) bool {
	return run.Outcome == Success &&
		!run.Resumable &&
		run.Reason == completion.Reason &&
		run.EndedAt.Equal(completion.CompletedAt)
}

func validateStage4TableRun(
	records []Run,
	completion Stage4TableCompletion,
	replay bool,
) error {
	latest, err := validateStage4RunIdentity(records, completion.RunID)
	if err != nil {
		return err
	}
	if completion.CompletedAt.Before(latest.StartedAt) {
		return fmt.Errorf(
			"%w: table completion precedes run start",
			ErrStateTransition,
		)
	}
	switch latest.Outcome {
	case Running:
		if !latest.Resumable {
			return fmt.Errorf(
				"%w: active run is not resumable",
				ErrStateTransition,
			)
		}
	case Success:
		if !replay {
			return fmt.Errorf(
				"%w: successful run has an incomplete table",
				ErrStateTransition,
			)
		}
	default:
		return fmt.Errorf(
			"%w: run outcome %q cannot publish table completion",
			ErrStateTransition,
			latest.Outcome,
		)
	}
	return nil
}

func validateStage4RunIdentity(records []Run, runID string) (Run, error) {
	if len(records) == 0 {
		return Run{}, fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	first := records[0]
	outcomes := make(map[Outcome]struct{}, len(records))
	runningFound := false
	successIndex := -1
	for index, record := range records {
		if record.ID != runID ||
			record.Source != first.Source ||
			record.Target != first.Target ||
			record.SourceEngine != first.SourceEngine ||
			record.SourceIdentity != first.SourceIdentity ||
			record.TargetIdentity != first.TargetIdentity ||
			record.LeaseTarget != first.LeaseTarget ||
			record.LeaseOwnerToken != first.LeaseOwnerToken ||
			record.LeaseGeneration != first.LeaseGeneration ||
			!record.StartedAt.Equal(first.StartedAt) {
			return Run{}, fmt.Errorf(
				"%w: run %q identity differs across outcomes",
				ErrImmutableEvidence,
				runID,
			)
		}
		if _, duplicate := outcomes[record.Outcome]; duplicate {
			return Run{}, fmt.Errorf(
				"%w: run %q has duplicate outcome %q",
				ErrImmutableEvidence,
				runID,
				record.Outcome,
			)
		}
		outcomes[record.Outcome] = struct{}{}
		if record.StartedAt.IsZero() ||
			strings.TrimSpace(record.Reason) == "" {
			return Run{}, fmt.Errorf(
				"%w: run %q has incomplete lifecycle evidence",
				ErrStateTransition,
				runID,
			)
		}
		switch record.Outcome {
		case Running:
			runningFound = true
			if !record.Resumable || !record.EndedAt.IsZero() {
				return Run{}, fmt.Errorf(
					"%w: run %q has malformed running evidence",
					ErrStateTransition,
					runID,
				)
			}
		case Failed, Cancelled, Partial:
			if !record.Resumable ||
				record.EndedAt.IsZero() ||
				record.EndedAt.Before(record.StartedAt) {
				return Run{}, fmt.Errorf(
					"%w: run %q has malformed recoverable outcome %q",
					ErrStateTransition,
					runID,
					record.Outcome,
				)
			}
		case Success:
			successIndex = index
			if record.Resumable ||
				record.EndedAt.IsZero() ||
				record.EndedAt.Before(record.StartedAt) {
				return Run{}, fmt.Errorf(
					"%w: run %q has malformed success evidence",
					ErrStateTransition,
					runID,
				)
			}
		default:
			return Run{}, fmt.Errorf(
				"%w: run %q has unknown outcome %q",
				ErrStateTransition,
				runID,
				record.Outcome,
			)
		}
	}
	if !runningFound {
		return Run{}, fmt.Errorf(
			"%w: run %q lacks running lifecycle evidence",
			ErrStateTransition,
			runID,
		)
	}
	if successIndex >= 0 && successIndex != len(records)-1 {
		return Run{}, fmt.Errorf(
			"%w: run %q has non-terminal success evidence",
			ErrStateTransition,
			runID,
		)
	}
	latest := records[0]
	for index := 1; index < len(records); index++ {
		candidate := records[index]
		if latest.StartedAt.Before(candidate.StartedAt) ||
			latest.StartedAt.Equal(candidate.StartedAt) {
			latest = candidate
		}
	}
	return latest, nil
}

func validateStage4ExpectedTableInventory(
	completion Stage4RunCompletion,
	ordinary []Task,
	workTasks []WorkTask,
	rangesByTask map[TaskKey][]RangeState,
	incremental []IncrementalAttempt,
	receipts map[TaskKey]Stage4TableCompletionReceipt,
) error {
	if len(ordinary) != len(completion.Tables) {
		return fmt.Errorf(
			"%w: exact ordinary table inventory differs",
			ErrImmutableEvidence,
		)
	}
	if len(receipts) != len(completion.Tables) {
		return fmt.Errorf(
			"%w: exact aggregate table receipt inventory differs",
			ErrImmutableEvidence,
		)
	}
	ordinaryByTable := make(map[string]Task, len(ordinary))
	for _, task := range ordinary {
		ordinaryByTable[task.Table] = task
	}
	tableWork := make(map[TaskKey]WorkTask, len(completion.Tables))
	for _, workTask := range workTasks {
		if !stage4TableWorkType(workTask.Key.Type) {
			continue
		}
		if workTask.Key.Partition != "" {
			return fmt.Errorf(
				"%w: aggregate table work %#v is partition-scoped",
				ErrImmutableEvidence,
				workTask.Key,
			)
		}
		if _, duplicate := tableWork[workTask.Key]; duplicate {
			return fmt.Errorf(
				"%w: duplicate aggregate table work %#v",
				ErrImmutableEvidence,
				workTask.Key,
			)
		}
		tableWork[workTask.Key] = workTask
	}
	if len(tableWork) != len(completion.Tables) {
		return fmt.Errorf(
			"%w: exact structured table inventory differs",
			ErrImmutableEvidence,
		)
	}
	attemptsByTask := make(map[TaskKey]IncrementalAttempt, len(incremental))
	for _, attempt := range incremental {
		if _, duplicate := attemptsByTask[attempt.Task]; duplicate {
			return fmt.Errorf(
				"%w: multiple incremental records for task %#v",
				ErrImmutableEvidence,
				attempt.Task,
			)
		}
		attemptsByTask[attempt.Task] = attempt
	}
	for _, expected := range completion.Tables {
		ordinaryTask, found := ordinaryByTable[expected.Table]
		if !found {
			return fmt.Errorf(
				"%w: expected ordinary task %q",
				ErrUnknownWork,
				expected.Table,
			)
		}
		workTask, found := tableWork[expected.Task]
		if !found {
			return fmt.Errorf(
				"%w: expected structured task %#v",
				ErrUnknownWork,
				expected.Task,
			)
		}
		receipt, found := receipts[expected.Task]
		if !found {
			return fmt.Errorf(
				"%w: aggregate receipt for expected task %#v",
				ErrUnknownWork,
				expected.Task,
			)
		}
		if err := validateStage4TableCompletionReceipt(
			receipt,
			expected,
		); err != nil {
			return err
		}
		var attempt *IncrementalAttempt
		if stored, found := attemptsByTask[expected.Task]; found {
			copy := stored
			attempt = &copy
		}
		if _, _, _, _, err := applyStage4TableCompletion(
			expected,
			ordinaryTask,
			workTask,
			rangesByTask[expected.Task],
			attempt,
		); err != nil {
			return fmt.Errorf(
				"verify expected aggregate table %q: %w",
				expected.Table,
				err,
			)
		}
	}
	if len(attemptsByTask) != 0 {
		expectedTasks := make(map[TaskKey]struct{}, len(completion.Tables))
		for _, table := range completion.Tables {
			expectedTasks[table.Task] = struct{}{}
		}
		for task := range attemptsByTask {
			if _, expected := expectedTasks[task]; !expected {
				return fmt.Errorf(
					"%w: incremental record belongs to unexpected table %#v",
					ErrImmutableEvidence,
					task,
				)
			}
		}
	}
	return nil
}

func stage4TableWorkType(taskType string) bool {
	switch taskType {
	case "network-table-copy", "table-copy", "analytical-table-copy":
		return true
	default:
		return false
	}
}

func stage4SnapshotMatches(stored, requested SchemaSnapshot) bool {
	return stored.RunID == requested.RunID &&
		stored.Task == requested.Task &&
		stored.CanonicalJSON == requested.CanonicalJSON &&
		stored.Digest == requested.Digest &&
		stored.CapturedAt.Equal(requested.CapturedAt)
}
