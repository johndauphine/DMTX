package migrate

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

type deleteKeyRows interface {
	Next() bool
	Values() ([]any, error)
	Err() error
	Close() error
}

type deleteKeySource interface {
	OpenDeletePrimaryKeys(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error)
}

// deleteTargetBatch must be applied atomically with a target-side durable
// receipt keyed by Token. Replaying a token must return the original receipt,
// even when the rows are already absent. This closes the target-commit/state-
// acknowledgement crash window without guessing from affected-row counts.
type deleteTargetBatch struct {
	Table       schema.Table
	Columns     []string
	PlanID      string
	Token       string
	Sequence    int64
	BatchDigest string
	Keys        [][]driver.Value
}

type deleteTargetBatchReceipt struct {
	PlanID        string
	Token         string
	Sequence      int64
	BatchDigest   string
	Candidates    int64
	DeletedRows   int64
	ReceiptDigest string
	// FailClosedReason is target-atomic journal evidence. A target that
	// returns a durable receipt and an error must store
	// target_mutation_failed here so receipt replay cannot erase the error.
	// Protector errors are deliberately not stored in the target journal:
	// they describe local mutation authority and may be retried under a
	// healthy protector if their state acknowledgement did not commit.
	FailClosedReason string
}

type deleteKeyTarget interface {
	OpenDeletePrimaryKeys(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error)
	MaxDeleteParameters() int
	ApplyDeleteBatch(
		context.Context,
		deleteTargetBatch,
	) (deleteTargetBatchReceipt, error)
}

// deleteMutationProtector must prove current target ownership immediately
// around every driver mutation. Returning without invoking mutation is the
// required stale-lease behavior.
type deleteMutationProtector interface {
	ProtectDeleteMutation(context.Context, func() error) error
}

type deleteReconciliationState interface {
	BeginDeleteReconciliation(
		state.DeleteReconciliation,
	) (state.DeleteReconciliation, bool, error)
	LoadDeleteReconciliation(
		string,
		state.TaskKey,
		string,
	) (state.DeleteReconciliation, bool, error)
	LoadLatestSuccessfulDeleteReconciliation(
		string,
		state.TaskKey,
	) (state.DeleteReconciliation, bool, error)
	SaveDeleteReconciliationPlan(state.DeleteReconciliationPlan) error
	BeginDeleteReconciliationBatch(
		state.DeleteReconciliationBatch,
	) (state.DeleteReconciliationBatch, bool, error)
	CommitDeleteReconciliationBatch(
		state.DeleteReconciliationBatchCommit,
	) error
	FinishDeleteReconciliation(state.DeleteReconciliationResult) error
}

var _ deleteReconciliationState = (state.Stage4Backend)(nil)

type deleteKeySide string

const (
	deleteKeySourceSide deleteKeySide = "source"
	deleteKeyTargetSide deleteKeySide = "target"
)

type deleteKeyColumnProof struct {
	Semantics         string `json:"semantics"`
	CollationEvidence string `json:"collation_evidence,omitempty"`
}

// deleteKeyEqualityProof is route-owned evidence that source and target key
// values share one exact equality domain. The core binds its digest into the
// durable candidate plan; it never infers text/collation semantics itself.
type deleteKeyEqualityProof struct {
	CanonicalizerID   string                 `json:"canonicalizer_id"`
	SourceFingerprint string                 `json:"source_fingerprint"`
	TargetFingerprint string                 `json:"target_fingerprint"`
	Columns           []deleteKeyColumnProof `json:"columns"`
}

type deleteCanonicalValue struct {
	Canonical []byte
	Parameter driver.Value
}

type deleteKeyCanonicalizer interface {
	ProveDeleteKeyEquality(
		schema.Table,
		schema.Table,
		[]schema.Column,
		[]schema.Column,
	) (deleteKeyEqualityProof, error)
	CanonicalizeDeleteKeyValue(
		deleteKeySide,
		deleteKeyEqualityProof,
		int,
		any,
	) (deleteCanonicalValue, error)
}

type deleteReconcileRequest struct {
	RunID          string
	AttemptID      string
	Task           state.TaskKey
	SourceTable    schema.Table
	TargetTable    schema.Table
	TargetMode     string
	Policy         config.DeletePolicy
	DryRun         bool
	Now            time.Time
	SpoolDirectory string
	MaxBatchBytes  int64
}

type deleteDueFacts struct {
	Due              bool
	Reason           string
	LastSuccessfulAt *time.Time
	NextDueAt        *time.Time
}

// DeleteReconciliationDueFacts is the read-only scheduling evidence exposed
// to dry-run callers. It is computed by the same due-state function used by
// the mutating reconciliation runner.
type DeleteReconciliationDueFacts struct {
	Due              bool
	Reason           string
	LastSuccessfulAt *time.Time
	NextDueAt        *time.Time
}

type deleteReconcileOutcome struct {
	Record                state.DeleteReconciliation
	DueFacts              deleteDueFacts
	StrictCountValidation bool
}

type deleteReconciler struct {
	state         deleteReconciliationState
	source        deleteKeySource
	target        deleteKeyTarget
	canonicalizer deleteKeyCanonicalizer
	protector     deleteMutationProtector
	now           func() time.Time
}

type deleteKeyPlan struct {
	sourcePrimaryKey []schema.Column
	targetPrimaryKey []schema.Column
	sourceColumns    []string
	targetColumns    []string
	proof            deleteKeyEqualityProof
	proofDigest      string
}

func deleteReconciliationDue(
	now time.Time,
	interval time.Duration,
	last state.DeleteReconciliation,
	found bool,
) (deleteDueFacts, error) {
	if now.IsZero() {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation current time is required",
		)
	}
	if interval <= 0 {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation interval must be positive",
		)
	}
	now = now.UTC()
	if !found {
		return deleteDueFacts{
			Due: true, Reason: "no prior successful reconciliation",
		}, nil
	}
	if err := state.ValidateDeleteReconciliationEvidence(last); err != nil {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation is malformed: %w",
			err,
		)
	}
	if last.Status != state.DeleteReconciliationCompleted {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation is not completed",
		)
	}
	completedAt := last.CompletedAt.UTC()
	if completedAt.After(now) {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation completion is in the future",
		)
	}
	nextDueAt := completedAt.Add(interval)
	if nextDueAt.Before(completedAt) {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation due time overflowed",
		)
	}
	lastAt, nextAt := completedAt, nextDueAt
	facts := deleteDueFacts{
		Due:              !now.Before(nextDueAt),
		LastSuccessfulAt: &lastAt,
		NextDueAt:        &nextAt,
	}
	if facts.Due {
		facts.Reason = "reconciliation interval elapsed"
	} else {
		facts.Reason = "reconciliation interval has not elapsed; next due at " +
			nextDueAt.Format(time.RFC3339Nano)
	}
	return facts, nil
}

// EvaluateDeleteReconciliationDue keeps dry-run scheduling facts on the exact
// production path without opening or mutating a state backend.
func EvaluateDeleteReconciliationDue(
	now time.Time,
	interval time.Duration,
	last state.DeleteReconciliation,
	found bool,
) (DeleteReconciliationDueFacts, error) {
	facts, err := deleteReconciliationDue(now, interval, last, found)
	if err != nil {
		return DeleteReconciliationDueFacts{}, err
	}
	return DeleteReconciliationDueFacts{
		Due:              facts.Due,
		Reason:           facts.Reason,
		LastSuccessfulAt: facts.LastSuccessfulAt,
		NextDueAt:        facts.NextDueAt,
	}, nil
}

func validateDeleteReconcileRequest(
	request deleteReconcileRequest,
	canonicalizer deleteKeyCanonicalizer,
) (deleteKeyPlan, error) {
	if strings.TrimSpace(request.RunID) == "" ||
		strings.TrimSpace(request.AttemptID) == "" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation run and attempt IDs are required",
		)
	}
	if err := request.Task.Validate(); err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation task: %w",
			err,
		)
	}
	switch request.Task.Type {
	case "table-copy", stage4AdapterNetworkTaskType:
	default:
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires one authenticated unpartitioned relational table-copy task",
		)
	}
	if request.Task.Partition != "" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires one authenticated unpartitioned relational table-copy task",
		)
	}
	if request.SourceTable.Name == "" ||
		request.TargetTable.Name == "" ||
		request.Task.Table != request.SourceTable.Name ||
		request.Task.Schema != request.SourceTable.Schema ||
		request.TargetTable.Name != request.SourceTable.Name {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation source, target, and task table identities differ",
		)
	}
	if request.TargetMode != "upsert" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires target mode upsert",
		)
	}
	if request.Policy.Mode != config.DeleteModeReconcile ||
		request.Policy.TargetBehavior != config.DeleteTargetHard ||
		request.Policy.Reconcile.Schedule !=
			config.DeleteScheduleInterval ||
		request.Policy.Reconcile.Interval <= 0 ||
		request.Policy.Reconcile.BatchSize <= 0 ||
		!request.Policy.Reconcile.RequirePrimaryKey {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires reconcile/hard/interval with a positive interval, positive batch size, and primary-key enforcement",
		)
	}
	if request.MaxBatchBytes <= 0 {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation maximum batch bytes must be positive",
		)
	}
	if canonicalizer == nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires an explicit key-equality canonicalizer",
		)
	}
	sourcePrimaryKey, err := deletePrimaryKeyColumns(
		request.SourceTable,
	)
	if err != nil {
		return deleteKeyPlan{}, err
	}
	targetPrimaryKey, err := deletePrimaryKeyColumns(
		request.TargetTable,
	)
	if err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"target delete primary key: %w",
			err,
		)
	}
	if len(sourcePrimaryKey) != len(targetPrimaryKey) {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation source and target primary-key widths differ",
		)
	}
	sourceColumns := make([]string, len(sourcePrimaryKey))
	targetColumns := make([]string, len(targetPrimaryKey))
	for index := range sourcePrimaryKey {
		sourceColumns[index] = sourcePrimaryKey[index].Name
		targetColumns[index] = targetPrimaryKey[index].Name
		if sourceColumns[index] != targetColumns[index] {
			return deleteKeyPlan{}, fmt.Errorf(
				"delete reconciliation source and target primary-key column %d differs",
				index+1,
			)
		}
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(
		request.SourceTable,
		request.TargetTable,
		sourcePrimaryKey,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"prove delete key equality: %w",
			err,
		)
	}
	proofDigest, err := validateDeleteKeyEqualityProof(
		proof,
		request.SourceTable,
		request.TargetTable,
		sourcePrimaryKey,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyPlan{}, err
	}
	return deleteKeyPlan{
		sourcePrimaryKey: sourcePrimaryKey,
		targetPrimaryKey: targetPrimaryKey,
		sourceColumns:    sourceColumns,
		targetColumns:    targetColumns,
		proof:            proof,
		proofDigest:      proofDigest,
	}, nil
}

func deletePrimaryKeyColumns(
	table schema.Table,
) ([]schema.Column, error) {
	seenNames := make(map[string]struct{}, len(table.Columns))
	positions := make(map[int]schema.Column)
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"delete reconciliation table %s has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := seenNames[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"delete reconciliation table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		seenNames[column.Name] = struct{}{}
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return nil, fmt.Errorf(
					"delete reconciliation table %s non-primary-key column %s has a key position",
					table.Name,
					column.Name,
				)
			}
			continue
		}
		if column.Nullable {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key column %s is nullable",
				table.Name,
				column.Name,
			)
		}
		if column.PrimaryKeyPosition <= 0 {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key column %s has no positive position",
				table.Name,
				column.Name,
			)
		}
		if previous, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key position %d is shared by %s and %s",
				table.Name,
				column.PrimaryKeyPosition,
				previous.Name,
				column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = column
	}
	if len(positions) == 0 {
		return nil, fmt.Errorf(
			"delete reconciliation table %s has no primary key",
			table.Name,
		)
	}
	primaryKey := make([]schema.Column, len(positions))
	for position := 1; position <= len(positions); position++ {
		column, exists := positions[position]
		if !exists {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key positions are not contiguous from one",
				table.Name,
			)
		}
		kind, err := validationKindForColumn(column)
		if err != nil {
			return nil, fmt.Errorf(
				"delete reconciliation primary-key column %s: %w",
				column.Name,
				err,
			)
		}
		if kind == validationDynamic {
			return nil, fmt.Errorf(
				"delete reconciliation rejects dynamic primary-key column %s",
				column.Name,
			)
		}
		primaryKey[position-1] = column
	}
	return primaryKey, nil
}
