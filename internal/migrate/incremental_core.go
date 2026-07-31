package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// IncrementalTemporalKind is an adapter-normalized temporal family. Adapters
// must admit a catalog type explicitly; the core never guesses from executable
// or engine-specific type text.
type IncrementalTemporalKind string

const (
	IncrementalTemporalNone      IncrementalTemporalKind = ""
	IncrementalTemporalDate      IncrementalTemporalKind = "date"
	IncrementalTemporalTimestamp IncrementalTemporalKind = "timestamp"
)

// IncrementalOrderAdmission records whether an adapter has proved that bound
// values, source comparison, and ORDER BY use the same deterministic order.
type IncrementalOrderAdmission string

const (
	IncrementalOrderExact IncrementalOrderAdmission = "exact"
)

// IncrementalColumn is the engine-neutral metadata needed to select a
// date-updated column and derive a complete stable row order.
type IncrementalColumn struct {
	Name               string
	TemporalKind       IncrementalTemporalKind
	Nullable           bool
	PrimaryKeyPosition int
	OrderAdmission     IncrementalOrderAdmission
}

// IncrementalTable is intentionally smaller than schema.Table. Source
// adapters construct it only after catalog types and ordering semantics have
// been admitted.
type IncrementalTable struct {
	Schema  string
	Name    string
	Columns []IncrementalColumn
}

type IncrementalCandidateAction string

const (
	IncrementalCandidateMissing          IncrementalCandidateAction = "missing"
	IncrementalCandidateIncompatibleType IncrementalCandidateAction = "incompatible_type"
	IncrementalCandidateUnsafeOrder      IncrementalCandidateAction = "unsafe_order"
	IncrementalCandidateSelected         IncrementalCandidateAction = "selected"
)

// IncrementalCandidateDecision makes the ordered candidate selection
// observable. A missing or incompatible earlier candidate does not hide a
// later compatible candidate.
type IncrementalCandidateDecision struct {
	Candidate string
	Action    IncrementalCandidateAction
	Reason    string
}

type IncrementalOrderRole string

const (
	IncrementalOrderUpdateColumn IncrementalOrderRole = "update_column"
	IncrementalOrderPrimaryKey   IncrementalOrderRole = "primary_key"
)

type IncrementalNullOrder string

const (
	IncrementalNullsExcluded IncrementalNullOrder = "excluded"
	IncrementalNullsFirst    IncrementalNullOrder = "first"
)

// IncrementalOrderTerm is rendered by an adapter. For an incremental window,
// NULL timestamps are excluded by the predicate. A baseline explicitly orders
// them first before the complete primary key.
type IncrementalOrderTerm struct {
	Column string
	Role   IncrementalOrderRole
	Nulls  IncrementalNullOrder
}

// IncrementalTablePlan is deterministic and contains no SQL. FullTableUpsert
// means no compatible configured date column exists, so every run must replay
// the table by complete primary key.
type IncrementalTablePlan struct {
	Table              IncrementalTable
	DateColumn         *IncrementalColumn
	PrimaryKey         []IncrementalColumn
	Ordering           []IncrementalOrderTerm
	CandidateDecisions []IncrementalCandidateDecision
	FullTableUpsert    bool
	PlanHash           string
}

// NormalizeIncrementalDateColumns trims configuration whitespace while
// retaining candidate order and case. Case folding is deliberately left to an
// adapter because quoted identifier semantics differ by engine.
func NormalizeIncrementalDateColumns(candidates []string) ([]string, error) {
	normalized := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return nil, fmt.Errorf("date-updated candidate %d is empty", index)
		}
		if _, exists := seen[candidate]; exists {
			return nil, fmt.Errorf("date-updated candidate %q is duplicated", candidate)
		}
		seen[candidate] = struct{}{}
		normalized = append(normalized, candidate)
	}
	return normalized, nil
}

// BuildIncrementalTablePlan selects the first existing compatible configured
// temporal column and proves a complete primary-key order. Unknown metadata is
// rejected rather than silently downgraded to an unsafe transfer.
func BuildIncrementalTablePlan(
	table IncrementalTable,
	dateUpdatedColumns []string,
) (IncrementalTablePlan, error) {
	if strings.TrimSpace(table.Name) == "" {
		return IncrementalTablePlan{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental table name is required"),
		)
	}
	candidates, err := NormalizeIncrementalDateColumns(dateUpdatedColumns)
	if err != nil {
		return IncrementalTablePlan{}, NewTransferError(ErrorClassPolicy, err)
	}
	columns := make(map[string]IncrementalColumn, len(table.Columns))
	primaryKey := make([]IncrementalColumn, 0)
	positions := make(map[int]string)
	for index, column := range table.Columns {
		if column.Name == "" {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("incremental table %s column %d has an empty name", table.Name, index),
			)
		}
		if _, exists := columns[column.Name]; exists {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("incremental table %s has duplicate column %q", table.Name, column.Name),
			)
		}
		switch column.TemporalKind {
		case IncrementalTemporalNone,
			IncrementalTemporalDate,
			IncrementalTemporalTimestamp:
		default:
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"incremental table %s column %s has unknown temporal admission %q",
					table.Name,
					column.Name,
					column.TemporalKind,
				),
			)
		}
		switch column.OrderAdmission {
		case "", IncrementalOrderExact:
		default:
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"incremental table %s column %s has unknown order admission %q",
					table.Name,
					column.Name,
					column.OrderAdmission,
				),
			)
		}
		if column.PrimaryKeyPosition < 0 {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"incremental table %s column %s has negative primary-key position",
					table.Name,
					column.Name,
				),
			)
		}
		if column.PrimaryKeyPosition > 0 {
			if previous, exists := positions[column.PrimaryKeyPosition]; exists {
				return IncrementalTablePlan{}, NewTransferError(
					ErrorClassPrimaryKey,
					fmt.Errorf(
						"incremental table %s primary-key position %d is shared by %s and %s",
						table.Name,
						column.PrimaryKeyPosition,
						previous,
						column.Name,
					),
				)
			}
			positions[column.PrimaryKeyPosition] = column.Name
			primaryKey = append(primaryKey, column)
		}
		columns[column.Name] = column
	}
	if len(primaryKey) == 0 {
		return IncrementalTablePlan{}, NewTransferError(
			ErrorClassPrimaryKey,
			fmt.Errorf(
				"incremental table %s has no primary key; duplicate-safe replay is impossible",
				table.Name,
			),
		)
	}
	sort.Slice(primaryKey, func(left, right int) bool {
		return primaryKey[left].PrimaryKeyPosition < primaryKey[right].PrimaryKeyPosition
	})
	for index, column := range primaryKey {
		expected := index + 1
		if column.PrimaryKeyPosition != expected {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"incremental table %s primary key is incomplete at position %d",
					table.Name,
					expected,
				),
			)
		}
		if column.Nullable {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"incremental table %s primary-key column %s is nullable",
					table.Name,
					column.Name,
				),
			)
		}
		if column.OrderAdmission != IncrementalOrderExact {
			return IncrementalTablePlan{}, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"incremental table %s primary-key column %s lacks exact ordering admission",
					table.Name,
					column.Name,
				),
			)
		}
	}

	plan := IncrementalTablePlan{
		Table:      cloneIncrementalTable(table),
		PrimaryKey: append([]IncrementalColumn(nil), primaryKey...),
	}
	for _, candidate := range candidates {
		column, exists := columns[candidate]
		switch {
		case !exists:
			plan.CandidateDecisions = append(
				plan.CandidateDecisions,
				IncrementalCandidateDecision{
					Candidate: candidate,
					Action:    IncrementalCandidateMissing,
					Reason:    "column does not exist in the selected source table",
				},
			)
		case column.TemporalKind != IncrementalTemporalDate &&
			column.TemporalKind != IncrementalTemporalTimestamp:
			plan.CandidateDecisions = append(
				plan.CandidateDecisions,
				IncrementalCandidateDecision{
					Candidate: candidate,
					Action:    IncrementalCandidateIncompatibleType,
					Reason:    "catalog type was not admitted as date or timestamp",
				},
			)
		case column.OrderAdmission != IncrementalOrderExact:
			plan.CandidateDecisions = append(
				plan.CandidateDecisions,
				IncrementalCandidateDecision{
					Candidate: candidate,
					Action:    IncrementalCandidateUnsafeOrder,
					Reason:    "bound temporal values do not have proven source ordering equivalence",
				},
			)
		default:
			selected := column
			plan.DateColumn = &selected
			plan.CandidateDecisions = append(
				plan.CandidateDecisions,
				IncrementalCandidateDecision{
					Candidate: candidate,
					Action:    IncrementalCandidateSelected,
					Reason:    "first existing compatible temporal column",
				},
			)
		}
		if plan.DateColumn != nil {
			break
		}
	}
	plan.FullTableUpsert = plan.DateColumn == nil
	if plan.DateColumn != nil {
		nullOrder := IncrementalNullsExcluded
		if plan.DateColumn.Nullable {
			nullOrder = IncrementalNullsFirst
		}
		plan.Ordering = append(plan.Ordering, IncrementalOrderTerm{
			Column: plan.DateColumn.Name,
			Role:   IncrementalOrderUpdateColumn,
			Nulls:  nullOrder,
		})
	}
	for _, column := range primaryKey {
		plan.Ordering = append(plan.Ordering, IncrementalOrderTerm{
			Column: column.Name,
			Role:   IncrementalOrderPrimaryKey,
		})
	}
	plan.PlanHash, err = incrementalPlanHash(plan)
	if err != nil {
		return IncrementalTablePlan{}, NewTransferError(ErrorClassPolicy, err)
	}
	return plan, nil
}

type IncrementalReadScope string

const (
	IncrementalReadFullTable IncrementalReadScope = "full_table"
	IncrementalReadWindow    IncrementalReadScope = "window"
)

// IncrementalWindow is (Lower, Upper]: lower is strict, upper is inclusive,
// and NULL is always outside the window. Empty is explicit because a nil
// sampled maximum means no non-NULL timestamp is currently readable.
type IncrementalWindow struct {
	Column         string
	Lower          *time.Time
	Upper          *time.Time
	LowerExclusive bool
	UpperInclusive bool
	ExcludeNull    bool
	Empty          bool
}

// Contains is primarily a contract/test helper for adapter predicate
// implementations.
func (window IncrementalWindow) Contains(value *time.Time) bool {
	if value == nil || window.Empty {
		return false
	}
	candidate := value.UTC()
	if window.Lower != nil && !candidate.After(window.Lower.UTC()) {
		return false
	}
	if window.Upper != nil && candidate.After(window.Upper.UTC()) {
		return false
	}
	return true
}

// IncrementalReadPlan carries only logical boundaries. In particular it has
// no positional or primary-key cursor, so a resume necessarily replays the
// whole changed window from the durable lower watermark.
type IncrementalReadPlan struct {
	Table                    IncrementalTable
	Scope                    IncrementalReadScope
	Ordering                 []IncrementalOrderTerm
	Window                   *IncrementalWindow
	Resumed                  bool
	ReplayFromLowerWatermark bool
	PositionalRestoreAllowed bool
}

// IncrementalState is the narrow state surface needed by the execution core.
// state.Stage4Backend satisfies it.
type IncrementalState interface {
	BeginIncrementalAttempt(state.IncrementalAttempt) (state.IncrementalAttempt, bool, error)
	LoadActiveIncrementalAttempt(string, state.TaskKey) (state.IncrementalAttempt, bool, error)
	LoadLatestCommittedIncrementalAttempt(string, state.TaskKey) (state.IncrementalAttempt, bool, error)
	CommitIncrementalAttempt(state.IncrementalCommit) error
}

// IncrementalFenceSampler samples MAX(non-NULL selected timestamp). It returns
// nil when the source currently has no non-NULL value.
type IncrementalFenceSampler func(
	context.Context,
	IncrementalTable,
	IncrementalColumn,
) (*time.Time, error)

// IncrementalTransfer executes the logical read plan and must durably finish
// all associated range work before returning success.
type IncrementalTransfer func(context.Context, IncrementalReadPlan) error

// IncrementalDurableBindingVerifier proves, before target transfer or a
// completed-table skip, that durable work-plan evidence binds this exact
// incremental plan hash and range topology. The callback must fail when either
// value differs from the persisted aggregate/range plan.
type IncrementalDurableBindingVerifier func(
	context.Context,
	state.IncrementalAttempt,
	string,
	string,
) error

// IncrementalCompletedTableVerifier revalidates the aggregate checkpoint and
// target row count before an already-completed table may be skipped on resume.
type IncrementalCompletedTableVerifier func(
	context.Context,
	state.IncrementalAttempt,
) error

// IncrementalCompletionPublisher atomically publishes an incremental
// watermark with the enclosing table-success evidence. Production Stage 4
// composition supplies the aggregate table completion backend here; the
// legacy state commit remains only for isolated core callers.
type IncrementalCompletionPublisher func(
	context.Context,
	state.IncrementalCommit,
) error

type IncrementalExecutionRequest struct {
	State                IncrementalState
	RunID                string
	Task                 state.TaskKey
	AttemptID            string
	TopologyHash         string
	StartedAt            time.Time
	Plan                 IncrementalTablePlan
	SampleUpperFence     IncrementalFenceSampler
	VerifyDurableBinding IncrementalDurableBindingVerifier
	VerifyCompletedTable IncrementalCompletedTableVerifier
	Transfer             IncrementalTransfer
	PublishCompletion    IncrementalCompletionPublisher
	CompletionTime       func() time.Time
	ArmOnly              bool
}

type IncrementalExecutionResult struct {
	Read             IncrementalReadPlan
	Attempt          state.IncrementalAttempt
	CreatedAttempt   bool
	ResumedAttempt   bool
	AlreadyCompleted bool
	Armed            bool
	Completed        bool
}

// ExecuteIncrementalTable establishes immutable attempt evidence before any
// source row transfer, runs a full-table or half-open-window transfer, and asks
// the state backend to atomically publish table completion plus the exact fence.
func ExecuteIncrementalTable(
	ctx context.Context,
	request IncrementalExecutionRequest,
) (IncrementalExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return IncrementalExecutionResult{}, err
	}
	if err := validateIncrementalTablePlan(request.Plan); err != nil {
		return IncrementalExecutionResult{}, NewTransferError(ErrorClassPolicy, err)
	}
	if request.Transfer == nil && !request.ArmOnly {
		return IncrementalExecutionResult{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental transfer callback is required"),
		)
	}
	if request.Plan.FullTableUpsert {
		read := incrementalFullTableRead(request.Plan, false)
		if request.ArmOnly {
			return IncrementalExecutionResult{
				Read:  read,
				Armed: true,
			}, nil
		}
		if err := request.Transfer(ctx, read); err != nil {
			return IncrementalExecutionResult{Read: read}, fmt.Errorf(
				"transfer full-table upsert for %s: %w",
				request.Plan.Table.Name,
				err,
			)
		}
		if err := ctx.Err(); err != nil {
			return IncrementalExecutionResult{Read: read}, err
		}
		return IncrementalExecutionResult{Read: read, Completed: true}, nil
	}
	if nilIncrementalState(request.State) {
		return IncrementalExecutionResult{}, NewTransferError(
			ErrorClassState,
			errors.New("incremental state backend is required"),
		)
	}
	if strings.TrimSpace(request.TopologyHash) == "" {
		return IncrementalExecutionResult{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental topology hash is required"),
		)
	}
	if request.VerifyDurableBinding == nil {
		return IncrementalExecutionResult{}, NewTransferError(
			ErrorClassState,
			errors.New(
				"incremental durable plan/topology binding verifier is required",
			),
		)
	}
	if request.RunID == "" {
		return IncrementalExecutionResult{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental run ID is required"),
		)
	}
	if err := request.Task.Validate(); err != nil {
		return IncrementalExecutionResult{}, NewTransferError(ErrorClassPolicy, err)
	}

	active, found, err := request.State.LoadActiveIncrementalAttempt(
		request.RunID,
		request.Task,
	)
	if err != nil {
		return IncrementalExecutionResult{}, incrementalStateError(
			"load active incremental attempt",
			err,
		)
	}
	var result IncrementalExecutionResult
	switch {
	case found:
		if err := validateStoredIncrementalAttempt(request, active); err != nil {
			return result, err
		}
		if active.Status != state.IncrementalRunning {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"active incremental lookup returned terminal attempt %q",
					active.AttemptID,
				),
			)
		}
		result.Attempt = cloneIncrementalAttempt(active)
		result.ResumedAttempt = true
		latest, latestFound, err := request.State.LoadLatestCommittedIncrementalAttempt(
			request.RunID,
			request.Task,
		)
		if err != nil {
			return result, incrementalStateError(
				"load committed incremental frontier for active resume",
				err,
			)
		}
		if err := validateActiveIncrementalFrontier(
			request,
			result.Attempt,
			latest,
			latestFound,
		); err != nil {
			return result, err
		}
	default:
		latest, latestFound, err := request.State.LoadLatestCommittedIncrementalAttempt(
			request.RunID,
			request.Task,
		)
		if err != nil {
			return result, incrementalStateError(
				"load latest incremental watermark",
				err,
			)
		}
		if latestFound && latest.RunID == request.RunID {
			if err := validateStoredIncrementalAttempt(request, latest); err != nil {
				return result, err
			}
			if request.AttemptID == "" || request.AttemptID != latest.AttemptID {
				return result, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"completed incremental attempt %q cannot satisfy requested attempt %q; verify the target and repair state before an explicit replay",
						latest.AttemptID,
						request.AttemptID,
					),
				)
			}
			result.Attempt = cloneIncrementalAttempt(latest)
			return verifyCompletedIncrementalReuse(ctx, request, result)
		}
		attempt, err := beginIncrementalAttempt(ctx, request, latest, latestFound)
		if err != nil {
			return result, err
		}
		stored, created, err := request.State.BeginIncrementalAttempt(attempt)
		if err != nil {
			return result, incrementalStateError("persist immutable incremental fence", err)
		}
		if err := validateStoredIncrementalAttempt(request, stored); err != nil {
			return result, err
		}
		result.Attempt = cloneIncrementalAttempt(stored)
		result.CreatedAttempt = created
		result.ResumedAttempt = !created && stored.Status == state.IncrementalRunning
		if stored.Status == state.IncrementalCompleted {
			return verifyCompletedIncrementalReuse(ctx, request, result)
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := request.VerifyDurableBinding(
		ctx,
		cloneIncrementalAttempt(result.Attempt),
		request.Plan.PlanHash,
		request.TopologyHash,
	); err != nil {
		return result, incrementalStateError(
			"verify durable incremental plan/topology binding before transfer",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	read, err := incrementalAttemptRead(request.Plan, result.Attempt, result.ResumedAttempt)
	if err != nil {
		return result, NewTransferError(ErrorClassPolicy, err)
	}
	result.Read = read
	if request.ArmOnly {
		result.Armed = true
		return result, nil
	}
	if err := request.Transfer(ctx, read); err != nil {
		return result, fmt.Errorf(
			"transfer incremental table %s: %w",
			request.Plan.Table.Name,
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	completedAt := time.Now().UTC()
	if request.CompletionTime != nil {
		completedAt = request.CompletionTime().UTC()
	}
	if completedAt.IsZero() {
		return result, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental completion time is required"),
		)
	}
	watermark := result.Attempt.UpperFence
	if watermark == nil {
		watermark = result.Attempt.LowerWatermark
	}
	commit := state.IncrementalCommit{
		RunID:        request.RunID,
		Task:         request.Task,
		AttemptID:    result.Attempt.AttemptID,
		TopologyHash: request.TopologyHash,
		Watermark:    cloneTimestampWatermark(watermark),
		CompletedAt:  completedAt,
	}
	var commitErr error
	if request.PublishCompletion != nil {
		commitErr = request.PublishCompletion(ctx, commit)
	} else {
		commitErr = request.State.CommitIncrementalAttempt(commit)
	}
	if commitErr != nil {
		return result, incrementalPostTransferStateError(
			"atomically complete incremental table and watermark",
			commitErr,
		)
	}
	result.Completed = true
	return result, nil
}

func validateActiveIncrementalFrontier(
	request IncrementalExecutionRequest,
	active state.IncrementalAttempt,
	latest state.IncrementalAttempt,
	latestFound bool,
) error {
	if !latestFound {
		if active.Mode != state.IncrementalBaseline {
			return NewTransferError(
				ErrorClassState,
				errors.New(
					"active incremental window has no prior committed baseline frontier",
				),
			)
		}
		if active.LowerWatermark != nil {
			return NewTransferError(
				ErrorClassState,
				errors.New(
					"active baseline attempt unexpectedly contains a lower watermark",
				),
			)
		}
		return nil
	}
	historicalRequest := request
	historicalRequest.RunID = latest.RunID
	historicalRequest.AttemptID = ""
	if err := validateStoredIncrementalAttempt(historicalRequest, latest); err != nil {
		return err
	}
	if latest.RunID == request.RunID {
		return NewTransferError(
			ErrorClassState,
			errors.New(
				"active incremental attempt conflicts with completed evidence in the same run",
			),
		)
	}
	if active.Mode != state.IncrementalWindow {
		return NewTransferError(
			ErrorClassState,
			errors.New(
				"active incremental baseline conflicts with a prior committed frontier",
			),
		)
	}
	if !equalTimestampWatermark(
		active.LowerWatermark,
		latest.CommittedWatermark,
	) {
		return NewTransferError(
			ErrorClassState,
			errors.New(
				"active incremental lower watermark does not match the prior committed frontier",
			),
		)
	}
	return nil
}

func verifyCompletedIncrementalReuse(
	ctx context.Context,
	request IncrementalExecutionRequest,
	result IncrementalExecutionResult,
) (IncrementalExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := request.VerifyDurableBinding(
		ctx,
		cloneIncrementalAttempt(result.Attempt),
		request.Plan.PlanHash,
		request.TopologyHash,
	); err != nil {
		return result, incrementalStateError(
			"verify durable incremental plan/topology binding before completed-table reuse",
			err,
		)
	}
	if request.VerifyCompletedTable == nil {
		return result, NewTransferError(
			ErrorClassState,
			errors.New(
				"completed incremental table requires aggregate checkpoint and target-count revalidation before reuse",
			),
		)
	}
	if err := request.VerifyCompletedTable(
		ctx,
		cloneIncrementalAttempt(result.Attempt),
	); err != nil {
		return result, incrementalStateError(
			"revalidate completed incremental aggregate checkpoint and target row count",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.AlreadyCompleted = true
	result.Completed = true
	return result, nil
}

func beginIncrementalAttempt(
	ctx context.Context,
	request IncrementalExecutionRequest,
	latest state.IncrementalAttempt,
	latestFound bool,
) (state.IncrementalAttempt, error) {
	if request.AttemptID == "" {
		return state.IncrementalAttempt{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental attempt ID is required"),
		)
	}
	if request.StartedAt.IsZero() {
		return state.IncrementalAttempt{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental attempt start time is required"),
		)
	}
	if request.SampleUpperFence == nil {
		return state.IncrementalAttempt{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("incremental upper-fence sampler is required"),
		)
	}
	attempt := state.IncrementalAttempt{
		RunID:     request.RunID,
		Task:      request.Task,
		AttemptID: request.AttemptID,
		Mode:      state.IncrementalBaseline,
		StartedAt: request.StartedAt.UTC(),
	}
	if latestFound {
		if latest.Task != request.Task {
			return state.IncrementalAttempt{}, NewTransferError(
				ErrorClassState,
				errors.New("latest incremental watermark belongs to a different task"),
			)
		}
		historicalRequest := request
		historicalRequest.RunID = latest.RunID
		historicalRequest.AttemptID = ""
		if err := validateStoredIncrementalAttempt(historicalRequest, latest); err != nil {
			return state.IncrementalAttempt{}, err
		}
		if latest.CommittedWatermark != nil &&
			latest.CommittedWatermark.Column != request.Plan.DateColumn.Name {
			return state.IncrementalAttempt{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"selected date-updated column changed from %s to %s; explicit baseline reset is required",
					latest.CommittedWatermark.Column,
					request.Plan.DateColumn.Name,
				),
			)
		}
		attempt.Mode = state.IncrementalWindow
		attempt.LowerWatermark = cloneTimestampWatermark(latest.CommittedWatermark)
	}
	if err := ctx.Err(); err != nil {
		return state.IncrementalAttempt{}, err
	}
	sampled, err := request.SampleUpperFence(
		ctx,
		cloneIncrementalTable(request.Plan.Table),
		*request.Plan.DateColumn,
	)
	if err != nil {
		return state.IncrementalAttempt{}, fmt.Errorf(
			"sample immutable upper fence for %s: %w",
			request.Plan.Table.Name,
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return state.IncrementalAttempt{}, err
	}
	if sampled != nil {
		upper := state.TimestampWatermark{
			Column: request.Plan.DateColumn.Name,
			Value:  sampled.UTC(),
		}
		if attempt.LowerWatermark != nil &&
			upper.Value.Before(attempt.LowerWatermark.Value) {
			return state.IncrementalAttempt{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"sampled incremental upper fence %s regresses below durable lower watermark %s",
					upper.Value.Format(time.RFC3339Nano),
					attempt.LowerWatermark.Value.Format(time.RFC3339Nano),
				),
			)
		}
		attempt.UpperFence = &upper
	}
	return attempt, nil
}

func incrementalAttemptRead(
	plan IncrementalTablePlan,
	attempt state.IncrementalAttempt,
	resumed bool,
) (IncrementalReadPlan, error) {
	if attempt.Mode == state.IncrementalBaseline {
		read := incrementalFullTableRead(plan, resumed)
		read.ReplayFromLowerWatermark = resumed
		return read, nil
	}
	if attempt.Mode != state.IncrementalWindow {
		return IncrementalReadPlan{}, fmt.Errorf(
			"incremental attempt has unknown mode %q",
			attempt.Mode,
		)
	}
	window := &IncrementalWindow{
		Column:         plan.DateColumn.Name,
		LowerExclusive: true,
		UpperInclusive: true,
		ExcludeNull:    true,
		Empty:          attempt.UpperFence == nil,
	}
	if attempt.LowerWatermark != nil {
		lower := attempt.LowerWatermark.Value.UTC()
		window.Lower = &lower
	}
	if attempt.UpperFence != nil {
		upper := attempt.UpperFence.Value.UTC()
		window.Upper = &upper
	}
	window.Empty = window.Upper == nil ||
		(window.Lower != nil && !window.Upper.After(*window.Lower))
	return IncrementalReadPlan{
		Table:                    cloneIncrementalTable(plan.Table),
		Scope:                    IncrementalReadWindow,
		Ordering:                 windowIncrementalOrdering(plan.Ordering),
		Window:                   window,
		Resumed:                  resumed,
		ReplayFromLowerWatermark: true,
		PositionalRestoreAllowed: false,
	}, nil
}

func incrementalFullTableRead(
	plan IncrementalTablePlan,
	resumed bool,
) IncrementalReadPlan {
	return IncrementalReadPlan{
		Table:                    cloneIncrementalTable(plan.Table),
		Scope:                    IncrementalReadFullTable,
		Ordering:                 baselineIncrementalOrdering(plan.Ordering),
		Resumed:                  resumed,
		PositionalRestoreAllowed: false,
	}
}

func baselineIncrementalOrdering(ordering []IncrementalOrderTerm) []IncrementalOrderTerm {
	cloned := append([]IncrementalOrderTerm(nil), ordering...)
	for index := range cloned {
		if cloned[index].Role == IncrementalOrderUpdateColumn {
			cloned[index].Nulls = IncrementalNullsFirst
		}
	}
	return cloned
}

func windowIncrementalOrdering(ordering []IncrementalOrderTerm) []IncrementalOrderTerm {
	cloned := append([]IncrementalOrderTerm(nil), ordering...)
	for index := range cloned {
		if cloned[index].Role == IncrementalOrderUpdateColumn {
			cloned[index].Nulls = IncrementalNullsExcluded
		}
	}
	return cloned
}

func validateStoredIncrementalAttempt(
	request IncrementalExecutionRequest,
	attempt state.IncrementalAttempt,
) error {
	if attempt.RunID != request.RunID || attempt.Task != request.Task {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored incremental attempt identity differs from the requested task"),
		)
	}
	if strings.TrimSpace(attempt.RunID) == "" {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored incremental attempt has no run ID"),
		)
	}
	if err := attempt.Task.Validate(); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("stored incremental task identity: %w", err),
		)
	}
	if request.AttemptID != "" && request.AttemptID != attempt.AttemptID {
		if attempt.Status == state.IncrementalCompleted {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed incremental attempt %q cannot satisfy requested attempt %q; verify the target and repair state before an explicit replay",
					attempt.AttemptID,
					request.AttemptID,
				),
			)
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"stored incremental attempt is %q, not requested attempt %q",
				attempt.AttemptID,
				request.AttemptID,
			),
		)
	}
	if attempt.Status != state.IncrementalRunning &&
		attempt.Status != state.IncrementalCompleted {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("stored incremental attempt has unknown status %q", attempt.Status),
		)
	}
	if attempt.Mode != state.IncrementalBaseline &&
		attempt.Mode != state.IncrementalWindow {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("stored incremental attempt has unknown mode %q", attempt.Mode),
		)
	}
	if attempt.StartedAt.IsZero() {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored incremental attempt has no start time"),
		)
	}
	if strings.TrimSpace(attempt.AttemptID) == "" {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored incremental attempt has no attempt ID"),
		)
	}
	for _, evidence := range []struct {
		label     string
		watermark *state.TimestampWatermark
	}{
		{label: "lower watermark", watermark: attempt.LowerWatermark},
		{label: "upper fence", watermark: attempt.UpperFence},
		{label: "committed watermark", watermark: attempt.CommittedWatermark},
	} {
		if evidence.watermark == nil {
			continue
		}
		if strings.TrimSpace(evidence.watermark.Column) == "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("stored incremental %s has a blank column", evidence.label),
			)
		}
	}
	switch attempt.Status {
	case state.IncrementalRunning:
		if attempt.CommittedWatermark != nil || attempt.TableSucceeded ||
			!attempt.CompletedAt.IsZero() {
			return NewTransferError(
				ErrorClassState,
				errors.New("running incremental attempt contains terminal evidence"),
			)
		}
	case state.IncrementalCompleted:
		if !attempt.TableSucceeded || attempt.CompletedAt.IsZero() {
			return NewTransferError(
				ErrorClassState,
				errors.New("completed incremental attempt lacks durable table-success evidence"),
			)
		}
		expected := attempt.UpperFence
		if expected == nil {
			expected = attempt.LowerWatermark
		}
		if !equalTimestampWatermark(attempt.CommittedWatermark, expected) {
			return NewTransferError(
				ErrorClassState,
				errors.New("completed incremental attempt does not publish its exact safe fence"),
			)
		}
		if attempt.CompletedAt.Before(attempt.StartedAt) {
			return NewTransferError(
				ErrorClassState,
				errors.New("stored incremental completion precedes its start"),
			)
		}
	}
	if attempt.Mode == state.IncrementalBaseline && attempt.LowerWatermark != nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored baseline incremental attempt has a lower watermark"),
		)
	}
	for _, evidence := range []struct {
		label     string
		watermark *state.TimestampWatermark
	}{
		{label: "lower watermark", watermark: attempt.LowerWatermark},
		{label: "upper fence", watermark: attempt.UpperFence},
		{label: "committed watermark", watermark: attempt.CommittedWatermark},
	} {
		if evidence.watermark != nil &&
			evidence.watermark.Column != request.Plan.DateColumn.Name {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"stored incremental %s uses column %s, not selected column %s",
					evidence.label,
					evidence.watermark.Column,
					request.Plan.DateColumn.Name,
				),
			)
		}
	}
	if attempt.LowerWatermark != nil && attempt.UpperFence != nil &&
		attempt.UpperFence.Value.Before(attempt.LowerWatermark.Value) {
		return NewTransferError(
			ErrorClassState,
			errors.New("stored incremental upper fence regresses below its lower watermark"),
		)
	}
	return nil
}

func validateIncrementalTablePlan(plan IncrementalTablePlan) error {
	if plan.Table.Name == "" || len(plan.PrimaryKey) == 0 ||
		len(plan.Ordering) == 0 || plan.PlanHash == "" {
		return errors.New("incremental table plan is incomplete")
	}
	recomputed, err := incrementalPlanHash(plan)
	if err != nil {
		return err
	}
	if recomputed != plan.PlanHash {
		return errors.New("incremental table plan was mutated after planning")
	}
	if plan.FullTableUpsert != (plan.DateColumn == nil) {
		return errors.New("incremental full-table decision contradicts the selected date column")
	}
	return nil
}

func incrementalPlanHash(plan IncrementalTablePlan) (string, error) {
	wire := struct {
		Table              IncrementalTable
		DateColumn         *IncrementalColumn
		PrimaryKey         []IncrementalColumn
		Ordering           []IncrementalOrderTerm
		CandidateDecisions []IncrementalCandidateDecision
		FullTableUpsert    bool
	}{
		Table:              plan.Table,
		DateColumn:         plan.DateColumn,
		PrimaryKey:         plan.PrimaryKey,
		Ordering:           plan.Ordering,
		CandidateDecisions: plan.CandidateDecisions,
		FullTableUpsert:    plan.FullTableUpsert,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode incremental plan: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func incrementalStateError(operation string, err error) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf("%s: %w", operation, err),
	)
}

func incrementalPostTransferStateError(operation string, err error) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf(
			"%s: target writes may already be committed; repair state and resume the existing run; do not start a competing fresh run: %w",
			operation,
			err,
		),
	)
}

func cloneIncrementalTable(table IncrementalTable) IncrementalTable {
	table.Columns = append([]IncrementalColumn(nil), table.Columns...)
	return table
}

func cloneTimestampWatermark(
	watermark *state.TimestampWatermark,
) *state.TimestampWatermark {
	if watermark == nil {
		return nil
	}
	cloned := *watermark
	cloned.Value = cloned.Value.UTC()
	return &cloned
}

func cloneIncrementalAttempt(attempt state.IncrementalAttempt) state.IncrementalAttempt {
	attempt.LowerWatermark = cloneTimestampWatermark(attempt.LowerWatermark)
	attempt.UpperFence = cloneTimestampWatermark(attempt.UpperFence)
	attempt.CommittedWatermark = cloneTimestampWatermark(attempt.CommittedWatermark)
	return attempt
}

func equalTimestampWatermark(
	left *state.TimestampWatermark,
	right *state.TimestampWatermark,
) bool {
	switch {
	case left == nil || right == nil:
		return left == nil && right == nil
	default:
		return left.Column == right.Column && left.Value.Equal(right.Value)
	}
}

func nilIncrementalState(value IncrementalState) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
