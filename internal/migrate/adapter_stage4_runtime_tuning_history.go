package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4AdapterRuntimeTuningHistorySession is one invocation-local controller
// identity. The state receipt persists controller intent and decisions, but
// never becomes input to a later controller: a resume uses a fresh controller
// and a fresh session ID rather than rehydrating adaptive values.
type stage4AdapterRuntimeTuningHistorySession struct {
	session state.Stage4RuntimeTuningSession
	sink    *stage4AdapterRuntimeTuningDecisionSink
}

// stage4AdapterRuntimeTuningDecisionSink serializes receipt chaining for one
// controller. The core already serializes observations before invoking it; the
// mutex additionally makes direct/fault tests and future callers safe.
type stage4AdapterRuntimeTuningDecisionSink struct {
	mu       sync.Mutex
	backend  state.Stage4RuntimeTuningHistoryBackend
	session  state.Stage4RuntimeTuningSession
	receipts map[uint64]state.Stage4RuntimeTuningDecisionReceipt
}

var stage4RuntimeTuningHistorySessionID = func() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate runtime-tuning session ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

var stage4RuntimeTuningHistorySessionNow = func() time.Time {
	return time.Now().UTC()
}

// bindRuntimeTuningHistory installs an optional full-local history sink after
// exact work/restores are durable and before a caller can reach PrepareTables.
// YAML intentionally has no resolver and therefore retains only the existing
// current-run Result report. A raw SQLite backend is likewise ignored here:
// production history is valid only through state.FenceBackend.
func (execution *stage4AdapterNetworkTableExecution) bindRuntimeTuningHistory(
	ctx context.Context,
) error {
	if execution == nil || execution.corePlan.RuntimeTuning == nil {
		return nil
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 runtime-tuning history context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	history, available := state.ResolveFencedStage4RuntimeTuningHistory(
		execution.parent.prepared.run.Backend,
	)
	if !available {
		return nil
	}
	entry, err := execution.parent.runtimeTuningHistoryForTable(
		ctx,
		execution,
		history,
	)
	if err != nil {
		return err
	}
	execution.corePlan.RuntimeTuningSink = entry.sink
	if _, err := validateNetworkTransferPlan(
		execution.corePlan,
		execution.callbacks(nil),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"validate Stage 4 runtime-tuning history binding for %s: %w",
				execution.work.task.Table,
				err,
			),
		)
	}
	return nil
}

func (execution *stage4AdapterNetworkExecution) runtimeTuningHistoryForTable(
	ctx context.Context,
	table *stage4AdapterNetworkTableExecution,
	backend state.Stage4RuntimeTuningHistoryBackend,
) (*stage4AdapterRuntimeTuningHistorySession, error) {
	if execution == nil || table == nil || table.corePlan.RuntimeTuning == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 runtime-tuning history table execution is unavailable"),
		)
	}
	if table.planIndex < 0 || table.planIndex >= len(execution.prepared.plans) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 runtime-tuning history table index is invalid"),
		)
	}
	execution.mu.Lock()
	if execution.runtimeTuningHistory == nil {
		execution.runtimeTuningHistory = make(
			map[int]*stage4AdapterRuntimeTuningHistorySession,
		)
	}
	entry := execution.runtimeTuningHistory[table.planIndex]
	if entry == nil {
		sessionID, err := stage4RuntimeTuningHistorySessionID()
		if err != nil {
			execution.mu.Unlock()
			return nil, NewTransferError(ErrorClassState, err)
		}
		session, err := stage4AdapterRuntimeTuningStateSession(
			execution,
			table,
			sessionID,
		)
		if err != nil {
			execution.mu.Unlock()
			return nil, err
		}
		entry = &stage4AdapterRuntimeTuningHistorySession{
			session: session,
			sink: &stage4AdapterRuntimeTuningDecisionSink{
				backend:  backend,
				session:  session,
				receipts: make(map[uint64]state.Stage4RuntimeTuningDecisionReceipt),
			},
		}
		execution.runtimeTuningHistory[table.planIndex] = entry
	}
	execution.mu.Unlock()
	current, err := stage4AdapterRuntimeTuningStateSession(
		execution,
		table,
		entry.session.SessionID,
	)
	if err != nil {
		return nil, err
	}
	// The invocation-local session ID/time stays fixed after its first durable
	// attempt, while every other authority field must reproduce exactly when a
	// table is reopened for execution after checkpoint planning.
	current.StartedAt = entry.session.StartedAt
	if !current.Equal(entry.session) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 runtime-tuning session authority changed for %s",
				table.work.task.Table,
			),
		)
	}

	receipt, _, err := backend.EnsureStage4RuntimeTuningSession(entry.session)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"persist Stage 4 runtime-tuning session for %s: %w",
				table.work.task.Table,
				err,
			),
		)
	}
	if !receipt.Session.Equal(entry.session) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"durable Stage 4 runtime-tuning session differs for %s",
				table.work.task.Table,
			),
		)
	}
	if err := entry.sink.load(ctx); err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"load Stage 4 runtime-tuning history for %s: %w",
				table.work.task.Table,
				err,
			),
		)
	}
	return entry, nil
}

func stage4AdapterRuntimeTuningStateSession(
	execution *stage4AdapterNetworkExecution,
	table *stage4AdapterNetworkTableExecution,
	sessionID string,
) (state.Stage4RuntimeTuningSession, error) {
	if execution == nil || table == nil || table.corePlan.RuntimeTuning == nil {
		return state.Stage4RuntimeTuningSession{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 runtime-tuning session requires a controller"),
		)
	}
	snapshot := table.corePlan.RuntimeTuning.Snapshot()
	if snapshot.HasBoundary || snapshot.TotalDecisions != 0 ||
		snapshot.RetainedDecisions != 0 || snapshot.Interval <= 0 {
		return state.Stage4RuntimeTuningSession{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 runtime-tuning session controller is not pristine"),
		)
	}
	intentDigest, err := stage4AdapterRuntimeTuningIntentDigest(table)
	if err != nil {
		return state.Stage4RuntimeTuningSession{}, err
	}
	return state.Stage4RuntimeTuningSession{
		Version:       state.Stage4RuntimeTuningHistoryVersion,
		RunID:         execution.prepared.run.RunID,
		SessionID:     sessionID,
		Resume:        execution.prepared.run.Resume,
		Task:          table.work.task,
		TopologyHash:  table.work.topology,
		SourceEngine:  table.corePlan.SourceEngine,
		TargetEngine:  table.corePlan.TargetEngine,
		IntentDigest:  intentDigest,
		IntervalNanos: int64(snapshot.Interval),
		DecisionLimit: table.corePlan.RuntimeTuning.limits.HistoryLimit,
		StartedAt:     stage4RuntimeTuningHistorySessionNow(),
	}, nil
}

func stage4AdapterRuntimeTuningIntentDigest(
	table *stage4AdapterNetworkTableExecution,
) (string, error) {
	if table == nil || table.corePlan.RuntimeTuning == nil {
		return "", fmt.Errorf("runtime-tuning intent controller is unavailable")
	}
	authority := struct {
		Version       int
		SourceEngine  string
		TargetEngine  string
		Resources     config.EffectiveTransferPlan
		RowWidth      RuntimeRowWidthEvidence
		Limits        RuntimeTuningLimits
		RangeCount    int
		IntervalNanos int64
	}{
		Version:       state.Stage4RuntimeTuningHistoryVersion,
		SourceEngine:  table.corePlan.SourceEngine,
		TargetEngine:  table.corePlan.TargetEngine,
		Resources:     table.corePlan.Resources,
		RowWidth:      table.corePlan.RowWidth,
		Limits:        table.corePlan.RuntimeTuning.limits,
		RangeCount:    len(table.corePlan.Ranges),
		IntervalNanos: int64(table.corePlan.RuntimeTuning.Snapshot().Interval),
	}
	payload, err := json.Marshal(authority)
	if err != nil {
		return "", fmt.Errorf("encode Stage 4 runtime-tuning intent: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (sink *stage4AdapterRuntimeTuningDecisionSink) load(
	ctx context.Context,
) error {
	if sink == nil || sink.backend == nil {
		return fmt.Errorf("runtime-tuning history sink is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	decisions, err := sink.backend.LoadStage4RuntimeTuningDecisions(
		sink.session.RunID,
		sink.session.SessionID,
	)
	if err != nil {
		return err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.receipts == nil {
		sink.receipts = make(map[uint64]state.Stage4RuntimeTuningDecisionReceipt)
	}
	for _, receipt := range decisions {
		if existing, found := sink.receipts[receipt.Decision.Boundary.Ordinal]; found && !existing.Equal(receipt) {
			return fmt.Errorf("runtime-tuning history receipt differs during load")
		}
		sink.receipts[receipt.Decision.Boundary.Ordinal] = receipt.Clone()
	}
	return nil
}

func (sink *stage4AdapterRuntimeTuningDecisionSink) PersistRuntimeTuningDecision(
	ctx context.Context,
	snapshot RuntimeTuningSnapshot,
	decision RuntimeTuningDecision,
) error {
	if sink == nil || sink.backend == nil {
		return fmt.Errorf("runtime-tuning history sink is unavailable")
	}
	if ctx == nil {
		return fmt.Errorf("runtime-tuning history context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if !snapshot.HasBoundary || snapshot.LastBoundary != decision.Boundary ||
		snapshot.TotalDecisions != decision.Boundary.Ordinal ||
		snapshot.AppliedBoundaries != decision.Boundary.Ordinal ||
		snapshot.Interval != time.Duration(sink.session.IntervalNanos) {
		return fmt.Errorf("runtime-tuning decision does not match controller snapshot")
	}
	if sink.receipts == nil {
		sink.receipts = make(map[uint64]state.Stage4RuntimeTuningDecisionReceipt)
	}
	previous := ""
	if decision.Boundary.Ordinal > 1 {
		prior, found := sink.receipts[decision.Boundary.Ordinal-1]
		if !found {
			return fmt.Errorf("runtime-tuning decision lacks prior durable receipt")
		}
		previous = prior.Digest
	}
	stateDecision := stage4AdapterRuntimeTuningStateDecision(
		sink.session,
		decision,
		previous,
	)
	if existing, found := sink.receipts[decision.Boundary.Ordinal]; found {
		if !existing.Decision.Equal(stateDecision) {
			return fmt.Errorf("runtime-tuning decision replay differs from durable receipt")
		}
		return nil
	}
	receipt, _, err := sink.backend.EnsureStage4RuntimeTuningDecision(
		stateDecision,
	)
	if err != nil {
		return err
	}
	if !receipt.Decision.Equal(stateDecision) {
		return fmt.Errorf("durable runtime-tuning decision differs")
	}
	sink.receipts[decision.Boundary.Ordinal] = receipt.Clone()
	minimum := uint64(1)
	if decision.Boundary.Ordinal >= uint64(sink.session.DecisionLimit) {
		minimum = decision.Boundary.Ordinal - uint64(sink.session.DecisionLimit) + 1
	}
	for ordinal := range sink.receipts {
		if ordinal < minimum {
			delete(sink.receipts, ordinal)
		}
	}
	return nil
}

func stage4AdapterRuntimeTuningStateDecision(
	session state.Stage4RuntimeTuningSession,
	decision RuntimeTuningDecision,
	previous string,
) state.Stage4RuntimeTuningDecision {
	return state.Stage4RuntimeTuningDecision{
		Version:   state.Stage4RuntimeTuningHistoryVersion,
		RunID:     session.RunID,
		SessionID: session.SessionID,
		Boundary: state.Stage4RuntimeTuningBoundary{
			Ordinal:       decision.Boundary.Ordinal,
			TableSchema:   decision.Boundary.TableSchema,
			TableName:     decision.Boundary.TableName,
			RangeIndex:    decision.Boundary.RangeIndex,
			ChunkSequence: decision.Boundary.ChunkSequence,
			Attempt:       decision.Boundary.Attempt,
		},
		Before:         stage4AdapterRuntimeTuningStateValues(decision.Before),
		After:          stage4AdapterRuntimeTuningStateValues(decision.After),
		Reasons:        stage4AdapterRuntimeTuningStateReasons(decision.Reasons),
		PreviousDigest: previous,
	}
}

func stage4AdapterRuntimeTuningStateValues(
	values RuntimeTuningValues,
) state.Stage4RuntimeTuningValues {
	return state.Stage4RuntimeTuningValues{
		ChunkRows:   stage4AdapterRuntimeTuningStateValue(values.ChunkRows),
		Writers:     stage4AdapterRuntimeTuningStateValue(values.Writers),
		BufferDepth: stage4AdapterRuntimeTuningStateValue(values.BufferDepth),
	}
}

func stage4AdapterRuntimeTuningStateValue(
	value RuntimeTuningValue,
) state.Stage4RuntimeTuningValue {
	return state.Stage4RuntimeTuningValue{
		Value:             value.Value,
		IntentValue:       value.IntentValue,
		IntentProvenance:  string(value.IntentProvenance),
		LiveProvenance:    string(value.LiveProvenance),
		PerformancePinned: value.PerformancePinned,
	}
}

func stage4AdapterRuntimeTuningStateReasons(
	reasons []RuntimeTuningReason,
) []string {
	result := make([]string, len(reasons))
	for index, reason := range reasons {
		result[index] = string(reason)
	}
	return result
}

var _ RuntimeTuningDecisionSink = (*stage4AdapterRuntimeTuningDecisionSink)(nil)
