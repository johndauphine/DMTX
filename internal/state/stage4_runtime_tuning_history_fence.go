package state

// fencedStage4RuntimeTuningHistoryBackend is intentionally constructed only
// through the resolver below. It keeps history writes behind the exact same
// run/lease generation check as every other Stage 4 mutation while preserving
// read-only diagnostics after a lease loss.
type fencedStage4RuntimeTuningHistoryBackend struct {
	*fencedBackend
	history Stage4RuntimeTuningHistoryBackend
}

// ResolveStage4RuntimeTuningHistory is promoted through every optional fenced
// aggregate/rebuild/readiness wrapper. FenceBackend therefore advertises the
// history resolver only when the underlying backend actually supports it,
// without widening any existing optional state capability.
func (backend *fencedBackend) ResolveStage4RuntimeTuningHistory() (
	Stage4RuntimeTuningHistoryBackend,
	bool,
) {
	if backend == nil {
		return nil, false
	}
	history := backend.historyBackend()
	if stage4RuntimeTuningHistoryBackendIsNil(history) {
		return nil, false
	}
	return &fencedStage4RuntimeTuningHistoryBackend{
		fencedBackend: backend,
		history:       history,
	}, true
}

func (backend *fencedBackend) historyBackend() Stage4RuntimeTuningHistoryBackend {
	if backend == nil {
		return nil
	}
	history, ok := backend.backend.(Stage4RuntimeTuningHistoryBackend)
	if !ok || stage4RuntimeTuningHistoryBackendIsNil(history) {
		return nil
	}
	return history
}

func (backend *fencedStage4RuntimeTuningHistoryBackend) EnsureStage4RuntimeTuningSession(
	session Stage4RuntimeTuningSession,
) (Stage4RuntimeTuningSessionReceipt, bool, error) {
	var receipt Stage4RuntimeTuningSessionReceipt
	var created bool
	err := backend.protectRun(session.RunID, func() error {
		var err error
		receipt, created, err = backend.history.EnsureStage4RuntimeTuningSession(session)
		return err
	})
	return receipt, created, err
}

func (backend *fencedStage4RuntimeTuningHistoryBackend) LoadStage4RuntimeTuningSession(
	runID, sessionID string,
) (Stage4RuntimeTuningSessionReceipt, bool, error) {
	return backend.history.LoadStage4RuntimeTuningSession(runID, sessionID)
}

func (backend *fencedStage4RuntimeTuningHistoryBackend) EnsureStage4RuntimeTuningDecision(
	decision Stage4RuntimeTuningDecision,
) (Stage4RuntimeTuningDecisionReceipt, bool, error) {
	var receipt Stage4RuntimeTuningDecisionReceipt
	var created bool
	err := backend.protectRun(decision.RunID, func() error {
		var err error
		receipt, created, err = backend.history.EnsureStage4RuntimeTuningDecision(decision)
		return err
	})
	return receipt, created, err
}

func (backend *fencedStage4RuntimeTuningHistoryBackend) LoadStage4RuntimeTuningDecisions(
	runID, sessionID string,
) ([]Stage4RuntimeTuningDecisionReceipt, error) {
	return backend.history.LoadStage4RuntimeTuningDecisions(runID, sessionID)
}

var _ Stage4RuntimeTuningHistoryResolver = (*fencedBackend)(nil)
var _ Stage4RuntimeTuningHistoryBackend = (*fencedStage4RuntimeTuningHistoryBackend)(nil)
