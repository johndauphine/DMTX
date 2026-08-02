package app

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

// Abandonment and terminal repair: ending a run deliberately, and finalizing
// one whose success was persisted but whose audit or lifecycle boundary did
// not complete.

func abandonResumeRun(out *outcomeBuilder, configPath string, cfg config.Config, run state.Run, store state.Backend, reason string) Outcome {
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		return out.failWith(StateError, "acquire target lease for abandonment: "+err.Error())
	}
	guard := state.NewLeaseGuard(leaseStore, lease)
	store = state.FenceBackend(store, guard)
	released := false
	defer func() {
		if !released {
			_ = guard.Release()
		}
	}()
	authoritative, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		return out.failWith(StateError, "reselect migration run for abandonment: "+err.Error())
	}
	if !found {
		return out.failWith(StateError, "abandon candidate disappeared after target lease acquisition")
	}
	if authoritative.Outcome == state.Success {
		return out.failWith(StateError, "abandon candidate was superseded by a successful run after target lease acquisition")
	}
	if !sameResumeCandidate(run, authoritative) {
		return out.failWith(StateError, "abandon candidate changed after target lease acquisition")
	}
	run = authoritative
	if err := store.BindRunLease(run.ID, lease); err != nil {
		return out.failWith(StateError, "bind abandoned run to target lease: "+err.Error())
	}
	if err := store.AbandonRun(run.ID, reason, time.Now().UTC()); err != nil {
		return out.failWith(StateError, "abandon run: "+err.Error())
	}
	if err := appendAudit(configPath, run.ID, "run_abandoned"); err != nil {
		return out.failWith(StateError, err.Error())
	}
	if err := guard.Release(); err != nil {
		return out.failWith(StateError, "release abandonment lease: "+err.Error())
	}
	released = true
	response := struct {
		RunID     string `json:"run_id"`
		Outcome   string `json:"outcome"`
		Resumable bool   `json:"resumable"`
	}{
		RunID: run.ID, Outcome: string(state.Failed), Resumable: false,
	}
	if run.Outcome == state.Partial {
		response.Outcome = string(state.Partial)
	}
	if err := out.setPayload(PayloadResumeResponse, response); err != nil {
		return out.failWith(FileError, "write abandonment result: "+err.Error())
	}
	return out.done(Success)
}
func finalizePersistedSuccess(
	out *outcomeBuilder,
	configPath string,
	cfg config.Config,
	configHashCandidates []string,
	run state.Run,
	store state.Backend,
) Outcome {
	if err := appLifecycleBoundary("resume_terminal_candidate_selected"); err != nil {
		return out.failWith(StateError, "resume lifecycle: "+err.Error())
	}
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		return out.failWith(StateError, "acquire target lease for terminal repair: "+err.Error())
	}
	guard := state.NewLeaseGuard(leaseStore, lease)
	store = state.FenceBackend(store, guard)
	leaseReleased := false
	_, heartbeat := startLeaseHeartbeat(context.Background(), guard, 30*time.Second)
	heartbeatStopped := false
	defer func() {
		if !heartbeatStopped {
			_ = heartbeat.Stop()
		}
		if !leaseReleased {
			_ = guard.Release()
		}
	}()
	authoritative, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		return out.failWith(StateError, "reselect successful migration run: "+err.Error())
	}
	if !found || authoritative.Outcome != state.Success ||
		!sameResumeCandidate(run, authoritative) {
		out.fail("terminal repair candidate changed after target lease acquisition")
		return out.done(StateError)
	}
	run = authoritative
	storedHash, hashFound, err := store.ConfigHash(run.ID)
	if err != nil {
		return out.failWith(StateError, "read configuration hash: "+err.Error())
	}
	if !hashFound {
		return out.failWith(ConfigurationError, "successful run is missing its data-plane configuration hash")
	}
	if !matchesHashCandidate(storedHash, configHashCandidates) {
		out.fail("force-resume cannot rewrite configuration evidence for terminal-state repair")
		return out.done(ConfigurationError)
	}
	auditPath := configPath + ".audit.ndjson"
	for _, terminalType := range []string{"run_succeeded", "resume_succeeded"} {
		found, err := audit.HasEvent(auditPath, run.ID, terminalType)
		if err != nil {
			return out.failWith(StateError, "inspect terminal audit: "+err.Error())
		}
		if found {
			return out.failWith(StateError, "no resumable run exists for this target")
		}
	}
	var terminalType string
	switch run.Reason {
	case runSuccessReason:
		terminalType = "run_succeeded"
	case resumeSuccessReason:
		terminalType = "resume_succeeded"
	default:
		out.fail(fmt.Sprintf("successful run has unknown completion provenance %q; refusing terminal repair", run.Reason))
		return out.done(StateError)
	}
	validated, err := audit.HasEvent(auditPath, run.ID, "validation_completed")
	if err != nil {
		return out.failWith(StateError, "inspect validation audit: "+err.Error())
	}
	if !validated {
		return out.failWith(StateError, "successful run is missing its validation audit; refusing terminal repair")
	}
	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		return out.failWith(StateError, "read completed table checkpoints: "+err.Error())
	}
	if len(tasks) == 0 {
		return out.failWith(StateError, "successful run has no completed table checkpoints; refusing terminal repair")
	}
	result := migrate.Result{Validated: true}
	for _, task := range tasks {
		if task.Status != "completed" {
			out.fail(fmt.Sprintf("successful run has incomplete table checkpoint %q; refusing terminal repair", task.Table))
			return out.done(StateError)
		}
		result.Tables++
		result.Rows += task.RowsDone
	}

	if err := store.BindRunLease(run.ID, lease); err != nil {
		return out.failWith(StateError, "bind terminal repair to target lease: "+err.Error())
	}
	if err := appendAudit(configPath, run.ID, "resume_finalization_started"); err != nil {
		_ = heartbeat.Stop()
		heartbeatStopped = true
		return out.failWith(StateError, err.Error())
	}
	heartbeatErr := heartbeat.Stop()
	heartbeatStopped = true
	if heartbeatErr != nil {
		return out.failWith(StateError, "terminal repair lease heartbeat: "+heartbeatErr.Error())
	}
	if err := guard.Renew(); err != nil {
		return out.failWith(StateError, "verify terminal repair lease: "+err.Error())
	}
	if err := appendAudit(configPath, run.ID, terminalType); err != nil {
		return out.failWith(StateError, err.Error())
	}
	if err := guard.Release(); err != nil {
		return out.failWith(StateError, "release terminal repair lease: "+err.Error())
	}
	leaseReleased = true
	if err := out.setPayload(PayloadResult, result); err != nil {
		return out.failWith(FileError, "write result: "+err.Error())
	}
	return out.done(Success)
}

func matchesHashCandidate(stored string, candidates []string) bool {
	for _, candidate := range candidates {
		if stored == candidate {
			return true
		}
	}
	return false
}

type resumeCheckpointObserver struct {
	tableCheckpointObserver
	existing map[string]bool
}

func (observer resumeCheckpointObserver) BeforeTables(ctx context.Context, tables []string) error {
	discovered := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		discovered[table] = struct{}{}
	}
	unexpected := make([]string, 0)
	for table := range observer.existing {
		if _, ok := discovered[table]; !ok {
			unexpected = append(unexpected, table)
		}
	}
	if len(unexpected) > 0 {
		sort.Strings(unexpected)
		return stateCheckpointError(
			"validate resumed table set",
			fmt.Errorf(
				"%w: persisted checkpoints were not rediscovered for %q",
				state.ErrTopologyChanged,
				unexpected,
			),
		)
	}

	missing := make([]string, 0, len(tables))
	for _, table := range tables {
		if !observer.existing[table] {
			missing = append(missing, table)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return observer.tableCheckpointObserver.BeforeTables(ctx, missing)
}

func (observer resumeCheckpointObserver) BeforeTable(ctx context.Context, table string) error {
	if observer.existing[table] {
		return nil
	}
	return observer.tableCheckpointObserver.BeforeTable(ctx, table)
}
