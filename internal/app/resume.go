package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

type resumeOptions struct {
	configPath              string
	statePath               string
	destructiveAcknowledged bool
	forceResume             bool
	abandon                 bool
	abandonReason           string
}

func executeResume(ctx context.Context, request Request) Outcome {
	out := newOutcome(request.Command)
	migrationContext, stopSignals := signal.NotifyContext(
		ctx,
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	options, ok := resumeOptionsFrom(request)
	if !ok {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx resume --config migration.yaml [--state migration.state.yaml] [--acknowledge-destructive] [--force-resume] [--abandon --abandon-reason TEXT]",
		)
	}
	configPath, statePath := options.configPath, options.statePath
	data, err := os.ReadFile(options.configPath)
	if err != nil {
		return out.failWith(FileError, "read configuration: "+err.Error())
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}
	if err := config.ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}
	if err := migrate.ValidateMigration(cfg); err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}
	store, err := state.NewBackend(statePath)
	if err != nil {
		return out.failWith(StateError, "state backend: "+err.Error())
	}

	run, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		return out.failWith(StateError, "read migration run: "+err.Error())
	}
	if !found {
		return out.failWith(StateError, "no resumable run exists for this target")
	}
	sourceMatches, err := runSourceMatchesEndpoint(run, cfg.Source)
	if err != nil {
		return out.failWith(StateError, "compare resumable run source identity: "+err.Error())
	}
	if !sourceMatches {
		return out.failWith(ConfigurationError, "resumable run source does not match the supplied configuration")
	}
	if options.abandon {
		if err := appLifecycleBoundary("resume_candidate_selected"); err != nil {
			return out.failWith(StateError, "resume lifecycle: "+err.Error())
		}
		return abandonResumeRun(out, configPath, cfg, run, store, options.abandonReason)
	}
	hashConfig := cfg
	hashConfig.Source.Database = run.Source
	hashConfig.Target.Database = run.Target
	configHash, err := config.Hash(hashConfig)
	if err != nil {
		return out.failWith(StateError, "configuration hash: "+err.Error())
	}
	configHashCandidates, err :=
		config.SQLiteIdentityHashCandidates(hashConfig)
	if err != nil {
		return out.failWith(StateError, "SQLite identity configuration hashes: "+err.Error())
	}
	resumeCompatibilityHash, err := config.ResumeCompatibilityHash(hashConfig)
	if err != nil {
		return out.failWith(StateError, "resume compatibility hash: "+err.Error())
	}
	resumeCompatibilityHashCandidates, err :=
		config.SQLiteIdentityResumeCompatibilityHashCandidates(hashConfig)
	if err != nil {
		out.fail("SQLite identity resume compatibility hashes: " + err.Error())
		return out.done(StateError)
	}
	if run.Outcome == state.Success {
		return finalizePersistedSuccess(
			out,
			configPath,
			cfg,
			configHashCandidates,
			run,
			store,
		)
	}
	if !run.Resumable || run.Outcome != state.Running && run.Outcome != state.Failed &&
		run.Outcome != state.Cancelled && run.Outcome != state.Partial {
		return out.failWith(StateError, "no resumable run exists for this target")
	}
	if err := appLifecycleBoundary("resume_candidate_selected"); err != nil {
		return out.failWith(StateError, "resume lifecycle: "+err.Error())
	}

	cfg.Migration.DestructiveAcknowledged = options.destructiveAcknowledged
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		return out.failWith(StateError, "acquire target lease: "+err.Error())
	}
	guard := state.NewLeaseGuard(leaseStore, lease)
	leaseReleased := false
	defer func() {
		if !leaseReleased {
			_ = guard.Release()
		}
	}()
	store, err = newStage4FencedStateBackend(store, guard)
	if err != nil {
		return out.failWith(StateError, "fence Stage 4 state backend: "+err.Error())
	}
	authoritative, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		return out.failWith(StateError, "reselect migration run: "+err.Error())
	}
	if !found {
		return out.failWith(StateError, "resume candidate disappeared after target lease acquisition")
	}
	if authoritative.Outcome == state.Success {
		return out.failWith(StateError, "resume candidate was superseded by a successful run after target lease acquisition")
	}
	if !sameResumeCandidate(run, authoritative) {
		return out.failWith(StateError, "resume candidate changed after target lease acquisition")
	}
	run = authoritative
	storedHash, hashFound, err := store.ConfigHash(run.ID)
	if err != nil {
		return out.failWith(StateError, "read configuration hash: "+err.Error())
	}
	if !hashFound {
		return out.failWith(ConfigurationError, "resumable run is missing its data-plane configuration hash")
	}
	configOverride := !matchesHashCandidate(
		storedHash,
		configHashCandidates,
	)
	if configOverride {
		if !options.forceResume {
			return out.failWith(ConfigurationError, "resumable run configuration does not match the supplied data-plane settings")
		}
		storedCompatibility, compatibilityFound, compatibilityErr :=
			store.ResumeCompatibilityHash(run.ID)
		if compatibilityErr != nil {
			return out.failWith(StateError, "read resume compatibility hash: "+compatibilityErr.Error())
		}
		if !compatibilityFound ||
			!matchesHashCandidate(
				storedCompatibility,
				resumeCompatibilityHashCandidates,
			) {
			return out.failWith(ConfigurationError, "force-resume cannot override a structurally incompatible data-plane change")
		}
	}
	if err := store.BindRunLease(run.ID, lease); err != nil {
		return out.failWith(StateError, "bind resumed run to target lease: "+err.Error())
	}
	if err := store.ReactivateRun(run.ID, "migration resume in progress"); err != nil {
		return out.failWith(StateError, "reactivate migration run: "+err.Error())
	}
	spoolDirectory, err := stage4SpoolDirectory(statePath, run.ID)
	if err != nil {
		if stateErr := persistStage4SpoolPreparationFailure(
			store,
			run.ID,
			err,
		); stateErr != nil {
			return out.failWith(StateError, "record Stage 4 spool preparation failure: "+stateErr.Error())
		}
		return out.failWith(StateError, "Stage 4 spool directory: "+err.Error())
	}
	if err := appLifecycleBoundary("resume_reactivated"); err != nil {
		return out.failWith(StateError, "resume lifecycle: "+err.Error())
	}
	if err := migrationContext.Err(); err != nil {
		disposition := migrationAttemptDisposition(migrate.Result{}, err, cfg.Migration)
		if stateErr := persistAttemptDisposition(
			store, run.ID, disposition, err.Error(), time.Now().UTC(),
		); stateErr != nil {
			return out.failWith(StateError, "record resume outcome: "+stateErr.Error())
		}
		if auditErr := appendAttemptTerminalAudit(
			configPath,
			run.ID,
			"resume",
			migrate.Result{},
			disposition,
			err,
		); auditErr != nil {
			return out.failWith(StateError, auditErr.Error())
		}
		if releaseErr := guard.Release(); releaseErr != nil {
			return out.failWith(StateError, "release target lease: "+releaseErr.Error())
		}
		leaseReleased = true
		out.fail("resume: " + err.Error())
		return out.done(disposition.exitCode)
	}
	if err := appendAudit(configPath, run.ID, "resume_started"); err != nil {
		return out.failWith(StateError, err.Error())
	}
	if configOverride {
		if err := store.AcknowledgeConfigOverride(
			run.ID, configHash, resumeCompatibilityHash,
		); err != nil {
			return out.failWith(StateError, "record forced configuration override: "+err.Error())
		}
		if err := appendAudit(configPath, run.ID, "resume_config_override"); err != nil {
			return out.failWith(StateError, err.Error())
		}
	}

	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		return out.failWith(StateError, "read table checkpoints: "+err.Error())
	}
	completed, existing := make(migrate.CompletedTableCheckpoints), make(map[string]bool)
	progress := make(map[string]migrate.TableProgress)
	for _, task := range tasks {
		existing[task.Table] = true
		if task.Status == "completed" {
			completed[task.Table] = migrate.CompletedTableCheckpoint{Rows: task.RowsDone}
		} else {
			progress[task.Table] = migrate.TableProgress{
				RowsDone:           task.RowsDone,
				IntegerWatermark:   task.IntegerWatermark,
				RowNumberWatermark: task.RowNumberWatermark,
			}
		}
	}
	observer := resumeCheckpointObserver{
		tableCheckpointObserver: tableCheckpointObserver{
			store:          store,
			runID:          run.ID,
			guard:          guard,
			resetTopology:  true,
			resume:         true,
			spoolDirectory: spoolDirectory,
			configPath:     configPath,
		},
		existing: existing,
	}
	migrationContext, heartbeat := startLeaseHeartbeat(migrationContext, guard, 30*time.Second)
	var result migrate.Result
	if cfg.Source.Type == "sqlite" && cfg.Target.Type == "sqlite" {
		result, err = migrate.SQLiteToSQLiteResumeWithProgress(
			migrationContext,
			cfg,
			completed,
			progress,
			observer,
		)
	} else {
		result, err = migrate.ExecuteResume(
			migrationContext,
			cfg,
			completed,
			observer,
		)
	}
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		err = fmt.Errorf("%w: renew target lease: %v", state.ErrState, heartbeatErr)
	}
	if err == nil {
		if ownershipErr := guard.Renew(); ownershipErr != nil {
			err = fmt.Errorf("%w: verify final target lease: %v", state.ErrState, ownershipErr)
		}
	}
	if err != nil {
		disposition := migrationAttemptDisposition(result, err, cfg.Migration)
		endedAt := time.Now().UTC()
		if stateErr := persistAttemptDisposition(
			store, run.ID, disposition, err.Error(), endedAt,
		); stateErr != nil {
			return out.failWith(StateError, "record resume outcome: "+stateErr.Error())
		}
		if auditErr := appendAttemptTerminalAudit(
			configPath,
			run.ID,
			"resume",
			result,
			disposition,
			err,
		); auditErr != nil {
			return out.failWith(StateError, auditErr.Error())
		}
		if releaseErr := guard.Release(); releaseErr != nil {
			return out.failWith(StateError, "release target lease: "+releaseErr.Error())
		}
		leaseReleased = true
		if disposition.acceptedPartial {
			result.Validated = false
			if encodeErr := out.setPayload(PayloadPartialResult, acceptedPartialResult{
				Result: result, Outcome: state.Partial, Resumable: false,
			}); encodeErr != nil {
				return out.failWith(FileError, "write partial result: "+encodeErr.Error())
			}
			return out.done(Success)
		}
		out.fail("resume: " + err.Error())
		return out.done(disposition.exitCode)
	}
	if err := appendAudit(configPath, run.ID, "validation_completed"); err != nil {
		return out.failWith(StateError, err.Error())
	}
	published, err := publishStage4RunSuccess(
		observer.tableCheckpointObserver,
		resumeSuccessReason,
	)
	if err != nil {
		return out.failWith(StateError, "publish resumed migration state: "+err.Error())
	}
	if !published {
		if err := store.Append(state.Run{ID: run.ID, Source: run.Source, Target: run.Target, Outcome: state.Success, Resumable: false, Reason: resumeSuccessReason, StartedAt: run.StartedAt, EndedAt: time.Now().UTC()}); err != nil {
			return out.failWith(StateError, "record resumed migration state: "+err.Error())
		}
	}
	if err := appLifecycleBoundary("resume_success_persisted"); err != nil {
		return out.failWith(StateError, "resume lifecycle: "+err.Error())
	}
	if err := guard.Release(); err != nil {
		return out.failWith(StateError, "release target lease: "+err.Error())
	}
	leaseReleased = true
	if err := appendAudit(configPath, run.ID, "resume_succeeded"); err != nil {
		return out.failWith(StateError, err.Error())
	}
	if err := out.setPayload(PayloadResult, result); err != nil {
		return out.failWith(FileError, "write result: "+err.Error())
	}
	return out.done(Success)
}

// resumeOptionsFrom builds the options from a Request rather than argv, so a
// surface with no command line can resume. The validity rules are shared with
// argv parsing rather than duplicated: a WebUI must not be able to request a
// combination the CLI refuses.
func resumeOptionsFrom(request Request) (resumeOptions, bool) {
	options := resumeOptions{
		configPath:              request.ConfigPath,
		statePath:               request.StatePath,
		destructiveAcknowledged: request.AcknowledgeDestructive,
		forceResume:             request.ForceResume,
		abandon:                 request.Abandon,
		abandonReason:           request.AbandonReason,
	}
	return validResumeOptions(options)
}

// validResumeOptions holds the rules both entry points must obey, so a surface
// with no command line cannot request a combination the CLI refuses.
func validResumeOptions(options resumeOptions) (resumeOptions, bool) {
	if options.configPath == "" || options.abandon != (options.abandonReason != "") ||
		options.abandon && (options.forceResume || options.destructiveAcknowledged) {
		return resumeOptions{}, false
	}
	if options.statePath == "" {
		options.statePath = options.configPath + ".state.db"
	}
	return options, true
}

func resumeArguments(args []string) (resumeOptions, bool) {
	var options resumeOptions
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--config":
			if index+1 >= len(args) || options.configPath != "" {
				return resumeOptions{}, false
			}
			options.configPath = args[index+1]
			index++
		case "--state":
			if index+1 >= len(args) || options.statePath != "" {
				return resumeOptions{}, false
			}
			options.statePath = args[index+1]
			index++
		case "--acknowledge-destructive":
			if options.destructiveAcknowledged {
				return resumeOptions{}, false
			}
			options.destructiveAcknowledged = true
		case "--force-resume":
			if options.forceResume {
				return resumeOptions{}, false
			}
			options.forceResume = true
		case "--abandon":
			if options.abandon {
				return resumeOptions{}, false
			}
			options.abandon = true
		case "--abandon-reason":
			if index+1 >= len(args) || options.abandonReason != "" {
				return resumeOptions{}, false
			}
			options.abandonReason = args[index+1]
			index++
		default:
			return resumeOptions{}, false
		}
	}
	return validResumeOptions(options)
}

func latestRunForTarget(
	store state.Backend,
	target config.Endpoint,
) (state.Run, bool, error) {
	targetIdentity, err := endpointWorkloadIdentity(target)
	if err != nil {
		return state.Run{}, false, err
	}
	runs, err := store.List()
	if err != nil {
		return state.Run{}, false, err
	}
	var selected state.Run
	var found bool
	for _, run := range runs {
		matches, err := runEndpointIdentityMatches(
			run.TargetIdentity,
			config.Endpoint{
				Type:     target.Type,
				Database: run.Target,
			},
			target,
			targetIdentity,
		)
		if err != nil {
			return state.Run{}, false, err
		}
		if !matches {
			continue
		}
		if run.Outcome == state.Success {
			// A later success supersedes every older resumable attempt. Keep
			// it selectable so resume can finish any missing terminal audit
			// or release bookkeeping without touching the data plane.
			selected, found = run, true
			continue
		}
		// A terminal non-resumable result supersedes only an older revision
		// of the same run. In particular, a SQL Server migration-snapshot
		// run that closed gracefully has released its physical snapshot and
		// must not fall back to that run's earlier running row.
		if !run.Resumable {
			if found && selected.ID == run.ID {
				selected, found = state.Run{}, false
			}
			continue
		}
		if run.Resumable && resumeEligibleOutcome(run.Outcome) {
			selected, found = run, true
		}
	}
	return selected, found, nil
}

func resumeEligibleOutcome(outcome state.Outcome) bool {
	switch outcome {
	case state.Running, state.Failed, state.Cancelled, state.Partial:
		return true
	default:
		return false
	}
}

func runSourceMatchesEndpoint(
	run state.Run,
	source config.Endpoint,
) (bool, error) {
	engine, err := config.CanonicalEngine(source.Type)
	if err != nil {
		return false, err
	}
	if run.SourceEngine != "" && run.SourceEngine != engine {
		return false, nil
	}
	identity, err := endpointWorkloadIdentity(source)
	if err != nil {
		return false, err
	}
	return runEndpointIdentityMatches(
		run.SourceIdentity,
		config.Endpoint{Type: engine, Database: run.Source},
		source,
		identity,
	)
}

func runEndpointIdentityMatches(
	storedIdentity string,
	legacy config.Endpoint,
	current config.Endpoint,
	currentIdentity string,
) (bool, error) {
	if storedIdentity != "" {
		if storedIdentity == currentIdentity {
			return true, nil
		}
		engine, err := config.CanonicalEngine(current.Type)
		if err != nil {
			return false, err
		}
		if engine != "sqlite" {
			return false, nil
		}
		return equivalentSQLiteLeaseIdentity(
			storedIdentity,
			currentIdentity,
		)
	}
	engine, err := config.CanonicalEngine(current.Type)
	if err != nil {
		return false, err
	}
	if engine != "sqlite" {
		// A legacy network run has no canonical host/port/schema evidence and
		// cannot safely be attached to the supplied endpoint.
		return false, nil
	}
	legacy.Type = engine
	return config.SameEndpoint(legacy, current), nil
}

func sameResumeCandidate(left, right state.Run) bool {
	return left.ID == right.ID &&
		left.Source == right.Source &&
		left.Target == right.Target &&
		left.SourceEngine == right.SourceEngine &&
		left.SourceIdentity == right.SourceIdentity &&
		left.TargetIdentity == right.TargetIdentity &&
		left.LeaseTarget == right.LeaseTarget &&
		left.LeaseOwnerToken == right.LeaseOwnerToken &&
		left.LeaseGeneration == right.LeaseGeneration &&
		left.Outcome == right.Outcome &&
		left.Resumable == right.Resumable &&
		left.Reason == right.Reason &&
		left.StartedAt.Equal(right.StartedAt) &&
		left.EndedAt.Equal(right.EndedAt)
}

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
