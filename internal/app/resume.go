package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func resume(args []string, stdout, stderr io.Writer) int {
	migrationContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	options, ok := resumeArguments(args)
	if !ok {
		fmt.Fprintln(stderr, "usage: dmtx resume --config migration.yaml [--state migration.state.yaml] [--acknowledge-destructive] [--force-resume] [--abandon --abandon-reason TEXT]")
		return ConfigurationError
	}
	configPath, statePath := options.configPath, options.statePath
	data, err := os.ReadFile(options.configPath)
	if err != nil {
		fmt.Fprintf(stderr, "read configuration: %v\n", err)
		return FileError
	}
	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	if err := migrate.ValidateMigration(cfg); err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	store, err := state.NewBackend(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "state backend: %v\n", err)
		return StateError
	}

	run, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		fmt.Fprintf(stderr, "read migration run: %v\n", err)
		return StateError
	}
	if !found {
		fmt.Fprintln(stderr, "no resumable run exists for this target")
		return StateError
	}
	sourceMatches, err := runSourceMatchesEndpoint(run, cfg.Source)
	if err != nil {
		fmt.Fprintf(stderr, "compare resumable run source identity: %v\n", err)
		return StateError
	}
	if !sourceMatches {
		fmt.Fprintln(stderr, "resumable run source does not match the supplied configuration")
		return ConfigurationError
	}
	if options.abandon {
		if err := appLifecycleBoundary("resume_candidate_selected"); err != nil {
			fmt.Fprintf(stderr, "resume lifecycle: %v\n", err)
			return StateError
		}
		return abandonResumeRun(configPath, cfg, run, store, options.abandonReason, stdout, stderr)
	}
	hashConfig := cfg
	hashConfig.Source.Database = run.Source
	hashConfig.Target.Database = run.Target
	configHash, err := config.Hash(hashConfig)
	if err != nil {
		fmt.Fprintf(stderr, "configuration hash: %v\n", err)
		return StateError
	}
	configHashCandidates, err :=
		config.SQLiteIdentityHashCandidates(hashConfig)
	if err != nil {
		fmt.Fprintf(stderr, "SQLite identity configuration hashes: %v\n", err)
		return StateError
	}
	resumeCompatibilityHash, err := config.ResumeCompatibilityHash(hashConfig)
	if err != nil {
		fmt.Fprintf(stderr, "resume compatibility hash: %v\n", err)
		return StateError
	}
	resumeCompatibilityHashCandidates, err :=
		config.SQLiteIdentityResumeCompatibilityHashCandidates(hashConfig)
	if err != nil {
		fmt.Fprintf(
			stderr,
			"SQLite identity resume compatibility hashes: %v\n",
			err,
		)
		return StateError
	}
	if run.Outcome == state.Success {
		return finalizePersistedSuccess(
			configPath,
			cfg,
			configHashCandidates,
			run,
			store,
			stdout,
			stderr,
		)
	}
	if !run.Resumable || run.Outcome != state.Running && run.Outcome != state.Failed &&
		run.Outcome != state.Cancelled && run.Outcome != state.Partial {
		fmt.Fprintln(stderr, "no resumable run exists for this target")
		return StateError
	}
	if err := appLifecycleBoundary("resume_candidate_selected"); err != nil {
		fmt.Fprintf(stderr, "resume lifecycle: %v\n", err)
		return StateError
	}

	cfg.Migration.DestructiveAcknowledged = options.destructiveAcknowledged
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease: %v\n", err)
		return StateError
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
		fmt.Fprintf(stderr, "fence Stage 4 state backend: %v\n", err)
		return StateError
	}
	authoritative, found, err := latestRunForTarget(store, cfg.Target)
	if err != nil {
		fmt.Fprintf(stderr, "reselect migration run: %v\n", err)
		return StateError
	}
	if !found {
		fmt.Fprintln(stderr, "resume candidate disappeared after target lease acquisition")
		return StateError
	}
	if authoritative.Outcome == state.Success {
		fmt.Fprintln(stderr, "resume candidate was superseded by a successful run after target lease acquisition")
		return StateError
	}
	if !sameResumeCandidate(run, authoritative) {
		fmt.Fprintln(stderr, "resume candidate changed after target lease acquisition")
		return StateError
	}
	run = authoritative
	storedHash, hashFound, err := store.ConfigHash(run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read configuration hash: %v\n", err)
		return StateError
	}
	if !hashFound {
		fmt.Fprintln(stderr, "resumable run is missing its data-plane configuration hash")
		return ConfigurationError
	}
	configOverride := !matchesHashCandidate(
		storedHash,
		configHashCandidates,
	)
	if configOverride {
		if !options.forceResume {
			fmt.Fprintln(stderr, "resumable run configuration does not match the supplied data-plane settings")
			return ConfigurationError
		}
		storedCompatibility, compatibilityFound, compatibilityErr :=
			store.ResumeCompatibilityHash(run.ID)
		if compatibilityErr != nil {
			fmt.Fprintf(stderr, "read resume compatibility hash: %v\n", compatibilityErr)
			return StateError
		}
		if !compatibilityFound ||
			!matchesHashCandidate(
				storedCompatibility,
				resumeCompatibilityHashCandidates,
			) {
			fmt.Fprintln(stderr, "force-resume cannot override a structurally incompatible data-plane change")
			return ConfigurationError
		}
	}
	if err := store.BindRunLease(run.ID, lease); err != nil {
		fmt.Fprintf(stderr, "bind resumed run to target lease: %v\n", err)
		return StateError
	}
	if err := store.ReactivateRun(run.ID, "migration resume in progress"); err != nil {
		fmt.Fprintf(stderr, "reactivate migration run: %v\n", err)
		return StateError
	}
	spoolDirectory, err := stage4SpoolDirectory(statePath, run.ID)
	if err != nil {
		if stateErr := persistStage4SpoolPreparationFailure(
			store,
			run.ID,
			err,
		); stateErr != nil {
			fmt.Fprintf(stderr, "record Stage 4 spool preparation failure: %v\n", stateErr)
			return StateError
		}
		fmt.Fprintf(stderr, "Stage 4 spool directory: %v\n", err)
		return StateError
	}
	if err := appLifecycleBoundary("resume_reactivated"); err != nil {
		fmt.Fprintf(stderr, "resume lifecycle: %v\n", err)
		return StateError
	}
	if err := migrationContext.Err(); err != nil {
		disposition := migrationAttemptDisposition(migrate.Result{}, err, cfg.Migration)
		if stateErr := persistAttemptDisposition(
			store, run.ID, disposition, err.Error(), time.Now().UTC(),
		); stateErr != nil {
			fmt.Fprintf(stderr, "record resume outcome: %v\n", stateErr)
			return StateError
		}
		if auditErr := appendAudit(
			configPath, run.ID, "resume_"+disposition.auditSuffix,
		); auditErr != nil {
			fmt.Fprintf(stderr, "%v\n", auditErr)
			return StateError
		}
		if releaseErr := guard.Release(); releaseErr != nil {
			fmt.Fprintf(stderr, "release target lease: %v\n", releaseErr)
			return StateError
		}
		leaseReleased = true
		fmt.Fprintf(stderr, "resume: %v\n", err)
		return disposition.exitCode
	}
	if err := appendAudit(configPath, run.ID, "resume_started"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if configOverride {
		if err := store.AcknowledgeConfigOverride(
			run.ID, configHash, resumeCompatibilityHash,
		); err != nil {
			fmt.Fprintf(stderr, "record forced configuration override: %v\n", err)
			return StateError
		}
		if err := appendAudit(configPath, run.ID, "resume_config_override"); err != nil {
			fmt.Fprintf(stderr, "%v\n", err)
			return StateError
		}
	}

	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read table checkpoints: %v\n", err)
		return StateError
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
			fmt.Fprintf(stderr, "record resume outcome: %v\n", stateErr)
			return StateError
		}
		if auditErr := appendAudit(configPath, run.ID, "resume_"+disposition.auditSuffix); auditErr != nil {
			fmt.Fprintf(stderr, "%v\n", auditErr)
			return StateError
		}
		if releaseErr := guard.Release(); releaseErr != nil {
			fmt.Fprintf(stderr, "release target lease: %v\n", releaseErr)
			return StateError
		}
		leaseReleased = true
		if disposition.acceptedPartial {
			result.Validated = false
			if encodeErr := json.NewEncoder(stdout).Encode(acceptedPartialResult{
				Result: result, Outcome: state.Partial, Resumable: false,
			}); encodeErr != nil {
				fmt.Fprintf(stderr, "write partial result: %v\n", encodeErr)
				return FileError
			}
			return Success
		}
		fmt.Fprintf(stderr, "resume: %v\n", err)
		return disposition.exitCode
	}
	if err := appendAudit(configPath, run.ID, "validation_completed"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	published, err := publishStage4RunSuccess(
		observer.tableCheckpointObserver,
		resumeSuccessReason,
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish resumed migration state: %v\n", err)
		return StateError
	}
	if !published {
		if err := store.Append(state.Run{ID: run.ID, Source: run.Source, Target: run.Target, Outcome: state.Success, Resumable: false, Reason: resumeSuccessReason, StartedAt: run.StartedAt, EndedAt: time.Now().UTC()}); err != nil {
			fmt.Fprintf(stderr, "record resumed migration state: %v\n", err)
			return StateError
		}
	}
	if err := appLifecycleBoundary("resume_success_persisted"); err != nil {
		fmt.Fprintf(stderr, "resume lifecycle: %v\n", err)
		return StateError
	}
	if err := guard.Release(); err != nil {
		fmt.Fprintf(stderr, "release target lease: %v\n", err)
		return StateError
	}
	leaseReleased = true
	if err := appendAudit(configPath, run.ID, "resume_succeeded"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
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
	if options.configPath == "" || options.abandon != (options.abandonReason != "") ||
		options.abandon && (options.forceResume || options.destructiveAcknowledged) {
		return resumeOptions{}, false
	}
	if options.statePath == "" {
		options.statePath = options.configPath + ".state.db"
	}
	return options, true
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

func abandonResumeRun(configPath string, cfg config.Config, run state.Run, store state.Backend, reason string, stdout, stderr io.Writer) int {
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease for abandonment: %v\n", err)
		return StateError
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
		fmt.Fprintf(stderr, "reselect migration run for abandonment: %v\n", err)
		return StateError
	}
	if !found {
		fmt.Fprintln(stderr, "abandon candidate disappeared after target lease acquisition")
		return StateError
	}
	if authoritative.Outcome == state.Success {
		fmt.Fprintln(stderr, "abandon candidate was superseded by a successful run after target lease acquisition")
		return StateError
	}
	if !sameResumeCandidate(run, authoritative) {
		fmt.Fprintln(stderr, "abandon candidate changed after target lease acquisition")
		return StateError
	}
	run = authoritative
	if err := store.BindRunLease(run.ID, lease); err != nil {
		fmt.Fprintf(stderr, "bind abandoned run to target lease: %v\n", err)
		return StateError
	}
	if err := store.AbandonRun(run.ID, reason, time.Now().UTC()); err != nil {
		fmt.Fprintf(stderr, "abandon run: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, run.ID, "run_abandoned"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := guard.Release(); err != nil {
		fmt.Fprintf(stderr, "release abandonment lease: %v\n", err)
		return StateError
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
	if err := json.NewEncoder(stdout).Encode(response); err != nil {
		fmt.Fprintf(stderr, "write abandonment result: %v\n", err)
		return FileError
	}
	return Success
}
func finalizePersistedSuccess(
	configPath string,
	cfg config.Config,
	configHashCandidates []string,
	run state.Run,
	store state.Backend,
	stdout, stderr io.Writer,
) int {
	if err := appLifecycleBoundary("resume_terminal_candidate_selected"); err != nil {
		fmt.Fprintf(stderr, "resume lifecycle: %v\n", err)
		return StateError
	}
	leaseStore, lease, err := acquireTargetLease(cfg.Target, run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease for terminal repair: %v\n", err)
		return StateError
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
		fmt.Fprintf(stderr, "reselect successful migration run: %v\n", err)
		return StateError
	}
	if !found || authoritative.Outcome != state.Success ||
		!sameResumeCandidate(run, authoritative) {
		fmt.Fprintln(
			stderr,
			"terminal repair candidate changed after target lease acquisition",
		)
		return StateError
	}
	run = authoritative
	storedHash, hashFound, err := store.ConfigHash(run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read configuration hash: %v\n", err)
		return StateError
	}
	if !hashFound {
		fmt.Fprintln(stderr, "successful run is missing its data-plane configuration hash")
		return ConfigurationError
	}
	if !matchesHashCandidate(storedHash, configHashCandidates) {
		fmt.Fprintln(
			stderr,
			"force-resume cannot rewrite configuration evidence for terminal-state repair",
		)
		return ConfigurationError
	}
	auditPath := configPath + ".audit.ndjson"
	for _, terminalType := range []string{"run_succeeded", "resume_succeeded"} {
		found, err := audit.HasEvent(auditPath, run.ID, terminalType)
		if err != nil {
			fmt.Fprintf(stderr, "inspect terminal audit: %v\n", err)
			return StateError
		}
		if found {
			fmt.Fprintln(stderr, "no resumable run exists for this target")

			return StateError
		}
	}
	var terminalType string
	switch run.Reason {
	case runSuccessReason:
		terminalType = "run_succeeded"
	case resumeSuccessReason:
		terminalType = "resume_succeeded"
	default:
		fmt.Fprintf(stderr, "successful run has unknown completion provenance %q; refusing terminal repair\n", run.Reason)
		return StateError
	}
	validated, err := audit.HasEvent(auditPath, run.ID, "validation_completed")
	if err != nil {
		fmt.Fprintf(stderr, "inspect validation audit: %v\n", err)
		return StateError
	}
	if !validated {
		fmt.Fprintln(stderr, "successful run is missing its validation audit; refusing terminal repair")
		return StateError
	}
	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read completed table checkpoints: %v\n", err)
		return StateError
	}
	if len(tasks) == 0 {
		fmt.Fprintln(stderr, "successful run has no completed table checkpoints; refusing terminal repair")
		return StateError
	}
	result := migrate.Result{Validated: true}
	for _, task := range tasks {
		if task.Status != "completed" {
			fmt.Fprintf(stderr, "successful run has incomplete table checkpoint %q; refusing terminal repair\n", task.Table)
			return StateError
		}
		result.Tables++
		result.Rows += task.RowsDone
	}

	if err := store.BindRunLease(run.ID, lease); err != nil {
		fmt.Fprintf(stderr, "bind terminal repair to target lease: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, run.ID, "resume_finalization_started"); err != nil {
		_ = heartbeat.Stop()
		heartbeatStopped = true
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	heartbeatErr := heartbeat.Stop()
	heartbeatStopped = true
	if heartbeatErr != nil {
		fmt.Fprintf(stderr, "terminal repair lease heartbeat: %v\n", heartbeatErr)
		return StateError
	}
	if err := guard.Renew(); err != nil {
		fmt.Fprintf(stderr, "verify terminal repair lease: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, run.ID, terminalType); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := guard.Release(); err != nil {
		fmt.Fprintf(stderr, "release terminal repair lease: %v\n", err)
		return StateError
	}
	leaseReleased = true
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
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
