package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
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
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		fmt.Fprintln(stderr, "resume is currently supported only for SQLite-to-SQLite migrations")
		return ConfigurationError
	}
	if err := engine.ValidateMigration(cfg); err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	store, err := state.NewBackend(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "state backend: %v\n", err)
		return StateError
	}

	run, found, err := latestRunForSQLiteTarget(store, cfg.Target.Database)
	if err != nil {
		fmt.Fprintf(stderr, "read migration run: %v\n", err)
		return StateError
	}
	if !found {
		fmt.Fprintln(stderr, "no resumable run exists for this target")
		return StateError
	}
	if !config.SameEndpoint(
		config.Endpoint{Type: "sqlite", Database: run.Source},
		cfg.Source,
	) {
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
	resumeCompatibilityHash, err := config.ResumeCompatibilityHash(hashConfig)
	if err != nil {
		fmt.Fprintf(stderr, "resume compatibility hash: %v\n", err)
		return StateError
	}
	if run.Outcome == state.Success {
		return finalizePersistedSuccess(
			configPath, cfg, configHash, run, store, stdout, stderr,
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
	store = state.FenceBackend(store, guard)
	leaseReleased := false
	defer func() {
		if !leaseReleased {
			_ = guard.Release()
		}
	}()
	authoritative, found, err := latestRunForSQLiteTarget(store, cfg.Target.Database)
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
	configOverride := storedHash != configHash
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
		if !compatibilityFound || storedCompatibility != resumeCompatibilityHash {
			fmt.Fprintln(stderr, "force-resume cannot override a structurally incompatible data-plane change")
			return ConfigurationError
		}
	}
	if err := store.ReactivateRun(run.ID, "migration resume in progress"); err != nil {
		fmt.Fprintf(stderr, "reactivate migration run: %v\n", err)
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
	observer := resumeCheckpointObserver{tableCheckpointObserver: tableCheckpointObserver{store: store, runID: run.ID, guard: guard, resetTopology: true}, existing: existing}
	migrationContext, heartbeat := startLeaseHeartbeat(migrationContext, guard, 30*time.Second)
	result, err := migrate.SQLiteToSQLiteResumeWithProgress(migrationContext, cfg, completed, progress, observer)
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
	if err := store.Append(state.Run{ID: run.ID, Source: run.Source, Target: run.Target, Outcome: state.Success, Resumable: false, Reason: resumeSuccessReason, StartedAt: run.StartedAt, EndedAt: time.Now().UTC()}); err != nil {
		fmt.Fprintf(stderr, "record resumed migration state: %v\n", err)
		return StateError
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

func latestRunForSQLiteTarget(store state.Backend, target string) (state.Run, bool, error) {
	runs, err := store.List()
	if err != nil {
		return state.Run{}, false, err
	}
	targetEndpoint := config.Endpoint{Type: "sqlite", Database: target}
	var selected state.Run
	var found bool
	for _, run := range runs {
		if !config.SameEndpoint(
			config.Endpoint{Type: "sqlite", Database: run.Target},
			targetEndpoint,
		) {
			continue
		}
		selected, found = run, true
	}
	return selected, found, nil
}

func sameResumeCandidate(left, right state.Run) bool {
	return left.ID == right.ID &&
		config.SameEndpoint(
			config.Endpoint{Type: "sqlite", Database: left.Source},
			config.Endpoint{Type: "sqlite", Database: right.Source},
		) &&
		config.SameEndpoint(
			config.Endpoint{Type: "sqlite", Database: left.Target},
			config.Endpoint{Type: "sqlite", Database: right.Target},
		) &&
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
	authoritative, found, err := latestRunForSQLiteTarget(store, cfg.Target.Database)
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
	configHash string,
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
	authoritative, found, err := latestRunForSQLiteTarget(
		store, cfg.Target.Database,
	)
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
	if storedHash != configHash {
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

type resumeCheckpointObserver struct {
	tableCheckpointObserver
	existing map[string]bool
}

func (observer resumeCheckpointObserver) BeforeTables(ctx context.Context, tables []string) error {
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
