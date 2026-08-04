package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

func executeResume(ctx context.Context, request Request, reporter *progressReporter) Outcome {
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
			progress:       reporter,
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
