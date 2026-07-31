// Package app owns the public command-line contract.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/contract"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

const Version = "0.3.0-dev"

const (
	Success = iota
	ConfigurationError
	ConnectionError
	TransferError
	ValidationError
	Cancelled
	StateError
	FileError
)

func Run(args []string, stdout, stderr io.Writer) int {
	if !contract.Valid() {
		fmt.Fprintln(stderr, "internal command registry is invalid")
		return StateError
	}
	if len(args) == 0 {
		fmt.Fprintln(stdout, "DMTX terminal UI is planned; use --help for automation commands.")
		return Success
	}
	switch args[0] {
	case "--version", "version":
		fmt.Fprintln(stdout, Version)
		return Success
	case "--help", "help":
		printHelp(stdout)
		return Success
	case "run":
		return run(args[1:], stdout, stderr)
	case "resume":
		return resume(args[1:], stdout, stderr)
	case "status":
		return showState(args[1:], stdout, true)
	case "history":
		return showState(args[1:], stdout, false)
	case "validate":
		return validate(args[1:], stdout, stderr)
	case "preflight", "health-check":
		return preflight(args[1:], stdout, stderr)
	default:
		for _, command := range contract.Commands {
			if command.Name == args[0] {
				fmt.Fprintf(stdout, "%s is planned in this stage.\n", command.Name)
				return Success
			}
		}
		fmt.Fprintf(stderr, "unknown command %q; use --help\n", args[0])
		return ConfigurationError
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	configPath, statePath, dryRun, destructiveAcknowledged, ok := runArguments(args)
	if !ok {
		fmt.Fprintln(stderr, "usage: dmtx run --config migration.yaml [--state migration.state.yaml] [--dry-run] [--acknowledge-destructive]")
		return ConfigurationError
	}
	data, err := os.ReadFile(configPath)
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
	cfg.Migration.DestructiveAcknowledged = destructiveAcknowledged
	if dryRun {
		plan, err := migrate.DryRun(context.Background(), cfg)
		if err != nil {
			fmt.Fprintf(stderr, "dry run: %v\n", err)
			return ConfigurationError
		}
		if err := json.NewEncoder(stdout).Encode(plan); err != nil {
			fmt.Fprintf(stderr, "write dry run: %v\n", err)
			return FileError
		}
		return Success
	}
	configHash, err := config.Hash(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "configuration hash: %v\n", err)
		return StateError
	}
	resumeCompatibilityHash, err := config.ResumeCompatibilityHash(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "resume compatibility hash: %v\n", err)
		return StateError
	}
	migrationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	store, err := state.NewBackend(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "state backend: %v\n", err)
		return StateError
	}
	started := time.Now().UTC()
	runID := started.Format("20060102T150405.000000000Z")
	leaseStore, lease, err := acquireTargetLease(cfg.Target, runID)
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
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		fmt.Fprintf(stderr, "source workload identity: %v\n", err)
		return StateError
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		fmt.Fprintf(stderr, "target workload identity: %v\n", err)
		return StateError
	}
	if err := store.InitializeRun(state.Run{
		ID:             runID,
		Source:         cfg.Source.Database,
		Target:         cfg.Target.Database,
		SourceEngine:   cfg.Source.Type,
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "migration in progress",
		StartedAt:      started,
	}, configHash); err != nil {
		fmt.Fprintf(stderr, "record migration state: %v\n", err)
		return StateError
	}
	if err := store.SaveResumeCompatibilityHash(runID, resumeCompatibilityHash); err != nil {
		fmt.Fprintf(stderr, "record resume compatibility: %v\n", err)
		return StateError
	}
	spoolDirectory, err := stage4SpoolDirectory(statePath, runID)
	if err != nil {
		if stateErr := persistStage4SpoolPreparationFailure(
			store,
			runID,
			err,
		); stateErr != nil {
			fmt.Fprintf(stderr, "record Stage 4 spool preparation failure: %v\n", stateErr)
			return StateError
		}
		fmt.Fprintf(stderr, "Stage 4 spool directory: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, runID, "run_started"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := appLifecycleBoundary("run_initialized"); err != nil {
		fmt.Fprintf(stderr, "run lifecycle: %v\n", err)
		return StateError
	}
	migrationContext, heartbeat := startLeaseHeartbeat(migrationContext, guard, 30*time.Second)
	observer := tableCheckpointObserver{
		store:          store,
		runID:          runID,
		guard:          guard,
		resume:         false,
		spoolDirectory: spoolDirectory,
		configPath:     configPath,
	}
	result, err := migrate.Execute(migrationContext, cfg, observer)
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
			store, runID, disposition, err.Error(), endedAt,
		); stateErr != nil {
			fmt.Fprintf(stderr, "record migration outcome: %v\n", stateErr)
			return StateError
		}
		if auditErr := appendAudit(configPath, runID, "run_"+disposition.auditSuffix); auditErr != nil {
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
		fmt.Fprintf(stderr, "migration: %v\n", err)
		return disposition.exitCode
	}
	if err := appendAudit(configPath, runID, "validation_completed"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	published, err := publishStage4RunSuccess(observer, runSuccessReason)
	if err != nil {
		fmt.Fprintf(stderr, "publish completed migration state: %v\n", err)
		return StateError
	}
	if !published {
		if err := store.Append(state.Run{
			ID:             runID,
			Source:         cfg.Source.Database,
			Target:         cfg.Target.Database,
			SourceEngine:   cfg.Source.Type,
			SourceIdentity: sourceIdentity,
			TargetIdentity: targetIdentity,
			Outcome:        state.Success,
			Resumable:      false,
			Reason:         runSuccessReason,
			StartedAt:      started,
			EndedAt:        time.Now().UTC(),
		}); err != nil {
			fmt.Fprintf(stderr, "record completed migration state: %v\n", err)
			return StateError
		}
	}
	if err := appLifecycleBoundary("run_success_persisted"); err != nil {
		fmt.Fprintf(stderr, "run lifecycle: %v\n", err)
		return StateError
	}
	if err := guard.Release(); err != nil {
		fmt.Fprintf(stderr, "release target lease: %v\n", err)
		return StateError
	}
	leaseReleased = true
	if err := appendAudit(configPath, runID, "run_succeeded"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
}

func migrationExitCode(err error) int {
	if isStateOrLeaseFailure(err) {
		return StateError
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Cancelled
	}
	if errors.Is(err, migrate.ErrDestructiveAcknowledgement) {
		return ConfigurationError
	}
	return TransferError
}

func runArguments(args []string) (configPath, statePath string, dryRun, destructiveAcknowledged, ok bool) {
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--config":
			if index+1 >= len(args) || configPath != "" {
				return "", "", false, false, false
			}
			configPath = args[index+1]
			index++
		case "--state":
			if index+1 >= len(args) || statePath != "" {
				return "", "", false, false, false
			}
			statePath = args[index+1]
			index++
		case "--dry-run":
			if dryRun {
				return "", "", false, false, false
			}
			dryRun = true
		case "--acknowledge-destructive":
			if destructiveAcknowledged {
				return "", "", false, false, false
			}
			destructiveAcknowledged = true
		default:
			return "", "", false, false, false
		}
	}
	if configPath == "" {
		return "", "", false, false, false
	}
	if statePath == "" {
		statePath = configPath + ".state.db"
	}
	return configPath, statePath, dryRun, destructiveAcknowledged, true
}

func showState(args []string, stdout io.Writer, latest bool) int {
	if len(args) != 2 || args[0] != "--state" {
		fmt.Fprintln(stdout, "usage: dmtx status --state migration.yaml.state.db")
		return ConfigurationError
	}
	store, err := state.NewBackend(args[1])
	if err != nil {
		fmt.Fprintln(stdout, err)
		return StateError
	}

	if latest {
		run, found, err := store.Latest()
		if err != nil {
			fmt.Fprintln(stdout, err)
			return StateError
		}
		if !found {
			fmt.Fprintln(stdout, "no runs recorded")
			return Success
		}
		if err := json.NewEncoder(stdout).Encode(publicRun(run)); err != nil {
			fmt.Fprintln(stdout, err)
			return FileError
		}
		return Success
	}
	runs, err := store.List()
	if err != nil {
		fmt.Fprintln(stdout, err)
		return StateError
	}
	publicRuns := make([]state.Run, len(runs))
	for index, run := range runs {
		publicRuns[index] = publicRun(run)
	}
	if err := json.NewEncoder(stdout).Encode(publicRuns); err != nil {
		fmt.Fprintln(stdout, err)
		return FileError
	}
	return Success
}

func publicRun(run state.Run) state.Run {
	run.LeaseOwnerToken = ""
	return run
}

func printHelp(output io.Writer) {
	fmt.Fprintln(output, "dmtx - deterministic database migration tool")
	fmt.Fprintln(output, "SQLite first pass: dmtx run --config migration.yaml")
	fmt.Fprintln(output, "Commands:")
	for _, command := range contract.Commands {
		fmt.Fprintf(output, "  %s\n", command.Name)
	}
}
