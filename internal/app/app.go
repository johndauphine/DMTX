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

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/contract"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
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
	configPath, dryRun, ok := runArguments(args)
	if !ok {
		fmt.Fprintln(stderr, "usage: dmt run --config migration.yaml [--dry-run]")
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
	migrationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	store := state.SQLiteStore{Path: configPath + ".state.db"}
	started := time.Now().UTC()
	runID := started.Format("20060102T150405.000000000Z")
	leaseStore, lease, err := acquireSQLiteTargetLease(cfg.Target.Database, runID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease: %v\n", err)
		return StateError
	}
	leaseReleased := false
	defer func() {
		if !leaseReleased {
			_ = leaseStore.ReleaseLease(lease)
		}
	}()
	if err := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Running, Resumable: true, Reason: "migration in progress", StartedAt: started}); err != nil {
		fmt.Fprintf(stderr, "record migration state: %v\n", err)
		return StateError
	}
	if err := store.SaveConfigHash(runID, configHash); err != nil {
		fmt.Fprintf(stderr, "record configuration hash: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, runID, "run_started"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	migrationContext, heartbeat := startLeaseHeartbeat(migrationContext, leaseStore, lease, 30*time.Second)
	observer := tableCheckpointObserver{store: store, runID: runID}
	result, err := migrate.Execute(migrationContext, cfg, observer)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		fmt.Fprintf(stderr, "renew target lease: %v\n", heartbeatErr)
		return StateError
	}
	if err != nil {
		if stateErr := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Failed, Resumable: true, Reason: err.Error(), StartedAt: started, EndedAt: time.Now().UTC()}); stateErr != nil {
			fmt.Fprintf(stderr, "record failed migration state: %v\n", stateErr)
			return StateError
		}
		if auditErr := appendAudit(configPath, runID, "run_failed"); auditErr != nil {
			fmt.Fprintf(stderr, "%v\n", auditErr)
			return StateError
		}
		fmt.Fprintf(stderr, "migration: %v\n", err)
		return migrationExitCode(err)
	}
	if err := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Success, Resumable: false, Reason: "migration completed", StartedAt: started, EndedAt: time.Now().UTC()}); err != nil {
		fmt.Fprintf(stderr, "record completed migration state: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, runID, "run_succeeded"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := leaseStore.ReleaseLease(lease); err != nil {
		fmt.Fprintf(stderr, "release target lease: %v\n", err)
		return StateError
	}
	leaseReleased = true
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
}

func migrationExitCode(err error) int {
	if errors.Is(err, context.Canceled) {
		return Cancelled
	}
	return TransferError
}

func runArguments(args []string) (configPath string, dryRun, ok bool) {
	if len(args) < 2 || len(args) > 3 {
		return "", false, false
	}
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--config":
			if index+1 >= len(args) || configPath != "" {
				return "", false, false
			}
			configPath = args[index+1]
			index++
		case "--dry-run":
			if dryRun {
				return "", false, false
			}
			dryRun = true
		default:
			return "", false, false
		}
	}
	return configPath, dryRun, configPath != ""
}

func showState(args []string, stdout io.Writer, latest bool) int {
	if len(args) != 2 || args[0] != "--state" {
		fmt.Fprintln(stdout, "usage: dmt status --state migration.yaml.state.db")
		return ConfigurationError
	}
	store := state.SQLiteStore{Path: args[1]}
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
		if err := json.NewEncoder(stdout).Encode(run); err != nil {
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
	if err := json.NewEncoder(stdout).Encode(runs); err != nil {
		fmt.Fprintln(stdout, err)
		return FileError
	}
	return Success
}

func printHelp(output io.Writer) {
	fmt.Fprintln(output, "dmt - deterministic database migration tool")
	fmt.Fprintln(output, "SQLite first pass: dmt run --config migration.yaml")
	fmt.Fprintln(output, "Commands:")
	for _, command := range contract.Commands {
		fmt.Fprintf(output, "  %s\n", command.Name)
	}
}
