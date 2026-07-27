// Package app owns the public command-line contract.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	case "health-check":
		fmt.Fprintln(stdout, "preflight is planned")
		return Success
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
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(stderr, "usage: dmt run --config migration.yaml")
		return ConfigurationError
	}
	data, err := os.ReadFile(args[1])
	if err != nil {
		fmt.Fprintf(stderr, "read configuration: %v\n", err)
		return FileError
	}
	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	configHash, err := config.Hash(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "configuration hash: %v\n", err)
		return StateError
	}

	store := state.SQLiteStore{Path: args[1] + ".state.db"}
	started := time.Now().UTC()
	runID := started.Format("20060102T150405.000000000Z")
	leaseStore, lease, err := acquireSQLiteTargetLease(cfg.Target.Database, runID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease: %v\n", err)
		return StateError
	}
	defer leaseStore.ReleaseLease(lease)
	if err := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Running, Resumable: true, Reason: "migration in progress", StartedAt: started}); err != nil {
		fmt.Fprintf(stderr, "record migration state: %v\n", err)
		return StateError
	}
	if err := store.SaveConfigHash(runID, configHash); err != nil {
		fmt.Fprintf(stderr, "record configuration hash: %v\n", err)
		return StateError
	}
	observer := tableCheckpointObserver{store: store, runID: runID}
	result, err := migrate.SQLiteToSQLiteWithObserver(context.Background(), cfg, observer)
	if err != nil {
		if stateErr := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Failed, Resumable: true, Reason: err.Error(), StartedAt: started, EndedAt: time.Now().UTC()}); stateErr != nil {
			fmt.Fprintf(stderr, "record failed migration state: %v\n", stateErr)
			return StateError
		}
		fmt.Fprintf(stderr, "migration: %v\n", err)
		return TransferError
	}
	if err := store.Append(state.Run{ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database, Outcome: state.Success, Resumable: false, Reason: "migration completed", StartedAt: started, EndedAt: time.Now().UTC()}); err != nil {
		fmt.Fprintf(stderr, "record completed migration state: %v\n", err)
		return StateError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
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
