// Package app owns the public command-line contract.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/contract"
	"github.com/johndauphine/DMTX/internal/migrate"
)

const Version = "0.2.0-dev"

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
	result, err := migrate.SQLiteToSQLite(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(stderr, "migration: %v\n", err)
		return TransferError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
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
