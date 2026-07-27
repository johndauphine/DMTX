// Package app owns the public command-line contract.
package app

import (
	"fmt"
	"io"

	"github.com/johndauphine/DMTX/internal/contract"
)

const Version = "0.1.0-dev"

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
	case "health-check":
		fmt.Fprintln(stdout, "preflight is planned")
		return Success
	default:
		for _, command := range contract.Commands {
			if command.Name == args[0] {
				fmt.Fprintf(stdout, "%s is planned in Stage 0.\n", command.Name)
				return Success
			}
		}
		fmt.Fprintf(stderr, "unknown command %q; use --help\n", args[0])
		return ConfigurationError
	}
}

func printHelp(output io.Writer) {
	fmt.Fprintln(output, "dmt - deterministic database migration tool")
	fmt.Fprintln(output, "Commands:")
	for _, command := range contract.Commands {
		fmt.Fprintf(output, "  %s\n", command.Name)
	}
}
