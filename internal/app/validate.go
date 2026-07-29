package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

func validate(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(stderr, "usage: dmtx validate --config migration.yaml")
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
	result, err := migrate.ValidateSQLite(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(stderr, "validation: %v\n", err)
		return ConfigurationError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write validation: %v\n", err)
		return FileError
	}
	if !result.Passed {
		return ValidationError
	}
	return Success
}
