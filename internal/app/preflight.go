package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
	_ "modernc.org/sqlite"
)

type preflightFinding struct {
	Class    string `json:"class"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func preflight(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(stderr, "usage: dmt preflight --config migration.yaml")
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
	findings := []preflightFinding{}
	if err := engine.ValidateMigration(cfg); err != nil {
		findings = append(findings, preflightFinding{"unsupported_capability", "error", err.Error()})
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" {
		findings = append(findings, preflightFinding{"missing_database_path", "error", "source and target database paths are required"})
	}
	if cfg.Source.Database != "" && cfg.Source.Database == cfg.Target.Database {
		findings = append(findings, preflightFinding{"same_database", "error", "source and target SQLite databases must differ"})
	}
	if cfg.Source.Type == "sqlite" && cfg.Source.Database != "" && cfg.Source.Database != cfg.Target.Database {
		if info, statErr := os.Stat(cfg.Source.Database); statErr != nil {
			if os.IsNotExist(statErr) {
				findings = append(findings, preflightFinding{"source_missing", "error", "source database does not exist"})
			} else {
				findings = append(findings, preflightFinding{"source_open", "error", "source database cannot be read"})
			}
		} else if info.IsDir() {
			findings = append(findings, preflightFinding{"source_open", "error", "source database path is a directory"})
		} else if db, openErr := sql.Open("sqlite", cfg.Source.Database); openErr != nil {
			findings = append(findings, preflightFinding{"source_open", "error", "source database cannot be opened"})
		} else {
			pingErr := db.Ping()
			db.Close()
			if pingErr != nil {
				findings = append(findings, preflightFinding{"source_open", "error", "source database cannot be read"})
			}
		}
	}
	if err := json.NewEncoder(stdout).Encode(findings); err != nil {
		fmt.Fprintf(stderr, "write preflight: %v\n", err)
		return FileError
	}
	for _, finding := range findings {
		if finding.Severity == "error" {
			return ConfigurationError
		}
	}
	return Success
}
