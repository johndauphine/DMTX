package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	_ "modernc.org/sqlite"
)

type preflightFinding struct {
	Class    string `json:"class"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func preflight(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(stderr, "usage: dmtx preflight --config migration.yaml")
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
	sameSQLiteDatabase := cfg.Source.Type == "sqlite" && cfg.Target.Type == "sqlite" && cfg.Source.Database != "" && cfg.Source.Database == cfg.Target.Database
	if err := engine.ValidateMigration(cfg); err != nil && !sameSQLiteDatabase {
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
	if cfg.Source.Type != "sqlite" {
		if err := preflightNetworkEndpoint(cfg.Source); err != nil {
			findings = append(findings, preflightFinding{"source_connect", "error", "source database cannot be reached: " + err.Error()})
		}
	}
	if cfg.Target.Type != "sqlite" {
		if err := preflightNetworkEndpoint(cfg.Target); err != nil {
			findings = append(findings, preflightFinding{"target_connect", "error", "target database cannot be reached: " + err.Error()})
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

func preflightNetworkEndpoint(endpoint config.Endpoint) error {
	password, err := config.ExpandSecret(endpoint.Password)
	if err != nil {
		return fmt.Errorf("resolve source password: %w", err)
	}
	endpoint.Password = password
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var database *sql.DB
	switch endpoint.Type {
	case "postgres":
		database, err = engine.OpenPostgres(ctx, endpoint)
	case "mysql":
		database, err = engine.OpenMySQL(ctx, endpoint)
	case "mssql":
		database, err = engine.OpenSQLServer(ctx, endpoint)
	case "clickhouse":
		database, err = engine.OpenClickHouse(ctx, endpoint)
	default:
		return fmt.Errorf("unsupported source engine %q", endpoint.Type)
	}
	if err != nil {
		return err
	}
	return database.Close()
}
