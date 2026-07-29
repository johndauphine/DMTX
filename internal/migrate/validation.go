package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	_ "modernc.org/sqlite"
)

// ValidationResult contains stable, machine-readable count findings.
type ValidationResult struct {
	Passed bool                `json:"passed"`
	Tables []ValidationFinding `json:"tables"`
}

type ValidationFinding struct {
	Table      string `json:"table"`
	SourceRows int    `json:"source_rows"`
	TargetRows int    `json:"target_rows"`
	Match      bool   `json:"match"`
}

// ValidateSQLite performs deterministic count validation without mutation.
func ValidateSQLite(ctx context.Context, cfg config.Config) (ValidationResult, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		return ValidationResult{}, fmt.Errorf("validation is currently supported only for SQLite-to-SQLite migrations")
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" {
		return ValidationResult{}, fmt.Errorf("SQLite source and target database paths are required")
	}
	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("open SQLite source: %w", err)
	}
	defer source.Close()
	target, err := sql.Open("sqlite", cfg.Target.Database)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("open SQLite target: %w", err)
	}
	defer target.Close()
	names, err := userTables(ctx, source)
	if err != nil {
		return ValidationResult{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return ValidationResult{}, err
	}
	result := ValidationResult{Passed: true, Tables: make([]ValidationFinding, 0, len(names))}
	for _, name := range names {
		sourceRows, err := countRows(ctx, source, name)
		if err != nil {
			return ValidationResult{}, fmt.Errorf("count source table %s: %w", name, err)
		}
		targetRows, err := countRows(ctx, target, name)
		if err != nil {
			return ValidationResult{}, fmt.Errorf("count target table %s: %w", name, err)
		}
		match := countsMatch(cfg.Migration.TargetMode, sourceRows, targetRows)
		result.Tables = append(result.Tables, ValidationFinding{Table: name, SourceRows: sourceRows, TargetRows: targetRows, Match: match})
		result.Passed = result.Passed && match
	}
	return result, nil
}

func countsMatch(mode string, sourceRows, targetRows int) bool {
	if mode == "upsert" {
		return targetRows >= sourceRows
	}
	return targetRows == sourceRows
}
