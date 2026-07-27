package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/engine"
	_ "modernc.org/sqlite"
)

// Plan is the deterministic, non-mutating result of a dry run.
type Plan struct {
	SourceType string         `json:"source_type"`
	TargetType string         `json:"target_type"`
	TargetMode string         `json:"target_mode"`
	Tables     []PlannedTable `json:"tables"`
}

type PlannedTable struct {
	Name string `json:"name"`
	Rows int    `json:"rows"`
}

// DryRun validates the implemented pair and discovers SQLite source tables.
// It deliberately does not open the target, state store, lease, or audit log.
func DryRun(ctx context.Context, cfg config.Config) (Plan, error) {
	if err := engine.ValidateMigration(cfg); err != nil {
		return Plan{}, err
	}
	if cfg.Source.Type == "postgres" {
		return postgresDryRun(ctx, cfg)
	}
	if cfg.Source.Type != "sqlite" {
		return Plan{}, fmt.Errorf("dry run discovery is currently implemented for SQLite sources")
	}
	if cfg.Source.Database == "" {
		return Plan{}, fmt.Errorf("SQLite source database path is required")
	}
	info, err := os.Stat(cfg.Source.Database)
	if err != nil {
		return Plan{}, fmt.Errorf("inspect SQLite source: %w", err)
	}
	if info.IsDir() {
		return Plan{}, fmt.Errorf("SQLite source database path is a directory")
	}
	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return Plan{}, fmt.Errorf("open source: %w", err)
	}
	defer source.Close()
	names, err := userTables(ctx, source)
	if err != nil {
		return Plan{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{SourceType: cfg.Source.Type, TargetType: cfg.Target.Type, TargetMode: cfg.Migration.TargetMode, Tables: make([]PlannedTable, 0, len(names))}
	for _, name := range names {
		rows, err := countRows(ctx, source, name)
		if err != nil {
			return Plan{}, fmt.Errorf("count source table %s: %w", name, err)
		}
		plan.Tables = append(plan.Tables, PlannedTable{Name: name, Rows: rows})
	}
	return plan, nil
}

func postgresDryRun(ctx context.Context, cfg config.Config) (Plan, error) {
	endpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Plan{}, err
	}
	source, err := engine.OpenPostgres(ctx, endpoint)
	if err != nil {
		return Plan{}, err
	}
	defer source.Close()
	namespace := cfg.Source.Schema
	if namespace == "" {
		namespace = "public"
	}
	names, err := engine.ListPostgresTables(ctx, source, namespace)
	if err != nil {
		return Plan{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{SourceType: cfg.Source.Type, TargetType: cfg.Target.Type, TargetMode: cfg.Migration.TargetMode, Tables: make([]PlannedTable, 0, len(names))}
	for _, name := range names {
		var rows int
		if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(namespace, name)).Scan(&rows); err != nil {
			return Plan{}, fmt.Errorf("count PostgreSQL source table %s: %w", name, err)
		}
		plan.Tables = append(plan.Tables, PlannedTable{Name: name, Rows: rows})
	}
	return plan, nil
}
