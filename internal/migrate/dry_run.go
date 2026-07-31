package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	_ "modernc.org/sqlite"
)

// Plan is the deterministic, non-mutating result of a dry run.
type Plan struct {
	SourceType string         `json:"source_type"`
	TargetType string         `json:"target_type"`
	TargetMode string         `json:"target_mode"`
	Tables     []PlannedTable `json:"tables"`
	Tuning     *PlannedTuning `json:"tuning,omitempty"`
	Deletes    *PlannedDelete `json:"deletes,omitempty"`
}

type PlannedTable struct {
	Name string `json:"name"`
	Rows int    `json:"rows"`
}

// PlannedSetting is one effective tuning value with the provenance that
// selected it, so an operator can tell a pinned value from a derived or
// safety-clamped one without reading the resolver.
type PlannedSetting struct {
	Value      int64  `json:"value"`
	Provenance string `json:"provenance"`
}

// PlannedTuning discloses the resource plan the migration would run under.
// Resolving it reads host memory evidence only; it opens nothing and writes
// nothing.
type PlannedTuning struct {
	ConnectionLimit PlannedSetting `json:"connection_limit"`
	Workers         PlannedSetting `json:"workers"`
	Readers         PlannedSetting `json:"readers"`
	Writers         PlannedSetting `json:"writers"`
	QueueDepth      PlannedSetting `json:"queue_depth"`
	ChunkRows       PlannedSetting `json:"chunk_rows"`
	MemoryBudget    PlannedSetting `json:"memory_budget_bytes"`
}

// PlannedDelete discloses the configured delete policy only. Whether a
// reconciliation is actually due depends on the durable last-success time, and
// a dry run deliberately does not open state, so DueStateKnown is always false
// and callers must not present this as due/not-due.
type PlannedDelete struct {
	Mode              string `json:"mode"`
	Schedule          string `json:"schedule,omitempty"`
	IntervalSeconds   int64  `json:"interval_seconds,omitempty"`
	RequirePrimaryKey bool   `json:"require_primary_key,omitempty"`
	DueStateKnown     bool   `json:"due_state_known"`
}

// planDryRunDisclosure derives the tuning and delete-policy facts a dry run can
// report without opening the target, state, lease, or audit log. Tuning comes
// from the same resolver the migration itself uses, so the disclosed numbers are
// the numbers that would apply rather than a parallel estimate.
func planDryRunDisclosure(
	ctx context.Context,
	cfg config.Config,
) (*PlannedTuning, *PlannedDelete, error) {
	resources, err := config.ResolveSystemEffectiveTransferPlan(
		ctx,
		cfg.Migration,
		config.TransferPlanOptions{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("disclose dry run tuning: %w", err)
	}
	fromInt := func(value config.EffectiveInt) PlannedSetting {
		return PlannedSetting{
			Value:      int64(value.Value),
			Provenance: string(value.Provenance),
		}
	}
	tuning := &PlannedTuning{
		ConnectionLimit: fromInt(resources.ConnectionLimit),
		Workers:         fromInt(resources.Workers),
		Readers:         fromInt(resources.Readers),
		Writers:         fromInt(resources.Writers),
		QueueDepth:      fromInt(resources.QueueDepth),
		ChunkRows:       fromInt(resources.ChunkRows),
		MemoryBudget: PlannedSetting{
			Value:      resources.MemoryBudget.Value,
			Provenance: string(resources.MemoryBudget.Provenance),
		},
	}
	deletes := &PlannedDelete{
		Mode:              string(cfg.Migration.Deletes.Mode),
		Schedule:          string(cfg.Migration.Deletes.Reconcile.Schedule),
		RequirePrimaryKey: cfg.Migration.Deletes.Reconcile.RequirePrimaryKey,
		DueStateKnown:     false,
	}
	if interval := cfg.Migration.Deletes.Reconcile.Interval; interval > 0 {
		deletes.IntervalSeconds = int64(interval / time.Second)
	}
	return tuning, deletes, nil
}

// DryRun discovers the plan and attaches the disclosures every source engine
// shares, so a new engine path cannot silently omit them.
func DryRun(ctx context.Context, cfg config.Config) (Plan, error) {
	plan, err := discoverDryRunPlan(ctx, cfg)
	if err != nil {
		return Plan{}, err
	}
	tuning, deletes, err := planDryRunDisclosure(ctx, cfg)
	if err != nil {
		return Plan{}, err
	}
	plan.Tuning = tuning
	plan.Deletes = deletes
	return plan, nil
}

// discoverDryRunPlan validates the implemented pair and discovers source
// tables. It deliberately does not open the target, state store, lease, or
// audit log.
func discoverDryRunPlan(ctx context.Context, cfg config.Config) (Plan, error) {
	if err := ValidateMigration(cfg); err != nil {
		return Plan{}, err
	}
	if cfg.Source.Type == "postgres" {
		return postgresDryRun(ctx, cfg)
	}
	if cfg.Source.Type == "mysql" {
		return mySQLDryRun(ctx, cfg)
	}
	if cfg.Source.Type == "mssql" {
		return sqlServerDryRun(ctx, cfg)
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

func sqlServerDryRun(ctx context.Context, cfg config.Config) (Plan, error) {
	endpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Plan{}, err
	}
	source, err := engine.OpenSQLServer(ctx, endpoint)
	if err != nil {
		return Plan{}, err
	}
	defer source.Close()
	namespace := cfg.Source.Schema
	if namespace == "" {
		namespace = "dbo"
	}
	names, err := engine.ListSQLServerTables(ctx, source, namespace)
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
		if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(namespace, name)).Scan(&rows); err != nil {
			return Plan{}, fmt.Errorf("count SQL Server source table %s: %w", name, err)
		}
		plan.Tables = append(plan.Tables, PlannedTable{Name: name, Rows: rows})
	}
	return plan, nil
}

func mySQLDryRun(ctx context.Context, cfg config.Config) (Plan, error) {
	endpoint, err := resolvedEndpoint(cfg.Source)
	if err != nil {
		return Plan{}, err
	}
	source, err := engine.OpenMySQL(ctx, endpoint)
	if err != nil {
		return Plan{}, err
	}
	defer source.Close()
	namespace := cfg.Source.Schema
	if namespace == "" {
		namespace = cfg.Source.Database
	}
	names, err := engine.ListMySQLTables(ctx, source, namespace)
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
		if err := source.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(namespace, name)).Scan(&rows); err != nil {
			return Plan{}, fmt.Errorf("count MySQL source table %s: %w", name, err)
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
