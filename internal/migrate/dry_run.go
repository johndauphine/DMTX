package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

// Plan is the deterministic, non-mutating result of a dry run.
type Plan struct {
	Proceed    bool                `json:"proceed"`
	SourceType string              `json:"source_type"`
	TargetType string              `json:"target_type"`
	TargetMode string              `json:"target_mode"`
	Tables     []PlannedTable      `json:"tables"`
	Admission  *PlannedAdmission   `json:"admission,omitempty"`
	Target     *PlannedTarget      `json:"target,omitempty"`
	Tuning     *PlannedTuning      `json:"tuning,omitempty"`
	Deletes    *PlannedDelete      `json:"deletes,omitempty"`
	Schema     *PlannedSchemaDrift `json:"schema_drift,omitempty"`

	currentSchema      schema.SchemaSnapshot
	currentSchemaReady bool
}

// PlannedSchemaDriftStatus describes whether an applicable successful schema
// baseline exists and, if it does, whether the filtered source shape changed.
type PlannedSchemaDriftStatus string

const (
	PlannedSchemaBaselineAbsent PlannedSchemaDriftStatus = "baseline_absent"
	PlannedSchemaUnchanged      PlannedSchemaDriftStatus = "unchanged"
	PlannedSchemaChanged        PlannedSchemaDriftStatus = "changed"
	PlannedSchemaUnavailable    PlannedSchemaDriftStatus = "unavailable"
)

// PlannedSchemaPolicy makes the exact effective Stage 4 schema policy visible
// alongside the decisions it produced. A nil contract retains the documented
// report-only default unless fail_on_schema_drift is true.
type PlannedSchemaPolicy struct {
	TargetMode        string                 `json:"target_mode"`
	FailOnSchemaDrift bool                   `json:"fail_on_schema_drift"`
	Contract          *config.SchemaContract `json:"schema_contract,omitempty"`
}

// PlannedSchemaDrift is deterministic source-schema evidence for dry-run.
// Facts and Decisions retain the existing structured drift/contract forms so
// table, column, and side-object changes never need to be reconstructed from a
// human message.
type PlannedSchemaDrift struct {
	Status         PlannedSchemaDriftStatus `json:"status"`
	CurrentDigest  string                   `json:"current_digest,omitempty"`
	BaselineDigest string                   `json:"baseline_digest,omitempty"`
	Facts          []schema.SchemaDriftFact `json:"facts,omitempty"`
	Decisions      []SchemaContractDecision `json:"decisions,omitempty"`
	Policy         PlannedSchemaPolicy      `json:"policy"`
	BlocksProceed  bool                     `json:"blocks_proceed"`
	Error          string                   `json:"error,omitempty"`
}

// DryRunSchemaBaseline is the read-only state evidence supplied by the CLI.
// A dry run intentionally cannot create a run merely to use the stateful
// Stage4Backend selector, so this value carries the selected durable record.
type DryRunSchemaBaseline struct {
	Found         bool
	CanonicalJSON string
	Digest        string
	Error         string
}

// PlannedAdmission records a configuration-only Stage 4 policy decision.
// Unlike target preflight it is determined without opening either endpoint,
// which makes a rejected dry run useful for source inventory planning without
// implying that the composed migration can proceed.
type PlannedAdmission struct {
	Supported bool   `json:"supported"`
	Error     string `json:"error,omitempty"`
}

type PlannedTargetPresence string

const (
	PlannedTargetPresent PlannedTargetPresence = "present"
	PlannedTargetAbsent  PlannedTargetPresence = "absent"
	PlannedTargetUnknown PlannedTargetPresence = "unknown"
)

type PlannedTargetPreflight string

const (
	PlannedTargetPreflightPassed  PlannedTargetPreflight = "passed"
	PlannedTargetPreflightSkipped PlannedTargetPreflight = "skipped"
	PlannedTargetPreflightFailed  PlannedTargetPreflight = "failed"
)

// PlannedTarget reports whether the target was inspected and whether its
// route-specific read-only preflight passed. Dry-run never opens an absent
// SQLite target because even opening it would create a target artifact; that
// inability to perform the required target preflight is a non-proceed result.
type PlannedTarget struct {
	Presence    PlannedTargetPresence  `json:"presence"`
	Preflight   PlannedTargetPreflight `json:"preflight"`
	Error       string                 `json:"error,omitempty"`
	Limitations []string               `json:"limitations,omitempty"`
}

// RowCountProvenance labels how a planned row count was obtained. Every source
// path currently issues an exact COUNT(*), so no dry-run row count is an
// estimate. The label exists so that a future cheaper path cannot start
// reporting estimates through a field operators already read as exact.
type RowCountProvenance string

const (
	RowCountExact    RowCountProvenance = "exact"
	RowCountEstimate RowCountProvenance = "estimate"
)

type PlannedTable struct {
	Name           string             `json:"name"`
	Rows           int                `json:"rows"`
	RowsProvenance RowCountProvenance `json:"rows_provenance"`
	Pagination     *PlannedPagination `json:"pagination,omitempty"`
}

// PlannedPagination reports the strategy a table would be read with. It is
// omitted rather than guessed when the source engine has no dry-run planning
// path, because a wrong strategy reads as a promise about how the migration will
// behave. Ranges are deliberately excluded: the strategy, keys, and partition
// count are the operator-relevant facts, and boundary lists are large and
// change with the data.
type PlannedPagination struct {
	Strategy     string   `json:"strategy"`
	Keys         []string `json:"keys,omitempty"`
	Partitions   int      `json:"partitions"`
	TopologyHash string   `json:"topology_hash"`
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

// DiscloseTuning resolves the resource plan a migration would run under,
// reading host memory evidence and nothing else. It opens no database, takes no
// lease, and writes nothing.
//
// Exported so that dry run and analyze disclose the same numbers in the same
// shape from one code path. Two paths producing "the effective plan" would be
// two chances to disagree about it, and an operator comparing them would have
// no way to tell which was right.
func DiscloseTuning(ctx context.Context, cfg config.Config) (*PlannedTuning, error) {
	resources, err := config.ResolveSystemEffectiveTransferPlan(
		ctx,
		cfg.Migration,
		config.TransferPlanOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("disclose tuning: %w", err)
	}
	fromInt := func(value config.EffectiveInt) PlannedSetting {
		return PlannedSetting{
			Value:      int64(value.Value),
			Provenance: string(value.Provenance),
		}
	}
	return &PlannedTuning{
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
	}, nil
}

// PlannedDelete discloses the configured delete policy and, when the caller
// supplies a state path, the read-only durable due-state evidence.
type PlannedDelete struct {
	Mode              string               `json:"mode"`
	Schedule          string               `json:"schedule,omitempty"`
	IntervalSeconds   int64                `json:"interval_seconds,omitempty"`
	RequirePrimaryKey bool                 `json:"require_primary_key,omitempty"`
	Tables            []PlannedDeleteTable `json:"tables,omitempty"`
	DueStateKnown     bool                 `json:"due_state_known"`
	Due               bool                 `json:"due"`
	LastSuccessfulAt  *time.Time           `json:"last_successful_at,omitempty"`
	NextDueAt         *time.Time           `json:"next_due_at,omitempty"`
	DueReason         string               `json:"due_reason,omitempty"`
	DueStateScope     string               `json:"due_state_scope,omitempty"`
	StateError        string               `json:"state_error,omitempty"`
}

// PlannedDeleteTable is the durable scheduling evidence for one exact source
// table workload. Delete reconciliation is never scheduled from a global
// state-file record: its source, target, and task identity must match this
// table before prior completion can affect due-ness.
type PlannedDeleteTable struct {
	Schema                       string                                 `json:"schema,omitempty"`
	Table                        string                                 `json:"table"`
	DueStateKnown                bool                                   `json:"due_state_known"`
	Due                          bool                                   `json:"due"`
	LastSuccessfulAt             *time.Time                             `json:"last_successful_at,omitempty"`
	NextDueAt                    *time.Time                             `json:"next_due_at,omitempty"`
	DueReason                    string                                 `json:"due_reason,omitempty"`
	CandidateImpactStatus        PlannedDeleteCandidateImpactStatus     `json:"candidate_impact_status,omitempty"`
	CandidateCount               *int64                                 `json:"candidate_count,omitempty"`
	CandidateDigest              string                                 `json:"candidate_digest,omitempty"`
	CandidateEqualityProofDigest string                                 `json:"candidate_equality_proof_digest,omitempty"`
	CandidateBatchCount          *int64                                 `json:"candidate_batch_count,omitempty"`
	CandidateProvenance          PlannedDeleteCandidateImpactProvenance `json:"candidate_provenance,omitempty"`
	CandidateLimitations         []string                               `json:"candidate_limitations,omitempty"`
}

// PlannedDeleteCandidateImpactStatus distinguishes an exact currently due
// key-set result from a deliberately unscanned not-due workload and from an
// unavailable proof. A nil CandidateCount therefore never means zero.
type PlannedDeleteCandidateImpactStatus string

const (
	PlannedDeleteCandidateImpactExact       PlannedDeleteCandidateImpactStatus = "exact"
	PlannedDeleteCandidateImpactNotDue      PlannedDeleteCandidateImpactStatus = "not_due"
	PlannedDeleteCandidateImpactUnavailable PlannedDeleteCandidateImpactStatus = "unavailable"
)

// PlannedDeleteCandidateImpactProvenance identifies the exact production
// comparison primitive behind a reported candidate count.
type PlannedDeleteCandidateImpactProvenance string

const (
	PlannedDeleteCandidateImpactPrimaryKeySetDifference PlannedDeleteCandidateImpactProvenance = "complete_primary_key_set_difference"
)

// planDryRunDisclosure derives the tuning and delete-policy facts a dry run can
// report without opening the target, state, lease, or audit log. Tuning comes
// from the same resolver the migration itself uses, so the disclosed numbers are
// the numbers that would apply rather than a parallel estimate.
func planDryRunDisclosure(
	ctx context.Context,
	cfg config.Config,
) (*PlannedTuning, *PlannedDelete, error) {
	tuning, err := DiscloseTuning(ctx, cfg)
	if err != nil {
		return nil, nil, err
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
	plan, err := dryRunWithDiscovery(ctx, cfg, discoverDryRunPlan)
	if err != nil || plan.Admission == nil || !plan.Admission.Supported {
		return plan, err
	}
	if err := attachDryRunCurrentSchema(ctx, cfg, &plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// ApplyDryRunSchemaDrift applies the existing deterministic snapshot and
// schema-contract logic to a read-only historical baseline. It never stages a
// snapshot or changes durable state. Callers may pass Error when state could
// not be read safely; that becomes a structured non-proceed limitation.
func ApplyDryRunSchemaDrift(
	plan *Plan,
	cfg config.Config,
	baseline DryRunSchemaBaseline,
) {
	if plan == nil || plan.Admission == nil || !plan.Admission.Supported {
		return
	}
	policy := PlannedSchemaPolicy{
		TargetMode:        cfg.Migration.TargetMode,
		FailOnSchemaDrift: cfg.Migration.FailOnSchemaDrift,
	}
	if cfg.Migration.SchemaContract != nil {
		contract := *cfg.Migration.SchemaContract
		policy.Contract = &contract
	}
	disclosure := &PlannedSchemaDrift{Policy: policy}
	plan.Schema = disclosure
	if !plan.currentSchemaReady {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = "current filtered source schema is unavailable"
		plan.Proceed = false
		return
	}
	currentDigest, err := plan.currentSchema.Digest()
	if err != nil {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = "current filtered source schema could not be digested"
		plan.Proceed = false
		return
	}
	disclosure.CurrentDigest = currentDigest
	if baseline.Error != "" {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = baseline.Error
		plan.Proceed = false
		return
	}
	if !baseline.Found {
		disclosure.Status = PlannedSchemaBaselineAbsent
		return
	}
	previous, err := schema.ParseSchemaSnapshot([]byte(baseline.CanonicalJSON))
	if err != nil {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = "applicable schema baseline is invalid"
		plan.Proceed = false
		return
	}
	previousDigest, err := previous.Digest()
	if err != nil || baseline.Digest == "" ||
		previousDigest != baseline.Digest {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = "applicable schema baseline digest is invalid"
		plan.Proceed = false
		return
	}
	disclosure.BaselineDigest = previousDigest
	facts, err := schema.CompareSchemaSnapshots(previous, plan.currentSchema)
	if err != nil {
		disclosure.Status = PlannedSchemaUnavailable
		disclosure.BlocksProceed = true
		disclosure.Error = "applicable schema drift facts are ambiguous or invalid"
		plan.Proceed = false
		return
	}
	disclosure.Facts = facts
	if len(facts) == 0 {
		disclosure.Status = PlannedSchemaUnchanged
		return
	}
	disclosure.Status = PlannedSchemaChanged
	contractPlan, contractErr := BuildSchemaContractPlan(
		previous,
		plan.currentSchema,
		SchemaContractOptions{
			Contract:           cfg.Migration.SchemaContract,
			FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
			TargetMode:         cfg.Migration.TargetMode,
			DateUpdatedColumns: append([]string(nil), cfg.Migration.DateUpdatedColumns...),
		},
	)
	disclosure.Decisions = contractPlan.Decisions
	if contractErr != nil {
		disclosure.BlocksProceed = true
		disclosure.Error = contractErr.Error()
		plan.Proceed = false
	}
}

func attachDryRunCurrentSchema(
	ctx context.Context,
	cfg config.Config,
	plan *Plan,
) (resultErr error) {
	if plan == nil {
		return errors.New("dry-run plan is required")
	}
	route, err := resolveMigration(cfg, builtInAdapters)
	if err != nil {
		return fmt.Errorf("resolve migration route for source schema: %w", err)
	}
	snapshotCfg, cleanup, err := dryRunSQLiteEndpointSnapshots(
		cfg,
		true,
		false,
	)
	if err != nil {
		return fmt.Errorf("capture source for dry-run schema: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, cleanup())
	}()
	source, err := route.source.open(ctx, snapshotCfg.Source)
	if err != nil {
		return fmt.Errorf("open source for dry-run schema: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, source.Close())
	}()
	tables := make([]schema.Table, 0, len(plan.Tables))
	for _, planned := range plan.Tables {
		table, inspectErr := source.InspectTable(ctx, planned.Name)
		if inspectErr != nil {
			return fmt.Errorf(
				"inspect source table %s for dry-run schema: %w",
				planned.Name,
				inspectErr,
			)
		}
		tables = append(tables, table)
	}
	snapshot, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		return fmt.Errorf("build current dry-run schema snapshot: %w", err)
	}
	plan.currentSchema = snapshot
	plan.currentSchemaReady = true
	return nil
}

// dryRunWithDiscovery keeps the pure composed-adapter policy decision ahead of
// the source-discovery function. Besides avoiding a misleading inventory for
// an unsupported execution mode, that ordering proves dry-run never opens
// either endpoint when the real composed runner would reject from config.
func dryRunWithDiscovery(
	ctx context.Context,
	cfg config.Config,
	discover func(context.Context, config.Config) (Plan, error),
) (Plan, error) {
	if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
		return Plan{
			Proceed:    false,
			SourceType: cfg.Source.Type,
			TargetType: cfg.Target.Type,
			TargetMode: cfg.Migration.TargetMode,
			Admission: &PlannedAdmission{
				Supported: false,
				Error:     err.Error(),
			},
		}, nil
	}
	plan, err := discover(ctx, cfg)
	if err != nil {
		return Plan{}, err
	}
	tuning, deletes, err := planDryRunDisclosure(ctx, cfg)
	if err != nil {
		return Plan{}, err
	}
	plan.Proceed = true
	plan.Tuning = tuning
	plan.Deletes = deletes
	plan.Admission = &PlannedAdmission{Supported: true}
	plan.Target = planDryRunTargetPreflight(ctx, cfg, plan)
	if plan.Target.Preflight == PlannedTargetPreflightFailed {
		plan.Proceed = false
	}
	return plan, nil
}

func planDryRunTargetPreflight(
	ctx context.Context,
	cfg config.Config,
	plan Plan,
) *PlannedTarget {
	targetPlan := &PlannedTarget{
		Presence:  PlannedTargetUnknown,
		Preflight: PlannedTargetPreflightFailed,
	}
	route, err := resolveMigration(cfg, builtInAdapters)
	if err != nil {
		targetPlan.Error = "migration route could not be resolved for target preflight"
		return targetPlan
	}

	if cfg.Target.Type == "sqlite" {
		path, pathErr := config.CanonicalSQLitePath(cfg.Target.Database)
		if pathErr != nil {
			targetPlan.Error = "SQLite target path could not be resolved"
			return targetPlan
		}
		info, statErr := os.Stat(path)
		switch {
		case statErr == nil && info.IsDir():
			targetPlan.Error = "SQLite target path is a directory"
			return targetPlan
		case statErr == nil:
			targetPlan.Presence = PlannedTargetPresent
		case os.IsNotExist(statErr) && cfg.Migration.TargetMode == "upsert":
			targetPlan.Presence = PlannedTargetAbsent
			targetPlan.Error =
				"upsert requires an existing target; the SQLite target file is absent"
			return targetPlan
		case os.IsNotExist(statErr):
			targetPlan.Presence = PlannedTargetAbsent
			targetPlan.Error =
				"SQLite target is absent; required target preflight cannot open it without creating a target artifact"
			targetPlan.Limitations = []string{
				"target SQLite catalog was not opened because the file is absent",
				"dry-run cannot perform deterministic target preflight without creating the target",
			}
			return targetPlan
		default:
			targetPlan.Error = "inspect SQLite target: target path could not be inspected"
			return targetPlan
		}
	}

	if err := runDryRunTargetPreflight(ctx, cfg, route, plan); err != nil {
		targetPlan.Error = err.Error()
		return targetPlan
	}
	targetPlan.Presence = PlannedTargetPresent
	targetPlan.Preflight = PlannedTargetPreflightPassed
	return targetPlan
}

func runDryRunTargetPreflight(
	ctx context.Context,
	cfg config.Config,
	route resolvedAdapterRoute,
	plan Plan,
) (resultErr error) {
	snapshotCfg, cleanup, err := dryRunSQLiteEndpointSnapshots(cfg, true, true)
	if err != nil {
		return fmt.Errorf("capture endpoints for read-only target preflight: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, cleanup())
	}()
	target, err := route.target.open(ctx, snapshotCfg.Target)
	if err != nil {
		return errors.New("target endpoint could not be opened for read-only preflight")
	}
	defer func() {
		resultErr = errors.Join(resultErr, target.Close())
	}()
	source, err := route.source.open(ctx, snapshotCfg.Source)
	if err != nil {
		return errors.New("source schema could not be reopened for target preflight")
	}
	defer func() {
		resultErr = errors.Join(resultErr, source.Close())
	}()
	sourceTables := make([]schema.Table, 0, len(plan.Tables))
	for _, planned := range plan.Tables {
		table, inspectErr := source.InspectTable(ctx, planned.Name)
		if inspectErr != nil {
			return errors.New("source schema could not be inspected for target preflight")
		}
		sourceTables = append(sourceTables, table)
	}
	if preflighter, ok := source.(adapterSourceRowPreflighter); ok {
		if err := preflighter.PreflightRows(ctx, sourceTables); err != nil {
			return errors.New("source data preflight failed")
		}
	}
	targetTables, err := target.PlanTables(
		route.source.engine,
		sourceTables,
		cfg.Migration.TargetMode,
	)
	if err != nil {
		return errors.New("target schema projection failed during dry-run preflight")
	}
	if err := preflightAdapterTargetPlan(
		ctx,
		target,
		targetTables,
		cfg.Migration.TargetMode,
	); err != nil {
		return errors.New("target route admission failed during dry-run preflight")
	}
	if err := target.PreflightTables(
		ctx,
		targetTables,
		cfg.Migration.TargetMode,
	); err != nil {
		return errors.New("target read-only preflight failed")
	}
	if preflighter, ok := target.(adapterTargetDestructivePreflighter); ok {
		if err := preflighter.PreflightDestructive(
			ctx,
			targetTables,
			cfg.Migration,
		); err != nil {
			return errors.New("destructive target preflight failed")
		}
	}
	plans := make([]adapterTablePlan, len(sourceTables))
	for index := range sourceTables {
		plans[index] = adapterTablePlan{
			source:  sourceTables[index],
			target:  targetTables[index],
			columns: adapterColumnNames(sourceTables[index]),
		}
	}
	if preflighter, ok := target.(adapterTargetSourceDataPreflighter); ok {
		if err := preflighter.PreflightSourceData(
			ctx,
			source,
			plans,
			cfg.Migration.TargetMode,
		); err != nil {
			return errors.New("source data preflight failed")
		}
	}
	return nil
}

// discoverDryRunPlan validates the implemented pair and discovers source
// tables. Target and state inspection are attached by DryRun as separate
// read-only phases, so a source discovery error cannot be mistaken for target
// readiness.
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
	return sqliteDryRun(ctx, cfg)
}

func sqliteDryRun(
	ctx context.Context,
	cfg config.Config,
) (plan Plan, resultErr error) {
	if cfg.Source.Database == "" {
		return Plan{}, fmt.Errorf("SQLite source database path is required")
	}
	snapshotCfg, cleanup, err := dryRunSQLiteEndpointSnapshots(cfg, true, false)
	if err != nil {
		return Plan{}, fmt.Errorf("inspect SQLite source: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, cleanup())
	}()
	sourcePath, err := config.CanonicalSQLitePath(snapshotCfg.Source.Database)
	if err != nil {
		return Plan{}, fmt.Errorf("resolve SQLite dry-run source snapshot: %w", err)
	}
	source, err := sql.Open("sqlite", sqliteReadOnlyURI(sourcePath))
	if err != nil {
		return Plan{}, fmt.Errorf("open source: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, source.Close())
	}()
	names, err := userTables(ctx, source)
	if err != nil {
		return Plan{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Plan{}, err
	}
	plan = Plan{SourceType: cfg.Source.Type, TargetType: cfg.Target.Type, TargetMode: cfg.Migration.TargetMode, Tables: make([]PlannedTable, 0, len(names))}
	for _, name := range names {
		rows, err := countRows(ctx, source, name)
		if err != nil {
			return Plan{}, fmt.Errorf("count source table %s: %w", name, err)
		}
		plan.Tables = append(plan.Tables, PlannedTable{
			Name:           name,
			Rows:           rows,
			RowsProvenance: RowCountExact,
			Pagination:     planSQLiteDryRunPagination(ctx, source, name, cfg),
		})
	}
	return plan, nil
}

// planSQLiteDryRunPagination reports the strategy this table would be read with,
// on a best-effort basis. A dry run must stay useful when pagination cannot be
// planned — an unreadable table should not deny the operator the rest of the
// plan — so every failure omits the disclosure instead of failing the run.
func planSQLiteDryRunPagination(
	ctx context.Context,
	source *sql.DB,
	name string,
	cfg config.Config,
) *PlannedPagination {
	table, _, err := inspectTable(ctx, source, name)
	if err != nil {
		return nil
	}
	partitions := cfg.Migration.Partitions
	if partitions <= 0 {
		partitions = config.DefaultPartitions
	}
	plan, err := PlanSQLitePagination(ctx, source, table, partitions)
	if err != nil {
		return nil
	}
	keys := make([]string, 0, len(plan.Keys))
	for _, key := range plan.Keys {
		keys = append(keys, key.Name)
	}
	return &PlannedPagination{
		Strategy:     string(plan.Strategy),
		Keys:         keys,
		Partitions:   len(plan.Ranges),
		TopologyHash: plan.TopologyHash,
	}
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
		plan.Tables = append(plan.Tables, PlannedTable{
			Name: name, Rows: rows, RowsProvenance: RowCountExact,
		})
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
		plan.Tables = append(plan.Tables, PlannedTable{
			Name: name, Rows: rows, RowsProvenance: RowCountExact,
		})
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
		plan.Tables = append(plan.Tables, PlannedTable{
			Name: name, Rows: rows, RowsProvenance: RowCountExact,
		})
	}
	return plan, nil
}
