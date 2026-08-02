package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// Stage 4 route admission: which configurations the composed adapter route
// accepts, and the runtime-tuning intent checks that must refuse before any
// endpoint is contacted.

const (
	stage4AdapterNetworkTaskType = "network-table-copy"
	stage4AdapterCopyStrategy    = "stage4_adapter_network_ranges_v1"
	stage4AdapterCopyRangeID     = "range/0"
)

type stage4AdapterAdmission struct {
	run     Stage4RunContext
	enabled bool
}

// resolveStage4AdapterAdmission performs every observer- and
// configuration-only Stage 4 check before a composed route opens either
// endpoint. Legacy observers remain on the optional Stage 3 path.
func resolveStage4AdapterAdmission(
	cfg config.Config,
	observer TableObserver,
	resume bool,
) (stage4AdapterAdmission, error) {
	run, enabled, err := ResolveStage4RunContext(observer)
	if err != nil {
		return stage4AdapterAdmission{}, err
	}
	if !enabled {
		return stage4AdapterAdmission{}, nil
	}
	if run.Resume != resume {
		operation := "fresh composed-adapter migration"
		contextKind := "resume"
		if resume {
			operation = "composed-adapter resume"
			contextKind = "fresh"
		}
		return stage4AdapterAdmission{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%s received a %s Stage 4 run context",
				operation,
				contextKind,
			),
		)
	}
	if _, err := requireStage4TableSetObserver(observer); err != nil {
		return stage4AdapterAdmission{}, err
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return stage4AdapterAdmission{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 composed-adapter migration requires a lease-fenced target mutation protector",
			),
		)
	}
	if err := requireStage4AdapterConfigurationSeams(cfg); err != nil {
		return stage4AdapterAdmission{}, err
	}
	return stage4AdapterAdmission{
		run:     run,
		enabled: true,
	}, nil
}

func requireStage4AdapterConfigurationSeams(cfg config.Config) error {
	if err := config.ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	if err := requireStage4LargeTableThresholdComposition(cfg); err != nil {
		return err
	}
	if err := requireStage4CheckpointFrequencyComposition(cfg); err != nil {
		return err
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		// Incremental execution owns a different bounded runner and does not
		// yet feed committed batch boundaries into RuntimeTuningController.
		// A generated compatibility default remains explicitly disclosed as
		// inactive on its result. Any operator-requested tuning input must fail
		// here, before endpoints/checkpoints, rather than being silently ignored.
		// In particular, an explicit interval is tuning intent even when the
		// runtime_tuning boolean was generated as its compatibility default.
		if stage4IncrementalRuntimeTuningExplicitlyRequested(cfg.Migration) {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 runtime tuning is not yet composed with date-based incremental transfer; omit migration.runtime_tuning_interval and set migration.runtime_tuning: false for that route",
				),
			)
		}
		mode, err := normalizeAdapterTargetMode(
			cfg.Migration.TargetMode,
		)
		if err != nil {
			return err
		}
		if mode != "upsert" {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 date-based incremental migration requires target mode upsert",
				),
			)
		}
	}
	if cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
		mode, err := normalizeAdapterTargetMode(
			cfg.Migration.TargetMode,
		)
		if err != nil {
			return err
		}
		if mode != "upsert" {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 delete reconciliation requires target mode upsert",
				),
			)
		}
		sourceEngine, sourceErr := config.CanonicalEngine(
			cfg.Source.Type,
		)
		targetEngine, targetErr := config.CanonicalEngine(
			cfg.Target.Type,
		)
		if sourceErr != nil || targetErr != nil ||
			!((sourceEngine == "postgres" && targetEngine == "postgres") ||
				(sourceEngine == "sqlite" && targetEngine == "sqlite") ||
				(sourceEngine == "mysql" && targetEngine == "mysql") ||
				(sourceEngine == "mssql" && targetEngine == "mssql")) {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 delete reconciliation is currently certified only for PostgreSQL-to-PostgreSQL, SQLite-to-SQLite, live same-flavor MySQL 8.0-to-MySQL 8.0 or MariaDB 10.11-to-MariaDB 10.11, and SQL Server 2022-to-SQL Server 2022",
				),
			)
		}
		if cfg.Migration.StrictConsistency &&
			!(sourceEngine == "mssql" && targetEngine == "mssql") {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete reconciliation is not yet certified inside one strict snapshot epoch",
				),
			)
		}
		if len(cfg.Migration.DateUpdatedColumns) != 0 &&
			!(sourceEngine == "sqlite" && targetEngine == "sqlite") {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 delete reconciliation with date-based incremental transfer is certified only for SQLite-to-SQLite retained-source windows",
				),
			)
		}
	}
	if cfg.Migration.StrictConsistency {
		mode, err := normalizeAdapterTargetMode(
			cfg.Migration.TargetMode,
		)
		if err != nil {
			return err
		}
		if mode != "upsert" {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 PostgreSQL strict consistency currently requires target mode upsert",
				),
			)
		}
		if len(cfg.Migration.DateUpdatedColumns) != 0 {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 strict consistency is certified for full-table work; incremental windows retain ordinary live-count semantics",
				),
			)
		}
	}
	if cfg.Migration.Validation.Mode == config.ValidationFull {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 validation mode full is unsupported"),
		)
	}
	return nil
}

func stage4RuntimeTuningExplicitlyRequested(
	migration config.Migration,
) bool {
	provenance, found := migration.SettingProvenance("runtime_tuning")
	return found && provenance == config.ProvenanceRequested &&
		migration.RuntimeTuning
}

// stage4RuntimeTuningIntervalExplicitlyRequested deliberately does not
// inspect the runtime_tuning boolean. An explicit interval has no meaning
// without a boundary consumer, so it remains operator tuning intent even
// when runtime_tuning was generated true or explicitly disabled.
func stage4RuntimeTuningIntervalExplicitlyRequested(
	migration config.Migration,
) bool {
	provenance, found := migration.SettingProvenance(
		"runtime_tuning_interval",
	)
	return found && provenance == config.ProvenanceRequested
}

func stage4IncrementalRuntimeTuningExplicitlyRequested(
	migration config.Migration,
) bool {
	return stage4RuntimeTuningExplicitlyRequested(migration) ||
		stage4RuntimeTuningIntervalExplicitlyRequested(migration)
}

func stage4GeneratedIncrementalRuntimeTuningReport(
	migration config.Migration,
) *RuntimeTuningReport {
	if !migration.RuntimeTuning ||
		stage4IncrementalRuntimeTuningExplicitlyRequested(migration) {
		return nil
	}
	provenance, found := migration.SettingProvenance("runtime_tuning")
	if !found || provenance != config.ProvenanceDerived {
		return nil
	}
	return &RuntimeTuningReport{
		Enabled: false,
		Reason:  "generated runtime_tuning default is inactive for date-based incremental transfer; explicit enable is refused until the incremental boundary controller is composed",
		Tables:  []RuntimeTuningTableReport{},
	}
}

// adapterStage4ValidationProbeProvider is the explicit route seam for
// validation modes deeper than exact counts. Constructing a probe must remain
// read-only; the runner resolves it before checkpoints or target mutation.
type adapterStage4ValidationProbeProvider interface {
	Stage4ValidationProbe(
		sourceAdapter,
		targetAdapter,
		[]adapterTablePlan,
	) (ValidationCoreProbe, error)
}

// adapterTargetSchemaEvolutionCapability is the complete, optional production
// seam for applying schema-contract evolution to a live target. Projection
// remains pure through targetAdapter.PlanTables; these methods own only the
// target dialect, exact catalog preflight, and the lease-fenced mutation.
type adapterTargetSchemaEvolutionCapability interface {
	TargetSchemaEvolutionDialect() schema.Dialect
	TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner
	ReadTargetSchemaEvolutionCatalog(
		context.Context,
	) (TargetSchemaEvolutionCatalog, error)
	PreflightTargetSchemaEvolution(
		context.Context,
		TargetSchemaEvolutionRequest,
	) (TargetSchemaEvolutionPlan, error)
	ApplyTargetSchemaEvolutionPlan(
		context.Context,
		TargetSchemaEvolutionPlan,
	) error
}

// PostgreSQL is the first production target with an exact evolution
// implementation. Keeping dialect admission on the composed-route capability
// prevents the runner from inferring executable SQL from an engine label.
func (*postgresTargetAdapter) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.Postgres
}

type stage4AdapterPrepared struct {
	run                                Stage4RunContext
	gate                               Stage4SchemaGateResult
	configDigest                       string
	mode                               string
	plans                              []adapterTablePlan
	names                              []string
	targetTables                       []schema.Table
	validation                         ValidationCoreProbe
	validationPrimaryKeyEqualityProofs map[stage4RichTableKey]string
	strictSourceRows                   map[stage4RichTableKey]int64
	sourceCatalog                      map[stage4RichTableKey]schema.Table
	work                               []stage4AdapterWork
	network                            *networkStateCoordinator
	evolution                          *stage4AdapterTargetSchemaEvolution
	incremental                        *stage4AdapterIncrementalPrepared
	deletes                            *stage4AdapterPostgresDeletePrepared
	deleteJournalReadiness             *stage4AdapterDeleteJournalReadinessCapability
	deleteReconciliationStrict         map[stage4RichTableKey]bool
}

type stage4AdapterWork struct {
	task       state.TaskKey
	strategy   string
	topology   string
	ranges     []state.RangeState
	pagination PaginationPlan
}
