package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

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

func migrateWithStage4Adapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	run Stage4RunContext,
) (Result, error) {
	if _, err := requireStage4TableSetObserver(observer); err != nil {
		return Result{}, err
	}
	if err := requireStage4StrictRoute(
		cfg,
		source,
		target,
		mode,
	); err != nil {
		return Result{}, err
	}
	prepared, err := prepareStage4AdapterRun(
		ctx,
		cfg,
		observer,
		source,
		target,
		mode,
		run,
	)
	if err != nil {
		return Result{}, err
	}
	if prepared.incremental != nil {
		result, runErr := migrateWithStage4IncrementalAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			false,
			nil,
		)
		if result.RuntimeTuning == nil {
			result.RuntimeTuning =
				stage4GeneratedIncrementalRuntimeTuningReport(
					cfg.Migration,
				)
		}
		return result, runErr
	}
	var networkOptions []stage4AdapterNetworkAdmissionOption
	if cfg.Migration.StrictConsistency {
		networkOptions = append(
			networkOptions,
			withStage4StrictSnapshotComposition(),
		)
	}
	if prepared.deletes != nil {
		networkOptions = append(
			networkOptions,
			withStage4DeleteReconciliationComposition(),
		)
	}
	networkExecution, err := admitStage4AdapterNetworkTransfer(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		nil,
		networkOptions...,
	)
	if err != nil {
		return Result{}, err
	}
	if cfg.Migration.StrictConsistency {
		var (
			result Result
			runErr error
		)
		switch source.Engine() {
		case "postgres":
			result, runErr = migrateWithStage4PostgresStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		case "mssql":
			result, runErr = migrateWithStage4SQLServerStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		case "mysql":
			result, runErr = migrateWithStage4MySQLStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, false, nil,
			)
		case "sqlite":
			result, runErr = migrateWithStage4SQLiteStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, false, nil,
			)
		default:
			return Result{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 strict consistency has no composed runner for source engine %q",
					source.Engine(),
				),
			)
		}
		attachStage4AdapterRuntimeTuningReport(&result, networkExecution)
		return result, runErr
	}
	if networkExecution != nil && networkExecution.deferred {
		if err := checkpointStage4AdapterStableNetworkWork(
			ctx,
			observer,
			networkExecution,
			false,
			nil,
		); err != nil {
			return Result{}, err
		}
		if err := applyStage4AdapterTargetSchema(
			ctx,
			observer,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		); err != nil {
			return Result{}, err
		}
		if err := preflightStage4AdapterDesiredTargetAfterEvolution(
			ctx,
			target,
			prepared,
		); err != nil {
			return Result{}, err
		}
		if err := activateStage4AdapterPostgresDeleteComposition(
			ctx,
			prepared,
		); err != nil {
			return Result{}, err
		}
		if err := prevalidateStage4AdapterPostgresDeleteCompletedTargets(
			ctx,
			target,
			prepared,
			networkExecution,
		); err != nil {
			return Result{}, err
		}
		if err := ensureStage4AdapterDeleteJournalReadiness(
			ctx,
			observer,
			prepared,
		); err != nil {
			return Result{}, err
		}
		var result Result
		if prepared.deletes != nil {
			result, err = runStage4AdapterPostgresDeleteNetworkTables(
				ctx,
				cfg,
				observer,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		} else {
			result, err = runStage4AdapterStableNetworkTables(
				ctx,
				cfg,
				observer,
				target,
				prepared,
				networkExecution,
				false,
				nil,
			)
		}
		if err != nil {
			return result, err
		}
		if err := completeStage4AdapterTerminalSchemaGateSentinels(
			ctx,
			prepared,
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	if err := checkpointStage4AdapterWork(
		ctx,
		observer,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := applyStage4AdapterTargetSchema(
		ctx,
		observer,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return Result{}, err
	}
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(
		ctx,
		target,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := activateStage4AdapterPostgresDeleteComposition(
		ctx,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if err := ensureStage4AdapterDeleteJournalReadiness(
		ctx,
		observer,
		prepared,
	); err != nil {
		return Result{}, err
	}
	if networkExecution != nil {
		if err := bindStage4AdapterNetworkRestoresAndValidate(
			ctx,
			observer,
			networkExecution,
		); err != nil {
			return Result{}, err
		}
	}

	networkTransferStarted := false
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare Stage 4 tables",
		func() error {
			return target.PrepareTables(
				ctx,
				prepared.targetTables,
				mode,
			)
		},
	); err != nil {
		return Result{}, err
	}

	copiedRows := make([]int, len(prepared.plans))
	if networkExecution != nil {
		for _, plan := range prepared.plans {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			if err := observer.BeforeTable(
				ctx,
				plan.source.Name,
			); err != nil {
				return Result{}, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"checkpoint before %s: %w",
						plan.source.Name,
						err,
					),
				)
			}
		}
		networkTransferStarted = true
		copiedRows, err = runStage4AdapterNetworkTransfer(
			ctx,
			observer,
			networkExecution,
		)
		if err != nil {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
			return Result{}, err
		}
	} else {
		for index, plan := range prepared.plans {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			if observer != nil {
				if err := observer.BeforeTable(
					ctx,
					plan.source.Name,
				); err != nil {
					return Result{}, NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"checkpoint before %s: %w",
							plan.source.Name,
							err,
						),
					)
				}
			}
			copied, copyErr := copyAdapterRows(
				ctx,
				observer,
				source,
				target,
				plan.source,
				plan.target,
				plan.columns,
				mode,
			)
			if copyErr != nil {
				return Result{}, copyErr
			}
			copiedRows[index] = copied
		}
	}

	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"finalize Stage 4 tables",
		func() error {
			return target.FinalizeTables(
				ctx,
				prepared.targetTables,
				mode,
			)
		},
	); err != nil {
		if networkTransferStarted {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
		}
		return Result{}, err
	}
	if err := validateStage4AdapterRun(
		ctx,
		cfg,
		source,
		target,
		prepared,
	); err != nil {
		if networkTransferStarted {
			if resetErr := resetStage4AdapterUnpublishedNetworkWork(
				networkExecution,
			); resetErr != nil {
				err = errors.Join(err, resetErr)
			}
		}
		return Result{}, err
	}

	result := Result{}
	for index, plan := range prepared.plans {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		copied := copiedRows[index]
		if observer != nil {
			if err := observer.AfterTable(
				ctx,
				plan.source.Name,
				copied,
			); err != nil {
				return result, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"checkpoint after %s: %w",
						plan.source.Name,
						err,
					),
				)
			}
		}
		result.Tables++
		result.Rows += copied
	}
	if err := completeStage4AdapterWork(
		ctx,
		prepared.run,
		prepared.work,
	); err != nil {
		return result, err
	}
	if err := completeStage4AdapterTerminalSchemaGateSentinels(
		ctx,
		prepared,
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

func checkpointStage4AdapterTableSet(
	ctx context.Context,
	observer TableObserver,
	names []string,
) error {
	setObserver, err := requireStage4TableSetObserver(observer)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := setObserver.BeforeTables(
		ctx,
		append([]string(nil), names...),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint Stage 4 table set: %w", err),
		)
	}
	return ctx.Err()
}

// checkpointStage4AdapterStableNetworkWork materializes and checkpoints every
// table's exact stable pagination plan before schema DDL is permitted. The
// stable sessions are intentionally closed after checkpointing; execution
// reopens each table and requires the newly observed plan to match the durable
// one before preparing or writing that table.
func checkpointStage4AdapterStableNetworkWork(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (resultErr error) {
	if execution == nil || !execution.deferred {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 stable network work checkpoint requires a deferred execution",
			),
		)
	}
	if !resume && len(completed) == 0 {
		return checkpointStage4AdapterFreshStableNetworkWork(
			ctx,
			observer,
			execution,
		)
	}
	if err := adoptStage4AdapterNetworkInventory(execution); err != nil {
		return err
	}
	if err := reviseStage4AdapterNetworkInventoryOnResume(
		ctx,
		execution,
		completed,
	); err != nil {
		return err
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		execution.prepared.names,
	); err != nil {
		return err
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	for planIndex, plan := range execution.prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if _, err := execution.validateCompletedTable(
				ctx,
				planIndex,
				rows,
				false,
			); err != nil {
				return err
			}
			if execution.prepared.deletes != nil {
				bound, found, err :=
					execution.classifyStage4AdapterPostgresDeleteTransferredTable(
						ctx,
						planIndex,
					)
				if err != nil {
					return err
				}
				if !found || !bound.taskCompleted || bound.rows != rows {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"completed Stage 4 PostgreSQL delete table %s lacks exact durable transfer evidence",
							plan.source.Name,
						),
					)
				}
				bound.ordinaryCompleted = true
				if stage4AdapterPostgresDeleteAuthorityActivated(
					execution.prepared.deletes,
				) {
					strict, err :=
						authenticateStage4AdapterPostgresDeleteTerminal(
							ctx,
							execution.prepared.deletes,
							planIndex,
							bound.work,
						)
					if err != nil {
						return err
					}
					bound.terminalAuthenticated = true
					bound.terminalStrict = strict
				}
				if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(
					planIndex,
					bound,
				); err != nil {
					return err
				}
			}
			continue
		}
		if resume && execution.prepared.deletes != nil {
			bound, found, err :=
				execution.classifyStage4AdapterPostgresDeleteTransferredTable(
					ctx,
					planIndex,
				)
			if err != nil {
				return err
			}
			if found {
				if bound.taskCompleted &&
					stage4AdapterPostgresDeleteAuthorityActivated(
						execution.prepared.deletes,
					) {
					strict, err := authenticateStage4AdapterPostgresDeleteTerminal(
						ctx,
						execution.prepared.deletes,
						planIndex,
						bound.work,
					)
					if err != nil {
						return err
					}
					bound.terminalAuthenticated = true
					bound.terminalStrict = strict
				}
				if err := execution.bindStage4AdapterPostgresDeleteTransferredTable(
					planIndex,
					bound,
				); err != nil {
					return err
				}
				continue
			}
			if err := execution.rejectStage4AdapterPostgresDeleteAttemptBeforeReplay(
				ctx,
				planIndex,
			); err != nil {
				return err
			}
		}
		tableExecution, err := execution.openTable(
			ctx,
			planIndex,
			resume,
		)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after checkpointing %s: %w",
				plan.source.Name,
				closeErr,
			)
		}
	}
	return ctx.Err()
}

// checkpointStage4AdapterFreshStableNetworkWork establishes the exact Stage 4
// table inventory before any ordinary task or table work exists, which is the
// only order EnsureStage4TableInventory accepts. Planning is therefore kept
// apart from the durable write: every table is planned and its stable session
// closed, the inventory is published, the ordinary table set is checkpointed,
// and only then is each planned work plan committed. Execution still reopens
// each table and requires the newly observed plan to match the durable one.
func checkpointStage4AdapterFreshStableNetworkWork(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkExecution,
) error {
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	planned := make(
		[]*stage4AdapterNetworkTableExecution,
		len(execution.prepared.plans),
	)
	for planIndex := range execution.prepared.plans {
		tableExecution, err := execution.planTableOnce(ctx, planIndex)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.session.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after planning %s: %w",
				execution.prepared.plans[planIndex].source.Name,
				closeErr,
			)
		}
		planned[planIndex] = tableExecution
	}
	if err := publishStage4AdapterNetworkInventory(
		ctx,
		execution,
		planned,
	); err != nil {
		return err
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		execution.prepared.names,
	); err != nil {
		return err
	}
	for planIndex, tableExecution := range planned {
		if err := tableExecution.resetOrEnsurePlan(ctx, false); err != nil {
			return err
		}
		if err := tableExecution.bindRestoresAndValidate(ctx); err != nil {
			return err
		}
		// Keep the bound plan separate from prepared.work. The latter is the
		// immutable seed for reopening a stable source and recomputing this exact
		// topology; replacing it with the derived topology would rehash it on the
		// next open and falsely look like an unsafe replan.
		execution.recordBoundWork(planIndex, tableExecution.work)
	}
	return ctx.Err()
}

// publishStage4AdapterNetworkInventory records the exact selected table set as
// immutable pre-mutation authority. A backend without aggregate completion
// leaves the run on the older separate-mutation path rather than failing, so
// the route stays usable wherever aggregate state is unavailable.
func publishStage4AdapterNetworkInventory(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	planned []*stage4AdapterNetworkTableExecution,
) error {
	work := make([]stage4AdapterWork, len(planned))
	for index, tableExecution := range planned {
		if tableExecution == nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network table inventory is missing planned work for %s",
					execution.prepared.plans[index].source.Name,
				),
			)
		}
		work[index] = cloneStage4AdapterNetworkWork(tableExecution.work)
	}
	return publishStage4AdapterNetworkWorkInventory(ctx, execution, work)
}

// publishStage4AdapterNetworkWorkInventory persists an already recomputed
// immutable work set. Recovery uses this form only when the absence of every
// ordinary/table checkpoint proves the original fresh checkpoint sequence
// stopped before PrepareTables could run.
func publishStage4AdapterNetworkWorkInventory(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	work []stage4AdapterWork,
) error {
	aggregate, ok := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil
	}
	if len(work) != len(execution.prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network table inventory is incomplete"),
		)
	}
	// The inventory binds itself to the validated source schema, so that
	// snapshot must already be durable. Staging is idempotent; the target
	// schema stage restages the identical evidence later.
	if err := stageStage4SchemaGateSnapshots(
		ctx,
		execution.prepared.run,
		execution.prepared.gate,
		execution.prepared.evolution,
	); err != nil {
		return err
	}
	inventory := state.Stage4TableInventory{
		RunID:                execution.prepared.run.RunID,
		SchemaTask:           execution.prepared.gate.Task,
		SchemaTopologyHash:   execution.prepared.gate.TopologyHash,
		SchemaSnapshotDigest: execution.prepared.gate.PendingSnapshot.Digest,
		Tables: make(
			[]state.Stage4TableInventoryEntry,
			len(work),
		),
	}
	for index, item := range work {
		ranges := make(
			[]state.Stage4InventoryRange,
			len(item.ranges),
		)
		for rangeIndex, workRange := range item.ranges {
			ranges[rangeIndex] = state.Stage4InventoryRange{
				ID: workRange.ID,
			}
		}
		inventory.Tables[index] = state.Stage4TableInventoryEntry{
			Table:        item.task.Table,
			Task:         item.task,
			Strategy:     item.strategy,
			TopologyHash: item.topology,
			Ranges:       ranges,
		}
	}
	if err := aggregate.EnsureStage4TableInventory(inventory); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"publish immutable Stage 4 network table inventory before table checkpoints: %w",
				err,
			),
		)
	}
	execution.aggregate = aggregate
	return nil
}

// reviseStage4AdapterNetworkInventoryOnResume republishes the table inventory
// when a resumed run legitimately replans. A source that grew during an outage
// yields a different partition count, and the durable inventory pins the exact
// range identities a table completion is validated against, so it must follow
// the replan. The revision window is enforced by the state layer and closes as
// soon as any table publishes terminal evidence, which is why a resume that
// already carries completed tables keeps the inventory it was given.
func reviseStage4AdapterNetworkInventoryOnResume(
	ctx context.Context,
	execution *stage4AdapterNetworkExecution,
	completed map[string]int,
) error {
	if execution.aggregate == nil || len(completed) != 0 {
		return nil
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	planned := make(
		[]*stage4AdapterNetworkTableExecution,
		len(execution.prepared.plans),
	)
	for planIndex := range execution.prepared.plans {
		tableExecution, err := execution.planTableOnce(ctx, planIndex)
		if err != nil {
			return err
		}
		if closeErr := tableExecution.session.Close(); closeErr != nil {
			return fmt.Errorf(
				"close Stage 4 stable source after replanning %s: %w",
				execution.prepared.plans[planIndex].source.Name,
				closeErr,
			)
		}
		planned[planIndex] = tableExecution
	}
	return publishStage4AdapterNetworkInventory(ctx, execution, planned)
}

// adoptStage4AdapterNetworkInventory binds a resumed run to the inventory its
// original attempt published. A run without one predates aggregate composition
// and must keep completing tables through the older separate mutations.
func adoptStage4AdapterNetworkInventory(
	execution *stage4AdapterNetworkExecution,
) error {
	aggregate, ok := execution.prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return nil
	}
	_, found, err := aggregate.LoadStage4TableInventory(
		execution.prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 network table inventory before resume: %w",
				err,
			),
		)
	}
	if found {
		execution.aggregate = aggregate
	}
	return nil
}

func runStage4AdapterStableNetworkTables(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	defer func() {
		attachStage4AdapterRuntimeTuningReport(&result, execution)
	}()
	if prepared.mode == "drop_recreate" {
		result, resultErr = runStage4AdapterStableNetworkRebuildTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			execution,
			resume,
			completed,
		)
		return result, resultErr
	}
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := execution.advanceCompletedTable(
				ctx,
				planIndex,
				rows,
			); err != nil {
				return result, err
			}
			result.Tables++
			result.Rows += rows
			continue
		}
		tableExecution, err := execution.openTable(
			ctx,
			planIndex,
			resume,
		)
		if err != nil {
			return result, err
		}
		copied, err := runStage4AdapterStableNetworkTable(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			planIndex,
			tableExecution,
			resume,
		)
		if err != nil {
			return result, err
		}
		result.Tables++
		result.Rows += copied
	}
	return result, nil
}

// resetStage4AdapterUnpublishedNetworkWork clears durable page completion
// facts when a target has not passed validation and finalization. A restart
// must replay from the first page, not mistake a partially validated target
// for completed work. The admitted network writer owns replay safety, so this
// reset is conservative for both upsert and rebuild paths.
func resetStage4AdapterUnpublishedNetworkWork(
	execution *stage4AdapterNetworkExecution,
) error {
	if execution == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("reset unpublished Stage 4 network work: execution is unavailable"),
		)
	}
	work, err := execution.snapshotBoundWork()
	if err != nil {
		return err
	}
	for _, work := range work {
		task := state.WorkTask{
			RunID:        execution.prepared.run.RunID,
			Key:          work.task,
			Strategy:     work.strategy,
			TopologyHash: work.topology,
			StartedAt:    time.Now().UTC(),
		}
		ranges := make([]state.RangeState, len(work.ranges))
		for index := range work.ranges {
			ranges[index] = cloneInitialNetworkStateRange(work.ranges[index])
		}
		if err := execution.prepared.run.Backend.ResetWorkPlan(task, ranges); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"reset unpublished Stage 4 network work for %s: %w",
					work.task.Table,
					err,
				),
			)
		}
	}
	return nil
}

// runStage4AdapterStableNetworkRebuildTableData transfers one table after the
// full target set has been recreated. Set-wide finalization occurs before
// validation, matching the public lifecycle; completion remains deferred until
// both phases succeed.
func runStage4AdapterStableNetworkRebuildTableData(
	ctx context.Context,
	observer TableObserver,
	execution *stage4AdapterNetworkTableExecution,
) (_ int, resultErr error) {
	if execution == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable rebuild table execution is unavailable"),
		)
	}
	defer func() {
		if closeErr := execution.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}
	}()
	copied, err := execution.run(ctx, observer)
	if err != nil {
		return 0, err
	}
	return copied, nil
}

func runStage4AdapterStableNetworkTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
	execution *stage4AdapterNetworkTableExecution,
	resume bool,
) (_ int, resultErr error) {
	if execution == nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 stable table execution is unavailable"),
		)
	}
	defer func() {
		if closeErr := execution.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}
	}()
	plan := prepared.plans[planIndex]
	name := plan.source.Name
	if resume {
		if err := checkAdapterResumeContext(
			ctx,
			"checkpoint before "+name,
		); err != nil {
			return 0, err
		}
	}
	if err := observer.BeforeTable(ctx, name); err != nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint before %s: %w", name, err),
		)
	}
	mutationObserver := observer
	if resume {
		mutationObserver = adapterResumeMutationGuard{
			ctx:      ctx,
			delegate: observer,
			boundary: "mutate resumed Stage 4 table " + name,
		}
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"prepare Stage 4 table "+name,
		func() error {
			return target.PrepareTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(plan.target),
				},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, err
	}
	copied, err := execution.run(ctx, mutationObserver)
	if err != nil {
		return 0, err
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"finalize Stage 4 table "+name,
		func() error {
			return target.FinalizeTables(
				ctx,
				[]schema.Table{
					cloneStage4RichTable(plan.target),
				},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, err
	}
	if err := validateStage4AdapterStableTable(
		ctx,
		cfg,
		observer,
		execution.parent.source,
		execution.source,
		target,
		prepared,
		planIndex,
	); err != nil {
		return 0, err
	}
	if err := completeStage4AdapterNetworkTable(
		ctx,
		observer,
		prepared.run,
		execution.parent.aggregate,
		execution.work,
		name,
		copied,
	); err != nil {
		return 0, fmt.Errorf(
			"complete Stage 4 work for %s: %w",
			name,
			err,
		)
	}
	return copied, nil
}

func validateStage4AdapterStableTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	providerSource sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
) error {
	plan := cloneStage4AdapterNetworkTablePlan(
		prepared.plans[planIndex],
	)
	probe, err := stage4AdapterValidationProbe(
		cfg,
		observer,
		source,
		target,
		[]adapterTablePlan{plan},
		providerSource,
	)
	if err != nil {
		return err
	}
	tablePrepared := prepared
	tablePrepared.plans = []adapterTablePlan{plan}
	tablePrepared.validation = probe
	tablePrepared.gate.ValidationTables = []schema.Table{
		cloneStage4RichTable(plan.source),
	}
	return validateStage4AdapterRun(
		ctx,
		cfg,
		source,
		target,
		tablePrepared,
	)
}

func resumeWithStage4Adapters(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	observer TableObserver,
	taskObserver TableSetObserver,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	run Stage4RunContext,
) (Result, error) {
	if _, err := requireStage4TableSetObserver(observer); err != nil {
		return Result{}, err
	}
	if taskObserver == nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 composed-adapter resume is missing its required table-set observer",
			),
		)
	}
	if err := requireStage4StrictRoute(
		cfg,
		source,
		target,
		mode,
	); err != nil {
		return Result{}, err
	}
	prepared, err := prepareStage4AdapterRun(
		ctx,
		cfg,
		observer,
		source,
		target,
		mode,
		run,
	)
	if err != nil {
		return Result{}, err
	}
	if mode == "drop_recreate" {
		if prepared.incremental != nil || prepared.deletes != nil ||
			cfg.Migration.StrictConsistency {
			return Result{}, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 drop_recreate resume cannot compose incremental, delete reconciliation, or strict consistency work",
				),
			)
		}
		networkExecution, err := admitStage4AdapterNetworkTransfer(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			nil,
		)
		if err != nil {
			return Result{}, err
		}
		rebuildCompleted := make(map[string]int, len(completed))
		for table, checkpoint := range completed {
			rebuildCompleted[table] = checkpoint.Rows
		}
		if err := completeStage4AdapterNetworkRebuildCheckpointPrefix(
			ctx,
			observer,
			networkExecution,
			rebuildCompleted,
		); err != nil {
			return Result{}, err
		}
		// Staging the schema gate is a state-only operation for a rebuild; the
		// target set itself is recreated only after durable recovery admission.
		if err := applyStage4AdapterTargetSchema(
			ctx,
			observer,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		); err != nil {
			return Result{}, err
		}
		if err := preflightStage4AdapterDesiredTargetAfterEvolution(
			ctx,
			target,
			prepared,
		); err != nil {
			return Result{}, err
		}
		if err := ensureStage4AdapterDeleteJournalReadiness(
			ctx,
			observer,
			prepared,
		); err != nil {
			return Result{}, err
		}
		result, err := runStage4AdapterStableNetworkRebuildTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			networkExecution,
			true,
			rebuildCompleted,
		)
		attachStage4AdapterRuntimeTuningReport(&result, networkExecution)
		if err != nil {
			return result, err
		}
		if err := completeStage4AdapterTerminalSchemaGateSentinels(
			ctx,
			prepared,
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	validated, err := validateCompletedStage4NetworkTableCheckpoints(
		ctx,
		target,
		prepared.plans,
		completed,
		false,
	)
	if err != nil {
		return Result{}, err
	}
	if prepared.incremental != nil {
		result, runErr := migrateWithStage4IncrementalAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			true,
			validated,
		)
		if result.RuntimeTuning == nil {
			result.RuntimeTuning =
				stage4GeneratedIncrementalRuntimeTuningReport(
					cfg.Migration,
				)
		}
		return result, runErr
	}
	// Static route, target, dependency, replay, and resource admission precedes
	// BeforeTables and every per-table durable reset/ensure operation.
	var networkOptions []stage4AdapterNetworkAdmissionOption
	if cfg.Migration.StrictConsistency {
		networkOptions = append(
			networkOptions,
			withStage4StrictSnapshotComposition(),
		)
	}
	if prepared.deletes != nil {
		networkOptions = append(
			networkOptions,
			withStage4DeleteReconciliationComposition(),
		)
	}
	networkExecution, err := admitStage4AdapterNetworkTransfer(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		nil,
		networkOptions...,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if cfg.Migration.StrictConsistency {
		var (
			result Result
			runErr error
		)
		switch source.Engine() {
		case "postgres":
			result, runErr = migrateWithStage4PostgresStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				true,
				validated,
			)
		case "mssql":
			result, runErr = migrateWithStage4SQLServerStrictAdapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				prepared,
				networkExecution,
				true,
				validated,
			)
		case "mysql":
			result, runErr = migrateWithStage4MySQLStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, true, validated,
			)
		case "sqlite":
			result, runErr = migrateWithStage4SQLiteStrictAdapters(
				ctx, cfg, observer, source, target, prepared,
				networkExecution, true, validated,
			)
		default:
			return resultForValidatedAdapterCheckpoints(validated), NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 strict consistency has no composed runner for source engine %q",
					source.Engine(),
				),
			)
		}
		attachStage4AdapterRuntimeTuningReport(&result, networkExecution)
		return result, runErr
	}
	if err := networkExecution.prevalidateCompletedTables(
		ctx,
		validated,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := checkpointStage4AdapterStableNetworkWork(
		ctx,
		observer,
		networkExecution,
		true,
		validated,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := applyStage4AdapterTargetSchema(
		ctx,
		observer,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := preflightStage4AdapterDesiredTargetAfterEvolution(
		ctx,
		target,
		prepared,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := activateStage4AdapterPostgresDeleteComposition(
		ctx,
		prepared,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := prevalidateStage4AdapterPostgresDeleteCompletedTargets(
		ctx,
		target,
		prepared,
		networkExecution,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := ensureStage4AdapterDeleteJournalReadiness(
		ctx,
		observer,
		prepared,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	var result Result
	if prepared.deletes != nil {
		result, err = runStage4AdapterPostgresDeleteNetworkTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			networkExecution,
			true,
			validated,
		)
	} else {
		result, err = runStage4AdapterStableNetworkTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			networkExecution,
			true,
			validated,
		)
	}
	if err != nil {
		return result, err
	}
	if err := completeStage4AdapterTerminalSchemaGateSentinels(
		ctx,
		prepared,
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

func validateCompletedStage4NetworkTableCheckpoints(
	ctx context.Context,
	target targetAdapter,
	plans []adapterTablePlan,
	completed CompletedTableCheckpoints,
	reconciliationStrict bool,
) (map[string]int, error) {
	selected := make(map[string]adapterTablePlan, len(plans))
	for _, plan := range plans {
		if _, duplicate := selected[plan.source.Name]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"selected Stage 4 plan contains duplicate table %s",
					plan.source.Name,
				),
			)
		}
		selected[plan.source.Name] = plan
	}
	for name := range completed {
		if _, exists := selected[name]; !exists {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed checkpoint references table %s outside the current selection",
					name,
				),
			)
		}
	}
	validated := make(map[string]int, len(completed))
	for _, plan := range plans {
		checkpoint, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		if checkpoint.Rows < 0 {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed checkpoint for %s has invalid row count %d",
					plan.source.Name,
					checkpoint.Rows,
				),
			)
		}
		if err := checkAdapterResumeContext(
			ctx,
			"count completed target table "+plan.source.Name,
		); err != nil {
			return nil, err
		}
		targetRows, err := target.CountRows(ctx, plan.target)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"validate completed checkpoint for %s against target: %w",
					plan.source.Name,
					err,
				),
			)
		}
		if targetRows < checkpoint.Rows ||
			reconciliationStrict && targetRows != checkpoint.Rows {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed checkpoint for %s is not reusable: checkpoint has %d rows and target has %d rows",
					plan.source.Name,
					checkpoint.Rows,
					targetRows,
				),
			)
		}
		validated[plan.source.Name] = checkpoint.Rows
	}
	return validated, nil
}

func prepareStage4AdapterRun(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	run Stage4RunContext,
) (stage4AdapterPrepared, error) {
	result := stage4AdapterPrepared{run: run, mode: mode}
	names, err := source.ListTables(ctx)
	if err != nil {
		return result, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return result, err
	}
	discovered, err := discoverStage4AdapterTables(
		ctx,
		source,
		names,
		mode,
	)
	if err != nil {
		return result, err
	}
	result.sourceCatalog = make(
		map[stage4RichTableKey]schema.Table,
		len(discovered),
	)
	for _, table := range discovered {
		result.sourceCatalog[stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = cloneStage4RichTable(table)
	}
	configDigest, err := config.Hash(cfg)
	if err != nil {
		return result, fmt.Errorf(
			"build credential-free Stage 4 configuration identity: %w",
			err,
		)
	}
	result.configDigest = configDigest
	if err := ctx.Err(); err != nil {
		return result, err
	}
	gateOptions := Stage4SchemaGateOptions{
		SourceEngine:       source.Engine(),
		TargetEngine:       target.Engine(),
		TargetMode:         mode,
		IncludeTables:      cfg.Migration.IncludeTables,
		ExcludeTables:      cfg.Migration.ExcludeTables,
		ConfigIdentity:     configDigest,
		Contract:           cfg.Migration.SchemaContract,
		FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
		DateUpdatedColumns: cfg.Migration.DateUpdatedColumns,
	}
	gate, err := PrepareStage4SchemaGate(
		run,
		discovered,
		gateOptions,
	)
	if err != nil {
		return result, fmt.Errorf("prepare Stage 4 schema gate: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.gate = gate
	if err := verifyStage4SchemaSentinelEvidence(
		ctx,
		run,
		gate,
	); err != nil {
		return result, err
	}
	if err := publishStage4SchemaDecisions(
		ctx,
		observer,
		run,
		gate,
		source.Engine(),
		target.Engine(),
		mode,
	); err != nil {
		return result, err
	}
	if err := requireStage4AdapterSeams(
		cfg,
		observer,
		source,
		target,
		gate,
		mode,
	); err != nil {
		return result, err
	}
	result.evolution, err = prepareStage4AdapterTargetSchema(
		ctx,
		run,
		gateOptions,
		source,
		target,
		mode,
		gate,
	)
	if err != nil {
		return result, err
	}

	effective := append(
		[]schema.Table(nil),
		gate.TransferTables...,
	)
	if orderer, ok := target.(adapterTargetSourceTableOrderer); ok {
		requested, orderErr := orderer.OrderSourceTables(
			source.Engine(),
			append([]schema.Table(nil), effective...),
			mode,
		)
		if orderErr != nil {
			return result, fmt.Errorf(
				"order Stage 4 source tables for target %s: %w",
				target.Engine(),
				orderErr,
			)
		}
		effective, err = validateAdapterTargetSourceTableOrder(
			effective,
			requested,
		)
		if err != nil {
			return result, fmt.Errorf(
				"order Stage 4 source tables for target %s: %w",
				target.Engine(),
				err,
			)
		}
	}
	plans, err := planStage4AdapterTargets(
		source.Engine(),
		target,
		effective,
		mode,
	)
	if err != nil {
		return result, err
	}
	result.plans = plans
	result.names = make([]string, len(plans))
	result.targetTables = make([]schema.Table, len(plans))
	for index, plan := range plans {
		result.names[index] = plan.source.Name
		result.targetTables[index] = plan.target
	}
	if err := preflightAdapterTargetPlan(
		ctx,
		target,
		result.targetTables,
		mode,
	); err != nil {
		return result, fmt.Errorf("preflight Stage 4 target plan: %w", err)
	}
	preflightTables := result.targetTables
	if result.evolution != nil {
		preflightTables, err =
			stage4AdapterExistingEvolutionTargetTables(
				result.evolution,
				result.targetTables,
			)
		if err != nil {
			return result, err
		}
	}
	if err := target.PreflightTables(
		ctx,
		preflightTables,
		mode,
	); err != nil {
		return result, fmt.Errorf(
			"preflight existing Stage 4 target tables before schema evolution: %w",
			err,
		)
	}
	if preflighter, ok := target.(adapterTargetDestructivePreflighter); ok {
		if err := preflighter.PreflightDestructive(
			ctx,
			result.targetTables,
			cfg.Migration,
		); err != nil {
			return result, fmt.Errorf(
				"preflight Stage 4 destructive target action: %w",
				err,
			)
		}
	}
	if preflighter, ok := target.(adapterTargetSourceDataPreflighter); ok {
		if err := preflighter.PreflightSourceData(
			ctx,
			source,
			plans,
			mode,
		); err != nil {
			return result, fmt.Errorf(
				"preflight Stage 4 source data for target: %w",
				err,
			)
		}
	}
	if len(cfg.Migration.DateUpdatedColumns) == 0 {
		result.validation, err = stage4AdapterValidationProbe(
			cfg,
			observer,
			source,
			target,
			plans,
		)
		if err != nil {
			return result, err
		}
		result.validationPrimaryKeyEqualityProofs, err =
			prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
				cfg.Migration.Validation.Mode,
				mode,
				result.validation,
				gate.ValidationTables,
			)
		if err != nil {
			return result, err
		}
	}
	result.work, err = buildStage4AdapterWork(
		configDigest,
		mode,
		plans,
	)
	if err != nil {
		return result, err
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		result.incremental, result.work, err =
			prepareStage4AdapterIncremental(
				ctx,
				cfg,
				source,
				target,
				result,
			)
		if err != nil {
			return result, err
		}
	}
	if cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
		result.deletes, err =
			prepareStage4AdapterPostgresDeleteComposition(
				ctx,
				cfg,
				observer,
				source,
				target,
				result,
			)
		if err != nil {
			return result, err
		}
		if result.incremental != nil {
			if err := prepareStage4AdapterSQLiteIncrementalDeleteComposition(
				ctx,
				cfg,
				source,
				target,
				&result,
			); err != nil {
				return result, err
			}
		}
		result.deleteJournalReadiness, err =
			admitStage4AdapterDeleteJournalReadinessForRun(
				ctx,
				observer,
				result.run,
				target,
			)
		if err != nil {
			return result, err
		}
	}
	if result.incremental != nil {
		return result, nil
	}
	if stage4AdapterNetworkRelationalEngine(source.Engine()) {
		// Relational network pagination, retained width, and durable ranges are
		// intentionally deferred until the runner owns one table-scoped stable
		// source view. Rebuild uses the same deferred inventory so every selected
		// target can be prepared as one destructive set before the first page.
		// Global preparation remains read-only and connection bounded.
		return result, nil
	}
	result.work, err = bindStage4AdapterPagination(
		ctx,
		source,
		cfg.Migration.Partitions,
		result.work,
		plans,
	)
	if err != nil {
		return result, err
	}
	result.network, err = newStage4AdapterNetworkCoordinator(
		run,
		result.work,
	)
	if err != nil {
		return result, err
	}
	return result, nil
}

func publishStage4SchemaDecisions(
	ctx context.Context,
	observer TableObserver,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	sourceEngine string,
	targetEngine string,
	targetMode string,
) error {
	sink, ok := observer.(Stage4SchemaDecisionObserver)
	if !ok {
		if len(gate.Plan.Decisions) == 0 {
			return nil
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 schema decisions require a typed decision observer before target planning",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	previousDigest, err := gate.PreviousSnapshot.Digest()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"digest previous Stage 4 schema decision evidence: %w",
				err,
			),
		)
	}
	currentDigest, err := gate.CurrentSnapshot.Digest()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"digest current Stage 4 schema decision evidence: %w",
				err,
			),
		)
	}
	successfulDigest, err := gate.Plan.SuccessfulSnapshot.Digest()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"digest successful Stage 4 schema decision evidence: %w",
				err,
			),
		)
	}
	decisions := make(
		[]SchemaContractDecision,
		len(gate.Plan.Decisions),
	)
	for index, decision := range gate.Plan.Decisions {
		decisions[index] = decision
		decisions[index].Previous = append(
			json.RawMessage(nil),
			decision.Previous...,
		)
		decisions[index].Current = append(
			json.RawMessage(nil),
			decision.Current...,
		)
	}
	report := Stage4SchemaDecisionReport{
		RunID:                  run.RunID,
		Resume:                 run.Resume,
		Baseline:               gate.Baseline,
		SourceEngine:           sourceEngine,
		TargetEngine:           targetEngine,
		TargetMode:             targetMode,
		GateTopologyHash:       gate.TopologyHash,
		PreviousSchemaDigest:   previousDigest,
		CurrentSchemaDigest:    currentDigest,
		SuccessfulSchemaDigest: successfulDigest,
		Decisions:              decisions,
	}
	if err := sink.ObserveStage4SchemaDecisions(
		ctx,
		report,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"publish Stage 4 schema decisions before target planning: %w",
				err,
			),
		)
	}
	return nil
}

func discoverStage4AdapterTables(
	ctx context.Context,
	source sourceAdapter,
	names []string,
	mode string,
) ([]schema.Table, error) {
	sourceEngine := source.Engine()
	if sourceEngine == "" {
		return nil, fmt.Errorf("Stage 4 source adapter engine is required")
	}
	tables := make([]schema.Table, 0, len(names))
	for _, name := range names {
		table, err := source.InspectTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if table.Name != name {
			return nil, fmt.Errorf(
				"source adapter %s inspected table %q as %q",
				sourceEngine,
				name,
				table.Name,
			)
		}
		if err := requireAdapterSourceRowOrder(
			source,
			table,
			mode,
		); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	var err error
	tables, err = orderAdapterSourceTablesForMode(tables, mode)
	if err != nil {
		return nil, err
	}
	if preflighter, ok := source.(adapterSourceRowPreflighter); ok {
		if err := preflighter.PreflightRows(ctx, tables); err != nil {
			return nil, fmt.Errorf(
				"preflight Stage 4 source rows: %w",
				err,
			)
		}
	}
	return tables, nil
}

func planStage4AdapterTargets(
	sourceEngine string,
	target targetAdapter,
	sourceTables []schema.Table,
	mode string,
) ([]adapterTablePlan, error) {
	if sourceEngine == "" || target.Engine() == "" {
		return nil, fmt.Errorf("Stage 4 source and target adapter engines are required")
	}
	targetTables, err := target.PlanTables(
		sourceEngine,
		sourceTables,
		mode,
	)
	if err != nil {
		return nil, err
	}
	if len(targetTables) != len(sourceTables) {
		return nil, fmt.Errorf(
			"target adapter %s planned %d tables for %d Stage 4 source tables",
			target.Engine(),
			len(targetTables),
			len(sourceTables),
		)
	}
	plans := make([]adapterTablePlan, len(sourceTables))
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		if targetTable.Name != sourceTable.Name {
			return nil, fmt.Errorf(
				"target adapter %s changed Stage 4 table name %s to %s",
				target.Engine(),
				sourceTable.Name,
				targetTable.Name,
			)
		}
		plans[index] = adapterTablePlan{
			source:  sourceTable,
			target:  targetTable,
			columns: adapterColumnNames(sourceTable),
		}
	}
	return plans, nil
}

type stage4AdapterTargetSchemaEvolution struct {
	capability adapterTargetSchemaEvolutionCapability
	authority  Stage4TargetShapeAuthority
	pending    state.SchemaSnapshot
	request    TargetSchemaEvolutionRequest
	plan       TargetSchemaEvolutionPlan
	prior      []schema.Table
	current    []schema.Table
}

func prepareStage4AdapterTargetSchema(
	ctx context.Context,
	run Stage4RunContext,
	gateOptions Stage4SchemaGateOptions,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	gate Stage4SchemaGateResult,
) (*stage4AdapterTargetSchemaEvolution, error) {
	// Drop/recreate owns its complete target lifecycle through the target
	// adapter's ordinary deterministic planner. The in-place catalog
	// evolution protocol is intentionally upsert-only.
	if mode != "upsert" {
		return nil, nil
	}
	requiresEvolution, decision := stage4AdapterTargetEvolutionDecision(
		mode,
		gate.Plan.Decisions,
	)
	capability, ok := target.(adapterTargetSchemaEvolutionCapability)
	if !ok || stage4AdapterTargetSchemaEvolutionCapabilityIsNil(capability) {
		if !requiresEvolution {
			return nil, nil
		}
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 upsert schema action %q for table %s requires a composed target-catalog evolution executor seam",
				decision.Action,
				decision.Object.Table,
			),
		)
	}
	dialect := capability.TargetSchemaEvolutionDialect()
	if dialect == "" {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target-catalog evolution capability for %s returned an empty target dialect",
				target.Engine(),
			),
		)
	}
	createPlanner := capability.TargetSchemaEvolutionCreatePlanner()
	if targetSchemaEvolutionCreatePlannerIsNil(createPlanner) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target-catalog evolution capability for %s returned a nil create planner",
				target.Engine(),
			),
		)
	}
	authority, err := PrepareStage4TargetShapeAuthority(
		run,
		gate,
		gateOptions,
		Stage4TargetShapeSeed{},
	)
	if errors.Is(err, ErrStage4TargetShapeSeedRequired) {
		catalog, readErr :=
			capability.ReadTargetSchemaEvolutionCatalog(ctx)
		if readErr != nil {
			return nil, fmt.Errorf(
				"read exact Stage 4 target catalog for shape authority: %w",
				readErr,
			)
		}
		seed, seedErr := NewStage4TargetShapeSeed(catalog)
		if seedErr != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"freeze exact Stage 4 target catalog seed: %w",
					seedErr,
				),
			)
		}
		authority, err = PrepareStage4TargetShapeAuthority(
			run,
			gate,
			gateOptions,
			seed,
		)
	}
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"prepare Stage 4 target-shape authority: %w",
				err,
			),
		)
	}
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		source.Engine(),
		target,
		mode,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"project Stage 4 target-catalog evolution: %w",
				err,
			),
		)
	}
	pending, err := BindStage4TargetShapeProjection(
		authority,
		projection,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"bind Stage 4 target-shape projection: %w",
				err,
			),
		)
	}
	request, err := NewTargetSchemaEvolutionRequest(
		dialect,
		projection,
		createPlanner,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"authorize Stage 4 target-catalog evolution: %w",
				err,
			),
		)
	}
	plan, err := capability.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		return nil, fmt.Errorf(
			"preflight Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	if plan.Digest() == "" || plan.Target() != dialect {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"preflight Stage 4 target-catalog evolution returned invalid plan authority",
			),
		)
	}
	return &stage4AdapterTargetSchemaEvolution{
		capability: capability,
		authority:  authority,
		pending:    pending,
		request:    request,
		plan:       plan,
		prior:      projection.PriorTables(),
		current:    projection.CurrentTables(),
	}, nil
}

func applyStage4AdapterTargetSchema(
	ctx context.Context,
	observer TableObserver,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := stageStage4SchemaGateSnapshots(
		ctx,
		run,
		gate,
		evolution,
	); err != nil {
		return err
	}
	if evolution == nil || evolution.plan.Complete() {
		return nil
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"apply Stage 4 target schema evolution",
		func() error {
			return evolution.capability.ApplyTargetSchemaEvolutionPlan(
				ctx,
				evolution.plan,
			)
		},
	); err != nil {
		return fmt.Errorf(
			"apply Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	verified, err := evolution.capability.PreflightTargetSchemaEvolution(
		ctx,
		evolution.request,
	)
	if err != nil {
		return fmt.Errorf(
			"reverify Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	if !verified.Complete() {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reverify Stage 4 target-catalog evolution: target catalog remains incomplete after apply",
			),
		)
	}
	if verified.Digest() != evolution.plan.Digest() {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reverify Stage 4 target-catalog evolution: plan digest changed from %s to %s",
				evolution.plan.Digest(),
				verified.Digest(),
			),
		)
	}
	return nil
}

func stage4AdapterExistingEvolutionTargetTables(
	evolution *stage4AdapterTargetSchemaEvolution,
	desired []schema.Table,
) ([]schema.Table, error) {
	if evolution == nil {
		return cloneTargetSchemaEvolutionTables(desired), nil
	}
	// PreflightTargetSchemaEvolution has already authenticated the exact live
	// catalog prefix. A process can fail after target DDL commits but before the
	// immediate post-apply reverify/state completion; on resume that prefix is
	// final, not the original prior. Use it for retained-table preflight so an
	// already-committed immutable evolution is not rejected as a shape drift.
	// The same rule handles a target dialect whose durable plan can expose an
	// authenticated nonzero partial prefix.
	existingTables := evolution.prior
	if evolution.plan.valid() {
		existingTables = evolution.plan.states[evolution.plan.AppliedPrefix()]
	}
	prior := make(
		map[targetSchemaEvolutionTableKey]schema.Table,
		len(existingTables),
	)
	for _, table := range existingTables {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, duplicate := prior[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape authority contains duplicate prior table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		prior[key] = cloneStage4RichTable(table)
	}
	result := make([]schema.Table, 0, len(desired))
	for _, table := range desired {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		existing, found := prior[key]
		if !found {
			continue
		}
		result = append(result, existing)
	}
	sortTargetSchemaEvolutionTables(result)
	return result, nil
}

func stage4AdapterCurrentEvolutionTargetTables(
	evolution *stage4AdapterTargetSchemaEvolution,
	transfer []schema.Table,
) ([]schema.Table, error) {
	if evolution == nil {
		return cloneTargetSchemaEvolutionTables(transfer), nil
	}
	current := make(
		map[targetSchemaEvolutionTableKey]schema.Table,
		len(evolution.current),
	)
	for _, table := range evolution.current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, duplicate := current[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape projection contains duplicate current table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		current[key] = cloneStage4RichTable(table)
	}
	result := make([]schema.Table, 0, len(transfer))
	for _, table := range transfer {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		authenticated, found := current[key]
		if !found {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape projection is missing current transfer table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		result = append(result, authenticated)
	}
	sortTargetSchemaEvolutionTables(result)
	return result, nil
}

func preflightStage4AdapterDesiredTargetAfterEvolution(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) error {
	if prepared.evolution == nil {
		return nil
	}
	currentTables, err := stage4AdapterCurrentEvolutionTargetTables(
		prepared.evolution,
		prepared.targetTables,
	)
	if err != nil {
		return err
	}
	if err := target.PreflightTables(
		ctx,
		cloneTargetSchemaEvolutionTables(currentTables),
		prepared.mode,
	); err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"preflight desired Stage 4 target tables after schema evolution: %w",
				err,
			),
		)
	}
	if prepared.mode == "upsert" {
		if err := preflightStage4NetworkReplayIsolation(
			ctx,
			target,
			cloneTargetSchemaEvolutionTables(
				currentTables,
			),
		); err != nil {
			return fmt.Errorf(
				"preflight desired Stage 4 network replay isolation after schema evolution: %w",
				err,
			)
		}
	}
	return ctx.Err()
}

func stage4AdapterTargetEvolutionDecision(
	mode string,
	decisions []SchemaContractDecision,
) (bool, SchemaContractDecision) {
	if mode != "upsert" {
		return false, SchemaContractDecision{}
	}
	for _, decision := range decisions {
		switch decision.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
			return true, decision
		}
	}
	return false, SchemaContractDecision{}
}

func stage4AdapterTargetSchemaEvolutionCapabilityIsNil(
	capability adapterTargetSchemaEvolutionCapability,
) bool {
	if capability == nil {
		return true
	}
	value := reflect.ValueOf(capability)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func targetSchemaEvolutionCreatePlannerIsNil(
	planner TargetSchemaEvolutionCreatePlanner,
) bool {
	if planner == nil {
		return true
	}
	value := reflect.ValueOf(planner)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func requireStage4AdapterSeams(
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	gate Stage4SchemaGateResult,
	mode string,
) error {
	if err := requireStage4AdapterConfigurationSeams(cfg); err != nil {
		return err
	}
	if gate.RebuildRequiresTargetCatalog {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 schema rebuild retains prior-only objects but route %s-to-%s has no composed target-catalog rebuild seam",
				source.Engine(),
				target.Engine(),
			),
		)
	}
	// Date-based incremental validation is admitted and executed through its
	// attempt-bound evidence probe. It must not construct the ordinary
	// whole-table validation probe, which would observe a later live source
	// state rather than the immutable transferred window.
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		return nil
	}
	validationMode := cfg.Migration.Validation.Mode
	if validationMode == "" ||
		validationMode == config.ValidationCountOnly {
		return nil
	}
	if stage4ValidationProvider(observer, source, target) == nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q requires a composed adapter validation probe seam",
				validationMode,
			),
		)
	}
	return nil
}

func stage4ValidationProvider(
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
) adapterStage4ValidationProbeProvider {
	for _, candidate := range []any{observer, source, target} {
		if provider, ok := candidate.(adapterStage4ValidationProbeProvider); ok &&
			!isNilInterface(provider) {
			return provider
		}
	}
	return nil
}

func stage4AdapterValidationProbe(
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
	providerSources ...sourceAdapter,
) (ValidationCoreProbe, error) {
	mode := cfg.Migration.Validation.Mode
	providerSource := source
	if len(providerSources) > 1 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 validation accepts at most one provider source",
			),
		)
	}
	if len(providerSources) == 1 {
		providerSource = providerSources[0]
	}
	// Count through the supplied provider, not the pool adapter. A stable
	// network table validates while its source view still holds a pinned
	// connection, and MySQL, MariaDB, and SQL Server cap that pool at one
	// connection, so counting through the pool waits forever for a connection
	// the caller itself is holding. Counting through the stable view is also
	// the more truthful measurement: it counts the same snapshot that was
	// transferred rather than whatever the source looks like afterwards.
	if mode == "" || mode == config.ValidationCountOnly {
		return &stage4AdapterCountProbe{
			source: providerSource,
			target: target,
			plans:  stage4AdapterPlansBySource(plans),
		}, nil
	}
	provider := stage4ValidationProvider(
		observer,
		providerSource,
		target,
	)
	if provider == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q requires a composed adapter validation probe seam",
				mode,
			),
		)
	}
	probe, err := provider.Stage4ValidationProbe(
		source,
		target,
		append([]adapterTablePlan(nil), plans...),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"construct Stage 4 validation probe: %w",
			err,
		)
	}
	if isNilInterface(probe) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation probe provider returned no probe",
			),
		)
	}
	return probe, nil
}

func prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
	validationMode config.ValidationMode,
	targetMode string,
	probe ValidationCoreProbe,
	tables []schema.Table,
) (map[stage4RichTableKey]string, error) {
	if targetMode != "upsert" ||
		(validationMode != config.ValidationNullParity &&
			validationMode != config.ValidationSample) {
		return nil, nil
	}
	provider, ok := probe.(adapterStage4ValidationEqualityProofProvider)
	if !ok || isNilInterface(provider) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q in upsert mode requires a composed route-bound primary-key equality proof seam",
				validationMode,
			),
		)
	}
	proofs := make(
		map[stage4RichTableKey]string,
		len(tables),
	)
	for _, table := range tables {
		key := stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, exists := proofs[key]; exists {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 validation equality proof inventory duplicates table (%q, %q)",
					table.Schema,
					table.Name,
				),
			)
		}
		proof, err := provider.Stage4ValidationPrimaryKeyEqualityProof(
			cloneStage4RichTable(table),
		)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"prepare Stage 4 primary-key equality proof for table (%q, %q): %w",
					table.Schema,
					table.Name,
					err,
				),
			)
		}
		if !validValidationEqualityProofDigest(proof) {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 primary-key equality proof for table (%q, %q) is not a canonical SHA-256 digest",
					table.Schema,
					table.Name,
				),
			)
		}
		proofs[key] = proof
	}
	return proofs, nil
}

func stage4AdapterPlansBySource(
	plans []adapterTablePlan,
) map[stage4RichTableKey]adapterTablePlan {
	result := make(
		map[stage4RichTableKey]adapterTablePlan,
		len(plans),
	)
	for _, plan := range plans {
		result[stage4RichTableKey{
			schema: plan.source.Schema,
			table:  plan.source.Name,
		}] = plan
	}
	return result
}

type stage4AdapterCountProbe struct {
	source     sourceAdapter
	target     targetAdapter
	plans      map[stage4RichTableKey]adapterTablePlan
	sourceGate stage4AdapterProbeGate
	targetGate stage4AdapterProbeGate
}

// stage4AdapterProbeGate serializes operations against one adapter while
// allowing a caller that has already timed out or been canceled to stop
// waiting. Source and target use separate gates so independent engines remain
// concurrent.
type stage4AdapterProbeGate struct {
	once  sync.Once
	token chan struct{}
}

func (gate *stage4AdapterProbeGate) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	gate.once.Do(func() {
		gate.token = make(chan struct{}, 1)
		gate.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate.token:
		if err := ctx.Err(); err != nil {
			gate.release()
			return err
		}
		return nil
	}
}

func (gate *stage4AdapterProbeGate) release() {
	gate.token <- struct{}{}
}

func (probe *stage4AdapterCountProbe) ExactCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plan, err := probe.plan(table)
	if err != nil {
		return 0, err
	}
	var (
		count int
		gate  *stage4AdapterProbeGate
	)
	switch side {
	case ValidationSource:
		gate = &probe.sourceGate
	case ValidationTarget:
		gate = &probe.targetGate
	default:
		return 0, fmt.Errorf("unknown Stage 4 validation side %q", side)
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	switch side {
	case ValidationSource:
		count, err = probe.source.CountRows(ctx, plan.source)
	case ValidationTarget:
		count, err = probe.target.CountRows(ctx, plan.target)
	}
	return int64(count), err
}

func (probe *stage4AdapterCountProbe) EstimateCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plan, err := probe.plan(table)
	if err != nil {
		return 0, err
	}
	var (
		estimator adapterRowCountEstimator
		selected  schema.Table
		gate      *stage4AdapterProbeGate
	)
	switch side {
	case ValidationSource:
		estimator, _ = probe.source.(adapterRowCountEstimator)
		selected = plan.source
		gate = &probe.sourceGate
	case ValidationTarget:
		estimator, _ = probe.target.(adapterRowCountEstimator)
		selected = plan.target
		gate = &probe.targetGate
	default:
		return 0, fmt.Errorf("unknown Stage 4 validation side %q", side)
	}
	if estimator == nil {
		return 0, fmt.Errorf(
			"Stage 4 %s count estimate is unavailable; exact count was not relabeled",
			side,
		)
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	estimate, err := estimator.EstimateRows(ctx, selected)
	if err != nil {
		return 0, err
	}
	if estimate < 0 {
		return 0, fmt.Errorf(
			"Stage 4 %s count estimate is negative",
			side,
		)
	}
	return estimate, nil
}

func (probe *stage4AdapterCountProbe) NullCounts(
	context.Context,
	ValidationSide,
	schema.Table,
	[]string,
	ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	return ValidationNullCountEvidence{}, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement NULL parity",
	)
}

func (probe *stage4AdapterCountProbe) SampleSourceRows(
	context.Context,
	schema.Table,
	[]string,
	int,
) ([]ValidationSampleRow, error) {
	return nil, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement row sampling",
	)
}

func (probe *stage4AdapterCountProbe) SampleTargetRows(
	context.Context,
	schema.Table,
	[]string,
	[]ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	return nil, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement row sampling",
	)
}

func (probe *stage4AdapterCountProbe) plan(
	table schema.Table,
) (adapterTablePlan, error) {
	plan, ok := probe.plans[stage4RichTableKey{
		schema: table.Schema,
		table:  table.Name,
	}]
	if !ok {
		return adapterTablePlan{}, fmt.Errorf(
			"no Stage 4 adapter plan for validation table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	return plan, nil
}

func validateStage4AdapterRun(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		return err
	}
	report, err := RunValidationCore(
		ctx,
		ValidationCoreOptions{
			Mode:                   cfg.Migration.Validation.Mode,
			TargetMode:             prepared.mode,
			FailOnMismatch:         cfg.Migration.Validation.FailOnMismatch,
			FailOnTimeout:          cfg.Migration.Validation.FailOnTimeout,
			FailOnEstimateMismatch: cfg.Migration.Validation.FailOnEstimateMismatch,
			ExactCountTimeout:      30 * time.Second,
			TableTimeout:           2 * time.Minute,
			TableConcurrency:       stage4ValidationConcurrency(len(specs)),
			SampleLimit:            100,
		},
		specs,
		prepared.validation,
	)
	if err != nil {
		return fmt.Errorf("run Stage 4 validation core: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !report.Passed {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"Stage 4 post-finalize validation failed for route %s-to-%s",
				source.Engine(),
				target.Engine(),
			),
		)
	}
	return nil
}

func stage4AdapterValidationTableSpecs(
	prepared stage4AdapterPrepared,
) ([]ValidationTableSpec, error) {
	plans := stage4AdapterPlansBySource(prepared.plans)
	specs := make(
		[]ValidationTableSpec,
		0,
		len(prepared.gate.ValidationTables),
	)
	primaryKeyEqualityProofs := prepared.validationPrimaryKeyEqualityProofs
	if prepared.incremental != nil {
		primaryKeyEqualityProofs =
			prepared.incremental.validationPrimaryKeyEqualityProofs
		if primaryKeyEqualityProofs == nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"prepared Stage 4 incremental primary-key equality proof inventory is unavailable",
				),
			)
		}
	}
	for _, table := range prepared.gate.ValidationTables {
		if _, ok := plans[stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}]; !ok {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 validation projection contains table (%q, %q) outside the transfer plan",
					table.Schema,
					table.Name,
				),
			)
		}
		var primaryKeyEqualityProof string
		if prepared.mode == "upsert" &&
			primaryKeyEqualityProofs != nil {
			primaryKeyEqualityProof =
				primaryKeyEqualityProofs[stage4RichTableKey{
					schema: table.Schema,
					table:  table.Name,
				}]
			if !validValidationEqualityProofDigest(
				primaryKeyEqualityProof,
			) {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 primary-key equality proof for validation table (%q, %q) is missing or invalid",
						table.Schema,
						table.Name,
					),
				)
			}
		}
		var strictSourceRows *int64
		if prepared.strictSourceRows != nil {
			count, ok := prepared.strictSourceRows[stage4RichTableKey{
				schema: table.Schema,
				table:  table.Name,
			}]
			if !ok || count < 0 {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 strict-snapshot count for validation table (%q, %q) is missing or invalid",
						table.Schema,
						table.Name,
					),
				)
			}
			value := count
			strictSourceRows = &value
		}
		reconciliationStrict := false
		if prepared.deletes != nil {
			key := stage4RichTableKey{
				schema: table.Schema,
				table:  table.Name,
			}
			strict, ok := prepared.deleteReconciliationStrict[key]
			if !ok {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 delete-reconciliation outcome for validation table (%q, %q) is missing",
						table.Schema,
						table.Name,
					),
				)
			}
			reconciliationStrict = strict
		}
		specs = append(specs, ValidationTableSpec{
			Table:                   table,
			Projection:              adapterColumnNames(table),
			StrictSourceRows:        strictSourceRows,
			ReconciliationStrict:    reconciliationStrict,
			PrimaryKeyEqualityProof: primaryKeyEqualityProof,
		})
	}
	return specs, nil
}

func stage4ValidationConcurrency(tableCount int) int {
	if tableCount <= 1 {
		return 1
	}
	if tableCount > 8 {
		return 8
	}
	return tableCount
}

func buildStage4AdapterWork(
	configDigest string,
	mode string,
	plans []adapterTablePlan,
) ([]stage4AdapterWork, error) {
	result := make([]stage4AdapterWork, len(plans))
	seen := make(map[state.TaskKey]struct{}, len(plans))
	for index, plan := range plans {
		task := state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: plan.source.Schema,
			Table:  plan.source.Name,
		}
		if _, duplicate := seen[task]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 table work task for (%q, %q)",
					task.Schema,
					task.Table,
				),
			)
		}
		seen[task] = struct{}{}
		sourceSnapshot, err := schema.NewSchemaSnapshot(
			[]schema.Table{plan.source},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize Stage 4 source work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		sourceCanonical, err := sourceSnapshot.CanonicalJSON()
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 source work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		targetSnapshot, err := schema.NewSchemaSnapshot(
			[]schema.Table{plan.target},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize Stage 4 target work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		targetCanonical, err := targetSnapshot.CanonicalJSON()
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 target work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		var sourceIdentityFrontier, targetIdentityFrontier *int64
		if mode != "upsert" {
			sourceIdentityFrontier = stage4IdentityFrontier(plan.source)
			targetIdentityFrontier = stage4IdentityFrontier(plan.target)
		}
		wire := struct {
			Version                int      `json:"version"`
			ConfigDigest           string   `json:"config_digest"`
			Mode                   string   `json:"mode"`
			SourceCanonical        string   `json:"source_canonical"`
			TargetCanonical        string   `json:"target_canonical"`
			SourceIdentityFrontier *int64   `json:"source_identity_frontier"`
			TargetIdentityFrontier *int64   `json:"target_identity_frontier"`
			Projection             []string `json:"projection"`
		}{
			Version:                1,
			ConfigDigest:           configDigest,
			Mode:                   mode,
			SourceCanonical:        string(sourceCanonical),
			TargetCanonical:        string(targetCanonical),
			SourceIdentityFrontier: sourceIdentityFrontier,
			TargetIdentityFrontier: targetIdentityFrontier,
			Projection:             append([]string(nil), plan.columns...),
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			return nil, fmt.Errorf(
				"encode Stage 4 table work topology for %s: %w",
				plan.source.Name,
				err,
			)
		}
		digest := sha256.Sum256(encoded)
		result[index] = stage4AdapterWork{
			task:     task,
			strategy: stage4AdapterCopyStrategy,
			topology: hex.EncodeToString(digest[:]),
		}
	}
	return result, nil
}

func stage4IdentityFrontier(table schema.Table) *int64 {
	if table.Identity == nil || table.Identity.Frontier == nil {
		return nil
	}
	value := *table.Identity.Frontier
	return &value
}

type stage4WorkInventory struct {
	tasks  map[state.TaskKey]state.WorkTask
	ranges map[state.TaskKey][]state.RangeState
}

func loadStage4WorkInventory(
	ctx context.Context,
	run Stage4RunContext,
) (stage4WorkInventory, error) {
	result := stage4WorkInventory{
		tasks:  make(map[state.TaskKey]state.WorkTask),
		ranges: make(map[state.TaskKey][]state.RangeState),
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 work evidence: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for _, task := range tasks {
		if _, duplicate := result.tasks[task.Key]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 work task %#v",
					task.Key,
				),
			)
		}
		result.tasks[task.Key] = task
	}
	for _, workRange := range ranges {
		result.ranges[workRange.Task] = append(
			result.ranges[workRange.Task],
			workRange,
		)
	}
	return result, nil
}

func (inventory stage4WorkInventory) exact(
	key state.TaskKey,
	rangeID string,
	strategy string,
	topology string,
	allowMissing bool,
) (state.WorkTask, state.RangeState, bool, error) {
	task, found := inventory.tasks[key]
	taskRanges := inventory.ranges[key]
	if !found {
		if len(taskRanges) != 0 {
			return state.WorkTask{}, state.RangeState{}, false,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"orphaned Stage 4 work ranges exist for missing task %#v",
						key,
					),
				)
		}
		if allowMissing {
			return state.WorkTask{}, state.RangeState{}, false, nil
		}
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf("missing Stage 4 work task %#v", key),
			)
	}
	if len(taskRanges) != 1 || taskRanges[0].ID != rangeID {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work task %#v has an unsafe range set",
					key,
				),
			)
	}
	workRange := taskRanges[0]
	if task.Strategy != strategy ||
		task.TopologyHash != topology ||
		workRange.Strategy != strategy ||
		workRange.TopologyHash != topology {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work topology changed for task %#v",
					key,
				),
			)
	}
	if err := validateStage4CoarseWorkState(
		task,
		workRange,
	); err != nil {
		return state.WorkTask{}, state.RangeState{}, false,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 work state is unsafe for task %#v: %w",
					key,
					err,
				),
			)
	}
	return task, workRange, true, nil
}

func validateStage4CoarseWorkState(
	task state.WorkTask,
	workRange state.RangeState,
) error {
	switch task.Status {
	case "running", "completed":
	default:
		return fmt.Errorf("task status is %q", task.Status)
	}
	switch workRange.Status {
	case "running", "completed":
	default:
		return fmt.Errorf("range status is %q", workRange.Status)
	}
	if task.Status == "completed" && workRange.Status != "completed" {
		return fmt.Errorf(
			"completed task has non-completed range status %q",
			workRange.Status,
		)
	}
	if task.Attempts != 0 || task.Retries != 0 || task.Error != "" {
		return fmt.Errorf("coarse task contains unexpected retry evidence")
	}
	if len(workRange.Lower) != 0 ||
		len(workRange.Upper) != 0 ||
		workRange.LowerInclusive ||
		workRange.UpperInclusive ||
		workRange.FirstRow != 0 ||
		workRange.LastRow != 0 ||
		len(workRange.Frontier) != 0 ||
		workRange.FrontierValid ||
		workRange.NextSequence != 0 ||
		workRange.SequenceOffset != 0 ||
		workRange.RowsDone != 0 ||
		workRange.RowsTotal != 0 ||
		workRange.CommittedPrefix != 0 ||
		workRange.Attempts != 0 ||
		workRange.Retries != 0 ||
		workRange.Error != "" ||
		len(workRange.Pending) != 0 {
		return fmt.Errorf(
			"coarse range contains unexpected progress or retry evidence",
		)
	}
	return nil
}

func verifyStage4SchemaSentinelEvidence(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
) error {
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		return err
	}
	task, workRange, _, err := inventory.exact(
		gate.Task,
		stage4SchemaGateRangeID,
		stage4SchemaGateStrategy,
		gate.TopologyHash,
		false,
	)
	if err != nil {
		return fmt.Errorf(
			"verify Stage 4 schema sentinel before target planning: %w",
			err,
		)
	}
	snapshot, found, err := run.Backend.LoadSchemaSnapshot(
		run.RunID,
		gate.Task,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"read Stage 4 schema sentinel evidence before target planning: %w",
				err,
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !found {
		if task.Status != "running" ||
			workRange.Status != "running" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 schema sentinel is complete without its prior validated snapshot",
				),
			)
		}
		return nil
	}
	pending := gate.PendingSnapshot
	if snapshot.RunID != pending.RunID ||
		snapshot.Task != pending.Task ||
		snapshot.CanonicalJSON != pending.CanonicalJSON ||
		snapshot.Digest != pending.Digest ||
		!snapshot.CapturedAt.Equal(pending.CapturedAt) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 schema sentinel snapshot changed after policy evaluation",
			),
		)
	}
	return nil
}

func checkpointStage4AdapterWork(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
) error {
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network admission requires a lease-fenced target mutation protector",
			),
		)
	}
	setObserver, err := requireStage4TableSetObserver(observer)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := setObserver.BeforeTables(
		ctx,
		append([]string(nil), prepared.names...),
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint Stage 4 table set: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared.network == nil {
		if len(prepared.work) == 0 {
			return nil
		}
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 network admission is unavailable"),
		)
	}
	if err := prepared.network.ensurePlans(ctx); err != nil {
		return fmt.Errorf(
			"checkpoint Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	if _, err := prepared.network.loadRestores(ctx); err != nil {
		return fmt.Errorf(
			"verify Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	return nil
}

func requireStage4TableSetObserver(
	observer TableObserver,
) (TableSetObserver, error) {
	setObserver, ok := observer.(TableSetObserver)
	if !ok || isNilAdapterResumeObserver(observer) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 composed-adapter migration requires a table-set observer so ordinary checkpoints exist before target preparation",
			),
		)
	}
	return setObserver, nil
}

func ensureStage4AdapterWork(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
) error {
	if len(work) == 0 {
		return ctx.Err()
	}
	coordinator, err := newStage4AdapterNetworkCoordinator(run, work)
	if err != nil {
		return err
	}
	if err := coordinator.ensurePlans(ctx); err != nil {
		return fmt.Errorf(
			"checkpoint Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	_, err = coordinator.loadRestores(ctx)
	if err != nil {
		return fmt.Errorf(
			"verify Stage 4 network ranges before target preparation: %w",
			err,
		)
	}
	return nil
}

func verifyStage4ResumeWorkEvidence(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
	validated map[string]int,
	allowMissingIncomplete bool,
) error {
	inventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		return fmt.Errorf(
			"read Stage 4 table work before resume mutation: %w",
			err,
		)
	}
	expected := make(map[state.TaskKey]struct{}, len(work))
	for _, item := range work {
		expected[item.task] = struct{}{}
	}
	for key := range inventory.tasks {
		if key.Type != stage4AdapterNetworkTaskType &&
			key.Type != "table-copy" &&
			key.Type != "analytical-table-copy" {
			continue
		}
		if _, ok := expected[key]; !ok {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"unexpected stale Stage 4 table work task %#v before resume",
					key,
				),
			)
		}
	}
	for key, ranges := range inventory.ranges {
		for range ranges {
			if key.Type != stage4AdapterNetworkTaskType &&
				key.Type != "table-copy" &&
				key.Type != "analytical-table-copy" {
				continue
			}
			if _, ok := expected[key]; !ok {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"unexpected stale Stage 4 table work range for task %#v before resume",
						key,
					),
				)
			}
		}
	}
	for _, item := range work {
		checkpointRows, checkpointComplete := validated[item.task.Table]
		task, ranges, found, err := exactStage4AdapterWork(
			inventory,
			item,
			allowMissingIncomplete && !checkpointComplete,
		)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if task.Status == "completed" && !checkpointComplete {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 structured work marks table %s complete but its ordinary checkpoint is not reusable",
					item.task.Table,
				),
			)
		}
		if checkpointComplete {
			var structuredRows int64
			for _, workRange := range ranges {
				if workRange.Status != "completed" {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 ordinary checkpoint marks table %s complete but structured range %s is not complete",
							item.task.Table,
							workRange.ID,
						),
					)
				}
				if workRange.RowsDone >
					math.MaxInt64-structuredRows {
					return NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 structured row total overflows for table %s",
							item.task.Table,
						),
					)
				}
				structuredRows += workRange.RowsDone
			}
			if structuredRows != int64(checkpointRows) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 ordinary checkpoint row total differs from completed structured ranges for table %s",
						item.task.Table,
					),
				)
			}
		}
	}
	return nil
}

func completeStage4AdapterWork(
	ctx context.Context,
	run Stage4RunContext,
	work []stage4AdapterWork,
) error {
	for _, item := range work {
		if err := completeStage4AdapterWorkItem(
			ctx,
			run,
			item,
		); err != nil {
			return fmt.Errorf(
				"complete Stage 4 work for %s: %w",
				item.task.Table,
				err,
			)
		}
	}
	return nil
}

func stageStage4SchemaGateSnapshots(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run.Backend.SaveSchemaSnapshot(
		gate.PendingSnapshot,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"save validated Stage 4 schema snapshot before sentinel completion: %w",
				err,
			),
		)
	}
	if evolution != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := run.Backend.SaveSchemaSnapshot(
			evolution.pending,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"save validated Stage 4 target-shape snapshot before schema DDL: %w",
					err,
				),
			)
		}
	}
	return ctx.Err()
}

// completeStage4AdapterTerminalSchemaGateSentinels leaves schema sentinels
// running for every route that has already published immutable aggregate table
// inventory. PublishStage4RunCompletion owns their one terminal timestamp and
// atomically records it with the successful run outcome. A backend without
// aggregate support, or a legacy route that never published inventory, keeps
// the older direct-completion path.
func completeStage4AdapterTerminalSchemaGateSentinels(
	ctx context.Context,
	prepared stage4AdapterPrepared,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	aggregate, ok := prepared.run.Backend.(state.Stage4AggregateBackend)
	if ok && !nilStage4AggregateBackend(aggregate) {
		_, found, err := aggregate.LoadStage4TableInventory(
			prepared.run.RunID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"read Stage 4 table inventory before terminal sentinel completion: %w",
					err,
				),
			)
		}
		if found {
			return nil
		}
	}
	return completeStage4SchemaGateSentinels(
		ctx,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	)
}

func completeStage4SchemaGateSentinels(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if evolution != nil {
		if err := completeStage4WorkTask(
			ctx,
			run,
			evolution.authority.Task(),
			stage4TargetShapeRangeID,
			evolution.authority.TopologyHash(),
		); err != nil {
			return fmt.Errorf(
				"complete validated Stage 4 target-shape sentinel: %w",
				err,
			)
		}
	}
	if err := completeStage4WorkTask(
		ctx,
		run,
		gate.Task,
		stage4SchemaGateRangeID,
		gate.TopologyHash,
	); err != nil {
		return fmt.Errorf(
			"complete validated Stage 4 schema sentinel: %w",
			err,
		)
	}
	return nil
}

func completeStage4WorkTask(
	ctx context.Context,
	run Stage4RunContext,
	task state.TaskKey,
	rangeID string,
	topology string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 work before completion: %w", err),
		)
	}
	var matchedTask *state.WorkTask
	for index := range tasks {
		if tasks[index].Key != task {
			continue
		}
		if matchedTask != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("duplicate Stage 4 work task %#v", task),
			)
		}
		matchedTask = &tasks[index]
	}
	if matchedTask == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("missing Stage 4 work task %#v", task),
		)
	}
	if matchedTask.TopologyHash != topology {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work topology changed for task %#v",
				task,
			),
		)
	}
	var matchedRange *state.RangeState
	for index := range ranges {
		if ranges[index].Task != task ||
			ranges[index].ID != rangeID {
			continue
		}
		if matchedRange != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"duplicate Stage 4 work range %q for task %#v",
					rangeID,
					task,
				),
			)
		}
		matchedRange = &ranges[index]
	}
	if matchedRange == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"missing Stage 4 work range %q for task %#v",
				rangeID,
				task,
			),
		)
	}
	if matchedRange.TopologyHash != topology {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work range topology changed for task %#v",
				task,
			),
		)
	}
	if matchedTask.Status == "completed" {
		if matchedRange.Status != "completed" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 work task has non-completed range status %q",
					matchedRange.Status,
				),
			)
		}
		return nil
	}
	if matchedTask.Status != "running" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work task has unsafe status %q",
				matchedTask.Status,
			),
		)
	}
	now := time.Now().UTC()
	switch matchedRange.Status {
	case "completed":
	case "running":
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := run.Backend.CompleteRange(
			run.RunID,
			task,
			rangeID,
			topology,
			matchedRange.NextSequence,
			now,
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("complete Stage 4 work range: %w", err),
			)
		}
	default:
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 work range %q has unsafe status %q",
				rangeID,
				matchedRange.Status,
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run.Backend.CompleteWorkTask(
		run.RunID,
		task,
		topology,
		now,
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("complete Stage 4 work task: %w", err),
		)
	}
	return nil
}
