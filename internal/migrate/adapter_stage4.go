package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	if cfg.Migration.RuntimeTuning {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 runtime tuning requires a composed adapter chunk-boundary tuning seam",
			),
		)
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 date-based incremental migration requires a composed adapter incremental-window seam",
			),
		)
	}
	if cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 delete reconciliation requires a composed adapter delete seam",
			),
		)
	}
	if cfg.Migration.StrictConsistency {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency requires a composed adapter snapshot seam",
			),
		)
	}
	if cfg.Migration.Validation.Mode == config.ValidationFull {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 validation mode full is unsupported"),
		)
	}
	return nil
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

type stage4AdapterPrepared struct {
	run           Stage4RunContext
	gate          Stage4SchemaGateResult
	configDigest  string
	mode          string
	plans         []adapterTablePlan
	names         []string
	targetTables  []schema.Table
	validation    ValidationCoreProbe
	sourceCatalog map[stage4RichTableKey]schema.Table
	work          []stage4AdapterWork
	network       *networkStateCoordinator
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
	var networkExecution *stage4AdapterNetworkExecution
	if mode == "upsert" {
		networkExecution, err = admitStage4AdapterNetworkTransfer(
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
	}
	if networkExecution != nil {
		if err := checkpointStage4AdapterTableSet(
			ctx,
			observer,
			prepared.names,
		); err != nil {
			return Result{}, err
		}
		result, err := runStage4AdapterStableNetworkTables(
			ctx,
			cfg,
			observer,
			target,
			prepared,
			networkExecution,
			false,
			nil,
		)
		if err != nil {
			return result, err
		}
		if err := publishStage4SchemaGate(
			ctx,
			prepared.run,
			prepared.gate,
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
	if networkExecution != nil {
		if err := bindStage4AdapterNetworkRestoresAndValidate(
			ctx,
			observer,
			networkExecution,
		); err != nil {
			return Result{}, err
		}
	}

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
		copiedRows, err = runStage4AdapterNetworkTransfer(
			ctx,
			observer,
			networkExecution,
		)
		if err != nil {
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
		return Result{}, err
	}
	if err := validateStage4AdapterRun(
		ctx,
		cfg,
		source,
		target,
		prepared,
	); err != nil {
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
	if err := publishStage4SchemaGate(
		ctx,
		prepared.run,
		prepared.gate,
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
		execution.source,
		execution.parent.source,
		target,
		prepared,
		planIndex,
	); err != nil {
		return 0, err
	}
	if err := completeStage4AdapterWorkItem(
		ctx,
		prepared.run,
		execution.work,
	); err != nil {
		return 0, fmt.Errorf(
			"complete Stage 4 work for %s: %w",
			name,
			err,
		)
	}
	if err := observer.AfterTable(ctx, name, copied); err != nil {
		return 0, NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint after %s: %w", name, err),
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
	run Stage4RunContext,
) (Result, error) {
	const mode = "upsert"

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
	validated, err := validateCompletedStage4NetworkTableCheckpoints(
		ctx,
		target,
		prepared.plans,
		completed,
		cfg.Migration.Deletes.Mode == config.DeleteModeReconcile,
	)
	if err != nil {
		return Result{}, err
	}
	// Static route, target, dependency, replay, and resource admission precedes
	// BeforeTables and every per-table durable reset/ensure operation.
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
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := networkExecution.prevalidateCompletedTables(
		ctx,
		validated,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	if err := checkpointStage4AdapterTableSet(
		ctx,
		observer,
		prepared.names,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(validated), err
	}
	result, err := runStage4AdapterStableNetworkTables(
		ctx,
		cfg,
		observer,
		target,
		prepared,
		networkExecution,
		true,
		validated,
	)
	if err != nil {
		return result, err
	}
	if err := publishStage4SchemaGate(
		ctx,
		prepared.run,
		prepared.gate,
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
	gate, err := PrepareStage4SchemaGate(
		run,
		discovered,
		Stage4SchemaGateOptions{
			SourceEngine:       source.Engine(),
			TargetEngine:       target.Engine(),
			TargetMode:         mode,
			IncludeTables:      cfg.Migration.IncludeTables,
			ExcludeTables:      cfg.Migration.ExcludeTables,
			ConfigIdentity:     configDigest,
			Contract:           cfg.Migration.SchemaContract,
			FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
			DateUpdatedColumns: cfg.Migration.DateUpdatedColumns,
		},
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
	if err := target.PreflightTables(
		ctx,
		result.targetTables,
		mode,
	); err != nil {
		return result, fmt.Errorf("preflight Stage 4 target tables: %w", err)
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
	result.work, err = buildStage4AdapterWork(
		configDigest,
		mode,
		plans,
	)
	if err != nil {
		return result, err
	}
	if mode == "upsert" &&
		stage4AdapterNetworkRelationalEngine(source.Engine()) {
		// Relational network pagination, retained width, and durable ranges are
		// intentionally deferred until the runner owns one table-scoped stable
		// source view. Global preparation remains read-only and connection
		// bounded.
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
	if mode == "upsert" {
		for _, decision := range gate.Plan.Decisions {
			switch decision.Action {
			case SchemaContractCreateTable,
				SchemaContractAddColumn,
				SchemaContractRelaxNullability,
				SchemaContractWidenType:
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 upsert schema action %q for table %s requires a composed target-catalog evolution executor seam",
						decision.Action,
						decision.Object.Table,
					),
				)
			}
		}
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
	if mode == "upsert" {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q in upsert mode requires a composed route-bound primary-key equality proof seam",
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
		if provider, ok := candidate.(adapterStage4ValidationProbeProvider); ok {
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
	if mode == "" || mode == config.ValidationCountOnly {
		return &stage4AdapterCountProbe{
			source: source,
			target: target,
			plans:  stage4AdapterPlansBySource(plans),
		}, nil
	}
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
	if probe == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation probe provider returned no probe",
			),
		)
	}
	return probe, nil
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
	plans := stage4AdapterPlansBySource(prepared.plans)
	specs := make(
		[]ValidationTableSpec,
		0,
		len(prepared.gate.ValidationTables),
	)
	for _, table := range prepared.gate.ValidationTables {
		if _, ok := plans[stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}]; !ok {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 validation projection contains table (%q, %q) outside the transfer plan",
					table.Schema,
					table.Name,
				),
			)
		}
		specs = append(specs, ValidationTableSpec{
			Table:      table,
			Projection: adapterColumnNames(table),
		})
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

func publishStage4SchemaGate(
	ctx context.Context,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
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
	if err := ctx.Err(); err != nil {
		return err
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
