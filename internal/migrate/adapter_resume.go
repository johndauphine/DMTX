package migrate

import (
	"context"
	"fmt"
	"reflect"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// ExecuteResume resumes a certified composed-adapter migration. A completed
// Stage 4 network table is reused from its exact durable range evidence without
// reopening a later source snapshot, provided the target still contains at
// least the checkpointed rows. Other adapter paths validate completed row
// counts against the current source. A completed strict reconciliation also
// requires exact target equality. Incomplete upsert tables are replayed from
// the beginning so the target adapter's idempotent upsert contract is the only
// row-level recovery primitive required by this first network-resume
// implementation.
//
// Drop/recreate resume remains fail-closed: safely resuming a rebuild requires
// a duplicate-safe table-rebuild replay protocol rather than the upsert replay
// used here.
func ExecuteResume(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	observer TableObserver,
) (Result, error) {
	return executeResumeWithRegistry(
		ctx,
		cfg,
		completed,
		observer,
		builtInAdapters,
	)
}

func executeResumeWithRegistry(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	observer TableObserver,
	registry adapterRegistry,
) (Result, error) {
	if err := checkAdapterResumeContext(ctx, "start"); err != nil {
		return Result{}, err
	}
	mode, err := normalizeAdapterTargetMode(cfg.Migration.TargetMode)
	if err != nil {
		return Result{}, err
	}
	route, err := resolveMigration(cfg, registry)
	if err != nil {
		return Result{}, err
	}
	if mode != "upsert" {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"composed-adapter resume does not support target mode %q; drop/recreate resume requires a duplicate-safe rebuild replay protocol",
				mode,
			),
		)
	}
	if route.override != nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"migration pair %s-to-%s uses a compatibility override and cannot use composed-adapter resume",
				route.source.engine,
				route.target.engine,
			),
		)
	}
	taskObserver, ok := observer.(TableSetObserver)
	if !ok || isNilAdapterResumeObserver(observer) {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"composed-adapter resume requires a table-set observer so every task is checkpointed before target mutation",
			),
		)
	}
	if route.source.open == nil || route.target.open == nil {
		return Result{}, fmt.Errorf(
			"migration pair %s-to-%s has no composable adapter implementation",
			route.source.engine,
			route.target.engine,
		)
	}
	stage4, err := resolveStage4AdapterAdmission(
		cfg,
		observer,
		true,
	)
	if err != nil {
		return Result{}, err
	}

	if err := checkAdapterResumeContext(
		ctx,
		"open source adapter",
	); err != nil {
		return Result{}, err
	}
	source, err := route.source.open(ctx, cfg.Source)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	if err := checkAdapterResumeContext(
		ctx,
		"verify source adapter",
	); err != nil {
		return Result{}, err
	}
	if source.Engine() != route.source.engine {
		return Result{}, fmt.Errorf(
			"source adapter factory for %s returned %s",
			route.source.engine,
			source.Engine(),
		)
	}
	if err := checkAdapterResumeContext(
		ctx,
		"open target adapter",
	); err != nil {
		return Result{}, err
	}
	target, err := route.target.open(ctx, cfg.Target)
	if err != nil {
		return Result{}, err
	}
	defer target.Close()
	if err := checkAdapterResumeContext(
		ctx,
		"verify target adapter",
	); err != nil {
		return Result{}, err
	}
	if target.Engine() != route.target.engine {
		return Result{}, fmt.Errorf(
			"target adapter factory for %s returned %s",
			route.target.engine,
			target.Engine(),
		)
	}
	if err := requireDistinctLiveAdapterDatabases(
		ctx,
		route.source.engine,
		route.target.engine,
		source,
		target,
	); err != nil {
		return Result{}, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"begin discovery",
	); err != nil {
		return Result{}, err
	}

	return resumeWithAdaptersAdmission(
		ctx,
		cfg,
		completed,
		observer,
		taskObserver,
		source,
		target,
		stage4,
	)
}

func requireDistinctLiveAdapterDatabases(
	ctx context.Context,
	sourceEngine string,
	targetEngine string,
	source sourceAdapter,
	target targetAdapter,
) error {
	if sourceEngine != targetEngine {
		return nil
	}
	switch sourceEngine {
	case "postgres":
		return requireDistinctLivePostgresDatabases(ctx, source, target)
	case "mysql":
		return requireDistinctLiveMySQLDatabases(ctx, source, target)
	case "mssql":
		return requireDistinctLiveSQLServerDatabases(ctx, source, target)
	case "clickhouse":
		return requireDistinctLiveClickHouseDatabases(ctx, source, target)
	default:
		return fmt.Errorf(
			"%s-to-%s resume cannot verify distinct live source and target databases",
			sourceEngine,
			targetEngine,
		)
	}
}

func resumeWithAdapters(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	observer TableObserver,
	taskObserver TableSetObserver,
	source sourceAdapter,
	target targetAdapter,
) (Result, error) {
	const mode = "upsert"

	stage4, err := resolveStage4AdapterAdmission(
		cfg,
		observer,
		true,
	)
	if err != nil {
		return Result{}, err
	}
	return resumeWithAdaptersAdmission(
		ctx,
		cfg,
		completed,
		observer,
		taskObserver,
		source,
		target,
		stage4,
	)
}

func resumeWithAdaptersAdmission(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	observer TableObserver,
	taskObserver TableSetObserver,
	source sourceAdapter,
	target targetAdapter,
	stage4 stage4AdapterAdmission,
) (Result, error) {
	const mode = "upsert"

	if stage4.enabled {
		return resumeWithStage4Adapters(
			ctx,
			cfg,
			completed,
			observer,
			taskObserver,
			source,
			target,
			stage4.run,
		)
	}

	if err := checkAdapterResumeContext(
		ctx,
		"list source tables",
	); err != nil {
		return Result{}, err
	}
	names, err := source.ListTables(ctx)
	if err != nil {
		return Result{}, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"select source tables",
	); err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"plan source and target tables",
	); err != nil {
		return Result{}, err
	}
	plans, err := planAdapterTables(ctx, source, target, names, mode)
	if err != nil {
		return Result{}, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"preflight target plan",
	); err != nil {
		return Result{}, err
	}

	names = make([]string, len(plans))
	targetTables := make([]schema.Table, len(plans))
	for index, plan := range plans {
		names[index] = plan.source.Name
		targetTables[index] = plan.target
	}
	if err := preflightAdapterTargetPlan(
		ctx,
		target,
		targetTables,
		mode,
	); err != nil {
		return Result{}, fmt.Errorf("preflight target plan: %w", err)
	}
	if err := checkAdapterResumeContext(
		ctx,
		"preflight target tables",
	); err != nil {
		return Result{}, err
	}
	if err := target.PreflightTables(ctx, targetTables, mode); err != nil {
		return Result{}, fmt.Errorf("preflight target tables: %w", err)
	}
	if err := checkAdapterResumeContext(
		ctx,
		"preflight destructive target action",
	); err != nil {
		return Result{}, err
	}
	if preflighter, ok := target.(adapterTargetDestructivePreflighter); ok {
		if err := preflighter.PreflightDestructive(
			ctx,
			targetTables,
			cfg.Migration,
		); err != nil {
			return Result{}, fmt.Errorf(
				"preflight destructive target action: %w",
				err,
			)
		}
	}
	if err := checkAdapterResumeContext(
		ctx,
		"preflight source data for target",
	); err != nil {
		return Result{}, err
	}
	if preflighter, ok := target.(adapterTargetSourceDataPreflighter); ok {
		if err := preflighter.PreflightSourceData(
			ctx,
			source,
			plans,
			mode,
		); err != nil {
			return Result{}, fmt.Errorf(
				"preflight source data for target: %w",
				err,
			)
		}
	}
	if err := checkAdapterResumeContext(
		ctx,
		"validate completed table checkpoints",
	); err != nil {
		return Result{}, err
	}

	validated, err := validateCompletedAdapterTableCheckpoints(
		ctx,
		source,
		target,
		plans,
		completed,
		cfg.Migration.Deletes.Mode == config.DeleteModeReconcile,
	)
	if err != nil {
		return Result{}, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"checkpoint table set",
	); err != nil {
		return Result{}, err
	}
	if err := taskObserver.BeforeTables(
		ctx,
		append([]string(nil), names...),
	); err != nil {
		return Result{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint table set: %w", err),
		)
	}
	if err := checkAdapterResumeContext(
		ctx,
		"prepare incomplete table set",
	); err != nil {
		return Result{}, err
	}

	result := resultForValidatedAdapterCheckpoints(validated)
	incompletePlans := make([]adapterTablePlan, 0, len(plans))
	incompleteTargets := make([]schema.Table, 0, len(plans))
	for _, plan := range plans {
		if _, complete := validated[plan.source.Name]; complete {
			continue
		}
		incompletePlans = append(incompletePlans, plan)
		incompleteTargets = append(incompleteTargets, plan.target)
	}
	if len(incompletePlans) == 0 {
		if err := checkAdapterResumeContext(
			ctx,
			"report completed resume",
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}

	prepareObserver := adapterResumeMutationGuard{
		ctx:      ctx,
		delegate: observer,
		boundary: "prepare incomplete resume tables",
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		prepareObserver,
		"prepare incomplete resume tables",
		func() error {
			return target.PrepareTables(ctx, incompleteTargets, mode)
		},
	); err != nil {
		return result, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"transfer incomplete resume tables",
	); err != nil {
		return result, err
	}

	copiedRows := make([]int, len(incompletePlans))
	for index, plan := range incompletePlans {
		name := plan.source.Name
		if err := checkAdapterResumeContext(
			ctx,
			"checkpoint before "+name,
		); err != nil {
			return result, err
		}
		if err := observer.BeforeTable(ctx, name); err != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"checkpoint before %s: %w",
					name,
					err,
				),
			)
		}
		if err := checkAdapterResumeContext(
			ctx,
			"replay table "+name,
		); err != nil {
			return result, err
		}
		writeObserver := adapterResumeMutationGuard{
			ctx:      ctx,
			delegate: observer,
			boundary: "write resumed table " + name,
		}
		copied, err := copyAdapterRows(
			ctx,
			writeObserver,
			source,
			target,
			plan.source,
			plan.target,
			plan.columns,
			mode,
		)
		if err != nil {
			return result, err
		}
		if err := checkAdapterResumeContext(
			ctx,
			"validate resumed table "+name,
		); err != nil {
			return result, err
		}
		if err := validateAdapterCount(
			ctx,
			source,
			target,
			plan.source,
			plan.target,
			mode,
		); err != nil {
			return result, err
		}
		if err := checkAdapterResumeContext(
			ctx,
			"finish resumed table "+name,
		); err != nil {
			return result, err
		}
		copiedRows[index] = copied
	}

	if err := checkAdapterResumeContext(
		ctx,
		"finalize incomplete resume tables",
	); err != nil {
		return result, err
	}
	finalizeObserver := adapterResumeMutationGuard{
		ctx:      ctx,
		delegate: observer,
		boundary: "finalize incomplete resume tables",
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		finalizeObserver,
		"finalize incomplete resume tables",
		func() error {
			return target.FinalizeTables(ctx, incompleteTargets, mode)
		},
	); err != nil {
		return result, err
	}
	if err := checkAdapterResumeContext(
		ctx,
		"checkpoint completed resume tables",
	); err != nil {
		return result, err
	}

	for index, plan := range incompletePlans {
		copied := copiedRows[index]
		if err := checkAdapterResumeContext(
			ctx,
			"checkpoint after "+plan.source.Name,
		); err != nil {
			return result, err
		}
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
		result.Tables++
		result.Rows += copied
		if err := checkAdapterResumeContext(
			ctx,
			"continue table completion",
		); err != nil {
			return result, err
		}
	}
	if err := checkAdapterResumeContext(
		ctx,
		"report successful resume",
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

func validateCompletedAdapterTableCheckpoints(
	ctx context.Context,
	source sourceAdapter,
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
					"selected adapter plan contains duplicate table %s",
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
			"count completed source table "+plan.source.Name,
		); err != nil {
			return nil, err
		}
		sourceRows, err := source.CountRows(ctx, plan.source)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"validate completed checkpoint for %s against source: %w",
					plan.source.Name,
					err,
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
		if err := checkAdapterResumeContext(
			ctx,
			"accept completed checkpoint for "+plan.source.Name,
		); err != nil {
			return nil, err
		}
		sourceChanged := checkpoint.Rows != sourceRows
		targetChanged := targetRows < sourceRows ||
			reconciliationStrict && targetRows != sourceRows
		if sourceChanged || targetChanged {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed checkpoint for %s is not reusable: checkpoint has %d rows, source has %d rows, target has %d rows",
					plan.source.Name,
					checkpoint.Rows,
					sourceRows,
					targetRows,
				),
			)
		}
		validated[plan.source.Name] = checkpoint.Rows
	}
	return validated, nil
}

type adapterResumeMutationGuard struct {
	ctx      context.Context
	delegate TableObserver
	boundary string
}

func (guard adapterResumeMutationGuard) BeforeTable(
	ctx context.Context,
	table string,
) error {
	return guard.delegate.BeforeTable(ctx, table)
}

func (guard adapterResumeMutationGuard) AfterTable(
	ctx context.Context,
	table string,
	rows int,
) error {
	return guard.delegate.AfterTable(ctx, table, rows)
}

func (guard adapterResumeMutationGuard) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	guarded := func() error {
		if err := checkAdapterResumeContext(
			guard.ctx,
			guard.boundary,
		); err != nil {
			return err
		}
		if err := mutation(); err != nil {
			return err
		}
		return checkAdapterResumeContext(
			guard.ctx,
			"complete "+guard.boundary,
		)
	}
	if protector, ok := guard.delegate.(adapterTargetMutationProtector); ok {
		if err := checkAdapterResumeContext(
			guard.ctx,
			guard.boundary,
		); err != nil {
			return err
		}
		return protector.ProtectTargetMutation(ctx, guarded)
	}
	return guarded()
}

func checkAdapterResumeContext(
	ctx context.Context,
	boundary string,
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"composed-adapter resume canceled at %s: %w",
			boundary,
			err,
		)
	}
	return nil
}

func isNilAdapterResumeObserver(observer TableObserver) bool {
	if observer == nil {
		return true
	}
	value := reflect.ValueOf(observer)
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

func resultForValidatedAdapterCheckpoints(
	validated map[string]int,
) Result {
	result := Result{}
	for _, rows := range validated {
		result.Tables++
		result.Rows += rows
	}
	return result
}
