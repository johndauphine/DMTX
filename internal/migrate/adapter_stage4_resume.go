package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
)

// Stage 4 resume: re-entering an interrupted composed run and proving the
// checkpoints it was handed still describe the target.

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
