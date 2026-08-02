package migrate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// Stage 4 run preparation: discovery, target planning, and the schema
// decisions published before any data moves.

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
