package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func (route resolvedAdapterRoute) execute(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if route.override != nil {
		return route.override(ctx, cfg, observer)
	}
	if route.source.open == nil || route.target.open == nil {
		return Result{}, fmt.Errorf(
			"migration pair %s-to-%s has no composable adapter implementation",
			route.source.engine,
			route.target.engine,
		)
	}
	source, err := route.source.open(ctx, cfg.Source)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	if source.Engine() != route.source.engine {
		return Result{}, fmt.Errorf(
			"source adapter factory for %s returned %s",
			route.source.engine,
			source.Engine(),
		)
	}
	target, err := route.target.open(ctx, cfg.Target)
	if err != nil {
		return Result{}, err
	}
	defer target.Close()
	if target.Engine() != route.target.engine {
		return Result{}, fmt.Errorf(
			"target adapter factory for %s returned %s",
			route.target.engine,
			target.Engine(),
		)
	}
	return migrateWithAdapters(ctx, cfg, observer, source, target)
}

func executeBuiltInComposedRoute(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	pair adapterPair,
) (Result, error) {
	route, err := resolveMigration(cfg, builtInAdapters)
	if err != nil {
		return Result{}, err
	}
	resolvedPair := adapterPair{
		source: route.source.engine,
		target: route.target.engine,
	}
	if resolvedPair != pair {
		return Result{}, fmt.Errorf(
			"resolved migration pair %s-to-%s does not match requested pair %s-to-%s",
			resolvedPair.source,
			resolvedPair.target,
			pair.source,
			pair.target,
		)
	}
	if route.override != nil {
		return Result{}, fmt.Errorf(
			"migration pair %s-to-%s is not a composed adapter route",
			pair.source,
			pair.target,
		)
	}
	return route.execute(ctx, cfg, observer)
}

type adapterTablePlan struct {
	source  schema.Table
	target  schema.Table
	columns []string
}

type adapterTargetMutationProtector interface {
	ProtectTargetMutation(context.Context, func() error) error
}

func protectAdapterTargetMutation(
	ctx context.Context,
	observer TableObserver,
	mutation func() error,
) error {
	if protector, ok := observer.(adapterTargetMutationProtector); ok {
		return protector.ProtectTargetMutation(ctx, mutation)
	}
	return mutation()
}

func protectAdapterTargetMutationOnce(
	ctx context.Context,
	observer TableObserver,
	operation string,
	mutation func() error,
) (int, error) {
	calls := 0
	err := protectAdapterTargetMutation(
		ctx,
		observer,
		func() error {
			calls++
			if calls != 1 {
				return fmt.Errorf(
					"target mutation protector invoked %s multiple times",
					operation,
				)
			}
			return mutation()
		},
	)
	if calls == 1 {
		return calls, err
	}
	violation := fmt.Errorf(
		"target mutation protector invoked %s %d times; expected exactly once",
		operation,
		calls,
	)
	if err != nil {
		violation = errors.Join(violation, err)
	}
	return calls, NewTransferError(
		ErrorClassState,
		fmt.Errorf("protect target mutation: %w", violation),
	)
}

func migrateWithAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
) (Result, error) {
	mode, err := normalizeAdapterTargetMode(cfg.Migration.TargetMode)
	if err != nil {
		return Result{}, err
	}
	names, err := source.ListTables(ctx)
	if err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	plans, err := planAdapterTables(ctx, source, target, names, mode)
	if err != nil {
		return Result{}, err
	}
	names = make([]string, len(plans))
	for index, plan := range plans {
		names[index] = plan.source.Name
	}
	targetTables := make([]schema.Table, len(plans))
	for index, plan := range plans {
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
	if err := target.PreflightTables(ctx, targetTables, mode); err != nil {
		return Result{}, fmt.Errorf("preflight target tables: %w", err)
	}
	if setObserver, ok := observer.(TableSetObserver); ok {
		if err := setObserver.BeforeTables(
			ctx,
			append([]string(nil), names...),
		); err != nil {
			return Result{}, fmt.Errorf("checkpoint table set: %w", err)
		}
	}

	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare tables",
		func() error {
			return target.PrepareTables(ctx, targetTables, mode)
		},
	); err != nil {
		return Result{}, err
	}

	copiedRows := make([]int, len(plans))
	for index, plan := range plans {
		name := plan.source.Name
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return Result{}, fmt.Errorf(
					"checkpoint before %s: %w",
					name,
					err,
				)
			}
		}
		copied, err := copyAdapterRows(
			ctx,
			observer,
			source,
			target,
			plan.source,
			plan.target,
			plan.columns,
			mode,
		)
		if err != nil {
			return Result{}, err
		}
		if err := validateAdapterCount(
			ctx,
			source,
			target,
			plan.source,
			plan.target,
			mode,
		); err != nil {
			return Result{}, err
		}
		copiedRows[index] = copied
	}

	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"finalize tables",
		func() error {
			return target.FinalizeTables(ctx, targetTables, mode)
		},
	); err != nil {
		return Result{}, err
	}

	result := Result{}
	for index, plan := range plans {
		name := plan.source.Name
		copied := copiedRows[index]
		if observer != nil {
			if err := observer.AfterTable(ctx, name, copied); err != nil {
				return result, fmt.Errorf(
					"checkpoint after %s: %w",
					name,
					err,
				)
			}
		}
		result.Tables++
		result.Rows += copied
	}
	result.Validated = true
	return result, nil
}

func normalizeAdapterTargetMode(mode string) (string, error) {
	if mode == "" {
		return "drop_recreate", nil
	}
	if mode != "drop_recreate" && mode != "upsert" {
		return "", fmt.Errorf("unsupported target mode %q", mode)
	}
	return mode, nil
}

func planAdapterTables(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	names []string,
	mode string,
) ([]adapterTablePlan, error) {
	sourceEngine := source.Engine()
	if sourceEngine == "" || target.Engine() == "" {
		return nil, fmt.Errorf("source and target adapter engines are required")
	}
	sourceTables := make([]schema.Table, 0, len(names))
	for _, name := range names {
		sourceTable, err := source.InspectTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if sourceTable.Name != name {
			return nil, fmt.Errorf(
				"source adapter %s inspected table %q as %q",
				sourceEngine,
				name,
				sourceTable.Name,
			)
		}
		if !hasPrimaryKey(sourceTable) {
			return nil, fmt.Errorf(
				"table %s has no primary key; deterministic transfer requires a primary key",
				name,
			)
		}
		sourceTables = append(sourceTables, sourceTable)
	}
	sourceTables, err := orderAdapterSourceTablesForMode(
		sourceTables,
		mode,
	)
	if err != nil {
		return nil, err
	}
	targetTables, err := target.PlanTables(sourceEngine, sourceTables, mode)
	if err != nil {
		return nil, err
	}
	if len(targetTables) != len(sourceTables) {
		return nil, fmt.Errorf(
			"target adapter %s planned %d tables for %d source tables",
			target.Engine(),
			len(targetTables),
			len(sourceTables),
		)
	}
	plans := make([]adapterTablePlan, 0, len(sourceTables))
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		if targetTable.Name != sourceTable.Name {
			return nil, fmt.Errorf(
				"target adapter %s changed table name %s to %s",
				target.Engine(),
				sourceTable.Name,
				targetTable.Name,
			)
		}
		plans = append(plans, adapterTablePlan{
			source:  sourceTable,
			target:  targetTable,
			columns: adapterColumnNames(sourceTable),
		})
	}
	return plans, nil
}

func copyAdapterRows(
	ctx context.Context,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
	columns []string,
	mode string,
) (int, error) {
	rows, err := source.OpenRows(ctx, sourceTable, columns)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	batch := make([][]any, 0, sqliteWriteBatchSize)
	copied := 0
	flush := func() error {
		receipt, err := writeAdapterBatch(
			ctx,
			observer,
			target,
			targetTable,
			columns,
			mode,
			batch,
		)
		if err != nil {
			return err
		}
		copied += int(receipt.CommittedRows)
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, fmt.Errorf(
				"read %s table %s: %w",
				source.DisplayName(),
				sourceTable.Name,
				err,
			)
		}
		batch = append(batch, cloneAdapterRow(values))
		if len(batch) == sqliteWriteBatchSize {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf(
			"read %s table %s: %w",
			source.DisplayName(),
			sourceTable.Name,
			err,
		)
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return 0, err
		}
	}
	return copied, nil
}

func writeAdapterBatch(
	ctx context.Context,
	observer TableObserver,
	target targetAdapter,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	attempted := int64(len(rows))
	receipt := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: attempted,
	}
	mutationCalls, writeErr := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"write table "+table.Name,
		func() error {
			var err error
			receipt, err = target.WriteBatch(
				ctx,
				table,
				columns,
				mode,
				rows,
			)
			return err
		},
	)
	if mutationCalls != 1 {
		return receipt, writeErr
	}
	if receiptErr := receipt.Validate(); receiptErr != nil {
		cause := error(receiptErr)
		if writeErr != nil {
			cause = errors.Join(receiptErr, writeErr)
		}
		return receipt, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"write %s returned an invalid receipt: %w",
				table.Name,
				cause,
			),
		)
	}
	if writeErr != nil {
		switch receipt.Certainty {
		case CommitNotCommitted:
			return receipt, writeErr
		case CommitUnknown:
			return receipt, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"write %s commit outcome is unknown; refusing checkpoint: %w",
					table.Name,
					writeErr,
				),
			)
		default:
			return receipt, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"write %s failed after reporting commit certainty %s; refusing checkpoint: %w",
					table.Name,
					receipt.Certainty,
					writeErr,
				),
			)
		}
	}
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptOffset != 0 ||
		receipt.AttemptedRows != attempted ||
		receipt.CommittedRows != attempted {
		return receipt, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"write %s did not durably commit the complete batch; refusing checkpoint",
				table.Name,
			),
		)
	}
	return receipt, nil
}

func cloneAdapterRow(values []any) []any {
	row := make([]any, len(values))
	for index, value := range values {
		if bytes, ok := value.([]byte); ok {
			owned := make([]byte, len(bytes))
			copy(owned, bytes)
			row[index] = owned
			continue
		}
		row[index] = value
	}
	return row
}

func adapterColumnNames(table schema.Table) []string {
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		columns[index] = column.Name
	}
	return columns
}

func validateAdapterCount(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
	mode string,
) error {
	sourceCount, err := source.CountRows(ctx, sourceTable)
	if err != nil {
		return err
	}
	targetCount, err := target.CountRows(ctx, targetTable)
	if err != nil {
		return err
	}
	if mode == "upsert" {
		if targetCount < sourceCount {
			return fmt.Errorf(
				"validation failed for %s: source has %d rows, target has only %d after upsert",
				sourceTable.Name,
				sourceCount,
				targetCount,
			)
		}
		return nil
	}
	if sourceCount != targetCount {
		return fmt.Errorf(
			"validation failed for %s: source has %d rows, target has %d",
			sourceTable.Name,
			sourceCount,
			targetCount,
		)
	}
	return nil
}
