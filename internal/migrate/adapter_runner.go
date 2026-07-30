package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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
	mode, err := normalizeAdapterTargetMode(cfg.Migration.TargetMode)
	if err != nil {
		return Result{}, err
	}
	stage4, err := resolveStage4AdapterAdmission(
		cfg,
		observer,
		false,
	)
	if err != nil {
		return Result{}, err
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
	if route.source.engine == "postgres" &&
		route.target.engine == "postgres" {
		if err := requireDistinctLivePostgresDatabases(
			ctx,
			source,
			target,
		); err != nil {
			return Result{}, err
		}
	}
	if route.source.engine == "mysql" && route.target.engine == "mysql" {
		if err := requireDistinctLiveMySQLDatabases(
			ctx,
			source,
			target,
		); err != nil {
			return Result{}, err
		}
	}
	if route.source.engine == "mssql" && route.target.engine == "mssql" {
		if err := requireDistinctLiveSQLServerDatabases(
			ctx,
			source,
			target,
		); err != nil {
			return Result{}, err
		}
	}
	if route.source.engine == "clickhouse" &&
		route.target.engine == "clickhouse" {
		if err := requireDistinctLiveClickHouseDatabases(
			ctx,
			source,
			target,
		); err != nil {
			return Result{}, err
		}
	}
	return migrateWithAdaptersAdmission(
		ctx,
		cfg,
		observer,
		source,
		target,
		mode,
		stage4,
	)
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

// adapterSourceRebuildRowOrderer is the narrow exception to relational
// primary-key ordering for rebuild-only analytical sources. The returned
// column list must be a complete permutation of the source columns. It is row
// ordering metadata only and must not be interpreted as uniqueness.
type adapterSourceRebuildRowOrderer interface {
	RebuildRowOrder(schema.Table) ([]string, error)
}

// adapterSourceRowPreflighter permits a source to reject legacy values whose
// invalid shape cannot be proven from catalog metadata alone. It runs after
// every selected source table has been inspected and before target planning or
// mutation.
type adapterSourceRowPreflighter interface {
	PreflightRows(context.Context, []schema.Table) error
}

// adapterTargetSourceTableOrderer permits a target with constraints active
// during row loading to request a deterministic source-table execution order.
// The runner verifies that the returned slice is an exact metadata-preserving
// permutation before any target planning or mutation.
type adapterTargetSourceTableOrderer interface {
	OrderSourceTables(
		string,
		[]schema.Table,
		string,
	) ([]schema.Table, error)
}

// adapterTargetSourceDataPreflighter permits a target to perform bounded,
// read-only source checks required by its narrower value or lifecycle
// contracts. It runs after all metadata and target-catalog preflight and
// before the first target mutation or checkpoint.
type adapterTargetSourceDataPreflighter interface {
	PreflightSourceData(
		context.Context,
		sourceAdapter,
		[]adapterTablePlan,
		string,
	) error
}

// adapterTargetDestructivePreflighter lets a target enforce backup or
// destructive acknowledgement against its live, in-scope objects. It remains
// read-only and runs after ordinary catalog preflight but before checkpoints
// or target mutation.
type adapterTargetDestructivePreflighter interface {
	PreflightDestructive(
		context.Context,
		[]schema.Table,
		config.Migration,
	) error
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
	stage4, err := resolveStage4AdapterAdmission(
		cfg,
		observer,
		false,
	)
	if err != nil {
		return Result{}, err
	}
	return migrateWithAdaptersAdmission(
		ctx,
		cfg,
		observer,
		source,
		target,
		mode,
		stage4,
	)
}

func migrateWithAdaptersAdmission(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	stage4 stage4AdapterAdmission,
) (Result, error) {
	if stage4.enabled {
		return migrateWithStage4Adapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			mode,
			stage4.run,
		)
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
		if err := requireAdapterSourceRowOrder(
			source,
			sourceTable,
			mode,
		); err != nil {
			return nil, err
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
	if orderer, ok := target.(adapterTargetSourceTableOrderer); ok {
		requested, orderErr := orderer.OrderSourceTables(
			sourceEngine,
			append([]schema.Table(nil), sourceTables...),
			mode,
		)
		if orderErr != nil {
			return nil, fmt.Errorf(
				"order source tables for target %s: %w",
				target.Engine(),
				orderErr,
			)
		}
		sourceTables, err = validateAdapterTargetSourceTableOrder(
			sourceTables,
			requested,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"order source tables for target %s: %w",
				target.Engine(),
				err,
			)
		}
	}
	if preflighter, ok := source.(adapterSourceRowPreflighter); ok {
		if err := preflighter.PreflightRows(
			ctx,
			sourceTables,
		); err != nil {
			return nil, fmt.Errorf(
				"preflight source rows: %w",
				err,
			)
		}
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

func validateAdapterTargetSourceTableOrder(
	original []schema.Table,
	requested []schema.Table,
) ([]schema.Table, error) {
	if len(requested) != len(original) {
		return nil, fmt.Errorf(
			"target returned %d source tables for a %d-table selection",
			len(requested),
			len(original),
		)
	}
	available := make(map[string]schema.Table, len(original))
	for _, table := range original {
		key := adapterSourceTableKey(table.Schema, table.Name)
		if _, duplicate := available[key]; duplicate {
			return nil, fmt.Errorf(
				"source table %s is duplicated before target ordering",
				table.Name,
			)
		}
		available[key] = table
	}
	ordered := make([]schema.Table, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for index, table := range requested {
		key := adapterSourceTableKey(table.Schema, table.Name)
		canonical, exists := available[key]
		if !exists {
			return nil, fmt.Errorf(
				"target ordering returned unknown source table %s",
				table.Name,
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf(
				"target ordering duplicated source table %s",
				table.Name,
			)
		}
		if !reflect.DeepEqual(table, canonical) {
			return nil, fmt.Errorf(
				"target ordering changed source table metadata for %s",
				table.Name,
			)
		}
		seen[key] = struct{}{}
		ordered[index] = canonical
	}
	return ordered, nil
}

func requireAdapterSourceRowOrder(
	source sourceAdapter,
	table schema.Table,
	mode string,
) error {
	if hasPrimaryKey(table) {
		return nil
	}
	orderer, ok := source.(adapterSourceRebuildRowOrderer)
	if !ok || mode != "drop_recreate" {
		return fmt.Errorf(
			"table %s has no primary key; deterministic transfer requires a primary key",
			table.Name,
		)
	}
	order, err := orderer.RebuildRowOrder(table)
	if err != nil {
		return fmt.Errorf(
			"determine rebuild row order for %s: %w",
			table.Name,
			err,
		)
	}
	if len(order) != len(table.Columns) {
		return fmt.Errorf(
			"table %s rebuild row order has %d columns for a %d-column source schema",
			table.Name,
			len(order),
			len(table.Columns),
		)
	}
	columns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return fmt.Errorf(
				"table %s source schema has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := columns[column.Name]; duplicate {
			return fmt.Errorf(
				"table %s source schema has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		columns[column.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		if _, exists := columns[name]; !exists {
			return fmt.Errorf(
				"table %s rebuild row order contains unknown column %s",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf(
				"table %s rebuild row order contains duplicate column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
	}
	return nil
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
