package migrate

import (
	"context"
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
	if route.override != nil {
		return Result{}, fmt.Errorf(
			"migration pair %s-to-%s is not a composed adapter route",
			pair.source,
			pair.target,
		)
	}
	return route.execute(ctx, cfg, observer)
}

func migrateWithAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
) (Result, error) {
	names, err := source.ListTables(ctx)
	if err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	result := Result{Validated: true}
	for _, name := range names {
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return Result{}, fmt.Errorf("checkpoint before %s: %w", name, err)
			}
		}
		sourceTable, err := source.InspectTable(ctx, name)
		if err != nil {
			return Result{}, err
		}
		if !hasPrimaryKey(sourceTable) {
			return Result{}, fmt.Errorf(
				"table %s has no primary key; deterministic transfer requires a primary key",
				name,
			)
		}
		columns := adapterColumnNames(sourceTable)
		targetTable, err := target.PrepareTable(
			ctx,
			sourceTable,
			cfg.Migration.TargetMode,
		)
		if err != nil {
			return Result{}, err
		}
		copied, err := copyAdapterRows(
			ctx,
			source,
			target,
			sourceTable,
			targetTable,
			columns,
			cfg.Migration.TargetMode,
		)
		if err != nil {
			return Result{}, err
		}
		if err := validateAdapterCount(
			ctx,
			source,
			target,
			sourceTable,
			targetTable,
		); err != nil {
			return Result{}, err
		}
		if observer != nil {
			if err := observer.AfterTable(ctx, name, copied); err != nil {
				return Result{}, fmt.Errorf("checkpoint after %s: %w", name, err)
			}
		}
		result.Tables++
		result.Rows += copied
	}
	return result, nil
}

func copyAdapterRows(
	ctx context.Context,
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
		receipt, err := target.WriteBatch(
			ctx,
			targetTable,
			columns,
			mode,
			batch,
		)
		if err != nil {
			return err
		}
		if err := receipt.Validate(); err != nil {
			return fmt.Errorf(
				"write %s returned an invalid receipt: %w",
				targetTable.Name,
				err,
			)
		}
		if receipt.Certainty != CommitDurable ||
			receipt.AttemptedRows != int64(len(batch)) {
			return fmt.Errorf(
				"write %s did not durably commit the complete batch",
				targetTable.Name,
			)
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

func cloneAdapterRow(values []any) []any {
	row := make([]any, len(values))
	for index, value := range values {
		if bytes, ok := value.([]byte); ok {
			row[index] = append([]byte(nil), bytes...)
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
) error {
	sourceCount, err := source.CountRows(ctx, sourceTable)
	if err != nil {
		return err
	}
	targetCount, err := target.CountRows(ctx, targetTable)
	if err != nil {
		return err
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
