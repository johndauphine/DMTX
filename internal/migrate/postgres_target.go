package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// SQLiteToPostgresWithObserver migrates SQLite tables into a PostgreSQL target
// through the shared source/target adapter runner.
func SQLiteToPostgresWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf(
			"SQLite-to-PostgreSQL requires source.type sqlite and target.type postgres",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "sqlite", target: "postgres"},
	)
}

func preparePostgresTargets(
	ctx context.Context,
	target *sql.DB,
	tables []schema.Table,
) error {
	drop, err := schema.DropTables(schema.Postgres, tables)
	if err != nil {
		return fmt.Errorf("plan PostgreSQL target table set: %w", err)
	}
	creates := make([]string, len(tables))
	for index, table := range tables {
		creates[index], err = schema.CreateTable(schema.Postgres, table)
		if err != nil {
			return fmt.Errorf(
				"plan PostgreSQL table %s: %w",
				table.Name,
				err,
			)
		}
	}

	transaction, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL target preparation: %w", err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()
	if _, err := transaction.ExecContext(ctx, drop); err != nil {
		return fmt.Errorf("drop PostgreSQL target table set: %w", err)
	}
	for index, statement := range creates {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf(
				"create PostgreSQL table %s: %w",
				tables[index].Name,
				err,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf(
			"commit PostgreSQL target preparation: %w",
			err,
		)
	}
	return nil
}

func finalizePostgresTargets(
	ctx context.Context,
	target *sql.DB,
	tables []schema.Table,
	mode string,
) error {
	var objectPlan []schema.PostgresObjectStatement
	if mode == "drop_recreate" {
		var err error
		objectPlan, err = schema.PlanPostgresDropRecreateObjects(
			tables,
			schema.PostgresObjectPlanOptions{},
		)
		if err != nil {
			return fmt.Errorf(
				"plan PostgreSQL post-load objects: %w",
				err,
			)
		}
	}
	transaction, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL target finalization: %w", err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()
	for _, statement := range objectPlan {
		if _, err := transaction.ExecContext(
			ctx,
			statement.SQL(),
		); err != nil {
			return fmt.Errorf(
				"create PostgreSQL post-load object %s on table %s: %w",
				statement.Name(),
				statement.Table(),
				err,
			)
		}
	}
	if err := finalizePostgresIdentitySequences(
		ctx,
		transaction,
		tables,
	); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf(
			"commit PostgreSQL target finalization: %w",
			err,
		)
	}
	return nil
}
