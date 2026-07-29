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

func preparePostgresTarget(
	ctx context.Context,
	target *sql.DB,
	table schema.Table,
	mode string,
) error {
	if mode == "drop_recreate" {
		drop, err := schema.DropTable(schema.Postgres, table)
		if err != nil {
			return err
		}
		if _, err := target.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf(
				"drop PostgreSQL table %s: %w",
				table.Name,
				err,
			)
		}
	}
	var exists bool
	err := target.QueryRowContext(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		table.Schema,
		table.Name,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf(
			"check PostgreSQL table %s: %w",
			table.Name,
			err,
		)
	}
	if exists {
		return nil
	}
	ddl, err := schema.CreateTable(schema.Postgres, table)
	if err != nil {
		return fmt.Errorf("plan PostgreSQL table %s: %w", table.Name, err)
	}
	if _, err := target.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create PostgreSQL table %s: %w", table.Name, err)
	}
	return nil
}
