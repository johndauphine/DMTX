package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

type stage4PostgresReplayCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// stage4PostgresReplayCatalogRows keeps the catalog validator independent of
// the driver row type. Admission uses database/sql rows while the page writer
// uses pgx rows from the exact transaction holding the DDL fence.
type stage4PostgresReplayCatalogRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

type stage4PostgresReplayCatalogReader interface {
	ReadStage4PostgresRetainedShape(
		context.Context,
		schema.Table,
	) (postgresCatalogTableShape, bool, error)
	QueryStage4PostgresIncomingForeignKeys(
		context.Context,
		string,
		string,
	) (stage4PostgresReplayCatalogRows, error)
}

type stage4PostgresSQLReplayCatalogReader struct {
	queryer stage4PostgresReplayCatalogQueryer
}

func (reader stage4PostgresSQLReplayCatalogReader) ReadStage4PostgresRetainedShape(
	ctx context.Context,
	table schema.Table,
) (postgresCatalogTableShape, bool, error) {
	return readPostgresUpsertCatalogShape(
		ctx,
		reader.queryer,
		table,
	)
}

func (reader stage4PostgresSQLReplayCatalogReader) QueryStage4PostgresIncomingForeignKeys(
	ctx context.Context,
	namespace string,
	table string,
) (stage4PostgresReplayCatalogRows, error) {
	return reader.queryer.QueryContext(
		ctx,
		stage4PostgresIncomingForeignKeysQuery,
		namespace,
		table,
	)
}

const stage4PostgresIncomingForeignKeysQuery = `
	SELECT
		parent_namespace.nspname,
		parent_table.relname,
		foreign_key.conname,
		referenced_namespace.nspname,
		referenced_table.relname,
		foreign_key.confupdtype::text,
		referenced_column.attname
	FROM pg_catalog.pg_constraint AS foreign_key
	JOIN pg_catalog.pg_class AS parent_table
	  ON parent_table.oid = foreign_key.conrelid
	JOIN pg_catalog.pg_namespace AS parent_namespace
	  ON parent_namespace.oid = parent_table.relnamespace
	JOIN pg_catalog.pg_class AS referenced_table
	  ON referenced_table.oid = foreign_key.confrelid
	JOIN pg_catalog.pg_namespace AS referenced_namespace
	  ON referenced_namespace.oid = referenced_table.relnamespace
	JOIN LATERAL unnest(foreign_key.confkey)
	     WITH ORDINALITY AS referenced_key(attnum, position)
	  ON true
	JOIN pg_catalog.pg_attribute AS referenced_column
	  ON referenced_column.attrelid = referenced_table.oid
	 AND referenced_column.attnum = referenced_key.attnum
	WHERE foreign_key.contype = 'f'
	  AND referenced_namespace.nspname = $1
	  AND referenced_table.relname = $2
	ORDER BY
		parent_namespace.nspname,
		parent_table.relname,
		foreign_key.conname,
		referenced_key.position
`

func (adapter *postgresTargetAdapter) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	tables []schema.Table,
) error {
	if adapter == nil || adapter.database == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL target database is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	return preflightStage4PostgresNetworkReplayIsolation(
		ctx,
		adapter.database,
		tables,
	)
}

func preflightStage4PostgresNetworkReplayIsolation(
	ctx context.Context,
	queryer stage4PostgresReplayCatalogQueryer,
	tables []schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL Stage 4 network replay-isolation context is required",
			),
		)
	}
	if queryer == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL target catalog is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	return validateStage4PostgresNetworkReplayIsolation(
		ctx,
		stage4PostgresSQLReplayCatalogReader{queryer: queryer},
		tables,
	)
}

func validateStage4PostgresNetworkReplayIsolation(
	ctx context.Context,
	reader stage4PostgresReplayCatalogReader,
	tables []schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL Stage 4 network replay-isolation context is required",
			),
		)
	}
	if reader == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"PostgreSQL target replay catalog reader is required for Stage 4 network replay-isolation validation",
			),
		)
	}
	profiles, err := stage4NetworkReplayTableProfiles(
		"PostgreSQL",
		tables,
		stage4ExactIdentifier,
	)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table.Schema == "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL network target table %s has an empty schema",
					table.Name,
				),
			)
		}
		profile := profiles[adapterSourceTableKey(
			table.Schema,
			table.Name,
		)]
		actual, exists, err := reader.ReadStage4PostgresRetainedShape(
			ctx,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect PostgreSQL Stage 4 network retained target %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if !exists {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 PostgreSQL network target table %s.%s disappeared during replay proof",
					table.Schema,
					table.Name,
				),
			)
		}
		if err := validatePostgresUpsertCatalogShape(
			table,
			actual,
		); err != nil {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 PostgreSQL network target table %s.%s retained shape changed after admission: %w",
					table.Schema,
					table.Name,
					err,
				),
			)
		}
		rows, err := reader.QueryStage4PostgresIncomingForeignKeys(
			ctx,
			table.Schema,
			table.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect PostgreSQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if rows == nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"PostgreSQL incoming foreign-key catalog returned no row iterator for Stage 4 network table %s.%s",
					table.Schema,
					table.Name,
				),
			)
		}
		for rows.Next() {
			var dependency stage4NetworkIncomingForeignKey
			var actionCode string
			if err := rows.Scan(
				&dependency.parentNamespace,
				&dependency.parentTable,
				&dependency.name,
				&dependency.referencedNamespace,
				&dependency.referencedTable,
				&actionCode,
				&dependency.referencedColumn,
			); err != nil {
				_ = rows.Close()
				return fmt.Errorf(
					"read PostgreSQL incoming foreign key for Stage 4 network table %s.%s: %w",
					table.Schema,
					table.Name,
					err,
				)
			}
			dependency.updateAction, err =
				stage4PostgresForeignKeyUpdateAction(actionCode)
			if err != nil {
				_ = rows.Close()
				return stage4NetworkReplayCatalogShapeError(
					"PostgreSQL",
					profile,
					fmt.Sprintf(
						"incoming foreign key %s: %v",
						dependency.name,
						err,
					),
				)
			}
			if err := validateStage4NetworkIncomingForeignKey(
				"PostgreSQL",
				profile,
				dependency,
				stage4ExactIdentifier,
			); err != nil {
				_ = rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"iterate PostgreSQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf(
				"close PostgreSQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
	}
	return nil
}

func stage4PostgresForeignKeyUpdateAction(value string) (string, error) {
	switch value {
	case "a":
		return "NO ACTION", nil
	case "r":
		return "RESTRICT", nil
	case "c":
		return "CASCADE", nil
	case "n":
		return "SET NULL", nil
	case "d":
		return "SET DEFAULT", nil
	default:
		return "", fmt.Errorf(
			"unexpected PostgreSQL confupdtype %q",
			value,
		)
	}
}
