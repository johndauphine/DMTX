package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

func (adapter *sqliteTargetAdapter) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	tables []schema.Table,
) error {
	if adapter == nil || adapter.database == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite target database is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	return preflightStage4SQLiteNetworkReplayIsolation(
		ctx,
		adapter.database,
		tables,
	)
}

func preflightStage4SQLiteNetworkReplayIsolation(
	ctx context.Context,
	queryer sqliteTargetCatalogQuerier,
	tables []schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite Stage 4 network replay-isolation context is required",
			),
		)
	}
	if queryer == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQLite target catalog is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	for _, table := range tables {
		if table.Schema != "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 SQLite network target table %s has unexpected schema %q",
					table.Name,
					table.Schema,
				),
			)
		}
	}
	profiles, err := stage4NetworkReplayTableProfiles(
		"SQLite",
		tables,
		stage4SQLiteIdentifier,
	)
	if err != nil {
		return err
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT name
		   FROM sqlite_schema
		  WHERE type = 'table'
		    AND substr(lower(name), 1, 7) <> 'sqlite_'
		  ORDER BY name COLLATE NOCASE, name`,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQLite tables for Stage 4 network replay isolation: %w",
			err,
		)
	}
	liveTables := make([]string, 0)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"read SQLite table for Stage 4 network replay isolation: %w",
				err,
			)
		}
		if table == "" {
			_ = rows.Close()
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 SQLite network replay catalog contains an empty table name",
				),
			)
		}
		liveTables = append(liveTables, table)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf(
			"iterate SQLite tables for Stage 4 network replay isolation: %w",
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf(
			"close SQLite tables for Stage 4 network replay isolation: %w",
			err,
		)
	}

	for _, parentTable := range liveTables {
		if err := preflightStage4SQLiteIncomingForeignKeys(
			ctx,
			queryer,
			parentTable,
			profiles,
		); err != nil {
			return err
		}
	}
	return nil
}

// validateStage4SQLiteRetainedReplayTarget re-proves the retained-table
// contract while the page's BEGIN IMMEDIATE reservation is active. The
// ordinary adapter preflight runs before any pages are issued; repeating the
// proof here prevents a trigger or shape change committed between pages from
// turning a replayable upsert into an externally visible side effect.
func validateStage4SQLiteRetainedReplayTarget(
	ctx context.Context,
	queryer sqliteTargetCatalogQuerier,
	table schema.Table,
) error {
	if err := rejectSQLiteTableTriggers(
		ctx,
		queryer,
		table.Name,
	); err != nil {
		return fmt.Errorf(
			"Stage 4 SQLite network target %s: %w",
			table.Name,
			err,
		)
	}
	discovered, _, err := inspectSQLiteSchema(
		ctx,
		queryer,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect Stage 4 SQLite network target %s retained shape: %w",
			table.Name,
			err,
		)
	}
	if err := validateSQLiteRetainedTable(
		table,
		discovered,
	); err != nil {
		return fmt.Errorf(
			"validate Stage 4 SQLite network target %s retained shape: %w",
			table.Name,
			err,
		)
	}
	return nil
}

func preflightStage4SQLiteIncomingForeignKeys(
	ctx context.Context,
	queryer sqliteTargetCatalogQuerier,
	parentTable string,
	profiles map[string]stage4NetworkReplayTableProfile,
) error {
	rows, err := queryer.QueryContext(
		ctx,
		"PRAGMA foreign_key_list("+quote(parentTable)+")",
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQLite incoming foreign keys from dependent table %s for Stage 4 network replay isolation: %w",
			parentTable,
			err,
		)
	}
	for rows.Next() {
		var (
			id, sequence             int
			referencedTable          string
			localColumn              string
			referencedColumn         sql.NullString
			updateAction, deleteRule string
			match                    string
		)
		if err := rows.Scan(
			&id,
			&sequence,
			&referencedTable,
			&localColumn,
			&referencedColumn,
			&updateAction,
			&deleteRule,
			&match,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"read SQLite incoming foreign key from dependent table %s for Stage 4 network replay isolation: %w",
				parentTable,
				err,
			)
		}
		referencedKey := adapterSourceTableKey(
			"",
			stage4SQLiteIdentifier(referencedTable),
		)
		profile, selected := profiles[referencedKey]
		if !selected {
			continue
		}
		dependency := stage4NetworkIncomingForeignKey{
			parentTable:         parentTable,
			name:                fmt.Sprintf("foreign_key_list id %d", id),
			referencedTable:     referencedTable,
			referencedColumn:    referencedColumn.String,
			updateAction:        updateAction,
			implicitPrimaryKey:  !referencedColumn.Valid,
			parentNamespace:     "",
			referencedNamespace: "",
		}
		if err := validateStage4NetworkIncomingForeignKey(
			"SQLite",
			profile,
			dependency,
			stage4SQLiteIdentifier,
		); err != nil {
			_ = rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf(
			"iterate SQLite incoming foreign keys from dependent table %s for Stage 4 network replay isolation: %w",
			parentTable,
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf(
			"close SQLite incoming foreign keys from dependent table %s for Stage 4 network replay isolation: %w",
			parentTable,
			err,
		)
	}
	return nil
}
