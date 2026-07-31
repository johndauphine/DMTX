package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

const stage4SQLServerNetworkIncomingForeignKeysQuery = `
	SELECT
		parent_schema.name,
		parent_table.name,
		target_foreign_key.name,
		referenced_schema.name,
		referenced_table.name,
		target_foreign_key.update_referential_action,
		target_foreign_key.update_referential_action_desc,
		referenced_column.name
	FROM sys.foreign_keys AS target_foreign_key
	JOIN sys.tables AS parent_table
	  ON parent_table.object_id =
	     target_foreign_key.parent_object_id
	JOIN sys.schemas AS parent_schema
	  ON parent_schema.schema_id = parent_table.schema_id
	JOIN sys.tables AS referenced_table
	  ON referenced_table.object_id =
	     target_foreign_key.referenced_object_id
	JOIN sys.schemas AS referenced_schema
	  ON referenced_schema.schema_id =
	     referenced_table.schema_id
	JOIN sys.foreign_key_columns AS target_foreign_key_column
	  ON target_foreign_key_column.constraint_object_id =
	     target_foreign_key.object_id
	JOIN sys.columns AS referenced_column
	  ON referenced_column.object_id =
	     target_foreign_key_column.referenced_object_id
	 AND referenced_column.column_id =
	     target_foreign_key_column.referenced_column_id
	WHERE referenced_schema.name = @p1
	  AND referenced_table.name = @p2
	ORDER BY
		parent_schema.name,
		parent_table.name,
		target_foreign_key.name,
		target_foreign_key_column.constraint_column_id
`

const stage4SQLServerNetworkPrimaryKeyQuery = `
	SELECT
		target_schema.name,
		target_table.name,
		target_index.name,
		target_index.is_primary_key,
		target_index.is_unique,
		target_index.type_desc,
		target_index.is_disabled,
		target_index.has_filter,
		target_index.filter_definition,
		key_column.key_ordinal,
		key_column.is_descending_key,
		key_column.is_included_column,
		target_column.name
	FROM sys.tables AS target_table
	JOIN sys.schemas AS target_schema
	  ON target_schema.schema_id = target_table.schema_id
	JOIN sys.indexes AS target_index
	  ON target_index.object_id = target_table.object_id
	 AND target_index.is_primary_key = 1
	JOIN sys.index_columns AS key_column
	  ON key_column.object_id = target_index.object_id
	 AND key_column.index_id = target_index.index_id
	 AND key_column.key_ordinal > 0
	JOIN sys.columns AS target_column
	  ON target_column.object_id = key_column.object_id
	 AND target_column.column_id = key_column.column_id
	WHERE target_schema.name = @p1
	  AND target_table.name = @p2
	ORDER BY key_column.key_ordinal
`

const stage4SQLServerNetworkMetadataVisibilityQuery = `
	SELECT
		CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
			DB_NAME(), 'DATABASE', 'VIEW DEFINITION'
		), 0)),
		CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
			DB_NAME(), 'DATABASE', 'VIEW SECURITY DEFINITION'
		), 0)),
		CONVERT(bit, CASE WHEN EXISTS (
			SELECT 1
			  FROM sys.database_permissions AS denied_permission
			  JOIN sys.user_token AS current_token
			    ON current_token.principal_id =
			       denied_permission.grantee_principal_id
			 WHERE denied_permission.state = 'D'
			   AND denied_permission.class IN (0, 1, 3)
		) OR EXISTS (
			SELECT 1
			  FROM sys.server_permissions AS denied_permission
			  JOIN sys.login_token AS current_token
			    ON current_token.principal_id =
			       denied_permission.grantee_principal_id
			 WHERE denied_permission.state = 'D'
			   AND denied_permission.class = 100
		) THEN 1 ELSE 0 END)
`

func stage4SQLServerNetworkReplayFenceStatement(
	table schema.Table,
) string {
	return "SELECT TOP (1) 1 FROM " +
		sqlServerQualified(table.Schema, table.Name) +
		" WITH (TABLOCKX, HOLDLOCK)"
}

// SQL Server already rejects retained foreign keys crossing the selected table
// boundary during ordinary upsert preflight. Network admission repeats that
// exact engine-owned proof rather than introducing a weaker parallel catalog
// interpretation.
func (adapter *sqlServerTargetAdapter) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	tables []schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQL Server Stage 4 network replay-isolation context is required",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if adapter == nil || adapter.database == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQL Server target database is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	if len(tables) == 0 {
		return nil
	}
	selected, plannedNamespace, err := sqlServerSelectedTargetTables(tables)
	if err != nil {
		return err
	}
	if err := validateStage4SQLServerNetworkNamespaceIdentity(
		adapter.namespace,
		adapter.namespace,
		tables,
	); err != nil {
		return err
	}
	var catalogNamespace string
	err = adapter.database.QueryRowContext(
		ctx,
		`SELECT name
		   FROM sys.schemas
		  WHERE name = @p1`,
		plannedNamespace,
	).Scan(&catalogNamespace)
	if errors.Is(err, sql.ErrNoRows) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target schema %q is absent",
				plannedNamespace,
			),
		)
	}
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network target schema %q: %w",
			plannedNamespace,
			err,
		)
	}
	if err := validateStage4SQLServerNetworkNamespaceIdentity(
		adapter.namespace,
		catalogNamespace,
		tables,
	); err != nil {
		return err
	}
	return preflightSQLServerExternalForeignKeys(
		ctx,
		adapter.database,
		catalogNamespace,
		selected,
	)
}

func validateStage4SQLServerNetworkNamespaceIdentity(
	expected string,
	catalog string,
	tables []schema.Table,
) error {
	if expected == "" || catalog == "" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 SQL Server network target schema identity is empty",
			),
		)
	}
	if catalog != expected {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target schema spelling %q differs from catalog identity %q",
				expected,
				catalog,
			),
		)
	}
	for _, table := range tables {
		if table.Schema != expected {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 SQL Server network target table %s schema %q differs from target schema %q",
					table.Name,
					table.Schema,
					expected,
				),
			)
		}
	}
	return nil
}

// preflightStage4SQLServerNetworkReplayIsolationTransaction repeats the
// complete current-table proof after the native writer has acquired its
// transaction-held table lock. It deliberately does not depend on the
// selected-table set: every incoming relationship must be independently safe
// for a page replay.
func preflightStage4SQLServerNetworkReplayIsolationTransaction(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	table schema.Table,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQL Server Stage 4 network replay-isolation context is required",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if queryer == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQL Server target transaction catalog is required for Stage 4 network replay isolation",
			),
		)
	}
	if table.Schema == "" || table.Name == "" {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"SQL Server Stage 4 network target identity is empty",
			),
		)
	}

	var (
		viewDefinition         bool
		viewSecurityDefinition bool
		hasOverridingDeny      bool
	)
	if err := queryer.QueryRowContext(
		ctx,
		stage4SQLServerNetworkMetadataVisibilityQuery,
	).Scan(
		&viewDefinition,
		&viewSecurityDefinition,
		&hasOverridingDeny,
	); err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network foreign-key metadata visibility: %w",
			err,
		)
	}
	if err := validateStage4SQLServerNetworkMetadataVisibility(
		viewDefinition,
		viewSecurityDefinition,
		hasOverridingDeny,
	); err != nil {
		return err
	}

	var catalogSchema, catalogTable string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT target_schema.name, target_table.name
		   FROM sys.tables AS target_table
		   JOIN sys.schemas AS target_schema
		     ON target_schema.schema_id = target_table.schema_id
		  WHERE target_schema.name = @p1
		    AND target_table.name = @p2`,
		table.Schema,
		table.Name,
	).Scan(&catalogSchema, &catalogTable)
	if errors.Is(err, sql.ErrNoRows) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target table %s.%s is absent during replay proof",
				table.Schema,
				table.Name,
			),
		)
	}
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network target identity %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if catalogSchema != table.Schema || catalogTable != table.Name {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target spelling %q.%q differs from catalog identity %q.%q",
				table.Schema,
				table.Name,
				catalogSchema,
				catalogTable,
			),
		)
	}
	targetCatalog, exists, err := readSQLServerTargetTableCatalog(
		ctx,
		queryer,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network retained target catalog %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if !exists {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target table %s.%s disappeared during replay proof",
				table.Schema,
				table.Name,
			),
		)
	}
	if err := validateSQLServerTargetTableCatalog(
		table.Name,
		targetCatalog,
	); err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	if err := preflightStage4SQLServerNetworkPrimaryKey(
		ctx,
		queryer,
		table,
	); err != nil {
		return err
	}
	actual, err := engine.InspectSQLServerTargetTableWithQueryer(
		ctx,
		queryer,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network retained shape for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := validateSQLServerRetainedTableShape(
		table,
		actual,
	); err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target %s.%s full retained shape differs from the admitted plan: %w",
				table.Schema,
				table.Name,
				err,
			),
		)
	}

	profiles, err := stage4NetworkReplayTableProfiles(
		"SQL Server",
		[]schema.Table{table},
		stage4ExactIdentifier,
	)
	if err != nil {
		return err
	}
	profile := profiles[adapterSourceTableKey(
		table.Schema,
		table.Name,
	)]
	rows, err := queryer.QueryContext(
		ctx,
		stage4SQLServerNetworkIncomingForeignKeysQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server incoming foreign keys for Stage 4 network table %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	for rows.Next() {
		var dependency stage4NetworkIncomingForeignKey
		var updateAction int
		var updateDescription string
		if err := rows.Scan(
			&dependency.parentNamespace,
			&dependency.parentTable,
			&dependency.name,
			&dependency.referencedNamespace,
			&dependency.referencedTable,
			&updateAction,
			&updateDescription,
			&dependency.referencedColumn,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"read SQL Server incoming foreign key for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		dependency.updateAction, err =
			stage4SQLServerNetworkUpdateAction(
				updateAction,
				updateDescription,
			)
		if err != nil {
			_ = rows.Close()
			return stage4NetworkReplayCatalogShapeError(
				"SQL Server",
				profile,
				fmt.Sprintf(
					"incoming foreign key %s: %v",
					dependency.name,
					err,
				),
			)
		}
		if err := validateStage4NetworkIncomingForeignKey(
			"SQL Server",
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
			"iterate SQL Server incoming foreign keys for Stage 4 network table %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf(
			"close SQL Server incoming foreign keys for Stage 4 network table %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return nil
}

func validateStage4SQLServerNetworkMetadataVisibility(
	viewDefinition bool,
	viewSecurityDefinition bool,
	hasOverridingDeny bool,
) error {
	if !viewDefinition || !viewSecurityDefinition {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network replay isolation requires database VIEW DEFINITION and VIEW SECURITY DEFINITION permissions to prove complete incoming-foreign-key, trigger, and security-predicate visibility",
			),
		)
	}
	if hasOverridingDeny {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network replay isolation rejects overriding server, database, schema, object, or column metadata DENY permissions because they can hide incoming foreign keys, triggers, or security predicates",
			),
		)
	}
	return nil
}

func preflightStage4SQLServerNetworkPrimaryKey(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	table schema.Table,
) error {
	expected := primaryKeyColumns(table)
	if len(expected) == 0 {
		return NewTransferError(
			ErrorClassPrimaryKey,
			fmt.Errorf(
				"Stage 4 SQL Server network target table %s.%s has no planned primary key",
				table.Schema,
				table.Name,
			),
		)
	}
	rows, err := queryer.QueryContext(
		ctx,
		stage4SQLServerNetworkPrimaryKeyQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server Stage 4 network primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()
	position := 0
	for rows.Next() {
		var (
			catalogSchema, catalogTable, indexName, indexType string
			columnName                                        string
			primary, unique, disabled, filtered               bool
			descending, included                              bool
			filterDefinition                                  sql.NullString
			ordinal                                           int
		)
		if err := rows.Scan(
			&catalogSchema,
			&catalogTable,
			&indexName,
			&primary,
			&unique,
			&indexType,
			&disabled,
			&filtered,
			&filterDefinition,
			&ordinal,
			&descending,
			&included,
			&columnName,
		); err != nil {
			return fmt.Errorf(
				"read SQL Server Stage 4 network primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		position++
		if catalogSchema != table.Schema ||
			catalogTable != table.Name ||
			indexName == "" ||
			!primary ||
			!unique ||
			(indexType != "CLUSTERED" &&
				indexType != "NONCLUSTERED") ||
			disabled ||
			filtered ||
			filterDefinition.Valid ||
			ordinal != position ||
			included ||
			position > len(expected) ||
			columnName != expected[position-1] {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 SQL Server network target table %s.%s primary-key catalog differs from the planned ordered key",
					table.Schema,
					table.Name,
				),
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQL Server Stage 4 network primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if position != len(expected) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 SQL Server network target table %s.%s primary key has %d columns; planned ordered key has %d",
				table.Schema,
				table.Name,
				position,
				len(expected),
			),
		)
	}
	return nil
}

func stage4SQLServerNetworkUpdateAction(
	code int,
	description string,
) (string, error) {
	description = strings.ToUpper(strings.TrimSpace(description))
	expected := ""
	canonical := ""
	switch code {
	case 0:
		expected, canonical = "NO_ACTION", "NO ACTION"
	case 1:
		expected, canonical = "CASCADE", "CASCADE"
	case 2:
		expected, canonical = "SET_NULL", "SET NULL"
	case 3:
		expected, canonical = "SET_DEFAULT", "SET DEFAULT"
	default:
		return "", fmt.Errorf(
			"unexpected ON UPDATE action code %d",
			code,
		)
	}
	if description != expected {
		return "", fmt.Errorf(
			"ON UPDATE action code %d conflicts with description %q",
			code,
			description,
		)
	}
	return canonical, nil
}
