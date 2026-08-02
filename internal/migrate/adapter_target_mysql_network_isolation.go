package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type stage4MySQLReplayCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const stage4MySQLIncomingForeignKeysExactQuery = `
	SELECT
		column_usage.TABLE_SCHEMA,
		column_usage.TABLE_NAME,
		column_usage.CONSTRAINT_NAME,
		column_usage.REFERENCED_TABLE_SCHEMA,
		column_usage.REFERENCED_TABLE_NAME,
		referential.UPDATE_RULE,
		column_usage.REFERENCED_COLUMN_NAME
	FROM information_schema.KEY_COLUMN_USAGE AS column_usage
	JOIN information_schema.REFERENTIAL_CONSTRAINTS AS referential
	  ON referential.CONSTRAINT_SCHEMA =
	     column_usage.CONSTRAINT_SCHEMA
	 AND referential.CONSTRAINT_NAME =
	     column_usage.CONSTRAINT_NAME
	 AND referential.TABLE_NAME =
	     column_usage.TABLE_NAME
	WHERE BINARY column_usage.REFERENCED_TABLE_SCHEMA = BINARY ?
	  AND BINARY column_usage.REFERENCED_TABLE_NAME = BINARY ?
	  AND column_usage.REFERENCED_COLUMN_NAME IS NOT NULL
	ORDER BY
		column_usage.TABLE_SCHEMA,
		column_usage.TABLE_NAME,
		column_usage.CONSTRAINT_NAME,
		column_usage.ORDINAL_POSITION
`

const stage4MySQLIncomingForeignKeysFoldedQuery = `
	SELECT
		column_usage.TABLE_SCHEMA,
		column_usage.TABLE_NAME,
		column_usage.CONSTRAINT_NAME,
		column_usage.REFERENCED_TABLE_SCHEMA,
		column_usage.REFERENCED_TABLE_NAME,
		referential.UPDATE_RULE,
		column_usage.REFERENCED_COLUMN_NAME
	FROM information_schema.KEY_COLUMN_USAGE AS column_usage
	JOIN information_schema.REFERENTIAL_CONSTRAINTS AS referential
	  ON referential.CONSTRAINT_SCHEMA =
	     column_usage.CONSTRAINT_SCHEMA
	 AND referential.CONSTRAINT_NAME =
	     column_usage.CONSTRAINT_NAME
	 AND referential.TABLE_NAME =
	     column_usage.TABLE_NAME
	WHERE LOWER(column_usage.REFERENCED_TABLE_SCHEMA) = LOWER(?)
	  AND LOWER(column_usage.REFERENCED_TABLE_NAME) = LOWER(?)
	  AND column_usage.REFERENCED_COLUMN_NAME IS NOT NULL
	ORDER BY
		column_usage.TABLE_SCHEMA,
		column_usage.TABLE_NAME,
		column_usage.CONSTRAINT_NAME,
		column_usage.ORDINAL_POSITION
`

const stage4OracleMySQLForeignKeyVisibilityQuery = `
	SELECT
		COUNT(*) > 0,
		@@global.partial_revokes
	FROM information_schema.USER_PRIVILEGES
	WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
	  AND PRIVILEGE_TYPE IN ('REFERENCES', 'SELECT')
`

const stage4MariaDBForeignKeyVisibilityQuery = `
	SELECT COUNT(*) > 0
	FROM information_schema.USER_PRIVILEGES
	WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
	  AND PRIVILEGE_TYPE IN ('REFERENCES', 'SELECT')
`

const stage4OracleMySQLTriggerVisibilityQuery = `
	SELECT
		EXISTS (
			SELECT 1
			FROM information_schema.USER_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.SCHEMA_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND TABLE_SCHEMA = ?
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.TABLE_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND TABLE_SCHEMA = ?
			  AND TABLE_NAME = ?
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		),
		@@global.partial_revokes
`

const stage4MariaDBTriggerVisibilityQuery = `
	SELECT
		EXISTS (
			SELECT 1
			FROM information_schema.USER_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.SCHEMA_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND TABLE_SCHEMA = ?
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.TABLE_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND TABLE_SCHEMA = ?
			  AND TABLE_NAME = ?
			  AND PRIVILEGE_TYPE = 'TRIGGER'
		)
`

const stage4MySQLReplayTargetExactQuery = `
	SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE
	FROM information_schema.TABLES
	WHERE BINARY TABLE_SCHEMA = BINARY ?
	  AND BINARY TABLE_NAME = BINARY ?
`

const stage4MySQLReplayTargetFoldedQuery = `
	SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE
	FROM information_schema.TABLES
	WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
	  AND LOWER(TABLE_NAME) = LOWER(?)
`

const stage4MySQLReplayPrimaryKeyExactQuery = `
	SELECT
		target_constraint.CONSTRAINT_NAME,
		target_constraint.CONSTRAINT_TYPE,
		target_index.SEQ_IN_INDEX,
		target_index.COLUMN_NAME,
		target_index.COLLATION,
		target_index.NON_UNIQUE,
		target_index.INDEX_TYPE,
		target_index.SUB_PART,
		target_index.NULLABLE
	FROM information_schema.TABLE_CONSTRAINTS AS target_constraint
	JOIN information_schema.STATISTICS AS target_index
	  ON target_index.TABLE_SCHEMA = target_constraint.TABLE_SCHEMA
	 AND target_index.TABLE_NAME = target_constraint.TABLE_NAME
	 AND target_index.INDEX_NAME = target_constraint.CONSTRAINT_NAME
	WHERE BINARY target_constraint.TABLE_SCHEMA = BINARY ?
	  AND BINARY target_constraint.TABLE_NAME = BINARY ?
	  AND target_constraint.CONSTRAINT_TYPE = 'PRIMARY KEY'
	ORDER BY target_index.SEQ_IN_INDEX
`

const stage4MySQLReplayPrimaryKeyFoldedQuery = `
	SELECT
		target_constraint.CONSTRAINT_NAME,
		target_constraint.CONSTRAINT_TYPE,
		target_index.SEQ_IN_INDEX,
		target_index.COLUMN_NAME,
		target_index.COLLATION,
		target_index.NON_UNIQUE,
		target_index.INDEX_TYPE,
		target_index.SUB_PART,
		target_index.NULLABLE
	FROM information_schema.TABLE_CONSTRAINTS AS target_constraint
	JOIN information_schema.STATISTICS AS target_index
	  ON target_index.TABLE_SCHEMA = target_constraint.TABLE_SCHEMA
	 AND target_index.TABLE_NAME = target_constraint.TABLE_NAME
	 AND target_index.INDEX_NAME = target_constraint.CONSTRAINT_NAME
	WHERE LOWER(target_constraint.TABLE_SCHEMA) = LOWER(?)
	  AND LOWER(target_constraint.TABLE_NAME) = LOWER(?)
	  AND target_constraint.CONSTRAINT_TYPE = 'PRIMARY KEY'
	ORDER BY target_index.SEQ_IN_INDEX
`

const stage4MySQLReplayTriggersExactQuery = `
	SELECT
		EVENT_OBJECT_SCHEMA,
		EVENT_OBJECT_TABLE,
		TRIGGER_NAME
	FROM information_schema.TRIGGERS
	WHERE BINARY EVENT_OBJECT_SCHEMA = BINARY ?
	  AND BINARY EVENT_OBJECT_TABLE = BINARY ?
	ORDER BY TRIGGER_NAME
`

const stage4MySQLReplayTriggersFoldedQuery = `
	SELECT
		EVENT_OBJECT_SCHEMA,
		EVENT_OBJECT_TABLE,
		TRIGGER_NAME
	FROM information_schema.TRIGGERS
	WHERE LOWER(EVENT_OBJECT_SCHEMA) = LOWER(?)
	  AND LOWER(EVENT_OBJECT_TABLE) = LOWER(?)
	ORDER BY TRIGGER_NAME
`

func stage4MySQLNetworkReplayFenceStatement(
	table schema.Table,
) string {
	return "SELECT 1 FROM " +
		mySQLQualified(table.Schema, table.Name) +
		" LIMIT 1 FOR UPDATE"
}

func (adapter *mysqlTargetAdapter) PreflightStage4NetworkReplayIsolation(
	ctx context.Context,
	tables []schema.Table,
) error {
	if adapter == nil || adapter.database == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"MySQL target database is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	for _, table := range tables {
		if table.Schema != adapter.namespace {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 MySQL network target table %s database %q differs from target database %q",
					table.Name,
					table.Schema,
					adapter.namespace,
				),
			)
		}
	}
	return preflightStage4MySQLNetworkReplayIsolation(
		ctx,
		adapter.database,
		tables,
		adapter.flavor,
	)
}

func preflightStage4MySQLNetworkReplayIsolation(
	ctx context.Context,
	queryer stage4MySQLReplayCatalogQueryer,
	tables []schema.Table,
	flavor engine.MySQLServerFlavor,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"MySQL Stage 4 network replay-isolation context is required",
			),
		)
	}
	if queryer == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"MySQL target catalog is required for Stage 4 network replay-isolation preflight",
			),
		)
	}
	if err := engine.VerifyMySQLTargetForFlavor(
		ctx,
		queryer,
		flavor,
	); err != nil {
		return fmt.Errorf(
			"verify exact MySQL Stage 4 network target session: %w",
			err,
		)
	}
	if err := preflightStage4MySQLForeignKeyMetadataVisibility(
		ctx,
		queryer,
		flavor,
	); err != nil {
		return err
	}
	var lowerCaseTableNames int
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT @@lower_case_table_names",
	).Scan(&lowerCaseTableNames); err != nil {
		return fmt.Errorf(
			"inspect MySQL identifier case for Stage 4 network replay isolation: %w",
			err,
		)
	}
	normalize, catalogQuery, err := stage4MySQLReplayIdentifierContract(
		lowerCaseTableNames,
	)
	if err != nil {
		return err
	}
	profiles, err := stage4NetworkReplayTableProfiles(
		"MySQL",
		tables,
		normalize,
	)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table.Schema == "" {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 MySQL network target table %s has an empty database",
					table.Name,
				),
			)
		}
		if err := preflightStage4MySQLTriggerMetadataVisibility(
			ctx,
			queryer,
			flavor,
			table,
		); err != nil {
			return err
		}
		profile := profiles[adapterSourceTableKey(
			normalize(table.Schema),
			normalize(table.Name),
		)]
		if err := preflightStage4MySQLRetainedReplayTarget(
			ctx,
			queryer,
			table,
			flavor,
			normalize,
			lowerCaseTableNames,
		); err != nil {
			return err
		}
		rows, err := queryer.QueryContext(
			ctx,
			catalogQuery,
			table.Schema,
			table.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect MySQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		for rows.Next() {
			var dependency stage4NetworkIncomingForeignKey
			if err := rows.Scan(
				&dependency.parentNamespace,
				&dependency.parentTable,
				&dependency.name,
				&dependency.referencedNamespace,
				&dependency.referencedTable,
				&dependency.updateAction,
				&dependency.referencedColumn,
			); err != nil {
				_ = rows.Close()
				return fmt.Errorf(
					"read MySQL incoming foreign key for Stage 4 network table %s.%s: %w",
					table.Schema,
					table.Name,
					err,
				)
			}
			if err := validateStage4NetworkIncomingForeignKey(
				"MySQL",
				profile,
				dependency,
				normalize,
			); err != nil {
				_ = rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"iterate MySQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf(
				"close MySQL incoming foreign keys for Stage 4 network table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
	}
	return nil
}

func preflightStage4MySQLTriggerMetadataVisibility(
	ctx context.Context,
	queryer stage4MySQLReplayCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	table schema.Table,
) error {
	var hasGlobalVisibility, hasSchemaVisibility, hasTableVisibility bool
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		var partialRevokes int
		if err := queryer.QueryRowContext(
			ctx,
			stage4OracleMySQLTriggerVisibilityQuery,
			table.Schema,
			table.Schema,
			table.Name,
		).Scan(
			&hasGlobalVisibility,
			&hasSchemaVisibility,
			&hasTableVisibility,
			&partialRevokes,
		); err != nil {
			return fmt.Errorf(
				"inspect MySQL trigger metadata visibility for Stage 4 network replay isolation: %w",
				err,
			)
		}
		return validateStage4MySQLTriggerMetadataVisibility(
			"MySQL",
			hasGlobalVisibility,
			hasSchemaVisibility,
			hasTableVisibility,
			partialRevokes,
		)
	case engine.MySQLServerFlavorMariaDB1011:
		if err := queryer.QueryRowContext(
			ctx,
			stage4MariaDBTriggerVisibilityQuery,
			table.Schema,
			table.Schema,
			table.Name,
		).Scan(
			&hasGlobalVisibility,
			&hasSchemaVisibility,
			&hasTableVisibility,
		); err != nil {
			return fmt.Errorf(
				"inspect MariaDB trigger metadata visibility for Stage 4 network replay isolation: %w",
				err,
			)
		}
		return validateStage4MySQLTriggerMetadataVisibility(
			"MariaDB",
			hasGlobalVisibility,
			hasSchemaVisibility,
			hasTableVisibility,
			0,
		)
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network replay isolation received an unsupported server flavor",
			),
		)
	}
}

func validateStage4MySQLTriggerMetadataVisibility(
	engineName string,
	hasGlobalVisibility bool,
	hasSchemaVisibility bool,
	hasTableVisibility bool,
	partialRevokes int,
) error {
	if hasSchemaVisibility || hasTableVisibility {
		return nil
	}
	if !hasGlobalVisibility {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 %s network replay isolation requires global, target-schema, or selected-table TRIGGER privilege to prove complete selected-table trigger metadata visibility",
				engineName,
			),
		)
	}
	if partialRevokes != 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 %s network replay isolation requires partial_revokes=0 to prove complete selected-table trigger metadata visibility",
				engineName,
			),
		)
	}
	return nil
}

func preflightStage4MySQLRetainedReplayTarget(
	ctx context.Context,
	queryer stage4MySQLReplayCatalogQueryer,
	table schema.Table,
	flavor engine.MySQLServerFlavor,
	normalize stage4NetworkIdentifierNormalizer,
	lowerCaseTableNames int,
) error {
	targetQuery := stage4MySQLReplayTargetExactQuery
	primaryKeyQuery := stage4MySQLReplayPrimaryKeyExactQuery
	triggerQuery := stage4MySQLReplayTriggersExactQuery
	if lowerCaseTableNames == 1 || lowerCaseTableNames == 2 {
		targetQuery = stage4MySQLReplayTargetFoldedQuery
		primaryKeyQuery = stage4MySQLReplayPrimaryKeyFoldedQuery
		triggerQuery = stage4MySQLReplayTriggersFoldedQuery
	}

	targetRows, err := queryer.QueryContext(
		ctx,
		targetQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL Stage 4 network retained target %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	targetCount := 0
	for targetRows.Next() {
		var (
			catalogSchema, catalogTable, tableType string
			storageEngine                          sql.NullString
		)
		if err := targetRows.Scan(
			&catalogSchema,
			&catalogTable,
			&tableType,
			&storageEngine,
		); err != nil {
			_ = targetRows.Close()
			return fmt.Errorf(
				"read MySQL Stage 4 network retained target %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		targetCount++
		if normalize(catalogSchema) != normalize(table.Schema) ||
			normalize(catalogTable) != normalize(table.Name) ||
			tableType != "BASE TABLE" ||
			!storageEngine.Valid ||
			!strings.EqualFold(storageEngine.String, "InnoDB") {
			_ = targetRows.Close()
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 MySQL network target %s.%s is not the planned InnoDB base table",
					table.Schema,
					table.Name,
				),
			)
		}
	}
	if err := targetRows.Err(); err != nil {
		_ = targetRows.Close()
		return fmt.Errorf(
			"iterate MySQL Stage 4 network retained target %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := targetRows.Close(); err != nil {
		return fmt.Errorf(
			"close MySQL Stage 4 network retained target %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if targetCount != 1 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network target identity %s.%s resolved to %d catalog objects",
				table.Schema,
				table.Name,
				targetCount,
			),
		)
	}

	expectedKey := primaryKeyColumns(table)
	if len(expectedKey) == 0 {
		return NewTransferError(
			ErrorClassPrimaryKey,
			fmt.Errorf(
				"Stage 4 MySQL network target table %s.%s has no planned primary key",
				table.Schema,
				table.Name,
			),
		)
	}
	keyRows, err := queryer.QueryContext(
		ctx,
		primaryKeyQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL Stage 4 network primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	position := 0
	for keyRows.Next() {
		var (
			constraintName, constraintType string
			column, direction              sql.NullString
			indexType, nullable            string
			ordinal, nonUnique             int
			prefixLength                   sql.NullInt64
		)
		if err := keyRows.Scan(
			&constraintName,
			&constraintType,
			&ordinal,
			&column,
			&direction,
			&nonUnique,
			&indexType,
			&prefixLength,
			&nullable,
		); err != nil {
			_ = keyRows.Close()
			return fmt.Errorf(
				"read MySQL Stage 4 network primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		position++
		if constraintName != "PRIMARY" ||
			constraintType != "PRIMARY KEY" ||
			ordinal != position ||
			!column.Valid ||
			nonUnique != 0 ||
			!strings.EqualFold(indexType, "BTREE") ||
			prefixLength.Valid ||
			nullable != "" ||
			position > len(expectedKey) ||
			column.String != expectedKey[position-1] {
			_ = keyRows.Close()
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 MySQL network target %s.%s primary-key catalog differs from the planned ordered key",
					table.Schema,
					table.Name,
				),
			)
		}
	}
	if err := keyRows.Err(); err != nil {
		_ = keyRows.Close()
		return fmt.Errorf(
			"iterate MySQL Stage 4 network primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := keyRows.Close(); err != nil {
		return fmt.Errorf(
			"close MySQL Stage 4 network primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if position != len(expectedKey) {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network target %s.%s primary key has %d columns; planned ordered key has %d",
				table.Schema,
				table.Name,
				position,
				len(expectedKey),
			),
		)
	}

	triggerRows, err := queryer.QueryContext(
		ctx,
		triggerQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL Stage 4 network target triggers for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	// One trigger is enough to refuse, so this reads a single row rather than
	// iterating: every path below returns. Written as a loop it looked like it
	// scanned all triggers, which it never did.
	if triggerRows.Next() {
		var catalogSchema, catalogTable, triggerName string
		if err := triggerRows.Scan(
			&catalogSchema,
			&catalogTable,
			&triggerName,
		); err != nil {
			_ = triggerRows.Close()
			return fmt.Errorf(
				"read MySQL Stage 4 network target trigger for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		_ = triggerRows.Close()
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network target %s.%s has replay-unsafe trigger %s on catalog table %s.%s",
				table.Schema,
				table.Name,
				triggerName,
				catalogSchema,
				catalogTable,
			),
		)
	}
	if err := triggerRows.Err(); err != nil {
		_ = triggerRows.Close()
		return fmt.Errorf(
			"iterate MySQL Stage 4 network target triggers for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := triggerRows.Close(); err != nil {
		return fmt.Errorf(
			"close MySQL Stage 4 network target triggers for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	actual, err := engine.InspectMySQLTableForFlavor(
		ctx,
		queryer,
		flavor,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL Stage 4 network retained shape for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if err := validateMySQLRetainedTableShape(table, actual); err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network target %s.%s full retained shape differs from the admitted plan: %w",
				table.Schema,
				table.Name,
				err,
			),
		)
	}
	return nil
}

func preflightStage4MySQLForeignKeyMetadataVisibility(
	ctx context.Context,
	queryer stage4MySQLReplayCatalogQueryer,
	flavor engine.MySQLServerFlavor,
) error {
	var hasGlobalVisibility bool
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		var partialRevokes int
		if err := queryer.QueryRowContext(
			ctx,
			stage4OracleMySQLForeignKeyVisibilityQuery,
		).Scan(
			&hasGlobalVisibility,
			&partialRevokes,
		); err != nil {
			return fmt.Errorf(
				"inspect MySQL foreign-key metadata visibility for Stage 4 network replay isolation: %w",
				err,
			)
		}
		return validateStage4MySQLForeignKeyMetadataVisibility(
			"MySQL",
			hasGlobalVisibility,
			partialRevokes,
		)
	case engine.MySQLServerFlavorMariaDB1011:
		if err := queryer.QueryRowContext(
			ctx,
			stage4MariaDBForeignKeyVisibilityQuery,
		).Scan(&hasGlobalVisibility); err != nil {
			return fmt.Errorf(
				"inspect MariaDB foreign-key metadata visibility for Stage 4 network replay isolation: %w",
				err,
			)
		}
		return validateStage4MySQLForeignKeyMetadataVisibility(
			"MariaDB",
			hasGlobalVisibility,
			0,
		)
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network replay isolation received an unsupported server flavor",
			),
		)
	}
}

func validateStage4MySQLForeignKeyMetadataVisibility(
	engineName string,
	hasGlobalVisibility bool,
	partialRevokes int,
) error {
	if !hasGlobalVisibility {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 %s network replay isolation requires global REFERENCES or SELECT privilege to prove complete foreign-key metadata visibility",
				engineName,
			),
		)
	}
	if partialRevokes != 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 %s network replay isolation requires partial_revokes=0 to prove complete foreign-key metadata visibility",
				engineName,
			),
		)
	}
	return nil
}

func stage4MySQLReplayIdentifierContract(
	lowerCaseTableNames int,
) (stage4NetworkIdentifierNormalizer, string, error) {
	switch lowerCaseTableNames {
	case 0:
		return stage4ExactIdentifier,
			stage4MySQLIncomingForeignKeysExactQuery,
			nil
	case 1, 2:
		return strings.ToLower,
			stage4MySQLIncomingForeignKeysFoldedQuery,
			nil
	default:
		return nil, "", NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 MySQL network replay catalog returned unsupported lower_case_table_names=%d",
				lowerCaseTableNames,
			),
		)
	}
}
