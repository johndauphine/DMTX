package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	postgresDeleteJournalSchema  = "dmtx_internal"
	postgresDeleteJournalTable   = "delete_batch_receipts"
	postgresDeleteJournalVersion = int16(1)
)

type postgresDeleteJournalQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type postgresDeleteJournalCatalog struct {
	schemaExists bool
	tableExists  bool
	schemaOID    int64
	relationOID  int64
}

func inspectPostgresDeleteReceiptJournal(
	ctx context.Context,
	queryer postgresDeleteJournalQueryer,
) (postgresDeleteJournalCatalog, error) {
	if queryer == nil {
		return postgresDeleteJournalCatalog{}, fmt.Errorf(
			"PostgreSQL delete receipt catalog is unavailable",
		)
	}
	var (
		schemaOwner  bool
		schemaUsage  bool
		schemaCreate bool
		nonOwnerACLs int
		catalog      postgresDeleteJournalCatalog
	)
	err := queryer.QueryRowContext(ctx, `
		SELECT
			namespace.oid::bigint,
			pg_catalog.pg_get_userbyid(namespace.nspowner) =
				CURRENT_USER,
			pg_catalog.has_schema_privilege(
				CURRENT_USER,
				namespace.oid,
				'USAGE'
			),
			pg_catalog.has_schema_privilege(
				CURRENT_USER,
				namespace.oid,
				'CREATE'
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.aclexplode(
					COALESCE(
						namespace.nspacl,
						pg_catalog.acldefault(
							'n',
							namespace.nspowner
						)
					)
				) AS acl
				WHERE acl.grantee <> namespace.nspowner
			)
		FROM pg_catalog.pg_namespace AS namespace
		WHERE namespace.nspname = $1
	`, postgresDeleteJournalSchema).Scan(
		&catalog.schemaOID,
		&schemaOwner,
		&schemaUsage,
		&schemaCreate,
		&nonOwnerACLs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		var databaseCreate bool
		if err := queryer.QueryRowContext(ctx, `
			SELECT pg_catalog.has_database_privilege(
				CURRENT_USER,
				CURRENT_DATABASE(),
				'CREATE'
			)
		`).Scan(&databaseCreate); err != nil {
			return postgresDeleteJournalCatalog{}, fmt.Errorf(
				"inspect PostgreSQL journal schema creation privilege: %w",
				err,
			)
		}
		if !databaseCreate {
			return postgresDeleteJournalCatalog{}, fmt.Errorf(
				"current PostgreSQL user cannot create the private %s receipt schema",
				postgresDeleteJournalSchema,
			)
		}
		return postgresDeleteJournalCatalog{}, nil
	}
	if err != nil {
		return postgresDeleteJournalCatalog{}, fmt.Errorf(
			"inspect PostgreSQL delete receipt schema: %w",
			err,
		)
	}
	catalog.schemaExists = true
	if catalog.schemaOID <= 0 ||
		!schemaOwner ||
		!schemaUsage ||
		!schemaCreate ||
		nonOwnerACLs != 0 {
		return postgresDeleteJournalCatalog{}, fmt.Errorf(
			"PostgreSQL delete receipt schema %s must have an exact owner-only ACL",
			postgresDeleteJournalSchema,
		)
	}

	var (
		relationKind         string
		relationPersistence  string
		relationAccessMethod string
		relationOwner        bool
		isPartition          bool
		hasSubclass          bool
		rowSecurity          bool
		forceRowSecurity     bool
		parentCount          int
		childCount           int
		userTriggers         int
		rules                int
		policies             int
		relationACLs         int
		columnACLs           int
		droppedColumns       int
	)
	err = queryer.QueryRowContext(ctx, `
		SELECT
			relation.oid::bigint,
			relation.relkind::text,
			relation.relpersistence::text,
			(
				SELECT access_method.amname
				FROM pg_catalog.pg_am AS access_method
				WHERE access_method.oid = relation.relam
			),
			pg_catalog.pg_get_userbyid(relation.relowner) =
				CURRENT_USER,
			relation.relispartition,
			relation.relhassubclass,
			relation.relrowsecurity,
			relation.relforcerowsecurity,
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_inherits AS inheritance
				WHERE inheritance.inhrelid = relation.oid
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_inherits AS inheritance
				WHERE inheritance.inhparent = relation.oid
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_trigger AS trigger_object
				WHERE trigger_object.tgrelid = relation.oid
				  AND NOT trigger_object.tgisinternal
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_rewrite AS rule_object
				WHERE rule_object.ev_class = relation.oid
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_policy AS policy
				WHERE policy.polrelid = relation.oid
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.aclexplode(
					COALESCE(
						relation.relacl,
						pg_catalog.acldefault(
							'r',
							relation.relowner
						)
					)
				) AS acl
				WHERE acl.grantee <> relation.relowner
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_attribute AS attribute
				WHERE attribute.attrelid = relation.oid
				  AND attribute.attnum > 0
				  AND NOT attribute.attisdropped
				  AND attribute.attacl IS NOT NULL
			),
			(
				SELECT COUNT(*)
				FROM pg_catalog.pg_attribute AS attribute
				WHERE attribute.attrelid = relation.oid
				  AND attribute.attnum > 0
				  AND attribute.attisdropped
			)
		FROM pg_catalog.pg_class AS relation
		WHERE relation.relnamespace = $1
		  AND relation.relname = $2
	`, catalog.schemaOID, postgresDeleteJournalTable).Scan(
		&catalog.relationOID,
		&relationKind,
		&relationPersistence,
		&relationAccessMethod,
		&relationOwner,
		&isPartition,
		&hasSubclass,
		&rowSecurity,
		&forceRowSecurity,
		&parentCount,
		&childCount,
		&userTriggers,
		&rules,
		&policies,
		&relationACLs,
		&columnACLs,
		&droppedColumns,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return catalog, nil
	}
	if err != nil {
		return postgresDeleteJournalCatalog{}, fmt.Errorf(
			"inspect PostgreSQL delete receipt relation: %w",
			err,
		)
	}
	catalog.tableExists = true
	if catalog.relationOID <= 0 ||
		relationKind != "r" ||
		relationPersistence != "p" ||
		relationAccessMethod != "heap" ||
		!relationOwner ||
		isPartition ||
		hasSubclass ||
		rowSecurity ||
		forceRowSecurity ||
		parentCount != 0 ||
		childCount != 0 ||
		userTriggers != 0 ||
		rules != 0 ||
		policies != 0 ||
		relationACLs != 0 ||
		columnACLs != 0 ||
		droppedColumns != 0 {
		return postgresDeleteJournalCatalog{}, fmt.Errorf(
			"PostgreSQL delete receipt journal is not an exact owner-only, unpartitioned, hook-free heap",
		)
	}
	if err := validatePostgresDeleteJournalColumns(
		ctx,
		queryer,
		catalog.relationOID,
	); err != nil {
		return postgresDeleteJournalCatalog{}, err
	}
	if err := validatePostgresDeleteJournalConstraints(
		ctx,
		queryer,
		catalog.relationOID,
	); err != nil {
		return postgresDeleteJournalCatalog{}, err
	}
	if err := validatePostgresDeleteJournalIndex(
		ctx,
		queryer,
		catalog.relationOID,
	); err != nil {
		return postgresDeleteJournalCatalog{}, err
	}
	return catalog, nil
}

type postgresDeleteJournalColumn struct {
	name       string
	dataType   string
	notNull    bool
	hasDefault bool
	identity   string
	generated  string
}

func validatePostgresDeleteJournalColumns(
	ctx context.Context,
	queryer postgresDeleteJournalQueryer,
	relationOID int64,
) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			attribute.attname,
			pg_catalog.format_type(
				attribute.atttypid,
				attribute.atttypmod
			),
			attribute.attnotnull,
			attribute.atthasdef,
			attribute.attidentity::text,
			attribute.attgenerated::text
		FROM pg_catalog.pg_attribute AS attribute
		WHERE attribute.attrelid = $1
		  AND attribute.attnum > 0
		  AND NOT attribute.attisdropped
		ORDER BY attribute.attnum
	`, relationOID)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL delete receipt columns: %w",
			err,
		)
	}
	defer rows.Close()
	expected := []postgresDeleteJournalColumn{
		{name: "journal_version", dataType: "smallint", notNull: true},
		{name: "token", dataType: "text", notNull: true},
		{name: "plan_id", dataType: "text", notNull: true},
		{name: "sequence", dataType: "bigint", notNull: true},
		{name: "batch_digest", dataType: "text", notNull: true},
		{name: "candidates", dataType: "bigint", notNull: true},
		{name: "deleted_rows", dataType: "bigint", notNull: true},
		{name: "target_relation_oid", dataType: "bigint", notNull: true},
		{name: "target_catalog_digest", dataType: "text", notNull: true},
		{name: "receipt_digest", dataType: "text", notNull: true},
	}
	index := 0
	for rows.Next() {
		var column postgresDeleteJournalColumn
		if err := rows.Scan(
			&column.name,
			&column.dataType,
			&column.notNull,
			&column.hasDefault,
			&column.identity,
			&column.generated,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL delete receipt column: %w",
				err,
			)
		}
		if index >= len(expected) || column != expected[index] {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal columns differ from version %d",
				postgresDeleteJournalVersion,
			)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != len(expected) {
		return fmt.Errorf(
			"PostgreSQL delete receipt journal columns differ from version %d",
			postgresDeleteJournalVersion,
		)
	}
	return nil
}

type postgresDeleteJournalConstraint struct {
	name        string
	constraint  string
	definition  string
	validated   bool
	deferrable  bool
	deferred    bool
	noInherit   bool
	local       bool
	inheritance int
	parent      bool
	indexOID    int64
}

func normalizePostgresDeleteDefinition(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func postgresDeleteJournalExpectedConstraints() map[string]string {
	return map[string]string{
		"delete_batch_receipts_pkey":                        "PRIMARY KEY (token)",
		"delete_batch_receipts_journal_version_check":       "CHECK (journal_version = 1)",
		"delete_batch_receipts_token_check":                 "CHECK (token ~ '^[0-9a-f]{64}$'::text)",
		"delete_batch_receipts_plan_id_check":               "CHECK (plan_id ~ '^[0-9a-f]{32}$'::text)",
		"delete_batch_receipts_sequence_check":              "CHECK (sequence >= 0)",
		"delete_batch_receipts_batch_digest_check":          "CHECK (batch_digest ~ '^[0-9a-f]{64}$'::text)",
		"delete_batch_receipts_candidates_check":            "CHECK (candidates > 0)",
		"delete_batch_receipts_deleted_rows_check":          "CHECK (deleted_rows >= 0 AND deleted_rows <= candidates)",
		"delete_batch_receipts_target_relation_oid_check":   "CHECK (target_relation_oid > 0)",
		"delete_batch_receipts_target_catalog_digest_check": "CHECK (target_catalog_digest ~ '^[0-9a-f]{64}$'::text)",
		"delete_batch_receipts_receipt_digest_check":        "CHECK (receipt_digest ~ '^[0-9a-f]{64}$'::text)",
	}
}

func validatePostgresDeleteJournalConstraints(
	ctx context.Context,
	queryer postgresDeleteJournalQueryer,
	relationOID int64,
) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			constraint_object.conname,
			constraint_object.contype::text,
			pg_catalog.pg_get_constraintdef(
				constraint_object.oid,
				true
			),
			constraint_object.convalidated,
			constraint_object.condeferrable,
			constraint_object.condeferred,
			constraint_object.connoinherit,
			constraint_object.conislocal,
			constraint_object.coninhcount,
			COALESCE(
				(to_jsonb(constraint_object)->>'conparentid')::oid,
				0
			) <> 0,
			constraint_object.conindid::bigint
		FROM pg_catalog.pg_constraint AS constraint_object
		WHERE constraint_object.conrelid = $1
		ORDER BY constraint_object.conname
	`, relationOID)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL delete receipt constraints: %w",
			err,
		)
	}
	defer rows.Close()
	expected := postgresDeleteJournalExpectedConstraints()
	seen := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var constraint postgresDeleteJournalConstraint
		if err := rows.Scan(
			&constraint.name,
			&constraint.constraint,
			&constraint.definition,
			&constraint.validated,
			&constraint.deferrable,
			&constraint.deferred,
			&constraint.noInherit,
			&constraint.local,
			&constraint.inheritance,
			&constraint.parent,
			&constraint.indexOID,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL delete receipt constraint: %w",
				err,
			)
		}
		definition, exists := expected[constraint.name]
		if !exists {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal has an unexpected constraint %s",
				constraint.name,
			)
		}
		if _, duplicate := seen[constraint.name]; duplicate {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal repeats constraint %s",
				constraint.name,
			)
		}
		seen[constraint.name] = struct{}{}
		isPrimary := constraint.name == "delete_batch_receipts_pkey"
		if normalizePostgresDeleteDefinition(constraint.definition) !=
			normalizePostgresDeleteDefinition(definition) ||
			!constraint.validated ||
			constraint.deferrable ||
			constraint.deferred ||
			!constraint.local ||
			constraint.inheritance != 0 ||
			constraint.parent ||
			(isPrimary && (constraint.constraint != "p" ||
				!constraint.noInherit ||
				constraint.indexOID <= 0)) ||
			(!isPrimary && (constraint.constraint != "c" ||
				constraint.noInherit ||
				constraint.indexOID != 0)) {
			return fmt.Errorf(
				"PostgreSQL delete receipt constraint %s differs from version %d",
				constraint.name,
				postgresDeleteJournalVersion,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(seen) != len(expected) {
		return fmt.Errorf(
			"PostgreSQL delete receipt journal lacks version %d constraints",
			postgresDeleteJournalVersion,
		)
	}
	return nil
}

func validatePostgresDeleteJournalIndex(
	ctx context.Context,
	queryer postgresDeleteJournalQueryer,
	relationOID int64,
) error {
	if err := validatePostgresDeleteJournalIndexInventory(
		ctx,
		queryer,
		relationOID,
	); err != nil {
		return err
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			index_relation.relname,
			access_method.amname,
			index_metadata.indisprimary,
			index_metadata.indisunique,
			index_metadata.indisvalid,
			index_metadata.indisready,
			index_metadata.indislive,
			index_metadata.indnullsnotdistinct,
			index_metadata.indnkeyatts,
			index_metadata.indnatts,
			index_metadata.indpred IS NOT NULL,
			index_metadata.indexprs IS NOT NULL,
			key_column.position,
			attribute.attname,
			key_column.index_option,
			opclass_namespace.nspname,
			operator_class.opcname,
			operator_class.opcdefault,
			COALESCE(collation_object.oid::bigint, 0),
			COALESCE(collation_object.collisdeterministic, false),
			COALESCE(collation_object.collversion, '') =
				COALESCE(
					pg_catalog.pg_collation_actual_version(
						collation_object.oid
					),
					''
				)
		FROM pg_catalog.pg_index AS index_metadata
		JOIN pg_catalog.pg_class AS index_relation
		  ON index_relation.oid = index_metadata.indexrelid
		JOIN pg_catalog.pg_am AS access_method
		  ON access_method.oid = index_relation.relam
		CROSS JOIN LATERAL unnest(
			index_metadata.indkey::smallint[],
			index_metadata.indclass::oid[],
			index_metadata.indcollation::oid[],
			index_metadata.indoption::smallint[]
		) WITH ORDINALITY AS key_column(
			attnum,
			opclass_oid,
			collation_oid,
			index_option,
			position
		)
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = index_metadata.indrelid
		 AND attribute.attnum = key_column.attnum
		JOIN pg_catalog.pg_opclass AS operator_class
		  ON operator_class.oid = key_column.opclass_oid
		JOIN pg_catalog.pg_namespace AS opclass_namespace
		  ON opclass_namespace.oid = operator_class.opcnamespace
		LEFT JOIN pg_catalog.pg_collation AS collation_object
		  ON collation_object.oid = key_column.collation_oid
		WHERE index_metadata.indrelid = $1
		ORDER BY index_relation.oid, key_column.position
	`, relationOID)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL delete receipt index: %w",
			err,
		)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var (
			name, method, column, opclassNamespace, opclass string
			primary, unique, valid, ready, live             bool
			nullsNotDistinct, partial, expression           bool
			defaultOpclass, deterministic, versionCurrent   bool
			keyColumns, totalColumns, position, option      int
			collationOID                                    int64
		)
		if err := rows.Scan(
			&name,
			&method,
			&primary,
			&unique,
			&valid,
			&ready,
			&live,
			&nullsNotDistinct,
			&keyColumns,
			&totalColumns,
			&partial,
			&expression,
			&position,
			&column,
			&option,
			&opclassNamespace,
			&opclass,
			&defaultOpclass,
			&collationOID,
			&deterministic,
			&versionCurrent,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL delete receipt index: %w",
				err,
			)
		}
		count++
		if count != 1 ||
			name != "delete_batch_receipts_pkey" ||
			method != "btree" ||
			!primary ||
			!unique ||
			!valid ||
			!ready ||
			!live ||
			nullsNotDistinct ||
			keyColumns != 1 ||
			totalColumns != 1 ||
			partial ||
			expression ||
			position != 1 ||
			column != "token" ||
			option != 0 ||
			opclassNamespace != "pg_catalog" ||
			opclass != "text_ops" ||
			!defaultOpclass ||
			collationOID <= 0 ||
			!deterministic ||
			!versionCurrent {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal index differs from version %d",
				postgresDeleteJournalVersion,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf(
			"PostgreSQL delete receipt journal must have exactly one versioned primary index",
		)
	}
	return nil
}

// validatePostgresDeleteJournalIndexInventory enumerates pg_index directly,
// before any attribute joins can hide expression keys whose indkey entry is
// zero. The receipt relation has one exact primary index and no other physical
// index shape.
func validatePostgresDeleteJournalIndexInventory(
	ctx context.Context,
	queryer postgresDeleteJournalQueryer,
	relationOID int64,
) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			index_metadata.indexrelid::bigint,
			index_metadata.indpred IS NOT NULL,
			index_metadata.indexprs IS NOT NULL
		FROM pg_catalog.pg_index AS index_metadata
		WHERE index_metadata.indrelid = $1
		ORDER BY index_metadata.indexrelid
	`, relationOID)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL delete receipt index inventory: %w",
			err,
		)
	}
	defer rows.Close()
	seen := make(map[int64]struct{})
	count := 0
	for rows.Next() {
		var (
			indexOID            int64
			partial, expression bool
		)
		if err := rows.Scan(
			&indexOID,
			&partial,
			&expression,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL delete receipt index inventory: %w",
				err,
			)
		}
		if indexOID <= 0 {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal has an invalid index identity",
			)
		}
		if _, duplicate := seen[indexOID]; duplicate {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal repeats index identity %d",
				indexOID,
			)
		}
		seen[indexOID] = struct{}{}
		count++
		if partial || expression {
			return fmt.Errorf(
				"PostgreSQL delete receipt journal has an unexpected expression or partial index",
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate PostgreSQL delete receipt index inventory: %w",
			err,
		)
	}
	if count != 1 {
		return fmt.Errorf(
			"PostgreSQL delete receipt journal must have exactly one versioned primary index",
		)
	}
	return nil
}

func preflightPostgresDeleteReceiptJournal(
	ctx context.Context,
	database *sql.DB,
) error {
	if database == nil {
		return fmt.Errorf(
			"PostgreSQL delete receipt database is unavailable",
		)
	}
	_, err := inspectPostgresDeleteReceiptJournal(ctx, database)
	return err
}

func ensurePostgresDeleteReceiptJournal(
	ctx context.Context,
	tx *sql.Tx,
) error {
	if tx == nil {
		return fmt.Errorf(
			"PostgreSQL delete receipt transaction is unavailable",
		)
	}
	tableExists, err := lockPostgresDeleteReceiptJournal(
		ctx,
		tx,
	)
	if err != nil {
		return err
	}
	if tableExists {
		catalog, err := inspectPostgresDeleteReceiptJournal(ctx, tx)
		if err != nil {
			return err
		}
		if !catalog.schemaExists || !catalog.tableExists {
			return fmt.Errorf(
				"locked PostgreSQL delete receipt journal disappeared",
			)
		}
		return nil
	}
	catalog, err := inspectPostgresDeleteReceiptJournal(ctx, tx)
	if err != nil {
		return err
	}
	if !catalog.schemaExists {
		if _, err := tx.ExecContext(
			ctx,
			"CREATE SCHEMA "+
				postgresIdentifier(postgresDeleteJournalSchema),
		); err != nil {
			return fmt.Errorf(
				"create private PostgreSQL delete receipt schema: %w",
				err,
			)
		}
		if _, err := tx.ExecContext(
			ctx,
			"REVOKE ALL ON SCHEMA "+
				postgresIdentifier(postgresDeleteJournalSchema)+
				" FROM PUBLIC",
		); err != nil {
			return fmt.Errorf(
				"make PostgreSQL delete receipt schema private: %w",
				err,
			)
		}
	}
	if !catalog.tableExists {
		if _, err := tx.ExecContext(ctx, `
			CREATE TABLE `+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		)+` (
				journal_version smallint NOT NULL,
				token text NOT NULL,
				plan_id text NOT NULL,
				sequence bigint NOT NULL,
				batch_digest text NOT NULL,
				candidates bigint NOT NULL,
				deleted_rows bigint NOT NULL,
				target_relation_oid bigint NOT NULL,
				target_catalog_digest text NOT NULL,
				receipt_digest text NOT NULL,
				CONSTRAINT delete_batch_receipts_pkey
					PRIMARY KEY (token),
				CONSTRAINT delete_batch_receipts_journal_version_check
					CHECK (journal_version = 1),
				CONSTRAINT delete_batch_receipts_token_check
					CHECK (token ~ '^[0-9a-f]{64}$'),
				CONSTRAINT delete_batch_receipts_plan_id_check
					CHECK (plan_id ~ '^[0-9a-f]{32}$'),
				CONSTRAINT delete_batch_receipts_sequence_check
					CHECK (sequence >= 0),
				CONSTRAINT delete_batch_receipts_batch_digest_check
					CHECK (batch_digest ~ '^[0-9a-f]{64}$'),
				CONSTRAINT delete_batch_receipts_candidates_check
					CHECK (candidates > 0),
				CONSTRAINT delete_batch_receipts_deleted_rows_check
					CHECK (
						deleted_rows >= 0 AND
						deleted_rows <= candidates
					),
				CONSTRAINT delete_batch_receipts_target_relation_oid_check
					CHECK (target_relation_oid > 0),
				CONSTRAINT delete_batch_receipts_target_catalog_digest_check
					CHECK (
						target_catalog_digest ~
						'^[0-9a-f]{64}$'
					),
				CONSTRAINT delete_batch_receipts_receipt_digest_check
					CHECK (receipt_digest ~ '^[0-9a-f]{64}$')
			)
		`); err != nil {
			return fmt.Errorf(
				"create PostgreSQL delete receipt journal: %w",
				err,
			)
		}
		if _, err := tx.ExecContext(
			ctx,
			"REVOKE ALL ON TABLE "+
				postgresQualified(
					postgresDeleteJournalSchema,
					postgresDeleteJournalTable,
				)+
				" FROM PUBLIC",
		); err != nil {
			return fmt.Errorf(
				"make PostgreSQL delete receipt journal private: %w",
				err,
			)
		}
	}
	catalog, err = inspectPostgresDeleteReceiptJournal(ctx, tx)
	if err != nil {
		return err
	}
	if !catalog.schemaExists || !catalog.tableExists {
		return fmt.Errorf(
			"PostgreSQL delete receipt journal was not created with its exact versioned shape",
		)
	}
	return nil
}

type postgresDeleteSQLStateError interface {
	SQLState() string
}

func lockPostgresDeleteReceiptJournal(
	ctx context.Context,
	tx *sql.Tx,
) (bool, error) {
	const savepoint = "dmtx_delete_journal_probe"
	if _, err := tx.ExecContext(
		ctx,
		"SAVEPOINT "+savepoint,
	); err != nil {
		return false, fmt.Errorf(
			"open PostgreSQL delete journal lock probe: %w",
			err,
		)
	}
	_, lockErr := tx.ExecContext(
		ctx,
		"LOCK TABLE "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" IN ACCESS EXCLUSIVE MODE",
	)
	if lockErr == nil {
		if _, err := tx.ExecContext(
			ctx,
			"RELEASE SAVEPOINT "+savepoint,
		); err != nil {
			return false, fmt.Errorf(
				"release PostgreSQL delete journal lock probe: %w",
				err,
			)
		}
		return true, nil
	}
	_, rollbackErr := tx.ExecContext(
		ctx,
		"ROLLBACK TO SAVEPOINT "+savepoint,
	)
	_, releaseErr := tx.ExecContext(
		ctx,
		"RELEASE SAVEPOINT "+savepoint,
	)
	var sqlState postgresDeleteSQLStateError
	missing := errors.As(lockErr, &sqlState) &&
		(sqlState.SQLState() == "42P01" ||
			sqlState.SQLState() == "3F000")
	if missing && rollbackErr == nil && releaseErr == nil {
		return false, nil
	}
	return false, fmt.Errorf(
		"lock exact PostgreSQL delete receipt journal: %w",
		errors.Join(lockErr, rollbackErr, releaseErr),
	)
}

type postgresDeleteReceiptDigestInput struct {
	JournalVersion      int16  `json:"journal_version"`
	PlanID              string `json:"plan_id"`
	Token               string `json:"token"`
	Sequence            int64  `json:"sequence"`
	BatchDigest         string `json:"batch_digest"`
	Candidates          int64  `json:"candidates"`
	DeletedRows         int64  `json:"deleted_rows"`
	TargetRelationOID   int64  `json:"target_relation_oid"`
	TargetCatalogDigest string `json:"target_catalog_digest"`
}

func validatePostgresDeleteReceiptAuthority(
	authority postgresDeleteCatalogAuthority,
) error {
	if authority.RelationOID <= 0 {
		return fmt.Errorf(
			"PostgreSQL delete receipt target relation OID is unavailable",
		)
	}
	expected, err := postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		return err
	}
	if err := validateLowerSHA256(
		"PostgreSQL delete receipt target catalog digest",
		authority.CatalogDigest,
	); err != nil || authority.CatalogDigest != expected {
		return fmt.Errorf(
			"PostgreSQL delete receipt target catalog authority differs",
		)
	}
	return nil
}

func postgresDeleteReceiptDigest(
	receipt deleteTargetBatchReceipt,
	authority postgresDeleteCatalogAuthority,
) (string, error) {
	if err := validatePostgresDeleteReceiptAuthority(authority); err != nil {
		return "", err
	}
	payload, err := json.Marshal(postgresDeleteReceiptDigestInput{
		JournalVersion:      postgresDeleteJournalVersion,
		PlanID:              receipt.PlanID,
		Token:               receipt.Token,
		Sequence:            receipt.Sequence,
		BatchDigest:         receipt.BatchDigest,
		Candidates:          receipt.Candidates,
		DeletedRows:         receipt.DeletedRows,
		TargetRelationOID:   authority.RelationOID,
		TargetCatalogDigest: authority.CatalogDigest,
	})
	if err != nil {
		return "", fmt.Errorf(
			"encode PostgreSQL delete receipt digest: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validatePostgresDeleteReceipt(
	batch deleteTargetBatch,
	receipt deleteTargetBatchReceipt,
	authority postgresDeleteCatalogAuthority,
) error {
	if receipt.PlanID != batch.PlanID ||
		receipt.Token != batch.Token ||
		receipt.Sequence != batch.Sequence ||
		receipt.BatchDigest != batch.BatchDigest ||
		receipt.Candidates != int64(len(batch.Keys)) ||
		receipt.DeletedRows < 0 ||
		receipt.DeletedRows > receipt.Candidates ||
		receipt.FailClosedReason != "" {
		return fmt.Errorf(
			"PostgreSQL delete receipt differs from the requested batch",
		)
	}
	expected, err := postgresDeleteReceiptDigest(receipt, authority)
	if err != nil {
		return err
	}
	if receipt.ReceiptDigest != expected {
		return fmt.Errorf(
			"PostgreSQL delete receipt digest differs",
		)
	}
	return nil
}

func loadPostgresDeleteReceipt(
	ctx context.Context,
	tx *sql.Tx,
	token string,
	authority postgresDeleteCatalogAuthority,
) (deleteTargetBatchReceipt, bool, error) {
	var (
		version             int16
		targetRelationOID   int64
		targetCatalogDigest string
		receipt             deleteTargetBatchReceipt
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			journal_version,
			plan_id,
			token,
			sequence,
			batch_digest,
			candidates,
			deleted_rows,
			target_relation_oid,
			target_catalog_digest,
			receipt_digest
		FROM `+postgresQualified(
		postgresDeleteJournalSchema,
		postgresDeleteJournalTable,
	)+`
		WHERE token = $1
	`, token).Scan(
		&version,
		&receipt.PlanID,
		&receipt.Token,
		&receipt.Sequence,
		&receipt.BatchDigest,
		&receipt.Candidates,
		&receipt.DeletedRows,
		&targetRelationOID,
		&targetCatalogDigest,
		&receipt.ReceiptDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deleteTargetBatchReceipt{}, false, nil
	}
	if err != nil {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf(
			"read PostgreSQL delete receipt: %w",
			err,
		)
	}
	if version != postgresDeleteJournalVersion ||
		targetRelationOID != authority.RelationOID ||
		targetCatalogDigest != authority.CatalogDigest {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf(
			"PostgreSQL delete receipt belongs to different versioned target authority",
		)
	}
	return receipt, true, nil
}

func insertPostgresDeleteReceipt(
	ctx context.Context,
	tx *sql.Tx,
	receipt deleteTargetBatchReceipt,
	authority postgresDeleteCatalogAuthority,
) error {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO `+postgresQualified(
		postgresDeleteJournalSchema,
		postgresDeleteJournalTable,
	)+` (
			journal_version,
			token,
			plan_id,
			sequence,
			batch_digest,
			candidates,
			deleted_rows,
			target_relation_oid,
			target_catalog_digest,
			receipt_digest
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10
		)
	`, postgresDeleteJournalVersion, receipt.Token, receipt.PlanID,
		receipt.Sequence, receipt.BatchDigest, receipt.Candidates,
		receipt.DeletedRows, authority.RelationOID,
		authority.CatalogDigest, receipt.ReceiptDigest)
	if err != nil {
		return fmt.Errorf(
			"write PostgreSQL delete receipt: %w",
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf(
			"verify PostgreSQL delete receipt write: affected=%d err=%w",
			affected,
			err,
		)
	}
	return nil
}

func postgresDeleteAdvisoryLockKey(token string) (int64, error) {
	decoded, err := hex.DecodeString(token)
	if err != nil || len(decoded) != sha256.Size {
		return 0, fmt.Errorf(
			"PostgreSQL delete token is not a SHA-256 digest",
		)
	}
	return int64(binary.BigEndian.Uint64(decoded[:8])), nil
}

func flattenPostgresDeleteArguments(
	keys [][]driver.Value,
) []any {
	count := 0
	for _, key := range keys {
		count += len(key)
	}
	arguments := make([]any, 0, count)
	for _, key := range keys {
		for _, value := range key {
			arguments = append(arguments, value)
		}
	}
	return arguments
}

func validatePostgresDeleteBatchAuthority(
	batch deleteTargetBatch,
	authority postgresDeleteCatalogAuthority,
) error {
	if err := validatePostgresDeleteReceiptAuthority(authority); err != nil {
		return err
	}
	sameShape, err := samePostgresDeleteStableTableShape(
		batch.Table,
		authority.TableShape,
	)
	if err != nil {
		return fmt.Errorf(
			"compare PostgreSQL delete batch table authority: %w",
			err,
		)
	}
	if !sameShape {
		return fmt.Errorf(
			"PostgreSQL delete batch table differs from admitted target catalog authority",
		)
	}
	if len(batch.Columns) != len(authority.PrimaryKey) {
		return fmt.Errorf(
			"PostgreSQL delete batch key width differs from the admitted target primary key",
		)
	}
	for index := range authority.PrimaryKey {
		if batch.Columns[index] != authority.PrimaryKey[index].Name {
			return fmt.Errorf(
				"PostgreSQL delete batch columns are not in exact admitted primary-key order",
			)
		}
	}
	return nil
}

func (capability *postgresDeleteTargetCapability) ApplyDeleteBatch(
	ctx context.Context,
	batch deleteTargetBatch,
) (deleteTargetBatchReceipt, error) {
	if capability == nil ||
		capability.adapter == nil ||
		capability.adapter.database == nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL delete target is unavailable",
		)
	}
	adapter := capability.adapter
	if adapter.namespace == postgresDeleteJournalSchema {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL target namespace %s is reserved for DMTX receipt evidence",
			postgresDeleteJournalSchema,
		)
	}
	keys, err := validatePostgresDeleteBatch(adapter.namespace, batch)
	if err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	if err := validatePostgresDeleteBatchAuthority(
		batch,
		capability.authority,
	); err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	statement, err := postgresDeleteBatchStatement(
		batch.Table,
		batch.Columns,
		len(keys),
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	lockKey, err := postgresDeleteAdvisoryLockKey(batch.Token)
	if err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	tx, err := adapter.database.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"begin PostgreSQL delete receipt transaction: %w",
			err,
		)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(
		ctx,
		"LOCK TABLE "+
			postgresQualified(
				batch.Table.Schema,
				batch.Table.Name,
			)+
			" IN SHARE ROW EXCLUSIVE MODE",
	); err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"lock PostgreSQL delete target before mutation; no delete or receipt was committed, repair the target and resume this existing run and attempt with the same pending batch token: %w",
			err,
		)
	}
	lockedAuthority, err := inspectPostgresDeleteCatalogAuthority(
		ctx,
		tx,
		adapter.namespace,
		capability.authority.TableShape,
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"revalidate locked PostgreSQL delete target catalog; no delete or receipt was committed, repair the target and resume this existing run and attempt with the same pending batch token: %w",
			err,
		)
	}
	if !lockedAuthority.CanDelete ||
		!samePostgresDeleteCatalogAuthority(
			capability.authority,
			lockedAuthority,
		) {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"locked PostgreSQL delete target authority changed; no delete or receipt was committed, restore the admitted relation and resume this existing run and attempt with the same pending batch token",
		)
	}
	if err := ensurePostgresDeleteReceiptJournal(ctx, tx); err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"prepare exact private PostgreSQL delete receipt journal; no delete or receipt was committed, repair the journal and resume this existing run and attempt with the same pending batch token: %w",
			err,
		)
	}
	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_catalog.pg_advisory_xact_lock($1)`,
		lockKey,
	); err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"lock PostgreSQL delete batch token; no delete or receipt was committed, resume this existing run and attempt with the same pending batch token: %w",
			err,
		)
	}
	stored, found, err := loadPostgresDeleteReceipt(
		ctx,
		tx,
		batch.Token,
		capability.authority,
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	if found {
		if err := validatePostgresDeleteReceipt(
			batch,
			stored,
			capability.authority,
		); err != nil {
			return deleteTargetBatchReceipt{}, err
		}
		if err := tx.Commit(); err != nil {
			return deleteTargetBatchReceipt{}, fmt.Errorf(
				"confirm replayed PostgreSQL delete receipt: %w",
				err,
			)
		}
		return stored, nil
	}
	result, err := tx.ExecContext(
		ctx,
		statement,
		flattenPostgresDeleteArguments(keys)...,
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL delete batch failed atomically and no receipt was published; the pending batch token remains durable, repair the target and resume this existing run and attempt with the same token: %w",
			err,
		)
	}
	deletedRows, err := result.RowsAffected()
	if err != nil ||
		deletedRows < 0 ||
		deletedRows > int64(len(keys)) {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL delete batch returned an unsafe affected-row count; the delete and receipt were rolled back, and the pending batch token must be resumed in this existing run and attempt: affected=%d err=%w",
			deletedRows,
			err,
		)
	}
	receipt := deleteTargetBatchReceipt{
		PlanID:      batch.PlanID,
		Token:       batch.Token,
		Sequence:    batch.Sequence,
		BatchDigest: batch.BatchDigest,
		Candidates:  int64(len(keys)),
		DeletedRows: deletedRows,
	}
	receipt.ReceiptDigest, err = postgresDeleteReceiptDigest(
		receipt,
		capability.authority,
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, err
	}
	if err := insertPostgresDeleteReceipt(
		ctx,
		tx,
		receipt,
		capability.authority,
	); err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL delete batch and receipt were rolled back with no replayable receipt; repair the exact private journal and resume this existing run and attempt with the same pending batch token: %w",
			err,
		)
	}
	if err := tx.Commit(); err != nil {
		return deleteTargetBatchReceipt{}, fmt.Errorf(
			"PostgreSQL delete commit outcome is unknown; resume this existing run and attempt with the same batch token so the exact durable receipt can resolve it: %w",
			err,
		)
	}
	return receipt, nil
}

var _ deleteKeyTarget = (*postgresDeleteTargetCapability)(nil)
