package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresDeleteIndexKeyAuthority struct {
	Position               int    `json:"position"`
	Column                 string `json:"column"`
	OperatorClassNamespace string `json:"operator_class_namespace"`
	OperatorClass          string `json:"operator_class"`
	CollationOID           int64  `json:"collation_oid"`
	CollationNamespace     string `json:"collation_namespace"`
	Collation              string `json:"collation"`
	CollationProvider      string `json:"collation_provider"`
	CollationVersion       string `json:"collation_version"`
	CollationDeterministic bool   `json:"collation_deterministic"`
}

type postgresDeleteCatalogAuthority struct {
	ServerAddress    string                            `json:"server_address"`
	ServerPort       int                               `json:"server_port"`
	SystemIdentifier string                            `json:"system_identifier"`
	CurrentUser      string                            `json:"current_user"`
	DatabaseOID      int64                             `json:"database_oid"`
	SchemaOwnerOID   int64                             `json:"schema_owner_oid"`
	SchemaOID        int64                             `json:"schema_oid"`
	RelationOwnerOID int64                             `json:"relation_owner_oid"`
	RelationOID      int64                             `json:"relation_oid"`
	ConstraintOID    int64                             `json:"constraint_oid"`
	IndexOID         int64                             `json:"index_oid"`
	Database         string                            `json:"database"`
	Schema           string                            `json:"schema"`
	Table            string                            `json:"table"`
	Constraint       string                            `json:"constraint"`
	TableShape       schema.Table                      `json:"table_shape"`
	PrimaryKey       []schema.Column                   `json:"primary_key"`
	IndexKeys        []postgresDeleteIndexKeyAuthority `json:"index_keys"`
	CatalogDigest    string                            `json:"-"`
	CanSelect        bool                              `json:"-"`
	CanDelete        bool                              `json:"-"`
	HasSchemaUsage   bool                              `json:"-"`
	ServerEncoding   string                            `json:"server_encoding"`
	ServerVersion    int                               `json:"server_version"`
}

type postgresDeleteAuthorityDigest struct {
	SystemIdentifier string                            `json:"system_identifier"`
	CurrentUser      string                            `json:"current_user"`
	DatabaseOID      int64                             `json:"database_oid"`
	SchemaOwnerOID   int64                             `json:"schema_owner_oid"`
	SchemaOID        int64                             `json:"schema_oid"`
	RelationOwnerOID int64                             `json:"relation_owner_oid"`
	RelationOID      int64                             `json:"relation_oid"`
	ConstraintOID    int64                             `json:"constraint_oid"`
	IndexOID         int64                             `json:"index_oid"`
	Database         string                            `json:"database"`
	Schema           string                            `json:"schema"`
	Table            string                            `json:"table"`
	Constraint       string                            `json:"constraint"`
	TableShape       json.RawMessage                   `json:"table_shape"`
	PrimaryKey       []schema.Column                   `json:"primary_key"`
	IndexKeys        []postgresDeleteIndexKeyAuthority `json:"index_keys"`
	CanSelect        bool                              `json:"can_select"`
	CanDelete        bool                              `json:"can_delete"`
	HasSchemaUsage   bool                              `json:"has_schema_usage"`
	ServerEncoding   string                            `json:"server_encoding"`
	ServerVersion    int                               `json:"server_version"`
}

func postgresDeleteAuthorityDigestValue(
	authority postgresDeleteCatalogAuthority,
) (string, error) {
	tableShape, err := postgresDeleteStableTableEvidence(
		authority.TableShape,
	)
	if err != nil {
		return "", fmt.Errorf(
			"encode PostgreSQL delete stable table authority: %w",
			err,
		)
	}
	payload, err := json.Marshal(postgresDeleteAuthorityDigest{
		SystemIdentifier: authority.SystemIdentifier,
		CurrentUser:      authority.CurrentUser,
		DatabaseOID:      authority.DatabaseOID,
		SchemaOwnerOID:   authority.SchemaOwnerOID,
		SchemaOID:        authority.SchemaOID,
		RelationOwnerOID: authority.RelationOwnerOID,
		RelationOID:      authority.RelationOID,
		ConstraintOID:    authority.ConstraintOID,
		IndexOID:         authority.IndexOID,
		Database:         authority.Database,
		Schema:           authority.Schema,
		Table:            authority.Table,
		Constraint:       authority.Constraint,
		TableShape:       tableShape,
		PrimaryKey:       authority.PrimaryKey,
		IndexKeys:        authority.IndexKeys,
		CanSelect:        authority.CanSelect,
		CanDelete:        authority.CanDelete,
		HasSchemaUsage:   authority.HasSchemaUsage,
		ServerEncoding:   authority.ServerEncoding,
		ServerVersion:    authority.ServerVersion,
	})
	if err != nil {
		return "", fmt.Errorf(
			"encode PostgreSQL delete catalog authority: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// postgresDeleteStableTableEvidence is the structural catalog boundary for a
// durable candidate plan and its target receipts. The canonical schema
// snapshot preserves defaults and CHECK expressions but deliberately excludes
// the identity sequence frontier, which is mutable data-plane state rather
// than schema authority.
func postgresDeleteStableTableEvidence(
	table schema.Table,
) (json.RawMessage, error) {
	snapshot, err := schema.NewSchemaSnapshot([]schema.Table{table})
	if err != nil {
		return nil, err
	}
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		return nil, err
	}
	return json.RawMessage(append([]byte(nil), encoded...)), nil
}

func samePostgresDeleteStableTableShape(
	left schema.Table,
	right schema.Table,
) (bool, error) {
	leftEvidence, err := postgresDeleteStableTableEvidence(left)
	if err != nil {
		return false, err
	}
	rightEvidence, err := postgresDeleteStableTableEvidence(right)
	if err != nil {
		return false, err
	}
	return string(leftEvidence) == string(rightEvidence), nil
}

func inspectPostgresDeleteCatalogAuthority(
	ctx context.Context,
	queryer engine.PostgresCatalogQueryer,
	namespace string,
	expected schema.Table,
) (postgresDeleteCatalogAuthority, error) {
	if queryer == nil || strings.TrimSpace(namespace) == "" ||
		expected.Schema != namespace ||
		strings.TrimSpace(expected.Name) == "" {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"PostgreSQL delete catalog identity is incomplete",
		)
	}
	live, err := engine.InspectPostgresTableWithQueryer(
		ctx,
		queryer,
		namespace,
		expected.Name,
	)
	if err != nil {
		return postgresDeleteCatalogAuthority{}, err
	}
	sameShape, err := samePostgresDeleteStableTableShape(live, expected)
	if err != nil {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"compare PostgreSQL delete stable catalog shape: %w",
			err,
		)
	}
	if !sameShape {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"PostgreSQL delete catalog shape changed after discovery",
		)
	}
	expectedKey, err := deletePrimaryKeyColumns(expected)
	if err != nil {
		return postgresDeleteCatalogAuthority{}, err
	}
	liveKey, err := deletePrimaryKeyColumns(live)
	if err != nil {
		return postgresDeleteCatalogAuthority{}, err
	}
	if !reflect.DeepEqual(expectedKey, liveKey) {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"PostgreSQL delete primary-key catalog changed after discovery",
		)
	}
	var authority postgresDeleteCatalogAuthority
	var (
		relationKind         string
		relationPersistence  string
		relationAccessMethod string
		isPartition          bool
		hasSubclass          bool
		rowSecurity          bool
		forceRowSecurity     bool
		parentCount          int
		childCount           int
		userTriggers         int
		rules                int
		mutatingIncomingFKs  int
	)
	err = queryer.QueryRowContext(ctx, `
		SELECT
			COALESCE(
				pg_catalog.inet_server_addr()::text,
				'local-socket'
				),
				COALESCE(pg_catalog.inet_server_port(), 0),
				(
					SELECT control.system_identifier::text
					FROM pg_catalog.pg_control_system() AS control
				),
				CURRENT_USER,
			current_database(),
			(
				SELECT database_object.oid::bigint
				FROM pg_catalog.pg_database AS database_object
				WHERE database_object.datname = current_database()
			),
			table_namespace.nspowner::bigint,
			table_namespace.oid::bigint,
			relation.relowner::bigint,
			relation.oid::bigint,
			relation.relkind::text,
			relation.relpersistence::text,
			(
				SELECT access_method.amname
				FROM pg_catalog.pg_am AS access_method
				WHERE access_method.oid = relation.relam
			),
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
				FROM pg_catalog.pg_constraint AS incoming_fk
				WHERE incoming_fk.confrelid = relation.oid
				  AND incoming_fk.contype = 'f'
				  AND incoming_fk.confdeltype NOT IN ('a', 'r')
			),
			pg_catalog.has_schema_privilege(
				CURRENT_USER,
				table_namespace.oid,
				'USAGE'
			),
			pg_catalog.has_table_privilege(
				CURRENT_USER,
				relation.oid,
				'SELECT'
			),
			pg_catalog.has_table_privilege(
				CURRENT_USER,
				relation.oid,
				'DELETE'
			),
			current_setting('server_encoding'),
			current_setting('server_version_num')::integer
		FROM pg_catalog.pg_namespace AS table_namespace
		JOIN pg_catalog.pg_class AS relation
		  ON relation.relnamespace = table_namespace.oid
		WHERE table_namespace.nspname = $1
		  AND relation.relname = $2
	`, namespace, expected.Name).Scan(
		&authority.ServerAddress,
		&authority.ServerPort,
		&authority.SystemIdentifier,
		&authority.CurrentUser,
		&authority.Database,
		&authority.DatabaseOID,
		&authority.SchemaOwnerOID,
		&authority.SchemaOID,
		&authority.RelationOwnerOID,
		&authority.RelationOID,
		&relationKind,
		&relationPersistence,
		&relationAccessMethod,
		&isPartition,
		&hasSubclass,
		&rowSecurity,
		&forceRowSecurity,
		&parentCount,
		&childCount,
		&userTriggers,
		&rules,
		&mutatingIncomingFKs,
		&authority.HasSchemaUsage,
		&authority.CanSelect,
		&authority.CanDelete,
		&authority.ServerEncoding,
		&authority.ServerVersion,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"PostgreSQL delete table disappeared during catalog admission",
		)
	}
	if err != nil {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"inspect PostgreSQL delete relation authority: %w",
			err,
		)
	}
	if relationKind != "r" ||
		relationPersistence != "p" ||
		relationAccessMethod != "heap" ||
		isPartition ||
		hasSubclass ||
		rowSecurity ||
		forceRowSecurity ||
		parentCount != 0 ||
		childCount != 0 ||
		userTriggers != 0 ||
		rules != 0 ||
		mutatingIncomingFKs != 0 ||
		strings.TrimSpace(authority.ServerAddress) == "" ||
		strings.TrimSpace(authority.SystemIdentifier) == "" ||
		strings.TrimSpace(authority.CurrentUser) == "" ||
		strings.TrimSpace(authority.Database) == "" ||
		authority.DatabaseOID <= 0 ||
		authority.SchemaOwnerOID <= 0 ||
		authority.RelationOwnerOID <= 0 ||
		!authority.HasSchemaUsage ||
		!authority.CanSelect ||
		authority.ServerEncoding != "UTF8" ||
		authority.ServerVersion < 160000 ||
		authority.ServerVersion >= 170000 {
		return postgresDeleteCatalogAuthority{}, fmt.Errorf(
			"PostgreSQL delete relation is not an unpartitioned, hook-free PostgreSQL 16 heap with exact read authority",
		)
	}
	authority.Schema = namespace
	authority.Table = expected.Name
	authority.TableShape = live
	authority.PrimaryKey = append(
		[]schema.Column(nil),
		expectedKey...,
	)
	if err := readPostgresDeletePrimaryIndexAuthority(
		ctx,
		queryer,
		&authority,
	); err != nil {
		return postgresDeleteCatalogAuthority{}, err
	}
	authority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		return postgresDeleteCatalogAuthority{}, err
	}
	return authority, nil
}

type postgresDeletePrimaryIndexRow struct {
	constraintOID      int64
	constraintName     string
	validated          bool
	deferrable         bool
	deferred           bool
	noInherit          bool
	local              bool
	inheritanceCount   int
	parent             bool
	indexOID           int64
	primary            bool
	unique             bool
	valid              bool
	ready              bool
	live               bool
	nullsNotDistinct   bool
	partial            bool
	expression         bool
	keyColumns         int
	totalColumns       int
	method             string
	position           int
	column             string
	columnCollationOID int64
	indexOption        int
	opclassNamespace   string
	opclass            string
	defaultOpclass     bool
	collationOID       int64
	collationNamespace string
	collation          string
	collationProvider  string
	collationVersion   string
	collationIsCurrent bool
	deterministic      bool
}

func readPostgresDeletePrimaryIndexAuthority(
	ctx context.Context,
	queryer engine.PostgresCatalogQueryer,
	authority *postgresDeleteCatalogAuthority,
) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			constraint_object.oid::bigint,
			constraint_object.conname,
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
			index_relation.oid::bigint,
			index_metadata.indisprimary,
			index_metadata.indisunique,
			index_metadata.indisvalid,
			index_metadata.indisready,
			index_metadata.indislive,
			index_metadata.indnullsnotdistinct,
			index_metadata.indpred IS NOT NULL,
			index_metadata.indexprs IS NOT NULL,
			index_metadata.indnkeyatts,
			index_metadata.indnatts,
			access_method.amname,
			key_column.position,
			attribute.attname,
			attribute.attcollation::bigint,
			key_column.index_option,
			opclass_namespace.nspname,
			operator_class.opcname,
			operator_class.opcdefault,
			COALESCE(collation_object.oid::bigint, 0),
			COALESCE(collation_namespace.nspname, ''),
			COALESCE(collation_object.collname, ''),
			COALESCE(collation_object.collprovider::text, ''),
			COALESCE(collation_object.collversion, ''),
			COALESCE(collation_object.collversion, '') =
				COALESCE(
					pg_catalog.pg_collation_actual_version(
						collation_object.oid
					),
					''
				),
			COALESCE(collation_object.collisdeterministic, false)
		FROM pg_catalog.pg_constraint AS constraint_object
		JOIN pg_catalog.pg_index AS index_metadata
		  ON index_metadata.indexrelid = constraint_object.conindid
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
		  ON attribute.attrelid = constraint_object.conrelid
		 AND attribute.attnum = key_column.attnum
		JOIN pg_catalog.pg_opclass AS operator_class
		  ON operator_class.oid = key_column.opclass_oid
		JOIN pg_catalog.pg_namespace AS opclass_namespace
		  ON opclass_namespace.oid = operator_class.opcnamespace
		LEFT JOIN pg_catalog.pg_collation AS collation_object
		  ON collation_object.oid = key_column.collation_oid
		LEFT JOIN pg_catalog.pg_namespace AS collation_namespace
		  ON collation_namespace.oid = collation_object.collnamespace
		WHERE constraint_object.conrelid = $1
		  AND constraint_object.contype = 'p'
		ORDER BY constraint_object.oid, key_column.position
	`, authority.RelationOID)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL delete primary-key backing index: %w",
			err,
		)
	}
	defer rows.Close()
	var constraintOID, indexOID int64
	for rows.Next() {
		var row postgresDeletePrimaryIndexRow
		if err := rows.Scan(
			&row.constraintOID,
			&row.constraintName,
			&row.validated,
			&row.deferrable,
			&row.deferred,
			&row.noInherit,
			&row.local,
			&row.inheritanceCount,
			&row.parent,
			&row.indexOID,
			&row.primary,
			&row.unique,
			&row.valid,
			&row.ready,
			&row.live,
			&row.nullsNotDistinct,
			&row.partial,
			&row.expression,
			&row.keyColumns,
			&row.totalColumns,
			&row.method,
			&row.position,
			&row.column,
			&row.columnCollationOID,
			&row.indexOption,
			&row.opclassNamespace,
			&row.opclass,
			&row.defaultOpclass,
			&row.collationOID,
			&row.collationNamespace,
			&row.collation,
			&row.collationProvider,
			&row.collationVersion,
			&row.collationIsCurrent,
			&row.deterministic,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL delete primary-key backing index: %w",
				err,
			)
		}
		if constraintOID == 0 {
			constraintOID = row.constraintOID
			indexOID = row.indexOID
			authority.Constraint = row.constraintName
		}
		if row.constraintOID <= 0 ||
			row.constraintOID != constraintOID ||
			row.indexOID <= 0 ||
			row.indexOID != indexOID ||
			strings.TrimSpace(row.constraintName) == "" ||
			row.constraintName != authority.Constraint ||
			!row.validated ||
			row.deferrable ||
			row.deferred ||
			!row.noInherit ||
			!row.local ||
			row.inheritanceCount != 0 ||
			row.parent ||
			!row.primary ||
			!row.unique ||
			!row.valid ||
			!row.ready ||
			!row.live ||
			row.nullsNotDistinct ||
			row.partial ||
			row.expression ||
			row.keyColumns != len(authority.PrimaryKey) ||
			row.totalColumns != len(authority.PrimaryKey) ||
			row.method != "btree" ||
			row.position != len(authority.IndexKeys)+1 ||
			row.position > len(authority.PrimaryKey) ||
			row.column !=
				authority.PrimaryKey[row.position-1].Name ||
			row.columnCollationOID != row.collationOID ||
			row.indexOption != 0 ||
			row.opclassNamespace != "pg_catalog" ||
			strings.TrimSpace(row.opclass) == "" ||
			!row.defaultOpclass {
			return fmt.Errorf(
				"PostgreSQL delete primary-key backing index has an unsupported catalog shape",
			)
		}
		kind, err := validationKindForColumn(
			authority.PrimaryKey[row.position-1],
		)
		if err != nil {
			return err
		}
		textual := kind == validationText
		if textual {
			if row.collationOID <= 0 ||
				strings.TrimSpace(row.collationNamespace) == "" ||
				strings.TrimSpace(row.collation) == "" ||
				strings.TrimSpace(row.collationProvider) == "" ||
				!row.collationIsCurrent ||
				!row.deterministic {
				return fmt.Errorf(
					"PostgreSQL text delete key lacks deterministic backing-index collation evidence",
				)
			}
		} else if row.collationOID != 0 ||
			row.collationNamespace != "" ||
			row.collation != "" ||
			row.collationProvider != "" ||
			row.collationVersion != "" ||
			row.deterministic {
			return fmt.Errorf(
				"PostgreSQL non-text delete key has unexpected collation evidence",
			)
		}
		authority.IndexKeys = append(
			authority.IndexKeys,
			postgresDeleteIndexKeyAuthority{
				Position:               row.position,
				Column:                 row.column,
				OperatorClassNamespace: row.opclassNamespace,
				OperatorClass:          row.opclass,
				CollationOID:           row.collationOID,
				CollationNamespace:     row.collationNamespace,
				Collation:              row.collation,
				CollationProvider:      row.collationProvider,
				CollationVersion:       row.collationVersion,
				CollationDeterministic: row.deterministic,
			},
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(authority.IndexKeys) != len(authority.PrimaryKey) {
		return fmt.Errorf(
			"PostgreSQL delete primary-key backing index width differs",
		)
	}
	authority.ConstraintOID = constraintOID
	authority.IndexOID = indexOID
	return nil
}

func samePostgresDeleteCatalogAuthority(
	left postgresDeleteCatalogAuthority,
	right postgresDeleteCatalogAuthority,
) bool {
	return left.CatalogDigest != "" &&
		left.CatalogDigest == right.CatalogDigest
}

func samePostgresDeleteRelation(
	left postgresDeleteCatalogAuthority,
	right postgresDeleteCatalogAuthority,
) bool {
	return strings.TrimSpace(left.SystemIdentifier) != "" &&
		left.SystemIdentifier == right.SystemIdentifier &&
		left.DatabaseOID > 0 &&
		left.DatabaseOID == right.DatabaseOID &&
		left.RelationOID > 0 &&
		left.RelationOID == right.RelationOID
}
