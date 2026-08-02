package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	sqlServerDeleteJournalSchema        = "dmtx_internal"
	sqlServerDeleteJournalTable         = "delete_batch_receipts"
	sqlServerDeleteJournalHeaderTable   = "delete_journal_header"
	sqlServerDeleteJournalVersion       = 1
	sqlServerDeleteJournalPKConstraint  = "pk_dmtx_internal_delete_batch_receipts"
	sqlServerDeleteJournalHeaderPK      = "pk_dmtx_internal_delete_journal_header"
	sqlServerDeleteJournalTextCollation = "Latin1_General_100_BIN2_UTF8"
)

type sqlServerDeleteJournalColumn struct {
	ID            int
	Name          string
	Type          string
	MaxLength     int
	Precision     int
	Scale         int
	Nullable      bool
	Identity      bool
	Computed      bool
	Collation     string
	DefaultObject int64
	Generated     int
	Encrypted     int
	Sparse        bool
	ColumnSet     bool
	RowGUID       bool
	FileStream    bool
	Hidden        bool
}

type sqlServerDeleteJournalIndex struct {
	ID               int
	Name             string
	Type             int
	TypeDescription  string
	Unique           bool
	PrimaryKey       bool
	UniqueConstraint bool
	HasFilter        bool
	Filter           string
	Disabled         bool
	Hypothetical     bool
	AutoCreated      bool
	KeyColumnID      int
	KeyOrdinal       int
	Descending       bool
	Included         bool
}

// sqlServerDeleteJournalCatalog contains only exact, credential-free native
// facts. The private schema is intentionally distinct from every configured
// target namespace, so normal source discovery and target evolution do not
// enumerate it as a user table.
type sqlServerDeleteJournalCatalog struct {
	SchemaExists   bool
	SchemaID       int64
	SchemaOwner    string
	Exists         bool
	ObjectID       int64
	Columns        []sqlServerDeleteJournalColumn
	Index          sqlServerDeleteJournalIndex
	HeaderObjectID int64
	HeaderColumns  []sqlServerDeleteJournalColumn
	HeaderIndex    sqlServerDeleteJournalIndex
	HeaderIdentity string
	CatalogDigest  string
}

type sqlServerDeleteJournalRelationCatalog struct {
	Exists   bool
	ObjectID int64
	Columns  []sqlServerDeleteJournalColumn
	Index    sqlServerDeleteJournalIndex
}

type sqlServerDeleteJournalQueryer interface {
	engine.SQLServerCatalogQueryer
}

type sqlServerDeleteJournalMutationConnection interface {
	sqlServerDeleteJournalQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func sqlServerDeleteExpectedJournalColumns() []sqlServerDeleteJournalColumn {
	text := func(id int, name, typ string, length int) sqlServerDeleteJournalColumn {
		return sqlServerDeleteJournalColumn{
			ID: id, Name: name, Type: typ, MaxLength: length,
			Collation: sqlServerDeleteJournalTextCollation,
		}
	}
	return []sqlServerDeleteJournalColumn{
		text(1, "token", "varchar", 64),
		text(2, "plan_id", "char", 32),
		{ID: 3, Name: "sequence", Type: "bigint", MaxLength: 8},
		text(4, "batch_digest", "char", 64),
		{ID: 5, Name: "candidates", Type: "bigint", MaxLength: 8},
		{ID: 6, Name: "deleted_rows", Type: "bigint", MaxLength: 8},
		text(7, "receipt_digest", "char", 64),
		text(8, "target_catalog_digest", "char", 64),
		text(9, "target_identity_digest", "char", 64),
		text(10, "journal_catalog_digest", "char", 64),
		{ID: 11, Name: "journal_version", Type: "smallint", MaxLength: 2},
	}
}

func sqlServerDeleteExpectedJournalHeaderColumns() []sqlServerDeleteJournalColumn {
	return []sqlServerDeleteJournalColumn{
		{ID: 1, Name: "singleton", Type: "tinyint", MaxLength: 1},
		{
			ID:        2,
			Name:      "journal_identity",
			Type:      "char",
			MaxLength: 64,
			Collation: sqlServerDeleteJournalTextCollation,
		},
	}
}

func inspectSQLServerDeleteReceiptJournal(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
) (sqlServerDeleteJournalCatalog, error) {
	if queryer == nil {
		return sqlServerDeleteJournalCatalog{}, errors.New("SQL Server delete receipt catalog is unavailable")
	}
	var catalog sqlServerDeleteJournalCatalog
	err := queryer.QueryRowContext(ctx, `
		SELECT target_schema.schema_id, USER_NAME(target_schema.principal_id)
		  FROM sys.schemas AS target_schema
		 WHERE target_schema.name = @p1
	`, sqlServerDeleteJournalSchema).Scan(&catalog.SchemaID, &catalog.SchemaOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return catalog, nil
	}
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, fmt.Errorf("inspect SQL Server delete receipt schema: %w", err)
	}
	catalog.SchemaExists = true
	catalog.SchemaOwner = strings.TrimSpace(catalog.SchemaOwner)
	if catalog.SchemaID <= 0 || catalog.SchemaOwner == "" {
		return sqlServerDeleteJournalCatalog{}, errors.New("SQL Server delete receipt schema identity is incomplete")
	}
	receipts, err := inspectSQLServerDeleteJournalRelation(
		ctx, queryer, catalog.SchemaID,
		sqlServerDeleteJournalTable, sqlServerDeleteJournalPKConstraint,
		sqlServerDeleteExpectedJournalColumns(), "receipt",
	)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	header, err := inspectSQLServerDeleteJournalRelation(
		ctx, queryer, catalog.SchemaID,
		sqlServerDeleteJournalHeaderTable, sqlServerDeleteJournalHeaderPK,
		sqlServerDeleteExpectedJournalHeaderColumns(), "header",
	)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	if !receipts.Exists && !header.Exists {
		return catalog, nil
	}
	if !receipts.Exists || !header.Exists {
		return sqlServerDeleteJournalCatalog{}, errors.New("SQL Server delete journal/header catalog is an incomplete partial authority")
	}
	headerIdentity, err := readSQLServerDeleteJournalHeaderIdentity(ctx, queryer)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	catalog.Exists = true
	catalog.ObjectID = receipts.ObjectID
	catalog.Columns = receipts.Columns
	catalog.Index = receipts.Index
	catalog.HeaderObjectID = header.ObjectID
	catalog.HeaderColumns = header.Columns
	catalog.HeaderIndex = header.Index
	catalog.HeaderIdentity = headerIdentity
	catalog.CatalogDigest, err = sqlServerDeleteJournalCatalogDigest(catalog)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	return catalog, nil
}

// inspectSQLServerDeleteJournalRelation authenticates either half of the
// private journal as a precise user table. A same-name view, procedure, or
// malformed table is never treated as an absent object that readiness can
// recreate.
func inspectSQLServerDeleteJournalRelation(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
	schemaID int64,
	name string,
	primaryKeyConstraint string,
	expectedColumns []sqlServerDeleteJournalColumn,
	label string,
) (sqlServerDeleteJournalRelationCatalog, error) {
	var relation sqlServerDeleteJournalRelationCatalog
	if schemaID <= 0 || strings.TrimSpace(name) == "" || strings.TrimSpace(label) == "" {
		return relation, errors.New("SQL Server delete journal relation identity is incomplete")
	}
	var (
		objectType      string
		typeDescription string
		msShipped       bool
		temporalType    int
		memoryOptimized bool
		durability      string
		fileTable       bool
		external        bool
		ledgerType      int
	)
	err := queryer.QueryRowContext(ctx, `
		SELECT
			target_object.object_id,
			RTRIM(target_object.type),
			target_object.type_desc,
			CONVERT(bit, target_object.is_ms_shipped),
			target_table.temporal_type,
			CONVERT(bit, target_table.is_memory_optimized),
			target_table.durability_desc,
			CONVERT(bit, target_table.is_filetable),
			CONVERT(bit, target_table.is_external),
			target_table.ledger_type
		  FROM sys.objects AS target_object
		  JOIN sys.tables AS target_table
		    ON target_table.object_id = target_object.object_id
		 WHERE target_object.schema_id = @p1
		   AND target_object.name = @p2
	`, schemaID, name).Scan(
		&relation.ObjectID,
		&objectType,
		&typeDescription,
		&msShipped,
		&temporalType,
		&memoryOptimized,
		&durability,
		&fileTable,
		&external,
		&ledgerType,
	)
	if errors.Is(err, sql.ErrNoRows) {
		var collisions int
		if collisionErr := queryer.QueryRowContext(ctx, `
			SELECT COUNT(*)
			  FROM sys.objects
			 WHERE schema_id = @p1 AND name = @p2
		`, schemaID, name).Scan(&collisions); collisionErr != nil {
			return relation, fmt.Errorf("inspect SQL Server delete journal %s name collision: %w", label, collisionErr)
		}
		if collisions != 0 {
			return relation, fmt.Errorf("SQL Server delete journal %s name collides with a non-table object", label)
		}
		return relation, nil
	}
	if err != nil {
		return relation, fmt.Errorf("inspect SQL Server delete journal %s table: %w", label, err)
	}
	if relation.ObjectID <= 0 || objectType != "U" || typeDescription != "USER_TABLE" ||
		msShipped || temporalType != 0 || memoryOptimized || durability != "SCHEMA_AND_DATA" ||
		fileTable || external || ledgerType != 0 {
		return relation, fmt.Errorf("SQL Server delete journal %s has an unsupported table identity", label)
	}
	columns, err := readSQLServerDeleteJournalColumns(ctx, queryer, relation.ObjectID)
	if err != nil {
		return relation, err
	}
	if err := validateSQLServerDeleteJournalExactColumns(label, columns, expectedColumns); err != nil {
		return relation, err
	}
	index, err := readSQLServerDeleteJournalIndex(ctx, queryer, relation.ObjectID)
	if err != nil {
		return relation, err
	}
	if err := validateSQLServerDeleteJournalExactIndex(label, index, primaryKeyConstraint); err != nil {
		return relation, err
	}
	if err := validateSQLServerDeleteJournalAttachments(ctx, queryer, relation.ObjectID); err != nil {
		return relation, err
	}
	relation.Exists = true
	relation.Columns = columns
	relation.Index = index
	return relation, nil
}

func readSQLServerDeleteJournalHeaderIdentity(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
) (string, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT [singleton], [journal_identity] FROM "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalHeaderTable)+" WITH (HOLDLOCK)")
	if err != nil {
		return "", fmt.Errorf("read SQL Server delete journal header identity: %w", err)
	}
	defer rows.Close()
	count := 0
	var identity string
	for rows.Next() {
		var singleton int
		var observed string
		if err := rows.Scan(&singleton, &observed); err != nil {
			return "", err
		}
		count++
		if singleton != 1 || count != 1 {
			return "", errors.New("SQL Server delete journal header must contain exactly singleton row 1")
		}
		identity = strings.TrimSpace(observed)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if count != 1 || validateLowerSHA256("SQL Server delete journal header identity", identity) != nil {
		return "", errors.New("SQL Server delete journal header identity is malformed")
	}
	return identity, nil
}

func readSQLServerDeleteJournalColumns(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
	objectID int64,
) ([]sqlServerDeleteJournalColumn, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			column_object.column_id,
			column_object.name,
			TYPE_NAME(column_object.system_type_id),
			column_object.max_length,
			column_object.precision,
			column_object.scale,
			CONVERT(bit, column_object.is_nullable),
			CONVERT(bit, column_object.is_identity),
			CONVERT(bit, column_object.is_computed),
			COALESCE(column_object.collation_name, N''),
			CONVERT(bigint, column_object.default_object_id),
			column_object.generated_always_type,
			COALESCE(column_object.encryption_type, 0),
			CONVERT(bit, column_object.is_sparse),
			CONVERT(bit, column_object.is_column_set),
			CONVERT(bit, column_object.is_rowguidcol),
			CONVERT(bit, column_object.is_filestream),
			CONVERT(bit, column_object.is_hidden)
		  FROM sys.columns AS column_object
		 WHERE column_object.object_id = @p1
		 ORDER BY column_object.column_id
	`, objectID)
	if err != nil {
		return nil, fmt.Errorf("read SQL Server delete receipt columns: %w", err)
	}
	defer rows.Close()
	var result []sqlServerDeleteJournalColumn
	for rows.Next() {
		var item sqlServerDeleteJournalColumn
		if err := rows.Scan(
			&item.ID, &item.Name, &item.Type, &item.MaxLength, &item.Precision,
			&item.Scale, &item.Nullable, &item.Identity, &item.Computed,
			&item.Collation, &item.DefaultObject, &item.Generated, &item.Encrypted,
			&item.Sparse, &item.ColumnSet, &item.RowGUID, &item.FileStream, &item.Hidden,
		); err != nil {
			return nil, err
		}
		item.Name = strings.TrimSpace(item.Name)
		item.Type = strings.ToLower(strings.TrimSpace(item.Type))
		item.Collation = strings.TrimSpace(item.Collation)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validateSQLServerDeleteJournalColumns(columns []sqlServerDeleteJournalColumn) error {
	return validateSQLServerDeleteJournalExactColumns("receipt", columns, sqlServerDeleteExpectedJournalColumns())
}

func validateSQLServerDeleteJournalExactColumns(
	label string,
	columns []sqlServerDeleteJournalColumn,
	expected []sqlServerDeleteJournalColumn,
) error {
	if len(columns) != len(expected) {
		return fmt.Errorf("SQL Server delete journal %s column count differs", label)
	}
	for index, item := range columns {
		want := expected[index]
		if item.ID != want.ID || item.Name != want.Name || item.Type != want.Type ||
			item.MaxLength != want.MaxLength || item.Nullable || item.Identity || item.Computed ||
			item.DefaultObject != 0 || item.Generated != 0 || item.Encrypted != 0 || item.Sparse ||
			item.ColumnSet || item.RowGUID || item.FileStream || item.Hidden {
			return fmt.Errorf("SQL Server delete journal %s column %d differs from exact authority", label, index+1)
		}
		if want.Type == "varchar" || want.Type == "char" {
			if !strings.EqualFold(item.Collation, want.Collation) {
				return fmt.Errorf("SQL Server delete journal %s column %s lacks binary collation authority", label, item.Name)
			}
		} else if item.Collation != "" {
			return fmt.Errorf("SQL Server delete journal %s non-text column %s has collation authority", label, item.Name)
		}
	}
	return nil
}

func readSQLServerDeleteJournalIndex(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
	objectID int64,
) (sqlServerDeleteJournalIndex, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT
			index_object.index_id,
			index_object.name,
			index_object.type,
			index_object.type_desc,
			CONVERT(bit, index_object.is_unique),
			CONVERT(bit, index_object.is_primary_key),
			CONVERT(bit, index_object.is_unique_constraint),
			CONVERT(bit, index_object.has_filter),
			COALESCE(index_object.filter_definition, N''),
			CONVERT(bit, index_object.is_disabled),
			CONVERT(bit, index_object.is_hypothetical),
			CONVERT(bit, index_object.auto_created),
			index_column.column_id,
			index_column.key_ordinal,
			CONVERT(bit, index_column.is_descending_key),
			CONVERT(bit, index_column.is_included_column)
		  FROM sys.indexes AS index_object
		  JOIN sys.index_columns AS index_column
		    ON index_column.object_id = index_object.object_id
		   AND index_column.index_id = index_object.index_id
		 WHERE index_object.object_id = @p1
		   AND index_object.index_id > 0
		 ORDER BY index_object.index_id, index_column.key_ordinal, index_column.index_column_id
	`, objectID)
	if err != nil {
		return sqlServerDeleteJournalIndex{}, fmt.Errorf("read SQL Server delete receipt indexes: %w", err)
	}
	defer rows.Close()
	var items []sqlServerDeleteJournalIndex
	for rows.Next() {
		var item sqlServerDeleteJournalIndex
		if err := rows.Scan(
			&item.ID, &item.Name, &item.Type, &item.TypeDescription, &item.Unique,
			&item.PrimaryKey, &item.UniqueConstraint, &item.HasFilter, &item.Filter,
			&item.Disabled, &item.Hypothetical, &item.AutoCreated, &item.KeyColumnID,
			&item.KeyOrdinal, &item.Descending, &item.Included,
		); err != nil {
			return sqlServerDeleteJournalIndex{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return sqlServerDeleteJournalIndex{}, err
	}
	if len(items) != 1 {
		return sqlServerDeleteJournalIndex{}, errors.New("SQL Server delete receipt journal index inventory differs")
	}
	return items[0], nil
}

func validateSQLServerDeleteJournalExactIndex(
	label string,
	index sqlServerDeleteJournalIndex,
	primaryKeyConstraint string,
) error {
	if index.ID != 1 || index.Name != primaryKeyConstraint ||
		index.Type != 1 || index.TypeDescription != "CLUSTERED" || !index.Unique ||
		!index.PrimaryKey || index.UniqueConstraint || index.HasFilter || index.Filter != "" ||
		index.Disabled || index.Hypothetical || index.AutoCreated || index.KeyColumnID != 1 ||
		index.KeyOrdinal != 1 || index.Descending || index.Included {
		return fmt.Errorf("SQL Server delete journal %s lacks the exact singleton primary-key index", label)
	}
	return nil
}

func validateSQLServerDeleteJournalAttachments(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
	objectID int64,
) error {
	var triggers, foreignKeys, checks, defaults, security int
	if err := queryer.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sys.triggers WHERE parent_id = @p1),
			(SELECT COUNT(*) FROM sys.foreign_keys WHERE parent_object_id = @p1 OR referenced_object_id = @p1),
			(SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id = @p1),
			(SELECT COUNT(*) FROM sys.default_constraints WHERE parent_object_id = @p1),
			(SELECT COUNT(*) FROM sys.security_predicates WHERE target_object_id = @p1)
	`, objectID).Scan(&triggers, &foreignKeys, &checks, &defaults, &security); err != nil {
		return fmt.Errorf("read SQL Server delete receipt attachments: %w", err)
	}
	if triggers != 0 || foreignKeys != 0 || checks != 0 || defaults != 0 || security != 0 {
		return errors.New("SQL Server delete receipt journal has unsupported attached authority")
	}
	return nil
}

func sqlServerDeleteJournalCatalogDigest(catalog sqlServerDeleteJournalCatalog) (string, error) {
	if !catalog.Exists || catalog.SchemaID <= 0 || catalog.ObjectID <= 0 ||
		catalog.HeaderObjectID <= 0 || strings.TrimSpace(catalog.SchemaOwner) == "" ||
		len(catalog.Columns) == 0 || len(catalog.HeaderColumns) == 0 ||
		validateLowerSHA256("SQL Server delete journal header identity", catalog.HeaderIdentity) != nil {
		return "", errors.New("SQL Server delete receipt journal catalog is incomplete")
	}
	payload, err := json.Marshal(struct {
		Version        int                            `json:"version"`
		SchemaID       int64                          `json:"schema_id"`
		SchemaOwner    string                         `json:"schema_owner"`
		ObjectID       int64                          `json:"object_id"`
		Columns        []sqlServerDeleteJournalColumn `json:"columns"`
		Index          sqlServerDeleteJournalIndex    `json:"index"`
		HeaderObjectID int64                          `json:"header_object_id"`
		HeaderColumns  []sqlServerDeleteJournalColumn `json:"header_columns"`
		HeaderIndex    sqlServerDeleteJournalIndex    `json:"header_index"`
		HeaderIdentity string                         `json:"header_identity"`
	}{
		Version:        sqlServerDeleteJournalVersion,
		SchemaID:       catalog.SchemaID,
		SchemaOwner:    catalog.SchemaOwner,
		ObjectID:       catalog.ObjectID,
		Columns:        catalog.Columns,
		Index:          catalog.Index,
		HeaderObjectID: catalog.HeaderObjectID,
		HeaderColumns:  catalog.HeaderColumns,
		HeaderIndex:    catalog.HeaderIndex,
		HeaderIdentity: catalog.HeaderIdentity,
	})
	if err != nil {
		return "", fmt.Errorf("encode SQL Server delete receipt catalog: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

type sqlServerDeleteJournalPrivileges struct {
	ViewDefinition bool
	CreateSchema   bool
	CreateTable    bool
	SchemaControl  bool
	SchemaAlter    bool
	SchemaSelect   bool
	SchemaInsert   bool
}

func readSQLServerDeleteJournalPrivileges(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
) (sqlServerDeleteJournalPrivileges, error) {
	var privileges sqlServerDeleteJournalPrivileges
	if err := queryer.QueryRowContext(ctx, `
		SELECT
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SCHEMA'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'CONTROL'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'ALTER'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT'), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'INSERT'), 0))
	`, sqlServerDeleteJournalSchema).Scan(
		&privileges.ViewDefinition, &privileges.CreateSchema, &privileges.CreateTable,
		&privileges.SchemaControl, &privileges.SchemaAlter, &privileges.SchemaSelect,
		&privileges.SchemaInsert,
	); err != nil {
		return sqlServerDeleteJournalPrivileges{}, fmt.Errorf("inspect SQL Server delete receipt privileges: %w", err)
	}
	return privileges, nil
}

func validateSQLServerDeleteJournalAdmission(
	identity sqlServerDeleteEndpointIdentity,
	catalog sqlServerDeleteJournalCatalog,
	privileges sqlServerDeleteJournalPrivileges,
) error {
	if !privileges.ViewDefinition || strings.TrimSpace(identity.Principal) == "" {
		return errors.New("SQL Server delete receipt journal requires VIEW DEFINITION and a current database principal")
	}
	if !catalog.SchemaExists {
		if !privileges.CreateSchema || !privileges.CreateTable {
			return errors.New("SQL Server delete receipt journal requires CREATE SCHEMA and CREATE TABLE privileges")
		}
		return nil
	}
	if !strings.EqualFold(catalog.SchemaOwner, identity.Principal) {
		// The private schema is database-global. Until a shared multi-principal
		// ownership/grant protocol is independently proven, a different
		// principal must fail before checkpoint or mutation rather than racing
		// or reusing another principal's journal authority.
		return errors.New("SQL Server delete receipt shared schema is owned by a different principal; multi-principal journal reuse is not certified")
	}
	if !privileges.SchemaControl || !privileges.SchemaAlter || !privileges.SchemaSelect ||
		!privileges.SchemaInsert {
		return errors.New("SQL Server delete receipt schema must be current-principal owned with CONTROL, ALTER, SELECT, and INSERT authority")
	}
	if !catalog.Exists && !privileges.CreateTable {
		return errors.New("SQL Server delete receipt journal requires CREATE TABLE privilege")
	}
	return nil
}

func preflightSQLServerDeleteReceiptJournal(
	ctx context.Context,
	adapter *sqlServerTargetAdapter,
) error {
	if adapter == nil || adapter.database == nil || strings.TrimSpace(adapter.namespace) == "" ||
		isSQLServerDeleteJournalNamespace(adapter.namespace) {
		return errors.New("SQL Server delete receipt journal target is unavailable or reserved")
	}
	if ctx == nil {
		return errors.New("SQL Server delete receipt journal preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := engine.VerifySQLServer2022Target(ctx, adapter.database); err != nil {
		return fmt.Errorf("verify SQL Server delete receipt target: %w", err)
	}
	identity, err := readSQLServerDeleteEndpointIdentity(ctx, adapter.database, adapter.namespace)
	if err != nil {
		return err
	}
	catalog, err := inspectSQLServerDeleteReceiptJournal(ctx, adapter.database)
	if err != nil {
		return err
	}
	privileges, err := readSQLServerDeleteJournalPrivileges(ctx, adapter.database)
	if err != nil {
		return err
	}
	return validateSQLServerDeleteJournalAdmission(identity, catalog, privileges)
}

// PreflightStage4DeleteJournalReadiness is the deliberately read-only half of
// the generic lifecycle. It runs before ordinary work/checkpoint persistence,
// so malformed journal authority or missing privileges cannot be discovered
// after a target schema/data mutation has become reachable.
func (adapter *sqlServerTargetAdapter) PreflightStage4DeleteJournalReadiness(
	ctx context.Context,
) error {
	return preflightSQLServerDeleteReceiptJournal(ctx, adapter)
}

func validateSQLServerDeleteReadinessRequest(request Stage4DeleteJournalReadinessRequest) error {
	if strings.TrimSpace(request.RunID) == "" || request.RunID != strings.TrimSpace(request.RunID) {
		return errors.New("SQL Server delete journal readiness run ID is required")
	}
	if err := validateLowerSHA256("SQL Server delete journal readiness inventory digest", request.InventoryDigest); err != nil {
		return err
	}
	if request.Existing != nil {
		if err := request.Existing.Validate(); err != nil {
			return fmt.Errorf("stored SQL Server delete journal readiness receipt is invalid: %w", err)
		}
		if request.Existing.Readiness.RunID != request.RunID ||
			request.Existing.Readiness.InventoryDigest != request.InventoryDigest ||
			request.Existing.Readiness.TargetEngine != "mssql" {
			return errors.New("stored SQL Server delete journal readiness receipt differs from run inventory or target engine")
		}
	}
	return nil
}

func sqlServerDeleteJournalLockResource(identity sqlServerDeleteEndpointIdentity) (string, error) {
	incarnation, err := sqlServerDeleteJournalDatabaseIncarnation(identity)
	if err != nil {
		return "", errors.New("SQL Server delete journal lock identity is unavailable")
	}
	digest := sha256.Sum256([]byte("dmtx.stage4.delete-journal.v1\x00" + incarnation))
	return "dmtx.stage4.delete-journal.v1." + hex.EncodeToString(digest[:20]), nil
}

// sqlServerDeleteJournalDatabaseIncarnation intentionally excludes the
// configured user schema. The private journal lives once per SQL Server
// database, so two configured schemas on that database must serialize the
// same DDL/readiness boundary.
func sqlServerDeleteJournalDatabaseIncarnation(identity sqlServerDeleteEndpointIdentity) (string, error) {
	if strings.TrimSpace(identity.Server) == "" || strings.TrimSpace(identity.Database) == "" ||
		identity.DatabaseID <= 0 || identity.CreatedAt.IsZero() {
		return "", errors.New("SQL Server delete journal database incarnation is incomplete")
	}
	return fmt.Sprintf(
		"mssql-delete-journal-v1 server=%s database=%s database_id=%d created_at=%s",
		strings.ToLower(strings.TrimSpace(identity.Server)),
		strings.ToLower(strings.TrimSpace(identity.Database)),
		identity.DatabaseID,
		identity.CreatedAt.UTC().Format(time.RFC3339Nano),
	), nil
}

func createSQLServerDeleteReceiptJournal(
	ctx context.Context,
	connection sqlServerDeleteJournalMutationConnection,
	identity sqlServerDeleteEndpointIdentity,
	catalog sqlServerDeleteJournalCatalog,
) error {
	if connection == nil {
		return errors.New("SQL Server delete receipt journal mutation connection is unavailable")
	}
	headerIdentity, err := newSQLServerDeleteJournalHeaderIdentity()
	if err != nil {
		return err
	}
	if !catalog.SchemaExists {
		if _, err := connection.ExecContext(
			ctx,
			"CREATE SCHEMA "+sqlServerIdentifier(sqlServerDeleteJournalSchema)+
				" AUTHORIZATION "+sqlServerIdentifier(identity.Principal),
		); err != nil {
			return fmt.Errorf("create SQL Server delete receipt schema: %w", err)
		}
	}
	headerStatement := "CREATE TABLE " +
		sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalHeaderTable) + " (" +
		"[singleton] tinyint NOT NULL, " +
		"[journal_identity] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"CONSTRAINT " + sqlServerIdentifier(sqlServerDeleteJournalHeaderPK) +
		" PRIMARY KEY CLUSTERED ([singleton])" +
		")"
	if _, err := connection.ExecContext(ctx, headerStatement); err != nil {
		return fmt.Errorf("create SQL Server delete receipt journal header: %w", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalHeaderTable)+
			" ([singleton], [journal_identity]) VALUES (1, @p1)",
		headerIdentity,
	); err != nil {
		return fmt.Errorf("persist immutable SQL Server delete receipt journal header: %w", err)
	}
	statement := "CREATE TABLE " +
		sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable) + " (" +
		"[token] varchar(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[plan_id] char(32) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[sequence] bigint NOT NULL, " +
		"[batch_digest] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[candidates] bigint NOT NULL, " +
		"[deleted_rows] bigint NOT NULL, " +
		"[receipt_digest] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[target_catalog_digest] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[target_identity_digest] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[journal_catalog_digest] char(64) COLLATE " + sqlServerDeleteJournalTextCollation + " NOT NULL, " +
		"[journal_version] smallint NOT NULL, " +
		"CONSTRAINT " + sqlServerIdentifier(sqlServerDeleteJournalPKConstraint) +
		" PRIMARY KEY CLUSTERED ([token])" +
		")"
	if _, err := connection.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create SQL Server delete receipt table: %w", err)
	}
	return nil
}

func newSQLServerDeleteJournalHeaderIdentity() (string, error) {
	value := make([]byte, sha256.Size)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("read SQL Server delete journal header entropy: %w", err)
	}
	return hex.EncodeToString(value), nil
}

// ensureSQLServerDeleteReceiptJournal never lazily creates a receipt journal
// from ApplyDeleteBatch. Creation is permitted only at the durable readiness
// boundary after immutable inventory is bound. The batch writer passes false
// and therefore treats an absent journal as a fail-closed state/target drift.
func ensureSQLServerDeleteReceiptJournal(
	ctx context.Context,
	connection sqlServerDeleteJournalMutationConnection,
	allowCreate bool,
) (sqlServerDeleteJournalCatalog, error) {
	catalog, err := inspectSQLServerDeleteReceiptJournal(ctx, connection)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	if catalog.Exists {
		return catalog, nil
	}
	if !allowCreate {
		return sqlServerDeleteJournalCatalog{}, errors.New("SQL Server delete receipt journal is absent after readiness admission")
	}
	identity, err := readSQLServerDeleteEndpointIdentity(ctx, connection, "dmtx_internal")
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	privileges, err := readSQLServerDeleteJournalPrivileges(ctx, connection)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	if err := validateSQLServerDeleteJournalAdmission(identity, catalog, privileges); err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	if err := createSQLServerDeleteReceiptJournal(ctx, connection, identity, catalog); err != nil {
		return sqlServerDeleteJournalCatalog{}, err
	}
	catalog, err = inspectSQLServerDeleteReceiptJournal(ctx, connection)
	if err != nil {
		return sqlServerDeleteJournalCatalog{}, fmt.Errorf("reread created SQL Server delete receipt journal: %w", err)
	}
	if !catalog.Exists {
		return sqlServerDeleteJournalCatalog{}, errors.New("created SQL Server delete receipt journal is absent on exact native reread")
	}
	return catalog, nil
}

func sqlServerDeleteReadinessFromCatalog(
	request Stage4DeleteJournalReadinessRequest,
	workloadIdentity string,
	identity sqlServerDeleteEndpointIdentity,
	catalog sqlServerDeleteJournalCatalog,
) (state.Stage4DeleteJournalReadiness, error) {
	if strings.TrimSpace(workloadIdentity) == "" || !catalog.Exists || catalog.CatalogDigest == "" {
		return state.Stage4DeleteJournalReadiness{}, errors.New("SQL Server delete journal readiness requires an exact journal catalog")
	}
	journalDigest, err := sqlServerDeleteReadinessJournalDigest(identity, catalog)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, err
	}
	return state.NewStage4DeleteJournalReadiness(
		request.RunID,
		request.InventoryDigest,
		workloadIdentity,
		"mssql",
		"sqlserver-2022",
		identity.Version,
		journalDigest,
		sqlServerDeleteJournalVersion,
		time.Now().UTC(),
	)
}

// sqlServerDeleteReadinessJournalDigest binds the private random header and
// exact catalog to the native database incarnation. The run identity remains
// the application's canonical configured endpoint identity, which lets the
// state backend verify that readiness belongs to this run rather than merely
// to a target adapter instance.
func sqlServerDeleteReadinessJournalDigest(
	identity sqlServerDeleteEndpointIdentity,
	catalog sqlServerDeleteJournalCatalog,
) (string, error) {
	incarnation, err := sqlServerDeleteJournalDatabaseIncarnation(identity)
	if err != nil {
		return "", err
	}
	if !catalog.Exists || validateLowerSHA256("SQL Server delete journal catalog digest", catalog.CatalogDigest) != nil {
		return "", errors.New("SQL Server delete journal readiness catalog authority is incomplete")
	}
	payload, err := json.Marshal(struct {
		Version     int    `json:"version"`
		Incarnation string `json:"incarnation"`
		Catalog     string `json:"catalog"`
	}{
		Version: 1, Incarnation: incarnation, Catalog: catalog.CatalogDigest,
	})
	if err != nil {
		return "", fmt.Errorf("encode SQL Server delete journal readiness authority: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func classifySQLServerDeleteJournalCommitAmbiguity(
	ctx context.Context,
	adapter *sqlServerTargetAdapter,
	request Stage4DeleteJournalReadinessRequest,
	commitErr error,
) (state.Stage4DeleteJournalReadiness, error) {
	if adapter == nil || adapter.database == nil {
		return state.Stage4DeleteJournalReadiness{}, errors.Join(commitErr, errors.New("SQL Server delete journal commit ambiguity authority is unavailable"))
	}
	verifyCtx, cancel := sqlServerDeleteDetachedContext(ctx)
	defer cancel()
	identity, err := readSQLServerDeleteEndpointIdentity(verifyCtx, adapter.database, adapter.namespace)
	if err == nil {
		var catalog sqlServerDeleteJournalCatalog
		catalog, err = inspectSQLServerDeleteReceiptJournal(verifyCtx, adapter.database)
		if err == nil && !catalog.Exists {
			err = errors.New("SQL Server delete receipt journal is absent after readiness commit acknowledgement failure")
		}
		if err == nil {
			return sqlServerDeleteReadinessFromCatalog(request, adapter.workloadIdentity, identity, catalog)
		}
	}
	return state.Stage4DeleteJournalReadiness{}, errors.Join(
		fmt.Errorf("SQL Server delete journal commit outcome is unknown; resume the existing run so exact native authority can be reread: %w", commitErr),
		err,
	)
}

// PrepareStage4DeleteJournalReadiness is the one native-DLL boundary allowed
// by delete reconciliation. It runs only after durable inventory/work binding,
// owns a transaction-scoped application lock, and rechecks catalog/identity on
// the pinned connection before returning immutable readiness authority.
func (adapter *sqlServerTargetAdapter) PrepareStage4DeleteJournalReadiness(
	ctx context.Context,
	request Stage4DeleteJournalReadinessRequest,
) (result state.Stage4DeleteJournalReadiness, resultErr error) {
	if err := validateSQLServerDeleteReadinessRequest(request); err != nil {
		return result, err
	}
	if adapter == nil || adapter.database == nil || strings.TrimSpace(adapter.namespace) == "" ||
		strings.TrimSpace(adapter.workloadIdentity) == "" || isSQLServerDeleteJournalNamespace(adapter.namespace) {
		return result, errors.New("SQL Server delete journal readiness target is unavailable or reserved")
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire pinned SQL Server delete journal connection: %w", err)
	}
	active := false
	closed := false
	sessionConfigured := false
	defer func() {
		discarded := false
		if active {
			if rollbackErr := rollbackSQLServerDeleteTransaction(ctx, connection); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				discardSQLServerDeleteConnection(connection)
				discarded = true
				result = state.Stage4DeleteJournalReadiness{}
				resultErr = errors.Join(resultErr, fmt.Errorf("roll back SQL Server delete journal transaction: %w", rollbackErr))
			}
		}
		if sessionConfigured && !closed && !discarded {
			// Both XACT_ABORT and SERIALIZABLE are session scoped. Reset only
			// during cleanup and discard the handle if either reset is uncertain.
			if resetErr := resetSQLServerDeleteSession(ctx, connection); resetErr != nil {
				discardSQLServerDeleteConnection(connection)
				discarded = true
				if resultErr != nil {
					result = state.Stage4DeleteJournalReadiness{}
					resultErr = errors.Join(resultErr, fmt.Errorf("reset SQL Server delete journal session: %w", resetErr))
				}
			}
		}
		if !closed {
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
				discardSQLServerDeleteConnection(connection)
				if active || resultErr != nil {
					result = state.Stage4DeleteJournalReadiness{}
					resultErr = errors.Join(resultErr, fmt.Errorf("close pinned SQL Server delete journal connection: %w", closeErr))
				}
			}
		}
	}()
	// Setup retains caller cancellation/deadlines; detached contexts are used
	// only for cleanup after a transaction might already exist.
	sessionConfigured = true
	if err := configureSQLServerDeleteTransaction(ctx, connection); err != nil {
		return result, fmt.Errorf("configure SQL Server delete journal transaction: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		// A failed BEGIN remains a setup error. The deferred cleanup resets and
		// closes this session without classifying a nonexistent transaction.
		return result, fmt.Errorf("begin SQL Server delete journal transaction: %w", err)
	}
	active = true
	if err := engine.VerifySQLServer2022TargetWithQueryer(ctx, connection); err != nil {
		return result, fmt.Errorf("verify pinned SQL Server delete journal target: %w", err)
	}
	identity, err := readSQLServerDeleteEndpointIdentity(ctx, connection, adapter.namespace)
	if err != nil {
		return result, err
	}
	lockResource, err := sqlServerDeleteJournalLockResource(identity)
	if err != nil {
		return result, err
	}
	if err := acquireSQLServerDeleteBatchLock(ctx, connection, lockResource); err != nil {
		return result, fmt.Errorf("acquire SQL Server transaction-owned delete journal lock: %w", err)
	}
	catalog, err := inspectSQLServerDeleteReceiptJournal(ctx, connection)
	if err != nil {
		return result, err
	}
	privileges, err := readSQLServerDeleteJournalPrivileges(ctx, connection)
	if err != nil {
		return result, err
	}
	if err := validateSQLServerDeleteJournalAdmission(identity, catalog, privileges); err != nil {
		return result, err
	}
	if request.Existing != nil && !catalog.Exists {
		return result, errors.New("durable SQL Server delete journal readiness exists but the private journal is absent; refusing recreation")
	}
	if request.Existing != nil && request.Existing.Readiness.TargetIdentity != adapter.workloadIdentity {
		return result, errors.New("durable SQL Server delete journal readiness target identity differs from the configured target")
	}
	if !catalog.Exists {
		catalog, err = ensureSQLServerDeleteReceiptJournal(ctx, connection, true)
		if err != nil {
			return result, err
		}
	}
	result, err = sqlServerDeleteReadinessFromCatalog(request, adapter.workloadIdentity, identity, catalog)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, err
	}
	if _, commitErr := connection.ExecContext(ctx, "COMMIT TRANSACTION"); commitErr != nil {
		active = false
		discardSQLServerDeleteConnection(connection)
		if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			commitErr = errors.Join(commitErr, fmt.Errorf("close ambiguous SQL Server delete journal connection: %w", closeErr))
		}
		closed = true
		return classifySQLServerDeleteJournalCommitAmbiguity(ctx, adapter, request, commitErr)
	}
	active = false
	return result, nil
}

func sqlServerDeleteReceiptDigest(
	receipt deleteTargetBatchReceipt,
	authority sqlServerDeleteCatalogAuthority,
	journal sqlServerDeleteJournalCatalog,
) (string, error) {
	if receipt.FailClosedReason != "" || !journal.Exists || journal.CatalogDigest == "" {
		return "", errors.New("SQL Server delete receipt authority is incomplete")
	}
	targetDigest, err := sqlServerDeleteAuthorityDigestValue(authority)
	if err != nil || authority.CatalogDigest != targetDigest {
		return "", errors.New("SQL Server delete receipt target authority digest is invalid")
	}
	identityDigest := sha256.Sum256([]byte(authority.Endpoint.IdentityKey))
	payload, err := json.Marshal(struct {
		Version        int    `json:"version"`
		PlanID         string `json:"plan_id"`
		Token          string `json:"token"`
		Sequence       int64  `json:"sequence"`
		BatchDigest    string `json:"batch_digest"`
		Candidates     int64  `json:"candidates"`
		DeletedRows    int64  `json:"deleted_rows"`
		TargetCatalog  string `json:"target_catalog"`
		TargetIdentity string `json:"target_identity"`
		JournalCatalog string `json:"journal_catalog"`
	}{
		Version: sqlServerDeleteJournalVersion, PlanID: receipt.PlanID, Token: receipt.Token,
		Sequence: receipt.Sequence, BatchDigest: receipt.BatchDigest,
		Candidates: receipt.Candidates, DeletedRows: receipt.DeletedRows,
		TargetCatalog:  authority.CatalogDigest,
		TargetIdentity: hex.EncodeToString(identityDigest[:]),
		JournalCatalog: journal.CatalogDigest,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func loadSQLServerDeleteReceipt(
	ctx context.Context,
	queryer sqlServerDeleteJournalQueryer,
	token string,
	authority sqlServerDeleteCatalogAuthority,
	journal sqlServerDeleteJournalCatalog,
) (deleteTargetBatchReceipt, bool, error) {
	if queryer == nil || !journal.Exists || strings.TrimSpace(token) == "" {
		return deleteTargetBatchReceipt{}, false, errors.New("SQL Server delete receipt lookup authority is unavailable")
	}
	var (
		stored         deleteTargetBatchReceipt
		targetCatalog  string
		targetIdentity string
		journalCatalog string
		journalVersion int
	)
	err := queryer.QueryRowContext(ctx, "SELECT [plan_id], [sequence], [batch_digest], [candidates], [deleted_rows], [receipt_digest], [target_catalog_digest], [target_identity_digest], [journal_catalog_digest], [journal_version] FROM "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable)+" WHERE [token] = @p1", token).Scan(
		&stored.PlanID, &stored.Sequence, &stored.BatchDigest, &stored.Candidates,
		&stored.DeletedRows, &stored.ReceiptDigest, &targetCatalog, &targetIdentity,
		&journalCatalog, &journalVersion,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deleteTargetBatchReceipt{}, false, nil
	}
	if err != nil {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf("load SQL Server delete receipt: %w", err)
	}
	stored.Token = token
	expectedTarget, digestErr := sqlServerDeleteAuthorityDigestValue(authority)
	identityDigest := sha256.Sum256([]byte(authority.Endpoint.IdentityKey))
	if digestErr != nil || targetCatalog != expectedTarget || targetIdentity != hex.EncodeToString(identityDigest[:]) ||
		journalCatalog != journal.CatalogDigest || journalVersion != sqlServerDeleteJournalVersion {
		return deleteTargetBatchReceipt{}, false, errors.New("SQL Server delete receipt authority differs from the exact target/journal catalog")
	}
	return stored, true, nil
}

func validateSQLServerDeleteReceipt(
	batch deleteTargetBatch,
	receipt deleteTargetBatchReceipt,
	authority sqlServerDeleteCatalogAuthority,
	journal sqlServerDeleteJournalCatalog,
) error {
	if receipt.PlanID != batch.PlanID || receipt.Token != batch.Token ||
		receipt.Sequence != batch.Sequence || receipt.BatchDigest != batch.BatchDigest ||
		receipt.Candidates != int64(len(batch.Keys)) || receipt.DeletedRows < 0 ||
		receipt.DeletedRows > receipt.Candidates || receipt.FailClosedReason != "" {
		return errors.New("SQL Server delete receipt differs from the pending batch")
	}
	digest, err := sqlServerDeleteReceiptDigest(deleteTargetBatchReceipt{
		PlanID: receipt.PlanID, Token: receipt.Token, Sequence: receipt.Sequence,
		BatchDigest: receipt.BatchDigest, Candidates: receipt.Candidates, DeletedRows: receipt.DeletedRows,
	}, authority, journal)
	if err != nil || receipt.ReceiptDigest != digest {
		return errors.New("SQL Server delete receipt digest differs from durable receipt authority")
	}
	return nil
}

func insertSQLServerDeleteReceipt(
	ctx context.Context,
	connection sqlServerDeleteJournalMutationConnection,
	receipt deleteTargetBatchReceipt,
	authority sqlServerDeleteCatalogAuthority,
	journal sqlServerDeleteJournalCatalog,
) error {
	if connection == nil {
		return errors.New("SQL Server delete receipt mutation connection is unavailable")
	}
	targetDigest, err := sqlServerDeleteAuthorityDigestValue(authority)
	if err != nil || targetDigest != authority.CatalogDigest {
		return errors.New("SQL Server delete receipt target authority digest is invalid")
	}
	identityDigest := sha256.Sum256([]byte(authority.Endpoint.IdentityKey))
	result, err := connection.ExecContext(ctx, "INSERT INTO "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable)+" ([token], [plan_id], [sequence], [batch_digest], [candidates], [deleted_rows], [receipt_digest], [target_catalog_digest], [target_identity_digest], [journal_catalog_digest], [journal_version]) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11)", receipt.Token, receipt.PlanID, receipt.Sequence, receipt.BatchDigest, receipt.Candidates, receipt.DeletedRows, receipt.ReceiptDigest, authority.CatalogDigest, hex.EncodeToString(identityDigest[:]), journal.CatalogDigest, sqlServerDeleteJournalVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read SQL Server delete receipt insert affected rows: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("SQL Server delete receipt insert affected rows=%d, want exactly 1", affected)
	}
	return nil
}
