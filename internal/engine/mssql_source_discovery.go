package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

const sqlServer2022MajorVersion = 16

// VerifySQLServer2022Source pins source discovery to the SQL Server 2022
// catalog contract exercised by DMTX's live fixtures. Azure SQL and future
// major catalog shapes require separate admission.
func VerifySQLServer2022Source(
	ctx context.Context,
	database *sql.DB,
) error {
	catalog, err := readSQLServer2022SourceCatalog(ctx, database)
	if err != nil {
		return err
	}
	return validateSQLServer2022SourceCatalog(catalog)
}

type sqlServer2022SourceCatalog struct {
	productMajorVersion int
	engineEdition       int
	productVersion      string
	edition             string
	databaseName        string
	compatibilityLevel  int
	state               string
	userAccess          string
	containment         string
	readOnly            bool
	autoClose           bool
	autoShrink          bool
	standby             bool
	sourceDatabaseID    sql.NullInt64
	published           bool
	subscribed          bool
	mergePublished      bool
	distributor         bool
	changeDataCapture   bool
}

const sqlServer2022SourceCatalogQuery = `
	SELECT
		CONVERT(int, SERVERPROPERTY('ProductMajorVersion')),
		CONVERT(int, SERVERPROPERTY('EngineEdition')),
		CONVERT(varchar(128), SERVERPROPERTY('ProductVersion')),
		CONVERT(varchar(128), SERVERPROPERTY('Edition')),
		source_database.name,
		source_database.compatibility_level,
		source_database.state_desc,
		source_database.user_access_desc,
		source_database.containment_desc,
		source_database.is_read_only,
		source_database.is_auto_close_on,
		source_database.is_auto_shrink_on,
		source_database.is_in_standby,
		source_database.source_database_id,
		source_database.is_published,
		source_database.is_subscribed,
		source_database.is_merge_published,
		source_database.is_distributor,
		source_database.is_cdc_enabled
	FROM sys.databases AS source_database
	WHERE source_database.database_id = DB_ID()
`

func readSQLServer2022SourceCatalog(
	ctx context.Context,
	database *sql.DB,
) (sqlServer2022SourceCatalog, error) {
	var result sqlServer2022SourceCatalog
	err := database.QueryRowContext(
		ctx,
		sqlServer2022SourceCatalogQuery,
	).Scan(
		&result.productMajorVersion,
		&result.engineEdition,
		&result.productVersion,
		&result.edition,
		&result.databaseName,
		&result.compatibilityLevel,
		&result.state,
		&result.userAccess,
		&result.containment,
		&result.readOnly,
		&result.autoClose,
		&result.autoShrink,
		&result.standby,
		&result.sourceDatabaseID,
		&result.published,
		&result.subscribed,
		&result.mergePublished,
		&result.distributor,
		&result.changeDataCapture,
	)
	if err != nil {
		return sqlServer2022SourceCatalog{}, fmt.Errorf(
			"read SQL Server 2022 source catalog: %w",
			err,
		)
	}
	return result, nil
}

func validateSQLServer2022SourceCatalog(
	value sqlServer2022SourceCatalog,
) error {
	if value.productMajorVersion != sqlServer2022MajorVersion ||
		!strings.HasPrefix(
			value.productVersion,
			fmt.Sprintf("%d.", sqlServer2022MajorVersion),
		) {
		return sqlServerSourcePolicy(
			"catalog version",
			fmt.Sprintf(
				"version=%q major=%d; SQL Server 2022 is required",
				value.productVersion,
				value.productMajorVersion,
			),
		)
	}
	switch value.engineEdition {
	case 2, 3, 4:
	default:
		return sqlServerSourcePolicy(
			"engine edition",
			fmt.Sprintf(
				"edition=%q engine_edition=%d; Azure and unknown editions are unsupported",
				value.edition,
				value.engineEdition,
			),
		)
	}
	if !validSQLServerSourceIdentifier(value.databaseName) {
		return sqlServerSourcePolicy(
			"database identifier",
			"invalid database name",
		)
	}
	if value.compatibilityLevel != 160 {
		return sqlServerSourcePolicy(
			"database compatibility",
			fmt.Sprintf(
				"database=%q compatibility_level=%d; level 160 is required",
				value.databaseName,
				value.compatibilityLevel,
			),
		)
	}
	if value.state != "ONLINE" ||
		value.userAccess != "MULTI_USER" ||
		value.containment != "NONE" ||
		value.readOnly ||
		value.autoClose ||
		value.autoShrink ||
		value.standby ||
		value.sourceDatabaseID.Valid {
		return sqlServerSourcePolicy(
			"database catalog shape",
			value.databaseName,
		)
	}
	if value.published ||
		value.subscribed ||
		value.mergePublished ||
		value.distributor ||
		value.changeDataCapture {
		return sqlServerSourcePolicy(
			"database replication",
			value.databaseName,
		)
	}
	return nil
}

type sqlServerSourceTableCatalog struct {
	objectID                  int64
	typeDescription           string
	systemShipped             bool
	temporalType              int
	memoryOptimized           bool
	durability                string
	fileStreamDataSpaceID     sql.NullInt64
	fileTable                 bool
	replicated                bool
	mergePublished            bool
	syncTransactionSubscribed bool
	changeDataCapture         bool
	historyTableID            sql.NullInt64
	node                      bool
	edge                      bool
	ledgerType                int
	droppedLedgerTable        bool
	remoteDataArchive         bool
	external                  bool
	lockOnBulkLoad            bool
	columnCount               int
	maxPartition              int
	triggerCount              int
	securityPredicateCount    int
	fullTextIndexCount        int
	changeTrackingCount       int
	partitionSchemeCount      int
}

const sqlServerSourceTableCatalogQuery = `
	SELECT
		source_table.object_id,
		source_table.type_desc,
		source_table.is_ms_shipped,
		source_table.temporal_type,
		source_table.is_memory_optimized,
		source_table.durability_desc,
		source_table.filestream_data_space_id,
		source_table.is_filetable,
		source_table.is_replicated,
		source_table.is_merge_published,
		source_table.is_sync_tran_subscribed,
		source_table.is_tracked_by_cdc,
		source_table.history_table_id,
		source_table.is_node,
		source_table.is_edge,
		source_table.ledger_type,
		source_table.is_dropped_ledger_table,
		source_table.is_remote_data_archive_enabled,
		source_table.is_external,
		source_table.lock_on_bulk_load,
		(
			SELECT COUNT(*)
			FROM sys.columns AS source_column
			WHERE source_column.object_id = source_table.object_id
		),
		COALESCE((
			SELECT MAX(source_partition.partition_number)
			FROM sys.partitions AS source_partition
			WHERE source_partition.object_id = source_table.object_id
			  AND source_partition.index_id IN (0, 1)
		), 0),
		(
			SELECT COUNT(*)
			FROM sys.triggers AS source_trigger
			WHERE source_trigger.parent_id = source_table.object_id
			  AND source_trigger.is_ms_shipped = 0
		),
		(
			SELECT COUNT(*)
			FROM sys.security_predicates AS source_predicate
			WHERE source_predicate.target_object_id = source_table.object_id
		),
		(
			SELECT COUNT(*)
			FROM sys.fulltext_indexes AS source_fulltext
			WHERE source_fulltext.object_id = source_table.object_id
		),
		(
			SELECT COUNT(*)
			FROM sys.change_tracking_tables AS source_tracking
			WHERE source_tracking.object_id = source_table.object_id
		),
		(
			SELECT COUNT(*)
			FROM sys.indexes AS source_index
			JOIN sys.data_spaces AS source_space
			  ON source_space.data_space_id = source_index.data_space_id
			WHERE source_index.object_id = source_table.object_id
			  AND source_space.type = 'PS'
		)
	FROM sys.tables AS source_table
	JOIN sys.schemas AS source_schema
	  ON source_schema.schema_id = source_table.schema_id
	WHERE source_schema.name = @p1
	  AND source_table.name = @p2
`

func readSQLServerSourceTableCatalog(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (sqlServerSourceTableCatalog, error) {
	var result sqlServerSourceTableCatalog
	err := database.QueryRowContext(
		ctx,
		sqlServerSourceTableCatalogQuery,
		namespace,
		name,
	).Scan(
		&result.objectID,
		&result.typeDescription,
		&result.systemShipped,
		&result.temporalType,
		&result.memoryOptimized,
		&result.durability,
		&result.fileStreamDataSpaceID,
		&result.fileTable,
		&result.replicated,
		&result.mergePublished,
		&result.syncTransactionSubscribed,
		&result.changeDataCapture,
		&result.historyTableID,
		&result.node,
		&result.edge,
		&result.ledgerType,
		&result.droppedLedgerTable,
		&result.remoteDataArchive,
		&result.external,
		&result.lockOnBulkLoad,
		&result.columnCount,
		&result.maxPartition,
		&result.triggerCount,
		&result.securityPredicateCount,
		&result.fullTextIndexCount,
		&result.changeTrackingCount,
		&result.partitionSchemeCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlServerSourceTableCatalog{}, fmt.Errorf(
			"SQL Server table %s.%s does not exist",
			namespace,
			name,
		)
	}
	if err != nil {
		return sqlServerSourceTableCatalog{}, fmt.Errorf(
			"inspect SQL Server table %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if err := validateSQLServerSourceTableCatalog(
		namespace,
		name,
		result,
	); err != nil {
		return sqlServerSourceTableCatalog{}, err
	}
	return result, nil
}

func validateSQLServerSourceTableCatalog(
	namespace string,
	name string,
	value sqlServerSourceTableCatalog,
) error {
	identity := namespace + "." + name
	switch {
	case !validSQLServerSourceIdentifier(namespace) ||
		!validSQLServerSourceIdentifier(name):
		return sqlServerSourcePolicy("table identifier", identity)
	case value.objectID <= 0 ||
		value.typeDescription != "USER_TABLE" ||
		value.systemShipped:
		return sqlServerSourcePolicy("table catalog shape", identity)
	case value.temporalType != 0 || value.historyTableID.Valid:
		return sqlServerSourcePolicy("temporal table", identity)
	case value.memoryOptimized || value.durability != "SCHEMA_AND_DATA":
		return sqlServerSourcePolicy("table durability", identity)
	case value.fileStreamDataSpaceID.Valid || value.fileTable:
		return sqlServerSourcePolicy("FILESTREAM table", identity)
	case value.replicated || value.mergePublished ||
		value.syncTransactionSubscribed || value.changeDataCapture:
		return sqlServerSourcePolicy("table replication", identity)
	case value.node || value.edge:
		return sqlServerSourcePolicy("graph table", identity)
	case value.ledgerType != 0 || value.droppedLedgerTable:
		return sqlServerSourcePolicy("ledger table", identity)
	case value.remoteDataArchive || value.external:
		return sqlServerSourcePolicy("external table storage", identity)
	case value.lockOnBulkLoad:
		return sqlServerSourcePolicy("bulk-load table lock", identity)
	case value.columnCount <= 0:
		return sqlServerSourcePolicy("columns", identity)
	case value.maxPartition != 1 || value.partitionSchemeCount != 0:
		return sqlServerSourcePolicy("partitioning", identity)
	case value.triggerCount != 0:
		return sqlServerSourcePolicy(
			"triggers",
			fmt.Sprintf("%s count=%d", identity, value.triggerCount),
		)
	case value.securityPredicateCount != 0:
		return sqlServerSourcePolicy("row security", identity)
	case value.fullTextIndexCount != 0:
		return sqlServerSourcePolicy("full-text index", identity)
	case value.changeTrackingCount != 0:
		return sqlServerSourcePolicy("change tracking", identity)
	default:
		return nil
	}
}

type sqlServerSourceColumnCatalog struct {
	position                     int
	name                         string
	typeSchema                   string
	typeName                     string
	userDefined                  bool
	assemblyType                 bool
	maxLength                    int
	precision                    int
	scale                        int
	collation                    sql.NullString
	nullable                     bool
	ansiPadded                   bool
	rowGUID                      bool
	identity                     bool
	computed                     bool
	fileStream                   bool
	sparse                       bool
	columnSet                    bool
	replicated                   bool
	nonSQLSubscribed             bool
	mergePublished               bool
	dataTransformationReplicated bool
	generatedAlwaysType          int
	encryptionType               sql.NullInt64
	hidden                       bool
	masked                       bool
	graphType                    sql.NullInt64
	xmlCollectionID              int64
	defaultObjectID              int64
	ruleObjectID                 int64
	defaultName                  sql.NullString
	defaultDefinition            sql.NullString
	defaultSystemNamed           sql.NullBool
	identitySeed                 sql.NullInt64
	identityIncrement            sql.NullInt64
	identityLast                 sql.NullInt64
	identityNotForReplication    sql.NullBool
}

type sqlServerSourceIdentityCatalog struct {
	column   string
	frontier *int64
}

const sqlServerSourceColumnsQuery = `
	SELECT
		source_column.column_id,
		source_column.name,
		type_schema.name,
		source_type.name,
		source_type.is_user_defined,
		source_type.is_assembly_type,
		source_column.max_length,
		source_column.precision,
		source_column.scale,
		source_column.collation_name,
		source_column.is_nullable,
		source_column.is_ansi_padded,
		source_column.is_rowguidcol,
		source_column.is_identity,
		source_column.is_computed,
		source_column.is_filestream,
		source_column.is_sparse,
		source_column.is_column_set,
		source_column.is_replicated,
		source_column.is_non_sql_subscribed,
		source_column.is_merge_published,
		source_column.is_dts_replicated,
		source_column.generated_always_type,
		source_column.encryption_type,
		source_column.is_hidden,
		COALESCE(CONVERT(int, masked_column.is_masked), 0),
		source_column.graph_type,
		source_column.xml_collection_id,
		source_column.default_object_id,
		source_column.rule_object_id,
		source_default.name,
		source_default.definition,
		source_default.is_system_named,
		TRY_CONVERT(bigint, source_identity.seed_value),
		TRY_CONVERT(bigint, source_identity.increment_value),
		TRY_CONVERT(bigint, source_identity.last_value),
		source_identity.is_not_for_replication
	FROM sys.columns AS source_column
	JOIN sys.types AS source_type
	  ON source_type.user_type_id = source_column.user_type_id
	JOIN sys.schemas AS type_schema
	  ON type_schema.schema_id = source_type.schema_id
	LEFT JOIN sys.default_constraints AS source_default
	  ON source_default.object_id = source_column.default_object_id
	LEFT JOIN sys.identity_columns AS source_identity
	  ON source_identity.object_id = source_column.object_id
	 AND source_identity.column_id = source_column.column_id
	LEFT JOIN sys.masked_columns AS masked_column
	  ON masked_column.object_id = source_column.object_id
	 AND masked_column.column_id = source_column.column_id
	WHERE source_column.object_id = @p1
	ORDER BY source_column.column_id
`

func readSQLServerSourceColumns(
	ctx context.Context,
	database *sql.DB,
	table sqlServerSourceTableCatalog,
	namespace string,
	name string,
) (
	[]schema.Column,
	*sqlServerSourceIdentityCatalog,
	error,
) {
	rows, err := database.QueryContext(
		ctx,
		sqlServerSourceColumnsQuery,
		table.objectID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"list SQL Server columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	defer rows.Close()

	columns := make([]schema.Column, 0, table.columnCount)
	var identity *sqlServerSourceIdentityCatalog
	foldedNames := make(map[string]bool, table.columnCount)
	for rows.Next() {
		var catalog sqlServerSourceColumnCatalog
		if err := rows.Scan(
			&catalog.position,
			&catalog.name,
			&catalog.typeSchema,
			&catalog.typeName,
			&catalog.userDefined,
			&catalog.assemblyType,
			&catalog.maxLength,
			&catalog.precision,
			&catalog.scale,
			&catalog.collation,
			&catalog.nullable,
			&catalog.ansiPadded,
			&catalog.rowGUID,
			&catalog.identity,
			&catalog.computed,
			&catalog.fileStream,
			&catalog.sparse,
			&catalog.columnSet,
			&catalog.replicated,
			&catalog.nonSQLSubscribed,
			&catalog.mergePublished,
			&catalog.dataTransformationReplicated,
			&catalog.generatedAlwaysType,
			&catalog.encryptionType,
			&catalog.hidden,
			&catalog.masked,
			&catalog.graphType,
			&catalog.xmlCollectionID,
			&catalog.defaultObjectID,
			&catalog.ruleObjectID,
			&catalog.defaultName,
			&catalog.defaultDefinition,
			&catalog.defaultSystemNamed,
			&catalog.identitySeed,
			&catalog.identityIncrement,
			&catalog.identityLast,
			&catalog.identityNotForReplication,
		); err != nil {
			return nil, nil, fmt.Errorf(
				"read SQL Server column for %s.%s: %w",
				namespace,
				name,
				err,
			)
		}
		if catalog.position != len(columns)+1 {
			return nil, nil, sqlServerSourcePolicy(
				"column order",
				fmt.Sprintf(
					"%s.%s position=%d expected=%d",
					namespace,
					name,
					catalog.position,
					len(columns)+1,
				),
			)
		}
		folded := strings.ToLower(catalog.name)
		if foldedNames[folded] {
			return nil, nil, sqlServerSourcePolicy(
				"column identifier",
				namespace+"."+name+"."+catalog.name,
			)
		}
		foldedNames[folded] = true
		column, discoveredIdentity, err :=
			sqlServerSourceColumnFromCatalog(catalog)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"discover SQL Server column %s.%s.%s: %w",
				namespace,
				name,
				catalog.name,
				err,
			)
		}
		if discoveredIdentity != nil {
			if identity != nil {
				return nil, nil, sqlServerSourcePolicy(
					"identity",
					namespace+"."+name+" has multiple identity columns",
				)
			}
			identity = discoveredIdentity
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf(
			"iterate SQL Server columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if len(columns) != table.columnCount {
		return nil, nil, sqlServerSourcePolicy(
			"column catalog shape",
			fmt.Sprintf(
				"%s.%s table columns=%d discovered=%d",
				namespace,
				name,
				table.columnCount,
				len(columns),
			),
		)
	}
	return columns, identity, nil
}

func sqlServerSourceColumnFromCatalog(
	catalog sqlServerSourceColumnCatalog,
) (
	schema.Column,
	*sqlServerSourceIdentityCatalog,
	error,
) {
	if !validSQLServerSourceIdentifier(catalog.name) ||
		catalog.typeSchema != "sys" ||
		catalog.userDefined ||
		catalog.assemblyType ||
		catalog.rowGUID ||
		catalog.computed ||
		catalog.fileStream ||
		catalog.sparse ||
		catalog.columnSet ||
		catalog.replicated ||
		catalog.nonSQLSubscribed ||
		catalog.mergePublished ||
		catalog.dataTransformationReplicated ||
		catalog.generatedAlwaysType != 0 ||
		catalog.encryptionType.Valid ||
		catalog.hidden ||
		catalog.masked ||
		catalog.graphType.Valid ||
		catalog.xmlCollectionID != 0 ||
		catalog.ruleObjectID != 0 {
		return schema.Column{}, nil, sqlServerSourcePolicy(
			"column catalog shape",
			catalog.name+" "+catalog.typeName,
		)
	}
	if catalog.defaultObjectID == 0 {
		if catalog.defaultName.Valid ||
			catalog.defaultDefinition.Valid ||
			catalog.defaultSystemNamed.Valid {
			return schema.Column{}, nil, sqlServerSourcePolicy(
				"default catalog shape",
				catalog.name,
			)
		}
	} else if !catalog.defaultName.Valid ||
		!validSQLServerSourceIdentifier(catalog.defaultName.String) ||
		!catalog.defaultDefinition.Valid ||
		!catalog.defaultSystemNamed.Valid {
		return schema.Column{}, nil, sqlServerSourcePolicy(
			"default catalog shape",
			catalog.name,
		)
	}

	column := schema.Column{
		Name:     catalog.name,
		Nullable: catalog.nullable,
	}
	if err := applySQLServerSourceType(&column, catalog); err != nil {
		return schema.Column{}, nil, err
	}
	if catalog.defaultDefinition.Valid {
		value := catalog.defaultDefinition.String
		expression, err := schema.ParseSQLServerCatalogDefault(
			column,
			&value,
		)
		if err != nil {
			return schema.Column{}, nil, err
		}
		column.Default = expression
	}

	if !catalog.identity {
		if catalog.identitySeed.Valid ||
			catalog.identityIncrement.Valid ||
			catalog.identityLast.Valid ||
			catalog.identityNotForReplication.Valid {
			return schema.Column{}, nil, sqlServerSourcePolicy(
				"identity catalog shape",
				catalog.name,
			)
		}
		return column, nil, nil
	}
	if column.Nullable ||
		column.Default != nil ||
		catalog.typeName != "bigint" ||
		!catalog.identitySeed.Valid ||
		catalog.identitySeed.Int64 != 1 ||
		!catalog.identityIncrement.Valid ||
		catalog.identityIncrement.Int64 != 1 ||
		!catalog.identityNotForReplication.Valid ||
		catalog.identityNotForReplication.Bool {
		return schema.Column{}, nil, sqlServerSourcePolicy(
			"identity catalog shape",
			catalog.name,
		)
	}
	result := &sqlServerSourceIdentityCatalog{column: catalog.name}
	if catalog.identityLast.Valid {
		frontier := catalog.identityLast.Int64
		result.frontier = &frontier
	}
	return column, result, nil
}

func applySQLServerSourceType(
	column *schema.Column,
	catalog sqlServerSourceColumnCatalog,
) error {
	noCollation := func() bool { return !catalog.collation.Valid }
	exact := func(length, precision, scale int) bool {
		return catalog.maxLength == length &&
			catalog.precision == precision &&
			catalog.scale == scale &&
			!catalog.ansiPadded &&
			noCollation()
	}
	declaration := func(base string, arguments ...int) {
		column.DeclaredType = &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		}
	}
	unsupported := func() error {
		return sqlServerSourcePolicy(
			"column type",
			catalog.name+" "+catalog.typeName,
		)
	}

	switch catalog.typeName {
	case "tinyint":
		if !exact(1, 3, 0) {
			return unsupported()
		}
		column.Type = "integer"
		declaration("tinyint")
	case "smallint":
		if !exact(2, 5, 0) {
			return unsupported()
		}
		column.Type = "integer"
		declaration("smallint")
	case "int":
		if !exact(4, 10, 0) {
			return unsupported()
		}
		column.Type = "integer"
		declaration("int")
	case "bigint":
		if !exact(8, 19, 0) {
			return unsupported()
		}
		column.Type = "bigint"
		declaration("bigint")
	case "bit":
		if !exact(1, 1, 0) {
			return unsupported()
		}
		column.Type = "boolean"
		declaration("bool")
	case "decimal", "numeric":
		if !noCollation() ||
			catalog.ansiPadded ||
			catalog.precision < 1 ||
			catalog.precision > 38 ||
			catalog.scale < 0 ||
			catalog.scale > catalog.precision ||
			catalog.maxLength != sqlServerDecimalStorageBytes(
				catalog.precision,
			) {
			return unsupported()
		}
		column.Type = "numeric"
		declaration(
			catalog.typeName,
			catalog.precision,
			catalog.scale,
		)
	case "real":
		if !exact(4, 24, 0) {
			return unsupported()
		}
		column.Type = "real"
		declaration("real")
	case "float":
		if !exact(8, 53, 0) {
			return unsupported()
		}
		column.Type = "double precision"
		declaration("double precision")
	case "char", "varchar":
		if err := applySQLServerSourceTextType(
			column,
			catalog,
		); err != nil {
			return err
		}
	case "binary", "varbinary":
		if !noCollation() ||
			!catalog.ansiPadded ||
			(catalog.maxLength < 1 ||
				catalog.maxLength > 8_000) &&
				catalog.maxLength != -1 ||
			catalog.maxLength == -1 &&
				catalog.typeName != "varbinary" ||
			catalog.precision != 0 ||
			catalog.scale != 0 {
			return unsupported()
		}
		column.Type = "blob"
		if catalog.maxLength == -1 {
			declaration("blob")
		} else {
			declaration(catalog.typeName, catalog.maxLength)
		}
	case "date":
		if !exact(3, 10, 0) {
			return unsupported()
		}
		column.Type = "date"
		declaration("date")
	case "time":
		if !noCollation() ||
			catalog.ansiPadded ||
			catalog.scale < 0 ||
			catalog.scale > 6 ||
			catalog.precision != sqlServerTemporalPrecision(
				"time",
				catalog.scale,
			) ||
			catalog.maxLength != sqlServerTemporalStorageBytes(
				"time",
				catalog.scale,
			) {
			return unsupported()
		}
		column.Type = "time"
		declaration("time", catalog.scale)
	case "datetime2":
		if !noCollation() ||
			catalog.ansiPadded ||
			catalog.scale < 0 ||
			catalog.scale > 6 ||
			catalog.precision != sqlServerTemporalPrecision(
				"datetime2",
				catalog.scale,
			) ||
			catalog.maxLength != sqlServerTemporalStorageBytes(
				"datetime2",
				catalog.scale,
			) {
			return unsupported()
		}
		column.Type = "datetime"
		declaration("timestamp", catalog.scale)
	case "smalldatetime":
		if !exact(4, 16, 0) {
			return unsupported()
		}
		column.Type = "datetime"
		declaration("smalldatetime")
	case "uniqueidentifier":
		if !exact(16, 0, 0) {
			return unsupported()
		}
		column.Type = "uuid"
		declaration("uuid")
	default:
		return unsupported()
	}
	return nil
}

func applySQLServerSourceTextType(
	column *schema.Column,
	catalog sqlServerSourceColumnCatalog,
) error {
	if !catalog.collation.Valid ||
		!sqlServerPortableTextColumnCollation(
			catalog.typeName,
			catalog.collation.String,
		) ||
		!catalog.ansiPadded ||
		catalog.precision != 0 ||
		catalog.scale != 0 {
		return sqlServerSourcePolicy(
			"column type",
			catalog.name+" "+catalog.typeName,
		)
	}
	length := catalog.maxLength
	if length == -1 && catalog.typeName != "varchar" {
		return sqlServerSourcePolicy(
			"column type",
			catalog.name+" "+catalog.typeName,
		)
	}
	if length != -1 && (length < 1 || length > 8_000) {
		return sqlServerSourcePolicy(
			"column type",
			catalog.name+" "+catalog.typeName,
		)
	}
	column.Type = "text"
	if length == -1 {
		column.DeclaredType = &schema.DeclaredType{Base: "text"}
		return nil
	}
	column.DeclaredType = &schema.DeclaredType{
		Base:      catalog.typeName,
		Arguments: []int{length},
	}
	return nil
}

func sqlServerPortableTextColumnCollation(
	typeName string,
	collation string,
) bool {
	switch typeName {
	case "char", "varchar":
		return strings.EqualFold(
			strings.TrimSpace(collation),
			"Latin1_General_100_BIN2_UTF8",
		)
	default:
		return false
	}
}

func sqlServerDecimalStorageBytes(precision int) int {
	switch {
	case precision <= 9:
		return 5
	case precision <= 19:
		return 9
	case precision <= 28:
		return 13
	default:
		return 17
	}
}

func sqlServerTemporalStorageBytes(base string, scale int) int {
	fractional := 3
	switch {
	case scale <= 2:
		fractional = 3
	case scale <= 4:
		fractional = 4
	default:
		fractional = 5
	}
	switch base {
	case "time":
		return fractional
	case "datetime2":
		return fractional + 3
	default:
		return 0
	}
}

func sqlServerTemporalPrecision(base string, scale int) int {
	basePrecision := 0
	switch base {
	case "time":
		basePrecision = 8
	case "datetime2":
		basePrecision = 19
	default:
		return 0
	}
	if scale == 0 {
		return basePrecision
	}
	return basePrecision + 1 + scale
}

func inspectSQLServer2022Table(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (schema.Table, error) {
	first, firstObjectID, err := inspectSQLServer2022TableOnce(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	second, secondObjectID, err := inspectSQLServer2022TableOnce(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	if firstObjectID != secondObjectID ||
		!reflect.DeepEqual(first, second) {
		return schema.Table{}, sqlServerSourcePolicy(
			"stable table catalog",
			namespace+"."+name,
		)
	}
	return second, nil
}

func inspectSQLServer2022TableOnce(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (schema.Table, int64, error) {
	catalog, err := readSQLServerSourceTableCatalog(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, 0, err
	}
	columns, identityCatalog, err := readSQLServerSourceColumns(
		ctx,
		database,
		catalog,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, 0, err
	}
	table := schema.Table{
		Schema:  namespace,
		Name:    name,
		Columns: columns,
	}
	if err := discoverSQLServerSourcePrimaryKey(
		ctx,
		database,
		&table,
		catalog.objectID,
	); err != nil {
		return schema.Table{}, 0, err
	}
	if identityCatalog != nil {
		if err := applySQLServerSourceIdentity(
			&table,
			identityCatalog,
		); err != nil {
			return schema.Table{}, 0, err
		}
	}
	table.Indexes, err = discoverSQLServerSourceIndexes(
		ctx,
		database,
		table,
		catalog.objectID,
	)
	if err != nil {
		return schema.Table{}, 0, err
	}
	table.Checks, err = discoverSQLServerSourceChecks(
		ctx,
		database,
		table,
		catalog.objectID,
	)
	if err != nil {
		return schema.Table{}, 0, err
	}
	table.ForeignKeys, err = discoverSQLServerSourceForeignKeys(
		ctx,
		database,
		table,
		catalog.objectID,
	)
	if err != nil {
		return schema.Table{}, 0, err
	}
	if err := validateSQLServerSourceObjectNames(table); err != nil {
		return schema.Table{}, 0, err
	}
	return table, catalog.objectID, nil
}

func applySQLServerSourceIdentity(
	table *schema.Table,
	catalog *sqlServerSourceIdentityCatalog,
) error {
	if catalog == nil {
		return nil
	}
	for _, column := range table.Columns {
		if column.Name != catalog.column {
			continue
		}
		if column.PrimaryKeyPosition != 1 ||
			len(orderedSQLServerPrimaryKeyColumns(*table)) != 1 ||
			column.Nullable ||
			column.Default != nil ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != "bigint" ||
			len(column.DeclaredType.Arguments) != 0 {
			break
		}
		table.Identity = &schema.Identity{
			Column:     catalog.column,
			Generation: schema.IdentityByDefault,
			Frontier:   catalog.frontier,
		}
		return nil
	}
	return sqlServerSourcePolicy(
		"identity",
		table.Schema+"."+table.Name+"."+catalog.column,
	)
}

func orderedSQLServerPrimaryKeyColumns(
	table schema.Table,
) []schema.Column {
	result := make(
		[]schema.Column,
		0,
		len(table.Columns),
	)
	for _, column := range table.Columns {
		if column.PrimaryKeyPosition > 0 {
			result = append(result, column)
		}
	}
	for left := 0; left < len(result); left++ {
		for right := left + 1; right < len(result); right++ {
			if result[right].PrimaryKeyPosition <
				result[left].PrimaryKeyPosition {
				result[left], result[right] =
					result[right], result[left]
			}
		}
	}
	return result
}

func validSQLServerSourceIdentifier(value string) bool {
	return value != "" &&
		utf8.ValidString(value) &&
		!strings.ContainsRune(value, '\x00') &&
		!strings.ContainsRune(value, '\uFFFD') &&
		!strings.HasSuffix(value, " ") &&
		utf8.RuneCountInString(value) <= 128
}

func sqlServerSourcePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: "discover SQL Server " + operation,
		Type:      value,
		Target:    string(schema.SQLServer),
	}
}
