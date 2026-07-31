package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type sqlServerSourcePrimaryKeyColumn struct {
	indexColumnID      int
	keyOrdinal         int
	partitionOrdinal   int
	columnStoreOrdinal int
	descending         bool
	included           bool
	columnID           int
	name               string
	collation          sql.NullString
}

type sqlServerSourcePrimaryKeyCatalog struct {
	tableObjectID        int64
	namespace            string
	table                string
	objectID             int64
	name                 string
	typ                  string
	typeDescription      string
	parentObjectID       int64
	indexID              int
	indexName            string
	indexType            int
	indexTypeDescription string
	unique               bool
	primary              bool
	uniqueConstraint     bool
	disabled             bool
	hypothetical         bool
	filtered             bool
	filterDefinition     sql.NullString
	ignoreDuplicateKey   bool
	columns              []sqlServerSourcePrimaryKeyColumn
}

const sqlServerSourcePrimaryKeyQuery = `
	SELECT
		source_table.object_id,
		source_schema.name,
		source_table.name,
		source_constraint.object_id,
		source_constraint.name,
		source_constraint.type,
		source_constraint.type_desc,
		source_constraint.parent_object_id,
		source_index.index_id,
		source_index.name,
		source_index.type,
		source_index.type_desc,
		source_index.is_unique,
		source_index.is_primary_key,
		source_index.is_unique_constraint,
		source_index.is_disabled,
		source_index.is_hypothetical,
		source_index.has_filter,
		source_index.filter_definition,
		source_index.ignore_dup_key,
		source_index_column.index_column_id,
		source_index_column.key_ordinal,
		source_index_column.partition_ordinal,
		source_index_column.column_store_order_ordinal,
		source_index_column.is_descending_key,
		source_index_column.is_included_column,
		source_column.column_id,
		source_column.name,
		source_column.collation_name
	FROM sys.tables AS source_table
	JOIN sys.schemas AS source_schema
	  ON source_schema.schema_id = source_table.schema_id
	JOIN sys.key_constraints AS source_constraint
	  ON source_constraint.parent_object_id = source_table.object_id
	 AND source_constraint.type = 'PK'
	JOIN sys.indexes AS source_index
	  ON source_index.object_id = source_constraint.parent_object_id
	 AND source_index.index_id = source_constraint.unique_index_id
	JOIN sys.index_columns AS source_index_column
	  ON source_index_column.object_id = source_index.object_id
	 AND source_index_column.index_id = source_index.index_id
	JOIN sys.columns AS source_column
	  ON source_column.object_id = source_index_column.object_id
	 AND source_column.column_id = source_index_column.column_id
	WHERE source_schema.name = @p1
	  AND source_table.name = @p2
	  AND source_table.object_id = @p3
	ORDER BY
		source_constraint.object_id,
		source_index_column.index_column_id
`

// discoverSQLServerSourcePrimaryKey discovers one ordered SQL Server primary
// key and updates the table only after every catalog row has passed the
// fail-closed SQL Server 2022 contract.
func discoverSQLServerSourcePrimaryKey(
	ctx context.Context,
	database *sql.DB,
	table *schema.Table,
	tableObjectID int64,
) error {
	rows, err := database.QueryContext(
		ctx,
		sqlServerSourcePrimaryKeyQuery,
		table.Schema,
		table.Name,
		tableObjectID,
	)
	if err != nil {
		return fmt.Errorf(
			"list SQL Server primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []sqlServerSourcePrimaryKeyCatalog
	for rows.Next() {
		var (
			catalog sqlServerSourcePrimaryKeyCatalog
			column  sqlServerSourcePrimaryKeyColumn
		)
		if err := rows.Scan(
			&catalog.tableObjectID,
			&catalog.namespace,
			&catalog.table,
			&catalog.objectID,
			&catalog.name,
			&catalog.typ,
			&catalog.typeDescription,
			&catalog.parentObjectID,
			&catalog.indexID,
			&catalog.indexName,
			&catalog.indexType,
			&catalog.indexTypeDescription,
			&catalog.unique,
			&catalog.primary,
			&catalog.uniqueConstraint,
			&catalog.disabled,
			&catalog.hypothetical,
			&catalog.filtered,
			&catalog.filterDefinition,
			&catalog.ignoreDuplicateKey,
			&column.indexColumnID,
			&column.keyOrdinal,
			&column.partitionOrdinal,
			&column.columnStoreOrdinal,
			&column.descending,
			&column.included,
			&column.columnID,
			&column.name,
			&column.collation,
		); err != nil {
			return fmt.Errorf(
				"read SQL Server primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.tableObjectID != tableObjectID {
			return sqlServerSourcePolicy(
				"primary-key table identity",
				sqlServerSourceIdentity(*table, catalog.name),
			)
		}
		if len(catalogs) == 0 ||
			catalogs[len(catalogs)-1].objectID != catalog.objectID {
			catalog.columns = nil
			catalogs = append(catalogs, catalog)
		} else if !sameSQLServerPrimaryKeyCatalog(
			catalogs[len(catalogs)-1],
			catalog,
		) {
			return sqlServerSourcePolicy(
				"primary-key catalog shape",
				sqlServerSourceIdentity(*table, catalog.name),
			)
		}
		current := &catalogs[len(catalogs)-1]
		current.columns = append(current.columns, column)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQL Server primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if len(catalogs) == 0 {
		return nil
	}
	if len(catalogs) != 1 {
		return sqlServerSourcePolicy(
			"primary-key catalog shape",
			sqlServerSourceIdentity(*table, ""),
		)
	}
	positions, err := sqlServerSourcePrimaryKeyFromCatalog(
		*table,
		catalogs[0],
	)
	if err != nil {
		return err
	}
	for index := range table.Columns {
		position := positions[table.Columns[index].Name]
		if position == 0 {
			continue
		}
		table.Columns[index].PrimaryKey = true
		table.Columns[index].PrimaryKeyPosition = position
	}
	return nil
}

func sameSQLServerPrimaryKeyCatalog(
	left, right sqlServerSourcePrimaryKeyCatalog,
) bool {
	return left.tableObjectID == right.tableObjectID &&
		left.namespace == right.namespace &&
		left.table == right.table &&
		left.objectID == right.objectID &&
		left.name == right.name &&
		left.typ == right.typ &&
		left.typeDescription == right.typeDescription &&
		left.parentObjectID == right.parentObjectID &&
		left.indexID == right.indexID &&
		left.indexName == right.indexName &&
		left.indexType == right.indexType &&
		left.indexTypeDescription == right.indexTypeDescription &&
		left.unique == right.unique &&
		left.primary == right.primary &&
		left.uniqueConstraint == right.uniqueConstraint &&
		left.disabled == right.disabled &&
		left.hypothetical == right.hypothetical &&
		left.filtered == right.filtered &&
		left.filterDefinition == right.filterDefinition &&
		left.ignoreDuplicateKey == right.ignoreDuplicateKey
}

func sqlServerSourcePrimaryKeyFromCatalog(
	table schema.Table,
	catalog sqlServerSourcePrimaryKeyCatalog,
) (map[string]int, error) {
	identity := sqlServerSourceIdentity(table, catalog.name)
	if catalog.tableObjectID <= 0 ||
		catalog.namespace != table.Schema ||
		catalog.table != table.Name ||
		catalog.objectID <= 0 ||
		!validSQLServerSourceIdentifier(catalog.name) ||
		catalog.typ != "PK" ||
		catalog.typeDescription != "PRIMARY_KEY_CONSTRAINT" ||
		catalog.parentObjectID != catalog.tableObjectID ||
		catalog.indexID <= 0 ||
		catalog.indexName != catalog.name ||
		catalog.indexType != 1 ||
		catalog.indexTypeDescription != "CLUSTERED" ||
		!catalog.unique ||
		!catalog.primary ||
		catalog.uniqueConstraint ||
		catalog.disabled ||
		catalog.hypothetical ||
		catalog.filtered ||
		catalog.filterDefinition.Valid ||
		catalog.ignoreDuplicateKey ||
		len(catalog.columns) == 0 {
		return nil, sqlServerSourcePolicy(
			"primary-key catalog shape",
			identity,
		)
	}
	positions := make(map[string]int, len(catalog.columns))
	for index, source := range catalog.columns {
		column, exists := findSQLServerSourceColumn(table, source.name)
		// The neutral primary-key model cannot carry SQL Server's comparison
		// semantics. Text, binary, and uniqueidentifier keys all compare
		// differently in PostgreSQL even when their copied scalar values are
		// lossless, so they remain outside this certified route.
		if source.indexColumnID != index+1 ||
			source.keyOrdinal != index+1 ||
			source.partitionOrdinal != 0 ||
			source.columnStoreOrdinal != 0 ||
			source.descending ||
			source.included ||
			source.columnID <= 0 ||
			!exists ||
			column.Nullable ||
			sqlServerSourceColumnHasNonportableComparison(column) ||
			source.collation.Valid !=
				sqlServerSourceColumnIsText(column) {
			return nil, sqlServerSourcePolicy(
				"primary-key column catalog shape",
				identity+"."+source.name,
			)
		}
		if caseFoldedNameExists(positions, source.name) {
			return nil, sqlServerSourcePolicy(
				"primary-key columns",
				identity+"."+source.name,
			)
		}
		positions[source.name] = index + 1
	}
	return positions, nil
}

type sqlServerSourceIndexColumnCatalog struct {
	indexColumnID      int
	keyOrdinal         int
	partitionOrdinal   int
	columnStoreOrdinal int
	descending         bool
	included           bool
	columnID           int
	name               string
	collation          sql.NullString
}

type sqlServerSourceIndexCatalog struct {
	tableObjectID      int64
	namespace          string
	table              string
	indexID            int
	name               string
	typ                int
	typeDescription    string
	unique             bool
	primary            bool
	uniqueConstraint   bool
	disabled           bool
	hypothetical       bool
	filtered           bool
	filterDefinition   sql.NullString
	ignoreDuplicateKey bool
	columns            []sqlServerSourceIndexColumnCatalog
}

const sqlServerSourceIndexesQuery = `
	SELECT
		source_table.object_id,
		source_schema.name,
		source_table.name,
		source_index.index_id,
		source_index.name,
		source_index.type,
		source_index.type_desc,
		source_index.is_unique,
		source_index.is_primary_key,
		source_index.is_unique_constraint,
		source_index.is_disabled,
		source_index.is_hypothetical,
		source_index.has_filter,
		source_index.filter_definition,
		source_index.ignore_dup_key,
		source_index_column.index_column_id,
		source_index_column.key_ordinal,
		source_index_column.partition_ordinal,
		source_index_column.column_store_order_ordinal,
		source_index_column.is_descending_key,
		source_index_column.is_included_column,
		source_column.column_id,
		source_column.name,
		source_column.collation_name
	FROM sys.tables AS source_table
	JOIN sys.schemas AS source_schema
	  ON source_schema.schema_id = source_table.schema_id
	JOIN sys.indexes AS source_index
	  ON source_index.object_id = source_table.object_id
	JOIN sys.index_columns AS source_index_column
	  ON source_index_column.object_id = source_index.object_id
	 AND source_index_column.index_id = source_index.index_id
	JOIN sys.columns AS source_column
	  ON source_column.object_id = source_index_column.object_id
	 AND source_column.column_id = source_index_column.column_id
	WHERE source_schema.name = @p1
	  AND source_table.name = @p2
	  AND source_table.object_id = @p3
	  AND source_index.index_id > 0
	  AND source_index.is_primary_key = 0
	ORDER BY
		source_index.index_id,
		source_index_column.index_column_id
`

func discoverSQLServerSourceIndexes(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
	tableObjectID int64,
) ([]schema.Index, error) {
	rows, err := database.QueryContext(
		ctx,
		sqlServerSourceIndexesQuery,
		table.Schema,
		table.Name,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list SQL Server indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []sqlServerSourceIndexCatalog
	for rows.Next() {
		var (
			catalog sqlServerSourceIndexCatalog
			column  sqlServerSourceIndexColumnCatalog
		)
		if err := rows.Scan(
			&catalog.tableObjectID,
			&catalog.namespace,
			&catalog.table,
			&catalog.indexID,
			&catalog.name,
			&catalog.typ,
			&catalog.typeDescription,
			&catalog.unique,
			&catalog.primary,
			&catalog.uniqueConstraint,
			&catalog.disabled,
			&catalog.hypothetical,
			&catalog.filtered,
			&catalog.filterDefinition,
			&catalog.ignoreDuplicateKey,
			&column.indexColumnID,
			&column.keyOrdinal,
			&column.partitionOrdinal,
			&column.columnStoreOrdinal,
			&column.descending,
			&column.included,
			&column.columnID,
			&column.name,
			&column.collation,
		); err != nil {
			return nil, fmt.Errorf(
				"read SQL Server index for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.tableObjectID != tableObjectID {
			return nil, sqlServerSourcePolicy(
				"index table identity",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		if len(catalogs) == 0 ||
			catalogs[len(catalogs)-1].indexID != catalog.indexID {
			catalog.columns = nil
			catalogs = append(catalogs, catalog)
		} else if !sameSQLServerIndexCatalog(
			catalogs[len(catalogs)-1],
			catalog,
		) {
			return nil, sqlServerSourcePolicy(
				"index catalog shape",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		current := &catalogs[len(catalogs)-1]
		current.columns = append(current.columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate SQL Server indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}

	result := make([]schema.Index, 0, len(catalogs))
	names := make(map[string]int, len(catalogs))
	ids := make(map[int]struct{}, len(catalogs))
	for _, catalog := range catalogs {
		if _, duplicate := ids[catalog.indexID]; duplicate {
			return nil, sqlServerSourcePolicy(
				"index catalog order",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		ids[catalog.indexID] = struct{}{}
		index, err := sqlServerSourceIndexFromCatalog(table, catalog)
		if err != nil {
			return nil, err
		}
		if caseFoldedNameExists(names, index.Name) {
			return nil, sqlServerSourcePolicy(
				"index names",
				sqlServerSourceIdentity(table, index.Name),
			)
		}
		names[index.Name] = len(result)
		result = append(result, index)
	}
	return result, nil
}

func sameSQLServerIndexCatalog(
	left, right sqlServerSourceIndexCatalog,
) bool {
	return left.tableObjectID == right.tableObjectID &&
		left.namespace == right.namespace &&
		left.table == right.table &&
		left.indexID == right.indexID &&
		left.name == right.name &&
		left.typ == right.typ &&
		left.typeDescription == right.typeDescription &&
		left.unique == right.unique &&
		left.primary == right.primary &&
		left.uniqueConstraint == right.uniqueConstraint &&
		left.disabled == right.disabled &&
		left.hypothetical == right.hypothetical &&
		left.filtered == right.filtered &&
		left.filterDefinition == right.filterDefinition &&
		left.ignoreDuplicateKey == right.ignoreDuplicateKey
}

func sqlServerSourceIndexFromCatalog(
	table schema.Table,
	catalog sqlServerSourceIndexCatalog,
) (schema.Index, error) {
	identity := sqlServerSourceIdentity(table, catalog.name)
	if catalog.tableObjectID <= 0 ||
		catalog.namespace != table.Schema ||
		catalog.table != table.Name ||
		catalog.indexID <= 0 ||
		!validSQLServerSourceIdentifier(catalog.name) ||
		catalog.typ != 2 ||
		catalog.typeDescription != "NONCLUSTERED" ||
		catalog.primary ||
		catalog.uniqueConstraint ||
		catalog.disabled ||
		catalog.hypothetical ||
		catalog.filtered ||
		catalog.filterDefinition.Valid ||
		catalog.ignoreDuplicateKey ||
		len(catalog.columns) == 0 {
		return schema.Index{}, sqlServerSourcePolicy(
			"index catalog shape",
			identity,
		)
	}
	result := schema.Index{
		Name:    catalog.name,
		Unique:  catalog.unique,
		Columns: make([]schema.IndexColumn, len(catalog.columns)),
	}
	names := make(map[string]int, len(catalog.columns))
	for position, source := range catalog.columns {
		column, exists := findSQLServerSourceColumn(table, source.name)
		if source.indexColumnID != position+1 ||
			source.keyOrdinal != position+1 ||
			source.partitionOrdinal != 0 ||
			source.columnStoreOrdinal != 0 ||
			source.included ||
			source.columnID <= 0 ||
			!exists ||
			catalog.unique && column.Nullable ||
			sqlServerSourceColumnHasNonportableComparison(column) ||
			source.collation.Valid !=
				sqlServerSourceColumnIsText(column) {
			return schema.Index{}, sqlServerSourcePolicy(
				"index column catalog shape",
				identity+"."+source.name,
			)
		}
		if caseFoldedNameExists(names, source.name) {
			return schema.Index{}, sqlServerSourcePolicy(
				"index columns",
				identity+"."+source.name,
			)
		}
		names[source.name] = position
		result.Columns[position] = schema.IndexColumn{
			Name:       source.name,
			Descending: source.descending,
		}
	}
	return result, nil
}

type sqlServerSourceCheckCatalog struct {
	tableObjectID         int64
	namespace             string
	table                 string
	objectID              int64
	name                  string
	typ                   string
	typeDescription       string
	parentObjectID        int64
	parentColumnID        int
	parentColumn          sql.NullString
	disabled              bool
	notTrusted            bool
	notForReplication     bool
	usesDatabaseCollation bool
	definition            sql.NullString
}

const sqlServerSourceChecksQuery = `
	SELECT
		source_table.object_id,
		source_schema.name,
		source_table.name,
		source_check.object_id,
		source_check.name,
		RTRIM(source_check.type),
		source_check.type_desc,
		source_check.parent_object_id,
		source_check.parent_column_id,
		source_column.name,
		source_check.is_disabled,
		source_check.is_not_trusted,
		source_check.is_not_for_replication,
		source_check.uses_database_collation,
		source_check.definition
	FROM sys.tables AS source_table
	JOIN sys.schemas AS source_schema
	  ON source_schema.schema_id = source_table.schema_id
	JOIN sys.check_constraints AS source_check
	  ON source_check.parent_object_id = source_table.object_id
	LEFT JOIN sys.columns AS source_column
	  ON source_column.object_id = source_check.parent_object_id
	 AND source_column.column_id = source_check.parent_column_id
	WHERE source_schema.name = @p1
	  AND source_table.name = @p2
	  AND source_table.object_id = @p3
	ORDER BY source_check.object_id
`

func discoverSQLServerSourceChecks(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
	tableObjectID int64,
) ([]schema.CheckConstraint, error) {
	rows, err := database.QueryContext(
		ctx,
		sqlServerSourceChecksQuery,
		table.Schema,
		table.Name,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list SQL Server CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []sqlServerSourceCheckCatalog
	for rows.Next() {
		var catalog sqlServerSourceCheckCatalog
		if err := rows.Scan(
			&catalog.tableObjectID,
			&catalog.namespace,
			&catalog.table,
			&catalog.objectID,
			&catalog.name,
			&catalog.typ,
			&catalog.typeDescription,
			&catalog.parentObjectID,
			&catalog.parentColumnID,
			&catalog.parentColumn,
			&catalog.disabled,
			&catalog.notTrusted,
			&catalog.notForReplication,
			&catalog.usesDatabaseCollation,
			&catalog.definition,
		); err != nil {
			return nil, fmt.Errorf(
				"read SQL Server CHECK constraint for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.tableObjectID != tableObjectID {
			return nil, sqlServerSourcePolicy(
				"CHECK constraint table identity",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		catalogs = append(catalogs, catalog)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate SQL Server CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}

	result := make([]schema.CheckConstraint, 0, len(catalogs))
	names := make(map[string]int, len(catalogs))
	ids := make(map[int64]struct{}, len(catalogs))
	for _, catalog := range catalogs {
		if _, duplicate := ids[catalog.objectID]; duplicate {
			return nil, sqlServerSourcePolicy(
				"CHECK constraint catalog order",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		ids[catalog.objectID] = struct{}{}
		check, err := sqlServerSourceCheckFromCatalog(table, catalog)
		if err != nil {
			return nil, err
		}
		if caseFoldedNameExists(names, check.Name) {
			return nil, sqlServerSourcePolicy(
				"CHECK constraint names",
				sqlServerSourceIdentity(table, check.Name),
			)
		}
		names[check.Name] = len(result)
		result = append(result, check)
	}
	return result, nil
}

func sqlServerSourceCheckFromCatalog(
	table schema.Table,
	catalog sqlServerSourceCheckCatalog,
) (schema.CheckConstraint, error) {
	identity := sqlServerSourceIdentity(table, catalog.name)
	if catalog.tableObjectID <= 0 ||
		catalog.namespace != table.Schema ||
		catalog.table != table.Name ||
		catalog.objectID <= 0 ||
		!validSQLServerSourceIdentifier(catalog.name) ||
		catalog.typ != "C" ||
		catalog.typeDescription != "CHECK_CONSTRAINT" ||
		catalog.parentObjectID != catalog.tableObjectID ||
		catalog.parentColumnID < 0 ||
		catalog.disabled ||
		catalog.notTrusted ||
		catalog.notForReplication ||
		!catalog.definition.Valid ||
		strings.TrimSpace(catalog.definition.String) == "" {
		return schema.CheckConstraint{}, sqlServerSourcePolicy(
			"CHECK constraint catalog shape",
			identity,
		)
	}
	// SQL Server 2022 marks ordinary identifier-bearing CHECK constraints as
	// using the database collation even when their expression is numeric-only.
	// That catalog bit therefore cannot distinguish portable checks. Portability
	// is instead established by the typed expression parser below and by the
	// binary-collation admission applied to every discovered text column.
	if catalog.parentColumnID == 0 {
		if catalog.parentColumn.Valid {
			return schema.CheckConstraint{}, sqlServerSourcePolicy(
				"CHECK constraint parent column",
				identity,
			)
		}
	} else {
		if !catalog.parentColumn.Valid {
			return schema.CheckConstraint{}, sqlServerSourcePolicy(
				"CHECK constraint parent column",
				identity,
			)
		}
		if _, exists := findSQLServerSourceColumn(
			table,
			catalog.parentColumn.String,
		); !exists {
			return schema.CheckConstraint{}, sqlServerSourcePolicy(
				"CHECK constraint parent column",
				identity+"."+catalog.parentColumn.String,
			)
		}
	}
	expression, err := schema.ParseSQLServerCatalogCheck(
		catalog.definition.String,
		table.Columns,
	)
	if err != nil {
		return schema.CheckConstraint{}, fmt.Errorf(
			"discover SQL Server CHECK constraint %s: %w",
			identity,
			err,
		)
	}
	return schema.CheckConstraint{
		Name:       catalog.name,
		Expression: expression,
	}, nil
}

type sqlServerSourceForeignKeyColumn struct {
	position           int
	parentObjectID     int64
	parentColumnID     int
	parentColumn       string
	referencedObjectID int64
	referencedColumnID int
	referencedColumn   string
}

type sqlServerSourceForeignKeyCatalog struct {
	databaseID                     int
	tableObjectID                  int64
	namespace                      string
	table                          string
	objectID                       int64
	name                           string
	typ                            string
	typeDescription                string
	parentObjectID                 int64
	referencedObjectID             int64
	referencedNamespace            string
	referencedTable                string
	keyIndexID                     int
	disabled                       bool
	notTrusted                     bool
	notForReplication              bool
	updateAction                   int
	updateActionDescription        string
	deleteAction                   int
	deleteActionDescription        string
	referencedIndexType            int
	referencedIndexTypeDescription string
	referencedIndexUnique          bool
	referencedIndexDisabled        bool
	referencedIndexHypothetical    bool
	referencedIndexFiltered        bool
	referencedIndexFilter          sql.NullString
	columns                        []sqlServerSourceForeignKeyColumn
}

const sqlServerSourceForeignKeysQuery = `
	SELECT
		DB_ID(),
		source_table.object_id,
		source_schema.name,
		source_table.name,
		source_foreign_key.object_id,
		source_foreign_key.name,
		RTRIM(source_foreign_key.type),
		source_foreign_key.type_desc,
		source_foreign_key.parent_object_id,
		source_foreign_key.referenced_object_id,
		referenced_schema.name,
		referenced_table.name,
		source_foreign_key.key_index_id,
		source_foreign_key.is_disabled,
		source_foreign_key.is_not_trusted,
		source_foreign_key.is_not_for_replication,
		source_foreign_key.update_referential_action,
		source_foreign_key.update_referential_action_desc,
		source_foreign_key.delete_referential_action,
		source_foreign_key.delete_referential_action_desc,
		referenced_index.type,
		referenced_index.type_desc,
		referenced_index.is_unique,
		referenced_index.is_disabled,
		referenced_index.is_hypothetical,
		referenced_index.has_filter,
		referenced_index.filter_definition,
		source_foreign_key_column.constraint_column_id,
		source_foreign_key_column.parent_object_id,
		source_foreign_key_column.parent_column_id,
		parent_column.name,
		source_foreign_key_column.referenced_object_id,
		source_foreign_key_column.referenced_column_id,
		referenced_column.name
	FROM sys.tables AS source_table
	JOIN sys.schemas AS source_schema
	  ON source_schema.schema_id = source_table.schema_id
	JOIN sys.foreign_keys AS source_foreign_key
	  ON source_foreign_key.parent_object_id = source_table.object_id
	JOIN sys.tables AS referenced_table
	  ON referenced_table.object_id =
	     source_foreign_key.referenced_object_id
	JOIN sys.schemas AS referenced_schema
	  ON referenced_schema.schema_id = referenced_table.schema_id
	JOIN sys.indexes AS referenced_index
	  ON referenced_index.object_id =
	     source_foreign_key.referenced_object_id
	 AND referenced_index.index_id = source_foreign_key.key_index_id
	JOIN sys.foreign_key_columns AS source_foreign_key_column
	  ON source_foreign_key_column.constraint_object_id =
	     source_foreign_key.object_id
	JOIN sys.columns AS parent_column
	  ON parent_column.object_id =
	     source_foreign_key_column.parent_object_id
	 AND parent_column.column_id =
	     source_foreign_key_column.parent_column_id
	JOIN sys.columns AS referenced_column
	  ON referenced_column.object_id =
	     source_foreign_key_column.referenced_object_id
	 AND referenced_column.column_id =
	     source_foreign_key_column.referenced_column_id
	WHERE source_schema.name = @p1
	  AND source_table.name = @p2
	  AND source_table.object_id = @p3
	ORDER BY
		source_foreign_key.object_id,
		source_foreign_key_column.constraint_column_id
`

func discoverSQLServerSourceForeignKeys(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
	tableObjectID int64,
) ([]schema.ForeignKey, error) {
	rows, err := database.QueryContext(
		ctx,
		sqlServerSourceForeignKeysQuery,
		table.Schema,
		table.Name,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list SQL Server foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []sqlServerSourceForeignKeyCatalog
	for rows.Next() {
		var (
			catalog sqlServerSourceForeignKeyCatalog
			column  sqlServerSourceForeignKeyColumn
		)
		if err := rows.Scan(
			&catalog.databaseID,
			&catalog.tableObjectID,
			&catalog.namespace,
			&catalog.table,
			&catalog.objectID,
			&catalog.name,
			&catalog.typ,
			&catalog.typeDescription,
			&catalog.parentObjectID,
			&catalog.referencedObjectID,
			&catalog.referencedNamespace,
			&catalog.referencedTable,
			&catalog.keyIndexID,
			&catalog.disabled,
			&catalog.notTrusted,
			&catalog.notForReplication,
			&catalog.updateAction,
			&catalog.updateActionDescription,
			&catalog.deleteAction,
			&catalog.deleteActionDescription,
			&catalog.referencedIndexType,
			&catalog.referencedIndexTypeDescription,
			&catalog.referencedIndexUnique,
			&catalog.referencedIndexDisabled,
			&catalog.referencedIndexHypothetical,
			&catalog.referencedIndexFiltered,
			&catalog.referencedIndexFilter,
			&column.position,
			&column.parentObjectID,
			&column.parentColumnID,
			&column.parentColumn,
			&column.referencedObjectID,
			&column.referencedColumnID,
			&column.referencedColumn,
		); err != nil {
			return nil, fmt.Errorf(
				"read SQL Server foreign key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.tableObjectID != tableObjectID {
			return nil, sqlServerSourcePolicy(
				"foreign-key table identity",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		if len(catalogs) == 0 ||
			catalogs[len(catalogs)-1].objectID != catalog.objectID {
			catalog.columns = nil
			catalogs = append(catalogs, catalog)
		} else if !sameSQLServerForeignKeyCatalog(
			catalogs[len(catalogs)-1],
			catalog,
		) {
			return nil, sqlServerSourcePolicy(
				"foreign-key catalog shape",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		current := &catalogs[len(catalogs)-1]
		current.columns = append(current.columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate SQL Server foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}

	result := make([]schema.ForeignKey, 0, len(catalogs))
	names := make(map[string]int, len(catalogs))
	ids := make(map[int64]struct{}, len(catalogs))
	for _, catalog := range catalogs {
		if _, duplicate := ids[catalog.objectID]; duplicate {
			return nil, sqlServerSourcePolicy(
				"foreign-key catalog order",
				sqlServerSourceIdentity(table, catalog.name),
			)
		}
		ids[catalog.objectID] = struct{}{}
		foreignKey, err := sqlServerSourceForeignKeyFromCatalog(
			table,
			catalog,
		)
		if err != nil {
			return nil, err
		}
		if caseFoldedNameExists(names, foreignKey.Name) {
			return nil, sqlServerSourcePolicy(
				"foreign-key names",
				sqlServerSourceIdentity(table, foreignKey.Name),
			)
		}
		names[foreignKey.Name] = len(result)
		result = append(result, foreignKey)
	}
	return result, nil
}

func sameSQLServerForeignKeyCatalog(
	left, right sqlServerSourceForeignKeyCatalog,
) bool {
	return left.databaseID == right.databaseID &&
		left.tableObjectID == right.tableObjectID &&
		left.namespace == right.namespace &&
		left.table == right.table &&
		left.objectID == right.objectID &&
		left.name == right.name &&
		left.typ == right.typ &&
		left.typeDescription == right.typeDescription &&
		left.parentObjectID == right.parentObjectID &&
		left.referencedObjectID == right.referencedObjectID &&
		left.referencedNamespace == right.referencedNamespace &&
		left.referencedTable == right.referencedTable &&
		left.keyIndexID == right.keyIndexID &&
		left.disabled == right.disabled &&
		left.notTrusted == right.notTrusted &&
		left.notForReplication == right.notForReplication &&
		left.updateAction == right.updateAction &&
		left.updateActionDescription ==
			right.updateActionDescription &&
		left.deleteAction == right.deleteAction &&
		left.deleteActionDescription ==
			right.deleteActionDescription &&
		left.referencedIndexType == right.referencedIndexType &&
		left.referencedIndexTypeDescription ==
			right.referencedIndexTypeDescription &&
		left.referencedIndexUnique == right.referencedIndexUnique &&
		left.referencedIndexDisabled ==
			right.referencedIndexDisabled &&
		left.referencedIndexHypothetical ==
			right.referencedIndexHypothetical &&
		left.referencedIndexFiltered ==
			right.referencedIndexFiltered &&
		left.referencedIndexFilter == right.referencedIndexFilter
}

func sqlServerSourceForeignKeyFromCatalog(
	table schema.Table,
	catalog sqlServerSourceForeignKeyCatalog,
) (schema.ForeignKey, error) {
	identity := sqlServerSourceIdentity(table, catalog.name)
	if catalog.databaseID <= 0 ||
		catalog.tableObjectID <= 0 ||
		catalog.namespace != table.Schema ||
		catalog.table != table.Name ||
		catalog.objectID <= 0 ||
		!validSQLServerSourceIdentifier(catalog.name) ||
		catalog.typ != "F" ||
		catalog.typeDescription != "FOREIGN_KEY_CONSTRAINT" ||
		catalog.parentObjectID != catalog.tableObjectID ||
		catalog.referencedObjectID <= 0 ||
		catalog.referencedNamespace != table.Schema ||
		!validSQLServerSourceIdentifier(catalog.referencedTable) ||
		catalog.keyIndexID <= 0 ||
		catalog.disabled ||
		catalog.notTrusted ||
		catalog.notForReplication ||
		!validSQLServerRowstoreIndexType(
			catalog.referencedIndexType,
			catalog.referencedIndexTypeDescription,
		) ||
		!catalog.referencedIndexUnique ||
		catalog.referencedIndexDisabled ||
		catalog.referencedIndexHypothetical ||
		catalog.referencedIndexFiltered ||
		catalog.referencedIndexFilter.Valid ||
		len(catalog.columns) == 0 {
		return schema.ForeignKey{}, sqlServerSourcePolicy(
			"foreign-key catalog shape",
			identity,
		)
	}
	onUpdate, ok := sqlServerSourceForeignKeyAction(
		catalog.updateAction,
		catalog.updateActionDescription,
	)
	if !ok {
		return schema.ForeignKey{}, sqlServerSourcePolicy(
			"foreign-key update action",
			identity,
		)
	}
	onDelete, ok := sqlServerSourceForeignKeyAction(
		catalog.deleteAction,
		catalog.deleteActionDescription,
	)
	if !ok {
		return schema.ForeignKey{}, sqlServerSourcePolicy(
			"foreign-key delete action",
			identity,
		)
	}
	result := schema.ForeignKey{
		Name:              catalog.name,
		ReferencedSchema:  catalog.referencedNamespace,
		ReferencedTable:   catalog.referencedTable,
		OnUpdate:          onUpdate,
		OnDelete:          onDelete,
		Match:             "SIMPLE",
		Columns:           make([]string, len(catalog.columns)),
		ReferencedColumns: make([]string, len(catalog.columns)),
	}
	localNames := make(map[string]int, len(catalog.columns))
	referencedNames := make(map[string]int, len(catalog.columns))
	for index, source := range catalog.columns {
		localColumn, localExists := findSQLServerSourceColumn(
			table,
			source.parentColumn,
		)
		// Foreign-key comparison details are not represented by
		// schema.ForeignKey. Reject every scalar family whose SQL Server
		// equality/order contract differs from PostgreSQL's.
		if source.position != index+1 ||
			source.parentObjectID != catalog.tableObjectID ||
			source.parentColumnID <= 0 ||
			!localExists ||
			sqlServerSourceColumnHasNonportableComparison(localColumn) ||
			source.referencedObjectID != catalog.referencedObjectID ||
			source.referencedColumnID <= 0 ||
			!validSQLServerSourceIdentifier(
				source.referencedColumn,
			) {
			return schema.ForeignKey{}, sqlServerSourcePolicy(
				"foreign-key column catalog shape",
				identity,
			)
		}
		if caseFoldedNameExists(localNames, source.parentColumn) ||
			caseFoldedNameExists(
				referencedNames,
				source.referencedColumn,
			) {
			return schema.ForeignKey{}, sqlServerSourcePolicy(
				"foreign-key columns",
				identity,
			)
		}
		localNames[source.parentColumn] = index
		referencedNames[source.referencedColumn] = index
		result.Columns[index] = source.parentColumn
		result.ReferencedColumns[index] = source.referencedColumn
	}
	return result, nil
}

func sqlServerSourceForeignKeyAction(
	value int,
	description string,
) (string, bool) {
	switch value {
	case 0:
		return "NO ACTION", description == "NO_ACTION"
	case 1:
		return "CASCADE", description == "CASCADE"
	case 2:
		return "SET NULL", description == "SET_NULL"
	case 3:
		return "SET DEFAULT", description == "SET_DEFAULT"
	default:
		return "", false
	}
}

// validateSQLServerSourceObjectNames rejects names which are distinct only
// under a case-sensitive comparison. The neutral object plan must remain
// deterministic when it is applied to case-insensitive SQL Server databases
// and to targets with different object-name namespaces.
func validateSQLServerSourceObjectNames(table schema.Table) error {
	names := make(map[string]int)
	for _, index := range table.Indexes {
		if caseFoldedNameExists(names, index.Name) {
			return sqlServerSourcePolicy(
				"schema object names",
				sqlServerSourceIdentity(table, index.Name),
			)
		}
		names[index.Name] = len(names)
	}
	for _, check := range table.Checks {
		if caseFoldedNameExists(names, check.Name) {
			return sqlServerSourcePolicy(
				"schema object names",
				sqlServerSourceIdentity(table, check.Name),
			)
		}
		names[check.Name] = len(names)
	}
	for _, foreignKey := range table.ForeignKeys {
		if caseFoldedNameExists(names, foreignKey.Name) {
			return sqlServerSourcePolicy(
				"schema object names",
				sqlServerSourceIdentity(table, foreignKey.Name),
			)
		}
		names[foreignKey.Name] = len(names)
	}
	return nil
}

func validSQLServerRowstoreIndexType(
	value int,
	description string,
) bool {
	switch value {
	case 1:
		return description == "CLUSTERED"
	case 2:
		return description == "NONCLUSTERED"
	default:
		return false
	}
}

func findSQLServerSourceColumn(
	table schema.Table,
	name string,
) (schema.Column, bool) {
	for _, column := range table.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return schema.Column{}, false
}

func sqlServerSourceColumnIsText(column schema.Column) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(
			column.DeclaredType.Base,
		))
	}
	switch base {
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext":
		return true
	default:
		return false
	}
}

func sqlServerSourceColumnHasNonportableComparison(
	column schema.Column,
) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(
			column.DeclaredType.Base,
		))
	}
	switch base {
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext",
		"binary", "varbinary", "image", "blob", "bytea",
		"uniqueidentifier", "uuid":
		return true
	default:
		return false
	}
}

func caseFoldedNameExists[T any](
	values map[string]T,
	candidate string,
) bool {
	for existing := range values {
		if strings.EqualFold(existing, candidate) {
			return true
		}
	}
	return false
}

func sqlServerSourceIdentity(
	table schema.Table,
	object string,
) string {
	identity := table.Schema + "." + table.Name
	if object != "" {
		identity += "." + object
	}
	return identity
}
