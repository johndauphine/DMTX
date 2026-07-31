package engine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresSourcePrimaryKeyRow struct {
	objectID         int64
	name             string
	validated        bool
	deferrable       bool
	deferred         bool
	noInherit        bool
	local            bool
	inheritanceCount int
	parent           bool
	position         int
	column           string
}

const postgresSourcePrimaryKeyQuery = `
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
		key_column.position,
		attribute.attname
	FROM pg_catalog.pg_constraint AS constraint_object
	CROSS JOIN LATERAL unnest(constraint_object.conkey)
	  WITH ORDINALITY AS key_column(attnum, position)
	JOIN pg_catalog.pg_attribute AS attribute
	  ON attribute.attrelid = constraint_object.conrelid
	 AND attribute.attnum = key_column.attnum
	WHERE constraint_object.conrelid = $1
	  AND constraint_object.contype = 'p'
	ORDER BY constraint_object.oid, key_column.position
`

func discoverPostgresSourcePrimaryKey(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	tableObjectID int64,
	table *schema.Table,
) error {
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourcePrimaryKeyQuery,
		tableObjectID,
	)
	if err != nil {
		return fmt.Errorf(
			"list PostgreSQL primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	positions := make(map[string]int)
	var primaryObjectID int64
	for rows.Next() {
		var row postgresSourcePrimaryKeyRow
		if err := rows.Scan(
			&row.objectID,
			&row.name,
			&row.validated,
			&row.deferrable,
			&row.deferred,
			&row.noInherit,
			&row.local,
			&row.inheritanceCount,
			&row.parent,
			&row.position,
			&row.column,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if primaryObjectID == 0 {
			primaryObjectID = row.objectID
		}
		if row.objectID <= 0 ||
			row.objectID != primaryObjectID ||
			row.name == "" ||
			!row.validated ||
			row.deferrable ||
			row.deferred ||
			!row.noInherit ||
			!row.local ||
			row.inheritanceCount != 0 ||
			row.parent ||
			row.position != len(positions)+1 {
			return postgresSourcePolicy(
				"primary-key catalog shape",
				table.Schema+"."+table.Name,
			)
		}
		if _, exists := positions[row.column]; exists {
			return postgresSourcePolicy(
				"primary-key columns",
				table.Schema+"."+table.Name+"."+row.column,
			)
		}
		if _, exists := findPostgresSourceColumn(*table, row.column); !exists {
			return postgresSourcePolicy(
				"primary-key column",
				table.Schema+"."+table.Name+"."+row.column,
			)
		}
		positions[row.column] = row.position
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate PostgreSQL primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	for index := range table.Columns {
		position := positions[table.Columns[index].Name]
		if position == 0 {
			continue
		}
		table.Columns[index].PrimaryKey = true
		table.Columns[index].PrimaryKeyPosition = position
		if table.Columns[index].Nullable {
			return postgresSourcePolicy(
				"primary-key nullability",
				table.Schema+"."+table.Name+"."+
					table.Columns[index].Name,
			)
		}
	}
	return nil
}

type postgresSourceIndexColumn struct {
	name            string
	descending      bool
	nullsFirst      bool
	collationSchema string
	collationName   string
	defaultOperator bool
}

type postgresSourceIndexCatalog struct {
	objectID         int64
	namespace        string
	name             string
	unique           bool
	nullsNotDistinct bool
	valid            bool
	ready            bool
	live             bool
	exclusion        bool
	clustered        bool
	replicaIdentity  bool
	method           string
	keyColumns       int
	totalColumns     int
	partial          bool
	expression       bool
	constraintType   string
	columns          []postgresSourceIndexColumn
}

const postgresSourceIndexesQuery = `
	SELECT
		index_relation.oid::bigint,
		index_namespace.nspname,
		index_relation.relname,
		index_metadata.indisunique,
		index_metadata.indnullsnotdistinct,
		index_metadata.indisvalid,
		index_metadata.indisready,
		index_metadata.indislive,
		index_metadata.indisexclusion,
		index_metadata.indisclustered,
		index_metadata.indisreplident,
		access_method.amname,
		index_metadata.indnkeyatts,
		index_metadata.indnatts,
		index_metadata.indpred IS NOT NULL,
		index_metadata.indexprs IS NOT NULL,
		COALESCE(backing_constraint.contype::text, ''),
		key_column.position,
		attribute.attname,
		CASE
			WHEN key_column.position <= index_metadata.indnkeyatts
			THEN (
				(index_metadata.indoption)[key_column.position - 1] & 1
			) <> 0
			ELSE false
		END,
		CASE
			WHEN key_column.position <= index_metadata.indnkeyatts
			THEN (
				(index_metadata.indoption)[key_column.position - 1] & 2
			) <> 0
			ELSE false
		END,
		COALESCE(collation_namespace.nspname, ''),
		COALESCE(collation_object.collname, ''),
		COALESCE(operator_class.opcdefault, false)
	FROM pg_catalog.pg_index AS index_metadata
	JOIN pg_catalog.pg_class AS index_relation
	  ON index_relation.oid = index_metadata.indexrelid
	JOIN pg_catalog.pg_namespace AS index_namespace
	  ON index_namespace.oid = index_relation.relnamespace
	JOIN pg_catalog.pg_am AS access_method
	  ON access_method.oid = index_relation.relam
	CROSS JOIN LATERAL unnest(index_metadata.indkey)
	  WITH ORDINALITY AS key_column(attnum, position)
	LEFT JOIN pg_catalog.pg_attribute AS attribute
	  ON attribute.attrelid = index_metadata.indrelid
	 AND attribute.attnum = key_column.attnum
	LEFT JOIN pg_catalog.pg_collation AS collation_object
	  ON collation_object.oid =
	     (index_metadata.indcollation)[key_column.position - 1]
	LEFT JOIN pg_catalog.pg_namespace AS collation_namespace
	  ON collation_namespace.oid = collation_object.collnamespace
	LEFT JOIN pg_catalog.pg_opclass AS operator_class
	  ON operator_class.oid =
	     (index_metadata.indclass)[key_column.position - 1]
	LEFT JOIN pg_catalog.pg_constraint AS backing_constraint
	  ON backing_constraint.conindid = index_metadata.indexrelid
	 AND backing_constraint.conrelid = index_metadata.indrelid
	 AND backing_constraint.contype IN ('u', 'x')
	WHERE index_metadata.indrelid = $1
	  AND NOT index_metadata.indisprimary
	ORDER BY index_relation.relname, index_relation.oid, key_column.position
`

func discoverPostgresSourceIndexes(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	tableObjectID int64,
	table schema.Table,
) ([]schema.Index, error) {
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourceIndexesQuery,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list PostgreSQL indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []postgresSourceIndexCatalog
	var currentObjectID int64
	for rows.Next() {
		var (
			catalog    postgresSourceIndexCatalog
			columnName sql.NullString
			position   int
			column     postgresSourceIndexColumn
		)
		if err := rows.Scan(
			&catalog.objectID,
			&catalog.namespace,
			&catalog.name,
			&catalog.unique,
			&catalog.nullsNotDistinct,
			&catalog.valid,
			&catalog.ready,
			&catalog.live,
			&catalog.exclusion,
			&catalog.clustered,
			&catalog.replicaIdentity,
			&catalog.method,
			&catalog.keyColumns,
			&catalog.totalColumns,
			&catalog.partial,
			&catalog.expression,
			&catalog.constraintType,
			&position,
			&columnName,
			&column.descending,
			&column.nullsFirst,
			&column.collationSchema,
			&column.collationName,
			&column.defaultOperator,
		); err != nil {
			return nil, fmt.Errorf(
				"read PostgreSQL index for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.objectID != currentObjectID {
			catalog.columns = nil
			catalogs = append(catalogs, catalog)
			currentObjectID = catalog.objectID
		}
		current := &catalogs[len(catalogs)-1]
		if position != len(current.columns)+1 ||
			!columnName.Valid {
			return nil, postgresSourcePolicy(
				"index columns",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		column.name = columnName.String
		current.columns = append(current.columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate PostgreSQL indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}

	indexes := make([]schema.Index, 0, len(catalogs))
	for _, catalog := range catalogs {
		index, err := postgresSourceIndexFromCatalog(table, catalog)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func postgresSourceIndexFromCatalog(
	table schema.Table,
	catalog postgresSourceIndexCatalog,
) (schema.Index, error) {
	identity := table.Schema + "." + table.Name + "." + catalog.name
	if catalog.objectID <= 0 ||
		catalog.namespace != table.Schema ||
		catalog.name == "" ||
		catalog.nullsNotDistinct ||
		!catalog.valid ||
		!catalog.ready ||
		!catalog.live ||
		catalog.exclusion ||
		catalog.clustered ||
		catalog.replicaIdentity ||
		catalog.method != "btree" ||
		catalog.keyColumns <= 0 ||
		catalog.keyColumns != catalog.totalColumns ||
		catalog.keyColumns != len(catalog.columns) ||
		catalog.partial ||
		catalog.expression ||
		catalog.constraintType != "" {
		return schema.Index{}, postgresSourcePolicy(
			"index catalog shape",
			identity,
		)
	}
	result := schema.Index{
		Name:    catalog.name,
		Unique:  catalog.unique,
		Columns: make([]schema.IndexColumn, len(catalog.columns)),
	}
	seen := make(map[string]struct{}, len(catalog.columns))
	for index, source := range catalog.columns {
		column, exists := findPostgresSourceColumn(table, source.name)
		if !exists ||
			!source.defaultOperator ||
			source.nullsFirst != !source.descending {
			return schema.Index{}, postgresSourcePolicy(
				"index column catalog shape",
				identity+"."+source.name,
			)
		}
		if _, duplicate := seen[source.name]; duplicate {
			return schema.Index{}, postgresSourcePolicy(
				"index columns",
				identity+"."+source.name,
			)
		}
		seen[source.name] = struct{}{}
		result.Columns[index] = schema.IndexColumn{
			Name:       source.name,
			Descending: source.descending,
		}
		if postgresSourceColumnIsText(column) {
			if source.collationSchema != "pg_catalog" ||
				source.collationName != "C" {
				return schema.Index{}, postgresSourcePolicy(
					"index collation",
					identity+"."+source.name,
				)
			}
			result.Columns[index].Collation = "BINARY"
		} else if source.collationSchema != "" ||
			source.collationName != "" {
			return schema.Index{}, postgresSourcePolicy(
				"index collation",
				identity+"."+source.name,
			)
		}
	}
	return result, nil
}

type postgresSourceCheckCatalog struct {
	objectID         int64
	name             string
	validated        bool
	local            bool
	inheritanceCount int
	noInherit        bool
	parent           bool
	expression       string
}

const postgresSourceChecksQuery = `
	SELECT
		constraint_object.oid::bigint,
		constraint_object.conname,
		constraint_object.convalidated,
		constraint_object.conislocal,
		constraint_object.coninhcount,
		constraint_object.connoinherit,
		COALESCE(
			(to_jsonb(constraint_object)->>'conparentid')::oid,
			0
		) <> 0,
		pg_catalog.pg_get_expr(
			constraint_object.conbin,
			constraint_object.conrelid,
			true
		)
	FROM pg_catalog.pg_constraint AS constraint_object
	WHERE constraint_object.conrelid = $1
	  AND constraint_object.contype = 'c'
	ORDER BY constraint_object.conname, constraint_object.oid
`

func discoverPostgresSourceChecks(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	tableObjectID int64,
	table schema.Table,
) ([]schema.CheckConstraint, error) {
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourceChecksQuery,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list PostgreSQL CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()
	var result []schema.CheckConstraint
	seen := make(map[string]struct{})
	for rows.Next() {
		var catalog postgresSourceCheckCatalog
		if err := rows.Scan(
			&catalog.objectID,
			&catalog.name,
			&catalog.validated,
			&catalog.local,
			&catalog.inheritanceCount,
			&catalog.noInherit,
			&catalog.parent,
			&catalog.expression,
		); err != nil {
			return nil, fmt.Errorf(
				"read PostgreSQL CHECK constraint for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.objectID <= 0 ||
			catalog.name == "" ||
			!catalog.validated ||
			!catalog.local ||
			catalog.inheritanceCount != 0 ||
			catalog.noInherit ||
			catalog.parent {
			return nil, postgresSourcePolicy(
				"CHECK constraint catalog shape",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		if _, duplicate := seen[catalog.name]; duplicate {
			return nil, postgresSourcePolicy(
				"CHECK constraint names",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		seen[catalog.name] = struct{}{}
		expression, err := schema.ParsePostgresCatalogCheck(
			catalog.expression,
			table.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"discover PostgreSQL CHECK constraint %s.%s.%s: %w",
				table.Schema,
				table.Name,
				catalog.name,
				err,
			)
		}
		result = append(result, schema.CheckConstraint{
			Name:       catalog.name,
			Expression: expression,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate PostgreSQL CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return result, nil
}

type postgresSourceForeignKeyColumn struct {
	local      string
	referenced string
}

type postgresSourceForeignKeyCatalog struct {
	objectID         int64
	name             string
	validated        bool
	deferrable       bool
	deferred         bool
	noInherit        bool
	local            bool
	inheritanceCount int
	parent           bool
	deleteSetColumns bool
	period           bool
	onUpdate         string
	onDelete         string
	match            string
	referencedSchema string
	referencedTable  string
	columns          []postgresSourceForeignKeyColumn
}

const postgresSourceForeignKeysQuery = `
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
		CASE
			WHEN pg_catalog.jsonb_typeof(
				to_jsonb(constraint_object)->'confdelsetcols'
			) = 'array'
			THEN pg_catalog.jsonb_array_length(
				to_jsonb(constraint_object)->'confdelsetcols'
			) > 0
			ELSE false
		END,
		COALESCE(
			(to_jsonb(constraint_object)->>'conperiod')::boolean,
			false
		),
		constraint_object.confupdtype::text,
		constraint_object.confdeltype::text,
		constraint_object.confmatchtype::text,
		referenced_namespace.nspname,
		referenced_table.relname,
		local_key.position,
		local_attribute.attname,
		referenced_attribute.attname
	FROM pg_catalog.pg_constraint AS constraint_object
	JOIN pg_catalog.pg_class AS referenced_table
	  ON referenced_table.oid = constraint_object.confrelid
	JOIN pg_catalog.pg_namespace AS referenced_namespace
	  ON referenced_namespace.oid = referenced_table.relnamespace
	CROSS JOIN LATERAL unnest(constraint_object.conkey)
	  WITH ORDINALITY AS local_key(attnum, position)
	JOIN LATERAL unnest(constraint_object.confkey)
	  WITH ORDINALITY AS referenced_key(attnum, position)
	  ON referenced_key.position = local_key.position
	JOIN pg_catalog.pg_attribute AS local_attribute
	  ON local_attribute.attrelid = constraint_object.conrelid
	 AND local_attribute.attnum = local_key.attnum
	JOIN pg_catalog.pg_attribute AS referenced_attribute
	  ON referenced_attribute.attrelid = constraint_object.confrelid
	 AND referenced_attribute.attnum = referenced_key.attnum
	WHERE constraint_object.conrelid = $1
	  AND constraint_object.contype = 'f'
	ORDER BY
		constraint_object.conname,
		constraint_object.oid,
		local_key.position
`

func discoverPostgresSourceForeignKeys(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	tableObjectID int64,
	table schema.Table,
) ([]schema.ForeignKey, error) {
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourceForeignKeysQuery,
		tableObjectID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list PostgreSQL foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	var catalogs []postgresSourceForeignKeyCatalog
	var currentObjectID int64
	for rows.Next() {
		var (
			catalog  postgresSourceForeignKeyCatalog
			position int
			column   postgresSourceForeignKeyColumn
		)
		if err := rows.Scan(
			&catalog.objectID,
			&catalog.name,
			&catalog.validated,
			&catalog.deferrable,
			&catalog.deferred,
			&catalog.noInherit,
			&catalog.local,
			&catalog.inheritanceCount,
			&catalog.parent,
			&catalog.deleteSetColumns,
			&catalog.period,
			&catalog.onUpdate,
			&catalog.onDelete,
			&catalog.match,
			&catalog.referencedSchema,
			&catalog.referencedTable,
			&position,
			&column.local,
			&column.referenced,
		); err != nil {
			return nil, fmt.Errorf(
				"read PostgreSQL foreign key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if catalog.objectID != currentObjectID {
			catalog.columns = nil
			catalogs = append(catalogs, catalog)
			currentObjectID = catalog.objectID
		}
		current := &catalogs[len(catalogs)-1]
		if position != len(current.columns)+1 {
			return nil, postgresSourcePolicy(
				"foreign-key columns",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		current.columns = append(current.columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate PostgreSQL foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}

	result := make([]schema.ForeignKey, 0, len(catalogs))
	seen := make(map[string]struct{}, len(catalogs))
	for _, catalog := range catalogs {
		foreignKey, err := postgresSourceForeignKeyFromCatalog(
			table,
			catalog,
		)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[foreignKey.Name]; duplicate {
			return nil, postgresSourcePolicy(
				"foreign-key names",
				table.Schema+"."+table.Name+"."+foreignKey.Name,
			)
		}
		seen[foreignKey.Name] = struct{}{}
		result = append(result, foreignKey)
	}
	return result, nil
}

func postgresSourceForeignKeyFromCatalog(
	table schema.Table,
	catalog postgresSourceForeignKeyCatalog,
) (schema.ForeignKey, error) {
	identity := table.Schema + "." + table.Name + "." + catalog.name
	if catalog.objectID <= 0 ||
		catalog.name == "" ||
		!catalog.validated ||
		catalog.deferrable ||
		catalog.deferred ||
		!catalog.noInherit ||
		!catalog.local ||
		catalog.inheritanceCount != 0 ||
		catalog.parent ||
		catalog.deleteSetColumns ||
		catalog.period ||
		catalog.referencedSchema != table.Schema ||
		catalog.referencedTable == "" ||
		len(catalog.columns) == 0 {
		return schema.ForeignKey{}, postgresSourcePolicy(
			"foreign-key catalog shape",
			identity,
		)
	}
	onUpdate, ok := postgresSourceForeignKeyAction(catalog.onUpdate)
	if !ok {
		return schema.ForeignKey{}, postgresSourcePolicy(
			"foreign-key update action",
			identity+" catalog value "+catalog.onUpdate,
		)
	}
	onDelete, ok := postgresSourceForeignKeyAction(catalog.onDelete)
	if !ok {
		return schema.ForeignKey{}, postgresSourcePolicy(
			"foreign-key delete action",
			identity+" catalog value "+catalog.onDelete,
		)
	}
	match, ok := postgresSourceForeignKeyMatch(catalog.match)
	if !ok {
		return schema.ForeignKey{}, postgresSourcePolicy(
			"foreign-key match",
			identity+" catalog value "+catalog.match,
		)
	}
	result := schema.ForeignKey{
		Name:              catalog.name,
		ReferencedSchema:  catalog.referencedSchema,
		ReferencedTable:   catalog.referencedTable,
		OnUpdate:          onUpdate,
		OnDelete:          onDelete,
		Match:             match,
		Columns:           make([]string, len(catalog.columns)),
		ReferencedColumns: make([]string, len(catalog.columns)),
	}
	seenLocal := make(map[string]struct{}, len(catalog.columns))
	seenReferenced := make(map[string]struct{}, len(catalog.columns))
	for index, column := range catalog.columns {
		if _, exists := findPostgresSourceColumn(table, column.local); !exists {
			return schema.ForeignKey{}, postgresSourcePolicy(
				"foreign-key column",
				identity+"."+column.local,
			)
		}
		if column.referenced == "" {
			return schema.ForeignKey{}, postgresSourcePolicy(
				"foreign-key referenced column",
				identity,
			)
		}
		if _, duplicate := seenLocal[column.local]; duplicate {
			return schema.ForeignKey{}, postgresSourcePolicy(
				"foreign-key columns",
				identity+"."+column.local,
			)
		}
		if _, duplicate := seenReferenced[column.referenced]; duplicate {
			return schema.ForeignKey{}, postgresSourcePolicy(
				"foreign-key referenced columns",
				identity+"."+column.referenced,
			)
		}
		seenLocal[column.local] = struct{}{}
		seenReferenced[column.referenced] = struct{}{}
		result.Columns[index] = column.local
		result.ReferencedColumns[index] = column.referenced
	}
	return result, nil
}

func postgresSourceForeignKeyAction(value string) (string, bool) {
	switch value {
	case "a":
		return "NO ACTION", true
	case "r":
		return "RESTRICT", true
	case "c":
		return "CASCADE", true
	case "n":
		return "SET NULL", true
	case "d":
		return "SET DEFAULT", true
	default:
		return "", false
	}
}

func postgresSourceForeignKeyMatch(value string) (string, bool) {
	switch value {
	case "s":
		return "SIMPLE", true
	case "f":
		return "FULL", true
	default:
		return "", false
	}
}
