package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresRetainedIndexColumn struct {
	name            string
	descending      bool
	nullsFirst      bool
	collationSchema string
	collationName   string
	defaultOperator bool
}

type postgresRetainedIndex struct {
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
	columns          []postgresRetainedIndexColumn
}

type postgresRetainedForeignKeyColumn struct {
	local      string
	referenced string
}

type postgresRetainedForeignKey struct {
	name             string
	validated        bool
	deferrable       bool
	deferred         bool
	noInherit        bool
	local            bool
	inheritanceCount int
	parentConstraint bool
	deleteSetColumns bool
	period           bool
	onUpdate         string
	onDelete         string
	match            string
	referencedSchema string
	referencedTable  string
	columns          []postgresRetainedForeignKeyColumn
}

type postgresRetainedObjectPlan struct {
	indexes     map[string][]postgresRetainedIndex
	foreignKeys map[string][]postgresRetainedForeignKey
}

func preflightPostgresRetainedIndexesAndForeignKeys(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	planned, err := planPostgresRetainedIndexesAndForeignKeys(tables)
	if err != nil {
		return fmt.Errorf("plan retained PostgreSQL objects: %w", err)
	}
	for _, table := range tables {
		key := postgresRetainedTableKey(table.Schema, table.Name)
		actualIndexes, err := readPostgresRetainedIndexes(
			ctx,
			database,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect retained PostgreSQL indexes for table %s: %w",
				table.Name,
				err,
			)
		}
		if err := validatePostgresRetainedIndexes(
			table,
			planned.indexes[key],
			actualIndexes,
		); err != nil {
			return err
		}

		actualForeignKeys, err := readPostgresRetainedForeignKeys(
			ctx,
			database,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect retained PostgreSQL foreign keys for table %s: %w",
				table.Name,
				err,
			)
		}
		if err := validatePostgresRetainedForeignKeys(
			table,
			planned.foreignKeys[key],
			actualForeignKeys,
		); err != nil {
			return err
		}
	}
	return nil
}

func planPostgresRetainedIndexesAndForeignKeys(
	tables []schema.Table,
) (postgresRetainedObjectPlan, error) {
	statements, err := schema.PlanPostgresDropRecreateObjects(
		tables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		return postgresRetainedObjectPlan{}, err
	}
	indexNames := make(map[string][]string)
	foreignKeyNames := make(map[string][]string)
	for _, statement := range statements {
		key := postgresRetainedTableKey(statement.Schema, statement.Table)
		switch statement.Kind {
		case schema.PostgresIndexObject:
			indexNames[key] = append(indexNames[key], statement.Name)
		case schema.PostgresForeignKeyObject:
			foreignKeyNames[key] = append(
				foreignKeyNames[key],
				statement.Name,
			)
		}
	}

	result := postgresRetainedObjectPlan{
		indexes:     make(map[string][]postgresRetainedIndex, len(tables)),
		foreignKeys: make(map[string][]postgresRetainedForeignKey, len(tables)),
	}
	byName := make(map[string]schema.Table, len(tables))
	for _, table := range tables {
		key := postgresRetainedTableKey(table.Schema, table.Name)
		byName[key] = table
	}
	for _, table := range tables {
		key := postgresRetainedTableKey(table.Schema, table.Name)
		indexes := append([]schema.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(left, right int) bool {
			return postgresRetainedIndexSortKey(indexes[left]) <
				postgresRetainedIndexSortKey(indexes[right])
		})
		if len(indexNames[key]) != len(indexes) {
			return postgresRetainedObjectPlan{}, fmt.Errorf(
				"table %s planned %d PostgreSQL index names for %d indexes",
				table.Name,
				len(indexNames[key]),
				len(indexes),
			)
		}
		for index, source := range indexes {
			expected, err := expectedPostgresRetainedIndex(
				table,
				indexNames[key][index],
				source,
			)
			if err != nil {
				return postgresRetainedObjectPlan{}, err
			}
			result.indexes[key] = append(result.indexes[key], expected)
		}

		foreignKeys := append([]schema.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(foreignKeys, func(left, right int) bool {
			return postgresRetainedForeignKeySortKey(foreignKeys[left]) <
				postgresRetainedForeignKeySortKey(foreignKeys[right])
		})
		if len(foreignKeyNames[key]) != len(foreignKeys) {
			return postgresRetainedObjectPlan{}, fmt.Errorf(
				"table %s planned %d PostgreSQL foreign-key names for %d foreign keys",
				table.Name,
				len(foreignKeyNames[key]),
				len(foreignKeys),
			)
		}
		for index, source := range foreignKeys {
			referencedSchema := source.ReferencedSchema
			if referencedSchema == "" {
				referencedSchema = table.Schema
			}
			referencedKey := postgresRetainedTableKey(
				referencedSchema,
				source.ReferencedTable,
			)
			referenced, ok := byName[referencedKey]
			if !ok {
				return postgresRetainedObjectPlan{}, fmt.Errorf(
					"table %s references unplanned PostgreSQL table %s",
					table.Name,
					source.ReferencedTable,
				)
			}
			expected, err := expectedPostgresRetainedForeignKey(
				table,
				foreignKeyNames[key][index],
				source,
				referenced,
			)
			if err != nil {
				return postgresRetainedObjectPlan{}, err
			}
			result.foreignKeys[key] = append(
				result.foreignKeys[key],
				expected,
			)
		}
	}
	return result, nil
}

func expectedPostgresRetainedIndex(
	table schema.Table,
	name string,
	source schema.Index,
) (postgresRetainedIndex, error) {
	columnsByName := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		columnsByName[column.Name] = column
	}
	result := postgresRetainedIndex{
		name:         name,
		unique:       source.Unique,
		valid:        true,
		ready:        true,
		live:         true,
		method:       "btree",
		keyColumns:   len(source.Columns),
		totalColumns: len(source.Columns),
		columns:      make([]postgresRetainedIndexColumn, len(source.Columns)),
	}
	for index, sourceColumn := range source.Columns {
		column, ok := columnsByName[sourceColumn.Name]
		if !ok {
			return postgresRetainedIndex{}, fmt.Errorf(
				"table %s index %s references unknown column %s",
				table.Name,
				name,
				sourceColumn.Name,
			)
		}
		expected := postgresRetainedIndexColumn{
			name:            sourceColumn.Name,
			descending:      sourceColumn.Descending,
			nullsFirst:      !sourceColumn.Descending,
			defaultOperator: true,
		}
		if postgresRetainedColumnIsText(column) {
			expected.collationSchema = "pg_catalog"
			expected.collationName = "C"
		}
		result.columns[index] = expected
	}
	return result, nil
}

func expectedPostgresRetainedForeignKey(
	table schema.Table,
	name string,
	source schema.ForeignKey,
	referenced schema.Table,
) (postgresRetainedForeignKey, error) {
	referencedColumns := append(
		[]string(nil),
		source.ReferencedColumns...,
	)
	if len(referencedColumns) == 0 {
		referencedColumns = primaryKeyColumns(referenced)
	}
	if len(source.Columns) != len(referencedColumns) {
		return postgresRetainedForeignKey{}, fmt.Errorf(
			"table %s foreign key %s has mismatched columns",
			table.Name,
			name,
		)
	}
	onUpdate, err := postgresRetainedForeignKeyAction(source.OnUpdate)
	if err != nil {
		return postgresRetainedForeignKey{}, err
	}
	onDelete, err := postgresRetainedForeignKeyAction(source.OnDelete)
	if err != nil {
		return postgresRetainedForeignKey{}, err
	}
	match, err := postgresRetainedForeignKeyMatch(source.Match)
	if err != nil {
		return postgresRetainedForeignKey{}, err
	}
	result := postgresRetainedForeignKey{
		name:             name,
		validated:        true,
		noInherit:        true,
		local:            true,
		onUpdate:         onUpdate,
		onDelete:         onDelete,
		match:            match,
		referencedSchema: referenced.Schema,
		referencedTable:  referenced.Name,
		columns: make(
			[]postgresRetainedForeignKeyColumn,
			len(source.Columns),
		),
	}
	for index := range source.Columns {
		result.columns[index] = postgresRetainedForeignKeyColumn{
			local:      source.Columns[index],
			referenced: referencedColumns[index],
		}
	}
	return result, nil
}

const postgresRetainedIndexesQuery = `
	SELECT
		index_relation.oid::bigint,
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
	JOIN pg_catalog.pg_class AS table_relation
	  ON table_relation.oid = index_metadata.indrelid
	JOIN pg_catalog.pg_namespace AS table_namespace
	  ON table_namespace.oid = table_relation.relnamespace
	JOIN pg_catalog.pg_class AS index_relation
	  ON index_relation.oid = index_metadata.indexrelid
	JOIN pg_catalog.pg_am AS access_method
	  ON access_method.oid = index_relation.relam
	CROSS JOIN LATERAL unnest(index_metadata.indkey)
	  WITH ORDINALITY AS key_column(attnum, position)
	LEFT JOIN pg_catalog.pg_attribute AS attribute
	  ON attribute.attrelid = table_relation.oid
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
	 AND backing_constraint.conrelid = table_relation.oid
	 AND backing_constraint.contype IN ('u', 'x')
	WHERE table_namespace.nspname = $1
	  AND table_relation.relname = $2
	  AND NOT index_metadata.indisprimary
	ORDER BY index_relation.relname, index_relation.oid, key_column.position
`

func readPostgresRetainedIndexes(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]postgresRetainedIndex, error) {
	rows, err := database.QueryContext(
		ctx,
		postgresRetainedIndexesQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []postgresRetainedIndex
	var currentObjectID int64 = -1
	for rows.Next() {
		var (
			objectID   int64
			position   int
			columnName sql.NullString
			index      postgresRetainedIndex
			column     postgresRetainedIndexColumn
		)
		if err := rows.Scan(
			&objectID,
			&index.name,
			&index.unique,
			&index.nullsNotDistinct,
			&index.valid,
			&index.ready,
			&index.live,
			&index.exclusion,
			&index.clustered,
			&index.replicaIdentity,
			&index.method,
			&index.keyColumns,
			&index.totalColumns,
			&index.partial,
			&index.expression,
			&index.constraintType,
			&position,
			&columnName,
			&column.descending,
			&column.nullsFirst,
			&column.collationSchema,
			&column.collationName,
			&column.defaultOperator,
		); err != nil {
			return nil, err
		}
		if objectID != currentObjectID {
			result = append(result, index)
			currentObjectID = objectID
		}
		if !columnName.Valid {
			column.name = ""
		} else {
			column.name = columnName.String
		}
		if position != len(result[len(result)-1].columns)+1 {
			return nil, fmt.Errorf(
				"index %s has non-contiguous catalog columns",
				index.name,
			)
		}
		result[len(result)-1].columns = append(
			result[len(result)-1].columns,
			column,
		)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

const postgresRetainedForeignKeysQuery = `
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
		COALESCE((to_jsonb(constraint_object)->>'conperiod')::boolean, false),
		constraint_object.confupdtype::text,
		constraint_object.confdeltype::text,
		constraint_object.confmatchtype::text,
		referenced_namespace.nspname,
		referenced_table.relname,
		local_key.position,
		local_attribute.attname,
		referenced_attribute.attname
	FROM pg_catalog.pg_constraint AS constraint_object
	JOIN pg_catalog.pg_class AS table_relation
	  ON table_relation.oid = constraint_object.conrelid
	JOIN pg_catalog.pg_namespace AS table_namespace
	  ON table_namespace.oid = table_relation.relnamespace
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
	  ON local_attribute.attrelid = table_relation.oid
	 AND local_attribute.attnum = local_key.attnum
	JOIN pg_catalog.pg_attribute AS referenced_attribute
	  ON referenced_attribute.attrelid = referenced_table.oid
	 AND referenced_attribute.attnum = referenced_key.attnum
	WHERE table_namespace.nspname = $1
	  AND table_relation.relname = $2
	  AND constraint_object.contype = 'f'
	ORDER BY
		constraint_object.conname,
		constraint_object.oid,
		local_key.position
`

func readPostgresRetainedForeignKeys(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]postgresRetainedForeignKey, error) {
	rows, err := database.QueryContext(
		ctx,
		postgresRetainedForeignKeysQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []postgresRetainedForeignKey
	var currentObjectID int64 = -1
	for rows.Next() {
		var (
			objectID   int64
			position   int
			foreignKey postgresRetainedForeignKey
			column     postgresRetainedForeignKeyColumn
		)
		if err := rows.Scan(
			&objectID,
			&foreignKey.name,
			&foreignKey.validated,
			&foreignKey.deferrable,
			&foreignKey.deferred,
			&foreignKey.noInherit,
			&foreignKey.local,
			&foreignKey.inheritanceCount,
			&foreignKey.parentConstraint,
			&foreignKey.deleteSetColumns,
			&foreignKey.period,
			&foreignKey.onUpdate,
			&foreignKey.onDelete,
			&foreignKey.match,
			&foreignKey.referencedSchema,
			&foreignKey.referencedTable,
			&position,
			&column.local,
			&column.referenced,
		); err != nil {
			return nil, err
		}
		if objectID != currentObjectID {
			result = append(result, foreignKey)
			currentObjectID = objectID
		}
		if position != len(result[len(result)-1].columns)+1 {
			return nil, fmt.Errorf(
				"foreign key %s has non-contiguous catalog columns",
				foreignKey.name,
			)
		}
		result[len(result)-1].columns = append(
			result[len(result)-1].columns,
			column,
		)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePostgresRetainedIndexes(
	table schema.Table,
	planned []postgresRetainedIndex,
	actual []postgresRetainedIndex,
) error {
	plannedByName, err := postgresRetainedIndexesByName(planned)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s indexes: %w",
			table.Name,
			err,
		)
	}
	actualByName, err := postgresRetainedIndexesByName(actual)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s indexes: %w",
			table.Name,
			err,
		)
	}
	for name, expected := range plannedByName {
		found, ok := actualByName[name]
		if !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: required secondary index %q is missing",
				table.Name,
				name,
			)
		}
		if !samePostgresRetainedIndex(expected, found) {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: secondary index %q differs from the planned shape",
				table.Name,
				name,
			)
		}
	}
	for name := range actualByName {
		if _, ok := plannedByName[name]; !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: unexpected secondary index %q is retained",
				table.Name,
				name,
			)
		}
	}
	return nil
}

func validatePostgresRetainedForeignKeys(
	table schema.Table,
	planned []postgresRetainedForeignKey,
	actual []postgresRetainedForeignKey,
) error {
	plannedByName, err := postgresRetainedForeignKeysByName(planned)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s foreign keys: %w",
			table.Name,
			err,
		)
	}
	actualByName, err := postgresRetainedForeignKeysByName(actual)
	if err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s foreign keys: %w",
			table.Name,
			err,
		)
	}
	for name, expected := range plannedByName {
		found, ok := actualByName[name]
		if !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: required foreign key %q is missing",
				table.Name,
				name,
			)
		}
		if !samePostgresRetainedForeignKey(expected, found) {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: foreign key %q differs from the planned shape",
				table.Name,
				name,
			)
		}
	}
	for name := range actualByName {
		if _, ok := plannedByName[name]; !ok {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: unexpected foreign key %q is retained",
				table.Name,
				name,
			)
		}
	}
	return nil
}

func postgresRetainedIndexesByName(
	values []postgresRetainedIndex,
) (map[string]postgresRetainedIndex, error) {
	result := make(map[string]postgresRetainedIndex, len(values))
	for _, value := range values {
		if _, exists := result[value.name]; exists {
			return nil, fmt.Errorf("duplicate index %q", value.name)
		}
		result[value.name] = value
	}
	return result, nil
}

func postgresRetainedForeignKeysByName(
	values []postgresRetainedForeignKey,
) (map[string]postgresRetainedForeignKey, error) {
	result := make(map[string]postgresRetainedForeignKey, len(values))
	for _, value := range values {
		if _, exists := result[value.name]; exists {
			return nil, fmt.Errorf("duplicate foreign key %q", value.name)
		}
		result[value.name] = value
	}
	return result, nil
}

func samePostgresRetainedIndex(
	left postgresRetainedIndex,
	right postgresRetainedIndex,
) bool {
	if left.name != right.name ||
		left.unique != right.unique ||
		left.nullsNotDistinct != right.nullsNotDistinct ||
		left.valid != right.valid ||
		left.ready != right.ready ||
		left.live != right.live ||
		left.exclusion != right.exclusion ||
		left.clustered != right.clustered ||
		left.replicaIdentity != right.replicaIdentity ||
		left.method != right.method ||
		left.keyColumns != right.keyColumns ||
		left.totalColumns != right.totalColumns ||
		left.partial != right.partial ||
		left.expression != right.expression ||
		left.constraintType != right.constraintType ||
		len(left.columns) != len(right.columns) {
		return false
	}
	for index := range left.columns {
		if left.columns[index] != right.columns[index] {
			return false
		}
	}
	return true
}

func samePostgresRetainedForeignKey(
	left postgresRetainedForeignKey,
	right postgresRetainedForeignKey,
) bool {
	if left.name != right.name ||
		left.validated != right.validated ||
		left.deferrable != right.deferrable ||
		left.deferred != right.deferred ||
		left.noInherit != right.noInherit ||
		left.local != right.local ||
		left.inheritanceCount != right.inheritanceCount ||
		left.parentConstraint != right.parentConstraint ||
		left.deleteSetColumns != right.deleteSetColumns ||
		left.period != right.period ||
		left.onUpdate != right.onUpdate ||
		left.onDelete != right.onDelete ||
		left.match != right.match ||
		left.referencedSchema != right.referencedSchema ||
		left.referencedTable != right.referencedTable ||
		len(left.columns) != len(right.columns) {
		return false
	}
	for index := range left.columns {
		if left.columns[index] != right.columns[index] {
			return false
		}
	}
	return true
}

func postgresRetainedIndexSortKey(index schema.Index) string {
	parts := []string{
		index.Name,
		strconv.FormatBool(index.Unique),
		strconv.FormatBool(index.Inline),
	}
	for _, column := range index.Columns {
		parts = append(
			parts,
			column.Name,
			strconv.FormatBool(column.Descending),
			strings.ToUpper(strings.TrimSpace(column.Collation)),
		)
	}
	return strings.Join(parts, "\x00")
}

func postgresRetainedForeignKeySortKey(
	foreignKey schema.ForeignKey,
) string {
	parts := []string{foreignKey.Name}
	parts = append(parts, foreignKey.Columns...)
	if foreignKey.ReferencedSchema != "" {
		parts = append(parts, foreignKey.ReferencedSchema)
	}
	parts = append(parts, foreignKey.ReferencedTable)
	parts = append(parts, foreignKey.ReferencedColumns...)
	parts = append(
		parts,
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnUpdate,
		), " ")),
		strings.ToUpper(strings.Join(strings.Fields(
			foreignKey.OnDelete,
		), " ")),
		strings.ToUpper(strings.TrimSpace(foreignKey.Match)),
	)
	return strings.Join(parts, "\x00")
}

func postgresRetainedColumnIsText(column schema.Column) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	switch base {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		return true
	default:
		return false
	}
}

func postgresRetainedForeignKeyAction(value string) (string, error) {
	normalized := strings.ToUpper(strings.Join(strings.Fields(value), " "))
	if normalized == "" {
		normalized = "NO ACTION"
	}
	switch normalized {
	case "NO ACTION":
		return "a", nil
	case "RESTRICT":
		return "r", nil
	case "CASCADE":
		return "c", nil
	case "SET NULL":
		return "n", nil
	case "SET DEFAULT":
		return "d", nil
	default:
		return "", fmt.Errorf(
			"unsupported PostgreSQL foreign-key action %q",
			value,
		)
	}
}

func postgresRetainedForeignKeyMatch(value string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch normalized {
	case "", "NONE", "SIMPLE":
		return "s", nil
	case "FULL":
		return "f", nil
	default:
		return "", fmt.Errorf(
			"unsupported PostgreSQL foreign-key match %q",
			value,
		)
	}
}

func postgresRetainedTableKey(namespace, table string) string {
	return namespace + "\x00" + table
}
