package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type mariaDB1011PrimaryKeyCatalog struct {
	constraintName string
	constraintType string
	position       int
	column         sql.NullString
	direction      sql.NullString
	nonUnique      int
	indexType      string
	prefixLength   sql.NullInt64
	packed         sql.NullString
	nullable       string
	comment        sql.NullString
	indexComment   string
	ignored        string
}

const mariaDB1011SourcePrimaryKeyQuery = `
	SELECT
		source_constraint.CONSTRAINT_NAME,
		source_constraint.CONSTRAINT_TYPE,
		source_index.SEQ_IN_INDEX,
		source_index.COLUMN_NAME,
		source_index.COLLATION,
		source_index.NON_UNIQUE,
		source_index.INDEX_TYPE,
		source_index.SUB_PART,
		source_index.PACKED,
		source_index.NULLABLE,
		source_index.COMMENT,
		source_index.INDEX_COMMENT,
		source_index.IGNORED
	FROM information_schema.TABLE_CONSTRAINTS AS source_constraint
	JOIN information_schema.STATISTICS AS source_index
	  ON source_index.TABLE_SCHEMA = source_constraint.TABLE_SCHEMA
	 AND source_index.TABLE_NAME = source_constraint.TABLE_NAME
	 AND source_index.INDEX_NAME = source_constraint.CONSTRAINT_NAME
	WHERE source_constraint.TABLE_SCHEMA = ?
	  AND source_constraint.TABLE_NAME = ?
	  AND source_constraint.CONSTRAINT_TYPE = 'PRIMARY KEY'
	ORDER BY source_index.SEQ_IN_INDEX
`

func discoverMariaDB1011SourcePrimaryKey(
	ctx context.Context,
	database MySQLCatalogQueryer,
	table *schema.Table,
) error {
	rows, err := database.QueryContext(
		ctx,
		mariaDB1011SourcePrimaryKeyQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"list MariaDB primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	byName := make(map[string]int, len(table.Columns))
	for index, column := range table.Columns {
		byName[column.Name] = index
	}
	position := 0
	for rows.Next() {
		var catalog mariaDB1011PrimaryKeyCatalog
		if err := rows.Scan(
			&catalog.constraintName,
			&catalog.constraintType,
			&catalog.position,
			&catalog.column,
			&catalog.direction,
			&catalog.nonUnique,
			&catalog.indexType,
			&catalog.prefixLength,
			&catalog.packed,
			&catalog.nullable,
			&catalog.comment,
			&catalog.indexComment,
			&catalog.ignored,
		); err != nil {
			return fmt.Errorf(
				"read MariaDB primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		position++
		if catalog.constraintName != "PRIMARY" ||
			catalog.constraintType != "PRIMARY KEY" ||
			catalog.position != position ||
			!catalog.column.Valid ||
			!catalog.direction.Valid ||
			catalog.direction.String != "A" ||
			catalog.nonUnique != 0 ||
			!strings.EqualFold(catalog.indexType, "BTREE") ||
			catalog.prefixLength.Valid ||
			catalog.packed.Valid ||
			catalog.nullable != "" ||
			!catalog.comment.Valid ||
			catalog.comment.String != "" ||
			catalog.indexComment != "" ||
			catalog.ignored != "NO" {
			return mariaDB1011SourcePolicy(
				"primary key catalog shape",
				table.Schema+"."+table.Name,
			)
		}
		index, ok := byName[catalog.column.String]
		if !ok ||
			table.Columns[index].PrimaryKey ||
			table.Columns[index].PrimaryKeyPosition != 0 ||
			table.Columns[index].Nullable {
			return mariaDB1011SourcePolicy(
				"primary key column",
				table.Schema+"."+table.Name+"."+
					catalog.column.String,
			)
		}
		table.Columns[index].PrimaryKey = true
		table.Columns[index].PrimaryKeyPosition = position
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MariaDB primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return nil
}

type mariaDB1011IndexCatalog struct {
	name            string
	nonUnique       int
	position        int
	column          sql.NullString
	direction       sql.NullString
	prefixLength    sql.NullInt64
	packed          sql.NullString
	indexNullable   string
	indexType       string
	comment         sql.NullString
	indexComment    string
	ignored         string
	dataType        string
	columnCollation sql.NullString
	columnNullable  string
}

const mariaDB1011SourceIndexesQuery = `
	SELECT
		source_index.INDEX_NAME,
		source_index.NON_UNIQUE,
		source_index.SEQ_IN_INDEX,
		source_index.COLUMN_NAME,
		source_index.COLLATION,
		source_index.SUB_PART,
		source_index.PACKED,
		source_index.NULLABLE,
		source_index.INDEX_TYPE,
		source_index.COMMENT,
		source_index.INDEX_COMMENT,
		source_index.IGNORED,
		source_column.DATA_TYPE,
		source_column.COLLATION_NAME,
		source_column.IS_NULLABLE
	FROM information_schema.STATISTICS AS source_index
	LEFT JOIN information_schema.COLUMNS AS source_column
	  ON source_column.TABLE_SCHEMA = source_index.TABLE_SCHEMA
	 AND source_column.TABLE_NAME = source_index.TABLE_NAME
	 AND source_column.COLUMN_NAME = source_index.COLUMN_NAME
	WHERE source_index.TABLE_SCHEMA = ?
	  AND source_index.TABLE_NAME = ?
	  AND source_index.INDEX_NAME <> 'PRIMARY'
	ORDER BY source_index.INDEX_NAME, source_index.SEQ_IN_INDEX
`

func discoverMariaDB1011SourceIndexes(
	ctx context.Context,
	database MySQLCatalogQueryer,
	table schema.Table,
) ([]schema.Index, error) {
	rows, err := database.QueryContext(
		ctx,
		mariaDB1011SourceIndexesQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list MariaDB indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	columns := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	indexes := make([]schema.Index, 0)
	var current *schema.Index
	for rows.Next() {
		var catalog mariaDB1011IndexCatalog
		if err := rows.Scan(
			&catalog.name,
			&catalog.nonUnique,
			&catalog.position,
			&catalog.column,
			&catalog.direction,
			&catalog.prefixLength,
			&catalog.packed,
			&catalog.indexNullable,
			&catalog.indexType,
			&catalog.comment,
			&catalog.indexComment,
			&catalog.ignored,
			&catalog.dataType,
			&catalog.columnCollation,
			&catalog.columnNullable,
		); err != nil {
			return nil, fmt.Errorf(
				"read MariaDB index for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if current == nil || current.Name != catalog.name {
			if !validMySQLSourceIdentifier(catalog.name) ||
				catalog.name == "PRIMARY" ||
				catalog.position != 1 ||
				(catalog.nonUnique != 0 && catalog.nonUnique != 1) {
				return nil, mariaDB1011SourcePolicy(
					"index catalog shape",
					table.Schema+"."+table.Name+"."+catalog.name,
				)
			}
			indexes = append(indexes, schema.Index{
				Name:   catalog.name,
				Unique: catalog.nonUnique == 0,
			})
			current = &indexes[len(indexes)-1]
		} else if catalog.position != len(current.Columns)+1 ||
			current.Unique != (catalog.nonUnique == 0) {
			return nil, mariaDB1011SourcePolicy(
				"index column order",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		if !catalog.column.Valid ||
			!catalog.direction.Valid ||
			(catalog.direction.String != "A" &&
				catalog.direction.String != "D") ||
			catalog.prefixLength.Valid ||
			catalog.packed.Valid ||
			!strings.EqualFold(catalog.indexType, "BTREE") ||
			!catalog.comment.Valid ||
			catalog.comment.String != "" ||
			catalog.indexComment != "" ||
			catalog.ignored != "NO" {
			return nil, mariaDB1011SourcePolicy(
				"index catalog shape",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		column, ok := columns[catalog.column.String]
		if !ok ||
			column.DeclaredType == nil ||
			!strings.EqualFold(
				column.DeclaredType.Base,
				catalog.dataType,
			) ||
			catalog.columnNullable !=
				mySQLCatalogNullable(column.Nullable) ||
			catalog.indexNullable !=
				mySQLIndexNullable(column.Nullable) {
			return nil, mariaDB1011SourcePolicy(
				"index column",
				table.Schema+"."+table.Name+"."+
					catalog.column.String,
			)
		}
		indexColumn := schema.IndexColumn{
			Name:       catalog.column.String,
			Descending: catalog.direction.String == "D",
		}
		if mySQLSourceTextColumn(column) {
			if !catalog.columnCollation.Valid ||
				!strings.EqualFold(
					catalog.columnCollation.String,
					"utf8mb4_nopad_bin",
				) {
				return nil, mariaDB1011SourcePolicy(
					"index collation",
					table.Schema+"."+table.Name+"."+
						catalog.column.String,
				)
			}
			indexColumn.Collation = "BINARY"
		} else if catalog.columnCollation.Valid {
			return nil, mariaDB1011SourcePolicy(
				"index collation",
				table.Schema+"."+table.Name+"."+
					catalog.column.String,
			)
		}
		for _, existing := range current.Columns {
			if existing.Name == indexColumn.Name {
				return nil, mariaDB1011SourcePolicy(
					"index duplicate column",
					table.Schema+"."+table.Name+"."+
						catalog.name+"."+indexColumn.Name,
				)
			}
		}
		current.Columns = append(current.Columns, indexColumn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate MariaDB indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return indexes, nil
}

type mariaDB1011CheckCatalog struct {
	name        string
	typ         string
	level       string
	checkClause string
}

const mariaDB1011SourceChecksQuery = `
	SELECT
		source_constraint.CONSTRAINT_NAME,
		source_constraint.CONSTRAINT_TYPE,
		source_check.LEVEL,
		source_check.CHECK_CLAUSE
	FROM information_schema.TABLE_CONSTRAINTS AS source_constraint
	JOIN information_schema.CHECK_CONSTRAINTS AS source_check
	  ON source_check.CONSTRAINT_SCHEMA = source_constraint.CONSTRAINT_SCHEMA
	 AND source_check.TABLE_NAME = source_constraint.TABLE_NAME
	 AND source_check.CONSTRAINT_NAME = source_constraint.CONSTRAINT_NAME
	WHERE source_constraint.TABLE_SCHEMA = ?
	  AND source_constraint.TABLE_NAME = ?
	  AND source_constraint.CONSTRAINT_TYPE = 'CHECK'
	ORDER BY source_constraint.CONSTRAINT_NAME
`

func discoverMariaDB1011SourceChecks(
	ctx context.Context,
	database MySQLCatalogQueryer,
	table *schema.Table,
) ([]schema.CheckConstraint, error) {
	rows, err := database.QueryContext(
		ctx,
		mariaDB1011SourceChecksQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list MariaDB CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	catalogs := make([]mariaDB1011CheckCatalog, 0)
	previous := ""
	for rows.Next() {
		var catalog mariaDB1011CheckCatalog
		if err := rows.Scan(
			&catalog.name,
			&catalog.typ,
			&catalog.level,
			&catalog.checkClause,
		); err != nil {
			return nil, fmt.Errorf(
				"read MariaDB CHECK constraint for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if !validMySQLSourceIdentifier(catalog.name) ||
			catalog.typ != "CHECK" ||
			(catalog.level != "Column" &&
				catalog.level != "Table") ||
			catalog.name == previous {
			return nil, mariaDB1011SourcePolicy(
				"CHECK catalog shape",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		catalogs = append(catalogs, catalog)
		previous = catalog.name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate MariaDB CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return applyMariaDB1011SourceChecks(table, catalogs)
}

func applyMariaDB1011SourceChecks(
	table *schema.Table,
	catalogs []mariaDB1011CheckCatalog,
) ([]schema.CheckConstraint, error) {
	checks := make([]schema.CheckConstraint, 0, len(catalogs))
	for _, catalog := range catalogs {
		if catalog.level == "Column" {
			if err := applyMariaDB1011JSONCheck(
				table,
				catalog,
			); err != nil {
				return nil, err
			}
			continue
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			catalog.checkClause,
			table.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"discover MariaDB CHECK %s.%s.%s: %w",
				table.Schema,
				table.Name,
				catalog.name,
				err,
			)
		}
		checks = append(checks, schema.CheckConstraint{
			Name:       catalog.name,
			Expression: expression,
		})
	}
	return checks, nil
}

func applyMariaDB1011JSONCheck(
	table *schema.Table,
	catalog mariaDB1011CheckCatalog,
) error {
	columnName, ok := mariaDB1011JSONCheckColumn(catalog.checkClause)
	if !ok || columnName != catalog.name {
		return mariaDB1011SourcePolicy(
			"column CHECK",
			table.Schema+"."+table.Name+"."+catalog.name,
		)
	}
	for index := range table.Columns {
		column := &table.Columns[index]
		if column.Name != columnName {
			continue
		}
		if column.Type != "text" ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != "longtext" ||
			len(column.DeclaredType.Arguments) != 0 {
			return mariaDB1011SourcePolicy(
				"JSON alias",
				table.Schema+"."+table.Name+"."+columnName,
			)
		}
		if column.Default != nil {
			return mariaDB1011SourcePolicy(
				"JSON default",
				table.Schema+"."+table.Name+"."+columnName,
			)
		}
		column.Type = "json"
		column.DeclaredType = &schema.DeclaredType{Base: "json"}
		return nil
	}
	return mariaDB1011SourcePolicy(
		"JSON alias column",
		table.Schema+"."+table.Name+"."+columnName,
	)
}

func mariaDB1011JSONCheckColumn(value string) (string, bool) {
	value = strings.TrimSpace(value)
	const function = "json_valid"
	if len(value) < len(function) ||
		!strings.EqualFold(value[:len(function)], function) {
		return "", false
	}
	value = strings.TrimSpace(value[len(function):])
	if len(value) < 2 || value[0] != '(' {
		return "", false
	}
	value = strings.TrimSpace(value[1:])
	if len(value) < 3 || value[0] != '`' {
		return "", false
	}
	var name strings.Builder
	index := 1
	for index < len(value) {
		if value[index] != '`' {
			name.WriteByte(value[index])
			index++
			continue
		}
		if index+1 < len(value) && value[index+1] == '`' {
			name.WriteByte('`')
			index += 2
			continue
		}
		index++
		break
	}
	if index > len(value) ||
		index == 1 ||
		!validMySQLSourceIdentifier(name.String()) {
		return "", false
	}
	value = strings.TrimSpace(value[index:])
	if value != ")" {
		return "", false
	}
	return name.String(), true
}

func discoverMariaDB1011SourceForeignKeys(
	ctx context.Context,
	database MySQLCatalogQueryer,
	table schema.Table,
) ([]schema.ForeignKey, error) {
	rows, err := database.QueryContext(
		ctx,
		mysql80SourceForeignKeysQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list MariaDB foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	columns := make(map[string]bool, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = true
	}
	foreignKeys := make([]schema.ForeignKey, 0)
	var current *schema.ForeignKey
	var uniqueName string
	for rows.Next() {
		var catalog mysql80ForeignKeyCatalog
		if err := rows.Scan(
			&catalog.name,
			&catalog.position,
			&catalog.column,
			&catalog.referencedSchema,
			&catalog.referencedTable,
			&catalog.referencedColumn,
			&catalog.uniquePosition,
			&catalog.uniqueConstraintSchema,
			&catalog.uniqueConstraintName,
			&catalog.match,
			&catalog.onUpdate,
			&catalog.onDelete,
		); err != nil {
			return nil, fmt.Errorf(
				"read MariaDB foreign key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if current == nil || current.Name != catalog.name {
			if !validMySQLSourceIdentifier(catalog.name) ||
				catalog.position != 1 ||
				!columns[catalog.column] ||
				!validMySQLForeignKeyCatalog(table, catalog) {
				return nil, mariaDB1011SourcePolicy(
					"foreign key catalog shape",
					table.Schema+"."+table.Name+"."+
						catalog.name,
				)
			}
			foreignKeys = append(
				foreignKeys,
				schema.ForeignKey{
					Name:             catalog.name,
					ReferencedSchema: catalog.referencedSchema.String,
					ReferencedTable:  catalog.referencedTable.String,
					OnUpdate:         catalog.onUpdate,
					OnDelete:         catalog.onDelete,
					Match:            catalog.match,
				},
			)
			current = &foreignKeys[len(foreignKeys)-1]
			uniqueName = catalog.uniqueConstraintName.String
		} else if catalog.position != len(current.Columns)+1 ||
			catalog.referencedTable.String !=
				current.ReferencedTable ||
			catalog.onUpdate != current.OnUpdate ||
			catalog.onDelete != current.OnDelete ||
			catalog.match != current.Match ||
			catalog.uniqueConstraintName.String != uniqueName ||
			!columns[catalog.column] ||
			!validMySQLForeignKeyCatalog(table, catalog) {
			return nil, mariaDB1011SourcePolicy(
				"foreign key column order",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		current.Columns = append(current.Columns, catalog.column)
		current.ReferencedColumns = append(
			current.ReferencedColumns,
			catalog.referencedColumn.String,
		)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate MariaDB foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return foreignKeys, nil
}
