package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type mysql80PrimaryKeyCatalog struct {
	constraintName string
	constraintType string
	enforced       string
	position       int
	column         sql.NullString
	direction      sql.NullString
	nonUnique      int
	indexType      string
	prefixLength   sql.NullInt64
	packed         sql.NullString
	nullable       string
	comment        string
	indexComment   string
	visible        string
	expression     sql.NullString
}

const mysql80SourcePrimaryKeyQuery = `
	SELECT
		source_constraint.CONSTRAINT_NAME,
		source_constraint.CONSTRAINT_TYPE,
		source_constraint.ENFORCED,
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
		source_index.IS_VISIBLE,
		source_index.EXPRESSION
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

func discoverMySQL80SourcePrimaryKey(
	ctx context.Context,
	database *sql.DB,
	table *schema.Table,
) error {
	rows, err := database.QueryContext(
		ctx,
		mysql80SourcePrimaryKeyQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return fmt.Errorf(
			"list MySQL primary key for %s.%s: %w",
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
		var catalog mysql80PrimaryKeyCatalog
		if err := rows.Scan(
			&catalog.constraintName,
			&catalog.constraintType,
			&catalog.enforced,
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
			&catalog.visible,
			&catalog.expression,
		); err != nil {
			return fmt.Errorf(
				"read MySQL primary key for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		position++
		if catalog.constraintName != "PRIMARY" ||
			catalog.constraintType != "PRIMARY KEY" ||
			catalog.enforced != "YES" ||
			catalog.position != position ||
			!catalog.column.Valid ||
			!catalog.direction.Valid ||
			catalog.direction.String != "A" ||
			catalog.nonUnique != 0 ||
			!strings.EqualFold(catalog.indexType, "BTREE") ||
			catalog.prefixLength.Valid ||
			catalog.packed.Valid ||
			catalog.nullable != "" ||
			catalog.comment != "" ||
			catalog.indexComment != "" ||
			catalog.visible != "YES" ||
			catalog.expression.Valid {
			return mysqlSourcePolicy(
				"primary key catalog shape",
				table.Schema+"."+table.Name,
			)
		}
		index, ok := byName[catalog.column.String]
		if !ok ||
			table.Columns[index].PrimaryKey ||
			table.Columns[index].PrimaryKeyPosition != 0 ||
			table.Columns[index].Nullable {
			return mysqlSourcePolicy(
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
			"iterate MySQL primary key for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return nil
}

type mysql80IndexCatalog struct {
	name            string
	nonUnique       int
	position        int
	column          sql.NullString
	direction       sql.NullString
	prefixLength    sql.NullInt64
	packed          sql.NullString
	indexNullable   string
	indexType       string
	comment         string
	indexComment    string
	visible         string
	expression      sql.NullString
	dataType        string
	columnCollation sql.NullString
	columnNullable  string
}

const mysql80SourceIndexesQuery = `
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
		source_index.IS_VISIBLE,
		source_index.EXPRESSION,
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

func discoverMySQL80SourceIndexes(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]schema.Index, error) {
	rows, err := database.QueryContext(
		ctx,
		mysql80SourceIndexesQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list MySQL indexes for %s.%s: %w",
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
		var catalog mysql80IndexCatalog
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
			&catalog.visible,
			&catalog.expression,
			&catalog.dataType,
			&catalog.columnCollation,
			&catalog.columnNullable,
		); err != nil {
			return nil, fmt.Errorf(
				"read MySQL index for %s.%s: %w",
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
				return nil, mysqlSourcePolicy(
					"index catalog shape",
					table.Schema+"."+table.Name+"."+
						catalog.name,
				)
			}
			indexes = append(indexes, schema.Index{
				Name:   catalog.name,
				Unique: catalog.nonUnique == 0,
			})
			current = &indexes[len(indexes)-1]
		} else if catalog.position != len(current.Columns)+1 ||
			current.Unique != (catalog.nonUnique == 0) {
			return nil, mysqlSourcePolicy(
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
			catalog.comment != "" ||
			catalog.indexComment != "" ||
			catalog.visible != "YES" ||
			catalog.expression.Valid {
			return nil, mysqlSourcePolicy(
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
			catalog.columnNullable != mySQLCatalogNullable(column.Nullable) ||
			catalog.indexNullable != mySQLIndexNullable(column.Nullable) {
			return nil, mysqlSourcePolicy(
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
				!mySQLBinaryUTF8Collation(
					catalog.columnCollation.String,
				) {
				return nil, mysqlSourcePolicy(
					"index collation",
					table.Schema+"."+table.Name+"."+
						catalog.column.String,
				)
			}
			indexColumn.Collation = "BINARY"
		} else if catalog.columnCollation.Valid {
			return nil, mysqlSourcePolicy(
				"index collation",
				table.Schema+"."+table.Name+"."+
					catalog.column.String,
			)
		}
		for _, existing := range current.Columns {
			if existing.Name == indexColumn.Name {
				return nil, mysqlSourcePolicy(
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
			"iterate MySQL indexes for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return indexes, nil
}

func mySQLSourceTextColumn(column schema.Column) bool {
	if column.DeclaredType == nil {
		return column.Type == "text" ||
			column.Type == "char" ||
			column.Type == "varchar"
	}
	switch column.DeclaredType.Base {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func mySQLCatalogNullable(nullable bool) string {
	if nullable {
		return "YES"
	}
	return "NO"
}

func mySQLIndexNullable(nullable bool) string {
	if nullable {
		return "YES"
	}
	return ""
}

type mysql80CheckCatalog struct {
	name        string
	typ         string
	enforced    string
	checkClause string
}

const mysql80SourceChecksQuery = `
	SELECT
		source_constraint.CONSTRAINT_NAME,
		source_constraint.CONSTRAINT_TYPE,
		source_constraint.ENFORCED,
		source_check.CHECK_CLAUSE
	FROM information_schema.TABLE_CONSTRAINTS AS source_constraint
	JOIN information_schema.CHECK_CONSTRAINTS AS source_check
	  ON source_check.CONSTRAINT_SCHEMA = source_constraint.CONSTRAINT_SCHEMA
	 AND source_check.CONSTRAINT_NAME = source_constraint.CONSTRAINT_NAME
	WHERE source_constraint.TABLE_SCHEMA = ?
	  AND source_constraint.TABLE_NAME = ?
	  AND source_constraint.CONSTRAINT_TYPE = 'CHECK'
	ORDER BY source_constraint.CONSTRAINT_NAME
`

func discoverMySQL80SourceChecks(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) ([]schema.CheckConstraint, error) {
	rows, err := database.QueryContext(
		ctx,
		mysql80SourceChecksQuery,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list MySQL CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()

	checks := make([]schema.CheckConstraint, 0)
	previous := ""
	for rows.Next() {
		var catalog mysql80CheckCatalog
		if err := rows.Scan(
			&catalog.name,
			&catalog.typ,
			&catalog.enforced,
			&catalog.checkClause,
		); err != nil {
			return nil, fmt.Errorf(
				"read MySQL CHECK constraint for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if !validMySQLSourceIdentifier(catalog.name) ||
			catalog.typ != "CHECK" ||
			catalog.enforced != "YES" ||
			catalog.name == previous {
			return nil, mysqlSourcePolicy(
				"CHECK catalog shape",
				table.Schema+"."+table.Name+"."+catalog.name,
			)
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			catalog.checkClause,
			table.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"discover MySQL CHECK %s.%s.%s: %w",
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
		previous = catalog.name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate MySQL CHECK constraints for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return checks, nil
}

type mysql80ForeignKeyCatalog struct {
	name                   string
	position               int
	column                 string
	referencedSchema       sql.NullString
	referencedTable        sql.NullString
	referencedColumn       sql.NullString
	uniquePosition         sql.NullInt64
	uniqueConstraintSchema sql.NullString
	uniqueConstraintName   sql.NullString
	match                  string
	onUpdate               string
	onDelete               string
}

const mysql80SourceForeignKeysQuery = `
	SELECT
		source_key.CONSTRAINT_NAME,
		source_key.ORDINAL_POSITION,
		source_key.COLUMN_NAME,
		source_key.REFERENCED_TABLE_SCHEMA,
		source_key.REFERENCED_TABLE_NAME,
		source_key.REFERENCED_COLUMN_NAME,
		source_key.POSITION_IN_UNIQUE_CONSTRAINT,
		source_reference.UNIQUE_CONSTRAINT_SCHEMA,
		source_reference.UNIQUE_CONSTRAINT_NAME,
		source_reference.MATCH_OPTION,
		source_reference.UPDATE_RULE,
		source_reference.DELETE_RULE
	FROM information_schema.KEY_COLUMN_USAGE AS source_key
	JOIN information_schema.REFERENTIAL_CONSTRAINTS AS source_reference
	  ON source_reference.CONSTRAINT_SCHEMA = source_key.CONSTRAINT_SCHEMA
	 AND source_reference.CONSTRAINT_NAME = source_key.CONSTRAINT_NAME
	 AND source_reference.TABLE_NAME = source_key.TABLE_NAME
	WHERE source_key.TABLE_SCHEMA = ?
	  AND source_key.TABLE_NAME = ?
	  AND source_key.REFERENCED_TABLE_NAME IS NOT NULL
	ORDER BY source_key.CONSTRAINT_NAME, source_key.ORDINAL_POSITION
`

func discoverMySQL80SourceForeignKeys(
	ctx context.Context,
	database *sql.DB,
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
			"list MySQL foreign keys for %s.%s: %w",
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
				"read MySQL foreign key for %s.%s: %w",
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
				return nil, mysqlSourcePolicy(
					"foreign key catalog shape",
					table.Schema+"."+table.Name+"."+
						catalog.name,
				)
			}
			foreignKeys = append(foreignKeys, schema.ForeignKey{
				Name:            catalog.name,
				ReferencedTable: catalog.referencedTable.String,
				OnUpdate:        catalog.onUpdate,
				OnDelete:        catalog.onDelete,
				Match:           catalog.match,
			})
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
			return nil, mysqlSourcePolicy(
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
			"iterate MySQL foreign keys for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	return foreignKeys, nil
}

func validMySQLForeignKeyCatalog(
	table schema.Table,
	value mysql80ForeignKeyCatalog,
) bool {
	if !value.referencedSchema.Valid ||
		value.referencedSchema.String != table.Schema ||
		!value.referencedTable.Valid ||
		!validMySQLSourceIdentifier(value.referencedTable.String) ||
		!value.referencedColumn.Valid ||
		!validMySQLSourceIdentifier(value.referencedColumn.String) ||
		!value.uniquePosition.Valid ||
		value.uniquePosition.Int64 != int64(value.position) ||
		!value.uniqueConstraintSchema.Valid ||
		value.uniqueConstraintSchema.String != table.Schema ||
		!value.uniqueConstraintName.Valid ||
		!validMySQLSourceIdentifier(value.uniqueConstraintName.String) ||
		value.match != "NONE" {
		return false
	}
	return validMySQLForeignKeyAction(value.onUpdate) &&
		validMySQLForeignKeyAction(value.onDelete)
}

func validMySQLForeignKeyAction(value string) bool {
	switch value {
	case "CASCADE", "RESTRICT", "NO ACTION", "SET NULL":
		return true
	default:
		return false
	}
}
