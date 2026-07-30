package migrate

import (
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// validatePostgresSQLiteTables closes the whole-plan invariants that cannot be
// proven while projecting one table in isolation. SQLite object names are
// database-global, and the target enforces foreign keys while tables are
// written, so every referenced parent must be selected and precede its child.
func validatePostgresSQLiteTables(
	sourceTables []schema.Table,
	targetTables []schema.Table,
) error {
	if len(sourceTables) != len(targetTables) {
		return sqlitePostgresProjectionPolicy(
			"map PostgreSQL table set",
			"source and target table counts differ",
		)
	}

	sourceByName := make(map[string]schema.Table, len(sourceTables))
	sourcePositions := make(map[string]int, len(sourceTables))
	targetByName := make(map[string]schema.Table, len(targetTables))
	objectNames := make(map[string]string)
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		if sourceTable.Name == "" ||
			sourceTable.Name != targetTable.Name ||
			targetTable.Schema != "" {
			return sqlitePostgresProjectionPolicy(
				"map PostgreSQL table set order",
				sourceTable.Name,
			)
		}
		key := strings.ToLower(targetTable.Name)
		if earlier, collision := objectNames[key]; collision {
			return sqlitePostgresProjectionPolicy(
				"map SQLite global object names",
				earlier+" and table "+targetTable.Name,
			)
		}
		objectNames[key] = "table " + targetTable.Name
		sourceByName[sourceTable.Name] = sourceTable
		sourcePositions[sourceTable.Name] = index
		targetByName[targetTable.Name] = targetTable
	}
	for _, targetTable := range targetTables {
		for _, targetIndex := range targetTable.Indexes {
			if targetIndex.Inline {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL index lifecycle",
					targetTable.Name+"."+targetIndex.Name,
				)
			}
			key := strings.ToLower(targetIndex.Name)
			if earlier, collision := objectNames[key]; collision {
				return sqlitePostgresProjectionPolicy(
					"map SQLite global object names",
					earlier+" and index "+targetIndex.Name,
				)
			}
			objectNames[key] = "index " + targetIndex.Name
		}
	}

	for childPosition, child := range sourceTables {
		localColumns := postgresSQLiteColumnMap(child)
		targetChild := targetByName[child.Name]
		if len(targetChild.ForeignKeys) != len(child.ForeignKeys) {
			return sqlitePostgresProjectionPolicy(
				"map PostgreSQL foreign-key set",
				child.Name,
			)
		}
		for foreignKeyIndex, foreignKey := range child.ForeignKeys {
			parent, selected := sourceByName[foreignKey.ReferencedTable]
			parentPosition := sourcePositions[foreignKey.ReferencedTable]
			if !selected {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign key",
					child.Name+"."+foreignKey.Name+
						" references an unselected table",
				)
			}
			if parentPosition >= childPosition {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key plan order",
					child.Name+"."+foreignKey.Name+
						" requires parent "+
						foreignKey.ReferencedTable+
						" before child",
				)
			}
			if len(foreignKey.Columns) == 0 ||
				len(foreignKey.Columns) !=
					len(foreignKey.ReferencedColumns) {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign key",
					child.Name+"."+foreignKey.Name,
				)
			}
			if !postgresSQLitePrimaryKeyMatches(
				parent,
				foreignKey.ReferencedColumns,
			) {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key parent key",
					child.Name+"."+foreignKey.Name,
				)
			}

			parentColumns := postgresSQLiteColumnMap(parent)
			targetForeignKey :=
				targetChild.ForeignKeys[foreignKeyIndex]
			if targetForeignKey.ReferencedTable !=
				foreignKey.ReferencedTable ||
				len(targetForeignKey.Columns) !=
					len(foreignKey.Columns) ||
				len(targetForeignKey.ReferencedColumns) !=
					len(foreignKey.ReferencedColumns) {
				return sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key target shape",
					child.Name+"."+foreignKey.Name,
				)
			}
			for pairIndex, localName := range foreignKey.Columns {
				referencedName :=
					foreignKey.ReferencedColumns[pairIndex]
				local, localExists := localColumns[localName]
				referenced, referencedExists :=
					parentColumns[referencedName]
				localKind :=
					postgresSQLiteExactReferenceKind(local)
				referencedKind :=
					postgresSQLiteExactReferenceKind(referenced)
				if !localExists ||
					!referencedExists ||
					localKind == "" ||
					localKind != referencedKind ||
					targetForeignKey.Columns[pairIndex] !=
						localName ||
					targetForeignKey.ReferencedColumns[pairIndex] !=
						referencedName {
					return sqlitePostgresProjectionPolicy(
						"map PostgreSQL foreign-key comparison",
						child.Name+"."+foreignKey.Name+
							"."+localName,
					)
				}
			}
		}
	}
	return nil
}

func postgresSQLiteColumnMap(
	table schema.Table,
) map[string]schema.Column {
	columns := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	return columns
}

func postgresSQLitePrimaryKeyMatches(
	table schema.Table,
	names []string,
) bool {
	if len(names) == 0 {
		return false
	}
	positions := make(map[int]string)
	for _, column := range table.Columns {
		if column.PrimaryKeyPosition > 0 {
			positions[column.PrimaryKeyPosition] = column.Name
		}
	}
	if len(positions) != len(names) {
		return false
	}
	for index, name := range names {
		if positions[index+1] != name {
			return false
		}
	}
	return true
}
