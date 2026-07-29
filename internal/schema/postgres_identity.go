package schema

import "strings"

// postgresIdentityColumn validates the narrow identity shape that preserves
// SQLite AUTOINCREMENT semantics on PostgreSQL. Explicit source keys remain
// loadable through BY DEFAULT identity generation, while the migration layer
// restores the sequence frontier after row transfer.
func postgresIdentityColumn(table Table) (string, error) {
	if table.AutoIncrementColumn == "" {
		if table.SQLiteSequence != nil {
			return "", postgresIdentityPolicy(table.Name)
		}
		return "", nil
	}
	if table.SQLiteSequence != nil && *table.SQLiteSequence < 0 {
		return "", postgresIdentityPolicy(table.Name)
	}

	columnIndex := -1
	for index, column := range table.Columns {
		if column.Name == table.AutoIncrementColumn {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		return "", postgresIdentityPolicy(table.Name)
	}

	keys := orderedPrimaryKeyColumns(table)
	if len(keys) != 1 || keys[0].Name != table.AutoIncrementColumn {
		return "", postgresIdentityPolicy(table.Name)
	}
	column := table.Columns[columnIndex]
	if column.Default != nil {
		return "", postgresIdentityPolicy(table.Name)
	}
	renderedType, err := renderColumnType(column, Postgres)
	if err != nil || !strings.EqualFold(renderedType, "BIGINT") {
		return "", postgresIdentityPolicy(table.Name)
	}
	return column.Name, nil
}

func postgresIdentityPolicy(table string) error {
	return &PolicyError{
		Operation: "map SQLite AUTOINCREMENT",
		Type:      table,
		Target:    string(Postgres),
	}
}
