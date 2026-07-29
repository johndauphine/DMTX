package schema

import "strings"

// postgresIdentityColumn validates the engine-neutral identity shape supported
// by PostgreSQL. Explicit source keys remain loadable through BY DEFAULT
// identity generation, while the migration layer restores the source frontier
// after row transfer.
func postgresIdentityColumn(table Table) (string, error) {
	if table.Identity == nil {
		return "", nil
	}
	identity := table.Identity
	if identity.Generation != IdentityByDefault ||
		identity.Column == "" ||
		identity.Frontier != nil && *identity.Frontier < 0 {
		return "", postgresIdentityPolicy(table.Name)
	}

	columnIndex := -1
	for index, column := range table.Columns {
		if column.Name == identity.Column {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		return "", postgresIdentityPolicy(table.Name)
	}

	keys := orderedPrimaryKeyColumns(table)
	if len(keys) != 1 || keys[0].Name != identity.Column {
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
		Operation: "render PostgreSQL identity",
		Type:      table,
		Target:    string(Postgres),
	}
}
