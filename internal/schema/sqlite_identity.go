package schema

import "strings"

// sqliteIdentityColumn validates the portable identity shape SQLite can
// preserve as INTEGER PRIMARY KEY AUTOINCREMENT. SQLite's INTEGER storage
// covers the signed widths supported by DMTX's neutral identity frontier.
func sqliteIdentityColumn(table Table) (string, error) {
	if table.Identity == nil {
		return "", nil
	}
	identity := table.Identity
	if identity.Generation != IdentityByDefault ||
		identity.Column == "" ||
		table.SQLiteWithoutRowID ||
		identity.Frontier != nil && *identity.Frontier < 0 {
		return "", sqliteIdentityPolicy(table.Name)
	}

	columnIndex := -1
	for index, column := range table.Columns {
		if column.Name == identity.Column {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		return "", sqliteIdentityPolicy(table.Name)
	}
	keys := orderedPrimaryKeyColumns(table)
	if len(keys) != 1 || keys[0].Name != identity.Column {
		return "", sqliteIdentityPolicy(table.Name)
	}
	column := table.Columns[columnIndex]
	if column.Default != nil {
		return "", sqliteIdentityPolicy(table.Name)
	}
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		if len(column.DeclaredType.Arguments) != 0 {
			return "", sqliteIdentityPolicy(table.Name)
		}
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	switch base {
	case "tinyint", "smallint", "int2", "mediumint",
		"int", "integer", "int4", "bigint", "int8":
	default:
		return "", sqliteIdentityPolicy(table.Name)
	}
	return column.Name, nil
}

func sqliteIdentityPolicy(table string) error {
	return &PolicyError{
		Operation: "render SQLite identity",
		Type:      table,
		Target:    string(SQLite),
	}
}
