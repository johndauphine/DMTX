package schema

// DropTable renders a dialect-safe removal of one fully-qualified table.
// The schema planner owns this operation so migration services never assemble
// schema-changing SQL from catalog text.
func DropTable(target Dialect, table Table) (string, error) {
	if table.Name == "" {
		return "", &PolicyError{Operation: "drop table", Type: "empty table name", Target: string(target)}
	}
	return "DROP TABLE IF EXISTS " + qualified(target, table.Schema, table.Name) + ";", nil
}
