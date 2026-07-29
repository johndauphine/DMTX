package schema

import (
	"sort"
	"strings"
)

// DropTable renders a dialect-safe removal of one fully-qualified table.
// The schema planner owns this operation so migration services never assemble
// schema-changing SQL from catalog text.
func DropTable(target Dialect, table Table) (string, error) {
	if table.Name == "" {
		return "", &PolicyError{Operation: "drop table", Type: "empty table name", Target: string(target)}
	}
	return "DROP TABLE IF EXISTS " + qualified(target, table.Schema, table.Name) + ";", nil
}

// DropTables renders one deterministic PostgreSQL statement that removes the
// entire selected table set together. In-scope foreign-key dependencies are
// therefore removed as part of the same operation, while dependencies from
// out-of-scope objects still fail closed because CASCADE is never used.
func DropTables(target Dialect, tables []Table) (string, error) {
	if target != Postgres {
		return "", &PolicyError{
			Operation: "drop multiple tables",
			Type:      string(target),
			Target:    string(target),
		}
	}
	if len(tables) == 0 {
		return "", &PolicyError{
			Operation: "drop multiple tables",
			Type:      "empty table set",
			Target:    string(target),
		}
	}
	identities := make([]string, len(tables))
	seen := make(map[string]struct{}, len(tables))
	for index, table := range tables {
		if table.Name == "" {
			return "", &PolicyError{
				Operation: "drop multiple tables",
				Type:      "empty table name",
				Target:    string(target),
			}
		}
		identity := qualified(target, table.Schema, table.Name)
		if _, duplicate := seen[identity]; duplicate {
			return "", &PolicyError{
				Operation: "drop multiple tables",
				Type:      "duplicate table " + table.Name,
				Target:    string(target),
			}
		}
		seen[identity] = struct{}{}
		identities[index] = identity
	}
	sort.Strings(identities)
	return "DROP TABLE IF EXISTS " + strings.Join(identities, ", ") + ";", nil
}
