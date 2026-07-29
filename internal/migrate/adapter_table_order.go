package migrate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// orderAdapterSourceTablesForMode returns the execution order for an already
// inspected table set. Drop/recreate keeps source order because constraints do
// not exist while rows load. Upsert retains constraints, so every selected
// parent must be written before its selected children.
func orderAdapterSourceTablesForMode(
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	ordered := append([]schema.Table(nil), tables...)
	if mode != "upsert" || len(ordered) < 2 {
		if mode == "upsert" && len(ordered) == 1 &&
			hasInScopeSelfReference(ordered[0]) {
			return nil, upsertDependencyCycleError(
				[]string{ordered[0].Name},
			)
		}
		return ordered, nil
	}

	byKey := make(map[string]schema.Table, len(ordered))
	indegree := make(map[string]int, len(ordered))
	children := make(map[string]map[string]struct{}, len(ordered))
	for _, table := range ordered {
		key := adapterSourceTableKey(table.Schema, table.Name)
		if _, exists := byKey[key]; exists {
			return nil, fmt.Errorf(
				"upsert source table %s is duplicated",
				table.Name,
			)
		}
		byKey[key] = table
		indegree[key] = 0
	}
	for _, child := range ordered {
		childKey := adapterSourceTableKey(child.Schema, child.Name)
		for _, foreignKey := range child.ForeignKeys {
			parentKey := adapterSourceTableKey(
				child.Schema,
				foreignKey.ReferencedTable,
			)
			if _, selected := byKey[parentKey]; !selected {
				continue
			}
			if children[parentKey] == nil {
				children[parentKey] = make(map[string]struct{})
			}
			if _, duplicate := children[parentKey][childKey]; duplicate {
				continue
			}
			children[parentKey][childKey] = struct{}{}
			indegree[childKey]++
		}
	}

	result := make([]schema.Table, 0, len(ordered))
	emitted := make(map[string]struct{}, len(ordered))
	for len(result) < len(ordered) {
		var available []string
		for key, count := range indegree {
			if count != 0 {
				continue
			}
			if _, done := emitted[key]; done {
				continue
			}
			available = append(available, key)
		}
		if len(available) == 0 {
			var cycle []string
			for key, table := range byKey {
				if _, done := emitted[key]; !done {
					cycle = append(cycle, table.Name)
				}
			}
			sort.Strings(cycle)
			return nil, upsertDependencyCycleError(cycle)
		}
		sort.Strings(available)
		next := available[0]
		emitted[next] = struct{}{}
		result = append(result, byKey[next])
		for childKey := range children[next] {
			indegree[childKey]--
		}
	}
	return result, nil
}

func hasInScopeSelfReference(table schema.Table) bool {
	for _, foreignKey := range table.ForeignKeys {
		if foreignKey.ReferencedTable == table.Name {
			return true
		}
	}
	return false
}

func adapterSourceTableKey(namespace, table string) string {
	return namespace + "\x00" + table
}

func upsertDependencyCycleError(tables []string) error {
	return fmt.Errorf(
		"upsert foreign-key dependency cycle among tables: %s",
		strings.Join(tables, ", "),
	)
}
