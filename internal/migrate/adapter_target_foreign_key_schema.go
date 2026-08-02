package migrate

import (
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// rebaseProjectedForeignKeySchemas maps owner-relative and explicit
// owner-schema references into the one target namespace selected by an
// adapter. A reference to any other source namespace cannot be represented
// without collapsing distinct relation identities, so it fails before any
// target DDL is planned.
func rebaseProjectedForeignKeySchemas(
	sourceSchema string,
	targetSchema string,
	targetEngine string,
	table *schema.Table,
) error {
	foreignKeys := make([]schema.ForeignKey, len(table.ForeignKeys))
	for index, source := range table.ForeignKeys {
		foreignKeys[index] = source
		foreignKeys[index].Columns = append(
			[]string(nil),
			source.Columns...,
		)
		foreignKeys[index].ReferencedColumns = append(
			[]string(nil),
			source.ReferencedColumns...,
		)
	}
	table.ForeignKeys = foreignKeys
	for index := range table.ForeignKeys {
		foreignKey := &table.ForeignKeys[index]
		if foreignKey.ReferencedSchema == "" {
			foreignKey.ReferencedSchema = targetSchema
			continue
		}
		if foreignKey.ReferencedSchema != sourceSchema {
			return fmt.Errorf(
				"map %s foreign key %s.%s: cross-schema reference %s.%s cannot be preserved in the single target namespace",
				targetEngine,
				table.Name,
				foreignKey.Name,
				foreignKey.ReferencedSchema,
				foreignKey.ReferencedTable,
			)
		}
		foreignKey.ReferencedSchema = targetSchema
	}
	return nil
}
