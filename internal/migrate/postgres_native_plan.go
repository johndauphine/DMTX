package migrate

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

func validatePostgresWriteShape(
	table schema.Table,
	columns []string,
	mode string,
) error {
	if table.Schema == "" || table.Name == "" {
		return fmt.Errorf(
			"PostgreSQL target schema and table name are required",
		)
	}
	if mode != "drop_recreate" && mode != "upsert" {
		return fmt.Errorf(
			"write PostgreSQL table %s: unsupported target mode %q",
			table.Name,
			mode,
		)
	}
	if len(columns) == 0 {
		return fmt.Errorf(
			"write PostgreSQL table %s: at least one column is required",
			table.Name,
		)
	}

	metadata := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if column.Name == "" {
			return fmt.Errorf(
				"write PostgreSQL table %s: schema contains an empty column name",
				table.Name,
			)
		}
		if _, duplicate := metadata[column.Name]; duplicate {
			return fmt.Errorf(
				"write PostgreSQL table %s: schema contains duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		metadata[column.Name] = struct{}{}
	}

	requested := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf(
				"write PostgreSQL table %s: requested column name is empty",
				table.Name,
			)
		}
		if _, duplicate := requested[column]; duplicate {
			return fmt.Errorf(
				"write PostgreSQL table %s: requested column %s is duplicated",
				table.Name,
				column,
			)
		}
		if _, exists := metadata[column]; !exists {
			return fmt.Errorf(
				"write PostgreSQL table %s: requested column %s is not present in schema",
				table.Name,
				column,
			)
		}
		requested[column] = struct{}{}
	}

	if mode != "upsert" {
		return nil
	}
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return fmt.Errorf(
			"table %s has no primary key; PostgreSQL upsert requires a primary key",
			table.Name,
		)
	}
	for _, key := range keys {
		if _, included := requested[key]; !included {
			return fmt.Errorf(
				"write PostgreSQL table %s: upsert primary-key column %s is not included",
				table.Name,
				key,
			)
		}
	}
	return nil
}

func postgresStageTableName(
	table schema.Table,
	columns []string,
) string {
	identity := table.Schema + "\x00" + table.Name + "\x00" +
		strings.Join(columns, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("dmtx_stage_%x", digest[:8])
}

func postgresCreateStageStatement(
	table schema.Table,
	columns []string,
	stage string,
) string {
	return "CREATE TEMP TABLE " + postgresIdentifier(stage) +
		" ON COMMIT DROP AS SELECT " + quotedColumns(columns) +
		" FROM " + postgresQualified(table.Schema, table.Name) +
		" WITH NO DATA"
}

func postgresMergeStageStatement(
	table schema.Table,
	columns []string,
	stage string,
) (string, bool, error) {
	keys := primaryKeyColumns(table)
	if len(keys) == 0 {
		return "", false, fmt.Errorf(
			"table %s has no primary key; PostgreSQL upsert requires a primary key",
			table.Name,
		)
	}
	statement := "INSERT INTO " +
		postgresQualified(table.Schema, table.Name) +
		" (" + quotedColumns(columns) + ") SELECT " +
		quotedColumns(columns) + " FROM " +
		postgresQualified("pg_temp", stage) +
		" WHERE true ON CONFLICT (" + quotedColumns(keys) + ")"

	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if !contains(keys, column) {
			updates = append(
				updates,
				postgresIdentifier(column)+" = EXCLUDED."+
					postgresIdentifier(column),
			)
		}
	}
	if len(updates) == 0 {
		return statement + " DO NOTHING", false, nil
	}
	return statement + " DO UPDATE SET " +
		strings.Join(updates, ", "), true, nil
}
