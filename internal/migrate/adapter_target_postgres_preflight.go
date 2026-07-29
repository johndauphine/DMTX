package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

const postgresDefaultTimestampPrecision = 6

type postgresCatalogTypeShape struct {
	name               string
	characterLength    *int
	numericPrecision   *int
	numericScale       *int
	timestampPrecision *int
}

type postgresCatalogColumnShape struct {
	name             string
	columnType       postgresCatalogTypeShape
	notNull          bool
	generated        string
	identity         string
	defaultCollation bool
}

type postgresCatalogTableShape struct {
	relationKind string
	persistence  string
	columns      []postgresCatalogColumnShape
	primaryKey   []string
	userTriggers int
	userRules    int
	rowSecurity  bool
}

func (adapter *postgresTargetAdapter) PreflightTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	for _, targetTable := range targetTables {
		if mode != "upsert" {
			if err := preflightPostgresDropRecreateTable(
				ctx,
				adapter.database,
				targetTable,
			); err != nil {
				return err
			}
			continue
		}
		actual, exists, err := readPostgresUpsertCatalogShape(
			ctx,
			adapter.database,
			targetTable,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if !exists {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: upsert requires an existing target table",
				targetTable.Name,
			)
		}
		if err := validatePostgresUpsertCatalogShape(
			targetTable,
			actual,
		); err != nil {
			return fmt.Errorf(
				"preflight PostgreSQL table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if targetTable.AutoIncrementColumn != "" {
			if err := preflightPostgresIdentitySequence(
				ctx,
				adapter.database,
				targetTable,
			); err != nil {
				return fmt.Errorf(
					"preflight PostgreSQL table %s: %w",
					targetTable.Name,
					err,
				)
			}
		}
	}
	if mode == "upsert" {
		if err := preflightPostgresRetainedIndexesAndForeignKeys(
			ctx,
			adapter.database,
			targetTables,
		); err != nil {
			return err
		}
		if err := preflightPostgresRetainedChecks(
			ctx,
			adapter.database,
			targetTables,
		); err != nil {
			return err
		}
		if err := preflightPostgresRetainedDefaults(
			ctx,
			adapter.database,
			targetTables,
		); err != nil {
			return err
		}
	}
	return nil
}

// Drop/recreate preflight deliberately retains its previous read-only
// existence query. Shape compatibility matters only when existing objects are
// retained by upsert.
func preflightPostgresDropRecreateTable(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) error {
	var exists bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`,
		table.Schema,
		table.Name,
	).Scan(&exists); err != nil {
		return fmt.Errorf(
			"preflight PostgreSQL table %s: %w",
			table.Name,
			err,
		)
	}
	return nil
}

func readPostgresUpsertCatalogShape(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) (postgresCatalogTableShape, bool, error) {
	var result postgresCatalogTableShape
	err := database.QueryRowContext(
		ctx,
		`SELECT
			relation.relkind::text,
			relation.relpersistence::text,
			relation.relrowsecurity OR relation.relforcerowsecurity
		   FROM pg_catalog.pg_class AS relation
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2`,
		table.Schema,
		table.Name,
	).Scan(
		&result.relationKind,
		&result.persistence,
		&result.rowSecurity,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return postgresCatalogTableShape{}, false, nil
	}
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	if result.relationKind != "r" {
		return result, true, nil
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT
			attribute.attname,
			column_type.typname,
			attribute.atttypmod,
			attribute.attnotnull,
			attribute.attgenerated::text,
			attribute.attidentity::text,
			attribute.attcollation = column_type.typcollation
		   FROM pg_catalog.pg_attribute AS attribute
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = attribute.attrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		   JOIN pg_catalog.pg_type AS column_type
		     ON column_type.oid = attribute.atttypid
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND relation.relkind = 'r'
		    AND attribute.attnum > 0
		    AND NOT attribute.attisdropped
		  ORDER BY attribute.attnum`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			column   postgresCatalogColumnShape
			typeName string
			typeMode int32
		)
		if err := rows.Scan(
			&column.name,
			&typeName,
			&typeMode,
			&column.notNull,
			&column.generated,
			&column.identity,
			&column.defaultCollation,
		); err != nil {
			return postgresCatalogTableShape{}, false, err
		}
		column.columnType = postgresCatalogTypeFromModifier(
			typeName,
			typeMode,
		)
		result.columns = append(result.columns, column)
	}
	if err := rows.Err(); err != nil {
		return postgresCatalogTableShape{}, false, err
	}

	keyRows, err := database.QueryContext(
		ctx,
		`SELECT attribute.attname
		   FROM pg_catalog.pg_index AS index
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = index.indrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		   CROSS JOIN LATERAL unnest(index.indkey)
		     WITH ORDINALITY AS key_column(attnum, position)
		   JOIN pg_catalog.pg_attribute AS attribute
		     ON attribute.attrelid = relation.oid
		    AND attribute.attnum = key_column.attnum
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND index.indisprimary
		  ORDER BY key_column.position`,
		table.Schema,
		table.Name,
	)
	if err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	defer keyRows.Close()
	for keyRows.Next() {
		var name string
		if err := keyRows.Scan(&name); err != nil {
			return postgresCatalogTableShape{}, false, err
		}
		result.primaryKey = append(result.primaryKey, name)
	}
	if err := keyRows.Err(); err != nil {
		return postgresCatalogTableShape{}, false, err
	}

	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_trigger AS trigger
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = trigger.tgrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2
		    AND NOT trigger.tgisinternal`,
		table.Schema,
		table.Name,
	).Scan(&result.userTriggers); err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_rewrite AS rewrite_rule
		   JOIN pg_catalog.pg_class AS relation
		     ON relation.oid = rewrite_rule.ev_class
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = relation.relnamespace
		  WHERE namespace.nspname = $1
		    AND relation.relname = $2`,
		table.Schema,
		table.Name,
	).Scan(&result.userRules); err != nil {
		return postgresCatalogTableShape{}, false, err
	}
	return result, true, nil
}

func postgresCatalogTypeFromModifier(
	name string,
	typeMode int32,
) postgresCatalogTypeShape {
	result := postgresCatalogTypeShape{name: name}
	if (name == "bpchar" || name == "varchar") && typeMode >= 4 {
		length := int(typeMode - 4)
		result.characterLength = &length
	}
	if name == "numeric" && typeMode >= 4 {
		modifier := int(typeMode - 4)
		precision := (modifier >> 16) & 0xffff
		scale := modifier & 0x7ff
		if scale >= 1024 {
			scale -= 2048
		}
		result.numericPrecision = &precision
		result.numericScale = &scale
	}
	if name == "timestamp" || name == "timestamptz" {
		precision := postgresDefaultTimestampPrecision
		if typeMode >= 0 {
			precision = int(typeMode)
		}
		result.timestampPrecision = &precision
	}
	return result
}

func validatePostgresUpsertCatalogShape(
	planned schema.Table,
	actual postgresCatalogTableShape,
) error {
	if actual.relationKind != "r" {
		return fmt.Errorf(
			"upsert target is not an ordinary PostgreSQL table (catalog relation kind %q)",
			actual.relationKind,
		)
	}
	if actual.persistence != "p" {
		return fmt.Errorf(
			"upsert target persistence is %s; only permanent PostgreSQL tables are supported",
			describePostgresPersistence(actual.persistence),
		)
	}
	if len(actual.columns) != len(planned.Columns) {
		return fmt.Errorf(
			"upsert target has %d migrated columns; planned shape requires exactly %d",
			len(actual.columns),
			len(planned.Columns),
		)
	}
	plannedKey := primaryKeyColumns(planned)
	for index, plannedColumn := range planned.Columns {
		actualColumn := actual.columns[index]
		position := index + 1
		if actualColumn.name != plannedColumn.Name {
			return fmt.Errorf(
				"upsert target column %d is %q; planned column is %q",
				position,
				actualColumn.name,
				plannedColumn.Name,
			)
		}
		if actualColumn.generated != "" {
			return fmt.Errorf(
				"upsert target column %q is generated",
				actualColumn.name,
			)
		}
		expectedIdentity := ""
		if plannedColumn.Name == planned.AutoIncrementColumn {
			expectedIdentity = "d"
		}
		if actualColumn.identity != expectedIdentity {
			return fmt.Errorf(
				"upsert target column %q identity generation differs from the planned shape",
				actualColumn.name,
			)
		}
		expectedType, err := expectedPostgresCatalogType(plannedColumn)
		if err != nil {
			return err
		}
		if !samePostgresCatalogType(expectedType, actualColumn.columnType) {
			return fmt.Errorf(
				"upsert target column %q type is %s; planned type is %s",
				actualColumn.name,
				describePostgresCatalogType(actualColumn.columnType),
				describePostgresCatalogType(expectedType),
			)
		}
		if !actualColumn.defaultCollation {
			return fmt.Errorf(
				"upsert target column %q uses a non-default collation",
				actualColumn.name,
			)
		}
		expectedNotNull := !plannedColumn.Nullable ||
			contains(plannedKey, plannedColumn.Name)
		if actualColumn.notNull != expectedNotNull {
			return fmt.Errorf(
				"upsert target column %q effective nullability differs from the planned shape",
				actualColumn.name,
			)
		}
	}
	if !sameStrings(actual.primaryKey, plannedKey) {
		return fmt.Errorf(
			"upsert target primary key is (%s); planned primary key is (%s)",
			strings.Join(actual.primaryKey, ", "),
			strings.Join(plannedKey, ", "),
		)
	}
	if actual.userTriggers != 0 {
		return fmt.Errorf(
			"upsert target has %d non-internal user triggers",
			actual.userTriggers,
		)
	}
	if actual.userRules != 0 {
		return fmt.Errorf(
			"upsert target has %d user rewrite rules",
			actual.userRules,
		)
	}
	if actual.rowSecurity {
		return fmt.Errorf("upsert target has row-level security enabled or forced")
	}
	return nil
}

func expectedPostgresCatalogType(
	column schema.Column,
) (postgresCatalogTypeShape, error) {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
		switch base {
		case "char", "character":
			length, constrained, err := postgresCharacterColumnLength(column)
			if err != nil || !constrained {
				return postgresCatalogTypeShape{}, fmt.Errorf(
					"planned PostgreSQL column %q has invalid character modifiers",
					column.Name,
				)
			}
			return postgresCatalogTypeShape{
				name:            "bpchar",
				characterLength: intPointer(length),
			}, nil
		case "varchar", "character varying":
			length, constrained, err := postgresCharacterColumnLength(column)
			if err != nil || !constrained {
				return postgresCatalogTypeShape{}, fmt.Errorf(
					"planned PostgreSQL column %q has invalid character modifiers",
					column.Name,
				)
			}
			return postgresCatalogTypeShape{
				name:            "varchar",
				characterLength: intPointer(length),
			}, nil
		case "timestamp", "datetime":
			precision, constrained, err :=
				postgresTemporalColumnPrecision(column)
			if err != nil || !constrained {
				return postgresCatalogTypeShape{}, fmt.Errorf(
					"planned PostgreSQL column %q has invalid temporal modifiers",
					column.Name,
				)
			}
			return postgresCatalogTypeShape{
				name: "timestamp",
				timestampPrecision: intPointer(
					precision,
				),
			}, nil
		case "timestamptz":
			precision, constrained, err :=
				postgresTemporalColumnPrecision(column)
			if err != nil || !constrained {
				return postgresCatalogTypeShape{}, fmt.Errorf(
					"planned PostgreSQL column %q has invalid temporal modifiers",
					column.Name,
				)
			}
			return postgresCatalogTypeShape{
				name: "timestamptz",
				timestampPrecision: intPointer(
					precision,
				),
			}, nil
		}
	}
	switch base {
	case "int", "integer", "int4":
		return postgresCatalogTypeShape{name: "int4"}, nil
	case "bigint", "int8":
		return postgresCatalogTypeShape{name: "int8"}, nil
	case "real", "float", "float4", "double", "double precision", "float8":
		return postgresCatalogTypeShape{name: "float8"}, nil
	case "decimal", "numeric":
		precision, scale, err := postgresNumericColumnModifiers(column)
		if err != nil {
			return postgresCatalogTypeShape{}, fmt.Errorf(
				"planned PostgreSQL column %q has invalid numeric modifiers",
				column.Name,
			)
		}
		return postgresCatalogTypeShape{
			name:             "numeric",
			numericPrecision: intPointer(int(precision)),
			numericScale:     intPointer(int(scale)),
		}, nil
	case "text", "varchar", "character varying":
		return postgresCatalogTypeShape{name: "text"}, nil
	case "uuid":
		return postgresCatalogTypeShape{name: "uuid"}, nil
	case "blob", "binary", "varbinary", "bytea":
		return postgresCatalogTypeShape{name: "bytea"}, nil
	case "json":
		return postgresCatalogTypeShape{name: "json"}, nil
	case "jsonb":
		return postgresCatalogTypeShape{name: "jsonb"}, nil
	case "bool", "boolean":
		return postgresCatalogTypeShape{name: "bool"}, nil
	case "timestamp", "datetime":
		return postgresCatalogTypeShape{
			name:               "timestamp",
			timestampPrecision: intPointer(postgresDefaultTimestampPrecision),
		}, nil
	case "timestamptz":
		return postgresCatalogTypeShape{
			name:               "timestamptz",
			timestampPrecision: intPointer(postgresDefaultTimestampPrecision),
		}, nil
	case "date":
		return postgresCatalogTypeShape{name: "date"}, nil
	default:
		return postgresCatalogTypeShape{}, fmt.Errorf(
			"planned PostgreSQL column %q has unsupported catalog type %q",
			column.Name,
			base,
		)
	}
}

func samePostgresCatalogType(
	left postgresCatalogTypeShape,
	right postgresCatalogTypeShape,
) bool {
	return left.name == right.name &&
		sameOptionalInt(left.characterLength, right.characterLength) &&
		sameOptionalInt(left.numericPrecision, right.numericPrecision) &&
		sameOptionalInt(left.numericScale, right.numericScale) &&
		sameOptionalInt(left.timestampPrecision, right.timestampPrecision)
}

func sameOptionalInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func describePostgresCatalogType(value postgresCatalogTypeShape) string {
	switch value.name {
	case "bpchar":
		if value.characterLength != nil {
			return fmt.Sprintf("character(%d)", *value.characterLength)
		}
		return "character"
	case "varchar":
		if value.characterLength != nil {
			return fmt.Sprintf(
				"character varying(%d)",
				*value.characterLength,
			)
		}
		return "character varying"
	case "numeric":
		if value.numericPrecision != nil && value.numericScale != nil {
			return fmt.Sprintf(
				"numeric(%d,%d)",
				*value.numericPrecision,
				*value.numericScale,
			)
		}
		return "numeric"
	case "timestamp":
		if value.timestampPrecision != nil {
			return fmt.Sprintf(
				"timestamp(%d) without time zone",
				*value.timestampPrecision,
			)
		}
		return "timestamp without time zone"
	case "timestamptz":
		if value.timestampPrecision != nil {
			return fmt.Sprintf(
				"timestamp(%d) with time zone",
				*value.timestampPrecision,
			)
		}
		return "timestamp with time zone"
	default:
		return value.name
	}
}

func describePostgresPersistence(value string) string {
	switch value {
	case "p":
		return "permanent"
	case "u":
		return "UNLOGGED"
	case "t":
		return "temporary"
	default:
		return fmt.Sprintf("catalog value %q", value)
	}
}

func intPointer(value int) *int {
	return &value
}
