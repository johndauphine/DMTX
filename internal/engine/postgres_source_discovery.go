package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	postgres16MinimumVersion = 160000
	postgres17MinimumVersion = 170000
)

// VerifyPostgres16Source pins source discovery to the catalog shape it has
// been proven against. New PostgreSQL majors must be reviewed explicitly
// instead of silently inheriting assumptions about pg_catalog.
func VerifyPostgres16Source(
	ctx context.Context,
	database *sql.DB,
) error {
	return verifyPostgres16Source(ctx, database)
}

func verifyPostgres16Source(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
) error {
	var version int
	if err := queryer.QueryRowContext(
		ctx,
		`SELECT current_setting('server_version_num')::integer`,
	).Scan(&version); err != nil {
		return fmt.Errorf("read PostgreSQL source version: %w", err)
	}
	return validatePostgres16SourceVersion(version)
}

func validatePostgres16SourceVersion(version int) error {
	if version < postgres16MinimumVersion ||
		version >= postgres17MinimumVersion {
		return postgresSourcePolicy(
			"verify catalog version",
			fmt.Sprintf("server_version_num=%d; PostgreSQL 16 is required", version),
		)
	}
	return nil
}

type postgresSourceTableCatalog struct {
	objectID         int64
	relationKind     string
	persistence      string
	accessMethod     string
	attributeCount   int
	partition        bool
	rowSecurity      bool
	forceRowSecurity bool
	replicaIdentity  string
	relationOptions  int
	tableSpace       int64
	parents          int
	children         int
	userTriggers     int
	userRules        int
}

const postgresSourceTableCatalogQuery = `
	SELECT
		relation.oid::bigint,
		relation.relkind::text,
		relation.relpersistence::text,
		COALESCE(access_method.amname, ''),
		relation.relnatts,
		relation.relispartition,
		relation.relrowsecurity,
		relation.relforcerowsecurity,
		relation.relreplident::text,
		COALESCE(pg_catalog.cardinality(relation.reloptions), 0),
		relation.reltablespace::bigint,
		(
			SELECT COUNT(*)
			FROM pg_catalog.pg_inherits AS inheritance
			WHERE inheritance.inhrelid = relation.oid
		),
		(
			SELECT COUNT(*)
			FROM pg_catalog.pg_inherits AS inheritance
			WHERE inheritance.inhparent = relation.oid
		),
		(
			SELECT COUNT(*)
			FROM pg_catalog.pg_trigger AS trigger
			WHERE trigger.tgrelid = relation.oid
			  AND NOT trigger.tgisinternal
		),
		(
			SELECT COUNT(*)
			FROM pg_catalog.pg_rewrite AS rewrite_rule
			WHERE rewrite_rule.ev_class = relation.oid
		)
	FROM pg_catalog.pg_class AS relation
	JOIN pg_catalog.pg_namespace AS namespace
	  ON namespace.oid = relation.relnamespace
	LEFT JOIN pg_catalog.pg_am AS access_method
	  ON access_method.oid = relation.relam
	WHERE namespace.nspname = $1
	  AND relation.relname = $2
`

func readPostgresSourceTableCatalog(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	namespace string,
	name string,
) (postgresSourceTableCatalog, error) {
	var result postgresSourceTableCatalog
	err := queryer.QueryRowContext(
		ctx,
		postgresSourceTableCatalogQuery,
		namespace,
		name,
	).Scan(
		&result.objectID,
		&result.relationKind,
		&result.persistence,
		&result.accessMethod,
		&result.attributeCount,
		&result.partition,
		&result.rowSecurity,
		&result.forceRowSecurity,
		&result.replicaIdentity,
		&result.relationOptions,
		&result.tableSpace,
		&result.parents,
		&result.children,
		&result.userTriggers,
		&result.userRules,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return postgresSourceTableCatalog{}, fmt.Errorf(
			"PostgreSQL table %s.%s does not exist",
			namespace,
			name,
		)
	}
	if err != nil {
		return postgresSourceTableCatalog{}, fmt.Errorf(
			"inspect PostgreSQL table %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if err := validatePostgresSourceTableCatalog(
		namespace,
		name,
		result,
	); err != nil {
		return postgresSourceTableCatalog{}, err
	}
	return result, nil
}

func validatePostgresSourceTableCatalog(
	namespace string,
	name string,
	value postgresSourceTableCatalog,
) error {
	identity := namespace + "." + name
	switch {
	case value.objectID <= 0:
		return postgresSourcePolicy(
			"table",
			identity+" has an invalid catalog object identifier",
		)
	case value.relationKind != "r":
		return postgresSourcePolicy(
			"table",
			identity+" relation kind "+quotedCatalogValue(value.relationKind),
		)
	case value.persistence != "p":
		return postgresSourcePolicy(
			"table",
			identity+" persistence "+quotedCatalogValue(value.persistence),
		)
	case value.accessMethod != "heap":
		return postgresSourcePolicy(
			"table",
			identity+" access method "+quotedCatalogValue(value.accessMethod),
		)
	case value.attributeCount <= 0:
		return postgresSourcePolicy(
			"table",
			identity+" has no catalog columns",
		)
	case value.partition:
		return postgresSourcePolicy(
			"table",
			identity+" is a partition",
		)
	case value.parents != 0 || value.children != 0:
		return postgresSourcePolicy(
			"table inheritance",
			identity,
		)
	case value.rowSecurity || value.forceRowSecurity:
		return postgresSourcePolicy(
			"row security",
			identity,
		)
	case value.replicaIdentity != "d":
		return postgresSourcePolicy(
			"replica identity",
			identity+" catalog value "+quotedCatalogValue(value.replicaIdentity),
		)
	case value.relationOptions != 0:
		return postgresSourcePolicy(
			"table storage options",
			identity,
		)
	case value.tableSpace != 0:
		return postgresSourcePolicy(
			"table tablespace",
			identity,
		)
	case value.userTriggers != 0:
		return postgresSourcePolicy(
			"user triggers",
			fmt.Sprintf("%s count=%d", identity, value.userTriggers),
		)
	case value.userRules != 0:
		return postgresSourcePolicy(
			"rewrite rules",
			fmt.Sprintf("%s count=%d", identity, value.userRules),
		)
	default:
		return nil
	}
}

type postgresSourceColumnCatalog struct {
	position         int
	name             string
	typeNamespace    string
	typeName         string
	typeKind         string
	typeElement      int64
	typeModifier     int32
	formattedType    string
	notNull          bool
	identity         string
	generated        string
	inheritanceCount int
	local            bool
	defaultCollation bool
	defaultSQL       sql.NullString
}

const postgresSourceColumnsQuery = `
	SELECT
		attribute.attnum,
		attribute.attname,
		type_namespace.nspname,
		column_type.typname,
		column_type.typtype::text,
		column_type.typelem::bigint,
		attribute.atttypmod,
		pg_catalog.format_type(
			attribute.atttypid,
			attribute.atttypmod
		),
		attribute.attnotnull,
		attribute.attidentity::text,
		attribute.attgenerated::text,
		attribute.attinhcount,
		attribute.attislocal,
		attribute.attcollation = column_type.typcollation,
		pg_catalog.pg_get_expr(
			default_expression.adbin,
			default_expression.adrelid,
			true
		)
	FROM pg_catalog.pg_attribute AS attribute
	JOIN pg_catalog.pg_type AS column_type
	  ON column_type.oid = attribute.atttypid
	JOIN pg_catalog.pg_namespace AS type_namespace
	  ON type_namespace.oid = column_type.typnamespace
	LEFT JOIN pg_catalog.pg_attrdef AS default_expression
	  ON default_expression.adrelid = attribute.attrelid
	 AND default_expression.adnum = attribute.attnum
	WHERE attribute.attrelid = $1
	  AND attribute.attnum > 0
	  AND NOT attribute.attisdropped
	ORDER BY attribute.attnum
`

func readPostgresSourceColumns(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	table postgresSourceTableCatalog,
	namespace string,
	name string,
) ([]schema.Column, []string, error) {
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourceColumnsQuery,
		table.objectID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"list PostgreSQL columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	defer rows.Close()

	columns := make([]schema.Column, 0, table.attributeCount)
	identities := make([]string, 0, 1)
	for rows.Next() {
		var catalog postgresSourceColumnCatalog
		if err := rows.Scan(
			&catalog.position,
			&catalog.name,
			&catalog.typeNamespace,
			&catalog.typeName,
			&catalog.typeKind,
			&catalog.typeElement,
			&catalog.typeModifier,
			&catalog.formattedType,
			&catalog.notNull,
			&catalog.identity,
			&catalog.generated,
			&catalog.inheritanceCount,
			&catalog.local,
			&catalog.defaultCollation,
			&catalog.defaultSQL,
		); err != nil {
			return nil, nil, fmt.Errorf(
				"read PostgreSQL column for %s.%s: %w",
				namespace,
				name,
				err,
			)
		}
		if catalog.position != len(columns)+1 {
			return nil, nil, postgresSourcePolicy(
				"column order",
				fmt.Sprintf(
					"%s.%s catalog position=%d expected=%d",
					namespace,
					name,
					catalog.position,
					len(columns)+1,
				),
			)
		}
		column, err := postgresSourceColumnFromCatalog(catalog)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"discover PostgreSQL column %s.%s.%s: %w",
				namespace,
				name,
				catalog.name,
				err,
			)
		}
		if catalog.defaultSQL.Valid {
			defaultSQL := catalog.defaultSQL.String
			column.Default, err = schema.ParsePostgresCatalogDefault(
				column,
				&defaultSQL,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"discover PostgreSQL default for %s.%s.%s: %w",
					namespace,
					name,
					catalog.name,
					err,
				)
			}
		}
		if catalog.identity != "" {
			identities = append(identities, catalog.name)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf(
			"iterate PostgreSQL columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if len(columns) != table.attributeCount {
		return nil, nil, postgresSourcePolicy(
			"dropped columns",
			fmt.Sprintf(
				"%s.%s catalog columns=%d visible columns=%d",
				namespace,
				name,
				table.attributeCount,
				len(columns),
			),
		)
	}
	if len(identities) > 1 {
		return nil, nil, postgresSourcePolicy(
			"identity",
			fmt.Sprintf("%s.%s has %d identity columns", namespace, name, len(identities)),
		)
	}
	return columns, identities, nil
}

func postgresSourceColumnFromCatalog(
	catalog postgresSourceColumnCatalog,
) (schema.Column, error) {
	if catalog.name == "" ||
		catalog.typeNamespace != "pg_catalog" ||
		catalog.typeKind != "b" ||
		catalog.typeElement != 0 ||
		catalog.inheritanceCount != 0 ||
		!catalog.local ||
		!catalog.defaultCollation ||
		catalog.generated != "" {
		return schema.Column{}, postgresSourcePolicy(
			"column catalog shape",
			catalog.name+" "+catalog.formattedType,
		)
	}
	if catalog.identity != "" && catalog.identity != "d" {
		return schema.Column{}, postgresSourcePolicy(
			"identity generation",
			catalog.name+" catalog value "+quotedCatalogValue(catalog.identity),
		)
	}

	column := schema.Column{
		Name:     catalog.name,
		Nullable: !catalog.notNull,
	}
	switch catalog.typeName {
	case "int4":
		if catalog.typeModifier != -1 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = "integer"
	case "int8":
		if catalog.typeModifier != -1 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = "bigint"
	case "float8":
		if catalog.typeModifier != -1 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = "double precision"
	case "float4":
		if catalog.typeModifier != -1 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = "real"
		column.DeclaredType = &schema.DeclaredType{Base: "real"}
	case "text", "uuid", "bytea", "json", "jsonb", "bool", "date":
		if catalog.typeModifier != -1 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = mapPostgresSourceScalarType(catalog.typeName)
	case "bpchar", "varchar":
		length := int(catalog.typeModifier) - 4
		if catalog.typeModifier < 5 ||
			length > 10_485_760 {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = mapPostgresSourceScalarType(catalog.typeName)
		column.DeclaredType = &schema.DeclaredType{
			Base:      column.Type,
			Arguments: []int{length},
		}
	case "numeric":
		precision, scale, ok := postgresSourceNumericModifiers(
			catalog.typeModifier,
		)
		if !ok {
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
		column.Type = "numeric"
		column.DeclaredType = &schema.DeclaredType{
			Base:      "numeric",
			Arguments: []int{precision, scale},
		}
	case "time", "timestamp", "timestamptz":
		column.Type = catalog.typeName
		switch {
		case catalog.typeModifier == -1:
		case catalog.typeModifier >= 0 && catalog.typeModifier <= 6:
			column.DeclaredType = &schema.DeclaredType{
				Base:      catalog.typeName,
				Arguments: []int{int(catalog.typeModifier)},
			}
		default:
			return schema.Column{}, unsupportedPostgresSourceType(catalog)
		}
	default:
		return schema.Column{}, unsupportedPostgresSourceType(catalog)
	}
	return column, nil
}

func mapPostgresSourceScalarType(value string) string {
	switch value {
	case "bpchar":
		return "char"
	case "varchar":
		return "varchar"
	case "bool":
		return "boolean"
	default:
		return value
	}
}

func postgresSourceNumericModifiers(
	typeModifier int32,
) (precision int, scale int, ok bool) {
	if typeModifier < 4 {
		return 0, 0, false
	}
	modifier := int(typeModifier - 4)
	precision = (modifier >> 16) & 0xffff
	scale = modifier & 0x7ff
	if scale >= 1024 {
		scale -= 2048
	}
	if precision < 1 || precision > 1000 ||
		scale < -1000 || scale > 1000 {
		return 0, 0, false
	}
	return precision, scale, true
}

func unsupportedPostgresSourceType(
	catalog postgresSourceColumnCatalog,
) error {
	return postgresSourcePolicy(
		"type modifier",
		catalog.name+" "+catalog.formattedType,
	)
}

func inspectPostgres16Table(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	namespace string,
	name string,
) (schema.Table, error) {
	if err := verifyPostgres16Source(ctx, queryer); err != nil {
		return schema.Table{}, err
	}
	tableCatalog, err := readPostgresSourceTableCatalog(
		ctx,
		queryer,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	columns, identities, err := readPostgresSourceColumns(
		ctx,
		queryer,
		tableCatalog,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table := schema.Table{
		Schema:  namespace,
		Name:    name,
		Columns: columns,
	}
	if err := discoverPostgresSourcePrimaryKey(
		ctx,
		queryer,
		tableCatalog.objectID,
		&table,
	); err != nil {
		return schema.Table{}, err
	}
	if len(identities) == 1 {
		identity, err := discoverPostgresSourceIdentity(
			ctx,
			queryer,
			table,
			identities[0],
		)
		if err != nil {
			return schema.Table{}, err
		}
		table.Identity = identity
	}
	table.Indexes, err = discoverPostgresSourceIndexes(
		ctx,
		queryer,
		tableCatalog.objectID,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.Checks, err = discoverPostgresSourceChecks(
		ctx,
		queryer,
		tableCatalog.objectID,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.ForeignKeys, err = discoverPostgresSourceForeignKeys(
		ctx,
		queryer,
		tableCatalog.objectID,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	return table, nil
}

func postgresSourcePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: "discover PostgreSQL source " + operation,
		Type:      value,
		Target:    string(schema.Postgres),
	}
}

func quotedCatalogValue(value string) string {
	return fmt.Sprintf("%q", value)
}

func validatePostgresSourceIdentityColumn(
	table schema.Table,
	columnName string,
) error {
	keys := make([]schema.Column, 0, len(table.Columns))
	var identity *schema.Column
	for index := range table.Columns {
		column := &table.Columns[index]
		if column.PrimaryKey || column.PrimaryKeyPosition > 0 {
			keys = append(keys, *column)
		}
		if column.Name == columnName {
			identity = column
		}
	}
	if identity == nil ||
		identity.Type != "bigint" ||
		identity.Default != nil ||
		len(keys) != 1 ||
		keys[0].Name != columnName ||
		keys[0].PrimaryKeyPosition != 1 {
		return postgresSourcePolicy(
			"identity column",
			table.Schema+"."+table.Name+"."+columnName,
		)
	}
	return nil
}

type postgresSourceIdentitySequence struct {
	objectID    int64
	namespace   string
	name        string
	persistence string
	dataType    string
	start       int64
	increment   int64
	minimum     int64
	maximum     int64
	cache       int64
	cycle       bool
	lastValue   sql.NullInt64
	canRead     bool
}

const postgresSourceIdentitySequenceQuery = `
	SELECT
		sequence_relation.oid::bigint,
		sequence_namespace.nspname::text,
		sequence_relation.relname::text,
		sequence_relation.relpersistence::text,
		pg_catalog.format_type(sequence.seqtypid, NULL),
		sequence.seqstart,
		sequence.seqincrement,
		sequence.seqmin,
		sequence.seqmax,
		sequence.seqcache,
		sequence.seqcycle,
		sequence_view.last_value,
		pg_catalog.has_sequence_privilege(
			current_user,
			sequence_relation.oid,
			'SELECT'
		)
	FROM pg_catalog.pg_class AS table_relation
	JOIN pg_catalog.pg_namespace AS table_namespace
	  ON table_namespace.oid = table_relation.relnamespace
	JOIN pg_catalog.pg_attribute AS attribute
	  ON attribute.attrelid = table_relation.oid
	JOIN pg_catalog.pg_depend AS dependency
	  ON dependency.refclassid =
	     'pg_catalog.pg_class'::pg_catalog.regclass
	 AND dependency.refobjid = table_relation.oid
	 AND dependency.refobjsubid = attribute.attnum
	 AND dependency.classid =
	     'pg_catalog.pg_class'::pg_catalog.regclass
	 AND dependency.objsubid = 0
	 AND dependency.deptype = 'i'
	JOIN pg_catalog.pg_class AS sequence_relation
	  ON sequence_relation.oid = dependency.objid
	 AND sequence_relation.relkind = 'S'
	JOIN pg_catalog.pg_namespace AS sequence_namespace
	  ON sequence_namespace.oid = sequence_relation.relnamespace
	JOIN pg_catalog.pg_sequence AS sequence
	  ON sequence.seqrelid = sequence_relation.oid
	LEFT JOIN pg_catalog.pg_sequences AS sequence_view
	  ON sequence_view.schemaname = sequence_namespace.nspname
	 AND sequence_view.sequencename = sequence_relation.relname
	WHERE table_namespace.nspname = $1
	  AND table_relation.relname = $2
	  AND table_relation.relkind = 'r'
	  AND attribute.attname = $3
	  AND attribute.attidentity = 'd'
	  AND attribute.attnum > 0
	  AND NOT attribute.attisdropped
`

func discoverPostgresSourceIdentity(
	ctx context.Context,
	queryer PostgresCatalogQueryer,
	table schema.Table,
	columnName string,
) (*schema.Identity, error) {
	if err := validatePostgresSourceIdentityColumn(
		table,
		columnName,
	); err != nil {
		return nil, err
	}
	rows, err := queryer.QueryContext(
		ctx,
		postgresSourceIdentitySequenceQuery,
		table.Schema,
		table.Name,
		columnName,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"inspect PostgreSQL identity sequence for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	defer rows.Close()
	var states []postgresSourceIdentitySequence
	for rows.Next() {
		var state postgresSourceIdentitySequence
		if err := rows.Scan(
			&state.objectID,
			&state.namespace,
			&state.name,
			&state.persistence,
			&state.dataType,
			&state.start,
			&state.increment,
			&state.minimum,
			&state.maximum,
			&state.cache,
			&state.cycle,
			&state.lastValue,
			&state.canRead,
		); err != nil {
			return nil, fmt.Errorf(
				"read PostgreSQL identity sequence for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate PostgreSQL identity sequence for %s.%s: %w",
			table.Schema,
			table.Name,
			err,
		)
	}
	if len(states) != 1 {
		return nil, postgresSourcePolicy(
			"identity sequence",
			fmt.Sprintf(
				"%s.%s.%s catalog rows=%d",
				table.Schema,
				table.Name,
				columnName,
				len(states),
			),
		)
	}
	state := states[0]
	if state.objectID <= 0 ||
		state.namespace != table.Schema ||
		state.name == "" ||
		state.persistence != "p" ||
		state.dataType != "bigint" ||
		state.start != 1 ||
		state.increment != 1 ||
		state.minimum != 1 ||
		state.maximum != math.MaxInt64 ||
		state.cache != 1 ||
		state.cycle ||
		!state.canRead ||
		state.lastValue.Valid &&
			(state.lastValue.Int64 < 1 ||
				state.lastValue.Int64 > math.MaxInt64) {
		return nil, postgresSourcePolicy(
			"identity sequence",
			table.Schema+"."+table.Name+"."+columnName,
		)
	}
	var frontier *int64
	if state.lastValue.Valid {
		value := state.lastValue.Int64
		frontier = &value
	}
	return &schema.Identity{
		Column:     columnName,
		Generation: schema.IdentityByDefault,
		Frontier:   frontier,
	}, nil
}

func findPostgresSourceColumn(
	table schema.Table,
	name string,
) (schema.Column, bool) {
	for _, column := range table.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return schema.Column{}, false
}

func postgresSourceColumnIsText(column schema.Column) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	switch base {
	case "char", "varchar", "text":
		return true
	default:
		return false
	}
}
