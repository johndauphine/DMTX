package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

const postgresRelationNameMaximumBytes = 63

type adapterTargetPlanPreflighter interface {
	PreflightTargetPlan(
		context.Context,
		[]schema.Table,
		string,
	) error
}

func preflightAdapterTargetPlan(
	ctx context.Context,
	target targetAdapter,
	tables []schema.Table,
	mode string,
) error {
	preflighter, ok := target.(adapterTargetPlanPreflighter)
	if !ok {
		return nil
	}
	return preflighter.PreflightTargetPlan(ctx, tables, mode)
}

type postgresPlannedRelationName struct {
	namespace string
	name      string
	kind      string
	table     string
}

type postgresExistingRelationName struct {
	objectID               int64
	namespace              string
	name                   string
	relationKind           string
	indexOwnerNamespace    string
	indexOwnerTable        string
	sequenceOwnerNamespace string
	sequenceOwnerTable     string
}

func (adapter *postgresTargetAdapter) PreflightTargetPlan(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	if mode != "drop_recreate" {
		return nil
	}
	return preflightPostgresDropRecreateRelationNames(
		ctx,
		adapter.database,
		tables,
	)
}

func preflightPostgresDropRecreateRelationNames(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	planned, err := planPostgresDropRecreateRelationNames(tables)
	if err != nil {
		return fmt.Errorf(
			"plan PostgreSQL drop/recreate relation names: %w",
			err,
		)
	}
	selected := postgresSelectedTargetTableNames(tables)
	var existing []postgresExistingRelationName
	for _, namespace := range postgresPlannedRelationNamespaces(planned) {
		relations, err := readPostgresNamespaceRelations(
			ctx,
			database,
			namespace,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect PostgreSQL relation names in namespace %s: %w",
				namespace,
				err,
			)
		}
		existing = append(existing, relations...)
	}
	return validatePostgresDropRecreateRelationNames(
		planned,
		existing,
		selected,
	)
}

func planPostgresDropRecreateRelationNames(
	tables []schema.Table,
) ([]postgresPlannedRelationName, error) {
	objectPlan, err := schema.PlanPostgresDropRecreateObjects(
		tables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		return nil, err
	}

	planned := make([]postgresPlannedRelationName, 0, len(tables)*3)
	byName := make(map[string]postgresPlannedRelationName)
	add := func(relation postgresPlannedRelationName) error {
		key := postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)
		if existing, found := byName[key]; found {
			return fmt.Errorf(
				"planned PostgreSQL %s for table %s and %s for table %s share relation name %s.%s",
				existing.kind,
				existing.table,
				relation.kind,
				relation.table,
				relation.namespace,
				relation.name,
			)
		}
		byName[key] = relation
		planned = append(planned, relation)
		return nil
	}

	for _, table := range tables {
		if err := add(postgresPlannedRelationName{
			namespace: table.Schema,
			name:      table.Name,
			kind:      "table",
			table:     table.Name,
		}); err != nil {
			return nil, err
		}
		if postgresTableHasPrimaryKey(table) {
			if err := add(postgresPlannedRelationName{
				namespace: table.Schema,
				name: postgresAutomaticRelationName(
					table.Name,
					"",
					"pkey",
				),
				kind:  "primary-key index",
				table: table.Name,
			}); err != nil {
				return nil, err
			}
		}
		if table.Identity != nil {
			if err := add(postgresPlannedRelationName{
				namespace: table.Schema,
				name: postgresAutomaticRelationName(
					table.Name,
					table.Identity.Column,
					"seq",
				),
				kind:  "identity sequence",
				table: table.Name,
			}); err != nil {
				return nil, err
			}
		}
	}
	for _, statement := range objectPlan {
		if statement.Kind != schema.PostgresIndexObject {
			continue
		}
		if err := add(postgresPlannedRelationName{
			namespace: statement.Schema,
			name:      statement.Name,
			kind:      "post-load index",
			table:     statement.Table,
		}); err != nil {
			return nil, err
		}
	}
	sort.Slice(planned, func(left, right int) bool {
		leftKey := postgresRelationNameKey(
			planned[left].namespace,
			planned[left].name,
		)
		rightKey := postgresRelationNameKey(
			planned[right].namespace,
			planned[right].name,
		)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return planned[left].kind < planned[right].kind
	})
	return planned, nil
}

func postgresTableHasPrimaryKey(table schema.Table) bool {
	for _, column := range table.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition > 0 {
			return true
		}
	}
	return false
}

// postgresAutomaticRelationName mirrors PostgreSQL's makeObjectName behavior
// for the unnamed primary-key indexes and identity sequences emitted by
// schema.CreateTable. Separators and the suffix remain intact while the two
// identifier components are clipped to PostgreSQL's 63-byte limit.
func postgresAutomaticRelationName(
	first string,
	second string,
	label string,
) string {
	firstBytes := len(first)
	secondBytes := len(second)
	overhead := len(label) + 1
	if second != "" {
		overhead++
	}
	available := postgresRelationNameMaximumBytes - overhead
	for firstBytes+secondBytes > available {
		if firstBytes > secondBytes {
			firstBytes--
		} else {
			secondBytes--
		}
	}
	first = truncatePostgresRelationComponent(first, firstBytes)
	second = truncatePostgresRelationComponent(second, secondBytes)
	if second == "" {
		return first + "_" + label
	}
	return first + "_" + second + "_" + label
}

func truncatePostgresRelationComponent(
	value string,
	maximumBytes int,
) string {
	if len(value) <= maximumBytes {
		return value
	}
	end := maximumBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func postgresPlannedRelationNamespaces(
	planned []postgresPlannedRelationName,
) []string {
	seen := make(map[string]struct{})
	for _, relation := range planned {
		seen[relation.namespace] = struct{}{}
	}
	namespaces := make([]string, 0, len(seen))
	for namespace := range seen {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	return namespaces
}

func postgresSelectedTargetTableNames(
	tables []schema.Table,
) map[string]struct{} {
	selected := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		selected[postgresRelationNameKey(
			table.Schema,
			table.Name,
		)] = struct{}{}
	}
	return selected
}

func postgresRelationNameKey(namespace, name string) string {
	return namespace + "\x00" + name
}

const postgresDropRecreateRelationCatalogQuery = `
	SELECT
		relation.oid::bigint,
		namespace.nspname,
		relation.relname,
		relation.relkind::text,
		COALESCE(index_owner_namespace.nspname, ''),
		COALESCE(index_owner.relname, ''),
		COALESCE(sequence_owner.namespace_name, ''),
		COALESCE(sequence_owner.relation_name, '')
	FROM pg_catalog.pg_class AS relation
	JOIN pg_catalog.pg_namespace AS namespace
	  ON namespace.oid = relation.relnamespace
	LEFT JOIN pg_catalog.pg_index AS index_metadata
	  ON index_metadata.indexrelid = relation.oid
	LEFT JOIN pg_catalog.pg_class AS index_owner
	  ON index_owner.oid = index_metadata.indrelid
	LEFT JOIN pg_catalog.pg_namespace AS index_owner_namespace
	  ON index_owner_namespace.oid = index_owner.relnamespace
	LEFT JOIN LATERAL (
		SELECT
			owner_namespace.nspname AS namespace_name,
			owner_relation.relname AS relation_name
		FROM pg_catalog.pg_depend AS dependency
		JOIN pg_catalog.pg_class AS owner_relation
		  ON owner_relation.oid = dependency.refobjid
		JOIN pg_catalog.pg_namespace AS owner_namespace
		  ON owner_namespace.oid = owner_relation.relnamespace
		WHERE relation.relkind = 'S'
		  AND dependency.classid =
		      'pg_catalog.pg_class'::pg_catalog.regclass
		  AND dependency.objid = relation.oid
		  AND dependency.objsubid = 0
		  AND dependency.refclassid =
		      'pg_catalog.pg_class'::pg_catalog.regclass
		  AND dependency.refobjsubid > 0
		  AND dependency.deptype IN ('a', 'i')
		ORDER BY
			owner_namespace.nspname,
			owner_relation.relname,
			dependency.refobjsubid
		LIMIT 1
	) AS sequence_owner ON true
	WHERE namespace.nspname = $1
	ORDER BY relation.relname, relation.oid
`

func readPostgresNamespaceRelations(
	ctx context.Context,
	database *sql.DB,
	namespace string,
) ([]postgresExistingRelationName, error) {
	rows, err := database.QueryContext(
		ctx,
		postgresDropRecreateRelationCatalogQuery,
		namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var relations []postgresExistingRelationName
	for rows.Next() {
		var relation postgresExistingRelationName
		if err := rows.Scan(
			&relation.objectID,
			&relation.namespace,
			&relation.name,
			&relation.relationKind,
			&relation.indexOwnerNamespace,
			&relation.indexOwnerTable,
			&relation.sequenceOwnerNamespace,
			&relation.sequenceOwnerTable,
		); err != nil {
			return nil, err
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return relations, nil
}

func validatePostgresDropRecreateRelationNames(
	planned []postgresPlannedRelationName,
	existing []postgresExistingRelationName,
	selected map[string]struct{},
) error {
	plannedByName := make(
		map[string]postgresPlannedRelationName,
		len(planned),
	)
	for _, relation := range planned {
		plannedByName[postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)] = relation
	}
	for _, relation := range existing {
		key := postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)
		plannedRelation, collision := plannedByName[key]
		if !collision ||
			postgresRelationDropsWithSelectedTable(
				relation,
				selected,
			) {
			continue
		}
		return fmt.Errorf(
			"planned PostgreSQL %s %s.%s for table %s collides with existing %s relation outside selected target tables",
			plannedRelation.kind,
			plannedRelation.namespace,
			plannedRelation.name,
			plannedRelation.table,
			describePostgresRelationKind(relation.relationKind),
		)
	}
	return nil
}

func postgresRelationDropsWithSelectedTable(
	relation postgresExistingRelationName,
	selected map[string]struct{},
) bool {
	if relation.relationKind == "r" ||
		relation.relationKind == "p" {
		if _, ok := selected[postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)]; ok {
			return true
		}
	}
	if _, ok := selected[postgresRelationNameKey(
		relation.indexOwnerNamespace,
		relation.indexOwnerTable,
	)]; ok && relation.indexOwnerTable != "" {
		return true
	}
	if _, ok := selected[postgresRelationNameKey(
		relation.sequenceOwnerNamespace,
		relation.sequenceOwnerTable,
	)]; ok && relation.sequenceOwnerTable != "" {
		return true
	}
	return false
}

func describePostgresRelationKind(kind string) string {
	switch kind {
	case "r":
		return "table"
	case "p":
		return "partitioned-table"
	case "i":
		return "index"
	case "I":
		return "partitioned-index"
	case "S":
		return "sequence"
	case "v":
		return "view"
	case "m":
		return "materialized-view"
	case "f":
		return "foreign-table"
	default:
		return fmt.Sprintf("catalog-kind-%q", kind)
	}
}
