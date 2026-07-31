package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type sqlServerCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (adapter *sqlServerTargetAdapter) PreflightTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	for _, table := range targetTables {
		if table.Schema != adapter.namespace {
			return fmt.Errorf(
				"preflight SQL Server table %s: planned schema %q does not match target schema %q",
				table.Name,
				table.Schema,
				adapter.namespace,
			)
		}
	}
	if err := preflightSQLServerTargetPermissions(
		ctx,
		adapter.database,
		adapter.namespace,
		mode,
		sqlServerTargetPlanHasIdentity(targetTables),
	); err != nil {
		return err
	}
	if err := preflightSQLServerLiveIdentifierContract(
		ctx,
		adapter.database,
		targetTables,
	); err != nil {
		return err
	}
	selected, namespace, err := sqlServerSelectedTargetTables(targetTables)
	if err != nil {
		return err
	}
	if err := preflightSQLServerRelationKinds(
		ctx,
		adapter.database,
		namespace,
		selected,
	); err != nil {
		return err
	}
	if err := preflightSQLServerTargetObjectPermissions(
		ctx,
		adapter.database,
		targetTables,
		mode,
	); err != nil {
		return err
	}
	if mode == "drop_recreate" {
		return preflightSQLServerDropRecreate(
			ctx,
			adapter.database,
			targetTables,
		)
	}

	if err := preflightSQLServerUpsertForeignKeyKeys(
		targetTables,
	); err != nil {
		return err
	}
	if err := preflightSQLServerExternalForeignKeys(
		ctx,
		adapter.database,
		namespace,
		selected,
	); err != nil {
		return err
	}
	for _, planned := range targetTables {
		exists, err := sqlServerTargetBaseTableExists(
			ctx,
			adapter.database,
			planned,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight SQL Server table %s: %w",
				planned.Name,
				err,
			)
		}
		if !exists {
			return fmt.Errorf(
				"preflight SQL Server table %s: upsert requires an existing target table",
				planned.Name,
			)
		}
		actual, err := engine.InspectSQLServerTargetTableWithQueryer(
			ctx,
			adapter.database,
			planned.Schema,
			planned.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight SQL Server table %s retained shape: %w",
				planned.Name,
				err,
			)
		}
		if err := validateSQLServerRetainedTableShape(
			planned,
			actual,
		); err != nil {
			return fmt.Errorf(
				"preflight SQL Server table %s: retained target shape differs from the plan: %w",
				planned.Name,
				err,
			)
		}
	}
	return nil
}

func preflightSQLServerUpsertForeignKeyKeys(
	tables []schema.Table,
) error {
	byName := make(map[string]schema.Table, len(tables))
	for _, table := range tables {
		byName[sqlServerPreflightTableKey(
			table.Schema,
			table.Name,
		)] = table
	}
	for _, table := range tables {
		for _, foreignKey := range table.ForeignKeys {
			referencedSchema := foreignKey.ReferencedSchema
			if referencedSchema == "" {
				referencedSchema = table.Schema
			}
			referenced, exists := byName[sqlServerPreflightTableKey(
				referencedSchema,
				foreignKey.ReferencedTable,
			)]
			if !exists {
				return fmt.Errorf(
					"preflight SQL Server table %s: foreign key %s references an unselected table",
					table.Name,
					foreignKey.Name,
				)
			}
			primaryKey := primaryKeyColumns(referenced)
			if !reflect.DeepEqual(
				foreignKey.ReferencedColumns,
				primaryKey,
			) {
				return fmt.Errorf(
					"preflight SQL Server upsert: foreign key %s references a mutable non-primary unique key",
					foreignKey.Name,
				)
			}
		}
	}
	return nil
}

func sqlServerPreflightTableKey(namespace, table string) string {
	return strings.ToLower(namespace) + "\x00" + strings.ToLower(table)
}

func preflightSQLServerTargetPermissions(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	mode string,
	requiresIdentityAuthority bool,
) error {
	var permissions sqlServerTargetPermissionCatalog
	if err := queryer.QueryRowContext(
		ctx,
		`SELECT
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				DB_NAME(), 'DATABASE', 'VIEW DEFINITION'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				@p1, 'SCHEMA', 'CONTROL'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				DB_NAME(), 'DATABASE', 'CREATE TABLE'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				@p1, 'SCHEMA', 'ALTER'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				@p1, 'SCHEMA', 'SELECT'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				@p1, 'SCHEMA', 'INSERT'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				@p1, 'SCHEMA', 'DELETE'
			), 0)),
			CONVERT(bit, CASE
				WHEN IS_SRVROLEMEMBER('sysadmin') = 1
				  OR IS_MEMBER('db_owner') = 1
				  OR IS_MEMBER('db_ddladmin') = 1
				  OR EXISTS (
					SELECT 1
					  FROM sys.schemas AS identity_schema
					 WHERE identity_schema.name = @p1
					   AND (
						identity_schema.principal_id =
							DATABASE_PRINCIPAL_ID()
						OR IS_ROLEMEMBER(USER_NAME(
							identity_schema.principal_id
						)) = 1
					   )
				  )
				THEN 1 ELSE 0 END)`,
		namespace,
	).Scan(
		&permissions.viewDefinition,
		&permissions.schemaControl,
		&permissions.createTable,
		&permissions.schemaAlter,
		&permissions.schemaSelect,
		&permissions.schemaInsert,
		&permissions.schemaDelete,
		&permissions.identityAuthority,
	); err != nil {
		return fmt.Errorf(
			"inspect SQL Server target permissions: %w",
			err,
		)
	}
	return validateSQLServerTargetPermissions(
		permissions,
		mode,
		requiresIdentityAuthority,
	)
}

type sqlServerTargetPermissionCatalog struct {
	viewDefinition    bool
	schemaControl     bool
	createTable       bool
	schemaAlter       bool
	schemaSelect      bool
	schemaInsert      bool
	schemaDelete      bool
	identityAuthority bool
}

func sqlServerTargetPlanHasIdentity(tables []schema.Table) bool {
	for _, table := range tables {
		if table.Identity != nil {
			return true
		}
	}
	return false
}

func validateSQLServerTargetPermissions(
	permissions sqlServerTargetPermissionCatalog,
	mode string,
	requiresIdentityAuthority bool,
) error {
	if !permissions.viewDefinition || !permissions.schemaControl {
		return fmt.Errorf(
			"preflight SQL Server target: database VIEW DEFINITION and schema CONTROL permissions are required",
		)
	}
	if mode == "drop_recreate" &&
		(!permissions.createTable ||
			!permissions.schemaAlter ||
			!permissions.schemaSelect ||
			!permissions.schemaInsert) {
		return fmt.Errorf(
			"preflight SQL Server target: CREATE TABLE plus effective schema ALTER, SELECT, and INSERT permissions are required for drop/recreate",
		)
	}
	if mode == "drop_recreate" &&
		requiresIdentityAuthority &&
		!permissions.schemaDelete {
		return fmt.Errorf(
			"preflight SQL Server target: effective schema DELETE permission is required for identity frontier preservation",
		)
	}
	if requiresIdentityAuthority && !permissions.identityAuthority {
		return fmt.Errorf(
			"preflight SQL Server target: target-schema ownership or sysadmin, db_owner, or db_ddladmin membership is required to preserve identity frontiers",
		)
	}
	return nil
}

type sqlServerTargetObjectPermissionCatalog struct {
	selectRows bool
	insertRows bool
	updateRows bool
	alterTable bool
}

func preflightSQLServerTargetObjectPermissions(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	tables []schema.Table,
	mode string,
) error {
	for _, table := range tables {
		exists, err := sqlServerTargetBaseTableExists(
			ctx,
			queryer,
			table,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQL Server object permissions for table %s: %w",
				table.Name,
				err,
			)
		}
		if !exists {
			continue
		}
		var permissions sqlServerTargetObjectPermissionCatalog
		if err := queryer.QueryRowContext(
			ctx,
			`SELECT
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					QUOTENAME(@p1) + '.' + QUOTENAME(@p2),
					'OBJECT', 'SELECT'
				), 0)),
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					QUOTENAME(@p1) + '.' + QUOTENAME(@p2),
					'OBJECT', 'INSERT'
				), 0)),
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					QUOTENAME(@p1) + '.' + QUOTENAME(@p2),
					'OBJECT', 'UPDATE'
				), 0)),
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					QUOTENAME(@p1) + '.' + QUOTENAME(@p2),
					'OBJECT', 'ALTER'
				), 0))`,
			table.Schema,
			table.Name,
		).Scan(
			&permissions.selectRows,
			&permissions.insertRows,
			&permissions.updateRows,
			&permissions.alterTable,
		); err != nil {
			return fmt.Errorf(
				"inspect SQL Server effective object permissions for table %s: %w",
				table.Name,
				err,
			)
		}
		if err := validateSQLServerTargetObjectPermissions(
			table,
			mode,
			permissions,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLServerTargetObjectPermissions(
	table schema.Table,
	mode string,
	permissions sqlServerTargetObjectPermissionCatalog,
) error {
	missing := make([]string, 0, 4)
	if mode == "drop_recreate" {
		if !permissions.alterTable {
			missing = append(missing, "ALTER")
		}
	} else {
		if !permissions.selectRows {
			missing = append(missing, "SELECT")
		}
		if !permissions.insertRows {
			missing = append(missing, "INSERT")
		}
		requiresUpdate := false
		for _, column := range table.Columns {
			if !column.PrimaryKey &&
				column.PrimaryKeyPosition == 0 {
				requiresUpdate = true
				break
			}
		}
		if requiresUpdate && !permissions.updateRows {
			missing = append(missing, "UPDATE")
		}
		if table.Identity != nil && !permissions.alterTable {
			missing = append(missing, "ALTER")
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"preflight SQL Server table %s: effective object %s permission is required for %s",
		table.Name,
		strings.Join(missing, ", "),
		mode,
	)
}

type sqlServerPlannedIdentifier struct {
	name  string
	kind  string
	table string
}

func preflightSQLServerLiveIdentifierContract(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	tables []schema.Table,
) error {
	if len(tables) == 0 {
		return nil
	}
	namespace := tables[0].Schema
	schemaNames := make([]sqlServerPlannedIdentifier, 0, len(tables))
	for _, table := range tables {
		schemaNames = append(schemaNames, sqlServerPlannedIdentifier{
			name:  table.Name,
			kind:  "table",
			table: table.Name,
		})
		primaryKey, err := schema.SQLServerPrimaryKeyConstraintName(table)
		if err != nil {
			return fmt.Errorf(
				"preflight SQL Server primary-key name for %s: %w",
				table.Name,
				err,
			)
		}
		if primaryKey != "" {
			schemaNames = append(
				schemaNames,
				sqlServerPlannedIdentifier{
					name:  primaryKey,
					kind:  "primary key",
					table: table.Name,
				},
			)
		}
		columns := make([]sqlServerPlannedIdentifier, len(table.Columns))
		for index, column := range table.Columns {
			columns[index] = sqlServerPlannedIdentifier{
				name:  column.Name,
				kind:  "column",
				table: table.Name,
			}
		}
		if err := preflightSQLServerIdentifierGroup(
			ctx,
			queryer,
			columns,
		); err != nil {
			return err
		}
	}
	objects, err := schema.PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		return fmt.Errorf(
			"preflight SQL Server target object names: %w",
			err,
		)
	}
	indexes := make(map[string][]sqlServerPlannedIdentifier)
	for _, object := range objects {
		identifier := sqlServerPlannedIdentifier{
			name:  object.Name,
			table: object.Table,
		}
		switch object.Kind {
		case schema.SQLServerIndexObject:
			identifier.kind = "index"
			indexes[object.Table] = append(
				indexes[object.Table],
				identifier,
			)
		case schema.SQLServerCheckObject:
			identifier.kind = "CHECK constraint"
			schemaNames = append(schemaNames, identifier)
		case schema.SQLServerForeignKeyObject:
			identifier.kind = "foreign key"
			schemaNames = append(schemaNames, identifier)
		default:
			return fmt.Errorf(
				"preflight SQL Server target: unknown planned object kind %d",
				object.Kind,
			)
		}
	}
	for _, group := range indexes {
		if err := preflightSQLServerIdentifierGroup(
			ctx,
			queryer,
			group,
		); err != nil {
			return err
		}
	}
	if err := preflightSQLServerIdentifierGroup(
		ctx,
		queryer,
		schemaNames,
	); err != nil {
		return err
	}
	for _, planned := range schemaNames {
		var actual string
		err := queryer.QueryRowContext(
			ctx,
			`SELECT target_object.name
			   FROM sys.objects AS target_object
			   JOIN sys.schemas AS target_schema
			     ON target_schema.schema_id =
			        target_object.schema_id
			  WHERE target_schema.name = @p1
			    AND target_object.name = @p2`,
			namespace,
			planned.name,
		).Scan(&actual)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf(
				"resolve SQL Server target identifier %s: %w",
				planned.name,
				err,
			)
		}
		if actual != planned.name {
			return fmt.Errorf(
				"preflight SQL Server target: planned %s %q resolves to existing catalog spelling %q under the target collation",
				planned.kind,
				planned.name,
				actual,
			)
		}
	}
	return nil
}

func preflightSQLServerIdentifierGroup(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	identifiers []sqlServerPlannedIdentifier,
) error {
	if len(identifiers) < 2 {
		return nil
	}
	// SQL Server accepts at most 2,100 statement parameters. Keep headroom for
	// future query predicates and fail closed rather than split a collation
	// equivalence class across chunks.
	if len(identifiers) > 2_000 {
		return fmt.Errorf(
			"preflight SQL Server target: %d identifiers in one name scope exceed the certified collation-check bound",
			len(identifiers),
		)
	}
	var query strings.Builder
	query.WriteString(`SELECT TOP (1)
		MIN(planned.position),
		MAX(planned.position)
	   FROM (VALUES `)
	arguments := make([]any, len(identifiers))
	for index, identifier := range identifiers {
		if index > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(")
		query.WriteString(fmt.Sprintf("%d, @p%d", index, index+1))
		query.WriteString(")")
		arguments[index] = identifier.name
	}
	query.WriteString(`) AS planned(position, name)
	  GROUP BY planned.name COLLATE DATABASE_DEFAULT
	 HAVING COUNT(*) > 1
	  ORDER BY MIN(planned.position)`)
	var left, right int
	err := queryer.QueryRowContext(
		ctx,
		query.String(),
		arguments...,
	).Scan(&left, &right)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"compare SQL Server target identifiers: %w",
			err,
		)
	}
	if left < 0 || left >= len(identifiers) ||
		right < 0 || right >= len(identifiers) ||
		left == right {
		return fmt.Errorf(
			"compare SQL Server target identifiers: invalid collision positions",
		)
	}
	return fmt.Errorf(
		"preflight SQL Server target: planned %s %q and %s %q collide under the target database collation",
		identifiers[left].kind,
		identifiers[left].name,
		identifiers[right].kind,
		identifiers[right].name,
	)
}

func sqlServerTargetBaseTableExists(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	table schema.Table,
) (bool, error) {
	var objectType string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT RTRIM(target_object.type)
		   FROM sys.objects AS target_object
		   JOIN sys.schemas AS target_schema
		     ON target_schema.schema_id = target_object.schema_id
		  WHERE target_schema.name = @p1
		    AND target_object.name = @p2`,
		table.Schema,
		table.Name,
	).Scan(&objectType)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if objectType != "U" {
		return false, fmt.Errorf(
			"existing target object is type %s, not a user table",
			objectType,
		)
	}
	return true, nil
}

func validateSQLServerRetainedTableShape(
	planned schema.Table,
	actual schema.Table,
) error {
	planned = normalizeSQLServerRetainedTable(planned)
	actual = normalizeSQLServerRetainedTable(actual)
	plannedChecks := planned.Checks
	actualChecks := actual.Checks
	planned.Checks = nil
	actual.Checks = nil
	plannedDefaults := make([]*schema.Expression, len(planned.Columns))
	actualDefaults := make([]*schema.Expression, len(actual.Columns))
	for index := range planned.Columns {
		plannedDefaults[index] = planned.Columns[index].Default
		planned.Columns[index].Default = nil
	}
	for index := range actual.Columns {
		actualDefaults[index] = actual.Columns[index].Default
		actual.Columns[index].Default = nil
	}
	if !reflect.DeepEqual(planned, actual) {
		return fmt.Errorf("catalog metadata differs")
	}
	for index := range planned.Columns {
		plannedDefault := plannedDefaults[index]
		actualDefault := actualDefaults[index]
		if (plannedDefault == nil) != (actualDefault == nil) {
			return fmt.Errorf("column default metadata differs")
		}
		if plannedDefault == nil {
			continue
		}
		plannedColumn := planned.Columns[index]
		plannedColumn.Default = plannedDefault
		actualColumn := actual.Columns[index]
		actualColumn.Default = actualDefault
		plannedSQL, err := schema.RenderSQLServerDefault(plannedColumn)
		if err != nil {
			return fmt.Errorf(
				"render planned default for %s: %w",
				plannedColumn.Name,
				err,
			)
		}
		actualSQL, err := schema.RenderSQLServerDefault(actualColumn)
		if err != nil {
			return fmt.Errorf(
				"render retained default for %s: %w",
				actualColumn.Name,
				err,
			)
		}
		if plannedSQL != actualSQL {
			return fmt.Errorf("column default metadata differs")
		}
	}
	if len(plannedChecks) != len(actualChecks) {
		return fmt.Errorf("CHECK constraint metadata differs")
	}
	for index := range plannedChecks {
		if plannedChecks[index].Name != actualChecks[index].Name {
			return fmt.Errorf("CHECK constraint metadata differs")
		}
		plannedExpression, err := schema.RenderPortableCheckForSQLServer(
			plannedChecks[index].Expression,
			planned.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"render planned CHECK %s: %w",
				plannedChecks[index].Name,
				err,
			)
		}
		actualExpression, err := schema.RenderPortableCheckForSQLServer(
			actualChecks[index].Expression,
			actual.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"render retained CHECK %s: %w",
				actualChecks[index].Name,
				err,
			)
		}
		if plannedExpression != actualExpression {
			return fmt.Errorf("CHECK constraint metadata differs")
		}
	}
	return nil
}

func normalizeSQLServerRetainedTable(table schema.Table) schema.Table {
	table = cloneSQLServerTargetTable(table)
	if table.Identity != nil {
		table.Identity.Frontier = nil
	}
	sort.Slice(table.Indexes, func(left, right int) bool {
		return sqlServerFoldedName(table.Indexes[left].Name) <
			sqlServerFoldedName(table.Indexes[right].Name)
	})
	sort.Slice(table.Checks, func(left, right int) bool {
		return sqlServerFoldedName(table.Checks[left].Name) <
			sqlServerFoldedName(table.Checks[right].Name)
	})
	sort.Slice(table.ForeignKeys, func(left, right int) bool {
		return sqlServerFoldedName(table.ForeignKeys[left].Name) <
			sqlServerFoldedName(table.ForeignKeys[right].Name)
	})
	return table
}

func preflightSQLServerDropRecreate(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	tables []schema.Table,
) error {
	selected, namespace, err := sqlServerSelectedTargetTables(tables)
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		return nil
	}
	if err := preflightSQLServerDDLTriggers(ctx, queryer); err != nil {
		return err
	}
	if err := preflightSQLServerExternalForeignKeys(
		ctx,
		queryer,
		namespace,
		selected,
	); err != nil {
		return err
	}
	if err := preflightSQLServerDependencies(
		ctx,
		queryer,
		namespace,
		selected,
	); err != nil {
		return err
	}
	if err := preflightSQLServerSynonyms(ctx, queryer); err != nil {
		return err
	}
	return preflightSQLServerConstraintNames(
		ctx,
		queryer,
		namespace,
		tables,
		selected,
	)
}

// preflightSQLServerDDLTriggers is transaction-compatible so preparation can
// repeat the proof immediately before destructive DDL. Database-scoped DDL
// triggers are fully visible under the required database VIEW DEFINITION
// permission. Server-scoped metadata is complete only with server-level
// VIEW ANY DEFINITION, so the target fails closed when that proof is absent.
func preflightSQLServerDDLTriggers(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
) error {
	var name string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT TOP (1) target_trigger.name
		   FROM sys.triggers AS target_trigger
		  WHERE target_trigger.parent_class = 0
		    AND target_trigger.is_disabled = 0
		  ORDER BY target_trigger.name, target_trigger.object_id`,
	).Scan(&name)
	if err == nil {
		return fmt.Errorf(
			"preflight SQL Server target: enabled database DDL trigger %s prevents safe target preparation",
			name,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"inspect SQL Server database DDL triggers: %w",
			err,
		)
	}
	var viewAnyDefinition bool
	if err := queryer.QueryRowContext(
		ctx,
		`SELECT CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
			NULL, NULL, 'VIEW ANY DEFINITION'
		), 0))`,
	).Scan(&viewAnyDefinition); err != nil {
		return fmt.Errorf(
			"inspect SQL Server server-trigger metadata permission: %w",
			err,
		)
	}
	if err := validateSQLServerServerTriggerVisibility(
		viewAnyDefinition,
	); err != nil {
		return err
	}
	err = queryer.QueryRowContext(
		ctx,
		`SELECT TOP (1) target_trigger.name
		   FROM sys.server_triggers AS target_trigger
		  WHERE target_trigger.is_disabled = 0
		  ORDER BY target_trigger.name, target_trigger.object_id`,
	).Scan(&name)
	if err == nil {
		return fmt.Errorf(
			"preflight SQL Server target: visible enabled server DDL trigger %s prevents safe target preparation",
			name,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"inspect SQL Server server DDL triggers: %w",
			err,
		)
	}
	return nil
}

func validateSQLServerServerTriggerVisibility(
	viewAnyDefinition bool,
) error {
	if viewAnyDefinition {
		return nil
	}
	return fmt.Errorf(
		"preflight SQL Server target: server VIEW ANY DEFINITION permission is required to exclude hidden server DDL triggers",
	)
}

func sqlServerSelectedTargetTables(
	tables []schema.Table,
) (map[string]schema.Table, string, error) {
	selected := make(map[string]schema.Table, len(tables))
	namespace := ""
	for _, table := range tables {
		if namespace == "" {
			namespace = table.Schema
		} else if table.Schema != namespace {
			return nil, "", fmt.Errorf(
				"preflight SQL Server target: all tables must use one target schema",
			)
		}
		key := sqlServerFoldedName(table.Name)
		if previous, duplicate := selected[key]; duplicate {
			return nil, "", fmt.Errorf(
				"preflight SQL Server target: planned tables %q and %q collide under case-insensitive catalog rules",
				previous.Name,
				table.Name,
			)
		}
		selected[key] = table
	}
	return selected, namespace, nil
}

func preflightSQLServerRelationKinds(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	selected map[string]schema.Table,
) error {
	if len(selected) == 0 {
		return nil
	}
	keys := make([]string, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		planned := selected[key]
		catalog, exists, err := readSQLServerTargetTableCatalog(
			ctx,
			queryer,
			namespace,
			planned.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQL Server target table %s: %w",
				planned.Name,
				err,
			)
		}
		if !exists {
			continue
		}
		if err := validateSQLServerTargetTableCatalog(
			planned.Name,
			catalog,
		); err != nil {
			return err
		}
	}
	return nil
}

type sqlServerTargetTableCatalog struct {
	name                        string
	objectID                    int64
	objectType                  string
	typeDescription             string
	systemShipped               bool
	temporalType                int
	memoryOptimized             bool
	durability                  string
	fileStreamDataSpaceID       int64
	fileTable                   bool
	replicated                  bool
	replicationFilter           bool
	mergePublished              bool
	syncTransactionSubscribed   bool
	changeDataCapture           bool
	historyTableID              int64
	node                        bool
	edge                        bool
	ledgerType                  int
	droppedLedgerTable          bool
	remoteDataArchive           bool
	external                    bool
	lockOnBulkLoad              bool
	uncheckedAssemblyData       bool
	textInRowLimit              int
	largeValuesOutOfRow         bool
	lockEscalation              int
	baseIndexCount              int
	baseIndexType               int
	baseIndexTypeDescription    string
	baseIndexDataSpaceType      string
	maxPartition                int
	partitionSchemeCount        int
	compressedPartitionCount    int
	nonRowstoreIndexCount       int
	unmodeledIndexOptionCount   int
	includedIndexColumnCount    int
	unmodeledColumnFeatureCount int
	triggerCount                int
	securityPredicateCount      int
	fullTextIndexCount          int
	changeTrackingCount         int
}

const sqlServerTargetTableCatalogQuery = `
	SELECT
		target_object.name,
		target_object.object_id,
		RTRIM(target_object.type),
		target_object.type_desc,
		target_object.is_ms_shipped,
		COALESCE(target_table.temporal_type, -1),
		CONVERT(bit, COALESCE(target_table.is_memory_optimized, 0)),
		COALESCE(target_table.durability_desc, ''),
		COALESCE(target_table.filestream_data_space_id, 0),
		CONVERT(bit, COALESCE(target_table.is_filetable, 0)),
		CONVERT(bit, COALESCE(target_table.is_replicated, 0)),
		CONVERT(bit, COALESCE(target_table.has_replication_filter, 0)),
		CONVERT(bit, COALESCE(target_table.is_merge_published, 0)),
		CONVERT(bit, COALESCE(
			target_table.is_sync_tran_subscribed, 0
		)),
		CONVERT(bit, COALESCE(target_table.is_tracked_by_cdc, 0)),
		COALESCE(target_table.history_table_id, 0),
		CONVERT(bit, COALESCE(target_table.is_node, 0)),
		CONVERT(bit, COALESCE(target_table.is_edge, 0)),
		COALESCE(target_table.ledger_type, 0),
		CONVERT(bit, COALESCE(
			target_table.is_dropped_ledger_table, 0
		)),
		CONVERT(bit, COALESCE(
			target_table.is_remote_data_archive_enabled, 0
		)),
		CONVERT(bit, COALESCE(target_table.is_external, 0)),
		CONVERT(bit, COALESCE(target_table.lock_on_bulk_load, 0)),
		CONVERT(bit, COALESCE(
			target_table.has_unchecked_assembly_data, 0
		)),
		COALESCE(target_table.text_in_row_limit, 0),
		CONVERT(bit, COALESCE(
			target_table.large_value_types_out_of_row, 0
		)),
		COALESCE(target_table.lock_escalation, 0),
		(
			SELECT COUNT(*)
			  FROM sys.indexes AS base_index
			 WHERE base_index.object_id = target_table.object_id
			   AND base_index.index_id IN (0, 1)
		),
		COALESCE((
			SELECT MAX(base_index.type)
			  FROM sys.indexes AS base_index
			 WHERE base_index.object_id = target_table.object_id
			   AND base_index.index_id IN (0, 1)
		), -1),
		COALESCE((
			SELECT MAX(base_index.type_desc)
			  FROM sys.indexes AS base_index
			 WHERE base_index.object_id = target_table.object_id
			   AND base_index.index_id IN (0, 1)
		), ''),
		COALESCE((
			SELECT MAX(base_space.type)
			  FROM sys.indexes AS base_index
			  JOIN sys.data_spaces AS base_space
			    ON base_space.data_space_id =
			       base_index.data_space_id
			 WHERE base_index.object_id = target_table.object_id
			   AND base_index.index_id IN (0, 1)
		), ''),
		COALESCE((
			SELECT MAX(target_partition.partition_number)
			  FROM sys.partitions AS target_partition
			 WHERE target_partition.object_id =
			       target_table.object_id
		), 0),
		(
			SELECT COUNT(*)
			  FROM sys.indexes AS target_index
			  JOIN sys.data_spaces AS target_space
			    ON target_space.data_space_id =
			       target_index.data_space_id
			 WHERE target_index.object_id = target_table.object_id
			   AND target_space.type = 'PS'
		),
		(
			SELECT COUNT(*)
			  FROM sys.partitions AS target_partition
			 WHERE target_partition.object_id =
			       target_table.object_id
			   AND target_partition.data_compression <> 0
		),
		(
			SELECT COUNT(*)
			  FROM sys.indexes AS target_index
			 WHERE target_index.object_id = target_table.object_id
			   AND target_index.type NOT IN (0, 1, 2)
		),
		(
			SELECT COUNT(*)
			  FROM sys.indexes AS target_index
			 WHERE target_index.object_id = target_table.object_id
			   AND (
				target_index.is_disabled = 1 OR
				target_index.is_hypothetical = 1 OR
				target_index.has_filter = 1 OR
				target_index.filter_definition IS NOT NULL OR
				target_index.ignore_dup_key = 1 OR
				target_index.fill_factor <> 0 OR
				target_index.is_padded = 1 OR
				target_index.allow_row_locks = 0 OR
				target_index.allow_page_locks = 0
			   )
		),
		(
			SELECT COUNT(*)
			  FROM sys.index_columns AS target_index_column
			 WHERE target_index_column.object_id =
			       target_table.object_id
			   AND target_index_column.is_included_column = 1
		),
		(
			SELECT COUNT(*)
			  FROM sys.columns AS target_column
			  LEFT JOIN sys.masked_columns AS masked_column
			    ON masked_column.object_id = target_column.object_id
			   AND masked_column.column_id = target_column.column_id
			 WHERE target_column.object_id = target_table.object_id
			   AND (
				target_column.is_rowguidcol = 1 OR
				target_column.is_computed = 1 OR
				target_column.is_filestream = 1 OR
				target_column.is_sparse = 1 OR
				target_column.is_column_set = 1 OR
				target_column.generated_always_type <> 0 OR
				target_column.encryption_type IS NOT NULL OR
				target_column.is_hidden = 1 OR
				COALESCE(masked_column.is_masked, 0) = 1 OR
				target_column.graph_type IS NOT NULL OR
				target_column.xml_collection_id <> 0 OR
				target_column.rule_object_id <> 0
			   )
		),
		(
			SELECT COUNT(*)
			  FROM sys.triggers AS target_trigger
			 WHERE target_trigger.parent_id = target_table.object_id
			   AND target_trigger.parent_class = 1
		),
		(
			SELECT COUNT(*)
			  FROM sys.security_predicates AS target_predicate
			 WHERE target_predicate.target_object_id =
			       target_table.object_id
		),
		(
			SELECT COUNT(*)
			  FROM sys.fulltext_indexes AS target_fulltext
			 WHERE target_fulltext.object_id = target_table.object_id
		),
		(
			SELECT COUNT(*)
			  FROM sys.change_tracking_tables AS target_tracking
			 WHERE target_tracking.object_id = target_table.object_id
		)
	   FROM sys.objects AS target_object
	   JOIN sys.schemas AS target_schema
	     ON target_schema.schema_id = target_object.schema_id
	   LEFT JOIN sys.tables AS target_table
	     ON target_table.object_id = target_object.object_id
	  WHERE target_schema.name = @p1
	    AND target_object.name = @p2
`

func readSQLServerTargetTableCatalog(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	name string,
) (sqlServerTargetTableCatalog, bool, error) {
	var result sqlServerTargetTableCatalog
	err := queryer.QueryRowContext(
		ctx,
		sqlServerTargetTableCatalogQuery,
		namespace,
		name,
	).Scan(
		&result.name,
		&result.objectID,
		&result.objectType,
		&result.typeDescription,
		&result.systemShipped,
		&result.temporalType,
		&result.memoryOptimized,
		&result.durability,
		&result.fileStreamDataSpaceID,
		&result.fileTable,
		&result.replicated,
		&result.replicationFilter,
		&result.mergePublished,
		&result.syncTransactionSubscribed,
		&result.changeDataCapture,
		&result.historyTableID,
		&result.node,
		&result.edge,
		&result.ledgerType,
		&result.droppedLedgerTable,
		&result.remoteDataArchive,
		&result.external,
		&result.lockOnBulkLoad,
		&result.uncheckedAssemblyData,
		&result.textInRowLimit,
		&result.largeValuesOutOfRow,
		&result.lockEscalation,
		&result.baseIndexCount,
		&result.baseIndexType,
		&result.baseIndexTypeDescription,
		&result.baseIndexDataSpaceType,
		&result.maxPartition,
		&result.partitionSchemeCount,
		&result.compressedPartitionCount,
		&result.nonRowstoreIndexCount,
		&result.unmodeledIndexOptionCount,
		&result.includedIndexColumnCount,
		&result.unmodeledColumnFeatureCount,
		&result.triggerCount,
		&result.securityPredicateCount,
		&result.fullTextIndexCount,
		&result.changeTrackingCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlServerTargetTableCatalog{}, false, nil
	}
	if err != nil {
		return sqlServerTargetTableCatalog{}, false, err
	}
	return result, true, nil
}

func validateSQLServerTargetTableCatalog(
	plannedName string,
	value sqlServerTargetTableCatalog,
) error {
	reject := func(feature string) error {
		return fmt.Errorf(
			"preflight SQL Server table %s: existing target uses unsupported %s",
			plannedName,
			feature,
		)
	}
	switch {
	case value.name != plannedName ||
		value.objectID <= 0 ||
		value.objectType != "U" ||
		value.typeDescription != "USER_TABLE" ||
		value.systemShipped:
		return reject("catalog shape")
	case value.temporalType != 0 || value.historyTableID != 0:
		return reject("temporal-table metadata")
	case value.memoryOptimized || value.durability != "SCHEMA_AND_DATA":
		return reject("table durability")
	case value.fileStreamDataSpaceID != 0 || value.fileTable:
		return reject("FILESTREAM storage")
	case value.replicated || value.replicationFilter ||
		value.mergePublished ||
		value.syncTransactionSubscribed ||
		value.changeDataCapture:
		return reject("replication or change-data-capture metadata")
	case value.node || value.edge:
		return reject("graph metadata")
	case value.ledgerType != 0 || value.droppedLedgerTable:
		return reject("ledger metadata")
	case value.remoteDataArchive || value.external:
		return reject("external table storage")
	case value.lockOnBulkLoad:
		return reject("table lock on bulk load")
	case value.uncheckedAssemblyData:
		return reject("unchecked assembly data")
	case value.textInRowLimit != 0 || value.largeValuesOutOfRow:
		return reject("out-of-row large-value table options")
	case value.lockEscalation != 0:
		return reject("non-default lock escalation")
	case value.baseIndexCount != 1 ||
		(value.baseIndexType != 0 && value.baseIndexType != 1) ||
		(value.baseIndexType == 0 &&
			value.baseIndexTypeDescription != "HEAP") ||
		(value.baseIndexType == 1 &&
			value.baseIndexTypeDescription != "CLUSTERED") ||
		value.baseIndexDataSpaceType != "FG":
		return reject("non-rowstore base storage")
	case value.maxPartition != 1 ||
		value.partitionSchemeCount != 0:
		return reject("partitioned table or index storage")
	case value.compressedPartitionCount != 0:
		return reject("compressed table or index storage")
	case value.nonRowstoreIndexCount != 0:
		return reject("non-rowstore index")
	case value.unmodeledIndexOptionCount != 0 ||
		value.includedIndexColumnCount != 0:
		return reject("unmodeled index options")
	case value.unmodeledColumnFeatureCount != 0:
		return reject("unmodeled column features")
	case value.triggerCount != 0:
		return reject("DML triggers")
	case value.securityPredicateCount != 0:
		return reject("row-level security predicates")
	case value.fullTextIndexCount != 0:
		return reject("full-text indexes")
	case value.changeTrackingCount != 0:
		return reject("change tracking")
	default:
		return nil
	}
}

func preflightSQLServerExternalForeignKeys(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	selected map[string]schema.Table,
) error {
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT
			parent_schema.name,
			parent_table.name,
			target_foreign_key.name,
			referenced_schema.name,
			referenced_table.name
		   FROM sys.foreign_keys AS target_foreign_key
		   JOIN sys.tables AS parent_table
		     ON parent_table.object_id =
		        target_foreign_key.parent_object_id
		   JOIN sys.schemas AS parent_schema
		     ON parent_schema.schema_id = parent_table.schema_id
		   JOIN sys.tables AS referenced_table
		     ON referenced_table.object_id =
		        target_foreign_key.referenced_object_id
		   JOIN sys.schemas AS referenced_schema
		     ON referenced_schema.schema_id =
		        referenced_table.schema_id
		  WHERE parent_schema.name = @p1
		     OR referenced_schema.name = @p1
		  ORDER BY
			parent_schema.name,
			parent_table.name,
			target_foreign_key.name`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server foreign-key dependencies: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var parentSchema, parentTable, constraint string
		var referencedSchema, referencedTable string
		if err := rows.Scan(
			&parentSchema,
			&parentTable,
			&constraint,
			&referencedSchema,
			&referencedTable,
		); err != nil {
			return fmt.Errorf(
				"read SQL Server foreign-key dependency: %w",
				err,
			)
		}
		if err := validateSQLServerForeignKeyDependency(
			namespace,
			selected,
			sqlServerForeignKeyDependency{
				parentSchema:     parentSchema,
				parentTable:      parentTable,
				constraint:       constraint,
				referencedSchema: referencedSchema,
				referencedTable:  referencedTable,
			},
		); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQL Server foreign-key dependencies: %w",
			err,
		)
	}
	return nil
}

type sqlServerForeignKeyDependency struct {
	parentSchema     string
	parentTable      string
	constraint       string
	referencedSchema string
	referencedTable  string
}

func validateSQLServerForeignKeyDependency(
	namespace string,
	selected map[string]schema.Table,
	dependency sqlServerForeignKeyDependency,
) error {
	_, parentSelected := selected[sqlServerFoldedName(
		dependency.parentTable,
	)]
	parentSelected = parentSelected &&
		dependency.parentSchema == namespace
	_, referencedSelected := selected[sqlServerFoldedName(
		dependency.referencedTable,
	)]
	referencedSelected = referencedSelected &&
		dependency.referencedSchema == namespace
	switch {
	case referencedSelected && !parentSelected:
		return fmt.Errorf(
			"preflight SQL Server table %s: external table %s.%s retains foreign key %s",
			dependency.referencedTable,
			dependency.parentSchema,
			dependency.parentTable,
			dependency.constraint,
		)
	case parentSelected && !referencedSelected:
		return fmt.Errorf(
			"preflight SQL Server table %s: retained foreign key %s references unselected table %s.%s",
			dependency.parentTable,
			dependency.constraint,
			dependency.referencedSchema,
			dependency.referencedTable,
		)
	default:
		return nil
	}
}

func preflightSQLServerDependencies(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	selected map[string]schema.Table,
) error {
	if len(selected) == 0 {
		return nil
	}
	keys := make([]string, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		table := selected[key]
		rows, err := queryer.QueryContext(
			ctx,
			`SELECT
				referencing_schema.name,
				referencing_object.name,
				referencing_object.type_desc,
				CONVERT(bit, CASE
					WHEN dependency.referenced_id IS NULL
					THEN 1 ELSE 0 END)
			   FROM sys.sql_expression_dependencies AS dependency
			   JOIN sys.objects AS referencing_object
			     ON referencing_object.object_id =
			        dependency.referencing_id
			   JOIN sys.schemas AS referencing_schema
			     ON referencing_schema.schema_id =
			        referencing_object.schema_id
			   LEFT JOIN sys.tables AS referenced_table
			     ON referenced_table.object_id =
			        dependency.referenced_id
			   LEFT JOIN sys.schemas AS referenced_schema
			     ON referenced_schema.schema_id =
			        referenced_table.schema_id
			  WHERE referencing_object.type IN
			        ('V', 'P', 'FN', 'IF', 'TF', 'TR')
			    AND (
				(
				 dependency.referenced_id IS NOT NULL
				 AND referenced_schema.name = @p1
				 AND referenced_table.name = @p2
				)
				OR
				(
				 dependency.referenced_id IS NULL
				 AND dependency.referenced_entity_name = @p2
				 AND (
					dependency.referenced_schema_name = @p1
					OR dependency.referenced_schema_name IS NULL
				 )
				 AND (
					dependency.referenced_database_name IS NULL
					OR dependency.referenced_database_name =
					   DB_NAME()
					OR dependency.referenced_server_name IS NOT NULL
				 )
				)
			    )
			  ORDER BY
				referencing_schema.name,
				referencing_object.name,
				dependency.referencing_minor_id`,
			namespace,
			table.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"inspect SQL Server expression dependencies for table %s: %w",
				table.Name,
				err,
			)
		}
		var dependencyError error
		for rows.Next() {
			var referencingSchema, referencingName, kind string
			var nameBased bool
			if err := rows.Scan(
				&referencingSchema,
				&referencingName,
				&kind,
				&nameBased,
			); err != nil {
				dependencyError = fmt.Errorf(
					"read SQL Server expression dependency for table %s: %w",
					table.Name,
					err,
				)
				break
			}
			dependencyError = sqlServerExpressionDependencyError(
				table.Name,
				referencingSchema,
				referencingName,
				kind,
				nameBased,
			)
			break
		}
		if dependencyError == nil {
			if err := rows.Err(); err != nil {
				dependencyError = fmt.Errorf(
					"iterate SQL Server expression dependencies for table %s: %w",
					table.Name,
					err,
				)
			}
		}
		if err := rows.Close(); err != nil &&
			dependencyError == nil {
			dependencyError = fmt.Errorf(
				"close SQL Server expression dependencies for table %s: %w",
				table.Name,
				err,
			)
		}
		if dependencyError != nil {
			return dependencyError
		}
	}
	return nil
}

func sqlServerExpressionDependencyError(
	table string,
	referencingSchema string,
	referencingName string,
	kind string,
	nameBased bool,
) error {
	if nameBased {
		return fmt.Errorf(
			"preflight SQL Server table %s: name-based %s %s.%s may depend on the selected target",
			table,
			kind,
			referencingSchema,
			referencingName,
		)
	}
	return fmt.Errorf(
		"preflight SQL Server table %s: %s %s.%s depends on the selected target",
		table,
		kind,
		referencingSchema,
		referencingName,
	)
}

func preflightSQLServerSynonyms(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
) error {
	var namespace, name string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT TOP (1)
			synonym_schema.name,
			target_synonym.name
		   FROM sys.synonyms AS target_synonym
		   JOIN sys.schemas AS synonym_schema
		     ON synonym_schema.schema_id =
		        target_synonym.schema_id
		  ORDER BY synonym_schema.name, target_synonym.name`,
	).Scan(&namespace, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server target synonyms: %w",
			err,
		)
	}
	return fmt.Errorf(
		"preflight SQL Server target: synonym %s.%s prevents a complete dependency proof",
		namespace,
		name,
	)
}

func preflightSQLServerConstraintNames(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	tables []schema.Table,
	selected map[string]schema.Table,
) error {
	planned := make(map[string]string)
	add := func(name, table, kind string) error {
		key := sqlServerFoldedName(name)
		if name == "" {
			return fmt.Errorf(
				"preflight SQL Server table %s: planned %s name is empty",
				table,
				kind,
			)
		}
		if previous, duplicate := planned[key]; duplicate {
			return fmt.Errorf(
				"preflight SQL Server target: planned constraints %s and %s share name %q",
				previous,
				table+" "+kind,
				name,
			)
		}
		planned[key] = table + " " + kind
		return nil
	}
	for _, table := range tables {
		primaryKey, err := schema.SQLServerPrimaryKeyConstraintName(table)
		if err != nil {
			return fmt.Errorf(
				"preflight SQL Server primary-key name for %s: %w",
				table.Name,
				err,
			)
		}
		if primaryKey != "" {
			if err := add(
				primaryKey,
				table.Name,
				"primary key",
			); err != nil {
				return err
			}
		}
		for _, check := range table.Checks {
			if err := add(check.Name, table.Name, "CHECK"); err != nil {
				return err
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if err := add(
				foreignKey.Name,
				table.Name,
				"foreign key",
			); err != nil {
				return err
			}
		}
	}
	if len(planned) == 0 {
		return nil
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT
			target_object.name,
			COALESCE(parent_table.name, '')
		   FROM sys.objects AS target_object
		   JOIN sys.schemas AS target_schema
		     ON target_schema.schema_id = target_object.schema_id
		   LEFT JOIN sys.tables AS parent_table
		     ON parent_table.object_id =
		        target_object.parent_object_id
		  WHERE target_schema.name = @p1
		  ORDER BY target_object.name, target_object.object_id`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQL Server constraint names: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var name, parent string
		if err := rows.Scan(&name, &parent); err != nil {
			return fmt.Errorf(
				"read SQL Server constraint name: %w",
				err,
			)
		}
		if _, reserved := planned[sqlServerFoldedName(name)]; !reserved {
			continue
		}
		if _, selectedParent := selected[sqlServerFoldedName(
			parent,
		)]; selectedParent {
			continue
		}
		return fmt.Errorf(
			"preflight SQL Server target: planned constraint name %q collides with an unselected object",
			name,
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQL Server constraint names: %w",
			err,
		)
	}
	return nil
}

func sqlServerFoldedName(value string) string {
	return strings.ToLower(value)
}
