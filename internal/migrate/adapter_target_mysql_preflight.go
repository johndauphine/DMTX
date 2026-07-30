package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func (adapter *mysqlTargetAdapter) PreflightTables(
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
				"preflight MySQL table %s: planned database %q does not match target database %q",
				table.Name,
				table.Schema,
				adapter.namespace,
			)
		}
	}
	if mode == "drop_recreate" {
		return preflightMySQLDropRecreate(
			ctx,
			adapter.database,
			targetTables,
			adapter.flavor,
		)
	}
	return preflightMySQLRetainedTables(
		ctx,
		adapter.database,
		targetTables,
	)
}

func preflightMySQLRetainedTables(
	ctx context.Context,
	database *sql.DB,
	targetTables []schema.Table,
) error {
	for _, planned := range targetTables {
		exists, err := mysqlTargetTableExists(
			ctx,
			database,
			planned,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight MySQL table %s: %w",
				planned.Name,
				err,
			)
		}
		if !exists {
			return fmt.Errorf(
				"preflight MySQL table %s: upsert requires an existing target table",
				planned.Name,
			)
		}
		actual, err := engine.InspectMySQLTable(
			ctx,
			database,
			planned.Schema,
			planned.Name,
		)
		if err != nil {
			return fmt.Errorf(
				"preflight MySQL table %s retained shape: %w",
				planned.Name,
				err,
			)
		}
		if err := validateMySQLRetainedTableShape(
			planned,
			actual,
		); err != nil {
			return fmt.Errorf(
				"preflight MySQL table %s: retained target shape differs from the plan: %w",
				planned.Name,
				err,
			)
		}
	}
	return nil
}

func mysqlTargetTableExists(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) (bool, error) {
	var exists bool
	err := database.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ?
			  AND TABLE_NAME = ?
			  AND TABLE_TYPE = 'BASE TABLE'
		)`,
		table.Schema,
		table.Name,
	).Scan(&exists)
	return exists, err
}

func validateMySQLRetainedTableShape(
	planned schema.Table,
	actual schema.Table,
) error {
	planned = mysqlTableShapeWithoutFrontier(planned)
	actual = mysqlTableShapeWithoutFrontier(actual)
	if planned.Schema != actual.Schema ||
		planned.Name != actual.Name ||
		planned.MySQLCollation != actual.MySQLCollation ||
		planned.SQLiteStrict != actual.SQLiteStrict ||
		planned.SQLiteWithoutRowID != actual.SQLiteWithoutRowID {
		return fmt.Errorf("table identity or options differ")
	}
	if err := validateMySQLRetainedIdentity(
		planned.Identity,
		actual.Identity,
	); err != nil {
		return err
	}
	if len(planned.Columns) != len(actual.Columns) {
		return fmt.Errorf(
			"column count is %d, expected %d",
			len(actual.Columns),
			len(planned.Columns),
		)
	}
	for index := range planned.Columns {
		if err := validateMySQLRetainedColumn(
			planned.Columns[index],
			actual.Columns[index],
		); err != nil {
			return fmt.Errorf(
				"column %d (%s): %w",
				index+1,
				planned.Columns[index].Name,
				err,
			)
		}
	}
	plannedIndexes, err := mysqlRetainedIndexesByName(planned.Indexes)
	if err != nil {
		return err
	}
	actualIndexes, err := mysqlRetainedIndexesByName(actual.Indexes)
	if err != nil {
		return err
	}
	if len(plannedIndexes) != len(actualIndexes) {
		return fmt.Errorf(
			"index count is %d, expected %d",
			len(actualIndexes),
			len(plannedIndexes),
		)
	}
	for key, expected := range plannedIndexes {
		found, exists := actualIndexes[key]
		if !exists {
			return fmt.Errorf("required index %q is missing", expected.Name)
		}
		if expected.Name != found.Name ||
			expected.Unique != found.Unique ||
			expected.Inline != found.Inline ||
			!slices.Equal(expected.Columns, found.Columns) {
			return fmt.Errorf(
				"index %q differs from the planned shape",
				expected.Name,
			)
		}
	}
	plannedChecks, err := mysqlRetainedChecksByName(planned.Checks)
	if err != nil {
		return err
	}
	actualChecks, err := mysqlRetainedChecksByName(actual.Checks)
	if err != nil {
		return err
	}
	if len(plannedChecks) != len(actualChecks) {
		return fmt.Errorf(
			"CHECK count is %d, expected %d",
			len(actualChecks),
			len(plannedChecks),
		)
	}
	for key, expected := range plannedChecks {
		found, exists := actualChecks[key]
		if !exists {
			return fmt.Errorf("required CHECK %q is missing", expected.Name)
		}
		expectedSQL, expectedErr := renderMySQLRetainedCheck(
			planned,
			expected,
		)
		foundSQL, foundErr := renderMySQLRetainedCheck(actual, found)
		if expected.Name != found.Name ||
			expectedErr != nil ||
			foundErr != nil ||
			expectedSQL != foundSQL {
			return fmt.Errorf(
				"CHECK %q differs from the planned shape",
				expected.Name,
			)
		}
	}
	plannedForeignKeys, err := mysqlRetainedForeignKeysByName(
		planned.ForeignKeys,
	)
	if err != nil {
		return err
	}
	actualForeignKeys, err := mysqlRetainedForeignKeysByName(
		actual.ForeignKeys,
	)
	if err != nil {
		return err
	}
	if len(plannedForeignKeys) != len(actualForeignKeys) {
		return fmt.Errorf(
			"foreign-key count is %d, expected %d",
			len(actualForeignKeys),
			len(plannedForeignKeys),
		)
	}
	for key, expected := range plannedForeignKeys {
		found, exists := actualForeignKeys[key]
		if !exists {
			return fmt.Errorf(
				"required foreign key %q is missing",
				expected.Name,
			)
		}
		if expected.Name != found.Name ||
			expected.ReferencedTable != found.ReferencedTable ||
			expected.OnUpdate != found.OnUpdate ||
			expected.OnDelete != found.OnDelete ||
			expected.Match != found.Match ||
			!slices.Equal(expected.Columns, found.Columns) ||
			!slices.Equal(
				expected.ReferencedColumns,
				found.ReferencedColumns,
			) {
			return fmt.Errorf(
				"foreign key %q differs from the planned shape",
				expected.Name,
			)
		}
	}
	return nil
}

func mysqlRetainedIndexesByName(
	indexes []schema.Index,
) (map[string]schema.Index, error) {
	result := make(map[string]schema.Index, len(indexes))
	for _, index := range indexes {
		key := strings.ToLower(index.Name)
		if index.Name == "" {
			return nil, fmt.Errorf("retained index name is empty")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf(
				"duplicate retained index %q",
				index.Name,
			)
		}
		result[key] = index
	}
	return result, nil
}

func mysqlRetainedChecksByName(
	checks []schema.CheckConstraint,
) (map[string]schema.CheckConstraint, error) {
	result := make(
		map[string]schema.CheckConstraint,
		len(checks),
	)
	for _, check := range checks {
		key := strings.ToLower(check.Name)
		if check.Name == "" {
			return nil, fmt.Errorf("retained CHECK name is empty")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf(
				"duplicate retained CHECK %q",
				check.Name,
			)
		}
		result[key] = check
	}
	return result, nil
}

func mysqlRetainedForeignKeysByName(
	foreignKeys []schema.ForeignKey,
) (map[string]schema.ForeignKey, error) {
	result := make(
		map[string]schema.ForeignKey,
		len(foreignKeys),
	)
	for _, foreignKey := range foreignKeys {
		key := strings.ToLower(foreignKey.Name)
		if foreignKey.Name == "" {
			return nil, fmt.Errorf("retained foreign-key name is empty")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf(
				"duplicate retained foreign key %q",
				foreignKey.Name,
			)
		}
		result[key] = foreignKey
	}
	return result, nil
}

func mysqlTableShapeWithoutFrontier(table schema.Table) schema.Table {
	table.Identity = cloneSchemaIdentity(table.Identity)
	if table.Identity != nil {
		table.Identity.Frontier = nil
	}
	return table
}

func validateMySQLRetainedIdentity(
	planned *schema.Identity,
	actual *schema.Identity,
) error {
	if planned == nil || actual == nil {
		if planned == nil && actual == nil {
			return nil
		}
		return fmt.Errorf("identity presence differs")
	}
	if planned.Column != actual.Column ||
		planned.Generation != actual.Generation {
		return fmt.Errorf("identity shape differs")
	}
	return nil
}

func validateMySQLRetainedColumn(
	planned schema.Column,
	actual schema.Column,
) error {
	if planned.Name != actual.Name ||
		mysqlRetainedEffectiveColumnType(planned) !=
			mysqlRetainedEffectiveColumnType(actual) ||
		planned.Nullable != actual.Nullable ||
		planned.PrimaryKey != actual.PrimaryKey ||
		planned.PrimaryKeyPosition != actual.PrimaryKeyPosition {
		return fmt.Errorf("type, nullability, or key metadata differs")
	}
	if planned.DeclaredType == nil || actual.DeclaredType == nil {
		if planned.DeclaredType != nil || actual.DeclaredType != nil {
			return fmt.Errorf("declared type presence differs")
		}
	} else if planned.DeclaredType.Base != actual.DeclaredType.Base ||
		!slices.Equal(
			planned.DeclaredType.Arguments,
			actual.DeclaredType.Arguments,
		) {
		return fmt.Errorf("declared type differs")
	}
	plannedSQL, plannedErr := renderMySQLRetainedColumn(planned)
	actualSQL, actualErr := renderMySQLRetainedColumn(actual)
	if plannedErr != nil || actualErr != nil || plannedSQL != actualSQL {
		return fmt.Errorf("default differs")
	}
	return nil
}

func mysqlRetainedEffectiveColumnType(column schema.Column) string {
	semantic := strings.ToLower(strings.Join(
		strings.Fields(column.Type),
		" ",
	))
	if semantic != "uuid" || column.DeclaredType == nil {
		return semantic
	}
	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))
	if base == "varchar" &&
		slices.Equal(column.DeclaredType.Arguments, []int{36}) {
		return "varchar"
	}
	return semantic
}

func renderMySQLRetainedColumn(column schema.Column) (string, error) {
	column.PrimaryKey = false
	column.PrimaryKeyPosition = 0
	return schema.CreateTable(schema.MySQL, schema.Table{
		Name:    "dmtx_retained_column",
		Columns: []schema.Column{column},
	})
}

func renderMySQLRetainedCheck(
	table schema.Table,
	check schema.CheckConstraint,
) (string, error) {
	table.Identity = nil
	table.Indexes = nil
	table.ForeignKeys = nil
	table.Checks = []schema.CheckConstraint{check}
	statements, err := schema.PlanMySQLDropRecreateObjects(
		[]schema.Table{table},
	)
	if err != nil {
		return "", err
	}
	if len(statements) != 1 ||
		statements[0].Kind != schema.MySQLCheckObject {
		return "", fmt.Errorf("unexpected retained CHECK plan")
	}
	return statements[0].SQL, nil
}

type mysqlExternalForeignKey struct {
	namespace           string
	table               string
	name                string
	referencedNamespace string
	referencedTable     string
}

func preflightMySQLDropRecreate(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	tables []schema.Table,
	flavor engine.MySQLServerFlavor,
) error {
	if len(tables) == 0 {
		return nil
	}
	selected := make(map[string]struct{}, len(tables))
	namespaces := make(map[string]struct{}, 1)
	for _, table := range tables {
		selected[adapterSourceTableKey(
			table.Schema,
			table.Name,
		)] = struct{}{}
		namespaces[table.Schema] = struct{}{}
	}
	if len(namespaces) != 1 {
		return fmt.Errorf(
			"preflight MySQL target: all tables must use one target database",
		)
	}
	if err := preflightMySQLSelectedTargetTriggers(
		ctx,
		database,
		tables[0].Schema,
		selected,
	); err != nil {
		return err
	}
	if err := preflightMySQLRelationKindsAndViews(
		ctx,
		database,
		tables[0].Schema,
		selected,
		flavor,
	); err != nil {
		return err
	}
	if err := preflightMySQLConstraintNames(
		ctx,
		database,
		tables,
		selected,
	); err != nil {
		return err
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT
			TABLE_SCHEMA,
			TABLE_NAME,
			CONSTRAINT_NAME,
			REFERENCED_TABLE_SCHEMA,
			REFERENCED_TABLE_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_SCHEMA = ?
		  AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY
			TABLE_SCHEMA,
			TABLE_NAME,
			CONSTRAINT_NAME,
			ORDINAL_POSITION`,
		tables[0].Schema,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL foreign-key dependencies: %w",
			err,
		)
	}
	defer rows.Close()

	for rows.Next() {
		var dependency mysqlExternalForeignKey
		if err := rows.Scan(
			&dependency.namespace,
			&dependency.table,
			&dependency.name,
			&dependency.referencedNamespace,
			&dependency.referencedTable,
		); err != nil {
			return fmt.Errorf(
				"read MySQL foreign-key dependency: %w",
				err,
			)
		}
		referencedKey := adapterSourceTableKey(
			dependency.referencedNamespace,
			dependency.referencedTable,
		)
		if _, selectedReference := selected[referencedKey]; !selectedReference {
			continue
		}
		sourceKey := adapterSourceTableKey(
			dependency.namespace,
			dependency.table,
		)
		if _, selectedSource := selected[sourceKey]; selectedSource {
			continue
		}
		return fmt.Errorf(
			"preflight MySQL table %s: external table %s.%s retains foreign key %s",
			dependency.referencedTable,
			dependency.namespace,
			dependency.table,
			dependency.name,
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MySQL foreign-key dependencies: %w",
			err,
		)
	}
	return nil
}

func preflightMySQLRelationKindsAndViews(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	namespace string,
	selected map[string]struct{},
	flavor engine.MySQLServerFlavor,
) error {
	rows, err := database.QueryContext(
		ctx,
		`SELECT TABLE_NAME, TABLE_TYPE
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf("inspect MySQL target relations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return fmt.Errorf("read MySQL target relation: %w", err)
		}
		if _, planned := selected[adapterSourceTableKey(
			namespace,
			name,
		)]; planned && kind != "BASE TABLE" {
			return fmt.Errorf(
				"preflight MySQL table %s: existing target object is %s, not a base table",
				name,
				kind,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate MySQL target relations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close MySQL target relation catalog: %w", err)
	}
	if err := preflightMySQLGlobalViewVisibility(
		ctx,
		database,
		flavor,
	); err != nil {
		return err
	}

	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return preflightOracleMySQLViewDependencies(
			ctx,
			database,
			namespace,
			selected,
		)
	case engine.MySQLServerFlavorMariaDB1011:
		return preflightMariaDBViewDependencies(
			ctx,
			database,
			selected,
		)
	default:
		return fmt.Errorf(
			"inspect MySQL target view dependencies: unsupported server flavor",
		)
	}
}

func preflightMySQLSelectedTargetTriggers(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	namespace string,
	selected map[string]struct{},
) error {
	rows, err := database.QueryContext(
		ctx,
		`SELECT
			EVENT_OBJECT_SCHEMA,
			EVENT_OBJECT_TABLE,
			TRIGGER_NAME
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = ?
		ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL target triggers: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var schemaName, tableName, triggerName string
		if err := rows.Scan(
			&schemaName,
			&tableName,
			&triggerName,
		); err != nil {
			return fmt.Errorf(
				"read MySQL target trigger: %w",
				err,
			)
		}
		if _, planned := selected[adapterSourceTableKey(
			schemaName,
			tableName,
		)]; planned {
			return fmt.Errorf(
				"preflight MySQL table %s: target trigger %s prevents safe replacement",
				tableName,
				triggerName,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MySQL target triggers: %w",
			err,
		)
	}
	return nil
}

func preflightOracleMySQLViewDependencies(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	namespace string,
	selected map[string]struct{},
) error {
	viewRows, err := database.QueryContext(
		ctx,
		`SELECT
			VIEW_SCHEMA,
			VIEW_NAME,
			TABLE_SCHEMA,
			TABLE_NAME
		FROM information_schema.VIEW_TABLE_USAGE
		WHERE TABLE_SCHEMA = ?
		ORDER BY VIEW_SCHEMA, VIEW_NAME, TABLE_NAME`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf("inspect MySQL target view dependencies: %w", err)
	}
	defer viewRows.Close()
	for viewRows.Next() {
		var viewSchema, viewName, tableSchema, tableName string
		if err := viewRows.Scan(
			&viewSchema,
			&viewName,
			&tableSchema,
			&tableName,
		); err != nil {
			return fmt.Errorf(
				"read MySQL target view dependency: %w",
				err,
			)
		}
		if _, planned := selected[adapterSourceTableKey(
			tableSchema,
			tableName,
		)]; planned {
			return fmt.Errorf(
				"preflight MySQL table %s: view %s.%s depends on the selected target",
				tableName,
				viewSchema,
				viewName,
			)
		}
	}
	if err := viewRows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MySQL target view dependencies: %w",
			err,
		)
	}
	return nil
}

func preflightMariaDBViewDependencies(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	selected map[string]struct{},
) error {
	type selectedRelation struct {
		table  string
		needle string
	}
	relations := make([]selectedRelation, 0, len(selected))
	for key := range selected {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf(
				"inspect MariaDB target view dependencies: invalid selected relation",
			)
		}
		relations = append(relations, selectedRelation{
			table: parts[1],
			needle: mySQLIdentifier(parts[0]) + "." +
				mySQLIdentifier(parts[1]),
		})
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION, DEFINER
		FROM information_schema.VIEWS
		ORDER BY TABLE_SCHEMA, TABLE_NAME`,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MariaDB target view dependencies: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var viewSchema, viewName, definition, definer string
		if err := rows.Scan(
			&viewSchema,
			&viewName,
			&definition,
			&definer,
		); err != nil {
			return fmt.Errorf(
				"read MariaDB target view dependency: %w",
				err,
			)
		}
		visible, err := validateMariaDBViewDefinition(
			viewSchema,
			viewName,
			definition,
			definer,
		)
		if err != nil {
			return err
		}
		if !visible {
			continue
		}
		for _, relation := range relations {
			// MariaDB 10.11 exposes canonical VIEW_DEFINITION text with
			// every table reference schema-qualified and backtick-quoted.
			// Matching that exact identifier pair may conservatively reject
			// a definition containing the same text in a literal, but it
			// cannot miss a visible direct dependency.
			if strings.Contains(definition, relation.needle) {
				return fmt.Errorf(
					"preflight MySQL table %s: view %s.%s depends on the selected target",
					relation.table,
					viewSchema,
					viewName,
				)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MariaDB target view dependencies: %w",
			err,
		)
	}
	return nil
}

func preflightMySQLGlobalViewVisibility(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	flavor engine.MySQLServerFlavor,
) error {
	var hasGlobalShowView bool
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		var partialRevokes int
		if err := database.QueryRowContext(
			ctx,
			`SELECT
				COUNT(*) > 0,
				@@global.partial_revokes
			FROM information_schema.USER_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND PRIVILEGE_TYPE = 'SHOW VIEW'`,
		).Scan(
			&hasGlobalShowView,
			&partialRevokes,
		); err != nil {
			return fmt.Errorf(
				"inspect MySQL target view visibility: %w",
				err,
			)
		}
		return validateOracleMySQLGlobalViewVisibility(
			hasGlobalShowView,
			partialRevokes,
		)
	case engine.MySQLServerFlavorMariaDB1011:
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*) > 0
			FROM information_schema.USER_PRIVILEGES
			WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER()
			  AND PRIVILEGE_TYPE = 'SHOW VIEW'`,
		).Scan(&hasGlobalShowView); err != nil {
			return fmt.Errorf(
				"inspect MariaDB target view visibility: %w",
				err,
			)
		}
		return validateMariaDBGlobalViewVisibility(
			hasGlobalShowView,
		)
	default:
		return fmt.Errorf(
			"inspect MySQL target view visibility: unsupported server flavor",
		)
	}
}

func validateOracleMySQLGlobalViewVisibility(
	hasGlobalShowView bool,
	partialRevokes int,
) error {
	if !hasGlobalShowView {
		return fmt.Errorf(
			"inspect MySQL target view visibility: global SHOW VIEW privilege is required",
		)
	}
	if partialRevokes != 0 {
		return fmt.Errorf(
			"inspect MySQL target view visibility: partial_revokes must be disabled",
		)
	}
	return nil
}

func validateMariaDBGlobalViewVisibility(hasGlobalShowView bool) error {
	if !hasGlobalShowView {
		return fmt.Errorf(
			"inspect MariaDB target view visibility: global SHOW VIEW privilege is required",
		)
	}
	return nil
}

func validateMariaDBViewDefinition(
	schemaName string,
	viewName string,
	definition string,
	definer string,
) (bool, error) {
	if strings.TrimSpace(definition) != "" {
		return true, nil
	}
	if isMariaDBBuiltInSystemView(schemaName, definer) {
		return false, nil
	}
	return false, fmt.Errorf(
		"read MariaDB target view dependency: view %s.%s has no visible definition",
		schemaName,
		viewName,
	)
}

func isMariaDBBuiltInSystemView(schemaName, definer string) bool {
	if definer != "mariadb.sys@localhost" {
		return false
	}
	switch schemaName {
	case "mysql", "sys":
		return true
	default:
		return false
	}
}

func preflightMySQLConstraintNames(
	ctx context.Context,
	database mysqlTargetCatalogQueryer,
	tables []schema.Table,
	selected map[string]struct{},
) error {
	type plannedConstraint struct {
		name  string
		table string
	}
	planned := make(map[string]plannedConstraint)
	namespace := tables[0].Schema
	objectPlan, err := schema.PlanMySQLDropRecreateObjects(tables)
	if err != nil {
		return fmt.Errorf(
			"plan MySQL schema-scoped constraint names: %w",
			err,
		)
	}
	for _, object := range objectPlan {
		if object.Kind != schema.MySQLCheckObject &&
			object.Kind != schema.MySQLForeignKeyObject {
			continue
		}
		name, table := object.Name, object.Table
		key := strings.ToLower(name)
		if previous, duplicate := planned[key]; duplicate {
			return fmt.Errorf(
				"preflight MySQL target: tables %s and %s plan the same schema-scoped constraint name %s",
				previous.table,
				table,
				name,
			)
		}
		planned[key] = plannedConstraint{name: name, table: table}
	}
	if len(planned) == 0 {
		return nil
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT TABLE_NAME, CONSTRAINT_NAME
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ?
		  AND CONSTRAINT_TYPE IN ('CHECK', 'FOREIGN KEY')
		ORDER BY TABLE_NAME, CONSTRAINT_NAME`,
		namespace,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect MySQL schema-scoped constraint names: %w",
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			return fmt.Errorf(
				"read MySQL schema-scoped constraint name: %w",
				err,
			)
		}
		if _, selectedTable := selected[adapterSourceTableKey(
			namespace,
			table,
		)]; selectedTable {
			continue
		}
		if plannedObject, collision := planned[strings.ToLower(name)]; collision {
			return fmt.Errorf(
				"preflight MySQL table %s: unselected table %s retains schema-scoped constraint name %s",
				plannedObject.table,
				table,
				plannedObject.name,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate MySQL schema-scoped constraint names: %w",
			err,
		)
	}
	return nil
}
