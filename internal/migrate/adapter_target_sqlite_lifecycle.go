package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type sqliteTargetCatalogQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func openSQLiteTargetDatabase(
	ctx context.Context,
	databasePath string,
) (*sql.DB, error) {
	path, err := config.CanonicalSQLitePath(databasePath)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite target path: %w", err)
	}
	database, err := sql.Open("sqlite", sqliteTargetURI(path))
	if err != nil {
		return nil, fmt.Errorf("open SQLite target: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("open SQLite target: %w", err)
	}
	if err := requireSQLiteForeignKeys(ctx, database); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}

func sqliteTargetURI(path string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" &&
		!strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{
		Scheme: "file",
		Path:   normalized,
	}
	query := location.Query()
	query.Add("_pragma", "foreign_keys(1)")
	location.RawQuery = query.Encode()
	return location.String()
}

func requireSQLiteForeignKeys(
	ctx context.Context,
	database sqliteTargetCatalogQuerier,
) error {
	var enabled int
	if err := database.QueryRowContext(
		ctx,
		"PRAGMA foreign_keys",
	).Scan(&enabled); err != nil {
		return fmt.Errorf(
			"verify SQLite target foreign-key enforcement: %w",
			err,
		)
	}
	if enabled != 1 {
		return fmt.Errorf(
			"verify SQLite target foreign-key enforcement: PRAGMA foreign_keys returned %d, want 1",
			enabled,
		)
	}
	return nil
}

// PreflightDestructive enforces the rebuild backup gate against the selected
// live SQLite tables. PrepareTables repeats the populated check after taking
// SQLite's writer reservation, so a writer cannot populate an empty table in
// the gap between read-only preflight and the first DDL statement.
func (adapter *sqliteTargetAdapter) PreflightDestructive(
	ctx context.Context,
	targetTables []schema.Table,
	migration config.Migration,
) error {
	mode, err := normalizeAdapterTargetMode(migration.TargetMode)
	if err != nil {
		return err
	}
	adapter.destructiveAcknowledged = false
	if mode != "drop_recreate" {
		return nil
	}
	if migration.DestructiveAcknowledged {
		adapter.destructiveAcknowledged = true
		return nil
	}
	return requireUnpopulatedSQLiteTargets(
		ctx,
		adapter.database,
		targetTables,
	)
}

func requireUnpopulatedSQLiteTargets(
	ctx context.Context,
	database sqliteTargetCatalogQuerier,
	targetTables []schema.Table,
) error {
	for _, table := range targetTables {
		var exists int
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			   FROM sqlite_schema
			  WHERE type = 'table'
			    AND name = ? COLLATE BINARY`,
			table.Name,
		).Scan(&exists); err != nil {
			return fmt.Errorf(
				"inspect SQLite rebuild target %s: %w",
				table.Name,
				err,
			)
		}
		switch exists {
		case 0:
			continue
		case 1:
		default:
			return fmt.Errorf(
				"inspect SQLite rebuild target %s: catalog returned %d tables",
				table.Name,
				exists,
			)
		}
		var populated int
		if err := database.QueryRowContext(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM "+
				quote(table.Name)+" LIMIT 1)",
		).Scan(&populated); err != nil {
			return fmt.Errorf(
				"inspect SQLite rebuild target rows for %s: %w",
				table.Name,
				err,
			)
		}
		if populated != 0 {
			return fmt.Errorf(
				"%w: SQLite target table %q contains rows; rerun with --acknowledge-destructive",
				ErrDestructiveAcknowledgement,
				table.Name,
			)
		}
	}
	return nil
}

type sqliteTargetPlannedObject struct {
	kind  string
	name  string
	owner string
}

func preflightSQLiteTargetCatalog(
	ctx context.Context,
	database sqliteTargetCatalogQuerier,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if err := requireSQLiteForeignKeys(ctx, database); err != nil {
		return err
	}
	planned, selected, err := sqliteTargetPlannedObjects(targetTables)
	if err != nil {
		return err
	}

	rows, err := database.QueryContext(
		ctx,
		`SELECT type, name, tbl_name
		   FROM sqlite_schema
		  WHERE type IN ('table', 'index', 'view', 'trigger')
		    AND lower(name) NOT LIKE 'sqlite_%'
		  ORDER BY lower(name), type, name`,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQLite target object namespace: %w",
			err,
		)
	}
	type liveObject struct {
		kind  string
		name  string
		owner string
	}
	live := make([]liveObject, 0)
	for rows.Next() {
		var object liveObject
		if err := rows.Scan(
			&object.kind,
			&object.name,
			&object.owner,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf(
				"read SQLite target object namespace: %w",
				err,
			)
		}
		live = append(live, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf(
			"iterate SQLite target object namespace: %w",
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf(
			"close SQLite target object namespace: %w",
			err,
		)
	}

	for _, object := range live {
		key := strings.ToLower(object.name)
		expected, plannedName := planned[key]
		if plannedName {
			switch expected.kind {
			case "table":
				if object.kind != "table" ||
					object.name != expected.name ||
					object.owner != expected.owner {
					return sqliteTargetLifecyclePolicy(
						"preflight SQLite object name",
						object.kind+" "+object.name+
							" collides with table "+
							expected.name,
					)
				}
			case "index":
				ownerSelected := selected[strings.ToLower(object.owner)]
				if object.kind != "index" ||
					object.name != expected.name ||
					mode == "upsert" &&
						object.owner != expected.owner ||
					mode == "drop_recreate" &&
						!ownerSelected {
					return sqliteTargetLifecyclePolicy(
						"preflight SQLite object name",
						object.kind+" "+object.name+
							" collides with index "+
							expected.name,
					)
				}
			}
		}
		if mode != "drop_recreate" {
			continue
		}
		if object.kind == "view" {
			return sqliteTargetLifecyclePolicy(
				"replace SQLite tables with an unproven view dependency",
				object.name,
			)
		}
		if object.kind == "trigger" &&
			!selected[strings.ToLower(object.owner)] {
			return sqliteTargetLifecyclePolicy(
				"replace SQLite tables with an unproven trigger dependency",
				object.name,
			)
		}
	}

	if mode != "drop_recreate" {
		return nil
	}
	for _, object := range live {
		if object.kind != "table" ||
			selected[strings.ToLower(object.name)] {
			continue
		}
		if err := rejectSQLiteExternalForeignKeyDependency(
			ctx,
			database,
			object.name,
			selected,
		); err != nil {
			return err
		}
	}
	return nil
}

func sqliteTargetPlannedObjects(
	tables []schema.Table,
) (map[string]sqliteTargetPlannedObject, map[string]bool, error) {
	planned := make(
		map[string]sqliteTargetPlannedObject,
		len(tables),
	)
	selected := make(map[string]bool, len(tables))
	add := func(object sqliteTargetPlannedObject) error {
		if strings.TrimSpace(object.name) == "" {
			return sqliteTargetLifecyclePolicy(
				"plan SQLite object name",
				"empty "+object.kind+" name",
			)
		}
		if strings.HasPrefix(
			strings.ToLower(object.name),
			"sqlite_",
		) {
			return sqliteTargetLifecyclePolicy(
				"plan SQLite reserved object name",
				object.name,
			)
		}
		key := strings.ToLower(object.name)
		if earlier, exists := planned[key]; exists {
			return sqliteTargetLifecyclePolicy(
				"plan SQLite object names",
				earlier.kind+" "+earlier.name+
					" collides with "+object.kind+" "+
					object.name,
			)
		}
		planned[key] = object
		return nil
	}
	for _, table := range tables {
		if table.Schema != "" ||
			table.MySQLCollation != "" ||
			len(table.ClickHouseOrderBy) != 0 {
			return nil, nil, sqliteTargetLifecyclePolicy(
				"plan SQLite table metadata",
				table.Name,
			)
		}
		tableObject := sqliteTargetPlannedObject{
			kind:  "table",
			name:  table.Name,
			owner: table.Name,
		}
		if err := add(tableObject); err != nil {
			return nil, nil, err
		}
		selected[strings.ToLower(table.Name)] = true
		for _, index := range table.Indexes {
			if index.Inline {
				if index.Name != "" {
					return nil, nil, sqliteTargetLifecyclePolicy(
						"plan named SQLite inline index",
						table.Name+"."+index.Name,
					)
				}
				continue
			}
			if err := add(sqliteTargetPlannedObject{
				kind:  "index",
				name:  index.Name,
				owner: table.Name,
			}); err != nil {
				return nil, nil, err
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if foreignKey.Name != "" {
				return nil, nil, sqliteTargetLifecyclePolicy(
					"plan named SQLite foreign key",
					table.Name+"."+foreignKey.Name,
				)
			}
		}
		for _, check := range table.Checks {
			if check.Name != "" {
				return nil, nil, sqliteTargetLifecyclePolicy(
					"plan named SQLite CHECK",
					table.Name+"."+check.Name,
				)
			}
		}
	}
	return planned, selected, nil
}

func rejectSQLiteExternalForeignKeyDependency(
	ctx context.Context,
	database sqliteTargetCatalogQuerier,
	table string,
	selected map[string]bool,
) error {
	rows, err := database.QueryContext(
		ctx,
		"PRAGMA foreign_key_list("+quote(table)+")",
	)
	if err != nil {
		return fmt.Errorf(
			"inspect SQLite foreign keys for unselected table %s: %w",
			table,
			err,
		)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, sequence                int
			referenced, local, onUpdate string
			onDelete, match             string
			referencedColumn            sql.NullString
		)
		if err := rows.Scan(
			&id,
			&sequence,
			&referenced,
			&local,
			&referencedColumn,
			&onUpdate,
			&onDelete,
			&match,
		); err != nil {
			return fmt.Errorf(
				"read SQLite foreign keys for unselected table %s: %w",
				table,
				err,
			)
		}
		if selected[strings.ToLower(referenced)] {
			return sqliteTargetLifecyclePolicy(
				"replace SQLite table with an external foreign-key dependency",
				table+" references "+referenced,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQLite foreign keys for unselected table %s: %w",
			table,
			err,
		)
	}
	return nil
}

type sqliteTargetPreparation struct {
	table  schema.Table
	drop   string
	create string
}

func planSQLiteTargetPreparation(
	targetTables []schema.Table,
) ([]sqliteTargetPreparation, error) {
	// Creation must be parent-before-child while foreign-key enforcement is
	// active.  prepareDropRecreate executes the resulting plan in reverse for
	// drops, then forward for creates, so a populated child never blocks its
	// parent's drop and no selected table is recreated before the complete old
	// set has been removed.
	ordered, err := orderAdapterSourceTablesForMode(targetTables, "upsert")
	if err != nil {
		return nil, fmt.Errorf("order SQLite target preparation: %w", err)
	}
	preparation := make(
		[]sqliteTargetPreparation,
		len(ordered),
	)
	for index, table := range ordered {
		drop, err := schema.DropTable(schema.SQLite, table)
		if err != nil {
			return nil, fmt.Errorf(
				"prepare SQLite table %s drop: %w",
				table.Name,
				err,
			)
		}
		create, err := schema.CreateTable(schema.SQLite, table)
		if err != nil {
			return nil, fmt.Errorf(
				"prepare SQLite table %s create: %w",
				table.Name,
				err,
			)
		}
		preparation[index] = sqliteTargetPreparation{
			table:  table,
			drop:   drop,
			create: create,
		}
	}
	return preparation, nil
}

func (adapter *sqliteTargetAdapter) prepareDropRecreate(
	ctx context.Context,
	targetTables []schema.Table,
) error {
	preparation, err := planSQLiteTargetPreparation(targetTables)
	if err != nil {
		return err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return fmt.Errorf(
			"reserve SQLite target preparation connection: %w",
			err,
		)
	}
	defer connection.Close()
	if err := requireSQLiteForeignKeys(ctx, connection); err != nil {
		return err
	}
	if _, err := connection.ExecContext(
		ctx,
		"BEGIN IMMEDIATE",
	); err != nil {
		return fmt.Errorf(
			"begin SQLite target preparation: %w",
			err,
		)
	}
	active := true
	defer func() {
		if active {
			_, _ = connection.ExecContext(
				context.Background(),
				"ROLLBACK",
			)
		}
	}()
	rollback := func(cause error) error {
		active = false
		if _, rollbackErr := connection.ExecContext(
			context.Background(),
			"ROLLBACK",
		); rollbackErr != nil {
			return fmt.Errorf(
				"%w; SQLite target preparation rollback failed: %v; target state is uncertain and rerunning drop_recreate mode is the recovery path",
				cause,
				rollbackErr,
			)
		}
		return fmt.Errorf(
			"%w; SQLite target preparation transaction rolled back without target changes",
			cause,
		)
	}

	if _, err := connection.ExecContext(
		ctx,
		"PRAGMA defer_foreign_keys = ON",
	); err != nil {
		return rollback(fmt.Errorf(
			"defer SQLite target foreign keys: %w",
			err,
		))
	}
	if err := preflightSQLiteTargetCatalog(
		ctx,
		connection,
		targetTables,
		"drop_recreate",
	); err != nil {
		return rollback(fmt.Errorf(
			"revalidate SQLite target catalog under writer reservation: %w",
			err,
		))
	}
	if !adapter.destructiveAcknowledged {
		if err := requireUnpopulatedSQLiteTargets(
			ctx,
			connection,
			targetTables,
		); err != nil {
			return rollback(err)
		}
	}
	// Drop in reverse dependency order, then recreate in forward dependency
	// order below.  Both loops remain set-wide, preserving the Stage 4 rebuild
	// lifecycle: no CREATE is issued until every selected table was dropped.
	for index := len(preparation) - 1; index >= 0; index-- {
		item := preparation[index]
		if _, err := connection.ExecContext(
			ctx,
			item.drop,
		); err != nil {
			return rollback(fmt.Errorf(
				"drop SQLite table %s: %w",
				item.table.Name,
				err,
			))
		}
	}
	for _, item := range preparation {
		if _, err := connection.ExecContext(
			ctx,
			item.create,
		); err != nil {
			return rollback(fmt.Errorf(
				"create SQLite table %s: %w",
				item.table.Name,
				err,
			))
		}
	}
	if err := preflightSQLiteForeignKeyIntegrity(
		ctx,
		connection,
		"",
	); err != nil {
		return rollback(err)
	}
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		active = false
		_, rollbackErr := connection.ExecContext(
			context.Background(),
			"ROLLBACK",
		)
		if rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf(
				"rollback after failed SQLite preparation commit: %w",
				rollbackErr,
			))
		}
		return fmt.Errorf(
			"commit SQLite target preparation: %w; commit outcome is uncertain and rerunning drop_recreate mode is the recovery path",
			err,
		)
	}
	active = false
	return nil
}

func preflightSQLiteForeignKeyIntegrity(
	ctx context.Context,
	database sqliteTargetCatalogQuerier,
	table string,
) error {
	query := "PRAGMA foreign_key_check"
	label := "target"
	if table != "" {
		query += "(" + quote(table) + ")"
		label = "table " + table
	}
	rows, err := database.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf(
			"verify SQLite foreign-key integrity for %s: %w",
			label,
			err,
		)
	}
	defer rows.Close()
	if rows.Next() {
		return sqliteTargetLifecyclePolicy(
			"verify SQLite foreign-key integrity",
			label+" contains a violation",
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate SQLite foreign-key integrity for %s: %w",
			label,
			err,
		)
	}
	return nil
}

func validateSQLiteRetainedTable(
	planned schema.Table,
	discovered schema.Table,
) error {
	if planned.Name != discovered.Name ||
		planned.Schema != "" ||
		discovered.Schema != "" ||
		planned.SQLiteStrict != discovered.SQLiteStrict ||
		planned.SQLiteWithoutRowID != discovered.SQLiteWithoutRowID ||
		len(planned.Columns) != len(discovered.Columns) {
		return sqliteTargetLifecyclePolicy(
			"validate retained SQLite table shape",
			planned.Name,
		)
	}
	for index := range planned.Columns {
		left := planned.Columns[index]
		right := discovered.Columns[index]
		identityColumn :=
			planned.Identity != nil &&
				planned.Identity.Column == left.Name ||
				discovered.Identity != nil &&
					discovered.Identity.Column == right.Name
		leftType := &schema.DeclaredType{Base: "integer"}
		if !identityColumn {
			var err error
			leftType, err = sqliteEffectivePlannedDeclaredType(
				planned,
				left,
			)
			if err != nil {
				return fmt.Errorf(
					"validate retained SQLite column %s.%s type: %w",
					planned.Name,
					left.Name,
					err,
				)
			}
		}
		if left.Name != right.Name ||
			!identityColumn &&
				left.Nullable != right.Nullable ||
			left.PrimaryKey != right.PrimaryKey ||
			left.PrimaryKeyPosition != right.PrimaryKeyPosition ||
			!sameSQLiteDeclaredType(
				leftType,
				right.DeclaredType,
			) ||
			!sameSQLiteExpression(left.Default, right.Default) {
			return sqliteTargetLifecyclePolicy(
				"validate retained SQLite column",
				planned.Name+"."+left.Name,
			)
		}
	}
	if !sameSQLiteIdentityShape(
		planned.Identity,
		discovered.Identity,
	) {
		return sqliteTargetLifecyclePolicy(
			"validate retained SQLite identity",
			planned.Name,
		)
	}
	if !sameSQLiteIndexSet(planned.Indexes, discovered.Indexes) {
		return sqliteTargetLifecyclePolicy(
			"validate retained SQLite indexes",
			planned.Name,
		)
	}
	if !sameSQLiteForeignKeySet(
		planned.ForeignKeys,
		discovered.ForeignKeys,
	) {
		return sqliteTargetLifecyclePolicy(
			"validate retained SQLite foreign keys",
			planned.Name,
		)
	}
	if !sameSQLiteCheckSet(planned.Checks, discovered.Checks) {
		return sqliteTargetLifecyclePolicy(
			"validate retained SQLite CHECK constraints",
			planned.Name,
		)
	}
	return nil
}

func sqliteEffectivePlannedDeclaredType(
	table schema.Table,
	column schema.Column,
) (*schema.DeclaredType, error) {
	if table.Identity != nil &&
		table.Identity.Column == column.Name {
		return &schema.DeclaredType{Base: "integer"}, nil
	}
	if column.DeclaredType != nil {
		declaration := *column.DeclaredType
		declaration.Arguments = append(
			[]int(nil),
			column.DeclaredType.Arguments...,
		)
		return &declaration, nil
	}
	rendered, err := schema.MapType(column.Type, schema.SQLite)
	if err != nil {
		return nil, err
	}
	return schema.ParseSQLiteDeclaredType(rendered)
}

func sameSQLiteExpression(
	left *schema.Expression,
	right *schema.Expression,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.CanonicalSQL() == right.CanonicalSQL()
}

func sameSQLiteIndexSet(
	left []schema.Index,
	right []schema.Index,
) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]schema.Index(nil), left...)
	right = append([]schema.Index(nil), right...)
	sort.Slice(left, func(first, second int) bool {
		return sqliteRetainedIndexKey(left[first]) <
			sqliteRetainedIndexKey(left[second])
	})
	sort.Slice(right, func(first, second int) bool {
		return sqliteRetainedIndexKey(right[first]) <
			sqliteRetainedIndexKey(right[second])
	})
	for index := range left {
		if sqliteRetainedIndexKey(left[index]) !=
			sqliteRetainedIndexKey(right[index]) {
			return false
		}
	}
	return true
}

func sqliteRetainedIndexKey(index schema.Index) string {
	parts := []string{
		index.Name,
		fmt.Sprintf("%t", index.Unique),
		fmt.Sprintf("%t", index.Inline),
	}
	for _, column := range index.Columns {
		collation := strings.ToUpper(
			strings.TrimSpace(column.Collation),
		)
		if collation == "" {
			collation = "BINARY"
		}
		parts = append(
			parts,
			column.Name,
			fmt.Sprintf("%t", column.Descending),
			collation,
		)
	}
	return strings.Join(parts, "\x00")
}

func sameSQLiteForeignKeySet(
	left []schema.ForeignKey,
	right []schema.ForeignKey,
) bool {
	if len(left) != len(right) {
		return false
	}
	leftKeys := make([]string, len(left))
	rightKeys := make([]string, len(right))
	for index := range left {
		leftKeys[index] = sqliteRetainedForeignKeyKey(left[index])
		rightKeys[index] = sqliteRetainedForeignKeyKey(right[index])
	}
	sort.Strings(leftKeys)
	sort.Strings(rightKeys)
	for index := range leftKeys {
		if leftKeys[index] != rightKeys[index] {
			return false
		}
	}
	return true
}

func sqliteRetainedForeignKeyKey(
	foreignKey schema.ForeignKey,
) string {
	action := func(value string) string {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			return "NO ACTION"
		}
		return value
	}
	match := strings.ToUpper(strings.TrimSpace(foreignKey.Match))
	if match == "" {
		match = "NONE"
	}
	parts := append([]string(nil), foreignKey.Columns...)
	parts = append(
		parts,
		foreignKey.ReferencedTable,
	)
	parts = append(parts, foreignKey.ReferencedColumns...)
	parts = append(
		parts,
		action(foreignKey.OnUpdate),
		action(foreignKey.OnDelete),
		match,
	)
	return strings.Join(parts, "\x00")
}

func sameSQLiteCheckSet(
	left []schema.CheckConstraint,
	right []schema.CheckConstraint,
) bool {
	if len(left) != len(right) {
		return false
	}
	leftKeys := make([]string, len(left))
	rightKeys := make([]string, len(right))
	for index := range left {
		leftKeys[index] = left[index].Expression.CanonicalSQL()
		rightKeys[index] = right[index].Expression.CanonicalSQL()
	}
	sort.Strings(leftKeys)
	sort.Strings(rightKeys)
	for index := range leftKeys {
		if leftKeys[index] != rightKeys[index] {
			return false
		}
	}
	return true
}

func sqliteTargetLifecyclePolicy(
	operation string,
	value string,
) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLite),
	}
}
