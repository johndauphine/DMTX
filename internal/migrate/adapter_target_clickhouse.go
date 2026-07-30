package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type clickHouseTargetAdapter struct {
	database    *sql.DB
	batchWriter clickHouseBatchWriter
	namespace   string
}

var _ targetAdapter = (*clickHouseTargetAdapter)(nil)

func (adapter *clickHouseTargetAdapter) clickHouseDatabaseHandle() *sql.DB {
	if adapter == nil {
		return nil
	}
	return adapter.database
}

func validateClickHouseTargetEndpoint(endpoint config.Endpoint) error {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.User == "" {
		return fmt.Errorf(
			"ClickHouse host, database, and user are required",
		)
	}
	switch strings.ToLower(endpoint.Database) {
	case "system", "information_schema":
		return fmt.Errorf(
			"ClickHouse target database %q is a reserved system database",
			endpoint.Database,
		)
	}
	if endpoint.Schema != "" && endpoint.Schema != endpoint.Database {
		return fmt.Errorf(
			"ClickHouse target schema must be empty or match the target database",
		)
	}
	return nil
}

func openClickHouseTargetAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (targetAdapter, error) {
	if err := validateClickHouseTargetEndpoint(endpoint); err != nil {
		return nil, err
	}
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolve ClickHouse target: %w", err)
	}
	database, err := engine.OpenClickHouse(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if err := engine.VerifyClickHouse248Target(
		ctx,
		database,
		resolved.Database,
	); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"%w (close ClickHouse target: %v)",
				err,
				closeErr,
			)
		}
		return nil, err
	}
	return &clickHouseTargetAdapter{
		database:    database,
		batchWriter: newClickHouseNativeWriter(database),
		namespace:   resolved.Database,
	}, nil
}

func (adapter *clickHouseTargetAdapter) Engine() string {
	return "clickhouse"
}

func (adapter *clickHouseTargetAdapter) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return nil, err
	}
	if mode != "drop_recreate" {
		return nil, clickHouseTargetPolicy(
			"map target mode",
			mode,
		)
	}
	if sourceEngine != "sqlite" && sourceEngine != "clickhouse" {
		return nil, fmt.Errorf(
			"ClickHouse target does not support source engine %q",
			sourceEngine,
		)
	}
	if adapter.namespace == "" {
		return nil, fmt.Errorf(
			"ClickHouse target database is not configured",
		)
	}

	targetTables := make([]schema.Table, 0, len(sourceTables))
	for _, sourceTable := range sourceTables {
		var targetTable schema.Table
		switch sourceEngine {
		case "sqlite":
			targetTable, err = projectSQLiteTableForClickHouse(sourceTable)
		case "clickhouse":
			targetTable, err = projectClickHouseTableForClickHouse(sourceTable)
		}
		if err != nil {
			return nil, err
		}
		targetTable.Schema = adapter.namespace
		if _, err := schema.DropTable(
			schema.ClickHouse,
			targetTable,
		); err != nil {
			return nil, fmt.Errorf(
				"plan ClickHouse table %s drop: %w",
				targetTable.Name,
				err,
			)
		}
		if _, err := schema.CreateTable(
			schema.ClickHouse,
			targetTable,
		); err != nil {
			return nil, fmt.Errorf(
				"plan ClickHouse table %s: %w",
				targetTable.Name,
				err,
			)
		}
		if err := validateClickHouseWriteShape(
			targetTable,
			adapterColumnNames(targetTable),
			mode,
		); err != nil {
			return nil, err
		}
		targetTables = append(targetTables, targetTable)
	}
	return targetTables, nil
}

func projectSQLiteTableForClickHouse(
	source schema.Table,
) (schema.Table, error) {
	if !source.SQLiteStrict {
		return schema.Table{}, clickHouseTargetPolicy(
			"map non-STRICT SQLite table",
			source.Name,
		)
	}
	if source.Identity != nil {
		return schema.Table{}, clickHouseTargetPolicy(
			"map SQLite identity",
			source.Name,
		)
	}
	if len(source.Indexes) > 0 {
		return schema.Table{}, clickHouseTargetPolicy(
			"map SQLite indexes",
			source.Name,
		)
	}
	if len(source.ForeignKeys) > 0 {
		return schema.Table{}, clickHouseTargetPolicy(
			"map SQLite foreign keys",
			source.Name,
		)
	}
	if len(source.Checks) > 0 {
		return schema.Table{}, clickHouseTargetPolicy(
			"map SQLite CHECK constraints",
			source.Name,
		)
	}

	target := source
	target.ClickHouseOrderBy = primaryKeyColumns(source)
	if len(target.ClickHouseOrderBy) == 0 {
		return schema.Table{}, clickHouseTargetPolicy(
			"map SQLite ordering key",
			source.Name,
		)
	}
	target.Identity = nil
	target.Indexes = nil
	target.ForeignKeys = nil
	target.Checks = nil
	target.SQLiteStrict = false
	target.SQLiteWithoutRowID = false
	target.Columns = make([]schema.Column, len(source.Columns))
	for index, sourceColumn := range source.Columns {
		if sourceColumn.Default != nil {
			return schema.Table{}, clickHouseTargetPolicy(
				"map SQLite default",
				source.Name+"."+sourceColumn.Name,
			)
		}
		if sourceColumn.DeclaredType == nil ||
			len(sourceColumn.DeclaredType.Arguments) != 0 {
			return schema.Table{}, clickHouseTargetPolicy(
				"map SQLite declared type",
				source.Name+"."+sourceColumn.Name,
			)
		}
		targetColumn := sourceColumn
		targetColumn.DeclaredType = nil
		targetColumn.Default = nil
		switch sourceColumn.DeclaredType.Base {
		case "int", "integer":
			targetColumn.Type = "bigint"
		case "real":
			targetColumn.Type = "double"
		case "text":
			targetColumn.Type = "text"
		case "blob":
			targetColumn.Type = "blob"
		default:
			return schema.Table{}, clickHouseTargetPolicy(
				"map SQLite declared type",
				source.Name+"."+sourceColumn.Name+
					" "+sourceColumn.DeclaredType.Base,
			)
		}
		// SQLite INTEGER PRIMARY KEY columns are reported nullable by PRAGMA,
		// but every stored key is non-NULL. ClickHouse ordering keys are
		// deliberately non-nullable under the admitted server settings.
		if targetColumn.PrimaryKey {
			targetColumn.Nullable = false
		}
		targetColumn.PrimaryKey = false
		targetColumn.PrimaryKeyPosition = 0
		target.Columns[index] = targetColumn
	}
	return target, nil
}

func projectClickHouseTableForClickHouse(
	source schema.Table,
) (schema.Table, error) {
	if source.MySQLCollation != "" ||
		source.Identity != nil ||
		len(source.Indexes) != 0 ||
		len(source.ForeignKeys) != 0 ||
		len(source.Checks) != 0 ||
		source.SQLiteStrict ||
		source.SQLiteWithoutRowID {
		return schema.Table{}, clickHouseTargetPolicy(
			"rebuild ClickHouse source metadata",
			source.Name,
		)
	}
	if len(source.ClickHouseOrderBy) == 0 {
		return schema.Table{}, clickHouseTargetPolicy(
			"rebuild ClickHouse ordering key",
			source.Name,
		)
	}
	columns := make(map[string]schema.Column, len(source.Columns))
	target := source
	target.Columns = make([]schema.Column, len(source.Columns))
	for index, column := range source.Columns {
		if column.Name == "" {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse empty column",
				source.Name,
			)
		}
		if _, duplicate := columns[column.Name]; duplicate {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse duplicate column",
				source.Name+"."+column.Name,
			)
		}
		if column.PrimaryKey ||
			column.PrimaryKeyPosition != 0 ||
			column.DeclaredType != nil ||
			column.Default != nil {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse column metadata",
				source.Name+"."+column.Name,
			)
		}
		switch strings.ToLower(strings.TrimSpace(column.Type)) {
		case "bigint", "double", "text":
		default:
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse column type",
				source.Name+"."+column.Name+" "+column.Type,
			)
		}
		columns[column.Name] = column
		target.Columns[index] = column
	}
	seen := make(map[string]struct{}, len(source.ClickHouseOrderBy))
	target.ClickHouseOrderBy = append(
		[]string(nil),
		source.ClickHouseOrderBy...,
	)
	for _, name := range target.ClickHouseOrderBy {
		column, exists := columns[name]
		if !exists {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse ordering column",
				source.Name+"."+name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse duplicate ordering column",
				source.Name+"."+name,
			)
		}
		if column.Nullable {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse nullable ordering column",
				source.Name+"."+name,
			)
		}
		if strings.EqualFold(
			strings.TrimSpace(column.Type),
			"double",
		) {
			return schema.Table{}, clickHouseTargetPolicy(
				"rebuild ClickHouse floating-point ordering column",
				source.Name+"."+name,
			)
		}
		seen[name] = struct{}{}
	}
	return target, nil
}

func clickHouseTargetPolicy(operation, typ string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      typ,
		Target:    string(schema.ClickHouse),
	}
}

func (adapter *clickHouseTargetAdapter) PreflightTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if mode != "drop_recreate" {
		return clickHouseTargetPolicy("preflight target mode", mode)
	}
	for _, table := range targetTables {
		var engineName string
		var dependenciesDatabase, dependenciesTable []string
		err := adapter.database.QueryRowContext(
			ctx,
			`SELECT
				engine,
				dependencies_database,
				dependencies_table
			 FROM system.tables
			 WHERE database = ? AND name = ?`,
			table.Schema,
			table.Name,
		).Scan(
			&engineName,
			&dependenciesDatabase,
			&dependenciesTable,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf(
				"preflight ClickHouse table %s: %w",
				table.Name,
				err,
			)
		}
		if engineName != "MergeTree" {
			return clickHouseTargetPolicy(
				"replace existing target engine",
				table.Name+" "+engineName,
			)
		}
		if len(dependenciesDatabase) != 0 ||
			len(dependenciesTable) != 0 {
			return clickHouseTargetPolicy(
				"replace target with dependent objects",
				table.Name,
			)
		}
	}
	return nil
}

func (adapter *clickHouseTargetAdapter) PreflightDestructive(
	ctx context.Context,
	targetTables []schema.Table,
	migration config.Migration,
) error {
	mode, err := normalizeAdapterTargetMode(migration.TargetMode)
	if err != nil {
		return err
	}
	if mode != "drop_recreate" ||
		migration.DestructiveAcknowledged {
		return nil
	}
	for _, table := range targetTables {
		var exists uint64
		if err := adapter.database.QueryRowContext(
			ctx,
			`SELECT count()
			   FROM system.tables
			  WHERE database = ? AND name = ?`,
			table.Schema,
			table.Name,
		).Scan(&exists); err != nil {
			return fmt.Errorf(
				"inspect ClickHouse rebuild target %s: %w",
				table.Name,
				err,
			)
		}
		if exists == 0 {
			continue
		}
		if exists != 1 {
			return fmt.Errorf(
				"inspect ClickHouse rebuild target %s: catalog returned %d tables",
				table.Name,
				exists,
			)
		}
		var rows uint64
		if err := adapter.database.QueryRowContext(
			ctx,
			"SELECT count() FROM "+
				clickHouseQualified(table.Schema, table.Name),
		).Scan(&rows); err != nil {
			return fmt.Errorf(
				"inspect ClickHouse rebuild target rows for %s: %w",
				table.Name,
				err,
			)
		}
		if rows != 0 {
			return fmt.Errorf(
				"%w: ClickHouse target table %q contains rows; rerun with --acknowledge-destructive",
				ErrDestructiveAcknowledgement,
				table.Name,
			)
		}
	}
	return nil
}

func (adapter *clickHouseTargetAdapter) PrepareTables(
	ctx context.Context,
	targetTables []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if mode != "drop_recreate" {
		return clickHouseTargetPolicy("prepare target mode", mode)
	}
	ordered := append([]schema.Table(nil), targetTables...)
	sort.Slice(ordered, func(left, right int) bool {
		return adapterSourceTableKey(
			ordered[left].Schema,
			ordered[left].Name,
		) < adapterSourceTableKey(
			ordered[right].Schema,
			ordered[right].Name,
		)
	})
	drops := make([]string, len(ordered))
	creates := make([]string, len(ordered))
	for index, table := range ordered {
		drop, err := schema.DropTable(schema.ClickHouse, table)
		if err != nil {
			return fmt.Errorf(
				"prepare ClickHouse table %s drop: %w",
				table.Name,
				err,
			)
		}
		drops[index] = drop
		create, err := schema.CreateTable(schema.ClickHouse, table)
		if err != nil {
			return fmt.Errorf(
				"prepare ClickHouse table %s create: %w",
				table.Name,
				err,
			)
		}
		creates[index] = create
	}
	for index, statement := range drops {
		if _, err := adapter.database.ExecContext(
			ctx,
			statement,
		); err != nil {
			if index == 0 {
				return fmt.Errorf(
					"drop ClickHouse table %s: %w",
					ordered[index].Name,
					err,
				)
			}
			return fmt.Errorf(
				"drop ClickHouse table %s: %w; target preparation is partial and rerunning drop_recreate mode is the recovery path",
				ordered[index].Name,
				err,
			)
		}
	}
	for index, statement := range creates {
		if _, err := adapter.database.ExecContext(
			ctx,
			statement,
		); err != nil {
			return fmt.Errorf(
				"create ClickHouse table %s: %w; selected target tables were dropped and rerunning drop_recreate mode is the recovery path",
				ordered[index].Name,
				err,
			)
		}
	}
	return nil
}

func (adapter *clickHouseTargetAdapter) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	if adapter.batchWriter == nil {
		return WriteReceipt{
				Certainty:     CommitNotCommitted,
				AttemptedRows: int64(len(rows)),
			}, fmt.Errorf(
				"ClickHouse native batch writer is not configured",
			)
	}
	return adapter.batchWriter.WriteBatch(
		ctx,
		table,
		columns,
		mode,
		rows,
	)
}

func (adapter *clickHouseTargetAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	if err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			clickHouseQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count ClickHouse table %s: %w",
			table.Name,
			err,
		)
	}
	return count, nil
}

func (adapter *clickHouseTargetAdapter) FinalizeTables(
	_ context.Context,
	_ []schema.Table,
	mode string,
) error {
	mode, err := normalizeAdapterTargetMode(mode)
	if err != nil {
		return err
	}
	if mode != "drop_recreate" {
		return clickHouseTargetPolicy("finalize target mode", mode)
	}
	return nil
}

func (adapter *clickHouseTargetAdapter) Close() error {
	return adapter.database.Close()
}

func clickHouseQualified(database, name string) string {
	return quote(database) + "." + quote(name)
}
