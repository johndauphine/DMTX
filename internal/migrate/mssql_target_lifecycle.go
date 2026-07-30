package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func prepareSQLServerTargets(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
	destructiveAcknowledged bool,
) (result error) {
	ordered := orderedSQLServerTargetTables(tables)
	drops := make([]string, len(ordered))
	creates := make([]string, len(ordered))
	for index, table := range ordered {
		var err error
		drops[index], err = schema.DropSQLServerTable(table)
		if err != nil {
			return fmt.Errorf(
				"plan SQL Server table %s drop: %w",
				table.Name,
				err,
			)
		}
		creates[index], err = schema.CreateSQLServerTable(table)
		if err != nil {
			return fmt.Errorf(
				"plan SQL Server table %s: %w",
				table.Name,
				err,
			)
		}
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		return newSQLServerSafeOperationError(
			"acquire SQL Server schema preparation connection for",
			"selected tables",
			err,
		)
	}
	defer connection.Close()

	transaction, err := connection.BeginTx(
		ctx,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		return newSQLServerSafeOperationError(
			"begin SQL Server schema preparation for",
			"selected tables",
			err,
		)
	}
	committed := false
	defer func() {
		if !committed {
			finishSQLServerLifecycleRollback(
				transaction,
				connection,
				"schema preparation",
				&result,
			)
		}
	}()

	// Recheck destructive dependencies inside the same serializable
	// transaction that performs the drops. SQL Server DDL is transactional, so
	// any later failure rolls the complete preparation back.
	if err := preflightSQLServerDropRecreate(
		ctx,
		transaction,
		ordered,
	); err != nil {
		return fmt.Errorf(
			"recheck SQL Server drop/recreate preflight: %w",
			err,
		)
	}
	var lockedExisting map[string]struct{}
	if !destructiveAcknowledged {
		lockedExisting, err = preflightSQLServerDestructiveRows(
			ctx,
			transaction,
			ordered,
		)
		if err != nil {
			return fmt.Errorf(
				"recheck SQL Server destructive acknowledgement: %w",
				err,
			)
		}
	}
	constraints, err := readSQLServerSelectedForeignKeyDrops(
		ctx,
		transaction,
		ordered,
	)
	if err != nil {
		return err
	}
	for _, statement := range constraints {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return newSQLServerSafeOperationError(
				"drop SQL Server selected foreign key on",
				"selected tables",
				err,
			)
		}
	}
	for index, statement := range drops {
		if !destructiveAcknowledged {
			if _, exists := lockedExisting[sqlServerFoldedName(
				ordered[index].Name,
			)]; !exists {
				// The name was absent while the serializable preparation
				// transaction inspected the catalog. Do not issue DROP IF
				// EXISTS: if another session creates that name now, CREATE
				// below must collide and roll the transaction back rather
				// than destroy an unacknowledged table.
				continue
			}
		}
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return newSQLServerSafeOperationError(
				"drop SQL Server table",
				ordered[index].Name,
				err,
			)
		}
	}
	for index, statement := range creates {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return newSQLServerSafeOperationError(
				"create SQL Server table",
				ordered[index].Name,
				err,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		discardSQLServerLifecycleConnection(connection)
		return unknownSQLServerLifecycleState(
			"SQL Server schema preparation commit",
			newSQLServerSafeOperationError(
				"commit SQL Server schema preparation for",
				"selected tables",
				err,
			),
		)
	}
	committed = true
	return nil
}

// preflightSQLServerDestructiveRows both rechecks and locks every existing
// selected table. TABLOCKX plus HOLDLOCK keeps an empty table empty until the
// surrounding serializable preparation transaction either rolls back or
// drops it, closing the acknowledgement race between the runner's read-only
// preflight and destructive DDL.
func preflightSQLServerDestructiveRows(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	tables []schema.Table,
) (map[string]struct{}, error) {
	lockedExisting := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		exists, err := sqlServerTargetBaseTableExists(
			ctx,
			queryer,
			table,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"inspect SQL Server rebuild target %s: %w",
				table.Name,
				err,
			)
		}
		if !exists {
			continue
		}
		lockedExisting[sqlServerFoldedName(table.Name)] = struct{}{}
		var marker int
		err = queryer.QueryRowContext(
			ctx,
			"SELECT TOP (1) 1 FROM "+
				sqlServerQualified(table.Schema, table.Name)+
				" WITH (TABLOCKX, HOLDLOCK)",
		).Scan(&marker)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, newSQLServerSafeOperationError(
				"inspect SQL Server rebuild target rows for",
				table.Name,
				err,
			)
		}
		return nil, fmt.Errorf(
			"%w: SQL Server target table %q contains rows; rerun with --acknowledge-destructive",
			ErrDestructiveAcknowledgement,
			table.Name,
		)
	}
	return lockedExisting, nil
}

func readSQLServerSelectedForeignKeyDrops(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	tables []schema.Table,
) ([]string, error) {
	selected, namespace, err := sqlServerSelectedTargetTables(tables)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, nil
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT
			parent_schema.name,
			parent_table.name,
			target_foreign_key.name
		   FROM sys.foreign_keys AS target_foreign_key
		   JOIN sys.tables AS parent_table
		     ON parent_table.object_id =
		        target_foreign_key.parent_object_id
		   JOIN sys.schemas AS parent_schema
		     ON parent_schema.schema_id = parent_table.schema_id
		  WHERE parent_schema.name = @p1
		  ORDER BY parent_table.name, target_foreign_key.name`,
		namespace,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"inspect SQL Server selected foreign keys: %w",
			err,
		)
	}
	defer rows.Close()
	var statements []string
	for rows.Next() {
		var parentSchema, parentTable, constraint string
		if err := rows.Scan(
			&parentSchema,
			&parentTable,
			&constraint,
		); err != nil {
			return nil, fmt.Errorf(
				"read SQL Server selected foreign key: %w",
				err,
			)
		}
		if _, planned := selected[sqlServerFoldedName(
			parentTable,
		)]; !planned {
			continue
		}
		statements = append(
			statements,
			"ALTER TABLE "+
				sqlServerQualified(parentSchema, parentTable)+
				" DROP CONSTRAINT "+
				sqlServerIdentifier(constraint),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate SQL Server selected foreign keys: %w",
			err,
		)
	}
	return statements, nil
}

func finalizeSQLServerTargets(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
	mode string,
) (result error) {
	var objects []schema.SQLServerObjectStatement
	if mode == "drop_recreate" {
		var err error
		objects, err = schema.PlanSQLServerDropRecreateObjects(tables)
		if err != nil {
			return fmt.Errorf(
				"plan SQL Server post-load objects: %w",
				err,
			)
		}
	}
	steps := sqlServerFinalizationSteps(objects, tables)

	connection, err := database.Conn(ctx)
	if err != nil {
		return newSQLServerSafeOperationError(
			"acquire SQL Server target finalization connection for",
			"selected tables",
			err,
		)
	}
	defer connection.Close()

	transaction, err := connection.BeginTx(
		ctx,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		return newSQLServerSafeOperationError(
			"begin SQL Server target finalization for",
			"selected tables",
			err,
		)
	}
	committed := false
	defer func() {
		if !committed {
			finishSQLServerLifecycleRollback(
				transaction,
				connection,
				"target finalization",
				&result,
			)
		}
	}()

	identityStateMayHaveChanged := false
	for _, step := range steps {
		if !step.identity {
			if _, err := transaction.ExecContext(
				ctx,
				step.object.SQL,
			); err != nil {
				return newSQLServerSafeOperationError(
					"create SQL Server post-load object on",
					step.object.Table,
					err,
				)
			}
			continue
		}

		mayHaveChanged, err := finalizeSQLServerIdentityFrontier(
			ctx,
			transaction,
			step.table,
			tables,
			mode,
		)
		identityStateMayHaveChanged = identityStateMayHaveChanged ||
			mayHaveChanged
		if err == nil {
			continue
		}
		if identityStateMayHaveChanged {
			return unknownSQLServerLifecycleState(
				"SQL Server identity finalization",
				err,
			)
		}
		return err
	}
	if err := transaction.Commit(); err != nil {
		discardSQLServerLifecycleConnection(connection)
		return unknownSQLServerLifecycleState(
			"SQL Server target finalization commit",
			newSQLServerSafeOperationError(
				"commit SQL Server target finalization for",
				"selected tables",
				err,
			),
		)
	}
	committed = true
	return nil
}

type sqlServerFinalizationStep struct {
	object   schema.SQLServerObjectStatement
	table    schema.Table
	identity bool
}

// sqlServerFinalizationSteps makes the safety ordering explicit: every
// fallible post-load DDL statement runs before any identity operation. An
// empty-table identity primer advances SQL Server's nontransactional identity
// frontier, so no index, CHECK, or foreign-key creation may follow it.
func sqlServerFinalizationSteps(
	objects []schema.SQLServerObjectStatement,
	tables []schema.Table,
) []sqlServerFinalizationStep {
	steps := make(
		[]sqlServerFinalizationStep,
		0,
		len(objects)+len(tables),
	)
	for _, object := range objects {
		steps = append(steps, sqlServerFinalizationStep{
			object: object,
		})
	}
	for _, table := range orderedSQLServerTargetTables(tables) {
		if table.Identity == nil {
			continue
		}
		steps = append(steps, sqlServerFinalizationStep{
			table:    table,
			identity: true,
		})
	}
	return steps
}

func finalizeSQLServerIdentityFrontier(
	ctx context.Context,
	transaction *sql.Tx,
	table schema.Table,
	tables []schema.Table,
	mode string,
) (bool, error) {
	if table.Identity == nil ||
		table.Identity.Column == "" ||
		table.Identity.Generation != schema.IdentityByDefault {
		return false, fmt.Errorf(
			"plan SQL Server table %s identity: invalid identity metadata",
			table.Name,
		)
	}
	var maximum sql.NullInt64
	if err := transaction.QueryRowContext(
		ctx,
		"SELECT MAX("+sqlServerIdentifier(table.Identity.Column)+
			") FROM "+
			sqlServerQualified(table.Schema, table.Name)+
			" WITH (TABLOCKX, HOLDLOCK)",
	).Scan(&maximum); err != nil {
		return false, newSQLServerSafeOperationError(
			"read SQL Server identity maximum for",
			table.Name,
			err,
		)
	}
	var current sql.NullInt64
	if err := transaction.QueryRowContext(
		ctx,
		`SELECT TRY_CONVERT(bigint, identity_column.last_value)
		   FROM sys.identity_columns AS identity_column
		   JOIN sys.tables AS identity_table
		     ON identity_table.object_id = identity_column.object_id
		   JOIN sys.schemas AS identity_schema
		     ON identity_schema.schema_id = identity_table.schema_id
		  WHERE identity_schema.name = @p1
		    AND identity_table.name = @p2
		    AND identity_column.name = @p3`,
		table.Schema,
		table.Name,
		table.Identity.Column,
	).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf(
				"finalize SQL Server identity for %s: planned identity column is missing",
				table.Name,
			)
		}
		return false, newSQLServerSafeOperationError(
			"read SQL Server identity frontier for",
			table.Name,
			err,
		)
	}

	desired, haveDesired, err := sqlServerDesiredIdentityFrontier(
		table,
		mode,
		maximum,
		current,
	)
	if err != nil {
		return false, err
	}
	if !haveDesired {
		return false, nil
	}
	if desired == math.MaxInt64 &&
		!maximum.Valid && !current.Valid {
		return false, fmt.Errorf(
			"finalize SQL Server identity for %s: exhausted empty identity cannot preserve its next-value failure safely",
			table.Name,
		)
	}

	if !maximum.Valid && !current.Valid {
		if mode != "drop_recreate" {
			return false, fmt.Errorf(
				"finalize SQL Server identity for %s: empty retained identity has ambiguous next-value semantics",
				table.Name,
			)
		}
		if err := validateSQLServerEmptyIdentityPrimer(
			table,
			tables,
		); err != nil {
			return false, err
		}
		return primeEmptySQLServerIdentity(
			ctx,
			transaction,
			table,
			desired,
		)
	}
	if current.Valid && current.Int64 == desired {
		return false, nil
	}
	return reseedSQLServerIdentity(
		ctx,
		transaction,
		table,
		desired,
	)
}

func sqlServerDesiredIdentityFrontier(
	table schema.Table,
	mode string,
	maximum sql.NullInt64,
	current sql.NullInt64,
) (int64, bool, error) {
	if table.Identity == nil {
		return 0, false, fmt.Errorf(
			"finalize SQL Server identity for %s: missing identity metadata",
			table.Name,
		)
	}
	if table.Identity.Frontier != nil &&
		*table.Identity.Frontier < 0 {
		return 0, false, fmt.Errorf(
			"finalize SQL Server identity for %s: negative source frontier is unsupported",
			table.Name,
		)
	}

	if mode == "drop_recreate" {
		if table.Identity.Frontier != nil {
			// The neutral frontier is the greatest value allocated by the
			// source generator. Explicit source rows may legitimately be
			// higher, so neither their MAX nor SQL Server's IDENTITY_INSERT
			// side effect may raise it.
			return *table.Identity.Frontier, true, nil
		}
		if maximum.Valid || current.Valid {
			// A nil neutral frontier means the source generator is uncalled.
			// Explicit migrated keys must not change its next generated value.
			return 0, true, nil
		}
		return 0, false, nil
	}
	if mode != "upsert" {
		return 0, false, fmt.Errorf(
			"finalize SQL Server identity for %s: unsupported target mode %q",
			table.Name,
			mode,
		)
	}

	// Upsert retains target rows and generator history. Lowering either state
	// to the source frontier could reuse a value previously generated on the
	// retained target, so conservatively keep the greatest observed state.
	desired := int64(0)
	haveDesired := false
	if table.Identity.Frontier != nil {
		desired = *table.Identity.Frontier
		haveDesired = true
	}
	for _, candidate := range []sql.NullInt64{maximum, current} {
		if !candidate.Valid {
			continue
		}
		haveDesired = true
		if candidate.Int64 > desired {
			desired = candidate.Int64
		}
	}
	return desired, haveDesired, nil
}

func validateSQLServerEmptyIdentityPrimer(
	table schema.Table,
	tables []schema.Table,
) error {
	if table.Identity == nil {
		return fmt.Errorf(
			"plan SQL Server empty identity primer for %s: missing identity metadata",
			table.Name,
		)
	}
	if len(table.Checks) > 0 {
		return fmt.Errorf(
			"plan SQL Server empty identity primer for %s: CHECK constraints make a synthetic row unprovable",
			table.Name,
		)
	}
	if len(table.ForeignKeys) > 0 {
		return fmt.Errorf(
			"plan SQL Server empty identity primer for %s: foreign keys make a synthetic row unprovable",
			table.Name,
		)
	}
	for _, candidate := range tables {
		for _, foreignKey := range candidate.ForeignKeys {
			if strings.EqualFold(
				foreignKey.ReferencedTable,
				table.Name,
			) {
				return fmt.Errorf(
					"plan SQL Server empty identity primer for %s: incoming foreign key %s makes primer removal unprovable",
					table.Name,
					foreignKey.Name,
				)
			}
		}
	}
	if _, _, err := sqlServerIdentityPrimerInsert(table); err != nil {
		return err
	}
	return nil
}

func primeEmptySQLServerIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	table schema.Table,
	frontier int64,
) (bool, error) {
	statement, arguments, err := sqlServerIdentityPrimerInsert(
		table,
	)
	if err != nil {
		return false, err
	}
	mayHaveChanged, err := reseedSQLServerIdentity(
		ctx,
		transaction,
		table,
		frontier,
	)
	if err != nil {
		return mayHaveChanged, err
	}
	result, err := transaction.ExecContext(ctx, statement, arguments...)
	if err != nil {
		return true, newSQLServerSafeOperationError(
			"prime empty SQL Server identity for",
			table.Name,
			err,
		)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return true, fmt.Errorf(
			"prime empty SQL Server identity for %s: inserted %d rows; expected 1",
			table.Name,
			affected,
		)
	}
	result, err = transaction.ExecContext(
		ctx,
		"DELETE FROM "+
			sqlServerQualified(table.Schema, table.Name)+
			" WHERE "+sqlServerIdentifier(table.Identity.Column)+
			" = @p1",
		frontier,
	)
	if err != nil {
		return true, newSQLServerSafeOperationError(
			"remove SQL Server identity primer from",
			table.Name,
			err,
		)
	}
	affected, err = result.RowsAffected()
	if err != nil || affected != 1 {
		return true, fmt.Errorf(
			"remove SQL Server identity primer from %s: deleted %d rows; expected 1",
			table.Name,
			affected,
		)
	}
	return true, nil
}

func reseedSQLServerIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	table schema.Table,
	frontier int64,
) (bool, error) {
	qualified := sqlServerQualified(table.Schema, table.Name)
	literal := strings.ReplaceAll(qualified, "'", "''")
	statement := "DBCC CHECKIDENT (N'" + literal +
		"', RESEED, " + strconv.FormatInt(frontier, 10) +
		") WITH NO_INFOMSGS"
	if _, err := transaction.ExecContext(ctx, statement); err != nil {
		// A lost response does not prove whether DBCC CHECKIDENT applied its
		// nontransactional identity side effect.
		return true, newSQLServerSafeOperationError(
			"reseed SQL Server identity for",
			table.Name,
			err,
		)
	}
	return true, nil
}

func sqlServerIdentityPrimerInsert(
	table schema.Table,
) (string, []any, error) {
	columns := make([]string, 0, len(table.Columns)-1)
	arguments := make([]any, 0, len(table.Columns)-1)
	for _, column := range table.Columns {
		if column.Name == table.Identity.Column ||
			column.Nullable ||
			sqlServerIdentityPrimerCanUseDefault(column) {
			continue
		}
		value, err := sqlServerIdentityPrimerValue(column)
		if err != nil {
			return "", nil, fmt.Errorf(
				"plan SQL Server identity primer for %s.%s: %w",
				table.Name,
				column.Name,
				err,
			)
		}
		columns = append(columns, column.Name)
		arguments = append(arguments, value)
	}
	if len(columns) == 0 {
		return "INSERT INTO " +
			sqlServerQualified(table.Schema, table.Name) +
			" DEFAULT VALUES", nil, nil
	}
	return "INSERT INTO " +
		sqlServerQualified(table.Schema, table.Name) +
		" (" + sqlServerQuotedColumns(columns) + ") VALUES (" +
		sqlServerPlaceholders(len(columns)) + ")", arguments, nil
}

func sqlServerIdentityPrimerCanUseDefault(column schema.Column) bool {
	if column.Default == nil {
		return false
	}
	// SQL Server admits a DEFAULT NULL constraint on a NOT NULL column, but
	// applying that default still violates the column constraint. Supply the
	// same explicit type-safe primer value used when there is no default.
	return !strings.EqualFold(
		strings.TrimSpace(column.Default.CanonicalSQL()),
		"NULL",
	)
}

func sqlServerIdentityPrimerValue(column schema.Column) (any, error) {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(
			column.DeclaredType.Base,
		))
	}
	switch base {
	case "tinyint", "smallint", "int", "integer", "bigint":
		return int64(0), nil
	case "decimal", "numeric", "real", "float", "double precision":
		return float64(0), nil
	case "bit", "bool", "boolean":
		return false, nil
	case "char", "varchar", "text":
		return "", nil
	case "binary", "varbinary", "blob":
		return []byte{}, nil
	case "date":
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "time":
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "timestamp", "datetime2", "datetime", "smalldatetime":
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "uuid", "uniqueidentifier":
		return "00000000-0000-0000-0000-000000000000", nil
	default:
		return nil, &schema.PolicyError{
			Operation: "prime empty SQL Server identity",
			Type:      column.Name + " " + base,
			Target:    string(schema.SQLServer),
		}
	}
}

func finishSQLServerLifecycleRollback(
	transaction *sql.Tx,
	connection *sql.Conn,
	operation string,
	result *error,
) {
	rollbackError := transaction.Rollback()
	if rollbackError == nil ||
		errors.Is(rollbackError, sql.ErrTxDone) ||
		sqlServerRollbackAlreadyCompletedByXACTAbort(rollbackError) {
		return
	}
	discardSQLServerLifecycleConnection(connection)
	safeRollback := newSQLServerSafeOperationError(
		"roll back SQL Server "+operation+" for",
		"selected tables",
		rollbackError,
	)
	combined := safeRollback
	if *result != nil {
		combined = errors.Join(*result, safeRollback)
	}
	*result = unknownSQLServerLifecycleState(
		"SQL Server "+operation+" rollback",
		combined,
	)
}

func unknownSQLServerLifecycleState(
	operation string,
	err error,
) error {
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf("%s outcome is unknown: %w", operation, err),
	)
}

func discardSQLServerLifecycleConnection(connection *sql.Conn) {
	_ = connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

func orderedSQLServerTargetTables(
	tables []schema.Table,
) []schema.Table {
	ordered := append([]schema.Table(nil), tables...)
	sort.Slice(ordered, func(left, right int) bool {
		return adapterSourceTableKey(
			ordered[left].Schema,
			ordered[left].Name,
		) < adapterSourceTableKey(
			ordered[right].Schema,
			ordered[right].Name,
		)
	})
	return ordered
}
