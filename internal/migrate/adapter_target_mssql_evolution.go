package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

const sqlServerTargetEvolutionLockTimeoutMilliseconds = 15_000

const sqlServerTargetEvolutionAcquireLockQuery = `
	DECLARE @result int;
	EXEC @result = sys.sp_getapplock
		@Resource = @p1,
		@LockMode = 'Exclusive',
		@LockOwner = 'Transaction',
		@LockTimeout = @p2,
		@DbPrincipal = 'public';
	SELECT @result;
`

const sqlServerTargetEvolutionRelationsQuery = `
	SELECT
		target_object.object_id,
		target_object.name,
		RTRIM(target_object.type),
		target_object.type_desc,
		target_object.parent_object_id,
		CONVERT(bit, target_object.is_ms_shipped)
	  FROM sys.objects AS target_object
	  JOIN sys.schemas AS target_schema
	    ON target_schema.schema_id = target_object.schema_id
	 WHERE target_schema.name = @p1
	   AND target_object.is_ms_shipped = 0
	 ORDER BY target_object.name, target_object.object_id
`

const sqlServerTargetEvolutionEnvironmentQuery = `
	SELECT
		target_database.name,
		target_database.compatibility_level,
		target_database.state_desc,
		target_database.user_access_desc,
		target_database.containment_desc,
		CONVERT(bit, target_database.is_read_only),
		CONVERT(bit, target_database.is_auto_close_on),
		CONVERT(bit, target_database.is_auto_shrink_on),
		CONVERT(bit, target_database.is_in_standby),
		target_database.source_database_id,
		CONVERT(bit, target_database.is_published),
		CONVERT(bit, target_database.is_subscribed),
		CONVERT(bit, target_database.is_merge_published),
		CONVERT(bit, target_database.is_distributor),
		CONVERT(bit, target_database.is_cdc_enabled),
		CONVERT(int, SERVERPROPERTY('ProductMajorVersion')),
		CONVERT(int, SERVERPROPERTY('EngineEdition')),
		CONVERT(varchar(128), SERVERPROPERTY('ProductVersion')),
		target_schema.schema_id,
		target_schema.name,
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
		), 0))
	  FROM sys.databases AS target_database
	  JOIN sys.schemas AS target_schema
	    ON target_schema.name = @p1
	 WHERE target_database.database_id = DB_ID()
`

var _ adapterTargetSchemaEvolutionCapability = (*sqlServerTargetAdapter)(nil)

// TargetSchemaEvolutionDialect keeps executable evolution bound to the target
// adapter that has already passed SQL Server 2022/TLS admission.
func (*sqlServerTargetAdapter) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.SQLServer
}

func (*sqlServerTargetAdapter) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	return sqlServerTargetSchemaEvolutionCreatePlanner{}
}

// PreflightTargetSchemaEvolution builds an immutable plan from a complete,
// SQL Server 2022 target catalog. It also fences unsupported DDL/dependency
// shapes before any statement can reach a target mutation session.
func (adapter *sqlServerTargetAdapter) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	if request.target != schema.SQLServer {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"SQL Server target adapter requires target dialect mssql",
			nil,
		)
	}
	if err := validateSQLServerTargetEvolutionDefaultConstraintTransitions(request); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateSQLServerTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"SQL Server target evolution adapter is not configured",
			err,
		)
	}
	plan, err := PreflightTargetSchemaEvolution(ctx, request, adapter)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateSQLServerTargetEvolutionPlanNamespace(
		plan,
		adapter.namespace,
	); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := preflightSQLServerTargetEvolutionOperations(
		ctx,
		adapter.database,
		adapter.namespace,
		plan,
	); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	return plan, nil
}

// validateSQLServerTargetEvolutionDefaultConstraintTransitions rejects the
// otherwise generic scalar-default allowance before target admission. SQL
// Server materializes every new DEFAULT as a separately named constraint; the
// server may allocate that name, while Stage 4 preserves exact immutable name
// reservations across every recovery prefix. DMTX therefore cannot certify a
// deterministic reservation transition for a new default in this slice.
func validateSQLServerTargetEvolutionDefaultConstraintTransitions(
	request TargetSchemaEvolutionRequest,
) error {
	prior := indexTargetSchemaEvolutionTables(request.priorTables)
	for _, current := range request.currentTables {
		key := targetSchemaEvolutionTableKey{
			schema: current.Schema,
			table:  current.Name,
		}
		previous, existed := prior[key]
		for _, column := range current.Columns {
			if column.Default == nil {
				continue
			}
			if existed && findTargetSchemaEvolutionColumnIndex(previous, column.Name) >= 0 {
				continue
			}
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				fmt.Sprintf(
					"SQL Server schema evolution does not support creating default constraints for %s.%s; add the column without a default or manage the default in separate target DDL",
					current.Name,
					column.Name,
				),
				nil,
			)
		}
	}
	return nil
}

// ReadTargetSchemaEvolutionCatalog observes one serializable catalog snapshot.
// The complete reader includes target-only tables and every non-table name
// reservation in the configured schema, so independent DDL is never mistaken
// for an evolution prefix.
func (adapter *sqlServerTargetAdapter) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if err := validateSQLServerTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	if err := engine.VerifySQLServer2022Target(ctx, adapter.database); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"verify SQL Server 2022 target evolution environment: %w",
			err,
		)
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"acquire SQL Server target schema evolution connection: %w",
			err,
		)
	}
	defer connection.Close()
	// go-mssqldb does not expose a server read-only transaction mode. This
	// serializable snapshot issues only catalog reads and always rolls back.
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"begin SQL Server target schema evolution catalog snapshot: %w",
			err,
		)
	}
	catalog, readErr := readSQLServerTargetEvolutionCatalog(
		ctx,
		transaction,
		adapter.namespace,
	)
	rollbackErr := transaction.Rollback()
	if rollbackErr != nil {
		discardSQLServerTargetEvolutionConnection(connection)
	}
	if readErr != nil {
		if rollbackErr != nil {
			return TargetSchemaEvolutionCatalog{}, errors.Join(
				readErr,
				fmt.Errorf("roll back SQL Server catalog snapshot: %w", rollbackErr),
			)
		}
		return TargetSchemaEvolutionCatalog{}, readErr
	}
	if rollbackErr != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"roll back SQL Server target schema evolution catalog snapshot: %w",
			rollbackErr,
		)
	}
	return catalog, nil
}

// ApplyTargetSchemaEvolutionPlan owns a pinned SQL Server session and a
// transaction-owned, database-scoped application fence. SQL Server DDL is
// transactional on the certified target shape, so every failure is rolled back
// as one unit. The fence is released atomically by COMMIT or ROLLBACK; a fresh
// complete catalog read resolves commit acknowledgement ambiguity.
func (adapter *sqlServerTargetAdapter) ApplyTargetSchemaEvolutionPlan(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
) (result error) {
	if err := validateSQLServerTargetEvolutionAdapter(adapter); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"SQL Server target evolution adapter is not configured",
			err,
		)
	}
	if err := validateSQLServerTargetEvolutionPlanNamespace(plan, adapter.namespace); err != nil {
		return err
	}
	if err := engine.VerifySQLServer2022Target(ctx, adapter.database); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"verify SQL Server 2022 target before schema evolution",
			err,
		)
	}

	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire pinned SQL Server target connection",
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := connection.Close(); closeErr != nil {
				discardSQLServerTargetEvolutionConnection(connection)
				result = joinSQLServerTargetEvolutionConnectionCloseError(
					result,
					closeErr,
				)
			}
		}
	}()

	if _, err := connection.ExecContext(ctx, "SET XACT_ABORT ON;"); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"enable SQL Server transactional DDL safety",
			err,
		)
	}
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"begin transaction",
			"begin serializable SQL Server target schema evolution transaction",
			err,
		)
	}
	lockResource := sqlServerTargetEvolutionLockResource(adapter.namespace)
	if err := acquireSQLServerTargetEvolutionLock(ctx, transaction, lockResource); err != nil {
		lockErr := targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire deterministic SQL Server target transaction fence",
			err,
		)
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			discardSQLServerTargetEvolutionConnection(connection)
			return joinSQLServerTargetEvolutionRollbackError(lockErr, rollbackErr)
		}
		return lockErr
	}
	session := &sqlServerTargetEvolutionMutationSession{
		executor: transaction,
		plan:     plan,
		readCatalog: func(readCtx context.Context) (
			TargetSchemaEvolutionCatalog,
			error,
		) {
			return readSQLServerTargetEvolutionCatalog(
				readCtx,
				transaction,
				adapter.namespace,
			)
		},
	}
	if applyErr := ApplyTargetSchemaEvolution(ctx, plan, session); applyErr != nil {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			discardSQLServerTargetEvolutionConnection(connection)
			return joinSQLServerTargetEvolutionRollbackError(applyErr, rollbackErr)
		}
		return applyErr
	}
	if commitErr := transaction.Commit(); commitErr != nil {
		// A transport error after COMMIT is an unknown outcome. Discard the
		// physical session and classify the exact durable catalog from a fresh
		// verified session; the transaction-owned fence is released with it.
		discardSQLServerTargetEvolutionConnection(connection)
		return adapter.classifySQLServerTargetEvolutionCommitAmbiguity(
			ctx,
			plan,
			commitErr,
		)
	}
	if err := connection.Close(); err != nil {
		discardSQLServerTargetEvolutionConnection(connection)
		closed = true
		// COMMIT already released the transaction-owned fence. Still require an
		// independent exact catalog before reporting the cleanup failure; a
		// caller must not mistake an unverified physical-session failure for a
		// clean completed apply.
		committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
		return errors.Join(
			joinSQLServerTargetEvolutionConnectionCloseError(nil, err),
			verifySQLServerTargetEvolutionCommittedCatalog(plan, committed, readErr),
		)
	}
	closed = true
	committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	return verifySQLServerTargetEvolutionCommittedCatalog(plan, committed, readErr)
}

func validateSQLServerTargetEvolutionAdapter(adapter *sqlServerTargetAdapter) error {
	if adapter == nil || adapter.database == nil {
		return fmt.Errorf("database is not configured")
	}
	if strings.TrimSpace(adapter.namespace) == "" {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"configuration",
			"SQL Server target evolution requires a configured target schema",
			nil,
		)
	}
	return nil
}

func sqlServerTargetEvolutionLockResource(namespace string) string {
	digest := sha256.Sum256([]byte("dmtx:stage4:mssql-evolution:" + namespace))
	return "dmtx-s4-evo-" + hex.EncodeToString(digest[:20])
}

func acquireSQLServerTargetEvolutionLock(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	resource string,
) error {
	var result int
	if err := queryer.QueryRowContext(
		ctx,
		sqlServerTargetEvolutionAcquireLockQuery,
		resource,
		sqlServerTargetEvolutionLockTimeoutMilliseconds,
	).Scan(&result); err != nil {
		return fmt.Errorf("acquire SQL Server target evolution application lock: %w", err)
	}
	if result < 0 {
		return fmt.Errorf(
			"acquire SQL Server target evolution application lock returned %d",
			result,
		)
	}
	return nil
}

func discardSQLServerTargetEvolutionConnection(connection *sql.Conn) {
	if connection == nil {
		return
	}
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
}

func joinSQLServerTargetEvolutionRollbackError(
	primary error,
	rollbackErr error,
) error {
	return errors.Join(
		primary,
		targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"rollback",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server schema evolution failed and rollback also failed",
			),
			rollbackErr,
		),
	)
}

func joinSQLServerTargetEvolutionConnectionCloseError(
	primary error,
	closeErr error,
) error {
	if closeErr == nil {
		return primary
	}
	cleanup := targetSchemaEvolutionError(
		TargetSchemaEvolutionVerifyFailed,
		"DDL fence cleanup",
		targetSchemaEvolutionRecoveryWording(
			"SQL Server schema evolution could not release its pinned mutation connection",
		),
		closeErr,
	)
	return errors.Join(primary, cleanup)
}

type sqlServerTargetSchemaEvolutionCreatePlanner struct{}

func (sqlServerTargetSchemaEvolutionCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesiredTables []schema.Table,
	actualCatalog TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.SQLServer {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQL Server target create planner cannot render %q",
			target,
		)
	}
	created := cloneTargetSchemaEvolutionTables(tables)
	sortTargetSchemaEvolutionTables(created)
	if len(created) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQL Server target create planner has no tables",
		)
	}
	desired := cloneTargetSchemaEvolutionTables(completeDesiredTables)
	sortTargetSchemaEvolutionTables(desired)
	createdKeys := make(map[targetSchemaEvolutionTableKey]struct{}, len(created))
	for _, table := range created {
		createdKeys[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = struct{}{}
	}
	if err := validateSQLServerTargetEvolutionCreateNames(
		createdKeys,
		desired,
		actualCatalog,
	); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}
	objects, err := schema.PlanSQLServerDropRecreateObjects(desired)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"plan complete SQL Server target objects: %w",
			err,
		)
	}

	state := make([]schema.Table, 0, len(created))
	steps := make([]TargetSchemaCreateStep, 0, len(created)+len(objects))
	for _, table := range created {
		statement, err := schema.SQLServerCreateTableDDL(table)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"plan complete SQL Server target table %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		base := cloneStage4RichTable(table)
		base.Indexes = nil
		base.Checks = nil
		base.ForeignKeys = nil
		state = append(state, base)
		sortTargetSchemaEvolutionTables(state)
		steps = append(steps, TargetSchemaCreateStep{
			Statement:    statement,
			ResultTables: cloneTargetSchemaEvolutionTables(state),
		})
	}
	for _, object := range objects {
		key := targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}
		if _, created := createdKeys[key]; !created {
			continue
		}
		if err := addSQLServerTargetEvolutionObject(state, created, object); err != nil {
			return CompleteTargetSchemaCreateBundle{}, err
		}
		statement, err := schema.SQLServerPlannedObjectDDL(desired, object)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"seal SQL Server target object %s.%s.%s: %w",
				object.Schema,
				object.Table,
				object.Name,
				err,
			)
		}
		steps = append(steps, TargetSchemaCreateStep{
			Statement:    statement,
			ResultTables: cloneTargetSchemaEvolutionTables(state),
		})
	}
	return NewCompleteTargetSchemaCreateBundle(schema.SQLServer, created, steps)
}

func addSQLServerTargetEvolutionObject(
	state []schema.Table,
	final []schema.Table,
	object schema.SQLServerObjectStatement,
) error {
	key := targetSchemaEvolutionTableKey{schema: object.Schema, table: object.Table}
	stateIndex := findTargetSchemaEvolutionTable(state, key)
	finalIndex := findTargetSchemaEvolutionTable(final, key)
	if stateIndex < 0 || finalIndex < 0 || strings.TrimSpace(object.Name) == "" {
		return fmt.Errorf(
			"planned SQL Server object %s.%s.%s has incomplete created-table authority",
			object.Schema,
			object.Table,
			object.Name,
		)
	}
	switch object.Kind {
	case schema.SQLServerIndexObject:
		for _, index := range final[finalIndex].Indexes {
			if index.Name == object.Name {
				state[stateIndex].Indexes = append(state[stateIndex].Indexes, index)
				return nil
			}
		}
	case schema.SQLServerCheckObject:
		for _, check := range final[finalIndex].Checks {
			if check.Name == object.Name {
				state[stateIndex].Checks = append(state[stateIndex].Checks, check)
				return nil
			}
		}
	case schema.SQLServerForeignKeyObject:
		for _, foreignKey := range final[finalIndex].ForeignKeys {
			if foreignKey.Name == object.Name {
				state[stateIndex].ForeignKeys = append(
					state[stateIndex].ForeignKeys,
					foreignKey,
				)
				return nil
			}
		}
	default:
		return fmt.Errorf(
			"SQL Server object plan returned unsupported object kind %d",
			object.Kind,
		)
	}
	return fmt.Errorf(
		"planned SQL Server object name %s on %s.%s does not exactly match target metadata; explicit round-trippable object names are required",
		object.Name,
		object.Schema,
		object.Table,
	)
}

func validateSQLServerTargetEvolutionCreateNames(
	created map[targetSchemaEvolutionTableKey]struct{},
	completeDesired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) error {
	actualTables := actual.Tables()
	actualIndex := indexTargetSchemaEvolutionTables(actualTables)
	reservedRelations := make(map[string]struct{})
	reservedConstraints := make(map[string]struct{})
	for _, reservation := range actual.Reservations() {
		if reservation.Scope == "relation" {
			reservedRelations[sqlServerFoldedName(reservation.Namespace)+"\x00"+
				sqlServerFoldedName(reservation.Name)] = struct{}{}
		}
		if reservation.Scope == "constraint" {
			reservedConstraints[sqlServerFoldedName(reservation.Namespace)+"\x00"+
				sqlServerFoldedName(reservation.Name)] = struct{}{}
		}
	}
	constraints := make(map[string]targetSchemaEvolutionTableKey)
	for _, table := range actualTables {
		key := targetSchemaEvolutionTableKey{schema: table.Schema, table: table.Name}
		primaryKey, err := schema.SQLServerPrimaryKeyConstraintName(table)
		if err != nil {
			return fmt.Errorf(
				"plan SQL Server evolution primary-key name for %s.%s: %w",
				table.Schema,
				table.Name,
				err,
			)
		}
		if primaryKey != "" {
			constraints[sqlServerFoldedName(table.Schema)+"\x00"+
				sqlServerFoldedName(primaryKey)] = key
		}
		for _, check := range table.Checks {
			constraints[sqlServerFoldedName(table.Schema)+"\x00"+
				sqlServerFoldedName(check.Name)] = key
		}
		for _, foreignKey := range table.ForeignKeys {
			constraints[sqlServerFoldedName(table.Schema)+"\x00"+
				sqlServerFoldedName(foreignKey.Name)] = key
		}
	}
	for _, table := range completeDesired {
		key := targetSchemaEvolutionTableKey{schema: table.Schema, table: table.Name}
		if _, isCreated := created[key]; !isCreated {
			continue
		}
		if _, resumed := actualIndex[key]; resumed {
			continue
		}
		relationKey := sqlServerFoldedName(table.Schema) + "\x00" +
			sqlServerFoldedName(table.Name)
		if _, collision := reservedRelations[relationKey]; collision {
			return fmt.Errorf(
				"planned SQL Server evolution table %s.%s collides with an existing relation reservation",
				table.Schema,
				table.Name,
			)
		}
		for _, index := range table.Indexes {
			if strings.TrimSpace(index.Name) == "" {
				return fmt.Errorf(
					"planned SQL Server evolution index on new table %s.%s has no explicit catalog name",
					table.Schema,
					table.Name,
				)
			}
		}
		for _, check := range table.Checks {
			if strings.TrimSpace(check.Name) == "" {
				return fmt.Errorf(
					"planned SQL Server evolution CHECK on new table %s.%s has no explicit catalog name",
					table.Schema,
					table.Name,
				)
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if strings.TrimSpace(foreignKey.Name) == "" {
				return fmt.Errorf(
					"planned SQL Server evolution foreign key on new table %s.%s has no explicit catalog name",
					table.Schema,
					table.Name,
				)
			}
		}
	}
	objects, err := schema.PlanSQLServerDropRecreateObjects(completeDesired)
	if err != nil {
		return fmt.Errorf("plan SQL Server evolution object names: %w", err)
	}
	for _, table := range completeDesired {
		key := targetSchemaEvolutionTableKey{schema: table.Schema, table: table.Name}
		if _, isCreated := created[key]; !isCreated {
			continue
		}
		primaryKey, err := schema.SQLServerPrimaryKeyConstraintName(table)
		if err != nil {
			return err
		}
		if primaryKey != "" {
			nameKey := sqlServerFoldedName(table.Schema) + "\x00" +
				sqlServerFoldedName(primaryKey)
			if _, collision := reservedConstraints[nameKey]; collision {
				return fmt.Errorf(
					"planned SQL Server evolution primary key %s on %s.%s collides with an existing target constraint reservation",
					primaryKey,
					table.Schema,
					table.Name,
				)
			}
			if owner, collision := constraints[nameKey]; collision && owner != key {
				return fmt.Errorf(
					"planned SQL Server evolution primary key %s on %s.%s collides with existing target constraint on %s.%s",
					primaryKey,
					table.Schema,
					table.Name,
					owner.schema,
					owner.table,
				)
			}
		}
	}
	for _, object := range objects {
		owner := targetSchemaEvolutionTableKey{schema: object.Schema, table: object.Table}
		if _, isCreated := created[owner]; !isCreated ||
			(object.Kind != schema.SQLServerCheckObject &&
				object.Kind != schema.SQLServerForeignKeyObject) {
			continue
		}
		nameKey := sqlServerFoldedName(object.Schema) + "\x00" +
			sqlServerFoldedName(object.Name)
		if _, collision := reservedConstraints[nameKey]; collision {
			return fmt.Errorf(
				"planned SQL Server evolution constraint %s on %s.%s collides with an existing target constraint reservation",
				object.Name,
				object.Schema,
				object.Table,
			)
		}
		if currentOwner, collision := constraints[nameKey]; collision && currentOwner != owner {
			return fmt.Errorf(
				"planned SQL Server evolution constraint %s on %s.%s collides with existing target constraint on %s.%s",
				object.Name,
				object.Schema,
				object.Table,
				currentOwner.schema,
				currentOwner.table,
			)
		}
	}
	return nil
}

type sqlServerTargetEvolutionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type sqlServerTargetEvolutionMutationSession struct {
	executor    sqlServerTargetEvolutionExecutor
	readCatalog func(context.Context) (TargetSchemaEvolutionCatalog, error)
	plan        TargetSchemaEvolutionPlan
}

func (session *sqlServerTargetEvolutionMutationSession) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if session == nil || session.readCatalog == nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"SQL Server target schema evolution catalog reader is not configured",
		)
	}
	return session.readCatalog(ctx)
}

func (session *sqlServerTargetEvolutionMutationSession) ExecuteTargetSchemaEvolution(
	ctx context.Context,
	operations []TargetSchemaEvolutionOperation,
) error {
	if session == nil || session.executor == nil {
		return fmt.Errorf(
			"SQL Server target schema evolution executor is not configured",
		)
	}
	if !sameTargetSchemaEvolutionOperations(
		session.plan.PendingOperations(),
		operations,
	) {
		return fmt.Errorf(
			"SQL Server target schema evolution executor received statements outside the immutable pending suffix",
		)
	}
	start := session.plan.observedPrefix
	for index, operation := range operations {
		statements := operation.Statements()
		if len(statements) != 1 || strings.TrimSpace(statements[0]) == "" {
			return fmt.Errorf(
				"SQL Server target schema evolution operation %d does not contain exactly one core-rendered statement",
				start+index,
			)
		}
		if _, err := session.executor.ExecContext(ctx, statements[0]); err != nil {
			return fmt.Errorf(
				"execute SQL Server target schema evolution operation %d (%s): %w",
				start+index,
				operation.Action(),
				err,
			)
		}
		actual, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
		if err != nil {
			return fmt.Errorf(
				"read SQL Server catalog after operation %d (%s): %w",
				start+index,
				operation.Action(),
				err,
			)
		}
		if _, err := matchTargetSchemaEvolutionState(
			[][]schema.Table{session.plan.states[start+index+1]},
			session.plan.reservations,
			actual,
		); err != nil {
			return fmt.Errorf(
				"SQL Server catalog after operation %d (%s) does not match its exact declared cumulative state: %w",
				start+index,
				operation.Action(),
				err,
			)
		}
	}
	return nil
}

type sqlServerTargetEvolutionEnvironment struct {
	databaseName       string
	compatibilityLevel int
	state              string
	userAccess         string
	containment        string
	readOnly           bool
	autoClose          bool
	autoShrink         bool
	standby            bool
	sourceDatabaseID   sql.NullInt64
	published          bool
	subscribed         bool
	mergePublished     bool
	distributor        bool
	changeDataCapture  bool
	productMajor       int
	engineEdition      int
	productVersion     string
	namespaceID        int64
	namespace          string
	viewDefinition     bool
	schemaControl      bool
	createTable        bool
	schemaAlter        bool
}

func readSQLServerTargetEvolutionEnvironment(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
) (sqlServerTargetEvolutionEnvironment, error) {
	var result sqlServerTargetEvolutionEnvironment
	err := queryer.QueryRowContext(
		ctx,
		sqlServerTargetEvolutionEnvironmentQuery,
		namespace,
	).Scan(
		&result.databaseName,
		&result.compatibilityLevel,
		&result.state,
		&result.userAccess,
		&result.containment,
		&result.readOnly,
		&result.autoClose,
		&result.autoShrink,
		&result.standby,
		&result.sourceDatabaseID,
		&result.published,
		&result.subscribed,
		&result.mergePublished,
		&result.distributor,
		&result.changeDataCapture,
		&result.productMajor,
		&result.engineEdition,
		&result.productVersion,
		&result.namespaceID,
		&result.namespace,
		&result.viewDefinition,
		&result.schemaControl,
		&result.createTable,
		&result.schemaAlter,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf(
			"SQL Server target schema %s does not exist",
			namespace,
		)
	}
	if err != nil {
		return result, fmt.Errorf(
			"inspect SQL Server target evolution environment: %w",
			err,
		)
	}
	return result, nil
}

func validateSQLServerTargetEvolutionEnvironment(
	namespace string,
	value sqlServerTargetEvolutionEnvironment,
) error {
	if namespace == "" || value.namespace != namespace || value.namespaceID <= 0 {
		return fmt.Errorf(
			"SQL Server target evolution requires exact configured namespace identity",
		)
	}
	if value.databaseName == "" || value.productMajor != 16 ||
		!strings.HasPrefix(value.productVersion, "16.") {
		return fmt.Errorf(
			"SQL Server target schema evolution requires SQL Server 2022; version=%q major=%d",
			value.productVersion,
			value.productMajor,
		)
	}
	switch value.engineEdition {
	case 2, 3, 4:
	default:
		return fmt.Errorf(
			"SQL Server target schema evolution does not support engine edition %d",
			value.engineEdition,
		)
	}
	if value.compatibilityLevel != 160 || value.state != "ONLINE" ||
		value.userAccess != "MULTI_USER" || value.containment != "NONE" ||
		value.readOnly || value.autoClose || value.autoShrink || value.standby ||
		value.sourceDatabaseID.Valid || value.published || value.subscribed ||
		value.mergePublished || value.distributor || value.changeDataCapture {
		return fmt.Errorf(
			"SQL Server target database is not the certified writable SQL Server 2022 catalog shape",
		)
	}
	if !value.viewDefinition || !value.schemaControl || !value.createTable ||
		!value.schemaAlter {
		return fmt.Errorf(
			"SQL Server target evolution requires effective database VIEW DEFINITION, database CREATE TABLE, and configured-schema CONTROL/ALTER privileges",
		)
	}
	return nil
}

type sqlServerTargetEvolutionRelation struct {
	objectID        int64
	name            string
	objectType      string
	typeDescription string
	parentObjectID  int64
	systemShipped   bool
}

func readSQLServerTargetEvolutionCatalog(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
) (TargetSchemaEvolutionCatalog, error) {
	environment, err := readSQLServerTargetEvolutionEnvironment(
		ctx,
		queryer,
		namespace,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	if err := validateSQLServerTargetEvolutionEnvironment(namespace, environment); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	relations, err := readSQLServerTargetEvolutionRelations(ctx, queryer, namespace)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	tableRelations, err := validateSQLServerTargetEvolutionRelations(
		namespace,
		relations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	tables := make([]schema.Table, 0, len(tableRelations))
	tablesByObjectID := make(map[int64]schema.Table, len(tableRelations))
	for _, relation := range tableRelations {
		catalog, exists, err := readSQLServerTargetTableCatalog(
			ctx,
			queryer,
			namespace,
			relation.name,
		)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"inspect complete SQL Server target table %s.%s: %w",
				namespace,
				relation.name,
				err,
			)
		}
		if !exists || catalog.objectID != relation.objectID {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"SQL Server target relation %s.%s changed while reading its complete catalog",
				namespace,
				relation.name,
			)
		}
		if err := validateSQLServerTargetTableCatalog(relation.name, catalog); err != nil {
			return TargetSchemaEvolutionCatalog{}, err
		}
		table, err := engine.InspectSQLServerTargetTableWithQueryer(
			ctx,
			queryer,
			namespace,
			relation.name,
		)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"inspect complete SQL Server target table %s.%s: %w",
				namespace,
				relation.name,
				err,
			)
		}
		if table.Schema != namespace || table.Name != relation.name {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"SQL Server target table catalog identity differs from requested %s.%s",
				namespace,
				relation.name,
			)
		}
		tables = append(tables, table)
		tablesByObjectID[relation.objectID] = table
	}
	sortTargetSchemaEvolutionTables(tables)
	reservations, err := sqlServerTargetEvolutionReservations(
		namespace,
		relations,
		tablesByObjectID,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"validate complete SQL Server target schema %s: %w",
			namespace,
			err,
		)
	}
	return catalog, nil
}

func readSQLServerTargetEvolutionRelations(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
) ([]sqlServerTargetEvolutionRelation, error) {
	rows, err := queryer.QueryContext(
		ctx,
		sqlServerTargetEvolutionRelationsQuery,
		namespace,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"enumerate complete SQL Server target schema %s: %w",
			namespace,
			err,
		)
	}
	defer rows.Close()
	var relations []sqlServerTargetEvolutionRelation
	for rows.Next() {
		var relation sqlServerTargetEvolutionRelation
		if err := rows.Scan(
			&relation.objectID,
			&relation.name,
			&relation.objectType,
			&relation.typeDescription,
			&relation.parentObjectID,
			&relation.systemShipped,
		); err != nil {
			return nil, fmt.Errorf("read SQL Server target relation: %w", err)
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQL Server target relation catalog: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQL Server target relation catalog: %w", err)
	}
	return relations, nil
}

func validateSQLServerTargetEvolutionRelations(
	namespace string,
	relations []sqlServerTargetEvolutionRelation,
) ([]sqlServerTargetEvolutionRelation, error) {
	seenObjectIDs := make(map[int64]struct{}, len(relations))
	seenTableNames := make(map[string]struct{}, len(relations))
	tables := make([]sqlServerTargetEvolutionRelation, 0, len(relations))
	for _, relation := range relations {
		if relation.objectID <= 0 || strings.TrimSpace(relation.name) == "" ||
			strings.TrimSpace(relation.objectType) == "" || relation.systemShipped {
			return nil, fmt.Errorf(
				"SQL Server target schema %s returned incomplete or system relation metadata",
				namespace,
			)
		}
		if _, duplicate := seenObjectIDs[relation.objectID]; duplicate {
			return nil, fmt.Errorf(
				"SQL Server target schema %s returned duplicate object id %d",
				namespace,
				relation.objectID,
			)
		}
		seenObjectIDs[relation.objectID] = struct{}{}
		if relation.objectType != "U" {
			continue
		}
		if relation.parentObjectID != 0 || relation.typeDescription != "USER_TABLE" {
			return nil, fmt.Errorf(
				"SQL Server target relation %s.%s is not an ordinary user table",
				namespace,
				relation.name,
			)
		}
		name := sqlServerFoldedName(relation.name)
		if _, duplicate := seenTableNames[name]; duplicate {
			return nil, fmt.Errorf(
				"SQL Server target schema %s has duplicate case-folded table name %s",
				namespace,
				relation.name,
			)
		}
		seenTableNames[name] = struct{}{}
		tables = append(tables, relation)
	}
	sort.Slice(tables, func(left, right int) bool {
		return sqlServerFoldedName(tables[left].name) <
			sqlServerFoldedName(tables[right].name)
	})
	return tables, nil
}

func sqlServerTargetEvolutionReservations(
	namespace string,
	relations []sqlServerTargetEvolutionRelation,
	tables map[int64]schema.Table,
) ([]TargetSchemaEvolutionNameReservation, error) {
	reservations := make([]TargetSchemaEvolutionNameReservation, 0, len(relations))
	seen := make(map[string]struct{}, len(relations))
	appendReservation := func(scope, name string) error {
		key := scope + "\x00" + sqlServerFoldedName(name)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf(
				"SQL Server target schema %s returned duplicate %s reservation %s",
				namespace,
				scope,
				name,
			)
		}
		seen[key] = struct{}{}
		reservations = append(reservations, TargetSchemaEvolutionNameReservation{
			Scope:     scope,
			Namespace: namespace,
			Name:      name,
		})
		return nil
	}
	for _, relation := range relations {
		switch relation.objectType {
		case "U":
			if _, exists := tables[relation.objectID]; !exists {
				return nil, fmt.Errorf(
					"SQL Server target ordinary table %s.%s was omitted from the logical catalog",
					namespace,
					relation.name,
				)
			}
		case "PK":
			table, exists := tables[relation.parentObjectID]
			if !exists {
				return nil, fmt.Errorf(
					"SQL Server primary key %s has no enumerated table owner",
					relation.name,
				)
			}
			expected, err := schema.SQLServerPrimaryKeyConstraintName(table)
			if err != nil {
				return nil, err
			}
			if expected == "" || !strings.EqualFold(expected, relation.name) {
				if err := appendReservation("constraint", relation.name); err != nil {
					return nil, err
				}
			}
		case "UQ":
			if !sqlServerTargetEvolutionHasIndex(tables[relation.parentObjectID], relation.name) {
				return nil, fmt.Errorf(
					"SQL Server unique constraint %s is not represented by its target table index catalog",
					relation.name,
				)
			}
		case "C":
			if !sqlServerTargetEvolutionHasCheck(tables[relation.parentObjectID], relation.name) {
				return nil, fmt.Errorf(
					"SQL Server CHECK constraint %s is not represented by its target table catalog",
					relation.name,
				)
			}
		case "F":
			if !sqlServerTargetEvolutionHasForeignKey(tables[relation.parentObjectID], relation.name) {
				return nil, fmt.Errorf(
					"SQL Server foreign key %s is not represented by its target table catalog",
					relation.name,
				)
			}
		case "D":
			if _, exists := tables[relation.parentObjectID]; !exists {
				return nil, fmt.Errorf(
					"SQL Server default constraint %s has no enumerated table owner",
					relation.name,
				)
			}
			// A default name does not describe the logical default expression (and
			// SQL Server may generate one), but it does occupy the schema-wide
			// constraint namespace. Retain it as collision authority so a newly
			// planned primary key, CHECK, or FK cannot hide a target-only default.
			if err := appendReservation("constraint", relation.name); err != nil {
				return nil, err
			}
		default:
			if relation.parentObjectID != 0 {
				return nil, fmt.Errorf(
					"SQL Server target schema %s contains unsupported attached %s %s; evolution cannot prove its complete table shape",
					namespace,
					relation.typeDescription,
					relation.name,
				)
			}
			if err := appendReservation("relation", relation.name); err != nil {
				return nil, err
			}
		}
	}
	return reservations, nil
}

func sqlServerTargetEvolutionHasIndex(table schema.Table, name string) bool {
	for _, index := range table.Indexes {
		if strings.EqualFold(index.Name, name) {
			return true
		}
	}
	return false
}

func sqlServerTargetEvolutionHasCheck(table schema.Table, name string) bool {
	for _, check := range table.Checks {
		if strings.EqualFold(check.Name, name) {
			return true
		}
	}
	return false
}

func sqlServerTargetEvolutionHasForeignKey(table schema.Table, name string) bool {
	for _, foreignKey := range table.ForeignKeys {
		if strings.EqualFold(foreignKey.Name, name) {
			return true
		}
	}
	return false
}

func validateSQLServerTargetEvolutionPlanNamespace(
	plan TargetSchemaEvolutionPlan,
	namespace string,
) error {
	if !plan.valid() || plan.Target() != schema.SQLServer {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"SQL Server target adapter requires a valid mssql evolution plan",
			nil,
		)
	}
	for _, state := range plan.states {
		for _, table := range state {
			if table.Schema != namespace {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"apply",
					fmt.Sprintf(
						"plan table %s.%s is outside SQL Server target namespace %s",
						table.Schema,
						table.Name,
						namespace,
					),
					nil,
				)
			}
		}
	}
	for _, reservation := range plan.reservations {
		if reservation.Namespace != namespace {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"apply",
				fmt.Sprintf(
					"plan reservation %s/%s.%s is outside SQL Server target namespace %s",
					reservation.Scope,
					reservation.Namespace,
					reservation.Name,
					namespace,
				),
				nil,
			)
		}
	}
	return nil
}

func preflightSQLServerTargetEvolutionOperations(
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	namespace string,
	plan TargetSchemaEvolutionPlan,
) error {
	if err := preflightSQLServerDDLTriggers(ctx, queryer); err != nil {
		return fmt.Errorf("preflight SQL Server evolution DDL triggers: %w", err)
	}
	if err := preflightSQLServerSynonyms(ctx, queryer); err != nil {
		return fmt.Errorf("preflight SQL Server evolution synonyms: %w", err)
	}
	changed := make(map[string]schema.Table)
	for _, operation := range plan.PendingOperations() {
		for _, object := range operation.Objects() {
			if object.Schema != namespace || object.Table == "" {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"SQL Server evolution operation references a table outside the configured target schema",
					nil,
				)
			}
			changed[sqlServerFoldedName(object.Table)] = schema.Table{
				Schema: namespace,
				Name:   object.Table,
			}
		}
	}
	if len(changed) == 0 {
		return nil
	}
	if err := preflightSQLServerExternalForeignKeys(
		ctx,
		queryer,
		namespace,
		changed,
	); err != nil {
		return fmt.Errorf("preflight SQL Server evolution foreign-key dependencies: %w", err)
	}
	if err := preflightSQLServerDependencies(
		ctx,
		queryer,
		namespace,
		changed,
	); err != nil {
		return fmt.Errorf("preflight SQL Server evolution expression dependencies: %w", err)
	}
	return nil
}

func verifySQLServerTargetEvolutionCommittedCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server evolution committed but an independent complete catalog snapshot could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server evolution committed but the independent complete catalog snapshot is structurally invalid",
			),
			err,
		)
	}
	if _, err := matchTargetSchemaEvolutionState(
		[][]schema.Table{plan.states[len(plan.states)-1]},
		plan.reservations,
		actual,
	); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server evolution committed but an independent snapshot found concurrent or unexpected catalog drift",
			),
			err,
		)
	}
	return nil
}

func (adapter *sqlServerTargetAdapter) classifySQLServerTargetEvolutionCommitAmbiguity(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	commitErr error,
) error {
	actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	return classifySQLServerTargetEvolutionCommitCatalog(
		plan,
		actual,
		readErr,
		commitErr,
	)
}

func classifySQLServerTargetEvolutionCommitCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
	commitErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server commit returned an error and an independent complete catalog snapshot could not be read",
			),
			errors.Join(commitErr, readErr),
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server commit returned an error and the independent catalog is structurally invalid",
			),
			errors.Join(commitErr, err),
		)
	}
	prefix, err := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		actual,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQL Server commit returned an error and the independent catalog has unexpected drift",
			),
			errors.Join(commitErr, err),
		)
	}
	if prefix == len(plan.operations) {
		// The exact final immutable catalog is stronger evidence than an
		// acknowledgement lost after commit. Continue truthfully without
		// rerunning DDL or treating an already-applied change as uncertain.
		return nil
	}
	return targetSchemaEvolutionError(
		TargetSchemaEvolutionApplyFailed,
		"commit",
		targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
			"SQL Server commit returned an error after exact verified prefix %d of %d",
			prefix,
			len(plan.operations),
		)),
		commitErr,
	)
}
