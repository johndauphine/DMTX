package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	postgresTargetEvolutionMinimumVersion = 160000
	postgresTargetEvolutionMaximumVersion = 170000
)

const postgresTargetEvolutionEnvironmentQuery = `
	SELECT
		namespace.oid::bigint,
		current_database(),
		current_setting('server_version_num')::integer,
		pg_catalog.has_schema_privilege(
			current_user,
			namespace.oid,
			'USAGE'
		),
		pg_catalog.has_schema_privilege(
			current_user,
			namespace.oid,
			'CREATE'
		),
		current_setting('default_transaction_read_only')::boolean,
		pg_catalog.pg_is_in_recovery()
	FROM pg_catalog.pg_namespace AS namespace
	WHERE namespace.nspname = $1
`

const postgresTargetEvolutionRelationQuery = `
	SELECT
		relation.oid::bigint,
		namespace.nspname,
		relation.relname,
		relation.relkind::text,
		COALESCE(index_owner_namespace.nspname, ''),
		COALESCE(index_owner.relname, ''),
		COALESCE(sequence_owner.namespace_name, ''),
		COALESCE(sequence_owner.relation_name, ''),
		pg_catalog.pg_has_role(
			current_user,
			relation.relowner,
			'USAGE'
		)
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

const postgresTargetEvolutionDependencyQuery = `
	SELECT
		dependency_kind,
		dependent_namespace,
		dependent_name,
		referenced_namespace,
		referenced_name
	FROM (
		SELECT
			'foreign-key'::text AS dependency_kind,
			owner_namespace.nspname::text AS dependent_namespace,
			owner_relation.relname::text AS dependent_name,
			referenced_namespace.nspname::text AS referenced_namespace,
			referenced_relation.relname::text AS referenced_name
		FROM pg_catalog.pg_constraint AS constraint_record
		JOIN pg_catalog.pg_class AS owner_relation
		  ON owner_relation.oid = constraint_record.conrelid
		JOIN pg_catalog.pg_namespace AS owner_namespace
		  ON owner_namespace.oid = owner_relation.relnamespace
		JOIN pg_catalog.pg_class AS referenced_relation
		  ON referenced_relation.oid = constraint_record.confrelid
		JOIN pg_catalog.pg_namespace AS referenced_namespace
		  ON referenced_namespace.oid = referenced_relation.relnamespace
		WHERE constraint_record.contype = 'f'
		  AND referenced_namespace.nspname = $1
		  AND owner_namespace.nspname <> $1

		UNION ALL

		SELECT DISTINCT
			CASE dependent_relation.relkind
				WHEN 'v' THEN 'view'
				WHEN 'm' THEN 'materialized-view'
				ELSE 'rewrite-relation'
			END::text AS dependency_kind,
			dependent_namespace.nspname::text AS dependent_namespace,
			dependent_relation.relname::text AS dependent_name,
			referenced_namespace.nspname::text AS referenced_namespace,
			referenced_relation.relname::text AS referenced_name
		FROM pg_catalog.pg_depend AS dependency
		JOIN pg_catalog.pg_rewrite AS rewrite_rule
		  ON dependency.classid =
		     'pg_catalog.pg_rewrite'::pg_catalog.regclass
		 AND rewrite_rule.oid = dependency.objid
		JOIN pg_catalog.pg_class AS dependent_relation
		  ON dependent_relation.oid = rewrite_rule.ev_class
		JOIN pg_catalog.pg_namespace AS dependent_namespace
		  ON dependent_namespace.oid = dependent_relation.relnamespace
		JOIN pg_catalog.pg_class AS referenced_relation
		  ON dependency.refclassid =
		     'pg_catalog.pg_class'::pg_catalog.regclass
		 AND referenced_relation.oid = dependency.refobjid
		JOIN pg_catalog.pg_namespace AS referenced_namespace
		  ON referenced_namespace.oid = referenced_relation.relnamespace
		WHERE referenced_namespace.nspname = $1
		  AND dependent_namespace.nspname <> $1
		  AND dependent_relation.relkind IN ('v', 'm')
	) AS hazards
	ORDER BY
		dependency_kind,
		dependent_namespace,
		dependent_name,
		referenced_namespace,
		referenced_name
`

const postgresTargetEvolutionAdvisoryLockStatement = `
	SELECT pg_catalog.pg_advisory_xact_lock($1)
`

type postgresTargetEvolutionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type postgresTargetEvolutionRelation struct {
	postgresExistingRelationName
	canAlter bool
}

type postgresTargetEvolutionEnvironment struct {
	namespaceObjectID int64
	databaseName      string
	version           int
	canUseNamespace   bool
	canCreate         bool
	defaultReadOnly   bool
	inRecovery        bool
}

type postgresTargetEvolutionCatalogRead func(
	context.Context,
) (
	TargetSchemaEvolutionCatalog,
	[]postgresTargetEvolutionRelation,
	error,
)

// postgresTargetSchemaEvolutionCreatePlanner is deliberately target-owned.
// The core supplies both the requested new tables and the complete desired
// namespace so foreign keys and engine-wide relation names are planned
// globally rather than inferred from a partial create set.
type postgresTargetSchemaEvolutionCreatePlanner struct{}

func (planner postgresTargetSchemaEvolutionCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesiredTables []schema.Table,
	actualCatalog TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.Postgres {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"PostgreSQL target create planner cannot render %q",
			target,
		)
	}
	created := cloneTargetSchemaEvolutionTables(tables)
	sortTargetSchemaEvolutionTables(created)
	if len(created) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"PostgreSQL target create planner has no tables",
		)
	}

	allTables := cloneTargetSchemaEvolutionTables(completeDesiredTables)
	sortTargetSchemaEvolutionTables(allTables)
	createdKeys := make(map[targetSchemaEvolutionTableKey]struct{}, len(created))
	for _, table := range created {
		createdKeys[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = struct{}{}
	}
	if err := validatePostgresTargetEvolutionCreateNames(
		createdKeys,
		allTables,
		actualCatalog,
	); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}

	objectPlan, err := schema.PlanPostgresDropRecreateObjects(
		allTables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"plan complete PostgreSQL target objects: %w",
			err,
		)
	}

	state := make([]schema.Table, 0, len(created))
	steps := make(
		[]TargetSchemaCreateStep,
		0,
		len(created)+len(objectPlan),
	)
	for _, table := range created {
		statement, err := schema.CreateTableDDL(schema.Postgres, table)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"plan complete PostgreSQL target table %s.%s: %w",
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
	for _, object := range objectPlan {
		key := targetSchemaEvolutionTableKey{
			schema: object.Schema(),
			table:  object.Table(),
		}
		if _, isCreated := createdKeys[key]; !isCreated {
			continue
		}
		if err := addPostgresTargetEvolutionObject(
			state,
			created,
			object,
		); err != nil {
			return CompleteTargetSchemaCreateBundle{}, err
		}
		statement, err := schema.PostgresObjectDDL(object)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"seal PostgreSQL target object %s.%s.%s: %w",
				object.Schema(),
				object.Table(),
				object.Name(),
				err,
			)
		}
		steps = append(steps, TargetSchemaCreateStep{
			Statement:    statement,
			ResultTables: cloneTargetSchemaEvolutionTables(state),
		})
	}
	return NewCompleteTargetSchemaCreateBundle(
		schema.Postgres,
		created,
		steps,
	)
}

func addPostgresTargetEvolutionObject(
	state []schema.Table,
	final []schema.Table,
	object schema.PostgresObjectStatement,
) error {
	key := targetSchemaEvolutionTableKey{
		schema: object.Schema(),
		table:  object.Table(),
	}
	stateIndex := findTargetSchemaEvolutionTable(state, key)
	finalIndex := findTargetSchemaEvolutionTable(final, key)
	if stateIndex < 0 || finalIndex < 0 {
		return fmt.Errorf(
			"PostgreSQL object plan references unknown created table %s.%s",
			object.Schema(),
			object.Table(),
		)
	}
	switch object.Kind() {
	case schema.PostgresIndexObject:
		for _, index := range final[finalIndex].Indexes {
			if index.Name == object.Name() {
				state[stateIndex].Indexes = append(
					state[stateIndex].Indexes,
					index,
				)
				return nil
			}
		}
	case schema.PostgresCheckObject:
		for _, check := range final[finalIndex].Checks {
			if check.Name == object.Name() {
				state[stateIndex].Checks = append(
					state[stateIndex].Checks,
					check,
				)
				return nil
			}
		}
	case schema.PostgresForeignKeyObject:
		for _, foreignKey := range final[finalIndex].ForeignKeys {
			if foreignKey.Name == object.Name() {
				state[stateIndex].ForeignKeys = append(
					state[stateIndex].ForeignKeys,
					foreignKey,
				)
				return nil
			}
		}
	default:
		return fmt.Errorf(
			"PostgreSQL object plan returned unsupported object kind %d",
			object.Kind(),
		)
	}
	return fmt.Errorf(
		"planned PostgreSQL object name %s on %s.%s does not exactly match target metadata; explicit round-trippable object names are required",
		object.Name(),
		object.Schema(),
		object.Table(),
	)
}

// PreflightTargetSchemaEvolution reads an exact, privilege-proved PostgreSQL
// namespace and builds a pure immutable plan. It never opens an executor.
func (adapter *postgresTargetAdapter) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	if request.target != schema.Postgres {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"PostgreSQL target adapter requires target dialect postgres",
			nil,
		)
	}
	return PreflightTargetSchemaEvolution(ctx, request, adapter)
}

func (*postgresTargetAdapter) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	return postgresTargetSchemaEvolutionCreatePlanner{}
}

// ReadTargetSchemaEvolutionCatalog makes postgresTargetAdapter an exact
// preflight reader. A standalone read uses one serializable, read-only
// transaction; mutation uses the separate pinned session below.
func (adapter *postgresTargetAdapter) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if adapter == nil || adapter.database == nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"PostgreSQL target schema evolution database is not configured",
		)
	}
	namespace := postgresTargetEvolutionNamespace(adapter.namespace)
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"acquire PostgreSQL target schema evolution connection: %w",
			err,
		)
	}
	defer connection.Close()
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"begin PostgreSQL target schema evolution catalog snapshot: %w",
			err,
		)
	}
	catalog, _, readErr := readPostgresTargetEvolutionCatalog(
		ctx,
		transaction,
		namespace,
		false,
	)
	rollbackErr := transaction.Rollback()
	if rollbackErr != nil {
		_ = connection.Raw(func(any) error {
			return driver.ErrBadConn
		})
	}
	if readErr != nil {
		if rollbackErr != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"%w; roll back PostgreSQL catalog snapshot: %v",
				readErr,
				rollbackErr,
			)
		}
		return TargetSchemaEvolutionCatalog{}, readErr
	}
	if rollbackErr != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"roll back PostgreSQL target schema evolution catalog snapshot: %w",
			rollbackErr,
		)
	}
	return catalog, nil
}

// ApplyTargetSchemaEvolutionPlan owns one pinned connection and one
// serializable transaction from the advisory DDL fence through exact
// in-transaction post-catalog verification and commit. The advisory lock
// coordinates DMTX sessions; deterministic table locks fence non-cooperating
// ALTER/FK work. A separate post-commit snapshot detects non-cooperating
// namespace CREATE work that became visible outside the transaction snapshot.
func (adapter *postgresTargetAdapter) ApplyTargetSchemaEvolutionPlan(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
) error {
	if adapter == nil || adapter.database == nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"PostgreSQL target schema evolution database is not configured",
			nil,
		)
	}
	namespace := postgresTargetEvolutionNamespace(adapter.namespace)
	if err := validatePostgresTargetEvolutionPlanNamespace(
		plan,
		namespace,
	); err != nil {
		return err
	}

	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"begin transaction",
			"acquire pinned PostgreSQL target connection",
			err,
		)
	}
	defer connection.Close()
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"begin transaction",
			"begin serializable PostgreSQL target transaction",
			err,
		)
	}
	if _, err := transaction.ExecContext(
		ctx,
		postgresTargetEvolutionAdvisoryLockStatement,
		postgresTargetEvolutionAdvisoryKey(namespace),
	); err != nil {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil {
			_ = connection.Raw(func(any) error {
				return driver.ErrBadConn
			})
			err = fmt.Errorf("%w; rollback: %v", err, rollbackErr)
		}
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire deterministic PostgreSQL target namespace fence",
			err,
		)
	}

	session := &postgresTargetEvolutionMutationSession{
		executor:  transaction,
		namespace: namespace,
		plan:      plan,
		readCatalog: func(
			readCtx context.Context,
		) (
			TargetSchemaEvolutionCatalog,
			[]postgresTargetEvolutionRelation,
			error,
		) {
			return readPostgresTargetEvolutionCatalog(
				readCtx,
				transaction,
				namespace,
				true,
			)
		},
	}
	commitFailed := false
	rollbackFailed := false
	err = runPostgresTargetEvolutionUnitOfWork(
		ctx,
		plan,
		session,
		func() error {
			commitErr := transaction.Commit()
			commitFailed = commitErr != nil
			return commitErr
		},
		func() error {
			rollbackErr := transaction.Rollback()
			rollbackFailed = rollbackErr != nil
			return rollbackErr
		},
	)
	if commitFailed || rollbackFailed {
		// A commit transport error is an unknown outcome. Prevent database/sql
		// from ever reusing the physical connection for a later migration. A
		// failed rollback gets the same treatment because its transaction state
		// is not trustworthy either.
		_ = connection.Raw(func(any) error {
			return driver.ErrBadConn
		})
	}
	if err != nil {
		return err
	}
	if err := connection.Close(); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"PostgreSQL evolution committed but the mutation connection could not be released before an independent catalog read",
			),
			err,
		)
	}
	committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	return verifyPostgresTargetEvolutionCommittedCatalog(
		plan,
		committed,
		readErr,
	)
}

func verifyPostgresTargetEvolutionCommittedCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"PostgreSQL evolution committed but an independent complete catalog snapshot could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"PostgreSQL evolution committed but the independent complete catalog snapshot is structurally invalid",
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
				"PostgreSQL evolution committed but an independent snapshot found concurrent or unexpected catalog drift",
			),
			err,
		)
	}
	return nil
}

func runPostgresTargetEvolutionUnitOfWork(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	session TargetSchemaEvolutionMutationSession,
	commit func() error,
	rollback func() error,
) error {
	if err := ApplyTargetSchemaEvolution(ctx, plan, session); err != nil {
		if rollbackErr := rollback(); rollbackErr != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionApplyFailed,
				"rollback",
				targetSchemaEvolutionRecoveryWording(
					"PostgreSQL schema evolution failed and rollback also failed",
				),
				fmt.Errorf(
					"apply: %w; rollback: %v",
					err,
					rollbackErr,
				),
			)
		}
		return err
	}
	if err := commit(); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"PostgreSQL commit returned an error and the commit outcome is unknown",
			),
			err,
		)
	}
	return nil
}

type postgresTargetEvolutionMutationSession struct {
	executor    postgresTargetEvolutionExecutor
	namespace   string
	plan        TargetSchemaEvolutionPlan
	readCatalog postgresTargetEvolutionCatalogRead
	relations   []postgresTargetEvolutionRelation
}

func (session *postgresTargetEvolutionMutationSession) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if session == nil || session.readCatalog == nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"PostgreSQL target schema evolution catalog reader is not configured",
		)
	}
	catalog, relations, err := session.readCatalog(ctx)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	session.relations = append(
		session.relations[:0],
		relations...,
	)
	return NewTargetSchemaEvolutionCatalog(
		catalog.Tables(),
		catalog.Reservations(),
	)
}

func (session *postgresTargetEvolutionMutationSession) ExecuteTargetSchemaEvolution(
	ctx context.Context,
	operations []TargetSchemaEvolutionOperation,
) error {
	if session == nil || session.executor == nil {
		return fmt.Errorf(
			"PostgreSQL target schema evolution executor is not configured",
		)
	}
	expected := session.plan.PendingOperations()
	if !sameTargetSchemaEvolutionOperations(expected, operations) {
		return fmt.Errorf(
			"PostgreSQL target schema evolution executor received statements outside the immutable pending suffix",
		)
	}
	if err := validatePostgresTargetEvolutionRelationNames(
		session.plan,
		session.relations,
	); err != nil {
		return err
	}

	start := session.plan.observedPrefix
	for index, operation := range operations {
		statements := operation.Statements()
		if len(statements) != 1 ||
			strings.TrimSpace(statements[0]) == "" {
			return fmt.Errorf(
				"PostgreSQL target schema evolution operation %d does not contain exactly one core-rendered statement",
				start+index,
			)
		}
		if _, err := session.executor.ExecContext(
			ctx,
			statements[0],
		); err != nil {
			return fmt.Errorf(
				"execute PostgreSQL target schema evolution operation %d (%s): %w",
				start+index,
				operation.Action(),
				err,
			)
		}
		actual, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
		if err != nil {
			return fmt.Errorf(
				"read PostgreSQL catalog after operation %d (%s): %w",
				start+index,
				operation.Action(),
				err,
			)
		}
		if _, err := matchTargetSchemaEvolutionState(
			[][]schema.Table{
				session.plan.states[start+index+1],
			},
			session.plan.reservations,
			actual,
		); err != nil {
			return fmt.Errorf(
				"PostgreSQL catalog after operation %d (%s) does not match its exact declared cumulative state: %w",
				start+index,
				operation.Action(),
				err,
			)
		}
	}
	return nil
}

func sameTargetSchemaEvolutionOperations(
	left []TargetSchemaEvolutionOperation,
	right []TargetSchemaEvolutionOperation,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].action != right[index].action ||
			left[index].beforeDigest != right[index].beforeDigest ||
			left[index].afterDigest != right[index].afterDigest ||
			!sameStrings(left[index].statements, right[index].statements) ||
			!reflect.DeepEqual(left[index].objects, right[index].objects) {
			return false
		}
	}
	return true
}

func readPostgresTargetEvolutionCatalog(
	ctx context.Context,
	queryer interface {
		engine.PostgresCatalogQueryer
		postgresTargetEvolutionExecutor
	},
	namespace string,
	lockTables bool,
) (
	TargetSchemaEvolutionCatalog,
	[]postgresTargetEvolutionRelation,
	error,
) {
	environment, err := readPostgresTargetEvolutionEnvironment(
		ctx,
		queryer,
		namespace,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}
	if err := validatePostgresTargetEvolutionEnvironment(
		namespace,
		environment,
	); err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}
	relations, err := readPostgresTargetEvolutionRelations(
		ctx,
		queryer,
		namespace,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}
	tableNames, err := validatePostgresTargetEvolutionRelations(
		namespace,
		relations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}
	if lockTables {
		for _, tableName := range tableNames {
			if _, err := queryer.ExecContext(
				ctx,
				"LOCK TABLE "+
					postgresQualified(namespace, tableName)+
					" IN SHARE UPDATE EXCLUSIVE MODE",
			); err != nil {
				return TargetSchemaEvolutionCatalog{}, nil, fmt.Errorf(
					"lock PostgreSQL target table %s.%s for schema evolution: %w",
					namespace,
					tableName,
					err,
				)
			}
		}
	}
	if err := rejectPostgresTargetEvolutionDependencies(
		ctx,
		queryer,
		namespace,
	); err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}

	tables := make([]schema.Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		table, err := engine.InspectPostgresTableWithQueryer(
			ctx,
			queryer,
			namespace,
			tableName,
		)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, nil, fmt.Errorf(
				"inspect complete PostgreSQL target table %s.%s: %w",
				namespace,
				tableName,
				err,
			)
		}
		tables = append(tables, table)
	}
	sortTargetSchemaEvolutionTables(tables)
	reservations, err := postgresTargetEvolutionReservations(
		tables,
		relations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, err
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(
		tables,
		reservations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, nil, fmt.Errorf(
			"validate complete PostgreSQL target namespace %s: %w",
			namespace,
			err,
		)
	}
	return catalog, relations, nil
}

func readPostgresTargetEvolutionEnvironment(
	ctx context.Context,
	queryer engine.PostgresCatalogQueryer,
	namespace string,
) (postgresTargetEvolutionEnvironment, error) {
	var result postgresTargetEvolutionEnvironment
	if err := queryer.QueryRowContext(
		ctx,
		postgresTargetEvolutionEnvironmentQuery,
		namespace,
	).Scan(
		&result.namespaceObjectID,
		&result.databaseName,
		&result.version,
		&result.canUseNamespace,
		&result.canCreate,
		&result.defaultReadOnly,
		&result.inRecovery,
	); err != nil {
		if err == sql.ErrNoRows {
			return result, fmt.Errorf(
				"PostgreSQL target namespace %s does not exist",
				namespace,
			)
		}
		return result, fmt.Errorf(
			"inspect PostgreSQL target namespace %s environment: %w",
			namespace,
			err,
		)
	}
	return result, nil
}

func validatePostgresTargetEvolutionEnvironment(
	namespace string,
	value postgresTargetEvolutionEnvironment,
) error {
	if namespace == "" ||
		namespace == "pg_catalog" ||
		namespace == "information_schema" ||
		namespace == "pg_toast" ||
		strings.HasPrefix(namespace, "pg_temp_") ||
		strings.HasPrefix(namespace, "pg_toast_temp_") {
		return fmt.Errorf(
			"PostgreSQL target namespace %q is not an evolvable user namespace",
			namespace,
		)
	}
	if value.namespaceObjectID <= 0 || value.databaseName == "" {
		return fmt.Errorf(
			"PostgreSQL target namespace %s returned incomplete catalog identity",
			namespace,
		)
	}
	if value.version < postgresTargetEvolutionMinimumVersion ||
		value.version >= postgresTargetEvolutionMaximumVersion {
		return fmt.Errorf(
			"PostgreSQL target schema evolution requires PostgreSQL 16; server_version_num=%d",
			value.version,
		)
	}
	if !value.canUseNamespace || !value.canCreate {
		return fmt.Errorf(
			"PostgreSQL target role lacks required USAGE and CREATE privileges on namespace %s",
			namespace,
		)
	}
	if value.defaultReadOnly || value.inRecovery {
		return fmt.Errorf(
			"PostgreSQL target database is read-only or in recovery",
		)
	}
	return nil
}

func readPostgresTargetEvolutionRelations(
	ctx context.Context,
	queryer engine.PostgresCatalogQueryer,
	namespace string,
) ([]postgresTargetEvolutionRelation, error) {
	rows, err := queryer.QueryContext(
		ctx,
		postgresTargetEvolutionRelationQuery,
		namespace,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"enumerate complete PostgreSQL target namespace %s: %w",
			namespace,
			err,
		)
	}
	defer rows.Close()
	var relations []postgresTargetEvolutionRelation
	for rows.Next() {
		var relation postgresTargetEvolutionRelation
		if err := rows.Scan(
			&relation.objectID,
			&relation.namespace,
			&relation.name,
			&relation.relationKind,
			&relation.indexOwnerNamespace,
			&relation.indexOwnerTable,
			&relation.sequenceOwnerNamespace,
			&relation.sequenceOwnerTable,
			&relation.canAlter,
		); err != nil {
			return nil, fmt.Errorf(
				"read PostgreSQL target namespace relation: %w",
				err,
			)
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate PostgreSQL target namespace relations: %w",
			err,
		)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(
			"close PostgreSQL target namespace relation catalog: %w",
			err,
		)
	}
	return relations, nil
}

func validatePostgresTargetEvolutionRelations(
	namespace string,
	relations []postgresTargetEvolutionRelation,
) ([]string, error) {
	ordinaryTables := make(map[string]struct{})
	seen := make(map[string]struct{}, len(relations))
	var tableNames []string
	for _, relation := range relations {
		key := postgresRelationNameKey(relation.namespace, relation.name)
		if relation.objectID <= 0 ||
			relation.namespace != namespace ||
			relation.name == "" {
			return nil, fmt.Errorf(
				"PostgreSQL target namespace %s returned incomplete or mismatched relation identity",
				namespace,
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf(
				"PostgreSQL target namespace %s returned duplicate relation name %s",
				namespace,
				relation.name,
			)
		}
		seen[key] = struct{}{}
		if relation.relationKind != "r" {
			continue
		}
		if !relation.canAlter {
			return nil, fmt.Errorf(
				"PostgreSQL target role does not own or inherit ownership of table %s.%s; ALTER privilege cannot be proved",
				namespace,
				relation.name,
			)
		}
		ordinaryTables[key] = struct{}{}
		tableNames = append(tableNames, relation.name)
	}
	for _, relation := range relations {
		switch relation.relationKind {
		case "r":
		case "i":
			ownerKey := postgresRelationNameKey(
				relation.indexOwnerNamespace,
				relation.indexOwnerTable,
			)
			if _, ok := ordinaryTables[ownerKey]; !ok ||
				relation.indexOwnerNamespace != namespace {
				return nil, fmt.Errorf(
					"PostgreSQL target index %s.%s is not owned by an enumerated ordinary table",
					namespace,
					relation.name,
				)
			}
		case "S":
			ownerKey := postgresRelationNameKey(
				relation.sequenceOwnerNamespace,
				relation.sequenceOwnerTable,
			)
			if _, ok := ordinaryTables[ownerKey]; !ok ||
				relation.sequenceOwnerNamespace != namespace {
				return nil, fmt.Errorf(
					"PostgreSQL target sequence %s.%s is not owned by an enumerated ordinary table",
					namespace,
					relation.name,
				)
			}
		default:
			return nil, fmt.Errorf(
				"PostgreSQL target namespace %s contains unsupported %s relation %s; views, partitioned tables, foreign tables, and other catalog shapes must be removed or isolated before evolution",
				namespace,
				describePostgresRelationKind(relation.relationKind),
				relation.name,
			)
		}
	}
	sort.Strings(tableNames)
	return tableNames, nil
}

func postgresTargetEvolutionReservations(
	tables []schema.Table,
	relations []postgresTargetEvolutionRelation,
) ([]TargetSchemaEvolutionNameReservation, error) {
	modeled := make(map[string]struct{})
	for _, table := range tables {
		modeled[postgresRelationNameKey(table.Schema, table.Name)] =
			struct{}{}
		for _, index := range table.Indexes {
			modeled[postgresRelationNameKey(table.Schema, index.Name)] =
				struct{}{}
		}
		if postgresTableHasPrimaryKey(table) {
			modeled[postgresRelationNameKey(
				table.Schema,
				postgresAutomaticRelationName(table.Name, "", "pkey"),
			)] = struct{}{}
		}
		if table.Identity != nil {
			modeled[postgresRelationNameKey(
				table.Schema,
				postgresAutomaticRelationName(
					table.Name,
					table.Identity.Column,
					"seq",
				),
			)] = struct{}{}
		}
	}
	var reservations []TargetSchemaEvolutionNameReservation
	for _, relation := range relations {
		key := postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)
		if _, represented := modeled[key]; represented {
			continue
		}
		switch relation.relationKind {
		case "i", "S":
			reservations = append(
				reservations,
				TargetSchemaEvolutionNameReservation{
					Scope:     "relation",
					Namespace: relation.namespace,
					Name:      relation.name,
				},
			)
		case "r":
			return nil, fmt.Errorf(
				"ordinary PostgreSQL table %s.%s was omitted from the logical catalog",
				relation.namespace,
				relation.name,
			)
		default:
			return nil, fmt.Errorf(
				"unsupported PostgreSQL relation %s.%s cannot be represented as a target evolution reservation",
				relation.namespace,
				relation.name,
			)
		}
	}
	return reservations, nil
}

func validatePostgresTargetEvolutionCreateNames(
	created map[targetSchemaEvolutionTableKey]struct{},
	completeDesired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) error {
	planned, err := planPostgresDropRecreateRelationNames(
		completeDesired,
	)
	if err != nil {
		return fmt.Errorf(
			"plan complete PostgreSQL evolution relation names: %w",
			err,
		)
	}
	actualTables := actual.Tables()
	actualPlanned, err := planPostgresDropRecreateRelationNames(actualTables)
	if err != nil {
		return fmt.Errorf(
			"plan actual PostgreSQL evolution relation names: %w",
			err,
		)
	}
	existingNames := make(map[string]struct{}, len(actualPlanned))
	for _, relation := range actualPlanned {
		existingNames[postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)] = struct{}{}
	}
	for _, reservation := range actual.Reservations() {
		if reservation.Scope != "relation" {
			continue
		}
		existingNames[postgresRelationNameKey(
			reservation.Namespace,
			reservation.Name,
		)] = struct{}{}
	}
	actualTableKeys := indexTargetSchemaEvolutionTables(actualTables)
	for _, relation := range planned {
		owner := targetSchemaEvolutionTableKey{
			schema: relation.namespace,
			table:  relation.table,
		}
		if _, isCreated := created[owner]; !isCreated {
			continue
		}
		key := postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)
		if _, collision := existingNames[key]; !collision {
			continue
		}
		if _, resumableTable := actualTableKeys[owner]; resumableTable {
			// The core compares the complete actual table with the declared
			// create prefixes. A relation owned by that already-created table
			// is allowed here only so an exact prefix can resume.
			continue
		}
		return fmt.Errorf(
			"planned PostgreSQL evolution %s %s.%s collides with an existing relation reservation",
			relation.kind,
			relation.namespace,
			relation.name,
		)
	}
	return nil
}

func rejectPostgresTargetEvolutionDependencies(
	ctx context.Context,
	queryer engine.PostgresCatalogQueryer,
	namespace string,
) error {
	rows, err := queryer.QueryContext(
		ctx,
		postgresTargetEvolutionDependencyQuery,
		namespace,
	)
	if err != nil {
		return fmt.Errorf(
			"inspect PostgreSQL target namespace %s dependencies: %w",
			namespace,
			err,
		)
	}
	defer rows.Close()
	if rows.Next() {
		var (
			kind                string
			dependentNamespace  string
			dependentName       string
			referencedNamespace string
			referencedName      string
		)
		if err := rows.Scan(
			&kind,
			&dependentNamespace,
			&dependentName,
			&referencedNamespace,
			&referencedName,
		); err != nil {
			return fmt.Errorf(
				"read PostgreSQL target dependency hazard: %w",
				err,
			)
		}
		return fmt.Errorf(
			"PostgreSQL target namespace has external %s dependency %s.%s -> %s.%s; complete evolution cannot prove that hidden dependent shape",
			kind,
			dependentNamespace,
			dependentName,
			referencedNamespace,
			referencedName,
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate PostgreSQL target dependency hazards: %w",
			err,
		)
	}
	return rows.Close()
}

func validatePostgresTargetEvolutionPlanNamespace(
	plan TargetSchemaEvolutionPlan,
	namespace string,
) error {
	if !plan.valid() || plan.Target() != schema.Postgres {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"PostgreSQL target adapter requires a valid postgres evolution plan",
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
						"plan table %s.%s is outside PostgreSQL target namespace %s",
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
					"plan reservation %s/%s.%s is outside PostgreSQL target namespace %s",
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

func validatePostgresTargetEvolutionRelationNames(
	plan TargetSchemaEvolutionPlan,
	existing []postgresTargetEvolutionRelation,
) error {
	if len(plan.states) == 0 {
		return fmt.Errorf("PostgreSQL evolution plan has no catalog states")
	}
	initial := indexTargetSchemaEvolutionTables(plan.states[0])
	final := plan.states[len(plan.states)-1]
	newTables := make(map[targetSchemaEvolutionTableKey]struct{})
	for _, table := range final {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, existed := initial[key]; !existed {
			newTables[key] = struct{}{}
		}
	}
	if len(newTables) == 0 {
		return nil
	}
	plannedFinal, err := planPostgresDropRecreateRelationNames(final)
	if err != nil {
		return fmt.Errorf(
			"plan PostgreSQL evolution relation names: %w",
			err,
		)
	}
	currentPlanned, err := planPostgresDropRecreateRelationNames(
		plan.states[plan.observedPrefix],
	)
	if err != nil {
		return fmt.Errorf(
			"plan current PostgreSQL evolution relation names: %w",
			err,
		)
	}
	currentNames := make(map[string]struct{}, len(currentPlanned))
	for _, relation := range currentPlanned {
		currentNames[postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)] = struct{}{}
	}
	existingNames := make(map[string]postgresTargetEvolutionRelation, len(existing))
	for _, relation := range existing {
		existingNames[postgresRelationNameKey(
			relation.namespace,
			relation.name,
		)] = relation
	}
	for _, relation := range plannedFinal {
		if _, created := newTables[targetSchemaEvolutionTableKey{
			schema: relation.namespace,
			table:  relation.table,
		}]; !created {
			continue
		}
		key := postgresRelationNameKey(relation.namespace, relation.name)
		existingRelation, collision := existingNames[key]
		if !collision {
			continue
		}
		if _, alreadyVerified := currentNames[key]; alreadyVerified {
			continue
		}
		return fmt.Errorf(
			"planned PostgreSQL evolution %s %s.%s collides with existing %s relation before its declared create step",
			relation.kind,
			relation.namespace,
			relation.name,
			describePostgresRelationKind(existingRelation.relationKind),
		)
	}
	return nil
}

func postgresTargetEvolutionAdvisoryKey(namespace string) int64 {
	digest := sha256.Sum256(
		[]byte("dmtx/postgres-target-schema-evolution/v1\x00" + namespace),
	)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func postgresTargetEvolutionNamespace(namespace string) string {
	if namespace == "" {
		return "public"
	}
	return namespace
}
