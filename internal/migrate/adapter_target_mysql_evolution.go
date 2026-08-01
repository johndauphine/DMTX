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

const mysqlTargetEvolutionLockWaitSeconds = 15

var _ adapterTargetSchemaEvolutionCapability = (*mysqlTargetAdapter)(nil)

// TargetSchemaEvolutionDialect advertises only the canonical MySQL dialect.
// adapter.flavor, captured from the live target during adapter open, remains
// the authority for MySQL 8.0 versus MariaDB 10.11 catalog/session behavior.
func (*mysqlTargetAdapter) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.MySQL
}

func (adapter *mysqlTargetAdapter) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	if adapter == nil {
		return nil
	}
	return mysqlTargetSchemaEvolutionCreatePlanner{flavor: adapter.flavor}
}

// PreflightTargetSchemaEvolution reads a complete, flavor-pinned catalog and
// constructs a deterministic immutable operation plan. It performs no DDL.
func (adapter *mysqlTargetAdapter) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	if request.target != schema.MySQL {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"MySQL target adapter requires target dialect mysql",
			nil,
		)
	}
	if err := validateMySQLTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"MySQL target evolution adapter is not configured",
			err,
		)
	}
	plan, err := PreflightTargetSchemaEvolution(ctx, request, adapter)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateMySQLTargetEvolutionPlanNamespace(plan, adapter.namespace); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	return plan, nil
}

// ReadTargetSchemaEvolutionCatalog uses one repeatable-read, read-only target
// transaction. MySQL DDL auto-commits, so this reader is intentionally also
// usable by the mutation session after every statement to classify recovery
// only from an exact declared prefix.
func (adapter *mysqlTargetAdapter) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if err := validateMySQLTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"acquire MySQL target schema evolution connection: %w",
			err,
		)
	}
	defer connection.Close()
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"begin MySQL target schema evolution catalog snapshot: %w",
			err,
		)
	}
	catalog, readErr := readMySQLTargetEvolutionCatalog(
		ctx,
		transaction,
		adapter.flavor,
		adapter.namespace,
	)
	rollbackErr := transaction.Rollback()
	if rollbackErr != nil {
		_ = connection.Raw(func(any) error { return driver.ErrBadConn })
	}
	if readErr != nil {
		if rollbackErr != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"%w; roll back MySQL catalog snapshot: %v",
				readErr,
				rollbackErr,
			)
		}
		return TargetSchemaEvolutionCatalog{}, readErr
	}
	if rollbackErr != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"roll back MySQL target schema evolution catalog snapshot: %w",
			rollbackErr,
		)
	}
	return catalog, nil
}

// ApplyTargetSchemaEvolutionPlan uses a pinned target session and a DMTX
// named DDL fence. Because MySQL-family DDL commits each statement, the
// session re-reads the full target catalog after every statement; generic
// ApplyTargetSchemaEvolution then classifies only an exact committed prefix.
func (adapter *mysqlTargetAdapter) ApplyTargetSchemaEvolutionPlan(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
) (result error) {
	if err := validateMySQLTargetEvolutionAdapter(adapter); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"MySQL target evolution adapter is not configured",
			err,
		)
	}
	if err := validateMySQLTargetEvolutionPlanNamespace(plan, adapter.namespace); err != nil {
		return err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire pinned MySQL target connection",
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			_ = connection.Close()
		}
	}()
	if err := engine.VerifyMySQLTargetForFlavor(
		ctx,
		connection,
		adapter.flavor,
	); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"verify pinned MySQL target session",
			err,
		)
	}

	lockName := mysqlTargetEvolutionLockName(adapter.namespace)
	var locked int
	if err := connection.QueryRowContext(
		ctx,
		"SELECT GET_LOCK(?, ?)",
		lockName,
		mysqlTargetEvolutionLockWaitSeconds,
	).Scan(&locked); err != nil || locked != 1 {
		if err == nil {
			err = fmt.Errorf("GET_LOCK returned %d", locked)
		}
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire deterministic MySQL target database fence",
			err,
		)
	}
	released := false
	defer func() {
		if released {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			targetSchemaEvolutionVerificationTimeout,
		)
		defer cancel()
		if releaseErr := releaseMySQLTargetEvolutionLock(
			cleanupCtx,
			connection,
			lockName,
		); releaseErr != nil {
			discardMySQLConnection(connection)
			result = joinMySQLTargetEvolutionReleaseError(result, releaseErr)
		}
	}()

	session := &mysqlTargetEvolutionMutationSession{
		executor: connection,
		readCatalog: func(readCtx context.Context) (
			TargetSchemaEvolutionCatalog,
			error,
		) {
			return readMySQLTargetEvolutionCatalog(
				readCtx,
				connection,
				adapter.flavor,
				adapter.namespace,
			)
		},
		plan: plan,
	}
	if err := ApplyTargetSchemaEvolution(ctx, plan, session); err != nil {
		return err
	}
	if err := releaseMySQLTargetEvolutionLock(ctx, connection, lockName); err != nil {
		discardMySQLConnection(connection)
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"DDL fence",
			targetSchemaEvolutionRecoveryWording(
				"MySQL evolution completed but the DMTX DDL fence could not be released",
			),
			err,
		)
	}
	released = true
	if err := connection.Close(); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"MySQL evolution completed but the mutation connection could not be released before an independent catalog read",
			),
			err,
		)
	}
	closed = true
	committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	return verifyMySQLTargetEvolutionCommittedCatalog(plan, committed, readErr)
}

func validateMySQLTargetEvolutionAdapter(adapter *mysqlTargetAdapter) error {
	if adapter == nil || adapter.database == nil {
		return fmt.Errorf("database is not configured")
	}
	if strings.TrimSpace(adapter.namespace) == "" {
		return fmt.Errorf("target database is not configured")
	}
	switch adapter.flavor {
	case engine.MySQLServerFlavorOracle80, engine.MySQLServerFlavorMariaDB1011:
		return nil
	default:
		return fmt.Errorf("server flavor is not a certified MySQL 8.0 or MariaDB 10.11 target")
	}
}

func mysqlTargetEvolutionLockName(namespace string) string {
	digest := sha256.Sum256([]byte("dmtx:stage4:mysql-evolution:" + namespace))
	return "dmtx-s4-evo-" + hex.EncodeToString(digest[:20])
}

func releaseMySQLTargetEvolutionLock(
	ctx context.Context,
	connection *sql.Conn,
	lockName string,
) error {
	var released int
	if err := connection.QueryRowContext(
		ctx,
		"SELECT RELEASE_LOCK(?)",
		lockName,
	).Scan(&released); err != nil {
		return fmt.Errorf("release MySQL target evolution lock: %w", err)
	}
	if released != 1 {
		return fmt.Errorf("release MySQL target evolution lock returned %d", released)
	}
	return nil
}

// joinMySQLTargetEvolutionReleaseError preserves both the operation failure
// and a failed best-effort fence release. The latter is material even when a
// DDL operation failed: retaining it tells recovery to discard the session
// and prove the immutable operation prefix again on a fresh connection.
func joinMySQLTargetEvolutionReleaseError(primary error, releaseErr error) error {
	if releaseErr == nil {
		return primary
	}
	cleanup := targetSchemaEvolutionError(
		TargetSchemaEvolutionVerifyFailed,
		"DDL fence",
		targetSchemaEvolutionRecoveryWording(
			"MySQL evolution completed or failed but the DMTX DDL fence could not be released",
		),
		releaseErr,
	)
	return errors.Join(primary, cleanup)
}

type mysqlTargetSchemaEvolutionCreatePlanner struct {
	flavor engine.MySQLServerFlavor
}

func (planner mysqlTargetSchemaEvolutionCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesiredTables []schema.Table,
	actualCatalog TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.MySQL {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"MySQL target create planner cannot render %q",
			target,
		)
	}
	if err := validateMySQLTargetEvolutionFlavor(planner.flavor); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}
	created := cloneTargetSchemaEvolutionTables(tables)
	sortTargetSchemaEvolutionTables(created)
	if len(created) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"MySQL target create planner has no tables",
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
	if err := validateMySQLTargetEvolutionCreateNames(
		createdKeys,
		desired,
		actualCatalog,
	); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}
	objects, err := schema.PlanMySQLDropRecreateObjects(desired)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"plan complete MySQL target objects: %w",
			err,
		)
	}

	state := make([]schema.Table, 0, len(created))
	steps := make([]TargetSchemaCreateStep, 0, len(created)+len(objects))
	for _, table := range created {
		statement, err := schema.CreateTableDDL(schema.MySQL, table)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"plan complete MySQL target table %s.%s: %w",
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
		if err := addMySQLTargetEvolutionObject(state, created, object); err != nil {
			return CompleteTargetSchemaCreateBundle{}, err
		}
		statement, err := schema.MySQLPlannedObjectDDL(desired, object)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"seal MySQL target object %s.%s.%s: %w",
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
	return NewCompleteTargetSchemaCreateBundle(schema.MySQL, created, steps)
}

func validateMySQLTargetEvolutionFlavor(flavor engine.MySQLServerFlavor) error {
	switch flavor {
	case engine.MySQLServerFlavorOracle80,
		engine.MySQLServerFlavorMariaDB1011:
		return nil
	default:
		return fmt.Errorf("MySQL target schema evolution requires a certified MySQL 8.0 or MariaDB 10.11 flavor")
	}
}

func addMySQLTargetEvolutionObject(
	state []schema.Table,
	final []schema.Table,
	object schema.MySQLObjectStatement,
) error {
	key := targetSchemaEvolutionTableKey{schema: object.Schema, table: object.Table}
	stateIndex := findTargetSchemaEvolutionTable(state, key)
	finalIndex := findTargetSchemaEvolutionTable(final, key)
	if stateIndex < 0 || finalIndex < 0 || strings.TrimSpace(object.Name) == "" {
		return fmt.Errorf(
			"planned MySQL object %s.%s.%s has incomplete created-table authority",
			object.Schema,
			object.Table,
			object.Name,
		)
	}
	switch object.Kind {
	case schema.MySQLIndexObject:
		for _, index := range final[finalIndex].Indexes {
			if index.Name == object.Name {
				state[stateIndex].Indexes = append(state[stateIndex].Indexes, index)
				return nil
			}
		}
	case schema.MySQLCheckObject:
		for _, check := range final[finalIndex].Checks {
			if check.Name == object.Name {
				state[stateIndex].Checks = append(state[stateIndex].Checks, check)
				return nil
			}
		}
	case schema.MySQLForeignKeyObject:
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
			"MySQL object plan returned unsupported object kind %d",
			object.Kind,
		)
	}
	return fmt.Errorf(
		"planned MySQL object name %s on %s.%s does not exactly match target metadata; explicit round-trippable object names are required",
		object.Name,
		object.Schema,
		object.Table,
	)
}

func validateMySQLTargetEvolutionCreateNames(
	created map[targetSchemaEvolutionTableKey]struct{},
	completeDesired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) error {
	actualTables := actual.Tables()
	actualIndex := indexTargetSchemaEvolutionTables(actualTables)
	for _, table := range completeDesired {
		key := targetSchemaEvolutionTableKey{schema: table.Schema, table: table.Name}
		if _, isCreated := created[key]; !isCreated {
			continue
		}
		if _, resumed := actualIndex[key]; resumed {
			continue
		}
		for _, reservation := range actual.Reservations() {
			if reservation.Scope == "relation" &&
				reservation.Namespace == table.Schema &&
				strings.EqualFold(reservation.Name, table.Name) {
				return fmt.Errorf(
					"planned MySQL evolution table %s.%s collides with an existing relation reservation",
					table.Schema,
					table.Name,
				)
			}
		}
	}
	objects, err := schema.PlanMySQLDropRecreateObjects(completeDesired)
	if err != nil {
		return fmt.Errorf("plan MySQL evolution object names: %w", err)
	}
	existingConstraints := make(map[string]targetSchemaEvolutionTableKey)
	for _, table := range actualTables {
		key := targetSchemaEvolutionTableKey{schema: table.Schema, table: table.Name}
		for _, check := range table.Checks {
			existingConstraints[strings.ToLower(check.Name)] = key
		}
		for _, foreignKey := range table.ForeignKeys {
			existingConstraints[strings.ToLower(foreignKey.Name)] = key
		}
	}
	for _, object := range objects {
		owner := targetSchemaEvolutionTableKey{schema: object.Schema, table: object.Table}
		if _, isCreated := created[owner]; !isCreated ||
			(object.Kind != schema.MySQLCheckObject && object.Kind != schema.MySQLForeignKeyObject) {
			continue
		}
		if currentOwner, collision := existingConstraints[strings.ToLower(object.Name)]; collision && currentOwner != owner {
			return fmt.Errorf(
				"planned MySQL evolution constraint %s on %s.%s collides with existing target constraint on %s.%s",
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

type mysqlTargetEvolutionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type mysqlTargetEvolutionMutationSession struct {
	executor    mysqlTargetEvolutionExecutor
	readCatalog func(context.Context) (TargetSchemaEvolutionCatalog, error)
	plan        TargetSchemaEvolutionPlan
}

func (session *mysqlTargetEvolutionMutationSession) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if session == nil || session.readCatalog == nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"MySQL target schema evolution catalog reader is not configured",
		)
	}
	return session.readCatalog(ctx)
}

func (session *mysqlTargetEvolutionMutationSession) ExecuteTargetSchemaEvolution(
	ctx context.Context,
	operations []TargetSchemaEvolutionOperation,
) error {
	if session == nil || session.executor == nil {
		return fmt.Errorf(
			"MySQL target schema evolution executor is not configured",
		)
	}
	if !sameTargetSchemaEvolutionOperations(
		session.plan.PendingOperations(), operations,
	) {
		return fmt.Errorf(
			"MySQL target schema evolution executor received statements outside the immutable pending suffix",
		)
	}
	start := session.plan.observedPrefix
	for index, operation := range operations {
		statements := operation.Statements()
		if len(statements) != 1 || strings.TrimSpace(statements[0]) == "" {
			return fmt.Errorf(
				"MySQL target schema evolution operation %d does not contain exactly one core-rendered statement",
				start+index,
			)
		}
		if _, err := session.executor.ExecContext(ctx, statements[0]); err != nil {
			return fmt.Errorf(
				"execute MySQL target schema evolution operation %d (%s): %w",
				start+index,
				operation.Action(),
				err,
			)
		}
		actual, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
		if err != nil {
			return fmt.Errorf(
				"read MySQL catalog after operation %d (%s): %w",
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
				"MySQL catalog after operation %d (%s) does not match its exact declared cumulative state: %w",
				start+index,
				operation.Action(),
				err,
			)
		}
	}
	return nil
}

func readMySQLTargetEvolutionCatalog(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) (TargetSchemaEvolutionCatalog, error) {
	if err := validateMySQLTargetEvolutionEnvironment(
		ctx,
		queryer,
		flavor,
		namespace,
	); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	tableNames, reservations, err := mysqlTargetEvolutionRelations(
		ctx,
		queryer,
		flavor,
		namespace,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	tables := make([]schema.Table, 0, len(tableNames))
	for _, name := range tableNames {
		table, err := engine.InspectMySQLTableForFlavor(
			ctx,
			queryer,
			flavor,
			namespace,
			name,
		)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"inspect complete MySQL target table %s.%s: %w",
				namespace,
				name,
				err,
			)
		}
		tables = append(tables, table)
	}
	sortTargetSchemaEvolutionTables(tables)
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"validate complete MySQL target database %s: %w",
			namespace,
			err,
		)
	}
	return catalog, nil
}

func validateMySQLTargetEvolutionEnvironment(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) error {
	if strings.TrimSpace(namespace) == "" ||
		strings.TrimSpace(namespace) != namespace {
		return fmt.Errorf("MySQL target database is not a canonical user database")
	}
	switch strings.ToLower(namespace) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return fmt.Errorf("MySQL target database %q is a reserved system database", namespace)
	}
	if err := engine.VerifyMySQLTargetForFlavor(ctx, queryer, flavor); err != nil {
		return fmt.Errorf("verify MySQL target flavor/session contract: %w", err)
	}
	var databaseName string
	var readOnly int
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		var superReadOnly int
		if err := queryer.QueryRowContext(ctx, `SELECT DATABASE(), @@GLOBAL.read_only, @@GLOBAL.super_read_only`).Scan(
			&databaseName,
			&readOnly,
			&superReadOnly,
		); err != nil {
			return fmt.Errorf("inspect MySQL 8.0 target evolution environment: %w", err)
		}
		if superReadOnly != 0 {
			return fmt.Errorf("MySQL 8.0 target is super-read-only")
		}
	case engine.MySQLServerFlavorMariaDB1011:
		if err := queryer.QueryRowContext(ctx, `SELECT DATABASE(), @@GLOBAL.read_only`).Scan(
			&databaseName,
			&readOnly,
		); err != nil {
			return fmt.Errorf("inspect MariaDB 10.11 target evolution environment: %w", err)
		}
	default:
		return fmt.Errorf("MySQL target schema evolution has unsupported server flavor")
	}
	if databaseName != namespace {
		return fmt.Errorf(
			"MySQL target evolution connection database %q differs from configured database %q",
			databaseName,
			namespace,
		)
	}
	if readOnly != 0 {
		return fmt.Errorf("MySQL target database is read-only")
	}
	privileges, err := mysqlTargetEvolutionPrivileges(ctx, queryer, namespace)
	if err != nil {
		return err
	}
	for _, required := range []string{"CREATE", "ALTER", "INDEX", "REFERENCES"} {
		if !privileges[required] {
			return fmt.Errorf(
				"MySQL target role lacks required %s privilege on database %s",
				required,
				namespace,
			)
		}
	}
	return nil
}

func mysqlTargetEvolutionPrivileges(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	namespace string,
) (map[string]bool, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT PRIVILEGE_TYPE
		  FROM information_schema.SCHEMA_PRIVILEGES
		 WHERE TABLE_SCHEMA = ?
		   AND GRANTEE = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		   )
		UNION
		SELECT PRIVILEGE_TYPE
		  FROM information_schema.USER_PRIVILEGES
		 WHERE GRANTEE = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		 )`, namespace)
	if err != nil {
		return nil, fmt.Errorf("inspect MySQL target evolution privileges: %w", err)
	}
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			return nil, fmt.Errorf("read MySQL target evolution privilege: %w", err)
		}
		result[strings.ToUpper(strings.TrimSpace(privilege))] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate MySQL target evolution privileges: %w", err)
	}
	if result["ALL PRIVILEGES"] {
		for _, privilege := range []string{"CREATE", "ALTER", "INDEX", "REFERENCES"} {
			result[privilege] = true
		}
	}
	return result, nil
}

func mysqlTargetEvolutionRelations(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) ([]string, []TargetSchemaEvolutionNameReservation, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT TABLE_NAME, TABLE_TYPE
		  FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = ?
		 ORDER BY TABLE_NAME, TABLE_TYPE`, namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("list MySQL target evolution relations: %w", err)
	}
	defer rows.Close()
	type relation struct{ name, kind string }
	var relations []relation
	for rows.Next() {
		var item relation
		if err := rows.Scan(&item.name, &item.kind); err != nil {
			return nil, nil, fmt.Errorf("read MySQL target evolution relation: %w", err)
		}
		relations = append(relations, item)
	}
	// The journal inspection issues further catalog and table queries through
	// this same queryer. Finish TABLES before that work: MySQL will otherwise
	// treat the nested query as a protocol violation on a pinned transaction.
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, nil, fmt.Errorf("iterate MySQL target evolution relations: %w", err)
	}
	var tables []string
	var reservations []TargetSchemaEvolutionNameReservation
	for _, relation := range relations {
		name, kind := relation.name, relation.kind
		// MySQL/MariaDB target admission pins lower_case_table_names=0, so
		// this exact lower-case name cannot hide a distinct user relation.
		// Only the complete authenticated private journal is omitted; any
		// same-name view/table collision fails before evolution can treat it as
		// user-owned target state.
		if name == mysqlDeleteJournalTable {
			journal, journalErr := inspectMySQLDeleteReceiptJournal(
				ctx,
				queryer,
				flavor,
				namespace,
			)
			if journalErr != nil {
				return nil, nil, fmt.Errorf(
					"authenticate private MySQL delete receipt journal reservation: %w",
					journalErr,
				)
			}
			if !journal.Exists || journal.EmptyPrefix {
				return nil, nil, fmt.Errorf(
					"private MySQL delete receipt journal reservation lacks immutable authenticated authority",
				)
			}
			continue
		}
		if strings.EqualFold(kind, "BASE TABLE") {
			tables = append(tables, name)
			continue
		}
		reservations = append(reservations, TargetSchemaEvolutionNameReservation{
			Scope:     "relation",
			Namespace: namespace,
			Name:      name,
		})
	}
	sort.Strings(tables)
	return tables, reservations, nil
}

func validateMySQLTargetEvolutionPlanNamespace(
	plan TargetSchemaEvolutionPlan,
	namespace string,
) error {
	if !plan.valid() || plan.target != schema.MySQL {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"MySQL target schema evolution plan is incomplete or has the wrong dialect",
			nil,
		)
	}
	for _, state := range plan.states {
		for _, table := range state {
			if table.Schema != namespace {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"apply",
					"MySQL target schema evolution plan contains a table outside the configured database",
					nil,
				)
			}
		}
	}
	for _, reservation := range plan.reservations {
		if reservation.Scope != "relation" || reservation.Namespace != namespace {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"apply",
				"MySQL target schema evolution plan contains an unsupported reservation outside the configured database",
				nil,
			)
		}
	}
	return nil
}

func verifyMySQLTargetEvolutionCommittedCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"MySQL evolution completed but an independent complete catalog snapshot could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"MySQL evolution completed but the independent complete catalog snapshot is structurally invalid",
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
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"MySQL evolution completed but an independent snapshot found concurrent or unexpected catalog drift",
			),
			err,
		)
	}
	return nil
}
