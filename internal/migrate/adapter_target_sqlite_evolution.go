package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

const sqliteTargetEvolutionNamespace = "main"

var _ adapterTargetSchemaEvolutionCapability = (*sqliteTargetAdapter)(nil)

// TargetSchemaEvolutionDialect advertises the SQLite operations that the
// adapter can apply under its pinned BEGIN IMMEDIATE evolution fence. Nullable
// additions and complete creation use direct DDL; proved relax/widen actions
// use a retained-row copy/swap bundle rather than unsafe in-place SQL.
func (*sqliteTargetAdapter) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.SQLite
}

func (*sqliteTargetAdapter) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	return sqliteTargetSchemaEvolutionCreatePlanner{}
}

// PreflightTargetSchemaEvolution builds the immutable plan before any target
// mutation. The generic proof engine binds every action to exact catalog
// authority, then the SQLite adapter rejects any copy/swap shape it cannot
// reconstruct faithfully.
func (adapter *sqliteTargetAdapter) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	if request.target != schema.SQLite {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"SQLite target adapter requires target dialect sqlite",
			nil,
		)
	}
	if err := validateSQLiteTargetEvolutionRequest(request); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateSQLiteTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"SQLite target evolution adapter is not configured",
			err,
		)
	}
	plan, err := PreflightTargetSchemaEvolution(ctx, request, adapter)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := adapter.validateSQLiteTargetEvolutionCopySwapPlan(ctx, plan); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	return plan, nil
}

func validateSQLiteTargetEvolutionRequest(request TargetSchemaEvolutionRequest) error {
	for _, decision := range request.decisions {
		switch decision.contract.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
		default:
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"SQLite schema evolution received an unsupported schema-contract action "+string(decision.contract.Action),
				nil,
			)
		}
	}
	return nil
}

// ReadTargetSchemaEvolutionCatalog obtains one read transaction snapshot. It
// models every user table, including target-only tables, and reserves views
// and triggers with both their names and exact sqlite_schema SQL fingerprints.
// That makes an independently edited object drift rather than an apparent
// immutable evolution prefix.
func (adapter *sqliteTargetAdapter) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if err := validateSQLiteTargetEvolutionAdapter(adapter); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"acquire SQLite target schema evolution catalog connection: %w", err,
		)
	}
	defer connection.Close()
	if err := requireSQLiteForeignKeys(ctx, connection); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	if _, err := connection.ExecContext(ctx, "BEGIN"); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"begin SQLite target schema evolution catalog snapshot: %w", err,
		)
	}
	catalog, readErr := readSQLiteTargetEvolutionCatalog(ctx, connection)
	rollbackErr := rollbackSQLiteTargetEvolutionTransaction(ctx, connection)
	if rollbackErr != nil {
		discardSQLiteTargetEvolutionConnection(connection)
	}
	if readErr != nil {
		if rollbackErr != nil {
			return TargetSchemaEvolutionCatalog{}, errors.Join(
				readErr,
				fmt.Errorf("roll back SQLite catalog snapshot: %w", rollbackErr),
			)
		}
		return TargetSchemaEvolutionCatalog{}, readErr
	}
	if rollbackErr != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"roll back SQLite target schema evolution catalog snapshot: %w", rollbackErr,
		)
	}
	return catalog, nil
}

// ApplyTargetSchemaEvolutionPlan holds BEGIN IMMEDIATE from the exact
// pre-apply snapshot through all DDL and catalog-prefix checks. SQLite DDL is
// transactional, so an execution error rolls the whole suffix back; a commit
// acknowledgement error is resolved only by a fresh independent catalog read.
func (adapter *sqliteTargetAdapter) ApplyTargetSchemaEvolutionPlan(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
) (result error) {
	if err := validateSQLiteTargetEvolutionAdapter(adapter); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"SQLite target evolution adapter is not configured",
			err,
		)
	}
	if plan.Target() != schema.SQLite {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"SQLite target evolution plan has a different dialect",
			nil,
		)
	}
	// Apply also performs the read-only copy/swap admission pass. Callers
	// normally obtain a plan through PreflightTargetSchemaEvolution, but an
	// immutable plan can be resumed or supplied directly. In either case an
	// incoming foreign key must be refused before BEGIN IMMEDIATE: SQLite DROP
	// TABLE can execute that FK's ON DELETE action even when deferred checking
	// is enabled.
	if err := adapter.validateSQLiteTargetEvolutionCopySwapPlan(ctx, plan); err != nil {
		return err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire pinned SQLite target connection",
			err,
		)
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := connection.Close(); closeErr != nil {
				discardSQLiteTargetEvolutionConnection(connection)
				result = joinSQLiteTargetEvolutionCleanupError(result, closeErr)
			}
		}
	}()
	if err := requireSQLiteForeignKeys(ctx, connection); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"verify pinned SQLite foreign-key enforcement",
			err,
		)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"DDL fence",
			"acquire SQLite BEGIN IMMEDIATE writer reservation",
			err,
		)
	}
	active := true
	rollback := func(cause error) error {
		if !active {
			return cause
		}
		active = false
		if rollbackErr := rollbackSQLiteTargetEvolutionTransaction(ctx, connection); rollbackErr != nil {
			discardSQLiteTargetEvolutionConnection(connection)
			return errors.Join(cause, targetSchemaEvolutionError(
				TargetSchemaEvolutionApplyFailed,
				"rollback",
				targetSchemaEvolutionRecoveryWording(
					"SQLite target schema evolution failed and rollback also failed",
				),
				rollbackErr,
			))
		}
		return cause
	}

	session := &sqliteTargetEvolutionMutationSession{
		executor: connection,
		queryer:  connection,
		readCatalog: func(readCtx context.Context) (TargetSchemaEvolutionCatalog, error) {
			return readSQLiteTargetEvolutionCatalog(readCtx, connection)
		},
		plan: plan,
	}
	if err := ApplyTargetSchemaEvolution(ctx, plan, session); err != nil {
		return rollback(err)
	}
	// Closing an active SQLite connection rolls back, so COMMIT must happen
	// before the independent reader can use the single-connection pool.
	if _, err := adapter.commitSQLiteTargetEvolution(ctx, connection); err != nil {
		active = false
		discardSQLiteTargetEvolutionConnection(connection)
		closeErr := connection.Close()
		// discardSQLiteTargetEvolutionConnection intentionally returns the
		// pinned driver connection as ErrBadConn. database/sql may therefore
		// report ErrConnDone from this follow-up Close even though the resource
		// has already been discarded; do not turn a fully authenticated
		// commit-no-ack recovery into a false cleanup failure.
		if errors.Is(closeErr, sql.ErrConnDone) {
			closeErr = nil
		}
		closed = true
		verificationCtx, cancel := sqliteTargetEvolutionDetachedContext(ctx)
		defer cancel()
		return errors.Join(
			adapter.classifySQLiteTargetEvolutionCommitAmbiguity(
				verificationCtx,
				plan,
				session.retained,
				err,
			),
			joinSQLiteTargetEvolutionCleanupError(nil, closeErr),
		)
	}
	active = false
	if err := connection.Close(); err != nil {
		discardSQLiteTargetEvolutionConnection(connection)
		closed = true
		verificationCtx, cancel := sqliteTargetEvolutionDetachedContext(ctx)
		defer cancel()
		committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(verificationCtx)
		return errors.Join(
			joinSQLiteTargetEvolutionCleanupError(nil, err),
			adapter.verifySQLiteTargetEvolutionCommittedAuthority(
				verificationCtx,
				plan,
				session.retained,
				committed,
				readErr,
			),
		)
	}
	closed = true
	verificationCtx, cancel := sqliteTargetEvolutionDetachedContext(ctx)
	defer cancel()
	committed, readErr := adapter.ReadTargetSchemaEvolutionCatalog(verificationCtx)
	return adapter.verifySQLiteTargetEvolutionCommittedAuthority(
		verificationCtx,
		plan,
		session.retained,
		committed,
		readErr,
	)
}

func (adapter *sqliteTargetAdapter) commitSQLiteTargetEvolution(
	ctx context.Context,
	connection *sql.Conn,
) (sql.Result, error) {
	if adapter != nil && adapter.evolutionCommit != nil {
		return adapter.evolutionCommit(ctx, connection)
	}
	return connection.ExecContext(ctx, "COMMIT")
}

func validateSQLiteTargetEvolutionAdapter(adapter *sqliteTargetAdapter) error {
	if adapter == nil || adapter.database == nil {
		return fmt.Errorf("database is not configured")
	}
	return nil
}

type sqliteTargetEvolutionMutationSession struct {
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}
	queryer     sqliteQueryer
	readCatalog func(context.Context) (TargetSchemaEvolutionCatalog, error)
	plan        TargetSchemaEvolutionPlan
	retained    []sqliteTargetEvolutionRetainedDataAuthority
}

func (session *sqliteTargetEvolutionMutationSession) ReadTargetSchemaEvolutionCatalog(
	ctx context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if session == nil || session.readCatalog == nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"SQLite target schema evolution catalog reader is not configured",
		)
	}
	return session.readCatalog(ctx)
}

func (session *sqliteTargetEvolutionMutationSession) ExecuteTargetSchemaEvolution(
	ctx context.Context,
	operations []TargetSchemaEvolutionOperation,
) error {
	if session == nil || session.executor == nil {
		return fmt.Errorf("SQLite target schema evolution executor is not configured")
	}
	if session.queryer == nil {
		return fmt.Errorf("SQLite target schema evolution queryer is not configured")
	}
	if !sameTargetSchemaEvolutionOperations(session.plan.PendingOperations(), operations) {
		return fmt.Errorf(
			"SQLite target schema evolution executor received statements outside the immutable pending suffix",
		)
	}
	start := session.plan.observedPrefix
	copySwap := false
	for index, operation := range operations {
		statements := operation.Statements()
		operationIndex := start + index
		if len(statements) != 1 || strings.TrimSpace(statements[0]) == "" {
			return fmt.Errorf(
				"SQLite target schema evolution operation %d does not contain exactly one core-rendered statement",
				operationIndex,
			)
		}
		if statements[0] == sqliteTargetEvolutionCopySwapStatement {
			if err := session.executeSQLiteTargetEvolutionCopySwap(
				ctx,
				operationIndex,
				operation,
			); err != nil {
				return err
			}
			copySwap = true
		} else if _, err := session.executor.ExecContext(ctx, statements[0]); err != nil {
			return fmt.Errorf(
				"execute SQLite target schema evolution operation %d (%s): %w",
				operationIndex,
				operation.Action(),
				err,
			)
		}
		actual, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
		if err != nil {
			return fmt.Errorf(
				"read SQLite catalog after operation %d (%s): %w",
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
				"SQLite catalog after operation %d (%s) does not match its exact declared cumulative state: %w",
				start+index,
				operation.Action(),
				err,
			)
		}
	}
	if copySwap {
		if err := verifySQLiteTargetEvolutionCopySwapIntegrity(ctx, session.queryer); err != nil {
			return err
		}
	}
	return nil
}

// validateSQLiteTargetEvolutionCopySwapPlan performs only read-only checks.
// The same checks are repeated by the pinned mutation session immediately
// before its first destructive statement, so a catalog edit between preflight
// and BEGIN IMMEDIATE cannot turn into an unreviewed table rebuild.
func (adapter *sqliteTargetAdapter) validateSQLiteTargetEvolutionCopySwapPlan(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
) error {
	if !plan.valid() || plan.Target() != schema.SQLite {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"SQLite retained-row copy/swap requires a complete SQLite evolution plan",
			nil,
		)
	}
	for operationIndex, operation := range plan.operations {
		if operation.Action() != SchemaContractRelaxNullability &&
			operation.Action() != SchemaContractWidenType {
			if statements := operation.Statements(); len(statements) == 1 &&
				statements[0] == sqliteTargetEvolutionCopySwapStatement {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"SQLite retained-row copy/swap marker is bound to relax_nullability or widen_type only",
					nil,
				)
			}
			continue
		}
		if _, _, _, err := sqliteTargetEvolutionCopySwapOperation(
			plan,
			operationIndex,
			operation,
		); err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap operation",
				err,
			)
		}
		// A completed immutable prefix is authenticated by the catalog state,
		// but every pending rebuild must be screened now. In particular, an
		// add-column operation preceding a later copy/swap must not be allowed
		// to mutate a table whose trigger makes the subsequent reconstruction
		// unsafe.
		if operationIndex < plan.observedPrefix {
			continue
		}
		before, after, temporary, err := sqliteTargetEvolutionCopySwapOperation(
			plan,
			operationIndex,
			operation,
		)
		if err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap operation",
				err,
			)
		}
		if err := validateSQLiteTargetEvolutionCopySwapObjects(
			ctx,
			adapter.database,
			before.Name,
			temporary,
		); err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap object authority",
				err,
			)
		}
		if err := validateSQLiteTargetEvolutionPlannedIncomingForeignKeys(
			plan,
			operationIndex,
			before.Name,
		); err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap planned incoming foreign-key authority",
				err,
			)
		}
		if err := validateSQLiteTargetEvolutionCopySwapRendering(after); err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap reconstruction",
				err,
			)
		}
		if err := validateSQLiteTargetEvolutionNoIncomingForeignKeys(
			ctx,
			adapter.database,
			before.Name,
		); err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"validate SQLite retained-row copy/swap incoming foreign-key authority",
				err,
			)
		}
	}
	return nil
}

// validateSQLiteTargetEvolutionPlannedIncomingForeignKeys closes the gap
// between the live preflight catalog and a later copy/swap state. A preceding
// immutable CREATE/ADD operation can introduce a child relationship before a
// subsequent table rebuild. That relationship is already deterministic plan
// authority, so reject the whole pending suffix before BEGIN IMMEDIATE rather
// than discover it after an earlier transactional DDL statement.
func validateSQLiteTargetEvolutionPlannedIncomingForeignKeys(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
	table string,
) error {
	if operationIndex < 0 || operationIndex >= len(plan.states) ||
		strings.TrimSpace(table) == "" {
		return fmt.Errorf("copy/swap planned incoming foreign-key authority is incomplete")
	}
	state := plan.states[operationIndex]
	type incoming struct {
		child    string
		foreign  int
		onDelete string
		onUpdate string
	}
	var dependencies []incoming
	for _, candidate := range state {
		if strings.TrimSpace(candidate.Name) == "" {
			return fmt.Errorf("copy/swap planned incoming foreign-key authority has an empty child table identity")
		}
		for index, foreignKey := range candidate.ForeignKeys {
			if stage4SQLiteIdentifier(foreignKey.ReferencedTable) != stage4SQLiteIdentifier(table) {
				continue
			}
			onDelete := strings.TrimSpace(foreignKey.OnDelete)
			if onDelete == "" {
				onDelete = "UNKNOWN"
			}
			onUpdate := strings.TrimSpace(foreignKey.OnUpdate)
			if onUpdate == "" {
				onUpdate = "UNKNOWN"
			}
			dependencies = append(dependencies, incoming{
				child:    candidate.Name,
				foreign:  index,
				onDelete: onDelete,
				onUpdate: onUpdate,
			})
		}
	}
	if len(dependencies) == 0 {
		return nil
	}
	parts := make([]string, len(dependencies))
	for index, dependency := range dependencies {
		parts[index] = fmt.Sprintf(
			"%s(foreign_key=%d,on_delete=%s,on_update=%s)",
			dependency.child,
			dependency.foreign,
			dependency.onDelete,
			dependency.onUpdate,
		)
	}
	return fmt.Errorf(
		"copy/swap table %s has planned incoming foreign-key dependencies (%s)",
		table,
		strings.Join(parts, ", "),
	)
}

// validateSQLiteTargetEvolutionCopySwapRendering proves every DDL fragment
// needed to reconstruct the sealed final table before the writer reservation
// or any destructive operation. Catalog equality alone does not establish
// that a target-owned index/constraint can be rendered faithfully.
func validateSQLiteTargetEvolutionCopySwapRendering(after schema.Table) error {
	statement, err := schema.CreateTableDDL(schema.SQLite, after)
	if err != nil {
		return fmt.Errorf("render replacement table %s: %w", after.Name, err)
	}
	if _, err := schema.RenderDDLStatement(statement, schema.SQLite); err != nil {
		return fmt.Errorf("authenticate replacement table %s: %w", after.Name, err)
	}
	for _, index := range sqliteTargetEvolutionStandaloneIndexes(after.Indexes) {
		statement, err := schema.SQLitePlannedIndexDDL(after, index)
		if err != nil {
			return fmt.Errorf("render replacement index %s on %s: %w", index.Name, after.Name, err)
		}
		if _, err := schema.RenderDDLStatement(statement, schema.SQLite); err != nil {
			return fmt.Errorf("authenticate replacement index %s on %s: %w", index.Name, after.Name, err)
		}
	}
	return nil
}

func sqliteTargetEvolutionCopySwapOperation(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
	operation TargetSchemaEvolutionOperation,
) (schema.Table, schema.Table, string, error) {
	if operationIndex < 0 || operationIndex >= len(plan.operations) ||
		operationIndex+1 >= len(plan.states) {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation index %d is outside the immutable plan", operationIndex,
		)
	}
	if operation.Action() != SchemaContractRelaxNullability &&
		operation.Action() != SchemaContractWidenType {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d has unsupported action %s",
			operationIndex,
			operation.Action(),
		)
	}
	statements := operation.Statements()
	if len(statements) != 1 || statements[0] != sqliteTargetEvolutionCopySwapStatement {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d does not carry the immutable SQLite marker",
			operationIndex,
		)
	}
	objects := operation.Objects()
	if len(objects) != 1 || objects[0].Schema != "" ||
		strings.TrimSpace(objects[0].Table) == "" ||
		strings.TrimSpace(objects[0].Column) == "" {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d has incomplete SQLite table/column authority",
			operationIndex,
		)
	}
	key := targetSchemaEvolutionTableKey{table: objects[0].Table}
	beforeIndex := findTargetSchemaEvolutionTable(plan.states[operationIndex], key)
	afterIndex := findTargetSchemaEvolutionTable(plan.states[operationIndex+1], key)
	if beforeIndex < 0 || afterIndex < 0 {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d is missing its table from an exact catalog state",
			operationIndex,
		)
	}
	before := cloneStage4RichTable(plan.states[operationIndex][beforeIndex])
	after := cloneStage4RichTable(plan.states[operationIndex+1][afterIndex])
	if err := validateSQLiteTargetEvolutionCopySwapShape(
		before,
		after,
		operation.Action(),
		objects[0].Column,
	); err != nil {
		return schema.Table{}, schema.Table{}, "", err
	}
	if stage4SQLiteIdentifier(before.Name) == stage4SQLiteIdentifier(sqliteDeleteJournalTable) {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap refuses private DMTX delete receipt table %s", before.Name,
		)
	}
	temporary, err := sqliteTargetEvolutionCopySwapTemporaryName(plan, operationIndex)
	if err != nil {
		return schema.Table{}, schema.Table{}, "", err
	}
	return before, after, temporary, nil
}

func validateSQLiteTargetEvolutionCopySwapShape(
	before schema.Table,
	after schema.Table,
	action SchemaContractAction,
	column string,
) error {
	if before.Schema != "" || after.Schema != "" ||
		strings.TrimSpace(before.Name) == "" ||
		before.Name != after.Name || len(before.Columns) != len(after.Columns) {
		return fmt.Errorf("copy/swap changes table identity or column cardinality")
	}
	changed := -1
	for index := range before.Columns {
		if before.Columns[index].Name != after.Columns[index].Name {
			return fmt.Errorf("copy/swap changes column ordering or identity")
		}
		if before.Columns[index].Name == column {
			changed = index
		}
	}
	if changed < 0 {
		return fmt.Errorf("copy/swap changed column %s is absent", column)
	}
	if _, err := sqliteTargetEvolutionCopySwapPrimaryKey(before); err != nil {
		return err
	}
	if _, err := sqliteTargetEvolutionCopySwapPrimaryKey(after); err != nil {
		return err
	}
	normalizedBefore := cloneStage4RichTable(before)
	normalizedAfter := cloneStage4RichTable(after)
	if normalizedBefore.Identity != nil {
		normalizedBefore.Identity.Frontier = nil
	}
	if normalizedAfter.Identity != nil {
		normalizedAfter.Identity.Frontier = nil
	}
	switch action {
	case SchemaContractRelaxNullability:
		if normalizedBefore.Columns[changed].Nullable ||
			!normalizedAfter.Columns[changed].Nullable {
			return fmt.Errorf("copy/swap relax_nullability does not change NOT NULL to NULL")
		}
		normalizedAfter.Columns[changed].Nullable = normalizedBefore.Columns[changed].Nullable
	case SchemaContractWidenType:
		if normalizedBefore.Columns[changed].Type == normalizedAfter.Columns[changed].Type &&
			reflect.DeepEqual(
				normalizedBefore.Columns[changed].DeclaredType,
				normalizedAfter.Columns[changed].DeclaredType,
			) {
			return fmt.Errorf("copy/swap widen_type does not change the declared type")
		}
		normalizedAfter.Columns[changed].Type = normalizedBefore.Columns[changed].Type
		normalizedAfter.Columns[changed].DeclaredType = cloneStage4RichColumn(
			normalizedBefore.Columns[changed],
		).DeclaredType
	default:
		return fmt.Errorf("copy/swap action %s is unsupported", action)
	}
	if !reflect.DeepEqual(normalizedBefore, normalizedAfter) {
		return fmt.Errorf(
			"copy/swap %s changes objects beyond %s.%s",
			action,
			before.Name,
			column,
		)
	}
	return nil
}

func sqliteTargetEvolutionCopySwapPrimaryKey(table schema.Table) ([]schema.Column, error) {
	keys := make([]schema.Column, 0)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			continue
		}
		if column.PrimaryKeyPosition <= 0 || column.Nullable {
			return nil, fmt.Errorf(
				"copy/swap table %s has no complete non-null primary-key authority",
				table.Name,
			)
		}
		keys = append(keys, cloneStage4RichColumn(column))
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf(
			"copy/swap table %s has no complete primary-key authority", table.Name,
		)
	}
	sort.Slice(keys, func(left, right int) bool {
		return keys[left].PrimaryKeyPosition < keys[right].PrimaryKeyPosition
	})
	for index := range keys {
		if keys[index].PrimaryKeyPosition != index+1 {
			return nil, fmt.Errorf(
				"copy/swap table %s has non-contiguous primary-key authority",
				table.Name,
			)
		}
	}
	return keys, nil
}

func sqliteTargetEvolutionCopySwapTemporaryName(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
) (string, error) {
	if !plan.valid() || operationIndex < 0 || operationIndex >= len(plan.operations) {
		return "", fmt.Errorf("copy/swap temporary name has incomplete immutable plan authority")
	}
	reserved := make(map[string]struct{})
	for _, state := range plan.states {
		for _, table := range state {
			reserved[stage4SQLiteIdentifier(table.Name)] = struct{}{}
			for _, index := range table.Indexes {
				if index.Name != "" {
					reserved[stage4SQLiteIdentifier(index.Name)] = struct{}{}
				}
			}
		}
	}
	for _, reservation := range plan.reservations {
		if reservation.Namespace != sqliteTargetEvolutionNamespace ||
			(reservation.Scope != "relation" && reservation.Scope != "trigger") {
			continue
		}
		reserved[stage4SQLiteIdentifier(reservation.Name)] = struct{}{}
	}
	hash := sha256.Sum256([]byte(plan.Digest() + "\\x00" + strconv.Itoa(operationIndex)))
	prefix := "__dmtx_evolve_" + fmt.Sprintf("%x", hash[:12])
	for attempt := 0; attempt < 1024; attempt++ {
		candidate := prefix + "_" + strconv.Itoa(attempt)
		if _, exists := reserved[stage4SQLiteIdentifier(candidate)]; !exists {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("copy/swap temporary name collides with immutable SQLite catalog authority")
}

func validateSQLiteTargetEvolutionCopySwapObjects(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
	temporary string,
) error {
	if queryer == nil {
		return fmt.Errorf("copy/swap object authority queryer is not configured")
	}
	if stage4SQLiteIdentifier(table) == stage4SQLiteIdentifier(sqliteDeleteJournalTable) {
		return fmt.Errorf("copy/swap refuses private DMTX delete receipt table %s", table)
	}
	rows, err := queryer.QueryContext(ctx, `SELECT type, name, tbl_name FROM sqlite_schema ORDER BY type, name`)
	if err != nil {
		return fmt.Errorf("list SQLite copy/swap object authority: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind, name, owner string
		if err := rows.Scan(&kind, &name, &owner); err != nil {
			return fmt.Errorf("read SQLite copy/swap object authority: %w", err)
		}
		if stage4SQLiteIdentifier(name) == stage4SQLiteIdentifier(temporary) {
			return fmt.Errorf(
				"copy/swap deterministic temporary name %s collides with existing %s %s",
				temporary,
				kind,
				name,
			)
		}
		// DROP TABLE deliberately leaves views alone, so their exact SQL
		// reservations remain authenticated. SQLite drops triggers owned by
		// the table, however, and there is no sealed trigger renderer in this
		// bounded slice. Refuse before any DDL rather than execute catalog SQL.
		if kind == "trigger" && stage4SQLiteIdentifier(owner) == stage4SQLiteIdentifier(table) {
			return fmt.Errorf(
				"copy/swap table %s owns trigger %s which cannot be faithfully reconstructed",
				table,
				name,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite copy/swap object authority: %w", err)
	}
	return nil
}

// validateSQLiteTargetEvolutionNoIncomingForeignKeys rejects any table that
// is referenced by another SQLite table. DROP TABLE applies incoming ON
// DELETE actions (including CASCADE, SET NULL, and SET DEFAULT) immediately;
// PRAGMA defer_foreign_keys delays constraint checks only and cannot protect
// dependent rows. This read-only admission screen is deliberately repeated
// under the writer fence immediately before DDL.
func validateSQLiteTargetEvolutionNoIncomingForeignKeys(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
) error {
	if queryer == nil {
		return fmt.Errorf("copy/swap incoming foreign-key authority queryer is not configured")
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("copy/swap incoming foreign-key authority has an empty table identity")
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT name
		  FROM sqlite_schema
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		 ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list SQLite tables for incoming foreign-key authority: %w", err)
	}
	defer rows.Close()
	var children []string
	for rows.Next() {
		var child string
		if err := rows.Scan(&child); err != nil {
			return fmt.Errorf("read SQLite table for incoming foreign-key authority: %w", err)
		}
		if strings.TrimSpace(child) == "" {
			return fmt.Errorf("SQLite incoming foreign-key authority has an empty child table identity")
		}
		children = append(children, child)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite tables for incoming foreign-key authority: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close SQLite tables for incoming foreign-key authority: %w", err)
	}
	type incoming struct {
		child    string
		id       int
		sequence int
		onDelete string
		onUpdate string
	}
	var dependencies []incoming
	for _, child := range children {
		foreignKeys, err := queryer.QueryContext(
			ctx,
			"PRAGMA foreign_key_list("+quote(child)+")",
		)
		if err != nil {
			return fmt.Errorf("inspect SQLite foreign keys for table %s: %w", child, err)
		}
		for foreignKeys.Next() {
			var (
				id, sequence                          int
				referenced, local, onUpdate, onDelete string
				match                                 string
				referencedColumn                      sql.NullString
			)
			if err := foreignKeys.Scan(
				&id,
				&sequence,
				&referenced,
				&local,
				&referencedColumn,
				&onUpdate,
				&onDelete,
				&match,
			); err != nil {
				_ = foreignKeys.Close()
				return fmt.Errorf("read SQLite foreign keys for table %s: %w", child, err)
			}
			if id < 0 || sequence < 0 || strings.TrimSpace(referenced) == "" ||
				strings.TrimSpace(local) == "" || strings.TrimSpace(onUpdate) == "" ||
				strings.TrimSpace(onDelete) == "" || strings.TrimSpace(match) == "" {
				_ = foreignKeys.Close()
				return fmt.Errorf("SQLite foreign-key authority for table %s is incomplete", child)
			}
			if stage4SQLiteIdentifier(referenced) == stage4SQLiteIdentifier(table) {
				dependencies = append(dependencies, incoming{
					child:    child,
					id:       id,
					sequence: sequence,
					onDelete: onDelete,
					onUpdate: onUpdate,
				})
			}
		}
		if err := foreignKeys.Err(); err != nil {
			_ = foreignKeys.Close()
			return fmt.Errorf("iterate SQLite foreign keys for table %s: %w", child, err)
		}
		if err := foreignKeys.Close(); err != nil {
			return fmt.Errorf("close SQLite foreign keys for table %s: %w", child, err)
		}
	}
	if len(dependencies) == 0 {
		return nil
	}
	parts := make([]string, len(dependencies))
	for index, dependency := range dependencies {
		parts[index] = fmt.Sprintf(
			"%s(id=%d,seq=%d,on_delete=%s,on_update=%s)",
			dependency.child,
			dependency.id,
			dependency.sequence,
			dependency.onDelete,
			dependency.onUpdate,
		)
	}
	return fmt.Errorf(
		"copy/swap table %s has incoming foreign-key dependencies (%s); SQLite DROP TABLE can apply their ON DELETE actions",
		table,
		strings.Join(parts, ", "),
	)
}

func (session *sqliteTargetEvolutionMutationSession) executeSQLiteTargetEvolutionCopySwap(
	ctx context.Context,
	operationIndex int,
	operation TargetSchemaEvolutionOperation,
) error {
	before, after, temporary, err := sqliteTargetEvolutionCopySwapOperation(
		session.plan,
		operationIndex,
		operation,
	)
	if err != nil {
		return fmt.Errorf("validate SQLite copy/swap operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionCopySwapRendering(after); err != nil {
		return fmt.Errorf("revalidate SQLite copy/swap reconstruction for operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionCopySwapObjects(
		ctx,
		session.queryer,
		before.Name,
		temporary,
	); err != nil {
		return fmt.Errorf("revalidate SQLite copy/swap operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionPlannedIncomingForeignKeys(
		session.plan,
		operationIndex,
		before.Name,
	); err != nil {
		return fmt.Errorf(
			"revalidate SQLite copy/swap planned incoming foreign-key authority for operation %d: %w",
			operationIndex,
			err,
		)
	}
	if err := validateSQLiteTargetEvolutionNoIncomingForeignKeys(
		ctx,
		session.queryer,
		before.Name,
	); err != nil {
		return fmt.Errorf(
			"revalidate SQLite copy/swap incoming foreign-key authority for operation %d: %w",
			operationIndex,
			err,
		)
	}
	// Deferred foreign-key checks keep outbound references valid while the
	// sealed replacement is populated. They do not make incoming references
	// safe; validateSQLiteTargetEvolutionNoIncomingForeignKeys has already
	// rejected those before the first DROP TABLE.
	if _, err := session.executor.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
		return fmt.Errorf("defer SQLite foreign keys for copy/swap operation %d: %w", operationIndex, err)
	}
	retained, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
		ctx,
		session.queryer,
		before,
	)
	if err != nil {
		return fmt.Errorf("capture retained SQLite data authority before operation %d: %w", operationIndex, err)
	}
	priorSequence := sqliteTargetEvolutionSequenceAuthority{}
	if before.Identity != nil {
		priorSequence, err = sqliteTargetEvolutionReadSequence(
			ctx,
			session.queryer,
			before.Name,
			true,
		)
		if err != nil {
			return fmt.Errorf("authenticate SQLite sequence before copy/swap operation %d: %w", operationIndex, err)
		}
	}
	priorMaximum, priorMaximumKnown, err := sqliteTargetEvolutionPositiveMaximum(
		ctx,
		session.queryer,
		before,
	)
	if err != nil {
		return fmt.Errorf("read SQLite copied-row frontier before operation %d: %w", operationIndex, err)
	}
	temporaryTable := cloneStage4RichTable(after)
	temporaryTable.Name = temporary
	temporaryTable.Schema = ""
	create, err := schema.CreateTableDDL(schema.SQLite, temporaryTable)
	if err != nil {
		return fmt.Errorf("render SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	createSQL, err := schema.RenderDDLStatement(create, schema.SQLite)
	if err != nil {
		return fmt.Errorf("authenticate SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	copySQL, err := sqliteTargetEvolutionCopySwapCopyStatement(before, temporary)
	if err != nil {
		return fmt.Errorf("render SQLite copy/swap copy for %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, copySQL); err != nil {
		return fmt.Errorf("copy retained SQLite rows for %s: %w", before.Name, err)
	}
	if err := sqliteTargetEvolutionVerifyCopiedRows(ctx, session.queryer, before, temporary); err != nil {
		return fmt.Errorf("verify retained SQLite rows for %s: %w", before.Name, err)
	}
	copiedAuthority, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
		ctx,
		session.queryer,
		temporaryTable,
	)
	if err != nil {
		return fmt.Errorf("capture copied SQLite data authority for %s: %w", before.Name, err)
	}
	if !retained.sameData(copiedAuthority) {
		return fmt.Errorf("retained SQLite data authority changed during copy/swap")
	}
	copiedMaximum, copiedMaximumKnown, err := sqliteTargetEvolutionPositiveMaximum(
		ctx,
		session.queryer,
		temporaryTable,
	)
	if err != nil {
		return fmt.Errorf("read SQLite copied-row frontier after operation %d: %w", operationIndex, err)
	}
	if _, err := session.executor.ExecContext(ctx, "DROP TABLE "+quote(before.Name)); err != nil {
		return fmt.Errorf("drop replaced SQLite table %s: %w", before.Name, err)
	}
	// Do not rename the temporary table into place. SQLite validates views
	// during ALTER TABLE RENAME while the original relation is absent, and a
	// view that correctly references the final name would be rejected in that
	// transient interval. Recreate the original name from sealed after-state
	// metadata, copy from the verified temporary table, then remove it.
	finalCreate, err := schema.CreateTableDDL(schema.SQLite, after)
	if err != nil {
		return fmt.Errorf("render SQLite final copy/swap table %s: %w", before.Name, err)
	}
	finalCreateSQL, err := schema.RenderDDLStatement(finalCreate, schema.SQLite)
	if err != nil {
		return fmt.Errorf("authenticate SQLite final copy/swap table %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, finalCreateSQL); err != nil {
		return fmt.Errorf("restore SQLite copy/swap table name %s: %w", before.Name, err)
	}
	copyFinalSQL, err := sqliteTargetEvolutionCopySwapCopyStatement(temporaryTable, before.Name)
	if err != nil {
		return fmt.Errorf("render SQLite final copy/swap copy for %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, copyFinalSQL); err != nil {
		return fmt.Errorf("restore retained SQLite rows for %s: %w", before.Name, err)
	}
	if err := sqliteTargetEvolutionVerifyCopiedRows(ctx, session.queryer, temporaryTable, before.Name); err != nil {
		return fmt.Errorf("verify restored SQLite rows for %s: %w", before.Name, err)
	}
	finalAuthority, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(ctx, session.queryer, after)
	if err != nil {
		return fmt.Errorf("capture restored SQLite data authority for %s: %w", before.Name, err)
	}
	if !retained.sameData(finalAuthority) {
		return fmt.Errorf("retained SQLite data authority changed while restoring original table name")
	}
	if _, err := session.executor.ExecContext(ctx, "DROP TABLE "+quote(temporary)); err != nil {
		return fmt.Errorf("remove SQLite copy/swap temporary table %s: %w", temporary, err)
	}
	for _, index := range sqliteTargetEvolutionStandaloneIndexes(after.Indexes) {
		statement, renderErr := schema.SQLitePlannedIndexDDL(after, index)
		if renderErr != nil {
			return fmt.Errorf("render SQLite copy/swap index %s on %s: %w", index.Name, before.Name, renderErr)
		}
		statementSQL, renderErr := schema.RenderDDLStatement(statement, schema.SQLite)
		if renderErr != nil {
			return fmt.Errorf("authenticate SQLite copy/swap index %s on %s: %w", index.Name, before.Name, renderErr)
		}
		if _, err := session.executor.ExecContext(ctx, statementSQL); err != nil {
			return fmt.Errorf("restore SQLite copy/swap index %s on %s: %w", index.Name, before.Name, err)
		}
	}
	restoredSequence := sqliteTargetEvolutionSequenceAuthority{}
	if before.Identity != nil {
		restoredSequence, err = sqliteTargetEvolutionRestoreSequence(
			ctx,
			session.executor,
			session.queryer,
			before.Name,
			temporary,
			priorSequence,
			priorMaximum,
			priorMaximumKnown,
			copiedMaximum,
			copiedMaximumKnown,
		)
		if err != nil {
			return fmt.Errorf("restore SQLite sequence for copy/swap table %s: %w", before.Name, err)
		}
	}
	if err := sqliteTargetEvolutionAssertTemporaryObjectAbsent(
		ctx,
		session.queryer,
		temporary,
	); err != nil {
		return fmt.Errorf("verify SQLite copy/swap temporary object cleanup: %w", err)
	}
	retained.sequence = restoredSequence
	session.retained = append(session.retained, retained)
	return nil
}

func sqliteTargetEvolutionCopySwapCopyStatement(
	table schema.Table,
	temporary string,
) (string, error) {
	if strings.TrimSpace(temporary) == "" || len(table.Columns) == 0 {
		return "", fmt.Errorf("copy/swap table or temporary identity is empty")
	}
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		if strings.TrimSpace(column.Name) == "" {
			return "", fmt.Errorf("copy/swap table has an empty column identity")
		}
		columns[index] = quote(column.Name)
	}
	return "INSERT INTO " + quote(temporary) + " (" + strings.Join(columns, ", ") + ") SELECT " +
		strings.Join(columns, ", ") + " FROM " + quote(table.Name), nil
}

func sqliteTargetEvolutionVerifyCopiedRows(
	ctx context.Context,
	queryer sqliteQueryer,
	before schema.Table,
	temporary string,
) error {
	keys, err := sqliteTargetEvolutionCopySwapPrimaryKey(before)
	if err != nil {
		return err
	}
	var beforeCount, copiedCount int64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(before.Name),
	).Scan(&beforeCount); err != nil {
		return fmt.Errorf("count original rows: %w", err)
	}
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(temporary),
	).Scan(&copiedCount); err != nil {
		return fmt.Errorf("count copied rows: %w", err)
	}
	if beforeCount != copiedCount {
		return fmt.Errorf("row count changed from %d to %d", beforeCount, copiedCount)
	}
	join := make([]string, len(keys))
	for index, key := range keys {
		join[index] = `"source".` + quote(key.Name) + ` IS "copy".` + quote(key.Name)
	}
	comparisons := make([]string, len(before.Columns))
	for index, column := range before.Columns {
		left := `"source".` + quote(column.Name)
		right := `"copy".` + quote(column.Name)
		comparisons[index] = "(typeof(" + left + ") IS " +
			"typeof(" + right + ") AND quote(" + left + ") IS quote(" + right + "))"
	}
	query := "SELECT COUNT(*) FROM " + quote(before.Name) + ` AS "source" LEFT JOIN ` +
		quote(temporary) + ` AS "copy" ON ` + strings.Join(join, " AND ") +
		` WHERE "copy".` + quote(keys[0].Name) + " IS NULL OR NOT (" +
		strings.Join(comparisons, " AND ") + ")"
	var mismatches int64
	if err := queryer.QueryRowContext(ctx, query).Scan(&mismatches); err != nil {
		return fmt.Errorf("compare original and copied values: %w", err)
	}
	if mismatches != 0 {
		return fmt.Errorf("%d retained rows changed storage class or value", mismatches)
	}
	return nil
}

type sqliteTargetEvolutionSequenceAuthority struct {
	present bool
	value   int64
}

// sqliteTargetEvolutionRetainedDataAuthority is captured from the old table
// while BEGIN IMMEDIATE owns the writer fence. It is deliberately streaming:
// no retained target row is held in memory, but the ordered complete primary
// key/value sequence and row count remain independently verifiable after a
// COMMIT acknowledgement loss.
type sqliteTargetEvolutionRetainedDataAuthority struct {
	table    string
	columns  []string
	keys     []string
	rows     int64
	digest   [sha256.Size]byte
	sequence sqliteTargetEvolutionSequenceAuthority
}

func (authority sqliteTargetEvolutionRetainedDataAuthority) sameData(
	other sqliteTargetEvolutionRetainedDataAuthority,
) bool {
	return reflect.DeepEqual(authority.columns, other.columns) &&
		reflect.DeepEqual(authority.keys, other.keys) &&
		authority.rows == other.rows && authority.digest == other.digest
}

func sqliteTargetEvolutionCaptureRetainedDataAuthority(
	ctx context.Context,
	queryer sqliteQueryer,
	table schema.Table,
) (sqliteTargetEvolutionRetainedDataAuthority, error) {
	keys, err := sqliteTargetEvolutionCopySwapPrimaryKey(table)
	if err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		if strings.TrimSpace(column.Name) == "" {
			return sqliteTargetEvolutionRetainedDataAuthority{}, fmt.Errorf("table has an empty column identity")
		}
		columns[index] = column.Name
	}
	keyNames := make([]string, len(keys))
	for index, key := range keys {
		keyNames[index] = key.Name
	}
	queryColumns := make([]string, len(columns))
	for index, column := range columns {
		queryColumns[index] = quote(column)
	}
	queryKeys := make([]string, len(keyNames))
	for index, key := range keyNames {
		queryKeys[index] = quote(key)
	}
	rows, err := queryer.QueryContext(
		ctx,
		"SELECT "+strings.Join(queryColumns, ", ")+" FROM "+quote(table.Name)+
			" ORDER BY "+strings.Join(queryKeys, ", "),
	)
	if err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	defer rows.Close()
	digest := sha256.New()
	writeSQLiteTargetEvolutionDigestString(digest, "dmtx-sqlite-copy-swap-data-v1")
	for _, column := range columns {
		writeSQLiteTargetEvolutionDigestString(digest, column)
	}
	for _, key := range keyNames {
		writeSQLiteTargetEvolutionDigestString(digest, key)
	}
	values := make([]any, len(columns))
	scanned := make([]any, len(columns))
	for index := range scanned {
		scanned[index] = &values[index]
	}
	var count int64
	for rows.Next() {
		if err := rows.Scan(scanned...); err != nil {
			return sqliteTargetEvolutionRetainedDataAuthority{}, err
		}
		for _, value := range values {
			if err := writeSQLiteTargetEvolutionDigestValue(digest, value); err != nil {
				return sqliteTargetEvolutionRetainedDataAuthority{}, err
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	if err := rows.Close(); err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	writeSQLiteTargetEvolutionDigestInt64(digest, count)
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(nil))
	return sqliteTargetEvolutionRetainedDataAuthority{
		table:   table.Name,
		columns: columns,
		keys:    keyNames,
		rows:    count,
		digest:  sum,
	}, nil
}

func writeSQLiteTargetEvolutionDigestString(digest hash.Hash, value string) {
	writeSQLiteTargetEvolutionDigestBytes(digest, []byte(value))
}

func writeSQLiteTargetEvolutionDigestBytes(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func writeSQLiteTargetEvolutionDigestInt64(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = digest.Write(encoded[:])
}

func writeSQLiteTargetEvolutionDigestValue(digest hash.Hash, value any) error {
	switch typed := value.(type) {
	case nil:
		_, _ = digest.Write([]byte{0})
	case int64:
		_, _ = digest.Write([]byte{1})
		writeSQLiteTargetEvolutionDigestInt64(digest, typed)
	case float64:
		_, _ = digest.Write([]byte{2})
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], math.Float64bits(typed))
		_, _ = digest.Write(encoded[:])
	case bool:
		_, _ = digest.Write([]byte{3})
		if typed {
			_, _ = digest.Write([]byte{1})
		} else {
			_, _ = digest.Write([]byte{0})
		}
	case string:
		_, _ = digest.Write([]byte{4})
		writeSQLiteTargetEvolutionDigestString(digest, typed)
	case []byte:
		_, _ = digest.Write([]byte{5})
		writeSQLiteTargetEvolutionDigestBytes(digest, typed)
	case time.Time:
		_, _ = digest.Write([]byte{6})
		writeSQLiteTargetEvolutionDigestString(digest, typed.UTC().Format(time.RFC3339Nano))
	default:
		return fmt.Errorf("copy/swap retained data has unsupported SQLite scan type %T", value)
	}
	return nil
}

func sqliteTargetEvolutionReadSequence(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
	required bool,
) (sqliteTargetEvolutionSequenceAuthority, error) {
	exists, err := sqliteTargetEvolutionSequenceTableExists(ctx, queryer)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, err
	}
	if !exists {
		if required {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf(
				"sqlite_sequence is absent for AUTOINCREMENT table %s", table,
			)
		}
		return sqliteTargetEvolutionSequenceAuthority{}, nil
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT name, seq FROM sqlite_sequence WHERE name = ? COLLATE NOCASE`,
		table,
	)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("read sqlite_sequence: %w", err)
	}
	defer rows.Close()
	var result sqliteTargetEvolutionSequenceAuthority
	for rows.Next() {
		var name string
		var sequence int64
		if err := rows.Scan(&name, &sequence); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("scan sqlite_sequence: %w", err)
		}
		if stage4SQLiteIdentifier(name) != stage4SQLiteIdentifier(table) || sequence < 0 || result.present {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf(
				"sqlite_sequence authority for table %s is ambiguous or invalid", table,
			)
		}
		result.present = true
		result.value = sequence
	}
	if err := rows.Err(); err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("iterate sqlite_sequence: %w", err)
	}
	return result, nil
}

func sqliteTargetEvolutionSequenceTableExists(
	ctx context.Context,
	queryer sqliteQueryer,
) (bool, error) {
	var name string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'`,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("authenticate sqlite_sequence catalog object: %w", err)
	}
	if name != "sqlite_sequence" {
		return false, fmt.Errorf("sqlite_sequence catalog object has unexpected identity %q", name)
	}
	return true, nil
}

func sqliteTargetEvolutionPositiveMaximum(
	ctx context.Context,
	queryer sqliteQueryer,
	table schema.Table,
) (int64, bool, error) {
	if table.Identity == nil || strings.TrimSpace(table.Identity.Column) == "" {
		return 0, false, nil
	}
	var maximum sql.NullInt64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT MAX("+quote(table.Identity.Column)+") FROM "+quote(table.Name)+
			" WHERE "+quote(table.Identity.Column)+" > 0",
	).Scan(&maximum); err != nil {
		return 0, false, err
	}
	if !maximum.Valid {
		return 0, false, nil
	}
	return maximum.Int64, true, nil
}

func sqliteTargetEvolutionRestoreSequence(
	ctx context.Context,
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	queryer sqliteQueryer,
	table string,
	temporary string,
	prior sqliteTargetEvolutionSequenceAuthority,
	priorMaximum int64,
	priorMaximumKnown bool,
	copiedMaximum int64,
	copiedMaximumKnown bool,
) (sqliteTargetEvolutionSequenceAuthority, error) {
	frontier := sqliteTargetEvolutionSequenceAuthority{}
	for _, candidate := range []sqliteTargetEvolutionSequenceAuthority{prior} {
		if candidate.present && (!frontier.present || candidate.value > frontier.value) {
			frontier = candidate
		}
	}
	for _, candidate := range []struct {
		value int64
		known bool
	}{
		{value: priorMaximum, known: priorMaximumKnown},
		{value: copiedMaximum, known: copiedMaximumKnown},
	} {
		if candidate.known && (!frontier.present || candidate.value > frontier.value) {
			frontier = sqliteTargetEvolutionSequenceAuthority{present: true, value: candidate.value}
		}
	}
	for _, name := range []string{table, temporary} {
		if _, err := executor.ExecContext(
			ctx,
			`DELETE FROM sqlite_sequence WHERE name = ? COLLATE NOCASE`,
			name,
		); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("clear sqlite_sequence entry %s: %w", name, err)
		}
	}
	if frontier.present {
		if _, err := executor.ExecContext(
			ctx,
			`INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)`,
			table,
			frontier.value,
		); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("restore sqlite_sequence frontier %d: %w", frontier.value, err)
		}
	}
	restored, err := sqliteTargetEvolutionReadSequence(ctx, queryer, table, true)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("authenticate restored sqlite_sequence: %w", err)
	}
	if restored != frontier {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("restored sqlite_sequence frontier differs from authenticated safe frontier")
	}
	temporaryEntry, err := sqliteTargetEvolutionReadSequence(ctx, queryer, temporary, true)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("authenticate temporary sqlite_sequence cleanup: %w", err)
	}
	if temporaryEntry.present {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("sqlite_sequence retained an orphan temporary-name entry")
	}
	return frontier, nil
}

func sqliteTargetEvolutionAssertTemporaryObjectAbsent(
	ctx context.Context,
	queryer sqliteQueryer,
	temporary string,
) error {
	rows, err := queryer.QueryContext(ctx, `SELECT type, name FROM sqlite_schema ORDER BY type, name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			return err
		}
		if stage4SQLiteIdentifier(name) == stage4SQLiteIdentifier(temporary) {
			return fmt.Errorf("temporary %s object %s remains", kind, name)
		}
	}
	return rows.Err()
}

func verifySQLiteTargetEvolutionCopySwapIntegrity(
	ctx context.Context,
	queryer sqliteQueryer,
) error {
	if err := preflightSQLiteForeignKeyIntegrity(ctx, queryer, ""); err != nil {
		return fmt.Errorf("verify SQLite copy/swap foreign-key integrity: %w", err)
	}
	rows, err := queryer.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("run SQLite copy/swap quick_check: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate SQLite copy/swap quick_check: %w", err)
		}
		return fmt.Errorf("SQLite copy/swap quick_check returned no authority")
	}
	var result string
	if err := rows.Scan(&result); err != nil {
		return fmt.Errorf("read SQLite copy/swap quick_check: %w", err)
	}
	if result != "ok" || rows.Next() {
		return fmt.Errorf("SQLite copy/swap quick_check is not exact ok authority")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite copy/swap quick_check: %w", err)
	}
	return nil
}

func readSQLiteTargetEvolutionCatalog(
	ctx context.Context,
	queryer sqliteQueryer,
) (TargetSchemaEvolutionCatalog, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT type, name, tbl_name, sql
		  FROM sqlite_schema
		 WHERE name NOT LIKE 'sqlite_%'
		 ORDER BY type, name`)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"list complete SQLite target schema catalog: %w", err,
		)
	}
	defer rows.Close()
	tableNames := make([]string, 0)
	reservations := make([]TargetSchemaEvolutionNameReservation, 0)
	for rows.Next() {
		var kind, name, tableName string
		var statement sql.NullString
		if err := rows.Scan(&kind, &name, &tableName, &statement); err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"read SQLite target schema catalog: %w", err,
			)
		}
		switch kind {
		case "table":
			tableNames = append(tableNames, name)
		case "index":
			// Every supported SQLite index is represented by its owning table's
			// exact inspector shape. An index with no table authority cannot be
			// preserved or checked as a deterministic evolution state.
			if tableName == "" {
				return TargetSchemaEvolutionCatalog{}, targetSchemaEvolutionError(
					TargetSchemaEvolutionReadFailed,
					"catalog",
					"SQLite index "+name+" has no owning table",
					nil,
				)
			}
		case "view":
			var reservationErr error
			reservations, reservationErr = appendSQLiteTargetEvolutionObjectReservations(
				reservations, "relation", "view", name, statement,
			)
			if reservationErr != nil {
				return TargetSchemaEvolutionCatalog{}, reservationErr
			}
		case "trigger":
			var reservationErr error
			reservations, reservationErr = appendSQLiteTargetEvolutionObjectReservations(
				reservations, "trigger", "trigger", name, statement,
			)
			if reservationErr != nil {
				return TargetSchemaEvolutionCatalog{}, reservationErr
			}
		default:
			return TargetSchemaEvolutionCatalog{}, targetSchemaEvolutionError(
				TargetSchemaEvolutionReadFailed,
				"catalog",
				"SQLite sqlite_schema contains unsupported object type "+kind,
				nil,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"iterate SQLite target schema catalog: %w", err,
		)
	}
	if err := rows.Close(); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"close SQLite target schema catalog: %w", err,
		)
	}
	sort.Strings(tableNames)
	tables := make([]schema.Table, 0, len(tableNames))
	for _, name := range tableNames {
		table, _, err := inspectSQLiteSchema(ctx, queryer, name)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"inspect complete SQLite target table %s: %w", name, err,
			)
		}
		// SQLite materializes the portable identity contract as INTEGER PRIMARY
		// KEY AUTOINCREMENT. Its physical declared type is necessarily INTEGER,
		// but its value domain is the signed 64-bit domain represented by the
		// portable bigint identity contract. The complete evolution catalog is
		// semantic authority consumed alongside projected target shapes, so
		// normalize only this independently authenticated identity column back
		// to that portable representation. Ordinary retained-shape preflight
		// continues to compare the physical INTEGER AUTOINCREMENT form.
		table, err = canonicalizeSQLiteTargetEvolutionIdentity(table)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, err
		}
		tables = append(tables, table)
	}
	sortTargetSchemaEvolutionTables(tables)
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"validate complete SQLite target catalog: %w", err,
		)
	}
	return catalog, nil
}

func canonicalizeSQLiteTargetEvolutionIdentity(
	table schema.Table,
) (schema.Table, error) {
	if table.Identity == nil {
		return table, nil
	}
	identity := table.Identity.Column
	for index := range table.Columns {
		column := &table.Columns[index]
		if column.Name != identity {
			continue
		}
		if !column.PrimaryKey || column.PrimaryKeyPosition != 1 ||
			column.DeclaredType == nil ||
			!strings.EqualFold(
				strings.TrimSpace(column.DeclaredType.Base), "integer",
			) || len(column.DeclaredType.Arguments) != 0 {
			return schema.Table{}, targetSchemaEvolutionError(
				TargetSchemaEvolutionReadFailed,
				"catalog",
				"SQLite identity "+table.Name+"."+identity+
					" is not the exact INTEGER PRIMARY KEY AUTOINCREMENT shape",
				nil,
			)
		}
		column.Type = "bigint"
		column.Nullable = false
		column.DeclaredType = &schema.DeclaredType{Base: "bigint"}
		return table, nil
	}
	return schema.Table{}, targetSchemaEvolutionError(
		TargetSchemaEvolutionReadFailed,
		"catalog",
		"SQLite identity metadata references missing column "+
			table.Name+"."+identity,
		nil,
	)
}

func appendSQLiteTargetEvolutionObjectReservations(
	reservations []TargetSchemaEvolutionNameReservation,
	collisionScope string,
	definitionScope string,
	name string,
	statement sql.NullString,
) ([]TargetSchemaEvolutionNameReservation, error) {
	// Both the user-visible name and a stable statement fingerprint are needed:
	// the former protects new relation allocation and the latter makes a
	// same-name trigger/view rewrite catalog drift rather than trusted state.
	if !statement.Valid || strings.TrimSpace(statement.String) == "" {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"catalog",
			"persistent SQLite "+definitionScope+" "+name+" has no exact sqlite_schema SQL authority",
			nil,
		)
	}
	reservations = append(reservations, TargetSchemaEvolutionNameReservation{
		Scope: collisionScope, Namespace: sqliteTargetEvolutionNamespace, Name: name,
	})
	hash := sha256.Sum256([]byte(strings.TrimSpace(statement.String)))
	return append(reservations, TargetSchemaEvolutionNameReservation{
		Scope:     definitionScope + "_definition",
		Namespace: sqliteTargetEvolutionNamespace,
		Name:      name + "@" + fmt.Sprintf("%x", hash[:]),
	}), nil
}

type sqliteTargetSchemaEvolutionCreatePlanner struct{}

func (sqliteTargetSchemaEvolutionCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesiredTables []schema.Table,
	actualCatalog TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.SQLite {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQLite target create planner cannot render %q", target,
		)
	}
	created := cloneTargetSchemaEvolutionTables(tables)
	sortTargetSchemaEvolutionTables(created)
	if len(created) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQLite target create planner has no tables",
		)
	}
	desired := cloneTargetSchemaEvolutionTables(completeDesiredTables)
	sortTargetSchemaEvolutionTables(desired)
	if err := validateSQLiteTargetEvolutionCreateNames(
		created, desired, actualCatalog,
	); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}

	state := make([]schema.Table, 0, len(created))
	steps := make([]TargetSchemaCreateStep, 0, len(created)*2)
	for _, table := range created {
		statement, err := schema.CreateTableDDL(schema.SQLite, table)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"plan complete SQLite target table %s: %w", table.Name, err,
			)
		}
		base := cloneStage4RichTable(table)
		base.Indexes = sqliteTargetEvolutionInlineIndexes(table.Indexes)
		state = append(state, base)
		sortTargetSchemaEvolutionTables(state)
		steps = append(steps, TargetSchemaCreateStep{
			Statement: statement, ResultTables: cloneTargetSchemaEvolutionTables(state),
		})
	}
	for _, table := range created {
		indexes := sqliteTargetEvolutionStandaloneIndexes(table.Indexes)
		for _, index := range indexes {
			statement, err := schema.SQLitePlannedIndexDDL(table, index)
			if err != nil {
				return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
					"seal SQLite target index %s on %s: %w", index.Name, table.Name, err,
				)
			}
			stateIndex := findTargetSchemaEvolutionTable(state, targetSchemaEvolutionTableKey{table: table.Name})
			if stateIndex < 0 {
				return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
					"SQLite target create state lost table %s", table.Name,
				)
			}
			state[stateIndex].Indexes = append(state[stateIndex].Indexes, index)
			steps = append(steps, TargetSchemaCreateStep{
				Statement: statement, ResultTables: cloneTargetSchemaEvolutionTables(state),
			})
		}
	}
	return NewCompleteTargetSchemaCreateBundle(schema.SQLite, created, steps)
}

func sqliteTargetEvolutionInlineIndexes(indexes []schema.Index) []schema.Index {
	result := make([]schema.Index, 0, len(indexes))
	for _, index := range indexes {
		if index.Inline {
			result = append(result, index)
		}
	}
	return result
}

func sqliteTargetEvolutionStandaloneIndexes(indexes []schema.Index) []schema.Index {
	result := make([]schema.Index, 0, len(indexes))
	for _, index := range indexes {
		if !index.Inline {
			result = append(result, index)
		}
	}
	sort.SliceStable(result, func(left, right int) bool {
		return sqliteIndexSortKey(result[left]) < sqliteIndexSortKey(result[right])
	})
	return result
}

func validateSQLiteTargetEvolutionCreateNames(
	created []schema.Table,
	desired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) error {
	createdNames := make(map[string]string, len(created))
	type sqliteExistingObject struct {
		kind  string
		owner string
	}
	existingObjects := make(map[string]sqliteExistingObject)
	addExisting := func(name, kind, owner string) error {
		key := stage4SQLiteIdentifier(name)
		if earlier, exists := existingObjects[key]; exists {
			return fmt.Errorf(
				"SQLite target catalog has case-insensitive object collision between %s and %s",
				earlier.kind, kind,
			)
		}
		existingObjects[key] = sqliteExistingObject{kind: kind, owner: owner}
		return nil
	}
	for _, table := range actual.Tables() {
		if err := addExisting(table.Name, "table "+table.Name, table.Name); err != nil {
			return err
		}
		for _, index := range table.Indexes {
			if index.Inline || index.Name == "" {
				continue
			}
			if err := addExisting(index.Name, "index "+index.Name, table.Name); err != nil {
				return err
			}
		}
	}
	for _, reservation := range actual.Reservations() {
		if reservation.Namespace != sqliteTargetEvolutionNamespace {
			return fmt.Errorf("SQLite target catalog has reservation outside main")
		}
		if reservation.Scope != "relation" && reservation.Scope != "trigger" {
			continue
		}
		if err := addExisting(reservation.Name, reservation.Scope+" "+reservation.Name, ""); err != nil {
			return err
		}
	}
	addCreated := func(name, kind, owner string) error {
		key := stage4SQLiteIdentifier(name)
		if existing, exists := existingObjects[key]; exists {
			// A complete table or index may already be present only as an
			// authenticated immutable prefix for the same created table. The
			// generic state machine compares that full shape before permitting a
			// resume; this narrow exception merely lets its planner rebuild.
			if stage4SQLiteIdentifier(existing.owner) != stage4SQLiteIdentifier(owner) ||
				!strings.HasPrefix(existing.kind, strings.Fields(kind)[0]+" ") {
				return fmt.Errorf("SQLite target create %s collides with existing %s", kind, existing.kind)
			}
		}
		if earlier, exists := createdNames[key]; exists {
			return fmt.Errorf("SQLite target create %s collides with planned %s", kind, earlier)
		}
		createdNames[key] = kind
		return nil
	}
	for _, table := range created {
		if table.Schema != "" || strings.TrimSpace(table.Name) == "" ||
			strings.HasPrefix(strings.ToLower(table.Name), "sqlite_") {
			return fmt.Errorf(
				"SQLite target create table has an unsupported namespace or reserved name %q", table.Name,
			)
		}
		if err := addCreated(table.Name, "table "+table.Name, table.Name); err != nil {
			return err
		}
		for _, index := range table.Indexes {
			if index.Inline && index.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named inline constraint %s that cannot be recovered from the exact catalog",
					table.Name, index.Name,
				)
			}
			if !index.Inline && strings.TrimSpace(index.Name) == "" {
				return fmt.Errorf(
					"SQLite target create table %s has an unnamed standalone index", table.Name,
				)
			}
			if !index.Inline {
				if err := addCreated(index.Name, "index "+index.Name, table.Name); err != nil {
					return err
				}
			}
		}
		for _, check := range table.Checks {
			if check.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named CHECK constraint %s that cannot be recovered from the exact catalog",
					table.Name, check.Name,
				)
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if foreignKey.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named foreign key %s that cannot be recovered from the exact catalog",
					table.Name, foreignKey.Name,
				)
			}
		}
	}
	_ = desired // desired coverage is authenticated by CompleteTargetSchemaCreateBundle.
	return nil
}

func rollbackSQLiteTargetEvolutionTransaction(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := sqliteTargetEvolutionDetachedContext(ctx)
	defer cancel()
	_, err := connection.ExecContext(cleanupCtx, "ROLLBACK")
	return err
}

func sqliteTargetEvolutionDetachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(ctx),
		targetSchemaEvolutionVerificationTimeout,
	)
}

func discardSQLiteTargetEvolutionConnection(connection *sql.Conn) {
	if connection == nil {
		return
	}
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
}

func joinSQLiteTargetEvolutionCleanupError(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	return errors.Join(primary, targetSchemaEvolutionError(
		TargetSchemaEvolutionVerifyFailed,
		"DDL fence cleanup",
		targetSchemaEvolutionRecoveryWording(
			"SQLite target schema evolution could not release its pinned mutation connection",
		),
		cleanup,
	))
}

func verifySQLiteTargetEvolutionCommittedCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but an independent complete catalog snapshot could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but the independent complete catalog snapshot is structurally invalid",
			),
			err,
		)
	}
	if _, err := matchTargetSchemaEvolutionState(
		[][]schema.Table{plan.states[len(plan.states)-1]}, plan.reservations, actual,
	); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but an independent snapshot found concurrent or unexpected catalog drift",
			),
			err,
		)
	}
	return nil
}

// verifySQLiteTargetEvolutionCommittedAuthority supplements exact catalog
// comparison with the retained-row authority captured under the writer fence.
// A COMMIT/no-ack path is not success merely because the DDL shape matches:
// every copied table must still have the same ordered typed key/value sequence
// and the exact safe AUTOINCREMENT frontier.
func (adapter *sqliteTargetAdapter) verifySQLiteTargetEvolutionCommittedAuthority(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	retained []sqliteTargetEvolutionRetainedDataAuthority,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if err := verifySQLiteTargetEvolutionCommittedCatalog(plan, actual, readErr); err != nil {
		return err
	}
	if len(retained) == 0 {
		return nil
	}
	tables := actual.Tables()
	for _, expected := range retained {
		index := findTargetSchemaEvolutionTable(
			tables,
			targetSchemaEvolutionTableKey{table: expected.table},
		)
		if index < 0 {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but a retained-data table is absent from exact catalog authority",
				),
				nil,
			)
		}
		observed, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
			ctx,
			adapter.database,
			tables[index],
		)
		if err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but retained data could not be independently read",
				),
				err,
			)
		}
		if tables[index].Identity != nil {
			observed.sequence, err = sqliteTargetEvolutionReadSequence(
				ctx,
				adapter.database,
				tables[index].Name,
				true,
			)
			if err != nil {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionVerifyFailed,
					"post-commit retained data verification",
					targetSchemaEvolutionRecoveryWording(
						"SQLite evolution committed but AUTOINCREMENT authority could not be independently read",
					),
					err,
				)
			}
		}
		if !expected.sameData(observed) || expected.sequence != observed.sequence {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but retained rows or AUTOINCREMENT frontier differ from writer-fenced authority",
				),
				nil,
			)
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) classifySQLiteTargetEvolutionCommitAmbiguity(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	retained []sqliteTargetEvolutionRetainedDataAuthority,
	commitErr error,
) error {
	actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err := classifySQLiteTargetEvolutionCommitCatalog(
		plan,
		actual,
		readErr,
		commitErr,
	); err != nil {
		return err
	}
	return adapter.verifySQLiteTargetEvolutionCommittedAuthority(
		ctx,
		plan,
		retained,
		actual,
		readErr,
	)
}

func classifySQLiteTargetEvolutionCommitCatalog(
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
				"SQLite commit returned an error and an independent complete catalog snapshot could not be read",
			),
			errors.Join(commitErr, readErr),
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQLite commit returned an error and the independent catalog is structurally invalid",
			),
			errors.Join(commitErr, err),
		)
	}
	prefix, err := matchTargetSchemaEvolutionState(plan.states, plan.reservations, actual)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQLite commit returned an error and the independent catalog has unexpected drift",
			),
			errors.Join(commitErr, err),
		)
	}
	if prefix == len(plan.operations) {
		return nil
	}
	return targetSchemaEvolutionError(
		TargetSchemaEvolutionApplyFailed,
		"commit",
		targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
			"SQLite commit returned an error after exact verified prefix %d of %d",
			prefix, len(plan.operations),
		)),
		commitErr,
	)
}
