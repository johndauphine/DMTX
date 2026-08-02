package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
