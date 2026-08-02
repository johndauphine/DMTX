package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const stage4SQLServerStrictWorkVersion = 1

// stage4SQLServerStrictParallelSource is a source view whose every parallel
// reader is admitted by one SQL Server table-lock owner. It deliberately owns
// no ordinary source adapter: secondary pages must be opened through the
// session callback below, which acquires a reader lock and verifies owner
// liveness before exposing a queryer.
type stage4SQLServerStrictParallelSource struct {
	adapterStableNetworkSource

	readerSlots   chan struct{}
	primaryReader chan struct{}
	openSecondary func(
		context.Context,
		func(adapterStableNetworkSource) error,
	) error
}

func newStage4SQLServerStrictParallelSource(
	primary adapterStableNetworkSource,
	readerLimit int,
	openSecondary func(
		context.Context,
		func(adapterStableNetworkSource) error,
	) error,
) (*stage4SQLServerStrictParallelSource, error) {
	if isNilInterface(primary) {
		return nil, errors.New(
			"SQL Server strict parallel source requires a lock-bound primary reader",
		)
	}
	if readerLimit < 1 {
		return nil, errors.New(
			"SQL Server strict parallel source reader limit must be positive",
		)
	}
	if readerLimit > 1 && isNilInterface(openSecondary) {
		return nil, errors.New(
			"SQL Server strict parallel source requires a lock-bound secondary reader opener",
		)
	}
	result := &stage4SQLServerStrictParallelSource{
		adapterStableNetworkSource: primary,
		readerSlots:                make(chan struct{}, readerLimit),
		primaryReader:              make(chan struct{}, 1),
		openSecondary:              openSecondary,
	}
	result.primaryReader <- struct{}{}
	return result, nil
}

func (source *stage4SQLServerStrictParallelSource) ReadNetworkRangePage(
	ctx context.Context,
	table schema.Table,
	columns []string,
	pagination PaginationPlan,
	rangePlan PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	if source == nil || isNilInterface(source.adapterStableNetworkSource) {
		return NetworkReadPage{}, errors.New(
			"SQL Server strict parallel source is unavailable",
		)
	}
	if ctx == nil {
		return NetworkReadPage{}, errors.New(
			"SQL Server strict parallel source context is required",
		)
	}
	select {
	case <-ctx.Done():
		return NetworkReadPage{}, ctx.Err()
	case source.readerSlots <- struct{}{}:
	}
	defer func() { <-source.readerSlots }()

	select {
	case <-source.primaryReader:
		defer func() { source.primaryReader <- struct{}{} }()
		return source.adapterStableNetworkSource.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			pagination,
			rangePlan,
			request,
		)
	default:
	}
	if isNilInterface(source.openSecondary) {
		return NetworkReadPage{}, errors.New(
			"SQL Server strict parallel source has no lock-bound secondary reader",
		)
	}
	var page NetworkReadPage
	err := source.openSecondary(ctx, func(secondary adapterStableNetworkSource) error {
		if isNilInterface(secondary) {
			return errors.New(
				"SQL Server strict secondary reader is unavailable",
			)
		}
		var readErr error
		page, readErr = secondary.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			pagination,
			rangePlan,
			request,
		)
		return readErr
	})
	return page, err
}

func (source *stage4SQLServerStrictParallelSource) Stage4ValidationProbe(
	routeSource sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if source == nil || isNilInterface(source.adapterStableNetworkSource) {
		return nil, errors.New(
			"SQL Server strict parallel validation source is unavailable",
		)
	}
	provider, ok := source.adapterStableNetworkSource.(adapterStage4ValidationProbeProvider)
	if !ok || isNilInterface(provider) {
		return nil, errors.New(
			"SQL Server strict primary reader lacks a lock-bound validation seam",
		)
	}
	return provider.Stage4ValidationProbe(routeSource, target, plans)
}

func inheritStage4SQLServerStrictLockProofs(
	primary adapterStableNetworkSource,
	secondary adapterStableNetworkSource,
) error {
	primaryView, ok := primary.(*adapterRetainedStableRelationalView)
	if !ok || primaryView == nil {
		return errors.New(
			"SQL Server strict primary reader has an unexpected stable-view type",
		)
	}
	secondaryView, ok := secondary.(*adapterRetainedStableRelationalView)
	if !ok || secondaryView == nil {
		return errors.New(
			"SQL Server strict secondary reader has an unexpected stable-view type",
		)
	}
	primaryView.mu.Lock()
	if primaryView.source == nil || primaryView.tableScope == nil ||
		primaryView.tableCatalog == nil {
		primaryView.mu.Unlock()
		return errors.New(
			"SQL Server strict primary reader lacks table-lock authority",
		)
	}
	routeSource := primaryView.source
	scope := *primaryView.tableScope
	catalog := cloneStage4RichTable(*primaryView.tableCatalog)
	retained := make(map[string]int64, len(primaryView.retainedRowBounds))
	for identity, bound := range primaryView.retainedRowBounds {
		retained[identity] = bound
	}
	pagination := make(map[string]PaginationPlan, len(primaryView.paginationPlans))
	for identity, plan := range primaryView.paginationPlans {
		pagination[identity] = clonePaginationPlan(plan)
	}
	primaryView.mu.Unlock()
	if len(retained) == 0 || len(pagination) == 0 {
		return errors.New(
			"SQL Server strict primary reader lacks exact stable planning proofs",
		)
	}

	secondaryView.mu.Lock()
	defer secondaryView.mu.Unlock()
	if secondaryView.source != routeSource || secondaryView.tableScope == nil ||
		*secondaryView.tableScope != scope || secondaryView.tableCatalog == nil {
		return errors.New(
			"SQL Server strict secondary reader differs from the lock-bound table authority",
		)
	}
	sameCatalog, err := stage4AdapterNetworkCatalogEqual(
		*secondaryView.tableCatalog,
		catalog,
	)
	if err != nil {
		return err
	}
	if !sameCatalog {
		return errors.New(
			"SQL Server strict secondary reader catalog differs from the lock-bound primary view",
		)
	}
	secondaryView.retainedRowBounds = retained
	secondaryView.paginationPlans = pagination
	return nil
}

func requireStage4StrictRoute(
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	mode string,
) error {
	if !cfg.Migration.StrictConsistency {
		return nil
	}
	if isNilInterface(source) {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("Stage 4 strict consistency requires a source adapter"),
		)
	}
	switch source.Engine() {
	case "postgres":
		return requireStage4PostgresStrictRoute(cfg, source, target, mode)
	case "mssql":
		return requireStage4SQLServerStrictRoute(cfg, source, target, mode)
	case "mysql":
		return requireStage4MySQLStrictRoute(cfg, source, target, mode)
	case "sqlite":
		return requireStage4SQLiteStrictRoute(cfg, source, target, mode)
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency has no certified source composition for engine %q",
				source.Engine(),
			),
		)
	}
}

func requireStage4SQLServerStrictRoute(
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	mode string,
) error {
	if !cfg.Migration.StrictConsistency {
		return nil
	}
	if mode != "upsert" || isNilInterface(source) || isNilInterface(target) ||
		source.Engine() != "mssql" ||
		!stage4AdapterNetworkRelationalEngine(target.Engine()) {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"Stage 4 SQL Server strict consistency requires an upsert route to a certified relational or SQLite target",
			),
		)
	}
	scope, err := stage4SQLServerStrictScope(
		cfg.Migration.StrictConsistencyScope,
	)
	if err != nil {
		return err
	}
	if scope != state.StrictSnapshotTable &&
		scope != state.StrictSnapshotMigration {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("Stage 4 SQL Server strict consistency scope is unsupported"),
		)
	}
	return nil
}

func migrateWithStage4SQLServerStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (Result, error) {
	if err := requireStage4SQLServerStrictRoute(cfg, source, target, prepared.mode); err != nil {
		return Result{}, err
	}
	if networkExecution == nil || !networkExecution.deferred {
		return Result{}, NewTransferError(
			ErrorClassState,
			errors.New(
				"SQL Server strict composition requires deferred network work",
			),
		)
	}
	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational == nil || relational.database == nil ||
		relational.spec.engine != "mssql" {
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"SQL Server strict composition requires the production SQL Server source adapter",
			),
		)
	}
	if prepared.deletes != nil {
		return migrateWithStage4SQLServerStrictDeleteAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			networkExecution,
			resume,
			completed,
		)
	}
	scope, err := stage4SQLServerStrictScope(
		cfg.Migration.StrictConsistencyScope,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	var cleanupBackend state.StrictMigrationCleanupBackend
	if scope == state.StrictSnapshotMigration {
		var supported bool
		cleanupBackend, supported = prepared.run.Backend.(state.StrictMigrationCleanupBackend)
		if !supported || isNilInterface(cleanupBackend) {
			return resultForValidatedAdapterCheckpoints(completed), NewTransferError(
				ErrorClassPolicy,
				errors.New("SQL Server migration-scope strict consistency requires durable cleanup-intent state support"),
			)
		}
	}
	completedBinding, err := validateCompletedStage4SQLServerStrictEvidence(
		ctx,
		prepared,
		scope,
		completed,
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if len(completedBinding.tables) != 0 {
		networkExecution.finalizeWork = completedBinding.finalizeWork
	}
	if err := networkExecution.prevalidateCompletedTables(ctx, completed); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	selectedTasks := make([]state.TaskKey, 0, len(prepared.work))
	for index, plan := range prepared.plans {
		if _, complete := completed[plan.source.Name]; !complete {
			selectedTasks = append(selectedTasks, prepared.work[index].task)
		}
	}
	if len(selectedTasks) == 0 {
		result := resultForValidatedAdapterCheckpoints(completed)
		if scope == state.StrictSnapshotMigration {
			if !resume {
				return result, NewTransferError(
					ErrorClassState,
					errors.New("completed SQL Server migration-strict work requires explicit resume for snapshot cleanup"),
				)
			}
			owner, found, err := prepared.run.Backend.LoadLatestStrictMigrationSnapshot(
				prepared.run.RunID,
			)
			if err != nil {
				return result, NewTransferError(
					ErrorClassState,
					fmt.Errorf("load completed SQL Server migration snapshot cleanup owner: %w", err),
				)
			}
			if !found {
				return result, NewTransferError(
					ErrorClassState,
					errors.New("completed SQL Server migration-strict work lacks a durable database snapshot cleanup owner"),
				)
			}
			opener, err := NewSQLServerMigrationSnapshotOpener(
				relational.database,
				relational.endpoint,
				relational.namespace,
			)
			if err != nil {
				return result, err
			}
			if err := cleanupCompletedStage4SQLServerMigrationSnapshot(
				ctx,
				prepared,
				owner,
				opener,
				cleanupBackend,
			); err != nil {
				var classified transferErrorClassifier
				if errors.As(err, &classified) {
					return result, fmt.Errorf(
						"verified SQL Server migration snapshot cleanup before completion: %w",
						err,
					)
				}
				return result, NewTransferError(
					ErrorClassPermanent,
					fmt.Errorf("verified SQL Server migration snapshot cleanup before completion: %w", err),
				)
			}
		} else if err := completeStage4SchemaGateSentinels(
			ctx,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		); err != nil {
			return result, err
		}
		result.Validated = true
		return result, nil
	}
	processEpoch, err := newStage4SQLServerProcessEpoch()
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	if scope == state.StrictSnapshotMigration {
		opener, err := NewSQLServerMigrationSnapshotOpener(
			relational.database,
			relational.endpoint,
			relational.namespace,
		)
		if err != nil {
			return resultForValidatedAdapterCheckpoints(completed), err
		}
		return migrateWithStage4SQLServerMigrationStrictAdapters(
			ctx,
			cfg,
			observer,
			source,
			target,
			prepared,
			networkExecution,
			opener,
			processEpoch,
			completedBinding,
			resume,
			completed,
			selectedTasks,
			cleanupBackend,
		)
	}
	if err := configureStage4SQLServerTableStrictSourcePool(
		relational,
		networkExecution.resources,
	); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	opener, err := NewSQLServerStrictConsistencyOpener(relational.database, relational.namespace)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	return migrateWithStage4SQLServerTableStrictAdapters(
		ctx,
		cfg,
		observer,
		source,
		target,
		prepared,
		networkExecution,
		opener,
		processEpoch,
		completedBinding,
		resume,
		completed,
	)
}

// configureStage4SQLServerTableStrictSourcePool raises the ordinary SQL
// Server source pool's deliberately conservative one-connection default only
// for a table-strict epoch. That epoch owns one serializable TABLOCK/HOLDLOCK
// transaction and opens each admitted range reader on a separate
// transaction-bound connection. The network admission path has already
// reserved the owner inside ConnectionLimit, so this must never create more
// source connections than that admitted strict reader envelope allows.
func configureStage4SQLServerTableStrictSourcePool(
	source *relationalSourceAdapter,
	resources config.EffectiveTransferPlan,
) error {
	if source == nil || source.database == nil || source.spec.engine != "mssql" {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server table-strict source pool requires the production SQL Server source adapter"),
		)
	}
	if resources.Readers.Value < 1 {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server table-strict source pool requires at least one admitted reader"),
		)
	}
	ownerAndReaders := resources.Readers.Value + 1
	if resources.ConnectionLimit.Value < ownerAndReaders {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"SQL Server table-strict source pool requires %d connections for its owner and admitted readers within connection_limit",
				ownerAndReaders,
			),
		)
	}
	source.database.SetMaxOpenConns(ownerAndReaders)
	source.database.SetMaxIdleConns(ownerAndReaders)
	return nil
}

type stage4SQLServerMigrationSnapshotFinalizer interface {
	OpenCompletedMigrationSnapshot(
		context.Context,
		string,
		state.StrictMigrationSnapshot,
		bool,
	) (SQLServerMigrationSnapshotFinalizationSession, bool, error)
}

func cleanupCompletedStage4SQLServerMigrationSnapshot(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	owner state.StrictMigrationSnapshot,
	finalizer stage4SQLServerMigrationSnapshotFinalizer,
	cleanupBackend state.StrictMigrationCleanupBackend,
) error {
	if isNilInterface(finalizer) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot cleanup seam is unavailable"),
		)
	}
	request := StrictConsistencyRequest{
		RunID: prepared.run.RunID, SourceEngine: StrictConsistencyMSSQL,
		Scope: state.StrictSnapshotMigration,
	}
	if err := validateDurableMigrationSnapshot(request, owner); err != nil {
		return NewTransferError(ErrorClassState, err)
	}
	if isNilInterface(cleanupBackend) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot cleanup receipt backend is unavailable"),
		)
	}
	intent, intentFound, err := cleanupBackend.LoadStrictMigrationCleanupIntent(
		prepared.run.RunID,
		owner.EpochID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("load SQL Server migration snapshot cleanup intent: %w", err),
		)
	}
	if intentFound && !stage4SQLServerMigrationCleanupIntentMatchesOwner(intent, owner) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot cleanup intent differs from durable owner"),
		)
	}
	session, absent, err := finalizer.OpenCompletedMigrationSnapshot(
		ctx,
		prepared.run.RunID,
		owner,
		intentFound,
	)
	if err != nil {
		if errors.Is(err, errSQLServerMigrationSnapshotMissing) {
			return errors.Join(err, ErrSQLServerMigrationSnapshotNotResumable)
		}
		return err
	}
	if absent {
		if !intentFound {
			return NewTransferError(
				ErrorClassState,
				errors.New("SQL Server migration snapshot is absent before durable cleanup intent"),
			)
		}
		return completeStage4SchemaGateSentinels(
			ctx,
			prepared.run,
			prepared.gate,
			prepared.evolution,
		)
	}
	if session == nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot finalizer returned no session"),
		)
	}
	return finalizeStage4SQLServerMigrationSnapshotCleanup(
		ctx,
		session,
		owner,
		intent,
		intentFound,
		cleanupBackend,
		func() error {
			return completeStage4SchemaGateSentinels(
				ctx,
				prepared.run,
				prepared.gate,
				prepared.evolution,
			)
		},
	)
}

func finalizeStage4SQLServerMigrationSnapshotCleanup(
	ctx context.Context,
	session SQLServerMigrationSnapshotFinalizationSession,
	owner state.StrictMigrationSnapshot,
	intent state.StrictMigrationCleanupIntent,
	intentFound bool,
	cleanupBackend state.StrictMigrationCleanupBackend,
	completeFinalState func() error,
) (resultErr error) {
	if session == nil || isNilInterface(session) {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot finalization session is unavailable"),
		)
	}
	if completeFinalState == nil {
		return NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration snapshot final state callback is unavailable"),
		)
	}
	if err := session.PreserveSnapshotForResume(); err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			cleanupCtx, cancel := context.WithTimeout(
				context.Background(),
				strictConsistencyCleanupTimeout,
			)
			defer cancel()
			if closeErr := session.Close(cleanupCtx); closeErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf(
					"preserve SQL Server migration snapshot after finalization failure: %w",
					closeErr,
				))
			}
		}
	}()
	if err := completeFinalState(); err != nil {
		return err
	}
	if !intentFound {
		intent = state.StrictMigrationCleanupIntent{
			RunID: owner.RunID, EpochID: owner.EpochID,
			SourceEngine:      owner.SourceEngine,
			SnapshotReference: owner.SnapshotReference,
			ProcessEpoch:      owner.ProcessEpoch,
			CapturedAt:        owner.CapturedAt,
			IntentAt:          time.Now().UTC(),
		}
		if err := cleanupBackend.SaveStrictMigrationCleanupIntent(intent); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("persist SQL Server migration snapshot cleanup intent before DROP: %w", err),
			)
		}
	}
	if err := session.AuthorizeSnapshotCleanup(); err != nil {
		return err
	}
	err := session.Close(ctx)
	closed = true
	return err
}

func stage4SQLServerMigrationCleanupIntentMatchesOwner(
	intent state.StrictMigrationCleanupIntent,
	owner state.StrictMigrationSnapshot,
) bool {
	return intent.RunID == owner.RunID && intent.EpochID == owner.EpochID &&
		intent.SourceEngine == owner.SourceEngine &&
		intent.SnapshotReference == owner.SnapshotReference &&
		intent.ProcessEpoch == owner.ProcessEpoch &&
		intent.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) &&
		!intent.IntentAt.IsZero()
}

func migrateWithStage4SQLServerMigrationStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	opener *SQLServerMigrationSnapshotOpener,
	processEpoch string,
	completedBinding stage4SQLServerStrictEpochBinding,
	resume bool,
	completed map[string]int,
	selectedTasks []state.TaskKey,
	cleanupBackend state.StrictMigrationCleanupBackend,
) (result Result, resultErr error) {
	strictExecution, err := BeginPlannedStrictConsistency(
		ctx,
		PlannedStrictConsistencyRequest{
			RunID:        prepared.run.RunID,
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			Resume:       resume,
			ProcessEpoch: processEpoch,
			State:        prepared.run.Backend,
			Tasks:        append([]state.TaskKey(nil), selectedTasks...),
		},
		opener,
		func(
			planCtx context.Context,
			rawSession StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			session, ok := rawSession.(*SQLServerMigrationSnapshotSession)
			if !ok || session == nil {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New(
						"SQL Server planned migration-strict session has an unexpected type",
					),
				)
			}
			currentBinding, err := newStage4SQLServerMigrationEpochBinding(
				capture,
			)
			if err != nil {
				return nil, err
			}
			binding, err := completedBinding.merge(currentBinding)
			if err != nil {
				return nil, err
			}
			networkExecution.finalizeWork = binding.finalizeWork
			return planStage4SQLServerMigrationStrictNetworkWork(
				planCtx,
				observer,
				source,
				prepared,
				networkExecution,
				session,
				resume,
				completed,
			)
		},
	)
	if err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	owner, found := strictExecution.MigrationSnapshot()
	if !found || owner.SourceEngine != "mssql" ||
		owner.SnapshotReference == "" || owner.EpochID == "" {
		// Report a close failure alongside the ownership failure. Joining it
		// into err discarded it, because the return below builds a fresh
		// error — so a strict execution that failed to release its snapshot
		// and locks did so silently.
		ownershipErr := errors.New(
			"SQL Server migration strict execution lacks durable snapshot ownership",
		)
		if closeErr := strictExecution.Close(ctx); closeErr != nil {
			ownershipErr = errors.Join(ownershipErr, closeErr)
		}
		return resultForValidatedAdapterCheckpoints(completed), NewTransferError(
			ErrorClassState,
			ownershipErr,
		)
	}
	strictPrepared := prepared
	strictPrepared.strictSourceRows, err = stage4SQLServerMigrationStrictSourceRows(
		owner,
		strictExecution.Evidence(),
	)
	if err != nil {
		closeErr := strictExecution.Close(ctx)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	currentBinding, err := stage4SQLServerMigrationBindingFromEvidence(
		owner,
		strictExecution.Evidence(),
	)
	if err != nil {
		closeErr := strictExecution.Close(ctx)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	nextBinding, err := completedBinding.merge(currentBinding)
	if err != nil {
		closeErr := strictExecution.Close(ctx)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	resultErr = strictExecution.Run(
		ctx,
		func(runCtx context.Context, rawSession StrictConsistencySession) error {
			session, ok := rawSession.(*SQLServerMigrationSnapshotSession)
			if !ok || session == nil {
				return NewTransferError(
					ErrorClassState,
					errors.New(
						"SQL Server migration-strict execution session has an unexpected type",
					),
				)
			}
			if err := applyStage4AdapterTargetSchema(
				runCtx,
				observer,
				strictPrepared.run,
				strictPrepared.gate,
				strictPrepared.evolution,
			); err != nil {
				return err
			}
			if err := preflightStage4AdapterDesiredTargetAfterEvolution(
				runCtx,
				target,
				strictPrepared,
			); err != nil {
				return err
			}
			for planIndex, plan := range strictPrepared.plans {
				if rows, complete := completed[plan.source.Name]; complete {
					if err := networkExecution.advanceCompletedTable(
						runCtx,
						planIndex,
						rows,
					); err != nil {
						return err
					}
					continue
				}
				copied, err := runStage4SQLServerMigrationStrictTable(
					runCtx,
					cfg,
					observer,
					source,
					target,
					strictPrepared,
					networkExecution,
					session,
					planIndex,
					resume,
				)
				if err != nil {
					return err
				}
				if copied < 0 || result.Rows > math.MaxInt-copied {
					return NewTransferError(
						ErrorClassState,
						errors.New("SQL Server migration-strict result row total overflows"),
					)
				}
				result.Tables++
				result.Rows += copied
			}
			// Ordinary transfer and target failures above are terminal for this
			// snapshot and must release it. Only after every strict source/table
			// work item is durable do we retain the snapshot through final
			// sentinel and cleanup-intent state transitions.
			if err := session.PreserveSnapshotForResume(); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("preserve SQL Server migration snapshot for finalization: %w", err),
				)
			}
			if err := completeStage4SchemaGateSentinels(
				runCtx,
				strictPrepared.run,
				strictPrepared.gate,
				strictPrepared.evolution,
			); err != nil {
				return err
			}
			if err := cleanupBackend.SaveStrictMigrationCleanupIntent(
				state.StrictMigrationCleanupIntent{
					RunID:             owner.RunID,
					EpochID:           owner.EpochID,
					SourceEngine:      owner.SourceEngine,
					SnapshotReference: owner.SnapshotReference,
					ProcessEpoch:      owner.ProcessEpoch,
					CapturedAt:        owner.CapturedAt,
					IntentAt:          time.Now().UTC(),
				},
			); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("persist SQL Server migration snapshot cleanup intent before DROP: %w", err),
				)
			}
			if err := session.AuthorizeSnapshotCleanup(); err != nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("authorize SQL Server migration snapshot cleanup after durable intent: %w", err),
				)
			}
			result.Validated = true
			return nil
		},
	)
	if resultErr == nil {
		networkExecution.finalizeWork = nextBinding.finalizeWork
	}
	return result, resultErr
}

func migrateWithStage4SQLServerTableStrictAdapters(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	opener *SQLServerStrictConsistencyOpener,
	processEpoch string,
	completedBinding stage4SQLServerStrictEpochBinding,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	if err := checkpointStage4AdapterTableSet(ctx, observer, prepared.names); err != nil {
		return resultForValidatedAdapterCheckpoints(completed), err
	}
	result = resultForValidatedAdapterCheckpoints(completed)
	schemaApplied := false
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := networkExecution.advanceCompletedTable(ctx, planIndex, rows); err != nil {
				return result, err
			}
			continue
		}
		task := prepared.work[planIndex].task
		strictExecution, err := BeginPlannedStrictConsistency(
			ctx,
			PlannedStrictConsistencyRequest{
				RunID:        prepared.run.RunID,
				SourceEngine: StrictConsistencyMSSQL,
				Scope:        state.StrictSnapshotTable,
				Resume:       resume,
				ProcessEpoch: processEpoch,
				State:        prepared.run.Backend,
				Tasks:        []state.TaskKey{task},
			},
			opener,
			func(
				planCtx context.Context,
				rawSession StrictConsistencySession,
				capture StrictConsistencyCapture,
			) ([]StrictConsistencyTable, error) {
				session, ok := rawSession.(*SQLServerStrictConsistencySession)
				if !ok || session == nil {
					return nil, NewTransferError(
						ErrorClassState,
						errors.New(
							"SQL Server planned table-strict session has an unexpected type",
						),
					)
				}
				currentBinding, err := newStage4SQLServerStrictEpochBinding(
					processEpoch,
					capture,
				)
				if err != nil {
					return nil, err
				}
				binding, err := completedBinding.merge(currentBinding)
				if err != nil {
					return nil, err
				}
				networkExecution.finalizeWork = binding.finalizeWork
				table, err := planStage4SQLServerStrictNetworkTable(
					planCtx,
					source,
					prepared,
					networkExecution,
					session,
					planIndex,
					resume,
				)
				if err != nil {
					return nil, err
				}
				return []StrictConsistencyTable{table}, nil
			},
		)
		if err != nil {
			return result, err
		}
		strictPrepared := prepared
		strictPrepared.strictSourceRows, err = stage4SQLServerStrictSourceRows(
			strictExecution.Evidence(),
		)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		currentBinding, err := newStage4SQLServerStrictEpochBinding(
			processEpoch,
			strictConsistencyCaptureFromEvidence(strictExecution.Evidence()),
		)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		nextBinding, err := completedBinding.merge(currentBinding)
		if err != nil {
			closeErr := strictExecution.Close(ctx)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, err
		}
		resultErr = strictExecution.Run(
			ctx,
			func(runCtx context.Context, rawSession StrictConsistencySession) error {
				session, ok := rawSession.(*SQLServerStrictConsistencySession)
				if !ok || session == nil {
					return NewTransferError(
						ErrorClassState,
						errors.New(
							"SQL Server table-strict execution session has an unexpected type",
						),
					)
				}
				if !schemaApplied {
					if err := applyStage4AdapterTargetSchema(
						runCtx,
						observer,
						strictPrepared.run,
						strictPrepared.gate,
						strictPrepared.evolution,
					); err != nil {
						return err
					}
					if err := preflightStage4AdapterDesiredTargetAfterEvolution(
						runCtx,
						target,
						strictPrepared,
					); err != nil {
						return err
					}
					schemaApplied = true
				}
				copied, err := runStage4SQLServerStrictTable(
					runCtx,
					cfg,
					observer,
					source,
					target,
					strictPrepared,
					networkExecution,
					session,
					planIndex,
					resume,
				)
				if err != nil {
					return err
				}
				if copied < 0 || result.Rows > math.MaxInt-copied {
					return NewTransferError(
						ErrorClassState,
						errors.New("SQL Server strict result row total overflows"),
					)
				}
				result.Tables++
				result.Rows += copied
				return nil
			},
		)
		if resultErr != nil {
			return result, resultErr
		}
		completedBinding = nextBinding
		networkExecution.finalizeWork = completedBinding.finalizeWork
	}
	if err := completeStage4SchemaGateSentinels(
		ctx,
		prepared.run,
		prepared.gate,
		prepared.evolution,
	); err != nil {
		return result, err
	}
	result.Validated = true
	return result, nil
}

func planStage4SQLServerStrictNetworkTable(
	ctx context.Context,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *SQLServerStrictConsistencySession,
	planIndex int,
	resume bool,
) (_ StrictConsistencyTable, resultErr error) {
	plan := prepared.plans[planIndex]
	execution.mu.Lock()
	startGlobalRange := execution.nextGlobalRange
	execution.mu.Unlock()
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = startGlobalRange
		execution.mu.Unlock()
	}()
	var work stage4AdapterWork
	err := RunSQLServerAdapterStableNetworkReader(
		ctx,
		session,
		prepared.work[planIndex].task,
		source,
		plan.source,
		func(readerCtx context.Context, stable adapterStableNetworkSource) error {
			borrowed := &adapterStableNetworkTableSession{
				source:      stable,
				readerLimit: execution.resources.Readers.Value,
			}
			tableExecution, err := execution.openTable(
				readerCtx,
				planIndex,
				resume,
				borrowed,
			)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := tableExecution.Close(); closeErr != nil {
					resultErr = errors.Join(resultErr, closeErr)
				}
			}()
			work = cloneStage4AdapterNetworkWork(tableExecution.work)
			return nil
		},
	)
	if err != nil {
		return StrictConsistencyTable{}, err
	}
	attemptID, err := BuildStrictConsistencyAttemptID(work.task, work.topology, 0)
	if err != nil {
		return StrictConsistencyTable{}, fmt.Errorf(
			"build SQL Server table-strict work attempt for %s: %w",
			plan.source.Name,
			err,
		)
	}
	return StrictConsistencyTable{
		Task:                work.task,
		AttemptID:           attemptID,
		WorkTopologyHash:    work.topology,
		DurableWorkAttempts: 0,
	}, resultErr
}

func planStage4SQLServerMigrationStrictNetworkWork(
	ctx context.Context,
	observer TableObserver,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	session *SQLServerMigrationSnapshotSession,
	resume bool,
	completed map[string]int,
) (_ []StrictConsistencyTable, resultErr error) {
	if err := checkpointStage4AdapterTableSet(ctx, observer, prepared.names); err != nil {
		return nil, err
	}
	defer func() {
		execution.mu.Lock()
		execution.nextGlobalRange = 0
		execution.mu.Unlock()
	}()
	result := make([]StrictConsistencyTable, 0, len(prepared.plans))
	for planIndex, plan := range prepared.plans {
		if rows, complete := completed[plan.source.Name]; complete {
			if err := execution.advanceCompletedTable(ctx, planIndex, rows); err != nil {
				return nil, err
			}
			continue
		}
		var work stage4AdapterWork
		err := RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
			ctx,
			session,
			prepared.work[planIndex].task,
			source,
			plan.source,
			func(readerCtx context.Context, stable adapterStableNetworkSource) error {
				borrowed := &adapterStableNetworkTableSession{
					source: stable, readerLimit: execution.resources.Readers.Value,
				}
				tableExecution, err := execution.openTable(
					readerCtx,
					planIndex,
					resume,
					borrowed,
				)
				if err != nil {
					return err
				}
				defer func() {
					if closeErr := tableExecution.Close(); closeErr != nil {
						resultErr = errors.Join(resultErr, closeErr)
					}
				}()
				work = cloneStage4AdapterNetworkWork(tableExecution.work)
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
		attemptID, err := BuildStrictConsistencyAttemptID(work.task, work.topology, 0)
		if err != nil {
			return nil, fmt.Errorf(
				"build SQL Server migration-strict work attempt for %s: %w",
				plan.source.Name,
				err,
			)
		}
		result = append(result, StrictConsistencyTable{
			Task:                work.task,
			AttemptID:           attemptID,
			WorkTopologyHash:    work.topology,
			DurableWorkAttempts: 0,
		})
	}
	return result, resultErr
}

func runStage4SQLServerStrictTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	session *SQLServerStrictConsistencySession,
	planIndex int,
	resume bool,
) (copied int, resultErr error) {
	plan := prepared.plans[planIndex]
	err := RunSQLServerAdapterStableNetworkReader(
		ctx,
		session,
		prepared.work[planIndex].task,
		source,
		plan.source,
		func(readerCtx context.Context, stable adapterStableNetworkSource) error {
			parallel, err := newStage4SQLServerStrictParallelSource(
				stable,
				networkExecution.resources.Readers.Value,
				func(
					secondaryCtx context.Context,
					work func(adapterStableNetworkSource) error,
				) error {
					return RunSQLServerAdapterStableNetworkReader(
						secondaryCtx,
						session,
						prepared.work[planIndex].task,
						source,
						plan.source,
						func(_ context.Context, secondary adapterStableNetworkSource) error {
							if err := inheritStage4SQLServerStrictLockProofs(stable, secondary); err != nil {
								return err
							}
							return work(secondary)
						},
					)
				},
			)
			if err != nil {
				return err
			}
			borrowed := &adapterStableNetworkTableSession{
				source:      parallel,
				readerLimit: networkExecution.resources.Readers.Value,
			}
			tableExecution, err := networkExecution.openTable(
				readerCtx,
				planIndex,
				false,
				borrowed,
			)
			if err != nil {
				return err
			}
			copied, err = runStage4AdapterStableNetworkTable(
				readerCtx,
				cfg,
				observer,
				target,
				prepared,
				planIndex,
				tableExecution,
				resume,
			)
			return err
		},
	)
	if err != nil {
		return 0, err
	}
	return copied, resultErr
}

func runStage4SQLServerMigrationStrictTable(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	networkExecution *stage4AdapterNetworkExecution,
	session *SQLServerMigrationSnapshotSession,
	planIndex int,
	resume bool,
) (copied int, resultErr error) {
	plan := prepared.plans[planIndex]
	err := RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
		ctx,
		session,
		prepared.work[planIndex].task,
		source,
		plan.source,
		func(readerCtx context.Context, stable adapterStableNetworkSource) error {
			parallel, err := newStage4SQLServerStrictParallelSource(
				stable,
				networkExecution.resources.Readers.Value,
				func(
					secondaryCtx context.Context,
					work func(adapterStableNetworkSource) error,
				) error {
					return RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
						secondaryCtx,
						session,
						prepared.work[planIndex].task,
						source,
						plan.source,
						func(_ context.Context, secondary adapterStableNetworkSource) error {
							if err := inheritStage4SQLServerStrictLockProofs(stable, secondary); err != nil {
								return err
							}
							return work(secondary)
						},
					)
				},
			)
			if err != nil {
				return err
			}
			borrowed := &adapterStableNetworkTableSession{
				source: parallel, readerLimit: networkExecution.resources.Readers.Value,
			}
			tableExecution, err := networkExecution.openTable(
				readerCtx,
				planIndex,
				false,
				borrowed,
			)
			if err != nil {
				return err
			}
			copied, err = runStage4AdapterStableNetworkTable(
				readerCtx,
				cfg,
				observer,
				target,
				prepared,
				planIndex,
				tableExecution,
				resume,
			)
			return err
		},
	)
	if err != nil {
		return 0, err
	}
	return copied, resultErr
}

type stage4SQLServerStrictEpochBinding struct {
	tables map[state.TaskKey]stage4SQLServerStrictTableBinding
}

type stage4SQLServerStrictTableBinding struct {
	scope             state.StrictSnapshotScope
	processEpoch      string
	migrationEpoch    string
	snapshotReference string
}

func newStage4SQLServerStrictEpochBinding(
	processEpoch string,
	capture StrictConsistencyCapture,
) (stage4SQLServerStrictEpochBinding, error) {
	if err := validateCredentialFreeIdentifier(
		"SQL Server strict process epoch",
		processEpoch,
	); err != nil {
		return stage4SQLServerStrictEpochBinding{}, err
	}
	tables := make(map[state.TaskKey]stage4SQLServerStrictTableBinding, len(capture.Tables))
	for _, table := range capture.Tables {
		if _, duplicate := tables[table.Task]; duplicate {
			return stage4SQLServerStrictEpochBinding{}, errors.New(
				"SQL Server strict capture duplicates a table reference",
			)
		}
		if err := validateSnapshotReference(table.SnapshotReference); err != nil {
			return stage4SQLServerStrictEpochBinding{}, err
		}
		tables[table.Task] = stage4SQLServerStrictTableBinding{
			scope:             state.StrictSnapshotTable,
			processEpoch:      processEpoch,
			snapshotReference: table.SnapshotReference,
		}
	}
	if len(tables) == 0 {
		return stage4SQLServerStrictEpochBinding{}, errors.New(
			"SQL Server strict capture has no table references",
		)
	}
	return stage4SQLServerStrictEpochBinding{tables: tables}, nil
}

// newStage4SQLServerMigrationEpochBinding intentionally omits the caller's
// fresh coordinator epoch from work topology. A migration database snapshot is
// durable across resume, while each coordinator process epoch is deliberately
// fresh; using it here would make identical work/attempt identities drift on
// every resume (including the pre-owner crash recovery path).
func newStage4SQLServerMigrationEpochBinding(
	capture StrictConsistencyCapture,
) (stage4SQLServerStrictEpochBinding, error) {
	if err := validateCredentialFreeIdentifier(
		"SQL Server migration snapshot epoch",
		capture.MigrationEpochID,
	); err != nil {
		return stage4SQLServerStrictEpochBinding{}, err
	}
	if err := validateSnapshotReference(capture.MigrationSnapshotReference); err != nil {
		return stage4SQLServerStrictEpochBinding{}, err
	}
	if capture.MigrationCapturedAt.IsZero() {
		return stage4SQLServerStrictEpochBinding{}, errors.New(
			"SQL Server migration snapshot capture time is missing",
		)
	}
	tables := make(map[state.TaskKey]stage4SQLServerStrictTableBinding, len(capture.Tables))
	for _, table := range capture.Tables {
		if _, duplicate := tables[table.Task]; duplicate {
			return stage4SQLServerStrictEpochBinding{}, errors.New(
				"SQL Server migration strict capture duplicates a table reference",
			)
		}
		if table.SnapshotReference != capture.MigrationSnapshotReference {
			return stage4SQLServerStrictEpochBinding{}, fmt.Errorf(
				"SQL Server migration strict table %s.%s differs from the durable database snapshot",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		tables[table.Task] = stage4SQLServerStrictTableBinding{
			scope:             state.StrictSnapshotMigration,
			migrationEpoch:    capture.MigrationEpochID,
			snapshotReference: capture.MigrationSnapshotReference,
		}
	}
	if len(tables) == 0 {
		return stage4SQLServerStrictEpochBinding{}, errors.New(
			"SQL Server migration strict capture has no table references",
		)
	}
	return stage4SQLServerStrictEpochBinding{tables: tables}, nil
}

func stage4SQLServerMigrationBindingFromEvidence(
	owner state.StrictMigrationSnapshot,
	evidence []state.StrictSnapshotEvidence,
) (stage4SQLServerStrictEpochBinding, error) {
	capture := StrictConsistencyCapture{Tables: make([]StrictConsistencyTableCapture, 0, len(evidence))}
	for _, record := range evidence {
		if record.SourceEngine != "mssql" ||
			record.Scope != state.StrictSnapshotMigration ||
			record.MigrationEpochID != owner.EpochID ||
			record.SnapshotReference != owner.SnapshotReference ||
			record.ProcessEpoch != owner.ProcessEpoch ||
			!record.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				errors.New("SQL Server migration evidence has an invalid scope"),
			)
		}
		if capture.MigrationEpochID == "" {
			capture.MigrationEpochID = record.MigrationEpochID
			capture.MigrationSnapshotReference = record.SnapshotReference
			capture.MigrationCapturedAt = record.CapturedAt
		} else if record.MigrationEpochID != capture.MigrationEpochID ||
			record.SnapshotReference != capture.MigrationSnapshotReference ||
			!record.CapturedAt.UTC().Equal(capture.MigrationCapturedAt.UTC()) {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				errors.New("SQL Server migration evidence spans multiple database snapshots"),
			)
		}
		capture.Tables = append(capture.Tables, StrictConsistencyTableCapture{
			Task: record.Task, AttemptID: record.AttemptID,
			SnapshotReference:   record.SnapshotReference,
			ExactSourceRowCount: record.ExactSourceRowCount,
			CapturedAt:          record.CapturedAt,
		})
	}
	return newStage4SQLServerMigrationEpochBinding(capture)
}

func (binding stage4SQLServerStrictEpochBinding) merge(
	other stage4SQLServerStrictEpochBinding,
) (stage4SQLServerStrictEpochBinding, error) {
	tables := make(map[state.TaskKey]stage4SQLServerStrictTableBinding, len(binding.tables)+len(other.tables))
	for task, table := range binding.tables {
		tables[task] = table
	}
	for task, table := range other.tables {
		if _, duplicate := tables[task]; duplicate {
			return stage4SQLServerStrictEpochBinding{}, fmt.Errorf(
				"SQL Server strict epoch binding duplicates task %s.%s",
				task.Schema,
				task.Table,
			)
		}
		tables[task] = table
	}
	return stage4SQLServerStrictEpochBinding{tables: tables}, nil
}

func (binding stage4SQLServerStrictEpochBinding) finalizeWork(
	work stage4AdapterWork,
) (stage4AdapterWork, error) {
	table, ok := binding.tables[work.task]
	if !ok {
		return stage4AdapterWork{}, fmt.Errorf(
			"SQL Server strict epoch has no table-lock reference for %s.%s",
			work.task.Schema,
			work.task.Table,
		)
	}
	wire := struct {
		Version           int    `json:"version"`
		Scope             string `json:"scope"`
		BaseTopology      string `json:"base_topology"`
		ProcessEpoch      string `json:"process_epoch"`
		MigrationEpoch    string `json:"migration_epoch"`
		SnapshotReference string `json:"snapshot_reference"`
	}{
		Version:           stage4SQLServerStrictWorkVersion,
		Scope:             string(table.scope),
		BaseTopology:      work.topology,
		ProcessEpoch:      table.processEpoch,
		MigrationEpoch:    table.migrationEpoch,
		SnapshotReference: table.snapshotReference,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return stage4AdapterWork{}, fmt.Errorf(
			"encode SQL Server strict work identity: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	work.topology = hex.EncodeToString(digest[:])
	for index := range work.ranges {
		work.ranges[index].TopologyHash = work.topology
	}
	return work, nil
}

func stage4SQLServerStrictScope(value string) (state.StrictSnapshotScope, error) {
	normalized, err := normalizedStrictConsistencyScope(value)
	if err != nil {
		return "", NewTransferError(ErrorClassPolicy, err)
	}
	switch normalized {
	case config.StrictConsistencyTable:
		return state.StrictSnapshotTable, nil
	case config.StrictConsistencyMigration:
		return state.StrictSnapshotMigration, nil
	default:
		return "", NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("SQL Server strict consistency scope %q is unsupported", normalized),
		)
	}
}

func newStage4SQLServerProcessEpoch() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", NewTransferError(
			ErrorClassState,
			fmt.Errorf("generate SQL Server strict process epoch: %w", err),
		)
	}
	return "mssql-process-" + hex.EncodeToString(value[:]), nil
}

func stage4SQLServerStrictSourceRows(
	evidence []state.StrictSnapshotEvidence,
) (map[stage4RichTableKey]int64, error) {
	result := make(map[stage4RichTableKey]int64, len(evidence))
	for _, record := range evidence {
		if record.SourceEngine != "mssql" || record.Scope != state.StrictSnapshotTable ||
			record.ExactSourceRowCount < 0 {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New("SQL Server strict execution returned invalid count evidence"),
			)
		}
		key := stage4RichTableKey{schema: record.Task.Schema, table: record.Task.Table}
		if _, duplicate := result[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQL Server strict execution duplicates count evidence for %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		result[key] = record.ExactSourceRowCount
	}
	return result, nil
}

func stage4SQLServerMigrationStrictSourceRows(
	owner state.StrictMigrationSnapshot,
	evidence []state.StrictSnapshotEvidence,
) (map[stage4RichTableKey]int64, error) {
	if owner.SourceEngine != "mssql" || owner.EpochID == "" ||
		owner.SnapshotReference == "" || owner.CapturedAt.IsZero() {
		return nil, NewTransferError(
			ErrorClassState,
			errors.New("SQL Server migration strict execution lacks a valid durable snapshot owner"),
		)
	}
	result := make(map[stage4RichTableKey]int64, len(evidence))
	for _, record := range evidence {
		if record.SourceEngine != "mssql" ||
			record.Scope != state.StrictSnapshotMigration ||
			record.MigrationEpochID != owner.EpochID ||
			record.SnapshotReference != owner.SnapshotReference ||
			record.ProcessEpoch != owner.ProcessEpoch ||
			record.ExactSourceRowCount < 0 || record.CapturedAt.IsZero() ||
			!record.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New("SQL Server migration strict execution returned invalid database-snapshot count evidence"),
			)
		}
		key := stage4RichTableKey{schema: record.Task.Schema, table: record.Task.Table}
		if _, duplicate := result[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"SQL Server migration strict execution duplicates count evidence for %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		result[key] = record.ExactSourceRowCount
	}
	return result, nil
}

func validateCompletedStage4SQLServerStrictEvidence(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	scope state.StrictSnapshotScope,
	completed map[string]int,
) (stage4SQLServerStrictEpochBinding, error) {
	if scope == state.StrictSnapshotMigration {
		return validateCompletedStage4SQLServerMigrationStrictEvidence(
			ctx,
			prepared,
			completed,
		)
	}
	if scope != state.StrictSnapshotTable {
		return stage4SQLServerStrictEpochBinding{}, NewTransferError(
			ErrorClassPolicy,
			errors.New("SQL Server strict completion validation has an unsupported scope"),
		)
	}
	binding := stage4SQLServerStrictEpochBinding{
		tables: make(map[state.TaskKey]stage4SQLServerStrictTableBinding, len(completed)),
	}
	if len(completed) == 0 {
		return binding, ctx.Err()
	}
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return stage4SQLServerStrictEpochBinding{}, err
	}
	for index, plan := range prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		base := prepared.work[index]
		task, found := inventory.tasks[base.task]
		if !found || task.Status != "completed" || task.TopologyHash == "" {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed SQL Server strict table %s lacks exact durable work",
					plan.source.Name,
				),
			)
		}
		attemptID, err := BuildStrictConsistencyAttemptID(task.Key, task.TopologyHash, 0)
		if err != nil {
			return stage4SQLServerStrictEpochBinding{}, err
		}
		evidence, found, err := prepared.run.Backend.LoadStrictSnapshotEvidence(
			prepared.run.RunID,
			task.Key,
			attemptID,
		)
		if err != nil {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"load completed SQL Server strict evidence for %s: %w",
					plan.source.Name,
					err,
				),
			)
		}
		if !found || evidence.RunID != prepared.run.RunID || evidence.Task != task.Key ||
			evidence.AttemptID != attemptID || evidence.SourceEngine != "mssql" ||
			evidence.Scope != state.StrictSnapshotTable || evidence.MigrationEpochID != "" ||
			evidence.ProcessEpoch == "" || evidence.SnapshotReference == "" ||
			evidence.ExactSourceRowCount != int64(rows) || evidence.CapturedAt.IsZero() {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed SQL Server strict table %s lacks matching immutable lock evidence",
					plan.source.Name,
				),
			)
		}
		if err := validateCredentialFreeIdentifier(
			"completed SQL Server strict process epoch",
			evidence.ProcessEpoch,
		); err != nil {
			return stage4SQLServerStrictEpochBinding{}, err
		}
		if err := validateSnapshotReference(evidence.SnapshotReference); err != nil {
			return stage4SQLServerStrictEpochBinding{}, err
		}
		binding.tables[task.Key] = stage4SQLServerStrictTableBinding{
			scope:             state.StrictSnapshotTable,
			processEpoch:      evidence.ProcessEpoch,
			snapshotReference: evidence.SnapshotReference,
		}
	}
	return binding, ctx.Err()
}

func validateCompletedStage4SQLServerMigrationStrictEvidence(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	completed map[string]int,
) (stage4SQLServerStrictEpochBinding, error) {
	binding := stage4SQLServerStrictEpochBinding{
		tables: make(map[state.TaskKey]stage4SQLServerStrictTableBinding, len(completed)),
	}
	if len(completed) == 0 {
		return binding, ctx.Err()
	}
	owner, found, err := prepared.run.Backend.LoadLatestStrictMigrationSnapshot(
		prepared.run.RunID,
	)
	if err != nil {
		return stage4SQLServerStrictEpochBinding{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("load completed SQL Server migration snapshot owner: %w", err),
		)
	}
	request := StrictConsistencyRequest{
		RunID: prepared.run.RunID, SourceEngine: StrictConsistencyMSSQL,
		Scope: state.StrictSnapshotMigration,
	}
	if !found {
		return stage4SQLServerStrictEpochBinding{}, NewTransferError(
			ErrorClassState,
			errors.New("completed SQL Server migration-strict work lacks its durable database snapshot owner"),
		)
	}
	if err := validateDurableMigrationSnapshot(request, owner); err != nil {
		return stage4SQLServerStrictEpochBinding{}, NewTransferError(ErrorClassState, err)
	}
	inventory, err := loadStage4WorkInventory(ctx, prepared.run)
	if err != nil {
		return stage4SQLServerStrictEpochBinding{}, err
	}
	for index, plan := range prepared.plans {
		rows, complete := completed[plan.source.Name]
		if !complete {
			continue
		}
		base := prepared.work[index]
		task, found := inventory.tasks[base.task]
		if !found || task.Status != "completed" || task.TopologyHash == "" {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server migration-strict table %s lacks exact durable work", plan.source.Name),
			)
		}
		attemptID, err := BuildStrictConsistencyAttemptID(task.Key, task.TopologyHash, 0)
		if err != nil {
			return stage4SQLServerStrictEpochBinding{}, err
		}
		evidence, found, err := prepared.run.Backend.LoadStrictSnapshotEvidence(
			prepared.run.RunID,
			task.Key,
			attemptID,
		)
		if err != nil {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("load completed SQL Server migration strict evidence for %s: %w", plan.source.Name, err),
			)
		}
		if !found || evidence.RunID != prepared.run.RunID || evidence.Task != task.Key ||
			evidence.AttemptID != attemptID || evidence.SourceEngine != "mssql" ||
			evidence.Scope != state.StrictSnapshotMigration ||
			evidence.MigrationEpochID != owner.EpochID ||
			evidence.SnapshotReference != owner.SnapshotReference ||
			evidence.ProcessEpoch != owner.ProcessEpoch ||
			evidence.ExactSourceRowCount != int64(rows) || evidence.CapturedAt.IsZero() ||
			!evidence.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) {
			return stage4SQLServerStrictEpochBinding{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf("completed SQL Server migration-strict table %s lacks matching immutable database-snapshot evidence", plan.source.Name),
			)
		}
		binding.tables[task.Key] = stage4SQLServerStrictTableBinding{
			scope:             state.StrictSnapshotMigration,
			migrationEpoch:    owner.EpochID,
			snapshotReference: owner.SnapshotReference,
		}
	}
	return binding, ctx.Err()
}

var _ adapterNetworkRangePageSource = (*stage4SQLServerStrictParallelSource)(nil)
var _ adapterStage4ValidationProbeProvider = (*stage4SQLServerStrictParallelSource)(nil)
