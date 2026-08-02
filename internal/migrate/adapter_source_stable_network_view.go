package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// adapterStableNetworkSource is the complete source-side surface whose
// planning and reads must share one engine-owned view.
type adapterStableNetworkSource interface {
	adapterNetworkStableRangePageSource
	paginationSourceAdapter
	adapterSourceRetainedRowBounder
}

// adapterNetworkStableRangePageSource is the admission marker for a source
// whose ROW_NUMBER topology and pages are guaranteed to use one stable view.
// A mutable relationalSourceAdapter deliberately does not implement it.
type adapterNetworkStableRangePageSource interface {
	sourceAdapter
	adapterNetworkRangePageSource
	networkStableRangePageSource()
}

var (
	_ adapterStableNetworkSource          = (*adapterRetainedStableRelationalView)(nil)
	_ adapterStableNetworkSource          = (*sqliteSourceAdapter)(nil)
	_ sourceAdapter                       = (*adapterRetainedStableRelationalView)(nil)
	_ adapterNetworkStableRangePageSource = (*adapterRetainedStableRelationalView)(nil)
	_ adapterNetworkStableRangePageSource = (*sqliteSourceAdapter)(nil)
)

func (*adapterRetainedStableRelationalView) networkStableRangePageSource() {}
func (*sqliteSourceAdapter) networkStableRangePageSource()                 {}

func (view *adapterRetainedStableRelationalView) ListTables(
	ctx context.Context,
) ([]string, error) {
	if ctx == nil {
		return nil, errors.New("stable source metadata context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if view == nil {
		return nil, errors.New("stable relational source view is unavailable")
	}
	view.mu.Lock()
	catalog := view.tableCatalog
	view.mu.Unlock()
	if catalog == nil {
		return nil, errors.New(
			"stable relational source view has no immutable table catalog",
		)
	}
	return []string{catalog.Name}, nil
}

func (view *adapterRetainedStableRelationalView) InspectTable(
	ctx context.Context,
	name string,
) (schema.Table, error) {
	if ctx == nil {
		return schema.Table{}, errors.New(
			"stable source metadata context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return schema.Table{}, err
	}
	if view == nil {
		return schema.Table{}, errors.New(
			"stable relational source view is unavailable",
		)
	}
	view.mu.Lock()
	catalog := view.tableCatalog
	view.mu.Unlock()
	if catalog == nil || catalog.Name != name {
		return schema.Table{}, fmt.Errorf(
			"stable relational source view has no immutable catalog for table %q",
			name,
		)
	}
	if view.source == nil || isNilInterface(view.view) {
		return schema.Table{}, errors.New(
			"stable relational source view is unavailable",
		)
	}
	switch view.source.spec.engine {
	case "postgres":
		inspected, err := engine.InspectPostgresTableWithQueryer(
			ctx,
			view.view,
			view.source.namespace,
			name,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"inspect stable PostgreSQL source table %s: %w",
				name,
				err,
			)
		}
		return inspected, nil
	case "mysql":
		if view.source.mySQLFlavor ==
			engine.MySQLServerFlavorUnknown {
			return schema.Table{}, errors.New(
				"stable MySQL-family source flavor is unavailable",
			)
		}
		inspected, err := engine.InspectMySQLTableForFlavor(
			ctx,
			view.view,
			view.source.mySQLFlavor,
			view.source.namespace,
			name,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"inspect stable MySQL-family source table %s: %w",
				name,
				err,
			)
		}
		return inspected, nil
	case "mssql":
		inspect := engine.InspectSQLServerTableWithQueryer
		if view.sqlServerSnapshot {
			inspect = engine.InspectSQLServerMigrationSnapshotTableWithQueryer
		}
		inspected, err := inspect(
			ctx,
			view.view,
			view.source.namespace,
			name,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"inspect stable SQL Server source table %s: %w",
				name,
				err,
			)
		}
		return inspected, nil
	}
	return cloneStage4RichTable(*catalog), nil
}

func (view *adapterRetainedStableRelationalView) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	if ctx == nil {
		return 0, errors.New("stable source count context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if view == nil || view.source == nil || isNilInterface(view.view) {
		return 0, errors.New("stable relational source view is unavailable")
	}
	if err := view.admitTable(table); err != nil {
		return 0, err
	}
	countExpression := "COUNT(*)"
	if view.source.spec.engine == "mssql" {
		countExpression = "COUNT_BIG(*)"
	}
	var count int64
	if err := view.view.QueryRowContext(
		ctx,
		"SELECT "+countExpression+" FROM "+
			view.source.spec.qualifiedTable(
				view.source.namespace,
				table.Name,
			),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count %s stable source table %s: %w",
			view.source.spec.displayName,
			table.Name,
			err,
		)
	}
	if count < 0 || count > int64(math.MaxInt) {
		return 0, fmt.Errorf(
			"count %s stable source table %s exceeds process limits",
			view.source.spec.displayName,
			table.Name,
		)
	}
	return int(count), nil
}

// Close never closes the original adapter or its engine-owned transaction.
// The owning table session or strict reader callback controls that lifecycle.
func (*adapterRetainedStableRelationalView) Close() error { return nil }

// adapterStableNetworkTableSession owns one table-stable source view. The
// current relational implementation deliberately exposes a reader limit of
// one because *sql.Tx is tied to one physical connection. Orchestration must
// honor that limit until an engine-specific multi-session snapshot pool is
// composed.
type adapterStableNetworkTableSession struct {
	source      adapterStableNetworkSource
	readerLimit int
	transaction *sql.Tx
	connection  *sql.Conn
	closeFn     func() error

	closeOnce sync.Once
	closeErr  error
}

// adapterStableNetworkTableSessionOpener is an internal composition seam for a
// source that owns an engine-specific stable table lifecycle. Production
// relational and SQLite adapters use the concrete paths below; deterministic
// lifecycle fixtures use this seam to prove orchestration without a database.
type adapterStableNetworkTableSessionOpener interface {
	openStableNetworkTableSource(
		context.Context,
		schema.Table,
	) (*adapterStableNetworkTableSession, error)
}

type adapterSQLTransactionStableView struct {
	transaction *sql.Tx
	engine      string
}

func (view *adapterSQLTransactionStableView) QueryContext(
	ctx context.Context,
	query string,
	arguments ...any,
) (*sql.Rows, error) {
	if view == nil || view.transaction == nil {
		return nil, errors.New("stable source transaction is unavailable")
	}
	return view.transaction.QueryContext(ctx, query, arguments...)
}

func (view *adapterSQLTransactionStableView) QueryRowContext(
	ctx context.Context,
	query string,
	arguments ...any,
) *sql.Row {
	return view.transaction.QueryRowContext(ctx, query, arguments...)
}

func (view *adapterSQLTransactionStableView) retainedStableViewEngine() string {
	if view == nil {
		return ""
	}
	return view.engine
}

// OpenAdapterStableNetworkTableSource opens a production table-scoped stable
// view before pagination or retained-width planning. The caller must keep the
// returned session alive through every page read and close it on every exit.
//
// PostgreSQL and MySQL-family sources use a read-only REPEATABLE READ
// transaction and pin its MVCC snapshot immediately. SQL Server uses a
// SERIALIZABLE transaction and acquires a table-level HOLDLOCK before any
// planning query, so source writes wait until the session closes. SQLite
// reuses the read transaction opened by its source adapter; closing this
// borrowed session does not close the adapter.
func OpenAdapterStableNetworkTableSource(
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
) (*adapterStableNetworkTableSession, error) {
	if ctx == nil {
		return nil, errors.New("stable network source context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNilInterface(source) {
		return nil, errors.New("stable network source adapter is required")
	}
	if table.Name == "" {
		return nil, errors.New("stable network source table is required")
	}
	if opener, ok := source.(adapterStableNetworkTableSessionOpener); ok {
		session, err := opener.openStableNetworkTableSource(ctx, table)
		if err != nil {
			return nil, err
		}
		if session == nil {
			return nil, errors.New(
				"stable network source opener returned no session",
			)
		}
		fail := func(primary error) (
			*adapterStableNetworkTableSession,
			error,
		) {
			if closeErr := session.Close(); closeErr != nil {
				primary = errors.Join(
					primary,
					fmt.Errorf(
						"close invalid stable network source session: %w",
						closeErr,
					),
				)
			}
			return nil, primary
		}
		if _, err := session.Source(); err != nil {
			return fail(err)
		}
		if session.ReaderLimit() < 1 {
			return fail(errors.New(
				"stable network source reader limit is invalid",
			))
		}
		return session, nil
	}
	if sqlite, ok := source.(*sqliteSourceAdapter); ok {
		if sqlite == nil || sqlite.snapshot == nil {
			return nil, errors.New(
				"SQLite stable network source snapshot is unavailable",
			)
		}
		if _, err := adapterPaginationPrimaryKey(
			"sqlite",
			"",
			table,
		); err != nil {
			return nil, err
		}
		return &adapterStableNetworkTableSession{
			source:      sqlite,
			readerLimit: 1,
		}, nil
	}

	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational == nil || relational.database == nil {
		return nil, fmt.Errorf(
			"source engine %q has no table-stable network lifecycle",
			source.Engine(),
		)
	}
	if table.Schema != relational.namespace {
		return nil, fmt.Errorf(
			"stable network source table %s has schema %q, want %q",
			table.Name,
			table.Schema,
			relational.namespace,
		)
	}
	if _, err := adapterPaginationPrimaryKey(
		relational.spec.engine,
		relational.namespace,
		table,
	); err != nil {
		return nil, err
	}

	options := &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	}
	switch relational.spec.engine {
	case "postgres", "mysql":
	case "mssql":
		options = &sql.TxOptions{Isolation: sql.LevelSerializable}
	default:
		return nil, fmt.Errorf(
			"source engine %q has no table-stable network lifecycle",
			relational.spec.engine,
		)
	}
	connection, err := relational.database.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"acquire %s table-stable source connection: %w",
			relational.spec.displayName,
			err,
		)
	}
	// Queries retain the caller's cancellable context, but transaction
	// ownership is explicit. Binding the transaction lifetime to ctx lets
	// database/sql race its automatic rollback with Close; go-mssqldb can
	// then return context.Canceled without releasing HOLDLOCK. The pinned
	// connection was acquired with ctx above, so detaching only the
	// transaction lifetime preserves cancellable admission while ensuring
	// Close can always issue rollback.
	transaction, err := connection.BeginTx(
		context.WithoutCancel(ctx),
		options,
	)
	if err != nil {
		primary := fmt.Errorf(
			"begin %s table-stable source view: %w",
			relational.spec.displayName,
			err,
		)
		if closeErr := connection.Close(); closeErr != nil {
			primary = errors.Join(
				primary,
				fmt.Errorf(
					"release %s table-stable source connection after begin failure: %w",
					relational.spec.displayName,
					closeErr,
				),
			)
		}
		return nil, primary
	}
	fail := func(primary error) (*adapterStableNetworkTableSession, error) {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			primary = errors.Join(
				primary,
				fmt.Errorf(
					"release %s table-stable source view after open failure: %w",
					relational.spec.displayName,
					rollbackErr,
				),
			)
			if discardErr := discardAdapterStableSQLConnection(
				connection,
			); discardErr != nil {
				primary = errors.Join(primary, discardErr)
			}
		}
		if closeErr := connection.Close(); closeErr != nil {
			primary = errors.Join(
				primary,
				fmt.Errorf(
					"release %s table-stable source connection after open failure: %w",
					relational.spec.displayName,
					closeErr,
				),
			)
		}
		return nil, primary
	}

	qualified := relational.spec.qualifiedTable(
		relational.namespace,
		table.Name,
	)
	countExpression := "COUNT(*)"
	lockHint := ""
	if relational.spec.engine == "mssql" {
		countExpression = "COUNT_BIG(*)"
		lockHint = " WITH (TABLOCK, HOLDLOCK)"
	}
	var count int64
	if err := transaction.QueryRowContext(
		ctx,
		"SELECT "+countExpression+" FROM "+qualified+lockHint,
	).Scan(&count); err != nil {
		return fail(fmt.Errorf(
			"pin %s table-stable source view for %s: %w",
			relational.spec.displayName,
			table.Name,
			err,
		))
	}
	if count < 0 {
		return fail(fmt.Errorf(
			"pin %s table-stable source view for %s: negative row count",
			relational.spec.displayName,
			table.Name,
		))
	}

	view, err := newAdapterRetainedStableRelationalView(
		relational,
		&adapterSQLTransactionStableView{
			transaction: transaction,
			engine:      relational.spec.engine,
		},
	)
	if err != nil {
		return fail(err)
	}
	if err := view.bindTableScope(table); err != nil {
		return fail(err)
	}
	return &adapterStableNetworkTableSession{
		source:      view,
		readerLimit: 1,
		transaction: transaction,
		connection:  connection,
	}, nil
}

func (session *adapterStableNetworkTableSession) Source() (
	adapterStableNetworkSource,
	error,
) {
	if session == nil || isNilInterface(session.source) {
		return nil, errors.New("stable network table session is unavailable")
	}
	return session.source, nil
}

func (session *adapterStableNetworkTableSession) ReaderLimit() int {
	if session == nil {
		return 0
	}
	return session.readerLimit
}

func (session *adapterStableNetworkTableSession) Close() error {
	if session == nil {
		return nil
	}
	session.closeOnce.Do(func() {
		if session.closeFn != nil {
			session.closeErr = session.closeFn()
		}
		if session.transaction == nil {
			if session.connection == nil {
				return
			}
		} else {
			err := session.transaction.Rollback()
			if err != nil && !errors.Is(err, sql.ErrTxDone) {
				rollbackErr := fmt.Errorf(
					"close table-stable network source view: %w",
					err,
				)
				if session.closeErr == nil {
					session.closeErr = rollbackErr
				} else {
					session.closeErr = errors.Join(
						session.closeErr,
						rollbackErr,
					)
				}
				if discardErr := discardAdapterStableSQLConnection(
					session.connection,
				); discardErr != nil {
					session.closeErr = errors.Join(
						session.closeErr,
						discardErr,
					)
				}
			}
		}
		if session.connection != nil {
			err := session.connection.Close()
			if err == nil {
				return
			}
			connectionErr := fmt.Errorf(
				"close table-stable network source connection: %w",
				err,
			)
			if session.closeErr == nil {
				session.closeErr = connectionErr
			} else {
				session.closeErr = errors.Join(
					session.closeErr,
					connectionErr,
				)
			}
		}
	})
	return session.closeErr
}

func discardAdapterStableSQLConnection(connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	err := connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if err == nil ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return fmt.Errorf(
		"discard table-stable network source connection: %w",
		err,
	)
}

// RunPostgresAdapterStableNetworkReader binds the existing exported-snapshot
// strict lifecycle directly to the stable network source capability. The
// wrapper cannot escape with a live transaction: RunReader always rolls the
// imported reader back after work returns.
func RunPostgresAdapterStableNetworkReader(
	ctx context.Context,
	session *PostgresStrictConsistencySession,
	task state.TaskKey,
	source sourceAdapter,
	table schema.Table,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil {
		return errors.New("PostgreSQL strict stable session is required")
	}
	if isNilInterface(work) {
		return errors.New(
			"PostgreSQL stable network reader callback is required",
		)
	}
	if task.Schema != table.Schema || task.Table != table.Name {
		return errors.New(
			"PostgreSQL stable network task differs from table catalog",
		)
	}
	return session.RunReader(
		ctx,
		task,
		func(
			readerCtx context.Context,
			queryer PostgresStrictSnapshotQueryer,
		) error {
			view, err := newPostgresAdapterRetainedStableRelationalView(
				source,
				queryer,
			)
			if err != nil {
				return err
			}
			if err := view.bindTableScope(table); err != nil {
				return err
			}
			return work(readerCtx, view)
		},
	)
}
