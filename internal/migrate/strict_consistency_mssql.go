package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// SQLServerStrictConsistencyOpener serves the SQL Server table-scope strict
// contract: one serializable transaction holding a shared table lock, so the
// view cannot change and writers to that table wait.
//
// Unlike PostgreSQL and MySQL, SQL Server's table-scope guarantee is explicitly
// allowed to block writers — Section 10 requires only that the migration scope
// avoid it. Holding the lock is therefore the mechanism, not a side effect, and
// the cost is stated rather than hidden.
type SQLServerStrictConsistencyOpener struct {
	source    *sql.DB
	namespace string
}

type SQLServerStrictConsistencySession struct {
	namespace   string
	runID       string
	epoch       string
	reference   string
	tables      []StrictConsistencyTable
	selected    map[state.TaskKey]struct{}
	database    *sql.DB
	connection  *sql.Conn
	transaction *sql.Tx

	mu        sync.Mutex
	ownerMu   sync.Mutex
	readers   map[*sqlServerStrictReader]struct{}
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	// beforeReaderAdopt is a deterministic lifecycle-test seam. Production
	// leaves it nil; it exercises session Close racing the interval after a
	// reader transaction begins but before ownership is published.
	beforeReaderAdopt func()
}

// SQLServerStrictSnapshotQueryer is the intentionally narrow query surface
// exposed to a strict SQL Server reader. It does not expose the pool or a
// commit operation, so a caller cannot turn an admitted reader into an
// ordinary, unprotected query path.
type SQLServerStrictSnapshotQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// sqlServerStrictReader owns one serializable table-lock reader. Readers take
// their own TABLOCK,HOLDLOCK before they can run work, while the session owner
// remains live for the entire callback. The duplicated shared locks make the
// SQL Server table contract parallel without allowing a pool read to inherit
// strictness merely because an owner happens to exist somewhere else.
type sqlServerStrictReader struct {
	mu sync.Mutex

	connection  *sql.Conn
	transaction *sql.Tx
	cancel      context.CancelFunc
	closing     bool
	callback    bool

	setupDone    chan struct{}
	callbackDone chan struct{}
	closeDone    chan struct{}
	setupOnce    sync.Once
	callbackOnce sync.Once
	closeOnce    sync.Once
	closeErr     error
}

func NewSQLServerStrictConsistencyOpener(
	source *sql.DB,
	namespace string,
) (*SQLServerStrictConsistencyOpener, error) {
	if source == nil {
		return nil, errors.New(
			"SQL Server strict opener requires a source handle",
		)
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("SQL Server strict opener requires a schema")
	}
	return &SQLServerStrictConsistencyOpener{
		source:    source,
		namespace: namespace,
	}, nil
}

func (opener *SQLServerStrictConsistencyOpener) OpenStrictConsistency(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
) (_ StrictConsistencySession, resultErr error) {
	normalized, err := normalizeSQLServerStrictOpenRequest(request)
	if err != nil {
		return nil, err
	}
	connection, err := opener.source.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"acquire SQL Server strict connection: %w",
			err,
		)
	}
	// Setup retains the caller context and deadline. A strict owner that is
	// waiting on an incompatible source lock must stop when the run is
	// cancelled; only the later rollback/Close path is detached and bounded.
	transaction, err := connection.BeginTx(
		ctx,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("begin SQL Server strict view: %w", err),
			connection.Close(),
		)
	}
	session := &SQLServerStrictConsistencySession{
		namespace:   opener.namespace,
		runID:       normalized.RunID,
		epoch:       normalized.ProcessEpoch,
		tables:      normalized.Tables,
		selected:    make(map[state.TaskKey]struct{}, len(normalized.Tables)),
		database:    opener.source,
		connection:  connection,
		transaction: transaction,
		readers:     make(map[*sqlServerStrictReader]struct{}),
		closeDone:   make(chan struct{}),
		reference: sqlServerStrictViewToken(
			normalized.RunID,
			normalized.ProcessEpoch,
		),
	}
	defer func() {
		if resultErr == nil {
			return
		}
		resultErr = errors.Join(resultErr, closeStrictConsistencySession(ctx, session))
	}()

	// Acquiring the shared lock is what makes the view stable. Doing it here,
	// rather than lazily at first read, means a strict route that cannot hold
	// the lock fails before any target work is authorized. COUNT_BIG is used
	// rather than TOP (0): SQL Server may optimize a zero-row query without
	// touching the table, which would not prove that TABLOCK,HOLDLOCK was
	// actually acquired for an empty table.
	for _, table := range normalized.Tables {
		session.selected[table.Task] = struct{}{}
		qualified, err := opener.qualify(table.Task.Table)
		if err != nil {
			return nil, err
		}
		if err := acquireSQLServerStrictTableLock(ctx, transaction, qualified); err != nil {
			return nil, fmt.Errorf(
				"hold SQL Server strict table lock for %q: %w",
				table.Task.Table,
				err,
			)
		}
	}
	return session, nil
}

// OpenPlannedStrictConsistency opens the SQL Server table lock before the
// pagination-derived durable topology exists. Its placeholder attempt IDs are
// deliberately cleared before returning; BeginPlannedStrictConsistency binds
// the final work identity and persists exact same-view count evidence before
// target mutation.
func (opener *SQLServerStrictConsistencyOpener) OpenPlannedStrictConsistency(
	ctx context.Context,
	request PlannedStrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	normalized, err := normalizeSQLServerPlannedOpenRequest(request)
	if err != nil {
		return nil, err
	}
	tables := make([]StrictConsistencyTable, len(normalized.Tasks))
	for index, task := range normalized.Tasks {
		payload := []byte(
			normalized.RunID + "\x00" +
				normalized.ProcessEpoch + "\x00" +
				task.Type + "\x00" +
				task.Schema + "\x00" +
				task.Table + "\x00" +
				task.Partition,
		)
		digest := sha256.Sum256(payload)
		topology := "planned-" + hex.EncodeToString(digest[:])
		attemptID, err := BuildStrictConsistencyAttemptID(task, topology, 0)
		if err != nil {
			return nil, fmt.Errorf(
				"build SQL Server planned strict placeholder for %s.%s: %w",
				task.Schema,
				task.Table,
				err,
			)
		}
		tables[index] = StrictConsistencyTable{
			Task:             task,
			AttemptID:        attemptID,
			WorkTopologyHash: topology,
		}
	}
	raw, err := opener.OpenStrictConsistency(
		ctx,
		StrictConsistencyOpenRequest{
			RunID:        normalized.RunID,
			SourceEngine: normalized.SourceEngine,
			Scope:        normalized.Scope,
			Resume:       normalized.Resume,
			ProcessEpoch: normalized.ProcessEpoch,
			Tables:       tables,
		},
	)
	if err != nil {
		return raw, err
	}
	session, ok := raw.(*SQLServerStrictConsistencySession)
	if !ok || session == nil {
		primary := errors.New(
			"SQL Server planned strict opener returned an unexpected session",
		)
		if !isNilInterface(raw) {
			if cleanupErr := closeStrictConsistencySession(ctx, raw); cleanupErr != nil {
				return nil, errors.Join(
					primary,
					fmt.Errorf(
						"release unexpected SQL Server planned strict session: %w",
						cleanupErr,
					),
				)
			}
		}
		return nil, primary
	}
	session.mu.Lock()
	for index := range session.tables {
		session.tables[index].AttemptID = ""
	}
	session.mu.Unlock()
	return session, nil
}

func normalizeSQLServerPlannedOpenRequest(
	request PlannedStrictConsistencyOpenRequest,
) (PlannedStrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencyMSSQL {
		return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQL Server planned strict opener cannot serve source engine %q",
			request.SourceEngine,
		)
	}
	if request.Scope != state.StrictSnapshotTable {
		return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQL Server table-lock planned strict opener cannot serve scope %q",
			request.Scope,
		)
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server planned strict run ID",
		request.RunID,
	); err != nil {
		return PlannedStrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server planned strict process epoch",
		request.ProcessEpoch,
	); err != nil {
		return PlannedStrictConsistencyOpenRequest{}, err
	}
	if len(request.Tasks) == 0 {
		return PlannedStrictConsistencyOpenRequest{}, errors.New(
			"SQL Server planned strict consistency requires selected tables",
		)
	}
	tasks := append([]state.TaskKey(nil), request.Tasks...)
	seen := make(map[state.TaskKey]struct{}, len(tasks))
	for index, task := range tasks {
		if err := task.Validate(); err != nil {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server planned strict table %d: %w", index, err,
			)
		}
		if task.Type != stage4AdapterNetworkTaskType ||
			task.Schema == "" || task.Partition != "" {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server planned strict table %d requires one unpartitioned %s task with an explicit schema",
				index,
				stage4AdapterNetworkTaskType,
			)
		}
		if _, duplicate := seen[task]; duplicate {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server planned strict table task is duplicated: schema=%q table=%q",
				task.Schema,
				task.Table,
			)
		}
		seen[task] = struct{}{}
	}
	sort.Slice(tasks, func(left, right int) bool {
		return strictConsistencyTaskLess(tasks[left], tasks[right])
	})
	request.Tasks = tasks
	return request, nil
}

func (opener *SQLServerStrictConsistencyOpener) qualify(
	table string,
) (string, error) {
	namespace, err := quoteSQLServerStrictIdentifier(opener.namespace)
	if err != nil {
		return "", err
	}
	name, err := quoteSQLServerStrictIdentifier(table)
	if err != nil {
		return "", err
	}
	return namespace + "." + name, nil
}

type sqlServerStrictTableLockQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// acquireSQLServerStrictTableLock issues a real aggregate read while asking
// SQL Server for a transaction-held table lock. Unlike a TOP (0) probe this
// must execute for both an empty and a non-empty table, so a successful return
// is an auditable proof that the strict source view is lock-bound.
func acquireSQLServerStrictTableLock(
	ctx context.Context,
	queryer sqlServerStrictTableLockQueryer,
	qualified string,
) error {
	if ctx == nil {
		return errors.New("SQL Server strict lock context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if queryer == nil || strings.TrimSpace(qualified) == "" {
		return errors.New("SQL Server strict lock queryer is unavailable")
	}
	var count int64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT_BIG(*) FROM "+qualified+" WITH (TABLOCK, HOLDLOCK)",
	).Scan(&count); err != nil {
		return err
	}
	if count < 0 {
		return errors.New("SQL Server strict lock count is negative")
	}
	return nil
}

func sqlServerStrictViewToken(runID string, epoch string) string {
	digest := sha256.Sum256([]byte(
		"mssql-strict\x00" + runID + "\x00" + epoch,
	))
	return "mssql-view-" + hex.EncodeToString(digest[:16])
}

func (session *SQLServerStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return StrictConsistencyCapture{}, errors.New(
			"SQL Server strict session is closed",
		)
	}
	session.mu.Unlock()
	session.ownerMu.Lock()
	defer session.ownerMu.Unlock()
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	capturedAt := time.Now().UTC()
	captures := make([]StrictConsistencyTableCapture, 0, len(session.tables))
	for _, table := range session.tables {
		namespace, err := quoteSQLServerStrictIdentifier(session.namespace)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		name, err := quoteSQLServerStrictIdentifier(table.Task.Table)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		var count int64
		// COUNT_BIG avoids the silent overflow COUNT would hit past 2^31 rows.
		if err := session.transaction.QueryRowContext(
			ctx,
			"SELECT COUNT_BIG(*) FROM "+namespace+"."+name,
		).Scan(&count); err != nil {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"count SQL Server strict table %q: %w",
				table.Task.Table,
				err,
			)
		}
		captures = append(captures, StrictConsistencyTableCapture{
			Task:                table.Task,
			AttemptID:           table.AttemptID,
			SnapshotReference:   session.reference,
			ExactSourceRowCount: count,
			CapturedAt:          capturedAt,
		})
	}
	return StrictConsistencyCapture{Tables: captures}, nil
}

// RunReader binds a source-read callback to the table-lock owner. A reader
// first takes its own serializable TABLOCK,HOLDLOCK and then proves the owner
// transaction remains live while both locks overlap. This gives every
// parallel range reader a direct, checked strictness proof; ordinary database
// pool reads cannot obtain this queryer.
func (session *SQLServerStrictConsistencySession) RunReader(
	ctx context.Context,
	task state.TaskKey,
	work func(context.Context, SQLServerStrictSnapshotQueryer) error,
) (resultErr error) {
	if ctx == nil {
		return errors.New("SQL Server strict reader context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNilInterface(work) {
		return errors.New("SQL Server strict reader callback is required")
	}
	if session == nil || session.database == nil {
		return errors.New("SQL Server strict reader session is unavailable")
	}

	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return errors.New("SQL Server strict session is closed")
	}
	if _, selected := session.selected[task]; !selected {
		session.mu.Unlock()
		return fmt.Errorf(
			"SQL Server strict reader task is not selected: type=%q schema=%q table=%q partition=%q",
			task.Type,
			task.Schema,
			task.Table,
			task.Partition,
		)
	}
	// The owner transaction deliberately detaches its lifetime from the caller
	// to guarantee cleanup of HOLDLOCK. Reader setup needs the opposite
	// property: it remains cancelable by both the caller and session Close, but
	// its cleanup cannot race a partially published connection/transaction.
	readerCtx, cancelReader := context.WithCancel(context.WithoutCancel(ctx))
	stopCallerCancellation := context.AfterFunc(ctx, cancelReader)
	defer stopCallerCancellation()
	reader := &sqlServerStrictReader{
		cancel:       cancelReader,
		setupDone:    make(chan struct{}),
		callbackDone: make(chan struct{}),
		closeDone:    make(chan struct{}),
	}
	session.readers[reader] = struct{}{}
	session.mu.Unlock()
	defer func() {
		reader.finishSetup()
		reader.rejectCallback()
		// The callback has finished, so release the reader transaction directly.
		// Canceling its BeginTx context first makes database/sql report a
		// self-induced context.Canceled from an otherwise successful rollback.
		cleanupErr := reader.release()
		session.mu.Lock()
		delete(session.readers, reader)
		session.mu.Unlock()
		if cleanupErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("release SQL Server strict reader: %w", cleanupErr),
			)
		}
	}()

	connection, err := session.database.Conn(readerCtx)
	if err != nil {
		return fmt.Errorf("acquire SQL Server strict reader connection: %w", err)
	}
	transaction, err := connection.BeginTx(
		readerCtx,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
	)
	if err != nil {
		_ = connection.Close()
		return fmt.Errorf("begin SQL Server strict reader: %w", err)
	}
	if session.beforeReaderAdopt != nil {
		session.beforeReaderAdopt()
	}
	if err := reader.adopt(connection, transaction); err != nil {
		return err
	}

	qualified, err := session.qualify(task)
	if err != nil {
		return err
	}
	if err := acquireSQLServerStrictTableLock(readerCtx, transaction, qualified); err != nil {
		return fmt.Errorf(
			"hold SQL Server strict reader table lock for %q: %w",
			task.Table,
			err,
		)
	}
	if err := session.requireOwnerLive(readerCtx); err != nil {
		return err
	}
	session.mu.Lock()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		return errors.New("SQL Server strict owner closed during reader setup")
	}
	if !reader.beginCallback() {
		return errors.New("SQL Server strict owner closed during reader setup")
	}
	defer reader.finishCallback()
	reader.finishSetup()
	resultErr = work(readerCtx, transaction)
	if resultErr == nil {
		resultErr = readerCtx.Err()
	}
	return resultErr
}

// RunSQLServerAdapterStableNetworkReader is the only bridge from the
// SQL Server strict session to the Stage 4 retained stable-view API. The
// callback cannot obtain the ordinary source pool; it receives only a reader
// whose lock is bound to a live owner and whose table scope is fixed.
func RunSQLServerAdapterStableNetworkReader(
	ctx context.Context,
	session *SQLServerStrictConsistencySession,
	task state.TaskKey,
	source sourceAdapter,
	table schema.Table,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil {
		return errors.New("SQL Server strict stable session is required")
	}
	if isNilInterface(work) {
		return errors.New("SQL Server strict stable reader callback is required")
	}
	if task.Schema != table.Schema || task.Table != table.Name {
		return errors.New("SQL Server strict stable task differs from table catalog")
	}
	return session.RunReader(
		ctx,
		task,
		func(
			readerCtx context.Context,
			queryer SQLServerStrictSnapshotQueryer,
		) error {
			transaction, ok := queryer.(*sql.Tx)
			if !ok || transaction == nil {
				return errors.New(
					"SQL Server strict reader returned an unexpected queryer",
				)
			}
			view, err := newAdapterRetainedStableRelationalView(
				source,
				&adapterSQLTransactionStableView{
					transaction: transaction,
					engine:      "mssql",
				},
			)
			if err != nil {
				return err
			}
			if err := view.bindTableScope(table); err != nil {
				return err
			}
			view.sqlServerStrict = true
			return work(readerCtx, view)
		},
	)
}

func (session *SQLServerStrictConsistencySession) qualify(
	task state.TaskKey,
) (string, error) {
	if task.Schema != session.namespace {
		return "", fmt.Errorf(
			"SQL Server strict reader task schema %q differs from owner schema %q",
			task.Schema,
			session.namespace,
		)
	}
	namespace, err := quoteSQLServerStrictIdentifier(session.namespace)
	if err != nil {
		return "", err
	}
	table, err := quoteSQLServerStrictIdentifier(task.Table)
	if err != nil {
		return "", err
	}
	return namespace + "." + table, nil
}

func (session *SQLServerStrictConsistencySession) requireOwnerLive(
	ctx context.Context,
) error {
	session.ownerMu.Lock()
	defer session.ownerMu.Unlock()
	session.mu.Lock()
	closed := session.closed
	transaction := session.transaction
	session.mu.Unlock()
	if closed || transaction == nil {
		return errors.New("SQL Server strict lock owner is unavailable")
	}
	var value int
	if err := transaction.QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil {
		return fmt.Errorf(
			"verify SQL Server strict lock owner liveness: %w",
			err,
		)
	}
	if value != 1 {
		return errors.New("SQL Server strict lock owner returned an invalid liveness result")
	}
	return nil
}

func (reader *sqlServerStrictReader) close() error {
	return reader.closeWithCancellation(true)
}

// release closes a reader after its callback has returned normally. Unlike
// Close racing setup or an active callback, this path must not cancel the
// BeginTx context before Rollback: database/sql may otherwise surface that
// self-induced cancellation as a cleanup failure.
func (reader *sqlServerStrictReader) release() error {
	return reader.closeWithCancellation(false)
}

func (reader *sqlServerStrictReader) closeWithCancellation(cancelSetup bool) error {
	if reader == nil {
		return nil
	}
	reader.closeOnce.Do(func() {
		reader.mu.Lock()
		reader.closing = true
		cancel := reader.cancel
		reader.mu.Unlock()
		if cancelSetup && cancel != nil {
			cancel()
		}
		go reader.finishClose()
	})
	<-reader.closeDone
	return reader.closeErr
}

// adopt publishes a complete reader resource pair. Close either observes the
// pair under this mutex or wins first, in which case adopt releases both
// handles itself. A nil/half-published reader can therefore never escape
// cleanup, including when session Close races setup.
func (reader *sqlServerStrictReader) adopt(
	connection *sql.Conn,
	transaction *sql.Tx,
) error {
	if reader == nil || connection == nil || transaction == nil {
		return errors.New("SQL Server strict reader setup returned no transaction")
	}
	reader.mu.Lock()
	if !reader.closing {
		reader.connection = connection
		reader.transaction = transaction
		reader.mu.Unlock()
		return nil
	}
	reader.mu.Unlock()
	var failures []error
	if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		failures = append(failures, err)
	}
	if err := connection.Close(); err != nil {
		failures = append(failures, err)
	}
	if len(failures) != 0 {
		return fmt.Errorf(
			"SQL Server strict reader closed during setup: %w",
			errors.Join(failures...),
		)
	}
	return errors.New("SQL Server strict reader closed during setup")
}

func (reader *sqlServerStrictReader) finishSetup() {
	if reader != nil {
		reader.setupOnce.Do(func() { close(reader.setupDone) })
	}
}

func (reader *sqlServerStrictReader) beginCallback() bool {
	if reader == nil {
		return false
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closing {
		return false
	}
	reader.callback = true
	return true
}

func (reader *sqlServerStrictReader) finishCallback() {
	if reader != nil {
		reader.callbackOnce.Do(func() { close(reader.callbackDone) })
	}
}

func (reader *sqlServerStrictReader) rejectCallback() {
	if reader == nil {
		return
	}
	reader.mu.Lock()
	callback := reader.callback
	reader.mu.Unlock()
	if !callback {
		reader.finishCallback()
	}
}

func (reader *sqlServerStrictReader) finishClose() {
	<-reader.setupDone
	reader.rejectCallback()
	<-reader.callbackDone
	reader.mu.Lock()
	transaction := reader.transaction
	connection := reader.connection
	reader.transaction = nil
	reader.connection = nil
	reader.mu.Unlock()
	var failures []error
	if transaction != nil {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, err)
		}
	}
	if connection != nil {
		if err := connection.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		reader.closeErr = errors.Join(failures...)
	}
	close(reader.closeDone)
}

func (session *SQLServerStrictConsistencySession) Close(
	ctx context.Context,
) error {
	if session == nil {
		return errors.New("SQL Server strict session is nil")
	}
	if ctx == nil {
		return errors.New("SQL Server strict cleanup context is required")
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		readers := make([]*sqlServerStrictReader, 0, len(session.readers))
		for reader := range session.readers {
			readers = append(readers, reader)
		}
		session.readers = make(map[*sqlServerStrictReader]struct{})
		transaction := session.transaction
		connection := session.connection
		session.mu.Unlock()

		go func() {
			var failures []error
			for _, reader := range readers {
				if err := reader.close(); err != nil {
					failures = append(failures, err)
				}
			}
			session.ownerMu.Lock()
			if transaction != nil {
				if err := transaction.Rollback(); err != nil &&
					!errors.Is(err, sql.ErrTxDone) {
					failures = append(failures, err)
				}
			}
			session.ownerMu.Unlock()
			if connection != nil {
				if err := connection.Close(); err != nil {
					failures = append(failures, err)
				}
			}
			if len(failures) != 0 {
				session.closeErr = fmt.Errorf(
					"close SQL Server strict session: %w",
					errors.Join(failures...),
				)
			}
			close(session.closeDone)
		}()
	})
	select {
	case <-session.closeDone:
		return session.closeErr
	case <-ctx.Done():
		return fmt.Errorf(
			"SQL Server strict cleanup did not finish before context ended: %w",
			context.Cause(ctx),
		)
	}
}

func normalizeSQLServerStrictOpenRequest(
	request StrictConsistencyOpenRequest,
) (StrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencyMSSQL {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQL Server strict opener cannot serve source engine %q",
			request.SourceEngine,
		)
	}
	// Migration scope requires a durable database snapshot, which this opener
	// does not create. Refusing it is honest; the core admits the scope for SQL
	// Server, so silently serving a table lock instead would be a false claim.
	if request.Scope != state.StrictSnapshotTable {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQL Server table-scope strict opener cannot serve scope %q",
			request.Scope,
		)
	}
	if request.RequiredMigrationSnapshot != nil {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQL Server table-scope strict consistency has no durable migration snapshot to reuse",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server strict run ID",
		request.RunID,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server strict process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQL Server strict consistency requires selected tables",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, selected := range tables {
		if err := selected.Task.Validate(); err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server strict table %d: %w",
				index,
				err,
			)
		}
		if selected.Task.Partition != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server strict table %d must be unpartitioned",
				index,
			)
		}
		if _, duplicate := seen[selected.Task]; duplicate {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server strict table task is duplicated: %q",
				selected.Task.Table,
			)
		}
		seen[selected.Task] = struct{}{}
	}
	request.Tables = tables
	return request, nil
}

func quoteSQLServerStrictIdentifier(name string) (string, error) {
	if name == "" {
		return "", errors.New("SQL Server strict identifier is required")
	}
	for _, symbol := range name {
		if symbol == 0 || symbol == ']' {
			return "", fmt.Errorf(
				"SQL Server strict identifier %q contains an unsupported character",
				name,
			)
		}
	}
	return "[" + name + "]", nil
}

var _ StrictConsistencyOpener = (*SQLServerStrictConsistencyOpener)(nil)
var _ PlannedStrictConsistencyOpener = (*SQLServerStrictConsistencyOpener)(nil)
var _ StrictConsistencySession = (*SQLServerStrictConsistencySession)(nil)
