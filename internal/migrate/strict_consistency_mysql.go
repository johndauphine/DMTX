package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// MySQLStrictConsistencyOpener serves the MySQL and MariaDB strict contract:
// InnoDB repeatable-read sessions whose snapshots are pinned at one instant by
// a brief LOCK TABLES ... READ held on a separate connection.
//
// MySQL has no exportable snapshot, so agreement between readers is created by
// timing rather than by a handle: while the lock holder blocks writers, each
// reader issues START TRANSACTION WITH CONSISTENT SNAPSHOT, and every snapshot
// therefore observes the same committed state. The lock is released as soon as
// the readers exist, which is what keeps the write outage brief.
type MySQLStrictConsistencyOpener struct {
	source    *sql.DB
	namespace string
	engine    StrictConsistencyEngine
	readers   int
}

// MySQLStrictConsistencySession owns the reader transactions. The lock holder
// is released during Open and is deliberately not retained.
type MySQLStrictConsistencySession struct {
	namespace   string
	runID       string
	epoch       string
	gtid        string
	tables      []StrictConsistencyTable
	connections []*sql.Conn

	mu        sync.Mutex
	available chan *sql.Conn
	active    sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewMySQLStrictConsistencyOpener(
	source *sql.DB,
	namespace string,
	engine StrictConsistencyEngine,
	readers int,
) (*MySQLStrictConsistencyOpener, error) {
	if source == nil {
		return nil, errors.New("MySQL strict opener requires a source handle")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("MySQL strict opener requires a schema")
	}
	if engine != StrictConsistencyMySQL &&
		engine != StrictConsistencyMariaDB {
		return nil, fmt.Errorf(
			"MySQL strict opener cannot serve engine %q",
			engine,
		)
	}
	if readers < 1 {
		return nil, errors.New("MySQL strict opener requires at least one reader")
	}
	return &MySQLStrictConsistencyOpener{
		source:    source,
		namespace: namespace,
		engine:    engine,
		readers:   readers,
	}, nil
}

// Preflight verifies the source-only strict prerequisites before Stage 4
// checkpoints any table-set state. OpenStrictConsistency repeats the checks
// immediately before the lock window because grants or table engines can
// change between admission and execution.
func (opener *MySQLStrictConsistencyOpener) Preflight(
	ctx context.Context,
	tables []StrictConsistencyTable,
) error {
	if opener == nil {
		return errors.New("MySQL strict opener is unavailable")
	}
	if len(tables) == 0 {
		return errors.New("MySQL strict preflight requires selected tables")
	}
	if ctx == nil {
		return errors.New("MySQL strict preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := opener.verifyInnoDBTables(ctx, tables); err != nil {
		return err
	}
	// SHOW GRANTS is not scope-exact: a syntactically valid LOCK TABLES grant
	// may apply only to another schema. Prove the actual selected relation lock
	// before a durable checkpoint, then OpenStrictConsistency repeats it for
	// the real retained view.
	probeCtx, cancel := context.WithTimeout(ctx, strictConsistencyCleanupTimeout)
	defer cancel()
	holder, err := opener.source.Conn(probeCtx)
	if err != nil {
		return fmt.Errorf("acquire MySQL strict preflight lock holder: %w", err)
	}
	statement, err := opener.lockStatement(tables)
	if err != nil {
		_ = holder.Close()
		return err
	}
	if _, err := holder.ExecContext(probeCtx, statement); err != nil {
		_ = holder.Close()
		return NewTransferError(ErrorClassPolicy, fmt.Errorf("prove MySQL strict LOCK TABLES capability: %w", err))
	}
	cleanupCtx, cleanupCancel := mysqlStrictCleanupContext(probeCtx)
	defer cleanupCancel()
	_, unlockErr := holder.ExecContext(cleanupCtx, "UNLOCK TABLES")
	var discardErr error
	if unlockErr != nil {
		discardErr = discardMySQLStrictConnection(holder)
	}
	closeErr := holder.Close()
	if unlockErr != nil || discardErr != nil || closeErr != nil {
		return errors.Join(unlockErr, discardErr, closeErr)
	}
	return nil
}

func (opener *MySQLStrictConsistencyOpener) OpenStrictConsistency(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
) (_ StrictConsistencySession, resultErr error) {
	normalized, err := normalizeMySQLStrictOpenRequest(request, opener.engine)
	if err != nil {
		return nil, err
	}
	if err := opener.verifyInnoDBTables(ctx, normalized.Tables); err != nil {
		return nil, err
	}
	// The lock holder must be a distinct connection: LOCK TABLES implicitly
	// commits, so the session holding the lock can never be one of the readers.
	holder, err := opener.source.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire MySQL strict lock holder: %w", err)
	}
	lockStatement, err := opener.lockStatement(normalized.Tables)
	if err != nil {
		_ = holder.Close()
		return nil, err
	}
	if _, err := holder.ExecContext(ctx, lockStatement); err != nil {
		_ = holder.Close()
		return nil, fmt.Errorf("hold MySQL strict read lock: %w", err)
	}
	// The lock is released on every path below, including failure, so a
	// half-opened strict session can never leave the source blocked.
	releaseHolder := func() error {
		cleanupCtx, cancel := mysqlStrictCleanupContext(ctx)
		defer cancel()
		_, unlockErr := holder.ExecContext(
			cleanupCtx,
			"UNLOCK TABLES",
		)
		var discardErr error
		if unlockErr != nil {
			discardErr = discardMySQLStrictConnection(holder)
		}
		closeErr := holder.Close()
		return errors.Join(unlockErr, discardErr, closeErr)
	}

	session := &MySQLStrictConsistencySession{
		namespace: opener.namespace,
		runID:     normalized.RunID,
		epoch:     normalized.ProcessEpoch,
		tables:    normalized.Tables,
		available: make(chan *sql.Conn, opener.readers),
		closeDone: make(chan struct{}),
	}
	defer func() {
		if resultErr == nil {
			return
		}
		resultErr = errors.Join(
			resultErr,
			closeStrictConsistencySession(ctx, session),
		)
	}()

	for index := 0; index < opener.readers; index++ {
		connection, err := opener.source.Conn(ctx)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("acquire MySQL strict reader %d: %w", index, err),
				releaseHolder(),
			)
		}
		session.connections = append(session.connections, connection)
		if _, err := connection.ExecContext(
			ctx,
			"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		); err != nil {
			return nil, errors.Join(
				fmt.Errorf("set MySQL strict reader %d isolation: %w", index, err),
				releaseHolder(),
			)
		}
		// database/sql cannot express WITH CONSISTENT SNAPSHOT, so the
		// transaction is started by statement and tracked explicitly.
		if _, err := connection.ExecContext(
			ctx,
			"START TRANSACTION WITH CONSISTENT SNAPSHOT",
		); err != nil {
			return nil, errors.Join(
				fmt.Errorf("open MySQL strict reader %d snapshot: %w", index, err),
				releaseHolder(),
			)
		}
	}
	for _, connection := range session.connections {
		session.available <- connection
	}
	if err := releaseHolder(); err != nil {
		return nil, fmt.Errorf("release MySQL strict read lock: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate MySQL strict view identity: %w", err)
	}
	session.gtid = mysqlStrictViewToken(normalized.RunID, normalized.ProcessEpoch, hex.EncodeToString(nonce[:]))
	return session, nil
}

func mysqlStrictCleanupContext(caller context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if caller != nil {
		base = context.WithoutCancel(caller)
	}
	now := time.Now()
	deadline := now.Add(strictConsistencyCleanupTimeout)
	if caller != nil {
		if callerDeadline, ok := caller.Deadline(); ok && callerDeadline.After(now) && callerDeadline.Before(deadline) {
			deadline = callerDeadline
		}
	}
	return context.WithDeadline(base, deadline)
}

// OpenPlannedStrictConsistency makes the pre-existing MySQL/MariaDB strict
// view available while pagination derives its durable topology. Placeholder
// attempt IDs are cleared before return; the core binds the final identities
// only after work and exact same-view evidence are durably saved.
func (opener *MySQLStrictConsistencyOpener) OpenPlannedStrictConsistency(ctx context.Context, request PlannedStrictConsistencyOpenRequest) (StrictConsistencySession, error) {
	if request.SourceEngine != StrictConsistencyMySQL && request.SourceEngine != StrictConsistencyMariaDB {
		return nil, fmt.Errorf("MySQL planned strict opener cannot serve source engine %q", request.SourceEngine)
	}
	if request.SourceEngine != opener.engine {
		return nil, fmt.Errorf("MySQL planned strict opener for %q cannot serve source engine %q", opener.engine, request.SourceEngine)
	}
	if request.Scope != state.StrictSnapshotTable {
		return nil, fmt.Errorf("MySQL planned strict opener cannot serve scope %q", request.Scope)
	}
	if request.RequiredMigrationSnapshot != nil || request.ReopenUnownedMigrationSnapshot {
		return nil, errors.New("MySQL planned strict opener cannot reopen a migration snapshot")
	}
	if err := validateCredentialFreeIdentifier("MySQL planned strict run ID", request.RunID); err != nil {
		return nil, err
	}
	if err := validateCredentialFreeIdentifier("MySQL planned strict process epoch", request.ProcessEpoch); err != nil {
		return nil, err
	}
	if len(request.Tasks) == 0 {
		return nil, errors.New("MySQL planned strict consistency requires selected tables")
	}
	tables := make([]StrictConsistencyTable, len(request.Tasks))
	seen := make(map[state.TaskKey]struct{}, len(request.Tasks))
	for index, task := range request.Tasks {
		if err := task.Validate(); err != nil {
			return nil, fmt.Errorf("MySQL planned strict table %d: %w", index, err)
		}
		if task.Type != stage4AdapterNetworkTaskType || task.Schema == "" || task.Partition != "" {
			return nil, fmt.Errorf("MySQL planned strict table %d requires one unpartitioned %s task with an explicit schema", index, stage4AdapterNetworkTaskType)
		}
		if _, duplicate := seen[task]; duplicate {
			return nil, fmt.Errorf("MySQL planned strict table task is duplicated: %q", task.Table)
		}
		seen[task] = struct{}{}
		payload := []byte(request.RunID + "\x00" + request.ProcessEpoch + "\x00" + task.Type + "\x00" + task.Schema + "\x00" + task.Table)
		digest := sha256.Sum256(payload)
		topology := "planned-" + hex.EncodeToString(digest[:])
		attempt, err := BuildStrictConsistencyAttemptID(task, topology, 0)
		if err != nil {
			return nil, fmt.Errorf("build MySQL planned strict placeholder for %s.%s: %w", task.Schema, task.Table, err)
		}
		tables[index] = StrictConsistencyTable{Task: task, AttemptID: attempt, WorkTopologyHash: topology}
	}
	raw, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{RunID: request.RunID, SourceEngine: request.SourceEngine, Scope: request.Scope, Resume: request.Resume, ProcessEpoch: request.ProcessEpoch, Tables: tables})
	if err != nil {
		return raw, err
	}
	session, ok := raw.(*MySQLStrictConsistencySession)
	if !ok || session == nil {
		return nil, errors.New("MySQL planned strict opener returned an unexpected session")
	}
	session.mu.Lock()
	for index := range session.tables {
		session.tables[index].AttemptID = ""
	}
	session.mu.Unlock()
	return session, nil
}

func (opener *MySQLStrictConsistencyOpener) lockStatement(
	tables []StrictConsistencyTable,
) (string, error) {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		quoted, err := quoteMySQLStrictIdentifier(table.Task.Table)
		if err != nil {
			return "", err
		}
		namespace, err := quoteMySQLStrictIdentifier(opener.namespace)
		if err != nil {
			return "", err
		}
		names = append(names, namespace+"."+quoted+" READ")
	}
	return "LOCK TABLES " + strings.Join(names, ", "), nil
}

// verifyInnoDBTables refuses a non-transactional storage engine outright. MyISAM
// would accept the lock and the transaction statements without providing any
// snapshot, so the run would look strict while reading whatever it liked.
func (opener *MySQLStrictConsistencyOpener) verifyInnoDBTables(
	ctx context.Context,
	tables []StrictConsistencyTable,
) error {
	for _, table := range tables {
		var storage string
		if err := opener.source.QueryRowContext(
			ctx,
			`SELECT ENGINE FROM information_schema.TABLES
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			opener.namespace,
			table.Task.Table,
		).Scan(&storage); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf(
					"MySQL strict table %q was not found in schema %q",
					table.Task.Table,
					opener.namespace,
				)
			}
			return fmt.Errorf(
				"verify MySQL strict storage engine for %q: %w",
				table.Task.Table,
				err,
			)
		}
		if !strings.EqualFold(storage, "InnoDB") {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"MySQL strict consistency requires InnoDB; table %q uses %q",
					table.Task.Table,
					storage,
				),
			)
		}
	}
	return nil
}

// verifyLockPrivilege proves the grant before the migration commits to a strict
// route, so a privilege failure surfaces as policy rather than as a mid-run
// error after the target has been touched.
func (opener *MySQLStrictConsistencyOpener) verifyLockPrivilege(
	ctx context.Context,
) error {
	rows, err := opener.source.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return fmt.Errorf("read MySQL strict grants: %w", err)
	}
	defer rows.Close()
	granted := false
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("scan MySQL strict grant: %w", err)
		}
		upper := strings.ToUpper(grant)
		if strings.Contains(upper, "ALL PRIVILEGES") ||
			strings.Contains(upper, "LOCK TABLES") {
			granted = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate MySQL strict grants: %w", err)
	}
	if !granted {
		return NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"MySQL strict consistency requires the LOCK TABLES privilege",
			),
		)
	}
	return nil
}

func mysqlStrictViewToken(runID string, epoch string, nonce string) string {
	digest := sha256.Sum256([]byte(
		"mysql-strict\x00" + runID + "\x00" + epoch + "\x00" + nonce,
	))
	return "mysql-view-" + hex.EncodeToString(digest[:16])
}

func (session *MySQLStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	capturedAt := time.Now().UTC()
	captures := make([]StrictConsistencyTableCapture, 0, len(session.tables))
	for _, table := range session.tables {
		namespace, err := quoteMySQLStrictIdentifier(session.namespace)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		name, err := quoteMySQLStrictIdentifier(table.Task.Table)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		var count int64
		// Check every opened reader. This is especially important for a
		// one-table run: rotating by table alone would never exercise the
		// secondary sessions later used for parallel page reads.
		for readerIndex := 0; readerIndex < len(session.connections); readerIndex++ {
			reader, release, err := session.borrowReader(ctx)
			if err != nil {
				return StrictConsistencyCapture{}, err
			}
			var observed int64
			err = reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+namespace+"."+name).Scan(&observed)
			release()
			if err != nil {
				return StrictConsistencyCapture{}, fmt.Errorf("count MySQL strict table %q: %w", table.Task.Table, err)
			}
			if readerIndex == 0 {
				count = observed
			} else if observed != count {
				return StrictConsistencyCapture{}, NewTransferError(ErrorClassState, fmt.Errorf("MySQL strict reader snapshots disagree for table %q: %d versus %d", table.Task.Table, count, observed))
			}
		}
		captures = append(captures, StrictConsistencyTableCapture{
			Task:                table.Task,
			AttemptID:           table.AttemptID,
			SnapshotReference:   session.gtid,
			ExactSourceRowCount: count,
			CapturedAt:          capturedAt,
		})
	}
	return StrictConsistencyCapture{Tables: captures}, nil
}

func (session *MySQLStrictConsistencySession) Close(
	ctx context.Context,
) error {
	if session == nil {
		return errors.New("MySQL strict session is unavailable")
	}
	if ctx == nil {
		return errors.New("MySQL strict cleanup context is required")
	}
	if session.closeDone == nil {
		return errors.New("MySQL strict cleanup state is unavailable")
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		connections := append([]*sql.Conn(nil), session.connections...)
		session.mu.Unlock()
		go func() {
			// A callback retains a leased connection. Waiting happens off the
			// caller path; callers keep their supplied cleanup deadline while no
			// ROLLBACK can race that callback.
			session.active.Wait()
			cleanupCtx, cancel := mysqlStrictCleanupContext(ctx)
			defer cancel()
			var failures []error
			for _, connection := range connections {
				if _, err := connection.ExecContext(cleanupCtx, "ROLLBACK"); err != nil {
					failures = append(failures, err)
					if discardErr := discardMySQLStrictConnection(connection); discardErr != nil {
						failures = append(failures, discardErr)
					}
				}
				if err := connection.Close(); err != nil {
					failures = append(failures, err)
				}
			}
			if len(failures) != 0 {
				session.closeErr = fmt.Errorf("close MySQL strict session: %w", errors.Join(failures...))
			}
			close(session.closeDone)
		}()
	})
	select {
	case <-session.closeDone:
		return session.closeErr
	case <-ctx.Done():
		return fmt.Errorf("MySQL strict snapshot cleanup did not finish before context ended; rollback may still be in progress: %w", context.Cause(ctx))
	}
}

// A manually issued START TRANSACTION is invisible to database/sql. If its
// ROLLBACK failed, Close alone could return a live transaction connection to
// the pool; force the driver to discard it before releasing the handle.
func discardMySQLStrictConnection(connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	err := connection.Raw(func(any) error { return driver.ErrBadConn })
	if err == nil || errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return fmt.Errorf("discard MySQL strict reader connection: %w", err)
}

// MySQLStrictSnapshotQueryer is deliberately narrower than the source pool:
// it represents exactly one transaction started during the LOCK TABLES window.
type MySQLStrictSnapshotQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (session *MySQLStrictConsistencySession) borrowReader(
	ctx context.Context,
) (*sql.Conn, func(), error) {
	if session == nil {
		return nil, nil, errors.New("MySQL strict session is unavailable")
	}
	if ctx == nil {
		return nil, nil, errors.New("MySQL strict reader context is required")
	}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil, nil, errors.New("MySQL strict session is closed")
	}
	session.active.Add(1)
	session.mu.Unlock()
	select {
	case <-ctx.Done():
		session.active.Done()
		return nil, nil, ctx.Err()
	case connection := <-session.available:
		return connection, func() {
			session.available <- connection
			session.active.Done()
		}, nil
	}
}

// RunReader leases one of the transactions whose consistent snapshots were
// opened while LOCK TABLES was held. A physical MySQL connection cannot run
// concurrent statements, so callers may only parallelize up to the number of
// independently opened leases.
func (session *MySQLStrictConsistencySession) RunReader(
	ctx context.Context,
	task state.TaskKey,
	work func(context.Context, MySQLStrictSnapshotQueryer) error,
) error {
	if isNilInterface(work) {
		return errors.New("MySQL strict reader callback is required")
	}
	if err := task.Validate(); err != nil {
		return fmt.Errorf("MySQL strict reader task: %w", err)
	}
	found := false
	for _, selected := range session.tables {
		if selected.Task == task {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("MySQL strict reader task is not selected: type=%q schema=%q table=%q partition=%q", task.Type, task.Schema, task.Table, task.Partition)
	}
	connection, release, err := session.borrowReader(ctx)
	if err != nil {
		return err
	}
	defer release()
	return work(ctx, connection)
}

// RunMySQLAdapterStableNetworkReader is the only bridge from a MySQL/MariaDB
// strict transaction to the retained stable-view capability used by Stage 4.
func RunMySQLAdapterStableNetworkReader(
	ctx context.Context,
	session *MySQLStrictConsistencySession,
	task state.TaskKey,
	source sourceAdapter,
	table schema.Table,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil {
		return errors.New("MySQL strict stable session is required")
	}
	if isNilInterface(work) {
		return errors.New("MySQL strict stable reader callback is required")
	}
	if task.Schema != table.Schema || task.Table != table.Name {
		return errors.New("MySQL strict stable task differs from table catalog")
	}
	return session.RunReader(ctx, task, func(readerCtx context.Context, queryer MySQLStrictSnapshotQueryer) error {
		connection, ok := queryer.(*sql.Conn)
		if !ok || connection == nil {
			return errors.New("MySQL strict reader returned an unexpected queryer")
		}
		view, err := newAdapterRetainedStableRelationalView(source, &mySQLAdapterStrictStableView{connection: connection})
		if err != nil {
			return err
		}
		if err := view.bindTableScope(table); err != nil {
			return err
		}
		return work(readerCtx, view)
	})
}

type mySQLAdapterStrictStableView struct{ connection *sql.Conn }

func (view *mySQLAdapterStrictStableView) QueryContext(ctx context.Context, query string, arguments ...any) (*sql.Rows, error) {
	if view == nil || view.connection == nil {
		return nil, errors.New("MySQL strict stable view is unavailable")
	}
	return view.connection.QueryContext(ctx, query, arguments...)
}
func (view *mySQLAdapterStrictStableView) QueryRowContext(ctx context.Context, query string, arguments ...any) *sql.Row {
	return view.connection.QueryRowContext(ctx, query, arguments...)
}
func (*mySQLAdapterStrictStableView) retainedStableViewEngine() string { return "mysql" }

func normalizeMySQLStrictOpenRequest(
	request StrictConsistencyOpenRequest,
	engine StrictConsistencyEngine,
) (StrictConsistencyOpenRequest, error) {
	if request.SourceEngine != engine {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"MySQL strict opener for %q cannot serve source engine %q",
			engine,
			request.SourceEngine,
		)
	}
	// Migration scope needs one view across independently planned work. MySQL
	// can only pin agreement at an instant, not hand that instant to a session
	// opened later, so the core already refuses this and so does the opener.
	if request.Scope != state.StrictSnapshotTable {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"MySQL strict consistency supports table scope only, not %q",
			request.Scope,
		)
	}
	if request.RequiredMigrationSnapshot != nil {
		return StrictConsistencyOpenRequest{}, errors.New(
			"MySQL cannot reuse a durable migration snapshot",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"MySQL strict run ID",
		request.RunID,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"MySQL strict process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyOpenRequest{}, errors.New(
			"MySQL strict consistency requires selected tables",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, selected := range tables {
		if err := selected.Task.Validate(); err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"MySQL strict table %d: %w",
				index,
				err,
			)
		}
		if selected.Task.Partition != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"MySQL strict table %d must be unpartitioned",
				index,
			)
		}
		if _, duplicate := seen[selected.Task]; duplicate {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"MySQL strict table task is duplicated: %q",
				selected.Task.Table,
			)
		}
		seen[selected.Task] = struct{}{}
	}
	request.Tables = tables
	return request, nil
}

// quoteMySQLStrictIdentifier refuses what it cannot quote rather than escaping
// aggressively: a strict count against the wrong relation is worse than a
// refused migration.
func quoteMySQLStrictIdentifier(name string) (string, error) {
	if name == "" {
		return "", errors.New("MySQL strict identifier is required")
	}
	for _, symbol := range name {
		if symbol == 0 || symbol == '`' {
			return "", fmt.Errorf(
				"MySQL strict identifier %q contains an unsupported character",
				name,
			)
		}
	}
	return "`" + name + "`", nil
}

var _ StrictConsistencyOpener = (*MySQLStrictConsistencyOpener)(nil)
var _ PlannedStrictConsistencyOpener = (*MySQLStrictConsistencyOpener)(nil)
var _ StrictConsistencySession = (*MySQLStrictConsistencySession)(nil)
