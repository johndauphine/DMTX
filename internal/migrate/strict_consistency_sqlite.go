package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// SQLiteStrictConsistencyOpener serves the SQLite strict contract: one
// serializable read transaction and no parallel source readers.
//
// SQLite has no exported snapshot handle to hand another connection, so the
// stable view is the read transaction itself and cannot be shared. That is why
// only table scope is offered and why the session refuses concurrent readers —
// a second reader would be a second view, which is exactly the guarantee strict
// consistency exists to deny.
type SQLiteStrictConsistencyOpener struct {
	source *sql.DB
}

// SQLiteStrictConsistencySession owns one read transaction for the whole
// strict execution. Every count and every row read must pass through it.
type SQLiteStrictConsistencySession struct {
	transaction *sql.Tx
	connection  *sql.Conn
	runID       string
	epoch       string
	dataVersion int64
	viewID      string
	tables      []StrictConsistencyTable

	mu         sync.Mutex
	readerBusy bool
	readerDone chan struct{}
	closed     bool

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewSQLiteStrictConsistencyOpener(
	source *sql.DB,
) (*SQLiteStrictConsistencyOpener, error) {
	if source == nil {
		return nil, errors.New("SQLite strict opener requires a source handle")
	}
	return &SQLiteStrictConsistencyOpener{source: source}, nil
}

func (opener *SQLiteStrictConsistencyOpener) OpenStrictConsistency(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	normalized, err := normalizeSQLiteStrictOpenRequest(request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connection, err := opener.source.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire SQLite strict source connection: %w", err)
	}
	// A read-only transaction is the stable view. SQLite promises a consistent
	// read for its lifetime, so it must be opened before any count and held
	// until the execution finishes. Its ownership is deliberately detached
	// from ctx: database/sql otherwise races an automatic rollback with Close
	// as soon as a caller cancels. The callback keeps the original cancellable
	// context, while this session owns the eventual rollback and connection
	// release.
	transaction, err := connection.BeginTx(context.WithoutCancel(ctx), &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		closeErr := connection.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release SQLite strict source connection after begin failure: %w", closeErr))
		}
		return nil, fmt.Errorf("open SQLite strict read transaction: %w", err)
	}
	// data_version changes when another connection commits. Reading it inside
	// the transaction pins a value that identifies this view, which gives the
	// core a real snapshot reference rather than an invented token.
	var dataVersion int64
	if err := transaction.QueryRowContext(
		ctx,
		"PRAGMA data_version",
	).Scan(&dataVersion); err != nil {
		return nil, closeSQLiteStrictOpenFailure(
			fmt.Errorf("read SQLite strict data version: %w", err),
			transaction,
			connection,
		)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, closeSQLiteStrictOpenFailure(
			fmt.Errorf("generate SQLite strict view identity: %w", err),
			transaction,
			connection,
		)
	}
	return &SQLiteStrictConsistencySession{
		transaction: transaction,
		connection:  connection,
		runID:       normalized.RunID,
		epoch:       normalized.ProcessEpoch,
		dataVersion: dataVersion,
		viewID:      hex.EncodeToString(nonce[:]),
		tables:      normalized.Tables,
		closeDone:   make(chan struct{}),
	}, nil
}

// Preflight proves that the production SQLite source can acquire and pin a
// read transaction before Stage 4 records any durable table-set checkpoint.
// OpenStrictConsistency repeats the acquisition immediately before each
// table's planning view, so a changed source or connection is never trusted
// merely because this short admission probe passed.
func (opener *SQLiteStrictConsistencyOpener) Preflight(
	ctx context.Context,
) error {
	if opener == nil || opener.source == nil {
		return errors.New("SQLite strict opener is unavailable")
	}
	if ctx == nil {
		return errors.New("SQLite strict preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	connection, err := opener.source.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire SQLite strict preflight connection: %w", err)
	}
	transaction, err := connection.BeginTx(context.WithoutCancel(ctx), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		closeErr := connection.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release SQLite strict preflight connection after begin failure: %w", closeErr))
		}
		return fmt.Errorf("begin SQLite strict preflight read transaction: %w", err)
	}
	var dataVersion int64
	if err := transaction.QueryRowContext(ctx, "PRAGMA data_version").Scan(&dataVersion); err != nil {
		return closeSQLiteStrictOpenFailure(
			fmt.Errorf("establish SQLite strict preflight view: %w", err),
			transaction,
			connection,
		)
	}
	return closeSQLiteStrictOpenFailure(nil, transaction, connection)
}

func closeSQLiteStrictOpenFailure(
	primary error,
	transaction *sql.Tx,
	connection *sql.Conn,
) error {
	if transaction != nil {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			primary = errors.Join(primary, fmt.Errorf("release SQLite strict read transaction: %w", err))
		}
	}
	if connection != nil {
		if err := connection.Close(); err != nil {
			primary = errors.Join(primary, fmt.Errorf("release SQLite strict source connection: %w", err))
		}
	}
	return primary
}

// OpenPlannedStrictConsistency opens the SQLite read view before the exact
// range topology exists. It intentionally clears the temporary attempts: the
// strict core binds real work only after pagination through this same view is
// checkpointed.
func (opener *SQLiteStrictConsistencyOpener) OpenPlannedStrictConsistency(
	ctx context.Context,
	request PlannedStrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	normalized, err := normalizeSQLitePlannedOpenRequest(request)
	if err != nil {
		return nil, err
	}
	tables := make([]StrictConsistencyTable, len(normalized.Tasks))
	for index, task := range normalized.Tasks {
		payload := []byte(normalized.RunID + "\x00" + normalized.ProcessEpoch + "\x00" + task.Type + "\x00" + task.Schema + "\x00" + task.Table + "\x00" + task.Partition)
		digest := sha256.Sum256(payload)
		topology := "planned-" + hex.EncodeToString(digest[:])
		attempt, err := BuildStrictConsistencyAttemptID(task, topology, 0)
		if err != nil {
			return nil, fmt.Errorf("build SQLite planned strict placeholder for %s: %w", task.Table, err)
		}
		tables[index] = StrictConsistencyTable{Task: task, AttemptID: attempt, WorkTopologyHash: topology}
	}
	raw, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        normalized.RunID,
		SourceEngine: normalized.SourceEngine,
		Scope:        normalized.Scope,
		Resume:       normalized.Resume,
		ProcessEpoch: normalized.ProcessEpoch,
		Tables:       tables,
	})
	if err != nil {
		return raw, err
	}
	session, ok := raw.(*SQLiteStrictConsistencySession)
	if !ok || session == nil {
		primary := errors.New("SQLite planned strict opener returned an unexpected session")
		if !isNilInterface(raw) {
			if cleanupErr := closeStrictConsistencySession(ctx, raw); cleanupErr != nil {
				primary = errors.Join(primary, fmt.Errorf("release unexpected SQLite planned strict session: %w", cleanupErr))
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

func normalizeSQLitePlannedOpenRequest(
	request PlannedStrictConsistencyOpenRequest,
) (PlannedStrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencySQLite {
		return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf("SQLite planned strict opener cannot serve source engine %q", request.SourceEngine)
	}
	if request.Scope != state.StrictSnapshotTable {
		return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf("SQLite planned strict consistency supports table scope only, not %q", request.Scope)
	}
	if request.RequiredMigrationSnapshot != nil || request.ReopenUnownedMigrationSnapshot {
		return PlannedStrictConsistencyOpenRequest{}, errors.New("SQLite planned strict opener cannot reopen a migration snapshot")
	}
	if err := validateCredentialFreeIdentifier("SQLite planned strict run ID", request.RunID); err != nil {
		return PlannedStrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier("SQLite planned strict process epoch", request.ProcessEpoch); err != nil {
		return PlannedStrictConsistencyOpenRequest{}, err
	}
	if len(request.Tasks) == 0 {
		return PlannedStrictConsistencyOpenRequest{}, errors.New("SQLite planned strict consistency requires selected tables")
	}
	tasks := append([]state.TaskKey(nil), request.Tasks...)
	seen := make(map[state.TaskKey]struct{}, len(tasks))
	for index, task := range tasks {
		if err := task.Validate(); err != nil {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf("SQLite planned strict table %d: %w", index, err)
		}
		if task.Type != stage4AdapterNetworkTaskType || task.Schema != "" || task.Partition != "" {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf("SQLite planned strict table %d requires one unpartitioned %s task without a source schema", index, stage4AdapterNetworkTaskType)
		}
		if _, duplicate := seen[task]; duplicate {
			return PlannedStrictConsistencyOpenRequest{}, fmt.Errorf("SQLite planned strict table task is duplicated: %q", task.Table)
		}
		seen[task] = struct{}{}
	}
	sort.Slice(tasks, func(left, right int) bool { return strictConsistencyTaskLess(tasks[left], tasks[right]) })
	request.Tasks = tasks
	return request, nil
}

// sqliteStrictSnapshotReference derives a stable, credential-free token from
// the run, epoch, pinned data version, and the physical view nonce. A table
// scope opens one transaction at a time; two separately opened views must not
// be represented as the same durable source authority merely because no write
// happened between their data_version reads.
func sqliteStrictSnapshotReference(
	runID string,
	epoch string,
	dataVersion int64,
	viewID string,
) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"sqlite-strict\x00%s\x00%s\x00%d\x00%s",
		runID,
		epoch,
		dataVersion,
		viewID,
	)))
	return "sqlite-view-" + hex.EncodeToString(digest[:16])
}

func (session *SQLiteStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	if ctx == nil {
		return StrictConsistencyCapture{}, errors.New("SQLite strict evidence context is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return StrictConsistencyCapture{}, errors.New(
			"SQLite strict session is closed",
		)
	}
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	if session.readerBusy {
		return StrictConsistencyCapture{}, errors.New("SQLite strict session has an active reader")
	}
	reference := sqliteStrictSnapshotReference(
		session.runID,
		session.epoch,
		session.dataVersion,
		session.viewID,
	)
	capturedAt := time.Now().UTC()
	captures := make([]StrictConsistencyTableCapture, 0, len(session.tables))
	for _, table := range session.tables {
		count, err := session.countTable(ctx, table.Task)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		captures = append(captures, StrictConsistencyTableCapture{
			Task:                table.Task,
			AttemptID:           table.AttemptID,
			SnapshotReference:   reference,
			ExactSourceRowCount: count,
			CapturedAt:          capturedAt,
		})
	}
	// Table scope leaves every migration field empty; the core rejects a
	// migration reference at this scope.
	return StrictConsistencyCapture{Tables: captures}, nil
}

func (session *SQLiteStrictConsistencySession) countTable(
	ctx context.Context,
	task state.TaskKey,
) (int64, error) {
	quoted, err := quoteSQLiteStrictIdentifier(task.Table)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := session.transaction.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quoted,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count SQLite strict table %q: %w",
			task.Table,
			err,
		)
	}
	if count < 0 {
		return 0, fmt.Errorf(
			"SQLite strict table %q reported a negative count",
			task.Table,
		)
	}
	return count, nil
}

// RunReader lends the single stable view to one caller at a time. A second
// concurrent caller is refused rather than queued, because waiting would hide
// the contract violation the SQLite strict route is meant to make visible.
func (session *SQLiteStrictConsistencySession) RunReader(
	ctx context.Context,
	read func(context.Context, *sql.Tx) error,
) error {
	if ctx == nil {
		return errors.New("SQLite strict reader context is required")
	}
	if read == nil {
		return errors.New("SQLite strict reader callback is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return errors.New("SQLite strict session is closed")
	}
	if session.readerBusy {
		session.mu.Unlock()
		return errors.New(
			"SQLite strict consistency permits one source reader at a time",
		)
	}
	session.readerBusy = true
	session.readerDone = make(chan struct{})
	readerDone := session.readerDone
	transaction := session.transaction
	session.mu.Unlock()

	defer func() {
		session.mu.Lock()
		session.readerBusy = false
		close(readerDone)
		session.mu.Unlock()
	}()
	return read(ctx, transaction)
}

func (session *SQLiteStrictConsistencySession) Close(
	ctx context.Context,
) error {
	if ctx == nil {
		return errors.New("SQLite strict close context is required")
	}
	if session == nil {
		return nil
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		readerDone := session.readerDone
		transaction := session.transaction
		connection := session.connection
		session.mu.Unlock()
		go func() {
			// A callback owns the transaction until it returns. Rolling it back
			// early races live source queries; instead Close shuts the admission
			// gate, then waits for that single borrower before releasing it.
			if readerDone != nil {
				<-readerDone
			}
			closeErr := closeSQLiteStrictOpenFailure(nil, transaction, connection)
			session.mu.Lock()
			session.closeErr = closeErr
			session.mu.Unlock()
			close(session.closeDone)
		}()
	})
	select {
	case <-session.closeDone:
		session.mu.Lock()
		err := session.closeErr
		session.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func normalizeSQLiteStrictOpenRequest(
	request StrictConsistencyOpenRequest,
) (StrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencySQLite {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQLite strict opener cannot serve source engine %q",
			request.SourceEngine,
		)
	}
	// Migration scope would require handing one view to work spanning separate
	// reads. SQLite cannot export its view, so the honest answer is refusal.
	if request.Scope != state.StrictSnapshotTable {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQLite strict consistency supports table scope only, not %q",
			request.Scope,
		)
	}
	if request.RequiredMigrationSnapshot != nil {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQLite cannot reuse a durable migration snapshot; its stable view is the read transaction",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"SQLite strict run ID",
		request.RunID,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"SQLite strict process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQLite strict consistency requires selected tables",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, selected := range tables {
		if err := selected.Task.Validate(); err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQLite strict table %d: %w",
				index,
				err,
			)
		}
		if selected.Task.Partition != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQLite strict table %d must be unpartitioned",
				index,
			)
		}
		if selected.Task.Schema != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQLite strict table %d must not declare a source schema",
				index,
			)
		}
		if _, duplicate := seen[selected.Task]; duplicate {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQLite strict table task is duplicated: %q",
				selected.Task.Table,
			)
		}
		seen[selected.Task] = struct{}{}
	}
	request.Tables = tables
	return request, nil
}

// RunSQLiteAdapterStableNetworkReader is the only bridge from the strict
// transaction to Stage 4's stable source capability. It hands callers a
// fresh adapter facade backed by the strict transaction, never the ordinary
// source snapshot retained for initial discovery.
func RunSQLiteAdapterStableNetworkReader(
	ctx context.Context,
	session *SQLiteStrictConsistencySession,
	task state.TaskKey,
	source sourceAdapter,
	table schema.Table,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil {
		return errors.New("SQLite strict stable session is required")
	}
	if isNilInterface(work) {
		return errors.New("SQLite strict stable reader callback is required")
	}
	if task.Schema != table.Schema || task.Table != table.Name {
		return errors.New("SQLite strict stable task differs from table catalog")
	}
	base, ok := source.(*sqliteSourceAdapter)
	if !ok || base == nil || base.database == nil {
		return errors.New("SQLite strict composition requires the production SQLite source adapter")
	}
	return session.RunReader(ctx, func(readerCtx context.Context, transaction *sql.Tx) error {
		if transaction == nil {
			return errors.New("SQLite strict reader returned no transaction")
		}
		view := &sqliteSourceAdapter{database: base.database, snapshot: transaction}
		return work(readerCtx, view)
	})
}

// quoteSQLiteStrictIdentifier refuses anything it cannot quote safely rather
// than escaping aggressively, because a strict count that reads the wrong
// relation is worse than a refused migration.
func quoteSQLiteStrictIdentifier(name string) (string, error) {
	if name == "" {
		return "", errors.New("SQLite strict table name is required")
	}
	for _, symbol := range name {
		if symbol == 0 || symbol == '"' {
			return "", fmt.Errorf(
				"SQLite strict table name %q contains an unsupported character",
				name,
			)
		}
	}
	return `"` + name + `"`, nil
}

var _ StrictConsistencyOpener = (*SQLiteStrictConsistencyOpener)(nil)
var _ PlannedStrictConsistencyOpener = (*SQLiteStrictConsistencyOpener)(nil)
var _ StrictConsistencySession = (*SQLiteStrictConsistencySession)(nil)
