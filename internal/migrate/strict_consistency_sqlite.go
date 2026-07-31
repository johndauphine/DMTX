package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

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
	runID       string
	epoch       string
	dataVersion int64
	tables      []StrictConsistencyTable

	mu         sync.Mutex
	readerBusy bool
	closed     bool
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
	// A read-only transaction is the stable view. SQLite promises a consistent
	// read for its lifetime, so it must be opened before any count and held
	// until the execution finishes.
	transaction, err := opener.source.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
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
		return nil, errors.Join(
			fmt.Errorf("read SQLite strict data version: %w", err),
			transaction.Rollback(),
		)
	}
	return &SQLiteStrictConsistencySession{
		transaction: transaction,
		runID:       normalized.RunID,
		epoch:       normalized.ProcessEpoch,
		dataVersion: dataVersion,
		tables:      normalized.Tables,
	}, nil
}

// sqliteStrictSnapshotReference derives a stable, credential-free token from
// the run, epoch, and pinned data version. It is deterministic so a replayed
// capture of the same view produces the same reference.
func sqliteStrictSnapshotReference(
	runID string,
	epoch string,
	dataVersion int64,
) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"sqlite-strict\x00%s\x00%s\x00%d",
		runID,
		epoch,
		dataVersion,
	)))
	return "sqlite-view-" + hex.EncodeToString(digest[:16])
}

func (session *SQLiteStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
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
	reference := sqliteStrictSnapshotReference(
		session.runID,
		session.epoch,
		session.dataVersion,
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
	if read == nil {
		return errors.New("SQLite strict reader callback is required")
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
	transaction := session.transaction
	session.mu.Unlock()

	defer func() {
		session.mu.Lock()
		session.readerBusy = false
		session.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	return read(ctx, transaction)
}

func (session *SQLiteStrictConsistencySession) Close(
	ctx context.Context,
) error {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	session.closed = true
	transaction := session.transaction
	session.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- transaction.Rollback() }()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			return fmt.Errorf("close SQLite strict read transaction: %w", err)
		}
		return nil
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
var _ StrictConsistencySession = (*SQLiteStrictConsistencySession)(nil)
