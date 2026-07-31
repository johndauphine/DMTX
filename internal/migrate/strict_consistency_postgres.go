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

	"github.com/johndauphine/dmtx/internal/state"
)

// PostgresStrictConsistencyOpener establishes PostgreSQL 16 exported MVCC
// snapshots. The supplied database remains caller-owned; only transactions
// opened by a strict session are released by that session.
type PostgresStrictConsistencyOpener struct {
	database *sql.DB

	beginTransaction func(context.Context) (postgresStrictTransaction, error)
	admitTransaction func(context.Context, postgresStrictTransaction) error
	exportSnapshot   func(context.Context, postgresStrictTransaction) (string, time.Time, error)
	countTable       func(context.Context, postgresStrictTransaction, state.TaskKey) (int64, error)
	importSnapshot   func(context.Context, postgresStrictTransaction, string) error
}

// PostgresStrictConsistencySession is the concrete reader capability handed to
// StrictConsistencyExecution.Run. RunReader may be called concurrently; every
// callback receives a separate read-only repeatable-read transaction importing
// the selected exported snapshot.
type PostgresStrictConsistencySession struct {
	mu sync.Mutex

	scope      state.StrictSnapshotScope
	tables     map[state.TaskKey]string
	capture    StrictConsistencyCapture
	owners     []postgresStrictTransaction
	readers    map[*postgresStrictReader]struct{}
	begin      func(context.Context) (postgresStrictTransaction, error)
	admit      func(context.Context, postgresStrictTransaction) error
	importRef  func(context.Context, postgresStrictTransaction, string) error
	beforeWork func()
	closed     bool

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// PostgresStrictSnapshotQueryer is the source-read surface provided inside a
// snapshot reader callback. It is intentionally compatible with pagination
// planning and row readers without exposing transaction commit operations.
type PostgresStrictSnapshotQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type postgresStrictTransaction interface {
	PostgresStrictSnapshotQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Rollback() error
}

type postgresStrictSQLTransaction struct {
	*sql.Tx
}

type postgresStrictReader struct {
	ready            chan struct{}
	callbackDecision chan struct{}
	callbackDone     chan struct{}
	callbackOnce     sync.Once
	callbackDoneOnce sync.Once
	callbackAdmitted bool
	once             sync.Once
	tx               postgresStrictTransaction
	err              error
}

var (
	_ StrictConsistencyOpener  = (*PostgresStrictConsistencyOpener)(nil)
	_ StrictConsistencySession = (*PostgresStrictConsistencySession)(nil)
)

// NewPostgresStrictConsistencyOpener returns a fail-closed PostgreSQL opener.
// Connection verification is deliberately deferred until snapshot creation so
// it runs on every physical transaction that owns or imports a stable view.
func NewPostgresStrictConsistencyOpener(
	database *sql.DB,
) (*PostgresStrictConsistencyOpener, error) {
	if database == nil {
		return nil, errors.New(
			"PostgreSQL strict consistency database is required",
		)
	}
	opener := &PostgresStrictConsistencyOpener{database: database}
	opener.beginTransaction = func(
		ctx context.Context,
	) (postgresStrictTransaction, error) {
		transaction, err := database.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"begin PostgreSQL strict read transaction: %w",
				err,
			)
		}
		return &postgresStrictSQLTransaction{Tx: transaction}, nil
	}
	opener.admitTransaction = admitPostgresStrictTransaction
	opener.exportSnapshot = exportPostgresStrictSnapshot
	opener.countTable = countPostgresStrictTable
	opener.importSnapshot = importPostgresStrictSnapshot
	return opener, nil
}

// OpenStrictConsistency opens all snapshot owners before returning. Any
// admission, export, or count failure rolls back every transaction opened so
// far and returns no usable session.
func (opener *PostgresStrictConsistencyOpener) OpenStrictConsistency(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	normalized, err := normalizePostgresStrictOpenRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New(
			"PostgreSQL strict consistency context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opener == nil || opener.database == nil ||
		opener.beginTransaction == nil ||
		opener.admitTransaction == nil ||
		opener.exportSnapshot == nil ||
		opener.countTable == nil ||
		opener.importSnapshot == nil {
		return nil, errors.New(
			"PostgreSQL strict consistency opener is not initialized",
		)
	}

	session := &PostgresStrictConsistencySession{
		scope:     normalized.Scope,
		tables:    make(map[state.TaskKey]string, len(normalized.Tables)),
		readers:   make(map[*postgresStrictReader]struct{}),
		begin:     opener.beginTransaction,
		admit:     opener.admitTransaction,
		importRef: opener.importSnapshot,
		closeDone: make(chan struct{}),
	}
	fail := func(primary error) (StrictConsistencySession, error) {
		cleanupErr := rollbackPostgresStrictTransactions(session.owners)
		session.owners = nil
		if cleanupErr != nil {
			primary = errors.Join(
				primary,
				fmt.Errorf(
					"release PostgreSQL strict snapshots after open failure: %w",
					cleanupErr,
				),
			)
		}
		return nil, primary
	}

	switch normalized.Scope {
	case state.StrictSnapshotMigration:
		transaction, err := opener.beginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		session.owners = append(session.owners, transaction)
		if err := opener.admitTransaction(ctx, transaction); err != nil {
			return fail(err)
		}
		reference, capturedAt, err := opener.exportSnapshot(ctx, transaction)
		if err != nil {
			return fail(err)
		}
		if err := validatePostgresSnapshotReference(reference); err != nil {
			return fail(fmt.Errorf(
				"PostgreSQL exported snapshot reference: %w",
				err,
			))
		}
		capturedAt = capturedAt.UTC()
		if capturedAt.IsZero() {
			return fail(errors.New(
				"PostgreSQL exported snapshot capture time is missing",
			))
		}
		session.capture = StrictConsistencyCapture{
			MigrationEpochID: postgresStrictEpochID(
				normalized.ProcessEpoch,
				reference,
			),
			MigrationSnapshotReference: reference,
			MigrationCapturedAt:        capturedAt,
			Tables: make(
				[]StrictConsistencyTableCapture,
				0,
				len(normalized.Tables),
			),
		}
		for _, selected := range normalized.Tables {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			count, err := opener.countTable(
				ctx,
				transaction,
				selected.Task,
			)
			if err != nil {
				return fail(err)
			}
			if count < 0 {
				return fail(fmt.Errorf(
					"PostgreSQL strict count for %s.%s is negative",
					selected.Task.Schema,
					selected.Task.Table,
				))
			}
			session.tables[selected.Task] = reference
			session.capture.Tables = append(
				session.capture.Tables,
				StrictConsistencyTableCapture{
					Task:                selected.Task,
					AttemptID:           selected.AttemptID,
					SnapshotReference:   reference,
					ExactSourceRowCount: count,
					CapturedAt:          capturedAt,
				},
			)
		}
	case state.StrictSnapshotTable:
		session.capture.Tables = make(
			[]StrictConsistencyTableCapture,
			0,
			len(normalized.Tables),
		)
		for _, selected := range normalized.Tables {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			transaction, err := opener.beginTransaction(ctx)
			if err != nil {
				return fail(err)
			}
			session.owners = append(session.owners, transaction)
			if err := opener.admitTransaction(ctx, transaction); err != nil {
				return fail(err)
			}
			reference, capturedAt, err := opener.exportSnapshot(
				ctx,
				transaction,
			)
			if err != nil {
				return fail(err)
			}
			if err := validatePostgresSnapshotReference(reference); err != nil {
				return fail(fmt.Errorf(
					"PostgreSQL exported snapshot reference for %s.%s: %w",
					selected.Task.Schema,
					selected.Task.Table,
					err,
				))
			}
			capturedAt = capturedAt.UTC()
			if capturedAt.IsZero() {
				return fail(fmt.Errorf(
					"PostgreSQL exported snapshot capture time for %s.%s is missing",
					selected.Task.Schema,
					selected.Task.Table,
				))
			}
			count, err := opener.countTable(
				ctx,
				transaction,
				selected.Task,
			)
			if err != nil {
				return fail(err)
			}
			if count < 0 {
				return fail(fmt.Errorf(
					"PostgreSQL strict count for %s.%s is negative",
					selected.Task.Schema,
					selected.Task.Table,
				))
			}
			session.tables[selected.Task] = reference
			session.capture.Tables = append(
				session.capture.Tables,
				StrictConsistencyTableCapture{
					Task:                selected.Task,
					AttemptID:           selected.AttemptID,
					SnapshotReference:   reference,
					ExactSourceRowCount: count,
					CapturedAt:          capturedAt,
				},
			)
		}
	default:
		return fail(fmt.Errorf(
			"PostgreSQL strict consistency scope %q is unsupported",
			normalized.Scope,
		))
	}
	return session, nil
}

// CaptureSameViewEvidence returns the exact counts captured through the
// exporter transaction(s). Evidence remains immutable after open.
func (session *PostgresStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	if ctx == nil {
		return StrictConsistencyCapture{}, errors.New(
			"PostgreSQL strict evidence context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	if session == nil {
		return StrictConsistencyCapture{}, errors.New(
			"PostgreSQL strict session is nil",
		)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return StrictConsistencyCapture{}, errors.New(
			"PostgreSQL strict session is closed",
		)
	}
	capture := session.capture
	capture.Tables = append(
		[]StrictConsistencyTableCapture(nil),
		session.capture.Tables...,
	)
	return capture, nil
}

// RunReader imports the selected exported snapshot before invoking work. The
// queryer is valid only for the callback duration and is always rolled back.
// Multiple callbacks may run in parallel against the same exported snapshot.
func (session *PostgresStrictConsistencySession) RunReader(
	ctx context.Context,
	task state.TaskKey,
	work func(context.Context, PostgresStrictSnapshotQueryer) error,
) (resultErr error) {
	if ctx == nil {
		return errors.New("PostgreSQL strict reader context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNilInterface(work) {
		return errors.New("PostgreSQL strict reader callback is required")
	}
	if session == nil {
		return errors.New("PostgreSQL strict session is nil")
	}

	reader := &postgresStrictReader{
		ready:            make(chan struct{}),
		callbackDecision: make(chan struct{}),
		callbackDone:     make(chan struct{}),
	}
	callbackAdmitted := false
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return errors.New("PostgreSQL strict session is closed")
	}
	reference, selected := session.tables[task]
	if !selected {
		session.mu.Unlock()
		return fmt.Errorf(
			"PostgreSQL strict reader task is not selected: type=%q schema=%q table=%q partition=%q",
			task.Type,
			task.Schema,
			task.Table,
			task.Partition,
		)
	}
	session.readers[reader] = struct{}{}
	session.mu.Unlock()
	defer func() {
		if callbackAdmitted {
			reader.finishCallback()
		} else {
			reader.rejectCallback()
		}
		cleanupErr := reader.close()
		session.mu.Lock()
		delete(session.readers, reader)
		session.mu.Unlock()
		if cleanupErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf(
					"release PostgreSQL strict snapshot reader: %w",
					cleanupErr,
				),
			)
		}
	}()

	transaction, err := session.begin(ctx)
	reader.bind(transaction)
	if err != nil {
		return err
	}
	if transaction == nil {
		return errors.New(
			"PostgreSQL strict reader transaction is unavailable",
		)
	}
	if err := session.readerSetupContext(ctx); err != nil {
		return err
	}
	if err := session.importRef(ctx, transaction, reference); err != nil {
		return err
	}
	if err := session.readerSetupContext(ctx); err != nil {
		return err
	}
	if err := session.admit(ctx, transaction); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return errors.New(
			"PostgreSQL strict session closed during reader setup",
		)
	}
	reader.admitCallback()
	callbackAdmitted = true
	beforeWork := session.beforeWork
	session.mu.Unlock()
	if beforeWork != nil {
		beforeWork()
	}
	resultErr = work(ctx, transaction)
	if resultErr == nil {
		resultErr = ctx.Err()
	}
	return resultErr
}

// Close releases imported readers first and exporter transactions second.
// Rollback has no context-aware database/sql API, so cleanup runs in one
// detached worker. Every Close caller waits on that shared cleanup with its own
// context; a short-deadline caller never inherits another caller's longer wait.
// Cleanup is initiated exactly once, and its eventual result remains stable.
func (session *PostgresStrictConsistencySession) Close(
	ctx context.Context,
) error {
	if session == nil {
		return errors.New("PostgreSQL strict session is nil")
	}
	if ctx == nil {
		return errors.New(
			"PostgreSQL strict cleanup context is required",
		)
	}
	if session.closeDone == nil {
		return errors.New(
			"PostgreSQL strict cleanup state is unavailable",
		)
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		readers := make(
			[]*postgresStrictReader,
			0,
			len(session.readers),
		)
		for reader := range session.readers {
			readers = append(readers, reader)
		}
		session.readers = make(map[*postgresStrictReader]struct{})
		owners := append(
			[]postgresStrictTransaction(nil),
			session.owners...,
		)
		session.owners = nil
		session.mu.Unlock()

		go func() {
			var cleanup []error
			for _, reader := range readers {
				if err := reader.close(); err != nil {
					cleanup = append(
						cleanup,
						fmt.Errorf(
							"release PostgreSQL strict snapshot reader: %w",
							err,
						),
					)
				}
			}
			if err := rollbackPostgresStrictTransactions(owners); err != nil {
				cleanup = append(cleanup, err)
			}
			session.closeErr = errors.Join(cleanup...)
			close(session.closeDone)
		}()
	})

	select {
	case <-session.closeDone:
		return session.closeErr
	default:
	}
	select {
	case <-session.closeDone:
		return session.closeErr
	case <-ctx.Done():
		return fmt.Errorf(
			"PostgreSQL strict snapshot cleanup did not finish before this caller's context ended; database rollback may still be in progress: %w",
			context.Cause(ctx),
		)
	}
}

func (session *PostgresStrictConsistencySession) readerSetupContext(
	ctx context.Context,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session.mu.Lock()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		return errors.New(
			"PostgreSQL strict session closed during reader setup",
		)
	}
	return nil
}

func (reader *postgresStrictReader) bind(
	transaction postgresStrictTransaction,
) {
	reader.tx = transaction
	close(reader.ready)
}

func (reader *postgresStrictReader) admitCallback() {
	reader.callbackAdmitted = true
	reader.callbackOnce.Do(func() {
		close(reader.callbackDecision)
	})
}

func (reader *postgresStrictReader) rejectCallback() {
	reader.callbackOnce.Do(func() {
		close(reader.callbackDecision)
	})
}

func (reader *postgresStrictReader) finishCallback() {
	reader.callbackDoneOnce.Do(func() {
		close(reader.callbackDone)
	})
}

func (reader *postgresStrictReader) close() error {
	if reader == nil {
		return nil
	}
	if reader.ready != nil {
		<-reader.ready
	}
	if reader.callbackDecision != nil {
		<-reader.callbackDecision
		if reader.callbackAdmitted && reader.callbackDone != nil {
			<-reader.callbackDone
		}
	}
	reader.once.Do(func() {
		reader.err = rollbackPostgresStrictTransaction(reader.tx)
	})
	return reader.err
}

func normalizePostgresStrictOpenRequest(
	request StrictConsistencyOpenRequest,
) (StrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencyPostgres {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"PostgreSQL strict opener cannot serve source engine %q",
			request.SourceEngine,
		)
	}
	if request.Scope != state.StrictSnapshotTable &&
		request.Scope != state.StrictSnapshotMigration {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"PostgreSQL strict consistency scope %q is unsupported",
			request.Scope,
		)
	}
	if request.RequiredMigrationSnapshot != nil {
		return StrictConsistencyOpenRequest{}, errors.New(
			"PostgreSQL cannot reuse a durable migration snapshot; resume requires a new process epoch and exported snapshot",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"PostgreSQL strict run ID",
		request.RunID,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"PostgreSQL strict process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyOpenRequest{}, errors.New(
			"PostgreSQL strict consistency requires selected tables",
		)
	}

	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, selected := range tables {
		if err := selected.Task.Validate(); err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d: %w",
				index,
				err,
			)
		}
		if selected.Task.Type != stage4AdapterNetworkTaskType ||
			selected.Task.Schema == "" ||
			selected.Task.Partition != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d requires one unpartitioned %s task with an explicit schema",
				index,
				stage4AdapterNetworkTaskType,
			)
		}
		if _, duplicate := seen[selected.Task]; duplicate {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table task is duplicated: schema=%q table=%q",
				selected.Task.Schema,
				selected.Task.Table,
			)
		}
		seen[selected.Task] = struct{}{}
		if selected.WorkTopologyHash == "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d work topology hash is required",
				index,
			)
		}
		if selected.DurableWorkAttempts < 0 {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d durable work attempts must not be negative",
				index,
			)
		}
		expected, err := BuildStrictConsistencyAttemptID(
			selected.Task,
			selected.WorkTopologyHash,
			selected.DurableWorkAttempts,
		)
		if err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d attempt identity: %w",
				index,
				err,
			)
		}
		if selected.AttemptID != expected {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"PostgreSQL strict table %d attempt ID does not match durable work identity",
				index,
			)
		}
	}
	sort.Slice(tables, func(left, right int) bool {
		return strictConsistencyTableLess(tables[left], tables[right])
	})
	request.Tables = tables
	return request, nil
}

func admitPostgresStrictTransaction(
	ctx context.Context,
	transaction postgresStrictTransaction,
) error {
	if transaction == nil {
		return errors.New(
			"PostgreSQL strict transaction is unavailable",
		)
	}
	var (
		serverVersion int
		isolation     string
		readOnly      bool
		tls           bool
	)
	err := transaction.QueryRowContext(ctx, `
		SELECT
			current_setting('server_version_num')::integer,
			current_setting('transaction_isolation'),
			current_setting('transaction_read_only')::boolean,
			COALESCE((
				SELECT ssl
				FROM pg_catalog.pg_stat_ssl
				WHERE pid = pg_backend_pid()
			), false)
	`).Scan(&serverVersion, &isolation, &readOnly, &tls)
	if err != nil {
		return fmt.Errorf(
			"verify PostgreSQL strict transaction: %w",
			err,
		)
	}
	if serverVersion < 160000 || serverVersion >= 170000 {
		return fmt.Errorf(
			"PostgreSQL strict consistency requires PostgreSQL 16, found server_version_num %d",
			serverVersion,
		)
	}
	if strings.ToLower(strings.TrimSpace(isolation)) != "repeatable read" {
		return fmt.Errorf(
			"PostgreSQL strict transaction isolation is %q, expected repeatable read",
			isolation,
		)
	}
	if !readOnly {
		return errors.New(
			"PostgreSQL strict transaction is not read-only",
		)
	}
	if !tls {
		return errors.New(
			"PostgreSQL strict consistency requires TLS on every snapshot connection",
		)
	}
	return nil
}

func exportPostgresStrictSnapshot(
	ctx context.Context,
	transaction postgresStrictTransaction,
) (string, time.Time, error) {
	var (
		reference string
		captured  time.Time
	)
	if err := transaction.QueryRowContext(ctx, `
		SELECT pg_export_snapshot(), transaction_timestamp()
	`).Scan(&reference, &captured); err != nil {
		return "", time.Time{}, fmt.Errorf(
			"export PostgreSQL strict snapshot: %w",
			err,
		)
	}
	return reference, captured, nil
}

func countPostgresStrictTable(
	ctx context.Context,
	transaction postgresStrictTransaction,
	task state.TaskKey,
) (int64, error) {
	var count int64
	if err := transaction.QueryRowContext(
		ctx,
		"SELECT count(*)::bigint FROM "+
			postgresQualified(task.Schema, task.Table),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"count PostgreSQL strict snapshot table %s.%s: %w",
			task.Schema,
			task.Table,
			err,
		)
	}
	return count, nil
}

func importPostgresStrictSnapshot(
	ctx context.Context,
	transaction postgresStrictTransaction,
	reference string,
) error {
	if err := validatePostgresSnapshotReference(reference); err != nil {
		return fmt.Errorf(
			"import PostgreSQL strict snapshot reference: %w",
			err,
		)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"SET TRANSACTION SNAPSHOT '"+reference+"'",
	); err != nil {
		return fmt.Errorf(
			"import PostgreSQL strict snapshot: %w",
			err,
		)
	}
	return nil
}

func validatePostgresSnapshotReference(reference string) error {
	if len(reference) == 0 || len(reference) > 256 {
		return errors.New(
			"snapshot reference must contain 1..256 bytes",
		)
	}
	parts := strings.Split(reference, "-")
	if len(parts) != 3 {
		return errors.New(
			"snapshot reference must contain exactly three hexadecimal fields",
		)
	}
	for _, part := range parts {
		if part == "" {
			return errors.New(
				"snapshot reference contains an empty hexadecimal field",
			)
		}
		for _, character := range []byte(part) {
			hexadecimal := character >= '0' && character <= '9' ||
				character >= 'A' && character <= 'F' ||
				character >= 'a' && character <= 'f'
			if !hexadecimal {
				return errors.New(
					"snapshot reference contains a non-hexadecimal character",
				)
			}
		}
	}
	return nil
}

func postgresStrictEpochID(processEpoch, reference string) string {
	digest := sha256.Sum256(
		[]byte(processEpoch + "\x00" + reference),
	)
	return "pg-epoch-" + hex.EncodeToString(digest[:])
}

func rollbackPostgresStrictTransactions(
	transactions []postgresStrictTransaction,
) error {
	var cleanup []error
	for index := len(transactions) - 1; index >= 0; index-- {
		if err := rollbackPostgresStrictTransaction(
			transactions[index],
		); err != nil {
			cleanup = append(
				cleanup,
				fmt.Errorf(
					"release PostgreSQL strict snapshot owner %d: %w",
					index,
					err,
				),
			)
		}
	}
	return errors.Join(cleanup...)
}

func rollbackPostgresStrictTransaction(
	transaction postgresStrictTransaction,
) error {
	if transaction == nil {
		return nil
	}
	err := transaction.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}
