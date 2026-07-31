package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
	connection  *sql.Conn
	transaction *sql.Tx

	mu     sync.Mutex
	closed bool
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
	// The transaction lifetime is deliberately detached from ctx. go-mssqldb can
	// return context.Canceled without releasing HOLDLOCK if database/sql races
	// its automatic rollback with Close, which would leave the source table
	// locked. The connection above was still acquired with ctx, so admission
	// stays cancellable while cleanup stays guaranteed.
	transaction, err := connection.BeginTx(
		context.WithoutCancel(ctx),
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
		connection:  connection,
		transaction: transaction,
		reference: sqlServerStrictViewToken(
			normalized.RunID,
			normalized.ProcessEpoch,
		),
	}
	defer func() {
		if resultErr == nil {
			return
		}
		resultErr = errors.Join(
			resultErr,
			session.Close(context.WithoutCancel(ctx)),
		)
	}()

	// Acquiring the shared lock is what makes the view stable. Doing it here,
	// rather than lazily at first read, means a strict route that cannot hold
	// the lock fails before any target work is authorized.
	for _, table := range normalized.Tables {
		qualified, err := opener.qualify(table.Task.Table)
		if err != nil {
			return nil, err
		}
		if _, err := transaction.ExecContext(
			ctx,
			"SELECT TOP(0) 1 FROM "+qualified+" WITH (TABLOCK, HOLDLOCK)",
		); err != nil {
			return nil, fmt.Errorf(
				"hold SQL Server strict table lock for %q: %w",
				table.Task.Table,
				err,
			)
		}
	}
	return session, nil
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
	defer session.mu.Unlock()
	if session.closed {
		return StrictConsistencyCapture{}, errors.New(
			"SQL Server strict session is closed",
		)
	}
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

func (session *SQLServerStrictConsistencySession) Close(
	ctx context.Context,
) error {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	session.closed = true
	transaction := session.transaction
	connection := session.connection
	session.mu.Unlock()

	var failures []error
	if transaction != nil {
		if err := transaction.Rollback(); err != nil &&
			!errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, err)
		}
	}
	if connection != nil {
		if err := connection.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return fmt.Errorf(
			"close SQL Server strict session: %w",
			errors.Join(failures...),
		)
	}
	return nil
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
var _ StrictConsistencySession = (*SQLServerStrictConsistencySession)(nil)
