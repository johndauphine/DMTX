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

	mu     sync.Mutex
	next   int
	closed bool
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
	if err := opener.verifyLockPrivilege(ctx); err != nil {
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
		_, unlockErr := holder.ExecContext(
			context.WithoutCancel(ctx),
			"UNLOCK TABLES",
		)
		closeErr := holder.Close()
		return errors.Join(unlockErr, closeErr)
	}

	session := &MySQLStrictConsistencySession{
		namespace: opener.namespace,
		runID:     normalized.RunID,
		epoch:     normalized.ProcessEpoch,
		tables:    normalized.Tables,
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
	if err := releaseHolder(); err != nil {
		return nil, fmt.Errorf("release MySQL strict read lock: %w", err)
	}
	session.gtid = mysqlStrictViewToken(
		normalized.RunID,
		normalized.ProcessEpoch,
	)
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

func mysqlStrictViewToken(runID string, epoch string) string {
	digest := sha256.Sum256([]byte(
		"mysql-strict\x00" + runID + "\x00" + epoch,
	))
	return "mysql-view-" + hex.EncodeToString(digest[:16])
}

func (session *MySQLStrictConsistencySession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return StrictConsistencyCapture{}, errors.New(
			"MySQL strict session is closed",
		)
	}
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	capturedAt := time.Now().UTC()
	captures := make([]StrictConsistencyTableCapture, 0, len(session.tables))
	for index, table := range session.tables {
		// Rotate readers so the capture proves every session observes the same
		// view, not merely that one of them is self-consistent.
		reader := session.connections[index%len(session.connections)]
		namespace, err := quoteMySQLStrictIdentifier(session.namespace)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		name, err := quoteMySQLStrictIdentifier(table.Task.Table)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		var count int64
		if err := reader.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+namespace+"."+name,
		).Scan(&count); err != nil {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"count MySQL strict table %q: %w",
				table.Task.Table,
				err,
			)
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
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	session.closed = true
	connections := session.connections
	session.mu.Unlock()

	// The snapshot was started by statement, so it must be ended by statement:
	// returning a connection to the pool with an open transaction would leak
	// the read view into whatever borrows it next.
	var failures []error
	for _, connection := range connections {
		if _, err := connection.ExecContext(
			context.WithoutCancel(ctx),
			"ROLLBACK",
		); err != nil {
			failures = append(failures, err)
		}
		if err := connection.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return fmt.Errorf(
			"close MySQL strict session: %w",
			errors.Join(failures...),
		)
	}
	return nil
}

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
var _ StrictConsistencySession = (*MySQLStrictConsistencySession)(nil)
