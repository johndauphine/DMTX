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

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// ErrSQLServerMigrationSnapshotNotResumable is attached when a graceful
// migration-scope attempt has released (or cannot prove it retained) its
// database snapshot. A fresh run is required unless a durable cleanup receipt
// authorizes the all-work-complete cleanup-only path.
var ErrSQLServerMigrationSnapshotNotResumable = errors.New(
	"SQL Server migration snapshot is not resumable after graceful closure",
)

var errSQLServerMigrationSnapshotMissing = errors.New(
	"SQL Server migration snapshot is missing",
)

// SQLServerMigrationSnapshotOpener owns the SQL Server migration-scope
// contract. It creates one database snapshot with a deterministic, run-bound
// name, opens readers against that immutable database, and reopens exactly the
// persisted snapshot on resume. It never substitutes a new snapshot for a
// missing durable owner.
type SQLServerMigrationSnapshotOpener struct {
	source    *sql.DB
	endpoint  config.Endpoint
	namespace string

	verifySource func(context.Context, *sql.DB) error
	openSnapshot func(context.Context, config.Endpoint) (*sql.DB, error)
	now          func() time.Time

	// The test seams keep fault-path coverage at the production decision
	// boundary without making snapshot semantics depend on a mock driver.
	preflightFn       func(context.Context, string) (sqlServerMigrationSnapshotPlan, error)
	lookupFn          func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error)
	lookupIfPresentFn func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, bool, error)
	createFn          func(context.Context, sqlServerMigrationSnapshotPlan) error
	dropFn            func(context.Context, string) error
}

type sqlServerMigrationSnapshotPlan struct {
	sourceDatabase   string
	sourceDatabaseID int64
	snapshot         string
	files            []sqlServerMigrationSnapshotFile
}

type sqlServerMigrationSnapshotFile struct {
	id       int
	logical  string
	physical string
}

type sqlServerMigrationSnapshotRecord struct {
	sourceDatabaseID int64
	state            string
	readOnly         bool
	capturedAt       time.Time
}

type SQLServerMigrationSnapshotSession struct {
	source           *sql.DB
	snapshot         *sql.DB
	namespace        string
	runID            string
	sourceDatabase   string
	sourceDatabaseID int64

	reference string
	epoch     string
	captured  time.Time
	tables    []StrictConsistencyTable
	selected  map[state.TaskKey]struct{}

	mu                sync.Mutex
	readers           map[*sqlServerMigrationSnapshotReader]struct{}
	closed            bool
	preserve          bool
	cleanupAuthorized bool
	closeOnce         sync.Once
	closeDone         chan struct{}
	closeErr          error
	// dropSnapshot is a test seam for the terminal cleanup decision. Production
	// always leaves it nil and uses dropVerifiedSnapshot.
	dropSnapshot func(context.Context) error
}

type sqlServerMigrationSnapshotReader struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// SQLServerMigrationSnapshotFinalizationSession is the deliberately small
// no-work resume surface. It lets the Stage 4 runner retain the physical
// snapshot until final durable sentinels and the cleanup receipt are written,
// then explicitly authorizes DROP.
type SQLServerMigrationSnapshotFinalizationSession interface {
	PreserveSnapshotForResume() error
	AuthorizeSnapshotCleanup() error
	Close(context.Context) error
}

func NewSQLServerMigrationSnapshotOpener(
	source *sql.DB,
	endpoint config.Endpoint,
	namespace string,
) (*SQLServerMigrationSnapshotOpener, error) {
	if source == nil {
		return nil, errors.New(
			"SQL Server migration snapshot opener requires a source handle",
		)
	}
	if strings.TrimSpace(endpoint.Database) == "" {
		return nil, errors.New(
			"SQL Server migration snapshot opener requires the source database identity",
		)
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New(
			"SQL Server migration snapshot opener requires a schema",
		)
	}
	opener := &SQLServerMigrationSnapshotOpener{
		source:       source,
		endpoint:     endpoint,
		namespace:    namespace,
		verifySource: engine.VerifySQLServer2022Source,
		openSnapshot: engine.OpenSQLServer,
		now:          func() time.Time { return time.Now().UTC() },
	}
	return opener, nil
}

func (opener *SQLServerMigrationSnapshotOpener) OpenStrictConsistency(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	normalized, err := normalizeSQLServerMigrationOpenRequest(request)
	if err != nil {
		return nil, err
	}
	return opener.open(ctx, normalized, false)
}

func (opener *SQLServerMigrationSnapshotOpener) OpenPlannedStrictConsistency(
	ctx context.Context,
	request PlannedStrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	if request.SourceEngine != StrictConsistencyMSSQL ||
		request.Scope != state.StrictSnapshotMigration {
		return nil, fmt.Errorf(
			"SQL Server migration snapshot opener cannot serve %s/%s",
			request.SourceEngine,
			request.Scope,
		)
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server planned migration run ID",
		request.RunID,
	); err != nil {
		return nil, err
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server planned migration process epoch",
		request.ProcessEpoch,
	); err != nil {
		return nil, err
	}
	if len(request.Tasks) == 0 {
		return nil, errors.New(
			"SQL Server planned migration snapshot requires selected tables",
		)
	}
	tables := make([]StrictConsistencyTable, len(request.Tasks))
	seen := make(map[state.TaskKey]struct{}, len(request.Tasks))
	for index, task := range request.Tasks {
		if err := task.Validate(); err != nil {
			return nil, fmt.Errorf(
				"SQL Server planned migration task %d: %w", index, err,
			)
		}
		if task.Type != stage4AdapterNetworkTaskType || task.Schema == "" ||
			task.Partition != "" {
			return nil, fmt.Errorf(
				"SQL Server planned migration task %d requires one unpartitioned %s task with an explicit schema",
				index,
				stage4AdapterNetworkTaskType,
			)
		}
		if _, duplicate := seen[task]; duplicate {
			return nil, fmt.Errorf(
				"SQL Server planned migration task is duplicated: schema=%q table=%q",
				task.Schema,
				task.Table,
			)
		}
		seen[task] = struct{}{}
		tables[index] = StrictConsistencyTable{Task: task}
	}
	sort.Slice(tables, func(left, right int) bool {
		return strictConsistencyTaskLess(tables[left].Task, tables[right].Task)
	})
	return opener.open(ctx, StrictConsistencyOpenRequest{
		RunID:                          request.RunID,
		SourceEngine:                   request.SourceEngine,
		Scope:                          request.Scope,
		Resume:                         request.Resume,
		ProcessEpoch:                   request.ProcessEpoch,
		Tables:                         tables,
		RequiredMigrationSnapshot:      request.RequiredMigrationSnapshot,
		ReopenUnownedMigrationSnapshot: request.ReopenUnownedMigrationSnapshot,
	}, true)
}

// OpenCompletedMigrationSnapshot is the no-work resume path. A process may
// die after the final table checkpoint but before StrictConsistencyExecution
// closes its database snapshot. Reopen, authenticate, and drop that exact
// durable owner before reporting the run fully complete; otherwise a successful
// resume would silently orphan a SQL Server database snapshot.
func (opener *SQLServerMigrationSnapshotOpener) OpenCompletedMigrationSnapshot(
	ctx context.Context,
	runID string,
	owner state.StrictMigrationSnapshot,
	allowAbsentAfterIntent bool,
) (SQLServerMigrationSnapshotFinalizationSession, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("SQL Server migration snapshot cleanup context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if opener == nil || opener.source == nil || opener.verifySource == nil ||
		opener.openSnapshot == nil || opener.now == nil {
		return nil, false, errors.New("SQL Server migration snapshot opener is not initialized")
	}
	if err := opener.verifySource(ctx, opener.source); err != nil {
		return nil, false, fmt.Errorf("preflight SQL Server completed migration snapshot cleanup: %w", err)
	}
	plan, err := opener.preflightForOpen(ctx, runID)
	if err != nil {
		return nil, false, err
	}
	if owner.RunID != runID || owner.SourceEngine != "mssql" ||
		owner.SnapshotReference != plan.snapshot ||
		owner.EpochID != sqlServerMigrationSnapshotEpoch(plan.snapshot) ||
		owner.CapturedAt.IsZero() {
		return nil, false, errors.New("SQL Server completed migration snapshot owner does not match this configured source")
	}
	record, found, err := opener.lookupIfPresentForOpen(ctx, plan)
	if err != nil {
		return nil, false, fmt.Errorf("reopen completed SQL Server migration snapshot %q for cleanup: %w", plan.snapshot, err)
	}
	if !found {
		if allowAbsentAfterIntent {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf(
			"%w: completed SQL Server migration snapshot %q is missing without a durable cleanup intent",
			errSQLServerMigrationSnapshotMissing,
			plan.snapshot,
		)
	}
	if record.sourceDatabaseID != plan.sourceDatabaseID || record.state != "ONLINE" ||
		!record.readOnly || record.capturedAt.IsZero() {
		return nil, false, fmt.Errorf(
			"completed SQL Server migration snapshot %q is not an online read-only snapshot of the configured source",
			plan.snapshot,
		)
	}
	if !record.capturedAt.Equal(owner.CapturedAt.UTC()) {
		return nil, false, errors.New("SQL Server completed migration snapshot creation time differs from durable owner")
	}
	snapshotEndpoint := opener.endpoint
	snapshotEndpoint.Database = plan.snapshot
	snapshotDatabase, err := opener.openSnapshot(ctx, snapshotEndpoint)
	if err != nil {
		return nil, false, fmt.Errorf("open verified TLS SQL Server completed migration snapshot before cleanup: %w", err)
	}
	if err := snapshotDatabase.PingContext(ctx); err != nil {
		_ = snapshotDatabase.Close()
		return nil, false, fmt.Errorf("verify SQL Server completed migration snapshot reader before cleanup: %w", err)
	}
	session := &SQLServerMigrationSnapshotSession{
		source:           opener.source,
		snapshot:         snapshotDatabase,
		namespace:        opener.namespace,
		runID:            runID,
		sourceDatabase:   plan.sourceDatabase,
		sourceDatabaseID: plan.sourceDatabaseID,
		reference:        plan.snapshot,
		epoch:            sqlServerMigrationSnapshotEpoch(plan.snapshot),
		captured:         record.capturedAt.UTC(),
		selected:         make(map[state.TaskKey]struct{}),
		readers:          make(map[*sqlServerMigrationSnapshotReader]struct{}),
		closeDone:        make(chan struct{}),
	}
	return session, false, nil
}

func (opener *SQLServerMigrationSnapshotOpener) open(
	ctx context.Context,
	request StrictConsistencyOpenRequest,
	planned bool,
) (_ StrictConsistencySession, resultErr error) {
	if ctx == nil {
		return nil, errors.New("SQL Server migration snapshot context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opener == nil || opener.source == nil || opener.verifySource == nil ||
		opener.openSnapshot == nil || opener.now == nil {
		return nil, errors.New("SQL Server migration snapshot opener is not initialized")
	}
	if err := opener.verifySource(ctx, opener.source); err != nil {
		return nil, fmt.Errorf(
			"preflight SQL Server migration snapshot server/version/Azure admission: %w",
			err,
		)
	}
	plan, err := opener.preflightForOpen(ctx, request.RunID)
	if err != nil {
		return nil, err
	}
	var record sqlServerMigrationSnapshotRecord
	created := false
	if request.RequiredMigrationSnapshot != nil {
		required := *request.RequiredMigrationSnapshot
		if required.SourceEngine != "mssql" ||
			required.RunID != request.RunID ||
			required.SnapshotReference != plan.snapshot ||
			required.EpochID != sqlServerMigrationSnapshotEpoch(plan.snapshot) {
			return nil, errors.New(
				"SQL Server migration resume durable snapshot identity does not match this source",
			)
		}
		record, err = opener.lookupForOpen(ctx, plan)
		if err != nil {
			return nil, fmt.Errorf(
				"reopen required SQL Server migration snapshot %q: %w",
				plan.snapshot,
				err,
			)
		}
		if !record.capturedAt.Equal(required.CapturedAt.UTC()) {
			return nil, errors.New(
				"SQL Server migration resume snapshot creation time differs from durable authority",
			)
		}
	} else if request.Resume && request.ReopenUnownedMigrationSnapshot {
		// CREATE DATABASE SNAPSHOT happens before the coordinator can durably
		// save its owner. A process crash in that interval may leave precisely
		// this deterministic snapshot behind. It is safe to adopt only after
		// lookupSnapshot proves it belongs to this configured source. In
		// particular, do not fall through to creation: resume must never move
		// the source instant forward.
		record, err = opener.lookupForOpen(ctx, plan)
		if err != nil {
			return nil, fmt.Errorf(
				"reopen unowned SQL Server migration snapshot %q after pre-state crash: %w",
				plan.snapshot,
				err,
			)
		}
	} else {
		if _, found, lookupErr := opener.lookupIfPresentForOpen(ctx, plan); lookupErr != nil {
			return nil, lookupErr
		} else if found {
			return nil, fmt.Errorf(
				"SQL Server migration snapshot %q already exists without durable resume authority",
				plan.snapshot,
			)
		}
		if err := opener.createForOpen(ctx, plan); err != nil {
			return nil, opener.reconcileAmbiguousCreateFailure(ctx, plan, err)
		}
		created = true
		record, err = opener.lookupForOpen(ctx, plan)
		if err != nil {
			return nil, opener.cleanupCreatedSnapshot(
				ctx,
				plan,
				nil,
				fmt.Errorf("verify created SQL Server migration snapshot: %w", err),
			)
		}
	}

	snapshotEndpoint := opener.endpoint
	snapshotEndpoint.Database = plan.snapshot
	snapshotDatabase, err := opener.openSnapshot(ctx, snapshotEndpoint)
	if err != nil {
		if created {
			return nil, opener.cleanupCreatedSnapshot(
				ctx,
				plan,
				&record,
				fmt.Errorf(
					"open verified TLS SQL Server migration snapshot reader: %w",
					err,
				),
			)
		}
		return nil, fmt.Errorf(
			"open verified TLS SQL Server migration snapshot reader: %w",
			err,
		)
	}
	session := &SQLServerMigrationSnapshotSession{
		source:           opener.source,
		snapshot:         snapshotDatabase,
		namespace:        opener.namespace,
		runID:            request.RunID,
		sourceDatabase:   plan.sourceDatabase,
		sourceDatabaseID: plan.sourceDatabaseID,
		reference:        plan.snapshot,
		epoch:            sqlServerMigrationSnapshotEpoch(plan.snapshot),
		captured:         record.capturedAt.UTC(),
		tables:           append([]StrictConsistencyTable(nil), request.Tables...),
		selected:         make(map[state.TaskKey]struct{}, len(request.Tables)),
		readers:          make(map[*sqlServerMigrationSnapshotReader]struct{}),
		closeDone:        make(chan struct{}),
	}
	for _, table := range session.tables {
		session.selected[table.Task] = struct{}{}
	}
	if planned {
		for index := range session.tables {
			session.tables[index].AttemptID = ""
		}
	}
	return session, nil
}

// cleanupCreatedSnapshot makes failure after this invocation has created a
// snapshot operationally visible without allowing a cancelled caller context
// to make physical cleanup unbounded. Immediately before DROP it rereads the
// snapshot record and requires the captured creation time to match the one
// this invocation verified, so a same-name replacement cannot be deleted. It
// is intentionally used only when this process observed a successful CREATE;
// durable/resume snapshots are never eligible for this fallback.
func (opener *SQLServerMigrationSnapshotOpener) cleanupCreatedSnapshot(
	_ context.Context,
	plan sqlServerMigrationSnapshotPlan,
	expected *sqlServerMigrationSnapshotRecord,
	primary error,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(),
		strictConsistencyCleanupTimeout,
	)
	defer cancel()
	record, err := opener.lookupForOpen(cleanupCtx, plan)
	if err != nil {
		return errors.Join(primary, fmt.Errorf(
			"reauthenticate SQL Server migration snapshot %q before cleanup: %w",
			plan.snapshot,
			err,
		))
	}
	if expected != nil && !record.capturedAt.Equal(expected.capturedAt.UTC()) {
		return errors.Join(primary, errors.New(
			"refuse cleanup because SQL Server migration snapshot creation time changed after setup verification",
		))
	}
	if err := opener.dropForOpen(cleanupCtx, plan.snapshot); err != nil {
		return errors.Join(primary, fmt.Errorf(
			"cleanup SQL Server migration snapshot %q after setup failure: %w",
			plan.snapshot,
			err,
		))
	}
	return primary
}

// reconcileAmbiguousCreateFailure handles the unavoidable SQL Server window
// where CREATE DATABASE SNAPSHOT may commit before the caller receives an
// error. The first lookup proved the deterministic name absent, so an exact
// verified snapshot found here belongs to this attempted creation and must be
// removed with bounded cleanup rather than left as an unowned orphan. If the
// existence or identity cannot be proved, preserve the primary error together
// with that uncertainty: resume's unowned deterministic-snapshot recovery is
// the only permitted reopening path and never creates a replacement.
func (opener *SQLServerMigrationSnapshotOpener) reconcileAmbiguousCreateFailure(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
	createErr error,
) error {
	primary := fmt.Errorf(
		"create SQL Server migration snapshot %q: %w", plan.snapshot, createErr,
	)
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(),
		strictConsistencyCleanupTimeout,
	)
	defer cancel()
	record, found, err := opener.lookupIfPresentForOpen(cleanupCtx, plan)
	if err != nil {
		return errors.Join(primary, fmt.Errorf(
			"inspect SQL Server migration snapshot %q after ambiguous CREATE acknowledgement: %w",
			plan.snapshot,
			err,
		))
	}
	if !found {
		return primary
	}
	if err := verifySQLServerMigrationSnapshotRecord(plan, record); err != nil {
		return errors.Join(primary, fmt.Errorf(
			"refuse cleanup of SQL Server migration snapshot %q after ambiguous CREATE acknowledgement: %w",
			plan.snapshot,
			err,
		))
	}
	return opener.cleanupCreatedSnapshot(ctx, plan, &record, primary)
}

func (opener *SQLServerMigrationSnapshotOpener) preflightForOpen(
	ctx context.Context,
	runID string,
) (sqlServerMigrationSnapshotPlan, error) {
	if opener.preflightFn != nil {
		return opener.preflightFn(ctx, runID)
	}
	return opener.preflight(ctx, runID)
}

func (opener *SQLServerMigrationSnapshotOpener) lookupForOpen(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) (sqlServerMigrationSnapshotRecord, error) {
	if opener.lookupFn != nil {
		return opener.lookupFn(ctx, plan)
	}
	return opener.lookupSnapshot(ctx, plan)
}

func (opener *SQLServerMigrationSnapshotOpener) lookupIfPresentForOpen(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) (sqlServerMigrationSnapshotRecord, bool, error) {
	if opener.lookupIfPresentFn != nil {
		return opener.lookupIfPresentFn(ctx, plan)
	}
	return opener.lookupSnapshotIfPresent(ctx, plan)
}

func (opener *SQLServerMigrationSnapshotOpener) createForOpen(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) error {
	if opener.createFn != nil {
		return opener.createFn(ctx, plan)
	}
	return opener.createSnapshot(ctx, plan)
}

func (opener *SQLServerMigrationSnapshotOpener) dropForOpen(
	ctx context.Context,
	reference string,
) error {
	if opener.dropFn != nil {
		return opener.dropFn(ctx, reference)
	}
	return opener.dropSnapshot(ctx, reference)
}

func normalizeSQLServerMigrationOpenRequest(
	request StrictConsistencyOpenRequest,
) (StrictConsistencyOpenRequest, error) {
	if request.SourceEngine != StrictConsistencyMSSQL ||
		request.Scope != state.StrictSnapshotMigration {
		return StrictConsistencyOpenRequest{}, fmt.Errorf(
			"SQL Server migration snapshot opener cannot serve %s/%s",
			request.SourceEngine,
			request.Scope,
		)
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server migration run ID",
		request.RunID,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if err := validateCredentialFreeIdentifier(
		"SQL Server migration process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyOpenRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQL Server migration snapshot requires selected tables",
		)
	}
	if request.ReopenUnownedMigrationSnapshot &&
		(!request.Resume || request.RequiredMigrationSnapshot != nil) {
		return StrictConsistencyOpenRequest{}, errors.New(
			"SQL Server unowned migration snapshot recovery requires resume without a durable owner",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, table := range tables {
		if err := table.Task.Validate(); err != nil {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server migration table %d: %w", index, err,
			)
		}
		if table.Task.Type != stage4AdapterNetworkTaskType ||
			table.Task.Schema == "" || table.Task.Partition != "" {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server migration table %d requires one unpartitioned %s task with an explicit schema",
				index,
				stage4AdapterNetworkTaskType,
			)
		}
		if _, duplicate := seen[table.Task]; duplicate {
			return StrictConsistencyOpenRequest{}, fmt.Errorf(
				"SQL Server migration task is duplicated: schema=%q table=%q",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		seen[table.Task] = struct{}{}
	}
	sort.Slice(tables, func(left, right int) bool {
		return strictConsistencyTaskLess(tables[left].Task, tables[right].Task)
	})
	request.Tables = tables
	return request, nil
}

func (opener *SQLServerMigrationSnapshotOpener) preflight(
	ctx context.Context,
	runID string,
) (sqlServerMigrationSnapshotPlan, error) {
	var databaseName string
	var databaseID int64
	var createDatabase, alterAnyDatabase bool
	if err := opener.source.QueryRowContext(ctx, `
		SELECT
			DB_NAME(),
			CONVERT(bigint, DB_ID()),
			CONVERT(bit, CASE WHEN
				HAS_PERMS_BY_NAME('master', 'DATABASE', 'CREATE DATABASE') = 1 OR
				HAS_PERMS_BY_NAME(NULL, NULL, 'CREATE ANY DATABASE') = 1 OR
				HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY DATABASE') = 1
			THEN 1 ELSE 0 END),
			CONVERT(bit, CASE WHEN
				HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY DATABASE') = 1 OR
				IS_SRVROLEMEMBER('sysadmin') = 1
			THEN 1 ELSE 0 END)
	`).Scan(&databaseName, &databaseID, &createDatabase, &alterAnyDatabase); err != nil {
		return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
			"preflight SQL Server migration snapshot permissions: %w", err,
		)
	}
	if databaseName != opener.endpoint.Database || databaseID <= 0 {
		return sqlServerMigrationSnapshotPlan{}, errors.New(
			"SQL Server migration snapshot source identity differs from the configured source database",
		)
	}
	if !createDatabase || !alterAnyDatabase {
		return sqlServerMigrationSnapshotPlan{}, errors.New(
			"SQL Server migration snapshot requires CREATE DATABASE plus ALTER ANY DATABASE (or sysadmin) for verified creation and cleanup",
		)
	}
	rows, err := opener.source.QueryContext(ctx, `
		SELECT file_id, name, physical_name, type_desc, state_desc
		FROM sys.database_files
		ORDER BY file_id
	`)
	if err != nil {
		return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
			"inspect SQL Server migration snapshot data-file path: %w", err,
		)
	}
	defer rows.Close()
	files := make([]sqlServerMigrationSnapshotFile, 0, 2)
	for rows.Next() {
		var id int
		var logical, physical, typeDescription, state string
		if err := rows.Scan(&id, &logical, &physical, &typeDescription, &state); err != nil {
			return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
				"read SQL Server migration snapshot data file: %w", err,
			)
		}
		if state != "ONLINE" {
			return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
				"SQL Server migration snapshot refuses non-online file %q", logical,
			)
		}
		switch typeDescription {
		case "ROWS":
			files = append(files, sqlServerMigrationSnapshotFile{
				id: id, logical: logical, physical: physical,
			})
		case "LOG":
		case "FILESTREAM":
			return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
				"SQL Server migration snapshot refuses FILESTREAM source file %q", logical,
			)
		default:
			return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
				"SQL Server migration snapshot refuses unsupported source file type %q for %q",
				typeDescription,
				logical,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return sqlServerMigrationSnapshotPlan{}, fmt.Errorf(
			"iterate SQL Server migration snapshot data files: %w", err,
		)
	}
	if len(files) == 0 {
		return sqlServerMigrationSnapshotPlan{}, errors.New(
			"SQL Server migration snapshot source has no online data files",
		)
	}
	return sqlServerMigrationSnapshotPlan{
		sourceDatabase: databaseName, sourceDatabaseID: databaseID,
		snapshot: sqlServerMigrationSnapshotName(runID, databaseName), files: files,
	}, nil
}

func sqlServerMigrationSnapshotName(runID, database string) string {
	digest := sha256.Sum256([]byte("dmtx-mssql-snapshot\x00" + runID + "\x00" + database))
	return "dmtx_ss_" + hex.EncodeToString(digest[:20])
}

func sqlServerMigrationSnapshotEpoch(snapshot string) string {
	digest := sha256.Sum256([]byte("dmtx-mssql-snapshot-epoch\x00" + snapshot))
	return "mssql-epoch-" + hex.EncodeToString(digest[:20])
}

func sqlServerMigrationSnapshotFilePath(
	physical string,
	snapshot string,
	fileID int,
) (string, error) {
	if fileID < 1 || strings.IndexByte(physical, 0) >= 0 {
		return "", errors.New("SQL Server migration snapshot file path is invalid")
	}
	separator := strings.LastIndexAny(physical, `/\\`)
	if separator < 1 || separator == len(physical)-1 {
		return "", fmt.Errorf(
			"SQL Server migration snapshot cannot derive a sparse-file directory from %q",
			physical,
		)
	}
	return physical[:separator+1] + snapshot + "_" + fmt.Sprint(fileID) + ".ss", nil
}

func (opener *SQLServerMigrationSnapshotOpener) createSnapshot(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) error {
	snapshot, err := quoteSQLServerStrictIdentifier(plan.snapshot)
	if err != nil {
		return err
	}
	source, err := quoteSQLServerStrictIdentifier(plan.sourceDatabase)
	if err != nil {
		return err
	}
	parts := make([]string, 0, len(plan.files))
	for _, file := range plan.files {
		logical, err := quoteSQLServerStrictIdentifier(file.logical)
		if err != nil {
			return fmt.Errorf("SQL Server migration snapshot file identity: %w", err)
		}
		path, err := sqlServerMigrationSnapshotFilePath(file.physical, plan.snapshot, file.id)
		if err != nil {
			return err
		}
		parts = append(parts, "(NAME = "+logical+", FILENAME = N'"+
			strings.ReplaceAll(path, "'", "''")+"')")
	}
	statement := "CREATE DATABASE " + snapshot + " ON " +
		strings.Join(parts, ", ") + " AS SNAPSHOT OF " + source
	if _, err := opener.source.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf(
			"create SQL Server migration snapshot %q: %w", plan.snapshot, err,
		)
	}
	return nil
}

func (opener *SQLServerMigrationSnapshotOpener) lookupSnapshotIfPresent(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) (sqlServerMigrationSnapshotRecord, bool, error) {
	record, err := opener.lookupSnapshotRecord(ctx, plan)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlServerMigrationSnapshotRecord{}, false, nil
	}
	if err != nil {
		return sqlServerMigrationSnapshotRecord{}, false, err
	}
	return record, true, nil
}

func (opener *SQLServerMigrationSnapshotOpener) lookupSnapshot(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) (sqlServerMigrationSnapshotRecord, error) {
	record, err := opener.lookupSnapshotRecord(ctx, plan)
	if errors.Is(err, sql.ErrNoRows) {
		return sqlServerMigrationSnapshotRecord{}, fmt.Errorf(
			"%w: %q", errSQLServerMigrationSnapshotMissing, plan.snapshot,
		)
	}
	if err != nil {
		return sqlServerMigrationSnapshotRecord{}, err
	}
	if err := verifySQLServerMigrationSnapshotRecord(plan, record); err != nil {
		return sqlServerMigrationSnapshotRecord{}, err
	}
	return record, nil
}

func verifySQLServerMigrationSnapshotRecord(
	plan sqlServerMigrationSnapshotPlan,
	record sqlServerMigrationSnapshotRecord,
) error {
	if record.sourceDatabaseID != plan.sourceDatabaseID || record.state != "ONLINE" ||
		!record.readOnly || record.capturedAt.IsZero() {
		return fmt.Errorf(
			"SQL Server migration snapshot %q is not an online read-only snapshot of the configured source",
			plan.snapshot,
		)
	}
	return nil
}

func (opener *SQLServerMigrationSnapshotOpener) lookupSnapshotRecord(
	ctx context.Context,
	plan sqlServerMigrationSnapshotPlan,
) (sqlServerMigrationSnapshotRecord, error) {
	var record sqlServerMigrationSnapshotRecord
	err := opener.source.QueryRowContext(ctx, `
		SELECT source_database_id, state_desc, is_read_only, create_date
		FROM sys.databases
		WHERE name = @p1
	`, plan.snapshot).Scan(
		&record.sourceDatabaseID,
		&record.state,
		&record.readOnly,
		&record.capturedAt,
	)
	if err != nil {
		return sqlServerMigrationSnapshotRecord{}, err
	}
	record.capturedAt = record.capturedAt.UTC()
	return record, nil
}

func (opener *SQLServerMigrationSnapshotOpener) dropSnapshot(
	ctx context.Context,
	reference string,
) error {
	quoted, err := quoteSQLServerStrictIdentifier(reference)
	if err != nil {
		return err
	}
	if _, err := opener.source.ExecContext(ctx, "DROP DATABASE "+quoted); err != nil {
		return fmt.Errorf("drop SQL Server migration snapshot %q: %w", reference, err)
	}
	return nil
}

func (session *SQLServerMigrationSnapshotSession) CaptureSameViewEvidence(
	ctx context.Context,
) (StrictConsistencyCapture, error) {
	if session == nil || session.snapshot == nil {
		return StrictConsistencyCapture{}, errors.New(
			"SQL Server migration snapshot session is unavailable",
		)
	}
	if ctx == nil {
		return StrictConsistencyCapture{}, errors.New(
			"SQL Server migration snapshot evidence context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return StrictConsistencyCapture{}, err
	}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return StrictConsistencyCapture{}, errors.New(
			"SQL Server migration snapshot session is closed",
		)
	}
	tables := append([]StrictConsistencyTable(nil), session.tables...)
	namespace := session.namespace
	reference := session.reference
	epoch := session.epoch
	captured := session.captured
	session.mu.Unlock()
	transaction, err := session.snapshot.BeginTx(ctx, &sql.TxOptions{
		// go-mssqldb rejects TxOptions.ReadOnly. The session connects to a
		// verified SQL Server database snapshot, which is intrinsically
		// read-only, so repeatable-read isolation is the relevant request.
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return StrictConsistencyCapture{}, fmt.Errorf(
			"begin SQL Server migration snapshot evidence read: %w", err,
		)
	}
	defer transaction.Rollback()
	quotedNamespace, err := quoteSQLServerStrictIdentifier(namespace)
	if err != nil {
		return StrictConsistencyCapture{}, err
	}
	capture := StrictConsistencyCapture{
		MigrationEpochID:           epoch,
		MigrationSnapshotReference: reference,
		MigrationCapturedAt:        captured,
		Tables:                     make([]StrictConsistencyTableCapture, 0, len(tables)),
	}
	for _, table := range tables {
		quotedTable, err := quoteSQLServerStrictIdentifier(table.Task.Table)
		if err != nil {
			return StrictConsistencyCapture{}, err
		}
		var count int64
		if err := transaction.QueryRowContext(
			ctx,
			"SELECT COUNT_BIG(*) FROM "+quotedNamespace+"."+quotedTable,
		).Scan(&count); err != nil {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"count SQL Server migration snapshot table %q: %w",
				table.Task.Table,
				err,
			)
		}
		if count < 0 {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"SQL Server migration snapshot count for %s.%s is negative",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		capture.Tables = append(capture.Tables, StrictConsistencyTableCapture{
			Task:                table.Task,
			AttemptID:           table.AttemptID,
			SnapshotReference:   reference,
			ExactSourceRowCount: count,
			CapturedAt:          captured,
		})
	}
	return capture, nil
}

func (session *SQLServerMigrationSnapshotSession) RunReader(
	ctx context.Context,
	task state.TaskKey,
	work func(context.Context, SQLServerStrictSnapshotQueryer) error,
) (resultErr error) {
	if ctx == nil {
		return errors.New("SQL Server migration snapshot reader context is required")
	}
	if isNilInterface(work) {
		return errors.New("SQL Server migration snapshot reader callback is required")
	}
	readerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopCallerCancellation := context.AfterFunc(ctx, cancel)
	defer stopCallerCancellation()
	reader := &sqlServerMigrationSnapshotReader{cancel: cancel, done: make(chan struct{})}
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return errors.New("SQL Server migration snapshot session is closed")
	}
	if _, selected := session.selected[task]; !selected {
		session.mu.Unlock()
		return fmt.Errorf(
			"SQL Server migration snapshot reader task is not selected: type=%q schema=%q table=%q partition=%q",
			task.Type, task.Schema, task.Table, task.Partition,
		)
	}
	session.readers[reader] = struct{}{}
	snapshot := session.snapshot
	session.mu.Unlock()
	defer func() {
		close(reader.done)
		session.mu.Lock()
		delete(session.readers, reader)
		session.mu.Unlock()
	}()
	transaction, err := snapshot.BeginTx(readerCtx, &sql.TxOptions{
		// The database snapshot itself is read-only; do not set ReadOnly here
		// because go-mssqldb rejects that TxOption.
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("begin SQL Server migration snapshot reader: %w", err)
	}
	defer func() {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf(
				"release SQL Server migration snapshot reader: %w", err,
			))
		}
	}()
	resultErr = work(readerCtx, transaction)
	if resultErr == nil {
		resultErr = readerCtx.Err()
	}
	return resultErr
}

// PreserveSnapshotForResume prevents Close from dropping the physical snapshot.
// The Stage 4 migration runner arms it only after every strict source/table
// work item is complete, so an ordinary transfer or cancellation still releases
// the owned snapshot while finalization-state failures remain resumable.
func (session *SQLServerMigrationSnapshotSession) PreserveSnapshotForResume() error {
	if session == nil {
		return errors.New("SQL Server migration snapshot session is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return errors.New("SQL Server migration snapshot session is closed")
	}
	session.preserve = true
	return nil
}

// AuthorizeSnapshotCleanup releases a prior preservation only after the
// immutable cleanup intent is durable. Close may then perform the verified
// DROP DATABASE operation.
func (session *SQLServerMigrationSnapshotSession) AuthorizeSnapshotCleanup() error {
	if session == nil {
		return errors.New("SQL Server migration snapshot session is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return errors.New("SQL Server migration snapshot session is closed")
	}
	if !session.preserve {
		return errors.New("SQL Server migration snapshot cleanup was not preserved")
	}
	session.preserve = false
	session.cleanupAuthorized = true
	return nil
}

// StrictMigrationSnapshotResumeAvailable reports whether Close may safely be
// followed by a resume. A retained snapshot is directly resumable; after the
// durable cleanup receipt is written, the all-work-complete cleanup-only path
// is also safe even though the physical snapshot may already be gone.
func (session *SQLServerMigrationSnapshotSession) StrictMigrationSnapshotResumeAvailable() bool {
	if session == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.preserve || session.cleanupAuthorized
}

func RunSQLServerMigrationSnapshotAdapterStableNetworkReader(
	ctx context.Context,
	session *SQLServerMigrationSnapshotSession,
	task state.TaskKey,
	source sourceAdapter,
	table schema.Table,
	work func(context.Context, adapterStableNetworkSource) error,
) error {
	if session == nil {
		return errors.New("SQL Server migration snapshot stable session is required")
	}
	if isNilInterface(work) {
		return errors.New("SQL Server migration snapshot stable reader callback is required")
	}
	if task.Schema != table.Schema || task.Table != table.Name {
		return errors.New("SQL Server migration snapshot task differs from table catalog")
	}
	return session.RunReader(ctx, task, func(readerCtx context.Context, queryer SQLServerStrictSnapshotQueryer) error {
		transaction, ok := queryer.(*sql.Tx)
		if !ok || transaction == nil {
			return errors.New("SQL Server migration snapshot reader returned an unexpected queryer")
		}
		view, err := newAdapterRetainedStableRelationalView(source, &adapterSQLTransactionStableView{
			transaction: transaction, engine: "mssql",
		})
		if err != nil {
			return err
		}
		if err := view.bindTableScope(table); err != nil {
			return err
		}
		view.sqlServerStrict = true
		view.sqlServerSnapshot = true
		return work(readerCtx, view)
	})
}

func (session *SQLServerMigrationSnapshotSession) Close(ctx context.Context) error {
	if session == nil {
		return errors.New("SQL Server migration snapshot session is nil")
	}
	if ctx == nil {
		return errors.New("SQL Server migration snapshot cleanup context is required")
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		readers := make([]*sqlServerMigrationSnapshotReader, 0, len(session.readers))
		for reader := range session.readers {
			readers = append(readers, reader)
		}
		snapshot := session.snapshot
		preserve := session.preserve
		session.mu.Unlock()
		go func() {
			for _, reader := range readers {
				reader.cancel()
			}
			for _, reader := range readers {
				<-reader.done
			}
			var failures []error
			if snapshot != nil {
				if err := snapshot.Close(); err != nil {
					failures = append(failures, err)
				}
			}
			if !preserve {
				cleanupCtx, cancel := context.WithTimeout(
					context.Background(),
					strictConsistencyCleanupTimeout,
				)
				defer cancel()
				drop := session.dropSnapshot
				if drop == nil {
					drop = session.dropVerifiedSnapshot
				}
				if err := drop(cleanupCtx); err != nil {
					failures = append(failures, err)
				}
			}
			if len(failures) != 0 {
				session.closeErr = fmt.Errorf(
					"close SQL Server migration snapshot session: %w",
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
			"SQL Server migration snapshot cleanup did not finish before context ended: %w",
			context.Cause(ctx),
		)
	}
}

// dropVerifiedSnapshot refuses to turn state data into an arbitrary DROP
// DATABASE command. The session reference must be this run's deterministic
// identity and still be an online read-only snapshot of the configured source.
func (session *SQLServerMigrationSnapshotSession) dropVerifiedSnapshot(
	ctx context.Context,
) error {
	if session.source == nil {
		return errors.New(
			"SQL Server migration snapshot source cleanup handle is unavailable",
		)
	}
	expected := sqlServerMigrationSnapshotName(
		session.runID,
		session.sourceDatabase,
	)
	if session.reference != expected {
		return errors.New(
			"refuse cleanup of SQL Server migration snapshot with a non-deterministic identity",
		)
	}
	var record sqlServerMigrationSnapshotRecord
	err := session.source.QueryRowContext(ctx, `
		SELECT source_database_id, state_desc, is_read_only, create_date
		FROM sys.databases
		WHERE name = @p1
	`, session.reference).Scan(
		&record.sourceDatabaseID,
		&record.state,
		&record.readOnly,
		&record.capturedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"verify SQL Server migration snapshot cleanup identity: %w",
			err,
		)
	}
	if err := session.requireVerifiedCleanupRecord(record); err != nil {
		return err
	}
	quoted, err := quoteSQLServerStrictIdentifier(session.reference)
	if err != nil {
		return err
	}
	if _, err := session.source.ExecContext(ctx, "DROP DATABASE "+quoted); err != nil {
		return fmt.Errorf(
			"drop SQL Server migration snapshot %q: %w",
			session.reference,
			err,
		)
	}
	return nil
}

func (session *SQLServerMigrationSnapshotSession) requireVerifiedCleanupRecord(
	record sqlServerMigrationSnapshotRecord,
) error {
	if record.sourceDatabaseID != session.sourceDatabaseID ||
		record.state != "ONLINE" || !record.readOnly {
		return errors.New(
			"refuse cleanup because SQL Server database is not the verified migration snapshot",
		)
	}
	if !record.capturedAt.UTC().Equal(session.captured.UTC()) {
		return errors.New(
			"refuse cleanup because SQL Server migration snapshot creation time differs from the opened snapshot",
		)
	}
	return nil
}

var _ StrictConsistencyOpener = (*SQLServerMigrationSnapshotOpener)(nil)
var _ PlannedStrictConsistencyOpener = (*SQLServerMigrationSnapshotOpener)(nil)
var _ StrictConsistencySession = (*SQLServerMigrationSnapshotSession)(nil)
var _ SQLServerMigrationSnapshotFinalizationSession = (*SQLServerMigrationSnapshotSession)(nil)
