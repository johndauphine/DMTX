package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4SQLServerMigrationLiveBackend makes the two crash-recovery boundaries
// observable against a real SQL Server snapshot. Saving evidence happens only
// after the snapshot exists, while refusing the first cleanup intent happens
// only after every table checkpoint is durable.
type stage4SQLServerMigrationLiveBackend struct {
	Stage4StateBackend
	cleanup state.StrictMigrationCleanupBackend

	mutateOnce  sync.Once
	mutate      func() error
	mutationErr error

	mu             sync.Mutex
	failNextIntent bool
}

func (backend *stage4SQLServerMigrationLiveBackend) SaveStrictSnapshotEvidence(
	evidence state.StrictSnapshotEvidence,
) error {
	if err := backend.Stage4StateBackend.SaveStrictSnapshotEvidence(evidence); err != nil {
		return err
	}
	backend.mutateOnce.Do(func() {
		if backend.mutate == nil {
			backend.mutationErr = errors.New("SQL Server migration live source mutation is unavailable")
			return
		}
		backend.mutationErr = backend.mutate()
	})
	return backend.mutationErr
}

func (backend *stage4SQLServerMigrationLiveBackend) SaveStrictMigrationCleanupIntent(
	intent state.StrictMigrationCleanupIntent,
) error {
	backend.mu.Lock()
	fail := backend.failNextIntent
	backend.failNextIntent = false
	backend.mu.Unlock()
	if fail {
		return errors.New("injected SQL Server migration cleanup-intent state failure")
	}
	return backend.cleanup.SaveStrictMigrationCleanupIntent(intent)
}

func (backend *stage4SQLServerMigrationLiveBackend) LoadStrictMigrationCleanupIntent(
	runID string,
	epochID string,
) (state.StrictMigrationCleanupIntent, bool, error) {
	return backend.cleanup.LoadStrictMigrationCleanupIntent(runID, epochID)
}

func openSQLServerStrictLiveSource(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	if dsn == "" || os.Getenv("DMTX_TEST_MSSQL_CA") == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server strict route",
		)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_MSSQL_DSN: %v", err)
	}
	if parsed.Query().Get("encrypt") != "true" {
		t.Fatal("DMTX_TEST_MSSQL_DSN must require verified TLS")
	}
	database, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open SQL Server strict source: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	database.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server strict source: %v", err)
	}

	table := "dmtx_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quoted := "[dbo].[" + table + "]"
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, payload NVARCHAR(40) NOT NULL)`,
		quoted,
	)); err != nil {
		t.Fatalf("create SQL Server strict fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+quoted,
		); err != nil {
			t.Errorf("drop SQL Server strict fixture: %v", err)
		}
	})
	if _, err := database.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (1,'a'),(2,'b'),(3,'c')`,
		quoted,
	)); err != nil {
		t.Fatalf("seed SQL Server strict fixture: %v", err)
	}
	return database, "dbo", table
}

// TestSQLServerStrictTableLockLive proves the Section 10 SQL Server table
// contract against a real server, including the part that differs from every
// other engine: writes to the locked table are expected to wait. The test
// asserts the block rather than tolerating it, because a strict table view that
// silently allowed writes would not be strict at all.
func TestSQLServerStrictTableLockLive(t *testing.T) {
	source, namespace, table := openSQLServerStrictLiveSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opener, err := NewSQLServerStrictConsistencyOpener(source, namespace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mssql-strict-live",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      state.TaskKey{Type: "table-copy", Table: table},
			AttemptID: "attempt-1",
		}},
	})
	if err != nil {
		t.Fatalf("open SQL Server strict view: %v", err)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		t.Fatalf("capture SQL Server strict evidence: %v", err)
	}
	if len(capture.Tables) != 1 ||
		capture.Tables[0].ExactSourceRowCount != 3 {
		t.Fatalf("SQL Server capture = %#v", capture)
	}
	if err := validateSnapshotReference(
		capture.Tables[0].SnapshotReference,
	); err != nil {
		t.Fatalf("snapshot reference rejected by the core: %v", err)
	}

	// A writer must wait while the shared lock is held. A short deadline turns
	// "waits" into an observable fact rather than an untested claim.
	quoted := "[" + namespace + "].[" + table + "]"
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 3*time.Second)
	_, writeErr := source.ExecContext(blockedCtx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (99,'blocked')`,
		quoted,
	))
	blockedCancel()
	if writeErr == nil {
		t.Fatal("a writer committed while the strict table lock was held")
	}
	if !errors.Is(writeErr, context.DeadlineExceeded) {
		t.Logf("writer failed with %v (accepted: it did not commit)", writeErr)
	}

	// Releasing the view must release the source, or the strict route would
	// leave the table locked for the life of the connection pool.
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close SQL Server strict view: %v", err)
	}
	releasedCtx, releasedCancel := context.WithTimeout(ctx, 20*time.Second)
	defer releasedCancel()
	if _, err := source.ExecContext(releasedCtx, fmt.Sprintf(
		`INSERT INTO %s (id, payload) VALUES (99,'after-close')`,
		quoted,
	)); err != nil {
		t.Fatalf("SQL Server strict view did not release its lock: %v", err)
	}
}

// TestStage4SQLServerTableStrictComposedMSSQLLiveTLS closes the one remaining
// source/scope boundary in the compact strict live matrix: the production
// aggregate SQL Server table-strict runner, including its real native target
// writer. It deliberately uses the already independently certified MSSQL
// upsert/validation target instead of multiplying this source-contract test
// across every admitted target. Cross-target writer capability is covered by
// TestStage4PostgresStrictCrossTargetLiveTLS and the target-specific live
// writer suites; this sentinel owns the held-table source view, its durable
// evidence, and the required concurrent-writer behavior.
func TestStage4SQLServerTableStrictComposedMSSQLLiveTLS(t *testing.T) {
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
	// Register this first: temporary-database cleanups are LIFO, so the audit
	// runs after both fixture databases have been dropped.
	t.Cleanup(func() {
		assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
		assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
	})
	sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		base,
		&sourceCleanup,
	)
	targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		base,
		&targetCleanup,
	)
	sourceDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"table-strict source",
		sourceEndpoint,
	)
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"table-strict target",
		targetEndpoint,
	)
	table := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	for _, fixture := range []struct {
		database *sql.DB
		endpoint config.Endpoint
		role     string
	}{
		{sourceDatabase, sourceEndpoint, "source"},
		{targetDatabase, targetEndpoint, "target"},
	} {
		if _, err := fixture.database.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified(fixture.endpoint.Schema, table)+
				" ([id] BIGINT NOT NULL PRIMARY KEY, [payload] VARCHAR(40) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL)",
		); err != nil {
			t.Fatalf("create SQL Server table-strict %s table: %v", fixture.role, err)
		}
	}
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, table)+
			" ([id], [payload]) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
	); err != nil {
		t.Fatalf("seed SQL Server table-strict source: %v", err)
	}

	rawBackend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "mssql-table-strict-composed-" + table
	initializeStage4SQLServerStrictLiveRun(
		t,
		rawBackend,
		runID,
		sourceEndpoint,
		targetEndpoint,
		time.Now().UTC().Add(-time.Minute),
	)
	backend := &stage4SQLServerTableStrictLiveBackend{
		Stage4StateBackend: rawBackend,
		source:             sourceDatabase,
		schema:             sourceEndpoint.Schema,
		table:              table,
	}
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, false),
	}
	cfg := stage4AdapterTestConfig(t, sourceEndpoint.Password, targetEndpoint.Password)
	cfg.Source, cfg.Target = sourceEndpoint, targetEndpoint
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{table}
	cfg.Migration.ConnectionLimit = 4
	cfg.Migration.Workers = 2
	cfg.Migration.ChunkSize = 1
	cfg.Migration.Partitions = 1
	cfg.Migration.ReaderParallelism = 1
	cfg.Migration.WriterParallelism = 1
	cfg.Migration.ReadAhead = 1
	cfg.Migration.MaxRetries = 0
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	if cfg.Source.SSLMode != "verify-full" || cfg.Target.SSLMode != "verify-full" ||
		cfg.Source.TLSCAFile == "" || cfg.Target.TLSCAFile == "" {
		t.Fatal("production SQL Server strict endpoints lost verified-TLS authority")
	}

	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("run SQL Server table-strict composed route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("SQL Server table-strict result = %#v", result)
	}
	if backend.mutationErr != nil {
		t.Fatalf("SQL Server table-strict concurrent source writer: %v", backend.mutationErr)
	}
	releasedCtx, releasedCancel := context.WithTimeout(ctx, 20*time.Second)
	defer releasedCancel()
	if _, err := sourceDatabase.ExecContext(
		releasedCtx,
		"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, table)+
			" ([id], [payload]) VALUES (4, 'after-release')",
	); err != nil {
		t.Fatalf("SQL Server table-strict writer did not enter after source-view release: %v", err)
	}
	for _, check := range []struct {
		database *sql.DB
		endpoint config.Endpoint
		role     string
		want     int
	}{
		{sourceDatabase, sourceEndpoint, "source", 4},
		{targetDatabase, targetEndpoint, "target", 3},
	} {
		var count int
		if err := check.database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(check.endpoint.Schema, table),
		).Scan(&count); err != nil || count != check.want {
			t.Fatalf("SQL Server table-strict %s count=%d err=%v, want %d", check.role, count, err, check.want)
		}
	}

	work, _, err := rawBackend.ListWork(runID)
	if err != nil {
		t.Fatalf("list SQL Server table-strict work: %v", err)
	}
	for _, item := range work {
		if item.Key.Type != stage4AdapterNetworkTaskType {
			continue
		}
		attempt, attemptErr := BuildStrictConsistencyAttemptID(item.Key, item.TopologyHash, 0)
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		evidence, found, loadErr := rawBackend.LoadStrictSnapshotEvidence(runID, item.Key, attempt)
		if loadErr != nil || !found || evidence.SourceEngine != "mssql" ||
			evidence.Scope != state.StrictSnapshotTable || evidence.ExactSourceRowCount != 3 {
			t.Fatalf("SQL Server table-strict evidence=%#v found=%t err=%v", evidence, found, loadErr)
		}
		return
	}
	t.Fatal("SQL Server table-strict route did not persist network evidence")
}

// stage4SQLServerTableStrictLiveBackend observes the requirement unique to
// SQL Server table scope: after same-view evidence is durable, a competing
// source writer must wait instead of entering the strict epoch. The short
// deadline makes that wait observable while allowing the composed transfer to
// retain its lock and complete normally.
type stage4SQLServerTableStrictLiveBackend struct {
	Stage4StateBackend

	source *sql.DB
	schema string
	table  string

	mutationOnce sync.Once
	mutationErr  error
}

func (backend *stage4SQLServerTableStrictLiveBackend) SaveStrictSnapshotEvidence(
	evidence state.StrictSnapshotEvidence,
) error {
	if err := backend.Stage4StateBackend.SaveStrictSnapshotEvidence(evidence); err != nil {
		return err
	}
	backend.mutationOnce.Do(func() {
		if backend.source == nil || backend.schema == "" || backend.table == "" {
			backend.mutationErr = errors.New("SQL Server table-strict live writer is unavailable")
			return
		}
		// SQL Server queues a competing X lock behind the held strict-table
		// view. Bound that wait on the server, not by client cancellation: an
		// unfinished X waiter can fairly queue later readers and create a
		// test-only deadlock. The post-run write below separately proves release.
		writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		connection, err := backend.source.Conn(writeCtx)
		if err != nil {
			backend.mutationErr = fmt.Errorf("acquire SQL Server table-strict writer connection: %w", err)
			return
		}
		defer connection.Close()
		if _, err := connection.ExecContext(writeCtx, "SET LOCK_TIMEOUT 500"); err != nil {
			backend.mutationErr = fmt.Errorf("set SQL Server table-strict writer lock timeout: %w", err)
			return
		}
		_, err = connection.ExecContext(
			writeCtx,
			"INSERT INTO "+sqlServerQualified(backend.schema, backend.table)+
				" ([id], [payload]) VALUES (4, 'blocked')",
		)
		if err == nil {
			backend.mutationErr = errors.New("SQL Server table-strict writer committed while the held source view was active")
			return
		}
		if containsSQLServerErrorNumber(stage4SQLServerErrorNumbers(err), 1222) {
			return
		}
		backend.mutationErr = fmt.Errorf("SQL Server table-strict writer returned an unexpected error: %w", err)
	})
	return backend.mutationErr
}

// initializeStage4SQLServerStrictLiveRun binds lifecycle state to canonical
// SQL Server endpoint authority. The generic fixture represents PostgreSQL
// source identity and would make this real strict source fail before opening
// its stable view.
func initializeStage4SQLServerStrictLiveRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	source config.Endpoint,
	target config.Endpoint,
	started time.Time,
) {
	t.Helper()
	sourceIdentity, err := config.NetworkEndpointWorkloadIdentity(source)
	if err != nil {
		t.Fatalf("identify SQL Server table-strict source workload: %v", err)
	}
	targetIdentity, err := config.NetworkEndpointWorkloadIdentity(target)
	if err != nil {
		t.Fatalf("identify SQL Server table-strict target workload: %v", err)
	}
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         source.Database,
		Target:         target.Database,
		SourceEngine:   "mssql",
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      started,
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
}

// TestSQLServerStrictRejectsMigrationScopeLive proves the table-scope opener
// refuses migration scope instead of quietly serving a table lock in place of
// the database snapshot that scope requires.
func TestSQLServerStrictRejectsMigrationScopeLive(t *testing.T) {
	source, namespace, table := openSQLServerStrictLiveSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opener, err := NewSQLServerStrictConsistencyOpener(source, namespace)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opener.OpenStrictConsistency(ctx, StrictConsistencyOpenRequest{
		RunID:        "mssql-strict-scope",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotMigration,
		ProcessEpoch: "epoch-1",
		Tables: []StrictConsistencyTable{{
			Task:      state.TaskKey{Type: "table-copy", Table: table},
			AttemptID: "attempt-1",
		}},
	})
	if session != nil {
		_ = session.Close(context.Background())
	}
	if err == nil || !strings.Contains(err.Error(), "cannot serve scope") {
		t.Fatalf("SQL Server migration-scope error = %v", err)
	}
}

// TestSQLServerMigrationSnapshotLiveTLS exercises the actual database-snapshot
// opener against a user database. The configured source DSN intentionally
// points to master for shared fixtures, but master cannot be snapshotted, so
// this test creates a private source database and derives its configured
// endpoint from the same verified-TLS authority.
func TestSQLServerMigrationSnapshotLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server migration snapshot lifecycle",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse SQL Server migration snapshot admin DSN: %v", err)
	}
	adminQuery := parsed.Query()
	adminQuery.Set("database", "master")
	parsed.RawQuery = adminQuery.Encode()
	admin, err := sql.Open("sqlserver", parsed.String())
	if err != nil {
		t.Fatalf("open SQL Server migration snapshot admin database: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close SQL Server migration snapshot admin database: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server migration snapshot admin database: %v", err)
	}

	sourceName := "dmtx_snapshot_live_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quotedSource, err := quoteSQLServerStrictIdentifier(sourceName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quotedSource); err != nil {
		t.Fatalf("create SQL Server migration snapshot source database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx,
			"ALTER DATABASE "+quotedSource+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedSource,
		); err != nil {
			t.Errorf("drop SQL Server migration snapshot source database: %v", err)
		}
	})

	sourceEndpoint.Database = sourceName
	source, err := engine.OpenSQLServer2022Source(ctx, sourceEndpoint)
	if err != nil {
		t.Fatalf("open verified-TLS SQL Server migration snapshot source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQL Server migration snapshot source: %v", err)
		}
	})
	table := "items"
	quotedTable := sqlServerQualified("dbo", table)
	if _, err := source.ExecContext(ctx,
		"CREATE TABLE "+quotedTable+" ("+
			"id BIGINT NOT NULL PRIMARY KEY, "+
			"nullable_text NVARCHAR(40) NULL, "+
			"payload VARBINARY(32) NULL)",
	); err != nil {
		t.Fatalf("create SQL Server migration snapshot source table: %v", err)
	}
	if _, err := source.ExecContext(ctx,
		"INSERT INTO "+quotedTable+" (id, nullable_text, payload) VALUES "+
			"(1, N'one', 0x0102), (2, NULL, NULL), (3, N'three', 0x0A0B)",
	); err != nil {
		t.Fatalf("seed SQL Server migration snapshot source table: %v", err)
	}

	runID := "mssql-snapshot-live"
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: table,
	}
	opener, err := NewSQLServerMigrationSnapshotOpener(source, sourceEndpoint, "dbo")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := opener.OpenPlannedStrictConsistency(ctx,
		PlannedStrictConsistencyOpenRequest{
			RunID:        runID,
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "initial-process",
			Tasks:        []state.TaskKey{task},
		},
	)
	if err != nil {
		t.Fatalf("create/open verified SQL Server migration snapshot: %v", err)
	}
	initial, ok := opened.(*SQLServerMigrationSnapshotSession)
	if !ok || initial == nil {
		t.Fatalf("migration snapshot session = %T, want *SQLServerMigrationSnapshotSession", opened)
	}
	capture, err := initial.CaptureSameViewEvidence(ctx)
	if err != nil {
		t.Fatalf("capture SQL Server migration snapshot evidence: %v", err)
	}
	if len(capture.Tables) != 1 || capture.Tables[0].ExactSourceRowCount != 3 ||
		capture.MigrationSnapshotReference != initial.reference ||
		!capture.MigrationCapturedAt.Equal(initial.captured) {
		t.Fatalf("SQL Server migration snapshot capture = %#v", capture)
	}
	assertSnapshotRows := func(session *SQLServerMigrationSnapshotSession, want int64) {
		t.Helper()
		var got int64
		if err := session.RunReader(ctx, task, func(
			readerCtx context.Context,
			queryer SQLServerStrictSnapshotQueryer,
		) error {
			return queryer.QueryRowContext(
				readerCtx,
				"SELECT COUNT_BIG(*) FROM "+quotedTable,
			).Scan(&got)
		}); err != nil {
			t.Fatalf("read SQL Server migration snapshot rows: %v", err)
		}
		if got != want {
			t.Fatalf("SQL Server migration snapshot rows = %d, want %d", got, want)
		}
	}
	assertSnapshotRows(initial, 3)
	if _, err := source.ExecContext(ctx,
		"INSERT INTO "+quotedTable+" (id, nullable_text, payload) VALUES (99, N'after snapshot', 0xFFFF)",
	); err != nil {
		t.Fatalf("mutate SQL Server source after snapshot: %v", err)
	}
	assertSnapshotRows(initial, 3)

	owner := state.StrictMigrationSnapshot{
		RunID:             runID,
		EpochID:           initial.epoch,
		SourceEngine:      "mssql",
		SnapshotReference: initial.reference,
		ProcessEpoch:      "initial-process",
		CapturedAt:        initial.captured,
	}
	if err := initial.PreserveSnapshotForResume(); err != nil {
		t.Fatalf("preserve SQL Server migration snapshot for resume: %v", err)
	}
	if err := initial.Close(ctx); err != nil {
		t.Fatalf("close preserved SQL Server migration snapshot: %v", err)
	}
	var preserved int
	if err := admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		owner.SnapshotReference,
	).Scan(&preserved); err != nil || preserved != 1 {
		t.Fatalf("preserved SQL Server migration snapshot exists=%d err=%v", preserved, err)
	}

	resumedRaw, err := opener.OpenPlannedStrictConsistency(ctx,
		PlannedStrictConsistencyOpenRequest{
			RunID:                     runID,
			SourceEngine:              StrictConsistencyMSSQL,
			Scope:                     state.StrictSnapshotMigration,
			Resume:                    true,
			ProcessEpoch:              "resume-process",
			Tasks:                     []state.TaskKey{task},
			RequiredMigrationSnapshot: &owner,
		},
	)
	if err != nil {
		t.Fatalf("reopen verified SQL Server migration snapshot: %v", err)
	}
	resumed, ok := resumedRaw.(*SQLServerMigrationSnapshotSession)
	if !ok || resumed == nil || resumed.reference != owner.SnapshotReference ||
		!resumed.captured.Equal(owner.CapturedAt) {
		t.Fatalf("resumed SQL Server migration snapshot = %#v", resumedRaw)
	}
	assertSnapshotRows(resumed, 3)
	if err := resumed.Close(ctx); err != nil {
		t.Fatalf("drop resumed SQL Server migration snapshot: %v", err)
	}
	var remaining int
	if err := admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		owner.SnapshotReference,
	).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("dropped SQL Server migration snapshot exists=%d err=%v", remaining, err)
	}
}

// TestSQLServerMigrationStrictComposedPostgresLiveTLS exercises the complete
// migration-scope Stage 4 path rather than only the primitive snapshot opener.
// It deliberately fails the final cleanup-intent write after both table
// checkpoints. A fresh resume must reopen that exact durable snapshot, finish
// the cleanup receipt/DROP protocol, and leave no snapshot orphan. The source
// mutation is triggered only after snapshot evidence is persisted, proving the
// target counts remain bound to the single shared immutable view.
func TestSQLServerMigrationStrictComposedPostgresLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" || caPath == "" || postgresDSN == "" {
		t.Skip("set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, and DMTX_TEST_POSTGRES_DSN to run the composed SQL Server migration-strict sentinel")
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL migration-strict target DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require verified TLS")
	}
	targetCAFile := stage4PostgresDeleteLiveCAFile(t, postgresDSN)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse SQL Server migration-strict admin DSN: %v", err)
	}
	adminQuery := parsed.Query()
	adminQuery.Set("database", "master")
	parsed.RawQuery = adminQuery.Encode()
	admin, err := sql.Open("sqlserver", parsed.String())
	if err != nil {
		t.Fatalf("open SQL Server migration-strict admin database: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close SQL Server migration-strict admin database: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server migration-strict admin database: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceName := "dmtx_s4_migration_live_" + suffix
	quotedSource, err := quoteSQLServerStrictIdentifier(sourceName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quotedSource); err != nil {
		t.Fatalf("create SQL Server migration-strict source database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx,
			"ALTER DATABASE "+quotedSource+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedSource,
		); err != nil {
			t.Errorf("drop SQL Server migration-strict source database: %v", err)
		}
	})

	sourceEndpoint.Database = sourceName
	source, err := engine.OpenSQLServer2022Source(ctx, sourceEndpoint)
	if err != nil {
		t.Fatalf("open verified-TLS SQL Server migration-strict source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQL Server migration-strict source: %v", err)
		}
	})
	tables := []string{"orders", "items"}
	for index, table := range tables {
		qualified := sqlServerQualified("dbo", table)
		if _, err := source.ExecContext(ctx,
			"CREATE TABLE "+qualified+" ("+
				"id BIGINT NOT NULL PRIMARY KEY, "+
				"payload BIGINT NOT NULL)",
		); err != nil {
			t.Fatalf("create SQL Server migration-strict source table %s: %v", table, err)
		}
		if _, err := source.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO %s (id, payload) VALUES (1, %d), (2, %d), (3, %d)",
			qualified,
			index*100+11,
			index*100+22,
			index*100+33,
		)); err != nil {
			t.Fatalf("seed SQL Server migration-strict source table %s: %v", table, err)
		}
	}

	targetNamespace := "dmtx_mssql_migration_strict_" + suffix
	targetEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      postgresConfig.Host,
		Port:      int(postgresConfig.Port),
		Database:  postgresConfig.Database,
		User:      postgresConfig.User,
		Password:  postgresConfig.Password,
		Schema:    targetNamespace,
		SSLMode:   "verify-full",
		TLSCAFile: targetCAFile,
	}
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build PostgreSQL migration-strict target DSN: %v", err)
	}
	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open verified-TLS PostgreSQL migration-strict target: %v", err)
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Errorf("close PostgreSQL migration-strict target: %v", err)
		}
	})
	if err := target.PingContext(ctx); err != nil {
		t.Fatalf("ping verified-TLS PostgreSQL migration-strict target: %v", err)
	}
	if _, err := target.ExecContext(ctx,
		"CREATE SCHEMA "+postgresIdentifier(targetNamespace),
	); err != nil {
		t.Fatalf("create PostgreSQL migration-strict target schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := target.ExecContext(cleanupCtx,
			"DROP SCHEMA IF EXISTS "+postgresIdentifier(targetNamespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL migration-strict target schema: %v", err)
		}
	})
	for _, table := range tables {
		if _, err := target.ExecContext(ctx,
			"CREATE TABLE "+postgresQualified(targetNamespace, table)+" ("+
				"id BIGINT NOT NULL PRIMARY KEY, "+
				"payload BIGINT NOT NULL)",
		); err != nil {
			t.Fatalf("create PostgreSQL migration-strict target table %s: %v", table, err)
		}
	}

	cfg, err := config.Parse([]byte(`
source:
  type: mssql
  host: placeholder.invalid
  database: placeholder
  user: placeholder
target:
  type: postgres
  host: placeholder.invalid
  database: placeholder
  user: placeholder
migration:
  target_mode: upsert
  include_tables:
    - orders
    - items
  connection_limit: 3
  workers: 2
  chunk_size: 1
  partitions: 1
  reader_parallelism: 1
  writer_parallelism: 1
  read_ahead: 1
  max_retries: 0
  runtime_tuning: false
  strict_consistency: true
  strict_consistency_scope: migration
  validation:
    mode: count_only
`))
	if err != nil {
		t.Fatalf("parse SQL Server migration-strict configuration: %v", err)
	}
	cfg.Source = sourceEndpoint
	cfg.Target = targetEndpoint
	cfg.Migration.IncludeTables = append([]string(nil), tables...)
	if cfg.Target.SSLMode != "verify-full" || cfg.Target.TLSCAFile != targetCAFile {
		t.Fatalf("production PostgreSQL target endpoint lost verified TLS authority: %#v", cfg.Target)
	}

	runID := "mssql-migration-composed-" + suffix
	rawBackend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	if err := rawBackend.InitializeRun(state.Run{
		ID:             runID,
		Source:         sourceName,
		Target:         targetEndpoint.Database,
		SourceEngine:   "mssql",
		SourceIdentity: "mssql-live:" + sourceName,
		TargetIdentity: "postgres-live:" + targetEndpoint.Database,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running SQL Server migration strict fixture",
		StartedAt:      time.Now().UTC(),
	}, "mssql-migration-strict-live"); err != nil {
		t.Fatalf("initialize SQL Server migration-strict state: %v", err)
	}
	cleanupBackend, ok := any(rawBackend).(state.StrictMigrationCleanupBackend)
	if !ok {
		t.Fatalf("%T lacks SQL Server migration cleanup state", rawBackend)
	}
	liveBackend := &stage4SQLServerMigrationLiveBackend{
		Stage4StateBackend: rawBackend,
		cleanup:            cleanupBackend,
		failNextIntent:     true,
		mutate: func() error {
			for _, table := range tables {
				if _, err := source.ExecContext(ctx, fmt.Sprintf(
					"INSERT INTO %s (id, payload) VALUES (99, 999)",
					sqlServerQualified("dbo", table),
				)); err != nil {
					return fmt.Errorf("write SQL Server source after migration snapshot: %w", err)
				}
			}
			return nil
		},
	}
	run := stage4LifecycleRunContext(t, liveBackend, runID, false)
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	result, err := Execute(ctx, cfg, observer)
	if err == nil || ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "injected SQL Server migration cleanup-intent state failure") {
		t.Fatalf("first composed SQL Server migration-strict result=%#v error=%v", result, err)
	}
	if liveBackend.mutationErr != nil {
		t.Fatalf("post-snapshot SQL Server source mutation: %v", liveBackend.mutationErr)
	}

	owner, found, err := rawBackend.LoadLatestStrictMigrationSnapshot(runID)
	if err != nil || !found {
		t.Fatalf("load durable SQL Server migration snapshot owner: found=%t err=%v", found, err)
	}
	var snapshotCount int
	if err := admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		owner.SnapshotReference,
	).Scan(&snapshotCount); err != nil || snapshotCount != 1 {
		t.Fatalf("preserved SQL Server migration snapshot exists=%d err=%v", snapshotCount, err)
	}

	work, _, err := rawBackend.ListWork(runID)
	if err != nil {
		t.Fatalf("list SQL Server migration-strict work: %v", err)
	}
	networkWork := make([]state.WorkTask, 0, len(tables))
	for _, item := range work {
		if item.Key.Type == stage4AdapterNetworkTaskType {
			networkWork = append(networkWork, item)
		}
	}
	if len(networkWork) != len(tables) {
		t.Fatalf("SQL Server migration-strict network work = %#v, want two tables", networkWork)
	}
	for _, item := range networkWork {
		attemptID, err := BuildStrictConsistencyAttemptID(item.Key, item.TopologyHash, 0)
		if err != nil {
			t.Fatal(err)
		}
		evidence, found, err := rawBackend.LoadStrictSnapshotEvidence(runID, item.Key, attemptID)
		if err != nil || !found || evidence.Scope != state.StrictSnapshotMigration ||
			evidence.ExactSourceRowCount != 3 || evidence.SnapshotReference != owner.SnapshotReference ||
			!evidence.CapturedAt.UTC().Equal(owner.CapturedAt.UTC()) {
			t.Fatalf("SQL Server migration-strict evidence for %s = %#v found=%t err=%v", item.Key.Table, evidence, found, err)
		}
	}
	for _, table := range tables {
		var sourceCount, targetCount int
		if err := source.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified("dbo", table),
		).Scan(&sourceCount); err != nil {
			t.Fatalf("count changed SQL Server source table %s: %v", table, err)
		}
		if err := target.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+postgresQualified(targetNamespace, table),
		).Scan(&targetCount); err != nil {
			t.Fatalf("count PostgreSQL snapshot target table %s: %v", table, err)
		}
		if sourceCount != 4 || targetCount != 3 {
			t.Fatalf("snapshot-bound table %s source=%d target=%d, want source=4 target=3", table, sourceCount, targetCount)
		}
	}

	// A new observer models a process boundary. It carries no in-memory
	// snapshot handle, so the resume path must authenticate/reopen the durable
	// owner above before it may issue the final DROP.
	run.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    run,
	}
	result, err = ExecuteResume(ctx, cfg, CompletedTableCheckpoints{}, resumeObserver)
	if err != nil {
		t.Fatalf("resume SQL Server migration-strict cleanup from durable snapshot: %v", err)
	}
	if !result.Validated || result.Tables != 2 || result.Rows != 6 {
		t.Fatalf("resumed SQL Server migration-strict result = %#v", result)
	}
	if err := admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		owner.SnapshotReference,
	).Scan(&snapshotCount); err != nil || snapshotCount != 0 {
		t.Fatalf("SQL Server migration snapshot orphan count=%d err=%v", snapshotCount, err)
	}
	intent, found, err := cleanupBackend.LoadStrictMigrationCleanupIntent(runID, owner.EpochID)
	if err != nil || !found || !stage4SQLServerMigrationCleanupIntentMatchesOwner(intent, owner) {
		t.Fatalf("SQL Server migration-strict final cleanup intent=%#v found=%t err=%v", intent, found, err)
	}
}
