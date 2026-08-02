package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4SQLServerStrictDeleteCompositionLiveTLS exercises the only newly
// certified strict-delete cell, MSSQL-to-MSSQL.  The table child proves that a
// post-evidence writer cannot enter the held-table strict epoch.  The
// migration child instead commits that writer after snapshot evidence, fails
// the final cleanup-intent persistence, and proves a fresh process resumes the
// exact snapshot without re-reading the changed live source.
func TestStage4SQLServerStrictDeleteCompositionLiveTLS(t *testing.T) {
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	t.Run("table scope holds source complete-key view", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
		// Register this before the temporary-database helpers so their LIFO
		// cleanup has dropped both databases before the post-test audit runs.
		t.Cleanup(func() {
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
		})
		sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
		targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)

		sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete table source", sourceEndpoint)
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete table target", targetEndpoint)
		tableName := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, tableName)
		createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, tableName)
		seedSQLServerStrictDeleteLiveRows(t, ctx, sourceDatabase, sourceEndpoint, tableName, false)
		seedSQLServerStrictDeleteLiveRows(t, ctx, targetDatabase, targetEndpoint, tableName, true)

		rawBackend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		runID := "mssql-strict-delete-table-" + tableName
		initializeStage4SQLServerStrictDeleteLifecycleRun(
			t,
			rawBackend,
			runID,
			sourceEndpoint,
			targetEndpoint,
			time.Now().UTC().Add(-time.Minute),
		)
		writerBlocked := &stage4SQLServerStrictDeleteLiveBackend{
			stage4SQLServerMigrationLiveBackend: &stage4SQLServerMigrationLiveBackend{
				Stage4StateBackend: rawBackend,
				mutate: func() error {
					writeCtx, writeCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
					defer writeCancel()
					_, writeErr := sourceDatabase.ExecContext(
						writeCtx,
						"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, tableName)+
							" ([tenant_id], [item_id], [payload]) VALUES (1, 4, 'blocked')",
					)
					if writeErr == nil {
						return errors.New("SQL Server table-strict post-evidence source write entered while the strict epoch was active")
					}
					if errors.Is(writeErr, context.DeadlineExceeded) || errors.Is(writeErr, context.Canceled) {
						return nil
					}
					return fmt.Errorf("SQL Server table-strict post-evidence source write returned an unexpected error: %w", writeErr)
				},
			},
			Backend:                rawBackend,
			Stage4AggregateBackend: rawBackend,
			readiness:              rawBackend,
		}
		result, err := SQLServerToSQLServerWithObserver(
			ctx,
			stage4SQLServerStrictDeleteLiveConfig(
				sourceEndpoint,
				targetEndpoint,
				tableName,
				config.StrictConsistencyTable,
			),
			stage4SQLServerStrictDeleteLiveObserver(t, writerBlocked, runID, false),
		)
		if err != nil {
			t.Fatalf("run SQL Server table-strict delete route: %v", err)
		}
		if result != (Result{Tables: 1, Rows: 3, Validated: true}) || writerBlocked.mutationErr != nil {
			t.Fatalf("table-strict delete result=%#v source writer=%v", result, writerBlocked.mutationErr)
		}
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, tableName, 3, 3)
		assertSQLServerStrictDeleteLiveEvidence(t, rawBackend, runID, sourceEndpoint.Schema, tableName, state.StrictSnapshotTable, 3)
		assertSQLServerStrictDeleteLiveJournalAndReceipt(t, ctx, rawBackend, runID, sourceEndpoint.Schema, tableName, targetDatabase)
	})

	t.Run("migration scope snapshot resume excludes post-evidence commit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
		// See the table-scope child: the audit must run after, not before, the
		// helper-owned DROP DATABASE cleanup callbacks.
		t.Cleanup(func() {
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
		})
		sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
		targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)

		sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete migration source", sourceEndpoint)
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete migration target", targetEndpoint)
		tableName := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, tableName)
		createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, tableName)
		seedSQLServerStrictDeleteLiveRows(t, ctx, sourceDatabase, sourceEndpoint, tableName, false)
		seedSQLServerStrictDeleteLiveRows(t, ctx, targetDatabase, targetEndpoint, tableName, true)

		rawBackend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		runID := "mssql-strict-delete-migration-" + tableName
		initializeStage4SQLServerStrictDeleteLifecycleRun(
			t,
			rawBackend,
			runID,
			sourceEndpoint,
			targetEndpoint,
			time.Now().UTC().Add(-time.Minute),
		)
		cleanupBackend, ok := any(rawBackend).(state.StrictMigrationCleanupBackend)
		if !ok {
			t.Fatalf("%T lacks SQL Server migration cleanup state", rawBackend)
		}
		backend := &stage4SQLServerStrictDeleteLiveBackend{
			stage4SQLServerMigrationLiveBackend: &stage4SQLServerMigrationLiveBackend{
				Stage4StateBackend: rawBackend,
				cleanup:            cleanupBackend,
				failNextIntent:     true,
				mutate: func() error {
					_, mutateErr := sourceDatabase.ExecContext(
						ctx,
						"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, tableName)+
							" ([tenant_id], [item_id], [payload]) VALUES (1, 4, 'after-snapshot')",
					)
					return mutateErr
				},
			},
			Backend:                rawBackend,
			Stage4AggregateBackend: rawBackend,
			readiness:              rawBackend,
		}
		cfg := stage4SQLServerStrictDeleteLiveConfig(
			sourceEndpoint,
			targetEndpoint,
			tableName,
			config.StrictConsistencyMigration,
		)
		first, err := SQLServerToSQLServerWithObserver(
			ctx,
			cfg,
			stage4SQLServerStrictDeleteLiveObserver(t, backend, runID, false),
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassState ||
			!strings.Contains(err.Error(), "injected SQL Server migration cleanup-intent state failure") {
			t.Fatalf("first SQL Server migration-strict delete result=%#v error=%v", first, err)
		}
		if backend.mutationErr != nil {
			t.Fatalf("post-snapshot source mutation: %v", backend.mutationErr)
		}
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, tableName, 4, 3)
		assertSQLServerStrictDeleteLiveEvidence(t, rawBackend, runID, sourceEndpoint.Schema, tableName, state.StrictSnapshotMigration, 3)
		assertSQLServerStrictDeleteLiveJournalAndReceipt(t, ctx, rawBackend, runID, sourceEndpoint.Schema, tableName, targetDatabase)
		owner, found, ownerErr := rawBackend.LoadLatestStrictMigrationSnapshot(runID)
		if ownerErr != nil || !found || owner.SnapshotReference == "" || owner.EpochID == "" {
			t.Fatalf("load preserved SQL Server strict-delete migration snapshot owner=%#v found=%t err=%v", owner, found, ownerErr)
		}

		resumedObserver := stage4SQLServerStrictDeleteLiveObserver(t, backend, runID, true)
		// The initial attempt completed the aggregate table publication before
		// the injected cleanup-intent failure. App resume derives this exact
		// checkpoint from the durable ordinary task, so the live harness must do
		// the same instead of trying to republish it.
		resumed, err := ExecuteResume(
			ctx,
			cfg,
			CompletedTableCheckpoints{tableName: {Rows: 3}},
			resumedObserver,
		)
		if err != nil {
			t.Fatalf("resume SQL Server migration-strict delete route: %v", err)
		}
		if resumed != (Result{Tables: 1, Rows: 3, Validated: true}) {
			t.Fatalf("resumed SQL Server migration-strict delete result=%#v", resumed)
		}
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, tableName, 4, 3)
		intent, found, intentErr := cleanupBackend.LoadStrictMigrationCleanupIntent(runID, owner.EpochID)
		if intentErr != nil || !found || !stage4SQLServerMigrationCleanupIntentMatchesOwner(intent, owner) {
			t.Fatalf("SQL Server strict-delete migration cleanup intent=%#v found=%t err=%v", intent, found, intentErr)
		}
		var snapshots int
		if err := sourceDatabase.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
			owner.SnapshotReference,
		).Scan(&snapshots); err != nil || snapshots != 0 {
			t.Fatalf("SQL Server strict-delete migration snapshot cleanup count=%d err=%v", snapshots, err)
		}
	})

	t.Run("migration scope repairs a partial table-publication prefix on the same snapshot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
		t.Cleanup(func() {
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
			assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
		})
		sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
		targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)
		sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete partial migration source", sourceEndpoint)
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "strict-delete partial migration target", targetEndpoint)
		firstTable := "items_a_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		secondTable := "items_b_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		for _, table := range []string{firstTable, secondTable} {
			createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, table)
			createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, table)
			seedSQLServerStrictDeleteLiveRows(t, ctx, sourceDatabase, sourceEndpoint, table, false)
			seedSQLServerStrictDeleteLiveRows(t, ctx, targetDatabase, targetEndpoint, table, true)
		}

		rawBackend := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
		runID := "mssql-strict-delete-partial-" + firstTable
		initializeStage4SQLServerStrictDeleteLifecycleRun(
			t,
			rawBackend,
			runID,
			sourceEndpoint,
			targetEndpoint,
			time.Now().UTC().Add(-time.Minute),
		)
		cleanupBackend, ok := any(rawBackend).(state.StrictMigrationCleanupBackend)
		if !ok {
			t.Fatalf("%T lacks SQL Server migration cleanup state", rawBackend)
		}
		backend := &stage4SQLServerStrictDeleteLiveBackend{
			stage4SQLServerMigrationLiveBackend: &stage4SQLServerMigrationLiveBackend{
				Stage4StateBackend: rawBackend,
				cleanup:            cleanupBackend,
				mutate: func() error {
					_, err := sourceDatabase.ExecContext(
						ctx,
						"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, secondTable)+
							" ([tenant_id], [item_id], [payload]) VALUES (1, 4, 'after-snapshot')",
					)
					return err
				},
			},
			Backend:                rawBackend,
			Stage4AggregateBackend: rawBackend,
			readiness:              rawBackend,
		}
		cfg := stage4SQLServerStrictDeleteLiveConfig(
			sourceEndpoint,
			targetEndpoint,
			firstTable,
			config.StrictConsistencyMigration,
		)
		cfg.Migration.IncludeTables = []string{firstTable, secondTable}
		faultObserver := &stage4SQLServerStrictDeletePublicationFaultObserver{
			stage4AdapterObserver: stage4SQLServerStrictDeleteLiveObserver(t, backend, runID, false),
			failTable:             firstTable,
			err:                   errors.New("injected SQL Server strict-delete partial publication failure"),
		}
		if _, err := SQLServerToSQLServerWithObserver(ctx, cfg, faultObserver); err == nil ||
			ClassifyTransferError(err) != ErrorClassState ||
			!strings.Contains(err.Error(), "injected SQL Server strict-delete partial publication failure") {
			t.Fatalf("first SQL Server migration strict-delete partial run error=%v", err)
		}
		if backend.mutationErr != nil {
			t.Fatalf("post-snapshot source mutation: %v", backend.mutationErr)
		}
		owner, found, ownerErr := rawBackend.LoadLatestStrictMigrationSnapshot(runID)
		if ownerErr != nil || !found || owner.SnapshotReference == "" || owner.EpochID == "" {
			t.Fatalf("load preserved SQL Server strict-delete partial snapshot owner=%#v found=%t err=%v", owner, found, ownerErr)
		}
		var snapshots int
		if err := sourceDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM sys.databases WHERE name = @p1", owner.SnapshotReference).Scan(&snapshots); err != nil || snapshots != 1 {
			t.Fatalf("SQL Server strict-delete partial snapshot count=%d err=%v", snapshots, err)
		}
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, firstTable, 3, 3)
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, secondTable, 4, 3)
		assertSQLServerStrictDeleteLiveEvidence(t, rawBackend, runID, sourceEndpoint.Schema, firstTable, state.StrictSnapshotMigration, 3)
		assertSQLServerStrictDeleteLiveEvidence(t, rawBackend, runID, sourceEndpoint.Schema, secondTable, state.StrictSnapshotMigration, 3)
		assertSQLServerStrictDeleteLiveJournalAndReceipt(t, ctx, rawBackend, runID, sourceEndpoint.Schema, firstTable, targetDatabase)
		assertSQLServerStrictDeleteLiveJournalAndReceipt(t, ctx, rawBackend, runID, sourceEndpoint.Schema, secondTable, targetDatabase)
		assertSQLServerStrictDeleteLiveWorkAttempts(t, rawBackend, runID, sourceEndpoint.Schema, secondTable, 3)

		resumedObserver := stage4SQLServerStrictDeleteLiveObserver(t, backend, runID, true)
		resumed, err := ExecuteResume(
			ctx,
			cfg,
			CompletedTableCheckpoints{firstTable: {Rows: 3}},
			resumedObserver,
		)
		if err != nil {
			t.Fatalf("resume SQL Server migration-strict delete partial route: %v", err)
		}
		if resumed != (Result{Tables: 2, Rows: 6, Validated: true}) {
			t.Fatalf("resumed SQL Server migration-strict delete partial result=%#v", resumed)
		}
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, firstTable, 3, 3)
		assertSQLServerStrictDeleteLiveCounts(t, ctx, sourceDatabase, targetDatabase, sourceEndpoint, targetEndpoint, secondTable, 4, 3)
		assertSQLServerStrictDeleteLiveEvidenceAtAttempt(
			t,
			rawBackend,
			runID,
			sourceEndpoint.Schema,
			secondTable,
			state.StrictSnapshotMigration,
			3,
			3,
		)
		if err := sourceDatabase.QueryRowContext(ctx, "SELECT COUNT(*) FROM sys.databases WHERE name = @p1", owner.SnapshotReference).Scan(&snapshots); err != nil || snapshots != 0 {
			t.Fatalf("SQL Server strict-delete partial snapshot cleanup count=%d err=%v", snapshots, err)
		}
	})
}

type stage4SQLServerStrictDeletePublicationFaultObserver struct {
	stage4AdapterObserver
	failTable string
	err       error
	failed    bool
}

func (observer *stage4SQLServerStrictDeletePublicationFaultObserver) AfterStage4TablePublication(
	ctx context.Context,
	table string,
	rows int,
) error {
	if err := observer.stage4AdapterObserver.AfterStage4TablePublication(ctx, table, rows); err != nil {
		return err
	}
	if !observer.failed && table == observer.failTable {
		observer.failed = true
		return observer.err
	}
	return nil
}

// stage4SQLServerStrictDeleteLiveBackend preserves the strict snapshot fault
// harness while forwarding the additive journal-readiness state surface.  The
// production fenced backend already retains that optional capability; this
// wrapper makes the live crash test exercise the same lifecycle boundary.
type stage4SQLServerStrictDeleteLiveBackend struct {
	*stage4SQLServerMigrationLiveBackend
	state.Backend
	state.Stage4AggregateBackend
	readiness state.Stage4DeleteJournalReadinessBackend
}

func (backend *stage4SQLServerStrictDeleteLiveBackend) ValidateStage4DeleteJournalReadinessBoundary(
	boundary state.Stage4DeleteJournalReadinessBoundary,
) error {
	if backend == nil || backend.readiness == nil {
		return errors.New("SQL Server strict-delete live readiness state is unavailable")
	}
	return backend.readiness.ValidateStage4DeleteJournalReadinessBoundary(boundary)
}

func (backend *stage4SQLServerStrictDeleteLiveBackend) SaveStage4DeleteJournalReadiness(
	readiness state.Stage4DeleteJournalReadiness,
) error {
	if backend == nil || backend.readiness == nil {
		return errors.New("SQL Server strict-delete live readiness state is unavailable")
	}
	return backend.readiness.SaveStage4DeleteJournalReadiness(readiness)
}

func (backend *stage4SQLServerStrictDeleteLiveBackend) EnsureStage4DeleteJournalReadiness(
	readiness state.Stage4DeleteJournalReadiness,
) (state.Stage4DeleteJournalReadinessReceipt, bool, error) {
	if backend == nil || backend.readiness == nil {
		return state.Stage4DeleteJournalReadinessReceipt{}, false, errors.New("SQL Server strict-delete live readiness state is unavailable")
	}
	return backend.readiness.EnsureStage4DeleteJournalReadiness(readiness)
}

func (backend *stage4SQLServerStrictDeleteLiveBackend) LoadStage4DeleteJournalReadiness(
	runID string,
) (state.Stage4DeleteJournalReadinessReceipt, bool, error) {
	if backend == nil || backend.readiness == nil {
		return state.Stage4DeleteJournalReadinessReceipt{}, false, errors.New("SQL Server strict-delete live readiness state is unavailable")
	}
	return backend.readiness.LoadStage4DeleteJournalReadiness(runID)
}

func stage4SQLServerStrictDeleteLiveConfig(
	source config.Endpoint,
	target config.Endpoint,
	table string,
	scope config.StrictConsistencyScope,
) config.Config {
	return config.Config{
		Source: source,
		Target: target,
		Migration: config.Migration{
			TargetMode:             "upsert",
			MemoryCeilingBytes:     64 << 20,
			IncludeTables:          []string{table},
			ConnectionLimit:        4,
			Workers:                2,
			ChunkSize:              1,
			Partitions:             1,
			ReaderParallelism:      1,
			WriterParallelism:      1,
			ReadAhead:              1,
			MaxRetries:             0,
			StrictConsistency:      true,
			StrictConsistencyScope: scope,
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationCountOnly,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes: config.DeletePolicy{
				Mode:           config.DeleteModeReconcile,
				TargetBehavior: config.DeleteTargetHard,
				Reconcile: config.DeleteReconcilePolicy{
					Schedule:          config.DeleteScheduleInterval,
					Interval:          time.Hour,
					BatchSize:         1,
					RequirePrimaryKey: true,
				},
			},
		},
	}
}

// initializeStage4SQLServerStrictDeleteLifecycleRun deliberately does not use
// the generic lifecycle fixture: that fixture models a PostgreSQL source,
// while SQL Server strict snapshot evidence is immutably bound to mssql.
func initializeStage4SQLServerStrictDeleteLifecycleRun(
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
		t.Fatalf("identify SQL Server strict-delete source workload: %v", err)
	}
	targetIdentity, err := config.NetworkEndpointWorkloadIdentity(target)
	if err != nil {
		t.Fatalf("identify SQL Server strict-delete target workload: %v", err)
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

func stage4SQLServerStrictDeleteLiveObserver(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	resume bool,
) stage4AdapterObserver {
	t.Helper()
	events := make([]string, 0)
	return stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, resume),
	}
}

func seedSQLServerStrictDeleteLiveRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	table string,
	includeOrphan bool,
) {
	t.Helper()
	values := "(1, 1, 'one'), (1, 2, 'two'), (1, 3, 'three')"
	if includeOrphan {
		values += ", (9, 9, 'orphan')"
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified(endpoint.Schema, table)+
			" ([tenant_id], [item_id], [payload]) VALUES "+values,
	); err != nil {
		t.Fatalf("seed SQL Server strict-delete %s rows: %v", endpoint.Database, err)
	}
}

func assertSQLServerStrictDeleteLiveCounts(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	sourceEndpoint config.Endpoint,
	targetEndpoint config.Endpoint,
	table string,
	wantSource int,
	wantTarget int,
) {
	t.Helper()
	var sourceRows, targetRows, orphans int
	if err := source.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+sqlServerQualified(sourceEndpoint.Schema, table),
	).Scan(&sourceRows); err != nil || sourceRows != wantSource {
		t.Fatalf("strict-delete source rows=%d err=%v, want %d", sourceRows, err, wantSource)
	}
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, table),
	).Scan(&targetRows); err != nil || targetRows != wantTarget {
		t.Fatalf("strict-delete target rows=%d err=%v, want %d", targetRows, err, wantTarget)
	}
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, table)+
			" WHERE [tenant_id] = 9 AND [item_id] = 9",
	).Scan(&orphans); err != nil || orphans != 0 {
		t.Fatalf("strict-delete target orphan rows=%d err=%v", orphans, err)
	}
}

func assertSQLServerStrictDeleteLiveEvidence(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	namespace string,
	table string,
	scope state.StrictSnapshotScope,
	wantRows int64,
) {
	assertSQLServerStrictDeleteLiveEvidenceAtAttempt(
		t, backend, runID, namespace, table, scope, 0, wantRows,
	)
}

func assertSQLServerStrictDeleteLiveEvidenceAtAttempt(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	namespace string,
	table string,
	scope state.StrictSnapshotScope,
	wantAttempts int,
	wantRows int64,
) {
	t.Helper()
	work, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatalf("list SQL Server strict-delete work: %v", err)
	}
	key := state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table}
	for _, item := range work {
		if item.Key != key {
			continue
		}
		attemptID, attemptErr := BuildStrictConsistencyAttemptID(item.Key, item.TopologyHash, wantAttempts)
		if attemptErr != nil {
			t.Fatal(attemptErr)
		}
		evidence, found, loadErr := backend.LoadStrictSnapshotEvidence(runID, item.Key, attemptID)
		if loadErr != nil || !found || evidence.Scope != scope || evidence.SourceEngine != "mssql" || evidence.ExactSourceRowCount != wantRows {
			t.Fatalf("SQL Server strict-delete evidence=%#v found=%t err=%v", evidence, found, loadErr)
		}
		return
	}
	t.Fatalf("SQL Server strict-delete work %#v is absent: %#v", key, work)
}

func assertSQLServerStrictDeleteLiveWorkAttempts(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	namespace string,
	table string,
	want int,
) {
	t.Helper()
	work, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatalf("list SQL Server strict-delete work: %v", err)
	}
	key := state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table}
	for _, item := range work {
		if item.Key == key {
			if item.Attempts != want {
				t.Fatalf("SQL Server strict-delete work %s.%s attempts=%d, want %d", namespace, table, item.Attempts, want)
			}
			return
		}
	}
	t.Fatalf("SQL Server strict-delete work %#v is absent: %#v", key, work)
}

func assertSQLServerStrictDeleteLiveJournalAndReceipt(
	t *testing.T,
	ctx context.Context,
	backend Stage4StateBackend,
	runID string,
	namespace string,
	table string,
	target *sql.DB,
) {
	t.Helper()
	readiness, ok := any(backend).(state.Stage4DeleteJournalReadinessBackend)
	if !ok {
		t.Fatalf("%T lacks SQL Server strict-delete journal readiness state", backend)
	}
	ready, found, err := readiness.LoadStage4DeleteJournalReadiness(runID)
	if err != nil || !found || ready.Readiness.TargetEngine != "mssql" {
		t.Fatalf("SQL Server strict-delete readiness=%#v found=%t err=%v", ready, found, err)
	}
	record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(
		runID,
		state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table},
	)
	if err != nil || !found || record.Candidates != 1 || record.DeletedRows != 1 {
		t.Fatalf("SQL Server strict-delete reconciliation=%#v found=%t err=%v", record, found, err)
	}
	journal, err := inspectSQLServerDeleteReceiptJournal(ctx, target)
	if err != nil || !journal.Exists || journal.HeaderIdentity == "" {
		t.Fatalf("SQL Server strict-delete journal=%#v err=%v", journal, err)
	}
}
