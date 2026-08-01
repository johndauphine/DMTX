package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

var registerSQLServerStrictOwnerSetupDriver sync.Once

type sqlServerStrictOwnerSetupDriver struct{}

func (sqlServerStrictOwnerSetupDriver) Open(string) (driver.Conn, error) {
	return sqlServerStrictOwnerSetupConnection{}, nil
}

type sqlServerStrictOwnerSetupConnection struct{}

func (sqlServerStrictOwnerSetupConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected SQL Server strict owner prepare")
}

func (sqlServerStrictOwnerSetupConnection) Close() error { return nil }

func (sqlServerStrictOwnerSetupConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected SQL Server strict owner Begin")
}

func (sqlServerStrictOwnerSetupConnection) BeginTx(
	ctx context.Context,
	_ driver.TxOptions,
) (driver.Tx, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

var _ driver.ConnBeginTx = sqlServerStrictOwnerSetupConnection{}

func openSQLServerStrictOwnerSetupDatabase(t *testing.T) *sql.DB {
	t.Helper()
	registerSQLServerStrictOwnerSetupDriver.Do(func() {
		sql.Register("dmtx_mssql_strict_owner_setup", sqlServerStrictOwnerSetupDriver{})
	})
	database, err := sql.Open("dmtx_mssql_strict_owner_setup", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func TestSQLServerStrictOwnerSetupHonorsCallerCancellation(t *testing.T) {
	database := openSQLServerStrictOwnerSetupDatabase(t)
	opener, err := NewSQLServerStrictConsistencyOpener(database, "dbo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = opener.OpenPlannedStrictConsistency(ctx, PlannedStrictConsistencyOpenRequest{
		RunID:        "strict-owner-cancel",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "process-cancel",
		Tasks: []state.TaskKey{{
			Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
		}},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled SQL Server strict owner setup error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("SQL Server strict owner setup ignored caller deadline for %s", elapsed)
	}
}

func TestConfigureStage4SQLServerTableStrictSourcePoolReservesOwnerAndReaders(t *testing.T) {
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec:     relationalSourceSpec{engine: "mssql"},
		database: database,
	}
	resources := config.EffectiveTransferPlan{
		ConnectionLimit: config.EffectiveInt{Value: 4},
		Readers:         config.EffectiveInt{Value: 3},
	}
	if err := configureStage4SQLServerTableStrictSourcePool(source, resources); err != nil {
		t.Fatalf("configure SQL Server table-strict source pool: %v", err)
	}
	if got := database.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("SQL Server table-strict source pool max connections=%d, want owner plus readers=4", got)
	}

	resources.ConnectionLimit.Value = 3
	if err := configureStage4SQLServerTableStrictSourcePool(source, resources); err == nil {
		t.Fatal("configure SQL Server table-strict source pool accepted owner plus readers beyond connection_limit")
	}
	if got := database.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("rejected SQL Server table-strict pool reconfigured database to %d, want unchanged 4", got)
	}
}

// TestSQLServerStrictReaderCloseDuringSetupOwnsLateResources is deliberately
// driver-independent: it holds a reader immediately after BeginTx and before
// its connection/transaction pair is published to the reader lifecycle. The
// concurrent Close must cancel setup, wait for that handoff, and ensure the
// late pair is closed instead of racing a nil close or leaking it.
func TestSQLServerStrictReaderCloseDuringSetupOwnsLateResources(t *testing.T) {
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = database.Close() })
	owner, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	session := &SQLServerStrictConsistencySession{
		namespace:   "dbo",
		database:    database,
		transaction: owner,
		selected:    map[state.TaskKey]struct{}{task: {}},
		readers:     make(map[*sqlServerStrictReader]struct{}),
		closeDone:   make(chan struct{}),
		beforeReaderAdopt: func() {
			close(entered)
			<-release
		},
	}

	readErr := make(chan error, 1)
	go func() {
		readErr <- session.RunReader(
			context.Background(),
			task,
			func(context.Context, SQLServerStrictSnapshotQueryer) error {
				return errors.New("reader callback must not start")
			},
		)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not reach the setup handoff")
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- session.Close(context.Background()) }()
	select {
	case err := <-closeErr:
		t.Fatalf("session Close returned before setup handoff released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-readErr; err == nil ||
		!strings.Contains(err.Error(), "closed during setup") {
		t.Fatalf("reader setup race error = %v", err)
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("close SQL Server strict session: %v", err)
	}
	if inUse := database.Stats().InUse; inUse != 0 {
		t.Fatalf("setup/close race leaked %d database connections", inUse)
	}
}

func TestSQLServerPlannedStrictOpenerRejectsMigrationScope(t *testing.T) {
	_, err := normalizeSQLServerPlannedOpenRequest(
		PlannedStrictConsistencyOpenRequest{
			RunID:        "run-1",
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "process-1",
			Tasks: []state.TaskKey{{
				Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
			}},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot serve scope") {
		t.Fatalf("migration planned-opener error = %v", err)
	}
}

func TestValidateStage4ComposedConfigurationAdmitsSQLServerTableStrictTargets(
	t *testing.T,
) {
	for _, target := range []string{"postgres", "mysql", "mssql", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			cfg := stage4AdapterTestConfig(t, "source-secret", "target-secret")
			cfg.Source.Type = "mssql"
			cfg.Target.Type = target
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.StrictConsistency = true
			cfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
			if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
				t.Fatalf("admit SQL Server table strict target %s: %v", target, err)
			}
		})
	}

	for _, target := range []string{"postgres", "mysql", "mssql", "sqlite"} {
		t.Run("migration-"+target, func(t *testing.T) {
			cfg := stage4AdapterTestConfig(t, "source-secret", "target-secret")
			cfg.Source.Type = "mssql"
			cfg.Target.Type = target
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.StrictConsistency = true
			cfg.Migration.StrictConsistencyScope = config.StrictConsistencyMigration
			if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
				t.Fatalf("admit SQL Server migration strict target %s: %v", target, err)
			}
		})
	}
}

func TestSQLServerMigrationStrictTopologySurvivesPreOwnerAndOrdinaryResume(
	t *testing.T,
) {
	const runID = "mssql-migration-topology"
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
	}
	reference := sqlServerMigrationSnapshotName(runID, "source_db")
	capture := StrictConsistencyCapture{
		MigrationEpochID:           sqlServerMigrationSnapshotEpoch(reference),
		MigrationSnapshotReference: reference,
		MigrationCapturedAt:        strictCoreTime(),
		Tables: []StrictConsistencyTableCapture{{
			Task:                task,
			SnapshotReference:   reference,
			ExactSourceRowCount: 4,
			CapturedAt:          strictCoreTime(),
		}},
	}
	preOwner, err := newStage4SQLServerMigrationEpochBinding(capture)
	if err != nil {
		t.Fatal(err)
	}
	base := stage4AdapterWork{task: task, topology: "base-topology"}
	first, err := preOwner.finalizeWork(base)
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt, err := BuildStrictConsistencyAttemptID(task, first.topology, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Recovery after CREATE DATABASE SNAPSHOT but before owner persistence sees
	// the same deterministic snapshot. A fresh coordinator epoch must not alter
	// the finalized work topology or strict attempt.
	recovered, err := newStage4SQLServerMigrationEpochBinding(capture)
	if err != nil {
		t.Fatal(err)
	}
	afterCrash, err := recovered.finalizeWork(base)
	if err != nil {
		t.Fatal(err)
	}
	if afterCrash.topology != first.topology {
		t.Fatalf("pre-owner recovery topology = %q, want %q", afterCrash.topology, first.topology)
	}

	owner := state.StrictMigrationSnapshot{
		RunID: runID, EpochID: capture.MigrationEpochID, SourceEngine: "mssql",
		SnapshotReference: reference, ProcessEpoch: "original-process",
		CapturedAt: strictCoreTime(),
	}
	ordinaryResume, err := stage4SQLServerMigrationBindingFromEvidence(
		owner,
		[]state.StrictSnapshotEvidence{{
			RunID: runID, Task: task, AttemptID: firstAttempt,
			SourceEngine: "mssql", Scope: state.StrictSnapshotMigration,
			MigrationEpochID:    capture.MigrationEpochID,
			SnapshotReference:   reference,
			ProcessEpoch:        "original-process",
			ExactSourceRowCount: 4, CapturedAt: strictCoreTime(),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	afterResume, err := ordinaryResume.finalizeWork(base)
	if err != nil {
		t.Fatal(err)
	}
	if afterResume.topology != first.topology {
		t.Fatalf("ordinary resume topology = %q, want %q", afterResume.topology, first.topology)
	}
	attempt, err := BuildStrictConsistencyAttemptID(task, afterResume.topology, 0)
	if err != nil {
		t.Fatal(err)
	}
	if attempt != firstAttempt {
		t.Fatalf("ordinary resume attempt = %q, want %q", attempt, firstAttempt)
	}
}

func TestSQLServerMigrationEvidenceRejectsDivergentCaptureTime(t *testing.T) {
	owner := state.StrictMigrationSnapshot{
		RunID: "run-1", EpochID: "mssql-epoch-1", SourceEngine: "mssql",
		SnapshotReference: "dmtx_ss_capture", ProcessEpoch: "process-1",
		CapturedAt: strictCoreTime(),
	}
	evidence := []state.StrictSnapshotEvidence{{
		RunID: "run-1",
		Task: state.TaskKey{
			Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
		},
		AttemptID:    "strict-attempt",
		SourceEngine: "mssql", Scope: state.StrictSnapshotMigration,
		MigrationEpochID: owner.EpochID, SnapshotReference: owner.SnapshotReference,
		ProcessEpoch: owner.ProcessEpoch, ExactSourceRowCount: 1,
		CapturedAt: owner.CapturedAt.Add(time.Nanosecond),
	}}
	if _, err := stage4SQLServerMigrationStrictSourceRows(owner, evidence); err == nil ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("divergent migration source evidence error = %v", err)
	}
	if _, err := stage4SQLServerMigrationBindingFromEvidence(owner, evidence); err == nil ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("divergent migration topology evidence error = %v", err)
	}
}

func TestCleanupCompletedStage4SQLServerMigrationSnapshot(t *testing.T) {
	owner := state.StrictMigrationSnapshot{
		RunID: "run-1", EpochID: "mssql-epoch-1", SourceEngine: "mssql",
		SnapshotReference: "dmtx_ss_deadbeef", ProcessEpoch: "process-1",
		CapturedAt: strictCoreTime(),
	}
	session := &stage4SQLServerMigrationSnapshotFinalizationFake{}
	backend := &stage4SQLServerMigrationCleanupBackendFake{}
	if err := finalizeStage4SQLServerMigrationSnapshotCleanup(
		context.Background(),
		session,
		owner,
		state.StrictMigrationCleanupIntent{},
		false,
		backend,
		func() error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	if !session.preserved || !session.authorized || session.closes != 1 {
		t.Fatalf("cleanup finalization session = %#v", session)
	}
	if backend.saved.RunID != owner.RunID || backend.saved.EpochID != owner.EpochID {
		t.Fatalf("cleanup intent = %#v", backend.saved)
	}
	backend.saveErr = errors.New("state unavailable")
	failing := &stage4SQLServerMigrationSnapshotFinalizationFake{}
	if err := finalizeStage4SQLServerMigrationSnapshotCleanup(
		context.Background(),
		failing,
		owner,
		state.StrictMigrationCleanupIntent{},
		false,
		backend,
		func() error { return nil },
	); !errors.Is(err, backend.saveErr) ||
		ClassifyTransferError(err) != ErrorClassState ||
		!failing.preserved || failing.authorized {
		t.Fatalf("cleanup state failure=%v finalization=%#v", err, failing)
	}
	primary := errors.New("schema sentinel failed")
	closeFailure := errors.New("snapshot reader release failed")
	closeFailing := &stage4SQLServerMigrationSnapshotFinalizationFake{
		closeErr: closeFailure,
	}
	if err := finalizeStage4SQLServerMigrationSnapshotCleanup(
		context.Background(),
		closeFailing,
		owner,
		state.StrictMigrationCleanupIntent{},
		false,
		&stage4SQLServerMigrationCleanupBackendFake{},
		func() error { return primary },
	); !errors.Is(err, primary) || !errors.Is(err, closeFailure) {
		t.Fatalf("finalization/cleanup error = %v", err)
	}
}

func TestSQLServerMigrationSnapshotDropsOnOrdinaryFailureButPreservesFinalizationFailure(t *testing.T) {
	t.Run("ordinary-transfer-or-cancel", func(t *testing.T) {
		drops := 0
		session := newSQLServerMigrationSnapshotCloseTestSession(func(context.Context) error {
			drops++
			return nil
		})
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if drops != 1 {
			t.Fatalf("ordinary failure cleanup drops=%d, want 1", drops)
		}
	})

	for _, test := range []struct {
		name string
		new  func(*testing.T) state.StrictMigrationCleanupBackend
	}{
		{
			name: "yaml",
			new: func(t *testing.T) state.StrictMigrationCleanupBackend {
				store := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
				return initializeSQLServerCleanupState(t, store)
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) state.StrictMigrationCleanupBackend {
				store := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
				return initializeSQLServerCleanupState(t, store)
			},
		},
	} {
		t.Run("finalization-state-failure-"+test.name, func(t *testing.T) {
			owner := strictMigrationCleanupOwner()
			actual := test.new(t)
			injected := errors.New("inject cleanup intent write failure")
			backend := failingSQLServerMigrationCleanupBackend{
				StrictMigrationCleanupBackend: actual,
				saveErr:                       injected,
			}
			drops := 0
			session := newSQLServerMigrationSnapshotCloseTestSession(func(context.Context) error {
				drops++
				return nil
			})
			err := finalizeStage4SQLServerMigrationSnapshotCleanup(
				context.Background(),
				session,
				owner,
				state.StrictMigrationCleanupIntent{},
				false,
				&backend,
				func() error { return nil },
			)
			if !errors.Is(err, injected) {
				t.Fatalf("cleanup intent failure = %v", err)
			}
			if drops != 0 || !session.preserve || session.closed == false {
				t.Fatalf("state failure did not preserve snapshot: drops=%d session=%#v", drops, session)
			}
			if _, found, err := actual.LoadStrictMigrationCleanupIntent(owner.RunID, owner.EpochID); err != nil || found {
				t.Fatalf("failed cleanup intent persisted: found=%v err=%v", found, err)
			}
		})
	}
}

func TestCleanupCompletedStage4SQLServerMigrationSnapshotBranches(t *testing.T) {
	for _, stateKind := range []string{"yaml", "sqlite"} {
		t.Run(stateKind, func(t *testing.T) {
			t.Run("present-without-intent-finalizes-and-drops", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				session := &stage4SQLServerMigrationSnapshotFinalizationFake{}
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{session: session}
				if err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, cleanup,
				); err != nil {
					t.Fatal(err)
				}
				if finalizer.calls != 1 || !session.preserved || !session.authorized ||
					!session.dropped || session.closes != 1 {
					t.Fatalf("present no-intent cleanup = finalizer=%#v session=%#v", finalizer, session)
				}
				intent, found, err := cleanup.LoadStrictMigrationCleanupIntent(owner.RunID, owner.EpochID)
				if err != nil || !found || !stage4SQLServerMigrationCleanupIntentMatchesOwner(intent, owner) {
					t.Fatalf("durable cleanup intent found=%v intent=%#v err=%v", found, intent, err)
				}
				assertSQLServerMigrationCleanupGateCompleted(t, prepared)
			})

			t.Run("absent-without-intent-fails-closed", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{absent: true}
				err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, cleanup,
				)
				if err == nil || ClassifyTransferError(err) != ErrorClassState {
					t.Fatalf("missing no-intent snapshot error = %v", err)
				}
				if finalizer.calls != 1 || finalizer.session != nil {
					t.Fatalf("missing no-intent finalizer = %#v", finalizer)
				}
			})

			t.Run("absent-with-matching-intent-completes-idempotently", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				if err := cleanup.SaveStrictMigrationCleanupIntent(
					stage4SQLServerMigrationCleanupIntentForOwner(owner),
				); err != nil {
					t.Fatal(err)
				}
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{absent: true}
				if err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, cleanup,
				); err != nil {
					t.Fatal(err)
				}
				if finalizer.calls != 1 || !finalizer.allowAbsentAfterIntent {
					t.Fatalf("receipt-authorized absent cleanup finalizer = %#v", finalizer)
				}
				assertSQLServerMigrationCleanupGateCompleted(t, prepared)
			})

			t.Run("mismatched-intent-fails-before-open-or-drop", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				mismatched := stage4SQLServerMigrationCleanupIntentForOwner(owner)
				mismatched.SnapshotReference = "dmtx_ss_other"
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{
					session: &stage4SQLServerMigrationSnapshotFinalizationFake{},
				}
				corrupt := corruptSQLServerMigrationCleanupBackend{
					StrictMigrationCleanupBackend: cleanup,
					intent:                        mismatched,
				}
				err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, &corrupt,
				)
				if err == nil || ClassifyTransferError(err) != ErrorClassState {
					t.Fatalf("mismatched cleanup intent error = %v", err)
				}
				if finalizer.calls != 0 || finalizer.session.closes != 0 || finalizer.session.dropped {
					t.Fatalf("mismatched cleanup intent reached snapshot = %#v", finalizer)
				}
			})

			t.Run("present-with-existing-intent-retries-drop", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				if err := cleanup.SaveStrictMigrationCleanupIntent(
					stage4SQLServerMigrationCleanupIntentForOwner(owner),
				); err != nil {
					t.Fatal(err)
				}
				session := &stage4SQLServerMigrationSnapshotFinalizationFake{}
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{session: session}
				if err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, cleanup,
				); err != nil {
					t.Fatal(err)
				}
				if finalizer.calls != 1 || !finalizer.allowAbsentAfterIntent ||
					!session.authorized || !session.dropped {
					t.Fatalf("existing-intent cleanup did not retry drop: finalizer=%#v session=%#v", finalizer, session)
				}
			})

			t.Run("final-sentinel-failure-preserves-existing-snapshot", func(t *testing.T) {
				prepared, owner, cleanup := newSQLServerMigrationCleanupPrepared(t, stateKind)
				injected := errors.New("inject final sentinel state failure")
				prepared.run.Backend = failingSQLServerMigrationFinalSentinelBackend{
					Stage4StateBackend: prepared.run.Backend,
					listErr:            injected,
				}
				session := &stage4SQLServerMigrationSnapshotFinalizationFake{}
				finalizer := &stage4SQLServerMigrationSnapshotFinalizerFake{session: session}
				err := cleanupCompletedStage4SQLServerMigrationSnapshot(
					context.Background(), prepared, owner, finalizer, cleanup,
				)
				if !errors.Is(err, injected) || ClassifyTransferError(err) != ErrorClassState {
					t.Fatalf("final sentinel failure = %v", err)
				}
				if !session.preserved || session.authorized || session.dropped || session.closes != 1 {
					t.Fatalf("final sentinel failure did not preserve snapshot: %#v", session)
				}
			})
		})
	}
}

func newSQLServerMigrationCleanupPrepared(
	t *testing.T,
	stateKind string,
) (stage4AdapterPrepared, state.StrictMigrationSnapshot, state.StrictMigrationCleanupBackend) {
	t.Helper()
	owner := strictMigrationCleanupOwner()
	run := newNetworkStateTestRun(t, stateKind, owner.RunID)
	backend, ok := run.Backend.(state.Backend)
	if !ok {
		t.Fatalf("%T lacks state.Backend", run.Backend)
	}
	cleanup := initializeSQLServerCleanupState(t, backend)
	gate := Stage4SchemaGateResult{
		Task:         stage4SchemaGateTask,
		TopologyHash: "mssql-cleanup-schema-gate",
	}
	if _, err := run.Backend.EnsureWorkPlan(state.WorkTask{
		RunID: owner.RunID, Key: gate.Task, Strategy: stage4SchemaGateStrategy,
		TopologyHash: gate.TopologyHash, StartedAt: strictCoreTime(),
	}, []state.RangeState{{
		ID: stage4SchemaGateRangeID, Strategy: stage4SchemaGateStrategy,
		TopologyHash: gate.TopologyHash,
	}}); err != nil {
		t.Fatal(err)
	}
	return stage4AdapterPrepared{run: run, gate: gate}, owner, cleanup
}

func stage4SQLServerMigrationCleanupIntentForOwner(
	owner state.StrictMigrationSnapshot,
) state.StrictMigrationCleanupIntent {
	return state.StrictMigrationCleanupIntent{
		RunID: owner.RunID, EpochID: owner.EpochID,
		SourceEngine:      owner.SourceEngine,
		SnapshotReference: owner.SnapshotReference,
		ProcessEpoch:      owner.ProcessEpoch,
		CapturedAt:        owner.CapturedAt,
		IntentAt:          owner.CapturedAt.Add(time.Second),
	}
}

func assertSQLServerMigrationCleanupGateCompleted(
	t *testing.T,
	prepared stage4AdapterPrepared,
) {
	t.Helper()
	tasks, ranges, err := prepared.run.Backend.ListWork(prepared.run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	assertStage4AdapterWorkCompleted(
		t,
		tasks,
		ranges,
		prepared.gate.Task,
		stage4SchemaGateRangeID,
	)
}

func newSQLServerMigrationSnapshotCloseTestSession(
	drop func(context.Context) error,
) *SQLServerMigrationSnapshotSession {
	return &SQLServerMigrationSnapshotSession{
		readers:      make(map[*sqlServerMigrationSnapshotReader]struct{}),
		selected:     make(map[state.TaskKey]struct{}),
		closeDone:    make(chan struct{}),
		dropSnapshot: drop,
	}
}

func strictMigrationCleanupOwner() state.StrictMigrationSnapshot {
	return state.StrictMigrationSnapshot{
		RunID: "mssql-cleanup-run", EpochID: "mssql-cleanup-epoch",
		SourceEngine: "mssql", SnapshotReference: "dmtx_ss_cleanup",
		ProcessEpoch: "mssql-cleanup-process", CapturedAt: strictCoreTime(),
	}
}

func initializeSQLServerCleanupState(
	t *testing.T,
	backend state.Backend,
) state.StrictMigrationCleanupBackend {
	t.Helper()
	owner := strictMigrationCleanupOwner()
	if err := backend.InitializeRun(state.Run{
		ID: owner.RunID, Source: "source", Target: "target",
		SourceEngine:   "mssql",
		SourceIdentity: "mssql:source.example:1433/source",
		TargetIdentity: "sqlite:target", Outcome: state.Running,
		Resumable: true, Reason: "running", StartedAt: strictCoreTime(),
	}, "cleanup-config"); err != nil {
		t.Fatal(err)
	}
	stage4, ok := backend.(state.Stage4Backend)
	if !ok {
		t.Fatalf("%T lacks Stage4Backend", backend)
	}
	if err := stage4.SaveStrictMigrationSnapshot(owner); err != nil {
		t.Fatal(err)
	}
	cleanup, ok := backend.(state.StrictMigrationCleanupBackend)
	if !ok {
		t.Fatalf("%T lacks StrictMigrationCleanupBackend", backend)
	}
	return cleanup
}

type failingSQLServerMigrationCleanupBackend struct {
	state.StrictMigrationCleanupBackend
	saveErr error
}

func (backend *failingSQLServerMigrationCleanupBackend) SaveStrictMigrationCleanupIntent(
	state.StrictMigrationCleanupIntent,
) error {
	return backend.saveErr
}

type stage4SQLServerMigrationSnapshotFinalizationFake struct {
	preserved  bool
	authorized bool
	dropped    bool
	closes     int
	closeErr   error
}

func (session *stage4SQLServerMigrationSnapshotFinalizationFake) PreserveSnapshotForResume() error {
	session.preserved = true
	return nil
}

func (session *stage4SQLServerMigrationSnapshotFinalizationFake) AuthorizeSnapshotCleanup() error {
	if !session.preserved {
		return errors.New("cleanup was not preserved")
	}
	session.authorized = true
	return nil
}

func (session *stage4SQLServerMigrationSnapshotFinalizationFake) Close(
	context.Context,
) error {
	session.closes++
	if session.authorized {
		session.dropped = true
	}
	return session.closeErr
}

type stage4SQLServerMigrationSnapshotFinalizerFake struct {
	session                *stage4SQLServerMigrationSnapshotFinalizationFake
	absent                 bool
	err                    error
	calls                  int
	allowAbsentAfterIntent bool
}

func (finalizer *stage4SQLServerMigrationSnapshotFinalizerFake) OpenCompletedMigrationSnapshot(
	_ context.Context,
	_ string,
	_ state.StrictMigrationSnapshot,
	allowAbsentAfterIntent bool,
) (SQLServerMigrationSnapshotFinalizationSession, bool, error) {
	finalizer.calls++
	finalizer.allowAbsentAfterIntent = allowAbsentAfterIntent
	if finalizer.err != nil {
		return nil, false, finalizer.err
	}
	if finalizer.absent {
		return nil, true, nil
	}
	return finalizer.session, false, nil
}

type corruptSQLServerMigrationCleanupBackend struct {
	state.StrictMigrationCleanupBackend
	intent state.StrictMigrationCleanupIntent
}

func (backend *corruptSQLServerMigrationCleanupBackend) LoadStrictMigrationCleanupIntent(
	string,
	string,
) (state.StrictMigrationCleanupIntent, bool, error) {
	return backend.intent, true, nil
}

type failingSQLServerMigrationFinalSentinelBackend struct {
	Stage4StateBackend
	listErr error
}

func (backend failingSQLServerMigrationFinalSentinelBackend) ListWork(
	string,
) ([]state.WorkTask, []state.RangeState, error) {
	return nil, nil, backend.listErr
}

type stage4SQLServerMigrationCleanupBackendFake struct {
	saved   state.StrictMigrationCleanupIntent
	saveErr error
}

func (backend *stage4SQLServerMigrationCleanupBackendFake) SaveStrictMigrationCleanupIntent(
	intent state.StrictMigrationCleanupIntent,
) error {
	if backend.saveErr != nil {
		return backend.saveErr
	}
	backend.saved = intent
	return nil
}

func (backend *stage4SQLServerMigrationCleanupBackendFake) LoadStrictMigrationCleanupIntent(
	string,
	string,
) (state.StrictMigrationCleanupIntent, bool, error) {
	return state.StrictMigrationCleanupIntent{}, false, nil
}
