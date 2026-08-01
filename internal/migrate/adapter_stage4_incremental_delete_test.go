package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4IncrementalDeleteMutationWriter commits a source mutation after the
// exact target window write. It intentionally returns the successful target
// receipt, so the test reaches the pre-delete authority check instead of
// manufacturing a transfer failure.
type stage4IncrementalDeleteMutationWriter struct {
	delegate    sqliteStage4NetworkBatchWriter
	mutate      func() error
	called      bool
	mutationErr error
}

func (writer *stage4IncrementalDeleteMutationWriter) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	receipt, err := writer.delegate.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
	)
	if err != nil || writer.called || writer.mutate == nil {
		return receipt, err
	}
	writer.called = true
	writer.mutationErr = writer.mutate()
	return receipt, nil
}

// stage4IncrementalDeleteTestBackend keeps the optional state capabilities
// visible through a fault-injecting wrapper. The real runner requires all of
// them before it can reach target writes or the target-private journal.
type stage4IncrementalDeleteTestBackend interface {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	state.Stage4DeleteJournalReadinessBackend
}

type stage4IncrementalDeleteFailRangeAckBackend struct {
	stage4IncrementalDeleteTestBackend
	failNext bool
}

func (backend *stage4IncrementalDeleteFailRangeAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	if backend.failNext &&
		acknowledgement.Task.Type == stage4AdapterNetworkTaskType &&
		acknowledgement.RangeID == stage4AdapterIncrementalRangeID {
		backend.failNext = false
		return state.RangeState{}, errors.New(
			"injected SQLite incremental delete range acknowledgement failure",
		)
	}
	return backend.stage4IncrementalDeleteTestBackend.AcknowledgeRange(
		acknowledgement,
	)
}

// stage4IncrementalDeleteFailPlanAckBackend models the one safe crash window
// after immutable source keys are durably spooled but before the caller sees
// the successful state write. Resume must replay that plan, not scan a later
// source database.
type stage4IncrementalDeleteFailPlanAckBackend struct {
	stage4IncrementalDeleteTestBackend
	failNext bool
}

func (backend *stage4IncrementalDeleteFailPlanAckBackend) SaveDeleteReconciliationPlan(
	plan state.DeleteReconciliationPlan,
) error {
	if err := backend.stage4IncrementalDeleteTestBackend.SaveDeleteReconciliationPlan(plan); err != nil {
		return err
	}
	if backend.failNext {
		backend.failNext = false
		return errors.New(
			"injected SQLite incremental delete durable-plan acknowledgement failure",
		)
	}
	return nil
}

func TestStage4SQLiteIncrementalDeleteReconcilesAfterTransfer(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sourcePath, targetPath := stage4SQLiteIncrementalDeleteDatabaseFixture(t, ctx)
			backend := newBackend(t)
			runID := "sqlite-incremental-delete-" + backendName
			stage4InitializeSQLiteIncrementalDeleteRun(
				t,
				backend,
				runID,
				sourcePath,
				targetPath,
			)
			cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)

			source, target := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			run := Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			}
			events := []string{}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				run,
			)
			if err != nil {
				t.Fatalf("fresh SQLite incremental+delete run: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
				t.Fatalf("fresh SQLite incremental+delete result = %#v", result)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, target.database, []int64{1, 2})
			task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}
			attempt, found, err := backend.LoadLatestCommittedIncrementalAttempt(runID, task)
			if err != nil || !found || attempt.UpperFence == nil ||
				attempt.UpperFence.Column != "updated_at" {
				t.Fatalf("stored incremental upper-fence attempt found=%t attempt=%#v err=%v", found, attempt, err)
			}
			deleteRecord, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(runID, task)
			if err != nil || !found ||
				deleteRecord.Status != state.DeleteReconciliationCompleted ||
				deleteRecord.Candidates != 1 || deleteRecord.DeletedRows != 1 ||
				deleteRecord.Plan == nil {
				t.Fatalf("stored SQLite incremental delete record found=%t record=%#v err=%v", found, deleteRecord, err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStage4SQLiteIncrementalDeleteEvolvesAbsentTargetBeforeJournalReadiness(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sourcePath, targetPath := stage4SQLiteIncrementalDeleteSourceOnlyFixture(t, ctx)
			backend := newBackend(t)
			runID := "sqlite-incremental-delete-absent-target-" + backendName
			stage4InitializeSQLiteIncrementalDeleteRun(
				t,
				backend,
				runID,
				sourcePath,
				targetPath,
			)
			cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)
			cfg.Migration.SchemaContract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}
			source, target := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			defer func() { _ = source.Close() }()
			defer func() { _ = target.Close() }()
			run := Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			}
			events := []string{}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				run,
			)
			if err != nil {
				t.Fatalf("run absent-target SQLite incremental+delete: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 1, Validated: true}) {
				t.Fatalf("absent-target incremental+delete result = %#v", result)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, target.database, []int64{1})
			task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}
			record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(runID, task)
			if err != nil || !found ||
				record.Status != state.DeleteReconciliationCompleted ||
				record.Candidates != 0 || record.DeletedRows != 0 ||
				record.CommittedBatches != 0 {
				t.Fatalf("absent-target due-zero delete evidence found=%t record=%#v err=%v", found, record, err)
			}
		})
	}
}

func TestStage4SQLiteIncrementalDeleteMidTransferResumeRemainsResumable(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			raw, ok := any(newBackend(t)).(stage4IncrementalDeleteTestBackend)
			if !ok {
				t.Fatal("lifecycle backend lacks required incremental-delete state capabilities")
			}
			backend := &stage4IncrementalDeleteFailRangeAckBackend{
				stage4IncrementalDeleteTestBackend: raw,
				failNext:                           true,
			}
			sourcePath, targetPath := stage4SQLiteIncrementalDeleteDatabaseFixture(t, ctx)
			runID := "sqlite-incremental-delete-mid-transfer-" + backendName
			stage4InitializeSQLiteIncrementalDeleteRun(
				t,
				backend,
				runID,
				sourcePath,
				targetPath,
			)
			cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)
			source, target := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			run := Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			}
			events := []string{}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			if _, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				run,
			); err == nil || !strings.Contains(err.Error(), "range acknowledgement failure") {
				t.Fatalf("injected mid-transfer failure = %v", err)
			}
			task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}
			active, found, err := backend.LoadActiveIncrementalAttempt(runID, task)
			if err != nil || !found || active.Status != state.IncrementalRunning ||
				active.TableSucceeded || active.UpperFence == nil {
				t.Fatalf("mid-transfer incremental attempt found=%t attempt=%#v err=%v", found, active, err)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, target.database, []int64{1, 2, 99})
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}

			resumedSource, resumedTarget := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			defer func() { _ = resumedSource.Close() }()
			defer func() { _ = resumedTarget.Close() }()
			resumeRun := run
			resumeRun.Resume = true
			resumeObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    resumeRun,
			}
			result, err := resumeWithStage4Adapters(
				ctx,
				cfg,
				CompletedTableCheckpoints{},
				resumeObserver,
				resumeObserver,
				resumedSource,
				resumedTarget,
				"upsert",
				resumeRun,
			)
			if err != nil {
				t.Fatalf("resume mid-transfer SQLite incremental+delete: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
				t.Fatalf("mid-transfer resume result = %#v", result)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, resumedTarget.database, []int64{1, 2})
		})
	}
}

func TestStage4SQLiteIncrementalDeleteReplaysDurablePlanAfterTransfer(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			raw, ok := any(newBackend(t)).(stage4IncrementalDeleteTestBackend)
			if !ok {
				t.Fatal("lifecycle backend lacks required incremental-delete state capabilities")
			}
			backend := &stage4IncrementalDeleteFailPlanAckBackend{
				stage4IncrementalDeleteTestBackend: raw,
				failNext:                           true,
			}
			sourcePath, targetPath := stage4SQLiteIncrementalDeleteDatabaseFixture(t, ctx)
			runID := "sqlite-incremental-delete-plan-replay-" + backendName
			stage4InitializeSQLiteIncrementalDeleteRun(
				t,
				backend,
				runID,
				sourcePath,
				targetPath,
			)
			cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)
			source, target := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			run := Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			}
			events := []string{}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			if _, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				run,
			); err == nil || !strings.Contains(err.Error(), "durable-plan acknowledgement failure") {
				t.Fatalf("injected post-transfer plan acknowledgement failure = %v", err)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, target.database, []int64{1, 2, 99})
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}

			resumedSource, resumedTarget := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			defer func() { _ = resumedSource.Close() }()
			defer func() { _ = resumedTarget.Close() }()
			resumeRun := run
			resumeRun.Resume = true
			resumeObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    resumeRun,
			}
			result, err := resumeWithStage4Adapters(
				ctx,
				cfg,
				CompletedTableCheckpoints{"items": {Rows: 2}},
				resumeObserver,
				resumeObserver,
				resumedSource,
				resumedTarget,
				"upsert",
				resumeRun,
			)
			if err != nil {
				t.Fatalf("resume SQLite incremental+delete durable plan: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
				t.Fatalf("durable-plan resume result = %#v", result)
			}
			stage4AssertSQLiteIncrementalDeleteRows(t, ctx, resumedTarget.database, []int64{1, 2})
		})
	}
}

func TestStage4SQLiteIncrementalDeleteRejectsPostSnapshotSourceMutation(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			stage4RunSQLiteIncrementalDeletePostSnapshotMutation(
				t,
				newBackend(t),
				backendName,
			)
		})
	}
}

func stage4RunSQLiteIncrementalDeletePostSnapshotMutation(
	t *testing.T,
	backend stage4LifecycleTestBackend,
	backendName string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sourcePath, targetPath := stage4SQLiteIncrementalDeleteDatabaseFixture(t, ctx)
	runID := "sqlite-incremental-delete-post-snapshot-mutation-" + backendName
	stage4InitializeSQLiteIncrementalDeleteRun(t, backend, runID, sourcePath, targetPath)
	cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)
	source, target := stage4OpenSQLiteIncrementalDeleteAdapters(t, ctx, sourcePath, targetPath)
	defer func() { _ = source.Close() }()
	defer func() { _ = target.Close() }()

	writer, err := sql.Open("sqlite", sqliteSourceTestURI(sourcePath, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	mutationWriter := &stage4IncrementalDeleteMutationWriter{
		delegate: target.stage4BatchWriter,
		mutate: func() error {
			_, err := writer.ExecContext(
				ctx,
				"INSERT INTO items(id, payload, updated_at) VALUES (3, 'after-fence', '2026-08-01 00:00:03.000')",
			)
			return err
		},
	}
	target.stage4BatchWriter = mutationWriter
	run := Stage4RunContext{
		RunID:          runID,
		Backend:        backend,
		SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
	}
	events := []string{}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	result, err := migrateWithStage4Adapters(
		ctx,
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if mutationWriter.mutationErr != nil {
		t.Fatalf("concurrent SQLite source mutation: %v", mutationWriter.mutationErr)
	}
	if !mutationWriter.called {
		t.Fatal("incremental target write did not reach the source-mutation sentinel")
	}
	if err == nil || !strings.Contains(err.Error(), "source changed after the retained incremental snapshot") {
		t.Fatalf("post-snapshot source mutation error = %v", err)
	}
	if result.Validated {
		t.Fatalf("post-snapshot source mutation unexpectedly validated %#v", result)
	}
	// The transfer is allowed to have completed before this authority check;
	// reconciliation itself must not delete the target-only row.
	stage4AssertSQLiteIncrementalDeleteRows(t, ctx, target.database, []int64{1, 2, 99})
	task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}
	if _, found, loadErr := backend.LoadLatestSuccessfulDeleteReconciliation(runID, task); loadErr != nil || found {
		t.Fatalf("post-snapshot source mutation created delete evidence found=%t err=%v", found, loadErr)
	}

	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	resumedSource, resumedTarget := stage4OpenSQLiteIncrementalDeleteAdapters(t, ctx, sourcePath, targetPath)
	defer func() { _ = resumedSource.Close() }()
	defer func() { _ = resumedTarget.Close() }()
	resumeRun := run
	resumeRun.Resume = true
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    resumeRun,
	}
	_, err = resumeWithStage4Adapters(
		ctx,
		cfg,
		CompletedTableCheckpoints{"items": {Rows: 2}},
		resumeObserver,
		resumeObserver,
		resumedSource,
		resumedTarget,
		"upsert",
		resumeRun,
	)
	if err == nil || !strings.Contains(err.Error(), "completed table items without its original retained source view or a durable source-key plan") {
		t.Fatalf("resume without original source view error = %v", err)
	}
	stage4AssertSQLiteIncrementalDeleteRows(t, ctx, resumedTarget.database, []int64{1, 2, 99})
}

func TestStage4IncrementalDeleteConfigurationKeepsOtherRoutesClosed(
	t *testing.T,
) {
	base := stage4SQLiteIncrementalDeleteConfig("source.db", "target.db")
	for _, route := range []struct {
		name, source, target string
	}{
		{name: "postgres", source: "postgres", target: "postgres"},
		{name: "mysql", source: "mysql", target: "mysql"},
		{name: "mssql", source: "mssql", target: "mssql"},
	} {
		t.Run(route.name, func(t *testing.T) {
			cfg := base
			cfg.Source.Type = route.source
			cfg.Target.Type = route.target
			err := requireStage4AdapterConfigurationSeams(cfg)
			if err == nil {
				t.Fatalf("%s-to-%s incremental+delete route was admitted", route.source, route.target)
			}
			if class := ClassifyTransferError(err); class != ErrorClassPolicy {
				t.Fatalf("%s-to-%s rejection class = %q, want policy: %v", route.source, route.target, class, err)
			}
		})
	}
	if err := requireStage4AdapterConfigurationSeams(base); err != nil {
		t.Fatalf("SQLite-to-SQLite incremental+delete configuration was refused: %v", err)
	}
}

func TestStage4SQLiteIncrementalDeleteRejectsAliasedSourceTargetBeforeWork(
	t *testing.T,
) {
	for _, alias := range []struct {
		name    string
		prepare func(*testing.T, string, string) string
	}{
		{
			name: "same_path",
			prepare: func(_ *testing.T, sourcePath, _ string) string {
				return sourcePath
			},
		},
		{
			name: "hardlink",
			prepare: func(t *testing.T, sourcePath, targetPath string) string {
				t.Helper()
				if err := os.Link(sourcePath, targetPath); err != nil {
					t.Fatal(err)
				}
				return targetPath
			},
		},
		{
			name: "symlink",
			prepare: func(t *testing.T, sourcePath, targetPath string) string {
				t.Helper()
				if err := os.Symlink(sourcePath, targetPath); err != nil {
					t.Fatal(err)
				}
				return targetPath
			},
		},
	} {
		alias := alias
		t.Run(alias.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sourcePath, unusedTargetPath := stage4SQLiteIncrementalDeleteSourceOnlyFixture(t, ctx)
			targetPath := alias.prepare(t, sourcePath, unusedTargetPath)
			backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
			runID := "sqlite-incremental-delete-alias-" + alias.name
			stage4InitializeSQLiteIncrementalDeleteRun(
				t,
				backend,
				runID,
				sourcePath,
				targetPath,
			)
			cfg := stage4SQLiteIncrementalDeleteConfig(sourcePath, targetPath)
			source, target := stage4OpenSQLiteIncrementalDeleteAdapters(
				t,
				ctx,
				sourcePath,
				targetPath,
			)
			defer func() { _ = source.Close() }()
			defer func() { _ = target.Close() }()
			events := []string{}
			run := Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			}
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    run,
			}
			if _, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				run,
			); err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
				!strings.Contains(err.Error(), "requires distinct live source and target databases") {
				t.Fatalf("aliased SQLite incremental+delete error = %v", err)
			}
			if len(events) != 0 {
				t.Fatalf("aliased SQLite route reached table work: %v", events)
			}
			var journalCount int
			if err := source.database.QueryRowContext(
				ctx,
				"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND lower(name) = lower(?)",
				sqliteDeleteJournalTable,
			).Scan(&journalCount); err != nil || journalCount != 0 {
				t.Fatalf("aliased SQLite route created private journal count=%d err=%v", journalCount, err)
			}
		})
	}
}

func stage4SQLiteIncrementalDeleteDatabaseFixture(
	t *testing.T,
	ctx context.Context,
) (string, string) {
	t.Helper()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := openSQLiteTargetDatabase(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (
			id INTEGER NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL,
			updated_at TIMESTAMP(3) NOT NULL
		);
		INSERT INTO items(id, payload, updated_at) VALUES
			(1, 'source-one', '2026-08-01 00:00:01.000'),
			(2, 'source-two', '2026-08-01 00:00:02.000');
	`); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.ExecContext(ctx, `
		CREATE TABLE items (
			id INTEGER NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL,
			updated_at TIMESTAMP(3) NOT NULL
		);
		INSERT INTO items(id, payload, updated_at) VALUES
			(1, 'stale-target-one', '2026-08-01 00:00:00.000'),
			(99, 'target-only', '2026-07-31 00:00:00.000');
	`); err != nil {
		_ = target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	return sourcePath, targetPath
}

func stage4SQLiteIncrementalDeleteSourceOnlyFixture(
	t *testing.T,
	ctx context.Context,
) (string, string) {
	t.Helper()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := openSQLiteTargetDatabase(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (
			id INTEGER NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL,
			updated_at TIMESTAMP(3) NOT NULL
		);
		INSERT INTO items(id, payload, updated_at)
		VALUES (1, 'source-one', '2026-08-01 00:00:01.000');
	`); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	return sourcePath, targetPath
}

func stage4SQLiteIncrementalDeleteConfig(
	sourcePath string,
	targetPath string,
) config.Config {
	return config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{"items"},
			MemoryCeilingBytes: 64 << 20,
			ConnectionLimit:    3,
			Workers:            1,
			ChunkSize:          1,
			Partitions:         1,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			ReadAhead:          1,
			MaxRetries:         0,
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode:           config.ValidationCountOnly,
				FailOnMismatch: true,
				FailOnTimeout:  true,
			},
			Deletes: config.DeletePolicy{
				Mode:           config.DeleteModeReconcile,
				TargetBehavior: config.DeleteTargetHard,
				Reconcile: config.DeleteReconcilePolicy{
					Schedule:          config.DeleteScheduleInterval,
					Interval:          time.Hour,
					BatchSize:         10,
					RequirePrimaryKey: true,
				},
			},
		},
	}
}

func stage4InitializeSQLiteIncrementalDeleteRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	sourcePath string,
	targetPath string,
) {
	t.Helper()
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "sqlite",
		SourceIdentity: "sqlite:" + sourcePath,
		TargetIdentity: "sqlite:" + targetPath,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      time.Now().UTC().Add(-time.Minute),
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
}

func stage4OpenSQLiteIncrementalDeleteAdapters(
	t *testing.T,
	ctx context.Context,
	sourcePath string,
	targetPath string,
) (*sqliteSourceAdapter, *sqliteTargetAdapter) {
	t.Helper()
	sourceValue, err := openSQLiteSourceAdapter(
		ctx,
		config.Endpoint{Type: "sqlite", Database: sourcePath},
	)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := sourceValue.(*sqliteSourceAdapter)
	if !ok {
		_ = sourceValue.Close()
		t.Fatal("SQLite source adapter type differs")
	}
	targetValue, err := openSQLiteTargetAdapter(
		ctx,
		config.Endpoint{Type: "sqlite", Database: targetPath},
	)
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	target, ok := targetValue.(*sqliteTargetAdapter)
	if !ok {
		_ = source.Close()
		_ = targetValue.Close()
		t.Fatal("SQLite target adapter type differs")
	}
	return source, target
}

func stage4AssertSQLiteIncrementalDeleteRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	want []int64,
) {
	t.Helper()
	rows, err := database.QueryContext(ctx, "SELECT id FROM items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make([]int64, 0, len(want))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("target ids = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("target ids = %v, want %v", got, want)
		}
	}
}
