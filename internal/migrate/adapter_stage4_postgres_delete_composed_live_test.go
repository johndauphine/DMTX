package migrate

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4PostgresDeleteLiveFailCommitBackend struct {
	state.YAMLStore
	failNextDeleteCommit bool
}

func (backend *stage4PostgresDeleteLiveFailCommitBackend) CommitDeleteReconciliationBatch(
	commit state.DeleteReconciliationBatchCommit,
) error {
	if backend.failNextDeleteCommit {
		backend.failNextDeleteCommit = false
		return errors.New(
			"injected Stage 4 PostgreSQL delete frontier acknowledgement failure",
		)
	}
	return backend.YAMLStore.CommitDeleteReconciliationBatch(commit)
}

func TestStage4PostgresDeleteCompositionLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL composed delete sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL composed delete DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	sourceSetup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL composed delete source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL composed delete source: %v", err)
		}
	})
	if err := sourceSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL composed delete source: %T", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, sourceSetup, "source")

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_delete_" + suffix
	targetDatabaseName := "dmtx_s4_delete_target_" + suffix
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabaseName),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL composed delete target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+
				postgresIdentifier(targetDatabaseName)+" WITH (FORCE)",
		); err != nil {
			t.Errorf(
				"drop Stage 4 PostgreSQL composed delete target database: %v",
				err,
			)
		}
	})
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL composed delete source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop Stage 4 PostgreSQL composed delete source schema: %v",
				err,
			)
		}
	})

	sourceEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      parsed.Host,
		Port:      int(parsed.Port),
		Database:  parsed.Database,
		User:      parsed.User,
		Password:  parsed.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: caFile,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabaseName
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build Stage 4 PostgreSQL composed delete target DSN: %v", err)
	}
	targetParsed, err := pgx.ParseConfig(targetDSN)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL composed delete target DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(targetParsed) {
		t.Fatal("public target endpoint did not retain certificate and hostname verification")
	}
	targetSetup, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL composed delete target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL composed delete target: %v", err)
		}
	})
	if err := targetSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL composed delete target: %v", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, targetSetup, "target")
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL composed delete target schema: %v", err)
	}

	const (
		parentTable = "a_parent_rows"
		childTable  = "b_child_rows"
		zeroTable   = "c_zero_rows"
	)
	parent := postgresQualified(namespace, parentTable)
	child := postgresQualified(namespace, childTable)
	zero := postgresQualified(namespace, zeroTable)
	parentDDL := ` (
		id bigint NOT NULL,
		payload text NOT NULL,
		CONSTRAINT a_parent_rows_pkey PRIMARY KEY (id)
	)`
	childDDL := ` (
		id bigint NOT NULL,
		parent_id bigint NOT NULL,
		payload text NOT NULL,
		CONSTRAINT b_child_rows_pkey PRIMARY KEY (id),
		CONSTRAINT b_child_rows_parent_fk FOREIGN KEY (parent_id)
			REFERENCES ` + parent + ` (id)
	)`
	zeroDDL := ` (
		id bigint NOT NULL,
		payload text NOT NULL,
		CONSTRAINT c_zero_rows_pkey PRIMARY KEY (id)
	)`
	for label, database := range map[string]*sql.DB{
		"source": sourceSetup,
		"target": targetSetup,
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+parent+parentDDL+"; "+
				"CREATE TABLE "+child+childDDL+"; "+
				"CREATE TABLE "+zero+zeroDDL,
		); err != nil {
			t.Fatalf("create Stage 4 PostgreSQL composed delete %s tables: %v", label, err)
		}
	}
	if _, err := sourceSetup.ExecContext(ctx, `
		INSERT INTO `+parent+` (id, payload) VALUES
			(1, 'source-parent-one'),
			(2, 'source-parent-two');
		INSERT INTO `+child+` (id, parent_id, payload) VALUES
			(11, 1, 'source-child-one'),
			(22, 2, 'source-child-two');
		INSERT INTO `+zero+` (id, payload) VALUES
			(7, 'source-zero');
	`); err != nil {
		t.Fatalf("seed Stage 4 PostgreSQL composed delete source: %v", err)
	}
	if _, err := targetSetup.ExecContext(ctx, `
		INSERT INTO `+parent+` (id, payload) VALUES
			(1, 'stale-parent-one'),
			(2, 'stale-parent-two'),
			(900, 'target-only-parent');
		INSERT INTO `+child+` (id, parent_id, payload) VALUES
			(11, 1, 'stale-child-one'),
			(22, 2, 'stale-child-two'),
			(900, 900, 'target-only-child');
		INSERT INTO `+zero+` (id, payload) VALUES
			(7, 'stale-zero');
	`); err != nil {
		t.Fatalf("seed Stage 4 PostgreSQL composed delete target: %v", err)
	}

	cfg := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode: "upsert",
			IncludeTables: []string{
				parentTable,
				childTable,
				zeroTable,
			},
			ConnectionLimit:    4,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			MemoryCeilingBytes: 64 << 20,
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
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "stage4-pg-delete-composed-" + suffix
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute).UTC(),
	)
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, false),
	}
	result, err := PostgresToPostgresWithObserver(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL composed delete route: %v", err)
	}
	if result != (Result{Tables: 3, Rows: 5, Validated: true}) {
		t.Fatalf("Stage 4 PostgreSQL composed delete result = %#v", result)
	}

	reconciliations := make(map[string]state.DeleteReconciliation, 3)
	for _, table := range []string{parentTable, childTable, zeroTable} {
		task := state.TaskKey{
			Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table,
		}
		record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(
			runID,
			task,
		)
		if err != nil || !found {
			t.Fatalf(
				"load %s delete reconciliation found=%t record=%#v err=%v",
				table,
				found,
				record,
				err,
			)
		}
		outcome, err := terminalDeleteReconcileOutcome(record)
		if err != nil {
			t.Fatalf("authenticate %s terminal delete outcome: %v", table, err)
		}
		strict, err := stage4AdapterPostgresDeleteTerminalStrictness(outcome)
		if err != nil || !strict {
			t.Fatalf(
				"%s reconciliation strict=%t record=%#v err=%v",
				table,
				strict,
				record,
				err,
			)
		}
		reconciliations[table] = record
	}
	if record := reconciliations[zeroTable]; record.Status != state.DeleteReconciliationCompleted ||
		record.Candidates != 0 || record.DeletedRows != 0 ||
		record.CommittedBatches != 0 {
		t.Fatalf("zero-candidate reconciliation = %#v", record)
	}
	for _, table := range []string{childTable, parentTable} {
		record := reconciliations[table]
		if record.Status != state.DeleteReconciliationCompleted ||
			record.Candidates != 1 || record.DeletedRows != 1 ||
			record.CommittedBatches != 1 {
			t.Fatalf("%s reconciliation = %#v", table, record)
		}
	}
	zeroRecord := reconciliations[zeroTable]
	childRecord := reconciliations[childTable]
	parentRecord := reconciliations[parentTable]
	if childRecord.StartedAt.Before(zeroRecord.CompletedAt) ||
		parentRecord.StartedAt.Before(childRecord.CompletedAt) {
		t.Fatalf(
			"delete order is not reverse dependency order: zero=%s..%s child=%s..%s parent=%s..%s",
			zeroRecord.StartedAt,
			zeroRecord.CompletedAt,
			childRecord.StartedAt,
			childRecord.CompletedAt,
			parentRecord.StartedAt,
			parentRecord.CompletedAt,
		)
	}

	for table, want := range map[string]int{
		parent: 2,
		child:  2,
		zero:   1,
	} {
		var count int
		if err := targetSetup.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count); err != nil {
			t.Fatalf("count reconciled target table %s: %v", table, err)
		}
		if count != want {
			t.Fatalf("reconciled target table %s rows=%d want=%d", table, count, want)
		}
	}
	var targetOnly int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT (SELECT COUNT(*) FROM "+parent+" WHERE id = 900) + "+
			"(SELECT COUNT(*) FROM "+child+" WHERE id = 900)",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("read Stage 4 PostgreSQL target-only rows: %v", err)
	}
	if targetOnly != 0 {
		t.Fatalf("Stage 4 PostgreSQL target-only rows remain = %d", targetOnly)
	}
	for table, want := range map[string]string{
		parent: "source-parent-one",
		child:  "source-child-one",
		zero:   "source-zero",
	} {
		var payload string
		if err := targetSetup.QueryRowContext(
			ctx,
			"SELECT payload FROM "+table+" ORDER BY id LIMIT 1",
		).Scan(&payload); err != nil {
			t.Fatalf("read reconciled target payload for %s: %v", table, err)
		}
		if payload != want {
			t.Fatalf("reconciled target payload for %s=%q want=%q", table, payload, want)
		}
	}

	var journalRowsBefore int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		),
	).Scan(&journalRowsBefore); err != nil {
		t.Fatalf("count run-A PostgreSQL delete receipts: %v", err)
	}
	if journalRowsBefore != 2 {
		t.Fatalf("run-A PostgreSQL delete receipts=%d want=2", journalRowsBefore)
	}
	completeStage4IncrementalTestRun(t, backend, runID)
	if _, err := targetSetup.ExecContext(
		ctx,
		"INSERT INTO "+zero+" (id, payload) VALUES (900, 'run-b-target-only')",
	); err != nil {
		t.Fatalf("seed run-B target-only row: %v", err)
	}

	runBID := "stage4-pg-delete-not-due-" + suffix
	initializeStage4LifecycleRun(
		t,
		backend,
		runBID,
		time.Now().UTC(),
	)
	runB := stage4LifecycleRunContext(t, backend, runBID, false)
	runBEvents := make([]string, 0)
	runBObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &runBEvents},
		run:                    runB,
	}
	runBResult, err := PostgresToPostgresWithObserver(ctx, cfg, runBObserver)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL cross-run not-due route: %v", err)
	}
	if runBResult != (Result{Tables: 3, Rows: 5, Validated: true}) {
		t.Fatalf("Stage 4 PostgreSQL cross-run not-due result = %#v", runBResult)
	}
	runBInventory, err := loadStage4WorkInventory(ctx, runB)
	if err != nil {
		t.Fatalf("load run-B Stage 4 network work: %v", err)
	}
	for _, table := range []string{parentTable, childTable, zeroTable} {
		task := state.TaskKey{
			Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: table,
		}
		taskState, found := runBInventory.tasks[task]
		if !found {
			t.Fatalf("run-B network task for %s was not found", table)
		}
		attemptID, err := stage4AdapterPostgresDeleteAttemptID(
			runBID,
			stage4AdapterWork{
				task: task, strategy: taskState.Strategy,
				topology: taskState.TopologyHash,
			},
		)
		if err != nil {
			t.Fatalf("derive run-B delete attempt for %s: %v", table, err)
		}
		record, found, err := backend.LoadDeleteReconciliation(
			runBID,
			task,
			attemptID,
		)
		if err != nil || !found || record.RunID != runBID || record.Due ||
			record.Status != state.DeleteReconciliationNotDue ||
			record.Candidates != 0 || record.DeletedRows != 0 ||
			record.CommittedBatches != 0 || record.LastBatchCommit != nil {
			t.Fatalf(
				"run-B %s not-due record found=%t record=%#v err=%v",
				table,
				found,
				record,
				err,
			)
		}
		outcome, err := terminalDeleteReconcileOutcome(record)
		if err != nil {
			t.Fatalf("authenticate run-B %s not-due outcome: %v", table, err)
		}
		strict, err := stage4AdapterPostgresDeleteTerminalStrictness(outcome)
		if err != nil || strict {
			t.Fatalf(
				"run-B %s not-due strict=%t record=%#v err=%v",
				table,
				strict,
				record,
				err,
			)
		}
	}
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+zero+" WHERE id = 900",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("read run-B preserved target-only row: %v", err)
	}
	if targetOnly != 1 {
		t.Fatalf("run-B not-due reconciliation removed target-only rows: %d", targetOnly)
	}
	var journalRowsAfter int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		),
	).Scan(&journalRowsAfter); err != nil {
		t.Fatalf("count run-B PostgreSQL delete receipts: %v", err)
	}
	if journalRowsAfter != journalRowsBefore {
		t.Fatalf(
			"run-B not-due route mutated delete journal: before=%d after=%d",
			journalRowsBefore,
			journalRowsAfter,
		)
	}
}

func TestStage4PostgresDeleteCompositionCrashResumeLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL composed delete crash-resume sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL delete resume DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	sourceSetup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL delete resume source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL delete resume source: %v", err)
		}
	})
	if err := sourceSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL delete resume source: %T", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, sourceSetup, "resume source")

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_delete_resume_" + suffix
	targetDatabaseName := "dmtx_s4_delete_resume_target_" + suffix
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabaseName),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL delete resume target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+
				postgresIdentifier(targetDatabaseName)+" WITH (FORCE)",
		); err != nil {
			t.Errorf("drop Stage 4 PostgreSQL delete resume target database: %v", err)
		}
	})
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL delete resume source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceSetup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop Stage 4 PostgreSQL delete resume source schema: %v", err)
		}
	})

	sourceEndpoint := config.Endpoint{
		Type:      "postgres",
		Host:      parsed.Host,
		Port:      int(parsed.Port),
		Database:  parsed.Database,
		User:      parsed.User,
		Password:  parsed.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: caFile,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabaseName
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build Stage 4 PostgreSQL delete resume target DSN: %v", err)
	}
	targetParsed, err := pgx.ParseConfig(targetDSN)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL delete resume target DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(targetParsed) {
		t.Fatal("resume target endpoint did not retain certificate and hostname verification")
	}
	targetSetup, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL delete resume target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL delete resume target: %v", err)
		}
	})
	if err := targetSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL delete resume target: %v", err)
	}
	assertStage4PostgresDeleteLiveTLS(t, ctx, targetSetup, "resume target")
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 PostgreSQL delete resume target schema: %v", err)
	}

	const tableName = "items"
	qualified := postgresQualified(namespace, tableName)
	tableDDL := ` (
		id bigint NOT NULL,
		payload text NOT NULL,
		CONSTRAINT items_pkey PRIMARY KEY (id)
	)`
	for label, database := range map[string]*sql.DB{
		"source": sourceSetup,
		"target": targetSetup,
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+qualified+tableDDL,
		); err != nil {
			t.Fatalf("create Stage 4 PostgreSQL delete resume %s table: %v", label, err)
		}
	}
	if _, err := sourceSetup.ExecContext(
		ctx,
		"INSERT INTO "+qualified+" (id, payload) VALUES "+
			"(1, 'source-one'), (2, 'source-two')",
	); err != nil {
		t.Fatalf("seed Stage 4 PostgreSQL delete resume source: %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		"INSERT INTO "+qualified+" (id, payload) VALUES "+
			"(1, 'stale-one'), (2, 'stale-two'), (900, 'target-only')",
	); err != nil {
		t.Fatalf("seed Stage 4 PostgreSQL delete resume target: %v", err)
	}

	cfg := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{tableName},
			ConnectionLimit:    4,
			ReaderParallelism:  1,
			WriterParallelism:  1,
			MemoryCeilingBytes: 64 << 20,
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
	backend := &stage4PostgresDeleteLiveFailCommitBackend{
		YAMLStore:            state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
		failNextDeleteCommit: true,
	}
	runID := "stage4-pg-delete-resume-" + suffix
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute).UTC(),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	freshEvents := make([]string, 0)
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &freshEvents},
		run:                    run,
	}
	_, err = PostgresToPostgresWithObserver(ctx, cfg, freshObserver)
	if err == nil ||
		!strings.Contains(err.Error(), "durable frontier acknowledgement failed") ||
		!strings.Contains(err.Error(), "resume this existing run and attempt") ||
		!strings.Contains(err.Error(), "do not start a fresh run") {
		t.Fatalf("Stage 4 PostgreSQL delete crash-window error = %v", err)
	}
	if ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf(
			"Stage 4 PostgreSQL delete crash-window class=%s want=%s: %v",
			ClassifyTransferError(err),
			ErrorClassState,
			err,
		)
	}
	var targetOnly int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+qualified+" WHERE id = 900",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("read post-crash Stage 4 PostgreSQL target-only row: %v", err)
	}
	if targetOnly != 0 {
		t.Fatalf("target delete did not commit before injected state failure: %d", targetOnly)
	}
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: namespace, Table: tableName,
	}
	preResumeInventory, err := loadStage4WorkInventory(ctx, run)
	if err != nil {
		t.Fatalf("load pre-resume Stage 4 PostgreSQL work: %v", err)
	}
	preResumeTask, found := preResumeInventory.tasks[task]
	if !found || preResumeTask.Status != "running" ||
		preResumeTask.Attempts <= 0 {
		t.Fatalf("pre-resume Stage 4 PostgreSQL task = %#v found=%t", preResumeTask, found)
	}
	preResumeRanges := append(
		[]state.RangeState(nil),
		preResumeInventory.ranges[task]...,
	)
	if len(preResumeRanges) == 0 {
		t.Fatal("pre-resume Stage 4 PostgreSQL task has no durable ranges")
	}
	for _, workRange := range preResumeRanges {
		if workRange.Status != "completed" || workRange.Attempts <= 0 ||
			workRange.RowsDone != 2 || len(workRange.Pending) != 0 ||
			workRange.CompletedAt.IsZero() {
			t.Fatalf("pre-resume Stage 4 PostgreSQL range = %#v", workRange)
		}
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(
		runID,
		stage4AdapterWork{
			task: task, strategy: preResumeTask.Strategy,
			topology: preResumeTask.TopologyHash,
		},
	)
	if err != nil {
		t.Fatalf("derive pre-resume Stage 4 PostgreSQL delete attempt: %v", err)
	}
	runningRecord, found, err := backend.LoadDeleteReconciliation(
		runID,
		task,
		attemptID,
	)
	if err != nil || !found ||
		runningRecord.Status != state.DeleteReconciliationRunning ||
		runningRecord.AttemptID != attemptID || runningRecord.Plan == nil ||
		runningRecord.PendingBatch == nil || runningRecord.LastBatchCommit != nil ||
		runningRecord.Frontier != 0 || runningRecord.CommittedBatches != 0 {
		t.Fatalf(
			"pre-resume Stage 4 PostgreSQL delete record found=%t record=%#v err=%v",
			found,
			runningRecord,
			err,
		)
	}
	runningPlan := *runningRecord.Plan
	runningPending := *runningRecord.PendingBatch
	var receiptRowsBefore int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		)+" WHERE token = $1",
		runningPending.Token,
	).Scan(&receiptRowsBefore); err != nil {
		t.Fatalf("count pre-resume PostgreSQL delete receipt: %v", err)
	}
	if receiptRowsBefore != 1 {
		t.Fatalf("pre-resume PostgreSQL delete receipt rows=%d want=1", receiptRowsBefore)
	}

	resumeRun := run
	resumeRun.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &resumeEvents},
		run:                    resumeRun,
	}
	result, err := ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf("resume Stage 4 PostgreSQL composed delete route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("resumed Stage 4 PostgreSQL delete result = %#v", result)
	}
	record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(
		runID,
		task,
	)
	if err != nil || !found ||
		record.Status != state.DeleteReconciliationCompleted ||
		record.AttemptID != runningRecord.AttemptID || record.Plan == nil ||
		!reflect.DeepEqual(*record.Plan, runningPlan) ||
		record.Candidates != 1 || record.DeletedRows != 1 ||
		record.Frontier != 1 || record.CommittedBatches != 1 ||
		record.PendingBatch != nil || record.LastBatchCommit == nil {
		t.Fatalf(
			"resumed Stage 4 PostgreSQL delete record found=%t record=%#v err=%v",
			found,
			record,
			err,
		)
	}
	commit := *record.LastBatchCommit
	if commit.PlanID != runningPending.PlanID ||
		commit.Token != runningPending.Token ||
		commit.Sequence != runningPending.Sequence ||
		commit.FirstCandidate != runningPending.FirstCandidate ||
		commit.BatchDigest != runningPending.BatchDigest ||
		commit.Candidates != runningPending.Candidates ||
		commit.EncodedBytes != runningPending.EncodedBytes ||
		commit.DeletedRows != runningPending.Candidates ||
		commit.ReceiptDigest == "" {
		t.Fatalf(
			"resumed delete commit=%#v does not match pending batch=%#v",
			commit,
			runningPending,
		)
	}
	var (
		receiptVersion     int16
		receiptPlanID      string
		receiptToken       string
		receiptSequence    int64
		receiptBatchDigest string
		receiptCandidates  int64
		receiptDeleted     int64
		receiptDigest      string
	)
	if err := targetSetup.QueryRowContext(
		ctx,
		`SELECT journal_version, plan_id, token, sequence,
		        batch_digest, candidates, deleted_rows, receipt_digest
		   FROM `+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		)+` WHERE token = $1`,
		runningPending.Token,
	).Scan(
		&receiptVersion,
		&receiptPlanID,
		&receiptToken,
		&receiptSequence,
		&receiptBatchDigest,
		&receiptCandidates,
		&receiptDeleted,
		&receiptDigest,
	); err != nil {
		t.Fatalf("read resumed PostgreSQL delete receipt: %v", err)
	}
	var receiptRowsAfter int
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+postgresQualified(
			postgresDeleteJournalSchema,
			postgresDeleteJournalTable,
		)+" WHERE token = $1",
		runningPending.Token,
	).Scan(&receiptRowsAfter); err != nil {
		t.Fatalf("count resumed PostgreSQL delete receipt: %v", err)
	}
	if receiptRowsAfter != 1 || receiptVersion != postgresDeleteJournalVersion ||
		receiptPlanID != commit.PlanID || receiptToken != commit.Token ||
		receiptSequence != commit.Sequence ||
		receiptBatchDigest != commit.BatchDigest ||
		receiptCandidates != commit.Candidates ||
		receiptDeleted != commit.DeletedRows ||
		receiptDigest != commit.ReceiptDigest {
		t.Fatalf(
			"resumed PostgreSQL receipt rows=%d version=%d plan=%q token=%q sequence=%d digest=%q candidates=%d deleted=%d receipt=%q commit=%#v",
			receiptRowsAfter,
			receiptVersion,
			receiptPlanID,
			receiptToken,
			receiptSequence,
			receiptBatchDigest,
			receiptCandidates,
			receiptDeleted,
			receiptDigest,
			commit,
		)
	}
	postResumeInventory, err := loadStage4WorkInventory(ctx, resumeRun)
	if err != nil {
		t.Fatalf("load post-resume Stage 4 PostgreSQL work: %v", err)
	}
	postResumeTask, found := postResumeInventory.tasks[task]
	if !found || postResumeTask.Status != "completed" ||
		postResumeTask.Attempts != preResumeTask.Attempts ||
		postResumeTask.Retries != preResumeTask.Retries ||
		postResumeTask.Strategy != preResumeTask.Strategy ||
		postResumeTask.TopologyHash != preResumeTask.TopologyHash ||
		!reflect.DeepEqual(postResumeInventory.ranges[task], preResumeRanges) {
		t.Fatalf(
			"resume recopied/reset network work: before_task=%#v after_task=%#v before_ranges=%#v after_ranges=%#v",
			preResumeTask,
			postResumeTask,
			preResumeRanges,
			postResumeInventory.ranges[task],
		)
	}
	outcome, err := terminalDeleteReconcileOutcome(record)
	if err != nil {
		t.Fatalf("authenticate resumed terminal delete outcome: %v", err)
	}
	strict, err := stage4AdapterPostgresDeleteTerminalStrictness(outcome)
	if err != nil || !strict {
		t.Fatalf("resumed delete strict=%t record=%#v err=%v", strict, record, err)
	}
	var (
		rowCount int
		payload  string
	)
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT COUNT(*), MIN(payload) FILTER (WHERE id = 1) FROM "+qualified,
	).Scan(&rowCount, &payload); err != nil {
		t.Fatalf("read resumed Stage 4 PostgreSQL target: %v", err)
	}
	if rowCount != 2 || payload != "source-one" {
		t.Fatalf("resumed Stage 4 PostgreSQL target rows=%d payload=%q", rowCount, payload)
	}
}

func stage4PostgresDeleteLiveCAFile(t *testing.T, dsn string) string {
	t.Helper()
	if value := strings.TrimSpace(os.Getenv("PGSSLROOTCERT")); value != "" {
		return stage4PostgresDeleteRequireLiveCAFile(t, value)
	}
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse PostgreSQL live DSN for sslrootcert: %T", err)
		}
		if value := parsed.Query().Get("sslrootcert"); value != "" {
			return stage4PostgresDeleteRequireLiveCAFile(t, value)
		}
	}
	if value, found := stage4PostgresDeleteKeywordValue(dsn, "sslrootcert"); found {
		return stage4PostgresDeleteRequireLiveCAFile(t, value)
	}
	t.Fatal(
		"DMTX_TEST_POSTGRES_DSN verifies TLS but does not expose sslrootcert; set PGSSLROOTCERT so the public endpoint can retain verify-full",
	)
	return ""
}

func stage4PostgresDeleteRequireLiveCAFile(t *testing.T, value string) string {
	t.Helper()
	value = strings.TrimSpace(value)
	if value == "" || value == "system" {
		t.Fatal("Stage 4 PostgreSQL composed delete sentinel requires an explicit CA file")
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		t.Fatalf("resolve Stage 4 PostgreSQL live CA file: %v", err)
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("inspect Stage 4 PostgreSQL live CA file: %v", err)
	}
	return absolute
}

func stage4PostgresDeleteKeywordValue(dsn, wanted string) (string, bool) {
	for offset := 0; offset < len(dsn); {
		for offset < len(dsn) && (dsn[offset] == ' ' || dsn[offset] == '\t' ||
			dsn[offset] == '\n' || dsn[offset] == '\r') {
			offset++
		}
		keyStart := offset
		for offset < len(dsn) && dsn[offset] != '=' && dsn[offset] != ' ' &&
			dsn[offset] != '\t' && dsn[offset] != '\n' && dsn[offset] != '\r' {
			offset++
		}
		key := dsn[keyStart:offset]
		for offset < len(dsn) && (dsn[offset] == ' ' || dsn[offset] == '\t') {
			offset++
		}
		if offset >= len(dsn) || dsn[offset] != '=' {
			for offset < len(dsn) && dsn[offset] != ' ' && dsn[offset] != '\t' &&
				dsn[offset] != '\n' && dsn[offset] != '\r' {
				offset++
			}
			continue
		}
		offset++
		for offset < len(dsn) && (dsn[offset] == ' ' || dsn[offset] == '\t') {
			offset++
		}
		var value strings.Builder
		quote := byte(0)
		if offset < len(dsn) && (dsn[offset] == '\'' || dsn[offset] == '"') {
			quote = dsn[offset]
			offset++
		}
		for offset < len(dsn) {
			character := dsn[offset]
			if character == '\\' && offset+1 < len(dsn) {
				offset++
				value.WriteByte(dsn[offset])
				offset++
				continue
			}
			if quote != 0 {
				if character == quote {
					offset++
					break
				}
			} else if character == ' ' || character == '\t' ||
				character == '\n' || character == '\r' {
				break
			}
			value.WriteByte(character)
			offset++
		}
		if key == wanted {
			return value.String(), true
		}
	}
	return "", false
}

func assertStage4PostgresDeleteLiveTLS(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	label string,
) {
	t.Helper()
	var (
		encrypted bool
		version   string
		cipher    string
	)
	if err := database.QueryRowContext(ctx, `
		SELECT ssl, version, cipher
		  FROM pg_catalog.pg_stat_ssl
		 WHERE pid = pg_catalog.pg_backend_pid()
	`).Scan(&encrypted, &version, &cipher); err != nil {
		t.Fatalf("inspect Stage 4 PostgreSQL composed delete %s TLS: %v", label, err)
	}
	if !encrypted || strings.TrimSpace(version) == "" ||
		strings.TrimSpace(cipher) == "" {
		t.Fatalf(
			"Stage 4 PostgreSQL composed delete %s TLS evidence = encrypted:%t version:%q cipher:%q",
			label,
			encrypted,
			version,
			cipher,
		)
	}
}

func TestStage4PostgresDeleteKeywordValue(t *testing.T) {
	for _, test := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "plain",
			dsn:  "host=localhost sslrootcert=/tmp/ca.pem dbname=dmtx",
			want: "/tmp/ca.pem",
		},
		{
			name: "single quoted",
			dsn:  "host=localhost sslrootcert='/tmp/test ca.pem' dbname=dmtx",
			want: "/tmp/test ca.pem",
		},
		{
			name: "escaped whitespace",
			dsn:  `host=localhost sslrootcert=/tmp/test\ ca.pem dbname=dmtx`,
			want: "/tmp/test ca.pem",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, found := stage4PostgresDeleteKeywordValue(test.dsn, "sslrootcert")
			if !found || got != test.want {
				t.Fatalf("sslrootcert=%q found=%t want=%q", got, found, test.want)
			}
		})
	}
	if _, found := stage4PostgresDeleteKeywordValue(
		"host=localhost dbname=dmtx",
		"sslrootcert",
	); found {
		t.Fatal("missing sslrootcert was reported present")
	}
}
