package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4IncrementalLiveFailAckBackend struct {
	state.YAMLStore
	failNext bool
}

func (backend *stage4IncrementalLiveFailAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	if backend.failNext &&
		acknowledgement.Task.Type == stage4AdapterNetworkTaskType &&
		acknowledgement.RangeID == stage4AdapterIncrementalRangeID {
		backend.failNext = false
		return state.RangeState{}, errors.New(
			"injected Stage 4 incremental acknowledgement failure",
		)
	}
	return backend.YAMLStore.AcknowledgeRange(acknowledgement)
}

func TestStage4PostgresIncrementalCompositionLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL incremental composition sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse Stage 4 PostgreSQL incremental DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require verified TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		150*time.Second,
	)
	defer cancel()
	sourceSetup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL incremental source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL incremental source: %v", err)
		}
	})
	if err := sourceSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL incremental source: %T", err)
	}
	assertStage4IncrementalPostgresTLS(
		t,
		ctx,
		sourceSetup,
		"source",
	)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_inc_" + suffix
	tableName := "events"
	targetDatabaseName := "dmtx_s4_inc_target_" + suffix
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabaseName),
	); err != nil {
		t.Fatalf("create Stage 4 incremental target database: %v", err)
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
				postgresIdentifier(targetDatabaseName)+
				" WITH (FORCE)",
		); err != nil {
			t.Errorf("drop Stage 4 incremental target database: %v", err)
		}
	})
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 incremental source schema: %v", err)
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
			t.Errorf("drop Stage 4 incremental source schema: %v", err)
		}
	})

	sourceEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabaseName
	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	targetSetup, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open Stage 4 PostgreSQL incremental target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetSetup.Close(); err != nil {
			t.Errorf("close Stage 4 PostgreSQL incremental target: %v", err)
		}
	})
	if err := targetSetup.PingContext(ctx); err != nil {
		t.Fatalf("ping Stage 4 PostgreSQL incremental target: %v", err)
	}
	assertStage4IncrementalPostgresTLS(
		t,
		ctx,
		targetSetup,
		"target",
	)
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 incremental target schema: %v", err)
	}
	sourceTable := postgresQualified(namespace, tableName)
	targetTable := postgresQualified(namespace, tableName)
	tableDDL := ` (
		id bigint NOT NULL PRIMARY KEY,
		payload text NOT NULL,
		updated_at timestamp(3)
	)`
	if _, err := sourceSetup.ExecContext(
		ctx,
		"CREATE TABLE "+sourceTable+tableDDL+`;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at) VALUES
			(1, 'baseline-one', timestamp '2026-07-30 10:00:00.000'),
			(2, 'baseline-null', NULL),
			(3, 'baseline-equal', timestamp '2026-07-30 10:00:00.000')`,
	); err != nil {
		t.Fatalf("create Stage 4 incremental source table: %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE TABLE "+targetTable+tableDDL,
	); err != nil {
		t.Fatalf("create Stage 4 incremental target table: %v", err)
	}

	cfg := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:         "upsert",
			IncludeTables:      []string{tableName},
			DateUpdatedColumns: []string{"updated_at"},
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
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
	stateStore := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: namespace,
		Table:  tableName,
	}

	baselineRun := "stage4-pg-incremental-baseline"
	initializeStage4LifecycleRun(
		t,
		stateStore,
		baselineRun,
		time.Now().Add(-time.Minute),
	)
	baselineEvents := make([]string, 0)
	baselineObserver := stage4IncrementalTestObserver{
		events:  &baselineEvents,
		backend: stateStore,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			baselineRun,
			false,
		),
	}
	result, err := PostgresToPostgresWithObserver(
		ctx,
		cfg,
		baselineObserver,
	)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL incremental baseline: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("baseline result = %#v", result)
	}
	baselineAttempt, found, err :=
		stateStore.LoadLatestCommittedIncrementalAttempt(
			baselineRun,
			task,
		)
	if err != nil || !found ||
		baselineAttempt.CommittedWatermark == nil ||
		!baselineAttempt.CommittedWatermark.Value.Equal(
			time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
		) {
		t.Fatalf(
			"baseline committed attempt found=%v attempt=%#v err=%v",
			found,
			baselineAttempt,
			err,
		)
	}
	completeStage4IncrementalTestRun(t, stateStore, baselineRun)

	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'window-one',
		        updated_at = timestamp '2026-07-30 10:01:00.000'
		  WHERE id = 1;
		 UPDATE `+sourceTable+`
		    SET payload = 'equal-lower-must-not-copy'
		  WHERE id = 3;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at)
		 VALUES (4, 'window-four', timestamp '2026-07-30 10:02:00.000')`,
	); err != nil {
		t.Fatalf("prepare Stage 4 strict-lower window: %v", err)
	}
	windowRun := "stage4-pg-incremental-window"
	initializeStage4LifecycleRun(
		t,
		stateStore,
		windowRun,
		time.Now().Add(-time.Minute),
	)
	windowEvents := make([]string, 0)
	windowObserver := stage4IncrementalTestObserver{
		events:  &windowEvents,
		backend: stateStore,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			windowRun,
			false,
		),
	}
	result, err = PostgresToPostgresWithObserver(
		ctx,
		cfg,
		windowObserver,
	)
	if err != nil {
		t.Fatalf("run Stage 4 PostgreSQL strict-lower window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("strict-lower result = %#v", result)
	}
	var equalLowerPayload string
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT payload FROM "+targetTable+" WHERE id = 3",
	).Scan(&equalLowerPayload); err != nil {
		t.Fatalf("read strict-lower target row: %v", err)
	}
	if equalLowerPayload != "baseline-equal" {
		t.Fatalf(
			"equal-lower row was copied: payload=%q",
			equalLowerPayload,
		)
	}

	if _, err := targetSetup.ExecContext(
		ctx,
		"UPDATE "+targetTable+
			" SET payload = 'tampered' WHERE id = 1",
	); err != nil {
		t.Fatalf("tamper completed incremental target: %v", err)
	}
	tamperEvents := make([]string, 0)
	tamperObserver := stage4IncrementalTestObserver{
		events:  &tamperEvents,
		backend: stateStore,
		resume:  true,
		run: stage4LifecycleRunContext(
			t,
			stateStore,
			windowRun,
			true,
		),
	}
	_, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{
			tableName: {Rows: 2},
		},
		tamperObserver,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"revalidate exact completed Stage 4 incremental target values",
		) {
		t.Fatalf("completed-target tamper resume error = %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		`UPDATE `+targetTable+`
		    SET payload = 'window-one',
		        updated_at = timestamp '2026-07-30 10:01:00.000'
		  WHERE id = 1`,
	); err != nil {
		t.Fatalf("repair completed incremental target: %v", err)
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{
			tableName: {Rows: 2},
		},
		tamperObserver,
	)
	if err != nil {
		t.Fatalf("revalidate repaired completed window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("completed-window reuse result = %#v", result)
	}
	completeStage4IncrementalTestRun(t, stateStore, windowRun)

	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'crash-four',
		        updated_at = timestamp '2026-07-30 10:03:00.000'
		  WHERE id = 4;
		 INSERT INTO `+sourceTable+` (id, payload, updated_at)
		 VALUES (6, 'crash-six', timestamp '2026-07-30 10:04:00.000')`,
	); err != nil {
		t.Fatalf("prepare Stage 4 crash window: %v", err)
	}
	crashRun := "stage4-pg-incremental-crash-resume"
	failingStore := &stage4IncrementalLiveFailAckBackend{
		YAMLStore: stateStore,
		failNext:  true,
	}
	initializeStage4LifecycleRun(
		t,
		failingStore,
		crashRun,
		time.Now().Add(-time.Minute),
	)
	crashEvents := make([]string, 0)
	crashObserver := stage4IncrementalTestObserver{
		events:  &crashEvents,
		backend: failingStore,
		run: stage4LifecycleRunContext(
			t,
			failingStore,
			crashRun,
			false,
		),
	}
	result, err = PostgresToPostgresWithObserver(
		ctx,
		cfg,
		crashObserver,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"injected Stage 4 incremental acknowledgement failure",
		) {
		t.Fatalf(
			"crash-window result=%#v error=%v",
			result,
			err,
		)
	}
	active, found, err := failingStore.LoadActiveIncrementalAttempt(
		crashRun,
		task,
	)
	immutableUpper := time.Date(
		2026,
		7,
		30,
		10,
		4,
		0,
		0,
		time.UTC,
	)
	if err != nil || !found ||
		active.UpperFence == nil ||
		!active.UpperFence.Value.Equal(immutableUpper) {
		t.Fatalf(
			"active crash fence found=%v attempt=%#v err=%v",
			found,
			active,
			err,
		)
	}
	if _, err := sourceSetup.ExecContext(
		ctx,
		`UPDATE `+sourceTable+`
		    SET payload = 'inserted-inside-stored-window',
		        updated_at = timestamp '2026-07-30 10:03:30.000'
		  WHERE id = 1`,
	); err != nil {
		t.Fatalf("mutate an already-passed key inside stored window: %v", err)
	}
	resumeEvents := make([]string, 0)
	resumeObserver := stage4IncrementalTestObserver{
		events:  &resumeEvents,
		backend: failingStore,
		resume:  true,
		run: stage4LifecycleRunContext(
			t,
			failingStore,
			crashRun,
			true,
		),
	}
	result, err = ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
	)
	if err != nil {
		t.Fatalf("resume full immutable PostgreSQL window: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("crash-resume result = %#v", result)
	}
	committed, found, err :=
		failingStore.LoadLatestCommittedIncrementalAttempt(
			crashRun,
			task,
		)
	if err != nil || !found ||
		committed.CommittedWatermark == nil ||
		!committed.CommittedWatermark.Value.Equal(immutableUpper) {
		t.Fatalf(
			"crash-resume committed fence found=%v attempt=%#v err=%v",
			found,
			committed,
			err,
		)
	}
	var replayedPayload string
	if err := targetSetup.QueryRowContext(
		ctx,
		"SELECT payload FROM "+targetTable+" WHERE id = 1",
	).Scan(&replayedPayload); err != nil {
		t.Fatalf("read full-window replayed target row: %v", err)
	}
	if replayedPayload != "inserted-inside-stored-window" {
		t.Fatalf(
			"full-window resume missed the earlier key: payload=%q",
			replayedPayload,
		)
	}
}

func assertStage4IncrementalPostgresTLS(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	label string,
) {
	t.Helper()
	var tlsActive bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect Stage 4 incremental %s TLS: %v", label, err)
	}
	if !tlsActive {
		t.Fatalf(
			"Stage 4 incremental %s established a non-TLS session",
			label,
		)
	}
}

var _ stage4IncrementalTestState = (*stage4IncrementalLiveFailAckBackend)(nil)

var _ state.Stage4AggregateBackend = (*stage4IncrementalLiveFailAckBackend)(nil)
