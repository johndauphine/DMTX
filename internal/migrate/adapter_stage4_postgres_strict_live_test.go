package migrate

import (
	"context"
	"database/sql"
	"fmt"
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

type stage4PostgresStrictMutationBackend struct {
	Stage4StateBackend

	once   sync.Once
	mutate func() error
	err    error
}

func (backend *stage4PostgresStrictMutationBackend) SaveStrictSnapshotEvidence(
	evidence state.StrictSnapshotEvidence,
) error {
	if err := backend.Stage4StateBackend.
		SaveStrictSnapshotEvidence(evidence); err != nil {
		return err
	}
	backend.once.Do(func() {
		if backend.mutate == nil {
			backend.err = fmt.Errorf(
				"strict live source mutation callback is unavailable",
			)
			return
		}
		backend.err = backend.mutate()
	})
	return backend.err
}

type stage4PostgresStrictFailTableAckBackend struct {
	Stage4StateBackend

	mu        sync.Mutex
	failTable string
	failed    bool
}

func (backend *stage4PostgresStrictFailTableAckBackend) AcknowledgeRange(
	acknowledgement state.RangeAcknowledgement,
) (state.RangeState, error) {
	backend.mu.Lock()
	fail := !backend.failed &&
		acknowledgement.Task.Table == backend.failTable
	if fail {
		backend.failed = true
	}
	backend.mu.Unlock()
	if fail {
		return state.RangeState{}, fmt.Errorf(
			"injected durable acknowledgement failure for table %s",
			acknowledgement.Task.Table,
		)
	}
	return backend.Stage4StateBackend.
		AcknowledgeRange(acknowledgement)
}

func TestStage4PostgresStrictComposedRouteStableEpochLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL strict composed-route sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 strict DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 strict database: %T", err)
	}
	database.SetMaxOpenConns(12)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf(
				"close PostgreSQL Stage 4 strict database: %v",
				err,
			)
		}
	})
	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf(
			"verify PostgreSQL Stage 4 strict TLS endpoint: %T",
			err,
		)
	}

	for _, scope := range []string{
		config.StrictConsistencyTable,
		config.StrictConsistencyMigration,
	} {
		scope := scope
		t.Run(scope, func(t *testing.T) {
			suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
			sourceNamespace := "dmtx_s4_strict_source_" + suffix
			targetNamespace := "dmtx_s4_strict_target_" + suffix
			for _, namespace := range []string{
				sourceNamespace,
				targetNamespace,
			} {
				if _, err := database.ExecContext(
					ctx,
					"CREATE SCHEMA "+
						postgresIdentifier(namespace),
				); err != nil {
					t.Fatalf(
						"create PostgreSQL strict schema: %v",
						err,
					)
				}
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cleanupCancel()
				for _, namespace := range []string{
					sourceNamespace,
					targetNamespace,
				} {
					if _, err := database.ExecContext(
						cleanupCtx,
						"DROP SCHEMA IF EXISTS "+
							postgresIdentifier(namespace)+
							" CASCADE",
					); err != nil {
						t.Errorf(
							"drop PostgreSQL strict schema %s: %v",
							namespace,
							err,
						)
					}
				}
			})
			sourceQualified := postgresQualified(
				sourceNamespace,
				"items",
			)
			targetQualified := postgresQualified(
				targetNamespace,
				"items",
			)
			for _, qualified := range []string{
				sourceQualified,
				targetQualified,
			} {
				if _, err := database.ExecContext(ctx, `
					CREATE TABLE `+qualified+` (
						id bigint PRIMARY KEY,
						payload text NOT NULL
					)`); err != nil {
					t.Fatalf(
						"create PostgreSQL strict table: %v",
						err,
					)
				}
			}
			if _, err := database.ExecContext(ctx, `
				INSERT INTO `+sourceQualified+` (id, payload)
				VALUES
					(1, 'one'),
					(2, 'two'),
					(3, 'three')`); err != nil {
				t.Fatalf(
					"seed PostgreSQL strict source: %v",
					err,
				)
			}

			source := &relationalSourceAdapter{
				spec: relationalSourceSpec{
					engine:         "postgres",
					displayName:    "PostgreSQL",
					listTables:     engine.ListPostgresTables,
					inspectTable:   engine.InspectPostgresTable,
					readQuery:      postgresReadQuery,
					qualifiedTable: postgresQualified,
				},
				database:  database,
				namespace: sourceNamespace,
			}
			target := &postgresTargetAdapter{
				database:    database,
				batchWriter: newPostgresNativeWriter(database),
				namespace:   targetNamespace,
			}
			rawBackend := state.YAMLStore{
				Path: filepath.Join(
					t.TempDir(),
					"state.yaml",
				),
			}
			runID := "stage4-pg-strict-" + scope + "-" + suffix
			initializeStage4LifecycleRun(
				t,
				rawBackend,
				runID,
				time.Now().Add(-time.Minute),
			)
			mutationCtx, mutationCancel := context.WithTimeout(
				ctx,
				5*time.Second,
			)
			defer mutationCancel()
			backend := &stage4PostgresStrictMutationBackend{
				Stage4StateBackend: rawBackend,
				mutate: func() error {
					_, err := database.ExecContext(
						mutationCtx,
						"INSERT INTO "+sourceQualified+
							" (id, payload) VALUES "+
							"(4, 'concurrent')",
					)
					return err
				},
			}
			events := make([]string, 0)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &events,
				},
				run: stage4LifecycleRunContext(
					t,
					backend,
					runID,
					false,
				),
			}
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Source = config.Endpoint{
				Type:     "postgres",
				Host:     parsed.Host,
				Port:     int(parsed.Port),
				Database: parsed.Database,
				User:     parsed.User,
				Password: parsed.Password,
				Schema:   sourceNamespace,
			}
			cfg.Target = cfg.Source
			cfg.Target.Schema = targetNamespace
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.IncludeTables = []string{"items"}
			cfg.Migration.Partitions = 2
			cfg.Migration.StrictConsistency = true
			cfg.Migration.StrictConsistencyScope = scope
			cfg.Migration.Validation.Mode =
				config.ValidationSample

			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				observer.run,
			)
			if err != nil {
				t.Fatalf(
					"run PostgreSQL strict %s route: %v",
					scope,
					err,
				)
			}
			if result != (Result{
				Tables:    1,
				Rows:      3,
				Validated: true,
			}) {
				t.Fatalf(
					"PostgreSQL strict %s result = %#v",
					scope,
					result,
				)
			}
			if backend.err != nil {
				t.Fatalf(
					"concurrent PostgreSQL source write: %v",
					backend.err,
				)
			}
			for _, check := range []struct {
				qualified string
				want      int
			}{
				{sourceQualified, 4},
				{targetQualified, 3},
			} {
				var count int
				if err := database.QueryRowContext(
					ctx,
					"SELECT COUNT(*) FROM "+check.qualified,
				).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != check.want {
					t.Fatalf(
						"PostgreSQL strict count for %s = %d, want %d",
						check.qualified,
						count,
						check.want,
					)
				}
			}
			tasks, _, err := rawBackend.ListWork(runID)
			if err != nil {
				t.Fatal(err)
			}
			var work state.WorkTask
			for _, candidate := range tasks {
				if candidate.Key.Type ==
					stage4AdapterNetworkTaskType {
					work = candidate
				}
			}
			attemptID, err := BuildStrictConsistencyAttemptID(
				work.Key,
				work.TopologyHash,
				0,
			)
			if err != nil {
				t.Fatal(err)
			}
			evidence, found, err := rawBackend.
				LoadStrictSnapshotEvidence(
					runID,
					work.Key,
					attemptID,
				)
			if err != nil {
				t.Fatal(err)
			}
			if !found ||
				evidence.ExactSourceRowCount != 3 ||
				evidence.Scope !=
					state.StrictSnapshotScope(scope) {
				t.Fatalf(
					"PostgreSQL strict durable evidence = %#v found=%t",
					evidence,
					found,
				)
			}
			assertPostgresStrictSnapshotReleased(
				t,
				ctx,
				database,
				evidence.SnapshotReference,
			)
		})
	}
}

func TestStage4PostgresStrictResumeUsesNewEpochAndReplaysLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL strict resume sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 strict resume DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 strict resume database: %T", err)
	}
	database.SetMaxOpenConns(12)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf(
				"close PostgreSQL Stage 4 strict resume database: %v",
				err,
			)
		}
	})
	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf(
			"verify PostgreSQL Stage 4 strict resume TLS endpoint: %T",
			err,
		)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceNamespace := "dmtx_s4_strict_resume_source_" + suffix
	targetNamespace := "dmtx_s4_strict_resume_target_" + suffix
	for _, namespace := range []string{
		sourceNamespace,
		targetNamespace,
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(namespace),
		); err != nil {
			t.Fatalf(
				"create PostgreSQL strict resume schema: %v",
				err,
			)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, namespace := range []string{
			sourceNamespace,
			targetNamespace,
		} {
			if _, err := database.ExecContext(
				cleanupCtx,
				"DROP SCHEMA IF EXISTS "+
					postgresIdentifier(namespace)+" CASCADE",
			); err != nil {
				t.Errorf(
					"drop PostgreSQL strict resume schema %s: %v",
					namespace,
					err,
				)
			}
		}
	})
	sourceQualified := postgresQualified(sourceNamespace, "items")
	targetQualified := postgresQualified(targetNamespace, "items")
	for _, qualified := range []string{
		sourceQualified,
		targetQualified,
	} {
		if _, err := database.ExecContext(ctx, `
			CREATE TABLE `+qualified+` (
				id bigint PRIMARY KEY,
				payload text NOT NULL
			)`); err != nil {
			t.Fatalf(
				"create PostgreSQL strict resume table: %v",
				err,
			)
		}
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO `+sourceQualified+` (id, payload)
		VALUES
			(1, 'one'),
			(2, 'two'),
			(3, 'three')`); err != nil {
		t.Fatalf("seed PostgreSQL strict resume source: %v", err)
	}

	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:         "postgres",
			displayName:    "PostgreSQL",
			listTables:     engine.ListPostgresTables,
			inspectTable:   engine.InspectPostgresTable,
			readQuery:      postgresReadQuery,
			qualifiedTable: postgresQualified,
		},
		database:  database,
		namespace: sourceNamespace,
	}
	target := &postgresTargetAdapter{
		database:    database,
		batchWriter: newPostgresNativeWriter(database),
		namespace:   targetNamespace,
	}
	rawBackend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-pg-strict-resume-" + suffix
	initializeStage4LifecycleRun(
		t,
		rawBackend,
		runID,
		time.Now().Add(-time.Minute),
	)
	backend := &stage4NetworkLifecycleFailAckBackend{
		Stage4StateBackend: rawBackend,
		failNext:           true,
	}
	events := make([]string, 0)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &events,
		},
		run: run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Source = config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   sourceNamespace,
	}
	cfg.Target = cfg.Source
	cfg.Target.Schema = targetNamespace
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{"items"}
	cfg.Migration.Partitions = 1
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope =
		config.StrictConsistencyMigration
	cfg.Migration.Validation.Mode = config.ValidationSample

	result, err := migrateWithStage4Adapters(
		ctx,
		cfg,
		freshObserver,
		source,
		target,
		"upsert",
		freshObserver.run,
	)
	if err == nil ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(
			err.Error(),
			"injected durable acknowledgement failure",
		) {
		t.Fatalf(
			"first PostgreSQL strict result=%#v error=%v, want injected acknowledgement failure",
			result,
			err,
		)
	}
	oldWork, _ := stage4NetworkLifecycleWork(
		t,
		rawBackend,
		runID,
		"items",
	)
	oldAttemptID, err := BuildStrictConsistencyAttemptID(
		oldWork.Key,
		oldWork.TopologyHash,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	oldEvidence, found, err := rawBackend.LoadStrictSnapshotEvidence(
		runID,
		oldWork.Key,
		oldAttemptID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || oldEvidence.ExactSourceRowCount != 3 {
		t.Fatalf(
			"initial PostgreSQL strict resume evidence = %#v found=%t",
			oldEvidence,
			found,
		)
	}
	assertPostgresStrictSnapshotReleased(
		t,
		ctx,
		database,
		oldEvidence.SnapshotReference,
	)

	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+sourceQualified+
			" (id, payload) VALUES (4, 'after-crash')",
	); err != nil {
		t.Fatalf(
			"extend PostgreSQL source after failed strict epoch: %v",
			err,
		)
	}
	run.Resume = true
	resumeEvents := make([]string, 0)
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{
			events: &resumeEvents,
		},
		run: run,
	}
	result, err = resumeWithStage4Adapters(
		ctx,
		cfg,
		CompletedTableCheckpoints{},
		resumeObserver,
		resumeObserver,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatalf("resume PostgreSQL strict route: %v", err)
	}
	if result != (Result{
		Tables:    1,
		Rows:      4,
		Validated: true,
	}) {
		t.Fatalf(
			"resumed PostgreSQL strict result = %#v",
			result,
		)
	}
	rows, err := database.QueryContext(
		ctx,
		"SELECT id, payload FROM "+targetQualified+" ORDER BY id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var targetRows [][2]string
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		targetRows = append(targetRows, [2]string{
			strconv.FormatInt(id, 10),
			payload,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantRows := [][2]string{
		{"1", "one"},
		{"2", "two"},
		{"3", "three"},
		{"4", "after-crash"},
	}
	if fmt.Sprint(targetRows) != fmt.Sprint(wantRows) {
		t.Fatalf(
			"resumed PostgreSQL strict target rows = %#v, want %#v",
			targetRows,
			wantRows,
		)
	}

	newWork, _ := stage4NetworkLifecycleWork(
		t,
		rawBackend,
		runID,
		"items",
	)
	newAttemptID, err := BuildStrictConsistencyAttemptID(
		newWork.Key,
		newWork.TopologyHash,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	newEvidence, found, err := rawBackend.LoadStrictSnapshotEvidence(
		runID,
		newWork.Key,
		newAttemptID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || newEvidence.ExactSourceRowCount != 4 {
		t.Fatalf(
			"resumed PostgreSQL strict evidence = %#v found=%t",
			newEvidence,
			found,
		)
	}
	if newWork.TopologyHash == oldWork.TopologyHash ||
		newEvidence.AttemptID == oldEvidence.AttemptID ||
		newEvidence.ProcessEpoch == oldEvidence.ProcessEpoch ||
		newEvidence.SnapshotReference ==
			oldEvidence.SnapshotReference ||
		newEvidence.MigrationEpochID ==
			oldEvidence.MigrationEpochID {
		t.Fatalf(
			"PostgreSQL strict resume reused old epoch identity: old_work=%#v new_work=%#v old_evidence=%#v new_evidence=%#v",
			oldWork,
			newWork,
			oldEvidence,
			newEvidence,
		)
	}
	assertPostgresStrictSnapshotReleased(
		t,
		ctx,
		database,
		newEvidence.SnapshotReference,
	)
}

func TestStage4PostgresStrictMixedCompletedResumeLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL strict mixed-resume sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf(
			"parse PostgreSQL Stage 4 strict mixed-resume DSN: %T",
			err,
		)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	for _, scope := range []string{
		config.StrictConsistencyTable,
		config.StrictConsistencyMigration,
	} {
		scope := scope
		t.Run(scope, func(t *testing.T) {
			database, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf(
					"open PostgreSQL strict mixed-resume database: %T",
					err,
				)
			}
			// One exporter, one imported reader, and one writer are the
			// minimum viable strict-consistency connection envelope.
			database.SetMaxOpenConns(3)
			database.SetMaxIdleConns(3)
			t.Cleanup(func() {
				if err := database.Close(); err != nil {
					t.Errorf(
						"close PostgreSQL strict mixed-resume database: %v",
						err,
					)
				}
			})
			ctx, cancel := context.WithTimeout(
				context.Background(),
				90*time.Second,
			)
			defer cancel()
			if err := database.PingContext(ctx); err != nil {
				t.Fatalf(
					"verify PostgreSQL strict mixed-resume TLS endpoint: %T",
					err,
				)
			}

			suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
			sourceNamespace :=
				"dmtx_s4_strict_mixed_source_" + suffix
			targetNamespace :=
				"dmtx_s4_strict_mixed_target_" + suffix
			for _, namespace := range []string{
				sourceNamespace,
				targetNamespace,
			} {
				if _, err := database.ExecContext(
					ctx,
					"CREATE SCHEMA "+
						postgresIdentifier(namespace),
				); err != nil {
					t.Fatalf(
						"create PostgreSQL strict mixed-resume schema: %v",
						err,
					)
				}
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cleanupCancel()
				for _, namespace := range []string{
					sourceNamespace,
					targetNamespace,
				} {
					if _, err := database.ExecContext(
						cleanupCtx,
						"DROP SCHEMA IF EXISTS "+
							postgresIdentifier(namespace)+
							" CASCADE",
					); err != nil {
						t.Errorf(
							"drop PostgreSQL strict mixed-resume schema %s: %v",
							namespace,
							err,
						)
					}
				}
			})
			for _, table := range []string{"alpha", "beta"} {
				for _, namespace := range []string{
					sourceNamespace,
					targetNamespace,
				} {
					if _, err := database.ExecContext(
						ctx,
						"CREATE TABLE "+
							postgresQualified(namespace, table)+
							` (
								id bigint PRIMARY KEY,
								payload text NOT NULL
							)`,
					); err != nil {
						t.Fatalf(
							"create PostgreSQL strict mixed-resume table %s: %v",
							table,
							err,
						)
					}
				}
				if _, err := database.ExecContext(
					ctx,
					"INSERT INTO "+
						postgresQualified(sourceNamespace, table)+
						` (id, payload)
						VALUES (1, 'one'), (2, 'two')`,
				); err != nil {
					t.Fatalf(
						"seed PostgreSQL strict mixed-resume source %s: %v",
						table,
						err,
					)
				}
			}

			source := &relationalSourceAdapter{
				spec: relationalSourceSpec{
					engine:         "postgres",
					displayName:    "PostgreSQL",
					listTables:     engine.ListPostgresTables,
					inspectTable:   engine.InspectPostgresTable,
					readQuery:      postgresReadQuery,
					qualifiedTable: postgresQualified,
				},
				database:  database,
				namespace: sourceNamespace,
			}
			target := &postgresTargetAdapter{
				database:    database,
				batchWriter: newPostgresNativeWriter(database),
				namespace:   targetNamespace,
			}
			rawBackend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "stage4-pg-strict-mixed-" + scope + "-" + suffix
			initializeStage4LifecycleRun(
				t,
				rawBackend,
				runID,
				time.Now().Add(-time.Minute),
			)
			backend := &stage4PostgresStrictFailTableAckBackend{
				Stage4StateBackend: rawBackend,
				failTable:          "beta",
			}
			run := stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			)
			freshEvents := make([]string, 0)
			freshObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &freshEvents,
				},
				run: run,
			}
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Source = config.Endpoint{
				Type:     "postgres",
				Host:     parsed.Host,
				Port:     int(parsed.Port),
				Database: parsed.Database,
				User:     parsed.User,
				Password: parsed.Password,
				Schema:   sourceNamespace,
			}
			cfg.Target = cfg.Source
			cfg.Target.Schema = targetNamespace
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.IncludeTables = []string{"alpha", "beta"}
			cfg.Migration.Partitions = 1
			cfg.Migration.ConnectionLimit = 3
			cfg.Migration.Workers = 2
			cfg.Migration.ReaderParallelism = 1
			cfg.Migration.WriterParallelism = 1
			cfg.Migration.StrictConsistency = true
			cfg.Migration.StrictConsistencyScope = scope
			cfg.Migration.Validation.Mode = config.ValidationSample

			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				freshObserver,
				source,
				target,
				"upsert",
				freshObserver.run,
			)
			if err == nil ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(
					err.Error(),
					"injected durable acknowledgement failure for table beta",
				) {
				t.Fatalf(
					"fresh PostgreSQL strict mixed result=%#v error=%v",
					result,
					err,
				)
			}
			if result.Tables != 1 || result.Rows != 2 {
				t.Fatalf(
					"fresh PostgreSQL strict mixed partial result = %#v",
					result,
				)
			}
			oldAlphaWork, oldAlphaRange :=
				stage4NetworkLifecycleWork(
					t,
					rawBackend,
					runID,
					"alpha",
				)
			oldBetaWork, oldBetaRange :=
				stage4NetworkLifecycleWork(
					t,
					rawBackend,
					runID,
					"beta",
				)
			if oldAlphaWork.Status != "completed" ||
				oldAlphaRange.Status != "completed" ||
				oldBetaWork.Status == "completed" ||
				oldBetaRange.Status == "completed" ||
				len(oldBetaRange.Pending) == 0 {
				t.Fatalf(
					"mixed strict durability alpha=%#v/%#v beta=%#v/%#v",
					oldAlphaWork,
					oldAlphaRange,
					oldBetaWork,
					oldBetaRange,
				)
			}
			oldAlphaEvidence :=
				stage4PostgresStrictEvidenceForWork(
					t,
					rawBackend,
					runID,
					oldAlphaWork,
				)
			oldBetaEvidence :=
				stage4PostgresStrictEvidenceForWork(
					t,
					rawBackend,
					runID,
					oldBetaWork,
				)
			if oldAlphaEvidence.ExactSourceRowCount != 2 ||
				oldBetaEvidence.ExactSourceRowCount != 2 {
				t.Fatalf(
					"initial strict mixed evidence alpha=%#v beta=%#v",
					oldAlphaEvidence,
					oldBetaEvidence,
				)
			}

			for _, table := range []string{"alpha", "beta"} {
				if _, err := database.ExecContext(
					ctx,
					"INSERT INTO "+
						postgresQualified(sourceNamespace, table)+
						" (id, payload) VALUES (3, 'after-crash')",
				); err != nil {
					t.Fatalf(
						"extend PostgreSQL strict mixed source %s: %v",
						table,
						err,
					)
				}
			}
			run.Resume = true
			resumeEvents := make([]string, 0)
			resumeObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &resumeEvents,
				},
				run: run,
			}
			result, err = resumeWithStage4Adapters(
				ctx,
				cfg,
				CompletedTableCheckpoints{
					"alpha": {Rows: 2},
				},
				resumeObserver,
				resumeObserver,
				source,
				target,
				"upsert",
				run,
			)
			if err != nil {
				t.Fatalf(
					"resume PostgreSQL strict mixed route: %v",
					err,
				)
			}
			if result != (Result{
				Tables:    2,
				Rows:      5,
				Validated: true,
			}) {
				t.Fatalf(
					"resumed PostgreSQL strict mixed result = %#v",
					result,
				)
			}
			for _, check := range []struct {
				table string
				want  int
			}{
				{table: "alpha", want: 2},
				{table: "beta", want: 3},
			} {
				var count int
				if err := database.QueryRowContext(
					ctx,
					"SELECT COUNT(*) FROM "+
						postgresQualified(
							targetNamespace,
							check.table,
						),
				).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != check.want {
					t.Fatalf(
						"resumed strict target %s rows = %d, want %d",
						check.table,
						count,
						check.want,
					)
				}
			}

			newAlphaWork, _ := stage4NetworkLifecycleWork(
				t,
				rawBackend,
				runID,
				"alpha",
			)
			newBetaWork, _ := stage4NetworkLifecycleWork(
				t,
				rawBackend,
				runID,
				"beta",
			)
			newAlphaEvidence :=
				stage4PostgresStrictEvidenceForWork(
					t,
					rawBackend,
					runID,
					newAlphaWork,
				)
			newBetaEvidence :=
				stage4PostgresStrictEvidenceForWork(
					t,
					rawBackend,
					runID,
					newBetaWork,
				)
			if newAlphaWork.TopologyHash !=
				oldAlphaWork.TopologyHash ||
				newAlphaEvidence.AttemptID !=
					oldAlphaEvidence.AttemptID ||
				newAlphaEvidence.ExactSourceRowCount != 2 {
				t.Fatalf(
					"completed alpha changed strict epoch: old=%#v/%#v new=%#v/%#v",
					oldAlphaWork,
					oldAlphaEvidence,
					newAlphaWork,
					newAlphaEvidence,
				)
			}
			if newBetaWork.TopologyHash ==
				oldBetaWork.TopologyHash ||
				newBetaEvidence.AttemptID ==
					oldBetaEvidence.AttemptID ||
				newBetaEvidence.ProcessEpoch ==
					oldBetaEvidence.ProcessEpoch ||
				newBetaEvidence.SnapshotReference ==
					oldBetaEvidence.SnapshotReference ||
				newBetaEvidence.ExactSourceRowCount != 3 {
				t.Fatalf(
					"incomplete beta did not enter a new strict epoch: old=%#v/%#v new=%#v/%#v",
					oldBetaWork,
					oldBetaEvidence,
					newBetaWork,
					newBetaEvidence,
				)
			}

			allCompletedEvents := make([]string, 0)
			allCompletedObserver := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{
					events: &allCompletedEvents,
				},
				run: run,
			}
			result, err = resumeWithStage4Adapters(
				ctx,
				cfg,
				CompletedTableCheckpoints{
					"alpha": {Rows: 2},
					"beta":  {Rows: 3},
				},
				allCompletedObserver,
				allCompletedObserver,
				source,
				target,
				"upsert",
				run,
			)
			if err != nil {
				t.Fatalf(
					"resume all-completed PostgreSQL strict route: %v",
					err,
				)
			}
			if result != (Result{
				Tables:    2,
				Rows:      5,
				Validated: true,
			}) {
				t.Fatalf(
					"all-completed PostgreSQL strict result = %#v",
					result,
				)
			}
			for _, reference := range []string{
				oldAlphaEvidence.SnapshotReference,
				oldBetaEvidence.SnapshotReference,
				newBetaEvidence.SnapshotReference,
			} {
				assertPostgresStrictSnapshotReleased(
					t,
					ctx,
					database,
					reference,
				)
			}
		})
	}
}

func stage4PostgresStrictEvidenceForWork(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	work state.WorkTask,
) state.StrictSnapshotEvidence {
	t.Helper()
	attemptID, err := BuildStrictConsistencyAttemptID(
		work.Key,
		work.TopologyHash,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence, found, err := backend.LoadStrictSnapshotEvidence(
		runID,
		work.Key,
		attemptID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf(
			"missing PostgreSQL strict evidence for work %#v",
			work,
		)
	}
	return evidence
}
