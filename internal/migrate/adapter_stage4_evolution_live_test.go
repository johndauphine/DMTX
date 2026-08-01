package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4AdapterPostgresSchemaEvolutionComposedRouteLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL composed schema-evolution sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 evolution DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 evolution database: %T", err)
	}
	database.SetMaxOpenConns(8)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL Stage 4 evolution database: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL Stage 4 evolution TLS endpoint: %T", err)
	}

	tests := []struct {
		name          string
		prior         func() schema.Table
		priorDDL      func(string) []string
		contract      config.SchemaContract
		current       schema.Table
		wantOperation SchemaContractAction
		wantRetained  bool
		wantVarchar   int
	}{
		{
			name: "create table",
			contract: config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			current:       stage4AdapterTestTable(),
			wantOperation: SchemaContractCreateTable,
		},
		{
			name: "add nullable column",
			prior: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns = append(
					[]schema.Column(nil),
					table.Columns[:1]...,
				)
				return table
			},
			priorDDL: func(namespace string) []string {
				return []string{
					"CREATE TABLE " +
						postgresQualified(namespace, "items") +
						` ("id" bigint NOT NULL PRIMARY KEY)`,
				}
			},
			contract: config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			current: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns[1].Nullable = true
				return table
			}(),
			wantOperation: SchemaContractAddColumn,
		},
		{
			name: "retain target-only subobjects",
			prior: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns[1].Nullable = false
				return table
			},
			priorDDL: func(namespace string) []string {
				qualified := postgresQualified(namespace, "items")
				return []string{
					"CREATE TABLE " + qualified + ` (
						"id" bigint NOT NULL PRIMARY KEY,
						"payload" text NOT NULL,
						"owner_id" bigint,
						CONSTRAINT "items_owner_id_check"
							CHECK ("owner_id" >= 0),
						CONSTRAINT "items_owner_id_fk"
							FOREIGN KEY ("owner_id")
							REFERENCES ` + qualified + ` ("id")
							ON UPDATE NO ACTION
							ON DELETE NO ACTION
					)`,
					"CREATE INDEX \"items_owner_id_idx\" ON " +
						qualified +
						` ("owner_id" ASC NULLS FIRST)`,
				}
			},
			contract: config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			current: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns[1].Nullable = true
				return table
			}(),
			wantOperation: SchemaContractRelaxNullability,
			wantRetained:  true,
		},
		{
			// This must travel through the composed route rather than only the
			// native PostgreSQL planner. It is the target-matrix proof that a
			// safe existing-column type widening reaches the production target
			// capability before the first transfer write.
			name: "widen safe varchar",
			prior: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns[1].Type = "varchar"
				table.Columns[1].DeclaredType = &schema.DeclaredType{
					Base: "varchar", Arguments: []int{10},
				}
				return table
			},
			priorDDL: func(namespace string) []string {
				return []string{
					"CREATE TABLE " + postgresQualified(namespace, "items") +
						` ("id" bigint NOT NULL PRIMARY KEY, "payload" varchar(10) NOT NULL)`,
				}
			},
			contract: config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			current: func() schema.Table {
				table := stage4AdapterTestTable()
				table.Columns[1].Type = "varchar"
				table.Columns[1].DeclaredType = &schema.DeclaredType{
					Base: "varchar", Arguments: []int{32},
				}
				return table
			}(),
			wantOperation: SchemaContractWidenType,
			wantVarchar:   32,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			suffix := fmt.Sprintf(
				"%d_%d",
				os.Getpid(),
				time.Now().UnixNano(),
			)
			namespace := "dmtx_stage4_route_evolution_" + suffix
			if _, err := database.ExecContext(
				ctx,
				"CREATE SCHEMA "+postgresIdentifier(namespace),
			); err != nil {
				t.Fatalf("create PostgreSQL evolution schema: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cleanupCancel()
				if _, err := database.ExecContext(
					cleanupCtx,
					"DROP SCHEMA IF EXISTS "+
						postgresIdentifier(namespace)+" CASCADE",
				); err != nil {
					t.Errorf(
						"drop PostgreSQL evolution schema %s: %v",
						namespace,
						err,
					)
				}
			})

			target := &postgresTargetAdapter{
				database:    database,
				batchWriter: newPostgresNativeWriter(database),
				namespace:   namespace,
			}
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.Partitions = 1
			cfg.Migration.SchemaContract = &test.contract
			started := time.Now().Add(-3 * time.Minute).UTC()
			if test.prior != nil {
				previous := test.prior()
				for _, statement := range test.priorDDL(namespace) {
					if _, err := database.ExecContext(
						ctx,
						statement,
					); err != nil {
						t.Fatalf(
							"create prior PostgreSQL evolution table: %v",
							err,
						)
					}
				}
				stage4AdapterInstallPostgresEvolutionLiveBaseline(
					t,
					ctx,
					backend,
					target,
					cfg,
					"stage4-pg-evolution-live-prior-"+suffix,
					started,
					previous,
				)
			}

			runID := "stage4-pg-evolution-live-current-" + suffix
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				started.Add(2*time.Minute),
			)
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  test.current,
			}
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
					"run PostgreSQL composed %s evolution: %v",
					test.wantOperation,
					err,
				)
			}
			if result != (Result{
				Tables:    1,
				Rows:      2,
				Validated: true,
			}) {
				t.Fatalf("PostgreSQL evolution result = %#v", result)
			}
			var nullable string
			if err := database.QueryRowContext(
				ctx,
				`SELECT is_nullable
				   FROM information_schema.columns
				  WHERE table_schema = $1
				    AND table_name = $2
				    AND column_name = 'payload'`,
				namespace,
				test.current.Name,
			).Scan(&nullable); err != nil {
				t.Fatalf(
					"read evolved PostgreSQL payload column: %v",
					err,
				)
			}
			wantNullable := "NO"
			if test.current.Columns[1].Nullable {
				wantNullable = "YES"
			}
			if nullable != wantNullable {
				t.Fatalf(
					"PostgreSQL payload nullable = %q, want %q",
					nullable,
					wantNullable,
				)
			}
			if test.wantVarchar != 0 {
				var width sql.NullInt64
				if err := database.QueryRowContext(
					ctx,
					`SELECT character_maximum_length
					   FROM information_schema.columns
					  WHERE table_schema = $1
					    AND table_name = $2
					    AND column_name = 'payload'`,
					namespace,
					test.current.Name,
				).Scan(&width); err != nil {
					t.Fatalf("read evolved PostgreSQL payload width: %v", err)
				}
				if !width.Valid || width.Int64 != int64(test.wantVarchar) {
					t.Fatalf(
						"PostgreSQL payload width = %#v, want %d",
						width,
						test.wantVarchar,
					)
				}
			}
			var rows int
			if err := database.QueryRowContext(
				ctx,
				"SELECT count(*) FROM "+
					postgresQualified(namespace, test.current.Name),
			).Scan(&rows); err != nil {
				t.Fatalf("count evolved PostgreSQL rows: %v", err)
			}
			if rows != 2 {
				t.Fatalf("evolved PostgreSQL rows = %d, want 2", rows)
			}
			if test.wantRetained {
				assertStage4AdapterPostgresRetainedSubobjectsLive(
					t,
					ctx,
					database,
					namespace,
				)
			}
			tasks, _, err := backend.ListWork(runID)
			if err != nil {
				t.Fatal(err)
			}
			// Aggregate schema sentinels intentionally remain running here. The
			// app publishes them atomically with the successful run outcome after
			// validation audit persistence; completing them in the migration
			// runner would recreate the terminal-evidence timestamp conflict.
			for _, key := range []state.TaskKey{
				stage4SchemaGateTask,
				stage4TargetShapeTask,
			} {
				var pending bool
				for _, task := range tasks {
					if task.Key == key && task.Status == "running" {
						pending = true
					}
				}
				if !pending {
					t.Fatalf(
						"PostgreSQL evolution sentinel %#v is not pending aggregate publication: %#v",
						key,
						tasks,
					)
				}
			}
		})
	}
}

func assertStage4AdapterPostgresRetainedSubobjectsLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var retainedColumn bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM information_schema.columns
			 WHERE table_schema = $1
			   AND table_name = 'items'
			   AND column_name = 'owner_id'
		)`,
		namespace,
	).Scan(&retainedColumn); err != nil {
		t.Fatalf("inspect retained PostgreSQL column: %v", err)
	}
	var retainedIndex bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM pg_catalog.pg_class AS index_relation
			  JOIN pg_catalog.pg_namespace AS namespace
			    ON namespace.oid = index_relation.relnamespace
			 WHERE namespace.nspname = $1
			   AND index_relation.relname = 'items_owner_id_idx'
			   AND index_relation.relkind = 'i'
		)`,
		namespace,
	).Scan(&retainedIndex); err != nil {
		t.Fatalf("inspect retained PostgreSQL index: %v", err)
	}
	var retainedCheck, retainedForeignKey bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			COUNT(*) FILTER (
				WHERE constraint_record.conname =
					'items_owner_id_check'
				  AND constraint_record.contype = 'c'
			) = 1,
			COUNT(*) FILTER (
				WHERE constraint_record.conname =
					'items_owner_id_fk'
				  AND constraint_record.contype = 'f'
			) = 1
		   FROM pg_catalog.pg_constraint AS constraint_record
		   JOIN pg_catalog.pg_class AS owner
		     ON owner.oid = constraint_record.conrelid
		   JOIN pg_catalog.pg_namespace AS namespace
		     ON namespace.oid = owner.relnamespace
		  WHERE namespace.nspname = $1
		    AND owner.relname = 'items'`,
		namespace,
	).Scan(
		&retainedCheck,
		&retainedForeignKey,
	); err != nil {
		t.Fatalf(
			"inspect retained PostgreSQL constraints: %v",
			err,
		)
	}
	if !retainedColumn ||
		!retainedIndex ||
		!retainedCheck ||
		!retainedForeignKey {
		t.Fatalf(
			"retained PostgreSQL subobjects column=%t index=%t check=%t foreign-key=%t",
			retainedColumn,
			retainedIndex,
			retainedCheck,
			retainedForeignKey,
		)
	}
}

func TestStage4AdapterPostgresStableDeepValidationComposedRouteLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 PostgreSQL stable deep-validation composed-route sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL Stage 4 validation DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL Stage 4 validation database: %T", err)
	}
	database.SetMaxOpenConns(8)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf(
				"close PostgreSQL Stage 4 validation database: %v",
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
			"verify PostgreSQL Stage 4 validation TLS endpoint: %T",
			err,
		)
	}

	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	sourceNamespace := "dmtx_stage4_deep_source_" + suffix
	targetNamespace := "dmtx_stage4_deep_target_" + suffix
	for _, namespace := range []string{
		sourceNamespace,
		targetNamespace,
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(namespace),
		); err != nil {
			t.Fatalf(
				"create PostgreSQL deep-validation schema: %v",
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
					"drop PostgreSQL deep-validation schema %s: %v",
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
				payload text,
				marker bytea NOT NULL
			)`); err != nil {
			t.Fatalf(
				"create PostgreSQL deep-validation table: %v",
				err,
			)
		}
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO `+sourceQualified+` (id, payload, marker)
		VALUES
			(1, 'alpha', decode('01', 'hex')),
			(2, NULL,    decode('02', 'hex')),
			(3, 'gamma', decode('03', 'hex'))`); err != nil {
		t.Fatalf("seed PostgreSQL deep-validation source: %v", err)
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
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-pg-stable-deep-validation-" + suffix
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
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
	cfg.Migration.Partitions = 1
	cfg.Migration.Validation.Mode = config.ValidationSample

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
			"run PostgreSQL stable deep-validation composed route: %v",
			err,
		)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf(
			"PostgreSQL stable deep-validation result = %#v",
			result,
		)
	}
	var rows int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+targetQualified,
	).Scan(&rows); err != nil {
		t.Fatalf(
			"count PostgreSQL deep-validation target rows: %v",
			err,
		)
	}
	if rows != 3 {
		t.Fatalf(
			"PostgreSQL deep-validation target rows = %d, want 3",
			rows,
		)
	}
}

func stage4AdapterInstallPostgresEvolutionLiveBaseline(
	t *testing.T,
	ctx context.Context,
	backend state.YAMLStore,
	target *postgresTargetAdapter,
	cfg config.Config,
	runID string,
	started time.Time,
	table schema.Table,
) {
	t.Helper()
	initializeStage4LifecycleRun(t, backend, runID, started)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	configDigest, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	options := Stage4SchemaGateOptions{
		SourceEngine:       "postgres",
		TargetEngine:       "postgres",
		TargetMode:         "upsert",
		IncludeTables:      cfg.Migration.IncludeTables,
		ExcludeTables:      cfg.Migration.ExcludeTables,
		ConfigIdentity:     configDigest,
		Contract:           cfg.Migration.SchemaContract,
		FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
		DateUpdatedColumns: cfg.Migration.DateUpdatedColumns,
		CapturedAt:         started,
	}
	gate, err := PrepareStage4SchemaGate(
		run,
		[]schema.Table{table},
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := target.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewStage4TargetShapeSeed(catalog)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := PrepareStage4TargetShapeAuthority(
		run,
		gate,
		options,
		seed,
	)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	pendingTarget, err := BindStage4TargetShapeProjection(
		authority,
		projection,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(gate.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(pendingTarget); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		ctx,
		run,
		authority.Task(),
		stage4TargetShapeRangeID,
		authority.TopologyHash(),
	); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		ctx,
		run,
		gate.Task,
		stage4SchemaGateRangeID,
		gate.TopologyHash,
	); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source.example:5432/app",
		TargetIdentity: "postgres:target.example:5432/app",
		Outcome:        state.Success,
		Resumable:      true,
		Reason:         "complete",
		StartedAt:      started,
		EndedAt:        started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
}
