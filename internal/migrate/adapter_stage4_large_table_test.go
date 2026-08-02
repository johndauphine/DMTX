package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4LargeTableThresholdConfigurationAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*config.Config)
		wantErr string
	}{
		{name: "ordinary composed route is admitted"},
		{
			name: "strict remains refused",
			mutate: func(cfg *config.Config) {
				cfg.Migration.StrictConsistency = true
			},
			wantErr: "not yet composed with strict consistency",
		},
		{
			name: "incremental remains refused",
			mutate: func(cfg *config.Config) {
				cfg.Migration.DateUpdatedColumns = []string{"updated_at"}
			},
			wantErr: "not yet composed with date-based incremental",
		},
		{
			name: "delete remains refused",
			mutate: func(cfg *config.Config) {
				cfg.Migration.Deletes.Mode = config.DeleteModeReconcile
			},
			wantErr: "not yet composed with delete reconciliation",
		},
		{
			name: "compatibility override remains refused",
			mutate: func(cfg *config.Config) {
				cfg.Source.Type = "sqlite"
				cfg.Target.Type = "sqlite"
			},
			wantErr: "SQLite-to-SQLite compatibility routing",
		},
		{
			name: "analytical target remains refused",
			mutate: func(cfg *config.Config) {
				cfg.Target.Type = "clickhouse"
			},
			wantErr: "requires a certified composed relational or SQLite network route",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := stage4LargeTableThresholdTestConfig(t)
			if test.mutate != nil {
				test.mutate(&cfg)
			}
			err := requireStage4AdapterConfigurationSeams(cfg)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("configuration admission: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("configuration error = %v, want %q", err, test.wantErr)
			}
			if ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf("configuration class = %q, want policy: %v", ClassifyTransferError(err), err)
			}
		})
	}

	generated := stage4LargeTableThresholdTestConfig(t)
	generated.Migration.LargeTableThreshold = 0
	if err := requireStage4AdapterConfigurationSeams(generated); err != nil {
		t.Fatalf("generated threshold default rejected: %v", err)
	}
}

func TestStage4LargeTableThresholdRejectsCompatibilityOverrideAtRouteValidation(
	t *testing.T,
) {
	cfg := stage4LargeTableThresholdTestConfig(t)
	cfg.Source.Type = "sqlite"
	cfg.Target.Type = "sqlite"
	cfg.Source.Database = filepath.Join(t.TempDir(), "source.db")
	cfg.Target.Database = filepath.Join(t.TempDir(), "target.db")

	err := ValidateMigration(cfg)
	if err == nil || !strings.Contains(
		err.Error(),
		"SQLite-to-SQLite compatibility routing",
	) {
		t.Fatalf("compatibility route validation error = %v", err)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("compatibility route class = %q, want policy", ClassifyTransferError(err))
	}
}

func TestStage4LargeTableThresholdRefusalPrecedesEndpointConstruction(
	t *testing.T,
) {
	events := make([]string, 0)
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "large-table-threshold-pre-open"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	protected := false
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run:                    stage4LifecycleRunContext(t, backend, runID, false),
		},
		protected: &protected,
	}
	sourceOpened, targetOpened := false, false
	route := resolvedAdapterRoute{
		source: sourceRole{
			engine: "postgres",
			open: func(context.Context, config.Endpoint) (sourceAdapter, error) {
				sourceOpened = true
				return nil, errors.New("source must not open")
			},
		},
		target: targetRole{
			engine: "clickhouse",
			open: func(context.Context, config.Endpoint) (targetAdapter, error) {
				targetOpened = true
				return nil, errors.New("target must not open")
			},
		},
	}
	cfg := stage4LargeTableThresholdTestConfig(t)
	cfg.Target.Type = "clickhouse"

	_, err := route.execute(context.Background(), cfg, observer)
	if err == nil || !strings.Contains(
		err.Error(),
		"requires a certified composed relational or SQLite network route",
	) {
		t.Fatalf("pre-open admission error = %v", err)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("pre-open admission class = %q", ClassifyTransferError(err))
	}
	if sourceOpened || targetOpened || len(events) != 0 {
		t.Fatalf(
			"threshold refusal opened or wrote before admission: source=%v target=%v events=%v",
			sourceOpened,
			targetOpened,
			events,
		)
	}
	tasks, ranges, listErr := backend.ListWork(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(tasks) != 0 || len(ranges) != 0 {
		t.Fatalf("threshold refusal wrote work: tasks=%v ranges=%v", tasks, ranges)
	}
}

func TestStage4LargeTableThresholdUsesRetainedSQLiteSizeAndBindsResumeTopology(
	t *testing.T,
) {
	for name, newBackend := range stage4LifecycleBackendFactories() {
		name, newBackend := name, newBackend
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "source.db")
			writer, err := sql.Open("sqlite", sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			for _, statement := range []string{
				"PRAGMA journal_mode=WAL",
				"CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
				"INSERT INTO items(id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
			} {
				if _, err := writer.ExecContext(context.Background(), statement); err != nil {
					t.Fatalf("setup SQLite source %q: %v", statement, err)
				}
			}

			first := stage4LargeTableOpenSQLiteSource(t, sourcePath)
			table, err := first.InspectTable(context.Background(), "items")
			if err != nil {
				_ = first.Close()
				t.Fatal(err)
			}
			if _, err := writer.ExecContext(
				context.Background(),
				"INSERT INTO items(id, payload) VALUES (4, 'after-snapshot')",
			); err != nil {
				_ = first.Close()
				t.Fatalf("write after retained snapshot: %v", err)
			}

			seed := stage4LargeTableSeedWork(t, table)
			firstWork, firstDecision := stage4LargeTableBindWork(
				t,
				first,
				table,
				seed,
				4,
				4,
			)
			if firstDecision.exactSourceRows != 3 ||
				firstDecision.effectivePartitions != 1 ||
				len(firstWork.ranges) != 1 {
				t.Fatalf(
					"retained size decision=%#v ranges=%#v, want three rows and one range",
					firstDecision,
					firstWork.ranges,
				)
			}

			backend := newBackend(t)
			runID := "large-table-threshold-resume-" + name
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			run := stage4LifecycleRunContext(t, backend, runID, false)
			if err := ensureStage4AdapterWork(
				context.Background(),
				run,
				[]stage4AdapterWork{firstWork},
			); err != nil {
				_ = first.Close()
				t.Fatalf("persist first immutable plan: %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}

			second := stage4LargeTableOpenSQLiteSource(t, sourcePath)
			defer func() {
				if err := second.Close(); err != nil {
					t.Error(err)
				}
			}()
			secondTable, err := second.InspectTable(context.Background(), "items")
			if err != nil {
				t.Fatal(err)
			}
			secondWork, secondDecision := stage4LargeTableBindWork(
				t,
				second,
				secondTable,
				seed,
				4,
				4,
			)
			if secondDecision.exactSourceRows != 4 ||
				secondDecision.effectivePartitions != 4 ||
				len(secondWork.ranges) != 4 {
				t.Fatalf(
					"reopened size decision=%#v ranges=%#v, want four rows and four ranges",
					secondDecision,
					secondWork.ranges,
				)
			}
			if firstWork.topology == secondWork.topology {
				t.Fatal("retained source size change did not alter immutable work topology")
			}
			if err := ensureStage4AdapterWork(
				context.Background(),
				run,
				[]stage4AdapterWork{secondWork},
			); err == nil || !errors.Is(err, state.ErrTopologyChanged) {
				t.Fatalf("resume topology change error = %v, want %v", err, state.ErrTopologyChanged)
			}
		})
	}
}

func TestStage4LargeTableThresholdFailsClosedWithoutStableSizeAuthority(
	t *testing.T,
) {
	_, err := stage4AdapterLargeTableDecisionForStableSource(
		context.Background(),
		stage4LargeTableUnstableSource{},
		stage4AdapterTestTable(),
		10,
		4,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"requires exact retained source table-size authority",
	) {
		t.Fatalf("untrusted size authority error = %v", err)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("untrusted size authority class = %q", ClassifyTransferError(err))
	}
}

func TestStage4LargeTableThresholdRejectsPreboundNetworkPlan(t *testing.T) {
	run := newNetworkStateTestRun(t, "yaml", "large-table-threshold-prebound")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.LargeTableThreshold = 10
	resources := stage4NetworkRunnerResources()

	_, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"requires deferred table-stable network planning",
	) {
		t.Fatalf("prebound large-table admission error = %v", err)
	}
	if ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("prebound large-table admission class = %q", ClassifyTransferError(err))
	}
	if writes := target.snapshotWrites(); len(writes) != 0 {
		t.Fatalf("prebound large-table refusal wrote target: %#v", writes)
	}
}

func TestStage4LargeTableThresholdChangesDeferredRangePartitioning(t *testing.T) {
	for _, test := range []struct {
		name      string
		threshold int64
		want      int
	}{
		{name: "below threshold remains one range", threshold: 5, want: 1},
		{name: "at threshold uses requested ranges", threshold: 4, want: 4},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  stage4AdapterTestTable(),
				rows:   []string{"one", "two", "three", "four"},
			}
			backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
			runID := "large-table-threshold-deferred-" + strings.ReplaceAll(test.name, " ", "-")
			initializeStage4LifecycleRun(t, backend, runID, time.Now().Add(-time.Minute))
			protected := false
			target := &stage4NetworkAdmissionTarget{
				recordingAdapterTarget: &recordingAdapterTarget{
					events:    &events,
					protected: &protected,
				},
				backend: backend,
				runID:   runID,
			}
			observer := &stage4NetworkAdmissionObserver{
				stage4AdapterObserver: stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{events: &events},
					run:                    stage4LifecycleRunContext(t, backend, runID, false),
				},
				protected: &protected,
			}
			cfg := stage4LargeTableThresholdTestConfig(t)
			cfg.Migration.Partitions = 4
			cfg.Migration.LargeTableThreshold = test.threshold
			prepared, err := prepareStage4AdapterRun(
				context.Background(),
				cfg,
				observer,
				source,
				target,
				"upsert",
				observer.run,
			)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			execution, err := admitStage4AdapterNetworkTransfer(
				context.Background(),
				cfg,
				observer,
				source,
				target,
				prepared,
				nil,
			)
			if err != nil {
				t.Fatalf("admit: %v", err)
			}
			if !execution.deferred || execution.largeTableThreshold != test.threshold {
				t.Fatalf("threshold was not carried into deferred execution: %#v", execution)
			}
			tableExecution, err := execution.planTable(context.Background(), 0, 0)
			if err != nil {
				t.Fatalf("plan retained table: %v", err)
			}
			defer func() {
				if err := tableExecution.Close(); err != nil {
					t.Error(err)
				}
			}()
			if len(tableExecution.ranges) != test.want {
				t.Fatalf("range count = %d, want %d", len(tableExecution.ranges), test.want)
			}
			if tableExecution.work.topology == prepared.work[0].topology {
				t.Fatal("exact size evidence was not bound into deferred work topology")
			}
		})
	}
}

func stage4LargeTableThresholdTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 4
	cfg.Migration.LargeTableThreshold = 4
	return cfg
}

func stage4LargeTableOpenSQLiteSource(
	t *testing.T,
	path string,
) *sqliteSourceAdapter {
	t.Helper()
	opened, err := openSQLiteSourceAdapter(
		context.Background(),
		config.Endpoint{Type: "sqlite", Database: path},
	)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := opened.(*sqliteSourceAdapter)
	if !ok || source == nil {
		_ = opened.Close()
		t.Fatalf("SQLite source type = %T", opened)
	}
	return source
}

func stage4LargeTableSeedWork(
	t *testing.T,
	table schema.Table,
) stage4AdapterWork {
	t.Helper()
	seed, err := buildStage4AdapterWork(
		"large-table-threshold-config",
		"upsert",
		[]adapterTablePlan{{
			source:  table,
			target:  cloneStage4RichTable(table),
			columns: adapterColumnNames(table),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return seed[0]
}

func stage4LargeTableBindWork(
	t *testing.T,
	source sourceAdapter,
	table schema.Table,
	seed stage4AdapterWork,
	threshold int64,
	partitions int,
) (stage4AdapterWork, stage4AdapterLargeTableDecision) {
	t.Helper()
	decision, err := stage4AdapterLargeTableDecisionForStableSource(
		context.Background(),
		source,
		table,
		threshold,
		partitions,
	)
	if err != nil {
		t.Fatal(err)
	}
	seed.topology, err = stage4AdapterLargeTableTopology(seed.topology, decision)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := bindStage4AdapterPagination(
		context.Background(),
		source,
		decision.effectivePartitions,
		[]stage4AdapterWork{seed},
		[]adapterTablePlan{{
			source:  table,
			target:  cloneStage4RichTable(table),
			columns: adapterColumnNames(table),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return bound[0], decision
}

type stage4LargeTableUnstableSource struct{}

func (stage4LargeTableUnstableSource) Engine() string { return "postgres" }

func (stage4LargeTableUnstableSource) DisplayName() string { return "unstable" }

func (stage4LargeTableUnstableSource) ListTables(context.Context) ([]string, error) {
	return nil, errors.New("unexpected list")
}

func (stage4LargeTableUnstableSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return schema.Table{}, errors.New("unexpected inspect")
}

func (stage4LargeTableUnstableSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, errors.New("unexpected rows")
}

func (stage4LargeTableUnstableSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, errors.New("unexpected count")
}

func (stage4LargeTableUnstableSource) Close() error { return nil }
