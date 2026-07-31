package migrate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4AdapterObserver struct {
	recordingTableObserver
	run Stage4RunContext
}

type stage4AdapterUnprotectedObserver struct {
	recordingTableObserver
	run Stage4RunContext
}

type stage4AdapterValidationProviderObserver struct {
	stage4AdapterObserver
	equalityProofEnabled bool
	equalityProof        string
	equalityProofErr     error
	equalityProofCalls   *[]schema.Table
	returnTypedNilProbe  bool
}

func (observer stage4AdapterValidationProviderObserver) Stage4ValidationProbe(
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if observer.returnTypedNilProbe {
		return (*stage4AdapterValidationEqualityProofTestProbe)(nil), nil
	}
	countProbe := &stage4AdapterCountProbe{
		source: source,
		target: target,
		plans:  stage4AdapterPlansBySource(plans),
	}
	if !observer.equalityProofEnabled {
		return countProbe, nil
	}
	return &stage4AdapterValidationEqualityProofTestProbe{
		stage4AdapterCountProbe: countProbe,
		proof:                   observer.equalityProof,
		err:                     observer.equalityProofErr,
		calls:                   observer.equalityProofCalls,
	}, nil
}

type stage4AdapterValidationEqualityProofTestProbe struct {
	*stage4AdapterCountProbe
	proof string
	err   error
	calls *[]schema.Table
}

type stage4AdapterTypedNilValidationProvider struct{}

func (*stage4AdapterTypedNilValidationProvider) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (*stage4AdapterTypedNilValidationProvider) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func (*stage4AdapterTypedNilValidationProvider) Stage4ValidationProbe(
	sourceAdapter,
	targetAdapter,
	[]adapterTablePlan,
) (ValidationCoreProbe, error) {
	panic("typed-nil validation provider must not be invoked")
}

func (probe *stage4AdapterValidationEqualityProofTestProbe) Stage4ValidationPrimaryKeyEqualityProof(
	table schema.Table,
) (string, error) {
	if probe.calls != nil {
		*probe.calls = append(
			*probe.calls,
			cloneStage4RichTable(table),
		)
	}
	return probe.proof, probe.err
}

func (observer stage4AdapterUnprotectedObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func (observer stage4AdapterObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func (stage4AdapterObserver) ObserveStage4SchemaDecisions(
	context.Context,
	Stage4SchemaDecisionReport,
) error {
	return nil
}

func (observer stage4AdapterObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutation()
}

func TestStage4AdapterFreshOrdersSchemaWorkMutationAndPublication(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-adapter-fresh"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	result, err := migrateWithAdapters(
		context.Background(),
		stage4AdapterTestConfig(t, "source-password", "target-password"),
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}

	assertStage4AdapterEventBefore(
		t,
		events,
		"source_inspect",
		"target_plan",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_preflight",
		"before_tables:items",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"before_tables:items",
		"target_prepare",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_prepare",
		"target_write",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_write",
		"target_finalize",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_finalize",
		"source_count",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_finalize",
		"target_count",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_count",
		"after:items",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_count",
		"after:items",
	)

	snapshot, found, err := backend.LoadSchemaSnapshot(
		runID,
		stage4SchemaGateTask,
	)
	if err != nil || !found {
		t.Fatalf("published snapshot found=%v err=%v", found, err)
	}
	if snapshot.Digest == "" {
		t.Fatal("published schema snapshot has no digest")
	}
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	assertStage4AdapterWorkCompleted(
		t,
		tasks,
		ranges,
		stage4SchemaGateTask,
		stage4SchemaGateRangeID,
	)
	assertStage4AdapterWorkCompleted(
		t,
		tasks,
		ranges,
		state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: "public",
			Table:  "items",
		},
		stage4AdapterCopyRangeID,
	)
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("latest run found=%v err=%v", found, err)
	}
	if latest.Outcome != state.Running {
		t.Fatalf(
			"adapter runner published run outcome %q; app owns success",
			latest.Outcome,
		)
	}
}

func TestStage4AdapterFailsBeforeTargetPlanningWhenRequiredSeamIsMissing(
	t *testing.T,
) {
	tests := []struct {
		name       string
		configure  func(*config.Config)
		wantError  string
		targetMode string
	}{
		{
			name: "runtime tuning",
			configure: func(cfg *config.Config) {
				cfg.Migration.RuntimeTuning = true
			},
			wantError:  "chunk-boundary tuning seam",
			targetMode: "upsert",
		},
		{
			name: "delete reconciliation",
			configure: func(cfg *config.Config) {
				cfg.Migration.Deletes.Mode = config.DeleteModeReconcile
			},
			wantError:  "delete seam",
			targetMode: "upsert",
		},
		{
			name: "strict consistency",
			configure: func(cfg *config.Config) {
				cfg.Migration.StrictConsistency = true
			},
			wantError:  "snapshot seam",
			targetMode: "upsert",
		},
		{
			name: "null parity",
			configure: func(cfg *config.Config) {
				cfg.Migration.Validation.Mode =
					config.ValidationNullParity
			},
			wantError:  "validation probe seam",
			targetMode: "upsert",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  stage4AdapterTestTable(),
			}
			target := &recordingAdapterTarget{events: &events}
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "missing-" + strings.ReplaceAll(
				test.name,
				" ",
				"-",
			)
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Migration.TargetMode = test.targetMode
			test.configure(&cfg)
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
			_, err := migrateWithAdapters(
				context.Background(),
				cfg,
				observer,
				source,
				target,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
			for _, forbidden := range []string{
				"target_plan",
				"target_preflight",
				"target_prepare",
				"target_write",
				"target_finalize",
			} {
				if stage4AdapterEventIndex(events, forbidden) >= 0 {
					t.Fatalf(
						"missing seam reached %s: %v",
						forbidden,
						events,
					)
				}
			}
			if stage4AdapterEventsContain(events, "before") {
				t.Fatalf(
					"missing seam checkpointed ordinary work: %v",
					events,
				)
			}
		})
	}
}

func TestStage4AdapterAdmissionPrecedesEndpointConstruction(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*config.Config, *Stage4RunContext)
		observer  func(*[]string, Stage4RunContext) TableObserver
		want      string
	}{
		{
			name: "missing target fence",
			observer: func(
				events *[]string,
				run Stage4RunContext,
			) TableObserver {
				return stage4AdapterUnprotectedObserver{
					recordingTableObserver: recordingTableObserver{
						events: events,
					},
					run: run,
				}
			},
			want: "target mutation protector",
		},
		{
			name: "fresh route with resume context",
			configure: func(
				_ *config.Config,
				run *Stage4RunContext,
			) {
				run.Resume = true
			},
			observer: func(
				events *[]string,
				run Stage4RunContext,
			) TableObserver {
				return stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{
						events: events,
					},
					run: run,
				}
			},
			want: "resume Stage 4 run context",
		},
		{
			name: "runtime tuning without composed seam",
			configure: func(
				cfg *config.Config,
				_ *Stage4RunContext,
			) {
				cfg.Migration.RuntimeTuning = true
			},
			observer: func(
				events *[]string,
				run Stage4RunContext,
			) TableObserver {
				return stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{
						events: events,
					},
					run: run,
				}
			},
			want: "chunk-boundary tuning seam",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "stage4-pre-open-" +
				strings.ReplaceAll(test.name, " ", "-")
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			run := stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			)
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			if test.configure != nil {
				test.configure(&cfg, &run)
			}
			route := resolvedAdapterRoute{
				source: sourceRole{
					engine: "postgres",
					open: func(
						context.Context,
						config.Endpoint,
					) (sourceAdapter, error) {
						events = append(events, "source_open")
						return nil, errors.New("unexpected source open")
					},
				},
				target: targetRole{
					engine: "sqlite",
					open: func(
						context.Context,
						config.Endpoint,
					) (targetAdapter, error) {
						events = append(events, "target_open")
						return nil, errors.New("unexpected target open")
					},
				},
			}
			_, err := route.execute(
				context.Background(),
				cfg,
				test.observer(&events, run),
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("admission error = %v, want %q", err, test.want)
			}
			if len(events) != 0 {
				t.Fatalf(
					"failed Stage 4 admission opened an endpoint: %v",
					events,
				)
			}
			tasks, ranges, listErr := backend.ListWork(runID)
			if listErr != nil {
				t.Fatal(listErr)
			}
			if len(tasks) != 0 || len(ranges) != 0 {
				t.Fatalf(
					"failed Stage 4 admission wrote work: tasks=%v ranges=%v",
					tasks,
					ranges,
				)
			}
		})
	}
}

func TestStage4AdapterResumeAdmissionPrecedesEndpointConstruction(
	t *testing.T,
) {
	events := make([]string, 0)
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-resume-pre-open"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine: "postgres",
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				events = append(events, "source_open")
				return nil, errors.New("unexpected source open")
			},
		}},
		[]targetRole{{
			engine:     "sqlite",
			capability: testTargetCapability,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				events = append(events, "target_open")
				return nil, errors.New("unexpected target open")
			},
		}},
		[]adapterPair{{source: "postgres", target: "sqlite"}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeResumeWithRegistry(
		context.Background(),
		cfg,
		nil,
		observer,
		registry,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "fresh Stage 4 run context") {
		t.Fatalf("resume admission error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("failed resume admission opened an endpoint: %v", events)
	}
}

func TestStage4AdapterFreshRequiresOrdinaryTableSetCheckpoint(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-missing-table-set-observer"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterPerTableObserver{
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	_, err := migrateWithAdapters(
		context.Background(),
		stage4AdapterTestConfig(
			t,
			"source-password",
			"target-password",
		),
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "requires a table-set observer") {
		t.Fatalf("missing table-set observer error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf(
			"missing ordinary checkpoint reached adapter work: %v",
			events,
		)
	}
	tasks, ranges, listErr := backend.ListWork(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(tasks) != 0 || len(ranges) != 0 {
		t.Fatalf(
			"missing ordinary checkpoint wrote structured work: tasks=%v ranges=%v",
			tasks,
			ranges,
		)
	}
}

func TestStage4AdapterUpsertDeepValidationRequiresEqualityProofSeam(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-upsert-deep-validation-proof"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Validation.Mode = config.ValidationNullParity
	observer := stage4AdapterValidationProviderObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			),
		},
	}
	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"primary-key equality proof seam",
		) {
		t.Fatalf("deep upsert validation error = %v", err)
	}
	if stage4AdapterEventsContain(events, "before") ||
		stage4AdapterEventIndex(events, "target_prepare") >= 0 ||
		stage4AdapterEventIndex(events, "target_write") >= 0 ||
		stage4AdapterEventIndex(events, "target_finalize") >= 0 {
		t.Fatalf(
			"deep upsert validation crossed admission: %v",
			events,
		)
	}
}

func TestStage4AdapterUpsertDeepValidationBindsEqualityProofBeforeTargetMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-upsert-deep-validation-proof-bound"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Validation.Mode = config.ValidationNullParity
	proof := strings.Repeat("a", 64)
	calls := make([]schema.Table, 0)
	observer := stage4AdapterValidationProviderObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			),
		},
		equalityProofEnabled: true,
		equalityProof:        proof,
		equalityProofCalls:   &calls,
	}
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
		t.Fatalf("prepare Stage 4 adapter run: %v", err)
	}
	if len(calls) != 1 ||
		calls[0].Schema != "public" ||
		calls[0].Name != "items" {
		t.Fatalf("equality proof calls = %#v", calls)
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		t.Fatalf("build validation specs: %v", err)
	}
	if len(specs) != 1 ||
		specs[0].PrimaryKeyEqualityProof != proof {
		t.Fatalf("validation specs = %#v", specs)
	}
	if stage4AdapterEventsContain(events, "before") ||
		stage4AdapterEventIndex(events, "target_prepare") >= 0 ||
		stage4AdapterEventIndex(events, "target_write") >= 0 ||
		stage4AdapterEventIndex(events, "target_finalize") >= 0 {
		t.Fatalf(
			"equality proof preparation crossed target mutation: %v",
			events,
		)
	}
}

func TestStage4AdapterUpsertDeepValidationRejectsInvalidEqualityProofProviders(
	t *testing.T,
) {
	table := stage4AdapterTestTable()
	countProbe := &stage4AdapterCountProbe{}
	var typedNil *stage4AdapterValidationEqualityProofTestProbe
	tests := []struct {
		name  string
		probe ValidationCoreProbe
		want  string
	}{
		{
			name:  "missing provider",
			probe: countProbe,
			want:  "equality proof seam",
		},
		{
			name:  "typed nil provider",
			probe: typedNil,
			want:  "equality proof seam",
		},
		{
			name: "invalid digest",
			probe: &stage4AdapterValidationEqualityProofTestProbe{
				stage4AdapterCountProbe: countProbe,
				proof:                   "not-a-digest",
			},
			want: "canonical SHA-256 digest",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
				config.ValidationNullParity,
				"upsert",
				test.probe,
				[]schema.Table{table},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("proof admission error = %v", err)
			}
		})
	}
}

func TestStage4AdapterDeepValidationRejectsTypedNilProbeBeforeTargetMutation(
	t *testing.T,
) {
	for _, targetMode := range []string{"drop_recreate", "upsert"} {
		t.Run(targetMode, func(t *testing.T) {
			events := make([]string, 0)
			source := &recordingAdapterSource{
				events: &events,
				table:  stage4AdapterTestTable(),
			}
			target := &recordingAdapterTarget{events: &events}
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			runID := "stage4-typed-nil-validation-" + targetMode
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				time.Now().Add(-time.Minute),
			)
			cfg := stage4AdapterTestConfig(
				t,
				"source-password",
				"target-password",
			)
			cfg.Migration.TargetMode = targetMode
			cfg.Migration.Validation.Mode =
				config.ValidationNullParity
			observer := stage4AdapterValidationProviderObserver{
				stage4AdapterObserver: stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{
						events: &events,
					},
					run: stage4LifecycleRunContext(
						t,
						backend,
						runID,
						false,
					),
				},
				returnTypedNilProbe: true,
			}
			_, err := migrateWithAdapters(
				context.Background(),
				cfg,
				observer,
				source,
				target,
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"returned no probe",
				) {
				t.Fatalf(
					"typed-nil %s validation error = %v",
					targetMode,
					err,
				)
			}
			if stage4AdapterEventsContain(events, "before") ||
				stage4AdapterEventIndex(
					events,
					"target_prepare",
				) >= 0 ||
				stage4AdapterEventIndex(
					events,
					"target_write",
				) >= 0 ||
				stage4AdapterEventIndex(
					events,
					"target_finalize",
				) >= 0 {
				t.Fatalf(
					"typed-nil %s probe crossed target mutation: %v",
					targetMode,
					events,
				)
			}
		})
	}
}

func TestStage4ValidationProviderSkipsTypedNilProvider(t *testing.T) {
	var typedNil *stage4AdapterTypedNilValidationProvider
	if provider := stage4ValidationProvider(
		typedNil,
		&recordingAdapterSource{},
		&recordingAdapterTarget{},
	); provider != nil {
		t.Fatalf("typed-nil validation provider = %#v", provider)
	}
}

func TestStage4AdapterCredentialChangesDoNotChangeWorkTopology(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-credential-change"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	cfg := stage4AdapterTestConfig(
		t,
		"source-password-one",
		"target-password-one",
	)
	first, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: run,
		},
		source,
		target,
		"drop_recreate",
		run,
	)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	cfg.Source.Password = "source-password-two"
	cfg.Target.Password = "target-password-two"
	second, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{
				events: &events,
			},
			run: run,
		},
		source,
		target,
		"drop_recreate",
		run,
	)
	if err != nil {
		t.Fatalf("prepare after credential rotation: %v", err)
	}
	if first.configDigest != second.configDigest ||
		first.gate.TopologyHash != second.gate.TopologyHash ||
		!reflect.DeepEqual(first.work, second.work) {
		t.Fatalf(
			"credential rotation changed durable topology: first=%#v second=%#v",
			first,
			second,
		)
	}
}

func TestStage4AdapterSnapshotSaveFailurePrecedesEveryTargetMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	raw := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-save-failure-resume"
	initializeStage4LifecycleRun(
		t,
		raw,
		runID,
		time.Now().Add(-time.Minute),
	)
	backend := &stage4AdapterSaveFailureBackend{
		Stage4StateBackend: raw,
		fail:               true,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	freshObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		freshObserver,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "save validated Stage 4 schema") {
		t.Fatalf("save failure error = %v", err)
	}
	if result != (Result{}) {
		t.Fatalf("result before schema staging = %#v", result)
	}
	for _, forbidden := range []string{
		"target_prepare",
		"target_write",
		"target_finalize",
		"source_count",
		"target_count",
		"after:items",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"schema staging failure crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
	if _, found, loadErr := raw.LoadSchemaSnapshot(
		runID,
		stage4SchemaGateTask,
	); loadErr != nil || found {
		t.Fatalf(
			"failed save published snapshot found=%v err=%v",
			found,
			loadErr,
		)
	}
}

func TestStage4AdapterResumeRejectsCompletedCheckpointWithoutStructuredWork(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{
		events:      &events,
		rowsByTable: map[string]int{"items": 2},
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-missing-structured-completion"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	_, err := resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		observer,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"lacks exact durable network work",
		) {
		t.Fatalf("missing structured work error = %v", err)
	}
	for _, forbidden := range []string{
		"before_tables:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"missing structured work crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterResumeRejectsCompletedCheckpointWithRunningStructuredRange(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{
		events:      &events,
		rowsByTable: map[string]int{"items": 2},
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-completed-checkpoint-running-range"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2

	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared = bindStage4AdapterTestStableNetworkWork(
		t,
		context.Background(),
		cfg,
		source,
		prepared,
	)
	if err := ensureStage4AdapterWork(
		context.Background(),
		run,
		prepared.work,
	); err != nil {
		t.Fatal(err)
	}
	item := prepared.work[0]
	if len(item.ranges) != 2 {
		t.Fatalf("network ranges = %#v", item.ranges)
	}
	if err := backend.CompleteRange(
		runID,
		item.task,
		item.ranges[0].ID,
		item.topology,
		0,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	_, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var completedRanges, runningRanges int
	for _, workRange := range ranges {
		if workRange.Task != item.task {
			continue
		}
		switch workRange.Status {
		case "completed":
			completedRanges++
		case "running":
			runningRanges++
		}
	}
	if completedRanges != 1 || runningRanges != 1 {
		t.Fatalf(
			"structured range states completed=%d running=%d: %#v",
			completedRanges,
			runningRanges,
			ranges,
		)
	}

	events = events[:0]
	_, err = resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		observer,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"lacks exact durable network work",
		) {
		t.Fatalf("running structured range error = %v", err)
	}
	for _, forbidden := range []string{
		"before_tables:items",
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"running structured range crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterResumeRejectsCompletedCheckpointWithMismatchedStructuredRows(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{
		events:      &events,
		rowsByTable: map[string]int{"items": 2},
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-completed-checkpoint-mismatched-structured-rows"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2

	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared = bindStage4AdapterTestStableNetworkWork(
		t,
		context.Background(),
		cfg,
		source,
		prepared,
	)
	if err := ensureStage4AdapterWork(
		context.Background(),
		run,
		prepared.work,
	); err != nil {
		t.Fatal(err)
	}
	if len(prepared.network.bindings) != 2 {
		t.Fatalf(
			"network bindings = %#v",
			prepared.network.bindings,
		)
	}
	for _, binding := range prepared.network.bindings {
		if err := prepared.network.checkpoint(
			context.Background(),
			NetworkRangeCheckpoint{
				RangeIndex:   binding.RangeIndex,
				TopologyHash: binding.Initial.TopologyHash,
				Frontier: AckFrontier{
					RangeID: fmt.Sprintf(
						"range/%d",
						binding.RangeIndex,
					),
				},
				Complete: true,
			},
		); err != nil {
			t.Fatalf(
				"complete empty structured range %d: %v",
				binding.RangeIndex,
				err,
			)
		}
	}
	item := prepared.work[0]
	if err := backend.CompleteWorkTask(
		runID,
		item.task,
		item.topology,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("complete structured task: %v", err)
	}
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var taskStatus string
	for _, task := range tasks {
		if task.Key == item.task {
			taskStatus = task.Status
		}
	}
	var completedRanges int
	var structuredRows int64
	for _, workRange := range ranges {
		if workRange.Task != item.task {
			continue
		}
		if workRange.Status == "completed" {
			completedRanges++
		}
		structuredRows += workRange.RowsDone
	}
	if taskStatus != "completed" ||
		completedRanges != 2 ||
		structuredRows != 0 {
		t.Fatalf(
			"completed structured evidence task=%q ranges=%d rows=%d: tasks=%#v ranges=%#v",
			taskStatus,
			completedRanges,
			structuredRows,
			tasks,
			ranges,
		)
	}

	events = events[:0]
	_, err = resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{
			"items": {Rows: 2},
		},
		observer,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"checkpoint differs from durable ranges",
		) {
		t.Fatalf("mismatched structured row total error = %v", err)
	}
	for _, forbidden := range []string{
		"before_tables:items",
		"before:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"mismatched structured row total crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterResumeResetsIncompletePlanBeforeInteriorInsertReplay(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
		ids:    []int64{10, 30, 50},
		rows:   []string{"ten", "thirty", "fifty"},
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-completed-range-incomplete-table"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 1
	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared = bindStage4AdapterTestStableNetworkWork(
		t,
		context.Background(),
		cfg,
		source,
		prepared,
	)
	if err := ensureStage4AdapterWork(
		context.Background(),
		run,
		prepared.work,
	); err != nil {
		t.Fatal(err)
	}
	item := prepared.work[0]
	firstFrontier, err := adapterRangePageEncodeFrontier(
		[]int64{30},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(item.ranges) != 1 {
		t.Fatalf("network ranges = %#v", item.ranges)
	}
	firstIssued := NetworkIssuedChunk{
		RangeIndex:  0,
		Sequence:    0,
		Rows:        2,
		EndFrontier: firstFrontier,
		Fingerprint: strings.Repeat("a", 64),
		Exhausted:   false,
	}
	if err := prepared.network.recordIssued(
		context.Background(),
		firstIssued,
	); err != nil {
		t.Fatal(err)
	}
	firstRows := [][]any{
		{int64(10), []byte("ten")},
		{int64(30), []byte("thirty")},
	}
	write := prepared.network.wrapWrite(
		observer,
		func(
			ctx context.Context,
			_ NetworkWriteRequest,
		) (WriteReceipt, error) {
			return target.WriteStage4NetworkBatch(
				ctx,
				prepared.plans[0].target,
				prepared.plans[0].columns,
				firstRows,
			)
		},
	)
	if _, err := write(
		context.Background(),
		NetworkWriteRequest{
			Range: NetworkRangePlan{
				RangeIndex:   0,
				TopologyHash: item.topology,
			},
			Sequence: 0,
			Mode:     NetworkWriteIdempotentUpsert,
			Rows:     firstRows,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := prepared.network.checkpoint(
		context.Background(),
		NetworkRangeCheckpoint{
			RangeIndex:   0,
			TopologyHash: item.topology,
			Frontier: AckFrontier{
				RangeID:      stage4AdapterCopyRangeID,
				NextSequence: 1,
				Rows:         2,
			},
			FrontierBytes: firstFrontier,
			Complete:      false,
		},
	); err != nil {
		t.Fatal(err)
	}
	restores, err := prepared.network.loadRestores(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("restore partial network work: %v", err)
	}
	if len(restores) != 1 ||
		restores[0].Complete ||
		restores[0].RowsDone != 2 {
		t.Fatalf("partial network restores = %#v", restores)
	}
	// The inserted key is behind the saved keyset frontier while the old
	// min/max and topology remain unchanged. Resume must reset the entire
	// incomplete task, not reuse the positional frontier and skip id=20.
	source.ids = []int64{10, 20, 30, 50}
	source.rows = []string{"ten", "twenty", "thirty", "fifty"}
	events = events[:0]
	result, err := resumeWithAdapters(
		context.Background(),
		cfg,
		nil,
		observer,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("resume reset network work: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 4, Validated: true}) {
		t.Fatalf("reset network resume result = %#v", result)
	}
	if stage4AdapterEventIndex(events, "target_prepare") < 0 ||
		stage4AdapterEventIndex(events, "target_write") <
			stage4AdapterEventIndex(events, "target_prepare") {
		t.Fatalf("reset network resume order = %v", events)
	}
	targetRows, err := target.CountRows(
		context.Background(),
		prepared.plans[0].target,
	)
	if err != nil {
		t.Fatal(err)
	}
	if targetRows != 4 {
		t.Fatalf("target rows after reset replay = %d, want 4", targetRows)
	}
	var sawInterior bool
	for _, row := range target.captured {
		if len(row) != 0 && row[0] == int64(20) {
			sawInterior = true
		}
	}
	if !sawInterior {
		t.Fatalf("reset replay skipped interior key: %#v", target.captured)
	}
}

func TestStage4AdapterResumeRejectsStaleTableWorkOutsideCurrentPlan(
	t *testing.T,
) {
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-stale-table-work"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		true,
	)
	stale := state.TaskKey{
		Type:   "table-copy",
		Schema: "public",
		Table:  "removed",
	}
	if _, err := backend.EnsureWorkPlan(
		state.WorkTask{
			RunID:        runID,
			Key:          stale,
			Strategy:     stage4AdapterCopyStrategy,
			TopologyHash: "stale-topology",
		},
		[]state.RangeState{{
			ID:           stage4AdapterCopyRangeID,
			Strategy:     stage4AdapterCopyStrategy,
			TopologyHash: "stale-topology",
		}},
	); err != nil {
		t.Fatal(err)
	}
	err := verifyStage4ResumeWorkEvidence(
		context.Background(),
		run,
		[]stage4AdapterWork{{
			task: state.TaskKey{
				Type:   "table-copy",
				Schema: "public",
				Table:  "items",
			},
			topology: "current-topology",
		}},
		nil,
		true,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "unexpected stale Stage 4 table work") {
		t.Fatalf("stale work error = %v", err)
	}
}

func TestStage4AdapterRejectsCompletedSchemaSentinelWithoutSnapshot(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-completed-schema-without-snapshot"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	configDigest, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := PrepareStage4SchemaGate(
		run,
		[]schema.Table{stage4AdapterTestTable()},
		Stage4SchemaGateOptions{
			SourceEngine:       source.Engine(),
			TargetEngine:       target.Engine(),
			TargetMode:         "drop_recreate",
			IncludeTables:      cfg.Migration.IncludeTables,
			ExcludeTables:      cfg.Migration.ExcludeTables,
			ConfigIdentity:     configDigest,
			Contract:           cfg.Migration.SchemaContract,
			FailOnSchemaDrift:  cfg.Migration.FailOnSchemaDrift,
			DateUpdatedColumns: cfg.Migration.DateUpdatedColumns,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		run,
		gate.Task,
		stage4SchemaGateRangeID,
		gate.TopologyHash,
	); err != nil {
		t.Fatal(err)
	}
	events = events[:0]
	_, err = migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"complete without its prior validated snapshot",
		) {
		t.Fatalf("schema sentinel error = %v", err)
	}
	for _, forbidden := range []string{
		"target_plan",
		"before_tables:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"invalid schema sentinel crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
}

func TestStage4AdapterValidationFailureNeverPublishesSchemaOrCompletion(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	baseTarget := &recordingAdapterTarget{events: &events}
	target := &stage4AdapterCountMismatchTarget{
		recordingAdapterTarget: baseTarget,
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-validation-failure"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	_, err := migrateWithAdapters(
		context.Background(),
		stage4AdapterTestConfig(
			t,
			"source-password",
			"target-password",
		),
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "post-finalize validation failed") {
		t.Fatalf("validation error = %v", err)
	}
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_finalize",
		"source_count",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"target_finalize",
		"target_count",
	)
	if stage4AdapterEventIndex(events, "after:items") >= 0 {
		t.Fatalf(
			"failed validation completed ordinary checkpoint: %v",
			events,
		)
	}
	snapshot, found, loadErr := backend.LoadSchemaSnapshot(
		runID,
		stage4SchemaGateTask,
	)
	if loadErr != nil || !found || snapshot.Digest == "" {
		t.Fatalf(
			"failed validation did not retain its staged immutable schema found=%v snapshot=%#v err=%v",
			found,
			snapshot,
			loadErr,
		)
	}
	tasks, ranges, listErr := backend.ListWork(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	for _, task := range tasks {
		if task.Status != "running" {
			t.Fatalf(
				"failed validation completed task %#v: %q",
				task.Key,
				task.Status,
			)
		}
	}
	for _, workRange := range ranges {
		if workRange.Status != "running" {
			t.Fatalf(
				"failed validation completed range %#v: %q",
				workRange.Task,
				workRange.Status,
			)
		}
	}
}

func TestStage4AdapterCancellationAfterFinalizeCannotPublishCompletion(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &stage4AdapterCancelAfterFinalizeTarget{
		recordingAdapterTarget: &recordingAdapterTarget{events: &events},
		cancel:                 cancel,
	}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-cancel-after-finalize"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	_, err := migrateWithAdapters(
		ctx,
		stage4AdapterTestConfig(
			t,
			"source-password",
			"target-password",
		),
		observer,
		source,
		target,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if stage4AdapterEventIndex(events, "target_finalize") < 0 {
		t.Fatalf("fixture did not reach finalization: %v", events)
	}
	for _, forbidden := range []string{
		"source_count",
		"target_count",
		"after:items",
	} {
		if stage4AdapterEventIndex(events, forbidden) >= 0 {
			t.Fatalf(
				"canceled Stage 4 run crossed %s: %v",
				forbidden,
				events,
			)
		}
	}
	snapshot, found, loadErr := backend.LoadSchemaSnapshot(
		runID,
		stage4SchemaGateTask,
	)
	if loadErr != nil || !found || snapshot.Digest == "" {
		t.Fatalf(
			"canceled run did not retain its staged immutable schema found=%v snapshot=%#v err=%v",
			found,
			snapshot,
			loadErr,
		)
	}
	tasks, ranges, listErr := backend.ListWork(runID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	for _, task := range tasks {
		if task.Status != "running" {
			t.Fatalf("canceled run completed task %#v", task.Key)
		}
	}
	for _, workRange := range ranges {
		if workRange.Status != "running" {
			t.Fatalf(
				"canceled run completed range %#v",
				workRange.Task,
			)
		}
	}
}

func TestStage4AdapterPlansExactRichSchemaContractProjection(
	t *testing.T,
) {
	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	defaultValue, err := schema.ParseSQLiteDefault(`'kept'`)
	if err != nil {
		t.Fatal(err)
	}
	previous.Columns[1].DeclaredType = &schema.DeclaredType{
		Base:      "VARCHAR",
		Arguments: []int{80},
	}
	previous.Columns[1].Default = defaultValue
	previous.Indexes = []schema.Index{{
		Name:    "items_payload_idx",
		Columns: []schema.IndexColumn{{Name: "payload"}},
	}}
	current := previous
	current.Columns = append(
		append([]schema.Column(nil), previous.Columns...),
		schema.Column{
			Name:     "discarded",
			Type:     "text",
			Nullable: true,
		},
	)
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	initializeStage4LifecycleRun(
		t,
		backend,
		"stage4-projection-previous",
		started,
	)
	previousRun := stage4LifecycleRunContext(
		t,
		backend,
		"stage4-projection-previous",
		false,
	)
	baseline, err := PrepareStage4SchemaGate(
		previousRun,
		[]schema.Table{previous},
		Stage4SchemaGateOptions{
			SourceEngine:   "postgres",
			TargetEngine:   "sqlite",
			TargetMode:     "upsert",
			ConfigIdentity: "projection-baseline",
			CapturedAt:     started,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(
		baseline.PendingSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		previousRun,
		baseline.Task,
		stage4SchemaGateRangeID,
		baseline.TopologyHash,
	); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID:             "stage4-projection-previous",
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

	currentRunID := "stage4-projection-current"
	initializeStage4LifecycleRun(
		t,
		backend,
		currentRunID,
		started.Add(90*time.Second),
	)
	source := &recordingAdapterSource{
		events: &events,
		table:  current,
	}
	baseTarget := &recordingAdapterTarget{events: &events}
	target := &stage4AdapterPlanningTarget{
		recordingAdapterTarget: baseTarget,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractDiscardValue,
		DataType: config.SchemaContractEvolve,
	}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			currentRunID,
			false,
		),
	}
	if _, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	); err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if len(target.received) != 1 ||
		len(target.received[0].Columns) != 2 ||
		target.received[0].Columns[1].Name != "payload" {
		t.Fatalf(
			"target received non-projected schema: %#v",
			target.received,
		)
	}
	projected := target.received[0]
	if projected.Columns[1].Default == nil ||
		projected.Columns[1].Default.CanonicalSQL() != `'kept'` ||
		projected.Columns[1].DeclaredType == nil ||
		!reflect.DeepEqual(
			projected.Columns[1].DeclaredType.Arguments,
			[]int{80},
		) ||
		len(projected.Indexes) != 1 ||
		projected.Indexes[0].Name != "items_payload_idx" {
		t.Fatalf("rich projection was weakened: %#v", projected)
	}
}

func TestStage4AdapterRejectsRetainedRebuildWithoutTargetCatalogSeam(
	t *testing.T,
) {
	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	current := previous
	current.Columns = append(
		[]schema.Column(nil),
		previous.Columns[:1]...,
	)
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	previousID := "stage4-retained-rebuild-previous"
	initializeStage4LifecycleRun(
		t,
		backend,
		previousID,
		started,
	)
	previousRun := stage4LifecycleRunContext(
		t,
		backend,
		previousID,
		false,
	)
	baseline, err := PrepareStage4SchemaGate(
		previousRun,
		[]schema.Table{previous},
		Stage4SchemaGateOptions{
			SourceEngine:   "postgres",
			TargetEngine:   "sqlite",
			TargetMode:     "drop_recreate",
			ConfigIdentity: "retained-rebuild-baseline",
			CapturedAt:     started,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(
		baseline.PendingSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		previousRun,
		baseline.Task,
		stage4SchemaGateRangeID,
		baseline.TopologyHash,
	); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID:             previousID,
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

	currentID := "stage4-retained-rebuild-current"
	initializeStage4LifecycleRun(
		t,
		backend,
		currentID,
		started.Add(90*time.Second),
	)
	source := &recordingAdapterSource{
		events: &events,
		table:  current,
	}
	target := &recordingAdapterTarget{events: &events}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			currentID,
			false,
		),
	}
	_, err = migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "target-catalog rebuild seam") {
		t.Fatalf("retained rebuild error = %v", err)
	}
	if stage4AdapterEventIndex(events, "target_plan") >= 0 ||
		stage4AdapterEventsContain(events, "before") ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf(
			"retained rebuild crossed fail-closed boundary: %v",
			events,
		)
	}
}

func TestStage4AdapterRejectsUpsertEvolutionDecisionBeforeTargetPlanning(
	t *testing.T,
) {
	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	current := previous
	current.Columns = append(
		append([]schema.Column(nil), previous.Columns...),
		schema.Column{
			Name:     "added",
			Type:     "text",
			Nullable: true,
		},
	)
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	stage4AdapterInstallSuccessfulBaseline(
		t,
		backend,
		"stage4-upsert-evolution-previous",
		started,
		previous,
		"upsert",
	)
	currentID := "stage4-upsert-evolution-current"
	initializeStage4LifecycleRun(
		t,
		backend,
		currentID,
		started.Add(90*time.Second),
	)
	source := &recordingAdapterSource{
		events: &events,
		table:  current,
	}
	target := &recordingAdapterTarget{events: &events}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractEvolve,
		Columns:  config.SchemaContractEvolve,
		DataType: config.SchemaContractEvolve,
	}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: stage4LifecycleRunContext(
			t,
			backend,
			currentID,
			false,
		),
	}
	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "target-catalog evolution executor seam") ||
		!strings.Contains(err.Error(), "add_column") {
		t.Fatalf("upsert evolution error = %v", err)
	}
	if stage4AdapterEventIndex(events, "target_plan") >= 0 ||
		stage4AdapterEventsContain(events, "before") ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf(
			"upsert evolution crossed fail-closed boundary: %v",
			events,
		)
	}
}

func TestStage4AdapterWorkTopologyIncludesCanonicalDefaults(
	t *testing.T,
) {
	first := stage4AdapterTestTable()
	firstDefault, err := schema.ParseSQLiteDefault(`'first'`)
	if err != nil {
		t.Fatal(err)
	}
	first.Columns[1].Default = firstDefault
	second := first
	second.Columns = append([]schema.Column(nil), first.Columns...)
	secondDefault, err := schema.ParseSQLiteDefault(`'second'`)
	if err != nil {
		t.Fatal(err)
	}
	second.Columns[1].Default = secondDefault
	targetFirst := first
	targetFirst.Schema = ""
	targetSecond := second
	targetSecond.Schema = ""
	firstWork, err := buildStage4AdapterWork(
		"configuration",
		"upsert",
		[]adapterTablePlan{{
			source:  first,
			target:  targetFirst,
			columns: adapterColumnNames(first),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	secondWork, err := buildStage4AdapterWork(
		"configuration",
		"upsert",
		[]adapterTablePlan{{
			source:  second,
			target:  targetSecond,
			columns: adapterColumnNames(second),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstWork) != 1 ||
		len(secondWork) != 1 ||
		firstWork[0].topology == secondWork[0].topology {
		t.Fatalf(
			"default change was absent from work topology: first=%v second=%v",
			firstWork,
			secondWork,
		)
	}
}

func TestStage4AdapterUpsertWorkTopologyIgnoresIdentityFrontier(
	t *testing.T,
) {
	source := stage4AdapterTestTable()
	sourceFrontier := int64(2)
	source.Identity = &schema.Identity{
		Column:     "id",
		Generation: schema.IdentityByDefault,
		Frontier:   &sourceFrontier,
	}
	target := cloneStage4RichTable(source)
	target.Schema = ""
	targetFrontier := int64(7)
	target.Identity.Frontier = &targetFrontier
	first, err := buildStage4AdapterWork(
		"configuration",
		"upsert",
		[]adapterTablePlan{{
			source:  source,
			target:  target,
			columns: adapterColumnNames(source),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	laterSource := cloneStage4RichTable(source)
	laterTarget := cloneStage4RichTable(target)
	laterSourceFrontier := int64(200)
	laterTargetFrontier := int64(700)
	laterSource.Identity.Frontier = &laterSourceFrontier
	laterTarget.Identity.Frontier = &laterTargetFrontier
	second, err := buildStage4AdapterWork(
		"configuration",
		"upsert",
		[]adapterTablePlan{{
			source:  laterSource,
			target:  laterTarget,
			columns: adapterColumnNames(laterSource),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 ||
		first[0].topology != second[0].topology {
		t.Fatalf(
			"mutable identity frontier changed upsert work topology: first=%#v second=%#v",
			first,
			second,
		)
	}
}

func TestStage4AdapterCountProbeNeverRelabelsExactCountAsEstimate(
	t *testing.T,
) {
	events := make([]string, 0)
	table := stage4AdapterTestTable()
	targetTable := table
	targetTable.Schema = ""
	probe := &stage4AdapterCountProbe{
		source: &recordingAdapterSource{
			events: &events,
			table:  table,
		},
		target: &recordingAdapterTarget{events: &events},
		plans: stage4AdapterPlansBySource([]adapterTablePlan{{
			source:  table,
			target:  targetTable,
			columns: adapterColumnNames(table),
		}}),
	}
	if _, err := probe.EstimateCount(
		context.Background(),
		ValidationSource,
		table,
	); err == nil ||
		!strings.Contains(err.Error(), "estimate is unavailable") {
		t.Fatalf("estimate error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("estimate reran exact count: %v", events)
	}
}

func stage4AdapterInstallSuccessfulBaseline(
	t *testing.T,
	backend state.YAMLStore,
	runID string,
	started time.Time,
	table schema.Table,
	mode string,
) {
	t.Helper()
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		started,
	)
	run := stage4LifecycleRunContext(
		t,
		backend,
		runID,
		false,
	)
	baseline, err := PrepareStage4SchemaGate(
		run,
		[]schema.Table{table},
		Stage4SchemaGateOptions{
			SourceEngine:   "postgres",
			TargetEngine:   "sqlite",
			TargetMode:     mode,
			ConfigIdentity: "stage4-adapter-baseline",
			CapturedAt:     started,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(
		baseline.PendingSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	if err := completeStage4WorkTask(
		context.Background(),
		run,
		baseline.Task,
		stage4SchemaGateRangeID,
		baseline.TopologyHash,
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

type stage4AdapterSaveFailureBackend struct {
	Stage4StateBackend
	fail bool
}

func (backend *stage4AdapterSaveFailureBackend) SaveSchemaSnapshot(
	snapshot state.SchemaSnapshot,
) error {
	if backend.fail {
		return errors.New("injected schema snapshot save failure")
	}
	return backend.Stage4StateBackend.SaveSchemaSnapshot(snapshot)
}

type stage4AdapterPlanningTarget struct {
	*recordingAdapterTarget
	received []schema.Table
}

func (target *stage4AdapterPlanningTarget) PlanTables(
	sourceEngine string,
	sourceTables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	target.received = append(
		[]schema.Table(nil),
		sourceTables...,
	)
	return target.recordingAdapterTarget.PlanTables(
		sourceEngine,
		sourceTables,
		mode,
	)
}

type stage4AdapterCountMismatchTarget struct {
	*recordingAdapterTarget
}

func (target *stage4AdapterCountMismatchTarget) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	count, err := target.recordingAdapterTarget.CountRows(ctx, table)
	return count + 1, err
}

type stage4AdapterCancelAfterFinalizeTarget struct {
	*recordingAdapterTarget
	cancel context.CancelFunc
}

func (target *stage4AdapterCancelAfterFinalizeTarget) FinalizeTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	if err := target.recordingAdapterTarget.FinalizeTables(
		ctx,
		tables,
		mode,
	); err != nil {
		return err
	}
	target.cancel()
	return nil
}

type stage4AdapterPerTableObserver struct {
	run Stage4RunContext
}

func (observer stage4AdapterPerTableObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (observer stage4AdapterPerTableObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func (observer stage4AdapterPerTableObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func stage4AdapterTestTable() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
	}
}

func bindStage4AdapterTestStableNetworkWork(
	t *testing.T,
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	prepared stage4AdapterPrepared,
) stage4AdapterPrepared {
	t.Helper()
	if len(prepared.plans) != len(prepared.work) {
		t.Fatal("deferred Stage 4 test work is incomplete")
	}
	bound := make([]stage4AdapterWork, len(prepared.work))
	for index := range prepared.plans {
		session, err := OpenAdapterStableNetworkTableSource(
			ctx,
			source,
			prepared.plans[index].source,
		)
		if err != nil {
			t.Fatal(err)
		}
		stable, err := session.Source()
		if err != nil {
			_ = session.Close()
			t.Fatal(err)
		}
		items, err := bindStage4AdapterPagination(
			ctx,
			stable,
			cfg.Migration.Partitions,
			[]stage4AdapterWork{prepared.work[index]},
			[]adapterTablePlan{prepared.plans[index]},
		)
		closeErr := session.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		bound[index] = items[0]
	}
	coordinator, err := newStage4AdapterNetworkCoordinator(
		prepared.run,
		bound,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared.work = bound
	prepared.network = coordinator
	return prepared
}

func stage4AdapterTestConfig(
	t *testing.T,
	sourcePassword string,
	targetPassword string,
) config.Config {
	t.Helper()
	return config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     "source.example",
			Port:     5432,
			Database: "app",
			User:     "source-user",
			Password: sourcePassword,
		},
		Target: config.Endpoint{
			Type:     "sqlite",
			Database: filepath.Join(t.TempDir(), "target.db"),
			Password: targetPassword,
		},
		Migration: config.Migration{
			TargetMode: "drop_recreate",
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationCountOnly,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
			Deletes: config.DeletePolicy{
				Mode: config.DeleteModeOff,
			},
		},
	}
}

func assertStage4AdapterEventBefore(
	t *testing.T,
	events []string,
	before string,
	after string,
) {
	t.Helper()
	beforeIndex := -1
	afterIndex := -1
	for index, event := range events {
		if event == before && beforeIndex < 0 {
			beforeIndex = index
		}
		if event == after && afterIndex < 0 {
			afterIndex = index
		}
	}
	if beforeIndex < 0 || afterIndex < 0 || beforeIndex >= afterIndex {
		t.Fatalf(
			"event %q must precede %q: %v",
			before,
			after,
			events,
		)
	}
}

func assertStage4AdapterWorkCompleted(
	t *testing.T,
	tasks []state.WorkTask,
	ranges []state.RangeState,
	key state.TaskKey,
	rangeID string,
) {
	t.Helper()
	var taskFound, rangeFound bool
	for _, task := range tasks {
		if task.Key == key {
			taskFound = true
			if task.Status != "completed" {
				t.Fatalf("task %#v status = %q", key, task.Status)
			}
		}
	}
	for _, workRange := range ranges {
		if workRange.Task == key && workRange.ID == rangeID {
			rangeFound = true
			if workRange.Status != "completed" {
				t.Fatalf(
					"range %q for %#v status = %q",
					rangeID,
					key,
					workRange.Status,
				)
			}
		}
	}
	if !taskFound || !rangeFound {
		t.Fatalf(
			"completed work %#v/%q found task=%v range=%v; tasks=%v ranges=%v",
			key,
			rangeID,
			taskFound,
			rangeFound,
			tasks,
			ranges,
		)
	}
}

func stage4AdapterEventIndex(events []string, value string) int {
	for index, event := range events {
		if event == value {
			return index
		}
	}
	return -1
}

func stage4AdapterEventsContain(events []string, prefix string) bool {
	for _, event := range events {
		if strings.HasPrefix(event, prefix) {
			return true
		}
	}
	return false
}
