package migrate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4CheckpointFrequencyConfigurationAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*config.Config)
		wantErr string
	}{
		{
			name: "ordinary deferred network route is admitted",
		},
		{
			name: "date incremental remains fail closed",
			mutate: func(cfg *config.Config) {
				cfg.Migration.DateUpdatedColumns = []string{"updated_at"}
			},
			wantErr: "not yet composed with date-based incremental",
		},
		{
			name: "strict route remains fail closed",
			mutate: func(cfg *config.Config) {
				cfg.Migration.StrictConsistency = true
			},
			wantErr: "not yet composed with strict consistency",
		},
		{
			name: "delete reconciliation remains fail closed",
			mutate: func(cfg *config.Config) {
				cfg.Migration.Deletes.Mode = config.DeleteModeReconcile
			},
			wantErr: "not yet composed with delete reconciliation",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := stage4CheckpointFrequencyTestConfig(t)
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
		})
	}
}

func TestStage4CheckpointFrequencyProvenanceAndBound(t *testing.T) {
	t.Parallel()

	generated, err := config.Parse([]byte(
		"source:\n  type: postgres\ntarget:\n  type: postgres\nmigration:\n  target_mode: upsert\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStage4AdapterConfigurationSeams(generated); err != nil {
		t.Fatalf("generated default configuration rejected: %v", err)
	}
	frequency, err := stage4AdapterNetworkCheckpointFrequency(
		generated.Migration,
	)
	if err != nil || frequency != 0 {
		t.Fatalf("generated default frequency=%d err=%v, want immediate cadence", frequency, err)
	}

	explicitZero, err := config.Parse([]byte(
		"source:\n  type: postgres\ntarget:\n  type: postgres\nmigration:\n  target_mode: upsert\n  checkpoint_frequency: 0\n",
	))
	if err != nil {
		t.Fatal(err)
	}
	if !stage4CheckpointFrequencyExplicitlyRequested(
		explicitZero.Migration,
	) {
		t.Fatal("explicit zero checkpoint frequency lost requested provenance")
	}
	frequency, err = stage4AdapterNetworkCheckpointFrequency(
		explicitZero.Migration,
	)
	if err != nil || frequency != 0 {
		t.Fatalf("explicit zero frequency=%d err=%v", frequency, err)
	}

	tooLarge := stage4CheckpointFrequencyTestConfig(t)
	tooLarge.Migration.CheckpointFrequency =
		maximumNetworkCheckpointFrequency + 1
	if _, err := stage4AdapterNetworkCheckpointFrequency(
		tooLarge.Migration,
	); err == nil || !strings.Contains(err.Error(), "checkpoint_frequency") {
		t.Fatalf("unbounded frequency error = %v", err)
	}
}

func TestStage4DeferredNetworkAdmissionMapsCheckpointFrequencyToCorePlan(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "stage4-checkpoint-frequency"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	protected := false
	rawTarget := &recordingAdapterTarget{
		events:    &events,
		protected: &protected,
	}
	target := &stage4NetworkAdmissionTarget{
		recordingAdapterTarget: rawTarget,
		backend:                backend,
		runID:                  runID,
	}
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run:                    stage4LifecycleRunContext(t, backend, runID, false),
		},
		protected: &protected,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.CheckpointFrequency = 2
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
		t.Fatalf("prepare Stage 4: %v", err)
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
		t.Fatalf("admit Stage 4 network transfer: %v", err)
	}
	if !execution.deferred || execution.checkpointFrequency != 2 {
		t.Fatalf("deferred execution = %#v", execution)
	}
	tableExecution, err := execution.planTable(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("plan stable table: %v", err)
	}
	defer func() {
		if closeErr := tableExecution.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if tableExecution.corePlan.CheckpointFrequency != 2 {
		t.Fatalf("core checkpoint frequency = %d, want 2", tableExecution.corePlan.CheckpointFrequency)
	}
}

func TestStage4NetworkAdmissionCannotBypassCheckpointFrequencyRouteGate(
	t *testing.T,
) {
	t.Parallel()

	run := newNetworkStateTestRun(t, "sqlite", "stage4-checkpoint-route-gate")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.CheckpointFrequency = 2
	cfg.Migration.StrictConsistency = true
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
		"not yet composed with strict consistency",
	) {
		t.Fatalf("direct network admission error = %v", err)
	}
	if target.isolationCalls != 0 || len(target.snapshotWrites()) != 0 {
		t.Fatalf(
			"route-gate refusal opened/mutated target: isolation=%d writes=%#v",
			target.isolationCalls,
			target.snapshotWrites(),
		)
	}
}

func stage4CheckpointFrequencyTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.CheckpointFrequency = 2
	return cfg
}
