package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4DecisionCapturingObserver struct {
	stage4AdapterObserver
	reports []Stage4SchemaDecisionReport
	err     error
}

func (observer *stage4DecisionCapturingObserver) ObserveStage4SchemaDecisions(
	_ context.Context,
	report Stage4SchemaDecisionReport,
) error {
	*observer.recordingTableObserver.events = append(
		*observer.recordingTableObserver.events,
		"schema_decisions",
	)
	observer.reports = append(observer.reports, report)
	return observer.err
}

type stage4MissingDecisionObserver struct {
	recordingTableObserver
	run Stage4RunContext
}

func (observer stage4MissingDecisionObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func (observer stage4MissingDecisionObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutation()
}

func TestStage4SchemaDecisionsPublishBeforeTargetPlanning(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		current    func(schema.Table) schema.Table
		contract   config.SchemaContract
		wantAction SchemaContractAction
	}{
		{
			name: "report",
			current: func(previous schema.Table) schema.Table {
				current := cloneStage4RichTable(previous)
				current.Columns[1].Nullable = true
				return current
			},
			contract: config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractReport,
			},
			wantAction: SchemaContractReport,
		},
		{
			name: "discard value",
			current: func(previous schema.Table) schema.Table {
				current := cloneStage4RichTable(previous)
				current.Columns = append(
					current.Columns,
					schema.Column{
						Name:     "transient",
						Type:     "text",
						Nullable: true,
					},
				)
				return current
			},
			contract: config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractDiscardValue,
				DataType: config.SchemaContractReport,
			},
			wantAction: SchemaContractDiscardValue,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := make([]string, 0)
			previous := stage4AdapterTestTable()
			current := test.current(previous)
			backend := state.YAMLStore{
				Path: filepath.Join(t.TempDir(), "state.yaml"),
			}
			started := time.Now().Add(-2 * time.Minute).UTC()
			stage4AdapterInstallSuccessfulBaseline(
				t,
				backend,
				"stage4-decision-"+strings.ReplaceAll(test.name, " ", "-")+"-previous",
				started,
				previous,
				"drop_recreate",
			)
			runID := "stage4-decision-" +
				strings.ReplaceAll(test.name, " ", "-") +
				"-current"
			initializeStage4LifecycleRun(
				t,
				backend,
				runID,
				started.Add(90*time.Second),
			)
			source := &recordingAdapterSource{
				events: &events,
				table:  current,
			}
			target := &recordingAdapterTarget{events: &events}
			cfg := stage4AdapterTestConfig(
				t,
				"source-password-must-not-publish",
				"target-password-must-not-publish",
			)
			cfg.Migration.SchemaContract = &test.contract
			observer := &stage4DecisionCapturingObserver{
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

			if _, err := migrateWithAdapters(
				context.Background(),
				cfg,
				observer,
				source,
				target,
			); err != nil {
				t.Fatalf("migrateWithAdapters: %v", err)
			}
			assertStage4AdapterEventBefore(
				t,
				events,
				"schema_decisions",
				"target_plan",
			)
			if len(observer.reports) != 1 {
				t.Fatalf(
					"schema decision reports = %d, want 1",
					len(observer.reports),
				)
			}
			report := observer.reports[0]
			if report.RunID != runID ||
				report.Resume ||
				report.Baseline ||
				report.SourceEngine != "postgres" ||
				report.TargetEngine != "sqlite" ||
				report.TargetMode != "drop_recreate" ||
				len(report.GateTopologyHash) != 64 ||
				len(report.PreviousSchemaDigest) != 64 ||
				len(report.CurrentSchemaDigest) != 64 ||
				len(report.SuccessfulSchemaDigest) != 64 {
				t.Fatalf("incomplete schema decision report = %#v", report)
			}
			actionFound := false
			for _, decision := range report.Decisions {
				if decision.Action == test.wantAction {
					actionFound = true
				}
			}
			if !actionFound {
				t.Fatalf(
					"decision action %q absent from %#v",
					test.wantAction,
					report.Decisions,
				)
			}
			encoded, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(
				string(encoded),
				"password-must-not-publish",
			) {
				t.Fatalf("schema decision report leaked credentials: %s", encoded)
			}
		})
	}
}

func TestStage4SchemaDecisionSinkFailureStopsBeforePlanningAndMutation(
	t *testing.T,
) {
	t.Parallel()

	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	current := cloneStage4RichTable(previous)
	current.Columns[1].Nullable = true
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	stage4AdapterInstallSuccessfulBaseline(
		t,
		backend,
		"stage4-decision-sink-error-previous",
		started,
		previous,
		"drop_recreate",
	)
	runID := "stage4-decision-sink-error-current"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		started.Add(90*time.Second),
	)
	source := &recordingAdapterSource{events: &events, table: current}
	target := &recordingAdapterTarget{events: &events}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractReport,
		Columns:  config.SchemaContractReport,
		DataType: config.SchemaContractReport,
	}
	observer := &stage4DecisionCapturingObserver{
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
		err: errors.New("injected schema decision audit failure"),
	}

	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "before target planning") ||
		!strings.Contains(err.Error(), "injected schema decision audit failure") {
		t.Fatalf("schema decision sink error = %v", err)
	}
	assertStage4DecisionFailureStayedBeforeTarget(
		t,
		events,
		target,
	)
}

func TestStage4SchemaDriftRequiresDecisionSinkBeforeTargetPlanning(
	t *testing.T,
) {
	t.Parallel()

	events := make([]string, 0)
	previous := stage4AdapterTestTable()
	current := cloneStage4RichTable(previous)
	current.Columns[1].Nullable = true
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	started := time.Now().Add(-2 * time.Minute).UTC()
	stage4AdapterInstallSuccessfulBaseline(
		t,
		backend,
		"stage4-missing-decision-sink-previous",
		started,
		previous,
		"drop_recreate",
	)
	runID := "stage4-missing-decision-sink-current"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		started.Add(90*time.Second),
	)
	source := &recordingAdapterSource{events: &events, table: current}
	target := &recordingAdapterTarget{events: &events}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.SchemaContract = &config.SchemaContract{
		Tables:   config.SchemaContractReport,
		Columns:  config.SchemaContractReport,
		DataType: config.SchemaContractReport,
	}
	observer := stage4MissingDecisionObserver{
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
		cfg,
		observer,
		source,
		target,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "typed decision observer") {
		t.Fatalf("missing schema decision sink error = %v", err)
	}
	assertStage4DecisionFailureStayedBeforeTarget(
		t,
		events,
		target,
	)
}

func assertStage4DecisionFailureStayedBeforeTarget(
	t *testing.T,
	events []string,
	target *recordingAdapterTarget,
) {
	t.Helper()
	if stage4AdapterEventIndex(events, "target_plan") >= 0 ||
		stage4AdapterEventsContain(events, "before") ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 ||
		len(target.finalized) != 0 {
		t.Fatalf(
			"schema decision failure crossed target boundary: events=%v target=%#v",
			events,
			target,
		)
	}
}
