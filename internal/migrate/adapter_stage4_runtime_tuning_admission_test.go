package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4IncrementalRuntimeTuningAdmissionHonorsProvenance(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		migration  string
		wantErr    string
		wantReport bool
	}{
		{
			name:       "generated defaults remain compatible and disclosed inactive",
			wantReport: true,
		},
		{
			name:      "explicit enable refuses before incremental endpoints",
			migration: "  runtime_tuning: true\n",
			wantErr:   "not yet composed with date-based incremental",
		},
		{
			name:      "explicit interval with generated enable is operator intent",
			migration: "  runtime_tuning_interval: 7s\n",
			wantErr:   "not yet composed with date-based incremental",
		},
		{
			name:      "explicit disable without interval remains supported",
			migration: "  runtime_tuning: false\n",
		},
		{
			name:      "explicit interval remains refused when enable is disabled",
			migration: "  runtime_tuning: false\n  runtime_tuning_interval: 7s\n",
			wantErr:   "not yet composed with date-based incremental",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := parseStage4IncrementalRuntimeTuningConfig(
				t,
				test.migration,
			)
			err := requireStage4AdapterConfigurationSeams(cfg)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("requireStage4AdapterConfigurationSeams: %v", err)
			}

			report := stage4GeneratedIncrementalRuntimeTuningReport(
				cfg.Migration,
			)
			if test.wantReport {
				if report == nil || report.Enabled ||
					!strings.Contains(report.Reason, "generated runtime_tuning default") {
					t.Fatalf("generated report = %#v", report)
				}
				return
			}
			if report != nil {
				t.Fatalf("unexpected inactive report = %#v", report)
			}
		})
	}
}

func TestStage4DeferredRuntimeTuningChangesProductionChunkWritesAndDisclosesHistory(
	t *testing.T,
) {
	events := make([]string, 0)
	readSizes := make([]int, 0)
	rows := make([]string, 40)
	for index := range rows {
		rows[index] = fmt.Sprintf("payload-%02d", index+1)
	}
	source := &recordingAdapterSource{
		events:    &events,
		table:     stage4AdapterTestTable(),
		rows:      rows,
		readSizes: &readSizes,
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "stage4-runtime-tuning-deferred"
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
	target := &stage4RuntimeTuningProtocolLimitTarget{
		stage4NetworkAdmissionTarget: &stage4NetworkAdmissionTarget{
			recordingAdapterTarget: rawTarget,
			backend:                backend,
			runID:                  runID,
		},
		protocolFailures: 1,
	}
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run: stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			),
		},
		protected: &protected,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	cfg.Migration.ChunkSize = 8
	cfg.Migration.ReadAhead = 2
	cfg.Migration.MaxRetries = 1
	cfg.Migration.RuntimeTuning = true
	cfg.Migration.RuntimeTuningInterval = time.Hour

	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result.Tables != 1 || result.Rows != len(rows) || !result.Validated {
		t.Fatalf("result = %#v", result)
	}
	report := result.RuntimeTuning
	if report == nil || !report.Enabled || len(report.Tables) != 1 {
		t.Fatalf("runtime tuning report = %#v", report)
	}
	table := report.Tables[0]
	if table.Schema != "public" || table.Table != "items" ||
		table.Snapshot.Interval != time.Hour.String() ||
		table.Snapshot.Effective.ChunkRows.Value >= 8 ||
		table.Snapshot.Effective.ChunkRows.LiveProvenance !=
			string(RuntimeTuningSafetyReduction) {
		t.Fatalf("runtime tuning table report = %#v", table)
	}
	if !runtimeTuningReportHistoryHasReason(
		table.Adjustments,
		RuntimeReasonProtocolWriteError,
	) {
		t.Fatalf("runtime tuning adjustments = %#v", table.Adjustments)
	}
	if !containsRuntimeChunkSize(readSizes, 8) ||
		!hasRuntimeChunkSizeBelow(readSizes, 8) {
		t.Fatalf(
			"runtime controller did not change deferred source page limits: %v",
			readSizes,
		)
	}
}

func TestStage4DeferredRuntimeTuningFailureRetainsSafetyHistory(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
		rows:   []string{"one", "two", "three", "four"},
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "stage4-runtime-tuning-failed-transfer"
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
	target := &stage4RuntimeTuningProtocolLimitTarget{
		stage4NetworkAdmissionTarget: &stage4NetworkAdmissionTarget{
			recordingAdapterTarget: rawTarget,
			backend:                backend,
			runID:                  runID,
		},
		protocolFailures: 1,
	}
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run: stage4LifecycleRunContext(
				t,
				backend,
				runID,
				false,
			),
		},
		protected: &protected,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	cfg.Migration.ChunkSize = 8
	cfg.Migration.MaxRetries = 0
	cfg.Migration.RuntimeTuning = true
	cfg.Migration.RuntimeTuningInterval = time.Hour

	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "protocol limit") {
		t.Fatalf("transfer error = %v", err)
	}
	if result.Tables != 0 || result.Rows != 0 || result.Validated {
		t.Fatalf("failed result = %#v", result)
	}
	if result.RuntimeTuning == nil ||
		len(result.RuntimeTuning.Tables) != 1 ||
		!runtimeTuningReportHistoryHasReason(
			result.RuntimeTuning.Tables[0].Adjustments,
			RuntimeReasonProtocolWriteError,
		) {
		t.Fatalf("failed runtime tuning report = %#v", result.RuntimeTuning)
	}
	if result.RuntimeTuning.Tables[0].Snapshot.Effective.ChunkRows.Value >= 8 {
		t.Fatalf(
			"failed runtime tuning effective chunk rows = %#v",
			result.RuntimeTuning.Tables[0].Snapshot.Effective,
		)
	}
}

func TestStage4RuntimeTuningRejectsPreboundCompatibilityWavesBeforeMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "stage4-runtime-tuning-prebound"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.RuntimeTuning = true
	cfg.Migration.RuntimeTuningInterval = time.Second

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
		t.Fatalf("prepare Stage 4: %v", err)
	}
	prepared = bindStage4AdapterTestStableNetworkWork(
		t,
		context.Background(),
		cfg,
		source,
		prepared,
	)
	if _, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		prepared,
		nil,
	); err == nil ||
		!strings.Contains(err.Error(), "prebound dependency waves") {
		t.Fatalf("prebound runtime tuning admission error = %v", err)
	}
	for _, event := range []string{
		"before_tables:items",
		"target_prepare",
		"target_write",
		"target_finalize",
	} {
		if stage4AdapterEventIndex(events, event) >= 0 {
			t.Fatalf("prebound admission crossed %s: %v", event, events)
		}
	}
}

type stage4RuntimeTuningProtocolLimitTarget struct {
	*stage4NetworkAdmissionTarget
	protocolFailures int
}

func (target *stage4RuntimeTuningProtocolLimitTarget) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	if target.protocolFailures > 0 {
		target.protocolFailures--
		return WriteReceipt{
			Certainty:     CommitNotCommitted,
			AttemptedRows: int64(len(rows)),
		}, &NetworkWriteProtocolLimitError{}
	}
	return target.recordingAdapterTarget.WriteStage4NetworkBatch(
		ctx,
		table,
		columns,
		rows,
	)
}

func runtimeTuningReportHistoryHasReason(
	history []RuntimeTuningAdjustmentReport,
	want RuntimeTuningReason,
) bool {
	for _, decision := range history {
		for _, reason := range decision.Reasons {
			if reason == string(want) {
				return true
			}
		}
	}
	return false
}

func containsRuntimeChunkSize(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasRuntimeChunkSizeBelow(values []int, ceiling int) bool {
	for _, value := range values {
		if value > 0 && value < ceiling {
			return true
		}
	}
	return false
}

func parseStage4IncrementalRuntimeTuningConfig(
	t *testing.T,
	migrationExtra string,
) config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(
		"source:\n" +
			"  type: postgres\n" +
			"target:\n" +
			"  type: postgres\n" +
			"migration:\n" +
			"  target_mode: upsert\n" +
			"  date_updated_columns: [updated_at]\n" +
			migrationExtra,
	))
	if err != nil {
		t.Fatalf("parse configuration: %v", err)
	}
	return cfg
}
