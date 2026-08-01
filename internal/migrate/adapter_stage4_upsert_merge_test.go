package migrate

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4UpsertMergeAdmissionFailsClosedBeforeWorkOrWrite(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "upsert-merge-missing-proof")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.UpsertMergeSize = 2

	_, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err == nil || !strings.Contains(err.Error(), "upsert merge") {
		t.Fatalf("admission error = %v, want missing merge proof", err)
	}
	if target.isolationCalls != 1 || len(target.snapshotWrites()) != 0 ||
		source.readCount() != 0 {
		t.Fatalf(
			"admission reached stateful work: isolation=%d writes=%#v reads=%d",
			target.isolationCalls,
			target.snapshotWrites(),
			source.readCount(),
		)
	}
	tasks, ranges, listErr := run.Backend.ListWork(run.RunID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(tasks) != 0 || len(ranges) != 0 {
		t.Fatalf("missing merge proof persisted work: tasks=%#v ranges=%#v", tasks, ranges)
	}
}

func TestStage4UpsertMergeRequiresComposedStage4Runner(t *testing.T) {
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.UpsertMergeSize = 2

	_, err := migrateWithAdaptersAdmission(
		context.Background(),
		cfg,
		nil,
		nil,
		nil,
		"upsert",
		stage4AdapterAdmission{},
	)
	if err == nil || !strings.Contains(err.Error(), "composed Stage 4") {
		t.Fatalf("fallback admission error = %v, want composed-runner refusal", err)
	}
	_, err = resumeWithAdaptersAdmission(
		context.Background(),
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		"upsert",
		stage4AdapterAdmission{},
	)
	if err == nil || !strings.Contains(err.Error(), "composed Stage 4") {
		t.Fatalf("fallback resume error = %v, want composed-runner refusal", err)
	}
}

func TestStage4UpsertMergeRoutesSQLiteCompatibilityThroughComposedRunner(
	t *testing.T,
) {
	route := resolvedAdapterRoute{
		source: sourceRole{engine: "sqlite"},
		target: targetRole{engine: "sqlite"},
	}
	legacy := stage4NetworkRunnerConfig()
	if stage4SQLiteCompatibilityRouteRequiresComposition(legacy, route) {
		t.Fatal("derived upsert merge default unexpectedly bypassed SQLite compatibility route")
	}
	cfg := legacy
	cfg.Migration.UpsertMergeSize = 2
	if !stage4SQLiteCompatibilityRouteRequiresComposition(cfg, route) {
		t.Fatal("explicit upsert merge did not route SQLite compatibility through Stage 4")
	}
}

func TestStage4UpsertMergeRejectsLegacySQLitePipelineBeforeEndpointOpen(
	t *testing.T,
) {
	cfg := config.Config{
		Migration: config.Migration{UpsertMergeSize: 2},
	}
	if _, err := SQLiteToSQLiteWithObserver(
		context.Background(),
		cfg,
		nil,
	); err == nil || !strings.Contains(err.Error(), "composed Stage 4") {
		t.Fatalf("legacy SQLite cap error = %v, want pre-open composed-runner refusal", err)
	}
	if _, err := SQLiteToSQLiteResumeWithProgress(
		context.Background(),
		cfg,
		nil,
		nil,
		nil,
	); err == nil || !strings.Contains(err.Error(), "composed Stage 4") {
		t.Fatalf("legacy SQLite resume cap error = %v, want pre-open composed-runner refusal", err)
	}
}

func TestStage4UpsertMergeAdmissionIntersectsNativeAndResourceCaps(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "upsert-merge-intersection")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(8, 1)
	target := &stage4UpsertMergeRunnerTarget{
		stage4NetworkRunnerTestTarget: &stage4NetworkRunnerTestTarget{
			engine: "postgres",
		},
		maximumRows: 3,
	}
	resources := stage4NetworkRunnerResources()
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.UpsertMergeSize = 6

	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatalf("admit capped upsert route: %v", err)
	}
	if target.preflightCalls != 1 || execution.plan.UpsertMergeRows != 3 {
		t.Fatalf(
			"preflight=%d merge rows=%d, want 1/3",
			target.preflightCalls,
			execution.plan.UpsertMergeRows,
		)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		networkStateTestProtector{},
		execution,
	); err != nil {
		t.Fatalf("bind capped route: %v", err)
	}
	if _, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		networkStateTestProtector{},
		execution,
	); err != nil {
		t.Fatalf("run capped route: %v", err)
	}
	writes := target.snapshotWrites()
	if got := stage4UpsertMergeWriteSizes(writes); !reflect.DeepEqual(
		got,
		[]int{3, 3, 2},
	) {
		t.Fatalf("native upsert write sizes = %#v, want [3 3 2]", got)
	}
}

func TestStage4UpsertMergeAdmissionClampsToResourceLimit(t *testing.T) {
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.UpsertMergeSize = 6
	target := &stage4UpsertMergeRunnerTarget{
		stage4NetworkRunnerTestTarget: &stage4NetworkRunnerTestTarget{
			engine: "postgres",
		},
		maximumRows: 8,
	}
	rows, err := stage4AdapterExplicitUpsertMergeRows(
		context.Background(),
		cfg.Migration,
		"upsert",
		target,
		4,
	)
	if err != nil || rows != 4 || target.preflightCalls != 1 {
		t.Fatalf(
			"resource-capped admission rows=%d preflight=%d err=%v, want 4/1/nil",
			rows,
			target.preflightCalls,
			err,
		)
	}
}

func TestResumableNetworkTransferUpsertMergePreservesSourcePage(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	plan.Resources.TargetMode = "upsert"
	plan.ReplayMode = NetworkReplayIdempotentUpsert
	plan.UpsertMergeRows = 2
	var mu sync.Mutex
	var reads []NetworkReadRequest
	var issued []NetworkIssuedChunk
	var writes []NetworkWriteRequest
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				_ context.Context,
				request NetworkReadRequest,
			) (NetworkReadPage, error) {
				mu.Lock()
				reads = append(reads, cloneNetworkReadRequest(request))
				mu.Unlock()
				return networkTestPage(
					[][]any{{1}, {2}, {3}, {4}},
					[]int64{16, 16, 16, 16},
					"frontier-4",
					"digest-4",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				mu.Lock()
				writes = append(writes, cloneNetworkWriteRequest(request))
				mu.Unlock()
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				_ context.Context,
				value NetworkIssuedChunk,
			) error {
				mu.Lock()
				issued = append(issued, cloneNetworkIssuedChunk(value))
				mu.Unlock()
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 || result.CompletedRanges != 1 ||
		len(reads) != 1 || reads[0].MaxRows != 4 ||
		len(issued) != 1 || issued[0].Rows != 4 {
		t.Fatalf("source page identity changed: result=%#v reads=%#v issued=%#v", result, reads, issued)
	}
	if got := stage4UpsertMergeNetworkWriteFacts(writes); !reflect.DeepEqual(
		got,
		[][3]int{{0, 0, 2}, {0, 2, 2}},
	) {
		t.Fatalf("write fragments = %#v, want [sequence/offset/rows 0/0/2 0/2/2]", got)
	}
}

func TestResumableNetworkTransferUpsertMergeReplaysOnlyUnacknowledgedSuffix(
	t *testing.T,
) {
	t.Parallel()

	plan := networkTransferTestPlan(1)
	plan.Resources.TargetMode = "upsert"
	plan.ReplayMode = NetworkReplayIdempotentUpsert
	plan.UpsertMergeRows = 2
	var firstMu sync.Mutex
	var issued []NetworkIssuedChunk
	var checkpoints []NetworkRangeCheckpoint
	var firstWrites []NetworkWriteRequest
	_, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				return networkTestPage(
					[][]any{{1}, {2}, {3}, {4}},
					[]int64{16, 16, 16, 16},
					"frontier-4",
					"digest-4",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				firstMu.Lock()
				firstWrites = append(firstWrites, cloneNetworkWriteRequest(request))
				call := len(firstWrites)
				firstMu.Unlock()
				if call == 2 {
					return WriteReceipt{
						Certainty:     CommitNotCommitted,
						AttemptOffset: request.AttemptOffset,
						AttemptedRows: int64(len(request.Rows)),
					}, NewTransferError(ErrorClassPermanent, errors.New("injected write failure"))
				}
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				_ context.Context,
				value NetworkIssuedChunk,
			) error {
				firstMu.Lock()
				issued = append(issued, cloneNetworkIssuedChunk(value))
				firstMu.Unlock()
				return nil
			},
			Checkpoint: func(
				_ context.Context,
				value NetworkRangeCheckpoint,
			) error {
				firstMu.Lock()
				checkpoints = append(checkpoints, cloneNetworkCheckpoint(value))
				firstMu.Unlock()
				return nil
			},
		},
	)
	if err == nil || len(issued) != 1 || len(firstWrites) != 2 ||
		len(checkpoints) != 1 ||
		checkpoints[0].Frontier.SequenceOffset != 2 ||
		checkpoints[0].Frontier.Rows != 2 {
		t.Fatalf(
			"first failed run did not retain exactly one durable prefix: err=%v issued=%#v writes=%#v checkpoints=%#v",
			err,
			issued,
			firstWrites,
			checkpoints,
		)
	}

	resume := plan
	// The cap is a runtime write boundary, not part of the issued-page
	// identity. A resumed route can adopt a new, narrower explicit cap while
	// replaying the exact original source page and its durable suffix.
	resume.UpsertMergeRows = 1
	resume.Restores = []NetworkRangeRestore{{
		RangeIndex:     0,
		TopologyHash:   plan.Ranges[0].TopologyHash,
		SequenceOffset: 2,
		Issued:         []NetworkIssuedChunk{issued[0]},
	}}
	var resumeReads []NetworkReadRequest
	var resumeWrites []NetworkWriteRequest
	var recorded atomic.Int32
	result, resumeErr := RunResumableNetworkTransfer(
		context.Background(),
		resume,
		NetworkTransferCallbacks{
			ReadPage: func(
				_ context.Context,
				request NetworkReadRequest,
			) (NetworkReadPage, error) {
				resumeReads = append(resumeReads, cloneNetworkReadRequest(request))
				if request.ReplayExpected == nil ||
					request.ReplayExpected.Fingerprint != issued[0].Fingerprint ||
					request.MaxRows != issued[0].Rows {
					return NetworkReadPage{}, errors.New("replay identity changed")
				}
				return networkTestPage(
					[][]any{{1}, {2}, {3}, {4}},
					[]int64{16, 16, 16, 16},
					"frontier-4",
					"digest-4",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				resumeWrites = append(resumeWrites, cloneNetworkWriteRequest(request))
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(
				context.Context,
				NetworkIssuedChunk,
			) error {
				recorded.Add(1)
				return nil
			},
			Checkpoint: func(
				context.Context,
				NetworkRangeCheckpoint,
			) error {
				return nil
			},
		},
	)
	if resumeErr != nil {
		t.Fatalf("resume capped page: %v", resumeErr)
	}
	if result.Rows != 4 || result.CompletedRanges != 1 ||
		len(resumeReads) != 1 || recorded.Load() != 0 ||
		!reflect.DeepEqual(
			stage4UpsertMergeNetworkWriteFacts(resumeWrites),
			[][3]int{{0, 2, 1}, {0, 3, 1}},
		) {
		t.Fatalf(
			"resume did not preserve the original page/suffix: result=%#v reads=%#v writes=%#v recorded=%d",
			result,
			resumeReads,
			resumeWrites,
			recorded.Load(),
		)
	}
}

func TestStage4UpsertMergeConcurrentRangesRemainBounded(t *testing.T) {
	t.Parallel()

	plan := networkTransferTestPlan(4)
	plan.Resources.TargetMode = "upsert"
	plan.ReplayMode = NetworkReplayIdempotentUpsert
	plan.UpsertMergeRows = 2
	var writes atomic.Int32
	var maximum atomic.Int32
	result, err := RunResumableNetworkTransfer(
		context.Background(),
		plan,
		NetworkTransferCallbacks{
			ReadPage: func(
				_ context.Context,
				request NetworkReadRequest,
			) (NetworkReadPage, error) {
				if request.MaxRows != 4 || request.ReplayExpected != nil {
					return NetworkReadPage{}, errors.New("source page cap changed")
				}
				return networkTestPage(
					[][]any{{1}, {2}, {3}, {4}},
					[]int64{16, 16, 16, 16},
					"frontier",
					"digest",
					true,
				), nil
			},
			WritePage: func(
				_ context.Context,
				request NetworkWriteRequest,
			) (WriteReceipt, error) {
				if len(request.Rows) > 2 {
					return WriteReceipt{}, errors.New("native cap exceeded")
				}
				writes.Add(1)
				updateNetworkTestMaximum(&maximum, int32(len(request.Rows)))
				return WriteReceipt{
					Certainty:     CommitDurable,
					AttemptOffset: request.AttemptOffset,
					AttemptedRows: int64(len(request.Rows)),
					CommittedRows: int64(len(request.Rows)),
				}, nil
			},
			RecordIssued: func(context.Context, NetworkIssuedChunk) error { return nil },
			Checkpoint:   func(context.Context, NetworkRangeCheckpoint) error { return nil },
		},
	)
	if err != nil || result.Rows != 16 || result.CompletedRanges != 4 ||
		writes.Load() != 8 || maximum.Load() != 2 {
		t.Fatalf(
			"concurrent cap result=%#v err=%v writes=%d maximum=%d",
			result,
			err,
			writes.Load(),
			maximum.Load(),
		)
	}
}

func TestStage4UpsertMergeNativeTargetsExposeBoundedWriters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target adapterStage4UpsertMergeTarget
		engine string
	}{
		{
			name: "postgres",
			target: &postgresTargetAdapter{
				batchWriter: newPostgresNativeWriter(nil),
			},
			engine: "postgres",
		},
		{
			name: "mysql",
			target: &mysqlTargetAdapter{
				batchWriter: newMySQLNativeWriter(nil),
			},
			engine: "mysql",
		},
		{
			name: "mssql",
			target: &sqlServerTargetAdapter{
				batchWriter: newSQLServerNativeWriter(nil),
			},
			engine: "mssql",
		},
		{
			name: "sqlite",
			target: &sqliteTargetAdapter{
				stage4BatchWriter: newSQLiteStage4NetworkWriter(nil),
			},
			engine: "sqlite",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.target.Engine() != test.engine {
				t.Fatalf("target engine = %q, want %q", test.target.Engine(), test.engine)
			}
			maximum, err := test.target.PreflightStage4UpsertMerge(
				context.Background(),
			)
			if err != nil || maximum != config.MaxTransferChunkRows {
				t.Fatalf("native merge preflight = %d, %v", maximum, err)
			}
		})
	}
}

func TestStage4IncrementalUpsertMergeSplitsNativeWritesBeforeOneAcknowledgement(
	t *testing.T,
) {
	events := make([]string, 0)
	first := time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC)
	second := first.Add(time.Second)
	table := schema.Table{
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
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	source := &stage4IncrementalTestSource{
		events: &events,
		table:  table,
		rows: []stage4IncrementalTestRow{
			{id: 1, payload: "first", updated: &first},
			{id: 2, payload: "second", updated: &second},
		},
	}
	target := &stage4UpsertMergeIncrementalTarget{
		stage4IncrementalTestTarget: &stage4IncrementalTestTarget{
			events: &events,
		},
		maximumRows: 2,
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "upsert-merge-incremental"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run:     stage4LifecycleRunContext(t, backend, runID, false),
	}
	cfg := config.Config{
		Source: config.Endpoint{
			Type: "postgres", Host: "source.example", Port: 5432, Database: "source",
		},
		Target: config.Endpoint{
			Type: "postgres", Host: "target.example", Port: 5432, Database: "target",
		},
		Migration: config.Migration{
			TargetMode:         "upsert",
			UpsertMergeSize:    1,
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode: config.ValidationCountOnly,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("run capped incremental route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) ||
		target.preflightCalls != 1 ||
		stage4UpsertMergeEventCount(events, "target_write") != 2 ||
		stage4UpsertMergeEventCount(events, "target_validate") != 1 {
		t.Fatalf(
			"incremental cap result=%#v preflight=%d events=%#v",
			result,
			target.preflightCalls,
			events,
		)
	}
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var window *state.RangeState
	for index := range ranges {
		if ranges[index].Task.Type == stage4AdapterNetworkTaskType &&
			ranges[index].Task.Schema == "public" &&
			ranges[index].Task.Table == "items" &&
			ranges[index].ID == stage4AdapterIncrementalRangeID {
			window = &ranges[index]
			break
		}
	}
	if window == nil || window.RowsDone != 2 ||
		window.NextSequence != 1 || window.SequenceOffset != 0 ||
		window.Status != "completed" {
		t.Fatalf("incremental durable acknowledgement = tasks=%#v ranges=%#v", tasks, ranges)
	}
}

type stage4UpsertMergeRunnerTarget struct {
	*stage4NetworkRunnerTestTarget
	maximumRows    int
	preflightCalls int
}

type stage4UpsertMergeIncrementalTarget struct {
	*stage4IncrementalTestTarget
	maximumRows    int
	preflightCalls int
}

func (target *stage4UpsertMergeIncrementalTarget) PreflightStage4UpsertMerge(
	context.Context,
) (int, error) {
	target.preflightCalls++
	return target.maximumRows, nil
}

func (target *stage4UpsertMergeRunnerTarget) PreflightStage4UpsertMerge(
	context.Context,
) (int, error) {
	target.preflightCalls++
	return target.maximumRows, nil
}

func stage4UpsertMergeWriteSizes(
	writes []stage4NetworkRunnerWrite,
) []int {
	result := make([]int, len(writes))
	for index, write := range writes {
		result[index] = len(write.rows)
	}
	return result
}

func stage4UpsertMergeNetworkWriteFacts(
	writes []NetworkWriteRequest,
) [][3]int {
	result := make([][3]int, len(writes))
	for index, write := range writes {
		result[index] = [3]int{
			int(write.Sequence),
			int(write.AttemptOffset),
			len(write.Rows),
		}
	}
	return result
}

func stage4UpsertMergeEventCount(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

var _ adapterStage4UpsertMergeTarget = (*postgresTargetAdapter)(nil)
var _ adapterStage4UpsertMergeTarget = (*mysqlTargetAdapter)(nil)
var _ adapterStage4UpsertMergeTarget = (*sqlServerTargetAdapter)(nil)
var _ adapterStage4UpsertMergeTarget = (*sqliteTargetAdapter)(nil)
var _ adapterStage4UpsertMergeTarget = (*stage4UpsertMergeRunnerTarget)(nil)
var _ adapterStage4UpsertMergeTarget = (*stage4UpsertMergeIncrementalTarget)(nil)
