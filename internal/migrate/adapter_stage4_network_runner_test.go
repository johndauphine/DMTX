package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4AdapterNetworkRunnerMapsGlobalRangesAndDurableTotals(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-map")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{2, 1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(2, 100)
	source.rows[1] = stage4NetworkRunnerRows(3, 200)
	source.rows[2] = stage4NetworkRunnerRows(4, 300)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	resources.ConnectionLimit.Value = 2
	resources.Workers.Value = 2
	resources.Readers.Value = 1
	resources.Writers.Value = 1

	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatalf("admit Stage 4 network transfer: %v", err)
	}
	if got := []string{
		execution.plan.Ranges[0].TableName,
		execution.plan.Ranges[1].TableName,
		execution.plan.Ranges[2].TableName,
	}; !reflect.DeepEqual(got, []string{"table_0", "table_0", "table_1"}) {
		t.Fatalf("global range tables = %#v", got)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatalf("bind Stage 4 network restores: %v", err)
	}
	totals, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	)
	if err != nil {
		t.Fatalf("run Stage 4 network transfer: %v", err)
	}
	if !reflect.DeepEqual(totals, []int{5, 4}) {
		t.Fatalf("durable table totals = %#v, want [5 4]", totals)
	}
	writes := target.snapshotWrites()
	if len(writes) != 3 {
		t.Fatalf("target writes = %#v", writes)
	}
	byTable := make(map[string]int)
	for _, write := range writes {
		if write.mode != "upsert" {
			t.Fatalf("target mode = %q", write.mode)
		}
		byTable[write.table] += len(write.rows)
	}
	if !reflect.DeepEqual(byTable, map[string]int{
		"table_0": 5,
		"table_1": 4,
	}) {
		t.Fatalf("target rows by table = %#v", byTable)
	}
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Status != "running" {
			t.Fatalf("network task completed before AfterTable: %#v", task)
		}
	}
	for _, workRange := range ranges {
		if workRange.Status != "completed" {
			t.Fatalf("network range is not complete: %#v", workRange)
		}
	}
}

func TestStage4AdapterNetworkWriterRequiresIdempotentModeAndRebasesReceipt(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-rebase")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := NetworkWriteRequest{
		Range:         execution.plan.Ranges[0],
		Sequence:      9,
		AttemptOffset: 7,
		Mode:          NetworkWriteIdempotentUpsert,
		Rows:          stage4NetworkRunnerRows(2, 900),
	}
	receipt, err := writeStage4AdapterNetworkPage(
		context.Background(),
		target,
		execution.ranges,
		execution.plan.ReplayMode,
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptOffset != 7 ||
		receipt.AttemptedRows != 2 ||
		receipt.CommittedRows != 2 {
		t.Fatalf("rebased receipt = %#v", receipt)
	}
	before := len(target.snapshotWrites())
	request.Mode = NetworkWriteFreshInsert
	if _, err := writeStage4AdapterNetworkPage(
		context.Background(),
		target,
		execution.ranges,
		execution.plan.ReplayMode,
		request,
	); err == nil || len(target.snapshotWrites()) != before {
		t.Fatalf("fresh mode error=%v writes=%#v", err, target.snapshotWrites())
	}
}

func TestStage4AdapterNetworkRebuildUsesDuplicateSafeWriterForFreshAndReplay(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-rebuild")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	prepared.mode = "drop_recreate"
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "sqlite"}
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.TargetMode = "drop_recreate"
	resources := stage4NetworkRunnerResources()
	resources.TargetMode = "drop_recreate"

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
		t.Fatalf("admit rebuild network transfer: %v", err)
	}
	if execution.plan.ReplayMode != NetworkReplayDuplicateSafeInsertOnly {
		t.Fatalf("rebuild replay mode = %q", execution.plan.ReplayMode)
	}
	request := NetworkWriteRequest{
		Range: execution.plan.Ranges[0],
		Rows:  stage4NetworkRunnerRows(1, 100),
	}
	for _, mode := range []NetworkWriteMode{
		NetworkWriteFreshInsert,
		NetworkWriteDuplicateSafeInsertOnly,
	} {
		request.Mode = mode
		if _, err := writeStage4AdapterNetworkPage(
			context.Background(),
			target,
			execution.ranges,
			execution.plan.ReplayMode,
			request,
		); err != nil {
			t.Fatalf("write rebuild mode %q: %v", mode, err)
		}
	}
	writes := target.snapshotWrites()
	if len(writes) != 2 {
		t.Fatalf("rebuild writes = %#v", writes)
	}
	if writes[0].mode != string(NetworkWriteFreshInsert) ||
		writes[1].mode != string(NetworkWriteDuplicateSafeInsertOnly) {
		t.Fatalf("rebuild writer modes = %#v", writes)
	}
}

func TestStage4AdapterNetworkRebuildFailsClosedWithoutDuplicateSafeWriter(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-rebuild-no-writer")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	prepared.mode = "drop_recreate"
	cfg := stage4NetworkRunnerConfig()
	cfg.Migration.TargetMode = "drop_recreate"
	resources := stage4NetworkRunnerResources()
	resources.TargetMode = "drop_recreate"
	_, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		networkStateTestProtector{},
		newStage4NetworkRunnerTestSource("postgres"),
		&stage4NetworkRunnerNoReplayTarget{engine: "sqlite"},
		prepared,
		&resources,
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate-safe rebuild") {
		t.Fatalf("missing rebuild writer error = %v", err)
	}
}

func TestStage4AdapterNetworkAdmissionFreezesCallbackInputs(t *testing.T) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-freeze")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}

	prepared.plans[0].source.Columns[0].Name = "changed_source"
	prepared.plans[0].target.Columns[0].Name = "changed_target"
	prepared.plans[0].columns[0] = "changed_column"
	prepared.work[0].pagination.Keys[0].Name = "changed_key"
	(*prepared.work[0].pagination.Ranges[0].Upper)[0].Encoded = "99"
	prepared.work[0].ranges[0].Upper[0].Encoded = "99"

	frozen := execution.ranges[0]
	if frozen.plan.source.Columns[0].Name != "id" ||
		frozen.plan.target.Columns[0].Name != "id" ||
		frozen.plan.columns[0] != "id" ||
		frozen.work.pagination.Keys[0].Name != "id" ||
		(*frozen.rangePlan.Upper)[0].Encoded != "10" ||
		frozen.work.ranges[0].Upper[0].Encoded != "10" ||
		frozen.durable.Initial.Upper[0].Encoded != "10" {
		t.Fatalf("admitted callback inputs changed with prepared plan: %#v", frozen)
	}
}

func TestStage4AdapterNetworkCompletedRestorePerformsZeroIO(t *testing.T) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-complete")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(2, 100)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	stage4NetworkRunnerSeedCheckpoint(
		t,
		execution.coordinator,
		2,
		0,
		true,
	)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatal(err)
	}
	totals, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(totals, []int{2}) ||
		source.readCount() != 0 ||
		len(target.snapshotWrites()) != 0 {
		t.Fatalf(
			"totals=%#v source reads=%d writes=%#v",
			totals,
			source.readCount(),
			target.snapshotWrites(),
		)
	}
}

func TestValidateCompletedStage4AdapterRangeStrategyEvidence(
	t *testing.T,
) {
	rowNumberItem := stage4AdapterWork{
		pagination: PaginationPlan{
			Strategy: PaginationRowNumber,
			Keys: []KeySpec{{
				Name: "id",
			}},
			Ranges: []PaginationRange{{
				ID:       0,
				FirstRow: 1,
				LastRow:  3,
			}},
		},
	}
	emptyItem := stage4AdapterWork{
		pagination: PaginationPlan{
			Strategy: PaginationIntegerKeyset,
			Keys: []KeySpec{{
				Name: "id",
				Kind: KeyInteger,
			}},
			Ranges: []PaginationRange{{
				ID:    0,
				Empty: true,
			}},
		},
	}
	for _, testCase := range []struct {
		name      string
		task      state.WorkTask
		item      stage4AdapterWork
		workRange state.RangeState
		restore   NetworkRangeRestore
		wantError bool
	}{
		{
			name: "exact row-number interval",
			item: rowNumberItem,
			workRange: state.RangeState{
				RowsDone:      3,
				Frontier:      state.TypedTuple{state.Int64Value(3)},
				FrontierValid: true,
			},
		},
		{
			name: "row-number short count",
			item: rowNumberItem,
			workRange: state.RangeState{
				RowsDone:      2,
				Frontier:      state.TypedTuple{state.Int64Value(3)},
				FrontierValid: true,
			},
			wantError: true,
		},
		{
			name: "row-number wrong frontier",
			item: rowNumberItem,
			workRange: state.RangeState{
				RowsDone:      3,
				Frontier:      state.TypedTuple{state.Int64Value(2)},
				FrontierValid: true,
			},
			wantError: true,
		},
		{
			name: "empty range with progress",
			task: state.WorkTask{
				Attempts: 1,
			},
			item: emptyItem,
			workRange: state.RangeState{
				RowsDone:      1,
				NextSequence:  1,
				Frontier:      state.TypedTuple{state.Int64Value(1)},
				FrontierValid: true,
			},
			restore: NetworkRangeRestore{
				Frontier: []byte("progress"),
			},
			wantError: true,
		},
		{
			name: "exact empty range",
			item: emptyItem,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateCompletedStage4AdapterRange(
				testCase.task,
				testCase.item,
				0,
				testCase.workRange,
				testCase.restore,
			)
			if testCase.wantError && err == nil {
				t.Fatal("unsafe completed strategy evidence admitted")
			}
			if !testCase.wantError && err != nil {
				t.Fatalf(
					"valid completed strategy evidence rejected: %v",
					err,
				)
			}
		})
	}
}

func TestStage4AdapterNetworkNonStartedCallDoesNotConsumeExecution(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-not-started")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(1, 100)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runStage4AdapterNetworkTransfer(
		cancelled,
		observer,
		execution,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled call error = %v", err)
	}
	if _, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerPlainObserver{},
		execution,
	); err == nil || !strings.Contains(err.Error(), "mutation protector") {
		t.Fatalf("unprotected call error = %v", err)
	}
	execution.mu.Lock()
	started := execution.started
	execution.mu.Unlock()
	if started {
		t.Fatal("non-started call consumed the network execution")
	}
	if _, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatalf("valid call after non-started failures: %v", err)
	}
}

func TestStage4AdapterNetworkPartialIssuedChunkReplaysOnlySuffix(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-prefix")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(3, 100)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	stage4NetworkRunnerSeedCheckpoint(
		t,
		execution.coordinator,
		3,
		1,
		false,
	)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatal(err)
	}
	totals, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	)
	if err != nil {
		t.Fatal(err)
	}
	writes := target.snapshotWrites()
	if !reflect.DeepEqual(totals, []int{3}) ||
		source.readCount() != 1 ||
		len(writes) != 1 ||
		len(writes[0].rows) != 2 ||
		writes[0].rows[0][0] != int64(101) {
		t.Fatalf(
			"totals=%#v reads=%d writes=%#v",
			totals,
			source.readCount(),
			writes,
		)
	}
}

func TestStage4AdapterNetworkLaterDependencyWaveRestoresGlobalRange(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-wave-prefix")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1, 1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(1, 50)
	source.rows[1] = stage4NetworkRunnerRows(3, 100)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	stage4NetworkRunnerSeedCheckpointAt(
		t,
		execution.coordinator,
		1,
		3,
		1,
		false,
	)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatal(err)
	}
	totals, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(totals, []int{1, 3}) {
		t.Fatalf("dependency-wave totals = %#v", totals)
	}
	writes := target.snapshotWrites()
	if len(writes) != 2 ||
		writes[0].table != "table_0" ||
		len(writes[0].rows) != 1 ||
		writes[1].table != "table_1" ||
		len(writes[1].rows) != 2 {
		t.Fatalf("dependency-wave restored writes = %#v", writes)
	}
}

func TestStage4AdapterNetworkCheckpointFailureStopsAfterDurableWrite(
	t *testing.T,
) {
	base := newNetworkStateTestRun(t, "sqlite", "stage4-network-ack-fail")
	base.Backend = stage4NetworkRunnerFailAckBackend{
		Stage4StateBackend: base.Backend,
	}
	prepared := stage4NetworkRunnerTestPrepared(t, base, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(1, 100)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	observer := networkStateTestProtector{}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		observer,
		execution,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		observer,
		execution,
	); err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("checkpoint failure = %v", err)
	}
	if len(target.snapshotWrites()) != 1 {
		t.Fatalf("target writes = %#v", target.snapshotWrites())
	}
}

func TestStage4AdapterNetworkAdmissionFailsBeforeTargetWrite(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(
			*config.Config,
			*stage4AdapterPrepared,
			*stage4NetworkRunnerTestSource,
			*targetAdapter,
			*config.EffectiveTransferPlan,
		)
	}{
		{
			name: "strict consistency",
			mutate: func(
				cfg *config.Config,
				_ *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				cfg.Migration.StrictConsistency = true
			},
		},
		{
			name: "incremental",
			mutate: func(
				cfg *config.Config,
				_ *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				cfg.Migration.DateUpdatedColumns = []string{"updated_at"}
			},
		},
		{
			name: "delete reconciliation",
			mutate: func(
				cfg *config.Config,
				_ *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				cfg.Migration.Deletes.Mode = config.DeleteModeReconcile
			},
		},
		{
			name: "row number pagination",
			mutate: func(
				_ *config.Config,
				prepared *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				prepared.work[0].pagination.Strategy = PaginationRowNumber
			},
		},
		{
			name: "untrusted retained bound",
			mutate: func(
				_ *config.Config,
				_ *stage4AdapterPrepared,
				source *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				source.trustworthy = false
			},
		},
		{
			name: "global topology mismatch",
			mutate: func(
				_ *config.Config,
				prepared *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				prepared.network.bindings[0].Initial.TopologyHash = "changed"
			},
		},
		{
			name: "unsupported target",
			mutate: func(
				_ *config.Config,
				_ *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				target *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				*target = &stage4NetworkRunnerNoReplayTarget{
					engine: "postgres",
				}
			},
		},
		{
			name: "replay isolation",
			mutate: func(
				_ *config.Config,
				_ *stage4AdapterPrepared,
				_ *stage4NetworkRunnerTestSource,
				target *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				(*target).(*stage4NetworkRunnerTestTarget).
					isolationErr =
					errors.New("injected replay isolation failure")
			},
		},
		{
			name: "ClickHouse source",
			mutate: func(
				_ *config.Config,
				_ *stage4AdapterPrepared,
				source *stage4NetworkRunnerTestSource,
				_ *targetAdapter,
				_ *config.EffectiveTransferPlan,
			) {
				source.engine = "clickhouse"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				"sqlite",
				"stage4-network-reject-"+strings.ReplaceAll(test.name, " ", "-"),
			)
			prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
			source := newStage4NetworkRunnerTestSource("postgres")
			targetRecorder := &stage4NetworkRunnerTestTarget{engine: "postgres"}
			var target targetAdapter = targetRecorder
			cfg := stage4NetworkRunnerConfig()
			resources := stage4NetworkRunnerResources()
			test.mutate(
				&cfg,
				&prepared,
				source,
				&target,
				&resources,
			)
			if _, err := admitStage4AdapterNetworkTransfer(
				context.Background(),
				cfg,
				networkStateTestProtector{},
				source,
				target,
				prepared,
				&resources,
			); err == nil {
				t.Fatal("unsafe Stage 4 network admission succeeded")
			}
			if len(targetRecorder.snapshotWrites()) != 0 {
				t.Fatalf(
					"unsafe admission wrote target rows: %#v",
					targetRecorder.snapshotWrites(),
				)
			}
		})
	}
}

func TestStage4AdapterNetworkRunsParentBeforeChildDependencyWave(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-fk")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1, 1})
	prepared.plans[1].target.ForeignKeys = []schema.ForeignKey{{
		Name:              "child_parent_fk",
		Columns:           []string{"id"},
		ReferencedSchema:  prepared.plans[0].target.Schema,
		ReferencedTable:   prepared.plans[0].target.Name,
		ReferencedColumns: []string{"id"},
	}}
	source := newStage4NetworkRunnerTestSource("postgres")
	source.rows[0] = stage4NetworkRunnerRows(1, 100)
	source.rows[1] = stage4NetworkRunnerRows(1, 200)
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatalf("parent-before-child admission: %v", err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		networkStateTestProtector{},
		execution,
	); err != nil {
		t.Fatalf("bind parent-before-child waves: %v", err)
	}
	if _, err := runStage4AdapterNetworkTransfer(
		context.Background(),
		networkStateTestProtector{},
		execution,
	); err != nil {
		t.Fatalf("run parent-before-child waves: %v", err)
	}
	writes := target.snapshotWrites()
	if len(writes) != 2 ||
		writes[0].table != "table_0" ||
		writes[1].table != "table_1" {
		t.Fatalf("dependency-wave target order = %#v", writes)
	}
}

func TestStage4AdapterNetworkAdmissionRejectsChildBeforeParentOrder(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-fk-order")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1, 1})
	prepared.plans[0].target.ForeignKeys = []schema.ForeignKey{{
		Name:              "child_parent_fk",
		Columns:           []string{"id"},
		ReferencedSchema:  prepared.plans[1].target.Schema,
		ReferencedTable:   prepared.plans[1].target.Name,
		ReferencedColumns: []string{"id"},
	}}
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	if _, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	); err == nil || !strings.Contains(err.Error(), "not ordered after") {
		t.Fatalf("child-before-parent admission error = %v", err)
	}
}

func TestStage4AdapterNetworkAdmissionRejectsSelfForeignKeys(t *testing.T) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-self-fk")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	prepared.plans[0].target.ForeignKeys = []schema.ForeignKey{{
		Name:              "table_0_parent_fk",
		Columns:           []string{"id"},
		ReferencedSchema:  strings.ToUpper(prepared.plans[0].target.Schema),
		ReferencedTable:   strings.ToUpper(prepared.plans[0].target.Name),
		ReferencedColumns: []string{"id"},
	}}
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	if _, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	); err == nil || !strings.Contains(err.Error(), "foreign-key") {
		t.Fatalf("self-FK admission error = %v", err)
	}
	if len(target.snapshotWrites()) != 0 {
		t.Fatalf("self-FK admission wrote target: %#v", target.snapshotWrites())
	}
}

func TestStage4AdapterNetworkAdmissionClampsSQLiteConcurrency(t *testing.T) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-sqlite")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("sqlite")
	target := &stage4NetworkRunnerTestTarget{engine: "sqlite"}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	got := execution.plan.Resources
	if got.Readers.Value != 1 ||
		got.Readers.Provenance != config.ProvenanceSafetyClamped ||
		got.Writers.Value != 1 ||
		got.Writers.Provenance != config.ProvenanceSafetyClamped ||
		got.Workers.Value < got.Readers.Value+got.Writers.Value {
		t.Fatalf("SQLite-clamped resources = %#v", got)
	}
}

func TestStage4AdapterNetworkBindRejectsInvalidResourcesBeforeRun(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "stage4-network-invalid-plan")
	prepared := stage4NetworkRunnerTestPrepared(t, run, []int{1})
	source := newStage4NetworkRunnerTestSource("postgres")
	target := &stage4NetworkRunnerTestTarget{engine: "postgres"}
	resources := stage4NetworkRunnerResources()
	resources.QueueDepth.Value = 0
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		stage4NetworkRunnerConfig(),
		networkStateTestProtector{},
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}
	stage4NetworkRunnerEnsurePlans(t, prepared)
	if err := bindStage4AdapterNetworkRestoresAndValidate(
		context.Background(),
		networkStateTestProtector{},
		execution,
	); err == nil {
		t.Fatal("invalid resource plan bound before target preparation")
	}
	if len(target.snapshotWrites()) != 0 {
		t.Fatalf("invalid resource plan wrote target: %#v", target.snapshotWrites())
	}
}

type stage4NetworkRunnerTestSource struct {
	mu          sync.Mutex
	engine      string
	trustworthy bool
	rows        map[uint64][][]any
	reads       int
}

type stage4NetworkRunnerPlainObserver struct{}

func (stage4NetworkRunnerPlainObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (stage4NetworkRunnerPlainObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func newStage4NetworkRunnerTestSource(
	engine string,
) *stage4NetworkRunnerTestSource {
	return &stage4NetworkRunnerTestSource{
		engine:      engine,
		trustworthy: true,
		rows:        make(map[uint64][][]any),
	}
}

func (source *stage4NetworkRunnerTestSource) Engine() string {
	return source.engine
}

func (*stage4NetworkRunnerTestSource) DisplayName() string { return "test" }

func (*stage4NetworkRunnerTestSource) ListTables(
	context.Context,
) ([]string, error) {
	return nil, errors.New("unexpected ListTables")
}

func (*stage4NetworkRunnerTestSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return schema.Table{}, errors.New("unexpected InspectTable")
}

func (*stage4NetworkRunnerTestSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, errors.New("unexpected OpenRows")
}

func (*stage4NetworkRunnerTestSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, errors.New("unexpected CountRows")
}

func (*stage4NetworkRunnerTestSource) Close() error { return nil }

func (source *stage4NetworkRunnerTestSource) PlanRetainedRowWidth(
	_ context.Context,
	table schema.Table,
	columns []string,
) (RuntimeRowWidthEvidence, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if !source.trustworthy {
		return RuntimeRowWidthEvidence{}, nil
	}
	if len(columns) != len(table.Columns) {
		return RuntimeRowWidthEvidence{}, errors.New("incomplete projection")
	}
	return RuntimeRowWidthEvidence{
		Trustworthy:         true,
		CompleteColumnCount: len(columns),
		ExpectedColumnCount: len(columns),
		UpperBoundBytes:     4096,
	}, nil
}

func (source *stage4NetworkRunnerTestSource) ReadNetworkRangePage(
	_ context.Context,
	_ schema.Table,
	_ []string,
	_ PaginationPlan,
	_ PaginationRange,
	request NetworkReadRequest,
) (NetworkReadPage, error) {
	source.mu.Lock()
	source.reads++
	rows := cloneStage4NetworkRunnerRows(source.rows[request.Range.RangeIndex])
	source.mu.Unlock()
	if request.ReplayExpected != nil {
		if len(rows) != request.ReplayExpected.Rows {
			return NetworkReadPage{}, errors.New("replay row count changed")
		}
		return stage4NetworkRunnerPage(
			rows,
			request.ReplayExpected.EndFrontier,
			request.ReplayExpected.Fingerprint,
			request.ReplayExpected.Exhausted,
		)
	}
	if len(request.StartFrontier) != 0 {
		return NetworkReadPage{
			EndFrontier: cloneNetworkBytes(request.StartFrontier),
			Exhausted:   true,
		}, nil
	}
	frontier := mustEncodeStage4NetworkRunnerFrontier(
		request.Range.RangeIndex + 1,
	)
	digest := sha256.Sum256(
		[]byte(fmt.Sprintf("stage4-page-%d", request.Range.RangeIndex)),
	)
	return stage4NetworkRunnerPage(
		rows,
		frontier,
		hex.EncodeToString(digest[:]),
		true,
	)
}

func (source *stage4NetworkRunnerTestSource) readCount() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.reads
}

type stage4NetworkRunnerWrite struct {
	table string
	mode  string
	rows  [][]any
}

type stage4NetworkRunnerTestTarget struct {
	mu             sync.Mutex
	engine         string
	writes         []stage4NetworkRunnerWrite
	isolationErr   error
	isolationCalls int
}

func (*stage4NetworkRunnerTestTarget) stage4NetworkIdempotentUpsertTarget() {
}

func (*stage4NetworkRunnerTestTarget) stage4NetworkDuplicateSafeRebuildTarget() {
}

func (target *stage4NetworkRunnerTestTarget) PreflightStage4NetworkReplayIsolation(
	context.Context,
	[]schema.Table,
) error {
	target.mu.Lock()
	defer target.mu.Unlock()
	target.isolationCalls++
	return target.isolationErr
}

func (target *stage4NetworkRunnerTestTarget) PreflightStage4NetworkRebuild(
	context.Context,
	[]schema.Table,
) error {
	target.mu.Lock()
	defer target.mu.Unlock()
	target.isolationCalls++
	return target.isolationErr
}

func (target *stage4NetworkRunnerTestTarget) Engine() string {
	return target.engine
}

func (target *stage4NetworkRunnerTestTarget) WriteStage4NetworkBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	rows [][]any,
) (WriteReceipt, error) {
	return target.WriteBatch(ctx, table, columns, "upsert", rows)
}

func (target *stage4NetworkRunnerTestTarget) WriteStage4NetworkRebuildBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	return target.WriteBatch(
		ctx,
		table,
		columns,
		string(mode),
		rows,
	)
}

func (*stage4NetworkRunnerTestTarget) PlanTables(
	string,
	[]schema.Table,
	string,
) ([]schema.Table, error) {
	return nil, errors.New("unexpected PlanTables")
}

func (*stage4NetworkRunnerTestTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return errors.New("unexpected PreflightTables")
}

func (*stage4NetworkRunnerTestTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return errors.New("unexpected PrepareTables")
}

func (target *stage4NetworkRunnerTestTarget) WriteBatch(
	_ context.Context,
	table schema.Table,
	_ []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	target.mu.Lock()
	target.writes = append(target.writes, stage4NetworkRunnerWrite{
		table: table.Name,
		mode:  mode,
		rows:  cloneStage4NetworkRunnerRows(rows),
	})
	target.mu.Unlock()
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (*stage4NetworkRunnerTestTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, errors.New("unexpected CountRows")
}

func (*stage4NetworkRunnerTestTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return errors.New("unexpected FinalizeTables")
}

func (*stage4NetworkRunnerTestTarget) Close() error { return nil }

func (target *stage4NetworkRunnerTestTarget) snapshotWrites() []stage4NetworkRunnerWrite {
	target.mu.Lock()
	defer target.mu.Unlock()
	result := make([]stage4NetworkRunnerWrite, len(target.writes))
	for index, write := range target.writes {
		result[index] = write
		result[index].rows = cloneStage4NetworkRunnerRows(write.rows)
	}
	return result
}

type stage4NetworkRunnerNoReplayTarget struct {
	engine string
}

func (target *stage4NetworkRunnerNoReplayTarget) Engine() string {
	return target.engine
}

func (*stage4NetworkRunnerNoReplayTarget) PlanTables(
	string,
	[]schema.Table,
	string,
) ([]schema.Table, error) {
	return nil, nil
}

func (*stage4NetworkRunnerNoReplayTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return nil
}

func (*stage4NetworkRunnerNoReplayTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return nil
}

func (*stage4NetworkRunnerNoReplayTarget) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	return WriteReceipt{}, errors.New("unexpected WriteBatch")
}

func (*stage4NetworkRunnerNoReplayTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, nil
}

func (*stage4NetworkRunnerNoReplayTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	return nil
}

func (*stage4NetworkRunnerNoReplayTarget) Close() error { return nil }

type stage4NetworkRunnerFailAckBackend struct {
	Stage4StateBackend
}

func (stage4NetworkRunnerFailAckBackend) AcknowledgeRange(
	state.RangeAcknowledgement,
) (state.RangeState, error) {
	return state.RangeState{}, errors.New("injected checkpoint failure")
}

func stage4NetworkRunnerTestPrepared(
	t *testing.T,
	run Stage4RunContext,
	rangeCounts []int,
) stage4AdapterPrepared {
	t.Helper()
	plans := make([]adapterTablePlan, len(rangeCounts))
	work := make([]stage4AdapterWork, len(rangeCounts))
	for tableIndex, rangeCount := range rangeCounts {
		sourceTable := stage4NetworkRunnerTable("source", tableIndex)
		targetTable := stage4NetworkRunnerTable("target", tableIndex)
		plans[tableIndex] = adapterTablePlan{
			source:  sourceTable,
			target:  targetTable,
			columns: adapterColumnNames(sourceTable),
		}
		pagination := PaginationPlan{
			Strategy: PaginationIntegerKeyset,
			Keys: []KeySpec{{
				Name: "id",
				Kind: KeyInteger,
			}},
			TopologyHash: strings.Repeat(
				fmt.Sprintf("%x", tableIndex+1),
				64,
			)[:64],
		}
		ranges := make([]state.RangeState, rangeCount)
		for rangeIndex := 0; rangeIndex < rangeCount; rangeIndex++ {
			upper := KeyTuple{IntegerKey(int64((rangeIndex + 1) * 10))}
			planned := PaginationRange{
				ID:    rangeIndex,
				Upper: &upper,
			}
			if rangeIndex > 0 {
				lower := KeyTuple{IntegerKey(int64(rangeIndex * 10))}
				planned.Lower = &lower
			}
			pagination.Ranges = append(pagination.Ranges, planned)
		}
		topology := fmt.Sprintf("stage4-network-table-%d", tableIndex)
		for rangeIndex, planned := range pagination.Ranges {
			var err error
			ranges[rangeIndex], err = stage4AdapterStateRange(
				planned,
				topology,
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		work[tableIndex] = stage4AdapterWork{
			task: state.TaskKey{
				Type:   stage4AdapterNetworkTaskType,
				Schema: sourceTable.Schema,
				Table:  sourceTable.Name,
			},
			strategy:   stage4AdapterCopyStrategy,
			topology:   topology,
			ranges:     ranges,
			pagination: pagination,
		}
	}
	coordinator, err := newStage4AdapterNetworkCoordinator(run, work)
	if err != nil {
		t.Fatal(err)
	}
	return stage4AdapterPrepared{
		run:     run,
		mode:    "upsert",
		plans:   plans,
		work:    work,
		network: coordinator,
	}
}

func stage4NetworkRunnerTable(namespace string, index int) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   fmt.Sprintf("table_%d", index),
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "payload",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
			},
		},
	}
}

func stage4NetworkRunnerConfig() config.Config {
	return config.Config{Migration: config.Migration{
		TargetMode: "upsert",
		MaxRetries: 0,
		Deletes: config.DeletePolicy{
			Mode: config.DeleteModeOff,
		},
	}}
}

func stage4NetworkRunnerResources() config.EffectiveTransferPlan {
	return config.EffectiveTransferPlan{
		TargetMode: "upsert",
		ConnectionLimit: config.EffectiveInt{
			Value:      4,
			Provenance: config.ProvenanceDerived,
		},
		DetectedMemoryLimit: config.EffectiveBytes{
			Value:      64 << 20,
			Provenance: config.ProvenanceHostAvailable,
		},
		MemoryBudget: config.EffectiveBytes{
			Value:      32 << 20,
			Provenance: config.ProvenanceHostAvailable,
		},
		Workers: config.EffectiveInt{
			Value:      4,
			Provenance: config.ProvenanceDerived,
		},
		Readers: config.EffectiveInt{
			Value:      2,
			Provenance: config.ProvenanceDerived,
		},
		Writers: config.EffectiveInt{
			Value:      2,
			Provenance: config.ProvenanceDerived,
		},
		QueueDepth: config.EffectiveInt{
			Value:      4,
			Provenance: config.ProvenanceDerived,
		},
		ChunkRows: config.EffectiveInt{
			Value:      8,
			Provenance: config.ProvenanceDerived,
		},
	}
}

func stage4NetworkRunnerEnsurePlans(
	t *testing.T,
	prepared stage4AdapterPrepared,
) {
	t.Helper()
	if err := prepared.network.ensurePlans(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func stage4NetworkRunnerSeedCheckpoint(
	t *testing.T,
	coordinator *networkStateCoordinator,
	rows int,
	prefix int64,
	complete bool,
) {
	t.Helper()
	stage4NetworkRunnerSeedCheckpointAt(
		t,
		coordinator,
		0,
		rows,
		prefix,
		complete,
	)
}

func stage4NetworkRunnerSeedCheckpointAt(
	t *testing.T,
	coordinator *networkStateCoordinator,
	globalIndex uint64,
	rows int,
	prefix int64,
	complete bool,
) {
	t.Helper()
	end := mustEncodeStage4NetworkRunnerFrontier(1)
	issued := NetworkIssuedChunk{
		RangeIndex:  globalIndex,
		Sequence:    0,
		Rows:        rows,
		EndFrontier: end,
		Fingerprint: strings.Repeat("a", 64),
		Exhausted:   true,
	}
	if err := coordinator.recordIssued(context.Background(), issued); err != nil {
		t.Fatal(err)
	}
	binding := coordinator.bindings[globalIndex]
	if err := coordinator.run.Backend.RecordRangeAttempt(state.RangeAttempt{
		RunID:        coordinator.run.RunID,
		Task:         binding.Task,
		RangeID:      binding.Initial.ID,
		TopologyHash: binding.Initial.TopologyHash,
		Sequence:     0,
		At:           time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	frontier := AckFrontier{
		RangeID:        fmt.Sprintf("range/%d", globalIndex),
		Rows:           prefix,
		NextSequence:   0,
		SequenceOffset: prefix,
	}
	frontierBytes := []byte(nil)
	if complete {
		frontier.Rows = int64(rows)
		frontier.NextSequence = 1
		frontier.SequenceOffset = 0
		frontierBytes = end
	}
	if err := coordinator.checkpoint(
		context.Background(),
		NetworkRangeCheckpoint{
			RangeIndex:    globalIndex,
			TopologyHash:  binding.Initial.TopologyHash,
			Frontier:      frontier,
			FrontierBytes: frontierBytes,
			Complete:      complete,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func stage4NetworkRunnerRows(count int, first int64) [][]any {
	rows := make([][]any, count)
	for index := range rows {
		rows[index] = []any{
			first + int64(index),
			fmt.Sprintf("row-%d", first+int64(index)),
		}
	}
	return rows
}

func cloneStage4NetworkRunnerRows(rows [][]any) [][]any {
	result := make([][]any, len(rows))
	for index := range rows {
		result[index] = cloneAdapterRow(rows[index])
	}
	return result
}

func stage4NetworkRunnerPage(
	rows [][]any,
	frontier []byte,
	fingerprint string,
	exhausted bool,
) (NetworkReadPage, error) {
	page := NetworkReadPage{
		Rows:          cloneStage4NetworkRunnerRows(rows),
		RowBytes:      make([]int64, len(rows)),
		EndFrontier:   cloneNetworkBytes(frontier),
		Fingerprint:   fingerprint,
		Exhausted:     exhausted,
		RetainedBytes: 0,
	}
	for index, row := range page.Rows {
		retained, err := measureAdapterRetainedRowBytes(row)
		if err != nil {
			return NetworkReadPage{}, err
		}
		page.RowBytes[index] = retained
		page.RetainedBytes += retained
	}
	return page, nil
}

func mustEncodeStage4NetworkRunnerFrontier(value uint64) []byte {
	encoded, err := encodeNetworkStateFrontier(
		state.TypedTuple{state.Int64Value(int64(value))},
		true,
	)
	if err != nil {
		panic(err)
	}
	return encoded
}
