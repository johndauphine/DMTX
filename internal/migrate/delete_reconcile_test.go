package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestDeleteReconcileDryRunHasNoDurableWrites(t *testing.T) {
	t.Run("due", func(t *testing.T) {
		request := deleteTestRequest(t)
		request.DryRun = true
		backend := newDeleteFakeState()
		events := make([]string, 0)
		source := &deleteFakeSource{
			rows:   [][]any{{"a", int64(1)}},
			events: &events,
		}
		target := &deleteFakeTarget{
			rows: [][]any{
				{"a", int64(1)},
				{"b", int64(2)},
			},
			parameterLimit: 100,
			events:         &events,
		}
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		reconciler.protector = nil
		outcome, err := reconciler.reconcile(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationDryRun ||
			outcome.Record.Candidates != 1 ||
			outcome.Record.DeletedRows != 0 ||
			outcome.StrictCountValidation {
			t.Fatalf("dry-run outcome = %#v", outcome)
		}
		assertDeleteStateWriteCalls(t, backend, 0, 0, 0, 0, 0)
		if target.applyCalls != 0 {
			t.Fatal("due dry run mutated the target")
		}
		entries, err := os.ReadDir(request.SpoolDirectory)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("dry-run left delete spool files: %v", entries)
		}
		if !reflect.DeepEqual(
			events,
			[]string{
				"source-open", "source-close",
				"target-open", "target-close",
			},
		) {
			t.Fatalf("dry-run scan events = %v", events)
		}
	})

	t.Run("not due", func(t *testing.T) {
		request := deleteTestRequest(t)
		request.DryRun = true
		backend := newDeleteFakeState()
		backend.latest = deleteCompletedEvidence(
			request,
			request.Now.Add(-time.Hour),
		)
		backend.latestFound = true
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{parameterLimit: 100}
		outcome, err := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		).reconcile(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationNotDue ||
			outcome.Record.Due ||
			source.openCalls != 0 || target.openCalls != 0 {
			t.Fatalf("not-due dry-run outcome = %#v", outcome)
		}
		assertDeleteStateWriteCalls(t, backend, 0, 0, 0, 0, 0)
	})
}

func TestDeleteReconcileDiskSpoolAndBoundedBatches(t *testing.T) {
	request := deleteTestRequest(t)
	request.Policy.Reconcile.BatchSize = 10
	backend := newDeleteFakeState()
	events := make([]string, 0)
	source := &deleteFakeSource{
		rows: [][]any{
			{"tenant-a", int64(1)},
			{"tenant-b", []byte("1")},
		},
		events: &events,
	}
	target := &deleteFakeTarget{
		rows: [][]any{
			{"tenant-c", int64(3)},
			{[]byte("tenant-b"), int32(1)},
			{"tenant-a", int64(5)},
			{"tenant-b", int64(2)},
			{"tenant-a", []byte("1")},
		},
		parameterLimit: 4,
		events:         &events,
		state:          backend,
	}
	outcome, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationCompleted ||
		outcome.Record.Candidates != 3 ||
		outcome.Record.DeletedRows != 3 ||
		outcome.Record.Frontier != 3 ||
		outcome.Record.CommittedBatches != 2 ||
		!outcome.StrictCountValidation {
		t.Fatalf("completed outcome = %#v", outcome)
	}
	if got := deleteTargetBatchSizes(target.batches); !reflect.DeepEqual(
		got,
		[]int{2, 1},
	) {
		t.Fatalf("bounded batches = %v, want [2 1]", got)
	}
	if got := deleteTargetKeys(target.batches); !reflect.DeepEqual(
		got,
		[]string{
			"tenant-a/5", "tenant-b/2", "tenant-c/3",
		},
	) {
		t.Fatalf("stable candidate order = %v", got)
	}
	if len(events) < 5 ||
		!reflect.DeepEqual(
			events[:4],
			[]string{
				"source-open", "source-close",
				"target-open", "target-close",
			},
		) ||
		events[4] != "target-apply" {
		t.Fatalf("scan/delete ordering = %v", events)
	}
	assertDeleteStateWriteCalls(t, backend, 1, 1, 1, 2, 2)
	if backend.record.Plan == nil ||
		backend.record.Plan.CandidateDigest == "" ||
		backend.record.Plan.EqualityProofDigest == "" {
		t.Fatalf("durable candidate plan = %#v", backend.record.Plan)
	}
}

func TestDeleteReconcileLargeKeySetUsesBoundedSpoolBatches(t *testing.T) {
	request := deleteTestRequest(t)
	request.Policy.Reconcile.BatchSize = 17
	backend := newDeleteFakeState()
	targetRows := make([][]any, 1003)
	for index := range targetRows {
		targetRows[index] = []any{
			"tenant", int64(index + 1),
		}
	}
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows: targetRows, parameterLimit: 1000, state: backend,
	}
	outcome, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Candidates != 1003 ||
		outcome.Record.DeletedRows != 1003 {
		t.Fatalf("large reconciliation = %#v", outcome.Record)
	}
	for index, batch := range target.batches {
		if len(batch.Keys) > 17 {
			t.Fatalf("batch %d has %d keys", index, len(batch.Keys))
		}
	}
	if len(target.batches) != 59 {
		t.Fatalf("batch count = %d, want 59", len(target.batches))
	}
}

func TestDeleteReconcileRequiresExplicitSafeEqualityProof(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*deleteReconcileRequest, *deleteTestCanonicalizer)
		want   string
	}{
		{
			name: "no canonicalizer",
			mutate: func(
				_ *deleteReconcileRequest,
				canonicalizer *deleteTestCanonicalizer,
			) {
				canonicalizer.disabled = true
			},
			want: "explicit key-equality canonicalizer",
		},
		{
			name: "nullable source primary key",
			mutate: func(
				request *deleteReconcileRequest,
				_ *deleteTestCanonicalizer,
			) {
				request.SourceTable.Columns[0].Nullable = true
			},
			want: "is nullable",
		},
		{
			name: "dynamic source key",
			mutate: func(
				request *deleteReconcileRequest,
				_ *deleteTestCanonicalizer,
			) {
				request.SourceTable.Columns[0].Type = "any"
			},
			want: "dynamic primary-key",
		},
		{
			name: "unbound metadata",
			mutate: func(
				_ *deleteReconcileRequest,
				canonicalizer *deleteTestCanonicalizer,
			) {
				canonicalizer.badFingerprint = true
			},
			want: "does not bind",
		},
		{
			name: "text without collation evidence",
			mutate: func(
				_ *deleteReconcileRequest,
				canonicalizer *deleteTestCanonicalizer,
			) {
				canonicalizer.omitCollationEvidence = true
			},
			want: "binary-collation evidence",
		},
		{
			name: "text to integer mismatch",
			mutate: func(
				request *deleteReconcileRequest,
				_ *deleteTestCanonicalizer,
			) {
				request.TargetTable.Columns[0].Type = "bigint"
			},
			want: "text proof does not match metadata",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := deleteTestRequest(t)
			canonicalizer := &deleteTestCanonicalizer{}
			test.mutate(&request, canonicalizer)
			backend := newDeleteFakeState()
			source := &deleteFakeSource{}
			target := &deleteFakeTarget{parameterLimit: 100}
			var selected deleteKeyCanonicalizer = canonicalizer
			if canonicalizer.disabled {
				selected = nil
			}
			_, err := (deleteReconciler{
				state: backend, source: source, target: target,
				canonicalizer: selected,
				now: func() time.Time {
					return request.Now.Add(time.Minute)
				},
			}).reconcile(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("proof error = %v, want %q", err, test.want)
			}
			assertDeleteStateWriteCalls(t, backend, 0, 0, 0, 0, 0)
			if source.openCalls != 0 || target.openCalls != 0 ||
				target.applyCalls != 0 {
				t.Fatal("unsafe equality proof crossed mutation preflight")
			}
		})
	}
}

func TestDeleteReconcileTaskAdmissionIsExact(t *testing.T) {
	for _, taskType := range []string{
		"table-copy",
		stage4AdapterNetworkTaskType,
	} {
		t.Run("admits_"+taskType, func(t *testing.T) {
			request := deleteTestRequest(t)
			request.Task.Type = taskType
			plan, err := validateDeleteReconcileRequest(
				request,
				&deleteTestCanonicalizer{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.sourcePrimaryKey) != 2 ||
				len(plan.targetPrimaryKey) != 2 {
				t.Fatalf("delete key plan = %#v", plan)
			}
		})
	}

	for _, test := range []struct {
		name      string
		taskType  string
		partition string
	}{
		{
			name:     "rejects_analytical_task",
			taskType: "analytical-table-copy",
		},
		{
			name:     "rejects_unknown_task",
			taskType: "unknown-table-copy",
		},
		{
			name:      "rejects_partitioned_legacy_task",
			taskType:  "table-copy",
			partition: "partition/0",
		},
		{
			name:      "rejects_partitioned_network_task",
			taskType:  stage4AdapterNetworkTaskType,
			partition: "partition/0",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			request := deleteTestRequest(t)
			request.Task.Type = test.taskType
			request.Task.Partition = test.partition
			backend := newDeleteFakeState()
			source := &deleteFakeSource{}
			target := &deleteFakeTarget{parameterLimit: 100}
			_, err := deleteTestReconciler(
				request,
				backend,
				source,
				target,
			).reconcile(context.Background(), request)
			if err == nil || !strings.Contains(
				err.Error(),
				"authenticated unpartitioned relational table-copy task",
			) {
				t.Fatalf("task admission error = %v", err)
			}
			assertDeleteStateWriteCalls(t, backend, 0, 0, 0, 0, 0)
			if source.openCalls != 0 || target.openCalls != 0 ||
				target.applyCalls != 0 {
				t.Fatal("rejected delete task crossed state or adapter admission")
			}
		})
	}
}

func TestDeleteReconcileCandidatePlanAndBatchIntentPrecedeMutation(
	t *testing.T,
) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
		applyHook: func(
			batch deleteTargetBatch,
			_ int,
		) error {
			if backend.record.Plan == nil ||
				backend.record.Plan.Candidates != 1 ||
				backend.record.PendingBatch == nil ||
				backend.record.PendingBatch.Token != batch.Token ||
				backend.record.Frontier != 0 {
				return errors.New(
					"target mutation preceded durable plan or batch intent",
				)
			}
			return nil
		},
	}
	if _, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteReconcilePostPlanTamperFailsBeforeIntentOrMutation(
	t *testing.T,
) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.savePlanHook = func(plan state.DeleteReconciliationPlan) error {
		database, err := sql.Open("sqlite", plan.SpoolPath)
		if err != nil {
			return err
		}
		defer database.Close()
		parameters, err := encodeDeleteParameters(
			[]driver.Value{"PRIVATE-TAMPERED-KEY", int64(999)},
		)
		if err != nil {
			return err
		}
		_, err = database.Exec(
			`UPDATE target_keys SET parameters = ?`,
			parameters,
		)
		return err
	}
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	outcome, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"candidate evidence differs from durable plan",
		) {
		t.Fatalf("post-plan tamper error = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		outcome.Record.Reason !=
			state.DeleteReconciliationReasonSpoolVerificationFailed ||
		backend.beginBatchCalls != 0 ||
		backend.commitBatchCalls != 0 ||
		target.applyCalls != 0 {
		t.Fatalf(
			"post-plan tamper outcome=%#v begin=%d commit=%d apply=%d",
			outcome,
			backend.beginBatchCalls,
			backend.commitBatchCalls,
			target.applyCalls,
		)
	}
	if _, statErr := os.Stat(
		backend.record.Plan.SpoolPath,
	); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("terminal tampered spool still exists: %v", statErr)
	}
}

func TestDeleteReconcileCrashAfterTargetCommitReplaysReceipt(t *testing.T) {
	request := deleteTestRequest(t)
	request.Policy.Reconcile.BatchSize = 1
	backend := newDeleteFakeState()
	backend.commitBatchFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows: [][]any{
			{"a", int64(1)},
			{"b", int64(2)},
		},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"durable frontier acknowledgement failed",
		) ||
		!strings.Contains(
			err.Error(),
			"do not start a fresh run",
		) {
		t.Fatalf("first crash-window error = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationRunning ||
		backend.record.PendingBatch == nil ||
		backend.record.Frontier != 0 ||
		target.mutations != 1 {
		t.Fatalf(
			"post-target/pre-state crash = %#v backend=%#v mutations=%d",
			outcome.Record,
			backend.record,
			target.mutations,
		)
	}

	outcome, err = reconciler.reconcile(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationCompleted ||
		outcome.Record.Candidates != 2 ||
		outcome.Record.DeletedRows != 2 ||
		target.applyCalls != 3 ||
		target.mutations != 2 {
		t.Fatalf(
			"receipt replay outcome=%#v apply=%d mutation=%d",
			outcome,
			target.applyCalls,
			target.mutations,
		)
	}
	if target.tokenCalls[target.firstToken] != 2 {
		t.Fatalf(
			"first token calls = %d, want replay",
			target.tokenCalls[target.firstToken],
		)
	}
}

func TestDeleteReconcileTargetErrorReceiptSurvivesStateCommitFailure(
	t *testing.T,
) {
	const targetFailure = "target returned a receipt and then failed"
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.commitBatchFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
		receiptErr:     errors.New(targetFailure),
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!strings.Contains(err.Error(), targetFailure) ||
		!strings.Contains(
			err.Error(),
			"durable frontier acknowledgement failed",
		) {
		t.Fatalf("target-error crash-window error = %v", err)
	}
	receipt := target.receipts[target.firstToken]
	if outcome.Record.Status !=
		state.DeleteReconciliationRunning ||
		backend.record.PendingBatch == nil ||
		backend.record.Frontier != 0 ||
		backend.record.LastBatchCommit != nil ||
		receipt.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		target.applyCalls != 1 ||
		target.mutations != 1 {
		t.Fatalf(
			"target-error crash window outcome=%#v backend=%#v receipt=%#v apply=%d mutation=%d",
			outcome.Record,
			backend.record,
			receipt,
			target.applyCalls,
			target.mutations,
		)
	}
	spoolPath := backend.record.Plan.SpoolPath

	outcome, err = reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			state.DeleteReconciliationReasonTargetMutationFailed,
		) ||
		strings.Contains(err.Error(), targetFailure) {
		t.Fatalf("journaled target-error replay = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		outcome.Record.Reason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		outcome.StrictCountValidation ||
		backend.record.LastBatchCommit == nil ||
		backend.record.LastBatchCommit.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		backend.latestFound ||
		target.applyCalls != 2 ||
		target.mutations != 1 {
		t.Fatalf(
			"journaled target-error outcome=%#v backend=%#v apply=%d mutation=%d",
			outcome,
			backend.record,
			target.applyCalls,
			target.mutations,
		)
	}
	if _, statErr := os.Stat(spoolPath); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("terminal target-error spool still exists: %v", statErr)
	}
}

func TestDeleteReconcileProtectorPostCallbackErrorCanReplayAfterStateCommitFailure(
	t *testing.T,
) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.commitBatchFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	protector := &deleteFakeProtector{
		afterErr: state.ErrLeaseLost,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	reconciler.protector = protector
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!errors.Is(err, state.ErrLeaseLost) ||
		!strings.Contains(
			err.Error(),
			"durable frontier acknowledgement failed",
		) {
		t.Fatalf("protector crash-window error = %v", err)
	}
	receipt := target.receipts[target.firstToken]
	if outcome.Record.Status !=
		state.DeleteReconciliationRunning ||
		backend.record.PendingBatch == nil ||
		backend.record.Frontier != 0 ||
		backend.record.LastBatchCommit != nil ||
		receipt.FailClosedReason != "" ||
		target.applyCalls != 1 ||
		target.mutations != 1 {
		t.Fatalf(
			"protector crash window outcome=%#v backend=%#v receipt=%#v apply=%d mutation=%d",
			outcome.Record,
			backend.record,
			receipt,
			target.applyCalls,
			target.mutations,
		)
	}

	// The target journal is clean. Once local authority is healthy, replay may
	// accept the exact receipt and advance without repeating the mutation.
	protector.afterErr = nil
	outcome, err = reconciler.reconcile(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationCompleted ||
		!outcome.StrictCountValidation ||
		backend.record.LastBatchCommit == nil ||
		backend.record.LastBatchCommit.FailClosedReason != "" ||
		!backend.latestFound ||
		target.applyCalls != 2 ||
		target.mutations != 1 {
		t.Fatalf(
			"healthy protector replay outcome=%#v backend=%#v apply=%d mutation=%d",
			outcome,
			backend.record,
			target.applyCalls,
			target.mutations,
		)
	}
}

func TestDeleteReconcileCrashAfterStateCommitUsesDurableFrontier(
	t *testing.T,
) {
	request := deleteTestRequest(t)
	request.Policy.Reconcile.BatchSize = 1
	backend := newDeleteFakeState()
	backend.commitBatchApplyThenFailOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows: [][]any{
			{"a", int64(1)},
			{"b", int64(2)},
		},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	if _, err := reconciler.reconcile(
		context.Background(),
		request,
	); err == nil {
		t.Fatal("state commit crash window did not fail")
	}
	if backend.record.Frontier != 1 ||
		backend.record.PendingBatch != nil ||
		target.mutations != 1 {
		t.Fatalf(
			"durable frontier = %#v mutations=%d",
			backend.record,
			target.mutations,
		)
	}
	firstToken := target.firstToken
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationCompleted ||
		target.tokenCalls[firstToken] != 1 ||
		target.mutations != 2 {
		t.Fatalf(
			"frontier replay outcome=%#v tokenCalls=%d mutations=%d",
			outcome,
			target.tokenCalls[firstToken],
			target.mutations,
		)
	}
}

func TestDeleteReconcileCancellationAfterFinalAcknowledgementStaysRunning(
	t *testing.T,
) {
	t.Run("after durable batch acknowledgement", func(t *testing.T) {
		request := deleteTestRequest(t)
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		ctx, cancel := context.WithCancel(context.Background())
		target := &deleteFakeTarget{
			rows:             [][]any{{"a", int64(1)}},
			parameterLimit:   100,
			state:            backend,
			afterReceiptHook: cancel,
		}
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		outcome, err := reconciler.reconcile(ctx, request)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("post-acknowledgement cancellation error = %v", err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationRunning ||
			backend.record.Status != state.DeleteReconciliationRunning ||
			backend.record.Frontier != 1 ||
			backend.record.DeletedRows != 1 ||
			backend.record.PendingBatch != nil ||
			backend.finishCalls != 0 ||
			outcome.StrictCountValidation {
			t.Fatalf(
				"post-acknowledgement cancellation outcome=%#v backend=%#v finish=%d",
				outcome,
				backend.record,
				backend.finishCalls,
			)
		}
		spoolPath := backend.record.Plan.SpoolPath
		if _, err := os.Stat(spoolPath); err != nil {
			t.Fatalf("cancelled acknowledgement lost its spool: %v", err)
		}
		target.afterReceiptHook = nil
		applyCalls := target.applyCalls
		outcome, err = reconciler.reconcile(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationCompleted ||
			!outcome.StrictCountValidation ||
			target.applyCalls != applyCalls {
			t.Fatalf(
				"post-cancellation resume outcome=%#v apply=%d",
				outcome,
				target.applyCalls,
			)
		}
		if _, err := os.Stat(spoolPath); !errors.Is(
			err,
			os.ErrNotExist,
		) {
			t.Fatalf("completed resume spool still exists: %v", err)
		}
	})

	t.Run("immediately before finish", func(t *testing.T) {
		request := deleteTestRequest(t)
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows:           [][]any{{"a", int64(1)}},
			parameterLimit: 100,
			state:          backend,
		}
		ctx, cancel := context.WithCancel(context.Background())
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		clockCalls := 0
		reconciler.now = func() time.Time {
			clockCalls++
			if clockCalls == 4 {
				cancel()
			}
			return request.Now.Add(
				time.Duration(clockCalls) * time.Minute,
			)
		}
		outcome, err := reconciler.reconcile(ctx, request)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-finish cancellation error = %v", err)
		}
		if clockCalls != 4 ||
			outcome.Record.Status != state.DeleteReconciliationRunning ||
			backend.record.Status != state.DeleteReconciliationRunning ||
			backend.record.Frontier != 1 ||
			backend.finishCalls != 0 ||
			outcome.StrictCountValidation {
			t.Fatalf(
				"pre-finish cancellation outcome=%#v backend=%#v clock=%d finish=%d",
				outcome,
				backend.record,
				clockCalls,
				backend.finishCalls,
			)
		}
		spoolPath := backend.record.Plan.SpoolPath
		if _, err := os.Stat(spoolPath); err != nil {
			t.Fatalf("pre-finish cancellation lost its spool: %v", err)
		}
		applyCalls := target.applyCalls
		reconciler.now = func() time.Time {
			return request.Now.Add(5 * time.Minute)
		}
		outcome, err = reconciler.reconcile(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationCompleted ||
			!outcome.StrictCountValidation ||
			target.applyCalls != applyCalls {
			t.Fatalf(
				"pre-finish resume outcome=%#v apply=%d",
				outcome,
				target.applyCalls,
			)
		}
	})
}

func TestDeleteReconcileReceiptErrorFinishFailureStaysFailClosed(
	t *testing.T,
) {
	const secret = "postgres://operator:credential@host/PRIVATE-ROW-VALUE"
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.finishFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
		receiptErr:     errors.New(secret),
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!strings.Contains(err.Error(), secret) ||
		!strings.Contains(
			err.Error(),
			"persist incomplete delete reconciliation",
		) ||
		!strings.Contains(
			err.Error(),
			"repair state and resume this existing run and attempt",
		) ||
		!strings.Contains(
			err.Error(),
			"do not start a fresh run",
		) {
		t.Fatalf("compound receipt/finish error = %v", err)
	}
	if outcome.Record.Status != state.DeleteReconciliationRunning ||
		backend.record.Status != state.DeleteReconciliationRunning ||
		backend.record.Frontier != 1 ||
		backend.record.DeletedRows != 1 ||
		backend.record.PendingBatch != nil ||
		backend.record.LastBatchCommit == nil ||
		backend.record.LastBatchCommit.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		backend.latestFound {
		t.Fatalf(
			"compound receipt/finish state outcome=%#v backend=%#v",
			outcome.Record,
			backend.record,
		)
	}
	encoded, marshalErr := json.Marshal(backend.record)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("durable delete state leaked a row value: %s", encoded)
	}
	spoolPath := backend.record.Plan.SpoolPath
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("finish-state failure discarded resume spool: %v", err)
	}

	openCalls := source.openCalls + target.openCalls
	applyCalls := target.applyCalls
	outcome, err = reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("fail-closed resume error = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		outcome.Record.Reason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		backend.latestFound ||
		source.openCalls+target.openCalls != openCalls ||
		target.applyCalls != applyCalls {
		t.Fatalf(
			"fail-closed resume outcome=%#v latest=%v opens=%d apply=%d",
			outcome,
			backend.latestFound,
			source.openCalls+target.openCalls,
			target.applyCalls,
		)
	}
	encoded, marshalErr = json.Marshal(backend.record)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("terminal delete state leaked a row value: %s", encoded)
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal incomplete spool still exists: %v", err)
	}
}

func TestDeleteReconcileTargetReceiptReasonOverridesCancellationDiagnostic(
	t *testing.T,
) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
		receiptErr:     context.Canceled,
	}
	outcome, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("target cancellation diagnostic = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		outcome.Record.Reason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		backend.record.LastBatchCommit == nil ||
		backend.record.LastBatchCommit.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed ||
		backend.finishCalls != 1 ||
		backend.latestFound ||
		outcome.StrictCountValidation {
		t.Fatalf(
			"target cancellation receipt outcome=%#v backend=%#v finish=%d",
			outcome,
			backend.record,
			backend.finishCalls,
		)
	}
}

func TestValidateDeleteTargetReceiptRequiresTargetAtomicFailureEvidence(
	t *testing.T,
) {
	intent := state.DeleteReconciliationBatch{
		PlanID: "plan", Token: "token", Sequence: 1,
		BatchDigest: strings.Repeat("a", 64),
		Candidates:  1,
	}
	receipt := deleteTargetBatchReceipt{
		PlanID: "plan", Token: "token", Sequence: 1,
		BatchDigest:   intent.BatchDigest,
		Candidates:    1,
		DeletedRows:   1,
		ReceiptDigest: strings.Repeat("b", 64),
	}
	targetErr := errors.New("target failure")
	if err := validateDeleteTargetReceipt(
		intent,
		receipt,
		targetErr,
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"without target-atomic fail-closed evidence",
		) {
		t.Fatalf("missing target failure evidence error = %v", err)
	}

	receipt.FailClosedReason =
		state.DeleteReconciliationReasonKeyScanFailed
	if err := validateDeleteTargetReceipt(
		intent,
		receipt,
		nil,
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"invalid fail-closed reason",
		) {
		t.Fatalf("invalid target failure evidence error = %v", err)
	}

	receipt.FailClosedReason =
		state.DeleteReconciliationReasonTargetMutationFailed
	if err := validateDeleteTargetReceipt(
		intent,
		receipt,
		targetErr,
	); err != nil {
		t.Fatalf("initial target failure evidence: %v", err)
	}
	if err := validateDeleteTargetReceipt(
		intent,
		receipt,
		nil,
	); err != nil {
		t.Fatalf("replayed target failure evidence: %v", err)
	}
}

func TestDeleteReconcileFinishFailureReusesOriginalPlan(t *testing.T) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.finishFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	if _, err := reconciler.reconcile(
		context.Background(),
		request,
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"persist completed delete reconciliation after target deletes",
		) ||
		!strings.Contains(
			err.Error(),
			"repair state and resume this existing run and attempt",
		) ||
		!strings.Contains(
			err.Error(),
			"do not start a fresh run",
		) {
		t.Fatalf("finish failure = %v", err)
	}
	planID := backend.record.Plan.PlanID
	if backend.record.Status != state.DeleteReconciliationRunning ||
		backend.record.Frontier != 1 ||
		target.mutations != 1 {
		t.Fatalf("finish failure state = %#v", backend.record)
	}
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationCompleted ||
		outcome.Record.Plan.PlanID != planID ||
		target.applyCalls != 1 ||
		target.mutations != 1 {
		t.Fatalf(
			"finish replay outcome=%#v apply=%d mutations=%d",
			outcome,
			target.applyCalls,
			target.mutations,
		)
	}
}

func TestDeleteReconcileMalformedLoadedEvidenceFailsClosed(t *testing.T) {
	request := deleteTestRequest(t)
	tests := []struct {
		name   string
		record state.DeleteReconciliation
	}{
		{
			name: "terminal counts",
			record: state.DeleteReconciliation{
				RunID: request.RunID, Task: request.Task,
				AttemptID:  request.AttemptID,
				Due:        true,
				Status:     state.DeleteReconciliationCompleted,
				Candidates: 1, DeletedRows: 2,
				StartedAt:   request.Now.Add(-time.Minute),
				CompletedAt: request.Now,
			},
		},
		{
			name: "running progress without plan",
			record: state.DeleteReconciliation{
				RunID: request.RunID, Task: request.Task,
				AttemptID:  request.AttemptID,
				Due:        true,
				Status:     state.DeleteReconciliationRunning,
				Candidates: 1, Frontier: 1,
				StartedAt: request.Now,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newDeleteFakeState()
			backend.record = test.record
			backend.recordFound = true
			source := &deleteFakeSource{}
			target := &deleteFakeTarget{parameterLimit: 100}
			outcome, err := deleteTestReconciler(
				request,
				backend,
				source,
				target,
			).reconcile(context.Background(), request)
			if err == nil || !strings.Contains(
				err.Error(),
				"malformed",
			) {
				t.Fatalf("malformed evidence error = %v", err)
			}
			if outcome.StrictCountValidation ||
				source.openCalls != 0 ||
				target.openCalls != 0 ||
				target.applyCalls != 0 {
				t.Fatal("malformed evidence authorized work")
			}
		})
	}
}

func TestDeleteReconcileSpoolTamperFailsBeforeReplayMutation(t *testing.T) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.finishFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	if _, err := reconciler.reconcile(
		context.Background(),
		request,
	); err == nil {
		t.Fatal("expected injected finish failure")
	}
	spoolPath := backend.record.Plan.SpoolPath
	database, err := sql.Open(
		"sqlite",
		spoolPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(
		`UPDATE target_keys SET parameters = X'00'`,
	); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	applyCalls := target.applyCalls
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil {
		t.Fatal("tampered spool was accepted")
	}
	if target.applyCalls != applyCalls ||
		outcome.Record.Status !=
			state.DeleteReconciliationIncomplete {
		t.Fatalf(
			"tamper outcome=%#v apply calls=%d",
			outcome,
			target.applyCalls,
		)
	}
	if _, statErr := os.Stat(spoolPath); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("terminal tampered spool still exists: %v", statErr)
	}
}

func TestDeleteReconcileTerminalCleanupFailureIsVisible(t *testing.T) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	backend.finishFailBeforeOnce = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	if _, err := reconciler.reconcile(
		context.Background(),
		request,
	); err == nil {
		t.Fatal("expected injected finish failure")
	}
	spoolPath := backend.record.Plan.SpoolPath
	if err := os.Remove(spoolPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.db")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, spoolPath); err != nil {
		t.Fatal(err)
	}
	applyCalls := target.applyCalls
	outcome, err := reconciler.reconcile(
		context.Background(),
		request,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"terminal delete reconciliation spool cleanup failed",
		) {
		t.Fatalf("cleanup failure error = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		outcome.Record.Reason !=
			state.DeleteReconciliationReasonSpoolUnavailable ||
		target.applyCalls != applyCalls {
		t.Fatalf(
			"cleanup failure outcome=%#v apply=%d",
			outcome,
			target.applyCalls,
		)
	}
	linkInfo, statErr := os.Lstat(spoolPath)
	if statErr != nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf(
			"unsafe cleanup target changed info=%#v err=%v",
			linkInfo,
			statErr,
		)
	}
}

func TestDeleteReconcileTerminalReplayCleansCrashLeftoverSpool(t *testing.T) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
	}
	reconciler := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	)
	outcome, err := reconciler.reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.Status != state.DeleteReconciliationCompleted ||
		outcome.Record.Plan == nil {
		t.Fatalf("completed delete outcome = %#v", outcome)
	}
	spoolPath := outcome.Record.Plan.SpoolPath
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initial terminal spool still exists: %v", err)
	}
	if err := os.WriteFile(spoolPath, []byte("crash-leftover"), 0o600); err != nil {
		t.Fatal(err)
	}
	applyCalls := target.applyCalls
	openCalls := source.openCalls
	replayed, err := reconciler.reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Record.Status != state.DeleteReconciliationCompleted ||
		!replayed.StrictCountValidation || target.applyCalls != applyCalls ||
		source.openCalls != openCalls {
		t.Fatalf(
			"terminal replay outcome=%#v source opens=%d target applies=%d",
			replayed,
			source.openCalls,
			target.applyCalls,
		)
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal replay did not remove crash-leftover spool: %v", err)
	}
}

func TestDeleteReconcileIncompleteNeverAdvancesLastSuccess(t *testing.T) {
	request := deleteTestRequest(t)
	backend := newDeleteFakeState()
	previous := deleteCompletedEvidence(
		request,
		request.Now.Add(-48*time.Hour),
	)
	previous.AttemptID = "previous"
	backend.latest = previous
	backend.latestFound = true
	source := &deleteFakeSource{}
	target := &deleteFakeTarget{
		rows:           [][]any{{"a", int64(1)}},
		parameterLimit: 100,
		state:          backend,
		shortDelete:    true,
	}
	outcome, err := deleteTestReconciler(
		request,
		backend,
		source,
		target,
	).reconcile(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "deleted 0 of 1") {
		t.Fatalf("short delete error = %v", err)
	}
	if outcome.Record.Status !=
		state.DeleteReconciliationIncomplete ||
		backend.latest.AttemptID != "previous" {
		t.Fatalf(
			"incomplete outcome=%#v latest=%#v",
			outcome,
			backend.latest,
		)
	}
}

func TestDeleteReconcileDuplicateKeysFailBeforeDelete(t *testing.T) {
	for _, side := range []deleteKeySide{
		deleteKeySourceSide,
		deleteKeyTargetSide,
	} {
		t.Run(string(side), func(t *testing.T) {
			request := deleteTestRequest(t)
			backend := newDeleteFakeState()
			source := &deleteFakeSource{}
			target := &deleteFakeTarget{
				parameterLimit: 100,
				state:          backend,
			}
			rows := [][]any{
				{"a", int64(1)},
				{[]byte("a"), int32(1)},
			}
			if side == deleteKeySourceSide {
				source.rows = rows
			} else {
				target.rows = rows
			}
			outcome, err := deleteTestReconciler(
				request,
				backend,
				source,
				target,
			).reconcile(context.Background(), request)
			if err == nil || !strings.Contains(
				err.Error(),
				"duplicate complete primary key",
			) {
				t.Fatalf("duplicate error = %v", err)
			}
			if outcome.Record.Status !=
				state.DeleteReconciliationIncomplete ||
				target.applyCalls != 0 {
				t.Fatalf("duplicate outcome = %#v", outcome)
			}
		})
	}
}

func TestDeleteReconcileMutationProtectionFailsClosed(t *testing.T) {
	t.Run("protector is required", func(t *testing.T) {
		request := deleteTestRequest(t)
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows:           [][]any{{"a", int64(1)}},
			parameterLimit: 100,
			state:          backend,
		}
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		reconciler.protector = nil
		outcome, err := reconciler.reconcile(
			context.Background(),
			request,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"mutation protector",
		) {
			t.Fatalf("missing protector error = %v", err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationIncomplete ||
			source.openCalls != 0 || target.openCalls != 0 ||
			target.applyCalls != 0 {
			t.Fatalf(
				"missing protector crossed preflight: outcome=%#v source=%d target=%d apply=%d",
				outcome,
				source.openCalls,
				target.openCalls,
				target.applyCalls,
			)
		}
	})

	t.Run("stale owner cannot reach target driver", func(t *testing.T) {
		request := deleteTestRequest(t)
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows:           [][]any{{"a", int64(1)}},
			parameterLimit: 100,
			state:          backend,
		}
		protector := &deleteFakeProtector{
			err: state.ErrLeaseLost,
		}
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		reconciler.protector = protector
		outcome, err := reconciler.reconcile(
			context.Background(),
			request,
		)
		if err == nil || !errors.Is(err, state.ErrLeaseLost) ||
			!strings.Contains(err.Error(), "ownership is restored") {
			t.Fatalf("stale protector error = %v", err)
		}
		if protector.calls != 1 || target.applyCalls != 0 ||
			outcome.Record.Status != state.DeleteReconciliationRunning ||
			outcome.Record.PendingBatch == nil ||
			backend.record.PendingBatch == nil {
			t.Fatalf(
				"stale protector outcome=%#v backend=%#v calls=%d apply=%d",
				outcome,
				backend.record,
				protector.calls,
				target.applyCalls,
			)
		}
	})

	t.Run("protector must invoke callback", func(t *testing.T) {
		request := deleteTestRequest(t)
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows:           [][]any{{"a", int64(1)}},
			parameterLimit: 100,
			state:          backend,
		}
		reconciler := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		)
		reconciler.protector = deleteNoopProtector{}
		outcome, err := reconciler.reconcile(
			context.Background(),
			request,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"without invoking the protected operation",
		) {
			t.Fatalf("no-callback protector error = %v", err)
		}
		if target.applyCalls != 0 ||
			outcome.Record.Status != state.DeleteReconciliationRunning {
			t.Fatalf(
				"no-callback protector outcome=%#v apply=%d",
				outcome,
				target.applyCalls,
			)
		}
	})
}

func TestDeleteReconcileBatchByteCeiling(t *testing.T) {
	t.Run("positive ceiling is required before state or scans", func(t *testing.T) {
		request := deleteTestRequest(t)
		request.MaxBatchBytes = 0
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{parameterLimit: 100}
		_, err := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		).reconcile(context.Background(), request)
		if err == nil || !strings.Contains(
			err.Error(),
			"maximum batch bytes must be positive",
		) {
			t.Fatalf("zero byte ceiling error = %v", err)
		}
		assertDeleteStateWriteCalls(t, backend, 0, 0, 0, 0, 0)
		if source.openCalls != 0 || target.openCalls != 0 {
			t.Fatal("zero byte ceiling crossed preflight")
		}
	})

	t.Run("single oversized key fails before mutation", func(t *testing.T) {
		request := deleteTestRequest(t)
		request.MaxBatchBytes = 128
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows: [][]any{
				{strings.Repeat("x", 512), int64(1)},
			},
			parameterLimit: 100,
			state:          backend,
		}
		outcome, err := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		).reconcile(context.Background(), request)
		if err == nil || !strings.Contains(
			err.Error(),
			"exceeding the 128-byte ceiling",
		) {
			t.Fatalf("oversized key error = %v", err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationIncomplete ||
			target.applyCalls != 0 {
			t.Fatalf(
				"oversized key outcome=%#v apply=%d",
				outcome,
				target.applyCalls,
			)
		}
	})

	t.Run("byte ceiling splits count-admissible batch", func(t *testing.T) {
		request := deleteTestRequest(t)
		request.Policy.Reconcile.BatchSize = 10
		request.MaxBatchBytes = 150
		backend := newDeleteFakeState()
		source := &deleteFakeSource{}
		target := &deleteFakeTarget{
			rows: [][]any{
				{"a", int64(1)},
				{"b", int64(2)},
				{"c", int64(3)},
			},
			parameterLimit: 100,
			state:          backend,
		}
		outcome, err := deleteTestReconciler(
			request,
			backend,
			source,
			target,
		).reconcile(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if outcome.Record.Status !=
			state.DeleteReconciliationCompleted ||
			outcome.Record.Candidates != 3 ||
			outcome.Record.DeletedRows != 3 ||
			!reflect.DeepEqual(
				deleteTargetBatchSizes(target.batches),
				[]int{1, 1, 1},
			) {
			t.Fatalf(
				"byte-bounded outcome=%#v batches=%v",
				outcome,
				deleteTargetBatchSizes(target.batches),
			)
		}
		if backend.record.Plan == nil ||
			backend.record.Plan.BatchByteLimit != 150 ||
			backend.record.LastBatchCommit == nil {
			t.Fatalf(
				"byte ceiling evidence = %#v",
				backend.record,
			)
		}
	})
}

func TestOpenDeleteKeySpoolRejectsSymlinkAndEscape(t *testing.T) {
	directory := t.TempDir()
	outsideDirectory := t.TempDir()
	outside := filepath.Join(outsideDirectory, "outside.db")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "dmtx-delete-link.db")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := openDeleteKeySpool(
		directory,
		link,
	); err == nil || !strings.Contains(
		err.Error(),
		"must not be a symbolic link",
	) {
		t.Fatalf("symlink spool error = %v", err)
	}
	if _, err := openDeleteKeySpool(
		directory,
		outside,
	); err == nil || !strings.Contains(
		err.Error(),
		"escapes its configured directory",
	) {
		t.Fatalf("escaped spool error = %v", err)
	}
}

func TestDeleteSpoolReadSnapshotPreventsCandidateTOCTOU(t *testing.T) {
	directory := t.TempDir()
	const planID = "snapshot-toctou"
	spool, err := newDeleteKeySpool(directory, planID)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupDeleteKeySpool(directory, spool)
	originalParameters, err := encodeDeleteParameters(
		[]driver.Value{"original", int64(1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.db.Exec(
		`INSERT INTO target_keys (canonical, parameters) VALUES (?, ?)`,
		[]byte("canonical-key"),
		originalParameters,
	); err != nil {
		t.Fatal(err)
	}
	candidates, candidateDigest, err := spool.finalize(
		context.Background(),
		planID,
		strings.Repeat("a", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := state.DeleteReconciliationPlan{
		PlanID: planID, CandidateDigest: candidateDigest,
		Candidates: candidates,
	}
	snapshot, err := spool.beginReadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := snapshot.verify(
		context.Background(),
		plan,
		strings.Repeat("a", 64),
	); err != nil {
		t.Fatal(err)
	}

	writer, err := sql.Open("sqlite", spool.path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	writerTransaction, err := writer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer writerTransaction.Rollback()
	tamperedParameters, err := encodeDeleteParameters(
		[]driver.Value{"tampered", int64(999)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writerTransaction.Exec(
		`UPDATE target_keys SET parameters = ?`,
		tamperedParameters,
	); err != nil {
		t.Fatal(err)
	}
	commitStarted := make(chan struct{})
	commitDone := make(chan error, 1)
	go func() {
		close(commitStarted)
		commitDone <- writerTransaction.Commit()
	}()
	<-commitStarted

	batch, _, _, err := snapshot.candidateBatch(
		context.Background(),
		0,
		1,
		1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 ||
		!reflect.DeepEqual(
			batch[0].parameters,
			[]driver.Value{"original", int64(1)},
		) {
		t.Fatalf("snapshot candidate batch = %#v", batch)
	}
	select {
	case err := <-commitDone:
		t.Fatalf("concurrent writer committed through read snapshot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatalf("commit concurrent spool writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent spool writer stayed blocked after snapshot close")
	}
	after, err := spool.beginReadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if err := after.verify(
		context.Background(),
		plan,
		strings.Repeat("a", 64),
	); err == nil {
		t.Fatal("post-snapshot spool tamper matched the durable plan")
	}
}

func deleteTestRequest(t *testing.T) deleteReconcileRequest {
	t.Helper()
	now := time.Date(2026, 7, 30, 15, 0, 0, 0, time.UTC)
	request := deleteReconcileRequest{
		RunID: "run-delete", AttemptID: "attempt-delete",
		Task: state.TaskKey{
			Type: "table-copy", Schema: "public", Table: "items",
		},
		SourceTable: schema.Table{
			Schema: "public", Name: "items",
			Columns: []schema.Column{
				{
					Name: "tenant", Type: "text",
					PrimaryKey: true, PrimaryKeyPosition: 1,
				},
				{
					Name: "id", Type: "bigint",
					PrimaryKey: true, PrimaryKeyPosition: 2,
				},
				{Name: "payload", Type: "text"},
			},
		},
		TargetMode: "upsert",
		Policy: config.DeletePolicy{
			Mode:           config.DeleteModeReconcile,
			TargetBehavior: config.DeleteTargetHard,
			Reconcile: config.DeleteReconcilePolicy{
				Schedule:          config.DeleteScheduleInterval,
				Interval:          24 * time.Hour,
				BatchSize:         100,
				RequirePrimaryKey: true,
			},
		},
		Now:            now,
		SpoolDirectory: t.TempDir(),
		MaxBatchBytes:  1 << 20,
	}
	request.TargetTable = request.SourceTable
	request.TargetTable.Schema = "destination"
	request.TargetTable.Columns = append(
		[]schema.Column(nil),
		request.SourceTable.Columns...,
	)
	return request
}

func deleteTestReconciler(
	request deleteReconcileRequest,
	backend *deleteFakeState,
	source *deleteFakeSource,
	target *deleteFakeTarget,
) deleteReconciler {
	return deleteReconciler{
		state: backend, source: source, target: target,
		canonicalizer: &deleteTestCanonicalizer{},
		protector:     &deleteFakeProtector{},
		now: func() time.Time {
			return request.Now.Add(time.Minute)
		},
	}
}

type deleteFakeProtector struct {
	err      error
	afterErr error
	calls    int
}

func (protector *deleteFakeProtector) ProtectDeleteMutation(
	_ context.Context,
	mutation func() error,
) error {
	protector.calls++
	if protector.err != nil {
		return protector.err
	}
	return errors.Join(mutation(), protector.afterErr)
}

type deleteNoopProtector struct{}

func (deleteNoopProtector) ProtectDeleteMutation(
	_ context.Context,
	_ func() error,
) error {
	return nil
}

func deleteCompletedEvidence(
	request deleteReconcileRequest,
	completedAt time.Time,
) state.DeleteReconciliation {
	return state.DeleteReconciliation{
		RunID: request.RunID, Task: request.Task,
		AttemptID: "latest-success",
		Due:       true, Status: state.DeleteReconciliationCompleted,
		StartedAt:   completedAt.Add(-time.Minute),
		CompletedAt: completedAt,
	}
}

type deleteTestCanonicalizer struct {
	disabled              bool
	badFingerprint        bool
	omitCollationEvidence bool
}

func (canonicalizer *deleteTestCanonicalizer) ProveDeleteKeyEquality(
	source schema.Table,
	target schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (deleteKeyEqualityProof, error) {
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		source,
		sourcePrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		target,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	if canonicalizer.badFingerprint {
		sourceFingerprint = strings.Repeat("0", 64)
	}
	proof := deleteKeyEqualityProof{
		CanonicalizerID:   "test-exact-key-v1",
		SourceFingerprint: sourceFingerprint,
		TargetFingerprint: targetFingerprint,
		Columns: make(
			[]deleteKeyColumnProof,
			len(sourcePrimaryKey),
		),
	}
	for index, column := range sourcePrimaryKey {
		kind, err := validationKindForColumn(column)
		if err != nil {
			return deleteKeyEqualityProof{}, err
		}
		switch kind {
		case validationText, validationUUID:
			proof.Columns[index].Semantics = "binary_text"
			if !canonicalizer.omitCollationEvidence {
				proof.Columns[index].CollationEvidence =
					"test-proven-binary-equality"
			}
		case validationInteger:
			proof.Columns[index].Semantics = "integer"
		case validationBoolean:
			proof.Columns[index].Semantics = "boolean"
		case validationDecimal:
			proof.Columns[index].Semantics = "decimal"
		case validationFloat:
			proof.Columns[index].Semantics = "float_exact"
		case validationBytes:
			proof.Columns[index].Semantics = "binary"
		case validationDate:
			proof.Columns[index].Semantics = "date"
		case validationTime:
			proof.Columns[index].Semantics = "time"
		case validationTimestamp:
			proof.Columns[index].Semantics = "timestamp"
		default:
			return deleteKeyEqualityProof{}, fmt.Errorf(
				"unsupported test key kind %s",
				kind,
			)
		}
	}
	return proof, nil
}

func (canonicalizer *deleteTestCanonicalizer) CanonicalizeDeleteKeyValue(
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	index int,
	value any,
) (deleteCanonicalValue, error) {
	var payload []byte
	var err error
	switch proof.Columns[index].Semantics {
	case "binary_text", "uuid_binary_text":
		payload, err = canonicalValidationText(value)
	case "integer":
		payload, err = canonicalValidationInteger(value)
	case "boolean":
		payload, err = canonicalValidationBoolean(value)
	case "decimal":
		payload, err = canonicalValidationDecimal(value)
	case "float_exact":
		payload, err = canonicalValidationFloat(value)
	case "binary":
		payload, err = canonicalValidationBytes(value)
	case "date":
		payload, err = canonicalValidationDate(value)
	case "time":
		payload, err = canonicalValidationTime(value)
	case "timestamp":
		payload, err = canonicalValidationTimestamp(value)
	default:
		err = fmt.Errorf("unsupported test proof")
	}
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	result := deleteCanonicalValue{
		Canonical: append([]byte(nil), payload...),
	}
	if side == deleteKeyTargetSide {
		parameter, err := driver.DefaultParameterConverter.
			ConvertValue(value)
		if err != nil {
			return deleteCanonicalValue{}, err
		}
		result.Parameter = parameter
	}
	return result, nil
}

type deleteFakeRows struct {
	rows   [][]any
	index  int
	rowErr error
	events *[]string
	label  string
}

func (rows *deleteFakeRows) Next() bool {
	if rows.index >= len(rows.rows) {
		return false
	}
	rows.index++
	return true
}

func (rows *deleteFakeRows) Values() ([]any, error) {
	return append([]any(nil), rows.rows[rows.index-1]...), nil
}

func (rows *deleteFakeRows) Err() error { return rows.rowErr }

func (rows *deleteFakeRows) Close() error {
	if rows.events != nil {
		*rows.events = append(
			*rows.events,
			rows.label+"-close",
		)
	}
	return nil
}

type deleteFakeSource struct {
	rows      [][]any
	openCalls int
	events    *[]string
}

func (source *deleteFakeSource) OpenDeletePrimaryKeys(
	_ context.Context,
	_ schema.Table,
	_ []string,
) (deleteKeyRows, error) {
	source.openCalls++
	if source.events != nil {
		*source.events = append(*source.events, "source-open")
	}
	return &deleteFakeRows{
		rows: source.rows, events: source.events, label: "source",
	}, nil
}

type deleteFakeTarget struct {
	rows             [][]any
	parameterLimit   int
	openCalls        int
	applyCalls       int
	mutations        int
	batches          []deleteTargetBatch
	receipts         map[string]deleteTargetBatchReceipt
	tokenCalls       map[string]int
	firstToken       string
	events           *[]string
	state            *deleteFakeState
	applyHook        func(deleteTargetBatch, int) error
	afterReceiptHook func()
	shortDelete      bool
	receiptErr       error
}

func (target *deleteFakeTarget) OpenDeletePrimaryKeys(
	_ context.Context,
	_ schema.Table,
	_ []string,
) (deleteKeyRows, error) {
	target.openCalls++
	if target.events != nil {
		*target.events = append(*target.events, "target-open")
	}
	return &deleteFakeRows{
		rows: target.rows, events: target.events, label: "target",
	}, nil
}

func (target *deleteFakeTarget) MaxDeleteParameters() int {
	return target.parameterLimit
}

func (target *deleteFakeTarget) ApplyDeleteBatch(
	_ context.Context,
	batch deleteTargetBatch,
) (deleteTargetBatchReceipt, error) {
	target.applyCalls++
	if target.events != nil {
		*target.events = append(*target.events, "target-apply")
	}
	if target.tokenCalls == nil {
		target.tokenCalls = make(map[string]int)
	}
	target.tokenCalls[batch.Token]++
	if target.firstToken == "" {
		target.firstToken = batch.Token
	}
	if target.receipts == nil {
		target.receipts =
			make(map[string]deleteTargetBatchReceipt)
	}
	if receipt, found := target.receipts[batch.Token]; found {
		return receipt, nil
	}
	if target.applyHook != nil {
		if err := target.applyHook(
			batch,
			target.applyCalls,
		); err != nil {
			return deleteTargetBatchReceipt{}, err
		}
	}
	copied := batch
	copied.Columns = append([]string(nil), batch.Columns...)
	copied.Keys = make([][]driver.Value, len(batch.Keys))
	for index := range batch.Keys {
		copied.Keys[index] = append(
			[]driver.Value(nil),
			batch.Keys[index]...,
		)
	}
	target.batches = append(target.batches, copied)
	deleted := int64(len(batch.Keys))
	if target.shortDelete {
		deleted = 0
	}
	digest := sha256.Sum256([]byte(
		batch.Token + ":" + strconv.FormatInt(deleted, 10),
	))
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token,
		Sequence:      batch.Sequence,
		BatchDigest:   batch.BatchDigest,
		Candidates:    int64(len(batch.Keys)),
		DeletedRows:   deleted,
		ReceiptDigest: hex.EncodeToString(digest[:]),
	}
	if target.receiptErr != nil {
		receipt.FailClosedReason =
			state.DeleteReconciliationReasonTargetMutationFailed
	}
	target.receipts[batch.Token] = receipt
	target.mutations++
	if target.afterReceiptHook != nil {
		target.afterReceiptHook()
	}
	return receipt, target.receiptErr
}

type deleteFakeState struct {
	record      state.DeleteReconciliation
	recordFound bool
	latest      state.DeleteReconciliation
	latestFound bool

	beginCalls       int
	finishCalls      int
	savePlanCalls    int
	beginBatchCalls  int
	commitBatchCalls int

	commitBatchFailBeforeOnce    bool
	commitBatchApplyThenFailOnce bool
	finishFailBeforeOnce         bool
	savePlanHook                 func(state.DeleteReconciliationPlan) error
}

func newDeleteFakeState() *deleteFakeState {
	return &deleteFakeState{}
}

func (backend *deleteFakeState) LoadDeleteReconciliation(
	_ string,
	_ state.TaskKey,
	_ string,
) (state.DeleteReconciliation, bool, error) {
	return backend.record, backend.recordFound, nil
}

func (backend *deleteFakeState) LoadLatestSuccessfulDeleteReconciliation(
	_ string,
	_ state.TaskKey,
) (state.DeleteReconciliation, bool, error) {
	return backend.latest, backend.latestFound, nil
}

func (backend *deleteFakeState) BeginDeleteReconciliation(
	record state.DeleteReconciliation,
) (state.DeleteReconciliation, bool, error) {
	backend.beginCalls++
	if backend.recordFound {
		return backend.record, false, nil
	}
	if record.Due {
		record.Status = state.DeleteReconciliationRunning
	} else {
		record.Status = state.DeleteReconciliationNotDue
		record.CompletedAt = record.StartedAt
	}
	backend.record, backend.recordFound = record, true
	return record, true, nil
}

func (backend *deleteFakeState) SaveDeleteReconciliationPlan(
	plan state.DeleteReconciliationPlan,
) error {
	backend.savePlanCalls++
	if backend.record.Plan != nil {
		if *backend.record.Plan != plan {
			return errors.New("plan differs")
		}
		return nil
	}
	backend.record.Plan = &plan
	backend.record.Candidates = plan.Candidates
	if backend.savePlanHook != nil {
		return backend.savePlanHook(plan)
	}
	return nil
}

func (backend *deleteFakeState) BeginDeleteReconciliationBatch(
	batch state.DeleteReconciliationBatch,
) (state.DeleteReconciliationBatch, bool, error) {
	backend.beginBatchCalls++
	if backend.record.PendingBatch != nil {
		return *backend.record.PendingBatch, false, nil
	}
	backend.record.PendingBatch = &batch
	return batch, true, nil
}

func (backend *deleteFakeState) CommitDeleteReconciliationBatch(
	commit state.DeleteReconciliationBatchCommit,
) error {
	backend.commitBatchCalls++
	if backend.commitBatchFailBeforeOnce {
		backend.commitBatchFailBeforeOnce = false
		return errors.New("injected pre-state batch commit failure")
	}
	apply := func() {
		backend.record.Frontier += commit.Candidates
		backend.record.CommittedBatches++
		backend.record.DeletedRows += commit.DeletedRows
		backend.record.PendingBatch = nil
		backend.record.LastBatchCommit = &commit
	}
	if backend.commitBatchApplyThenFailOnce {
		backend.commitBatchApplyThenFailOnce = false
		apply()
		return errors.New("injected post-state batch commit failure")
	}
	apply()
	return nil
}

func (backend *deleteFakeState) FinishDeleteReconciliation(
	result state.DeleteReconciliationResult,
) error {
	backend.finishCalls++
	if backend.finishFailBeforeOnce {
		backend.finishFailBeforeOnce = false
		return errors.New("injected finish failure")
	}
	backend.record = deleteRecordFromResult(
		backend.record,
		result,
	)
	if result.Status == state.DeleteReconciliationCompleted {
		backend.latest = backend.record
		backend.latestFound = true
	}
	return nil
}

func assertDeleteStateWriteCalls(
	t *testing.T,
	backend *deleteFakeState,
	begin int,
	finish int,
	plan int,
	beginBatch int,
	commitBatch int,
) {
	t.Helper()
	got := []int{
		backend.beginCalls,
		backend.finishCalls,
		backend.savePlanCalls,
		backend.beginBatchCalls,
		backend.commitBatchCalls,
	}
	want := []int{begin, finish, plan, beginBatch, commitBatch}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state write calls = %v, want %v", got, want)
	}
}

func deleteTargetBatchSizes(
	batches []deleteTargetBatch,
) []int {
	result := make([]int, len(batches))
	for index := range batches {
		result[index] = len(batches[index].Keys)
	}
	return result
}

func deleteTargetKeys(
	batches []deleteTargetBatch,
) []string {
	result := make([]string, 0)
	for _, batch := range batches {
		for _, key := range batch.Keys {
			result = append(
				result,
				fmt.Sprintf("%s/%v", key[0], key[1]),
			)
		}
	}
	return result
}
