package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func TestNetworkStateCoordinatorRichReplayEvidenceConformance(
	t *testing.T,
) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			run := newNetworkStateTestRun(t, stateKind, "network-state")
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatalf("ensure plans: %v", err)
			}
			restores, err := coordinator.loadRestores(ctx)
			if err != nil {
				t.Fatalf("load initial restores: %v", err)
			}
			if len(restores) != 1 ||
				restores[0].NextSequence != 0 ||
				restores[0].RowsDone != 0 ||
				len(restores[0].Issued) != 0 {
				t.Fatalf("initial restores = %#v", restores)
			}

			end := mustEncodeNetworkStateFrontier(
				t,
				state.TypedTuple{state.Int64Value(2)},
			)
			issued := NetworkIssuedChunk{
				RangeIndex:  0,
				Sequence:    0,
				Rows:        2,
				EndFrontier: end,
				Fingerprint: "page-fingerprint-0",
				Exhausted:   true,
			}
			if err := coordinator.recordIssued(ctx, issued); err != nil {
				t.Fatalf("record issued: %v", err)
			}
			driverCalls := 0
			write := coordinator.wrapWrite(
				networkStateTestProtector{},
				func(
					context.Context,
					NetworkWriteRequest,
				) (WriteReceipt, error) {
					driverCalls++
					return WriteReceipt{
						Certainty:     CommitNotCommitted,
						AttemptedRows: 2,
					}, errors.New("fixture stops after authorization")
				},
			)
			if _, err := write(ctx, NetworkWriteRequest{
				Range: NetworkRangePlan{
					RangeIndex:   0,
					TopologyHash: "topology-a",
				},
				Sequence: 0,
				Rows:     [][]any{{int64(1)}, {int64(2)}},
			}); err == nil || driverCalls != 1 {
				t.Fatalf(
					"write error=%v driver calls=%d",
					err,
					driverCalls,
				)
			}
			if err := coordinator.checkpoint(
				ctx,
				NetworkRangeCheckpoint{
					RangeIndex:   0,
					TopologyHash: "topology-a",
					Frontier: AckFrontier{
						RangeID:        "range/0",
						SequenceOffset: 1,
						Rows:           1,
					},
				},
			); err != nil {
				t.Fatalf("checkpoint partial prefix: %v", err)
			}

			reopened := newNetworkStateTestCoordinator(t, run)
			restores, err = reopened.loadRestores(ctx)
			if err != nil {
				t.Fatalf("load reopened restores: %v", err)
			}
			want := NetworkRangeRestore{
				RangeIndex:     0,
				TopologyHash:   "topology-a",
				SequenceOffset: 1,
				Issued:         []NetworkIssuedChunk{issued},
			}
			if !reflect.DeepEqual(restores, []NetworkRangeRestore{want}) {
				t.Fatalf(
					"reopened restores = %#v, want %#v",
					restores,
					[]NetworkRangeRestore{want},
				)
			}
			_, workRange := networkStateTestSnapshot(t, run)
			if len(workRange.Pending) != 1 ||
				workRange.Pending[0].DurableRows != 1 ||
				!workRange.Pending[0].IssuedEndValid ||
				workRange.Pending[0].Fingerprint !=
					issued.Fingerprint ||
				!workRange.Pending[0].Exhausted {
				t.Fatalf(
					"rich pending evidence = %#v",
					workRange.Pending,
				)
			}
		})
	}
}

func TestNetworkStateCoordinatorCompletesOnlyDurableFrontier(
	t *testing.T,
) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				stateKind,
				"network-complete",
			)
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatal(err)
			}
			end := mustEncodeNetworkStateFrontier(
				t,
				state.TypedTuple{state.Int64Value(2)},
			)
			if err := coordinator.recordIssued(
				ctx,
				NetworkIssuedChunk{
					RangeIndex:  0,
					Sequence:    0,
					Rows:        2,
					EndFrontier: end,
					Fingerprint: "page-fingerprint-0",
					Exhausted:   true,
				},
			); err != nil {
				t.Fatal(err)
			}
			driverSawAttempt := false
			write := coordinator.wrapWrite(
				networkStateTestProtector{},
				func(
					context.Context,
					NetworkWriteRequest,
				) (WriteReceipt, error) {
					task, workRange := networkStateTestSnapshot(t, run)
					driverSawAttempt =
						task.Attempts == 1 &&
							workRange.Attempts == 1 &&
							workRange.Pending[0].Attempts == 1
					return WriteReceipt{
						Certainty:     CommitDurable,
						AttemptedRows: 2,
						CommittedRows: 2,
					}, nil
				},
			)
			receipt, err := write(ctx, NetworkWriteRequest{
				Range: NetworkRangePlan{
					RangeIndex:   0,
					TopologyHash: "topology-a",
				},
				Sequence: 0,
				Rows:     [][]any{{int64(1)}, {int64(2)}},
			})
			if err != nil || receipt.CommittedRows != 2 ||
				!driverSawAttempt {
				t.Fatalf(
					"write receipt=%#v err=%v attempt-before-driver=%v",
					receipt,
					err,
					driverSawAttempt,
				)
			}
			checkpoint := NetworkRangeCheckpoint{
				RangeIndex:   0,
				TopologyHash: "topology-a",
				Frontier: AckFrontier{
					RangeID:      "range/0",
					NextSequence: 1,
					Rows:         2,
				},
				FrontierBytes: end,
				Complete:      true,
			}
			if err := coordinator.checkpoint(ctx, checkpoint); err != nil {
				t.Fatalf("complete checkpoint: %v", err)
			}
			task, workRange := networkStateTestSnapshot(t, run)
			if task.Status != "completed" ||
				workRange.Status != "completed" ||
				workRange.NextSequence != 1 ||
				workRange.RowsDone != 2 ||
				len(workRange.Pending) != 0 {
				t.Fatalf(
					"completed task=%#v range=%#v",
					task,
					workRange,
				)
			}
			if err := coordinator.checkpoint(ctx, checkpoint); err != nil {
				t.Fatalf("idempotent complete checkpoint: %v", err)
			}
		})
	}
}

func TestNetworkStateCoordinatorComposesWithTransferCoreAndResume(
	t *testing.T,
) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				stateKind,
				"network-core-compose",
			)
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatal(err)
			}
			restores, err := coordinator.loadRestores(ctx)
			if err != nil {
				t.Fatal(err)
			}
			plan := networkTransferTestPlan(1)
			plan.Ranges[0].TableSchema = "source"
			plan.Ranges[0].TableName = "events"
			plan.Ranges[0].TopologyHash = "topology-a"
			plan.Restores = restores
			end := mustEncodeNetworkStateFrontier(
				t,
				state.TypedTuple{state.Int64Value(1)},
			)
			readCalls, writeCalls := 0, 0
			result, err := RunResumableNetworkTransfer(
				ctx,
				plan,
				NetworkTransferCallbacks{
					ReadPage: func(
						context.Context,
						NetworkReadRequest,
					) (NetworkReadPage, error) {
						readCalls++
						return NetworkReadPage{
							Rows:          [][]any{{int64(1)}},
							RowBytes:      []int64{8},
							RetainedBytes: 8,
							EndFrontier:   end,
							Fingerprint:   "page-0",
							Exhausted:     true,
						}, nil
					},
					WritePage: coordinator.wrapWrite(
						networkStateTestProtector{},
						func(
							_ context.Context,
							request NetworkWriteRequest,
						) (WriteReceipt, error) {
							writeCalls++
							return WriteReceipt{
								Certainty: CommitDurable,
								AttemptOffset: request.
									AttemptOffset,
								AttemptedRows: int64(
									len(request.Rows),
								),
								CommittedRows: int64(
									len(request.Rows),
								),
							}, nil
						},
					),
					RecordIssued: coordinator.recordIssued,
					Checkpoint:   coordinator.checkpoint,
				},
			)
			if err != nil ||
				result.Rows != 1 ||
				result.CompletedRanges != 1 ||
				readCalls != 1 ||
				writeCalls != 1 {
				t.Fatalf(
					"result=%#v err=%v reads=%d writes=%d",
					result,
					err,
					readCalls,
					writeCalls,
				)
			}

			reopened := newNetworkStateTestCoordinator(t, run)
			plan.Restores, err = reopened.loadRestores(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var callbacks int
			unexpectedRead := func(
				context.Context,
				NetworkReadRequest,
			) (NetworkReadPage, error) {
				callbacks++
				return NetworkReadPage{}, errors.New(
					"completed restore read unexpectedly",
				)
			}
			unexpectedWrite := func(
				context.Context,
				NetworkWriteRequest,
			) (WriteReceipt, error) {
				callbacks++
				return WriteReceipt{}, errors.New(
					"completed restore wrote unexpectedly",
				)
			}
			result, err = RunResumableNetworkTransfer(
				ctx,
				plan,
				NetworkTransferCallbacks{
					ReadPage:  unexpectedRead,
					WritePage: unexpectedWrite,
					RecordIssued: func(
						context.Context,
						NetworkIssuedChunk,
					) error {
						callbacks++
						return errors.New(
							"completed restore issued unexpectedly",
						)
					},
					Checkpoint: func(
						context.Context,
						NetworkRangeCheckpoint,
					) error {
						callbacks++
						return errors.New(
							"completed restore checkpointed unexpectedly",
						)
					},
				},
			)
			if err != nil ||
				result.Rows != 1 ||
				result.CompletedRanges != 1 ||
				callbacks != 0 {
				t.Fatalf(
					"resume result=%#v err=%v callbacks=%d",
					result,
					err,
					callbacks,
				)
			}
		})
	}
}

func TestNetworkStateCoordinatorStateFailurePreventsDriver(
	t *testing.T,
) {
	run := newNetworkStateTestRun(t, "sqlite", "network-no-intent")
	coordinator := newNetworkStateTestCoordinator(t, run)
	if err := coordinator.ensurePlans(context.Background()); err != nil {
		t.Fatal(err)
	}
	driverCalls := 0
	write := coordinator.wrapWrite(
		networkStateTestProtector{},
		func(
			context.Context,
			NetworkWriteRequest,
		) (WriteReceipt, error) {
			driverCalls++
			return WriteReceipt{}, nil
		},
	)
	receipt, err := write(
		context.Background(),
		NetworkWriteRequest{
			Range: NetworkRangePlan{
				RangeIndex:   0,
				TopologyHash: "topology-a",
			},
			Sequence: 0,
			Rows:     [][]any{{int64(1)}},
		},
	)
	if err == nil || driverCalls != 0 ||
		receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 {
		t.Fatalf(
			"receipt=%#v err=%v driver calls=%d",
			receipt,
			err,
			driverCalls,
		)
	}
}

func TestNetworkStateCoordinatorRequiresFencedMutationProtector(
	t *testing.T,
) {
	run := newNetworkStateTestRun(
		t,
		"sqlite",
		"network-requires-protector",
	)
	coordinator := newNetworkStateTestCoordinator(t, run)
	ctx := context.Background()
	if err := coordinator.ensurePlans(ctx); err != nil {
		t.Fatal(err)
	}
	end := mustEncodeNetworkStateFrontier(
		t,
		state.TypedTuple{state.Int64Value(1)},
	)
	if err := coordinator.recordIssued(
		ctx,
		NetworkIssuedChunk{
			RangeIndex:  0,
			Sequence:    0,
			Rows:        1,
			EndFrontier: end,
			Fingerprint: "page-0",
		},
	); err != nil {
		t.Fatal(err)
	}
	driverCalls := 0
	write := coordinator.wrapWrite(
		nil,
		func(
			context.Context,
			NetworkWriteRequest,
		) (WriteReceipt, error) {
			driverCalls++
			return WriteReceipt{}, nil
		},
	)
	receipt, err := write(ctx, NetworkWriteRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TopologyHash: "topology-a",
		},
		Sequence: 0,
		Rows:     [][]any{{int64(1)}},
	})
	if ClassifyTransferError(err) != ErrorClassState ||
		driverCalls != 0 ||
		receipt.Certainty != CommitNotCommitted {
		t.Fatalf(
			"receipt=%#v error=%v driver calls=%d",
			receipt,
			err,
			driverCalls,
		)
	}
	task, workRange := networkStateTestSnapshot(t, run)
	if task.Attempts != 0 || workRange.Attempts != 0 ||
		workRange.Pending[0].Attempts != 0 {
		t.Fatalf(
			"missing protector authorized target attempt: task=%#v range=%#v",
			task,
			workRange,
		)
	}
}

func TestNetworkStateCoordinatorTakeoverAfterAttemptPreventsDriver(
	t *testing.T,
) {
	run := newNetworkStateTestRun(
		t,
		"sqlite",
		"network-takeover-before-driver",
	)
	coordinator := newNetworkStateTestCoordinator(t, run)
	ctx := context.Background()
	if err := coordinator.ensurePlans(ctx); err != nil {
		t.Fatal(err)
	}
	end := mustEncodeNetworkStateFrontier(
		t,
		state.TypedTuple{state.Int64Value(1)},
	)
	if err := coordinator.recordIssued(
		ctx,
		NetworkIssuedChunk{
			RangeIndex:  0,
			Sequence:    0,
			Rows:        1,
			EndFrontier: end,
			Fingerprint: "page-0",
		},
	); err != nil {
		t.Fatal(err)
	}
	protectorCalls := 0
	protector := networkStateTestProtector{
		protect: func(
			_ context.Context,
			_ func() error,
		) error {
			protectorCalls++
			task, workRange := networkStateTestSnapshot(t, run)
			if task.Attempts != 1 ||
				workRange.Attempts != 1 ||
				workRange.Pending[0].Attempts != 1 {
				t.Fatalf(
					"attempt was not durable before lease check: task=%#v range=%#v",
					task,
					workRange,
				)
			}
			return state.ErrLeaseLost
		},
	}
	driverCalls := 0
	write := coordinator.wrapWrite(
		protector,
		func(
			context.Context,
			NetworkWriteRequest,
		) (WriteReceipt, error) {
			driverCalls++
			return WriteReceipt{}, nil
		},
	)
	receipt, err := write(ctx, NetworkWriteRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TopologyHash: "topology-a",
		},
		Sequence: 0,
		Rows:     [][]any{{int64(1)}},
	})
	if ClassifyTransferError(err) != ErrorClassState ||
		!errors.Is(err, state.ErrLeaseLost) ||
		protectorCalls != 1 ||
		driverCalls != 0 ||
		receipt.Certainty != CommitNotCommitted {
		t.Fatalf(
			"receipt=%#v error=%v protector calls=%d driver calls=%d",
			receipt,
			err,
			protectorCalls,
			driverCalls,
		)
	}
}

func TestNetworkStateCoordinatorRejectsNegativeAttemptEvidenceBeforeDriver(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*state.WorkTask, *state.RangeState)
	}{
		{
			name: "task attempts",
			mutate: func(task *state.WorkTask, _ *state.RangeState) {
				task.Attempts = -1
			},
		},
		{
			name: "task retries",
			mutate: func(task *state.WorkTask, _ *state.RangeState) {
				task.Retries = -1
			},
		},
		{
			name: "range attempts",
			mutate: func(_ *state.WorkTask, workRange *state.RangeState) {
				workRange.Attempts = -1
			},
		},
		{
			name: "range retries",
			mutate: func(_ *state.WorkTask, workRange *state.RangeState) {
				workRange.Retries = -1
			},
		},
		{
			name: "pending attempts",
			mutate: func(_ *state.WorkTask, workRange *state.RangeState) {
				workRange.Pending[0].Attempts = -1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				"sqlite",
				"network-negative-"+strings.ReplaceAll(
					test.name,
					" ",
					"-",
				),
			)
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatal(err)
			}
			end := mustEncodeNetworkStateFrontier(
				t,
				state.TypedTuple{state.Int64Value(1)},
			)
			if err := coordinator.recordIssued(
				ctx,
				NetworkIssuedChunk{
					RangeIndex:  0,
					Sequence:    0,
					Rows:        1,
					EndFrontier: end,
					Fingerprint: "page-0",
				},
			); err != nil {
				t.Fatal(err)
			}
			coordinator.run.Backend = networkStateTamperBackend{
				Stage4StateBackend: coordinator.run.Backend,
				mutate: func(
					tasks []state.WorkTask,
					ranges []state.RangeState,
				) {
					test.mutate(&tasks[0], &ranges[0])
				},
			}
			driverCalls := 0
			restoreError := func() error {
				restores, err := coordinator.loadRestores(ctx)
				if err != nil {
					return err
				}
				plan := networkTransferTestPlan(1)
				plan.Ranges[0].TableSchema = "source"
				plan.Ranges[0].TableName = "events"
				plan.Ranges[0].TopologyHash = "topology-a"
				plan.Restores = restores
				_, err = RunResumableNetworkTransfer(
					ctx,
					plan,
					NetworkTransferCallbacks{
						ReadPage: func(
							context.Context,
							NetworkReadRequest,
						) (NetworkReadPage, error) {
							return NetworkReadPage{},
								errors.New("read unexpectedly")
						},
						WritePage: coordinator.wrapWrite(
							networkStateTestProtector{},
							func(
								context.Context,
								NetworkWriteRequest,
							) (WriteReceipt, error) {
								driverCalls++
								return WriteReceipt{}, nil
							},
						),
						RecordIssued: coordinator.recordIssued,
						Checkpoint:   coordinator.checkpoint,
					},
				)
				return err
			}()
			if restoreError == nil || driverCalls != 0 {
				t.Fatalf(
					"error=%v driver calls=%d",
					restoreError,
					driverCalls,
				)
			}
		})
	}
}

func TestNetworkStateCoordinatorRecoversRangeTaskCompletionWindow(
	t *testing.T,
) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				stateKind,
				"network-task-recovery",
			)
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatal(err)
			}
			binding := coordinator.bindings[0]
			if err := run.Backend.CompleteRange(
				run.RunID,
				binding.Task,
				binding.Initial.ID,
				binding.Initial.TopologyHash,
				0,
				coordinator.now(),
			); err != nil {
				t.Fatal(err)
			}
			task, workRange := networkStateTestSnapshot(t, run)
			if task.Status != "running" ||
				workRange.Status != "completed" {
				t.Fatalf(
					"pre-recovery task=%#v range=%#v",
					task,
					workRange,
				)
			}
			restores, err := coordinator.loadRestores(ctx)
			if err != nil {
				t.Fatalf("recover/load restores: %v", err)
			}
			if len(restores) != 1 || !restores[0].Complete {
				t.Fatalf("restores = %#v", restores)
			}
			task, workRange = networkStateTestSnapshot(t, run)
			if task.Status != "completed" ||
				workRange.Status != "completed" {
				t.Fatalf(
					"post-recovery task=%#v range=%#v",
					task,
					workRange,
				)
			}
		})
	}
}

func TestNetworkStateCoordinatorValidatesBeforeCompletionRecovery(
	t *testing.T,
) {
	run := newNetworkStateTestRun(
		t,
		"sqlite",
		"network-validate-before-recovery",
	)
	coordinator := newNetworkStateTestCoordinator(t, run)
	ctx := context.Background()
	if err := coordinator.ensurePlans(ctx); err != nil {
		t.Fatal(err)
	}
	binding := coordinator.bindings[0]
	if err := run.Backend.BeginRangeChunk(state.RangeChunkIntent{
		RunID:        run.RunID,
		Task:         binding.Task,
		RangeID:      binding.Initial.ID,
		TopologyHash: binding.Initial.TopologyHash,
		Sequence:     0,
		ChunkRows:    1,
		Fingerprint:  "page-0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := run.Backend.RecordRangeAttempt(state.RangeAttempt{
		RunID:        run.RunID,
		Task:         binding.Task,
		RangeID:      binding.Initial.ID,
		TopologyHash: binding.Initial.TopologyHash,
		Sequence:     0,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := run.Backend.AcknowledgeRange(
		state.RangeAcknowledgement{
			RunID:        run.RunID,
			Task:         binding.Task,
			RangeID:      binding.Initial.ID,
			TopologyHash: binding.Initial.TopologyHash,
			Sequence:     0,
			ChunkRows:    1,
			DurableRows:  1,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := run.Backend.CompleteRange(
		run.RunID,
		binding.Task,
		binding.Initial.ID,
		binding.Initial.TopologyHash,
		1,
		coordinator.now(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.loadRestores(ctx); err == nil ||
		!strings.Contains(err.Error(), "missing its frontier") {
		t.Fatalf("load error = %v", err)
	}
	task, workRange := networkStateTestSnapshot(t, run)
	if task.Status != "running" ||
		workRange.Status != "completed" {
		t.Fatalf(
			"invalid restore mutated completion: task=%#v range=%#v",
			task,
			workRange,
		)
	}
}

func TestNetworkStateCoordinatorRejectsUnrepresentableLaterPrefix(
	t *testing.T,
) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			run := newNetworkStateTestRun(
				t,
				stateKind,
				"network-later-prefix",
			)
			coordinator := newNetworkStateTestCoordinator(t, run)
			ctx := context.Background()
			if err := coordinator.ensurePlans(ctx); err != nil {
				t.Fatal(err)
			}
			end := mustEncodeNetworkStateFrontier(
				t,
				state.TypedTuple{state.Int64Value(2)},
			)
			if err := coordinator.recordIssued(
				ctx,
				NetworkIssuedChunk{
					RangeIndex:  0,
					Sequence:    1,
					Rows:        1,
					EndFrontier: end,
					Fingerprint: "page-1",
				},
			); err != nil {
				t.Fatal(err)
			}
			write := coordinator.wrapWrite(
				networkStateTestProtector{},
				func(
					_ context.Context,
					request NetworkWriteRequest,
				) (WriteReceipt, error) {
					return WriteReceipt{
						Certainty:     CommitDurable,
						AttemptedRows: int64(len(request.Rows)),
						CommittedRows: int64(len(request.Rows)),
					}, nil
				},
			)
			if _, err := write(ctx, NetworkWriteRequest{
				Range: NetworkRangePlan{
					RangeIndex:   0,
					TopologyHash: "topology-a",
				},
				Sequence: 1,
				Rows:     [][]any{{int64(2)}},
			}); err != nil {
				t.Fatal(err)
			}
			binding := coordinator.bindings[0]
			if _, err := run.Backend.AcknowledgeRange(
				state.RangeAcknowledgement{
					RunID:        run.RunID,
					Task:         binding.Task,
					RangeID:      binding.Initial.ID,
					TopologyHash: binding.Initial.TopologyHash,
					Sequence:     1,
					ChunkRows:    1,
					DurableRows:  1,
					Frontier: state.TypedTuple{
						state.Int64Value(2),
					},
					FrontierValid: true,
				},
			); err != nil {
				t.Fatal(err)
			}
			beforeTask, beforeRange := networkStateTestSnapshot(
				t,
				run,
			)
			if _, err := coordinator.loadRestores(ctx); err == nil ||
				!strings.Contains(
					err.Error(),
					"unrepresentable durable prefix",
				) {
				t.Fatalf("load error = %v", err)
			}
			afterTask, afterRange := networkStateTestSnapshot(t, run)
			if !reflect.DeepEqual(beforeTask, afterTask) ||
				!reflect.DeepEqual(beforeRange, afterRange) {
				t.Fatal(
					"rejected later prefix mutated durable state",
				)
			}
		})
	}
}

func TestNetworkStateFrontierCodecFailsClosed(t *testing.T) {
	valid := state.TypedTuple{
		state.Int64Value(9_007_199_254_740_993),
		state.TextValue("exact"),
		state.BytesValue([]byte{0, 1, 2}),
	}
	encoded := mustEncodeNetworkStateFrontier(t, valid)
	decoded, found, err := decodeNetworkStateFrontier(encoded)
	if err != nil || !found || !reflect.DeepEqual(decoded, valid) {
		t.Fatalf(
			"decoded=%#v found=%v err=%v",
			decoded,
			found,
			err,
		)
	}
	for name, malformed := range map[string][]byte{
		"JSON whitespace": append([]byte(" "), encoded...),
		"null":            []byte("null"),
		"empty tuple":     []byte("[]"),
		"float": []byte(
			`[{"kind":"int64","encoded":"1.0"}]`,
		),
		"leading plus": []byte(
			`[{"kind":"int64","encoded":"+1"}]`,
		),
		"leading zero": []byte(
			`[{"kind":"int64","encoded":"01"}]`,
		),
		"negative zero": []byte(
			`[{"kind":"int64","encoded":"-0"}]`,
		),
		"base64 ignored newline": []byte(
			`[{"kind":"bytes","encoded":"A\nA=="}]`,
		),
		"base64 padding bits": []byte(
			`[{"kind":"bytes","encoded":"AB=="}]`,
		),
		"unknown kind": []byte(
			`[{"kind":"decimal","encoded":"1"}]`,
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, found, err := decodeNetworkStateFrontier(
				malformed,
			); err == nil || found {
				t.Fatalf("found=%v err=%v", found, err)
			}
		})
	}
	if _, err := encodeNetworkStateFrontier(
		state.TypedTuple{state.Int64Value(1)},
		false,
	); err == nil {
		t.Fatal("invalid frontier with values was accepted")
	}
	if _, err := encodeNetworkStateFrontier(
		state.TypedTuple{{
			Kind:    state.ValueText,
			Encoded: string([]byte{0xff}),
		}},
		true,
	); err == nil {
		t.Fatal("invalid UTF-8 text frontier was accepted")
	}
}

type networkStateTestProtector struct {
	protect func(context.Context, func() error) error
}

func (networkStateTestProtector) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (networkStateTestProtector) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func (protector networkStateTestProtector) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if protector.protect != nil {
		return protector.protect(ctx, mutation)
	}
	return mutation()
}

type networkStateTamperBackend struct {
	Stage4StateBackend
	mutate func([]state.WorkTask, []state.RangeState)
}

func (backend networkStateTamperBackend) ListWork(
	runID string,
) ([]state.WorkTask, []state.RangeState, error) {
	tasks, ranges, err := backend.Stage4StateBackend.ListWork(runID)
	if err == nil && backend.mutate != nil {
		backend.mutate(tasks, ranges)
	}
	return tasks, ranges, err
}

func newNetworkStateTestCoordinator(
	t *testing.T,
	run Stage4RunContext,
) *networkStateCoordinator {
	t.Helper()
	coordinator, err := newNetworkStateCoordinator(
		run,
		[]networkStateRangeBinding{{
			RangeIndex: 0,
			Task: state.TaskKey{
				Type:   "table-copy",
				Schema: "source",
				Table:  "events",
			},
			Initial: state.RangeState{
				ID:             "range-0",
				Strategy:       "integer_keyset",
				TopologyHash:   "topology-a",
				Lower:          state.TypedTuple{state.Int64Value(0)},
				Upper:          state.TypedTuple{state.Int64Value(100)},
				UpperInclusive: true,
			},
		}},
	)
	if err != nil {
		t.Fatalf("new network state coordinator: %v", err)
	}
	coordinator.now = func() time.Time {
		return time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	}
	return coordinator
}

func newNetworkStateTestRun(
	t *testing.T,
	stateKind string,
	runID string,
) Stage4RunContext {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "spool")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(runID))
	spool := filepath.Join(parent, hex.EncodeToString(digest[:]))
	if err := os.Mkdir(spool, 0o700); err != nil {
		t.Fatal(err)
	}
	var backend Stage4StateBackend
	switch stateKind {
	case "sqlite":
		backend = state.SQLiteStore{
			Path: filepath.Join(root, "state.db"),
		}
	case "yaml":
		backend = state.YAMLStore{
			Path: filepath.Join(root, "state.yaml"),
		}
	default:
		t.Fatalf("unknown state kind %q", stateKind)
	}
	return Stage4RunContext{
		RunID:          runID,
		Backend:        backend,
		SpoolDirectory: spool,
	}
}

func mustEncodeNetworkStateFrontier(
	t *testing.T,
	tuple state.TypedTuple,
) []byte {
	t.Helper()
	encoded, err := encodeNetworkStateFrontier(tuple, true)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func networkStateTestSnapshot(
	t *testing.T,
	run Stage4RunContext,
) (state.WorkTask, state.RangeState) {
	t.Helper()
	tasks, ranges, err := run.Backend.ListWork(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || len(ranges) != 1 {
		t.Fatalf("tasks=%#v ranges=%#v", tasks, ranges)
	}
	return tasks[0], ranges[0]
}
